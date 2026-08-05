# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Capability-free preparation contract for atomic PostgreSQL bundle cutovers.
This pathless module performs no I/O or mutation. Preparation is capped at 4,096
relations before relation traversal, sorting, or hashing. A future publisher must
recheck every fence transactionally and resolve ambiguous commits before retry.
Existing ``_old`` state requires separately authorized, capacity-checked GC.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hmac
from typing import Any, Literal, Mapping

from .staged_bundle_publication_contract_core import (
    MAX_STAGED_BUNDLE_RELATIONS,
    STAGED_BUNDLE_PUBLICATION_CONTRACT,
    CatalogFingerprints,
    StagedBundlePublicationContractError,
    _BUNDLE_FIELDS,
    _CONTRACT_DOMAIN,
    _FIXED_INPUTS,
    _OID_FENCE_DOMAIN,
    _POINTER_FENCE_DOMAIN,
    _PUBLICATION_MODES,
    _RELATION_FIELDS,
    _SOURCE_FENCE_DOMAIN,
    _bounded_relations,
    _canonical_sha256,
    _fail,
    _fingerprints_from_raw,
    _fingerprints_input,
    _fingerprints_tuple,
    _optional_public_id,
    _positive_oid,
    _require_exact_dict,
    _strict_identifier_tuple,
    _strict_literal,
    _strict_pg_identifier,
    _strict_public_id,
    _strict_stage_relation_name,
    _strict_sha256,
    derive_stage_relation_name,
)


@dataclass(frozen=True, slots=True, repr=False)
class StagedRelationIntent:
    """One intent with exact, name-neutral stage/live catalog fingerprints."""

    role: str
    live_relation: str
    stage_relation: str
    old_relation: str
    observed_live_oid: int | None
    observed_stage_oid: int
    observed_old_oid: None
    stage_logged: Literal[True]
    old_relation_expected_absent: Literal[True]
    catalog_parity_verified: Literal[True]
    stage_fingerprints: CatalogFingerprints
    live_fingerprints: CatalogFingerprints | None

    def __post_init__(self) -> None:
        try:
            canonical_by_field = _validated_relation_values(self)
            for field_name, canonical_value in canonical_by_field.items():
                object.__setattr__(self, field_name, canonical_value)
        except StagedBundlePublicationContractError:
            raise
        except Exception:
            raise _fail() from None


def _validated_relation_values(
    relation: object,
) -> dict[str, object]:
    if type(relation) is not StagedRelationIntent:
        raise _fail()
    role = _strict_pg_identifier(relation.role)
    live_relation = _strict_pg_identifier(relation.live_relation)
    stage_relation = _strict_stage_relation_name(relation.stage_relation, live_relation)
    old_relation = _strict_pg_identifier(relation.old_relation)
    live_oid = relation.observed_live_oid
    if live_oid is not None:
        live_oid = _positive_oid(live_oid)
    stage_oid = _positive_oid(relation.observed_stage_oid)
    stage_fingerprints = _fingerprints_tuple(relation.stage_fingerprints)
    live_fingerprints = relation.live_fingerprints
    if live_fingerprints is not None:
        live_fingerprints = _fingerprints_tuple(live_fingerprints)
    names = (role, live_relation, stage_relation, old_relation)
    if (
        len(names) != len(set(names))
        or old_relation != f"{live_relation}_old"
        or relation.observed_old_oid is not None
        or relation.stage_logged is not True
        or relation.old_relation_expected_absent is not True
        or relation.catalog_parity_verified is not True
        or stage_oid == live_oid
        or (live_oid is None) != (live_fingerprints is None)
        or (live_fingerprints is not None and live_fingerprints != stage_fingerprints)
    ):
        raise _fail()
    return {
        "role": role,
        "live_relation": live_relation,
        "stage_relation": stage_relation,
        "old_relation": old_relation,
        "observed_live_oid": live_oid,
        "observed_stage_oid": stage_oid,
        "observed_old_oid": None,
        "stage_logged": True,
        "old_relation_expected_absent": True,
        "catalog_parity_verified": True,
        "stage_fingerprints": stage_fingerprints,
        "live_fingerprints": live_fingerprints,
    }


def _relation_names(relation: StagedRelationIntent) -> tuple[str, str, str, str]:
    return (
        relation.role,
        relation.live_relation,
        relation.stage_relation,
        relation.old_relation,
    )


def _relation_from_raw(
    raw: object, *, schema: str, run_id: str
) -> StagedRelationIntent:
    _require_exact_dict(raw, _RELATION_FIELDS)
    live_raw = raw.get("live_fingerprints")
    relation = StagedRelationIntent(
        role=raw.get("role"),
        live_relation=raw.get("live_relation"),
        stage_relation=raw.get("stage_relation"),
        old_relation=raw.get("old_relation"),
        observed_live_oid=raw.get("observed_live_oid"),
        observed_stage_oid=raw.get("observed_stage_oid"),
        observed_old_oid=raw.get("observed_old_oid"),
        stage_logged=raw.get("stage_logged"),
        old_relation_expected_absent=raw.get("old_relation_expected_absent"),
        catalog_parity_verified=raw.get("catalog_parity_verified"),
        stage_fingerprints=_fingerprints_from_raw(raw.get("stage_fingerprints")),
        live_fingerprints=(
            None if live_raw is None else _fingerprints_from_raw(live_raw)
        ),
    )
    expected_stage = derive_stage_relation_name(
        schema, run_id, relation.role, relation.live_relation
    )
    if relation.stage_relation != expected_stage:
        raise _fail()
    return relation


def _relation_input(relation: StagedRelationIntent) -> dict[str, object]:
    relation_dict = _validated_relation_values(relation)
    relation_dict["stage_fingerprints"] = _fingerprints_input(
        relation.stage_fingerprints
    )
    relation_dict["live_fingerprints"] = (
        None
        if relation.live_fingerprints is None
        else _fingerprints_input(relation.live_fingerprints)
    )
    return relation_dict


def _validate_relation_collisions(relations: tuple[StagedRelationIntent, ...]) -> None:
    names = [value for relation in relations for value in _relation_names(relation)]
    oids = [
        oid
        for relation in relations
        for oid in (relation.observed_live_oid, relation.observed_stage_oid)
        if oid is not None
    ]
    if len(names) != len(set(names)) or len(oids) != len(set(oids)):
        raise _fail()


def _publication_mode(
    relations: tuple[StagedRelationIntent, ...],
    predecessor: str | None,
    current: str | None,
    previous: str | None,
    generation: str,
) -> Literal["initial", "replacement"]:
    live_states = {relation.observed_live_oid is not None for relation in relations}
    if len(live_states) != 1:
        raise _fail()
    if live_states == {False}:
        if any(value is not None for value in (predecessor, current, previous)):
            raise _fail()
        return "initial"
    if predecessor is None or current != predecessor:
        raise _fail()
    if generation in {predecessor, previous} or previous == current:
        raise _fail()
    return "replacement"


def _normalized_bundle(raw: object) -> dict[str, Any]:
    _require_exact_dict(raw, _BUNDLE_FIELDS)
    for name, expected in _FIXED_INPUTS.items():
        _strict_literal(raw.get(name), expected)
    schema = _strict_pg_identifier(raw.get("schema"))
    run_id = _strict_public_id(raw.get("run_id"))
    generation = _strict_public_id(raw.get("generation_id"))
    predecessor = _optional_public_id(raw.get("expected_predecessor_generation_id"))
    current = _optional_public_id(raw.get("expected_current_generation_id"))
    previous = _optional_public_id(raw.get("expected_previous_generation_id"))
    raw_relations = _bounded_relations(raw.get("relations"))
    relations = tuple(
        sorted(
            (
                _relation_from_raw(relation_spec, schema=schema, run_id=run_id)
                for relation_spec in raw_relations
            ),
            key=lambda relation: (relation.live_relation, relation.role),
        )
    )
    _validate_relation_collisions(relations)
    mode = _publication_mode(relations, predecessor, current, previous, generation)
    source_vector = _strict_sha256(raw.get("source_vector_sha256"))
    _strict_literal(raw.get("source_vector_canonical"), True)
    return {
        "schema": schema,
        "run_id": run_id,
        "generation_id": generation,
        "expected_predecessor_generation_id": predecessor,
        "expected_current_generation_id": current,
        "expected_previous_generation_id": previous,
        "source_vector_sha256": source_vector,
        "source_vector_canonical": True,
        "relations": relations,
        "mode": mode,
    }


def _derived_bundle_state(bundle_by_key: Mapping[str, Any]) -> dict[str, object]:
    relations = bundle_by_key["relations"]
    ordered_relation_names = tuple(relation.live_relation for relation in relations)
    ordered_lock_names = tuple(
        sorted(
            relation_name
            for relation in relations
            for relation_name in (
                relation.stage_relation,
                *(
                    ()
                    if relation.observed_live_oid is None
                    else (relation.live_relation,)
                ),
            )
        )
    )
    source_dict = {
        key: bundle_by_key[key]
        for key in ("schema", "run_id", "generation_id", "source_vector_sha256")
    }
    pointer_dict = {
        key: bundle_by_key[key]
        for key in (
            "generation_id",
            "expected_predecessor_generation_id",
            "expected_current_generation_id",
            "expected_previous_generation_id",
            "mode",
        )
    }
    oid_dict = {
        "schema": bundle_by_key["schema"],
        "relations": [
            {
                "role": relation.role,
                "live_relation": relation.live_relation,
                "stage_relation": relation.stage_relation,
                "old_relation": relation.old_relation,
                "observed_live_oid": relation.observed_live_oid,
                "observed_stage_oid": relation.observed_stage_oid,
                "observed_old_oid": None,
            }
            for relation in relations
        ],
    }
    return {
        "relation_order": ordered_relation_names,
        "lock_order": ordered_lock_names,
        "source_fence_sha256": _canonical_sha256(_SOURCE_FENCE_DOMAIN, source_dict),
        "pointer_fence_sha256": _canonical_sha256(_POINTER_FENCE_DOMAIN, pointer_dict),
        "oid_fence_sha256": _canonical_sha256(_OID_FENCE_DOMAIN, oid_dict),
    }


def _descriptor_payload(
    values: Mapping[str, Any], derived: Mapping[str, object]
) -> dict[str, object]:
    return {
        "contract": STAGED_BUNDLE_PUBLICATION_CONTRACT,
        **{key: values[key] for key in values if key != "relations"},
        "relations": [_relation_input(relation) for relation in values["relations"]],
        **derived,
        **_FIXED_INPUTS,
    }


@dataclass(frozen=True, slots=True, repr=False)
class StagedBundlePublicationDescriptor:
    """Frozen prepublication witness with no executable capability."""

    schema: str
    run_id: str
    generation_id: str
    expected_predecessor_generation_id: str | None
    expected_current_generation_id: str | None
    expected_previous_generation_id: str | None
    source_vector_sha256: str
    source_vector_canonical: Literal[True]
    relations: tuple[StagedRelationIntent, ...]
    mode: Literal["initial", "replacement"]
    relation_order: tuple[str, ...]
    lock_order: tuple[str, ...]
    source_fence_sha256: str
    pointer_fence_sha256: str
    oid_fence_sha256: str
    contract_sha256: str
    contract: str = field(default=STAGED_BUNDLE_PUBLICATION_CONTRACT, init=False)
    serving_authority: Literal["none"] = field(default="none", init=False)
    publication_authorized: Literal[False] = field(default=False, init=False)
    cleanup_authorized: Literal[False] = field(default=False, init=False)
    reverse_swap_authorized: Literal[False] = field(default=False, init=False)
    database_io_enabled: Literal[False] = field(default=False, init=False)
    retained_old_required: Literal[True] = field(default=True, init=False)
    automatic_old_deletion_enabled: Literal[False] = field(default=False, init=False)
    automatic_gc_enabled: Literal[False] = field(default=False, init=False)

    def __post_init__(self) -> None:
        try:
            normalized, derived = _validated_descriptor_state(self)
            object.__setattr__(self, "relations", normalized["relations"])
            object.__setattr__(self, "mode", normalized["mode"])
            for field_name, canonical_value in derived.items():
                object.__setattr__(self, field_name, canonical_value)
        except StagedBundlePublicationContractError:
            raise
        except Exception:
            raise _fail() from None


def _descriptor_input(
    descriptor: StagedBundlePublicationDescriptor,
) -> dict[str, object]:
    relations = _bounded_relations(descriptor.relations)
    input_dict = {
        "schema": _strict_pg_identifier(descriptor.schema),
        "run_id": _strict_public_id(descriptor.run_id),
        "generation_id": _strict_public_id(descriptor.generation_id),
        "expected_predecessor_generation_id": _optional_public_id(
            descriptor.expected_predecessor_generation_id
        ),
        "expected_current_generation_id": _optional_public_id(
            descriptor.expected_current_generation_id
        ),
        "expected_previous_generation_id": _optional_public_id(
            descriptor.expected_previous_generation_id
        ),
        "source_vector_sha256": _strict_sha256(descriptor.source_vector_sha256),
        "source_vector_canonical": _strict_literal(
            descriptor.source_vector_canonical, True
        ),
    }
    input_dict["relations"] = tuple(_relation_input(relation) for relation in relations)
    for name, expected in _FIXED_INPUTS.items():
        input_dict[name] = _strict_literal(getattr(descriptor, name), expected)
    return input_dict


def _validated_descriptor_state(
    descriptor: object,
) -> tuple[dict[str, Any], dict[str, object]]:
    if type(descriptor) is not StagedBundlePublicationDescriptor:
        raise _fail()
    _strict_literal(descriptor.contract, STAGED_BUNDLE_PUBLICATION_CONTRACT)
    normalized = _normalized_bundle(_descriptor_input(descriptor))
    derived = _derived_bundle_state(normalized)
    if type(descriptor.mode) is not str or descriptor.mode not in _PUBLICATION_MODES:
        raise _fail()
    supplied_by_field = {
        "relation_order": _strict_identifier_tuple(
            descriptor.relation_order, maximum=MAX_STAGED_BUNDLE_RELATIONS
        ),
        "lock_order": _strict_identifier_tuple(
            descriptor.lock_order,
            maximum=MAX_STAGED_BUNDLE_RELATIONS * 2,
            allow_stage_digest=True,
        ),
        "source_fence_sha256": _strict_sha256(descriptor.source_fence_sha256),
        "pointer_fence_sha256": _strict_sha256(descriptor.pointer_fence_sha256),
        "oid_fence_sha256": _strict_sha256(descriptor.oid_fence_sha256),
    }
    if descriptor.mode != normalized["mode"] or supplied_by_field != derived:
        raise _fail()
    expected_digest = _canonical_sha256(
        _CONTRACT_DOMAIN, _descriptor_payload(normalized, derived)
    )
    if not hmac.compare_digest(
        _strict_sha256(descriptor.contract_sha256), expected_digest
    ):
        raise _fail()
    return normalized, derived


def _descriptor_from_normalized(
    normalized: Mapping[str, Any],
) -> StagedBundlePublicationDescriptor:
    derived = _derived_bundle_state(normalized)
    return StagedBundlePublicationDescriptor(
        **normalized,
        **derived,
        contract_sha256=_canonical_sha256(
            _CONTRACT_DOMAIN, _descriptor_payload(normalized, derived)
        ),
    )


def build_staged_bundle_publication_contract(
    raw: Mapping[str, object],
) -> StagedBundlePublicationDescriptor:
    """Validate and freeze one deterministic, capability-free preparation."""
    try:
        return _descriptor_from_normalized(_normalized_bundle(raw))
    except StagedBundlePublicationContractError:
        raise
    except Exception:
        raise _fail() from None


def validate_staged_bundle_publication_contract(
    descriptor: object,
) -> StagedBundlePublicationDescriptor:
    """Rebuild an exact descriptor and reject foreign, deleted, or forged state."""
    try:
        normalized, _ = _validated_descriptor_state(descriptor)
        return _descriptor_from_normalized(normalized)
    except StagedBundlePublicationContractError:
        raise
    except Exception:
        raise _fail() from None
