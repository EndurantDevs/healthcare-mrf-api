# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Capability-free intent contract for future public-evidence bundle cutovers.

This module performs no I/O and grants no publication authority. A future
executor must revalidate durable source releases and transactionally recheck
generation pointers, relation OIDs, retained-name absence, persistence, and
catalog fingerprints before it may perform a separately authorized cutover.
"""

from __future__ import annotations

import hmac
from typing import Any, Literal, Mapping, NamedTuple

from public_evidence.staged_bundle_publication_observations import (
    StagedRelationIntent,
    _canonical_source_witnesses,
    _publication_mode,
    _relation_from_raw,
    _relation_input,
    _source_vector_sha256,
    _source_witnesses_from_releases,
    _validate_relation_collisions,
)
from public_evidence.staged_bundle_publication_primitives import (
    CATALOG_FINGERPRINT_CONTRACT,
    CATALOG_FINGERPRINT_EXCLUSIONS,
    MAX_STAGED_BUNDLE_RELATIONS,
    STAGED_BUNDLE_PUBLICATION_INTENT_CONTRACT,
    PublicEvidenceSourceWitness,
    StagedBundlePublicationIntentError,
    _BUNDLE_INPUT_FIELDS,
    _CATALOG_FENCE_DOMAIN,
    _CONTRACT_DOMAIN,
    _FIXED_DESCRIPTOR_STATE,
    _OID_FENCE_DOMAIN,
    _POINTER_FENCE_DOMAIN,
    _SOURCE_FENCE_DOMAIN,
    _bounded_tuple,
    _canonical_sha256,
    _fail,
    _fingerprints_payload,
    _optional_generation_ref,
    _require_exact_dict,
    _source_witness_payload,
    _strict_build_run_ref,
    _strict_generation_ref,
    _strict_identifier_tuple,
    _strict_literal,
    _strict_pg_identifier,
    _strict_sha256,
    _strict_string_tuple_literal,
)


class StagedBundlePublicationIntent(NamedTuple):
    """Deeply immutable, non-executable bundle publication intent."""

    contract: str
    catalog_fingerprint_contract: str
    catalog_fingerprint_exclusions: tuple[str, str]
    schema: str
    build_run_ref: str
    generation_ref: str
    expected_predecessor_generation_ref: str | None
    expected_current_generation_ref: str | None
    expected_previous_generation_ref: str | None
    source_witnesses: tuple[PublicEvidenceSourceWitness, ...]
    source_vector_sha256: str
    relations: tuple[StagedRelationIntent, ...]
    mode: Literal["initial", "replacement"]
    relation_order: tuple[str, ...]
    lock_order: tuple[str, ...]
    source_fence_sha256: str
    pointer_fence_sha256: str
    oid_fence_sha256: str
    catalog_fence_sha256: str
    contract_sha256: str
    lifecycle_state: Literal["validated_intent_only"]
    serving_authority: Literal["none"]
    current_pointer_authority: Literal["none"]
    executor_authority: Literal["none"]
    publication_authorized: Literal[False]
    publication_enabled: Literal[False]
    cleanup_authorized: Literal[False]
    reverse_swap_authorized: Literal[False]
    database_io_enabled: Literal[False]
    executable_rename_choreography_defined: Literal[False]
    index_rename_choreography_defined: Literal[False]
    retained_old_required: Literal[True]
    automatic_old_deletion_enabled: Literal[False]
    automatic_gc_enabled: Literal[False]

    def __repr__(self) -> str:
        return "StagedBundlePublicationIntent(<redacted>)"


def _normalized_bundle(raw: object) -> dict[str, Any]:
    _require_exact_dict(raw, _BUNDLE_INPUT_FIELDS)
    schema = _strict_pg_identifier(raw.get("schema"))
    build_run_ref = _strict_build_run_ref(raw.get("build_run_ref"))
    generation = _strict_generation_ref(raw.get("generation_ref"))
    current = _optional_generation_ref(raw.get("expected_current_generation_ref"))
    previous = _optional_generation_ref(raw.get("expected_previous_generation_ref"))
    source_witnesses = _source_witnesses_from_releases(raw.get("source_releases"))
    raw_relations = _bounded_tuple(
        raw.get("relations"),
        maximum=MAX_STAGED_BUNDLE_RELATIONS,
    )
    relations = tuple(
        sorted(
            (
                _relation_from_raw(
                    relation,
                    schema=schema,
                    build_run_ref=build_run_ref,
                )
                for relation in raw_relations
            ),
            key=lambda relation: (relation.live_relation, relation.role),
        )
    )
    _validate_relation_collisions(relations)
    mode, predecessor = _publication_mode(
        relations,
        generation,
        current,
        previous,
    )
    return {
        "schema": schema,
        "build_run_ref": build_run_ref,
        "generation_ref": generation,
        "expected_predecessor_generation_ref": predecessor,
        "expected_current_generation_ref": current,
        "expected_previous_generation_ref": previous,
        "source_witnesses": source_witnesses,
        "source_vector_sha256": _source_vector_sha256(source_witnesses),
        "relations": relations,
        "mode": mode,
    }


def _normalized_descriptor(descriptor: object) -> dict[str, Any]:
    if type(descriptor) is not StagedBundlePublicationIntent:
        raise _fail()
    schema = _strict_pg_identifier(descriptor.schema)
    build_run_ref = _strict_build_run_ref(descriptor.build_run_ref)
    generation = _strict_generation_ref(descriptor.generation_ref)
    current = _optional_generation_ref(descriptor.expected_current_generation_ref)
    previous = _optional_generation_ref(descriptor.expected_previous_generation_ref)
    witnesses = _canonical_source_witnesses(descriptor.source_witnesses)
    if descriptor.source_witnesses != witnesses:
        raise _fail()
    relations_raw = _bounded_tuple(
        descriptor.relations,
        maximum=MAX_STAGED_BUNDLE_RELATIONS,
    )
    relations = tuple(
        sorted(
            (
                _relation_from_raw(
                    _relation_input(
                        relation,
                        schema=schema,
                        build_run_ref=build_run_ref,
                    ),
                    schema=schema,
                    build_run_ref=build_run_ref,
                )
                for relation in relations_raw
            ),
            key=lambda relation: (relation.live_relation, relation.role),
        )
    )
    if descriptor.relations != relations:
        raise _fail()
    _validate_relation_collisions(relations)
    mode, predecessor = _publication_mode(relations, generation, current, previous)
    normalized_by_field = {
        "schema": schema,
        "build_run_ref": build_run_ref,
        "generation_ref": generation,
        "expected_predecessor_generation_ref": predecessor,
        "expected_current_generation_ref": current,
        "expected_previous_generation_ref": previous,
        "source_witnesses": witnesses,
        "source_vector_sha256": _source_vector_sha256(witnesses),
        "relations": relations,
        "mode": mode,
    }
    _strict_literal(descriptor.mode, mode)
    _strict_literal(descriptor.expected_predecessor_generation_ref, predecessor)
    if not hmac.compare_digest(
        _strict_sha256(descriptor.source_vector_sha256),
        normalized_by_field["source_vector_sha256"],
    ):
        raise _fail()
    return normalized_by_field


def _relation_payload(relation: StagedRelationIntent) -> dict[str, object]:
    return {
        "role": relation.role,
        "live_relation": relation.live_relation,
        "stage_relation": relation.stage_relation,
        "old_relation": relation.old_relation,
        "observed_live_oid": relation.observed_live_oid,
        "observed_stage_oid": relation.observed_stage_oid,
        "observed_old_oid": None,
        "stage_persistence": relation.stage_persistence,
        "expected_fingerprints": _fingerprints_payload(relation.expected_fingerprints),
        "stage_fingerprints": _fingerprints_payload(relation.stage_fingerprints),
        "live_fingerprints": (
            None
            if relation.live_fingerprints is None
            else _fingerprints_payload(relation.live_fingerprints)
        ),
        "stage_catalog_state": relation.stage_catalog_state,
        "live_catalog_state": relation.live_catalog_state,
        "old_relation_observed_absent": True,
    }


def _catalog_relation_payload(
    relation: StagedRelationIntent,
) -> dict[str, object]:
    return {
        "role": relation.role,
        "live_relation": relation.live_relation,
        "stage_persistence": relation.stage_persistence,
        "expected_fingerprints": _fingerprints_payload(relation.expected_fingerprints),
        "stage_fingerprints": _fingerprints_payload(relation.stage_fingerprints),
        "live_fingerprints": (
            None
            if relation.live_fingerprints is None
            else _fingerprints_payload(relation.live_fingerprints)
        ),
        "stage_catalog_state": relation.stage_catalog_state,
        "live_catalog_state": relation.live_catalog_state,
    }


def _relation_orders(
    bundle_by_field: Mapping[str, Any],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    relations = bundle_by_field["relations"]
    relation_names = tuple(relation.live_relation for relation in relations)
    lock_names = tuple(
        sorted(
            name
            for relation in relations
            for name in (
                relation.stage_relation,
                *(
                    (relation.live_relation,)
                    if bundle_by_field["mode"] == "replacement"
                    else ()
                ),
            )
        )
    )
    return relation_names, lock_names


def _source_fence_payload(bundle_by_field: Mapping[str, Any]) -> dict[str, object]:
    return {
        "schema": bundle_by_field["schema"],
        "build_run_ref": bundle_by_field["build_run_ref"],
        "generation_ref": bundle_by_field["generation_ref"],
        "source_vector_sha256": bundle_by_field["source_vector_sha256"],
        "source_witnesses": [
            _source_witness_payload(witness)
            for witness in bundle_by_field["source_witnesses"]
        ],
    }


def _pointer_fence_payload(bundle_by_field: Mapping[str, Any]) -> dict[str, object]:
    return {
        key: bundle_by_field[key]
        for key in (
            "generation_ref",
            "expected_predecessor_generation_ref",
            "expected_current_generation_ref",
            "expected_previous_generation_ref",
            "mode",
        )
    }


def _oid_fence_payload(bundle_by_field: Mapping[str, Any]) -> dict[str, object]:
    return {
        "schema": bundle_by_field["schema"],
        "relations": [
            {
                key: _relation_payload(relation)[key]
                for key in (
                    "role",
                    "live_relation",
                    "stage_relation",
                    "old_relation",
                    "observed_live_oid",
                    "observed_stage_oid",
                    "observed_old_oid",
                )
            }
            for relation in bundle_by_field["relations"]
        ],
    }


def _catalog_fence_payload(bundle_by_field: Mapping[str, Any]) -> dict[str, object]:
    return {
        "contract": CATALOG_FINGERPRINT_CONTRACT,
        "exclusions": list(CATALOG_FINGERPRINT_EXCLUSIONS),
        "schema": bundle_by_field["schema"],
        "relations": [
            _catalog_relation_payload(relation)
            for relation in bundle_by_field["relations"]
        ],
    }


def _derived_state(bundle_by_field: Mapping[str, Any]) -> dict[str, object]:
    """Derive deterministic orders and four domain-separated observation fences."""
    relation_names, lock_names = _relation_orders(bundle_by_field)
    return {
        "relation_order": relation_names,
        "lock_order": lock_names,
        "source_fence_sha256": _canonical_sha256(
            _SOURCE_FENCE_DOMAIN,
            _source_fence_payload(bundle_by_field),
        ),
        "pointer_fence_sha256": _canonical_sha256(
            _POINTER_FENCE_DOMAIN,
            _pointer_fence_payload(bundle_by_field),
        ),
        "oid_fence_sha256": _canonical_sha256(
            _OID_FENCE_DOMAIN,
            _oid_fence_payload(bundle_by_field),
        ),
        "catalog_fence_sha256": _canonical_sha256(
            _CATALOG_FENCE_DOMAIN,
            _catalog_fence_payload(bundle_by_field),
        ),
    }


def _descriptor_payload(
    values: Mapping[str, Any],
    derived: Mapping[str, object],
) -> dict[str, object]:
    return {
        "contract": STAGED_BUNDLE_PUBLICATION_INTENT_CONTRACT,
        "catalog_fingerprint_contract": CATALOG_FINGERPRINT_CONTRACT,
        "catalog_fingerprint_exclusions": list(CATALOG_FINGERPRINT_EXCLUSIONS),
        **{
            key: values[key]
            for key in (
                "schema",
                "build_run_ref",
                "generation_ref",
                "expected_predecessor_generation_ref",
                "expected_current_generation_ref",
                "expected_previous_generation_ref",
                "source_vector_sha256",
                "mode",
            )
        },
        "source_witnesses": [
            _source_witness_payload(witness) for witness in values["source_witnesses"]
        ],
        "relations": [_relation_payload(relation) for relation in values["relations"]],
        **derived,
        **_FIXED_DESCRIPTOR_STATE,
    }


def _intent_from_normalized(
    normalized: Mapping[str, Any],
) -> StagedBundlePublicationIntent:
    derived = _derived_state(normalized)
    contract_sha256 = _canonical_sha256(
        _CONTRACT_DOMAIN,
        _descriptor_payload(normalized, derived),
    )
    return StagedBundlePublicationIntent(
        contract=STAGED_BUNDLE_PUBLICATION_INTENT_CONTRACT,
        catalog_fingerprint_contract=CATALOG_FINGERPRINT_CONTRACT,
        catalog_fingerprint_exclusions=CATALOG_FINGERPRINT_EXCLUSIONS,
        **normalized,
        **derived,
        contract_sha256=contract_sha256,
        **_FIXED_DESCRIPTOR_STATE,
    )


def _validate_derived_state(
    descriptor: StagedBundlePublicationIntent,
    normalized: Mapping[str, Any],
) -> None:
    _strict_literal(descriptor.contract, STAGED_BUNDLE_PUBLICATION_INTENT_CONTRACT)
    _strict_literal(
        descriptor.catalog_fingerprint_contract, CATALOG_FINGERPRINT_CONTRACT
    )
    _strict_string_tuple_literal(
        descriptor.catalog_fingerprint_exclusions,
        CATALOG_FINGERPRINT_EXCLUSIONS,
    )
    for field_name, expected in _FIXED_DESCRIPTOR_STATE.items():
        _strict_literal(getattr(descriptor, field_name), expected)
    derived = _derived_state(normalized)
    supplied_by_field = {
        "relation_order": _strict_identifier_tuple(
            descriptor.relation_order,
            maximum=MAX_STAGED_BUNDLE_RELATIONS,
        ),
        "lock_order": _strict_identifier_tuple(
            descriptor.lock_order,
            maximum=MAX_STAGED_BUNDLE_RELATIONS * 2,
        ),
        "source_fence_sha256": _strict_sha256(descriptor.source_fence_sha256),
        "pointer_fence_sha256": _strict_sha256(descriptor.pointer_fence_sha256),
        "oid_fence_sha256": _strict_sha256(descriptor.oid_fence_sha256),
        "catalog_fence_sha256": _strict_sha256(descriptor.catalog_fence_sha256),
    }
    if supplied_by_field != derived:
        raise _fail()
    expected_digest = _canonical_sha256(
        _CONTRACT_DOMAIN,
        _descriptor_payload(normalized, derived),
    )
    if not hmac.compare_digest(
        _strict_sha256(descriptor.contract_sha256),
        expected_digest,
    ):
        raise _fail()


def build_staged_bundle_publication_intent(
    raw: Mapping[str, object],
) -> StagedBundlePublicationIntent:
    """Validate and freeze one deterministic, capability-free intent."""
    try:
        return _intent_from_normalized(_normalized_bundle(raw))
    except StagedBundlePublicationIntentError:
        raise
    except Exception:
        raise _fail() from None


def validate_staged_bundle_publication_intent(
    descriptor: object,
) -> StagedBundlePublicationIntent:
    """Rebuild an exact immutable intent and reject foreign or forged state."""
    try:
        normalized = _normalized_descriptor(descriptor)
        _validate_derived_state(descriptor, normalized)
        return _intent_from_normalized(normalized)
    except StagedBundlePublicationIntentError:
        raise
    except Exception:
        raise _fail() from None
