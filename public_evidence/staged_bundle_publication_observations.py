# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Immutable source and catalog observations for bundle publication intents."""

from __future__ import annotations

from typing import Literal, NamedTuple

from public_evidence.source_release_contract import (
    PublicEvidenceSourceReleaseDescriptor,
    PublicEvidenceSourceReleaseError,
    validate_public_evidence_source_release,
)
from public_evidence.source_release_policies import SOURCE_POLICIES
from public_evidence.staged_bundle_publication_primitives import (
    MAX_STAGED_BUNDLE_SOURCE_RELEASES,
    CatalogFingerprints,
    PublicEvidenceSourceWitness,
    _RELATION_INPUT_FIELDS,
    _SOURCE_VECTOR_DOMAIN,
    _bounded_tuple,
    _canonical_sha256,
    _fail,
    _fingerprints_from_raw,
    _fingerprints_payload,
    _positive_oid,
    _require_exact_dict,
    _source_witness_payload,
    _source_witness_tuple,
    _strict_literal,
    _strict_pg_identifier,
    derive_old_relation_name,
    derive_stage_relation_name,
)


class StagedRelationIntent(NamedTuple):
    """One deeply immutable relation observation with derived catalog states."""

    role: str
    live_relation: str
    stage_relation: str
    old_relation: str
    observed_live_oid: int | None
    observed_stage_oid: int
    observed_old_oid: None
    stage_persistence: Literal["logged"]
    expected_fingerprints: CatalogFingerprints
    stage_fingerprints: CatalogFingerprints
    live_fingerprints: CatalogFingerprints | None
    stage_catalog_state: Literal["verified_equal"]
    live_catalog_state: Literal["not_applicable_no_live", "verified_equal"]
    old_relation_observed_absent: Literal[True]

    def __repr__(self) -> str:
        return "StagedRelationIntent(<redacted>)"


def _relation_from_raw(
    raw: object,
    *,
    schema: str,
    build_run_ref: str,
) -> StagedRelationIntent:
    _require_exact_dict(raw, _RELATION_INPUT_FIELDS)
    role = _strict_pg_identifier(raw.get("role"))
    live_relation = _strict_pg_identifier(raw.get("live_relation"))
    stage_relation = _strict_pg_identifier(raw.get("stage_relation"))
    old_relation = _strict_pg_identifier(raw.get("old_relation"))
    if stage_relation != derive_stage_relation_name(
        schema,
        build_run_ref,
        role,
        live_relation,
    ) or old_relation != derive_old_relation_name(live_relation):
        raise _fail()

    live_oid = raw.get("observed_live_oid")
    if live_oid is not None:
        live_oid = _positive_oid(live_oid)
    stage_oid = _positive_oid(raw.get("observed_stage_oid"))
    _strict_literal(raw.get("observed_old_oid"), None)
    _strict_literal(raw.get("stage_persistence"), "logged")

    expected = _fingerprints_from_raw(raw.get("expected_fingerprints"))
    stage = _fingerprints_from_raw(raw.get("stage_fingerprints"))
    live_raw = raw.get("live_fingerprints")
    live = None if live_raw is None else _fingerprints_from_raw(live_raw)
    if expected != stage:
        raise _fail()
    if live_oid is None:
        if live is not None:
            raise _fail()
        live_state = "not_applicable_no_live"
    else:
        if live is None or live != expected:
            raise _fail()
        live_state = "verified_equal"
    if stage_oid == live_oid or len({live_relation, stage_relation, old_relation}) != 3:
        raise _fail()
    return StagedRelationIntent(
        role=role,
        live_relation=live_relation,
        stage_relation=stage_relation,
        old_relation=old_relation,
        observed_live_oid=live_oid,
        observed_stage_oid=stage_oid,
        observed_old_oid=None,
        stage_persistence="logged",
        expected_fingerprints=expected,
        stage_fingerprints=stage,
        live_fingerprints=live,
        stage_catalog_state="verified_equal",
        live_catalog_state=live_state,
        old_relation_observed_absent=True,
    )


def _relation_input(
    relation: object,
    *,
    schema: str,
    build_run_ref: str,
) -> dict[str, object]:
    if type(relation) is not StagedRelationIntent:
        raise _fail()
    relation_by_field = {
        "role": relation.role,
        "live_relation": relation.live_relation,
        "stage_relation": relation.stage_relation,
        "old_relation": relation.old_relation,
        "observed_live_oid": relation.observed_live_oid,
        "observed_stage_oid": relation.observed_stage_oid,
        "observed_old_oid": relation.observed_old_oid,
        "stage_persistence": relation.stage_persistence,
        "expected_fingerprints": _fingerprints_payload(relation.expected_fingerprints),
        "stage_fingerprints": _fingerprints_payload(relation.stage_fingerprints),
        "live_fingerprints": (
            None
            if relation.live_fingerprints is None
            else _fingerprints_payload(relation.live_fingerprints)
        ),
    }
    rebuilt = _relation_from_raw(
        relation_by_field,
        schema=schema,
        build_run_ref=build_run_ref,
    )
    _strict_literal(relation.stage_catalog_state, rebuilt.stage_catalog_state)
    _strict_literal(relation.live_catalog_state, rebuilt.live_catalog_state)
    _strict_literal(relation.old_relation_observed_absent, True)
    return relation_by_field


def _source_witnesses_from_releases(
    value: object,
) -> tuple[PublicEvidenceSourceWitness, ...]:
    releases = _bounded_tuple(
        value,
        maximum=MAX_STAGED_BUNDLE_SOURCE_RELEASES,
    )
    witnesses = []
    for candidate in releases:
        if type(candidate) is not PublicEvidenceSourceReleaseDescriptor:
            raise _fail()
        try:
            validated = validate_public_evidence_source_release(candidate)
        except PublicEvidenceSourceReleaseError:
            raise _fail() from None
        witnesses.append(
            PublicEvidenceSourceWitness(
                source_kind=validated.source_kind,
                source_release_ref=validated.source_release_ref,
                contract_sha256=validated.contract_sha256,
            )
        )
    return _canonical_source_witnesses(tuple(witnesses))


def _canonical_source_witnesses(
    value: object,
) -> tuple[PublicEvidenceSourceWitness, ...]:
    raw_witnesses = _bounded_tuple(
        value,
        maximum=MAX_STAGED_BUNDLE_SOURCE_RELEASES,
    )
    witnesses = tuple(_source_witness_tuple(item) for item in raw_witnesses)
    if any(witness.source_kind not in SOURCE_POLICIES for witness in witnesses):
        raise _fail()
    release_refs = tuple(witness.source_release_ref for witness in witnesses)
    if len(release_refs) != len(set(release_refs)):
        raise _fail()
    return tuple(
        sorted(
            witnesses,
            key=lambda witness: (
                witness.source_kind,
                witness.source_release_ref,
                witness.contract_sha256,
            ),
        )
    )


def _source_vector_sha256(
    witnesses: tuple[PublicEvidenceSourceWitness, ...],
) -> str:
    return _canonical_sha256(
        _SOURCE_VECTOR_DOMAIN,
        [_source_witness_payload(witness) for witness in witnesses],
    )


def _validate_relation_collisions(relations: tuple[StagedRelationIntent, ...]) -> None:
    roles = tuple(relation.role for relation in relations)
    names = tuple(
        name
        for relation in relations
        for name in (
            relation.live_relation,
            relation.stage_relation,
            relation.old_relation,
        )
    )
    oids = tuple(
        oid
        for relation in relations
        for oid in (relation.observed_live_oid, relation.observed_stage_oid)
        if oid is not None
    )
    if (
        len(roles) != len(set(roles))
        or len(names) != len(set(names))
        or len(oids) != len(set(oids))
    ):
        raise _fail()


def _publication_mode(
    relations: tuple[StagedRelationIntent, ...],
    generation: str,
    current: str | None,
    previous: str | None,
) -> tuple[Literal["initial", "replacement"], str | None]:
    live_states = {relation.observed_live_oid is not None for relation in relations}
    if len(live_states) != 1:
        raise _fail()
    if live_states == {False}:
        if current is not None or previous is not None:
            raise _fail()
        return "initial", None
    if current is None or generation == current:
        raise _fail()
    if previous is not None and previous in {generation, current}:
        raise _fail()
    return "replacement", current
