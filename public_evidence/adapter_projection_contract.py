# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Dormant, source-neutral adapter projection descriptors."""

from __future__ import annotations

import hmac
from typing import Literal, NamedTuple

from public_evidence.adapter_projection_policies import (
    ADAPTER_PROJECTION_RULES,
    AdapterProjectionRule,
    adapter_projection_rule_descriptor_sha256,
)
from public_evidence.evidence_record_contract import (
    PublicEvidenceRecord,
    validate_public_evidence_record,
)
from public_evidence.evidence_record_primitives import (
    MAX_PUBLIC_EVIDENCE_SOURCE_RECORDS,
    PublicEvidenceRecordError,
)
from public_evidence.source_record_inclusion_contract import (
    validate_source_record_inclusion_witness,
)
from public_evidence.source_record_inclusion_primitives import (
    PublicEvidenceSourceRecordInclusionError,
    PublicEvidenceSourceRecordInclusionWitness,
    _canonical_sha256,
    _derived_ref,
    _strict_sha256,
    _validate_derived_ref,
)
from public_evidence.source_release_contract import PUBLIC_EVIDENCE_FOUNDATION_SCOPE

PUBLIC_EVIDENCE_ADAPTER_PROJECTION_CONTRACT = (
    "healthporta.public-evidence-adapter-projection.v1"
)
PUBLIC_EVIDENCE_ADAPTER_PROJECTION_REF_PREFIX = "peproj1_"
_INVALID = "public_evidence_adapter_projection_invalid"


class PublicEvidenceAdapterProjectionError(RuntimeError):
    """One deliberately uniform adapter-projection validation failure."""


def _fail() -> PublicEvidenceAdapterProjectionError:
    return PublicEvidenceAdapterProjectionError(_INVALID)


class AdapterProjectionAuthorityState(NamedTuple):
    """Fixed dormant trust and execution boundary for one projection."""

    lifecycle_state: Literal["validated_projection_descriptor_only"]
    source_record_inventory_state: Literal["declared_inventory_only"]
    authenticated_replay_state: Literal["required_not_executed"]
    payload_derivation_state: Literal["required_not_executed"]
    adapter_contract_state: Literal["planned_descriptor_only"]
    normalized_record_validated: Literal[True]
    inventory_membership_verified: Literal[True]
    source_bytes_authenticated: Literal[False]
    complete_inventory_scan_verified: Literal[False]
    adapter_implementation_verified: Literal[False]
    positive_evidence_only: Literal[True]
    source_authenticity_claimed: Literal[False]
    whole_source_complete: Literal[False]
    legal_ownership_claimed: Literal[False]
    employment_claimed: Literal[False]
    facility_ownership_claimed: Literal[False]
    exact_rate_site_claimed: Literal[False]
    payer_confirmed_site_claimed: Literal[False]
    site_match_claimed: Literal[False]
    independence_claimed: Literal[False]
    confidence_claimed: Literal[False]
    adapter_execution_authority: Literal["none"]
    database_io_enabled: Literal[False]
    serving_authority: Literal["none"]
    current_pointer_authority: Literal["none"]
    publication_enabled: Literal[False]
    replacement_enabled: Literal[False]
    deletion_enabled: Literal[False]
    retirement_enabled: Literal[False]
    supersession_enabled: Literal[False]


class PublicEvidenceAdapterProjection(NamedTuple):
    """One record plus exact declared-inventory membership witnesses."""

    contract: str
    foundation_scope: str
    source_release_ref: str
    source_release_contract_sha256: str
    source_kind: str
    planned_adapter_contract_id: str
    projection_rule_id: str
    projection_rule_descriptor_sha256: str
    inclusion_witnesses: tuple[PublicEvidenceSourceRecordInclusionWitness, ...]
    source_record_count: int
    source_record_vector_sha256: str
    record: PublicEvidenceRecord
    output_record_count: Literal[1]
    output_record_vector_sha256: str
    projection_ref: str
    contract_sha256: str
    authority_state: AdapterProjectionAuthorityState

    def __repr__(self) -> str:
        return "<public-evidence-adapter-projection>"


def _fixed_authority() -> AdapterProjectionAuthorityState:
    return AdapterProjectionAuthorityState(
        lifecycle_state="validated_projection_descriptor_only",
        source_record_inventory_state="declared_inventory_only",
        authenticated_replay_state="required_not_executed",
        payload_derivation_state="required_not_executed",
        adapter_contract_state="planned_descriptor_only",
        normalized_record_validated=True,
        inventory_membership_verified=True,
        source_bytes_authenticated=False,
        complete_inventory_scan_verified=False,
        adapter_implementation_verified=False,
        positive_evidence_only=True,
        source_authenticity_claimed=False,
        whole_source_complete=False,
        legal_ownership_claimed=False,
        employment_claimed=False,
        facility_ownership_claimed=False,
        exact_rate_site_claimed=False,
        payer_confirmed_site_claimed=False,
        site_match_claimed=False,
        independence_claimed=False,
        confidence_claimed=False,
        adapter_execution_authority="none",
        database_io_enabled=False,
        serving_authority="none",
        current_pointer_authority="none",
        publication_enabled=False,
        replacement_enabled=False,
        deletion_enabled=False,
        retirement_enabled=False,
        supersession_enabled=False,
    )


def _validated_record(value: object) -> PublicEvidenceRecord:
    try:
        return validate_public_evidence_record(value)
    except PublicEvidenceRecordError:
        raise _fail() from None


def _record_relationship(record: PublicEvidenceRecord) -> str:
    relationship = getattr(record.evidence, "relationship_class", None)
    if type(relationship) is not str:
        raise _fail()
    return relationship


def _projection_rule(record: PublicEvidenceRecord) -> AdapterProjectionRule:
    rule = ADAPTER_PROJECTION_RULES.get(
        (record.record_type, _record_relationship(record))
    )
    if rule is None or rule.source_kind != record.release.source_kind:
        raise _fail()
    observed_kinds = tuple(
        sorted(source_record.record_kind for source_record in record.source_records)
    )
    if observed_kinds != rule.source_record_kinds:
        raise _fail()
    if rule.source_kind == "tic" and record.release.source_binding is None:
        raise _fail()
    return rule


def _validated_witnesses(
    value: object,
) -> tuple[PublicEvidenceSourceRecordInclusionWitness, ...]:
    if (
        type(value) is not tuple
        or not 1 <= len(value) <= MAX_PUBLIC_EVIDENCE_SOURCE_RECORDS
    ):
        raise _fail()
    try:
        witnesses = tuple(
            validate_source_record_inclusion_witness(candidate) for candidate in value
        )
    except PublicEvidenceSourceRecordInclusionError:
        raise _fail() from None
    ordered_witnesses = tuple(
        sorted(witnesses, key=lambda item: item.source_record.source_record_ref)
    )
    refs = tuple(item.source_record.source_record_ref for item in ordered_witnesses)
    if len(refs) != len(set(refs)):
        raise _fail()
    return ordered_witnesses


def _validate_projection_inputs(
    record: PublicEvidenceRecord,
    witnesses: tuple[PublicEvidenceSourceRecordInclusionWitness, ...],
    rule: AdapterProjectionRule,
) -> None:
    release = record.release
    witness_refs = tuple(
        witness.source_record.source_record_ref for witness in witnesses
    )
    record_refs = tuple(item.source_record_ref for item in record.source_records)
    if witness_refs != record_refs:
        raise _fail()
    for witness in witnesses:
        inventory = witness.inventory
        if (
            inventory.release.source_release_ref != release.source_release_ref
            or inventory.release.contract_sha256 != release.contract_sha256
            or inventory.source_kind != rule.source_kind
            or inventory.record_kind != witness.source_record.record_kind
        ):
            raise _fail()


def _source_record_vector_payload(
    witnesses: tuple[PublicEvidenceSourceRecordInclusionWitness, ...],
) -> list[dict[str, str]]:
    return [
        {
            "source_record_ref": witness.source_record.source_record_ref,
            "source_record_payload_sha256": witness.source_record.payload_sha256,
            "inventory_ref": witness.inventory.inventory_ref,
            "inventory_contract_sha256": witness.inventory.contract_sha256,
            "inclusion_ref": witness.inclusion_ref,
            "inclusion_contract_sha256": witness.contract_sha256,
        }
        for witness in witnesses
    ]


def _output_record_vector_payload(record: PublicEvidenceRecord) -> list[dict[str, str]]:
    return [
        {
            "evidence_ref": record.evidence_ref,
            "evidence_contract_sha256": record.contract_sha256,
        }
    ]


def _projection_payload(
    record: PublicEvidenceRecord,
    witnesses: tuple[PublicEvidenceSourceRecordInclusionWitness, ...],
    rule: AdapterProjectionRule,
    rule_sha256: str,
    source_vector_sha256: str,
    output_vector_sha256: str,
    authority: AdapterProjectionAuthorityState,
) -> dict[str, object]:
    return {
        "contract": PUBLIC_EVIDENCE_ADAPTER_PROJECTION_CONTRACT,
        "foundation_scope": PUBLIC_EVIDENCE_FOUNDATION_SCOPE,
        "source_release_ref": record.release.source_release_ref,
        "source_release_contract_sha256": record.release.contract_sha256,
        "source_kind": record.release.source_kind,
        "planned_adapter_contract_id": rule.planned_adapter_contract_id,
        "projection_rule_id": rule.projection_rule_id,
        "projection_rule_descriptor_sha256": rule_sha256,
        "source_record_count": len(witnesses),
        "source_record_vector_sha256": source_vector_sha256,
        "output_record_count": 1,
        "output_record_vector_sha256": output_vector_sha256,
        "authority_state": authority._asdict(),
    }


def build_public_evidence_adapter_projection(
    evidence_record: PublicEvidenceRecord,
    inclusion_witnesses: tuple[PublicEvidenceSourceRecordInclusionWitness, ...],
) -> PublicEvidenceAdapterProjection:
    """Freeze one positive-evidence projection without adapter execution claims."""
    try:
        fixed_record = _validated_record(evidence_record)
        witnesses = _validated_witnesses(inclusion_witnesses)
        rule = _projection_rule(fixed_record)
        _validate_projection_inputs(fixed_record, witnesses, rule)
        rule_sha256 = adapter_projection_rule_descriptor_sha256(rule)
        source_vector = _canonical_sha256(
            "adapter_projection_source_record_vector",
            _source_record_vector_payload(witnesses),
        )
        output_vector = _canonical_sha256(
            "adapter_projection_output_record_vector",
            _output_record_vector_payload(fixed_record),
        )
        authority = _fixed_authority()
        projection_contract_payload = _projection_payload(
            fixed_record,
            witnesses,
            rule,
            rule_sha256,
            source_vector,
            output_vector,
            authority,
        )
        return PublicEvidenceAdapterProjection(
            contract=PUBLIC_EVIDENCE_ADAPTER_PROJECTION_CONTRACT,
            foundation_scope=PUBLIC_EVIDENCE_FOUNDATION_SCOPE,
            source_release_ref=fixed_record.release.source_release_ref,
            source_release_contract_sha256=fixed_record.release.contract_sha256,
            source_kind=fixed_record.release.source_kind,
            planned_adapter_contract_id=rule.planned_adapter_contract_id,
            projection_rule_id=rule.projection_rule_id,
            projection_rule_descriptor_sha256=rule_sha256,
            inclusion_witnesses=witnesses,
            source_record_count=len(witnesses),
            source_record_vector_sha256=source_vector,
            record=fixed_record,
            output_record_count=1,
            output_record_vector_sha256=output_vector,
            projection_ref=_derived_ref(
                PUBLIC_EVIDENCE_ADAPTER_PROJECTION_REF_PREFIX,
                "adapter_projection",
                projection_contract_payload,
            ),
            contract_sha256=_canonical_sha256(
                "adapter_projection_contract", projection_contract_payload
            ),
            authority_state=authority,
        )
    except PublicEvidenceAdapterProjectionError:
        raise
    except Exception:
        raise _fail() from None


def validate_public_evidence_adapter_projection(
    candidate: object,
) -> PublicEvidenceAdapterProjection:
    """Rebuild one exact projection and reject any authority escalation."""
    if type(candidate) is not PublicEvidenceAdapterProjection:
        raise _fail()
    try:
        rebuilt = build_public_evidence_adapter_projection(
            candidate.record, candidate.inclusion_witnesses
        )
        fixed_pairs = (
            (candidate.contract, rebuilt.contract),
            (candidate.foundation_scope, rebuilt.foundation_scope),
            (candidate.source_release_ref, rebuilt.source_release_ref),
            (
                candidate.source_release_contract_sha256,
                rebuilt.source_release_contract_sha256,
            ),
            (candidate.source_kind, rebuilt.source_kind),
            (
                candidate.planned_adapter_contract_id,
                rebuilt.planned_adapter_contract_id,
            ),
            (candidate.projection_rule_id, rebuilt.projection_rule_id),
            (
                candidate.projection_rule_descriptor_sha256,
                rebuilt.projection_rule_descriptor_sha256,
            ),
            (candidate.inclusion_witnesses, rebuilt.inclusion_witnesses),
            (candidate.source_record_count, rebuilt.source_record_count),
            (
                candidate.source_record_vector_sha256,
                rebuilt.source_record_vector_sha256,
            ),
            (candidate.output_record_count, 1),
            (
                candidate.output_record_vector_sha256,
                rebuilt.output_record_vector_sha256,
            ),
            (candidate.authority_state, rebuilt.authority_state),
        )
        if any(
            type(left) is not type(right) or left != right
            for left, right in fixed_pairs
        ):
            raise _fail()
        _validate_derived_ref(
            candidate.projection_ref,
            PUBLIC_EVIDENCE_ADAPTER_PROJECTION_REF_PREFIX,
            rebuilt.projection_ref,
        )
        if not hmac.compare_digest(
            _strict_sha256(candidate.contract_sha256), rebuilt.contract_sha256
        ):
            raise _fail()
        return rebuilt
    except PublicEvidenceAdapterProjectionError:
        raise
    except Exception:
        raise _fail() from None
