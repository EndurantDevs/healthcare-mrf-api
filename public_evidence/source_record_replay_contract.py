# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Validation and construction for retained Organization replay results."""

from __future__ import annotations

import hmac
from dataclasses import fields, is_dataclass
from typing import Mapping, NamedTuple

from public_evidence.source_record_inclusion_contract import (
    validate_source_record_inventory_descriptor,
)
from public_evidence.source_record_inclusion_primitives import (
    PublicEvidenceSourceRecordInclusionError,
    PublicEvidenceSourceRecordInventoryDescriptor,
)
from public_evidence.source_record_replay_primitives import (
    CONNECTOR_FHIR_ORGANIZATION_SCAN_CONTRACT_ID,
    FHIR_ORGANIZATION_PAYLOAD_CANONICALIZATION_CONTRACT_ID,
    FHIR_ORGANIZATION_RECORD_IDENTITY_CONTRACT_ID,
    FHIR_ORGANIZATION_RECORD_IDENTITY_DESCRIPTOR_SHA256,
    FHIR_ORGANIZATION_RETAINED_ROW_REPLAY_POLICY_DESCRIPTOR_SHA256,
    FHIR_ORGANIZATION_RETAINED_ROW_REPLAY_POLICY_ID,
    PUBLIC_EVIDENCE_FHIR_ORGANIZATION_RETAINED_ROW_REPLAY_CONTRACT,
    PublicEvidenceFhirOrganizationReplayResult,
    PublicEvidenceFhirOrganizationReplayError,
    RecordKindReplayAuthorityState,
    canonical_replay_sha256,
    connector_token_policy_descriptor_sha256,
    derived_replay_ref,
    fixed_replay_authority,
    replay_validation_error,
    strict_replay_sha256,
    strict_replay_token_policy_id,
)
from public_evidence.source_release_contract import (
    PUBLIC_EVIDENCE_FOUNDATION_SCOPE,
    PublicEvidenceSourceReleaseDescriptor,
    PublicEvidenceSourceReleaseError,
    validate_public_evidence_source_release,
)

_EXECUTION_SEAL = object()


class _VerifiedReplayProof(NamedTuple):
    source_vector_sha256: str
    dataset_fence_sha256: str
    token_policy_id: str
    token_policy_descriptor_sha256: str
    source_record_vector_sha256: str
    scan_proof_sha256: str


def _verified_replay_proof(
    proof: _VerifiedReplayProof,
) -> _VerifiedReplayProof:
    if type(proof) is not _VerifiedReplayProof:
        raise replay_validation_error()
    policy_id = strict_replay_token_policy_id(proof.token_policy_id)
    policy_descriptor = strict_replay_sha256(proof.token_policy_descriptor_sha256)
    if not hmac.compare_digest(
        policy_descriptor,
        connector_token_policy_descriptor_sha256(policy_id),
    ):
        raise replay_validation_error()
    return _VerifiedReplayProof(
        source_vector_sha256=strict_replay_sha256(proof.source_vector_sha256),
        dataset_fence_sha256=strict_replay_sha256(proof.dataset_fence_sha256),
        token_policy_id=policy_id,
        token_policy_descriptor_sha256=policy_descriptor,
        source_record_vector_sha256=strict_replay_sha256(
            proof.source_record_vector_sha256
        ),
        scan_proof_sha256=strict_replay_sha256(proof.scan_proof_sha256),
    )


def _validated_release_and_inventory(
    release: object,
    inventory: object,
) -> tuple[
    PublicEvidenceSourceReleaseDescriptor,
    PublicEvidenceSourceRecordInventoryDescriptor,
]:
    try:
        fixed_release = validate_public_evidence_source_release(release)
        fixed_inventory = validate_source_record_inventory_descriptor(inventory)
    except (PublicEvidenceSourceReleaseError, PublicEvidenceSourceRecordInclusionError):
        raise replay_validation_error() from None
    is_wrong_scope = (
        fixed_release.source_kind != "public_provider_directory_fhir"
        or fixed_inventory.release != fixed_release
        or fixed_inventory.source_kind != fixed_release.source_kind
        or fixed_inventory.record_kind != "fhir_organization"
        or fixed_inventory.record_identity_contract_id
        != FHIR_ORGANIZATION_RECORD_IDENTITY_CONTRACT_ID
        or fixed_inventory.payload_canonicalization_contract_id
        != FHIR_ORGANIZATION_PAYLOAD_CANONICALIZATION_CONTRACT_ID
    )
    if is_wrong_scope:
        raise replay_validation_error()
    return fixed_release, fixed_inventory


def _result_payload(
    release: PublicEvidenceSourceReleaseDescriptor,
    inventory: PublicEvidenceSourceRecordInventoryDescriptor,
    proof: _VerifiedReplayProof,
    authority: RecordKindReplayAuthorityState,
) -> dict[str, object]:
    return {
        "contract": PUBLIC_EVIDENCE_FHIR_ORGANIZATION_RETAINED_ROW_REPLAY_CONTRACT,
        "foundation_scope": PUBLIC_EVIDENCE_FOUNDATION_SCOPE,
        "replay_policy_id": FHIR_ORGANIZATION_RETAINED_ROW_REPLAY_POLICY_ID,
        "replay_policy_descriptor_sha256": (
            FHIR_ORGANIZATION_RETAINED_ROW_REPLAY_POLICY_DESCRIPTOR_SHA256
        ),
        "source_release_ref": release.source_release_ref,
        "source_release_contract_sha256": release.contract_sha256,
        "inventory_ref": inventory.inventory_ref,
        "inventory_contract_sha256": inventory.contract_sha256,
        **proof._asdict(),
        "record_kind": inventory.record_kind,
        "record_identity_contract_id": inventory.record_identity_contract_id,
        "record_identity_descriptor_sha256": (
            FHIR_ORGANIZATION_RECORD_IDENTITY_DESCRIPTOR_SHA256
        ),
        "payload_canonicalization_contract_id": (
            inventory.payload_canonicalization_contract_id
        ),
        "member_count": inventory.member_count,
        "member_root_sha256": inventory.member_root_sha256,
        "scan_contract_id": CONNECTOR_FHIR_ORGANIZATION_SCAN_CONTRACT_ID,
        "authority_state": authority._asdict(),
    }


def _result_from_components(
    release: PublicEvidenceSourceReleaseDescriptor,
    inventory: PublicEvidenceSourceRecordInventoryDescriptor,
    proof: _VerifiedReplayProof,
    authority: RecordKindReplayAuthorityState,
    result_payload: Mapping[str, object],
) -> PublicEvidenceFhirOrganizationReplayResult:
    return PublicEvidenceFhirOrganizationReplayResult(
        contract=PUBLIC_EVIDENCE_FHIR_ORGANIZATION_RETAINED_ROW_REPLAY_CONTRACT,
        foundation_scope=PUBLIC_EVIDENCE_FOUNDATION_SCOPE,
        replay_policy_id=FHIR_ORGANIZATION_RETAINED_ROW_REPLAY_POLICY_ID,
        replay_policy_descriptor_sha256=(
            FHIR_ORGANIZATION_RETAINED_ROW_REPLAY_POLICY_DESCRIPTOR_SHA256
        ),
        release=release,
        inventory=inventory,
        source_vector_sha256=proof.source_vector_sha256,
        dataset_fence_sha256=proof.dataset_fence_sha256,
        token_policy_id=proof.token_policy_id,
        token_policy_descriptor_sha256=proof.token_policy_descriptor_sha256,
        record_kind=inventory.record_kind,
        record_identity_contract_id=inventory.record_identity_contract_id,
        record_identity_descriptor_sha256=(
            FHIR_ORGANIZATION_RECORD_IDENTITY_DESCRIPTOR_SHA256
        ),
        payload_canonicalization_contract_id=(
            inventory.payload_canonicalization_contract_id
        ),
        member_count=inventory.member_count,
        member_root_sha256=inventory.member_root_sha256,
        source_record_vector_sha256=proof.source_record_vector_sha256,
        scan_contract_id=CONNECTOR_FHIR_ORGANIZATION_SCAN_CONTRACT_ID,
        scan_proof_sha256=proof.scan_proof_sha256,
        replay_ref=derived_replay_ref("retained_row_replay", result_payload),
        contract_sha256=canonical_replay_sha256("retained_row_replay", result_payload),
        authority_state=authority,
    )


def _build_fhir_organization_replay_result(
    *,
    release: PublicEvidenceSourceReleaseDescriptor,
    inventory: PublicEvidenceSourceRecordInventoryDescriptor,
    proof: _VerifiedReplayProof,
    execution_seal: object,
) -> PublicEvidenceFhirOrganizationReplayResult:
    """Build the immutable result after the executor checks supplied inputs."""

    if execution_seal is not _EXECUTION_SEAL:
        raise replay_validation_error()
    fixed_release, fixed_inventory = _validated_release_and_inventory(
        release, inventory
    )
    fixed_proof = _verified_replay_proof(proof)
    authority = fixed_replay_authority()
    result_payload = _result_payload(
        fixed_release, fixed_inventory, fixed_proof, authority
    )
    return _result_from_components(
        fixed_release,
        fixed_inventory,
        fixed_proof,
        authority,
        result_payload,
    )


def _is_rebuilt_result_match(
    candidate: PublicEvidenceFhirOrganizationReplayResult,
    rebuilt: PublicEvidenceFhirOrganizationReplayResult,
) -> bool:
    fixed_candidate = candidate._replace(
        replay_ref=rebuilt.replay_ref,
        contract_sha256=rebuilt.contract_sha256,
    )
    return _is_strict_result_value_match(fixed_candidate, rebuilt)


def _is_strict_result_value_match(candidate: object, expected: object) -> bool:
    """Compare the closed result tree without trusting custom equality hooks."""

    if type(candidate) is not type(expected):
        return False
    if isinstance(expected, tuple):
        return len(candidate) == len(expected) and all(
            _is_strict_result_value_match(candidate_value, expected_value)
            for candidate_value, expected_value in zip(candidate, expected, strict=True)
        )
    if is_dataclass(expected):
        return all(
            _is_strict_result_value_match(
                getattr(candidate, field.name), getattr(expected, field.name)
            )
            for field in fields(expected)
        )
    return bool(candidate == expected)


def _validated_fhir_organization_replay_result_shape(
    candidate: object,
) -> PublicEvidenceFhirOrganizationReplayResult:
    """Rebuild one untrusted result shape without claiming replay execution."""

    if type(candidate) is not PublicEvidenceFhirOrganizationReplayResult:
        raise replay_validation_error()
    try:
        proof = _VerifiedReplayProof(
            candidate.source_vector_sha256,
            candidate.dataset_fence_sha256,
            candidate.token_policy_id,
            candidate.token_policy_descriptor_sha256,
            candidate.source_record_vector_sha256,
            candidate.scan_proof_sha256,
        )
        rebuilt = _build_fhir_organization_replay_result(
            release=candidate.release,
            inventory=candidate.inventory,
            proof=proof,
            execution_seal=_EXECUTION_SEAL,
        )
        if not _is_rebuilt_result_match(candidate, rebuilt):
            raise replay_validation_error()
        if not hmac.compare_digest(candidate.replay_ref, rebuilt.replay_ref):
            raise replay_validation_error()
        if not hmac.compare_digest(
            strict_replay_sha256(candidate.contract_sha256),
            rebuilt.contract_sha256,
        ):
            raise replay_validation_error()
        return rebuilt
    except PublicEvidenceFhirOrganizationReplayError:
        raise
    except Exception:
        raise replay_validation_error() from None
