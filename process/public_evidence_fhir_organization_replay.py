# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed retained-row replay for one FHIR Organization inventory."""

from __future__ import annotations

import hmac
import math
from collections.abc import Iterable, Mapping
from typing import Any, NamedTuple, cast

from public_evidence.evidence_record_primitives import (
    EvidenceSourceRecordReference,
    build_evidence_source_record_reference,
)
from public_evidence.source_record_inclusion_contract import (
    derive_inventory_leaf_sha256,
)
from public_evidence.source_record_inclusion_primitives import (
    PublicEvidenceSourceRecordInventoryDescriptor,
    derive_inventory_node_sha256,
)
from public_evidence.source_record_replay_contract import (
    _EXECUTION_SEAL,
    _VerifiedReplayProof,
    _build_fhir_organization_replay_result,
    _validated_fhir_organization_replay_result_shape,
    _validated_release_and_inventory,
)
from public_evidence.source_record_replay_primitives import (
    FHIR_ORGANIZATION_RECORD_IDENTITY_CONTRACT_ID,
    PublicEvidenceFhirOrganizationReplayResult,
    PublicEvidenceFhirOrganizationReplayError,
    canonical_replay_binding_sha256,
    canonical_source_record_vector_sha256,
    replay_validation_error,
)
from public_evidence.source_release_contract import (
    PublicEvidenceSourceReleaseDescriptor,
)
from process.tin_npi_connector_adapters import (
    extract_normalized_organization_evidence_for_policies,
)
from process.tin_npi_connector_evidence import (
    canonical_provider_directory_payload_hash,
)
from process.tin_npi_connector_scan import (
    FhirOrganizationScanRecord,
    canonical_fhir_organization_scan_proof_digest,
)
from process.tin_npi_connector_scan_build import scan_proofs_and_evidence
from process.tin_npi_connector_security import (
    TinTokenProjector,
    validate_tin_hmac_token_policy,
)
from process.tin_npi_connector_source import (
    FhirDatasetFenceIdentity,
    TinNpiConnectorSourceVector,
)
from process.tin_npi_connector_source_validation import (
    validate_connector_source_vector,
)
from process.tin_npi_connector_support import (
    FhirOrganizationEvidenceState,
    strict_evidence_text,
)


class _ReplayExecutionInputs(NamedTuple):
    """Validated immutable inputs kept private to the executor."""

    release: PublicEvidenceSourceReleaseDescriptor
    inventory: PublicEvidenceSourceRecordInventoryDescriptor
    source_vector: TinNpiConnectorSourceVector
    dataset: FhirDatasetFenceIdentity
    token_projectors: tuple[TinTokenProjector, ...]
    record_projector: TinTokenProjector


_RETAINED_ROW_FIELDS = frozenset(
    {"resource_type", "resource_id", "payload_hash", "payload_json"}
)
_MAX_JSON_DEPTH = 64


def _detached_json_value(candidate: object, depth: int = 0) -> object:
    """Copy an exact built-in JSON tree before any contract-dependent reads."""

    if depth > _MAX_JSON_DEPTH:
        raise replay_validation_error()
    if type(candidate) is dict:
        if any(type(key) is not str for key in candidate):
            raise replay_validation_error()
        return {
            key: _detached_json_value(child, depth + 1)
            for key, child in candidate.items()
        }
    if type(candidate) is list:
        return [_detached_json_value(child, depth + 1) for child in candidate]
    if candidate is None or type(candidate) in (str, bool, int):
        return candidate
    if type(candidate) is float and math.isfinite(candidate):
        return candidate
    raise replay_validation_error()


def _detached_retained_row(candidate: object) -> dict[str, Any]:
    """Return one closed retained-row snapshot with no custom mapping hooks."""

    if type(candidate) is not dict:
        raise replay_validation_error()
    candidate_keys = tuple(candidate)
    if (
        len(candidate_keys) != len(_RETAINED_ROW_FIELDS)
        or any(type(key) is not str for key in candidate_keys)
        or frozenset(candidate_keys) != _RETAINED_ROW_FIELDS
    ):
        raise replay_validation_error()
    return cast(dict[str, Any], _detached_json_value(candidate))


def _validated_projectors(
    source_vector: TinNpiConnectorSourceVector,
    token_projectors: object,
    record_identity_token_policy_id: object,
) -> tuple[tuple[TinTokenProjector, ...], TinTokenProjector]:
    if type(token_projectors) is not tuple or not token_projectors:
        raise replay_validation_error()
    fixed_projectors = tuple(
        validate_tin_hmac_token_policy(projector) for projector in token_projectors
    )
    projector_ids = tuple(projector.token_policy_id for projector in fixed_projectors)
    expected_ids = tuple(sorted(source_vector.token_policy_ids))
    if (
        projector_ids != expected_ids
        or type(record_identity_token_policy_id) is not str
    ):
        raise replay_validation_error()
    projector_by_id = {
        projector.token_policy_id: projector for projector in fixed_projectors
    }
    record_projector = projector_by_id.get(record_identity_token_policy_id)
    if record_projector is None:
        raise replay_validation_error()
    return fixed_projectors, record_projector


def _validated_execution_inputs(
    release: object,
    inventory: object,
    source_vector: object,
    token_projectors: object,
    record_identity_token_policy_id: object,
) -> _ReplayExecutionInputs:
    fixed_release, fixed_inventory = _validated_release_and_inventory(
        release, inventory
    )
    if type(source_vector) is not TinNpiConnectorSourceVector:
        raise replay_validation_error()
    validate_connector_source_vector(source_vector)
    if len(source_vector.fhir_datasets) != 1:
        raise replay_validation_error()
    dataset = source_vector.fhir_datasets[0]
    if (
        dataset.organization_resource_count < 1
        or fixed_inventory.member_count != dataset.organization_resource_count
    ):
        raise replay_validation_error()
    fixed_projectors, record_projector = _validated_projectors(
        source_vector,
        token_projectors,
        record_identity_token_policy_id,
    )
    return _ReplayExecutionInputs(
        release=fixed_release,
        inventory=fixed_inventory,
        source_vector=source_vector,
        dataset=dataset,
        token_projectors=fixed_projectors,
        record_projector=record_projector,
    )


def _selected_record_hmac(
    inputs: _ReplayExecutionInputs,
    resource_id: str,
) -> bytes:
    record_hmac = inputs.record_projector.pseudonymize_source_record(
        source_id=inputs.dataset.source_id,
        source_endpoint_id=inputs.dataset.endpoint_id,
        source_dataset_id=inputs.dataset.dataset_id,
        resource_id=resource_id,
    )
    if type(record_hmac) is not bytes or len(record_hmac) != 32:
        raise replay_validation_error()
    return record_hmac


def _validate_matched_record_hmac(
    extraction: Any,
    record_hmac: bytes,
    record_policy_id: str,
) -> None:
    if extraction.state is not FhirOrganizationEvidenceState.MATCHED:
        return
    selected_evidence_rows = tuple(
        evidence
        for evidence in extraction.evidence
        if evidence.token.token_policy_id == record_policy_id
    )
    if not selected_evidence_rows or any(
        not hmac.compare_digest(evidence.source_record_hmac_sha256, record_hmac)
        for evidence in selected_evidence_rows
    ):
        raise replay_validation_error()


def _replay_retained_row(
    inputs: _ReplayExecutionInputs,
    retained_row: object,
) -> tuple[EvidenceSourceRecordReference, FhirOrganizationScanRecord]:
    retained_row = _detached_retained_row(retained_row)
    resource_id = strict_evidence_text(
        retained_row.get("resource_id"),
        "FHIR Organization resource ID",
        limit=256,
    )
    retained_payload = retained_row.get("payload_json")
    if not isinstance(retained_payload, Mapping):
        raise replay_validation_error()
    payload_sha256 = canonical_provider_directory_payload_hash(retained_payload)
    supplied_payload_sha256 = retained_row.get("payload_hash")
    if type(supplied_payload_sha256) is not str or not hmac.compare_digest(
        supplied_payload_sha256, payload_sha256
    ):
        raise replay_validation_error()
    extraction = extract_normalized_organization_evidence_for_policies(
        retained_row,
        source_id=inputs.dataset.source_id,
        source_endpoint_id=inputs.dataset.endpoint_id,
        source_dataset_id=inputs.dataset.dataset_id,
        token_projectors=inputs.token_projectors,
        evidence_as_of=inputs.source_vector.evidence_as_of,
        identifier_policy=inputs.source_vector.identifier_policy,
    )
    if extraction.state is FhirOrganizationEvidenceState.NOT_ORGANIZATION:
        raise replay_validation_error()
    record_hmac = _selected_record_hmac(inputs, resource_id)
    _validate_matched_record_hmac(
        extraction,
        record_hmac,
        inputs.record_projector.token_policy_id,
    )
    source_record = build_evidence_source_record_reference(
        inputs.release,
        {
            "record_kind": "fhir_organization",
            "identity_contract_id": FHIR_ORGANIZATION_RECORD_IDENTITY_CONTRACT_ID,
            "record_hmac_sha256": record_hmac.hex(),
            "payload_sha256": payload_sha256,
        },
    )
    scan_record = FhirOrganizationScanRecord(
        source_id=inputs.dataset.source_id,
        source_endpoint_id=inputs.dataset.endpoint_id,
        source_dataset_id=inputs.dataset.dataset_id,
        resource_id=resource_id,
        payload_hash=payload_sha256,
        state=extraction.state,
        evidence=extraction.evidence,
    )
    return source_record, scan_record


def _consume_retained_rows(
    inputs: _ReplayExecutionInputs,
    retained_rows: object,
) -> tuple[
    tuple[EvidenceSourceRecordReference, ...],
    tuple[FhirOrganizationScanRecord, ...],
]:
    if isinstance(retained_rows, (str, bytes, bytearray, Mapping)):
        raise replay_validation_error()
    source_records: list[EvidenceSourceRecordReference] = []
    scan_records: list[FhirOrganizationScanRecord] = []
    try:
        retained_row_iterator = iter(retained_rows)
    except TypeError:
        raise replay_validation_error() from None
    for retained_row in retained_row_iterator:
        if len(source_records) >= inputs.inventory.member_count:
            raise replay_validation_error()
        source_record, scan_record = _replay_retained_row(inputs, retained_row)
        source_records.append(source_record)
        scan_records.append(scan_record)
    if len(source_records) != inputs.inventory.member_count:
        raise replay_validation_error()
    return tuple(source_records), tuple(scan_records)


def _merkle_root(
    leaf_sha256s: tuple[str, ...],
    start: int,
    stop: int,
) -> str:
    count = stop - start
    if count == 1:
        return leaf_sha256s[start]
    split = 1 << ((count - 1).bit_length() - 1)
    return derive_inventory_node_sha256(
        _merkle_root(leaf_sha256s, start, start + split),
        _merkle_root(leaf_sha256s, start + split, stop),
    )


def _verify_inventory_closure(
    inputs: _ReplayExecutionInputs,
    source_records: tuple[EvidenceSourceRecordReference, ...],
) -> str:
    ordered_records = tuple(
        sorted(source_records, key=lambda record: record.source_record_ref)
    )
    ordered_refs = tuple(record.source_record_ref for record in ordered_records)
    if len(set(ordered_refs)) != len(ordered_refs):
        raise replay_validation_error()
    namespace_by_field = {
        "record_kind": inputs.inventory.record_kind,
        "record_identity_contract_id": inputs.inventory.record_identity_contract_id,
        "payload_canonicalization_contract_id": (
            inputs.inventory.payload_canonicalization_contract_id
        ),
        "member_count": inputs.inventory.member_count,
    }
    leaves = tuple(
        derive_inventory_leaf_sha256(
            inputs.release, namespace_by_field, record, ordinal
        )
        for ordinal, record in enumerate(ordered_records)
    )
    observed_root = _merkle_root(leaves, 0, len(leaves))
    if not hmac.compare_digest(observed_root, inputs.inventory.member_root_sha256):
        raise replay_validation_error()
    return canonical_source_record_vector_sha256(ordered_refs)


def _execute_replay(
    inputs: _ReplayExecutionInputs,
    retained_rows: object,
) -> PublicEvidenceFhirOrganizationReplayResult:
    source_records, scan_records = _consume_retained_rows(inputs, retained_rows)
    record_vector_sha256 = _verify_inventory_closure(inputs, source_records)
    scan_proofs, _evidence = scan_proofs_and_evidence(
        scan_records,
        source_vector=inputs.source_vector,
    )
    if len(scan_proofs) != 1:
        raise replay_validation_error()
    descriptor_by_id = {
        descriptor.token_policy_id: descriptor
        for descriptor in inputs.source_vector.token_policies
    }
    selected_descriptor = descriptor_by_id[inputs.record_projector.token_policy_id]
    proof = _VerifiedReplayProof(
        source_vector_sha256=inputs.source_vector.source_vector_id,
        dataset_fence_sha256=canonical_replay_binding_sha256(
            "fhir_dataset_fence", inputs.dataset.public_payload()
        ),
        token_policy_id=selected_descriptor.token_policy_id,
        token_policy_descriptor_sha256=(
            selected_descriptor.token_policy_descriptor_sha256
        ),
        source_record_vector_sha256=record_vector_sha256,
        scan_proof_sha256=canonical_fhir_organization_scan_proof_digest(
            scan_proofs
        ).hex(),
    )
    return _build_fhir_organization_replay_result(
        release=inputs.release,
        inventory=inputs.inventory,
        proof=proof,
        execution_seal=_EXECUTION_SEAL,
    )


def replay_fhir_organization_retained_rows(
    *,
    release: PublicEvidenceSourceReleaseDescriptor,
    inventory: PublicEvidenceSourceRecordInventoryDescriptor,
    source_vector: TinNpiConnectorSourceVector,
    retained_rows: Iterable[Mapping[str, Any]],
    token_projectors: tuple[TinTokenProjector, ...],
    record_identity_token_policy_id: str,
) -> PublicEvidenceFhirOrganizationReplayResult:
    """Check one supplied retained Organization vector without source authority."""

    try:
        inputs = _validated_execution_inputs(
            release,
            inventory,
            source_vector,
            token_projectors,
            record_identity_token_policy_id,
        )
        replay_result = _execute_replay(inputs, retained_rows)
    except Exception:
        normalized_error = replay_validation_error()
    else:
        return replay_result
    raise normalized_error


def verify_fhir_organization_replay_result(
    candidate: object,
    *,
    release: PublicEvidenceSourceReleaseDescriptor,
    inventory: PublicEvidenceSourceRecordInventoryDescriptor,
    source_vector: TinNpiConnectorSourceVector,
    retained_rows: Iterable[Mapping[str, Any]],
    token_projectors: tuple[TinTokenProjector, ...],
    record_identity_token_policy_id: str,
) -> PublicEvidenceFhirOrganizationReplayResult:
    """Rerun every supplied row and compare one untrusted result exactly."""

    try:
        fixed_candidate = _validated_fhir_organization_replay_result_shape(candidate)
        expected = replay_fhir_organization_retained_rows(
            release=release,
            inventory=inventory,
            source_vector=source_vector,
            retained_rows=retained_rows,
            token_projectors=token_projectors,
            record_identity_token_policy_id=record_identity_token_policy_id,
        )
        if fixed_candidate != expected:
            raise replay_validation_error()
    except Exception:
        normalized_error = replay_validation_error()
    else:
        return expected
    raise normalized_error
