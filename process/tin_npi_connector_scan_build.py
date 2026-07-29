# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded Organization scan accumulation and per-source proof construction."""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from process.tin_npi_connector_evidence import (
    FhirTinNpiEvidence,
    _fhir_organization_identity_bytes,
)
from process.tin_npi_connector_scan import (
    FhirOrganizationScanProof,
    FhirOrganizationScanRecord,
    canonical_fhir_evidence_set_digest,
)
from process.tin_npi_connector_source import TinNpiConnectorSourceVector
from process.tin_npi_connector_support import (
    FHIR_ORGANIZATION_SCAN_TERMINAL_STATES,
    FhirOrganizationEvidenceState,
    TinNpiConnectorError,
)


@dataclass
class _ScanAccumulator:
    """Mutable state for one deterministic pass over selected datasets."""

    source_vector: TinNpiConnectorSourceVector
    selected_policy_ids: set[str]
    dataset_by_key: dict[tuple[str, str, str], Any]
    digest_by_dataset: dict[tuple[str, str, str], Any]
    count_by_dataset: dict[tuple[str, str, str], int]
    state_counts_by_dataset: dict[
        tuple[str, str, str],
        dict[FhirOrganizationEvidenceState, int],
    ]
    evidence_counts_by_dataset_policy: dict[
        tuple[str, str, str],
        dict[str, int],
    ]
    evidence_rows_by_dataset: dict[
        tuple[str, str, str],
        list[FhirTinNpiEvidence],
    ]
    evidence_rows: list[FhirTinNpiEvidence]
    previous_scan_key: tuple[bytes, bytes, bytes, bytes] | None = None


def _new_scan_accumulator(
    source_vector: TinNpiConnectorSourceVector,
) -> _ScanAccumulator:
    """Allocate isolated counters and digests for every selected dataset."""

    dataset_by_key = {
        (dataset.source_id, dataset.endpoint_id, dataset.dataset_id): dataset
        for dataset in source_vector.fhir_datasets
    }
    return _ScanAccumulator(
        source_vector=source_vector,
        selected_policy_ids=set(source_vector.token_policy_ids),
        dataset_by_key=dataset_by_key,
        digest_by_dataset={
            dataset_key: hashlib.sha256() for dataset_key in dataset_by_key
        },
        count_by_dataset=dict.fromkeys(dataset_by_key, 0),
        state_counts_by_dataset={
            dataset_key: dict.fromkeys(FHIR_ORGANIZATION_SCAN_TERMINAL_STATES, 0)
            for dataset_key in dataset_by_key
        },
        evidence_counts_by_dataset_policy={
            dataset_key: dict.fromkeys(source_vector.token_policy_ids, 0)
            for dataset_key in dataset_by_key
        },
        evidence_rows_by_dataset={dataset_key: [] for dataset_key in dataset_by_key},
        evidence_rows=[],
    )


def _scan_record_dataset_key(
    scan_record: FhirOrganizationScanRecord,
) -> tuple[str, str, str]:
    """Return the exact source/endpoint/dataset key for one scan record."""

    return (
        scan_record.source_id,
        scan_record.source_endpoint_id,
        scan_record.source_dataset_id,
    )


def _validate_scan_record_order(
    accumulator: _ScanAccumulator,
    scan_record: FhirOrganizationScanRecord,
) -> None:
    """Require exact class and strictly increasing authenticated scan keys."""

    if type(scan_record) is not FhirOrganizationScanRecord:
        raise TinNpiConnectorError("connector Organization scan record is invalid")
    if (
        accumulator.previous_scan_key is not None
        and scan_record.scan_key <= accumulator.previous_scan_key
    ):
        raise TinNpiConnectorError(
            "connector Organization scan is not strictly ordered"
        )
    accumulator.previous_scan_key = scan_record.scan_key


def _validate_scan_policy_coverage(
    accumulator: _ScanAccumulator,
    scan_record: FhirOrganizationScanRecord,
) -> None:
    """Require every matched record to cover the full selected policy set."""

    record_policy_ids = {
        evidence_row.token.token_policy_id for evidence_row in scan_record.evidence
    }
    if (
        scan_record.state is FhirOrganizationEvidenceState.MATCHED
        and record_policy_ids != accumulator.selected_policy_ids
    ):
        raise TinNpiConnectorError(
            "connector Organization scan does not cover every token policy"
        )
    if (
        scan_record.state is not FhirOrganizationEvidenceState.MATCHED
        and record_policy_ids
    ):
        raise TinNpiConnectorError(
            "connector Organization scan terminal state is inconsistent"
        )


def _validate_evidence_policy(
    accumulator: _ScanAccumulator,
    dataset: Any,
    evidence_row: FhirTinNpiEvidence,
) -> None:
    """Require evidence cutoff, identifier policy, and rule fence equality."""

    source_vector = accumulator.source_vector
    is_mismatched = (
        evidence_row.identifier_policy_id != source_vector.identifier_policy.policy_id
        or evidence_row.identifier_policy_sha256
        != source_vector.identifier_policy.descriptor_sha256
        or evidence_row.identifier_rule_id != dataset.identifier_rule_id
        or evidence_row.identifier_rule_sha256 != dataset.identifier_rule_sha256
        or evidence_row.evidence_as_of != source_vector.evidence_as_of
    )
    if is_mismatched:
        raise TinNpiConnectorError(
            "connector Organization scan identifier policy mismatch"
        )


def _append_scan_evidence(
    accumulator: _ScanAccumulator,
    scan_record: FhirOrganizationScanRecord,
    dataset_key: tuple[str, str, str],
    dataset: Any,
) -> None:
    """Validate and append every evidence row for one scan record."""

    for evidence_row in scan_record.evidence:
        _validate_evidence_policy(accumulator, dataset, evidence_row)
        policy_id = evidence_row.token.token_policy_id
        accumulator.evidence_counts_by_dataset_policy[dataset_key][policy_id] += 1
        accumulator.evidence_rows.append(evidence_row)
        accumulator.evidence_rows_by_dataset[dataset_key].append(evidence_row)


def _consume_scan_record(
    accumulator: _ScanAccumulator,
    scan_record: FhirOrganizationScanRecord,
) -> None:
    """Consume one ordered record into completeness and evidence state."""

    _validate_scan_record_order(accumulator, scan_record)
    dataset_key = _scan_record_dataset_key(scan_record)
    dataset = accumulator.dataset_by_key.get(dataset_key)
    if dataset is None:
        raise TinNpiConnectorError(
            "connector Organization scan is outside its source vector"
        )
    observed_count = accumulator.count_by_dataset[dataset_key]
    if observed_count:
        accumulator.digest_by_dataset[dataset_key].update(b"\n")
    accumulator.digest_by_dataset[dataset_key].update(
        _fhir_organization_identity_bytes(
            scan_record.resource_id,
            scan_record.payload_hash,
        )
    )
    accumulator.count_by_dataset[dataset_key] = observed_count + 1
    accumulator.state_counts_by_dataset[dataset_key][scan_record.state] += 1
    _validate_scan_policy_coverage(accumulator, scan_record)
    _append_scan_evidence(accumulator, scan_record, dataset_key, dataset)


def _consume_all_scan_records(
    scan_records: Iterable[FhirOrganizationScanRecord],
    accumulator: _ScanAccumulator,
) -> None:
    """Consume the scan iterator and normalize iteration failures."""

    try:
        for scan_record in scan_records:
            _consume_scan_record(accumulator, scan_record)
    except TypeError:
        raise TinNpiConnectorError("connector Organization scan is invalid") from None


def _build_dataset_scan_proof(
    accumulator: _ScanAccumulator,
    dataset_key: tuple[str, str, str],
) -> FhirOrganizationScanProof:
    """Build one source proof after validating count and identity digest."""

    dataset = accumulator.dataset_by_key[dataset_key]
    observed_count = accumulator.count_by_dataset[dataset_key]
    observed_digest = accumulator.digest_by_dataset[dataset_key].hexdigest()
    if (
        observed_count != dataset.organization_resource_count
        or not hmac.compare_digest(
            observed_digest,
            dataset.organization_resource_sha256,
        )
    ):
        raise TinNpiConnectorError(
            "connector Organization scan completeness proof mismatch"
        )
    return FhirOrganizationScanProof(
        source_id=dataset.source_id,
        endpoint_id=dataset.endpoint_id,
        dataset_id=dataset.dataset_id,
        source_summary_sha256=dataset.source_summary_sha256,
        identifier_rule_id=dataset.identifier_rule_id,
        identifier_rule_sha256=dataset.identifier_rule_sha256,
        organization_resource_count=observed_count,
        organization_resource_sha256=observed_digest,
        state_counts=tuple(
            (
                state.value,
                accumulator.state_counts_by_dataset[dataset_key][state],
            )
            for state in sorted(
                FHIR_ORGANIZATION_SCAN_TERMINAL_STATES,
                key=lambda terminal_state: terminal_state.value,
            )
        ),
        matched_evidence_counts=tuple(
            (
                policy_id,
                accumulator.evidence_counts_by_dataset_policy[dataset_key][policy_id],
            )
            for policy_id in sorted(accumulator.source_vector.token_policy_ids)
        ),
        matched_evidence_sha256=canonical_fhir_evidence_set_digest(
            accumulator.evidence_rows_by_dataset[dataset_key]
        ).hex(),
    )


def scan_proofs_and_evidence(
    scan_records: Iterable[FhirOrganizationScanRecord],
    *,
    source_vector: TinNpiConnectorSourceVector,
) -> tuple[
    tuple[FhirOrganizationScanProof, ...],
    tuple[FhirTinNpiEvidence, ...],
]:
    """Consume every selected Organization and return sealed source proofs."""

    if isinstance(scan_records, (str, bytes, bytearray)):
        raise TinNpiConnectorError("connector Organization scan is invalid")
    accumulator = _new_scan_accumulator(source_vector)
    _consume_all_scan_records(scan_records, accumulator)
    scan_proofs = tuple(
        _build_dataset_scan_proof(accumulator, dataset_key)
        for dataset_key in sorted(accumulator.dataset_by_key)
    )
    return scan_proofs, tuple(accumulator.evidence_rows)
