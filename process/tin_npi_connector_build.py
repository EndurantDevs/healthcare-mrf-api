# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Deterministic evidence validation and compact generation construction."""

from __future__ import annotations

from collections.abc import Iterable, Sequence

from process.tin_npi_connector_evidence import FhirTinNpiEvidence
from process.tin_npi_connector_generation import CompactTinNpiGeneration
from process.tin_npi_connector_lookup import (
    NpiTinLookupReference,
    NpiTinLookupRow,
    TinNpiLookupRow,
    _factor_forward_rows,
    _generation_id,
    _lookup_digest,
)
from process.tin_npi_connector_scan import (
    FhirOrganizationScanProof,
    FhirOrganizationScanRecord,
    canonical_fhir_organization_scan_proof_digest,
)
from process.tin_npi_connector_scan_build import scan_proofs_and_evidence
from process.tin_npi_connector_source import (
    TinNpiConnectorSourceVector,
    _canonical_source_ids,
    canonical_source_ordinal_map_digest,
)
from process.tin_npi_connector_support import TinNpiConnectorError


def _evidence_dataset_key(
    evidence_row: FhirTinNpiEvidence,
) -> tuple[str, str, str]:
    """Return the source/endpoint/dataset scope of one evidence row."""

    return (
        evidence_row.source_id,
        evidence_row.source_endpoint_id,
        evidence_row.source_dataset_id,
    )


def _validate_evidence_source_scope(
    evidence_row: FhirTinNpiEvidence,
    *,
    source_vector: TinNpiConnectorSourceVector,
    selected_dataset_by_key: dict[tuple[str, str, str], object],
    selected_policy_ids: set[str],
) -> None:
    """Require one evidence row to match every selected source-vector fence."""

    if type(evidence_row) is not FhirTinNpiEvidence:
        raise TinNpiConnectorError("connector evidence row is invalid")
    dataset_key = _evidence_dataset_key(evidence_row)
    is_outside_vector = (
        dataset_key not in selected_dataset_by_key
        or evidence_row.token.token_policy_id not in selected_policy_ids
        or evidence_row.identifier_policy_id
        != source_vector.identifier_policy.policy_id
        or evidence_row.identifier_policy_sha256
        != source_vector.identifier_policy.descriptor_sha256
        or evidence_row.evidence_as_of != source_vector.evidence_as_of
    )
    if is_outside_vector:
        raise TinNpiConnectorError("connector evidence is outside its source vector")
    selected_dataset = selected_dataset_by_key[dataset_key]
    if (
        evidence_row.identifier_rule_id != selected_dataset.identifier_rule_id
        or evidence_row.identifier_rule_sha256
        != selected_dataset.identifier_rule_sha256
    ):
        raise TinNpiConnectorError(
            "connector evidence identifier rule is outside its source vector"
        )


def _deduplicate_evidence_rows(
    evidence_rows: Sequence[FhirTinNpiEvidence],
    *,
    source_vector: TinNpiConnectorSourceVector,
    scan_proofs: tuple[FhirOrganizationScanProof, ...],
) -> tuple[FhirTinNpiEvidence, ...]:
    """Validate scope and return evidence in unique binary-ID order."""

    selected_dataset_by_key = {
        (dataset.source_id, dataset.endpoint_id, dataset.dataset_id): dataset
        for dataset in source_vector.fhir_datasets
    }
    selected_policy_ids = set(source_vector.token_policy_ids)
    unique_evidence_by_id: dict[bytes, FhirTinNpiEvidence] = {}
    for evidence_row in evidence_rows:
        _validate_evidence_source_scope(
            evidence_row,
            source_vector=source_vector,
            selected_dataset_by_key=selected_dataset_by_key,
            selected_policy_ids=selected_policy_ids,
        )
        incumbent_evidence = unique_evidence_by_id.setdefault(
            evidence_row.evidence_id,
            evidence_row,
        )
        if incumbent_evidence != evidence_row:
            raise TinNpiConnectorError("connector evidence identity collision")
    represented_policy_ids = {
        evidence_row.token.token_policy_id
        for evidence_row in unique_evidence_by_id.values()
    }
    if unique_evidence_by_id and represented_policy_ids != selected_policy_ids:
        raise TinNpiConnectorError(
            "connector evidence does not cover every token policy"
        )
    expected_evidence_count = sum(proof.matched_evidence_count for proof in scan_proofs)
    if len(unique_evidence_by_id) != expected_evidence_count:
        raise TinNpiConnectorError(
            "connector Organization scan evidence identity collision"
        )
    return tuple(
        unique_evidence_by_id[evidence_id]
        for evidence_id in sorted(unique_evidence_by_id)
    )


def _reverse_rows_from_forward(
    forward_rows: Sequence[TinNpiLookupRow],
) -> tuple[NpiTinLookupRow, ...]:
    """Factor sorted reverse NPI rows from already validated forward rows."""

    references_by_npi: dict[int, list[NpiTinLookupReference]] = {}
    for forward_row in forward_rows:
        for npi in forward_row.npis:
            references_by_npi.setdefault(npi, []).append(
                NpiTinLookupReference(
                    token=forward_row.token,
                    relationship_class=forward_row.relationship_class,
                )
            )
    return tuple(
        NpiTinLookupRow(
            npi=npi,
            tax_identities=tuple(
                sorted(
                    references_by_npi[npi],
                    key=lambda reference: (
                        reference.token.token_policy_id,
                        reference.token.tin_hmac_sha256,
                        reference.relationship_class,
                    ),
                )
            ),
        )
        for npi in sorted(references_by_npi)
    )


def _assemble_generation(
    *,
    source_vector: TinNpiConnectorSourceVector,
    scan_proofs: tuple[FhirOrganizationScanProof, ...],
    evidence_rows: tuple[FhirTinNpiEvidence, ...],
) -> CompactTinNpiGeneration:
    """Factor lookups and seal every digest into one immutable generation."""

    source_ordinal_map = _canonical_source_ids(
        dataset.source_id for dataset in source_vector.fhir_datasets
    )
    forward_rows = _factor_forward_rows(
        evidence_rows,
        source_ordinal_map=source_ordinal_map,
    )
    lookup_digest = _lookup_digest(forward_rows)
    scan_proof_digest = canonical_fhir_organization_scan_proof_digest(scan_proofs)
    source_vector_id = source_vector.source_vector_id
    return CompactTinNpiGeneration(
        generation_id=_generation_id(
            source_vector_id=source_vector_id,
            scan_proof_digest=scan_proof_digest,
            lookup_digest=lookup_digest,
        ),
        source_vector_id=source_vector_id,
        source_ordinal_map=source_ordinal_map,
        source_ordinal_map_digest=canonical_source_ordinal_map_digest(
            source_ordinal_map
        ),
        scan_proofs=scan_proofs,
        scan_proof_digest=scan_proof_digest,
        lookup_digest=lookup_digest,
        evidence_rows=evidence_rows,
        forward_rows=forward_rows,
        reverse_rows=_reverse_rows_from_forward(forward_rows),
    )


def build_compact_tin_npi_generation(
    scan_records: Iterable[FhirOrganizationScanRecord],
    *,
    source_vector: TinNpiConnectorSourceVector,
) -> CompactTinNpiGeneration:
    """Scan every Organization and factor its complete same-entity evidence."""

    if type(source_vector) is not TinNpiConnectorSourceVector:
        raise TinNpiConnectorError("connector source vector is invalid")
    scan_proofs, scanned_evidence_rows = scan_proofs_and_evidence(
        scan_records,
        source_vector=source_vector,
    )
    unique_evidence_rows = _deduplicate_evidence_rows(
        scanned_evidence_rows,
        source_vector=source_vector,
        scan_proofs=scan_proofs,
    )
    return _assemble_generation(
        source_vector=source_vector,
        scan_proofs=scan_proofs,
        evidence_rows=unique_evidence_rows,
    )


_scan_proofs_and_evidence = scan_proofs_and_evidence
