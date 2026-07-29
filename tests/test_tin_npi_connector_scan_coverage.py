# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed scan and proof boundary coverage for the TIN-to-NPI connector."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import pytest

from process.tin_npi_connector import (
    FHIR_ORGANIZATION_SCAN_TERMINAL_STATES,
    FhirOrganizationEvidenceState,
    FhirOrganizationScanProof,
    FhirOrganizationScanRecord,
    TinNpiConnectorError,
    build_compact_tin_npi_generation,
    canonical_fhir_evidence_set_digest,
    canonical_fhir_organization_scan_proof_json,
)
from process.tin_npi_connector_scan_build import (
    _new_scan_accumulator,
    _validate_scan_policy_coverage,
    scan_proofs_and_evidence,
)
from tests.tin_npi_connector_unit_support import (
    REVIEWED_TAX_AS_EIN_RULE,
    TEST_EIN,
    TOKEN_POLICY_ID,
    extract_evidence,
    matched_scan,
    npi_identifier,
    organization,
    source_vector,
    typed_identifier,
)


def _type_error_iterator():
    raise TypeError("synthetic iterator failure")
    yield None


def _matched_extraction(tmp_path):
    return extract_evidence(
        organization(
            npi_identifier("1234567893"),
            typed_identifier("NPI", "1000000004"),
            typed_identifier("TAX", TEST_EIN),
        ),
        tmp_path,
    )


def _unmatched_scan_proof() -> FhirOrganizationScanProof:
    state_counts = tuple(
        (
            state.value,
            int(state is FhirOrganizationEvidenceState.MISSING_IDENTIFIERS),
        )
        for state in sorted(
            FHIR_ORGANIZATION_SCAN_TERMINAL_STATES,
            key=lambda candidate_state: candidate_state.value,
        )
    )
    return FhirOrganizationScanProof(
        source_id="source-a",
        endpoint_id="endpoint-a",
        dataset_id="dataset-a",
        source_summary_sha256="d" * 64,
        identifier_rule_id=REVIEWED_TAX_AS_EIN_RULE.rule_id,
        identifier_rule_sha256=REVIEWED_TAX_AS_EIN_RULE.descriptor_sha256,
        organization_resource_count=1,
        organization_resource_sha256="e" * 64,
        state_counts=state_counts,
        matched_evidence_counts=((TOKEN_POLICY_ID, 0),),
        matched_evidence_sha256=canonical_fhir_evidence_set_digest(()).hex(),
    )


@pytest.mark.parametrize(
    ("state", "evidence"),
    (
        ("matched", ()),
        (FhirOrganizationEvidenceState.MATCHED, ()),
        (FhirOrganizationEvidenceState.MISSING_IDENTIFIERS, (object(),)),
        (FhirOrganizationEvidenceState.MISSING_IDENTIFIERS, []),
    ),
)
def test_scan_record_rejects_nonterminal_or_incoherent_shapes(state, evidence):
    with pytest.raises(TinNpiConnectorError, match="scan record is invalid"):
        FhirOrganizationScanRecord(
            source_id="source-a",
            source_endpoint_id="endpoint-a",
            source_dataset_id="dataset-a",
            resource_id="organization-1",
            payload_hash="c" * 64,
            state=state,
            evidence=evidence,
        )


def test_scan_record_rejects_evidence_from_another_dataset(tmp_path):
    extraction = _matched_extraction(tmp_path)
    foreign_evidence = replace(
        extraction.evidence[0],
        source_dataset_id="dataset-b",
    )

    with pytest.raises(TinNpiConnectorError, match="outside its record"):
        FhirOrganizationScanRecord(
            source_id="source-a",
            source_endpoint_id="endpoint-a",
            source_dataset_id="dataset-a",
            resource_id="organization-1",
            payload_hash=foreign_evidence.source_record_payload_hash,
            state=FhirOrganizationEvidenceState.MATCHED,
            evidence=(foreign_evidence,),
        )


@pytest.mark.parametrize("evidence_order", ("duplicate", "reversed"))
def test_scan_record_rejects_duplicate_or_unsorted_evidence(tmp_path, evidence_order):
    evidence_rows = _matched_extraction(tmp_path).evidence
    selected_rows = (
        (evidence_rows[0], evidence_rows[0])
        if evidence_order == "duplicate"
        else tuple(reversed(evidence_rows))
    )

    with pytest.raises(TinNpiConnectorError, match="duplicated or unordered"):
        FhirOrganizationScanRecord(
            source_id="source-a",
            source_endpoint_id="endpoint-a",
            source_dataset_id="dataset-a",
            resource_id="organization-1",
            payload_hash=evidence_rows[0].source_record_payload_hash,
            state=FhirOrganizationEvidenceState.MATCHED,
            evidence=selected_rows,
        )


@pytest.mark.parametrize(
    "invalid_evidence",
    ("not-an-iterable-set", (object(),), _type_error_iterator()),
)
def test_evidence_set_digest_rejects_invalid_or_failed_iterables(invalid_evidence):
    with pytest.raises(TinNpiConnectorError, match="evidence set is invalid"):
        canonical_fhir_evidence_set_digest(invalid_evidence)


def test_evidence_set_digest_rejects_duplicate_evidence(tmp_path):
    evidence = _matched_extraction(tmp_path).evidence[0]

    with pytest.raises(TinNpiConnectorError, match="evidence set is invalid"):
        canonical_fhir_evidence_set_digest((evidence, evidence))


@pytest.mark.parametrize(
    "invalid_proofs",
    ("not-a-proof-set", (object(),), _type_error_iterator()),
)
def test_scan_proof_json_rejects_invalid_or_failed_iterables(invalid_proofs):
    with pytest.raises(TinNpiConnectorError, match="scan proofs are invalid"):
        canonical_fhir_organization_scan_proof_json(invalid_proofs)


def test_scan_proof_json_rejects_duplicate_dataset_keys():
    proof = _unmatched_scan_proof()

    with pytest.raises(TinNpiConnectorError, match="duplicated or unordered"):
        canonical_fhir_organization_scan_proof_json((proof, proof))


def test_scan_builder_rejects_invalid_scan_containers_and_rows():
    vector = source_vector()

    for invalid_scan in ("not-a-scan", _type_error_iterator(), (object(),)):
        with pytest.raises(TinNpiConnectorError, match="Organization scan"):
            scan_proofs_and_evidence(invalid_scan, source_vector=vector)


def test_scan_policy_validator_rejects_evidence_on_nonmatched_terminal(tmp_path):
    extraction = _matched_extraction(tmp_path)
    accumulator = _new_scan_accumulator(source_vector())
    invalid_terminal = SimpleNamespace(
        state=FhirOrganizationEvidenceState.MISSING_IDENTIFIERS,
        evidence=extraction.evidence,
    )

    with pytest.raises(TinNpiConnectorError, match="terminal state is inconsistent"):
        _validate_scan_policy_coverage(accumulator, invalid_terminal)


def test_generation_rejects_a_non_vector_source_contract():
    with pytest.raises(TinNpiConnectorError, match="source vector is invalid"):
        build_compact_tin_npi_generation((), source_vector=object())


def test_matched_scan_fixture_remains_valid_for_scan_boundary_tests(tmp_path):
    extraction = _matched_extraction(tmp_path)

    scan_record = matched_scan(extraction)

    assert scan_record.state is FhirOrganizationEvidenceState.MATCHED
    assert len(scan_record.evidence) == 2
