# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed generation boundary tests for the TIN-to-NPI connector."""

from __future__ import annotations

import datetime as dt
from dataclasses import replace

import pytest

from process.tin_npi_connector import (
    FHIR_ORGANIZATION_SCAN_TERMINAL_STATES,
    FhirOrganizationEvidenceState,
    FhirOrganizationScanProof,
    FhirOrganizationScanRecord,
    TinNpiConnectorError,
    TinTaxIdentityToken,
    assert_generation_reuse_compatible,
    build_compact_tin_npi_generation,
    canonical_evidence_as_of,
    canonical_fhir_evidence_set_digest,
)
from tests.tin_npi_connector_unit_support import (
    OBSERVED_AT,
    REVIEWED_TAX_AS_EIN_POLICY,
    REVIEWED_TAX_AS_EIN_RULE,
    TEST_EIN,
    TOKEN_POLICY_ID,
    extract_evidence,
    fhir_dataset,
    matched_scan,
    npi_identifier,
    organization,
    source_vector,
    typed_identifier,
)


def test_generation_rejects_partial_or_empty_self_consistent_scan(tmp_path):
    extraction = extract_evidence(
        organization(
            npi_identifier("1234567893"),
            typed_identifier("NPI", "1000000004"),
            typed_identifier("TAX", TEST_EIN),
        ),
        tmp_path,
    )
    vector = source_vector(
        fhir_datasets=(
            fhir_dataset(
                organization_identities=(
                    (
                        "organization-1",
                        extraction.evidence[0].source_record_payload_hash,
                    ),
                ),
            ),
        ),
    )
    complete = build_compact_tin_npi_generation(
        (matched_scan(extraction),),
        source_vector=vector,
    )
    assert complete.source_vector_id == vector.source_vector_id
    assert complete.organization_count == 1
    assert complete.matched_organization_count == 1
    assert assert_generation_reuse_compatible(complete, complete) is True
    with pytest.raises(TinNpiConnectorError, match="scan completeness proof mismatch"):
        build_compact_tin_npi_generation((), source_vector=vector)


def test_zero_organization_dataset_has_complete_empty_generation():
    vector = source_vector(
        fhir_datasets=(fhir_dataset(organization_identities=()),),
    )

    generation = build_compact_tin_npi_generation((), source_vector=vector)

    assert generation.organization_count == 0
    assert generation.matched_organization_count == 0
    assert generation.evidence_count == 0
    assert generation.forward_rows == ()
    assert generation.reverse_rows == ()
    scan_proof = generation.scan_proofs[0]
    assert scan_proof.matched_evidence_counts == ((TOKEN_POLICY_ID, 0),)
    assert scan_proof.matched_evidence_sha256 == (
        canonical_fhir_evidence_set_digest(()).hex()
    )


@pytest.mark.parametrize(
    ("terminal_state", "matched_evidence_count"),
    (
        (FhirOrganizationEvidenceState.MISSING_IDENTIFIERS, 1),
        (FhirOrganizationEvidenceState.MATCHED, 0),
    ),
)
def test_scan_proof_rejects_zero_evidence_state_inconsistency(
    terminal_state,
    matched_evidence_count,
):
    state_counts = tuple(
        (state.value, int(state is terminal_state))
        for state in sorted(
            FHIR_ORGANIZATION_SCAN_TERMINAL_STATES,
            key=lambda candidate_state: candidate_state.value,
        )
    )

    with pytest.raises(TinNpiConnectorError, match="scan proof is invalid"):
        FhirOrganizationScanProof(
            source_id="source-a",
            endpoint_id="endpoint-a",
            dataset_id="dataset-a",
            source_summary_sha256="d" * 64,
            identifier_rule_id=REVIEWED_TAX_AS_EIN_RULE.rule_id,
            identifier_rule_sha256=REVIEWED_TAX_AS_EIN_RULE.descriptor_sha256,
            organization_resource_count=1,
            organization_resource_sha256="e" * 64,
            state_counts=state_counts,
            matched_evidence_counts=((TOKEN_POLICY_ID, matched_evidence_count),),
            matched_evidence_sha256=canonical_fhir_evidence_set_digest(()).hex(),
        )


def test_organization_scan_rejects_duplicate_or_out_of_order_rows():
    first_identity = ("organization-1", "1" * 64)
    second_identity = ("organization-2", "2" * 64)
    vector = source_vector(
        fhir_datasets=(
            fhir_dataset(
                organization_identities=(first_identity, second_identity),
            ),
        ),
    )
    first_scan = FhirOrganizationScanRecord(
        source_id="source-a",
        source_endpoint_id="endpoint-a",
        source_dataset_id="dataset-a",
        resource_id=first_identity[0],
        payload_hash=first_identity[1],
        state=FhirOrganizationEvidenceState.MISSING_IDENTIFIERS,
    )
    second_scan = replace(
        first_scan,
        resource_id=second_identity[0],
        payload_hash=second_identity[1],
    )

    invalid_orders = ((second_scan, first_scan), (first_scan, first_scan))
    for invalid_order in invalid_orders:
        with pytest.raises(TinNpiConnectorError, match="not strictly ordered"):
            build_compact_tin_npi_generation(
                invalid_order,
                source_vector=vector,
            )


def test_scan_proof_requires_every_selected_token_policy():
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

    with pytest.raises(TinNpiConnectorError, match="scan proof is invalid"):
        FhirOrganizationScanProof(
            source_id="source-a",
            endpoint_id="endpoint-a",
            dataset_id="dataset-a",
            source_summary_sha256="d" * 64,
            identifier_rule_id=REVIEWED_TAX_AS_EIN_RULE.rule_id,
            identifier_rule_sha256=REVIEWED_TAX_AS_EIN_RULE.descriptor_sha256,
            organization_resource_count=1,
            organization_resource_sha256="e" * 64,
            state_counts=state_counts,
            matched_evidence_counts=(),
            matched_evidence_sha256=canonical_fhir_evidence_set_digest(()).hex(),
        )


def test_scan_record_rejects_mixed_full_hmac_for_one_policy(tmp_path):
    extraction = _two_npi_extraction(tmp_path)
    original_token = extraction.evidence[0].token
    colliding_token = TinTaxIdentityToken(
        token_policy_id=original_token.token_policy_id,
        tin_id_128=original_token.tin_id_128,
        tin_hmac_sha256=original_token.tin_id_128 + b"\xff" * 16,
    )
    mixed_evidence = (
        extraction.evidence[0],
        replace(extraction.evidence[1], token=colliding_token),
    )

    with pytest.raises(TinNpiConnectorError, match="policy evidence is inconsistent"):
        FhirOrganizationScanRecord(
            source_id="source-a",
            source_endpoint_id="endpoint-a",
            source_dataset_id="dataset-a",
            resource_id="organization-1",
            payload_hash=extraction.evidence[0].source_record_payload_hash,
            state=FhirOrganizationEvidenceState.MATCHED,
            evidence=mixed_evidence,
        )


def _two_npi_extraction(tmp_path):
    return extract_evidence(
        organization(
            npi_identifier("1234567893"),
            typed_identifier("NPI", "1000000004"),
            typed_identifier("TAX", TEST_EIN),
        ),
        tmp_path,
    )


def test_compact_generation_keeps_hmac_collision_candidates_isolated(tmp_path):
    first_evidence = _record_evidence(tmp_path, "organization-a")
    second_evidence = _record_evidence(tmp_path, "organization-b")
    colliding_token = TinTaxIdentityToken(
        token_policy_id=second_evidence.token.token_policy_id,
        tin_id_128=second_evidence.token.tin_id_128,
        tin_hmac_sha256=second_evidence.token.tin_id_128 + b"\xff" * 16,
    )
    collision_evidence = replace(first_evidence, token=colliding_token)

    generation = _collision_generation(collision_evidence, second_evidence)

    assert len(generation.forward_rows) == 2
    first_lookup, second_lookup = generation.forward_rows
    assert first_lookup.token.tin_id_128 == second_lookup.token.tin_id_128
    assert first_lookup.token.tin_hmac_sha256 != second_lookup.token.tin_hmac_sha256


def _record_evidence(tmp_path, resource_id):
    return extract_evidence(
        organization(
            npi_identifier("1234567893"),
            typed_identifier("TAX", TEST_EIN),
            resource_id=resource_id,
        ),
        tmp_path,
    ).evidence[0]


def _collision_generation(collision_evidence, second_evidence):
    evidence_pairs = (
        ("organization-a", collision_evidence),
        ("organization-b", second_evidence),
    )
    scans = tuple(
        FhirOrganizationScanRecord(
            source_id="source-a",
            source_endpoint_id="endpoint-a",
            source_dataset_id="dataset-a",
            resource_id=resource_id,
            payload_hash=evidence.source_record_payload_hash,
            state=FhirOrganizationEvidenceState.MATCHED,
            evidence=(evidence,),
        )
        for resource_id, evidence in evidence_pairs
    )
    identities = tuple(
        (resource_id, evidence.source_record_payload_hash)
        for resource_id, evidence in evidence_pairs
    )
    return build_compact_tin_npi_generation(
        scans,
        source_vector=source_vector(
            fhir_datasets=(fhir_dataset(organization_identities=identities),)
        ),
    )


def test_generation_rejects_evidence_outside_dataset_or_policy(tmp_path):
    evidence = _record_evidence(tmp_path, "organization-1")
    vector = source_vector(
        fhir_datasets=(
            fhir_dataset(
                organization_identities=(
                    ("organization-1", evidence.source_record_payload_hash),
                ),
            ),
        ),
    )
    _assert_source_identity_mutations_rejected(evidence, vector)
    _assert_policy_mutations_rejected(evidence, vector)


def _record_for(evidence):
    return FhirOrganizationScanRecord(
        source_id=evidence.source_id,
        source_endpoint_id=evidence.source_endpoint_id,
        source_dataset_id=evidence.source_dataset_id,
        resource_id="organization-1",
        payload_hash=evidence.source_record_payload_hash,
        state=FhirOrganizationEvidenceState.MATCHED,
        evidence=(evidence,),
    )


def _assert_source_identity_mutations_rejected(evidence, vector):
    outside_candidates = (
        replace(evidence, source_dataset_id="other-dataset"),
        replace(evidence, source_endpoint_id="other-endpoint"),
    )
    for outside_evidence in outside_candidates:
        with pytest.raises(
            TinNpiConnectorError,
            match="scan is outside its source vector",
        ):
            build_compact_tin_npi_generation(
                (_record_for(outside_evidence),),
                source_vector=vector,
            )


def _assert_policy_mutations_rejected(evidence, vector):
    changed_rule = replace(
        REVIEWED_TAX_AS_EIN_RULE,
        ein_systems=("https://example.test/reviewed-ein",),
    )
    changed_vector = replace(
        vector,
        fhir_datasets=(
            replace(
                vector.fhir_datasets[0],
                identifier_rule_sha256=changed_rule.descriptor_sha256,
            ),
        ),
        identifier_policy=replace(
            REVIEWED_TAX_AS_EIN_POLICY,
            rules=(changed_rule,),
        ),
    )
    later_evidence = replace(
        evidence,
        evidence_as_of=canonical_evidence_as_of(OBSERVED_AT + dt.timedelta(days=1)),
    )
    for candidate_evidence, candidate_vector in (
        (evidence, changed_vector),
        (later_evidence, vector),
    ):
        with pytest.raises(TinNpiConnectorError, match="identifier policy mismatch"):
            build_compact_tin_npi_generation(
                (_record_for(candidate_evidence),),
                source_vector=candidate_vector,
            )
