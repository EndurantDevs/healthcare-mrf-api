# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Cross-language generation digest vector for the TIN-to-NPI connector."""

from __future__ import annotations

from process import tin_npi_connector as connector
from process.tin_npi_connector import (
    CompactTinNpiGeneration,
    FHIR_ORGANIZATION_SCAN_TERMINAL_STATES,
    FHIR_SAME_ORGANIZATION_RELATIONSHIP,
    FhirOrganizationEvidenceState,
    FhirOrganizationScanProof,
    FhirTinNpiEvidence,
    NpiTinLookupReference,
    NpiTinLookupRow,
    TinNpiLookupRow,
    TinTaxIdentityToken,
    canonical_fhir_evidence_set_digest,
    canonical_source_ordinal_map_digest,
    canonical_source_ordinal_map_json,
)
from tests.tin_npi_connector_unit_support import (
    EVIDENCE_AS_OF,
    RELEASE_1_TOKEN_POLICY_ID,
    REVIEWED_TAX_AS_EIN_POLICY,
    identifier_rule,
)


EXPECTED_SOURCE_JSON = (
    '[{"ordinal":0,"source_id":"source-00"},'
    '{"ordinal":1,"source_id":"source-01"},'
    '{"ordinal":2,"source_id":"source-02"},'
    '{"ordinal":3,"source_id":"source-03"},'
    '{"ordinal":4,"source_id":"source-04"},'
    '{"ordinal":5,"source_id":"source-05"},'
    '{"ordinal":6,"source_id":"source-06"},'
    '{"ordinal":7,"source_id":"source-07"},'
    '{"ordinal":8,"source_id":"source-08"}]'
)
EXPECTED_SOURCE_DIGEST = bytes.fromhex(
    "1a26df8b2720ba342b888e1a2bc5a9a2" "a9ab99ac24f76e866263a1a0eaa4ad51"
)


def test_compact_generation_digest_matches_cross_language_binary_vector():
    generation = _vector_generation()

    assert (
        canonical_source_ordinal_map_json(reversed(generation.source_ordinal_map))
        == EXPECTED_SOURCE_JSON
    )
    assert (
        canonical_source_ordinal_map_digest(generation.source_ordinal_map)
        == EXPECTED_SOURCE_DIGEST
    )
    assert generation.source_ordinal_map_json == EXPECTED_SOURCE_JSON
    assert generation.lookup_digest.hex() == (
        "b4f027a31ed2e3026a597fed9b43e92e" "8cf92d2a9cee792b9d9fbc522d39c1e0"
    )
    assert generation.scan_proof_digest.hex() == (
        "188bf914acedad21579d316310b24e4e" "d5692d7051df904952a943aaa83cec33"
    )
    assert generation.generation_id == (
        "daf9b03d6723970de7bf205867829040" "39f4adcd414ee0688ecab896a100f12f"
    )
    assert generation.evidence_count == 3


def _vector_generation():
    source_ids = tuple(f"source-{index:02d}" for index in range(9))
    token = TinTaxIdentityToken(
        token_policy_id=RELEASE_1_TOKEN_POLICY_ID,
        tin_id_128=bytes(range(16)),
        tin_hmac_sha256=bytes(range(32)),
    )
    lookup = _vector_lookup(token)
    evidence_by_index = _vector_evidence(token)
    evidence_rows = tuple(
        sorted(evidence_by_index.values(), key=lambda evidence: evidence.evidence_id)
    )
    scan_proofs = _build_vector_proof_set(source_ids, evidence_by_index)
    lookup_digest = connector._lookup_digest((lookup,))
    scan_digest = connector.canonical_fhir_organization_scan_proof_digest(scan_proofs)
    generation_id = connector._generation_id(
        source_vector_id="0" * 64,
        scan_proof_digest=scan_digest,
        lookup_digest=lookup_digest,
    )
    reverse_reference = NpiTinLookupReference(
        token=token,
        relationship_class=FHIR_SAME_ORGANIZATION_RELATIONSHIP,
    )
    return CompactTinNpiGeneration(
        generation_id=generation_id,
        source_vector_id="0" * 64,
        source_ordinal_map=source_ids,
        source_ordinal_map_digest=EXPECTED_SOURCE_DIGEST,
        scan_proofs=scan_proofs,
        scan_proof_digest=scan_digest,
        lookup_digest=lookup_digest,
        evidence_rows=evidence_rows,
        forward_rows=(lookup,),
        reverse_rows=tuple(
            NpiTinLookupRow(npi=npi, tax_identities=(reverse_reference,))
            for npi in lookup.npis
        ),
    )


def _vector_lookup(token):
    return TinNpiLookupRow(
        token=token,
        relationship_class=FHIR_SAME_ORGANIZATION_RELATIONSHIP,
        npis=(1000000004, 1234567893),
        evidence_count=3,
        source_ids=("source-00", "source-03", "source-08"),
        source_bitmap=b"\x09\x01",
        npi_source_bitmap_matrix=b"\x09\x00\x00\x01",
        source_evidence_counts=(1, 0, 0, 1, 0, 0, 0, 0, 1),
    )


def _vector_evidence(token):
    evidence_by_index = {}
    for index, npi in ((0, 1000000004), (3, 1000000004), (8, 1234567893)):
        source_id = f"source-{index:02d}"
        endpoint_id = f"endpoint-{index:02d}"
        reviewed_rule = identifier_rule(
            source_id=source_id,
            endpoint_id=endpoint_id,
        )
        evidence_by_index[index] = FhirTinNpiEvidence(
            token=token,
            npi=npi,
            source_id=source_id,
            source_endpoint_id=endpoint_id,
            source_dataset_id=f"dataset-{index:02d}",
            source_record_hmac_sha256=bytes([index]) * 32,
            source_record_identity_sha256=bytes([index + 1]) * 32,
            source_record_payload_hash=f"{index + 1:x}" * 64,
            evidence_as_of=EVIDENCE_AS_OF,
            identifier_policy_id=REVIEWED_TAX_AS_EIN_POLICY.policy_id,
            identifier_policy_sha256=REVIEWED_TAX_AS_EIN_POLICY.descriptor_sha256,
            identifier_rule_id=reviewed_rule.rule_id,
            identifier_rule_sha256=reviewed_rule.descriptor_sha256,
        )
    return evidence_by_index


def _build_vector_proof_set(source_ids, evidence_by_index):
    return tuple(
        _build_vector_proof(index, source_id, evidence_by_index)
        for index, source_id in enumerate(source_ids)
    )


def _build_vector_proof(index, source_id, evidence_by_index):
    reviewed_rule = identifier_rule(
        source_id=source_id,
        endpoint_id=f"endpoint-{index:02d}",
    )
    matched_state = (
        FhirOrganizationEvidenceState.MATCHED
        if index in {0, 3, 8}
        else FhirOrganizationEvidenceState.MISSING_IDENTIFIERS
    )
    state_counts = tuple(
        (state.value, int(state is matched_state))
        for state in sorted(
            FHIR_ORGANIZATION_SCAN_TERMINAL_STATES,
            key=lambda candidate_state: candidate_state.value,
        )
    )
    selected_evidence = (
        (evidence_by_index[index],) if index in evidence_by_index else ()
    )
    return FhirOrganizationScanProof(
        source_id=source_id,
        endpoint_id=f"endpoint-{index:02d}",
        dataset_id=f"dataset-{index:02d}",
        source_summary_sha256=f"{index + 1:x}" * 64,
        identifier_rule_id=reviewed_rule.rule_id,
        identifier_rule_sha256=reviewed_rule.descriptor_sha256,
        organization_resource_count=1,
        organization_resource_sha256=f"{index + 1:x}" * 64,
        state_counts=state_counts,
        matched_evidence_counts=((RELEASE_1_TOKEN_POLICY_ID, int(index in {0, 3, 8})),),
        matched_evidence_sha256=canonical_fhir_evidence_set_digest(
            selected_evidence
        ).hex(),
    )
