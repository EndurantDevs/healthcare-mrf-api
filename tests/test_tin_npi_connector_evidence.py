# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Evidence and multi-projector tests for the TIN-to-NPI connector."""

from __future__ import annotations

import pytest

from process.tin_npi_connector import (
    FhirOrganizationEvidenceState,
    FhirTinNpiEvidence,
    TIN_TOKEN_POLICY_PREFIX,
    TinNpiConnectorError,
    TinTaxIdentityToken,
    canonical_provider_directory_payload_hash,
    extract_fhir_organization_tin_npi_evidence_for_policies,
    extract_normalized_fhir_organization_tin_npi_evidence,
)
from tests.tin_npi_connector_unit_support import (
    EVIDENCE_AS_OF,
    NPI_SYSTEM,
    REVIEWED_TAX_AS_EIN_POLICY,
    TEST_EIN,
    TEST_EIN_NORMALIZED,
    TEST_HMAC_HEX,
    TEST_SECRET,
    TOKEN_POLICY_ID,
    TYPE_SYSTEM,
    RecordingProjector,
    extract_evidence,
    npi_identifier,
    organization,
    token_policy,
    typed_identifier,
)


def test_explicit_same_organization_identifiers_create_exact_evidence(tmp_path):
    extraction = extract_evidence(
        organization(
            npi_identifier("1234567893"),
            typed_identifier("TAX", TEST_EIN),
        ),
        tmp_path,
    )

    assert extraction.state is FhirOrganizationEvidenceState.MATCHED
    assert len(extraction.evidence) == 1
    evidence = extraction.evidence[0]
    assert evidence.npi == 1234567893
    assert evidence.source_id == "source-a"
    assert evidence.source_endpoint_id == "endpoint-a"
    assert evidence.source_dataset_id == "dataset-a"
    assert evidence.identifier_policy_id == REVIEWED_TAX_AS_EIN_POLICY.policy_id
    assert (
        evidence.identifier_policy_sha256
        == REVIEWED_TAX_AS_EIN_POLICY.descriptor_sha256
    )
    assert evidence.token.tin_hmac_sha256.hex() == TEST_HMAC_HEX
    assert len(evidence.source_record_hmac_sha256) == 32
    assert len(evidence.evidence_id) == 32
    assert TEST_EIN not in repr(evidence)
    assert TEST_EIN_NORMALIZED not in repr(evidence)


def test_evidence_id_matches_hardcoded_binary_vector():
    evidence = FhirTinNpiEvidence(
        token=TinTaxIdentityToken(
            token_policy_id=f"{TIN_TOKEN_POLICY_PREFIX}release-1",
            tin_id_128=bytes(range(16)),
            tin_hmac_sha256=bytes(range(32)),
        ),
        npi=1234567893,
        source_id="source-vector",
        source_endpoint_id="endpoint-vector",
        source_dataset_id="dataset-vector",
        source_record_hmac_sha256=b"\x11" * 32,
        source_record_identity_sha256=b"\x22" * 32,
        source_record_payload_hash="33" * 32,
        evidence_as_of=EVIDENCE_AS_OF,
        identifier_policy_id="policy-vector",
        identifier_policy_sha256="44" * 32,
        identifier_rule_id="rule-vector",
        identifier_rule_sha256="55" * 32,
    )

    assert evidence.evidence_id.hex() == (
        "5ecb13238da3c8fa0a595e4df70a6ee4" "d68cfab5b5f281a8a26e1ba74b94c7f2"
    )


def test_same_ein_can_return_sorted_deduplicated_npi_array_source(tmp_path):
    extraction = extract_evidence(
        organization(
            npi_identifier("1234567893"),
            typed_identifier("NPI", "1000000004"),
            typed_identifier("NPI", "1 000 000 004"),
            typed_identifier("TAX", TEST_EIN),
        ),
        tmp_path,
    )

    assert extraction.state is FhirOrganizationEvidenceState.MATCHED
    assert [evidence.npi for evidence in extraction.evidence] == [
        1000000004,
        1234567893,
    ]
    assert len({evidence.evidence_id for evidence in extraction.evidence}) == 2


def _two_projectors(tmp_path):
    second_policy_id = f"{TIN_TOKEN_POLICY_PREFIX}2026-08-b"
    first_projector = RecordingProjector(
        token_policy(tmp_path, policy_id=TOKEN_POLICY_ID),
    )
    second_projector = RecordingProjector(
        token_policy(
            tmp_path,
            secret=bytes(reversed(TEST_SECRET)),
            policy_id=second_policy_id,
        ),
    )
    return second_policy_id, first_projector, second_projector


def _projector_request(organization_resource):
    return {
        "source_id": "source-a",
        "source_endpoint_id": "endpoint-a",
        "source_dataset_id": "dataset-a",
        "resource_payload_hash": canonical_provider_directory_payload_hash(
            organization_resource
        ),
        "evidence_as_of": EVIDENCE_AS_OF,
        "identifier_policy": REVIEWED_TAX_AS_EIN_POLICY,
    }


def test_multi_projector_pass_normalizes_ein_once_for_every_policy(tmp_path):
    second_policy_id, first_projector, second_projector = _two_projectors(tmp_path)
    organization_resource = organization(
        npi_identifier("1234567893"),
        typed_identifier("TAX", TEST_EIN),
    )

    extraction = extract_fhir_organization_tin_npi_evidence_for_policies(
        organization_resource,
        token_projectors=(first_projector, second_projector),
        **_projector_request(organization_resource),
    )

    assert extraction.state is FhirOrganizationEvidenceState.MATCHED
    assert first_projector.normalized_eins == [TEST_EIN_NORMALIZED]
    assert second_projector.normalized_eins == [TEST_EIN_NORMALIZED]
    assert [
        evidence_row.token.token_policy_id for evidence_row in extraction.evidence
    ] == [TOKEN_POLICY_ID, second_policy_id]
    assert len(first_projector.source_record_calls) == 1
    assert len(second_projector.source_record_calls) == 1


def test_multi_projector_pass_fails_without_returning_partial_evidence(tmp_path):
    second_policy_id, first_projector, _ = _two_projectors(tmp_path)
    second_projector = RecordingProjector(
        token_policy(
            tmp_path,
            secret=bytes(reversed(TEST_SECRET)),
            policy_id=second_policy_id,
        ),
        tokenize_error=TinNpiConnectorError("synthetic projector failure"),
    )
    organization_resource = organization(
        npi_identifier("1234567893"),
        typed_identifier("TAX", TEST_EIN),
    )

    with pytest.raises(TinNpiConnectorError, match="synthetic projector failure"):
        extract_fhir_organization_tin_npi_evidence_for_policies(
            organization_resource,
            token_projectors=(first_projector, second_projector),
            **_projector_request(organization_resource),
        )

    assert first_projector.normalized_eins == [TEST_EIN_NORMALIZED]
    assert second_projector.normalized_eins == [TEST_EIN_NORMALIZED]


def test_multi_projector_pass_rejects_token_for_wrong_policy(tmp_path):
    second_policy_id = f"{TIN_TOKEN_POLICY_PREFIX}2026-08-b"
    first_delegate = token_policy(tmp_path, policy_id=TOKEN_POLICY_ID)
    wrong_policy_projector = RecordingProjector(
        token_policy(
            tmp_path,
            secret=bytes(reversed(TEST_SECRET)),
            policy_id=second_policy_id,
        ),
        returned_token=first_delegate.tokenize_ein(TEST_EIN),
    )
    organization_resource = organization(
        npi_identifier("1234567893"),
        typed_identifier("TAX", TEST_EIN),
    )

    with pytest.raises(TinNpiConnectorError, match="returned an invalid token"):
        extract_fhir_organization_tin_npi_evidence_for_policies(
            organization_resource,
            token_projectors=(wrong_policy_projector,),
            **_projector_request(organization_resource),
        )


def test_multi_projector_pass_rejects_duplicate_or_unordered_policies(tmp_path):
    _, first_projector, second_projector = _two_projectors(tmp_path)
    organization_resource = organization(
        npi_identifier("1234567893"),
        typed_identifier("TAX", TEST_EIN),
    )
    request_arguments = _projector_request(organization_resource)

    invalid_projector_orders = (
        (first_projector, first_projector),
        (second_projector, first_projector),
    )
    for projectors in invalid_projector_orders:
        with pytest.raises(TinNpiConnectorError, match="duplicated or unordered"):
            extract_fhir_organization_tin_npi_evidence_for_policies(
                organization_resource,
                token_projectors=projectors,
                **request_arguments,
            )
    with pytest.raises(TinNpiConnectorError, match="projectors are invalid"):
        extract_fhir_organization_tin_npi_evidence_for_policies(
            organization_resource,
            token_projectors=[first_projector, second_projector],
            **request_arguments,
        )


def test_normalized_organization_row_adapter_uses_explicit_identifiers(tmp_path):
    normalized_payload_map = {
        "resource_id": "normalized-organization",
        "active": True,
        "identifiers": [
            {"system": NPI_SYSTEM, "value": "1234567893"},
            {
                "type_codes": [{"system": TYPE_SYSTEM, "code": "TAX"}],
                "value": TEST_EIN,
            },
        ],
    }
    extraction = extract_normalized_fhir_organization_tin_npi_evidence(
        {
            "resource_type": "Organization",
            "resource_id": "normalized-organization",
            "payload_hash": canonical_provider_directory_payload_hash(
                normalized_payload_map
            ),
            "payload_json": normalized_payload_map,
        },
        source_id="source-a",
        source_endpoint_id="endpoint-a",
        source_dataset_id="dataset-a",
        token_projector=token_policy(tmp_path),
        evidence_as_of=EVIDENCE_AS_OF,
        identifier_policy=REVIEWED_TAX_AS_EIN_POLICY,
    )

    assert extraction.state is FhirOrganizationEvidenceState.MATCHED
    assert "normalized-organization" not in repr(extraction.evidence[0])
