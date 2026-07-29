# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed FHIR evidence boundary tests for the TIN-to-NPI connector."""

from __future__ import annotations

import pytest

from process.tin_npi_connector import (
    DEFAULT_FHIR_TIN_NPI_IDENTIFIER_POLICY,
    FhirOrganizationEvidenceState,
    FhirOrganizationScanRecord,
    TinNpiConnectorError,
    build_compact_tin_npi_generation,
    canonical_provider_directory_payload_hash,
    extract_fhir_organization_tin_npi_evidence,
    extract_normalized_fhir_organization_tin_npi_evidence,
)
from tests.tin_npi_connector_unit_support import (
    EVIDENCE_AS_OF,
    REVIEWED_TAX_AS_EIN_POLICY,
    TEST_EIN,
    TEST_EIN_NORMALIZED,
    extract_evidence,
    fhir_dataset,
    npi_identifier,
    organization,
    source_vector,
    token_policy,
    typed_identifier,
)


def test_extractors_recompute_dataset_payload_hash_before_evidence(tmp_path):
    organization_resource = organization(
        npi_identifier("1234567893"),
        typed_identifier("TAX", TEST_EIN),
    )
    original_hash = canonical_provider_directory_payload_hash(organization_resource)
    tampered_resource_map = {
        **organization_resource,
        "identifier": [
            npi_identifier("1000000004"),
            typed_identifier("TAX", TEST_EIN),
        ],
    }

    with pytest.raises(TinNpiConnectorError, match="payload hash mismatch"):
        extract_fhir_organization_tin_npi_evidence(
            tampered_resource_map,
            resource_payload_hash=original_hash,
            **_extraction_arguments(tmp_path),
        )
    _assert_normalized_hash_rejected(tmp_path, organization_resource["identifier"])


def _assert_normalized_hash_rejected(tmp_path, identifiers):
    normalized_payload_map = {
        "resource_id": "organization-1",
        "active": True,
        "identifiers": identifiers,
    }
    with pytest.raises(TinNpiConnectorError, match="payload hash mismatch"):
        extract_normalized_fhir_organization_tin_npi_evidence(
            {
                "resource_type": "Organization",
                "resource_id": "organization-1",
                "payload_hash": "0" * 64,
                "payload_json": normalized_payload_map,
            },
            **_extraction_arguments(tmp_path),
        )


def _extraction_arguments(tmp_path):
    return {
        "source_id": "source-a",
        "source_endpoint_id": "endpoint-a",
        "source_dataset_id": "dataset-a",
        "token_projector": token_policy(tmp_path),
        "evidence_as_of": EVIDENCE_AS_OF,
        "identifier_policy": REVIEWED_TAX_AS_EIN_POLICY,
    }


def test_generic_tax_code_is_not_ein_without_reviewed_source_policy(tmp_path):
    organization_resource = organization(
        npi_identifier("1234567893"),
        typed_identifier("TAX", TEST_EIN),
    )
    with pytest.raises(TinNpiConnectorError, match="does not cover source endpoint"):
        extract_fhir_organization_tin_npi_evidence(
            organization_resource,
            source_id="source-a",
            source_endpoint_id="endpoint-a",
            source_dataset_id="dataset-a",
            resource_payload_hash=canonical_provider_directory_payload_hash(
                organization_resource
            ),
            token_projector=token_policy(tmp_path),
            evidence_as_of=EVIDENCE_AS_OF,
            identifier_policy=DEFAULT_FHIR_TIN_NPI_IDENTIFIER_POLICY,
        )


def test_resource_id_npi_fallback_and_fuzzy_tax_descriptors_are_rejected(tmp_path):
    resource_id_only = extract_evidence(
        organization(
            typed_identifier("TAX", TEST_EIN),
            resource_id="1234567893",
        ),
        tmp_path,
    )
    fuzzy_tax = extract_evidence(
        organization(
            npi_identifier("1234567893"),
            {
                "system": "https://example.test/tin",
                "type": {"text": "Employer EIN"},
                "value": TEST_EIN,
            },
        ),
        tmp_path,
    )

    assert resource_id_only.state is FhirOrganizationEvidenceState.MISSING_NPI
    assert fuzzy_tax.state is FhirOrganizationEvidenceState.MISSING_EIN


def test_untrusted_resource_id_is_validated_but_never_retained(tmp_path):
    extraction = extract_evidence(
        organization(
            npi_identifier("1234567893"),
            typed_identifier("TAX", TEST_EIN),
            resource_id=TEST_EIN_NORMALIZED,
        ),
        tmp_path,
    )

    assert extraction.state is FhirOrganizationEvidenceState.MATCHED
    evidence = extraction.evidence[0]
    assert TEST_EIN_NORMALIZED not in repr(evidence)
    assert not hasattr(evidence, "resource_id")
    assert len(evidence.source_record_hmac_sha256) == 32
    assert len(evidence.source_record_identity_sha256) == 32


def test_distinct_source_records_remain_distinct_without_raw_resource_ids(tmp_path):
    identifiers = (
        npi_identifier("1234567893"),
        typed_identifier("TAX", TEST_EIN),
    )
    first_evidence = extract_evidence(
        organization(*identifiers, resource_id="organization-a"),
        tmp_path,
    ).evidence[0]
    second_evidence = extract_evidence(
        organization(*identifiers, resource_id="organization-b"),
        tmp_path,
    ).evidence[0]

    assert first_evidence.source_record_hmac_sha256 != (
        second_evidence.source_record_hmac_sha256
    )
    assert first_evidence.source_record_identity_sha256 != (
        second_evidence.source_record_identity_sha256
    )
    assert first_evidence.evidence_id != second_evidence.evidence_id
    generation = _two_record_generation(first_evidence, second_evidence)
    assert generation.evidence_count == 2
    assert generation.forward_rows[0].evidence_count == 2


def _two_record_generation(first_evidence, second_evidence):
    identities = (
        ("organization-a", first_evidence.source_record_payload_hash),
        ("organization-b", second_evidence.source_record_payload_hash),
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
        for resource_id, evidence in (
            ("organization-a", first_evidence),
            ("organization-b", second_evidence),
        )
    )
    return build_compact_tin_npi_generation(
        scans,
        source_vector=source_vector(
            fhir_datasets=(fhir_dataset(organization_identities=identities),)
        ),
    )


def test_scan_record_rejects_evidence_from_a_different_resource_identity(tmp_path):
    evidence_rows = extract_evidence(
        organization(
            npi_identifier("1234567893"),
            typed_identifier("TAX", TEST_EIN),
            resource_id="organization-a",
        ),
        tmp_path,
    ).evidence

    with pytest.raises(
        TinNpiConnectorError,
        match="scan evidence identity is inconsistent",
    ):
        FhirOrganizationScanRecord(
            source_id="source-a",
            source_endpoint_id="endpoint-a",
            source_dataset_id="dataset-a",
            resource_id="organization-a",
            payload_hash="b" * 64,
            state=FhirOrganizationEvidenceState.MATCHED,
            evidence=evidence_rows,
        )


@pytest.mark.parametrize(
    ("identifier_kind", "identifier_changes", "expected_state"),
    (
        ("npi", {"use": "old"}, FhirOrganizationEvidenceState.MISSING_NPI),
        ("ein", {"use": "old"}, FhirOrganizationEvidenceState.MISSING_EIN),
        (
            "ein",
            {"period": {"end": "2025-12-31"}},
            FhirOrganizationEvidenceState.MISSING_EIN,
        ),
        (
            "npi",
            {"period": {"start": "2027-01-01"}},
            FhirOrganizationEvidenceState.MISSING_NPI,
        ),
        (
            "ein",
            {"period": {"start": "not-a-date"}},
            FhirOrganizationEvidenceState.MALFORMED_IDENTIFIER_PERIOD,
        ),
        (
            "ein",
            {"period": "not-an-object"},
            FhirOrganizationEvidenceState.MALFORMED_IDENTIFIER_PERIOD,
        ),
        (
            "ein",
            {"period": {"end": "2026-07-26T23:59:59.999999Z"}},
            FhirOrganizationEvidenceState.MISSING_EIN,
        ),
        (
            "ein",
            {"period": {"end": "2026-07-27T00:00:00"}},
            FhirOrganizationEvidenceState.MALFORMED_IDENTIFIER_PERIOD,
        ),
        (
            "ein",
            {"period": {"end": "2026-07-27 00:00:00Z"}},
            FhirOrganizationEvidenceState.MALFORMED_IDENTIFIER_PERIOD,
        ),
    ),
)
def test_identifier_use_and_period_are_evaluated_at_generation_cutoff(
    identifier_kind,
    identifier_changes,
    expected_state,
    tmp_path,
):
    npi_value = npi_identifier("1234567893")
    ein_value = typed_identifier("TAX", TEST_EIN)
    selected_identifier = npi_value if identifier_kind == "npi" else ein_value
    selected_identifier.update(identifier_changes)

    extraction = extract_evidence(organization(npi_value, ein_value), tmp_path)

    assert extraction.state is expected_state
    assert extraction.evidence == ()


@pytest.mark.parametrize(
    "period_end",
    (
        "2026-07-27",
        "2026-07-27T00:00:00Z",
        "2026-07-27T00:00:00.000001Z",
    ),
)
def test_fhir_period_end_boundary_precision_is_explicit(period_end, tmp_path):
    extraction = _extract_period_end(period_end, tmp_path)
    assert extraction.state is FhirOrganizationEvidenceState.MATCHED


@pytest.mark.parametrize("period_end", ("9999", "9999-12", "9999-12-31"))
def test_fhir_maximum_partial_period_end_is_inclusive(period_end, tmp_path):
    extraction = _extract_period_end(period_end, tmp_path)
    assert extraction.state is FhirOrganizationEvidenceState.MATCHED


def _extract_period_end(period_end, tmp_path):
    return extract_evidence(
        organization(
            npi_identifier("1234567893"),
            {
                **typed_identifier("TAX", TEST_EIN),
                "period": {"end": period_end},
            },
        ),
        tmp_path,
    )


def test_generation_requires_an_explicit_evidence_cutoff(tmp_path):
    organization_resource = organization(
        npi_identifier("1234567893"),
        typed_identifier("TAX", TEST_EIN),
    )
    arguments = _extraction_arguments(tmp_path)
    arguments["evidence_as_of"] = None
    with pytest.raises(TinNpiConnectorError, match="cutoff is invalid"):
        extract_fhir_organization_tin_npi_evidence(
            organization_resource,
            resource_payload_hash=canonical_provider_directory_payload_hash(
                organization_resource
            ),
            **arguments,
        )


@pytest.mark.parametrize(
    ("organization_resource", "expected_state"),
    (
        (
            {"resourceType": "Practitioner", "id": "p1", "identifier": []},
            FhirOrganizationEvidenceState.NOT_ORGANIZATION,
        ),
        (
            organization(
                npi_identifier("1234567893"),
                typed_identifier("TAX", TEST_EIN),
                active=False,
            ),
            FhirOrganizationEvidenceState.INACTIVE,
        ),
        (
            organization(
                npi_identifier("1234567890"),
                typed_identifier("TAX", TEST_EIN),
            ),
            FhirOrganizationEvidenceState.MALFORMED_NPI,
        ),
        (
            organization(
                npi_identifier("1234567893"),
                typed_identifier("TAX", "not-an-ein"),
            ),
            FhirOrganizationEvidenceState.MALFORMED_EIN,
        ),
        (
            organization(
                npi_identifier("1234567893"),
                typed_identifier("TAX", TEST_EIN),
                typed_identifier("TAX", "98-7654321"),
            ),
            FhirOrganizationEvidenceState.AMBIGUOUS_EIN,
        ),
        (
            organization(
                {
                    "system": "http://hl7.org/fhir/sid/us-npi",
                    "type": {
                        "coding": [
                            {
                                "system": (
                                    "http://terminology.hl7.org/CodeSystem/v2-0203"
                                ),
                                "code": "TAX",
                            }
                        ]
                    },
                    "value": "1234567893",
                },
            ),
            FhirOrganizationEvidenceState.CONFLICTING_IDENTIFIER_CLASS,
        ),
    ),
)
def test_evidence_extraction_fails_closed_with_non_sensitive_states(
    organization_resource,
    expected_state,
    tmp_path,
):
    extraction = extract_evidence(organization_resource, tmp_path)

    assert extraction.state is expected_state
    assert extraction.evidence == ()
