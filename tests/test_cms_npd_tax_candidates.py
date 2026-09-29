# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""CMS NPI-only evidence cannot become a tax token."""

import pytest

from process.provider_directory_identifier_policy import CMS_NPD_PSEUDO_EIN_SYSTEM
from process.tin_npi_connector import (
    FhirOrganizationEvidenceState,
    FhirTinNpiIdentifierPolicy,
    FhirTinNpiIdentifierRule,
    TinNpiConnectorError,
    canonical_provider_directory_payload_hash,
    extract_fhir_organization_tin_npi_evidence,
    extract_normalized_fhir_organization_tin_npi_evidence,
)
from tests.tin_npi_connector_unit_support import (
    EVIDENCE_AS_OF,
    NPI_SYSTEM,
    TYPE_SYSTEM,
    RecordingProjector,
    token_policy,
)

_POLICY = FhirTinNpiIdentifierPolicy(
    policy_id="cms-npd-npi-only-test-v1",
    rules=(
        FhirTinNpiIdentifierRule(
            rule_id="cms-npd-npi-only-test-rule-v1",
            source_id="cms-npd",
            endpoint_id="endpoint-a",
            npi_systems=(NPI_SYSTEM,),
            npi_type_codings=(),
            ein_systems=("https://example.test/ein",),
            ein_type_codings=((TYPE_SYSTEM, "TAX"),),
        ),
    ),
)


def _arguments(projector):
    return {
        "source_id": "cms-npd",
        "source_endpoint_id": "endpoint-a",
        "source_dataset_id": "dataset-a",
        "token_projector": projector,
        "evidence_as_of": EVIDENCE_AS_OF,
        "identifier_policy": _POLICY,
    }


def test_pseudo_ein_yields_npi_only_candidates_without_tax_token(tmp_path):
    identifiers = [
        {"system": NPI_SYSTEM, "value": "1234567893"},
        {"system": NPI_SYSTEM, "value": "1000000004"},
        {
            "system": CMS_NPD_PSEUDO_EIN_SYSTEM,
            "type": {"coding": [{"system": TYPE_SYSTEM, "code": "TAX"}]},
            "value": "01-2345678",
        },
    ]
    resource_by_field = {"resourceType": "Organization", "id": "organization-a", "identifier": identifiers}
    projector = RecordingProjector(token_policy(tmp_path))
    extraction = extract_fhir_organization_tin_npi_evidence(
        resource_by_field,
        resource_payload_hash=canonical_provider_directory_payload_hash(resource_by_field),
        **_arguments(projector),
    )
    assert extraction.state is FhirOrganizationEvidenceState.MISSING_EIN
    assert extraction.npi_candidates == (1000000004, 1234567893)
    assert extraction.evidence == ()
    assert projector.normalized_eins == []

    normalized_payload_by_field = {"resource_id": "organization-a", "identifiers": identifiers}
    normalized = extract_normalized_fhir_organization_tin_npi_evidence(
        {
            "resource_type": "Organization",
            "resource_id": "organization-a",
            "payload_hash": canonical_provider_directory_payload_hash(normalized_payload_by_field),
            "payload_json": normalized_payload_by_field,
        },
        **_arguments(projector),
    )
    assert normalized.npi_candidates == extraction.npi_candidates
    assert normalized.evidence == ()
    assert projector.normalized_eins == []


def test_direct_ein_retains_reviewed_match_and_no_npi_only_candidates(tmp_path):
    resource_by_field = {
        "resourceType": "Organization",
        "id": "organization-a",
        "identifier": [
            {"system": NPI_SYSTEM, "value": "1234567893"},
            {"system": "https://example.test/ein", "value": "01-2345678"},
            {"system": CMS_NPD_PSEUDO_EIN_SYSTEM, "value": "12345678-1234-1234-1234-123456789abc"},
        ],
    }
    projector = RecordingProjector(token_policy(tmp_path))
    extraction = extract_fhir_organization_tin_npi_evidence(
        resource_by_field,
        resource_payload_hash=canonical_provider_directory_payload_hash(resource_by_field),
        **_arguments(projector),
    )
    assert extraction.state is FhirOrganizationEvidenceState.MATCHED
    assert extraction.npi_candidates == ()
    assert len(extraction.evidence) == 1
    assert projector.normalized_eins == ["012345678"]


def test_multi_npi_organization_cannot_create_cartesian_tax_links(tmp_path):
    resource_by_field = {
        "resourceType": "Organization",
        "id": "organization-a",
        "identifier": [
            {"system": NPI_SYSTEM, "value": "1234567893"},
            {"system": NPI_SYSTEM, "value": "1000000004"},
            {"system": "https://example.test/ein", "value": "01-2345678"},
        ],
    }
    projector = RecordingProjector(token_policy(tmp_path))
    with pytest.raises(TinNpiConnectorError, match="ambiguous NPI"):
        extract_fhir_organization_tin_npi_evidence(
            resource_by_field,
            resource_payload_hash=canonical_provider_directory_payload_hash(resource_by_field),
            **_arguments(projector),
        )
    assert projector.normalized_eins == []
