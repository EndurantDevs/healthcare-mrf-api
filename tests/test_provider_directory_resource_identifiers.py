# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib

from db.models import (
    ProviderDirectoryHealthcareService,
    ProviderDirectoryOrganizationAffiliation,
    ProviderDirectoryPractitionerRole,
)


importer = importlib.import_module("process.provider_directory_fhir")


def _rich_identifier():
    """Return an identifier covering every reviewed normalized component."""
    return {
        "use": "official",
        "type": {
            "coding": [{"system": "urn:id-type", "code": "license"}],
            "text": "State license",
        },
        "system": "https://example.test/licenses",
        "value": "LIC-123",
        "period": {"start": "2020-01-02", "end": "2027-01-01"},
        "assigner": {
            "reference": "Organization/licensing-board",
            "display": "State Licensing Board",
        },
    }


def _parse_with_dataset(resource):
    """Parse one resource and return its immutable payload projection."""
    model, row = importer.parse_fhir_resource("source-a", resource)
    dataset_rows = importer._endpoint_dataset_resource_rows(
        model,
        [row],
        dataset_id="dataset-a",
    )
    return model, row, dataset_rows[0]["payload_json"]


def test_practitioner_role_identifier_round_trips_to_dataset():
    """Preserve every reviewed role identifier component."""
    model, role_row, dataset_payload = _parse_with_dataset(
        {
            "resourceType": "PractitionerRole",
            "id": "role-1",
            "identifier": [_rich_identifier()],
        }
    )

    expected_identifier_map = {
        "use": "official",
        "type_codes": [
            {
                "system": "urn:id-type",
                "code": "license",
                "display": None,
                "text": "State license",
            }
        ],
        "system": "https://example.test/licenses",
        "value": "LIC-123",
        "period_start": "2020-01-02",
        "period_end": "2027-01-01",
        "assigner_ref": "Organization/licensing-board",
        "assigner_display": "State Licensing Board",
    }
    assert model is ProviderDirectoryPractitionerRole
    assert role_row["identifiers"] == [expected_identifier_map]
    assert dataset_payload["identifiers"] == [expected_identifier_map]


def test_healthcare_service_identifier_and_comment_round_trip_to_dataset():
    """Retain service identity and human-facing source comments."""
    model, row, payload = _parse_with_dataset(
        {
            "resourceType": "HealthcareService",
            "id": "service-1",
            "identifier": [_rich_identifier()],
            "comment": "Evening appointments are available by request.",
        }
    )

    assert model is ProviderDirectoryHealthcareService
    assert row["identifiers"][0]["value"] == "LIC-123"
    assert row["comment"] == "Evening appointments are available by request."
    assert payload["identifiers"] == row["identifiers"]
    assert payload["comment"] == row["comment"]


def test_organization_affiliation_identifier_round_trips_to_dataset():
    """Retain payer-issued organization-affiliation identity."""
    model, row, payload = _parse_with_dataset(
        {
            "resourceType": "OrganizationAffiliation",
            "id": "affiliation-1",
            "identifier": [_rich_identifier()],
        }
    )

    assert model is ProviderDirectoryOrganizationAffiliation
    assert row["identifiers"][0]["assigner_display"] == "State Licensing Board"
    assert payload["identifiers"] == row["identifiers"]
