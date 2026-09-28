# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
from unittest.mock import AsyncMock

import pytest

importer = importlib.import_module("process.provider_directory_fhir")
PSEUDO_EIN = "12345678-1234-1234-1234-123456789abc"


def test_cms_numeric_resource_ids_do_not_become_npis_or_tax_ids():
    practitioner_by_field = {
        "resourceType": "Practitioner",
        "id": "1003000126",
    }
    organization_by_field = {
        "resourceType": "Organization",
        "id": "1234567893",
        "identifier": [
            {
                "system": importer.CMS_NPD_PSEUDO_EIN_SYSTEM,
                "value": PSEUDO_EIN,
            }
        ],
    }

    _, practitioner_row = importer.parse_fhir_resource("cms-npd", practitioner_by_field)
    model, organization_row = importer.parse_fhir_resource("cms-npd", organization_by_field)
    assert practitioner_row["npi"] is None
    assert organization_row["npi"] is None
    assert organization_row["tax_id"] is None
    assert organization_row["identifiers"][0]["value"] == PSEUDO_EIN

    retained = importer._endpoint_dataset_resource_rows(
        model,
        [organization_row],
        dataset_id="dataset-a",
        resource_hash_contract=importer.DEFAULT_RESOURCE_HASH_CONTRACT,
    )[0]["payload_json"]
    assert retained["npi"] is None
    assert retained["tax_id"] is None
    assert retained["identifiers"][0]["value"] == PSEUDO_EIN


def test_cms_explicit_npis_preserve_legacy_fallback():
    resource_by_field = {
        "resourceType": "Organization",
        "id": "1003000126",
        "identifier": [
            {
                "system": "http://hl7.org/fhir/sid/us-npi",
                "value": "1234567893",
            }
        ],
    }
    _, cms_row = importer.parse_fhir_resource("cms-npd", resource_by_field)
    assert cms_row["npi"] == 1234567893

    resource_by_field["identifier"][0]["value"] = "1234567890"
    _, invalid_cms_row = importer.parse_fhir_resource("cms-npd", resource_by_field)
    _, legacy_row = importer.parse_fhir_resource("source-a", resource_by_field)
    assert invalid_cms_row["npi"] is None
    assert legacy_row["npi"] == 1234567890

    resource_by_field["identifier"].append(
        {
            "system": "http://hl7.org/fhir/sid/us-npi",
            "value": "1234567893",
        }
    )
    _, second_cms_row = importer.parse_fhir_resource("cms-npd", resource_by_field)
    assert second_cms_row["npi"] == 1234567893

    resource_by_field.pop("identifier")
    _, legacy_fallback_row = importer.parse_fhir_resource("source-a", resource_by_field)
    assert legacy_fallback_row["npi"] == 1003000126


def test_cms_national_provider_type_text_is_an_explicit_npi():
    resource_by_field = {
        "resourceType": "Organization",
        "id": "organization-a",
        "identifier": [{"type": {"text": "National Provider Identifier"}, "value": "1234567893"}],
    }
    _, row = importer.parse_fhir_resource("cms-npd", resource_by_field)
    assert row["npi"] == 1234567893


@pytest.mark.parametrize("identifier_value", ("1234567893\u00b2", "123456789\u00b2"))
def test_legacy_npi_helper_rejects_non_ascii_numeric_suffix(identifier_value):
    resource_by_field = {"identifier": [{"system": "http://hl7.org/fhir/sid/us-npi", "value": identifier_value}]}
    assert importer._npi(resource_by_field) is None


@pytest.mark.parametrize("resource_type", ("PractitionerRole", "HealthcareService"))
def test_cms_role_and_service_npis_require_valid_explicit_identifier(resource_type):
    resource_by_field = {"resourceType": resource_type, "id": "1003000126"}
    _, cms_row = importer.parse_fhir_resource("cms-npd", resource_by_field)
    _, legacy_row = importer.parse_fhir_resource("source-a", resource_by_field)
    assert cms_row["npi"] is None
    assert legacy_row["npi"] is None

    resource_by_field["identifier"] = [{"system": "http://hl7.org/fhir/sid/us-npi", "value": "1234567890"}]
    _, cms_row = importer.parse_fhir_resource("cms-npd", resource_by_field)
    _, legacy_row = importer.parse_fhir_resource("source-a", resource_by_field)
    assert cms_row["npi"] is None
    assert legacy_row["npi"] == 1234567890

    resource_by_field["identifier"][0]["value"] = "1234567893"
    _, cms_row = importer.parse_fhir_resource("cms-npd", resource_by_field)
    assert cms_row["npi"] == 1234567893

    for suffix in ("\u0661", "\u00b2", "\u2163"):
        resource_by_field["identifier"][0]["value"] = f"1234567893{suffix}"
        _, cms_row = importer.parse_fhir_resource("cms-npd", resource_by_field)
        assert cms_row["npi"] is None


def test_pseudo_ein_never_becomes_tax_id():
    organization_by_field = {
        "resourceType": "Organization",
        "id": "organization-a",
        "identifier": [
            {
                "system": importer.CMS_NPD_PSEUDO_EIN_SYSTEM,
                "value": PSEUDO_EIN,
            },
            {"system": "https://example.test/ein", "value": "12-3456789"},
        ],
    }
    _, row = importer.parse_fhir_resource("cms-npd", organization_by_field)
    assert row["tax_id"] == "12-3456789"
    assert [identifier["value"] for identifier in row["identifiers"]] == [
        PSEUDO_EIN,
        "12-3456789",
    ]

    organization_by_field["identifier"] = organization_by_field["identifier"][:1]
    _, other_source_row = importer.parse_fhir_resource("source-a", organization_by_field)
    assert other_source_row["tax_id"] is None


@pytest.mark.asyncio
async def test_resource_id_backfill_excludes_cms_in_scoped_and_unscoped_runs(monkeypatch):
    status = AsyncMock(return_value="UPDATE 0")
    monkeypatch.setattr(importer.db, "status", status)

    for source_ids in (["cms-npd"], ["cms-npd", "source-a"], None):
        result = await importer.backfill_provider_directory_resource_id_npis("mrf", source_ids=source_ids)
        assert result == {"Practitioner": 0, "Organization": 0}
    assert status.await_count == 6
    for call in status.await_args_list:
        assert "resource.source_id <> :cms_npd_source_id" in call.args[0]
        assert call.kwargs["cms_npd_source_id"] == "cms-npd"


@pytest.mark.asyncio
async def test_publication_preflight_ignores_cms_numeric_ids_but_checks_other_sources(monkeypatch):
    scalar = AsyncMock(side_effect=[False, False])
    monkeypatch.setattr(importer.db, "scalar", scalar)

    await importer._assert_no_resource_npi_candidates("mrf", ["cms-npd"])
    scalar.assert_not_awaited()

    await importer._assert_no_resource_npi_candidates("mrf", ["cms-npd", "source-a"])
    assert scalar.await_count == 2
    assert all(call.kwargs["source_ids"] == ["source-a"] for call in scalar.await_args_list)
