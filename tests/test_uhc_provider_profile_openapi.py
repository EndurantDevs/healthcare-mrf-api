# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from pathlib import Path

import yaml


OPENAPI_PATH = Path("doc/openapi.yaml")


def _assert_organization_contract(organization_by_field) -> None:
    assert organization_by_field["properties"]["tin_status"]["enum"] == [
        "unavailable_from_uhc_source"
    ]
    assert organization_by_field["properties"]["tax_id"]["nullable"] is True
    assert organization_by_field["properties"]["address_status"]["enum"] == [
        "payer_directory_candidate"
    ]


def _assert_membership_contract(membership_by_field) -> None:
    assert membership_by_field["properties"]["relationship_type"]["enum"] == [
        "payer_reported_provider_plan_membership"
    ]
    assert membership_by_field["properties"]["ownership_status"]["enum"] == [
        "not_asserted"
    ]
    assert {
        "network_tier",
        "network_key_id",
        "location_refs",
        "specialty_codes",
        "membership_codes",
        "period_start",
        "period_end",
        "active",
    } <= membership_by_field["properties"].keys()
    assert membership_by_field["properties"]["network_key_id"][
        "pattern"
    ] == "^[0-9a-f]{64}$"
    assert membership_by_field["properties"]["specialty_codes"]["items"] == {
        "type": "string"
    }
    assert membership_by_field["properties"]["membership_codes"]["items"] == {
        "type": "object",
        "additionalProperties": True,
    }
    assert {
        "participating_organization",
        "insurance_plan_refs",
        "plan_scope",
        "relationship_type",
        "ownership_status",
        "source_lineage",
    } <= set(membership_by_field["required"])
    assert "legal ownership" in membership_by_field["description"]


def _assert_lineage_contract(schemas_by_name) -> None:
    lineage_by_field = schemas_by_name["ProviderProfileUhcSourceLineage"]
    assert set(lineage_by_field["required"]) == {
        "catalog_set_sha256",
        "source_file_id",
        "file_name",
        "artifact_sha256",
        "record_ordinal",
        "logical_scope_id",
    }
    source_properties = schemas_by_name["ProviderProfileDocument"][
        "properties"
    ]["sources"]["items"]["properties"]
    assert {"endpoint_id", "dataset_id"} <= source_properties.keys()


def _assert_npi_address_contract(schemas_by_name) -> None:
    address_status = schemas_by_name["NpiAddress"]["properties"][
        "address_status"
    ]
    assert address_status["enum"] == ["payer_directory_candidate"]
    assert "exact network-bound office" in address_status["description"]
    assert "billing-group/TIN ownership" in address_status["description"]


def _assert_global_profile_generation_contract(schemas_by_name) -> None:
    for schema_name in (
        "ProviderDirectoryProfile",
        "ProviderDirectoryProfileEvidence",
    ):
        properties = schemas_by_name[schema_name]["properties"]
        generation = properties["generation_id"]
        assert generation["pattern"] == "^pdprofile_[0-9a-f]{32}$"
        assert "Global applied Provider Directory" in generation[
            "description"
        ]
        assert "materialization" in generation["description"]
        assert "pre-adoption" in generation["description"]
        assert "fails closed" in generation["description"]
        assert "atomically" in properties["published_at"]["description"]
        profile_as_of = properties["profile_as_of"]
        assert profile_as_of["format"] == "date"
        assert profile_as_of["nullable"] is True
        assert "distinct from `published_at`" in profile_as_of[
            "description"
        ]
    source_generations = schemas_by_name["ProviderProfileDocument"][
        "properties"
    ]["source_generations"]
    source_generation_description = " ".join(
        source_generations["description"].split()
    )
    assert (
        "singleton global serving generation"
        in source_generation_description
    )
    assert "pre-adoption transition" in source_generation_description
    composed_profile_as_of = schemas_by_name["ProviderProfileDocument"][
        "properties"
    ]["profile_as_of"]
    assert composed_profile_as_of["format"] == "date"
    assert "only the Provider Directory FHIR facts" in (
        " ".join(composed_profile_as_of["description"].split())
    )
    composed_evidence_as_of = schemas_by_name["ProviderProfileResponse"][
        "properties"
    ]["provider_profile_evidence"]["properties"]["profile_as_of"]
    assert composed_evidence_as_of["format"] == "date"
    assert composed_evidence_as_of["nullable"] is True


def test_uhc_organization_profile_documents_tin_and_membership_semantics():
    """Public contract makes TIN absence, lineage, and non-ownership explicit."""
    spec_by_field = yaml.safe_load(OPENAPI_PATH.read_text())
    schemas_by_name = spec_by_field["components"]["schemas"]
    _assert_organization_contract(
        schemas_by_name["ProviderProfileUhcOrganizationValue"]
    )
    _assert_membership_contract(
        schemas_by_name["ProviderProfilePlanMembershipValue"]
    )
    _assert_lineage_contract(schemas_by_name)
    _assert_npi_address_contract(schemas_by_name)
    _assert_global_profile_generation_contract(schemas_by_name)
