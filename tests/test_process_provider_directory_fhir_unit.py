# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import argparse
import asyncio
import contextlib
import csv
import dataclasses
import datetime
import email.utils
import hashlib
import io
import json
import importlib
import string
import urllib.parse
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from click.testing import CliRunner
from sqlalchemy.dialects import postgresql

import process as process_cli
from db.models import (
    ProviderDirectoryAPIEndpoint,
    ProviderDirectoryBulkAcquisitionCheckpoint,
    ProviderDirectoryBulkOutputCheckpoint,
    ProviderDirectoryCapability,
    ProviderDirectoryCanonicalResource,
    ProviderDirectoryDatasetResource,
    ProviderDirectoryEndpoint,
    ProviderDirectoryHealthcareService,
    ProviderDirectoryInsurancePlan,
    ProviderDirectoryLocation,
    ProviderDirectoryPractitioner,
    ProviderDirectoryOrganization,
    ProviderDirectoryOrganizationAffiliation,
    ProviderDirectoryPractitionerRole,
    ProviderDirectorySource,
    ProviderDirectorySourceResource,
)
from scripts.research import provider_directory_fhir_harness as harness

importer = importlib.import_module("process.provider_directory_fhir")


def _stub_resource_import_metadata(monkeypatch):
    monkeypatch.setattr(importer, "_update_source_resource_import_metadata", AsyncMock())


def test_provider_directory_json_gin_indexes_cast_json_columns_to_jsonb():
    index = next(
        spec
        for spec in ProviderDirectoryPractitionerRole.__my_additional_indexes__
        if spec["name"] == "provider_directory_role_location_refs_gin_idx"
    )

    elements_sql = importer._provider_directory_index_elements_sql(
        ProviderDirectoryPractitionerRole,
        index,
    )

    assert elements_sql == '("location_refs"::jsonb)'


def test_source_row_from_seed_normalizes_base_and_flags():
    row = importer._source_row_from_seed(
        {
            "id": "1",
            "org_name": " Cigna ",
            "plan_name": "MA HMO",
            "api_base": "HTTPS://FHIR.CIGNA.COM/ProviderDirectory/v1/",
            "requires_registration": "false",
            "requires_api_key": "0",
            "is_medicare_advantage": "true",
            "auth_type": "open",
            "last_validated_status": "valid",
            "source": "fixture",
        }
    )

    assert row["source_id"].startswith("pdfhir_")
    assert row["org_name"] == "Cigna"
    assert row["canonical_api_base"] == "https://fhir.cigna.com/ProviderDirectory/v1"
    assert row["requires_registration"] is False
    assert row["requires_api_key"] is False
    assert row["is_medicare_advantage"] is True


def test_normalized_resource_base_rebases_derived_endpoint_fields():
    row = importer._source_row_from_seed(
        {
            "id": "resource-base-1",
            "org_name": "Example Health Plan",
            "api_base": "https://example.test/fhir/Practitioner",
            "endpoint_practitioner": "https://example.test/fhir/Practitioner",
            "endpoint_location": "https://example.test/fhir/Practitioner/Location",
            "endpoint_organization": "https://example.test/fhir/Organization",
            "source": "fixture",
        }
    )

    assert row["api_base"] == "https://example.test/fhir"
    assert row["endpoint_practitioner"] == "https://example.test/fhir/Practitioner"
    assert row["endpoint_location"] == "https://example.test/fhir/Location"
    assert row["endpoint_organization"] == "https://example.test/fhir/Organization"


def test_source_row_from_seed_overrides_aetna_developer_portal_base():
    source_row = importer._source_row_from_seed(
        {
            "id": "aetna-1",
            "org_name": "Aetna",
            "plan_name": "Aetna Provider Directory",
            "portal_url": "https://developerportal.aetna.com/",
            "api_base": "https://fhir-ehr.cerner.com/r4/aetna",
            "auth_type": "OAuth2/SMART",
            "source": "provider-directory-db",
        }
    )

    assert source_row["api_base"] == importer.AETNA_PROVIDER_DIRECTORY_BASE
    assert source_row["canonical_api_base"] == importer.AETNA_PROVIDER_DIRECTORY_BASE
    assert source_row["requires_registration"] is True
    assert source_row["auth_type"] == "OAuth2/SMART"
    assert source_row["last_validated_status"] == "auth_required"
    assert source_row["metadata_json"]["provider_directory_override"] == "aetna_apif1_providerdirectory"
    assert importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE in source_row["metadata_json"]["provider_directory_equivalent_api_bases"]
    assert source_row["metadata_json"]["provider_directory_supported_resources"] == list(
        importer.AETNA_MEDICAID_SUPPORTED_RESOURCES
    )
    assert source_row["metadata_json"]["provider_directory_coverage_mode"] == "targeted"
    assert source_row["metadata_json"]["provider_directory_fully_enumerable_resources"] == []
    assert source_row["metadata_json"]["provider_directory_previous_api_base"] == "https://fhir-ehr.cerner.com/r4/aetna"
    assert importer.AETNA_MEDICAID_SUPPORTED_RESOURCES == (
        "InsurancePlan",
        "PractitionerRole",
        "Practitioner",
        "Organization",
        "Location",
        "OrganizationAffiliation",
    )


def test_aetna_provider_directorydata_supplemental_source_is_not_rewritten_to_medicaid_base():
    seed_rows = importer._aetna_provider_directory_data_seed_rows(source_query="Aetna")
    assert len(seed_rows) == 1

    source_row = importer._source_row_from_seed(seed_rows[0])

    assert source_row["api_base"] == importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE
    assert source_row["canonical_api_base"] == importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE
    assert source_row["requires_registration"] is True
    assert source_row["last_validated_status"] == "auth_required"
    assert source_row["metadata_json"]["provider_directory_override"] == "aetna_apif1_providerdirectorydata"
    assert source_row["metadata_json"]["provider_directory_bulk_export_omit_output_format"] is True
    assert source_row["metadata_json"]["provider_directory_bulk_export_auto_enabled"] is True
    assert source_row["metadata_json"]["provider_directory_bulk_export_eligible"] is True
    assert source_row["metadata_json"]["provider_directory_bulk_export_output_hosts"] == [
        "storage.googleapis.com"
    ]
    assert source_row["metadata_json"]["provider_directory_supported_resources"] == list(
        importer.AETNA_COMMERCIAL_SUPPORTED_RESOURCES
    )
    assert source_row["metadata_json"]["provider_directory_coverage_mode"] == "full"
    assert source_row["metadata_json"]["provider_directory_fully_enumerable_resources"] == list(
        importer.AETNA_COMMERCIAL_SUPPORTED_RESOURCES
    )
    assert set(source_row["metadata_json"]["provider_directory_resource_page_count_caps"]) == set(
        importer.AETNA_COMMERCIAL_SUPPORTED_RESOURCES
    )
    assert set(source_row["metadata_json"]["provider_directory_resource_page_count_caps"].values()) == {30}
    assert importer.AETNA_COMMERCIAL_SUPPORTED_RESOURCES == (
        "InsurancePlan",
        "PractitionerRole",
        "Practitioner",
        "Organization",
        "Location",
        "HealthcareService",
        "OrganizationAffiliation",
    )


def test_aetna_provider_directorydata_uses_providerdirectory_credentials(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_CREDENTIALS_JSON",
        json.dumps(
            {
                "api_bases": {
                    importer.AETNA_PROVIDER_DIRECTORY_BASE: {
                        "oauth2": {
                            "token_url": importer.AETNA_PROVIDER_DIRECTORY_TOKEN_URL,
                            "client_id": "client",
                            "client_secret": "secret",
                            "scope": "Public NonPII",
                        }
                    }
                }
            }
        ),
    )

    spec = importer._credential_spec_for_source(
        {
            "source_id": "aetna-data",
            "org_name": "Aetna",
            "api_base": importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE,
            "canonical_api_base": importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE,
            "metadata_json": {
                "provider_directory_equivalent_api_bases": [
                    importer.AETNA_PROVIDER_DIRECTORY_BASE,
                ],
            },
        }
    )

    assert spec["oauth2"]["scope"] == "Public NonPII"
    assert f"api_bases:{importer.AETNA_PROVIDER_DIRECTORY_BASE}" in spec["_matched_by"]


@pytest.mark.parametrize(
    ("api_base", "expected_scope"),
    [
        (importer.AETNA_PROVIDER_DIRECTORY_BASE, "medicaid-scope"),
        (importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE, "commercial-scope"),
    ],
)
def test_aetna_exact_base_credentials_do_not_cross_override(
    monkeypatch,
    api_base,
    expected_scope,
):
    monkeypatch.setenv(
        importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV,
        json.dumps(
            {
                "api_bases": {
                    importer.AETNA_PROVIDER_DIRECTORY_BASE: {
                        "oauth2": {"scope": "medicaid-scope"},
                    },
                    importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE: {
                        "oauth2": {"scope": "commercial-scope"},
                    },
                }
            }
        ),
    )
    equivalent_base = (
        importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE
        if api_base == importer.AETNA_PROVIDER_DIRECTORY_BASE
        else importer.AETNA_PROVIDER_DIRECTORY_BASE
    )

    spec = importer._credential_spec_for_source(
        {
            "source_id": "aetna",
            "org_name": "Aetna",
            "api_base": api_base,
            "metadata_json": {
                "provider_directory_equivalent_api_bases": [equivalent_base],
            },
        }
    )

    assert spec["oauth2"]["scope"] == expected_scope
    assert spec["_matched_by"] == [f"api_bases:{api_base}"]


def test_aetna_credentials_are_forwarded_only_within_exact_source_base(monkeypatch):
    monkeypatch.setenv(
        importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV,
        json.dumps(
            {
                "api_bases": {
                    importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE: {
                        "bearer_token": "commercial-token",
                    }
                }
            }
        ),
    )
    source_lookup = {
        "source_id": "aetna-commercial",
        "api_base": importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE,
    }

    allowed_options = importer._credential_request_options_for_source(
        source_lookup,
        f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}/Practitioner?_count=30",
    )
    sibling_options = importer._credential_request_options_for_source(
        source_lookup,
        f"{importer.AETNA_PROVIDER_DIRECTORY_BASE}/Practitioner?_count=30",
    )
    prefix_collision_options = importer._credential_request_options_for_source(
        source_lookup,
        f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}-other/Practitioner",
    )

    assert allowed_options["headers"] == {
        "Authorization": "Bearer commercial-token"
    }
    assert sibling_options == {"headers": {}, "query_params": {}, "descriptor": None}
    assert prefix_collision_options == {
        "headers": {},
        "query_params": {},
        "descriptor": None,
    }


def test_source_row_from_seed_overrides_aetna_stale_public_auth_label():
    row = importer._source_row_from_seed(
        {
            "id": "aetna-stale-auth",
            "org_name": "Aetna",
            "plan_name": "Aetna Provider Directory",
            "portal_url": "https://developerportal.aetna.com/apis",
            "api_base": "https://developerportal.aetna.com/apis",
            "auth_type": "none",
            "requires_registration": "false",
            "source": "provider-directory-db",
        }
    )

    assert row["api_base"] == importer.AETNA_PROVIDER_DIRECTORY_BASE
    assert row["requires_registration"] is True
    assert row["auth_type"] == "OAuth2/SMART"
    assert row["last_validated_status"] == "auth_required"


def test_source_row_from_seed_overrides_aetna_patientaccess_directory_variant():
    row = importer._source_row_from_seed(
        {
            "id": "aetna-patientaccess",
            "org_name": "Aetna",
            "api_base": "https://vteapif1.aetna.com/fhirdirectory/v1/patientaccess/",
            "auth_type": "none",
            "requires_registration": "false",
            "last_validated_status": "not_found",
            "source": "provider-directory-db",
        }
    )

    assert row["api_base"] == importer.AETNA_PROVIDER_DIRECTORY_BASE
    assert row["canonical_api_base"] == importer.AETNA_PROVIDER_DIRECTORY_BASE
    assert row["requires_registration"] is True
    assert row["auth_type"] == "OAuth2/SMART"
    assert row["last_validated_status"] == "auth_required"
    assert row["metadata_json"]["provider_directory_override"] == "aetna_apif1_providerdirectory"
    assert (
        row["metadata_json"]["provider_directory_previous_api_base"]
        == "https://vteapif1.aetna.com/fhirdirectory/v1/patientaccess/"
    )


def test_source_row_from_seed_overrides_cigna_availity_non_fhir_base():
    row = importer._source_row_from_seed(
        {
            "id": "cigna-1",
            "org_name": "Cigna Corporation",
            "plan_name": "Medicare Advantage",
            "api_base": "https://apps.availity.com/availity/public-fhir/fhir/v1/cigna/r4",
            "auth_type": "none",
            "last_validated_status": "valid_non_fhir",
            "source": "provider-directory-db",
        }
    )

    assert row["api_base"] == importer.CIGNA_PROVIDER_DIRECTORY_BASE
    assert row["canonical_api_base"] == importer.CIGNA_PROVIDER_DIRECTORY_BASE
    assert row["requires_registration"] is False
    assert row["auth_type"] == "none"
    assert row["last_validated_status"] == "valid_non_fhir"
    assert row["metadata_json"]["provider_directory_override"] == "cigna_public_providerdirectory"
    assert set(row["metadata_json"]["provider_directory_expected_nonempty_resources"]) == (
        importer.CIGNA_EXPECTED_NONEMPTY_RESOURCES
    )
    assert row["metadata_json"]["provider_directory_supported_resources"] == list(
        importer.DEFAULT_RESOURCES
    )
    assert (
        row["metadata_json"]["provider_directory_previous_api_base"]
        == "https://apps.availity.com/availity/public-fhir/fhir/v1/cigna/r4"
    )


def test_source_row_from_seed_overrides_caresource_stale_auth_label():
    observed_row_map = importer._source_row_from_seed(
        {
            "id": "57",
            "org_tin": "31-1703368",
            "org_name": "CareSource",
            "plan_name": "Medicare Advantage, Medicaid MCO",
            "api_base": importer.CARESOURCE_PROVIDER_DIRECTORY_BASE,
            "requires_registration": "true",
            "requires_api_key": "true",
            "auth_type": "OAuth2/SMART",
            "last_validated_status": "auth_required",
            "source": "defacto_2024",
        }
    )

    assert observed_row_map["source_id"] == "pdfhir_b627b38e07cae99151baa4b7"
    assert observed_row_map["api_base"] == importer.CARESOURCE_PROVIDER_DIRECTORY_BASE
    assert observed_row_map["canonical_api_base"] == importer.CARESOURCE_PROVIDER_DIRECTORY_BASE
    assert observed_row_map["requires_registration"] is False
    assert observed_row_map["requires_api_key"] is False
    assert observed_row_map["auth_type"] == "none"
    assert observed_row_map["last_validated_status"] == "valid"
    assert observed_row_map["endpoint_practitioner"] == (
        f"{importer.CARESOURCE_PROVIDER_DIRECTORY_BASE}/Practitioner"
    )
    metadata = observed_row_map["metadata_json"]
    assert metadata["provider_directory_override"] == (
        "caresource_public_provider_directory"
    )
    assert metadata["provider_directory_confirmed_metadata_url"] == (
        importer.CARESOURCE_PROVIDER_DIRECTORY_METADATA_URL
    )
    assert metadata["provider_directory_supported_resources"] == list(
        importer.DEFAULT_RESOURCES
    )
    assert set(metadata["provider_directory_expected_nonempty_resources"]) == (
        importer.CARESOURCE_EXPECTED_NONEMPTY_RESOURCES
    )
    assert metadata["provider_directory_fully_enumerable_resources"] == list(
        importer.DEFAULT_RESOURCES
    )
    assert importer.CARESOURCE_PROVIDER_DIRECTORY_BASE in (
        importer.PAGINATION_CHECKPOINT_API_BASES
    )


def test_caresource_full_refresh_uses_opaque_cursor_census_strategy():
    source_lookup = {
        "source_id": "caresource",
        "api_base": importer.CARESOURCE_PROVIDER_DIRECTORY_BASE,
    }

    assert importer._source_full_refresh_page_count(source_lookup, 100, 0, 0) == 1000
    assert importer._source_full_refresh_page_count(source_lookup, 100, 1, 0) == 100
    checkpoint_context = importer._pagination_checkpoint_context(
        source_lookup,
        ["caresource"],
        run_id="run_1",
        retry_of_run_id=None,
    )
    assert checkpoint_context is not None
    expected_scope_payload = json.dumps(
        {
            "strategy_version": importer.CARESOURCE_OPAQUE_CURSOR_STRATEGY_VERSION,
            "source_ids": ["caresource"],
            "resource_group": importer._resource_import_group_key(source_lookup),
        },
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    assert checkpoint_context.source_scope_hash == hashlib.sha256(
        expected_scope_payload.encode("utf-8")
    ).hexdigest()


def test_source_row_from_seed_overrides_centene_partner_portal_base():
    source_row = importer._source_row_from_seed(
        {
            "id": "centene-1",
            "org_name": "Centene Corporation",
            "plan_name": "Ambetter",
            "api_base": "https://partners.centene.com/apis",
            "auth_type": "OAuth2 Client Credentials",
            "source": "provider-directory-db",
        }
    )

    assert source_row["api_base"] == importer.CENTENE_PROVIDER_DIRECTORY_BASE
    assert source_row["canonical_api_base"] == importer.CENTENE_PROVIDER_DIRECTORY_BASE
    assert source_row["requires_registration"] is False
    assert source_row["auth_type"] == "none"
    assert source_row["metadata_json"]["provider_directory_override"] == "centene_iopc_pd_providerdirectory"
    assert source_row["metadata_json"]["provider_directory_previous_api_base"] == "https://partners.centene.com/apis"
    assert source_row["metadata_json"]["provider_directory_confirmed_catalog_url"] == importer.CENTENE_PARTNER_PORTAL_APIS_URL
    assert source_row["metadata_json"]["provider_directory_replaces_stale_generic_api_bases"] == [
        importer.CENTENE_STALE_GENERIC_BASE
    ]
    assert (
        source_row["metadata_json"]["provider_directory_special_case_california_base"]
        == importer.CENTENE_PROVIDER_DIRECTORY_CALIFORNIA_BASE
    )
    assert source_row["metadata_json"]["provider_directory_coverage_mode"] == "probe_only"
    assert source_row["metadata_json"]["provider_directory_fully_enumerable_resources"] == []
    assert (
        source_row["metadata_json"]["provider_directory_blocked_reason"]
        == importer.PROVIDER_DIRECTORY_PROBE_ONLY_BLOCKED_REASON
    )


def test_source_row_from_seed_overrides_centene_stale_generic_base():
    row = importer._source_row_from_seed(
        {
            "id": "centene-stale",
            "org_name": "Centene Corporation",
            "plan_name": "Provider Directory Retest",
            "api_base": "https://fhir.centene.com/provider-directory/",
            "auth_type": "none",
            "last_validated_status": "unreachable",
            "source": "provider-directory-db-retest",
        }
    )

    assert row["api_base"] == importer.CENTENE_PROVIDER_DIRECTORY_BASE
    assert row["canonical_api_base"] == importer.CENTENE_PROVIDER_DIRECTORY_BASE
    assert row["metadata_json"]["provider_directory_override"] == "centene_iopc_pd_providerdirectory"
    assert row["metadata_json"]["provider_directory_previous_api_base"] == "https://fhir.centene.com/provider-directory/"
    assert row["metadata_json"]["provider_directory_replaces_stale_generic_api_bases"] == [
        importer.CENTENE_STALE_GENERIC_BASE
    ]


def test_source_row_from_seed_overrides_amerihealth_caritas_plan_base_and_endpoints():
    observed_row_map = importer._source_row_from_seed(
        {
            "id": "amerihealth-caritas-dc",
            "org_name": "AmeriHealth Caritas",
            "plan_name": "AmeriHealth Caritas DC",
            "api_base": "https://apps.availity.com/availity/public-fhir/fhir/v1/amerihealthcaritas/r4",
            "endpoint_practitioner": (
                "https://apps.availity.com/availity/public-fhir/fhir/v1/"
                "amerihealthcaritas/r4/Practitioner"
            ),
            "endpoint_location": (
                "https://apps.availity.com/availity/public-fhir/fhir/v1/"
                "amerihealthcaritas/r4/Location"
            ),
            "auth_type": "API Key",
            "source": "provider-directory-db",
        }
    )

    expected_base = "https://api-ext.amerihealthcaritas.com/5400/provider-api"
    assert observed_row_map["api_base"] == expected_base
    assert observed_row_map["canonical_api_base"] == expected_base
    assert observed_row_map["requires_registration"] is False
    assert observed_row_map["auth_type"] == "none"
    assert observed_row_map["last_validated_status"] == "probe_only"
    assert observed_row_map["endpoint_practitioner"] == f"{expected_base}/Practitioner"
    assert observed_row_map["endpoint_location"] == f"{expected_base}/Location"
    assert observed_row_map["metadata_json"]["provider_directory_override"] == "amerihealth_caritas_api_ext_provider_api"
    assert observed_row_map["metadata_json"]["provider_directory_plan_code"] == "5400"
    assert observed_row_map["metadata_json"]["provider_directory_confirmed_metadata_url"] == f"{expected_base}/metadata"
    assert observed_row_map["metadata_json"]["provider_directory_supported_resources"] == list(
        importer.AMERIHEALTH_CARITAS_SUPPORTED_RESOURCES
    )
    assert observed_row_map["metadata_json"]["provider_directory_fully_enumerable_resources"] == []
    assert observed_row_map["metadata_json"]["provider_directory_coverage_mode"] == "probe_only"

    selected_sources, selection_metrics = importer._select_resource_import_sources(
        [observed_row_map],
        valid_source_ids=None,
        open_only=False,
        include_auth_required=True,
    )
    assert selected_sources == []
    assert selection_metrics["source_import_skipped_blocked_source"] == 1
    assert (
        selection_metrics[
            "source_import_skipped_blocked_source_coverage_mode_probe_only"
        ]
        == 1
    )
    assert importer._endpoint_dataset_selected_resources(
        [observed_row_map],
        list(importer.DEFAULT_RESOURCES),
    ) == []


def test_amerihealth_0900_acquisition_is_carrier_level_and_plan_neutral():
    source_row = importer._source_row_from_seed(
        {
            "id": "amerihealth-caritas-0900",
            "org_name": "AmeriHealth Caritas NH",
            "plan_name": "AmeriHealth Caritas NH",
            "api_base": "https://api-ext.amerihealthcaritas.com/0900/provider-api",
            "auth_type": "none",
            "source": "provider-directory-db",
        }
    )

    assert source_row["api_base"] == importer.AMERIHEALTH_CARITAS_CARRIER_PROVIDER_API_BASE
    assert source_row["canonical_api_base"] == importer.AMERIHEALTH_CARITAS_CARRIER_PROVIDER_API_BASE
    assert source_row["org_name"] == importer.AMERIHEALTH_CARITAS_CARRIER_ORG_NAME
    assert source_row["plan_name"] is None
    assert source_row["last_validated_status"] == "valid"
    metadata = source_row["metadata_json"]
    assert metadata["provider_directory_directory_scope"] == "carrier"
    assert metadata["provider_directory_plan_code"] is None
    assert metadata["provider_directory_plan_provenance_neutralized"] is True
    assert metadata["provider_directory_acquisition_enabled"] is True
    assert "provider_directory_acquisition_blocked_reason" not in metadata
    assert metadata["provider_directory_original_alias"] == {
        "plan_code": "0900",
        "org_name": "AmeriHealth Caritas NH",
        "plan_name": "AmeriHealth Caritas NH",
        "api_base": importer.AMERIHEALTH_CARITAS_CARRIER_PROVIDER_API_BASE,
    }
    selected_sources, _metrics = importer._select_resource_import_sources(
        [source_row], valid_source_ids=None, open_only=True, include_auth_required=False
    )
    assert selected_sources == [source_row]


def test_ambiguous_amerihealth_retest_base_is_not_guessed():
    row = importer._source_row_from_seed(
        {
            "id": "amerihealth-caritas-retest",
            "org_name": "AmeriHealth Caritas",
            "plan_name": "Provider Directory Retest",
            "api_base": "https://fhir.amerihealthcaritas.com/provider-directory/",
            "auth_type": "none",
            "last_validated_status": "not_found",
            "source": "provider-directory-db-retest",
        }
    )

    assert row["api_base"] == "https://fhir.amerihealthcaritas.com/provider-directory/"
    assert row["metadata_json"].get("provider_directory_override") is None


def test_amerihealth_caritas_catalog_parser_emits_plan_specific_sources():
    observed_rows = importer._amerihealth_caritas_seed_rows_from_catalog_html(
        """
        <table>
          <tr><td><b>PlanID</b></td><td><b>Plan Name</b></td><td><b>Provider Directory API</b></td></tr>
          <tr>
            <td>5400</td>
            <td>AmeriHealth Caritas District of Columbia</td>
            <td><a href="https://api-ext.amerihealthcaritas.com/5400/provider-api/swagger-ui/">API Documentation</a></td>
            <td><a href="https://api-ext.amerihealthcaritas.com/5400/provider-api/metadata">Capability Statement</a></td>
          </tr>
          <tr>
            <td>5400</td>
            <td>AmeriHealth Caritas District of Columbia</td>
            <td><a href="https://api-ext.amerihealthcaritas.com/5400/provider-api/swagger-ui/">API Documentation</a></td>
          </tr>
          <tr>
            <td>PA02</td>
            <td>AmeriHealth Caritas VIP Care</td>
            <td><a href="https://api-ext.amerihealthcaritas.com/PA02/provider-api/swagger-ui/">API Documentation</a></td>
          </tr>
        </table>
        """,
        source_query="AmeriHealth",
        source_url="fixture.html",
    )

    assert len(observed_rows) == 2
    assert observed_rows[0]["plan_name"] == "AmeriHealth Caritas District of Columbia"
    assert observed_rows[0]["api_base"] == "https://api-ext.amerihealthcaritas.com/5400/provider-api"
    assert observed_rows[0]["source"] == "amerihealth-caritas-developer-portal"
    assert observed_rows[0]["source_url"] == "fixture.html"
    assert observed_rows[1]["plan_name"] == "AmeriHealth Caritas VIP Care"
    assert observed_rows[1]["api_base"] == "https://api-ext.amerihealthcaritas.com/PA02/provider-api"
    assert observed_rows[1]["metadata_json"]["provider_directory_plan_code"] == "PA02"
    assert observed_rows[1]["metadata_json"]["provider_directory_supported_resources"] == list(
        importer.AMERIHEALTH_CARITAS_SUPPORTED_RESOURCES
    )


def test_source_row_from_amerihealth_caritas_catalog_marks_stale_generic_base_replaced():
    row = importer._source_row_from_seed(
        {
            "id": "amerihealth-caritas-0500",
            "org_name": "AmeriHealth Caritas",
            "plan_name": "AmeriHealth Caritas Pennsylvania",
            "api_base": "https://api-ext.amerihealthcaritas.com/0500/provider-api",
            "source": "amerihealth-caritas-developer-portal",
        }
    )

    assert row["api_base"] == "https://api-ext.amerihealthcaritas.com/0500/provider-api"
    assert row["metadata_json"]["provider_directory_replaces_stale_generic_api_bases"] == [
        importer.AMERIHEALTH_CARITAS_STALE_GENERIC_BASE
    ]
    assert row["metadata_json"]["provider_directory_confirmed_catalog_url"] == importer.AMERIHEALTH_CARITAS_DOC_URL


def test_amerihealth_concrete_plan_base_uses_full_harvest_controls():
    api_base = "https://api-ext.amerihealthcaritas.com/PA02/provider-api"
    source_lookup = {"source_id": "ameri-pa02", "api_base": api_base}

    assert importer._amerihealth_caritas_api_plan_code(api_base) == "PA02"
    assert importer._source_full_refresh_page_count(source_lookup, 100, 0, 0) == 250
    assert importer._source_full_refresh_page_count(source_lookup, 100, 1, 0) == 100
    checkpoint_context = importer._pagination_checkpoint_context(
        source_lookup,
        ["ameri-pa02"],
        run_id="run_1",
        retry_of_run_id=None,
    )
    assert checkpoint_context is not None
    assert checkpoint_context.canonical_api_base == api_base


def test_aetna_commercial_checkpoint_scope_includes_strategy_version():
    source_lookup = {
        "source_id": "aetna-commercial",
        "api_base": importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE,
    }

    checkpoint_context = importer._pagination_checkpoint_context(
        source_lookup,
        ["aetna-commercial"],
        run_id="run_1",
        retry_of_run_id=None,
    )
    expected_scope_payload = json.dumps(
        {
            "strategy_version": importer.PAGINATION_CHECKPOINT_STRATEGY_VERSION,
            "source_ids": ["aetna-commercial"],
            "resource_group": importer._resource_import_group_key(source_lookup),
        },
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )

    assert importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE in (
        importer.PAGINATION_CHECKPOINT_API_BASES
    )
    assert checkpoint_context is not None
    assert checkpoint_context.source_scope_hash == hashlib.sha256(
        expected_scope_payload.encode("utf-8")
    ).hexdigest()


def test_synthetic_position_checkpoint_scope_versions_page_guard():
    source_lookup = {
        "source_id": "iowa-medicaid",
        "api_base": importer.IOWA_MEDICAID_PROVIDER_DIRECTORY_BASE,
    }

    checkpoint_context = importer._pagination_checkpoint_context(
        source_lookup,
        ["iowa-medicaid"],
        run_id="run_1",
        retry_of_run_id=None,
    )
    expected_scope_payload = json.dumps(
        {
            "strategy_version": (
                importer.SYNTHETIC_POSITION_PAGINATION_STRATEGY_VERSION
            ),
            "source_ids": ["iowa-medicaid"],
            "resource_group": importer._resource_import_group_key(source_lookup),
        },
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )

    assert checkpoint_context is not None
    assert importer._pagination_checkpoint_strategy_version(
        importer.IOWA_MEDICAID_PROVIDER_DIRECTORY_BASE,
        {},
    ) == importer.SYNTHETIC_POSITION_PAGINATION_STRATEGY_VERSION
    assert checkpoint_context.source_scope_hash == hashlib.sha256(
        expected_scope_payload.encode("utf-8")
    ).hexdigest()


def test_netsmart_checkpoint_scope_versions_page_index_semantics():
    source_lookup = {
        "source_id": "san-mateo",
        "api_base": importer.SAN_MATEO_COUNTY_PROVIDER_DIRECTORY_BASE,
    }

    checkpoint_context = importer._pagination_checkpoint_context(
        source_lookup,
        ["san-mateo"],
        run_id="run_1",
        retry_of_run_id=None,
    )
    expected_scope_payload = json.dumps(
        {
            "strategy_version": (
                importer.NETSMART_PAGE_INDEX_PAGINATION_STRATEGY_VERSION
            ),
            "source_ids": ["san-mateo"],
            "resource_group": importer._resource_import_group_key(source_lookup),
        },
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )

    assert checkpoint_context is not None
    assert importer._pagination_checkpoint_strategy_version(
        importer.SAN_MATEO_COUNTY_PROVIDER_DIRECTORY_BASE,
        {},
    ) == importer.NETSMART_PAGE_INDEX_PAGINATION_STRATEGY_VERSION
    assert checkpoint_context.source_scope_hash == hashlib.sha256(
        expected_scope_payload.encode("utf-8")
    ).hexdigest()


def test_last_updated_partition_checkpoint_scope_includes_partition_identity():
    partition_resources_by_type = {
        "Practitioner": {
            "start": "2024-01-01T00:00:00Z",
            "end": "2024-01-03T00:00:00Z",
            "ceiling": 10,
            "minimum_width_seconds": 3600,
        }
    }
    source_lookup = {
        "source_id": "partitioned-source",
        "api_base": importer.SCAN_PROVIDER_DIRECTORY_BASE,
        "metadata_json": {
            importer.LAST_UPDATED_PARTITION_METADATA_KEY: {
                "enabled": True,
                "resources": partition_resources_by_type,
            }
        },
    }

    checkpoint_context = importer._pagination_checkpoint_context(
        source_lookup,
        ["partitioned-source"],
        run_id="run_1",
        retry_of_run_id=None,
    )
    expected_scope_payload = json.dumps(
        {
            "strategy_version": importer.LAST_UPDATED_PARTITION_STRATEGY_VERSION,
            "source_ids": ["partitioned-source"],
            "resource_group": importer._resource_import_group_key(source_lookup),
            "last_updated_partition_config": partition_resources_by_type,
        },
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )

    assert checkpoint_context is not None
    assert checkpoint_context.source_scope_hash == hashlib.sha256(
        expected_scope_payload.encode("utf-8")
    ).hexdigest()
    partition_resources_by_type["Practitioner"]["boundary_precision_seconds"] = 1
    second_precision_context = importer._pagination_checkpoint_context(
        source_lookup,
        ["partitioned-source"],
        run_id="run_2",
        retry_of_run_id=None,
    )
    assert second_precision_context is not None
    assert (
        second_precision_context.source_scope_hash
        != checkpoint_context.source_scope_hash
    )


def test_idaho_medicaid_supports_durable_pagination_checkpoints():
    source_lookup = {
        "source_id": "idaho-medicaid",
        "api_base": importer.IDAHO_MEDICAID_PROVIDER_DIRECTORY_BASE,
    }

    checkpoint_context = importer._pagination_checkpoint_context(
        source_lookup,
        ["idaho-medicaid"],
        run_id="run_1",
        retry_of_run_id=None,
    )

    assert importer.IDAHO_MEDICAID_PROVIDER_DIRECTORY_BASE in (
        importer.PAGINATION_CHECKPOINT_API_BASES
    )
    assert checkpoint_context is not None
    assert checkpoint_context.canonical_api_base == (
        importer.IDAHO_MEDICAID_PROVIDER_DIRECTORY_BASE
    )


def test_michigan_probe_only_source_supports_opaque_cursor_checkpoints():
    source_lookup = importer._source_row_from_seed(
        {
            "id": "michigan",
            "org_name": "Blue Cross Blue Shield of Michigan",
            "api_base": importer.INTEROPSTATION_MDHHS_PROVIDER_DIRECTORY_BASE,
        }
    )

    checkpoint_context = importer._pagination_checkpoint_context(
        source_lookup,
        ["michigan"],
        run_id="run_1",
        retry_of_run_id=None,
    )

    assert importer.INTEROPSTATION_MDHHS_PROVIDER_DIRECTORY_BASE not in (
        importer.PAGINATION_CHECKPOINT_API_BASES
    )
    assert importer.MICHIGAN_PROVIDER_DIRECTORY_BASE in (
        importer.PAGINATION_CHECKPOINT_API_BASES
    )
    assert checkpoint_context is not None
    assert checkpoint_context.canonical_api_base == (
        importer.MICHIGAN_PROVIDER_DIRECTORY_BASE
    )


def test_contra_costa_catalog_parser_extracts_provider_directory_base_from_external_link():
    rows = importer._contra_costa_seed_rows_from_developer_html(
        """
        <p>
          <a href="/?____isexternal=true&splash=https%3A%2F%2Fihyml0v6d9.execute-api.us-east-1.amazonaws.com%2Fhxprod%2Fmetadata">
            Provider Directory API Base URL
          </a>
        </p>
        """,
        source_query="Contra Costa",
        source_url=importer.CONTRA_COSTA_PROVIDER_DIRECTORY_DOC_URL,
    )

    assert len(rows) == 1
    assert rows[0]["org_name"] == "Contra Costa Health Plan"
    assert rows[0]["api_base"] == "https://ihyml0v6d9.execute-api.us-east-1.amazonaws.com/hxprod"
    assert rows[0]["source"] == "contra-costa-health-developer-page"
    assert rows[0]["is_medicaid_mco"] is True
    assert rows[0]["metadata_json"]["provider_directory_supported_resources"] == list(
        importer.PUBLIC_DIRECTORY_SEVEN_RESOURCES
    )


def test_contra_costa_catalog_uses_confirmed_fallback_when_page_fetch_fails(monkeypatch):
    def fail_read(*_args, **_kwargs):
        raise PermissionError("HTTP 403")

    monkeypatch.setattr(importer, "_read_text_from_path_or_url", fail_read)

    rows, metrics = importer._seed_rows_from_contra_costa_catalog(source_query="Contra Costa")

    assert len(rows) == 1
    assert rows[0]["api_base"] == importer.CONTRA_COSTA_PROVIDER_DIRECTORY_BASE
    assert metrics["rows"] == 1
    assert metrics["fallback"] is True
    assert "HTTP 403" in metrics["error"]


def test_health_partners_plans_supplemental_source_is_queryable_by_org():
    rows = importer._health_partners_plans_seed_rows(source_query="Health Partners Plans")

    assert len(rows) == 1
    assert rows[0]["org_name"] == "Health Partners Plans"
    assert rows[0]["api_base"] == importer.HEALTH_PARTNERS_PLANS_PROVIDER_DIRECTORY_BASE
    assert rows[0]["source"] == "health-partners-plans-fhir-root"


def test_reviewed_candidate_seeds_have_stable_ids_and_acquisition_controls():
    seed_rows = importer._reviewed_provider_directory_candidate_seed_rows()
    source_rows = [
        importer._source_row_from_seed(seed_record)
        for seed_record in seed_rows
    ]
    source_by_base = {
        source_row["canonical_api_base"]: source_row for source_row in source_rows
    }

    assert len(source_rows) == 8
    assert {
        source_row["source_id"] for source_row in source_rows
    } == {
        "pdfhir_fa3c581c79ffdfa5b73b43b9",
        "pdfhir_5fd73a1218a9b5b73066d000",
        "pdfhir_29c125ae13aeffa0afe0e121",
        "pdfhir_0c52480afd14b4c89386df94",
        "pdfhir_0d7938920b8933d55c1777ae",
        "pdfhir_46d7d27acc677651b0d3516e",
        "pdfhir_e3d82f2cd0a10ab4463dbdda",
        "pdfhir_95d5ad93c689d016351dd088",
    }
    blocked_candidate_bases = {
        importer.CAPITAL_BLUE_CROSS_PROVIDER_DIRECTORY_BASE,
        importer.EL_DORADO_COUNTY_PROVIDER_DIRECTORY_BASE,
    }
    for source_row in source_rows:
        metadata = source_row["metadata_json"]
        if source_row["canonical_api_base"] in blocked_candidate_bases:
            assert metadata["provider_directory_coverage_mode"] == "probe_only"
            assert metadata["provider_directory_acquisition_enabled"] is False
            assert metadata["provider_directory_fully_enumerable_resources"] == []
            assert importer._resource_acquisition_blocked_reason(source_row) == (
                importer.PROVIDER_DIRECTORY_PROBE_ONLY_BLOCKED_REASON
            )
        else:
            assert metadata["provider_directory_coverage_mode"] == "full"
            assert metadata["provider_directory_acquisition_enabled"] is True
            assert metadata["provider_directory_fully_enumerable_resources"]
            assert metadata["provider_directory_candidate_status"] in {
                importer.PROVIDER_DIRECTORY_TWIN_ROOT_PENDING,
                importer.PROVIDER_DIRECTORY_TWIN_ROOT_VERIFIED,
            }
            assert importer._resource_acquisition_blocked_reason(source_row) is None

    el_dorado_source = source_by_base[
        importer.EL_DORADO_COUNTY_PROVIDER_DIRECTORY_BASE
    ]
    assert el_dorado_source["requires_registration"] is True
    assert "registration and approval" in el_dorado_source["metadata_json"][
        "provider_directory_acquisition_blocked_reason"
    ]
    assert importer._reviewed_provider_directory_candidate_seed_rows(
        source_query="Devoted"
    )[0]["api_base"] == importer.DEVOTED_PROVIDER_DIRECTORY_BASE


def test_reviewed_candidate_statuses_match_completed_twin_campaigns():
    """Admit only sources whose two exhaustive roots matched exactly."""
    source_rows = [
        importer._source_row_from_seed(seed_row)
        for seed_row in importer._reviewed_provider_directory_candidate_seed_rows()
    ]
    status_by_base = {
        source_row["canonical_api_base"]: source_row["metadata_json"].get(
            "provider_directory_candidate_status"
        )
        for source_row in source_rows
    }

    verified_status = importer.PROVIDER_DIRECTORY_TWIN_ROOT_VERIFIED
    assert status_by_base[importer.DEVOTED_PROVIDER_DIRECTORY_BASE] == verified_status
    assert status_by_base[importer.SIMPRA_PROVIDER_DIRECTORY_BASE] == verified_status
    assert (
        status_by_base[importer.SAN_BERNARDINO_COUNTY_PROVIDER_DIRECTORY_BASE]
        == verified_status
    )
    assert (
        status_by_base[importer.SAN_MATEO_COUNTY_PROVIDER_DIRECTORY_BASE]
        == verified_status
    )
    pending_status = importer.PROVIDER_DIRECTORY_TWIN_ROOT_PENDING
    assert (
        status_by_base[importer.IOWA_MEDICAID_PROVIDER_DIRECTORY_BASE]
        == verified_status
    )
    assert (
        status_by_base[importer.PENNSYLVANIA_MEDICAID_PROVIDER_DIRECTORY_BASE]
        == verified_status
    )


@pytest.mark.asyncio
async def test_reverse_lookup_seed_producer_closes_every_idle_worker(monkeypatch):
    """Close every worker queue even when a source yields no lookup seeds."""

    async def no_seed_rows(*_args, **_kwargs):
        if False:
            yield None

    monkeypatch.setattr(
        importer,
        "_iter_scan_practitioner_role_seed_rows",
        no_seed_rows,
    )
    options = importer.ScanPractitionerRoleFetchOptions(0, 0, 0, 1, "run-1")
    request = importer._PractitionerRoleReverseLookupRequest(
        source={},
        rows_by_resource={},
        options=options,
        resource_timeout=1,
        deadline_at=None,
    )
    state = importer._PractitionerRoleReverseLookupState(
        result_model=object,
        retained_resource_rows=[],
        streamed_rows=importer._StreamedResourceRowBuffer(object, None, 1, []),
        retain_rows=True,
        page_limit=0,
    )
    seed_queue = asyncio.Queue()

    await state.produce_seed_rows(request, seed_queue, worker_count=2)

    assert [await seed_queue.get(), await seed_queue.get()] == [None, None]


def test_reverse_lookup_checkpoint_row_requires_canonical_base():
    """Exercise the fail-closed checkpoint identity guard."""
    with pytest.raises(
        ValueError,
        match="reverse lookup checkpoint requires a canonical API base",
    ):
        importer._reverse_lookup_checkpoint_row(
            {},
            "Practitioner",
            "seed-1",
            "run-1",
        )


def test_reviewed_candidate_rejects_missing_verification_campaign(monkeypatch):
    """Keep acquisition disabled when a reviewed source has no campaign."""
    monkeypatch.setattr(
        importer,
        "REVIEWED_PROVIDER_DIRECTORY_CAMPAIGN_BY_SEED_ID",
        {},
    )

    with pytest.raises(
        RuntimeError,
        match="provider_directory_reviewed_candidate_campaign_missing",
    ):
        importer._reviewed_provider_directory_candidate_seed_rows()


def test_iowa_reviewed_candidate_current_expected_nonempty_contract():
    """Guard populated Iowa collections without rejecting its empty Location."""
    seed_row = importer._reviewed_provider_directory_candidate_seed_rows(
        source_query="Iowa Medicaid"
    )[0]
    source_row = importer._source_row_from_seed(seed_row)
    expected_nonempty_resources = source_row["metadata_json"][
        "provider_directory_expected_nonempty_resources"
    ]

    assert set(expected_nonempty_resources) == (
        importer.IOWA_MEDICAID_EXPECTED_NONEMPTY_RESOURCES
    )
    assert "Location" not in expected_nonempty_resources


def test_el_dorado_candidate_prefers_registered_base_and_keeps_legacy_identity():
    """The current auth-gated endpoint must not erase stable legacy identity."""
    seed_row = importer._reviewed_provider_directory_candidate_seed_rows(
        source_query="El Dorado"
    )[0]
    source_row = importer._source_row_from_seed(seed_row)
    metadata = source_row["metadata_json"]

    assert source_row["source_id"] == "pdfhir_0d7938920b8933d55c1777ae"
    assert source_row["canonical_api_base"] == (
        importer.EL_DORADO_COUNTY_PROVIDER_DIRECTORY_BASE
    )
    assert source_row["requires_registration"] is True
    assert source_row["auth_type"] == "oauth2"
    assert source_row["last_validated_status"] == "auth_required"
    assert metadata["provider_directory_confirmed_catalog_url"] == (
        importer.EL_DORADO_COUNTY_DEVELOPER_RESOURCES_URL
    )
    assert metadata["provider_directory_confirmed_doc_url"] == (
        importer.EL_DORADO_COUNTY_PROVIDER_DIRECTORY_DOCUMENT_URL
    )
    assert metadata[
        "provider_directory_documented_practitioner_probe_status"
    ] == 401
    assert metadata["provider_directory_legacy_probe_base"] == (
        importer.EL_DORADO_COUNTY_LEGACY_PROVIDER_DIRECTORY_BASE
    )
    assert metadata[
        "provider_directory_legacy_practitioner_probe_status"
    ] == 200
    assert metadata["provider_directory_legacy_probe_scope"] == "bounded_only"


def test_provider_directory_blocker_rows_are_non_importable_catalog_sources():
    rows = importer._provider_directory_blocker_seed_rows(source_query="Puerto Rico")

    assert len(rows) == 1
    seed_row = rows[0]
    assert seed_row["org_name"] == "Territory of Puerto Rico"
    assert seed_row["api_base"] is None
    assert seed_row["last_validated_status"] == importer.PROVIDER_DIRECTORY_CATALOG_BLOCKED_STATUS
    assert seed_row["metadata_json"]["provider_directory_blocked"] is True
    assert seed_row["metadata_json"]["provider_directory_blocked_acquisition_method"] == (
        "not-importable"
    )
    assert seed_row["metadata_json"]["provider_directory_live_verification"] == {
        "status": "not_recorded",
        "checked_at": None,
    }

    source_row = importer._source_row_from_seed(seed_row)
    selected, metrics = importer._select_resource_import_sources(
        [source_row],
        valid_source_ids=None,
        open_only=True,
        include_auth_required=True,
    )
    assert selected == []
    assert metrics["source_import_skipped_missing_api_base"] == 1


def test_provider_directory_blocker_runtime_rejects_non_v2_registry(
    monkeypatch,
    tmp_path,
):
    registry = json.loads(
        importer.PROVIDER_DIRECTORY_BLOCKER_REGISTRY_PATH.read_text(encoding="utf-8")
    )
    registry["schema_version"] = 1
    registry_path = tmp_path / "provider-directory-blockers.json"
    registry_path.write_text(json.dumps(registry), encoding="utf-8")
    monkeypatch.setattr(
        importer,
        "PROVIDER_DIRECTORY_BLOCKER_REGISTRY_PATH",
        registry_path,
    )

    with pytest.raises(RuntimeError, match="blocker registry is invalid"):
        importer._provider_directory_blocker_seed_rows()


def _cms_sma_fixture_csv() -> str:
    """Verify this provider-directory regression contract."""
    section = [""] * 34
    section[0] = "State Medicaid Agency Interoperability and Patient Access Endpoint Directory"
    section[22] = "Provider Directory Endpoint Information "
    header = [""] * 34
    header[0] = "State"
    header[1] = "Information as of date\n(Date Completing the Survey)"
    header[22] = "Provider Directory Production Base URL"
    header[23] = "Status \n(drop-down list)"
    header[25] = "FHIR Capability Statement Link"
    header[26] = "Data Refresh Frequency (e.g. Real-Time, Hourly, Daily, Weekly, Monthly) "
    header[27] = "Is the API Public?\n(Y/N drop-down list) "
    header[28] = "FHIR Version\n(drop-down list)\n"

    def row(
        state: str,
        *,
        status: str,
        public: str,
        production_urls: str,
        capability_url: str,
        refresh: str = "Weekly",
    ) -> list[str]:
        item = [""] * 34
        item[0] = state
        item[1] = "4/28/2023"
        item[22] = production_urls
        item[23] = status
        item[25] = capability_url
        item[26] = refresh
        item[27] = public
        item[28] = "4.0.1"
        return item

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(section)
    writer.writerow(header)
    writer.writerow(
        row(
            "Alaska",
            status="Active",
            public="Yes",
            production_urls=(
                "https://api.alaskafhir.com/r4/public/Practitioner\n\n"
                "https://api.alaskafhir.com/r4/public/Organization\n\n"
                "https://api.alaskafhir.com/r4/public/Location"
            ),
            capability_url="https://api.alaskafhir.com/r4/metadata",
            refresh="Within 24 hours after claims adjudication. Normal cycle is weekly.",
        )
    )
    writer.writerow(
        row(
            "Alabama",
            status="Active",
            public="No",
            production_urls="https://api.esante.us/provider/fhir/Practitioner",
            capability_url="https://api.esante.us/provider/fhir/Practitioner/metadata",
        )
    )
    writer.writerow(
        row(
            "Oregon",
            status="In Development",
            public="Yes",
            production_urls="https://api.oregonfhir.com",
            capability_url="https://api.oregonfhir.com/r4/metadata",
        )
    )
    writer.writerow(
        row(
            "Pennsylvania",
            status="Active",
            public="Yes",
            production_urls=(
                "https://fite.pa-prd.gw03.abacusinsights.ai/provider-directory/Organization?_format=json\n"
                "https://fite.pa-prd.gw03.abacusinsights.ai/provider-directory/Practitioner?_format=json"
            ),
            capability_url=(
                "https://fite.pa-prd.gw03.abacusinsights.ai/carin-bb/metadata\n"
                "https://fite.pa-prd.gw03.abacusinsights.ai/provider-directory/metadata\n"
                "https://fite.pa-prd.gw03.abacusinsights.ai/open-formulary/metadata"
            ),
        )
    )
    return output.getvalue()


def test_cms_sma_endpoint_directory_parser_emits_public_active_catalog_sources():
    observed_rows = importer._cms_sma_endpoint_directory_seed_rows_from_csv(
        _cms_sma_fixture_csv(),
        source_url="fixture.csv",
    )

    assert len(observed_rows) == 3
    row_by_org = {observed_row_map["org_name"]: observed_row_map for observed_row_map in observed_rows}
    observed_row_map = row_by_org["State of Alaska"]
    assert observed_row_map["org_name"] == "State of Alaska"
    assert observed_row_map["plan_name"] == "Alaska Medicaid Provider Directory"
    assert observed_row_map["api_base"] == "https://api.alaskafhir.com/r4"
    assert observed_row_map["endpoint_practitioner"] == "https://api.alaskafhir.com/r4/public/Practitioner"
    assert observed_row_map["endpoint_organization"] == "https://api.alaskafhir.com/r4/public/Organization"
    assert observed_row_map["endpoint_location"] == "https://api.alaskafhir.com/r4/public/Location"
    assert observed_row_map["last_validated_status"] == importer.CMS_SMA_CATALOG_VALIDATION_STATUS
    assert observed_row_map["source"] == importer.CMS_SMA_ENDPOINT_DIRECTORY_SOURCE
    assert observed_row_map["metadata_json"]["provider_directory_confirmed_metadata_url"] == (
        "https://api.alaskafhir.com/r4/metadata"
    )
    assert len(observed_row_map["data_quality_checked"]) > 64
    source_row = importer._source_row_from_seed(observed_row_map)
    assert source_row["data_quality_checked"] == observed_row_map["data_quality_checked"]
    assert (
        str(
            ProviderDirectorySource.__table__.c.data_quality_checked.type.compile(
                dialect=postgresql.dialect()
            )
        ).upper()
        == "TEXT"
    )
    assert "https://api.alaskafhir.com/r4/public" in observed_row_map["metadata_json"][
        "provider_directory_equivalent_api_bases"
    ]
    pennsylvania = row_by_org["State of Pennsylvania"]
    assert pennsylvania["api_base"] == "https://fite.pa-prd.gw03.abacusinsights.ai/provider-directory"
    assert pennsylvania["metadata_json"]["provider_directory_confirmed_metadata_url"] == (
        "https://fite.pa-prd.gw03.abacusinsights.ai/provider-directory/metadata"
    )
    oregon = row_by_org["State of Oregon"]
    assert oregon["api_base"] == "https://api.oregonfhir.com/r4"
    assert oregon["last_validated_status"] == importer.CMS_SMA_CATALOG_IN_DEVELOPMENT_STATUS
    assert oregon["metadata_json"]["provider_directory_catalog_status"] == "In Development"


def test_cms_sma_catalog_sources_need_live_probe_before_resource_import():
    seed_row = importer._cms_sma_endpoint_directory_seed_rows_from_csv(
        _cms_sma_fixture_csv(),
        source_url="fixture.csv",
    )[0]
    source_row = importer._source_row_from_seed(seed_row)

    assert source_row["metadata_json"]["provider_directory_source_catalog"] == (
        importer.CMS_SMA_ENDPOINT_DIRECTORY_SOURCE
    )
    assert importer._candidate_metadata_urls(source_row)[0] == (
        "https://api.alaskafhir.com/r4",
        "https://api.alaskafhir.com/r4/metadata",
    )

    selected, metrics = importer._select_resource_import_sources(
        [source_row],
        valid_source_ids=None,
        open_only=True,
        include_auth_required=False,
    )
    assert selected == []
    assert metrics["source_import_skipped_validation_status"] == 1

    selected, metrics = importer._select_resource_import_sources(
        [source_row],
        valid_source_ids={source_row["source_id"]},
        open_only=True,
        include_auth_required=False,
    )
    assert selected == [source_row]
    assert metrics["source_import_sources_selected_live_probe_valid"] == 1


def test_source_row_from_seed_overrides_molina_developer_portal_base():
    row = importer._source_row_from_seed(
        {
            "id": "molina-ca",
            "org_name": "Molina Healthcare",
            "plan_name": "Molina CA",
            "api_base": "https://developer.interop.molinahealthcare.com",
            "auth_type": "API Key",
            "requires_registration": "true",
            "source": "provider-directory-db",
        }
    )

    assert row["api_base"] == importer.MOLINA_PROVIDER_DIRECTORY_BASE
    assert row["canonical_api_base"] == importer.MOLINA_PROVIDER_DIRECTORY_BASE
    assert row["requires_registration"] is False
    assert row["auth_type"] == "none"
    assert row["last_validated_status"] == "valid"
    assert row["endpoint_practitioner"] == f"{importer.MOLINA_PROVIDER_DIRECTORY_BASE}/Practitioner"
    assert row["metadata_json"]["provider_directory_override"] == "molina_interop_providerdirectory"
    assert row["metadata_json"]["provider_directory_confirmed_metadata_url"] == (
        importer.MOLINA_PROVIDER_DIRECTORY_METADATA_URL
    )
    assert importer._resource_start_url(row, "Practitioner", page_count=100) == (
        f"{importer.MOLINA_PROVIDER_DIRECTORY_BASE}/Practitioner?_count=100"
    )
    assert importer._resource_start_url(row, "InsurancePlan", page_count=100) is None
    assert importer._resource_start_url(row, "Endpoint", page_count=100) is None


def test_source_row_from_seed_overrides_molina_dead_fhir_host():
    row = importer._source_row_from_seed(
        {
            "id": "molina-retest",
            "org_name": "Molina Healthcare",
            "plan_name": "Provider Directory Retest",
            "api_base": "https://fhir.molinahealthcare.com/provider-directory/",
            "auth_type": "none",
            "last_validated_status": "unreachable",
            "source": "provider-directory-db-retest",
        }
    )

    assert row["api_base"] == importer.MOLINA_PROVIDER_DIRECTORY_BASE
    assert row["last_validated_status"] == "valid"
    assert row["metadata_json"]["provider_directory_override"] == "molina_interop_providerdirectory"
    assert (
        row["metadata_json"]["provider_directory_previous_api_base"]
        == "https://fhir.molinahealthcare.com/provider-directory/"
    )


def test_source_row_from_seed_overrides_uhc_interoperability_landing_page():
    source_row = importer._source_row_from_seed(
        {
            "id": "uhc-1",
            "org_name": "UnitedHealthcare",
            "plan_name": "UHC Community Plan",
            "api_base": "https://www.uhc.com/legal/interoperability-apis",
            "auth_type": "OAuth2 Client Credentials",
            "requires_registration": "true",
            "source": "provider-directory-db",
        }
    )

    assert source_row["api_base"] == importer.UHC_PROVIDER_DIRECTORY_BASE
    assert source_row["canonical_api_base"] == importer.UHC_PROVIDER_DIRECTORY_BASE
    assert source_row["requires_registration"] is False
    assert source_row["auth_type"] == "none"
    assert source_row["last_validated_status"] == "valid"
    assert source_row["metadata_json"]["provider_directory_override"] == "uhc_flex_optum_fhirpublic_r4"
    assert source_row["metadata_json"]["provider_directory_previous_api_base"] == importer.UHC_INTEROPERABILITY_APIS_URL
    assert source_row["metadata_json"]["provider_directory_confirmed_metadata_url"] == importer.UHC_PROVIDER_DIRECTORY_METADATA_URL
    assert source_row["metadata_json"]["provider_directory_resource_page_count_caps"] == {
        "InsurancePlan": 1,
    }
    assert source_row["metadata_json"]["provider_directory_supported_resources"] == list(
        importer.UHC_SUPPORTED_RESOURCES
    )
    assert source_row["metadata_json"]["provider_directory_coverage_mode"] == "probe_only"
    assert source_row["metadata_json"]["provider_directory_fully_enumerable_resources"] == []
    assert source_row["metadata_json"]["provider_directory_acquisition_enabled"] is False
    assert (
        importer._resource_acquisition_blocked_reason(source_row)
        == importer.PROVIDER_DIRECTORY_PROBE_ONLY_BLOCKED_REASON
    )
    assert importer._resource_start_url(source_row, "HealthcareService", page_count=100) is None
    assert importer._resource_start_url(source_row, "Endpoint", page_count=100) is None


def test_source_row_from_seed_overrides_uhc_dead_fhir_host():
    row = importer._source_row_from_seed(
        {
            "id": "uhc-dead-fhir",
            "org_name": "UnitedHealthcare",
            "plan_name": "Provider Directory Retest",
            "api_base": "https://fhir.uhc.com/v1/provider-directory/",
            "auth_type": "none",
            "requires_registration": "false",
            "last_validated_status": "unreachable",
            "source": "provider-directory-db-retest",
        }
    )

    assert row["api_base"] == importer.UHC_PROVIDER_DIRECTORY_BASE
    assert row["canonical_api_base"] == importer.UHC_PROVIDER_DIRECTORY_BASE
    assert row["requires_registration"] is False
    assert row["auth_type"] == "none"
    assert row["last_validated_status"] == "valid"
    assert row["metadata_json"]["provider_directory_override"] == "uhc_flex_optum_fhirpublic_r4"
    assert (
        row["metadata_json"]["provider_directory_previous_api_base"]
        == "https://fhir.uhc.com/v1/provider-directory/"
    )
    assert row["metadata_json"]["provider_directory_confirmed_metadata_url"] == importer.UHC_PROVIDER_DIRECTORY_METADATA_URL


def test_source_resource_timeout_uses_uhc_floor():
    assert (
        importer._source_resource_timeout(
            {
                "api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
                "canonical_api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
            },
            "InsurancePlan",
            10,
        )
        == 60
    )
    assert (
        importer._source_resource_timeout(
            {"api_base": "https://example.test/fhir"},
            "InsurancePlan",
            10,
        )
        == 10
    )


def test_source_row_from_seed_overrides_humana_stale_oauth_label():
    source_row = importer._source_row_from_seed(
        {
            "id": "humana-1",
            "org_name": "Humana Inc.",
            "plan_name": "Humana Medicare Advantage",
            "api_base": "https://fhir.humana.com/api/provider-directory/",
            "auth_type": "OAuth2 Client Credentials",
            "requires_registration": "true",
            "source": "provider-directory-db",
        }
    )

    assert source_row["api_base"] == importer.HUMANA_PROVIDER_DIRECTORY_BASE
    assert source_row["canonical_api_base"] == importer.HUMANA_PROVIDER_DIRECTORY_BASE
    assert source_row["requires_registration"] is False
    assert source_row["auth_type"] == "none"
    assert source_row["last_validated_status"] == "valid"
    assert source_row["plan_name"] is None
    metadata = source_row["metadata_json"]
    assert metadata["provider_directory_override"] == "humana_public_fhir_api"
    assert (
        metadata["provider_directory_confirmed_metadata_url"]
        == importer.HUMANA_PROVIDER_DIRECTORY_METADATA_URL
    )
    assert metadata["provider_directory_directory_scope"] == "carrier"
    assert metadata["provider_directory_coverage_mode"] == "carrier_directory"
    assert metadata["provider_directory_plan_provenance_neutralized"] is True
    assert metadata["provider_directory_fully_enumerable_resources"] == [
        "InsurancePlan",
        "Location",
        "Organization",
        "Practitioner",
        "PractitionerRole",
    ]
    assert metadata["provider_directory_original_alias"] == {
        "org_name": "Humana Inc.",
        "plan_name": "Humana Medicare Advantage",
        "api_base": "https://fhir.humana.com/api/provider-directory",
    }
    assert importer._resource_start_url(source_row, "InsurancePlan", page_count=100) == (
        f"{importer.HUMANA_PROVIDER_DIRECTORY_BASE}/InsurancePlan?_count=100"
    )
    assert (
        importer._resource_start_url(
            source_row, "OrganizationAffiliation", page_count=100
        )
        is None
    )
    assert (
        importer._resource_start_url(source_row, "Endpoint", page_count=100)
        is None
    )


def test_source_row_from_seed_normalizes_iehp_double_slash_endpoints():
    row = importer._source_row_from_seed(
        {
            "id": "iehp-double-slash",
            "org_name": "Inland Empire Health Plan",
            "plan_name": "IEHP Provider Directory",
            "api_base": "https://fhir.iehp.org/provider-directory/",
            "endpoint_practitioner": (
                "https://fhir.iehp.org/provider-directory//Practitioner"
            ),
            "auth_type": "none",
            "source": "provider-directory-db",
        }
    )

    assert row["api_base"] == importer.IEHP_PROVIDER_DIRECTORY_BASE
    assert row["canonical_api_base"] == importer.IEHP_PROVIDER_DIRECTORY_BASE
    assert row["endpoint_practitioner"] == (
        f"{importer.IEHP_PROVIDER_DIRECTORY_BASE}/Practitioner"
    )
    assert row["metadata_json"]["provider_directory_override"] == (
        "iehp_public_provider_directory"
    )
    assert importer._resource_start_url(row, "HealthcareService", page_count=100) == (
        f"{importer.IEHP_PROVIDER_DIRECTORY_BASE}/HealthcareService?_count=100"
    )
    assert importer._resource_start_url(row, "OrganizationAffiliation", page_count=100) is None
    assert importer._resource_start_url(row, "Endpoint", page_count=100) is None


def test_source_row_from_seed_overrides_tmhp_stale_oauth_label():
    row = importer._source_row_from_seed(
        {
            "id": "tmhp-1",
            "org_name": "Blue Cross and Blue Shield of Texas",
            "plan_name": "Provider Directory Retest",
            "api_base": importer.TMHP_PROVIDER_DIRECTORY_BASE,
            "auth_type": "OAuth2/SMART",
            "requires_registration": "true",
            "source": "provider-directory-db",
        }
    )

    assert row["api_base"] == importer.TMHP_PROVIDER_DIRECTORY_BASE
    assert row["canonical_api_base"] == importer.TMHP_PROVIDER_DIRECTORY_BASE
    assert row["requires_registration"] is False
    assert row["auth_type"] == "none"
    assert row["metadata_json"]["provider_directory_override"] == "tmhp_public_providerdirectory"
    assert row["metadata_json"]["provider_directory_confirmed_metadata_url"] == importer.TMHP_PROVIDER_DIRECTORY_METADATA_URL
    assert row["metadata_json"]["provider_directory_supported_resources"] == list(
        importer.PUBLIC_DIRECTORY_SEVEN_RESOURCES
    )
    assert importer._resource_start_url(row, "Practitioner", page_count=100) == (
        f"{importer.TMHP_PROVIDER_DIRECTORY_BASE}/Practitioner?"
        "_count=100&_sort=_id&_offset=0"
    )
    assert importer._resource_start_url(row, "Endpoint", page_count=100) is None


def test_source_row_from_seed_overrides_nebraska_dhhs_stale_oauth_label():
    source_row = importer._source_row_from_seed(
        {
            "id": "ne-dhhs-1",
            "org_name": "State of Nebraska",
            "plan_name": "Provider Directory Retest",
            "api_base": importer.NEBRASKA_DHHS_PROVIDER_DIRECTORY_BASE,
            "auth_type": "OAuth2/SMART",
            "requires_registration": "true",
            "source": "provider-directory-db",
        }
    )

    assert source_row["api_base"] == importer.NEBRASKA_DHHS_PROVIDER_DIRECTORY_BASE
    assert source_row["canonical_api_base"] == importer.NEBRASKA_DHHS_PROVIDER_DIRECTORY_BASE
    assert source_row["requires_registration"] is False
    assert source_row["auth_type"] == "none"
    assert source_row["metadata_json"]["provider_directory_override"] == "nebraska_dhhs_public_providerdirectory"
    assert (
        source_row["metadata_json"]["provider_directory_confirmed_metadata_url"]
        == importer.NEBRASKA_DHHS_PROVIDER_DIRECTORY_METADATA_URL
    )
    assert source_row["metadata_json"]["provider_directory_supported_resources"] == list(
        importer.PUBLIC_DIRECTORY_SEVEN_RESOURCES
    )
    assert importer._resource_start_url(source_row, "Practitioner", page_count=100) == (
        f"{importer.NEBRASKA_DHHS_PROVIDER_DIRECTORY_BASE}/Practitioner?"
        "_count=100&_sort=_id&_offset=0"
    )
    assert importer._resource_start_url(source_row, "Endpoint", page_count=100) is None


def test_source_row_from_seed_restricts_maine_to_public_resource_subset():
    row = importer._source_row_from_seed(
        {
            "id": "maine-1",
            "org_name": "State of Maine",
            "plan_name": "Maine Medicaid Provider Directory",
            "api_base": importer.MAINE_PROVIDER_DIRECTORY_BASE,
            "auth_type": "OAuth2/SMART",
            "requires_registration": "true",
            "source": "provider-directory-db",
        }
    )

    assert row["requires_registration"] is False
    assert row["auth_type"] == "none"
    assert row["last_validated_status"] == "valid"
    assert row["metadata_json"]["provider_directory_supported_resources"] == list(
        importer.MAINE_SUPPORTED_RESOURCES
    )
    assert importer._resource_start_url(row, "PractitionerRole", page_count=100) == (
        f"{importer.MAINE_PROVIDER_DIRECTORY_BASE}/PractitionerRole?_count=100"
    )
    assert importer._resource_start_url(row, "InsurancePlan", page_count=100) is None
    assert importer._resource_start_url(row, "HealthcareService", page_count=100) is None
    assert importer._resource_start_url(row, "Endpoint", page_count=100) is None


def test_arkansas_uses_stable_synthetic_skip_pagination():
    source_lookup = {
        "source_id": "arkansas",
        "api_base": importer.ARKANSAS_PROVIDER_DIRECTORY_BASE,
    }
    start_url = importer._resource_start_url(
        source_lookup,
        "Practitioner",
        page_count=25,
    )

    assert start_url == (
        f"{importer.ARKANSAS_PROVIDER_DIRECTORY_BASE}/Practitioner?"
        "_count=25&_sort=_id&_skip=0"
    )
    assert importer._synthetic_skip_pagination_next_url(
        source_lookup,
        start_url,
        25,
    ) == start_url.replace("_skip=0", "_skip=25")
    assert (
        importer._synthetic_skip_pagination_next_url(
            source_lookup,
            start_url,
            24,
        )
        is None
    )


@pytest.mark.parametrize(
    "api_base",
    (
        importer.IOWA_MEDICAID_PROVIDER_DIRECTORY_BASE,
        importer.PENNSYLVANIA_MEDICAID_PROVIDER_DIRECTORY_BASE,
    ),
)
def test_new_abacus_state_candidates_use_throttled_synthetic_skip(api_base):
    source_lookup = {"source_id": "state-candidate", "api_base": api_base}
    start_url = importer._resource_start_url(
        source_lookup,
        "PractitionerRole",
        page_count=100,
    )

    assert start_url == (
        f"{api_base}/PractitionerRole?_count=100&_sort=_id&_skip=0"
    )
    assert importer._synthetic_skip_pagination_next_url(
        source_lookup,
        start_url,
        100,
    ) == start_url.replace("_skip=0", "_skip=100")
    assert importer._source_request_interval_seconds(source_lookup) == 1.0
    assert importer._pagination_checkpoint_context(
        source_lookup,
        ["state-candidate"],
        run_id="run_candidate",
        retry_of_run_id=None,
    ) is not None


@pytest.mark.parametrize(
    "api_base",
    tuple(sorted(importer.NETSMART_PROVIDER_DIRECTORY_BASES)),
)
def test_netsmart_candidates_advance_synthetic_page_index(api_base):
    source_lookup = {"source_id": "county-candidate", "api_base": api_base}
    start_url = importer._resource_start_url(
        source_lookup,
        "Practitioner",
        page_count=100,
    )

    assert start_url == (
        f"{api_base}/Practitioner?_count=100&_sort=_id&_offset=0"
    )
    assert importer._synthetic_offset_pagination_next_url(
        source_lookup,
        start_url,
        100,
    ) == start_url.replace("_offset=0", "_offset=1")
    assert (
        importer._synthetic_offset_pagination_next_url(
            source_lookup,
            start_url,
            99,
        )
        is None
    )


@pytest.mark.asyncio
async def test_netsmart_fetch_advances_page_index_and_ignores_next_link(monkeypatch):
    api_base = importer.SAN_BERNARDINO_COUNTY_PROVIDER_DIRECTORY_BASE
    source_lookup = {"source_id": "county-candidate", "api_base": api_base}
    requested_urls: list[str] = []

    async def fake_fetch_source_json(_source, request_url, *, timeout):
        requested_urls.append(request_url)
        offset = urllib.parse.parse_qs(
            urllib.parse.urlsplit(request_url).query
        )["_offset"][0]
        resource_ids = ["org-1", "org-2"] if offset == "0" else ["org-3"]
        return (
            200,
            {
                "resourceType": "Bundle",
                "type": "searchset",
                "entry": [
                    {
                        "resource": {
                            "resourceType": "Organization",
                            "id": resource_id,
                        }
                    }
                    for resource_id in resource_ids
                ],
                "link": [
                    {
                        "relation": "next",
                        "url": (
                            f"{api_base}/Organization?"
                            "_count=2&_sort=_id&_offset=1"
                        ),
                    }
                ],
            },
            None,
            5,
        )

    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_source_json)

    fetch_result = await importer._fetch_resource_rows(
        source_lookup,
        "Organization",
        per_resource_limit=0,
        page_limit=0,
        page_count=2,
        timeout=3,
        run_id=None,
    )

    assert fetch_result is not None
    assert fetch_result.complete is True
    assert fetch_result.rows_fetched == 3
    assert requested_urls == [
        f"{api_base}/Organization?_count=2&_sort=_id&_offset=0",
        f"{api_base}/Organization?_count=2&_sort=_id&_offset=1",
    ]


@pytest.mark.asyncio
async def test_netsmart_fetch_fails_closed_on_repeated_full_page(monkeypatch):
    api_base = importer.SAN_BERNARDINO_COUNTY_PROVIDER_DIRECTORY_BASE
    source_lookup = {"source_id": "county-candidate", "api_base": api_base}
    requested_urls: list[str] = []

    async def fake_fetch_source_json(_source, request_url, *, timeout):
        requested_urls.append(request_url)
        return (
            200,
            {
                "resourceType": "Bundle",
                "type": "searchset",
                "entry": [
                    {
                        "resource": {
                            "resourceType": "Organization",
                            "id": resource_id,
                        }
                    }
                    for resource_id in ("org-1", "org-2")
                ],
            },
            None,
            timeout,
        )

    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_source_json)

    fetch_result = await importer._fetch_resource_rows(
        source_lookup,
        "Organization",
        per_resource_limit=0,
        page_limit=0,
        page_count=2,
        timeout=3,
        run_id=None,
    )

    assert fetch_result is not None
    assert fetch_result.complete is False
    assert fetch_result.error == "pagination_page_repeated"
    assert fetch_result.rows_fetched == 2
    assert fetch_result.pages_fetched == 2
    assert requested_urls == [
        f"{api_base}/Organization?_count=2&_sort=_id&_offset=0",
        f"{api_base}/Organization?_count=2&_sort=_id&_offset=1",
    ]


@pytest.mark.asyncio
async def test_netsmart_fetch_fails_closed_on_nonadjacent_page_cycle(monkeypatch):
    api_base = importer.SAN_MATEO_COUNTY_PROVIDER_DIRECTORY_BASE
    source_lookup = {"source_id": "county-candidate", "api_base": api_base}
    requested_urls: list[str] = []

    async def fake_fetch_source_json(_source, request_url, *, timeout):
        requested_urls.append(request_url)
        offset = int(
            urllib.parse.parse_qs(
                urllib.parse.urlsplit(request_url).query
            )["_offset"][0]
        )
        resource_ids = (
            ("org-a1", "org-a2")
            if offset in {0, 2}
            else ("org-b1", "org-b2")
        )
        return (
            200,
            {
                "resourceType": "Bundle",
                "type": "searchset",
                "entry": [
                    {
                        "resource": {
                            "resourceType": "Organization",
                            "id": resource_id,
                        }
                    }
                    for resource_id in resource_ids
                ],
            },
            None,
            timeout,
        )

    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_source_json)

    fetch_result = await importer._fetch_resource_rows(
        source_lookup,
        "Organization",
        per_resource_limit=0,
        page_limit=0,
        page_count=2,
        timeout=3,
        run_id=None,
    )

    assert fetch_result is not None
    assert fetch_result.complete is False
    assert fetch_result.error == "pagination_page_repeated"
    assert fetch_result.rows_fetched == 4
    assert fetch_result.pages_fetched == 3
    assert requested_urls == [
        f"{api_base}/Organization?_count=2&_sort=_id&_offset=0",
        f"{api_base}/Organization?_count=2&_sort=_id&_offset=1",
        f"{api_base}/Organization?_count=2&_sort=_id&_offset=2",
    ]


@pytest.mark.asyncio
async def test_synthetic_position_resume_requires_persisted_page_guard(monkeypatch):
    api_base = importer.IOWA_MEDICAID_PROVIDER_DIRECTORY_BASE
    source_lookup = {"source_id": "iowa-medicaid", "api_base": api_base}
    resume_url = (
        f"{api_base}/Organization?_count=2&_sort=_id&_skip=2"
    )
    monkeypatch.setattr(
        importer,
        "_load_or_initialize_pagination_checkpoint",
        AsyncMock(
            return_value=importer.PaginationResumeState(
                next_url=resume_url,
                pages_processed=1,
                rows_processed=2,
                recent_url_hashes=("prior-url-hash",),
                resumed=True,
                completeness={},
            )
        ),
    )
    fetch_source_json = AsyncMock(
        side_effect=AssertionError("missing page guard must fail before fetch")
    )
    monkeypatch.setattr(importer, "_fetch_source_json", fetch_source_json)
    checkpoint_context = importer.PaginationCheckpointContext(
        canonical_api_base=api_base,
        source_scope_hash="iowa-synthetic-v1",
        source_ids=("iowa-medicaid",),
        owner_run_id="run_retry",
        retry_of_run_id="run_original",
        acquisition_root_run_id="run_original",
        dataset_id="dataset_iowa",
        lineage_verified=True,
    )

    fetch_result = await importer._fetch_resource_rows(
        source_lookup,
        "Organization",
        per_resource_limit=0,
        page_limit=0,
        page_count=2,
        timeout=3,
        run_id="run_retry",
        row_batch_handler=AsyncMock(return_value=0),
        row_batch_size=100,
        retain_rows=False,
        pagination_checkpoint=checkpoint_context,
    )

    assert fetch_result is not None
    assert fetch_result.complete is False
    assert fetch_result.error == "pagination_page_guard_missing"
    assert fetch_result.rows_fetched == 2
    assert fetch_result.pages_fetched == 1
    assert fetch_result.next_url_remaining is True
    fetch_source_json.assert_not_awaited()


def test_source_row_from_seed_overrides_hap_stale_provider_directory_path():
    row = importer._source_row_from_seed(
        {
            "id": "hap-stale",
            "org_name": "Health Alliance Plan",
            "plan_name": "Provider Directory Retest",
            "api_base": "https://api.hap.org/fhir/provider-directory",
            "auth_type": "none",
            "last_validated_status": "not_found",
            "source": "provider-directory-db-retest",
        }
    )

    assert row["api_base"] == importer.HAP_PROVIDER_DIRECTORY_BASE
    assert row["canonical_api_base"] == importer.HAP_PROVIDER_DIRECTORY_BASE
    assert row["requires_registration"] is False
    assert row["auth_type"] == "none"
    assert row["last_validated_status"] == "valid"
    assert row["metadata_json"]["provider_directory_override"] == "hap_provider_directory_r4"
    assert row["metadata_json"]["provider_directory_confirmed_metadata_url"] == importer.HAP_PROVIDER_DIRECTORY_METADATA_URL
    assert row["metadata_json"]["provider_directory_request_interval_seconds"] == 20
    assert row["metadata_json"]["provider_directory_supported_resources"] == [
        "InsurancePlan",
        "Location",
        "Organization",
        "Practitioner",
        "PractitionerRole",
    ]


def test_hap_rewrites_blocked_pagination_sibling_host():
    source_lookup = {
        "api_base": importer.HAP_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.HAP_PROVIDER_DIRECTORY_BASE,
    }

    next_url = importer._resolved_fhir_next_url(
        source_lookup,
        f"{importer.HAP_PROVIDER_DIRECTORY_BASE}/Practitioner?_count=1",
        "https://fhir-prov-dir-r4.api.hap.org:443?_count=1&_getpages=next-token",
    )

    assert next_url == (
        f"{importer.HAP_PROVIDER_DIRECTORY_BASE}?_count=1&_getpages=next-token"
    )


def test_hap_full_refresh_uses_documented_page_size():
    source_lookup = {"api_base": importer.HAP_PROVIDER_DIRECTORY_BASE}

    assert importer._source_full_refresh_page_count(source_lookup, 100, 0, 0) == 1000
    assert importer._source_full_refresh_page_count(source_lookup, 100, 1, 0) == 100


@pytest.mark.parametrize(
    ("api_base", "next_host"),
    [
        (
            importer.TMHP_PROVIDER_DIRECTORY_BASE,
            "cmsinterop.tmhp.com",
        ),
        (
            importer.NEBRASKA_DHHS_PROVIDER_DIRECTORY_BASE,
            importer.NEBRASKA_DHHS_PAGINATION_HOST,
        ),
    ],
)
def test_state_directory_rewrites_hapi_next_link_to_sorted_offset(
    api_base,
    next_host,
):
    source_lookup = {
        "api_base": api_base,
        "canonical_api_base": api_base,
    }
    base_path = importer.urllib.parse.urlsplit(api_base).path
    current_url = (
        f"{api_base}/Practitioner?family=Smith&_count=100&_sort=_id&_offset=0"
    )
    advertised_next_url = (
        f"https://{next_host}{base_path}?"
        "_getpages=opaque&_getpagesoffset=100&_count=100&_bundletype=searchset"
    )

    next_url = importer._resolved_fhir_next_url(
        source_lookup,
        current_url,
        advertised_next_url,
    )

    assert next_url == (
        f"{api_base}/Practitioner?"
        "family=Smith&_sort=_id&_offset=100&_count=100"
    )


def test_state_directory_rejects_untrusted_offset_next_link():
    source_lookup = {
        "api_base": importer.NEBRASKA_DHHS_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.NEBRASKA_DHHS_PROVIDER_DIRECTORY_BASE,
    }

    with pytest.raises(ValueError, match="untrusted_offset_pagination_link"):
        importer._resolved_fhir_next_url(
            source_lookup,
            (
                f"{importer.NEBRASKA_DHHS_PROVIDER_DIRECTORY_BASE}/Practitioner?"
                "_count=100&_sort=_id&_offset=0"
            ),
            "https://evil.example/fhir?_getpagesoffset=100&_count=100",
        )


def test_generic_pagination_rejects_cross_origin_next_link():
    source_lookup = {"api_base": "https://payer.example/fhir"}

    with pytest.raises(ValueError, match="untrusted_pagination_link"):
        importer._resolved_fhir_next_url(
            source_lookup,
            "https://payer.example/fhir/Practitioner?_count=25",
            "https://untrusted.example/fhir/Practitioner?page=2",
        )


def test_generic_pagination_allows_configured_alias_host():
    source_lookup = {
        "api_base": "https://payer.example/fhir",
        "metadata_json": {
            "provider_directory_pagination_allowed_hosts": ["pagination.example"],
        },
    }
    next_url = "https://pagination.example/fhir/Practitioner?page=2"

    assert importer._resolved_fhir_next_url(
        source_lookup,
        "https://payer.example/fhir/Practitioner?_count=25",
        next_url,
    ) == next_url


def test_generic_pagination_normalizes_default_https_port():
    next_url = "https://payer.example:443/fhir/Practitioner?page=2"

    assert importer._resolved_fhir_next_url(
        {"api_base": "https://payer.example/fhir"},
        "https://payer.example/fhir/Practitioner?_count=25",
        next_url,
    ) == next_url


def test_idaho_medicaid_accepts_exact_alternate_ct_pagination_link():
    current_url = (
        f"{importer.IDAHO_MEDICAID_PROVIDER_DIRECTORY_BASE}/Practitioner?_count=100"
    )
    next_url = (
        "https://api-ida-prd.safhir.io/v1/api/provider-directory/Practitioner?"
        "_count=100&"
        "_profile=http%3A%2F%2Fhl7.org%2Ffhir%2Fus%2Fdavinci-pdex-plan-net%2F"
        "StructureDefinition%2Fplannet-Practitioner&ct=opaque-cursor"
    )

    assert importer._resolved_fhir_next_url(
        {"api_base": importer.IDAHO_MEDICAID_PROVIDER_DIRECTORY_BASE},
        current_url,
        next_url,
    ) == next_url


def test_idaho_medicaid_accepts_alternate_host_cursor_chain():
    current_url = (
        "https://api-ida-prd.safhir.io/v1/api/provider-directory/Practitioner?"
        "_count=100&ct=first-cursor"
    )
    next_url = (
        "https://api-ida-prd.safhir.io/v1/api/provider-directory/Practitioner?"
        "_count=100&ct=second-cursor"
    )

    assert importer._resolved_fhir_next_url(
        {"api_base": importer.IDAHO_MEDICAID_PROVIDER_DIRECTORY_BASE},
        current_url,
        next_url,
    ) == next_url


def test_idaho_medicaid_rejects_cursor_chain_from_untrusted_current_host():
    next_url = (
        "https://api-ida-prd.safhir.io/v1/api/provider-directory/Practitioner?"
        "_count=100&ct=second-cursor"
    )

    with pytest.raises(ValueError, match="untrusted_idaho_medicaid_pagination_link"):
        importer._resolved_fhir_next_url(
            {"api_base": importer.IDAHO_MEDICAID_PROVIDER_DIRECTORY_BASE},
            (
                "https://evil.example/v1/api/provider-directory/Practitioner?"
                "_count=100&ct=first-cursor"
            ),
            next_url,
        )


@pytest.mark.parametrize(
    "next_url",
    [
        "http://api-ida-prd.safhir.io/v1/api/provider-directory/Practitioner?ct=opaque",
        "https://api-ida-prd.safhir.io.evil.example/v1/api/provider-directory/Practitioner?ct=opaque",
        "https://api-ida-prd.safhir.io/v1/api/provider-directory/Location?ct=opaque",
        "https://api-ida-prd.safhir.io/v1/api/other-directory/Practitioner?ct=opaque",
        "https://api-ida-prd.safhir.io/v1/api/provider-directory/Practitioner?page=2",
        (
            "https://api-ida-prd.safhir.io/v1/api/provider-directory/Practitioner?"
            "ct=opaque&_profile=https%3A%2F%2Fevil.example%2Fprofile"
        ),
    ],
)
def test_idaho_medicaid_rejects_untrusted_alternate_pagination_link(next_url):
    with pytest.raises(ValueError, match="untrusted_idaho_medicaid_pagination_link"):
        importer._resolved_fhir_next_url(
            {"api_base": importer.IDAHO_MEDICAID_PROVIDER_DIRECTORY_BASE},
            (
                f"{importer.IDAHO_MEDICAID_PROVIDER_DIRECTORY_BASE}/Practitioner?"
                "_count=100"
            ),
            next_url,
        )


def test_aetna_commercial_accepts_same_resource_page_token_continuation():
    current_url = (
        f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}/Practitioner?_count=30"
    )
    next_url = (
        f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}/Practitioner?"
        "_page_token=opaque-token&_count=30"
    )

    assert importer._resolved_fhir_next_url(
        {"api_base": importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE},
        current_url,
        next_url,
    ) == next_url
    assert "_page_token" in importer.FHIR_CONTINUATION_QUERY_NAMES
    assert importer._source_fetch_candidate_urls(next_url) == [next_url]


@pytest.mark.parametrize(
    "next_url",
    [
        "https://evil.example/fhir/v1/providerdirectorydata/Practitioner?_page_token=x",
        f"{importer.AETNA_PROVIDER_DIRECTORY_BASE}/Practitioner?_page_token=x",
        f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}/Location?_page_token=x",
        f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}/Practitioner/extra?_page_token=x",
        f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}/Practitioner?_count=30",
        f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}/Practitioner?_page_token=x&_count=31",
        f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}/Practitioner?_page_token=x&name=Smith",
    ],
)
def test_aetna_commercial_rejects_untrusted_page_token_continuation(next_url):
    with pytest.raises(ValueError, match="untrusted_aetna_pagination_link"):
        importer._resolved_fhir_next_url(
            {"api_base": importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE},
            (
                f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}/Practitioner?"
                "_count=30"
            ),
            next_url,
        )


def test_aetna_page_token_identity_detects_replay_despite_query_changes():
    first_url = (
        f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}/Practitioner?"
        "_page_token=opaque-token&_count=30"
    )
    replay_url = (
        f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}/Practitioner?"
        "_count=25&_page_token=opaque-token"
    )

    assert importer._pagination_url_identity(first_url) == (
        importer._pagination_url_identity(replay_url)
    )


def test_molina_rewrites_exact_sapphire_next_link():
    source_lookup = {
        "api_base": importer.MOLINA_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.MOLINA_PROVIDER_DIRECTORY_BASE,
    }
    raw_query = "_count=100&cursorMark=abc%2Bdef&family=Smith%20Jones"

    next_url = importer._resolved_fhir_next_url(
        source_lookup,
        f"{importer.MOLINA_PROVIDER_DIRECTORY_BASE}/Practitioner?_count=100",
        f"https://{importer.MOLINA_PAGINATION_HOST}/fhir/Practitioner?{raw_query}",
    )

    assert next_url == (
        f"{importer.MOLINA_PROVIDER_DIRECTORY_BASE}/Practitioner?{raw_query}"
    )


@pytest.mark.parametrize(
    "next_url",
    [
        "https://evil.example/fhir/Practitioner?cursorMark=abc",
        "https://molina.sapphirethreesixtyfive.com:8443/fhir/Practitioner?cursorMark=abc",
        "https://molina.sapphirethreesixtyfive.com/fhir/Location?cursorMark=abc",
        "https://molina.sapphirethreesixtyfive.com/fhir/Practitioner/extra?cursorMark=abc",
        "https://molina.sapphirethreesixtyfive.com/fhir/Practitioner?_count=100",
    ],
)
def test_molina_rejects_unallowlisted_cross_origin_next_link(next_url):
    source_lookup = {
        "api_base": importer.MOLINA_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.MOLINA_PROVIDER_DIRECTORY_BASE,
    }

    with pytest.raises(ValueError, match="untrusted_molina_pagination_link"):
        importer._resolved_fhir_next_url(
            source_lookup,
            f"{importer.MOLINA_PROVIDER_DIRECTORY_BASE}/Practitioner?_count=100",
            next_url,
        )


@pytest.mark.asyncio
async def test_partition_fetch_detects_reordered_molina_cursor_cycle():
    state = importer.PartitionFetchState(
        result_model=object,
        retained_resource_rows=[],
    )
    first_url = (
        f"{importer.MOLINA_PROVIDER_DIRECTORY_BASE}/Practitioner?"
        "_count=100&cursorMark=abc%2Bdef&family=Smith"
    )
    reordered_url = (
        f"{importer.MOLINA_PROVIDER_DIRECTORY_BASE}/Practitioner?"
        "family=Smith&cursorMark=abc%2bdef&_count=100"
    )

    assert await importer._can_fetch_partition_url(state, first_url, max_pages=0) is True
    assert await importer._can_fetch_partition_url(state, reordered_url, max_pages=0) is False
    assert state.error_message == "pagination_cursor_repeated"


def test_source_row_from_seed_overrides_hap_name_only_seed_row():
    row = importer._source_row_from_seed(
        {
            "id": "hap-no-api",
            "org_name": "HAP",
            "plan_name": "Provider Directory Retest",
            "api_base": None,
            "auth_type": "none",
            "last_validated_status": "no_api",
            "source": "provider-directory-db",
        }
    )

    assert row["api_base"] == importer.HAP_PROVIDER_DIRECTORY_BASE
    assert row["last_validated_status"] == "valid"
    assert row["metadata_json"]["provider_directory_override"] == "hap_provider_directory_r4"


def test_source_row_from_seed_overrides_scan_developer_portal_base():
    """The SCAN portal seed resolves to its strict partitioned FHIR source."""
    source_record = importer._source_row_from_seed(
        {
            "id": "scan-1",
            "org_name": "SCAN Health Plan",
            "plan_name": "Medicare Advantage",
            "api_base": "https://developer.scanhealthplan.com",
            "auth_type": "API Key",
            "source": "provider-directory-db",
        }
    )

    assert source_record["api_base"] == importer.SCAN_PROVIDER_DIRECTORY_BASE
    assert source_record["canonical_api_base"] == importer.SCAN_PROVIDER_DIRECTORY_BASE
    assert source_record["requires_registration"] is False
    assert source_record["auth_type"] == "none"
    assert source_record["last_validated_status"] == "valid"
    assert source_record["metadata_json"]["provider_directory_override"] == "scan_providerdirectory_intersystems"
    assert source_record["metadata_json"]["provider_directory_previous_api_base"] == importer.SCAN_DEVELOPER_PORTAL_URL
    assert source_record["metadata_json"]["provider_directory_confirmed_doc_url"] == importer.SCAN_PROVIDER_DIRECTORY_DOC_URL
    assert source_record["metadata_json"]["provider_directory_coverage_mode"] == "probe_only"
    assert source_record["metadata_json"]["provider_directory_fully_enumerable_resources"] == []
    assert source_record["metadata_json"]["provider_directory_acquisition_enabled"] is True
    partition_resources = source_record["metadata_json"][
        importer.LAST_UPDATED_PARTITION_METADATA_KEY
    ]["resources"]
    assert set(partition_resources) == set(importer.DEFAULT_RESOURCES)
    assert all(
        resource_config == {
            "start": "1900-01-01T00:00:00Z",
            "end": "resolved_now",
            "ceiling": 1000,
            "minimum_width_seconds": 1,
            "boundary_precision_seconds": 1,
            "page_count": 100,
            "volatile_metadata_paths": [],
        }
        for resource_config in partition_resources.values()
    )
    assert importer._resource_acquisition_blocked_reason(
        source_record,
        list(importer.DEFAULT_RESOURCES),
    ) is None
    for resource_type in importer.DEFAULT_RESOURCES:
        partition_config, partition_error = importer._last_updated_partition_config(
            source_record,
            resource_type,
        )
        assert partition_error is None
        assert partition_config is not None
        assert partition_config.ceiling == 1000
        assert partition_config.page_count == 100
        assert partition_config.end_mode == "resolved_now"
        assert partition_config.boundary_precision == datetime.timedelta(seconds=1)
    checkpoint_context = importer._pagination_checkpoint_context(
        source_record,
        [source_record["source_id"]],
        run_id="run_scan_candidate",
        retry_of_run_id=None,
    )
    assert checkpoint_context is not None
    assert checkpoint_context.canonical_api_base == importer.SCAN_PROVIDER_DIRECTORY_BASE
    assert (
        source_record["metadata_json"]["provider_directory_blocked_reason"]
        == importer.PROVIDER_DIRECTORY_PROBE_ONLY_BLOCKED_REASON
    )


def test_scan_source_id_uses_normalized_base():
    portal_row = importer._source_row_from_seed(
        {
            "id": "scan-1",
            "org_name": "SCAN Health Plan",
            "plan_name": "Medicare Advantage",
            "api_base": "https://developer.scanhealthplan.com",
            "auth_type": "none",
            "source": "provider-directory-db",
        }
    )
    concrete_row = importer._source_row_from_seed(
        {
            "id": "scan-1",
            "org_name": "SCAN Health Plan",
            "plan_name": "Medicare Advantage",
            "api_base": importer.SCAN_PROVIDER_DIRECTORY_BASE,
            "auth_type": "none",
            "source": "provider-directory-db",
        }
    )

    assert portal_row["source_id"] == concrete_row["source_id"]
    assert portal_row["api_base"] == concrete_row["api_base"]


def test_source_row_from_seed_normalizes_resource_specific_api_base():
    row = importer._source_row_from_seed(
        {
            "id": "maine-1",
            "org_name": "State of Maine",
            "plan_name": "Medicaid",
            "api_base": "https://maineproviderdirectory.verityanalytics.org/fhir/Practitioner",
            "auth_type": "none",
            "last_validated_status": "valid",
            "source": "provider-directory-db",
        }
    )

    assert row["api_base"] == "https://maineproviderdirectory.verityanalytics.org/fhir"
    assert row["canonical_api_base"] == "https://maineproviderdirectory.verityanalytics.org/fhir"
    assert row["metadata_json"]["provider_directory_previous_api_base"] == (
        "https://maineproviderdirectory.verityanalytics.org/fhir/Practitioner"
    )
    assert row["metadata_json"]["provider_directory_base_normalization"] == "resource_or_metadata_parent_base"
    assert row["metadata_json"]["provider_directory_confirmed_base"] == (
        "https://maineproviderdirectory.verityanalytics.org/fhir"
    )


def test_resource_url_source_id_uses_normalized_base():
    resource_url_row = importer._source_row_from_seed(
        {
            "id": "maine-1",
            "org_name": "State of Maine",
            "plan_name": "Medicaid",
            "api_base": "https://maineproviderdirectory.verityanalytics.org/fhir/Practitioner",
            "auth_type": "none",
            "last_validated_status": "valid",
            "source": "provider-directory-db",
        }
    )
    parent_base_row = importer._source_row_from_seed(
        {
            "id": "maine-1",
            "org_name": "State of Maine",
            "plan_name": "Medicaid",
            "api_base": "https://maineproviderdirectory.verityanalytics.org/fhir",
            "auth_type": "none",
            "last_validated_status": "valid",
            "source": "provider-directory-db",
        }
    )

    assert resource_url_row["source_id"] == parent_base_row["source_id"]
    assert resource_url_row["api_base"] == parent_base_row["api_base"]


def test_source_row_from_seed_normalizes_resource_template_api_base():
    observed_row_map = importer._source_row_from_seed(
        {
            "id": "maryland-1",
            "org_name": "Maryland Physicians Care",
            "plan_name": "Medicaid",
            "api_base": "https://md-medicaid.convergent-pd.com/fhir/{resource}",
            "auth_type": "none",
            "last_validated_status": "not_found",
            "source": "provider-directory-db",
        }
    )
    encoded = importer._source_row_from_seed(
        {
            "id": "maryland-1",
            "org_name": "Maryland Physicians Care",
            "plan_name": "Medicaid",
            "api_base": "https://md-medicaid.convergent-pd.com/fhir/%7Bresource%7D",
            "auth_type": "none",
            "last_validated_status": "not_found",
            "source": "provider-directory-db",
        }
    )

    assert observed_row_map["api_base"] == "https://md-medicaid.convergent-pd.com/fhir"
    assert observed_row_map["canonical_api_base"] == "https://md-medicaid.convergent-pd.com/fhir"
    assert observed_row_map["source_id"] == encoded["source_id"]
    assert encoded["api_base"] == observed_row_map["api_base"]
    assert observed_row_map["last_validated_status"] == "not_found"
    assert (
        observed_row_map["metadata_json"]["provider_directory_base_normalization"]
        == "resource_or_metadata_parent_base"
    )
    assert observed_row_map["metadata_json"]["provider_directory_previous_api_base"] == (
        "https://md-medicaid.convergent-pd.com/fhir/{resource}"
    )
    assert observed_row_map["metadata_json"]["provider_directory_confirmed_base"] == (
        "https://md-medicaid.convergent-pd.com/fhir"
    )


def test_source_row_from_seed_overrides_alohr_public_app_base():
    row = importer._source_row_from_seed(
        {
            "id": "alohr-1",
            "org_name": "State of Alabama",
            "plan_name": "Medicaid",
            "portal_url": "https://alohr.esante.us/public/providers",
            "api_base": "https://alohr.esante.us/public/providers",
            "auth_type": "none",
            "source": "provider-directory-db",
        }
    )

    assert row["api_base"] == importer.ALOHR_FHIR_PROVIDER_DIRECTORY_BASE
    assert row["canonical_api_base"] == importer.ALOHR_FHIR_PROVIDER_DIRECTORY_BASE
    assert row["requires_registration"] is True
    assert row["auth_type"] == "OAuth2/SMART"
    assert row["metadata_json"]["provider_directory_override"] == "alohr_healthshare_providerdirectory"
    assert row["metadata_json"]["provider_directory_graphql_url"] == importer.ALOHR_GRAPHQL_URL
    assert row["metadata_json"]["provider_directory_graphql_tenant_id"] == importer.ALOHR_TENANT_ID


def _alohr_source_row_for_identity_test() -> dict[str, Any]:
    return importer._source_row_from_seed(
        {
            "id": "alohr-identity",
            "org_name": "State of Alabama",
            "plan_name": "Medicaid",
            "portal_url": importer.ALOHR_PUBLIC_PROVIDER_DIRECTORY_BASE,
            "api_base": importer.ALOHR_PUBLIC_PROVIDER_DIRECTORY_BASE,
            "auth_type": "none",
            "source": "provider-directory-db",
        }
    )


def test_alohr_endpoint_identity_records_graphql_acquisition_contract():
    source_row = _alohr_source_row_for_identity_test()
    endpoint_row = importer._provider_directory_api_endpoint_row(source_row)

    assert endpoint_row is not None
    contract = endpoint_row["metadata_json"]["connector_acquisition_contract"]
    assert endpoint_row["metadata_json"]["identity_version"] == "resource-import-group-v2"
    assert contract == {
        "transport": "graphql",
        "url": importer.ALOHR_GRAPHQL_URL,
        "tenant_header": "tenantId",
        "tenant_id": "alohr",
        "streams": [
            {
                "root": "providers",
                "item_root": "providers",
                "query_sha256": hashlib.sha256(
                    importer.ALOHR_PROVIDER_QUERY.encode("utf-8")
                ).hexdigest(),
            },
            {
                "root": "providerOrgs",
                "item_root": "providerOrganizations",
                "query_sha256": hashlib.sha256(
                    importer.ALOHR_ORGANIZATION_QUERY.encode("utf-8")
                ).hexdigest(),
            },
        ],
    }
    assert json.loads(
        endpoint_row["endpoint_signature_json"]["connector_acquisition_contract"]
    ) == contract


@pytest.mark.parametrize(
    ("contract_attribute", "replacement"),
    [
        ("ALOHR_GRAPHQL_URL", "https://replacement.example/graphql"),
        ("ALOHR_TENANT_ID", "replacement-tenant"),
        ("ALOHR_PROVIDER_GRAPHQL_ROOT", "replacementProviders"),
        ("ALOHR_PROVIDER_GRAPHQL_ITEM_ROOT", "replacementProviderItems"),
        ("ALOHR_ORGANIZATION_GRAPHQL_ROOT", "replacementOrganizations"),
        (
            "ALOHR_ORGANIZATION_GRAPHQL_ITEM_ROOT",
            "replacementOrganizationItems",
        ),
        (
            "ALOHR_PROVIDER_QUERY",
            "query Replacement { providers(criteria: {}) { nextToken } }",
        ),
        (
            "ALOHR_ORGANIZATION_QUERY",
            "query Replacement { providerOrgs(criteria: {}) { nextToken } }",
        ),
    ],
)
def test_alohr_endpoint_identity_hashes_every_graphql_contract_field(
    monkeypatch,
    contract_attribute,
    replacement,
):
    original_source = _alohr_source_row_for_identity_test()
    original_endpoint = importer._provider_directory_api_endpoint_row(original_source)
    assert original_endpoint is not None

    monkeypatch.setattr(importer, contract_attribute, replacement)
    changed_source = _alohr_source_row_for_identity_test()
    changed_endpoint = importer._provider_directory_api_endpoint_row(changed_source)

    assert changed_endpoint is not None
    assert changed_source["source_id"] == original_source["source_id"]
    assert changed_endpoint["endpoint_id"] != original_endpoint["endpoint_id"]


def test_alohr_graphql_endpoint_change_forces_a_fresh_four_resource_dataset(
    monkeypatch,
):
    original_source = _alohr_source_row_for_identity_test()
    original_endpoint = importer._provider_directory_api_endpoint_row(original_source)
    assert original_endpoint is not None

    monkeypatch.setattr(importer, "ALOHR_TENANT_ID", "replacement-tenant")
    changed_source = _alohr_source_row_for_identity_test()
    changed_endpoint = importer._provider_directory_api_endpoint_row(changed_source)
    assert changed_endpoint is not None

    selected_resources = importer._endpoint_dataset_expected_resources(
        [changed_source]
    )
    original_dataset_id = importer._endpoint_dataset_candidate_id(
        original_endpoint["endpoint_id"],
        selected_resources,
        "acquisition-root",
    )
    changed_dataset_id = importer._endpoint_dataset_candidate_id(
        changed_endpoint["endpoint_id"],
        selected_resources,
        "acquisition-root",
    )

    assert changed_source["source_id"] == original_source["source_id"]
    assert selected_resources == (
        "Location",
        "Organization",
        "Practitioner",
        "PractitionerRole",
    )
    assert changed_dataset_id != original_dataset_id


def test_source_row_from_seed_preserves_known_confirmed_importable_bases():
    """Verify this provider-directory regression contract."""
    cases = [
        (
            {
                "id": "aetna-r4",
                "org_name": "Aetna R4",
                "api_base": "https://fhir-ehr.cerner.com/r4/aetna",
                "auth_type": "OAuth2/SMART",
                "source": "provider-directory-db",
            },
            importer.AETNA_PROVIDER_DIRECTORY_BASE,
            "aetna_apif1_providerdirectory",
        ),
        (
            {
                "id": "alohr-public",
                "org_name": "Blue Cross and Blue Shield of Alabama",
                "api_base": "https://alohr.esante.us/public/providers",
                "auth_type": "none",
                "source": "provider-directory-db",
            },
            importer.ALOHR_FHIR_PROVIDER_DIRECTORY_BASE,
            "alohr_healthshare_providerdirectory",
        ),
        (
            {
                "id": "cigna-availity",
                "org_name": "Cigna",
                "api_base": "https://apps.availity.com/availity/public-fhir/fhir/v1/cigna/r4",
                "auth_type": "none",
                "source": "provider-directory-db",
            },
            importer.CIGNA_PROVIDER_DIRECTORY_BASE,
            "cigna_public_providerdirectory",
        ),
        (
            {
                "id": "centene-portal",
                "org_name": "Centene Corporation",
                "api_base": "https://partners.centene.com/apis",
                "auth_type": "OAuth2 Client Credentials",
                "source": "provider-directory-db",
            },
            importer.CENTENE_PROVIDER_DIRECTORY_BASE,
            "centene_iopc_pd_providerdirectory",
        ),
        (
            {
                "id": "uhc-portal",
                "org_name": "UnitedHealthcare",
                "api_base": "https://www.uhc.com/legal/interoperability-apis/patient-access-api",
                "auth_type": "OAuth2 Client Credentials",
                "source": "provider-directory-db",
            },
            importer.UHC_PROVIDER_DIRECTORY_BASE,
            "uhc_flex_optum_fhirpublic_r4",
        ),
        (
            {
                "id": "scan-portal",
                "org_name": "SCAN Health Plan",
                "api_base": "https://developer.scanhealthplan.com",
                "auth_type": "API Key",
                "source": "provider-directory-db",
            },
            importer.SCAN_PROVIDER_DIRECTORY_BASE,
            "scan_providerdirectory_intersystems",
        ),
    ]

    for seed_row, expected_base, expected_override in cases:
        observed_row_map = importer._source_row_from_seed(seed_row)

        assert observed_row_map["api_base"] == expected_base
        assert observed_row_map["canonical_api_base"] == expected_base
        assert observed_row_map["metadata_json"]["provider_directory_override"] == expected_override
        assert observed_row_map["metadata_json"]["provider_directory_confirmed_base"] == expected_base


def test_resource_import_source_selection_reports_credentialed_policy_counts():
    tested_source_records = [
        {
            "source_id": "open_valid",
            "api_base": "https://open.example/fhir",
            "auth_type": "open",
            "last_validated_status": "valid",
            "requires_registration": False,
        },
        {
            "source_id": "auth_required",
            "api_base": "https://auth.example/fhir",
            "auth_type": "OAuth2 Client Credentials",
            "last_validated_status": "auth_required",
            "requires_registration": True,
        },
        {
            "source_id": "non_fhir",
            "api_base": "https://app.example/providers",
            "auth_type": "open",
            "last_validated_status": "valid_non_fhir",
            "requires_registration": False,
        },
        {
            "source_id": "missing_base",
            "api_base": None,
            "auth_type": "open",
            "last_validated_status": "valid",
            "requires_registration": False,
        },
    ]

    selected, metrics = importer._select_resource_import_sources(
        tested_source_records,
        valid_source_ids=None,
        open_only=False,
        include_auth_required=True,
    )
    selected_without_auth, metrics_without_auth = importer._select_resource_import_sources(
        tested_source_records,
        valid_source_ids=None,
        open_only=False,
        include_auth_required=False,
    )

    assert [tested_source_map["source_id"] for tested_source_map in selected] == ["open_valid", "auth_required"]
    assert metrics["source_import_sources_considered"] == 4
    assert metrics["source_import_sources_selected"] == 2
    assert metrics["source_import_sources_selected_declared_credentialed"] == 1
    assert metrics["source_import_sources_selected_auth_required_seed"] == 1
    assert metrics["source_import_skipped_validation_status"] == 1
    assert metrics["source_import_skipped_missing_api_base"] == 1
    assert [tested_source_map["source_id"] for tested_source_map in selected_without_auth] == ["open_valid"]
    assert metrics_without_auth["source_import_skipped_auth_required_policy"] == 1


def test_resource_import_source_selection_allows_live_probe_success_over_open_only():
    source_record_by_field = {
        "source_id": "credentialed_valid",
        "api_base": "https://auth.example/fhir",
        "auth_type": "OAuth2 Client Credentials",
        "last_validated_status": "auth_required",
        "requires_registration": True,
    }

    selected, metrics = importer._select_resource_import_sources(
        [source_record_by_field],
        valid_source_ids={"credentialed_valid"},
        open_only=True,
        include_auth_required=False,
    )

    assert selected == [source_record_by_field]
    assert metrics["source_import_sources_selected"] == 1
    assert metrics["source_import_sources_selected_live_probe_valid"] == 1
    assert metrics["source_import_sources_selected_declared_credentialed"] == 1
    assert metrics["source_import_sources_selected_auth_required_seed"] == 1


@pytest.mark.parametrize(
    ("source", "expected_reason"),
    [
        (
            {
                "source_id": "shared_backend",
                "api_base": "https://shared.example/fhir",
                "last_validated_status": "shared_backend_unverified",
                "metadata_json": {},
            },
            "shared_backend_unverified",
        ),
        (
            {
                "source_id": "probe_only",
                "api_base": "https://probe.example/fhir",
                "last_validated_status": "valid",
                "metadata_json": {"provider_directory_coverage_mode": "probe_only"},
            },
            "coverage_mode_probe_only",
        ),
        (
            {
                "source_id": "not_enumerable",
                "api_base": "https://targeted.example/fhir",
                "last_validated_status": "valid",
                "metadata_json": {
                    "provider_directory_coverage_mode": "targeted",
                    "provider_directory_fully_enumerable_resources": [],
                },
            },
            "fully_enumerable_resources_empty",
        ),
    ],
)
def test_selection_blocks_non_acquisition_sources_after_live_probe(
    source,
    expected_reason,
):
    selected, metrics = importer._select_resource_import_sources(
        [source],
        valid_source_ids={source["source_id"]},
        open_only=False,
        include_auth_required=True,
        checkpoint_retry_source_ids={source["source_id"]},
    )

    assert selected == []
    assert metrics["source_import_skipped_blocked_source"] == 1
    assert metrics[f"source_import_skipped_blocked_source_{expected_reason}"] == 1


@pytest.mark.asyncio
async def test_process_data_acquires_only_neutral_0900_without_alias_fanout(
    monkeypatch,
):
    plan_name_by_code = {
        "0500": "AmeriHealth Caritas PA",
        "0900": "AmeriHealth Caritas NH",
        "1200": "AmeriHealth Caritas North Carolina",
        "2100": "AmeriHealth Caritas Louisiana",
        "5400": "AmeriHealth Caritas DC",
        "7100": "AmeriHealth Caritas DE",
    }
    seed_rows = [
        {
            "id": f"amerihealth-{plan_code}",
            "org_name": "AmeriHealth Caritas",
            "plan_name": plan_name_by_code[plan_code],
            "api_base": importer._amerihealth_caritas_provider_directory_base(plan_code),
            "auth_type": "none",
            "source": "provider-directory-db",
        }
        for plan_code in ("0500", "0900", "1200", "2100", "5400", "7100")
    ]
    resolved_rows = [importer._source_row_from_seed(seed) for seed in seed_rows]
    source_ids = {
        resolved_source["source_id"] for resolved_source in resolved_rows
    }
    probe_sources = AsyncMock(return_value=(6, 6, source_ids))
    import_resources = AsyncMock(return_value={"Practitioner": 1})
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(importer, "_clear_resource_rows_seen", AsyncMock(return_value=0))
    monkeypatch.setattr(importer, "_resolve_seed_db", lambda *_args, **_kwargs: ("fixture.db", None))
    monkeypatch.setattr(importer, "_seed_rows_from_sqlite", lambda *_args, **_kwargs: seed_rows)
    monkeypatch.setattr(importer, "_upsert_rows", AsyncMock(return_value=6))
    monkeypatch.setattr(importer, "_run_source_probe_batch", probe_sources)
    monkeypatch.setattr(importer, "_import_resources", import_resources)

    audit_metrics = await importer.process_data(
        {"context": {}},
        {
            "seed_db_path": "fixture.db",
            "probe": True,
            "import_resources": True,
            "resources": ",".join(importer.AMERIHEALTH_CARITAS_SUPPORTED_RESOURCES),
            "publish_artifacts": False,
            "provider_directory_endpoint_scope": importer.AMERIHEALTH_CARITAS_CARRIER_PROVIDER_API_BASE,
        },
    )

    imported_sources = import_resources.await_args.args[0]
    assert len(imported_sources) == 1
    assert imported_sources[0]["api_base"] == importer.AMERIHEALTH_CARITAS_CARRIER_PROVIDER_API_BASE
    assert imported_sources[0]["plan_name"] is None
    assert imported_sources[0]["metadata_json"]["provider_directory_original_alias"]["plan_name"] == "AmeriHealth Caritas NH"
    assert audit_metrics["source_import_sources_selected"] == 1
    assert audit_metrics["source_import_skipped_blocked_source"] == 5
    assert audit_metrics["source_import_groups_attempted"] == 1
    assert audit_metrics["source_import_duplicate_sources_collapsed"] == 0
    assert "amerihealth_shared_backend_preflight" not in audit_metrics
def test_seed_rows_from_retest_results_filters_to_provider_like_rows(tmp_path):
    """Verify seed rows from retest results filters to provider like rows."""
    retest_path = tmp_path / "retest_results.json"
    retest_path.write_text(
        json.dumps(
            {
                "tested_at": "2026-06-03T17:43:09Z",
                "results": [
                    {
                        "payer_id": 10,
                        "classification": "valid",
                        "org_name": "Public FHIR Payer",
                        "api_base": "https://public.example/fhir",
                        "status_code": 200,
                    },
                    {
                        "payer_id": 11,
                        "classification": "valid_non_fhir",
                        "org_name": "Public App Payer",
                        "api_base": "https://public-app.example/providers",
                        "status_code": 200,
                    },
                    {
                        "payer_id": 12,
                        "classification": "auth_required",
                        "org_name": "Auth Payer",
                        "api_base": "https://auth.example/fhir",
                        "status_code": 403,
                    },
                    {
                        "payer_id": 13,
                        "classification": "not_found",
                        "org_name": "Dead Payer",
                        "api_base": "https://dead.example/fhir",
                        "status_code": 404,
                    },
                    {
                        "payer_id": 14,
                        "classification": "not_found",
                        "org_name": "Aetna",
                        "api_base": "https://vteapif1.aetna.com/fhirdirectory/v1/patientaccess/",
                        "status_code": 404,
                    },
                    {
                        "payer_id": 15,
                        "classification": "client_error",
                        "org_name": "Humana Inc.",
                        "api_base": "https://fhir.humana.com/api/provider-directory/",
                        "status_code": 400,
                    },
                    {
                        "payer_id": 16,
                        "classification": "not_found",
                        "org_name": "State of Maine",
                        "api_base": "https://maineproviderdirectory.verityanalytics.org/fhir/Practitioner",
                        "status_code": 404,
                    },
                    {
                        "payer_id": 17,
                        "classification": "not_found",
                        "org_name": "Maryland Physicians Care",
                        "api_base": "https://md-medicaid.convergent-pd.com/fhir/{resource}",
                        "status_code": 404,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    observed_rows = importer._seed_rows_from_retest_results(retest_path)

    assert [observed_row_map["org_name"] for observed_row_map in observed_rows] == [
        "Public FHIR Payer",
        "Public App Payer",
        "Auth Payer",
        "Aetna",
        "Humana Inc.",
        "State of Maine",
        "Maryland Physicians Care",
    ]
    assert observed_rows[0]["auth_type"] == "open"
    assert observed_rows[0]["source"] == "provider-directory-db-retest"
    assert observed_rows[0]["source_date"] == "2026-06-03T17:43:09Z"
    assert observed_rows[1]["last_validated_status"] == "valid_non_fhir"
    assert observed_rows[2]["last_validated_status"] == "auth_required"
    assert observed_rows[2]["requires_registration"] is True
    assert observed_rows[2]["auth_type"] == "OAuth2 Client Credentials"
    assert observed_rows[3]["last_validated_status"] == "not_found"
    normalized_aetna = importer._source_row_from_seed(observed_rows[3])
    assert normalized_aetna["api_base"] == importer.AETNA_PROVIDER_DIRECTORY_BASE
    assert normalized_aetna["requires_registration"] is True
    assert normalized_aetna["last_validated_status"] == "auth_required"
    assert observed_rows[4]["last_validated_status"] == "client_error"
    normalized_humana = importer._source_row_from_seed(observed_rows[4])
    assert normalized_humana["api_base"] == importer.HUMANA_PROVIDER_DIRECTORY_BASE
    assert normalized_humana["requires_registration"] is False
    assert normalized_humana["last_validated_status"] == "valid"
    assert observed_rows[5]["last_validated_status"] == "not_found"
    normalized_maine = importer._source_row_from_seed(observed_rows[5])
    assert normalized_maine["api_base"] == "https://maineproviderdirectory.verityanalytics.org/fhir"
    assert normalized_maine["last_validated_status"] == "not_found"
    assert (
        normalized_maine["metadata_json"]["provider_directory_base_normalization"]
        == "resource_or_metadata_parent_base"
    )
    assert observed_rows[6]["last_validated_status"] == "not_found"
    normalized_maryland = importer._source_row_from_seed(observed_rows[6])
    assert normalized_maryland["api_base"] == "https://md-medicaid.convergent-pd.com/fhir"
    assert (
        normalized_maryland["metadata_json"]["provider_directory_base_normalization"]
        == "resource_or_metadata_parent_base"
    )


def test_dedupe_source_rows_skips_low_information_retest_duplicate():
    sqlite_row = importer._source_row_from_seed(
        {
            "id": "cigna-sqlite",
            "org_name": "Cigna",
            "plan_name": "Cigna (Primary)",
            "api_base": "https://apps.availity.com/availity/public-fhir/fhir/v1/cigna/r4",
            "source": "provider-directory-db",
        }
    )
    retest_row = importer._source_row_from_seed(
        {
            "id": "cigna-retest",
            "org_name": "Cigna",
            "plan_name": "Provider Directory Retest",
            "api_base": "https://fhir.cigna.com/ProviderDirectory/v1",
            "source": "provider-directory-db-retest",
        }
    )

    rows = importer._dedupe_source_rows([sqlite_row, retest_row])

    assert rows == [sqlite_row]


def test_dedupe_source_rows_merges_retest_equivalent_previous_base():
    sqlite_row = importer._source_row_from_seed(
        {
            "id": "molina-sqlite",
            "org_name": "Molina Healthcare",
            "plan_name": "Molina CA",
            "api_base": "https://developer.interop.molinahealthcare.com",
            "source": "provider-directory-db",
        }
    )
    retest_row = importer._source_row_from_seed(
        {
            "id": "molina-retest",
            "org_name": "Molina Healthcare",
            "plan_name": "Provider Directory Retest",
            "api_base": "https://fhir.molinahealthcare.com/provider-directory/",
            "last_validated_status": "unreachable",
            "source": "provider-directory-db-retest",
        }
    )

    observed_rows = importer._dedupe_source_rows([sqlite_row, retest_row])

    assert observed_rows == [sqlite_row]
    assert observed_rows[0]["metadata_json"]["provider_directory_equivalent_api_bases"] == [
        "https://fhir.molinahealthcare.com/provider-directory"
    ]
    assert observed_rows[0]["metadata_json"]["provider_directory_merged_overrides"] == [
        "molina_interop_providerdirectory"
    ]


def test_dedupe_source_rows_keeps_unique_auth_required_retest_source():
    sqlite_row = importer._source_row_from_seed(
        {
            "id": "cigna-sqlite",
            "org_name": "Cigna",
            "plan_name": "Cigna (Primary)",
            "api_base": "https://fhir.cigna.com/ProviderDirectory/v1",
            "source": "provider-directory-db",
        }
    )
    retest_row = importer._source_row_from_seed(
        {
            "id": "wellmed-retest",
            "org_name": "WellMed (UHC subsidiary)",
            "plan_name": "Provider Directory Retest",
            "api_base": "https://api.1up.health/fhir-r4/wellmeduhcsubsidiary",
            "auth_type": "OAuth2 Client Credentials",
            "last_validated_status": "auth_required",
            "requires_registration": True,
            "source": "provider-directory-db-retest",
        }
    )

    rows = importer._dedupe_source_rows([sqlite_row, retest_row])

    assert rows == [sqlite_row, retest_row]
    assert rows[1]["last_validated_status"] == "auth_required"
    assert rows[1]["requires_registration"] is True
    assert rows[1]["auth_type"] == "OAuth2 Client Credentials"


def test_alohr_provider_rows_emit_practitioner_location_and_role():
    rows_by_model: dict[type, list[dict[str, Any]]] = {}

    importer._append_alohr_provider_rows(
        rows_by_model,
        "source_alohr",
        {
            "providerId": "prac-1234567893",
            "firstName": "Ada",
            "lastName": "Lovelace",
            "npi": "1234567893",
            "phone": "+12055550123",
            "specialty": "207Q00000X",
            "specialtyDescription": "Family Medicine Physician",
            "telehealth": False,
            "acceptingMedicaid": True,
            "addresses": [
                {"type": "work", "street1": "100 Main St", "city": "Birmingham", "state": "AL", "zip": "35203"}
            ],
        },
        run_id="run_1",
    )

    practitioner = rows_by_model[ProviderDirectoryPractitioner][0]
    location = rows_by_model[ProviderDirectoryLocation][0]
    role = rows_by_model[ProviderDirectoryPractitionerRole][0]
    assert practitioner["npi"] == 1234567893
    assert practitioner["full_name"] == "Ada Lovelace"
    assert location["first_line"] == "100 Main St"
    assert location["city_name"] == "Birmingham"
    assert location["state_code"] == "AL"
    assert location["zip5"] == "35203"
    assert role["practitioner_ref"] == "Practitioner/prac-1234567893"
    assert role["location_refs"] == [f"Location/{location['resource_id']}"]
    assert role["telehealth"] == [{"supported": False}]
    assert role["accepting_medicaid"] is True
    assert practitioner["resource_url"] == (
        "urn:healthporta:provider-directory:alohr:"
        "Practitioner:prac-1234567893"
    )
    assert location["resource_url"].startswith(
        "urn:healthporta:provider-directory:alohr:Location:"
    )
    assert role["resource_url"].startswith(
        "urn:healthporta:provider-directory:alohr:PractitionerRole:"
    )


@pytest.mark.parametrize(
    ("provider_fields", "expected_telehealth", "expected_accepting_medicaid"),
    [
        ({}, [], None),
        ({"telehealth": None, "acceptingMedicaid": None}, [], None),
        ({"telehealth": True, "acceptingMedicaid": False}, [{"supported": True}], False),
    ],
)
def test_alohr_addressless_provider_keeps_role_without_inventing_booleans(
    provider_fields,
    expected_telehealth,
    expected_accepting_medicaid,
):
    rows_by_model: dict[type, list[dict[str, Any]]] = {}

    importer._append_alohr_provider_rows(
        rows_by_model,
        "source_alohr",
        {
            "providerId": "provider-without-address",
            "firstName": "Ada",
            "lastName": "Lovelace",
            "npi": "1234567893",
            "specialty": "207Q00000X",
            **provider_fields,
        },
        run_id="run_1",
    )

    assert ProviderDirectoryLocation not in rows_by_model
    role = rows_by_model[ProviderDirectoryPractitionerRole][0]
    assert role["location_refs"] == []
    assert role["specialty_codes"][0]["code"] == "207Q00000X"
    assert role["telehealth"] == expected_telehealth
    assert role["accepting_medicaid"] is expected_accepting_medicaid


def test_alohr_organization_does_not_invent_self_affiliation():
    rows_by_model: dict[type, list[dict[str, Any]]] = {}

    importer._append_alohr_organization_rows(
        rows_by_model,
        "source_alohr",
        {
            "orgId": "org-1",
            "name": "Example Clinic",
            "addresses": [
                {
                    "street1": "100 Main St",
                    "city": "Birmingham",
                    "state": "AL",
                    "zip": "35203",
                }
            ],
        },
        run_id="run_1",
    )

    assert len(rows_by_model[ProviderDirectoryOrganization]) == 1
    assert len(rows_by_model[ProviderDirectoryLocation]) == 1
    assert ProviderDirectoryOrganizationAffiliation not in rows_by_model


def test_fhir_address_normalizes_numeric_state_fips():
    address = importer._address(
        {
            "address": [
                {
                    "line": ["777 Township Line Rd", "Suite 150"],
                    "city": "Yardley",
                    "state": "42",
                    "postalCode": "19067",
                }
            ]
        }
    )

    assert address["state_name"] == "PA"
    assert address["state_code"] == "PA"


def test_fhir_address_normalizes_us_numeric_country_code():
    address = importer._address(
        {
            "address": [
                {
                    "line": ["1525 W 2100 S"],
                    "city": "S SALT LAKE",
                    "state": "UT",
                    "postalCode": "84119",
                    "country": "001",
                }
            ]
        }
    )

    assert address["country_code"] == "US"


def test_provider_directory_location_preserves_raw_contact_and_adds_canonical_digits():
    model, row = importer.parse_fhir_resource(
        "source_a",
        {
            "resourceType": "Location",
            "id": "loc-contact",
            "telecom": [
                {"system": "phone", "value": "+1 (312) 555-0100 ext. 45"},
                {"system": "fax", "value": "312.555.0199 # 22"},
            ],
            "address": {
                "line": ["100 Main St"],
                "city": "Chicago",
                "state": "IL",
                "postalCode": "60601",
                "country": "US",
            },
        },
    )

    assert model is ProviderDirectoryLocation
    assert row["telephone_number"] == "+1 (312) 555-0100 ext. 45"
    assert row["fax_number"] == "312.555.0199 # 22"
    assert row["phone_number"] == "3125550100"
    assert row["fax_number_digits"] == "3125550199"
    assert row["phone_extension"] == "45"
    assert row["fax_extension"] == "22"


def test_practitioner_npi_falls_back_to_numeric_resource_id():
    model, row = importer.parse_fhir_resource(
        "source_a",
        {
            "resourceType": "Practitioner",
            "id": "1000488800",
            "active": True,
            "name": [{"text": "Molina Shaped Practitioner"}],
        },
    )

    assert model is ProviderDirectoryPractitioner
    assert row["resource_id"] == "1000488800"
    assert row["npi"] == 1000488800


def test_organization_npi_falls_back_to_numeric_resource_id():
    model, row = importer.parse_fhir_resource(
        "source_a",
        {
            "resourceType": "Organization",
            "id": "1000053922",
            "active": True,
            "name": "Molina Shaped Organization",
        },
    )

    assert model is ProviderDirectoryOrganization
    assert row["resource_id"] == "1000053922"
    assert row["npi"] == 1000053922


def test_npi_skips_invalid_edifecs_identifier_before_valid_us_npi():
    assert importer._npi(
        {
            "identifier": [
                {
                    "system": "https://www.edifecs.com/fhir/identifier/id-npi",
                    "value": "12345678901",
                },
                {
                    "system": "http://hl7.org/fhir/sid/us-npi",
                    "value": "1234567893",
                },
            ]
        }
    ) == 1234567893


def test_npi_returns_none_when_all_npi_identifiers_are_invalid():
    assert importer._npi(
        {
            "identifier": [
                {
                    "system": "https://www.edifecs.com/fhir/identifier/id-npi",
                    "value": "12345678901",
                },
                {
                    "system": "http://hl7.org/fhir/sid/us-npi",
                    "value": "not-an-npi",
                },
            ]
        }
    ) is None


@pytest.mark.parametrize(
    "identifier",
    [
        {"system": "http://hl7.org/fhir/sid/us-npi", "value": "1234567893"},
        {"system": "https://example.test/fhir/npi", "value": "1234567893"},
    ],
)
def test_npi_accepts_ordinary_valid_compatible_identifiers(identifier):
    assert importer._npi({"identifier": [identifier]}) == 1234567893


def test_npi_prefers_exact_us_npi_system_over_earlier_compatible_match():
    assert importer._npi(
        {
            "identifier": [
                {"system": "https://example.test/fhir/npi", "value": "1234567893"},
                {"system": "http://hl7.org/fhir/sid/us-npi", "value": "1588616783"},
            ]
        }
    ) == 1588616783


def test_provider_directory_location_normalizes_valid_coordinates():
    model, row = importer.parse_fhir_resource(
        "source_a",
        {
            "resourceType": "Location",
            "id": "loc-geo",
            "position": {"latitude": "40.292922", "longitude": "-74.806333"},
            "address": {
                "line": ["1 Capital Way"],
                "city": "Pennington",
                "state": "NJ",
                "postalCode": "08534",
                "country": "US",
            },
        },
    )

    assert model is ProviderDirectoryLocation
    assert row["latitude"] == "40.292922"
    assert row["longitude"] == "-74.806333"


def test_provider_directory_location_restores_scaled_us_coordinates():
    model, row = importer.parse_fhir_resource(
        "source_a",
        {
            "resourceType": "Location",
            "id": "loc-scaled-geo",
            "position": {"latitude": "39104200", "longitude": "-84536000"},
            "address": {
                "line": ["100 Main St"],
                "city": "Cincinnati",
                "state": "OH",
                "postalCode": "45202",
                "country": "US",
            },
        },
    )

    assert model is ProviderDirectoryLocation
    assert row["latitude"] == "39.1042"
    assert row["longitude"] == "-84.536"


def test_provider_directory_location_drops_placeholder_or_implausible_coordinates():
    _, zero_row = importer.parse_fhir_resource(
        "source_a",
        {
            "resourceType": "Location",
            "id": "loc-zero-geo",
            "position": {"latitude": "0", "longitude": "0"},
            "address": {
                "line": ["100 Main St"],
                "city": "Chicago",
                "state": "IL",
                "postalCode": "60601",
                "country": "US",
            },
        },
    )
    _, implausible_row = importer.parse_fhir_resource(
        "source_a",
        {
            "resourceType": "Location",
            "id": "loc-implausible-geo",
            "position": {"latitude": "20022053", "longitude": "-100790700"},
            "address": {
                "line": ["100 Main St"],
                "city": "Chicago",
                "state": "IL",
                "postalCode": "60601",
                "country": "US",
            },
        },
    )

    assert zero_row["latitude"] is None
    assert zero_row["longitude"] is None
    assert implausible_row["latitude"] is None
    assert implausible_row["longitude"] is None


def test_provider_directory_location_can_skip_contact_derivation_for_streaming_batches():
    model, row = importer.parse_fhir_resource(
        "source_a",
        {
            "resourceType": "Location",
            "id": "loc-contact",
            "telecom": [
                {"system": "phone", "value": "+1 (312) 555-0100 ext. 45"},
                {"system": "fax", "value": "312.555.0199 # 22"},
            ],
            "address": {
                "line": ["100 Main St"],
                "city": "Chicago",
                "state": "IL",
                "postalCode": "60601",
                "country": "US",
            },
        },
        normalize_location_contacts=False,
    )

    assert model is ProviderDirectoryLocation
    assert row["telephone_number"] == "+1 (312) 555-0100 ext. 45"
    assert row["fax_number"] == "312.555.0199 # 22"
    assert "phone_number" not in row
    assert "fax_number_digits" not in row


def test_provider_directory_location_address_key_sql_recovers_numeric_state_fips():
    sql = importer.provider_directory_location_address_key_sql("mrf")

    assert "WHEN '42' THEN 'PA'" in sql
    assert "normalized_state" in sql
    assert "normalized_country" in sql
    assert "loc_src.country_code IS DISTINCT FROM" in sql
    assert "'840'" in sql
    assert "'001'" in sql
    assert 'LEFT JOIN "mrf"."geo_zip_lookup" AS geo' in sql
    assert "zip_restored_state" in sql
    assert "zip_restored_city" in sql
    assert "resolved_state" in sql
    assert "resolved_city" in sql
    assert "WHERE (loc_src.address_key IS NULL" in sql
    assert "OR loc_src.zip5 IS NULL" in sql
    assert "OR loc_src.city_name IS NULL" in sql
    assert "city_name = COALESCE(keyed.restored_city_name, loc.city_name)" in sql
    assert "state_name = COALESCE(keyed.restored_state_name, loc.state_name)" in sql
    assert "country_code = COALESCE(keyed.normalized_country, loc.country_code)" in sql


def test_provider_directory_location_address_key_sql_can_skip_zip_state_restore():
    sql = importer.provider_directory_location_address_key_sql("mrf", restore_state_from_zip=False)

    assert "geo_zip_lookup" not in sql
    assert "END AS zip_restored_state" in sql
    assert "END AS zip_restored_city" in sql
    assert "NULL::varchar) AS resolved_state" in sql
    assert "NULL::varchar) AS resolved_city" in sql


def test_address_key_sql_scopes_to_run_and_sources():
    sql = importer.provider_directory_location_address_key_sql(
        "mrf",
        run_id="run_1",
        source_ids=["source_a", "source_b"],
    )

    assert "loc_src.last_seen_run_id = CAST(:run_id AS varchar)" in sql
    assert "loc_src.source_id = ANY(CAST(:source_ids AS varchar[]))" in sql


def test_provider_directory_location_address_key_sql_can_scope_to_seen_stage():
    sql = importer.provider_directory_location_address_key_sql(
        "mrf",
        run_id="run_1",
        seen_table="provider_directory_import_seen_stage_test",
    )

    assert "SELECT DISTINCT seen.source_id" in sql
    assert 'FROM "mrf"."provider_directory_import_seen_stage_test" AS seen' in sql
    assert 'JOIN "mrf"."provider_directory_location" AS loc_src' in sql
    assert "loc_src.source_id = seen_scope.source_id" in sql
    assert "loc_src.resource_id = seen_scope.resource_id" in sql
    assert "seen.resource_type = 'Location'" in sql
    assert "AND seen.run_id = CAST(:run_id AS varchar)" in sql
    assert "EXISTS (" not in sql
    assert "loc_src.last_seen_run_id = CAST(:run_id AS varchar)" not in sql


def test_provider_directory_location_address_key_batch_sql_uses_keyset_limit():
    sql = importer.provider_directory_location_address_key_batch_sql("mrf", run_id="run_1")

    assert 'FROM "mrf"."provider_directory_location" AS loc_src' in sql
    assert "loc_src.last_seen_run_id = CAST(:run_id AS varchar)" in sql
    assert "CAST(:after_source_id AS varchar) IS NULL" in sql
    assert "(CAST(:after_source_id AS varchar), CAST(:after_resource_id AS varchar))" in sql
    assert "(loc_src.source_id, loc_src.resource_id)" in sql
    assert "LIMIT CAST(:batch_size AS integer)" in sql
    assert "zip_restored_city" in sql
    assert "resolved_city" in sql
    assert "city_name = COALESCE(keyed.restored_city_name, loc.city_name)" in sql
    assert "SELECT COUNT(*) FROM candidates" in sql
    assert "SELECT COUNT(*) FROM updated" in sql
    assert "last_source_id" in sql
    assert "last_resource_id" in sql


def test_address_key_batch_sql_scopes_to_seen_stage():
    sql = importer.provider_directory_location_address_key_batch_sql(
        "mrf",
        run_id="run_1",
        seen_table="provider_directory_import_seen_stage_test",
    )

    assert 'FROM "mrf"."provider_directory_location" AS loc_src' in sql
    assert 'FROM "mrf"."provider_directory_import_seen_stage_test" AS seen' in sql
    assert "seen.resource_type = 'Location'" in sql
    assert "AND seen.run_id = CAST(:run_id AS varchar)" in sql
    assert "AND seen.source_id = loc_src.source_id" in sql
    assert "AND seen.resource_id = loc_src.resource_id" in sql
    assert "loc_src.last_seen_run_id = CAST(:run_id AS varchar)" not in sql
    assert "CAST(:after_source_id AS varchar) IS NULL" in sql
    assert "LIMIT CAST(:batch_size AS integer)" in sql


@pytest.mark.asyncio
async def test_publish_provider_directory_location_address_keys_batches_without_seen_table(monkeypatch):
    class Row:
        def __init__(self, **values):
            self._mapping = values

    monkeypatch.setattr(importer, "_has_address_canon_functions", AsyncMock(return_value=True))
    monkeypatch.setattr(importer, "_table_exists", AsyncMock(return_value=True))
    first = AsyncMock(
        side_effect=[
            Row(candidate_rows=2, updated_rows=2, last_source_id="source_a", last_resource_id="loc-2"),
            Row(candidate_rows=1, updated_rows=0, last_source_id="source_b", last_resource_id="loc-3"),
            Row(candidate_rows=0, updated_rows=0, last_source_id=None, last_resource_id=None),
        ]
    )
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "first", first)
    monkeypatch.setattr(importer.db, "status", status)

    stamped = await importer.publish_provider_directory_location_address_keys("mrf", batch_size=2)

    assert stamped == 2
    assert first.await_count == 3
    assert first.await_args_list[0].kwargs["after_source_id"] is None
    assert first.await_args_list[1].kwargs["after_source_id"] == "source_a"
    assert first.await_args_list[1].kwargs["after_resource_id"] == "loc-2"
    assert first.await_args_list[2].kwargs["after_source_id"] == "source_b"
    assert first.await_args_list[2].kwargs["after_resource_id"] == "loc-3"
    assert all(call.kwargs["batch_size"] == 2 for call in first.await_args_list)
    status.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_provider_directory_location_address_keys_batches_with_seen_table(monkeypatch):
    class Row:
        def __init__(self, **values):
            self._mapping = values

    monkeypatch.setattr(importer, "_has_address_canon_functions", AsyncMock(return_value=True))
    monkeypatch.setattr(importer, "_table_exists", AsyncMock(return_value=True))
    first = AsyncMock(
        side_effect=[
            Row(candidate_rows=2, updated_rows=1, last_source_id="source_a", last_resource_id="loc-2"),
            Row(candidate_rows=0, updated_rows=0, last_source_id=None, last_resource_id=None),
        ]
    )
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "first", first)
    monkeypatch.setattr(importer.db, "status", status)

    stamped = await importer.publish_provider_directory_location_address_keys(
        "mrf",
        run_id="run_1",
        seen_table="provider_directory_import_seen_stage_test",
        batch_size=2,
    )

    assert stamped == 1
    assert first.await_count == 2
    assert first.await_args_list[0].kwargs["run_id"] == "run_1"
    assert first.await_args_list[0].kwargs["after_source_id"] is None
    assert first.await_args_list[1].kwargs["after_source_id"] == "source_a"
    assert first.await_args_list[1].kwargs["after_resource_id"] == "loc-2"
    assert all(call.kwargs["batch_size"] == 2 for call in first.await_args_list)
    status.assert_not_awaited()


def test_provider_directory_location_coordinate_batch_sql_scales_bad_coordinates():
    sql = importer.provider_directory_location_coordinate_batch_sql(
        "mrf",
        run_id="run_1",
        source_ids=["source_a"],
        seen_table="provider_directory_import_seen_stage_test",
    )

    assert 'UPDATE "mrf"."provider_directory_location" AS loc' in sql
    assert "loc_src.latitude" in sql
    assert "loc_src.longitude" in sql
    assert "::numeric / 1000000" in sql
    assert "::numeric / 10000000" in sql
    assert "NOT (ABS((loc_src.latitude)::numeric) < 0.0000001" in sql
    assert 'FROM "mrf"."provider_directory_import_seen_stage_test" AS seen' in sql
    assert "seen.resource_type = 'Location'" in sql
    assert "AND seen.run_id = CAST(:run_id AS varchar)" in sql
    assert "AND seen.source_id = loc_src.source_id" in sql
    assert "AND seen.resource_id = loc_src.resource_id" in sql
    assert "loc_src.source_id = ANY(CAST(:source_ids AS varchar[]))" in sql
    assert "LIMIT CAST(:batch_size AS integer)" in sql


@pytest.mark.asyncio
async def test_backfill_provider_directory_location_coordinates_batches(monkeypatch):
    class Row:
        def __init__(self, **values):
            self._mapping = values

    monkeypatch.setattr(importer, "_table_exists", AsyncMock(return_value=True))
    first = AsyncMock(
        side_effect=[
            Row(candidate_rows=2, updated_rows=2, last_source_id="source_a", last_resource_id="loc-2"),
            Row(candidate_rows=1, updated_rows=1, last_source_id="source_a", last_resource_id="loc-3"),
            Row(candidate_rows=0, updated_rows=0, last_source_id=None, last_resource_id=None),
        ]
    )
    monkeypatch.setattr(importer.db, "first", first)

    updated = await importer.backfill_provider_directory_location_coordinates(
        "mrf",
        run_id="run_1",
        source_ids=["source_a"],
        seen_table="provider_directory_import_seen_stage_test",
        batch_size=2,
    )

    assert updated == 3
    assert first.await_count == 3
    assert first.await_args_list[0].kwargs["run_id"] == "run_1"
    assert first.await_args_list[0].kwargs["source_ids"] == ["source_a"]
    assert first.await_args_list[0].kwargs["after_source_id"] is None
    assert first.await_args_list[1].kwargs["after_source_id"] == "source_a"
    assert first.await_args_list[1].kwargs["after_resource_id"] == "loc-2"
    assert first.await_args_list[2].kwargs["after_source_id"] == "source_a"
    assert first.await_args_list[2].kwargs["after_resource_id"] == "loc-3"
    assert all(call.kwargs["batch_size"] == 2 for call in first.await_args_list)


def test_upsert_changed_row_predicate_ignores_run_metadata_columns():
    table = ProviderDirectoryLocation.__table__
    columns = [column.name for column in table.columns]
    primary_keys = [column.name for column in table.primary_key.columns]
    statement = importer.pg_insert(table).values(
        {
            "source_id": "source_a",
            "resource_id": "loc-1",
            "first_line": "100 Main St",
            "last_seen_run_id": "run_2",
        }
    )

    predicate = importer._upsert_changed_row_predicate(
        table,
        statement,
        columns,
        primary_keys,
    )
    sql = str(predicate.compile(dialect=postgresql.dialect()))

    assert "first_line" in sql
    assert "CAST" in sql
    assert "JSONB" in sql
    assert "provider_directory_location.address_key IS DISTINCT FROM CASE" in sql
    assert "WHEN (excluded.address_key IS NOT NULL) THEN excluded.address_key" in sql
    assert "ELSE mrf.provider_directory_location.address_key END" in sql
    assert "last_seen_run_id" not in sql
    assert "observed_at" not in sql
    assert "updated_at" not in sql


def test_copy_stage_changed_where_ignores_run_metadata_columns():
    table = ProviderDirectoryLocation.__table__
    columns = [column.name for column in table.columns]
    primary_keys = [column.name for column in table.primary_key.columns]

    sql = importer._copy_stage_changed_where_sql(
        table,
        columns,
        primary_keys,
        target_alias="target_row",
        stage_alias="stage_row",
    )

    assert 'target_row."source_id" IS NULL' in sql
    assert 'target_row."telecom"::jsonb IS DISTINCT FROM stage_row."telecom"::jsonb' in sql
    assert "last_seen_run_id" not in sql
    assert "observed_at" not in sql
    assert "updated_at" not in sql


def test_location_upsert_preserves_unchanged_address_key():
    table = ProviderDirectoryLocation.__table__
    columns = [column.name for column in table.columns]
    primary_keys = [column.name for column in table.primary_key.columns]

    conflict_where = importer._copy_upsert_changed_where_sql(
        table,
        columns,
        primary_keys,
    )
    stage_where = importer._copy_stage_changed_where_sql(
        table,
        columns,
        primary_keys,
        target_alias="target_row",
        stage_alias="stage_row",
    )

    assert (
        '"provider_directory_location"."address_key" IS DISTINCT FROM CASE '
        'WHEN EXCLUDED."address_key" IS NOT NULL THEN EXCLUDED."address_key" '
        'WHEN "provider_directory_location"."first_line" IS DISTINCT FROM EXCLUDED."first_line"'
    ) in conflict_where
    assert 'ELSE "provider_directory_location"."address_key" END' in conflict_where
    assert (
        'target_row."address_key" IS DISTINCT FROM CASE '
        'WHEN stage_row."address_key" IS NOT NULL THEN stage_row."address_key" '
        'WHEN target_row."first_line" IS DISTINCT FROM stage_row."first_line"'
    ) in stage_where
    assert 'ELSE target_row."address_key" END' in stage_where


def test_provider_directory_source_seed_upsert_preserves_existing_probe_state():
    sql = importer._effective_update_sql(
        ProviderDirectorySource.__table__,
        "last_probe_status",
        target_prefix="target_row",
        incoming_prefix="stage_row",
    )

    assert (
        'CASE WHEN stage_row."last_probe_status" IS NULL '
        'THEN target_row."last_probe_status" '
        'ELSE stage_row."last_probe_status" END'
    ) == sql


def test_provider_directory_source_seed_upsert_preserves_serving_endpoint():
    table = ProviderDirectorySource.__table__
    sql = importer._effective_update_sql(
        table,
        "endpoint_id",
        target_prefix="target_row",
        incoming_prefix="stage_row",
    )

    assert sql == (
        'COALESCE(target_row."endpoint_id", stage_row."endpoint_id")'
    )

    statement = importer.pg_insert(table).values(
        source_id="source_a",
        org_name="ALOHR",
        endpoint_id="endpoint_graphql",
    )
    expression = importer._effective_update_expression(
        table,
        statement,
        "endpoint_id",
    )
    expression_sql = str(
        expression.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "coalesce" in expression_sql.lower()
    assert "provider_directory_source.endpoint_id" in expression_sql
    assert "excluded.endpoint_id" in expression_sql


def test_canonical_resource_compact_payload_update_preserves_only_matching_payload():
    table = ProviderDirectoryCanonicalResource.__table__
    sql = importer._effective_update_sql(
        table,
        "payload_json",
        target_prefix="target_row",
        incoming_prefix="stage_row",
    )

    assert sql == (
        'CASE WHEN stage_row."payload_json" IS NOT NULL '
        'THEN stage_row."payload_json" '
        'WHEN target_row."payload_hash" IS DISTINCT FROM stage_row."payload_hash" '
        'THEN NULL ELSE target_row."payload_json" END'
    )

    statement = importer.pg_insert(table).values(
        canonical_api_base="https://payer.example/fhir",
        resource_type="PractitionerRole",
        resource_id="role-1",
        payload_hash="new-hash",
        payload_json=None,
    )
    expression = importer._effective_update_expression(
        table,
        statement,
        "payload_json",
    )
    expression_sql = str(
        expression.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "payload_hash IS DISTINCT FROM excluded.payload_hash" in expression_sql
    assert "ELSE" in expression_sql


def test_provider_directory_source_seed_upsert_merges_metadata_json():
    sql = importer._effective_update_sql(
        ProviderDirectorySource.__table__,
        "metadata_json",
        target_prefix="target_row",
        incoming_prefix="stage_row",
    )

    merged = (
        'COALESCE(target_row."metadata_json"::jsonb, \'{}\'::jsonb) '
        '|| COALESCE(stage_row."metadata_json"::jsonb, \'{}\'::jsonb)'
    )
    assert sql == (
        'CASE WHEN COALESCE(stage_row."metadata_json"::jsonb ->> '
        "'provider_directory_acquisition_enabled', 'false') = 'true' "
        f"THEN ({merged}) - 'provider_directory_acquisition_blocked_reason' "
        f"ELSE {merged} END"
    )


def test_provider_directory_source_seed_upsert_clears_obsolete_block_reason():
    table = ProviderDirectorySource.__table__
    statement = importer.pg_insert(table).values(
        source_id="source_a",
        org_name="AmeriHealth Caritas",
        metadata_json={"provider_directory_acquisition_enabled": True},
    )

    expression = importer._effective_update_expression(
        table,
        statement,
        "metadata_json",
    )
    sql = str(
        expression.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "provider_directory_acquisition_enabled" in sql
    assert "provider_directory_acquisition_blocked_reason" in sql
    assert "CASE WHEN" in sql


@pytest.mark.asyncio
async def test_ensure_provider_directory_model_columns_adds_missing_stale_table_columns(monkeypatch):
    existing_columns = [
        {"column_name": column.name}
        for column in ProviderDirectoryLocation.__table__.columns
        if column.name != "phone_number"
    ]
    all_mock = AsyncMock(return_value=existing_columns)
    status_mock = AsyncMock()
    monkeypatch.setattr(importer.db, "all", all_mock)
    monkeypatch.setattr(importer.db, "status", status_mock)

    await importer._ensure_provider_directory_model_columns(
        ProviderDirectoryLocation,
        "mrf",
    )

    all_mock.assert_awaited_once()
    status_mock.assert_awaited_once()
    sql = status_mock.await_args.args[0]
    assert 'ALTER TABLE "mrf"."provider_directory_location" ADD COLUMN IF NOT EXISTS phone_number' in sql
    assert "VARCHAR(15)" in sql


@pytest.mark.asyncio
async def test_ensure_provider_directory_model_columns_ignores_empty_compiled_column(monkeypatch):
    fake_column = SimpleNamespace(name="missing_column")
    fake_model = SimpleNamespace(
        __tablename__="provider_directory_fake",
        __table__=SimpleNamespace(columns=(fake_column,)),
    )
    compiled_column = Mock()
    compiled_column.compile.return_value = " "
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "all", AsyncMock(return_value=[]))
    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(importer, "CreateColumn", Mock(return_value=compiled_column))

    await importer._ensure_provider_directory_model_columns(fake_model, "mrf")

    status.assert_not_awaited()


@pytest.mark.asyncio
async def test_ensure_provider_directory_source_column_types_widens_data_quality_checked(monkeypatch):
    scalar_mock = AsyncMock(return_value="character varying")
    status_mock = AsyncMock()
    monkeypatch.setattr(importer.db, "scalar", scalar_mock)
    monkeypatch.setattr(importer.db, "status", status_mock)

    await importer._ensure_provider_directory_source_column_types("mrf")

    status_mock.assert_awaited_once()
    sql = status_mock.await_args.args[0]
    assert 'ALTER TABLE IF EXISTS "mrf"."provider_directory_source"' in sql
    assert "ALTER COLUMN data_quality_checked TYPE text" in sql


@pytest.mark.asyncio
async def test_ensure_provider_directory_source_column_types_skips_current_text_column(monkeypatch):
    scalar_mock = AsyncMock(return_value="text")
    status_mock = AsyncMock()
    monkeypatch.setattr(importer.db, "scalar", scalar_mock)
    monkeypatch.setattr(importer.db, "status", status_mock)

    await importer._ensure_provider_directory_source_column_types("mrf")

    scalar_mock.assert_awaited_once()
    status_mock.assert_not_awaited()


def test_source_catalog_stale_cleanup_only_runs_for_unfiltered_full_refresh():
    assert importer._is_source_catalog_stale_cleanup_enabled(
        stale_cleanup=True,
        full_refresh=True,
        source_query=None,
        limit=None,
    )
    assert not importer._is_source_catalog_stale_cleanup_enabled(
        stale_cleanup=True,
        full_refresh=False,
        source_query=None,
        limit=None,
    )
    assert not importer._is_source_catalog_stale_cleanup_enabled(
        stale_cleanup=True,
        full_refresh=True,
        source_query="cigna",
        limit=None,
    )
    assert not importer._is_source_catalog_stale_cleanup_enabled(
        stale_cleanup=True,
        full_refresh=True,
        source_query=None,
        limit=10,
    )
    assert not importer._is_source_catalog_stale_cleanup_enabled(
        stale_cleanup=True,
        full_refresh=True,
        source_query=None,
        limit=None,
        requested_source_ids=["source_a"],
    )
    assert not importer._is_source_catalog_stale_cleanup_enabled(
        stale_cleanup=True,
        full_refresh=True,
        source_query=None,
        limit=None,
        retest_results_configured=False,
    )


def test_provider_directory_monthly_full_refresh_preset_sets_schedulable_defaults():
    task = importer._apply_provider_directory_refresh_preset(
        {
            "refresh_preset": "monthly_full",
            "publish_corroboration": False,
            "page_count": 250,
        }
    )

    assert task["refresh_preset"] == "monthly-full"
    assert task["import_resources"] is True
    assert task["full_refresh"] is True
    assert task["stale_cleanup"] is True
    assert task["publish_artifacts"] is True
    assert task["open_only"] is False
    assert task["include_auth_required"] is True
    assert task["bulk_export"] is True
    assert task["include_supplemental_catalogs"] is True
    assert task["publish_corroboration"] is False
    assert task["page_count"] == 250


def test_bulk_export_mode_metrics_distinguish_requested_from_effective():
    assert importer._bulk_export_mode_metrics(True, {}) == {
        "requested": True,
        "effective": False,
        "requested_resource_fetches": 0,
        "effective_resource_fetches": 0,
        "ineligible_resource_fetches": 0,
        "checkpoint_blocked_resource_fetches": 0,
        "rest_fallback_resource_fetches": 0,
    }
    assert importer._bulk_export_mode_metrics(
        True,
        {
            "Practitioner": {
                "bulk_export_requested_sources": 3,
                "bulk_export_ineligible_sources": 1,
                "bulk_export_rest_fallback_sources": 1,
                "bulk_export_sources": 2,
            },
            "Location": {
                "bulk_export_requested_sources": 1,
                "bulk_export_checkpoint_blocked_sources": 1,
                "bulk_export_rest_fallback_sources": 1,
                "bulk_export_sources": 0,
            },
        },
    ) == {
        "requested": True,
        "effective": True,
        "requested_resource_fetches": 4,
        "effective_resource_fetches": 2,
        "ineligible_resource_fetches": 1,
        "checkpoint_blocked_resource_fetches": 1,
        "rest_fallback_resource_fetches": 2,
    }


def test_cigna_bulk_request_is_ineligible():
    source_lookup = {"api_base": importer.CIGNA_PROVIDER_DIRECTORY_BASE}

    assert not importer._is_source_bulk_export_eligible(source_lookup)
    assert (
        importer._source_bulk_export_selection(
            source_lookup,
            True,
            per_resource_limit=25,
        )
        == importer.BULK_EXPORT_SELECTION_SOURCE_INELIGIBLE
    )
    assert not importer._is_source_bulk_export_effective(
        source_lookup,
        True,
        per_resource_limit=25,
    )


def test_aetna_bulk_source_is_eligible_and_unbounded_use_requires_checkpoint():
    source_lookup = {"api_base": importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}
    checkpoint_context = _bulk_test_context()

    assert importer._is_source_bulk_export_eligible(source_lookup)
    assert not importer._is_source_bulk_export_effective(
        source_lookup,
        True,
        per_resource_limit=0,
    )
    assert importer._is_source_bulk_export_effective(
        source_lookup,
        True,
        per_resource_limit=0,
        checkpoint_context=checkpoint_context,
    )
    assert importer._is_source_bulk_export_effective(
        source_lookup,
        True,
        per_resource_limit=25,
    )


def test_ordinary_rest_source_is_not_bulk_eligible():
    source_lookup = {"api_base": "https://example.test/fhir"}

    assert not importer._is_source_bulk_export_eligible(source_lookup)
    assert not importer._is_source_bulk_export_effective(
        source_lookup,
        True,
        per_resource_limit=25,
    )


def test_bulk_eligibility_metadata_is_explicit_extension_point():
    source_lookup = {
        "api_base": "https://bulk-capable.example/fhir",
        "metadata_json": {
            importer.PROVIDER_DIRECTORY_BULK_EXPORT_ELIGIBLE_METADATA_KEY: True,
        },
    }

    assert importer._is_source_bulk_export_eligible(source_lookup)
    assert importer._is_source_bulk_export_effective(
        source_lookup,
        True,
        per_resource_limit=25,
    )


def test_resource_stats_record_ineligible_rest_fallback():
    fetch_result = importer.ResourceFetchResult(
        ProviderDirectoryPractitioner,
        [],
        0,
        0,
        1,
        True,
        False,
        False,
        False,
        False,
        fetch_mode="paged",
    )
    resource_stats_by_type: dict[str, dict[str, Any]] = {}

    importer._record_resource_fetch_stats(
        resource_stats_by_type,
        "Practitioner",
        fetch_result,
        bulk_export_selection=importer.BULK_EXPORT_SELECTION_SOURCE_INELIGIBLE,
    )

    practitioner_stats = resource_stats_by_type["Practitioner"]
    assert practitioner_stats["bulk_export_requested_sources"] == 1
    assert practitioner_stats["bulk_export_ineligible_sources"] == 1
    assert practitioner_stats["bulk_export_rest_fallback_sources"] == 1


def test_aetna_catalog_opt_in_enables_checkpointed_bulk_for_general_refresh():
    source_lookup = {
        "api_base": importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE,
        "metadata_json": {"provider_directory_bulk_export_auto_enabled": True},
    }

    assert not importer._is_source_bulk_export_effective(
        source_lookup,
        False,
        per_resource_limit=0,
    )
    assert importer._is_source_bulk_export_effective(
        source_lookup,
        False,
        per_resource_limit=0,
        checkpoint_context=_bulk_test_context(),
    )


def test_provider_directory_cli_refresh_preset_leaves_defaults_unset_for_preset(monkeypatch):
    calls = []

    def fake_initiate_provider_directory_fhir(**kwargs):
        calls.append(kwargs)
        return "provider-directory-fhir-task"

    monkeypatch.setattr(process_cli, "initiate_provider_directory_fhir", fake_initiate_provider_directory_fhir)
    monkeypatch.setattr(process_cli, "_run", lambda task: calls.append({"run": task}))

    result = CliRunner().invoke(
        process_cli.provider_directory_fhir,
        ["--refresh-preset", "monthly-full", "--seed-only", "--no-probe"],
    )

    assert result.exit_code == 0
    assert calls[0]["refresh_preset"] == "monthly-full"
    assert calls[0]["import_resources"] is None
    assert calls[0]["full_refresh"] is None
    assert calls[0]["publish_after_acquisition"] is False
    assert calls[0]["seed_only"] is True
    assert calls[0]["probe"] is False


def test_provider_directory_refresh_preset_rejects_unknown_value():
    with pytest.raises(ValueError, match="Unsupported Provider Directory refresh_preset"):
        importer._apply_provider_directory_refresh_preset({"refresh_preset": "weekly"})


@pytest.mark.asyncio
async def test_delete_stale_provider_directory_source_catalog_prunes_dependent_rows(monkeypatch):
    status_mock = AsyncMock(return_value=1)
    monkeypatch.setattr(importer.db, "status", status_mock)

    cleanup = await importer._delete_stale_provider_directory_source_catalog(
        ["source_b", "source_a", "source_a", ""]
    )

    assert cleanup["deleted"]["provider_directory_source"] == 1
    assert cleanup["protected_source_ids_missing"] == []
    assert status_mock.await_count == len(importer.SOURCE_CATALOG_STALE_TABLE_MODELS) + 1
    for call in status_mock.await_args_list:
        assert call.kwargs["source_ids"] == ["source_a", "source_b"]
        assert "NOT (source_id = ANY(CAST(:source_ids AS varchar[])))" in call.args[0]
    assert '"mrf"."provider_directory_source"' in status_mock.await_args_list[-1].args[0]


@pytest.mark.asyncio
async def test_source_catalog_cleanup_retains_manifest_pinned_hap_alias(monkeypatch):
    pinned_source_id = "pdfhir_eee751f2c17d5c473daf3060"
    sibling_source_id = "pdfhir_f9d6f8a84714e7c298323833"
    all_mock = AsyncMock(return_value=[{"source_id": pinned_source_id}])
    status_mock = AsyncMock(return_value=0)
    monkeypatch.setattr(importer.db, "all", all_mock)
    monkeypatch.setattr(importer.db, "status", status_mock)

    cleanup = await importer._delete_stale_provider_directory_source_catalog(
        [sibling_source_id],
        protected_source_ids=[pinned_source_id],
    )

    assert cleanup == {"deleted": {}, "protected_source_ids_missing": []}
    assert all_mock.await_args.kwargs["source_ids"] == [pinned_source_id]
    for call in status_mock.await_args_list:
        assert call.kwargs["source_ids"] == [pinned_source_id, sibling_source_id]


@pytest.mark.asyncio
async def test_source_catalog_cleanup_reports_missing_protected_source_and_fails_closed(monkeypatch):
    pinned_source_id = "pdfhir_eee751f2c17d5c473daf3060"
    all_mock = AsyncMock(return_value=[])
    status_mock = AsyncMock()
    monkeypatch.setattr(importer.db, "all", all_mock)
    monkeypatch.setattr(importer.db, "status", status_mock)

    cleanup = await importer._delete_stale_provider_directory_source_catalog(
        ["pdfhir_f9d6f8a84714e7c298323833"],
        protected_source_ids=[pinned_source_id],
    )

    assert cleanup == {
        "deleted": {},
        "protected_source_ids_missing": [pinned_source_id],
    }
    status_mock.assert_not_awaited()


def test_catalog_protected_source_ids_include_manifest_acquisition_hap_source():
    assert "pdfhir_eee751f2c17d5c473daf3060" in importer._configured_catalog_protected_source_ids({})


@pytest.mark.asyncio
async def test_delete_stale_resource_rows_prunes_canonical_source_edges(monkeypatch):
    calls: list[tuple[str, dict[str, Any]]] = []

    async def fake_status(sql, **params):
        calls.append((sql, params))
        if '"provider_directory_location"' in sql:
            return 4
        if '"provider_directory_source_resource"' in sql:
            return 2
        return 0

    monkeypatch.setattr(importer.db, "status", fake_status)

    deleted = await importer._delete_stale_resource_rows(
        ProviderDirectoryLocation,
        "source_a",
        "run_1",
        use_seen_table=True,
    )

    assert deleted == 4
    assert len(calls) == 2
    edge_sql, edge_params = calls[0]
    resource_sql, resource_params = calls[1]
    assert '"provider_directory_source_resource"' in edge_sql
    assert '"provider_directory_location"' in resource_sql
    assert "provider_directory_import_seen" in edge_sql
    assert "provider_directory_import_seen" in resource_sql
    assert "seen.resource_id = edge.resource_id" in edge_sql
    assert "seen.resource_id = resource.resource_id" in resource_sql
    assert edge_params == {
        "source_id": "source_a",
        "run_id": "run_1",
        "resource_type": "Location",
    }
    assert resource_params == edge_params


@pytest.mark.asyncio
async def test_upsert_resource_rows_tracks_seen_and_skips_unchanged(monkeypatch):
    seen_calls = []
    upsert_calls = []

    async def fake_mark_seen(model, rows, run_id, **kwargs):
        seen_calls.append((model, rows, run_id, kwargs))
        return len(rows)

    async def fake_upsert(model, rows, **kwargs):
        upsert_calls.append((model, rows, kwargs))
        return len(rows)

    monkeypatch.setattr(importer, "_mark_resource_rows_seen", fake_mark_seen)
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)

    rows = [{"source_id": "source_a", "resource_id": "loc-1"}]
    written = await importer._upsert_resource_rows(
        ProviderDirectoryLocation,
        rows,
        run_id="run_1",
        track_seen=True,
    )

    assert written == 1
    assert seen_calls == [(ProviderDirectoryLocation, rows, "run_1", {"seen_table": None})]
    assert upsert_calls == [(ProviderDirectoryLocation, rows, {"skip_unchanged": True})]


@pytest.mark.asyncio
async def test_upsert_resource_rows_batches_location_contact_derivation(monkeypatch):
    """Verify this provider-directory regression contract."""
    seen_batches = []
    upsert_calls = []

    def fake_canonicalize_contact_batch(rows):
        rows = list(rows)
        seen_batches.append(rows)
        return [
            {
                "phone_number": "3125550100",
                "phone_extension": "45",
                "fax_number_digits": "3125550199",
                "fax_extension": None,
            },
            {
                "phone_number": "3125550101",
                "phone_extension": None,
                "fax_number_digits": None,
                "fax_extension": None,
            },
        ]

    async def fake_upsert(model, rows, **kwargs):
        upsert_calls.append((model, rows, kwargs))
        return len(rows)

    monkeypatch.setattr(importer, "canonicalize_contact_batch", fake_canonicalize_contact_batch)
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)

    observed_rows = [
        {
            "source_id": "source_a",
            "resource_id": "loc-1",
            "telephone_number": "+1 (312) 555-0100 ext. 45",
            "fax_number": "312.555.0199",
            "country_code": "US",
        },
        {
            "source_id": "source_a",
            "resource_id": "loc-2",
            "telephone_number": "312-555-0101",
            "country_code": "US",
        },
    ]
    written = await importer._upsert_resource_rows(
        ProviderDirectoryLocation,
        observed_rows,
        run_id="run_1",
        track_seen=False,
    )

    assert written == 2
    assert seen_batches == [
        [
            ("+1 (312) 555-0100 ext. 45", "312.555.0199", "US"),
            ("312-555-0101", None, "US"),
        ]
    ]
    assert upsert_calls == [(ProviderDirectoryLocation, observed_rows, {"skip_unchanged": False})]
    assert observed_rows[0]["phone_number"] == "3125550100"
    assert observed_rows[0]["phone_extension"] == "45"
    assert observed_rows[0]["fax_number_digits"] == "3125550199"
    assert observed_rows[1]["phone_number"] == "3125550101"


@pytest.mark.asyncio
async def test_upsert_resource_rows_writes_canonical_resource_and_source_edges(monkeypatch):
    seen_calls = []
    upsert_calls = []

    async def fake_mark_seen(model, rows, run_id, **kwargs):
        seen_calls.append((model, rows, run_id, kwargs))
        return len(rows)

    async def fake_upsert(model, rows, **kwargs):
        upsert_calls.append((model, rows, kwargs))
        return len(rows)

    monkeypatch.setattr(importer, "_mark_resource_rows_seen", fake_mark_seen)
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)

    observed_rows = [
        {
            "source_id": "source_a",
            "resource_id": "loc-1",
            "resource_url": "https://payer.example/fhir/Location/loc-1",
            "name": "Clinic",
            "last_seen_run_id": "run_1",
        },
        {
            "source_id": "source_b",
            "resource_id": "loc-1",
            "resource_url": "https://payer.example/fhir/Location/loc-1",
            "name": "Clinic",
            "last_seen_run_id": "run_1",
        },
    ]

    written = await importer._upsert_resource_rows(
        ProviderDirectoryLocation,
        observed_rows,
        run_id="run_1",
        track_seen=True,
        canonical_api_base="https://payer.example/fhir/",
        source_ids=["source_a", "source_b"],
    )

    assert written == 2
    canonical_call = upsert_calls[0]
    assert canonical_call[0] is ProviderDirectoryCanonicalResource
    assert canonical_call[2] == {"skip_unchanged": True}
    assert len(canonical_call[1]) == 1
    assert canonical_call[1][0]["canonical_api_base"] == "https://payer.example/fhir"
    assert canonical_call[1][0]["resource_type"] == "Location"
    assert canonical_call[1][0]["resource_id"] == "loc-1"
    assert canonical_call[1][0]["payload_json"]["name"] == "Clinic"
    assert canonical_call[1][0]["payload_hash"]

    edge_call = upsert_calls[1]
    assert edge_call[0] is ProviderDirectorySourceResource
    assert sorted(observed_row_map["source_id"] for observed_row_map in edge_call[1]) == ["source_a", "source_b"]
    assert {observed_row_map["resource_id"] for observed_row_map in edge_call[1]} == {"loc-1"}

    assert upsert_calls[2] == (ProviderDirectoryLocation, observed_rows, {"skip_unchanged": True})
    assert seen_calls == [(ProviderDirectoryLocation, observed_rows, "run_1", {"seen_table": None})]


@pytest.mark.asyncio
async def test_dataset_backed_upsert_stores_payload_only_in_endpoint_dataset(monkeypatch):
    upsert_calls = []

    async def fake_upsert(model, resource_rows, **kwargs):
        upsert_calls.append((model, resource_rows, kwargs))
        return len(resource_rows)

    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)

    resource_rows = [
        {
            "source_id": "source_a",
            "resource_id": "role-1",
            "resource_url": "https://payer.example/fhir/PractitionerRole/role-1",
            "practitioner_ref": "Practitioner/practitioner-1",
            "last_seen_run_id": "run_1",
        }
    ]

    written = await importer._upsert_resource_rows(
        ProviderDirectoryPractitionerRole,
        resource_rows,
        run_id="run_1",
        track_seen=False,
        canonical_api_base="https://payer.example/fhir",
        source_ids=["source_a"],
        dataset_id="dataset_1",
    )

    assert written == 1
    dataset_call = upsert_calls[0]
    canonical_call = upsert_calls[1]
    assert dataset_call[0] is ProviderDirectoryDatasetResource
    assert dataset_call[1][0]["payload_json"]["practitioner_ref"] == (
        "Practitioner/practitioner-1"
    )
    assert canonical_call[0] is ProviderDirectoryCanonicalResource
    assert canonical_call[1][0]["payload_json"] is None
    assert canonical_call[1][0]["payload_hash"] == dataset_call[1][0]["payload_hash"]
    assert upsert_calls[-1][0] is ProviderDirectoryPractitionerRole


@pytest.mark.asyncio
async def test_import_alohr_graphql_source_group_writes_existing_resource_tables(monkeypatch):
    """Verify this provider-directory regression contract."""
    calls: list[tuple[str, str | None]] = []

    async def fake_fetch_page(query, root_key, item_key, *, next_token, timeout):
        calls.append((root_key, next_token))
        if root_key == "providers":
            return (
                [
                    {
                        "providerId": "prac-1234567893",
                        "firstName": "Ada",
                        "lastName": "Lovelace",
                        "npi": "1234567893",
                        "phone": "+12055550123",
                        "address": {"street1": "100 Main St", "city": "Birmingham", "state": "AL", "zip": "35203"},
                    }
                ],
                None,
                None,
            )
        return (
            [
                {
                    "orgId": "org-1992793046",
                    "name": "New Day Senior Care",
                    "npi": "1992793046",
                    "addresses": [{"street1": "241 Robert K Wilson Dr", "city": "Carrollton", "state": "AL", "zip": "35447"}],
                    "contacts": [{"contacts": [{"system": "phone", "value": "+12055550999"}]}],
                }
            ],
            None,
            None,
        )

    upsert_rows_by_model: dict[type, list[dict[str, Any]]] = {}

    async def fake_upsert(model, rows, **_kwargs):
        upsert_rows_by_model.setdefault(model, []).extend(rows)
        return len(rows)

    monkeypatch.setattr(importer, "_fetch_alohr_graphql_page", fake_fetch_page)
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)

    options = importer.AlohrGraphQLImportOptions(
        per_resource_limit=0,
        page_limit=0,
        timeout=3,
        run_id="run_1",
        stale_cleanup=False,
        seen_table=None,
        checkpoint_context=None,
        resume_required_entries=None,
    )
    source_ids, diagnostics, counts, linked_counts, stats, stale, stale_ready = await importer._import_alohr_graphql_source_group(
        [{"source_id": "source_alohr", "api_base": importer.ALOHR_PUBLIC_PROVIDER_DIRECTORY_BASE}],
        ["Practitioner", "Location", "PractitionerRole", "Organization", "OrganizationAffiliation"],
        options,
    )

    assert source_ids == ["source_alohr"]
    assert linked_counts == {}
    assert stale == {}
    assert stale_ready == {}
    assert calls == [("providers", None), ("providerOrgs", None)]
    assert counts["Practitioner"] == 1
    assert counts["Location"] == 2
    assert counts["PractitionerRole"] == 1
    assert counts["Organization"] == 1
    assert counts["OrganizationAffiliation"] == 0
    assert ProviderDirectoryOrganizationAffiliation not in upsert_rows_by_model
    assert "OrganizationAffiliation" not in diagnostics
    assert upsert_rows_by_model[ProviderDirectoryOrganization][0]["npi"] == 1992793046
    assert diagnostics["Practitioner"]["complete"] is True
    assert diagnostics["Organization"]["rows_written"] == 1
    assert stats["Location"]["sources_completed"] == 1


@pytest.mark.asyncio
async def test_alohr_alias_group_uses_compatibility_owner(monkeypatch):
    capture = _AliasImportCapture()
    capture.install(monkeypatch)

    async def fetch_page(_query, root_key, _item_key, *, next_token, timeout):
        assert root_key == "providers" and next_token is None and timeout == 3
        return (
            [{"providerId": "prac-1", "firstName": "Ada", "lastName": "Lovelace"}],
            None,
            None,
        )

    monkeypatch.setattr(importer, "_fetch_alohr_graphql_page", fetch_page)
    options = importer.AlohrGraphQLImportOptions(
        0,
        0,
        3,
        "run_retry",
        True,
        None,
        None,
        None,
        "dataset_alohr",
    )
    source_ids, diagnostics, counts, _linked, stats, _stale, _ready = (
        await importer._import_alohr_graphql_source_group(
            _shared_alias_sources(importer.ALOHR_PUBLIC_PROVIDER_DIRECTORY_BASE),
            ["Practitioner"],
            options,
        )
    )

    assert source_ids == [f"source_{alias_index:02d}" for alias_index in range(31)]
    assert counts == {"Practitioner": 1}
    assert diagnostics["Practitioner"]["rows_written"] == 1
    assert stats["Practitioner"]["rows_fetched"] == 1
    assert stats["Practitioner"]["sources_attempted"] == 1
    assert capture.rows_by_model[ProviderDirectoryPractitioner][0]["source_id"] == "source_00"
    assert capture.rows_by_model[ProviderDirectorySourceResource][0]["source_id"] == "source_00"
    assert len(capture.rows_by_model[ProviderDirectoryDatasetResource]) == 1
    assert capture.seen_source_ids == ["source_00"]
    assert capture.stale_source_ids == ["source_00"]


def test_alohr_organization_query_uses_supported_nested_contacts_field():
    assert "contacts { contacts { system value use } }" in importer.ALOHR_ORGANIZATION_QUERY
    assert "organizationContact" not in importer.ALOHR_ORGANIZATION_QUERY


def test_alohr_graphql_checkpoint_locator_round_trips_opaque_token():
    locator = importer._alohr_graphql_checkpoint_locator("providerOrgs", "opaque+/=? token")

    assert (
        importer._alohr_graphql_checkpoint_token(locator, "providerOrgs")
        == "opaque+/=? token"
    )
    with pytest.raises(ValueError, match="invalid ALOHR"):
        importer._alohr_graphql_checkpoint_token(
            "https://untrusted.example/graphql?stream=providerOrgs&nextToken=opaque",
            "providerOrgs",
        )


def _alohr_checkpoint_context(
    owner_run_id: str,
    retry_of_run_id: str | None = None,
):
    return importer.PaginationCheckpointContext(
        canonical_api_base=importer.ALOHR_FHIR_PROVIDER_DIRECTORY_BASE,
        source_scope_hash="scope",
        source_ids=("source_alohr",),
        owner_run_id=owner_run_id,
        acquisition_root_run_id=retry_of_run_id or owner_run_id,
        retry_of_run_id=retry_of_run_id,
    )


def _alohr_sources():
    return [
        {
            "source_id": "source_alohr",
            "api_base": importer.ALOHR_FHIR_PROVIDER_DIRECTORY_BASE,
        }
    ]


def _alohr_checkpoint_options(context, owner_run_id, resume_required_entries):
    return importer.AlohrGraphQLImportOptions(
        0,
        0,
        3,
        owner_run_id,
        False,
        None,
        context,
        resume_required_entries,
    )


def _mock_interrupted_alohr_checkpoint(monkeypatch, events):
    start_locator = importer._alohr_graphql_checkpoint_locator("providerOrgs", None)
    next_locator = importer._alohr_graphql_checkpoint_locator("providerOrgs", "next-page")

    async def fake_load(_context, resource_type, locator):
        assert resource_type == importer.ALOHR_ORGANIZATION_CHECKPOINT_RESOURCE
        assert locator == start_locator
        return importer.PaginationResumeState(locator, 0, 0, ())

    async def fake_fetch(_query, root_key, _item_key, *, next_token, timeout):
        assert root_key == "providerOrgs" and timeout == 3
        if next_token is None:
            return [{"orgId": "org-1", "name": "One"}], "next-page", None
        assert next_token == "next-page"
        return [], None, "timeout"

    async def fake_upsert(model, model_rows, **_kwargs):
        assert model is ProviderDirectoryOrganization
        events.append(("write", [model_row["resource_id"] for model_row in model_rows]))
        return len(model_rows)

    async def fake_save(_context, resource_type, **checkpoint):
        assert resource_type == importer.ALOHR_ORGANIZATION_CHECKPOINT_RESOURCE
        events.append(("checkpoint", checkpoint["next_url"]))

    monkeypatch.setattr(importer, "_load_or_initialize_pagination_checkpoint", fake_load)
    monkeypatch.setattr(importer, "_fetch_alohr_graphql_page", fake_fetch)
    monkeypatch.setattr(importer, "_upsert_resource_rows", fake_upsert)
    monkeypatch.setattr(importer, "_save_pagination_checkpoint", fake_save)
    monkeypatch.setattr(importer, "_clear_pagination_checkpoints", AsyncMock())
    return next_locator


@pytest.mark.asyncio
async def test_alohr_graphql_checkpoint_advances_only_after_page_write(monkeypatch):
    events: list[tuple[str, Any]] = []
    resume_required_entries: set[str] = set()
    checkpoint_context = _alohr_checkpoint_context("run_1")
    next_locator = _mock_interrupted_alohr_checkpoint(monkeypatch, events)
    options = _alohr_checkpoint_options(
        checkpoint_context,
        "run_1",
        resume_required_entries,
    )
    _source_ids, diagnostics, counts, *_rest = await importer._import_alohr_graphql_source_group(
        _alohr_sources(),
        ["Organization"],
        options,
    )

    assert events == [("write", ["org-1"]), ("checkpoint", next_locator)]
    assert counts["Organization"] == 1
    assert diagnostics["Organization"]["complete"] is False
    assert diagnostics["Organization"]["error"] == "timeout"
    assert resume_required_entries == {
        f"source_alohr:{importer.ALOHR_ORGANIZATION_CHECKPOINT_RESOURCE}"
    }


def _mock_resumed_alohr_checkpoint(
    monkeypatch,
    saved_checkpoints,
    cleared_resource_types,
):
    start_locator = importer._alohr_graphql_checkpoint_locator("providerOrgs", None)
    resume_locator = importer._alohr_graphql_checkpoint_locator("providerOrgs", "resume-token")

    async def fake_load(_context, _resource_type, locator):
        assert locator == start_locator
        return importer.PaginationResumeState(
            resume_locator,
            7,
            700,
            (importer._pagination_url_hash(start_locator),),
            resumed=True,
        )

    async def fake_fetch(_query, _root_key, _item_key, *, next_token, timeout):
        assert next_token == "resume-token" and timeout == 3
        return [{"orgId": "org-701", "name": "Last"}], None, None

    async def fake_upsert(model, model_rows, **_kwargs):
        assert model is ProviderDirectoryOrganization
        return len(model_rows)

    async def fake_save(_context, _resource_type, **checkpoint):
        saved_checkpoints.append(checkpoint)

    async def fake_clear(_context, resource_types):
        cleared_resource_types.extend(resource_types)
        return len(resource_types)

    monkeypatch.setattr(importer, "_load_or_initialize_pagination_checkpoint", fake_load)
    monkeypatch.setattr(importer, "_fetch_alohr_graphql_page", fake_fetch)
    monkeypatch.setattr(importer, "_upsert_resource_rows", fake_upsert)
    monkeypatch.setattr(importer, "_save_pagination_checkpoint", fake_save)
    monkeypatch.setattr(importer, "_clear_pagination_checkpoints", fake_clear)


@pytest.mark.asyncio
async def test_alohr_graphql_checkpoint_resumes_exact_token_and_clears(monkeypatch):
    saved_checkpoints: list[dict[str, Any]] = []
    cleared_resource_types: list[str] = []
    checkpoint_context = _alohr_checkpoint_context("run_retry", "run_1")
    _mock_resumed_alohr_checkpoint(
        monkeypatch,
        saved_checkpoints,
        cleared_resource_types,
    )
    options = _alohr_checkpoint_options(checkpoint_context, "run_retry", set())
    _source_ids, diagnostics, counts, *_rest = await importer._import_alohr_graphql_source_group(
        _alohr_sources(),
        ["Organization"],
        options,
    )

    assert counts["Organization"] == 1
    assert diagnostics["Organization"]["complete"] is True
    assert diagnostics["Organization"]["rows_fetched"] == 701
    assert saved_checkpoints[0]["next_url"] is None
    assert saved_checkpoints[0]["pages_processed"] == 8
    assert saved_checkpoints[0]["rows_processed"] == 701
    assert cleared_resource_types == [importer.ALOHR_ORGANIZATION_CHECKPOINT_RESOURCE]


@pytest.mark.asyncio
async def test_update_source_resource_import_metadata_casts_json_to_jsonb(monkeypatch):
    calls: list[tuple[str, dict[str, Any]]] = []

    async def fake_status(sql, **params):
        calls.append((sql, params))
        return 1

    monkeypatch.setattr(importer.db, "status", fake_status)

    await importer._update_source_resource_import_metadata(
        ["source_a"],
        run_id="run_1",
        diagnostics={"Practitioner": {"error": "http_401", "rows_fetched": 0}},
    )

    assert len(calls) == 1
    assert "COALESCE(metadata_json::jsonb, '{}'::jsonb)" in calls[0][0]
    assert calls[0][1]["source_id"] == "source_a"
    payload = json.loads(calls[0][1]["payload"])
    assert payload["resources"]["Practitioner"]["error"] == "http_401"


def test_decode_json_body_accepts_utf8_bom():
    payload = importer._decode_json_body(
        b'\xef\xbb\xbf{"resourceType":"CapabilityStatement","fhirVersion":"4.0.1"}'
    )

    assert payload == {"resourceType": "CapabilityStatement", "fhirVersion": "4.0.1"}


def test_read_response_body_with_deadline_reads_chunks(monkeypatch):
    class FakeResponse:
        def __init__(self):
            self.chunks = [b'{"resourceType":"', b'Bundle"}', b""]

        def read(self, _size):
            return self.chunks.pop(0)

    monkeypatch.setattr(importer.time, "monotonic", lambda: 100.0)

    assert (
        importer._read_response_body_with_deadline(
            FakeResponse(),
            timeout=10,
        )
        == b'{"resourceType":"Bundle"}'
    )


def test_read_response_body_with_deadline_times_out(monkeypatch):
    class FakeResponse:
        def read(self, _size):
            return b"x"

    timestamps = iter([100.0, 100.5, 111.0])
    monkeypatch.setattr(importer.time, "monotonic", lambda: next(timestamps))

    with pytest.raises(TimeoutError):
        importer._read_response_body_with_deadline(
            FakeResponse(),
            timeout=10,
        )


def test_candidate_metadata_urls_repairs_resource_and_provider_directory_suffixes():
    humana = importer._candidate_metadata_urls(
        {"api_base": "https://fhir.humana.com/api/provider-directory/"}
    )
    maine = importer._candidate_metadata_urls(
        {"api_base": "https://maineproviderdirectory.verityanalytics.org/fhir/Practitioner"}
    )

    assert (
        "https://fhir.humana.com/api",
        "https://fhir.humana.com/api/metadata?_format=json",
    ) in humana
    assert (
        "https://maineproviderdirectory.verityanalytics.org/fhir",
        "https://maineproviderdirectory.verityanalytics.org/fhir/metadata?_format=json",
    ) in maine


def test_candidate_metadata_urls_ignores_non_absolute_catalog_values():
    assert importer._candidate_metadata_urls({"api_base": "UNCONFIRMED"}) == []
    assert importer._candidate_metadata_urls({"api_base": "N/A"}) == []


def test_provider_directory_credentials_resolve_env_headers_and_query(monkeypatch):
    monkeypatch.setenv("PAYER_DIRECTORY_KEY", "secret-key")
    monkeypatch.setenv("PAYER_DIRECTORY_CLIENT", "client-1")
    monkeypatch.setenv(
        importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV,
        json.dumps(
            {
                "hosts": {
                    "payer.example": {
                        "headers": {"X-API-Key": "env:PAYER_DIRECTORY_KEY"},
                        "query_params": {"client_id": "env:PAYER_DIRECTORY_CLIENT"},
                    }
                }
            }
        ),
    )

    options = importer._credential_request_options_for_source(
        {"source_id": "source_a", "api_base": "https://payer.example/fhir"},
        "https://payer.example/fhir/metadata",
    )

    assert options["headers"] == {"X-API-Key": "secret-key"}
    assert options["query_params"] == {"client_id": "client-1"}
    assert options["descriptor"] == {
        "matched_by": ["hosts:payer.example"],
        "header_names": ["X-API-Key"],
        "query_param_names": ["client_id"],
    }


def test_provider_directory_credentials_resolve_generated_api_key_template(monkeypatch):
    monkeypatch.setenv("PAYER_DIRECTORY_API_KEY_HEADER", "Ocp-Apim-Subscription-Key")
    monkeypatch.setenv("PAYER_DIRECTORY_API_KEY", "secret-key")
    monkeypatch.setenv(
        importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV,
        json.dumps(
            {
                "hosts": {
                    "payer.example": {
                        "api_key": {
                            "header": "env:PAYER_DIRECTORY_API_KEY_HEADER",
                            "value": "env:PAYER_DIRECTORY_API_KEY",
                        }
                    }
                }
            }
        ),
    )

    options = importer._credential_request_options_for_source(
        {"source_id": "source_a", "api_base": "https://payer.example/fhir"},
        "https://payer.example/fhir/metadata",
    )

    assert options["headers"] == {"Ocp-Apim-Subscription-Key": "secret-key"}
    assert options["descriptor"] == {
        "matched_by": ["hosts:payer.example"],
        "header_names": ["Ocp-Apim-Subscription-Key"],
        "query_param_names": [],
    }
    assert "secret-key" not in json.dumps(options["descriptor"])


def test_provider_directory_credentials_resolve_task_file_override(monkeypatch, tmp_path):
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV, raising=False)
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_FILE_ENV, raising=False)
    monkeypatch.setenv("PAYER_DIRECTORY_KEY", "secret-key")
    config_path = tmp_path / "provider-directory-credentials.json"
    config_path.write_text(
        json.dumps(
            {
                "hosts": {
                    "payer.example": {
                        "headers": {"X-API-Key": "env:PAYER_DIRECTORY_KEY"},
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    token = importer._PROVIDER_DIRECTORY_CREDENTIALS_FILE_OVERRIDE.set(str(config_path))
    try:
        options = importer._credential_request_options_for_source(
            {"source_id": "source_a", "api_base": "https://payer.example/fhir"},
            "https://payer.example/fhir/metadata",
        )
    finally:
        importer._PROVIDER_DIRECTORY_CREDENTIALS_FILE_OVERRIDE.reset(token)

    assert options["headers"] == {"X-API-Key": "secret-key"}
    assert options["query_params"] == {}
    assert options["descriptor"] == {
        "matched_by": ["hosts:payer.example"],
        "header_names": ["X-API-Key"],
        "query_param_names": [],
    }
    assert "secret-key" not in json.dumps(options["descriptor"])


def test_provider_directory_credentials_resolve_oauth2_client_credentials(monkeypatch):
    monkeypatch.setenv("PAYER_DIRECTORY_CLIENT_ID", "client-id")
    monkeypatch.setenv("PAYER_DIRECTORY_CLIENT_SECRET", "client-secret")
    monkeypatch.setenv(
        importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV,
        json.dumps(
            {
                "api_bases": {
                    "https://payer.example/fhir": {
                        "oauth2": {
                            "token_url": "https://auth.example/token",
                            "client_id": "env:PAYER_DIRECTORY_CLIENT_ID",
                            "client_secret": "env:PAYER_DIRECTORY_CLIENT_SECRET",
                            "scope": "system/*.read",
                        }
                    }
                }
            }
        ),
    )
    monkeypatch.setattr(
        importer,
        "_fetch_oauth2_client_credentials_token_sync",
        lambda _oauth2: "oauth-token",
    )

    options = importer._credential_request_options_for_source(
        {"source_id": "source_a", "api_base": "https://payer.example/fhir"},
        "https://payer.example/fhir/Practitioner?_count=1",
    )

    assert options["headers"] == {"Authorization": "Bearer oauth-token"}
    assert options["descriptor"] == {
        "matched_by": ["api_bases:https://payer.example/fhir"],
        "header_names": ["Authorization"],
        "query_param_names": [],
    }
    assert "client-secret" not in json.dumps(options["descriptor"])


def test_oauth2_client_credentials_token_request_uses_basic_auth_and_cache(monkeypatch):
    importer._OAUTH_TOKEN_CACHE.clear()
    calls = []

    class FakeResponse:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self, *_args):
            return b'{"access_token":"token-1","expires_in":3600}'

    def fake_urlopen(request, **_kwargs):
        calls.append(
            {
                "url": request.full_url,
                "authorization": request.get_header("Authorization"),
                "body": request.data.decode("utf-8"),
            }
        )
        return FakeResponse()

    monkeypatch.setenv("PAYER_DIRECTORY_CLIENT_ID", "client-id")
    monkeypatch.setenv("PAYER_DIRECTORY_CLIENT_SECRET", "client-secret")
    monkeypatch.setenv("PAYER_DIRECTORY_TOKEN_URL", "https://auth.example/token")
    monkeypatch.setattr(importer.urllib.request, "urlopen", fake_urlopen)

    oauth2_config_map = {
        "token_url": "env:PAYER_DIRECTORY_TOKEN_URL",
        "client_id": "env:PAYER_DIRECTORY_CLIENT_ID",
        "client_secret": "env:PAYER_DIRECTORY_CLIENT_SECRET",
        "scope": "system/*.read",
    }
    first = importer._fetch_oauth2_client_credentials_token_sync(oauth2_config_map)
    second = importer._fetch_oauth2_client_credentials_token_sync(oauth2_config_map)

    assert first == "token-1"
    assert second == "token-1"
    assert len(calls) == 1
    assert calls[0]["url"] == "https://auth.example/token"
    assert calls[0]["authorization"].startswith("Basic ")
    assert "grant_type=client_credentials" in calls[0]["body"]
    assert "scope=system%2F%2A.read" in calls[0]["body"]


def test_provider_directory_credentials_do_not_apply_to_cross_host_reference(monkeypatch):
    monkeypatch.setenv("PAYER_DIRECTORY_KEY", "secret-key")
    monkeypatch.setenv(
        importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV,
        json.dumps({"hosts": {"payer.example": {"headers": {"X-API-Key": "env:PAYER_DIRECTORY_KEY"}}}}),
    )

    options = importer._credential_request_options_for_source(
        {"source_id": "source_a", "api_base": "https://payer.example/fhir"},
        "https://other.example/fhir/Location/1",
    )

    assert options == {"headers": {}, "query_params": {}, "descriptor": None}


@pytest.mark.asyncio
async def test_source_fetch_retries_transient_get_failures(monkeypatch):
    responses = [
        (None, None, "TimeoutError: timed out", 7),
        (503, {"resourceType": "OperationOutcome"}, None, 11),
        (200, {"resourceType": "Bundle", "entry": []}, None, 5),
    ]

    async def fake_fetch_json(_url, *, timeout):
        return responses.pop(0)

    monkeypatch.setattr(importer, "_fetch_json", fake_fetch_json)
    monkeypatch.setattr(importer, "_source_fetch_retry_delay_seconds", lambda _attempt: 0)

    status_code, payload, fetch_error, elapsed_ms = await importer._fetch_source_json(
        {"source_id": "source_a", "api_base": "https://payer.example/fhir"},
        "https://payer.example/fhir/Practitioner?_count=1",
        timeout=3,
    )

    assert status_code == 200
    assert payload == {"resourceType": "Bundle", "entry": []}
    assert fetch_error is None
    assert elapsed_ms == 23
    assert responses == []


@pytest.mark.asyncio
async def test_cigna_source_fetch_cools_down_after_empty_search_sets(monkeypatch):
    responses = [
        (200, {"resourceType": "Bundle", "type": "searchset", "entry": []}, None, 5),
        (200, {"resourceType": "Bundle", "type": "searchset", "entry": []}, None, 7),
        (
            200,
            {
                "resourceType": "Bundle",
                "type": "searchset",
                "entry": [{"resource": {"resourceType": "Location", "id": "loc-1"}}],
            },
            None,
            11,
        ),
    ]
    fetch_source_json_once = AsyncMock(side_effect=responses)
    sleep_mock = AsyncMock()
    monkeypatch.setattr(importer, "_fetch_source_json_once", fetch_source_json_once)
    monkeypatch.setattr(importer.asyncio, "sleep", sleep_mock)
    monkeypatch.setattr(
        importer,
        "CIGNA_EMPTY_COLLECTION_RETRY_DELAYS_SECONDS",
        (2.0, 4.0),
    )

    fetch_result = await importer._fetch_source_json(
        {
            "source_id": "cigna",
            "api_base": importer.CIGNA_PROVIDER_DIRECTORY_BASE,
        },
        f"{importer.CIGNA_PROVIDER_DIRECTORY_BASE}/Location?_count=100",
        timeout=3,
    )

    assert fetch_result[0] == 200
    assert fetch_result[1]["entry"][0]["resource"]["id"] == "loc-1"
    assert fetch_result[3] == 23
    assert fetch_source_json_once.await_count == 3
    assert [sleep_call.args[0] for sleep_call in sleep_mock.await_args_list] == [2.0, 4.0]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "resource_url",
    [
        f"{importer.CIGNA_PROVIDER_DIRECTORY_BASE}/Endpoint?_count=100",
        f"{importer.CIGNA_PROVIDER_DIRECTORY_BASE}/Location?_count=100&ct=opaque",
    ],
)
async def test_cigna_empty_search_retry_excludes_empty_or_continuation_queries(
    monkeypatch,
    resource_url,
):
    fetch_source_json_once = AsyncMock(
        return_value=(
            200,
            {"resourceType": "Bundle", "type": "searchset", "entry": []},
            None,
            5,
        )
    )
    sleep_mock = AsyncMock()
    monkeypatch.setattr(importer, "_fetch_source_json_once", fetch_source_json_once)
    monkeypatch.setattr(importer.asyncio, "sleep", sleep_mock)

    result = await importer._fetch_source_json(
        {
            "source_id": "cigna",
            "api_base": importer.CIGNA_PROVIDER_DIRECTORY_BASE,
        },
        resource_url,
        timeout=3,
    )

    assert result[0] == 200
    assert result[1]["entry"] == []
    fetch_source_json_once.assert_awaited_once()
    sleep_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_aetna_http_200_operation_outcome_is_resource_error(monkeypatch):
    fetch_json = AsyncMock(
        return_value=(
            200,
            {
                "resourceType": "OperationOutcome",
                "issue": [{"diagnostics": "Maximum page size exceeded"}],
            },
            None,
            5,
        )
    )
    monkeypatch.setattr(importer, "_fetch_json", fetch_json)
    source_lookup = {
        "source_id": "aetna-commercial",
        "api_base": importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE,
    }

    fetch_result = await importer._fetch_resource_rows(
        source_lookup,
        "Practitioner",
        per_resource_limit=0,
        page_limit=0,
        page_count=30,
        timeout=3,
        run_id="run_1",
    )

    assert fetch_result is not None
    assert fetch_result.complete is False
    assert fetch_result.error == importer.AETNA_RESOURCE_SEARCH_OPERATION_OUTCOME_ERROR
    assert fetch_json.await_count == 1


@pytest.mark.asyncio
async def test_resource_probe_does_not_label_http_400_reachable(monkeypatch):
    capability_map = {
        "resourceType": "CapabilityStatement",
        "rest": [{"resource": [{"type": "Practitioner"}]}],
    }

    async def fake_fetch_source_json(_source, request_url, *, timeout):
        if request_url.endswith("/metadata"):
            return 200, capability_map, None, 4
        return 400, {"resourceType": "OperationOutcome"}, None, 5

    monkeypatch.setattr(
        importer,
        "_fetch_source_json",
        fake_fetch_source_json,
    )

    probe_map, returned_capability_map = await importer._probe_metadata_candidate(
        {
            "source_id": "source_a",
            "api_base": "https://payer.example/fhir",
            "auth_type": "open",
        },
        "https://payer.example/fhir",
        "https://payer.example/fhir/metadata",
        timeout=3,
        run_id="run_1",
    )

    assert returned_capability_map == capability_map
    assert probe_map["status"] == "resource_error"
    assert probe_map["http_status"] == 400
    assert probe_map["error"] == "resource_search_http_400"
    assert probe_map["resource_probe"]["status"] == "resource_error"


@pytest.mark.asyncio
async def test_aetna_medicaid_resource_probe_reports_target_required_without_request(
    monkeypatch,
):
    source_lookup = importer._source_row_from_seed(
        {
            "id": "aetna-medicaid",
            "org_name": "Aetna",
            "api_base": importer.AETNA_PROVIDER_DIRECTORY_BASE,
            "source": "fixture",
        }
    )
    fetch_source_json = AsyncMock(
        side_effect=AssertionError("targeted-only resource probe must not run")
    )
    monkeypatch.setattr(importer, "_fetch_source_json", fetch_source_json)

    probe = await importer._probe_resource_access(
        source_lookup,
        importer.AETNA_PROVIDER_DIRECTORY_BASE,
        {
            "resourceType": "CapabilityStatement",
            "rest": [{"resource": [{"type": "Practitioner"}]}],
        },
        timeout=3,
    )

    assert probe is not None
    assert probe["status"] == "targeted_required"
    assert probe["http_status"] is None
    assert probe["error"] == importer.AETNA_MEDICAID_TARGETED_QUERY_REQUIRED_ERROR
    fetch_source_json.assert_not_awaited()


@pytest.mark.asyncio
async def test_hap_source_request_pacing_serializes_anonymous_calls(monkeypatch):
    source_lookup = {
        "source_id": "hap",
        "api_base": importer.HAP_PROVIDER_DIRECTORY_BASE,
        "_provider_directory_last_request_monotonic": importer.time.monotonic() - 10.0,
    }
    sleep_mock = AsyncMock()
    monkeypatch.setattr(importer.asyncio, "sleep", sleep_mock)
    monkeypatch.setattr(
        importer,
        "_fetch_json",
        AsyncMock(return_value=(200, {"resourceType": "Bundle"}, None, 1)),
    )

    await importer._fetch_source_json_once(
        source_lookup,
        f"{importer.HAP_PROVIDER_DIRECTORY_BASE}/Practitioner?_count=1",
        timeout=3,
    )

    sleep_mock.assert_awaited_once()
    assert 9.0 <= sleep_mock.await_args.args[0] <= 10.0


@pytest.mark.asyncio
async def test_source_fetch_honors_retry_after_for_429(monkeypatch):
    responses = [
        (429, {importer.SOURCE_RETRY_AFTER_FIELD: "3"}, None, 1),
        (200, {"resourceType": "Bundle", "entry": []}, None, 1),
    ]
    sleep_mock = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_fetch_source_json_once",
        AsyncMock(side_effect=lambda *_args, **_kwargs: responses.pop(0)),
    )
    monkeypatch.setattr(importer.asyncio, "sleep", sleep_mock)

    fetch_result = await importer._fetch_source_json(
        {"source_id": "source_a", "api_base": "https://payer.example/fhir"},
        "https://payer.example/fhir/Practitioner?_count=1",
        timeout=3,
    )

    assert fetch_result[0] == 200
    sleep_mock.assert_awaited_once_with(3.0)


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [423, 503])
async def test_source_fetch_honors_delta_seconds_retry_after_for_transient_status(
    monkeypatch,
    status_code,
):
    responses = [
        (status_code, {importer.SOURCE_RETRY_AFTER_FIELD: "4"}, None, 1),
        (200, {"resourceType": "Bundle", "entry": []}, None, 1),
    ]
    sleep_mock = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_fetch_source_json_once",
        AsyncMock(side_effect=lambda *_args, **_kwargs: responses.pop(0)),
    )
    monkeypatch.setattr(importer.asyncio, "sleep", sleep_mock)

    fetch_result = await importer._fetch_source_json(
        {"source_id": "source_a", "api_base": "https://payer.example/fhir"},
        "https://payer.example/fhir/Practitioner?_count=1",
        timeout=3,
    )

    assert fetch_result[0] == 200
    sleep_mock.assert_awaited_once_with(4.0)


def test_source_fetch_honors_http_date_retry_after_for_503():
    current_time = datetime.datetime(2026, 7, 12, 12, 0, tzinfo=datetime.UTC)
    retry_at = current_time + datetime.timedelta(seconds=90)
    payload = {
        importer.SOURCE_RETRY_AFTER_FIELD: email.utils.format_datetime(
            retry_at,
            usegmt=True,
        )
    }

    assert importer._source_fetch_attempt_delay(
        503,
        payload,
        0,
        (),
        False,
        now_utc=current_time,
    ) == 90.0


@pytest.mark.asyncio
async def test_terminal_503_redacts_fetch_diagnostic(monkeypatch):
    """Persist only retry facts that cannot expose a cursor or response body."""
    source_lookup = {
        "source_id": "source_a",
        "api_base": "https://payer.example/fhir",
    }
    request_url = (
        "https://payer.example/fhir/Practitioner?"
        "_continuationToken=private-cursor-token"
    )
    monkeypatch.setattr(
        importer,
        "_fetch_source_json_once",
        AsyncMock(return_value=(503, {"diagnostics": "private-response"}, None, 7)),
    )
    monkeypatch.setattr(importer, "_source_fetch_retry_attempts", lambda: 3)
    monkeypatch.setattr(importer.asyncio, "sleep", AsyncMock())

    fetch_result = await importer._fetch_source_json(
        source_lookup,
        request_url,
        timeout=3,
    )
    fetch_diagnostic = importer._terminal_source_fetch_diagnostic(
        source_lookup,
        request_url,
    )
    assert fetch_result == (503, {"diagnostics": "private-response"}, None, 21)
    assert fetch_diagnostic == {
        "status": 503,
        "url_hash": hashlib.sha256(request_url.encode("utf-8")).hexdigest(),
        "retry_count": 2,
        "elapsed_ms": 21,
        "response_class": "transient_server_error",
    }
    resource_diagnostic = importer._resource_fetch_diagnostic(
        importer.ResourceFetchResult(
            model=ProviderDirectoryPractitioner,
            rows=[],
            rows_fetched=0,
            rows_written=0,
            pages_fetched=0,
            complete=False,
            row_limit_reached=False,
            page_limit_reached=False,
            hard_page_limit_reached=False,
            next_url_remaining=True,
            error="http_503",
            fetch_diagnostic=fetch_diagnostic,
        ),
        rows_written=0,
    )

    assert resource_diagnostic["source_fetch"] == fetch_diagnostic
    assert "private-cursor-token" not in json.dumps(resource_diagnostic)
    assert "private-response" not in json.dumps(resource_diagnostic)


@pytest.mark.asyncio
async def test_terminal_423_redacts_fetch_diagnostic(monkeypatch):
    source_lookup = {"source_id": "source_a", "api_base": "https://payer.example/fhir"}
    request_url = "https://payer.example/fhir/Practitioner?_continuationToken=private"
    monkeypatch.setattr(
        importer,
        "_fetch_source_json_once",
        AsyncMock(return_value=(423, {"diagnostics": "private-response"}, None, 7)),
    )
    monkeypatch.setattr(importer, "_source_fetch_retry_attempts", lambda: 2)
    monkeypatch.setattr(importer.asyncio, "sleep", AsyncMock())

    fetch_result = await importer._fetch_source_json(source_lookup, request_url, timeout=3)
    fetch_diagnostic = importer._terminal_source_fetch_diagnostic(source_lookup, request_url)

    assert fetch_result == (423, {"diagnostics": "private-response"}, None, 14)
    assert fetch_diagnostic == {
        "status": 423,
        "url_hash": hashlib.sha256(request_url.encode("utf-8")).hexdigest(),
        "retry_count": 1,
        "elapsed_ms": 14,
        "response_class": "transient_locked",
    }
    assert "private" not in json.dumps(fetch_diagnostic)


@pytest.mark.parametrize("status_code", [423, 503])
def test_terminal_transient_response_uses_bounded_fallback(status_code):
    current_time = datetime.datetime(2026, 7, 12, 12, 0, tzinfo=datetime.UTC)

    assert importer._transient_source_retry_not_before(
        status_code,
        {"diagnostics": "private-response"},
        None,
        retry_count=2,
        now_utc=current_time,
    ) == "2026-07-12T12:05:00Z"


@pytest.mark.asyncio
async def test_source_fetch_keeps_generic_403_out_of_transient_defer_handling(
    monkeypatch,
):
    source_lookup = {
        "source_id": "source_a",
        "api_base": "https://payer.example/fhir",
    }
    request_url = "https://payer.example/fhir/Practitioner?_continuationToken=private"
    fetch_once = AsyncMock(return_value=(403, {"resourceType": "OperationOutcome"}, None, 1))
    monkeypatch.setattr(importer, "_fetch_source_json_once", fetch_once)

    fetch_result = await importer._fetch_source_json(
        source_lookup,
        request_url,
        timeout=3,
    )

    assert fetch_result[0] == 403
    fetch_once.assert_awaited_once()
    assert importer._terminal_source_fetch_diagnostic(source_lookup, request_url) is None
    assert importer._transient_source_retry_not_before(
        403,
        fetch_result[1],
        fetch_result[2],
        retry_count=0,
    ) is None


def test_molina_quota_response_is_deferred_without_reclassifying_generic_403():
    quota_payload_dict = {
        "statusCode": 403,
        "message": "Out of call volume quota. Quota will be replenished in 04:56:59.",
    }
    source_lookup = {
        "source_id": "molina",
        "api_base": importer.MOLINA_PROVIDER_DIRECTORY_BASE,
    }
    fetch_result = importer._source_fetch_result_with_payload_error(
        source_lookup,
        f"{importer.MOLINA_PROVIDER_DIRECTORY_BASE}/Location?_count=100",
        (403, quota_payload_dict, None, 7),
    )

    assert importer._classify_http(
        403,
        None,
        quota_payload_dict,
        provider_directory_source=source_lookup,
    ) == "quota_exhausted"
    assert importer._classify_http(
        403,
        None,
        quota_payload_dict,
        provider_directory_source={"api_base": "https://payer.example/fhir"},
    ) == "auth_required"
    assert importer._classify_http(403, None, {"message": "Forbidden"}) == (
        "auth_required"
    )
    assert fetch_result == (
        403,
        quota_payload_dict,
        importer.SOURCE_QUOTA_EXHAUSTED_ERROR,
        7,
    )
    assert importer._is_transient_source_fetch_failure(
        fetch_result[0],
        fetch_result[2],
    ) is False


def test_molina_200_quota_is_deferred():
    current_time = datetime.datetime(2026, 7, 11, 23, 51, tzinfo=datetime.UTC)
    quota_payload_dict = {
        "resourceType": "OperationOutcome",
        "issue": [
            {
                "diagnostics": (
                    "Out of call volume quota. Quota will be replenished in "
                    "00:09:00."
                )
            }
        ],
    }
    molina_source_lookup = {
        "source_id": "molina",
        "api_base": importer.MOLINA_PROVIDER_DIRECTORY_BASE,
    }
    request_url = (
        f"{importer.MOLINA_PROVIDER_DIRECTORY_BASE}/Practitioner?"
        "_count=100&cursorMark=opaque"
    )

    fetch_result = importer._source_fetch_result_with_payload_error(
        molina_source_lookup,
        request_url,
        (200, quota_payload_dict, None, 7),
    )

    assert importer._classify_http(
        200,
        None,
        quota_payload_dict,
        provider_directory_source=molina_source_lookup,
    ) == "quota_exhausted"
    assert fetch_result == (
        200,
        quota_payload_dict,
        importer.SOURCE_QUOTA_EXHAUSTED_ERROR,
        7,
    )
    assert importer._molina_quota_retry_not_before(
        molina_source_lookup,
        fetch_result[0],
        fetch_result[1],
        now_utc=current_time,
    ) == "2026-07-12T00:01:00Z"


def test_generic_200_quota_text_is_unchanged():
    quota_payload_dict = {
        "resourceType": "OperationOutcome",
        "issue": [{"diagnostics": "Out of call volume quota."}],
    }

    generic_fetch_result = importer._source_fetch_result_with_payload_error(
        {"source_id": "other", "api_base": "https://payer.example/fhir"},
        "https://payer.example/fhir/Practitioner?page=2",
        (200, quota_payload_dict, None, 7),
    )
    assert generic_fetch_result == (200, quota_payload_dict, None, 7)


def test_non_bundle_backoff_is_bounded():
    current_time = datetime.datetime(2026, 7, 12, 12, 0, tzinfo=datetime.UTC)
    checkpoint_context = importer.PaginationCheckpointContext(
        canonical_api_base=importer.CIGNA_PROVIDER_DIRECTORY_BASE,
        source_scope_hash="scope_a",
        source_ids=("source_a",),
        owner_run_id="run_retry",
        retry_of_run_id="run_original",
        acquisition_root_run_id="run_original",
        dataset_id="dataset_a",
        lineage_verified=True,
    )
    response_payload_dict = {
        "resourceType": "OperationOutcome",
        importer.SOURCE_RETRY_AFTER_FIELD: "3600",
    }

    assert importer._checkpoint_non_bundle_retry_not_before(
        checkpoint_context,
        True,
        200,
        response_payload_dict,
        now_utc=current_time,
    ) == "2026-07-12T12:10:00Z"
    assert importer._checkpoint_non_bundle_retry_not_before(
        checkpoint_context,
        False,
        200,
        response_payload_dict,
        now_utc=current_time,
    ) is None


def test_molina_quota_selects_later_uncapped_reset():
    current_time = datetime.datetime(2026, 7, 11, 19, 0, tzinfo=datetime.UTC)
    quota_payload_dict = {
        "message": "Out of call volume quota. Quota will be replenished in 04:56:59.",
        importer.SOURCE_RETRY_AFTER_FIELD: "18000",
    }

    retry_not_before = importer._molina_quota_retry_not_before(
        {
            "source_id": "molina",
            "canonical_api_base": importer.MOLINA_PROVIDER_DIRECTORY_BASE,
        },
        403,
        quota_payload_dict,
        now_utc=current_time,
    )

    assert retry_not_before == "2026-07-12T00:01:00Z"
    assert importer._source_retry_after_seconds(quota_payload_dict) == 600.0
    assert importer._source_retry_after_seconds(
        quota_payload_dict,
        max_delay_seconds=None,
        now_utc=current_time,
    ) == 18000.0


def test_molina_quota_retry_rejects_non_finite_delay_and_rounds_up():
    current_time = datetime.datetime(2026, 7, 11, 19, 0, tzinfo=datetime.UTC)
    source_lookup = {
        "source_id": "molina",
        "api_base": importer.MOLINA_PROVIDER_DIRECTORY_BASE,
    }

    assert importer._source_retry_after_seconds(
        {importer.SOURCE_RETRY_AFTER_FIELD: "inf"},
        max_delay_seconds=None,
    ) is None
    assert importer._source_retry_after_seconds(
        {importer.SOURCE_RETRY_AFTER_FIELD: "nan"},
        max_delay_seconds=None,
    ) is None
    assert importer._molina_quota_retry_not_before(
        source_lookup,
        403,
        {
            "message": "Out of call volume quota.",
            importer.SOURCE_RETRY_AFTER_FIELD: "10.1",
        },
        now_utc=current_time,
    ) == "2026-07-11T19:01:11Z"


def test_resource_diagnostic_preserves_quota_retry_timestamp():
    retry_not_before = "2026-07-12T00:01:00Z"
    fetch_result = importer.ResourceFetchResult(
        model=ProviderDirectoryLocation,
        rows=[],
        rows_fetched=497700,
        rows_written=0,
        pages_fetched=4977,
        complete=False,
        row_limit_reached=False,
        page_limit_reached=False,
        hard_page_limit_reached=False,
        next_url_remaining=True,
        error=importer.SOURCE_QUOTA_EXHAUSTED_ERROR,
        retry_not_before=retry_not_before,
    )

    diagnostic_dict = importer._resource_fetch_diagnostic(
        fetch_result,
        rows_written=0,
    )

    assert diagnostic_dict["retry_not_before"] == retry_not_before


@pytest.mark.asyncio
async def test_source_fetch_does_not_retry_deterministic_http_error(monkeypatch):
    calls: list[str] = []

    async def fake_fetch_json(url, *, timeout):
        calls.append(url)
        return 404, None, None, 4

    monkeypatch.setattr(importer, "_fetch_json", fake_fetch_json)

    result = await importer._fetch_source_json(
        {"source_id": "source_a", "api_base": "https://payer.example/fhir"},
        "https://payer.example/fhir/Endpoint?_count=1",
        timeout=3,
    )

    assert result == (404, None, None, 4)
    assert calls == ["https://payer.example/fhir/Endpoint?_count=1"]


@pytest.mark.asyncio
async def test_source_fetch_reduces_oversized_initial_search_page(monkeypatch):
    calls: list[str] = []

    async def fake_fetch_json(url, *, timeout):
        calls.append(url)
        if url.endswith("_count=1000"):
            return 413, {"resourceType": "OperationOutcome"}, None, 4
        return 200, {"resourceType": "Bundle", "entry": []}, None, 5

    monkeypatch.setattr(importer, "_fetch_json", fake_fetch_json)

    result = await importer._fetch_source_json(
        {"source_id": "source_a", "api_base": "https://payer.example/fhir"},
        "https://payer.example/fhir/Practitioner?_count=1000",
        timeout=3,
    )

    assert result == (200, {"resourceType": "Bundle", "entry": []}, None, 9)
    assert calls == [
        "https://payer.example/fhir/Practitioner?_count=1000",
        "https://payer.example/fhir/Practitioner?_count=100",
    ]


@pytest.mark.asyncio
async def test_source_fetch_retries_transient_failure_without_resizing(monkeypatch):
    calls: list[str] = []
    responses = [
        (None, None, "TimeoutError: timed out", 7),
        (503, {"resourceType": "OperationOutcome"}, None, 11),
        (200, {"resourceType": "Bundle", "entry": []}, None, 5),
    ]

    async def fake_fetch_json(url, *, timeout):
        calls.append(url)
        return responses.pop(0)

    monkeypatch.setattr(importer, "_fetch_json", fake_fetch_json)
    monkeypatch.setattr(importer, "_source_fetch_retry_delay_seconds", lambda _attempt: 0)

    result = await importer._fetch_source_json(
        {"source_id": "source_a", "api_base": "https://payer.example/fhir"},
        "https://payer.example/fhir/Practitioner?_count=1000",
        timeout=3,
    )

    assert result == (200, {"resourceType": "Bundle", "entry": []}, None, 23)
    assert calls == ["https://payer.example/fhir/Practitioner?_count=1000"] * 3


@pytest.mark.parametrize(
    "continuation_query",
    [
        "_getpages=opaque",
        "_offset=100",
        "_continuationToken=opaque",
        "cursorMark=opaque",
        "ct=opaque",
        "nextToken=opaque",
    ],
)
def test_source_fetch_does_not_resize_continuation_page(continuation_query):
    url = f"https://payer.example/fhir/Practitioner?_count=1000&{continuation_query}"

    assert importer._source_fetch_candidate_urls(url) == [url]


@pytest.mark.parametrize("resource_type", sorted(importer.CIGNA_EXPECTED_NONEMPTY_RESOURCES))
def test_cigna_expected_nonempty_resource_fails_closed(resource_type):
    result = importer.ResourceFetchResult(
        model=object,
        rows=[],
        rows_fetched=0,
        rows_written=0,
        pages_fetched=1,
        complete=True,
        row_limit_reached=False,
        page_limit_reached=False,
        hard_page_limit_reached=False,
        next_url_remaining=False,
    )

    guarded_result = importer._fail_closed_on_unexpected_empty_resource(
        {"canonical_api_base": importer.CIGNA_PROVIDER_DIRECTORY_BASE},
        resource_type,
        result,
    )

    assert guarded_result.complete is False
    assert guarded_result.error == importer.CIGNA_UNEXPECTED_EMPTY_RESOURCE_ERROR


@pytest.mark.parametrize(
    "resource_type",
    sorted(importer.AETNA_COMMERCIAL_EXPECTED_NONEMPTY_RESOURCES),
)
def test_aetna_expected_nonempty_resource_fails_closed(resource_type):
    result = importer.ResourceFetchResult(
        model=object,
        rows=[],
        rows_fetched=0,
        rows_written=0,
        pages_fetched=1,
        complete=True,
        row_limit_reached=False,
        page_limit_reached=False,
        hard_page_limit_reached=False,
        next_url_remaining=False,
    )

    guarded_result = importer._fail_closed_on_unexpected_empty_resource(
        {"canonical_api_base": importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE},
        resource_type,
        result,
    )

    assert guarded_result.complete is False
    assert guarded_result.error == importer.EXPECTED_NONEMPTY_RESOURCE_ERROR


def test_cigna_empty_endpoint_remains_complete():
    result = importer.ResourceFetchResult(
        model=object,
        rows=[],
        rows_fetched=0,
        rows_written=0,
        pages_fetched=1,
        complete=True,
        row_limit_reached=False,
        page_limit_reached=False,
        hard_page_limit_reached=False,
        next_url_remaining=False,
    )

    guarded_result = importer._fail_closed_on_unexpected_empty_resource(
        {"canonical_api_base": importer.CIGNA_PROVIDER_DIRECTORY_BASE},
        "Endpoint",
        result,
    )

    assert guarded_result is result


@pytest.mark.parametrize(
    "api_base",
    [
        importer.ARKANSAS_PROVIDER_DIRECTORY_BASE,
        importer.CONTRA_COSTA_PROVIDER_DIRECTORY_BASE,
        importer.MAINE_PROVIDER_DIRECTORY_BASE,
        importer.MISSOURI_PROVIDER_DIRECTORY_BASE,
        importer.NEBRASKA_DHHS_PROVIDER_DIRECTORY_BASE,
        importer.TMHP_PROVIDER_DIRECTORY_BASE,
        importer.UHC_PROVIDER_DIRECTORY_BASE,
        importer.WASHINGTON_PROVIDER_DIRECTORY_BASE,
        importer.WYOMING_PROVIDER_DIRECTORY_BASE,
    ],
)
def test_state_directory_empty_core_resource_fails_closed(api_base):
    fetch_result = importer.ResourceFetchResult(
        model=object,
        rows=[],
        rows_fetched=0,
        rows_written=0,
        pages_fetched=1,
        complete=True,
        row_limit_reached=False,
        page_limit_reached=False,
        hard_page_limit_reached=False,
        next_url_remaining=False,
    )

    guarded_result = importer._fail_closed_on_unexpected_empty_resource(
        {"canonical_api_base": api_base},
        "Practitioner",
        fetch_result,
    )

    assert guarded_result.complete is False
    assert guarded_result.error == importer.EXPECTED_NONEMPTY_RESOURCE_ERROR


def test_iowa_current_empty_location_census_remains_complete():
    """Accept a proven empty Location while retaining populated-core guards."""
    source_row = importer._source_row_from_seed(
        importer._reviewed_provider_directory_candidate_seed_rows(
            source_query="Iowa Medicaid"
        )[0]
    )
    fetch_result = importer.ResourceFetchResult(
        model=object,
        rows=[],
        rows_fetched=0,
        rows_written=0,
        pages_fetched=1,
        complete=True,
        row_limit_reached=False,
        page_limit_reached=False,
        hard_page_limit_reached=False,
        next_url_remaining=False,
    )

    location_result = importer._fail_closed_on_unexpected_empty_resource(
        source_row,
        "Location",
        fetch_result,
    )
    practitioner_result = importer._fail_closed_on_unexpected_empty_resource(
        source_row,
        "Practitioner",
        fetch_result,
    )

    assert location_result is fetch_result
    assert location_result.complete is True
    assert location_result.error is None
    assert practitioner_result.complete is False
    assert practitioner_result.error == importer.EXPECTED_NONEMPTY_RESOURCE_ERROR


def test_amerihealth_provider_api_uses_state_nonempty_core():
    expected_resources = importer._expected_nonempty_resource_types(
        {"api_base": importer.AMERIHEALTH_CARITAS_CARRIER_PROVIDER_API_BASE}
    )

    assert expected_resources == set(importer.STATE_EXPECTED_NONEMPTY_RESOURCES)


@pytest.mark.parametrize(
    ("api_base", "org_name", "expected_resources"),
    [
        (
            importer.MICHIGAN_PROVIDER_DIRECTORY_BASE,
            "Blue Cross Blue Shield of Michigan",
            tuple(sorted(importer.MICHIGAN_SUPPORTED_RESOURCES)),
        ),
        (
            importer.SCAN_PROVIDER_DIRECTORY_BASE,
            "SCAN Health Plan",
            importer.SCAN_EXPECTED_NONEMPTY_RESOURCES,
        ),
    ],
)
def test_candidate_known_nonempty_resources_fail_closed(
    api_base,
    org_name,
    expected_resources,
):
    source_row = importer._source_row_from_seed(
        {
            "id": "candidate",
            "org_name": org_name,
            "api_base": api_base,
        }
    )
    assert source_row["metadata_json"][
        "provider_directory_expected_nonempty_resources"
    ] == list(expected_resources)

    for resource_type in expected_resources:
        fetch_result = importer.ResourceFetchResult(
            model=object,
            rows=[],
            rows_fetched=0,
            rows_written=0,
            pages_fetched=1,
            complete=True,
            row_limit_reached=False,
            page_limit_reached=False,
            hard_page_limit_reached=False,
            next_url_remaining=False,
        )

        guarded_result = importer._fail_closed_on_unexpected_empty_resource(
            source_row,
            resource_type,
            fetch_result,
        )

        assert guarded_result.complete is False
        assert guarded_result.error == importer.EXPECTED_NONEMPTY_RESOURCE_ERROR


@pytest.mark.parametrize("resource_type", ["HealthcareService", "Endpoint"])
def test_scan_known_empty_resources_remain_complete(resource_type):
    source_row = importer._source_row_from_seed(
        {
            "id": "scan",
            "org_name": "SCAN Health Plan",
            "api_base": importer.SCAN_PROVIDER_DIRECTORY_BASE,
        }
    )
    fetch_result = importer.ResourceFetchResult(
        model=object,
        rows=[],
        rows_fetched=0,
        rows_written=0,
        pages_fetched=1,
        complete=True,
        row_limit_reached=False,
        page_limit_reached=False,
        hard_page_limit_reached=False,
        next_url_remaining=False,
    )

    assert importer._fail_closed_on_unexpected_empty_resource(
        source_row,
        resource_type,
        fetch_result,
    ) is fetch_result


@pytest.mark.asyncio
async def test_probe_sources_records_credential_descriptor_without_secret(monkeypatch):
    monkeypatch.setenv("PAYER_DIRECTORY_TOKEN", "secret-token")
    monkeypatch.setenv(
        importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV,
        json.dumps({"sources": {"source_a": {"bearer_token": "env:PAYER_DIRECTORY_TOKEN"}}}),
    )

    async def fake_fetch_json_with_options(url, *, timeout, extra_headers=None, query_params=None):
        assert extra_headers == {"Authorization": "Bearer secret-token"}
        assert query_params == {}
        if url == "https://payer.example/fhir/Practitioner?_count=1":
            return 200, {"resourceType": "Bundle", "entry": []}, None, 5
        assert url == "https://payer.example/fhir/metadata?_format=json"
        return (
            200,
            {
                "resourceType": "CapabilityStatement",
                "fhirVersion": "4.0.1",
                "rest": [{"resource": [{"type": "Practitioner"}]}],
            },
            None,
            10,
        )

    upserts = []

    async def fake_upsert(model, rows, **_kwargs):
        upserts.append((model, rows))
        return len(rows)

    monkeypatch.setattr(importer, "_fetch_json_with_options", fake_fetch_json_with_options)
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)

    tested_source_map = {
        "source_id": "source_a",
        "org_name": "Payer",
        "api_base": "https://payer.example/fhir",
        "canonical_api_base": "https://payer.example/fhir",
        "metadata_json": {},
    }
    await importer._run_source_probe_batch(
        [tested_source_map],
        timeout=3,
        concurrency=1,
        run_id="run_1",
    )

    source_rows = next(observed_rows for model, observed_rows in upserts if model is ProviderDirectorySource)
    metadata = source_rows[0]["metadata_json"]
    assert source_rows[0]["last_validated_status"] == "valid"
    assert source_rows[0]["last_validated_at"] is not None
    assert metadata["credential"] == {
        "matched_by": ["sources:source_a"],
        "header_names": ["Authorization"],
        "query_param_names": [],
    }
    assert "secret-token" not in json.dumps(metadata)


@pytest.mark.asyncio
async def test_caresource_public_override_probes_and_selects_without_credentials(
    monkeypatch,
):
    """CareSource probes and imports anonymously despite its stale auth label."""
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV, raising=False)
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_FILE_ENV, raising=False)
    calls: list[str] = []
    capability_statement_map = {
        "resourceType": "CapabilityStatement",
        "fhirVersion": "4.0.1",
        "rest": [{"resource": [{"type": "Practitioner"}]}],
    }

    async def fake_fetch_source_json(_source, request_url, *, timeout):
        calls.append(request_url)
        if request_url == importer.CARESOURCE_PROVIDER_DIRECTORY_METADATA_URL:
            return 200, capability_statement_map, None, 4
        assert request_url == (
            f"{importer.CARESOURCE_PROVIDER_DIRECTORY_BASE}/Practitioner?_count=1"
        )
        return 200, {"resourceType": "Bundle", "entry": []}, None, 5

    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_source_json)
    tested_source_map = importer._source_row_from_seed(
        {
            "id": "57",
            "org_tin": "31-1703368",
            "org_name": "CareSource",
            "plan_name": "Medicare Advantage, Medicaid MCO",
            "api_base": importer.CARESOURCE_PROVIDER_DIRECTORY_BASE,
            "requires_registration": "false",
            "requires_api_key": "false",
            "auth_type": "OAuth2/SMART",
            "last_validated_status": "auth_required",
            "source": "defacto_2024",
        }
    )

    probe, returned_capability = await importer._probe_source(
        tested_source_map,
        timeout=3,
        run_id="run_caresource",
    )
    selected, metrics = importer._select_resource_import_sources(
        [tested_source_map],
        valid_source_ids={tested_source_map["source_id"]},
        open_only=True,
        include_auth_required=False,
        requested_resource_types=list(importer.DEFAULT_RESOURCES),
    )

    assert returned_capability == capability_statement_map
    assert probe["status"] == "valid"
    assert probe["credential"] is None
    assert selected == [tested_source_map]
    assert metrics["source_import_sources_selected"] == 1
    assert metrics["source_import_sources_selected_live_probe_valid"] == 1
    assert metrics["source_import_sources_selected_declared_credentialed"] == 0
    assert calls == [
        importer.CARESOURCE_PROVIDER_DIRECTORY_METADATA_URL,
        f"{importer.CARESOURCE_PROVIDER_DIRECTORY_BASE}/Practitioner?_count=1",
    ]


@pytest.mark.asyncio
async def test_probe_source_marks_resource_access_denied_as_auth_required(monkeypatch):
    calls: list[str] = []
    capability_payload_map = {
        "resourceType": "CapabilityStatement",
        "fhirVersion": "4.0.1",
        "rest": [{"resource": [{"type": "Practitioner"}]}],
    }

    async def fake_fetch_source_json(_source, request_url, *, timeout):
        calls.append(request_url)
        if "/metadata" in request_url:
            return 200, capability_payload_map, None, 5
        return (
            403,
            {
                "resourceType": "OperationOutcome",
                "issue": [{"diagnostics": "Access denied by rule"}],
            },
            None,
            7,
        )

    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_source_json)

    probe, returned_capability = await importer._probe_source(
        {
            "source_id": "gated_source",
            "api_base": "https://gated.example/fhir",
            "canonical_api_base": "https://gated.example/fhir",
        },
        timeout=3,
        run_id="run_1",
    )

    assert probe["status"] == "auth_required"
    assert probe["http_status"] == 403
    assert probe["resource_probe"]["resource_type"] == "Practitioner"
    assert returned_capability == capability_payload_map
    assert calls == [
        "https://gated.example/fhir/metadata?_format=json",
        "https://gated.example/fhir/Practitioner?_count=1",
        "https://gated.example/fhir/metadata",
        "https://gated.example/fhir/Practitioner?_count=1",
    ]


@pytest.mark.asyncio
async def test_probe_sources_marks_source_timeout_when_probe_exceeds_hard_deadline(monkeypatch):
    async def fake_probe_source(_source, *, timeout, run_id):
        await asyncio.sleep(1)
        return {"status": "valid"}, {"resourceType": "CapabilityStatement"}

    upserts = []

    async def fake_upsert(model, rows, **_kwargs):
        upserts.append((model, rows))
        return len(rows)

    monkeypatch.setattr(importer, "_probe_source", fake_probe_source)
    monkeypatch.setattr(importer, "_source_probe_hard_timeout_seconds", lambda _source, *, timeout: 0.01)
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)

    probed, valid, valid_source_ids = await importer._run_source_probe_batch(
        [
            {
                "source_id": "source_timeout",
                "org_name": "Slow Payer",
                "api_base": "https://slow.example/fhir",
                "canonical_api_base": "https://slow.example/fhir",
            }
        ],
        timeout=3,
        concurrency=1,
        run_id="run_1",
    )

    assert (probed, valid, valid_source_ids) == (1, 0, set())
    capability_rows = next(observed_rows for model, observed_rows in upserts if model is ProviderDirectoryCapability)
    source_rows = next(observed_rows for model, observed_rows in upserts if model is ProviderDirectorySource)
    assert capability_rows[0]["probe_status"] == "timeout"
    assert "hard deadline" in capability_rows[0]["error"]
    assert source_rows[0]["last_probe_status"] == "timeout"
    assert "hard deadline" in source_rows[0]["last_probe_error"]


@pytest.mark.asyncio
async def test_probe_source_classifies_declared_credentialed_non_fhir_as_auth_required(monkeypatch):
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV, raising=False)
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_FILE_ENV, raising=False)

    async def fake_fetch_source_json(source, url, *, timeout):
        raise AssertionError("credential-gated sources without credentials should not be fetched")

    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_source_json)

    probe, payload = await importer._probe_source(
        {
            "source_id": "source_a",
            "api_base": "https://apps.availity.com/availity/public-fhir/fhir/v1/example/r4",
            "auth_type": "OAuth2 Client Credentials",
            "requires_registration": True,
        },
        timeout=3,
        run_id="run_1",
    )

    assert payload is None
    assert probe["status"] == "auth_required"
    assert probe["http_status"] is None
    assert probe["credential"] is None
    assert "no matching credentials" in probe["error"]


@pytest.mark.asyncio
async def test_probe_source_classifies_declared_credentialed_metadata_as_auth_required_without_credentials(monkeypatch):
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV, raising=False)
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_FILE_ENV, raising=False)

    async def fake_fetch_source_json(source, url, *, timeout):
        raise AssertionError("credential-gated sources without credentials should not be fetched")

    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_source_json)

    probe, payload = await importer._probe_source(
        {
            "source_id": "source_a",
            "api_base": "https://payer.example/fhir",
            "auth_type": "OAuth2/SMART",
            "requires_registration": True,
        },
        timeout=3,
        run_id="run_1",
    )

    assert payload is None
    assert probe["status"] == "auth_required"
    assert probe["http_status"] is None
    assert probe["credential"] is None
    assert "no matching credentials" in probe["error"]


@pytest.mark.asyncio
async def test_probe_source_classifies_known_onboarding_gateway_without_credentials_as_auth_required(monkeypatch):
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV, raising=False)
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_FILE_ENV, raising=False)

    async def fake_fetch_source_json(source, url, *, timeout):
        raise AssertionError("known onboarding gateways without credentials should not be fetched")

    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_source_json)

    probe, payload = await importer._probe_source(
        {
            "source_id": "source_availity",
            "api_base": "https://apps.availity.com/availity/public-fhir/fhir/v1/example/r4",
            "auth_type": "open",
            "requires_registration": False,
        },
        timeout=3,
        run_id="run_1",
    )

    assert payload is None
    assert probe["status"] == "auth_required"
    assert probe["http_status"] is None
    assert "no matching credentials" in probe["error"]


@pytest.mark.asyncio
async def test_probe_source_classifies_cms_public_no_seed_as_auth_required(monkeypatch):
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV, raising=False)
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_FILE_ENV, raising=False)

    async def fake_fetch_source_json(source, url, *, timeout):
        raise AssertionError("CMS Public=No provider directory sources without credentials should not be fetched")

    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_source_json)

    probe, payload = await importer._probe_source(
        {
            "source_id": "source_non_public",
            "api_base": "https://us120.fhir.m3.edifecsfedcloud.com/nd_pd",
            "auth_type": "none",
            "requires_registration": False,
            "metadata_json": {
                "note": "CMS SMA Directory: Status=Active, Public=No, Impl=8/11/2022, FHIR=4.0.1",
            },
        },
        timeout=3,
        run_id="run_1",
    )

    assert payload is None
    assert probe["status"] == "auth_required"
    assert probe["http_status"] is None
    assert probe["credential"] is None
    assert "no matching credentials" in probe["error"]


@pytest.mark.asyncio
async def test_probe_sources_does_not_count_credentialed_metadata_without_credentials_as_valid(monkeypatch):
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV, raising=False)
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_FILE_ENV, raising=False)

    async def fake_fetch_source_json(source, url, *, timeout):
        return (
            200,
            {
                "resourceType": "CapabilityStatement",
                "fhirVersion": "4.0.1",
                "rest": [{"resource": [{"type": "Practitioner"}]}],
            },
            None,
            14,
        )

    upserts = []

    async def fake_upsert(model, rows, **_kwargs):
        upserts.append((model, rows))
        return len(rows)

    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_source_json)
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)

    tested_source_map = {
        "source_id": "source_a",
        "org_name": "Payer",
        "api_base": "https://payer.example/fhir",
        "canonical_api_base": "https://payer.example/fhir",
        "auth_type": "OAuth2/SMART",
        "requires_registration": True,
    }
    probed, valid, valid_source_ids = await importer._run_source_probe_batch(
        [tested_source_map],
        timeout=3,
        concurrency=1,
        run_id="run_1",
    )

    assert (probed, valid, valid_source_ids) == (1, 0, set())
    capability_rows = next(observed_rows for model, observed_rows in upserts if model is ProviderDirectoryCapability)
    source_rows = next(observed_rows for model, observed_rows in upserts if model is ProviderDirectorySource)
    assert capability_rows[0]["probe_status"] == "auth_required"
    assert capability_rows[0]["auth_required"] is True
    assert source_rows[0]["last_validated_status"] == "auth_required"


@pytest.mark.asyncio
async def test_probe_source_keeps_alohr_graphql_connector_valid_without_fhir_credentials(monkeypatch):
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV, raising=False)
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_FILE_ENV, raising=False)

    def fake_post_json(url, request_payload_by_name, *, timeout, extra_headers=None):
        assert url == importer.ALOHR_GRAPHQL_URL
        assert request_payload_by_name == {"query": importer.ALOHR_PROBE_QUERY, "variables": {}}
        assert extra_headers == {"tenantId": importer.ALOHR_TENANT_ID}
        return 200, {"data": {"providers": {"nextToken": "next"}}}, None, 14

    monkeypatch.setattr(importer, "_post_json_sync", fake_post_json)

    probe, payload = await importer._probe_source(
        {
            "source_id": "source_alohr",
            "api_base": importer.ALOHR_FHIR_PROVIDER_DIRECTORY_BASE,
            "auth_type": "OAuth2/SMART",
            "requires_registration": True,
            "metadata_json": {"provider_directory_graphql_tenant_id": importer.ALOHR_TENANT_ID},
        },
        timeout=3,
        run_id="run_1",
    )

    assert payload is None
    assert probe["status"] == "valid"
    assert probe["url"] == importer.ALOHR_GRAPHQL_URL
    assert probe["credential"]["header_names"] == ["tenantId"]


@pytest.mark.asyncio
async def test_probe_sources_uses_alohr_graphql_when_fhir_metadata_is_gone(monkeypatch):
    def fake_post_json(url, request_payload_by_name, *, timeout, extra_headers=None):
        assert url == importer.ALOHR_GRAPHQL_URL
        assert request_payload_by_name == {"query": importer.ALOHR_PROBE_QUERY, "variables": {}}
        assert timeout == 3
        assert extra_headers == {"tenantId": importer.ALOHR_TENANT_ID}
        return 200, {"data": {"providers": {"nextToken": "next"}}}, None, 25

    upsert_batches = []

    async def fake_upsert(model, model_rows, **_kwargs):
        upsert_batches.append((model, model_rows))
        return len(model_rows)

    monkeypatch.setattr(importer, "_post_json_sync", fake_post_json)
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)

    source_by_name = {
        "source_id": "source_alohr",
        "api_base": importer.ALOHR_FHIR_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.ALOHR_FHIR_PROVIDER_DIRECTORY_BASE,
        "auth_type": "OAuth2/SMART",
        "requires_registration": True,
        "metadata_json": {"provider_directory_graphql_tenant_id": importer.ALOHR_TENANT_ID},
    }
    probed, valid, valid_source_ids = await importer._run_source_probe_batch(
        [source_by_name],
        timeout=3,
        concurrency=1,
        run_id="run_1",
    )

    assert (probed, valid, valid_source_ids) == (1, 1, {"source_alohr"})
    capability_rows = next(model_rows for model, model_rows in upsert_batches if model is ProviderDirectoryCapability)
    source_rows = next(model_rows for model, model_rows in upsert_batches if model is ProviderDirectorySource)
    assert capability_rows[0]["probe_status"] == "valid"
    assert capability_rows[0]["metadata_url"] == importer.ALOHR_GRAPHQL_URL
    assert source_rows[0]["last_probe_status"] == "valid"
    assert source_rows[0]["last_validated_status"] == "valid"
    assert source_rows[0]["metadata_json"]["credential"]["header_names"] == ["tenantId"]


@pytest.mark.asyncio
async def test_probe_source_classifies_known_onboarding_gateway_non_fhir_as_auth_required(monkeypatch):
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV, raising=False)
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_FILE_ENV, raising=False)

    async def fake_fetch_source_json(source, url, *, timeout):
        raise AssertionError("known onboarding gateways without credentials should not be fetched")

    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_source_json)

    probe, payload = await importer._probe_source(
        {
            "source_id": "source_a",
            "api_base": "https://apps.availity.com/availity/public-fhir/fhir/v1/alignmenthealth/r4",
            "auth_type": "open",
            "requires_registration": False,
        },
        timeout=3,
        run_id="run_1",
    )

    assert payload is None
    assert probe["status"] == "auth_required"
    assert probe["http_status"] is None
    assert probe["credential"] is None
    assert "no matching credentials" in probe["error"]


@pytest.mark.asyncio
async def test_probe_source_keeps_open_non_fhir_as_valid_non_fhir(monkeypatch):
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV, raising=False)
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_FILE_ENV, raising=False)

    async def fake_fetch_source_json(source, url, *, timeout):
        return 200, None, None, 14

    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_source_json)

    probe, payload = await importer._probe_source(
        {
            "source_id": "source_a",
            "api_base": "https://payer.example/fhir",
            "auth_type": "open",
            "requires_registration": False,
        },
        timeout=3,
        run_id="run_1",
    )

    assert payload is None
    assert probe["status"] == "valid_non_fhir"
    assert probe["http_status"] == 200
    assert probe["credential"] is None
    assert probe["error"] is None


def test_parse_capability_extracts_resources_and_search_params():
    parsed = importer.parse_capability(
        {"source_id": "source_a", "api_base": "https://example.test/fhir"},
        {
            "resourceType": "CapabilityStatement",
            "fhirVersion": "4.0.1",
            "software": {"name": "HAPI"},
            "rest": [
                {
                    "resource": [
                        {"type": "InsurancePlan", "searchParam": [{"name": "name"}]},
                        {"type": "PractitionerRole", "searchParam": [{"name": "practitioner"}]},
                    ]
                }
            ],
        },
        {"status": "valid", "http_status": 200, "response_time_ms": 25, "url": "https://example.test/fhir/metadata"},
    )

    assert parsed["fhir_version"] == "4.0.1"
    assert parsed["supported_resources"] == ["InsurancePlan", "PractitionerRole"]
    assert parsed["search_params"]["InsurancePlan"] == ["name"]
    assert parsed["auth_required"] is False


@pytest.mark.asyncio
async def test_probe_sources_persists_resolved_api_base_for_repaired_catalog_url(monkeypatch):
    async def fake_fetch_json(url, *, timeout):
        if url == "https://fhir.humana.com/api/metadata?_format=json":
            return (
                200,
                {
                    "resourceType": "CapabilityStatement",
                    "fhirVersion": "4.0.1",
                    "rest": [{"resource": [{"type": "Practitioner"}]}],
                },
                None,
                10,
            )
        if url == "https://fhir.humana.com/api/Practitioner?_count=1":
            return 200, {"resourceType": "Bundle", "entry": []}, None, 5
        return 400, {"resourceType": "OperationOutcome"}, None, 5

    upserts = []

    async def fake_upsert(model, rows, **_kwargs):
        upserts.append((model, rows))
        return len(rows)

    monkeypatch.setattr(importer, "_fetch_json", fake_fetch_json)
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)

    tested_source_map = importer._source_row_from_seed(
        {
            "id": "humana",
            "org_name": "Humana",
            "plan_name": "Medicare Advantage",
            "api_base": "https://fhir.humana.com/api/provider-directory/",
        }
    )

    probed, valid, valid_source_ids = await importer._run_source_probe_batch(
        [tested_source_map],
        timeout=3,
        concurrency=1,
        run_id="run_1",
    )

    assert (probed, valid, valid_source_ids) == (1, 1, {tested_source_map["source_id"]})
    assert tested_source_map["api_base"] == "https://fhir.humana.com/api"
    capability_rows = next(observed_rows for model, observed_rows in upserts if model is ProviderDirectoryCapability)
    source_rows = next(observed_rows for model, observed_rows in upserts if model is ProviderDirectorySource)
    assert capability_rows[0]["api_base"] == "https://fhir.humana.com/api"
    assert source_rows[0]["api_base"] == "https://fhir.humana.com/api"
    assert source_rows[0]["last_validated_status"] == "valid"
    assert source_rows[0]["metadata_json"]["provider_directory_override"] == "humana_public_fhir_api"
    assert source_rows[0]["metadata_json"]["provider_directory_previous_api_base"] == (
        "https://fhir.humana.com/api/provider-directory/"
    )


def test_parse_fhir_resource_maps_plan_practitioner_location_role_and_endpoint():
    """Verify parse fhir resource maps plan practitioner location role and endpoint."""
    plan_model, plan_row = importer.parse_fhir_resource(
        "source_a",
        {
            "resourceType": "InsurancePlan",
            "id": "plan-1",
            "identifier": [{"system": "https://example.test/plan-id", "value": "H1234-001"}],
            "name": "Example HMO",
            "network": [{"reference": "Organization/network-1"}],
            "extension": [
                {
                    "url": (
                        "http://hl7.org/fhir/us/davinci-pdex-plan-net/"
                        "StructureDefinition/network-reference"
                    ),
                    "valueReference": {"reference": "Organization/ignored-plan-extension"},
                }
            ],
        },
        run_id="run_1",
    )
    practitioner_model, practitioner_row = importer.parse_fhir_resource(
        "source_a",
        {
            "resourceType": "Practitioner",
            "id": "prac-1",
            "identifier": [
                {"system": "http://hl7.org/fhir/sid/us-npi", "value": "1234567893"},
                {
                    "system": "https://example.test/license",
                    "value": "IL-1234",
                },
            ],
            "name": [
                {
                    "use": "official",
                    "family": "Rivera",
                    "given": ["Alex", "Morgan"],
                    "prefix": ["Dr."],
                    "suffix": ["MD"],
                }
            ],
            "gender": "female",
            "birthDate": "1970-01-01",
            "address": [
                {
                    "use": "work",
                    "line": ["100 Main St", "Suite 2"],
                    "city": "Chicago",
                    "state": "IL",
                    "postalCode": "60601",
                }
            ],
            "qualification": [
                {
                    "identifier": [
                        {
                            "system": "https://example.test/license",
                            "value": "IL-1234",
                        }
                    ],
                    "code": {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/v2-0360",
                                "code": "MD",
                                "display": "Doctor of Medicine",
                            }
                        ]
                    },
                    "period": {"start": "2001-01-01"},
                    "issuer": {
                        "reference": "Organization/medical-school-1",
                        "display": "Example Medical School",
                    },
                }
            ],
            "communication": [
                {
                    "coding": [
                        {
                            "system": "urn:ietf:bcp:47",
                            "code": "es",
                            "display": "Spanish",
                        }
                    ],
                    "text": "Spanish",
                }
            ],
            "photo": [
                {
                    "contentType": "image/jpeg",
                    "url": "https://images.example.test/practitioner/prac-1.jpg",
                    "data": "must-not-be-retained",
                    "title": "Portrait",
                },
                {
                    "contentType": "image/jpeg",
                    "url": "https://images.example.test/private.jpg?token=secret",
                },
            ],
        },
    )
    location_model, location_row = importer.parse_fhir_resource(
        "source_a",
        {
            "resourceType": "Location",
            "id": "loc-1",
            "name": "Example Clinic",
            "telecom": [{"system": "phone", "value": "312-555-0100"}],
            "address": {
                "line": ["100 Main St", "Suite 2"],
                "city": "Chicago",
                "state": "IL",
                "postalCode": "60601-1234",
                "country": "US",
            },
        },
    )
    role_model, role_row = importer.parse_fhir_resource(
        "source_a",
        {
            "resourceType": "PractitionerRole",
            "id": "role-1",
            "identifier": [{"system": "https://example.test/fhir/npi", "value": "1234567893"}],
            "practitioner": {"reference": "Practitioner/prac-1"},
            "organization": {"reference": "Organization/org-1"},
            "location": [{"reference": "Location/loc-1"}],
            "network": [{"reference": "Organization/network-1"}],
            "insurancePlan": [{"reference": "InsurancePlan/plan-1"}],
            "endpoint": [{"reference": "Endpoint/endpoint-1"}],
            "extension": [
                {
                    "url": (
                        "http://hl7.org/fhir/us/davinci-pdex-plan-net/"
                        "StructureDefinition/network-reference"
                    ),
                    "valueReference": {
                        "reference": "https://example.test/fhir/Organization/network-1"
                    },
                },
                {
                    "url": "https://example.test/wrapper",
                    "extension": [
                        {
                            "url": (
                                "https://hl7.org/fhir/us/davinci-pdex-plan-net/"
                                "StructureDefinition/network-reference|1.2.0"
                            ),
                            "valueReference": {"reference": "Organization/network-2"},
                        }
                    ],
                },
                {
                    "url": "https://example.test/StructureDefinition/network-reference",
                    "valueReference": {"reference": "Organization/wrong-namespace"},
                },
                {
                    "url": (
                        "http://hl7.org/fhir/us/davinci-pdex-plan-net/"
                        "StructureDefinition/network-reference"
                    ),
                    "valueReference": {"reference": "Practitioner/not-a-network"},
                },
            ],
        },
    )
    endpoint_model, endpoint_row = importer.parse_fhir_resource(
        "source_a",
        {
            "resourceType": "Endpoint",
            "id": "endpoint-1",
            "status": "active",
            "connectionType": {
                "system": "http://terminology.hl7.org/CodeSystem/endpoint-connection-type",
                "code": "hl7-fhir-rest",
                "display": "HL7 FHIR",
            },
            "name": "Provider Directory FHIR",
            "managingOrganization": {"reference": "Organization/org-1"},
            "contact": [{"system": "phone", "value": "312-555-1111"}],
            "payloadType": [{"coding": [{"system": "http://hl7.org/fhir/resource-types", "code": "Organization"}]}],
            "payloadMimeType": ["application/fhir+json"],
            "address": "https://example.test/fhir",
            "header": ["X-Test: true"],
            "period": {"start": "2026-01-01"},
        },
        run_id="run_2",
    )

    assert plan_model is ProviderDirectoryInsurancePlan
    assert plan_row["plan_identifier"] == "H1234-001"
    assert plan_row["network_refs"] == ["Organization/network-1"]
    assert plan_row["last_seen_run_id"] == "run_1"
    assert practitioner_model is ProviderDirectoryPractitioner
    assert practitioner_row["npi"] == 1234567893
    assert practitioner_row["family_name"] == "Rivera"
    assert practitioner_row["administrative_gender"] == "female"
    assert practitioner_row["age_years"] is not None
    assert practitioner_row["age_as_of"] == datetime.datetime.utcnow().date().isoformat()
    assert practitioner_row["years_of_practice"] == (
        datetime.datetime.utcnow().date().year - 2001
    )
    assert practitioner_row["years_of_practice_as_of"] == (
        datetime.datetime.utcnow().date().isoformat()
    )
    assert practitioner_row["years_of_practice_basis"] == (
        "FHIR Practitioner.qualification.period.start"
    )
    assert practitioner_row["years_of_practice_start_date"] == "2001-01-01"
    assert practitioner_row["names"][0]["prefix"] == ["Dr."]
    assert practitioner_row["addresses"][0]["postal_code"] == "60601"
    assert practitioner_row["qualifications"][0]["issuer_display"] == "Example Medical School"
    assert practitioner_row["communications"][0]["codes"][0]["code"] == "es"
    assert practitioner_row["photos"] == [
        {
            "content_type": "image/jpeg",
            "url": "https://images.example.test/practitioner/prac-1.jpg",
            "title": "Portrait",
        },
        {"content_type": "image/jpeg"},
    ]
    assert "birthDate" not in practitioner_row
    assert "birth_date" not in practitioner_row
    assert "data" not in json.dumps(practitioner_row["photos"])
    assert location_model is ProviderDirectoryLocation
    assert location_row["zip5"] == "60601"
    assert location_row["state_code"] == "IL"
    assert location_row["telephone_number"] == "312-555-0100"
    assert location_row["phone_number"] == "3125550100"
    assert role_model is ProviderDirectoryPractitionerRole
    assert role_row["npi"] == 1234567893
    assert role_row["location_refs"] == ["Location/loc-1"]
    assert role_row["network_refs"] == ["Organization/network-1", "Organization/network-2"]
    assert role_row["insurance_plan_refs"] == ["InsurancePlan/plan-1"]
    assert role_row["endpoint_refs"] == ["Endpoint/endpoint-1"]
    assert endpoint_model is ProviderDirectoryEndpoint
    assert endpoint_row["connection_type_code"] == "hl7-fhir-rest"
    assert endpoint_row["managing_organization_ref"] == "Organization/org-1"
    assert endpoint_row["payload_type_codes"][0]["code"] == "Organization"
    assert endpoint_row["payload_mime_types"] == ["application/fhir+json"]
    assert endpoint_row["address"] == "https://example.test/fhir"
    assert endpoint_row["period_start"] == "2026-01-01"
    assert endpoint_row["last_seen_run_id"] == "run_2"


@pytest.mark.parametrize(
    ("birth_date", "expected"),
    [
        ("2009-07-13", (None, None)),
        ("2008-07-13", (18, "2026-07-13")),
        ("1926-07-13", (100, "2026-07-13")),
        ("1925-07-13", (None, None)),
    ],
)
def test_derived_age_enforces_credible_provider_age_bounds(birth_date, expected):
    as_of = datetime.date(2026, 7, 13)

    assert importer._derived_age(birth_date, as_of=as_of) == expected


@pytest.mark.parametrize(
    "birth_date",
    ["1970", "1970-02-30", "2026-07-14", "2027-01-01"],
)
def test_derived_age_requires_safe_full_nonfuture_date(birth_date):
    as_of = datetime.date(2026, 7, 13)

    assert importer._derived_age("1970-07-14", as_of=as_of) == (
        55,
        "2026-07-13",
    )
    assert importer._derived_age(birth_date, as_of=as_of) == (None, None)


def test_derived_years_of_practice_uses_earliest_plausible_qualification():
    as_of = datetime.date(2026, 7, 13)
    qualifications = [
        {"period": {"start": "1980-01-01"}},
        {"period": {"start": "2005-07-14T00:00:00Z"}},
        {"period": {"start": "2001-01-01"}},
        {"period": {"start": "2027-01-01"}},
    ]

    assert importer._derived_years_of_practice(
        qualifications,
        as_of=as_of,
        birth_date_value="1970-01-01",
    ) == (
        25,
        "2026-07-13",
        "FHIR Practitioner.qualification.period.start",
        "2001-01-01",
    )


def test_derived_years_of_practice_rejects_unsupported_or_implausible_dates():
    as_of = datetime.date(2026, 7, 13)

    assert importer._derived_years_of_practice(
        [
            {"period": {"start": "1970"}},
            {"period": {"start": "1980-01-01"}},
            {"period": {"start": "2027-01-01"}},
        ],
        as_of=as_of,
        birth_date_value="1970-01-01",
    ) == (None, None, None, None)
    assert importer._derived_years_of_practice(
        [{"period": {"start": "1900-01-01"}}],
        as_of=as_of,
    ) == (None, None, None, None)


def test_contra_costa_affiliation_telecom_preserves_phone_and_fax_in_dataset_payload():
    contra_costa_affiliation_payload_dict = {
        "resourceType": "OrganizationAffiliation",
        "id": "contra-costa-affiliation-21438",
        "active": True,
        "organization": {"reference": "Organization/contra-costa-health-plan"},
        "participatingOrganization": {"reference": "Organization/21438"},
        "location": [{"reference": "Location/496899"}],
        "telecom": [
            {"system": "phone", "value": "(925) 313-6000", "use": "work", "rank": 1},
            {"system": "fax", "value": "(925) 313-6001", "use": "work", "rank": 2},
        ],
    }

    affiliation_model, affiliation_row = importer.parse_fhir_resource(
        "pdfhir_8ee2865f928f1d67b8a86090",
        contra_costa_affiliation_payload_dict,
        run_id="run_contra_costa",
    )
    endpoint_dataset_rows = importer._endpoint_dataset_resource_rows(
        affiliation_model,
        [affiliation_row],
        dataset_id="dataset_contra_costa",
    )
    expected_dataset_payload_dict = importer._canonical_resource_payload(affiliation_row)

    assert affiliation_model is ProviderDirectoryOrganizationAffiliation
    assert ProviderDirectoryOrganizationAffiliation.__table__.c.telecom.nullable is True
    assert affiliation_row["telecom"] == contra_costa_affiliation_payload_dict["telecom"]
    assert contra_costa_affiliation_payload_dict["telecom"][0]["value"] == "(925) 313-6000"
    assert len(endpoint_dataset_rows) == 1
    assert endpoint_dataset_rows[0]["dataset_id"] == "dataset_contra_costa"
    assert endpoint_dataset_rows[0]["resource_type"] == "OrganizationAffiliation"
    assert endpoint_dataset_rows[0]["resource_id"] == "contra-costa-affiliation-21438"
    assert endpoint_dataset_rows[0]["payload_json"] == expected_dataset_payload_dict
    assert endpoint_dataset_rows[0]["payload_json"]["telecom"] == contra_costa_affiliation_payload_dict[
        "telecom"
    ]


def _healthcare_service_plan_net_payload(new_patients_url):
    """Return a service fixture with accepted and discarded extension content."""

    return {
        "resourceType": "HealthcareService",
        "id": "service-1",
        "providedBy": {"reference": "Organization/org-1"},
        "identifier": [
            {
                "system": "http://hl7.org/fhir/sid/us-npi",
                "value": "1588616783",
            }
        ],
        "extension": [
            {
                "url": new_patients_url,
                "extension": [
                    {
                        "url": "acceptingPatients",
                        "valueCodeableConcept": {
                            "coding": [{"code": "newpt"}, {"code": "not-a-plan-net-code"}]
                        },
                    },
                    {
                        "url": "fromNetwork",
                        "valueReference": {"reference": "Organization/network-1"},
                    },
                    {"url": "implementation-detail", "valueString": "discarded"},
                ],
            },
            {
                "url": "https://example.test/extension/not-plan-net",
                "extension": [
                    {
                        "url": "acceptingPatients",
                        "valueCodeableConcept": {"coding": [{"code": "nopt"}]},
                    }
                ],
            },
        ],
    }


@pytest.mark.parametrize(
    "new_patients_url",
    [
        (
            "http://hl7.org/fhir/us/davinci-pdex-plan-net/"
            "StructureDefinition/newpatients"
        ),
        (
            "https://hl7.org/fhir/us/davinci-pdex-plan-net/"
            "StructureDefinition/plannet-NewPatients-extension"
        ),
    ],
)
def test_healthcare_service_preserves_controlled_plan_net_context_in_artifact_payloads(
    new_patients_url,
):
    service_payload = _healthcare_service_plan_net_payload(new_patients_url)
    parsed_model, parsed_row = importer.parse_fhir_resource(
        "source_a",
        service_payload,
        run_id="run_1",
    )

    canonical_artifacts = importer._canonical_resource_rows(
        parsed_model,
        [parsed_row],
        canonical_api_base="https://payer.example/fhir",
        run_id="run_1",
    )
    dataset_artifacts = importer._endpoint_dataset_resource_rows(
        parsed_model,
        [parsed_row],
        dataset_id="dataset_1",
    )

    expected_accepting_patients = [
        {"code": "newpt", "from_network_ref": "Organization/network-1"}
    ]
    assert parsed_model is ProviderDirectoryHealthcareService
    assert parsed_row["provided_by_ref"] == "Organization/org-1"
    assert parsed_row["npi"] == 1588616783
    assert parsed_row["accepting_patients"] == expected_accepting_patients
    assert canonical_artifacts[0]["payload_json"]["provided_by_ref"] == "Organization/org-1"
    assert canonical_artifacts[0]["payload_json"]["accepting_patients"] == expected_accepting_patients
    assert canonical_artifacts[0]["payload_json"]["npi"] == 1588616783
    assert dataset_artifacts[0]["payload_json"] == importer._canonical_resource_payload(parsed_row)
    assert dataset_artifacts[0]["payload_json"]["accepting_patients"] == expected_accepting_patients


def test_healthcare_service_preserves_reviewed_profile_context():
    parsed_model, parsed_row = importer.parse_fhir_resource(
        "source_a",
        {
            "resourceType": "HealthcareService",
            "id": "service-profile-1",
            "program": [{"text": "Diabetes education"}],
            "characteristic": [{"text": "Wheelchair accessible"}],
            "communication": [
                {
                    "coding": [
                        {"system": "urn:ietf:bcp:47", "code": "en"}
                    ]
                }
            ],
            "referralMethod": [{"text": "Phone"}],
            "serviceProvisionCode": [{"text": "No referral required"}],
            "eligibility": [
                {"code": {"text": "Adults"}, "comment": "Age 18 and older"}
            ],
            "appointmentRequired": True,
            "availableTime": [{"daysOfWeek": ["mon"], "allDay": True}],
            "notAvailable": [
                {"description": "Holiday", "during": {"start": "2026-12-25"}}
            ],
            "availabilityExceptions": "Closed on federal holidays",
            "extraDetails": "Call the office before arrival.",
            "photo": [
                {
                    "contentType": "image/png",
                    "url": "https://images.example.test/services/1.png",
                }
            ],
        },
    )

    assert parsed_model is ProviderDirectoryHealthcareService
    assert parsed_row["program_codes"] == [{"text": "Diabetes education"}]
    assert parsed_row["characteristic_codes"] == [
        {"text": "Wheelchair accessible"}
    ]
    assert parsed_row["communication_codes"][0]["code"] == "en"
    assert parsed_row["appointment_required"] is True
    assert parsed_row["eligibility"] == [
        {"code_codes": [{"text": "Adults"}], "comment": "Age 18 and older"}
    ]
    assert parsed_row["available_time"][0]["daysOfWeek"] == ["mon"]
    assert parsed_row["availability_exceptions"] == "Closed on federal holidays"
    assert parsed_row["photos"][0]["url"].endswith("/services/1.png")


def test_healthcare_service_discards_unrecognized_new_patient_extensions_and_codes():
    parsed_model, parsed_row = importer.parse_fhir_resource(
        "source_a",
        {
            "resourceType": "HealthcareService",
            "id": "service-1",
            "extension": [
                {
                    "url": "https://example.test/StructureDefinition/newpatients",
                    "extension": [
                        {
                            "url": "acceptingPatients",
                            "valueCodeableConcept": {"coding": [{"code": "newpt"}]},
                        }
                    ],
                },
                {
                    "url": (
                        "https://hl7.org/fhir/us/davinci-pdex-plan-net/"
                        "StructureDefinition/newpatients"
                    ),
                    "extension": [
                        {
                            "url": "acceptingPatients",
                            "valueCodeableConcept": {"coding": [{"code": "unknown"}]},
                        }
                    ],
                },
            ],
        },
    )

    assert parsed_model is ProviderDirectoryHealthcareService
    assert parsed_row["accepting_patients"] == []


def test_identifier_mapping_accepts_type_coded_npi_and_systemless_plan_ids():
    _role_model, role_row = importer.parse_fhir_resource(
        "source_michigan",
        {
            "resourceType": "PractitionerRole",
            "id": "role-type-coded-npi",
            "identifier": [
                {
                    "system": "https://example.test/generic-provider-id",
                    "type": {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                                "code": "NPI",
                                "display": "National provider identifier",
                            }
                        ]
                    },
                    "value": "1588616783",
                }
            ],
        },
    )
    _plan_model, plan_row = importer.parse_fhir_resource(
        "source_cigna",
        {
            "resourceType": "InsurancePlan",
            "id": "plan-systemless",
            "identifier": [{"value": "CIGNA-PPO-2026"}],
        },
    )
    _hios_model, hios_row = importer.parse_fhir_resource(
        "source_hios",
        {
            "resourceType": "InsurancePlan",
            "id": "plan-hios",
            "identifier": [
                {
                    "system": "https://example.test/generic-plan-id",
                    "type": {"coding": [{"code": "HIOS"}]},
                    "value": "12345IL0010001",
                }
            ],
        },
    )

    assert role_row["npi"] == 1588616783
    assert plan_row["plan_identifier"] == "CIGNA-PPO-2026"
    assert hios_row["plan_identifier"] == "12345IL0010001"


def test_parse_fhir_resource_maps_nebraska_participating_network_extension():
    _model, row = importer.parse_fhir_resource(
        "source_nebraska",
        {
            "resourceType": "PractitionerRole",
            "id": "role-nebraska-network",
            "extension": [
                {
                    "url": (
                        "http://hl7.org/fhir/us/davinci-pdex-plan-net/"
                        "StructureDefinition/plannet-ParticipatingNetwork-extension"
                    ),
                    "valueReference": {
                        "reference": "https://directory.example.test/fhir/Organization/ne-network"
                    },
                }
            ],
        },
    )

    assert row["network_refs"] == ["https://directory.example.test/fhir/Organization/ne-network"]


def test_parse_fhir_resource_maps_formulary_identifier_to_plan_identifier():
    _model, row = importer.parse_fhir_resource(
        "source_nebraska",
        {
            "resourceType": "InsurancePlan",
            "id": "plan-nebraska-formulary",
            "identifier": [
                {
                    "type": {"coding": [{"code": "formulary_id"}]},
                    "value": "NEMW000001-UNDETERMINED-2026",
                }
            ],
        },
    )

    assert row["plan_identifier"] == "NEMW000001-UNDETERMINED-2026"


def test_address_corroboration_sql_links_overlay_roles():
    """Validate the corroboration view joins overlay NPI addresses to FHIR roles."""
    sql = importer.provider_directory_address_corroboration_sql("mrf")

    assert 'CREATE OR REPLACE VIEW "mrf"."provider_directory_address_corroboration" AS' in sql
    assert 'FROM "mrf"."provider_directory_address_overlay" overlay' in sql
    assert 'FROM "mrf"."entity_address_unified" e' not in sql
    assert "WITH address_candidates AS" in sql
    assert "overlay.resource_type::varchar AS resource_type" in sql
    assert "overlay.resource_type IN ('PractitionerRole', 'OrganizationAffiliation')" in sql
    assert "AND overlay.resource_type IN ('PractitionerRole', 'OrganizationAffiliation')" in sql
    assert "split_part(overlay.source_record_id, ':', 5)" in sql
    assert 'JOIN "mrf"."provider_directory_practitioner" practitioner' in sql
    assert "practitioner.resource_id = NULLIF(regexp_replace(COALESCE(role.practitioner_ref, ''), '^.*/', ''), '')" in sql
    assert "COALESCE(practitioner.npi, role.npi) = e.npi" in sql
    assert 'LEFT JOIN "mrf"."provider_directory_location" loc' in sql
    assert "loc.resource_id = e.location_resource_id" in sql
    assert "COALESCE(role.network_refs::jsonb, '[]'::jsonb) AS provider_directory_network_refs" in sql
    assert "COALESCE(affiliation.network_refs::jsonb, '[]'::jsonb) AS provider_directory_network_refs" in sql
    assert "insurance_plan.resource_id = CASE WHEN BTRIM(plan_ref.value)" in sql
    assert "(?:^|/)InsurancePlan/" in sql
    assert "|| insurance_plan.resource_id" not in sql
    assert "JOIN LATERAL (\n              SELECT DISTINCT normalized_ref AS resource_id" in sql
    assert "organization.resource_id = organization_ref.resource_id" in sql
    assert "organization_address_matches AS" not in sql
    assert "'organization_address'::varchar AS provider_directory_match_type" not in sql
    assert 'LEFT JOIN "mrf"."provider_directory_network_catalog" network_catalog' in sql
    assert "network_catalog.network_resource_id = CASE WHEN BTRIM(network_ref.value)" in sql
    assert "(?:^|/)Organization/" in sql
    assert "(?:/_history/" in sql
    assert "overlay.address_key::uuid AS address_key" in sql
    assert "jsonb_build_object(" in sql
    assert "'matched_on'," in sql
    assert "'npi_address_key_role_location'" in sql
    assert "'npi_address_key_role_location_plan'" in sql


def test_provider_directory_address_corroboration_sql_projects_network_plan_evidence():
    """Validate provider-directory evidence fields survive the corroboration view."""
    sql = importer.provider_directory_address_corroboration_sql("mrf")

    assert "provider_directory_plan_context_matched" in sql
    assert "provider_directory_network_context_present" in sql
    assert "provider_directory_network_names" in sql
    assert "provider_directory_network_matches" in sql
    assert "'provider_directory_source', 'provider_directory_fhir'" in sql
    assert "'provider_directory_source_id', role.source_id" in sql
    assert "'provider_directory_source_id', affiliation.source_id" in sql
    assert "'provider_directory_network_key'," in sql
    assert "network_catalog.provider_directory_network_key" in sql
    assert "'provider_directory_org_name', network_catalog.source_org_name" in sql
    assert "'provider_directory_plan_name', network_catalog.source_plan_name" in sql
    assert "'provider_directory_issuer_key'," in sql
    assert "'provider_directory_issuer_network_match_key'," in sql
    assert "network_catalog.provider_directory_issuer_network_match_key" in sql
    assert "'org_name', provider_directory_org_name" in sql
    assert "'plan_name', provider_directory_plan_name" in sql
    assert "provider_directory_insurance_plan_matches" in sql
    assert "COALESCE(loc.phone_number, e.phone_number) AS provider_directory_phone_number" in sql
    assert "COALESCE(loc.fax_number_digits, e.fax_number_digits) AS provider_directory_fax_number_digits" in sql
    assert "insurance_plan.plan_identifier" in sql
    assert "insurance_plan.network_refs::jsonb" in sql
    assert "network_org" not in sql
    assert "NULL::varchar AS source_key" in sql
    assert "NULL::varchar AS snapshot_id" in sql
    assert "NULL::varchar AS plan_id" in sql
    assert "NULL::varchar AS ptg_plan_id" in sql
    assert "'payer_directory_corroborated_location'" in sql
    assert "'provider_directory_address'" in sql
    assert "'npi_address_key_organization_address'" not in sql
    assert "UNION ALL" in sql
    assert 'provider_directory_organization_affiliation' in sql


def test_provider_directory_address_corroboration_view_keeps_existing_column_order():
    sql = importer.provider_directory_address_corroboration_sql("mrf")

    assert "SELECT DISTINCT\n        *," not in sql
    assert (
        "provider_directory_network_refs,\n"
        "        provider_directory_insurance_plan_refs,\n"
        "        provider_directory_specialty_codes,\n"
        "        provider_directory_role_codes,\n"
        "        provider_directory_match_type"
    ) in sql
    assert (
        ") AS address_verification_evidence,\n"
        "        provider_directory_plan_context_matched,\n"
        "        provider_directory_network_context_present,\n"
        "        provider_directory_insurance_plan_matches,\n"
        "        provider_directory_network_names,\n"
        "        provider_directory_network_matches"
    ) in sql


def test_provider_directory_address_corroboration_select_sql_returns_query_body():
    sql = importer.provider_directory_address_corroboration_select_sql("mrf")

    assert not sql.startswith("CREATE OR REPLACE VIEW")
    assert sql.startswith("WITH address_candidates AS")
    assert 'FROM "mrf"."provider_directory_address_overlay" overlay' in sql
    assert 'FROM "mrf"."entity_address_unified" e' not in sql
    assert "provider_directory_network_names" in sql


def _artifact_publish_scalar(row_count: int, statements: list[str] | None = None):
    async def fake_scalar(sql, **params):
        sql_text = str(sql)
        if statements is not None:
            statements.append(sql_text)
        if "pg_try_advisory_xact_lock" in sql_text:
            return True
        if "cls.relpersistence" in sql_text:
            return "p"
        if "cls.relkind" in sql_text:
            relation_name = params["relation_name"]
            return None if relation_name.endswith("_old") else "r"
        return row_count

    return fake_scalar


def _stub_artifact_build_guard(monkeypatch):
    @contextlib.asynccontextmanager
    async def unlocked_guard(*_args, **_kwargs):
        yield

    monkeypatch.setattr(importer, "_provider_directory_artifact_build_guard", unlocked_guard)


def _artifact_stage_table(status_calls: list[tuple[str, dict[str, Any]]], prefix: str) -> str:
    """Return the unique stage identifier recorded by a publisher test."""

    marker = f'CREATE UNLOGGED TABLE "mrf"."{prefix}'
    create_sql = next(sql for sql, _params in status_calls if marker in sql)
    return create_sql.split('"')[3]


class _ArtifactPublishTransaction:
    """Minimal transaction context for staged publisher unit tests."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _stub_staged_publish_db(
    monkeypatch,
    *,
    row_count: int,
    status_responses: dict[str, Any],
) -> list[tuple[str, dict[str, Any]]]:
    """Install SQL-recording DB doubles for one staged publication."""

    status_calls: list[tuple[str, dict[str, Any]]] = []

    async def fake_status(sql, **params):
        sql_text = str(sql)
        status_calls.append((sql_text, params))
        return next((value for marker, value in status_responses.items() if marker in sql_text), "OK")

    monkeypatch.setattr(importer.db, "status", fake_status)
    monkeypatch.setattr(importer.db, "scalar", _artifact_publish_scalar(row_count))
    monkeypatch.setattr(importer.db, "all", AsyncMock(return_value=[]))
    monkeypatch.setattr(importer.db, "transaction", lambda: _ArtifactPublishTransaction())
    return status_calls


@pytest.mark.asyncio
async def test_publish_provider_directory_address_corroboration_table_swaps_indexed_table(monkeypatch):
    statements: list[str] = []
    _stub_artifact_build_guard(monkeypatch)

    async def fake_status(sql, **_params):
        statements.append(sql)
        return 0

    monkeypatch.setattr(importer.db, "status", fake_status)
    monkeypatch.setattr(importer.db, "scalar", _artifact_publish_scalar(7, statements))
    monkeypatch.setattr(importer, "_stage_table_name", lambda: "pd_stage_corrob")
    publish_catalog = AsyncMock(return_value={"published": True, "rows": 3})
    monkeypatch.setattr(importer, "publish_provider_directory_network_catalog", publish_catalog)

    operation_result = await importer.publish_provider_directory_address_corroboration_table("mrf")
    joined = "\n".join(statements)

    assert operation_result == {
        "published": True,
        "relation": '"mrf"."provider_directory_address_corroboration"',
        "rows": 7,
        "storage": "table",
        "network_catalog": {"published": True, "rows": 3},
    }
    publish_catalog.assert_awaited_once_with("mrf")
    assert 'DROP TABLE IF EXISTS "mrf"."pd_stage_corrob"' in joined
    assert 'CREATE UNLOGGED TABLE "mrf"."pd_stage_corrob" AS' in joined
    assert 'FROM "mrf"."provider_directory_address_overlay" overlay' in joined
    assert 'FROM "mrf"."entity_address_unified" e' not in joined
    assert 'DROP VIEW "mrf"."provider_directory_address_corroboration"' not in joined
    assert 'DROP TABLE "mrf"."provider_directory_address_corroboration";' not in joined
    assert 'ALTER TABLE "mrf"."pd_stage_corrob" SET LOGGED' in joined
    assert 'ACCESS EXCLUSIVE MODE NOWAIT' in joined
    assert (
        'ALTER TABLE "mrf"."provider_directory_address_corroboration" '
        'RENAME TO "provider_directory_address_corroboration_old"'
    ) in joined
    assert (
        'ALTER TABLE "mrf"."pd_stage_corrob" RENAME TO "provider_directory_address_corroboration"'
        in joined
    )
    assert 'CREATE INDEX IF NOT EXISTS "pd_stage_corrob_pd_price_addr_corrob_lookup_idx"' in joined
    assert 'CREATE INDEX IF NOT EXISTS "pd_stage_corrob_pd_price_addr_corrob_network_names_gin"' in joined
    assert (
        'ALTER INDEX IF EXISTS "mrf"."pd_stage_corrob_pd_price_addr_corrob_lookup_idx" '
        'RENAME TO "pd_price_addr_corrob_lookup_idx"'
    ) in joined
    assert 'CREATE INDEX IF NOT EXISTS "mrf"."pd_price_addr_corrob_lookup_idx"' not in joined
    assert 'ANALYZE "mrf"."pd_stage_corrob"' in joined
    assert 'ANALYZE "mrf"."provider_directory_address_corroboration"' in joined


@pytest.mark.asyncio
async def test_scoped_corroboration_preserves_unselected_source_evidence(monkeypatch):
    statements: list[str] = []
    _stub_artifact_build_guard(monkeypatch)

    async def fake_status(sql, **_params):
        statements.append(sql)
        return 0

    monkeypatch.setattr(importer.db, "status", fake_status)
    monkeypatch.setattr(importer.db, "scalar", _artifact_publish_scalar(4, statements))
    monkeypatch.setattr(importer, "_stage_table_name", lambda: "pd_stage_scoped_corrob")
    publish_catalog = AsyncMock(return_value={"published": True, "rows": 3})
    monkeypatch.setattr(importer, "publish_provider_directory_network_catalog", publish_catalog)

    result = await importer.publish_provider_directory_address_corroboration_table(
        "mrf",
        source_ids=["source_current"],
    )
    joined = "\n".join(statements)

    assert result["copied_existing"] == 4
    assert result["source_ids"] == ["source_current"]
    publish_catalog.assert_awaited_once_with("mrf", source_ids=["source_current"])
    assert 'CREATE UNLOGGED TABLE "mrf"."pd_stage_scoped_corrob" AS' in joined
    assert 'FROM "mrf"."provider_directory_address_corroboration"' in joined
    assert "provider_directory_source_id = ANY" in joined
    assert 'INSERT INTO "mrf"."pd_stage_scoped_corrob"' in joined
    assert "WITH address_candidates AS" in joined


@pytest.mark.asyncio
async def test_ensure_provider_directory_network_catalog_populated_publishes_empty_catalog(monkeypatch):
    ensure_catalog = AsyncMock()
    publish_catalog = AsyncMock(return_value={"published": True, "rows": 3})
    monkeypatch.setattr(importer, "_ensure_provider_directory_network_catalog_table", ensure_catalog)
    monkeypatch.setattr(importer, "_network_catalog_missing_requirement", AsyncMock(return_value=None))
    monkeypatch.setattr(importer, "_has_provider_directory_network_catalog_rows", AsyncMock(return_value=False))
    monkeypatch.setattr(importer, "publish_provider_directory_network_catalog", publish_catalog)

    result = await importer._ensure_provider_directory_network_catalog_populated("mrf")

    assert result == {"published": True, "rows": 3}
    ensure_catalog.assert_awaited_once_with("mrf")
    publish_catalog.assert_awaited_once_with("mrf")


@pytest.mark.asyncio
async def test_ensure_provider_directory_network_catalog_populated_skips_existing_catalog(monkeypatch):
    ensure_catalog = AsyncMock()
    publish_catalog = AsyncMock()
    monkeypatch.setattr(importer, "_ensure_provider_directory_network_catalog_table", ensure_catalog)
    monkeypatch.setattr(importer, "_network_catalog_missing_requirement", AsyncMock(return_value=None))
    monkeypatch.setattr(importer, "_has_provider_directory_network_catalog_rows", AsyncMock(return_value=True))
    monkeypatch.setattr(importer, "publish_provider_directory_network_catalog", publish_catalog)

    result = await importer._ensure_provider_directory_network_catalog_populated("mrf")

    assert result == {
        "published": False,
        "reason": "already_populated",
        "relation": '"mrf"."provider_directory_network_catalog"',
    }
    ensure_catalog.assert_awaited_once_with("mrf")
    publish_catalog.assert_not_awaited()


@pytest.mark.asyncio
async def test_network_catalog_scope_sources_resolves_run_owned_evidence(
    monkeypatch,
):
    all_rows = AsyncMock(
        return_value=[("source-a",), ("source-a",), ("source-b",)]
    )
    monkeypatch.setattr(importer.db, "all", all_rows)

    assert await importer._network_catalog_scope_sources(
        "mrf",
        run_id="run-a",
        source_ids=None,
    ) == ["source-a", "source-b"]

    sql = all_rows.await_args.args[0]
    assert 'FROM "mrf"."provider_directory_insurance_plan"' in sql
    assert 'FROM "mrf"."provider_directory_practitioner_role"' in sql
    assert 'FROM "mrf"."provider_directory_organization_affiliation"' in sql
    assert all_rows.await_args.kwargs == {"run_id": "run-a"}


def test_provider_directory_location_address_key_sql_uses_shared_canonical_functions():
    sql = importer.provider_directory_location_address_key_sql("mrf")

    assert 'UPDATE "mrf"."provider_directory_location" AS loc' in sql
    assert '"mrf".addr_key_v1(' in sql
    assert '"mrf".addr_zip5_norm_v1(loc_src.postal_code)' in sql
    assert '"mrf".addr_state_code_v1(resolved_state)' in sql
    assert '"mrf".addr_city_norm_v1(resolved_city)' in sql
    assert "keyed.computed_address_key IS NOT NULL" in sql
    assert "address_key = keyed.computed_address_key::text" in sql


def test_reference_resource_key_handles_relative_and_absolute_fhir_refs():
    assert importer._reference_resource_key("Location/loc-1", "Location") == (
        "Location",
        "loc-1",
    )
    assert importer._reference_resource_key(
        "https://example.test/fhir/Practitioner/prac%7C1",
        "Practitioner",
    ) == ("Practitioner", "prac|1")
    assert importer._reference_resource_key("Organization/org-1", "Location") is None


def test_sql_ref_matches_resource_accepts_absolute_and_versioned_references():
    sql = importer._sql_ref_matches_resource("refs.ref", "Organization", "org.resource_id")

    assert "substring(BTRIM(refs.ref) FROM" in sql
    assert "(?:^|/)Organization/" in sql
    assert "(?:/_history/" in sql
    assert "(?:[?#].*)?$" in sql
    assert "= org.resource_id" in sql


def test_linked_resource_candidate_urls_use_network_endpoint_for_network_refs():
    urls = importer._linked_resource_candidate_urls(
        {
            "api_base": "https://example.test/fhir",
            "endpoint_organization": "Organization",
            "endpoint_network": "Organization?type=ntwk",
        },
        "Organization",
        "network-1",
        reference="Organization/network-1",
        reference_field="network_refs",
    )

    assert urls[:2] == [
        "https://example.test/fhir/Organization?type=ntwk&_count=1&_id=network-1",
        "https://example.test/fhir/Organization/network-1",
    ]


def test_linked_resource_candidate_urls_use_organization_endpoint_for_owner_refs():
    urls = importer._linked_resource_candidate_urls(
        {
            "api_base": "https://example.test/fhir",
            "endpoint_organization": "Organization?type=prov",
            "endpoint_network": "Organization?type=ntwk",
        },
        "Organization",
        "org-1",
        reference="Organization/org-1",
        reference_field="owned_by_ref",
    )

    assert urls[:2] == [
        "https://example.test/fhir/Organization?type=prov&_count=1&_id=org-1",
        "https://example.test/fhir/Organization/org-1",
    ]


def test_linked_resource_candidate_urls_use_endpoint_endpoint_for_endpoint_refs():
    urls = importer._linked_resource_candidate_urls(
        {
            "api_base": "https://example.test/fhir",
            "endpoint_endpoint": "Endpoint?connection-type=hl7-fhir-rest",
        },
        "Endpoint",
        "endpoint-1",
        reference="Endpoint/endpoint-1",
        reference_field="endpoint_refs",
    )

    assert urls[:2] == [
        "https://example.test/fhir/Endpoint?connection-type=hl7-fhir-rest&_count=1&_id=endpoint-1",
        "https://example.test/fhir/Endpoint/endpoint-1",
    ]


def test_group_resource_import_sources_collapses_shared_base_with_linked_limit():
    source_rows = [
        {
            "source_id": "source_a",
            "api_base": "https://shared.example/fhir",
            "canonical_api_base": "https://shared.example/fhir",
        },
        {
            "source_id": "source_b",
            "api_base": "https://shared.example/fhir",
            "canonical_api_base": "https://shared.example/fhir",
        },
        {
            "source_id": "source_c",
            "api_base": "https://shared.example/fhir",
            "canonical_api_base": "https://shared.example/fhir",
            "endpoint_location": "Location?address-state=CA",
        },
    ]

    groups = importer._group_resource_import_sources(
        source_rows,
        linked_resource_limit=50000,
    )

    assert [[source["source_id"] for source in group] for group in groups] == [
        ["source_a", "source_b"],
        ["source_c"],
    ]


def _shared_alias_sources(
    api_base: str = "https://shared.example/fhir",
) -> list[dict[str, Any]]:
    return [
        {
            "source_id": f"source_{alias_index:02d}",
            "org_name": f"Alias {alias_index:02d}",
            "api_base": api_base,
            "canonical_api_base": api_base,
        }
        for alias_index in reversed(range(31))
    ]


class _AliasImportCapture:
    def __init__(self):
        self.rows_by_model: dict[type, list[dict[str, Any]]] = {}
        self.seen_source_ids: list[str] = []
        self.stale_source_ids: list[str] = []
        self.metadata_source_ids: list[str] = []
        self.fetch_sources: list[dict[str, Any]] = []
        self.finalized_diagnostics: list[dict[str, dict[str, Any]]] = []

    async def upsert(self, model, resource_rows, **_kwargs):
        self.rows_by_model.setdefault(model, []).extend(resource_rows)
        return len(resource_rows)

    async def fetch(self, source_record, resource_type, **_kwargs):
        self.fetch_sources.append(source_record)
        assert resource_type == "Practitioner"
        resource_rows = [
            {"source_id": source_record["source_id"], "resource_id": "prac-1"}
        ]
        return importer.ResourceFetchResult(
            ProviderDirectoryPractitioner,
            resource_rows,
            1,
            0,
            1,
            True,
            False,
            False,
            False,
            False,
        )

    async def mark_seen(self, _model, resource_rows, _run_id, **_kwargs):
        self.seen_source_ids.extend(row["source_id"] for row in resource_rows)
        return len(resource_rows)

    async def delete_stale(self, _model, source_id, _run_id, **_kwargs):
        self.stale_source_ids.append(source_id)
        return 0

    async def update_metadata(self, _sql, **query_params):
        self.metadata_source_ids.append(query_params["source_id"])
        return 1

    async def prepare_candidate(self, source_records, resource_types, **options):
        checkpoint_context = options["checkpoint_context"]
        return importer.EndpointDatasetCandidate(
            endpoint_id=source_records[0]["endpoint_id"],
            dataset_id="dataset_1",
            acquisition_root_run_id=(
                checkpoint_context.acquisition_root_run_id
                if checkpoint_context is not None
                else options["run_id"]
            ),
            source_ids=tuple(sorted(row["source_id"] for row in source_records)),
            selected_resources=tuple(resource_types),
            import_run_id=options["run_id"],
            previous_dataset_id=None,
        )

    async def finalize_candidate(self, _candidate, diagnostics):
        self.finalized_diagnostics.append(diagnostics)
        return {"dataset_id": "dataset_1", "published": True}

    def install(self, monkeypatch):
        monkeypatch.setattr(importer, "_upsert_rows", self.upsert)
        monkeypatch.setattr(importer, "_fetch_resource_rows", self.fetch)
        monkeypatch.setattr(importer, "_mark_resource_rows_seen", self.mark_seen)
        monkeypatch.setattr(importer, "_delete_stale_resource_rows", self.delete_stale)
        monkeypatch.setattr(importer, "_prepare_endpoint_dataset_candidate", self.prepare_candidate)
        monkeypatch.setattr(importer, "_finalize_endpoint_dataset_candidate", self.finalize_candidate)
        monkeypatch.setattr(importer, "_is_seen_stage_enabled", lambda: False)
        monkeypatch.setattr(importer.db, "status", self.update_metadata)


@pytest.mark.asyncio
async def test_alias_group_writes_one_compatibility_copy(monkeypatch):
    capture = _AliasImportCapture()
    capture.install(monkeypatch)
    source_rows = _shared_alias_sources()
    endpoint_count = await importer._upsert_provider_directory_source_rows(source_rows)
    fetch_stats_by_resource: dict[str, dict[str, Any]] = {}
    completed_source_ids_by_resource: dict[str, set[str]] = {}

    resource_counts = await importer._import_resources(
        source_rows,
        resources=["Practitioner"],
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_retry_2",
        retry_of_run_id="run_retry_1",
        pagination_root_run_id="run_root",
        stale_cleanup=True,
        resource_fetch_stats=fetch_stats_by_resource,
        resource_completion=completed_source_ids_by_resource,
    )

    assert endpoint_count == 1
    assert resource_counts == {"Practitioner": 1}
    assert len(capture.rows_by_model[ProviderDirectorySource]) == 31
    assert len({source_record["endpoint_id"] for source_record in source_rows}) == 1
    assert len(capture.rows_by_model[ProviderDirectoryDatasetResource]) == 1
    assert len(capture.rows_by_model[ProviderDirectoryCanonicalResource]) == 1
    assert capture.rows_by_model[ProviderDirectoryPractitioner][0]["source_id"] == "source_00"
    assert capture.rows_by_model[ProviderDirectorySourceResource][0]["source_id"] == "source_00"
    assert capture.seen_source_ids == ["source_00"]
    assert capture.stale_source_ids == ["source_00"]
    assert sorted(capture.metadata_source_ids) == sorted(
        source_record["source_id"] for source_record in source_rows
    )
    assert fetch_stats_by_resource["Practitioner"]["rows_fetched"] == 1
    assert fetch_stats_by_resource["Practitioner"]["sources_attempted"] == 1
    assert completed_source_ids_by_resource["Practitioner"] == {
        source_record["source_id"] for source_record in source_rows
    }
    assert capture.finalized_diagnostics[0]["Practitioner"]["rows_written"] == 1
    assert capture.fetch_sources[0]["_partition_checkpoint_owner_run_id"] == "run_root"


def _artifact_dataset(
    source_id: str = "source_a",
    **dataset_overrides: Any,
) -> importer.ProviderDirectoryArtifactDataset:
    dataset_field_map = {
        "endpoint_id": "endpoint_1",
        "dataset_id": "dataset_current",
        "evidence_run_id": "acquisition_root",
        "selected_resources": (),
        "recorded_expected_resources": None,
        "status": importer.ENDPOINT_DATASET_PUBLISHED,
        "is_current": True,
        "previous_dataset_id": None,
        "expected_incumbent_dataset_id": None,
        "promote_on_cutover": False,
        "dataset_hash": None,
        "resource_count": None,
        "validated_at": None,
        "publication_metadata_hash": None,
    }
    expected_resources = dataset_overrides.pop("expected_resources", None)
    dataset_field_map.update(dataset_overrides)
    return importer.ProviderDirectoryArtifactDataset(
        source_id=source_id,
        expected_resources=(
            dataset_field_map["selected_resources"]
            if expected_resources is None
            else expected_resources
        ),
        **dataset_field_map,
    )


def _artifact_source_record(
    source_id: str,
    resources: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "metadata_json": {
            "provider_directory_supported_resources": list(resources),
            "provider_directory_fully_enumerable_resources": list(resources),
        },
    }


@contextlib.asynccontextmanager
async def _recording_transaction(events: list[str]):
    events.append("transaction-enter")
    try:
        yield
    finally:
        events.append("transaction-exit")


def _stub_artifact_scope_lifecycle(monkeypatch) -> None:
    @contextlib.asynccontextmanager
    async def scope_guard(_schema):
        yield

    scope_tables = [
        f"{ProviderDirectorySource.__tablename__}_scope",
        *[f"{model.__tablename__}_scope" for model in importer.RESOURCE_MODELS],
    ]
    monkeypatch.setattr(importer, "_provider_directory_artifact_scope_guard", scope_guard)
    monkeypatch.setattr(
        importer,
        "_reap_provider_directory_artifact_scope_tables",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_artifact_scope_projection",
        AsyncMock(
            return_value={
                "dataset_rows": 1,
                "projected_rows": 1,
                "resource_types": ["Location"],
                "alias_amplification": 1.0,
                "projected_rows_by_resource": {"Location": 1},
            }
        ),
    )
    monkeypatch.setattr(
        importer,
        "_materialize_artifact_scope_tables",
        AsyncMock(return_value=({}, scope_tables)),
    )


@pytest.mark.asyncio
async def test_artifact_dataset_selection_is_current_published_isolates_failed_sources(
    monkeypatch,
):
    lookup = AsyncMock(
        return_value=[
            {
                "source_id": "source_a",
                "endpoint_id": "endpoint_1",
                "source_record_json": {
                    "source_id": "source_a",
                    "metadata_json": {
                        "provider_directory_supported_resources": ["Location"],
                        "provider_directory_fully_enumerable_resources": ["Location"],
                    },
                },
                "dataset_id": "dataset_current",
                "evidence_run_id": "acquisition_root",
                "selected_resources": ["Location"],
            },
            {
                "source_id": "source_b",
                "endpoint_id": "endpoint_1",
                "source_record_json": {
                    "source_id": "source_b",
                    "metadata_json": {
                        "provider_directory_supported_resources": ["Location"],
                        "provider_directory_fully_enumerable_resources": ["Location"],
                    },
                },
                "dataset_id": "dataset_current",
                "evidence_run_id": "acquisition_root",
                "selected_resources": ["Location"],
            },
        ]
    )
    monkeypatch.setattr(importer.db, "all", lookup)

    fence = await importer._resolve_provider_directory_artifact_datasets(
        ["source_b", "source_a"]
    )

    sql = lookup.await_args.args[0]
    assert "dataset.is_current = true" in sql
    assert "dataset.status = :published_status" in sql
    assert "dataset.status IN" not in sql
    assert [dataset.source_id for dataset in fence.datasets] == [
        "source_b",
        "source_a",
    ]
    assert fence.dataset_id_by_endpoint_id == {"endpoint_1": "dataset_current"}


@pytest.mark.asyncio
async def test_artifact_dataset_selection_scopes_explicit_and_filters_all_source_publish(
    monkeypatch,
):
    lookup = AsyncMock(
        return_value=[
            {
                "source_id": "source_a",
                "endpoint_id": "endpoint_1",
                "source_record_json": {
                    "source_id": "source_a",
                    "metadata_json": {
                        "provider_directory_supported_resources": ["Location"],
                        "provider_directory_fully_enumerable_resources": ["Location"],
                    },
                },
                "dataset_id": "dataset_current",
                "evidence_run_id": "acquisition_root",
                "selected_resources": ["Location"],
            },
            {
                "source_id": "source_b",
                "endpoint_id": "endpoint_1",
                "source_record_json": {
                    "source_id": "source_b",
                    "metadata_json": {
                        "provider_directory_supported_resources": ["Location"],
                        "provider_directory_fully_enumerable_resources": ["Location"],
                    },
                },
                "dataset_id": "dataset_current",
                "evidence_run_id": "acquisition_root",
                "selected_resources": ["Location"],
            },
        ]
    )
    monkeypatch.setattr(importer.db, "all", lookup)
    explicit_fence = await importer._resolve_provider_directory_artifact_datasets(
        ["source_a"]
    )
    all_fence = await importer._resolve_provider_directory_artifact_datasets(None)
    explicit_sql = lookup.await_args_list[0].args[0]
    all_sql = lookup.await_args_list[1].args[0]

    assert [dataset.source_id for dataset in explicit_fence.datasets] == [
        "source_a",
    ]
    assert [dataset.source_id for dataset in all_fence.datasets] == [
        "source_a",
        "source_b",
    ]
    assert "selected_sources AS MATERIALIZED" in explicit_sql
    assert "sibling.endpoint_id = requested.endpoint_id" not in explicit_sql
    assert "selected_endpoints AS MATERIALIZED" in all_sql
    assert "ranked_datasets AS MATERIALIZED" in all_sql
    assert "validated_candidate_count" in all_sql
    assert "dataset.superseded_at IS NULL" in all_sql
    assert "publication_metadata_json::jsonb -> 'source_ids'" in all_sql


@pytest.mark.asyncio
async def test_humana_explicit_artifact_selection_rejects_18_alias_expansion(
    monkeypatch,
):
    canonical_source_id = "pdfhir_00a3d35311756763d420b0d6"
    endpoint_id = "endpoint_humana"
    dataset_rows = [
        _artifact_selection_row(
            canonical_source_id if alias_index == 0 else f"pdfhir_humana_alias_{alias_index:02d}",
            endpoint_id,
            "dataset_humana",
        )
        for alias_index in range(18)
    ]
    lookup = AsyncMock(return_value=dataset_rows)
    monkeypatch.setattr(importer.db, "all", lookup)

    fence = await importer._resolve_provider_directory_artifact_datasets(
        [canonical_source_id]
    )

    assert fence.source_ids == [canonical_source_id]
    assert fence.endpoint_ids == [endpoint_id]
    assert "sibling.endpoint_id" not in lookup.await_args.args[0]


def _artifact_selection_row(
    source_id: str,
    endpoint_id: str,
    dataset_id: str,
    **selection_overrides: Any,
) -> dict[str, Any]:
    selection_field_map = {
        "status": importer.ENDPOINT_DATASET_PUBLISHED,
        "is_current": True,
        "previous_dataset_id": None,
        "current_dataset_id": dataset_id,
        "validated_candidate_count": 0,
        "promote_on_cutover": False,
    }
    selection_field_map.update(selection_overrides)
    metadata = {
        "source_ids": [source_id],
        "selected_resources": ["Location"],
        "expected_resources": ["Location"],
        "resource_diagnostics": {"Location": {"complete": True}},
    }
    return {
        "source_id": source_id,
        "endpoint_id": endpoint_id,
        "source_record_json": _artifact_source_record(
            source_id,
            ("Location",),
        ),
        "dataset_id": dataset_id,
        "evidence_run_id": f"root_{endpoint_id}",
        "selected_resources": ["Location"],
        "recorded_expected_resources": ["Location"],
        "status": selection_field_map["status"],
        "is_current": selection_field_map["is_current"],
        "previous_dataset_id": selection_field_map["previous_dataset_id"],
        "dataset_hash": "a" * 64,
        "resource_count": 1,
        "validated_at": "2026-07-13T10:00:00",
        "publication_metadata_json": metadata,
        "current_dataset_count": (
            1 if selection_field_map["current_dataset_id"] else 0
        ),
        "current_dataset_id": selection_field_map["current_dataset_id"],
        "validated_candidate_count": selection_field_map[
            "validated_candidate_count"
        ],
        "promote_on_cutover": selection_field_map["promote_on_cutover"],
    }


@pytest.mark.asyncio
async def test_full_artifact_selection_prefers_validated_and_falls_back_to_current(
    monkeypatch,
):
    lookup = AsyncMock(
        return_value=[
            _artifact_selection_row(
                "source_candidate",
                "endpoint_candidate",
                "dataset_candidate",
                status=importer.ENDPOINT_DATASET_VALIDATED,
                is_current=False,
                previous_dataset_id="dataset_incumbent",
                current_dataset_id="dataset_incumbent",
                validated_candidate_count=1,
                promote_on_cutover=True,
            ),
            _artifact_selection_row(
                "source_current",
                "endpoint_current",
                "dataset_current",
                status=importer.ENDPOINT_DATASET_PUBLISHED,
                is_current=True,
                previous_dataset_id="dataset_older",
                current_dataset_id="dataset_current",
                validated_candidate_count=0,
                promote_on_cutover=False,
            ),
        ]
    )
    monkeypatch.setattr(importer.db, "all", lookup)

    fence = await importer._resolve_provider_directory_artifact_datasets(
        None,
        should_select_validated_candidates=True,
    )

    assert fence.should_select_validated_candidates is True
    assert [dataset.dataset_id for dataset in fence.promotion_datasets] == [
        "dataset_candidate"
    ]
    assert fence.incumbent_dataset_ids == {
        "endpoint_candidate": "dataset_incumbent",
        "endpoint_current": "dataset_current",
    }
    assert (
        lookup.await_args.kwargs["select_validated_candidates"] is True
    )


@pytest.mark.asyncio
async def test_targeted_artifact_selection_remains_current_only(monkeypatch):
    lookup = AsyncMock(
        return_value=[
            _artifact_selection_row(
                "source_a",
                "endpoint_1",
                "dataset_current",
                status=importer.ENDPOINT_DATASET_PUBLISHED,
                is_current=True,
                previous_dataset_id="dataset_older",
                current_dataset_id="dataset_current",
                validated_candidate_count=1,
                promote_on_cutover=False,
            )
        ]
    )
    monkeypatch.setattr(importer.db, "all", lookup)

    fence = await importer._resolve_provider_directory_artifact_datasets(
        ["source_a"],
        should_select_validated_candidates=False,
    )

    assert fence.dataset_id_by_endpoint_id == {
        "endpoint_1": "dataset_current"
    }
    assert fence.promotion_datasets == []
    assert (
        lookup.await_args.kwargs["select_validated_candidates"] is False
    )


def test_full_artifact_selection_fails_closed_on_multiple_validated_candidates():
    ambiguous_row = _artifact_selection_row(
        "source_a",
        "endpoint_1",
        "dataset_candidate_a",
        status=importer.ENDPOINT_DATASET_VALIDATED,
        is_current=False,
        previous_dataset_id="dataset_current",
        current_dataset_id="dataset_current",
        validated_candidate_count=2,
        promote_on_cutover=True,
    )

    with pytest.raises(
        RuntimeError,
        match="provider_directory_artifact_validated_candidate_ambiguous",
    ):
        importer._validate_provider_directory_artifact_datasets(
            [ambiguous_row],
            ["source_a"],
            should_select_validated_candidates=True,
        )


def test_only_full_base_artifact_publication_can_promote_candidates():
    all_targets = set(importer.PROVIDER_DIRECTORY_PUBLISH_ARTIFACT_TARGETS)

    assert importer._should_select_validated_artifacts(None)
    assert importer._should_select_validated_artifacts(
        all_targets
    )
    assert not importer._should_select_validated_artifacts(
        {"corroboration"}
    )
    assert not importer._should_select_validated_artifacts(
        all_targets - {"profile"}
    )


def test_artifact_dataset_selection_rejects_missing_and_ambiguous_current():
    missing_dataset_row_map = {
        "source_id": "source_a",
        "endpoint_id": "endpoint_1",
        "dataset_id": None,
        "evidence_run_id": None,
    }
    with pytest.raises(RuntimeError, match="current_dataset_missing:source_a"):
        importer._validate_provider_directory_artifact_datasets(
            [missing_dataset_row_map],
            ["source_a"],
        )

    duplicate_rows = [
        {
            "source_id": "source_a",
            "endpoint_id": "endpoint_1",
            "source_record_json": {
                "source_id": "source_a",
                "metadata_json": {
                    "provider_directory_supported_resources": ["Location"],
                    "provider_directory_fully_enumerable_resources": ["Location"],
                },
            },
            "dataset_id": dataset_id,
            "evidence_run_id": "acquisition_root",
            "selected_resources": ["Location"],
        }
        for dataset_id in ("dataset_a", "dataset_b")
    ]
    with pytest.raises(RuntimeError, match="current_dataset_ambiguous:source_a"):
        importer._validate_provider_directory_artifact_datasets(
            duplicate_rows,
            ["source_a"],
        )


def test_artifact_dataset_legacy_metadata_uses_current_source_profile():
    dataset = importer._provider_directory_artifact_dataset_from_row(
        {
            "source_id": "source_a",
            "endpoint_id": "endpoint_1",
            "source_record_json": _artifact_source_record(
                "source_a",
                ("Location",),
            ),
            "dataset_id": "dataset_current",
            "evidence_run_id": "acquisition_root",
            "selected_resources": ["Location"],
            "recorded_expected_resources": None,
        }
    )

    assert dataset is not None
    assert dataset.expected_resources == ("Location",)
    assert dataset.recorded_expected_resources is None
    importer._assert_provider_directory_artifact_target_dependencies(
        importer.ProviderDirectoryArtifactDatasetFence((dataset,)),
        publish_artifacts_targets={"location_archive"},
        publish_corroboration=False,
    )


def test_artifact_dataset_recorded_profile_ignores_alias_profile_drift():
    changed_source_dataset = importer._provider_directory_artifact_dataset_from_row(
        {
            "source_id": "source_a",
            "endpoint_id": "endpoint_1",
            "source_record_json": _artifact_source_record(
                "source_a",
                ("Location", "Organization"),
            ),
            "dataset_id": "dataset_current",
            "evidence_run_id": "acquisition_root",
            "selected_resources": ["Location"],
            "recorded_expected_resources": ["Location"],
        }
    )
    assert changed_source_dataset is not None
    assert changed_source_dataset.expected_resources == ("Location",)
    importer._assert_provider_directory_artifact_target_dependencies(
        importer.ProviderDirectoryArtifactDatasetFence(
            (changed_source_dataset,)
        ),
        publish_artifacts_targets={"location_archive"},
        publish_corroboration=False,
    )


def test_artifact_dataset_rejects_internal_recorded_profile_drift():
    with pytest.raises(RuntimeError, match="dataset_profile:source_a"):
        importer._assert_provider_directory_artifact_target_dependencies(
            importer.ProviderDirectoryArtifactDatasetFence(
                (
                    _artifact_dataset(
                        selected_resources=("Location", "Organization"),
                        expected_resources=("Location",),
                        recorded_expected_resources=("Location",),
                    ),
                )
            ),
            publish_artifacts_targets={"location_archive"},
            publish_corroboration=False,
        )


def test_artifact_dataset_recorded_profile_does_not_expand_explicit_scope():
    dataset_rows = [
        {
            "source_id": source_id,
            "endpoint_id": "endpoint_1",
            "source_record_json": _artifact_source_record(source_id, resources),
            "dataset_id": "dataset_current",
            "evidence_run_id": "acquisition_root",
            "selected_resources": ["Location"],
            "recorded_expected_resources": ["Location"],
        }
        for source_id, resources in (
            ("source_a", ("Location",)),
            ("source_b", ("Endpoint", "Location")),
        )
    ]

    fence = importer._validate_provider_directory_artifact_datasets(
        dataset_rows,
        ["source_a"],
    )

    assert [dataset.source_id for dataset in fence.datasets] == ["source_a"]
    assert {
        dataset.expected_resources for dataset in fence.datasets
    } == {("Location",)}


def test_endpoint_dataset_expected_resources_rejects_alias_profile_drift():
    with pytest.raises(
        RuntimeError,
        match="endpoint_dataset_alias_profiles_ambiguous",
    ):
        importer._endpoint_dataset_expected_resources(
            [
                _artifact_source_record("source_a", ("Location",)),
                _artifact_source_record(
                    "source_b",
                    ("Location", "Organization"),
                ),
            ]
        )


def test_artifact_scope_sql_defers_bounded_primary_key_and_expands_aliases():
    create_sql = importer._provider_directory_artifact_scope_table_sql(
        ProviderDirectoryLocation,
        "mrf",
        "location_scope_test",
    )
    primary_key_sql = importer._artifact_scope_pk_sql(
        ProviderDirectoryLocation,
        "mrf",
        "location_scope_test",
    )
    insert_sql = importer._provider_directory_artifact_resource_insert_sql(
        ProviderDirectoryLocation,
        "mrf",
        "location_scope_test",
    )

    assert 'CREATE UNLOGGED TABLE "mrf"."location_scope_test"' in create_sql
    assert "resource_id VARCHAR(256) NOT NULL" in create_sql
    assert "latitude VARCHAR(64)" in create_sql
    assert "PRIMARY KEY" not in create_sql
    assert primary_key_sql == (
        'CREATE UNIQUE INDEX "location_scope_test_pk_build_idx" '
        'ON "mrf"."location_scope_test" ("source_id", "resource_id");',
        'ALTER TABLE "mrf"."location_scope_test" '
        'ADD CONSTRAINT "location_scope_test_pkey" PRIMARY KEY '
        'USING INDEX "location_scope_test_pk_build_idx";',
    )
    assert "FROM unnest(" in insert_sql
    assert "selected.source_id" in insert_sql
    assert "selected.evidence_run_id" in insert_sql
    assert 'JOIN "mrf"."provider_directory_dataset_resource" AS resource' in insert_sql
    assert "resource.resource_type = :resource_type" in insert_sql
    assert "provider_directory_location AS" not in insert_sql

    long_table_name = "provider_directory_practitioner_role_artifact_scope_0123456789abcdef"
    names = importer._artifact_scope_pk_names(
        long_table_name
    )
    assert names == importer._artifact_scope_pk_names(
        long_table_name
    )
    assert names[0] != names[1]
    assert all(len(name) <= importer.POSTGRES_IDENTIFIER_MAX_LENGTH for name in names)


def test_artifact_relation_scope_wires_address_and_network_builders():
    relation_token = importer._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.set(
        {
            "provider_directory_source": "source_scope",
            "provider_directory_location": "location_scope",
            "provider_directory_organization": "organization_scope",
            "provider_directory_insurance_plan": "plan_scope",
            "provider_directory_practitioner_role": "role_scope",
            "provider_directory_organization_affiliation": "affiliation_scope",
        }
    )
    try:
        archive_sql = importer.provider_directory_location_archive_stage_sql(
            "mrf",
            "archive_stage",
        )
        network_sql = importer.provider_directory_network_catalog_insert_sql(
            "mrf",
            "network_stage",
            source_ids=["source_a"],
        )
    finally:
        importer._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.reset(
            relation_token
        )

    assert 'FROM "mrf"."location_scope" AS loc' in archive_sql
    assert 'FROM "mrf"."organization_scope" AS organization' in archive_sql
    assert 'FROM "mrf"."plan_scope" AS insurance_plan' in network_sql
    assert 'FROM "mrf"."role_scope" AS role' in network_sql
    assert 'JOIN "mrf"."organization_scope" AS network_org' in network_sql
    assert 'JOIN "mrf"."source_scope" AS src' in network_sql


@pytest.mark.asyncio
async def test_artifact_dataset_scope_drops_private_tables_in_finally(monkeypatch):
    fence = importer.ProviderDirectoryArtifactDatasetFence((_artifact_dataset(),))
    monkeypatch.setattr(
        importer,
        "_resolve_provider_directory_artifact_datasets",
        AsyncMock(return_value=fence),
    )
    _stub_artifact_scope_lifecycle(monkeypatch)
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)

    with pytest.raises(RuntimeError, match="stop after materialization"):
        async with importer._provider_directory_artifact_dataset_scope(
            run_id="progress_run",
            source_ids=["source_a"],
        ):
            raise RuntimeError("stop after materialization")

    drop_sql_calls = [
        call.args[0]
        for call in status.await_args_list
        if call.args[0].startswith("DROP TABLE IF EXISTS")
    ]
    assert len(drop_sql_calls) == len(importer.RESOURCE_MODELS) + 1
    assert importer._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get() == {}
    assert importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.get() is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "materialization_failure",
    [RuntimeError("materialization failed"), asyncio.CancelledError()],
)
async def test_artifact_scope_materialization_cleans_partial_tables(
    monkeypatch,
    materialization_failure,
):
    fence = importer.ProviderDirectoryArtifactDatasetFence(
        (_artifact_dataset(selected_resources=("Location",)),)
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_artifact_scope_table_name",
        lambda table_name, _run_id: f"{table_name}_scope",
    )
    monkeypatch.setattr(
        importer,
        "_materialize_provider_directory_artifact_source_scope",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_materialize_provider_directory_artifact_resource_scope",
        AsyncMock(side_effect=materialization_failure),
    )
    cleanup = AsyncMock()
    monkeypatch.setattr(importer, "_drop_artifact_scope_tables", cleanup)

    with pytest.raises(type(materialization_failure)):
        await importer._materialize_artifact_scope_tables(
            "mrf",
            "run_1",
            fence,
            frozenset({"Location"}),
        )

    cleanup.assert_awaited_once_with(
        "mrf",
        [
            "provider_directory_source_scope",
            "provider_directory_insurance_plan_scope",
        ],
    )


@pytest.mark.asyncio
async def test_missing_current_dataset_fails_before_artifact_build(monkeypatch):
    monkeypatch.setattr(
        importer.db,
        "all",
        AsyncMock(
            return_value=[
                {
                    "source_id": "source_a",
                    "endpoint_id": "endpoint_1",
                    "dataset_id": None,
                    "evidence_run_id": None,
                }
            ]
        ),
    )
    artifact_publish = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_publish_provider_directory_artifacts",
        artifact_publish,
    )

    with pytest.raises(RuntimeError, match="current_dataset_missing:source_a"):
        await importer._publish_provider_directory_dataset_artifacts(
            run_id="artifact_progress_run",
            metrics={},
            source_ids=["source_a"],
            publish_corroboration=False,
            publish_artifacts_targets=None,
        )

    artifact_publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_after_acquisition_uses_dataset_artifact_path(monkeypatch):
    dataset_publish = AsyncMock(return_value={"path": "dataset"})
    compatibility_publish = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_publish_provider_directory_dataset_artifacts",
        dataset_publish,
    )
    monkeypatch.setattr(
        importer,
        "_publish_provider_directory_artifacts",
        compatibility_publish,
    )

    metrics = await importer._publish_selected_provider_directory_artifacts(
        run_id="progress_run",
        metrics={},
        source_ids=["source_a"],
        seen_table="seen_stage",
        publish_after_acquisition=True,
        publish_corroboration=False,
        publish_artifacts_targets={"network_catalog"},
    )

    assert metrics == {"path": "dataset"}
    dataset_publish.assert_awaited_once()
    compatibility_publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_artifact_dataset_fence_rejects_changed_current_dataset(monkeypatch):
    fence = importer.ProviderDirectoryArtifactDatasetFence((_artifact_dataset(),))
    monkeypatch.setattr(
        importer.db,
        "all",
        AsyncMock(
            return_value=[
                {"endpoint_id": "endpoint_1", "dataset_id": "dataset_new"}
            ]
        ),
    )

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="endpoint_dataset_current_changed",
    ):
        await importer._verify_provider_directory_artifact_dataset_fence(fence)


@pytest.mark.asyncio
async def test_artifact_cutover_locks_endpoint_and_rejects_alias_repoint(monkeypatch):
    fence = importer.ProviderDirectoryArtifactDatasetFence(
        (
            _artifact_dataset(
                selected_resources=("Location", "Organization"),
            ),
        )
    )
    lookup = AsyncMock(
        side_effect=[
            [{"endpoint_id": "endpoint_1"}],
            [
                {
                    "source_id": "source_a",
                    "endpoint_id": "endpoint_repointed",
                    "source_record_json": {
                        "source_id": "source_a",
                        "metadata_json": {
                            "provider_directory_supported_resources": [
                                "Location",
                                "Organization",
                            ],
                            "provider_directory_fully_enumerable_resources": [
                                "Location",
                                "Organization",
                            ],
                        },
                    },
                    "dataset_id": "dataset_current",
                    "evidence_run_id": "acquisition_root",
                    "selected_resources": ["Location", "Organization"],
                }
            ],
        ]
    )
    monkeypatch.setattr(importer.db, "all", lookup)
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="source_endpoint_dataset_changed",
    ):
        await importer._lock_and_verify_artifact_dataset_fence(
            fence
        )

    endpoint_lock_sql = lookup.await_args_list[0].args[0]
    alias_lock_sql = lookup.await_args_list[1].args[0]
    assert "FOR UPDATE" in endpoint_lock_sql
    assert "provider_directory_api_endpoint" in endpoint_lock_sql
    assert status.await_args.args[0].startswith("LOCK TABLE")
    assert "FOR SHARE OF source" in alias_lock_sql
    assert "source.source_id = ANY" in alias_lock_sql
    assert lookup.await_args_list[1].kwargs["source_ids"] == ["source_a"]
    assert "provider_directory_endpoint_dataset" not in alias_lock_sql


def test_artifact_target_dependencies_fail_before_an_unsafe_target_build():
    incomplete_fence = importer.ProviderDirectoryArtifactDatasetFence(
        (
            _artifact_dataset(
                selected_resources=("Location",),
                expected_resources=("InsurancePlan", "Location"),
            ),
        )
    )

    with pytest.raises(
        RuntimeError,
        match="target_dependencies_missing:dataset_profile:source_a",
    ):
        importer._assert_provider_directory_artifact_target_dependencies(
            incomplete_fence,
            publish_artifacts_targets={"network_catalog"},
            publish_corroboration=False,
        )

    with pytest.raises(
        RuntimeError,
        match="corroboration_prerequisites_missing:address_overlay,network_catalog",
    ):
        importer._assert_provider_directory_artifact_target_dependencies(
            importer.ProviderDirectoryArtifactDatasetFence(
                (
                    _artifact_dataset(
                        selected_resources=(
                            "InsurancePlan",
                            "Organization",
                            "Practitioner",
                            "Location",
                            "PractitionerRole",
                            "HealthcareService",
                            "OrganizationAffiliation",
                        ),
                    ),
                )
            ),
            publish_artifacts_targets={"corroboration"},
            publish_corroboration=True,
        )


@pytest.mark.parametrize(
    "selected_resources",
    (
        (
            "InsurancePlan",
            "Location",
            "Organization",
            "Practitioner",
            "PractitionerRole",
        ),
        (
            "Location",
            "Organization",
            "OrganizationAffiliation",
            "Practitioner",
            "PractitionerRole",
        ),
    ),
)
def test_artifact_targets_accept_complete_source_specific_profiles(
    selected_resources,
):
    fence = importer.ProviderDirectoryArtifactDatasetFence(
        (_artifact_dataset(selected_resources=selected_resources),)
    )

    importer._assert_provider_directory_artifact_target_dependencies(
        fence,
        publish_artifacts_targets=None,
        publish_corroboration=True,
    )


def test_artifact_resource_types_are_target_specific():
    assert importer._provider_directory_artifact_resource_types(
        {"location_contacts"},
        publish_corroboration=False,
    ) == frozenset({"Location"})
    assert importer._provider_directory_artifact_resource_types(
        {"network_catalog"},
        publish_corroboration=False,
    ) == frozenset(
        {
            "InsurancePlan",
            "Organization",
            "OrganizationAffiliation",
            "PractitionerRole",
        }
    )


@pytest.mark.asyncio
async def test_artifact_resource_scope_does_not_copy_unrequested_model(monkeypatch):
    fence = importer.ProviderDirectoryArtifactDatasetFence(
        (
            _artifact_dataset(
                selected_resources=("Location", "Practitioner"),
            ),
        )
    )
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)

    await importer._materialize_provider_directory_artifact_resource_scope(
        "mrf",
        "practitioner_scope",
        ProviderDirectoryPractitioner,
        fence,
        frozenset({"Location"}),
    )

    assert len(status.await_args_list) == 4
    assert status.await_args_list[0].args[0].startswith(
        'CREATE UNLOGGED TABLE "mrf"."practitioner_scope"'
    )
    assert status.await_args_list[1].args[0].startswith(
        'CREATE UNIQUE INDEX "practitioner_scope_pk_build_idx"'
    )
    assert status.await_args_list[2].args[0].startswith(
        'ALTER TABLE "mrf"."practitioner_scope"'
    )
    assert status.await_args_list[3].args[0] == (
        'ANALYZE "mrf"."practitioner_scope";'
    )


@pytest.mark.asyncio
async def test_artifact_source_scope_builds_primary_key_after_insert(monkeypatch):
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)

    await importer._materialize_provider_directory_artifact_source_scope(
        "mrf",
        "source_scope",
        ["source_a"],
    )

    statements = [call.args[0].strip() for call in status.await_args_list]
    assert len(statements) == 5
    assert statements[0].startswith('CREATE UNLOGGED TABLE "mrf"."source_scope"')
    assert statements[1].startswith('INSERT INTO "mrf"."source_scope"')
    assert statements[2].startswith(
        'CREATE UNIQUE INDEX "source_scope_pk_build_idx"'
    )
    assert statements[3].startswith('ALTER TABLE "mrf"."source_scope"')
    assert statements[4] == 'ANALYZE "mrf"."source_scope";'


@pytest.mark.asyncio
async def test_artifact_resource_scope_builds_primary_key_after_insert(monkeypatch):
    fence = importer.ProviderDirectoryArtifactDatasetFence(
        (_artifact_dataset(selected_resources=("Location",)),)
    )
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)

    await importer._materialize_provider_directory_artifact_resource_scope(
        "mrf",
        "location_scope",
        ProviderDirectoryLocation,
        fence,
        frozenset({"Location"}),
    )

    statements = [call.args[0].strip() for call in status.await_args_list]
    assert len(statements) == 5
    assert statements[0].startswith('CREATE UNLOGGED TABLE "mrf"."location_scope"')
    assert statements[1].startswith('INSERT INTO "mrf"."location_scope"')
    assert statements[2].startswith(
        'CREATE UNIQUE INDEX "location_scope_pk_build_idx"'
    )
    assert statements[3].startswith('ALTER TABLE "mrf"."location_scope"')
    assert statements[4] == 'ANALYZE "mrf"."location_scope";'


@pytest.mark.asyncio
async def test_dataset_artifact_bundle_promotes_after_complete_stage_build(monkeypatch):
    """Dataset publishing promotes only after every serving stage is prepared."""
    fence = importer.ProviderDirectoryArtifactDatasetFence(
        (_artifact_dataset(selected_resources=("Location",)),)
    )
    scope_options_by_name: dict[str, Any] = {}
    events: list[str] = []
    promote_bundle = AsyncMock(
        side_effect=lambda: events.append("promote") or 1
    )

    @contextlib.asynccontextmanager
    async def dataset_scope(**kwargs):
        scope_options_by_name.update(kwargs)
        yield fence

    async def publish_artifacts(**kwargs):
        assert isinstance(
            importer._PROVIDER_DIRECTORY_ARTIFACT_BUNDLE.get(),
            importer.ProviderDirectoryArtifactBundle,
        )
        events.append("build")
        return kwargs["metrics"]

    monkeypatch.setattr(
        importer,
        "_resolve_provider_directory_artifact_datasets",
        AsyncMock(return_value=fence),
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_artifact_dataset_scope",
        dataset_scope,
    )
    monkeypatch.setattr(
        importer.ProviderDirectoryArtifactBundle,
        "promote",
        promote_bundle,
    )
    monkeypatch.setattr(
        importer,
        "_publish_provider_directory_artifacts",
        publish_artifacts,
    )

    metrics = await importer._publish_provider_directory_dataset_artifacts(
        run_id="run_1",
        metrics={},
        source_ids=["source_a"],
        publish_corroboration=False,
        publish_artifacts_targets={"location_archive"},
    )

    assert metrics["artifact_dataset_ids"] == ["dataset_current"]
    assert events == ["build", "promote"]
    assert importer._PROVIDER_DIRECTORY_ARTIFACT_BUNDLE.get() is None
    assert scope_options_by_name["resource_types"] == frozenset(
        {"Location", "Organization", "Practitioner"}
    )


def _validated_artifact_candidate_fence():
    metadata_hash = importer._identity_hash(
        {
            "selected_resources": ["Location"],
            "expected_resources": ["Location"],
        }
    )
    candidate = _artifact_dataset(
        dataset_id="dataset_candidate",
        status=importer.ENDPOINT_DATASET_VALIDATED,
        is_current=False,
        previous_dataset_id="dataset_current",
        expected_incumbent_dataset_id="dataset_current",
        promote_on_cutover=True,
        dataset_hash="a" * 64,
        resource_count=1,
        validated_at="2026-07-13T10:00:00",
        publication_metadata_hash=metadata_hash,
        selected_resources=("Location",),
    )
    return importer.ProviderDirectoryArtifactDatasetFence(
        (candidate,),
        should_select_validated_candidates=True,
    )


def _complete_candidate_artifact_metrics():
    return {
        importer.PROVIDER_DIRECTORY_DATASET_NETWORK_PLAN_METADATA_KEY: {
            "published": True
        },
        importer.PROVIDER_DIRECTORY_DATASET_AFFILIATION_ORGANIZATION_METADATA_KEY: {
            "published": True
        },
        "location_contacts_backfilled": {"rows": 0},
        "location_coordinates_backfilled": 0,
        "resource_id_npis_backfilled": 0,
        "location_address_keys_stamped": 0,
        "location_archive": {"inserted": 0},
        "address_overlay": {"published": True},
        "profile": {"profile_rows": 0},
        "network_catalog": {"published": True},
    }


def _complete_candidate_artifact_bundle():
    keep_indexes = AsyncMock()
    return importer.ProviderDirectoryArtifactBundle(
        stages=[
            importer.ProviderDirectoryPreparedArtifactStage(
                "mrf",
                f"stage_{relation_name}",
                relation_name,
                keep_indexes,
            )
            for relation_name in (
                importer.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE,
                importer.profile_artifact.PROFILE_EVIDENCE_TABLE,
                importer.profile_artifact.PROFILE_TABLE,
                importer.PROVIDER_DIRECTORY_NETWORK_CATALOG_TABLE,
            )
        ]
    )


def test_candidate_artifact_bundle_fails_closed_on_skipped_required_target():
    metrics = _complete_candidate_artifact_metrics()
    metrics["network_catalog"] = {
        "skipped": True,
        "reason": "provider_directory_insurance_plan_unavailable",
    }

    with pytest.raises(
        RuntimeError,
        match="provider_directory_candidate_artifact_skipped:network_catalog",
    ):
        importer._assert_candidate_artifact_bundle_complete(
            _validated_artifact_candidate_fence(),
            metrics,
            _complete_candidate_artifact_bundle(),
            publish_corroboration=False,
            publish_artifacts_targets=None,
        )


def test_candidate_artifact_bundle_requires_every_deferred_relation():
    artifact_bundle = _complete_candidate_artifact_bundle()
    artifact_bundle.stages = [
        stage
        for stage in artifact_bundle.stages
        if stage.target_relation
        != importer.PROVIDER_DIRECTORY_NETWORK_CATALOG_TABLE
    ]

    with pytest.raises(
        RuntimeError,
        match="provider_directory_candidate_artifact_bundle_incomplete",
    ):
        importer._assert_candidate_artifact_bundle_complete(
            _validated_artifact_candidate_fence(),
            _complete_candidate_artifact_metrics(),
            artifact_bundle,
            publish_corroboration=False,
            publish_artifacts_targets=None,
        )


def _install_confirmed_promotion_harness(monkeypatch, fence, events):
    """Install deterministic build, promotion, and cleanup collaborators."""
    resolve = AsyncMock(return_value=fence)
    @contextlib.asynccontextmanager
    async def dataset_scope(**_kwargs):
        yield fence

    async def publish_artifacts(**kwargs):
        events.append("build")
        return kwargs["metrics"]

    async def promote_bundle(_bundle):
        events.append("promote")
        return 1

    async def clear_retry_state(_fence):
        events.append("clear")
        return ["dataset_candidate"]
    monkeypatch.setattr(
        importer,
        "_resolve_provider_directory_artifact_datasets",
        resolve,
    )
    monkeypatch.setattr(
        importer,
        "_assert_provider_directory_artifact_target_dependencies",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        importer,
        "_publish_current_dataset_relation_artifacts",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_artifact_dataset_scope",
        dataset_scope,
    )
    monkeypatch.setattr(
        importer,
        "_publish_provider_directory_artifacts",
        publish_artifacts,
    )
    monkeypatch.setattr(
        importer,
        "_assert_candidate_artifact_bundle_complete",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        importer.ProviderDirectoryArtifactBundle,
        "promote",
        promote_bundle,
    )
    monkeypatch.setattr(
        importer,
        "_clear_promoted_endpoint_dataset_retry_state",
        clear_retry_state,
    )
    return resolve


@pytest.mark.asyncio
async def test_full_bundle_clears_retry_state_only_after_confirmed_promotion(
    monkeypatch,
):
    fence = _validated_artifact_candidate_fence()
    events: list[str] = []
    resolve = _install_confirmed_promotion_harness(
        monkeypatch,
        fence,
        events,
    )

    metrics = await importer._publish_provider_directory_dataset_artifacts(
        run_id="publish-run",
        metrics={},
        source_ids=["source_a"],
        publish_corroboration=False,
        publish_artifacts_targets=None,
    )

    assert events == ["build", "promote", "clear"]
    assert metrics["artifact_promoted_dataset_ids"] == ["dataset_candidate"]
    assert all(
        call.kwargs["should_select_validated_candidates"] is True
        for call in resolve.await_args_list
    )


@pytest.mark.asyncio
async def test_corroboration_bundle_reads_prepared_overlay_and_network(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    keep_indexes = AsyncMock()
    artifact_bundle = importer.ProviderDirectoryArtifactBundle(
        stages=[
            importer.ProviderDirectoryPreparedArtifactStage(
                "mrf",
                "overlay_stage",
                importer.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE,
                keep_indexes,
            ),
            importer.ProviderDirectoryPreparedArtifactStage(
                "mrf",
                "network_stage",
                importer.PROVIDER_DIRECTORY_NETWORK_CATALOG_TABLE,
                keep_indexes,
            ),
        ]
    )
    corroboration_stage = importer.ProviderDirectoryPreparedArtifactStage(
        "mrf",
        "corroboration_stage",
        importer.PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_VIEW,
        keep_indexes,
    )

    async def publish_corroboration(*_args, **_kwargs):
        assert importer._qt(
            "mrf",
            importer.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE,
        ) == '"mrf"."overlay_stage"'
        assert importer._qt(
            "mrf",
            importer.PROVIDER_DIRECTORY_NETWORK_CATALOG_TABLE,
        ) == '"mrf"."network_stage"'
        return {"rows": 11}, corroboration_stage

    monkeypatch.setattr(importer, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(
        importer,
        "publish_provider_directory_address_corroboration_table",
        publish_corroboration,
    )

    published = await importer._is_provider_directory_corroboration_artifact_published(
        source_ids=["source_a"],
        network_catalog_metrics={"rows": 7},
        artifact_bundle=artifact_bundle,
    )

    assert published is True
    assert artifact_bundle.stages[-1] is corroboration_stage
    assert importer._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get() == {}


def test_endpoint_dataset_rejects_unbounded_partial_source_profile():
    candidate = importer.EndpointDatasetCandidate(
        endpoint_id="endpoint-1",
        dataset_id="dataset-1",
        acquisition_root_run_id="run-1",
        source_ids=("source-1",),
        selected_resources=("Location",),
        import_run_id="run-1",
        previous_dataset_id=None,
        expected_resources=("Location", "Organization"),
    )

    assert importer._is_endpoint_dataset_publishable(
        candidate,
        {
            "Location": {
                "complete": True,
                "error": None,
                "bounded": False,
                "next_url_remaining": False,
            }
        },
    ) is False


@pytest.mark.asyncio
async def test_artifact_dataset_publish_fails_before_scope_when_address_dependencies_missing(
    monkeypatch,
):
    fence = importer.ProviderDirectoryArtifactDatasetFence(
        (
            _artifact_dataset(
                selected_resources=("Location",),
                expected_resources=("Location", "Organization"),
            ),
        )
    )
    scope = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_resolve_provider_directory_artifact_datasets",
        AsyncMock(return_value=fence),
    )
    monkeypatch.setattr(importer, "_provider_directory_artifact_dataset_scope", scope)

    with pytest.raises(
        RuntimeError,
        match="target_dependencies_missing:dataset_profile:source_a",
    ):
        await importer._publish_provider_directory_dataset_artifacts(
            run_id="run_1",
            metrics={},
            source_ids=["source_a"],
            publish_corroboration=False,
            publish_artifacts_targets={"location_archive"},
        )

    scope.assert_not_awaited()


@pytest.mark.asyncio
async def test_artifact_scope_projection_measures_alias_amplification_and_capacity(
    monkeypatch,
):
    fence = importer.ProviderDirectoryArtifactDatasetFence(
        (
            _artifact_dataset("source_a", selected_resources=("Location",)),
            _artifact_dataset("source_b", selected_resources=("Location",)),
        )
    )
    monkeypatch.setattr(
        importer.db,
        "all",
        AsyncMock(
            return_value=[
                {
                    "dataset_id": "dataset_current",
                    "resource_type": "Location",
                    "resource_rows": 5,
                },
            ]
        ),
    )

    projection = await importer._provider_directory_artifact_scope_projection(
        fence,
        frozenset({"Location"}),
    )
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_SCOPE_MAX_PROJECTED_ROWS",
        "9",
    )

    assert projection == {
        "dataset_count": 1,
        "alias_count": 2,
        "dataset_rows": 5,
        "projected_rows": 10,
        "resource_types": ["Location"],
        "alias_amplification": 2.0,
        "aliases_by_dataset": {"dataset_current": 2},
        "projected_rows_by_resource": {"Location": 10},
    }
    artifact_metrics_by_name: dict[str, Any] = {}
    importer._record_artifact_scope_metrics(
        artifact_metrics_by_name,
        projection,
        ["provider_directory_location_artifact_scope_old"],
    )
    assert artifact_metrics_by_name["artifact_scope_alias_count"] == 2
    assert artifact_metrics_by_name["artifact_scope_aliases_by_dataset"] == {
        "dataset_current": 2
    }
    assert artifact_metrics_by_name["artifact_scope_reaped_table_count"] == 1
    with pytest.raises(
        RuntimeError,
        match="scope_projected_rows_exceeded:projected=10:capacity=9",
    ):
        importer._assert_provider_directory_artifact_scope_capacity(projection)


@pytest.mark.asyncio
async def test_artifact_scope_reaper_removes_only_sorted_scope_prefixes(monkeypatch):
    source_prefix = "provider_directory_source_artifact_scope_"
    location_prefix = "provider_directory_location_artifact_scope_"
    monkeypatch.setattr(
        importer.db,
        "all",
        AsyncMock(
            return_value=[
                {"tablename": f"{location_prefix}old"},
                {"tablename": "provider_directory_location_archive_stage_keep"},
                {"tablename": f"{source_prefix}old"},
            ]
        ),
    )
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)

    reaped = await importer._reap_provider_directory_artifact_scope_tables("mrf")

    assert reaped == [f"{location_prefix}old", f"{source_prefix}old"]
    assert [call.args[0] for call in status.await_args_list] == [
        f'DROP TABLE IF EXISTS "mrf"."{location_prefix}old";',
        f'DROP TABLE IF EXISTS "mrf"."{source_prefix}old";',
    ]


def _endpoint_dataset_candidate() -> importer.EndpointDatasetCandidate:
    return importer.EndpointDatasetCandidate(
        endpoint_id="endpoint_1",
        dataset_id="dataset_new",
        acquisition_root_run_id="run_2",
        source_ids=("source_a", "source_b"),
        selected_resources=("Practitioner",),
        import_run_id="run_2",
        previous_dataset_id="dataset_old",
    )


def test_endpoint_dataset_metadata_has_versioned_completion_proof():
    candidate = dataclasses.replace(
        _endpoint_dataset_candidate(),
        selected_resources=("OrganizationAffiliation",),
        import_run_id="run_terminal",
    )
    diagnostics_by_resource = {
        "OrganizationAffiliation": {
            "complete": True,
            "bounded": False,
            "error": None,
            "next_url_remaining": False,
            "rows_fetched": 0,
            "rows_written": 0,
        }
    }

    metadata = importer._endpoint_dataset_publication_metadata(
        candidate,
        diagnostics_by_resource,
    )

    assert metadata["completion_proof_v1"] == {
        "acquisition_root_run_id": "run_2",
        "terminal_run_id": "run_terminal",
        "source_ids": ["source_a", "source_b"],
        "selected_resources": ["OrganizationAffiliation"],
        "resource_diagnostics": diagnostics_by_resource,
    }


def test_endpoint_dataset_source_summary_sql_reads_only_immutable_payload():
    summary_sql = importer._endpoint_dataset_source_summary_metrics_sql()

    assert "provider_directory_dataset_resource" in summary_sql
    assert "payload_json::jsonb" in summary_sql
    assert "provider_directory_practitioner" not in summary_sql
    assert "provider_directory_location" not in summary_sql
    assert "COUNT(DISTINCT" in summary_sql
    assert "MATERIALIZED" not in summary_sql
    assert "summary_resource_types" in summary_sql


def _source_summary_candidate():
    return dataclasses.replace(
        _endpoint_dataset_candidate(),
        selected_resources=(
            "InsurancePlan",
            "Location",
            "Organization",
            "OrganizationAffiliation",
            "Practitioner",
            "PractitionerRole",
        ),
    )


def _source_summary_content_proof():
    count_by_resource = {
        "InsurancePlan": 2,
        "Location": 4,
        "Organization": 3,
        "OrganizationAffiliation": 1,
        "Practitioner": 5,
        "PractitionerRole": 7,
    }
    return importer.EndpointDatasetContentProof(
        dataset_hash="a" * 64,
        resource_count=sum(count_by_resource.values()),
        resource_hashes={
            resource_type: str(index) * 64
            for index, resource_type in enumerate(
                count_by_resource,
                start=1,
            )
        },
        resource_counts=count_by_resource,
    )


def _source_summary_relation_proof_by_name():
    return {
        importer.PROVIDER_DIRECTORY_DATASET_NETWORK_PLAN_METADATA_KEY: {
            "complete": True,
            "edge_count": 9,
        },
        (
            importer.PROVIDER_DIRECTORY_DATASET_AFFILIATION_ORGANIZATION_METADATA_KEY
        ): {"complete": True, "edge_count": 1},
    }


@pytest.mark.asyncio
async def test_endpoint_dataset_source_summary_is_exact_and_dataset_bound():
    """Expose only counts sealed to the exact immutable dataset proof."""
    candidate = _source_summary_candidate()
    content_proof = _source_summary_content_proof()
    connection = Mock(
        first=AsyncMock(
            return_value={
                "distinct_npis": 6,
                "address_records": 8,
                "addressed_locations": 3,
                "geocoded_locations": 1,
            }
        )
    )

    summary_map = await importer._endpoint_dataset_source_summary(
        connection,
        candidate,
        content_proof,
        _source_summary_relation_proof_by_name(),
    )

    assert summary_map["dataset_id"] == candidate.dataset_id
    assert summary_map["dataset_hash"] == content_proof.dataset_hash
    assert summary_map["total_resources"] == 22
    assert summary_map["resource_counts"] == content_proof.resource_counts
    assert summary_map["distinct_npis"] == 6
    assert summary_map["individual_practitioners"] == 5
    assert summary_map["organization_resources"] == 3
    assert summary_map["practitioner_role_resources"] == 7
    assert summary_map["network_plan_links"] == 9
    assert summary_map["organization_affiliation_links"] == 1
    assert summary_map["intentional_drop_counts"] == {}
    assert summary_map["unknown_field_counts"] == {}
    connection.first.assert_awaited_once()


@pytest.mark.asyncio
async def test_endpoint_dataset_source_summary_skips_legacy_missing_root():
    connection = Mock(first=AsyncMock())
    candidate = dataclasses.replace(
        _endpoint_dataset_candidate(),
        acquisition_root_run_id=None,
    )
    content_proof = importer.EndpointDatasetContentProof(
        dataset_hash="a" * 64,
        resource_count=0,
        resource_hashes={"Practitioner": hashlib.sha256().hexdigest()},
        resource_counts={"Practitioner": 0},
    )

    assert (
        await importer._endpoint_dataset_source_summary(
            connection,
            candidate,
            content_proof,
            {},
        )
        is None
    )
    connection.first.assert_not_awaited()


@pytest.mark.asyncio
async def test_incomplete_endpoint_dataset_candidate_is_not_published(monkeypatch):
    mark_candidate = AsyncMock()
    validate_candidate = AsyncMock()
    monkeypatch.setattr(importer, "_mark_endpoint_dataset_candidate", mark_candidate)
    monkeypatch.setattr(importer, "_validate_endpoint_dataset_candidate", validate_candidate)
    diagnostics_by_resource = {
        "Practitioner": {
            "complete": False,
            "bounded": True,
            "error": None,
            "next_url_remaining": True,
        }
    }

    finalization_summary_map = await importer._finalize_endpoint_dataset_candidate(
        _endpoint_dataset_candidate(),
        diagnostics_by_resource,
    )

    assert finalization_summary_map == {
        "dataset_id": "dataset_new",
        "status": importer.ENDPOINT_DATASET_INCOMPLETE,
        "published": False,
    }
    mark_candidate.assert_awaited_once_with(
        _endpoint_dataset_candidate(),
        importer.ENDPOINT_DATASET_INCOMPLETE,
        diagnostics_by_resource,
    )
    validate_candidate.assert_not_awaited()


@pytest.mark.asyncio
async def test_endpoint_dataset_checkpoint_clears_only_after_validation(monkeypatch):
    checkpoint_context = _cigna_checkpoint_context(
        "run_2",
        root_run_id="run_2",
        dataset_id="dataset_new",
    )
    candidate = dataclasses.replace(
        _endpoint_dataset_candidate(),
        checkpoint_context=checkpoint_context,
    )
    clear_dataset_checkpoints = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_clear_finalized_dataset_checkpoints",
        clear_dataset_checkpoints,
    )

    await importer._clear_finalized_endpoint_dataset_pagination_checkpoints(
        candidate,
        {
            "dataset_id": candidate.dataset_id,
            "status": importer.ENDPOINT_DATASET_INCOMPLETE,
            "published": False,
        },
    )
    clear_dataset_checkpoints.assert_not_awaited()

    await importer._clear_finalized_endpoint_dataset_pagination_checkpoints(
        candidate,
        {
            "dataset_id": candidate.dataset_id,
            "status": importer.ENDPOINT_DATASET_VALIDATED,
            "validated": True,
            "published": False,
        },
    )

    clear_dataset_checkpoints.assert_awaited_once_with(candidate)


@pytest.mark.asyncio
async def test_failed_endpoint_dataset_candidate_does_not_change_current_pointer(
    monkeypatch,
):
    mark_candidate = AsyncMock()
    validate_candidate = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_mark_endpoint_dataset_candidate",
        mark_candidate,
    )
    monkeypatch.setattr(
        importer,
        "_validate_endpoint_dataset_candidate",
        validate_candidate,
    )
    diagnostics_by_resource = {
        "Practitioner": {
            "complete": False,
            "bounded": False,
            "error": "upstream_failed",
            "next_url_remaining": False,
        }
    }

    summary = await importer._finalize_endpoint_dataset_candidate(
        _endpoint_dataset_candidate(),
        diagnostics_by_resource,
    )

    assert summary == {
        "dataset_id": "dataset_new",
        "status": importer.ENDPOINT_DATASET_FAILED,
        "published": False,
    }
    mark_candidate.assert_awaited_once_with(
        _endpoint_dataset_candidate(),
        importer.ENDPOINT_DATASET_FAILED,
        diagnostics_by_resource,
    )
    validate_candidate.assert_not_awaited()


class _EndpointDatasetPromotionHarness:
    def __init__(self):
        self.datasets = {
            "dataset_old": {"dataset_id": "dataset_old", "is_current": True, "status": "published"},
            "dataset_new": {
                "dataset_id": "dataset_new",
                "acquisition_root_run_id": "run_2",
                "is_current": False,
                "status": "acquiring",
            },
        }
        self.resources = [
            {"resource_type": "Practitioner", "resource_id": "prac-2", "payload_hash": "hash-2"},
            {"resource_type": "Organization", "resource_id": "org-1", "payload_hash": "hash-1"},
            {"resource_type": "Practitioner", "resource_id": "prac-1", "payload_hash": "hash-3"},
        ]
        self.events: list[str] = []
        self.committed = False
        self.max_batch_rows = 0

    async def __aenter__(self):
        self.events.append("begin")
        return self

    async def __aexit__(self, exc_type, _exc, _traceback):
        self.committed = exc_type is None
        self.events.append("commit" if self.committed else "rollback")

    async def first(self, sql, **params):
        if "AS distinct_npis" in sql:
            self.events.append("summarize_resources")
            return {
                "distinct_npis": 2,
                "address_records": 0,
                "addressed_locations": 0,
                "geocoded_locations": 0,
            }
        if "provider_directory_api_endpoint" in sql:
            self.events.append("lock_endpoint")
            return {"endpoint_id": params["endpoint_id"]}
        if "SELECT dataset_id, acquisition_root_run_id" in sql:
            self.events.append("lock_candidate")
            return self.datasets[params["dataset_id"]]
        self.events.append("lock_current")
        return self.datasets["dataset_old"]

    async def all(self, _sql, **params):
        if "provider_directory_bulk_acquisition_checkpoint" in _sql:
            return []
        ordered_resources = sorted(
            self.resources,
            key=lambda resource: (resource["resource_type"], resource["resource_id"]),
        )
        after_key = (
            params.get("after_resource_type", ""),
            params.get("after_resource_id", ""),
        )
        available_resources = [
            resource
            for resource in ordered_resources
            if (resource["resource_type"], resource["resource_id"]) > after_key
        ]
        resource_batch = available_resources[: params["batch_size"]]
        self.max_batch_rows = max(self.max_batch_rows, len(resource_batch))
        self.events.append("read_resources")
        return resource_batch

    async def status(self, sql, **params):
        sql_text = str(sql)
        if sql_text.startswith("SET TRANSACTION"):
            self.events.append("set_snapshot")
            return 1
        if params.get("status") != importer.ENDPOINT_DATASET_VALIDATED:
            raise AssertionError(f"unexpected validation SQL: {sql_text}")
        self.events.append("validate")
        self.datasets[params["dataset_id"]].update(
            is_current=False,
            status=params["status"],
            previous_dataset_id=params["previous_dataset_id"],
        )
        return 1


def _mock_endpoint_dataset_serving_builds(monkeypatch) -> None:
    monkeypatch.setattr(
        importer,
        "_lock_dataset_serving_relation_build",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_build_provider_directory_dataset_network_plan",
        AsyncMock(return_value={"complete": True, "edge_count": 0}),
    )
    monkeypatch.setattr(
        importer,
        "_build_provider_directory_dataset_affiliation_organization",
        AsyncMock(return_value={"complete": True, "edge_count": 0}),
    )


@pytest.mark.asyncio
async def test_endpoint_dataset_completion_validates_without_superseding_current(monkeypatch):
    harness = _EndpointDatasetPromotionHarness()
    monkeypatch.setattr(importer.db, "acquire", lambda: harness)
    monkeypatch.setattr(importer, "ENDPOINT_DATASET_HASH_BATCH_SIZE", 2)
    _mock_endpoint_dataset_serving_builds(monkeypatch)
    diagnostics_by_resource = {
        "Practitioner": {
            "complete": True,
            "bounded": False,
            "error": None,
            "next_url_remaining": False,
        }
    }

    validation_summary_map = await importer._finalize_endpoint_dataset_candidate(
        _endpoint_dataset_candidate(),
        diagnostics_by_resource,
    )

    assert harness.committed is True
    assert harness.events == [
        "begin",
        "set_snapshot",
        "lock_endpoint",
        "lock_candidate",
        "lock_current",
        "read_resources",
        "read_resources",
        "summarize_resources",
        "validate",
        "commit",
    ]
    assert harness.datasets["dataset_old"] == {
        "dataset_id": "dataset_old",
        "is_current": True,
        "status": importer.ENDPOINT_DATASET_PUBLISHED,
    }
    assert harness.datasets["dataset_new"]["previous_dataset_id"] == "dataset_old"
    assert harness.datasets["dataset_new"]["is_current"] is False
    assert harness.datasets["dataset_new"]["status"] == importer.ENDPOINT_DATASET_VALIDATED
    assert validation_summary_map["resource_count"] == 3
    assert validation_summary_map["published"] is False
    assert validation_summary_map["validated"] is True
    assert harness.max_batch_rows == 2
    expected_hash_input = "\n".join(
        importer._stable_identity_json(identity)
        for identity in sorted(
            (
                dataset_resource["resource_type"],
                dataset_resource["resource_id"],
                dataset_resource["payload_hash"],
            )
            for dataset_resource in harness.resources
        )
    )
    assert validation_summary_map["dataset_hash"] == hashlib.sha256(
        expected_hash_input.encode("utf-8")
    ).hexdigest()


@pytest.mark.asyncio
async def test_endpoint_dataset_validation_preserves_partition_state(
    monkeypatch,
):
    harness = _EndpointDatasetPromotionHarness()
    monkeypatch.setattr(importer.db, "acquire", lambda: harness)
    _mock_endpoint_dataset_serving_builds(monkeypatch)
    checkpoint_context = importer.PaginationCheckpointContext(
        canonical_api_base="https://example.test/fhir",
        source_scope_hash="partition_scope",
        source_ids=("source_a", "source_b"),
        owner_run_id="run_2",
        acquisition_root_run_id="run_2",
        endpoint_id="endpoint_1",
        dataset_id="dataset_new",
        lineage_verified=True,
    )
    candidate = dataclasses.replace(
        _endpoint_dataset_candidate(),
        checkpoint_context=checkpoint_context,
    )
    diagnostics_by_resource = {
        "Practitioner": {
            "complete": True,
            "bounded": False,
            "error": None,
            "next_url_remaining": False,
        }
    }

    publication_summary = await importer._finalize_endpoint_dataset_candidate(
        candidate,
        diagnostics_by_resource,
    )

    assert publication_summary["published"] is False
    assert publication_summary["validated"] is True
    assert harness.committed is True
    assert "clear_partition_proof" not in harness.events
    assert "clear_partition_checkpoint" not in harness.events
    assert harness.events[-2:] == ["validate", "commit"]


@pytest.mark.asyncio
async def test_endpoint_dataset_retry_reuses_checkpoint_candidate(monkeypatch):
    expected_dataset_id = importer._endpoint_dataset_candidate_id(
        "endpoint_1",
        ("Practitioner",),
        "run_original",
    )
    checkpoint_context = importer.PaginationCheckpointContext(
        canonical_api_base="https://shared.example/fhir",
        source_scope_hash="scope_1",
        source_ids=("source_a", "source_b"),
        owner_run_id="run_retry",
        retry_of_run_id="run_original",
        acquisition_root_run_id="run_original",
        lineage_verified=True,
    )
    checkpoint_dataset = AsyncMock(return_value=expected_dataset_id)
    ensure_candidate = AsyncMock()
    monkeypatch.setattr(importer, "_checkpoint_candidate_dataset_id", checkpoint_dataset)
    monkeypatch.setattr(
        importer,
        "_endpoint_dataset_state",
        AsyncMock(
            return_value={
                "endpoint_id": "endpoint_1",
                "acquisition_root_run_id": "run_original",
                "status": importer.ENDPOINT_DATASET_INCOMPLETE,
                "is_current": False,
                "previous_dataset_id": "dataset_old",
            }
        ),
    )
    monkeypatch.setattr(importer, "_ensure_endpoint_dataset_candidate", ensure_candidate)
    current_dataset = AsyncMock()
    monkeypatch.setattr(importer, "_current_endpoint_dataset_id", current_dataset)

    candidate = await importer._prepare_endpoint_dataset_candidate(
        [
            {"source_id": source_id, "endpoint_id": "endpoint_1"}
            for source_id in ("source_a", "source_b")
        ],
        ["Practitioner"],
        run_id="run_retry",
        retry_of_run_id="run_original",
        pagination_root_run_id="run_original",
        checkpoint_context=checkpoint_context,
    )

    assert candidate is not None
    assert candidate.dataset_id == expected_dataset_id
    assert candidate.acquisition_root_run_id == "run_original"
    assert candidate.reused_from_checkpoint is True
    assert candidate.checkpoint_context.dataset_id == expected_dataset_id
    assert candidate.checkpoint_context.endpoint_id == "endpoint_1"
    assert candidate.checkpoint_context.lineage_verified is True
    current_dataset.assert_not_awaited()
    ensure_candidate.assert_awaited_once_with(candidate)


class _AtomicCandidateInitializationHarness:
    def __init__(self, *, fail_checkpoint=False):
        self.fail_checkpoint = fail_checkpoint
        self.events = []
        self.committed = False

    async def __aenter__(self):
        self.events.append("begin")
        return self

    async def __aexit__(self, exc_type, _exc, _traceback):
        self.committed = exc_type is None
        self.events.append("commit" if self.committed else "rollback")

    async def first(self, sql, **_params):
        sql_text = str(sql)
        if "pg_advisory_xact_lock" in sql_text:
            self.events.append("lock")
        elif "provider_directory_api_endpoint" in sql_text:
            self.events.append("endpoint_lock")
        elif "WHERE dataset_id = :dataset_id" in sql_text:
            self.events.append("candidate_lookup")
        else:
            self.events.append("conflict_lookup")
        return None

    async def all(self, sql, **params):
        sql_text = str(sql)
        assert "ORDER BY dataset_id" in sql_text
        assert "FOR SHARE" in sql_text
        self.events.append("dataset_resource_parent_lock")
        return [
            {
                "dataset_id": dataset_id,
                "status": importer.ENDPOINT_DATASET_ACQUIRING,
            }
            for dataset_id in params["dataset_ids"]
        ]

    async def status(self, sql, **_params):
        sql_text = str(sql)
        if "provider_directory_pagination_checkpoint" in sql_text:
            self.events.append("checkpoint_upsert")
            if self.fail_checkpoint:
                raise RuntimeError("checkpoint_write_failed")
        elif "INSERT INTO" in sql_text and "endpoint_dataset" in sql_text:
            self.events.append("candidate_upsert")
        else:
            self.events.append("candidate_rows_clear")
        return 1


@pytest.mark.asyncio
async def test_partition_candidate_and_initial_checkpoint_share_transaction(
    monkeypatch,
):
    harness = _AtomicCandidateInitializationHarness(fail_checkpoint=True)
    monkeypatch.setattr(importer.db, "acquire", lambda: harness)
    directory_source = _last_updated_partition_test_source()
    partition_config, config_error = importer._last_updated_partition_config(
        directory_source,
        "Practitioner",
    )
    assert config_error is None and partition_config is not None
    context = dataclasses.replace(
        _last_updated_partition_test_context(),
        endpoint_id="endpoint_scan_partition_test",
        lineage_verified=True,
    )
    candidate = importer.EndpointDatasetCandidate(
        endpoint_id="endpoint_scan_partition_test",
        dataset_id="dataset_partition",
        acquisition_root_run_id="run_partition",
        source_ids=("source_scan_partition_test",),
        selected_resources=("Practitioner",),
        import_run_id="run_partition",
        previous_dataset_id=None,
        expected_resources=("Practitioner",),
        checkpoint_context=context,
    )
    initialization = importer._last_updated_partition_initialization(
        "Practitioner",
        partition_config,
    )

    with pytest.raises(RuntimeError, match="checkpoint_write_failed"):
        await importer._ensure_endpoint_dataset_candidate(
            candidate,
            (initialization,),
        )

    assert harness.committed is False
    assert harness.events == [
        "begin",
        "lock",
        "endpoint_lock",
        "candidate_lookup",
        "conflict_lookup",
        "candidate_upsert",
        "dataset_resource_parent_lock",
        "candidate_rows_clear",
        "dataset_resource_parent_lock",
        "checkpoint_upsert",
        "rollback",
    ]


@pytest.mark.asyncio
async def test_checkpoint_candidate_lookup_falls_back_to_same_root_ancestor(
    monkeypatch,
):
    checkpoint_lookup = AsyncMock(return_value={"dataset_id": "dataset_a"})
    monkeypatch.setattr(importer.db, "first", checkpoint_lookup)
    checkpoint_context = importer.PaginationCheckpointContext(
        canonical_api_base="https://shared.example/fhir",
        source_scope_hash="scope_1",
        source_ids=("source_a",),
        owner_run_id="run_retry_3",
        retry_of_run_id="run_retry_2",
        acquisition_root_run_id="run_root",
    )

    dataset_id = await importer._checkpoint_candidate_dataset_id(
        checkpoint_context,
        "endpoint_1",
        ("Practitioner",),
    )

    assert dataset_id == "dataset_a"
    lookup_sql = checkpoint_lookup.await_args.args[0]
    assert "OR CAST(:allow_ancestor_owner AS boolean)" in lookup_sql
    assert "checkpoint.owner_run_id = :expected_owner_run_id" in lookup_sql
    assert checkpoint_lookup.await_args.kwargs["allow_ancestor_owner"] is True
    assert checkpoint_lookup.await_args.kwargs["acquisition_root_run_id"] == "run_root"


def test_pagination_checkpoint_context_requires_nonempty_root():
    with pytest.raises(
        ValueError,
        match="provider_directory_pagination_root_missing",
    ):
        importer.PaginationCheckpointContext(
            canonical_api_base="https://shared.example/fhir",
            source_scope_hash="scope_1",
            source_ids=("source_a",),
            owner_run_id="run_1",
            acquisition_root_run_id="",
        )


@pytest.mark.asyncio
async def test_uncheckpointed_candidate_cleanup_is_root_fenced(monkeypatch):
    candidate = importer.EndpointDatasetCandidate(
        endpoint_id="endpoint_1",
        dataset_id="dataset_1",
        acquisition_root_run_id="run_root",
        source_ids=("source_a",),
        selected_resources=("Practitioner",),
        import_run_id="run_root",
        previous_dataset_id=None,
    )
    status_mock = AsyncMock(return_value=1)

    @contextlib.asynccontextmanager
    async def mutation_fence(dataset_ids, **_kwargs):
        assert dataset_ids == [candidate.dataset_id]
        yield type("CleanupExecutor", (), {"status": status_mock})()

    monkeypatch.setattr(
        importer,
        "_mutable_endpoint_dataset_resource_mutation",
        mutation_fence,
    )

    await importer._clear_uncheckpointed_endpoint_dataset_candidate(candidate)

    cleanup_sql = status_mock.await_args.args[0]
    assert "acquisition_root_run_id IS NOT DISTINCT FROM" in cleanup_sql
    assert status_mock.await_args.kwargs["acquisition_root_run_id"] == "run_root"


def _aetna_candidate_retry_context() -> importer.PaginationCheckpointContext:
    return importer.PaginationCheckpointContext(
        canonical_api_base=importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE,
        source_scope_hash="scope_aetna",
        source_ids=("aetna",),
        owner_run_id="run_retry_2",
        retry_of_run_id="run_root",
        acquisition_root_run_id="run_root",
        lineage_verified=True,
    )


@pytest.mark.asyncio
async def test_partition_candidate_reuses_root_lineage(monkeypatch):
    """A verified retry continues the root candidate without clearing its rows."""
    datasets_by_id: dict[str, dict[str, Any]] = {}
    expected_dataset_id = importer._endpoint_dataset_candidate_id(
        "endpoint_aetna",
        ("Practitioner",),
        "run_root",
    )

    async def dataset_state(dataset_id):
        return datasets_by_id.get(dataset_id, {})

    async def ensure_candidate(candidate):
        datasets_by_id[candidate.dataset_id] = {
            "endpoint_id": candidate.endpoint_id,
            "acquisition_root_run_id": candidate.acquisition_root_run_id,
            "status": importer.ENDPOINT_DATASET_INCOMPLETE,
            "is_current": False,
        }

    async def checkpoint_candidate_dataset_id(context, _endpoint_id, _resources):
        return expected_dataset_id if context is not None else None

    monkeypatch.setattr(
        importer,
        "_checkpoint_candidate_dataset_id",
        checkpoint_candidate_dataset_id,
    )
    monkeypatch.setattr(importer, "_endpoint_dataset_state", dataset_state)
    monkeypatch.setattr(importer, "_current_endpoint_dataset_id", AsyncMock(return_value=None))
    monkeypatch.setattr(importer, "_ensure_endpoint_dataset_candidate", ensure_candidate)
    source_records = [{"source_id": "aetna", "endpoint_id": "endpoint_aetna"}]

    root_candidate = await importer._prepare_endpoint_dataset_candidate(
        source_records,
        ["Practitioner"],
        run_id="run_root",
        retry_of_run_id=None,
        pagination_root_run_id="run_root",
        checkpoint_context=None,
    )
    checkpoint_context = _aetna_candidate_retry_context()
    retry_candidate = await importer._prepare_endpoint_dataset_candidate(
        source_records,
        ["Practitioner"],
        run_id="run_retry_2",
        retry_of_run_id="run_root",
        pagination_root_run_id="run_root",
        checkpoint_context=checkpoint_context,
    )

    assert root_candidate is not None and retry_candidate is not None
    assert retry_candidate.dataset_id == root_candidate.dataset_id
    assert retry_candidate.reused_from_checkpoint is True
    delete_status = AsyncMock()
    monkeypatch.setattr(importer.db, "status", delete_status)
    await importer._clear_uncheckpointed_endpoint_dataset_candidate(retry_candidate)
    delete_status.assert_not_awaited()


@pytest.mark.asyncio
async def test_endpoint_dataset_superseded_candidate_cannot_be_reused(monkeypatch):
    checkpoint_context = importer.PaginationCheckpointContext(
        canonical_api_base="https://shared.example/fhir",
        source_scope_hash="scope_1",
        source_ids=("source_a",),
        owner_run_id="run_retry",
        retry_of_run_id="run_original",
        acquisition_root_run_id="run_original",
        lineage_verified=True,
    )
    dataset_id = importer._endpoint_dataset_candidate_id(
        "endpoint_1",
        ("Practitioner",),
        "run_original",
    )
    monkeypatch.setattr(
        importer,
        "_checkpoint_candidate_dataset_id",
        AsyncMock(return_value=dataset_id),
    )
    monkeypatch.setattr(
        importer,
        "_endpoint_dataset_state",
        AsyncMock(
            return_value={
                "endpoint_id": "endpoint_1",
                "acquisition_root_run_id": "run_original",
                "status": importer.ENDPOINT_DATASET_SUPERSEDED,
                "is_current": False,
            }
        ),
    )

    with pytest.raises(
        RuntimeError,
        match="provider_directory_endpoint_dataset_root_already_finalized",
    ):
        await importer._prepare_endpoint_dataset_candidate(
            [{"source_id": "source_a", "endpoint_id": "endpoint_1"}],
            ["Practitioner"],
            run_id="run_retry",
            retry_of_run_id="run_original",
            pagination_root_run_id="run_original",
            checkpoint_context=checkpoint_context,
        )


def _published_endpoint_dataset_test_context():
    return importer.PaginationCheckpointContext(
        canonical_api_base="https://shared.example/fhir",
        source_scope_hash="scope_1",
        source_ids=("source_a",),
        owner_run_id="run_retry",
        retry_of_run_id="run_original",
        acquisition_root_run_id="run_original",
        lineage_verified=True,
    )


def _published_endpoint_dataset_metadata_by_field():
    return {
        "acquisition_root_run_id": "run_original",
        "selected_resources": ["Practitioner"],
        "expected_resources": ["Practitioner"],
        "source_ids": ["source_a"],
        "resource_diagnostics": {
            "Practitioner": {
                "complete": True,
                "bounded": False,
                "error": None,
                "next_url_remaining": False,
            }
        },
    }


def _published_endpoint_dataset_source_record():
    return {
        "source_id": "source_a",
        "endpoint_id": "endpoint_1",
        "metadata_json": {
            "provider_directory_supported_resources": ["Practitioner"],
            "provider_directory_fully_enumerable_resources": ["Practitioner"],
        },
    }


def _assert_validated_replay_completion(candidate):
    completions_by_resource = {"Practitioner": {"fresh_source"}}
    assert importer._finalized_endpoint_dataset_import_summary(
        candidate,
        completions_by_resource,
    )[2] == {"Practitioner": 0}
    assert completions_by_resource == {
        "Practitioner": {"fresh_source", "source_a"}
    }


def _install_published_endpoint_dataset_mocks(monkeypatch):
    checkpoint_lookup = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_checkpoint_candidate_dataset_id",
        checkpoint_lookup,
    )
    monkeypatch.setattr(
        importer,
        "_endpoint_dataset_state",
        AsyncMock(
            return_value={
                "endpoint_id": "endpoint_1",
                "acquisition_root_run_id": "run_original",
                "status": importer.ENDPOINT_DATASET_PUBLISHED,
                "is_current": True,
                "publication_metadata_json": (
                    _published_endpoint_dataset_metadata_by_field()
                ),
            }
        ),
    )
    ensure_candidate = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_ensure_endpoint_dataset_candidate",
        ensure_candidate,
    )
    return checkpoint_lookup, ensure_candidate


@pytest.mark.asyncio
async def test_endpoint_dataset_published_root_is_idempotent(monkeypatch):
    """A retry of the current root reuses its immutable publication."""
    checkpoint_context = _published_endpoint_dataset_test_context()
    dataset_id = importer._endpoint_dataset_candidate_id(
        "endpoint_1",
        ("Practitioner",),
        "run_original",
    )
    checkpoint_lookup, ensure_candidate = (
        _install_published_endpoint_dataset_mocks(monkeypatch)
    )

    candidate = await importer._prepare_endpoint_dataset_candidate(
        [_published_endpoint_dataset_source_record()],
        ["Practitioner"],
        run_id="run_retry",
        retry_of_run_id="run_original",
        pagination_root_run_id="run_original",
        checkpoint_context=checkpoint_context,
    )

    assert candidate is not None
    assert candidate.already_published is True
    assert candidate.dataset_id == dataset_id
    assert importer._published_endpoint_dataset_import_summary(candidate)[2] == {
        "Practitioner": 0
    }
    checkpoint_lookup.assert_not_awaited()
    ensure_candidate.assert_not_awaited()


@pytest.mark.asyncio
async def test_endpoint_dataset_validated_root_is_idempotent(monkeypatch):
    """A retry replays validated diagnostics without refetch or downgrade."""
    checkpoint_context = _published_endpoint_dataset_test_context()
    dataset_id = importer._endpoint_dataset_candidate_id(
        "endpoint_1",
        ("Practitioner",),
        "run_original",
    )
    checkpoint_lookup = AsyncMock()
    ensure_candidate = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_checkpoint_candidate_dataset_id",
        checkpoint_lookup,
    )
    monkeypatch.setattr(
        importer,
        "_endpoint_dataset_state",
        AsyncMock(
            return_value={
                "endpoint_id": "endpoint_1",
                "acquisition_root_run_id": "run_original",
                "status": importer.ENDPOINT_DATASET_VALIDATED,
                "is_current": False,
                "previous_dataset_id": "dataset_current",
                "dataset_hash": "a" * 64,
                "validated_at": "2026-07-13T10:00:00",
                "publication_metadata_json": (
                    _published_endpoint_dataset_metadata_by_field()
                ),
            }
        ),
    )
    monkeypatch.setattr(
        importer,
        "_ensure_endpoint_dataset_candidate",
        ensure_candidate,
    )

    candidate = await importer._prepare_endpoint_dataset_candidate(
        [_published_endpoint_dataset_source_record()],
        ["Practitioner"],
        run_id="run_retry",
        retry_of_run_id="run_original",
        pagination_root_run_id="run_original",
        checkpoint_context=checkpoint_context,
    )

    assert candidate is not None
    assert candidate.dataset_id == dataset_id
    assert candidate.already_validated is True
    assert candidate.already_published is False
    _assert_validated_replay_completion(candidate)
    checkpoint_lookup.assert_not_awaited()
    ensure_candidate.assert_not_awaited()


@pytest.mark.asyncio
async def test_endpoint_dataset_rejects_caller_root_mismatch(monkeypatch):
    checkpoint_context = importer.PaginationCheckpointContext(
        canonical_api_base="https://shared.example/fhir",
        source_scope_hash="scope_1",
        source_ids=("source_a",),
        owner_run_id="run_retry",
        retry_of_run_id="run_original",
        acquisition_root_run_id="run_original",
        lineage_verified=True,
    )
    checkpoint_lookup = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_checkpoint_candidate_dataset_id",
        checkpoint_lookup,
    )

    with pytest.raises(
        RuntimeError,
        match="provider_directory_endpoint_dataset_root_mismatch",
    ):
        await importer._prepare_endpoint_dataset_candidate(
            [{"source_id": "source_a", "endpoint_id": "endpoint_1"}],
            ["Practitioner"],
            run_id="run_retry",
            retry_of_run_id="run_original",
            pagination_root_run_id="different_root",
            checkpoint_context=checkpoint_context,
        )

    checkpoint_lookup.assert_not_awaited()


def test_endpoint_dataset_uses_only_resources_supported_by_endpoint_connector():
    requested_resources = list(importer.DEFAULT_RESOURCES)
    molina_source_map = {
        "source_id": "pdfhir_molina",
        "metadata_json": {
            "provider_directory_supported_resources": [
                "Location",
                "Practitioner",
                "PractitionerRole",
            ]
        },
    }
    alohr_source_map = {
        "source_id": "pdfhir_alohr",
        "api_base": importer.ALOHR_FHIR_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.ALOHR_FHIR_PROVIDER_DIRECTORY_BASE,
    }

    assert importer._endpoint_dataset_selected_resources(
        [molina_source_map],
        requested_resources,
    ) == ["PractitionerRole", "Practitioner", "Location"]
    assert importer._endpoint_dataset_selected_resources(
        [alohr_source_map],
        requested_resources,
    ) == [
        "PractitionerRole",
        "Practitioner",
        "Organization",
        "Location",
    ]
    assert importer._endpoint_dataset_expected_resources([alohr_source_map]) == (
        "Location",
        "Organization",
        "Practitioner",
        "PractitionerRole",
    )


def test_aetna_endpoint_dataset_selection_requires_fully_enumerable_resources():
    requested_resources = list(importer.DEFAULT_RESOURCES)
    commercial_source = importer._source_row_from_seed(
        importer._aetna_provider_directory_data_seed_rows()[0]
    )
    medicaid_source = importer._source_row_from_seed(
        {
            "id": "aetna-medicaid",
            "org_name": "Aetna",
            "api_base": importer.AETNA_PROVIDER_DIRECTORY_BASE,
            "source": "fixture",
        }
    )

    assert importer._endpoint_dataset_selected_resources(
        [commercial_source],
        requested_resources,
    ) == list(importer.AETNA_COMMERCIAL_SUPPORTED_RESOURCES)
    assert importer._endpoint_dataset_selected_resources(
        [medicaid_source],
        requested_resources,
    ) == []


def _maine_endpoint_source_record():
    return {
        "source_id": "maine",
        "endpoint_id": "endpoint_maine",
        "api_base": importer.MAINE_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.MAINE_PROVIDER_DIRECTORY_BASE,
    }


def _mock_maine_endpoint_candidate_storage(monkeypatch):
    monkeypatch.setattr(
        importer,
        "_select_endpoint_dataset_candidate",
        AsyncMock(
            return_value=importer.EndpointDatasetCandidateSelection(
                dataset_id="dataset_maine",
                acquisition_root_run_id="run_maine",
                previous_dataset_id=None,
                reused_from_checkpoint=False,
            )
        ),
    )
    monkeypatch.setattr(importer, "_ensure_endpoint_dataset_candidate", AsyncMock())


@pytest.mark.asyncio
async def test_endpoint_dataset_uses_selected_maine_public_resource_scope(monkeypatch):
    """Treat the verified five public Maine collections as the complete profile."""
    selected_resource_names = list(importer.MAINE_SUPPORTED_RESOURCES)
    _mock_maine_endpoint_candidate_storage(monkeypatch)

    candidate = await importer._prepare_endpoint_dataset_candidate(
        [_maine_endpoint_source_record()],
        selected_resource_names,
        run_id="run_maine",
        retry_of_run_id=None,
        pagination_root_run_id=None,
        checkpoint_context=None,
    )

    assert candidate is not None
    assert candidate.selected_resources == tuple(sorted(selected_resource_names))
    assert candidate.expected_resources == tuple(sorted(selected_resource_names))
    assert importer._is_endpoint_dataset_publishable(
        candidate,
        {
            resource_type: {
                "complete": True,
                "error": None,
                "bounded": False,
                "next_url_remaining": False,
            }
            for resource_type in selected_resource_names
        },
    )


@pytest.mark.asyncio
async def test_endpoint_dataset_rejects_partial_maine_public_resource_scope(monkeypatch):
    _mock_maine_endpoint_candidate_storage(monkeypatch)

    partial_candidate = await importer._prepare_endpoint_dataset_candidate(
        [_maine_endpoint_source_record()],
        ["Location"],
        run_id="run_maine",
        retry_of_run_id=None,
        pagination_root_run_id=None,
        checkpoint_context=None,
    )

    assert partial_candidate is not None
    assert partial_candidate.selected_resources == ("Location",)
    assert partial_candidate.expected_resources == tuple(
        sorted(importer.MAINE_SUPPORTED_RESOURCES)
    )
    assert importer._is_endpoint_dataset_publishable(
        partial_candidate,
        {
            "Location": {
                "complete": True,
                "error": None,
                "bounded": False,
                "next_url_remaining": False,
            }
        },
    ) is False


@pytest.mark.parametrize(
    "diagnostic_by_field",
    [
        {
            "complete": True,
            "error": "provider_directory_resource_fetch_incomplete",
            "bounded": False,
            "next_url_remaining": False,
        },
        {
            "complete": True,
            "error": None,
            "bounded": True,
            "next_url_remaining": False,
        },
        {
            "complete": True,
            "error": None,
            "bounded": False,
            "next_url_remaining": True,
        },
        None,
    ],
)
def test_endpoint_dataset_rejects_incomplete_selected_resource(
    diagnostic_by_field,
):
    candidate = importer.EndpointDatasetCandidate(
        endpoint_id="endpoint_maine",
        dataset_id="dataset_maine",
        acquisition_root_run_id="run_maine",
        source_ids=("maine",),
        selected_resources=("Location", "PractitionerRole"),
        import_run_id="run_maine",
        previous_dataset_id=None,
        expected_resources=("Location", "PractitionerRole"),
    )

    diagnostics_by_resource = {
        "Location": {
            "complete": True,
            "error": None,
            "bounded": False,
            "next_url_remaining": False,
        }
    }
    if diagnostic_by_field is not None:
        diagnostics_by_resource["PractitionerRole"] = diagnostic_by_field

    assert importer._is_endpoint_dataset_publishable(
        candidate,
        diagnostics_by_resource,
    ) is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "seed_row",
    [
        {
            "id": "centene-probe-only",
            "org_name": "Centene Corporation",
            "api_base": importer.CENTENE_PROVIDER_DIRECTORY_BASE,
            "source": "fixture",
        },
        {
            "id": "uhc-probe-only",
            "org_name": "UnitedHealthcare",
            "api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
            "source": "fixture",
        },
    ],
)
async def test_probe_only_sources_refuse_resource_acquisition(seed_row):
    source = importer._source_row_from_seed(seed_row)

    with pytest.raises(
        RuntimeError,
        match=(
            rf"provider_directory_resource_acquisition_blocked:{source['source_id']}"
            rf":{importer.PROVIDER_DIRECTORY_PROBE_ONLY_BLOCKED_REASON}"
        ),
    ):
        await importer._import_resources(
            [source],
            resources=list(importer.DEFAULT_RESOURCES),
            per_resource_limit=0,
            page_limit=0,
            page_count=100,
            timeout=1,
            run_id=None,
        )


@pytest.mark.asyncio
async def test_endpoint_dataset_does_not_publish_an_empty_resource_selection():
    candidate = await importer._prepare_endpoint_dataset_candidate(
        [{"source_id": "source_empty", "endpoint_id": "endpoint_empty"}],
        [],
        run_id="run_empty",
        retry_of_run_id=None,
        pagination_root_run_id=None,
        checkpoint_context=None,
    )

    assert candidate is None


@pytest.mark.asyncio
async def test_import_linked_resource_rows_fetches_role_references_and_upserts(monkeypatch):
    """Verify this provider-directory regression contract."""
    async def fake_fetch_json(url, *, timeout):
        if "/Practitioner/prac-1" in url:
            return 200, {"resourceType": "Practitioner", "id": "prac-1"}, None, 5
        if "/Location/loc-1" in url:
            return (
                200,
                {
                    "resourceType": "Location",
                    "id": "loc-1",
                    "address": {"line": ["100 Main"], "city": "Chicago", "state": "IL", "postalCode": "60601"},
                },
                None,
                5,
            )
        if "/InsurancePlan/plan-1" in url:
            return 200, {"resourceType": "InsurancePlan", "id": "plan-1", "name": "Plan"}, None, 5
        if "/Endpoint/endpoint-1" in url:
            return 200, {"resourceType": "Endpoint", "id": "endpoint-1", "status": "active"}, None, 5
        return 404, None, None, 5

    upserts = []

    async def fake_upsert(model, rows, **_kwargs):
        upserts.append((model, rows, _kwargs))
        return len(rows)

    monkeypatch.setattr(importer, "_fetch_json", fake_fetch_json)
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)

    linked_resource_counts = await importer._import_linked_resource_rows(
        {"source_id": "source_a", "api_base": "https://example.test/fhir"},
        {
            "PractitionerRole": [
                {
                    "resource_id": "role-1",
                    "practitioner_ref": "Practitioner/prac-1",
                    "location_refs": ["Location/loc-1"],
                    "insurance_plan_refs": ["InsurancePlan/plan-1"],
                    "endpoint_refs": ["Endpoint/endpoint-1"],
                }
            ]
        },
        per_source_limit=5,
        timeout=3,
        run_id="run_1",
    )

    assert linked_resource_counts == {
        "Practitioner": 1,
        "Location": 1,
        "InsurancePlan": 1,
        "Endpoint": 1,
    }
    concrete_models = [
        model
        for model, _rows, _kwargs in upserts
        if model not in {ProviderDirectoryCanonicalResource, ProviderDirectorySourceResource}
    ]
    assert concrete_models == [
        ProviderDirectoryPractitioner,
        ProviderDirectoryLocation,
        ProviderDirectoryInsurancePlan,
        ProviderDirectoryEndpoint,
    ]
    canonical_rows = [
        observed_row_map
        for model, observed_rows, _kwargs in upserts
        if model is ProviderDirectoryCanonicalResource
        for observed_row_map in observed_rows
    ]
    source_edge_rows = [
        observed_row_map
        for model, observed_rows, _kwargs in upserts
        if model is ProviderDirectorySourceResource
        for observed_row_map in observed_rows
    ]
    assert {observed_row_map["resource_type"] for observed_row_map in canonical_rows} == {
        "Practitioner",
        "Location",
        "InsurancePlan",
        "Endpoint",
    }
    assert {observed_row_map["canonical_api_base"] for observed_row_map in canonical_rows} == {"https://example.test/fhir"}
    assert {observed_row_map["source_id"] for observed_row_map in source_edge_rows} == {"source_a"}
    assert {observed_row_map["resource_type"] for observed_row_map in source_edge_rows} == {
        "Practitioner",
        "Location",
        "InsurancePlan",
        "Endpoint",
    }
    assert {observed_row_map["last_seen_run_id"] for observed_row_map in source_edge_rows} == {"run_1"}


@pytest.mark.asyncio
async def test_linked_rows_use_compatibility_owner(monkeypatch):
    async def fake_fetch_json(fetch_url, *, timeout):
        if "/Practitioner/prac-1" in fetch_url:
            return 200, {"resourceType": "Practitioner", "id": "prac-1"}, None, 5
        return 404, None, None, 5

    captured_upsert_calls = []

    async def fake_upsert(resource_model, resource_rows, **upsert_kwargs):
        captured_upsert_calls.append((resource_model, resource_rows, upsert_kwargs))
        return len(resource_rows)

    monkeypatch.setattr(importer, "_fetch_json", fake_fetch_json)
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)

    linked_resource_counts = await importer._import_linked_resource_rows(
        {"source_id": "source_a", "api_base": "https://example.test/fhir"},
        {
            "PractitionerRole": [
                {
                    "resource_id": "role-1",
                    "practitioner_ref": "Practitioner/prac-1",
                }
            ]
        },
        per_source_limit=5,
        timeout=3,
        run_id="run_1",
        source_ids=["source_b", "source_a"],
    )

    assert linked_resource_counts == {"Practitioner": 1}
    practitioner_rows = [
        practitioner_row
        for resource_model, resource_rows, _upsert_kwargs in captured_upsert_calls
        if resource_model is ProviderDirectoryPractitioner
        for practitioner_row in resource_rows
    ]
    source_edge_rows = [
        source_edge_row
        for resource_model, resource_rows, _upsert_kwargs in captured_upsert_calls
        if resource_model is ProviderDirectorySourceResource
        for source_edge_row in resource_rows
    ]
    assert [practitioner_row["source_id"] for practitioner_row in practitioner_rows] == [
        "source_a"
    ]
    assert [source_edge_row["source_id"] for source_edge_row in source_edge_rows] == [
        "source_a"
    ]


@pytest.mark.asyncio
async def test_import_linked_resource_rows_fetches_network_organization_from_network_endpoint(monkeypatch):
    calls: list[str] = []

    async def fake_fetch_json(url, *, timeout):
        calls.append(url)
        if "type=ntwk" in url:
            return 200, {"resourceType": "Bundle", "entry": [{"resource": {"resourceType": "Organization", "id": "network-1", "name": "Gold Network"}}]}, None, 5
        return 404, None, None, 5

    upserts = []

    async def fake_upsert(model, rows, **_kwargs):
        upserts.append((model, rows, _kwargs))
        return len(rows)

    monkeypatch.setattr(importer, "_fetch_json", fake_fetch_json)
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)

    counts = await importer._import_linked_resource_rows(
        {
            "source_id": "source_a",
            "api_base": "https://example.test/fhir",
            "endpoint_network": "Organization?type=ntwk",
        },
        {
            "InsurancePlan": [
                {
                    "resource_id": "plan-1",
                    "network_refs": ["Organization/network-1"],
                }
            ]
        },
        per_source_limit=5,
        timeout=3,
        run_id="run_1",
    )

    assert counts == {"Organization": 1}
    assert calls[0] == "https://example.test/fhir/Organization?type=ntwk&_count=1&_id=network-1"
    assert [model for model, _rows, _kwargs in upserts] == [
        ProviderDirectoryCanonicalResource,
        ProviderDirectorySourceResource,
        ProviderDirectoryOrganization,
    ]
    assert upserts[0][1][0]["canonical_api_base"] == "https://example.test/fhir"
    assert upserts[0][1][0]["resource_type"] == "Organization"
    assert upserts[1][1][0]["source_id"] == "source_a"
    assert upserts[1][1][0]["last_seen_run_id"] == "run_1"
    assert upserts[2][1][0]["name"] == "Gold Network"


@pytest.mark.asyncio
async def test_import_linked_resource_rows_reports_progress_when_flushing(monkeypatch):
    async def fake_fetch_json(url, *, timeout):
        if "/Practitioner/prac-1" in url:
            return 200, {"resourceType": "Practitioner", "id": "prac-1"}, None, 5
        if "/Location/loc-1" in url:
            return (
                200,
                {
                    "resourceType": "Location",
                    "id": "loc-1",
                    "address": {"line": ["100 Main"], "city": "Chicago", "state": "IL", "postalCode": "60601"},
                },
                None,
                5,
            )
        return 404, None, None, 5

    async def fake_upsert(_model, rows, **_kwargs):
        return len(rows)

    progress: list[tuple[str, int]] = []

    async def linked_progress(resource_type: str, written: int) -> None:
        progress.append((resource_type, written))

    monkeypatch.setattr(importer, "_fetch_json", fake_fetch_json)
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)

    counts = await importer._import_linked_resource_rows(
        {"source_id": "source_a", "api_base": "https://example.test/fhir"},
        {
            "PractitionerRole": [
                {
                    "resource_id": "role-1",
                    "practitioner_ref": "Practitioner/prac-1",
                    "location_refs": ["Location/loc-1"],
                }
            ]
        },
        per_source_limit=5,
        timeout=3,
        run_id="run_1",
        progress_callback=linked_progress,
        flush_rows=1,
    )

    assert counts == {"Practitioner": 1, "Location": 1}
    assert sorted(progress) == [("Location", 1), ("Practitioner", 1)]


@pytest.mark.asyncio
async def test_import_resources_accumulates_linked_resource_counts(monkeypatch):
    """Verify this provider-directory regression contract."""
    _stub_resource_import_metadata(monkeypatch)

    async def fake_fetch_resource_rows(source, resource_type, **_kwargs):
        if resource_type != "PractitionerRole":
            return None
        return importer.ResourceFetchResult(
            model=ProviderDirectoryPractitionerRole,
            rows=[
                {
                    "source_id": source["source_id"],
                    "resource_id": "role-1",
                    "practitioner_ref": "Practitioner/prac-1",
                    "location_refs": ["Location/loc-1"],
                }
            ],
            rows_fetched=1,
            rows_written=0,
            pages_fetched=1,
            complete=True,
            row_limit_reached=False,
            page_limit_reached=False,
            hard_page_limit_reached=False,
            next_url_remaining=False,
        )

    async def fake_upsert(_model, rows, **_kwargs):
        return len(rows)

    linked_kwargs = []

    async def fake_import_linked(_source, _rows_by_resource, **_kwargs):
        linked_kwargs.append(_kwargs)
        return {"Location": 2}

    monkeypatch.setattr(importer, "_fetch_resource_rows", fake_fetch_resource_rows)
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)
    monkeypatch.setattr(importer, "_import_linked_resource_rows", fake_import_linked)

    linked_counts_by_resource = {}
    counts = await importer._import_resources(
        [{"source_id": "source_a", "api_base": "https://example.test/fhir"}],
        resources=["PractitionerRole"],
        per_resource_limit=1,
        page_limit=1,
        page_count=1,
        timeout=3,
        run_id="run_1",
        linked_resource_limit=5,
        linked_counts=linked_counts_by_resource,
    )

    assert counts == {"PractitionerRole": 1}
    assert linked_counts_by_resource == {"Location": 2}
    assert len(linked_kwargs) == 1
    assert linked_kwargs[0]["per_source_limit"] == 5
    assert linked_kwargs[0]["concurrency"] == 5
    assert linked_kwargs[0]["timeout"] == 3
    assert linked_kwargs[0]["run_id"] == "run_1"
    assert linked_kwargs[0]["source_ids"] == ["source_a"]
    assert linked_kwargs[0]["track_seen"] is False
    assert linked_kwargs[0]["seen_table"] is None
    assert linked_kwargs[0]["deadline_seconds"] == 0
    assert callable(linked_kwargs[0]["progress_callback"])


@pytest.mark.asyncio
async def test_import_resources_retries_suspicious_zero_practitioner_role(monkeypatch):
    """Verify import resources retries suspicious zero practitioner role."""
    metadata_calls: list[dict[str, Any]] = []

    async def fake_update_metadata(_source_ids, **kwargs):
        metadata_calls.append(kwargs["diagnostics"])

    fetch_calls: list[str] = []

    async def fake_fetch_resource_rows(source_row, resource_type, **_kwargs):
        """Support the fake fetch resource rows test fixture."""
        fetch_calls.append(resource_type)
        if resource_type == "PractitionerRole" and fetch_calls.count("PractitionerRole") == 1:
            return importer.ResourceFetchResult(
                model=ProviderDirectoryPractitionerRole,
                rows=[],
                rows_fetched=0,
                rows_written=0,
                pages_fetched=1,
                complete=True,
                row_limit_reached=False,
                page_limit_reached=False,
                hard_page_limit_reached=False,
                next_url_remaining=False,
            )
        if resource_type == "PractitionerRole":
            return importer.ResourceFetchResult(
                model=ProviderDirectoryPractitionerRole,
                rows=[
                    {
                        "source_id": source_row["source_id"],
                        "resource_id": "role-1",
                        "practitioner_ref": "Practitioner/prac-1",
                        "location_refs": ["Location/loc-1"],
                    }
                ],
                rows_fetched=1,
                rows_written=0,
                pages_fetched=1,
                complete=True,
                row_limit_reached=False,
                page_limit_reached=False,
                hard_page_limit_reached=False,
                next_url_remaining=False,
            )
        if resource_type == "Practitioner":
            return importer.ResourceFetchResult(
                model=ProviderDirectoryPractitioner,
                rows=[{"source_id": source_row["source_id"], "resource_id": "prac-1"}],
                rows_fetched=1,
                rows_written=0,
                pages_fetched=1,
                complete=True,
                row_limit_reached=False,
                page_limit_reached=False,
                hard_page_limit_reached=False,
                next_url_remaining=False,
            )
        if resource_type == "Location":
            return importer.ResourceFetchResult(
                model=ProviderDirectoryLocation,
                rows=[{"source_id": source_row["source_id"], "resource_id": "loc-1"}],
                rows_fetched=1,
                rows_written=0,
                pages_fetched=1,
                complete=True,
                row_limit_reached=False,
                page_limit_reached=False,
                hard_page_limit_reached=False,
                next_url_remaining=False,
            )
        return None

    async def fake_upsert(_model, rows, **_kwargs):
        return len(rows)

    monkeypatch.setattr(importer, "_update_source_resource_import_metadata", fake_update_metadata)
    monkeypatch.setattr(importer, "_fetch_resource_rows", fake_fetch_resource_rows)
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)

    counts = await importer._import_resources(
        [{"source_id": "source_a", "api_base": "https://example.test/fhir"}],
        resources=["PractitionerRole", "Practitioner", "Location"],
        per_resource_limit=0,
        page_limit=0,
        page_count=25,
        timeout=3,
        run_id="run_1",
    )

    assert fetch_calls == ["PractitionerRole", "Practitioner", "Location", "PractitionerRole"]
    assert counts["PractitionerRole"] == 1
    assert counts["Practitioner"] == 1
    assert counts["Location"] == 1
    role_diagnostic = metadata_calls[0]["PractitionerRole"]
    assert role_diagnostic["retry_of_zero_rows"] is True
    assert role_diagnostic["retry_reason"] == importer.PRACTITIONER_ROLE_ZERO_RETRY_REASON
    assert role_diagnostic["fetch_mode"] == "paged_retry"
    assert role_diagnostic["rows_fetched"] == 1
    assert role_diagnostic["error"] is None


@pytest.mark.asyncio
async def test_import_resources_does_not_stale_cleanup_suspicious_zero_role_after_retry(monkeypatch):
    """Verify this provider-directory regression contract."""
    metadata_calls: list[dict[str, Any]] = []

    async def fake_update_metadata(_source_ids, **kwargs):
        metadata_calls.append(kwargs["diagnostics"])

    async def fake_fetch_resource_rows(source, resource_type, **_kwargs):
        model_by_resource = {
            "PractitionerRole": ProviderDirectoryPractitionerRole,
            "Practitioner": ProviderDirectoryPractitioner,
            "Location": ProviderDirectoryLocation,
        }
        rows_by_resource = {
            "PractitionerRole": [],
            "Practitioner": [{"source_id": source["source_id"], "resource_id": "prac-1"}],
            "Location": [{"source_id": source["source_id"], "resource_id": "loc-1"}],
        }
        rows = rows_by_resource[resource_type]
        return importer.ResourceFetchResult(
            model=model_by_resource[resource_type],
            rows=rows,
            rows_fetched=len(rows),
            rows_written=0,
            pages_fetched=1,
            complete=True,
            row_limit_reached=False,
            page_limit_reached=False,
            hard_page_limit_reached=False,
            next_url_remaining=False,
        )

    async def fake_upsert(_model, rows, **_kwargs):
        return len(rows)

    deleted_models: list[type] = []

    async def fake_delete_stale(model, *_args, **_kwargs):
        deleted_models.append(model)
        return 0

    async def fake_mark_seen(*_args, **_kwargs):
        return None

    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_SEEN_STAGE", "0")
    monkeypatch.setattr(importer, "_update_source_resource_import_metadata", fake_update_metadata)
    monkeypatch.setattr(importer, "_fetch_resource_rows", fake_fetch_resource_rows)
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)
    monkeypatch.setattr(importer, "_delete_stale_resource_rows", fake_delete_stale)
    monkeypatch.setattr(importer, "_mark_resource_rows_seen", fake_mark_seen)

    counts = await importer._import_resources(
        [{"source_id": "source_a", "api_base": "https://example.test/fhir"}],
        resources=["PractitionerRole", "Practitioner", "Location"],
        per_resource_limit=0,
        page_limit=0,
        page_count=25,
        timeout=3,
        run_id="run_1",
        stale_cleanup=True,
    )

    assert counts["PractitionerRole"] == 0
    assert counts["Practitioner"] == 1
    assert counts["Location"] == 1
    assert ProviderDirectoryPractitionerRole not in deleted_models
    assert ProviderDirectoryPractitioner in deleted_models
    assert ProviderDirectoryLocation in deleted_models
    role_diagnostic = metadata_calls[0]["PractitionerRole"]
    assert role_diagnostic["complete"] is False
    assert role_diagnostic["error"] == importer.PRACTITIONER_ROLE_ZERO_RETRY_EMPTY_ERROR
    assert role_diagnostic["retry_of_zero_rows"] is True


@pytest.mark.asyncio
async def test_publish_provider_directory_location_address_keys_skips_without_canonical_functions(monkeypatch):
    monkeypatch.setattr(importer, "_has_address_canon_functions", AsyncMock(return_value=False))
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)

    stamped = await importer.publish_provider_directory_location_address_keys("mrf")

    assert stamped == 0
    status.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_provider_directory_location_address_keys_runs_canonical_update(monkeypatch):
    class Row:
        def __init__(self, **values):
            self._mapping = values

    monkeypatch.setattr(importer, "_has_address_canon_functions", AsyncMock(return_value=True))
    table_exists = AsyncMock(return_value=True)
    monkeypatch.setattr(importer, "_table_exists", table_exists)
    first = AsyncMock(
        side_effect=[
            Row(candidate_rows=7, updated_rows=7, last_source_id="source_a", last_resource_id="loc-7"),
            Row(candidate_rows=0, updated_rows=0, last_source_id=None, last_resource_id=None),
        ]
    )
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "first", first)
    monkeypatch.setattr(importer.db, "status", status)

    stamped = await importer.publish_provider_directory_location_address_keys(
        "mrf",
        run_id="run_1",
        source_ids=["source_a"],
        seen_table="provider_directory_import_seen_stage_test",
    )

    assert stamped == 7
    table_exists.assert_awaited_once_with("mrf", "geo_zip_lookup")
    assert first.await_count == 2
    sql = first.await_args_list[0].args[0]
    assert 'UPDATE "mrf"."provider_directory_location" AS loc' in sql
    assert 'LEFT JOIN "mrf"."geo_zip_lookup" AS geo' in sql
    assert 'FROM "mrf"."provider_directory_import_seen_stage_test" AS seen' in sql
    assert first.await_args_list[0].kwargs["run_id"] == "run_1"
    assert first.await_args_list[0].kwargs["source_ids"] == ["source_a"]
    assert first.await_args_list[0].kwargs["after_source_id"] is None
    assert first.await_args_list[1].kwargs["after_source_id"] == "source_a"
    assert first.await_args_list[1].kwargs["after_resource_id"] == "loc-7"
    status.assert_not_awaited()


def test_provider_directory_location_archive_stage_sql_filters_keyable_uuid_locations():
    sql = importer.provider_directory_location_archive_stage_sql("mrf", "provider_directory_location_archive_stage_test")

    assert 'CREATE UNLOGGED TABLE "mrf"."provider_directory_location_archive_stage_test" AS' in sql
    assert 'FROM "mrf"."provider_directory_location" AS loc' in sql
    assert 'FROM "mrf"."provider_directory_organization" AS organization' in sql
    assert "loc.address_key ~*" in sql
    assert "loc.address_key::uuid AS address_key" in sql
    assert '"mrf".addr_key_v1(' in sql
    assert "NOT IN ('UN', 'XX', 'ZZ', 'NULL', 'N/A')" in sql
    assert "IN ('US', 'USA', 'UNITEDSTATES', 'UNITEDSTATESOFAMERICA', '840', '001')" in sql
    assert "NULLIF(BTRIM(loc.first_line), '') IS NOT NULL" in sql
    assert "OR NULLIF(BTRIM(loc.city_name), '') IS NOT NULL" in sql
    assert "SELECT DISTINCT ON (address_key)" in sql
    assert "WHERE address_key IS NOT NULL" in sql


def test_provider_directory_location_archive_stage_sql_can_scope_to_seen_stage():
    sql = importer.provider_directory_location_archive_stage_sql(
        "mrf",
        "provider_directory_location_archive_stage_test",
        run_id="run_1",
        seen_table="provider_directory_import_seen_stage_test",
    )

    assert 'FROM "mrf"."provider_directory_import_seen_stage_test" AS seen' in sql
    assert "seen.resource_type = 'Location'" in sql
    assert "AND seen.run_id = CAST(:run_id AS varchar)" in sql
    assert "AND seen.source_id = loc.source_id" in sql
    assert "AND seen.resource_id = loc.resource_id" in sql
    assert "seen.resource_type = 'Organization'" in sql
    assert "AND seen.source_id = organization.source_id" in sql
    assert "AND seen.resource_id = organization.resource_id" in sql
    assert "loc.last_seen_run_id = CAST(:run_id AS varchar)" not in sql


def test_location_archive_sql_scopes_without_seen_stage():
    sql = importer.provider_directory_location_archive_stage_sql(
        "mrf",
        "provider_directory_location_archive_stage_test",
        run_id="run_1",
    )

    assert "loc.last_seen_run_id = CAST(:run_id AS varchar)" in sql
    assert "organization.last_seen_run_id = CAST(:run_id AS varchar)" in sql
    assert "provider_directory_import_seen_stage" not in sql


def test_provider_directory_openaddresses_archive_backfill_sql_is_exact_and_guarded():
    sql = importer._provider_directory_openaddresses_archive_backfill_sql(
        "mrf",
        "provider_directory_location_archive_stage_test",
    )

    assert 'FROM "mrf"."provider_directory_location_archive_stage_test" AS stage_row' in sql
    assert 'FROM "mrf"."openaddresses_geocode" AS oa' in sql
    assert "oa.address_key = target.address_key" in sql
    assert "HAVING count(*) > 0" in sql
    assert "max(oa.lat) - min(oa.lat) <= :coord_tolerance" in sql
    assert "oa.lat BETWEEN 24 AND 50 AND oa.long BETWEEN -125 AND -66" in sql
    assert "archive.lat IS NULL" in sql
    assert "archive.long IS NULL" in sql
    assert "geocode_source = 'openaddresses_address_key'" in sql


@pytest.mark.asyncio
async def test_provider_directory_openaddresses_archive_backfill_skips_without_source(monkeypatch):
    monkeypatch.setattr(importer, "_table_exists", AsyncMock(return_value=False))
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)

    updated = await importer._backfill_archive_openaddresses_coordinates(
        "mrf",
        "provider_directory_location_archive_stage_test",
    )

    assert updated == 0
    status.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_provider_directory_location_archive_resolves_and_cleans_stage(monkeypatch):
    monkeypatch.setattr(importer, "_has_address_canon_functions", AsyncMock(return_value=True))
    monkeypatch.setattr(importer, "_table_exists", AsyncMock(return_value=True))
    status = AsyncMock(return_value=0)
    monkeypatch.setattr(importer.db, "status", status)

    class Stats:
        def __init__(self):
            self.staged = 10
            self.distinct_keys = 8
            self.inserted = 7
            self.provenance_updates = 1
            self.null_key_rows = 0

    resolve = AsyncMock(return_value=Stats())
    monkeypatch.setattr(importer, "resolve_into_archive", resolve)

    stats = await importer.publish_provider_directory_location_archive(
        "mrf",
        run_id="run_1",
        stage_table="provider_directory_location_archive_stage_test",
    )

    assert stats["inserted"] == 7
    assert stats["provenance_updates"] == 1
    assert stats["openaddresses_coordinate_backfill_rows"] == 0
    status_calls = [call.args[0] for call in status.await_args_list]
    assert status_calls[0] == 'DROP TABLE IF EXISTS "mrf"."provider_directory_location_archive_stage_test";'
    assert 'CREATE UNLOGGED TABLE "mrf"."provider_directory_location_archive_stage_test" AS' in status_calls[1]
    assert "loc.last_seen_run_id = CAST(:run_id AS varchar)" in status_calls[1]
    assert status.await_args_list[1].kwargs == {"run_id": "run_1"}
    assert status_calls[2] == 'ANALYZE "mrf"."provider_directory_location_archive_stage_test";'
    assert any('FROM "mrf"."openaddresses_geocode" AS oa' in sql for sql in status_calls)
    assert status_calls[-1] == 'DROP TABLE IF EXISTS "mrf"."provider_directory_location_archive_stage_test";'
    resolve.assert_awaited_once()
    assert resolve.await_args.args[0] == "provider_directory_location_archive_stage_test"
    assert resolve.await_args.kwargs["source_bit"] == importer.PROVIDER_DIRECTORY_ADDRESS_ARCHIVE_SOURCE_BIT
    assert resolve.await_args.kwargs["priority"] == importer.PROVIDER_DIRECTORY_ADDRESS_ARCHIVE_PRIORITY
    assert resolve.await_args.kwargs["schema"] == "mrf"


@pytest.mark.asyncio
async def test_location_archive_holds_active_dataset_fence_through_resolve(monkeypatch):
    """Archive mutation keeps the complete dataset fence through resolve."""
    fence = importer.ProviderDirectoryArtifactDatasetFence(
        (_artifact_dataset(selected_resources=("Location", "Organization")),)
    )
    events: list[str] = []

    class Stats:
        inserted = 1
        provenance_updates = 0

    fence_lock = AsyncMock(side_effect=lambda *_args: events.append("fence_lock"))
    resolve = AsyncMock(side_effect=lambda *_args, **_kwargs: events.append("resolve") or Stats())
    backfill = AsyncMock(side_effect=lambda *_args, **_kwargs: events.append("backfill") or 2)
    monkeypatch.setattr(importer, "_has_address_canon_functions", AsyncMock(return_value=True))
    monkeypatch.setattr(importer, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(importer.db, "status", AsyncMock(return_value=0))
    monkeypatch.setattr(importer.db, "transaction", lambda: _recording_transaction(events))
    monkeypatch.setattr(
        importer,
        "_lock_and_verify_artifact_dataset_fence",
        fence_lock,
    )
    monkeypatch.setattr(importer, "resolve_into_archive", resolve)
    monkeypatch.setattr(importer, "_backfill_archive_openaddresses_coordinates", backfill)
    fence_token = importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.set(fence)
    try:
        archive_metrics = await importer.publish_provider_directory_location_archive(
            "mrf",
            stage_table="provider_directory_location_archive_stage_test",
        )
    finally:
        importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.reset(fence_token)

    assert archive_metrics["openaddresses_coordinate_backfill_rows"] == 2
    assert events == [
        "transaction-enter",
        "fence_lock",
        "resolve",
        "backfill",
        "transaction-exit",
    ]
    fence_lock.assert_awaited_once_with(fence, importer.db)


@pytest.mark.asyncio
async def test_publish_provider_directory_location_archive_skips_without_archive(monkeypatch):
    monkeypatch.setattr(importer, "_has_address_canon_functions", AsyncMock(return_value=True))

    async def is_table_existing(_schema, table_name):
        return table_name != "address_archive_v2"

    monkeypatch.setattr(importer, "_table_exists", is_table_existing)
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)

    stats = await importer.publish_provider_directory_location_archive("mrf")

    assert stats == {"skipped": True, "reason": "address_archive_v2_unavailable"}
    status.assert_not_awaited()


@pytest.mark.asyncio
async def test_corroboration_skips_without_unified_addresses(
    monkeypatch,
):
    monkeypatch.setattr(importer, "_table_exists", AsyncMock(return_value=False))
    publish = AsyncMock()
    monkeypatch.setattr(importer, "publish_provider_directory_address_corroboration_table", publish)

    published = await importer.publish_provider_directory_address_corroboration_if_available("mrf")

    assert published is False
    publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_corroboration_publishes_with_unified_addresses(
    monkeypatch,
):
    monkeypatch.setattr(importer, "_table_exists", AsyncMock(return_value=True))
    publish = AsyncMock()
    monkeypatch.setattr(importer, "publish_provider_directory_address_corroboration_table", publish)

    published = await importer.publish_provider_directory_address_corroboration_if_available("mrf")

    assert published is True
    publish.assert_awaited_once_with("mrf", refresh_network_catalog=True)


@pytest.mark.asyncio
async def test_process_data_merges_supplemental_retest_sources(monkeypatch, tmp_path):
    retest_path = tmp_path / "retest_results.json"
    retest_path.write_text(
        json.dumps(
            {
                "tested_at": "2026-06-03T17:43:09Z",
                "results": [
                    {
                        "payer_id": 21,
                        "classification": "valid",
                        "org_name": "Supplemental Public Payer",
                        "api_base": "https://supplemental.example/fhir",
                        "status_code": 200,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    upserted_source_rows: list[dict[str, Any]] = []

    async def fake_upsert(model, rows, **_kwargs):
        if model is ProviderDirectorySource:
            upserted_source_rows.extend(rows)
        return len(rows)

    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(importer, "_clear_resource_rows_seen", AsyncMock(return_value=0))
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)

    metrics = await importer.process_data(
        {"context": {}},
        {
            "test": True,
            "probe": False,
            "seed_only": True,
            "retest_results_path": str(retest_path),
        },
    )

    assert metrics["sources_seeded"] == 2
    assert metrics["supplemental_retest_sources_considered"] == 1
    assert {observed_row_map["org_name"] for observed_row_map in upserted_source_rows} == {"Cigna", "Supplemental Public Payer"}
    supplemental = next(observed_row_map for observed_row_map in upserted_source_rows if observed_row_map["org_name"] == "Supplemental Public Payer")
    assert supplemental["api_base"] == "https://supplemental.example/fhir"
    assert supplemental["seed_source"] == "provider-directory-db-retest"


@pytest.mark.asyncio
async def test_process_data_merges_supplemental_catalog_sources(monkeypatch, tmp_path):
    """Verify process data merges supplemental catalog sources."""
    amerihealth_catalog_path = tmp_path / "amerihealth.html"
    amerihealth_catalog_path.write_text(
        """
        <table>
          <tr><td>PlanID</td><td>Plan Name</td><td>Provider Directory API</td></tr>
          <tr>
            <td>5400</td>
            <td>AmeriHealth Caritas District of Columbia</td>
            <td><a href="https://api-ext.amerihealthcaritas.com/5400/provider-api/swagger-ui/">API Documentation</a></td>
          </tr>
        </table>
        """,
        encoding="utf-8",
    )
    contra_costa_catalog_path = tmp_path / "contra-costa.html"
    contra_costa_catalog_path.write_text(
        """
        <p>
          <a href="/?____isexternal=true&splash=https%3A%2F%2Fihyml0v6d9.execute-api.us-east-1.amazonaws.com%2Fhxprod%2Fmetadata">
            Provider Directory API Base URL
          </a>
        </p>
        """,
        encoding="utf-8",
    )
    cms_sma_catalog_path = tmp_path / "cms-sma.csv"
    cms_sma_catalog_path.write_text("State Medicaid Agency Interoperability and Patient Access Endpoint Directory\n", encoding="utf-8")
    upserted_source_rows: list[dict[str, Any]] = []

    async def fake_upsert(model, rows, **_kwargs):
        if model is ProviderDirectorySource:
            upserted_source_rows.extend(rows)
        return len(rows)

    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(importer, "_clear_resource_rows_seen", AsyncMock(return_value=0))
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)

    metrics = await importer.process_data(
        {"context": {}},
        {
            "test": True,
            "probe": False,
            "seed_only": True,
            "include_supplemental_catalogs": True,
            "amerihealth_caritas_catalog_path": str(amerihealth_catalog_path),
            "contra_costa_catalog_path": str(contra_costa_catalog_path),
            "cms_sma_endpoint_directory_path": str(cms_sma_catalog_path),
        },
    )

    assert metrics["sources_seeded"] == 16
    assert metrics["supplemental_catalog_sources_considered"] == 15
    assert metrics["supplemental_catalogs"]["catalogs"][
        "reviewed_provider_directory_candidates"
    ]["rows"] == 8
    assert metrics["supplemental_catalogs"]["catalogs"]["amerihealth_caritas"]["rows"] == 1
    assert metrics["supplemental_catalogs"]["catalogs"]["contra_costa"]["rows"] == 1
    assert metrics["supplemental_catalogs"]["catalogs"]["cms_sma_endpoint_directory"]["rows"] == 0
    assert metrics["supplemental_catalogs"]["catalogs"]["health_partners_plans"]["rows"] == 1
    assert metrics["supplemental_catalogs"]["catalogs"]["aetna_provider_directorydata"]["rows"] == 1
    assert metrics["supplemental_catalogs"]["catalogs"]["provider_directory_blockers"]["rows"] == 3
    amerihealth = next(
        observed_row_map for observed_row_map in upserted_source_rows if observed_row_map["seed_source"] == "amerihealth-caritas-developer-portal"
    )
    assert amerihealth["org_name"] == "AmeriHealth Caritas"
    assert amerihealth["plan_name"] == "AmeriHealth Caritas District of Columbia"
    assert amerihealth["api_base"] == "https://api-ext.amerihealthcaritas.com/5400/provider-api"
    contra_costa = next(
        observed_row_map for observed_row_map in upserted_source_rows if observed_row_map["seed_source"] == "contra-costa-health-developer-page"
    )
    assert contra_costa["org_name"] == "Contra Costa Health Plan"
    assert contra_costa["api_base"] == "https://ihyml0v6d9.execute-api.us-east-1.amazonaws.com/hxprod"
    health_partners_plans = next(
        observed_row_map for observed_row_map in upserted_source_rows if observed_row_map["seed_source"] == "health-partners-plans-fhir-root"
    )
    assert health_partners_plans["org_name"] == "Health Partners Plans"
    assert health_partners_plans["api_base"] == importer.HEALTH_PARTNERS_PLANS_PROVIDER_DIRECTORY_BASE
    aetna = next(
        observed_row_map for observed_row_map in upserted_source_rows if observed_row_map["seed_source"] == "aetna-developer-portal"
    )
    assert aetna["org_name"] == "Aetna"
    assert aetna["api_base"] == importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE
    blockers = [
        observed_row_map for observed_row_map in upserted_source_rows if observed_row_map["seed_source"] == importer.PROVIDER_DIRECTORY_BLOCKER_REGISTRY_SOURCE
    ]
    assert {observed_row_map["org_name"] for observed_row_map in blockers} == {
        "Chorus Community Health Plans (fka Children's Community Health Plan)",
        "First Medical Health Plan, Inc.",
        "Territory of Puerto Rico",
    }
    assert all(observed_row_map["api_base"] is None for observed_row_map in blockers)
    assert all(observed_row_map["metadata_json"]["provider_directory_blocked"] is True for observed_row_map in blockers)


@pytest.mark.asyncio
async def test_process_data_skips_full_refresh_source_catalog_stale_cleanup_without_retest(monkeypatch):
    upserted_source_rows: list[dict[str, Any]] = []

    async def fake_upsert(model, rows, **_kwargs):
        if model is ProviderDirectorySource:
            upserted_source_rows.extend(rows)
        return len(rows)

    cleanup = AsyncMock(return_value={"deleted": {"provider_directory_source": 1}, "protected_source_ids_missing": []})
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(importer, "_clear_resource_rows_seen", AsyncMock(return_value=0))
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)
    monkeypatch.setattr(importer, "_delete_stale_provider_directory_source_catalog", cleanup)

    metrics = await importer.process_data(
        {"context": {}},
        {
            "test": True,
            "probe": False,
            "seed_only": True,
            "full_refresh": True,
            "stale_cleanup": True,
        },
    )

    cleanup.assert_not_awaited()
    assert metrics["sources_seeded"] == 1
    assert metrics["stale_source_rows_deleted"] == {}


@pytest.mark.asyncio
async def test_process_data_reports_full_refresh_source_catalog_stale_cleanup_with_retest(
    monkeypatch,
    tmp_path,
):
    retest_path = tmp_path / "retest_results.json"
    retest_path.write_text(
        json.dumps(
            {
                "tested_at": "2026-06-03T17:43:09Z",
                "results": [
                    {
                        "payer_id": 21,
                        "classification": "valid",
                        "org_name": "Supplemental Public Payer",
                        "api_base": "https://supplemental.example/fhir",
                        "status_code": 200,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    upserted_source_rows: list[dict[str, Any]] = []

    async def fake_upsert(model, rows, **_kwargs):
        if model is ProviderDirectorySource:
            upserted_source_rows.extend(rows)
        return len(rows)

    cleanup = AsyncMock(return_value={"deleted": {"provider_directory_source": 1}, "protected_source_ids_missing": []})
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(importer, "_clear_resource_rows_seen", AsyncMock(return_value=0))
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)
    monkeypatch.setattr(importer, "_delete_stale_provider_directory_source_catalog", cleanup)

    metrics = await importer.process_data(
        {"context": {}},
        {
            "test": True,
            "probe": False,
            "seed_only": True,
            "full_refresh": True,
            "stale_cleanup": True,
            "retest_results_path": str(retest_path),
        },
    )

    protected_source_ids = importer._configured_catalog_protected_source_ids({})
    cleanup.assert_awaited_once_with(
        [observed_row_map["source_id"] for observed_row_map in upserted_source_rows],
        protected_source_ids=protected_source_ids,
    )
    assert metrics["sources_seeded"] == 2
    assert metrics["stale_source_rows_deleted"] == {"provider_directory_source": 1}
    assert metrics["protected_source_ids"] == protected_source_ids
    assert metrics["protected_source_ids_missing"] == []


@pytest.mark.asyncio
async def test_non_fhir_retest_requires_valid_probe(monkeypatch, tmp_path):
    retest_path = tmp_path / "retest_results.json"
    retest_path.write_text(
        json.dumps(
            {
                "tested_at": "2026-06-03T17:43:09Z",
                "results": [
                    {
                        "payer_id": 31,
                        "classification": "valid_non_fhir",
                        "org_name": "Supplemental Public App",
                        "api_base": "https://public-app.example/providers",
                        "status_code": 200,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    imported_sources: list[dict[str, Any]] = []

    async def fake_upsert(_model, rows, **_kwargs):
        return len(rows)

    async def fake_import_resources(sources, **_kwargs):
        imported_sources.extend(sources)
        return {"Location": 1}

    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(importer, "_clear_resource_rows_seen", AsyncMock(return_value=0))
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)
    monkeypatch.setattr(importer, "_import_resources", fake_import_resources)

    metrics = await importer.process_data(
        {"context": {}},
        {
            "test": True,
            "probe": False,
            "import_resources": True,
            "resources": "Location",
            "publish_artifacts": False,
            "retest_results_path": str(retest_path),
        },
    )

    assert metrics["sources_seeded"] == 2
    assert metrics["supplemental_retest_sources_considered"] == 1
    assert [tested_source_map["org_name"] for tested_source_map in imported_sources] == ["Cigna"]
    assert metrics["sources_import_attempted"] == 1
    assert metrics["source_import_sources_considered"] == 2
    assert metrics["source_import_sources_selected"] == 1
    assert metrics["source_import_skipped_validation_status"] == 1


@pytest.mark.asyncio
async def test_process_data_stamps_locations_and_publishes_corroboration_view_when_requested(monkeypatch):
    """Verify this provider-directory regression contract."""
    import_resources = AsyncMock(return_value={"Location": 2})
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(importer, "_clear_resource_rows_seen", AsyncMock(return_value=0))
    monkeypatch.setattr(importer, "_upsert_rows", AsyncMock(return_value=1))
    monkeypatch.setattr(importer, "_import_resources", import_resources)
    monkeypatch.setattr(
        importer,
        "backfill_provider_directory_location_contacts",
        AsyncMock(return_value={"location_contact_rows_updated": 5}),
    )
    monkeypatch.setattr(importer, "backfill_provider_directory_location_coordinates", AsyncMock(return_value=6))
    monkeypatch.setattr(importer, "backfill_provider_directory_resource_id_npis", AsyncMock(return_value={"Practitioner": 2, "Organization": 1}))
    monkeypatch.setattr(importer, "publish_provider_directory_location_address_keys", AsyncMock(return_value=3))
    monkeypatch.setattr(
        importer,
        "publish_provider_directory_location_archive",
        AsyncMock(return_value={"inserted": 4, "provenance_updates": 1}),
    )
    monkeypatch.setattr(importer, "publish_provider_directory_address_overlay", AsyncMock(return_value={}))
    monkeypatch.setattr(importer, "publish_provider_directory_network_catalog", AsyncMock(return_value={"rows": 8}))
    monkeypatch.setattr(
        importer,
        "publish_provider_directory_address_corroboration_if_available",
        AsyncMock(return_value=True),
    )

    metrics = await importer.process_data(
        {"context": {}},
        {
            "test": True,
            "run_id": "run_full",
            "probe": False,
            "import_resources": True,
            "resources": "Location",
            "publish_artifacts": True,
            "publish_corroboration": True,
            "bulk_export_max_pending_seconds": 23456,
        },
    )
    assert metrics["resource_rows"] == {"Location": 2}
    assert metrics["bulk_export_max_pending_seconds"] == 23456
    assert (
        import_resources.await_args.kwargs["bulk_export_max_pending_seconds"]
        == 23456
    )
    assert metrics["sources_import_attempted"] == 1
    assert metrics["location_contacts_backfilled"] == {"location_contact_rows_updated": 5}
    assert metrics["location_coordinates_backfilled"] == 6
    assert metrics["resource_id_npis_backfilled"] == {"Practitioner": 2, "Organization": 1}
    assert metrics["location_address_keys_stamped"] == 3
    assert metrics["location_archive"] == {"inserted": 4, "provenance_updates": 1}
    assert metrics["network_catalog"] == {"rows": 8}
    assert metrics["publish_corroboration"] is True
    assert metrics["ptg_corroboration_view_published"] is True
    importer.backfill_provider_directory_location_contacts.assert_awaited_once()
    importer.backfill_provider_directory_location_coordinates.assert_awaited_once()
    assert importer.backfill_provider_directory_location_coordinates.await_args.kwargs["run_id"] == "run_full"
    importer.backfill_provider_directory_resource_id_npis.assert_awaited_once()
    assert importer.backfill_provider_directory_resource_id_npis.await_args.kwargs["run_id"] == "run_full"
    importer.publish_provider_directory_location_address_keys.assert_awaited_once()
    assert importer.publish_provider_directory_location_address_keys.await_args.kwargs["run_id"] == "run_full"
    importer.publish_provider_directory_location_archive.assert_awaited_once()
    assert importer.publish_provider_directory_location_archive.await_args.kwargs["run_id"] == "run_full"
    importer.publish_provider_directory_address_overlay.assert_awaited_once()
    assert importer.publish_provider_directory_address_overlay.await_args.kwargs["run_id"] == "run_full"
    importer.publish_provider_directory_network_catalog.assert_awaited_once()
    assert importer.publish_provider_directory_network_catalog.await_args.kwargs["run_id"] == "run_full"
    importer.publish_provider_directory_address_corroboration_if_available.assert_awaited_once()
    assert (
        importer.publish_provider_directory_address_corroboration_if_available.await_args.kwargs[
            "refresh_network_catalog"
        ]
        is False
    )


@pytest.mark.asyncio
async def test_main_forwards_publish_artifact_targets(monkeypatch):
    process_data_mock = AsyncMock(return_value={"published": True})
    monkeypatch.setattr(importer, "startup", AsyncMock())
    monkeypatch.setattr(importer, "process_data", process_data_mock)
    monkeypatch.setattr(importer, "shutdown", AsyncMock())

    result = await importer.main(
        test_mode=True,
        publish_artifacts_only=True,
        publish_artifacts_targets="corroboration",
        bulk_export_max_pending_seconds=12345,
    )

    assert result == {"published": True}
    assert process_data_mock.await_args.args[1]["publish_artifacts_targets"] == "corroboration"
    assert (
        process_data_mock.await_args.args[1]["bulk_export_max_pending_seconds"]
        == 12345
    )


def _stub_artifact_dataset_scope(monkeypatch) -> None:
    @contextlib.asynccontextmanager
    async def dataset_scope(**_kwargs):
        yield importer.ProviderDirectoryArtifactDatasetFence(())

    monkeypatch.setattr(
        importer,
        "_provider_directory_artifact_dataset_scope",
        dataset_scope,
    )
    monkeypatch.setattr(
        importer,
        "_resolve_provider_directory_artifact_datasets",
        AsyncMock(return_value=importer.ProviderDirectoryArtifactDatasetFence(())),
    )


@pytest.mark.asyncio
async def test_process_data_publish_artifacts_only_does_not_scope_to_empty_run(monkeypatch):
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    _stub_artifact_dataset_scope(monkeypatch)
    artifact_mock_map = {
        "backfill_provider_directory_location_contacts": AsyncMock(
            return_value={"location_contact_rows_updated": 0}
        ),
        "backfill_provider_directory_location_coordinates": AsyncMock(return_value=0),
        "backfill_provider_directory_resource_id_npis": AsyncMock(return_value={"Practitioner": 0, "Organization": 0}),
        "publish_provider_directory_location_address_keys": AsyncMock(return_value=0),
        "publish_provider_directory_location_archive": AsyncMock(return_value={"inserted": 0}),
        "publish_provider_directory_address_overlay": AsyncMock(return_value={"rows": 7}),
        "publish_provider_directory_network_catalog": AsyncMock(return_value={"rows": 8}),
        "publish_provider_directory_address_corroboration_table": AsyncMock(
            return_value={"rows": 9}
        ),
    }
    for artifact_name, artifact_mock in artifact_mock_map.items():
        monkeypatch.setattr(importer, artifact_name, artifact_mock)
    monkeypatch.setattr(importer, "_table_exists", AsyncMock(return_value=True))

    metrics = await importer.process_data(
        {"context": {}},
        {
            "test": True,
            "run_id": "run_artifact_only",
            "publish_artifacts_only": True,
            "publish_corroboration": True,
        },
    )

    assert metrics["publish_artifacts_only"] is True
    assert metrics["address_overlay"] == {"rows": 7}
    assert metrics["network_catalog"] == {"rows": 8}
    assert metrics["ptg_corroboration_view_published"] is True
    for artifact_name in (
        "backfill_provider_directory_location_coordinates",
        "backfill_provider_directory_resource_id_npis",
        "publish_provider_directory_location_address_keys",
        "publish_provider_directory_location_archive",
        "publish_provider_directory_address_overlay",
        "publish_provider_directory_network_catalog",
    ):
        artifact_mock_map[artifact_name].assert_awaited_once()
        assert artifact_mock_map[artifact_name].await_args.kwargs["run_id"] is None
    corroboration_publish = artifact_mock_map[
        "publish_provider_directory_address_corroboration_table"
    ]
    corroboration_publish.assert_awaited_once()
    assert corroboration_publish.await_args.kwargs["refresh_network_catalog"] is False
    assert corroboration_publish.await_args.kwargs["defer_cutover"] is True


def test_provider_directory_publish_artifact_targets_parse_aliases():
    assert importer._provider_directory_publish_artifact_targets(None) is None
    assert importer._provider_directory_publish_artifact_targets("network, ptg-corroboration") == {
        "dataset_affiliation_organization",
        "dataset_network_plan",
        "network_catalog",
        "corroboration",
    }
    assert importer._provider_directory_publish_artifact_targets(["addresses"]) == {
        "location_contacts",
        "location_coordinates",
        "resource_id_npis",
        "location_address_keys",
        "location_archive",
        "address_overlay",
    }

    with pytest.raises(ValueError, match="Unsupported Provider Directory publish_artifacts_targets"):
        importer._provider_directory_publish_artifact_targets("bad-stage")


def _mock_artifact_only_publishers(monkeypatch) -> dict[str, AsyncMock]:
    _stub_artifact_dataset_scope(monkeypatch)
    artifact_publish_mocks_by_name = {
        "backfill_provider_directory_location_contacts": AsyncMock(
            return_value={"location_contact_rows_updated": 0}
        ),
        "backfill_provider_directory_location_coordinates": AsyncMock(return_value=0),
        "publish_provider_directory_location_address_keys": AsyncMock(return_value=0),
        "publish_provider_directory_location_archive": AsyncMock(return_value={"inserted": 0}),
        "publish_provider_directory_address_overlay": AsyncMock(return_value={"rows": 0}),
        "publish_provider_directory_network_catalog": AsyncMock(return_value={"rows": 8}),
        "publish_provider_directory_address_corroboration_if_available": AsyncMock(return_value=True),
    }
    for publisher_name, publisher_mock in artifact_publish_mocks_by_name.items():
        monkeypatch.setattr(importer, publisher_name, publisher_mock)
    return artifact_publish_mocks_by_name


@pytest.mark.asyncio
async def test_process_data_publish_artifacts_only_can_target_network_catalog(monkeypatch):
    """Only the requested network catalog artifact should run."""
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    artifact_publish_mocks_by_name = _mock_artifact_only_publishers(monkeypatch)

    metrics = await importer.process_data(
        {"context": {}},
        {
            "test": True,
            "run_id": "run_artifact_only",
            "publish_artifacts_only": True,
            "publish_artifacts_targets": "network_catalog",
            "publish_corroboration": True,
        },
    )

    assert metrics["publish_artifacts_targets"] == ["network_catalog"]
    skipped_metric_names = (
        "location_contacts_backfilled",
        "location_coordinates_backfilled",
        "resource_id_npis_backfilled",
        "location_address_keys_stamped",
        "location_archive",
        "address_overlay",
    )
    for skipped_metric_name in skipped_metric_names:
        assert metrics[skipped_metric_name] == {"skipped": True, "reason": "target_not_requested"}
    assert metrics["network_catalog"] == {"rows": 8}
    assert metrics["ptg_corroboration_view_published"] is False
    assert metrics["ptg_corroboration_view_skipped"] == {"skipped": True, "reason": "target_not_requested"}
    skipped_stage_mocks = (
        artifact_publish_mocks_by_name["backfill_provider_directory_location_contacts"],
        artifact_publish_mocks_by_name["backfill_provider_directory_location_coordinates"],
        artifact_publish_mocks_by_name["publish_provider_directory_location_address_keys"],
        artifact_publish_mocks_by_name["publish_provider_directory_location_archive"],
        artifact_publish_mocks_by_name["publish_provider_directory_address_overlay"],
    )
    for skipped_stage_mock in skipped_stage_mocks:
        skipped_stage_mock.assert_not_awaited()
    artifact_publish_mocks_by_name["publish_provider_directory_network_catalog"].assert_awaited_once()
    assert artifact_publish_mocks_by_name["publish_provider_directory_network_catalog"].await_args.kwargs["run_id"] is None
    artifact_publish_mocks_by_name["publish_provider_directory_address_corroboration_if_available"].assert_not_awaited()


@pytest.mark.asyncio
async def test_probe_only_refresh_skips_artifact_publish(monkeypatch):
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(importer, "_clear_resource_rows_seen", AsyncMock(return_value=0))
    monkeypatch.setattr(importer, "_upsert_rows", AsyncMock(return_value=1))
    monkeypatch.setattr(importer, "_run_source_probe_batch", AsyncMock(return_value=(1, 1, {"source_a"})))
    publish_artifacts = AsyncMock(return_value={})
    monkeypatch.setattr(importer, "_publish_provider_directory_artifacts", publish_artifacts)

    metrics = await importer.process_data(
        {"context": {}},
        {
            "test": True,
            "full_refresh": True,
            "probe": True,
            "import_resources": False,
        },
    )

    assert metrics["publish_artifacts"] is False
    assert metrics["resource_rows"] == {}
    publish_artifacts.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_data_skips_artifact_publish_for_targeted_resource_import(monkeypatch):
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(importer, "_upsert_rows", AsyncMock(return_value=1))
    monkeypatch.setattr(importer, "_import_resources", AsyncMock(return_value={"PractitionerRole": 2}))
    monkeypatch.setattr(
        importer,
        "backfill_provider_directory_location_contacts",
        AsyncMock(return_value={"location_contact_rows_updated": 5}),
    )
    monkeypatch.setattr(importer, "backfill_provider_directory_location_coordinates", AsyncMock(return_value=0))
    monkeypatch.setattr(importer, "publish_provider_directory_location_address_keys", AsyncMock(return_value=3))
    monkeypatch.setattr(
        importer,
        "publish_provider_directory_location_archive",
        AsyncMock(return_value={"inserted": 4}),
    )
    monkeypatch.setattr(
        importer,
        "publish_provider_directory_address_corroboration_if_available",
        AsyncMock(return_value=True),
    )

    metrics = await importer.process_data(
        {"context": {}},
        {
            "test": True,
            "probe": False,
            "import_resources": True,
            "resources": "PractitionerRole",
        },
    )

    assert metrics["publish_artifacts"] is False
    assert metrics["location_contacts_backfilled"] == {
        "skipped": True,
        "reason": "publish_artifacts_disabled",
    }
    assert metrics["location_coordinates_backfilled"] == {
        "skipped": True,
        "reason": "publish_artifacts_disabled",
    }
    assert metrics["location_address_keys_stamped"] == 0
    assert metrics["location_archive"] == {"skipped": True, "reason": "publish_artifacts_disabled"}
    assert metrics["ptg_corroboration_view_published"] is False
    importer.backfill_provider_directory_location_contacts.assert_not_awaited()
    importer.backfill_provider_directory_location_coordinates.assert_not_awaited()
    importer.publish_provider_directory_location_address_keys.assert_not_awaited()
    importer.publish_provider_directory_location_archive.assert_not_awaited()
    importer.publish_provider_directory_address_corroboration_if_available.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_data_skips_artifact_publish_when_required_fetches_are_bounded(monkeypatch):
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(importer, "_clear_resource_rows_seen", AsyncMock(return_value=0))
    monkeypatch.setattr(importer, "_upsert_rows", AsyncMock(return_value=1))
    publish_artifacts = AsyncMock(return_value={})
    monkeypatch.setattr(importer, "_publish_provider_directory_artifacts", publish_artifacts)

    async def fake_import_resources(*_args, resource_fetch_stats, **_kwargs):
        for resource_type in ("Practitioner", "Location", "PractitionerRole"):
            resource_fetch_stats[resource_type] = {
                "sources_attempted": 1,
                "sources_completed": 0,
                "sources_bounded": 1,
                "sources_failed": 0,
                "sources_empty": 0,
                "bulk_export_sources": 0,
                "pages_fetched": 1,
                "rows_fetched": 2,
            }
        return {"Practitioner": 2, "Location": 2, "PractitionerRole": 2}

    monkeypatch.setattr(importer, "_import_resources", fake_import_resources)

    metrics = await importer.process_data(
        {"context": {}},
        {
            "test": True,
            "probe": False,
            "import_resources": True,
            "publish_artifacts": True,
            "resources": "Practitioner,Location,PractitionerRole",
        },
    )

    assert metrics["publishable_artifact_source_ids"] == []
    assert metrics["address_overlay"]["reason"] == "critical_resource_fetch_incomplete"
    assert metrics["artifact_publish_required_resources"] == [
        "PractitionerRole",
        "Practitioner",
        "Location",
    ]
    publish_artifacts.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_data_source_id_scopes_seed_probe_and_resource_import(monkeypatch):
    upserted_source_rows: list[dict[str, Any]] = []

    async def fake_upsert(model, source_rows_to_upsert, **_kwargs):
        if model is ProviderDirectorySource:
            upserted_source_rows.extend(source_rows_to_upsert)
        return len(source_rows_to_upsert)

    def fake_source_row(seed_row):
        source_id = seed_row["id"]
        return {
            "source_id": source_id,
            "org_name": f"Source {source_id}",
            "api_base": f"https://{source_id}.example.test/fhir",
            "canonical_api_base": f"https://{source_id}.example.test/fhir",
            "auth_type": "none",
            "last_validated_status": "valid",
            "metadata_json": {},
        }

    probe_sources = AsyncMock(return_value=(1, 1, {"source_b"}))
    import_resources = AsyncMock(return_value={"Practitioner": 1})
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(importer, "_clear_resource_rows_seen", AsyncMock(return_value=0))
    monkeypatch.setattr(importer, "_resolve_seed_db", lambda *_args, **_kwargs: ("/tmp/provider-directory.db", None))
    monkeypatch.setattr(
        importer,
        "_seed_rows_from_sqlite",
        lambda *_args, **_kwargs: [{"id": "source_a"}, {"id": "source_b"}],
    )
    monkeypatch.setattr(importer, "_source_row_from_seed", fake_source_row)
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)
    monkeypatch.setattr(importer, "_run_source_probe_batch", probe_sources)
    monkeypatch.setattr(importer, "_import_resources", import_resources)

    metrics = await importer.process_data(
        {"context": {}},
        {
            "seed_db_path": "/tmp/provider-directory.db",
            "source_id": ["source_b"],
            "probe": True,
            "import_resources": True,
            "resources": "Practitioner",
            "publish_artifacts": False,
        },
    )

    assert metrics["sources_seeded"] == 1
    assert [source_row["source_id"] for source_row in upserted_source_rows] == ["source_b"]
    probe_sources.assert_awaited_once()
    assert [source_row["source_id"] for source_row in probe_sources.await_args.args[0]] == ["source_b"]
    import_resources.assert_awaited_once()
    assert [source_row["source_id"] for source_row in import_resources.await_args.args[0]] == ["source_b"]
    assert metrics["sources_import_attempted"] == 1


@pytest.mark.asyncio
async def test_process_data_source_id_missing_from_catalog_fails_fast(monkeypatch):
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(importer, "_clear_resource_rows_seen", AsyncMock(return_value=0))
    monkeypatch.setattr(importer, "_resolve_seed_db", lambda *_args, **_kwargs: ("/tmp/provider-directory.db", None))
    monkeypatch.setattr(importer, "_seed_rows_from_sqlite", lambda *_args, **_kwargs: [{"id": "source_a"}])
    monkeypatch.setattr(
        importer,
        "_source_row_from_seed",
        lambda _row: {
            "source_id": "source_a",
            "org_name": "Source A",
            "api_base": "https://source-a.example.test/fhir",
            "canonical_api_base": "https://source-a.example.test/fhir",
            "auth_type": "none",
            "last_validated_status": "valid",
            "metadata_json": {},
        },
    )
    monkeypatch.setattr(importer, "_upsert_rows", AsyncMock())
    monkeypatch.setattr(importer, "_run_source_probe_batch", AsyncMock())
    monkeypatch.setattr(importer, "_import_resources", AsyncMock())

    with pytest.raises(ValueError, match="source_id not found"):
        await importer.process_data(
            {"context": {}},
            {
                "seed_db_path": "/tmp/provider-directory.db",
                "source_id": ["source_missing"],
                "probe": True,
                "import_resources": True,
            },
        )

    importer._upsert_rows.assert_not_awaited()
    importer._run_source_probe_batch.assert_not_awaited()
    importer._import_resources.assert_not_awaited()


@pytest.mark.asyncio
async def test_explicit_source_import_fails_when_requested_source_is_not_selected(
    monkeypatch,
):
    requested_source_id = "pdfhir_0f81c146991b27031b1ec366"
    process_context_map = {"context": {}}
    import_resources = AsyncMock()
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(importer, "_clear_resource_rows_seen", AsyncMock(return_value=0))
    monkeypatch.setattr(
        importer,
        "_resolve_seed_db",
        lambda *_args, **_kwargs: ("fixture.db", None),
    )
    monkeypatch.setattr(
        importer,
        "_seed_rows_from_sqlite",
        lambda *_args, **_kwargs: [{"id": requested_source_id}],
    )
    monkeypatch.setattr(
        importer,
        "_source_row_from_seed",
        lambda _seed_row: {
            "source_id": requested_source_id,
            "org_name": "Requested source",
            "api_base": "https://payer.example/fhir",
            "canonical_api_base": "https://payer.example/fhir",
            "auth_type": "none",
            "last_validated_status": "valid_non_fhir",
            "metadata_json": {},
        },
    )
    monkeypatch.setattr(importer, "_upsert_rows", AsyncMock(return_value=1))
    monkeypatch.setattr(importer, "_import_resources", import_resources)

    with pytest.raises(
        RuntimeError,
        match=importer.REQUESTED_SOURCE_IMPORT_EMPTY_ERROR,
    ) as raised_error:
        await importer.process_data(
            process_context_map,
            {
                "seed_db_path": "fixture.db",
                "source_ids": [requested_source_id],
                "probe": False,
                "import_resources": True,
                "include_auth_required": True,
                "publish_artifacts": False,
            },
        )

    assert "validation_status" in str(raised_error.value)
    assert process_context_map["context"]["audit"][
        "source_import_skipped_validation_status"
    ] == 1
    import_resources.assert_not_awaited()


def _stub_checkpoint_retry_process(monkeypatch, requested_source_id):
    import_resources = AsyncMock(return_value={"Location": 1})
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(importer, "_clear_resource_rows_seen", AsyncMock(return_value=0))
    monkeypatch.setattr(
        importer,
        "_resolve_seed_db",
        lambda *_args, **_kwargs: ("fixture.db", None),
    )
    monkeypatch.setattr(
        importer,
        "_seed_rows_from_sqlite",
        lambda *_args, **_kwargs: [{"id": requested_source_id}],
    )
    monkeypatch.setattr(
        importer,
        "_source_row_from_seed",
        lambda _seed_row: {
            "source_id": requested_source_id,
            "org_name": "Checkpointed source",
            "api_base": "https://payer.example/fhir",
            "canonical_api_base": "https://payer.example/fhir",
            "auth_type": "oauth2",
            "last_validated_status": "auth_required",
            "metadata_json": {},
        },
    )
    monkeypatch.setattr(importer, "_upsert_rows", AsyncMock(return_value=1))
    monkeypatch.setattr(
        importer,
        "_run_source_probe_batch",
        AsyncMock(return_value=(1, 0, set())),
    )
    monkeypatch.setattr(importer, "_import_resources", import_resources)
    return import_resources


def _checkpoint_retry_task(requested_source_id):
    """Return the unbounded retry task shared by checkpoint regressions."""
    return {
        "run_id": "run_retry",
        "retry_of_run_id": "run_parent",
        "provider_directory_pagination_root_run_id": "run_root",
        "seed_db_path": "fixture.db",
        "source_ids": [requested_source_id],
        "probe": True,
        "import_resources": True,
        "resources": "Location",
        "full_refresh": True,
        "resource_limit": 0,
        "page_limit": 0,
        "stream_batch_size": 5000,
        "stale_cleanup": False,
        "publish_artifacts": False,
    }


@pytest.mark.asyncio
async def test_checkpoint_retry_keeps_exact_source_after_transient_probe_rejection(
    monkeypatch,
):
    requested_source_id = "pdfhir_checkpointed"
    import_resources = _stub_checkpoint_retry_process(
        monkeypatch,
        requested_source_id,
    )

    metrics = await importer.process_data(
        {"context": {}},
        _checkpoint_retry_task(requested_source_id),
    )

    assert metrics["source_import_sources_selected_checkpoint_retry"] == 1
    assert metrics["source_import_sources_selected_live_probe_valid"] == 0
    import_resources.assert_awaited_once()
    assert import_resources.await_args.args[0][0]["source_id"] == requested_source_id
    assert import_resources.await_args.kwargs["require_complete_resources"] is True


@pytest.mark.asyncio
async def test_checkpoint_retry_surfaces_quota_deadline_before_resume_failure(
    monkeypatch,
):
    requested_source_id = "pdfhir_checkpointed"
    _stub_checkpoint_retry_process(monkeypatch, requested_source_id)
    retry_not_before = "2026-07-12T00:01:00Z"

    async def fake_import_resources(
        _sources,
        *,
        pagination_resume_required,
        resource_retry_not_before,
        **_kwargs,
    ):
        pagination_resume_required.add(f"{requested_source_id}:Location")
        resource_retry_not_before["Location"] = retry_not_before
        return {"Location": 0}

    monkeypatch.setattr(importer, "_import_resources", fake_import_resources)
    process_context_map = {"context": {}}

    with pytest.raises(RuntimeError, match=importer.PAGINATION_RESUME_REQUIRED_ERROR):
        await importer.process_data(
            process_context_map,
            _checkpoint_retry_task(requested_source_id),
        )

    audit_dict = process_context_map["context"]["audit"]
    assert audit_dict[importer.SOURCE_RETRY_NOT_BEFORE_METRIC] == retry_not_before
    assert audit_dict["provider_directory_retry_not_before_by_resource"] == {
        "Location": retry_not_before
    }


@pytest.mark.asyncio
async def test_checkpoint_retry_persists_completion_before_resume_failure(
    monkeypatch,
):
    requested_source_id = "pdfhir_checkpointed"
    _stub_checkpoint_retry_process(monkeypatch, requested_source_id)

    async def fake_import_resources(
        _sources,
        *,
        pagination_resume_required,
        resource_completion,
        **_kwargs,
    ):
        resource_completion["Organization"] = {requested_source_id}
        pagination_resume_required.add(f"{requested_source_id}:Location")
        return {"Organization": 1, "Location": 0}

    monkeypatch.setattr(importer, "_import_resources", fake_import_resources)
    process_context_map = {"context": {}}

    with pytest.raises(RuntimeError, match=importer.PAGINATION_RESUME_REQUIRED_ERROR):
        await importer.process_data(
            process_context_map,
            _checkpoint_retry_task(requested_source_id),
        )

    audit_dict = process_context_map["context"]["audit"]
    assert audit_dict["resource_fetch_completed_source_ids"] == {
        "Organization": [requested_source_id]
    }
    assert audit_dict["pagination_resume_required"] == [
        f"{requested_source_id}:Location"
    ]


@pytest.mark.asyncio
async def test_process_data_reports_completed_resource_when_next_resource_starts(
    monkeypatch,
):
    """Progress exposes a completed resource before the next resource finishes."""
    requested_source_id = "pdfhir_checkpointed"
    _stub_checkpoint_retry_process(monkeypatch, requested_source_id)
    completion_snapshots: list[dict[str, list[str]]] = []

    async def fake_mark_progress(
        _run_id,
        *,
        phase,
        details=None,
        metrics=None,
        **_kwargs,
    ):
        active_groups = (
            details.get("active_source_groups", [])
            if isinstance(details, dict)
            else []
        )
        if (
            phase == "provider-directory importing resources"
            and active_groups
            and active_groups[0].get("current_resource") == "Organization"
        ):
            completion_snapshots.append(
                dict(metrics.get("resource_fetch_completed_source_ids", {}))
            )

    async def fake_import_resources(
        _sources,
        *,
        resource_completion,
        progress_callback,
        **_kwargs,
    ):
        await progress_callback(
            0,
            1,
            {"InsurancePlan": 1413, "Organization": 0},
            {
                "active_source_groups": [
                    {
                        "sample_source_id": requested_source_id,
                        "current_resource": "InsurancePlan",
                    }
                ]
            },
        )
        resource_completion["InsurancePlan"] = {requested_source_id}
        await progress_callback(
            0,
            1,
            {"InsurancePlan": 1413, "Organization": 0},
            {
                "active_source_groups": [
                    {
                        "sample_source_id": requested_source_id,
                        "current_resource": "Organization",
                    }
                ]
            },
        )
        return {"InsurancePlan": 1413, "Organization": 0}

    monkeypatch.setattr(importer, "_mark_provider_directory_progress", fake_mark_progress)
    monkeypatch.setattr(importer, "_import_resources", fake_import_resources)
    task = _checkpoint_retry_task(requested_source_id)
    task["resources"] = "InsurancePlan,Organization"

    metrics = await importer.process_data({"context": {}}, task)

    assert completion_snapshots == [
        {"InsurancePlan": [requested_source_id]}
    ]
    assert metrics["resource_fetch_completed_source_ids"] == {
        "InsurancePlan": [requested_source_id]
    }


@pytest.mark.asyncio
async def test_process_data_uses_live_probe_success_over_seed_auth_required_status(monkeypatch):
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(importer, "_resolve_seed_db", lambda *_args, **_kwargs: ("/tmp/provider-directory.db", None))
    monkeypatch.setattr(importer, "_seed_rows_from_sqlite", lambda *_args, **_kwargs: [{"id": "source-a"}])
    monkeypatch.setattr(
        importer,
        "_source_row_from_seed",
        lambda _row: {
            "source_id": "source_a",
            "org_name": "Source A",
            "api_base": "https://example.test/fhir",
            "canonical_api_base": "https://example.test/fhir",
            "auth_type": "oauth2",
            "last_validated_status": "auth_required",
            "metadata_json": {},
        },
    )
    monkeypatch.setattr(importer, "_upsert_rows", AsyncMock(return_value=1))
    monkeypatch.setattr(importer, "_run_source_probe_batch", AsyncMock(return_value=(1, 1, {"source_a"})))
    import_resources = AsyncMock(return_value={"Practitioner": 1})
    monkeypatch.setattr(importer, "_import_resources", import_resources)
    monkeypatch.setattr(importer, "publish_provider_directory_location_address_keys", AsyncMock(return_value=0))
    monkeypatch.setattr(importer, "publish_provider_directory_location_archive", AsyncMock(return_value={"inserted": 0}))
    monkeypatch.setattr(
        importer,
        "publish_provider_directory_address_corroboration_if_available",
        AsyncMock(return_value=True),
    )

    metrics = await importer.process_data(
        {"context": {}},
        {
            "seed_db_path": "/tmp/provider-directory.db",
            "probe": True,
            "import_resources": True,
            "resources": "Practitioner",
        },
    )

    assert metrics["sources_import_attempted"] == 1
    assert metrics["source_import_sources_selected_live_probe_valid"] == 1
    assert metrics["source_import_sources_selected_declared_credentialed"] == 1
    assert metrics["source_import_sources_selected_auth_required_seed"] == 1
    import_resources.assert_awaited_once()
    assert import_resources.await_args.args[0][0]["source_id"] == "source_a"


def test_dedupe_rows_by_primary_key_keeps_last_live_fhir_duplicate():
    rows = [
        {"source_id": "source_a", "resource_id": "prac-1", "full_name": "Old"},
        {"source_id": "source_a", "resource_id": "prac-1", "full_name": "New"},
        {"source_id": "source_a", "resource_id": None, "full_name": "Skip"},
        {"source_id": "source_a", "resource_id": "prac-2", "full_name": "Other"},
    ]

    deduped = importer._dedupe_rows_by_primary_key(["source_id", "resource_id"], rows)

    assert deduped == [
        {"source_id": "source_a", "resource_id": "prac-1", "full_name": "New"},
        {"source_id": "source_a", "resource_id": "prac-2", "full_name": "Other"},
    ]


def test_max_rows_per_statement_stays_under_asyncpg_parameter_limit():
    assert importer._max_rows_per_statement(1) == 500
    assert importer._max_rows_per_statement(100) == 300
    assert importer._max_rows_per_statement(40000) == 1


class _FakeCopyDriver:
    def __init__(self):
        self.calls = []

    async def copy_records_to_table(self, table, **kwargs):
        self.calls.append(
            {
                "table": table,
                "schema_name": kwargs.get("schema_name"),
                "columns": list(kwargs["columns"]),
                "records": list(kwargs["records"]),
            }
        )


class _FakeAcquireConnection:
    def __init__(self):
        self.driver = _FakeCopyDriver()
        self.raw_connection = type("RawConnection", (), {"driver_connection": self.driver})()
        self.statements = []

    async def status(self, sql, **_params):
        self.statements.append(sql)
        return 0


class _FakeAcquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.mark.asyncio
async def test_copy_upsert_rows_uses_temp_copy_and_changed_predicate(monkeypatch):
    conn = _FakeAcquireConnection()
    monkeypatch.setattr(importer.db, "acquire", lambda: _FakeAcquire(conn))
    monkeypatch.setattr(importer, "_stage_table_name", lambda: "pd_stage_test")

    table = ProviderDirectoryLocation.__table__
    columns = [column.name for column in table.columns]
    observed_row_map = {column: None for column in columns}
    observed_row_map.update(
        {
            "source_id": "source_a",
            "resource_id": "loc-1",
            "resource_url": "https://example.test/fhir/Location/loc-1",
            "telecom": [{"system": "phone", "value": "555\x001212"}],
            "last_seen_run_id": "run_1",
        }
    )

    written = await importer._copy_upsert_rows(
        ProviderDirectoryLocation,
        [observed_row_map],
        columns,
        ["source_id", "resource_id"],
        skip_unchanged=True,
    )

    assert written == 1
    assert conn.driver.calls[0]["table"] == "pd_stage_test"
    assert conn.driver.calls[0]["columns"] == columns
    copied = conn.driver.calls[0]["records"][0]
    assert json.loads(copied[columns.index("telecom")]) == [{"system": "phone", "value": "5551212"}]
    assert "CREATE TEMP TABLE \"pd_stage_test\"" in conn.statements[0]
    assert 'ANALYZE "pd_stage_test"' in conn.statements[1]
    assert 'FROM "pd_stage_test" AS stage_row' in conn.statements[2]
    assert 'LEFT JOIN "mrf"."provider_directory_location" AS target_row' in conn.statements[2]
    assert 'target_row."source_id" IS NULL' in conn.statements[2]
    assert 'ON CONFLICT ("source_id", "resource_id") DO UPDATE SET' in conn.statements[2]
    assert (
        '"address_key" = CASE WHEN EXCLUDED."address_key" IS NOT NULL THEN EXCLUDED."address_key" '
        'WHEN "provider_directory_location"."first_line" IS DISTINCT FROM EXCLUDED."first_line"'
    ) in conn.statements[2]
    assert 'ELSE "provider_directory_location"."address_key" END' in conn.statements[2]

    where_sql = importer._copy_upsert_changed_where_sql(
        table,
        columns,
        ["source_id", "resource_id"],
    )
    assert '"provider_directory_location"."telecom"::jsonb IS DISTINCT FROM EXCLUDED."telecom"::jsonb' in where_sql
    assert "last_seen_run_id" not in where_sql
    assert "observed_at" not in where_sql
    assert "updated_at" not in where_sql


@pytest.mark.asyncio
async def test_copy_mark_resource_rows_seen_uses_distinct_stage_merge(monkeypatch):
    conn = _FakeAcquireConnection()
    monkeypatch.setattr(importer.db, "acquire", lambda: _FakeAcquire(conn))
    monkeypatch.setattr(importer, "_stage_table_name", lambda: "pd_seen_test")

    written = await importer._copy_mark_resource_rows_seen(
        "Location",
        [("source_a", "loc-1"), ("source_a", "loc-1"), ("source_a", "loc-2")],
        "run_1",
    )

    assert written == 3
    assert conn.driver.calls[0]["table"] == "pd_seen_test"
    assert conn.driver.calls[0]["columns"] == ["run_id", "resource_type", "source_id", "resource_id"]
    assert conn.driver.calls[0]["records"][0] == ("run_1", "Location", "source_a", "loc-1")
    assert "CREATE TEMP TABLE \"pd_seen_test\"" in conn.statements[0]
    assert "SELECT DISTINCT" in conn.statements[1]
    assert "ON CONFLICT (run_id, resource_type, source_id, resource_id) DO NOTHING" in conn.statements[1]


@pytest.mark.asyncio
async def test_copy_mark_rows_seen_appends_without_indexed_merge(monkeypatch):
    conn = _FakeAcquireConnection()
    monkeypatch.setattr(importer.db, "acquire", lambda: _FakeAcquire(conn))

    written = await importer._copy_mark_resource_rows_seen(
        "Location",
        [("source_a", "loc-1"), ("source_a", "loc-2")],
        "run_1",
        seen_table="provider_directory_import_seen_stage_test",
    )

    assert written == 2
    assert conn.driver.calls == [
        {
            "table": "provider_directory_import_seen_stage_test",
            "schema_name": "mrf",
            "columns": ["run_id", "resource_type", "source_id", "resource_id"],
            "records": [
                ("run_1", "Location", "source_a", "loc-1"),
                ("run_1", "Location", "source_a", "loc-2"),
            ],
        }
    ]
    assert conn.statements == []


@pytest.mark.asyncio
async def test_import_resources_can_preserve_seen_stage_for_publish(monkeypatch):
    ensure_stage = AsyncMock(return_value="provider_directory_import_seen_stage_test")
    drop_stage = AsyncMock()
    monkeypatch.setattr(importer, "_ensure_provider_directory_import_seen_stage_table", ensure_stage)
    monkeypatch.setattr(importer, "_drop_provider_directory_import_seen_stage_table", drop_stage)

    counts = await importer._import_resources(
        [],
        resources=["Location"],
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_1",
        stale_cleanup=True,
        preserve_seen_stage=True,
    )

    assert counts == {"Location": 0}
    ensure_stage.assert_awaited_once_with("run_1")
    drop_stage.assert_not_awaited()


@pytest.mark.asyncio
async def test_provider_directory_table_ensure_skips_existing_index_ddl(
    monkeypatch,
):
    existing_index_name = "provider_directory_source_endpoint_id_idx"
    status_statements: list[str] = []

    async def fake_status(sql, **_params):
        status_statements.append(str(sql))
        return 0

    monkeypatch.setattr(importer, "SOURCE_MODELS", (ProviderDirectorySource,))
    monkeypatch.setattr(importer, "CANONICAL_RESOURCE_MODELS", ())
    monkeypatch.setattr(importer.db, "create_table", AsyncMock())
    monkeypatch.setattr(importer, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(
        importer.db,
        "all",
        AsyncMock(return_value=[{"indexname": existing_index_name}]),
    )
    monkeypatch.setattr(importer.db, "status", fake_status)
    monkeypatch.setattr(
        importer,
        "_ensure_provider_directory_model_columns",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_ensure_provider_directory_source_column_types",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_ensure_provider_directory_import_seen_table",
        AsyncMock(),
    )

    await importer._ensure_provider_directory_tables()

    joined_statements = "\n".join(status_statements)
    assert existing_index_name not in joined_statements
    assert "provider_directory_source_api_base_idx" in joined_statements
    importer.db.all.assert_awaited_once()
    assert importer.db.all.await_args.kwargs == {"schema_name": "mrf"}


@pytest.mark.asyncio
async def test_provider_directory_table_ensure_current_schema_runs_no_ddl(
    monkeypatch,
):
    index_names = [
        index["name"]
        for index in ProviderDirectorySource.__my_additional_indexes__
    ]
    status = AsyncMock()
    create_table = AsyncMock()
    monkeypatch.setattr(importer, "SOURCE_MODELS", (ProviderDirectorySource,))
    monkeypatch.setattr(importer, "CANONICAL_RESOURCE_MODELS", ())
    monkeypatch.setattr(importer, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(importer.db, "create_table", create_table)
    monkeypatch.setattr(
        importer.db,
        "all",
        AsyncMock(return_value=[{"indexname": name} for name in index_names]),
    )
    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(
        importer,
        "_ensure_provider_directory_model_columns",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_ensure_provider_directory_source_column_types",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_ensure_provider_directory_import_seen_table",
        AsyncMock(),
    )

    await importer._ensure_provider_directory_tables()

    status.assert_not_awaited()
    create_table.assert_not_awaited()


@pytest.mark.asyncio
async def test_provider_directory_table_ensure_repairs_missing_schema_and_table(
    monkeypatch,
):
    index_names = [
        index["name"]
        for index in ProviderDirectorySource.__my_additional_indexes__
    ]
    status = AsyncMock()
    create_table = AsyncMock()
    monkeypatch.setattr(importer, "SOURCE_MODELS", (ProviderDirectorySource,))
    monkeypatch.setattr(importer, "CANONICAL_RESOURCE_MODELS", ())
    monkeypatch.setattr(
        importer,
        "_table_exists",
        AsyncMock(side_effect=(False, False)),
    )
    monkeypatch.setattr(importer.db, "create_table", create_table)
    monkeypatch.setattr(
        importer.db,
        "all",
        AsyncMock(return_value=[{"indexname": name} for name in index_names]),
    )
    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(
        importer,
        "_ensure_provider_directory_model_columns",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_ensure_provider_directory_source_column_types",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_ensure_provider_directory_import_seen_table",
        AsyncMock(),
    )

    await importer._ensure_provider_directory_tables()

    status.assert_awaited_once_with('CREATE SCHEMA IF NOT EXISTS "mrf";')
    create_table.assert_awaited_once_with(
        ProviderDirectorySource.__table__,
        checkfirst=True,
    )


@pytest.mark.asyncio
async def test_provider_directory_existing_index_names_ignores_malformed_rows(
    monkeypatch,
):
    monkeypatch.setattr(
        importer.db,
        "all",
        AsyncMock(
            return_value=[
                {"indexname": " index_a "},
                {"indexname": ""},
                {"other": "index_b"},
                object(),
            ]
        ),
    )

    names = await importer._provider_directory_existing_index_names("mrf")

    assert names == {"index_a"}


@pytest.mark.asyncio
async def test_ensure_provider_directory_seen_table_creates_missing_table(monkeypatch):
    statements: list[str] = []

    async def fake_status(sql, **_params):
        statements.append(sql)
        return 0

    monkeypatch.setattr(importer, "_table_exists", AsyncMock(return_value=False))
    monkeypatch.setattr(importer.db, "status", fake_status)

    await importer._ensure_provider_directory_import_seen_table("mrf")

    combined = "\n".join(statements)
    assert 'CREATE UNLOGGED TABLE IF NOT EXISTS "mrf"."provider_directory_import_seen"' in combined
    assert "DROP INDEX" not in combined
    assert "CREATE INDEX IF NOT EXISTS provider_directory_import_seen_source_idx" not in combined


@pytest.mark.asyncio
async def test_ensure_provider_directory_seen_table_current_schema_runs_no_ddl(
    monkeypatch,
):
    status = AsyncMock()
    monkeypatch.setattr(importer, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(importer.db, "status", status)

    await importer._ensure_provider_directory_import_seen_table("mrf")

    status.assert_not_awaited()


@pytest.mark.asyncio
async def test_ensure_provider_directory_seen_stage_requires_run_id(monkeypatch):
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)

    stage_table = await importer._ensure_provider_directory_import_seen_stage_table(
        None,
        schema="mrf",
    )

    assert stage_table is None
    status.assert_not_awaited()


@pytest.mark.asyncio
async def test_drop_provider_directory_seen_stage_ignores_missing_name(monkeypatch):
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)

    await importer._drop_provider_directory_import_seen_stage_table(
        None,
        schema="mrf",
    )

    status.assert_not_awaited()


def test_resource_start_url_prefers_endpoint_and_preserves_existing_count():
    url = importer._resource_start_url(
        {
            "api_base": "https://example.test/fhir",
            "endpoint_practitioner": "https://payer.example/custom/Practitioner?active=true&_count=20",
        },
        "Practitioner",
        page_count=100,
    )

    assert url == "https://payer.example/custom/Practitioner?active=true&_count=20"


def test_resource_start_url_resolves_relative_endpoint_and_adds_count():
    url = importer._resource_start_url(
        {
            "api_base": "https://example.test/fhir/base",
            "endpoint_location": "Location?address-state=CA",
        },
        "Location",
        page_count=250,
    )

    assert url == "https://example.test/fhir/base/Location?address-state=CA&_count=250"


def test_resource_start_url_bounds_count_by_configured_max(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_MAX_PAGE_COUNT", "500")

    url = importer._resource_start_url(
        {"api_base": "https://example.test/fhir"},
        "HealthcareService",
        page_count=1000,
    )

    assert url == "https://example.test/fhir/HealthcareService?_count=500"


def test_michigan_direct_probe_caps_role_without_synthesizing_offset():
    source_row = importer._source_row_from_seed(
        {
            "id": "michigan",
            "org_name": "Blue Cross Blue Shield of Michigan",
            "api_base": importer.INTEROPSTATION_MDHHS_PROVIDER_DIRECTORY_BASE,
        }
    )
    url = importer._resource_start_url(
        source_row,
        "PractitionerRole",
        page_count=100,
    )

    assert url == (
        f"{importer.MICHIGAN_PROVIDER_DIRECTORY_BASE}/PractitionerRole?_count=25"
    )


def test_michigan_offset_substitute_is_not_equivalent_to_canonical_cursor_page():
    canonical_second_page_ids = {"75147991", "75147992", "75204637"}
    synthetic_offset_page_ids = {"75040123", "75317648", "75419649"}

    assert canonical_second_page_ids.isdisjoint(synthetic_offset_page_ids)


def test_resource_start_url_caps_uhc_insurance_plan_page_count():
    url = importer._resource_start_url(
        {"api_base": importer.UHC_PROVIDER_DIRECTORY_BASE},
        "InsurancePlan",
        page_count=100,
    )

    assert url == "https://flex.optum.com/fhirpublic/R4/InsurancePlan?_count=1"


@pytest.mark.parametrize(
    ("api_base", "resource_type"),
    [
        (importer.WASHINGTON_PROVIDER_DIRECTORY_BASE, "Location"),
        (importer.WYOMING_PROVIDER_DIRECTORY_BASE, "PractitionerRole"),
    ],
)
def test_resource_start_url_uses_standard_count_for_state_directory_pages(
    api_base,
    resource_type,
):
    url = importer._resource_start_url(
        {"api_base": api_base},
        resource_type,
        page_count=100,
    )

    assert url == f"{api_base}/{resource_type}?_count=100"


def test_michigan_relay_and_direct_base_have_stable_upstream_identity():
    relay_row = importer._source_row_from_seed(
        {
            "id": "michigan",
            "org_name": "Blue Cross Blue Shield of Michigan",
            "api_base": importer.INTEROPSTATION_MDHHS_PROVIDER_DIRECTORY_BASE,
        }
    )
    direct_row = importer._source_row_from_seed(
        {
            "id": "michigan",
            "org_name": "Blue Cross Blue Shield of Michigan",
            "api_base": importer.MICHIGAN_PROVIDER_DIRECTORY_BASE,
        }
    )

    assert relay_row["source_id"] == direct_row["source_id"]
    assert relay_row["source_id"] == importer._stable_source_id(
        {
            "org_name": "Blue Cross Blue Shield of Michigan",
            "api_base": importer.INTEROPSTATION_MDHHS_PROVIDER_DIRECTORY_BASE,
        }
    )
    assert relay_row["api_base"] == importer.MICHIGAN_PROVIDER_DIRECTORY_BASE
    assert relay_row["canonical_api_base"] == importer.MICHIGAN_PROVIDER_DIRECTORY_BASE
    assert relay_row["metadata_json"]["provider_directory_previous_api_base"] == (
        importer.INTEROPSTATION_MDHHS_PROVIDER_DIRECTORY_BASE
    )


def test_michigan_direct_base_uses_upstream_for_metadata_and_resource_probes():
    source_row = importer._source_row_from_seed(
        {
            "id": "michigan",
            "org_name": "Blue Cross Blue Shield of Michigan",
            "api_base": importer.MICHIGAN_PROVIDER_DIRECTORY_BASE,
        }
    )

    assert source_row["api_base"] == importer.MICHIGAN_PROVIDER_DIRECTORY_BASE
    assert source_row["metadata_json"]["provider_directory_confirmed_metadata_url"] == (
        f"{importer.MICHIGAN_PROVIDER_DIRECTORY_BASE}/metadata"
    )
    assert source_row["endpoint_practitioner_role"] == (
        f"{importer.MICHIGAN_PROVIDER_DIRECTORY_BASE}/PractitionerRole"
    )


def test_michigan_candidate_allows_only_verified_resources_and_records_page_caps():
    source_row = importer._source_row_from_seed(
        {
            "id": "michigan",
            "org_name": "Blue Cross Blue Shield of Michigan",
            "api_base": importer.INTEROPSTATION_MDHHS_PROVIDER_DIRECTORY_BASE,
        }
    )
    metadata = source_row["metadata_json"]

    assert metadata["provider_directory_supported_resources"] == sorted(
        importer.MICHIGAN_SUPPORTED_RESOURCES
    )
    assert metadata["provider_directory_fully_enumerable_resources"] == sorted(
        importer.MICHIGAN_SUPPORTED_RESOURCES
    )
    assert metadata["provider_directory_coverage_mode"] == "full"
    assert metadata["provider_directory_acquisition_enabled"] is True
    assert metadata["provider_directory_candidate_status"] == (
        "pending_two_matching_exhaustive_acquisitions"
    )
    assert metadata["provider_directory_resource_page_count_caps"] == {
        resource_type: 25 if resource_type == "PractitionerRole" else 100
        for resource_type in sorted(importer.MICHIGAN_SUPPORTED_RESOURCES)
    }
    assert importer._resource_start_url(source_row, "InsurancePlan", page_count=100) is None
    assert importer._resource_start_url(source_row, "Endpoint", page_count=100) is None
    assert importer._resource_start_url(source_row, "PractitionerRole", page_count=100) == (
        f"{importer.MICHIGAN_PROVIDER_DIRECTORY_BASE}/PractitionerRole?_count=25"
    )
    assert importer._resource_start_url(source_row, "Location", page_count=500) == (
        f"{importer.MICHIGAN_PROVIDER_DIRECTORY_BASE}/Location?_count=100"
    )
    assert importer._resource_acquisition_blocked_reason(
        source_row,
        sorted(importer.MICHIGAN_SUPPORTED_RESOURCES),
    ) is None


def test_michigan_preserves_advertised_opaque_next_link_without_offset_synthesis():
    current_url = (
        f"{importer.MICHIGAN_PROVIDER_DIRECTORY_BASE}/PractitionerRole?_count=25"
    )
    advertised_next_url = (
        f"{importer.MICHIGAN_PROVIDER_DIRECTORY_BASE}/PractitionerRole?"
        "_getpages=opaque&_pageId=signed-page&_bundletype=searchset"
    )

    next_url = importer._resolved_fhir_next_url(
        {"api_base": importer.MICHIGAN_PROVIDER_DIRECTORY_BASE},
        current_url,
        advertised_next_url,
    )

    assert next_url == advertised_next_url
    assert "_getpagesoffset" not in next_url


def test_michigan_candidate_excludes_unsupported_resource_acquisition():
    source_row = importer._source_row_from_seed(
        {
            "id": "michigan",
            "org_name": "Blue Cross Blue Shield of Michigan",
            "api_base": importer.MICHIGAN_PROVIDER_DIRECTORY_BASE,
        }
    )

    assert importer._resource_acquisition_blocked_reason(
        source_row,
        sorted(importer.MICHIGAN_SUPPORTED_RESOURCES),
    ) is None
    assert importer._resource_start_url(
        source_row,
        "HealthcareService",
        page_count=100,
    ) is None
    assert importer._resource_start_url(
        source_row,
        "InsurancePlan",
        page_count=100,
    ) is None
    assert importer._resource_start_url(
        source_row,
        "Endpoint",
        page_count=100,
    ) is None


def test_washington_source_profile_is_probe_only_and_acquisition_blocked():
    source_row = importer._source_row_from_seed(
        {
            "id": "washington",
            "org_name": "Community Health Plan of Washington",
            "api_base": importer.WASHINGTON_PROVIDER_DIRECTORY_BASE,
        }
    )

    assert source_row["last_validated_status"] == "valid"
    assert source_row["metadata_json"]["provider_directory_supported_resources"] == (
        list(importer.DEFAULT_RESOURCES)
    )
    assert source_row["metadata_json"][
        "provider_directory_fully_enumerable_resources"
    ] == []
    assert source_row["metadata_json"]["provider_directory_coverage_mode"] == (
        "probe_only"
    )
    assert (
        source_row["metadata_json"]["provider_directory_acquisition_enabled"]
        is False
    )
    assert "checkpoint progress regresses" in source_row["metadata_json"][
        "provider_directory_acquisition_blocked_reason"
    ]
    assert importer._resource_acquisition_blocked_reason(source_row) == (
        "coverage_mode_probe_only"
    )


def test_resource_start_url_uses_metadata_resource_page_count_cap():
    url = importer._resource_start_url(
        {
            "api_base": "https://payer.example/fhir",
            "metadata_json": {
                "provider_directory_resource_page_count_caps": {
                    "PractitionerRole": 10,
                }
            },
        },
        "PractitionerRole",
        page_count=100,
    )

    assert url == "https://payer.example/fhir/PractitionerRole?_count=10"


def test_resource_start_url_ignores_catalog_annotation_endpoint():
    url = importer._resource_start_url(
        {
            "api_base": "https://fhir.humana.com/api",
            "endpoint_location": "/api/provider-directory/Location (HTTP 400 - may need query parameters)",
        },
        "Location",
        page_count=100,
    )

    assert url == "https://fhir.humana.com/api/Location?_count=100"


def test_resource_start_url_ignores_unreachable_catalog_endpoint():
    url = importer._resource_start_url(
        {
            "api_base": "https://fp.medicaid.utah.gov/ProviderDirectoryServices",
            "endpoint_location": (
                "https://fp.medicaid.utah.gov/ProviderDirectoryServices/metadata/Location "
                "(unreachable)"
            ),
        },
        "Location",
        page_count=100,
    )

    assert url == "https://fp.medicaid.utah.gov/ProviderDirectoryServices/Location?_count=100"


def test_partitioned_resource_start_urls_partitions_scan_too_costly_resources():
    practitioner_urls = importer._partitioned_resource_start_urls(
        {"api_base": importer.SCAN_PROVIDER_DIRECTORY_BASE},
        "Practitioner",
        page_count=25,
    )
    location_urls = importer._partitioned_resource_start_urls(
        {"api_base": importer.SCAN_PROVIDER_DIRECTORY_BASE},
        "Location",
        page_count=25,
    )
    organization_urls = importer._partitioned_resource_start_urls(
        {"api_base": importer.SCAN_PROVIDER_DIRECTORY_BASE},
        "Organization",
        page_count=25,
    )
    plan_urls = importer._partitioned_resource_start_urls(
        {"api_base": importer.SCAN_PROVIDER_DIRECTORY_BASE},
        "InsurancePlan",
        page_count=25,
    )

    assert practitioner_urls[0] == (
        "https://providerdirectory.scanhealthplan.com/Practitioner?_count=25&family=AA"
    )
    assert practitioner_urls[(26 * 26) - 1] == (
        "https://providerdirectory.scanhealthplan.com/Practitioner?_count=25&family=ZZ"
    )
    assert practitioner_urls[26 * 26].endswith("_count=25&family%3Aexact=A")
    assert practitioner_urls[-1].endswith("_count=25&family=Z%27")
    assert len(practitioner_urls) == (26 * 26) + 26 + 26
    assert organization_urls[(26 * 26) - 1].endswith("_count=25&name=ZZ")
    assert organization_urls[-10].endswith("_count=25&name=0")
    assert organization_urls[-1].endswith("_count=25&name=9")
    assert len(organization_urls) == (26 * 26) + 10
    assert location_urls[:2] == [
        "https://providerdirectory.scanhealthplan.com/Location?_count=25&name=A",
        "https://providerdirectory.scanhealthplan.com/Location?_count=25&name=B",
    ]
    assert len(location_urls) == 26
    assert plan_urls == ["https://providerdirectory.scanhealthplan.com/InsurancePlan?_count=25"]


def test_resource_start_url_caps_scan_server_page_size():
    url = importer._resource_start_url(
        {"api_base": importer.SCAN_PROVIDER_DIRECTORY_BASE},
        "Practitioner",
        page_count=1000,
    )

    assert url == f"{importer.SCAN_PROVIDER_DIRECTORY_BASE}/Practitioner?_count=100"


def test_aetna_commercial_resources_use_one_unfiltered_count_30_stream():
    source_lookup = importer._source_row_from_seed(
        importer._aetna_provider_directory_data_seed_rows()[0]
    )

    for resource_type in importer.AETNA_COMMERCIAL_SUPPORTED_RESOURCES:
        assert importer._partitioned_resource_start_urls(
            source_lookup,
            resource_type,
            page_count=100,
        ) == [
            f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}/{resource_type}?_count=30"
        ]

    assert importer._partitioned_resource_start_urls(
        source_lookup,
        "Endpoint",
        page_count=100,
    ) == []


def test_aetna_medicaid_target_requires_npi_or_name_and_location():
    base_url = f"{importer.AETNA_PROVIDER_DIRECTORY_BASE}/Practitioner"

    assert importer._is_aetna_medicaid_targeted_search(
        f"{base_url}?identifier=1234567893"
    )
    assert importer._is_aetna_medicaid_targeted_search(
        f"{base_url}?family=Smith&address-state=IL"
    )
    assert not importer._is_aetna_medicaid_targeted_search(
        f"{base_url}?family=Smith"
    )
    assert not importer._is_aetna_medicaid_targeted_search(
        f"{base_url}?address-state=IL"
    )


@pytest.mark.asyncio
async def test_aetna_medicaid_unfiltered_import_fails_closed_without_request(monkeypatch):
    source_lookup = importer._source_row_from_seed(
        {
            "id": "aetna-medicaid",
            "org_name": "Aetna",
            "api_base": importer.AETNA_PROVIDER_DIRECTORY_BASE,
            "source": "fixture",
        }
    )
    fetch_source_json = AsyncMock(
        side_effect=AssertionError("unfiltered Aetna Medicaid search must not run")
    )
    monkeypatch.setattr(importer, "_fetch_source_json", fetch_source_json)

    fetch_result = await importer._fetch_resource_rows(
        source_lookup,
        "Practitioner",
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_1",
        bulk_export=True,
    )

    assert fetch_result is not None
    assert fetch_result.complete is False
    assert fetch_result.fetch_mode == "targeted_required"
    assert fetch_result.error == importer.AETNA_MEDICAID_TARGETED_QUERY_REQUIRED_ERROR
    fetch_source_json.assert_not_awaited()


@pytest.mark.asyncio
async def test_aetna_medicaid_targeted_import_can_write_evidence(monkeypatch):
    source_lookup = importer._source_row_from_seed(
        {
            "id": "aetna-medicaid",
            "org_name": "Aetna",
            "api_base": importer.AETNA_PROVIDER_DIRECTORY_BASE,
            "source": "fixture",
        }
    )
    source_lookup["endpoint_practitioner"] = (
        f"{importer.AETNA_PROVIDER_DIRECTORY_BASE}/Practitioner?"
        "identifier=1234567893"
    )
    written_rows = []

    async def fake_fetch_source_json(_source, request_url, *, timeout):
        assert "identifier=1234567893" in request_url
        return (
            200,
            {
                "resourceType": "Bundle",
                "entry": [
                    {
                        "resource": {
                            "resourceType": "Practitioner",
                            "id": "prac-1",
                        }
                    }
                ],
            },
            None,
            5,
        )

    async def write_rows(_model, resource_rows):
        written_rows.extend(resource_rows)
        return len(resource_rows)

    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_source_json)

    fetch_result = await importer._fetch_resource_rows(
        source_lookup,
        "Practitioner",
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_1",
        row_batch_handler=write_rows,
        row_batch_size=10,
        retain_rows=False,
    )

    assert fetch_result is not None
    assert fetch_result.complete is True
    assert fetch_result.rows_written == 1
    assert [written_row["resource_id"] for written_row in written_rows] == ["prac-1"]


def test_partitioned_resource_start_urls_partitions_uhc_provider_directory_searches():
    practitioner_urls = importer._partitioned_resource_start_urls(
        {"api_base": importer.UHC_PROVIDER_DIRECTORY_BASE},
        "Practitioner",
        page_count=100,
    )
    organization_urls = importer._partitioned_resource_start_urls(
        {"api_base": importer.UHC_PROVIDER_DIRECTORY_BASE},
        "Organization",
        page_count=100,
    )
    location_urls = importer._partitioned_resource_start_urls(
        {"api_base": importer.UHC_PROVIDER_DIRECTORY_BASE},
        "Location",
        page_count=100,
    )
    role_urls = importer._partitioned_resource_start_urls(
        {"api_base": importer.UHC_PROVIDER_DIRECTORY_BASE},
        "PractitionerRole",
        page_count=100,
    )

    assert practitioner_urls[0] == "https://flex.optum.com/fhirpublic/R4/Practitioner?_count=100&family=A"
    assert practitioner_urls[25].endswith("_count=100&family=Z")
    assert practitioner_urls[26].endswith("_count=100&family=0")
    assert practitioner_urls[-1].endswith("_count=100&family=9")
    assert len(practitioner_urls) == len(importer.UHC_NAME_ROOT_ALPHABET)
    assert organization_urls[0] == "https://flex.optum.com/fhirpublic/R4/Organization?_count=100&name=A"
    assert organization_urls[25].endswith("_count=100&name=Z")
    assert organization_urls[26].endswith("_count=100&name=0")
    assert organization_urls[-1].endswith("_count=100&name=9")
    assert len(organization_urls) == len(importer.UHC_NAME_ROOT_ALPHABET)
    assert location_urls[0] == "https://flex.optum.com/fhirpublic/R4/Location?_count=100&address-state=AL"
    assert location_urls[-1].endswith("_count=100&address-state=VI")
    assert len(location_urls) == len(importer.US_STATE_ABBRS)
    assert role_urls[0] == (
        "https://flex.optum.com/fhirpublic/R4/PractitionerRole?"
        "_count=100&location.address-postalcode=0"
    )
    assert role_urls[9].endswith("_count=100&location.address-postalcode=9")
    assert role_urls[10].endswith("_count=100&location.address-postalcode=A")
    assert role_urls[-1].endswith("_count=100&location.address-postalcode=Z")
    assert len(role_urls) == 36


def test_provider_directory_partition_concurrency_defaults_to_sixteen(monkeypatch):
    monkeypatch.delenv("HLTHPRT_PROVIDER_DIRECTORY_PARTITION_CONCURRENCY", raising=False)

    assert importer._partition_fetch_concurrency() == 16


@pytest.mark.asyncio
async def test_uhc_partition_scan_uses_deadline_instead_of_global_page_cap(monkeypatch):
    observed_options: list[importer.PartitionFetchOptions] = []

    async def capture_partition_options(
        _source,
        _resource_type,
        resource_model,
        _start_urls,
        fetch_options,
    ):
        observed_options.append(fetch_options)
        return importer.ResourceFetchResult(
            model=resource_model,
            rows=[],
            rows_fetched=0,
            rows_written=0,
            pages_fetched=0,
            complete=False,
            row_limit_reached=False,
            page_limit_reached=False,
            hard_page_limit_reached=False,
            next_url_remaining=True,
            error="uhc_location_state_residual_unverified",
            fetch_mode="partitioned_paged",
        )

    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_MAX_FULL_PAGES", "1")
    monkeypatch.setattr(
        importer,
        "_fetch_partitioned_resource_rows",
        capture_partition_options,
    )

    await importer._fetch_resource_rows(
        {"source_id": "uhc", "api_base": importer.UHC_PROVIDER_DIRECTORY_BASE},
        "Location",
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_1",
        deadline_seconds=60,
    )

    assert observed_options[0].max_pages == 0
    assert observed_options[0].deadline_at is not None


def test_uhc_postal_checkpoint_type_is_scoped_to_alias_set():
    source_lookup = {
        "source_id": "uhc-primary",
        "api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
    }

    single_alias_type = importer._uhc_role_postal_checkpoint_type(
        source_lookup,
        ["uhc-primary"],
    )
    grouped_alias_type = importer._uhc_role_postal_checkpoint_type(
        source_lookup,
        ["uhc-primary", "uhc-community"],
    )

    assert single_alias_type != grouped_alias_type
    assert single_alias_type.startswith(importer.UHC_ROLE_POSTAL_CHECKPOINT_TYPE + ":")


@pytest.mark.asyncio
async def test_clear_reverse_checkpoints_can_scope_seed_type(monkeypatch):
    statements: list[tuple[str, dict[str, Any]]] = []

    async def fake_db_status(statement, **query_params):
        statements.append((statement, query_params))
        return 4

    monkeypatch.setattr(importer.db, "status", fake_db_status)
    source_lookup = {
        "source_id": "uhc-primary",
        "canonical_api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
    }
    checkpoint_type = importer._uhc_role_postal_checkpoint_type(source_lookup)

    deleted_count = await importer._clear_reverse_lookup_checkpoints(
        source_lookup,
        seed_resource_type=checkpoint_type,
    )

    assert deleted_count == 4
    assert "seed_resource_type = :seed_resource_type" in statements[0][0]
    assert statements[0][1]["seed_resource_type"] == checkpoint_type


def test_uhc_adaptive_partition_child_urls_split_capped_prefix():
    source_dict = {
        "api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
    }
    children = importer._uhc_adaptive_partition_child_urls(
        source_dict,
        "Practitioner",
        "https://flex.optum.com/fhirpublic/R4/Practitioner?_count=100&family=SM",
        {"resourceType": "Bundle", "total": 10000},
    )

    assert children[0] == "https://flex.optum.com/fhirpublic/R4/Practitioner?_count=100&family=SMA"
    assert any(child.endswith("_count=100&family=SM0") for child in children)
    assert any(child.endswith("_count=100&family=SM+") for child in children)
    assert any(child.endswith("_count=100&family=SM-") for child in children)
    assert len(children) == len(importer.UHC_TEXT_SEARCH_ALPHABET)
    assert (
        importer._uhc_adaptive_partition_child_urls(
            source_dict,
            "Practitioner",
            "https://flex.optum.com/fhirpublic/R4/Practitioner?_count=100&family=AA",
            {"resourceType": "Bundle", "total": 9999},
        )
        == []
    )


def test_uhc_text_partition_uses_safe_root_and_separator_sets():
    expected_root_characters = set(string.ascii_uppercase + string.digits)
    expected_child_characters = set(string.ascii_uppercase + string.digits + " -.")

    assert set(importer.UHC_NAME_ROOT_ALPHABET) == expected_root_characters
    assert set(importer.UHC_TEXT_SEARCH_ALPHABET) == expected_child_characters


@pytest.mark.parametrize(
    "continuation_name",
    ("_getpages", "_getpagesoffset", "_page_token", "cursor"),
)
def test_uhc_partition_helpers_recognize_all_continuation_shapes(continuation_name):
    source_lookup = {"api_base": importer.UHC_PROVIDER_DIRECTORY_BASE}
    request_url = (
        "https://flex.optum.com/fhirpublic/R4/Location?"
        f"address-state=CA&{continuation_name}=continuation"
    )
    capped_payload_dict = {"resourceType": "Bundle", "total": 10000}

    assert not importer._uhc_adaptive_partition_child_urls(
        source_lookup,
        "Location",
        request_url,
        capped_payload_dict,
    )
    assert not importer._is_uhc_partition_cap_exhausted(
        source_lookup,
        request_url,
        capped_payload_dict,
    )
    assert importer._uhc_partition_identity(
        source_lookup,
        "Location",
        request_url,
    ) is None


def test_uhc_exact_partition_entries_separate_parent_value_from_children():
    practitioner_payload_dict = {
        "resourceType": "Bundle",
        "entry": [
            {
                "resource": {
                    "resourceType": "Practitioner",
                    "id": "exact-sm",
                    "name": [{"family": "SM"}],
                }
            },
            {
                "resource": {
                    "resourceType": "Practitioner",
                    "id": "longer-smith",
                    "name": [{"family": "Smith"}],
                }
            },
        ],
    }
    exact_entries = importer._uhc_exact_partition_entries(
        "Practitioner",
        "https://flex.optum.com/fhirpublic/R4/Practitioner?family=SM",
        practitioner_payload_dict,
    )

    assert [bundle_entry["resource"]["id"] for bundle_entry in exact_entries or []] == [
        "exact-sm"
    ]


def test_uhc_exact_partition_entries_keep_organization_alias_match():
    organization_payload_dict = {
        "resourceType": "Bundle",
        "entry": [
            {
                "resource": {
                    "resourceType": "Organization",
                    "id": "alias-sm",
                    "name": "Acme Health",
                    "alias": ["SM"],
                }
            },
            {
                "resource": {
                    "resourceType": "Organization",
                    "id": "longer-smith",
                    "name": "Smith Health",
                }
            },
        ],
    }

    exact_entries = importer._uhc_exact_partition_entries(
        "Organization",
        "https://flex.optum.com/fhirpublic/R4/Organization?name=SM",
        organization_payload_dict,
    )

    assert [bundle_entry["resource"]["id"] for bundle_entry in exact_entries or []] == [
        "alias-sm"
    ]


def test_uhc_location_state_parent_keeps_missing_city_residual():
    location_payload_dict = {
        "resourceType": "Bundle",
        "entry": [
            {
                "resource": {
                    "resourceType": "Location",
                    "id": "missing-city",
                    "address": {"state": "CA"},
                }
            },
            {
                "resource": {
                    "resourceType": "Location",
                    "id": "san-diego",
                    "address": {"state": "CA", "city": "San Diego"},
                }
            },
        ],
    }
    exact_entries = importer._uhc_exact_partition_entries(
        "Location",
        "https://flex.optum.com/fhirpublic/R4/Location?address-state=CA",
        location_payload_dict,
    )

    assert [bundle_entry["resource"]["id"] for bundle_entry in exact_entries or []] == [
        "missing-city"
    ]


def test_uhc_adaptive_partition_child_urls_split_capped_role_postal_prefix():
    source_lookup = {
        "api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
    }
    children = importer._uhc_adaptive_partition_child_urls(
        source_lookup,
        "PractitionerRole",
        (
            "https://flex.optum.com/fhirpublic/R4/PractitionerRole?"
            "_count=100&location.address-postalcode=606"
        ),
        {"resourceType": "Bundle", "total": 10000},
    )

    assert children[0].endswith("_count=100&location.address-postalcode=6060")
    assert children[-1].endswith("_count=100&location.address-postalcode=6069")
    assert len(children) == 10


def test_uhc_location_capped_state_splits_into_city_prefixes():
    source_lookup = {
        "api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
    }
    children = importer._uhc_adaptive_partition_child_urls(
        source_lookup,
        "Location",
        "https://flex.optum.com/fhirpublic/R4/Location?_count=100&address-state=CA",
        {"resourceType": "Bundle", "total": 10000},
    )

    assert children[0].endswith("_count=100&address-state=CA&address-city=A")
    assert any(child.endswith("_count=100&address-state=CA&address-city=0") for child in children)
    assert not any(child.endswith("_count=100&address-state=CA&address-city=+") for child in children)
    assert len(children) == len(importer.UHC_NAME_ROOT_ALPHABET)


def test_uhc_location_city_partition_covers_space_branch_beyond_four_characters():
    source_lookup = {"api_base": importer.UHC_PROVIDER_DIRECTORY_BASE}
    children = importer._uhc_adaptive_partition_child_urls(
        source_lookup,
        "Location",
        (
            "https://flex.optum.com/fhirpublic/R4/Location?"
            "_count=100&address-state=CA&address-city=SAN+"
        ),
        {"resourceType": "Bundle", "total": 10000},
    )

    assert any(child.endswith("address-city=SAN+D") for child in children)
    assert any(child.endswith("address-city=SAN+J") for child in children)


def test_uhc_location_partition_identity_preserves_significant_space():
    source_lookup = {"api_base": importer.UHC_PROVIDER_DIRECTORY_BASE}

    assert importer._uhc_partition_identity(
        source_lookup,
        "Location",
        (
            "https://flex.optum.com/fhirpublic/R4/Location?"
            "_count=100&address-state=CA&address-city=SAN+"
        ),
    ) == "address-state=CA&address-city=SAN+"


@pytest.mark.parametrize(
    ("request_url", "address", "expected_error"),
    (
        (
            "https://flex.optum.com/fhirpublic/R4/Location?address-state=CA",
            {"state": "NV", "city": "Carson City"},
            "uhc_location_address_state_predicate_rejected",
        ),
        (
            "https://flex.optum.com/fhirpublic/R4/Location?address-state=CA&address-city=S",
            {"state": "CA", "city": "Oakland"},
            "uhc_location_address_city_predicate_rejected",
        ),
    ),
)
def test_uhc_location_partition_rejects_ignored_predicates(
    request_url,
    address,
    expected_error,
):
    source_lookup = {"api_base": importer.UHC_PROVIDER_DIRECTORY_BASE}
    payload = {
        "resourceType": "Bundle",
        "total": 10000,
        "entry": [
            {
                "resource": {
                    "resourceType": "Location",
                    "id": "location-1",
                    "address": address,
                }
            }
        ],
    }

    assert (
        importer._uhc_location_partition_predicate_error(
            source_lookup,
            "Location",
            request_url,
            payload,
        )
        == expected_error
    )


@pytest.mark.asyncio
async def test_uhc_location_predicate_rejection_does_not_enqueue_children():
    source_lookup = {"api_base": importer.UHC_PROVIDER_DIRECTORY_BASE}
    start_url_queue: asyncio.Queue[str] = asyncio.Queue()
    partition_fetch_state = importer.PartitionFetchState(
        result_model=ProviderDirectoryLocation,
        retained_resource_rows=[],
    )
    bounded_outcome = await importer._partition_page_bounded_outcome(
        source_lookup,
        "Location",
        start_url_queue,
        partition_fetch_state,
        "https://flex.optum.com/fhirpublic/R4/Location?address-state=CA",
        "https://flex.optum.com/fhirpublic/R4/Location?address-state=CA",
        {
            "resourceType": "Bundle",
            "total": 10000,
            "entry": [
                {
                    "resource": {
                        "resourceType": "Location",
                        "id": "location-1",
                        "address": {"state": "NV", "city": "Reno"},
                    }
                }
            ],
        },
    )

    assert bounded_outcome == "predicate_failed"
    assert start_url_queue.empty()
    assert partition_fetch_state.error_message == (
        "partition_errors_1_last_uhc_location_address_state_predicate_rejected"
    )


@pytest.mark.asyncio
async def test_uhc_location_continuation_uses_partition_start_predicate():
    source_lookup = {"api_base": importer.UHC_PROVIDER_DIRECTORY_BASE}
    partition_fetch_state = importer.PartitionFetchState(
        result_model=ProviderDirectoryLocation,
        retained_resource_rows=[],
    )
    bounded_outcome = await importer._partition_page_bounded_outcome(
        source_lookup,
        "Location",
        asyncio.Queue(),
        partition_fetch_state,
        "https://flex.optum.com/fhirpublic/R4/Location?address-state=CA&address-city=S",
        "https://flex.optum.com/fhirpublic/R4?_getpages=cursor&_getpagesoffset=100",
        {
            "resourceType": "Bundle",
            "total": 9999,
            "entry": [
                {
                    "resource": {
                        "resourceType": "Location",
                        "id": "location-2",
                        "address": {"state": "CA", "city": "Oakland"},
                    }
                }
            ],
        },
    )

    assert bounded_outcome == "predicate_failed"
    assert partition_fetch_state.error_message == (
        "partition_errors_1_last_uhc_location_address_city_predicate_rejected"
    )


def test_uhc_location_max_city_prefix_reports_unsplittable_cap():
    source_lookup = {"api_base": importer.UHC_PROVIDER_DIRECTORY_BASE}
    request_url = (
        "https://flex.optum.com/fhirpublic/R4/Location?"
        "_count=100&address-state=CA&address-city=ABCDEFGH"
    )
    payload = {"resourceType": "Bundle", "total": 10000}

    assert not importer._uhc_adaptive_partition_child_urls(
        source_lookup,
        "Location",
        request_url,
        payload,
    )
    assert importer._is_uhc_partition_cap_exhausted(
        source_lookup,
        request_url,
        payload,
    )


def test_uhc_organization_affiliation_has_no_false_partition_strategy():
    source_lookup = {"api_base": importer.UHC_PROVIDER_DIRECTORY_BASE}
    start_urls = importer._partitioned_resource_start_urls(
        source_lookup,
        "OrganizationAffiliation",
        page_count=100,
    )

    assert start_urls == [
        "https://flex.optum.com/fhirpublic/R4/OrganizationAffiliation?_count=100"
    ]
    assert not importer._uhc_adaptive_partition_child_urls(
        source_lookup,
        "OrganizationAffiliation",
        start_urls[0] + "&address-state=CA&address-city=S",
        {"resourceType": "Bundle", "total": 10000},
    )
    assert importer._uhc_partition_checkpoint_type(
        source_lookup,
        "OrganizationAffiliation",
    ) is None


def test_uhc_role_postal_partition_reports_unsplittable_zip_cap():
    source_lookup = {
        "api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
    }

    assert importer._is_uhc_role_zip_cap_hit(
        source_lookup,
        "PractitionerRole",
        (
            "https://flex.optum.com/fhirpublic/R4/PractitionerRole?"
            "_count=100&location.address-postalcode=60601"
        ),
        {"resourceType": "Bundle", "total": 10000},
    )


@pytest.mark.parametrize(
    ("resource_type", "request_url"),
    (
        (
            "Location",
            "https://flex.optum.com/fhirpublic/R4/Location?_count=100&address-state=CA",
        ),
        (
            "Practitioner",
            "https://flex.optum.com/fhirpublic/R4/Practitioner?_count=100&family=ABCD",
        ),
        (
            "OrganizationAffiliation",
            "https://flex.optum.com/fhirpublic/R4/OrganizationAffiliation?_count=100",
        ),
    ),
)
def test_uhc_unsplittable_resource_total_cap_is_exhausted(resource_type, request_url):
    del resource_type
    source_lookup = {
        "api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
    }

    assert importer._is_uhc_partition_cap_exhausted(
        source_lookup,
        request_url,
        {"resourceType": "Bundle", "total": 10000},
    )


@pytest.mark.parametrize(
    ("resource_type", "expected_error"),
    (
        ("Practitioner", "uhc_practitioner_name_residual_unverified"),
        ("PractitionerRole", "uhc_practitionerrole_residual_unverified"),
        ("Organization", "uhc_organization_name_residual_unverified"),
        ("Location", "uhc_location_state_residual_unverified"),
    ),
)
def test_uhc_partitioned_core_resources_report_unverified_residuals(
    resource_type,
    expected_error,
):
    source_lookup = {
        "api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
    }

    assert importer._uhc_partition_residual_error(source_lookup, resource_type) == expected_error


@pytest.mark.asyncio
async def test_uhc_unpartitioned_total_cap_is_incomplete(monkeypatch):
    async def fake_fetch_json(_source_record, _request_url, *, timeout):
        return 200, {"resourceType": "Bundle", "total": 10000}, None, 5

    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_json)
    source_lookup = {
        "source_id": "uhc",
        "api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
    }

    fetch_result = await importer._fetch_resource_rows(
        source_lookup,
        "OrganizationAffiliation",
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_1",
    )

    assert fetch_result is not None
    assert fetch_result.complete is False
    assert fetch_result.error == "uhc_organizationaffiliation_partition_cap_exhausted"


def test_uhc_insurance_plan_collects_top_level_and_nested_networks():
    parsed = importer.parse_fhir_resource(
        "uhc",
        {
            "resourceType": "InsurancePlan",
            "id": "plan-1",
            "network": [{"reference": "Organization/network-top"}],
            "plan": [
                {"network": [{"reference": "Organization/network-nested"}]},
                {"network": [{"reference": "Organization/network-top"}]},
            ],
        },
    )

    assert parsed is not None
    assert parsed[1]["network_refs"] == [
        "Organization/network-top",
        "Organization/network-nested",
    ]


def test_uhc_plan_graph_rejects_unresolvable_plan_network_reference():
    assert importer._has_invalid_uhc_network_ref(
        [{"resource_id": "plan-1", "network_refs": ["urn:network:unknown"]}]
    )
    assert importer._has_invalid_uhc_network_ref(
        [{"resource_id": "plan-without-network", "network_refs": []}]
    )


def test_uhc_plan_graph_network_predicate_normalizes_absolute_references():
    matching_payload_map = {
        "resourceType": "PractitionerRole",
        "id": "role-1",
        "network": [
            {
                "reference": (
                    "https://flex.optum.com/fhirpublic/R4/Organization/network-1"
                )
            }
        ],
    }

    assert (
        importer._uhc_plan_graph_network_predicate_error(
            "PractitionerRole",
            "Organization/network-1",
            [matching_payload_map],
        )
        is None
    )
    assert importer._uhc_plan_graph_network_predicate_error(
        "PractitionerRole",
        "Organization/network-2",
        [matching_payload_map],
    ) == "uhc_plan_graph_practitionerrole_network_predicate_rejected"


@pytest.mark.asyncio
async def test_uhc_plan_graph_network_shard_cap_fails_closed(monkeypatch):
    async def capped_fetch(_source, _url, *, timeout):
        del timeout
        return 200, {"resourceType": "Bundle", "total": 10000}, None, 2

    monkeypatch.setattr(importer, "_fetch_source_json", capped_fetch)
    rows, pages, fetch_error, deadline_reached = (
        await importer._fetch_uhc_plan_graph_collection(
            _uhc_reverse_lookup_source(),
            "PractitionerRole",
            page_count=100,
            timeout=3,
            run_id="run-1",
            deadline_at=None,
            network_reference="Organization/network-1",
        )
    )

    assert rows == []
    assert pages == 1
    assert fetch_error == "uhc_plan_graph_practitionerrole_cap_exhausted"
    assert deadline_reached is False


@pytest.mark.asyncio
async def test_uhc_plan_graph_requires_search_total(monkeypatch):
    async def total_omitting_fetch(_source, _url, *, timeout):
        del timeout
        return 200, {"resourceType": "Bundle", "entry": []}, None, 2

    monkeypatch.setattr(importer, "_fetch_source_json", total_omitting_fetch)

    rows, pages, fetch_error, deadline_reached = (
        await importer._fetch_uhc_plan_graph_collection(
            _uhc_reverse_lookup_source(),
            "InsurancePlan",
            page_count=1,
            timeout=3,
            run_id="run-1",
            deadline_at=None,
        )
    )

    assert rows == []
    assert pages == 1
    assert fetch_error == "uhc_plan_graph_insuranceplan_total_missing"
    assert deadline_reached is False


@pytest.mark.asyncio
async def test_uhc_plan_graph_enumerates_both_role_types_per_plan_network(monkeypatch):
    collection_calls: list[tuple[str, str | None]] = []

    async def fetch_collection(_source, resource_type, **options):
        network_reference = options.get("network_reference")
        collection_calls.append((resource_type, network_reference))
        if resource_type == "InsurancePlan":
            return [
                {
                    "resource_id": "plan-1",
                    "network_refs": ["Organization/network-1", "Organization/network-2"],
                }
            ], 1, None, False
        resource_id = f"{resource_type}-{network_reference.rsplit('/', 1)[-1]}"
        return [{"resource_id": resource_id}], 1, None, False

    monkeypatch.setattr(importer, "_fetch_uhc_plan_graph_collection", fetch_collection)
    monkeypatch.setattr(
        importer,
        "_fetch_uhc_plan_graph_linked_rows",
        AsyncMock(return_value=({}, None, False)),
    )

    graph_snapshot = await importer._fetch_uhc_plan_graph_snapshot(
        _uhc_reverse_lookup_source(),
        page_count=1,
        timeout=3,
        run_id="run-1",
        deadline_at=None,
    )

    assert graph_snapshot.error is None
    assert graph_snapshot.network_count == 2
    assert collection_calls[1:] == [
        (resource_type, f"Organization/network-{network_number}")
        for network_number in (1, 2)
        for resource_type in ("PractitionerRole", "OrganizationAffiliation")
    ]


@pytest.mark.asyncio
async def test_uhc_plan_graph_rejects_empty_insurance_plan_snapshot(monkeypatch):
    monkeypatch.setattr(
        importer,
        "_fetch_uhc_plan_graph_collection",
        AsyncMock(return_value=([], 1, None, False)),
    )

    graph_snapshot = await importer._fetch_uhc_plan_graph_snapshot(
        _uhc_reverse_lookup_source(),
        page_count=1,
        timeout=3,
        run_id="run-1",
        deadline_at=None,
    )

    assert graph_snapshot.error == "uhc_plan_graph_insuranceplan_unexpected_empty"


@pytest.mark.asyncio
async def test_uhc_plan_graph_unresolved_linked_reference_fails_closed(monkeypatch):
    monkeypatch.setattr(
        importer,
        "_fetch_linked_resource_row",
        AsyncMock(return_value=None),
    )
    role_rows_by_resource = {
        "PractitionerRole": [
            {
                "resource_id": "role-1",
                "practitioner_ref": "Practitioner/practitioner-1",
            }
        ]
    }

    linked_rows, fetch_error, deadline_reached = (
        await importer._fetch_uhc_plan_graph_linked_rows(
            _uhc_reverse_lookup_source(),
            role_rows_by_resource,
            timeout=3,
            run_id="run-1",
            deadline_at=None,
        )
    )

    assert linked_rows == {}
    assert fetch_error == "uhc_plan_graph_unresolved_practitioner_reference"
    assert deadline_reached is False


def test_uhc_plan_graph_rows_dedupe_by_resource_identity():
    deduped_rows = importer._dedupe_uhc_plan_graph_rows(
        {
            "PractitionerRole": [
                {"resource_id": "role-2", "active": True},
                {"resource_id": "role-1", "active": True},
                {"resource_id": "role-2", "active": True},
            ]
        }
    )

    assert [row["resource_id"] for row in deduped_rows["PractitionerRole"]] == [
        "role-1",
        "role-2",
    ]


@pytest.mark.asyncio
async def test_uhc_plan_graph_requires_two_stable_snapshots(monkeypatch):
    stable_snapshot = importer.UHCPlanGraphSnapshot(
        rows_by_resource={"InsurancePlan": [{"resource_id": "plan-1"}]},
        pages_by_resource={"InsurancePlan": 1},
        network_count=0,
    )
    mutated_snapshot = importer.UHCPlanGraphSnapshot(
        rows_by_resource={"InsurancePlan": [{"resource_id": "plan-2"}]},
        pages_by_resource={"InsurancePlan": 1},
        network_count=0,
    )
    monkeypatch.setattr(
        importer,
        "_fetch_uhc_plan_graph_snapshot",
        AsyncMock(side_effect=[stable_snapshot, mutated_snapshot]),
    )

    acquisition = await importer._acquire_uhc_plan_graph(
        _uhc_reverse_lookup_source(),
        page_count=1,
        timeout=3,
        run_id="run-1",
        deadline_seconds=0,
    )

    assert acquisition.plan_graph_complete is False
    assert acquisition.collection_complete is False
    assert acquisition.stability_verified is False
    assert acquisition.snapshot.error == "uhc_plan_graph_snapshot_mutated"


def test_uhc_plan_graph_diagnostic_separates_completeness_states():
    acquisition = importer.UHCPlanGraphAcquisition(
        importer.UHCPlanGraphSnapshot(
            rows_by_resource={"InsurancePlan": [{"resource_id": "plan-1"}]},
            pages_by_resource={"InsurancePlan": 2},
            network_count=1,
        ),
        plan_graph_complete=True,
        stability_verified=True,
    )

    diagnostic = importer._uhc_plan_graph_resource_diagnostic(
        acquisition,
        "InsurancePlan",
        rows_written=1,
    )

    assert diagnostic["complete"] is False
    assert diagnostic["collection_complete"] is False
    assert diagnostic["plan_graph_complete"] is True
    assert diagnostic["stability_verified"] is True
    graph_stats = importer._uhc_plan_graph_stats(acquisition, "InsurancePlan")
    assert graph_stats["plan_graph_complete_sources"] == 1
    assert graph_stats["collection_complete_sources"] == 0
    assert graph_stats["sources_completed"] == 1
    candidate = importer.EndpointDatasetCandidate(
        endpoint_id="endpoint-1",
        dataset_id="dataset-1",
        acquisition_root_run_id="run-1",
        source_ids=("uhc",),
        selected_resources=("InsurancePlan",),
        import_run_id="run-1",
        previous_dataset_id=None,
    )
    assert not importer._is_endpoint_dataset_publishable(
        candidate,
        {"InsurancePlan": diagnostic},
    )


@pytest.mark.asyncio
async def test_uhc_plan_graph_records_strategy_completion(monkeypatch):
    acquisition = importer.UHCPlanGraphAcquisition(
        importer.UHCPlanGraphSnapshot(
            rows_by_resource={
                "InsurancePlan": [{"resource_id": "plan-1"}],
                "PractitionerRole": [{"resource_id": "role-1"}],
                "OrganizationAffiliation": [{"resource_id": "affiliation-1"}],
            },
            pages_by_resource={
                "InsurancePlan": 1,
                "PractitionerRole": 1,
                "OrganizationAffiliation": 1,
            },
            network_count=1,
        ),
        plan_graph_complete=True,
        stability_verified=True,
    )
    monkeypatch.setattr(
        importer,
        "_acquire_uhc_plan_graph",
        AsyncMock(return_value=acquisition),
    )
    monkeypatch.setattr(
        importer,
        "_write_uhc_plan_graph_resources",
        AsyncMock(
            return_value={
                "InsurancePlan": 1,
                "PractitionerRole": 1,
                "OrganizationAffiliation": 1,
            }
        ),
    )
    completion_by_resource: dict[str, set[str]] = {}

    import_summary = await importer._import_uhc_plan_graph_source_group(
        _uhc_reverse_lookup_source(),
        ["InsurancePlan", "PractitionerRole", "OrganizationAffiliation"],
        page_count=100,
        timeout=3,
        run_id="run-1",
        deadline_seconds=0,
        stale_cleanup=False,
        seen_table=None,
    )

    importer._record_uhc_plan_graph_completion(
        completion_by_resource,
        import_summary[0],
        import_summary[1],
    )

    assert completion_by_resource == {
        "InsurancePlan": {"uhc"},
        "PractitionerRole": {"uhc"},
        "OrganizationAffiliation": {"uhc"},
    }
    assert import_summary[4]["InsurancePlan"]["sources_completed"] == 1


@pytest.mark.asyncio
async def test_uhc_plan_graph_stale_cleanup_uses_atomic_seen_stage(monkeypatch):
    acquisition = importer.UHCPlanGraphAcquisition(
        importer.UHCPlanGraphSnapshot(
            rows_by_resource={"InsurancePlan": [{"resource_id": "plan-1"}]},
            pages_by_resource={"InsurancePlan": 1},
            network_count=1,
        ),
        plan_graph_complete=True,
        stability_verified=True,
    )
    delete_stale_rows = AsyncMock(return_value=4)
    monkeypatch.setattr(importer, "_delete_stale_resource_rows", delete_stale_rows)

    stale_counts, ready_source_ids = await importer._finalize_uhc_plan_graph_stale_rows(
        _uhc_reverse_lookup_source(),
        acquisition,
        ["InsurancePlan"],
        run_id="run-1",
        stale_cleanup=True,
        seen_table="provider_directory_seen_run_1",
    )

    assert stale_counts == {}
    assert ready_source_ids == {"InsurancePlan": ["uhc"]}
    delete_stale_rows.assert_not_awaited()


@pytest.mark.asyncio
async def test_uhc_plan_graph_stale_cleanup_deletes_without_seen_stage(monkeypatch):
    acquisition = importer.UHCPlanGraphAcquisition(
        importer.UHCPlanGraphSnapshot(
            rows_by_resource={"InsurancePlan": [{"resource_id": "plan-1"}]},
            pages_by_resource={"InsurancePlan": 1},
            network_count=1,
        ),
        plan_graph_complete=True,
        stability_verified=True,
    )
    delete_stale_rows = AsyncMock(return_value=4)
    monkeypatch.setattr(importer, "_delete_stale_resource_rows", delete_stale_rows)

    stale_counts, ready_source_ids = await importer._finalize_uhc_plan_graph_stale_rows(
        _uhc_reverse_lookup_source(),
        acquisition,
        ["InsurancePlan"],
        run_id="run-1",
        stale_cleanup=True,
        seen_table=None,
    )

    assert stale_counts == {"InsurancePlan": 4}
    assert ready_source_ids == {}
    delete_stale_rows.assert_awaited_once_with(
        importer.ProviderDirectoryInsurancePlan,
        "uhc",
        "run-1",
        use_seen_table=True,
    )


@pytest.mark.asyncio
async def test_uhc_plan_graph_completion_does_not_request_pagination_retry(monkeypatch):
    source_lookup = _uhc_reverse_lookup_source()
    source_lookup["_pagination_checkpoint_context"] = importer._pagination_checkpoint_context(
        source_lookup,
        ["uhc"],
        run_id="run-1",
        retry_of_run_id=None,
        pagination_root_run_id=None,
    )
    clear_checkpoints = AsyncMock(return_value=0)
    monkeypatch.setattr(importer, "_clear_pagination_checkpoints", clear_checkpoints)
    resume_required_entries: set[str] = set()
    graph_diagnostic_map = {
        "complete": False,
        "collection_complete": False,
        "plan_graph_complete": True,
        "stability_verified": True,
        "fetch_mode": importer.UHC_PLAN_GRAPH_FETCH_MODE,
        "bounded": False,
        "error": None,
        "next_url_remaining": False,
    }

    await importer._finalize_source_pagination_checkpoints(
        source_lookup,
        {"InsurancePlan": graph_diagnostic_map},
        resume_required_entries,
    )

    assert resume_required_entries == set()
    clear_checkpoints.assert_awaited_once()


@pytest.mark.asyncio
async def test_complete_candidate_checkpoint_is_retained_until_validation(monkeypatch):
    source_lookup = _cigna_checkpoint_source()
    source_lookup["_pagination_checkpoint_context"] = (
        _cigna_checkpoint_context("run-1")
    )
    clear_checkpoints = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_clear_pagination_checkpoints",
        clear_checkpoints,
    )

    await importer._finalize_source_pagination_checkpoints(
        source_lookup,
        {
            "Location": {
                "complete": True,
                "bounded": False,
                "error": None,
            }
        },
        set(),
        retain_complete_checkpoint=True,
    )

    clear_checkpoints.assert_not_awaited()


@pytest.mark.asyncio
async def test_uhc_plan_graph_failure_precedes_generic_checkpoint_resume():
    source_lookup = _uhc_reverse_lookup_source()
    source_lookup["_pagination_checkpoint_context"] = importer._pagination_checkpoint_context(
        source_lookup,
        ["uhc"],
        run_id="run-1",
        retry_of_run_id=None,
        pagination_root_run_id=None,
    )
    resume_required_entries: set[str] = set()

    with pytest.raises(
        RuntimeError,
        match=importer.UHC_PLAN_GRAPH_INCOMPLETE_ERROR,
    ):
        await importer._finalize_source_pagination_checkpoints(
            source_lookup,
            _failed_uhc_plan_graph_summary(["PractitionerRole"])[1],
            resume_required_entries,
        )

    assert resume_required_entries == set()


@pytest.mark.asyncio
async def test_generic_rest_failure_with_checkpoint_requires_resume():
    source_lookup = _cigna_checkpoint_source()
    source_lookup["_pagination_checkpoint_context"] = _cigna_checkpoint_context("run-1")
    resume_required_entries: set[str] = set()

    await importer._finalize_source_pagination_checkpoints(
        source_lookup,
        {
            "Location": {
                "complete": False,
                "bounded": False,
                "error": "http_503",
            }
        },
        resume_required_entries,
    )

    assert resume_required_entries == {"source_a:Location"}


@pytest.mark.asyncio
async def test_last_updated_census_mismatch_is_terminal_not_resumable():
    source_lookup = _last_updated_partition_test_source()
    source_lookup["_pagination_checkpoint_context"] = (
        _last_updated_partition_test_context()
    )
    resume_required_entries: set[str] = set()
    blocked_error = (
        f"{importer.LAST_UPDATED_PARTITION_BLOCKED_ERROR}:"
        "census_unfiltered_post_mismatch:1!=2"
    )

    with pytest.raises(RuntimeError, match="census_unfiltered_post_mismatch"):
        await importer._finalize_source_pagination_checkpoints(
            source_lookup,
            {
                "Practitioner": {
                    "complete": False,
                    "fetch_mode": importer.LAST_UPDATED_PARTITION_FETCH_MODE,
                    "bounded": False,
                    "error": blocked_error,
                }
            },
            resume_required_entries,
        )

    assert resume_required_entries == set()


def _failed_uhc_plan_graph_summary(resource_types: list[str]):
    """Build a failed graph result for branch-integration tests."""
    failed_diagnostics_by_resource = {
        resource_type: {
            "complete": False,
            "collection_complete": False,
            "plan_graph_complete": False,
            "stability_verified": False,
            "fetch_mode": importer.UHC_PLAN_GRAPH_FETCH_MODE,
            "bounded": False,
            "error": "uhc_plan_graph_practitionerrole_cap_exhausted",
            "next_url_remaining": True,
        }
        for resource_type in resource_types
    }
    return (
        ["uhc"],
        failed_diagnostics_by_resource,
        {resource_type: 0 for resource_type in resource_types},
        {},
        {},
        {},
        {},
    )


@pytest.mark.asyncio
async def test_uhc_plan_graph_branch_does_not_create_synthetic_checkpoints(monkeypatch):
    _stub_resource_import_metadata(monkeypatch)
    selected_resources = [
        "InsurancePlan",
        "OrganizationAffiliation",
        "PractitionerRole",
    ]
    failed_summary = _failed_uhc_plan_graph_summary(selected_resources)
    prepared_source = _uhc_reverse_lookup_source()
    prepared_source["_pagination_checkpoint_context"] = importer._pagination_checkpoint_context(
        prepared_source,
        ["uhc"],
        run_id="run-1",
        retry_of_run_id=None,
        pagination_root_run_id=None,
    )
    monkeypatch.setattr(
        importer,
        "_prepare_resource_import_source_group",
        AsyncMock(return_value=([prepared_source], None)),
    )
    load_checkpoint = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_load_or_initialize_pagination_checkpoint",
        load_checkpoint,
    )
    monkeypatch.setattr(
        importer,
        "_import_uhc_plan_graph_source_group",
        AsyncMock(return_value=failed_summary),
    )
    resume_required_entries: set[str] = set()
    with pytest.raises(
        RuntimeError,
        match=importer.UHC_PLAN_GRAPH_INCOMPLETE_ERROR,
    ):
        await importer._import_resources(
            [_uhc_reverse_lookup_source()],
            resources=selected_resources,
            per_resource_limit=0,
            page_limit=0,
            page_count=100,
            timeout=3,
            run_id="run-1",
            pagination_resume_required=resume_required_entries,
        )

    load_checkpoint.assert_not_awaited()
    assert resume_required_entries == set()


def test_endpoint_scope_rejects_mismatch_and_multiple_bases():
    uhc_source = _uhc_reverse_lookup_source()
    other_source_map = {
        "source_id": "other",
        "api_base": "https://example.test/fhir",
        "canonical_api_base": "https://example.test/fhir",
    }

    with pytest.raises(RuntimeError, match="endpoint_scope_mismatch"):
        importer._validate_provider_directory_endpoint_scope(
            [other_source_map],
            importer.UHC_PROVIDER_DIRECTORY_BASE,
        )
    with pytest.raises(RuntimeError, match="endpoint_scope_multiple_bases"):
        importer._validate_provider_directory_endpoint_scope(
            [uhc_source, other_source_map],
            importer.UHC_PROVIDER_DIRECTORY_BASE,
        )
    with pytest.raises(RuntimeError, match="endpoint_scope_unresolved_base"):
        importer._validate_provider_directory_endpoint_scope(
            [{"source_id": "missing-base"}],
            importer.UHC_PROVIDER_DIRECTORY_BASE,
        )
    assert importer._validate_provider_directory_endpoint_scope(
        [uhc_source],
        importer.UHC_PROVIDER_DIRECTORY_BASE + "/",
    ) == importer.UHC_PROVIDER_DIRECTORY_BASE


@pytest.mark.asyncio
async def test_endpoint_scope_fails_before_resource_acquisition(monkeypatch):
    fetch_resource_rows = AsyncMock()
    monkeypatch.setattr(importer, "_fetch_resource_rows", fetch_resource_rows)

    with pytest.raises(RuntimeError, match="endpoint_scope_mismatch"):
        await importer._import_resources(
            [{"source_id": "other", "api_base": "https://example.test/fhir"}],
            resources=["InsurancePlan"],
            per_resource_limit=0,
            page_limit=0,
            page_count=1,
            timeout=3,
            run_id="run-1",
            provider_directory_endpoint_scope=importer.UHC_PROVIDER_DIRECTORY_BASE,
        )

    fetch_resource_rows.assert_not_awaited()


def _practitioner_partition_bundle(resource_id: str) -> dict[str, Any]:
    return {
        "resourceType": "Bundle",
        "entry": [
            {
                "resource": {
                    "resourceType": "Practitioner",
                    "id": resource_id,
                    "name": [{"family": f"Family {resource_id}"}],
                }
            }
        ],
    }


def _practitioner_role_partition_bundle() -> dict[str, Any]:
    return {
        "resourceType": "Bundle",
        "total": 1,
        "entry": [
            {
                "resource": {
                    "resourceType": "PractitionerRole",
                    "id": "role-1",
                    "practitioner": {"reference": "Practitioner/prac-1"},
                }
            }
        ],
    }


def _capped_practitioner_prefix_payload(
    exact_resource_id: str,
    longer_resource_id: str,
    longer_family_name: str,
    next_url: str | None = None,
) -> dict[str, Any]:
    """Build one capped UHC page with exact and longer family values."""
    payload_dict: dict[str, Any] = {
        "resourceType": "Bundle",
        "total": 10000,
        "entry": [
            {
                "resource": {
                    "resourceType": "Practitioner",
                    "id": exact_resource_id,
                    "name": [{"family": "SM"}],
                }
            },
            {
                "resource": {
                    "resourceType": "Practitioner",
                    "id": longer_resource_id,
                    "name": [{"family": longer_family_name}],
                }
            },
        ],
    }
    if next_url:
        payload_dict["link"] = [{"relation": "next", "url": next_url}]
    return payload_dict


def _capped_practitioner_prefix_fetcher(
    partition_start_url: str,
    continuation_url: str,
):
    """Return a two-page capped-prefix HTTP stub."""
    async def fetch_source_json(_source_record, request_url, *, timeout):
        del timeout
        if request_url == partition_start_url:
            return (
                200,
                _capped_practitioner_prefix_payload(
                    "exact-sm-1",
                    "longer-smith",
                    "Smith",
                    continuation_url,
                ),
                None,
                5,
            )
        assert request_url == continuation_url
        return (
            200,
            _capped_practitioner_prefix_payload(
                "exact-sm-2",
                "longer-smythe",
                "Smythe",
            ),
            None,
            5,
        )

    return fetch_source_json


@pytest.mark.asyncio
async def test_uhc_capped_parent_cursor_keeps_only_exact_prefix_rows(monkeypatch):
    """Capped parent cursors retain exact values without claiming completeness."""
    partition_start_url = (
        "https://flex.optum.com/fhirpublic/R4/Practitioner?_count=100&family=SM"
    )
    continuation_url = (
        "https://flex.optum.com/fhirpublic/R4?"
        "_getpages=cursor&_getpagesoffset=100"
    )

    fetch_source_json = _capped_practitioner_prefix_fetcher(
        partition_start_url,
        continuation_url,
    )
    record_checkpoint = AsyncMock()
    monkeypatch.setattr(importer, "_fetch_source_json", fetch_source_json)
    monkeypatch.setattr(importer, "_record_partition_checkpoint", record_checkpoint)
    source_lookup = {
        "source_id": "uhc",
        "api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
    }
    child_url_queue: asyncio.Queue[str] = asyncio.Queue()
    partition_fetch_state = importer.PartitionFetchState(
        result_model=ProviderDirectoryPractitioner,
        retained_resource_rows=[],
    )

    await importer._fetch_partition_url_chain(
        importer.PartitionChainRequest(
            source_record=source_lookup,
            resource_type="Practitioner",
            start_url_queue=child_url_queue,
            state=partition_fetch_state,
            request_url=partition_start_url,
            fetch_options=importer.PartitionFetchOptions(
                timeout=3,
                run_id="run_1",
                row_batch_handler=None,
                row_batch_size=100,
                retain_rows=True,
                deadline_at=None,
                max_pages=0,
            ),
        ),
    )

    assert [
        resource_row["resource_id"]
        for resource_row in partition_fetch_state.retained_resource_rows
    ] == [
        "exact-sm-1",
        "exact-sm-2",
    ]
    assert child_url_queue.qsize() == len(importer.UHC_TEXT_SEARCH_ALPHABET)
    record_checkpoint.assert_not_awaited()


@pytest.mark.asyncio
async def test_fetch_resource_rows_uses_concurrent_partition_workers(monkeypatch):
    requested_urls: list[str] = []
    written_resource_batches: list[dict[str, Any]] = []
    fetch_count_by_name = {"active": 0, "peak": 0}

    async def fake_fetch_json(_source_record, request_url, *, timeout):
        requested_urls.append(request_url)
        fetch_count_by_name["active"] += 1
        fetch_count_by_name["peak"] = max(fetch_count_by_name["peak"], fetch_count_by_name["active"])
        await asyncio.sleep(0.01)
        fetch_count_by_name["active"] -= 1
        resource_id = request_url.rsplit("=", 1)[-1]
        return 200, _practitioner_partition_bundle(resource_id), None, 5

    async def fake_write(_model, resource_batch):
        written_resource_batches.extend(resource_batch)
        return len(resource_batch)

    start_urls = [
        "https://example.test/fhir/Practitioner?part=1",
        "https://example.test/fhir/Practitioner?part=2",
        "https://example.test/fhir/Practitioner?part=3",
    ]
    monkeypatch.setattr(importer, "_partitioned_resource_start_urls", lambda *_args, **_kwargs: start_urls)
    monkeypatch.setattr(importer, "_partition_fetch_concurrency", lambda: 2)
    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_json)

    practitioner_fetch_result = await importer._fetch_resource_rows(
        {"source_id": "source_a", "api_base": "https://example.test/fhir"},
        "Practitioner",
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_1",
        row_batch_handler=fake_write,
        row_batch_size=2,
        retain_rows=False,
    )

    assert practitioner_fetch_result is not None
    assert practitioner_fetch_result.fetch_mode == "partitioned_paged"
    assert practitioner_fetch_result.complete is True
    assert practitioner_fetch_result.rows_fetched == 3
    assert practitioner_fetch_result.rows_written == 3
    assert fetch_count_by_name["peak"] == 2
    assert sorted(requested_urls) == start_urls
    assert [resource_row["resource_id"] for resource_row in written_resource_batches] == ["1", "2", "3"]


@pytest.mark.asyncio
async def test_single_partition_worker_checks_cancellation_before_fetch(monkeypatch):
    start_urls = [
        "https://example.test/fhir/Practitioner?part=1",
        "https://example.test/fhir/Practitioner?part=2",
    ]
    fetch_source_json = AsyncMock(
        side_effect=AssertionError("partition request ran after cancellation")
    )

    async def cancel_partition(_cancel_ctx, _cancel_task):
        raise RuntimeError("partition canceled")

    monkeypatch.setattr(importer, "_partitioned_resource_start_urls", lambda *_args, **_kwargs: start_urls)
    monkeypatch.setattr(importer, "_partition_fetch_concurrency", lambda: 1)
    monkeypatch.setattr(importer, "_fetch_source_json", fetch_source_json)
    monkeypatch.setattr(importer, "raise_if_cancelled", cancel_partition)

    with pytest.raises(RuntimeError, match="partition canceled"):
        await importer._fetch_resource_rows(
            {"source_id": "source_a", "api_base": "https://example.test/fhir"},
            "Practitioner",
            per_resource_limit=0,
            page_limit=0,
            page_count=100,
            timeout=3,
            run_id="run_1",
            cancel_ctx={},
            cancel_task={},
        )

    fetch_source_json.assert_not_awaited()


@pytest.mark.asyncio
async def test_uhc_role_partition_persists_completed_postal_prefix(monkeypatch):
    checkpoint_batches: list[list[dict[str, Any]]] = []

    async def fake_db_all(_statement, **_query_params):
        return []

    async def fake_fetch_json(_source_record, _request_url, *, timeout):
        return 200, _practitioner_role_partition_bundle(), None, 5

    async def fake_upsert_rows(model, rows, **_kwargs):
        assert model is importer.ProviderDirectoryReverseLookupCheckpoint
        checkpoint_batches.append(rows)
        return len(rows)

    async def fail_clear_checkpoints(_source_record, **_options):
        raise AssertionError("residual-unverified scan cleared successful checkpoints")

    monkeypatch.setattr(importer.db, "all", fake_db_all)
    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_json)
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert_rows)
    monkeypatch.setattr(importer, "_clear_reverse_lookup_checkpoints", fail_clear_checkpoints)
    source_lookup = {
        "source_id": "uhc",
        "api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
    }
    partition_fetch_result = await importer._fetch_partitioned_resource_rows(
        source_lookup,
        "PractitionerRole",
        ProviderDirectoryPractitionerRole,
        [
            "https://flex.optum.com/fhirpublic/R4/PractitionerRole?"
            "_count=100&location.address-postalcode=1"
        ],
        importer.PartitionFetchOptions(
            timeout=3,
            run_id="run_1",
            row_batch_handler=None,
            row_batch_size=100,
            retain_rows=True,
            deadline_at=None,
            max_pages=0,
        ),
    )

    assert partition_fetch_result.complete is False
    assert partition_fetch_result.error == "uhc_practitionerrole_residual_unverified"
    assert partition_fetch_result.rows_fetched == 1
    checkpoint_type = importer._uhc_role_postal_checkpoint_type(source_lookup)
    assert checkpoint_batches[0][0]["seed_resource_type"] == checkpoint_type
    assert checkpoint_batches[0][0]["seed_resource_id"] == "1"


@pytest.mark.asyncio
async def test_partition_checkpoints_require_matching_lineage(monkeypatch):
    checkpoint_queries: list[tuple[str, dict[str, Any]]] = []

    async def load_checkpoints(statement, **query_params):
        checkpoint_queries.append((statement, query_params))
        if query_params["checkpoint_owner_run_id"] == "run_root":
            return [("1",)]
        return []

    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_UHC_ROLE_POSTAL_PARTITIONS", "1")
    monkeypatch.setattr(importer.db, "all", load_checkpoints)
    source_lookup = {
        "source_id": "uhc",
        "api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
        "_partition_checkpoint_owner_run_id": "run_root",
    }

    resumed_prefixes = await importer._load_partition_checkpoints(
        source_lookup,
        "PractitionerRole",
        "run_retry_2",
    )
    fresh_prefixes = await importer._load_partition_checkpoints(
        {
            source_key: source_value
            for source_key, source_value in source_lookup.items()
            if not source_key.startswith("_")
        },
        "PractitionerRole",
        "run_fresh",
    )
    checkpoint_row = importer._reverse_lookup_checkpoint_row(
        source_lookup,
        importer._uhc_role_postal_checkpoint_type(source_lookup),
        "1",
        "run_retry_2",
    )

    assert resumed_prefixes == {"1"}
    assert fresh_prefixes == set()
    assert checkpoint_row["last_completed_run_id"] == "run_root"
    assert "last_completed_run_id = :checkpoint_owner_run_id" in checkpoint_queries[0][0]
    assert [query[1]["checkpoint_owner_run_id"] for query in checkpoint_queries] == [
        "run_root",
        "run_fresh",
    ]


@pytest.mark.asyncio
async def test_uhc_non_role_partition_checkpoint_resumes_retry_lineage(monkeypatch):
    checkpoint_queries: list[dict[str, Any]] = []

    async def load_checkpoints(_statement, **query_params):
        checkpoint_queries.append(query_params)
        return [("address-state=CA&address-city=S",)]

    monkeypatch.setattr(importer.db, "all", load_checkpoints)
    source_lookup = {
        "source_id": "uhc",
        "api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
        "_partition_checkpoint_source_ids": ("uhc", "uhc-community"),
        "_partition_checkpoint_owner_run_id": "run_root",
    }

    completed = await importer._load_partition_checkpoints(
        source_lookup,
        "Location",
        "run_retry_2",
    )

    assert completed == {"address-state=CA&address-city=S"}
    assert checkpoint_queries[0]["checkpoint_owner_run_id"] == "run_root"
    assert checkpoint_queries[0]["seed_resource_type"] == (
        importer._uhc_partition_checkpoint_type(source_lookup, "Location")
    )


@pytest.mark.asyncio
async def test_uhc_incomplete_partition_requires_resume():
    source_lookup = {
        "source_id": "uhc_owner",
        "api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
    }
    checkpoint_context = importer._pagination_checkpoint_context(
        source_lookup,
        ["uhc_owner"],
        run_id="run_retry",
        retry_of_run_id="run_root",
        pagination_root_run_id="run_root",
    )
    source_lookup["_pagination_checkpoint_context"] = checkpoint_context
    resume_required_entries: set[str] = set()

    await importer._finalize_source_pagination_checkpoints(
        source_lookup,
        {
            "Practitioner": {
                "complete": False,
                "error": "partition_errors_1_last_http_503",
                "bounded": False,
            }
        },
        resume_required_entries,
    )

    assert importer.UHC_PROVIDER_DIRECTORY_BASE in importer.PAGINATION_CHECKPOINT_API_BASES
    assert checkpoint_context is not None
    assert resume_required_entries == {"uhc_owner:Practitioner"}


@pytest.mark.asyncio
async def test_uncheckpointed_full_acquisition_fails_on_incomplete_resource():
    with pytest.raises(
        RuntimeError,
        match=(
            rf"{importer.RESOURCE_FETCH_INCOMPLETE_ERROR}:"
            r"uncheckpointed:Location=http_403"
        ),
    ):
        await importer._finalize_source_pagination_checkpoints(
            {
                "source_id": "uncheckpointed",
                "api_base": "https://example.test/fhir",
            },
            {
                "Location": {
                    "complete": False,
                    "error": "http_403",
                    "bounded": False,
                }
            },
            set(),
            require_complete_resources=True,
        )


@pytest.mark.asyncio
async def test_non_role_uhc_residual_partition_retains_checkpoints(monkeypatch):
    checkpoint_rows: list[dict[str, Any]] = []

    async def fake_db_all(_statement, **_query_params):
        return []

    async def fake_upsert_rows(model, rows, **_kwargs):
        assert model is importer.ProviderDirectoryReverseLookupCheckpoint
        checkpoint_rows.extend(rows)
        return len(rows)

    async def fake_fetch_json(_source_record, request_url, *, timeout):
        resource_id = request_url.rsplit("=", 1)[-1]
        return 200, _practitioner_partition_bundle(resource_id), None, 5

    async def fail_clear_checkpoints(_source_record, **_kwargs):
        raise AssertionError("non-role resource cleared PractitionerRole checkpoints")

    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_json)
    monkeypatch.setattr(importer.db, "all", fake_db_all)
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert_rows)
    monkeypatch.setattr(importer, "_clear_reverse_lookup_checkpoints", fail_clear_checkpoints)
    source_lookup = {
        "source_id": "uhc",
        "api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
    }

    partition_fetch_result = await importer._fetch_partitioned_resource_rows(
        source_lookup,
        "Practitioner",
        ProviderDirectoryPractitioner,
        ["https://flex.optum.com/fhirpublic/R4/Practitioner?_count=100&family=AB"],
        importer.PartitionFetchOptions(
            timeout=3,
            run_id="run_1",
            row_batch_handler=None,
            row_batch_size=100,
            retain_rows=True,
            deadline_at=None,
            max_pages=0,
        ),
    )

    assert partition_fetch_result.complete is False
    assert partition_fetch_result.error == "uhc_practitioner_name_residual_unverified"
    assert checkpoint_rows[0]["seed_resource_id"] == "family=AB"


@pytest.mark.asyncio
async def test_fetch_resource_rows_continues_after_scan_partition_error(monkeypatch):
    """Verify fetch resource rows continues after scan partition error."""
    calls: list[str] = []

    async def fake_fetch_json(_source, url, *, timeout):
        calls.append(url)
        if "name=A" in url:
            return 413, {"resourceType": "OperationOutcome"}, None, 5
        if "name=B" in url:
            return (
                200,
                {
                    "resourceType": "Bundle",
                    "entry": [
                        {
                            "fullUrl": "https://providerdirectory.scanhealthplan.com/Location/location-1",
                            "resource": {
                                "resourceType": "Location",
                                "id": "location-1",
                                "name": "B Clinic",
                                "address": {
                                    "line": ["1 Main St"],
                                    "city": "Long Beach",
                                    "state": "CA",
                                    "postalCode": "90802",
                                    "country": "US",
                                },
                            },
                        }
                    ],
                },
                None,
                5,
            )
        raise AssertionError(f"unexpected URL {url}")

    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_json)

    location_fetch_result = await importer._fetch_resource_rows(
        {
            "source_id": "scan",
            "api_base": importer.SCAN_PROVIDER_DIRECTORY_BASE,
            "canonical_api_base": importer.SCAN_PROVIDER_DIRECTORY_BASE,
        },
        "Location",
        per_resource_limit=1,
        page_limit=0,
        page_count=25,
        timeout=3,
        run_id="run_1",
    )

    assert calls == [
        "https://providerdirectory.scanhealthplan.com/Location?_count=25&name=A",
        "https://providerdirectory.scanhealthplan.com/Location?_count=25&name=B",
    ]
    assert location_fetch_result is not None
    assert location_fetch_result.model is ProviderDirectoryLocation
    assert location_fetch_result.rows_fetched == 1
    assert location_fetch_result.rows[0]["resource_id"] == "location-1"
    assert location_fetch_result.error == "partition_errors_1_last_http_413"


@pytest.mark.asyncio
async def test_fetch_resource_rows_stops_at_deadline(monkeypatch):
    fetched_urls: list[str] = []

    async def fake_fetch_json(_source, url, *, timeout):
        fetched_urls.append(url)
        return (
            200,
            {
                "resourceType": "Bundle",
                "link": [{"relation": "next", "url": "https://example.test/fhir/Location?page=2"}],
                "entry": [
                    {
                        "fullUrl": f"https://example.test/fhir/Location/location-{len(fetched_urls)}",
                        "resource": {
                            "resourceType": "Location",
                            "id": f"location-{len(fetched_urls)}",
                            "address": {
                                "line": ["1 Main St"],
                                "city": "Long Beach",
                                "state": "CA",
                                "postalCode": "90802",
                                "country": "US",
                            },
                        },
                    }
                ],
            },
            None,
            5,
        )

    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_json)
    monotonic_values = iter([0.0, 0.0, 2.0])
    monkeypatch.setattr(importer.time, "monotonic", lambda: next(monotonic_values, 2.0))

    location_fetch_result = await importer._fetch_resource_rows(
        {
            "source_id": "example",
            "api_base": "https://example.test/fhir",
            "canonical_api_base": "https://example.test/fhir",
        },
        "Location",
        per_resource_limit=0,
        page_limit=0,
        page_count=25,
        timeout=3,
        run_id="run_1",
        deadline_seconds=1,
    )

    assert location_fetch_result is not None
    assert location_fetch_result.rows_fetched == 1
    assert location_fetch_result.complete is False
    assert location_fetch_result.bounded is True
    assert location_fetch_result.deadline_reached is True
    assert location_fetch_result.next_url_remaining is True
    assert location_fetch_result.error == "deadline_reached"
    assert len(fetched_urls) == 1


@pytest.mark.asyncio
async def test_fetch_resource_rows_defers_scan_practitioner_role_reverse_lookup(monkeypatch):
    calls: list[str] = []

    async def fake_fetch_json(_source, url, *, timeout):
        calls.append(url)
        raise AssertionError("SCAN PractitionerRole should not use a broad paged search")

    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_json)

    operation_result = await importer._fetch_resource_rows(
        {
            "source_id": "scan",
            "api_base": importer.SCAN_PROVIDER_DIRECTORY_BASE,
            "canonical_api_base": importer.SCAN_PROVIDER_DIRECTORY_BASE,
        },
        "PractitionerRole",
        per_resource_limit=5,
        page_limit=0,
        page_count=25,
        timeout=3,
        run_id="run_1",
    )

    assert calls == []
    assert operation_result is not None
    assert operation_result.model is ProviderDirectoryPractitionerRole
    assert operation_result.rows == []
    assert operation_result.complete is False
    assert operation_result.error == importer.SCAN_PRACTITIONER_ROLE_REVERSE_LOOKUP_ERROR
    assert operation_result.fetch_mode == "source_specific_deferred"


@pytest.mark.asyncio
async def test_scan_candidate_dispatches_practitioner_role_to_last_updated(monkeypatch):
    source_row = importer._source_row_from_seed(
        {
            "id": "scan",
            "org_name": "SCAN Health Plan",
            "api_base": importer.SCAN_PROVIDER_DIRECTORY_BASE,
        }
    )
    expected_result = object()
    partition_fetch = AsyncMock(return_value=expected_result)
    monkeypatch.setattr(
        importer,
        "_fetch_last_updated_partition_resource_rows",
        partition_fetch,
    )

    assert importer._needs_practitioner_role_reverse_lookup(
        source_row,
        "PractitionerRole",
    ) is False
    assert importer._is_practitioner_role_reverse_lookup_planned(
        source_row,
        list(importer.DEFAULT_RESOURCES),
    ) is False

    operation_result = await importer._fetch_resource_rows(
        source_row,
        "PractitionerRole",
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_scan_candidate",
    )

    assert operation_result is expected_result
    partition_fetch.assert_awaited_once()
    assert partition_fetch.await_args.args[:3] == (
        source_row,
        "PractitionerRole",
        ProviderDirectoryPractitionerRole,
    )


@pytest.mark.asyncio
async def test_fetch_scan_practitioner_role_rows_reverse_looks_up_by_practitioner(monkeypatch):
    calls: list[str] = []

    async def fake_fetch_json(_source, url, *, timeout):
        calls.append(url)
        return (
            200,
            {
                "resourceType": "Bundle",
                "entry": [
                    {
                        "fullUrl": "https://providerdirectory.scanhealthplan.com/PractitionerRole/role-1",
                        "resource": {
                            "resourceType": "PractitionerRole",
                            "id": "role-1",
                            "practitioner": {"reference": "Practitioner/prac-1"},
                            "location": [{"reference": "Location/loc-1"}],
                        },
                    }
                ],
            },
            None,
            5,
        )

    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_json)

    reverse_lookup_result = await importer._fetch_scan_practitioner_role_rows(
        {
            "source_id": "scan",
            "api_base": importer.SCAN_PROVIDER_DIRECTORY_BASE,
            "canonical_api_base": importer.SCAN_PROVIDER_DIRECTORY_BASE,
        },
        {"Practitioner": [{"resource_id": "prac-1"}]},
        importer.ScanPractitionerRoleFetchOptions(
            per_resource_limit=5,
            page_limit=0,
            page_count=25,
            timeout=3,
            run_id="run_1",
        ),
    )

    assert calls == [
        "https://providerdirectory.scanhealthplan.com/PractitionerRole?practitioner=Practitioner%2Fprac-1&_count=25"
    ]
    assert reverse_lookup_result.model is ProviderDirectoryPractitionerRole
    assert reverse_lookup_result.rows_fetched == 1
    assert reverse_lookup_result.rows[0]["resource_id"] == "role-1"
    assert reverse_lookup_result.rows[0]["practitioner_ref"] == "Practitioner/prac-1"
    assert reverse_lookup_result.fetch_mode == "source_specific_reverse_lookup"


@pytest.mark.asyncio
async def test_scan_practitioner_role_reverse_lookup_writes_row_batches(monkeypatch):
    async def fake_fetch_json(_source, _url, *, timeout):
        return (
            200,
            {
                "resourceType": "Bundle",
                "entry": [
                    {
                        "fullUrl": "https://providerdirectory.scanhealthplan.com/PractitionerRole/role-1",
                        "resource": {
                            "resourceType": "PractitionerRole",
                            "id": "role-1",
                            "practitioner": {"reference": "Practitioner/prac-1"},
                        },
                    }
                ],
            },
            None,
            5,
        )

    streamed_row_batches: list[list[dict[str, object]]] = []

    async def row_batch_handler(_model, row_batch):
        streamed_row_batches.append(row_batch)
        return len(row_batch)

    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_json)

    reverse_lookup_batch_result = await importer._fetch_scan_practitioner_role_rows(
        {
            "source_id": "scan",
            "api_base": importer.SCAN_PROVIDER_DIRECTORY_BASE,
            "canonical_api_base": importer.SCAN_PROVIDER_DIRECTORY_BASE,
        },
        {"Practitioner": [{"resource_id": "prac-1"}]},
        importer.ScanPractitionerRoleFetchOptions(
            per_resource_limit=5,
            page_limit=0,
            page_count=25,
            timeout=3,
            run_id="run_1",
            row_batch_handler=row_batch_handler,
            row_batch_size=1,
            retain_rows=False,
        ),
    )

    assert reverse_lookup_batch_result.rows == []
    assert reverse_lookup_batch_result.rows_fetched == 1
    assert reverse_lookup_batch_result.rows_written == 1
    assert streamed_row_batches[0][0]["resource_id"] == "role-1"


def _scan_reverse_lookup_source() -> dict[str, str]:
    return {
        "source_id": "scan",
        "api_base": importer.SCAN_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.SCAN_PROVIDER_DIRECTORY_BASE,
    }


def _scan_stage_seed_options(row_batch_handler):
    return importer.ScanPractitionerRoleFetchOptions(
        per_resource_limit=5,
        page_limit=0,
        page_count=25,
        timeout=3,
        run_id="run_1",
        row_batch_handler=row_batch_handler,
        row_batch_size=1,
        retain_rows=False,
        seed_stage_table="provider_directory_import_seen_stage_test",
        seed_source_ids=("scan",),
    )


def _scan_practitioner_role_bundle_response():
    return (
        200,
        {
            "resourceType": "Bundle",
            "entry": [
                {
                    "fullUrl": "https://providerdirectory.scanhealthplan.com/PractitionerRole/role-1",
                    "resource": {
                        "resourceType": "PractitionerRole",
                        "id": "role-1",
                        "practitioner": {"reference": "Practitioner/prac-1"},
                    },
                }
            ],
        },
        None,
        5,
    )


@pytest.mark.asyncio
async def test_scan_practitioner_role_reverse_lookup_pages_seed_rows_from_stage(monkeypatch):
    """SCAN reverse lookup can page seed resource ids from the import stage."""
    seed_queries: list[dict[str, Any]] = []

    async def fake_db_all(_statement, **query_params):
        seed_queries.append(query_params)
        if query_params["resource_type"] == "Practitioner" and query_params["last_resource_id"] == "":
            return [("prac-1",)]
        return []

    fetch_calls: list[str] = []

    async def fake_fetch_json(_source, request_url, *, timeout):
        fetch_calls.append(request_url)
        return _scan_practitioner_role_bundle_response()

    streamed_row_batches: list[list[dict[str, object]]] = []

    async def row_batch_handler(_model, provider_role_batch):
        streamed_row_batches.append(provider_role_batch)
        return len(provider_role_batch)

    monkeypatch.setattr(importer.db, "all", fake_db_all)
    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_json)

    reverse_lookup_result = await importer._fetch_scan_practitioner_role_rows(
        _scan_reverse_lookup_source(),
        {},
        _scan_stage_seed_options(row_batch_handler),
    )

    assert reverse_lookup_result.rows == []
    assert reverse_lookup_result.rows_fetched == 1
    assert reverse_lookup_result.rows_written == 1
    assert streamed_row_batches[0][0]["resource_id"] == "role-1"
    assert fetch_calls == [
        "https://providerdirectory.scanhealthplan.com/PractitionerRole?practitioner=Practitioner%2Fprac-1&_count=25"
    ]
    assert seed_queries[0]["source_ids"] == ["scan"]
    assert [seed_query["resource_type"] for seed_query in seed_queries] == [
        "Practitioner",
        "Organization",
        "Location",
    ]


@pytest.mark.asyncio
async def test_reverse_lookup_db_seeds_exclude_completed_checkpoint(monkeypatch):
    """An interrupted UHC scan resumes after already completed practitioner seeds."""
    seed_queries: list[tuple[str, dict[str, Any]]] = []

    async def fake_db_all(statement, **query_params):
        seed_queries.append((statement, query_params))
        if query_params["last_resource_id"] == "":
            return [("prac-2",)]
        return []

    monkeypatch.setattr(importer.db, "all", fake_db_all)
    options = importer.ScanPractitionerRoleFetchOptions(
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=60,
        run_id="run_resume",
        source={
            "source_id": "uhc",
            "api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
            "canonical_api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
        },
        existing_seed_source_ids=("uhc",),
        resume_completed_seeds=True,
    )

    seeds = [seed async for seed in importer._scan_role_db_seed_rows(options)]

    assert seeds == [("practitioner", "Practitioner", "prac-2")]
    assert "provider_directory_reverse_lookup_checkpoint" in seed_queries[0][0]
    assert seed_queries[0][1]["resource_type"] == "Practitioner"
    assert seed_queries[0][1]["checkpoint_canonical_api_base"] == importer.UHC_PROVIDER_DIRECTORY_BASE


@pytest.mark.asyncio
async def test_reverse_lookup_checkpoint_flushes_completed_seed(monkeypatch):
    """Completed reverse lookup seeds are persisted before a later resume run."""
    persisted_rows: list[dict[str, Any]] = []

    async def fake_upsert_rows(model, rows, **_kwargs):
        assert model is importer.ProviderDirectoryReverseLookupCheckpoint
        persisted_rows.extend(rows)
        return len(rows)

    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert_rows)
    uhc_source_lookup = {
        "source_id": "uhc",
        "api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
    }
    options = importer.ScanPractitionerRoleFetchOptions(
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=60,
        run_id="run_resume",
        source=uhc_source_lookup,
        resume_completed_seeds=True,
    )
    request = importer._new_role_lookup_request(uhc_source_lookup, {}, options)
    state = importer._PractitionerRoleReverseLookupState(
        result_model=ProviderDirectoryPractitionerRole,
        retained_resource_rows=[],
        streamed_rows=importer._StreamedResourceRowBuffer(
            model=ProviderDirectoryPractitionerRole,
            row_batch_handler=None,
            row_batch_size=1,
            pending_row_items=[],
        ),
        retain_rows=False,
        page_limit=0,
    )

    await state.add_completed_seed(request, "Practitioner", "prac-2")
    await state.flush_completed_seed_rows()

    assert persisted_rows[0]["canonical_api_base"] == importer.UHC_PROVIDER_DIRECTORY_BASE
    assert persisted_rows[0]["seed_resource_type"] == "Practitioner"
    assert persisted_rows[0]["seed_resource_id"] == "prac-2"
    assert persisted_rows[0]["last_completed_run_id"] == "run_resume"


@pytest.mark.asyncio
async def test_reverse_lookup_resume_marks_checkpointed_roles_seen(monkeypatch):
    """Stale cleanup retains roles completed by an earlier checkpointed segment."""
    statements: list[tuple[str, dict[str, Any]]] = []

    async def fake_db_status(statement, **query_params):
        statements.append((statement, query_params))
        return 2

    monkeypatch.setattr(importer.db, "status", fake_db_status)
    options = importer.ScanPractitionerRoleFetchOptions(
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=60,
        run_id="run_resume",
        source={
            "source_id": "uhc",
            "api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
            "canonical_api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
        },
        seed_source_ids=("uhc",),
        resume_completed_seeds=True,
        seen_table="provider_directory_import_seen_stage_test",
    )

    marked_rows = await importer._mark_checkpointed_reverse_lookup_roles_seen(options)

    assert marked_rows == 2
    assert len(statements) == 2
    assert "UPDATE" in statements[0][0]
    assert "INSERT INTO" in statements[1][0]
    assert "provider_directory_import_seen_stage_test" in statements[1][0]
    assert "ON CONFLICT" not in statements[1][0]
    assert statements[0][1]["source_ids"] == ["uhc"]


@pytest.mark.asyncio
async def test_postal_partition_resume_marks_existing_roles_seen(monkeypatch):
    statements: list[tuple[str, dict[str, Any]]] = []

    async def fake_db_status(statement, **query_params):
        statements.append((statement, query_params))
        return 3

    monkeypatch.setattr(importer.db, "status", fake_db_status)
    source_lookup = {
        "source_id": "uhc",
        "api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
    }

    marked_rows = await importer._mark_postal_checkpointed_roles_seen(
        source_lookup,
        ["uhc"],
        "run_resume",
        "provider_directory_import_seen_stage_test",
    )

    assert marked_rows == 3
    assert len(statements) == 2
    assert "UPDATE" in statements[0][0]
    assert "INSERT INTO" in statements[1][0]
    assert "provider_directory_import_seen_stage_test" in statements[1][0]
    assert statements[0][1]["checkpoint_type"] == importer._uhc_role_postal_checkpoint_type(
        source_lookup,
        ["uhc"],
    )
    assert statements[0][1]["source_ids"] == ["uhc"]


@pytest.mark.asyncio
async def test_import_resources_fetches_scan_practitioner_roles_after_practitioners(monkeypatch):
    """Verify import resources fetches scan practitioner roles after practitioners."""
    _stub_resource_import_metadata(monkeypatch)

    async def fake_fetch_resource_rows(source, resource_type, **_kwargs):
        if resource_type == "PractitionerRole":
            raise AssertionError("SCAN PractitionerRole should be reverse-looked-up after practitioners")
        if resource_type != "Practitioner":
            return None
        return importer.ResourceFetchResult(
            model=ProviderDirectoryPractitioner,
            rows=[
                {
                    "source_id": source["source_id"],
                    "resource_id": "prac-1",
                    "npi": "1234567890",
                }
            ],
            rows_fetched=1,
            rows_written=0,
            pages_fetched=1,
            complete=True,
            row_limit_reached=False,
            page_limit_reached=False,
            hard_page_limit_reached=False,
            next_url_remaining=False,
        )

    role_calls: list[str] = []

    async def fake_fetch_source_json(_source, url, *, timeout):
        role_calls.append(url)
        return (
            200,
            {
                "resourceType": "Bundle",
                "entry": [
                    {
                        "fullUrl": "https://providerdirectory.scanhealthplan.com/PractitionerRole/role-1",
                        "resource": {
                            "resourceType": "PractitionerRole",
                            "id": "role-1",
                            "practitioner": {"reference": "Practitioner/prac-1"},
                        },
                    }
                ],
            },
            None,
            5,
        )

    upserts: list[tuple[type, list[dict[str, Any]]]] = []

    async def fake_upsert_resource_rows(model, rows, **_kwargs):
        upserts.append((model, rows))
        return len(rows)

    monkeypatch.setattr(importer, "_fetch_resource_rows", fake_fetch_resource_rows)
    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_source_json)
    monkeypatch.setattr(importer, "_upsert_resource_rows", fake_upsert_resource_rows)

    resource_fetch_stats_by_type: dict[str, dict[str, Any]] = {}
    counts = await importer._import_resources(
        [
            {
                "source_id": "scan",
                "api_base": importer.SCAN_PROVIDER_DIRECTORY_BASE,
                "canonical_api_base": importer.SCAN_PROVIDER_DIRECTORY_BASE,
            }
        ],
        resources=["PractitionerRole", "Practitioner"],
        per_resource_limit=5,
        page_limit=0,
        page_count=25,
        timeout=3,
        run_id="run_1",
        resource_fetch_stats=resource_fetch_stats_by_type,
    )

    assert counts == {"PractitionerRole": 1, "Practitioner": 1}
    assert [model for model, _rows in upserts] == [
        ProviderDirectoryPractitioner,
        ProviderDirectoryPractitionerRole,
    ]
    assert role_calls == [
        "https://providerdirectory.scanhealthplan.com/PractitionerRole?practitioner=Practitioner%2Fprac-1&_count=25"
    ]
    assert resource_fetch_stats_by_type["PractitionerRole"]["sources_attempted"] == 1
    assert resource_fetch_stats_by_type["PractitionerRole"]["rows_fetched"] == 1


def _uhc_reverse_lookup_source() -> dict[str, str]:
    return {
        "source_id": "uhc",
        "api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
    }


@pytest.mark.asyncio
async def test_uhc_practitioner_role_seeds_can_fall_back_to_existing_practitioners(monkeypatch):
    db_calls: list[dict[str, Any]] = []

    async def fake_all(_sql, **params):
        db_calls.append(params)
        if params["last_resource_id"] == "":
            return [("prac-1",), ("prac-2",)]
        if params["last_resource_id"] == "prac-2":
            return [("prac-3",)]
        return []

    monkeypatch.setattr(importer.db, "all", fake_all)

    options = importer.ScanPractitionerRoleFetchOptions(
        per_resource_limit=0,
        page_limit=0,
        page_count=25,
        timeout=3,
        run_id="run_1",
        source=_uhc_reverse_lookup_source(),
        existing_seed_source_ids=("uhc",),
        seed_page_size=2,
    )

    seeds = [
        seed
        async for seed in importer._iter_scan_practitioner_role_seed_rows(
            rows_by_resource={},
            options=options,
        )
    ]

    assert seeds == [
        ("practitioner", "Practitioner", "prac-1"),
        ("practitioner", "Practitioner", "prac-2"),
        ("practitioner", "Practitioner", "prac-3"),
    ]
    assert [call["last_resource_id"] for call in db_calls] == ["", "prac-2"]
    assert all(call["source_ids"] == ["uhc"] for call in db_calls)


def _uhc_practitioner_fetch_result() -> importer.ResourceFetchResult:
    return importer.ResourceFetchResult(
        model=ProviderDirectoryPractitioner,
        rows=[],
        rows_fetched=1,
        rows_written=1,
        pages_fetched=1,
        complete=True,
        row_limit_reached=False,
        page_limit_reached=False,
        hard_page_limit_reached=False,
        next_url_remaining=False,
    )


def _uhc_practitioner_role_bundle() -> dict[str, Any]:
    return {
        "resourceType": "Bundle",
        "entry": [
            {
                "fullUrl": "https://flex.optum.com/fhirpublic/R4/PractitionerRole/role-1",
                "resource": {
                    "resourceType": "PractitionerRole",
                    "id": "role-1",
                    "practitioner": {"reference": "Practitioner/prac-1"},
                },
            }
        ],
    }


def _install_uhc_reverse_lookup_import_fakes(monkeypatch):
    role_call_rows: list[tuple[str, int]] = []
    upsert_call_rows: list[tuple[type, list[dict[str, Any]]]] = []

    async def fake_fetch_resource_rows(source, resource_type, **kwargs):
        if resource_type == "PractitionerRole":
            raise AssertionError("UHC PractitionerRole should be reverse-looked-up after practitioners")
        if resource_type != "Practitioner":
            return None
        row_batch_handler = kwargs.get("row_batch_handler")
        rows = [{"source_id": source["source_id"], "resource_id": "prac-1", "npi": "1234567890"}]
        assert row_batch_handler is not None
        await row_batch_handler(ProviderDirectoryPractitioner, rows)
        return _uhc_practitioner_fetch_result()

    async def fake_fetch_source_json(_source, url, *, timeout):
        role_call_rows.append((url, timeout))
        return (200, _uhc_practitioner_role_bundle(), None, 5)

    async def fake_upsert_resource_rows(model, rows, **_kwargs):
        upsert_call_rows.append((model, rows))
        return len(rows)

    monkeypatch.setattr(importer, "_fetch_resource_rows", fake_fetch_resource_rows)
    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_source_json)
    monkeypatch.setattr(importer, "_upsert_resource_rows", fake_upsert_resource_rows)
    return role_call_rows, upsert_call_rows


@pytest.mark.asyncio
async def test_import_resources_fetches_uhc_practitioner_roles_after_practitioners(monkeypatch):
    """UHC roles are recovered through practitioner reverse lookup with the UHC timeout floor."""

    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_UHC_ROLE_POSTAL_PARTITIONS", "0")
    _stub_resource_import_metadata(monkeypatch)
    role_call_rows, upsert_call_rows = _install_uhc_reverse_lookup_import_fakes(monkeypatch)

    resource_fetch_stats_by_resource: dict[str, dict[str, Any]] = {}
    completed_source_ids_by_resource: dict[str, set[str]] = {}
    counts = await importer._import_resources(
        [_uhc_reverse_lookup_source()],
        resources=["PractitionerRole", "Practitioner"],
        per_resource_limit=5,
        page_limit=0,
        page_count=25,
        timeout=3,
        run_id="run_1",
        stream_batch_size=5,
        resource_fetch_stats=resource_fetch_stats_by_resource,
        resource_completion=completed_source_ids_by_resource,
    )

    assert counts == {"PractitionerRole": 1, "Practitioner": 1}
    assert [model for model, _rows in upsert_call_rows] == [
        ProviderDirectoryPractitioner,
        ProviderDirectoryPractitionerRole,
    ]
    assert role_call_rows == [
        (
            "https://flex.optum.com/fhirpublic/R4/PractitionerRole?practitioner=Practitioner%2Fprac-1&_count=25",
            60,
        )
    ]
    assert resource_fetch_stats_by_resource["PractitionerRole"]["rows_fetched"] == 1
    assert completed_source_ids_by_resource["PractitionerRole"] == {"uhc"}


def _practitioner_stream_fetch_result(rows_written: int):
    return importer.ResourceFetchResult(
        model=ProviderDirectoryPractitioner,
        rows=[],
        rows_fetched=1,
        rows_written=rows_written,
        pages_fetched=1,
        complete=True,
        row_limit_reached=False,
        page_limit_reached=False,
        hard_page_limit_reached=False,
        next_url_remaining=False,
    )


def _scan_practitioner_role_fetch_result():
    return importer.ResourceFetchResult(
        model=ProviderDirectoryPractitionerRole,
        rows=[],
        rows_fetched=1,
        rows_written=1,
        pages_fetched=1,
        complete=True,
        row_limit_reached=False,
        page_limit_reached=False,
        hard_page_limit_reached=False,
        next_url_remaining=False,
        fetch_mode="source_specific_reverse_lookup",
    )


@pytest.mark.asyncio
async def test_import_resources_does_not_retain_streamed_scan_roles_for_linked_fallback(monkeypatch):
    _stub_resource_import_metadata(monkeypatch)
    captured_scan_options: list[importer.ScanPractitionerRoleFetchOptions] = []

    async def fake_fetch_resource_rows(source, resource_type, **kwargs):
        if resource_type == "PractitionerRole":
            raise AssertionError("SCAN PractitionerRole should be reverse-looked-up after practitioners")
        if resource_type != "Practitioner":
            return None
        row_batch_handler = kwargs.get("row_batch_handler")
        rows = [{"source_id": source["source_id"], "resource_id": "prac-1", "npi": "1234567890"}]
        rows_written = await row_batch_handler(ProviderDirectoryPractitioner, rows) if row_batch_handler else 0
        return _practitioner_stream_fetch_result(rows_written)

    async def fake_fetch_scan_practitioner_role_rows(_source, _rows_by_resource, options):
        captured_scan_options.append(options)
        await options.row_batch_handler(
            ProviderDirectoryPractitionerRole,
            [{"source_id": "scan", "resource_id": "role-1"}],
        )
        return _scan_practitioner_role_fetch_result()

    async def fake_upsert_resource_rows(_model, rows, **_kwargs):
        return len(rows)

    monkeypatch.setattr(importer, "_fetch_resource_rows", fake_fetch_resource_rows)
    monkeypatch.setattr(importer, "_fetch_scan_practitioner_role_rows", fake_fetch_scan_practitioner_role_rows)
    monkeypatch.setattr(importer, "_upsert_resource_rows", fake_upsert_resource_rows)

    counts = await importer._import_resources(
        [
            {
                "source_id": "scan",
                "api_base": importer.SCAN_PROVIDER_DIRECTORY_BASE,
                "canonical_api_base": importer.SCAN_PROVIDER_DIRECTORY_BASE,
            }
        ],
        resources=["PractitionerRole", "Practitioner"],
        per_resource_limit=5,
        page_limit=0,
        page_count=25,
        timeout=3,
        run_id="run_1",
        linked_resource_limit=50000,
        stream_batch_size=5000,
    )

    assert counts == {"PractitionerRole": 1, "Practitioner": 1}
    assert captured_scan_options
    assert captured_scan_options[0].retain_rows is False


@pytest.mark.asyncio
async def test_import_resources_keeps_compact_streamed_rows_for_linked_fallback(monkeypatch):
    _stub_resource_import_metadata(monkeypatch)
    fetch_kwargs_dict: dict[str, Any] = {}
    linked_rows_by_resource: dict[str, list[dict[str, Any]]] = {}

    _patch_compact_streamed_row_fallback(
        monkeypatch,
        fetch_kwargs_dict=fetch_kwargs_dict,
        linked_rows_by_resource=linked_rows_by_resource,
    )

    counts = await importer._import_resources(
        [{"source_id": "source_a", "api_base": "https://example.test/fhir"}],
        resources=["PractitionerRole"],
        per_resource_limit=0,
        page_limit=0,
        page_count=25,
        timeout=3,
        run_id="run_1",
        linked_resource_limit=50000,
        stream_batch_size=1,
    )

    assert counts == {"PractitionerRole": 1}
    assert fetch_kwargs_dict["retain_rows"] is False
    assert linked_rows_by_resource == {
        "PractitionerRole": [
            {
                "resource_id": "role-1",
                "practitioner_ref": "Practitioner/prac-1",
                "location_refs": ["Location/loc-1"],
                "network_refs": ["Organization/network-1"],
            }
        ]
    }


def _patch_compact_streamed_row_fallback(
    monkeypatch,
    *,
    fetch_kwargs_dict: dict[str, Any],
    linked_rows_by_resource: dict[str, list[dict[str, Any]]],
) -> None:
    async def fake_fetch_resource_rows(source, resource_type, **kwargs):
        fetch_kwargs_dict.update(kwargs)
        assert resource_type == "PractitionerRole"
        row_batch_handler = kwargs["row_batch_handler"]
        rows_written = await row_batch_handler(
            ProviderDirectoryPractitionerRole,
            [_practitioner_role_stream_row(source["source_id"])],
        )
        return importer.ResourceFetchResult(
            model=ProviderDirectoryPractitionerRole,
            rows=[],
            rows_fetched=1,
            rows_written=rows_written,
            pages_fetched=1,
            complete=True,
            row_limit_reached=False,
            page_limit_reached=False,
            hard_page_limit_reached=False,
            next_url_remaining=False,
        )

    async def fake_upsert_resource_rows(_model, rows, **_kwargs):
        return len(rows)

    async def fake_import_linked_resource_rows(_source, rows_by_resource, **_kwargs):
        linked_rows_by_resource.update({key: list(value) for key, value in rows_by_resource.items()})
        return {}

    monkeypatch.setattr(importer, "_fetch_resource_rows", fake_fetch_resource_rows)
    monkeypatch.setattr(importer, "_upsert_resource_rows", fake_upsert_resource_rows)
    monkeypatch.setattr(importer, "_import_linked_resource_rows", fake_import_linked_resource_rows)


def _practitioner_role_stream_row(source_id: str) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "resource_id": "role-1",
        "practitioner_ref": "Practitioner/prac-1",
        "location_refs": ["Location/loc-1"],
        "network_refs": ["Organization/network-1"],
        "specialty_codes": [{"code": "207Q00000X"}],
        "telecom": [{"system": "phone", "value": "3125550100"}],
    }


async def _bulk_test_write_batch(_model, resource_rows):
    return len(resource_rows)


def _bulk_test_context(
    *,
    owner_run_id: str = "run_1",
    retry_of_run_id: str | None = None,
    root_run_id: str = "root_1",
    dataset_id: str = "dataset_1",
):
    return importer.PaginationCheckpointContext(
        canonical_api_base=importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE,
        source_scope_hash="scope_1",
        source_ids=("aetna-commercial",),
        owner_run_id=owner_run_id,
        retry_of_run_id=retry_of_run_id,
        endpoint_id="endpoint_1",
        dataset_id=dataset_id,
        acquisition_root_run_id=root_run_id,
    )


def _bulk_test_identity(checkpoint_context, resource_type="Practitioner"):
    start_url = importer._bulk_export_start_url(
        {"api_base": importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE},
        resource_type,
    )
    return importer._bulk_export_checkpoint_identity(
        checkpoint_context,
        resource_type,
        start_url,
    )


def _bulk_test_source(api_base=importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE):
    return {
        "source_id": "aetna-commercial",
        "api_base": api_base,
        "metadata_json": {
            "provider_directory_bulk_export_output_hosts": ["storage.example"]
        },
    }


@pytest.fixture
def bulk_checkpoint_key(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY",
        "provider-directory-bulk-unit-test-key",
    )


def _bulk_status_payload(
    *output_urls,
    requires_access_token=False,
    request_url=None,
    transaction_time="2026-07-10T00:00:00Z",
):
    if request_url is None:
        request_url = importer._bulk_export_start_url(
            _bulk_test_source(),
            "Practitioner",
        )
    return {
        "transactionTime": transaction_time,
        "request": request_url,
        "requiresAccessToken": requires_access_token,
        "output": [
            {"type": "Practitioner", "url": output_url}
            for output_url in output_urls
        ],
    }


def _bulk_fetch_options():
    return importer.BulkExportFetchOptions(
        timeout=3,
        run_id="run_1",
        row_batch_handler=_bulk_test_write_batch,
        row_batch_size=2,
        retain_rows=False,
        bulk_export_max_pending_seconds=(
            importer.DEFAULT_BULK_EXPORT_MAX_PENDING_SECONDS
        ),
    )


class _BulkCheckpointMemory:
    def __init__(self):
        self.checkpoint_by_id = {}
        self.output_by_id = {}
        self.event_log = []

    def install(self, monkeypatch):
        @contextlib.asynccontextmanager
        async def worker_guard(_identity):
            async def ownership_probe():
                return None

            yield ownership_probe

        monkeypatch.setattr(
            importer,
            "_bulk_checkpoint_worker_guard",
            worker_guard,
        )
        monkeypatch.setattr(importer, "_load_bulk_export_checkpoint", self.load)
        monkeypatch.setattr(importer, "_adopt_bulk_export_checkpoint", self.adopt)
        monkeypatch.setattr(importer, "_reserve_bulk_export_checkpoint", self.reserve)
        monkeypatch.setattr(importer, "_release_bulk_export_reservation", self.release)
        monkeypatch.setattr(importer, "_accept_bulk_export_checkpoint", self.accept)
        monkeypatch.setattr(
            importer,
            "_persist_bulk_export_next_poll_at",
            self.persist_next_poll_at,
        )
        monkeypatch.setattr(importer, "_persist_bulk_export_manifest", self.persist_manifest)
        monkeypatch.setattr(importer, "_load_bulk_output_checkpoints", self.load_outputs)
        monkeypatch.setattr(importer, "_begin_bulk_export_output", self.begin_output)
        monkeypatch.setattr(importer, "_record_bulk_export_output_progress", self.record_progress)
        monkeypatch.setattr(importer, "_complete_bulk_export_output", self.complete_output)
        monkeypatch.setattr(importer, "_record_bulk_export_output_error", self.record_output_error)
        monkeypatch.setattr(importer, "_complete_bulk_export_checkpoint", self.complete_checkpoint)
        monkeypatch.setattr(importer, "_record_bulk_export_checkpoint_error", self.record_checkpoint_error)
        monkeypatch.setattr(
            importer,
            "_repair_terminal_bulk_export_checkpoint",
            self.repair_terminal_checkpoint,
        )

    def _checkpoint_map(
        self,
        identity,
        status_url=None,
        state=importer.BULK_EXPORT_CHECKPOINT_ACCEPTED,
    ):
        status_url_ciphertext = (
            importer._encrypt_bulk_capability(status_url) if status_url else None
        )
        status_url_hash = (
            hashlib.sha256(status_url.encode("utf-8")).hexdigest()
            if status_url
            else None
        )
        return {
            "checkpoint_id": identity.checkpoint_id,
            "canonical_api_base": identity.canonical_api_base,
            "resource_type": identity.resource_type,
            "source_scope_hash": identity.source_scope_hash,
            "strategy_version": identity.strategy_version,
            "acquisition_root_run_id": identity.acquisition_root_run_id,
            "owner_run_id": identity.owner_run_id,
            "retry_of_run_id": identity.retry_of_run_id,
            "endpoint_id": identity.endpoint_id,
            "dataset_id": identity.dataset_id,
            "start_url_hash": identity.start_url_hash,
            "status_url_ciphertext": status_url_ciphertext,
            "status_url_hash": status_url_hash,
            "manifest_hash": None,
            "manifest_ciphertext": None,
            "manifest_json": None,
            "state": state,
            "rows_written": 0,
            "error": None,
            "accepted_at": (
                importer._bulk_export_now_utc()
                if state != importer.BULK_EXPORT_CHECKPOINT_STARTING
                else None
            ),
            "next_poll_at": None,
        }

    def seed_accepted(self, identity, status_url):
        self.checkpoint_by_id[identity.checkpoint_id] = self._checkpoint_map(
            identity,
            status_url,
        )

    def seed_manifest(self, identity, manifest, states, row_counts=None):
        self.seed_accepted(identity, f"{identity.canonical_api_base}/status/1")
        checkpoint = self.checkpoint_by_id[identity.checkpoint_id]
        checkpoint.update(
            manifest_hash=manifest.identity_hash,
            manifest_ciphertext=importer._encrypt_bulk_capability(
                json.dumps(manifest.payload, sort_keys=True)
            ),
            manifest_json=importer._bulk_manifest_audit_payload(manifest.payload),
            state=importer.BULK_EXPORT_CHECKPOINT_MANIFEST_READY,
        )
        for output, state, row_count in zip(
            manifest.outputs,
            states,
            row_counts or [0] * len(states),
        ):
            output_id = importer._bulk_manifest_output_id(identity.checkpoint_id, output)
            self.output_by_id[output_id] = self._output_map(
                identity.checkpoint_id,
                output_id,
                output,
                state,
                row_count,
            )
        checkpoint["rows_written"] = sum(row_counts or [])

    @staticmethod
    def _output_map(checkpoint_id, output_id, output, state, row_count=0):
        return {
            "checkpoint_id": checkpoint_id,
            "output_id": output_id,
            "output_index": output.output_index,
            "resource_type": output.resource_type,
            "output_url_ciphertext": importer._encrypt_bulk_capability(output.url),
            "output_url_hash": output.url_hash,
            "state": state,
            "rows_written": row_count,
            "content_length_bytes": None,
            "etag_ciphertext": None,
            "etag_hash": None,
            "committed_bytes": 0,
            "output_expires_at": None,
            "validator_checked_at": None,
            "attempt_count": 0,
            "error": None,
            "last_error": None,
            "last_error_at": None,
        }

    async def load(self, identity):
        checkpoint = self.checkpoint_by_id.get(identity.checkpoint_id)
        return dict(checkpoint) if checkpoint else {}

    async def adopt(self, identity):
        checkpoint = self.checkpoint_by_id[identity.checkpoint_id]
        checkpoint["owner_run_id"] = identity.owner_run_id
        checkpoint["retry_of_run_id"] = identity.retry_of_run_id
        return dict(checkpoint)

    async def reserve(self, identity):
        is_reservation_owner = identity.checkpoint_id not in self.checkpoint_by_id
        checkpoint = self.checkpoint_by_id.setdefault(
            identity.checkpoint_id,
            self._checkpoint_map(identity, state=importer.BULK_EXPORT_CHECKPOINT_STARTING),
        )
        self.event_log.append(("reserve", identity.checkpoint_id))
        return dict(checkpoint), is_reservation_owner

    async def release(self, identity):
        self.checkpoint_by_id.pop(identity.checkpoint_id, None)
        self.event_log.append(("release", identity.checkpoint_id))

    async def accept(self, identity, status_url):
        checkpoint = self.checkpoint_by_id.setdefault(
            identity.checkpoint_id,
            self._checkpoint_map(identity, status_url),
        )
        if checkpoint["state"] == importer.BULK_EXPORT_CHECKPOINT_STARTING:
            checkpoint["status_url_ciphertext"] = (
                importer._encrypt_bulk_capability(status_url)
                if status_url
                else None
            )
            checkpoint["status_url_hash"] = (
                hashlib.sha256(status_url.encode("utf-8")).hexdigest()
                if status_url
                else None
            )
            checkpoint["state"] = importer.BULK_EXPORT_CHECKPOINT_ACCEPTED
            checkpoint["accepted_at"] = importer._bulk_export_now_utc()
        elif importer._bulk_checkpoint_status_url(checkpoint) != status_url:
            raise RuntimeError("bulk_export_status_url_mismatch")
        self.event_log.append(("accept", status_url))
        return dict(checkpoint)

    async def persist_next_poll_at(self, identity, next_poll_at):
        checkpoint = self.checkpoint_by_id[identity.checkpoint_id]
        assert checkpoint["owner_run_id"] == identity.owner_run_id
        checkpoint["next_poll_at"] = next_poll_at
        self.event_log.append(("next_poll", next_poll_at))

    async def persist_manifest(self, identity, manifest):
        checkpoint = self.checkpoint_by_id[identity.checkpoint_id]
        importer._assert_bulk_export_manifest_unchanged(checkpoint, manifest)
        checkpoint.update(
            manifest_hash=manifest.identity_hash,
            manifest_ciphertext=importer._encrypt_bulk_capability(
                json.dumps(manifest.payload, sort_keys=True)
            ),
            manifest_json=importer._bulk_manifest_audit_payload(manifest.payload),
            state=importer.BULK_EXPORT_CHECKPOINT_MANIFEST_READY,
        )
        for output in manifest.outputs:
            output_id = importer._bulk_manifest_output_id(identity.checkpoint_id, output)
            self.output_by_id.setdefault(
                output_id,
                self._output_map(
                    identity.checkpoint_id,
                    output_id,
                    output,
                    importer.BULK_EXPORT_OUTPUT_PENDING,
                ),
            )
        self.event_log.append(("manifest", manifest.identity_hash))
        return dict(checkpoint)

    async def load_outputs(self, checkpoint_id):
        return sorted(
            [
                dict(output_checkpoint)
                for output_checkpoint in self.output_by_id.values()
                if output_checkpoint["checkpoint_id"] == checkpoint_id
            ],
            key=lambda output_checkpoint: output_checkpoint["output_index"],
        )

    async def begin_output(self, identity, output_id, *, preserve_progress):
        checkpoint_id = identity.checkpoint_id
        assert (
            self.checkpoint_by_id[checkpoint_id]["owner_run_id"]
            == identity.owner_run_id
        )
        output_checkpoint = self.output_by_id[output_id]
        output_checkpoint.update(
            state=importer.BULK_EXPORT_OUTPUT_STREAMING,
            rows_written=(
                output_checkpoint["rows_written"] if preserve_progress else 0
            ),
            committed_bytes=(
                output_checkpoint["committed_bytes"] if preserve_progress else 0
            ),
            attempt_count=output_checkpoint["attempt_count"] + 1,
            error=None,
        )
        self.event_log.append(("begin", output_id))

    async def record_progress(
        self,
        identity,
        output_id,
        rows_written,
        committed_bytes,
    ):
        assert (
            self.checkpoint_by_id[identity.checkpoint_id]["owner_run_id"]
            == identity.owner_run_id
        )
        self.output_by_id[output_id]["rows_written"] = rows_written
        self.output_by_id[output_id]["committed_bytes"] = committed_bytes
        self.event_log.append(("progress", rows_written))

    async def complete_output(
        self,
        identity,
        output_id,
        rows_written,
        committed_bytes,
        *,
        require_validator,
    ):
        assert (
            self.checkpoint_by_id[identity.checkpoint_id]["owner_run_id"]
            == identity.owner_run_id
        )
        self.output_by_id[output_id].update(
            state=importer.BULK_EXPORT_OUTPUT_COMPLETE,
            rows_written=rows_written,
            committed_bytes=committed_bytes,
        )
        if require_validator:
            assert committed_bytes == self.output_by_id[output_id][
                "content_length_bytes"
            ]
        self.event_log.append(("complete_output", output_id))

    async def record_output_error(
        self,
        identity,
        output_id,
        rows_written,
        committed_bytes,
        error,
        *,
        record_checkpoint=True,
    ):
        is_terminal = importer._is_bulk_export_error_terminal(error)
        self.output_by_id[output_id].update(
            state=(
                importer.BULK_EXPORT_OUTPUT_FAILED
                if is_terminal
                else importer.BULK_EXPORT_OUTPUT_PENDING
            ),
            rows_written=rows_written,
            committed_bytes=committed_bytes,
            error=error,
            last_error=error,
        )
        if record_checkpoint:
            await self.record_checkpoint_error(
                identity,
                error,
                terminal=is_terminal,
            )

    async def complete_checkpoint(self, identity, *, require_validators):
        checkpoint_id = identity.checkpoint_id
        output_checkpoints = [
            output_checkpoint
            for output_checkpoint in self.output_by_id.values()
            if output_checkpoint["checkpoint_id"] == checkpoint_id
        ]
        if not output_checkpoints or any(
            output_checkpoint["state"] != importer.BULK_EXPORT_OUTPUT_COMPLETE
            for output_checkpoint in output_checkpoints
        ):
            raise RuntimeError("bulk_export_checkpoint_completion_lost")
        if require_validators and any(
            output_checkpoint["committed_bytes"]
            != output_checkpoint["content_length_bytes"]
            for output_checkpoint in output_checkpoints
        ):
            raise RuntimeError("bulk_export_checkpoint_completion_lost")
        checkpoint = self.checkpoint_by_id[checkpoint_id]
        checkpoint["state"] = importer.BULK_EXPORT_CHECKPOINT_COMPLETE
        checkpoint["rows_written"] = sum(
            output_checkpoint["rows_written"]
            for output_checkpoint in output_checkpoints
        )
        checkpoint["status_url_ciphertext"] = None
        checkpoint["manifest_ciphertext"] = None
        for output_checkpoint in output_checkpoints:
            output_checkpoint["output_url_ciphertext"] = None
            output_checkpoint["etag_ciphertext"] = None
        self.event_log.append(("complete_checkpoint", checkpoint_id))

    async def record_checkpoint_error(self, identity, error, *, terminal):
        checkpoint_id = identity.checkpoint_id
        checkpoint = self.checkpoint_by_id[checkpoint_id]
        assert checkpoint["owner_run_id"] == identity.owner_run_id
        checkpoint["error"] = error
        if terminal:
            checkpoint["state"] = importer.BULK_EXPORT_CHECKPOINT_FAILED
            checkpoint["status_url_ciphertext"] = None
            checkpoint["manifest_ciphertext"] = None
            for output_checkpoint in self.output_by_id.values():
                if output_checkpoint["checkpoint_id"] == checkpoint_id:
                    output_checkpoint["output_url_ciphertext"] = None
                    output_checkpoint["etag_ciphertext"] = None

    async def repair_terminal_checkpoint(self, identity, terminal_state):
        checkpoint_id = identity.checkpoint_id
        checkpoint = self.checkpoint_by_id[checkpoint_id]
        assert checkpoint["state"] == terminal_state
        checkpoint["rows_written"] = sum(
            output_checkpoint["rows_written"]
            for output_checkpoint in self.output_by_id.values()
            if output_checkpoint["checkpoint_id"] == checkpoint_id
        )
        checkpoint["status_url_ciphertext"] = None
        checkpoint["manifest_ciphertext"] = None
        for output_checkpoint in self.output_by_id.values():
            if output_checkpoint["checkpoint_id"] == checkpoint_id:
                output_checkpoint["output_url_ciphertext"] = None
                output_checkpoint["etag_ciphertext"] = None


def test_bulk_export_start_url_uses_base_export_operation():
    url = importer._bulk_export_start_url(
        {"api_base": "https://example.test/fhir/"},
        "PractitionerRole",
    )

    assert url == (
        "https://example.test/fhir/$export?_type=PractitionerRole"
        "&_outputFormat=application%2Ffhir%2Bndjson"
    )


def test_bulk_export_start_url_omits_output_format_for_aetna_providerdirectorydata():
    url = importer._bulk_export_start_url(
        {"api_base": importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE},
        "Practitioner",
    )

    assert url == f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}/$export?_type=Practitioner"


def test_bulk_export_pre_stream_failures_fall_back_to_paged_reads():
    assert importer._should_bulk_export_pre_stream_fallback(500, None) is True
    assert importer._should_bulk_export_pre_stream_fallback(401, None) is True
    assert importer._should_bulk_export_pre_stream_fallback(None, "timeout") is True
    assert importer._should_bulk_export_pre_stream_fallback(202, None) is False
    assert importer._should_bulk_export_pre_stream_fallback(200, None) is False


def test_bulk_export_output_urls_filters_requested_resource_type():
    urls = importer._bulk_export_output_urls(
        {
            "output": [
                {"type": "Practitioner", "url": "https://bulk.example/practitioner.ndjson"},
                {"type": "Location", "url": "https://bulk.example/location.ndjson"},
                {"type": "Location"},
            ]
        },
        "Location",
    )

    assert urls == ["https://bulk.example/location.ndjson"]


def test_is_bulk_export_status_payload_and_error_detection():
    assert importer._is_bulk_export_status_payload(
        {"transactionTime": "2026-06-30T00:00:00Z", "output": []}
    )
    assert not importer._is_bulk_export_status_payload(
        {"resourceType": "Bundle", "entry": []}
    )
    assert (
        importer._bulk_export_payload_error(
            {"error": [{"type": "OperationOutcome", "responseCode": 500}]}
        )
        == "bulk_export_error_http_500"
    )


def test_bulk_checkpoint_models_persist_contract():
    acquisition_columns = {
        column.name
        for column in ProviderDirectoryBulkAcquisitionCheckpoint.__table__.columns
    }
    output_columns = {
        column.name
        for column in ProviderDirectoryBulkOutputCheckpoint.__table__.columns
    }

    assert {
        "checkpoint_id",
        "canonical_api_base",
        "resource_type",
        "source_scope_hash",
        "strategy_version",
        "acquisition_root_run_id",
        "owner_run_id",
        "endpoint_id",
        "dataset_id",
        "status_url_ciphertext",
        "status_url_hash",
        "manifest_hash",
        "manifest_ciphertext",
        "manifest_json",
        "lease_expires_at",
        "rows_written",
        "state",
        "error",
        "accepted_at",
        "next_poll_at",
        "manifest_received_at",
        "completed_at",
        "failed_at",
    }.issubset(acquisition_columns)
    assert {
        "checkpoint_id",
        "output_id",
        "output_index",
        "output_url_ciphertext",
        "output_url_hash",
        "state",
        "rows_written",
        "content_length_bytes",
        "etag_ciphertext",
        "etag_hash",
        "committed_bytes",
        "output_expires_at",
        "validator_checked_at",
        "attempt_count",
        "error",
        "last_error",
        "last_error_at",
        "started_at",
        "completed_at",
    }.issubset(output_columns)
    assert ProviderDirectoryBulkAcquisitionCheckpoint in importer.CANONICAL_RESOURCE_MODELS
    assert ProviderDirectoryBulkOutputCheckpoint in importer.CANONICAL_RESOURCE_MODELS


def test_bulk_export_pending_default_and_retry_after_formats():
    observed_at = datetime.datetime(2026, 7, 13, 12, 0, tzinfo=datetime.UTC)
    retry_at = observed_at + datetime.timedelta(days=2, seconds=17)

    assert importer.DEFAULT_BULK_EXPORT_MAX_PENDING_SECONDS == 7 * 24 * 60 * 60
    assert importer._bulk_export_retry_after_seconds(
        "172817",
        now_utc=observed_at,
    ) == 172817.0
    assert importer._bulk_export_retry_after_seconds(
        email.utils.format_datetime(retry_at, usegmt=True),
        now_utc=observed_at,
    ) == 172817.0


@pytest.mark.asyncio
async def test_bulk_poll_honors_durable_next_poll_boundary(monkeypatch):
    accepted_at = datetime.datetime(2026, 7, 13, 10, 0, tzinfo=datetime.UTC)
    next_poll_at = accepted_at + datetime.timedelta(minutes=20)
    observed_times = iter((accepted_at, next_poll_at))
    sleep_mock = AsyncMock()
    status_payload = _bulk_status_payload()
    http_get = AsyncMock(return_value=(200, {}, status_payload, None))
    monkeypatch.setattr(importer, "_bulk_export_now_utc", lambda: next(observed_times))
    monkeypatch.setattr(importer.asyncio, "sleep", sleep_mock)
    monkeypatch.setattr(importer, "_bulk_http_get_json", http_get)

    status_payload_result, poll_error, polls = await importer._bulk_export_poll_manifest(
        object(),
        _bulk_test_source(),
        "https://providerdirectory.api.aetna.com/fhir/status/1",
        resource_type="Practitioner",
        timeout=1,
        poll_options=importer.BulkExportPollOptions(
            accepted_at=accepted_at,
            next_poll_at=next_poll_at,
        ),
    )

    assert status_payload_result == status_payload
    assert poll_error is None
    assert polls == 1
    sleep_mock.assert_awaited_once_with(20 * 60)
    http_get.assert_awaited_once()


@pytest.mark.asyncio
async def test_bulk_final_poll_persists_full_retry_after_without_sleep(monkeypatch):
    observed_at = datetime.datetime(2026, 7, 13, 12, 0, tzinfo=datetime.UTC)
    next_poll_handler = AsyncMock()
    sleep_mock = AsyncMock()
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_BULK_EXPORT_MAX_POLLS", "1")
    monkeypatch.setattr(importer, "_bulk_export_now_utc", lambda: observed_at)
    monkeypatch.setattr(importer.asyncio, "sleep", sleep_mock)
    monkeypatch.setattr(
        importer,
        "_bulk_http_get_json",
        AsyncMock(return_value=(202, {"retry-after": "864000"}, None, None)),
    )

    status_payload, poll_error, polls = await importer._bulk_export_poll_manifest(
        object(),
        _bulk_test_source(),
        "https://providerdirectory.api.aetna.com/fhir/status/1",
        resource_type="Practitioner",
        timeout=1,
        poll_options=importer.BulkExportPollOptions(
            accepted_at=observed_at,
            max_pending_seconds=2 * 24 * 60 * 60,
            next_poll_handler=next_poll_handler,
        ),
    )

    assert status_payload is None
    assert poll_error == "bulk_export_timeout"
    assert polls == 1
    next_poll_handler.assert_awaited_once_with(
        observed_at + datetime.timedelta(days=2)
    )
    sleep_mock.assert_not_awaited()


def _assert_completed_bulk_checkpoint(checkpoint: dict[str, Any]) -> None:
    expected_status_url = "https://apif1.aetna.com/bulk-status/1"
    assert checkpoint["state"] == importer.BULK_EXPORT_CHECKPOINT_COMPLETE
    assert checkpoint["rows_written"] == 2
    assert checkpoint["status_url_ciphertext"] is None
    assert checkpoint["status_url_hash"] == hashlib.sha256(
        expected_status_url.encode("utf-8")
    ).hexdigest()
    assert checkpoint["manifest_hash"] is not None
    assert checkpoint["manifest_ciphertext"] is None
    assert checkpoint["manifest_json"]["transactionTime"] == "2026-07-10T00:00:00Z"
    assert "request" not in checkpoint["manifest_json"]
    assert checkpoint["acquisition_root_run_id"] == "root_1"
    assert checkpoint["dataset_id"] == "dataset_1"


@pytest.mark.asyncio
async def test_bulk_checkpoint_initial_flow(monkeypatch, bulk_checkpoint_key):
    """An accepted export persists and then clears encrypted capabilities."""
    checkpoint_memory = _BulkCheckpointMemory()
    checkpoint_memory.install(monkeypatch)
    checkpoint_context = _bulk_test_context()
    status_payload = _bulk_status_payload(
        "https://storage.example/snapshot/practitioners.ndjson?sig=one"
    )
    requested_events = []

    async def accept_export(*_args, **request_options):
        requested_events.append("start")
        assert request_options["prefer_async"] is True
        return 202, {"content-location": "/bulk-status/1"}, None, None

    async def poll_export(*_args, **_request_options):
        requested_events.append("poll")
        return status_payload, None, 2

    async def stream_output(*_args, resume_options, **_request_options):
        requested_events.append("stream")
        await resume_options.row_progress_handler(2, 0)
        return [], 2, 2, False, None

    monkeypatch.setattr(importer, "_bulk_http_get_json", accept_export)
    monkeypatch.setattr(importer, "_bulk_export_poll_manifest", poll_export)
    monkeypatch.setattr(importer, "_stream_bulk_export_output_rows", stream_output)

    fetch_result = await importer._fetch_checkpointed_bulk_export_resource_rows(
        _bulk_test_source(),
        "Practitioner",
        checkpoint_context,
        _bulk_fetch_options(),
    )

    assert fetch_result is not None
    assert fetch_result.complete is True
    assert fetch_result.fetch_mode == "checkpointed_bulk_export"
    assert fetch_result.rows_fetched == 2
    assert requested_events == ["start", "poll", "stream"]
    resource_stats_by_type = {}
    importer._record_resource_fetch_stats(
        resource_stats_by_type,
        "Practitioner",
        fetch_result,
    )
    assert resource_stats_by_type["Practitioner"]["bulk_export_sources"] == 1
    checkpoint = next(iter(checkpoint_memory.checkpoint_by_id.values()))
    _assert_completed_bulk_checkpoint(checkpoint)


@pytest.mark.asyncio
async def test_bulk_checkpoint_blocks_unknown_acceptance(
    monkeypatch,
    bulk_checkpoint_key,
):
    checkpoint_memory = _BulkCheckpointMemory()
    checkpoint_memory.install(monkeypatch)
    checkpoint_context = _bulk_test_context()
    checkpoint_identity = _bulk_test_identity(checkpoint_context)
    await checkpoint_memory.reserve(checkpoint_identity)
    monkeypatch.setattr(
        importer,
        "_bulk_http_get_json",
        AsyncMock(side_effect=AssertionError("unknown acceptance must not restart")),
    )

    fetch_result = await importer._fetch_checkpointed_bulk_export_resource_rows(
        _bulk_test_source(checkpoint_identity.canonical_api_base),
        "Practitioner",
        checkpoint_context,
        _bulk_fetch_options(),
    )

    assert fetch_result is not None
    assert fetch_result.complete is False
    assert fetch_result.error == "bulk_export_acceptance_outcome_unknown"
    checkpoint = checkpoint_memory.checkpoint_by_id[checkpoint_identity.checkpoint_id]
    assert checkpoint["state"] == importer.BULK_EXPORT_CHECKPOINT_FAILED


@pytest.mark.asyncio
async def test_bulk_checkpoint_deadline_is_shared_across_retry_and_never_restarts(
    monkeypatch,
    bulk_checkpoint_key,
):
    checkpoint_memory = _BulkCheckpointMemory()
    original_context = _bulk_test_context(owner_run_id="run_1", root_run_id="root_1")
    retry_context = _bulk_test_context(
        owner_run_id="run_retry",
        retry_of_run_id="run_1",
        root_run_id="root_1",
    )
    original_identity = _bulk_test_identity(original_context)
    retry_identity = _bulk_test_identity(retry_context)
    assert retry_identity.checkpoint_id == original_identity.checkpoint_id
    checkpoint_memory.seed_accepted(
        original_identity,
        f"{original_identity.canonical_api_base}/status/pending",
    )
    observed_at = datetime.datetime(2026, 7, 13, 12, 0, tzinfo=datetime.UTC)
    accepted_at = observed_at - datetime.timedelta(days=8)
    checkpoint_memory.checkpoint_by_id[original_identity.checkpoint_id][
        "accepted_at"
    ] = accepted_at
    checkpoint_memory.install(monkeypatch)
    start_or_poll = AsyncMock(
        side_effect=AssertionError("expired accepted export must not restart or poll")
    )
    monkeypatch.setattr(importer, "_bulk_export_now_utc", lambda: observed_at)
    monkeypatch.setattr(importer, "_bulk_http_get_json", start_or_poll)

    first_result = await importer._fetch_checkpointed_bulk_export_resource_rows(
        _bulk_test_source(retry_identity.canonical_api_base),
        "Practitioner",
        retry_context,
        _bulk_fetch_options(),
    )
    second_result = await importer._fetch_checkpointed_bulk_export_resource_rows(
        _bulk_test_source(retry_identity.canonical_api_base),
        "Practitioner",
        retry_context,
        _bulk_fetch_options(),
    )

    assert first_result is not None
    assert first_result.error == importer.BULK_EXPORT_STATUS_DEADLINE_EXCEEDED
    assert second_result is not None
    assert second_result.error == importer.BULK_EXPORT_STATUS_DEADLINE_EXCEEDED
    start_or_poll.assert_not_awaited()
    checkpoint = checkpoint_memory.checkpoint_by_id[retry_identity.checkpoint_id]
    assert checkpoint["accepted_at"] == accepted_at
    assert checkpoint["owner_run_id"] == "run_retry"
    assert checkpoint["state"] == importer.BULK_EXPORT_CHECKPOINT_FAILED
    assert checkpoint["status_url_ciphertext"] is None


@pytest.mark.asyncio
async def test_bulk_checkpoint_releases_unsupported_start(
    monkeypatch,
    bulk_checkpoint_key,
):
    checkpoint_memory = _BulkCheckpointMemory()
    checkpoint_memory.install(monkeypatch)
    checkpoint_context = _bulk_test_context()

    monkeypatch.setattr(
        importer,
        "_bulk_http_get_json",
        AsyncMock(return_value=(404, {}, None, None)),
    )

    fetch_result = await importer._fetch_checkpointed_bulk_export_resource_rows(
        {
            "source_id": "aetna-commercial",
            "api_base": importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE,
        },
        "Practitioner",
        checkpoint_context,
        _bulk_fetch_options(),
    )

    assert fetch_result is None
    assert checkpoint_memory.checkpoint_by_id == {}
    assert any(event_name == "release" for event_name, _detail in checkpoint_memory.event_log)


@pytest.mark.asyncio
async def test_bulk_checkpoint_skips_completed_output(
    monkeypatch,
    bulk_checkpoint_key,
):
    checkpoint_memory = _BulkCheckpointMemory()
    checkpoint_context = _bulk_test_context()
    checkpoint_identity = _bulk_test_identity(checkpoint_context)
    manifest = importer._bulk_export_manifest_from_payload(
        _bulk_status_payload(
            "https://storage.example/snapshot/part-1.ndjson?sig=one",
            "https://storage.example/snapshot/part-2.ndjson?sig=two",
        ),
        "Practitioner",
    )
    checkpoint_memory.seed_manifest(
        checkpoint_identity,
        manifest,
        [importer.BULK_EXPORT_OUTPUT_COMPLETE, importer.BULK_EXPORT_OUTPUT_PENDING],
        [3, 0],
    )
    checkpoint_memory.install(monkeypatch)
    streamed_urls = []

    async def stream_output(_session, _source_record, output_url, **request_options):
        streamed_urls.append(output_url)
        await request_options["resume_options"].row_progress_handler(4, 0)
        return [], 4, 4, False, None

    monkeypatch.setattr(importer, "_stream_bulk_export_output_rows", stream_output)
    monkeypatch.setattr(
        importer,
        "_bulk_http_get_json",
        AsyncMock(side_effect=AssertionError("completed manifest must not restart")),
    )

    fetch_result = await importer._fetch_checkpointed_bulk_export_resource_rows(
        _bulk_test_source(checkpoint_identity.canonical_api_base),
        "Practitioner",
        checkpoint_context,
        _bulk_fetch_options(),
    )

    assert fetch_result is not None and fetch_result.complete
    assert streamed_urls == [manifest.outputs[1].url]
    assert fetch_result.rows_fetched == 7
    assert [event_name for event_name, _detail in checkpoint_memory.event_log].count("begin") == 1


@pytest.mark.asyncio
async def test_bulk_checkpoint_restarts_interrupted_output(
    monkeypatch,
    bulk_checkpoint_key,
):
    checkpoint_memory = _BulkCheckpointMemory()
    checkpoint_context = _bulk_test_context(owner_run_id="run_retry", retry_of_run_id="run_1")
    checkpoint_identity = _bulk_test_identity(checkpoint_context)
    manifest = importer._bulk_export_manifest_from_payload(
        _bulk_status_payload("https://storage.example/snapshot/part.ndjson?sig=one"),
        "Practitioner",
    )
    checkpoint_memory.seed_manifest(
        checkpoint_identity,
        manifest,
        [importer.BULK_EXPORT_OUTPUT_STREAMING],
        [2],
    )
    checkpoint_memory.install(monkeypatch)
    stream_calls = []

    async def stream_output(_session, _source_record, output_url, **request_options):
        stream_calls.append(output_url)
        await request_options["resume_options"].row_progress_handler(3, 0)
        return [], 3, 3, False, None

    monkeypatch.setattr(importer, "_stream_bulk_export_output_rows", stream_output)

    fetch_result = await importer._fetch_checkpointed_bulk_export_resource_rows(
        _bulk_test_source(checkpoint_identity.canonical_api_base),
        "Practitioner",
        checkpoint_context,
        _bulk_fetch_options(),
    )

    assert fetch_result is not None and fetch_result.complete
    assert stream_calls == [manifest.outputs[0].url]
    assert fetch_result.rows_fetched == 3
    output_checkpoint = next(iter(checkpoint_memory.output_by_id.values()))
    assert output_checkpoint["attempt_count"] == 1
    assert output_checkpoint["rows_written"] == 3


def test_bulk_checkpoint_rejects_manifest_change():
    first_manifest = importer._bulk_export_manifest_from_payload(
        _bulk_status_payload("https://storage.example/snapshot/part.ndjson?sig=one"),
        "Practitioner",
    )
    changed_manifest = importer._bulk_export_manifest_from_payload(
        _bulk_status_payload("https://storage.example/snapshot/part.ndjson?sig=two"),
        "Practitioner",
    )

    with pytest.raises(ValueError, match="bulk_export_manifest_mismatch"):
        importer._assert_bulk_export_manifest_unchanged(
            {"manifest_hash": first_manifest.identity_hash},
            changed_manifest,
        )


def test_bulk_checkpoint_validates_status_identity(bulk_checkpoint_key):
    status_url = f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}/status/1"
    valid_checkpoint_map = {
        "status_url_ciphertext": importer._encrypt_bulk_capability(status_url),
        "status_url_hash": hashlib.sha256(status_url.encode("utf-8")).hexdigest(),
    }

    assert importer._bulk_checkpoint_status_url_error(valid_checkpoint_map) is None
    assert (
        importer._bulk_checkpoint_status_url_error(
            {
                **valid_checkpoint_map,
                "status_url_ciphertext": importer._encrypt_bulk_capability(
                    f"{status_url}-changed"
                ),
            }
        )
        == "bulk_export_status_url_hash_mismatch"
    )


@pytest.mark.asyncio
async def test_bulk_checkpoint_rejects_expired_status(
    monkeypatch,
    bulk_checkpoint_key,
):
    checkpoint_memory = _BulkCheckpointMemory()
    checkpoint_context = _bulk_test_context()
    checkpoint_identity = _bulk_test_identity(checkpoint_context)
    checkpoint_memory.seed_accepted(
        checkpoint_identity,
        f"{checkpoint_identity.canonical_api_base}/status/expired",
    )
    checkpoint_memory.install(monkeypatch)
    monkeypatch.setattr(
        importer,
        "_bulk_export_poll_manifest",
        AsyncMock(return_value=(None, "bulk_export_status_http_410", 1)),
    )
    monkeypatch.setattr(
        importer,
        "_bulk_http_get_json",
        AsyncMock(side_effect=AssertionError("retry must use accepted status")),
    )

    fetch_result = await importer._fetch_checkpointed_bulk_export_resource_rows(
        _bulk_test_source(checkpoint_identity.canonical_api_base),
        "Practitioner",
        checkpoint_context,
        _bulk_fetch_options(),
    )

    assert fetch_result is not None
    assert fetch_result.complete is False
    assert fetch_result.error == "bulk_export_status_http_410"
    checkpoint = checkpoint_memory.checkpoint_by_id[checkpoint_identity.checkpoint_id]
    assert checkpoint["state"] == importer.BULK_EXPORT_CHECKPOINT_FAILED


@pytest.mark.asyncio
async def test_bulk_checkpoint_rejects_expired_output(
    monkeypatch,
    bulk_checkpoint_key,
):
    checkpoint_memory = _BulkCheckpointMemory()
    checkpoint_context = _bulk_test_context()
    checkpoint_identity = _bulk_test_identity(checkpoint_context)
    manifest = importer._bulk_export_manifest_from_payload(
        _bulk_status_payload("https://storage.example/snapshot/expired.ndjson?sig=one"),
        "Practitioner",
    )
    checkpoint_memory.seed_manifest(
        checkpoint_identity,
        manifest,
        [importer.BULK_EXPORT_OUTPUT_PENDING],
    )
    checkpoint_memory.install(monkeypatch)
    stream_count_by_name = {"value": 0}

    async def expired_output(*_args, **_request_options):
        stream_count_by_name["value"] += 1
        return [], 0, 0, False, "bulk_export_output_http_410"

    monkeypatch.setattr(importer, "_stream_bulk_export_output_rows", expired_output)
    source_lookup = _bulk_test_source(checkpoint_identity.canonical_api_base)
    first_result = await importer._fetch_checkpointed_bulk_export_resource_rows(
        source_lookup,
        "Practitioner",
        checkpoint_context,
        _bulk_fetch_options(),
    )
    retry_result = await importer._fetch_checkpointed_bulk_export_resource_rows(
        source_lookup,
        "Practitioner",
        checkpoint_context,
        _bulk_fetch_options(),
    )

    assert first_result is not None and first_result.error == "bulk_export_output_http_410"
    assert retry_result is not None and retry_result.error == "bulk_export_output_http_410"
    assert stream_count_by_name["value"] == 1
    checkpoint = checkpoint_memory.checkpoint_by_id[checkpoint_identity.checkpoint_id]
    assert checkpoint["state"] == importer.BULK_EXPORT_CHECKPOINT_FAILED


def test_bulk_output_credential_safety(monkeypatch):
    credential_calls = []
    credential_options_map = {
        "headers": {"Authorization": "Bearer payer-secret"},
        "query_params": {"key": "payer-secret"},
        "descriptor": {"header_names": ["Authorization"]},
    }

    def payer_credentials(_source_record, request_url):
        credential_calls.append(request_url)
        return credential_options_map

    monkeypatch.setattr(importer, "_credential_request_options_for_source", payer_credentials)
    source_lookup = _bulk_test_source()
    payer_output_url = f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}/files/part.ndjson"
    presigned_output_url = "https://storage.example/part.ndjson?signature=abc123"

    assert importer._bulk_export_output_request_options(
        source_lookup,
        payer_output_url,
        requires_access_token=True,
    ) == credential_options_map
    assert importer._bulk_export_output_request_options(
        source_lookup,
        payer_output_url,
        requires_access_token=False,
    ) == {"headers": {}, "query_params": {}, "descriptor": None}
    assert importer._bulk_export_output_request_options(
        source_lookup,
        presigned_output_url,
        requires_access_token=False,
    ) == {"headers": {}, "query_params": {}, "descriptor": None}
    assert credential_calls == [payer_output_url]
    with pytest.raises(ValueError, match="external_output_requires_access_token"):
        importer._bulk_export_output_request_options(
            source_lookup,
            presigned_output_url,
            requires_access_token=True,
        )
    with pytest.raises(ValueError, match="untrusted_status_url"):
        importer._resolved_bulk_export_status_url(
            source_lookup,
            f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}/$export",
            "https://unrelated.example/status/1",
        )


def test_bulk_checkpoint_isolates_roots():
    original_context = _bulk_test_context(owner_run_id="run_1", root_run_id="root_1")
    retry_context = _bulk_test_context(
        owner_run_id="run_2",
        retry_of_run_id="run_1",
        root_run_id="root_1",
    )
    fresh_context = _bulk_test_context(owner_run_id="run_3", root_run_id="root_3")
    other_dataset_context = _bulk_test_context(
        owner_run_id="run_2",
        root_run_id="root_1",
        dataset_id="dataset_2",
    )

    original_identity = _bulk_test_identity(original_context)
    assert _bulk_test_identity(retry_context).checkpoint_id == original_identity.checkpoint_id
    assert _bulk_test_identity(fresh_context).checkpoint_id != original_identity.checkpoint_id
    assert (
        _bulk_test_identity(other_dataset_context).checkpoint_id
        != original_identity.checkpoint_id
    )


@pytest.mark.asyncio
async def test_bulk_checkpoint_requires_all_outputs(
    monkeypatch,
    bulk_checkpoint_key,
):
    checkpoint_memory = _BulkCheckpointMemory()
    checkpoint_context = _bulk_test_context()
    checkpoint_identity = _bulk_test_identity(checkpoint_context)
    manifest = importer._bulk_export_manifest_from_payload(
        _bulk_status_payload("https://storage.example/snapshot/part.ndjson?sig=one"),
        "Practitioner",
    )
    checkpoint_memory.seed_manifest(
        checkpoint_identity,
        manifest,
        [importer.BULK_EXPORT_OUTPUT_PENDING],
    )
    checkpoint_memory.install(monkeypatch)

    async def stream_output(*_args, resume_options, **_request_options):
        await resume_options.row_progress_handler(1, 0)
        return [], 1, 1, False, None

    async def lose_output_completion(
        _checkpoint_id,
        _output_id,
        _rows_written,
        _committed_bytes,
        *,
        require_validator,
    ):
        assert require_validator is False
        return None

    monkeypatch.setattr(importer, "_stream_bulk_export_output_rows", stream_output)
    monkeypatch.setattr(importer, "_complete_bulk_export_output", lose_output_completion)

    fetch_result = await importer._fetch_checkpointed_bulk_export_resource_rows(
        _bulk_test_source(checkpoint_identity.canonical_api_base),
        "Practitioner",
        checkpoint_context,
        _bulk_fetch_options(),
    )

    assert fetch_result is not None
    assert fetch_result.complete is False
    assert fetch_result.error == "bulk_export_outputs_incomplete"
    checkpoint = checkpoint_memory.checkpoint_by_id[checkpoint_identity.checkpoint_id]
    assert checkpoint["state"] == importer.BULK_EXPORT_CHECKPOINT_FAILED


@pytest.mark.asyncio
async def test_fetch_resource_rows_uses_bulk_export_when_available(monkeypatch):
    async def fake_bulk_export(source, resource_type, **kwargs):
        assert source["source_id"] == "aetna-commercial"
        assert resource_type == "Practitioner"
        assert kwargs["per_resource_limit"] == 10
        return importer.ResourceFetchResult(
            model=ProviderDirectoryPractitioner,
            rows=[{"source_id": "source_a", "resource_id": "prac-1"}],
            rows_fetched=1,
            rows_written=0,
            pages_fetched=2,
            complete=True,
            row_limit_reached=False,
            page_limit_reached=False,
            hard_page_limit_reached=False,
            next_url_remaining=False,
            fetch_mode="bulk_export",
        )

    async def fail_paged_fetch(*_args, **_kwargs):
        raise AssertionError("paged fetch should not be used when bulk export succeeds")

    monkeypatch.setattr(importer, "_fetch_bulk_export_resource_rows", fake_bulk_export)
    monkeypatch.setattr(importer, "_fetch_json", fail_paged_fetch)

    operation_result = await importer._fetch_resource_rows(
        _bulk_test_source(),
        "Practitioner",
        per_resource_limit=10,
        page_limit=1,
        page_count=25,
        timeout=3,
        run_id="run_1",
        bulk_export=True,
    )

    assert operation_result is not None
    assert operation_result.fetch_mode == "bulk_export"
    assert operation_result.rows[0]["resource_id"] == "prac-1"
    resource_fetch_stats_by_type: dict[str, dict[str, Any]] = {}
    importer._record_resource_fetch_stats(resource_fetch_stats_by_type, "Practitioner", operation_result)
    assert resource_fetch_stats_by_type["Practitioner"]["bulk_export_sources"] == 1


@pytest.mark.asyncio
async def test_ineligible_bulk_source_uses_rest_fetch(monkeypatch):
    """A Bulk request for an ineligible source must transparently use REST."""
    bulk_fetch = AsyncMock(
        side_effect=AssertionError("ineligible sources must not request Bulk export")
    )
    rest_fetch = AsyncMock(
        return_value=(
            200,
            {
                "resourceType": "Bundle",
                "entry": [
                    {
                        "resource": {
                            "resourceType": "Practitioner",
                            "id": "prac-rest",
                        }
                    }
                ],
            },
            None,
            10,
        )
    )
    source_lookup = {
        "source_id": "rest-source",
        "api_base": "https://example.test/fhir",
    }
    monkeypatch.setattr(importer, "_fetch_bulk_export_resource_rows", bulk_fetch)
    monkeypatch.setattr(importer, "_fetch_json", rest_fetch)

    fetch_result = await importer._fetch_resource_rows(
        source_lookup,
        "Practitioner",
        per_resource_limit=1,
        page_limit=1,
        page_count=25,
        timeout=3,
        run_id="run_1",
        bulk_export=True,
    )

    assert fetch_result is not None
    assert fetch_result.fetch_mode == "paged"
    assert fetch_result.rows[0]["resource_id"] == "prac-rest"
    bulk_fetch.assert_not_awaited()
    rest_fetch.assert_awaited_once_with(
        "https://example.test/fhir/Practitioner?_count=25",
        timeout=3,
    )


@pytest.mark.asyncio
async def test_bulk_export_non_bulk_200_falls_back(monkeypatch):
    async def fake_bulk_http_get_json(*_args, **kwargs):
        assert kwargs["prefer_async"] is True
        return 200, {}, {"resourceType": "Bundle", "entry": []}, None

    async def fake_fetch_json(url, *, timeout):
        assert url == (
            f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}"
            "/Practitioner?_count=25"
        )
        return (
            200,
            {
                "resourceType": "Bundle",
                "entry": [
                    {
                        "resource": {
                            "resourceType": "Practitioner",
                            "id": "prac-1",
                            "name": [{"family": "Fallback"}],
                        },
                    }
                ],
            },
            None,
            10,
        )

    monkeypatch.setattr(importer, "_bulk_http_get_json", fake_bulk_http_get_json)
    monkeypatch.setattr(importer, "_fetch_json", fake_fetch_json)

    operation_result = await importer._fetch_resource_rows(
        _bulk_test_source(),
        "Practitioner",
        per_resource_limit=1,
        page_limit=1,
        page_count=25,
        timeout=3,
        run_id="run_1",
        bulk_export=True,
    )

    assert operation_result is not None
    assert operation_result.fetch_mode == "paged"
    assert operation_result.rows[0]["resource_id"] == "prac-1"


@pytest.mark.asyncio
async def test_fetch_resource_rows_records_accepted_bulk_export_poll_failure(monkeypatch):
    async def fake_bulk_http_get_json(*_args, **kwargs):
        assert kwargs["prefer_async"] is True
        return 202, {"content-location": "/bulk-status/1"}, None, None

    async def fake_poll_outputs(*_args, **_kwargs):
        return None, "bulk_export_timeout", 3

    async def fail_paged_fetch(*_args, **_kwargs):
        raise AssertionError("accepted bulk export failures should not fall back to paged reads")

    monkeypatch.setattr(importer, "_bulk_http_get_json", fake_bulk_http_get_json)
    monkeypatch.setattr(importer, "_bulk_export_poll_outputs", fake_poll_outputs)
    monkeypatch.setattr(importer, "_fetch_json", fail_paged_fetch)

    operation_result = await importer._fetch_resource_rows(
        _bulk_test_source(),
        "Practitioner",
        per_resource_limit=1,
        page_limit=1,
        page_count=25,
        timeout=3,
        run_id="run_1",
        bulk_export=True,
    )

    assert operation_result is not None
    assert operation_result.fetch_mode == "bulk_export"
    assert operation_result.error == "bulk_export_timeout"
    assert operation_result.complete is False
    assert operation_result.next_url_remaining is True
    resource_fetch_stats_by_type: dict[str, dict[str, Any]] = {}
    importer._record_resource_fetch_stats(resource_fetch_stats_by_type, "Practitioner", operation_result)
    assert resource_fetch_stats_by_type["Practitioner"]["bulk_export_sources"] == 1
    assert resource_fetch_stats_by_type["Practitioner"]["sources_failed"] == 1


@pytest.mark.asyncio
async def test_fetch_resource_rows_accepts_bundle_shaped_payload_without_resource_type(monkeypatch):
    async def fake_fetch_json(url, *, timeout):
        assert url == "https://example.test/fhir/Practitioner?_count=25"
        return (
            200,
            {
                "type": "searchset",
                "total": 1,
                "link": [{"relation": "self", "url": url}],
                "entry": [
                    {
                        "fullUrl": "https://example.test/fhir/Practitioner/prac-1",
                        "resource": {
                            "resourceType": "Practitioner",
                            "id": "prac-1",
                            "name": [{"family": "Aetna"}],
                        },
                    }
                ],
            },
            None,
            10,
        )

    monkeypatch.setattr(importer, "_fetch_json", fake_fetch_json)

    operation_result = await importer._fetch_resource_rows(
        {"source_id": "source_a", "api_base": "https://example.test/fhir"},
        "Practitioner",
        per_resource_limit=1,
        page_limit=1,
        page_count=25,
        timeout=3,
        run_id="run_1",
    )

    assert operation_result is not None
    assert operation_result.rows_fetched == 1
    assert operation_result.rows[0]["resource_id"] == "prac-1"


@pytest.mark.asyncio
async def test_fetch_resource_rows_falls_back_when_bulk_export_unsupported(monkeypatch):
    async def fake_bulk_export(*_args, **_kwargs):
        return None

    async def fake_fetch_json(url, *, timeout):
        assert url == (
            f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}"
            "/Practitioner?_count=25"
        )
        return (
            200,
            {
                "resourceType": "Bundle",
                "entry": [
                    {
                        "resource": {
                            "resourceType": "Practitioner",
                            "id": "prac-1",
                            "name": [{"family": "Fallback"}],
                        },
                    }
                ],
            },
            None,
            10,
        )

    monkeypatch.setattr(importer, "_fetch_bulk_export_resource_rows", fake_bulk_export)
    monkeypatch.setattr(importer, "_fetch_json", fake_fetch_json)

    operation_result = await importer._fetch_resource_rows(
        _bulk_test_source(),
        "Practitioner",
        per_resource_limit=1,
        page_limit=1,
        page_count=25,
        timeout=3,
        run_id="run_1",
        bulk_export=True,
    )

    assert operation_result is not None
    assert operation_result.fetch_mode == "paged"
    assert operation_result.rows[0]["resource_id"] == "prac-1"


def _arkansas_practitioner_bundle(resource_ids, next_link=None):
    bundle_dict = {
        "resourceType": "Bundle",
        "entry": [
            {
                "resource": {
                    "resourceType": "Practitioner",
                    "id": resource_id,
                    "name": [{"family": resource_id}],
                }
            }
            for resource_id in resource_ids
        ],
    }
    if next_link:
        bundle_dict["link"] = [{"relation": "next", "url": next_link}]
    return bundle_dict


@pytest.mark.asyncio
async def test_fetch_resource_rows_ignores_dead_arkansas_cursor(monkeypatch):
    source_lookup = {
        "source_id": "arkansas",
        "api_base": importer.ARKANSAS_PROVIDER_DIRECTORY_BASE,
    }
    expected_urls = [
        f"{importer.ARKANSAS_PROVIDER_DIRECTORY_BASE}/Practitioner?"
        f"_count=2&_sort=_id&_skip={skip_count}"
        for skip_count in (0, 2)
    ]
    fetched_urls: list[str] = []

    async def fake_fetch_source_json(_source, request_url, *, timeout):
        assert timeout == 3
        fetched_urls.append(request_url)
        if request_url == expected_urls[0]:
            return 200, _arkansas_practitioner_bundle(["prac-1", "prac-2"], "/dead"), None, 1
        assert request_url == expected_urls[1]
        return 200, _arkansas_practitioner_bundle(["prac-3"]), None, 1

    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_source_json)

    fetch_result = await importer._fetch_resource_rows(
        source_lookup,
        "Practitioner",
        per_resource_limit=0,
        page_limit=0,
        page_count=2,
        timeout=3,
        run_id="run_1",
    )

    assert fetch_result is not None
    assert fetch_result.complete is True
    assert fetch_result.rows_fetched == 3
    assert fetched_urls == expected_urls


@pytest.mark.asyncio
async def test_fetch_resource_rows_fails_closed_on_cross_origin_next(monkeypatch):
    async def fake_fetch_source_json(_source, _request_url, *, timeout):
        assert timeout == 3
        return (
            200,
            _arkansas_practitioner_bundle(
                ["prac-1"],
                "https://untrusted.example/fhir/Practitioner?page=2",
            ),
            None,
            1,
        )

    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_source_json)

    fetch_result = await importer._fetch_resource_rows(
        {"source_id": "payer", "api_base": "https://payer.example/fhir"},
        "Practitioner",
        per_resource_limit=0,
        page_limit=0,
        page_count=25,
        timeout=3,
        run_id="run_1",
    )

    assert fetch_result is not None
    assert fetch_result.complete is False
    assert fetch_result.error == "untrusted_pagination_link"
    assert fetch_result.next_url_remaining is True


@pytest.mark.asyncio
async def test_fetch_resource_rows_rewrites_live_hap_page_two(monkeypatch):
    first_url = f"{importer.HAP_PROVIDER_DIRECTORY_BASE}/Practitioner?_count=1"
    second_url = (
        f"{importer.HAP_PROVIDER_DIRECTORY_BASE}?_count=1&_getpages=next-token"
    )
    fetched_urls: list[str] = []

    async def fake_fetch_source_json(_source, request_url, *, timeout):
        assert timeout == 3
        fetched_urls.append(request_url)
        if request_url == first_url:
            return (
                200,
                _arkansas_practitioner_bundle(
                    ["prac-1"],
                    "https://fhir-prov-dir-r4.api.hap.org:443?"
                    "_count=1&_getpages=next-token",
                ),
                None,
                1,
            )
        assert request_url == second_url
        return 200, _arkansas_practitioner_bundle(["prac-2"]), None, 1

    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_source_json)

    fetch_result = await importer._fetch_resource_rows(
        {"source_id": "hap", "api_base": importer.HAP_PROVIDER_DIRECTORY_BASE},
        "Practitioner",
        per_resource_limit=2,
        page_limit=0,
        page_count=1,
        timeout=3,
        run_id="run_1",
    )

    assert fetch_result is not None
    assert fetch_result.complete is True
    assert fetch_result.rows_fetched == 2
    assert fetched_urls == [first_url, second_url]


@pytest.mark.asyncio
async def test_fetch_resource_rows_resolves_relative_next_links(monkeypatch):
    """Verify this provider-directory regression contract."""
    calls: list[str] = []

    async def fake_fetch_json(url, *, timeout):
        calls.append(url)
        if len(calls) == 1:
            return (
                200,
                {
                    "resourceType": "Bundle",
                    "entry": [
                        {
                            "fullUrl": "https://example.test/fhir/Practitioner/prac-1",
                            "resource": {
                                "resourceType": "Practitioner",
                                "id": "prac-1",
                                "name": [{"family": "Old"}],
                            },
                        }
                    ],
                    "link": [{"relation": "next", "url": "/page/2"}],
                },
                None,
                10,
            )
        return (
            200,
            {
                "resourceType": "Bundle",
                "entry": [
                    {
                        "fullUrl": "https://example.test/fhir/Practitioner/prac-2",
                        "resource": {
                            "resourceType": "Practitioner",
                            "id": "prac-2",
                            "name": [{"family": "New"}],
                        },
                    }
                ],
            },
            None,
            10,
        )

    monkeypatch.setattr(importer, "_fetch_json", fake_fetch_json)

    operation_result = await importer._fetch_resource_rows(
        {"source_id": "source_a", "api_base": "https://example.test/fhir"},
        "Practitioner",
        per_resource_limit=2,
        page_limit=2,
        page_count=10,
        timeout=3,
        run_id="run_1",
    )

    assert operation_result is not None
    assert operation_result.model is ProviderDirectoryPractitioner
    assert operation_result.complete is True
    assert [observed_row_map["resource_id"] for observed_row_map in operation_result.rows] == ["prac-1", "prac-2"]
    assert calls == [
        "https://example.test/fhir/Practitioner?_count=10",
        "https://example.test/page/2",
    ]


@pytest.mark.asyncio
async def test_fetch_resource_rows_uses_specific_resource_endpoint(monkeypatch):
    calls: list[str] = []

    async def fake_fetch_json(url, *, timeout):
        calls.append(url)
        return (
            200,
            {
                "resourceType": "Bundle",
                "entry": [
                    {
                        "fullUrl": "https://payer.example/custom/Practitioner/prac-1",
                        "resource": {
                            "resourceType": "Practitioner",
                            "id": "prac-1",
                            "name": [{"family": "Endpoint"}],
                        },
                    }
                ],
            },
            None,
            10,
        )

    monkeypatch.setattr(importer, "_fetch_json", fake_fetch_json)

    operation_result = await importer._fetch_resource_rows(
        {
            "source_id": "source_a",
            "api_base": "https://example.test/fhir",
            "endpoint_practitioner": "https://payer.example/custom/Practitioner?active=true",
        },
        "Practitioner",
        per_resource_limit=1,
        page_limit=1,
        page_count=25,
        timeout=3,
        run_id="run_1",
    )

    assert operation_result is not None
    assert operation_result.complete is True
    assert [observed_row_map["resource_id"] for observed_row_map in operation_result.rows] == ["prac-1"]
    assert calls == ["https://payer.example/custom/Practitioner?active=true&_count=25"]


@pytest.mark.asyncio
async def test_fetch_resource_rows_zero_limits_follow_until_terminal_page(monkeypatch):
    calls: list[str] = []

    async def fake_fetch_json(url, *, timeout):
        calls.append(url)
        page_no = len(calls)
        payload = {
            "resourceType": "Bundle",
            "entry": [
                {
                    "resource": {
                        "resourceType": "Location",
                        "id": f"loc-{page_no}",
                        "address": {"line": [f"{page_no} Main"], "city": "Chicago", "state": "IL", "postalCode": "60601"},
                    }
                }
            ],
        }
        if page_no < 3:
            payload["link"] = [{"relation": "next", "url": f"/fhir/Location?page={page_no + 1}"}]
        return 200, payload, None, 10

    monkeypatch.setattr(importer, "_fetch_json", fake_fetch_json)

    operation_result = await importer._fetch_resource_rows(
        {"source_id": "source_a", "api_base": "https://example.test/fhir"},
        "Location",
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_1",
    )

    assert operation_result is not None
    assert operation_result.complete is True
    assert operation_result.pages_fetched == 3
    assert [observed_row_map["resource_id"] for observed_row_map in operation_result.rows] == ["loc-1", "loc-2", "loc-3"]
    assert calls == [
        "https://example.test/fhir/Location?_count=100",
        "https://example.test/fhir/Location?page=2",
        "https://example.test/fhir/Location?page=3",
    ]


@pytest.mark.asyncio
async def test_fetch_resource_rows_streams_batches_without_retaining_rows(monkeypatch):
    async def fake_fetch_json(url, *, timeout):
        page = "2" if "page=2" in url else "1"
        payload = {
            "resourceType": "Bundle",
            "entry": [
                {
                    "resource": {
                        "resourceType": "Practitioner",
                        "id": f"prac-{page}-a",
                        "name": [{"family": "Alpha"}],
                    }
                },
                {
                    "resource": {
                        "resourceType": "Practitioner",
                        "id": f"prac-{page}-b",
                        "name": [{"family": "Beta"}],
                    }
                },
            ],
        }
        if page == "1":
            payload["link"] = [{"relation": "next", "url": "/fhir/Practitioner?page=2"}]
        return 200, payload, None, 10

    batches: list[list[str]] = []

    async def row_batch_handler(_model, rows):
        batches.append([row["resource_id"] for row in rows])
        return len(rows)

    monkeypatch.setattr(importer, "_fetch_json", fake_fetch_json)

    operation_result = await importer._fetch_resource_rows(
        {"source_id": "source_a", "api_base": "https://example.test/fhir"},
        "Practitioner",
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_1",
        row_batch_handler=row_batch_handler,
        row_batch_size=3,
        retain_rows=False,
    )

    assert operation_result is not None
    assert operation_result.complete is True
    assert operation_result.rows == []
    assert operation_result.rows_fetched == 4
    assert operation_result.rows_written == 4
    assert batches == [["prac-1-a", "prac-1-b", "prac-2-a"], ["prac-2-b"]]
    resource_fetch_stats_by_type: dict[str, dict[str, Any]] = {}
    importer._record_resource_fetch_stats(resource_fetch_stats_by_type, "Practitioner", operation_result)
    assert resource_fetch_stats_by_type["Practitioner"]["sources_empty"] == 0


def _cigna_checkpoint_source():
    return {
        "source_id": "source_a",
        "api_base": importer.CIGNA_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.CIGNA_PROVIDER_DIRECTORY_BASE,
    }


def _molina_checkpoint_source():
    return {
        "source_id": "molina",
        "api_base": importer.MOLINA_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.MOLINA_PROVIDER_DIRECTORY_BASE,
    }


def _molina_checkpoint_context():
    return importer.PaginationCheckpointContext(
        canonical_api_base=importer.MOLINA_PROVIDER_DIRECTORY_BASE,
        source_scope_hash="molina_scope",
        source_ids=("molina",),
        owner_run_id="run_retry",
        retry_of_run_id="run_original",
        acquisition_root_run_id="run_original",
        dataset_id="dataset_molina",
        lineage_verified=True,
    )


def _cigna_checkpoint_context(
    owner_run_id: str,
    retry_of_run_id: str | None = None,
    *,
    root_run_id: str | None = None,
    is_lineage_verified: bool | None = None,
    dataset_id: str | None = None,
):
    if root_run_id is None:
        root_run_id = retry_of_run_id or owner_run_id
    if is_lineage_verified is None:
        is_lineage_verified = retry_of_run_id is not None
    return importer.PaginationCheckpointContext(
        canonical_api_base=importer.CIGNA_PROVIDER_DIRECTORY_BASE,
        source_scope_hash="scope_a",
        source_ids=("source_a",),
        owner_run_id=owner_run_id,
        retry_of_run_id=retry_of_run_id,
        acquisition_root_run_id=root_run_id,
        dataset_id=dataset_id,
        lineage_verified=is_lineage_verified,
    )


def _humana_checkpoint_values():
    source_lookup = {
        "source_id": "humana_source",
        "api_base": importer.HUMANA_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.HUMANA_PROVIDER_DIRECTORY_BASE,
    }
    checkpoint_context = importer.PaginationCheckpointContext(
        canonical_api_base=importer.HUMANA_PROVIDER_DIRECTORY_BASE,
        source_scope_hash="humana_scope",
        source_ids=("humana_source",),
        owner_run_id="run_retry",
        retry_of_run_id="run_original",
        acquisition_root_run_id="run_original",
        dataset_id="dataset_humana",
        lineage_verified=True,
    )
    return source_lookup, checkpoint_context


def _practitioner_search_bundle(resource_id: str, next_url: str | None = None):
    payload = {
        "resourceType": "Bundle",
        "entry": [
            {
                "resource": {
                    "resourceType": "Practitioner",
                    "id": resource_id,
                    "name": [{"family": "Checkpoint"}],
                }
            }
        ],
    }
    if next_url:
        payload["link"] = [{"relation": "next", "url": next_url}]
    return payload


def _caresource_checkpoint_context():
    return importer.PaginationCheckpointContext(
        canonical_api_base=importer.CARESOURCE_PROVIDER_DIRECTORY_BASE,
        source_scope_hash="caresource_scope",
        source_ids=("caresource_source",),
        owner_run_id="run_retry",
        retry_of_run_id="run_original",
        acquisition_root_run_id="run_original",
        dataset_id="dataset_caresource",
        lineage_verified=True,
    )


def _caresource_role_bundle(resource_id: str, next_url: str | None = None):
    payload = {
        "resourceType": "Bundle",
        "type": "searchset",
        "entry": [
            {
                "resource": {
                    "resourceType": "PractitionerRole",
                    "id": resource_id,
                }
            }
        ],
    }
    if next_url:
        payload["link"] = [{"relation": "next", "url": next_url}]
    return payload


def _caresource_count_bundle(total: int):
    return {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": total,
    }


def _checkpoint_page_callbacks(
    start_url: str,
    next_url: str,
    calls: list[str],
):
    async def load_checkpoint(_context, resource_type, request_url):
        assert resource_type == "Practitioner"
        assert request_url == start_url
        return importer.PaginationResumeState(
            next_url=start_url,
            pages_processed=0,
            rows_processed=0,
            recent_url_hashes=(),
        )

    async def fetch_source_json(_source, request_url, *, timeout):
        calls.append(request_url)
        return (
            200,
            _practitioner_search_bundle(
                f"prac-{len(calls)}",
                next_url if len(calls) == 1 else None,
            ),
            None,
            5,
        )

    return load_checkpoint, fetch_source_json


def _expired_resume_callbacks(
    resume_url: str,
    request_urls: list[str],
    saved_next_urls: list[str | None],
):
    async def load_checkpoint(_context, _resource_type, _start_url):
        return importer.PaginationResumeState(
            next_url=resume_url,
            pages_processed=5,
            rows_processed=500,
            recent_url_hashes=(),
            resumed=True,
        )

    async def fetch_source_json(_source, request_url, *, timeout):
        request_urls.append(request_url)
        return 403, {"resourceType": "OperationOutcome"}, None, 5

    async def save_checkpoint(_context, _resource_type, **checkpoint_values_by_name):
        saved_next_urls.append(checkpoint_values_by_name["next_url"])

    return load_checkpoint, fetch_source_json, save_checkpoint


def _stub_checkpoint_restart_connection(monkeypatch, *status_results):
    connection = AsyncMock()
    connection.status = AsyncMock(side_effect=status_results)

    async def mutable_dataset_parents(_sql, **params):
        return [
            {
                "dataset_id": dataset_id,
                "status": importer.ENDPOINT_DATASET_ACQUIRING,
            }
            for dataset_id in params["dataset_ids"]
        ]

    connection.all = AsyncMock(side_effect=mutable_dataset_parents)

    @contextlib.asynccontextmanager
    async def acquire_connection():
        yield connection

    monkeypatch.setattr(importer.db, "acquire", acquire_connection)
    return connection


def _checkpoint_complete_fetch_result(model, rows_written: int):
    return importer.ResourceFetchResult(
        model=model,
        rows=[],
        rows_fetched=rows_written,
        rows_written=rows_written,
        pages_fetched=1,
        complete=True,
        row_limit_reached=False,
        page_limit_reached=False,
        hard_page_limit_reached=False,
        next_url_remaining=False,
    )


def test_caresource_census_url_removes_cursor_and_page_controls():
    census_url = importer._caresource_census_url(
        f"{importer.CARESOURCE_PROVIDER_DIRECTORY_BASE}/PractitionerRole?"
        "_count=1000&_searchId=opaque&_page=17&active=true"
    )

    assert urllib.parse.parse_qs(urllib.parse.urlsplit(census_url).query) == {
        "_summary": ["count"],
        "active": ["true"],
    }


@pytest.mark.parametrize(
    ("proof_overrides", "expected_failure"),
    (
        ({"post_count": 3}, "census_drift"),
        ({"processed_rows": 1, "unique_candidate_rows": 1}, "cursor_loss"),
        ({"processed_rows": 3, "unique_candidate_rows": 2}, "duplicate_resource_ids"),
        ({"processed_rows": 3, "unique_candidate_rows": 3}, "processed_count_mismatch"),
    ),
)
def test_caresource_completeness_proof_fails_closed(
    proof_overrides,
    expected_failure,
):
    proof_by_field = {
        "pre_count": 2,
        "post_count": 2,
        "processed_rows": 2,
        "unique_candidate_rows": 2,
        **proof_overrides,
    }

    assert importer._caresource_proof_failure(proof_by_field) == expected_failure


def _install_caresource_scan_callbacks(
    monkeypatch,
    start_url: str,
    next_url: str,
):
    requested_urls: list[str] = []
    saved_checkpoints: list[dict[str, Any]] = []
    saved_proofs: list[dict[str, Any]] = []

    async def load_checkpoint(_context, resource_type, request_url):
        assert resource_type == "PractitionerRole"
        assert request_url == start_url
        return importer.PaginationResumeState(start_url, 0, 0, ())

    async def fetch_source_json(_source, request_url, *, timeout):
        assert timeout == 3
        requested_urls.append(request_url)
        if "_summary=count" in request_url:
            return 200, _caresource_count_bundle(2), None, 1
        if request_url == start_url:
            return 200, _caresource_role_bundle("role-1", next_url), None, 1
        assert request_url == next_url
        return 200, _caresource_role_bundle("role-2"), None, 1

    async def save_checkpoint(_context, _resource_type, **checkpoint):
        saved_checkpoints.append(checkpoint)

    async def save_proof(_context, _resource_type, proof):
        saved_proofs.append(dict(proof))

    async def row_batch_handler(_model, rows):
        return len(rows)

    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_MAX_FULL_PAGES", "1")
    monkeypatch.setattr(
        importer,
        "_load_or_initialize_pagination_checkpoint",
        load_checkpoint,
    )
    monkeypatch.setattr(importer, "_fetch_source_json", fetch_source_json)
    monkeypatch.setattr(importer, "_save_pagination_checkpoint", save_checkpoint)
    monkeypatch.setattr(
        importer,
        "_save_pagination_checkpoint_completeness",
        save_proof,
    )
    monkeypatch.setattr(
        importer,
        "_caresource_unique_candidate_count",
        AsyncMock(return_value=2),
    )
    return requested_urls, saved_checkpoints, saved_proofs, row_batch_handler


def _assert_caresource_completion_metrics(fetch_result):
    resource_stats_by_type: dict[str, dict[str, Any]] = {}
    importer._record_resource_fetch_stats(
        resource_stats_by_type,
        "PractitionerRole",
        fetch_result,
    )
    role_stats_by_name = resource_stats_by_type["PractitionerRole"]
    assert role_stats_by_name["caresource_opaque_cursor_sources"] == 1
    assert role_stats_by_name["caresource_opaque_cursor_verified_sources"] == 1
    assert role_stats_by_name["caresource_opaque_cursor_pre_count"] == 2
    assert role_stats_by_name["caresource_opaque_cursor_processed_rows"] == 2
    assert role_stats_by_name["caresource_opaque_cursor_unique_candidate_rows"] == 2
    assert role_stats_by_name["caresource_opaque_cursor_post_count"] == 2


def _assert_caresource_checkpoint_proofs(saved_proofs):
    assert saved_proofs == [
        {
            "strategy_version": importer.CARESOURCE_OPAQUE_CURSOR_STRATEGY_VERSION,
            "verified": False,
            "pre_count": 2,
        },
        {
            "strategy_version": importer.CARESOURCE_OPAQUE_CURSOR_STRATEGY_VERSION,
            "verified": True,
            "pre_count": 2,
            "post_count": 2,
            "processed_rows": 2,
            "unique_candidate_rows": 2,
        },
    ]


@pytest.mark.asyncio
async def test_caresource_opaque_cursor_scan_reconciles_and_checkpoints_terminal(
    monkeypatch,
):
    """Complete only after opaque pagination and exact pre/post staging proof."""
    source_lookup = {
        "source_id": "caresource_source",
        "api_base": importer.CARESOURCE_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.CARESOURCE_PROVIDER_DIRECTORY_BASE,
    }
    start_url = (
        f"{importer.CARESOURCE_PROVIDER_DIRECTORY_BASE}/PractitionerRole?_count=1000"
    )
    next_url = (
        f"{importer.CARESOURCE_PROVIDER_DIRECTORY_BASE}/PractitionerRole?"
        "_count=1000&_searchId=opaque-cursor&_page=2"
    )
    requested_urls, saved_checkpoints, saved_proofs, row_batch_handler = (
        _install_caresource_scan_callbacks(monkeypatch, start_url, next_url)
    )

    fetch_result = await importer._fetch_resource_rows(
        source_lookup,
        "PractitionerRole",
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_retry",
        row_batch_handler=row_batch_handler,
        row_batch_size=1000,
        retain_rows=False,
        pagination_checkpoint=_caresource_checkpoint_context(),
    )

    assert fetch_result is not None
    assert fetch_result.complete is True
    assert fetch_result.fetch_mode == importer.CARESOURCE_OPAQUE_CURSOR_FETCH_MODE
    assert fetch_result.rows_fetched == 2
    assert fetch_result.rows_written == 2
    assert fetch_result.pages_fetched == 2
    assert requested_urls == [
        importer._caresource_census_url(start_url),
        start_url,
        next_url,
        importer._caresource_census_url(start_url),
    ]
    assert saved_checkpoints[-1]["next_url"] is None
    assert saved_checkpoints[-1]["rows_processed"] == 2
    _assert_caresource_checkpoint_proofs(saved_proofs)
    _assert_caresource_completion_metrics(fetch_result)


def _caresource_expired_census_proofs():
    old_proof_by_field = {
        "strategy_version": importer.CARESOURCE_OPAQUE_CURSOR_STRATEGY_VERSION,
        "verified": False,
        "pre_count": 2,
    }
    fresh_proof_by_field = {
        "strategy_version": importer.CARESOURCE_OPAQUE_CURSOR_STRATEGY_VERSION,
        "verified": False,
        "pre_count": 0,
    }
    verified_proof_by_field = {
        **fresh_proof_by_field,
        "post_count": 0,
        "processed_rows": 0,
        "unique_candidate_rows": 0,
        "verified": True,
    }
    return old_proof_by_field, fresh_proof_by_field, verified_proof_by_field


def _patch_caresource_expired_cursor_callbacks(monkeypatch, start_url, resume_url):
    old_proof_by_field, fresh_proof_by_field, verified_proof_by_field = (
        _caresource_expired_census_proofs()
    )
    prepare_census = AsyncMock(
        side_effect=[old_proof_by_field, fresh_proof_by_field]
    )
    reset_checkpoint = AsyncMock()
    finish_census = AsyncMock(
        return_value=importer.CareSourceOpaqueCursorOutcome(
            proof=verified_proof_by_field,
        )
    )

    async def load_checkpoint(_context, _resource_type, _start_url):
        return importer.PaginationResumeState(
            next_url=resume_url,
            pages_processed=2,
            rows_processed=2,
            recent_url_hashes=("old-cursor",),
            resumed=True,
            completeness=old_proof_by_field,
        )

    async def fetch_source_json(_source, request_url, *, timeout):
        assert timeout == 3
        if request_url == resume_url:
            return 410, {"resourceType": "OperationOutcome"}, None, 1
        assert request_url == start_url
        return 200, {"resourceType": "Bundle", "type": "searchset"}, None, 1

    callback_by_name = {
        "_load_or_initialize_pagination_checkpoint": load_checkpoint,
        "_prepare_caresource_pre_census": prepare_census,
        "_fetch_source_json": fetch_source_json,
        "_reset_pagination_checkpoint": reset_checkpoint,
        "_finish_caresource_opaque_cursor_census": finish_census,
        "_save_pagination_checkpoint": AsyncMock(),
    }
    for callback_name, callback in callback_by_name.items():
        monkeypatch.setattr(importer, callback_name, callback)
    return (
        prepare_census,
        reset_checkpoint,
        finish_census,
        fresh_proof_by_field,
        verified_proof_by_field,
    )


@pytest.mark.asyncio
async def test_caresource_expired_cursor_restarts_with_fresh_census(monkeypatch):
    """Restart an expired CareSource cursor against a new census proof."""
    start_url = (
        f"{importer.CARESOURCE_PROVIDER_DIRECTORY_BASE}/"
        "PractitionerRole?_count=1000"
    )
    resume_url = f"{start_url}&_searchId=expired&_page=2"
    (
        prepare_census,
        reset_checkpoint,
        finish_census,
        fresh_proof_by_field,
        verified_proof_by_field,
    ) = _patch_caresource_expired_cursor_callbacks(
        monkeypatch,
        start_url,
        resume_url,
    )

    fetch_result = await importer._fetch_resource_rows(
        {
            "source_id": "caresource_source",
            "api_base": importer.CARESOURCE_PROVIDER_DIRECTORY_BASE,
            "canonical_api_base": importer.CARESOURCE_PROVIDER_DIRECTORY_BASE,
            "endpoint_practitioner_role": start_url,
        },
        "PractitionerRole",
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_retry",
        row_batch_handler=AsyncMock(return_value=0),
        row_batch_size=1000,
        retain_rows=False,
        pagination_checkpoint=_caresource_checkpoint_context(),
    )

    assert fetch_result is not None
    assert fetch_result.complete is True
    assert fetch_result.fetch_diagnostic == verified_proof_by_field
    reset_checkpoint.assert_awaited_once_with(
        _caresource_checkpoint_context(),
        "PractitionerRole",
        start_url,
        hashlib.sha256(start_url.encode("utf-8")).hexdigest(),
    )
    assert prepare_census.await_count == 2
    fresh_resume_state = prepare_census.await_args_list[1].args[4]
    assert fresh_resume_state.rows_processed == 0
    assert fresh_resume_state.completeness == {}
    finish_census.assert_awaited_once()
    assert finish_census.await_args.args[4] == fresh_proof_by_field


@pytest.mark.asyncio
async def test_caresource_retry_reuses_persisted_pre_census(monkeypatch):
    persisted_proof_by_field = {
        "strategy_version": importer.CARESOURCE_OPAQUE_CURSOR_STRATEGY_VERSION,
        "verified": False,
        "pre_count": 1828628,
    }
    fetch_census = AsyncMock()
    monkeypatch.setattr(importer, "_fetch_caresource_census_count", fetch_census)

    proof_by_field = await importer._prepare_caresource_pre_census(
        {
            "source_id": "caresource_source",
            "api_base": importer.CARESOURCE_PROVIDER_DIRECTORY_BASE,
        },
        "PractitionerRole",
        ProviderDirectoryPractitionerRole,
        _caresource_checkpoint_context(),
        importer.PaginationResumeState(
            next_url="https://example.test/opaque",
            pages_processed=1000,
            rows_processed=1000000,
            recent_url_hashes=(),
            resumed=True,
            completeness=persisted_proof_by_field,
        ),
        f"{importer.CARESOURCE_PROVIDER_DIRECTORY_BASE}/PractitionerRole?_count=1000",
        timeout=3,
    )

    assert proof_by_field == persisted_proof_by_field
    fetch_census.assert_not_awaited()


def test_caresource_pre_census_round_trips_from_checkpoint_row():
    context = _caresource_checkpoint_context()
    start_url_hash = "start-hash"
    proof_by_field = {
        "strategy_version": importer.CARESOURCE_OPAQUE_CURSOR_STRATEGY_VERSION,
        "verified": False,
        "pre_count": 1828628,
    }
    checkpoint_by_field = {
        "dataset_id": context.dataset_id,
        "source_ids": list(context.source_ids),
        "acquisition_root_run_id": context.acquisition_root_run_id,
        "owner_run_id": context.owner_run_id,
        "start_url_hash": start_url_hash,
        "next_url": "https://example.test/opaque",
        "state": importer.PAGINATION_CHECKPOINT_ACTIVE,
        "pages_processed": 1000,
        "rows_processed": 1000000,
        "recent_cursor_hashes": ["cursor-hash"],
        "completeness_json": proof_by_field,
    }

    resume_state = importer._compatible_pagination_resume_state(
        checkpoint_by_field,
        context,
        start_url_hash,
    )

    assert resume_state is not None
    assert resume_state.completeness == proof_by_field
    assert resume_state.rows_processed == 1000000


def test_caresource_terminal_proof_failure_cannot_enter_resume_queue():
    diagnostic_by_field = {
        "fetch_mode": importer.CARESOURCE_OPAQUE_CURSOR_FETCH_MODE,
        "error": (
            f"{importer.CARESOURCE_OPAQUE_CURSOR_BLOCKED_ERROR}:cursor_loss"
        ),
    }

    with pytest.raises(RuntimeError, match="cursor_loss"):
        importer._handle_incomplete_source_pagination(
            {"_pagination_checkpoint_context": _caresource_checkpoint_context()},
            {"PractitionerRole": diagnostic_by_field},
            ["PractitionerRole"],
            set(),
            True,
        )


def test_pagination_checkpoint_mode_requires_acquisition_only():
    enabled_options_by_name = {
        "run_id": "run_1",
        "full_refresh": True,
        "resource_limit": 0,
        "page_limit": 0,
        "stream_batch_size": 1000,
        "stale_cleanup": False,
        "publish_artifacts": False,
    }

    assert (
        importer._is_pagination_checkpoint_mode_enabled(**enabled_options_by_name)
        is True
    )
    for option_name, disabled_value in (
        ("run_id", None),
        ("full_refresh", False),
        ("resource_limit", 1),
        ("page_limit", 1),
        ("stream_batch_size", 0),
        ("stale_cleanup", True),
        ("publish_artifacts", True),
    ):
        options_by_name = {**enabled_options_by_name, option_name: disabled_value}
        assert importer._is_pagination_checkpoint_mode_enabled(**options_by_name) is False


def test_pagination_guard_serializes_same_endpoint_across_roots():
    first_context = _cigna_checkpoint_context("run_first")
    second_context = _cigna_checkpoint_context("run_second")

    assert importer._pagination_checkpoint_guard_key(first_context) == (
        importer._pagination_checkpoint_guard_key(second_context)
    )


@pytest.mark.asyncio
async def test_pagination_guard_uses_dedicated_connection(monkeypatch):
    guard_connection = AsyncMock()
    open_connection = AsyncMock(return_value=guard_connection)
    monkeypatch.setattr(
        importer,
        "_open_pagination_checkpoint_guard_connection",
        open_connection,
    )

    async with importer._pagination_checkpoint_worker_guard(
        _cigna_checkpoint_context("run_1")
    ):
        assert guard_connection.fetchval.await_count == 1

    open_connection.assert_awaited_once_with()
    assert guard_connection.fetchval.await_count == 2
    assert "pg_advisory_lock" in guard_connection.fetchval.await_args_list[0].args[0]
    assert "pg_advisory_unlock" in guard_connection.fetchval.await_args_list[1].args[0]
    guard_connection.close.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_pagination_guard_terminates_connection_when_lock_wait_is_cancelled(
    monkeypatch,
):
    guard_connection = AsyncMock()
    guard_connection.fetchval.side_effect = asyncio.CancelledError
    guard_connection.terminate = Mock()
    monkeypatch.setattr(
        importer,
        "_open_pagination_checkpoint_guard_connection",
        AsyncMock(return_value=guard_connection),
    )

    with pytest.raises(asyncio.CancelledError):
        async with importer._pagination_checkpoint_worker_guard(
            _cigna_checkpoint_context("run_1")
        ):
            raise AssertionError("guard must not yield after cancellation")

    guard_connection.terminate.assert_called_once_with()
    guard_connection.close.assert_not_awaited()


@pytest.mark.asyncio
async def test_pagination_checkpoint_retry_adopts_direct_predecessor(monkeypatch):
    start_url = f"{importer.CIGNA_PROVIDER_DIRECTORY_BASE}/Practitioner?_count=100"
    start_url_hash = hashlib.sha256(start_url.encode("utf-8")).hexdigest()
    checkpoint_by_name = {
        "source_ids": ["source_a"],
        "acquisition_root_run_id": "run_original",
        "owner_run_id": "run_original",
        "start_url_hash": start_url_hash,
        "next_url": start_url + "&_getpages=resume",
        "state": importer.PAGINATION_CHECKPOINT_ACTIVE,
        "pages_processed": 5,
        "rows_processed": 500,
        "recent_cursor_hashes": ["prior_hash"],
    }
    status_mock = AsyncMock(return_value=1)
    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value=checkpoint_by_name))
    monkeypatch.setattr(importer.db, "status", status_mock)

    resume_state = await importer._load_or_initialize_pagination_checkpoint(
        _cigna_checkpoint_context("run_retry", "run_original"),
        "Practitioner",
        start_url,
    )

    assert resume_state.resumed is True
    assert resume_state.pages_processed == 5
    assert resume_state.rows_processed == 500
    assert "UPDATE" in status_mock.await_args.args[0]
    assert status_mock.await_args.kwargs["previous_owner_run_id"] == "run_original"


@pytest.mark.asyncio
async def test_completed_checkpoint_survives_page_size_change(monkeypatch):
    old_start_url = f"{importer.CIGNA_PROVIDER_DIRECTORY_BASE}/Practitioner?_count=75"
    new_start_url = f"{importer.CIGNA_PROVIDER_DIRECTORY_BASE}/Practitioner?_count=100"
    checkpoint_by_name = {
        "dataset_id": "dataset_a",
        "source_ids": ["source_a"],
        "acquisition_root_run_id": "run_original",
        "owner_run_id": "run_older_retry",
        "start_url_hash": hashlib.sha256(old_start_url.encode("utf-8")).hexdigest(),
        "next_url": None,
        "state": importer.PAGINATION_CHECKPOINT_COMPLETE,
        "pages_processed": 7,
        "rows_processed": 541,
        "recent_cursor_hashes": ["prior_hash"],
    }
    status_mock = AsyncMock(return_value=1)
    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value=checkpoint_by_name))
    monkeypatch.setattr(importer.db, "status", status_mock)

    resume_state = await importer._load_or_initialize_pagination_checkpoint(
        _cigna_checkpoint_context(
            "run_current_retry",
            "run_direct_parent",
            root_run_id="run_original",
            dataset_id="dataset_a",
        ),
        "Practitioner",
        new_start_url,
    )

    assert resume_state.complete is True
    assert resume_state.pages_processed == 7
    assert resume_state.rows_processed == 541
    assert status_mock.await_args.kwargs["previous_owner_run_id"] == "run_older_retry"


@pytest.mark.asyncio
async def test_empty_checkpoint_restarts_after_page_size_change(monkeypatch):
    old_start_url = f"{importer.CIGNA_PROVIDER_DIRECTORY_BASE}/Location?_count=75"
    new_start_url = f"{importer.CIGNA_PROVIDER_DIRECTORY_BASE}/Location?_count=100"
    checkpoint_by_name = {
        "dataset_id": "dataset_a",
        "source_ids": ["source_a"],
        "acquisition_root_run_id": "run_original",
        "owner_run_id": "run_older_retry",
        "start_url_hash": hashlib.sha256(old_start_url.encode("utf-8")).hexdigest(),
        "next_url": old_start_url,
        "state": importer.PAGINATION_CHECKPOINT_ACTIVE,
        "pages_processed": 0,
        "rows_processed": 0,
        "recent_cursor_hashes": [],
    }
    status_mock = AsyncMock(return_value=1)
    first_mock = AsyncMock(return_value=checkpoint_by_name)
    restart_connection = _stub_checkpoint_restart_connection(monkeypatch, 1, 0)
    monkeypatch.setattr(importer.db, "first", first_mock)
    monkeypatch.setattr(importer.db, "status", status_mock)

    resume_state = await importer._load_or_initialize_pagination_checkpoint(
        _cigna_checkpoint_context(
            "run_current_retry",
            "run_direct_parent",
            root_run_id="run_original",
            dataset_id="dataset_a",
        ),
        "Location",
        new_start_url,
    )

    assert resume_state.resumed is False
    assert resume_state.next_url == new_start_url
    restart_call = restart_connection.status.await_args_list[0]
    assert restart_call.kwargs["start_url_hash"] == hashlib.sha256(
        new_start_url.encode("utf-8")
    ).hexdigest()
    cleanup_call = restart_connection.status.await_args_list[1]
    assert "UPDATE" in restart_call.args[0]
    assert "DELETE FROM" in cleanup_call.args[0]
    status_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_empty_checkpoint_restart_rejects_concurrent_progress(monkeypatch):
    old_start_url = f"{importer.CIGNA_PROVIDER_DIRECTORY_BASE}/Location?_count=75"
    new_start_url = f"{importer.CIGNA_PROVIDER_DIRECTORY_BASE}/Location?_count=100"
    checkpoint_by_name = {
        "dataset_id": "dataset_a",
        "source_ids": ["source_a"],
        "acquisition_root_run_id": "run_original",
        "owner_run_id": "run_direct_parent",
        "start_url_hash": hashlib.sha256(old_start_url.encode("utf-8")).hexdigest(),
        "next_url": old_start_url,
        "state": importer.PAGINATION_CHECKPOINT_ACTIVE,
        "pages_processed": 0,
        "rows_processed": 0,
        "recent_cursor_hashes": [],
    }
    first_mock = AsyncMock(return_value=checkpoint_by_name)
    restart_connection = _stub_checkpoint_restart_connection(monkeypatch, 0)
    monkeypatch.setattr(importer.db, "first", first_mock)

    with pytest.raises(
        RuntimeError,
        match="provider_directory_pagination_checkpoint_ownership_lost",
    ):
        await importer._load_or_initialize_pagination_checkpoint(
            _cigna_checkpoint_context(
                "run_current_retry",
                "run_direct_parent",
                root_run_id="run_original",
                dataset_id="dataset_a",
            ),
            "Location",
            new_start_url,
        )

    restart_sql = restart_connection.status.await_args.args[0]
    assert "source_ids::jsonb = CAST(:source_ids AS jsonb)" in restart_sql
    assert "pages_processed = 0" in restart_sql
    assert "rows_processed = 0" in restart_sql
    assert "completeness_json = '{}'::jsonb" in restart_sql
    assert "owner_run_id = :observed_owner_run_id" in restart_sql
    assert restart_connection.status.await_count == 1


@pytest.mark.asyncio
async def test_checkpoint_adoption_rejects_concurrent_cursor_change(monkeypatch):
    start_url = f"{importer.CIGNA_PROVIDER_DIRECTORY_BASE}/Practitioner?_count=100"
    checkpoint_by_name = {
        "dataset_id": "dataset_a",
        "source_ids": ["source_a"],
        "acquisition_root_run_id": "run_original",
        "owner_run_id": "run_direct_parent",
        "start_url_hash": hashlib.sha256(start_url.encode("utf-8")).hexdigest(),
        "next_url": start_url + "&_getpages=resume",
        "state": importer.PAGINATION_CHECKPOINT_ACTIVE,
        "pages_processed": 5,
        "rows_processed": 500,
        "recent_cursor_hashes": ["prior_hash"],
    }
    status_mock = AsyncMock(return_value=0)
    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value=checkpoint_by_name))
    monkeypatch.setattr(importer.db, "status", status_mock)

    with pytest.raises(
        RuntimeError,
        match="provider_directory_pagination_checkpoint_ownership_lost",
    ):
        await importer._load_or_initialize_pagination_checkpoint(
            _cigna_checkpoint_context(
                "run_current_retry",
                "run_direct_parent",
                root_run_id="run_original",
                dataset_id="dataset_a",
            ),
            "Practitioner",
            start_url,
        )

    adoption_sql = status_mock.await_args.args[0]
    assert "source_ids::jsonb = CAST(:source_ids AS jsonb)" in adoption_sql
    assert "next_url IS NOT DISTINCT FROM :observed_next_url" in adoption_sql
    assert "pages_processed = :observed_pages_processed" in adoption_sql
    assert "rows_processed = :observed_rows_processed" in adoption_sql


@pytest.mark.asyncio
async def test_checkpoint_worker_guard_covers_group_writes(monkeypatch):
    _stub_resource_import_metadata(monkeypatch)
    source_record = _cigna_checkpoint_source()
    checkpoint_context = _cigna_checkpoint_context("run_1")
    events: list[str] = []

    @contextlib.asynccontextmanager
    async def fake_guard(context):
        assert context.owner_run_id == "run_1"
        events.append("guard_enter")
        yield
        events.append("guard_exit")

    async def fake_prepare(source_records, *_args, **_kwargs):
        prepared_source_lookup = dict(source_records[0])
        prepared_source_lookup["_pagination_checkpoint_context"] = checkpoint_context
        return [prepared_source_lookup], None

    async def fake_fetch(source_record, _resource_type, **kwargs):
        events.append("fetch")
        written = await kwargs["row_batch_handler"](
            ProviderDirectoryLocation,
            [{"source_id": source_record["source_id"], "resource_id": "loc-1"}],
        )
        return _checkpoint_complete_fetch_result(
            ProviderDirectoryLocation,
            written,
        )

    async def fake_upsert(_model, rows, **_kwargs):
        events.append("write")
        return len(rows)

    async def fake_finalize(*_args, **_kwargs):
        events.append("finalize")

    monkeypatch.setattr(importer, "_pagination_checkpoint_worker_guard", fake_guard)
    monkeypatch.setattr(importer, "_prepare_resource_import_source_group", fake_prepare)
    monkeypatch.setattr(importer, "_fetch_resource_rows", fake_fetch)
    monkeypatch.setattr(importer, "_upsert_resource_rows", fake_upsert)
    monkeypatch.setattr(importer, "_finalize_source_pagination_checkpoints", fake_finalize)

    counts = await importer._import_resources(
        [source_record], resources=["Location"], per_resource_limit=0,
        page_limit=0, page_count=100, timeout=3, run_id="run_1",
        stream_batch_size=1, is_pagination_checkpointing_enabled=True,
    )

    assert counts == {"Location": 1}
    assert events == ["guard_enter", "fetch", "write", "finalize", "guard_exit"]


def _checkpoint_validation_candidate(source_record, checkpoint_context):
    return importer.EndpointDatasetCandidate(
        endpoint_id="endpoint_1",
        dataset_id="dataset_1",
        acquisition_root_run_id="run_1",
        source_ids=(source_record["source_id"],),
        selected_resources=("Location",),
        import_run_id="run_1",
        previous_dataset_id=None,
        checkpoint_context=checkpoint_context,
    )


def _patch_candidate_checkpoint_flow(monkeypatch, checkpoint_context, endpoint_candidate):
    events: list[str] = []

    @contextlib.asynccontextmanager
    async def fake_guard(_context):
        events.append("guard_enter")
        yield
        events.append("guard_exit")

    async def fake_prepare(source_records, *_args, **_kwargs):
        prepared_source_lookup = dict(source_records[0])
        prepared_source_lookup["_pagination_checkpoint_context"] = (
            checkpoint_context
        )
        prepared_source_lookup["_endpoint_dataset_id"] = (
            endpoint_candidate.dataset_id
        )
        return [prepared_source_lookup], endpoint_candidate

    async def fake_fetch(source_lookup, _resource_type, **kwargs):
        events.append("fetch")
        written = await kwargs["row_batch_handler"](
            ProviderDirectoryLocation,
            [{"source_id": source_lookup["source_id"], "resource_id": "loc-1"}],
        )
        return _checkpoint_complete_fetch_result(
            ProviderDirectoryLocation,
            written,
        )

    async def fake_upsert(_model, rows, **_kwargs):
        events.append("write")
        return len(rows)

    async def fake_validate(_candidate, _diagnostics):
        events.append("validate")
        return {
            "dataset_id": endpoint_candidate.dataset_id,
            "status": importer.ENDPOINT_DATASET_VALIDATED,
            "validated": True,
            "published": False,
        }

    async def fake_clear(cleanup_candidate):
        assert cleanup_candidate == endpoint_candidate
        events.append("clear")

    callback_by_name = {
        "_pagination_checkpoint_worker_guard": fake_guard,
        "_prepare_resource_import_source_group": fake_prepare,
        "_fetch_resource_rows": fake_fetch,
        "_upsert_resource_rows": fake_upsert,
        "_finalize_endpoint_dataset_candidate": fake_validate,
        "_clear_finalized_dataset_checkpoints": fake_clear,
    }
    for callback_name, callback in callback_by_name.items():
        monkeypatch.setattr(importer, callback_name, callback)
    return events


@pytest.mark.asyncio
async def test_candidate_checkpoint_clears_after_validation_inside_guard(monkeypatch):
    """Clear candidate checkpoints only after validation and inside the guard."""
    _stub_resource_import_metadata(monkeypatch)
    source_record = _cigna_checkpoint_source()
    checkpoint_context = _cigna_checkpoint_context(
        "run_1",
        root_run_id="run_1",
        dataset_id="dataset_1",
    )
    endpoint_candidate = _checkpoint_validation_candidate(
        source_record,
        checkpoint_context,
    )
    events = _patch_candidate_checkpoint_flow(
        monkeypatch,
        checkpoint_context,
        endpoint_candidate,
    )

    counts = await importer._import_resources(
        [source_record],
        resources=["Location"],
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_1",
        stream_batch_size=1,
        is_pagination_checkpointing_enabled=True,
    )

    assert counts == {"Location": 1}
    assert events == [
        "guard_enter",
        "fetch",
        "write",
        "validate",
        "clear",
        "guard_exit",
    ]


@pytest.mark.asyncio
async def test_progressed_checkpoint_rejects_page_size_change(monkeypatch):
    old_start_url = f"{importer.CIGNA_PROVIDER_DIRECTORY_BASE}/Location?_count=75"
    new_start_url = f"{importer.CIGNA_PROVIDER_DIRECTORY_BASE}/Location?_count=100"
    checkpoint_by_name = {
        "dataset_id": "dataset_a",
        "source_ids": ["source_a"],
        "acquisition_root_run_id": "run_original",
        "owner_run_id": "run_direct_parent",
        "start_url_hash": hashlib.sha256(old_start_url.encode("utf-8")).hexdigest(),
        "next_url": old_start_url + "&_getpages=resume",
        "state": importer.PAGINATION_CHECKPOINT_ACTIVE,
        "pages_processed": 1,
        "rows_processed": 75,
        "recent_cursor_hashes": ["prior_hash"],
    }
    status_mock = AsyncMock(return_value=1)
    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value=checkpoint_by_name))
    monkeypatch.setattr(importer.db, "status", status_mock)

    with pytest.raises(
        RuntimeError,
        match="provider_directory_pagination_checkpoint_lineage_conflict",
    ):
        await importer._load_or_initialize_pagination_checkpoint(
            _cigna_checkpoint_context(
                "run_current_retry",
                "run_direct_parent",
                root_run_id="run_original",
                dataset_id="dataset_a",
            ),
            "Location",
            new_start_url,
        )

    status_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_older_checkpoint_owner_requires_verified_dataset_lineage(monkeypatch):
    start_url = f"{importer.CIGNA_PROVIDER_DIRECTORY_BASE}/Location?_count=100"
    checkpoint_by_name = {
        "dataset_id": "dataset_a",
        "source_ids": ["source_a"],
        "acquisition_root_run_id": "run_original",
        "owner_run_id": "run_older_retry",
        "start_url_hash": hashlib.sha256(start_url.encode("utf-8")).hexdigest(),
        "next_url": start_url,
        "state": importer.PAGINATION_CHECKPOINT_ACTIVE,
        "pages_processed": 0,
        "rows_processed": 0,
        "recent_cursor_hashes": [],
    }
    status_mock = AsyncMock(return_value=1)
    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value=checkpoint_by_name))
    monkeypatch.setattr(importer.db, "status", status_mock)

    with pytest.raises(
        RuntimeError,
        match="provider_directory_pagination_checkpoint_lineage_conflict",
    ):
        await importer._load_or_initialize_pagination_checkpoint(
            _cigna_checkpoint_context(
                "run_current_retry",
                "run_direct_parent",
                root_run_id="run_original",
                is_lineage_verified=False,
                dataset_id="dataset_a",
            ),
            "Location",
            start_url,
        )

    status_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_pagination_checkpoint_new_root_does_not_load_stale_owner(monkeypatch):
    start_url = f"{importer.CIGNA_PROVIDER_DIRECTORY_BASE}/Practitioner?_count=100"
    status_mock = AsyncMock(return_value=1)
    first_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(importer.db, "first", first_mock)
    monkeypatch.setattr(importer.db, "status", status_mock)

    resume_state = await importer._load_or_initialize_pagination_checkpoint(
        _cigna_checkpoint_context("run_new_schedule"),
        "Practitioner",
        start_url,
    )

    assert resume_state.resumed is False
    assert resume_state.next_url == start_url
    assert first_mock.await_args.kwargs["acquisition_root_run_id"] == (
        "run_new_schedule"
    )
    assert "acquisition_root_run_id = :acquisition_root_run_id" in (
        first_mock.await_args.args[0]
    )
    assert "acquisition_root_run_id" in status_mock.await_args.args[0]


def test_pagination_retry_context_requires_explicit_root():
    with pytest.raises(
        ValueError,
        match="provider_directory_pagination_retry_root_missing",
    ):
        importer._pagination_checkpoint_context(
            _cigna_checkpoint_source(),
            ["source_a"],
            run_id="run_retry",
            retry_of_run_id="run_original",
        )


@pytest.mark.asyncio
async def test_pagination_retry_requires_verified_lineage_before_reset(monkeypatch):
    start_url = f"{importer.CIGNA_PROVIDER_DIRECTORY_BASE}/Practitioner?_count=100"
    status_mock = AsyncMock(return_value=1)
    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value=None))
    monkeypatch.setattr(importer.db, "status", status_mock)

    with pytest.raises(
        RuntimeError,
        match="provider_directory_pagination_checkpoint_retry_unverified",
    ):
        await importer._load_or_initialize_pagination_checkpoint(
            _cigna_checkpoint_context(
                "run_retry",
                "run_original",
                is_lineage_verified=False,
            ),
            "Practitioner",
            start_url,
        )

    status_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_fetch_resource_rows_checkpoints_only_after_page_write(monkeypatch):
    start_url = f"{importer.CIGNA_PROVIDER_DIRECTORY_BASE}/Practitioner?_count=100"
    next_url = (
        f"{importer.CIGNA_PROVIDER_DIRECTORY_BASE}/Practitioner?"
        "_getpages=opaque&_count=100"
    )
    calls: list[str] = []
    events: list[tuple[str, Any]] = []
    load_checkpoint, fetch_source_json = _checkpoint_page_callbacks(
        start_url,
        next_url,
        calls,
    )

    async def row_batch_handler(_model, rows):
        events.append(("write", [row["resource_id"] for row in rows]))
        return len(rows)

    async def fake_save_checkpoint(_context, _resource_type, **checkpoint):
        events.append(("checkpoint", checkpoint["next_url"]))

    monkeypatch.setattr(
        importer,
        "_load_or_initialize_pagination_checkpoint",
        load_checkpoint,
    )
    monkeypatch.setattr(importer, "_fetch_source_json", fetch_source_json)
    monkeypatch.setattr(importer, "_save_pagination_checkpoint", fake_save_checkpoint)

    fetch_result = await importer._fetch_resource_rows(
        _cigna_checkpoint_source(),
        "Practitioner",
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_1",
        row_batch_handler=row_batch_handler,
        row_batch_size=1000,
        retain_rows=False,
        pagination_checkpoint=_cigna_checkpoint_context("run_1"),
    )

    assert fetch_result is not None
    assert fetch_result.complete is True
    assert fetch_result.fetch_mode == "checkpointed_paged"
    assert fetch_result.rows_fetched == 2
    assert fetch_result.rows_written == 2
    assert calls == [start_url, next_url]
    assert events == [
        ("write", ["prac-1"]),
        ("checkpoint", next_url),
        ("write", ["prac-2"]),
        ("checkpoint", None),
    ]


@pytest.mark.asyncio
async def test_fetch_resource_rows_resumes_exact_checkpoint_url(monkeypatch):
    resume_url = (
        f"{importer.CIGNA_PROVIDER_DIRECTORY_BASE}/Practitioner?"
        "_getpages=resume-token&_count=100"
    )
    saved_checkpoints: list[dict[str, Any]] = []

    async def fake_load_checkpoint(_context, _resource_type, _start_url):
        return importer.PaginationResumeState(
            next_url=resume_url,
            pages_processed=7,
            rows_processed=700,
            recent_url_hashes=("prior_hash",),
            resumed=True,
        )

    async def fake_fetch_source_json(_source, request_url, *, timeout):
        assert request_url == resume_url
        return 200, _practitioner_search_bundle("prac-701"), None, 5

    async def row_batch_handler(_model, rows):
        return len(rows)

    async def fake_save_checkpoint(_context, _resource_type, **checkpoint):
        saved_checkpoints.append(checkpoint)

    monkeypatch.setattr(
        importer,
        "_load_or_initialize_pagination_checkpoint",
        fake_load_checkpoint,
    )
    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_source_json)
    monkeypatch.setattr(importer, "_save_pagination_checkpoint", fake_save_checkpoint)

    fetch_result = await importer._fetch_resource_rows(
        _cigna_checkpoint_source(),
        "Practitioner",
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_retry",
        row_batch_handler=row_batch_handler,
        row_batch_size=1000,
        retain_rows=False,
        pagination_checkpoint=_cigna_checkpoint_context(
            "run_retry",
            "run_original",
        ),
    )

    assert fetch_result is not None
    assert fetch_result.complete is True
    assert fetch_result.pages_fetched == 8
    assert fetch_result.rows_fetched == 701
    assert saved_checkpoints[0]["next_url"] is None
    assert saved_checkpoints[0]["pages_processed"] == 8
    assert saved_checkpoints[0]["rows_processed"] == 701


def _non_bundle_checkpoint_callbacks(resume_url):
    saved_checkpoints: list[dict[str, Any]] = []

    async def load_checkpoint(_context, _resource_type, _start_url):
        return importer.PaginationResumeState(
            next_url=resume_url,
            pages_processed=3057,
            rows_processed=305700,
            recent_url_hashes=(),
            resumed=True,
        )

    async def fetch_source_json(_source, request_url, *, timeout):
        assert request_url == resume_url
        assert timeout == 3
        return (
            200,
            {
                "resourceType": "OperationOutcome",
                "issue": [{"diagnostics": "Temporary upstream response"}],
            },
            None,
            5,
        )

    async def save_checkpoint(_context, _resource_type, **checkpoint_values):
        saved_checkpoints.append(checkpoint_values)

    return saved_checkpoints, load_checkpoint, fetch_source_json, save_checkpoint


@pytest.mark.asyncio
async def test_checkpoint_non_bundle_preserves_cursor(monkeypatch):
    resume_url = (
        f"{importer.MOLINA_PROVIDER_DIRECTORY_BASE}/OrganizationAffiliation?"
        "_count=100&cursorMark=opaque"
    )
    (
        saved_checkpoints,
        load_checkpoint,
        fetch_source_json,
        save_checkpoint,
    ) = _non_bundle_checkpoint_callbacks(resume_url)

    reset_checkpoint = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_load_or_initialize_pagination_checkpoint",
        load_checkpoint,
    )
    monkeypatch.setattr(importer, "_fetch_source_json", fetch_source_json)
    monkeypatch.setattr(importer, "_save_pagination_checkpoint", save_checkpoint)
    monkeypatch.setattr(
        importer,
        "_reset_pagination_checkpoint",
        reset_checkpoint,
    )

    fetch_result = await importer._fetch_resource_rows(
        _molina_checkpoint_source(),
        "OrganizationAffiliation",
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_retry",
        row_batch_handler=AsyncMock(return_value=0),
        row_batch_size=1000,
        retain_rows=False,
        pagination_checkpoint=_molina_checkpoint_context(),
    )

    assert fetch_result is not None
    assert fetch_result.complete is False
    assert fetch_result.error == "non_bundle_payload"
    assert fetch_result.pages_fetched == 3057
    assert fetch_result.rows_fetched == 305700
    assert fetch_result.next_url_remaining is True
    assert fetch_result.retry_not_before is not None
    assert saved_checkpoints == []
    reset_checkpoint.assert_not_awaited()


def _cooldown_checkpoint_callbacks(resume_url, responses):
    requested_urls: list[str] = []
    saved_checkpoints: list[dict[str, Any]] = []

    async def load_checkpoint(_context, _resource_type, _start_url):
        return importer.PaginationResumeState(
            next_url=resume_url,
            pages_processed=7,
            rows_processed=700,
            recent_url_hashes=(),
            resumed=True,
        )

    async def fetch_source_json(_source, request_url, *, timeout):
        assert timeout == 3
        requested_urls.append(request_url)
        return responses.pop(0)

    async def save_checkpoint(_context, _resource_type, **checkpoint):
        saved_checkpoints.append(checkpoint)

    return (
        requested_urls,
        saved_checkpoints,
        load_checkpoint,
        fetch_source_json,
        save_checkpoint,
    )


async def _fetch_checkpointed_cigna_practitioners():
    return await importer._fetch_resource_rows(
        _cigna_checkpoint_source(),
        "Practitioner",
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_retry",
        row_batch_handler=AsyncMock(return_value=1),
        row_batch_size=1000,
        retain_rows=False,
        deadline_seconds=1000,
        pagination_checkpoint=_cigna_checkpoint_context(
            "run_retry",
            "run_original",
        ),
    )


def _assert_cooldown_recovery_observability(fetch_result):
    diagnostic = importer._resource_fetch_diagnostic(fetch_result, rows_written=1)
    assert diagnostic["pagination_cooldown_retries"] == 1
    assert diagnostic["pagination_cooldown_wait_seconds"] == 4.0
    assert diagnostic["pagination_cooldown_recovered"] is True
    assert "locked-token" not in json.dumps(diagnostic)

    stats_by_resource: dict[str, dict[str, Any]] = {}
    importer._record_resource_fetch_stats(
        stats_by_resource,
        "Practitioner",
        fetch_result,
    )
    assert stats_by_resource["Practitioner"]["pagination_cooldown_retries"] == 1
    assert stats_by_resource["Practitioner"]["pagination_cooldown_wait_seconds"] == 4.0
    assert (
        stats_by_resource["Practitioner"]["pagination_cooldown_recovered_sources"]
        == 1
    )


@pytest.mark.asyncio
async def test_checkpoint_page_423_cooldown_retries_exact_url_and_recovers(monkeypatch):
    """A locked checkpoint page retries in place without resetting progress."""
    resume_url = (
        f"{importer.CIGNA_PROVIDER_DIRECTORY_BASE}/Practitioner?"
        "_getpages=locked-token&_count=100"
    )
    responses = [
        (
            423,
            {importer.SOURCE_RETRY_AFTER_FIELD: "4", "diagnostics": "private"},
            None,
            5,
        ),
        (200, _practitioner_search_bundle("prac-701"), None, 5),
    ]
    request_urls, saved_checkpoints, load_checkpoint, fetch_source_json, save_checkpoint = (
        _cooldown_checkpoint_callbacks(resume_url, responses)
    )
    sleep_mock = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_load_or_initialize_pagination_checkpoint",
        load_checkpoint,
    )
    monkeypatch.setattr(importer, "_fetch_source_json", fetch_source_json)
    monkeypatch.setattr(importer, "_save_pagination_checkpoint", save_checkpoint)
    monkeypatch.setattr(importer.asyncio, "sleep", sleep_mock)

    fetch_result = await _fetch_checkpointed_cigna_practitioners()

    assert fetch_result is not None
    assert fetch_result.complete is True
    assert fetch_result.pages_fetched == 8
    assert fetch_result.rows_fetched == 701
    assert fetch_result.pagination_cooldown_retries == 1
    assert fetch_result.pagination_cooldown_wait_seconds == 4.0
    assert fetch_result.is_pagination_cooldown_recovered is True
    assert request_urls == [resume_url, resume_url]
    sleep_mock.assert_awaited_once_with(4.0)
    assert len(saved_checkpoints) == 1
    assert saved_checkpoints[0]["next_url"] is None
    assert saved_checkpoints[0]["pages_processed"] == 8
    assert saved_checkpoints[0]["rows_processed"] == 701
    _assert_cooldown_recovery_observability(fetch_result)


@pytest.mark.asyncio
async def test_checkpoint_transport_timeout_uses_300_second_cooldown(monkeypatch):
    checkpoint_url = "https://payer.example/fhir/Location?_getpages=timeout-token"
    fetch_source_json = AsyncMock(
        return_value=(200, {"resourceType": "Bundle", "entry": []}, None, 5)
    )
    sleep_mock = AsyncMock()
    monkeypatch.setattr(importer, "_fetch_source_json", fetch_source_json)
    monkeypatch.setattr(importer.asyncio, "sleep", sleep_mock)

    cooldown_result = await importer._retry_rest_pagination_after_cooldown(
        {"source_id": "source_a", "api_base": "https://payer.example/fhir"},
        checkpoint_url,
        (None, None, "TimeoutError: timed out", 9),
        timeout=3,
        deadline_at=None,
    )

    assert cooldown_result.fetch_result[0] == 200
    assert cooldown_result.retries == 1
    assert cooldown_result.wait_seconds == 300.0
    assert cooldown_result.recovered is True
    sleep_mock.assert_awaited_once_with(300.0)
    fetch_source_json.assert_awaited_once_with(
        {"source_id": "source_a", "api_base": "https://payer.example/fhir"},
        checkpoint_url,
        timeout=3,
    )


def test_checkpoint_cooldown_excludes_other_transient_failures():
    assert not importer._is_rest_pagination_cooldown_failure(503, None)
    assert not importer._is_rest_pagination_cooldown_failure(
        None,
        "ConnectionResetError: connection reset by peer",
    )


@pytest.mark.asyncio
async def test_checkpoint_cooldown_stops_before_resource_deadline(monkeypatch):
    fetch_source_json = AsyncMock()
    sleep_mock = AsyncMock()
    monkeypatch.setattr(importer, "_fetch_source_json", fetch_source_json)
    monkeypatch.setattr(importer.asyncio, "sleep", sleep_mock)

    cooldown_result = await importer._retry_rest_pagination_after_cooldown(
        {"source_id": "source_a", "api_base": "https://payer.example/fhir"},
        "https://payer.example/fhir/Location?_getpages=deadline-token",
        (423, {importer.SOURCE_RETRY_AFTER_FIELD: "300"}, None, 9),
        timeout=3,
        deadline_at=importer.time.monotonic() + 299,
    )

    assert cooldown_result.fetch_result[0] == 423
    assert cooldown_result.retries == 0
    assert cooldown_result.deadline_blocked is True
    sleep_mock.assert_not_awaited()
    fetch_source_json.assert_not_awaited()


@pytest.mark.asyncio
async def test_checkpoint_423_cooldown_exhaustion_preserves_resume_checkpoint(
    monkeypatch,
):
    """Exhausted cooldowns leave the durable resume checkpoint untouched."""
    resume_url = (
        f"{importer.CIGNA_PROVIDER_DIRECTORY_BASE}/Practitioner?"
        "_getpages=locked-token&_count=100"
    )
    locked_response = (423, {importer.SOURCE_RETRY_AFTER_FIELD: "1"}, None, 5)
    responses = [locked_response] * 3
    request_urls, saved_checkpoints, load_checkpoint, fetch_source_json, save_checkpoint = (
        _cooldown_checkpoint_callbacks(resume_url, responses)
    )
    clear_checkpoint = AsyncMock()
    sleep_mock = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_load_or_initialize_pagination_checkpoint",
        load_checkpoint,
    )
    monkeypatch.setattr(importer, "_fetch_source_json", fetch_source_json)
    monkeypatch.setattr(importer, "_save_pagination_checkpoint", save_checkpoint)
    monkeypatch.setattr(
        importer,
        "_clear_checkpoint_dataset_resource_type",
        clear_checkpoint,
    )
    monkeypatch.setattr(importer.asyncio, "sleep", sleep_mock)

    fetch_result = await _fetch_checkpointed_cigna_practitioners()

    assert fetch_result is not None
    assert fetch_result.complete is False
    assert fetch_result.error == "http_423"
    assert fetch_result.pages_fetched == 7
    assert fetch_result.rows_fetched == 700
    assert fetch_result.next_url_remaining is True
    assert fetch_result.pagination_cooldown_retries == 2
    assert fetch_result.pagination_cooldown_wait_seconds == 2.0
    assert fetch_result.is_pagination_cooldown_exhausted is True
    assert request_urls == [resume_url, resume_url, resume_url]
    assert [call.args[0] for call in sleep_mock.await_args_list] == [1.0, 1.0]
    assert saved_checkpoints == []
    clear_checkpoint.assert_not_awaited()


def _terminal_empty_offset_page_callbacks(api_base):
    terminal_url = (
        f"{api_base}/Organization?"
        "_sort=_id&_offset=2585000&_count=100"
    )
    invalid_next_url = (
        f"{api_base}/Organization?"
        "_sort=_id&_offset=2585001&_count=100"
    )
    requested_urls: list[str] = []
    saved_next_urls: list[str | None] = []

    async def fake_load_checkpoint(_context, _resource_type, _start_url):
        return importer.PaginationResumeState(
            next_url=terminal_url,
            pages_processed=25850,
            rows_processed=1000,
            recent_url_hashes=(),
            resumed=True,
        )

    async def fake_fetch_source_json(_source, request_url, *, timeout):
        requested_urls.append(request_url)
        return (
            200,
            {
                "resourceType": "Bundle",
                "total": 2585000,
                "entry": [],
                "link": [{"relation": "next", "url": invalid_next_url}],
            },
            None,
            5,
        )

    async def fake_save_checkpoint(_context, _resource_type, **checkpoint):
        saved_next_urls.append(checkpoint["next_url"])

    return terminal_url, requested_urls, saved_next_urls, fake_load_checkpoint, fake_fetch_source_json, fake_save_checkpoint


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "api_base",
    (
        importer.TMHP_PROVIDER_DIRECTORY_BASE,
        importer.NEBRASKA_DHHS_PROVIDER_DIRECTORY_BASE,
    ),
)
async def test_offset_pagination_stops_on_terminal_empty_bundle(monkeypatch, api_base):
    """Stop state-directory checkpoints at invalid empty terminal pages."""
    (
        terminal_url,
        requested_urls,
        saved_next_urls,
        fake_load_checkpoint,
        fake_fetch_source_json,
        fake_save_checkpoint,
    ) = _terminal_empty_offset_page_callbacks(api_base)
    monkeypatch.setattr(
        importer,
        "_load_or_initialize_pagination_checkpoint",
        fake_load_checkpoint,
    )
    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_source_json)
    monkeypatch.setattr(importer, "_save_pagination_checkpoint", fake_save_checkpoint)

    checkpoint_context = importer.PaginationCheckpointContext(
        canonical_api_base=api_base,
        source_scope_hash="state_scope",
        source_ids=("state_source",),
        owner_run_id="run_retry",
        retry_of_run_id="run_original",
        acquisition_root_run_id="run_original",
        dataset_id="dataset_state",
        lineage_verified=True,
    )

    fetch_result = await importer._fetch_resource_rows(
        {
            "source_id": "state_source",
            "api_base": api_base,
            "canonical_api_base": api_base,
        },
        "Organization",
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_retry",
        row_batch_handler=AsyncMock(return_value=0),
        row_batch_size=1000,
        retain_rows=False,
        pagination_checkpoint=checkpoint_context,
    )

    assert fetch_result is not None
    assert fetch_result.complete is True
    assert fetch_result.next_url_remaining is False
    assert requested_urls == [terminal_url]
    assert saved_next_urls == [None]


def _generic_expired_resume_test_callbacks(start_url, resume_url):
    requested_urls: list[str] = []
    saved_next_urls: list[str | None] = []
    reset_checkpoint = AsyncMock()

    async def load_checkpoint(_context, _resource_type, _start_url):
        return importer.PaginationResumeState(
            next_url=resume_url,
            pages_processed=5,
            rows_processed=500,
            recent_url_hashes=(),
            resumed=True,
        )

    async def fetch_source_json(_source, request_url, *, timeout):
        requested_urls.append(request_url)
        if request_url == resume_url:
            return 410, {"resourceType": "OperationOutcome"}, None, timeout
        return 200, {"resourceType": "Bundle", "entry": []}, None, timeout

    async def save_checkpoint(_context, _resource_type, **checkpoint):
        saved_next_urls.append(checkpoint["next_url"])

    async def row_batch_handler(_model, rows):
        return len(rows)

    return (
        requested_urls,
        saved_next_urls,
        reset_checkpoint,
        load_checkpoint,
        fetch_source_json,
        save_checkpoint,
        row_batch_handler,
    )


@pytest.mark.asyncio
async def test_fetch_resource_rows_restarts_expired_resume_once(monkeypatch):
    """Restart one expired generic cursor and persist the replacement state."""
    start_url = f"{importer.CIGNA_PROVIDER_DIRECTORY_BASE}/Practitioner?_count=100"
    resume_url = (
        f"{importer.CIGNA_PROVIDER_DIRECTORY_BASE}/Practitioner?"
        "_getpages=expired&_count=100"
    )
    (
        requested_urls,
        saved_next_urls,
        reset_checkpoint,
        load_checkpoint,
        fetch_source_json,
        save_checkpoint,
        row_batch_handler,
    ) = _generic_expired_resume_test_callbacks(start_url, resume_url)

    monkeypatch.setattr(
        importer,
        "_load_or_initialize_pagination_checkpoint",
        load_checkpoint,
    )
    monkeypatch.setattr(importer, "_fetch_source_json", fetch_source_json)
    monkeypatch.setattr(
        importer,
        "_reset_pagination_checkpoint",
        reset_checkpoint,
    )
    monkeypatch.setattr(importer, "_save_pagination_checkpoint", save_checkpoint)

    fetch_result = await importer._fetch_resource_rows(
        _cigna_checkpoint_source(),
        "Practitioner",
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_retry",
        row_batch_handler=row_batch_handler,
        row_batch_size=1000,
        retain_rows=False,
        pagination_checkpoint=_cigna_checkpoint_context(
            "run_retry",
            "run_original",
        ),
    )

    assert fetch_result is not None
    assert fetch_result.complete is True
    assert requested_urls == [resume_url, start_url]
    assert saved_next_urls == [None]
    reset_checkpoint.assert_awaited_once_with(
        _cigna_checkpoint_context("run_retry", "run_original"),
        "Practitioner",
        start_url,
        hashlib.sha256(start_url.encode("utf-8")).hexdigest(),
    )


@pytest.mark.parametrize(
    ("status_code", "request_url"),
    (
        (400, f"{importer.CIGNA_PROVIDER_DIRECTORY_BASE}/Practitioner?_getpages=expired"),
        (404, f"{importer.WASHINGTON_PROVIDER_DIRECTORY_BASE}/Practitioner?_getpages=expired"),
        (410, f"{importer.CIGNA_PROVIDER_DIRECTORY_BASE}/Practitioner?_page_token=expired"),
    ),
)
def test_expired_pagination_statuses_restart_resumed_checkpoint(
    status_code,
    request_url,
):
    resume_state = importer.PaginationResumeState(
        next_url=request_url,
        pages_processed=1,
        rows_processed=100,
        recent_url_hashes=(),
        resumed=True,
    )

    assert importer._should_restart_expired_pagination_checkpoint(
        status_code=status_code,
        fetch_error=None,
        request_url=request_url,
        resume_state=resume_state,
        has_restart_attempted=False,
    )


@pytest.mark.parametrize(
    "request_url",
    (
        f"{importer.CIGNA_PROVIDER_DIRECTORY_BASE}/Practitioner?_continuationToken=expired",
        f"{importer.HUMANA_PROVIDER_DIRECTORY_BASE}/Practitioner?_getpages=expired",
    ),
)
def test_http_403_restart_requires_humana_continuation_token(request_url):
    resume_state = importer.PaginationResumeState(
        next_url=request_url,
        pages_processed=1,
        rows_processed=100,
        recent_url_hashes=(),
        resumed=True,
    )

    assert not importer._should_restart_expired_pagination_checkpoint(
        status_code=403,
        fetch_error=None,
        request_url=request_url,
        resume_state=resume_state,
        has_restart_attempted=False,
    )


@pytest.mark.asyncio
async def test_humana_403_continuation_restart_stops_after_one_reset(monkeypatch):
    start_url = f"{importer.HUMANA_PROVIDER_DIRECTORY_BASE}/Practitioner?_count=100"
    resume_url = f"{start_url}&_continuationToken=expired"
    source_record, checkpoint_context = _humana_checkpoint_values()
    request_urls: list[str] = []
    saved_next_urls: list[str | None] = []
    load_checkpoint, fetch_source_json, save_checkpoint = _expired_resume_callbacks(
        resume_url,
        request_urls,
        saved_next_urls,
    )
    reset_checkpoint = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_load_or_initialize_pagination_checkpoint",
        load_checkpoint,
    )
    monkeypatch.setattr(importer, "_fetch_source_json", fetch_source_json)
    monkeypatch.setattr(importer, "_save_pagination_checkpoint", save_checkpoint)
    monkeypatch.setattr(
        importer,
        "_reset_pagination_checkpoint",
        reset_checkpoint,
    )

    fetch_result = await importer._fetch_resource_rows(
        source_record,
        "Practitioner",
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_retry",
        row_batch_handler=AsyncMock(return_value=0),
        row_batch_size=1000,
        retain_rows=False,
        pagination_checkpoint=checkpoint_context,
    )

    assert fetch_result is not None
    assert fetch_result.complete is False
    assert fetch_result.error == "http_403"
    assert request_urls == [resume_url, start_url]
    assert saved_next_urls == []
    reset_checkpoint.assert_awaited_once_with(
        checkpoint_context,
        "Practitioner",
        start_url,
        hashlib.sha256(start_url.encode("utf-8")).hexdigest(),
    )


@pytest.mark.asyncio
async def test_generic_403_continuation_remains_fetch_error(monkeypatch):
    start_url = f"{importer.CIGNA_PROVIDER_DIRECTORY_BASE}/Practitioner?_count=100"
    resume_url = f"{start_url}&_continuationToken=blocked"
    request_urls: list[str] = []
    saved_next_urls: list[str | None] = []
    load_checkpoint, fetch_source_json, save_checkpoint = _expired_resume_callbacks(
        resume_url,
        request_urls,
        saved_next_urls,
    )
    clear_checkpoint = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_load_or_initialize_pagination_checkpoint",
        load_checkpoint,
    )
    monkeypatch.setattr(importer, "_fetch_source_json", fetch_source_json)
    monkeypatch.setattr(importer, "_save_pagination_checkpoint", save_checkpoint)
    monkeypatch.setattr(
        importer,
        "_clear_checkpoint_dataset_resource_type",
        clear_checkpoint,
    )

    fetch_result = await importer._fetch_resource_rows(
        _cigna_checkpoint_source(),
        "Practitioner",
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_retry",
        row_batch_handler=AsyncMock(return_value=0),
        row_batch_size=1000,
        retain_rows=False,
        pagination_checkpoint=_cigna_checkpoint_context(
            "run_retry",
            "run_original",
        ),
    )

    assert fetch_result is not None
    assert fetch_result.error == "http_403"
    assert request_urls == [resume_url]
    assert saved_next_urls == []
    clear_checkpoint.assert_not_awaited()


@pytest.mark.asyncio
async def test_import_resources_deletes_stale_rows_only_after_complete_scan(monkeypatch):
    """Verify this provider-directory regression contract."""
    _stub_resource_import_metadata(monkeypatch)

    fetch_results = [
        importer.ResourceFetchResult(
            model=ProviderDirectoryLocation,
            rows=[{"source_id": "source_a", "resource_id": "loc-1"}],
            rows_fetched=1,
            rows_written=0,
            pages_fetched=1,
            complete=True,
            row_limit_reached=False,
            page_limit_reached=False,
            hard_page_limit_reached=False,
            next_url_remaining=False,
        ),
        importer.ResourceFetchResult(
            model=ProviderDirectoryPractitioner,
            rows=[{"source_id": "source_a", "resource_id": "prac-1"}],
            rows_fetched=1,
            rows_written=0,
            pages_fetched=1,
            complete=False,
            row_limit_reached=True,
            page_limit_reached=False,
            hard_page_limit_reached=False,
            next_url_remaining=True,
        ),
    ]

    async def fake_fetch_resource_rows(_source, _resource_type, **_kwargs):
        return fetch_results.pop(0)

    async def fake_upsert(_model, rows, **_kwargs):
        return len(rows)

    deletes: list[tuple[str, dict[str, Any]]] = []

    async def fake_status(sql, **params):
        if "DELETE FROM" in sql.upper():
            deletes.append((sql, params))
            return 4
        return 0

    monkeypatch.setattr(importer, "_fetch_resource_rows", fake_fetch_resource_rows)
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)
    monkeypatch.setattr(importer.db, "status", fake_status)

    resource_fetch_stats_by_type: dict[str, dict[str, Any]] = {}
    stale_counts_by_resource: dict[str, int] = {}
    counts = await importer._import_resources(
        [{"source_id": "source_a", "api_base": "https://example.test/fhir"}],
        resources=["Location", "Practitioner"],
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_1",
        resource_fetch_stats=resource_fetch_stats_by_type,
        stale_counts=stale_counts_by_resource,
        stale_cleanup=True,
    )

    assert counts == {"Location": 1, "Practitioner": 1}
    assert stale_counts_by_resource == {"Location": 4}
    assert len(deletes) == 2
    assert '"provider_directory_source_resource"' in deletes[0][0]
    assert '"provider_directory_location"' in deletes[1][0]
    assert "provider_directory_import_seen" in deletes[0][0]
    assert "provider_directory_import_seen" in deletes[1][0]
    assert deletes[0][1] == {"source_id": "source_a", "run_id": "run_1", "resource_type": "Location"}
    assert deletes[1][1] == {"source_id": "source_a", "run_id": "run_1", "resource_type": "Location"}
    assert resource_fetch_stats_by_type["Location"]["sources_completed"] == 1
    assert resource_fetch_stats_by_type["Practitioner"]["sources_bounded"] == 1


@pytest.mark.asyncio
async def test_import_resources_honors_source_concurrency(monkeypatch):
    _stub_resource_import_metadata(monkeypatch)

    concurrency_by_name = {"active": 0, "max_active": 0}

    async def fake_fetch_resource_rows(source, resource_type, **_kwargs):
        assert resource_type == "Location"
        concurrency_by_name["active"] += 1
        concurrency_by_name["max_active"] = max(concurrency_by_name["max_active"], concurrency_by_name["active"])
        await asyncio.sleep(0.01)
        concurrency_by_name["active"] -= 1
        return importer.ResourceFetchResult(
            model=ProviderDirectoryLocation,
            rows=[{"source_id": source["source_id"], "resource_id": f"{source['source_id']}-loc"}],
            rows_fetched=1,
            rows_written=0,
            pages_fetched=1,
            complete=True,
            row_limit_reached=False,
            page_limit_reached=False,
            hard_page_limit_reached=False,
            next_url_remaining=False,
        )

    async def fake_upsert(_model, rows, **_kwargs):
        return len(rows)

    monkeypatch.setattr(importer, "_fetch_resource_rows", fake_fetch_resource_rows)
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)

    stats_by_resource: dict[str, dict[str, Any]] = {}
    resource_counts = await importer._import_resources(
        [
            {"source_id": "source_a", "api_base": "https://a.example/fhir"},
            {"source_id": "source_b", "api_base": "https://b.example/fhir"},
            {"source_id": "source_c", "api_base": "https://c.example/fhir"},
        ],
        resources=["Location"],
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_1",
        resource_fetch_stats=stats_by_resource,
        source_concurrency=2,
    )

    assert resource_counts == {"Location": 3}
    assert concurrency_by_name["max_active"] == 2
    assert stats_by_resource["Location"]["sources_completed"] == 3
    assert stats_by_resource["Location"]["rows_fetched"] == 3


@pytest.mark.asyncio
async def test_import_resources_reports_streaming_partial_progress(monkeypatch):
    _stub_resource_import_metadata(monkeypatch)

    async def fake_fetch_resource_rows(source, _resource_type, **kwargs):
        row_batch_handler = kwargs["row_batch_handler"]
        written = await row_batch_handler(
            ProviderDirectoryLocation,
            [{"source_id": source["source_id"], "resource_id": "loc-1"}],
        )
        return importer.ResourceFetchResult(
            model=ProviderDirectoryLocation,
            rows=[],
            rows_fetched=1,
            rows_written=written,
            pages_fetched=1,
            complete=True,
            row_limit_reached=False,
            page_limit_reached=False,
            hard_page_limit_reached=False,
            next_url_remaining=False,
        )

    async def fake_upsert_resource_rows(_model, rows, **_kwargs):
        return len(rows)

    progress_events: list[tuple[int, int, dict[str, int], dict[str, object] | None]] = []

    async def progress_callback(done, total, counts, details=None):
        progress_events.append((done, total, dict(counts), details))

    monkeypatch.setattr(importer, "_fetch_resource_rows", fake_fetch_resource_rows)
    monkeypatch.setattr(importer, "_upsert_resource_rows", fake_upsert_resource_rows)

    counts = await importer._import_resources(
        [{"source_id": "source_a", "api_base": "https://a.example/fhir"}],
        resources=["Location"],
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_1",
        stream_batch_size=1,
        progress_callback=progress_callback,
    )

    assert counts == {"Location": 1}
    assert progress_events[0][:3] == (0, 1, {"Location": 0})
    first_active = progress_events[0][3]["active_source_groups"][0]
    assert first_active["sample_source_id"] == "source_a"
    assert first_active["api_base"] == "https://a.example/fhir"
    assert first_active["current_resource"] is None
    assert any(
        event[:3] == (0, 1, {"Location": 1})
        and event[3]["active_source_groups"][0]["current_resource"] == "Location"
        for event in progress_events
    )
    assert progress_events[-1][:3] == (1, 1, {"Location": 1})
    assert progress_events[-1][3]["active_source_groups"] == []


@pytest.mark.asyncio
async def test_mark_provider_directory_progress_persists_structured_detail(monkeypatch):
    calls: list[dict[str, Any]] = []

    async def fake_mark_control_run(*_args, **kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(importer, "mark_control_run", fake_mark_control_run)

    await importer._mark_provider_directory_progress(
        "run_1",
        phase="provider-directory importing resources",
        done=1,
        total=2,
        message="importing",
        details={"active_source_groups": [{"sample_source_id": "source_a", "current_resource": "Location"}]},
    )

    assert calls[0]["progress"]["detail"] == {
        "active_source_groups": [{"sample_source_id": "source_a", "current_resource": "Location"}]
    }


@pytest.mark.asyncio
async def test_duplicate_base_uses_one_compatibility_row(monkeypatch):
    _stub_resource_import_metadata(monkeypatch)

    fetch_calls: list[tuple[str, str]] = []
    upserted_source_ids: list[str] = []

    async def fake_fetch_resource_rows(source, resource_type, **_kwargs):
        fetch_calls.append((source["source_id"], resource_type))
        return importer.ResourceFetchResult(
            model=ProviderDirectoryLocation,
            rows=[{"source_id": source["source_id"], "resource_id": "loc-1"}],
            rows_fetched=1,
            rows_written=0,
            pages_fetched=1,
            complete=True,
            row_limit_reached=False,
            page_limit_reached=False,
            hard_page_limit_reached=False,
            next_url_remaining=False,
        )

    async def fake_upsert(model, rows, **_kwargs):
        if model is ProviderDirectoryLocation:
            upserted_source_ids.extend(row["source_id"] for row in rows)
        return len(rows)

    monkeypatch.setattr(importer, "_fetch_resource_rows", fake_fetch_resource_rows)
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)

    stats_by_resource: dict[str, dict[str, Any]] = {}
    resource_counts = await importer._import_resources(
        [
            {"source_id": "source_a", "api_base": "https://same.example/fhir"},
            {"source_id": "source_b", "api_base": "https://same.example/fhir/"},
        ],
        resources=["Location"],
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_1",
        resource_fetch_stats=stats_by_resource,
    )

    assert fetch_calls == [("source_a", "Location")]
    assert upserted_source_ids == ["source_a"]
    assert resource_counts == {"Location": 1}
    assert stats_by_resource["Location"]["sources_attempted"] == 1
    assert stats_by_resource["Location"]["sources_completed"] == 1


def test_selected_resources_rejects_unknown_resource():
    assert "Endpoint" in importer._selected_resources(None)
    assert importer._selected_resources("InsurancePlan,Location") == ["InsurancePlan", "Location"]
    assert importer._selected_resources((" InsurancePlan ", "Location")) == [
        "InsurancePlan",
        "Location",
    ]
    assert importer._selected_resources('["InsurancePlan", "Location"]') == [
        "InsurancePlan",
        "Location",
    ]
    with pytest.raises(ValueError, match="Unsupported Provider Directory FHIR resources"):
        importer._selected_resources("InsurancePlan,Patient")


def test_selected_resources_rejects_malformed_json_and_non_string_entries():
    with pytest.raises(ValueError, match="valid JSON array"):
        importer._selected_resources('["Location"')
    with pytest.raises(ValueError, match="index 1 must be a string"):
        importer._selected_resources(["Location", 42])
    with pytest.raises(ValueError, match="index 1 must not be empty"):
        importer._selected_resources(["Location", " "])


def test_source_resource_fetch_order_attempts_cigna_practitioner_role_last():
    requested_resources = [
        "PractitionerRole",
        "InsurancePlan",
        "Practitioner",
        "Organization",
        "Location",
        "HealthcareService",
        "OrganizationAffiliation",
        "Endpoint",
    ]

    fetch_order = importer._source_resource_fetch_order(
        {"canonical_api_base": importer.CIGNA_PROVIDER_DIRECTORY_BASE},
        requested_resources,
    )

    assert fetch_order == [
        "InsurancePlan",
        "Practitioner",
        "Organization",
        "Location",
        "HealthcareService",
        "OrganizationAffiliation",
        "Endpoint",
        "PractitionerRole",
    ]
    assert requested_resources[0] == "PractitionerRole"


def test_source_resource_fetch_order_preserves_non_cigna_requested_order():
    requested_resources = ["PractitionerRole", "Location", "Practitioner"]

    fetch_order = importer._source_resource_fetch_order(
        {"api_base": "https://payer.example/fhir"},
        requested_resources,
    )

    assert fetch_order == requested_resources


def test_cigna_resource_page_count_uses_live_hundred_row_bundles():
    source_lookup = {"api_base": importer.CIGNA_PROVIDER_DIRECTORY_BASE}

    assert importer._source_resource_page_count(
        source_lookup,
        "PractitionerRole",
        100,
    ) == 100
    assert importer._source_resource_page_count(
        source_lookup,
        "Practitioner",
        50,
    ) == 50


def test_canonical_backfill_resource_sql_uses_existing_resource_rows():
    canonical_sql, edge_sql = importer._canonical_backfill_resource_sql(
        "Location",
        ProviderDirectoryLocation.__tablename__,
    )

    assert 'INSERT INTO "mrf"."provider_directory_canonical_resource"' in canonical_sql
    assert 'INSERT INTO "mrf"."provider_directory_source_resource"' in edge_sql
    assert 'JOIN "mrf"."provider_directory_source" AS src' in canonical_sql
    assert 'JOIN "mrf"."provider_directory_source" AS src' in edge_sql
    assert "COALESCE(NULLIF(src.canonical_api_base, ''), NULLIF(src.api_base, ''))" in canonical_sql
    assert "(to_jsonb(r) - 'source_id' - 'last_seen_run_id' - 'observed_at' - 'updated_at')" in canonical_sql
    assert "ON CONFLICT (canonical_api_base, resource_type, resource_id) DO UPDATE" in canonical_sql
    assert "ON CONFLICT (source_id, resource_type, resource_id) DO UPDATE" in edge_sql


def test_provider_directory_location_contact_backfill_sql_updates_canonical_contacts():
    sql = importer.provider_directory_location_contact_backfill_sql("mrf")

    assert 'UPDATE "mrf"."provider_directory_location" AS target' in sql
    assert "target.phone_number IS DISTINCT FROM computed.phone_number" in sql
    assert "target.fax_number_digits IS DISTINCT FROM computed.fax_number_digits" in sql
    assert "regexp_replace(COALESCE(loc.telephone_number, '')" in sql
    assert "regexp_replace(COALESCE(loc.fax_number, '')" in sql
    assert "('US', 'USA', 'UNITEDSTATES', 'UNITEDSTATESOFAMERICA')" in sql
    assert "updated_at = now()" in sql


@pytest.mark.asyncio
async def test_process_data_canonical_backfill_only_skips_seed_resolution(monkeypatch):
    expected_metrics_map = {"canonical_rows": 2, "source_edge_rows": 3, "resources": {"Location": {}}}
    backfill = AsyncMock(return_value=expected_metrics_map)

    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(importer, "backfill_provider_directory_canonical_resources", backfill)
    monkeypatch.setattr(
        importer,
        "_resolve_seed_db",
        lambda *_args, **_kwargs: pytest.fail("seed DB should not be resolved for canonical backfill"),
    )

    result = await importer.process_data(
        {"context": {}},
        {"canonical_backfill_only": True, "resources": "Location"},
    )

    assert result == expected_metrics_map
    backfill.assert_awaited_once_with(resources="Location")


@pytest.mark.asyncio
async def test_process_data_reuses_startup_schema_readiness(monkeypatch):
    ensure_tables = AsyncMock()
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", ensure_tables)
    monkeypatch.setattr(
        importer,
        "backfill_provider_directory_canonical_resources",
        AsyncMock(return_value={"canonical_rows": 0}),
    )

    await importer.process_data(
        {"context": {"provider_directory_tables_ready": True}},
        {"canonical_backfill_only": True},
    )

    ensure_tables.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_data_routes_dataset_rehydrate_without_acquisition(monkeypatch):
    ctx = {"context": {"provider_directory_tables_ready": True}}
    expected_metrics = {"resource_rows": 7}
    rehydrate = AsyncMock(return_value=expected_metrics)
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_run_provider_directory_dataset_rehydrate", rehydrate)

    result = await importer.process_data(
        ctx,
        {
            "dataset_rehydrate_only": True,
            "run_id": "run_rehydrate",
            "source_id": "source_rehydrate",
        },
    )

    assert result == expected_metrics
    awaited_ctx, awaited_task, awaited_run_id, awaited_source_ids = (
        rehydrate.await_args.args
    )
    assert awaited_ctx is ctx
    assert awaited_task["dataset_rehydrate_only"] is True
    assert awaited_run_id == "run_rehydrate"
    assert awaited_source_ids == ["source_rehydrate"]


@pytest.mark.asyncio
async def test_process_data_contact_backfill_only_skips_seed_resolution(monkeypatch):
    expected_metrics_map = {"location_contact_rows_updated": 7}
    backfill = AsyncMock(return_value=expected_metrics_map)

    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(importer, "backfill_provider_directory_location_contacts", backfill)
    monkeypatch.setattr(
        importer,
        "_resolve_seed_db",
        lambda *_args, **_kwargs: pytest.fail("seed DB should not be resolved for contact backfill"),
    )

    result = await importer.process_data(
        {"context": {}},
        {"contact_backfill_only": True},
    )

    assert result == expected_metrics_map
    backfill.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_process_data_publish_artifacts_only_skips_seed_resolution(monkeypatch):
    """Verify process data publish artifacts only skips seed resolution."""
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(importer, "_mark_provider_directory_progress", AsyncMock())
    _stub_artifact_dataset_scope(monkeypatch)
    monkeypatch.setattr(
        importer,
        "backfill_provider_directory_location_contacts",
        AsyncMock(return_value={"location_contact_rows_updated": 5}),
    )
    monkeypatch.setattr(importer, "backfill_provider_directory_location_coordinates", AsyncMock(return_value=6))
    monkeypatch.setattr(
        importer,
        "backfill_provider_directory_resource_id_npis",
        AsyncMock(return_value={"Practitioner": 2, "Organization": 1}),
    )
    monkeypatch.setattr(importer, "publish_provider_directory_location_address_keys", AsyncMock(return_value=3))
    monkeypatch.setattr(
        importer,
        "publish_provider_directory_location_archive",
        AsyncMock(return_value={"inserted": 4, "provenance_updates": 1}),
    )
    monkeypatch.setattr(importer, "publish_provider_directory_address_overlay", AsyncMock(return_value={}))
    monkeypatch.setattr(importer, "publish_provider_directory_network_catalog", AsyncMock(return_value={"rows": 8}))
    monkeypatch.setattr(
        importer,
        "publish_provider_directory_address_corroboration_if_available",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        importer,
        "_resolve_seed_db",
        lambda *_args, **_kwargs: pytest.fail("seed DB should not be resolved for artifact publish"),
    )

    operation_result = await importer.process_data(
        {"context": {}},
        {"publish_artifacts_only": True, "run_id": "run_publish"},
    )

    assert operation_result["publish_artifacts_only"] is True
    assert operation_result["location_contacts_backfilled"] == {"location_contact_rows_updated": 5}
    assert operation_result["location_coordinates_backfilled"] == 6
    assert operation_result["resource_id_npis_backfilled"] == {"Practitioner": 2, "Organization": 1}
    assert operation_result["location_address_keys_stamped"] == 3
    assert operation_result["location_archive"] == {"inserted": 4, "provenance_updates": 1}
    assert operation_result["network_catalog"] == {"rows": 8}
    assert operation_result["publish_corroboration"] is False
    assert operation_result["ptg_corroboration_view_published"] is False
    assert operation_result["ptg_corroboration_view_skipped"] == {
        "reason": "publish_corroboration_disabled",
    }
    importer.backfill_provider_directory_location_contacts.assert_awaited_once()
    importer.backfill_provider_directory_location_coordinates.assert_awaited_once_with(
        run_id=None,
        source_ids=[],
        seen_table=None,
    )
    importer.backfill_provider_directory_resource_id_npis.assert_awaited_once_with(
        run_id=None,
        source_ids=[],
        seen_table=None,
    )
    importer.publish_provider_directory_location_address_keys.assert_awaited_once_with(
        run_id=None, source_ids=[],
        seen_table=None,
    )
    importer.publish_provider_directory_location_archive.assert_awaited_once_with(
        run_id=None, source_ids=[],
        seen_table=None,
    )
    importer.publish_provider_directory_address_overlay.assert_awaited_once_with(
        run_id=None,
        source_ids=[],
        defer_cutover=True,
    )
    importer.publish_provider_directory_network_catalog.assert_awaited_once_with(
        run_id=None,
        source_ids=[],
        defer_cutover=True,
    )
    importer.publish_provider_directory_address_corroboration_if_available.assert_not_awaited()

def test_harness_fixture_case_and_report_rendering(tmp_path):
    fixture_case_result = harness._run_fixture_case()
    report_payload_map = {
        "generated_at": "2026-06-28T00:00:00Z",
        "overall_status": "succeeded",
        "results": [
            fixture_case_result.to_json(),
            harness.CaseResult(
                case_id="coverage-audit",
                kind="audit",
                status="failed",
                elapsed_seconds=0.2,
                metrics={
                    "serving_readiness_status": "not_ready",
                    "serving_required_fail_count": 1,
                    "serving_failed_required_checks": ["searchable_phone_overlay"],
                    "serving_phone_rows": 0,
                    "serving_phone_pct": 0.0,
                },
            ).to_json(),
        ],
    }
    harness.write_report(report_payload_map, tmp_path)

    assert fixture_case_result.status == "succeeded"
    assert fixture_case_result.metrics["supported_resources"] == ["Endpoint", "InsurancePlan", "PractitionerRole"]
    assert fixture_case_result.metrics["resource_counts"]["provider_directory_endpoint"] == 1
    assert (tmp_path / "report.json").exists()
    assert (tmp_path / "report.md").exists()
    assert json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))["overall_status"] == "succeeded"
    report_markdown = (tmp_path / "report.md").read_text(encoding="utf-8")
    assert "## Coverage Audit Readiness" in report_markdown
    assert "failed required checks: `searchable_phone_overlay`" in report_markdown
    assert "phone rows: `0` (0.0%)" in report_markdown


def test_harness_sql_typing_rejects_unsafe_schema_name():
    assert harness._validate_identifier("provider_directory_sql_typing_test", label="schema") == (
        "provider_directory_sql_typing_test"
    )
    with pytest.raises(ValueError):
        harness._validate_identifier("provider_directory_sql_typing_test;drop", label="schema")


def _harness_cli_args(**overrides: Any) -> argparse.Namespace:
    """Build the full local CLI harness namespace used by direct helper tests."""
    cli_arg_map: dict[str, Any] = dict(
        python="python-test",
        seed_db_path="/tmp/provider_directory.db",
        seed_db_url=None,
        retest_results_path=None,
        retest_results_url=None,
        credential_config_file=None,
        limit=5,
        timeout=10,
        concurrency=2,
        source_query=None,
        cli_test_mode=True,
        refresh_preset=None,
        include_supplemental_catalogs=None,
        import_resources=False,
        full_refresh=False,
        stale_cleanup=None,
        resource_limit=None,
        resource_deadline_seconds=None,
        linked_resource_limit=None,
        linked_resource_deadline_seconds=None,
        page_limit=None,
        page_count=None,
        stream_batch_size=None,
        source_concurrency=None,
        publish_artifacts=None,
        resources=None,
        include_credentialed=False,
        seed_only=True,
        no_probe=True,
        env={},
        command_timeout=30,
        db_host="127.0.0.1",
        db_port=5440,
        db_database="healthporta_test",
        db_user="nick",
        db_password="",
        db_schema="mrf",
        coverage_audit_timeout=30,
        coverage_audit_statement_timeout_ms=0,
        coverage_audit_pod_safe=True,
        coverage_audit_require_serving_ready=False,
        coverage_audit_fast_serving_readiness=False,
    )
    cli_arg_map.update(overrides)
    return argparse.Namespace(**cli_arg_map)


def test_harness_local_cli_passes_retest_supplement_args(monkeypatch, tmp_path):
    """The harness forwards monthly catalog controls to local CLI runs."""
    calls: list[dict[str, Any]] = []

    class Result:
        returncode = 0
        stdout = (
            "PROVIDER_DIRECTORY_FHIR_IMPORT_DONE\t"
            '{"sources_seeded": 2, "supplemental_retest_sources_considered": 1}'
        )

    def fake_run(command, **kwargs):
        calls.append({"command": command, "kwargs": kwargs})
        return Result()

    monkeypatch.setattr(harness.subprocess, "run", fake_run)
    args = _harness_cli_args(
        retest_results_path="/tmp/retest_results.json",
        retest_results_url="https://example.test/retest_results.json",
        credential_config_file="/tmp/provider-directory-credentials.json",
        refresh_preset="monthly-full",
        include_supplemental_catalogs=True,
    )

    harness_result = harness._run_cli_case("local-cli", args)

    assert harness_result.status == "succeeded"
    command = calls[0]["command"]
    assert command[:4] == ["python-test", "main.py", "start", "provider-directory-fhir"]
    assert "--test" in command
    assert command[command.index("--retest-results-path") + 1] == "/tmp/retest_results.json"
    assert command[command.index("--retest-results-url") + 1] == "https://example.test/retest_results.json"
    assert command[command.index("--credential-config-file") + 1] == "/tmp/provider-directory-credentials.json"
    assert command[command.index("--refresh-preset") + 1] == "monthly-full"
    assert "--include-supplemental-catalogs" in command
    assert harness_result.metrics["supplemental_retest_sources_considered"] == 1


def test_harness_parse_args_accepts_retest_supplement_args():
    args = harness.parse_args(
        [
            "--retest-results-path",
            "/tmp/retest_results.json",
            "--retest-results-url",
            "https://example.test/retest_results.json",
            "--credential-config-file",
            "/tmp/provider-directory-credentials.json",
            "--coverage-audit",
            "--coverage-audit-statement-timeout-ms",
            "2500",
            "--coverage-audit-full",
            "--coverage-audit-require-serving-ready",
            "--coverage-audit-fast-serving-readiness",
            "--db-schema",
            "mrf_test",
            "--no-cli-test-mode",
            "--seed-db-url",
            "https://example.test/provider_directory.db",
            "--refresh-preset",
            "monthly-full",
            "--include-supplemental-catalogs",
            "--linked-resource-deadline-seconds",
            "1800",
        ]
    )

    assert args.retest_results_path == "/tmp/retest_results.json"
    assert args.retest_results_url == "https://example.test/retest_results.json"
    assert args.credential_config_file == "/tmp/provider-directory-credentials.json"
    assert args.coverage_audit is True
    assert args.coverage_audit_statement_timeout_ms == 2500
    assert args.coverage_audit_pod_safe is False
    assert args.coverage_audit_require_serving_ready is True
    assert args.coverage_audit_fast_serving_readiness is True
    assert args.db_schema == "mrf_test"
    assert args.cli_test_mode is False
    assert args.seed_db_url == "https://example.test/provider_directory.db"
    assert args.refresh_preset == "monthly-full"
    assert args.include_supplemental_catalogs is True
    assert args.linked_resource_deadline_seconds == 1800


def test_harness_fixture_covers_healthcare_service_location_refs():
    result = harness._run_fixture_case()

    assert result.status == "succeeded"
    assert result.metrics["resource_counts"]["provider_directory_healthcare_service"] == 1
    assert result.metrics["resource_counts"]["provider_directory_practitioner_role"] == 1


def _coverage_audit_ready_report_json() -> str:
    return json.dumps(
        {
            "source_summary": {
                "source_count": 2,
                "live_valid_count": 2,
                "live_auth_required_count": 0,
                "never_probed_count": 0,
            },
            "resource_summary": {
                "provider_directory_location": {"row_count": 20},
                "provider_directory_practitioner_role": {"row_count": 20},
            },
            "serving_readiness": {
                "status": "ready",
                "required_fail_count": 0,
                "checks": [
                    {
                        "name": "searchable_phone_overlay",
                        "status": "pass",
                        "required": True,
                        "metrics": {
                            "provider_directory_phone_rows": 12,
                            "provider_directory_phone_pct": 60.0,
                        },
                    }
                ],
            },
        }
    )


def test_harness_coverage_audit_case_reports_compact_metrics(monkeypatch):
    """Harness audit metrics retain serving-readiness details."""
    invoked_commands: list[list[str]] = []

    class Result:
        returncode = 0
        stdout = _coverage_audit_ready_report_json()

    def fake_run(command, **_kwargs):
        invoked_commands.append(command)
        return Result()

    monkeypatch.setattr(harness.subprocess, "run", fake_run)
    args = _harness_cli_args(
        db_database="wrong_database",
        db_schema="wrong_schema",
        coverage_audit_statement_timeout_ms=2500,
        env={
            "HLTHPRT_DB_DATABASE": "hp_pd_harness_test",
            "HLTHPRT_DB_SCHEMA": "mrf_test",
        },
    )

    audit_result = harness._run_coverage_audit_case(args)

    assert audit_result.status == "succeeded"
    assert invoked_commands[0][0:2] == ["python-test", "scripts/research/provider_directory_coverage_audit.py"]
    assert invoked_commands[0][invoked_commands[0].index("--database") + 1] == "hp_pd_harness_test"
    assert invoked_commands[0][invoked_commands[0].index("--schema") + 1] == "mrf_test"
    assert "--pod-safe" in invoked_commands[0]
    assert audit_result.metrics["source_count"] == 2
    assert audit_result.metrics["resource_rows"]["provider_directory_location"] == 20
    assert audit_result.metrics["serving_readiness_status"] == "ready"
    assert audit_result.metrics["serving_required_fail_count"] == 0
    assert audit_result.metrics["serving_required_checks"] == ["searchable_phone_overlay"]
    assert audit_result.metrics["serving_failed_required_checks"] == []
    assert audit_result.metrics["serving_phone_rows"] == 12
    assert audit_result.metrics["serving_phone_pct"] == 60.0


def test_harness_coverage_audit_full_mode_can_gate_serving_readiness(monkeypatch):
    invoked_commands: list[list[str]] = []

    class Result:
        returncode = 1
        stdout = json.dumps(
            {
                "source_summary": {"source_count": 1},
                "resource_summary": {},
                "serving_readiness": {
                    "status": "not_ready",
                    "required_fail_count": 2,
                    "checks": [
                        {
                            "name": "searchable_phone_overlay",
                            "status": "fail",
                            "required": True,
                            "metrics": {"provider_directory_phone_rows": 0},
                        },
                        {
                            "name": "network_catalog_published",
                            "status": "fail",
                            "required": True,
                            "metrics": {},
                        },
                    ],
                },
            }
        )

    def fake_run(command, **_kwargs):
        invoked_commands.append(command)
        return Result()

    monkeypatch.setattr(harness.subprocess, "run", fake_run)
    args = _harness_cli_args(
        coverage_audit_pod_safe=False,
        coverage_audit_require_serving_ready=True,
        coverage_audit_fast_serving_readiness=True,
    )

    audit_result = harness._run_coverage_audit_case(args)

    assert audit_result.status == "failed"
    assert "--pod-safe" not in invoked_commands[0]
    assert "--require-serving-ready" in invoked_commands[0]
    assert "--fast-serving-readiness" in invoked_commands[0]
    assert audit_result.metrics["serving_readiness_status"] == "not_ready"
    assert audit_result.metrics["serving_required_fail_count"] == 2
    assert audit_result.metrics["serving_failed_required_checks"] == [
        "searchable_phone_overlay",
        "network_catalog_published",
    ]
    assert audit_result.metrics["serving_phone_rows"] == 0


def test_harness_run_includes_sql_typing_case(monkeypatch, tmp_path):
    monkeypatch.setattr(
        harness,
        "_run_fixture_case",
        lambda: harness.CaseResult(
            case_id="fixture-parser",
            kind="fixture",
            status="succeeded",
            elapsed_seconds=0.1,
        ),
    )
    monkeypatch.setattr(
        harness,
        "_run_sql_typing_case",
        lambda _args: harness.CaseResult(
            case_id="sql-typing",
            kind="sql",
            status="succeeded",
            elapsed_seconds=0.2,
            metrics={"schema": "provider_directory_sql_typing_test"},
        ),
    )
    args = argparse.Namespace(
        output_dir=str(tmp_path),
        sql_typing=True,
        local_cli=False,
        coverage_audit=False,
        control_url=None,
    )

    report = harness.run(args)

    assert report["overall_status"] == "succeeded"
    assert [sql_case["case_id"] for sql_case in report["results"]] == ["fixture-parser", "sql-typing"]
    assert json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))["results"][1]["kind"] == "sql"


def test_harness_run_includes_requested_coverage_audit_case(monkeypatch, tmp_path):
    monkeypatch.setattr(
        harness,
        "_run_fixture_case",
        lambda: harness.CaseResult(case_id="fixture-parser", kind="fixture", status="succeeded", elapsed_seconds=0.1),
    )
    monkeypatch.setattr(
        harness,
        "_run_coverage_audit_case",
        lambda _args: harness.CaseResult(
            case_id="coverage-audit",
            kind="audit",
            status="succeeded",
            elapsed_seconds=0.2,
            metrics={"source_count": 2},
        ),
    )
    args = argparse.Namespace(
        output_dir=str(tmp_path),
        sql_typing=False,
        local_cli=False,
        coverage_audit=True,
        control_url=None,
    )

    report = harness.run(args)

    assert report["overall_status"] == "succeeded"
    assert [item["case_id"] for item in report["results"]] == ["fixture-parser", "coverage-audit"]


def test_harness_local_cli_uses_importer_default_seed_when_not_supplied(monkeypatch):
    invoked_commands: list[list[str]] = []

    class Result:
        returncode = 0
        stdout = "Provider Directory FHIR import done: sources_seeded=1 sources_probed=0 valid_capability_sources=0 resource_rows={}"

    def fake_run(command, **_kwargs):
        invoked_commands.append(command)
        return Result()

    monkeypatch.setattr(harness.subprocess, "run", fake_run)
    args = _harness_cli_args(
        seed_db_path=None,
        seed_db_url=None,
        limit=1,
        concurrency=1,
        cli_test_mode=False,
    )

    harness_result = harness._run_cli_case("local-cli", args)

    assert harness_result.status == "succeeded"
    assert "--test" not in invoked_commands[0]
    assert "--seed-db-path" not in invoked_commands[0]
    assert "--seed-db-url" not in invoked_commands[0]


def test_primary_gateway_host():
    public_cigna_dict = {
        "source_id": "cigna_public",
        "api_base": importer.CIGNA_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.CIGNA_PROVIDER_DIRECTORY_BASE,
        "endpoint_network": "https://apps.availity.com/availity/public-fhir/fhir/v1/cigna/r4/Network",
    }
    availity_primary_dict = {
        "source_id": "availity_source",
        "api_base": "https://apps.availity.com/availity/public-fhir/fhir/v1/payer/r4",
        "canonical_api_base": "https://apps.availity.com/availity/public-fhir/fhir/v1/payer/r4",
    }

    assert importer._uses_source_known_onboarding_gateway(public_cigna_dict) is False
    assert importer._uses_source_known_onboarding_gateway(availity_primary_dict) is True


def test_location_archive_source_scope():
    sql = importer.provider_directory_location_archive_stage_sql(
        "mrf",
        "provider_directory_location_archive_stage_test",
        run_id="run_1",
        source_ids=["source_a", "source_b"],
    )

    assert "loc.last_seen_run_id = CAST(:run_id AS varchar)" in sql
    assert "loc.source_id = ANY(CAST(:source_ids AS varchar[]))" in sql


@pytest.mark.asyncio
async def test_artifact_publish_source_scope(monkeypatch):
    """Artifact publishing should pass run and source scope to each stage."""
    monkeypatch.setattr(importer, "_mark_provider_directory_progress", AsyncMock())
    monkeypatch.setattr(
        importer,
        "backfill_provider_directory_location_contacts",
        AsyncMock(return_value={"location_contact_rows_updated": 2}),
    )
    coordinate_backfill = AsyncMock(return_value=8)
    key_publish = AsyncMock(return_value=3)
    archive_publish = AsyncMock(return_value={"inserted": 4, "provenance_updates": 5})
    overlay_publish = AsyncMock(return_value={"published": True, "rows": 6})
    network_catalog_publish = AsyncMock(return_value={"published": True, "rows": 7})
    monkeypatch.setattr(importer, "backfill_provider_directory_location_coordinates", coordinate_backfill)
    monkeypatch.setattr(importer, "backfill_provider_directory_resource_id_npis", AsyncMock(return_value={}))
    monkeypatch.setattr(importer, "publish_provider_directory_location_address_keys", key_publish)
    monkeypatch.setattr(importer, "publish_provider_directory_location_archive", archive_publish)
    monkeypatch.setattr(importer, "publish_provider_directory_address_overlay", overlay_publish)
    monkeypatch.setattr(importer, "publish_provider_directory_network_catalog", network_catalog_publish)

    metrics = await importer._publish_provider_directory_artifacts(
        run_id="run_1",
        metrics={},
        address_key_run_id="run_1",
        source_ids=["source_a", "source_b"],
    )

    assert metrics["location_coordinates_backfilled"] == 8
    assert metrics["location_address_keys_stamped"] == 3
    assert metrics["location_archive"]["inserted"] == 4
    assert metrics["address_overlay"] == {"published": True, "rows": 6}
    assert metrics["network_catalog"] == {"published": True, "rows": 7}
    coordinate_backfill.assert_awaited_once_with(
        run_id="run_1",
        source_ids=["source_a", "source_b"],
        seen_table=None,
    )
    key_publish.assert_awaited_once_with(
        run_id="run_1",
        source_ids=["source_a", "source_b"],
        seen_table=None,
    )
    archive_publish.assert_awaited_once_with(
        run_id="run_1",
        source_ids=["source_a", "source_b"],
        seen_table=None,
    )
    overlay_publish.assert_awaited_once_with(
        run_id="run_1",
        source_ids=["source_a", "source_b"],
    )
    network_catalog_publish.assert_awaited_once_with(
        run_id="run_1",
        source_ids=["source_a", "source_b"],
    )


@pytest.mark.asyncio
async def test_resource_id_npi_backfill_receives_publish_scope(monkeypatch):
    """The resource-id NPI stage should honor artifact run and source scope."""
    monkeypatch.setattr(importer, "_mark_provider_directory_progress", AsyncMock())
    npi_backfill = AsyncMock(return_value={"Practitioner": 2, "Organization": 1})
    monkeypatch.setattr(importer, "backfill_provider_directory_resource_id_npis", npi_backfill)

    metrics = await importer._publish_provider_directory_artifacts(
        run_id="run_1",
        metrics={},
        address_key_run_id="run_1",
        source_ids=["source_a", "source_b"],
        publish_artifacts_targets={"resource_id_npis"},
    )

    assert metrics["resource_id_npis_backfilled"] == {"Practitioner": 2, "Organization": 1}
    npi_backfill.assert_awaited_once_with(
        run_id="run_1",
        source_ids=["source_a", "source_b"],
        seen_table=None,
    )


def test_address_overlay_sql_scope():
    sql = importer.provider_directory_address_overlay_insert_sql(
        "mrf",
        "provider_directory_address_overlay_stage_test",
        run_id="run_1",
        source_ids=["source_a"],
    )

    assert 'INSERT INTO "mrf"."provider_directory_address_overlay_stage_test"' in sql
    assert "provider_directory_organization" in sql
    assert "provider_directory_fhir:organization_address:" in sql
    assert "organization.last_seen_run_id = CAST(:run_id AS varchar)" in sql
    assert "organization.source_id = ANY(CAST(:source_ids AS varchar[]))" in sql
    assert "addr_key_v1" in sql
    assert "UNITEDSTATESOFAMERICA" in sql
    assert "THEN 'US'" in sql
    assert "role_phone.telephone_number," in sql
    assert "WHEN loc.phone_number IS NOT NULL THEN loc.telephone_number" in sql
    assert "practitioner_phone.telephone_number" in sql
    assert "organization_phone.telephone_number" in sql
    assert "role.healthcare_service_refs" in sql
    assert "affiliation.healthcare_service_refs" in sql
    assert "provider_directory_healthcare_service" in sql
    assert "COALESCE(practitioner.npi, role.npi)::bigint AS npi" in sql
    assert "COALESCE(practitioner.npi, role.npi) BETWEEN 1000000000 AND 9999999999" in sql
    assert "AS role_phone ON TRUE" in sql
    assert "loc.latitude" in sql
    assert "/ 1000000" in sql
    assert "ABS(" in sql
    assert "lat,\n        long," in sql
    assert "'HealthcareService'::varchar AS resource_type" not in sql
    assert "healthcare_service.npi" not in sql


def test_address_projection_stays_role_and_affiliation_driven_when_services_have_npis():
    sql = importer.provider_directory_address_overlay_insert_sql(
        "mrf",
        "provider_directory_address_overlay_stage_test",
    )

    assert "provider_directory_fhir:practitioner_role:" in sql
    assert "provider_directory_fhir:organization_affiliation:" in sql
    assert "provider_directory_fhir:healthcare_service:" not in sql
    assert "'HealthcareService'::varchar AS resource_type" not in sql
    assert "healthcare_service.npi" not in sql


def test_address_overlay_table_sql_includes_coordinates():
    sql = importer.provider_directory_address_overlay_table_sql("mrf")
    alter_sql = importer.address_overlay_coordinate_columns_sql("mrf")

    assert "lat numeric" in sql
    assert "long numeric" in sql
    assert "ADD COLUMN IF NOT EXISTS lat numeric" not in sql
    assert "ADD COLUMN IF NOT EXISTS lat numeric" in alter_sql
    assert "ADD COLUMN IF NOT EXISTS long numeric" in alter_sql


def test_address_overlay_component_sql_is_bounded_to_one_component():
    sql = importer._address_overlay_component_insert_sql(
        "mrf",
        "provider_directory_address_overlay_stage_test",
        component="organization_address",
        run_id="run_1",
        source_ids=["source_a"],
    )

    assert 'INSERT INTO "mrf"."provider_directory_address_overlay_stage_test"' in sql
    assert "provider_directory_fhir:organization_address:" in sql
    assert "provider_directory_practitioner_role" not in sql
    assert "provider_directory_organization_affiliation" not in sql
    assert "UNION ALL" not in sql
    assert "organization.last_seen_run_id = CAST(:run_id AS varchar)" in sql
    assert "organization.source_id = ANY(CAST(:source_ids AS varchar[]))" in sql
    assert "raw_org_addresses AS MATERIALIZED" in sql
    assert "org_address_keys AS MATERIALIZED" in sql
    assert "SELECT DISTINCT" in sql
    assert "address_lookup_key" in sql
    assert "JOIN org_address_keys AS keys" in sql
    assert "keys.address_lookup_key = raw.address_lookup_key" in sql
    assert "IS NOT DISTINCT FROM raw" not in sql
    assert "UNITEDSTATESOFAMERICA" in sql
    assert "THEN 'US'" in sql
    assert sql.count("addr_key_v1(") == 1
    assert "raw.telephone_number" in sql
    assert "NULL::varchar AS phone_number" not in sql


def test_address_overlay_direct_practitioner_component_is_canonical_and_bounded():
    sql = importer._address_overlay_component_insert_sql(
        "mrf",
        "provider_directory_address_overlay_stage_test",
        component="practitioner_address",
        run_id="run_1",
        source_ids=["source_a"],
    )

    assert 'INSERT INTO "mrf"."provider_directory_address_overlay_stage_test"' in sql
    assert "provider_directory_fhir:practitioner_address:" in sql
    assert "'Practitioner'::varchar AS resource_type" in sql
    assert 'FROM "mrf"."provider_directory_practitioner" AS practitioner' in sql
    assert "provider_directory_practitioner_role" not in sql
    assert "provider_directory_organization_affiliation" not in sql
    assert "practitioner.last_seen_run_id = CAST(:run_id AS varchar)" in sql
    assert "practitioner.source_id = ANY(CAST(:source_ids AS varchar[]))" in sql
    assert "practitioner.active IS DISTINCT FROM false" in sql
    assert "COALESCE(practitioner_rows.addresses, '[]'::jsonb)" in sql
    assert "raw_practitioner_addresses AS MATERIALIZED" in sql
    assert "practitioner_address_keys AS MATERIALIZED" in sql
    assert sql.count("addr_key_v1(") == 1
    assert "raw.telephone_number" in sql
    assert "raw.fax_number" in sql
    assert "UNITEDSTATESOFAMERICA" in sql
    assert "NULL::numeric AS lat" in sql
    assert "NULL::numeric AS long" in sql


def test_address_overlay_practitioner_component_uses_all_phone_and_location_paths():
    sql = importer._address_overlay_component_insert_sql(
        "mrf",
        "provider_directory_address_overlay_stage_test",
        component="practitioner_role",
        run_id="run_1",
        source_ids=["source_a"],
    )

    assert "role_phone.telephone_number," in sql
    assert "WHEN loc.phone_number IS NOT NULL THEN loc.telephone_number" in sql
    assert "practitioner_phone.telephone_number" in sql
    assert "practitioner_fax.fax_number" in sql
    assert "AS role_phone ON TRUE" in sql
    assert "AS role_fax ON TRUE" in sql
    assert "AS practitioner_phone ON TRUE" in sql
    assert "AS practitioner_fax ON TRUE" in sql
    assert "role.healthcare_service_refs" in sql
    assert 'JOIN "mrf"."provider_directory_healthcare_service" AS healthcare_service' in sql
    assert "healthcare_service.location_refs" in sql
    assert "loc.latitude" in sql
    assert "loc.longitude" in sql
    assert "/ 1000000" in sql
    assert "ABS(" in sql


@pytest.mark.parametrize(
    "sql",
    [
        importer.provider_directory_address_overlay_insert_sql(
            "mrf",
            "provider_directory_address_overlay_stage_test",
        ),
        importer._address_overlay_component_insert_sql(
            "mrf",
            "provider_directory_address_overlay_stage_test",
            component="practitioner_role",
        ),
    ],
)
def test_practitioner_role_address_phone_prefers_usable_location_then_role(sql):
    role_sql = sql.split("provider_directory_fhir:practitioner_role:", 1)[1]
    compact_sql = " ".join(role_sql.split())
    raw_phone_sql, normalized_phone_sql = compact_sql.split("AS telephone_number", 1)[0], compact_sql.split(
        "AS fax_number,", 1
    )[1].split("AS phone_number", 1)[0]

    assert (
        "COALESCE( CASE WHEN loc.phone_number IS NOT NULL THEN loc.telephone_number END, "
        "role_phone.telephone_number, practitioner_phone.telephone_number )::varchar AS telephone_number"
        in f"{raw_phone_sql}AS telephone_number"
    )
    assert normalized_phone_sql.lstrip().startswith("COALESCE( loc.phone_number,")
    assert normalized_phone_sql.index("role_phone.telephone_number") < normalized_phone_sql.index(
        "practitioner_phone.telephone_number"
    )


def test_address_overlay_affiliation_component_uses_context_contact_fallbacks_and_service_locations():
    sql = importer._address_overlay_component_insert_sql(
        "mrf",
        "provider_directory_address_overlay_stage_test",
        component="organization_affiliation",
        run_id="run_1",
        source_ids=["source_a"],
    )
    compact_sql = " ".join(sql.split())

    assert (
        "COALESCE( CASE WHEN loc.phone_number IS NOT NULL THEN loc.telephone_number END, "
        "organization_phone.telephone_number, affiliation_phone.telephone_number )::varchar "
        "AS telephone_number"
    ) in compact_sql
    assert (
        "COALESCE( loc.fax_number, organization_fax.fax_number, "
        "affiliation_fax.fax_number )::varchar AS fax_number"
    ) in compact_sql
    assert "AS organization_phone ON TRUE" in sql
    assert "AS organization_fax ON TRUE" in sql
    assert "COALESCE(affiliation.telecom::jsonb, '[]'::jsonb)" in sql
    assert "AS affiliation_phone ON TRUE" in sql
    assert "AS affiliation_fax ON TRUE" in sql
    assert "affiliation.healthcare_service_refs" in sql
    assert 'JOIN "mrf"."provider_directory_healthcare_service" AS healthcare_service' in sql
    assert "healthcare_service.location_refs" in sql
    assert "WITH overlay_rows AS MATERIALIZED" in sql
    assert "FROM overlay_rows" in sql
    assert sql.count("addr_key_v1(") == 1


def test_address_overlay_affiliation_uses_organization_address_only_without_location():
    sql = importer._address_overlay_component_insert_sql(
        "mrf",
        "provider_directory_address_overlay_stage_test",
        component="organization_affiliation",
        run_id="run_1",
        source_ids=["source_a"],
    )
    compact_sql = " ".join(sql.split())

    assert "WITH direct_locations AS MATERIALIZED" in compact_sql
    assert "organization_addresses AS" in compact_sql
    assert "organization.address_json::jsonb" in compact_sql
    assert (
        "SELECT * FROM direct_locations UNION ALL SELECT * FROM organization_addresses "
        "WHERE NOT EXISTS (SELECT 1 FROM direct_locations)"
    ) in compact_sql
    assert (
        "'organization-' || organization.resource_id || '-address-' || addr.ordinal::varchar"
    ) in compact_sql
    assert "NULL::varchar AS latitude" in compact_sql
    assert "NULL::varchar AS longitude" in compact_sql


@pytest.mark.parametrize(
    "sql",
    [
        importer.provider_directory_address_overlay_insert_sql(
            "mrf",
            "provider_directory_address_overlay_stage_test",
        ),
        importer._address_overlay_component_insert_sql(
            "mrf",
            "provider_directory_address_overlay_stage_test",
            component="organization_affiliation",
        ),
    ],
)
def test_contra_costa_affiliation_telecom_fallback_keeps_extensions_and_canonical_digits(sql):
    phone = "(925) 313-6000 ext. 204"
    fax = "(925) 313-6001 x9"
    canonical = importer._location_contact_fields(phone, fax, "US")
    compact_sql = " ".join(sql.split())
    affiliation_sql = compact_sql.split(
        "provider_directory_fhir:organization_affiliation:", 1
    )[1]

    assert "organization_phone.telephone_number, affiliation_phone.telephone_number" in compact_sql
    assert "organization_fax.fax_number, affiliation_fax.fax_number" in compact_sql
    assert "affiliation_phone.telephone_number" in affiliation_sql.split(
        "AS phone_number", 1
    )[0]
    assert "affiliation_fax.fax_number" in affiliation_sql.split(
        "AS fax_number_digits", 1
    )[0]
    assert canonical == {
        "phone_number": "9253136000",
        "fax_number_digits": "9253136001",
        "phone_extension": "204",
        "fax_extension": "9",
    }


def test_address_overlay_affiliation_keeps_location_lookup_correlated():
    sql = importer._address_overlay_component_insert_sql(
        "mrf",
        "provider_directory_address_overlay_stage_test",
        component="organization_affiliation",
        run_id="run_1",
        source_ids=["source_a"],
    )
    compact_sql = " ".join(sql.split())

    assert (
        'JOIN "mrf"."provider_directory_location" AS location '
        "ON location.source_id = affiliation.source_id"
    ) in compact_sql
    assert "UNION SELECT service_location_ref.value" in compact_sql
    assert "OFFSET 0 ) AS loc ON TRUE" in compact_sql
    assert (
        'AS location_ref ON TRUE JOIN "mrf"."provider_directory_location"'
        not in compact_sql
    )
    assert "location.status IS NULL OR lower(location.status) <> 'inactive'" in compact_sql


def test_address_overlay_stage_index_names_are_hash_safe():
    stage_table = importer._address_overlay_stage_table_name("run_1")
    index_name = importer._address_overlay_index_name(stage_table, "source_record_idx")

    assert len(index_name) <= 63
    assert index_name.startswith("provider_directory_address_overlay_stage_")
    assert index_name.endswith("_idx") or "_" in index_name


def _address_overlay_cleanup_fixtures():
    """Build run ownership and catalog rows for cleanup policy coverage."""
    stages_by_status = {
        name: importer._address_overlay_stage_table_name(f"run_{name}")
        for name in ("current", "orphan", "active", "recent", "unowned")
    }
    invalid_stage = "provider_directory_address_overlay_stage_bad; DROP TABLE mrf.important"
    owner_rows = [
        ("run_current", "running", False),
        ("run_orphan", "failed", True),
        ("run_active", "running", False),
        ("run_recent", "failed", False),
    ]
    catalog_rows = [
        (stages_by_status[name],) for name in stages_by_status
    ] + [(invalid_stage,)]
    return stages_by_status, invalid_stage, owner_rows, catalog_rows


@pytest.mark.asyncio
async def test_address_overlay_orphan_stage_cleanup_removes_only_valid_orphans(monkeypatch):
    """Only an old terminal run may lose its validated overlay stage."""
    stages_by_status, invalid_stage, owner_rows, catalog_rows = (
        _address_overlay_cleanup_fixtures()
    )
    catalog_query = AsyncMock(side_effect=[owner_rows, catalog_rows])
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "all", catalog_query)
    monkeypatch.setattr(importer.db, "status", status)

    metrics = await importer._cleanup_orphan_address_overlay_stages(
        "mrf",
        current_stage_table=stages_by_status["current"],
    )

    assert metrics == {
        "scanned": 6,
        "removed": 1,
        "current_stage_retained": 1,
        "active_stage_retained": 1,
        "recent_stage_retained": 1,
        "unowned_stage_retained": 1,
        "ambiguous_owner_retained": 0,
        "ownership_unavailable": 0,
        "rejected": 1,
    }
    owner_call, catalog_call = catalog_query.await_args_list
    owner_sql = str(owner_call.args[0])
    assert 'FROM "mrf"."import_run"' in owner_sql
    assert "finished_at <= now() - make_interval" in owner_sql
    assert owner_call.kwargs == {
        "minimum_age_seconds": (
            importer.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_STAGE_CLEANUP_MIN_AGE_SECONDS
        ),
        "owner_history_limit": (
            importer.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_STAGE_OWNER_HISTORY_LIMIT
        ),
    }
    catalog_sql = str(catalog_call.args[0])
    assert "pg_catalog.pg_class" in catalog_sql
    assert "LIMIT :cleanup_limit" in catalog_sql
    assert catalog_call.kwargs == {
        "schema_name": "mrf",
        "stage_name_pattern": "provider\\_directory\\_address\\_overlay\\_stage\\_%",
        "cleanup_limit": importer.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_STAGE_CLEANUP_LIMIT,
    }
    status.assert_awaited_once_with(
        f'DROP TABLE IF EXISTS "mrf"."{stages_by_status["orphan"]}";'
    )
    assert stages_by_status["current"] not in str(status.await_args.args[0])
    assert invalid_stage not in str(status.await_args.args[0])


@pytest.mark.asyncio
async def test_address_overlay_orphan_cleanup_fails_closed_without_run_ownership(monkeypatch):
    catalog = AsyncMock(side_effect=RuntimeError("import run unavailable"))
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "all", catalog)
    monkeypatch.setattr(importer.db, "status", status)

    metrics = await importer._cleanup_orphan_address_overlay_stages(
        "mrf",
        current_stage_table=importer._address_overlay_stage_table_name("run_current"),
    )

    assert metrics["ownership_unavailable"] == 1
    assert metrics["removed"] == 0
    status.assert_not_awaited()


@pytest.mark.asyncio
async def test_address_overlay_live_ensure_does_not_build_scope_index(monkeypatch):
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)

    await importer._ensure_provider_directory_address_overlay_table("mrf")

    joined_sql = "\n".join(call.args[0] for call in status.await_args_list)
    assert "provider_directory_address_overlay_source_idx" in joined_sql
    assert "provider_directory_address_overlay_source_run_resource_idx" not in joined_sql
    assert "(source_id, last_seen_run_id, resource_type, resource_id)" not in joined_sql


@pytest.mark.asyncio
async def test_address_overlay_stage_builds_and_renames_scope_index(monkeypatch):
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)
    stage_table = importer._address_overlay_stage_table_name("run_1")
    stage_index = importer._address_overlay_index_name(
        stage_table, "source_run_resource_idx"
    )

    await importer._create_address_overlay_stage_indexes("mrf", stage_table)
    await importer._rename_address_overlay_stage_indexes("mrf", stage_table)

    joined_sql = "\n".join(call.args[0] for call in status.await_args_list)
    assert f'CREATE INDEX IF NOT EXISTS "{stage_index}"' in joined_sql
    assert "(source_id, last_seen_run_id, resource_type, resource_id)" in joined_sql
    assert (
        f'ALTER INDEX IF EXISTS "mrf"."{stage_index}" '
        'RENAME TO "provider_directory_address_overlay_source_run_resource_idx"'
    ) in joined_sql


@pytest.mark.asyncio
async def test_address_overlay_stage_dedupe_is_scoped_to_refreshed_sources(monkeypatch):
    status_mock = AsyncMock(return_value="DELETE 2")
    monkeypatch.setattr(importer.db, "status", status_mock)

    removed = await importer._dedupe_address_overlay_stage(
        '"mrf"."provider_directory_address_overlay_stage_test"',
        source_ids=["source_b", "source_a", "source_a", ""],
        resource_types=["PractitionerRole", "OrganizationAffiliation"],
    )

    assert removed == 2
    sql = status_mock.await_args.args[0]
    assert "FROM \"mrf\".\"provider_directory_address_overlay_stage_test\"" in sql
    assert "WHERE source_id = ANY(CAST(:dedupe_source_ids AS varchar[]))" in sql
    assert "resource_type = ANY(CAST(:dedupe_resource_types AS varchar[]))" in sql
    assert status_mock.await_args.kwargs == {
        "dedupe_source_ids": ["source_b", "source_a"],
        "dedupe_resource_types": ["PractitionerRole", "OrganizationAffiliation"],
    }


def test_network_catalog_table_sql_shape():
    sql = importer.provider_directory_network_catalog_table_sql("mrf")

    assert 'CREATE TABLE IF NOT EXISTS "mrf"."provider_directory_network_catalog"' in sql
    assert "provider_directory_network_key varchar NOT NULL" in sql
    assert "provider_directory_issuer_network_match_key varchar" in sql
    assert "source_resource_counts jsonb NOT NULL DEFAULT '{}'" in sql
    assert "PRIMARY KEY (source_id, network_resource_id)" in sql


def test_network_catalog_insert_sql_resolves_network_refs():
    sql = importer.provider_directory_network_catalog_insert_sql(
        "mrf",
        "provider_directory_network_catalog_stage_test",
        run_id="run_1",
        source_ids=["source_a"],
    )

    assert 'INSERT INTO "mrf"."provider_directory_network_catalog_stage_test"' in sql
    assert 'FROM "mrf"."provider_directory_insurance_plan" AS insurance_plan' in sql
    assert 'FROM "mrf"."provider_directory_practitioner_role" AS role' in sql
    assert 'FROM "mrf"."provider_directory_organization_affiliation" AS affiliation' in sql
    assert 'JOIN "mrf"."provider_directory_organization" AS network_org' in sql
    assert "substring(BTRIM(refs_raw.network_ref) FROM" in sql
    assert "(?:^|/)Organization/" in sql
    assert "(?:/_history/" in sql
    assert "provider_directory_issuer_network_match_key" in sql
    assert "insurance_plan.last_seen_run_id = CAST(:run_id AS varchar)" in sql
    assert "insurance_plan.source_id = ANY(CAST(:source_ids AS varchar[]))" in sql
    assert "role.last_seen_run_id = CAST(:run_id AS varchar)" in sql
    assert "affiliation.last_seen_run_id = CAST(:run_id AS varchar)" in sql


def test_network_catalog_stage_index_names_are_hash_safe():
    stage_table = importer._network_catalog_stage_table_name("run_1")
    index_name = importer._network_catalog_index_name(stage_table, "issuer_network_key_idx")

    assert len(index_name) <= 63
    assert index_name.startswith("provider_directory_network_catalog_stage_")
    assert index_name.endswith("_idx") or "_" in index_name


@pytest.mark.asyncio
async def test_network_catalog_publish_uses_staged_swap(monkeypatch):
    """A scoped network publication swaps its fully built stage atomically."""

    _stub_artifact_build_guard(monkeypatch)
    monkeypatch.setattr(importer, "_network_catalog_missing_requirement", AsyncMock(return_value=None))
    status_calls = _stub_staged_publish_db(
        monkeypatch,
        row_count=13,
        status_responses={
            "WHERE NOT (source_id = ANY": "INSERT 0 4",
            "WITH refs_raw AS MATERIALIZED": "INSERT 0 9",
        },
    )

    metrics = await importer.publish_provider_directory_network_catalog(
        "mrf",
        run_id="run_1",
        source_ids=["source_a"],
    )

    sql_calls = [sql for sql, _params in status_calls]
    joined_sql = "\n".join(sql_calls)
    stage_table = _artifact_stage_table(status_calls, importer.PROVIDER_DIRECTORY_NETWORK_CATALOG_STAGE_PREFIX)
    stage_source_network_idx = importer._network_catalog_index_name(stage_table, "source_network_idx")

    assert metrics == {
        "published": True,
        "rows": 13,
        "inserted": 9,
        "copied_existing": 4,
        "source_ids": ["source_a"],
        "relation": '"mrf"."provider_directory_network_catalog"',
    }
    assert 'CREATE TABLE IF NOT EXISTS "mrf"."provider_directory_network_catalog"' in joined_sql
    assert f'CREATE UNLOGGED TABLE "mrf"."{stage_table}"' in joined_sql
    assert f'CREATE UNIQUE INDEX IF NOT EXISTS "{stage_source_network_idx}"' in joined_sql
    assert 'FROM "mrf"."provider_directory_network_catalog"' in joined_sql
    assert "WHERE NOT (source_id = ANY(CAST(:source_ids AS varchar[])))" in joined_sql
    assert f'ALTER TABLE "mrf"."{stage_table}" RENAME TO "provider_directory_network_catalog"' in joined_sql
    assert (
        'ALTER TABLE "mrf"."provider_directory_network_catalog" '
        'RENAME TO "provider_directory_network_catalog_old"'
    ) in joined_sql
    assert (
        f'ALTER INDEX IF EXISTS "mrf"."{stage_source_network_idx}" '
        'RENAME TO "provider_directory_network_catalog_source_network_idx"'
    ) in joined_sql
    assert "entity_address_unified" not in joined_sql


@pytest.mark.asyncio
async def test_network_catalog_publish_skips_run_scope_without_sources(monkeypatch):
    monkeypatch.setattr(importer, "_network_catalog_missing_requirement", AsyncMock(return_value=None))
    ensure_catalog = AsyncMock()
    monkeypatch.setattr(importer, "_ensure_provider_directory_network_catalog_table", ensure_catalog)
    monkeypatch.setattr(importer, "_network_catalog_scope_sources", AsyncMock(return_value=[]))
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)

    metrics = await importer.publish_provider_directory_network_catalog("mrf", run_id="run_empty")

    assert metrics == {
        "skipped": True,
        "reason": "no_scoped_sources",
        "source_ids": [],
        "relation": '"mrf"."provider_directory_network_catalog"',
    }
    ensure_catalog.assert_awaited_once_with("mrf")
    status.assert_not_awaited()


def _assert_overlay_staged_swap_sql(status_calls):
    """Verify the overlay stage is indexed and atomically promoted."""
    sql_calls = [sql for sql, _params in status_calls]
    joined_sql = "\n".join(sql_calls)
    stage_table = _artifact_stage_table(
        status_calls,
        importer.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_STAGE_PREFIX,
    )
    stage_source_record_idx = importer._address_overlay_index_name(
        stage_table,
        "source_record_idx",
    )
    stage_npi_idx = importer._address_overlay_index_name(stage_table, "npi_idx")

    assert f'CREATE UNLOGGED TABLE "mrf"."{stage_table}"' in joined_sql
    assert f'CREATE UNIQUE INDEX IF NOT EXISTS "{stage_source_record_idx}"' in joined_sql
    assert f'CREATE INDEX IF NOT EXISTS "{stage_npi_idx}"' in joined_sql
    assert 'FROM "mrf"."provider_directory_address_overlay"' in joined_sql
    assert "WHERE NOT (source_id = ANY(CAST(:source_ids AS varchar[])))" in joined_sql
    assert "PARTITION BY source_record_id" in joined_sql
    assert "duplicate_rank > 1" in joined_sql
    assert 'FROM "mrf"."address_archive_v2" AS archive' in joined_sql
    assert f'ALTER TABLE "mrf"."{stage_table}" RENAME TO "provider_directory_address_overlay"' in joined_sql
    assert 'ALTER TABLE "mrf"."provider_directory_address_overlay" RENAME TO "provider_directory_address_overlay_old"' in joined_sql
    assert (
        f'ALTER INDEX IF EXISTS "mrf"."{stage_source_record_idx}" '
        'RENAME TO "provider_directory_address_overlay_source_record_idx"'
    ) in joined_sql
    assert "entity_address_unified" not in joined_sql


@pytest.mark.asyncio
async def test_overlay_publish_uses_staged_swap(monkeypatch):
    """Publish through an isolated stage before the atomic relation swap."""
    _stub_artifact_build_guard(monkeypatch)
    monkeypatch.setattr(importer, "_address_overlay_missing_requirement", AsyncMock(return_value=None))
    status_calls = _stub_staged_publish_db(
        monkeypatch,
        row_count=10,
        status_responses={
            "WHERE NOT (source_id = ANY": "INSERT 0 4",
            "provider_directory_fhir:organization_address:": "INSERT 0 6",
            "provider_directory_fhir:practitioner_address:": "INSERT 0 5",
            "provider_directory_fhir:practitioner_role:": "INSERT 0 7",
            "provider_directory_fhir:organization_affiliation:": "INSERT 0 8",
            "duplicate_rank > 1": "DELETE 2",
        },
    )

    metrics = await importer.publish_provider_directory_address_overlay(
        "mrf",
        run_id="run_1",
        source_ids=["source_a"],
    )

    assert metrics["published"] is True
    assert metrics["rows"] == 10
    assert metrics["inserted"] == 26
    assert metrics["inserted_by_component"] == {
        "organization_address": 6,
        "practitioner_address": 5,
        "practitioner_role": 7,
        "organization_affiliation": 8,
    }
    assert set(metrics["component_seconds"]) == {
        "organization_address",
        "practitioner_address",
        "practitioner_role",
        "organization_affiliation",
    }
    assert metrics["duplicates_removed"] == 2
    assert metrics["copied_existing"] == 4
    assert metrics["archive_coordinate_backfill_rows"] == 0
    assert metrics["orphan_stage_cleanup"]["scanned"] == 0
    assert metrics["source_ids"] == ["source_a"]
    _assert_overlay_staged_swap_sql(status_calls)


@pytest.mark.asyncio
async def test_overlay_publish_does_not_cleanup_when_build_guard_conflicts(monkeypatch):
    @contextlib.asynccontextmanager
    async def conflicting_guard(*_args, **_kwargs):
        raise importer.ProviderDirectoryArtifactCutoverConflict(
            importer.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE
        )
        yield

    cleanup = AsyncMock()
    monkeypatch.setattr(importer, "_provider_directory_artifact_build_guard", conflicting_guard)
    monkeypatch.setattr(importer, "_address_overlay_missing_requirement", AsyncMock(return_value=None))
    monkeypatch.setattr(importer, "_ensure_provider_directory_address_overlay_table", AsyncMock())
    monkeypatch.setattr(importer, "_address_overlay_scope_sources", AsyncMock(return_value=["source_a"]))
    monkeypatch.setattr(importer, "_cleanup_orphan_address_overlay_stages", cleanup)

    with pytest.raises(importer.ProviderDirectoryArtifactCutoverConflict):
        await importer.publish_provider_directory_address_overlay("mrf", run_id="run_1")

    cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_overlay_publish_can_refresh_practitioner_component_only(monkeypatch):
    _stub_artifact_build_guard(monkeypatch)
    monkeypatch.setattr(importer, "_address_overlay_missing_requirement", AsyncMock(return_value=None))
    status_calls = _stub_staged_publish_db(
        monkeypatch,
        row_count=18,
        status_responses={
            "refresh_resource_types": "INSERT 0 11",
            "provider_directory_fhir:practitioner_role:": "INSERT 0 7",
            "duplicate_rank > 1": "DELETE 0",
        },
    )

    metrics = await importer.publish_provider_directory_address_overlay(
        "mrf",
        source_ids=["source_a"],
        components=["practitioner_role"],
    )

    joined_sql = "\n".join(sql for sql, _params in status_calls)
    copy_params = next(params for sql, params in status_calls if "refresh_resource_types" in sql)

    assert metrics["published"] is True
    assert metrics["components"] == ["practitioner_role"]
    assert metrics["copied_existing"] == 11
    assert metrics["inserted"] == 7
    assert metrics["inserted_by_component"] == {"practitioner_role": 7}
    assert set(metrics["component_seconds"]) == {"practitioner_role"}
    assert copy_params["refresh_resource_types"] == ["PractitionerRole"]
    assert "provider_directory_fhir:organization_address:" not in joined_sql
    assert "provider_directory_fhir:organization_affiliation:" not in joined_sql
    assert "resource_type = ANY(CAST(:refresh_resource_types AS varchar[]))" in joined_sql


@pytest.mark.asyncio
async def test_overlay_publish_skips_run_scope_without_sources(monkeypatch):
    monkeypatch.setattr(importer, "_address_overlay_missing_requirement", AsyncMock(return_value=None))
    ensure_overlay = AsyncMock()
    monkeypatch.setattr(importer, "_ensure_provider_directory_address_overlay_table", ensure_overlay)
    monkeypatch.setattr(importer, "_address_overlay_scope_sources", AsyncMock(return_value=[]))
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)

    metrics = await importer.publish_provider_directory_address_overlay("mrf", run_id="run_empty")

    assert metrics == {
        "skipped": True,
        "reason": "no_scoped_sources",
        "source_ids": [],
        "relation": '"mrf"."provider_directory_address_overlay"',
    }
    ensure_overlay.assert_awaited_once_with("mrf")
    status.assert_not_awaited()


def _last_updated_partition_test_source(**resource_config_overrides):
    """Build a probe-only source with one partition-enabled resource."""
    resource_config_by_field = {
        "start": "2024-01-01T00:00:00Z",
        "end": "2024-01-03T00:00:00Z",
        "ceiling": 10,
        "minimum_width_seconds": 3600,
        "page_count": 10,
        "volatile_metadata_paths": ["/meta/lastUpdated"],
        **resource_config_overrides,
    }
    return {
        "source_id": "source_scan_partition_test",
        "endpoint_id": "endpoint_scan_partition_test",
        "api_base": importer.SCAN_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.SCAN_PROVIDER_DIRECTORY_BASE,
        "last_validated_status": "valid",
        "metadata_json": {
            "provider_directory_coverage_mode": "probe_only",
            "provider_directory_fully_enumerable_resources": [],
            importer.LAST_UPDATED_PARTITION_METADATA_KEY: {
                "enabled": True,
                "resources": {"Practitioner": resource_config_by_field},
            },
        },
    }


def _last_updated_partition_test_context():
    """Build the durable checkpoint identity shared by partition tests."""
    return importer.PaginationCheckpointContext(
        canonical_api_base=importer.SCAN_PROVIDER_DIRECTORY_BASE,
        source_scope_hash="scan-partition-scope",
        source_ids=("source_scan_partition_test",),
        owner_run_id="run_partition",
        acquisition_root_run_id="run_partition",
        dataset_id="dataset_partition",
    )


def _last_updated_partition_test_resource(
    resource_id: str,
    *,
    family: str = "Stable",
    last_updated: str = "2024-01-01T01:00:00Z",
):
    """Build one Practitioner whose volatile timestamp can vary by pass."""
    return {
        "resourceType": "Practitioner",
        "id": resource_id,
        "meta": {"lastUpdated": last_updated},
        "name": [{"family": family}],
    }


def _last_updated_partition_test_bundle(resources, *, total=None):
    """Wrap partition test resources in a searchset Bundle."""
    payload = {
        "resourceType": "Bundle",
        "type": "searchset",
        "entry": [{"resource": resource} for resource in resources],
    }
    if total is not None:
        payload["total"] = total
    return payload


def _last_updated_partition_expected_proof(resource_count, cutoff):
    return {
        "strategy_version": importer.LAST_UPDATED_PARTITION_STRATEGY_VERSION,
        "cutoff": cutoff,
        "verified": True,
        "failure": None,
        **{
            field_name: resource_count
            for field_name in (
                "unfiltered_pre",
                "ranged_root_pre",
                "exact_leaf_count_sum",
                "pass1_unique",
                "pass2_unique",
                "staged_candidate_count",
                "ranged_root_post",
                "unfiltered_post",
            )
        },
    }


def _new_last_updated_partition_resume(partition_config):
    """Build the initial durable state for a partition acquisition."""
    return importer.LastUpdatedPartitionResume(
        importer.PartitionPlan.create(
            partition_config.start,
            partition_config.end,
            ceiling=partition_config.ceiling,
            minimum_width=partition_config.minimum_width,
            volatile_metadata_paths=partition_config.volatile_metadata_paths,
            boundary_precision=partition_config.boundary_precision,
        )
    )


def _last_updated_partition_test_window(*, count=1):
    return importer.TimeWindow(
        "root",
        datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
        datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC),
        count=count,
    )


def test_last_updated_partition_count_url_requires_accurate_total():
    request_url = importer._last_updated_partition_url(
        "https://example.test/fhir/Practitioner?_total=estimate&_count=5",
        _last_updated_partition_test_window(),
        page_count=10,
        count_only=True,
    )
    query_lookup = urllib.parse.parse_qs(
        urllib.parse.urlsplit(request_url).query
    )

    assert query_lookup["_summary"] == ["count"]
    assert query_lookup["_total"] == ["accurate"]
    assert "_count" not in query_lookup


@pytest.mark.asyncio
async def test_last_updated_partition_rejects_non_searchset_bundles(monkeypatch):
    directory_source = _last_updated_partition_test_source()
    non_searchset_bundle_by_field = {
        "resourceType": "Bundle",
        "type": "collection",
        "total": 1,
        "entry": [],
    }
    monkeypatch.setattr(
        importer,
        "_fetch_source_json",
        AsyncMock(return_value=(200, non_searchset_bundle_by_field, None, 1)),
    )

    count_fetch = await importer._fetch_last_updated_partition_count(
        directory_source,
        "https://example.test/fhir/Practitioner?_summary=count",
        timeout=3,
    )
    page_fetch = await importer._fetch_last_updated_window_page(
        directory_source,
        "Practitioner",
        "https://example.test/fhir/Practitioner",
        _last_updated_partition_test_window(),
        timeout=3,
    )

    assert count_fetch.observation is not None
    assert count_fetch.observation.kind.value == "unknown"
    assert count_fetch.error == "non_searchset_count_bundle"
    assert page_fetch.error == "non_searchset_bundle"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("unexpected_field", "expected_error"),
    [
        (
            {
                "entry": [
                    {
                        "resource": _last_updated_partition_test_resource(
                            "prac-1"
                        )
                    }
                ]
            },
            "count_bundle_contains_entries",
        ),
        (
            {
                "link": [
                    {
                        "relation": "next",
                        "url": "https://example.test/fhir/Practitioner?page=2",
                    }
                ]
            },
            "count_bundle_has_next_link",
        ),
    ],
)
async def test_last_updated_partition_rejects_paginated_count_bundles(
    monkeypatch,
    unexpected_field,
    expected_error,
):
    directory_source = _last_updated_partition_test_source()
    count_bundle_dict = {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": 1,
        **unexpected_field,
    }
    monkeypatch.setattr(
        importer,
        "_fetch_source_json",
        AsyncMock(return_value=(200, count_bundle_dict, None, 1)),
    )

    count_fetch = await importer._fetch_last_updated_partition_count(
        directory_source,
        "https://example.test/fhir/Practitioner?_summary=count",
        timeout=3,
    )

    assert count_fetch.observation is not None
    assert count_fetch.observation.kind.value == "unknown"
    assert count_fetch.error == expected_error


@pytest.mark.parametrize(
    ("last_updated", "expected_error"),
    [
        ("2024-01-01T00:00:00Z", None),
        ("2024-01-01T23:59:59Z", None),
        ("2024-01-02T00:00:00Z", "resource_last_updated_outside_window"),
        ("2023-12-31T23:59:59Z", "resource_last_updated_outside_window"),
        ("not-an-instant", "resource_last_updated_invalid"),
        (None, "resource_last_updated_invalid"),
    ],
)
def test_last_updated_partition_validates_resource_window(
    last_updated,
    expected_error,
):
    resource = _last_updated_partition_test_resource("prac-1")
    resource["meta"]["lastUpdated"] = last_updated

    assert (
        importer._last_updated_partition_resource_window_error(
            resource,
            _last_updated_partition_test_window(),
        )
        == expected_error
    )


def test_scan_second_precision_aligns_live_shaped_fractional_split():
    """Keep SCAN query bounds aligned while rejecting any truly earlier row."""
    directory_source = _last_updated_partition_test_source(
        start="2025-02-10T08:00:24Z",
        end="2025-02-10T08:00:27.247716Z",
        minimum_width_seconds=1,
        boundary_precision_seconds=1,
    )
    partition_config, config_error = importer._last_updated_partition_config(
        directory_source,
        "Practitioner",
    )
    assert config_error is None and partition_config is not None

    plan = importer._new_last_updated_partition_plan(partition_config)
    plan.observe_count("root", importer.CountObservation.exact(11))
    right_window = plan.windows["root.1"]
    returned_resource = _last_updated_partition_test_resource(
        "scan-practitioner",
        last_updated="2025-02-10T08:00:26Z",
    )
    request_url = importer._last_updated_partition_url(
        f"{importer.SCAN_PROVIDER_DIRECTORY_BASE}/Practitioner",
        right_window,
        page_count=100,
        count_only=False,
    )

    assert plan.windows["root"].end == importer.parse_utc_instant(
        "2025-02-10T08:00:28Z"
    )
    assert right_window.start == importer.parse_utc_instant(
        "2025-02-10T08:00:26Z"
    )
    assert urllib.parse.parse_qs(urllib.parse.urlsplit(request_url).query)[
        "_lastUpdated"
    ] == [
        "ge2025-02-10T08:00:26.000000Z",
        "lt2025-02-10T08:00:28.000000Z",
    ]
    assert (
        importer._last_updated_partition_resource_window_error(
            returned_resource,
            right_window,
        )
        is None
    )
    returned_resource["meta"]["lastUpdated"] = "2025-02-10T08:00:25Z"
    assert (
        importer._last_updated_partition_resource_window_error(
            returned_resource,
            right_window,
        )
        == "resource_last_updated_outside_window"
    )


@pytest.mark.parametrize("resource", [{}, {"meta": {}}])
def test_last_updated_partition_rejects_rows_missing_last_updated(resource):
    assert importer._last_updated_partition_resource_window_error(
        resource,
        _last_updated_partition_test_window(),
    ) in {"resource_meta_missing", "resource_last_updated_invalid"}


@pytest.mark.asyncio
async def test_last_updated_partition_rejects_rows_beyond_exact_count(monkeypatch):
    directory_source = _last_updated_partition_test_source()
    monkeypatch.setattr(
        importer,
        "_fetch_source_json",
        AsyncMock(
            return_value=(
                200,
                _last_updated_partition_test_bundle(
                    [
                        _last_updated_partition_test_resource("prac-1"),
                        _last_updated_partition_test_resource("prac-2"),
                    ]
                ),
                None,
                1,
            )
        ),
    )

    window_fetch = await importer._fetch_last_updated_partition_window(
        directory_source,
        "Practitioner",
        "https://example.test/fhir/Practitioner",
        _last_updated_partition_test_window(count=1),
        timeout=3,
        cancel_ctx=None,
        cancel_task=None,
        deadline_at=None,
    )

    assert window_fetch.complete is False
    assert window_fetch.bounded is True
    assert window_fetch.error == "window_resource_ceiling_reached"
    assert window_fetch.resources == ()


@pytest.mark.asyncio
async def test_last_updated_partition_bounds_unique_next_pages(monkeypatch):
    directory_source = _last_updated_partition_test_source()
    requested_urls = []

    async def fetch_page(_source, request_url, *, timeout):
        requested_urls.append(request_url)
        resource_number = len(requested_urls)
        payload = _last_updated_partition_test_bundle(
            [
                _last_updated_partition_test_resource(
                    f"prac-{resource_number}"
                )
            ]
        )
        payload["link"] = [
            {
                "relation": "next",
                "url": (
                    f"{importer.SCAN_PROVIDER_DIRECTORY_BASE}/Practitioner"
                    f"?page={resource_number + 1}"
                ),
            }
        ]
        return 200, payload, None, 1

    monkeypatch.setattr(importer, "_fetch_source_json", fetch_page)
    monkeypatch.setattr(importer, "_max_page_count", lambda: 2)

    window_fetch = await importer._fetch_last_updated_partition_window(
        directory_source,
        "Practitioner",
        f"{importer.SCAN_PROVIDER_DIRECTORY_BASE}/Practitioner?page=1",
        _last_updated_partition_test_window(count=3),
        timeout=3,
        cancel_ctx=None,
        cancel_task=None,
        deadline_at=None,
    )

    assert len(requested_urls) == 2
    assert window_fetch.complete is False
    assert window_fetch.bounded is True
    assert window_fetch.error == "window_page_limit_reached"
    assert len(window_fetch.resources) == 2


class _LastUpdatedPartitionFetchHarness:
    """Capture staged rows and proof records from one partition fetch."""

    def __init__(self, directory_source, fingerprints_by_window_and_pass=None):
        self.directory_source = directory_source
        self.staged_resources_by_id = {}
        self.written_resource_ids = []
        self.fingerprints_by_window_and_pass = (
            fingerprints_by_window_and_pass
            if fingerprints_by_window_and_pass is not None
            else {}
        )

    async def _stage_window(
        self,
        _context,
        _source,
        _resource_type,
        _model,
        resources,
        **_kwargs,
    ):
        candidate_hashes_by_id = {
            resource["id"]: f"candidate-hash:{resource['id']}"
            for resource in resources
        }
        return importer.LastUpdatedPartitionStage(
            rows=tuple(
                {
                    "dataset_id": "dataset_partition",
                    "resource_type": "Practitioner",
                    "resource_id": resource["id"],
                    "payload_hash": candidate_hashes_by_id[resource["id"]],
                    "payload_json": resource,
                }
                for resource in resources
            ),
            candidate_hashes_by_id=candidate_hashes_by_id,
        )

    async def _stage_rows(self, _connection, staged_rows):
        self.staged_resources_by_id.update(
            {
                staged_row["resource_id"]: staged_row["payload_json"]
                for staged_row in staged_rows
            }
        )

    async def _stream_staged(
        self,
        _context,
        _source,
        _resource_type,
        _model,
        partition_plan,
        *,
        row_batch_handler,
        **_kwargs,
    ):
        expected_resource_ids = {
            resource_id
            for partition_window in partition_plan.leaf_windows()
            for resource_id in self.fingerprints_by_window_and_pass[
                (partition_window.window_id, 2)
            ]
        }
        assert set(self.staged_resources_by_id) == expected_resource_ids
        output_rows = [
            {
                "source_id": self.directory_source["source_id"],
                "resource_id": resource_id,
            }
            for resource_id in sorted(self.staged_resources_by_id)
        ]
        written_count = await row_batch_handler(
            ProviderDirectoryPractitioner,
            output_rows,
        )
        return len(output_rows), written_count

    async def _proof_counts(
        self,
        _context,
        _resource_type,
        partition_plan,
    ):
        expected_count = sum(
            partition_window.count or 0
            for partition_window in partition_plan.leaf_windows()
        )
        return importer.LastUpdatedPartitionProofCounts(
            leaf_count_sum=expected_count,
            pass1_unique=expected_count,
            pass2_unique=expected_count,
            staged_candidate_count=len(self.staged_resources_by_id),
            invalid_candidate_count=0,
            orphan_proof_count=0,
        )

    async def _write_rows(self, _model, output_rows):
        self.written_resource_ids.extend(
            output_row["resource_id"] for output_row in output_rows
        )
        return len(output_rows)

    async def _load_fingerprints(
        self,
        _context,
        _resource_type,
        window_id,
        pass_number,
    ):
        return dict(
            self.fingerprints_by_window_and_pass.get(
                (window_id, pass_number),
                {},
            )
        )

    async def _verify_and_store(
        self,
        _context,
        _resource_type,
        partition_window,
        pass_number,
        candidate_hashes_by_id=None,
        **_kwargs,
    ):
        fingerprints_by_id = dict(
            partition_window.passes[pass_number].resource_fingerprints
        )
        if pass_number == 2:
            assert self.fingerprints_by_window_and_pass[
                (partition_window.window_id, 1)
            ] == fingerprints_by_id
        self.fingerprints_by_window_and_pass[
            (partition_window.window_id, pass_number)
        ] = fingerprints_by_id
        if pass_number == 2:
            assert set(candidate_hashes_by_id or {}) == set(fingerprints_by_id)

    def _install_importer_mocks(
        self,
        monkeypatch,
        fetch_source_json,
        partition_resume,
        save_plan_callback,
    ):
        monkeypatch.setattr(
            importer,
            "_load_partition_plan",
            AsyncMock(return_value=partition_resume),
        )
        monkeypatch.setattr(importer, "_fetch_source_json", fetch_source_json)
        monkeypatch.setattr(
            importer,
            "_save_last_updated_partition_plan",
            save_plan_callback or AsyncMock(),
        )
        monkeypatch.setattr(
            importer,
            "_stage_last_updated_partition_window",
            self._stage_window,
        )
        monkeypatch.setattr(
            importer,
            "_load_last_updated_partition_window_fingerprints",
            self._load_fingerprints,
        )
        monkeypatch.setattr(
            importer,
            "_store_partition_pass_proof",
            self._verify_and_store,
        )
        monkeypatch.setattr(
            importer,
            "_upsert_dataset_resource_rows_on_connection",
            self._stage_rows,
        )
        @contextlib.asynccontextmanager
        async def partition_transaction():
            yield object()

        monkeypatch.setattr(
            importer.db,
            "acquire",
            partition_transaction,
        )
        monkeypatch.setattr(
            importer,
            "_stream_last_updated_partition_staged_rows",
            self._stream_staged,
        )
        monkeypatch.setattr(
            importer,
            "_assert_last_updated_partition_candidate_proof",
            self._proof_counts,
        )

    async def _fetch_resource_rows(
        self,
        partition_config,
        *,
        deadline_seconds=0,
    ):
        fetch_options = importer.LastUpdatedPartitionFetchOptions(
            per_resource_limit=0,
            page_limit=0,
            timeout=3,
            run_id="run_partition",
            row_batch_handler=self._write_rows,
            row_batch_size=2,
            retain_rows=False,
            cancel_ctx=None,
            cancel_task=None,
            deadline_seconds=deadline_seconds,
            pagination_checkpoint=_last_updated_partition_test_context(),
        )
        return await importer._fetch_last_updated_partition_resource_rows(
            self.directory_source,
            "Practitioner",
            ProviderDirectoryPractitioner,
            partition_config,
            fetch_options,
        )


async def _run_last_updated_partition_test_fetch(
    monkeypatch,
    directory_source,
    fetch_source_json,
    *,
    partition_resume=None,
    save_plan_callback=None,
    fingerprints_by_window_and_pass=None,
    deadline_seconds=0,
):
    """Exercise the production partition path with deterministic test stores."""
    partition_config, config_error = importer._last_updated_partition_config(
        directory_source,
        "Practitioner",
    )
    assert config_error is None
    assert partition_config is not None
    durable_resume = partition_resume or _new_last_updated_partition_resume(
        partition_config
    )
    fetch_harness = _LastUpdatedPartitionFetchHarness(
        directory_source,
        fingerprints_by_window_and_pass,
    )
    fetch_harness._install_importer_mocks(
        monkeypatch,
        fetch_source_json,
        durable_resume,
        save_plan_callback,
    )
    fetch_outcome = await fetch_harness._fetch_resource_rows(
        partition_config,
        deadline_seconds=deadline_seconds,
    )
    return (
        fetch_outcome,
        fetch_harness.staged_resources_by_id,
        fetch_harness.written_resource_ids,
    )


@pytest.mark.asyncio
async def test_last_updated_partition_transient_count_remains_resumable(monkeypatch):
    directory_source = _last_updated_partition_test_source()
    saved_plan_statuses = []

    async def fetch_transient(_source, _request_url, *, timeout):
        return 503, {"resourceType": "OperationOutcome"}, None, 1

    async def save_plan(
        _context,
        _resource_type,
        _partition_config,
        partition_plan,
        **_kwargs,
    ):
        saved_plan_statuses.append(partition_plan.status.value)

    fetch_outcome, staged_resources_by_id, written_resource_ids = (
        await _run_last_updated_partition_test_fetch(
            monkeypatch,
            directory_source,
            fetch_transient,
            save_plan_callback=save_plan,
        )
    )

    assert fetch_outcome.complete is False
    assert fetch_outcome.next_url_remaining is True
    assert importer.LAST_UPDATED_PARTITION_RETRYABLE_ERROR in fetch_outcome.error
    assert fetch_outcome.retry_not_before is not None
    assert saved_plan_statuses == ["counting"]
    assert staged_resources_by_id == {}
    assert written_resource_ids == []


class _CensusSequenceResponder:
    def __init__(self, counts):
        self.counts = iter(counts)
        self.requested_urls = []

    async def _fetch_source_json(self, _source, request_url, *, timeout):
        self.requested_urls.append(request_url)
        if "_summary=count" in request_url:
            return (
                200,
                _last_updated_partition_test_bundle([], total=next(self.counts)),
                None,
                1,
            )
        return (
            200,
            _last_updated_partition_test_bundle(
                [_last_updated_partition_test_resource("prac-1")]
            ),
            None,
            1,
        )


@pytest.mark.asyncio
async def test_last_updated_partition_blocks_pre_census_mismatch(monkeypatch):
    directory_source = _last_updated_partition_test_source()
    responder = _CensusSequenceResponder([2, 1])

    fetch_outcome, staged_resources_by_id, written_resource_ids = (
        await _run_last_updated_partition_test_fetch(
            monkeypatch,
            directory_source,
            responder._fetch_source_json,
        )
    )

    assert fetch_outcome.complete is False
    assert fetch_outcome.next_url_remaining is False
    assert "census_ranged_root_pre_mismatch:2!=1" in fetch_outcome.error
    assert fetch_outcome.fetch_diagnostic["unfiltered_pre"] == 2
    assert fetch_outcome.fetch_diagnostic["ranged_root_pre"] == 1
    assert staged_resources_by_id == {}
    assert written_resource_ids == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("counts", "failure_name"),
    [
        ([1, 1, 2], "census_ranged_root_post_mismatch"),
        ([1, 1, 1, 2], "census_unfiltered_post_mismatch"),
    ],
)
async def test_last_updated_partition_blocks_post_census_drift(
    monkeypatch,
    counts,
    failure_name,
):
    directory_source = _last_updated_partition_test_source()
    responder = _CensusSequenceResponder(counts)

    fetch_outcome, staged_resources_by_id, written_resource_ids = (
        await _run_last_updated_partition_test_fetch(
            monkeypatch,
            directory_source,
            responder._fetch_source_json,
        )
    )

    assert fetch_outcome.complete is False
    assert fetch_outcome.next_url_remaining is False
    assert failure_name in fetch_outcome.error
    assert set(staged_resources_by_id) == {"prac-1"}
    assert written_resource_ids == []


@pytest.mark.asyncio
async def test_last_updated_partition_transient_page_remains_resumable(monkeypatch):
    directory_source = _last_updated_partition_test_source()
    saved_plan_statuses = []

    async def fetch_then_fail(_source, request_url, *, timeout):
        if "_summary=count" in request_url:
            return (
                200,
                _last_updated_partition_test_bundle([], total=1),
                None,
                1,
            )
        return 503, {"resourceType": "OperationOutcome"}, None, 1

    async def save_plan(
        _context,
        _resource_type,
        _partition_config,
        partition_plan,
        **_kwargs,
    ):
        saved_plan_statuses.append(partition_plan.status.value)

    fetch_outcome, staged_resources_by_id, written_resource_ids = (
        await _run_last_updated_partition_test_fetch(
            monkeypatch,
            directory_source,
            fetch_then_fail,
            save_plan_callback=save_plan,
        )
    )

    assert fetch_outcome.complete is False
    assert fetch_outcome.next_url_remaining is True
    assert importer.LAST_UPDATED_PARTITION_RETRYABLE_ERROR in fetch_outcome.error
    assert fetch_outcome.retry_not_before is not None
    assert saved_plan_statuses == [
        "counting",
        "counting",
        "acquiring",
        "acquiring",
    ]
    assert staged_resources_by_id == {}
    assert written_resource_ids == []


@pytest.mark.asyncio
async def test_last_updated_partition_deadline_remains_resumable(monkeypatch):
    directory_source = _last_updated_partition_test_source()
    fetch_source_json = AsyncMock()
    saved_plan_statuses = []

    async def save_plan(
        _context,
        _resource_type,
        _partition_config,
        partition_plan,
        **_kwargs,
    ):
        saved_plan_statuses.append(partition_plan.status.value)

    monotonic_clock = Mock(side_effect=[100.0, 102.0])
    monkeypatch.setattr(
        importer,
        "_last_updated_partition_monotonic",
        monotonic_clock,
    )

    fetch_outcome, staged_resources_by_id, written_resource_ids = (
        await _run_last_updated_partition_test_fetch(
            monkeypatch,
            directory_source,
            fetch_source_json,
            save_plan_callback=save_plan,
            deadline_seconds=1,
        )
    )

    assert fetch_outcome.complete is False
    assert fetch_outcome.deadline_reached is True
    assert fetch_outcome.next_url_remaining is True
    assert importer.LAST_UPDATED_PARTITION_RETRYABLE_ERROR in fetch_outcome.error
    assert saved_plan_statuses == ["counting"]
    assert staged_resources_by_id == {}
    assert written_resource_ids == []
    fetch_source_json.assert_not_awaited()


class _CensusResumeRecorder:
    def __init__(self):
        self.persisted_resume = None

    async def _save(
        self,
        _context,
        _resource_type,
        partition_config,
        partition_plan,
        **checkpoint_fields,
    ):
        census = checkpoint_fields["census"]
        checkpoint_payload = importer._last_updated_partition_checkpoint_payload(
            partition_config,
            partition_plan,
            census,
        )
        restored_resume = importer._last_updated_partition_resume_from_checkpoint(
            checkpoint_payload,
            partition_config,
        )
        self.persisted_resume = dataclasses.replace(
            restored_resume,
            pages_processed=checkpoint_fields["pages_processed"],
            rows_processed=checkpoint_fields["rows_processed"],
        )


class _RepeatedPartitionNextResponder:
    """Return one partial page whose next link never advances."""

    def __init__(self):
        self.requested_urls = []

    async def _fetch_source_json(self, _source, request_url, *, timeout):
        self.requested_urls.append(request_url)
        if "_summary=count" in request_url:
            return (
                200,
                _last_updated_partition_test_bundle([], total=2),
                None,
                1,
            )
        payload = _last_updated_partition_test_bundle(
            [_last_updated_partition_test_resource("prac-1")]
        )
        payload["link"] = [{"relation": "next", "url": request_url}]
        return 200, payload, None, 1


class _CleanPartitionRetryResponder:
    """Complete both partition passes and their post-acquisition census."""

    def __init__(self):
        self.requested_urls = []

    async def _fetch_source_json(self, _source, request_url, *, timeout):
        self.requested_urls.append(request_url)
        if "_summary=count" in request_url:
            return (
                200,
                _last_updated_partition_test_bundle([], total=2),
                None,
                1,
            )
        return (
            200,
            _last_updated_partition_test_bundle(
                [
                    _last_updated_partition_test_resource("prac-1"),
                    _last_updated_partition_test_resource("prac-2"),
                ]
            ),
            None,
            1,
        )


async def _run_repeated_next_partition_retry(
    monkeypatch,
    fingerprints_by_window_and_pass,
):
    directory_source = _last_updated_partition_test_source()
    responder = _RepeatedPartitionNextResponder()
    resume_recorder = _CensusResumeRecorder()
    fetch_outcome, staged_resources_by_id, written_resource_ids = (
        await _run_last_updated_partition_test_fetch(
            monkeypatch,
            directory_source,
            responder._fetch_source_json,
            save_plan_callback=resume_recorder._save,
            fingerprints_by_window_and_pass=fingerprints_by_window_and_pass,
        )
    )
    return (
        directory_source,
        responder,
        resume_recorder,
        fetch_outcome,
        staged_resources_by_id,
        written_resource_ids,
    )


@pytest.mark.asyncio
async def test_last_updated_partition_repeated_next_url_remains_retryable(
    monkeypatch,
):
    fingerprints_by_window_and_pass = {}
    (
        _directory_source,
        responder,
        resume_recorder,
        fetch_outcome,
        staged_resources_by_id,
        written_resource_ids,
    ) = await _run_repeated_next_partition_retry(
        monkeypatch,
        fingerprints_by_window_and_pass,
    )

    assert fetch_outcome.complete is False
    assert fetch_outcome.next_url_remaining is True
    assert fetch_outcome.hard_page_limit_reached is False
    assert fetch_outcome.error == (
        f"{importer.LAST_UPDATED_PARTITION_RETRYABLE_ERROR}:"
        "pagination_loop_detected"
    )
    assert fetch_outcome.pages_fetched == 3
    assert fetch_outcome.rows_fetched == 1
    assert len(responder.requested_urls) == 3
    assert resume_recorder.persisted_resume is not None
    assert resume_recorder.persisted_resume.plan.status is importer.PlanStatus.ACQUIRING
    assert resume_recorder.persisted_resume.plan.failure is None
    assert resume_recorder.persisted_resume.plan.windows["root"].passes == {}
    assert resume_recorder.persisted_resume.pages_processed == 3
    assert resume_recorder.persisted_resume.rows_processed == 1
    assert fingerprints_by_window_and_pass == {}
    assert staged_resources_by_id == {}
    assert written_resource_ids == []


@pytest.mark.asyncio
async def test_last_updated_partition_repeated_next_retry_resumes_cleanly(
    monkeypatch,
):
    fingerprints_by_window_and_pass = {}
    (
        directory_source,
        _responder,
        resume_recorder,
        first_outcome,
        _staged,
        _written,
    ) = await _run_repeated_next_partition_retry(
        monkeypatch,
        fingerprints_by_window_and_pass,
    )
    assert importer.LAST_UPDATED_PARTITION_RETRYABLE_ERROR in first_outcome.error
    assert resume_recorder.persisted_resume is not None

    clean_responder = _CleanPartitionRetryResponder()
    fetch_outcome, staged_resources_by_id, written_resource_ids = (
        await _run_last_updated_partition_test_fetch(
            monkeypatch,
            directory_source,
            clean_responder._fetch_source_json,
            partition_resume=resume_recorder.persisted_resume,
            fingerprints_by_window_and_pass=fingerprints_by_window_and_pass,
        )
    )

    assert fetch_outcome.complete is True
    assert fetch_outcome.fetch_diagnostic == (
        _last_updated_partition_expected_proof(
            2,
            "2024-01-03T00:00:00.000000Z",
        )
    )
    assert set(staged_resources_by_id) == {"prac-1", "prac-2"}
    assert written_resource_ids == ["prac-1", "prac-2"]
    assert set(fingerprints_by_window_and_pass) == {
        ("root", 1),
        ("root", 2),
    }
    assert len(clean_responder.requested_urls) == 4


@pytest.mark.asyncio
async def test_last_updated_partition_bounded_ceiling_remains_terminal(
    monkeypatch,
):
    directory_source = _last_updated_partition_test_source()
    resume_recorder = _CensusResumeRecorder()

    async def fetch_beyond_ceiling(_source, request_url, *, timeout):
        if "_summary=count" in request_url:
            return (
                200,
                _last_updated_partition_test_bundle([], total=1),
                None,
                1,
            )
        return (
            200,
            _last_updated_partition_test_bundle(
                [
                    _last_updated_partition_test_resource("prac-1"),
                    _last_updated_partition_test_resource("prac-2"),
                ]
            ),
            None,
            1,
        )

    fetch_outcome, staged_resources_by_id, written_resource_ids = (
        await _run_last_updated_partition_test_fetch(
            monkeypatch,
            directory_source,
            fetch_beyond_ceiling,
            save_plan_callback=resume_recorder._save,
        )
    )

    assert fetch_outcome.complete is False
    assert fetch_outcome.next_url_remaining is False
    assert fetch_outcome.hard_page_limit_reached is True
    assert "bounded_window" in fetch_outcome.error
    assert resume_recorder.persisted_resume is not None
    assert resume_recorder.persisted_resume.plan.status is importer.PlanStatus.FAILED
    assert resume_recorder.persisted_resume.plan.failure is not None
    assert resume_recorder.persisted_resume.plan.failure.code == "bounded_window"
    assert staged_resources_by_id == {}
    assert written_resource_ids == []


class _PartitionCheckpointPayloadRecorder:
    def __init__(self):
        self.payloads = []

    async def _save(
        self,
        _context,
        _resource_type,
        partition_config,
        partition_plan,
        **checkpoint_fields,
    ):
        checkpoint_payload = importer._last_updated_partition_checkpoint_payload(
            partition_config,
            partition_plan,
            checkpoint_fields["census"],
        )
        self.payloads.append(json.loads(checkpoint_payload))


def _partition_retry_state_with_completed_window():
    directory_source = _last_updated_partition_test_source(
        ceiling=1,
        page_count=1,
    )
    partition_config, config_error = importer._last_updated_partition_config(
        directory_source,
        "Practitioner",
    )
    assert config_error is None and partition_config is not None
    partition_plan = _new_last_updated_partition_resume(partition_config).plan
    partition_plan.observe_count("root", importer.CountObservation.exact(2))
    partition_plan.observe_count("root.0", importer.CountObservation.exact(1))
    partition_plan.observe_count("root.1", importer.CountObservation.exact(1))
    completed_resource = _last_updated_partition_test_resource("prac-left")
    partition_plan.record_pass("root.0", 1, [completed_resource], complete=True)
    partition_plan.record_pass("root.0", 2, [completed_resource], complete=True)
    state = importer.LastUpdatedPartitionState(
        context=_last_updated_partition_test_context(),
        plan=partition_plan,
        census=importer.LastUpdatedCompletenessCensus(
            unfiltered_pre=2,
            ranged_root_pre=2,
        ),
        start_url=f"{importer.SCAN_PROVIDER_DIRECTORY_BASE}/Practitioner",
        pages_fetched=0,
        rows_fetched=0,
        deadline_at=None,
    )
    fetch_options = importer.LastUpdatedPartitionFetchOptions(
        per_resource_limit=0,
        page_limit=0,
        timeout=3,
        run_id="run_partition",
        row_batch_handler=AsyncMock(return_value=0),
        row_batch_size=2,
        retain_rows=False,
        cancel_ctx=None,
        cancel_task=None,
        deadline_seconds=0,
        pagination_checkpoint=_last_updated_partition_test_context(),
    )
    return directory_source, partition_config, partition_plan, state, fetch_options


@pytest.mark.asyncio
async def test_partition_retry_preserves_prior_completed_window_markers(
    monkeypatch,
):
    (
        directory_source,
        partition_config,
        partition_plan,
        state,
        fetch_options,
    ) = _partition_retry_state_with_completed_window()
    retry_fetch = importer.LastUpdatedWindowFetch(
        resources=(
            _last_updated_partition_test_resource(
                "prac-right",
                last_updated="2024-01-02T01:00:00Z",
            ),
        ),
        pages_fetched=1,
        complete=False,
        error="pagination_loop_detected",
    )
    monkeypatch.setattr(
        importer,
        "_fetch_partition_window_for_pass",
        AsyncMock(return_value=("https://example.test/repeated", retry_fetch)),
    )
    checkpoint_recorder = _PartitionCheckpointPayloadRecorder()
    monkeypatch.setattr(
        importer,
        "_save_last_updated_partition_plan",
        checkpoint_recorder._save,
    )
    persist_pass = AsyncMock()
    monkeypatch.setattr(importer, "_persist_partition_pass", persist_pass)

    fetch_outcome = await importer._fetch_partition_plan_passes(
        directory_source,
        "Practitioner",
        ProviderDirectoryPractitioner,
        partition_config,
        state,
        fetch_options,
    )

    assert fetch_outcome is not None
    assert fetch_outcome.error.endswith(":pagination_loop_detected")
    assert partition_plan.status is importer.PlanStatus.ACQUIRING
    assert partition_plan.failure is None
    assert set(partition_plan.windows["root.0"].passes) == {1, 2}
    assert partition_plan.windows["root.1"].passes == {}
    completed_passes_by_window = {
        window_fields["window_id"]: window_fields["completed_passes"]
        for window_fields in checkpoint_recorder.payloads[-1]["control"]["windows"]
    }
    assert completed_passes_by_window["root.0"] == [1, 2]
    assert completed_passes_by_window["root.1"] == []
    persist_pass.assert_not_awaited()


@pytest.mark.asyncio
async def test_partition_permanent_unbounded_failure_remains_terminal(
    monkeypatch,
):
    (
        directory_source,
        partition_config,
        partition_plan,
        state,
        fetch_options,
    ) = _partition_retry_state_with_completed_window()
    permanent_fetch = importer.LastUpdatedWindowFetch(
        resources=(),
        pages_fetched=1,
        complete=False,
        error="non_searchset_bundle",
    )
    monkeypatch.setattr(
        importer,
        "_fetch_partition_window_for_pass",
        AsyncMock(return_value=("https://example.test/invalid", permanent_fetch)),
    )
    persist_pass = AsyncMock()
    monkeypatch.setattr(importer, "_persist_partition_pass", persist_pass)

    fetch_outcome = await importer._fetch_partition_plan_passes(
        directory_source,
        "Practitioner",
        ProviderDirectoryPractitioner,
        partition_config,
        state,
        fetch_options,
    )

    assert fetch_outcome is not None
    assert "incomplete_window" in fetch_outcome.error
    assert fetch_outcome.next_url_remaining is False
    assert partition_plan.status is importer.PlanStatus.FAILED
    assert partition_plan.failure is not None
    assert partition_plan.failure.code == "incomplete_window"
    persist_pass.assert_awaited_once()


class _TransientCensusResponder:
    def __init__(self):
        self.first_requested_urls = []
        self.resumed_requested_urls = []

    async def first_fetch(self, _source, request_url, *, timeout):
        self.first_requested_urls.append(request_url)
        if "_lastUpdated" not in request_url:
            return 200, _last_updated_partition_test_bundle([], total=1), None, 1
        return 503, {"resourceType": "OperationOutcome"}, None, 1

    async def resumed_fetch(self, _source, request_url, *, timeout):
        self.resumed_requested_urls.append(request_url)
        if "_summary=count" in request_url:
            return 200, _last_updated_partition_test_bundle([], total=1), None, 1
        return (
            200,
            _last_updated_partition_test_bundle(
                [_last_updated_partition_test_resource("prac-1")]
            ),
            None,
            1,
        )


@pytest.mark.asyncio
async def test_last_updated_partition_resumes_transient_ranged_census(monkeypatch):
    """Reuse the saved cutoff and unfiltered pre-count after a transient."""
    directory_source = _last_updated_partition_test_source()
    responder = _TransientCensusResponder()
    resume_recorder = _CensusResumeRecorder()
    first_outcome, _staged, _written = (
        await _run_last_updated_partition_test_fetch(
            monkeypatch,
            directory_source,
            responder.first_fetch,
            save_plan_callback=resume_recorder._save,
        )
    )

    assert importer.LAST_UPDATED_PARTITION_RETRYABLE_ERROR in first_outcome.error
    assert resume_recorder.persisted_resume is not None
    assert resume_recorder.persisted_resume.census.unfiltered_pre == 1
    assert resume_recorder.persisted_resume.census.ranged_root_pre is None
    assert len(responder.first_requested_urls) == 2

    fetch_outcome, staged_resources_by_id, written_resource_ids = (
        await _run_last_updated_partition_test_fetch(
            monkeypatch,
            directory_source,
            responder.resumed_fetch,
            partition_resume=resume_recorder.persisted_resume,
        )
    )

    assert fetch_outcome.complete is True
    assert "_lastUpdated" in responder.resumed_requested_urls[0]
    assert all(
        not (
            "_summary=count" in request_url
            and "_lastUpdated" not in request_url
        )
        for request_url in responder.resumed_requested_urls[:-1]
    )
    assert set(staged_resources_by_id) == {"prac-1"}
    assert written_resource_ids == ["prac-1"]


class _PartitionResumeRecorder:
    """Persist the first pass and interrupt before its verification pass."""

    def __init__(self):
        self.persisted_resume = None

    async def _save_and_interrupt(
        self,
        _context,
        _resource_type,
        partition_config,
        partition_plan,
        **checkpoint_fields,
    ):
        census = checkpoint_fields["census"]
        checkpoint_payload = importer._last_updated_partition_checkpoint_payload(
            partition_config,
            partition_plan,
            census,
        )
        restored_resume = importer._last_updated_partition_resume_from_checkpoint(
            checkpoint_payload,
            partition_config,
        )
        self.persisted_resume = importer.LastUpdatedPartitionResume(
            restored_resume.plan,
            census=restored_resume.census,
            pages_processed=checkpoint_fields["pages_processed"],
            rows_processed=checkpoint_fields["rows_processed"],
        )
        has_unverified_first_pass = any(
            1 in partition_window.passes and 2 not in partition_window.passes
            for partition_window in partition_plan.leaf_windows()
        )
        if has_unverified_first_pass:
            raise asyncio.CancelledError


@pytest.mark.asyncio
async def test_last_updated_partition_acquisition_resumes_after_pass_one(monkeypatch):
    directory_source = _last_updated_partition_test_source()
    practitioner_resource = _last_updated_partition_test_resource("prac-1")
    fingerprints_by_window_and_pass = {}
    first_requested_urls = []

    async def first_fetch(_source, request_url, *, timeout):
        first_requested_urls.append(request_url)
        if "_summary=count" in request_url:
            return 200, _last_updated_partition_test_bundle([], total=1), None, 1
        return 200, _last_updated_partition_test_bundle(
            [practitioner_resource]
        ), None, 1

    resume_recorder = _PartitionResumeRecorder()
    with pytest.raises(asyncio.CancelledError):
        await _run_last_updated_partition_test_fetch(
            monkeypatch,
            directory_source,
            first_fetch,
            save_plan_callback=resume_recorder._save_and_interrupt,
            fingerprints_by_window_and_pass=fingerprints_by_window_and_pass,
        )

    assert resume_recorder.persisted_resume is not None
    assert len(first_requested_urls) == 3
    resumed_requested_urls = []

    async def resumed_fetch(_source, request_url, *, timeout):
        resumed_requested_urls.append(request_url)
        if "_summary=count" in request_url:
            return (
                200,
                _last_updated_partition_test_bundle([], total=1),
                None,
                1,
            )
        return 200, _last_updated_partition_test_bundle(
            [practitioner_resource]
        ), None, 1

    fetch_outcome, staged_resources_by_id, written_resource_ids = (
        await _run_last_updated_partition_test_fetch(
            monkeypatch,
            directory_source,
            resumed_fetch,
            partition_resume=resume_recorder.persisted_resume,
            fingerprints_by_window_and_pass=fingerprints_by_window_and_pass,
        )
    )

    assert fetch_outcome.complete is True
    assert fetch_outcome.fetch_mode == importer.LAST_UPDATED_PARTITION_FETCH_MODE
    assert set(staged_resources_by_id) == {"prac-1"}
    assert written_resource_ids == ["prac-1"]
    assert len(resumed_requested_urls) == 3
    assert "_summary=count" not in resumed_requested_urls[0]
    assert "_lastUpdated" in resumed_requested_urls[1]
    assert "_lastUpdated" not in resumed_requested_urls[2]


@pytest.mark.asyncio
async def test_last_updated_partition_acquisition_blocks_count_mismatch(monkeypatch):
    directory_source = _last_updated_partition_test_source()

    async def fetch_source_json(_source, request_url, *, timeout):
        if "_summary=count" in request_url:
            return 200, _last_updated_partition_test_bundle([], total=2), None, 1
        return 200, _last_updated_partition_test_bundle(
            [_last_updated_partition_test_resource("prac-1")]
        ), None, 1

    fetch_outcome, staged_resources_by_id, written_resource_ids = (
        await _run_last_updated_partition_test_fetch(
            monkeypatch,
            directory_source,
            fetch_source_json,
        )
    )

    assert fetch_outcome.complete is False
    assert "count_mismatch" in fetch_outcome.error
    assert staged_resources_by_id == {}
    assert written_resource_ids == []


@pytest.mark.asyncio
async def test_last_updated_partition_acquisition_rejects_duplicate_ids(monkeypatch):
    directory_source = _last_updated_partition_test_source()
    duplicate_resource = _last_updated_partition_test_resource("prac-duplicate")

    async def fetch_source_json(_source, request_url, *, timeout):
        if "_summary=count" in request_url:
            return 200, _last_updated_partition_test_bundle([], total=2), None, 1
        return 200, _last_updated_partition_test_bundle(
            [duplicate_resource, duplicate_resource]
        ), None, 1

    fetch_outcome, staged_resources_by_id, written_resource_ids = (
        await _run_last_updated_partition_test_fetch(
            monkeypatch,
            directory_source,
            fetch_source_json,
        )
    )

    assert fetch_outcome.complete is False
    assert "duplicate_resource_id" in fetch_outcome.error
    assert staged_resources_by_id == {}
    assert written_resource_ids == []


class _UnstableTwinPassResponder:
    """Return a changed stable field during the verification pass."""

    def __init__(self):
        self.family_names_by_pass = iter(("First", "Changed"))

    async def _fetch_source_json(self, _source, request_url, *, timeout):
        if "_summary=count" in request_url:
            return 200, _last_updated_partition_test_bundle([], total=1), None, 1
        family_name = next(self.family_names_by_pass)
        practitioner_resource = _last_updated_partition_test_resource(
            "prac-1",
            family=family_name,
        )
        return 200, _last_updated_partition_test_bundle(
            [practitioner_resource]
        ), None, 1


@pytest.mark.asyncio
async def test_last_updated_partition_acquisition_blocks_unstable_twin_pass(monkeypatch):
    directory_source = _last_updated_partition_test_source()
    responder = _UnstableTwinPassResponder()

    fetch_outcome, staged_resources_by_id, written_resource_ids = (
        await _run_last_updated_partition_test_fetch(
            monkeypatch,
            directory_source,
            responder._fetch_source_json,
        )
    )

    assert fetch_outcome.complete is False
    assert "twin_pass_mismatch" in fetch_outcome.error
    assert staged_resources_by_id == {}
    assert written_resource_ids == []


class _DeterministicPartitionResponder:
    """Serve one split partition with volatile-only pass differences."""

    def __init__(self):
        self.requested_urls = []

    def _count_for_bounds(self, partition_bounds):
        counts_by_bounds = {
            (
                "ge2024-01-01T00:00:00.000000Z",
                "lt2024-01-03T00:00:00.000000Z",
            ): 3,
            (
                "ge2024-01-01T00:00:00.000000Z",
                "lt2024-01-02T00:00:00.000000Z",
            ): 1,
            (
                "ge2024-01-02T00:00:00.000000Z",
                "lt2024-01-03T00:00:00.000000Z",
            ): 2,
        }
        return counts_by_bounds[partition_bounds]

    def _resources_for_bounds(self, partition_bounds):
        lower_bound, _upper_bound = partition_bounds
        if lower_bound != "ge2024-01-01T00:00:00.000000Z":
            return [
                _last_updated_partition_test_resource(
                    "prac-right-1",
                    last_updated="2024-01-02T01:00:00Z",
                ),
                _last_updated_partition_test_resource(
                    "prac-right-2",
                    last_updated="2024-01-02T02:00:00Z",
                ),
            ]
        volatile_timestamp = (
            "2024-01-01T01:00:00Z"
            if len(self.requested_urls) % 2
            else "2024-01-01T01:00:01Z"
        )
        return [
            _last_updated_partition_test_resource(
                "prac-left",
                last_updated=volatile_timestamp,
            )
        ]

    async def _fetch_source_json(self, _source, request_url, *, timeout):
        self.requested_urls.append(request_url)
        query_lookup = urllib.parse.parse_qs(
            urllib.parse.urlsplit(request_url).query
        )
        if query_lookup.get("_summary") == ["count"]:
            partition_bounds = tuple(query_lookup.get("_lastUpdated", ()))
            partition_count = (
                self._count_for_bounds(partition_bounds)
                if partition_bounds
                else 3
            )
            return 200, _last_updated_partition_test_bundle(
                [],
                total=partition_count,
            ), None, 1
        partition_bounds = tuple(query_lookup["_lastUpdated"])
        practitioner_resources = self._resources_for_bounds(partition_bounds)
        return 200, _last_updated_partition_test_bundle(
            practitioner_resources
        ), None, 1


@pytest.mark.asyncio
async def test_last_updated_partition_acquisition_succeeds_deterministically(monkeypatch):
    """Prove every census, leaf, pass, and staging count is identical."""
    directory_source = _last_updated_partition_test_source(
        ceiling=2,
        page_count=2,
        minimum_width_seconds=3600,
    )
    responder = _DeterministicPartitionResponder()

    assert (
        importer._resource_acquisition_blocked_reason(
            directory_source,
            ["Practitioner"],
        )
        is None
    )
    fetch_outcome, staged_resources_by_id, written_resource_ids = (
        await _run_last_updated_partition_test_fetch(
            monkeypatch,
            directory_source,
            responder._fetch_source_json,
        )
    )

    assert fetch_outcome.complete is True
    assert fetch_outcome.rows_fetched == 3
    assert fetch_outcome.fetch_diagnostic == (
        _last_updated_partition_expected_proof(
            3,
            "2024-01-03T00:00:00.000000Z",
        )
    )
    assert set(staged_resources_by_id) == {
        "prac-left",
        "prac-right-1",
        "prac-right-2",
    }
    assert written_resource_ids == ["prac-left", "prac-right-1", "prac-right-2"]
    count_windows = [
        urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)["_lastUpdated"]
        for url in responder.requested_urls
        if "_summary=count" in url and "_lastUpdated" in url
    ]
    assert count_windows == [
        ["ge2024-01-01T00:00:00.000000Z", "lt2024-01-03T00:00:00.000000Z"],
        ["ge2024-01-01T00:00:00.000000Z", "lt2024-01-02T00:00:00.000000Z"],
        ["ge2024-01-02T00:00:00.000000Z", "lt2024-01-03T00:00:00.000000Z"],
        ["ge2024-01-01T00:00:00.000000Z", "lt2024-01-03T00:00:00.000000Z"],
    ]
    assert sum(
        "_summary=count" in url and "_lastUpdated" not in url
        for url in responder.requested_urls
    ) == 2


@pytest.mark.asyncio
async def test_last_updated_partition_empty_resource_has_complete_zero_proof(
    monkeypatch,
):
    directory_source = _last_updated_partition_test_source()

    async def fetch_empty(_source, request_url, *, timeout):
        if "_summary=count" in request_url:
            return 200, _last_updated_partition_test_bundle([], total=0), None, 1
        return 200, _last_updated_partition_test_bundle([]), None, 1

    fetch_outcome, staged_resources_by_id, written_resource_ids = (
        await _run_last_updated_partition_test_fetch(
            monkeypatch,
            directory_source,
            fetch_empty,
        )
    )

    assert fetch_outcome.complete is True
    assert fetch_outcome.rows_fetched == 0
    assert fetch_outcome.fetch_diagnostic["verified"] is True
    assert {
        fetch_outcome.fetch_diagnostic[field_name]
        for field_name in (
            "unfiltered_pre",
            "ranged_root_pre",
            "exact_leaf_count_sum",
            "pass1_unique",
            "pass2_unique",
            "staged_candidate_count",
            "ranged_root_post",
            "unfiltered_post",
        )
    } == {0}
    assert staged_resources_by_id == {}
    assert written_resource_ids == []


def test_last_updated_partition_proof_is_exposed_in_resource_metrics():
    proof_count = 5
    proof_by_field = {
        "verified": True,
        **{
            field_name: proof_count
            for field_name in (
                "unfiltered_pre",
                "ranged_root_pre",
                "exact_leaf_count_sum",
                "pass1_unique",
                "pass2_unique",
                "staged_candidate_count",
                "ranged_root_post",
                "unfiltered_post",
            )
        },
    }
    fetch_result = importer.ResourceFetchResult(
        model=ProviderDirectoryPractitioner,
        rows=[],
        rows_fetched=proof_count,
        rows_written=proof_count,
        pages_fetched=4,
        complete=True,
        row_limit_reached=False,
        page_limit_reached=False,
        hard_page_limit_reached=False,
        next_url_remaining=False,
        fetch_mode=importer.LAST_UPDATED_PARTITION_FETCH_MODE,
        fetch_diagnostic=proof_by_field,
    )
    stats_by_resource = {}

    importer._record_resource_fetch_stats(
        stats_by_resource,
        "Practitioner",
        fetch_result,
    )
    diagnostic = importer._resource_fetch_diagnostic(
        fetch_result,
        rows_written=proof_count,
    )

    practitioner_stats = stats_by_resource["Practitioner"]
    assert practitioner_stats["last_updated_partition_sources"] == 1
    assert practitioner_stats["last_updated_completeness_verified_sources"] == 1
    assert practitioner_stats["last_updated_unfiltered_post"] == proof_count
    assert diagnostic["last_updated_completeness"] == proof_by_field
    assert diagnostic["source_fetch"] is None


def test_last_updated_partition_checkpoint_omits_resource_fingerprints():
    directory_source = _last_updated_partition_test_source()
    partition_config, config_error = importer._last_updated_partition_config(
        directory_source,
        "Practitioner",
    )
    assert config_error is None
    assert partition_config is not None
    partition_plan = importer.PartitionPlan.create(
        partition_config.start,
        partition_config.end,
        ceiling=partition_config.ceiling,
        minimum_width=partition_config.minimum_width,
        volatile_metadata_paths=partition_config.volatile_metadata_paths,
        boundary_precision=partition_config.boundary_precision,
    )
    partition_plan.observe_count("root", importer.CountObservation.exact(1))
    partition_plan.record_pass(
        "root",
        1,
        [_last_updated_partition_test_resource("sensitive-resource-id")],
        complete=True,
    )

    checkpoint_payload = importer._last_updated_partition_checkpoint_payload(
        partition_config,
        partition_plan,
    )

    assert "sensitive-resource-id" not in checkpoint_payload
    assert json.loads(checkpoint_payload)["control"]["windows"][0][
        "completed_passes"
    ] == [1]


def test_last_updated_partition_checkpoint_replays_census_immutably():
    directory_source = _last_updated_partition_test_source()
    partition_config, config_error = importer._last_updated_partition_config(
        directory_source,
        "Practitioner",
    )
    assert config_error is None and partition_config is not None
    partition_plan = importer.PartitionPlan.create(
        partition_config.start,
        partition_config.end,
        ceiling=partition_config.ceiling,
        minimum_width=partition_config.minimum_width,
        volatile_metadata_paths=partition_config.volatile_metadata_paths,
        boundary_precision=partition_config.boundary_precision,
    )
    census = importer.LastUpdatedCompletenessCensus(
        unfiltered_pre=4,
        ranged_root_pre=4,
    )
    checkpoint_payload = importer._last_updated_partition_checkpoint_payload(
        partition_config,
        partition_plan,
        census,
    )

    restored = importer._last_updated_partition_resume_from_checkpoint(
        checkpoint_payload,
        partition_config,
    )

    assert restored.census == census
    assert importer._last_updated_partition_checkpoint_payload(
        partition_config,
        restored.plan,
        restored.census,
    ) == checkpoint_payload
    conflicting_checkpoint = json.loads(checkpoint_payload)
    conflicting_checkpoint["control"]["census"]["ranged_root_pre"] = 3
    with pytest.raises(RuntimeError, match="checkpoint_invalid"):
        importer._last_updated_partition_resume_from_checkpoint(
            json.dumps(conflicting_checkpoint),
            partition_config,
        )


@pytest.mark.parametrize("identity_drift", ["strategy", "precision"])
def test_last_updated_partition_checkpoint_rejects_precision_identity_drift(
    identity_drift,
):
    directory_source = _last_updated_partition_test_source(
        boundary_precision_seconds=1,
    )
    partition_config, config_error = importer._last_updated_partition_config(
        directory_source,
        "Practitioner",
    )
    assert config_error is None and partition_config is not None
    partition_plan = importer._new_last_updated_partition_plan(partition_config)
    checkpoint = json.loads(
        importer._last_updated_partition_checkpoint_payload(
            partition_config,
            partition_plan,
        )
    )
    if identity_drift == "strategy":
        checkpoint["strategy_version"] = "provider-directory-fhir-last-updated-v3"
    else:
        checkpoint["config"]["boundary_precision_microseconds"] = 1

    with pytest.raises(RuntimeError, match="checkpoint_invalid"):
        importer._last_updated_partition_resume_from_checkpoint(
            json.dumps(checkpoint),
            partition_config,
        )


@pytest.mark.asyncio
async def test_last_updated_partition_resolves_root_cutoff_once(monkeypatch):
    directory_source = _last_updated_partition_test_source(
        end="resolved_now",
        boundary_precision_seconds=1,
    )
    partition_config, config_error = importer._last_updated_partition_config(
        directory_source,
        "Practitioner",
    )
    assert config_error is None
    assert partition_config is not None
    assert partition_config.identity()["end"] == "resolved_now"
    monkeypatch.setattr(
        importer,
        "_fetch_pagination_checkpoint",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        importer,
        "_clear_checkpoint_dataset_resource_type",
        AsyncMock(),
    )
    reset_checkpoint = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_reset_pagination_checkpoint",
        reset_checkpoint,
    )
    before_resolution = datetime.datetime.now(datetime.UTC)

    resume = await importer._load_partition_plan(
        _last_updated_partition_test_context(),
        "Practitioner",
        partition_config,
    )

    after_resolution = datetime.datetime.now(datetime.UTC)
    root_window = resume.plan.windows["root"]
    assert before_resolution <= root_window.end
    assert root_window.end < after_resolution + datetime.timedelta(seconds=1)
    assert root_window.end.microsecond == 0
    durable_payload = reset_checkpoint.await_args.args[2]
    restored_plan = importer._last_updated_partition_plan_from_checkpoint(
        durable_payload,
        partition_config,
    )
    assert restored_plan.windows["root"].end == root_window.end


def test_probe_only_partition_opt_in_must_cover_every_requested_resource():
    source = _last_updated_partition_test_source()

    assert importer._resource_acquisition_blocked_reason(
        source,
        ["Practitioner"],
    ) is None
    assert importer._resource_acquisition_blocked_reason(
        source,
        ["Practitioner", "Location"],
    ) == "coverage_mode_probe_only"


def test_partition_opt_in_is_an_enumerable_endpoint_dataset_resource():
    source = _last_updated_partition_test_source()

    assert importer._endpoint_dataset_selected_resources(
        [source],
        ["Practitioner", "Location"],
    ) == ["Practitioner"]
    assert importer._endpoint_dataset_expected_resources([source]) == (
        "Practitioner",
    )


@pytest.mark.asyncio
async def test_partition_source_group_binds_candidate_checkpoint_context(monkeypatch):
    directory_source = _last_updated_partition_test_source()
    monkeypatch.setattr(
        importer,
        "_select_endpoint_dataset_candidate",
        AsyncMock(
            return_value=importer.EndpointDatasetCandidateSelection(
                dataset_id="dataset_partition",
                acquisition_root_run_id="run_partition",
                previous_dataset_id="dataset_previous",
                reused_from_checkpoint=False,
            )
        ),
    )
    ensure_candidate = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_ensure_endpoint_dataset_candidate",
        ensure_candidate,
    )

    prepared_sources, candidate = (
        await importer._prepare_resource_import_source_group(
            [directory_source],
            ["Practitioner"],
            run_id="run_partition",
            retry_of_run_id=None,
            pagination_root_run_id=None,
            is_checkpointing_enabled=True,
        )
    )

    assert candidate is not None
    assert candidate.selected_resources == ("Practitioner",)
    assert candidate.expected_resources == ("Practitioner",)
    assert candidate.checkpoint_context is not None
    assert candidate.checkpoint_context.dataset_id == "dataset_partition"
    assert candidate.checkpoint_context.lineage_verified is True
    assert prepared_sources[0]["_endpoint_dataset_id"] == "dataset_partition"
    assert (
        prepared_sources[0]["_pagination_checkpoint_context"]
        == candidate.checkpoint_context
    )
    ensure_candidate.assert_awaited_once()
    ensured_candidate, partition_initializations = ensure_candidate.await_args.args
    assert ensured_candidate is candidate
    assert len(partition_initializations) == 1
    assert partition_initializations[0].resource_type == "Practitioner"
    assert (
        partition_initializations[0].checkpoint_resource_type
        == "Practitioner:LastUpdatedPartition"
    )


@pytest.mark.asyncio
async def test_unconfigured_resource_keeps_normal_paged_acquisition(monkeypatch):
    directory_source = _last_updated_partition_test_source()
    directory_source["metadata_json"]["provider_directory_supported_resources"] = [
        "Practitioner",
        "Location",
    ]
    partition_fetch = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_fetch_last_updated_partition_resource_rows",
        partition_fetch,
    )
    monkeypatch.setattr(
        importer,
        "_partitioned_resource_start_urls",
        lambda *_args, **_kwargs: ["https://example.test/fhir/Location"],
    )
    monkeypatch.setattr(
        importer,
        "_fetch_source_json",
        AsyncMock(
            return_value=(
                200,
                _last_updated_partition_test_bundle([]),
                None,
                1,
            )
        ),
    )

    fetch_result = await importer._fetch_resource_rows(
        directory_source,
        "Location",
        per_resource_limit=0,
        page_limit=0,
        page_count=10,
        timeout=10,
        run_id="run_partition",
    )

    assert fetch_result is not None
    assert fetch_result.complete is True
    assert fetch_result.fetch_mode == "paged"
    partition_fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_unconfigured_resource_keeps_normal_bulk_acquisition(monkeypatch):
    directory_source = _last_updated_partition_test_source()
    directory_source["metadata_json"]["provider_directory_supported_resources"] = [
        "Practitioner",
        "Location",
    ]
    partition_fetch = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_fetch_last_updated_partition_resource_rows",
        partition_fetch,
    )
    monkeypatch.setattr(
        importer,
        "_is_source_bulk_export_effective",
        lambda *_args, **_kwargs: True,
    )
    bulk_result = importer.ResourceFetchResult(
        model=ProviderDirectoryLocation,
        rows=[],
        rows_fetched=3,
        rows_written=3,
        pages_fetched=1,
        complete=True,
        row_limit_reached=False,
        page_limit_reached=False,
        hard_page_limit_reached=False,
        next_url_remaining=False,
        fetch_mode="bulk_export",
    )
    bulk_fetch = AsyncMock(return_value=bulk_result)
    monkeypatch.setattr(
        importer,
        "_fetch_bulk_export_resource_rows",
        bulk_fetch,
    )

    fetch_result = await importer._fetch_resource_rows(
        directory_source,
        "Location",
        per_resource_limit=0,
        page_limit=0,
        page_count=10,
        timeout=10,
        run_id="run_partition",
        bulk_export=True,
    )

    assert fetch_result is bulk_result
    bulk_fetch.assert_awaited_once()
    partition_fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_last_updated_partition_pass_two_never_overwrites_pass_one(monkeypatch):
    window = importer.TimeWindow(
        "root",
        datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
        datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC),
        count=1,
        passes={2: importer.WindowPass({"prac-1": "pass-two-hash"})},
    )
    monkeypatch.setattr(
        importer,
        "_load_last_updated_partition_window_fingerprints",
        AsyncMock(return_value={"prac-1": "pass-one-hash"}),
    )
    store_fingerprints = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_store_last_updated_partition_window_fingerprints",
        store_fingerprints,
    )

    with pytest.raises(RuntimeError, match="twin_pass_mismatch"):
        await importer._store_partition_pass_proof(
            _last_updated_partition_test_context(),
            "Practitioner",
            window,
            2,
        )

    store_fingerprints.assert_not_awaited()


class _AtomicPartitionPassHarness:
    """Record transaction ordering and fail the final durable marker."""

    def __init__(self):
        self.events = []
        self.connection = object()

    @contextlib.asynccontextmanager
    async def transaction(self):
        self.events.append("begin")
        try:
            yield self.connection
        except BaseException:
            self.events.append("rollback")
            raise
        else:
            self.events.append("commit")

    async def stage_rows(self, observed_connection, _rows):
        assert observed_connection is self.connection
        self.events.append("candidate_rows")

    async def store_proof(
        self,
        *_args,
        database_connection=None,
        **_kwargs,
    ):
        assert database_connection is self.connection
        self.events.append("proof")

    async def save_marker(
        self,
        *_args,
        database_connection=None,
        **_kwargs,
    ):
        assert database_connection is self.connection
        self.events.append("marker")
        raise RuntimeError("marker_write_failed")

    def install(self, monkeypatch):
        monkeypatch.setattr(importer.db, "acquire", self.transaction)
        monkeypatch.setattr(
            importer,
            "_upsert_dataset_resource_rows_on_connection",
            self.stage_rows,
        )
        monkeypatch.setattr(
            importer,
            "_store_partition_pass_proof",
            self.store_proof,
        )
        monkeypatch.setattr(
            importer,
            "_save_last_updated_partition_plan",
            self.save_marker,
        )


def _atomic_partition_pass_state_and_stage():
    directory_source, partition_plan = _stable_partition_test_plan()
    partition_config, config_error = importer._last_updated_partition_config(
        directory_source,
        "Practitioner",
    )
    assert config_error is None and partition_config is not None
    state = importer.LastUpdatedPartitionState(
        context=_last_updated_partition_test_context(),
        plan=partition_plan,
        census=importer.LastUpdatedCompletenessCensus(
            unfiltered_pre=2,
            ranged_root_pre=2,
        ),
        start_url="https://example.test/fhir/Practitioner",
        pages_fetched=2,
        rows_fetched=2,
        deadline_at=None,
    )
    candidate_stage = importer.LastUpdatedPartitionStage(
        rows=(
            {
                "dataset_id": "dataset_partition",
                "resource_type": "Practitioner",
                "resource_id": "prac-1",
                "payload_hash": "candidate-hash",
                "payload_json": {"resource_id": "prac-1"},
            },
        ),
        candidate_hashes_by_id={"prac-1": "candidate-hash"},
    )
    return partition_plan, partition_config, state, candidate_stage


@pytest.mark.asyncio
async def test_partition_pass_rows_proof_and_marker_share_transaction(monkeypatch):
    """Candidate rows, proof, and marker roll back as one transaction."""
    harness = _AtomicPartitionPassHarness()
    harness.install(monkeypatch)
    partition_plan, partition_config, state, candidate_stage = (
        _atomic_partition_pass_state_and_stage()
    )

    with pytest.raises(RuntimeError, match="marker_write_failed"):
        await importer._persist_partition_pass(
            "Practitioner",
            partition_config,
            state,
            partition_plan.leaf_windows()[0],
            2,
            candidate_stage,
        )

    assert harness.events == [
        "begin",
        "candidate_rows",
        "proof",
        "marker",
        "rollback",
    ]


def _stable_partition_test_plan():
    """Build a verified two-resource plan for staged-row streaming."""
    directory_source = _last_updated_partition_test_source()
    partition_config, config_error = importer._last_updated_partition_config(
        directory_source,
        "Practitioner",
    )
    assert config_error is None
    assert partition_config is not None
    partition_plan = importer.PartitionPlan.create(
        partition_config.start,
        partition_config.end,
        ceiling=partition_config.ceiling,
        minimum_width=partition_config.minimum_width,
        volatile_metadata_paths=partition_config.volatile_metadata_paths,
    )
    partition_plan.observe_count("root", importer.CountObservation.exact(2))
    stable_resources = [
        _last_updated_partition_test_resource("prac-1"),
        _last_updated_partition_test_resource("prac-2"),
    ]
    partition_plan.record_pass("root", 1, stable_resources, complete=True)
    partition_plan.record_pass("root", 2, stable_resources, complete=True)
    partition_plan.windows["root"].passes = {
        1: importer.WindowPass({}),
        2: importer.WindowPass({}),
    }
    return directory_source, partition_plan


class _StagedPartitionRowResponder:
    """Serve deterministic keyset pages from the staged-row relation."""

    def __init__(self):
        self.requested_cursors = []

    async def _fetch_rows(self, _sql, **query_params):
        cursor = query_params["after_resource_id"]
        self.requested_cursors.append(cursor)
        rows_by_cursor = {
            "": [
                {
                    "resource_id": "prac-1",
                    "payload_json": {"resource_id": "prac-1"},
                }
            ],
            "prac-1": [
                {
                    "resource_id": "prac-2",
                    "payload_json": {"resource_id": "prac-2"},
                }
            ],
            "prac-2": [],
        }
        return rows_by_cursor[cursor]


@pytest.mark.asyncio
async def test_last_updated_partition_staged_rows_stream_with_keyset(monkeypatch):
    directory_source, partition_plan = _stable_partition_test_plan()
    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(
                return_value={
                    "candidate_count": 2,
                    "pass1_count": 2,
                    "pass2_count": 2,
                    "invalid_candidate_count": 0,
                "orphan_proof_count": 0,
            }
        ),
    )
    row_responder = _StagedPartitionRowResponder()
    monkeypatch.setattr(importer.db, "all", row_responder._fetch_rows)
    written_batches = []

    async def write_batch(_model, output_rows):
        written_batches.append(
            [output_row["resource_id"] for output_row in output_rows]
        )
        return len(output_rows)

    streamed_count, written_count = (
        await importer._stream_last_updated_partition_staged_rows(
            _last_updated_partition_test_context(),
            directory_source,
            "Practitioner",
            ProviderDirectoryPractitioner,
            partition_plan,
            run_id="run_partition",
            row_batch_handler=write_batch,
            row_batch_size=1,
        )
    )

    assert streamed_count == written_count == 2
    assert row_responder.requested_cursors == ["", "prac-1", "prac-2"]
    assert written_batches == [["prac-1"], ["prac-2"]]
