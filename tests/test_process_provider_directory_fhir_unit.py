# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import importlib
from typing import Any
from unittest.mock import AsyncMock

import pytest
from click.testing import CliRunner
from sqlalchemy.dialects import postgresql

import process as process_cli
from db.models import (
    ProviderDirectoryCapability,
    ProviderDirectoryCanonicalResource,
    ProviderDirectoryEndpoint,
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


def test_source_row_from_seed_overrides_aetna_developer_portal_base():
    row = importer._source_row_from_seed(
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

    assert row["api_base"] == importer.AETNA_PROVIDER_DIRECTORY_BASE
    assert row["canonical_api_base"] == importer.AETNA_PROVIDER_DIRECTORY_BASE
    assert row["requires_registration"] is True
    assert row["auth_type"] == "OAuth2/SMART"
    assert row["last_validated_status"] == "auth_required"
    assert row["metadata_json"]["provider_directory_override"] == "aetna_apif1_providerdirectory"
    assert row["metadata_json"]["provider_directory_previous_api_base"] == "https://fhir-ehr.cerner.com/r4/aetna"


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
    assert (
        row["metadata_json"]["provider_directory_previous_api_base"]
        == "https://apps.availity.com/availity/public-fhir/fhir/v1/cigna/r4"
    )


def test_source_row_from_seed_overrides_centene_partner_portal_base():
    row = importer._source_row_from_seed(
        {
            "id": "centene-1",
            "org_name": "Centene Corporation",
            "plan_name": "Ambetter",
            "api_base": "https://partners.centene.com/apis",
            "auth_type": "OAuth2 Client Credentials",
            "source": "provider-directory-db",
        }
    )

    assert row["api_base"] == importer.CENTENE_PROVIDER_DIRECTORY_BASE
    assert row["canonical_api_base"] == importer.CENTENE_PROVIDER_DIRECTORY_BASE
    assert row["requires_registration"] is False
    assert row["auth_type"] == "none"
    assert row["metadata_json"]["provider_directory_override"] == "centene_iopc_pd_providerdirectory"
    assert row["metadata_json"]["provider_directory_previous_api_base"] == "https://partners.centene.com/apis"
    assert row["metadata_json"]["provider_directory_confirmed_catalog_url"] == importer.CENTENE_PARTNER_PORTAL_APIS_URL
    assert row["metadata_json"]["provider_directory_replaces_stale_generic_api_bases"] == [
        importer.CENTENE_STALE_GENERIC_BASE
    ]
    assert (
        row["metadata_json"]["provider_directory_special_case_california_base"]
        == importer.CENTENE_PROVIDER_DIRECTORY_CALIFORNIA_BASE
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
    row = importer._source_row_from_seed(
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
    assert row["api_base"] == expected_base
    assert row["canonical_api_base"] == expected_base
    assert row["requires_registration"] is False
    assert row["auth_type"] == "none"
    assert row["last_validated_status"] == "valid"
    assert row["endpoint_practitioner"] == f"{expected_base}/Practitioner"
    assert row["endpoint_location"] == f"{expected_base}/Location"
    assert row["metadata_json"]["provider_directory_override"] == "amerihealth_caritas_api_ext_provider_api"
    assert row["metadata_json"]["provider_directory_plan_code"] == "5400"
    assert row["metadata_json"]["provider_directory_confirmed_metadata_url"] == f"{expected_base}/metadata"


def test_source_row_from_seed_does_not_guess_ambiguous_amerihealth_caritas_retest_base():
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
    rows = importer._amerihealth_caritas_seed_rows_from_catalog_html(
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

    assert len(rows) == 2
    assert rows[0]["plan_name"] == "AmeriHealth Caritas District of Columbia"
    assert rows[0]["api_base"] == "https://api-ext.amerihealthcaritas.com/5400/provider-api"
    assert rows[0]["source"] == "amerihealth-caritas-developer-portal"
    assert rows[0]["source_url"] == "fixture.html"
    assert rows[1]["plan_name"] == "AmeriHealth Caritas VIP Care"
    assert rows[1]["api_base"] == "https://api-ext.amerihealthcaritas.com/PA02/provider-api"


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


def test_provider_directory_blocker_rows_are_non_importable_catalog_sources():
    rows = importer._provider_directory_blocker_seed_rows(source_query="Puerto Rico")

    assert len(rows) == 1
    seed_row = rows[0]
    assert seed_row["org_name"] == "Territory of Puerto Rico"
    assert seed_row["api_base"] is None
    assert seed_row["last_validated_status"] == importer.PROVIDER_DIRECTORY_CATALOG_BLOCKED_STATUS
    assert seed_row["metadata_json"]["provider_directory_blocked"] is True

    source_row = importer._source_row_from_seed(seed_row)
    selected, metrics = importer._select_resource_import_sources(
        [source_row],
        valid_source_ids=None,
        open_only=True,
        include_auth_required=True,
    )
    assert selected == []
    assert metrics["source_import_skipped_missing_api_base"] == 1


def _cms_sma_fixture_csv() -> str:
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
    rows = importer._cms_sma_endpoint_directory_seed_rows_from_csv(
        _cms_sma_fixture_csv(),
        source_url="fixture.csv",
    )

    assert len(rows) == 3
    row_by_org = {row["org_name"]: row for row in rows}
    row = row_by_org["State of Alaska"]
    assert row["org_name"] == "State of Alaska"
    assert row["plan_name"] == "Alaska Medicaid Provider Directory"
    assert row["api_base"] == "https://api.alaskafhir.com/r4"
    assert row["endpoint_practitioner"] == "https://api.alaskafhir.com/r4/public/Practitioner"
    assert row["endpoint_organization"] == "https://api.alaskafhir.com/r4/public/Organization"
    assert row["endpoint_location"] == "https://api.alaskafhir.com/r4/public/Location"
    assert row["last_validated_status"] == importer.CMS_SMA_CATALOG_VALIDATION_STATUS
    assert row["source"] == importer.CMS_SMA_ENDPOINT_DIRECTORY_SOURCE
    assert row["metadata_json"]["provider_directory_confirmed_metadata_url"] == (
        "https://api.alaskafhir.com/r4/metadata"
    )
    assert len(row["data_quality_checked"]) > 64
    source_row = importer._source_row_from_seed(row)
    assert source_row["data_quality_checked"] == row["data_quality_checked"]
    assert (
        str(
            ProviderDirectorySource.__table__.c.data_quality_checked.type.compile(
                dialect=postgresql.dialect()
            )
        ).upper()
        == "TEXT"
    )
    assert "https://api.alaskafhir.com/r4/public" in row["metadata_json"][
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
    row = importer._source_row_from_seed(
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

    assert row["api_base"] == importer.UHC_PROVIDER_DIRECTORY_BASE
    assert row["canonical_api_base"] == importer.UHC_PROVIDER_DIRECTORY_BASE
    assert row["requires_registration"] is False
    assert row["auth_type"] == "none"
    assert row["last_validated_status"] == "valid"
    assert row["metadata_json"]["provider_directory_override"] == "uhc_flex_optum_fhirpublic_r4"
    assert row["metadata_json"]["provider_directory_previous_api_base"] == importer.UHC_INTEROPERABILITY_APIS_URL
    assert row["metadata_json"]["provider_directory_confirmed_metadata_url"] == importer.UHC_PROVIDER_DIRECTORY_METADATA_URL


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


def test_source_row_from_seed_overrides_humana_stale_oauth_label():
    row = importer._source_row_from_seed(
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

    assert row["api_base"] == importer.HUMANA_PROVIDER_DIRECTORY_BASE
    assert row["canonical_api_base"] == importer.HUMANA_PROVIDER_DIRECTORY_BASE
    assert row["requires_registration"] is False
    assert row["auth_type"] == "none"
    assert row["last_validated_status"] == "valid"
    assert row["metadata_json"]["provider_directory_override"] == "humana_public_fhir_api"
    assert row["metadata_json"]["provider_directory_confirmed_metadata_url"] == importer.HUMANA_PROVIDER_DIRECTORY_METADATA_URL


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


def test_source_row_from_seed_overrides_nebraska_dhhs_stale_oauth_label():
    row = importer._source_row_from_seed(
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

    assert row["api_base"] == importer.NEBRASKA_DHHS_PROVIDER_DIRECTORY_BASE
    assert row["canonical_api_base"] == importer.NEBRASKA_DHHS_PROVIDER_DIRECTORY_BASE
    assert row["requires_registration"] is False
    assert row["auth_type"] == "none"
    assert row["metadata_json"]["provider_directory_override"] == "nebraska_dhhs_public_providerdirectory"
    assert (
        row["metadata_json"]["provider_directory_confirmed_metadata_url"]
        == importer.NEBRASKA_DHHS_PROVIDER_DIRECTORY_METADATA_URL
    )


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
    row = importer._source_row_from_seed(
        {
            "id": "scan-1",
            "org_name": "SCAN Health Plan",
            "plan_name": "Medicare Advantage",
            "api_base": "https://developer.scanhealthplan.com",
            "auth_type": "API Key",
            "source": "provider-directory-db",
        }
    )

    assert row["api_base"] == importer.SCAN_PROVIDER_DIRECTORY_BASE
    assert row["canonical_api_base"] == importer.SCAN_PROVIDER_DIRECTORY_BASE
    assert row["requires_registration"] is False
    assert row["auth_type"] == "none"
    assert row["last_validated_status"] == "valid"
    assert row["metadata_json"]["provider_directory_override"] == "scan_providerdirectory_intersystems"
    assert row["metadata_json"]["provider_directory_previous_api_base"] == importer.SCAN_DEVELOPER_PORTAL_URL
    assert row["metadata_json"]["provider_directory_confirmed_doc_url"] == importer.SCAN_PROVIDER_DIRECTORY_DOC_URL


def test_source_row_from_seed_uses_normalized_base_for_stable_scan_source_id():
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


def test_source_row_from_seed_uses_normalized_base_for_stable_resource_url_source_id():
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
    row = importer._source_row_from_seed(
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

    assert row["api_base"] == "https://md-medicaid.convergent-pd.com/fhir"
    assert row["canonical_api_base"] == "https://md-medicaid.convergent-pd.com/fhir"
    assert row["source_id"] == encoded["source_id"]
    assert encoded["api_base"] == row["api_base"]
    assert row["last_validated_status"] == "not_found"
    assert (
        row["metadata_json"]["provider_directory_base_normalization"]
        == "resource_or_metadata_parent_base"
    )
    assert row["metadata_json"]["provider_directory_previous_api_base"] == (
        "https://md-medicaid.convergent-pd.com/fhir/{resource}"
    )
    assert row["metadata_json"]["provider_directory_confirmed_base"] == (
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


def test_source_row_from_seed_preserves_known_confirmed_importable_bases():
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
        row = importer._source_row_from_seed(seed_row)

        assert row["api_base"] == expected_base
        assert row["canonical_api_base"] == expected_base
        assert row["metadata_json"]["provider_directory_override"] == expected_override
        assert row["metadata_json"]["provider_directory_confirmed_base"] == expected_base


def test_resource_import_source_selection_reports_credentialed_policy_counts():
    sources = [
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
        sources,
        valid_source_ids=None,
        open_only=False,
        include_auth_required=True,
    )
    selected_without_auth, metrics_without_auth = importer._select_resource_import_sources(
        sources,
        valid_source_ids=None,
        open_only=False,
        include_auth_required=False,
    )

    assert [source["source_id"] for source in selected] == ["open_valid", "auth_required"]
    assert metrics["source_import_sources_considered"] == 4
    assert metrics["source_import_sources_selected"] == 2
    assert metrics["source_import_sources_selected_declared_credentialed"] == 1
    assert metrics["source_import_sources_selected_auth_required_seed"] == 1
    assert metrics["source_import_skipped_validation_status"] == 1
    assert metrics["source_import_skipped_missing_api_base"] == 1
    assert [source["source_id"] for source in selected_without_auth] == ["open_valid"]
    assert metrics_without_auth["source_import_skipped_auth_required_policy"] == 1


def test_resource_import_source_selection_allows_live_probe_success_over_open_only():
    source = {
        "source_id": "credentialed_valid",
        "api_base": "https://auth.example/fhir",
        "auth_type": "OAuth2 Client Credentials",
        "last_validated_status": "auth_required",
        "requires_registration": True,
    }

    selected, metrics = importer._select_resource_import_sources(
        [source],
        valid_source_ids={"credentialed_valid"},
        open_only=True,
        include_auth_required=False,
    )

    assert selected == [source]
    assert metrics["source_import_sources_selected"] == 1
    assert metrics["source_import_sources_selected_live_probe_valid"] == 1
    assert metrics["source_import_sources_selected_declared_credentialed"] == 1
    assert metrics["source_import_sources_selected_auth_required_seed"] == 1


def test_seed_rows_from_retest_results_filters_to_provider_like_rows(tmp_path):
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

    rows = importer._seed_rows_from_retest_results(retest_path)

    assert [row["org_name"] for row in rows] == [
        "Public FHIR Payer",
        "Public App Payer",
        "Auth Payer",
        "Aetna",
        "Humana Inc.",
        "State of Maine",
        "Maryland Physicians Care",
    ]
    assert rows[0]["auth_type"] == "open"
    assert rows[0]["source"] == "provider-directory-db-retest"
    assert rows[0]["source_date"] == "2026-06-03T17:43:09Z"
    assert rows[1]["last_validated_status"] == "valid_non_fhir"
    assert rows[2]["last_validated_status"] == "auth_required"
    assert rows[2]["requires_registration"] is True
    assert rows[2]["auth_type"] == "OAuth2 Client Credentials"
    assert rows[3]["last_validated_status"] == "not_found"
    normalized_aetna = importer._source_row_from_seed(rows[3])
    assert normalized_aetna["api_base"] == importer.AETNA_PROVIDER_DIRECTORY_BASE
    assert normalized_aetna["requires_registration"] is True
    assert normalized_aetna["last_validated_status"] == "auth_required"
    assert rows[4]["last_validated_status"] == "client_error"
    normalized_humana = importer._source_row_from_seed(rows[4])
    assert normalized_humana["api_base"] == importer.HUMANA_PROVIDER_DIRECTORY_BASE
    assert normalized_humana["requires_registration"] is False
    assert normalized_humana["last_validated_status"] == "valid"
    assert rows[5]["last_validated_status"] == "not_found"
    normalized_maine = importer._source_row_from_seed(rows[5])
    assert normalized_maine["api_base"] == "https://maineproviderdirectory.verityanalytics.org/fhir"
    assert normalized_maine["last_validated_status"] == "not_found"
    assert (
        normalized_maine["metadata_json"]["provider_directory_base_normalization"]
        == "resource_or_metadata_parent_base"
    )
    assert rows[6]["last_validated_status"] == "not_found"
    normalized_maryland = importer._source_row_from_seed(rows[6])
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

    rows = importer._dedupe_source_rows([sqlite_row, retest_row])

    assert rows == [sqlite_row]
    assert rows[0]["metadata_json"]["provider_directory_equivalent_api_bases"] == [
        "https://fhir.molinahealthcare.com/provider-directory"
    ]
    assert rows[0]["metadata_json"]["provider_directory_merged_overrides"] == [
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


def test_provider_directory_location_address_key_sql_can_scope_to_current_run_and_sources():
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


def test_provider_directory_location_address_key_batch_sql_can_scope_to_seen_stage():
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

    monkeypatch.setattr(importer, "_address_canon_functions_available", AsyncMock(return_value=True))
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

    monkeypatch.setattr(importer, "_address_canon_functions_available", AsyncMock(return_value=True))
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


def test_provider_directory_location_upsert_preserves_existing_address_key_when_raw_address_unchanged():
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


def test_provider_directory_source_seed_upsert_merges_metadata_json():
    sql = importer._effective_update_sql(
        ProviderDirectorySource.__table__,
        "metadata_json",
        target_prefix="target_row",
        incoming_prefix="stage_row",
    )

    assert sql == (
        'COALESCE(target_row."metadata_json"::jsonb, \'{}\'::jsonb) '
        '|| COALESCE(stage_row."metadata_json"::jsonb, \'{}\'::jsonb)'
    )


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
async def test_ensure_provider_directory_source_column_types_widens_data_quality_checked(monkeypatch):
    status_mock = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status_mock)

    await importer._ensure_provider_directory_source_column_types("mrf")

    status_mock.assert_awaited_once()
    sql = status_mock.await_args.args[0]
    assert 'ALTER TABLE IF EXISTS "mrf"."provider_directory_source"' in sql
    assert "ALTER COLUMN data_quality_checked TYPE text" in sql


def test_source_catalog_stale_cleanup_only_runs_for_unfiltered_full_refresh():
    assert importer._source_catalog_stale_cleanup_enabled(
        stale_cleanup=True,
        full_refresh=True,
        source_query=None,
        limit=None,
    )
    assert not importer._source_catalog_stale_cleanup_enabled(
        stale_cleanup=True,
        full_refresh=False,
        source_query=None,
        limit=None,
    )
    assert not importer._source_catalog_stale_cleanup_enabled(
        stale_cleanup=True,
        full_refresh=True,
        source_query="cigna",
        limit=None,
    )
    assert not importer._source_catalog_stale_cleanup_enabled(
        stale_cleanup=True,
        full_refresh=True,
        source_query=None,
        limit=10,
    )
    assert not importer._source_catalog_stale_cleanup_enabled(
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
    assert calls[0]["seed_only"] is True
    assert calls[0]["probe"] is False


def test_provider_directory_refresh_preset_rejects_unknown_value():
    with pytest.raises(ValueError, match="Unsupported Provider Directory refresh_preset"):
        importer._apply_provider_directory_refresh_preset({"refresh_preset": "weekly"})


@pytest.mark.asyncio
async def test_delete_stale_provider_directory_source_catalog_prunes_dependent_rows(monkeypatch):
    status_mock = AsyncMock(return_value=1)
    monkeypatch.setattr(importer.db, "status", status_mock)

    deleted = await importer._delete_stale_provider_directory_source_catalog(
        ["source_b", "source_a", "source_a", ""]
    )

    assert deleted["provider_directory_source"] == 1
    assert status_mock.await_count == len(importer.SOURCE_CATALOG_STALE_TABLE_MODELS) + 1
    for call in status_mock.await_args_list:
        assert call.kwargs["source_ids"] == ["source_a", "source_b"]
        assert "NOT (source_id = ANY(CAST(:source_ids AS varchar[])))" in call.args[0]
    assert '"mrf"."provider_directory_source"' in status_mock.await_args_list[-1].args[0]


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

    rows = [
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
        rows,
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
    assert upsert_calls == [(ProviderDirectoryLocation, rows, {"skip_unchanged": False})]
    assert rows[0]["phone_number"] == "3125550100"
    assert rows[0]["phone_extension"] == "45"
    assert rows[0]["fax_number_digits"] == "3125550199"
    assert rows[1]["phone_number"] == "3125550101"


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

    rows = [
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
        rows,
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
    assert sorted(row["source_id"] for row in edge_call[1]) == ["source_a", "source_b"]
    assert {row["resource_id"] for row in edge_call[1]} == {"loc-1"}

    assert upsert_calls[2] == (ProviderDirectoryLocation, rows, {"skip_unchanged": True})
    assert seen_calls == [(ProviderDirectoryLocation, rows, "run_1", {"seen_table": None})]


@pytest.mark.asyncio
async def test_import_alohr_graphql_source_group_writes_existing_resource_tables(monkeypatch):
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

    upserts: dict[type, list[dict[str, Any]]] = {}

    async def fake_upsert(model, rows, **_kwargs):
        upserts.setdefault(model, []).extend(rows)
        return len(rows)

    monkeypatch.setattr(importer, "_fetch_alohr_graphql_page", fake_fetch_page)
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)

    source_ids, diagnostics, counts, linked_counts, stats, stale, stale_ready = await importer._import_alohr_graphql_source_group(
        [{"source_id": "source_alohr", "api_base": importer.ALOHR_PUBLIC_PROVIDER_DIRECTORY_BASE}],
        resources=["Practitioner", "Location", "PractitionerRole", "Organization", "OrganizationAffiliation"],
        per_resource_limit=0,
        page_limit=0,
        timeout=3,
        run_id="run_1",
        stale_cleanup=False,
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
    assert counts["OrganizationAffiliation"] == 1
    assert upserts[ProviderDirectoryOrganization][0]["npi"] == 1992793046
    assert diagnostics["Practitioner"]["complete"] is True
    assert diagnostics["Organization"]["rows_written"] == 1
    assert stats["Location"]["sources_completed"] == 1


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

    oauth2 = {
        "token_url": "env:PAYER_DIRECTORY_TOKEN_URL",
        "client_id": "env:PAYER_DIRECTORY_CLIENT_ID",
        "client_secret": "env:PAYER_DIRECTORY_CLIENT_SECRET",
        "scope": "system/*.read",
    }
    first = importer._fetch_oauth2_client_credentials_token_sync(oauth2)
    second = importer._fetch_oauth2_client_credentials_token_sync(oauth2)

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
async def test_probe_sources_records_credential_descriptor_without_secret(monkeypatch):
    monkeypatch.setenv("PAYER_DIRECTORY_TOKEN", "secret-token")
    monkeypatch.setenv(
        importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV,
        json.dumps({"sources": {"source_a": {"bearer_token": "env:PAYER_DIRECTORY_TOKEN"}}}),
    )

    async def fake_fetch_json_with_options(url, *, timeout, extra_headers=None, query_params=None):
        assert url == "https://payer.example/fhir/metadata?_format=json"
        assert extra_headers == {"Authorization": "Bearer secret-token"}
        assert query_params == {}
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

    source = {
        "source_id": "source_a",
        "org_name": "Payer",
        "api_base": "https://payer.example/fhir",
        "canonical_api_base": "https://payer.example/fhir",
        "metadata_json": {},
    }
    await importer._probe_sources(
        [source],
        timeout=3,
        concurrency=1,
        run_id="run_1",
    )

    source_rows = next(rows for model, rows in upserts if model is ProviderDirectorySource)
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

    probed, valid, valid_source_ids = await importer._probe_sources(
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
    capability_rows = next(rows for model, rows in upserts if model is ProviderDirectoryCapability)
    source_rows = next(rows for model, rows in upserts if model is ProviderDirectorySource)
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

    source = {
        "source_id": "source_a",
        "org_name": "Payer",
        "api_base": "https://payer.example/fhir",
        "canonical_api_base": "https://payer.example/fhir",
        "auth_type": "OAuth2/SMART",
        "requires_registration": True,
    }
    probed, valid, valid_source_ids = await importer._probe_sources(
        [source],
        timeout=3,
        concurrency=1,
        run_id="run_1",
    )

    assert (probed, valid, valid_source_ids) == (1, 0, set())
    capability_rows = next(rows for model, rows in upserts if model is ProviderDirectoryCapability)
    source_rows = next(rows for model, rows in upserts if model is ProviderDirectorySource)
    assert capability_rows[0]["probe_status"] == "auth_required"
    assert capability_rows[0]["auth_required"] is True
    assert source_rows[0]["last_validated_status"] == "auth_required"


@pytest.mark.asyncio
async def test_probe_source_keeps_alohr_graphql_connector_valid_without_fhir_credentials(monkeypatch):
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

    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_source_json)

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

    assert payload["resourceType"] == "CapabilityStatement"
    assert probe["status"] == "valid"
    assert probe["credential"] is None


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
        return 400, {"resourceType": "OperationOutcome"}, None, 5

    upserts = []

    async def fake_upsert(model, rows, **_kwargs):
        upserts.append((model, rows))
        return len(rows)

    monkeypatch.setattr(importer, "_fetch_json", fake_fetch_json)
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)

    source = importer._source_row_from_seed(
        {
            "id": "humana",
            "org_name": "Humana",
            "plan_name": "Medicare Advantage",
            "api_base": "https://fhir.humana.com/api/provider-directory/",
        }
    )

    probed, valid, valid_source_ids = await importer._probe_sources(
        [source],
        timeout=3,
        concurrency=1,
        run_id="run_1",
    )

    assert (probed, valid, valid_source_ids) == (1, 1, {source["source_id"]})
    assert source["api_base"] == "https://fhir.humana.com/api"
    capability_rows = next(rows for model, rows in upserts if model is ProviderDirectoryCapability)
    source_rows = next(rows for model, rows in upserts if model is ProviderDirectorySource)
    assert capability_rows[0]["api_base"] == "https://fhir.humana.com/api"
    assert source_rows[0]["api_base"] == "https://fhir.humana.com/api"
    assert source_rows[0]["last_validated_status"] == "valid"
    assert source_rows[0]["metadata_json"]["provider_directory_override"] == "humana_public_fhir_api"
    assert source_rows[0]["metadata_json"]["provider_directory_previous_api_base"] == (
        "https://fhir.humana.com/api/provider-directory/"
    )


def test_parse_fhir_resource_maps_plan_practitioner_location_role_and_endpoint():
    plan_model, plan_row = importer.parse_fhir_resource(
        "source_a",
        {
            "resourceType": "InsurancePlan",
            "id": "plan-1",
            "identifier": [{"system": "https://example.test/plan-id", "value": "H1234-001"}],
            "name": "Example HMO",
            "network": [{"reference": "Organization/network-1"}],
        },
        run_id="run_1",
    )
    practitioner_model, practitioner_row = importer.parse_fhir_resource(
        "source_a",
        {
            "resourceType": "Practitioner",
            "id": "prac-1",
            "identifier": [{"system": "http://hl7.org/fhir/sid/us-npi", "value": "1234567893"}],
            "name": [{"family": "Rivera", "given": ["Alex"]}],
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
            "practitioner": {"reference": "Practitioner/prac-1"},
            "organization": {"reference": "Organization/org-1"},
            "location": [{"reference": "Location/loc-1"}],
            "insurancePlan": [{"reference": "InsurancePlan/plan-1"}],
            "endpoint": [{"reference": "Endpoint/endpoint-1"}],
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
    assert location_model is ProviderDirectoryLocation
    assert location_row["zip5"] == "60601"
    assert location_row["state_code"] == "IL"
    assert location_row["telephone_number"] == "312-555-0100"
    assert location_row["phone_number"] == "3125550100"
    assert role_model is ProviderDirectoryPractitionerRole
    assert role_row["location_refs"] == ["Location/loc-1"]
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


def test_provider_directory_address_corroboration_sql_links_unified_npi_address_roles():
    """Validate the corroboration view joins unified NPI addresses to FHIR roles."""
    sql = importer.provider_directory_address_corroboration_sql("mrf")

    assert 'CREATE OR REPLACE VIEW "mrf"."provider_directory_address_corroboration" AS' in sql
    assert 'FROM "mrf"."entity_address_unified" e' in sql
    assert "WITH address_candidates AS" in sql
    assert "'provider_directory_fhir' = ANY(e.address_sources)" in sql
    assert 'JOIN "mrf"."provider_directory_practitioner" practitioner' in sql
    assert "practitioner.npi = e.npi" in sql
    assert 'JOIN "mrf"."provider_directory_location" loc' in sql
    assert "COALESCE(role.location_refs::jsonb, '[]'::jsonb)" in sql
    assert 'JOIN "mrf"."provider_directory_healthcare_service" AS healthcare_service' in sql
    assert "COALESCE(role.healthcare_service_refs::jsonb, '[]'::jsonb)" in sql
    assert "COALESCE(healthcare_service.location_refs::jsonb, '[]'::jsonb)" in sql
    assert "COALESCE(role.network_refs::jsonb, '[]'::jsonb) AS network_refs" in sql
    assert "COALESCE(affiliation.location_refs::jsonb, '[]'::jsonb)" in sql
    assert "COALESCE(affiliation.healthcare_service_refs::jsonb, '[]'::jsonb)" in sql
    assert "COALESCE(affiliation.network_refs::jsonb, '[]'::jsonb) AS network_refs" in sql
    assert "role.location_ref IN (loc.resource_id, 'Location/' || loc.resource_id)" in sql
    assert "role.location_ref LIKE '%/Location/' || loc.resource_id" in sql
    assert 'LEFT JOIN "mrf"."provider_directory_network_catalog" network_catalog' in sql
    assert "network_catalog.network_resource_id = NULLIF(BTRIM(CASE" in sql
    assert "regexp_replace(network_ref.value, '^.*/Organization/', '')" in sql
    assert "regexp_replace(network_ref.value, '^Organization/', '')" in sql
    assert "loc.address_key ~* '^[0-9a-f]{8}-" in sql
    assert "THEN loc.address_key::uuid" in sql
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
    assert "loc.phone_number AS provider_directory_phone_number" in sql
    assert "loc.fax_number_digits AS provider_directory_fax_number_digits" in sql
    assert "insurance_plan.plan_identifier" in sql
    assert "insurance_plan.network_refs::jsonb" in sql
    assert "network_org" not in sql
    assert "NULL::varchar AS source_key" in sql
    assert "NULL::varchar AS snapshot_id" in sql
    assert "NULL::varchar AS plan_id" in sql
    assert "NULL::varchar AS ptg_plan_id" in sql
    assert "'payer_directory_corroborated_location'" in sql
    assert "'provider_directory_address'" in sql
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
    assert 'FROM "mrf"."entity_address_unified" e' in sql
    assert "provider_directory_network_names" in sql


@pytest.mark.asyncio
async def test_publish_provider_directory_address_corroboration_table_swaps_indexed_table(monkeypatch):
    statements: list[str] = []

    async def fake_status(sql, **_params):
        statements.append(sql)
        return 0

    async def fake_scalar(sql, **_params):
        statements.append(sql)
        return 7

    monkeypatch.setattr(importer.db, "status", fake_status)
    monkeypatch.setattr(importer.db, "scalar", fake_scalar)
    monkeypatch.setattr(importer, "_stage_table_name", lambda: "pd_stage_corrob")
    publish_catalog = AsyncMock(return_value={"published": True, "rows": 3})
    monkeypatch.setattr(importer, "publish_provider_directory_network_catalog", publish_catalog)

    result = await importer.publish_provider_directory_address_corroboration_table("mrf")
    joined = "\n".join(statements)

    assert result == {
        "published": True,
        "relation": '"mrf"."provider_directory_address_corroboration"',
        "rows": 7,
        "storage": "table",
        "network_catalog": {"published": True, "rows": 3},
    }
    publish_catalog.assert_awaited_once_with("mrf")
    assert 'DROP TABLE IF EXISTS "mrf"."pd_stage_corrob"' in joined
    assert 'CREATE UNLOGGED TABLE "mrf"."pd_stage_corrob" AS' in joined
    assert 'FROM "mrf"."entity_address_unified" e' in joined
    assert 'DROP VIEW "mrf"."provider_directory_address_corroboration"' in joined
    assert 'DROP TABLE "mrf"."provider_directory_address_corroboration";' not in joined
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
async def test_ensure_provider_directory_network_catalog_populated_publishes_empty_catalog(monkeypatch):
    ensure_catalog = AsyncMock()
    publish_catalog = AsyncMock(return_value={"published": True, "rows": 3})
    monkeypatch.setattr(importer, "_ensure_provider_directory_network_catalog_table", ensure_catalog)
    monkeypatch.setattr(importer, "_network_catalog_missing_requirement", AsyncMock(return_value=None))
    monkeypatch.setattr(importer, "_provider_directory_network_catalog_has_rows", AsyncMock(return_value=False))
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
    monkeypatch.setattr(importer, "_provider_directory_network_catalog_has_rows", AsyncMock(return_value=True))
    monkeypatch.setattr(importer, "publish_provider_directory_network_catalog", publish_catalog)

    result = await importer._ensure_provider_directory_network_catalog_populated("mrf")

    assert result == {
        "published": False,
        "reason": "already_populated",
        "relation": '"mrf"."provider_directory_network_catalog"',
    }
    ensure_catalog.assert_awaited_once_with("mrf")
    publish_catalog.assert_not_awaited()


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


def test_sql_ref_matches_resource_accepts_absolute_url_suffixes():
    sql = importer._sql_ref_matches_resource("refs.ref", "Organization", "org.resource_id")

    assert "refs.ref IN (org.resource_id, 'Organization/' || org.resource_id)" in sql
    assert "refs.ref LIKE '%/Organization/' || org.resource_id" in sql


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


@pytest.mark.asyncio
async def test_import_linked_resource_rows_fetches_role_references_and_upserts(monkeypatch):
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

    counts = await importer._import_linked_resource_rows(
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

    assert counts == {"Practitioner": 1, "Location": 1, "InsurancePlan": 1, "Endpoint": 1}
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
        row
        for model, rows, _kwargs in upserts
        if model is ProviderDirectoryCanonicalResource
        for row in rows
    ]
    source_edge_rows = [
        row
        for model, rows, _kwargs in upserts
        if model is ProviderDirectorySourceResource
        for row in rows
    ]
    assert {row["resource_type"] for row in canonical_rows} == {
        "Practitioner",
        "Location",
        "InsurancePlan",
        "Endpoint",
    }
    assert {row["canonical_api_base"] for row in canonical_rows} == {"https://example.test/fhir"}
    assert {row["source_id"] for row in source_edge_rows} == {"source_a"}
    assert {row["resource_type"] for row in source_edge_rows} == {
        "Practitioner",
        "Location",
        "InsurancePlan",
        "Endpoint",
    }
    assert {row["last_seen_run_id"] for row in source_edge_rows} == {"run_1"}


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

    linked_counts = {}
    counts = await importer._import_resources(
        [{"source_id": "source_a", "api_base": "https://example.test/fhir"}],
        resources=["PractitionerRole"],
        per_resource_limit=1,
        page_limit=1,
        page_count=1,
        timeout=3,
        run_id="run_1",
        linked_resource_limit=5,
        linked_counts=linked_counts,
    )

    assert counts == {"PractitionerRole": 1}
    assert linked_counts == {"Location": 2}
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
    metadata_calls: list[dict[str, Any]] = []

    async def fake_update_metadata(_source_ids, **kwargs):
        metadata_calls.append(kwargs["diagnostics"])

    fetch_calls: list[str] = []

    async def fake_fetch_resource_rows(source, resource_type, **_kwargs):
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
        if resource_type == "Practitioner":
            return importer.ResourceFetchResult(
                model=ProviderDirectoryPractitioner,
                rows=[{"source_id": source["source_id"], "resource_id": "prac-1"}],
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
    monkeypatch.setattr(importer, "_address_canon_functions_available", AsyncMock(return_value=False))
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

    monkeypatch.setattr(importer, "_address_canon_functions_available", AsyncMock(return_value=True))
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


def test_provider_directory_location_archive_stage_sql_can_scope_to_current_run_without_seen_stage():
    sql = importer.provider_directory_location_archive_stage_sql(
        "mrf",
        "provider_directory_location_archive_stage_test",
        run_id="run_1",
    )

    assert "loc.last_seen_run_id = CAST(:run_id AS varchar)" in sql
    assert "organization.last_seen_run_id = CAST(:run_id AS varchar)" in sql
    assert "provider_directory_import_seen_stage" not in sql


@pytest.mark.asyncio
async def test_publish_provider_directory_location_archive_resolves_and_cleans_stage(monkeypatch):
    monkeypatch.setattr(importer, "_address_canon_functions_available", AsyncMock(return_value=True))
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
    status_calls = [call.args[0] for call in status.await_args_list]
    assert status_calls[0] == 'DROP TABLE IF EXISTS "mrf"."provider_directory_location_archive_stage_test";'
    assert 'CREATE UNLOGGED TABLE "mrf"."provider_directory_location_archive_stage_test" AS' in status_calls[1]
    assert "loc.last_seen_run_id = CAST(:run_id AS varchar)" in status_calls[1]
    assert status.await_args_list[1].kwargs == {"run_id": "run_1"}
    assert status_calls[2] == 'ANALYZE "mrf"."provider_directory_location_archive_stage_test";'
    assert status_calls[-1] == 'DROP TABLE IF EXISTS "mrf"."provider_directory_location_archive_stage_test";'
    resolve.assert_awaited_once()
    assert resolve.await_args.args[0] == "provider_directory_location_archive_stage_test"
    assert resolve.await_args.kwargs["source_bit"] == importer.PROVIDER_DIRECTORY_ADDRESS_ARCHIVE_SOURCE_BIT
    assert resolve.await_args.kwargs["priority"] == importer.PROVIDER_DIRECTORY_ADDRESS_ARCHIVE_PRIORITY
    assert resolve.await_args.kwargs["schema"] == "mrf"


@pytest.mark.asyncio
async def test_publish_provider_directory_location_archive_skips_without_archive(monkeypatch):
    monkeypatch.setattr(importer, "_address_canon_functions_available", AsyncMock(return_value=True))

    async def fake_table_exists(_schema, table_name):
        return table_name != "address_archive_v2"

    monkeypatch.setattr(importer, "_table_exists", fake_table_exists)
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)

    stats = await importer.publish_provider_directory_location_archive("mrf")

    assert stats == {"skipped": True, "reason": "address_archive_v2_unavailable"}
    status.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_provider_directory_address_corroboration_if_available_skips_without_entity_address_unified(
    monkeypatch,
):
    monkeypatch.setattr(importer, "_table_exists", AsyncMock(return_value=False))
    publish = AsyncMock()
    monkeypatch.setattr(importer, "publish_provider_directory_address_corroboration_table", publish)

    published = await importer.publish_provider_directory_address_corroboration_if_available("mrf")

    assert published is False
    publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_provider_directory_address_corroboration_if_available_publishes_when_entity_address_unified_exists(
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
    upserted: list[dict[str, Any]] = []

    async def fake_upsert(_model, rows, **_kwargs):
        upserted.extend(rows)
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
    assert {row["org_name"] for row in upserted} == {"Cigna", "Supplemental Public Payer"}
    supplemental = next(row for row in upserted if row["org_name"] == "Supplemental Public Payer")
    assert supplemental["api_base"] == "https://supplemental.example/fhir"
    assert supplemental["seed_source"] == "provider-directory-db-retest"


@pytest.mark.asyncio
async def test_process_data_merges_supplemental_catalog_sources(monkeypatch, tmp_path):
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
    upserted: list[dict[str, Any]] = []

    async def fake_upsert(_model, rows, **_kwargs):
        upserted.extend(rows)
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

    assert metrics["sources_seeded"] == 7
    assert metrics["supplemental_catalog_sources_considered"] == 6
    assert metrics["supplemental_catalogs"]["catalogs"]["amerihealth_caritas"]["rows"] == 1
    assert metrics["supplemental_catalogs"]["catalogs"]["contra_costa"]["rows"] == 1
    assert metrics["supplemental_catalogs"]["catalogs"]["cms_sma_endpoint_directory"]["rows"] == 0
    assert metrics["supplemental_catalogs"]["catalogs"]["health_partners_plans"]["rows"] == 1
    assert metrics["supplemental_catalogs"]["catalogs"]["provider_directory_blockers"]["rows"] == 3
    amerihealth = next(
        row for row in upserted if row["seed_source"] == "amerihealth-caritas-developer-portal"
    )
    assert amerihealth["org_name"] == "AmeriHealth Caritas"
    assert amerihealth["plan_name"] == "AmeriHealth Caritas District of Columbia"
    assert amerihealth["api_base"] == "https://api-ext.amerihealthcaritas.com/5400/provider-api"
    contra_costa = next(
        row for row in upserted if row["seed_source"] == "contra-costa-health-developer-page"
    )
    assert contra_costa["org_name"] == "Contra Costa Health Plan"
    assert contra_costa["api_base"] == "https://ihyml0v6d9.execute-api.us-east-1.amazonaws.com/hxprod"
    health_partners_plans = next(
        row for row in upserted if row["seed_source"] == "health-partners-plans-fhir-root"
    )
    assert health_partners_plans["org_name"] == "Health Partners Plans"
    assert health_partners_plans["api_base"] == importer.HEALTH_PARTNERS_PLANS_PROVIDER_DIRECTORY_BASE
    blockers = [
        row for row in upserted if row["seed_source"] == importer.PROVIDER_DIRECTORY_BLOCKER_REGISTRY_SOURCE
    ]
    assert {row["org_name"] for row in blockers} == {
        "Chorus Community Health Plans (fka Children's Community Health Plan)",
        "First Medical Health Plan, Inc.",
        "Territory of Puerto Rico",
    }
    assert all(row["api_base"] is None for row in blockers)
    assert all(row["metadata_json"]["provider_directory_blocked"] is True for row in blockers)


@pytest.mark.asyncio
async def test_process_data_skips_full_refresh_source_catalog_stale_cleanup_without_retest(monkeypatch):
    upserted: list[dict[str, Any]] = []

    async def fake_upsert(_model, rows, **_kwargs):
        upserted.extend(rows)
        return len(rows)

    cleanup = AsyncMock(return_value={"provider_directory_source": 1})
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
    upserted: list[dict[str, Any]] = []

    async def fake_upsert(_model, rows, **_kwargs):
        upserted.extend(rows)
        return len(rows)

    cleanup = AsyncMock(return_value={"provider_directory_source": 1})
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

    cleanup.assert_awaited_once_with([row["source_id"] for row in upserted])
    assert metrics["sources_seeded"] == 2
    assert metrics["stale_source_rows_deleted"] == {"provider_directory_source": 1}


@pytest.mark.asyncio
async def test_process_data_does_not_import_valid_non_fhir_retest_rows_without_valid_probe(monkeypatch, tmp_path):
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
    assert [source["org_name"] for source in imported_sources] == ["Cigna"]
    assert metrics["sources_import_attempted"] == 1
    assert metrics["source_import_sources_considered"] == 2
    assert metrics["source_import_sources_selected"] == 1
    assert metrics["source_import_skipped_validation_status"] == 1


@pytest.mark.asyncio
async def test_process_data_stamps_locations_and_publishes_corroboration_view_when_requested(monkeypatch):
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(importer, "_clear_resource_rows_seen", AsyncMock(return_value=0))
    monkeypatch.setattr(importer, "_upsert_rows", AsyncMock(return_value=1))
    monkeypatch.setattr(importer, "_import_resources", AsyncMock(return_value={"Location": 2}))
    monkeypatch.setattr(
        importer,
        "backfill_provider_directory_location_contacts",
        AsyncMock(return_value={"location_contact_rows_updated": 5}),
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
        },
    )
    assert metrics["resource_rows"] == {"Location": 2}
    assert metrics["sources_import_attempted"] == 1
    assert metrics["location_contacts_backfilled"] == {"location_contact_rows_updated": 5}
    assert metrics["location_address_keys_stamped"] == 3
    assert metrics["location_archive"] == {"inserted": 4, "provenance_updates": 1}
    assert metrics["network_catalog"] == {"rows": 8}
    assert metrics["publish_corroboration"] is True
    assert metrics["ptg_corroboration_view_published"] is True
    importer.backfill_provider_directory_location_contacts.assert_awaited_once()
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
async def test_process_data_publish_artifacts_only_does_not_scope_to_empty_run(monkeypatch):
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(
        importer,
        "backfill_provider_directory_location_contacts",
        AsyncMock(return_value={"location_contact_rows_updated": 0}),
    )
    monkeypatch.setattr(importer, "publish_provider_directory_location_address_keys", AsyncMock(return_value=0))
    monkeypatch.setattr(
        importer,
        "publish_provider_directory_location_archive",
        AsyncMock(return_value={"inserted": 0}),
    )
    monkeypatch.setattr(
        importer,
        "publish_provider_directory_address_overlay",
        AsyncMock(return_value={"rows": 7}),
    )
    monkeypatch.setattr(
        importer,
        "publish_provider_directory_network_catalog",
        AsyncMock(return_value={"rows": 8}),
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
            "run_id": "run_artifact_only",
            "publish_artifacts_only": True,
            "publish_corroboration": True,
        },
    )

    assert metrics["publish_artifacts_only"] is True
    assert metrics["address_overlay"] == {"rows": 7}
    assert metrics["network_catalog"] == {"rows": 8}
    assert metrics["ptg_corroboration_view_published"] is True
    importer.publish_provider_directory_location_address_keys.assert_awaited_once()
    assert importer.publish_provider_directory_location_address_keys.await_args.kwargs["run_id"] is None
    importer.publish_provider_directory_location_archive.assert_awaited_once()
    assert importer.publish_provider_directory_location_archive.await_args.kwargs["run_id"] is None
    importer.publish_provider_directory_address_overlay.assert_awaited_once()
    assert importer.publish_provider_directory_address_overlay.await_args.kwargs["run_id"] is None
    importer.publish_provider_directory_network_catalog.assert_awaited_once()
    assert importer.publish_provider_directory_network_catalog.await_args.kwargs["run_id"] is None
    importer.publish_provider_directory_address_corroboration_if_available.assert_awaited_once()
    assert (
        importer.publish_provider_directory_address_corroboration_if_available.await_args.kwargs[
            "refresh_network_catalog"
        ]
        is False
    )


def test_provider_directory_publish_artifact_targets_parse_aliases():
    assert importer._provider_directory_publish_artifact_targets(None) is None
    assert importer._provider_directory_publish_artifact_targets("network, ptg-corroboration") == {
        "network_catalog",
        "corroboration",
    }
    assert importer._provider_directory_publish_artifact_targets(["addresses"]) == {
        "location_contacts",
        "location_address_keys",
        "location_archive",
        "address_overlay",
    }

    with pytest.raises(ValueError, match="Unsupported Provider Directory publish_artifacts_targets"):
        importer._provider_directory_publish_artifact_targets("bad-stage")


@pytest.mark.asyncio
async def test_process_data_publish_artifacts_only_can_target_network_catalog(monkeypatch):
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(
        importer,
        "backfill_provider_directory_location_contacts",
        AsyncMock(return_value={"location_contact_rows_updated": 0}),
    )
    monkeypatch.setattr(importer, "publish_provider_directory_location_address_keys", AsyncMock(return_value=0))
    monkeypatch.setattr(
        importer,
        "publish_provider_directory_location_archive",
        AsyncMock(return_value={"inserted": 0}),
    )
    monkeypatch.setattr(
        importer,
        "publish_provider_directory_address_overlay",
        AsyncMock(return_value={"rows": 0}),
    )
    monkeypatch.setattr(
        importer,
        "publish_provider_directory_network_catalog",
        AsyncMock(return_value={"rows": 8}),
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
            "run_id": "run_artifact_only",
            "publish_artifacts_only": True,
            "publish_artifacts_targets": "network_catalog",
            "publish_corroboration": True,
        },
    )

    assert metrics["publish_artifacts_targets"] == ["network_catalog"]
    assert metrics["location_contacts_backfilled"] == {"skipped": True, "reason": "target_not_requested"}
    assert metrics["location_address_keys_stamped"] == {"skipped": True, "reason": "target_not_requested"}
    assert metrics["location_archive"] == {"skipped": True, "reason": "target_not_requested"}
    assert metrics["address_overlay"] == {"skipped": True, "reason": "target_not_requested"}
    assert metrics["network_catalog"] == {"rows": 8}
    assert metrics["ptg_corroboration_view_published"] is False
    assert metrics["ptg_corroboration_view_skipped"] == {"skipped": True, "reason": "target_not_requested"}
    importer.backfill_provider_directory_location_contacts.assert_not_awaited()
    importer.publish_provider_directory_location_address_keys.assert_not_awaited()
    importer.publish_provider_directory_location_archive.assert_not_awaited()
    importer.publish_provider_directory_address_overlay.assert_not_awaited()
    importer.publish_provider_directory_network_catalog.assert_awaited_once()
    assert importer.publish_provider_directory_network_catalog.await_args.kwargs["run_id"] is None
    importer.publish_provider_directory_address_corroboration_if_available.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_data_probe_only_full_refresh_does_not_publish_artifacts_by_default(monkeypatch):
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(importer, "_clear_resource_rows_seen", AsyncMock(return_value=0))
    monkeypatch.setattr(importer, "_upsert_rows", AsyncMock(return_value=1))
    monkeypatch.setattr(importer, "_probe_sources", AsyncMock(return_value=(1, 1, {"source_a"})))
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
    assert metrics["location_address_keys_stamped"] == 0
    assert metrics["location_archive"] == {"skipped": True, "reason": "publish_artifacts_disabled"}
    assert metrics["ptg_corroboration_view_published"] is False
    importer.backfill_provider_directory_location_contacts.assert_not_awaited()
    importer.publish_provider_directory_location_address_keys.assert_not_awaited()
    importer.publish_provider_directory_location_archive.assert_not_awaited()
    importer.publish_provider_directory_address_corroboration_if_available.assert_not_awaited()


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
    monkeypatch.setattr(importer, "_probe_sources", AsyncMock(return_value=(1, 1, {"source_a"})))
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
    row = {column: None for column in columns}
    row.update(
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
        [row],
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
async def test_copy_mark_resource_rows_seen_appends_to_run_stage_without_indexed_merge(monkeypatch):
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
async def test_ensure_provider_directory_seen_table_drops_redundant_prefix_index(monkeypatch):
    statements: list[str] = []

    async def fake_status(sql, **_params):
        statements.append(sql)
        return 0

    monkeypatch.setattr(importer.db, "status", fake_status)

    await importer._ensure_provider_directory_import_seen_table("mrf")

    combined = "\n".join(statements)
    assert 'CREATE UNLOGGED TABLE IF NOT EXISTS "mrf"."provider_directory_import_seen"' in combined
    assert 'DROP INDEX IF EXISTS "mrf"."provider_directory_import_seen_source_idx"' in combined
    assert "CREATE INDEX IF NOT EXISTS provider_directory_import_seen_source_idx" not in combined


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


def test_resource_start_urls_partitions_scan_too_costly_resources():
    practitioner_urls = importer._resource_start_urls(
        {"api_base": importer.SCAN_PROVIDER_DIRECTORY_BASE},
        "Practitioner",
        page_count=25,
    )
    location_urls = importer._resource_start_urls(
        {"api_base": importer.SCAN_PROVIDER_DIRECTORY_BASE},
        "Location",
        page_count=25,
    )
    plan_urls = importer._resource_start_urls(
        {"api_base": importer.SCAN_PROVIDER_DIRECTORY_BASE},
        "InsurancePlan",
        page_count=25,
    )

    assert practitioner_urls[0] == (
        "https://providerdirectory.scanhealthplan.com/Practitioner?_count=25&family=AA"
    )
    assert practitioner_urls[-1] == (
        "https://providerdirectory.scanhealthplan.com/Practitioner?_count=25&family=ZZ"
    )
    assert len(practitioner_urls) == 26 * 26
    assert location_urls[:2] == [
        "https://providerdirectory.scanhealthplan.com/Location?_count=25&name=A",
        "https://providerdirectory.scanhealthplan.com/Location?_count=25&name=B",
    ]
    assert len(location_urls) == 26
    assert plan_urls == ["https://providerdirectory.scanhealthplan.com/InsurancePlan?_count=25"]


@pytest.mark.asyncio
async def test_fetch_resource_rows_continues_after_scan_partition_error(monkeypatch):
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

    result = await importer._fetch_resource_rows(
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
    assert result is not None
    assert result.model is ProviderDirectoryPractitionerRole
    assert result.rows == []
    assert result.complete is False
    assert result.error == importer.SCAN_PRACTITIONER_ROLE_REVERSE_LOOKUP_ERROR
    assert result.fetch_mode == "source_specific_deferred"


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
async def test_import_resources_fetches_scan_practitioner_roles_after_practitioners(monkeypatch):
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

    resource_fetch_stats: dict[str, dict[str, Any]] = {}
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
        resource_fetch_stats=resource_fetch_stats,
    )

    assert counts == {"PractitionerRole": 1, "Practitioner": 1}
    assert [model for model, _rows in upserts] == [
        ProviderDirectoryPractitioner,
        ProviderDirectoryPractitionerRole,
    ]
    assert role_calls == [
        "https://providerdirectory.scanhealthplan.com/PractitionerRole?practitioner=Practitioner%2Fprac-1&_count=25"
    ]
    assert resource_fetch_stats["PractitionerRole"]["sources_attempted"] == 1
    assert resource_fetch_stats["PractitionerRole"]["rows_fetched"] == 1


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


def test_bulk_export_start_url_uses_base_export_operation():
    url = importer._bulk_export_start_url(
        {"api_base": "https://example.test/fhir/"},
        "PractitionerRole",
    )

    assert url == (
        "https://example.test/fhir/$export?_type=PractitionerRole"
        "&_outputFormat=application%2Ffhir%2Bndjson"
    )


def test_bulk_export_pre_stream_failures_fall_back_to_paged_reads():
    assert importer._bulk_export_pre_stream_should_fallback(500, None) is True
    assert importer._bulk_export_pre_stream_should_fallback(401, None) is True
    assert importer._bulk_export_pre_stream_should_fallback(None, "timeout") is True
    assert importer._bulk_export_pre_stream_should_fallback(202, None) is False
    assert importer._bulk_export_pre_stream_should_fallback(200, None) is False


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


def test_bulk_export_status_payload_and_error_detection():
    assert importer._bulk_export_status_payload(
        {"transactionTime": "2026-06-30T00:00:00Z", "output": []}
    )
    assert not importer._bulk_export_status_payload(
        {"resourceType": "Bundle", "entry": []}
    )
    assert (
        importer._bulk_export_payload_error(
            {"error": [{"type": "OperationOutcome", "responseCode": 500}]}
        )
        == "bulk_export_error_http_500"
    )


@pytest.mark.asyncio
async def test_fetch_resource_rows_uses_bulk_export_when_available(monkeypatch):
    async def fake_bulk_export(source, resource_type, **kwargs):
        assert source["source_id"] == "source_a"
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

    result = await importer._fetch_resource_rows(
        {"source_id": "source_a", "api_base": "https://example.test/fhir"},
        "Practitioner",
        per_resource_limit=10,
        page_limit=1,
        page_count=25,
        timeout=3,
        run_id="run_1",
        bulk_export=True,
    )

    assert result is not None
    assert result.fetch_mode == "bulk_export"
    assert result.rows[0]["resource_id"] == "prac-1"
    stats: dict[str, dict[str, Any]] = {}
    importer._record_resource_fetch_stats(stats, "Practitioner", result)
    assert stats["Practitioner"]["bulk_export_sources"] == 1


@pytest.mark.asyncio
async def test_fetch_resource_rows_falls_back_when_bulk_export_returns_non_bulk_200(monkeypatch):
    async def fake_bulk_http_get_json(*_args, **kwargs):
        assert kwargs["prefer_async"] is True
        return 200, {}, {"resourceType": "Bundle", "entry": []}, None

    async def fake_fetch_json(url, *, timeout):
        assert url == "https://example.test/fhir/Practitioner?_count=25"
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

    result = await importer._fetch_resource_rows(
        {"source_id": "source_a", "api_base": "https://example.test/fhir"},
        "Practitioner",
        per_resource_limit=1,
        page_limit=1,
        page_count=25,
        timeout=3,
        run_id="run_1",
        bulk_export=True,
    )

    assert result is not None
    assert result.fetch_mode == "paged"
    assert result.rows[0]["resource_id"] == "prac-1"


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

    result = await importer._fetch_resource_rows(
        {"source_id": "source_a", "api_base": "https://example.test/fhir"},
        "Practitioner",
        per_resource_limit=1,
        page_limit=1,
        page_count=25,
        timeout=3,
        run_id="run_1",
        bulk_export=True,
    )

    assert result is not None
    assert result.fetch_mode == "bulk_export"
    assert result.error == "bulk_export_timeout"
    assert result.complete is False
    assert result.next_url_remaining is True
    stats: dict[str, dict[str, Any]] = {}
    importer._record_resource_fetch_stats(stats, "Practitioner", result)
    assert stats["Practitioner"]["bulk_export_sources"] == 1
    assert stats["Practitioner"]["sources_failed"] == 1


@pytest.mark.asyncio
async def test_fetch_resource_rows_falls_back_when_bulk_export_unsupported(monkeypatch):
    async def fake_bulk_export(*_args, **_kwargs):
        return None

    async def fake_fetch_json(url, *, timeout):
        assert url == "https://example.test/fhir/Practitioner?_count=25"
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

    result = await importer._fetch_resource_rows(
        {"source_id": "source_a", "api_base": "https://example.test/fhir"},
        "Practitioner",
        per_resource_limit=1,
        page_limit=1,
        page_count=25,
        timeout=3,
        run_id="run_1",
        bulk_export=True,
    )

    assert result is not None
    assert result.fetch_mode == "paged"
    assert result.rows[0]["resource_id"] == "prac-1"


@pytest.mark.asyncio
async def test_fetch_resource_rows_resolves_relative_next_links(monkeypatch):
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

    result = await importer._fetch_resource_rows(
        {"source_id": "source_a", "api_base": "https://example.test/fhir"},
        "Practitioner",
        per_resource_limit=2,
        page_limit=2,
        page_count=10,
        timeout=3,
        run_id="run_1",
    )

    assert result is not None
    assert result.model is ProviderDirectoryPractitioner
    assert result.complete is True
    assert [row["resource_id"] for row in result.rows] == ["prac-1", "prac-2"]
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

    result = await importer._fetch_resource_rows(
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

    assert result is not None
    assert result.complete is True
    assert [row["resource_id"] for row in result.rows] == ["prac-1"]
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

    result = await importer._fetch_resource_rows(
        {"source_id": "source_a", "api_base": "https://example.test/fhir"},
        "Location",
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_1",
    )

    assert result is not None
    assert result.complete is True
    assert result.pages_fetched == 3
    assert [row["resource_id"] for row in result.rows] == ["loc-1", "loc-2", "loc-3"]
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

    result = await importer._fetch_resource_rows(
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

    assert result is not None
    assert result.complete is True
    assert result.rows == []
    assert result.rows_fetched == 4
    assert result.rows_written == 4
    assert batches == [["prac-1-a", "prac-1-b", "prac-2-a"], ["prac-2-b"]]
    stats: dict[str, dict[str, Any]] = {}
    importer._record_resource_fetch_stats(stats, "Practitioner", result)
    assert stats["Practitioner"]["sources_empty"] == 0


@pytest.mark.asyncio
async def test_import_resources_deletes_stale_rows_only_after_complete_scan(monkeypatch):
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

    stats: dict[str, dict[str, Any]] = {}
    stale_counts: dict[str, int] = {}
    counts = await importer._import_resources(
        [{"source_id": "source_a", "api_base": "https://example.test/fhir"}],
        resources=["Location", "Practitioner"],
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_1",
        resource_fetch_stats=stats,
        stale_counts=stale_counts,
        stale_cleanup=True,
    )

    assert counts == {"Location": 1, "Practitioner": 1}
    assert stale_counts == {"Location": 4}
    assert len(deletes) == 2
    assert '"provider_directory_source_resource"' in deletes[0][0]
    assert '"provider_directory_location"' in deletes[1][0]
    assert "provider_directory_import_seen" in deletes[0][0]
    assert "provider_directory_import_seen" in deletes[1][0]
    assert deletes[0][1] == {"source_id": "source_a", "run_id": "run_1", "resource_type": "Location"}
    assert deletes[1][1] == {"source_id": "source_a", "run_id": "run_1", "resource_type": "Location"}
    assert stats["Location"]["sources_completed"] == 1
    assert stats["Practitioner"]["sources_bounded"] == 1


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

    stats: dict[str, dict[str, Any]] = {}
    counts = await importer._import_resources(
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
        resource_fetch_stats=stats,
        source_concurrency=2,
    )

    assert counts == {"Location": 3}
    assert concurrency_by_name["max_active"] == 2
    assert stats["Location"]["sources_completed"] == 3
    assert stats["Location"]["rows_fetched"] == 3


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
async def test_import_resources_fetches_duplicate_base_once_and_fans_out_rows(monkeypatch):
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

    stats: dict[str, dict[str, Any]] = {}
    counts = await importer._import_resources(
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
        resource_fetch_stats=stats,
    )

    assert fetch_calls == [("source_a", "Location")]
    assert sorted(upserted_source_ids) == ["source_a", "source_b"]
    assert counts == {"Location": 2}
    assert stats["Location"]["sources_attempted"] == 2
    assert stats["Location"]["sources_completed"] == 2


def test_selected_resources_rejects_unknown_resource():
    assert "Endpoint" in importer._selected_resources(None)
    assert importer._selected_resources("InsurancePlan,Location") == ["InsurancePlan", "Location"]
    with pytest.raises(ValueError, match="Unsupported Provider Directory FHIR resources"):
        importer._selected_resources("InsurancePlan,Patient")


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
    expected = {"canonical_rows": 2, "source_edge_rows": 3, "resources": {"Location": {}}}
    backfill = AsyncMock(return_value=expected)

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

    assert result == expected
    backfill.assert_awaited_once_with(resources="Location")


@pytest.mark.asyncio
async def test_process_data_contact_backfill_only_skips_seed_resolution(monkeypatch):
    expected = {"location_contact_rows_updated": 7}
    backfill = AsyncMock(return_value=expected)

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

    assert result == expected
    backfill.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_process_data_publish_artifacts_only_skips_seed_resolution(monkeypatch):
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(importer, "_mark_provider_directory_progress", AsyncMock())
    monkeypatch.setattr(
        importer,
        "backfill_provider_directory_location_contacts",
        AsyncMock(return_value={"location_contact_rows_updated": 5}),
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

    result = await importer.process_data(
        {"context": {}},
        {"publish_artifacts_only": True, "run_id": "run_publish"},
    )

    assert result["publish_artifacts_only"] is True
    assert result["location_contacts_backfilled"] == {"location_contact_rows_updated": 5}
    assert result["location_address_keys_stamped"] == 3
    assert result["location_archive"] == {"inserted": 4, "provenance_updates": 1}
    assert result["network_catalog"] == {"rows": 8}
    assert result["publish_corroboration"] is False
    assert result["ptg_corroboration_view_published"] is False
    assert result["ptg_corroboration_view_skipped"] == {
        "reason": "publish_corroboration_disabled",
    }
    importer.backfill_provider_directory_location_contacts.assert_awaited_once()
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
    )
    importer.publish_provider_directory_network_catalog.assert_awaited_once_with(
        run_id=None,
        source_ids=[],
    )
    importer.publish_provider_directory_address_corroboration_if_available.assert_not_awaited()

def test_harness_fixture_case_and_report_rendering(tmp_path):
    fixture_case_result = harness._run_fixture_case()
    report = {
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
    harness.write_report(report, tmp_path)

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
    assert [item["case_id"] for item in report["results"]] == ["fixture-parser", "sql-typing"]
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

    assert importer._source_uses_known_onboarding_gateway(public_cigna_dict) is False
    assert importer._source_uses_known_onboarding_gateway(availity_primary_dict) is True


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
    monkeypatch.setattr(importer, "_mark_provider_directory_progress", AsyncMock())
    monkeypatch.setattr(
        importer,
        "backfill_provider_directory_location_contacts",
        AsyncMock(return_value={"location_contact_rows_updated": 2}),
    )
    key_publish = AsyncMock(return_value=3)
    archive_publish = AsyncMock(return_value={"inserted": 4, "provenance_updates": 5})
    overlay_publish = AsyncMock(return_value={"published": True, "rows": 6})
    network_catalog_publish = AsyncMock(return_value={"published": True, "rows": 7})
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

    assert metrics["location_address_keys_stamped"] == 3
    assert metrics["location_archive"]["inserted"] == 4
    assert metrics["address_overlay"] == {"published": True, "rows": 6}
    assert metrics["network_catalog"] == {"published": True, "rows": 7}
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
    assert sql.count("addr_key_v1(") == 1


def test_address_overlay_stage_index_names_are_hash_safe():
    stage_table = importer._address_overlay_stage_table_name("run_1")
    index_name = importer._address_overlay_index_name(stage_table, "source_record_idx")

    assert len(index_name) <= 63
    assert index_name.startswith("provider_directory_address_overlay_stage_")
    assert index_name.endswith("_idx") or "_" in index_name


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
    assert "regexp_replace(refs_raw.network_ref, '^.*/Organization/', '')" in sql
    assert "regexp_replace(refs_raw.network_ref, '^Organization/', '')" in sql
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
    monkeypatch.setattr(importer, "_network_catalog_missing_requirement", AsyncMock(return_value=None))
    status_calls: list[tuple[str, dict[str, Any]]] = []

    async def fake_status(sql, **params):
        sql_text = str(sql)
        status_calls.append((sql_text, params))
        if "WHERE NOT (source_id = ANY" in sql_text:
            return "INSERT 0 4"
        if "WITH refs_raw AS MATERIALIZED" in sql_text:
            return "INSERT 0 9"
        return "OK"

    class FakeTransaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(importer.db, "status", fake_status)
    monkeypatch.setattr(importer.db, "scalar", AsyncMock(return_value=13))
    monkeypatch.setattr(importer.db, "transaction", lambda: FakeTransaction())

    metrics = await importer.publish_provider_directory_network_catalog(
        "mrf",
        run_id="run_1",
        source_ids=["source_a"],
    )

    sql_calls = [sql for sql, _params in status_calls]
    joined_sql = "\n".join(sql_calls)
    stage_table = importer._network_catalog_stage_table_name("run_1")
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


@pytest.mark.asyncio
async def test_overlay_publish_uses_staged_swap(monkeypatch):
    monkeypatch.setattr(importer, "_address_overlay_missing_requirement", AsyncMock(return_value=None))
    status_calls: list[tuple[str, dict[str, Any]]] = []

    async def fake_status(sql, **params):
        sql_text = str(sql)
        status_calls.append((sql_text, params))
        if "WHERE NOT (source_id = ANY" in sql_text:
            return "INSERT 0 4"
        if "provider_directory_fhir:organization_address:" in sql_text:
            return "INSERT 0 6"
        if "provider_directory_fhir:practitioner_role:" in sql_text:
            return "INSERT 0 7"
        if "provider_directory_fhir:organization_affiliation:" in sql_text:
            return "INSERT 0 8"
        if "duplicate_rank > 1" in sql_text:
            return "DELETE 2"
        return "OK"

    class FakeTransaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(importer.db, "status", fake_status)
    monkeypatch.setattr(importer.db, "scalar", AsyncMock(return_value=10))
    monkeypatch.setattr(importer.db, "transaction", lambda: FakeTransaction())

    metrics = await importer.publish_provider_directory_address_overlay(
        "mrf",
        run_id="run_1",
        source_ids=["source_a"],
    )

    sql_calls = [sql for sql, _params in status_calls]
    joined_sql = "\n".join(sql_calls)
    stage_table = importer._address_overlay_stage_table_name("run_1")

    assert metrics["published"] is True
    assert metrics["rows"] == 10
    assert metrics["inserted"] == 21
    assert metrics["inserted_by_component"] == {
        "organization_address": 6,
        "practitioner_role": 7,
        "organization_affiliation": 8,
    }
    assert metrics["duplicates_removed"] == 2
    assert metrics["copied_existing"] == 4
    assert metrics["source_ids"] == ["source_a"]
    assert f'CREATE UNLOGGED TABLE "mrf"."{stage_table}"' in joined_sql
    stage_source_record_idx = importer._address_overlay_index_name(stage_table, "source_record_idx")
    stage_npi_idx = importer._address_overlay_index_name(stage_table, "npi_idx")
    assert f'CREATE UNIQUE INDEX IF NOT EXISTS "{stage_source_record_idx}"' in joined_sql
    assert f'CREATE INDEX IF NOT EXISTS "{stage_npi_idx}"' in joined_sql
    assert 'FROM "mrf"."provider_directory_address_overlay"' in joined_sql
    assert "WHERE NOT (source_id = ANY(CAST(:source_ids AS varchar[])))" in joined_sql
    assert "PARTITION BY source_record_id" in joined_sql
    assert "duplicate_rank > 1" in joined_sql
    assert f'ALTER TABLE "mrf"."{stage_table}" RENAME TO "provider_directory_address_overlay"' in joined_sql
    assert 'ALTER TABLE "mrf"."provider_directory_address_overlay" RENAME TO "provider_directory_address_overlay_old"' in joined_sql
    assert (
        f'ALTER INDEX IF EXISTS "mrf"."{stage_source_record_idx}" '
        'RENAME TO "provider_directory_address_overlay_source_record_idx"'
    ) in joined_sql
    assert "entity_address_unified" not in joined_sql


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
