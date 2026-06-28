# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import json
import importlib
from typing import Any
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects import postgresql

from db.models import (
    ProviderDirectoryCapability,
    ProviderDirectoryEndpoint,
    ProviderDirectoryInsurancePlan,
    ProviderDirectoryLocation,
    ProviderDirectoryPractitioner,
    ProviderDirectoryOrganization,
    ProviderDirectoryOrganizationAffiliation,
    ProviderDirectoryPractitionerRole,
    ProviderDirectorySource,
)
from scripts.research import provider_directory_fhir_harness as harness

importer = importlib.import_module("process.provider_directory_fhir")


def _stub_resource_import_metadata(monkeypatch):
    monkeypatch.setattr(importer, "_update_source_resource_import_metadata", AsyncMock())


def test_source_row_from_seed_normalizes_base_and_flags():
    row = importer._source_row_from_seed(  # pylint: disable=protected-access
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
    row = importer._source_row_from_seed(  # pylint: disable=protected-access
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
    assert row["metadata_json"]["provider_directory_override"] == "aetna_apif1_providerdirectory"
    assert row["metadata_json"]["provider_directory_previous_api_base"] == "https://fhir-ehr.cerner.com/r4/aetna"


def test_source_row_from_seed_overrides_alohr_public_app_base():
    row = importer._source_row_from_seed(  # pylint: disable=protected-access
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


def test_alohr_provider_rows_emit_practitioner_location_and_role():
    rows_by_model: dict[type, list[dict[str, Any]]] = {}

    importer._append_alohr_provider_rows(  # pylint: disable=protected-access
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
    address = importer._address(  # pylint: disable=protected-access
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
    address = importer._address(  # pylint: disable=protected-access
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


def test_provider_directory_location_address_key_sql_recovers_numeric_state_fips():
    sql = importer.provider_directory_location_address_key_sql("mrf")

    assert "WHEN '42' THEN 'PA'" in sql
    assert "normalized_state" in sql
    assert "normalized_country" in sql
    assert "WHEN UPPER(REGEXP_REPLACE(COALESCE((country_code)::varchar, ''), '[^A-Za-z0-9]+', '', 'g')) IN" in sql
    assert "'840', '001'" in sql
    assert 'LEFT JOIN "mrf"."geo_zip_lookup" AS geo' in sql
    assert "zip_restored_state" in sql
    assert "resolved_state" in sql
    assert "WHERE address_key IS NULL" in sql
    assert "OR zip5 IS NULL" in sql
    assert "state_name = COALESCE(keyed.restored_state_name, loc.state_name)" in sql
    assert "country_code = COALESCE(keyed.normalized_country, loc.country_code)" in sql


def test_provider_directory_location_address_key_sql_can_skip_zip_state_restore():
    sql = importer.provider_directory_location_address_key_sql("mrf", restore_state_from_zip=False)

    assert "geo_zip_lookup" not in sql
    assert "END AS zip_restored_state" in sql
    assert "NULL::varchar) AS resolved_state" in sql


def test_upsert_changed_row_predicate_ignores_run_metadata_columns():
    table = ProviderDirectoryLocation.__table__
    columns = [column.name for column in table.columns]
    primary_keys = [column.name for column in table.primary_key.columns]
    statement = importer.pg_insert(table).values(  # pylint: disable=protected-access
        {
            "source_id": "source_a",
            "resource_id": "loc-1",
            "first_line": "100 Main St",
            "last_seen_run_id": "run_2",
        }
    )

    predicate = importer._upsert_changed_row_predicate(  # pylint: disable=protected-access
        table,
        statement,
        columns,
        primary_keys,
    )
    sql = str(predicate.compile(dialect=postgresql.dialect()))

    assert "first_line" in sql
    assert "CAST" in sql
    assert "JSONB" in sql
    assert "last_seen_run_id" not in sql
    assert "observed_at" not in sql
    assert "updated_at" not in sql


def test_copy_stage_changed_where_ignores_run_metadata_columns():
    table = ProviderDirectoryLocation.__table__
    columns = [column.name for column in table.columns]
    primary_keys = [column.name for column in table.primary_key.columns]

    sql = importer._copy_stage_changed_where_sql(  # pylint: disable=protected-access
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
    written = await importer._upsert_resource_rows(  # pylint: disable=protected-access
        ProviderDirectoryLocation,
        rows,
        run_id="run_1",
        track_seen=True,
    )

    assert written == 1
    assert seen_calls == [(ProviderDirectoryLocation, rows, "run_1", {"seen_table": None})]
    assert upsert_calls == [(ProviderDirectoryLocation, rows, {"skip_unchanged": True})]


@pytest.mark.asyncio
async def test_import_alohr_graphql_source_group_writes_existing_resource_tables(monkeypatch):
    calls: list[tuple[str, str | None]] = []

    async def fake_fetch_page(query, root_key, item_key, *, next_token, timeout):  # pylint: disable=unused-argument
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

    source_ids, diagnostics, counts, linked_counts, stats, stale, stale_ready = await importer._import_alohr_graphql_source_group(  # pylint: disable=protected-access
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

    await importer._update_source_resource_import_metadata(  # pylint: disable=protected-access
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
    payload = importer._decode_json_body(  # pylint: disable=protected-access
        b'\xef\xbb\xbf{"resourceType":"CapabilityStatement","fhirVersion":"4.0.1"}'
    )

    assert payload == {"resourceType": "CapabilityStatement", "fhirVersion": "4.0.1"}


def test_candidate_metadata_urls_repairs_resource_and_provider_directory_suffixes():
    humana = importer._candidate_metadata_urls(  # pylint: disable=protected-access
        {"api_base": "https://fhir.humana.com/api/provider-directory/"}
    )
    maine = importer._candidate_metadata_urls(  # pylint: disable=protected-access
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
    assert importer._candidate_metadata_urls({"api_base": "UNCONFIRMED"}) == []  # pylint: disable=protected-access
    assert importer._candidate_metadata_urls({"api_base": "N/A"}) == []  # pylint: disable=protected-access


def test_provider_directory_credentials_resolve_env_headers_and_query(monkeypatch):
    monkeypatch.setenv("PAYER_DIRECTORY_KEY", "secret-key")
    monkeypatch.setenv("PAYER_DIRECTORY_CLIENT", "client-1")
    monkeypatch.setenv(
        importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV,  # pylint: disable=protected-access
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

    options = importer._credential_request_options_for_source(  # pylint: disable=protected-access
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


def test_provider_directory_credentials_resolve_oauth2_client_credentials(monkeypatch):
    monkeypatch.setenv("PAYER_DIRECTORY_CLIENT_ID", "client-id")
    monkeypatch.setenv("PAYER_DIRECTORY_CLIENT_SECRET", "client-secret")
    monkeypatch.setenv(
        importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV,  # pylint: disable=protected-access
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

    options = importer._credential_request_options_for_source(  # pylint: disable=protected-access
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
    importer._OAUTH_TOKEN_CACHE.clear()  # pylint: disable=protected-access
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
    monkeypatch.setattr(importer.urllib.request, "urlopen", fake_urlopen)

    oauth2 = {
        "token_url": "https://auth.example/token",
        "client_id": "env:PAYER_DIRECTORY_CLIENT_ID",
        "client_secret": "env:PAYER_DIRECTORY_CLIENT_SECRET",
        "scope": "system/*.read",
    }
    first = importer._fetch_oauth2_client_credentials_token_sync(oauth2)  # pylint: disable=protected-access
    second = importer._fetch_oauth2_client_credentials_token_sync(oauth2)  # pylint: disable=protected-access

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
        importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV,  # pylint: disable=protected-access
        json.dumps({"hosts": {"payer.example": {"headers": {"X-API-Key": "env:PAYER_DIRECTORY_KEY"}}}}),
    )

    options = importer._credential_request_options_for_source(  # pylint: disable=protected-access
        {"source_id": "source_a", "api_base": "https://payer.example/fhir"},
        "https://other.example/fhir/Location/1",
    )

    assert options == {"headers": {}, "query_params": {}, "descriptor": None}


@pytest.mark.asyncio
async def test_probe_sources_records_credential_descriptor_without_secret(monkeypatch):
    monkeypatch.setenv("PAYER_DIRECTORY_TOKEN", "secret-token")
    monkeypatch.setenv(
        importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV,  # pylint: disable=protected-access
        json.dumps({"sources": {"source_a": {"bearer_token": "env:PAYER_DIRECTORY_TOKEN"}}}),
    )

    async def fake_fetch_json_with_options(url, *, timeout, extra_headers=None, query_params=None):  # pylint: disable=unused-argument
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
    await importer._probe_sources(  # pylint: disable=protected-access
        [source],
        timeout=3,
        concurrency=1,
        run_id="run_1",
    )

    source_rows = next(rows for model, rows in upserts if model is ProviderDirectorySource)
    metadata = source_rows[0]["metadata_json"]
    assert metadata["credential"] == {
        "matched_by": ["sources:source_a"],
        "header_names": ["Authorization"],
        "query_param_names": [],
    }
    assert "secret-token" not in json.dumps(metadata)


@pytest.mark.asyncio
async def test_probe_source_classifies_declared_credentialed_non_fhir_as_auth_required(monkeypatch):
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV, raising=False)  # pylint: disable=protected-access
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_FILE_ENV, raising=False)  # pylint: disable=protected-access

    async def fake_fetch_source_json(source, url, *, timeout):  # pylint: disable=unused-argument
        return 200, None, None, 14

    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_source_json)

    probe, payload = await importer._probe_source(  # pylint: disable=protected-access
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
    assert probe["http_status"] == 200
    assert probe["credential"] is None
    assert "no matching credentials" in probe["error"]


@pytest.mark.asyncio
async def test_probe_source_classifies_known_onboarding_gateway_non_fhir_as_auth_required(monkeypatch):
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV, raising=False)  # pylint: disable=protected-access
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_FILE_ENV, raising=False)  # pylint: disable=protected-access

    async def fake_fetch_source_json(source, url, *, timeout):  # pylint: disable=unused-argument
        return 200, None, None, 14

    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_source_json)

    probe, payload = await importer._probe_source(  # pylint: disable=protected-access
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
    assert probe["http_status"] == 200
    assert probe["credential"] is None
    assert "no matching credentials" in probe["error"]


@pytest.mark.asyncio
async def test_probe_source_keeps_open_non_fhir_as_valid_non_fhir(monkeypatch):
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV, raising=False)  # pylint: disable=protected-access
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_FILE_ENV, raising=False)  # pylint: disable=protected-access

    async def fake_fetch_source_json(source, url, *, timeout):  # pylint: disable=unused-argument
        return 200, None, None, 14

    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch_source_json)

    probe, payload = await importer._probe_source(  # pylint: disable=protected-access
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
    async def fake_fetch_json(url, *, timeout):  # pylint: disable=unused-argument
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

    source = importer._source_row_from_seed(  # pylint: disable=protected-access
        {
            "id": "humana",
            "org_name": "Humana",
            "plan_name": "Medicare Advantage",
            "api_base": "https://fhir.humana.com/api/provider-directory/",
        }
    )

    probed, valid, valid_source_ids = await importer._probe_sources(  # pylint: disable=protected-access
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
    assert source_rows[0]["metadata_json"]["resolved_api_base_from"] == "https://fhir.humana.com/api/provider-directory"


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


def test_provider_directory_ptg_address_corroboration_sql_links_ptg_npi_address_to_fhir_roles():
    sql = importer.provider_directory_ptg_address_corroboration_sql("mrf")

    assert 'CREATE OR REPLACE VIEW "mrf"."ptg_provider_directory_address_corroboration" AS' in sql
    assert 'FROM "mrf"."ptg_address" p' in sql
    assert 'JOIN "mrf"."provider_directory_practitioner" practitioner' in sql
    assert "practitioner.npi = p.npi" in sql
    assert 'JOIN "mrf"."provider_directory_location" loc' in sql
    assert "COALESCE(role.location_refs::jsonb, '[]'::jsonb)" in sql
    assert "COALESCE(role.network_refs::jsonb, '[]'::jsonb) AS network_refs" in sql
    assert "COALESCE(affiliation.location_refs::jsonb, '[]'::jsonb)" in sql
    assert "COALESCE(affiliation.network_refs::jsonb, '[]'::jsonb) AS network_refs" in sql
    assert "role.location_ref IN (loc.resource_id, 'Location/' || loc.resource_id)" in sql
    assert "role.location_ref LIKE '%/Location/' || loc.resource_id" in sql
    assert "network_ref.value LIKE '%/Organization/' || network_org.resource_id" in sql
    assert "loc.address_key ~* '^[0-9a-f]{8}-" in sql
    assert "THEN loc.address_key::uuid" in sql
    assert "jsonb_build_object(" in sql
    assert "'matched_on'," in sql
    assert "'npi_address_key_role_location'" in sql
    assert "'npi_address_key_role_location_plan'" in sql
    assert "provider_directory_plan_context_matched" in sql
    assert "provider_directory_network_context_present" in sql
    assert "provider_directory_network_names" in sql
    assert "provider_directory_network_matches" in sql
    assert "provider_directory_insurance_plan_matches" in sql
    assert "insurance_plan.plan_identifier" in sql
    assert "insurance_plan.network_refs::jsonb" in sql
    assert '"provider_directory_organization" network_org' in sql
    assert "COALESCE(p.ptg_plan_array, ARRAY[]::varchar[])" in sql
    assert "'payer_directory_corroborated_location'" in sql
    assert "'provider_directory_address'" in sql
    assert "UNION ALL" in sql
    assert 'provider_directory_organization_affiliation' in sql


def test_provider_directory_ptg_address_corroboration_view_keeps_existing_column_order():
    sql = importer.provider_directory_ptg_address_corroboration_sql("mrf")

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


def test_provider_directory_ptg_address_corroboration_select_sql_returns_query_body():
    sql = importer.provider_directory_ptg_address_corroboration_select_sql("mrf")

    assert not sql.startswith("CREATE OR REPLACE VIEW")
    assert sql.startswith("WITH practitioner_role_locations AS")
    assert 'FROM "mrf"."ptg_address" p' in sql
    assert "provider_directory_network_names" in sql


@pytest.mark.asyncio
async def test_publish_provider_directory_ptg_address_corroboration_table_swaps_indexed_table(monkeypatch):
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

    result = await importer.publish_provider_directory_ptg_address_corroboration_table("mrf")
    joined = "\n".join(statements)

    assert result == {
        "published": True,
        "relation": '"mrf"."ptg_provider_directory_address_corroboration"',
        "rows": 7,
        "storage": "table",
    }
    assert 'DROP TABLE IF EXISTS "mrf"."pd_stage_corrob"' in joined
    assert 'CREATE UNLOGGED TABLE "mrf"."pd_stage_corrob" AS' in joined
    assert 'FROM "mrf"."ptg_address" p' in joined
    assert 'DROP VIEW "mrf"."ptg_provider_directory_address_corroboration"' in joined
    assert 'DROP TABLE "mrf"."ptg_provider_directory_address_corroboration"' in joined
    assert (
        'ALTER TABLE "mrf"."pd_stage_corrob" RENAME TO "ptg_provider_directory_address_corroboration"'
        in joined
    )
    assert '"mrf"."pd_ptg_corrob_lookup_idx"' in joined
    assert '"mrf"."pd_ptg_corrob_network_names_gin"' in joined
    assert 'ANALYZE "mrf"."ptg_provider_directory_address_corroboration"' in joined


def test_provider_directory_location_address_key_sql_uses_shared_canonical_functions():
    sql = importer.provider_directory_location_address_key_sql("mrf")

    assert 'UPDATE "mrf"."provider_directory_location" AS loc' in sql
    assert '"mrf".addr_key_v1(' in sql
    assert '"mrf".addr_zip5_norm_v1(postal_code)' in sql
    assert '"mrf".addr_state_code_v1(resolved_state)' in sql
    assert '"mrf".addr_city_norm_v1(city_name)' in sql
    assert "keyed.computed_address_key IS NOT NULL" in sql
    assert "address_key = keyed.computed_address_key::text" in sql


def test_reference_resource_key_handles_relative_and_absolute_fhir_refs():
    assert importer._reference_resource_key("Location/loc-1", "Location") == (  # pylint: disable=protected-access
        "Location",
        "loc-1",
    )
    assert importer._reference_resource_key(  # pylint: disable=protected-access
        "https://example.test/fhir/Practitioner/prac%7C1",
        "Practitioner",
    ) == ("Practitioner", "prac|1")
    assert importer._reference_resource_key("Organization/org-1", "Location") is None  # pylint: disable=protected-access


def test_sql_ref_matches_resource_accepts_absolute_url_suffixes():
    sql = importer._sql_ref_matches_resource("refs.ref", "Organization", "org.resource_id")  # pylint: disable=protected-access

    assert "refs.ref IN (org.resource_id, 'Organization/' || org.resource_id)" in sql
    assert "refs.ref LIKE '%/Organization/' || org.resource_id" in sql


def test_linked_resource_candidate_urls_use_network_endpoint_for_network_refs():
    urls = importer._linked_resource_candidate_urls(  # pylint: disable=protected-access
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
    urls = importer._linked_resource_candidate_urls(  # pylint: disable=protected-access
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
    urls = importer._linked_resource_candidate_urls(  # pylint: disable=protected-access
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
    async def fake_fetch_json(url, *, timeout):  # pylint: disable=unused-argument
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
        upserts.append((model, rows))
        return len(rows)

    monkeypatch.setattr(importer, "_fetch_json", fake_fetch_json)
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)

    counts = await importer._import_linked_resource_rows(  # pylint: disable=protected-access
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
    assert [model for model, _rows in upserts] == [
        ProviderDirectoryPractitioner,
        ProviderDirectoryLocation,
        ProviderDirectoryInsurancePlan,
        ProviderDirectoryEndpoint,
    ]


@pytest.mark.asyncio
async def test_import_linked_resource_rows_fetches_network_organization_from_network_endpoint(monkeypatch):
    calls: list[str] = []

    async def fake_fetch_json(url, *, timeout):  # pylint: disable=unused-argument
        calls.append(url)
        if "type=ntwk" in url:
            return 200, {"resourceType": "Bundle", "entry": [{"resource": {"resourceType": "Organization", "id": "network-1", "name": "Gold Network"}}]}, None, 5
        return 404, None, None, 5

    upserts = []

    async def fake_upsert(model, rows, **_kwargs):
        upserts.append((model, rows))
        return len(rows)

    monkeypatch.setattr(importer, "_fetch_json", fake_fetch_json)
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)

    counts = await importer._import_linked_resource_rows(  # pylint: disable=protected-access
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
    assert upserts[0][0] is ProviderDirectoryOrganization
    assert upserts[0][1][0]["name"] == "Gold Network"


@pytest.mark.asyncio
async def test_import_resources_accumulates_linked_resource_counts(monkeypatch):
    _stub_resource_import_metadata(monkeypatch)

    async def fake_fetch_resource_rows(source, resource_type, **_kwargs):  # pylint: disable=unused-argument
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

    async def fake_import_linked(_source, _rows_by_resource, **_kwargs):
        return {"Location": 2}

    monkeypatch.setattr(importer, "_fetch_resource_rows", fake_fetch_resource_rows)
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)
    monkeypatch.setattr(importer, "_import_linked_resource_rows", fake_import_linked)

    linked_counts = {}
    counts = await importer._import_resources(  # pylint: disable=protected-access
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
    monkeypatch.setattr(importer, "_address_canon_functions_available", AsyncMock(return_value=True))
    table_exists = AsyncMock(return_value=True)
    monkeypatch.setattr(importer, "_table_exists", table_exists)
    status = AsyncMock(return_value=7)
    monkeypatch.setattr(importer.db, "status", status)

    stamped = await importer.publish_provider_directory_location_address_keys("mrf")

    assert stamped == 7
    table_exists.assert_awaited_once_with("mrf", "geo_zip_lookup")
    status.assert_awaited_once()
    assert 'UPDATE "mrf"."provider_directory_location" AS loc' in status.await_args.args[0]
    assert 'LEFT JOIN "mrf"."geo_zip_lookup" AS geo' in status.await_args.args[0]


def test_provider_directory_location_archive_stage_sql_filters_keyable_uuid_locations():
    sql = importer.provider_directory_location_archive_stage_sql("mrf", "provider_directory_location_archive_stage_test")

    assert 'CREATE UNLOGGED TABLE "mrf"."provider_directory_location_archive_stage_test" AS' in sql
    assert 'FROM "mrf"."provider_directory_location" AS loc' in sql
    assert "loc.address_key ~*" in sql
    assert "loc.address_key::uuid AS address_key" in sql
    assert "NOT IN ('UN', 'XX', 'ZZ', 'NULL', 'N/A')" in sql
    assert "IN ('US', 'USA', 'UNITEDSTATES', 'UNITEDSTATESOFAMERICA', '840', '001')" in sql
    assert "NULLIF(BTRIM(loc.first_line), '') IS NOT NULL" in sql
    assert "OR NULLIF(BTRIM(loc.city_name), '') IS NOT NULL" in sql
    assert "SELECT DISTINCT ON (address_key)" in sql


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
async def test_publish_provider_directory_ptg_address_corroboration_if_available_skips_without_ptg_address(
    monkeypatch,
):
    monkeypatch.setattr(importer, "_table_exists", AsyncMock(return_value=False))
    publish = AsyncMock()
    monkeypatch.setattr(importer, "publish_provider_directory_ptg_address_corroboration_table", publish)

    published = await importer.publish_provider_directory_ptg_address_corroboration_if_available("mrf")

    assert published is False
    publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_provider_directory_ptg_address_corroboration_if_available_publishes_when_ptg_address_exists(
    monkeypatch,
):
    monkeypatch.setattr(importer, "_table_exists", AsyncMock(return_value=True))
    publish = AsyncMock()
    monkeypatch.setattr(importer, "publish_provider_directory_ptg_address_corroboration_table", publish)

    published = await importer.publish_provider_directory_ptg_address_corroboration_if_available("mrf")

    assert published is True
    publish.assert_awaited_once_with("mrf")


@pytest.mark.asyncio
async def test_process_data_stamps_locations_and_publishes_corroboration_view(monkeypatch):
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(importer, "_upsert_rows", AsyncMock(return_value=1))
    monkeypatch.setattr(importer, "_import_resources", AsyncMock(return_value={"Location": 2}))
    monkeypatch.setattr(importer, "publish_provider_directory_location_address_keys", AsyncMock(return_value=3))
    monkeypatch.setattr(
        importer,
        "publish_provider_directory_location_archive",
        AsyncMock(return_value={"inserted": 4, "provenance_updates": 1}),
    )
    monkeypatch.setattr(
        importer,
        "publish_provider_directory_ptg_address_corroboration_if_available",
        AsyncMock(return_value=True),
    )

    metrics = await importer.process_data(
        {"context": {}},
        {
            "test": True,
            "probe": False,
            "import_resources": True,
            "resources": "Location",
            "publish_artifacts": True,
        },
    )

    assert metrics["resource_rows"] == {"Location": 2}
    assert metrics["sources_import_attempted"] == 1
    assert metrics["location_address_keys_stamped"] == 3
    assert metrics["location_archive"] == {"inserted": 4, "provenance_updates": 1}
    assert metrics["ptg_corroboration_view_published"] is True
    importer.publish_provider_directory_location_address_keys.assert_awaited_once()
    importer.publish_provider_directory_location_archive.assert_awaited_once()
    importer.publish_provider_directory_ptg_address_corroboration_if_available.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_data_skips_artifact_publish_for_targeted_resource_import(monkeypatch):
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(importer, "_upsert_rows", AsyncMock(return_value=1))
    monkeypatch.setattr(importer, "_import_resources", AsyncMock(return_value={"PractitionerRole": 2}))
    monkeypatch.setattr(importer, "publish_provider_directory_location_address_keys", AsyncMock(return_value=3))
    monkeypatch.setattr(
        importer,
        "publish_provider_directory_location_archive",
        AsyncMock(return_value={"inserted": 4}),
    )
    monkeypatch.setattr(
        importer,
        "publish_provider_directory_ptg_address_corroboration_if_available",
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
    assert metrics["location_address_keys_stamped"] == 0
    assert metrics["location_archive"] == {"skipped": True, "reason": "publish_artifacts_disabled"}
    assert metrics["ptg_corroboration_view_published"] is False
    importer.publish_provider_directory_location_address_keys.assert_not_awaited()
    importer.publish_provider_directory_location_archive.assert_not_awaited()
    importer.publish_provider_directory_ptg_address_corroboration_if_available.assert_not_awaited()


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
        "publish_provider_directory_ptg_address_corroboration_if_available",
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
    import_resources.assert_awaited_once()
    assert import_resources.await_args.args[0][0]["source_id"] == "source_a"


def test_dedupe_rows_by_primary_key_keeps_last_live_fhir_duplicate():
    rows = [
        {"source_id": "source_a", "resource_id": "prac-1", "full_name": "Old"},
        {"source_id": "source_a", "resource_id": "prac-1", "full_name": "New"},
        {"source_id": "source_a", "resource_id": None, "full_name": "Skip"},
        {"source_id": "source_a", "resource_id": "prac-2", "full_name": "Other"},
    ]

    deduped = importer._dedupe_rows_by_primary_key(["source_id", "resource_id"], rows)  # pylint: disable=protected-access

    assert deduped == [
        {"source_id": "source_a", "resource_id": "prac-1", "full_name": "New"},
        {"source_id": "source_a", "resource_id": "prac-2", "full_name": "Other"},
    ]


def test_max_rows_per_statement_stays_under_asyncpg_parameter_limit():
    assert importer._max_rows_per_statement(1) == 500  # pylint: disable=protected-access
    assert importer._max_rows_per_statement(100) == 300  # pylint: disable=protected-access
    assert importer._max_rows_per_statement(40000) == 1  # pylint: disable=protected-access


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

    written = await importer._copy_upsert_rows(  # pylint: disable=protected-access
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

    where_sql = importer._copy_upsert_changed_where_sql(  # pylint: disable=protected-access
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

    written = await importer._copy_mark_resource_rows_seen(  # pylint: disable=protected-access
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

    written = await importer._copy_mark_resource_rows_seen(  # pylint: disable=protected-access
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
async def test_ensure_provider_directory_seen_table_drops_redundant_prefix_index(monkeypatch):
    statements: list[str] = []

    async def fake_status(sql, **_params):
        statements.append(sql)
        return 0

    monkeypatch.setattr(importer.db, "status", fake_status)

    await importer._ensure_provider_directory_import_seen_table("mrf")  # pylint: disable=protected-access

    combined = "\n".join(statements)
    assert 'CREATE UNLOGGED TABLE IF NOT EXISTS "mrf"."provider_directory_import_seen"' in combined
    assert 'DROP INDEX IF EXISTS "mrf"."provider_directory_import_seen_source_idx"' in combined
    assert "CREATE INDEX IF NOT EXISTS provider_directory_import_seen_source_idx" not in combined


def test_resource_start_url_prefers_endpoint_and_preserves_existing_count():
    url = importer._resource_start_url(  # pylint: disable=protected-access
        {
            "api_base": "https://example.test/fhir",
            "endpoint_practitioner": "https://payer.example/custom/Practitioner?active=true&_count=20",
        },
        "Practitioner",
        page_count=100,
    )

    assert url == "https://payer.example/custom/Practitioner?active=true&_count=20"


def test_resource_start_url_resolves_relative_endpoint_and_adds_count():
    url = importer._resource_start_url(  # pylint: disable=protected-access
        {
            "api_base": "https://example.test/fhir/base",
            "endpoint_location": "Location?address-state=CA",
        },
        "Location",
        page_count=250,
    )

    assert url == "https://example.test/fhir/base/Location?address-state=CA&_count=100"


@pytest.mark.asyncio
async def test_fetch_resource_rows_resolves_relative_next_links(monkeypatch):
    calls: list[str] = []

    async def fake_fetch_json(url, *, timeout):  # pylint: disable=unused-argument
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

    result = await importer._fetch_resource_rows(  # pylint: disable=protected-access
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

    async def fake_fetch_json(url, *, timeout):  # pylint: disable=unused-argument
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

    result = await importer._fetch_resource_rows(  # pylint: disable=protected-access
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

    async def fake_fetch_json(url, *, timeout):  # pylint: disable=unused-argument
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

    result = await importer._fetch_resource_rows(  # pylint: disable=protected-access
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
    async def fake_fetch_json(url, *, timeout):  # pylint: disable=unused-argument
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

    result = await importer._fetch_resource_rows(  # pylint: disable=protected-access
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
    importer._record_resource_fetch_stats(stats, "Practitioner", result)  # pylint: disable=protected-access
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
    counts = await importer._import_resources(  # pylint: disable=protected-access
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
    assert len(deletes) == 1
    assert '"provider_directory_location"' in deletes[0][0]
    assert "provider_directory_import_seen" in deletes[0][0]
    assert deletes[0][1] == {"source_id": "source_a", "run_id": "run_1", "resource_type": "Location"}
    assert stats["Location"]["sources_completed"] == 1
    assert stats["Practitioner"]["sources_bounded"] == 1


@pytest.mark.asyncio
async def test_import_resources_honors_source_concurrency(monkeypatch):
    _stub_resource_import_metadata(monkeypatch)

    active = 0
    max_active = 0

    async def fake_fetch_resource_rows(source, resource_type, **_kwargs):
        nonlocal active, max_active
        assert resource_type == "Location"
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.01)
        active -= 1
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
    counts = await importer._import_resources(  # pylint: disable=protected-access
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
    assert max_active == 2
    assert stats["Location"]["sources_completed"] == 3
    assert stats["Location"]["rows_fetched"] == 3


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

    async def fake_upsert(_model, rows, **_kwargs):
        upserted_source_ids.extend(row["source_id"] for row in rows)
        return len(rows)

    monkeypatch.setattr(importer, "_fetch_resource_rows", fake_fetch_resource_rows)
    monkeypatch.setattr(importer, "_upsert_rows", fake_upsert)

    stats: dict[str, dict[str, Any]] = {}
    counts = await importer._import_resources(  # pylint: disable=protected-access
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
    assert "Endpoint" in importer._selected_resources(None)  # pylint: disable=protected-access
    assert importer._selected_resources("InsurancePlan,Location") == ["InsurancePlan", "Location"]  # pylint: disable=protected-access
    with pytest.raises(ValueError, match="Unsupported Provider Directory FHIR resources"):
        importer._selected_resources("InsurancePlan,Patient")  # pylint: disable=protected-access


def test_harness_fixture_case_and_report_rendering(tmp_path):
    result = harness._run_fixture_case()  # pylint: disable=protected-access
    report = {
        "generated_at": "2026-06-28T00:00:00Z",
        "overall_status": "succeeded",
        "results": [result.to_json()],
    }
    harness.write_report(report, tmp_path)

    assert result.status == "succeeded"
    assert result.metrics["supported_resources"] == ["Endpoint", "InsurancePlan", "PractitionerRole"]
    assert result.metrics["resource_counts"]["provider_directory_endpoint"] == 1
    assert (tmp_path / "report.json").exists()
    assert (tmp_path / "report.md").exists()
    assert json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))["overall_status"] == "succeeded"
