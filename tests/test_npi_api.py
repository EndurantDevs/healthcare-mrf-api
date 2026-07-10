# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import types
from unittest.mock import AsyncMock

import pytest
import sanic.exceptions

from api.endpoint import npi as npi_module
from api.endpoint.npi import npi_index_status


@pytest.fixture
def uhc_provider_directory_alias_fixture():
    endpoint_id = "pd_endpoint_uhc_public"
    canonical_api_base = "https://public.providerexpress.com/providerdirectory"
    source_ids = [f"pdfhir_uhc_{alias_number:02d}" for alias_number in range(31)]
    source_record_ids = [
        (
            "provider_directory_fhir:practitioner_role:"
            f"{source_id}:role-{alias_number:02d}:location-{alias_number:02d}"
        )
        for alias_number, source_id in enumerate(source_ids)
    ]
    source_detail_map = {
        source_id: {
            "source": "provider_directory_fhir",
            "source_id": source_id,
            "endpoint_id": endpoint_id,
            "canonical_api_base": canonical_api_base,
            "org_name": f"UnitedHealthcare catalog alias {alias_number:02d}",
            "plan_name": f"Choice Plus catalog label {alias_number:02d}",
            "insurance_plan_refs": [],
            "network_refs": [],
        }
        for alias_number, source_id in enumerate(source_ids)
    }
    return {
        "endpoint_id": endpoint_id,
        "canonical_api_base": canonical_api_base,
        "source_ids": source_ids,
        "source_record_ids": source_record_ids,
        "source_detail_map": source_detail_map,
    }


@pytest.fixture
def provider_directory_plan_network_fixture():
    source_id, role_id, plan_id = "pdfhir_example", "role-100", "plan-200"
    network_id, location_id = "network-300", "location-400"
    return {
        "role": {
            "key": (source_id, role_id), "plan_ref": f"InsurancePlan/{plan_id}",
            "location_ref": f"Location/{location_id}",
        },
        "address": {
            "type": "practice",
            "address_key": "00000000-0000-0000-0000-000000000111",
            "address_precision": "street",
            "first_line": "100 Main St",
            "city_name": "Chicago", "state_name": "IL", "postal_code": "60601",
            "country_code": "US", "lat": 41.8818, "long": -87.6232, "checksum": 100,
            "address_sources": ["provider_directory_fhir"],
            "source_record_ids": [f"provider_directory_fhir:practitioner_role:{source_id}:{role_id}:{location_id}"],
            "aca_plan_array": ["H1234-001"],
            "aca_network_array": ["Example Choice Network"],
        },
        "source_detail_map": {
            source_id: {
                "source": "provider_directory_fhir", "source_id": source_id,
                "endpoint_id": "pd_endpoint_example", "canonical_api_base": "https://example.test/fhir",
                "org_name": "Example Health", "plan_name": "Example Directory",
            }
        },
        "role_evidence_map": {
            (source_id, role_id): {
                "insurance_plans": [{"resource_type": "InsurancePlan", "resource_id": plan_id, "identifier": "H1234-001"}],
                "networks": [
                    {
                        "resource_type": "Organization", "resource_id": network_id,
                        "name": "Example Choice Network", "reference": f"Organization/{network_id}",
                        "provenance": "provider_directory_network_catalog",
                    }
                ],
            }
        },
    }


@pytest.mark.asyncio
async def test_npi_index(monkeypatch):
    async def fake_counts():
        return 10, 5

    monkeypatch.setattr("api.endpoint.npi._compute_npi_counts", fake_counts)

    request = types.SimpleNamespace(
        app=types.SimpleNamespace(config={"RELEASE": "test", "ENVIRONMENT": "test"})
    )

    response = await npi_index_status(request)
    payload = json.loads(response.body)
    assert payload["product_count"] == 10
    assert payload["import_log_errors"] == 5


@pytest.mark.asyncio
async def test_get_npi_includes_other_names(monkeypatch):
    async def fake_build(_npi, **_kwargs):
        return {
            "npi": _npi,
            "taxonomy_list": [],
            "taxonomy_group_list": [],
            "address_list": [],
            "do_business_as": ["DBA"],
        }

    monkeypatch.setattr(npi_module, "_build_npi_details", fake_build)
    monkeypatch.setattr(
        npi_module,
        "_fetch_other_names",
        AsyncMock(
            return_value=[
                {
                    "npi": 1518379601,
                    "checksum": 1,
                    "other_provider_identifier": "ALT NAME",
                    "other_provider_identifier_type_code": "05",
                    "other_provider_identifier_state": None,
                    "other_provider_identifier_issuer": None,
                }
            ]
        ),
    )

    request = types.SimpleNamespace(args={})
    response = await npi_module.get_npi(request, "1518379601")
    payload = json.loads(response.body)
    assert "other_name_list" in payload
    assert payload["other_name_list"][0]["other_provider_identifier"] == "ALT NAME"
    assert payload["do_business_as"] == ["DBA"]


@pytest.mark.asyncio
async def test_get_npi_uses_other_names_for_dba(monkeypatch):
    async def fake_build(_npi, **_kwargs):
        return {
            "npi": _npi,
            "taxonomy_list": [],
            "taxonomy_group_list": [],
            "address_list": [],
            "do_business_as": [],
        }

    other_names = [
        {
            "other_provider_identifier": "Name One",
            "other_provider_identifier_type_code": "3",
            "other_provider_identifier_state": None,
            "other_provider_identifier_issuer": None,
        },
        {
            "other_provider_identifier": "Name Two",
            "other_provider_identifier_type_code": "3",
            "other_provider_identifier_state": None,
            "other_provider_identifier_issuer": None,
        },
        {
            "other_provider_identifier": "Ignore",
            "other_provider_identifier_type_code": "1",
            "other_provider_identifier_state": None,
            "other_provider_identifier_issuer": None,
        },
    ]

    monkeypatch.setattr(npi_module, "_build_npi_details", fake_build)
    monkeypatch.setattr(npi_module, "_fetch_other_names", AsyncMock(return_value=other_names))

    request = types.SimpleNamespace(args={})
    response = await npi_module.get_npi(request, "1518379601")
    payload = json.loads(response.body)
    assert payload["do_business_as"] == ["Name One", "Name Two"]
    assert payload["other_name_list"] == other_names


@pytest.mark.asyncio
async def test_fetch_other_names_deduplicates(monkeypatch):
    class FakeRow:
        def __init__(self, payload):
            self._payload = payload

        def to_json_dict(self):
            return dict(self._payload)

    class FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def scalars(self):
            return self._rows

    rows = [
        FakeRow(
            {
                "npi": 1,
                "checksum": 123,
                "other_provider_identifier": "Name One",
                "other_provider_identifier_type_code": "3",
                "other_provider_identifier_state": None,
                "other_provider_identifier_issuer": None,
            }
        ),
        FakeRow(
            {
                "npi": 1,
                "checksum": 123,
                "other_provider_identifier": "Name One",
                "other_provider_identifier_type_code": "3",
                "other_provider_identifier_state": None,
                "other_provider_identifier_issuer": None,
            }
        ),
    ]

    fake_result = FakeResult(rows)
    monkeypatch.setattr(npi_module.db, "execute", AsyncMock(return_value=fake_result))

    result = await npi_module._fetch_other_names(1)
    assert result == [
        {
            "other_provider_identifier": "Name One",
            "other_provider_identifier_type_code": "3",
            "other_provider_identifier_state": None,
            "other_provider_identifier_issuer": None,
        }
    ]


@pytest.mark.asyncio
async def test_provider_directory_sources_collapse_uhc_aliases(
    monkeypatch,
    uhc_provider_directory_alias_fixture,
):
    alias_fixture = uhc_provider_directory_alias_fixture
    stable_source_record_ids = list(alias_fixture["source_record_ids"])
    provider_address_map = {
        "source_record_ids": list(stable_source_record_ids),
        "plans_network_array": [],
    }
    fetch_source_details = AsyncMock(return_value=alias_fixture["source_detail_map"])
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_directory_source_detail_map",
        fetch_source_details,
    )

    await npi_module._attach_provider_directory_source_details([provider_address_map])

    fetch_source_details.assert_awaited_once_with(alias_fixture["source_ids"], session=None)
    assert provider_address_map["source_record_ids"] == stable_source_record_ids
    assert provider_address_map["plans_network_array"] == []
    endpoint_sources = provider_address_map["provider_directory_sources"]
    assert len(endpoint_sources) == 1
    endpoint_provenance = endpoint_sources[0]
    assert endpoint_provenance["endpoint_id"] == alias_fixture["endpoint_id"]
    assert endpoint_provenance["catalog_aliases_verified"] is False
    assert [
        alias["source_id"] for alias in endpoint_provenance["catalog_aliases"]
    ] == alias_fixture["source_ids"]
    assert set(endpoint_provenance) == {
        "source",
        "endpoint_id",
        "catalog_aliases_verified",
        "catalog_aliases",
    }


def test_provider_directory_legacy_aliases_group_by_canonical_base():
    canonical_api_base = "https://legacy.example/fhir/"
    source_detail_map = {
        source_id: {
            "source_id": source_id,
            "endpoint_id": None,
            "canonical_api_base": canonical_api_base,
            "org_name": "Legacy catalog organization",
            "plan_name": plan_name,
        }
        for source_id, plan_name in (
            ("pdfhir_legacy_a", "Legacy label A"),
            ("pdfhir_legacy_b", "Legacy label B"),
        )
    }

    endpoint_sources = npi_module._provider_directory_endpoint_provenance(
        list(source_detail_map),
        source_detail_map,
    )

    assert len(endpoint_sources) == 1
    assert "endpoint_id" not in endpoint_sources[0]
    assert "canonical_api_base" not in endpoint_sources[0]
    assert len(endpoint_sources[0]["catalog_aliases"]) == 2


def test_provider_directory_owner_source_expands_all_endpoint_aliases(
    uhc_provider_directory_alias_fixture,
):
    alias_fixture = uhc_provider_directory_alias_fixture

    endpoint_sources = npi_module._provider_directory_endpoint_provenance(
        [alias_fixture["source_ids"][0]],
        alias_fixture["source_detail_map"],
    )

    assert len(endpoint_sources) == 1
    assert [
        alias["source_id"] for alias in endpoint_sources[0]["catalog_aliases"]
    ] == alias_fixture["source_ids"]


@pytest.mark.asyncio
async def test_provider_directory_source_fetch_keeps_endpoint_identity(monkeypatch):
    class SourceDetailResult:
        def all(self):
            return [
                {
                    "source_id": "pdfhir_uhc_00",
                    "endpoint_id": "pd_endpoint_uhc_public",
                    "canonical_api_base": "https://public.providerexpress.com/providerdirectory",
                    "org_name": "UnitedHealthcare catalog alias",
                    "plan_name": "Choice Plus catalog label",
                }
            ]

    execute_source_query = AsyncMock(return_value=SourceDetailResult())
    monkeypatch.setattr(npi_module, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(npi_module, "_execute_stmt", execute_source_query)

    source_details = await npi_module._fetch_provider_directory_source_detail_map(
        ["pdfhir_uhc_00"]
    )

    assert source_details["pdfhir_uhc_00"]["endpoint_id"] == "pd_endpoint_uhc_public"
    assert source_details["pdfhir_uhc_00"]["canonical_api_base"] == (
        "https://public.providerexpress.com/providerdirectory"
    )
    source_query = execute_source_query.await_args.args[0]
    selected_column_names = {column.name for column in source_query.selected_columns}
    assert {"endpoint_id", "canonical_api_base"} <= selected_column_names


def test_provider_directory_role_evidence_sql_is_keyed_and_bounded():
    sql = npi_module._provider_directory_role_evidence_sql("mrf", has_catalog=True)

    assert "unnest(CAST(:source_ids AS varchar[]), CAST(:role_ids AS varchar[]))" in sql
    assert "role.source_id = requested.source_id AND role.resource_id = requested.role_id" in sql
    assert "insurance_plan.source_id = role.source_id" in sql
    assert "insurance_plan.resource_id = NULLIF(BTRIM(CASE" in sql
    assert "COALESCE(insurance_plan.network_refs::jsonb, '[]'::jsonb)" in sql
    assert "network_catalog.network_resource_id = network.resource_id" in sql
    assert "network_organization.resource_id = network.resource_id" in sql
    assert f"LIMIT {npi_module.MAX_PROVIDER_DIRECTORY_ROLE_EVIDENCE_ROWS}" in sql


def test_provider_directory_role_evidence_keys_do_not_require_existing_aca_arrays():
    addresses = [
        {
            "source_record_ids": [
                "provider_directory_fhir:practitioner_role:pdfhir_example:role-100:location-400"
            ],
            "aca_plan_array": [],
            "aca_network_array": [],
        }
    ]

    assert npi_module._provider_directory_role_keys_from_addresses(addresses) == [
        ("pdfhir_example", "role-100")
    ]


@pytest.mark.asyncio
async def test_get_npi_exposes_resolved_provider_directory_plan_network_evidence(
    monkeypatch,
    provider_directory_plan_network_fixture,
):
    fixture = provider_directory_plan_network_fixture
    role_key = fixture["role"]["key"]

    async def fake_build(npi, **_kwargs):
        return {
            "npi": npi,
            "taxonomy_list": [],
            "taxonomy_group_list": [],
            "address_list": [dict(fixture["address"])],
            "do_business_as": [],
        }

    fetch_role_evidence = AsyncMock(return_value=fixture["role_evidence_map"])
    patch_map = {
        "_build_npi_details": fake_build,
        "_fetch_provider_directory_address_overlay": AsyncMock(return_value=[]),
        "_fetch_provider_directory_source_detail_map": AsyncMock(return_value=fixture["source_detail_map"]),
        "_fetch_provider_directory_role_evidence_map": fetch_role_evidence,
        "_fetch_other_names": AsyncMock(return_value=[]),
        "_fetch_provider_enrichment_detail": AsyncMock(return_value=None),
    }
    for function_name, replacement in patch_map.items():
        monkeypatch.setattr(npi_module, function_name, replacement)

    app = types.SimpleNamespace(config={"NPI_API_UPDATE_GEOCODE": False}, add_task=lambda _coro: None)
    request = types.SimpleNamespace(
        args={"include_sources": "true", "include_evidence": "true"}, app=app,
    )
    response = await npi_module.get_npi(request, "1518379601")
    address = json.loads(response.body)["address_list"][0]
    evidence = address["provider_directory_sources"][0]

    fetch_role_evidence.assert_awaited_once_with([role_key], session=None)
    assert address["aca_plan_array"] == ["H1234-001"]
    assert address["aca_network_array"] == ["Example Choice Network"]
    assert evidence["source_ids"] == [role_key[0]]
    assert evidence["practitioner_role_ids"] == [role_key[1]]
    assert evidence["insurance_plans"][0]["identifier"] == "H1234-001"
    assert evidence["networks"][0] == fixture["role_evidence_map"][role_key]["networks"][0]


@pytest.mark.asyncio
async def test_provider_directory_overlay_fetch_projects_coordinates(monkeypatch):
    captured_sql_statements: list[str] = []

    class FakeResult:
        def all(self):
            return []

    async def fake_execute(stmt, **_kwargs):
        captured_sql_statements.append(str(stmt))
        return FakeResult()

    monkeypatch.setattr(npi_module, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(npi_module, "_table_columns", AsyncMock(return_value={"lat", "long"}))
    monkeypatch.setattr(npi_module, "_execute_stmt", fake_execute)

    rows = await npi_module._fetch_provider_directory_address_overlay(1588616783)

    assert rows == []
    assert "lat,\n            long," in captured_sql_statements[0]
    assert "address_precision, lat, long" in captured_sql_statements[0]
    assert "LEFT JOIN mrf.provider_directory_source AS directory_source" in captured_sql_statements[0]
    assert "directory_source.canonical_api_base" in captured_sql_statements[0]


@pytest.mark.asyncio
async def test_provider_directory_overlay_falls_back_to_alias_count_without_source_table(
    monkeypatch,
):
    captured_sql_statements: list[str] = []

    class FakeResult:
        def all(self):
            return []

    async def is_fake_table_present(table_name, **_kwargs):
        return table_name == npi_module.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE

    async def fake_execute(stmt, **_kwargs):
        captured_sql_statements.append(str(stmt))
        return FakeResult()

    monkeypatch.setattr(npi_module, "_table_exists", is_fake_table_present)
    monkeypatch.setattr(npi_module, "_table_columns", AsyncMock(return_value=set()))
    monkeypatch.setattr(npi_module, "_execute_stmt", fake_execute)

    rows = await npi_module._fetch_provider_directory_address_overlay(1588616783)

    assert rows == []
    assert "LEFT JOIN mrf.provider_directory_source" not in captured_sql_statements[0]
    assert (
        "COUNT(DISTINCT overlay.source_id)::integer AS independent_source_count"
        in captured_sql_statements[0]
    )


@pytest.mark.asyncio
async def test_provider_directory_overlay_fetch_tolerates_old_overlay_schema(monkeypatch):
    captured_sql_statements: list[str] = []

    class FakeResult:
        def all(self):
            return []

    async def fake_execute(stmt, **_kwargs):
        captured_sql_statements.append(str(stmt))
        return FakeResult()

    monkeypatch.setattr(npi_module, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(npi_module, "_table_columns", AsyncMock(return_value=set()))
    monkeypatch.setattr(npi_module, "_execute_stmt", fake_execute)

    rows = await npi_module._fetch_provider_directory_address_overlay(1588616783)

    assert rows == []
    assert "NULL::numeric AS lat" in captured_sql_statements[0]
    assert "NULL::numeric AS long" in captured_sql_statements[0]
    assert "address_precision, lat, long" not in captured_sql_statements[0]


def test_validate_section_filters_requires_classification_or_codes():
    with pytest.raises(sanic.exceptions.InvalidUsage):
        npi_module._validate_section_filters("Individual", None, None)


def test_validate_section_filters_allows_with_codes_or_classification():
    npi_module._validate_section_filters("Individual", "Clinic/Center", None)
    npi_module._validate_section_filters("Individual", None, ["261Q00000X"])
