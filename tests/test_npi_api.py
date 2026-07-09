# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import types
from unittest.mock import AsyncMock

import pytest
import sanic.exceptions

from api.endpoint import npi as npi_module
from api.endpoint.npi import npi_index_status


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
