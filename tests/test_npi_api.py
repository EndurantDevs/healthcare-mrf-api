# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import types
from unittest.mock import AsyncMock

import pytest

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
    async def fake_build(_npi):
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
    async def fake_build(_npi):
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
