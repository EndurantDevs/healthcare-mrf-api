import json
import types
from unittest.mock import AsyncMock

import pytest

pytest.importorskip("pytz")

from api.endpoint import npi as npi_module
from db.models import NPIAddress, NPIData, NPIDataTaxonomy


class FakeConnection:
    def __init__(self, responses):
        self._responses = responses

    async def all(self, _query, **_params):
        if not self._responses:
            return []
        return self._responses.pop(0)


class FakeAcquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _build_result_row(npi_value: int):
    row = [npi_value]
    for column in NPIData.__table__.columns:
        if column.key == "npi":
            row.append(npi_value)
        elif column.key == "do_business_as":
            row.append(["DBA"])
        else:
            row.append(f"data_{column.key}")
    for column in NPIAddress.__table__.columns:
        if column.key == "npi":
            row.append(npi_value)
        elif column.key == "checksum":
            row.append(111)
        elif column.key == "type":
            row.append("primary")
        else:
            row.append(f"addr_{column.key}")
    for column in NPIDataTaxonomy.__table__.columns:
        if column.key in {"npi", "checksum"}:
            row.append(None)
        else:
            row.append(f"tax_{column.key}")
    return row


@pytest.mark.asyncio
async def test_get_npi_includes_provider_enrichment(monkeypatch):
    async def fake_build(_npi):
        return {
            "npi": _npi,
            "taxonomy_list": [],
            "taxonomy_group_list": [],
            "address_list": [],
            "do_business_as": ["DBA"],
        }

    monkeypatch.setattr(npi_module, "_build_npi_details", fake_build)
    monkeypatch.setattr(npi_module, "_fetch_other_names", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_enrichment_detail",
        AsyncMock(return_value={"summary": {"status": "enriched"}, "enrollments": {"ffs_public": []}}),
    )

    request = types.SimpleNamespace(args={})
    response = await npi_module.get_npi(request, "1518379601")
    payload = json.loads(response.body)

    assert "provider_enrichment" in payload
    assert payload["provider_enrichment"]["summary"]["status"] == "enriched"


@pytest.mark.asyncio
async def test_get_all_attaches_provider_enrichment_summary(monkeypatch):
    connections = [
        FakeConnection([[(1,)]]),
        FakeConnection([[_build_result_row(1234567890)]]),
    ]

    class FakeDB:
        def acquire(self):
            return FakeAcquire(connections.pop(0))

    monkeypatch.setattr(npi_module, "db", FakeDB())
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_enrichment_summary_map",
        AsyncMock(
            return_value={
                1234567890: {
                    "status": "enriched",
                    "has_any_enrollment": True,
                    "latest_reporting_year": 2026,
                }
            }
        ),
    )

    request = types.SimpleNamespace(args={"limit": "1"}, app=types.SimpleNamespace())
    response = await npi_module.get_all(request)
    payload = json.loads(response.body)

    assert payload["total"] == 1
    assert payload["rows"][0]["provider_enrichment_summary"]["status"] == "enriched"
