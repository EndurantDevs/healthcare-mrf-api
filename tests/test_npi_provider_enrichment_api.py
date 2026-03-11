import json
import types
from unittest.mock import AsyncMock

import pytest
import sanic.exceptions

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


class FakeSessionContext:
    async def __aenter__(self):
        return self

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


def test_serialize_ffs_reassignment_row_keeps_counterparty_only():
    payload = npi_module._serialize_ffs_reassignment_row(
        {
            "reassigning_enrollment_id": "I20241030000487",
            "receiving_enrollment_id": "O20031121000089",
            "counterparty_npi": 1609838473,
            "counterparty_provider_type_code": "12-70",
            "counterparty_provider_type_text": "PART B SUPPLIER - CLINIC/GROUP PRACTICE",
            "reporting_year": 2025,
        }
    )

    assert payload["counterparty_npi"] == 1609838473
    assert payload["reporting_year"] == 2025
    assert "reassigning_npi" not in payload
    assert "receiving_npi" not in payload


def test_serialize_ffs_reassignment_row_handles_missing_npis():
    payload = npi_module._serialize_ffs_reassignment_row(
        {
            "reassigning_enrollment_id": "I20241030000487",
            "receiving_enrollment_id": "O20031121000089",
            "counterparty_npi": None,
            "counterparty_provider_type_code": None,
            "counterparty_provider_type_text": None,
            "reporting_year": 2025,
        }
    )

    assert payload["counterparty_npi"] is None


def test_normalize_provider_enrichment_view_defaults_to_full():
    assert npi_module._normalize_provider_enrichment_view(None) == "full"
    assert npi_module._normalize_provider_enrichment_view("") == "full"
    assert npi_module._normalize_provider_enrichment_view("summary") == "summary"


def test_normalize_provider_enrichment_view_rejects_invalid():
    with pytest.raises(sanic.exceptions.InvalidUsage):
        npi_module._normalize_provider_enrichment_view("chain")


@pytest.mark.asyncio
async def test_resolve_npi_filter_capabilities_uses_model_columns_without_schema_query(monkeypatch):
    monkeypatch.setattr(npi_module, "ENABLE_NPI_SCHEMA_CACHE", False)
    monkeypatch.setattr(
        npi_module,
        "_model_table_columns",
        lambda _model: {"procedures_array", "medications_array"},
    )
    monkeypatch.setattr(
        npi_module,
        "_table_exists",
        AsyncMock(side_effect=[True, False]),
    )

    execute_mock = AsyncMock(side_effect=AssertionError("unexpected schema introspection"))
    monkeypatch.setattr(npi_module, "_execute_stmt", execute_mock)

    capabilities = await npi_module._resolve_npi_filter_capabilities()

    assert capabilities == {
        "npi_procedures_array_available": True,
        "npi_medications_array_available": True,
        "pricing_provider_procedure_available": True,
        "pricing_provider_prescription_available": False,
    }
    execute_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_fetch_provider_enrichment_detail_moves_chain_flags_to_visibility(monkeypatch):
    async def fake_summary_map(_npis, include_chain=False, session=None):
        return {
            1518379601: {
                "status": "enriched",
                "ffs_chain_hidden": True,
                "ffs_chain_enrollment_count": 2,
            }
        }

    monkeypatch.setattr(npi_module, "_fetch_provider_enrichment_summary_map", fake_summary_map)
    monkeypatch.setattr(npi_module, "_table_exists", AsyncMock(return_value=False))
    monkeypatch.setattr(npi_module.db, "session", lambda: FakeSessionContext())

    payload = await npi_module._fetch_provider_enrichment_detail(1518379601)

    assert payload["summary"]["status"] == "enriched"
    assert "ffs_chain_hidden" not in payload["summary"]
    assert "ffs_chain_enrollment_count" not in payload["summary"]
    assert payload["ffs_visibility"]["chain_hidden"] is True
    assert payload["ffs_visibility"]["chain_enrollment_count"] == 2


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
    fetch_detail = AsyncMock(return_value={"summary": {"status": "enriched"}, "enrollments": {"ffs_public": []}})
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_enrichment_detail",
        fetch_detail,
    )

    request = types.SimpleNamespace(args={})
    response = await npi_module.get_npi(request, "1518379601")
    payload = json.loads(response.body)

    assert "provider_enrichment" in payload
    assert payload["provider_enrichment"]["summary"]["status"] == "enriched"
    fetch_detail.assert_awaited_once_with(1518379601, include_chain=False)


@pytest.mark.asyncio
async def test_get_npi_can_include_chain_provider_enrichment(monkeypatch):
    async def fake_build(_npi):
        return {
            "npi": _npi,
            "taxonomy_list": [],
            "taxonomy_group_list": [],
            "address_list": [],
            "do_business_as": [],
        }

    monkeypatch.setattr(npi_module, "_build_npi_details", fake_build)
    monkeypatch.setattr(npi_module, "_fetch_other_names", AsyncMock(return_value=[]))
    fetch_detail = AsyncMock(return_value={"summary": {"status": "enriched"}, "enrollments": {"ffs_public": []}})
    monkeypatch.setattr(npi_module, "_fetch_provider_enrichment_detail", fetch_detail)

    request = types.SimpleNamespace(args={"show": "chain"})
    response = await npi_module.get_npi(request, "1518379601")
    payload = json.loads(response.body)

    assert payload["provider_enrichment"]["summary"]["status"] == "enriched"
    fetch_detail.assert_awaited_once_with(1518379601, include_chain=True)


@pytest.mark.asyncio
async def test_get_npi_can_return_summary_view(monkeypatch):
    async def fake_build(_npi):
        return {
            "npi": _npi,
            "taxonomy_list": [],
            "taxonomy_group_list": [],
            "address_list": [],
            "do_business_as": [],
        }

    monkeypatch.setattr(npi_module, "_build_npi_details", fake_build)
    monkeypatch.setattr(npi_module, "_fetch_other_names", AsyncMock(return_value=[]))
    fetch_summary_detail = AsyncMock(
        return_value={
            "summary": {"status": "enriched"},
            "ffs_visibility": {"show_mode": "default", "chain_hidden": False},
        }
    )
    monkeypatch.setattr(npi_module, "_fetch_provider_enrichment_summary_detail", fetch_summary_detail)
    monkeypatch.setattr(npi_module, "_fetch_provider_enrichment_detail", AsyncMock())

    request = types.SimpleNamespace(args={"view": "summary"})
    response = await npi_module.get_npi(request, "1518379601")
    payload = json.loads(response.body)

    assert payload["provider_enrichment"] == {
        "summary": {"status": "enriched"},
        "ffs_visibility": {"show_mode": "default", "chain_hidden": False},
    }
    fetch_summary_detail.assert_awaited_once_with(1518379601, include_chain=False)


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
    fetch_summary = AsyncMock(
        return_value={
            1234567890: {
                "status": "enriched",
                "has_any_enrollment": True,
                "latest_reporting_year": 2026,
            }
        }
    )
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_enrichment_summary_map",
        fetch_summary,
    )

    request = types.SimpleNamespace(args={"limit": "1"}, app=types.SimpleNamespace())
    response = await npi_module.get_all(request)
    payload = json.loads(response.body)

    assert payload["total"] == 1
    assert payload["rows"][0]["provider_enrichment_summary"]["status"] == "enriched"
    fetch_summary.assert_awaited_once_with([1234567890], include_chain=False, session=None)


@pytest.mark.asyncio
async def test_get_all_can_include_chain_provider_enrichment(monkeypatch):
    connections = [
        FakeConnection([[(1,)]]),
        FakeConnection([[_build_result_row(1234567890)]]),
    ]

    class FakeDB:
        def acquire(self):
            return FakeAcquire(connections.pop(0))

    monkeypatch.setattr(npi_module, "db", FakeDB())
    fetch_summary = AsyncMock(
        return_value={
            1234567890: {
                "status": "enriched",
                "has_any_enrollment": True,
                "latest_reporting_year": 2026,
            }
        }
    )
    monkeypatch.setattr(npi_module, "_fetch_provider_enrichment_summary_map", fetch_summary)

    request = types.SimpleNamespace(args={"limit": "1", "show": "chain"}, app=types.SimpleNamespace())
    response = await npi_module.get_all(request)
    payload = json.loads(response.body)

    assert payload["rows"][0]["provider_enrichment_summary"]["status"] == "enriched"
    fetch_summary.assert_awaited_once_with([1234567890], include_chain=True, session=None)
