# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Filtered reports use live pharmacist counts even when a summary table exists."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api.endpoint import reports
from tests.test_reports_api import _FakeMappingRowResult, _FakeMappingsResult, make_request


@pytest.fixture
def report_reads(monkeypatch):
    availability = AsyncMock(return_value=True)
    monkeypatch.setattr(reports, "_is_table_available", availability)
    monkeypatch.setattr(
        reports,
        "_resolve_pharmacy_address_table_sql",
        AsyncMock(return_value="mrf.entity_address_unified"),
    )
    return availability


def _assert_staffing_source(statement, parameters, availability, session, minimum):
    if minimum == "2":
        availability.assert_not_awaited()
        assert "FROM mrf.npi_phone_staffing" not in statement
        assert "COUNT(DISTINCT a.npi)::int AS pharmacist_count" in statement
        assert "source_count >= 2" in statement
        assert parameters["min_address_sources"] == 2
    else:
        availability.assert_awaited_once_with(session, reports.NPIPhoneStaffing.__table__)
        assert "FROM mrf.npi_phone_staffing" in statement
        assert "min_address_sources" not in parameters


@pytest.mark.asyncio
@pytest.mark.parametrize("minimum", [None, "0", "2"])
async def test_chain_report_selects_eligible_staffing(monkeypatch, report_reads, minimum):
    chain_result = _FakeMappingRowResult(
        {
            "summary": {"pharmacy_npi_count": 1, "active_pharmacy_count": 0, "pharmacist_count": 0},
            "histogram": [{"label": "0", "count": 1}],
            "states": [{"state": "CA", "pharmacy_npi_count": 1, "pharmacist_count": 0}],
        }
    )
    execute = AsyncMock(return_value=chain_result)
    query_parameter_map = {"name_like": "sample pharmacy"}
    if minimum is not None:
        query_parameter_map["min_address_sources"] = minimum
    request = make_request(query_parameter_map)
    request.ctx.sa_session = SimpleNamespace(execute=execute)

    document = json.loads((await reports.get_pharmacy_chain_summary(request)).body)

    statement, parameters = execute.await_args.args
    session = request.ctx.sa_session
    _assert_staffing_source(statement.text, parameters, report_reads, session, minimum)
    assert document["summary"] == {
        "pharmacy_npi_count": 1,
        "insured_pharmacy_npi_count": 0,
        "active_pharmacy_count": 0,
        "pharmacist_count": 0,
    }
    assert document["histogram"] == [{"label": "0", "count": 1}]
    assert document["states"][0]["pharmacist_count"] == 0
    assert document["methodology"]["staffing_helper_available"] is (minimum != "2")
    assert document["methodology"]["staffing_mode"] == ("live_fallback_query" if minimum == "2" else "helper_table")


@pytest.mark.asyncio
@pytest.mark.parametrize("minimum", [None, "0", "2"])
async def test_state_report_selects_eligible_staffing(monkeypatch, report_reads, minimum):
    execute = AsyncMock(
        return_value=_FakeMappingsResult(
            [{"state": "CA", "nppes_pharmacies": 1, "nppes_pharmacists": 0, "active_pharmacists": 0}]
        )
    )
    request = make_request({} if minimum is None else {"min_address_sources": minimum})
    request.ctx.sa_session = SimpleNamespace(execute=execute)

    document = json.loads((await reports.get_pharmacy_state_stats(request)).body)

    statement, *bound = execute.await_args.args
    session = request.ctx.sa_session
    _assert_staffing_source(statement.text, bound[0] if bound else {}, report_reads, session, minimum)
    if minimum == "2":
        assert "FROM pharmacist_base" in statement.text
        assert "COUNT(DISTINCT phm.npi)::int AS active_pharmacists" in statement.text
    california = next(state for state in document["states"] if state["state"] == "CA")
    assert california["nppes_pharmacies"] == 1
    assert california["nppes_pharmacists"] == california["active_pharmacists"] == 0
    assert len(document["states"]) == 51
    assert document["methodology"]["staffing_helper_available"] is (minimum != "2")
    assert document["methodology"]["staffing_mode"] == ("live_fallback_query" if minimum == "2" else "helper_table")
