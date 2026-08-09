import json
import types
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import NotFound

from api.endpoint import reports


class FakeSession:
    async def execute(self, *_args, **_kwargs):
        raise AssertionError("execute should not be reached when helper is mocked")


class _FakeMappingsResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows


class _FakeScalarResult:
    def __init__(self, value):
        self._value = value

    def scalar(self):
        return self._value


class _FakeMappingRowResult:
    def __init__(self, row):
        self._row = row

    def mappings(self):
        return self

    def first(self):
        return self._row


class _Args(dict):
    def getlist(self, key):
        value = self.get(key)
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]


def make_request(args=None):
    ctx = types.SimpleNamespace(sa_session=FakeSession())
    return types.SimpleNamespace(args=_Args(args or {}), ctx=ctx)


@pytest.mark.asyncio
async def test_list_pharmacy_markets_returns_payload(monkeypatch):
    monkeypatch.setattr(
        reports,
        "_query_market_summaries",
        AsyncMock(
            return_value=(
                1,
                [
                    {
                        "market_id": "city:FL:miami",
                        "market_scope": "city",
                        "market_name": "Miami",
                        "state": "FL",
                        "city": "Miami",
                        "county": None,
                        "zip_code": None,
                        "metrics": {"pharmacy_count": 42, "access_score": 61.2},
                    }
                ],
            )
        ),
    )

    response = await reports.list_pharmacy_markets(make_request({"state": "FL", "limit": "10"}))
    payload = json.loads(response.body)

    assert payload["total"] == 1
    assert payload["items"][0]["market_id"] == "city:FL:miami"
    assert payload["filters"]["state"] == "FL"


@pytest.mark.asyncio
async def test_get_pharmacy_market_by_id_404(monkeypatch):
    monkeypatch.setattr(reports, "_query_market_summaries", AsyncMock(return_value=(0, [])))

    with pytest.raises(NotFound):
        await reports.get_pharmacy_market_by_id(make_request(), "city:FL:miami")


@pytest.mark.asyncio
async def test_list_pharmacy_access_rankings_adds_rank(monkeypatch):
    monkeypatch.setattr(
        reports,
        "_query_market_summaries",
        AsyncMock(
            return_value=(
                2,
                [
                    {"market_id": "city:TX:austin", "market_scope": "city", "metrics": {"access_score": 70.0}},
                    {"market_id": "city:TX:dallas", "market_scope": "city", "metrics": {"access_score": 68.0}},
                ],
            )
        ),
    )
    response = await reports.list_pharmacy_access_rankings(make_request({"state": "TX"}))
    payload = json.loads(response.body)

    assert payload["items"][0]["rank"] == 1
    assert payload["items"][1]["rank"] == 2


@pytest.mark.asyncio
async def test_get_pharmacy_market_context_returns_market(monkeypatch):
    monkeypatch.setattr(
        reports,
        "_fetch_pharmacy_context",
        AsyncMock(
            return_value={
                "npi": 1518379601,
                "name": "Sample Pharmacy",
                "state": "TX",
                "city": "Austin",
                "county": "Travis",
                "zip_code": "78701",
                "medicare_active": True,
                "mail_order": False,
                "pharmacy_type": "Retail",
                "has_active_state_license": True,
                "disciplinary_flag_any": False,
            }
        ),
    )
    monkeypatch.setattr(
        reports,
        "_query_market_summaries",
        AsyncMock(
            return_value=(
                1,
                [
                    {
                        "market_id": "city:TX:austin",
                        "market_scope": "city",
                        "market_name": "Austin",
                        "state": "TX",
                        "city": "Austin",
                        "county": None,
                        "zip_code": None,
                        "metrics": {"pharmacy_count": 10, "access_score": 77.1},
                    }
                ],
            )
        ),
    )

    response = await reports.get_pharmacy_market_context(make_request(), "1518379601")
    response_payload = json.loads(response.body)
    assert response_payload["npi"] == 1518379601
    assert response_payload["market"]["market_id"] == "city:TX:austin"


@pytest.mark.asyncio
async def test_fetch_pharmacy_context_uses_legacy_address_table_by_default(monkeypatch):
    monkeypatch.delenv("HLTHPRT_ADDRESS_SERVING_SOURCE", raising=False)
    monkeypatch.setattr(reports, "_is_table_available", AsyncMock(return_value=False))

    class Session:
        def __init__(self):
            self.sql = None

        async def execute(self, stmt, _params):
            self.sql = stmt.text
            return _FakeMappingRowResult(
                {
                    "npi": 1518379601,
                    "provider_organization_name": "Sample Pharmacy",
                    "do_business_as_text": None,
                    "chain_name": None,
                    "state_name": "TX",
                    "city_name": "Austin",
                    "county_name": "Travis",
                    "zip_code": "78701",
                    "medicare_active": False,
                    "mail_order": False,
                    "pharmacy_type": None,
                    "has_active_state_license": False,
                    "disciplinary_flag_any": False,
                }
            )

    session = Session()
    pharmacy_context = await reports._fetch_pharmacy_context(
        session,
        npi=1518379601,
        as_of=reports.datetime.date(2026, 6, 14),
    )

    assert pharmacy_context["npi"] == 1518379601
    assert "FROM mrf.npi_address a" in session.sql
    assert "FROM mrf.entity_address_unified a" not in session.sql


@pytest.mark.asyncio
async def test_fetch_pharmacy_context_uses_unified_address_table_by_default_when_available(monkeypatch):
    monkeypatch.delenv("HLTHPRT_ADDRESS_SERVING_SOURCE", raising=False)

    async def is_table_present(_session, table):
        return table is reports.EntityAddressUnified.__table__

    monkeypatch.setattr(reports, "_is_table_available", is_table_present)

    class Session:
        def __init__(self):
            self.sql = None

        async def execute(self, stmt, _params):
            self.sql = stmt.text
            return _FakeMappingRowResult(
                {
                    "npi": 1518379601,
                    "provider_organization_name": "Sample Pharmacy",
                    "do_business_as_text": None,
                    "chain_name": None,
                    "state_name": "TX",
                    "city_name": "Austin",
                    "county_name": "Travis",
                    "zip_code": "78701",
                    "medicare_active": False,
                    "mail_order": False,
                    "pharmacy_type": None,
                    "has_active_state_license": False,
                    "disciplinary_flag_any": False,
                }
            )

    session = Session()
    pharmacy_context = await reports._fetch_pharmacy_context(
        session,
        npi=1518379601,
        as_of=reports.datetime.date(2026, 6, 14),
    )

    assert pharmacy_context["npi"] == 1518379601
    assert "FROM mrf.entity_address_unified a" in session.sql
    assert "FROM mrf.npi_address a" not in session.sql
    assert "a.zip5 AS zip_code" in session.sql


def _assert_normalized_market_summary(market_summary):
    assert market_summary == {
        "market_id": "city:TX:austin",
        "market_scope": "city",
        "market_name": "Austin",
        "state": "TX",
        "city": "Austin",
        "county": None,
        "zip_code": None,
        "metrics": {
            "pharmacy_count": 10, "active_medicare_pharmacy_count": 9,
            "chain_count": 6, "independent_count": 4,
            "mail_order_count": 1, "retail_count": 9,
            "license_coverage_count": 10, "disciplinary_flag_count": 0,
            "ncpdp_registered_count": 0, "medicaid_identifier_count": 0,
            "railroad_medicare_identifier_count": 0, "ptan_identifier_count": 0,
            "clia_identifier_count": 0, "medicare_identifier_count": 0,
            "medicare_license_count": 0, "other_identifier_npi_count": 0,
            "population": 100000, "pharmacies_per_100k": 10.0,
            "active_medicare_share": 0.9, "license_coverage_share": 1.0,
            "mail_order_share": 0.1, "ncpdp_registered_share": 0.0,
            "medicaid_identifier_share": 0.0,
            "railroad_medicare_identifier_share": 0.0,
            "ptan_identifier_share": 0.0, "clia_identifier_share": 0.0,
            "medicare_identifier_share": 0.0, "medicare_license_share": 0.0,
            "other_identifier_share": 0.0, "chain_concentration": 0.6,
            "access_score": 53.25, "estimated_pharmacist_count_proxy": None,
            "top_chains": [],
        },
    }


@pytest.mark.asyncio
async def test_query_market_summaries_avoids_count_query_when_data_present(monkeypatch):
    monkeypatch.setattr(reports, "_is_table_available", AsyncMock(return_value=False))
    monkeypatch.setattr(reports, "_build_market_sql", lambda **_: ("SELECT count", "SELECT data", {}))

    market_summary_by_field = {
        "market_id": "city:TX:austin",
        "market_scope": "city",
        "market_name": "Austin",
        "state": "TX",
        "city": "Austin",
        "county": None,
        "zip_code": None,
        "pharmacy_count": 10,
        "active_medicare_pharmacy_count": 9,
        "chain_count": 6,
        "independent_count": 4,
        "mail_order_count": 1,
        "retail_count": 9,
        "license_coverage_count": 10,
        "disciplinary_flag_count": 0,
        "population": 100000,
        "pharmacies_per_100k": 10.0,
        "active_medicare_share": 0.9,
        "license_coverage_share": 1.0,
        "mail_order_share": 0.1,
        "chain_concentration": 0.6,
        "access_score": 75.0,
        "estimated_pharmacist_count_proxy": None,
        "top_chains": [],
        "total_count": 123,
    }

    class Session:
        async def execute(self, stmt, _params):
            if stmt.text == "SELECT data":
                return _FakeMappingsResult([market_summary_by_field])
            raise AssertionError("count query should not run when data rows are present")

    total, market_summaries = await reports._query_market_summaries(
        Session(),
        reports._MarketSummaryQuery(
            scope="city",
            sort="access_score",
            order="desc",
            as_of=reports.datetime.date(2026, 3, 18),
            include_staffing=False,
            limit=25,
            offset=0,
        ),
    )
    assert total == 123
    assert len(market_summaries) == 1
    _assert_normalized_market_summary(market_summaries[0])


@pytest.mark.asyncio
async def test_query_market_summaries_uses_count_fallback_for_empty_offset_page(monkeypatch):
    monkeypatch.setattr(reports, "_is_table_available", AsyncMock(return_value=False))
    monkeypatch.setattr(reports, "_build_market_sql", lambda **_: ("SELECT count", "SELECT data", {}))

    class Session:
        async def execute(self, stmt, _params):
            if stmt.text == "SELECT data":
                return _FakeMappingsResult([])
            if stmt.text == "SELECT count":
                return _FakeScalarResult(77)
            raise AssertionError(f"unexpected SQL: {stmt.text}")

    total, items = await reports._query_market_summaries(
        Session(),
        reports._MarketSummaryQuery(
            scope="city",
            sort="access_score",
            order="desc",
            as_of=reports.datetime.date(2026, 3, 18),
            include_staffing=False,
            limit=25,
            offset=25,
        ),
    )
    assert total == 77
    assert items == []
