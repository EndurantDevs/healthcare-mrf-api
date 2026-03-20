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
    payload = json.loads(response.body)
    assert payload["npi"] == 1518379601
    assert payload["market"]["market_id"] == "city:TX:austin"


@pytest.mark.asyncio
async def test_query_market_summaries_avoids_count_query_when_data_present(monkeypatch):
    monkeypatch.setattr(reports, "_table_exists", AsyncMock(return_value=False))
    monkeypatch.setattr(reports, "_build_market_sql", lambda **_: ("SELECT count", "SELECT data", {}))

    row = {
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
                return _FakeMappingsResult([row])
            raise AssertionError("count query should not run when data rows are present")

    total, items = await reports._query_market_summaries(
        Session(),
        scope="city",
        sort="access_score",
        order="desc",
        as_of=reports.datetime.date(2026, 3, 18),
        include_staffing=False,
        limit=25,
        offset=0,
    )
    assert total == 123
    assert items and items[0]["market_id"] == "city:TX:austin"


@pytest.mark.asyncio
async def test_query_market_summaries_uses_count_fallback_for_empty_offset_page(monkeypatch):
    monkeypatch.setattr(reports, "_table_exists", AsyncMock(return_value=False))
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
        scope="city",
        sort="access_score",
        order="desc",
        as_of=reports.datetime.date(2026, 3, 18),
        include_staffing=False,
        limit=25,
        offset=25,
    )
    assert total == 77
    assert items == []


@pytest.mark.asyncio
async def test_get_pharmacy_chain_summary_returns_payload(monkeypatch):
    monkeypatch.setattr(
        reports,
        "_query_chain_summary",
        AsyncMock(
            return_value=(
                {
                    "pharmacy_npi_count": 120,
                    "insured_pharmacy_npi_count": 75,
                    "active_pharmacy_count": 65,
                    "pharmacist_count": 210,
                },
                [{"label": "1", "count": 30}],
                [{"state": "CA", "active_pharmacy_count": 10, "pharmacy_npi_count": 12, "insured_pharmacy_npi_count": 8, "pharmacist_count": 22}],
                True,
            )
        ),
    )

    response = await reports.get_pharmacy_chain_summary(
        make_request({"name_like": ["cvs", "longs drugs stores"], "include_states": "1"})
    )
    payload = json.loads(response.body)

    assert payload["summary"]["pharmacy_npi_count"] == 120
    assert payload["histogram"][0]["label"] == "1"
    assert payload["states"][0]["state"] == "CA"
    assert payload["methodology"]["staffing_mode"] == "helper_table"


@pytest.mark.asyncio
async def test_get_pharmacy_state_stats_returns_payload(monkeypatch):
    monkeypatch.setattr(
        reports,
        "_query_pharmacy_state_stats",
        AsyncMock(
            return_value=(
                [
                    {
                        "state": "CA",
                        "nppes_pharmacies": 100,
                        "nppes_pharmacists": 200,
                        "active_pharmacists": 150,
                        "active_pharmacies": 75,
                        "aca_pharmacies": 80,
                    }
                ],
                True,
            )
        ),
    )

    response = await reports.get_pharmacy_state_stats(make_request())
    payload = json.loads(response.body)

    assert payload["states"][0]["state"] == "CA"
    assert payload["states"][0]["active_pharmacies"] == 75
    assert payload["methodology"]["staffing_mode"] == "helper_table"


@pytest.mark.asyncio
async def test_query_pharmacy_state_stats_normalizes_and_zero_fills_states(monkeypatch):
    monkeypatch.setattr(reports, "_table_exists", AsyncMock(return_value=False))

    class Session:
        async def execute(self, stmt):
            assert "WITH pharmacy_taxonomy AS" in stmt.text
            return _FakeMappingsResult(
                [
                    {
                        "state": "California",
                        "nppes_pharmacies": 10,
                        "nppes_pharmacists": 20,
                        "active_pharmacists": 15,
                        "active_pharmacies": 7,
                        "aca_pharmacies": 8,
                    },
                    {
                        "state": "CA",
                        "nppes_pharmacies": 3,
                        "nppes_pharmacists": 4,
                        "active_pharmacists": 2,
                        "active_pharmacies": 1,
                        "aca_pharmacies": 1,
                    },
                    {
                        "state": "District of Columbia",
                        "nppes_pharmacies": 5,
                        "nppes_pharmacists": 6,
                        "active_pharmacists": 4,
                        "active_pharmacies": 3,
                        "aca_pharmacies": 2,
                    },
                    {
                        "state": "DOHA",
                        "nppes_pharmacies": 999,
                        "nppes_pharmacists": 999,
                        "active_pharmacists": 999,
                        "active_pharmacies": 999,
                        "aca_pharmacies": 999,
                    },
                ]
            )

    rows, has_helper = await reports._query_pharmacy_state_stats(Session())
    by_state = {row["state"]: row for row in rows}

    assert has_helper is False
    assert len(rows) == 51
    assert by_state["CA"]["nppes_pharmacies"] == 13
    assert by_state["CA"]["nppes_pharmacists"] == 24
    assert by_state["DC"]["active_pharmacies"] == 3
    assert by_state["NY"]["nppes_pharmacies"] == 0
    assert "DOHA" not in by_state


@pytest.mark.asyncio
async def test_query_chain_summary_uses_helper_table_when_available(monkeypatch):
    monkeypatch.setattr(reports, "_table_exists", AsyncMock(return_value=True))

    captured = {}

    class Session:
        async def execute(self, stmt, params):
            captured["sql"] = stmt.text
            captured["params"] = params
            return _FakeMappingRowResult(
                {
                    "summary": {"pharmacy_npi_count": 2},
                    "histogram": [{"label": "1", "count": 2}],
                    "states": [{"state": "CA"}],
                }
            )

    summary, histogram, states, has_helper = await reports._query_chain_summary(
        Session(),
        names=["cvs"],
        include_states=True,
    )

    assert has_helper is True
    assert "FROM mrf.npi_phone_staffing" in captured["sql"]
    assert summary["pharmacy_npi_count"] == 2
    assert histogram[0]["label"] == "1"
    assert states[0]["state"] == "CA"
    assert captured["params"]["name_like_0"] == "%cvs%"


@pytest.mark.asyncio
async def test_query_chain_summary_falls_back_when_helper_missing(monkeypatch):
    monkeypatch.setattr(reports, "_table_exists", AsyncMock(return_value=False))

    captured = {}

    class Session:
        async def execute(self, stmt, params):
            captured["sql"] = stmt.text
            captured["params"] = params
            return _FakeMappingRowResult(
                {
                    "summary": {"pharmacy_npi_count": 0},
                    "histogram": [],
                    "states": [],
                }
            )

    summary, histogram, states, has_helper = await reports._query_chain_summary(
        Session(),
        names=["cvs"],
        include_states=False,
    )

    assert has_helper is False
    assert "FROM mrf.npi_phone_staffing" not in captured["sql"]
    assert "GROUP BY a.state_name, REGEXP_REPLACE(a.telephone_number, '[^0-9]', '', 'g')" in captured["sql"]
    assert captured["params"]["include_states"] is False
    assert summary["pharmacy_npi_count"] == 0
    assert histogram == []
    assert states == []


def test_build_market_sql_uses_index_friendly_state_and_zip_filters():
    _count_sql, data_sql, params = reports._build_market_sql(
        scope="state",
        sort="access_score",
        order="desc",
        include_staffing=False,
        has_partd=True,
        has_license=False,
        has_other_id=True,
        market_id_filter=None,
        state="TX",
        city=None,
        county=None,
        zip_code="78701",
        chain=None,
    )
    assert "a.state_name = :state" in data_sql
    assert "UPPER(COALESCE(a.state_name" not in data_sql
    assert "LEFT(a.postal_code, 5) = :zip_code" in data_sql
    assert "pd.npi9 = (a.npi / 10)" in data_sql
    assert "has_ncpdp_identifier" in data_sql
    assert "has_medicaid_identifier" in data_sql
    assert "has_railroad_medicare_identifier" in data_sql
    assert "has_ptan_identifier" in data_sql
    assert "has_clia_identifier" in data_sql
    assert "medicaid_identifier_count" in data_sql
    assert "railroad_medicare_identifier_count" in data_sql
    assert "ptan_identifier_count" in data_sql
    assert "clia_identifier_count" in data_sql
    assert "medicare_identifier_count" in data_sql
    assert params["state"] == "TX"
    assert params["zip_code"] == "78701"


@pytest.mark.asyncio
async def test_query_chain_summary_optimizes_match_all_wildcard(monkeypatch):
    monkeypatch.setattr(reports, "_table_exists", AsyncMock(return_value=True))

    captured = {}

    class Session:
        async def execute(self, stmt, params):
            captured["sql"] = stmt.text
            captured["params"] = params
            return _FakeMappingRowResult(
                {
                    "summary": {"pharmacy_npi_count": 5},
                    "histogram": [{"label": "1", "count": 5}],
                    "states": [],
                }
            )

    summary, histogram, states, has_helper = await reports._query_chain_summary(
        Session(),
        names=["%"],
        include_states=False,
    )

    assert has_helper is True
    assert "JOIN mrf.npi AS d ON d.npi = a.npi" not in captured["sql"]
    assert "WHERE a.type = 'primary'" in captured["sql"]
    assert "name_like_0" not in captured["params"]
    assert summary["pharmacy_npi_count"] == 5
    assert histogram[0]["count"] == 5
    assert states == []


@pytest.mark.asyncio
async def test_query_pharmacy_state_stats_uses_helper_table(monkeypatch):
    monkeypatch.setattr(reports, "_table_exists", AsyncMock(return_value=True))

    captured = {}

    class Session:
        async def execute(self, stmt, _params=None):
            captured["sql"] = stmt.text
            return _FakeMappingsResult(
                [
                    {
                        "state": "CA",
                        "nppes_pharmacies": 100,
                        "nppes_pharmacists": 200,
                        "active_pharmacists": 150,
                        "active_pharmacies": 75,
                        "aca_pharmacies": 80,
                    }
                ]
            )

    rows, has_helper = await reports._query_pharmacy_state_stats(Session())

    assert has_helper is True
    assert "FROM mrf.npi_phone_staffing" in captured["sql"]
    by_state = {row["state"]: row for row in rows}
    assert by_state["CA"]["state"] == "CA"


@pytest.mark.asyncio
async def test_query_pharmacy_state_stats_coerces_numeric_fields(monkeypatch):
    monkeypatch.setattr(reports, "_table_exists", AsyncMock(return_value=True))

    class Session:
        async def execute(self, stmt, _params=None):
            assert "FROM mrf.npi_phone_staffing" in stmt.text
            return _FakeMappingsResult(
                [
                    {
                        "state": "CA",
                        "nppes_pharmacies": 10,
                        "nppes_pharmacists": 12,
                        "active_pharmacists": 9,
                        "active_pharmacies": 8,
                        "aca_pharmacies": 7,
                    }
                ]
            )

    rows, has_helper = await reports._query_pharmacy_state_stats(Session())
    assert has_helper is True
    by_state = {row["state"]: row for row in rows}
    assert by_state["CA"]["state"] == "CA"
    assert by_state["CA"]["aca_pharmacies"] == 7
