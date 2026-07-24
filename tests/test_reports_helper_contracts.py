from __future__ import annotations

import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import InvalidUsage

from api.endpoint import reports


class _ScalarResult:
    def __init__(self, value):
        self._value = value

    def scalar(self):
        return self._value


class _MappingResult:
    def __init__(self, row):
        self._row = row

    def mappings(self):
        return self

    def first(self):
        return self._row


@pytest.mark.asyncio
async def test_report_session_and_address_table_selection_edges(monkeypatch) -> None:
    request = SimpleNamespace(ctx=SimpleNamespace())
    with pytest.raises(RuntimeError, match="session not available"):
        reports._get_session(request)

    session = object()
    request.ctx.sa_session = session
    assert reports._get_session(request) is session

    table = SimpleNamespace(schema=None, name="example")
    assert reports._qualified_table_name(table) == "mrf.example"
    assert reports._pharmacy_address_table_sql(reports.ADDRESS_SERVING_SOURCE_LEGACY) == (
        "mrf.npi_address"
    )
    assert reports._pharmacy_address_table_sql(
        reports.ADDRESS_SERVING_SOURCE_UNIFIED
    ) == "mrf.entity_address_unified"
    assert reports._pharmacy_address_zip5_expr(
        "a", "mrf.entity_address_unified"
    ) == "a.zip5"
    assert "postal_code" in reports._pharmacy_address_zip5_expr(
        "a", "mrf.npi_address"
    )
    assert "address_precision" in reports._pharmacy_address_order_expr(
        "a", "mrf.entity_address_unified"
    )
    assert "address_precision" not in reports._pharmacy_address_order_expr(
        "a", "mrf.npi_address"
    )

    monkeypatch.setenv(
        "HLTHPRT_ADDRESS_SERVING_SOURCE",
        reports.ADDRESS_SERVING_SOURCE_LEGACY,
    )
    assert await reports._resolve_pharmacy_address_table_sql(object()) == (
        "mrf.npi_address"
    )

    monkeypatch.setenv(
        "HLTHPRT_ADDRESS_SERVING_SOURCE",
        reports.ADDRESS_SERVING_SOURCE_UNIFIED,
    )
    availability = AsyncMock(side_effect=[True, False])
    monkeypatch.setattr(reports, "_is_table_available", availability)
    assert await reports._resolve_pharmacy_address_table_sql(object()) == (
        "mrf.entity_address_unified"
    )
    assert await reports._resolve_pharmacy_address_table_sql(object()) == (
        "mrf.npi_address"
    )


@pytest.mark.asyncio
async def test_report_table_availability_cache_and_database_results(monkeypatch) -> None:
    table = SimpleNamespace(schema="mrf", name="available_table")
    qualified = "mrf.available_table"
    reports._TABLE_EXISTS_CACHE.clear()
    monkeypatch.setattr(reports.time, "monotonic", lambda: 100.0)

    cached_session = SimpleNamespace(
        execute=AsyncMock(side_effect=AssertionError("cache miss"))
    )
    reports._TABLE_EXISTS_CACHE[qualified] = (99.0, True)
    assert await reports._is_table_available(cached_session, table) is True

    reports._TABLE_EXISTS_CACHE.clear()
    present_session = SimpleNamespace(
        execute=AsyncMock(return_value=_ScalarResult("mrf.available_table"))
    )
    assert await reports._is_table_available(present_session, table) is True
    assert reports._TABLE_EXISTS_CACHE[qualified] == (100.0, True)

    reports._TABLE_EXISTS_CACHE.clear()
    absent_session = SimpleNamespace(
        execute=AsyncMock(return_value=_ScalarResult(None))
    )
    assert await reports._is_table_available(absent_session, table) is False
    assert qualified not in reports._TABLE_EXISTS_CACHE


@pytest.mark.parametrize(
    ("value", "expected"),
    ((None, "unknown"), ("---", "unknown"), ("  Hello, World  ", "hello-world")),
)
def test_report_slug_edges(value, expected) -> None:
    assert reports._slugify(value) == expected


@pytest.mark.parametrize("value", ("not-a-number", "0", "-3"))
def test_report_npi_rejects_invalid_values(value) -> None:
    with pytest.raises(InvalidUsage):
        reports._parse_npi(value)


def test_report_npi_and_date_edges() -> None:
    assert reports._parse_npi(" 1234567890 ") == 1234567890
    for empty in (None, "", "null", "  "):
        assert reports._parse_date_param(empty, "as_of") is None
    assert reports._parse_date_param("2026-07-24", "as_of") == datetime.date(
        2026, 7, 24
    )
    with pytest.raises(InvalidUsage, match="valid ISO date"):
        reports._parse_date_param("24-07-2026", "as_of")


@pytest.mark.parametrize(
    ("parser", "valid", "default", "invalid"),
    (
        (reports._parse_scope, " ZIP ", "city", "planet"),
        (reports._parse_sort, " pharmacy_count ", "access_score", "cost"),
        (reports._parse_order, " ASC ", "desc", "sideways"),
    ),
)
def test_report_enum_parser_edges(parser, valid, default, invalid) -> None:
    assert parser(None, default=default) == default
    assert parser(valid, default=default) == valid.strip().lower()
    with pytest.raises(InvalidUsage):
        parser(invalid, default=default)


def test_report_boolean_zip_and_chain_edges() -> None:
    assert reports._is_boolean_parameter_enabled(None, default=True) is True
    assert reports._is_boolean_parameter_enabled(" yes ") is True
    assert reports._is_boolean_parameter_enabled("off") is False

    assert reports._parse_optional_zip(None) is None
    assert reports._parse_optional_zip(" 02139 ") == "02139"
    with pytest.raises(InvalidUsage, match="5-digit"):
        reports._parse_optional_zip("2139")

    assert reports._canonical_chain(None) is None
    assert reports._canonical_chain("  ") is None
    assert reports._canonical_chain("indy") == "independent"
    assert reports._canonical_chain("CVS") == "CVS"
    assert reports._canonical_chain("caremark") == "CVS"
    assert reports._canonical_chain("Main Street Walgreens #4") == "Walgreens"
    assert reports._canonical_chain("Neighborhood Pharmacy") == (
        "Neighborhood Pharmacy"
    )


def test_report_scope_and_json_hydration_edges() -> None:
    assert reports._scope_sql("state")["market_id"].startswith("CONCAT('state:'")
    assert reports._scope_sql("county")["market_id"].startswith("CONCAT('county:'")
    assert reports._scope_sql("zip")["market_id"].startswith("CONCAT('zip:'")
    assert reports._scope_sql("city")["market_id"].startswith("CONCAT('city:'")

    assert reports._hydrate_top_chains(None) == []
    assert reports._hydrate_top_chains([{"name": "a"}, "ignored"]) == [
        {"name": "a"}
    ]
    assert reports._hydrate_top_chains('[{"name":"b"}, 3]') == [
        {"name": "b"}
    ]
    assert reports._hydrate_top_chains("{") == []
    assert reports._hydrate_top_chains('{"name":"not-a-list"}') == []


def test_report_score_and_scalar_coercion_edges() -> None:
    empty = reports._score_components({"pharmacy_count": 5, "population": 0})
    populated = reports._score_components(
        {
            "pharmacy_count": 10,
            "population": 50_000,
            "active_medicare_share": 0.5,
            "mail_order_share": 0.25,
            "license_coverage_share": 0.75,
            "chain_concentration": 0.2,
        }
    )
    assert empty["density_per_100k"] == 0.0
    assert populated["density_per_100k"] == 20.0
    assert reports._sort_expression("pharmacy_count") == "f.pharmacy_count"
    assert reports._sort_expression("unknown") == "f.access_score"
    assert reports._coerce_int("4") == 4
    assert reports._coerce_int("bad") == 0
    assert reports._coerce_float("4.5") == 4.5
    assert reports._coerce_float(None) == 0.0


class _GetListArgs:
    def getlist(self, _name):
        return [" CVS ", "", "cvs", "Walgreens"]

    def get(self, _name):
        return "Costco"


class _GetAllArgs:
    def getall(self, _name):
        return ["Rite Aid"]

    def get(self, _name):
        return None


def test_report_name_filter_and_clause_edges() -> None:
    assert reports._extract_name_like_filters(_GetListArgs()) == [
        "cvs",
        "walgreens",
        "costco",
    ]
    assert reports._extract_name_like_filters(_GetAllArgs()) == ["rite aid"]
    assert reports._extract_name_like_filters({"name_like": "Publix"}) == [
        "publix"
    ]
    assert reports._name_like_clauses("d", []) == ("", {})
    clause, params = reports._name_like_clauses("d", ["cvs", "walgreens"])
    assert "name_like_0" in clause and "name_like_1" in clause
    assert params == {"name_like_0": "%cvs%", "name_like_1": "%walgreens%"}


def test_report_json_state_and_match_all_edges() -> None:
    assert reports._ensure_json_list([1]) == [1]
    assert reports._ensure_json_list("[1]") == [1]
    assert reports._ensure_json_list("{") == []
    assert reports._ensure_json_list('{"not":"a-list"}') == []
    assert reports._ensure_json_list(None) == []

    assert reports._ensure_json_dict({"a": 1}) == {"a": 1}
    assert reports._ensure_json_dict('{"a":1}') == {"a": 1}
    assert reports._ensure_json_dict("{") == {}
    assert reports._ensure_json_dict("[1]") == {}
    assert reports._ensure_json_dict(None) == {}

    assert reports._normalize_us_state_code(None) is None
    assert reports._normalize_us_state_code(" District   of Columbia ") == "DC"
    assert reports._normalize_us_state_code("unknown") is None

    state_statistics_rows = reports._canonicalize_pharmacy_state_stats_rows(
        [
            {"state": "California", "nppes_pharmacies": "2"},
            {"state": "CA", "nppes_pharmacies": "bad", "active_pharmacies": 3},
            {"state": "unknown", "nppes_pharmacies": 99},
        ]
    )
    california = next(
        state_statistics_row
        for state_statistics_row in state_statistics_rows
        if state_statistics_row["state"] == "CA"
    )
    assert california["nppes_pharmacies"] == 2
    assert california["active_pharmacies"] == 3

    assert reports._is_match_all_name_filters([]) is False
    assert reports._is_match_all_name_filters(["%", "%%"]) is True
    assert reports._is_match_all_name_filters(["%", "cvs"]) is False


def test_report_sql_builders_cover_validation_and_optional_dimensions() -> None:
    with pytest.raises(InvalidUsage, match="name_like"):
        reports._build_chain_summary_sql(
            names=[],
            has_staffing_helper=False,
            include_states=False,
        )

    count_sql, data_sql, params = reports._build_market_sql(
        scope="zip",
        sort="pharmacy_count",
        order="asc",
        include_staffing=True,
        has_partd=True,
        has_license=True,
        has_other_id=True,
        market_id_filter="zip:02139",
        state="MA",
        city="Cambridge",
        county="Middlesex",
        zip_code="02139",
        chain="independent",
        address_table_sql="mrf.entity_address_unified",
    )
    assert "license_by_npi" in data_sql
    assert "estimated_pharmacist_count_proxy" in data_sql
    assert params == {
        "state": "MA",
        "city": "Cambridge",
        "county": "middlesex",
        "zip_code": "02139",
        "market_id": "zip:02139",
    }

    count_sql, data_sql, params = reports._build_market_sql(
        scope="city",
        sort="unknown",
        order="desc",
        include_staffing=False,
        has_partd=False,
        has_license=False,
        has_other_id=False,
        market_id_filter=None,
        state=None,
        city=None,
        county=None,
        zip_code=None,
        chain="CVS",
        address_table_sql="mrf.npi_address",
    )
    assert "s.chain_name = :chain" in count_sql
    assert "LEFT(COALESCE(a.postal_code" in count_sql
    assert "estimated_pharmacist_count_proxy" in data_sql
    assert params == {"chain": "CVS"}


@pytest.mark.asyncio
async def test_report_pharmacy_context_optional_relations_and_missing_row(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        reports,
        "_is_table_available",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        reports,
        "_resolve_pharmacy_address_table_sql",
        AsyncMock(return_value="mrf.entity_address_unified"),
    )
    session = SimpleNamespace(
        execute=AsyncMock(return_value=_MappingResult(None))
    )

    assert await reports._fetch_pharmacy_context(
        session,
        npi=1234567890,
        as_of=datetime.date(2026, 7, 24),
    ) is None
    executed_sql = str(session.execute.await_args.args[0])
    assert "partd_by_npi" in executed_sql
    assert "license_by_npi" in executed_sql
    assert "other_identifier_count" in executed_sql
