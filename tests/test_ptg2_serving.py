# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json

import pytest

from api import ptg2_serving


class FakeResult:
    def __init__(self, scalar=None, rows=None):
        self._scalar = scalar
        self._rows = list(rows or [])

    def scalar(self):
        return self._scalar

    def __iter__(self):
        return iter(self._rows)


class FakeSession:
    def __init__(self, results):
        self._results = list(results)
        self.calls = []
        self.rollback_count = 0

    async def execute(self, *_args, **_kwargs):
        self.calls.append((_args, _kwargs))
        value = self._results.pop(0) if self._results else None
        if isinstance(value, Exception):
            raise value
        if isinstance(value, FakeResult):
            return value
        return FakeResult(value)

    async def rollback(self):
        self.rollback_count += 1


class FakePagination:
    limit = 25
    offset = 0


def _db_serving_session():
    return FakeSession(
        [
            None,
            "snap-db",
            {"table": "mrf.ptg2_serving_rate"},
            "mrf.ptg2_serving_rate",
            1,
            FakeResult(
                rows=[
                    {
                        "serving_rate_id": "rate-1",
                        "snapshot_id": "snap-db",
                        "plan_id": "010854205",
                        "plan_name": "Heartland",
                        "procedure_code": 123456,
                        "reported_code_system": "CPT",
                        "reported_code": "70551",
                        "billing_code": "70551",
                        "billing_code_type": "CPT",
                        "procedure_display_name": "MRI brain",
                        "provider_set_hash": "provider-set-1",
                        "provider_set_hashes": ["provider-set-a"],
                        "provider_count": 123,
                        "provider_set_count": 1,
                        "price_set_hash": "price-set-1",
                        "rate_pack_hash": "pack-1",
                        "prices": [
                            {
                                "negotiated_type": "negotiated",
                                "negotiated_rate": 450,
                                "service_code": ["23"],
                                "billing_code_modifier": ["TC", "ZZ"],
                            }
                        ],
                        "source_trace": [{"url": "https://example.test/rates.json.gz"}],
                        "confidence": {"network": "tic_rate_npi_tin"},
                    }
                ]
            ),
        ]
    )


def _fixture_payload():
    return {
        "version": 1,
        "snapshot_id": "snap-fixture",
        "plans": {"010854205": {"name": "Heartland Dental"}},
        "procedures": {
            "70551": {
                "code": "70551",
                "billing_code": "70551",
                "billing_code_type": "CPT",
                "name": "MRI brain",
            }
        },
        "providers": {
            "1": {
                "provider_ordinal": 1,
                "npi": 1234567890,
                "provider_name": "Example Imaging",
                "city": "Peoria",
                "state": "IL",
                "zip5": "61636",
            }
        },
        "rates": {
            "010854205": {
                "70551": [
                    {
                        "provider_ordinal": 1,
                        "prices": [{"negotiated_type": "negotiated", "negotiated_rate": 450}],
                        "source_trace": [{"url": "https://example.test/tic.json.gz"}],
                    }
                ]
            }
        },
    }


def test_search_ptg2_index_returns_prices_and_source_trace():
    index = ptg2_serving.PTG2ServingIndex.from_payload(_fixture_payload())

    payload = ptg2_serving.search_ptg2_index(index, plan_id="010854205", code="70551", state="IL")

    assert payload["pagination"]["total"] == 1
    item = payload["items"][0]
    assert item["npi"] == 1234567890
    assert item["tic_prices"][0]["negotiated_rate"] == 450
    assert item["source_trace"][0]["url"] == "https://example.test/tic.json.gz"
    assert item["confidence"]["network"] == "tic_rate_npi_tin"
    assert payload["query"]["source"] == "ptg2"


def test_db_serving_code_filter_accepts_signed_hp_procedure_codes():
    filters = []
    params = {}

    ptg2_serving._append_code_filter(
        filters,
        params,
        code="-1201887592",
        code_system="HP_PROCEDURE_CODE",
    )

    assert filters == ["procedure_code = :procedure_code"]
    assert params["procedure_code"] == -1201887592


def test_price_summary_groups_component_rates_and_counts_raw_prices():
    prices = [
        {
            "billing_class": "professional",
            "setting": "inpatient",
            "service_code": ["21", "22"],
            "billing_code_modifier": ["26"],
            "negotiated_rate": "83.09",
            "negotiated_type": "negotiated",
        },
        {
            "billing_class": "professional",
            "setting": "inpatient",
            "service_code": ["22", "21"],
            "billing_code_modifier": ["26"],
            "negotiated_rate": 83.09,
            "negotiated_type": "negotiated",
        },
        {
            "billing_class": "professional",
            "setting": "inpatient",
            "service_code": ["21"],
            "billing_code_modifier": ["TC"],
            "negotiated_rate": 158.58,
            "negotiated_type": "negotiated",
        },
        {
            "billing_class": "professional",
            "setting": "inpatient",
            "service_code": ["21"],
            "billing_code_modifier": [],
            "negotiated_rate": 241.67,
            "negotiated_type": "negotiated",
        },
    ]

    normalized = ptg2_serving._normalize_price_payload(prices)
    summary = ptg2_serving._summarize_price_payload(prices)

    assert len(normalized) == 3
    assert normalized[0]["service_code"] == ["21", "22"]
    assert normalized[0]["billing_code_modifier"] == ["26"]
    assert summary == [
        {
            "component": "global",
            "modifier": [],
            "rate": 241.67,
            "negotiated_type": "negotiated",
            "billing_class": "professional",
            "setting": "inpatient",
            "service_code": ["21"],
            "raw_price_count": 1,
        },
        {
            "component": "professional",
            "modifier": ["26"],
            "rate": 83.09,
            "negotiated_type": "negotiated",
            "billing_class": "professional",
            "setting": "inpatient",
            "service_code": ["21", "22"],
            "raw_price_count": 1,
        },
        {
            "component": "technical",
            "modifier": ["TC"],
            "rate": 158.58,
            "negotiated_type": "negotiated",
            "billing_class": "professional",
            "setting": "inpatient",
            "service_code": ["21"],
            "raw_price_count": 1,
        },
    ]


@pytest.mark.asyncio
async def test_load_current_ptg2_index_reads_snapshot_artifact(tmp_path):
    artifact = tmp_path / "snapshot.json"
    artifact.write_text(json.dumps(_fixture_payload()), encoding="utf-8")
    ptg2_serving.clear_ptg2_index_cache()
    session = FakeSession(["snap-fixture", artifact.resolve().as_uri()])

    index = await ptg2_serving.load_current_ptg2_index(session)

    assert index is not None
    assert index.snapshot_id == "snap-fixture"
    assert "010854205" in index.rates


@pytest.mark.asyncio
async def test_search_current_ptg2_index_reads_db_serving_table():
    session = _db_serving_session()

    payload = await ptg2_serving.search_current_ptg2_index(
        session,
        {"plan_id": "010854205", "code": "70551"},
        FakePagination(),
    )

    assert "source" not in payload["query"]
    assert "serving_table" not in payload["query"]
    assert "procedure_consolidation" not in payload["query"]
    item = payload["items"][0]
    assert item["procedure_code"] == 123456
    assert item["service_code"] == "70551"
    assert item["reported_code_system"] == "CPT"
    assert item["tic_prices"][0]["negotiated_rate"] == 450
    assert item["provider_count"] == 123
    assert "source_trace" not in item
    assert "confidence" not in item
    assert "price_set_hash" not in item
    assert "provider_set_hash" not in item


@pytest.mark.asyncio
async def test_search_current_ptg2_index_can_include_sources_without_debug_fields():
    session = _db_serving_session()

    payload = await ptg2_serving.search_current_ptg2_index(
        session,
        {"plan_id": "010854205", "code": "70551", "include_sources": "true"},
        FakePagination(),
    )

    assert payload["query"]["source"] == "ptg2_db"
    assert payload["query"]["serving_table"] == "mrf.ptg2_serving_rate"
    assert "procedure_consolidation" not in payload["query"]
    item = payload["items"][0]
    assert item["source_trace"][0]["url"] == "https://example.test/rates.json.gz"
    assert "confidence" not in item
    assert "price_set_hash" not in item


@pytest.mark.asyncio
async def test_search_current_ptg2_index_can_include_full_details():
    session = _db_serving_session()

    payload = await ptg2_serving.search_current_ptg2_index(
        session,
        {"plan_id": "010854205", "code": "70551", "include_details": "true"},
        FakePagination(),
    )

    assert payload["query"]["source"] == "ptg2_db"
    assert payload["query"]["procedure_consolidation"] == "HP_PROCEDURE_CODE"
    item = payload["items"][0]
    assert item["source_trace"][0]["url"] == "https://example.test/rates.json.gz"
    assert item["confidence"]["network"] == "tic_rate_npi_tin"
    assert item["price_set_hash"] == "price-set-1"
    assert item["provider_set_hash"] == "provider-set-1"


@pytest.mark.asyncio
async def test_search_current_ptg2_index_can_include_code_details():
    session = _db_serving_session()
    session._results.append(
        FakeResult(
            rows=[
                {
                    "code_system": "CPT",
                    "code": "70551",
                    "display_name": "MRI brain without contrast",
                    "short_description": "MRI brain without contrast",
                },
                {
                    "code_system": "POS",
                    "code": "23",
                    "display_name": "Emergency Room - Hospital",
                    "short_description": "Emergency Room - Hospital",
                },
                {
                    "code_system": "MODIFIER",
                    "code": "TC",
                    "display_name": "Technical component",
                    "short_description": "Technical component",
                },
            ]
        )
    )

    payload = await ptg2_serving.search_current_ptg2_index(
        session,
        {"plan_id": "010854205", "code": "70551", "include_code_details": "true"},
        FakePagination(),
    )

    item = payload["items"][0]
    assert item["billing_code_detail"]["display_name"] == "MRI brain without contrast"
    assert item["tic_prices"][0]["service_code_details"][0]["code_system"] == "POS"
    assert item["tic_prices"][0]["service_code_details"][0]["display_name"] == "Emergency Room - Hospital"
    assert item["tic_prices"][0]["billing_code_modifier_details"][0]["code_system"] == "MODIFIER"
    assert item["tic_prices"][0]["billing_code_modifier_details"][0]["display_name"] == "Technical component"
    assert item["tic_prices"][0]["billing_code_modifier_details"][1] == {
        "code_system": "MODIFIER",
        "code": "ZZ",
        "display_name": "Modifier ZZ",
        "short_description": None,
        "catalog_status": "missing",
    }
    assert "source_trace" not in item


@pytest.mark.asyncio
async def test_current_ptg2_snapshot_routes_by_plan_source_pointer():
    session = FakeSession(["snap-source"])

    snapshot_id = await ptg2_serving.resolve_current_ptg2_snapshot_id(
        session,
        {"plan_id": "010854205", "plan_market_type": "group", "source_key": "heartland_dental"},
    )

    assert snapshot_id == "snap-source"
    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert "ptg2_current_plan_source" in sql
    assert "JOIN mrf.ptg2_snapshot" in sql
    assert "cps.plan_market_type = :plan_market_type" in sql
    assert "cps.source_key = :source_key" in sql
    assert "s.status = 'published'" in sql
    assert "serving_index" in sql
    assert params["source_key"] == "heartland_dental"


@pytest.mark.asyncio
async def test_current_ptg2_snapshot_rolls_back_missing_source_pointer_before_fallback():
    session = FakeSession([RuntimeError("missing source pointer"), "snap-global"])

    snapshot_id = await ptg2_serving.resolve_current_ptg2_snapshot_id(
        session,
        {"plan_id": "010854205", "plan_market_type": "group"},
    )

    assert snapshot_id == "snap-global"
    assert session.rollback_count == 1
    assert len(session.calls) == 2


@pytest.mark.asyncio
async def test_ptg2_provider_procedures_uses_compact_snapshot_without_market_column():
    session = FakeSession(
        [
            "snap-token",
            {
                "table": "mrf.ptg2_serving_rate_compact_token",
                "price_table": "mrf.ptg2_price_set_token",
                "procedure_table": "mrf.ptg2_procedure_token",
                "provider_set_table": "mrf.ptg2_provider_set_token",
                "provider_group_member_table": "mrf.ptg2_provider_group_member_token",
            },
            "mrf.ptg2_serving_rate_compact_token",
            "mrf.ptg2_provider_set_token_group_hashes_gin_idx",
            1,
            FakeResult(
                rows=[
                    {
                        "serving_rate_id": "rate-1",
                        "snapshot_id": "snap-token",
                        "plan_id": "010854205",
                        "plan_name": None,
                        "plan_id_type": None,
                        "plan_market_type": None,
                        "issuer_name": None,
                        "plan_sponsor_name": None,
                        "procedure_code": None,
                        "reported_code_system": "CPT",
                        "reported_code": "99213",
                        "billing_code": "99213",
                        "billing_code_type": "CPT",
                        "procedure_name": "Office visit",
                        "procedure_description": "Established office visit",
                        "provider_set_hash": "provider-set-1",
                        "provider_count": 2,
                        "provider_set_count": None,
                        "price_set_hash": "price-set-1",
                        "prices": [{"negotiated_type": "negotiated", "negotiated_rate": 101.42}],
                    }
                ]
            ),
        ]
    )

    payload = await ptg2_serving.search_ptg2_provider_procedures(
        session,
        1083311500,
        {
            "plan_id": "010854205",
            "plan_market_type": "group",
            "source_key": "heartland_dental",
            "code": "99213",
            "code_system": "CPT",
        },
        FakePagination(),
    )

    assert payload["items"][0]["npi"] == 1083311500
    assert payload["items"][0]["reported_code"] == "99213"
    assert payload["items"][0]["tic_prices"][0]["negotiated_rate"] == 101.42
    count_sql = str(session.calls[4][0][0])
    row_sql = str(session.calls[5][0][0])
    assert "r.plan_market_type" not in count_sql
    assert "r.plan_market_type" not in row_sql
    assert "NULL::varchar AS plan_market_type" in row_sql
    assert "r.provider_set_count" not in row_sql
    assert "NULL::integer AS provider_set_count" in row_sql
    assert session.calls[4][0][1]["plan_id"] == "010854205"


@pytest.mark.asyncio
async def test_ptg2_provider_procedures_returns_no_match_after_snapshot_resolves():
    session = FakeSession(
        [
            "snap-token",
            {
                "table": "mrf.ptg2_serving_rate_compact_token",
                "price_table": "mrf.ptg2_price_set_token",
                "procedure_table": "mrf.ptg2_procedure_token",
                "provider_set_table": "mrf.ptg2_provider_set_token",
                "provider_group_member_table": "mrf.ptg2_provider_group_member_token",
            },
            "mrf.ptg2_serving_rate_compact_token",
            "mrf.ptg2_provider_set_token_group_hashes_gin_idx",
            0,
        ]
    )

    payload = await ptg2_serving.search_ptg2_provider_procedures(
        session,
        1083311500,
        {"plan_id": "010854205", "code": "99213", "code_system": "CPT", "include_details": "true"},
        FakePagination(),
    )

    assert payload["items"] == []
    assert payload["pagination"]["total"] == 0
    assert payload["query"]["snapshot_id"] == "snap-token"
    assert payload["query"]["status"] == "no_match"
    assert payload["query"]["source"] == "ptg2_db"


@pytest.mark.asyncio
async def test_ptg2_provider_procedures_accepts_hashed_provider_gin_index_name():
    session = FakeSession(
        [
            "snap-token",
            {
                "table": "mrf.ptg2_serving_rate_compact_token",
                "price_table": "mrf.ptg2_price_set_token",
                "procedure_table": "mrf.ptg2_procedure_token",
                "provider_set_table": "mrf.ptg2_provider_set_token",
                "provider_group_member_table": "mrf.ptg2_provider_group_member_token",
            },
            "mrf.ptg2_serving_rate_compact_token",
            None,
            True,
            0,
        ]
    )

    payload = await ptg2_serving.search_ptg2_provider_procedures(
        session,
        1083311500,
        {"plan_id": "010854205", "code": "99213", "code_system": "CPT"},
        FakePagination(),
    )

    gin_sql = str(session.calls[4][0][0])
    assert "pg_index" in gin_sql
    assert "index_am.amname = 'gin'" in gin_sql
    assert session.calls[4][0][1]["table_name"] == "mrf.ptg2_provider_set_token"
    assert session.calls[4][0][1]["column_name"] == "provider_group_hashes"
    assert payload["query"]["provider_reverse_index"] is True
    assert payload["query"]["status"] == "no_match"


@pytest.mark.asyncio
async def test_compact_serving_uses_snapshot_price_and_procedure_tables():
    session = FakeSession([FakeResult(rows=[])])
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_rate_compact_token",
        price_table="mrf.ptg2_price_set_token",
        procedure_table="mrf.ptg2_procedure_token",
    )

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        tables,
        "snap-token",
        {"plan_id": "010854205", "code": "70551"},
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "010854205", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[0][0][0])
    assert "FROM mrf.ptg2_price_set_token ps" in sql
    assert "FROM mrf.ptg2_procedure_token proc" in sql


@pytest.mark.asyncio
async def test_compact_serving_geo_search_allows_missing_specialty():
    session = FakeSession([FakeResult(rows=[])])

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        ptg2_serving.PTG2ServingTables(),
        "snap-token",
        {"plan_id": "010854205", "code": "70551", "zip5": "60601"},
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "010854205", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert "EXISTS (" in sql
    assert "provider_filter_npi" in sql
    assert "JOIN LATERAL (" in sql
    assert "FROM mrf.npi_address addr_filter" in sql
    assert "addr_filter.npi = provider_filter_npi.npi" in sql
    assert "LEFT(COALESCE(addr_filter.postal_code, ''), 5) = :zip5" in sql
    assert "npi_taxonomy" not in sql
    assert "specialty_like" not in params


@pytest.mark.asyncio
async def test_compact_serving_coordinate_search_filters_npi_addresses():
    session = FakeSession([FakeResult(rows=[])])

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        ptg2_serving.PTG2ServingTables(),
        "snap-token",
        {
            "plan_id": "010854205",
            "code": "70551",
            "lat": 29.7604,
            "long": -95.3698,
            "radius_miles": 10.0,
        },
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "010854205", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert "FROM mrf.npi_address addr_filter" in sql
    assert "addr_filter.lat::float8 BETWEEN :geo_min_lat AND :geo_max_lat" in sql
    assert "addr_filter.long::float8 BETWEEN :geo_min_long AND :geo_max_long" in sql
    assert ") <= :geo_radius_miles" in sql
    assert params["geo_lat"] == 29.7604
    assert params["geo_long"] == -95.3698
    assert params["geo_radius_miles"] == 10.0


@pytest.mark.asyncio
async def test_compact_serving_include_providers_expands_without_geo_filter():
    session = FakeSession(
        [
            FakeResult(
                rows=[
                    {
                        "npi": 1234567890,
                        "location_hash": "loc-1",
                        "state": "IL",
                        "city": "Peoria",
                        "zip5": "61636",
                        "location_source": "nppes",
                        "location_confidence_code": "nppes_practice_location",
                        "address_payload": {"line1": "1 Main St"},
                        "taxonomy_codes": [],
                        "specialties": [],
                        "provider_name": "Example Provider",
                        "procedure_code": None,
                        "reported_code_system": "RC",
                        "reported_code": "450",
                        "billing_code": "450",
                        "billing_code_type": "RC",
                        "procedure_display_name": "Emergency Room",
                        "procedure_name": "Emergency Room",
                        "procedure_description": "Emergency Room",
                        "provider_set_hashes": ["provider-set-1"],
                        "rate_count": 1,
                        "prices": [{"negotiated_type": "percentage", "negotiated_rate": 60}],
                        "source_trace": [],
                    }
                ]
            )
        ]
    )

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        ptg2_serving.PTG2ServingTables(),
        "snap-token",
        {"plan_id": "010854205", "code": "450", "include_providers": "true"},
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "010854205", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload["query"]["result_granularity"] == "provider"
    assert payload["query"]["include_providers"] is True
    item = payload["items"][0]
    assert item["npi"] == 1234567890
    assert item["provider_name"] == "Example Provider"
    assert item["state"] == "IL"
    assert item["tic_prices"][0]["negotiated_rate"] == 60
    sql = str(session.calls[0][0][0])
    assert "LEFT JOIN mrf.npi_address addr" in sql
    assert "addr.npi = pgm.npi" in sql
    assert "LEFT(COALESCE(addr.postal_code, ''), 5) = :zip5" not in sql


@pytest.mark.asyncio
async def test_compact_serving_specialty_search_joins_nucc_without_geo():
    session = FakeSession([FakeResult(rows=[])])

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        ptg2_serving.PTG2ServingTables(),
        "snap-token",
        {"plan_id": "010854205", "code": "70551", "specialty": "dentist"},
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "010854205", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert "EXISTS (" in sql
    assert "provider_filter_npi" in sql
    assert "FROM mrf.npi_taxonomy nt_filter" in sql
    assert "JOIN mrf.nucc_taxonomy nucc_filter" in sql
    assert "FROM mrf.ptg2_provider_set_component psc_filter" in sql
    assert "pgm_filter.provider_group_hash = psc_filter.provider_group_hash" in sql
    assert "WHERE nt_filter.npi = provider_filter_npi.npi" in sql
    assert params["specialty_like"] == "%dentist%"


def test_warm_cache_benchmark_fixture_p95_gate():
    index = ptg2_serving.PTG2ServingIndex.from_payload(_fixture_payload())

    result = ptg2_serving.warm_cache_benchmark(index, request_count=100)

    assert result["request_count"] == 100
    assert result["p95_ms"] <= 50.0
    assert result["passed"] is True
