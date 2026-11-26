# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Unit checks for asyncpg prepared-statement cache sizing and ANY() SQL shapes."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import Column, MetaData, String, Table, select
from sqlalchemy.dialects.postgresql import asyncpg as sa_asyncpg

from api.endpoint import npi as npi_endpoint
from api.endpoint import site_intelligence
from api.network_address_scope import _npi_batch_address_filters
from api.provider_detail_sql import (
    _npi_detail_address_filters,
    any_bigint_filter,
    any_int_filter,
    any_string_filter,
    build_external_rx_crosswalk_query,
    build_provider_cost_profiles_query,
    build_provider_geography_clauses,
)
from db.connection import Database


def _compile_asyncpg_sql(statement) -> str:
    return str(statement.compile(dialect=sa_asyncpg.dialect()))


@pytest.mark.asyncio
async def test_database_connect_uses_bounded_api_reader_and_worker_cache_defaults(monkeypatch):
    captured_kwargs_list: list[dict] = []

    def fake_create_engine(_url, **kwargs):
        captured_kwargs_list.append(kwargs)
        return SimpleNamespace(connect=AsyncMock())

    monkeypatch.delenv("HLTHPRT_DB_DRIVER", raising=False)
    monkeypatch.delenv("HLTHPRT_DB_PREPARED_STATEMENT_CACHE_SIZE", raising=False)
    monkeypatch.delenv("HLTHPRT_DB_QUERY_CACHE_SIZE", raising=False)
    monkeypatch.setattr("db.connection.create_async_engine", fake_create_engine)
    monkeypatch.setattr("db.connection.async_sessionmaker", lambda *_a, **_kw: lambda: "session")

    worker_db = Database()
    await worker_db.connect()

    api_db = Database()
    api_db._api_server_started = True
    await api_db.connect()

    reader_db = Database()
    reader_db._reader_login = ("reader_user", "")
    await reader_db.connect()

    worker_kwargs, api_kwargs, reader_kwargs = captured_kwargs_list
    assert worker_kwargs["query_cache_size"] == 500
    assert "connect_args" not in worker_kwargs
    assert api_kwargs["query_cache_size"] == 1000
    assert api_kwargs["connect_args"] == {
        "server_settings": {"jit": "off"},
        "prepared_statement_cache_size": 256,
    }
    assert reader_kwargs["query_cache_size"] == 1000
    assert reader_kwargs["connect_args"] == {
        "server_settings": {"jit": "off"},
        "prepared_statement_cache_size": 256,
    }


@pytest.mark.asyncio
async def test_database_connect_validates_cache_env_overrides_and_skips_psycopg(monkeypatch):
    captured_kwargs_list: list[dict] = []

    def fake_create_engine(_url, **kwargs):
        captured_kwargs_list.append(kwargs)
        return SimpleNamespace(connect=AsyncMock())

    monkeypatch.setattr("db.connection.create_async_engine", fake_create_engine)
    monkeypatch.setattr("db.connection.async_sessionmaker", lambda *_a, **_kw: lambda: "session")

    monkeypatch.setenv("HLTHPRT_DB_DRIVER", "asyncpg")
    monkeypatch.setenv("HLTHPRT_DB_PREPARED_STATEMENT_CACHE_SIZE", "512")
    monkeypatch.setenv("HLTHPRT_DB_QUERY_CACHE_SIZE", "2000")
    await Database().connect()
    assert captured_kwargs_list[-1]["query_cache_size"] == 2000
    assert captured_kwargs_list[-1]["connect_args"] == {"prepared_statement_cache_size": 512}

    monkeypatch.setenv("HLTHPRT_DB_DRIVER", "psycopg")
    await Database().connect()
    assert "connect_args" not in captured_kwargs_list[-1]

    monkeypatch.setenv("HLTHPRT_DB_PREPARED_STATEMENT_CACHE_SIZE", "5000")
    with pytest.raises(ValueError, match="HLTHPRT_DB_PREPARED_STATEMENT_CACHE_SIZE"):
        await Database().connect()

    monkeypatch.setenv("HLTHPRT_DB_PREPARED_STATEMENT_CACHE_SIZE", "256")
    monkeypatch.setenv("HLTHPRT_DB_QUERY_CACHE_SIZE", "-1")
    with pytest.raises(ValueError, match="HLTHPRT_DB_QUERY_CACHE_SIZE"):
        await Database().connect()


@pytest.mark.asyncio
async def test_site_intelligence_zcta_queries_compile_to_single_any_array_shape(monkeypatch):
    executed_statements_by_run: dict[str, list[str]] = {}

    class _FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def all(self):
            return list(self._rows)

    async def is_table_present(_session, _model):
        return True

    async def run_with_zips(label: str, zip_rows):
        captured_sqls: list[str] = []
        responses = [zip_rows, [], [], [], [], [], [], [], [], []]

        class _RecordingSession:
            async def execute(self, stmt):
                captured_sqls.append(_compile_asyncpg_sql(stmt))
                return _FakeResult(responses.pop(0))

        monkeypatch.setattr(site_intelligence, "_is_table_cached", is_table_present)
        monkeypatch.setattr(site_intelligence, "_get_session", lambda _request: _RecordingSession())
        request = SimpleNamespace(args={"lat": "41.89", "lng": "-87.63"}, ctx=SimpleNamespace())
        await site_intelligence.get_site_score(request)
        executed_statements_by_run[label] = captured_sqls

    await run_with_zips(
        "one_zip",
        [SimpleNamespace(zip_code="60654", state="IL", latitude=41.892, longitude=-87.637)],
    )
    await run_with_zips(
        "three_zips",
        [
            SimpleNamespace(zip_code="60654", state="IL", latitude=41.892, longitude=-87.637),
            SimpleNamespace(zip_code="60610", state="IL", latitude=41.905, longitude=-87.632),
            SimpleNamespace(zip_code="60611", state="IL", latitude=41.895, longitude=-87.624),
        ],
    )

    assert executed_statements_by_run["one_zip"] == executed_statements_by_run["three_zips"]
    any_queries = [sql for sql in executed_statements_by_run["one_zip"] if "= ANY ($1::VARCHAR[])" in sql]
    assert len(any_queries) == 5


@pytest.mark.asyncio
async def test_get_plans_by_npi_uses_buffered_all_instead_of_streaming_cursor(monkeypatch):
    class _BufferedQuery:
        def __init__(self):
            self.all_calls = 0

        def where(self, *_args, **_kwargs):
            return self

        def order_by(self, *_args, **_kwargs):
            return self

        async def all(self):
            self.all_calls += 1
            return [
                (SimpleNamespace(to_json_dict=lambda: {"npi": 123}), SimpleNamespace(to_json_dict=lambda: {"id": 9}))
            ]

        def iterate(self):
            raise AssertionError("get_plans_by_npi should not open a streaming cursor")

    buffered_query = _BufferedQuery()
    monkeypatch.setattr(npi_endpoint, "db", SimpleNamespace(select=lambda *_args, **_kwargs: buffered_query))

    http_response = await npi_endpoint.get_plans_by_npi(SimpleNamespace(), "123")
    assert http_response.status == 200
    assert buffered_query.all_calls == 1


def test_provider_filter_builders_compile_to_single_any_array_shape():
    unified_table = npi_endpoint.EntityAddressUnified.__table__
    legacy_table = npi_endpoint.NPIAddress.__table__

    batch_unified_1 = _compile_asyncpg_sql(
        select(unified_table.c.location_key).where(
            *_npi_batch_address_filters(npi_endpoint.EntityAddressUnified, unified_table, [1234567890])
        )
    )
    batch_unified_3 = _compile_asyncpg_sql(
        select(unified_table.c.location_key).where(
            *_npi_batch_address_filters(
                npi_endpoint.EntityAddressUnified, unified_table, [1234567890, 1234567891, 1234567892]
            )
        )
    )
    assert batch_unified_1 == batch_unified_3
    assert "= ANY ($1::BIGINT[])" in batch_unified_1

    detail_unified_1 = _compile_asyncpg_sql(
        select(unified_table.c.location_key).where(
            *_npi_detail_address_filters(
                npi_endpoint.EntityAddressUnified, unified_table, 1234567890, None, ["location:k1"]
            )
        )
    )
    detail_unified_3 = _compile_asyncpg_sql(
        select(unified_table.c.location_key).where(
            *_npi_detail_address_filters(
                npi_endpoint.EntityAddressUnified,
                unified_table,
                1234567890,
                None,
                ["location:k1", "location:k2", "location:k3"],
            )
        )
    )
    assert detail_unified_1 == detail_unified_3
    assert "= ANY ($2::VARCHAR[])" in detail_unified_1

    _, legacy_filter_1 = npi_endpoint._address_identity_filter(
        npi_endpoint.NPIAddress, legacy_table, ["legacy:1234567890:primary:77"]
    )
    _, legacy_filter_3 = npi_endpoint._address_identity_filter(
        npi_endpoint.NPIAddress,
        legacy_table,
        ["legacy:1234567890:primary:77", "legacy:1234567890:primary:78", "legacy:1234567890:primary:79"],
    )
    legacy_sql_1 = _compile_asyncpg_sql(select(legacy_table.c.checksum).where(legacy_filter_1))
    legacy_sql_3 = _compile_asyncpg_sql(select(legacy_table.c.checksum).where(legacy_filter_3))
    assert legacy_sql_1 == legacy_sql_3
    assert "= ANY ($1::BIGINT[])" in legacy_sql_1

    source_sql_1 = _compile_asyncpg_sql(npi_endpoint._provider_directory_source_detail_statement(["src-a"]))
    source_sql_3 = _compile_asyncpg_sql(
        npi_endpoint._provider_directory_source_detail_statement(["src-a", "src-b", "src-c"])
    )
    assert source_sql_1 == source_sql_3
    assert "= ANY ($1::VARCHAR[])" in source_sql_1


@pytest.mark.asyncio
async def test_provider_hydration_and_ffs_helpers_compile_to_single_any_array_shape(monkeypatch):
    captured_sqls: list[str] = []

    class _EmptyResult:
        def all(self):
            return []

        def scalars(self):
            return []

    async def fake_execute(statement, **_kwargs):
        captured_sqls.append(_compile_asyncpg_sql(statement))
        return _EmptyResult()

    fake_session = SimpleNamespace(execute=AsyncMock(side_effect=fake_execute))
    monkeypatch.setattr(npi_endpoint, "_execute_stmt", fake_execute)

    await npi_endpoint._build_npi_identity_details_map([1111111111], session=fake_session)
    await npi_endpoint._build_npi_identity_details_map([1111111111, 2222222222, 3333333333], session=fake_session)
    assert captured_sqls[-2] == captured_sqls[-1]
    assert captured_sqls[-1].count("= ANY ($1::BIGINT[])") == 3

    await npi_endpoint._fetch_other_names_map([1111111111])
    await npi_endpoint._fetch_other_names_map([1111111111, 2222222222, 3333333333])
    assert captured_sqls[-2] == captured_sqls[-1]
    assert "= ANY ($1::BIGINT[])" in captured_sqls[-1]

    await npi_endpoint._fetch_provider_enrichment_ffs_rows([1111111111])
    await npi_endpoint._fetch_provider_enrichment_ffs_rows([1111111111, 2222222222, 3333333333])
    assert captured_sqls[-2] == captured_sqls[-1]
    assert "= ANY ($1::BIGINT[])" in captured_sqls[-1]


def test_plan_and_pricing_any_filters_compile_to_single_array_shapes():
    from db.models import Plan, PricingProviderProcedureCostProfile

    plan_table = Plan.__table__
    plan_sql_1 = _compile_asyncpg_sql(
        select(plan_table.c.plan_id).where(
            any_int_filter(plan_table.c.issuer_id, [101], "issuer_ids"),
            any_string_filter(plan_table.c.state, ["IL"], "zip_states"),
        )
    )
    plan_sql_3 = _compile_asyncpg_sql(
        select(plan_table.c.plan_id).where(
            any_int_filter(plan_table.c.issuer_id, [101, 102, 103], "issuer_ids"),
            any_string_filter(plan_table.c.state, ["IL", "NY", "TX"], "zip_states"),
        )
    )
    assert plan_sql_1 == plan_sql_3
    assert "= ANY ($1::INTEGER[])" in plan_sql_1
    assert "= ANY ($2::VARCHAR[])" in plan_sql_1

    cost_table = PricingProviderProcedureCostProfile.__table__
    geo_clauses_1 = build_provider_geography_clauses(cost_table, {"zip5": ["60654"]})
    geo_clauses_3 = build_provider_geography_clauses(cost_table, {"zip5": ["60654", "60610", "60611"]})
    cost_sql_1 = _compile_asyncpg_sql(
        build_provider_cost_profiles_query(
            cost_table, year=2025, npis=[1111111111], internal_codes=[99213], geography_clauses=geo_clauses_1
        )
    )
    cost_sql_3 = _compile_asyncpg_sql(
        build_provider_cost_profiles_query(
            cost_table,
            year=2025,
            npis=[1111111111, 2222222222, 3333333333],
            internal_codes=[99213, 99214, 99215],
            geography_clauses=geo_clauses_3,
        )
    )
    assert cost_sql_1 == cost_sql_3
    assert cost_sql_1.count("::BIGINT[])") == 2
    assert "::VARCHAR[])" in cost_sql_1

    crosswalk_table = Table(
        "code_crosswalk",
        MetaData(),
        Column("from_system", String),
        Column("from_code", String),
        Column("to_system", String),
        Column("to_code", String),
    )
    rx_sql_1 = _compile_asyncpg_sql(build_external_rx_crosswalk_query(crosswalk_table, "RX", ["A"], ["NDC"]))
    rx_sql_3 = _compile_asyncpg_sql(
        build_external_rx_crosswalk_query(crosswalk_table, "RX", ["A", "B", "C"], ["NDC", "RXNORM"])
    )
    assert rx_sql_1 == rx_sql_3
    assert rx_sql_1.count("::VARCHAR[])") == 4
