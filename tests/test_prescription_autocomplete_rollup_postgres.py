# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import os
import uuid
from types import SimpleNamespace

import asyncpg
import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from db.prescription_autocomplete_rollup_sql import (
    prescription_autocomplete_rollup_insert_sql,
)
from tests.test_prescription_autocomplete_postgres import (
    _plan_nodes,
    _prepared_plans,
)
from tests.test_pricing_api import (
    FakeResult,
    autocomplete_prescriptions,
    make_request,
    pricing_module,
)


ROLLUP_TABLE = "pricing_provider_rx_rollup"


async def _rollup_query(*, rollup_available=True):
    prescription_row_by_field = {
        "_pagination_total": 1,
        "rx_code_system": "HP_RX_CODE",
        "rx_code": "RX-1",
        "rx_name": "Aspirin name",
        "generic_name": "Aspirin generic",
        "brand_name": "Aspirin brand",
        "total_claims": 1,
        "total_drug_cost": 1,
        "total_benes": 1,
    }
    request = make_request(
        [
            FakeResult(scalar="mrf.pricing_provider_prescription"),
            FakeResult(scalar=None),
            FakeResult(
                rows=[prescription_row_by_field],
                scalar=f"mrf.{ROLLUP_TABLE}" if rollup_available else None,
            ),
            FakeResult(rows=[prescription_row_by_field]),
        ],
        args={"q": "aspirin", "year": "2023", "limit": "1"},
    )
    await autocomplete_prescriptions(request)
    matching_queries = [
        execution_args[0]
        for execution_args, _execution_kwargs in request.ctx.sa_session.executions
        if "pricing_provider_prescription" in str(execution_args[0])
    ]
    assert len(matching_queries) == 1
    return matching_queries[0]


async def _create_provider_table(connection, relation):
    await connection.execute(
        f"""
        CREATE TABLE {relation} (
            npi bigint NOT NULL,
            year integer NOT NULL,
            rx_code_system text NOT NULL,
            rx_code text NOT NULL,
            rx_name text,
            generic_name text,
            brand_name text,
            total_claims double precision,
            total_drug_cost numeric,
            total_benes double precision
        )
        """
    )


async def _create_current_autocomplete_index(connection, relation):
    await connection.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    await connection.execute(
        f"""
        CREATE INDEX pricing_provider_rx_autocomplete_trgm_idx
            ON {relation} USING gin (
                lower(COALESCE(rx_name, '')) gin_trgm_ops,
                lower(COALESCE(generic_name, '')) gin_trgm_ops,
                lower(COALESCE(brand_name, '')) gin_trgm_ops,
                lower(COALESCE(rx_code, '')) gin_trgm_ops
            )
            WHERE rx_code_system = 'HP_RX_CODE'
        """
    )


async def _seed_stable_tie_rows(connection, relation):
    await connection.execute(
        f"""
        INSERT INTO {relation}
            (npi, year, rx_code_system, rx_code, rx_name, generic_name,
             brand_name, total_claims, total_drug_cost, total_benes)
        VALUES
            (2000000001, 2023, 'HP_RX_CODE', 'TIE-B', 'Stable tie', NULL,
             NULL, 10, 20, 30),
            (2000000002, 2023, 'HP_RX_CODE', 'TIE-A', 'Stable tie', NULL,
             NULL, 10, 20, 30)
        """
    )


async def _create_rollup_fixture(connection, relation, rollup_relation):
    await _create_provider_table(connection, relation)
    await connection.execute(
        f"""
        INSERT INTO {relation}
            (npi, year, rx_code_system, rx_code, rx_name, generic_name,
             brand_name, total_claims, total_drug_cost, total_benes)
        SELECT
            1000000000 + row_number,
            CASE WHEN row_number > 95000 THEN 2022 ELSE 2023 END,
            'HP_RX_CODE',
            CASE WHEN row_number % 20 = 4 THEN 'aspirin-code'
                 ELSE 'RX-' || row_number % 20 END,
            CASE WHEN row_number % 40 = 1 THEN 'Aspirin name'
                 WHEN row_number % 20 = 1 THEN 'Different name'
                 ELSE 'unrelated name ' || row_number % 20 END,
            CASE WHEN row_number % 20 = 2 THEN 'Aspirin generic'
                 ELSE 'unrelated generic ' || row_number % 20 END,
            CASE WHEN row_number % 20 = 3 THEN 'Aspirin brand'
                 ELSE 'unrelated brand ' || row_number % 20 END,
            row_number % 7 + 1,
            row_number % 11 + 0.25,
            row_number % 5 + 1
        FROM generate_series(1, 100000) AS row_number
        """
    )
    await _seed_stable_tie_rows(connection, relation)
    await _create_current_autocomplete_index(connection, relation)
    await connection.execute(
        f"""
        CREATE TABLE {rollup_relation} (
            year integer NOT NULL,
            rx_code_system text NOT NULL,
            rx_code text NOT NULL,
            variant_id bigint NOT NULL,
            rx_name text,
            generic_name text,
            brand_name text,
            total_claims double precision,
            total_drug_cost numeric,
            total_benes double precision,
            source_relation_fingerprint varchar(128) NOT NULL,
            PRIMARY KEY (year, rx_code_system, rx_code, variant_id)
        )
        """
    )
    schema = relation.split('"')[1]
    await connection.execute(
        prescription_autocomplete_rollup_insert_sql(
            schema=schema,
            rollup_table=ROLLUP_TABLE,
            provider_table="pricing_provider_prescription",
        )
    )


async def _query_rows(engine, query):
    async with engine.connect() as query_connection:
        query_result = await query_connection.execute(query)
        return [
            dict(query_row) for query_row in query_result.mappings().all()
        ]


async def _load_autocomplete_page(
    engine,
    schema,
    source_table,
    **query_arguments,
):
    pagination = SimpleNamespace(
        limit=query_arguments.pop("limit", 1),
        offset=query_arguments.pop("offset", 0),
    )
    async with engine.connect() as query_connection:
        query_connection = await query_connection.execution_options(
            schema_translate_map={"mrf": schema}
        )
        return await pricing_module._load_prescription_autocomplete_page(
            query_connection,
            source_table,
            terminology_internal_codes=query_arguments.pop(
                "terminology_internal_codes", []
            ),
            order_by=query_arguments.pop("order_by", "total_claims"),
            order=query_arguments.pop("order", "desc"),
            pagination=pagination,
            **query_arguments,
        )


async def _assert_rollup_semantic_parity(engine, schema):
    parity_cases = (
        {"search_query": "aspirin", "year": 2023},
        {"search_query": "aspirin", "year": 2022},
        {"search_query": "aspirin-code", "year": 2023},
        {
            "search_query": "mapped-term",
            "year": 2023,
            "terminology_internal_codes": ["RX-7"],
        },
        {"search_query": "not-present", "year": 2023},
        {"search_query": "aspirin", "year": 2023, "offset": 1},
        {"search_query": "aspirin", "year": 2023, "offset": 100},
        {
            "search_query": "aspirin",
            "year": 2023,
            "limit": 3,
            "order_by": "generic_name",
            "order": "asc",
        },
    )
    for query_arguments in parity_cases:
        provider_result = await _load_autocomplete_page(
            engine,
            schema,
            pricing_module.provider_prescription_table,
            **query_arguments,
        )
        rollup_result = await _load_autocomplete_page(
            engine,
            schema,
            pricing_module.provider_prescription_autocomplete_table,
            **query_arguments,
        )
        assert rollup_result == provider_result, query_arguments


async def _assert_stable_tie_pagination(engine, schema):
    """Require tied offset pages to use the stable prescription key."""

    source_pages = []
    for source_table in (
        pricing_module.provider_prescription_table,
        pricing_module.provider_prescription_autocomplete_table,
    ):
        page_codes = []
        for offset in (0, 1):
            rows, total = await _load_autocomplete_page(
                engine,
                schema,
                source_table,
                search_query="stable tie",
                year=2023,
                offset=offset,
            )
            assert total == 2
            page_codes.append(rows[0]["rx_code"])
        source_pages.append(page_codes)
    assert source_pages == [["TIE-A", "TIE-B"], ["TIE-A", "TIE-B"]]


def _assert_benchmark_plans(rollup_plan, provider_plan):
    provider_nodes = list(_plan_nodes(provider_plan["Plan"]))
    assert sum(
        node.get("Index Name") == "pricing_provider_rx_autocomplete_trgm_idx"
        for node in provider_nodes
    ) == 4
    assert provider_plan["Execution Time"] > rollup_plan["Execution Time"] * 5
    assert rollup_plan["Execution Time"] <= 5.0
    assert not any(
        node.get("Relation Name") == "pricing_provider_prescription"
        for node in _plan_nodes(rollup_plan["Plan"])
    )


@pytest.mark.asyncio
async def test_production_autocomplete_uses_exact_bounded_rollup(monkeypatch):
    """Keep exact provider matching without request-time provider aggregation."""

    dsn = os.getenv("HLTHPRT_PRESCRIPTION_AUTOCOMPLETE_POSTGRES_DSN")
    if not dsn:
        pytest.skip("requires disposable PostgreSQL")

    connection = await asyncpg.connect(dsn)
    engine = create_async_engine(
        dsn.replace("postgresql://", "postgresql+asyncpg://", 1),
        pool_size=1,
        max_overflow=0,
    )
    schema = f"prescription_rollup_{uuid.uuid4().hex}"
    relation = f'"{schema}".pricing_provider_prescription'
    rollup_relation = f'"{schema}".{ROLLUP_TABLE}'
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        await _create_rollup_fixture(connection, relation, rollup_relation)
        await connection.execute(f"ANALYZE {relation}")
        await connection.execute(f"ANALYZE {rollup_relation}")
        monkeypatch.setattr(
            pricing_module,
            "_PRESCRIPTION_AUTOCOMPLETE_FINGERPRINT_SQL",
            (
                "COALESCE(to_regclass('"
                f'"{schema}"."pricing_provider_prescription"'
                "')::oid::text, '0')"
            ),
        )

        fast_query = (await _rollup_query()).execution_options(
            schema_translate_map={"mrf": schema}
        )
        fallback_query = (
            await _rollup_query(rollup_available=False)
        ).execution_options(schema_translate_map={"mrf": schema})
        rollup_plan = (await _prepared_plans(engine, fast_query))[0][-1][0]
        provider_plan = (await _prepared_plans(engine, fallback_query))[0][-1][0]

        await _assert_rollup_semantic_parity(engine, schema)
        await _assert_stable_tie_pagination(engine, schema)
        assert await _query_rows(engine, fast_query) == await _query_rows(
            engine,
            fallback_query,
        )
        _assert_benchmark_plans(rollup_plan, provider_plan)
    finally:
        await engine.dispose()
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await connection.close()
