# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
import json
import os
import uuid
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import ClauseElement, Executable

from tests.test_pricing_api import (
    FakeResult,
    autocomplete_prescriptions,
    make_request,
    pricing_module,
)


INDEX_NAME = "pricing_provider_rx_autocomplete_trgm_idx"
drug_claims = importlib.import_module("process.drug_claims")


class _AsyncpgDatabase:
    def __init__(self, connection):
        self.connection = connection

    async def scalar(self, statement):
        return await self.connection.fetchval(str(statement))

    async def status(self, statement):
        return await self.connection.execute(str(statement))


class _PostgresExplain(Executable, ClauseElement):
    inherit_cache = False

    def __init__(self, statement):
        self.statement = statement
        self._execution_options = statement._execution_options


@compiles(_PostgresExplain, "postgresql")
def _compile_postgres_explain(element, compiler, **kwargs):
    return "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + compiler.process(
        element.statement,
        **kwargs,
    )


def _plan_nodes(node):
    yield node
    for child in node.get("Plans", ()):
        yield from _plan_nodes(child)


async def _production_query():
    request = make_request(
        [
            FakeResult(scalar="mrf.pricing_provider_prescription"),
            FakeResult(scalar=None),
            FakeResult(scalar=None),
            FakeResult(rows=[]),
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


async def _create_autocomplete_relation(connection, relation):
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
    await connection.execute(
        f"""
        INSERT INTO {relation}
            (npi, year, rx_code_system, rx_code, rx_name, generic_name,
             brand_name, total_claims, total_drug_cost, total_benes)
        SELECT 1000000000 + row_number,
               2023,
               'HP_RX_CODE',
               CASE WHEN row_number = 4 THEN 'aspirin-code'
                    ELSE 'RX-' || row_number END,
               CASE WHEN row_number = 1 THEN 'Aspirin name'
                    ELSE 'unrelated name ' || row_number END,
               CASE WHEN row_number = 2 THEN 'Aspirin generic'
                    ELSE 'unrelated generic ' || row_number END,
               CASE WHEN row_number = 3 THEN 'Aspirin brand'
                    ELSE 'unrelated brand ' || row_number END,
               50001 - row_number,
               row_number,
               1
          FROM generate_series(1, 50000) AS row_number
        """
    )


def _autocomplete_index_model():
    index_contracts = tuple(
        index
        for index in pricing_module.PricingProviderPrescription.__my_additional_indexes__
        if index.get("name") == INDEX_NAME
    )
    return type(
        "AutocompleteIndexModel",
        (),
        {
            "__tablename__": "pricing_provider_prescription",
            "__main_table__": "pricing_provider_prescription",
            "__my_index_elements__": (),
            "__my_additional_indexes__": index_contracts,
        },
    )


async def _prepared_plans(engine, query):
    plans = []
    async with engine.connect() as prepared_connection:
        for _attempt in range(7):
            plan_result = await prepared_connection.execute(_PostgresExplain(query))
            encoded_plan = plan_result.scalar_one()
            plans.append(
                json.loads(encoded_plan)
                if isinstance(encoded_plan, str)
                else encoded_plan
            )
        prepared_statements = (
            await prepared_connection.execute(
                text(
                    "SELECT statement, generic_plans, custom_plans "
                    "FROM pg_prepared_statements "
                    "WHERE statement LIKE "
                    "'EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT%' "
                    "AND statement LIKE '%pricing_provider_prescription%'"
                )
            )
        ).mappings().all()
    return plans, prepared_statements


@pytest.mark.asyncio
async def test_production_autocomplete_query_uses_trigram_index(monkeypatch):
    """Require the production prepared statement to use every trigram index arm."""
    dsn = os.getenv("HLTHPRT_PRESCRIPTION_AUTOCOMPLETE_POSTGRES_DSN")
    if not dsn:
        pytest.skip("requires disposable PostgreSQL")

    connection = await asyncpg.connect(dsn)
    engine = create_async_engine(
        dsn.replace("postgresql://", "postgresql+asyncpg://", 1),
        pool_size=1,
        max_overflow=0,
    )
    schema = f"prescription_autocomplete_{uuid.uuid4().hex}"
    relation = f'"{schema}".pricing_provider_prescription'
    try:
        await connection.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        await _create_autocomplete_relation(connection, relation)
        monkeypatch.setattr(drug_claims, "db", _AsyncpgDatabase(connection))
        await drug_claims._ensure_indexes(_autocomplete_index_model(), schema)
        await connection.execute(f"ANALYZE {relation}")

        query = (await _production_query()).execution_options(
            schema_translate_map={"mrf": schema}
        )
        plans, prepared_statements = await _prepared_plans(engine, query)

        assert len(prepared_statements) == 1
        assert "$1" in prepared_statements[0]["statement"]
        assert (
            prepared_statements[0]["generic_plans"]
            + prepared_statements[0]["custom_plans"]
        ) >= 7
        for plan in plans:
            nodes = list(_plan_nodes(plan[0]["Plan"]))
            assert sum(node.get("Index Name") == INDEX_NAME for node in nodes) == 4
            assert not any(
                node.get("Node Type") == "Seq Scan"
                and node.get("Relation Name") == "pricing_provider_prescription"
                for node in nodes
            )
    finally:
        await engine.dispose()
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await connection.close()


async def _create_procedure_terminology_relation(connection, schema, relation):
    """Create the minimal split-row terminology fixture for production SQL."""
    await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    await connection.execute(text(f"""
        CREATE TABLE {relation} (
            domain text NOT NULL,
            term_key text NOT NULL,
            synonym text NOT NULL,
            term_type text,
            target_system text NOT NULL,
            target_code text NOT NULL,
            target_display text,
            canonical_term text,
            is_broad boolean NOT NULL DEFAULT false,
            confidence numeric,
            source text,
            source_attribution text,
            license_status text,
            metadata_json text
        )
    """))
    await connection.execute(text(f"""
        INSERT INTO {relation}
            (domain, term_key, synonym, target_system, target_code,
             canonical_term, confidence)
        VALUES
            ('procedure', 'knee replacement', 'Knee replacement', 'CPT', '27446', 'Unilateral knee replacement', 0.94),
            ('procedure', 'arthroplasty', 'Arthroplasty', 'CPT', '27446', 'Unilateral knee replacement', 0.90),
            ('procedure', 'knee replacement', 'Knee replacement', 'CPT', '27447', 'Total knee replacement', 0.94),
            ('procedure', 'arthroplasty', 'Arthroplasty', 'CPT', '27447', 'Total knee replacement', 0.90),
            ('procedure', 'anesthesia for total knee joint replacement', 'Anesthesia for total knee joint replacement', 'CPT', '01402', 'Anesthesia for total knee joint replacement', 0.94),
            ('procedure', 'knee surgery', 'Knee surgery', 'CPT', '29888', 'Knee surgery', 0.90),
            ('procedure', 'knee exam', 'Knee exam', 'CPT', '29870', 'Knee exam', 0.94)
    """))


@asynccontextmanager
async def _procedure_terminology_session(monkeypatch, dsn):
    """Yield a session bound to a disposable production-query fixture."""
    schema = f"procedure_autocomplete_{uuid.uuid4().hex}"
    relation = f'"{schema}".terminology_synonym'
    engine = create_async_engine(
        dsn.replace("postgresql://", "postgresql+asyncpg://", 1),
        pool_size=1,
        max_overflow=0,
    )
    try:
        async with engine.begin() as connection:
            await _create_procedure_terminology_relation(connection, schema, relation)
        candidate_sql = str(
            pricing_module._PROCEDURE_AUTOCOMPLETE_TERMINOLOGY_SQL
        ).replace(pricing_module.terminology_synonym_table.fullname, relation)
        monkeypatch.setattr(
            pricing_module,
            "_PROCEDURE_AUTOCOMPLETE_TERMINOLOGY_SQL",
            text(candidate_sql),
        )
        monkeypatch.setattr(
            pricing_module,
            "_is_terminology_available",
            AsyncMock(return_value=True),
        )
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        async with session_factory() as session:
            yield session
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
async def test_procedure_autocomplete_setwise_token_semantics(monkeypatch):
    """Exercise split-row scoring, anesthesia demotion, and phrase gating."""
    dsn = os.getenv("HLTHPRT_PRESCRIPTION_AUTOCOMPLETE_POSTGRES_DSN")
    if not dsn:
        pytest.skip("requires disposable PostgreSQL")

    rows_by_query = {}
    query_terminology = pricing_module._query_procedure_autocomplete_terminology
    async with _procedure_terminology_session(monkeypatch, dsn) as session:
        for search_query in (
            "knee arthroplasty",
            "total knee replacement",
            "knee surgery",
        ):
            rows_by_query[search_query] = await query_terminology(
                session,
                search_query=search_query,
                target_systems=None,
                limit=50,
            )

    assert {
        terminology_row["target_code"]
        for terminology_row in rows_by_query["knee arthroplasty"][:2]
    } == {"27446", "27447"}
    replacement_codes = [
        terminology_row["target_code"]
        for terminology_row in rows_by_query["total knee replacement"]
    ]
    assert replacement_codes.index("27447") < replacement_codes.index("01402")
    assert {
        terminology_row["target_code"]
        for terminology_row in rows_by_query["knee surgery"]
    } == {"29888"}
