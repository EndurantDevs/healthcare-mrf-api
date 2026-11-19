# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Build and publish complete, named procedure taxonomy evidence snapshots."""

from __future__ import annotations

import os
import re
from uuid import uuid4

from db.plan_release_storage import storage_identifier, storage_relation
from db.procedure_taxonomy_signal_sql import (
    TAXONOMY_PUBLICATION_STATE,
    TAXONOMY_SERVING_VIEW,
    procedure_taxonomy_control_schema,
    procedure_taxonomy_publication_schema,
    procedure_taxonomy_signal_select_sql,
)

_GENERATION_PREFIX = "procedure_taxonomy_g_"
_GENERATION_NAME = re.compile(r"procedure_taxonomy_g_[0-9a-f]{32}\Z")
_COLUMNS = (
    ("procedure_code", "bigint"),
    ("year", "integer"),
    ("setting_key", "text"),
    ("evidence_source", "text"),
    ("taxonomy_code", "text"),
    ("classification", "text"),
    ("specialization", "text"),
    ("display_name", "text"),
    ("distinct_npis", "integer"),
    ("total_services", "double precision"),
    ("total_beneficiaries", "double precision"),
    ("provider_types", "text[]"),
)


def _publication_state_relation(publication_schema: str) -> str:
    return storage_relation(procedure_taxonomy_control_schema(publication_schema), TAXONOMY_PUBLICATION_STATE)


def procedure_taxonomy_publisher_role(schema: str) -> str | None:
    """Return the configured evidence reader for publications in the live source schema."""
    if procedure_taxonomy_publication_schema() is None or schema != (os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"):
        return None
    return os.getenv("HLTHPRT_PRICING_TAXONOMY_PUBLISHER_ROLE", "").strip() or None


async def mark_procedure_taxonomy_inputs_changed(schema, execute, *replaced_tables: str) -> None:
    """Advance the input revision in the existing source publication transaction."""

    publication_schema = procedure_taxonomy_publication_schema()
    if publication_schema is None or schema != (os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"):
        return
    publisher = procedure_taxonomy_publisher_role(schema)
    if publisher and replaced_tables:
        relations = ", ".join(storage_relation(schema, table) for table in replaced_tables)
        await execute(f"GRANT SELECT ON TABLE {relations} TO {storage_identifier(publisher)}")
    state = _publication_state_relation(publication_schema)
    await execute(f"UPDATE {state} SET input_revision = input_revision + 1 WHERE singleton")


async def initialize_procedure_taxonomy_publication(connection, schema: str) -> None:
    """Initialize the state and view in the publisher's provisioned schemas."""

    state = _publication_state_relation(schema)
    view = storage_relation(schema, TAXONOMY_SERVING_VIEW)
    async with connection.transaction():
        await connection.execute(f"""
            CREATE TABLE IF NOT EXISTS {state} (
                singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
                input_revision bigint NOT NULL DEFAULT 0 CHECK (input_revision >= 0),
                current_table text,
                current_revision bigint,
                previous_table text,
                previous_revision bigint,
                unpublished_table text,
                rollback_at_revision bigint,
                CHECK ((current_table IS NULL) = (current_revision IS NULL)),
                CHECK ((previous_table IS NULL) = (previous_revision IS NULL)),
                CHECK (current_table IS DISTINCT FROM previous_table OR current_table IS NULL),
                CHECK (unpublished_table IS NULL OR (
                    unpublished_table IS DISTINCT FROM current_table AND unpublished_table IS DISTINCT FROM previous_table
                ))
            )
        """)
        await connection.execute(f"INSERT INTO {state} (singleton) VALUES (true) ON CONFLICT DO NOTHING")
        await connection.fetchval(f"SELECT singleton FROM {state} WHERE singleton FOR UPDATE")
        if not await connection.fetchval("SELECT to_regclass($1) IS NOT NULL", view):
            projection = ", ".join(f"NULL::{kind} AS {column}" for column, kind in _COLUMNS)
            await connection.execute(f"CREATE VIEW {view} AS SELECT {projection} WHERE false")


def _generation_relation(schema: str, name: str) -> str:
    if not _GENERATION_NAME.fullmatch(name):
        raise ValueError("Invalid procedure taxonomy generation name")
    return storage_relation(schema, name)


async def _optional_source(connection, schema: str, table: str) -> tuple[str, str] | None:
    relation = storage_relation(schema, table)
    if await connection.fetchval("SELECT to_regclass($1) IS NOT NULL", relation):
        return schema, table
    return None


async def _build_generation(connection, source_schema: str, publication_schema: str, table_name: str):
    state = _publication_state_relation(publication_schema)
    generation_table = _generation_relation(publication_schema, table_name)
    async with connection.transaction(isolation="repeatable_read"):
        await connection.execute("SET LOCAL lock_timeout = '2s'")
        await connection.execute("SET LOCAL statement_timeout = '22min'")
        await connection.execute("SET LOCAL max_parallel_workers_per_gather = 0")
        revision = await connection.fetchval(f"SELECT input_revision FROM {state} WHERE singleton")
        select = procedure_taxonomy_signal_select_sql(
            provider=(source_schema, "pricing_provider"),
            procedure=(source_schema, "pricing_provider_procedure"),
            quality=await _optional_source(connection, source_schema, "pricing_provider_quality_feature"),
            npi_taxonomy=await _optional_source(connection, source_schema, "npi_taxonomy"),
            nucc_taxonomy=await _optional_source(connection, source_schema, "nucc_taxonomy"),
        )
        await connection.execute(f"CREATE TABLE {generation_table} AS {select}")
        await connection.execute(
            f"ALTER TABLE {generation_table} ADD PRIMARY KEY "
            "(procedure_code, year, setting_key, evidence_source, taxonomy_code)"
        )
        await connection.execute(
            f"CREATE INDEX ON {generation_table} (year, procedure_code, setting_key, evidence_source, "
            "distinct_npis DESC, total_services DESC, taxonomy_code)"
        )
        row_count, invalid_count = await connection.fetchrow(f"""
            SELECT count(*), count(*) FILTER (WHERE distinct_npis IS NULL OR distinct_npis <= 0
                   OR total_services IS NULL OR total_services < 0 OR total_services >= 'Infinity'::double precision
                   OR total_beneficiaries IS NULL OR total_beneficiaries < 0
                   OR total_beneficiaries >= 'Infinity'::double precision OR taxonomy_code = ''
                   OR provider_types IS NULL OR setting_key <> 'all'
                   OR evidence_source NOT IN ('quality_feature', 'quality_or_nppes'))
            FROM {generation_table}
        """)
        if row_count == 0 or invalid_count:
            raise RuntimeError("Procedure taxonomy generation is empty or contains invalid evidence")
        await connection.execute(f"ANALYZE {generation_table}")
        await connection.execute(f"UPDATE {state} SET unpublished_table = $1 WHERE singleton", table_name)
    return revision, row_count


async def _point_view(connection, schema: str, table_name: str) -> None:
    target = _generation_relation(schema, table_name)
    view = storage_relation(schema, TAXONOMY_SERVING_VIEW)
    projection = ", ".join(f"{column}::{kind} AS {column}" for column, kind in _COLUMNS)
    await connection.execute(f"CREATE OR REPLACE VIEW {view} AS SELECT {projection} FROM {target}")


async def _publish_generation(connection, schema: str, table_name: str, revision: int) -> str:
    state = _publication_state_relation(schema)
    async with connection.transaction():
        await connection.execute("SET LOCAL lock_timeout = '2s'")
        current = await connection.fetchrow(
            f"SELECT input_revision, unpublished_table FROM {state} WHERE singleton FOR UPDATE"
        )
        if current["input_revision"] != revision or current["unpublished_table"] != table_name:
            return "inputs_changed"
        await _point_view(connection, schema, table_name)
        await connection.execute(
            f"""UPDATE {state} SET unpublished_table = previous_table,
                previous_table = current_table, previous_revision = current_revision,
                current_table = $1, current_revision = $2, rollback_at_revision = NULL WHERE singleton""",
            table_name,
            revision,
        )
    return "published"


async def _drop_unpublished(connection, schema: str) -> str | None:
    state = _publication_state_relation(schema)
    try:
        async with connection.transaction():
            await connection.execute("SET LOCAL lock_timeout = '2s'")
            name = await connection.fetchval(f"SELECT unpublished_table FROM {state} WHERE singleton FOR UPDATE")
            if name is not None:
                await connection.execute(f"DROP TABLE IF EXISTS {_generation_relation(schema, name)}")
                await connection.execute(f"UPDATE {state} SET unpublished_table = NULL WHERE singleton")
    except Exception as error:
        if getattr(error, "sqlstate", None) != "55P03":
            raise
        return "cleanup_pending"
    return None


async def rebuild_procedure_taxonomy_signals(connection, source_schema: str, schema: str):
    """Replace the serving view only after a new generation passes validation."""

    key = f"procedure-taxonomy:{schema}"
    if not await connection.fetchval("SELECT pg_try_advisory_lock(hashtextextended($1, 0))", key):
        return {"status": "busy"}
    try:
        state = _publication_state_relation(schema)
        current = await connection.fetchrow(f"""
            SELECT input_revision, current_revision, rollback_at_revision FROM {state} WHERE singleton
        """)
        cleanup_status = await _drop_unpublished(connection, schema)
        if cleanup_status is not None:
            return {"status": cleanup_status}
        if current["input_revision"] in (current["current_revision"], current["rollback_at_revision"]):
            return {"status": "current"}
        table_name = _GENERATION_PREFIX + uuid4().hex
        revision, row_count = await _build_generation(connection, source_schema, schema, table_name)
        publication_status = await _publish_generation(connection, schema, table_name, revision)
        if publication_status == "inputs_changed":
            await _drop_unpublished(connection, schema)
            return {"status": publication_status}
        cleanup_status = await _drop_unpublished(connection, schema)
        return {
            "status": publication_status,
            "schema": schema,
            "table": table_name,
            "rows": row_count,
            "cleanup_pending": cleanup_status is not None,
        }
    finally:
        await connection.execute("SELECT pg_advisory_unlock(hashtextextended($1, 0))", key)


async def rollback_procedure_taxonomy_signals(connection, schema: str):
    """Restore the previous published generation until another source publication."""

    key = f"procedure-taxonomy:{schema}"
    state = _publication_state_relation(schema)
    async with connection.transaction():
        await connection.execute("SET LOCAL lock_timeout = '2s'")
        if not await connection.fetchval("SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))", key):
            return {"status": "busy"}
        previous = await connection.fetchval(f"SELECT previous_table FROM {state} WHERE singleton FOR UPDATE")
        if previous is None:
            raise RuntimeError("There is no previous procedure taxonomy generation")
        await _point_view(connection, schema, previous)
        await connection.execute(f"""
            UPDATE {state} SET current_table = previous_table, current_revision = previous_revision,
                previous_table = current_table, previous_revision = current_revision,
                rollback_at_revision = input_revision WHERE singleton
        """)
    return {"status": "rolled_back", "schema": schema, "table": previous}


async def run_procedure_taxonomy_publication(action: str):
    """Run a named publication operation with one dedicated database connection."""
    from db.connection import db
    from process.ext.utils import ensure_database

    schema = procedure_taxonomy_publication_schema()
    if schema is None:
        raise RuntimeError("The procedure taxonomy publication schema is not configured")
    await ensure_database(False)
    try:
        async with db.acquire_driver(detached=True) as connection:
            if action == "initialize":
                await initialize_procedure_taxonomy_publication(connection, schema)
                return {"status": "initialized", "schema": schema}
            if action == "rollback":
                return await rollback_procedure_taxonomy_signals(connection, schema)
            return await rebuild_procedure_taxonomy_signals(connection, os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", schema)
    finally:
        await db.disconnect()
