# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Capture every Tennessee license occurrence in one closed read-only snapshot."""

from __future__ import annotations

import asyncio
import hashlib

import asyncpg

from db.models import db
from process.massachusetts_profile_acquisition import encoded_json
from process import tennessee_profile_binding as binding

MAX_REGISTRY_ROWS = 250_000
MAX_ROW_BYTES = 64 * 1024
SNAPSHOT_SQL = """SELECT current_setting('transaction_read_only') AS read_only,
    current_setting('transaction_isolation') AS isolation,
    pg_current_snapshot()::text AS snapshot_id, transaction_timestamp()::text AS snapshot_started_at,
    current_setting('server_version') AS server_version, current_database() AS database_name,
    pg_backend_pid() AS backend_pid"""


async def _open_connection():
    if db.engine is None:
        await db.connect()
    # Own this connection independently of the importer's transaction and pool.
    return await asyncpg.connect(
        dsn=db.engine.url.set(drivername="postgresql").render_as_string(hide_password=False),
        timeout=10, server_settings={"statement_timeout": "90000", "lock_timeout": "5000"},
    )


async def _registry_rows(connection, query, expected_count, progress):
    digest = hashlib.sha256(b"[")
    registry_rows, row_bytes, previous = [], 2, None
    async for source_row in connection.cursor(query, prefetch=500):
        candidate_by_field = dict(source_row)
        binding._require(set(candidate_by_field) == set(binding.REGISTRY_COLUMNS) and candidate_by_field["license_state"] == "TN"
                         and type(candidate_by_field["npi"]) is int
                         and type(candidate_by_field["taxonomy_occurrence_checksum"]) is int, "registry_row_invalid")
        occurrence = candidate_by_field["npi"], candidate_by_field["taxonomy_occurrence_checksum"]
        binding._require(previous is None or previous <= occurrence, "registry_order_changed")
        content = encoded_json(candidate_by_field)
        binding._require(len(content) <= MAX_ROW_BYTES, "registry_row_too_large")
        piece = (b", " if registry_rows else b"") + content
        row_bytes += len(piece)
        binding._require(len(registry_rows) < MAX_REGISTRY_ROWS
                         and row_bytes <= binding.MAX_SNAPSHOT_BYTES - 1024 * 1024, "registry_too_large")
        digest.update(piece)
        registry_rows.append(candidate_by_field)
        previous = occurrence
        if len(registry_rows) % 500 == 0:
            await progress(len(registry_rows), expected_count)
    digest.update(b"]")
    binding._require(len(registry_rows) == expected_count, "registry_count_changed")
    await progress(len(registry_rows), expected_count)
    return {"registry_rows": registry_rows, "row_count": len(registry_rows),
            "registry_rows_bytes": row_bytes, "registry_rows_sha256": digest.hexdigest()}


async def capture_registry_snapshot(source_schema, progress):
    """Retain nullable joins and duplicates; finish and close before returning evidence."""
    query = binding.capture_query(source_schema)
    await progress(0, 0)
    connection = None
    try:
        async with asyncio.timeout(180):
            connection = await _open_connection()
            async with connection.transaction(isolation="repeatable_read", readonly=True):
                snapshot_by_field = dict(await connection.fetchrow(SNAPSHOT_SQL))
                expected_count = await connection.fetchval("SELECT count(*) FROM (" + query + ") AS occurrences")
                binding._require(type(expected_count) is int and 0 <= expected_count <= MAX_REGISTRY_ROWS,
                                 "registry_count_invalid")
                retained = await _registry_rows(connection, query, expected_count, progress)
                names = [source_schema + "." + name for name in ("npi", "npi_taxonomy", "nucc_taxonomy")]
                relation_oids_by_name = {relation_row["name"]: relation_row["oid"] for relation_row in await connection.fetch(
                    "SELECT name, to_regclass(name)::oid::bigint AS oid FROM unnest($1::text[]) name", names)}
                binding._require(dict(await connection.fetchrow(SNAPSHOT_SQL)) == snapshot_by_field, "snapshot_changed")
                retained.update(schema_version=binding.SNAPSHOT_SCHEMA, coverage_scope=binding.COVERAGE_SCOPE,
                                source_schema=source_schema, source_state="TN", snapshot=snapshot_by_field,
                                query_sha256=hashlib.sha256(query.encode("utf-8")).hexdigest(),
                                columns=list(binding.REGISTRY_COLUMNS), registry_relations=relation_oids_by_name,
                                expected_source_row_count=expected_count, all_rows_received=True)
    finally:
        if connection is not None:
            try:
                await connection.close(timeout=10)
            finally:
                if not connection.is_closed():
                    connection.terminate()
    binding._require(connection is not None and connection.is_closed(), "registry_connection_not_closed")
    retained.update(connection_closed=True, status="passed")
    binding._validate_snapshot_metadata(retained, source_schema)
    binding._require(len(encoded_json(retained)) <= binding.MAX_SNAPSHOT_BYTES, "registry_too_large")
    return retained
