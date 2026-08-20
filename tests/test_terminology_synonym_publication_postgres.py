# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL identity and rollback proof for terminology publication."""

from __future__ import annotations

import importlib
import os
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace

import asyncpg
import pytest


terminology_synonyms = importlib.import_module("process.terminology_synonyms")
LIVE_TABLE = "terminology_synonym"
OLD_TABLE = f"{LIVE_TABLE}_old"
STAGE_TABLE = f"{LIVE_TABLE}_stage"


class _AsyncpgDatabase:
    def __init__(self, connection):
        self.connection = connection

    async def status(self, statement):
        return await self.connection.execute(statement)

    async def all(self, statement, **params):
        arguments = []
        for position, (name, value) in enumerate(params.items(), start=1):
            statement = statement.replace(f":{name}", f"${position}")
            arguments.append(value)
        return await self.connection.fetch(statement, *arguments)

    @asynccontextmanager
    async def transaction(self):
        async with self.connection.transaction():
            yield


def _qualified(schema, table):
    return f'"{schema}"."{table}"'


async def _relation_state(connection, schema, table):
    relation_name = f"{schema}.{table}"
    relation_oid = await connection.fetchval(
        "SELECT to_regclass($1)::oid::bigint",
        relation_name,
    )
    if relation_oid is None:
        return None
    markers = tuple(
        await connection.fetch(
            f"SELECT marker FROM {_qualified(schema, table)} ORDER BY marker"
        )
    )
    return relation_oid, tuple(row["marker"] for row in markers)


async def _prepare_relations(
    connection,
    schema,
    stage_markers=None,
    old_markers=None,
):
    default_stage_markers = ["stage-a", "stage-b", "stage-c"]
    marker_list_by_table = {
        LIVE_TABLE: ["live-a", "live-b"],
        OLD_TABLE: old_markers if old_markers is not None else ["older"],
        STAGE_TABLE: stage_markers if stage_markers is not None else default_stage_markers,
    }
    await connection.execute(f'CREATE SCHEMA "{schema}"')
    for table, marker_list in marker_list_by_table.items():
        await connection.execute(f"CREATE TABLE {_qualified(schema, table)} (marker text NOT NULL)")
        await connection.executemany(
            f"INSERT INTO {_qualified(schema, table)} (marker) VALUES ($1)",
            [(marker,) for marker in marker_list],
        )
    return {
        table: await _relation_state(connection, schema, table)
        for table in marker_list_by_table
    }


@asynccontextmanager
async def _database_case(monkeypatch):
    dsn = os.getenv("HLTHPRT_TERMINOLOGY_PUBLICATION_POSTGRES_DSN")
    if not dsn:
        pytest.skip("requires disposable PostgreSQL")

    connection = await asyncpg.connect(dsn)
    schema = f"terminology_publish_{uuid.uuid4().hex}"
    monkeypatch.setattr(terminology_synonyms, "db", _AsyncpgDatabase(connection))
    try:
        yield connection, schema
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await connection.close()


@pytest.mark.asyncio
async def test_terminology_publication_preserves_predecessor_and_rolls_back_mismatch(monkeypatch):
    stage_cls = SimpleNamespace(__tablename__=STAGE_TABLE)
    async with _database_case(monkeypatch) as (connection, schema):
        original_state_by_table = await _prepare_relations(connection, schema)

        await terminology_synonyms._publish_stage(schema, stage_cls, 3)

        assert await _relation_state(connection, schema, LIVE_TABLE) == original_state_by_table[STAGE_TABLE]
        assert await _relation_state(connection, schema, OLD_TABLE) == original_state_by_table[LIVE_TABLE]
        assert await _relation_state(connection, schema, STAGE_TABLE) is None
        assert not await connection.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_class WHERE oid=$1)",
            original_state_by_table[OLD_TABLE][0],
        )

        await connection.execute(f'DROP SCHEMA "{schema}" CASCADE')
        original_state_by_table = await _prepare_relations(connection, schema)

        with pytest.raises(RuntimeError, match="promoted row count 3 does not match staged row count 4"):
            await terminology_synonyms._publish_stage(schema, stage_cls, 4)

        for table, original_state in original_state_by_table.items():
            assert await _relation_state(connection, schema, table) == original_state

        await connection.execute(f'DROP SCHEMA "{schema}" CASCADE')
        original_state_by_table = await _prepare_relations(connection, schema, stage_markers=[])

        with pytest.raises(RuntimeError, match="refusing to publish an empty terminology snapshot"):
            await terminology_synonyms._publish_stage(schema, stage_cls, 0)

        for table, original_state in original_state_by_table.items():
            assert await _relation_state(connection, schema, table) == original_state


@pytest.mark.asyncio
async def test_terminology_rollback_reverses_relation_oids_and_content(monkeypatch):
    stage_cls = SimpleNamespace(__tablename__=STAGE_TABLE)
    async with _database_case(monkeypatch) as (connection, schema):
        original_state_by_table = await _prepare_relations(connection, schema)
        await terminology_synonyms._publish_stage(schema, stage_cls, 3)

        rollback_result = await terminology_synonyms._rollback_terminology_snapshot(
            schema,
            expected_live_oid=original_state_by_table[STAGE_TABLE][0],
            expected_old_oid=original_state_by_table[LIVE_TABLE][0],
        )

        assert rollback_result == {
            "live_oid": original_state_by_table[LIVE_TABLE][0],
            "predecessor_oid": original_state_by_table[STAGE_TABLE][0],
            "schema": schema,
        }
        assert await _relation_state(connection, schema, LIVE_TABLE) == original_state_by_table[LIVE_TABLE]
        assert await _relation_state(connection, schema, OLD_TABLE) == original_state_by_table[STAGE_TABLE]


@pytest.mark.asyncio
async def test_terminology_rollback_rejects_empty_predecessor_without_mutation(monkeypatch):
    async with _database_case(monkeypatch) as (connection, schema):
        original_state_by_table = await _prepare_relations(
            connection,
            schema,
            old_markers=[],
        )

        with pytest.raises(RuntimeError, match="predecessor is empty"):
            await terminology_synonyms._rollback_terminology_snapshot(
                schema,
                expected_live_oid=original_state_by_table[LIVE_TABLE][0],
                expected_old_oid=original_state_by_table[OLD_TABLE][0],
            )

        for table, original_state in original_state_by_table.items():
            assert await _relation_state(connection, schema, table) == original_state
