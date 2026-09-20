# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact local pharmacy economics relation and generation publication."""

from __future__ import annotations

import importlib
import os
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

economics = importlib.import_module("process.pharmacy_economics")
generation = importlib.import_module("process.reference_family_result_generation")
archive = importlib.import_module("process.reference_family_archive")
_LIVE = "pharmacy_economics_summary"
_STAGE = _LIVE + "_stage"


class _Database:
    def __init__(self, connection):
        self.connection = connection

    async def status(self, statement):
        return await self.connection.execute(statement)

    async def all(self, statement, **params):
        statement = getattr(statement, "text", statement)
        arguments = []
        for position, (name, value) in enumerate(params.items(), start=1):
            statement = statement.replace(f":{name}", f"${position}")
            arguments.append(value)
        return await self.connection.fetch(statement, *arguments)

    async def first(self, statement, **params):
        rows = await self.all(statement, **params)
        return rows[0] if rows else None

    async def scalar(self, statement):
        return await self.connection.fetchval(statement)

    @asynccontextmanager
    async def transaction(self):
        async with self.connection.transaction():
            yield


@asynccontextmanager
async def _case(monkeypatch):
    dsn = os.getenv("HLTHPRT_PHARMACY_ECON_POSTGRES_DSN")
    if not dsn:
        pytest.skip("requires disposable PostgreSQL")
    connection = await asyncpg.connect(dsn)
    schema = "pharmacy_econ_snapshot_" + uuid4().hex
    stage = SimpleNamespace(
        __tablename__=_STAGE,
        __my_additional_indexes__=economics.PharmacyEconomicsSummary.__my_additional_indexes__,
    )
    monkeypatch.setattr(economics, "db", _Database(connection))
    monkeypatch.setattr(economics, "make_class", lambda *_args: stage)
    monkeypatch.setattr(economics, "ensure_database", AsyncMock())
    monkeypatch.setattr(economics, "mark_control_run", AsyncMock())
    monkeypatch.setattr(economics, "print_time_info", lambda *_args: None)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        for name, marker in ((_LIVE, "old"), (_STAGE, "new")):
            await connection.execute(
                f'CREATE TABLE "{schema}"."{name}" ('
                "state text, ndc11 text, sdud_volume integer, estimated_gross_margin float, marker text)"
            )
            await connection.execute(f'INSERT INTO "{schema}"."{name}" VALUES ($1,$2,1,1,$3)', "CA", "123", marker)
            await connection.execute(f'CREATE UNIQUE INDEX "{name}_idx_primary" ON "{schema}"."{name}" (state,ndc11)')
            for index in stage.__my_additional_indexes__:
                index_name = economics._stage_index_name(name, index["name"]) if name == _STAGE else f"{name}_idx_{index['name']}"
                where = f" WHERE {index['where']}" if index.get("where") else ""
                await connection.execute(
                    f'CREATE INDEX "{index_name}" ON "{schema}"."{name}" '
                    f"({', '.join(index['index_elements'])}){where}"
                )
        await connection.execute(
            f'CREATE TABLE "{schema}".reference_family_result_generation ('
            "importer_id text PRIMARY KEY,local_lineage_id uuid,local_generation bigint,"
            "origin_lineage_id uuid,origin_generation bigint,published_at timestamptz,relation_oids bigint[])"
        )
        await connection.execute(
            f'INSERT INTO "{schema}".reference_family_result_generation VALUES ($1,$2,0)',
            "pharmacy-economics", uuid4(),
        )
        yield connection, schema
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await connection.close()


async def _oid(connection, schema, name):
    return await connection.fetchval("SELECT to_regclass($1)::oid::bigint", f"{schema}.{name}")


@pytest.mark.asyncio
async def test_normal_publish_binds_exact_oid_and_rollback_is_atomic(monkeypatch):
    async with _case(monkeypatch) as (connection, schema):
        old_oid = await _oid(connection, schema, _LIVE)
        stage_oid = await _oid(connection, schema, _STAGE)
        ctx = {"import_date": "stage", "context": {"run": 1, "test_mode": True}}
        await economics.publish_pharmacy_economics_generation(ctx)
        authority = await generation.read_reference_family_result_generation_authority(
            economics.db, importer_id="pharmacy-economics", schema_name=schema
        )
        assert authority.local_generation == 1 and authority.relation_oids == (stage_oid,)
        assert await _oid(connection, schema, _LIVE) == stage_oid
        assert await _oid(connection, schema, _LIVE + "_old") == old_oid
        assert await connection.fetchval(f'SELECT marker FROM "{schema}"."{_LIVE}"') == "new"

    async with _case(monkeypatch) as (connection, schema):
        old_oid = await _oid(connection, schema, _LIVE)
        stage_oid = await _oid(connection, schema, _STAGE)
        real_publish = economics.publish_local_reference_family_generation

        async def fail_after_generation(*args, **kwargs):
            await real_publish(*args, **kwargs)
            raise RuntimeError("late publication failure")

        monkeypatch.setattr(economics, "publish_local_reference_family_generation", fail_after_generation)
        with pytest.raises(RuntimeError, match="late publication failure"):
            await economics.publish_pharmacy_economics_generation(ctx)
        assert await _oid(connection, schema, _LIVE) == old_oid
        assert await _oid(connection, schema, _STAGE) == stage_oid
        authority = await generation.read_reference_family_result_generation_authority(
            economics.db, importer_id="pharmacy-economics", schema_name=schema
        )
        assert authority.local_generation == 0 and authority.relation_oids is None


@pytest.mark.asyncio
async def test_pharmacy_reference_stage_has_complete_model_indexes():
    dsn = os.getenv("HLTHPRT_PHARMACY_ECON_POSTGRES_DSN")
    if not dsn:
        pytest.skip("requires disposable PostgreSQL")
    engine = create_async_engine(make_url(dsn).set(drivername="postgresql+asyncpg"))
    dataset_id = uuid4()
    stage_schema = archive.reference_family_stage_schema(dataset_id)
    try:
        async with engine.begin() as connection:
            owner = await archive.precreate_reference_family_restore(
                connection, importer_id="pharmacy-economics", dataset_id=dataset_id
            )
            assert owner.relation_oids[0][0] == _LIVE
            await archive.complete_reference_family_restore(connection, owner)
            indexes = (await connection.execute(text(
                "SELECT indexdef FROM pg_indexes WHERE schemaname=:schema"
            ), {"schema": stage_schema})).scalars().all()
            assert any("sdud_volume DESC" in definition for definition in indexes)
            assert any("estimated_gross_margin IS NOT NULL" in definition for definition in indexes)
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{stage_schema}" CASCADE'))
        await engine.dispose()
