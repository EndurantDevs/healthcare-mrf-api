# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native preflight admission, actual callers and transaction-local setting reset."""

import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.sql.elements import quoted_name

from api import custom_import_owner_worker as owner
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import import build_source
from process.custom_import import family_shard_handoff as handoff
from process.custom_import import family_shard_owner_cli as cli
from process.custom_import import family_shard_production as production
from process.custom_import.family_shard_owner_access import require_owner_connection
from process.custom_import.family_shard_sessions import import_session_factory
from tests.custom_import_postgres_support import _database_url
from tests.test_control_owner_handoff import _profile


@dataclass(frozen=True)
class _Case:
    engine: object
    factory: object
    schema: str
    owner_role: str


@asynccontextmanager
async def _native_case():
    schema = quoted_name("owner_access_" + uuid.uuid4().hex, True)
    engine = create_async_engine(
        _database_url(),
        pool_size=1,
        max_overflow=0,
        execution_options={"schema_translate_map": {CustomImportSnapshotFamily.__table__.schema: schema}},
    )
    try:
        async with engine.begin() as connection:
            role = await connection.scalar(text("SELECT CURRENT_USER"))
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            await connection.execute(
                text(
                    f'CREATE FUNCTION "{schema}".source_bulk_authorize(bigint,smallint,bigint,bytea,integer,bigint) RETURNS boolean LANGUAGE sql AS $$SELECT true$$'
                )
            )
            await connection.execute(
                text(
                    f'CREATE FUNCTION "{schema}".source_set_finalize(uuid,integer[]) RETURNS boolean LANGUAGE sql AS $$SELECT true$$'
                )
            )
            for name in (
                "custom_import_snapshot_family",
                "custom_import_family_block_layout",
                "custom_import_family_block_relation",
            ):
                await connection.execute(text(f'CREATE TABLE "{schema}".{name}(id bigint PRIMARY KEY)'))
        yield _Case(engine, async_sessionmaker(engine, expire_on_commit=False), schema, role)
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()


async def test_real_api_owner_preflight_requires_no_writer_window():
    async with _native_case() as case:
        await owner._require_owner(case.factory, {"owner_role": case.owner_role})
        async with case.factory() as session, session.begin():
            assert not session.info
            assert await session.scalar(text("SHOW transaction_read_only")) == "off"


async def test_native_preflight_keeps_import_memory_local_to_transaction():
    async with _native_case() as case:
        async with case.factory() as session, session.begin():
            original_memory = await session.scalar(text("SHOW work_mem"))
            backend = await session.scalar(text("SELECT pg_backend_pid()"))
        imported = import_session_factory(case.factory, work_mem_mb=1024, maintenance_work_mem_mb=512)
        async with imported() as session, session.begin():
            await require_owner_connection(session, case.owner_role)
            assert await session.scalar(text("SHOW transaction_read_only")) == "on"
            assert await session.scalar(text("SHOW work_mem")) == "1GB"
            assert await session.scalar(text("SHOW maintenance_work_mem")) == "512MB"
            assert build_source._WINDOW not in session.info
        async with case.factory() as session, session.begin():
            assert await session.scalar(text("SELECT pg_backend_pid()")) == backend
            assert await session.scalar(text("SHOW work_mem")) == original_memory
            assert await session.scalar(text("SHOW transaction_read_only")) == "off"


@pytest.mark.parametrize(
    "problem", ["wrong_role", "missing_procedure", "foreign_table_owner", "foreign_procedure_owner"]
)
async def test_native_catalog_drift_is_denied_before_any_pipeline(problem):
    async with _native_case() as case:
        async with case.engine.begin() as connection:
            match problem:
                case "missing_procedure":
                    await connection.execute(text(f'DROP FUNCTION "{case.schema}".source_set_finalize(uuid,integer[])'))
                case "foreign_table_owner":
                    await connection.execute(
                        text(f'ALTER TABLE "{case.schema}".custom_import_family_block_layout OWNER TO pg_monitor')
                    )
                case "foreign_procedure_owner":
                    await connection.execute(
                        text(f'ALTER FUNCTION "{case.schema}".source_set_finalize(uuid,integer[]) OWNER TO pg_monitor')
                    )
        with pytest.raises(ValueError, match="^owner handoff is unavailable$"):
            await owner._require_owner(
                case.factory, {"owner_role": "foreign" if problem == "wrong_role" else case.owner_role}
            )


class _PipelineBoundary(Exception):
    """Only native startup is under test; import processing is deliberately stopped."""


async def test_cli_crosses_real_native_preflight_before_entering_original_pipeline(monkeypatch):
    async with _native_case() as case:
        profile, signed = _profile()
        profile["owner_role"] = case.owner_role
        monkeypatch.setattr(cli, "profile", lambda: (profile, b"k" * 32, cli.source_limits(profile["source_limits"])))
        monkeypatch.setattr(cli, "_owner_inputs", lambda *_args: (signed, b"x" * 43, None))
        monkeypatch.setattr(cli, "require_profile", lambda *_args: None)
        monkeypatch.setattr(cli, "require_capacity", lambda *_args: None)
        pipeline = AsyncMock(side_effect=_PipelineBoundary())
        monkeypatch.setattr(production, "original_request", pipeline)
        database = SimpleNamespace(session_factory=case.factory, connect=AsyncMock(), disconnect=AsyncMock())
        with pytest.raises(_PipelineBoundary):
            await cli.run_owner(handoff.identity(signed), database)
        pipeline.assert_awaited_once()
        database.disconnect.assert_awaited_once()
