"""PTG legacy projections retain canonical plan identifiers."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import uuid

from alembic.migration import MigrationContext
from alembic.operations import Operations
import asyncpg
import pytest
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from db.models import PTGAllowedItem, PTGFile, PTGInNetworkItem


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260821143000_ptg_legacy_plan_identifier_width.py"
)
POSTGRES_DSN_ENV = "HLTHPRT_PTG_PLAN_ID_WIDTH_MIGRATION_POSTGRES_DSN"
TABLES = ("ptg_file", "ptg_in_network_item", "ptg_allowed_item")


@pytest.mark.parametrize("model", (PTGFile, PTGInNetworkItem, PTGAllowedItem))
def test_ptg_legacy_plan_identifier_accepts_canonical_width(model) -> None:
    assert model.__table__.c.plan_id.type.length == 64


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "ptg_legacy_plan_identifier_width_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _sqlalchemy_async_dsn(dsn: str) -> str:
    if dsn.startswith("postgresql://"):
        return dsn.replace("postgresql://", "postgresql+asyncpg://", 1)
    if dsn.startswith("postgres://"):
        return dsn.replace("postgres://", "postgresql+asyncpg://", 1)
    return dsn


async def _run_migration(engine, migration, monkeypatch, action: str) -> None:
    async with engine.connect() as connection:

        def run_action(sync_connection) -> None:
            context = MigrationContext.configure(sync_connection)
            monkeypatch.setattr(migration, "op", Operations(context))
            with context.begin_transaction():
                getattr(migration, action)()

        await connection.run_sync(run_action)


async def _column_widths(connection, schema: str) -> dict[str, int]:
    rows = await connection.fetch(
        """
        SELECT table_name, character_maximum_length
          FROM information_schema.columns
         WHERE table_schema = $1
           AND column_name = 'plan_id'
         ORDER BY table_name
        """,
        schema,
    )
    return {row["table_name"]: row["character_maximum_length"] for row in rows}


@pytest.mark.asyncio
async def test_plan_identifier_width_migration_round_trip(monkeypatch) -> None:
    dsn = os.getenv(POSTGRES_DSN_ENV)
    if not dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for the PostgreSQL proof")

    schema = f"ptg_plan_id_width_{uuid.uuid4().hex[:12]}"
    migration = _load_migration()
    connection = await asyncpg.connect(dsn)
    engine = create_async_engine(_sqlalchemy_async_dsn(dsn))
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        for table_name in TABLES:
            await connection.execute(
                f'CREATE TABLE "{schema}"."{table_name}" ' "(plan_id varchar(32))"
            )
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
        monkeypatch.delenv("DB_SCHEMA", raising=False)

        await _run_migration(engine, migration, monkeypatch, "upgrade")
        assert await _column_widths(connection, schema) == {
            table_name: 64 for table_name in TABLES
        }

        canonical_plan_id = "p" * 64
        for table_name in TABLES:
            await connection.execute(
                f'INSERT INTO "{schema}"."{table_name}" (plan_id) VALUES ($1)',
                canonical_plan_id,
            )

        with pytest.raises(DBAPIError):
            await _run_migration(engine, migration, monkeypatch, "downgrade")
        assert await _column_widths(connection, schema) == {
            table_name: 64 for table_name in TABLES
        }

        for table_name in TABLES:
            await connection.execute(f'TRUNCATE "{schema}"."{table_name}"')
        await _run_migration(engine, migration, monkeypatch, "downgrade")
        assert await _column_widths(connection, schema) == {
            table_name: 32 for table_name in TABLES
        }
    finally:
        await engine.dispose()
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await connection.close()
