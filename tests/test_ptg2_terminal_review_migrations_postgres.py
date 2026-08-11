# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable-PostgreSQL proofs for terminal-review migration ownership."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from process.ptg_parts import ptg2_artifact_blobs as artifact_blobs
from process.ptg_parts import table_setup
from tests.test_ptg_wave_recovery_storage_postgres import (
    _dsn,
    _load_migration,
    _quote,
)


ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_MIGRATION = ROOT / "alembic" / "versions" / (
    "20260810150000_ptg2_artifact_blob_chunks.py"
)
DOWNGRADE_MIGRATIONS = (
    ROOT / "alembic" / "versions" /
    "20260810120000_ptg2_layout_build_candidates.py",
    ROOT / "alembic" / "versions" /
    "20260810130000_ptg2_block_build_pins.py",
    ROOT / "alembic" / "versions" /
    "20260810140000_ptg2_plan_catalog_outbox.py",
)


def _database(*, pool_size: int = 8):
    engine = create_async_engine(
        _dsn().replace("postgresql://", "postgresql+asyncpg://", 1),
        pool_size=pool_size,
        max_overflow=0,
        pool_timeout=2,
    )
    sessions = async_sessionmaker(
        engine,
        expire_on_commit=False,
        autoflush=False,
    )
    return engine, Database(engine=engine, session_factory=sessions), sessions


def _statements(monkeypatch, path: Path, schema_name: str, direction: str):
    migration = _load_migration(path)
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)
    getattr(migration, direction)()
    return statements


async def _execute_statements(engine, statements: list[str]) -> None:
    async with engine.begin() as connection:
        for statement in statements:
            await connection.execute(text(statement))


async def _create_legacy_artifact_tables(engine, schema: str) -> None:
    async with engine.begin() as connection:
        await connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        await connection.execute(text(f"CREATE SCHEMA {schema}"))
        await connection.execute(
            text(
                f"""
                CREATE TABLE {schema}.ptg2_artifact_blob_chunk (
                    artifact_id varchar(96) NOT NULL,
                    chunk_no integer NOT NULL,
                    compression varchar(32),
                    payload bytea NOT NULL,
                    raw_byte_count integer NOT NULL,
                    byte_count integer NOT NULL,
                    created_at timestamp,
                    PRIMARY KEY (artifact_id, chunk_no)
                )
                """
            )
        )
        await connection.execute(
            text(
                f"""
                CREATE TABLE {schema}.ptg2_artifact_manifest (
                    artifact_id varchar(96) PRIMARY KEY,
                    snapshot_id varchar(96),
                    import_run_id varchar(96),
                    artifact_kind varchar(64),
                    storage_uri text,
                    sha256 varchar(64),
                    byte_count bigint,
                    payload json,
                    created_at timestamp
                )
                """
            )
        )


class _ArtifactTable:
    __tablename__ = "ptg2_artifact_blob_chunk"


def _patch_artifact_only_readiness(monkeypatch, local_db, schema_name: str) -> None:
    monkeypatch.setattr(table_setup, "db", local_db)
    monkeypatch.setattr(table_setup, "resolve_ptg2_schema", lambda: schema_name)
    monkeypatch.setattr(table_setup, "PTG2_MODEL_CLASSES", (_ArtifactTable,))
    monkeypatch.setattr(table_setup, "PTG2_ALLOWED_AMOUNT_MIGRATION_TABLE_NAMES", ())
    monkeypatch.setattr(table_setup, "PTG2_V4_ATTEMPT_MIGRATION_TABLE_NAMES", ())
    for capability_name in (
        "PTG2_LAYOUT_BUILD_CANDIDATE_TABLE",
        "PTG2_BLOCK_BUILD_PIN_TABLE",
        "PTG2_PLAN_CATALOG_OUTBOX_TABLE",
        "PTG2_ARTIFACT_BLOB_TABLE",
        "PTG2_LEGACY_GLOBAL_PROJECTION_QUEUE_TABLE",
    ):
        monkeypatch.setattr(table_setup, capability_name, _ArtifactTable.__tablename__)


@pytest.mark.asyncio
async def test_artifact_migration_adopts_exact_shape_and_hot_path_never_ddl(
    monkeypatch,
    tmp_path,
):
    """An old exact table is adopted; a held relation cannot stall artifact I/O."""

    schema_name = "ptg_artifact_migration_owned"
    schema = _quote(schema_name)
    engine, local_db, sessions = _database()
    await _create_legacy_artifact_tables(engine, schema)
    await _execute_statements(
        engine,
        _statements(monkeypatch, ARTIFACT_MIGRATION, schema_name, "upgrade"),
    )
    _patch_artifact_only_readiness(monkeypatch, local_db, schema_name)
    monkeypatch.setattr(artifact_blobs, "db", local_db)
    monkeypatch.setattr(
        artifact_blobs,
        "resolve_ptg2_schema",
        lambda _schema_name=None: schema_name,
    )

    holder = sessions()
    holder_transaction = await holder.begin()
    await holder.execute(
        text(
            f"LOCK TABLE {schema}.ptg2_artifact_blob_chunk "
            "IN ROW EXCLUSIVE MODE"
        )
    )
    artifact_path = tmp_path / "migration-owned-artifact.bin"
    artifact_path.write_bytes(b"migration-owned-artifact")
    stored = await asyncio.wait_for(
        artifact_blobs.store_ptg2_artifact_file_in_db(
            artifact_path,
            snapshot_id=None,
            artifact_kind="synthetic",
            retain_local_cache=True,
        ),
        timeout=2,
    )
    assert stored["storage"] == "postgresql_chunks_v1"
    await table_setup.require_ptg2_runtime_schema_ready()
    await holder_transaction.rollback()
    await holder.close()

    async with engine.begin() as connection:
        await connection.execute(
            text(f"DROP INDEX {schema}.ptg2_artifact_blob_artifact_idx")
        )
    with pytest.raises(
        table_setup.PTG2RuntimeSchemaUnavailable,
        match="index:ptg2_artifact_blob_artifact_idx",
    ):
        await table_setup.require_ptg2_runtime_schema_ready()
    async with engine.begin() as connection:
        await connection.execute(text(f"DROP SCHEMA {schema} CASCADE"))
    await engine.dispose()


async def _assert_nonempty_then_empty_downgrade(
    engine,
    *,
    schema_name: str,
    setup_statements: tuple[str, ...],
    insert_statement: str,
    downgrade_statements: list[str],
    table_name: str,
) -> None:
    schema = _quote(schema_name)
    async with engine.begin() as connection:
        await connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        await connection.execute(text(f"CREATE SCHEMA {schema}"))
        for statement in setup_statements:
            await connection.execute(text(statement.format(schema=schema)))
        await connection.execute(text(insert_statement.format(schema=schema)))
    with pytest.raises(Exception, match="refusing to downgrade"):
        async with engine.begin() as connection:
            await connection.execute(text(downgrade_statements[0]))
    async with engine.begin() as connection:
        await connection.execute(text(f"DELETE FROM {schema}.{table_name}"))
    await _execute_statements(engine, downgrade_statements)
    async with engine.connect() as connection:
        assert await connection.scalar(
            text("SELECT to_regclass(:relation_name)"),
            {"relation_name": f"{schema_name}.{table_name}"},
        ) is None


@pytest.mark.asyncio
async def test_terminal_review_migrations_refuse_nonempty_downgrades(monkeypatch):
    """Active candidate, pin, and every outbox state block downgrade."""

    engine, _local_db, _sessions = _database()
    candidate_schema = "ptg_candidate_downgrade_guard"
    await _assert_nonempty_then_empty_downgrade(
        engine,
        schema_name=candidate_schema,
        setup_statements=(
            "CREATE TABLE {schema}.ptg2_v3_layout_fingerprint (id integer)",
            "CREATE TABLE {schema}.ptg2_layout_build_candidate "
            "(snapshot_key bigint PRIMARY KEY)",
        ),
        insert_statement=(
            "INSERT INTO {schema}.ptg2_layout_build_candidate VALUES (1)"
        ),
        downgrade_statements=_statements(
            monkeypatch, DOWNGRADE_MIGRATIONS[0], candidate_schema, "downgrade"
        ),
        table_name="ptg2_layout_build_candidate",
    )
    pin_schema = "ptg_pin_downgrade_guard"
    await _assert_nonempty_then_empty_downgrade(
        engine,
        schema_name=pin_schema,
        setup_statements=(
            "CREATE TABLE {schema}.ptg2_block_build_pin "
            "(snapshot_key bigint PRIMARY KEY)",
        ),
        insert_statement="INSERT INTO {schema}.ptg2_block_build_pin VALUES (1)",
        downgrade_statements=_statements(
            monkeypatch, DOWNGRADE_MIGRATIONS[1], pin_schema, "downgrade"
        ),
        table_name="ptg2_block_build_pin",
    )
    outbox_schema = "ptg_outbox_downgrade_guard"
    await _assert_nonempty_then_empty_downgrade(
        engine,
        schema_name=outbox_schema,
        setup_statements=(
            "CREATE TABLE {schema}.ptg2_plan_catalog_outbox "
            "(request_id text PRIMARY KEY, lease_token text, "
            "terminal_error_code text)",
        ),
        insert_statement=(
            "INSERT INTO {schema}.ptg2_plan_catalog_outbox VALUES "
            "('pending', NULL, NULL), ('leased', 'token', NULL), "
            "('poison', NULL, 'immutable_conflict')"
        ),
        downgrade_statements=_statements(
            monkeypatch, DOWNGRADE_MIGRATIONS[2], outbox_schema, "downgrade"
        ),
        table_name="ptg2_plan_catalog_outbox",
    )
    for schema_name in (candidate_schema, pin_schema, outbox_schema):
        async with engine.begin() as connection:
            await connection.execute(
                text(f"DROP SCHEMA IF EXISTS {_quote(schema_name)} CASCADE")
            )
    await engine.dispose()
