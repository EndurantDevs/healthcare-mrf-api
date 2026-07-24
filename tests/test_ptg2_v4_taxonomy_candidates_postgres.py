# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL lifecycle proof for the V4 inferred-taxonomy sidecar."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import re
import struct
from typing import Any
import uuid

from alembic.migration import MigrationContext
from alembic.operations import Operations
import pytest
import sqlalchemy as sa
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / "20260724120000_ptg2_v4_taxonomy_candidates.py"
)
POSTGRES_DSN_ENV = "HLTHPRT_PTG2_V4_TAXONOMY_POSTGRES_DSN"
_DISPOSABLE_DATABASE_RE = re.compile(
    r"(?:^test(?:[_-]|$)|(?:^|[_-])test(?:[_-]|$))",
    re.IGNORECASE,
)
_DISPOSABLE_SCHEMA_RE = re.compile(
    r"^ptg2_v4_taxonomy_test_[0-9a-f]{32}$"
)


def _load_migration() -> Any:
    spec = importlib.util.spec_from_file_location(
        "ptg2_v4_taxonomy_candidates_postgres_proof",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def _quoted(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _async_database_url() -> sa.URL:
    raw_dsn = os.getenv(POSTGRES_DSN_ENV)
    if not raw_dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for the PostgreSQL proof")
    database_url = make_url(raw_dsn)
    database_name = str(database_url.database or "")
    if (
        not database_url.drivername.startswith("postgresql")
        or not database_name
        or not _DISPOSABLE_DATABASE_RE.search(database_name)
    ):
        pytest.fail(
            f"{POSTGRES_DSN_ENV} must target an explicit PostgreSQL test "
            "database; only a generated disposable schema is modified"
        )
    return database_url.set(drivername="postgresql+asyncpg")


async def _run_migration_action(
    engine: AsyncEngine,
    migration: Any,
    action: str,
) -> None:
    async with engine.connect() as async_connection:

        def run_action(sync_connection) -> None:
            migration_context = MigrationContext.configure(sync_connection)
            migration.op = Operations(migration_context)
            with migration_context.begin_transaction():
                getattr(migration, action)()

        await async_connection.run_sync(run_action)


async def _create_prerequisites(
    engine: AsyncEngine,
    schema_name: str,
) -> None:
    schema = _quoted(schema_name)
    statements = (
        f"CREATE SCHEMA {schema}",
        f"""
        CREATE TABLE {schema}.ptg2_v3_snapshot_layout (
            snapshot_key bigint PRIMARY KEY,
            generation varchar(32) NOT NULL,
            state varchar(16) NOT NULL
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg2_v4_snapshot_map_root (
            snapshot_key bigint PRIMARY KEY,
            state varchar(16) NOT NULL,
            CONSTRAINT ptg2_v4_taxonomy_test_root_layout_fkey
                FOREIGN KEY (snapshot_key)
                REFERENCES {schema}.ptg2_v3_snapshot_layout (snapshot_key)
                ON DELETE CASCADE
        )
        """,
        f"""
        CREATE FUNCTION {schema}.guard_ptg2_v4_snapshot_metadata()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        DECLARE
            root_state varchar(16);
            layout_generation varchar(32);
            layout_state varchar(16);
        BEGIN
            IF TG_OP = 'DELETE' THEN
                SELECT candidate.state
                  INTO root_state
                  FROM {schema}.ptg2_v4_snapshot_map_root AS candidate
                 WHERE candidate.snapshot_key = OLD.snapshot_key;
                IF root_state = 'complete' AND pg_trigger_depth() = 1 THEN
                    RAISE EXCEPTION 'ptg2_v4_snapshot_metadata_sealed_delete'
                        USING ERRCODE = '55000';
                END IF;
                RETURN OLD;
            END IF;
            IF TG_OP = 'UPDATE' THEN
                RAISE EXCEPTION 'ptg2_v4_snapshot_metadata_immutable'
                    USING ERRCODE = '55000';
            END IF;
            SELECT candidate.state, layout.generation, layout.state
              INTO root_state, layout_generation, layout_state
              FROM {schema}.ptg2_v4_snapshot_map_root AS candidate
              JOIN {schema}.ptg2_v3_snapshot_layout AS layout
                ON layout.snapshot_key = candidate.snapshot_key
             WHERE candidate.snapshot_key = NEW.snapshot_key
             FOR UPDATE OF candidate, layout;
            IF root_state IS NULL THEN
                RAISE EXCEPTION 'ptg2_v4_snapshot_map_root_missing'
                    USING ERRCODE = '23503';
            END IF;
            IF root_state <> 'building'
               OR layout_generation <> 'shared_blocks_v4'
               OR layout_state <> 'building' THEN
                RAISE EXCEPTION 'ptg2_v4_snapshot_metadata_not_building'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END;
        $function$
        """,
        f"""
        CREATE FUNCTION {schema}.guard_ptg2_v4_snapshot_map_root()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        BEGIN
            IF TG_OP = 'DELETE'
               AND OLD.state = 'complete'
               AND pg_trigger_depth() = 1 THEN
                RAISE EXCEPTION 'ptg2_v4_snapshot_map_root_sealed_delete'
                    USING ERRCODE = '55000';
            END IF;
            RETURN OLD;
        END;
        $function$
        """,
        f"""
        CREATE TRIGGER ptg2_v4_taxonomy_test_root_guard
        BEFORE DELETE ON {schema}.ptg2_v4_snapshot_map_root
        FOR EACH ROW
        EXECUTE FUNCTION {schema}.guard_ptg2_v4_snapshot_map_root()
        """,
        f"""
        INSERT INTO {schema}.ptg2_v3_snapshot_layout
            (snapshot_key, generation, state)
        VALUES
            (11, 'shared_blocks_v4', 'building'),
            (12, 'shared_blocks_v4', 'building')
        """,
        f"""
        INSERT INTO {schema}.ptg2_v4_snapshot_map_root
            (snapshot_key, state)
        VALUES (11, 'building'), (12, 'complete')
        """,
    )
    async with engine.begin() as connection:
        for statement in statements:
            await connection.exec_driver_sql(statement)


def _insert_statement(schema_name: str) -> sa.TextClause:
    schema = _quoted(schema_name)
    return sa.text(
        f"""
        INSERT INTO {schema}.ptg2_v4_inferred_taxonomy_candidate (
            snapshot_key,
            rule_digest,
            catalog_contract,
            catalog_digest,
            vector_format,
            member_count,
            member_digest,
            member_keys,
            representation,
            pattern_count,
            pattern_member_count,
            pattern_member_bytes,
            pattern_member_digest,
            pattern_member_payload
        ) VALUES (
            :snapshot_key,
            decode(repeat(:rule_hex, 32), 'hex'),
            'snapshot_npi_live_catalog_individual_v1',
            decode(repeat('22', 32), 'hex'),
            'sorted_u32le_v1',
            2,
            decode(repeat('33', 32), 'hex'),
            decode('0100000002000000', 'hex'),
            :representation,
            :pattern_count,
            :pattern_member_count,
            :pattern_member_bytes,
            decode(repeat('44', 32), 'hex'),
            decode(:pattern_payload_hex, 'hex')
        )
        """
    )


def _valid_pattern_payload_hex() -> str:
    return (
        struct.pack("<8sIIQ", b"PTG4TXP2", 1, 1, 2)
        + struct.pack("<II", 0, 2)
        + struct.pack("<II", 1, 2)
    ).hex()


def _observe_insert_statement(schema_name: str) -> sa.TextClause:
    schema = _quoted(schema_name)
    return sa.text(
        f"""
        INSERT INTO {schema}.ptg2_v4_inferred_taxonomy_candidate (
            snapshot_key,
            rule_digest,
            catalog_contract,
            catalog_digest,
            vector_format,
            member_count,
            member_digest,
            member_keys,
            representation,
            observe_reason,
            observe_count_lower_bound,
            pattern_count,
            pattern_member_count,
            pattern_member_bytes,
            pattern_member_digest,
            pattern_member_payload
        ) VALUES (
            11,
            decode(repeat(:rule_hex, 32), 'hex'),
            'snapshot_npi_live_catalog_individual_v1',
            decode(repeat('22', 32), 'hex'),
            'sorted_u32le_v1',
            :member_count,
            decode(repeat('33', 32), 'hex'),
            :member_keys,
            'observe_v1',
            :observe_reason,
            :observe_count_lower_bound,
            0,
            0,
            0,
            decode(repeat('44', 32), 'hex'),
            decode('', 'hex')
        )
        """
    )


async def _drop_disposable_schema(
    engine: AsyncEngine,
    schema_name: str,
) -> None:
    if not _DISPOSABLE_SCHEMA_RE.fullmatch(schema_name):
        raise RuntimeError(f"refusing to drop non-disposable schema {schema_name!r}")
    async with engine.begin() as connection:
        await connection.exec_driver_sql(
            f"DROP SCHEMA IF EXISTS {_quoted(schema_name)} CASCADE"
        )


@pytest.mark.asyncio
async def test_taxonomy_sidecar_postgres_lifecycle(monkeypatch) -> None:
    """Prove real constraints, building guards, and root ownership cascade."""

    engine = create_async_engine(
        _async_database_url(),
        pool_size=1,
        max_overflow=0,
    )
    schema_name = f"ptg2_v4_taxonomy_test_{uuid.uuid4().hex}"
    migration = _load_migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    is_schema_created = False
    try:
        await _create_prerequisites(engine, schema_name)
        is_schema_created = True
        await _run_migration_action(engine, migration, "upgrade")

        async with engine.begin() as connection:
            await connection.execute(
                _insert_statement(schema_name),
                {
                    "snapshot_key": 11,
                    "rule_hex": "11",
                    "representation": "direct_v1",
                    "pattern_count": 0,
                    "pattern_member_count": 0,
                    "pattern_member_bytes": 0,
                    "pattern_payload_hex": "",
                },
            )
            await connection.execute(
                _insert_statement(schema_name),
                {
                    "snapshot_key": 11,
                    "rule_hex": "77",
                    "representation": "pattern_v1",
                    "pattern_count": 1,
                    "pattern_member_count": 2,
                    "pattern_member_bytes": 40,
                    "pattern_payload_hex": _valid_pattern_payload_hex(),
                },
            )
            await connection.execute(
                _observe_insert_statement(schema_name),
                {
                    "rule_hex": "88",
                    "member_count": 37_001,
                    "member_keys": b"\x00\x00\x00\x00" * 37_001,
                    "observe_reason": "candidate_cap_exceeded",
                    "observe_count_lower_bound": 37_001,
                },
            )
            await connection.execute(
                _observe_insert_statement(schema_name),
                {
                    "rule_hex": "99",
                    "member_count": 2,
                    "member_keys": b"\x00\x00\x00\x00" * 2,
                    "observe_reason": "pattern_projection_cap_exceeded",
                    "observe_count_lower_bound": 131_073,
                },
            )

        with pytest.raises(DBAPIError, match="pattern_check"):
            async with engine.begin() as connection:
                await connection.execute(
                    _observe_insert_statement(schema_name),
                    {
                        "rule_hex": "aa",
                        "member_count": 2,
                        "member_keys": b"\x00\x00\x00\x00" * 2,
                        "observe_reason": "pattern_projection_cap_exceeded",
                        "observe_count_lower_bound": 131_072,
                    },
                )

        with pytest.raises(DBAPIError, match="pattern_check"):
            async with engine.begin() as connection:
                await connection.execute(
                    _insert_statement(schema_name),
                    {
                        "snapshot_key": 11,
                        "rule_hex": "55",
                        "representation": "pattern_v1",
                        "pattern_count": 1,
                        "pattern_member_count": 1,
                        "pattern_member_bytes": 25,
                        "pattern_payload_hex": "00" * 25,
                    },
                )

        with pytest.raises(
            DBAPIError,
            match="ptg2_v4_snapshot_metadata_not_building",
        ):
            async with engine.begin() as connection:
                await connection.execute(
                    _insert_statement(schema_name),
                    {
                        "snapshot_key": 12,
                        "rule_hex": "66",
                        "representation": "direct_v1",
                        "pattern_count": 0,
                        "pattern_member_count": 0,
                        "pattern_member_bytes": 0,
                        "pattern_payload_hex": "",
                    },
                )

        schema = _quoted(schema_name)
        with pytest.raises(
            DBAPIError,
            match="ptg2_v4_snapshot_metadata_immutable",
        ):
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(
                        f"""
                        UPDATE {schema}.ptg2_v4_inferred_taxonomy_candidate
                           SET member_count = member_count
                         WHERE snapshot_key = 11
                        """
                    )
                )

        async with engine.begin() as connection:
            await connection.execute(
                sa.text(
                    f"""
                    UPDATE {schema}.ptg2_v4_snapshot_map_root
                       SET state = 'complete'
                     WHERE snapshot_key = 11
                    """
                )
            )
            await connection.execute(
                sa.text(
                    f"""
                    UPDATE {schema}.ptg2_v3_snapshot_layout
                       SET state = 'sealed'
                     WHERE snapshot_key = 11
                    """
                )
            )

        with pytest.raises(
            DBAPIError,
            match="ptg2_v4_snapshot_metadata_sealed_delete",
        ):
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(
                        f"""
                        DELETE FROM {schema}.ptg2_v4_inferred_taxonomy_candidate
                         WHERE snapshot_key = 11
                        """
                    )
                )

        with pytest.raises(
            DBAPIError,
            match="ptg2_v4_snapshot_map_root_sealed_delete",
        ):
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(
                        f"""
                        DELETE FROM {schema}.ptg2_v4_snapshot_map_root
                         WHERE snapshot_key = 11
                        """
                    )
                )

        async with engine.begin() as connection:
            await connection.execute(
                sa.text(
                    f"""
                    DELETE FROM {schema}.ptg2_v3_snapshot_layout
                     WHERE snapshot_key = 11
                    """
                )
            )
            remaining_candidates = await connection.scalar(
                sa.text(
                    f"""
                    SELECT COUNT(*)
                      FROM {schema}.ptg2_v4_inferred_taxonomy_candidate
                    """
                )
            )
            remaining_roots = await connection.scalar(
                sa.text(
                    f"""
                    SELECT COUNT(*)
                      FROM {schema}.ptg2_v4_snapshot_map_root
                     WHERE snapshot_key = 11
                    """
                )
            )
        assert remaining_candidates == 0
        assert remaining_roots == 0

        await _run_migration_action(engine, migration, "downgrade")
    finally:
        if is_schema_created:
            await _drop_disposable_schema(engine, schema_name)
        await engine.dispose()
