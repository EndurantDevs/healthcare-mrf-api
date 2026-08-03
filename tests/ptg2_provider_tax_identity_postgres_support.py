# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable PostgreSQL support for provider tax-identity proofs."""

from __future__ import annotations

import importlib.util
import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from alembic.migration import MigrationContext
from alembic.operations import Operations
import pytest
import sqlalchemy as sa
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine

from process.tin_npi_connector_security import token_policy_descriptor_sha256


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / "20260727100000_ptg2_provider_tax_identity.py"
)
POSTGRES_DSN_ENV = "HLTHPRT_PTG2_TAX_IDENTITY_POSTGRES_DSN"
_DISPOSABLE_DATABASE_RE = re.compile(
    r"(?:^test(?:[_-]|$)|(?:^|[_-])test(?:[_-]|$))",
    re.IGNORECASE,
)
_DISPOSABLE_SCHEMA_RE = re.compile(
    r"^ptg2_tax_identity_test_[0-9a-f]{32}$"
)


def load_migration() -> Any:
    """Load the additive migration without importing repository state."""

    spec = importlib.util.spec_from_file_location(
        "ptg2_provider_tax_identity_postgres_proof",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def quoted(identifier: str) -> str:
    """Quote one PostgreSQL identifier."""

    return '"' + str(identifier).replace('"', '""') + '"'


def manifest_insert(
    schema_name: str,
    *,
    snapshot_key: int = 11,
) -> sa.TextClause:
    """Build the exact immutable manifest insert."""

    schema = quoted(schema_name)
    return sa.text(
        f"""
        INSERT INTO {schema}.ptg2_provider_tax_identity_manifest (
            snapshot_key, contract, token_policy_id,
            token_policy_descriptor_sha256, normalization_contract,
            hmac_contract, source_ordinal_contract, source_ordinal_map,
            source_ordinal_map_digest, source_shard_count,
            provider_group_count, tax_identity_count, matched_ein_count,
            missing_count, malformed_count, unsupported_type_count,
            content_digest
        ) VALUES (
            {snapshot_key},
            'ptg2_provider_group_tax_identity_v1',
            :token_policy_id,
            decode(:token_policy_descriptor_sha256, 'hex'),
            :normalization_contract,
            :hmac_contract,
            'snapshot_shard_id_sorted_lsb0_bitmap_v1',
            CAST(:source_map AS jsonb),
            decode(repeat('22', 32), 'hex'),
            :source_shard_count, 4, 1, 1, 1, 1, 1,
            decode(repeat('33', 32), 'hex')
        )
        """
    )


def manifest_parameters(**overrides: Any) -> dict[str, Any]:
    """Return exact Release-1 manifest contract parameters."""

    parameter_by_name: dict[str, Any] = {
        "source_map": '[{"ordinal":0,"shard_id":"shard-a"}]',
        "source_shard_count": 1,
        "token_policy_id": "ptg-tin-hmac-sha256-v1:2026-07",
        "token_policy_descriptor_sha256": token_policy_descriptor_sha256(
            "ptg-tin-hmac-sha256-v1:2026-07"
        ),
        "normalization_contract": "ein_ascii_digits_or_2_7_hyphen_v1",
        "hmac_contract": "hmac_sha256_ptg_tin_v1",
    }
    parameter_by_name.update(overrides)
    return parameter_by_name


def invalid_source_entries_by_snapshot() -> dict[int, list[dict[str, Any]]]:
    """Return exact malformed source-map cases accepted by base CHECKs."""

    return {
        21: [{"shard_id": "shard-a", "ordinal": 0, "extra": True}],
        22: [{"shard_id": "shard-a", "ordinal": 1}],
        23: [
            {"shard_id": "shard-a", "ordinal": 0},
            {"shard_id": "shard-a", "ordinal": 1},
        ],
        24: [
            {"shard_id": "shard-b", "ordinal": 0},
            {"shard_id": "shard-a", "ordinal": 1},
        ],
        25: [{"shard_id": "", "ordinal": 0}],
    }


async def insert_candidate_sidecar(
    engine: AsyncEngine,
    schema_name: str,
    *,
    snapshot_key: int,
    group_limit: int,
    bitmap_hex: str,
    manifest_overrides: Mapping[str, Any] | None = None,
) -> None:
    """Insert one building-root fixture through the exact sidecar schema."""

    schema = quoted(schema_name)
    async with engine.begin() as connection:
        await connection.execute(
            manifest_insert(schema_name, snapshot_key=snapshot_key),
            manifest_parameters(**dict(manifest_overrides or {})),
        )
        await connection.execute(
            sa.text(
                f"""
                INSERT INTO {schema}.ptg2_provider_tax_identity (
                    snapshot_key, tin_key, tin_id_128, tin_hmac_sha256
                ) VALUES (
                    :snapshot_key, 0, decode(repeat('44', 16), 'hex'),
                    decode(repeat('44', 16) || repeat('55', 16), 'hex')
                )
                """
            ),
            {"snapshot_key": snapshot_key},
        )
        await connection.execute(
            sa.text(
                f"""
                INSERT INTO {schema}.ptg2_provider_group_tax_identity (
                    snapshot_key, provider_group_global_id_128,
                    tax_identity_state, tin_key, source_bitmap
                )
                SELECT :snapshot_key, provider_group_global_id_128,
                       CASE provider_group_key
                           WHEN 1 THEN 'matched_ein'
                           WHEN 2 THEN 'missing'
                           WHEN 3 THEN 'malformed'
                           WHEN 4 THEN 'unsupported_type'
                       END,
                       CASE WHEN provider_group_key = 1 THEN 0 END,
                       decode(:bitmap_hex, 'hex')
                  FROM {schema}.ptg2_v3_provider_group
                 WHERE snapshot_key = :snapshot_key
                   AND provider_group_key <= :group_limit
                """
            ),
            {
                "snapshot_key": snapshot_key,
                "bitmap_hex": bitmap_hex,
                "group_limit": group_limit,
            },
        )


async def assert_pre_sidecar_v4_completion(
    engine: AsyncEngine,
    schema_name: str,
) -> None:
    """Prove a legacy V4 root without the additive manifest still completes."""

    schema = quoted(schema_name)
    async with engine.begin() as connection:
        completed_snapshot_key = await connection.scalar(
            sa.text(
                f"""
                UPDATE {schema}.ptg2_v4_snapshot_map_root
                   SET state = 'complete'
                 WHERE snapshot_key = 19
                   AND state = 'building'
                RETURNING snapshot_key
                """
            )
        )
        manifest_count = await connection.scalar(
            sa.text(
                f"""
                SELECT COUNT(*)
                  FROM {schema}.ptg2_provider_tax_identity_manifest
                 WHERE snapshot_key = 19
                """
            )
        )
    assert completed_snapshot_key == 19
    assert manifest_count == 0


async def assert_new_v4_requires_sidecar(
    engine: AsyncEngine,
    schema_name: str,
) -> None:
    """Allow only migration-adopted roots to complete without a manifest."""

    schema = quoted(schema_name)
    async with engine.begin() as connection:
        await connection.execute(
            sa.text(
                f"""
                INSERT INTO {schema}.ptg2_v3_snapshot_layout
                    (snapshot_key, generation, state)
                VALUES (26, 'shared_blocks_v4', 'building')
                """
            )
        )
        await connection.execute(
            sa.text(
                f"""
                INSERT INTO {schema}.ptg2_v4_snapshot_map_root
                    (snapshot_key, state)
                VALUES (26, 'building')
                """
            )
        )
    with pytest.raises(
        DBAPIError,
        match="ptg2_provider_tax_identity_manifest_missing",
    ):
        async with engine.begin() as connection:
            await connection.execute(
                sa.text(
                    f"UPDATE {schema}.ptg2_v4_snapshot_map_root "
                    "SET state = 'complete' WHERE snapshot_key = 26"
                )
            )


async def assert_layout_cascade(
    engine: AsyncEngine,
    schema_name: str,
) -> None:
    """Delete one layout and prove every tax-identity relation cascades."""

    schema = quoted(schema_name)
    table_names = (
        "ptg2_provider_tax_identity_legacy_layout",
        "ptg2_provider_tax_identity_manifest",
        "ptg2_provider_tax_identity",
        "ptg2_provider_group_tax_identity",
    )
    async with engine.begin() as connection:
        await connection.execute(
            sa.text(
                f"DELETE FROM {schema}.ptg2_v3_snapshot_layout "
                "WHERE snapshot_key = 11"
            )
        )
        remaining_count_by_table = {
            table_name: int(
                await connection.scalar(
                    sa.text(
                        f"SELECT COUNT(*) FROM {schema}.{table_name} "
                        "WHERE snapshot_key = 11"
                    )
                )
                or 0
            )
            for table_name in table_names
        }
    assert set(remaining_count_by_table.values()) == {0}


def async_database_url() -> sa.URL:
    """Resolve an explicit PostgreSQL test database."""

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


async def run_migration_action(
    engine: AsyncEngine,
    migration: Any,
    action: str,
) -> None:
    """Run one Alembic action on the disposable schema."""

    async with engine.connect() as async_connection:

        def run_action(sync_connection) -> None:
            migration_context = MigrationContext.configure(sync_connection)
            migration.op = Operations(migration_context)
            with migration_context.begin_transaction():
                getattr(migration, action)()

        await async_connection.run_sync(run_action)


def _prerequisite_table_statements(schema_name: str) -> tuple[str, ...]:
    schema = quoted(schema_name)
    return (
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
            CONSTRAINT ptg2_tax_identity_test_root_layout_fkey
                FOREIGN KEY (snapshot_key)
                REFERENCES {schema}.ptg2_v3_snapshot_layout (snapshot_key)
                ON DELETE CASCADE
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg2_v3_provider_group (
            snapshot_key bigint NOT NULL,
            provider_group_key integer NOT NULL,
            provider_group_global_id_128 bytea NOT NULL,
            PRIMARY KEY (snapshot_key, provider_group_key),
            UNIQUE (snapshot_key, provider_group_global_id_128),
            CONSTRAINT ptg2_tax_identity_test_group_layout_fkey
                FOREIGN KEY (snapshot_key)
                REFERENCES {schema}.ptg2_v3_snapshot_layout (snapshot_key)
                ON DELETE CASCADE
        )
        """,
    )


def _prerequisite_fixture_statements(schema_name: str) -> tuple[str, ...]:
    schema = quoted(schema_name)
    return (
        f"""
        INSERT INTO {schema}.ptg2_v3_snapshot_layout
            (snapshot_key, generation, state)
        VALUES
            (11, 'shared_blocks_v4', 'building'),
            (12, 'shared_blocks_v4', 'building'),
            (13, 'shared_blocks_v4', 'building'),
            (14, 'shared_blocks_v4', 'building'),
            (15, 'shared_blocks_v4', 'building'),
            (16, 'shared_blocks_v4', 'building'),
            (17, 'shared_blocks_v4', 'building'),
            (18, 'shared_blocks_v4', 'building'),
            (19, 'shared_blocks_v4', 'building'),
            (20, 'shared_blocks_v4', 'building'),
            (21, 'shared_blocks_v4', 'building'),
            (22, 'shared_blocks_v4', 'building'),
            (23, 'shared_blocks_v4', 'building'),
            (24, 'shared_blocks_v4', 'building'),
            (25, 'shared_blocks_v4', 'building')
        """,
        f"""
        INSERT INTO {schema}.ptg2_v4_snapshot_map_root
            (snapshot_key, state)
        VALUES
            (11, 'building'),
            (12, 'complete'),
            (13, 'building'),
            (14, 'building'),
            (15, 'building'),
            (16, 'building'),
            (17, 'building'),
            (18, 'building'),
            (19, 'building'),
            (20, 'building'),
            (21, 'building'),
            (22, 'building'),
            (23, 'building'),
            (24, 'building'),
            (25, 'building')
        """,
        f"""
        INSERT INTO {schema}.ptg2_v3_provider_group (
            snapshot_key,
            provider_group_key,
            provider_group_global_id_128
        )
        SELECT snapshot_key,
               ordinal,
               decode(repeat(to_hex(ordinal), 32), 'hex')
          FROM generate_series(1, 4) AS ordinal
         CROSS JOIN (
             VALUES (11), (13), (14), (18), (20), (21), (22),
                    (23), (24), (25)
         ) AS snapshots(snapshot_key)
        """,
    )


async def create_prerequisites(
    engine: AsyncEngine,
    schema_name: str,
) -> None:
    """Create only the shared layout, V4 root, and provider dictionary."""

    async with engine.begin() as connection:
        statements = (
            *_prerequisite_table_statements(schema_name),
            *_prerequisite_fixture_statements(schema_name),
        )
        for statement in statements:
            await connection.exec_driver_sql(statement)


async def drop_disposable_schema(
    engine: AsyncEngine,
    schema_name: str,
) -> None:
    """Drop only a uniquely named schema created by this proof."""

    if not _DISPOSABLE_SCHEMA_RE.fullmatch(schema_name):
        raise RuntimeError(f"refusing to drop non-disposable schema {schema_name!r}")
    async with engine.begin() as connection:
        await connection.exec_driver_sql(
            f"DROP SCHEMA IF EXISTS {quoted(schema_name)} CASCADE"
        )
