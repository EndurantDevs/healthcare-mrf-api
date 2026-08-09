# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable PostgreSQL support for canonical-NPI publication receipts."""

from __future__ import annotations

from contextlib import asynccontextmanager
import importlib.util
from pathlib import Path
from typing import Any, AsyncIterator

import asyncpg
import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from process.npi import (
    _install_npi_postseal_guards,
    _rotate_npi_canonical_table,
)
from tests.public_evidence_nppes_admission_postgres_support import (
    nppes_admission_schema,
    qualified,
)
from tests.public_evidence_storage_postgres_support import run_migration_action


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / "20260808230000_npi_canonical_publication_receipt.py"
)
CANONICAL_TABLES = (
    "npi",
    "npi_address",
    "npi_taxonomy",
    "npi_taxonomy_group",
    "npi_other_identifier",
    "npi_phone_staffing",
)


async def create_canonical_stage_tables(
    connection: asyncpg.Connection,
    schema_name: str,
    row_counts: tuple[int, ...],
    *,
    suffix: str = "20260809",
) -> dict[str, str]:
    """Create six synthetic staged relations with production-style index names."""

    assert len(row_counts) == len(CANONICAL_TABLES)
    stage_table_by_live_table: dict[str, str] = {}
    for table_name, row_count in zip(CANONICAL_TABLES, row_counts, strict=True):
        stage_table = f"{table_name}_{suffix}"
        stage_table_by_live_table[table_name] = stage_table
        await connection.execute(
            f"CREATE TABLE {qualified(schema_name, stage_table)} "
            "(synthetic_id bigint NOT NULL)"
        )
        if row_count:
            await connection.execute(
                f"INSERT INTO {qualified(schema_name, stage_table)} "
                "SELECT value FROM generate_series(1001, $1::bigint + 1000) AS value",
                row_count,
            )
        await connection.execute(
            f'CREATE UNIQUE INDEX "{stage_table}_idx_primary" '
            f"ON {qualified(schema_name, stage_table)} (synthetic_id)"
        )
    return stage_table_by_live_table


async def rotate_canonical_stage_tables(
    connection: asyncpg.Connection,
    schema_name: str,
    stage_table_by_live_table: dict[str, str],
) -> None:
    """Exercise the exact production guard installation and table rotation helpers."""

    await connection.execute(
        "LOCK TABLE "
        + ", ".join(
            qualified(schema_name, stage_table)
            for stage_table in stage_table_by_live_table.values()
        )
        + " IN ACCESS EXCLUSIVE MODE"
    )
    await _install_npi_postseal_guards(
        connection,
        schema=schema_name,
        stage_tables=tuple(stage_table_by_live_table.values()),
    )
    for live_table, stage_table in stage_table_by_live_table.items():
        await _rotate_npi_canonical_table(
            connection,
            schema=schema_name,
            live_table=live_table,
            stage_table=stage_table,
            index_suffixes=(),
        )


async def canonical_relation_state(
    connection: asyncpg.Connection,
    schema_name: str,
) -> tuple[tuple[int, ...], tuple[int, ...]]:
    """Return exact OIDs and row counts for the six current live relations."""

    relation_oids: list[int] = []
    row_counts: list[int] = []
    for table_name in CANONICAL_TABLES:
        relation_oids.append(
            await connection.fetchval(
                "SELECT to_regclass($1)::oid::bigint",
                f"{schema_name}.{table_name}",
            )
        )
        row_counts.append(
            await connection.fetchval(
                f"SELECT count(*)::bigint FROM {qualified(schema_name, table_name)}"
            )
        )
    return tuple(relation_oids), tuple(row_counts)


async def assert_published_state_is_frozen(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    """Reject every mutation of a sealed publication and its control run."""

    for statement in (
        f"UPDATE {qualified(schema_name, 'npi_canonical_publication_receipt')} "
        "SET npi_row_count=npi_row_count+1",
        f"DELETE FROM {qualified(schema_name, 'npi_canonical_publication_receipt')}",
        f"TRUNCATE {qualified(schema_name, 'npi_canonical_publication_receipt')}",
        f"UPDATE {qualified(schema_name, 'import_run')} "
        "SET status='failed' WHERE run_id='run_npi_publication_pg'",
    ):
        with pytest.raises(asyncpg.PostgresError):
            async with connection.transaction():
                await connection.execute(statement)


def load_publication_migration() -> Any:
    """Load the follow-on migration without package-global module state."""

    module_spec = importlib.util.spec_from_file_location(
        "npi_canonical_publication_postgres_proof",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


async def _create_runtime_roots(engine: AsyncEngine, schema_name: str) -> None:
    """Create the pre-existing control and canonical relations used by 210000."""

    async with engine.begin() as connection:
        await connection.exec_driver_sql(
            f"""
            CREATE TABLE {qualified(schema_name, 'import_run')} (
                run_id varchar(64) PRIMARY KEY,
                importer varchar(64) NOT NULL,
                status varchar(32) NOT NULL,
                phase_detail varchar(128),
                heartbeat_at timestamp,
                finished_at timestamp,
                progress json,
                metrics json,
                error json,
                snapshot_id varchar(96)
            )
            """
        )
        for ordinal, table_name in enumerate(CANONICAL_TABLES, start=1):
            table = qualified(schema_name, table_name)
            await connection.exec_driver_sql(
                f"CREATE TABLE {table} (synthetic_id bigint PRIMARY KEY)"
            )
            values = ",".join(f"({value})" for value in range(1, ordinal + 1))
            await connection.exec_driver_sql(
                f"INSERT INTO {table} (synthetic_id) VALUES {values}"
            )


@asynccontextmanager
async def npi_publication_schema() -> (
    AsyncIterator[tuple[AsyncEngine, Any, str, Any]]
):
    """Install NPI enumeration, NPPES lifecycle admission, and publication."""

    async with nppes_admission_schema() as (
        engine,
        database_url,
        schema_name,
        _admission_migration,
    ):
        await _create_runtime_roots(engine, schema_name)
        migration = load_publication_migration()
        migration._schema = lambda: schema_name
        await run_migration_action(engine, migration, "upgrade")
        yield engine, database_url, schema_name, migration


__all__ = (
    "CANONICAL_TABLES",
    "assert_published_state_is_frozen",
    "canonical_relation_state",
    "create_canonical_stage_tables",
    "load_publication_migration",
    "npi_publication_schema",
    "rotate_canonical_stage_tables",
)
