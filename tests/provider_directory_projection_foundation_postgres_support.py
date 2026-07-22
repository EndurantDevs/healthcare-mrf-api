# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL support for physical-projection foundation proofs."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
import importlib.util
import os
from pathlib import Path
import re
from typing import AsyncIterator, Iterable
import uuid

import asyncpg
from alembic.migration import MigrationContext
from alembic.operations import Operations
import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from db.connection import Database


ROOT = Path(__file__).resolve().parents[1]
PREDECESSOR_MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / "20260721160000_provider_directory_retained_artifact_acquisition.py"
)
MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / "20260721170000_provider_directory_physical_projection.py"
)
POSTGRES_DSN_ENV = "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_POSTGRES_DSN"
DISPOSABLE_DATABASE_PATTERN = re.compile(
    r"^ptg2_v3_lifecycle_test_[a-z0-9_]+$"
)
PROJECTION_RELATIONS = (
    "provider_directory_physical_projection",
    "provider_directory_projection_recipe",
    "provider_directory_projection_input_block",
    "provider_directory_projection_proof_shard",
    "provider_directory_physical_projection_resource",
    "provider_directory_physical_projection_resource_membership",
    "provider_directory_physical_projection_profile_contribution",
    "provider_directory_physical_projection_partition",
    "provider_directory_physical_projection_source_summary",
    "provider_directory_physical_projection_reference",
)


def _load_migration(migration_path: Path = MIGRATION_PATH):
    module_spec = importlib.util.spec_from_file_location(
        f"provider_directory_projection_test_migration_{migration_path.stem}",
        migration_path,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration_module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration_module)
    return migration_module


def _run_migration(
    sync_connection,
    operation: str,
    migration_path: Path = MIGRATION_PATH,
) -> None:
    migration_module = _load_migration(migration_path)
    migration_module.op = Operations(MigrationContext.configure(sync_connection))
    getattr(migration_module, operation)()


def _database_url():
    database_dsn = os.getenv(POSTGRES_DSN_ENV)
    if not database_dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for disposable PostgreSQL proofs")
    database_url = make_url(database_dsn)
    database_name = str(database_url.database or "")
    if (
        not database_url.drivername.startswith("postgresql")
        or not database_url.host
        or not database_url.username
        or not DISPOSABLE_DATABASE_PATTERN.fullmatch(database_name)
    ):
        pytest.fail(
            f"{POSTGRES_DSN_ENV} must target an explicit disposable PostgreSQL "
            "database named ptg2_v3_lifecycle_test_*"
        )
    return database_url


@dataclass
class ProjectionFoundationPostgres:
    """One isolated schema plus runtime and migration database handles."""

    database: Database
    migration_engine: AsyncEngine
    retained_connection: asyncpg.Connection
    schema: str

    async def upgrade(self) -> None:
        async with self.migration_engine.begin() as migration_connection:
            await migration_connection.run_sync(
                lambda sync_connection: _run_migration(sync_connection, "upgrade")
            )

    async def downgrade(self) -> None:
        async with self.migration_engine.begin() as migration_connection:
            await migration_connection.run_sync(
                lambda sync_connection: _run_migration(sync_connection, "downgrade")
            )

    async def relation_oids(
        self,
        relation_names: Iterable[str] = PROJECTION_RELATIONS,
    ) -> dict[str, int | None]:
        relation_oid_by_name: dict[str, int | None] = {}
        for relation_name in relation_names:
            relation_oid = await self.database.scalar(
                "SELECT CAST(to_regclass(:qualified_relation) AS oid);",
                qualified_relation=f"{self.schema}.{relation_name}",
            )
            relation_oid_by_name[relation_name] = (
                int(relation_oid) if relation_oid is not None else None
            )
        return relation_oid_by_name

    async def catalog_snapshot(self) -> dict[str, dict[str, tuple]]:
        """Return exact schema-owned relation, constraint, trigger, and function proof."""

        relation_rows = await self.database.all(
            """
            SELECT class_record.relname, class_record.oid::bigint,
                   class_record.relkind,
                   CASE WHEN class_record.relkind = 'i'
                        THEN pg_get_indexdef(class_record.oid)
                        ELSE NULL END
              FROM pg_class AS class_record
              JOIN pg_namespace AS namespace_record
                ON namespace_record.oid = class_record.relnamespace
             WHERE namespace_record.nspname = :schema
               AND class_record.relkind IN ('r', 'p', 'i')
             ORDER BY class_record.relname;
            """,
            schema=self.schema,
        )
        constraint_rows = await self.database.all(
            """
            SELECT table_record.relname, constraint_record.conname,
                   constraint_record.oid::bigint,
                   pg_get_constraintdef(constraint_record.oid, true)
              FROM pg_constraint AS constraint_record
              JOIN pg_namespace AS namespace_record
                ON namespace_record.oid = constraint_record.connamespace
              JOIN pg_class AS table_record
                ON table_record.oid = constraint_record.conrelid
             WHERE namespace_record.nspname = :schema
             ORDER BY table_record.relname, constraint_record.conname;
            """,
            schema=self.schema,
        )
        trigger_rows = await self.database.all(
            """
            SELECT table_record.relname, trigger_record.tgname,
                   trigger_record.oid::bigint,
                   pg_get_triggerdef(trigger_record.oid, true)
              FROM pg_trigger AS trigger_record
              JOIN pg_class AS table_record
                ON table_record.oid = trigger_record.tgrelid
              JOIN pg_namespace AS namespace_record
                ON namespace_record.oid = table_record.relnamespace
             WHERE namespace_record.nspname = :schema
               AND NOT trigger_record.tgisinternal
             ORDER BY table_record.relname, trigger_record.tgname;
            """,
            schema=self.schema,
        )
        function_rows = await self.database.all(
            """
            SELECT procedure_record.proname,
                   pg_get_function_identity_arguments(procedure_record.oid),
                   procedure_record.oid::bigint,
                   pg_get_functiondef(procedure_record.oid)
              FROM pg_proc AS procedure_record
              JOIN pg_namespace AS namespace_record
                ON namespace_record.oid = procedure_record.pronamespace
             WHERE namespace_record.nspname = :schema
             ORDER BY procedure_record.proname,
                      pg_get_function_identity_arguments(procedure_record.oid);
            """,
            schema=self.schema,
        )
        return {
            "relations": {
                str(catalog_row[0]): tuple(catalog_row[1:])
                for catalog_row in relation_rows
            },
            "constraints": {
                f"{catalog_row[0]}.{catalog_row[1]}": tuple(catalog_row[2:])
                for catalog_row in constraint_rows
            },
            "triggers": {
                f"{catalog_row[0]}.{catalog_row[1]}": tuple(catalog_row[2:])
                for catalog_row in trigger_rows
            },
            "functions": {
                f"{catalog_row[0]}({catalog_row[1]})": tuple(catalog_row[2:])
                for catalog_row in function_rows
            },
        }


@asynccontextmanager
async def projection_foundation_postgres(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[ProjectionFoundationPostgres]:
    """Yield an isolated schema at the retained-acquisition predecessor."""

    database_url = _database_url()
    schema_name = f"mrf_projection_foundation_{uuid.uuid4().hex[:12]}"
    monkeypatch.setenv("HLTHPRT_DB_DRIVER", "asyncpg")
    monkeypatch.setenv("HLTHPRT_DB_HOST", str(database_url.host))
    monkeypatch.setenv("HLTHPRT_DB_PORT", str(database_url.port or 5432))
    monkeypatch.setenv("HLTHPRT_DB_USER", str(database_url.username))
    monkeypatch.setenv("HLTHPRT_DB_PASSWORD", str(database_url.password or ""))
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", str(database_url.database))
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_RETAINED_ARTIFACT_KEY_ID",
        "projection-foundation-test-v1",
    )
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_RETAINED_ARTIFACT_KEY",
        "projection-foundation-retained-test-key-material",
    )
    monkeypatch.delenv("HLTHPRT_DB_DATABASE_OVERRIDE", raising=False)

    migration_engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg")
    )
    runtime_database = Database()
    await runtime_database.connect()
    async with migration_engine.begin() as migration_connection:
        await migration_connection.exec_driver_sql(f'CREATE SCHEMA "{schema_name}"')
        await migration_connection.run_sync(
            lambda sync_connection: _run_migration(
                sync_connection,
                "upgrade",
                PREDECESSOR_MIGRATION_PATH,
            )
        )
    retained_connection = await asyncpg.connect(
        host=str(database_url.host),
        port=int(database_url.port or 5432),
        user=str(database_url.username),
        password=str(database_url.password or ""),
        database=str(database_url.database),
    )
    foundation_database = ProjectionFoundationPostgres(
        database=runtime_database,
        migration_engine=migration_engine,
        retained_connection=retained_connection,
        schema=schema_name,
    )
    try:
        yield foundation_database
    finally:
        await retained_connection.close()
        await runtime_database.disconnect()
        async with migration_engine.begin() as cleanup_connection:
            await cleanup_connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'
            )
        await migration_engine.dispose()


__all__ = [
    "MIGRATION_PATH",
    "PREDECESSOR_MIGRATION_PATH",
    "PROJECTION_RELATIONS",
    "ProjectionFoundationPostgres",
    "projection_foundation_postgres",
]
