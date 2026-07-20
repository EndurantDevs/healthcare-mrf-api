# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import os
from pathlib import Path
import re
import subprocess
import sys
from types import SimpleNamespace

import asyncpg
import pytest
import sqlalchemy as sa
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from db import migration_index_adoption


ROOT = Path(__file__).resolve().parents[1]
OPT_IN_DSN_ENV = "HLTHPRT_PROVIDER_DIRECTORY_MIGRATION_POSTGRES_DSN"
DISPOSABLE_DATABASE_PATTERN = re.compile(
    r"^ptg2_v3_lifecycle_test_[a-z0-9][a-z0-9_]{7,}$"
)
BASELINE_REVISION = "20260610143000_address_checksums_bigint"
RUNTIME_SEED_REVISION = "20260709110000_provider_directory_overlay_coordinates"
PRE_REPAIR_REVISION = "20260714120000_ptg2_v3_schema_gc_consistency"
ADOPTION_SCHEMA = "mrf_provider_directory_adoption"
PLAN_TABLE = "provider_directory_dataset_insurance_plan"
ACTIVE_INDEX = "provider_directory_dataset_insurance_plan_active_lookup_idx"

RUNTIME_SCHEMA_SEED = r"""
import asyncio

from sqlalchemy import inspect

from db import models as loaded_models
from db.connection import Base, db
from db.maintenance import _ensure_columns, _ensure_indexes, _model_by_table_fullname


async def main():
    await db.connect()
    summary = {
        "tables": [],
        "columns": [],
        "indexes": [],
        "constraints": [],
        "skipped_columns": [],
        "retired": [],
    }
    async with db.engine.begin() as connection:
        def seed(sync_connection):
            models_by_table = _model_by_table_fullname()
            future_strict_tables = {
                "provider_directory_profile_build_checkpoint",
            }
            target_tables = [
                table
                for table in Base.metadata.sorted_tables
                if (
                    table.name == "import_run"
                    or table.name.startswith("provider_directory_")
                )
                and table.name not in future_strict_tables
            ]
            for table in target_tables:
                inspector = inspect(sync_connection)
                if not inspector.has_table(table.name, schema=table.schema):
                    table.create(bind=sync_connection, checkfirst=True)
                inspector = inspect(sync_connection)
                _ensure_columns(sync_connection, inspector, table, summary)
                inspector = inspect(sync_connection)
                model = models_by_table.get(table.fullname)
                if model is not None:
                    _ensure_indexes(sync_connection, inspector, table, model, summary)

        await connection.run_sync(seed)
    await db.disconnect()


asyncio.run(main())
"""


def _database_url():
    dsn = os.getenv(OPT_IN_DSN_ENV)
    if not dsn:
        pytest.skip(f"set {OPT_IN_DSN_ENV} to run PostgreSQL migration adoption")
    url = make_url(dsn)
    if not url.drivername.startswith("postgresql"):
        pytest.fail(f"{OPT_IN_DSN_ENV} must use PostgreSQL")
    database_name = str(url.database or "")
    if not DISPOSABLE_DATABASE_PATTERN.fullmatch(database_name):
        pytest.fail(
            f"refusing non-disposable PostgreSQL database {database_name!r}"
        )
    if not url.host or not url.username:
        pytest.fail(f"{OPT_IN_DSN_ENV} must include an explicit host and user")
    return url


def _database_environment(url, schema: str) -> dict[str, str]:
    environment = os.environ.copy()
    environment.update(
        {
            "HLTHPRT_DB_DRIVER": "asyncpg",
            "HLTHPRT_DB_HOST": str(url.host),
            "HLTHPRT_DB_PORT": str(url.port or 5432),
            "HLTHPRT_DB_USER": str(url.username),
            "HLTHPRT_DB_PASSWORD": str(url.password or ""),
            "HLTHPRT_DB_DATABASE": str(url.database),
            "HLTHPRT_DB_SCHEMA": schema,
            "DB_SCHEMA": schema,
            "HLTHPRT_MANAGED_SCHEMAS": schema,
        }
    )
    environment.pop("HLTHPRT_DB_DATABASE_OVERRIDE", None)
    return environment


def _run_alembic(environment: dict[str, str], *arguments: str) -> None:
    subprocess.run(
        [sys.executable, "-m", "alembic", *arguments],
        cwd=ROOT,
        env=environment,
        check=True,
    )


async def _connect(url):
    return await asyncpg.connect(
        host=str(url.host),
        port=int(url.port or 5432),
        user=str(url.username),
        password=str(url.password or ""),
        database=str(url.database),
    )


async def _assert_active_index_shape(url, schema: str) -> None:
    async_url = url.set(drivername="postgresql+asyncpg")
    engine = create_async_engine(async_url)
    try:
        async with engine.connect() as connection:
            shape = await connection.run_sync(
                lambda sync_connection: migration_index_adoption._existing_index_shape(
                    SimpleNamespace(get_bind=lambda: sync_connection),
                    ACTIVE_INDEX,
                    PLAN_TABLE,
                    schema,
                )
            )
            matches = await connection.run_sync(
                lambda sync_connection: migration_index_adoption.has_matching_index(
                    SimpleNamespace(get_bind=lambda: sync_connection),
                    ACTIVE_INDEX,
                    PLAN_TABLE,
                    ("dataset_id", "resource_id"),
                    schema=schema,
                    postgresql_include=("plan_identifier",),
                    postgresql_where=sa.text("plan_active"),
                )
            )
    finally:
        await engine.dispose()
    assert matches is True
    assert shape is not None
    assert shape.key_columns == ("dataset_id", "resource_id")
    assert shape.include_columns == ("plan_identifier",)
    assert shape.access_method == "btree"
    assert len(shape.operator_classes) == 2
    assert len(shape.collations) == 2
    assert shape.sort_options == (0, 0)
    assert shape.expression_keys is False


def test_provider_directory_runtime_schema_adoption_and_index_repair_cycle():
    url = _database_url()
    asyncio.run(_assert_active_index_shape(url, "mrf"))
    environment = _database_environment(url, ADOPTION_SCHEMA)

    async def reset_adoption_schema() -> None:
        connection = await _connect(url)
        try:
            await connection.execute(f'DROP SCHEMA IF EXISTS "{ADOPTION_SCHEMA}" CASCADE')
            await connection.execute(f'CREATE SCHEMA "{ADOPTION_SCHEMA}"')
        finally:
            await connection.close()

    async def drop_adoption_schema() -> None:
        connection = await _connect(url)
        try:
            await connection.execute(f'DROP SCHEMA IF EXISTS "{ADOPTION_SCHEMA}" CASCADE')
        finally:
            await connection.close()

    asyncio.run(reset_adoption_schema())
    try:
        _run_alembic(environment, "stamp", BASELINE_REVISION)
        _run_alembic(environment, "upgrade", RUNTIME_SEED_REVISION)
        subprocess.run(
            [sys.executable, "-c", RUNTIME_SCHEMA_SEED],
            cwd=ROOT,
            env=environment,
            check=True,
        )
        _run_alembic(environment, "upgrade", "head")
        asyncio.run(_assert_active_index_shape(url, ADOPTION_SCHEMA))

        _run_alembic(environment, "downgrade", PRE_REPAIR_REVISION)

        async def install_legacy_index() -> None:
            connection = await _connect(url)
            try:
                await connection.execute(
                    f'DROP INDEX "{ADOPTION_SCHEMA}"."{ACTIVE_INDEX}"'
                )
                await connection.execute(
                    f'CREATE INDEX "{ACTIVE_INDEX}" '
                    f'ON "{ADOPTION_SCHEMA}"."{PLAN_TABLE}" '
                    "(dataset_id, resource_id, plan_identifier) WHERE plan_active"
                )
            finally:
                await connection.close()

        asyncio.run(install_legacy_index())
        _run_alembic(environment, "upgrade", "head")
        asyncio.run(_assert_active_index_shape(url, ADOPTION_SCHEMA))
    finally:
        asyncio.run(drop_adoption_schema())
