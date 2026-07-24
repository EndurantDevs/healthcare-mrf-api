# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL autogenerate parity for V4 attempt authority metadata."""

from __future__ import annotations

from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from functools import partial
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from db.ptg2_v4_attempt_schema import (
    ATTEMPT_FENCE_TABLE,
    ATTEMPT_STAGE_RUN_INDEX,
    ATTEMPT_STAGE_TABLE,
    fence_table_elements,
    stage_table_elements,
)
from tests.ptg2_v4_attempt_migration_postgres_support import (
    FENCE_MIGRATION,
    attempt_migration_database,
    migration,
    run_migration_action,
)
from tests.ptg2_v4_stale_metadata_postgres_support import quoted


_TARGET_TABLES = {ATTEMPT_FENCE_TABLE, ATTEMPT_STAGE_TABLE}


def _target_metadata(schema_name: str) -> sa.MetaData:
    metadata = sa.MetaData()
    sa.Table(
        "ptg2_snapshot",
        metadata,
        sa.Column("snapshot_id", sa.String(96), primary_key=True),
        schema=schema_name,
    )
    sa.Table(
        "ptg2_import_run",
        metadata,
        sa.Column("import_run_id", sa.String(96), primary_key=True),
        schema=schema_name,
    )
    sa.Table(
        ATTEMPT_FENCE_TABLE,
        metadata,
        *fence_table_elements(schema_name),
        schema=schema_name,
    )
    sa.Table(
        ATTEMPT_STAGE_TABLE,
        metadata,
        *stage_table_elements(schema_name),
        sa.Index(ATTEMPT_STAGE_RUN_INDEX, "internal_run_id"),
        schema=schema_name,
    )
    return metadata


def _includes_target_object(
    database_object,
    _name,
    object_type,
    _is_reflected,
    _compared_object,
) -> bool:
    if object_type == "table":
        return database_object.name in _TARGET_TABLES
    owner_table = getattr(database_object, "table", None)
    if owner_table is not None:
        return owner_table.name in _TARGET_TABLES
    return True


def _includes_target_name(
    object_name,
    object_type,
    parent_names,
    *,
    expected_schema,
) -> bool:
    if object_type == "schema":
        return object_name == expected_schema
    if object_type == "table":
        return (
            parent_names.get("schema_name") == expected_schema
            and object_name in _TARGET_TABLES
        )
    return True


@pytest.mark.asyncio
async def test_live_attempt_catalog_has_no_model_autogenerate_drift(
    monkeypatch,
):
    async with attempt_migration_database(monkeypatch) as (
        dsn,
        schema_name,
        connection,
    ):
        await run_migration_action(
            dsn,
            migration(FENCE_MIGRATION),
            "upgrade",
        )
        schema = quoted(schema_name)
        await connection.execute(
            f"CREATE INDEX {ATTEMPT_STAGE_RUN_INDEX} "
            f"ON {schema}.{ATTEMPT_STAGE_TABLE} (internal_run_id)"
        )
        async_engine = create_async_engine(
            dsn.replace("postgresql://", "postgresql+asyncpg://", 1)
        )
        try:
            target_metadata = _target_metadata(schema_name)

            def model_differences(sync_connection):
                context = MigrationContext.configure(
                    sync_connection,
                    opts={
                        "include_schemas": True,
                        "include_object": _includes_target_object,
                        "include_name": partial(
                            _includes_target_name,
                            expected_schema=schema_name,
                        ),
                    },
                )
                return compare_metadata(context, target_metadata)

            async with async_engine.connect() as async_connection:
                differences = await async_connection.run_sync(
                    model_differences
                )
        finally:
            await async_engine.dispose()

        assert differences == []
