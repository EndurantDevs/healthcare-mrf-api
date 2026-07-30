# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
import os
import re
import uuid
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.sql.sqltypes import JSON as SQLAlchemyJSON

from db.models import (
    ProviderDirectoryOrganization,
    ProviderDirectoryOrganizationAffiliation,
    ProviderDirectoryPractitionerRole,
)


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260729120000_uhc_organization_evidence.py"
)
POSTGRES_DSN_ENV = "HLTHPRT_UHC_ORGANIZATION_EVIDENCE_POSTGRES_DSN"
DISPOSABLE_DATABASE_PATTERN = re.compile(
    r"^ptg2_v3_lifecycle_test_[a-z0-9][a-z0-9_]{7,}$"
)

EXPECTED_COLUMNS_BY_MODEL = {
    ProviderDirectoryOrganization: {
        "tin_status": sa.String,
        "source_lineage": SQLAlchemyJSON,
    },
    ProviderDirectoryPractitionerRole: {
        "plan_scope": SQLAlchemyJSON,
        "network_tier": sa.String,
        "network_key_id": sa.String,
    },
    ProviderDirectoryOrganizationAffiliation: {
        "insurance_plan_refs": SQLAlchemyJSON,
        "plan_scope": SQLAlchemyJSON,
        "network_tier": sa.String,
        "network_key_id": sa.String,
        "relationship_type": sa.String,
        "ownership_status": sa.String,
        "source_lineage": SQLAlchemyJSON,
    },
}


def _load_migration():
    migration_spec = importlib.util.spec_from_file_location(
        "uhc_organization_evidence_migration",
        MIGRATION_PATH,
    )
    assert migration_spec is not None and migration_spec.loader is not None
    migration = importlib.util.module_from_spec(migration_spec)
    migration_spec.loader.exec_module(migration)
    return migration


class _OperationRecorder:
    def __init__(self):
        self.added_columns = []
        self.dropped_columns = []

    def add_column(self, table_name, column, **kwargs):
        self.added_columns.append((table_name, column, kwargs))

    def drop_column(self, table_name, column_name, **kwargs):
        self.dropped_columns.append((table_name, column_name, kwargs))


def test_uhc_evidence_models_match_nullable_typed_contract():
    """Keep canonical rehydration fields aligned with SQLAlchemy models."""
    for model, expected_columns_by_name in EXPECTED_COLUMNS_BY_MODEL.items():
        for column_name, expected_type in expected_columns_by_name.items():
            column = model.__table__.c[column_name]
            assert column.nullable is True
            assert isinstance(column.type, expected_type)


def test_uhc_evidence_migration_descends_from_tin_connector(monkeypatch):
    """Require one exact Alembic child with reversible model parity."""
    migration = _load_migration()
    operations = _OperationRecorder()
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", operations)

    migration.upgrade()

    assert migration.revision == "20260729120000_uhc_organization_evidence"
    assert migration.down_revision == "20260729110000_tin_npi_connector"
    expected_pairs = {
        (model.__tablename__, column_name)
        for model, expected_columns_by_name in EXPECTED_COLUMNS_BY_MODEL.items()
        for column_name in expected_columns_by_name
    }
    assert {
        (table_name, column.name)
        for table_name, column, kwargs in operations.added_columns
        if kwargs == {"schema": "mrf"}
    } == expected_pairs
    assert len(operations.added_columns) == len(expected_pairs)

    migration.downgrade()

    assert {
        (table_name, column_name)
        for table_name, column_name, kwargs in operations.dropped_columns
        if kwargs == {"schema": "mrf"}
    } == expected_pairs


def test_uhc_evidence_migration_rejects_conflicting_schema_aliases(
    monkeypatch,
):
    """Fail closed when modern and legacy schema settings disagree."""
    migration = _load_migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "current_schema")
    monkeypatch.setenv("DB_SCHEMA", "legacy_schema")

    with pytest.raises(RuntimeError, match="must identify the same schema"):
        migration.upgrade()


def _postgres_url() -> URL:
    database_dsn = os.getenv(POSTGRES_DSN_ENV)
    if not database_dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for the PostgreSQL proof")
    database_url = make_url(database_dsn)
    database_name = str(database_url.database or "")
    if (
        not database_url.drivername.startswith("postgresql")
        or DISPOSABLE_DATABASE_PATTERN.fullmatch(database_name) is None
        or not database_url.host
        or not database_url.username
    ):
        pytest.fail(
            f"{POSTGRES_DSN_ENV} must target an explicit disposable "
            "PostgreSQL database"
        )
    return database_url.set(drivername="postgresql+asyncpg")


def _run_migration_lifecycle(sync_connection, migration, schema) -> None:
    migration.op = Operations(MigrationContext.configure(sync_connection))
    migration.upgrade()
    migration.upgrade()
    inspector = sa.inspect(sync_connection)
    for table_name, columns in migration.UHC_EVIDENCE_COLUMNS_BY_TABLE.items():
        actual_columns_by_name = {
            column["name"]: column
            for column in inspector.get_columns(table_name, schema=schema)
        }
        for expected_column in columns:
            assert expected_column.name in actual_columns_by_name
            assert actual_columns_by_name[expected_column.name]["nullable"] is True

    sync_connection.exec_driver_sql("SAVEPOINT uhc_type_drift")
    sync_connection.exec_driver_sql(
        f'ALTER TABLE "{schema}".'
        '"provider_directory_organization" '
        'ALTER COLUMN "tin_status" TYPE varchar(32)'
    )
    with pytest.raises(
        RuntimeError,
        match="existing_schema_column_type_mismatch",
    ):
        migration.upgrade()
    sync_connection.exec_driver_sql("ROLLBACK TO SAVEPOINT uhc_type_drift")
    migration.downgrade()
    downgraded_inspector = sa.inspect(sync_connection)
    for table_name, columns in migration.UHC_EVIDENCE_COLUMNS_BY_TABLE.items():
        remaining_names = {
            column["name"]
            for column in downgraded_inspector.get_columns(
                table_name,
                schema=schema,
            )
        }
        assert remaining_names.isdisjoint(column.name for column in columns)


async def _create_migration_base_tables(connection, schema, table_names) -> None:
    await connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
    for table_name in table_names:
        await connection.exec_driver_sql(
            f'CREATE TABLE "{schema}"."{table_name}" '
            '("fixture_id" bigint PRIMARY KEY)'
        )


@pytest.mark.asyncio
async def test_uhc_evidence_migration_postgres_lifecycle(monkeypatch):
    """Prove idempotent upgrade, drift refusal, and exact downgrade."""
    schema = f"uhc_evidence_migration_{uuid.uuid4().hex[:12]}"
    migration = _load_migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setenv("DB_SCHEMA", schema)
    engine = create_async_engine(_postgres_url())
    table_names = tuple(migration.UHC_EVIDENCE_COLUMNS_BY_TABLE)

    try:
        async with engine.begin() as connection:
            await _create_migration_base_tables(connection, schema, table_names)
            await connection.run_sync(
                lambda sync_connection: _run_migration_lifecycle(
                    sync_connection,
                    migration,
                    schema,
                )
            )
            await connection.exec_driver_sql(
                f'DROP SCHEMA "{schema}" CASCADE'
            )
    finally:
        await engine.dispose()
