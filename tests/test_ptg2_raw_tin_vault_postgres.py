# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable PostgreSQL proof for the inactive encrypted TIN vault."""

from __future__ import annotations

import os
import re
import uuid
from contextlib import asynccontextmanager

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from tests.test_ptg2_raw_tin_vault_migration import _load_migration


POSTGRES_DSN_ENV = "HLTHPRT_PTG2_RAW_TIN_VAULT_POSTGRES_DSN"
_DISPOSABLE_DATABASE_RE = re.compile(
    r"(?:^test(?:[_-]|$)|(?:^|[_-])test(?:[_-]|$))",
    re.IGNORECASE,
)


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _database_url() -> sa.URL:
    raw_dsn = os.getenv(POSTGRES_DSN_ENV)
    if not raw_dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for the PostgreSQL proof")
    database_url = make_url(raw_dsn)
    database_name = str(database_url.database or "")
    if (
        not database_url.drivername.startswith("postgresql")
        or not _DISPOSABLE_DATABASE_RE.search(database_name)
    ):
        pytest.fail(f"{POSTGRES_DSN_ENV} must target an explicit test database")
    return database_url.set(drivername="postgresql+asyncpg")


async def _run_migration(connection, migration, action: str) -> None:
    def _run(sync_connection) -> None:
        context = MigrationContext.configure(sync_connection)
        migration.op = Operations(context)
        getattr(migration, action)()

    await connection.run_sync(_run)


@asynccontextmanager
async def _vault_schema():
    engine = create_async_engine(_database_url(), pool_pre_ping=True)
    schema_name = f"ptg2_raw_tin_vault_test_{uuid.uuid4().hex}"
    schema = _quoted(schema_name)
    migration = _load_migration()
    migration._schema = lambda: schema_name
    try:
        async with engine.begin() as connection:
            await connection.exec_driver_sql(f"CREATE SCHEMA {schema}")
            await connection.exec_driver_sql(
                f"""
                CREATE FUNCTION {schema}.
                    tin_npi_connector_token_policy_descriptor_sha256(text)
                RETURNS bytea
                LANGUAGE sql
                IMMUTABLE
                STRICT
                AS $function$
                    SELECT decode(repeat('11', 32), 'hex');
                $function$;
                """
            )
            await _run_migration(connection, migration, "upgrade")
        yield engine, schema_name, migration
    finally:
        async with engine.begin() as connection:
            await connection.exec_driver_sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        await engine.dispose()


def _valid_insert(schema_name: str) -> sa.TextClause:
    schema = _quoted(schema_name)
    return sa.text(
        f"""
        INSERT INTO {schema}.ptg2_raw_tin_vault_entry (
            token_policy_id,
            token_policy_descriptor_sha256,
            tin_hmac_sha256,
            tin_type,
            encryption_contract,
            binding_contract,
            encryption_key_id,
            ciphertext
        ) VALUES (
            'ptg-tin-hmac-sha256-v1:test-v1',
            decode(repeat('11', 32), 'hex'),
            decode(repeat('22', 32), 'hex'),
            'ein',
            'fernet_hmac_sha256_bound_v1',
            'token_policy_full_hmac_ein_v1',
            'vault-v1',
            'hptinv1:vault-v1:' || repeat('A', 80) || '='
        )
        """
    )


async def _load_catalog_snapshot(connection, schema_name: str):
    schema = _quoted(schema_name)
    row_count = await connection.scalar(
        sa.text(f"SELECT COUNT(*) FROM {schema}.ptg2_raw_tin_vault_entry")
    )
    columns = (
        await connection.execute(
            sa.text(
                """
                SELECT column_name, data_type
                  FROM information_schema.columns
                 WHERE table_schema = :schema_name
                   AND table_name = 'ptg2_raw_tin_vault_entry'
                 ORDER BY ordinal_position
                """
            ),
            {"schema_name": schema_name},
        )
    ).all()
    indexes = (
        await connection.execute(
            sa.text(
                """
                SELECT indexname, indexdef
                  FROM pg_indexes
                 WHERE schemaname = :schema_name
                   AND tablename = 'ptg2_raw_tin_vault_entry'
                """
            ),
            {"schema_name": schema_name},
        )
    ).all()
    triggers = (
        await connection.execute(
            sa.text(
                """
                SELECT trigger_record.tgname, trigger_record.tgenabled
                  FROM pg_trigger AS trigger_record
                 WHERE trigger_record.tgrelid = CAST(:table_name AS regclass)
                   AND NOT trigger_record.tgisinternal
                """
            ),
            {"table_name": f"{schema_name}.ptg2_raw_tin_vault_entry"},
        )
    ).all()
    return row_count, columns, indexes, triggers


def _assert_catalog_contract(row_count, columns, indexes, triggers) -> None:
    assert row_count == 0
    assert [str(column.column_name) for column in columns] == [
        "token_policy_id", "token_policy_descriptor_sha256", "tin_hmac_sha256",
        "tin_type", "encryption_contract", "binding_contract",
        "encryption_key_id", "ciphertext", "created_at", "updated_at",
    ]
    assert {str(index.indexname) for index in indexes} == {
        "ptg2_raw_tin_vault_entry_pkey",
        "ptg2_raw_tin_vault_encryption_key_idx",
    }
    index_definition_by_name = {
        str(index.indexname): str(index.indexdef) for index in indexes
    }
    assert "tin_hmac_sha256" in index_definition_by_name[
        "ptg2_raw_tin_vault_entry_pkey"
    ]
    assert "encryption_key_id" in index_definition_by_name[
        "ptg2_raw_tin_vault_encryption_key_idx"
    ]
    trigger_state_by_name = {
        str(trigger.tgname): (
            trigger.tgenabled.decode("ascii")
            if isinstance(trigger.tgenabled, bytes)
            else str(trigger.tgenabled)
        )
        for trigger in triggers
    }
    assert trigger_state_by_name == {
        "ptg2_raw_tin_vault_mutation_guard": "A",
        "ptg2_raw_tin_vault_truncate_guard": "A",
    }


@pytest.mark.asyncio
async def test_raw_tin_vault_postgres_catalog() -> None:
    """Prove the upgrade installs only an empty guarded ciphertext table."""

    async with _vault_schema() as (engine, schema_name, _migration):
        async with engine.begin() as connection:
            catalog_snapshot = await _load_catalog_snapshot(connection, schema_name)
        _assert_catalog_contract(*catalog_snapshot)


@pytest.mark.asyncio
async def test_raw_tin_vault_postgres_mutation_guards() -> None:
    """Prove identity, destruction, and nonempty-downgrade guards in PostgreSQL."""

    async with _vault_schema() as (engine, schema_name, migration):
        schema = _quoted(schema_name)
        async with engine.begin() as connection:
            await connection.execute(_valid_insert(schema_name))
        with pytest.raises(DBAPIError, match="identity_immutable"):
            async with engine.begin() as connection:
                await connection.execute(sa.text(
                    f"UPDATE {schema}.ptg2_raw_tin_vault_entry SET tin_type = 'other'"
                ))
        with pytest.raises(DBAPIError, match="rewrap_required"):
            async with engine.begin() as connection:
                await connection.execute(sa.text(
                    f"UPDATE {schema}.ptg2_raw_tin_vault_entry "
                    "SET encryption_key_id = encryption_key_id"
                ))
        async with engine.begin() as connection:
            await connection.execute(sa.text(f"""
                UPDATE {schema}.ptg2_raw_tin_vault_entry
                   SET encryption_key_id = 'vault-v2',
                       ciphertext = 'hptinv1:vault-v2:' || repeat('B', 80) || '='
            """))
        with pytest.raises(DBAPIError, match="delete_forbidden"):
            async with engine.begin() as connection:
                await connection.execute(sa.text(
                    f"DELETE FROM {schema}.ptg2_raw_tin_vault_entry"
                ))
        with pytest.raises(DBAPIError, match="truncate_forbidden"):
            async with engine.begin() as connection:
                await connection.execute(sa.text(
                    f"TRUNCATE {schema}.ptg2_raw_tin_vault_entry"
                ))
        with pytest.raises(DBAPIError, match="downgrade_requires_empty_foundation"):
            async with engine.begin() as connection:
                await _run_migration(connection, migration, "downgrade")


@pytest.mark.asyncio
async def test_raw_tin_vault_postgres_rejects_invalid_row_and_empty_downgrades() -> None:
    """Prove row checks, key tags, and the empty-only downgrade on PostgreSQL."""

    async with _vault_schema() as (engine, schema_name, migration):
        schema = _quoted(schema_name)
        with pytest.raises(DBAPIError):
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(
                        str(_valid_insert(schema_name)).replace(
                            "decode(repeat('22', 32), 'hex')",
                            "decode(repeat('22', 16), 'hex')",
                        )
                    )
                )
        with pytest.raises(DBAPIError):
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(
                        str(_valid_insert(schema_name)).replace(
                            "hptinv1:vault-v1:",
                            "hptinv1:wrong-v2:",
                        )
                    )
                )
        async with engine.begin() as connection:
            await _run_migration(connection, migration, "downgrade")
            table_name = await connection.scalar(
                sa.text("SELECT to_regclass(:table_name)"),
                {"table_name": f"{schema_name}.ptg2_raw_tin_vault_entry"},
            )
        assert table_name is None
