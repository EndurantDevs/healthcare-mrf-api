# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL lifecycle proof for the formulary serving index."""

from __future__ import annotations

from pathlib import Path
import uuid

import pytest
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.asyncio import create_async_engine

from tests.formulary_fhir_twin_admission_pg_support import load_migration
from tests.test_formulary_fhir_storage_postgres import _connect
from tests.test_formulary_fhir_storage_postgres import _database_url
from tests.test_formulary_fhir_storage_postgres import _drop_schema
from tests.test_formulary_fhir_storage_postgres import _quoted
from tests.test_formulary_fhir_storage_postgres import _run_migration_action


ROOT = Path(__file__).resolve().parents[1]
VERSIONS = ROOT / "alembic" / "versions"
MIGRATION_PATHS = (
    VERSIONS / "20260807110000_fhir_formulary_storage_foundation.py",
    VERSIONS / "20260808110000_fhir_formulary_twin_attempt.py",
    VERSIONS / "20260808120000_fhir_formulary_twin_admission.py",
    VERSIONS / "20260808130000_fhir_formulary_publication_guards.py",
)
SERVING_INDEX_PATH = VERSIONS / (
    "20260808160000_fhir_formulary_serving_index.py"
)
INDEX_NAME = "fhir_formulary_membership_medication_version_idx"
TABLE_NAME = "fhir_formulary_alias_membership"


async def _upgrade_chain(engine) -> None:
    for index, migration_path in enumerate(MIGRATION_PATHS):
        migration = load_migration(
            migration_path,
            f"formulary_serving_index_dependency_{index}",
        )
        await _run_migration_action(engine, migration, "upgrade")


async def _index_shape(connection, schema_name: str):
    return await connection.fetchrow(
        "SELECT access_method.amname AS access_method, "
        "catalog.indisunique AS is_unique, catalog.indisvalid AS is_valid, "
        "catalog.indisready AS is_ready, "
        "pg_get_expr(catalog.indpred, catalog.indrelid) AS predicate, "
        "array_agg(attribute.attname ORDER BY key.ordinality) AS columns "
        "FROM pg_index AS catalog "
        "JOIN pg_class AS index_relation ON index_relation.oid = catalog.indexrelid "
        "JOIN pg_class AS table_relation ON table_relation.oid = catalog.indrelid "
        "JOIN pg_namespace AS namespace ON namespace.oid = table_relation.relnamespace "
        "JOIN pg_am AS access_method ON access_method.oid = index_relation.relam "
        "JOIN LATERAL unnest(catalog.indkey) WITH ORDINALITY "
        "AS key(attribute_number, ordinality) ON true "
        "JOIN pg_attribute AS attribute ON attribute.attrelid = table_relation.oid "
        "AND attribute.attnum = key.attribute_number "
        "WHERE namespace.nspname = $1 AND table_relation.relname = $2 "
        "AND index_relation.relname = $3 "
        "GROUP BY access_method.amname, catalog.indisunique, catalog.indisvalid, "
        "catalog.indisready, catalog.indpred, catalog.indrelid",
        schema_name,
        TABLE_NAME,
        INDEX_NAME,
    )


async def _table_indexes(connection, schema_name: str) -> set[str]:
    rows = await connection.fetch(
        "SELECT indexname FROM pg_indexes WHERE schemaname = $1 "
        "AND tablename = $2",
        schema_name,
        TABLE_NAME,
    )
    return {str(row["indexname"]) for row in rows}


def _assert_exact_shape(index_shape) -> None:
    assert index_shape is not None
    assert dict(index_shape) == {
        "access_method": "btree",
        "is_unique": False,
        "is_valid": True,
        "is_ready": True,
        "predicate": None,
        "columns": ["alias_version_id", "medication_version_id"],
    }


@pytest.mark.asyncio
async def test_serving_index_upgrade_downgrade_reupgrade_exact_shape(monkeypatch):
    """Change only the exact two-column serving index across its lifecycle."""

    database_url = _database_url()
    schema_name = f"fhir_formulary_test_{uuid.uuid4().hex}"
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg")
    )
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    connection = None
    try:
        async with engine.begin() as engine_connection:
            await engine_connection.exec_driver_sql(
                f"CREATE SCHEMA {_quoted(schema_name)}"
            )
        await _upgrade_chain(engine)
        connection = await _connect(database_url)
        indexes_before = await _table_indexes(connection, schema_name)
        serving_migration = load_migration(
            SERVING_INDEX_PATH,
            "formulary_serving_index_lifecycle",
        )

        await _run_migration_action(engine, serving_migration, "upgrade")
        _assert_exact_shape(await _index_shape(connection, schema_name))
        assert await _table_indexes(connection, schema_name) == (
            indexes_before | {INDEX_NAME}
        )

        await _run_migration_action(engine, serving_migration, "downgrade")
        assert await _index_shape(connection, schema_name) is None
        assert await _table_indexes(connection, schema_name) == indexes_before
        assert await connection.fetchval(
            "SELECT to_regclass($1)",
            f"{schema_name}.{TABLE_NAME}",
        ) == f"{schema_name}.{TABLE_NAME}"

        await _run_migration_action(engine, serving_migration, "upgrade")
        _assert_exact_shape(await _index_shape(connection, schema_name))
        assert await _table_indexes(connection, schema_name) == (
            indexes_before | {INDEX_NAME}
        )
    finally:
        if connection is not None:
            await connection.close()
        await _drop_schema(engine, schema_name)
        await engine.dispose()


@pytest.mark.asyncio
async def test_serving_index_rejects_wrong_preexisting_named_index(monkeypatch):
    """Fail rather than accept a weaker index hidden behind the owned name."""

    database_url = _database_url()
    schema_name = f"fhir_formulary_test_{uuid.uuid4().hex}"
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg")
    )
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    connection = None
    try:
        async with engine.begin() as engine_connection:
            await engine_connection.exec_driver_sql(
                f"CREATE SCHEMA {_quoted(schema_name)}"
            )
        await _upgrade_chain(engine)
        connection = await _connect(database_url)
        await connection.execute(
            f"CREATE INDEX {_quoted(INDEX_NAME)} ON "
            f"{_quoted(schema_name)}.{_quoted(TABLE_NAME)} "
            "(alias_version_id)"
        )
        serving_migration = load_migration(
            SERVING_INDEX_PATH,
            "formulary_serving_index_collision",
        )

        with pytest.raises(ProgrammingError, match="already exists"):
            await _run_migration_action(engine, serving_migration, "upgrade")

        wrong_shape = await _index_shape(connection, schema_name)
        assert wrong_shape is not None
        assert wrong_shape["columns"] == ["alias_version_id"]
    finally:
        if connection is not None:
            await connection.close()
        await _drop_schema(engine, schema_name)
        await engine.dispose()
