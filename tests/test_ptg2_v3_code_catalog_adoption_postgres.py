# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native adoption of real historical catalog rows without inferred metadata."""

import importlib.util
import os
import re
import uuid
from pathlib import Path

import pytest
import pytest_asyncio
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

ROOT = Path(__file__).resolve().parents[1]
LEGACY = ROOT / "alembic/versions/20260215103000_add_code_catalog_and_crosswalk.py"
TARGET = ROOT / "alembic/versions/20260712120000_ptg2_v3_shared_schema.py"


def _module(path):
    spec = importlib.util.spec_from_file_location("catalog_migration_" + uuid.uuid4().hex, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest_asyncio.fixture
async def native_catalog():
    if os.getenv("HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST") != "1":
        pytest.skip("requires the existing isolated PostgreSQL test opt-in")
    name = os.getenv("HLTHPRT_DB_DATABASE", "")
    if not re.search(r"(?:^|[_-])test(?:[_-]|$)", name, re.I):
        pytest.skip("requires an explicitly selected test database")
    engine = create_async_engine(
        URL.create(
            "postgresql+asyncpg",
            username=os.getenv("HLTHPRT_DB_USER", "postgres"),
            password=os.getenv("HLTHPRT_DB_PASSWORD") or None,
            host=os.getenv("HLTHPRT_DB_HOST", "127.0.0.1"),
            port=int(os.getenv("HLTHPRT_DB_PORT", "5432")),
            database=name,
        )
    )
    schema = "catalog_adoption_test_" + uuid.uuid4().hex
    async with engine.begin() as connection:
        await connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
    try:
        yield engine, schema
    finally:
        try:
            async with engine.begin() as connection:
                await connection.exec_driver_sql(f'DROP SCHEMA "{schema}" CASCADE')
        finally:
            await engine.dispose()


async def _invoke(engine, schema, path, *, legacy=False):
    module = _module(path)

    def run(connection):
        module.op = Operations(MigrationContext.configure(connection))
        module._schema = lambda: schema
        if legacy:
            module.upgrade()
        else:
            module._create_runtime_control_prerequisites(schema)

    async with engine.begin() as connection:
        await connection.run_sync(run)


async def _catalog_columns(connection, schema):
    """Read column metadata using the witness's existing native connection."""
    return (
        (
            await connection.execute(
                sa.text(
                    "SELECT column_name,data_type,character_maximum_length,is_nullable,column_default "
                    "FROM information_schema.columns WHERE table_schema=:schema AND table_name='code_catalog' "
                    "ORDER BY ordinal_position"
                ),
                {"schema": schema},
            )
        )
        .mappings()
        .all()
    )


async def _witness(engine, schema):
    async with engine.connect() as connection:
        table = (
            (
                await connection.execute(
                    sa.text("SELECT oid,relfilenode FROM pg_class WHERE oid=to_regclass(:name)"),
                    {"name": f'"{schema}".code_catalog'},
                )
            )
            .mappings()
            .one()
        )
        indexes = (
            (
                await connection.execute(
                    sa.text(
                        "SELECT indexrelid AS oid,c.relfilenode,pg_get_indexdef(indexrelid) AS definition "
                        "FROM pg_index JOIN pg_class c ON c.oid=indexrelid "
                        "WHERE indrelid=to_regclass(:name) ORDER BY c.relname"
                    ),
                    {"name": f'"{schema}".code_catalog'},
                )
            )
            .mappings()
            .all()
        )
        constraints = (
            (
                await connection.execute(
                    sa.text(
                        "SELECT oid,conname,contype,pg_get_constraintdef(oid) AS definition "
                        "FROM pg_constraint WHERE conrelid=to_regclass(:name) ORDER BY conname"
                    ),
                    {"name": f'"{schema}".code_catalog'},
                )
            )
            .mappings()
            .all()
        )
        columns = await _catalog_columns(connection, schema)
        catalog_rows = (
            (
                await connection.execute(
                    sa.text(f'SELECT to_jsonb(c) AS row FROM "{schema}".code_catalog c ORDER BY code_system,code')
                )
            )
            .scalars()
            .all()
        )
        return {
            "table": dict(table),
            "indexes": [dict(x) for x in indexes],
            "constraints": [dict(x) for x in constraints],
            "columns": [dict(x) for x in columns],
            "rows": catalog_rows,
        }


@pytest.mark.asyncio
async def test_real_legacy_catalog_keeps_values_storage_and_logical_keys(native_catalog):
    engine, schema = native_catalog
    await _invoke(engine, schema, LEGACY, legacy=True)
    async with engine.begin() as connection:
        await connection.execute(
            sa.text(
                f'INSERT INTO "{schema}".code_catalog '
                "(code_system,code,display_name,source,effective_year,is_active,updated_at) VALUES "
                "('EXAMPLE','001','Legacy catalog row','sample-source',2024,true,'2024-01-02 03:04:05.123456'),"
                "('UNCLASSIFIED','002',NULL,NULL,2023,NULL,NULL)"
            )
        )
    before = await _witness(engine, schema)
    await _invoke(engine, schema, TARGET)
    after = await _witness(engine, schema)
    assert after["table"] == before["table"]
    for old in before["indexes"]:
        current = next(x for x in after["indexes"] if x["definition"] == old["definition"])
        assert current["relfilenode"] == old["relfilenode"]
    assert [(x["conname"], x["contype"], x["definition"]) for x in after["constraints"]] == [
        (x["conname"], x["contype"], x["definition"]) for x in before["constraints"]
    ]
    metadata = {"code_type", "source_release", "source_attribution"}
    assert [
        {field_name: field_value for field_name, field_value in catalog_row.items() if field_name not in metadata}
        for catalog_row in after["rows"]
    ] == before["rows"]
    assert all(catalog_row[key] is None for catalog_row in after["rows"] for key in metadata)
    by_name = {x["column_name"]: x for x in after["columns"]}
    assert all(by_name[key]["column_default"] is None and by_name[key]["is_nullable"] == "YES" for key in metadata)
    assert by_name["code"]["character_maximum_length"] == by_name["source"]["character_maximum_length"] == 128
    # Native widening may replace index/constraint catalog OIDs while reusing
    # physical index storage. Readers retain the original table/heap identity.
    await _invoke(engine, schema, TARGET)
    assert await _witness(engine, schema) == after
    async with engine.begin() as connection:
        await connection.execute(
            sa.text(f'INSERT INTO "{schema}".code_catalog(code_system,code,source) VALUES(:system,:code,:source)'),
            {"system": "EXAMPLE", "code": "x" * 100, "source": "y" * 100},
        )


@pytest.mark.asyncio
async def test_current_catalog_replay_is_native_noop(native_catalog):
    engine, schema = native_catalog
    await _invoke(engine, schema, TARGET)
    async with engine.begin() as connection:
        await connection.exec_driver_sql(
            f'INSERT INTO "{schema}".code_catalog(code_system,code,code_type,source_release,source_attribution) '
            "VALUES('EXAMPLE','003','concept','sample-release','sample-attribution')"
        )
    before = await _witness(engine, schema)
    await _invoke(engine, schema, TARGET)
    assert await _witness(engine, schema) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", ["wrong_width", "metadata_type", "metadata_nullability", "metadata_default", "wrong_key"]
)
async def test_incompatible_catalog_refuses_without_partial_adoption(native_catalog, damage):
    engine, schema = native_catalog
    await _invoke(engine, schema, LEGACY, legacy=True)
    statement_by_damage = {
        "wrong_width": "ALTER COLUMN code TYPE varchar(16)",
        "metadata_type": "ADD COLUMN code_type integer",
        "metadata_nullability": "ADD COLUMN code_type varchar(32) NOT NULL",
        "metadata_default": "ADD COLUMN code_type varchar(32) DEFAULT 'guessed'",
        "wrong_key": "DROP CONSTRAINT code_catalog_pkey, ADD PRIMARY KEY(code)",
    }
    async with engine.begin() as connection:
        await connection.exec_driver_sql(f'ALTER TABLE "{schema}".code_catalog {statement_by_damage[damage]}')
        await connection.exec_driver_sql(
            f"INSERT INTO \"{schema}\".code_catalog(code_system,code,effective_year) VALUES('EXAMPLE','004',2024)"
        ) if damage != "metadata_nullability" else None
    before = await _witness(engine, schema)
    with pytest.raises(RuntimeError, match="existing_schema_"):
        await _invoke(engine, schema, TARGET)
    assert await _witness(engine, schema) == before
