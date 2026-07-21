# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared disposable-PostgreSQL fixtures for retained UHC admission tests."""

import hashlib
import importlib.util
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
import re
from typing import AsyncIterator

from alembic.migration import MigrationContext
from alembic.operations import Operations
import asyncpg
import pytest
import sqlalchemy as sa
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

import process.uhc_retained_source_registry as source_registry
from process.uhc_retained_native import retain_source_native
from process.uhc_retained_registry_contract import SourceBinding


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / "20260720130000_uhc_retained_artifact_admission.py"
)
OPT_IN_DSN_ENV = "HLTHPRT_UHC_RETAINED_ADMISSION_POSTGRES_DSN"
DISPOSABLE_DATABASE_PATTERN = re.compile(
    r"^uhc_retained_admission_test_[a-z0-9][a-z0-9_]{7,}$"
)
FRESH_SCHEMA = "mrf_uhc_admission_fresh"
ADOPTION_SCHEMA = "mrf_uhc_admission_adoption"
EXPECTED_ADMISSION_TABLES = {
    "provider_directory_uhc_raw_artifact",
    "provider_directory_uhc_raw_layout",
    "provider_directory_uhc_source_binding",
    "provider_directory_uhc_raw_range",
    "provider_directory_uhc_artifact_reference",
}


def _database_url():
    dsn = os.getenv(OPT_IN_DSN_ENV)
    if not dsn:
        pytest.skip(f"set {OPT_IN_DSN_ENV} to run PostgreSQL admission proofs")
    database_url = make_url(dsn)
    if not database_url.drivername.startswith("postgresql"):
        pytest.fail(f"{OPT_IN_DSN_ENV} must use PostgreSQL")
    database_name = str(database_url.database or "")
    if not DISPOSABLE_DATABASE_PATTERN.fullmatch(database_name):
        pytest.fail(f"refusing non-disposable PostgreSQL database {database_name!r}")
    if not database_url.host or not database_url.username:
        pytest.fail(f"{OPT_IN_DSN_ENV} must include an explicit host and user")
    return database_url


def _native_binary() -> Path:
    configured = os.getenv("HLTHPRT_PTG2_RUST_SCANNER_BIN")
    binary = (
        Path(configured)
        if configured
        else ROOT / "support" / "ptg2_scanner" / "target" / "debug" / "ptg2_scanner"
    )
    if not binary.is_file():
        pytest.fail(f"native UHC scanner is not built: {binary}")
    return binary.resolve()


def _load_migration():
    spec = importlib.util.spec_from_file_location(
        "uhc_retained_artifact_admission_postgres_migration",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def _reset_schema(engine, schema: str) -> None:
    async with engine.begin() as connection:
        await connection.exec_driver_sql(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')


async def _drop_schema(engine, schema: str) -> None:
    async with engine.begin() as connection:
        await connection.exec_driver_sql(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


async def _install_catalog_contract(engine, schema: str) -> None:
    statements = (
        f'''CREATE TABLE "{schema}"."provider_directory_uhc_catalog_set" (
                catalog_set_sha256 varchar(64) PRIMARY KEY
            )''',
        f'''CREATE TABLE "{schema}"."provider_directory_uhc_catalog_file" (
                catalog_set_sha256 varchar(64) NOT NULL,
                file_id varchar(64) NOT NULL,
                family varchar(8) NOT NULL,
                collection_kind varchar(32) NOT NULL,
                file_name varchar(256) NOT NULL,
                source_url text NOT NULL,
                catalog_modified_at varchar(64) NOT NULL,
                catalog_entry_sha256 varchar(64) NOT NULL,
                size_bytes bigint,
                availability varchar(32) NOT NULL,
                catalog_support varchar(32) NOT NULL,
                PRIMARY KEY (catalog_set_sha256, file_id),
                FOREIGN KEY (catalog_set_sha256)
                    REFERENCES "{schema}"."provider_directory_uhc_catalog_set"
                    (catalog_set_sha256)
            )''',
    )
    async with engine.begin() as connection:
        for statement in statements:
            await connection.exec_driver_sql(statement)


def _upgrade_on_connection(sync_connection, migration) -> None:
    migration_context = MigrationContext.configure(sync_connection)
    migration.op = Operations(migration_context)
    migration.upgrade()


async def _upgrade_admission(engine, migration) -> None:
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: _upgrade_on_connection(
                sync_connection,
                migration,
            )
        )


async def _admission_table_oids(engine, schema: str) -> dict[str, int]:
    query = sa.text(
        """SELECT table_record.relname, table_record.oid
             FROM pg_class AS table_record
             JOIN pg_namespace AS namespace_record
               ON namespace_record.oid=table_record.relnamespace
            WHERE namespace_record.nspname=:schema
              AND table_record.relkind='r'
              AND table_record.relname LIKE 'provider_directory_uhc_%'"""
    )
    async with engine.connect() as connection:
        rows = (await connection.execute(query, {"schema": schema})).all()
    return {
        str(table_name): int(table_oid)
        for table_name, table_oid in rows
        if str(table_name) in EXPECTED_ADMISSION_TABLES
    }


async def _asyncpg_connection(database_url):
    return await asyncpg.connect(
        host=str(database_url.host),
        port=int(database_url.port or 5432),
        user=str(database_url.username),
        password=str(database_url.password or ""),
        database=str(database_url.database),
    )


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _write_source(tmp_path: Path, label: str, count: int = 20) -> tuple[Path, str, int]:
    records = [
        {"ordinal": ordinal, "label": label, "padding": "x" * 200}
        for ordinal in range(count)
    ]
    encoded = json.dumps(records, separators=(",", ":")).encode("ascii")
    source_path = tmp_path / f"{label}.json"
    source_path.write_bytes(encoded)
    return source_path, hashlib.sha256(encoded).hexdigest(), len(encoded)


async def _native_source(
    source_path: Path,
    output_root: Path,
    artifact_sha256: str,
    artifact_byte_count: int,
    range_count: int,
):
    return await retain_source_native(
        source_path=source_path,
        output_root=output_root,
        expected_sha256=artifact_sha256,
        expected_byte_count=artifact_byte_count,
        range_count=range_count,
    )


def _binding(
    artifact_sha256: str,
    byte_count: int,
    *,
    catalog_label: str,
    collection_kind: str = "provider_membership",
) -> SourceBinding:
    file_name = (
        "JSON_PLANS_WY.json"
        if collection_kind == "plan_reference"
        else "JSON_Providers_AZDC.json"
    )
    category = "plans" if collection_kind == "plan_reference" else "providers"
    source_url = (
        f"https://providermrf.uhc.com/api/stream/ui/ifp/{category}/{file_name}"
    )
    catalog_modified_at = "2026-07-20T08:00:00Z"
    catalog_entry, source_file_id = source_registry._expected_catalog_file_hash_pair(
        family="ifp",
        collection_kind=collection_kind,
        file_name=file_name,
        source_url=source_url,
        catalog_modified_at=catalog_modified_at,
        size_bytes=byte_count,
    )
    return SourceBinding(
        catalog_set_sha256=_digest(catalog_label),
        source_file_id=source_file_id,
        family="ifp",
        collection_kind=collection_kind,
        file_name=file_name,
        source_url=source_url,
        catalog_modified_at=catalog_modified_at,
        size_bytes=byte_count,
        catalog_entry_sha256=catalog_entry,
        artifact_sha256=artifact_sha256,
    )


async def _insert_catalog_file(connection, schema: str, binding: SourceBinding) -> None:
    await connection.execute(
        f'''INSERT INTO "{schema}"."provider_directory_uhc_catalog_set"
                (catalog_set_sha256) VALUES ($1)''',
        binding.catalog_set_sha256,
    )
    await connection.execute(
        f'''INSERT INTO "{schema}"."provider_directory_uhc_catalog_file" (
                catalog_set_sha256, file_id, family, collection_kind,
                file_name, source_url, catalog_modified_at,
                catalog_entry_sha256, size_bytes,
                availability, catalog_support
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9,
                      'published', 'cataloged')''',
        binding.catalog_set_sha256,
        binding.source_file_id,
        binding.family,
        binding.collection_kind,
        binding.file_name,
        binding.source_url,
        binding.catalog_modified_at,
        binding.catalog_entry_sha256,
        binding.size_bytes,
    )


async def _proof_counts(connection, schema: str) -> dict[str, int]:
    row = await connection.fetchrow(
        f'''SELECT
            (SELECT count(*) FROM "{schema}".
                provider_directory_uhc_raw_artifact) AS raw_count,
            (SELECT count(*) FROM "{schema}".
                provider_directory_uhc_raw_layout) AS layout_count,
            (SELECT count(*) FROM "{schema}".
                provider_directory_uhc_source_binding) AS binding_count,
            (SELECT count(*) FROM "{schema}".
                provider_directory_uhc_raw_range) AS range_count,
            (SELECT count(*) FROM "{schema}".
                provider_directory_uhc_artifact_reference) AS reference_count'''
    )
    assert row is not None
    return dict(row)


@asynccontextmanager
async def admission_database(
    tmp_path: Path,
    monkeypatch,
) -> AsyncIterator[tuple[asyncpg.Connection, object]]:
    """Yield one migrated disposable schema and close every owned resource."""

    database_url = _database_url()
    engine = create_async_engine(database_url.set(drivername="postgresql+asyncpg"))
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", FRESH_SCHEMA)
    monkeypatch.setenv("HLTHPRT_PTG2_RUST_SCANNER_BIN", str(_native_binary()))
    monkeypatch.setattr(
        source_registry,
        "uhc_retained_artifact_root",
        lambda: tmp_path,
    )
    connection = None
    try:
        await _reset_schema(engine, FRESH_SCHEMA)
        await _install_catalog_contract(engine, FRESH_SCHEMA)
        await _upgrade_admission(engine, _load_migration())
        connection = await _asyncpg_connection(database_url)
        yield connection, database_url
    finally:
        if connection is not None:
            await connection.close()
        await _drop_schema(engine, FRESH_SCHEMA)
        await engine.dispose()
