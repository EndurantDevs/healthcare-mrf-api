# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable-PostgreSQL proof for hospital source-format migration."""

from __future__ import annotations

import uuid

import asyncpg
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from tests.test_hospital_price_storage import (
    ROOT,
    _database_url,
    _drop_schema,
    _load_migration,
    _prepare_schema,
    _quote,
    _run_migration,
    _seed_registry,
    _seed_version_header,
)


MIGRATION_PATH = (
    ROOT / "alembic/versions/20260827120000_hospital_price_source_format.py"
)


async def _formats(connection, quoted: str) -> dict[str, str]:
    rows = await connection.fetch(
        f"SELECT version_id, source_format FROM {quoted}.hospital_price_version "
        "ORDER BY version_id"
    )
    return {row["version_id"]: row["source_format"] for row in rows}


async def _constraint(connection, schema: str) -> tuple[bool, str]:
    row = await connection.fetchrow(
        "SELECT convalidated, pg_get_constraintdef(oid) AS definition "
        "FROM pg_constraint WHERE conrelid=$1::regclass "
        "AND conname='hospital_price_version_shape_check'",
        f"{schema}.hospital_price_version",
    )
    assert row is not None
    return row["convalidated"], row["definition"]


async def _reject_format(
    connection, quoted: str, version_id: str, source_format: str
) -> None:
    try:
        async with connection.transaction():
            await connection.execute(
                f"UPDATE {quoted}.hospital_price_version SET source_format=$1 "
                "WHERE version_id=$2",
                source_format,
                version_id,
            )
    except asyncpg.CheckViolationError:
        return
    raise AssertionError(f"source_format {source_format!r} was accepted")


async def _assert_state(
    connection,
    schema: str,
    expected: dict[str, str],
    *,
    allowed: tuple[str, str],
    rejected: tuple[str, str],
) -> None:
    quoted = _quote(schema)
    assert await _formats(connection, quoted) == expected
    validated, definition = await _constraint(connection, schema)
    assert validated is True
    for value in allowed:
        assert value in definition
    for value in rejected:
        assert value not in definition
        await _reject_format(connection, quoted, next(iter(expected)), value)


async def _seed_legacy_versions(
    database_url, schema: str, legacy_by_version: dict[str, str]
) -> None:
    connection = await asyncpg.connect(
        str(database_url.set(drivername="postgresql"))
    )
    try:
        quoted = _quote(schema)
        await _seed_registry(connection, quoted)
        for index, (version_id, source_format) in enumerate(
            legacy_by_version.items(), 1
        ):
            await _seed_version_header(
                connection, quoted, str(index) * 64, version_id
            )
            await connection.execute(
                f"UPDATE {quoted}.hospital_price_version SET source_format=$1 "
                "WHERE version_id=$2",
                source_format,
                version_id,
            )
        assert await _formats(connection, quoted) == legacy_by_version
    finally:
        await connection.close()


async def _assert_database_state(database_url, schema: str, *args, **kwargs) -> None:
    connection = await asyncpg.connect(
        str(database_url.set(drivername="postgresql"))
    )
    try:
        await _assert_state(connection, schema, *args, **kwargs)
    finally:
        await connection.close()


async def prove_source_format_forward_and_rollback(monkeypatch) -> None:
    """Prove legacy upgrade, canonical re-entry, and lossless rollback."""

    database_url = _database_url()
    schema = f"hospital_price_test_{uuid.uuid4().hex}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg"), poolclass=NullPool
    )
    base = _load_migration()
    repair = _load_migration(MIGRATION_PATH)
    ids = ("a" * 64, "b" * 64, "c" * 64)
    legacy_by_version = dict(zip(ids, ("json", "csv_tall", "csv_wide")))
    canonical_by_version = dict(zip(ids, ("json", "csv-tall", "csv-wide")))
    await _prepare_schema(engine, schema)
    try:
        await _run_migration(engine, base, "upgrade")
        await _seed_legacy_versions(database_url, schema, legacy_by_version)

        await _run_migration(engine, repair, "upgrade")
        await _assert_database_state(
            database_url,
            schema,
            canonical_by_version,
            allowed=("csv-tall", "csv-wide"),
            rejected=("csv_tall", "csv_wide"),
        )

        # Fresh installs may already have canonical rows and constraints.
        await _run_migration(engine, repair, "upgrade")
        await _run_migration(engine, repair, "downgrade")
        await _assert_database_state(
            database_url,
            schema,
            legacy_by_version,
            allowed=("csv_tall", "csv_wide"),
            rejected=("csv-tall", "csv-wide"),
        )
    finally:
        await _drop_schema(engine, schema)
        await engine.dispose()
