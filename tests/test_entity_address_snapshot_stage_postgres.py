# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
import os
from pathlib import Path
import re
import subprocess
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import MetaData


source = importlib.import_module("process.entity_address_snapshot_source")
_DSN_ENV = "HLTHPRT_ENTITY_ADDRESS_ARCHIVE_TEST_DSN"
_DUMP_ENV = "HLTHPRT_ENTITY_ADDRESS_ARCHIVE_TEST_PG_DUMP"
_RESTORE_ENV = "HLTHPRT_ENTITY_ADDRESS_ARCHIVE_TEST_PG_RESTORE"
_LOCAL_DATABASE_PATTERN = re.compile(r"^hc_entity_address_stage_[0-9a-f]{32}$")
_CI_DATABASE = "ptg2_v3_lifecycle_test_ci_runner"
_LOCAL_HOSTS = frozenset({"127.0.0.1", "localhost"})
_CI_HOSTS = _LOCAL_HOSTS | {"postgres"}


def _is_owned_native_test_database(url) -> bool:
    """Accept only the dedicated CI database or a UUID-scoped local database."""

    database_name = str(url.database or "")
    host = str(url.host or "")
    port = url.port
    if not url.drivername.startswith("postgresql") or not url.username:
        return False
    if port == 5440:
        return host in _LOCAL_HOSTS and bool(_LOCAL_DATABASE_PATTERN.fullmatch(database_name))
    return host in _CI_HOSTS and port in (None, 5432) and database_name == _CI_DATABASE


def _native_test_connection() -> tuple[str, dict[str, str]]:
    raw_dsn = os.environ.get(_DSN_ENV, "")
    if not raw_dsn:
        pytest.skip(f"{_DSN_ENV} is not set")
    url = make_url(raw_dsn)
    if not _is_owned_native_test_database(url):
        pytest.fail(f"{_DSN_ENV} must identify the dedicated native archive test database")
    environment = os.environ.copy()
    environment.update(
        PGHOST=url.host,
        PGPORT=str(url.port),
        PGUSER=url.username,
        PGDATABASE=url.database,
    )
    if url.password is not None:
        environment["PGPASSWORD"] = url.password
    return url.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False), environment


def _native_tool(name: str) -> str:
    tool = os.environ.get(name, "")
    if not tool:
        pytest.skip(f"{name} is not set")
    return tool


async def _create_model_family(connection, schema_name: str) -> None:
    await connection.execute(text(f'CREATE SCHEMA "{schema_name}"'))
    metadata = MetaData(schema=schema_name)
    for model in (
        source.entity_address_unified.EntityAddressUnified,
        *source.entity_address_unified.SUPPORT_TABLE_MODELS,
    ):
        model.__table__.to_metadata(metadata, schema=schema_name)
    await connection.run_sync(metadata.create_all)


async def _seed_live_sentinel(engine, live_schema: str, relations) -> None:
    """Create the live model family with one value that must survive staging."""

    async with engine.begin() as connection:
        await _create_model_family(connection, live_schema)
        await connection.execute(
            text(
                f'INSERT INTO "{live_schema}"."{relations[0].table_name}" '
                "(entity_type, entity_id, location_key, checksum, type) "
                "VALUES ('synthetic', 'owned', 'live-sentinel', 1, 'primary')"
            )
        )


def _dump_stage_capture(capture, *, dataset_id, stage_schema: str, dump_path: Path, pg_dump: str, environment) -> None:
    """Write the clone-only native archive covered by the stage snapshot pin."""

    assert capture.dataset_id == dataset_id
    assert capture.schema_name == stage_schema
    dump = subprocess.run(
        [
            pg_dump,
            "--format=custom",
            "--file",
            str(dump_path),
            f"--snapshot={capture.postgres_snapshot}",
            *[f"--table={stage_schema}.{relation.table_name}" for relation in capture.relations],
        ],
        capture_output=True,
        check=False,
        env=environment,
        text=True,
        timeout=30,
    )
    assert dump.returncode == 0, dump.stderr


async def _rename_live_sentinel(sessions, live_schema: str, table_name: str) -> None:
    """Prove that copying the owned stage leaves the live relation writable."""

    renamed_table = "renamed_while_stage_is_pinned"
    async with sessions() as contender, contender.begin():
        await contender.execute(text(f'ALTER TABLE "{live_schema}"."{table_name}" RENAME TO "{renamed_table}"'))
        await contender.execute(text(f'ALTER TABLE "{live_schema}"."{renamed_table}" RENAME TO "{table_name}"'))


async def _assert_live_sentinel(connection, schema_name: str, table_name: str) -> None:
    """Require that a relation still carries the pre-stage sentinel value."""

    assert await connection.scalar(text(f'SELECT location_key FROM "{schema_name}"."{table_name}"')) == "live-sentinel"


@pytest.mark.parametrize(
    ("dsn", "expected"),
    [
        ("postgresql://postgres@127.0.0.1:5440/hc_entity_address_stage_0123456789abcdef0123456789abcdef", True),
        ("postgresql://postgres@localhost:5432/ptg2_v3_lifecycle_test_ci_runner", True),
        ("postgresql://postgres@postgres:5432/ptg2_v3_lifecycle_test_ci_runner", True),
        ("postgresql://postgres@127.0.0.1:5440/ptg2_v3_lifecycle_test_ci_runner", False),
        ("postgresql://postgres@127.0.0.1:5432/another_database", False),
        ("postgresql://postgres@database:5432/ptg2_v3_lifecycle_test_ci_runner", False),
    ],
)
def test_native_test_database_guard(dsn: str, expected: bool) -> None:
    assert _is_owned_native_test_database(make_url(dsn)) is expected


def test_native_test_connection_preserves_an_authenticated_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    fixture_secret = "synthetic-dsn-secret"
    monkeypatch.setenv(
        _DSN_ENV,
        f"postgresql://postgres:{fixture_secret}@postgres:5432/{_CI_DATABASE}",
    )

    async_dsn, environment = _native_test_connection()

    assert fixture_secret in async_dsn
    assert "***" not in async_dsn
    assert environment["PGPASSWORD"] == fixture_secret


@pytest.mark.asyncio
async def test_native_stage_archive_preserves_live_sentinel(tmp_path: Path):
    """The staged archive restores the owned copy without changing the live family."""

    async_dsn, tool_environment = _native_test_connection()
    pg_dump = _native_tool(_DUMP_ENV)
    pg_restore = _native_tool(_RESTORE_ENV)
    engine = create_async_engine(async_dsn)
    live_schema = "address_archive_live_" + uuid4().hex
    dataset_id = uuid4()
    stage_schema = source.entity_address_archive_stage_schema(dataset_id)
    relations = source.entity_address_archive_relations()
    dump_path = tmp_path / "entity-address-stage.dump"
    try:
        await _seed_live_sentinel(engine, live_schema, relations)
        sessions = async_sessionmaker(engine, expire_on_commit=False)

        async def archive_copy(capture):
            """Dump the stage and prove it no longer locks live relations."""

            _dump_stage_capture(
                capture,
                dataset_id=dataset_id,
                stage_schema=stage_schema,
                dump_path=dump_path,
                pg_dump=pg_dump,
                environment=tool_environment,
            )
            await _rename_live_sentinel(sessions, live_schema, relations[0].table_name)

        manifest = await source.export_entity_address_archive_stage(
            sessions,
            schema_name=live_schema,
            dataset_id=dataset_id,
            archive_copy=archive_copy,
        )
        assert manifest.schema_name == stage_schema
        assert manifest.relations == relations
        async with engine.begin() as connection:
            await _assert_live_sentinel(connection, live_schema, relations[0].table_name)
            await connection.execute(text(f'DROP SCHEMA "{stage_schema}" CASCADE'))
            await connection.execute(text(f'CREATE SCHEMA "{stage_schema}"'))
        restore = subprocess.run(
            [pg_restore, "--no-owner", "--exit-on-error", "--dbname", tool_environment["PGDATABASE"], str(dump_path)],
            capture_output=True,
            check=False,
            env=tool_environment,
            text=True,
            timeout=30,
        )
        assert restore.returncode == 0, restore.stderr
        async with engine.connect() as connection:
            await _assert_live_sentinel(connection, stage_schema, relations[0].table_name)
            await _assert_live_sentinel(connection, live_schema, relations[0].table_name)
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{stage_schema}" CASCADE'))
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{live_schema}" CASCADE'))
        await engine.dispose()
