# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import os
import re
from datetime import datetime
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import MetaData

from db.models import CodeCatalog
from process.code_catalog_snapshot import (
    CodeCatalogSnapshotError,
    capture_code_catalog_result,
    promote_code_catalog_restored_stage,
    validate_code_catalog_restored_stage,
)

_DSN_ENV = "HLTHPRT_CODE_CATALOG_ARCHIVE_TEST_DSN"
_LOCAL_DATABASE_PATTERN = re.compile(r"^hc_code_catalog_archive_[0-9a-f]{32}$")
_CI_DATABASE = "ptg2_v3_lifecycle_test_ci_runner"
_LOCAL_HOSTS = frozenset({"127.0.0.1", "localhost"})
_CI_HOSTS = _LOCAL_HOSTS | {"postgres"}


def _is_owned_native_test_database(url) -> bool:
    """Accept only a UUID-owned local database or the dedicated CI database."""
    database_name = str(url.database or "")
    host = str(url.host or "")
    if not url.drivername.startswith("postgresql") or url.username != "postgres":
        return False
    if url.port == 5440:
        return host in _LOCAL_HOSTS and _LOCAL_DATABASE_PATTERN.fullmatch(database_name) is not None
    return host in _CI_HOSTS and url.port in (None, 5432) and database_name == _CI_DATABASE


def _dsn() -> str:
    raw_dsn = os.environ.get(_DSN_ENV, "")
    if not raw_dsn:
        pytest.skip(f"{_DSN_ENV} is not set")
    url = make_url(raw_dsn)
    if not _is_owned_native_test_database(url):
        pytest.fail(f"{_DSN_ENV} must identify a dedicated native archive test database")
    return url.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)


@pytest.mark.parametrize(
    ("dsn", "expected"),
    [
        ("postgresql://postgres@127.0.0.1:5440/hc_code_catalog_archive_0123456789abcdef0123456789abcdef", True),
        ("postgresql://postgres@localhost:5432/ptg2_v3_lifecycle_test_ci_runner", True),
        ("postgresql://postgres@postgres:5432/ptg2_v3_lifecycle_test_ci_runner", True),
        ("postgresql://postgres@127.0.0.1:5440/ptg2_v3_lifecycle_test_ci_runner", False),
        ("postgresql://postgres@localhost:5432/another_database", False),
    ],
)
def test_native_test_database_guard(dsn: str, expected: bool) -> None:
    assert _is_owned_native_test_database(make_url(dsn)) is expected


async def _create_catalog(connection, schema_name: str, table_name: str) -> None:
    metadata = MetaData(schema=schema_name)
    CodeCatalog.__table__.to_metadata(metadata, schema=schema_name, name=table_name)
    await connection.run_sync(metadata.create_all)


async def _insert_rows(connection, schema_name: str, table_name: str, rows) -> None:
    await connection.execute(
        text(
            f'INSERT INTO "{schema_name}"."{table_name}" '
            "(code_system, code, code_type, display_name, short_description, long_description, is_active, source, source_release, source_attribution, updated_at) "
            "VALUES (:code_system, :code, :code_type, :display_name, :short_description, :long_description, :is_active, :source, :source_release, :source_attribution, :updated_at)"
        ),
        rows,
    )


def _rows(*, suffix: str = ""):
    return [
        {
            "code_system": "POS",
            "code": f"21{suffix}",
            "code_type": "place",
            "display_name": "Synthetic inpatient",
            "short_description": "Inpatient",
            "long_description": "Synthetic description",
            "is_active": True,
            "source": "synthetic-pos",
            "source_release": "v1",
            "source_attribution": "synthetic",
            "updated_at": datetime(2026, 9, 13),
        },
        {
            "code_system": "RC",
            "code": f"0450{suffix}",
            "code_type": "revenue",
            "display_name": "Synthetic emergency",
            "short_description": None,
            "long_description": None,
            "is_active": True,
            "source": "synthetic-revenue",
            "source_release": None,
            "source_attribution": None,
            "updated_at": datetime(2026, 9, 13),
        },
        {
            "code_system": "MODIFIER",
            "code": f"25{suffix}",
            "code_type": "modifier",
            "display_name": "Synthetic distinct service",
            "short_description": "Distinct",
            "long_description": "Synthetic modifier",
            "is_active": False,
            "source": "synthetic-modifier",
            "source_release": "v2",
            "source_attribution": "synthetic",
            "updated_at": datetime(2026, 9, 13),
        },
    ]


async def _capture(sessions, schema_name: str):
    async with sessions() as session, session.begin():
        return await capture_code_catalog_result(session, schema_name=schema_name)


@pytest.mark.asyncio
async def test_native_catalog_capture_stage_validation_and_retained_atomic_promotion():
    engine = create_async_engine(_dsn())
    schema_name = "code_catalog_archive_" + uuid4().hex
    source_schema_name = "code_catalog_source_" + uuid4().hex
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema_name}"'))
            await connection.execute(text(f'CREATE SCHEMA "{source_schema_name}"'))
            await _create_catalog(connection, schema_name, "code_catalog")
            await _create_catalog(connection, schema_name, "code_catalog_stage")
            await _create_catalog(connection, source_schema_name, "code_catalog")
            await _insert_rows(connection, schema_name, "code_catalog", _rows(suffix="_old"))
            await _insert_rows(connection, schema_name, "code_catalog_stage", _rows())
            await _insert_rows(connection, source_schema_name, "code_catalog", _rows())
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        incumbent = await _capture(sessions, schema_name)
        source_capture = await _capture(sessions, source_schema_name)
        async with sessions() as session, session.begin():
            stage = await validate_code_catalog_restored_stage(
                session,
                schema_name=schema_name,
                stage_table_name="code_catalog_stage",
                expected_receipt=source_capture.receipt,
            )
        async with sessions() as session, session.begin():
            idle_calls = []

            async def require_import_idle(idle_session):
                assert idle_session is session
                idle_calls.append(True)

            promoted = await promote_code_catalog_restored_stage(
                session,
                schema_name=schema_name,
                stage_table_name="code_catalog_stage",
                retained_table_name="code_catalog_previous",
                incumbent_capture=incumbent,
                require_import_idle=require_import_idle,
            )
            assert promoted == stage
            assert idle_calls == [True]
        async with engine.connect() as connection:
            assert await connection.scalar(text(f'SELECT count(*) FROM "{schema_name}".code_catalog')) == 3
            assert await connection.scalar(text(f'SELECT count(*) FROM "{schema_name}".code_catalog_previous')) == 3
            assert (
                await connection.scalar(
                    text(f"SELECT code FROM \"{schema_name}\".code_catalog WHERE code_system='POS'")
                )
                == "21"
            )
            assert (
                await connection.scalar(
                    text(f"SELECT code FROM \"{schema_name}\".code_catalog_previous WHERE code_system='POS'")
                )
                == "21_old"
            )
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{source_schema_name}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
async def test_native_catalog_promotion_refuses_foreign_key_dependents_without_mutation():
    engine = create_async_engine(_dsn())
    schema_name = "code_catalog_archive_" + uuid4().hex
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema_name}"'))
            await _create_catalog(connection, schema_name, "code_catalog")
            await _create_catalog(connection, schema_name, "code_catalog_stage")
            await _insert_rows(connection, schema_name, "code_catalog", _rows(suffix="_old"))
            await _insert_rows(connection, schema_name, "code_catalog_stage", _rows())
            await connection.execute(
                text(
                    f'CREATE TABLE "{schema_name}".catalog_reference '
                    "(code_system varchar(32), code varchar(128), "
                    "FOREIGN KEY (code_system, code) REFERENCES "
                    f'"{schema_name}".code_catalog (code_system, code))'
                )
            )
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        incumbent = await _capture(sessions, schema_name)
        async with sessions() as session, session.begin():
            with pytest.raises(CodeCatalogSnapshotError, match="foreign-key dependents"):
                await promote_code_catalog_restored_stage(
                    session,
                    schema_name=schema_name,
                    stage_table_name="code_catalog_stage",
                    retained_table_name="code_catalog_previous",
                    incumbent_capture=incumbent,
                    require_import_idle=_idle,
                )
        async with engine.connect() as connection:
            assert await connection.scalar(text(f"SELECT to_regclass('{schema_name}.code_catalog_previous')")) is None
            assert await connection.scalar(text(f'SELECT count(*) FROM "{schema_name}".code_catalog')) == 3
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
async def test_native_catalog_promotion_refuses_dependent_views_without_mutation():
    engine = create_async_engine(_dsn())
    schema_name = "code_catalog_archive_" + uuid4().hex
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema_name}"'))
            await _create_catalog(connection, schema_name, "code_catalog")
            await _create_catalog(connection, schema_name, "code_catalog_stage")
            await _insert_rows(connection, schema_name, "code_catalog", _rows(suffix="_old"))
            await _insert_rows(connection, schema_name, "code_catalog_stage", _rows())
            await connection.execute(
                text(
                    f'CREATE VIEW "{schema_name}".catalog_view AS '
                    f'SELECT code_system, code FROM "{schema_name}".code_catalog'
                )
            )
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        incumbent = await _capture(sessions, schema_name)
        async with sessions() as session, session.begin():
            with pytest.raises(CodeCatalogSnapshotError, match="dependent views"):
                await promote_code_catalog_restored_stage(
                    session,
                    schema_name=schema_name,
                    stage_table_name="code_catalog_stage",
                    retained_table_name="code_catalog_previous",
                    incumbent_capture=incumbent,
                    require_import_idle=_idle,
                )
        async with engine.connect() as connection:
            assert await connection.scalar(text(f"SELECT to_regclass('{schema_name}.code_catalog_previous')")) is None
            assert await connection.scalar(text(f'SELECT count(*) FROM "{schema_name}".catalog_view')) == 3
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
async def test_native_catalog_promotion_requires_the_captured_incumbent_content():
    engine = create_async_engine(_dsn())
    schema_name = "code_catalog_archive_" + uuid4().hex
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema_name}"'))
            await _create_catalog(connection, schema_name, "code_catalog")
            await _create_catalog(connection, schema_name, "code_catalog_stage")
            await _insert_rows(connection, schema_name, "code_catalog", _rows(suffix="_old"))
            await _insert_rows(connection, schema_name, "code_catalog_stage", _rows())
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        incumbent = await _capture(sessions, schema_name)
        async with engine.begin() as connection:
            await connection.execute(
                text(f"UPDATE \"{schema_name}\".code_catalog SET display_name='changed' WHERE code_system='POS'")
            )
        async with sessions() as session, session.begin():
            with pytest.raises(CodeCatalogSnapshotError, match="incumbent changed"):
                await promote_code_catalog_restored_stage(
                    session,
                    schema_name=schema_name,
                    stage_table_name="code_catalog_stage",
                    retained_table_name="code_catalog_previous",
                    incumbent_capture=incumbent,
                    require_import_idle=_idle,
                )
        async with engine.connect() as connection:
            assert await connection.scalar(text(f"SELECT to_regclass('{schema_name}.code_catalog_previous')")) is None
            assert (
                await connection.scalar(
                    text(f"SELECT display_name FROM \"{schema_name}\".code_catalog WHERE code_system='POS'")
                )
                == "changed"
            )
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        await engine.dispose()


async def _idle(session) -> None:
    return None
