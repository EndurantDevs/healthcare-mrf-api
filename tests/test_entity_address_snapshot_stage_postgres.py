# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import importlib
import os
import re
import subprocess
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import MetaData

source = importlib.import_module("process.entity_address_snapshot_source")
receipt = importlib.import_module("process.entity_address_snapshot_receipt")
ownership = importlib.import_module("process.entity_address_snapshot_ownership")
restore = importlib.import_module("process.entity_address_snapshot_restore")
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


async def _create_alias_validation_relations(connection, schema_name: str) -> None:
    """Provide the actual empty alias relations required by native stage validation."""

    await connection.execute(
        text(
            f'CREATE TABLE "{schema_name}"."address_alias_state_v1" '
            "(singleton boolean PRIMARY KEY, schema_version smallint NOT NULL, "
            "active_ruleset_version smallint NOT NULL, generation bigint NOT NULL)"
        )
    )
    await connection.execute(
        text(
            f'INSERT INTO "{schema_name}"."address_alias_state_v1" '
            "(singleton, schema_version, active_ruleset_version, generation) VALUES (true, 2, 1, 0)"
        )
    )
    await connection.execute(
        text(f'CREATE TABLE "{schema_name}"."address_alias_v1" (source_address_key uuid, revoked_at timestamptz)')
    )


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


async def _seed_receipt_family(connection, schema_name: str, *, reversed_rows: bool, additional_rows: int = 0) -> None:
    rows = [("first", 1), ("second", 2)] + [(f"chunk-{ordinal:05d}", ordinal + 3) for ordinal in range(additional_rows)]
    if reversed_rows:
        rows.reverse()
    await connection.execute(
        text(
            f'INSERT INTO "{schema_name}"."entity_address_unified" '
            "(entity_type, entity_id, location_key, checksum, type, base_address_version) "
            "VALUES (:entity_type, :entity_id, :location_key, :checksum, 'primary', :base_address_version)"
        ),
        [
            {
                "entity_type": "synthetic",
                "entity_id": location_key,
                "location_key": location_key,
                "checksum": checksum,
                "base_address_version": source.entity_address_unified.ALIAS_BASE_ADDRESS_VERSION_PREFIX + "0",
            }
            for location_key, checksum in rows
        ],
    )
    await connection.execute(
        text(
            f'INSERT INTO "{schema_name}"."entity_address_evidence" '
            "(evidence_id, location_key, entity_type, entity_id, source_id, source_run_id, observed_at) "
            "VALUES (1, 'first', 'synthetic', 'first', 1, 'synthetic-run', TIMESTAMPTZ '2026-01-02 03:04:05+00')"
        )
    )


async def _capture_receipt(sessions, schema_name: str, timezone: str):
    async with sessions() as session:
        await session.execute(
            text("SELECT pg_catalog.set_config('TimeZone', :timezone, false)"), {"timezone": timezone}
        )
        await session.commit()
        async with session.begin():
            return await receipt.capture_entity_address_archive_receipt(session, schema_name=schema_name)


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


async def _assert_stage_share_pins_block_ddl(sessions, schema_name: str, table_name: str) -> None:
    """Require a rehydrated caller transaction to retain its exact stage lock."""

    async with sessions() as contender, contender.begin():
        await contender.execute(text("SET LOCAL lock_timeout = '100ms'"))
        with pytest.raises(DBAPIError) as error:
            await contender.execute(text(f'ALTER TABLE "{schema_name}"."{table_name}" RENAME TO "blocked_rename"'))
        assert getattr(error.value.orig, "sqlstate", None) == "55P03"


def _restore_archive(dump_path: Path, pg_restore: str, environment) -> None:
    """Restore the previously captured archive with the same native database tool."""

    result = subprocess.run(
        [pg_restore, "--no-owner", "--exit-on-error", "--dbname", environment["PGDATABASE"], str(dump_path)],
        capture_output=True,
        check=False,
        env=environment,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr


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
        async with sessions() as session, session.begin():
            await _seed_receipt_family(session, live_schema, reversed_rows=False)

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

        manifest, source_receipt = await source.export_entity_address_archive_with_receipt(
            sessions,
            schema_name=live_schema,
            dataset_id=dataset_id,
            archive_copy=archive_copy,
        )
        assert manifest.schema_name == stage_schema
        assert manifest.relations == relations
        async with engine.begin() as connection:
            await _assert_live_sentinel(connection, live_schema, relations[0].table_name)
            assert (
                await connection.scalar(
                    text("SELECT oid FROM pg_namespace WHERE nspname=:schema_name"), {"schema_name": stage_schema}
                )
                is None
            )
            await connection.execute(text(f'CREATE SCHEMA "{stage_schema}"'))
        _restore_archive(dump_path, pg_restore, tool_environment)
        restored_receipt = await _capture_receipt(sessions, stage_schema, "UTC")
        assert restored_receipt.as_dict() == source_receipt.as_dict()
        async with engine.connect() as connection:
            await _assert_live_sentinel(connection, stage_schema, relations[0].table_name)
            await _assert_live_sentinel(connection, live_schema, relations[0].table_name)
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{stage_schema}" CASCADE'))
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{live_schema}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
async def test_native_stage_receipt_is_portable_and_detects_drift():
    """The fixed seven-table receipt catches row and schema changes after restore."""

    async_dsn, _ = _native_test_connection()
    engine = create_async_engine(async_dsn, max_overflow=0, pool_size=1)
    source_schema = "address_receipt_source_" + uuid4().hex
    restored_schema = "address_receipt_restored_" + uuid4().hex
    try:
        async with engine.begin() as connection:
            await _create_model_family(connection, source_schema)
            await _create_model_family(connection, restored_schema)
            await _seed_receipt_family(connection, source_schema, reversed_rows=False)
            await _seed_receipt_family(connection, restored_schema, reversed_rows=True)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        source_receipt = await _capture_receipt(sessions, source_schema, "America/Los_Angeles")
        restored_receipt = await _capture_receipt(sessions, restored_schema, "Asia/Tokyo")
        assert source_receipt.as_dict() == restored_receipt.as_dict()

        async with sessions() as session, session.begin():
            await session.execute(
                text(f'UPDATE "{restored_schema}"."entity_address_unified" SET checksum=3 WHERE location_key=\'first\'')
            )
        assert (
            await _capture_receipt(sessions, restored_schema, "UTC")
        ).content_sha256 != source_receipt.content_sha256

        async with sessions() as session, session.begin():
            await session.execute(
                text(f'UPDATE "{restored_schema}"."entity_address_unified" SET checksum=1 WHERE location_key=\'first\'')
            )
        assert (await _capture_receipt(sessions, restored_schema, "UTC")).as_dict() == source_receipt.as_dict()

        async with sessions() as session, session.begin():
            await session.execute(
                text(
                    f'INSERT INTO "{restored_schema}"."entity_address_unified" '
                    "(entity_type, entity_id, location_key, checksum, type) "
                    "VALUES ('synthetic', 'third', 'third', 3, 'primary')"
                )
            )
        assert (
            await _capture_receipt(sessions, restored_schema, "UTC")
        ).content_sha256 != source_receipt.content_sha256

        async with sessions() as session, session.begin():
            await session.execute(
                text(f'ALTER TABLE "{restored_schema}"."entity_address_unified" ADD COLUMN archive_tamper text')
            )
        assert (await _capture_receipt(sessions, restored_schema, "UTC")).schema_sha256 != source_receipt.schema_sha256
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{source_schema}" CASCADE'))
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{restored_schema}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
async def test_native_stage_receipt_preserves_order_independence_across_chunk_boundary():
    """Rows on both sides of the 4096-row digest boundary remain canonical."""

    async_dsn, _ = _native_test_connection()
    engine = create_async_engine(async_dsn, max_overflow=0, pool_size=1)
    source_schema = "address_receipt_chunks_" + uuid4().hex
    restored_schema = "address_receipt_chunks_" + uuid4().hex
    try:
        async with engine.begin() as connection:
            await _create_model_family(connection, source_schema)
            await _create_model_family(connection, restored_schema)
            await _seed_receipt_family(connection, source_schema, reversed_rows=False, additional_rows=4097)
            await _seed_receipt_family(connection, restored_schema, reversed_rows=True, additional_rows=4097)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        source_receipt = await _capture_receipt(sessions, source_schema, "America/Los_Angeles")
        restored_receipt = await _capture_receipt(sessions, restored_schema, "Asia/Tokyo")

        assert source_receipt.as_dict() == restored_receipt.as_dict()
        assert source_receipt.tables[0].row_count == 4099
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{source_schema}" CASCADE'))
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{restored_schema}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ("receipt", "copy", "cancel"))
async def test_native_owned_export_cleans_its_clone_on_failure(monkeypatch, failure_stage):
    """Receipt failure, dump failure, and cancellation cannot leak the owned clone."""

    async_dsn, _ = _native_test_connection()
    engine = create_async_engine(async_dsn, max_overflow=0, pool_size=2)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    live_schema = "address_export_failure_" + uuid4().hex
    dataset_id = uuid4()
    stage_schema = source.entity_address_archive_stage_schema(dataset_id)
    failure = asyncio.CancelledError() if failure_stage == "cancel" else RuntimeError("synthetic export failure")
    copied = AsyncMock(side_effect=failure)
    if failure_stage == "receipt":
        monkeypatch.setattr(source, "capture_entity_address_archive_receipt", AsyncMock(side_effect=failure))
    try:
        await _seed_live_sentinel(engine, live_schema, source.entity_address_archive_relations())
        with pytest.raises(type(failure)):
            await source.export_entity_address_archive_with_receipt(
                sessions, schema_name=live_schema, dataset_id=dataset_id, archive_copy=copied
            )
        assert copied.await_count == (0 if failure_stage == "receipt" else 1)
        async with engine.begin() as connection:
            assert (
                await connection.scalar(
                    text("SELECT oid FROM pg_namespace WHERE nspname=:name"), {"name": stage_schema}
                )
                is None
            )
            await _assert_live_sentinel(connection, live_schema, "entity_address_unified")
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{live_schema}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
async def test_native_owned_export_never_cleans_a_preexisting_collision():
    """A failed CREATE SCHEMA grants no authority over the preexisting object."""

    async_dsn, _ = _native_test_connection()
    engine = create_async_engine(async_dsn, max_overflow=0, pool_size=2)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    live_schema = "address_export_collision_" + uuid4().hex
    dataset_id = uuid4()
    stage_schema = source.entity_address_archive_stage_schema(dataset_id)
    copied = AsyncMock()
    try:
        await _seed_live_sentinel(engine, live_schema, source.entity_address_archive_relations())
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{stage_schema}"'))
            incumbent_oid = await connection.scalar(
                text("SELECT oid FROM pg_namespace WHERE nspname=:name"), {"name": stage_schema}
            )
        from sqlalchemy.exc import DBAPIError

        with pytest.raises(DBAPIError):
            await source.export_entity_address_archive_with_receipt(
                sessions, schema_name=live_schema, dataset_id=dataset_id, archive_copy=copied
            )
        copied.assert_not_awaited()
        async with engine.begin() as connection:
            assert (
                await connection.scalar(
                    text("SELECT oid FROM pg_namespace WHERE nspname=:name"), {"name": stage_schema}
                )
                == incumbent_oid
            )
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{stage_schema}" CASCADE'))
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{live_schema}" CASCADE'))
        await engine.dispose()


async def _prepare_nonempty_restore_fixture(engine, sessions, destination_schema: str, dataset_id):
    """Create incumbent and owned staged rows that use the actual native model family."""

    async with engine.begin() as connection:
        await _create_model_family(connection, destination_schema)
        await _create_alias_validation_relations(connection, destination_schema)
        incumbent_relation_oid = await connection.scalar(
            text(
                "SELECT relation.oid FROM pg_catalog.pg_class AS relation "
                "JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace "
                "WHERE namespace.nspname = :schema_name AND relation.relname = 'entity_address_unified'"
            ),
            {"schema_name": destination_schema},
        )
        await connection.execute(
            text(
                f'INSERT INTO "{destination_schema}"."entity_address_unified" '
                "(entity_type, entity_id, location_key, checksum, type) "
                "VALUES ('synthetic', 'incumbent', 'live-sentinel', 1, 'primary')"
            )
        )
    async with sessions() as session, session.begin():
        owner = await restore.precreate_entity_address_archive_restore(
            session,
            dataset_id=dataset_id,
            db_schema=destination_schema,
            import_date="20260913",
        )
        await _seed_receipt_family(session, owner.schema_name, reversed_rows=False)
    expected_receipt = await _capture_receipt(sessions, owner.schema_name, "UTC")
    return owner, expected_receipt, incumbent_relation_oid


async def _assert_rehydrated_evidence_sequence(session, destination_schema: str, prepared_restore) -> None:
    """Require the stage-local evidence sequence to advance beyond restored explicit IDs."""

    evidence_stage_name = next(
        table_name
        for table_name, _ in prepared_restore.stage_relation_oids
        if table_name.startswith("entity_address_evidence_")
    )
    highest_evidence_id = await session.scalar(
        text(f'SELECT MAX(evidence_id) FROM "{destination_schema}"."{evidence_stage_name}"')
    )
    next_evidence_id = await session.scalar(
        text("SELECT nextval(to_regclass(:sequence_reference))"),
        {"sequence_reference": f"{destination_schema}.{evidence_stage_name}_evidence_id_seq"},
    )
    assert isinstance(highest_evidence_id, int)
    assert isinstance(next_evidence_id, int)
    assert next_evidence_id > highest_evidence_id


async def _assert_rehydrated_restore_state(
    sessions,
    *,
    destination_schema: str,
    prepared_restore,
    stored,
    incumbent_relation_oid: int,
) -> None:
    """Require rehydration to preserve incumbent data and retain restored rows under pins."""

    async with sessions() as session, session.begin():
        rehydrated = await restore.rehydrate_entity_address_archive_restore(session, stored=stored)
        assert rehydrated.context == prepared_restore.context
        assert rehydrated.publish_validation == prepared_restore.native_validation
        assert (
            await session.scalar(
                text(
                    "SELECT relation.oid FROM pg_catalog.pg_class AS relation "
                    "JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace "
                    "WHERE namespace.nspname = :schema_name AND relation.relname = 'entity_address_unified'"
                ),
                {"schema_name": destination_schema},
            )
            == incumbent_relation_oid
        )
        assert (
            await session.scalar(text(f'SELECT location_key FROM "{destination_schema}"."entity_address_unified"'))
            == "live-sentinel"
        )
        assert (
            await session.scalar(
                text(
                    f'SELECT COUNT(*) FROM "{destination_schema}"."entity_address_unified_20260913" '
                    "WHERE location_key IN ('first', 'second')"
                )
            )
            == 2
        )
        await _assert_rehydrated_evidence_sequence(session, destination_schema, prepared_restore)
        await _assert_stage_share_pins_block_ddl(
            sessions, destination_schema, prepared_restore.stage_relation_oids[0][0]
        )
        assert {table_name for table_name, _ in prepared_restore.stage_relation_oids} == {
            f"{relation.table_name}_20260913" for relation in source.entity_address_archive_relations()
        }
        assert (
            await session.scalar(
                text("SELECT to_regnamespace(:schema_name)"), {"schema_name": prepared_restore.ownership.schema_name}
            )
            is None
        )


@pytest.mark.asyncio
async def test_native_restore_finalization_rehydrates_pinned_stage():
    """Finalize an owned archive beside live relations, then rehydrate only its fences."""

    async_dsn, _ = _native_test_connection()
    engine = create_async_engine(async_dsn, max_overflow=0, pool_size=2)
    destination_schema = "address_restore_destination_" + uuid4().hex
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        owner, expected_receipt, incumbent_relation_oid = await _prepare_nonempty_restore_fixture(
            engine,
            sessions,
            destination_schema,
            uuid4(),
        )
        async with sessions() as session, session.begin():
            prepared_restore = await restore.finalize_entity_address_archive_restore(
                session,
                owner=owner,
                semantic_receipt=expected_receipt,
                db_schema=destination_schema,
                import_date="20260913",
            )
            stored = prepared_restore.as_dict()
        await _assert_rehydrated_restore_state(
            sessions,
            destination_schema=destination_schema,
            prepared_restore=prepared_restore,
            stored=stored,
            incumbent_relation_oid=incumbent_relation_oid,
        )
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{destination_schema}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
async def test_native_restore_precreate_and_cleanup_retain_exact_owned_family():
    """Precreate every real model relation and clean only its unchanged UUID owner."""

    async_dsn, _ = _native_test_connection()
    engine = create_async_engine(async_dsn)
    dataset_id = uuid4()
    destination_schema = "address_restore_destination_" + uuid4().hex
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{destination_schema}"'))
        async with sessions() as session, session.begin():
            owner = await restore.precreate_entity_address_archive_restore(
                session,
                dataset_id=dataset_id,
                db_schema=destination_schema,
                import_date="20260913",
            )
            assert owner.schema_name == ownership.entity_address_archive_stage_schema(dataset_id)
            assert {table_name for table_name, _ in owner.relation_oids} == {
                relation.table_name for relation in source.entity_address_archive_relations()
            }
            await ownership.cleanup_entity_address_archive_stage(session, owner=owner)
        async with engine.connect() as connection:
            assert (
                await connection.scalar(
                    text("SELECT to_regnamespace(:schema_name)"),
                    {"schema_name": owner.schema_name},
                )
                is None
            )
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{destination_schema}" CASCADE'))
        await engine.dispose()
