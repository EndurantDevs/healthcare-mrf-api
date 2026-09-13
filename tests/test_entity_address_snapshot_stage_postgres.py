# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import importlib
import os
import re
import subprocess
from datetime import datetime, timezone
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
alias_receipt = importlib.import_module("process.entity_address_snapshot_alias")
ownership = importlib.import_module("process.entity_address_snapshot_ownership")
restore = importlib.import_module("process.entity_address_snapshot_restore")
models = importlib.import_module("db.models")
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


async def _create_alias_receipt_relations(connection, schema_name: str) -> None:
    """Create the exact model-owned alias tables inspected by the receipt."""

    await connection.execute(text(f'CREATE SCHEMA "{schema_name}"'))
    metadata = MetaData(schema=schema_name)
    for model in (models.AddressAliasStateV1, models.AddressAliasV1):
        model.__table__.to_metadata(metadata, schema=schema_name)
    await connection.run_sync(metadata.create_all)


async def _seed_alias_receipt_relations(
    connection,
    schema_name: str,
    *,
    generation: int,
    history_variant: str,
    add_revoked_history: bool,
) -> None:
    """Seed one shared active mapping with independently variable local history."""

    await connection.execute(
        text(
            f'INSERT INTO "{schema_name}"."address_alias_state_v1" '
            "(singleton, schema_version, active_ruleset_version, generation, updated_at) "
            "VALUES (true, :schema_version, :ruleset_version, :generation, "
            "CAST(:updated_at AS timestamptz))"
        ),
        {
            "schema_version": alias_receipt.address_alias_sql.ADDRESS_ALIAS_SCHEMA_VERSION,
            "ruleset_version": alias_receipt.address_alias_sql.ADDRESS_ALIAS_RULESET_VERSION,
            "generation": generation,
            "updated_at": datetime(2026, 1, 1 if history_variant == "source" else 2, tzinfo=timezone.utc),
        },
    )
    await _insert_active_alias(connection, schema_name, history_variant)
    if add_revoked_history:
        await _insert_revoked_alias_history(connection, schema_name)


async def _insert_active_alias(connection, schema_name: str, history_variant: str) -> None:
    active_alias_by_field = {
        "source_address_key": "00000000-0000-0000-0000-000000000101",
        "source_identity_key": "synthetic-source-identity",
        "target_address_key": "00000000-0000-0000-0000-000000000202",
        "target_identity_key": "synthetic-target-identity",
        "alias_kind": alias_receipt.address_alias_sql.NUMERIC_GRID_ALIAS_KIND,
        "ruleset_version": alias_receipt.address_alias_sql.ADDRESS_ALIAS_RULESET_VERSION,
        "target_strict_source_bits": 5,
        "target_strict_source_count": 2,
        "candidate_count": 1,
        "shadow_run_id": f"00000000-0000-0000-0000-0000000003{1 if history_variant == 'source' else 2:02d}",
        "apply_run_id": f"00000000-0000-0000-0000-0000000004{1 if history_variant == 'source' else 2:02d}",
        "reviewed_candidate_digest": ("a" if history_variant == "source" else "b") * 64,
        "applied_at": datetime(2026, 2, 1 if history_variant == "source" else 2, tzinfo=timezone.utc),
        "created_at": datetime(2026, 2, 1 if history_variant == "source" else 2, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 2, 1 if history_variant == "source" else 2, 1, tzinfo=timezone.utc),
    }
    await connection.execute(
        text(
            f'INSERT INTO "{schema_name}"."address_alias_v1" '
            "(source_address_key, source_identity_key, target_address_key, target_identity_key, "
            "alias_kind, ruleset_version, target_strict_source_bits, target_strict_source_count, "
            "candidate_count, shadow_run_id, apply_run_id, reviewed_candidate_digest, "
            "applied_at, created_at, updated_at) VALUES ("
            "CAST(:source_address_key AS uuid), :source_identity_key, CAST(:target_address_key AS uuid), "
            ":target_identity_key, :alias_kind, :ruleset_version, :target_strict_source_bits, "
            ":target_strict_source_count, :candidate_count, CAST(:shadow_run_id AS uuid), "
            "CAST(:apply_run_id AS uuid), :reviewed_candidate_digest, CAST(:applied_at AS timestamptz), "
            "CAST(:created_at AS timestamptz), CAST(:updated_at AS timestamptz))"
        ),
        active_alias_by_field,
    )


async def _insert_revoked_alias_history(connection, schema_name: str) -> None:
    await connection.execute(
        text(
            f'INSERT INTO "{schema_name}"."address_alias_v1" '
            "(source_address_key, source_identity_key, target_address_key, target_identity_key, "
            "alias_kind, ruleset_version, target_strict_source_bits, target_strict_source_count, "
            "candidate_count, shadow_run_id, apply_run_id, reviewed_candidate_digest, applied_at, "
            "revoked_at, revoked_reason, revoked_by, revoke_run_id, created_at, updated_at) VALUES ("
            "'00000000-0000-0000-0000-000000000501'::uuid, 'old-source', "
            "'00000000-0000-0000-0000-000000000502'::uuid, 'old-target', :alias_kind, "
            ":ruleset_version, 3, 2, 1, '00000000-0000-0000-0000-000000000503'::uuid, "
            "'00000000-0000-0000-0000-000000000504'::uuid, :digest, "
            "TIMESTAMPTZ '2025-01-01 00:00:00+00', TIMESTAMPTZ '2025-02-01 00:00:00+00', "
            "'synthetic revoke', 'synthetic reviewer', "
            "'00000000-0000-0000-0000-000000000505'::uuid, "
            "TIMESTAMPTZ '2025-01-01 00:00:00+00', TIMESTAMPTZ '2025-02-01 00:00:00+00')"
        ),
        {
            "alias_kind": alias_receipt.address_alias_sql.NUMERIC_GRID_ALIAS_KIND,
            "ruleset_version": alias_receipt.address_alias_sql.ADDRESS_ALIAS_RULESET_VERSION,
            "digest": "c" * 64,
        },
    )


async def _capture_alias_receipt(sessions, schema_name: str):
    async with sessions() as session, session.begin():
        return await alias_receipt.capture_entity_address_alias_semantic_receipt(
            session,
            schema_name=schema_name,
        )


async def _change_active_alias_semantics(sessions, schema_name: str) -> None:
    async with sessions() as session, session.begin():
        await session.execute(
            text(
                f'UPDATE "{schema_name}"."address_alias_v1" '
                "SET target_identity_key = 'changed-target' WHERE revoked_at IS NULL"
            )
        )


async def _revoke_active_alias(sessions, schema_name: str) -> None:
    async with sessions() as session, session.begin():
        await session.execute(
            text(
                f'UPDATE "{schema_name}"."address_alias_v1" SET '
                "target_identity_key = 'synthetic-target-identity', "
                "revoked_at = TIMESTAMPTZ '2026-03-01 00:00:00+00', "
                "revoked_reason = 'synthetic revoke', revoked_by = 'synthetic reviewer', "
                "revoke_run_id = '00000000-0000-0000-0000-000000000601'::uuid "
                "WHERE revoked_at IS NULL"
            )
        )


def _assert_alias_semantics_mismatch(expected_receipt, actual_receipt) -> None:
    assert actual_receipt.active_alias_sha256 != expected_receipt.active_alias_sha256
    with pytest.raises(
        alias_receipt.EntityAddressSnapshotAliasError,
        match="active alias semantics differ",
    ):
        alias_receipt.require_matching_entity_address_alias_semantics(
            expected_receipt,
            actual_receipt,
        )


async def _drop_alias_receipt_schemas(engine, *schema_names: str) -> None:
    async with engine.begin() as connection:
        for schema_name in schema_names:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        remaining_schema_count = await connection.scalar(
            text("SELECT COUNT(*) FROM pg_catalog.pg_namespace WHERE nspname = ANY(:schema_names)"),
            {"schema_names": list(schema_names)},
        )
        assert remaining_schema_count == 0


async def _assert_alias_capture_requires_transaction(sessions, schema_name: str) -> None:
    async with sessions() as session:
        with pytest.raises(
            alias_receipt.EntityAddressSnapshotAliasError,
            match="requires a caller transaction",
        ):
            await alias_receipt.capture_entity_address_alias_semantic_receipt(
                session,
                schema_name=schema_name,
            )


async def _assert_unsupported_alias_versions(engine, sessions, schema_name: str) -> None:
    async with sessions() as session, session.begin():
        await session.execute(
            text(f'UPDATE "{schema_name}"."address_alias_state_v1" SET schema_version = :unsupported'),
            {"unsupported": alias_receipt.address_alias_sql.ADDRESS_ALIAS_SCHEMA_VERSION + 1},
        )
        with pytest.raises(
            alias_receipt.EntityAddressSnapshotAliasError,
            match="schema version is unsupported",
        ):
            await alias_receipt.capture_entity_address_alias_semantic_receipt(
                session,
                schema_name=schema_name,
            )

    async with engine.begin() as connection:
        await connection.execute(
            text(f'UPDATE "{schema_name}"."address_alias_state_v1" SET schema_version = :supported'),
            {"supported": alias_receipt.address_alias_sql.ADDRESS_ALIAS_SCHEMA_VERSION},
        )
        await connection.execute(
            text(
                f'UPDATE "{schema_name}"."address_alias_v1" SET ruleset_version = :unsupported WHERE revoked_at IS NULL'
            ),
            {"unsupported": alias_receipt.address_alias_sql.ADDRESS_ALIAS_RULESET_VERSION + 1},
        )
    async with sessions() as session, session.begin():
        with pytest.raises(
            alias_receipt.EntityAddressSnapshotAliasError,
            match="active alias version is unsupported",
        ):
            await alias_receipt.capture_entity_address_alias_semantic_receipt(
                session,
                schema_name=schema_name,
            )


async def _assert_unsupported_alias_shape(engine, sessions, schema_name: str) -> None:
    async with engine.begin() as connection:
        await connection.execute(
            text(
                f'UPDATE "{schema_name}"."address_alias_v1" SET ruleset_version = :supported WHERE revoked_at IS NULL'
            ),
            {"supported": alias_receipt.address_alias_sql.ADDRESS_ALIAS_RULESET_VERSION},
        )
        await connection.execute(
            text(f'ALTER TABLE "{schema_name}"."address_alias_v1" ADD COLUMN unsupported_shape text')
        )
    async with sessions() as session, session.begin():
        with pytest.raises(
            alias_receipt.EntityAddressSnapshotAliasError,
            match="relation shape is unsupported",
        ):
            await alias_receipt.capture_entity_address_alias_semantic_receipt(
                session,
                schema_name=schema_name,
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

    assert (
        await connection.scalar(
            text(f'SELECT location_key FROM "{schema_name}"."{table_name}" WHERE entity_id = \'owned\'')
        )
        == "live-sentinel"
    )


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
async def test_active_alias_receipt_portability_and_drift():
    """Only active mapping semantics, not local counters or history, define equality."""

    async_dsn, _ = _native_test_connection()
    engine = create_async_engine(async_dsn, max_overflow=0, pool_size=1)
    source_schema = "address_alias_receipt_source_" + uuid4().hex
    restored_schema = "address_alias_receipt_restored_" + uuid4().hex
    try:
        async with engine.begin() as connection:
            await _create_alias_receipt_relations(connection, source_schema)
            await _create_alias_receipt_relations(connection, restored_schema)
            await _seed_alias_receipt_relations(
                connection,
                source_schema,
                generation=3,
                history_variant="source",
                add_revoked_history=False,
            )
            await _seed_alias_receipt_relations(
                connection,
                restored_schema,
                generation=91,
                history_variant="restored",
                add_revoked_history=True,
            )
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        source_alias_receipt = await _capture_alias_receipt(sessions, source_schema)
        restored_alias_receipt = await _capture_alias_receipt(sessions, restored_schema)

        assert source_alias_receipt.local_generation == 3
        assert restored_alias_receipt.local_generation == 91
        assert source_alias_receipt.active_alias_count == 1
        assert source_alias_receipt.portable_identity() == restored_alias_receipt.portable_identity()
        assert (
            alias_receipt.validate_entity_address_alias_semantic_receipt(source_alias_receipt.as_dict())
            == source_alias_receipt
        )
        assert (
            alias_receipt.require_matching_entity_address_alias_semantics(
                source_alias_receipt,
                restored_alias_receipt,
            )
            == restored_alias_receipt
        )

        await _change_active_alias_semantics(sessions, restored_schema)
        changed_receipt = await _capture_alias_receipt(sessions, restored_schema)
        _assert_alias_semantics_mismatch(source_alias_receipt, changed_receipt)

        await _revoke_active_alias(sessions, restored_schema)
        revoked_receipt = await _capture_alias_receipt(sessions, restored_schema)
        assert revoked_receipt.active_alias_count == 0
        _assert_alias_semantics_mismatch(source_alias_receipt, revoked_receipt)
    finally:
        await _drop_alias_receipt_schemas(engine, source_schema, restored_schema)
        await engine.dispose()


@pytest.mark.asyncio
async def test_active_alias_receipt_rejects_unsupported_model_shape_and_state_version():
    """Receipt capture fails closed on schema drift and unsupported alias policy."""

    async_dsn, _ = _native_test_connection()
    engine = create_async_engine(async_dsn, max_overflow=0, pool_size=1)
    schema_name = "address_alias_receipt_invalid_" + uuid4().hex
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with engine.begin() as connection:
            await _create_alias_receipt_relations(connection, schema_name)
            await _seed_alias_receipt_relations(
                connection,
                schema_name,
                generation=1,
                history_variant="source",
                add_revoked_history=False,
            )

        await _assert_alias_capture_requires_transaction(sessions, schema_name)
        await _assert_unsupported_alias_versions(engine, sessions, schema_name)
        await _assert_unsupported_alias_shape(engine, sessions, schema_name)
    finally:
        await _drop_alias_receipt_schemas(engine, schema_name)
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
            assert stored["stage_integrity"] == prepared_restore.stage_integrity.as_dict()
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


def _prepared_main_stage(prepared_restore) -> tuple[str, int]:
    return next(
        (table_name, oid)
        for table_name, oid in prepared_restore.stage_relation_oids
        if table_name.startswith("entity_address_unified_")
    )


async def _insert_prepared_stage_row(session, destination_schema: str, main_stage_name: str) -> None:
    table_ref = f'"{destination_schema}"."{main_stage_name}"'
    await session.execute(
        text(
            f"INSERT INTO {table_ref} "
            "(entity_type, entity_id, location_key, checksum, type, base_address_version) "
            "VALUES ('synthetic', 'third', 'third', 3, 'primary', :base_address_version)"
        ),
        {"base_address_version": source.entity_address_unified.ALIAS_BASE_ADDRESS_VERSION_PREFIX + "0"},
    )


async def _update_prepared_stage_row(session, destination_schema: str, main_stage_name: str) -> None:
    table_ref = f'"{destination_schema}"."{main_stage_name}"'
    await session.execute(text(f"UPDATE {table_ref} SET checksum = checksum + 1 WHERE location_key = 'first'"))


async def _delete_prepared_stage_row(session, destination_schema: str, main_stage_name: str) -> None:
    table_ref = f'"{destination_schema}"."{main_stage_name}"'
    await session.execute(text(f"DELETE FROM {table_ref} WHERE location_key = 'second'"))


async def _alter_prepared_stage_schema(session, destination_schema: str, main_stage_name: str) -> None:
    table_ref = f'"{destination_schema}"."{main_stage_name}"'
    await session.execute(text(f"ALTER TABLE {table_ref} ADD COLUMN stage_integrity_tamper text"))


async def _drop_prepared_stage_index(session, destination_schema: str, main_stage_name: str) -> None:
    index_name = await session.scalar(
        text(
            "SELECT index_relation.relname FROM pg_catalog.pg_index AS index_meta "
            "JOIN pg_catalog.pg_class AS index_relation ON index_relation.oid = index_meta.indexrelid "
            "WHERE index_meta.indrelid = to_regclass(:table_reference) "
            "AND index_meta.indisprimary IS FALSE ORDER BY index_relation.relname LIMIT 1"
        ),
        {"table_reference": f"{destination_schema}.{main_stage_name}"},
    )
    assert isinstance(index_name, str)
    await session.execute(text(f'DROP INDEX "{destination_schema}"."{index_name}"'))


async def _mutate_prepared_stage(session, destination_schema: str, prepared_restore, mutation: str) -> None:
    main_stage_name, _main_stage_oid = _prepared_main_stage(prepared_restore)
    operations_by_mutation = {
        "insert": _insert_prepared_stage_row,
        "update": _update_prepared_stage_row,
        "delete": _delete_prepared_stage_row,
        "schema": _alter_prepared_stage_schema,
        "index": _drop_prepared_stage_index,
    }
    await operations_by_mutation[mutation](session, destination_schema, main_stage_name)


async def _finalize_restore_fixture(engine, sessions, destination_schema: str):
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
    return prepared_restore, stored, incumbent_relation_oid


async def _assert_prepared_stage_oid(session, destination_schema: str, prepared_restore) -> None:
    main_stage_name, main_stage_oid = _prepared_main_stage(prepared_restore)
    assert (
        await session.scalar(
            text("SELECT to_regclass(:table_reference)::oid"),
            {"table_reference": f"{destination_schema}.{main_stage_name}"},
        )
        == main_stage_oid
    )


async def _assert_incumbent_unchanged(engine, destination_schema: str, incumbent_relation_oid: int) -> None:
    async with engine.connect() as connection:
        assert (
            await connection.scalar(
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
            await connection.scalar(
                text(
                    f'SELECT location_key FROM "{destination_schema}"."entity_address_unified" '
                    "WHERE entity_id = 'incumbent'"
                )
            )
            == "live-sentinel"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ("insert", "update", "delete", "schema", "index"))
async def test_native_rehydration_rejects_same_oid_stage_mutation(mutation: str):
    """Rows, schema, and indexes cannot drift after durable preparation."""

    async_dsn, _ = _native_test_connection()
    engine = create_async_engine(async_dsn, max_overflow=0, pool_size=2)
    destination_schema = "address_restore_integrity_" + uuid4().hex
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        prepared_restore, stored, incumbent_relation_oid = await _finalize_restore_fixture(
            engine,
            sessions,
            destination_schema,
        )
        async with sessions() as session, session.begin():
            await _mutate_prepared_stage(session, destination_schema, prepared_restore, mutation)
            await _assert_prepared_stage_oid(session, destination_schema, prepared_restore)
        async with sessions() as session, session.begin():
            with pytest.raises(restore.EntityAddressSnapshotRestoreError, match="stage integrity differs"):
                await restore.rehydrate_entity_address_archive_restore(session, stored=stored)
        await _assert_incumbent_unchanged(engine, destination_schema, incumbent_relation_oid)
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
