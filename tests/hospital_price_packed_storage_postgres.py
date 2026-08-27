# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable-PostgreSQL integrity and GC helpers for packed hospital prices."""

from __future__ import annotations

from contextlib import asynccontextmanager
import hashlib
from pathlib import Path
from types import SimpleNamespace
import uuid

import asyncpg
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from db.connection import ConnectionProxy
from process import hospital_price_store
from tests.hospital_price_packed_storage_support import _packed_receipt
from tests.test_hospital_price_storage import (
    ROOT,
    _database_url,
    _drop_schema,
    _load_migration,
    _prepare_schema,
    _quote,
    _run_migration,
    _seed_content_version,
    _seed_registry,
)

PACKED_MIGRATION_PATH = (
    ROOT / "alembic/versions/20260826090000_hospital_price_packed_blocks.py"
)
SELECTOR_RANGE_MIGRATION_PATH = (
    ROOT / "alembic/versions/20260826200000_hospital_price_selector_range_index.py"
)
SOURCE_FORMAT_MIGRATION_PATH = (
    ROOT / "alembic/versions/20260827120000_hospital_price_source_format.py"
)
SELECTOR_PACKING_MIGRATION_PATH = (
    ROOT / "alembic/versions/20260827160000_hospital_price_selector_page_packing.py"
)

GC_ATTEMPTS_SQL = """INSERT INTO {quoted}.hospital_price_import_attempt (
attempt_id, hospital_id, locator_id, locator_observation_id,
registry_version, requested_source_url, expected_generation,
status, content_sha256, version_id, lease_owner, heartbeat_at,
lease_expires_at, finished_at) VALUES
('old-a', 'hospital-a', 'locator-1', 'observation-1', 1,
'https://hospital.example/old.json', 0, 'published', $1, $2,
'hospital-prices:test', clock_timestamp(),
clock_timestamp() + interval '5 minutes', clock_timestamp()),
('old-b', 'hospital-b', 'locator-1', 'observation-1', 1,
'https://hospital.example/old.json', 0, 'published', $1, $2,
'hospital-prices:test', clock_timestamp(),
clock_timestamp() + interval '5 minutes', clock_timestamp()),
('new-a', 'hospital-a', 'locator-1', 'observation-1', 1,
'https://hospital.example/new.json', 1, 'published', $3, $4,
'hospital-prices:test', clock_timestamp(),
clock_timestamp() + interval '5 minutes', clock_timestamp()),
('new-b', 'hospital-b', 'locator-1', 'observation-1', 1,
'https://hospital.example/new.json', 1, 'published', $3, $4,
'hospital-prices:test', clock_timestamp(),
clock_timestamp() + interval '5 minutes', clock_timestamp()),
('active-a', 'hospital-a', 'locator-1', 'observation-1', 1,
'https://hospital.example/active.json', 2, 'verified', $5, $6,
'hospital-prices:test', clock_timestamp(),
clock_timestamp() + interval '5 minutes', NULL)"""

GC_CURRENT_SQL = """INSERT INTO {quoted}.hospital_price_current(
hospital_id, version_id, generation, published_attempt_id,
latest_attempt_id, service_count, charge_count, payer_charge_count,
tax_identity_count, last_success_at) VALUES
('hospital-a', $1, 1, 'old-a', 'old-a', 1, 1, 1, 1, clock_timestamp()),
('hospital-b', $1, 1, 'old-b', 'old-b', 1, 1, 1, 1, clock_timestamp()),
('hospital-unbound', NULL, 0, NULL, NULL, 0, 0, 0, 0, NULL)"""


@asynccontextmanager
async def _packed_database(monkeypatch):
    database_url = _database_url()
    schema = f"hospital_price_test_{uuid.uuid4().hex}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(hospital_price_store, "schema_name", lambda: schema)
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg"), poolclass=NullPool
    )
    case = SimpleNamespace(
        database_url=database_url,
        schema=schema,
        quoted=_quote(schema),
        engine=engine,
        base_migration=_load_migration(),
        packed_migration=_load_migration(PACKED_MIGRATION_PATH),
        selector_range_migration=_load_migration(SELECTOR_RANGE_MIGRATION_PATH),
        source_format_migration=_load_migration(SOURCE_FORMAT_MIGRATION_PATH),
        selector_packing_migration=_load_migration(SELECTOR_PACKING_MIGRATION_PATH),
    )
    await _prepare_schema(engine, schema)
    try:
        yield case
    finally:
        await _drop_schema(engine, schema)
        await engine.dispose()


def _driver_dsn(case) -> str:
    return str(case.database_url.set(drivername="postgresql"))


async def _connection_proxy(connection) -> ConnectionProxy:
    raw_connection = await connection.get_raw_connection()
    return ConnectionProxy(hospital_price_store.db, connection, raw_connection)


async def _seed_packed_versions(case, version_ids: tuple[str, str, str]) -> None:
    version_id, bad_version_id, replay_version_id = version_ids
    connection = await asyncpg.connect(_driver_dsn(case))
    try:
        await _seed_registry(connection, case.quoted)
        for content_sha, stored_version_id in (
            ("1" * 64, version_id),
            ("3" * 64, bad_version_id),
            ("5" * 64, replay_version_id),
        ):
            await _seed_content_version(
                connection, case.quoted, content_sha, stored_version_id
            )
        await connection.execute(
            f"UPDATE {case.quoted}.hospital_price_version SET charge_count=513 "
            "WHERE version_id=$1",
            version_id,
        )
        await connection.execute(
            f"UPDATE {case.quoted}.hospital_price_version "
            "SET service_count=3, charge_count=3 WHERE version_id=$1",
            replay_version_id,
        )
    finally:
        await connection.close()


async def _prove_v1_selector_count_backfill(case) -> None:
    version_id = "0" * 64
    connection = await asyncpg.connect(_driver_dsn(case))
    try:
        await _seed_content_version(connection, case.quoted, "f" * 64, version_id)
        await connection.execute(
            f"INSERT INTO {case.quoted}.hospital_price_packed_root VALUES "
            "($1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, clock_timestamp())",
            version_id,
        )
        for kind, logical_first, key_digest, parent_digest, block_payload in (
            (3, 0, b"c" * 32, None, b"code"),
            (4, 1, b"p" * 32, b"p" * 32, b"payer"),
        ):
            await connection.execute(
                f"INSERT INTO {case.quoted}.hospital_price_data_block VALUES "
                "($1, $2, 0, $3, 1, 0, 1, 0, 1, $4, $5, $6, $7)",
                version_id,
                kind,
                logical_first,
                key_digest,
                parent_digest,
                hashlib.sha256(block_payload).digest(),
                block_payload,
            )
    finally:
        await connection.close()

    await _run_migration(case.engine, case.selector_packing_migration, "upgrade")
    connection = await asyncpg.connect(_driver_dsn(case))
    try:
        assert tuple(await connection.fetchrow(
            f"SELECT format_version, code_selector_block_count, "
            f"payer_plan_selector_block_count FROM {case.quoted}.hospital_price_packed_root "
            "WHERE version_id=$1",
            version_id,
        )) == (1, 1, 1)
        await connection.execute(
            f"DELETE FROM {case.quoted}.hospital_price_version WHERE version_id=$1",
            version_id,
        )
    finally:
        await connection.close()


async def _store_packed_receipt(case, receipt) -> None:
    async with case.engine.begin() as connection:
        proxy = await _connection_proxy(connection)
        await hospital_price_store._insert_packed_root(proxy, receipt)
        await hospital_price_store.copy_packed_blocks(proxy, receipt, case.schema)
        await hospital_price_store.validate_packed_storage(
            proxy, receipt, case.schema
        )


async def _assert_packed_constraints(case, version_id: str) -> None:
    connection = await asyncpg.connect(_driver_dsn(case))
    try:
        assert await connection.fetchval(
            f"SELECT COUNT(*) FROM {case.quoted}.hospital_price_data_block "
            "WHERE version_id=$1",
            version_id,
        ) == 5
        assert await connection.fetchval(
            "SELECT pg_total_relation_size($1::regclass)",
            f"{case.schema}.hospital_price_data_block",
        ) > 0
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await connection.execute(
                f"UPDATE {case.quoted}.hospital_price_packed_root "
                "SET service_count=service_count WHERE version_id=$1",
                version_id,
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                f"INSERT INTO {case.quoted}.hospital_price_data_block VALUES ("
                "$1, 1, 1, 1, 1, 1, 1, 0, 0, NULL, NULL, "
                "decode(repeat('00', 32), 'hex'), 'bad'::bytea)",
                version_id,
            )
    finally:
        await connection.close()


async def _assert_downgrade_is_blocked(case, version_id: str) -> None:
    with pytest.raises(
        sa.exc.DBAPIError, match="HOSPITAL_PRICE_PACKED_DOWNGRADE_BLOCKED"
    ):
        await _run_migration(case.engine, case.packed_migration, "downgrade")
    async with case.engine.connect() as connection:
        assert await connection.scalar(
            sa.text("SELECT to_regclass(:relation) IS NOT NULL"),
            {"relation": f"{case.schema}.hospital_price_data_block"},
        )
        assert await connection.scalar(
            sa.text(
                f"SELECT COUNT(*) FROM {case.quoted}.hospital_price_data_block "
                "WHERE version_id=:version"
            ),
            {"version": version_id},
        ) == 5


async def _assert_selector_packing_downgrade_is_blocked(case) -> None:
    with pytest.raises(sa.exc.DBAPIError, match="cannot downgrade.*v2 roots"):
        await _run_migration(case.engine, case.selector_packing_migration, "downgrade")


async def _assert_invalid_receipt_rolls_back(
    case, receipt, *, match: str = "logical ranges",
) -> None:
    with pytest.raises(RuntimeError, match=match):
        async with case.engine.begin() as connection:
            proxy = await _connection_proxy(connection)
            await hospital_price_store._insert_packed_root(proxy, receipt)
            await hospital_price_store.copy_packed_blocks(
                proxy, receipt, case.schema
            )
            await hospital_price_store.validate_packed_storage(
                proxy, receipt, case.schema
            )


async def _assert_packed_cascade(case, version_ids: tuple[str, str, str]) -> None:
    version_id, bad_version_id, replay_version_id = version_ids
    connection = await asyncpg.connect(_driver_dsn(case))
    try:
        assert await connection.fetchval(
            f"SELECT COUNT(*) FROM {case.quoted}.hospital_price_packed_root "
            "WHERE version_id=ANY($1::text[])",
            [bad_version_id, replay_version_id],
        ) == 0
        await connection.execute(
            f"DELETE FROM {case.quoted}.hospital_price_version WHERE version_id=$1",
            version_id,
        )
        assert await connection.fetchval(
            f"SELECT COUNT(*) FROM {case.quoted}.hospital_price_data_block "
            "WHERE version_id=$1",
            version_id,
        ) == 0
    finally:
        await connection.close()


async def _prove_packed_integrity(monkeypatch, tmp_path: Path) -> None:
    """Prove binary COPY integrity, immutable blocks, rollback, and cascade."""

    async with _packed_database(monkeypatch) as case:
        await _run_migration(case.engine, case.base_migration, "upgrade")
        await _run_migration(case.engine, case.packed_migration, "upgrade")
        await _run_migration(case.engine, case.selector_range_migration, "upgrade")
        await _run_migration(case.engine, case.source_format_migration, "upgrade")
        version_ids = ("2" * 64, "4" * 64, "6" * 64)
        await _seed_packed_versions(case, version_ids)
        await _prove_v1_selector_count_backfill(case)
        await _store_packed_receipt(
            case, _packed_receipt(tmp_path, version_ids[0], split_service=True)
        )
        await _assert_packed_constraints(case, version_ids[0])
        await _assert_selector_packing_downgrade_is_blocked(case)
        await _assert_downgrade_is_blocked(case, version_ids[0])
        await _assert_invalid_receipt_rolls_back(
            case, _packed_receipt(tmp_path, version_ids[1], service_first=1)
        )
        connection = await asyncpg.connect(_driver_dsn(case))
        try:
            await connection.execute(
                f"UPDATE {case.quoted}.hospital_price_version SET charge_count=513 "
                "WHERE version_id=$1",
                version_ids[1],
            )
        finally:
            await connection.close()
        await _assert_invalid_receipt_rolls_back(
            case,
            _packed_receipt(
                tmp_path,
                version_ids[1],
                split_service=True,
                mixed_null_code_parent=True,
            ),
            match="selector pages are incomplete",
        )
        await _assert_invalid_receipt_rolls_back(
            case, _packed_receipt(
                tmp_path, version_ids[2], replayed_services=True
            )
        )
        await _assert_packed_cascade(case, version_ids)
        await _run_migration(case.engine, case.selector_packing_migration, "downgrade")
        await _run_migration(case.engine, case.selector_range_migration, "downgrade")
        await _run_migration(case.engine, case.packed_migration, "downgrade")
        await _run_migration(case.engine, case.base_migration, "downgrade")


async def _seed_gc_scenario(case) -> SimpleNamespace:
    state = SimpleNamespace(
        old_version="7" * 64,
        new_version="8" * 64,
        active_version="9" * 64,
        old_content="a" * 64,
        new_content="b" * 64,
        active_content="c" * 64,
    )
    connection = await asyncpg.connect(_driver_dsn(case))
    try:
        await _seed_registry(connection, case.quoted)
        for content_sha, version_id in (
            (state.old_content, state.old_version),
            (state.new_content, state.new_version),
            (state.active_content, state.active_version),
        ):
            await _seed_content_version(
                connection, case.quoted, content_sha, version_id
            )
        await connection.execute(
            GC_ATTEMPTS_SQL.format(quoted=case.quoted),
            state.old_content,
            state.old_version,
            state.new_content,
            state.new_version,
            state.active_content,
            state.active_version,
        )
        await connection.execute(
            f"INSERT INTO {case.quoted}.hospital_price_hospital_tax_identity VALUES "
            "('hospital-a', $1, 'old-a', 'ein', '001234567', 'filename', 0), "
            "('hospital-b', $1, 'old-b', 'ein', '009876543', 'filename', 0)",
            state.old_version,
        )
        await connection.execute(
            GC_CURRENT_SQL.format(quoted=case.quoted), state.old_version
        )
    finally:
        await connection.close()
    return state


def _install_gc_connection(monkeypatch, case) -> None:
    @asynccontextmanager
    async def acquire_store_connection():
        async with case.engine.begin() as connection:
            yield await _connection_proxy(connection)

    monkeypatch.setattr(
        hospital_price_store.db, "acquire", acquire_store_connection
    )


async def _assert_shared_lkg_is_retained(case, state) -> None:
    connection = await asyncpg.connect(_driver_dsn(case))
    try:
        await connection.execute(
            f"UPDATE {case.quoted}.hospital_price_current SET version_id=$1, "
            "generation=2, published_attempt_id='new-a', "
            "latest_attempt_id='new-a', last_success_at=clock_timestamp() "
            "WHERE hospital_id='hospital-a'",
            state.new_version,
        )
        assert await hospital_price_store.garbage_collect_superseded_versions() == 0
        assert await connection.fetchval(
            f"SELECT COUNT(*) FROM {case.quoted}.hospital_price_data_block "
            "WHERE version_id=$1",
            state.old_version,
        ) == 4
        await connection.execute(
            f"INSERT INTO {case.quoted}.hospital_price_import_attempt ("
            "attempt_id, hospital_id, locator_id, locator_observation_id, "
            "registry_version, requested_source_url, expected_generation, "
            "status, lease_owner, heartbeat_at, lease_expires_at, finished_at, "
            "error_code) VALUES ('failed-b', 'hospital-b', 'locator-1', "
            "'observation-1', 1, 'https://hospital.example/failed.json', 1, "
            "'failed', 'hospital-prices:test', clock_timestamp(), "
            "clock_timestamp() + interval '5 minutes', clock_timestamp(), "
            "'invalid_source')"
        )
        await connection.execute(
            f"UPDATE {case.quoted}.hospital_price_current "
            "SET latest_attempt_id='failed-b' WHERE hospital_id='hospital-b'"
        )
        assert await hospital_price_store.garbage_collect_superseded_versions() == 0
        assert await connection.fetchval(
            f"SELECT version_id FROM {case.quoted}.hospital_price_current "
            "WHERE hospital_id='hospital-b'"
        ) == state.old_version
    finally:
        await connection.close()


async def _assert_superseded_version_is_collected(case, state) -> None:
    connection = await asyncpg.connect(_driver_dsn(case))
    try:
        await connection.execute(
            f"UPDATE {case.quoted}.hospital_price_current SET version_id=$1, "
            "generation=2, published_attempt_id='new-b', "
            "latest_attempt_id='new-b', last_success_at=clock_timestamp() "
            "WHERE hospital_id='hospital-b'",
            state.new_version,
        )
        assert await hospital_price_store.garbage_collect_superseded_versions() == 1
        for table_name in (
            "hospital_price_version",
            "hospital_price_data_block",
            "hospital_price_hospital_tax_identity",
        ):
            assert await connection.fetchval(
                f"SELECT COUNT(*) FROM {case.quoted}.{table_name} WHERE version_id=$1",
                state.old_version,
            ) == 0
        assert await connection.fetchval(
            f"SELECT COUNT(*) FROM {case.quoted}.hospital_price_import_attempt "
            "WHERE attempt_id IN ('old-a', 'old-b') AND status='published' "
            "AND content_sha256=$1 AND version_id IS NULL",
            state.old_content,
        ) == 2
        assert await connection.fetchval(
            f"SELECT COUNT(*) FROM {case.quoted}.hospital_price_version "
            "WHERE version_id=ANY($1::text[])",
            [state.new_version, state.active_version],
        ) == 2
    finally:
        await connection.close()


async def _prove_gc_retention(
    monkeypatch, tmp_path: Path
) -> None:
    """Keep shared LKG and active versions, then collect only stale storage."""

    async with _packed_database(monkeypatch) as case:
        await _run_migration(case.engine, case.base_migration, "upgrade")
        await _run_migration(case.engine, case.packed_migration, "upgrade")
        await _run_migration(case.engine, case.selector_range_migration, "upgrade")
        await _run_migration(case.engine, case.source_format_migration, "upgrade")
        await _run_migration(case.engine, case.selector_packing_migration, "upgrade")
        state = await _seed_gc_scenario(case)
        await _store_packed_receipt(
            case, _packed_receipt(tmp_path, state.old_version)
        )
        _install_gc_connection(monkeypatch, case)
        await _assert_shared_lkg_is_retained(case, state)
        await _assert_superseded_version_is_collected(case, state)
        await _run_migration(case.engine, case.selector_packing_migration, "downgrade")
        await _run_migration(case.engine, case.selector_range_migration, "downgrade")
        await _run_migration(case.engine, case.packed_migration, "downgrade")
        await _run_migration(case.engine, case.base_migration, "downgrade")
