"""PostgreSQL concurrency proofs for V5 retired-work write guards."""

from __future__ import annotations

import asyncio
from contextlib import suppress
import hashlib

import pytest

from tests.ptg_wave_materialized_preclaim_postgres_support import (
    insert_materialized_supersession,
    materialized_evidence,
    seed_materialized_predecessor,
)
from tests.test_ptg_wave_materialized_preclaim_storage_postgres import (
    _admit_v5,
)
from tests.test_ptg_wave_recovery_storage_postgres import (
    _dsn,
    _install_migration,
    _quote,
    asyncpg,
)


_ADMISSION_LOCK_SQL = (
    "SELECT pg_advisory_xact_lock("
    "pg_catalog.hashtextextended("
    "'import-run-admission:ptg-source-file', 0))"
)
_UNRETIRED_PROGRESS_STATEMENTS = (
    "INSERT INTO {schema}.ptg_import_wave_claim (wave_id) "
    "VALUES ('materialized-wave')",
    "INSERT INTO {schema}.ptg_source_attempt_event (outer_run_id, event_kind) "
    "VALUES ('materialized-run', 'worker_start_admitted')",
)
_STALE_SNAPSHOT_WRITE_STATEMENTS = (
    "UPDATE {schema}.import_run SET started_at = clock_timestamp() "
    "WHERE run_id = 'materialized-run'",
    "INSERT INTO {schema}.ptg_import_wave_claim (wave_id) "
    "VALUES ('materialized-wave')",
    "INSERT INTO {schema}.ptg_source_attempt_event (outer_run_id, event_kind) "
    "VALUES ('materialized-run', 'worker_start_admitted')",
)


async def _acquire_test_admission_lock(connection):
    lock_transaction = connection.transaction()
    await lock_transaction.start()
    await connection.fetchval(_ADMISSION_LOCK_SQL)
    return lock_transaction


async def _insert_unrelated_fhir_run(connection, quoted_schema: str) -> None:
    await connection.execute(
        f"INSERT INTO {quoted_schema}.import_run "
        "(run_id, importer, status, metrics) VALUES "
        "('unrelated-fhir-run', 'fhir', 'queued', '{}'::jsonb)"
    )


async def _persist_fresh_successor_work(
    connection,
    quoted_schema: str,
    successor_wave_id: str,
    successor_wave_digest: str,
) -> None:
    async with connection.transaction():
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.import_run (
                run_id, importer, status, source_file_import_id,
                import_id, params, metrics
            ) VALUES (
                'fresh-successor-run', 'ptg', 'queued',
                'materialized-source', 'materialized-source',
                jsonb_build_object('_wave_id', $1::text),
                jsonb_build_object(
                    'wave_id', $1::text,
                    'wave_digest', $2::text
                )
            )
            """,
            successor_wave_id,
            successor_wave_digest,
        )
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.ptg_import_wave_intent (
                wave_id, run_id, source_file_import_id, job_id, ordinal
            ) VALUES (
                $1, 'fresh-successor-run', 'materialized-source',
                'fresh-successor-job-0', 0
            )
            """,
            successor_wave_id,
        )
        await connection.execute(
            f"UPDATE {quoted_schema}.import_run SET status = 'running', "
            "started_at = clock_timestamp() "
            "WHERE run_id = 'fresh-successor-run'"
        )
        await connection.execute(
            f"INSERT INTO {quoted_schema}.ptg_source_attempt_event "
            "(outer_run_id, event_kind) VALUES "
            "('fresh-successor-run', 'worker_start_admitted')"
        )


async def _assert_fresh_successor_work(
    connection,
    quoted_schema: str,
    successor_wave_id: str,
) -> None:
    assert await connection.fetchval(
        f"SELECT count(*) FROM {quoted_schema}.import_run "
        "WHERE run_id = 'fresh-successor-run' "
        "AND source_file_import_id = 'materialized-source' "
        "AND status = 'running' AND started_at IS NOT NULL"
    ) == 1
    assert await connection.fetchval(
        f"SELECT count(*) FROM {quoted_schema}.ptg_import_wave_intent "
        "WHERE wave_id = $1 AND run_id = 'fresh-successor-run' "
        "AND source_file_import_id = 'materialized-source'",
        successor_wave_id,
    ) == 1
    assert await connection.fetchval(
        f"SELECT count(*) FROM {quoted_schema}.ptg_source_attempt_event "
        "WHERE outer_run_id = 'fresh-successor-run' "
        "AND event_kind = 'worker_start_admitted'"
    ) == 1


@pytest.mark.asyncio
async def test_v5_run_guard_does_not_serialize_unrelated_fhir_work(monkeypatch):
    """Keep unrelated FHIR writes outside the PTG admission lock."""

    schema = "wave_recovery_materialized_fhir_isolation"
    connection = await asyncpg.connect(_dsn())
    lock_connection = await asyncpg.connect(_dsn())
    guarded_connection = await asyncpg.connect(_dsn())
    lock_transaction = None
    retired_update_task = None
    try:
        await _install_migration(connection, monkeypatch, schema)
        descriptor = await seed_materialized_predecessor(connection, schema)
        await _admit_v5(connection, schema, descriptor, "isolation-successor")
        quoted_schema = _quote(schema)
        lock_transaction = await _acquire_test_admission_lock(lock_connection)
        await asyncio.wait_for(
            _insert_unrelated_fhir_run(guarded_connection, quoted_schema),
            timeout=1.0,
        )
        retired_update_task = asyncio.create_task(
            guarded_connection.execute(
                f"UPDATE {quoted_schema}.import_run "
                "SET started_at = clock_timestamp() "
                "WHERE run_id = 'materialized-run'"
            )
        )
        await asyncio.sleep(0.1)
        assert not retired_update_task.done()
        await lock_transaction.commit()
        lock_transaction = None
        with pytest.raises(
            asyncpg.PostgresError,
            match="MATERIALIZED_PRECLAIM_RETIRED",
        ):
            await retired_update_task
        retired_update_task = None
    finally:
        if lock_transaction is not None:
            await lock_transaction.rollback()
        if retired_update_task is not None:
            with suppress(asyncpg.PostgresError, asyncio.CancelledError):
                await retired_update_task
        await connection.execute(
            f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE"
        )
        await guarded_connection.close()
        await lock_connection.close()
        await connection.close()


@pytest.mark.asyncio
async def test_v5_allows_successor_run_to_reuse_source_with_fresh_identity(
    monkeypatch,
):
    """Retire only V10 work identity, never its reusable source identity."""

    schema = "wave_recovery_materialized_successor_run"
    connection = await asyncpg.connect(_dsn())
    try:
        await _install_migration(connection, monkeypatch, schema)
        descriptor = await seed_materialized_predecessor(connection, schema)
        successor_wave_id = "fresh-run-successor"
        await _admit_v5(connection, schema, descriptor, successor_wave_id)
        quoted_schema = _quote(schema)
        successor_wave_digest = await connection.fetchval(
            f"SELECT wave_digest FROM {quoted_schema}.ptg_import_wave "
            "WHERE wave_id = $1",
            successor_wave_id,
        )
        await _persist_fresh_successor_work(
            connection,
            quoted_schema,
            successor_wave_id,
            successor_wave_digest,
        )
        await _assert_fresh_successor_work(
            connection,
            quoted_schema,
            successor_wave_id,
        )
    finally:
        await connection.execute(
            f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE"
        )
        await connection.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("progress_statement", _UNRETIRED_PROGRESS_STATEMENTS)
async def test_v5_unretired_progress_never_waits_on_admission_lock(
    monkeypatch,
    progress_statement,
):
    """Avoid lock inversion, then make the V5 handoff reject new progress."""

    statement_digest = hashlib.sha256(progress_statement.encode()).hexdigest()
    schema = "wave_recovery_materialized_lock_order_" + statement_digest[:12]
    connection = await asyncpg.connect(_dsn())
    lock_connection = await asyncpg.connect(_dsn())
    guarded_connection = await asyncpg.connect(_dsn())
    lock_transaction = None
    try:
        await _install_migration(connection, monkeypatch, schema)
        descriptor = await seed_materialized_predecessor(connection, schema)
        lock_transaction = await _acquire_test_admission_lock(lock_connection)
        await asyncio.wait_for(
            guarded_connection.execute(
                progress_statement.format(schema=_quote(schema))
            ),
            timeout=1.0,
        )
        await lock_transaction.commit()
        lock_transaction = None
        with pytest.raises(
            asyncpg.PostgresError,
            match="MATERIALIZED_PRECLAIM_REQUIRED",
        ):
            await _admit_v5(
                connection,
                schema,
                descriptor,
                "progress-race-successor",
            )
    finally:
        if lock_transaction is not None:
            await lock_transaction.rollback()
        await connection.execute(
            f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE"
        )
        await guarded_connection.close()
        await lock_connection.close()
        await connection.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("write_statement", _STALE_SNAPSHOT_WRITE_STATEMENTS)
async def test_v5_stale_repeatable_read_snapshot_cannot_mutate_retired_work(
    monkeypatch,
    write_statement,
):
    """Reject a PTG write whose transaction snapshot predates retirement."""

    statement_digest = hashlib.sha256(write_statement.encode()).hexdigest()
    schema = "wave_recovery_materialized_stale_" + statement_digest[:12]
    connection = await asyncpg.connect(_dsn())
    stale_connection = await asyncpg.connect(_dsn())
    stale_transaction = stale_connection.transaction(
        isolation="repeatable_read",
    )
    is_stale_transaction_active = False
    try:
        await _install_migration(connection, monkeypatch, schema)
        descriptor = await seed_materialized_predecessor(connection, schema)
        quoted_schema = _quote(schema)
        await stale_transaction.start()
        is_stale_transaction_active = True
        await stale_connection.fetchval(
            f"SELECT count(*) FROM {quoted_schema}.ptg_import_wave_supersession"
        )
        await _admit_v5(connection, schema, descriptor, "stale-successor")
        await _insert_unrelated_fhir_run(stale_connection, quoted_schema)
        with pytest.raises(
            asyncpg.PostgresError,
            match="WRITE_ISOLATION_UNSUPPORTED",
        ):
            await stale_connection.execute(
                write_statement.format(schema=quoted_schema)
            )
    finally:
        if is_stale_transaction_active:
            await stale_transaction.rollback()
        await connection.execute(
            f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE"
        )
        await stale_connection.close()
        await connection.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("progress_statement", _UNRETIRED_PROGRESS_STATEMENTS)
async def test_v5_stale_repeatable_read_snapshot_cannot_retire_new_progress(
    monkeypatch,
    progress_statement,
):
    """Reject a V5 writer whose snapshot predates committed PTG progress."""

    statement_digest = hashlib.sha256(progress_statement.encode()).hexdigest()
    schema = "wave_recovery_materialized_stale_writer_" + statement_digest[:8]
    connection = await asyncpg.connect(_dsn())
    stale_connection = await asyncpg.connect(_dsn())
    stale_transaction = stale_connection.transaction(
        isolation="repeatable_read",
    )
    is_stale_transaction_active = False
    try:
        await _install_migration(connection, monkeypatch, schema)
        descriptor = await seed_materialized_predecessor(connection, schema)
        evidence, canonical = materialized_evidence(
            descriptor,
            "stale-writer-successor",
        )
        quoted_schema = _quote(schema)
        await stale_transaction.start()
        is_stale_transaction_active = True
        await stale_connection.fetchval(
            f"SELECT count(*) FROM {quoted_schema}.ptg_import_wave_supersession"
        )
        await connection.execute(
            progress_statement.format(schema=quoted_schema)
        )
        with pytest.raises(
            asyncpg.PostgresError,
            match="WRITE_ISOLATION_UNSUPPORTED",
        ):
            await insert_materialized_supersession(
                stale_connection,
                schema,
                descriptor["wave_id"],
                "stale-writer-successor",
                evidence,
                canonical,
            )
    finally:
        if is_stale_transaction_active:
            await stale_transaction.rollback()
        await connection.execute(
            f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE"
        )
        await stale_connection.close()
        await connection.close()


@pytest.mark.asyncio
async def test_v5_guards_preserve_unrelated_legacy_repeatable_read_writes(
    monkeypatch,
):
    """Keep non-boundary V1-V4 PTG writes compatible at prior isolation."""

    schema = "wave_recovery_materialized_legacy_isolation"
    connection = await asyncpg.connect(_dsn())
    legacy_connection = await asyncpg.connect(_dsn())
    legacy_transaction = legacy_connection.transaction(
        isolation="repeatable_read",
    )
    is_legacy_transaction_active = False
    try:
        await _install_migration(connection, monkeypatch, schema)
        quoted_schema = _quote(schema)
        await legacy_transaction.start()
        is_legacy_transaction_active = True
        await legacy_connection.execute(
            f"UPDATE {quoted_schema}.import_run "
            "SET started_at = clock_timestamp() WHERE run_id = 'run-1'"
        )
        await legacy_connection.execute(
            f"INSERT INTO {quoted_schema}.ptg_import_wave_claim (wave_id) "
            "VALUES ('predecessor-wave')"
        )
        await legacy_connection.execute(
            f"INSERT INTO {quoted_schema}.ptg_source_attempt_event "
            "(outer_run_id, event_kind) VALUES "
            "('run-1', 'worker_start_admitted')"
        )
        await legacy_transaction.commit()
        is_legacy_transaction_active = False
        assert await connection.fetchval(
            f"SELECT count(*) FROM {quoted_schema}.import_run "
            "WHERE run_id = 'run-1' AND started_at IS NOT NULL"
        ) == 1
        assert await connection.fetchval(
            f"SELECT count(*) FROM {quoted_schema}.ptg_import_wave_claim "
            "WHERE wave_id = 'predecessor-wave'"
        ) == 1
        assert await connection.fetchval(
            f"SELECT count(*) FROM {quoted_schema}.ptg_source_attempt_event "
            "WHERE outer_run_id = 'run-1'"
        ) == 1
    finally:
        if is_legacy_transaction_active:
            await legacy_transaction.rollback()
        await connection.execute(
            f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE"
        )
        await legacy_connection.close()
        await connection.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("write_statement", _STALE_SNAPSHOT_WRITE_STATEMENTS)
async def test_v5_ineligible_v4_boundary_preserves_repeatable_read_writes(
    monkeypatch,
    write_statement,
):
    """Do not constrain V4 rows that cannot pass the exact V5 boundary."""

    statement_digest = hashlib.sha256(write_statement.encode()).hexdigest()
    schema = "wave_recovery_materialized_ineligible_" + statement_digest[:8]
    connection = await asyncpg.connect(_dsn())
    legacy_connection = await asyncpg.connect(_dsn())
    legacy_transaction = legacy_connection.transaction(
        isolation="repeatable_read",
    )
    is_legacy_transaction_active = False
    try:
        await _install_migration(connection, monkeypatch, schema)
        await seed_materialized_predecessor(connection, schema)
        quoted_schema = _quote(schema)
        await connection.execute(
            f"UPDATE {quoted_schema}.ptg_import_wave "
            "SET k8s_post_ticket = NULL, k8s_post_started_at = NULL "
            "WHERE wave_id = 'materialized-wave'"
        )
        await legacy_transaction.start()
        is_legacy_transaction_active = True
        write_result = await legacy_connection.execute(
            write_statement.format(schema=quoted_schema)
        )
        assert write_result.endswith("1")
        await legacy_transaction.commit()
        is_legacy_transaction_active = False
    finally:
        if is_legacy_transaction_active:
            await legacy_transaction.rollback()
        await connection.execute(
            f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE"
        )
        await legacy_connection.close()
        await connection.close()
