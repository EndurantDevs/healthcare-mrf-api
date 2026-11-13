# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Metadata-only reads use genuine published captures and native readonly callers."""

import asyncio
import importlib
import json
import os
from contextlib import asynccontextmanager, suppress
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import asyncpg
import pytest
from sqlalchemy import text

from api import provider_directory_retained_fhir_metadata as metadata
from api.provider_directory_retained_fhir_metadata import verify_retained_fhir_metadata
from process import provider_directory_source_checkpoint_control as control
from process.network_fhir_retained_capture_contract import TABLES, RetainedFHIRCaptureError, capture_digest
from process.registry_execution_catalog import RegistryExecutionCatalogError
from tests.generic_fhir_capture_postgres_support import _capture
from tests.generic_fhir_capture_postgres_support import legacy_capture_db as legacy_capture_db
from tests.test_network_fhir_retained_capture import pin

_WAIT_SQL = """WITH activity AS MATERIALIZED (
 SELECT wait_event_type,wait_event,pg_catalog.pg_blocking_pids(pid) AS blockers
 FROM pg_catalog.pg_stat_activity WHERE pid=$1
)
SELECT wait_event_type,wait_event,$2::int=ANY(blockers) AS fixture_admin_is_blocker,
 pg_catalog.cardinality(pg_catalog.array_remove(blockers,$2::int)) AS other_blocker_count
FROM activity"""


@asynccontextmanager
async def _metadata_wait_observer(fixture, reader):
    """Keep bounded catalog-only wait evidence on the original test failure."""
    observers, failure = [], None
    diagnostic_by_field = {"samples": []}
    started = asyncio.get_running_loop().time()

    async def sample():
        try:
            async with asyncio.timeout(1.5):
                observer = await asyncpg.connect(
                    os.environ["NETWORK_REGISTRY_TEST_DSN"].replace("postgresql+asyncpg://", "postgresql://"),
                    timeout=0.25,
                    server_settings={"statement_timeout": "100ms"},
                )
                observers.append(observer)
                for _ in range(24):
                    row = await observer.fetchrow(_WAIT_SQL, reader.get_server_pid(), fixture.admin.get_server_pid())
                    if row is not None:
                        diagnostic_by_field["samples"].append(
                            {
                                "elapsed_ms": round((asyncio.get_running_loop().time() - started) * 1000, 3),
                                "wait_type": row["wait_event_type"],
                                "event": row["wait_event"],
                                "fixture_admin_is_blocker": row["fixture_admin_is_blocker"],
                                "other_blocker_count": row["other_blocker_count"],
                            }
                        )
                    await asyncio.sleep(0.05)
        except Exception as error:
            diagnostic_by_field["observer_error"] = type(error).__name__

    task = asyncio.create_task(sample())
    try:
        yield task
    except BaseException as error:
        failure = error
        raise
    finally:
        task.cancel()
        with suppress(BaseException):
            await asyncio.wait_for(task, timeout=0.2)
        for observer in observers:
            try:
                await asyncio.wait_for(observer.close(timeout=0.1), timeout=0.2)
            except BaseException as error:
                diagnostic_by_field["observer_close_error"] = type(error).__name__
                with suppress(BaseException):
                    observer.terminate()
        if failure is not None:
            failure.add_note("metadata_wait_observer=" + json.dumps(diagnostic_by_field, separators=(",", ":")))


async def test_wait_observer_bounds_sanitizes_and_preserves_failure_when_close_fails(monkeypatch):
    reached_limit, original_sleep = asyncio.Event(), asyncio.sleep
    observer, fixture, reader = Mock(), Mock(), Mock()
    observer.close = AsyncMock(side_effect=RuntimeError("unreported close detail"))
    fixture.admin.get_server_pid.return_value, reader.get_server_pid.return_value = 101, 102

    async def fetch(*_):
        if observer.fetchrow.await_count == 24:
            reached_limit.set()
        return {
            "wait_event_type": "Lock",
            "wait_event": "relation",
            "fixture_admin_is_blocker": True,
            "other_blocker_count": 2,
            "unreported": "must stay out",
        }

    async def yield_without_delay(_):
        await original_sleep(0)

    observer.fetchrow = AsyncMock(side_effect=fetch)
    connect = AsyncMock(return_value=observer)
    monkeypatch.setattr(asyncpg, "connect", connect)
    monkeypatch.setattr(asyncio, "sleep", yield_without_delay)
    monkeypatch.setenv("NETWORK_REGISTRY_TEST_DSN", "postgresql+asyncpg://example.invalid/diagnostic")
    failure, cause = RuntimeError("original failure"), ValueError("original cause")
    with pytest.raises(RuntimeError) as observed:
        async with _metadata_wait_observer(fixture, reader) as sampling_task:
            await sampling_task
            assert reached_limit.is_set()
            raise failure from cause
    assert observed.value is failure and failure.__cause__ is cause
    diagnostic_by_field = json.loads(failure.__notes__[0].split("=", 1)[1])
    assert len(diagnostic_by_field["samples"]) == observer.fetchrow.await_count == 24
    assert all(
        set(sample) == {"elapsed_ms", "wait_type", "event", "fixture_admin_is_blocker", "other_blocker_count"}
        and sample["fixture_admin_is_blocker"] is True
        and sample["other_blocker_count"] == 2
        for sample in diagnostic_by_field["samples"]
    )
    assert diagnostic_by_field["observer_close_error"] == "RuntimeError"
    assert "unreported" not in failure.__notes__[0] and "example.invalid" not in failure.__notes__[0]
    assert connect.call_args.kwargs == {"timeout": 0.25, "server_settings": {"statement_timeout": "100ms"}}
    observer.close.assert_awaited_once_with(timeout=0.1)
    observer.terminate.assert_called_once_with()


async def _no_capture_locks(reader, capture):
    locks = await reader.fetch(
        "SELECT c.relname,l.mode FROM pg_catalog.pg_locks l "
        "JOIN pg_catalog.pg_class c ON c.oid=l.relation "
        "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
        "WHERE l.pid=pg_catalog.pg_backend_pid() AND l.locktype='relation' AND n.nspname=$1",
        capture.schema_name,
    )
    assert locks == []


@pytest.mark.parametrize("legacy_capture_db", ["legacy", "modern"], indirect=True)
@pytest.mark.parametrize("isolation", ["repeatable_read", "serializable"])
async def test_genuine_capture_metadata_preserves_original_readonly_snapshot_without_heap_locks(
    legacy_capture_db,
    isolation,
):
    fixture = legacy_capture_db
    capture = await _capture(fixture)
    reader = await fixture.connect("reader")
    async with reader.transaction(isolation=isolation, readonly=True):
        snapshot = await reader.fetchrow(
            "SELECT transaction_timestamp(),current_setting('transaction_isolation'),current_setting('transaction_read_only')"
        )
        assert await verify_retained_fhir_metadata(reader, pin(capture)) == capture
        assert (
            await reader.fetchrow(
                "SELECT transaction_timestamp(),current_setting('transaction_isolation'),current_setting('transaction_read_only')"
            )
            == snapshot
        )
        await _no_capture_locks(reader, capture)


async def test_metadata_does_not_acquire_even_transient_locks_on_capture_heaps(legacy_capture_db):
    fixture = legacy_capture_db
    capture = await _capture(fixture)
    reader = await fixture.connect("reader")
    async with fixture.admin.transaction():
        await fixture.admin.execute(f'LOCK TABLE "{capture.schema_name}"."{TABLES[1]}" IN ACCESS EXCLUSIVE MODE NOWAIT')
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            await reader.execute("SET LOCAL statement_timeout='1000ms'")
            async with _metadata_wait_observer(fixture, reader):
                assert await verify_retained_fhir_metadata(reader, pin(capture)) == capture
                await _no_capture_locks(reader, capture)


@pytest.mark.parametrize("legacy_capture_db", ["legacy", "modern"], indirect=True)
async def test_named_catalog_preserves_original_logical_receipt_and_content(legacy_capture_db):
    fixture = legacy_capture_db
    capture = await _capture(fixture)
    reader = await fixture.connect("reader")
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        await reader.execute("SET LOCAL search_path TO pg_catalog")
        from process.network_fhir_retained_capture import require_retained_fhir_capture

        rows = await reader.fetch(metadata._CATALOG_SQL, capture.schema_name)
        metadata._require_retained_fhir_relations([dict(row) for row in rows], capture.schema_name)
        assert tuple(row["relname"] for row in rows) == tuple(sorted(TABLES))
        assert sum(len(json.loads(row["indexes"])) for row in rows) == 9
        assert await verify_retained_fhir_metadata(reader, pin(capture)) == capture
        assert await require_retained_fhir_capture(reader, capture) == capture


async def test_extra_check_is_refused_without_deparsing_or_locking_its_heap(legacy_capture_db):
    fixture = legacy_capture_db
    capture = await _capture(fixture)
    await fixture.admin.execute(
        f'ALTER TABLE "{capture.schema_name}"."{TABLES[1]}" ADD CONSTRAINT unexpected CHECK (resource_id<>\'never\') NOT VALID'
    )
    reader = await fixture.connect("reader")
    async with fixture.admin.transaction():
        await fixture.admin.execute(f'LOCK TABLE "{capture.schema_name}"."{TABLES[1]}" IN ACCESS EXCLUSIVE MODE NOWAIT')
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            await reader.execute("SET LOCAL statement_timeout='1000ms'")
            async with _metadata_wait_observer(fixture, reader):
                with pytest.raises(RetainedFHIRCaptureError) as refused:
                    await verify_retained_fhir_metadata(reader, pin(capture))
                assert isinstance(refused.value.__cause__, RegistryExecutionCatalogError)
                await _no_capture_locks(reader, capture)


async def test_untrusted_search_path_cannot_replace_native_types_and_is_restored(legacy_capture_db):
    fixture = legacy_capture_db
    capture = await _capture(fixture)
    await fixture.admin.execute(f'CREATE DOMAIN "{fixture.schema}".text AS pg_catalog.text CHECK (false)')
    reader = await fixture.connect("reader")
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        await reader.execute(f'SET LOCAL search_path TO "{fixture.schema}",pg_catalog')
        original_path = await reader.fetchval("SHOW search_path")
        assert await verify_retained_fhir_metadata(reader, pin(capture)) == capture
        assert await reader.fetchval("SHOW search_path") == original_path
        await _no_capture_locks(reader, capture)


async def _metadata_caller_state(reader):
    return await reader.fetchrow(
        "SELECT transaction_timestamp(),current_setting('transaction_isolation') AS isolation,"
        "current_setting('transaction_read_only') AS read_only,"
        "current_setting('jit') AS jit,current_setting('search_path') AS path,"
        "current_setting('statement_timeout') AS timeout,"
        "(SELECT count(*) FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory' AND granted) AS prior_locks"
    )


async def _verify_metadata_outcome(reader, capture, outcome, entered):
    """Exercise each verifier exit while retaining the caller's live transaction."""
    if outcome == "success":
        assert await verify_retained_fhir_metadata(reader, pin(capture)) == capture
    elif outcome == "cancel":
        task = asyncio.create_task(verify_retained_fhir_metadata(reader, pin(capture)))
        await asyncio.wait_for(entered.wait(), timeout=0.5)
        await asyncio.sleep(0.02)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        with pytest.raises(RetainedFHIRCaptureError) as refused:
            await verify_retained_fhir_metadata(reader, pin(capture))
        cause_by_outcome = {
            "refusal": RegistryExecutionCatalogError,
            "database_error": asyncpg.DivisionByZeroError,
            "timeout": asyncpg.QueryCanceledError,
        }
        assert type(refused.value.__cause__) is cause_by_outcome.get(outcome, type(None))


@pytest.mark.parametrize("caller_jit", ("on", "off"))
@pytest.mark.parametrize("outcome", ("success", "refusal", "database_error", "timeout", "cancel"))
async def test_metadata_restores_caller_settings_snapshot_and_prior_locks(
    legacy_capture_db, monkeypatch, caller_jit, outcome
):
    fixture = legacy_capture_db
    capture = await _capture(fixture)
    if outcome == "refusal":
        await fixture.admin.execute(
            f'ALTER TABLE "{capture.schema_name}"."{TABLES[1]}" ADD CONSTRAINT unexpected CHECK (resource_id<>\'never\') NOT VALID'
        )
    original, entered = metadata._catalog_metadata, asyncio.Event()

    async def inspect_settings(connection, selected_capture):
        assert await connection.fetchval("SHOW jit") == "off"
        entered.set()
        if outcome == "database_error":
            await connection.execute("SELECT 1/0")
        elif outcome in {"timeout", "cancel"}:
            await connection.execute("SELECT pg_sleep(5)")
        return await original(connection, selected_capture)

    monkeypatch.setattr(metadata, "_catalog_metadata", inspect_settings)
    reader = await fixture.connect("reader")
    async with fixture.admin.transaction():
        await fixture.admin.execute(f'LOCK TABLE "{capture.schema_name}"."{TABLES[1]}" IN ACCESS EXCLUSIVE MODE NOWAIT')
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            await reader.execute(f"SET LOCAL jit={caller_jit}")
            await reader.execute(f'SET LOCAL search_path TO "{fixture.schema}",pg_catalog')
            await reader.execute("SET LOCAL statement_timeout='1000ms'")
            await reader.fetchval("SELECT pg_advisory_xact_lock(hashtextextended($1,0))", capture.schema_name)
            previous = await _metadata_caller_state(reader)
            await _verify_metadata_outcome(reader, capture, outcome, entered)
            assert await _metadata_caller_state(reader) == previous
            assert previous["prior_locks"] == 1
            await _no_capture_locks(reader, capture)


@pytest.mark.parametrize(
    "isolation,readonly",
    [
        ("read_committed", True),
        ("repeatable_read", False),
        ("serializable", False),
    ],
)
async def test_native_readonly_repeatable_snapshot_is_mandatory(legacy_capture_db, isolation, readonly):
    fixture = legacy_capture_db
    capture = await _capture(fixture)
    reader = await fixture.connect("reader")
    async with reader.transaction(isolation=isolation, readonly=readonly):
        with pytest.raises(RetainedFHIRCaptureError):
            await verify_retained_fhir_metadata(reader, pin(capture))
        await _no_capture_locks(reader, capture)
    with pytest.raises(RetainedFHIRCaptureError):
        await verify_retained_fhir_metadata(reader, pin(capture))


async def _assert_write_denied(connection, statement):
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        async with connection.transaction():
            await connection.execute(statement)


async def test_published_capture_denies_application_dml(legacy_capture_db):
    from process.network_fhir_retained_capture import require_retained_fhir_capture

    fixture = legacy_capture_db
    capture = await _capture(fixture)
    for role in ("reader", "publisher"):
        application = await fixture.connect(role)
        async with application.transaction(isolation="repeatable_read"):
            for table in TABLES:
                key = "dataset_id" if table in TABLES[:2] else "source_id"
                for operation in (
                    f'INSERT INTO "{capture.schema_name}"."{table}" DEFAULT VALUES',
                    f'UPDATE "{capture.schema_name}"."{table}" SET "{key}"="{key}" WHERE false',
                    f'DELETE FROM "{capture.schema_name}"."{table}" WHERE false',
                    f'TRUNCATE "{capture.schema_name}"."{table}"',
                ):
                    await _assert_write_denied(application, operation)
        async with application.transaction(isolation="repeatable_read", readonly=True):
            assert await verify_retained_fhir_metadata(application, pin(capture)) == capture
            assert await require_retained_fhir_capture(application, capture) == capture


async def test_origin_graft_and_sibling_named_tables_cannot_replace_the_selected_capture(legacy_capture_db):
    fixture = legacy_capture_db
    capture, sibling = await _capture(fixture), await _capture(fixture)
    forged_origin = replace(capture.origin, endpoint_id="different-endpoint")
    grafted = replace(capture, origin=forged_origin)
    grafted = replace(grafted, capture_sha256=capture_digest(grafted))
    sibling_tables = replace(capture, tables=sibling.tables)
    sibling_tables = replace(sibling_tables, capture_sha256=capture_digest(sibling_tables))
    reader = await fixture.connect("reader")
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        for forged in (grafted, sibling_tables):
            with pytest.raises(RetainedFHIRCaptureError):
                await verify_retained_fhir_metadata(reader, pin(forged))
        assert await verify_retained_fhir_metadata(reader, pin(sibling)) == sibling
        await _no_capture_locks(reader, capture)
        await _no_capture_locks(reader, sibling)


@pytest.mark.parametrize("drift", ["comment", "index", "shape", "storage"])
async def test_native_metadata_drift_is_refused(legacy_capture_db, drift):
    fixture = legacy_capture_db
    capture = await _capture(fixture)
    schema = capture.schema_name
    statement_by_drift = {
        "comment": f"COMMENT ON SCHEMA \"{schema}\" IS 'changed-receipt'",
        "index": f'DROP INDEX "{schema}".retained_lookup_1_0',
        "shape": f'ALTER TABLE "{schema}"."{TABLES[1]}" ADD COLUMN unexpected integer',
        "storage": f'ALTER INDEX "{schema}".retained_lookup_1_0 SET (fillfactor=90)',
    }
    await fixture.admin.execute(statement_by_drift[drift])
    reader = await fixture.connect("reader")
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(RetainedFHIRCaptureError, match="^retained_fhir_capture_unavailable$"):
            await verify_retained_fhir_metadata(reader, pin(capture))
        await _no_capture_locks(reader, capture)


@pytest.mark.asyncio
async def test_native_transient_file_failure_preserves_original_guard_query(legacy_capture_db, monkeypatch):
    """A sibling transport error must not cancel the shared session's active query."""
    directory = importlib.import_module("process.provider_directory_fhir")
    engine = legacy_capture_db.session_factory("publisher").kw["bind"]
    identity = SimpleNamespace(checkpoint_id="bulk-file-guard")
    async with engine.connect() as original:
        connection = await original.execution_options(isolation_level="AUTOCOMMIT")
        pid = await connection.scalar(text("SELECT pg_backend_pid()"))
        lock_arguments_by_name = {"id": identity.checkpoint_id}
        assert await connection.scalar(
            text("SELECT pg_try_advisory_lock(hashtextextended(:id,0))"), lock_arguments_by_name
        )

        async def owner():
            await control.assert_bulk_checkpoint_guard_live(directory, connection, identity)

        async def stream(output):
            if output == 0:
                async with asyncio.timeout(2):
                    while (
                        await legacy_capture_db.admin.fetchval(
                            "SELECT wait_event FROM pg_stat_activity WHERE pid=$1", pid
                        )
                        != "PgSleep"
                    ):
                        await asyncio.sleep(0.001)
                return "bulk_export_transport_clientpayloaderror"
            await connection.scalar(text("SELECT pg_sleep(0.3)"))
            await owner()
            return None

        record_error = AsyncMock()
        monkeypatch.setattr(directory, "_record_bulk_export_checkpoint_error", record_error)
        options = SimpleNamespace(output_concurrency=2, ownership_probe=owner, range_resume_enabled=True)
        _, error = await directory._run_pending_bulk_output_tasks(identity, options, object(), [0, 1], stream)
        assert error == "bulk_export_transport_clientpayloaderror"
        record_error.assert_awaited_once_with(identity, error, terminal=False)
        assert not connection.invalidated
        assert await connection.scalar(text("SELECT pg_backend_pid()")) == pid
        await owner()
        assert await connection.scalar(
            text("SELECT pg_advisory_unlock(hashtextextended(:id,0))"), lock_arguments_by_name
        )
        with pytest.raises(RuntimeError, match="bulk_export_checkpoint_worker_guard_lost"):
            await owner()
