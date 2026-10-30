# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Metadata-only reads use genuine published captures and native readonly callers."""

import asyncio
import json
import os
from contextlib import asynccontextmanager, suppress
from dataclasses import replace
from unittest.mock import AsyncMock, Mock

import asyncpg
import pytest

from api import provider_directory_retained_fhir_metadata as metadata
from api.provider_directory_retained_fhir_metadata import verify_retained_fhir_metadata
from process.network_fhir_retained_capture_contract import TABLES, RetainedFHIRCaptureError, capture_digest
from process.network_fhir_source_epoch import _catalog_relations
from tests.generic_fhir_capture_postgres_support import _capture, legacy_capture_db
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
        yield
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
        async with _metadata_wait_observer(fixture, reader):
            await asyncio.wait_for(reached_limit.wait(), timeout=0.5)
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
        "WHERE l.pid=pg_catalog.pg_backend_pid() AND l.locktype='relation' AND n.oid=$1::oid",
        capture.schema_oid,
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
        identity = await reader.fetchrow("SELECT pg_backend_pid(),transaction_timestamp(),session_user,current_user")
        assert await verify_retained_fhir_metadata(reader, pin(capture)) == capture
        assert (
            await reader.fetchrow("SELECT pg_backend_pid(),transaction_timestamp(),session_user,current_user")
            == identity
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
async def test_raw_catalog_preserves_original_native_receipt_bytes(legacy_capture_db):
    fixture = legacy_capture_db
    capture = await _capture(fixture)
    reader = await fixture.connect("reader")
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        await reader.execute("SET LOCAL search_path TO pg_catalog")
        rebuilt = await metadata._catalog_relations_metadata(reader, capture.schema_name)
        original = await _catalog_relations(reader, capture.schema_name)
        assert rebuilt == [dict(relation) for relation in original]


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
                assert refused.value.__cause__ is None
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
        "SELECT pg_backend_pid(),transaction_timestamp(),session_user,current_user,"
        "current_setting('jit') AS jit,current_setting('search_path') AS path,"
        "current_setting('statement_timeout') AS timeout,"
        "(SELECT virtualxid FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='virtualxid' AND granted) AS xid,"
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
        cause_by_outcome = {"database_error": asyncpg.DivisionByZeroError, "timeout": asyncpg.QueryCanceledError}
        assert type(refused.value.__cause__) is cause_by_outcome.get(outcome, type(None))


@pytest.mark.parametrize("caller_jit", ("on", "off"))
@pytest.mark.parametrize("outcome", ("success", "refusal", "database_error", "timeout", "cancel"))
async def test_metadata_restores_caller_settings_identity_and_prior_locks(
    legacy_capture_db, monkeypatch, caller_jit, outcome
):
    fixture = legacy_capture_db
    capture = await _capture(fixture)
    if outcome == "refusal":
        await fixture.admin.execute(
            f'ALTER TABLE "{capture.schema_name}"."{TABLES[1]}" ADD CONSTRAINT unexpected CHECK (resource_id<>\'never\') NOT VALID'
        )
    original, entered = metadata._catalog_relations_metadata, asyncio.Event()

    async def inspect_settings(connection, schema):
        assert await connection.fetchval("SHOW jit") == "off"
        entered.set()
        if outcome == "database_error":
            await connection.execute("SELECT 1/0")
        elif outcome in {"timeout", "cancel"}:
            await connection.execute("SELECT pg_sleep(5)")
        return await original(connection, schema)

    monkeypatch.setattr(metadata, "_catalog_relations_metadata", inspect_settings)
    reader = await fixture.connect("reader")
    async with fixture.admin.transaction():
        await fixture.admin.execute(f'LOCK TABLE "{capture.schema_name}"."{TABLES[1]}" IN ACCESS EXCLUSIVE MODE NOWAIT')
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            await reader.execute(f"SET LOCAL jit={caller_jit}")
            await reader.execute(f'SET LOCAL search_path TO "{fixture.schema}",pg_catalog')
            await reader.execute("SET LOCAL statement_timeout='1000ms'")
            await reader.fetchval("SELECT pg_advisory_xact_lock($1::int,$2::int)", capture.schema_oid % 2147483647, 1)
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


async def test_publisher_cannot_pose_as_capture_api_reader(legacy_capture_db):
    fixture = legacy_capture_db
    capture = await _capture(fixture)
    publisher = await fixture.connect("publisher")
    async with publisher.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(RetainedFHIRCaptureError):
            await verify_retained_fhir_metadata(publisher, pin(capture))
        await _no_capture_locks(publisher, capture)


async def test_origin_graft_and_sibling_capture_oids_do_not_authenticate(legacy_capture_db):
    fixture = legacy_capture_db
    capture, sibling = await _capture(fixture), await _capture(fixture)
    forged_origin = replace(capture.origin, endpoint_id="different-endpoint")
    grafted = replace(capture, origin=forged_origin)
    grafted = replace(grafted, capture_sha256=capture_digest(grafted))
    sibling_oids = replace(capture, relation_oids=sibling.relation_oids)
    sibling_oids = replace(sibling_oids, capture_sha256=capture_digest(sibling_oids))
    reader = await fixture.connect("reader")
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        for forged in (grafted, sibling_oids):
            with pytest.raises(RetainedFHIRCaptureError):
                await verify_retained_fhir_metadata(reader, pin(forged))
        assert await verify_retained_fhir_metadata(reader, pin(sibling)) == sibling
        await _no_capture_locks(reader, capture)
        await _no_capture_locks(reader, sibling)


@pytest.mark.parametrize("drift", ["comment", "write_grant", "owner_reach", "index", "shape", "owner_login", "storage"])
async def test_native_metadata_drift_is_refused(legacy_capture_db, drift):
    fixture = legacy_capture_db
    capture = await _capture(fixture)
    schema, reader_role, owner = capture.schema_name, fixture.role_by_kind["reader"], fixture.role_by_kind["owner"]
    statement_by_drift = {
        "comment": f"COMMENT ON SCHEMA \"{schema}\" IS 'changed-receipt'",
        "write_grant": f'GRANT UPDATE ON "{schema}"."{TABLES[1]}" TO "{reader_role}"',
        "owner_reach": f'GRANT "{owner}" TO "{reader_role}" WITH INHERIT FALSE, SET TRUE',
        "index": f'DROP INDEX "{schema}".retained_lookup_1_0',
        "shape": f'ALTER TABLE "{schema}"."{TABLES[1]}" ADD COLUMN unexpected integer',
        "owner_login": f'ALTER ROLE "{owner}" LOGIN',
        "storage": f'ALTER INDEX "{schema}".retained_lookup_1_0 SET (fillfactor=90)',
    }
    await fixture.admin.execute(statement_by_drift[drift])
    reader = await fixture.connect("reader")
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(RetainedFHIRCaptureError, match="^retained_fhir_capture_unavailable$"):
            await verify_retained_fhir_metadata(reader, pin(capture))
        await _no_capture_locks(reader, capture)


async def test_unpinned_native_definer_escape_is_refused(legacy_capture_db):
    fixture = legacy_capture_db
    capture = await _capture(fixture)
    schema = "capture_escape_" + capture.capture_id.replace("-", "")
    fixture.captures.append(schema)
    owner, reader_role = fixture.role_by_kind["owner"], fixture.role_by_kind["reader"]
    await fixture.admin.execute(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{owner}"')
    await fixture.admin.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{reader_role}"')
    await fixture.admin.execute(
        f"CREATE FUNCTION \"{schema}\".escape() RETURNS integer LANGUAGE sql SECURITY DEFINER AS 'SELECT 1'"
    )
    await fixture.admin.execute(f'ALTER FUNCTION "{schema}".escape() OWNER TO "{owner}"')
    reader = await fixture.connect("reader")
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(RetainedFHIRCaptureError):
            await verify_retained_fhir_metadata(reader, pin(capture))
        await _no_capture_locks(reader, capture)
