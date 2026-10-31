# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native row-restore freezes admit readers and exclude other writers through commit."""

import asyncio
from contextlib import asynccontextmanager

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process import provider_directory_common_serving_recovery as common
from process import provider_directory_serving_recovery_receipts as receipts
from process.entity_address_cutover_contract import postgres_sqlstate
from tests.provider_directory_serving_recovery_postgres_support import ordinary_serving_case


async def _inverse_lock_document(case):
    for family in ("profile", "evidence"):
        table = receipts.qualified(case.schema, common.journal.documents.FAMILIES[family])
        await case.database.status(f"CREATE TABLE {table} (marker text PRIMARY KEY)")
        await case.database.status(f"INSERT INTO {table} VALUES ('old')")
    for name in ("retained_rows", "retained_scope"):
        table = receipts.qualified(case.schema, name)
        await case.database.status(f"CREATE TABLE {table} (marker text PRIMARY KEY)")
        await case.database.status(f"INSERT INTO {table} VALUES ('old')")
    return {
        "preparation": {
            "retention": {
                "families": [{"name": "retained_rows"}],
                "scopes": [{"name": "retained_scope"}],
            }
        }
    }


async def _reader_snapshot(case, table, owner_pid, *, locked=None, release=None):
    async with case.database.engine.begin() as reader:
        await reader.execute(text("SET TRANSACTION READ ONLY"))
        await reader.execute(text("SET LOCAL lock_timeout='100ms'"))
        assert await reader.scalar(text("SELECT pg_backend_pid()")) != owner_pid
        assert await reader.scalar(text("SELECT session_user=current_user AND current_user=:role"), {"role": case.role})
        assert await reader.scalar(text(f"SELECT marker FROM {table}")) == "old"
        assert await reader.scalar(
            text(
                "SELECT EXISTS(SELECT FROM pg_locks WHERE pid=pg_backend_pid() "
                "AND relation=to_regclass(:name) AND mode='AccessShareLock' AND granted)"
            ),
            {"name": table},
        )
        if locked is not None:
            locked.set()
            await release.wait()


async def _competing_statement(case, statement):
    with pytest.raises(DBAPIError) as blocked:
        async with case.database.engine.begin() as writer:
            await writer.execute(text("SET LOCAL lock_timeout='100ms'"))
            await writer.execute(text(statement))
    assert postgres_sqlstate(blocked.value) == "55P03"


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["update", "insert", "ddl"])
async def test_native_inverse_freeze_preserves_reads(monkeypatch, operation):
    async with ordinary_serving_case(monkeypatch) as case:
        document = await _inverse_lock_document(case)
        table = receipts.qualified(case.schema, "entity_address_unified")
        statement = {
            "update": f"UPDATE {table} SET marker='other_writer'",
            "insert": f"INSERT INTO {table} VALUES ('new_key')",
            "ddl": f"ALTER TABLE {table} ADD COLUMN other_writer text",
        }[operation]
        async with case.database.transaction():
            frozen = await common._lock_inputs(case.directory, document)
            assert frozen == set(common.journal.documents.FAMILIES.values()) | {"retained_rows", "retained_scope"}
            for name in frozen:
                assert await case.database.scalar(
                    "SELECT EXISTS(SELECT FROM pg_locks WHERE pid=pg_backend_pid() "
                    "AND relation=to_regclass(:name) AND mode='ShareLock' AND granted)",
                    name=receipts.qualified(case.schema, name),
                )
            _session, _driver, actor = await receipts.ordinary(case.database)
            assert actor["current"] == actor["session"] == case.role
            await case.database.status(f"UPDATE {table} SET marker='restored'")
            await _reader_snapshot(case, table, actor["backend"])
            await _competing_statement(case, statement)
            assert await case.database.scalar(f"SELECT marker FROM {table}") == "restored"
        assert await case.database.scalar(f"SELECT marker FROM {table}") == "restored"
        async with case.database.engine.begin() as writer:
            await writer.execute(text(statement))


async def _initial_lock_document(case):
    document = await _inverse_lock_document(case)
    renamed_names = set()
    for family in ("profile", "evidence"):
        root = common.journal.documents.FAMILIES[family]
        retained = "retained_" + family
        table = receipts.qualified(case.schema, retained)
        await case.database.status(f"CREATE TABLE {table} (marker text PRIMARY KEY)")
        await case.database.status(f"INSERT INTO {table} VALUES ('old')")
        document["preparation"]["retention"]["families"].append({"name": retained})
        renamed_names.update((root, retained))
    return document, renamed_names


@asynccontextmanager
async def _pinned_reader(case, table, owner_pid):
    locked, release = asyncio.Event(), asyncio.Event()
    reader = asyncio.create_task(_reader_snapshot(case, table, owner_pid, locked=locked, release=release))
    try:
        await asyncio.wait_for(locked.wait(), 2)
        yield release
    finally:
        release.set()
        try:
            await asyncio.wait_for(reader, 2)
        finally:
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)


async def _release_initial_reader(case, owner_pid, release):
    async def wait_for_owner():
        async with case.database.engine.connect() as observer:
            while not await observer.scalar(
                text(
                    "SELECT EXISTS(SELECT FROM pg_locks WHERE pid=:pid AND mode='AccessExclusiveLock' AND NOT granted)"
                ),
                {"pid": owner_pid},
            ):
                await asyncio.sleep(0.005)

    await asyncio.wait_for(wait_for_owner(), 1)
    await asyncio.sleep(0.15)
    release.set()


async def _cutover_limits(case):
    # Match the existing signed primitive limits; this fixture does not issue a lease.
    await case.database.status("SET LOCAL lock_timeout='500ms'")
    await case.database.status("SET LOCAL statement_timeout='1000ms'")


async def _assert_initial_lock_scope(case, frozen, renamed_names):
    modes = await case.database.all(
        "SELECT relation.relname,locked.mode FROM pg_locks locked "
        "JOIN pg_class relation ON relation.oid=locked.relation "
        "JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace "
        "WHERE locked.pid=pg_backend_pid() AND locked.granted AND namespace.nspname=:schema",
        schema=case.schema,
    )
    assert {name for name, mode in modes if mode == "AccessExclusiveLock"} == renamed_names
    assert {name for name, mode in modes if mode == "ShareLock"} >= frozen
    unrelated = receipts.qualified(case.schema, "entity_address_unified")
    _session, _driver, actor = await receipts.ordinary(case.database)
    await _reader_snapshot(case, unrelated, actor["backend"])
    await _competing_statement(case, f"UPDATE {unrelated} SET marker='other_writer'")


@pytest.mark.asyncio
async def test_initial_inverse_lock_waits_within_existing_cutover_limit(monkeypatch):
    """The real lock primitive waits briefly on the original restricted Builder transaction."""
    async with ordinary_serving_case(monkeypatch) as case:
        document, renamed_names = await _initial_lock_document(case)
        table = receipts.qualified(case.schema, common.journal.documents.FAMILIES["profile"])
        async with case.database.transaction() as session:
            frozen = await common._lock_inputs(case.directory, document)
            original_session, original_driver, original_actor = await receipts.ordinary(case.database)
            assert original_session is session and original_actor["current"] == original_actor["session"] == case.role
            async with _pinned_reader(case, table, original_actor["backend"]) as release:
                releaser = asyncio.create_task(_release_initial_reader(case, original_actor["backend"], release))
                try:
                    await _cutover_limits(case)
                    started = asyncio.get_running_loop().time()
                    await receipts.lock(case.database, case.schema, renamed_names, wait=True)
                    elapsed = asyncio.get_running_loop().time() - started
                    await releaser
                    assert 0.12 <= elapsed < 0.5
                    observed_session, observed_driver, observed_actor = await receipts.ordinary(case.database)
                    assert observed_session is original_session and observed_driver is original_driver
                    assert observed_actor == original_actor
                    await _assert_initial_lock_scope(case, frozen, renamed_names)
                finally:
                    releaser.cancel()
                    await asyncio.gather(releaser, return_exceptions=True)
        assert await case.database.scalar(f"SELECT marker FROM {table}") == "old"


async def _refuse_initial_lock(case, document, renamed_names, *, wait):
    table = receipts.qualified(case.schema, common.journal.documents.FAMILIES["profile"])
    with pytest.raises(asyncpg.LockNotAvailableError) as blocked:
        async with case.database.transaction():
            await common._lock_inputs(case.directory, document)
            _session, _driver, actor = await receipts.ordinary(case.database)
            async with _pinned_reader(case, table, actor["backend"]):
                await _cutover_limits(case)
                started = asyncio.get_running_loop().time()
                if wait:
                    await receipts.lock(case.database, case.schema, renamed_names, wait=True)
                else:
                    await receipts.lock(case.database, case.schema, renamed_names)
                pytest.fail("A held Reader must prevent the first inverse write")
    assert postgres_sqlstate(blocked.value) == "55P03"
    elapsed = asyncio.get_running_loop().time() - started
    assert (0.4 <= elapsed < 1) if wait else elapsed < 0.4


@pytest.mark.asyncio
@pytest.mark.parametrize("wait", (True, False), ids=("500ms-deadline", "default-nowait"))
async def test_initial_inverse_lock_refuses_without_writes(monkeypatch, wait):
    """A native refusal rolls back preparation locks and cannot rename or mutate a heap."""
    async with ordinary_serving_case(monkeypatch) as case:
        document, renamed_names = await _initial_lock_document(case)
        names = set(common.journal.documents.FAMILIES.values()) | {"retained_rows", "retained_scope"} | renamed_names
        oids_by_name = {
            name: await case.database.scalar(
                "SELECT to_regclass(:name)::oid", name=receipts.qualified(case.schema, name)
            )
            for name in names
        }
        await _refuse_initial_lock(case, document, renamed_names, wait=wait)
        assert case.database._transaction_binding() is None
        for name in names:
            table = receipts.qualified(case.schema, name)
            assert await case.database.scalar("SELECT to_regclass(:name)::oid", name=table) == oids_by_name[name]
            assert await case.database.scalar(f"SELECT marker FROM {table}") == "old"
        async with case.database.transaction():
            await receipts.lock(case.database, case.schema, names)
