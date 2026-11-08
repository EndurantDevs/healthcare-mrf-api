# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native attachment/rollback concurrency; no CMS authority or timing acceptance."""

import asyncio
import os
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_generic_source_completion as completion


@pytest_asyncio.fixture
async def attachment_case():
    asyncpg = pytest.importorskip("asyncpg")
    dsn = os.environ.get("HLTHPRT_CMS_CUSTODY_TEST_DSN")
    if not dsn:
        pytest.skip("native task database required")
    schema = "attachment_" + uuid4().hex
    connection = await asyncpg.connect(dsn)
    peer = None
    try:
        peer = await asyncpg.connect(dsn)
        await connection.execute("CREATE SCHEMA " + storage_identifier(schema))
        policy = SimpleNamespace(schema=schema, runtime_roles=(await connection.fetchval("SELECT current_user"),))
        tables_by_root = {}
        for index, root in enumerate(completion.storage.RELATIONS):
            parent = storage_relation(schema, root)
            stage = "stage_" + str(index)
            history = storage_relation(schema, "history_" + str(index))
            await connection.execute(
                f"CREATE TABLE {parent}(dataset_id text NOT NULL,value text NOT NULL,"
                "PRIMARY KEY(dataset_id,value)) PARTITION BY LIST(dataset_id)"
            )
            await connection.execute(f"CREATE TABLE {history} PARTITION OF {parent} FOR VALUES IN ('old')")
            await connection.execute(f"INSERT INTO {parent} VALUES('old','retained')")
            await connection.execute(
                f"CREATE TABLE {storage_relation(schema, stage)} (LIKE {parent} INCLUDING ALL) "
                "WITH (autovacuum_enabled=false)"
            )
            await connection.execute(f"INSERT INTO {storage_relation(schema, stage)} VALUES('new','prepared')")
            tables_by_root[root] = {"name": stage}
        yield SimpleNamespace(
            connection=connection,
            peer=peer,
            policy=policy,
            binding={"identity": {"dataset_id": "new"}, "tables": tables_by_root},
            asyncpg=asyncpg,
        )
    finally:
        if peer is not None:
            await peer.close()
        if connection.is_in_transaction():
            await connection.execute("ROLLBACK")
        await connection.execute("DROP SCHEMA IF EXISTS " + storage_identifier(schema) + " CASCADE")
        assert await connection.fetchval("SELECT to_regnamespace($1) IS NULL", schema)
        await connection.close()


async def _assert_unattached(case):
    for receipt in case.binding["tables"].values():
        relation = storage_relation(case.policy.schema, receipt["name"])
        assert not await case.connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=$1::regclass)", relation
        )
        assert not await case.connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_constraint WHERE conrelid=$1::regclass AND contype='c')", relation
        )
        assert await case.connection.fetchval("SELECT count(*) FROM " + relation) == 1


async def test_retained_reader_and_disjoint_history_writer_continue_during_attach(attachment_case):
    case = attachment_case
    parent = storage_relation(case.policy.schema, completion.storage.RELATIONS[0])
    history = storage_relation(case.policy.schema, "history_0")
    transaction = case.peer.transaction(isolation="repeatable_read")
    await transaction.start()
    try:
        assert await case.peer.fetchval("SELECT count(*) FROM " + parent + " WHERE dataset_id='old'") == 1
        await case.peer.execute("LOCK TABLE ONLY " + history + " IN SHARE UPDATE EXCLUSIVE MODE")
        async with case.connection.transaction():
            await case.connection.execute("SET LOCAL lock_timeout='100ms'")
            await case.connection.execute("SET LOCAL statement_timeout='1s'")
            await completion._attach_final_tables(case.connection, case.policy, case.binding)
            await case.peer.execute("INSERT INTO " + parent + " VALUES('old','parallel')")
            assert await case.peer.fetchval("SELECT count(*) FROM " + parent + " WHERE dataset_id='old'") == 2
        assert await case.peer.fetchval("SELECT count(*) FROM " + parent + " WHERE dataset_id='old'") == 2
    finally:
        await transaction.rollback()
    assert await case.connection.fetchval("SELECT count(*) FROM " + parent) == 2
    for root in completion.storage.RELATIONS:
        assert await case.connection.fetchval("SELECT count(*) FROM " + storage_relation(case.policy.schema, root)) == 2


async def test_busy_parent_refuses_nowait_and_rolls_back_private_preparation(attachment_case):
    case = attachment_case
    parent = storage_relation(case.policy.schema, sorted(completion.storage.RELATIONS)[0])
    async with case.peer.transaction():
        await case.peer.execute("LOCK TABLE ONLY " + parent + " IN SHARE UPDATE EXCLUSIVE MODE")
        async with case.connection.transaction():
            with pytest.raises(case.asyncpg.LockNotAvailableError):
                async with case.connection.transaction():
                    await case.connection.execute("SET LOCAL statement_timeout='1s'")
                    await completion._attach_final_tables(case.connection, case.policy, case.binding)
            await _assert_unattached(case)
            assert not await case.connection.fetchval(
                "SELECT EXISTS(SELECT FROM pg_locks WHERE pid=pg_backend_pid() AND mode='ShareUpdateExclusiveLock')"
            )


async def test_out_of_dataset_rows_refuse_before_any_attachment(attachment_case):
    case = attachment_case
    first = case.binding["tables"][completion.storage.RELATIONS[0]]["name"]
    await case.connection.execute("UPDATE " + storage_relation(case.policy.schema, first) + " SET dataset_id='other'")
    async with case.connection.transaction():
        with pytest.raises(case.asyncpg.CheckViolationError):
            async with case.connection.transaction():
                await completion._attach_final_tables(case.connection, case.policy, case.binding)
        await _assert_unattached(case)


async def test_late_overlap_rolls_back_the_complete_six_table_group(attachment_case):
    case = attachment_case
    last = storage_relation(case.policy.schema, sorted(completion.storage.RELATIONS)[-1])
    occupied = storage_relation(case.policy.schema, "occupied")
    await case.connection.execute(f"CREATE TABLE {occupied} PARTITION OF {last} FOR VALUES IN ('new')")
    async with case.connection.transaction():
        with pytest.raises(case.asyncpg.InvalidObjectDefinitionError):
            async with case.connection.transaction():
                await completion._attach_final_tables(case.connection, case.policy, case.binding)
        await _assert_unattached(case)


async def _blocked(connection, pid):
    async with asyncio.timeout(2):
        while True:
            await connection.execute("SELECT pg_stat_clear_snapshot()")
            if await connection.fetchval("SELECT wait_event_type='Lock' FROM pg_stat_activity WHERE pid=$1", pid):
                return
            await asyncio.sleep(0.01)


async def test_cancel_during_private_preparation_preserves_caller_transaction(attachment_case):
    case = attachment_case
    first = case.binding["tables"][completion.storage.RELATIONS[0]]["name"]
    relation = storage_relation(case.policy.schema, first)
    async with case.peer.transaction(), case.connection.transaction():
        await case.peer.execute("SELECT * FROM " + relation)

        async def attach():
            async with case.connection.transaction():
                await case.connection.execute("SET LOCAL statement_timeout='5s'")
                await completion._attach_final_tables(case.connection, case.policy, case.binding)

        task = asyncio.create_task(attach())
        try:
            await _blocked(case.peer, case.connection.get_server_pid())
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert case.connection.is_in_transaction()
        await _assert_unattached(case)
