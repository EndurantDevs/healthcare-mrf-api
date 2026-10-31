# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Parallel checkpoints can update their candidate without a lock upgrade cycle."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import pytest

from process.provider_directory_projection_stage import _copy_driver
from process.provider_directory_source_checkpoint_control import fence_checkpoint_dataset_parents
from tests.test_provider_directory_profile_selection_db import _profile_test_database


@asynccontextmanager
async def _checkpoint_header():
    """Create and clean only the exact native header owned by this test."""
    database = await _profile_test_database()
    schema = "checkpoint_lock_test_" + uuid4().hex[:16]
    try:
        await database.status(f"CREATE SCHEMA {schema}")
        await database.status(
            f"CREATE TABLE {schema}.provider_directory_endpoint_dataset "
            "(dataset_id varchar(96) PRIMARY KEY,acquisition_root_run_id varchar(64) NOT NULL,"
            "status varchar(32) NOT NULL,candidate_storage_json jsonb NOT NULL)"
        )
        await database.status(
            f"INSERT INTO {schema}.provider_directory_endpoint_dataset "
            "VALUES ('dataset','root','acquiring',CAST(:payload AS jsonb))",
            payload='{"ordinal":0}',
        )
        yield database, schema
    finally:
        try:
            await database.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await database.disconnect()


async def _checkpoint_writer(database, schema, signals, *, first):
    """Use an independent original transaction for each resource checkpoint."""
    async with database.transaction() as session:
        connection = await _copy_driver(session)
        await connection.execute("SET LOCAL statement_timeout='5s'")
        if not first:
            signals.second_started.set_result(connection.get_server_pid())
        publisher = SimpleNamespace(connection=connection, policy=SimpleNamespace(schema=schema))
        await fence_checkpoint_dataset_parents(publisher, ["dataset"], ["root"])
        (signals.first_fenced if first else signals.second_fenced).set()
        await signals.release_writes.wait()
        await connection.fetchrow(
            f"SELECT dataset_id FROM {schema}.provider_directory_endpoint_dataset "
            "WHERE dataset_id='dataset' FOR NO KEY UPDATE"
        )
        await connection.execute(
            f"UPDATE {schema}.provider_directory_endpoint_dataset SET candidate_storage_json="
            "jsonb_build_object('ordinal',(candidate_storage_json->>'ordinal')::integer+1)"
        )


@pytest.mark.asyncio
async def test_parallel_checkpoint_initialization_does_not_deadlock_on_candidate_header():
    """Both bounded initializers finish and retain their shared header updates."""
    signals = SimpleNamespace(
        first_fenced=asyncio.Event(),
        second_fenced=asyncio.Event(),
        second_started=asyncio.get_running_loop().create_future(),
        release_writes=asyncio.Event(),
    )
    async with _checkpoint_header() as (database, schema):
        async with asyncio.timeout(8), asyncio.TaskGroup() as tasks:
            tasks.create_task(_checkpoint_writer(database, schema, signals, first=True))
            await signals.first_fenced.wait()
            tasks.create_task(_checkpoint_writer(database, schema, signals, first=False))
            second_pid = await signals.second_started
            # Observe the competing lock or the old shared-lock path before writes start.
            while not signals.second_fenced.is_set():
                if await database.scalar(
                    "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=:pid AND NOT granted)", pid=second_pid
                ):
                    break
                await asyncio.sleep(0)
            signals.release_writes.set()
        assert signals.second_fenced.is_set()
        assert (
            await database.scalar(
                f"SELECT candidate_storage_json->>'ordinal' FROM {schema}.provider_directory_endpoint_dataset"
            )
            == "2"
        )
