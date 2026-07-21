# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import importlib
import uuid

import pytest
from sqlalchemy.exc import OperationalError

from db.connection import Database


importer = importlib.import_module("process.provider_directory_fhir")


async def _require_disposable_postgres(database: Database) -> None:
    try:
        database_name = str(
            await database.scalar("SELECT current_database();") or ""
        )
    except (OSError, OperationalError):
        pytest.skip("dataset-resource fence tests need disposable Postgres")
    if "test" not in database_name.lower():
        pytest.skip("dataset-resource fence tests need a test database")


async def _create_fence_tables(database: Database, schema: str) -> None:
    await database.status(f"CREATE SCHEMA {schema};")
    await database.status(
        f"""
        CREATE TABLE {schema}.provider_directory_endpoint_dataset (
            dataset_id varchar(96) PRIMARY KEY,
            endpoint_id varchar(64) NOT NULL,
            status varchar(32) NOT NULL,
            is_current boolean NOT NULL DEFAULT false
        );
        """
    )
    await database.status(
        f"""
        CREATE TABLE {schema}.provider_directory_dataset_resource (
            dataset_id varchar(96) NOT NULL REFERENCES
                {schema}.provider_directory_endpoint_dataset(dataset_id),
            resource_type varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            payload_hash varchar(64) NOT NULL,
            payload_json jsonb NOT NULL,
            PRIMARY KEY (dataset_id, resource_type, resource_id)
        );
        """
    )
    await database.status(
        f"""
        INSERT INTO {schema}.provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, status
        ) VALUES
            ('dataset_a', 'endpoint_a', :acquiring),
            ('dataset_b', 'endpoint_b', :incomplete),
            ('dataset_stale', 'endpoint_stale', :acquiring);
        """,
        acquiring=importer.ENDPOINT_DATASET_ACQUIRING,
        incomplete=importer.ENDPOINT_DATASET_INCOMPLETE,
    )


@asynccontextmanager
async def _fence_database(monkeypatch):
    schema = f"provider_directory_fence_{uuid.uuid4().hex[:12]}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", "5")
    monkeypatch.setattr(
        importer.ProviderDirectoryDatasetResource.__table__,
        "schema",
        schema,
    )
    database = Database()
    is_schema_created = False
    try:
        await database.connect()
        await _require_disposable_postgres(database)
        await _create_fence_tables(database, schema)
        is_schema_created = True
        yield database, schema
    finally:
        if is_schema_created:
            await database.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        await database.disconnect()


def _resource_row(dataset_id: str, resource_id: str) -> dict[str, object]:
    return {
        "dataset_id": dataset_id,
        "resource_type": "Practitioner",
        "resource_id": resource_id,
        "payload_hash": resource_id.rjust(64, "0")[-64:],
        "payload_json": {"resourceType": "Practitioner", "id": resource_id},
    }


async def _resource_count(
    database: Database,
    schema: str,
    dataset_id: str,
) -> int:
    return int(
        await database.scalar(
            f"SELECT count(*) FROM {schema}.provider_directory_dataset_resource "
            "WHERE dataset_id = :dataset_id;",
            dataset_id=dataset_id,
        )
        or 0
    )


@pytest.mark.asyncio
async def test_postgres_validation_waits_for_same_dataset_writer(monkeypatch):
    async with _fence_database(monkeypatch) as (database, schema):
        writer_holds_share_lock = asyncio.Event()
        release_writer = asyncio.Event()
        validation_started = asyncio.Event()
        validation_acquired = asyncio.Event()
        observed_counts: list[int] = []

        async def writer():
            async with database.acquire() as connection:
                await importer._upsert_dataset_resource_rows_on_connection(
                    connection,
                    [_resource_row("dataset_a", "practitioner_a")],
                )
                writer_holds_share_lock.set()
                await release_writer.wait()

        async def validator():
            await writer_holds_share_lock.wait()
            async with database.acquire() as connection:
                validation_started.set()
                await connection.first(
                    f"SELECT dataset_id FROM {schema}."
                    "provider_directory_endpoint_dataset "
                    "WHERE dataset_id = 'dataset_a' FOR UPDATE;"
                )
                observed_counts.append(
                    int(
                        await connection.scalar(
                            f"SELECT count(*) FROM {schema}."
                            "provider_directory_dataset_resource "
                            "WHERE dataset_id = 'dataset_a';"
                        )
                        or 0
                    )
                )
                validation_acquired.set()

        writer_task = asyncio.create_task(writer())
        validator_task = asyncio.create_task(validator())
        try:
            await asyncio.wait_for(validation_started.wait(), timeout=2)
            await asyncio.sleep(0.1)
            assert validation_acquired.is_set() is False
            release_writer.set()
            await asyncio.gather(writer_task, validator_task)
        finally:
            release_writer.set()
            await asyncio.gather(
                writer_task,
                validator_task,
                return_exceptions=True,
            )

        assert observed_counts == [1]


@pytest.mark.asyncio
async def test_postgres_stale_writer_rejects_dataset_sealed_while_waiting(
    monkeypatch,
):
    async with _fence_database(monkeypatch) as (database, schema):
        validation_holds_update_lock = asyncio.Event()
        release_validation = asyncio.Event()
        writer_started = asyncio.Event()

        async def validator():
            async with database.acquire() as connection:
                await connection.first(
                    f"SELECT dataset_id FROM {schema}."
                    "provider_directory_endpoint_dataset "
                    "WHERE dataset_id = 'dataset_stale' FOR UPDATE;"
                )
                await connection.status(
                    f"UPDATE {schema}.provider_directory_endpoint_dataset "
                    "SET status = :status WHERE dataset_id = 'dataset_stale';",
                    status=importer.ENDPOINT_DATASET_VALIDATED,
                )
                validation_holds_update_lock.set()
                await release_validation.wait()

        async def stale_writer():
            await validation_holds_update_lock.wait()
            writer_started.set()
            async with database.acquire() as connection:
                await importer._upsert_dataset_resource_rows_on_connection(
                    connection,
                    [_resource_row("dataset_stale", "practitioner_stale")],
                )

        validator_task = asyncio.create_task(validator())
        writer_task = asyncio.create_task(stale_writer())
        try:
            await asyncio.wait_for(writer_started.wait(), timeout=2)
            await asyncio.sleep(0.1)
            assert writer_task.done() is False
            release_validation.set()
            task_outcomes = await asyncio.gather(
                validator_task,
                writer_task,
                return_exceptions=True,
            )
        finally:
            release_validation.set()
            await asyncio.gather(
                validator_task,
                writer_task,
                return_exceptions=True,
            )

        assert task_outcomes[0] is None
        assert isinstance(task_outcomes[1], RuntimeError)
        assert "parent_immutable" in str(task_outcomes[1])
        assert await _resource_count(database, schema, "dataset_stale") == 0


@pytest.mark.asyncio
async def test_postgres_distinct_dataset_writers_remain_parallel(monkeypatch):
    async with _fence_database(monkeypatch) as (database, schema):
        first_writer_holds_lock = asyncio.Event()
        release_first_writer = asyncio.Event()
        second_writer_committed = asyncio.Event()

        async def first_writer():
            async with database.acquire() as connection:
                await importer._upsert_dataset_resource_rows_on_connection(
                    connection,
                    [_resource_row("dataset_a", "practitioner_a")],
                )
                first_writer_holds_lock.set()
                await release_first_writer.wait()

        async def second_writer():
            await first_writer_holds_lock.wait()
            async with database.acquire() as connection:
                await importer._upsert_dataset_resource_rows_on_connection(
                    connection,
                    [_resource_row("dataset_b", "practitioner_b")],
                )
            second_writer_committed.set()

        first_task = asyncio.create_task(first_writer())
        second_task = asyncio.create_task(second_writer())
        try:
            await asyncio.wait_for(second_writer_committed.wait(), timeout=2)
            assert first_task.done() is False
            assert await _resource_count(database, schema, "dataset_b") == 1
            release_first_writer.set()
            await asyncio.gather(first_task, second_task)
        finally:
            release_first_writer.set()
            await asyncio.gather(
                first_task,
                second_task,
                return_exceptions=True,
            )

        assert await _resource_count(database, schema, "dataset_a") == 1
