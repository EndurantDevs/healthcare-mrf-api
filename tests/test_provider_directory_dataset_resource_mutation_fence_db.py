# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import importlib
import json
from contextlib import asynccontextmanager

import pytest

from db.connection import Database
from db.plan_release_storage import storage_relation
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_policy import source_transaction
from tests.test_provider_directory_proof_store_db import _candidate, _proof_database, _resource

importer = importlib.import_module("process.provider_directory_fhir")


@asynccontextmanager
async def _fence_database(monkeypatch):
    """Use the actual Source actors and isolated candidates for writer fences."""
    async with _proof_database(monkeypatch) as (database, schema):
        async with source_transaction(database):
            for dataset_id in ("dataset_a", "dataset_b", "dataset_stale"):
                endpoint_id, source_id = f"endpoint_{dataset_id}", f"source_{dataset_id}"
                await database.status(
                    f'INSERT INTO "{schema}".provider_directory_api_endpoint '
                    "(endpoint_id,canonical_api_base,credential_descriptor_hash,endpoint_signature_hash) "
                    "VALUES (:endpoint,:base,repeat('c',64),repeat('d',64))",
                    endpoint=endpoint_id,
                    base=f"https://example.test/{dataset_id}/fhir",
                )
                await database.status(
                    f'INSERT INTO "{schema}".provider_directory_source '
                    "(source_id,org_name,endpoint_id,canonical_api_base,requires_registration,requires_api_key) "
                    "VALUES (:source,'Fence fixture',:endpoint,:base,false,false)",
                    source=source_id,
                    endpoint=endpoint_id,
                    base=f"https://example.test/{dataset_id}/fhir",
                )
                await importer._ensure_endpoint_dataset_candidate(
                    _candidate(dataset_id=dataset_id, endpoint_id=endpoint_id, source_ids=(source_id,))
                )
        yield database, schema


def _resource_row(dataset_id: str, resource_id: str) -> dict[str, object]:
    return _resource(
        "Practitioner", resource_id, {"resourceType": "Practitioner", "id": resource_id}, dataset_id=dataset_id
    )


async def _resource_count(database: Database, schema: str, dataset_id: str) -> int:
    """Observe the exact candidate input, including a refused append after sealing."""
    async with source_transaction(database):
        binding = await database.scalar(
            f"SELECT candidate_storage_json FROM {storage_relation(schema, 'provider_directory_endpoint_dataset')} "
            "WHERE dataset_id=:dataset_id",
            dataset_id=dataset_id,
        )
        if binding is None:
            return 0
        binding = json.loads(binding) if isinstance(binding, str) else binding
        relation = storage_relation(schema, binding["tables"]["resource_input"]["name"])
        return int(
            await database.scalar(
                f"SELECT count(*) FROM {relation} WHERE dataset_id=:dataset_id", dataset_id=dataset_id
            )
        )


async def _finish_tasks(*tasks):
    """A failed writer must not leave its event-waiting peer alive during cleanup."""
    for task in tasks:
        if not task.done():
            task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_postgres_validation_waits_for_same_dataset_writer(monkeypatch):
    async with _fence_database(monkeypatch) as (database, schema):
        writer_holds_share_lock = asyncio.Event()
        release_writer = asyncio.Event()
        validation_started = asyncio.Event()
        validation_acquired = asyncio.Event()
        observed_counts: list[int] = []

        async def writer():
            async with source_transaction(database), database.acquire() as connection:
                await importer._upsert_dataset_resource_rows_on_connection(
                    connection,
                    [_resource_row("dataset_a", "practitioner_a")],
                    resource_hash_contract=importer.LEGACY_RESOURCE_HASH_CONTRACT,
                )
                writer_holds_share_lock.set()
                await release_writer.wait()

        async def validator():
            await writer_holds_share_lock.wait()
            async with source_transaction(database), database.acquire() as connection:
                validation_started.set()
                await connection.first(
                    f"SELECT dataset_id FROM {schema}."
                    "provider_directory_endpoint_dataset "
                    "WHERE dataset_id = 'dataset_a' FOR UPDATE;"
                )
                observed_counts.append(await _resource_count(database, schema, "dataset_a"))
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
            await _finish_tasks(writer_task, validator_task)

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
            async with source_transaction(database), database.acquire() as connection:
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
            async with source_transaction(database), database.acquire() as connection:
                await importer._upsert_dataset_resource_rows_on_connection(
                    connection,
                    [_resource_row("dataset_stale", "practitioner_stale")],
                    resource_hash_contract=importer.LEGACY_RESOURCE_HASH_CONTRACT,
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
            await _finish_tasks(validator_task, writer_task)

        assert task_outcomes[0] is None
        assert isinstance(task_outcomes[1], GenericCandidateError)
        assert str(task_outcomes[1]) == "provider_directory_generic_source_header_sealed"
        assert await _resource_count(database, schema, "dataset_stale") == 0


@pytest.mark.asyncio
async def test_postgres_distinct_dataset_writers_remain_parallel(monkeypatch):
    async with _fence_database(monkeypatch) as (database, schema):
        first_writer_holds_lock = asyncio.Event()
        release_first_writer = asyncio.Event()
        second_writer_committed = asyncio.Event()

        async def first_writer():
            async with source_transaction(database), database.acquire() as connection:
                await importer._upsert_dataset_resource_rows_on_connection(
                    connection,
                    [_resource_row("dataset_a", "practitioner_a")],
                    resource_hash_contract=importer.LEGACY_RESOURCE_HASH_CONTRACT,
                )
                first_writer_holds_lock.set()
                await release_first_writer.wait()

        async def second_writer():
            await first_writer_holds_lock.wait()
            async with source_transaction(database), database.acquire() as connection:
                await importer._upsert_dataset_resource_rows_on_connection(
                    connection,
                    [_resource_row("dataset_b", "practitioner_b")],
                    resource_hash_contract=importer.LEGACY_RESOURCE_HASH_CONTRACT,
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
            await _finish_tasks(first_task, second_task)

        assert await _resource_count(database, schema, "dataset_a") == 1
