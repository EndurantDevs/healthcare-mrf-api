# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL fencing and rollback proof for semantic content."""

from __future__ import annotations

import asyncio

import pytest

from db.models import ProviderDirectoryPractitioner
from process.provider_directory_proof_store import (
    ProviderDirectoryStoredProofOptions,
    build_stored_dataset_proof,
)
from process.provider_directory_resource_hash import (
    LEGACY_RESOURCE_HASH_CONTRACT,
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    resource_payload_sha256_for_contract,
)
from tests.provider_directory_semantic_proof_v3_postgres_support import (
    CANONICAL_BASE,
    PROJECTION_AS_OF,
    SOURCE_ID,
    V3_DATASET_ROLLBACK,
    _compatibility_counts,
    _dataset_and_shard_counts,
    _dataset_row,
    _insert_parent,
    _observation,
    _semantic_database,
    _write_page,
    importer,
)


def _checkpoint_context(dataset_id):
    return importer.PaginationCheckpointContext(
        canonical_api_base=CANONICAL_BASE,
        source_scope_hash="scope-semantic-clear",
        source_ids=(SOURCE_ID,),
        owner_run_id="run-semantic-proof",
        acquisition_root_run_id=f"root-{dataset_id}"[:64],
        endpoint_id=f"endpoint-{dataset_id}"[:64],
        dataset_id=dataset_id,
        lineage_verified=True,
    )


async def _run_writer_and_checkpoint_clear(
    monkeypatch,
    database,
    dataset_id,
    incoming_row,
    resource_hash_contract,
    projection_as_of,
):
    writer_has_family_lock = asyncio.Event()
    release_writer = asyncio.Event()
    original_family_lock = importer._lock_endpoint_dataset_resource_family

    async def controlled_family_lock(executor, observed_id, resource_type):
        await original_family_lock(executor, observed_id, resource_type)
        if asyncio.current_task().get_name() == "semantic-writer":
            writer_has_family_lock.set()
            await release_writer.wait()

    async def write_candidate():
        async with database.acquire() as connection:
            await importer._upsert_dataset_resource_rows_on_connection(
                connection,
                [incoming_row],
                persist_content_proof=True,
                resource_hash_contract=resource_hash_contract,
                semantic_projection_as_of=projection_as_of,
            )

    monkeypatch.setattr(
        importer,
        "_lock_endpoint_dataset_resource_family",
        controlled_family_lock,
    )
    writer_task = asyncio.create_task(write_candidate(), name="semantic-writer")
    clear_task: asyncio.Task | None = None
    try:
        await asyncio.wait_for(writer_has_family_lock.wait(), timeout=2)
        clear_task = asyncio.create_task(
            importer._clear_checkpoint_dataset_resource_type(
                _checkpoint_context(dataset_id),
                "Practitioner",
            ),
            name="checkpoint-clear",
        )
        await asyncio.sleep(0.1)
        assert clear_task.done() is False
        release_writer.set()
        await asyncio.gather(writer_task, clear_task)
    finally:
        release_writer.set()
        await asyncio.gather(
            writer_task,
            *([clear_task] if clear_task is not None else []),
            return_exceptions=True,
        )

@pytest.mark.asyncio
@pytest.mark.parametrize(
    "resource_hash_contract",
    [
        TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    ],
)
async def test_postgres_checkpoint_clear_waits_for_same_family_writer(
    monkeypatch,
    resource_hash_contract,
):
    """Serialize reset cleanup with both historical and semantic writers."""

    async with _semantic_database(monkeypatch) as (database, schema):
        dataset_id = f"dataset-clear-{resource_hash_contract}"
        projection_as_of = (
            PROJECTION_AS_OF
            if resource_hash_contract
            == SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
            else None
        )
        await _insert_parent(
            database,
            schema,
            dataset_id,
            resource_hash_contract=resource_hash_contract,
            semantic_projection_as_of=projection_as_of,
        )
        incoming_row = importer._endpoint_dataset_resource_rows(
            ProviderDirectoryPractitioner,
            [_observation("Alex Example", page_number=1)],
            dataset_id=dataset_id,
            resource_hash_contract=resource_hash_contract,
        )[0]
        await _run_writer_and_checkpoint_clear(
            monkeypatch,
            database,
            dataset_id,
            incoming_row,
            resource_hash_contract,
            projection_as_of,
        )
        assert await _dataset_and_shard_counts(
            database,
            schema,
            dataset_id,
        ) == (0, 0)


@pytest.mark.asyncio
async def test_postgres_v3_proof_failure_rolls_back_row_and_shard(monkeypatch):
    async with _semantic_database(monkeypatch) as (database, schema):
        await _insert_parent(database, schema, V3_DATASET_ROLLBACK)
        original_persist_shard = importer.persist_dataset_proof_shard

        async def fail_after_shard(*args, **kwargs):
            await original_persist_shard(*args, **kwargs)
            raise RuntimeError("synthetic-proof-failure")

        monkeypatch.setattr(
            importer,
            "persist_dataset_proof_shard",
            fail_after_shard,
        )
        with pytest.raises(RuntimeError, match="synthetic-proof-failure"):
            await importer._persist_endpoint_dataset_rows(
                ProviderDirectoryPractitioner,
                [_observation("Alex Example", page_number=1)],
                V3_DATASET_ROLLBACK,
                resource_hash_contract=(
                    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
                ),
                semantic_projection_as_of=PROJECTION_AS_OF,
            )
        assert await _dataset_and_shard_counts(
            database,
            schema,
            V3_DATASET_ROLLBACK,
        ) == (0, 0)


@pytest.mark.asyncio
async def test_postgres_v3_typed_failure_rolls_back_all_representations(
    monkeypatch,
):
    """Hold the parent transaction through proof, edges, and typed rows."""

    async with _semantic_database(monkeypatch) as (database, schema):
        dataset_id = "dataset-semantic-typed-rollback"
        await _insert_parent(database, schema, dataset_id)
        original_upsert_rows = importer._upsert_rows

        async def fail_typed_write(model, rows, **kwargs):
            if model is ProviderDirectoryPractitioner:
                raise RuntimeError("synthetic-typed-failure")
            return await original_upsert_rows(model, rows, **kwargs)

        monkeypatch.setattr(importer, "_upsert_rows", fail_typed_write)
        with pytest.raises(RuntimeError, match="synthetic-typed-failure"):
            await _write_page(
                dataset_id,
                _observation("Alex Example", page_number=1),
            )

        assert await _dataset_and_shard_counts(
            database,
            schema,
            dataset_id,
        ) == (0, 0)
        assert await _compatibility_counts(database, schema) == (0, 0, 0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("parent_contract", "parent_date", "expected_error"),
    [
        (
            TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
            None,
            "hash_contract_changed",
        ),
        (
            SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
            "2026-08-10",
            "projection_date_changed",
        ),
    ],
)
async def test_postgres_v3_parent_contract_and_projection_date_are_fenced(
    monkeypatch,
    parent_contract,
    parent_date,
    expected_error,
):
    async with _semantic_database(monkeypatch) as (database, schema):
        dataset_id = f"dataset-fence-{expected_error}"
        await _insert_parent(
            database,
            schema,
            dataset_id,
            resource_hash_contract=parent_contract,
            semantic_projection_as_of=parent_date,
        )
        incoming_row = _dataset_row(
            dataset_id,
            _observation("Alex Example", page_number=1),
        )
        with pytest.raises(RuntimeError, match=expected_error):
            async with database.acquire() as connection:
                await importer._upsert_dataset_resource_rows_on_connection(
                    connection,
                    [incoming_row],
                    persist_content_proof=True,
                    resource_hash_contract=(
                        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
                    ),
                    semantic_projection_as_of=PROJECTION_AS_OF,
                )
        assert await _dataset_and_shard_counts(
            database,
            schema,
            dataset_id,
        ) == (0, 0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "resource_hash_contract",
    [
        LEGACY_RESOURCE_HASH_CONTRACT,
        TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    ],
)
async def test_postgres_historical_hash_contracts_remain_writable(
    monkeypatch,
    resource_hash_contract,
):
    async with _semantic_database(monkeypatch) as (database, schema):
        dataset_id = f"dataset-{resource_hash_contract}"
        await _insert_parent(
            database,
            schema,
            dataset_id,
            resource_hash_contract=resource_hash_contract,
            semantic_projection_as_of=None,
        )
        payload_by_field = {
            "resource_id": "organization-historical",
            "name": "Example Organization",
            "resource_url": f"{CANONICAL_BASE}/Organization/historical",
        }
        dataset_row_by_field = {
            "dataset_id": dataset_id,
            "resource_type": "Organization",
            "resource_id": payload_by_field["resource_id"],
            "payload_hash": resource_payload_sha256_for_contract(
                payload_by_field,
                resource_hash_contract,
            ),
            "payload_json": payload_by_field,
            "acquired_resource_sha256": None,
        }
        async with database.acquire() as connection:
            await importer._upsert_dataset_resource_rows_on_connection(
                connection,
                [dataset_row_by_field],
                persist_content_proof=True,
                resource_hash_contract=resource_hash_contract,
                semantic_projection_as_of=None,
            )
        stored_proof = await build_stored_dataset_proof(
            database,
            schema,
            dataset_id=dataset_id,
            endpoint_id=f"endpoint-{dataset_id}"[:64],
            acquisition_root_run_id=f"root-{dataset_id}"[:64],
            source_ids=[SOURCE_ID],
            selected_resources=["Organization"],
            options=ProviderDirectoryStoredProofOptions(
                expected_resource_hash_contract=resource_hash_contract,
            ),
        )
        assert stored_proof.resource_count == 1
        assert await _dataset_and_shard_counts(
            database,
            schema,
            dataset_id,
        ) == (1, 1)
