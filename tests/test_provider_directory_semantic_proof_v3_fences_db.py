# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL fencing and rollback proof for semantic content."""

from __future__ import annotations

import asyncio
import json

import pytest
from sqlalchemy import text

from db.models import ProviderDirectoryPractitioner
from process import provider_directory_generic_source_storage as source_storage
from process import provider_directory_output_snapshot_stage as output_stage
from process.provider_directory_generic_source_completion import complete_generic_source_candidate
from process.provider_directory_generic_source_policy import source_transaction
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
    _candidate_stored_proof,
    _compatibility_counts,
    _dataset_and_shard_counts,
    _dataset_row,
    _insert_parent,
    _materialize_semantic_dataset,
    _observation,
    _publish_semantic_source,
    _semantic_database,
    _stored_dataset_row,
    _write_page,
    importer,
)
from tests.provider_directory_semantic_proof_v3_postgres_support import (
    semantic_source_template as semantic_source_template,
)


async def _input_sequence(database, schema, dataset_id):
    """Read the genuine protected binding and its actual internally owned sequence."""
    async with source_transaction(database):
        binding = await database.scalar(
            f'SELECT candidate_storage_json FROM "{schema}".provider_directory_endpoint_dataset '
            "WHERE dataset_id=:dataset_id",
            dataset_id=dataset_id,
        )
        binding = json.loads(binding) if isinstance(binding, str) else binding
        relation = f"{schema}.{binding['tables']['resource_input']['name']}"
        sequence = await database.scalar("SELECT pg_get_serial_sequence(:relation,'batch_ordinal')", relation=relation)
    return binding, sequence


@pytest.mark.asyncio
async def test_postgres_sequence_gap_resumes_and_rewind_refuses(monkeypatch):
    """An aborted native allocation leaves a safe gap; rewinding cannot duplicate a committed batch."""
    async with _semantic_database(monkeypatch) as (database, schema):
        dataset_id = "dataset-semantic-sequence"
        await _insert_parent(database, schema, dataset_id)
        with pytest.raises(RuntimeError, match="abort batch"):
            async with source_transaction(database):
                await _write_page(dataset_id, _observation("Alex Example", page_number=1))
                raise RuntimeError("abort batch")
        assert await _dataset_and_shard_counts(database, schema, dataset_id) == (0, 0)
        await _write_page(dataset_id, _observation("Alex Example", page_number=1))
        binding, sequence = await _input_sequence(database, schema, dataset_id)
        assert binding["ordinal"] == 2
        async with database.semantic_component.admin_engine.begin() as connection:
            await connection.execute(text("SELECT setval(CAST(:sequence AS regclass),2,false)"), {"sequence": sequence})
        with pytest.raises(source_storage.GenericCandidateError, match="ordinal_exhausted"):
            await _write_page(dataset_id, _observation("Avery Sample", page_number=2))
        assert await _dataset_and_shard_counts(database, schema, dataset_id) == (1, 1)
        async with database.semantic_component.admin_engine.begin() as connection:
            await connection.execute(text("SELECT setval(CAST(:sequence AS regclass),2,true)"), {"sequence": sequence})
        await _write_page(dataset_id, _observation("Avery Sample", page_number=2))
        assert (await _input_sequence(database, schema, dataset_id))[0]["ordinal"] == 3
        assert await _dataset_and_shard_counts(database, schema, dataset_id) == (1, 2)


@pytest.mark.asyncio
async def test_postgres_completion_waits_for_native_copy(monkeypatch):
    """The exclusive completion fence drains COPY before reducing its committed observations."""
    async with _semantic_database(monkeypatch) as (database, schema):
        dataset_id = "dataset-semantic-completion-drain"
        await _insert_parent(database, schema, dataset_id)
        copy_started, release_copy = asyncio.Event(), asyncio.Event()
        original_copy = source_storage._copy

        async def paused_copy(*args, **kwargs):
            if asyncio.current_task().get_name() == "draining-writer":
                copy_started.set()
                await release_copy.wait()
            return await original_copy(*args, **kwargs)

        async def complete():
            async with source_transaction(database):
                return await complete_generic_source_candidate(
                    database, importer, database.semantic_candidates[dataset_id]
                )

        monkeypatch.setattr(source_storage, "_copy", paused_copy)
        writer = asyncio.create_task(
            _write_page(dataset_id, _observation("Alex Example", page_number=1)), name="draining-writer"
        )
        completer = None
        try:
            await asyncio.wait_for(copy_started.wait(), timeout=2)
            completer = asyncio.create_task(complete())
            await asyncio.sleep(0.1)
            assert not completer.done()
            release_copy.set()
            _, binding = await asyncio.gather(writer, completer)
            assert binding["phase"] == "prepared" and binding["ordinal"] == 1
            async with source_transaction(database):
                count = await database.scalar(
                    f'SELECT count(*) FROM "{schema}"."{binding["tables"][source_storage.RESOURCE]["name"]}"'
                )
                assert count == 1
        finally:
            release_copy.set()
            await asyncio.gather(writer, *([completer] if completer is not None else []), return_exceptions=True)


@pytest.mark.asyncio
async def test_postgres_disposal_drops_owned_input_sequence(monkeypatch):
    """Disposal removes the exact heap and its native identity sequence together."""
    from process.provider_directory_source_failure_control import mark_source_candidate

    async with _semantic_database(monkeypatch) as (database, schema):
        dataset_id = "dataset-semantic-sequence-disposal"
        await _insert_parent(database, schema, dataset_id)
        await _write_page(dataset_id, _observation("Alex Example", page_number=1))
        _, sequence = await _input_sequence(database, schema, dataset_id)
        candidate = database.semantic_candidates[dataset_id]
        await mark_source_candidate(
            importer,
            candidate,
            importer.ENDPOINT_DATASET_FAILED,
            importer._endpoint_dataset_candidate_metadata(candidate),
        )
        async with source_transaction(database):
            await source_storage.discard_generic_source_inputs(database, importer, candidate, None)
            assert await database.scalar("SELECT to_regclass(:sequence)", sequence=sequence) is None


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
    original_family_lock = source_storage._lock_resource_family

    async def controlled_family_lock(executor, policy, observed_id, resource_type):
        await original_family_lock(executor, policy, observed_id, resource_type)
        if asyncio.current_task().get_name() == "semantic-writer":
            writer_has_family_lock.set()
            await release_writer.wait()

    async def write_candidate():
        async with source_transaction(database), database.acquire() as connection:
            await importer._upsert_dataset_resource_rows_on_connection(
                connection,
                [incoming_row],
                persist_content_proof=True,
                resource_hash_contract=resource_hash_contract,
                semantic_projection_as_of=projection_as_of,
            )

    monkeypatch.setattr(
        source_storage,
        "_lock_resource_family",
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
            PROJECTION_AS_OF if resource_hash_contract == SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT else None
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
                resource_hash_contract=(SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT),
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
    """Preserve immutable Source while failed output stages leave all serving rows unchanged."""

    async with _semantic_database(monkeypatch) as (database, schema):
        dataset_id = "dataset-semantic-typed-rollback"
        await _insert_parent(database, schema, dataset_id)
        await _write_page(dataset_id, _observation("Alex Example", page_number=1))
        assert await _dataset_and_shard_counts(database, schema, dataset_id) == (1, 1)
        await _publish_semantic_source(database, dataset_id)
        retained_before = await _stored_dataset_row(database, schema, dataset_id)
        original_copy = output_stage.copy_output_rows

        async def fail_typed_write(directory, capability, stage, rows):
            if stage.target == ProviderDirectoryPractitioner.__tablename__:
                raise RuntimeError("synthetic-typed-failure")
            return await original_copy(directory, capability, stage, rows)

        monkeypatch.setattr(output_stage, "copy_output_rows", fail_typed_write)
        with pytest.raises(RuntimeError, match="synthetic-typed-failure"):
            await _materialize_semantic_dataset(database, dataset_id)

        assert await _dataset_and_shard_counts(
            database,
            schema,
            dataset_id,
        ) == (1, 0)
        assert await _stored_dataset_row(database, schema, dataset_id) == retained_before
        assert await _compatibility_counts(database, schema) == (0, 0, 0)
        monkeypatch.setattr(output_stage, "copy_output_rows", original_copy)
        await _materialize_semantic_dataset(database, dataset_id)
        assert await _compatibility_counts(database, schema) == (1, 1, 1)


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
            "hash_contract_changed",
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
        with pytest.raises(source_storage.GenericCandidateError, match=expected_error):
            async with source_transaction(database), database.acquire() as connection:
                await importer._upsert_dataset_resource_rows_on_connection(
                    connection,
                    [incoming_row],
                    persist_content_proof=True,
                    resource_hash_contract=(SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT),
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
            selected_resources=("Organization",),
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
        async with source_transaction(database), database.acquire() as connection:
            await importer._upsert_dataset_resource_rows_on_connection(
                connection,
                [dataset_row_by_field],
                persist_content_proof=True,
                resource_hash_contract=resource_hash_contract,
                semantic_projection_as_of=None,
            )
        stored_proof = await _candidate_stored_proof(database, database.semantic_candidates[dataset_id])
        assert stored_proof.resource_count == 1
        assert await _dataset_and_shard_counts(
            database,
            schema,
            dataset_id,
        ) == (1, 1)
