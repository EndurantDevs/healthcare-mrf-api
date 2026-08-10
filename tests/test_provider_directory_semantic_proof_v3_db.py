# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL ordering and materialization proof for semantic content."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest

from db.models import ProviderDirectoryPractitioner
from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    resource_payload_sha256_for_contract,
)
from tests.provider_directory_semantic_proof_v3_postgres_support import (
    CANONICAL_BASE,
    PROJECTION_AS_OF,
    RESOURCE_ID,
    SOURCE_ID,
    V3_DATASET_CONCURRENT,
    V3_DATASET_FORWARD,
    V3_DATASET_REVERSE,
    _dataset_and_shard_counts,
    _dataset_row,
    _insert_parent,
    _observation,
    _organization_observation,
    _proof,
    _semantic_database,
    _shard_records,
    _stored_dataset_row,
    _write_organization_page,
    _write_page,
    _write_partition_page,
    importer,
)


async def _write_opposite_order_pages(monkeypatch):
    first_observation = _observation("Alex Example", page_number=1)
    second_observation = _observation("Avery Sample", page_number=2)
    await _write_page(V3_DATASET_FORWARD, first_observation)
    await _write_page(V3_DATASET_FORWARD, second_observation)
    await _write_partition_page(
        monkeypatch,
        V3_DATASET_REVERSE,
        second_observation,
    )
    await _write_partition_page(
        monkeypatch,
        V3_DATASET_REVERSE,
        first_observation,
    )


async def _assert_union_proofs_match(database, schema):
    forward_proof = await _proof(database, schema, V3_DATASET_FORWARD)
    reverse_proof = await _proof(database, schema, V3_DATASET_REVERSE)
    assert (
        forward_proof.dataset_hash,
        forward_proof.resource_hashes,
        forward_proof.resource_counts,
    ) == (
        reverse_proof.dataset_hash,
        reverse_proof.resource_hashes,
        reverse_proof.resource_counts,
    )
    assert forward_proof.metadata["semantic_union"] == {
        "added_name_count": 1,
        "collision_identities": 1,
        "observation_variants": 2,
        "union_name_count": 2,
    }
    return forward_proof


async def _assert_union_shards_match(database, schema):
    forward_inputs, forward_records = await _shard_records(
        database,
        schema,
        V3_DATASET_FORWARD,
    )
    reverse_inputs, reverse_records = await _shard_records(
        database,
        schema,
        V3_DATASET_REVERSE,
    )
    assert forward_inputs == reverse_inputs
    assert len(forward_records) == len(reverse_records) == 2
    proof_record_fields = [*forward_records, *reverse_records]
    assert all(
        len(proof_record) == 10
        and proof_record[7] == SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
        and len(proof_record[9]) == 1
        for proof_record in proof_record_fields
    )


async def _assert_practitioner_materialization(
    database,
    schema,
    dataset_id,
    expected_payload,
    expected_hash,
):
    typed_names = await database.scalar(
        f'SELECT names FROM "{schema}".provider_directory_practitioner '
        "WHERE source_id=:source_id AND resource_id=:resource_id;",
        source_id=SOURCE_ID,
        resource_id=RESOURCE_ID,
    )
    canonical_record = await database.first(
        f'SELECT payload_hash, payload_json FROM "{schema}".'
        "provider_directory_canonical_resource "
        "WHERE canonical_api_base=:canonical_base "
        "AND resource_type='Practitioner' AND resource_id=:resource_id;",
        canonical_base=CANONICAL_BASE,
        resource_id=RESOURCE_ID,
    )
    assert list(typed_names) == expected_payload["names"]
    assert canonical_record[0] == expected_hash
    assert canonical_record[1] is None


async def _run_serialized_practitioner_writes(monkeypatch):
    first_at_typed_write = asyncio.Event()
    release_first_write = asyncio.Event()
    original_upsert_rows = importer._upsert_rows
    typed_write_state = SimpleNamespace(should_block=True)

    async def controlled_upsert(model, rows, **kwargs):
        if model is ProviderDirectoryPractitioner and typed_write_state.should_block:
            typed_write_state.should_block = False
            first_at_typed_write.set()
            await release_first_write.wait()
        return await original_upsert_rows(model, rows, **kwargs)

    monkeypatch.setattr(importer, "_upsert_rows", controlled_upsert)
    first_task = asyncio.create_task(
        _write_page(
            V3_DATASET_CONCURRENT,
            _observation("Alex Example", page_number=1),
        )
    )
    second_task: asyncio.Task | None = None
    try:
        await asyncio.wait_for(first_at_typed_write.wait(), timeout=2)
        second_task = asyncio.create_task(
            _write_page(
                V3_DATASET_CONCURRENT,
                _observation("Avery Sample", page_number=2),
            )
        )
        await asyncio.sleep(0.1)
        assert second_task.done() is False
        release_first_write.set()
        await asyncio.gather(first_task, second_task)
    finally:
        release_first_write.set()
        await asyncio.gather(
            first_task,
            *([second_task] if second_task is not None else []),
            return_exceptions=True,
        )


def _organization_dataset_row(dataset_id):
    organization_payload_by_field = {
        "resource_id": "organization-semantic-proof",
        "name": "Example Organization",
        "fhir_meta": {
            "versionId": "1",
            "lastUpdated": "2026-08-09T12:00:01Z",
        },
    }
    return {
        "dataset_id": dataset_id,
        "resource_type": "Organization",
        "resource_id": organization_payload_by_field["resource_id"],
        "payload_hash": resource_payload_sha256_for_contract(
            organization_payload_by_field,
            SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        ),
        "payload_json": organization_payload_by_field,
        "acquired_resource_sha256": None,
    }


async def _run_parallel_family_writes(monkeypatch, database, dataset_id):
    """Prove the family-scoped lock leaves sibling families independent."""

    practitioner_family_locked = asyncio.Event()
    release_practitioner_family = asyncio.Event()
    organization_family_complete = asyncio.Event()
    original_existing_resources = importer._existing_endpoint_dataset_resources
    async def controlled_existing_resources(
        executor,
        observed_dataset_id,
        resource_type,
        resource_ids,
    ):
        if resource_type == "Practitioner":
            practitioner_family_locked.set()
            await release_practitioner_family.wait()
        return await original_existing_resources(
            executor,
            observed_dataset_id,
            resource_type,
            resource_ids,
        )
    async def write_rows(dataset_rows):
        async with database.acquire() as connection:
            await importer._upsert_dataset_resource_rows_on_connection(
                connection,
                dataset_rows,
                persist_content_proof=True,
                resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
                semantic_projection_as_of=PROJECTION_AS_OF,
            )
    async def write_organization_family():
        await write_rows([_organization_dataset_row(dataset_id)])
        organization_family_complete.set()

    monkeypatch.setattr(
        importer,
        "_existing_endpoint_dataset_resources",
        controlled_existing_resources,
    )
    practitioner_row = _dataset_row(
        dataset_id,
        _observation("Alex Example", page_number=1),
    )
    practitioner_task = asyncio.create_task(write_rows([practitioner_row]))
    organization_task: asyncio.Task | None = None
    try:
        await asyncio.wait_for(practitioner_family_locked.wait(), timeout=2)
        organization_task = asyncio.create_task(write_organization_family())
        await asyncio.wait_for(organization_family_complete.wait(), timeout=2)
        assert practitioner_task.done() is False
        release_practitioner_family.set()
        await asyncio.gather(practitioner_task, organization_task)
    finally:
        release_practitioner_family.set()
        await asyncio.gather(
            practitioner_task,
            *([organization_task] if organization_task is not None else []),
            return_exceptions=True,
        )


@pytest.mark.asyncio
async def test_postgres_v3_union_is_page_order_independent_across_write_paths(
    monkeypatch,
):
    """Prove page order and write path cannot change semantic content."""

    async with _semantic_database(monkeypatch) as (database, schema):
        await _insert_parent(database, schema, V3_DATASET_FORWARD)
        await _insert_parent(database, schema, V3_DATASET_REVERSE)
        await _write_opposite_order_pages(monkeypatch)
        forward_hash, forward_payload = await _stored_dataset_row(
            database,
            schema,
            V3_DATASET_FORWARD,
        )
        reverse_hash, reverse_payload = await _stored_dataset_row(
            database,
            schema,
            V3_DATASET_REVERSE,
        )
        assert forward_payload == reverse_payload
        assert forward_hash == reverse_hash
        assert len(forward_payload["names"]) == 2
        forward_proof = await _assert_union_proofs_match(database, schema)
        direct_proof = await importer._endpoint_dataset_content_proof(
            database,
            V3_DATASET_FORWARD,
            ("Practitioner",),
            verify_payload_hashes=True,
            resource_hash_contract=(
                SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
            ),
        )
        assert direct_proof.dataset_hash == forward_proof.dataset_hash
        await _assert_union_shards_match(database, schema)
        await _assert_practitioner_materialization(
            database,
            schema,
            V3_DATASET_FORWARD,
            forward_payload,
            forward_hash,
        )


@pytest.mark.asyncio
async def test_postgres_v3_concurrent_first_insert_is_serialized(monkeypatch):
    """Serialize first inserts and retain every observed Practitioner name."""

    async with _semantic_database(monkeypatch) as (database, schema):
        await _insert_parent(database, schema, V3_DATASET_CONCURRENT)
        await _run_serialized_practitioner_writes(monkeypatch)
        payload_hash, dataset_payload = await _stored_dataset_row(
            database,
            schema,
            V3_DATASET_CONCURRENT,
        )
        assert len(dataset_payload["names"]) == 2
        assert await _dataset_and_shard_counts(
            database,
            schema,
            V3_DATASET_CONCURRENT,
        ) == (1, 2)
        await _assert_practitioner_materialization(
            database,
            schema,
            V3_DATASET_CONCURRENT,
            dataset_payload,
            payload_hash,
        )


@pytest.mark.asyncio
async def test_postgres_v3_distinct_resource_families_remain_parallel(
    monkeypatch,
):
    """Keep one slow semantic family from serializing sibling families."""

    async with _semantic_database(monkeypatch) as (database, schema):
        dataset_id = "dataset-semantic-parallel-families"
        await _insert_parent(
            database,
            schema,
            dataset_id,
            selected_resources=("Organization", "Practitioner"),
        )
        await _run_parallel_family_writes(monkeypatch, database, dataset_id)
        assert await _dataset_and_shard_counts(
            database,
            schema,
            dataset_id,
        ) == (2, 2)


@pytest.mark.asyncio
async def test_postgres_v3_non_practitioner_materialization_uses_retained_winner(
    monkeypatch,
):
    """Keep typed and canonical provenance aligned with deterministic v3 data."""

    async with _semantic_database(monkeypatch) as (database, schema):
        dataset_id = "dataset-semantic-organization-winner"
        await _insert_parent(
            database,
            schema,
            dataset_id,
            selected_resources=("Organization",),
        )
        winning_observation = _organization_observation(
            "z.example.test",
            page_number=2,
        )
        losing_observation = _organization_observation(
            "a.example.test",
            page_number=1,
        )

        await _write_organization_page(dataset_id, winning_observation)
        await _write_organization_page(dataset_id, losing_observation)

        dataset_record = await database.first(
            f'SELECT payload_hash, payload_json FROM "{schema}".'
            "provider_directory_dataset_resource "
            "WHERE dataset_id=:dataset_id AND resource_type='Organization' "
            "AND resource_id=:resource_id;",
            dataset_id=dataset_id,
            resource_id="organization-semantic-proof",
        )
        dataset_payload = dataset_record[1]
        if isinstance(dataset_payload, str):
            dataset_payload = json.loads(dataset_payload)
        typed_record = await database.first(
            f'SELECT resource_url, fhir_meta FROM "{schema}".'
            "provider_directory_organization WHERE source_id=:source_id "
            "AND resource_id=:resource_id;",
            source_id=SOURCE_ID,
            resource_id="organization-semantic-proof",
        )
        canonical_record = await database.first(
            f'SELECT resource_url, fhir_meta, payload_hash FROM "{schema}".'
            "provider_directory_canonical_resource "
            "WHERE canonical_api_base=:canonical_base "
            "AND resource_type='Organization' AND resource_id=:resource_id;",
            canonical_base=CANONICAL_BASE,
            resource_id="organization-semantic-proof",
        )

        assert dataset_payload["resource_url"] == (
            "https://z.example.test/organization-semantic-proof"
        )
        assert typed_record[0] == dataset_payload["resource_url"]
        assert typed_record[1] == dataset_payload["fhir_meta"]
        assert canonical_record[0] == dataset_payload["resource_url"]
        assert canonical_record[1] == dataset_payload["fhir_meta"]
        assert canonical_record[2] == dataset_record[0]
