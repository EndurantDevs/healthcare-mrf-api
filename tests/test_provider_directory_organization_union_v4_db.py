# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real-PostgreSQL convergence for semantic-v4 Organization unions."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.models import ProviderDirectoryOrganization
from process.provider_directory_proof_store import (
    PROVIDER_DIRECTORY_SEMANTIC_CONTENT_V4_PROOF_CONTRACT_ID,
)
from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
)
from tests.provider_directory_organization_union_v4_postgres_support import (
    RESOURCE_ID,
    V4_DATASET_CONCURRENT,
    V4_DATASET_FORWARD,
    V4_DATASET_REVERSE,
    V4_DATASET_ROLLBACK,
    materialized_organization,
    organization_observation,
    stored_dataset_row,
    stored_proof,
    write_page,
    write_partition_page,
)
from tests.provider_directory_semantic_proof_v3_postgres_support import (
    PROJECTION_AS_OF,
    SOURCE_ID,
    _dataset_and_shard_counts,
    _insert_parent,
    _organization_observation,
    _semantic_database,
    _shard_records,
    _write_organization_page,
    importer,
)


def _observations() -> tuple[dict[str, object], dict[str, object]]:
    """Return overlapping primary and alias observations."""

    return (
        organization_observation(
            "Community Health Center",
            aliases=["Regional Clinic"],
            page_number=1,
            transport_host="a.example.test",
        ),
        organization_observation(
            "COMMUNITY HEALTH SERVICES",
            aliases=["Community Health Center"],
            page_number=2,
            transport_host="z.example.test",
        ),
    )


async def _write_opposite_orders(monkeypatch) -> None:
    first, second = _observations()
    await write_page(V4_DATASET_FORWARD, first)
    await write_page(V4_DATASET_FORWARD, second)
    await write_partition_page(monkeypatch, V4_DATASET_REVERSE, second)
    await write_partition_page(monkeypatch, V4_DATASET_REVERSE, first)


async def _representation_counts(database, schema: str) -> tuple[int, int, int]:
    """Count typed, canonical, and source-edge Organization rows."""

    counts = await database.first(
        f'SELECT (SELECT count(*) FROM "{schema}".'
        "provider_directory_organization WHERE source_id=:source_id), "
        f'(SELECT count(*) FROM "{schema}".'
        "provider_directory_canonical_resource "
        "WHERE resource_type='Organization'), "
        f'(SELECT count(*) FROM "{schema}".'
        "provider_directory_source_resource "
        "WHERE resource_type='Organization');",
        source_id=SOURCE_ID,
    )
    return tuple(int(value) for value in counts)


async def _v3_organization_record(database, schema: str, dataset_id: str):
    """Return the exact retained and typed pre-union Organization state."""

    return await database.first(
        f'SELECT resource.payload_hash, resource.payload_json, typed.name, '
        f'typed.name_variants FROM "{schema}".'
        "provider_directory_dataset_resource resource JOIN "
        f'"{schema}".provider_directory_organization typed '
        "ON typed.source_id=:source_id AND typed.resource_id=resource.resource_id "
        "WHERE resource.dataset_id=:dataset_id "
        "AND resource.resource_type='Organization';",
        source_id=SOURCE_ID,
        dataset_id=dataset_id,
    )


async def _run_serialized_writes(monkeypatch) -> None:
    """Force the second write to wait behind the first typed write."""

    first_at_typed_write = asyncio.Event()
    release_first_write = asyncio.Event()
    original_upsert_rows = importer._upsert_rows
    write_state = SimpleNamespace(should_block=True)

    async def controlled_upsert(model, rows, **options):
        if model is ProviderDirectoryOrganization and write_state.should_block:
            write_state.should_block = False
            first_at_typed_write.set()
            await release_first_write.wait()
        return await original_upsert_rows(model, rows, **options)

    monkeypatch.setattr(importer, "_upsert_rows", controlled_upsert)
    first, second = _observations()
    first_task = asyncio.create_task(write_page(V4_DATASET_CONCURRENT, first))
    second_task = None
    try:
        await asyncio.wait_for(first_at_typed_write.wait(), timeout=2)
        second_task = asyncio.create_task(
            write_page(V4_DATASET_CONCURRENT, second)
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


def _assert_union_payload(payload_by_field: dict[str, object]) -> None:
    """Require the exact retained primary-name and alias projection."""

    assert payload_by_field["name"] == "Community Health Center"
    assert payload_by_field["name_variants"] == [
        "Community Health Center",
        "COMMUNITY HEALTH SERVICES",
    ]
    assert payload_by_field["aliases"] == [
        "COMMUNITY HEALTH SERVICES",
        "Regional Clinic",
    ]


async def _assert_ordered_proof_parity(database, schema: str) -> None:
    """Require forward, reverse, shard, and direct proofs to converge."""

    forward_proof = await stored_proof(
        database, schema, V4_DATASET_FORWARD
    )
    reverse_proof = await stored_proof(
        database, schema, V4_DATASET_REVERSE
    )
    assert forward_proof.dataset_hash == reverse_proof.dataset_hash
    assert forward_proof.metadata["contract_id"] == (
        PROVIDER_DIRECTORY_SEMANTIC_CONTENT_V4_PROOF_CONTRACT_ID
    )
    assert forward_proof.metadata["semantic_union"] == {
        "added_name_count": 1,
        "collision_identities": 1,
        "observation_variants": 2,
        "union_name_count": 2,
    }
    for dataset_id, expected_hash in (
        (V4_DATASET_FORWARD, forward_proof.dataset_hash),
        (V4_DATASET_REVERSE, reverse_proof.dataset_hash),
    ):
        direct_proof = await importer._endpoint_dataset_content_proof(
            database,
            dataset_id,
            ("Organization",),
            verify_payload_hashes=True,
            resource_hash_contract=(
                SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
            ),
        )
        assert direct_proof.dataset_hash == expected_hash
    forward_inputs, _forward_records = await _shard_records(
        database, schema, V4_DATASET_FORWARD
    )
    reverse_inputs, _reverse_records = await _shard_records(
        database, schema, V4_DATASET_REVERSE
    )
    assert forward_inputs == reverse_inputs


@pytest.mark.asyncio
async def test_postgres_v4_organization_union_is_order_independent(
    monkeypatch,
) -> None:
    """Converge normal and partition writes on one retained sealed proof."""

    async with _semantic_database(monkeypatch) as (database, schema):
        for dataset_id in (V4_DATASET_FORWARD, V4_DATASET_REVERSE):
            await _insert_parent(
                database,
                schema,
                dataset_id,
                resource_hash_contract=(
                    SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
                ),
                selected_resources=("Organization",),
            )
        await _write_opposite_orders(monkeypatch)
        forward_hash, forward_payload = await stored_dataset_row(
            database, schema, V4_DATASET_FORWARD
        )
        reverse_hash, reverse_payload = await stored_dataset_row(
            database, schema, V4_DATASET_REVERSE
        )
        assert (forward_hash, forward_payload) == (reverse_hash, reverse_payload)
        _assert_union_payload(forward_payload)
        await _assert_ordered_proof_parity(database, schema)
        typed_by_field, canonical_hash = await materialized_organization(
            database, schema
        )
        _assert_union_payload(typed_by_field)
        assert canonical_hash == forward_hash


@pytest.mark.asyncio
async def test_postgres_v4_concurrent_first_insert_is_serialized(
    monkeypatch,
) -> None:
    """Prevent a same-identity first-insert race from losing either name."""

    async with _semantic_database(monkeypatch) as (database, schema):
        await _insert_parent(
            database,
            schema,
            V4_DATASET_CONCURRENT,
            resource_hash_contract=SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
            selected_resources=("Organization",),
        )
        await _run_serialized_writes(monkeypatch)
        payload_hash, payload_by_field = await stored_dataset_row(
            database, schema, V4_DATASET_CONCURRENT
        )
        _assert_union_payload(payload_by_field)
        assert await _dataset_and_shard_counts(
            database, schema, V4_DATASET_CONCURRENT
        ) == (1, 2)
        typed_by_field, canonical_hash = await materialized_organization(
            database, schema
        )
        _assert_union_payload(typed_by_field)
        assert canonical_hash == payload_hash


async def _assert_typed_failure_rollback(
    monkeypatch,
    database,
    schema: str,
    second_payload_by_field: dict[str, object],
):
    """Fail after proof persistence and require every representation rollback."""

    before_row = await stored_dataset_row(
        database, schema, V4_DATASET_ROLLBACK
    )
    before_shards = await _shard_records(
        database, schema, V4_DATASET_ROLLBACK
    )
    before_counts = await _representation_counts(database, schema)
    original_upsert_rows = importer._upsert_rows
    persist_shard = AsyncMock(wraps=importer.persist_dataset_proof_shard)
    monkeypatch.setattr(importer, "persist_dataset_proof_shard", persist_shard)

    async def fail_typed_write(model, rows, **options):
        if model is ProviderDirectoryOrganization:
            raise RuntimeError("organization typed write failed")
        return await original_upsert_rows(model, rows, **options)

    monkeypatch.setattr(importer, "_upsert_rows", fail_typed_write)
    with pytest.raises(RuntimeError, match="typed write failed"):
        await write_page(V4_DATASET_ROLLBACK, second_payload_by_field)
    assert persist_shard.await_count == 1
    assert await stored_dataset_row(
        database, schema, V4_DATASET_ROLLBACK
    ) == before_row
    assert await _shard_records(
        database, schema, V4_DATASET_ROLLBACK
    ) == before_shards
    assert await _representation_counts(database, schema) == before_counts
    return original_upsert_rows


@pytest.mark.asyncio
async def test_postgres_v4_typed_failure_rolls_back_and_retries(
    monkeypatch,
) -> None:
    """Rollback every representation after proof persistence, then retry once."""

    async with _semantic_database(monkeypatch) as (database, schema):
        await _insert_parent(
            database,
            schema,
            V4_DATASET_ROLLBACK,
            resource_hash_contract=SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
            selected_resources=("Organization",),
        )
        first, second = _observations()
        await write_page(V4_DATASET_ROLLBACK, first)
        original_upsert_rows = await _assert_typed_failure_rollback(
            monkeypatch,
            database,
            schema,
            second,
        )
        monkeypatch.setattr(importer, "_upsert_rows", original_upsert_rows)
        await write_page(V4_DATASET_ROLLBACK, second)
        payload_hash, payload_by_field = await stored_dataset_row(
            database, schema, V4_DATASET_ROLLBACK
        )
        _assert_union_payload(payload_by_field)
        assert await _dataset_and_shard_counts(
            database, schema, V4_DATASET_ROLLBACK
        ) == (1, 2)
        _typed_by_field, canonical_hash = await materialized_organization(
            database, schema
        )
        assert canonical_hash == payload_hash


@pytest.mark.asyncio
async def test_postgres_v3_organization_name_drift_remains_strict(
    monkeypatch,
) -> None:
    """Keep pre-v4 Organization hashing and proof records unchanged."""

    dataset_id = "dataset-organization-v3-strict"
    async with _semantic_database(monkeypatch) as (database, schema):
        await _insert_parent(
            database,
            schema,
            dataset_id,
            selected_resources=("Organization",),
        )
        first = _organization_observation("a.example.test", page_number=1)
        second_payload_by_field = {
            **_organization_observation("z.example.test", page_number=2),
            "name": "Changed Organization Name",
        }
        await _write_organization_page(dataset_id, first)
        before_record = await _v3_organization_record(
            database, schema, dataset_id
        )
        before_shards = await _shard_records(database, schema, dataset_id)

        with pytest.raises(ValueError, match="resource_payload_conflict"):
            await _write_organization_page(
                dataset_id,
                second_payload_by_field,
            )
        assert await _v3_organization_record(
            database, schema, dataset_id
        ) == before_record
        assert await _shard_records(database, schema, dataset_id) == before_shards
        assert before_record[2] == "Example Organization"
        assert before_record[3] is None
        proof_record = before_shards[1][0]
        assert proof_record[7:] == [
            SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
            before_record[0],
            [],
        ]
