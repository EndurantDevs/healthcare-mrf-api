# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_proof_store as proof_store
from process.provider_directory_proof_store import (
    ProviderDirectoryProofStoreError,
    ProviderDirectoryStoredProofOptions,
    build_stored_dataset_proof,
    persist_dataset_proof_shard,
)
from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    resource_payload_sha256_for_contract,
)
from tests.test_provider_directory_proof_store import (
    DATASET_ID,
    ENDPOINT_ID,
    ROOT_RUN_ID,
    SELECTED_RESOURCES,
    SOURCE_IDS,
    _MemoryProofConnection,
    _dataset_resource,
)


def test_payload_metrics_and_row_validation_cover_each_resource_shape():
    assert proof_store._clean_text(None) == ""
    assert proof_store._payload_metrics(
        "Organization",
        {"npi": 123, "address_json": [{}]},
    ) == ("123", 1, 0, 0)
    assert proof_store._payload_metrics(
        "Practitioner",
        {"npi": "123", "addresses": [{}]},
    ) == ("123", 1, 0, 0)
    assert proof_store._payload_metrics(
        "Location",
        {
            "first_line": "1 Main",
            "latitude": "41",
            "longitude": "-87",
        },
    ) == ("", 0, 1, 1)
    assert proof_store._payload_metrics("InsurancePlan", {}) == ("", 0, 0, 0)

    valid_row_by_field = _dataset_resource(
        "Practitioner",
        "p",
        {"npi": "123"},
    )
    for mutation_by_field in (
        {"resource_type": ""},
        {"resource_id": ""},
        {"payload_hash": "bad"},
        {"payload_json": []},
    ):
        resource_row_by_field = {
            **valid_row_by_field,
            **mutation_by_field,
        }
        with pytest.raises(
            ProviderDirectoryProofStoreError,
            match="row is invalid",
        ):
            proof_store._proof_record(resource_row_by_field)


def test_batch_shard_rejects_conflicts_empty_lineage_and_mixed_families():
    original = _dataset_resource("Practitioner", "same", {"npi": "123"})
    changed = _dataset_resource("Practitioner", "same", {"npi": "456"})
    with pytest.raises(ProviderDirectoryProofStoreError, match="identity conflicts"):
        proof_store._framed_records([original, changed])
    for lineage_fields in (
        ("", ENDPOINT_ID, ROOT_RUN_ID, SOURCE_IDS),
        (DATASET_ID, "", ROOT_RUN_ID, SOURCE_IDS),
        (DATASET_ID, ENDPOINT_ID, "", SOURCE_IDS),
        (DATASET_ID, ENDPOINT_ID, ROOT_RUN_ID, [""]),
    ):
        with pytest.raises(ProviderDirectoryProofStoreError, match="lineage"):
            proof_store._proof_shard_lineage(*lineage_fields)
    with pytest.raises(ProviderDirectoryProofStoreError, match="is empty"):
        proof_store.build_dataset_proof_shard(
            [],
            dataset_id=DATASET_ID,
            endpoint_id=ENDPOINT_ID,
            acquisition_root_run_id=ROOT_RUN_ID,
            source_ids=SOURCE_IDS,
        )
    with pytest.raises(ProviderDirectoryProofStoreError, match="resource families"):
        proof_store.build_dataset_proof_shard(
            [
                _dataset_resource("Practitioner", "p", {"npi": "123"}),
                _dataset_resource("Location", "l", {}),
            ],
            dataset_id=DATASET_ID,
            endpoint_id=ENDPOINT_ID,
            acquisition_root_run_id=ROOT_RUN_ID,
            source_ids=SOURCE_IDS,
        )


def test_row_mapping_accepts_none_mapping_and_record_wrapper():
    assert proof_store._row_mapping(None) == {}
    assert proof_store._row_mapping({"a": 1}) == {"a": 1}
    assert proof_store._row_mapping(SimpleNamespace(_mapping={"b": 2})) == {
        "b": 2
    }
    assert proof_store._row_mapping(object()) == {}


@pytest.mark.asyncio
async def test_locked_parent_lineage_decodes_serialized_metadata():
    """Decode a valid historical parent without inventing semantic fields."""

    connection = SimpleNamespace(
        first=AsyncMock(
            return_value={
                "endpoint_id": ENDPOINT_ID,
                "acquisition_root_run_id": ROOT_RUN_ID,
                "publication_metadata_json": json.dumps(
                    {
                        "source_ids": SOURCE_IDS,
                        "selected_resources": SELECTED_RESOURCES,
                    }
                ),
            }
        )
    )

    assert await proof_store._locked_dataset_proof_lineage(
        connection,
        "mrf",
        DATASET_ID,
    ) == (
        ENDPOINT_ID,
        ROOT_RUN_ID,
        SOURCE_IDS,
        SELECTED_RESOURCES,
        None,
        "transport_bound_v1",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("metadata_key", "invalid_value", "error_pattern"),
    (
        ("source_ids", None, "source scope"),
        ("source_ids", [], "source scope"),
        ("source_ids", [""], "source scope"),
        ("selected_resources", None, "resource scope"),
        ("selected_resources", [], "resource scope"),
        ("selected_resources", [" Practitioner"], "resource scope"),
        (
            "selected_resources",
            ["Practitioner", "Practitioner"],
            "resource scope",
        ),
        (
            "selected_resources",
            ["Practitioner", "Organization"],
            "resource scope",
        ),
    ),
)
async def test_locked_parent_lineage_rejects_invalid_scope(
    metadata_key,
    invalid_value,
    error_pattern,
):
    """Reject every malformed source or selected-resource parent scope."""

    metadata_by_field = {
        "source_ids": SOURCE_IDS,
        "selected_resources": SELECTED_RESOURCES,
        metadata_key: invalid_value,
    }
    connection = SimpleNamespace(
        first=AsyncMock(
            return_value={
                "endpoint_id": ENDPOINT_ID,
                "acquisition_root_run_id": ROOT_RUN_ID,
                "publication_metadata_json": metadata_by_field,
            }
        )
    )

    with pytest.raises(
        ProviderDirectoryProofStoreError,
        match=error_pattern,
    ):
        await proof_store._locked_dataset_proof_lineage(
            connection,
            "mrf",
            DATASET_ID,
        )


def _semantic_resource(resource_type, resource_id, payload):
    resource_row_by_field = _dataset_resource(
        resource_type,
        resource_id,
        payload,
    )
    resource_row_by_field["payload_hash"] = (
        resource_payload_sha256_for_contract(
            payload,
            SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        )
    )
    return resource_row_by_field


def _semantic_scope_connection() -> _MemoryProofConnection:
    connection = _MemoryProofConnection()
    connection.parent["publication_metadata_json"] = {
        "source_ids": SOURCE_IDS,
        "selected_resources": ["Practitioner"],
        "proof_resource_scope": ["Practitioner"],
        "resource_hash_contract": SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    }
    return connection


@pytest.mark.asyncio
async def test_persisted_shard_rejects_resource_outside_parent_scope():
    connection = _semantic_scope_connection()
    organization = _semantic_resource(
        "Organization",
        "organization-outside-scope",
        {"name": "Example"},
    )

    with pytest.raises(
        ProviderDirectoryProofStoreError,
        match="parent resource scope changed",
    ):
        await persist_dataset_proof_shard(
            connection,
            "mrf",
            [organization],
            dataset_id=DATASET_ID,
        )
    assert connection.shards == {}


async def _insert_unscoped_semantic_organization(
    connection: _MemoryProofConnection,
) -> None:
    """Bypass the parent fence to model a tampered durable proof shard."""

    organization = _semantic_resource(
        "Organization",
        "organization-1",
        {"name": "Example"},
    )
    descriptor_by_field, compressed = proof_store.build_dataset_proof_shard(
        [organization],
        dataset_id=DATASET_ID,
        endpoint_id=ENDPOINT_ID,
        acquisition_root_run_id=ROOT_RUN_ID,
        source_ids=SOURCE_IDS,
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    )
    await connection.status(
        "INSERT INTO provider_directory_dataset_proof_shard",
        **proof_store._proof_shard_insert_params(
            descriptor_by_field,
            compressed,
        ),
    )


@pytest.mark.asyncio
async def test_stored_semantic_proof_rejects_extra_resource_family():
    """Reject a durable family outside the exact semantic proof closure."""

    connection = _semantic_scope_connection()
    practitioner = _semantic_resource(
        "Practitioner",
        "practitioner-1",
        {
            "npi": "123",
            "names": [{"family": "Example", "given": ["A"]}],
            "family_name": "Example",
            "given_names": ["A"],
            "full_name": "A Example",
        },
    )
    await persist_dataset_proof_shard(
        connection,
        "mrf",
        [practitioner],
        dataset_id=DATASET_ID,
    )
    await _insert_unscoped_semantic_organization(connection)

    with pytest.raises(
        ProviderDirectoryProofStoreError,
        match="resource scope changed",
    ):
        await build_stored_dataset_proof(
            connection,
            "mrf",
            dataset_id=DATASET_ID,
            endpoint_id=ENDPOINT_ID,
            acquisition_root_run_id=ROOT_RUN_ID,
            source_ids=SOURCE_IDS,
            selected_resources=["Practitioner"],
            options=ProviderDirectoryStoredProofOptions(
                proof_resource_scope=["Practitioner"],
                expected_resource_hash_contract=(
                    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
                ),
                expected_semantic_projection_as_of="2026-08-09",
            ),
        )


def test_exact_resource_maps_reject_an_extra_family():
    with pytest.raises(
        ProviderDirectoryProofStoreError,
        match="resource scope is invalid",
    ):
        proof_store._validated_resource_maps(
            {
                "resource_counts": {
                    "Organization": 1,
                    "Practitioner": 1,
                },
                "resource_hashes": {
                    "Organization": "a" * 64,
                    "Practitioner": "b" * 64,
                },
            },
            ["Practitioner"],
            exact_scope=True,
        )


@pytest.mark.asyncio
async def test_persisted_shard_replay_rejects_mutated_stored_fields():
    connection = _MemoryProofConnection()
    resource_row_by_field = _dataset_resource(
        "Practitioner",
        "p",
        {"npi": "123"},
    )
    descriptor_by_field = await persist_dataset_proof_shard(
        connection,
        "mrf",
        [resource_row_by_field],
        dataset_id=DATASET_ID,
    )
    connection.shards[(DATASET_ID, descriptor_by_field["shard_id"])][
        "endpoint_id"
    ] = "changed"

    with pytest.raises(ProviderDirectoryProofStoreError, match="replay changed"):
        await persist_dataset_proof_shard(
            connection,
            "mrf",
            [resource_row_by_field],
            dataset_id=DATASET_ID,
        )
