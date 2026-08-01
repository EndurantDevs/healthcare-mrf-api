# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import copy
import hashlib
import importlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
import zlib

import pytest

from process import provider_directory_proof_store as proof_store
from process.provider_directory_proof_store import (
    PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY,
    ProviderDirectoryProofStoreError,
    build_stored_dataset_proof,
    persist_dataset_proof_shard,
    validate_stored_dataset_proof_metadata,
)


importer = importlib.import_module("process.provider_directory_fhir")


DATASET_ID = "dataset-proof"
ENDPOINT_ID = "endpoint-proof"
ROOT_RUN_ID = "root-proof"
SOURCE_IDS = ["source-a", "source-b"]
SELECTED_RESOURCES = [
    "InsurancePlan",
    "Location",
    "Organization",
    "OrganizationAffiliation",
    "Practitioner",
]


def _stable_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _dataset_resource(resource_type, resource_id, payload):
    return {
        "dataset_id": DATASET_ID,
        "resource_type": resource_type,
        "resource_id": resource_id,
        "payload_hash": hashlib.sha256(
            json.dumps(payload, sort_keys=True).encode()
        ).hexdigest(),
        "payload_json": payload,
    }


def _sample_dataset_resources():
    return [
        _dataset_resource(
            "Practitioner",
            "practitioner-1",
            {"npi": "123", "addresses": [{}, {}]},
        ),
        _dataset_resource(
            "Organization",
            "organization-1",
            {"npi": "123", "address_json": [{}]},
        ),
        _dataset_resource(
            "Location",
            "location-1",
            {
                "addresses": [{}],
                "latitude": 0,
                "longitude": -90,
            },
        ),
        _dataset_resource("InsurancePlan", "plan-1", {"name": "Plan"}),
    ]


class _MemoryProofConnection:
    def __init__(self):
        self.shards = {}
        self.parent = {
            "endpoint_id": ENDPOINT_ID,
            "acquisition_root_run_id": ROOT_RUN_ID,
            "publication_metadata_json": {"source_ids": SOURCE_IDS},
        }

    async def status(self, sql, **params):
        if "INSERT INTO" in str(sql):
            key = params["dataset_id"], params["shard_id"]
            self.shards.setdefault(
                key,
                {
                    "shard_id": params["shard_id"],
                    "endpoint_id": params["endpoint_id"],
                    "acquisition_root_run_id": params[
                        "acquisition_root_run_id"
                    ],
                    "source_ids_json": json.loads(params["source_ids_json"]),
                    "resource_count": params["resource_count"],
                    "resource_counts_json": json.loads(
                        params["resource_counts_json"]
                    ),
                    "first_identity_json": json.loads(
                        params["first_identity_json"]
                    ),
                    "last_identity_json": json.loads(
                        params["last_identity_json"]
                    ),
                    "input_sha256": params["input_sha256"],
                    "artifact_sha256": params["artifact_sha256"],
                    "artifact_byte_count": params["artifact_byte_count"],
                    "payload_bytes": params["payload_bytes"],
                },
            )
        return 1

    async def first(self, sql, **params):
        if "provider_directory_endpoint_dataset" in str(sql):
            return self.parent
        return self.shards.get((params["dataset_id"], params["shard_id"]))

    async def all(self, _sql, **params):
        shard_rows = [
            dict(shard_by_field)
            for (dataset_id, shard_id), shard_by_field in sorted(
                self.shards.items()
            )
            if dataset_id == params["dataset_id"]
            and shard_id > params["after_shard_id"]
        ]
        return shard_rows[:128]


async def _stored_proof(connection):
    return await build_stored_dataset_proof(
        connection,
        "mrf",
        dataset_id=DATASET_ID,
        endpoint_id=ENDPOINT_ID,
        acquisition_root_run_id=ROOT_RUN_ID,
        source_ids=SOURCE_IDS,
        selected_resources=SELECTED_RESOURCES,
    )


async def _persist_rows_by_resource(connection, dataset_resources):
    descriptors = []
    for resource_type in sorted(
        {
            dataset_resource["resource_type"]
            for dataset_resource in dataset_resources
        }
    ):
        descriptors.append(
            await persist_dataset_proof_shard(
                connection,
                "mrf",
                [
                    dataset_resource
                    for dataset_resource in dataset_resources
                    if dataset_resource["resource_type"] == resource_type
                ],
                dataset_id=DATASET_ID,
            )
        )
    return descriptors


@pytest.mark.asyncio
async def test_durable_shards_reuse_retried_batch_and_build_exact_summary():
    connection = _MemoryProofConnection()
    dataset_resources = _sample_dataset_resources()

    first = await _persist_rows_by_resource(connection, dataset_resources)
    replay = await _persist_rows_by_resource(
        connection,
        list(reversed(dataset_resources)),
    )
    proof = await _stored_proof(connection)

    assert first == replay
    assert len(connection.shards) == 4
    assert proof.resource_count == 4
    assert proof.resource_counts == {
        "InsurancePlan": 1,
        "Location": 1,
        "Organization": 1,
        "OrganizationAffiliation": 0,
        "Practitioner": 1,
    }
    assert proof.source_metrics == {
        "address_records": 4,
        "addressed_locations": 1,
        "distinct_npis": 1,
        "geocoded_locations": 1,
    }
    identities = sorted(
        _stable_json(
            (
                dataset_resource["resource_type"],
                dataset_resource["resource_id"],
                dataset_resource["payload_hash"],
            )
        )
        for dataset_resource in dataset_resources
    )
    assert proof.dataset_hash == hashlib.sha256(
        "\n".join(identities).encode()
    ).hexdigest()
    assert all(
        shard["dataset_id"] == DATASET_ID
        for shard in proof.metadata["shards"]
    )


@pytest.mark.asyncio
async def test_durable_shards_reject_conflicting_retry_identity():
    connection = _MemoryProofConnection()
    original = _dataset_resource(
        "Practitioner", "practitioner-1", {"npi": "123"}
    )
    changed = _dataset_resource(
        "Practitioner", "practitioner-1", {"npi": "456"}
    )
    await persist_dataset_proof_shard(
        connection, "mrf", [original], dataset_id=DATASET_ID
    )
    await persist_dataset_proof_shard(
        connection, "mrf", [changed], dataset_id=DATASET_ID
    )

    with pytest.raises(
        ProviderDirectoryProofStoreError,
        match="proof shards conflict",
    ):
        await _stored_proof(connection)


@pytest.mark.asyncio
async def test_stored_proof_rejects_tampered_artifact_and_metadata():
    connection = _MemoryProofConnection()
    await _persist_rows_by_resource(connection, _sample_dataset_resources())
    proof = await _stored_proof(connection)
    raw_proof_by_field = dict(proof.metadata)
    raw_proof_by_field["source_metrics"] = {
        **raw_proof_by_field["source_metrics"],
        "distinct_npis": 2,
    }
    with pytest.raises(
        ProviderDirectoryProofStoreError,
        match="content proof changed",
    ):
        validate_stored_dataset_proof_metadata(
            raw_proof_by_field,
            dataset_id=DATASET_ID,
            endpoint_id=ENDPOINT_ID,
            acquisition_root_run_id=ROOT_RUN_ID,
            source_ids=SOURCE_IDS,
            selected_resources=SELECTED_RESOURCES,
        )

    stored_shard = next(iter(connection.shards.values()))
    stored_shard["payload_bytes"] += b"changed"
    with pytest.raises(
        ProviderDirectoryProofStoreError,
        match="proof artifact changed",
    ):
        await _stored_proof(connection)


def test_generic_content_proof_uses_stable_publication_metadata_key():
    assert (
        PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY
        == "provider_directory_content_proof_v1"
    )


def _artifact_context(proof):
    dataset = importer.ProviderDirectoryArtifactDataset(
        source_id="source-a",
        endpoint_id=ENDPOINT_ID,
        dataset_id=DATASET_ID,
        evidence_run_id=ROOT_RUN_ID,
        selected_resources=tuple(SELECTED_RESOURCES),
        expected_resources=tuple(SELECTED_RESOURCES),
        dataset_hash=proof.dataset_hash,
        resource_count=proof.resource_count,
    )
    candidate = importer.EndpointDatasetCandidate(
        endpoint_id=ENDPOINT_ID,
        dataset_id=DATASET_ID,
        acquisition_root_run_id=ROOT_RUN_ID,
        source_ids=tuple(SOURCE_IDS),
        selected_resources=tuple(SELECTED_RESOURCES),
        expected_resources=tuple(SELECTED_RESOURCES),
        import_run_id=ROOT_RUN_ID,
        previous_dataset_id=None,
    )
    return dataset, candidate


@pytest.mark.asyncio
async def test_published_generic_dataset_reuses_metadata_without_json_scan(
    monkeypatch,
):
    """Published source summaries must reuse the sealed content proof."""

    connection = _MemoryProofConnection()
    await _persist_rows_by_resource(connection, _sample_dataset_resources())
    proof = await _stored_proof(connection)
    dataset, candidate = _artifact_context(proof)
    legacy_scan = AsyncMock(
        side_effect=AssertionError("published proof must not scan JSON")
    )
    source_summary = AsyncMock(return_value={"dataset_id": DATASET_ID})
    record_proof = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_endpoint_dataset_content_proof",
        legacy_scan,
    )
    monkeypatch.setattr(
        importer,
        "_endpoint_dataset_source_summary",
        source_summary,
    )
    monkeypatch.setattr(
        importer,
        "_record_current_dataset_publication_proof",
        record_proof,
    )

    observed = await importer._refresh_current_artifact_dataset_source_summary(
        dataset,
        candidate,
        {PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY: proof.metadata},
        {},
    )

    assert observed == {"dataset_id": DATASET_ID}
    legacy_scan.assert_not_awaited()
    assert source_summary.await_args.args[2].source_metrics == (
        proof.source_metrics
    )
    assert record_proof.await_count == 2


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

    valid = _dataset_resource("Practitioner", "p", {"npi": "123"})
    for mutation in (
        {"resource_type": ""},
        {"resource_id": ""},
        {"payload_hash": "bad"},
        {"payload_json": []},
    ):
        resource_row_by_field = {**valid, **mutation}
        with pytest.raises(ProviderDirectoryProofStoreError, match="row is invalid"):
            proof_store._proof_record(resource_row_by_field)


def test_batch_shard_rejects_conflicts_empty_lineage_and_mixed_families():
    original = _dataset_resource("Practitioner", "same", {"npi": "123"})
    changed = _dataset_resource("Practitioner", "same", {"npi": "456"})
    with pytest.raises(ProviderDirectoryProofStoreError, match="identity conflicts"):
        proof_store._framed_records([original, changed])
    for lineage in (
        ("", ENDPOINT_ID, ROOT_RUN_ID, SOURCE_IDS),
        (DATASET_ID, "", ROOT_RUN_ID, SOURCE_IDS),
        (DATASET_ID, ENDPOINT_ID, "", SOURCE_IDS),
        (DATASET_ID, ENDPOINT_ID, ROOT_RUN_ID, [""]),
    ):
        with pytest.raises(ProviderDirectoryProofStoreError, match="lineage"):
            proof_store._proof_shard_lineage(*lineage)
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
async def test_locked_parent_lineage_decodes_json_and_rejects_invalid_scope():
    connection = SimpleNamespace(
        first=AsyncMock(
            return_value={
                "endpoint_id": ENDPOINT_ID,
                "acquisition_root_run_id": ROOT_RUN_ID,
                "publication_metadata_json": json.dumps(
                    {"source_ids": SOURCE_IDS}
                ),
            }
        )
    )
    assert await proof_store._locked_dataset_proof_lineage(
        connection,
        "mrf",
        DATASET_ID,
    ) == (ENDPOINT_ID, ROOT_RUN_ID, SOURCE_IDS)
    for source_ids in (None, [], [""]):
        connection.first.return_value = {
            "endpoint_id": ENDPOINT_ID,
            "acquisition_root_run_id": ROOT_RUN_ID,
            "publication_metadata_json": {"source_ids": source_ids},
        }
        with pytest.raises(ProviderDirectoryProofStoreError, match="source scope"):
            await proof_store._locked_dataset_proof_lineage(
                connection,
                "mrf",
                DATASET_ID,
            )


@pytest.mark.asyncio
async def test_persisted_shard_replay_rejects_mutated_stored_fields():
    connection = _MemoryProofConnection()
    row = _dataset_resource("Practitioner", "p", {"npi": "123"})
    descriptor = await persist_dataset_proof_shard(
        connection,
        "mrf",
        [row],
        dataset_id=DATASET_ID,
    )
    connection.shards[(DATASET_ID, descriptor["shard_id"])][
        "endpoint_id"
    ] = "changed"
    with pytest.raises(ProviderDirectoryProofStoreError, match="replay changed"):
        await persist_dataset_proof_shard(
            connection,
            "mrf",
            [row],
            dataset_id=DATASET_ID,
        )
