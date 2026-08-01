# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import copy
import hashlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
import zlib

import pytest

from process import provider_directory_proof_store as proof_store
from process.provider_directory_proof_store import (
    ProviderDirectoryProofStoreError,
    validate_stored_dataset_proof_metadata,
)
from tests.test_provider_directory_proof_store import (
    DATASET_ID,
    ENDPOINT_ID,
    ROOT_RUN_ID,
    SELECTED_RESOURCES,
    SOURCE_IDS,
    _MemoryProofConnection,
    _dataset_resource,
    _persist_rows_by_resource,
    _sample_dataset_resources,
    _stable_json,
    _stored_proof,
)

def test_record_spool_flushes_merges_and_rejects_unframed_runs(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(proof_store, "_SPOOL_ROWS", 1)
    monkeypatch.setattr(proof_store, "_MERGE_FAN_IN", 2)
    spool = proof_store._RecordSpool(tmp_path)
    spool.flush()
    for line in (b"c", b"a", b"b"):
        spool.add(line)
    paths = spool.bounded_paths()
    assert len(paths) == 2
    assert list(
        __import__("heapq").merge(*(spool.lines(path) for path in paths))
    ) == [b"a", b"b", b"c"]

    bad_path = tmp_path / "bad"
    bad_path.write_bytes(b"unframed")
    with pytest.raises(ProviderDirectoryProofStoreError, match="framing"):
        tuple(spool.lines(bad_path))


@pytest.mark.parametrize(
    "line",
    [
        b"\xff",
        b"not-json",
        b"{}",
        b"[]",
        b'["Practitioner","p","bad","",0,0,0]',
        b'["Practitioner","p","'
        + hashlib.sha256(b"payload").hexdigest().encode()
        + b'","",true,0,0]',
    ],
)
def test_decoded_record_rejects_invalid_json_or_shape(line):
    with pytest.raises(ProviderDirectoryProofStoreError):
        proof_store._decoded_record(line)


def test_decoded_json_field_rejects_malformed_string():
    assert proof_store._decoded_json_field({"value": "[]"}, "value") == []
    assert proof_store._decoded_json_field({"value": []}, "value") == []
    with pytest.raises(ProviderDirectoryProofStoreError, match="descriptor"):
        proof_store._decoded_json_field({"value": "{"}, "value")


def _valid_shard_payload():
    descriptor, compressed = proof_store.build_dataset_proof_shard(
        [_dataset_resource("Practitioner", "p", {"npi": "123"})],
        dataset_id=DATASET_ID,
        endpoint_id=ENDPOINT_ID,
        acquisition_root_run_id=ROOT_RUN_ID,
        source_ids=SOURCE_IDS,
    )
    shard_row_by_field = {
        **proof_store._expected_persisted_shard_fields(descriptor, compressed),
        "source_ids_json": descriptor["source_ids"],
        "resource_counts_json": descriptor["resource_counts"],
        "first_identity_json": descriptor["first_identity"],
        "last_identity_json": descriptor["last_identity"],
    }
    return descriptor, shard_row_by_field, compressed


def test_shard_payload_rejects_compression_input_count_order_and_descriptor_drift():
    descriptor, shard_row_by_field, compressed = _valid_shard_payload()
    invalid_compressed = b"not-zlib"
    invalid_row_by_field = {
        **shard_row_by_field,
        "artifact_sha256": hashlib.sha256(invalid_compressed).hexdigest(),
        "artifact_byte_count": len(invalid_compressed),
    }
    with pytest.raises(ProviderDirectoryProofStoreError, match="artifact is invalid"):
        proof_store._validated_shard_payload(
            invalid_row_by_field,
            invalid_compressed,
        )

    unframed = zlib.compress(b"record")
    unframed_row_by_field = {
        **shard_row_by_field,
        "artifact_sha256": hashlib.sha256(unframed).hexdigest(),
        "artifact_byte_count": len(unframed),
        "input_sha256": hashlib.sha256(b"record").hexdigest(),
    }
    with pytest.raises(ProviderDirectoryProofStoreError, match="input changed"):
        proof_store._validated_shard_payload(unframed_row_by_field, unframed)

    count_row_by_field = {**shard_row_by_field, "resource_count": 2}
    with pytest.raises(ProviderDirectoryProofStoreError, match="row count changed"):
        proof_store._validated_shard_payload(count_row_by_field, compressed)

    decoded_line = zlib.decompress(compressed)[:-1]
    duplicate_payload = decoded_line + b"\n" + decoded_line + b"\n"
    duplicate_compressed = zlib.compress(duplicate_payload)
    duplicate_row_by_field = {
        **shard_row_by_field,
        "resource_count": 2,
        "artifact_sha256": hashlib.sha256(duplicate_compressed).hexdigest(),
        "artifact_byte_count": len(duplicate_compressed),
        "input_sha256": hashlib.sha256(duplicate_payload).hexdigest(),
    }
    with pytest.raises(ProviderDirectoryProofStoreError, match="order changed"):
        proof_store._validated_shard_lines(
            duplicate_row_by_field,
            duplicate_compressed,
        )

    descriptor_row_by_field = {
        **shard_row_by_field,
        "resource_counts_json": {"Practitioner": 2},
    }
    with pytest.raises(ProviderDirectoryProofStoreError, match="descriptor changed"):
        proof_store._validated_shard_lines(
            descriptor_row_by_field,
            compressed,
        )
    assert descriptor["resource_count"] == 1


def _resign_metadata(proof):
    proof.pop("proof_sha256", None)
    proof["proof_sha256"] = proof_store._json_hash(proof)


async def _valid_metadata():
    connection = _MemoryProofConnection()
    await _persist_rows_by_resource(connection, _sample_dataset_resources())
    return (await _stored_proof(connection)).metadata


@pytest.mark.parametrize(
    "arguments",
    [
        {"dataset_id": "", "endpoint_id": ENDPOINT_ID},
        {"dataset_id": DATASET_ID, "endpoint_id": ""},
        {
            "dataset_id": DATASET_ID,
            "endpoint_id": ENDPOINT_ID,
            "acquisition_root_run_id": "",
        },
        {"dataset_id": DATASET_ID, "endpoint_id": ENDPOINT_ID, "source_ids": []},
        {
            "dataset_id": DATASET_ID,
            "endpoint_id": ENDPOINT_ID,
            "selected_resources": [],
        },
    ],
)
def test_finalization_lineage_rejects_empty_dimensions(arguments):
    complete_lineage_by_field = {
        "dataset_id": DATASET_ID,
        "endpoint_id": ENDPOINT_ID,
        "acquisition_root_run_id": ROOT_RUN_ID,
        "source_ids": SOURCE_IDS,
        "selected_resources": SELECTED_RESOURCES,
    }
    complete_lineage_by_field.update(arguments)
    with pytest.raises(ProviderDirectoryProofStoreError, match="lineage"):
        proof_store._validated_proof_lineage(**complete_lineage_by_field)


def test_validation_primitives_reject_invalid_values():
    with pytest.raises(ProviderDirectoryProofStoreError, match="hash"):
        proof_store._validated_hash("bad", "hash")
    for value in (True, "1", -1):
        with pytest.raises(ProviderDirectoryProofStoreError, match="count"):
            proof_store._validated_count(value, "count")
    with pytest.raises(ProviderDirectoryProofStoreError, match="count"):
        proof_store._validated_count(0, "count", positive=True)
    for value in (None, [], ["b", "a"], ["a", "a"], [""]):
        with pytest.raises(ProviderDirectoryProofStoreError, match="scope"):
            proof_store._validated_string_list(value, "scope")


@pytest.mark.asyncio
async def test_metadata_validation_rejects_each_public_contract_mutation():
    valid = await _valid_metadata()
    mutations = [
        ("raw_type", None),
        ("contract_id", "wrong"),
        ("complete", False),
        ("dataset_id", "wrong"),
        ("endpoint_id", "wrong"),
        ("acquisition_root_run_id", "wrong"),
        ("source_ids", ["wrong"]),
        ("selected_resources", ["wrong"]),
        ("dataset_hash", "bad"),
        ("npi_set_sha256", "bad"),
        ("resource_count", -1),
        ("resource_counts", []),
        ("resource_hashes", []),
        ("resource_keys", {}),
        ("resource_total", 999),
        ("source_metrics", {}),
        ("source_metric_value", -1),
        ("shards", {}),
        ("shard_count", 999),
        ("shard_set_sha256", "0" * 64),
        ("proof_sha256", "0" * 64),
    ]
    for mutation, mutation_value in mutations:
        candidate = _metadata_contract_mutation(
            valid,
            mutation,
            mutation_value,
        )
        with pytest.raises(ProviderDirectoryProofStoreError):
            validate_stored_dataset_proof_metadata(
                candidate,
                dataset_id=DATASET_ID,
                endpoint_id=ENDPOINT_ID,
                acquisition_root_run_id=ROOT_RUN_ID,
                source_ids=SOURCE_IDS,
                selected_resources=SELECTED_RESOURCES,
            )


def _metadata_contract_mutation(valid, mutation, mutation_value):
    if mutation == "raw_type":
        return mutation_value
    candidate = copy.deepcopy(valid)
    if mutation == "resource_keys":
        candidate["resource_hashes"] = mutation_value
    elif mutation == "resource_total":
        candidate["resource_count"] = mutation_value
    elif mutation == "source_metric_value":
        candidate["source_metrics"]["distinct_npis"] = mutation_value
    else:
        candidate[mutation] = mutation_value
    if mutation != "proof_sha256":
        _resign_metadata(candidate)
    return candidate


@pytest.mark.asyncio
async def test_shard_descriptor_validation_rejects_scope_identity_and_lineage():
    valid = await _valid_metadata()
    descriptor = valid["shards"][0]
    lineage = proof_store._validated_proof_lineage(
        dataset_id=DATASET_ID,
        endpoint_id=ENDPOINT_ID,
        acquisition_root_run_id=ROOT_RUN_ID,
        source_ids=SOURCE_IDS,
        selected_resources=SELECTED_RESOURCES,
    )
    with pytest.raises(ProviderDirectoryProofStoreError, match="descriptor"):
        proof_store._validated_shard_descriptor(
            None,
            dataset_id=DATASET_ID,
            endpoint_id=ENDPOINT_ID,
            acquisition_root_run_id=ROOT_RUN_ID,
            source_ids=lineage.source_ids,
        )
    for mutation, mutation_value in (
        ("dataset_id", "wrong"),
        ("resource_counts", {}),
        ("first_identity", []),
        ("last_identity", []),
    ):
        candidate = copy.deepcopy(descriptor)
        candidate[mutation] = mutation_value
        with pytest.raises(ProviderDirectoryProofStoreError):
            proof_store._validated_shard_descriptor(
                candidate,
                dataset_id=DATASET_ID,
                endpoint_id=ENDPOINT_ID,
                acquisition_root_run_id=ROOT_RUN_ID,
                source_ids=lineage.source_ids,
            )
    reversed_range = copy.deepcopy(descriptor)
    reversed_range["first_identity"] = [
        "Z",
        "z",
        hashlib.sha256(b"z").hexdigest(),
    ]
    with pytest.raises(ProviderDirectoryProofStoreError, match="identity range"):
        proof_store._validated_shard_descriptor(
            reversed_range,
            dataset_id=DATASET_ID,
            endpoint_id=ENDPOINT_ID,
            acquisition_root_run_id=ROOT_RUN_ID,
            source_ids=lineage.source_ids,
        )


@pytest.mark.asyncio
async def test_verified_public_shards_rejects_resource_scope_and_lineage():
    valid = await _valid_metadata()
    lineage = proof_store._validated_proof_lineage(
        dataset_id=DATASET_ID,
        endpoint_id=ENDPOINT_ID,
        acquisition_root_run_id=ROOT_RUN_ID,
        source_ids=SOURCE_IDS,
        selected_resources=SELECTED_RESOURCES,
    )
    missing_scope = proof_store._MergedDatasetProof(
        dataset_hash=valid["dataset_hash"],
        resource_count=valid["resource_count"],
        resource_hash_by_type={},
        resource_count_by_type={},
        source_metrics_by_name=valid["source_metrics"],
        npi_set_sha256=valid["npi_set_sha256"],
        shard_descriptors=[],
    )
    with pytest.raises(ProviderDirectoryProofStoreError, match="resource scope"):
        proof_store._verified_public_shards(lineage, missing_scope)
    invalid_descriptor_by_field = {
        **valid["shards"][0],
        "source_ids_json": ["wrong"],
    }
    bad_lineage = proof_store._MergedDatasetProof(
        dataset_hash=valid["dataset_hash"],
        resource_count=valid["resource_count"],
        resource_hash_by_type=valid["resource_hashes"],
        resource_count_by_type=valid["resource_counts"],
        source_metrics_by_name=valid["source_metrics"],
        npi_set_sha256=valid["npi_set_sha256"],
        shard_descriptors=[invalid_descriptor_by_field],
    )
    with pytest.raises(ProviderDirectoryProofStoreError, match="lineage changed"):
        proof_store._verified_public_shards(lineage, bad_lineage)


@pytest.mark.asyncio
async def test_missing_durable_shards_fail_closed():
    lineage = proof_store._validated_proof_lineage(
        dataset_id=DATASET_ID,
        endpoint_id=ENDPOINT_ID,
        acquisition_root_run_id=ROOT_RUN_ID,
        source_ids=SOURCE_IDS,
        selected_resources=SELECTED_RESOURCES,
    )
    with pytest.raises(ProviderDirectoryProofStoreError, match="are missing"):
        await proof_store._merged_stored_dataset_proof(
            _MemoryProofConnection(),
            "mrf",
            lineage,
        )


def test_merged_proof_handles_exact_duplicates_and_rejects_order_drift():
    def proof_line(resource_type, resource_id):
        return (
            _stable_json(
                [
                    resource_type,
                    resource_id,
                    hashlib.sha256(resource_id.encode()).hexdigest(),
                    "",
                    0,
                    0,
                    0,
                ]
            ).encode()
            + b"\n"
        )

    class Spool:
        def __init__(self, lines):
            self._lines = lines
            self.added = []

        def bounded_paths(self):
            return [Path("spool")]

        def lines(self, _path):
            return iter(self._lines)

        def add(self, line):
            self.added.append(line)

    duplicate = proof_line("Organization", "organization-a")
    merged = proof_store._merged_resource_proof(
        Spool([duplicate, duplicate]),
        Spool([]),
    )
    assert merged[1] == 1

    with pytest.raises(ProviderDirectoryProofStoreError, match="merge order"):
        proof_store._merged_resource_proof(
            Spool(
                [
                    proof_line("Organization", "organization-z"),
                    proof_line("Organization", "organization-a"),
                ]
            ),
            Spool([]),
        )

    with pytest.raises(ProviderDirectoryProofStoreError, match="type"):
        proof_store._validated_resource_maps(
            {
                "resource_counts": {"": 0},
                "resource_hashes": {"": "a" * 64},
            },
            [],
        )


@pytest.mark.asyncio
async def test_shard_loader_continues_after_full_page(monkeypatch):
    rows = [
        {
            "shard_id": f"{index:03d}",
            "payload_bytes": b"x",
        }
        for index in range(128)
    ]
    connection = SimpleNamespace(
        all=AsyncMock(side_effect=[rows, []]),
    )
    monkeypatch.setattr(
        proof_store,
        "_validated_shard_lines",
        lambda _row, _compressed: ([], {}),
    )
    descriptors = await proof_store._load_shards(
        connection,
        "mrf",
        DATASET_ID,
        SimpleNamespace(add=Mock()),
    )
    assert len(descriptors) == 128
    assert connection.all.await_count == 2
