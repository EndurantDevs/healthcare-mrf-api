# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Content parity and cancellation while proof work yields to other tasks."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import importlib
import json
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_proof_store as proof
from process.provider_directory_organization_hash import (
    canonical_organization_payload,
    composed_organization_semantic_sha256,
)
from process.provider_directory_resource_hash import (
    LEGACY_RESOURCE_HASH_CONTRACT,
    SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
    composed_practitioner_semantic_sha256,
    resource_payload_sha256_for_contract,
)
from tests.test_provider_directory_organization_hash_v4 import _organization_payload
from tests.test_provider_directory_proof_store import DATASET_ID, _dataset_resource, _MemoryProofConnection

fhir = importlib.import_module("process.provider_directory_fhir")


def _spools(directory, lines):
    records = proof._RecordSpool(directory / "records")
    npis = proof._RecordSpool(directory / "npis")
    records.directory.mkdir()
    npis.directory.mkdir()
    for line in lines:
        records.add(line)
    return records, npis


def _semantic_lines():
    payloads = [
        ("Organization", canonical_organization_payload(_organization_payload(name)))
        for name in ("Alpha Clinic", "Beta Clinic")
    ]
    payloads += [
        (
            "Practitioner",
            {
                "npi": "123",
                "names": [{"family": "Example", "given": [name]}],
                "family_name": "Example",
                "given_names": [name],
                "full_name": f"{name} Example",
            },
        )
        for name in ("Alpha", "Beta")
    ]
    lines = []
    for resource_type, payload in payloads:
        row = _dataset_resource(resource_type, "same", payload)
        row["payload_hash"] = resource_payload_sha256_for_contract(
            payload, SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT, resource_type=resource_type
        )
        lines.append(proof._stable_json(proof._proof_record(row, SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT)).encode())
    return lines + lines


async def _observe(ticks, stop):
    while not stop.is_set():
        ticks.append(1)
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_async_spool_completion_preserves_unions_and_metrics(monkeypatch, tmp_path):
    monkeypatch.setattr(proof, "_SPOOL_ROWS", 1)
    monkeypatch.setattr(proof, "_MERGE_FAN_IN", 2)
    monkeypatch.setattr(proof, "_PROOF_CHECKPOINT_ROWS", 1)
    sync_directory, async_directory = tmp_path / "sync", tmp_path / "async"
    sync_directory.mkdir()
    async_directory.mkdir()
    expected = proof._complete_spools(*_spools(sync_directory, _semantic_lines()))
    ticks, stop = [], asyncio.Event()
    observer = asyncio.create_task(_observe(ticks, stop))
    try:
        actual = await proof._drain_proof_steps_async(
            proof._complete_spool_steps(*_spools(async_directory, _semantic_lines()))
        )
    finally:
        stop.set()
        await observer
    assert actual == expected
    assert actual[1] == 2
    assert actual[3] == {"Organization": 1, "Practitioner": 1}
    assert actual[7]["collision_identities"] == 2
    assert len(ticks) > 3


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ("resource", "npi", "compaction", "complete"))
async def test_cancelled_proof_closes_streams_before_scratch_cleanup(monkeypatch, tmp_path, phase):
    monkeypatch.setattr(proof, "_SPOOL_ROWS", 1)
    monkeypatch.setattr(proof, "_MERGE_FAN_IN", 2)
    monkeypatch.setattr(proof, "_PROOF_CHECKPOINT_ROWS", 1)
    opened, original_open = [], Path.open

    def track_open(path, *args, **kwargs):
        stream = original_open(path, *args, **kwargs)
        if path.is_relative_to(tmp_path):
            opened.append(stream)
        return stream

    monkeypatch.setattr(Path, "open", track_open)
    with tempfile.TemporaryDirectory(dir=tmp_path) as scratch:
        record_spool, npi_spool = _spools(Path(scratch), _semantic_lines())
        for _ in range(8):
            npi_spool.add(b"123")
        if phase == "complete":
            steps = proof._complete_spool_steps(record_spool, npi_spool)
        elif phase == "compaction":
            steps = record_spool.bounded_path_steps()
        elif phase == "resource":
            steps = proof._merged_resource_proof_steps(record_spool, npi_spool, record_spool.bounded_paths())
        else:
            steps = proof._merged_npi_proof_steps(npi_spool, npi_spool.bounded_paths())
        task = asyncio.create_task(proof._drain_proof_steps_async(steps))
        await asyncio.sleep(0)
        assert any(not stream.closed for stream in opened)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert all(stream.closed for stream in opened)
        assert steps.gi_frame is None
    assert not Path(scratch).exists()


@pytest.mark.asyncio
async def test_shard_loading_advances_another_task(tmp_path):
    connection = _MemoryProofConnection()
    rows = [_dataset_resource("Practitioner", f"person-{index}", {"npi": "123"}) for index in range(4)]
    for row in rows:
        await proof.persist_dataset_proof_shard(connection, "mrf", [row], dataset_id=DATASET_ID)
    records, _npis = _spools(tmp_path, [])
    ticks, stop = [], asyncio.Event()
    observer = asyncio.create_task(_observe(ticks, stop))
    try:
        descriptors = await proof._load_shards(connection, "mrf", DATASET_ID, records)
    finally:
        stop.set()
        await observer
    assert len(descriptors) == len(rows)
    assert len(ticks) >= len(rows)


@pytest.mark.asyncio
@pytest.mark.parametrize("corrupt", (False, True))
async def test_direct_content_proof_yields_without_skipping_payload_verification(monkeypatch, corrupt):
    monkeypatch.setattr(fhir, "ENDPOINT_DATASET_HASH_BATCH_SIZE", 10_000)
    resource_rows = [_dataset_resource("Practitioner", f"person-{index:06d}", {"npi": "123"}) for index in range(2_001)]
    expected_hash = hashlib.sha256(
        b"\n".join(
            proof._stable_json(
                [resource_row["resource_type"], resource_row["resource_id"], resource_row["payload_hash"]]
            ).encode()
            for resource_row in resource_rows
        )
    ).hexdigest()
    if corrupt:
        resource_rows[-1]["payload_hash"] = "0" * 64
    before = copy.deepcopy(resource_rows)
    connection = SimpleNamespace(all=AsyncMock(return_value=resource_rows))
    ticks, stop = [], asyncio.Event()
    observer = asyncio.create_task(_observe(ticks, stop))
    try:
        if corrupt:
            with pytest.raises(RuntimeError, match="payload_hash"):
                await fhir._endpoint_dataset_content_proof(
                    connection,
                    DATASET_ID,
                    verify_payload_hashes=True,
                    resource_hash_contract=LEGACY_RESOURCE_HASH_CONTRACT,
                )
        else:
            completed_proof = await fhir._endpoint_dataset_content_proof(
                connection, DATASET_ID, verify_payload_hashes=True, resource_hash_contract=LEGACY_RESOURCE_HASH_CONTRACT
            )
            assert completed_proof.dataset_hash == expected_hash
            assert completed_proof.resource_count == len(resource_rows)
    finally:
        stop.set()
        await observer
    assert ticks
    assert resource_rows == before
    assert connection.all.await_count == 1


@pytest.mark.parametrize("compose", (composed_practitioner_semantic_sha256, composed_organization_semantic_sha256))
@pytest.mark.parametrize("invalid", ("A" * 64, "g" * 64, "a" * 63 + "\n", "ａ" * 64, "a" * 65))
@pytest.mark.parametrize("position", ("base", "name"))
def test_composed_hash_requires_exact_lowercase_hex(compose, invalid, position):
    base, names = (invalid, ["b" * 64]) if position == "base" else ("a" * 64, [invalid])
    with pytest.raises(ValueError, match="hash_invalid"):
        compose(base, names)


@pytest.mark.parametrize("compose", (composed_practitioner_semantic_sha256, composed_organization_semantic_sha256))
def test_composed_hash_retains_exact_canonical_digest(compose):
    canonical_by_name = {"base_hash": "a" * 64, "name_hashes": ["b" * 64, "c" * 64]}
    expected = hashlib.sha256(json.dumps(canonical_by_name, sort_keys=True).encode()).hexdigest()
    assert compose("a" * 64, ["c" * 64, "b" * 64, "c" * 64]) == expected
