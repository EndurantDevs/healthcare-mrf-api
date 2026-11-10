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
from unittest.mock import AsyncMock, Mock

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
from tests.provider_directory_proof_memory_support import proof_memory_authority as proof_memory_authority
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


def _content_pages(resource_rows):
    def page(_query, **parameters):
        after = parameters.get("after_resource_type"), parameters.get("after_resource_id")
        return [row for row in resource_rows if after[0] is None or (row["resource_type"], row["resource_id"]) > after][
            : parameters["batch_size"]
        ]

    return SimpleNamespace(all=AsyncMock(side_effect=page))


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
async def test_merge_allows_io_progress_before_finishing_records(monkeypatch, tmp_path):
    """A sequence of callback-backed I/O waits must finish before the resource merge."""
    lines = [
        proof._stable_json(
            proof._proof_record(
                _dataset_resource("Practitioner", f"person-{index:06d}", {"npi": "123"}),
                LEGACY_RESOURCE_HASH_CONTRACT,
            )
        ).encode()
        for index in range(4096)
    ]
    sync_directory, async_directory = tmp_path / "sync", tmp_path / "async"
    sync_directory.mkdir()
    async_directory.mkdir()
    expected = proof._complete_spools(*_spools(sync_directory, lines))
    consumed, original_decode = [], proof._decoded_record

    def decode(line):
        consumed.append(1)
        return original_decode(line)

    async def io_work():
        for _ in range(32):
            pending = asyncio.get_running_loop().create_future()
            asyncio.get_running_loop().call_soon(pending.set_result, None)
            await pending
        return len(consumed)

    monkeypatch.setattr(proof, "_decoded_record", decode)
    observer = asyncio.create_task(io_work())
    try:
        actual = await proof._drain_proof_steps_async(proof._complete_spool_steps(*_spools(async_directory, lines)))
    finally:
        progressed_at = await observer
    assert actual == expected
    assert 0 < progressed_at < len(lines)


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
    resource_rows = [_dataset_resource("Practitioner", f"person-{index:06d}", {"npi": "123"}) for index in range(513)]
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
    verify_payload_hash = Mock(wraps=fhir._assert_endpoint_dataset_resource_payload_hash)
    monkeypatch.setattr(fhir, "_assert_endpoint_dataset_resource_payload_hash", verify_payload_hash)
    task = asyncio.create_task(
        fhir._endpoint_dataset_content_proof(
            connection, DATASET_ID, verify_payload_hashes=True, resource_hash_contract=LEGACY_RESOURCE_HASH_CONTRACT
        )
    )
    await asyncio.sleep(0)
    assert verify_payload_hash.call_count == 256
    if corrupt:
        with pytest.raises(RuntimeError, match="payload_hash"):
            await task
    else:
        completed_proof = await task
        assert completed_proof.dataset_hash == expected_hash
        assert completed_proof.resource_count == len(resource_rows)
    assert verify_payload_hash.call_count == len(resource_rows)
    assert resource_rows == before
    assert connection.all.await_count == 1


@pytest.mark.asyncio
async def test_direct_content_proof_allows_pending_io_before_finishing(monkeypatch):
    resource_rows = [_dataset_resource("Practitioner", f"person-{index:06d}", {"npi": "123"}) for index in range(4096)]
    connection = _content_pages(resource_rows)
    verify_payload_hash = Mock(wraps=fhir._assert_endpoint_dataset_resource_payload_hash)
    monkeypatch.setattr(fhir, "_assert_endpoint_dataset_resource_payload_hash", verify_payload_hash)

    async def io_work():
        for _ in range(32):
            pending = asyncio.get_running_loop().create_future()
            asyncio.get_running_loop().call_soon(pending.set_result, None)
            await pending
        return verify_payload_hash.call_count

    observer = asyncio.create_task(io_work())
    try:
        completed = await fhir._endpoint_dataset_content_proof(
            connection, DATASET_ID, verify_payload_hashes=True, resource_hash_contract=LEGACY_RESOURCE_HASH_CONTRACT
        )
    finally:
        progressed_at = await observer
    assert completed.resource_count == verify_payload_hash.call_count == len(resource_rows)
    assert 0 < progressed_at < len(resource_rows)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "verify_payloads,configured,expected", ((True, 10_000, 1000), (True, 127, 127), (False, 10_000, 10_000))
)
async def test_content_proof_bounds_payload_pages_without_changing_hashes(
    monkeypatch, verify_payloads, configured, expected
):
    resource_rows = [
        _dataset_resource(resource_type, f"item-{index:06d}", {"npi": "123"})
        for resource_type in ("Location", "Practitioner")
        for index in range(1000)
    ]
    connection = _content_pages(resource_rows)
    monkeypatch.setattr(fhir, "ENDPOINT_DATASET_HASH_BATCH_SIZE", configured)
    checked = Mock(wraps=fhir._assert_endpoint_dataset_resource_payload_hash)
    monkeypatch.setattr(fhir, "_assert_endpoint_dataset_resource_payload_hash", checked)
    completed = await fhir._endpoint_dataset_content_proof(
        connection,
        DATASET_ID,
        verify_payload_hashes=verify_payloads,
        resource_hash_contract=LEGACY_RESOURCE_HASH_CONTRACT,
    )
    expected_hash = hashlib.sha256(
        b"\n".join(
            proof._stable_json((row["resource_type"], row["resource_id"], row["payload_hash"])).encode()
            for row in resource_rows
        )
    ).hexdigest()
    assert completed.dataset_hash == expected_hash and completed.resource_count == len(resource_rows)
    assert completed.resource_counts == {"Location": 1000, "Practitioner": 1000}
    assert checked.call_count == (len(resource_rows) if verify_payloads else 0)
    assert connection.all.await_count == len(resource_rows) // expected + 1
    assert all(call.kwargs["batch_size"] == expected for call in connection.all.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ("corrupt", "cancel"))
async def test_payload_proof_refuses_corruption_and_cancels_across_pages(monkeypatch, failure):
    resource_rows = [_dataset_resource("Practitioner", f"person-{index:06d}", {"npi": "123"}) for index in range(1001)]
    connection = _content_pages(resource_rows)
    checked = Mock(wraps=fhir._assert_endpoint_dataset_resource_payload_hash)
    monkeypatch.setattr(fhir, "_assert_endpoint_dataset_resource_payload_hash", checked)
    entered_second_page = asyncio.Event()
    page = connection.all.side_effect

    async def interrupted_page(query, **parameters):
        if parameters.get("after_resource_id") is not None:
            entered_second_page.set()
            await asyncio.Event().wait()
        return page(query, **parameters)

    if failure == "corrupt":
        resource_rows[-1]["payload_hash"] = "0" * 64
    else:
        connection.all.side_effect = interrupted_page
    task = asyncio.create_task(
        fhir._endpoint_dataset_content_proof(
            connection, DATASET_ID, verify_payload_hashes=True, resource_hash_contract=LEGACY_RESOURCE_HASH_CONTRACT
        )
    )
    if failure == "corrupt":
        with pytest.raises(RuntimeError, match="payload_hash_mismatch"):
            await task
        assert checked.call_count == len(resource_rows)
    else:
        await asyncio.wait_for(entered_second_page.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert checked.call_count == 1000
    assert connection.all.await_count == 2
    assert connection.all.await_args_list[1].kwargs["after_resource_id"] == resource_rows[999]["resource_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", (False, True))
async def test_acquired_resource_hashes_yield_without_changing_content(monkeypatch, cancel):
    monkeypatch.setattr(fhir, "ENDPOINT_DATASET_HASH_BATCH_SIZE", 10_000)
    resource_rows = [
        {"resource_type": "Practitioner", "resource_id": f"person-{index:06d}", "acquired_resource_sha256": "a" * 64}
        for index in range(513)
    ]
    before = copy.deepcopy(resource_rows)
    connection = SimpleNamespace(all=AsyncMock(return_value=resource_rows))
    candidate = SimpleNamespace(dataset_id=DATASET_ID, selected_resources=("Practitioner",))
    update_hash = Mock(wraps=fhir._update_subset_acquired_hash)
    monkeypatch.setattr(fhir, "_update_subset_acquired_hash", update_hash)
    task = asyncio.create_task(fhir._subset_acquired_resource_hashes(connection, candidate, {"Practitioner": 513}))
    await asyncio.sleep(0)
    assert update_hash.call_count == 256
    if cancel:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert update_hash.call_count == 256
    else:
        await asyncio.sleep(0)
        assert update_hash.call_count == 512
        assert await task == {
            "Practitioner": fhir.subset_canonical_sha256(
                [
                    {"resource_id": resource_row["resource_id"], "sha256": resource_row["acquired_resource_sha256"]}
                    for resource_row in resource_rows
                ]
            )
        }
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
