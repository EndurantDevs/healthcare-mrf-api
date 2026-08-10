# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime
import hashlib
import json
import struct
from collections import defaultdict
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from tests.live_progress_atomic_redis import AtomicLiveProgressRedis

from process import live_progress
from process.ptg_parts import ptg2_shared_snapshot_publish as shared_snapshot_publish
from process.ptg_parts import ptg2_shared_publish as shared_publish
from process.ptg_parts import live_progress as ptg_live_progress
from process.ptg_parts import ptg2_v4_graph_compiler as graph_compiler
from process.ptg_parts.ptg2_shared_blocks import SharedMappingDigestSummary
from process.ptg_parts.ptg2_shared_price import PreparedSharedPriceKeyMap
from process.ptg_parts.ptg2_shared_snapshot_publish import (
    _run_independent_publication_lanes,
    _validate_authoritative_mapping_summary,
)

from tests.ptg2_shared_snapshot_dictionary_test_support import (
    _DenseDictionaryRangeDriver,
    _SlowProgressAcquire,
    _SlowProgressDriver,
    _assert_dense_dictionary_range_statements,
    _assert_shared_publish_cadence,
    _dense_dictionary_progress_stage,
    _install_shared_publish_progress_capture,
    _publish_progress_to_live,
    _row_result,
)

def test_v4_dictionary_heartbeat_skips_unreportable_work():
    """Heartbeat is inert without a callback or before its next cadence."""

    progress_without_callback = shared_snapshot_publish._MeasuredPublicationProgress(
        "dictionary",
        None,
    )
    progress_without_callback.heartbeat()

    elapsed_seconds = [0.0]
    snapshots = []
    progress = shared_snapshot_publish._MeasuredPublicationProgress(
        "dictionary",
        lambda _stage, counters: snapshots.append(dict(counters)),
        interval_seconds=4.0,
        clock=lambda: elapsed_seconds[0],
    )
    progress.add("published_dictionary_rows", 1)
    progress.flush()
    elapsed_seconds[0] = 3.9
    progress.heartbeat()

    assert snapshots == [{"published_dictionary_rows": 1}]


@pytest.mark.asyncio
async def test_v4_dictionary_statement_rejects_invalid_heartbeat():
    """A nonpositive heartbeat cannot create an unmonitored SQL task."""

    statement_result = asyncio.get_running_loop().create_future()
    statement_result.set_result(None)

    with pytest.raises(
        ValueError,
        match="heartbeat must be positive",
    ):
        await shared_snapshot_publish._await_v4_dictionary_statement(
            statement_result,
            heartbeat_callback=None,
            heartbeat_seconds=0,
        )


@pytest.mark.asyncio
async def test_v4_dictionary_statement_preserves_immediate_failure():
    """A completed statement failure is returned without redundant cancellation."""

    async def fail_statement():
        raise RuntimeError("statement failed")

    with pytest.raises(
        RuntimeError,
        match="statement failed",
    ):
        await shared_snapshot_publish._await_v4_dictionary_statement(
            fail_statement(),
            heartbeat_callback=None,
        )


@pytest.mark.asyncio
async def test_v4_dictionary_statement_heartbeat_preserves_cancellation():
    """Heartbeat polling re-raises cancellation and settles the SQL task."""

    started = asyncio.Event()
    canceled = asyncio.Event()
    heartbeat_counts = [0]

    async def pending_statement():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            canceled.set()

    def heartbeat():
        heartbeat_counts[0] += 1

    task = asyncio.create_task(
        shared_snapshot_publish._await_v4_dictionary_statement(
            pending_statement(),
            heartbeat_callback=heartbeat,
            heartbeat_seconds=0.01,
        )
    )
    await started.wait()
    while heartbeat_counts[0] == 0:
        await asyncio.sleep(0.005)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert canceled.is_set()
    assert heartbeat_counts[0] >= 1


def test_v4_dictionary_publication_contract_is_manifest_safe():
    """The sealed runtime contract freezes every adaptive batch threshold."""

    contract = shared_snapshot_publish._V4_DICTIONARY_BATCH_CONTRACT.as_dict()

    assert contract == {
        "contract": "ptg2_v4_dictionary_publication_adaptive_v1",
        "default_range_rows": 100_000,
        "fallback_range_rows": 10_000,
        "max_estimated_row_work_bytes": 16 * 1024 * 1024,
        "fixed_work_overhead_bytes": 64 * 1024,
        "estimated_row_bytes": 160,
        "slow_statement_millis": 4_000,
        "recovery_statement_millis": 2_000,
        "heartbeat_millis": 4_000,
    }


@pytest.mark.asyncio
async def test_v4_dictionary_publication_rejects_extra_target_keys():
    """An authenticated stage cannot leave keys outside its dense span."""

    session = SimpleNamespace(scalar=AsyncMock(return_value=True))

    with pytest.raises(RuntimeError, match="persisted dictionary rows changed"):
        await shared_snapshot_publish._reject_v4_dictionary_extra_keys(
            session,
            schema='"mrf"',
            target_table='"ptg2_v4_provider_group"',
            key_name='"provider_group_key"',
            snapshot_key=17,
            expected_count=3,
        )


@pytest.mark.asyncio
async def test_v4_sparse_publication_rejects_incomplete_target(monkeypatch):
    """A sparse publication must authenticate the complete staged count."""

    monkeypatch.setattr(
        shared_snapshot_publish,
        "_v4_sparse_batch_boundary",
        AsyncMock(return_value=(0, None, 0.0)),
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "_count_v4_target_keys",
        AsyncMock(return_value=0),
    )
    session = SimpleNamespace()
    stage = _dense_dictionary_progress_stage()
    stage = shared_snapshot_publish._V4DenseDictionaryStage(
        **{**stage.__dict__, "expected_count": 1}
    )

    with pytest.raises(RuntimeError, match="persisted dictionary rows changed"):
        await shared_snapshot_publish._publish_v4_sparse_ranges(
            session,
            schema='"mrf"',
            snapshot_key=17,
            stage=stage,
            progress_callback=None,
        )


@pytest.mark.asyncio
async def test_sealed_layout_requires_a_serving_index(monkeypatch):
    """The post-seal readback fails closed when the index is absent."""

    session = SimpleNamespace(
        execute=AsyncMock(
            return_value=SimpleNamespace(scalar=lambda: None),
        )
    )

    @asynccontextmanager
    async def transaction():
        yield session

    monkeypatch.setattr(shared_snapshot_publish.db, "transaction", transaction)

    with pytest.raises(RuntimeError, match="missing its serving index"):
        await shared_snapshot_publish._sealed_shared_serving_index(
            schema_name="mrf",
            snapshot_key=17,
            expected_generation="shared_blocks_v4",
        )


def test_v4_reference_manifest_streams_exact_sorted_coordinates(tmp_path):
    path = tmp_path / "references.jsonl"
    reference_records = [
        {
            "object_kind": "v4_group_npis_exact_members_v1",
            "block_key": block_key,
            "fragment_no": 0,
            "entry_count": 2,
            "raw_byte_count": 8,
            "stored_byte_count": 8,
            "codec": "none",
            "hash": (bytes([block_key + 1]) * 32).hex(),
        }
        for block_key in (0, 4)
    ]
    path.write_text(
        "".join(
            json.dumps(reference_record) + "\n"
            for reference_record in reference_records
        )
    )
    reference_bytes = path.read_bytes()

    references = tuple(
        shared_snapshot_publish._iter_v4_block_references(
            path,
            expected_byte_count=len(reference_bytes),
            expected_sha256=hashlib.sha256(reference_bytes).hexdigest(),
            expected_row_count=2,
        )
    )

    assert tuple(reference.block_key for reference in references) == (0, 4)
    assert tuple(reference.entry_count for reference in references) == (2, 2)
    assert references[0].block_hash == b"\x01" * 32


def test_v4_reference_manifest_authentication_rejects_drift(tmp_path):
    """Require the complete byte, digest, and row contract while streaming."""

    path = tmp_path / "references.jsonl"
    reference_map = {
        "object_kind": "v4_group_npis_exact_members_v1",
        "block_key": 0,
        "fragment_no": 0,
        "entry_count": 2,
        "raw_byte_count": 8,
        "stored_byte_count": 8,
        "codec": "none",
        "hash": (b"a" * 32).hex(),
    }
    path.write_text(json.dumps(reference_map) + "\n")
    reference_bytes = path.read_bytes()
    expected_sha256 = hashlib.sha256(reference_bytes).hexdigest()
    path.write_bytes(reference_bytes.replace(b"61" * 32, b"62" * 32))

    with pytest.raises(RuntimeError, match="authentication changed"):
        tuple(
            shared_snapshot_publish._iter_v4_block_references(
                path,
                expected_byte_count=len(reference_bytes),
                expected_sha256=expected_sha256,
                expected_row_count=1,
            )
        )
    with pytest.raises(ValueError, match="authentication is incomplete"):
        tuple(
            shared_snapshot_publish._iter_v4_block_references(
                path,
                expected_byte_count=len(reference_bytes),
            )
        )


def test_v4_reference_manifest_rejects_noncanonical_order(tmp_path):
    path = tmp_path / "references.jsonl"
    rows = [
        {
            "object_kind": "v4_set_patterns_members_v1",
            "block_key": block_key,
            "fragment_no": 0,
            "entry_count": 1,
            "raw_byte_count": 4,
            "stored_byte_count": 4,
            "codec": "none",
            "hash": (bytes([block_key + 1]) * 32).hex(),
        }
        for block_key in (1, 0)
    ]
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))

    with pytest.raises(RuntimeError, match="ordering"):
        tuple(shared_snapshot_publish._iter_v4_block_references(path))


@pytest.mark.asyncio
async def test_failed_v4_graph_hashes_are_queued_in_bounded_batches(
    monkeypatch,
    tmp_path,
):
    """Retain every orphan candidate without one unbounded SQL parameter."""

    session = SimpleNamespace(execute=AsyncMock())

    @asynccontextmanager
    async def transaction():
        yield session

    def references(_path):
        for value in range(8_193):
            yield SimpleNamespace(block_hash=value.to_bytes(32, "big"))

    monkeypatch.setattr(
        shared_snapshot_publish,
        "_iter_v4_block_references",
        references,
    )
    monkeypatch.setattr(
        shared_snapshot_publish.db,
        "transaction",
        transaction,
    )

    await shared_snapshot_publish._queue_failed_v4_graph_blocks(
        schema_name="mrf",
        reference_manifest_path=tmp_path / "unused.jsonl",
    )

    assert session.execute.await_count == 2
    first_hashes = session.execute.await_args_list[0].args[1]["block_hashes"]
    second_hashes = session.execute.await_args_list[1].args[1]["block_hashes"]
    assert len(first_hashes) == 8_192
    assert first_hashes[0] == bytes(32)
    assert first_hashes[-1] == (8_191).to_bytes(32, "big")
    assert second_hashes == [(8_192).to_bytes(32, "big")]
