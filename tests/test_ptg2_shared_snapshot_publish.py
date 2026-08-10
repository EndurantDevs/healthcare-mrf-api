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
from tests.ptg2_shared_snapshot_atomic_test_support import _tax_stage_contract
from tests.test_ptg2_shared_snapshot_graph_contracts import (
    _patch_v4_graph_publication,
    _v4_graph_publication_fixture,
)

def test_snapshot_publish_rejects_missing_summary_mapping():
    with pytest.raises(RuntimeError, match="missing blocks"):
        shared_snapshot_publish._mapping(None, "blocks")


def _length_prefixed_sha256(domain, fields):
    digest = hashlib.sha256()
    digest.update(domain)
    for field in fields:
        digest.update(struct.pack(">I", len(field)))
        digest.update(field)
    return digest.hexdigest()


def _tax_identity_compilation_summary():
    policy_id = "ptg-tin-hmac-sha256-v1:release-1"
    normalization = "ein_ascii_digits_or_2_7_hyphen_v1"
    hmac_contract = "hmac_sha256_ptg_tin_v1"
    prefix_contract = "tin_id_128=first_16_bytes(tin_hmac_sha256)"
    authority_contract = "tin_hmac_sha256_full_32_bytes_authoritative"
    source_ordinals = [{"shard_id": "shard-a", "ordinal": 0}]
    source_digest = hashlib.sha256()
    source_digest.update(b"PTG2V4TAXORD\x01")
    source_digest.update(struct.pack(">I", 1))
    source_digest.update(struct.pack(">I", len(b"shard-a")))
    source_digest.update(b"shard-a")
    source_digest.update(struct.pack(">I", 0))
    tax_summary_by_name = {
        "contract": "ptg2_provider_tax_identity_projection_v1",
        "token_policy_id": policy_id,
        "token_policy_descriptor_sha256": _length_prefixed_sha256(
            b"PTG2V4TINPOLICY\x01",
            (
                policy_id.encode(),
                normalization.encode(),
                hmac_contract.encode(),
                prefix_contract.encode(),
                authority_contract.encode(),
            ),
        ),
        "normalization_contract": normalization,
        "hmac_contract": hmac_contract,
        "candidate_prefix_contract": prefix_contract,
        "authority_contract": authority_contract,
        "source_ordinal_contract": ("snapshot_shard_id_sorted_lsb0_bitmap_v1"),
        "source_ordinal_map": source_ordinals,
        "source_ordinal_map_digest": source_digest.hexdigest(),
        "source_shard_count": 1,
        "source_bitmap_bytes": 1,
        "provider_group_count": 4,
        "tax_identity_count": 1,
        "matched_ein_count": 1,
        "missing_count": 1,
        "malformed_count": 1,
        "unsupported_type_count": 1,
        "content_digest": "33" * 32,
    }
    return SimpleNamespace(
        summary={"tax_identity": tax_summary_by_name},
        observe={"group_count": 4},
        output_artifacts=(
            SimpleNamespace(
                name="provider_tax_identities",
                byte_count=91,
            ),
            SimpleNamespace(
                name="provider_group_tax_identities",
                byte_count=303,
            ),
        ),
    )


def test_v4_tax_policy_descriptor_matches_frozen_cross_language_vector():
    descriptor = shared_snapshot_publish._v4_tax_policy_descriptor(
        "ptg-tin-hmac-sha256-v1:release-1"
    )

    assert descriptor.hex() == (
        "a0c06f5494f80663686be6861038a880" "4d9509d0fdc2d2c8cc56c259e53d761c"
    )


def test_v4_tax_contract_is_recomputed_before_publication():
    compilation = _tax_identity_compilation_summary()

    contract = shared_snapshot_publish._validated_v4_tax_identity_contract(compilation)

    assert contract.provider_group_count == 4
    assert contract.tax_identity_count == 1
    assert contract.source_bitmap_bytes == 1
    assert shared_snapshot_publish._v4_tax_artifact_byte_count(compilation) == 394

    compilation.summary["tax_identity"]["token_policy_descriptor_sha256"] = "00" * 32
    with pytest.raises(RuntimeError, match="descriptor"):
        shared_snapshot_publish._validated_v4_tax_identity_contract(compilation)

@pytest.mark.asyncio
async def test_slow_shared_publish_copy_moves_progress_every_four_seconds(
    monkeypatch,
    tmp_path,
):
    """Measured publication bytes must advance progress_seq before COPY ends."""

    copy_path = tmp_path / "provider-graph.copy"
    copy_path.write_bytes(b"abcdefghijkl")
    elapsed_seconds = [0.0]
    progress_writes = _install_shared_publish_progress_capture(
        monkeypatch,
        elapsed_seconds,
    )
    publication_progress = shared_snapshot_publish._MeasuredPublicationProgress(
        "provider graph publication",
        _publish_progress_to_live,
        interval_seconds=4.0,
        clock=lambda: elapsed_seconds[0],
    )
    token = ptg_live_progress.set_live_progress_context(
        run_id="run-slow-shared-publish",
        attempt_id="attempt-1",
        attempt_started_at="2026-07-23T11:00:00Z",
    )
    try:
        await shared_publish._copy_binary_file_to_stage(
            copy_path,
            schema_name="mrf",
            stage_table="provider_graph_stage",
            columns=("payload",),
            progress_callback=publication_progress.add,
        )
        publication_progress.flush()
    finally:
        ptg_live_progress.reset_live_progress_context(token)

    _assert_shared_publish_cadence(progress_writes)


@pytest.mark.asyncio
async def test_dense_v4_dictionary_ranges_move_exact_progress_every_four_seconds():
    """Large dictionaries validate and publish through measured key ranges."""

    elapsed_seconds = [0.0]
    range_driver = _DenseDictionaryRangeDriver(elapsed_seconds)
    session = SimpleNamespace(
        execute=AsyncMock(side_effect=range_driver.execute_range_statement),
        scalar=AsyncMock(side_effect=range_driver.read_range_scalar),
    )
    progress_events = []
    progress = shared_snapshot_publish._MeasuredPublicationProgress(
        "dense dictionary publication",
        lambda stage, counters: progress_events.append(
            (elapsed_seconds[0], stage, dict(counters))
        ),
        interval_seconds=4.0,
        clock=lambda: elapsed_seconds[0],
    )
    dictionary_stage = _dense_dictionary_progress_stage()

    await shared_snapshot_publish._validate_v4_dictionary_stage(
        session,
        schema='"mrf"',
        stage=dictionary_stage,
        progress_callback=progress.add,
    )
    await shared_snapshot_publish._publish_v4_dictionary_stage_ranges(
        session,
        schema='"mrf"',
        snapshot_key=42,
        stage=dictionary_stage,
        progress_callback=progress.add,
    )
    progress.flush()

    emitted_times = [observed_at for observed_at, _stage, _counters in progress_events]
    assert (
        max(later - earlier for earlier, later in zip(emitted_times, emitted_times[1:]))
        <= 4.0
    )
    assert progress_events[-1][2]["validated_dictionary_rows"] == 200_001
    assert progress_events[-1][2]["published_dictionary_rows"] == 200_001
    _assert_dense_dictionary_range_statements(range_driver.statements)


def test_v4_dictionary_default_ranges_are_exact_and_ten_times_coarser():
    """One million dense rows need about one tenth of the former batches."""

    expected_count = 1_000_001
    ranges = tuple(shared_snapshot_publish._v4_dictionary_ranges(expected_count))

    assert len(ranges) == 11
    assert len(range(0, expected_count, 10_000)) == 101
    assert ranges[0] == (0, 100_000)
    assert ranges[-1] == (1_000_000, 1_000_001)
    assert all(
        earlier_end == later_start
        for (_earlier_start, earlier_end), (later_start, _later_end) in zip(
            ranges,
            ranges[1:],
        )
    )
    assert sum(range_end - range_start for range_start, range_end in ranges) == (
        expected_count
    )


def test_v4_dictionary_batch_sizer_enforces_bytes_and_adapts_time():
    """Byte ceilings win, while slow batches shrink and fast batches recover."""

    payload_budget = (
        shared_snapshot_publish._V4_DICTIONARY_MAX_ESTIMATED_ROW_WORK_BYTES
        - shared_snapshot_publish._V4_DICTIONARY_FIXED_WORK_OVERHEAD_BYTES
    )
    estimated_row_bytes = payload_budget // 40_000 + 1
    sizer = shared_snapshot_publish._V4DictionaryBatchSizer(
        estimated_row_bytes=estimated_row_bytes,
    )

    assert sizer.maximum_rows < 40_000
    assert (
        sizer.maximum_rows * estimated_row_bytes
        + shared_snapshot_publish._V4_DICTIONARY_FIXED_WORK_OVERHEAD_BYTES
        <= shared_snapshot_publish._V4_DICTIONARY_MAX_ESTIMATED_ROW_WORK_BYTES
    )
    initial_rows = sizer.current_rows
    sizer.observe(shared_snapshot_publish._V4_DICTIONARY_SLOW_STATEMENT_SECONDS)
    assert sizer.current_rows == max(sizer.fallback_rows, initial_rows // 2)
    reduced_rows = sizer.current_rows
    sizer.observe(shared_snapshot_publish._V4_DICTIONARY_RECOVERY_STATEMENT_SECONDS)
    assert sizer.current_rows == min(sizer.maximum_rows, reduced_rows * 2)


def test_v4_dictionary_batch_sizer_rejects_invalid_limits(monkeypatch):
    """Invalid estimates and fixed-overhead budgets fail before publication."""

    with pytest.raises(
        ValueError,
        match="row estimate must be positive",
    ):
        shared_snapshot_publish._V4DictionaryBatchSizer(
            estimated_row_bytes=0,
        )

    monkeypatch.setattr(
        shared_snapshot_publish,
        "_V4_DICTIONARY_MAX_ESTIMATED_ROW_WORK_BYTES",
        shared_snapshot_publish._V4_DICTIONARY_FIXED_WORK_OVERHEAD_BYTES,
    )
    with pytest.raises(
        RuntimeError,
        match="row-work budget is invalid",
    ):
        shared_snapshot_publish._V4DictionaryBatchSizer(
            estimated_row_bytes=1,
        )


@pytest.mark.asyncio
async def test_v4_dictionary_ranges_shrink_then_recover(monkeypatch):
    """Observed statement time changes only later contiguous boundaries."""

    elapsed_seconds = [0.0]
    durations = iter((5.0, 1.0, 1.0))
    ranges = []

    async def execute(_statement, parameters):
        ranges.append((parameters["range_start"], parameters["range_end"]))
        elapsed_seconds[0] += next(durations)
        row_count = parameters["range_end"] - parameters["range_start"]
        return SimpleNamespace(
            one=lambda: (
                row_count,
                parameters["range_start"],
                parameters["range_end"] - 1,
                True,
                0,
            )
        )

    monkeypatch.setattr(
        shared_snapshot_publish.time,
        "monotonic",
        lambda: elapsed_seconds[0],
    )
    session = SimpleNamespace(
        execute=AsyncMock(side_effect=execute),
        scalar=AsyncMock(return_value=None),
    )
    stage = _dense_dictionary_progress_stage()
    stage = shared_snapshot_publish._V4DenseDictionaryStage(
        **{**stage.__dict__, "expected_count": 250_000}
    )

    await shared_snapshot_publish._validate_v4_dictionary_stage(
        session,
        schema='"mrf"',
        stage=stage,
        progress_callback=None,
    )

    assert ranges == [(0, 100_000), (100_000, 150_000), (150_000, 250_000)]


@pytest.mark.asyncio
async def test_v4_target_key_enumeration_adapts_without_full_count(monkeypatch):
    """Persisted completeness uses indexed pages and observed statement time."""

    elapsed_seconds = [0.0]
    durations = iter((5.0, 1.0, 1.0))
    pages = iter((((1,), (2,)), ((3,),), ()))
    batch_rows = []
    statements = []

    async def execute(statement, parameters):
        statements.append(str(statement))
        batch_rows.append(parameters["batch_rows"])
        elapsed_seconds[0] += next(durations)
        return _row_result(next(pages))

    monkeypatch.setattr(
        shared_snapshot_publish.time,
        "monotonic",
        lambda: elapsed_seconds[0],
    )
    observed_count = await shared_snapshot_publish._count_v4_target_keys(
        SimpleNamespace(execute=execute),
        schema='"mrf"',
        target_table='"target"',
        key_name='"dictionary_key"',
        snapshot_key=17,
        initial_key=-1,
        estimated_row_bytes=160,
        heartbeat_callback=None,
    )

    assert observed_count == 3
    assert batch_rows == [100_000, 50_000, 100_000]
    assert all("ORDER BY" in statement for statement in statements)
    assert all("LIMIT :batch_rows" in statement for statement in statements)
    assert all("COUNT(" not in statement for statement in statements)


def test_v4_dictionary_heartbeat_repeats_only_completed_counters():
    """A long query heartbeat cannot claim an unfinished dictionary range."""

    elapsed_seconds = [0.0]
    snapshots = []
    progress = shared_snapshot_publish._MeasuredPublicationProgress(
        "dictionary",
        lambda _stage, counters: snapshots.append(dict(counters)),
        interval_seconds=4.0,
        clock=lambda: elapsed_seconds[0],
    )
    progress.add("published_dictionary_rows", 100_000)
    progress.flush()

    elapsed_seconds[0] = 4.0
    progress.heartbeat()

    assert snapshots == [
        {"published_dictionary_rows": 100_000},
        {"published_dictionary_rows": 100_000},
    ]
