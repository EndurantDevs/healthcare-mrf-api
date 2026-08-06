# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime
import hashlib
import json
import struct
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


class _SlowProgressDriver:
    def __init__(self, elapsed_seconds):
        self.elapsed_seconds = elapsed_seconds

    async def copy_to_table(self, _target_table, *, source, **_kwargs):
        while True:
            self.elapsed_seconds[0] += 1.0
            if not source.read(1):
                return


class _SlowProgressAcquire:
    def __init__(self, elapsed_seconds):
        self.elapsed_seconds = elapsed_seconds

    async def __aenter__(self):
        return SimpleNamespace(
            raw_connection=SimpleNamespace(
                driver_connection=_SlowProgressDriver(self.elapsed_seconds)
            )
        )

    async def __aexit__(self, *_exc_info):
        return False


def _install_shared_publish_progress_capture(monkeypatch, elapsed_seconds):
    progress_writes = []
    base_time = datetime.datetime(2026, 7, 23, 11, 0, 0)
    fake_redis = AtomicLiveProgressRedis(
        on_progress_write=lambda _key, _ttl, encoded_progress: progress_writes.append(
            (elapsed_seconds[0], json.loads(encoded_progress))
        )
    )
    monkeypatch.setattr(
        shared_publish.db,
        "acquire",
        lambda: _SlowProgressAcquire(elapsed_seconds),
    )
    monkeypatch.setattr(live_progress, "_redis", lambda: fake_redis)
    monkeypatch.setattr(
        live_progress,
        "_utc_now",
        lambda: base_time + datetime.timedelta(seconds=elapsed_seconds[0]),
    )
    monkeypatch.setattr(live_progress, "enqueue_status_event", lambda _event: None)
    return progress_writes


def _publish_progress_to_live(stage_name, counters_by_name):
    ptg_live_progress.write_live_progress(
        phase=f"publishing: {stage_name}",
        unit="publish_steps",
        done=5,
        total=8,
        pct=96,
        counters=dict(counters_by_name),
    )


def _assert_shared_publish_cadence(progress_writes):
    progress_snapshots = [
        progress_snapshot for _at, progress_snapshot in progress_writes
    ]
    progress_gaps = [
        later_at - earlier_at
        for (earlier_at, _earlier), (later_at, _later) in zip(
            progress_writes,
            progress_writes[1:],
        )
    ]
    assert [
        progress_snapshot["counters"]["copy_bytes"]
        for progress_snapshot in progress_snapshots
    ] == [4, 8, 12]
    assert max(progress_gaps) <= 4.0
    assert [
        progress_snapshot["progress_seq"] for progress_snapshot in progress_snapshots
    ] == [1, 2, 3]


class _DenseDictionaryRangeDriver:
    def __init__(self, elapsed_seconds):
        self.elapsed_seconds = elapsed_seconds
        self.statements = []

    async def execute_range_statement(self, statement, parameters):
        self.elapsed_seconds[0] += 4.0
        self.statements.append((str(statement), dict(parameters)))
        range_start = int(parameters["range_start"])
        range_end = int(parameters["range_end"])
        if str(statement).lstrip().startswith("SELECT COUNT"):
            row_count = range_end - range_start
            return SimpleNamespace(
                one=lambda: (
                    row_count,
                    range_start,
                    range_end - 1,
                    True,
                    0,
                )
            )
        return SimpleNamespace()

    async def read_range_scalar(self, statement, parameters):
        if str(statement).lstrip().startswith("SELECT COUNT"):
            return int(parameters["range_end"]) - int(parameters["range_start"])
        return None


def _row_result(rows):
    return SimpleNamespace(all=lambda: tuple(rows))


def _dense_dictionary_progress_stage():
    return shared_snapshot_publish._V4DenseDictionaryStage(
        stage_table="group_stage",
        key_name="provider_group_key",
        expected_count=200_001,
        target_table="ptg2_v3_provider_group",
        columns=("provider_group_key", "provider_group_global_id_128"),
        value_predicate="octet_length(provider_group_global_id_128) = 16",
    )


def _assert_dense_dictionary_range_statements(statements):
    range_statements = [
        statement for statement, _parameters in statements if "range_start" in statement
    ]
    assert len(range_statements) == 6
    assert all(">= :range_start" in statement for statement in range_statements)
    assert all("< :range_end" in statement for statement in range_statements)
    assert not any("COUNT(DISTINCT" in statement for statement in range_statements)


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


def _v4_graph_summary():
    """Return one complete pattern-selected publication summary."""

    encoding_options_by_name = {
        name: option_value
        for name, option_value in graph_compiler._effective_compiler_options(
            None
        ).items()
        if name in graph_compiler.PTG2_V4_GRAPH_ENCODING_OPTION_NAMES
    }
    return {
        **encoding_options_by_name,
        "format": "ptg2_provider_graph_v4",
        "selected_layout": "pattern",
        "selected_encoded_bytes": 231,
        "direct_layout_complete_prefix_eligible": True,
        "pattern_layout_sparse_prefix_eligible": True,
        "pattern_layout_serving_degree_eligible": True,
        "direct_complete_prefix_projection_encoded_bytes": 10,
        "pattern_sparse_prefix_owner_count": 0,
        "pattern_sparse_prefix_member_count": 0,
        "pattern_sparse_prefix_raw_bytes": 0,
        "pattern_sparse_prefix_projection_encoded_bytes": 10,
        "direct_graph_encoded_bytes": 100,
        "direct_mapping_persistence_encoded_bytes": 132,
        "direct_inferred_taxonomy_encoded_bytes": 0,
        "direct_inferred_taxonomy_eligible": True,
        "direct_inferred_taxonomy_rejection_reason": None,
        "direct_inferred_taxonomy_rejection_rule_digest": None,
        "direct_inferred_taxonomy_rejection_observed_count": None,
        "direct_inferred_taxonomy_rejection_cap": None,
        "direct_map_payload_encoded_bytes": 132,
        "direct_map_coordinate_count": 1,
        "direct_map_pack_count": 1,
        "direct_map_object_kind_count": 1,
        "direct_complete_encoded_bytes": 232,
        "pattern_graph_encoded_bytes": 99,
        "pattern_mapping_persistence_encoded_bytes": 132,
        "pattern_inferred_taxonomy_encoded_bytes": 0,
        "pattern_inferred_taxonomy_eligible": True,
        "pattern_inferred_taxonomy_rejection_reason": None,
        "pattern_inferred_taxonomy_rejection_rule_digest": None,
        "pattern_inferred_taxonomy_rejection_observed_count": None,
        "pattern_inferred_taxonomy_rejection_cap": None,
        "pattern_map_payload_encoded_bytes": 132,
        "pattern_map_coordinate_count": 1,
        "pattern_map_pack_count": 1,
        "pattern_map_object_kind_count": 1,
        "pattern_complete_encoded_bytes": 231,
        "npi_prefix_target": 200,
        "max_npi_prefix_override_owners": 250_000,
        "max_npi_prefix_override_bytes": 64 * 1024 * 1024,
        "max_set_patterns_per_set": 4096,
        "max_set_components_per_fallback_set": 4096,
        "resource_admission": {
            "max_estimated_model_bytes": 8 * 1024 * 1024 * 1024,
            "max_factor_edges": 1_000_000,
        },
        "observe": {"unsafe_pattern_component_set_count": 0},
    }


def _v4_graph_publication_fixture(tmp_path):
    """Return authenticated graph, CAS, and map publication evidence."""

    artifact = SimpleNamespace(
        name="graph_blocks",
        byte_count=12,
        sha256="a" * 64,
        row_count=1,
    )
    compilation = SimpleNamespace(
        output_artifacts=(artifact,),
        block_copy_path=tmp_path / "graph.copy",
        reference_manifest_path=tmp_path / "references.jsonl",
        selected_layout="pattern",
        summary=_v4_graph_summary(),
        relation_summaries=(),
        heavy_bitmaps=(),
        observe={"group_count": 3, "npi_count": 2},
        resource_admission={
            "input_factor_bytes": 512,
            "factor_edge_count": 9,
        },
        block_count=1,
        provider_set_audit_npi_copy_path=tmp_path / "audit.copy",
    )
    cas_publication = SimpleNamespace(
        staged_row_count=1,
        unique_block_count=1,
        logical_byte_count=12,
        stored_byte_count=12,
    )
    map_summary = SimpleNamespace(
        map_digest=b"m" * 32,
        object_kinds=("v4_set_patterns_members_v1",),
        object_kind_count=1,
        map_pack_count=1,
        coordinate_count=1,
        stored_map_byte_count=132,
    )
    return compilation, cas_publication, map_summary


def _patch_v4_graph_publication(
    monkeypatch,
    cas_publication,
    map_summary,
):
    taxonomy_publication = SimpleNamespace(
        packed_byte_count=12,
        pattern_member_bytes=40,
        manifest={},
    )
    tax_identity_publication = SimpleNamespace(
        artifact_byte_count=24,
        manifest={
            "contract": "ptg2_provider_group_tax_identity_v1",
            "provider_group_count": 3,
            "tax_identity_count": 1,
            "content_digest": (b"t" * 32).hex(),
        },
    )
    tax_identity_source_publication = SimpleNamespace(
        artifact_byte_count=16,
        as_dict=lambda: {
            "contract": "ptg2_provider_group_tax_identity_source_v1",
            "content_digest": "s" * 64,
        },
    )
    publish_maps_mock = AsyncMock(
        return_value=(
            cas_publication,
            map_summary,
            taxonomy_publication,
            tax_identity_publication,
            tax_identity_source_publication,
        )
    )
    replacements_by_name = {
        "create_shared_block_stage": AsyncMock(),
        "copy_shared_block_binary_file": AsyncMock(),
        "_publish_v4_dictionaries_and_maps": publish_maps_mock,
    }
    for name, replacement in replacements_by_name.items():
        monkeypatch.setattr(shared_snapshot_publish, name, replacement)
    monkeypatch.setattr(
        shared_snapshot_publish.db,
        "status",
        AsyncMock(),
    )
    return publish_maps_mock


@pytest.mark.asyncio
async def test_v4_graph_publish_threads_compressed_acquisition_resources(
    monkeypatch,
    tmp_path,
):
    """Seal acquisition bytes with graph diagnostics, not the CAS stage."""

    compilation, cas_publication, map_summary = _v4_graph_publication_fixture(tmp_path)
    publish_maps_mock = _patch_v4_graph_publication(
        monkeypatch,
        cas_publication,
        map_summary,
    )

    publication = await shared_snapshot_publish._publish_v4_graph(
        compilation,
        publication_context=shared_snapshot_publish._V4GraphCoordinates(
            schema_name="mrf",
            snapshot_key=17,
            build_token="token",
        ),
        compressed_acquisition_bytes=4_096,
        empty_npi_tin_only_normalization_count=2,
    )

    assert publication.logical_byte_count == 104
    assert publication.stored_byte_count == 236
    assert publication.provider_tax_identity["tax_identity_count"] == 1
    assert (
        publication.provider_tax_identity_source["contract"]
        == "ptg2_provider_group_tax_identity_source_v1"
    )
    publication_context = publish_maps_mock.await_args.kwargs[
        "publication_context"
    ]
    assert publication_context.block_stage.startswith(
        "ptg2_v3_block_stage_"
    )
    assert not hasattr(shared_snapshot_publish, "publish_v4_cas_block_stage")
    assert publish_maps_mock.await_args.kwargs["compressed_acquisition_bytes"] == 4_096
    assert (
        publish_maps_mock.await_args.kwargs["empty_npi_tin_only_normalization_count"]
        == 2
    )


def test_v4_graph_publish_rejects_packed_map_plan_drift(
    tmp_path,
):
    """Reject estimator drift inside the CAS/map publication transaction."""

    compilation, cas_publication, map_summary = _v4_graph_publication_fixture(tmp_path)
    drifted_map_summary = SimpleNamespace(
        **{
            **vars(map_summary),
            "stored_map_byte_count": map_summary.stored_map_byte_count + 1,
        }
    )
    with pytest.raises(
        RuntimeError,
        match="packed-map plan differs from publication",
    ):
        shared_snapshot_publish._require_v4_atomic_map_publication(
            compilation,
            cas_publication,
            drifted_map_summary,
        )


@pytest.mark.parametrize(
    ("cas_count", "map_count"),
    ((2, 1), (1, 2)),
    ids=("extra-cas-stage-row", "extra-map-coordinate"),
)
def test_v4_atomic_publication_rejects_coordinate_count_drift(
    cas_count,
    map_count,
):
    """CAS and map coordinates must both equal the compiler block count."""

    with pytest.raises(RuntimeError, match="coordinate counts changed"):
        shared_snapshot_publish._require_v4_atomic_coordinate_counts(
            1,
            SimpleNamespace(staged_row_count=cas_count),
            SimpleNamespace(coordinate_count=map_count),
        )


@pytest.mark.asyncio
async def test_v4_graph_publish_queues_blocks_after_stage_failure(
    monkeypatch,
    tmp_path,
):
    """A partial CAS copy stays recoverable while its stage is removed."""

    compilation, _, _ = _v4_graph_publication_fixture(tmp_path)
    queue_failed = AsyncMock()
    status = AsyncMock()
    monkeypatch.setattr(
        shared_snapshot_publish,
        "create_shared_block_stage",
        AsyncMock(),
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "copy_shared_block_binary_file",
        AsyncMock(side_effect=RuntimeError("copy failed")),
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "_queue_failed_v4_graph_blocks",
        queue_failed,
    )
    monkeypatch.setattr(shared_snapshot_publish.db, "status", status)

    with pytest.raises(RuntimeError, match="copy failed"):
        await shared_snapshot_publish._publish_v4_graph(
            compilation,
            publication_context=shared_snapshot_publish._V4GraphCoordinates(
                schema_name="mrf",
                snapshot_key=17,
                build_token="token",
            ),
            compressed_acquisition_bytes=1,
            empty_npi_tin_only_normalization_count=0,
        )

    queue_failed.assert_awaited_once_with(
        schema_name="mrf",
        reference_manifest_path=compilation.reference_manifest_path,
    )
    assert "DROP TABLE IF EXISTS" in status.await_args.args[0]


def _failed_tax_stage_compilation(tmp_path):
    """Return the minimum compilation needed to reach the build fence."""

    taxonomy_path = tmp_path / "inferred-taxonomy.copy"
    references_path = tmp_path / "graph-references.jsonl"
    return SimpleNamespace(
        observe={
            "group_count": 4,
            "component_count": 0,
            "npi_count": 0,
            "npi_prefix_override_owner_count": 0,
            "npi_prefix_override_member_count": 0,
        },
        selected_layout="direct",
        pattern_copy_path=None,
        summary={"npi_prefix_target": 201},
        group_copy_path=tmp_path / "groups.copy",
        component_copy_path=tmp_path / "components.copy",
        npi_copy_path=tmp_path / "npi.copy",
        provider_set_npi_prefix_override_copy_path=tmp_path / "prefix.copy",
        provider_tax_identity_copy_path=tmp_path / "tax.copy",
        provider_group_tax_identity_copy_path=tmp_path / "group-tax.copy",
        inferred_taxonomy_copy_path=taxonomy_path,
        reference_manifest_path=references_path,
        output_artifacts=(
            SimpleNamespace(
                name="inferred_taxonomy_candidates",
                path=taxonomy_path,
                byte_count=0,
                sha256="e3b0c44298fc1c149afbf4c8996fb924"
                "27ae41e4649b934ca495991b7852b855",
                row_count=0,
            ),
            SimpleNamespace(
                name="graph_references",
                path=references_path,
                byte_count=0,
                sha256="e3b0c44298fc1c149afbf4c8996fb924"
                "27ae41e4649b934ca495991b7852b855",
                row_count=0,
            ),
        ),
    )


def _tax_stage_contract():
    """Return one complete four-state publication contract."""

    return shared_snapshot_publish._V4TaxIdentityContract(
        token_policy_id="ptg-tin-hmac-sha256-v1:release-1",
        token_policy_descriptor_sha256=b"p" * 32,
        source_ordinal_map=({"shard_id": "shard-a", "ordinal": 0},),
        source_ordinal_map_digest=b"s" * 32,
        source_shard_count=1,
        source_bitmap_bytes=1,
        provider_group_count=4,
        tax_identity_count=1,
        matched_ein_count=1,
        missing_count=1,
        malformed_count=1,
        unsupported_type_count=1,
        content_digest=b"c" * 32,
    )


def _atomic_source_transaction_fixture():
    """Return callbacks that record one source-publication rollback path."""

    session = object()
    publication_events = []
    prepared = SimpleNamespace(
        cleanup=lambda: publication_events.append(("cleanup", None)),
    )

    @asynccontextmanager
    async def transaction():
        publication_events.append(("begin", session))
        try:
            yield session
        except BaseException:
            publication_events.append(("rollback", session))
            raise
        else:
            publication_events.append(("commit", session))

    async def stage_source(actual_session, actual_prepared):
        assert actual_session is session
        assert actual_prepared is prepared
        publication_events.append(("source-stage", actual_session))
        return "source-stage"

    async def publish_tax_groups(actual_session, **_kwargs):
        assert actual_session is session
        publication_events.append(("merged-tax-groups", actual_session))

    async def publish_source(actual_session, **kwargs):
        assert actual_session is session
        assert kwargs["prepared"] is prepared
        publication_events.append(("source-local-tax", actual_session))
        raise RuntimeError("post-source graph failure")

    return SimpleNamespace(
        session=session,
        publication_events=publication_events,
        prepared=prepared,
        transaction=transaction,
        stage_source=stage_source,
        publish_tax_groups=publish_tax_groups,
        publish_source=publish_source,
    )


def _install_atomic_source_transaction_mocks(monkeypatch, atomic_fixture) -> None:
    """Replace unrelated graph work while retaining the real call ordering."""

    monkeypatch.setattr(shared_snapshot_publish.db, "status", AsyncMock())
    monkeypatch.setattr(
        shared_snapshot_publish.db,
        "transaction",
        atomic_fixture.transaction,
    )
    replacements_by_name = {
        "_validated_v4_tax_identity_contract": (
            lambda _compilation: _tax_stage_contract()
        ),
        "_v4_tax_artifact_byte_count": lambda _compilation: 394,
        "prepare_tax_identity_source_projection": (
            lambda *_args, **_kwargs: atomic_fixture.prepared
        ),
        "_copy_binary_file_to_stage": AsyncMock(),
        "stage_tax_identity_source_projection": atomic_fixture.stage_source,
        "lock_v4_shared_layout_for_map_write": AsyncMock(),
        "_publish_v4_cas_in_session": AsyncMock(return_value=object()),
        "stage_v4_inferred_taxonomy_compiler_copy": AsyncMock(
            return_value=SimpleNamespace(table_name="taxonomy-stage")
        ),
        "_validate_v4_dictionary_stage": AsyncMock(),
        "_validate_v4_tax_identity_stages": AsyncMock(),
        "publish_v4_snapshot_maps": AsyncMock(return_value=object()),
        "_require_v4_atomic_map_publication": lambda *_args: None,
        "_publish_v4_tax_identity_manifest": AsyncMock(return_value={}),
        "_publish_v4_dictionary_stage_ranges": AsyncMock(),
        "_publish_v4_tax_group_ranges": atomic_fixture.publish_tax_groups,
        "publish_staged_tax_identity_source_projection": (
            atomic_fixture.publish_source
        ),
    }
    for name, replacement in replacements_by_name.items():
        monkeypatch.setattr(shared_snapshot_publish, name, replacement)


@pytest.mark.asyncio
async def test_v4_source_projection_shares_atomic_graph_transaction(
    monkeypatch,
    tmp_path,
) -> None:
    """Merged and source-local tax rows must roll back as one graph bundle."""

    atomic_fixture = _atomic_source_transaction_fixture()
    _install_atomic_source_transaction_mocks(monkeypatch, atomic_fixture)

    with pytest.raises(RuntimeError, match="post-source graph failure"):
        await shared_snapshot_publish._publish_v4_dictionaries_and_maps(
            _failed_tax_stage_compilation(tmp_path),
            publication_context=shared_snapshot_publish._V4AtomicPublishContext(
                schema_name="mrf",
                block_stage="ptg2_v3_block_stage_exact",
                snapshot_key=44,
                build_token="exact-build",
            ),
            compressed_acquisition_bytes=1,
            empty_npi_tin_only_normalization_count=0,
            tax_identity_source_artifacts=({},),
        )

    assert atomic_fixture.publication_events == [
        ("begin", atomic_fixture.session),
        ("source-stage", atomic_fixture.session),
        ("merged-tax-groups", atomic_fixture.session),
        ("source-local-tax", atomic_fixture.session),
        ("rollback", atomic_fixture.session),
        ("cleanup", None),
    ]


@pytest.mark.asyncio
async def test_v4_tax_group_lookup_is_bounded_and_heartbeated(monkeypatch):
    """Tax completeness uses a bounded indexed join and completed counters."""

    clock_seconds = [0.0]
    snapshots = []
    progress = shared_snapshot_publish._MeasuredPublicationProgress(
        "tax relation proof",
        lambda _stage, counters: snapshots.append(dict(counters)),
        clock=lambda: clock_seconds[0],
    )
    progress.add("validated_dictionary_rows", 4)
    progress.flush()

    async def await_statement(statement_awaitable, **kwargs):
        result = await statement_awaitable
        clock_seconds[0] = 4.0
        kwargs["heartbeat_callback"]()
        return result, 5.0

    session = SimpleNamespace(
        execute=AsyncMock(return_value=_row_result(())),
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "_await_v4_dictionary_statement",
        await_statement,
    )

    tax_group_records, elapsed_seconds = (
        await shared_snapshot_publish._v4_tax_group_rows_batch(
            session,
            schema='"mrf"',
            group_tax_stage='"group_tax_stage"',
            graph_group_stage='"group_stage"',
            previous_group_id=b"",
            batch_rows=100_000,
            heartbeat_callback=progress.heartbeat,
        )
    )

    statement = str(session.execute.await_args.args[0])
    assert tax_group_records == ()
    assert elapsed_seconds == 5.0
    assert "LEFT JOIN" in statement
    assert "ORDER BY sidecar.provider_group_global_id_128" in statement
    assert "LIMIT :batch_rows" in statement
    assert "COUNT(DISTINCT" not in statement
    assert "NOT EXISTS" not in statement
    assert snapshots == [
        {"validated_dictionary_rows": 4},
        {"validated_dictionary_rows": 4},
    ]


def test_v4_tax_group_reference_bitset_is_exact_and_group_bound():
    """Duplicate references count once and every sidecar must bind a group."""

    contract = _tax_stage_contract()
    digest = hashlib.sha256()
    count_by_state = {
        name: 0
        for name in (
            "matched_ein",
            "missing",
            "malformed",
            "unsupported_type",
        )
    }
    group_rows = (
        (b"\x01" * 16, "matched_ein", 0, b"\x01", True),
        (b"\x02" * 16, "matched_ein", 0, b"\x01", True),
    )

    latest_group_id, new_reference_count = (
        shared_snapshot_publish._consume_v4_tax_group_rows(
            group_rows,
            previous_group_id=b"",
            contract=contract,
            content_digest=digest,
            count_by_state=count_by_state,
            referenced_token_bits=bytearray(1),
        )
    )

    assert latest_group_id == b"\x02" * 16
    assert new_reference_count == 1
    assert count_by_state["matched_ein"] == 2
    with pytest.raises(
        RuntimeError,
        match="provider-group tax identity changed",
    ):
        shared_snapshot_publish._validated_v4_tax_group_row(
            (b"\x03" * 16, "missing", None, b"\x01", False),
            previous_group_id=b"\x02" * 16,
            contract=contract,
        )


@pytest.mark.asyncio
async def test_v4_tax_stages_are_removed_after_transaction_failure(
    monkeypatch,
    tmp_path,
) -> None:
    """A fenced publication failure must not strand token-bearing stages."""

    @asynccontextmanager
    async def transaction():
        yield object()

    status_mock = AsyncMock()
    monkeypatch.setattr(shared_snapshot_publish.db, "status", status_mock)
    monkeypatch.setattr(
        shared_snapshot_publish.db,
        "transaction",
        transaction,
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "_validated_v4_tax_identity_contract",
        lambda _compilation: _tax_stage_contract(),
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "_v4_tax_artifact_byte_count",
        lambda _compilation: 394,
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "_copy_binary_file_to_stage",
        AsyncMock(),
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "lock_v4_shared_layout_for_map_write",
        AsyncMock(side_effect=RuntimeError("fenced publication")),
    )

    with pytest.raises(RuntimeError, match="fenced publication"):
        await shared_snapshot_publish._publish_v4_dictionaries_and_maps(
            _failed_tax_stage_compilation(tmp_path),
            publication_context=shared_snapshot_publish._V4AtomicPublishContext(
                schema_name="mrf",
                block_stage="ptg2_v3_block_stage_exact",
                snapshot_key=41,
                build_token="exact-build",
            ),
            compressed_acquisition_bytes=1,
            empty_npi_tin_only_normalization_count=0,
        )

    cleanup_statement = str(status_mock.await_args.args[0])
    assert "DROP TABLE IF EXISTS" in cleanup_statement
    assert "ptg2_v4_tax_identity_stage_" in cleanup_statement
    assert "ptg2_v4_group_tax_identity_stage_" in cleanup_statement


@pytest.mark.asyncio
async def test_v4_partial_stage_creation_is_removed(
    monkeypatch,
    tmp_path,
) -> None:
    """A failed CREATE sequence removes every randomized stage name."""

    status_mock = AsyncMock(
        side_effect=(
            None,
            None,
            None,
            None,
            None,
            RuntimeError("stage create failed"),
            None,
        )
    )
    monkeypatch.setattr(shared_snapshot_publish.db, "status", status_mock)
    monkeypatch.setattr(
        shared_snapshot_publish,
        "_validated_v4_tax_identity_contract",
        lambda _compilation: _tax_stage_contract(),
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "_v4_tax_artifact_byte_count",
        lambda _compilation: 394,
    )

    with pytest.raises(RuntimeError, match="stage create failed"):
        await shared_snapshot_publish._publish_v4_dictionaries_and_maps(
            _failed_tax_stage_compilation(tmp_path),
            publication_context=shared_snapshot_publish._V4AtomicPublishContext(
                schema_name="mrf",
                block_stage="ptg2_v3_block_stage_exact",
                snapshot_key=42,
                build_token="exact-build",
            ),
            compressed_acquisition_bytes=1,
            empty_npi_tin_only_normalization_count=0,
        )

    cleanup_statement = str(status_mock.await_args.args[0])
    assert "DROP TABLE IF EXISTS" in cleanup_statement
    assert "ptg2_v4_tax_identity_stage_" in cleanup_statement


@pytest.mark.asyncio
async def test_v4_first_stage_creation_failure_preserves_original_error(
    monkeypatch,
    tmp_path,
) -> None:
    """No invalid empty DROP may mask failure of the first stage CREATE."""

    status_mock = AsyncMock(side_effect=RuntimeError("first stage create failed"))
    monkeypatch.setattr(shared_snapshot_publish.db, "status", status_mock)
    monkeypatch.setattr(
        shared_snapshot_publish,
        "_validated_v4_tax_identity_contract",
        lambda _compilation: _tax_stage_contract(),
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "_v4_tax_artifact_byte_count",
        lambda _compilation: 394,
    )

    with pytest.raises(RuntimeError, match="first stage create failed"):
        await shared_snapshot_publish._publish_v4_dictionaries_and_maps(
            _failed_tax_stage_compilation(tmp_path),
            publication_context=shared_snapshot_publish._V4AtomicPublishContext(
                schema_name="mrf",
                block_stage="ptg2_v3_block_stage_exact",
                snapshot_key=43,
                build_token="exact-build",
            ),
            compressed_acquisition_bytes=1,
            empty_npi_tin_only_normalization_count=0,
        )

    status_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_v4_dictionary_cleanup_preserves_primary_error_and_releases_copy(
    monkeypatch,
) -> None:
    """A failed stage drop must not mask publication failure or skip file cleanup."""

    drop_stages = AsyncMock(side_effect=RuntimeError("stage drop failed"))
    prepared = SimpleNamespace(cleanup=Mock())
    monkeypatch.setattr(
        shared_snapshot_publish,
        "_drop_v4_dictionary_stages",
        drop_stages,
    )

    await shared_snapshot_publish._cleanup_v4_dictionary_attempt(
        schema='"mrf"',
        stages=("stage-a",),
        prepared_tax_identity_source=prepared,
        preserve_primary_error=True,
    )

    prepared.cleanup.assert_called_once_with()


@pytest.mark.asyncio
async def test_v4_dictionary_cleanup_reports_drop_error_after_success(
    monkeypatch,
) -> None:
    """A stage-drop failure remains visible when no publication error exists."""

    drop_stages = AsyncMock(side_effect=RuntimeError("stage drop failed"))
    prepared = SimpleNamespace(cleanup=Mock())
    monkeypatch.setattr(
        shared_snapshot_publish,
        "_drop_v4_dictionary_stages",
        drop_stages,
    )

    with pytest.raises(RuntimeError, match="stage drop failed"):
        await shared_snapshot_publish._cleanup_v4_dictionary_attempt(
            schema='"mrf"',
            stages=("stage-a",),
            prepared_tax_identity_source=prepared,
            preserve_primary_error=False,
        )

    prepared.cleanup.assert_called_once_with()


def _patch_disabled_v4_publication(monkeypatch):
    prepared_price = object()

    @asynccontextmanager
    async def transaction():
        yield object()

    state = SimpleNamespace(
        prepared_price=prepared_price,
        prepare_mock=AsyncMock(return_value=(prepared_price, 0.0, None, None)),
        publish_v3_mock=AsyncMock(return_value="v3-publication"),
        compile_v4_mock=AsyncMock(),
        publish_v4_mock=AsyncMock(),
        cleanup_mock=AsyncMock(),
    )
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.setattr(shared_snapshot_publish.db, "transaction", transaction)
    replacements_by_name = {
        "touch_shared_layout_build": AsyncMock(),
        "_prepare_price_with_early_finalizer": state.prepare_mock,
        "_publish_prepared_shared_layout": state.publish_v3_mock,
        "compile_provider_graph_v4_rust": state.compile_v4_mock,
        "_publish_v4_graph": state.publish_v4_mock,
        "cleanup_prepared_shared_price_artifacts": state.cleanup_mock,
    }
    for name, replacement in replacements_by_name.items():
        monkeypatch.setattr(shared_snapshot_publish, name, replacement)
    return state


@pytest.mark.asyncio
async def test_v4_disabled_publication_keeps_v3_path(
    monkeypatch,
) -> None:
    """Leave the reviewed V3 publication path independent of V4 evidence."""

    state = _patch_disabled_v4_publication(monkeypatch)

    publication = await shared_snapshot_publish.publish_strict_shared_v3_layout(
        schema_name="mrf",
        manifest_stage_table="manifest-stage",
        reserved_snapshot_key=7,
        build_token="token",
        expected_coverage_scope_id=b"c" * 32,
        logical_snapshot_id="snapshot",
        expected_source_identities=(),
        serving_run_entries=(),
        code_dictionary_entries=(),
        provider_set_metadata_entries=(),
        source_audit_witness_entries=(),
        expected_raw_source_sha256=(),
        graph_artifact_entries=(),
        provider_identifier_quarantine={},
        provider_graph_v4=False,
    )

    assert publication == "v3-publication"
    assert state.publish_v3_mock.await_args.kwargs["provider_graph_v4"] is False
    assert (
        state.publish_v3_mock.await_args.kwargs["compressed_acquisition_bytes"] is None
    )
    assert (
        state.publish_v3_mock.await_args.kwargs[
            "empty_npi_tin_only_normalization_count"
        ]
        is None
    )
    state.compile_v4_mock.assert_not_awaited()
    state.publish_v4_mock.assert_not_awaited()
    state.cleanup_mock.assert_awaited_once_with(state.prepared_price)


@pytest.mark.asyncio
async def test_independent_publication_lanes_start_together():
    started_lanes: set[str] = set()
    release = asyncio.Event()

    async def lane(name: str) -> str:
        started_lanes.add(name)
        if len(started_lanes) == 4:
            release.set()
        await asyncio.wait_for(release.wait(), timeout=0.5)
        return name

    lane_outputs = await _run_independent_publication_lanes(
        finalizer_blocks=lambda: lane("finalizer_blocks"),
        provider_graph=lambda: lane("provider_graph"),
        price=lambda: lane("price"),
        source_witness=lambda: lane("source_witness"),
    )

    assert started_lanes == {
        "finalizer_blocks",
        "provider_graph",
        "price",
        "source_witness",
    }
    assert lane_outputs == (
        "finalizer_blocks",
        "provider_graph",
        "price",
        "source_witness",
    )


def _early_finalizer_callbacks(state, prepared_price):
    async def prepare_price(*, price_key_ready, **_kwargs):
        price_key_ready(
            PreparedSharedPriceKeyMap(
                schema_name="mrf",
                price_key_map="price_key_map",
                price_set_count=3,
            )
        )
        await state.atom_release.wait()
        return prepared_price

    async def run_finalizer(**kwargs):
        state.finalizer_calls.append(kwargs)
        state.finalizer_started.set()
        await state.finalizer_release.wait()
        return {"blocks": {}}

    async def publish_price(prepared):
        assert prepared is prepared_price
        state.price_publish_started.set()
        await state.price_publish_release.wait()
        return "published-price"

    return prepare_price, run_finalizer, publish_price


def _install_early_finalizer_mocks(monkeypatch, tmp_path, state, prepared_price):
    prepare_price, run_finalizer, publish_price = _early_finalizer_callbacks(
        state,
        prepared_price,
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "prepare_shared_price_artifacts",
        prepare_price,
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "export_shared_price_key_map",
        AsyncMock(return_value=tmp_path / "price-key-map.copy"),
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "run_v3_direct_finalizer",
        run_finalizer,
    )
    return publish_price


def _assert_early_finalizer_pipeline(pipeline_output, state, prepared_price):
    prepared, prepare_seconds, prepared_finalizer, price_publication = pipeline_output
    assert prepared is prepared_price
    assert prepare_seconds >= 0
    assert prepared_finalizer.summary == {"blocks": {}}
    assert prepared_finalizer.price_key_map_export_seconds >= 0
    assert prepared_finalizer.finalizer_seconds >= 0
    assert state.finalizer_calls[0]["price_key_map_row_count"] == 3
    assert state.finalizer_calls[0]["scratch_durability"] == (
        shared_snapshot_publish.PTG2_V3_EPHEMERAL_SCRATCH_DURABILITY
    )
    assert price_publication.publication == "published-price"
    assert price_publication.publish_seconds >= 0


@pytest.mark.asyncio
async def test_finalizer_starts_before_independent_atom_preparation_finishes(
    monkeypatch,
    tmp_path,
):
    """Start finalization and price publication at their exact dependencies."""

    prepared_price = object()
    state = SimpleNamespace(
        atom_release=asyncio.Event(),
        finalizer_started=asyncio.Event(),
        finalizer_release=asyncio.Event(),
        finalizer_calls=[],
        price_publish_started=asyncio.Event(),
        price_publish_release=asyncio.Event(),
    )
    publish_price = _install_early_finalizer_mocks(
        monkeypatch,
        tmp_path,
        state,
        prepared_price,
    )

    pipeline_task = asyncio.create_task(
        shared_snapshot_publish._prepare_price_with_early_finalizer(
            schema_name="mrf",
            manifest_stage_table="manifest_stage",
            price_set_summary_source_count=1,
            raw_work_directory=tmp_path,
            serving_run_entries=(),
            code_dictionary_entries=(),
            provider_set_metadata_entries=(),
            expected_source_identities=(),
            publish_prepared_price=publish_price,
        )
    )
    await asyncio.wait_for(state.finalizer_started.wait(), timeout=0.5)
    assert not state.atom_release.is_set()
    assert not pipeline_task.done()

    state.atom_release.set()
    await asyncio.wait_for(state.price_publish_started.wait(), timeout=0.5)
    assert not state.finalizer_release.is_set()
    state.price_publish_release.set()
    state.finalizer_release.set()
    _assert_early_finalizer_pipeline(
        await pipeline_task,
        state,
        prepared_price,
    )


@pytest.mark.asyncio
async def test_early_finalizer_failure_cleans_successful_price_preparation(
    monkeypatch,
    tmp_path,
):
    prepared_price = object()
    preparation_completed = asyncio.Event()
    cleanup = AsyncMock()

    async def prepare_price(*, price_key_ready, **_kwargs):
        price_key_ready(
            PreparedSharedPriceKeyMap(
                schema_name="mrf",
                price_key_map="price_key_map",
                price_set_count=3,
            )
        )
        preparation_completed.set()
        return prepared_price

    async def run_finalizer(**_kwargs):
        await preparation_completed.wait()
        raise RuntimeError("finalizer failed")

    monkeypatch.setattr(
        shared_snapshot_publish,
        "prepare_shared_price_artifacts",
        prepare_price,
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "export_shared_price_key_map",
        AsyncMock(return_value=tmp_path / "price-key-map.copy"),
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "run_v3_direct_finalizer",
        run_finalizer,
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "cleanup_prepared_shared_price_artifacts",
        cleanup,
    )

    with pytest.raises(RuntimeError, match="finalizer failed"):
        await shared_snapshot_publish._prepare_price_with_early_finalizer(
            schema_name="mrf",
            manifest_stage_table="manifest_stage",
            price_set_summary_source_count=1,
            raw_work_directory=tmp_path,
            serving_run_entries=(),
            code_dictionary_entries=(),
            provider_set_metadata_entries=(),
            expected_source_identities=(),
        )

    cleanup.assert_awaited_once_with(prepared_price)


@pytest.mark.asyncio
async def test_cancellation_before_price_key_readiness_drains_preparation(
    monkeypatch,
    tmp_path,
):
    preparation_started = asyncio.Event()
    preparation_cancelled = asyncio.Event()
    export = AsyncMock()

    async def prepare_price(**_kwargs):
        preparation_started.set()
        try:
            await asyncio.Future()
        finally:
            preparation_cancelled.set()

    monkeypatch.setattr(
        shared_snapshot_publish,
        "prepare_shared_price_artifacts",
        prepare_price,
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "export_shared_price_key_map",
        export,
    )

    pipeline_task = asyncio.create_task(
        shared_snapshot_publish._prepare_price_with_early_finalizer(
            schema_name="mrf",
            manifest_stage_table="manifest_stage",
            price_set_summary_source_count=1,
            raw_work_directory=tmp_path,
            serving_run_entries=(),
            code_dictionary_entries=(),
            provider_set_metadata_entries=(),
            expected_source_identities=(),
        )
    )
    await preparation_started.wait()
    pipeline_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pipeline_task

    assert preparation_cancelled.is_set()
    export.assert_not_awaited()


def test_authoritative_mapping_summary_matches_bounded_lane_metadata():
    lanes = (
        SimpleNamespace(
            object_kinds=("a_kind", "b_kind"),
            mapping_count=3,
            unique_block_count=2,
            logical_byte_count=30,
        ),
        SimpleNamespace(
            object_kinds=("c_kind",),
            mapping_count=2,
            unique_block_count=2,
            logical_byte_count=20,
        ),
    )
    summary = SharedMappingDigestSummary(
        mapping_digest=b"m" * 32,
        mapping_count=5,
        unique_block_count=4,
        entry_count=99,
        logical_byte_count=50,
        canonical_byte_count=400,
        object_kinds=("a_kind", "b_kind", "c_kind"),
    )

    _validate_authoritative_mapping_summary(summary, *lanes)


@pytest.mark.parametrize(
    ("summary_field", "summary_value"),
    [
        ("object_kinds", ("a_kind", "missing_kind")),
        ("mapping_count", 4),
        ("unique_block_count", 3),
        ("logical_byte_count", 49),
    ],
)
def test_authoritative_mapping_summary_rejects_lane_disagreement(
    summary_field,
    summary_value,
):
    summary_values_by_field = {
        "mapping_digest": b"m" * 32,
        "mapping_count": 5,
        "unique_block_count": 4,
        "entry_count": 99,
        "logical_byte_count": 50,
        "canonical_byte_count": 400,
        "object_kinds": ("a_kind", "b_kind", "c_kind"),
    }
    summary_values_by_field[summary_field] = summary_value
    summary = SharedMappingDigestSummary(**summary_values_by_field)
    lanes = (
        SimpleNamespace(
            object_kinds=("a_kind", "b_kind"),
            mapping_count=3,
            unique_block_count=2,
            logical_byte_count=30,
        ),
        SimpleNamespace(
            object_kinds=("c_kind",),
            mapping_count=2,
            unique_block_count=2,
            logical_byte_count=20,
        ),
    )

    with pytest.raises(RuntimeError, match=summary_field):
        _validate_authoritative_mapping_summary(summary, *lanes)


def test_authoritative_mapping_summary_rejects_overlapping_lane_kinds():
    summary = SharedMappingDigestSummary(
        mapping_digest=b"m" * 32,
        mapping_count=2,
        unique_block_count=2,
        entry_count=2,
        logical_byte_count=2,
        canonical_byte_count=100,
        object_kinds=("a_kind",),
    )
    lane = SimpleNamespace(
        object_kinds=("a_kind",),
        mapping_count=1,
        unique_block_count=1,
        logical_byte_count=1,
    )

    with pytest.raises(RuntimeError, match="overlap object kinds"):
        _validate_authoritative_mapping_summary(summary, lane, lane)


def _install_cleanup_cancellation_mocks(
    monkeypatch,
    transaction,
    prepare_price,
    publish_prepared,
    cleanup_prepared,
):
    prepare = AsyncMock(side_effect=prepare_price)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.setattr(shared_snapshot_publish.db, "transaction", transaction)
    monkeypatch.setattr(
        shared_snapshot_publish,
        "touch_shared_layout_build",
        AsyncMock(),
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "prepare_shared_price_artifacts",
        prepare,
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "export_shared_price_key_map",
        AsyncMock(return_value="price-key-map.copy"),
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "run_v3_direct_finalizer",
        AsyncMock(return_value={"blocks": {}}),
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "publish_shared_price_artifacts",
        AsyncMock(return_value=object()),
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "_publish_prepared_shared_layout",
        publish_prepared,
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "cleanup_prepared_shared_price_artifacts",
        cleanup_prepared,
    )
    return prepare


def _strict_shared_layout_arguments():
    return {
        "schema_name": "mrf",
        "manifest_stage_table": "manifest_stage",
        "reserved_snapshot_key": 7,
        "build_token": "build-token",
        "expected_coverage_scope_id": b"c" * 32,
        "logical_snapshot_id": "snapshot-id",
        "expected_source_identities": (),
        "serving_run_entries": (),
        "code_dictionary_entries": (),
        "provider_set_metadata_entries": (),
        "source_audit_witness_entries": (),
        "expected_raw_source_sha256": (),
        "graph_artifact_entries": (),
        "provider_identifier_quarantine": {},
    }


async def _cancel_publisher_after_cleanup_starts(
    publish_task,
    state,
    cancel_during_publication,
):
    await asyncio.wait_for(state.publication_started.wait(), timeout=0.5)
    if cancel_during_publication:
        publish_task.cancel()
    await asyncio.wait_for(state.cleanup_started.wait(), timeout=0.5)
    publish_task.cancel()
    await asyncio.sleep(0)
    publish_task.cancel()
    await asyncio.sleep(0)
    assert not publish_task.done()
    state.cleanup_release.set()
    with pytest.raises(asyncio.CancelledError):
        await publish_task


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_during_publication", [False, True])
async def test_prepared_price_cleanup_survives_repeated_cancellation_on_every_exit(
    monkeypatch,
    cancel_during_publication,
):
    """Finish prepared-price cleanup across every cancellation exit path."""

    prepared_price = SimpleNamespace(price_set_count=3)
    state = SimpleNamespace(
        publication_started=asyncio.Event(),
        cleanup_started=asyncio.Event(),
        cleanup_release=asyncio.Event(),
        cleanup_finished=asyncio.Event(),
    )

    @asynccontextmanager
    async def transaction():
        yield object()

    async def publish_prepared(**_kwargs):
        state.publication_started.set()
        if cancel_during_publication:
            await asyncio.Future()
        return object()

    async def cleanup_prepared(observed_prepared):
        assert observed_prepared is prepared_price
        state.cleanup_started.set()
        await state.cleanup_release.wait()
        state.cleanup_finished.set()

    async def prepare_price(**kwargs):
        kwargs["price_key_ready"](
            PreparedSharedPriceKeyMap(
                schema_name="mrf",
                price_key_map="price_key_map",
                price_set_count=3,
            )
        )
        return prepared_price

    prepare = _install_cleanup_cancellation_mocks(
        monkeypatch,
        transaction,
        prepare_price,
        publish_prepared,
        cleanup_prepared,
    )

    publish_task = asyncio.create_task(
        shared_snapshot_publish.publish_strict_shared_v3_layout(
            **_strict_shared_layout_arguments()
        )
    )
    await _cancel_publisher_after_cleanup_starts(
        publish_task,
        state,
        cancel_during_publication,
    )
    assert state.cleanup_finished.is_set()
    assert prepare.await_args.kwargs["price_set_summary_source_count"] is None
