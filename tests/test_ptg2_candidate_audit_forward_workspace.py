# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import gc
import tracemalloc
from unittest.mock import AsyncMock, Mock

import pytest

from api import ptg2_db_sidecars as sidecars
from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
    CandidateAuditDecodedRetentionError,
)


class _AuditAbort(BaseException):
    pass


def _exact_options():
    return sidecars._ForwardBatchOptions(
        shared_snapshot_key=1,
        source_count=1,
        price_dictionary_item_count=8,
        price_dictionary_block_bytes=64,
        provider_set_keys_by_code={7: (5,)},
        source_keys_by_code={7: (0,)},
        occurrence_keys=frozenset(((7, 5, 0),)),
    )


def _exact_workspace_bytes():
    return (
        sidecars._FORWARD_REQUEST_BASE_RETAINED_BYTES
        + sidecars._FORWARD_REQUEST_CODE_RETAINED_BYTES
        + 2 * sidecars._FORWARD_FILTER_MAP_ENTRY_RETAINED_BYTES
        + 2 * sidecars._FORWARD_PROVIDER_FILTER_COPY_RETAINED_BYTES
        + sidecars._FORWARD_SOURCE_FILTER_COPY_RETAINED_BYTES
        + sidecars._FORWARD_OCCURRENCE_WORKSPACE_RETAINED_BYTES
    )


@pytest.mark.asyncio
async def test_exact_forward_workspace_claims_before_normalization(monkeypatch):
    visit = AsyncMock()
    monkeypatch.setattr(
        sidecars,
        "_visit_forward_batch_keys_with_retention",
        visit,
    )
    expected_bytes = _exact_workspace_bytes()
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=expected_bytes)

    await sidecars._visit_forward_batch_keys(
        object(),
        (7,),
        _exact_options(),
        Mock(),
        retention_budget=budget,
    )

    visit.assert_awaited_once()
    assert budget.peak_retained_bytes == expected_bytes
    assert budget.retained_bytes == 0


@pytest.mark.asyncio
async def test_exact_forward_workspace_one_under_fails_before_visit(monkeypatch):
    visit = AsyncMock()
    monkeypatch.setattr(
        sidecars,
        "_visit_forward_batch_keys_with_retention",
        visit,
    )
    budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=_exact_workspace_bytes() - 1
    )

    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="occurrence-filter workspace",
    ):
        await sidecars._visit_forward_batch_keys(
            object(),
            (7,),
            _exact_options(),
            Mock(),
            retention_budget=budget,
        )

    visit.assert_not_awaited()
    assert budget.retained_bytes == 0


@pytest.mark.asyncio
async def test_reverse_source_workspace_is_claimed_and_released(monkeypatch):
    visit = AsyncMock()
    monkeypatch.setattr(
        sidecars,
        "_visit_forward_batch_keys_with_retention",
        visit,
    )
    expected_bytes = (
        sidecars._FORWARD_REQUEST_BASE_RETAINED_BYTES
        + sidecars._FORWARD_REQUEST_CODE_RETAINED_BYTES
        + sidecars._FORWARD_FILTER_MAP_ENTRY_RETAINED_BYTES
        + sidecars._FORWARD_SOURCE_FILTER_COPY_RETAINED_BYTES
    )
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=expected_bytes)
    options = sidecars._ForwardBatchOptions(
        shared_snapshot_key=1,
        source_count=1,
        price_dictionary_item_count=8,
        price_dictionary_block_bytes=64,
        source_keys_by_code={7: (0,)},
    )

    await sidecars._visit_forward_batch_keys(
        object(),
        (7,),
        options,
        Mock(),
        retention_budget=budget,
    )

    assert budget.peak_retained_bytes == expected_bytes
    assert budget.retained_bytes == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("filter_options", "filter_entry_count"),
    (
        ({"provider_set_keys_by_code": {7: (), 8: (), 9: ()}}, 3),
        ({"source_keys_by_code": {7: (0,), 8: (0,), 9: (0,)}}, 3),
    ),
)
async def test_forward_filter_maps_fail_before_normalization(
    monkeypatch,
    filter_options,
    filter_entry_count,
):
    visit = AsyncMock()
    monkeypatch.setattr(
        sidecars,
        "_visit_forward_batch_keys_with_retention",
        visit,
    )
    claimed_before_map = (
        sidecars._FORWARD_REQUEST_BASE_RETAINED_BYTES
        + sidecars._FORWARD_REQUEST_CODE_RETAINED_BYTES
    )
    budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=(
            claimed_before_map
            + filter_entry_count * sidecars._FORWARD_FILTER_MAP_ENTRY_RETAINED_BYTES
            - 1
        )
    )
    options = sidecars._ForwardBatchOptions(
        shared_snapshot_key=1,
        source_count=1,
        price_dictionary_item_count=8,
        price_dictionary_block_bytes=64,
        **filter_options,
    )

    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="filter-map entries",
    ):
        await sidecars._visit_forward_batch_keys(
            object(),
            (7,),
            options,
            Mock(),
            retention_budget=budget,
        )

    visit.assert_not_awaited()
    assert budget.retained_bytes == 0


@pytest.mark.asyncio
async def test_forward_workspace_cancellation_preserves_sentinel(monkeypatch):
    async def cancel_visit(*_args):
        raise asyncio.CancelledError()

    monkeypatch.setattr(
        sidecars,
        "_visit_forward_batch_keys_with_retention",
        cancel_visit,
    )
    sentinel_bytes = 13
    budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=_exact_workspace_bytes() + sentinel_bytes
    )
    budget.claim(sentinel_bytes, category="a caller sentinel")

    with pytest.raises(asyncio.CancelledError):
        await sidecars._visit_forward_batch_keys(
            object(),
            (7,),
            _exact_options(),
            Mock(),
            retention_budget=budget,
        )

    assert budget.retained_bytes == sentinel_bytes


def _fragment_workspace_context(monkeypatch, maximum_bytes):
    fragment_count = 2
    expected_bytes = (
        fragment_count * sidecars._FORWARD_FRAGMENT_WORKSPACE_RETAINED_BYTES
    )
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=maximum_bytes)
    retention = sidecars._ForwardTemporaryRetention(budget)
    block_key = 7 * sidecars._SERVING_BINARY_BY_CODE_BLOCK_SPAN
    monkeypatch.setattr(
        sidecars,
        "_forward_batch_shards_and_filters",
        AsyncMock(return_value=({7: (block_key,)}, {7: (5,)}, False)),
    )
    monkeypatch.setattr(
        sidecars,
        "_shared_serving_binary_payload_rows_for_keys",
        AsyncMock(return_value=[{"block_key": block_key}] * fragment_count),
    )
    group_fragments = Mock(return_value={7: ()})
    build_views = Mock(return_value=())
    monkeypatch.setattr(
        sidecars,
        "_group_forward_fragments_by_code",
        group_fragments,
    )
    monkeypatch.setattr(
        sidecars,
        "_forward_batch_fragment_views",
        build_views,
    )
    return expected_bytes, budget, retention, group_fragments, build_views


async def _fetch_fragment_workspace_views(retention):
    return await sidecars._fetch_forward_batch_fragment_views(
        object(),
        (7,),
        _exact_options(),
        {7: frozenset((0,))},
        {7: frozenset(((5, 0),))},
        retention,
    )


@pytest.mark.asyncio
async def test_fragment_workspace_claim_precedes_view_build(monkeypatch):
    expected_bytes = 2 * sidecars._FORWARD_FRAGMENT_WORKSPACE_RETAINED_BYTES
    context = _fragment_workspace_context(monkeypatch, expected_bytes)
    _, budget, retention, _group_fragments, build_views = context

    assert await _fetch_fragment_workspace_views(retention) == ()

    build_views.assert_called_once()
    assert budget.retained_bytes == expected_bytes
    retention.release()
    assert budget.retained_bytes == 0


@pytest.mark.asyncio
async def test_fragment_workspace_one_under_skips_view_build(monkeypatch):
    expected_bytes = 2 * sidecars._FORWARD_FRAGMENT_WORKSPACE_RETAINED_BYTES
    context = _fragment_workspace_context(monkeypatch, expected_bytes - 1)
    _, budget, retention, group_fragments, _build_views = context

    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="fragment-view workspace",
    ):
        await _fetch_fragment_workspace_views(retention)

    group_fragments.assert_not_called()
    assert budget.retained_bytes == 0


@pytest.mark.asyncio
async def test_partial_forward_result_cleanup_preserves_sentinel(monkeypatch):
    async def abort_visit(
        _session,
        _code_keys,
        _options,
        occurrence_consumer,
        **_kwargs,
    ):
        occurrence_consumer(7, 5, 8, 0)
        raise _AuditAbort("stop")

    monkeypatch.setattr(sidecars, "_visit_forward_batch_keys", abort_visit)
    sentinel_bytes = 17
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=64 * 1024)
    budget.claim(sentinel_bytes, category="a caller sentinel")

    with pytest.raises(_AuditAbort, match="stop"):
        await sidecars.lookup_forward_price_index_from_db(
            object(),
            (7,),
            retention_budget=budget,
            shared_snapshot_key=1,
            source_count=1,
            price_dictionary_item_count=8,
            price_dictionary_block_bytes=64,
        )

    assert budget.retained_bytes == sentinel_bytes


class _LazyRows:
    def __init__(self, rows):
        self.rows = iter(rows)
        self.consumed_count = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            row = next(self.rows)
        except StopIteration:
            raise StopAsyncIteration
        self.consumed_count += 1
        return row


class _StreamingShardSession:
    def __init__(self, rows):
        self.result = _LazyRows(rows)
        self.stream_calls = 0
        self.execute_calls = 0

    async def stream(self, _statement, _params):
        self.stream_calls += 1
        return self.result

    async def execute(self, _statement, _params):
        self.execute_calls += 1
        raise AssertionError("budgeted shard discovery must stream")


@pytest.mark.asyncio
async def test_shard_discovery_stops_at_first_unclaimable_streamed_row():
    block_base = 7 * sidecars._SERVING_BINARY_BY_CODE_BLOCK_SPAN
    session = _StreamingShardSession(
        (
            {"code_key": 7, "block_key": block_base},
            {"code_key": 7, "block_key": block_base + 1},
            {"code_key": 7, "block_key": block_base + 2},
        )
    )
    budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=sidecars._FORWARD_DISCOVERED_SHARD_RETAINED_BYTES
    )
    retention = sidecars._ForwardTemporaryRetention(budget)

    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="discovered forward shard",
    ):
        await sidecars._discover_forward_shard_keys(
            session,
            shared_snapshot_key=1,
            schema_name="mrf",
            code_keys=(7,),
            temporary_retention=retention,
        )

    assert session.result.consumed_count == 2
    assert session.stream_calls == 1
    assert session.execute_calls == 0
    retention.release()
    assert budget.retained_bytes == 0


def test_occurrence_workspace_exceeds_cpython_retained_peak():
    occurrence_count = 10_000
    raw_occurrences = tuple(
        (7, 1_000_000 + index, index % 16) for index in range(occurrence_count)
    )
    gc.collect()
    tracemalloc.start()
    try:
        before_bytes, _before_peak = tracemalloc.get_traced_memory()
        normalized_occurrences = frozenset(
            (provider_set_key, source_key)
            for _code_key, provider_set_key, source_key in raw_occurrences
        )
        block_filter = frozenset(normalized_occurrences)
        exact_views_by_occurrence = {occurrence: [None] for occurrence in block_filter}
        provider_filter = frozenset(
            provider_set_key for provider_set_key, _source_key in normalized_occurrences
        )
        retained_bytes, peak_bytes = tracemalloc.get_traced_memory()
        assert exact_views_by_occurrence and provider_filter
        observed_peak = max(retained_bytes, peak_bytes) - before_bytes
        assert observed_peak <= (
            occurrence_count * sidecars._FORWARD_OCCURRENCE_WORKSPACE_RETAINED_BYTES
        )
    finally:
        tracemalloc.stop()
