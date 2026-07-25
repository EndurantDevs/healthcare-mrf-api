# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Peak row and page admission for shared manifest code merging."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2ManifestArtifactError,
)
from tests.test_ptg2_v4_filtered_reverse_serving import _tables


def test_page_scan_charge_covers_strict_page_encoder_maximum():
    page_rows = serving.PTG2_SERVING_BINARY_V3_PAGE_ROWS
    strict_header_bytes = 1 + 5 + 1 + 1
    strict_source_vector_bytes = (page_rows * 31 + 7) // 8
    strict_code_page_bytes = (
        strict_header_bytes
        + page_rows * (5 + 5 + 5)
        + strict_source_vector_bytes
    )
    strict_provider_page_bytes = (
        strict_header_bytes
        + (1 + 5 + 10 + 1)
        + page_rows * (5 + 5)
        + strict_source_vector_bytes
    )

    assert serving._PTG2_PAGE_SCAN_MAX_RAW_BYTES >= max(
        strict_code_page_bytes,
        strict_provider_page_bytes,
    )


@pytest.mark.asyncio
async def test_multi_code_fast_pages_share_global_row_capacity(
    monkeypatch,
):
    async def page_rows(*_args, code_metadata, **_kwargs):
        code_key = int(code_metadata["code_key"])
        return [
            {"price_key": code_key - 6, "_ptg_provider_set_key": code_key},
            {"price_key": code_key - 3, "_ptg_provider_set_key": code_key},
        ]

    page_lookup = AsyncMock(side_effect=page_rows)
    monkeypatch.setattr(
        serving,
        "_version_three_forward_page_rows",
        page_lookup,
    )
    scan_budget = serving.ForwardReadBudget(
        8,
        16_384,
        maximum_row_capacity=4,
    )
    merged_rows = await serving._merge_manifest_code_variant_rows(
        object(),
        _tables(None),
        code_rows=[
            {"code_key": 7, "rate_count": 2},
            {"code_key": 8, "rate_count": 2},
        ],
        provider_set_keys=None,
        source_trace_set_hash=None,
        network_names=[],
        limit=2,
        offset=0,
        scan_budget=scan_budget,
    )

    assert [merged_row["price_key"] for merged_row in merged_rows] == [1, 2]
    assert page_lookup.await_count == 2
    assert scan_budget.peak_read_row_capacity == 4
    assert scan_budget.peak_result_row_capacity == 4
    assert scan_budget.active_read_row_capacity == 0
    assert scan_budget.active_result_row_capacity == 0
    assert scan_budget.fragment_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("code_rows", "maximum_fragments"),
    (
        (
            (
                {"code_key": 7, "rate_count": 1},
                {"code_key": 8, "rate_count": 1},
                {"code_key": 9, "rate_count": 1},
            ),
            2,
        ),
        (
            (
                {"code_key": 7, "rate_count": 2},
                {"code_key": 7, "rate_count": 2},
                {"code_key": 7, "rate_count": 2},
            ),
            8,
        ),
    ),
)
async def test_code_multiplicity_fails_before_fast_page_io(
    monkeypatch,
    code_rows,
    maximum_fragments,
):
    page_lookup = AsyncMock()
    monkeypatch.setattr(
        serving,
        "_version_three_forward_page_rows",
        page_lookup,
    )

    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded) as exc_info:
        await serving._merge_manifest_code_variant_rows(
            object(),
            _tables(None),
            code_rows=list(code_rows),
            provider_set_keys=None,
            source_trace_set_hash=None,
            network_names=[],
            limit=2,
            offset=0,
            scan_budget=serving.ForwardReadBudget(
                maximum_fragments,
                16_384,
                maximum_row_capacity=4,
            ),
        )

    assert exc_info.value.dimension == "forward_scan"
    page_lookup.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rate_counts",
    ((None,), (True,), (-1,), ("not-an-integer",), (2, 3)),
)
async def test_declared_rate_count_fails_before_fast_page_io(
    monkeypatch,
    rate_counts,
):
    page_lookup = AsyncMock()
    monkeypatch.setattr(
        serving,
        "_version_three_forward_page_rows",
        page_lookup,
    )
    code_rows = [
        {"code_key": 7, "rate_count": rate_count}
        for rate_count in rate_counts
    ]

    with pytest.raises(PTG2ManifestArtifactError, match="rate count"):
        await serving._merge_manifest_code_variant_rows(
            object(),
            _tables(None),
            code_rows=code_rows,
            provider_set_keys=None,
            source_trace_set_hash=None,
            network_names=[],
            limit=2,
            offset=0,
            scan_budget=serving.ForwardReadBudget(
                8,
                16_384,
                maximum_row_capacity=8,
            ),
        )

    page_lookup.assert_not_awaited()


def test_row_budget_requires_bounded_manifest_read():
    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded) as exc_info:
        serving._reserve_manifest_code_merge_capacity(
            serving.ForwardReadBudget(
                8,
                16_384,
                maximum_row_capacity=8,
            ),
            {7: ({"code_key": 7, "rate_count": 1},)},
            per_code_limit=None,
            retained_row_count=0,
        )

    assert exc_info.value.dimension == "forward_scan"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("code_rows", "limit"),
    (
        (({"code_key": 7, "rate_count": 1},), 0),
        (({"rate_count": 1},), 1),
    ),
)
async def test_empty_manifest_windows_return_before_code_io(
    monkeypatch,
    code_rows,
    limit,
):
    code_reader = AsyncMock()
    monkeypatch.setattr(
        serving,
        "_read_manifest_code_merge",
        code_reader,
    )

    merged_rows = await serving._merge_manifest_code_variant_rows(
        object(),
        _tables(None),
        code_rows=list(code_rows),
        provider_set_keys=None,
        source_trace_set_hash=None,
        network_names=[],
        limit=limit,
        offset=0,
        scan_budget=serving.ForwardReadBudget(
            8,
            16_384,
            maximum_row_capacity=8,
        ),
    )

    assert merged_rows == []
    code_reader.assert_not_awaited()


@pytest.mark.asyncio
async def test_unavailable_manifest_rows_release_reserved_capacity(monkeypatch):
    monkeypatch.setattr(
        serving,
        "_read_manifest_code_merge",
        AsyncMock(return_value=None),
    )
    scan_budget = serving.ForwardReadBudget(
        8,
        16_384,
        maximum_row_capacity=8,
    )

    merged_rows = await serving._merge_manifest_code_variant_rows(
        object(),
        _tables(None),
        code_rows=[{"code_key": 7, "rate_count": 1}],
        provider_set_keys=None,
        source_trace_set_hash=None,
        network_names=[],
        limit=1,
        offset=0,
        scan_budget=scan_budget,
    )

    assert merged_rows is None
    assert scan_budget.active_read_row_capacity == 0
    assert scan_budget.active_result_row_capacity == 0


@pytest.mark.asyncio
async def test_forward_selection_distinguishes_full_and_empty_reads(
    monkeypatch,
):
    full_lookup = AsyncMock(return_value=("full",))
    prefix_lookup = AsyncMock()
    monkeypatch.setattr(serving, "_lookup_shared_forward_rows", full_lookup)
    monkeypatch.setattr(
        serving,
        "_lookup_shared_forward_prefix_rows",
        prefix_lookup,
    )
    scan_budget = serving.ForwardReadBudget(8, 16_384)
    full_selection = serving._SharedForwardSelection(
        provider_set_keys=(3,),
        provider_counts_by_key={3: 1},
        limit=None,
        offset=0,
        descending=False,
        scan_budget=scan_budget,
    )
    empty_selection = serving._SharedForwardSelection(
        provider_set_keys=(3,),
        provider_counts_by_key={3: 1},
        limit=0,
        offset=0,
        descending=False,
        scan_budget=scan_budget,
    )

    assert await serving._selected_shared_forward_rows(
        object(),
        _tables(None),
        7,
        full_selection,
    ) == ("full",)
    assert await serving._selected_shared_forward_rows(
        object(),
        _tables(None),
        7,
        empty_selection,
    ) == ()
    assert full_lookup.await_args.kwargs == {
        "provider_set_keys": (3,),
        "provider_counts_by_key": {3: 1},
        "scan_budget": scan_budget,
    }
    prefix_lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_rows_cannot_exceed_declared_rate_count(monkeypatch):
    page_lookup = AsyncMock(
        return_value=[
            {"price_key": 1, "_ptg_provider_set_key": 7},
            {"price_key": 2, "_ptg_provider_set_key": 7},
        ]
    )
    monkeypatch.setattr(
        serving,
        "_version_three_forward_page_rows",
        page_lookup,
    )

    with pytest.raises(
        PTG2ManifestArtifactError,
        match="exceeds its declared rate count",
    ):
        await serving._merge_manifest_code_variant_rows(
            object(),
            _tables(None),
            code_rows=[{"code_key": 7, "rate_count": 1}],
            provider_set_keys=None,
            source_trace_set_hash=None,
            network_names=[],
            limit=2,
            offset=0,
            scan_budget=serving.ForwardReadBudget(
                8,
                16_384,
                maximum_row_capacity=8,
            ),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("maximum_row_capacity", "should_succeed"),
    ((144, True), (143, False)),
)
async def test_completion_peak_includes_retained_prefix_across_codes(
    monkeypatch,
    maximum_row_capacity,
    should_succeed,
):
    read_groups = AsyncMock(
        return_value=[
            {"price_key": price_key, "_ptg_provider_set_key": price_key}
            for price_key in range(80)
        ]
    )
    monkeypatch.setattr(
        serving,
        "_read_manifest_code_groups",
        read_groups,
    )
    monkeypatch.setattr(
        serving,
        "_version_three_provider_pages_for_keys",
        AsyncMock(return_value=None),
    )
    scan_budget = serving.ForwardReadBudget(
        8,
        16_384,
        maximum_row_capacity=maximum_row_capacity,
    )
    merge_call = serving._merge_manifest_code_variant_rows(
        object(),
        _tables(None),
        code_rows=[
            {"code_key": 7, "rate_count": 40},
            {"code_key": 8, "rate_count": 40},
        ],
        provider_set_keys=(1,),
        source_trace_set_hash=None,
        network_names=[],
        limit=40,
        offset=0,
        scan_budget=scan_budget,
        retained_row_count=64,
    )

    if should_succeed:
        assert len(await merge_call) == 40
        assert scan_budget.peak_read_row_capacity == 144
        assert scan_budget.peak_result_row_capacity == 144
        assert scan_budget.active_read_row_capacity == 0
        assert scan_budget.active_result_row_capacity == 0
    else:
        with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded) as exc_info:
            await merge_call
        assert exc_info.value.dimension == "forward_scan"
        read_groups.assert_not_awaited()
