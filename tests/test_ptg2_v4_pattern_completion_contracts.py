# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed contracts for selected-pattern completion."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from api.ptg2_shared_blocks import PTG2SharedBlockError
from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2ManifestArtifactError,
)
from tests.test_ptg2_v4_filtered_reverse_serving import _tables


def _provider_set_id(provider_set_key: int) -> str:
    return f"{provider_set_key:032x}"


def _completion_request(
    *,
    prefix_rows=(),
    candidate_provider_set_keys=(1,),
    is_source_exhausted=False,
    maximum_occurrences=4,
    maximum_code_sets=4,
):
    return serving._V4PatternCompletionRequest(
        code_rows=[{"code_key": 7, "rate_count": maximum_occurrences}],
        prefix_rows=list(prefix_rows),
        candidate_provider_set_keys=tuple(candidate_provider_set_keys),
        source_trace_set_hash=None,
        network_names=[],
        descending=False,
        is_source_exhausted=is_source_exhausted,
        maximum_occurrences=maximum_occurrences,
        maximum_code_sets=maximum_code_sets,
        scan_budget=serving.ForwardReadBudget(
            8,
            16_384,
            maximum_row_capacity=max(maximum_occurrences + 1, 1),
        ),
    )


@pytest.mark.asyncio
async def test_completion_projection_rejects_selected_npi_without_pattern(
    monkeypatch,
):
    relation_lookup = AsyncMock()
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_members",
        relation_lookup,
    )

    with pytest.raises(PTG2ManifestArtifactError, match="lost its pattern"):
        await serving._v4_pattern_completion_projection(
            object(),
            snapshot_key=1,
            selected_npi_keys=(9,),
            npi_keys_by_pattern={7: (1,)},
            max_members=8,
        )

    relation_lookup.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("message", "expected_exception", "expected_dimension"),
    (
        (
            "PTG V4 graph selection exceeds max_members",
            serving.PTG2OnlineWorkBudgetExceeded,
            "retained_memberships",
        ),
        ("authenticated graph payload is invalid", PTG2SharedBlockError, None),
    ),
)
async def test_completion_projection_preserves_graph_error_boundary(
    monkeypatch,
    message,
    expected_exception,
    expected_dimension,
):
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_members",
        AsyncMock(side_effect=PTG2SharedBlockError(message)),
    )

    with pytest.raises(expected_exception) as exc_info:
        await serving._v4_pattern_completion_projection(
            object(),
            snapshot_key=1,
            selected_npi_keys=(1,),
            npi_keys_by_pattern={7: (1,)},
            max_members=8,
        )

    if expected_dimension is not None:
        assert exc_info.value.dimension == expected_dimension


@pytest.mark.asyncio
async def test_completion_projection_requires_every_selected_pattern(
    monkeypatch,
):
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_members",
        AsyncMock(return_value={7: (3,)}),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="projection is incomplete"):
        await serving._v4_pattern_completion_projection(
            object(),
            snapshot_key=1,
            selected_npi_keys=(1, 2),
            npi_keys_by_pattern={7: (1,), 8: (2,)},
            max_members=8,
        )


@pytest.mark.asyncio
async def test_completion_projection_unions_patterns_by_provider_set(
    monkeypatch,
):
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_members",
        AsyncMock(return_value={7: (3, 4), 8: (3,)}),
    )

    provider_set_keys, pattern_keys_by_set = (
        await serving._v4_pattern_completion_projection(
            object(),
            snapshot_key=1,
            selected_npi_keys=(1, 2),
            npi_keys_by_pattern={8: (2,), 7: (1,)},
            max_members=8,
        )
    )

    assert provider_set_keys == (3, 4)
    assert pattern_keys_by_set == {3: (7, 8), 4: (7,)}


@pytest.mark.asyncio
async def test_completion_rate_rejects_prefix_above_occurrence_cap(monkeypatch):
    merge_rows = AsyncMock()
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        merge_rows,
    )
    request = _completion_request(
        prefix_rows=(
            {"_ptg_provider_set_key": 1},
            {"_ptg_provider_set_key": 1},
        ),
        maximum_occurrences=1,
    )

    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded) as exc_info:
        await serving._v4_pattern_completion_rate_rows(
            object(),
            _tables(None),
            request,
        )

    assert exc_info.value.dimension == "code_occurrences"
    merge_rows.assert_not_awaited()


@pytest.mark.asyncio
async def test_completion_rate_requires_available_forward_rows(monkeypatch):
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        AsyncMock(return_value=None),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="rows are unavailable"):
        await serving._v4_pattern_completion_rate_rows(
            object(),
            _tables(None),
            _completion_request(),
        )


@pytest.mark.asyncio
async def test_completion_rows_enforce_combined_occurrence_cap(monkeypatch):
    monkeypatch.setattr(
        serving,
        "_v4_pattern_completion_rate_rows",
        AsyncMock(
            return_value=[
                {"_ptg_provider_set_key": 1},
                {"_ptg_provider_set_key": 1},
            ]
        ),
    )
    request = _completion_request(
        prefix_rows=({"_ptg_provider_set_key": 1},),
        maximum_occurrences=2,
    )

    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded) as exc_info:
        await serving._v4_pattern_completion_rows(
            object(),
            _tables(None),
            request,
        )

    assert exc_info.value.dimension == "code_occurrences"


@pytest.mark.asyncio
async def test_completion_rows_enforce_prefix_and_result_set_union_cap(
    monkeypatch,
):
    completion_row_by_field = {
        "_ptg_provider_set_key": 2,
        "provider_set_global_id_128": _provider_set_id(2),
    }
    monkeypatch.setattr(
        serving,
        "_v4_pattern_completion_rate_rows",
        AsyncMock(return_value=[completion_row_by_field]),
    )
    request = _completion_request(
        prefix_rows=({"_ptg_provider_set_key": 1},),
        candidate_provider_set_keys=(2,),
        maximum_occurrences=4,
        maximum_code_sets=1,
    )

    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded) as exc_info:
        await serving._v4_pattern_completion_rows(
            object(),
            _tables(None),
            request,
        )

    assert exc_info.value.dimension == "code_sets"
