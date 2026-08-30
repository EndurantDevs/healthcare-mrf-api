# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Boundary behavior for bounded local-first aggregate geo pricing."""

from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from api.ptg2_v4_graph import V4GraphRoot
from tests.test_ptg2_geo_rate_prefix import _code_rows, _production_tables


@pytest.mark.asyncio
async def test_oversized_geo_rate_fails_after_one_location_bound(monkeypatch):
    location_reads = AsyncMock(
        return_value=[
            {"npi": 111, "_ptg_source_exhausted": False},
            {"npi": 222, "_ptg_source_exhausted": False},
        ]
    )
    graph_reads = AsyncMock()
    monkeypatch.setattr(serving, "_ptg2_manifest_location_match_limit", lambda: 2)
    monkeypatch.setattr(serving, "_membership_npi_rows", location_reads)
    monkeypatch.setattr(serving, "_v4_sets_by_npi", graph_reads)

    with pytest.raises(
        serving.PTG2LocationScopeError,
        match="Narrow the ZIP radius",
    ):
        await serving._select_geo_filtered_rate_prefix(
            object(),
            _production_tables(),
            code_rows=_code_rows(257),
            args={"zip5": "60611", "zip_radius_miles": 25},
            network_names=[],
            target_count=11,
            descending=False,
        )

    location_reads.assert_awaited_once()
    assert location_reads.await_args.kwargs["limit"] == 2
    graph_reads.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("reverse_scope", "expected", "error_type"),
    (
        (None, None, None),
        (((), True, 1), (), None),
        (((), False, 1), None, serving.PTG2LocationScopeError),
        (((111,), False, 1), None, serving.PTG2LocationScopeError),
    ),
)
async def test_oversized_geo_rate_handles_bounded_reverse_scope_outcomes(
    monkeypatch,
    reverse_scope,
    expected,
    error_type,
):
    graph_lookup = AsyncMock()
    monkeypatch.setattr(
        serving,
        "_cached_reverse_geo_scope",
        AsyncMock(return_value=reverse_scope),
    )
    monkeypatch.setattr(serving, "_v4_sets_by_npi", graph_lookup)
    selection_call = serving._oversized_geo_local_provider_sets(
        object(),
        _production_tables(),
        {"zip5": "48201"},
        serving._geo_rate_selection_budget(_production_tables()),
        serving._v4_geo_rate_forward_limits(_production_tables()),
    )

    if error_type is not None:
        with pytest.raises(error_type) as exc_info:
            await selection_call
        assert exc_info.value.error_code == "ptg2_location_scope_too_broad"
    else:
        assert await selection_call == expected
    graph_lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_oversized_geo_rate_preserves_unrelated_graph_errors(monkeypatch):
    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        AsyncMock(return_value=V4GraphRoot(17, "direct_v1", b"r" * 32)),
    )
    monkeypatch.setattr(
        serving,
        "_cached_reverse_geo_scope",
        AsyncMock(return_value=((111,), True, 1)),
    )
    monkeypatch.setattr(
        serving,
        "_v4_sets_by_npi",
        AsyncMock(side_effect=serving.PTG2SharedBlockError("corrupt provider graph")),
    )

    with pytest.raises(serving.PTG2SharedBlockError, match="corrupt provider graph"):
        await serving._oversized_geo_local_provider_sets(
            object(),
            _production_tables(),
            {"zip5": "48201"},
            serving._geo_rate_selection_budget(_production_tables()),
            serving._v4_geo_rate_forward_limits(_production_tables()),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("message", "error_type"),
    (
        (
            "provider-code intersections exceed their retention limit",
            serving.PTG2OnlineWorkBudgetExceeded,
        ),
        ("corrupt provider-code block", serving.PTG2ManifestArtifactError),
    ),
)
async def test_oversized_geo_rate_maps_only_provider_code_retention_errors(
    monkeypatch,
    message,
    error_type,
):
    monkeypatch.setattr(
        serving,
        "lookup_shared_provider_code_intersections_from_db",
        AsyncMock(side_effect=serving.PTG2ManifestArtifactError(message)),
    )

    with pytest.raises(error_type) as exc_info:
        await serving._oversized_geo_code_provider_sets(
            object(),
            _production_tables(),
            (1,),
            (1,),
            serving._geo_rate_selection_budget(_production_tables()),
            65_536,
            6_600,
        )
    if error_type is serving.PTG2OnlineWorkBudgetExceeded:
        assert exc_info.value.dimension == "candidate_members"
    else:
        assert str(exc_info.value) == message


@pytest.mark.asyncio
async def test_oversized_geo_rate_rejects_incomplete_provider_code_artifact(
    monkeypatch,
):
    monkeypatch.setattr(
        serving,
        "lookup_shared_provider_code_intersections_from_db",
        AsyncMock(return_value={1: (1,)}),
    )

    with pytest.raises(
        serving.PTG2ManifestArtifactError,
        match="missing a local provider set",
    ):
        await serving._oversized_geo_code_provider_sets(
            object(),
            _production_tables(),
            (1,),
            (1, 2),
            serving._geo_rate_selection_budget(_production_tables()),
            65_536,
            6_600,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("local_sets", "code_sets", "expected"),
    (
        (None, None, None),
        ((), None, serving._GeoRateSelection((), True)),
        ((1,), (), serving._GeoRateSelection((), True)),
    ),
)
async def test_oversized_geo_rate_returns_empty_bounded_scopes(
    monkeypatch,
    local_sets,
    code_sets,
    expected,
):
    code_lookup = AsyncMock(return_value=code_sets)
    rate_reader = AsyncMock(side_effect=AssertionError("empty scope read rates"))
    monkeypatch.setattr(
        serving,
        "_oversized_geo_local_provider_sets",
        AsyncMock(return_value=local_sets),
    )
    monkeypatch.setattr(serving, "_oversized_geo_code_provider_sets", code_lookup)
    monkeypatch.setattr(serving, "_read_oversized_geo_rate_rows", rate_reader)
    request = serving._GeoRateSelectionRequest(
        code_rows=_code_rows(257),
        args={"zip5": "48201"},
        network_names=[],
        target_count=1,
        descending=False,
    )

    assert (
        await serving._select_oversized_geo_rate_scope(
            object(),
            _production_tables(),
            request,
            serving._geo_rate_selection_budget(_production_tables()),
        )
        == expected
    )
    if local_sets:
        code_lookup.assert_awaited_once()
    else:
        code_lookup.assert_not_awaited()
    rate_reader.assert_not_awaited()


@pytest.mark.asyncio
async def test_oversized_geo_rate_propagates_unavailable_rate_page(monkeypatch):
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        AsyncMock(return_value=None),
    )
    tables = _production_tables()

    assert (
        await serving._read_oversized_geo_rate_rows(
            object(),
            tables,
            serving._GeoRateSelectionRequest(
                code_rows=_code_rows(257),
                args={"zip5": "48201"},
                network_names=[],
                target_count=1,
                descending=False,
            ),
            serving._geo_rate_selection_budget(tables),
            (1,),
            serving._v4_geo_rate_forward_limits(tables).scan_budget,
        )
        is None
    )
