# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reverse selection for oversized geo-filtered provider sets."""

from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from tests.test_ptg2_geo_rate_prefix import (
    _code_rows,
    _production_tables,
    _rate_row,
    _tables,
)


_EXHAUSTIVE_LOCAL_NPIS = tuple(range(101, 112))


def _exhaustive_location_rows(*_args, **kwargs):
    selected_npis = _EXHAUSTIVE_LOCAL_NPIS[: kwargs["limit"]]
    return [
        {
            "npi": npi,
            "_ptg_source_exhausted": len(selected_npis)
            == len(_EXHAUSTIVE_LOCAL_NPIS),
        }
        for npi in selected_npis
    ]


def _second_set_reverse_matches(
    _session,
    _tables,
    candidate_npis,
    provider_set_keys,
    **_kwargs,
):
    selected_set_key = next(iter(provider_set_keys))
    if selected_set_key == 2 and _EXHAUSTIVE_LOCAL_NPIS[-1] in candidate_npis:
        return {_EXHAUSTIVE_LOCAL_NPIS[-1]: (selected_set_key,)}
    return {}


@pytest.mark.asyncio
async def test_geo_rate_prefix_reverses_oversized_set_after_geo_taxonomy(
    monkeypatch,
):
    rate_row = _rate_row(0, provider_count=100_001)
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        AsyncMock(return_value=[rate_row]),
    )
    member_reads = AsyncMock()
    monkeypatch.setattr(serving, "_provider_npis_for_sets", member_reads)
    location_reads = AsyncMock(
        return_value=[{"npi": 111, "_ptg_source_exhausted": True}]
    )
    monkeypatch.setattr(serving, "_membership_npi_rows", location_reads)
    taxonomy_reads = AsyncMock(return_value=(111,))
    monkeypatch.setattr(serving, "_filter_npis_by_taxonomy", taxonomy_reads)
    reverse_reads = AsyncMock(return_value={111: (1,)})
    monkeypatch.setattr(serving, "_v4_sets_by_npi", reverse_reads)

    selection = await serving._select_geo_filtered_rate_prefix(
        object(),
        _production_tables(),
        code_rows=_code_rows(1),
        args={"state": "MI", "classification": "Synthetic classification"},
        network_names=[],
        target_count=1,
        descending=False,
    )

    assert selection == serving._GeoRateSelection((rate_row,), False)
    member_reads.assert_not_awaited()
    location_reads.assert_awaited_once()
    taxonomy_reads.assert_awaited_once()
    reverse_reads.assert_awaited_once()
    assert reverse_reads.await_args.kwargs["max_members"] == 100_000
    assert "max_projection_members" not in reverse_reads.await_args.kwargs


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("graph_error", "expected_error"),
    (
        (
            "PTG V4 graph selection exceeds max_members",
            serving.PTG2OnlineWorkBudgetExceeded,
        ),
        ("synthetic graph corruption", serving.PTG2SharedBlockError),
    ),
)
async def test_geo_rate_prefix_maps_only_bounded_reverse_graph_overflow(
    monkeypatch,
    graph_error,
    expected_error,
):
    rate_row = _rate_row(0, provider_count=100_001)
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        AsyncMock(return_value=[rate_row]),
    )
    monkeypatch.setattr(
        serving,
        "_membership_npi_rows",
        AsyncMock(return_value=[{"npi": 111, "_ptg_source_exhausted": True}]),
    )
    reverse_reads = AsyncMock(
        side_effect=serving.PTG2SharedBlockError(graph_error)
    )
    monkeypatch.setattr(serving, "_v4_sets_by_npi", reverse_reads)

    with pytest.raises(expected_error) as exc_info:
        await serving._select_geo_filtered_rate_prefix(
            object(),
            _production_tables(),
            code_rows=_code_rows(1),
            args={"state": "MI"},
            network_names=[],
            target_count=1,
            descending=False,
        )

    if expected_error is serving.PTG2OnlineWorkBudgetExceeded:
        assert exc_info.value.dimension == "candidate_members"
    else:
        assert type(exc_info.value) is serving.PTG2SharedBlockError
        assert str(exc_info.value) == graph_error
    assert reverse_reads.await_args.kwargs["max_members"] == 100_000
    assert "max_projection_members" not in reverse_reads.await_args.kwargs


@pytest.mark.asyncio
async def test_geo_rate_prefix_rejects_duplicate_reverse_set_identity(monkeypatch):
    rate_rows = [
        _rate_row(0, provider_count=100_001),
        {
            **_rate_row(1, provider_count=100_001),
            "provider_set_global_id_128": f"{1:032x}",
        },
    ]
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        AsyncMock(return_value=rate_rows),
    )
    location_reads = AsyncMock()
    monkeypatch.setattr(serving, "_membership_npi_rows", location_reads)

    with pytest.raises(
        serving.PTG2ManifestArtifactError,
        match="lost a provider-set identity",
    ):
        await serving._select_geo_filtered_rate_prefix(
            object(),
            _production_tables(),
            code_rows=_code_rows(2),
            args={"state": "MI"},
            network_names=[],
            target_count=1,
            descending=False,
        )

    location_reads.assert_not_awaited()


@pytest.mark.asyncio
async def test_geo_rate_prefix_preserves_unavailable_reverse_geo_source(
    monkeypatch,
):
    rate_row = _rate_row(0, provider_count=100_001)
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        AsyncMock(return_value=[rate_row]),
    )
    member_reads = AsyncMock()
    location_reads = AsyncMock(return_value=None)
    reverse_reads = AsyncMock()
    monkeypatch.setattr(serving, "_provider_npis_for_sets", member_reads)
    monkeypatch.setattr(serving, "_membership_npi_rows", location_reads)
    monkeypatch.setattr(
        serving,
        "_shared_provider_set_keys_by_npi",
        reverse_reads,
    )

    selection = await serving._select_geo_filtered_rate_prefix(
        object(),
        _production_tables(),
        code_rows=_code_rows(1),
        args={"state": "MI"},
        network_names=[],
        target_count=1,
        descending=False,
    )

    assert selection is None
    member_reads.assert_not_awaited()
    location_reads.assert_awaited_once()
    reverse_reads.assert_not_awaited()


@pytest.mark.asyncio
async def test_geo_rate_prefix_reuses_exhaustive_reverse_scope_across_pages(
    monkeypatch,
):
    """Keep one bounded reverse-geo candidate scope across rate pages."""
    rate_rows = [
        _rate_row(0, provider_count=21),
        _rate_row(1, provider_count=21),
    ]
    monkeypatch.setattr(serving, "_ptg2_manifest_location_match_limit", lambda: 1)
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        AsyncMock(
            side_effect=lambda *_args, **kwargs: rate_rows[
                kwargs["offset"] : kwargs["offset"] + kwargs["limit"]
            ]
        ),
    )
    location_reads = AsyncMock(side_effect=_exhaustive_location_rows)
    monkeypatch.setattr(serving, "_membership_npi_rows", location_reads)
    reverse_reads = AsyncMock(side_effect=_second_set_reverse_matches)
    monkeypatch.setattr(serving, "_shared_provider_set_keys_by_npi", reverse_reads)

    selection = await serving._select_geo_filtered_rate_prefix(
        object(),
        _tables(
            provider_expansion_rate_page_rows=1,
            max_online_provider_expansion_rate_rows=2,
            max_online_provider_expansion_provider_sets=2,
            max_online_provider_expansion_graph_batches=2,
        ),
        code_rows=_code_rows(2),
        args={"zip5": "48201"},
        network_names=[],
        target_count=2,
        descending=False,
    )

    assert selection == serving._GeoRateSelection((rate_rows[1],), True)
    location_reads.assert_awaited_once()
    assert reverse_reads.await_count == 2
    assert [call.kwargs["max_members"] for call in reverse_reads.await_args_list] == [
        20,
        20,
    ]
