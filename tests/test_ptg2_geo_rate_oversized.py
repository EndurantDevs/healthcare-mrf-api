# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded local-first selection for oversized aggregate geo pricing."""

from dataclasses import replace
from unittest.mock import AsyncMock, Mock

import pytest

from api import ptg2_serving as serving
from tests.test_ptg2_geo_rate_prefix import (
    _code_rows,
    _install_geo_prefix_reads,
    _production_tables,
    _rate_row,
    _tables,
)


def _install_local_scope_reads(monkeypatch, rate_rows, local_npis):
    local_provider_set_keys = tuple(range(1, 69))

    async def rate_read(*_args, **kwargs):
        provider_set_keys = kwargs["provider_set_keys"]
        matching_rates = (
            rate_rows
            if provider_set_keys is None
            else [
                rate_record
                for rate_record in rate_rows
                if rate_record["_ptg_provider_set_key"] in set(provider_set_keys)
            ]
        )
        offset = kwargs["offset"]
        limit = kwargs["limit"]
        return (
            matching_rates[offset:]
            if limit is None
            else matching_rates[offset : offset + limit]
        )

    reads_by_kind = {
        "rates": AsyncMock(side_effect=rate_read),
        "members": AsyncMock(),
        "locations": AsyncMock(
            return_value=[
                {"npi": npi, "_ptg_source_exhausted": True}
                for npi in local_npis
            ]
        ),
        "sets": AsyncMock(
            return_value={
                npi: (local_provider_set_keys[index % 68],)
                for index, npi in enumerate(local_npis)
            }
        ),
        "codes": AsyncMock(
            return_value={
                provider_set_key: ((1,) if provider_set_key <= 65 else ())
                for provider_set_key in local_provider_set_keys
            }
        ),
        "national": AsyncMock(
            side_effect=AssertionError("oversized selection decoded national rates")
        ),
    }
    monkeypatch.setattr(
        serving, "_merge_manifest_code_variant_rows", reads_by_kind["rates"]
    )
    monkeypatch.setattr(serving, "_provider_npis_for_sets", reads_by_kind["members"])
    monkeypatch.setattr(serving, "_membership_npi_rows", reads_by_kind["locations"])
    monkeypatch.setattr(serving, "_v4_sets_by_npi", reads_by_kind["sets"])
    monkeypatch.setattr(
        serving, "_shared_forward_entries_for_code_rows", reads_by_kind["national"]
    )
    monkeypatch.setattr(
        serving,
        "lookup_shared_provider_code_intersections_from_db",
        reads_by_kind["codes"],
    )
    return reads_by_kind


def _install_single_set_scope(monkeypatch, rate_rows):
    reads_by_kind = {
        "locations": AsyncMock(
            return_value=[{"npi": 111, "_ptg_source_exhausted": True}]
        ),
        "sets": AsyncMock(return_value={111: (1,)}),
        "codes": AsyncMock(return_value={1: (1,)}),
        "rates": AsyncMock(return_value=rate_rows),
    }
    monkeypatch.setattr(serving, "_membership_npi_rows", reads_by_kind["locations"])
    monkeypatch.setattr(serving, "_v4_sets_by_npi", reads_by_kind["sets"])
    monkeypatch.setattr(
        serving,
        "lookup_shared_provider_code_intersections_from_db",
        reads_by_kind["codes"],
    )
    monkeypatch.setattr(
        serving, "_merge_manifest_code_variant_rows", reads_by_kind["rates"]
    )
    return reads_by_kind


@pytest.mark.asyncio
async def test_oversized_geo_rate_finds_local_set_after_national_cap(monkeypatch):
    rate_rows = [_rate_row(rank) for rank in range(65)]
    local_npis = tuple(range(358, 502))
    reads_by_kind = _install_local_scope_reads(monkeypatch, rate_rows, local_npis)
    projection_scope = Mock(wraps=serving.v4_graph_taxonomy_projection_scope)
    monkeypatch.setattr(
        serving,
        "v4_graph_taxonomy_projection_scope",
        projection_scope,
    )

    selection = await serving._select_geo_filtered_rate_prefix(
        object(),
        _production_tables(),
        code_rows=_code_rows(257),
        args={"zip5": "48201", "zip_radius_miles": 30},
        network_names=[],
        target_count=256,
        descending=False,
    )

    assert selection == serving._GeoRateSelection(tuple(rate_rows), True)
    reads_by_kind["locations"].assert_awaited_once()
    assert reads_by_kind["locations"].await_args.kwargs["limit"] == (
        serving._ptg2_manifest_location_match_limit()
    )
    assert len(local_npis) > serving._geo_rate_selection_budget(
        _production_tables()
    ).caps.maximum_provider_sets
    reads_by_kind["sets"].assert_awaited_once()
    assert reads_by_kind["sets"].await_args.args[2] == local_npis
    assert reads_by_kind["sets"].await_args.kwargs["allowed_provider_set_keys"] is None
    assert reads_by_kind["sets"].await_args.kwargs["max_members"] == 65_536
    assert reads_by_kind["sets"].await_args.kwargs["max_projection_members"] == 131_072
    projection_scope.assert_called_once_with(
        maximum_members=131_072,
        maximum_pages=256,
        maximum_bytes=4 * 1024 * 1024,
        maximum_batches=32,
    )
    assert reads_by_kind["codes"].await_args.args[2:] == (
        tuple(range(1, 69)),
        (1,),
    )
    assert reads_by_kind["codes"].await_args.kwargs["max_retained_memberships"] == 257
    assert reads_by_kind["rates"].await_args.kwargs["provider_set_keys"] == tuple(
        range(1, 66)
    )
    assert reads_by_kind["rates"].await_args.kwargs["limit"] == 256
    scan_budget = reads_by_kind["rates"].await_args.kwargs["scan_budget"]
    assert scan_budget.maximum_fragments == 256
    assert scan_budget.maximum_raw_payload_bytes == 4 * 1024 * 1024
    assert scan_budget.maximum_row_capacity == 6_701
    reads_by_kind["national"].assert_not_awaited()
    reads_by_kind["members"].assert_not_awaited()


@pytest.mark.asyncio
async def test_oversized_geo_rate_rejects_graph_overflow_before_downstream_reads(
    monkeypatch,
):
    reads_by_kind = _install_single_set_scope(monkeypatch, [])
    reads_by_kind["sets"].side_effect = serving.PTG2OnlineWorkBudgetExceeded(
        "graph_pages"
    )

    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded) as exc_info:
        await serving._select_geo_filtered_rate_prefix(
            object(),
            _production_tables(),
            code_rows=_code_rows(257),
            args={"zip5": "48201"},
            network_names=[],
            target_count=1,
            descending=False,
        )

    assert exc_info.value.dimension == "candidate_members"
    reads_by_kind["sets"].assert_awaited_once()
    reads_by_kind["codes"].assert_not_awaited()
    reads_by_kind["rates"].assert_not_awaited()


@pytest.mark.asyncio
async def test_oversized_geo_rate_rejects_reverse_membership_overflow(monkeypatch):
    monkeypatch.setattr(
        serving,
        "_membership_npi_rows",
        AsyncMock(return_value=[{"npi": 111, "_ptg_source_exhausted": True}]),
    )
    monkeypatch.setattr(
        serving,
        "_v4_sets_by_npi",
        AsyncMock(
            side_effect=serving.PTG2SharedBlockError(
                "PTG V4 graph selection exceeds max_members"
            )
        ),
    )

    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded) as exc_info:
        await serving._select_geo_filtered_rate_prefix(
            object(),
            _production_tables(),
            code_rows=_code_rows(2560),
            args={"zip5": "48201", "zip_radius_miles": 30},
            network_names=[],
            target_count=11,
            descending=False,
        )

    assert exc_info.value.dimension == "candidate_members"


@pytest.mark.asyncio
async def test_oversized_geo_rate_rejects_more_than_sealed_local_sets(monkeypatch):
    local_set_keys = tuple(range(1, 258))
    monkeypatch.setattr(
        serving,
        "_membership_npi_rows",
        AsyncMock(return_value=[{"npi": 111, "_ptg_source_exhausted": True}]),
    )
    monkeypatch.setattr(
        serving, "_v4_sets_by_npi", AsyncMock(return_value={111: local_set_keys})
    )
    monkeypatch.setattr(
        serving,
        "lookup_shared_provider_code_intersections_from_db",
        AsyncMock(return_value={key: (1,) for key in local_set_keys}),
    )
    rate_reads = AsyncMock()
    monkeypatch.setattr(serving, "_merge_manifest_code_variant_rows", rate_reads)

    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded) as exc_info:
        await serving._select_geo_filtered_rate_prefix(
            object(),
            _production_tables(),
            code_rows=_code_rows(257),
            args={"zip5": "48201"},
            network_names=[],
            target_count=1,
            descending=False,
        )

    assert exc_info.value.dimension == "candidate_members"
    rate_reads.assert_not_awaited()


@pytest.mark.asyncio
async def test_oversized_geo_rate_returns_valid_sealed_boundary_page(monkeypatch):
    rate_rows = [_rate_row(rank) for rank in range(256)]
    _install_single_set_scope(monkeypatch, rate_rows)

    selection = await serving._select_geo_filtered_rate_prefix(
        object(),
        _production_tables(),
        code_rows=_code_rows(257),
        args={"zip5": "48201"},
        network_names=[],
        target_count=256,
        descending=False,
    )

    assert selection == serving._GeoRateSelection(tuple(rate_rows), False)


@pytest.mark.asyncio
async def test_oversized_geo_rate_rejects_page_deeper_than_sealed_rate_cap(monkeypatch):
    reads_by_kind = _install_single_set_scope(monkeypatch, [])

    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded) as exc_info:
        await serving._select_geo_filtered_rate_prefix(
            object(),
            _production_tables(),
            code_rows=_code_rows(257),
            args={"zip5": "48201"},
            network_names=[],
            target_count=257,
            descending=False,
        )

    assert exc_info.value.dimension == "candidate_members"
    assert all(not reader.await_args_list for reader in reads_by_kind.values())


@pytest.mark.asyncio
async def test_oversized_geo_rate_rejects_physical_code_fanout_before_reads(monkeypatch):
    reads_by_kind = _install_single_set_scope(monkeypatch, [])

    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded) as exc_info:
        await serving._select_geo_filtered_rate_prefix(
            object(),
            _production_tables(),
            code_rows=[
                {"code_key": code_key, "rate_count": 1}
                for code_key in range(1, 258)
            ],
            args={"zip5": "48201"},
            network_names=[],
            target_count=1,
            descending=False,
        )

    assert exc_info.value.dimension == "forward_scan"
    assert all(not reader.await_args_list for reader in reads_by_kind.values())


@pytest.mark.asyncio
async def test_oversized_geo_rate_without_new_limits_preserves_early_bounded_match(
    monkeypatch,
):
    rate_rows = [_rate_row(rank) for rank in range(13)]
    page_reads, member_reads = _install_geo_prefix_reads(
        monkeypatch,
        rate_rows,
        {101},
    )
    selection = await serving._select_geo_filtered_rate_prefix(
        object(),
        _tables(),
        code_rows=_code_rows(13),
        args={"zip5": "48201"},
        network_names=[],
        target_count=1,
        descending=False,
    )

    assert selection == serving._GeoRateSelection((rate_rows[0],), False)
    page_reads.assert_awaited_once()
    assert page_reads.await_args.kwargs["offset"] == 0
    member_reads.assert_not_awaited()


@pytest.mark.parametrize(
    ("projection_manifest", "error_match"),
    (
        (None, "missing sealed forward-read limits"),
        ({}, "forward-read limits are malformed"),
        (
            {"max_online_inferred_taxonomy_retained_memberships": 0},
            "forward-read limits must be positive",
        ),
    ),
)
def test_oversized_geo_rate_rejects_invalid_forward_limits(
    projection_manifest,
    error_match,
):
    tables = replace(
        _production_tables(),
        provider_graph_v4_inferred_taxonomy_candidates=projection_manifest,
    )

    with pytest.raises(serving.PTG2ManifestArtifactError, match=error_match):
        serving._v4_geo_rate_forward_limits(tables)


@pytest.mark.asyncio
async def test_oversized_geo_rate_caps_multi_code_membership_retention(monkeypatch):
    code_keys = tuple(range(1, 257))
    code_lookup = AsyncMock(return_value={1: code_keys})
    monkeypatch.setattr(
        serving,
        "lookup_shared_provider_code_intersections_from_db",
        code_lookup,
    )
    selection = await serving._oversized_geo_code_provider_sets(
        object(),
        _production_tables(),
        code_keys,
        (1,),
        serving._geo_rate_selection_budget(_production_tables()),
        65_536,
        6_600,
    )

    assert selection == (1,)
    assert code_lookup.await_args.kwargs["max_retained_memberships"] == 65_536


@pytest.mark.asyncio
@pytest.mark.parametrize(("set_count", "rejected"), ((6_600, False), (6_601, True)))
async def test_oversized_geo_rate_seals_provider_code_set_fanout(
    monkeypatch,
    set_count,
    rejected,
):
    local_set_keys = tuple(range(set_count))
    code_lookup = AsyncMock(
        return_value={provider_set_key: () for provider_set_key in local_set_keys}
    )
    monkeypatch.setattr(
        serving,
        "lookup_shared_provider_code_intersections_from_db",
        code_lookup,
    )

    selection_call = serving._oversized_geo_code_provider_sets(
        object(),
        _production_tables(),
        (1,),
        local_set_keys,
        serving._geo_rate_selection_budget(_production_tables()),
        65_536,
        6_600,
    )
    if rejected:
        with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded) as exc_info:
            await selection_call
        assert exc_info.value.dimension == "code_sets"
        code_lookup.assert_not_awaited()
    else:
        assert await selection_call == ()
        code_lookup.assert_awaited_once()
