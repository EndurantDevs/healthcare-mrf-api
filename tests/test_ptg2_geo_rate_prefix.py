# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded rate-first selection for aggregate geo pricing pages."""

from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from tests.ptg2_serving_coverage_paydown_support import strict_v3_tables
from tests.ptg2_v4_provider_prefix_support import sealed_v4_hot_prefix


def _tables(**limit_overrides):
    return strict_v3_tables(
        storage_generation="shared_blocks_v4",
        shared_block_layout="packed_snapshot_maps_v4",
        provider_graph_v4_hot_prefix=sealed_v4_hot_prefix(
            provider_expansion_rate_page_rows=4,
            max_online_provider_expansion_rate_rows=12,
            max_online_provider_expansion_provider_sets=12,
            max_online_provider_expansion_graph_batches=3,
            **limit_overrides,
        ),
    )


def _production_tables():
    return strict_v3_tables(
        storage_generation="shared_blocks_v4",
        shared_block_layout="packed_snapshot_maps_v4",
        provider_graph_v4_hot_prefix=sealed_v4_hot_prefix(),
    )


def _rate_row(rank: int, *, provider_count: int = 1) -> dict[str, object]:
    return {
        "provider_set_global_id_128": f"{rank + 1:032x}",
        "provider_count": provider_count,
        "price_key": rank + 1,
        "_ptg_provider_set_key": rank + 1,
        "serving_content_hash_128": f"{rank + 101:032x}",
    }


def _code_rows(count: int) -> list[dict[str, object]]:
    return [{"code_key": 1, "rate_count": count}]


def _install_geo_prefix_reads(monkeypatch, rate_rows, qualifying_npis):
    """Install deterministic rate, membership, and geo page readers."""

    page_reads = AsyncMock(
        side_effect=lambda *_args, **kwargs: rate_rows[
            kwargs["offset"] : kwargs["offset"] + kwargs["limit"]
        ]
    )
    member_reads = AsyncMock(
        side_effect=lambda _session, _tables, provider_set_ids, **_kwargs: {
            provider_set_id: (int(provider_set_id, 16) + 100,)
            for provider_set_id in provider_set_ids
        }
    )

    async def location_rows(*_args, **kwargs):
        return [
            {"npi": npi} for npi in kwargs["candidate_npis"] if npi in qualifying_npis
        ]

    monkeypatch.setattr(serving, "_merge_manifest_code_variant_rows", page_reads)
    monkeypatch.setattr(serving, "_provider_npis_for_sets", member_reads)
    monkeypatch.setattr(serving, "_membership_location_rows", location_rows)
    return page_reads, member_reads


@pytest.mark.asyncio
async def test_geo_rate_prefix_reads_disjoint_pages_and_preserves_rate_order(
    monkeypatch,
):
    rate_rows = [_rate_row(rank) for rank in range(8)]
    qualifying_npis = {102, 105, 108}
    page_reads, member_reads = _install_geo_prefix_reads(
        monkeypatch,
        rate_rows,
        qualifying_npis,
    )

    selection = await serving._select_geo_filtered_rate_prefix(
        object(),
        _tables(),
        code_rows=_code_rows(len(rate_rows)),
        args={"zip5": "48201", "zip_radius_miles": 30},
        network_names=[],
        target_count=3,
        descending=False,
    )

    assert selection is not None
    assert [rate_record["price_key"] for rate_record in selection.row_data] == [2, 5, 8]
    assert selection.exhausted is False
    assert [call.kwargs["offset"] for call in page_reads.await_args_list] == [0, 4]
    assert [call.kwargs["limit"] for call in page_reads.await_args_list] == [4, 4]
    assert member_reads.await_count == 2


@pytest.mark.asyncio
async def test_geo_rate_prefix_reaches_production_256_set_budget(monkeypatch):
    rate_rows = [_rate_row(rank) for rank in range(256)]
    qualifying_ranks = (49, 134, 148, 188, 206, 210)
    qualifying_npis = {rank + 101 for rank in qualifying_ranks}
    page_reads, member_reads = _install_geo_prefix_reads(
        monkeypatch,
        rate_rows,
        qualifying_npis,
    )

    selection = await serving._select_geo_filtered_rate_prefix(
        object(),
        _production_tables(),
        code_rows=_code_rows(256),
        args={"zip5": "48201", "zip_radius_miles": 30},
        network_names=[],
        target_count=6,
        descending=False,
    )

    assert selection is not None
    assert [rate_record["price_key"] for rate_record in selection.row_data] == [
        rank + 1 for rank in qualifying_ranks
    ]
    assert selection.exhausted is False
    assert [call.kwargs["offset"] for call in page_reads.await_args_list] == [
        0,
        64,
        128,
        192,
    ]
    assert member_reads.await_count == 4


@pytest.mark.asyncio
async def test_geo_rate_prefix_deep_page_rejects_at_sealed_rate_cap(monkeypatch):
    rate_rows = [_rate_row(rank) for rank in range(256)]
    qualifying_npis = {rank + 101 for rank in (49, 134, 148, 188, 206, 210)}
    _install_geo_prefix_reads(monkeypatch, rate_rows, qualifying_npis)

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
async def test_geo_rate_prefix_one_npi_can_witness_multiple_sets(monkeypatch):
    rate_rows = [_rate_row(0, provider_count=2), _rate_row(1, provider_count=2)]
    shared_npi = 999_999_991
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        AsyncMock(return_value=rate_rows),
    )
    monkeypatch.setattr(
        serving,
        "_provider_npis_for_sets",
        AsyncMock(
            return_value={
                rate_rows[0]["provider_set_global_id_128"]: (111, shared_npi),
                rate_rows[1]["provider_set_global_id_128"]: (222, shared_npi),
            }
        ),
    )
    membership = AsyncMock(return_value=[{"npi": shared_npi}])
    monkeypatch.setattr(serving, "_membership_location_rows", membership)

    selection = await serving._select_geo_filtered_rate_prefix(
        object(),
        _tables(),
        code_rows=_code_rows(2),
        args={"zip5": "48201"},
        network_names=[],
        target_count=3,
        descending=False,
    )

    assert selection == serving._GeoRateSelection(tuple(rate_rows), True)
    membership.assert_awaited_once()
    assert membership.await_args.kwargs["candidate_npis"] == (
        111,
        222,
        shared_npi,
    )


@pytest.mark.asyncio
async def test_geo_rate_prefix_absent_sets_are_exact_only_after_source_end(
    monkeypatch,
):
    rate_rows = [_rate_row(rank) for rank in range(3)]
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        AsyncMock(return_value=rate_rows),
    )
    monkeypatch.setattr(
        serving,
        "_provider_npis_for_sets",
        AsyncMock(
            side_effect=lambda _session, _tables, provider_set_ids, **_kwargs: {
                provider_set_id: (int(provider_set_id, 16),)
                for provider_set_id in provider_set_ids
            }
        ),
    )
    monkeypatch.setattr(
        serving,
        "_membership_location_rows",
        AsyncMock(return_value=[]),
    )

    selection = await serving._select_geo_filtered_rate_prefix(
        object(),
        _tables(),
        code_rows=_code_rows(3),
        args={"zip5": "48201"},
        network_names=[],
        target_count=2,
        descending=False,
    )

    assert selection == serving._GeoRateSelection((), True)


@pytest.mark.asyncio
async def test_geo_rate_prefix_rejects_member_budget_before_graph_or_sql(
    monkeypatch,
):
    rate_rows = [_rate_row(0, provider_count=3), _rate_row(1, provider_count=3)]
    monkeypatch.setattr(serving, "_ptg2_manifest_location_match_limit", lambda: 0)
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        AsyncMock(return_value=rate_rows),
    )
    member_reads = AsyncMock()
    location_reads = AsyncMock()
    monkeypatch.setattr(serving, "_provider_npis_for_sets", member_reads)
    monkeypatch.setattr(serving, "_membership_location_rows", location_reads)

    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded) as exc_info:
        await serving._select_geo_filtered_rate_prefix(
            object(),
            _tables(),
            code_rows=_code_rows(2),
            args={"zip5": "48201"},
            network_names=[],
            target_count=2,
            descending=False,
        )

    assert exc_info.value.dimension == "candidate_members"
    member_reads.assert_not_awaited()
    location_reads.assert_not_awaited()


@pytest.mark.asyncio
async def test_geo_rate_prefix_rejects_incomplete_exact_membership(monkeypatch):
    rate_row = _rate_row(0, provider_count=2)
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        AsyncMock(return_value=[rate_row]),
    )
    monkeypatch.setattr(
        serving,
        "_provider_npis_for_sets",
        AsyncMock(return_value={rate_row["provider_set_global_id_128"]: (111,)}),
    )
    location_reads = AsyncMock()
    monkeypatch.setattr(serving, "_membership_location_rows", location_reads)

    with pytest.raises(
        serving.PTG2ManifestArtifactError,
        match="membership disagrees",
    ):
        await serving._select_geo_filtered_rate_prefix(
            object(),
            _tables(),
            code_rows=_code_rows(1),
            args={"zip5": "48201"},
            network_names=[],
            target_count=1,
            descending=False,
        )

    location_reads.assert_not_awaited()
