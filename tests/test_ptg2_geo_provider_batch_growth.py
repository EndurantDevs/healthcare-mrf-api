# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Progressive graph batching for exact geo provider expansion."""

from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from tests.test_ptg2_geo_provider_expansion import (
    _geo_tables,
    _install_geo_completion,
    _install_provider_enrichment,
    _install_rate_scan,
    _rate_row,
    _select_geo,
)


def _install_sparse_prefix(monkeypatch):
    provider_set_ids = tuple(f"{number:032x}" for number in range(1, 9))
    npis_by_set = {
        provider_set_id: (1234567800 + index,)
        for index, provider_set_id in enumerate(provider_set_ids, start=1)
    }
    matching_npi = npis_by_set[provider_set_ids[6]][0]
    rate_rows = [
        _rate_row(provider_set_id, index, 1 if index < 8 else 2)
        for index, provider_set_id in enumerate(provider_set_ids, start=1)
    ]
    merge_rows, member_rows = _install_rate_scan(
        monkeypatch, rate_rows, npis_by_set, {matching_npi: 1.0}
    )
    monkeypatch.setattr(
        serving,
        "_membership_location_rows",
        AsyncMock(
            side_effect=lambda *_args, **kwargs: (
                [{"npi": matching_npi, "distance_miles": 1.0}]
                if matching_npi in kwargs["candidate_npis"]
                else []
            )
        ),
    )
    reverse_memberships, completion = _install_geo_completion(
        monkeypatch,
        provider_set_keys_by_npi={matching_npi: (7,)},
        provider_set_id_by_key={7: provider_set_ids[6]},
        completion_rows=[rate_rows[6]],
    )
    _install_provider_enrichment(monkeypatch)
    return (
        provider_set_ids,
        matching_npi,
        rate_rows,
        merge_rows,
        member_rows,
        reverse_memberships,
        completion,
    )


@pytest.mark.asyncio
async def test_sparse_geo_prefix_preserves_batch_growth_across_pages(monkeypatch):
    """Keep exact completion capacity after sparse disjoint rate pages."""

    (
        provider_set_ids,
        matching_npi,
        rate_rows,
        merge_rows,
        member_rows,
        reverse_memberships,
        completion,
    ) = _install_sparse_prefix(monkeypatch)

    tables = _geo_tables(
        provider_expansion_rate_page_rows=4,
        max_online_provider_expansion_graph_batches=6,
    )
    budget = serving._geo_rate_selection_budget(tables)
    monkeypatch.setattr(serving, "_geo_rate_selection_budget", lambda _tables: budget)
    selection = await _select_geo(
        tables,
        rate_count=len(rate_rows),
        target_count=1,
        descending=False,
    )

    assert selection is not None
    assert selection.exhausted is True
    assert list(selection.rank_by_key) == [
        ("npi", str(matching_npi), "CPT", "99213", "FFS", "0")
    ]
    assert selection.providers_by_set[provider_set_ids[6]][0]["npi"] == matching_npi
    assert [call.args[2] for call in member_rows.await_args_list] == [
        (provider_set_ids[0],),
        provider_set_ids[1:3],
        provider_set_ids[3:7],
    ]
    assert budget.graph_batches == 4
    assert budget.caps.maximum_graph_batches - budget.graph_batches == 2
    assert budget.provider_set_ids == set(provider_set_ids[:7])
    assert [
        call.kwargs["offset"]
        for call in merge_rows.await_args_list
        if call.kwargs["provider_set_keys"] is None
    ] == [0, 1, 3, 7]
    reverse_memberships.assert_awaited_once()
    completion.assert_awaited_once()
