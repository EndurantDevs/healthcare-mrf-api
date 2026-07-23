# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import pytest

from api import ptg2_serving as serving
from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2ManifestArtifactError,
)
from tests.ptg2_v4_incremental_cpt_expansion_support import (
    IncrementalExpansionHarness as _IncrementalExpansionHarness,
    provider_set_id as _provider_set_id,
    rate_row as _rate_row,
)


@pytest.fixture(autouse=True)
def _clear_provider_expansion_cache():
    serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()
    yield
    serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()


@pytest.mark.asyncio
async def test_selected_npi_receives_later_set_rates_beyond_rank_cutoff(
    monkeypatch,
):
    first_set = _provider_set_id(1)
    later_set = _provider_set_id(2)
    selected_npi = 1_000_000_001
    first_set_npis = tuple(range(selected_npi, selected_npi + 25))
    rate_rows = [
        _rate_row(
            1,
            occurrence,
            price_key=occurrence,
            provider_count=25,
        )
        for occurrence in range(1, 65)
    ]
    rate_rows.append(
        _rate_row(
            2,
            65,
            price_key=65,
            provider_count=1,
        )
    )
    harness = _IncrementalExpansionHarness(
        monkeypatch,
        rate_rows,
        {
            first_set: first_set_npis,
            later_set: (selected_npi,),
        },
    )

    selection = await harness.select(target_count=25)

    assert selection is not None
    assert selection.total_lower_bound == 25
    assert harness.rate_reads == [(64, 0, False)]
    assert harness.completion_membership_calls == [
        (first_set_npis, frozenset({1, 2}))
    ]
    assert harness.completion_reads == [
        ((1, 2), 64, 0, False),
        ((1, 2), 1, 64, False),
    ]
    assert selection.providers_by_set[later_set] == [
        {
            "npi": selected_npi,
            "provider_name": f"Provider {selected_npi}",
        }
    ]
    assert any(
        int(completion_row["_ptg_provider_set_key"]) == 2
        and int(completion_row["price_key"]) == 65
        for completion_row in selection.row_data
    )


@pytest.mark.asyncio
async def test_completion_set_cap_fails_without_partial_rate_rows(
    monkeypatch,
):
    selected_npi = 1_000_000_001
    first_set_npis = tuple(range(selected_npi, selected_npi + 25))
    harness = _IncrementalExpansionHarness(
        monkeypatch,
        [
            _rate_row(1, 1, price_key=1, provider_count=25),
            _rate_row(2, 2, price_key=2, provider_count=1),
        ],
        {
            _provider_set_id(1): first_set_npis,
            _provider_set_id(2): (selected_npi,),
        },
        sealed_cap_overrides={
            "maximum_provider_expansion_provider_sets": 1,
        },
    )

    with pytest.raises(
        PTG2ManifestArtifactError,
        match="distinct-provider-set cap",
    ):
        await harness.select(target_count=25)

    assert harness.completion_membership_calls == [
        (first_set_npis, frozenset({1, 2}))
    ]
    assert harness.completion_reads == []


@pytest.mark.asyncio
async def test_completion_rate_cap_fails_before_partial_completion_page(
    monkeypatch,
):
    first_set = _provider_set_id(1)
    harness = _IncrementalExpansionHarness(
        monkeypatch,
        [
            _rate_row(
                1,
                occurrence,
                price_key=occurrence,
                provider_count=25,
            )
            for occurrence in range(1, 101)
        ],
        {
            first_set: tuple(
                range(1_000_000_001, 1_000_000_026)
            )
        },
        sealed_cap_overrides={
            "maximum_provider_expansion_rate_rows": 100,
        },
    )

    with pytest.raises(PTG2ManifestArtifactError, match="rate-row cap"):
        await harness.select(target_count=25)

    assert harness.rate_reads == [(64, 0, False)]
    assert harness.completion_reads == []


@pytest.mark.asyncio
async def test_completion_graph_cap_fails_before_reverse_traversal(
    monkeypatch,
):
    provider_set = _provider_set_id(1)
    harness = _IncrementalExpansionHarness(
        monkeypatch,
        [_rate_row(1, 1, price_key=1, provider_count=25)],
        {
            provider_set: tuple(
                range(1_000_000_001, 1_000_000_026)
            )
        },
        sealed_cap_overrides={
            "maximum_provider_expansion_graph_batches": 1,
        },
    )

    with pytest.raises(PTG2ManifestArtifactError, match="graph-batch cap"):
        await harness.select(target_count=25)

    assert harness.graph_batches == [(provider_set,)]
    assert harness.completion_membership_calls == []
    assert harness.completion_reads == []
