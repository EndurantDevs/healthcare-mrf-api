# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import pytest

from api import ptg2_serving as serving
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.ptg2_v4_incremental_cpt_expansion_support import (
    IncrementalExpansionHarness as _IncrementalExpansionHarness,
    provider_set_id as _provider_set_id,
    rate_row as _rate_row,
    v4_tables as _v4_tables,
)


@pytest.fixture(autouse=True)
def _clear_provider_expansion_cache():
    serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()
    yield
    serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()


@pytest.mark.asyncio
async def test_first_set_proves_25_results_with_one_graph_traversal(
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
                provider_count=30,
            )
            for occurrence in range(1, 101)
        ],
        {first_set: tuple(range(1_000_000_001, 1_000_000_031))},
    )

    selection = await harness.select(target_count=25)

    assert selection is not None
    assert selection.total_lower_bound == 25
    assert harness.rate_reads == [(64, 0, False)]
    assert harness.graph_batches == [(first_set,)]
    assert harness.completion_reads == [
        ((1,), 64, 0, False),
        ((1,), 36, 64, False),
    ]
    assert len(selection.row_data) == 100


@pytest.mark.asyncio
async def test_small_sets_share_one_necessary_graph_batch(
    monkeypatch,
):
    rate_rows = [
        _rate_row(
            provider_set_value,
            provider_set_value,
            price_key=provider_set_value,
            provider_count=7,
        )
        for provider_set_value in range(1, 9)
    ]
    npis_by_set = {
        _provider_set_id(provider_set_value): tuple(
            range(
                1_000_000_000 + (provider_set_value * 10),
                1_000_000_007 + (provider_set_value * 10),
            )
        )
        for provider_set_value in range(1, 9)
    }
    harness = _IncrementalExpansionHarness(
        monkeypatch,
        rate_rows,
        npis_by_set,
    )

    selection = await harness.select(target_count=25)

    assert selection is not None
    assert selection.total_lower_bound == 25
    assert harness.rate_reads == [(8, 0, False)]
    assert harness.graph_batches == [
        tuple(
            _provider_set_id(provider_set_value)
            for provider_set_value in range(1, 5)
        )
    ]
    assert harness.completion_reads == [
        ((1, 2, 3, 4), 4, 0, False)
    ]
    assert len(selection.row_data) == 4


@pytest.mark.asyncio
async def test_empty_and_repeated_sets_are_exact_and_traversed_once(
    monkeypatch,
):
    empty_set = _provider_set_id(1)
    provider_set = _provider_set_id(2)
    harness = _IncrementalExpansionHarness(
        monkeypatch,
        [
            _rate_row(1, 1, price_key=1),
            _rate_row(1, 2, price_key=2),
            _rate_row(2, 3, price_key=3),
        ],
        {
            empty_set: (),
            provider_set: (1_000_000_003, 1_000_000_004),
        },
    )

    selection = await harness.select(target_count=4)

    assert selection is not None
    assert list(selection.rank_by_key) == [
        ("rate", f"{1:032x}", "CPT", "70553", "FFS", "0"),
        ("rate", f"{2:032x}", "CPT", "70553", "FFS", "0"),
        ("npi", "1000000003", "CPT", "70553", "FFS", "0"),
        ("npi", "1000000004", "CPT", "70553", "FFS", "0"),
    ]
    assert harness.graph_batches == [(empty_set,), (provider_set,)]
    assert harness.provider_memberships == {
        1_000_000_003: (provider_set,),
        1_000_000_004: (provider_set,),
    }
    assert harness.completion_reads == [
        ((1, 2), 3, 0, False)
    ]
    assert selection.providers_by_set[empty_set] == []


_EQUIVALENCE_CASES = (
    (
        [_rate_row(1, 1, price_key=1), _rate_row(2, 2, price_key=2)],
        {
            _provider_set_id(1): (1_000_000_001, 1_000_000_002),
            _provider_set_id(2): (1_000_000_003,),
        },
    ),
    (
        [
            _rate_row(1, 1, price_key=7),
            _rate_row(1, 2, price_key=7, source_key=1),
            _rate_row(2, 3, price_key=7),
            _rate_row(3, 4, price_key=8),
        ],
        {
            _provider_set_id(1): (1_000_000_001,),
            _provider_set_id(2): (1_000_000_001, 1_000_000_002),
            _provider_set_id(3): (),
        },
    ),
    (
        [
            _rate_row(3, 1, price_key=2),
            _rate_row(2, 2, price_key=2),
            _rate_row(1, 3, price_key=1),
        ],
        {
            _provider_set_id(1): (),
            _provider_set_id(2): (1_000_000_005,),
            _provider_set_id(3): (1_000_000_006,),
        },
    ),
)


@pytest.mark.asyncio
@pytest.mark.parametrize("descending", (False, True))
@pytest.mark.parametrize("target_count", range(1, 7))
@pytest.mark.parametrize(
    ("rate_rows", "npis_by_set"),
    _EQUIVALENCE_CASES,
)
async def test_incremental_prefix_matches_exhaustive_cost_order(
    monkeypatch,
    rate_rows,
    npis_by_set,
    target_count,
    descending,
):
    harness = _IncrementalExpansionHarness(
        monkeypatch,
        rate_rows,
        npis_by_set,
    )
    ordered_rows = harness.ordered_rows(descending)
    expected_rank, _, _ = serving._rank_provider_expansion_prefix(
        ordered_rows,
        npis_by_set,
        target_count=target_count,
    )

    selection = await harness.select(
        target_count=target_count,
        descending=descending,
    )

    assert selection is not None
    assert list(selection.rank_by_key) == list(expected_rank)
    assert list(selection.rank_by_key.values()) == list(
        range(len(expected_rank))
    )
    assert harness.rate_reads == [
        (len(rate_rows), 0, descending)
    ]
    graph_provider_set_ids = [
        provider_set_id
        for graph_batch in harness.graph_batches
        for provider_set_id in graph_batch
    ]
    assert graph_provider_set_ids == list(
        dict.fromkeys(graph_provider_set_ids)
    )


@pytest.mark.asyncio
async def test_incremental_fetches_a_second_physical_page_only_on_shortfall(
    monkeypatch,
):
    first_set = _provider_set_id(1)
    second_set = _provider_set_id(2)
    rate_rows = [
        _rate_row(
            1,
            occurrence,
            price_key=occurrence,
            provider_count=1,
        )
        for occurrence in range(1, 65)
    ]
    rate_rows.extend(
        [
            _rate_row(2, 65, price_key=65, provider_count=1),
            *[
                _rate_row(2, occurrence, price_key=occurrence)
                for occurrence in range(66, 71)
            ],
        ]
    )
    harness = _IncrementalExpansionHarness(
        monkeypatch,
        rate_rows,
        {
            first_set: (1_000_000_001,),
            second_set: (1_000_000_002,),
        },
    )

    selection = await harness.select(target_count=2)

    assert selection is not None
    assert selection.total_lower_bound == 2
    assert harness.rate_reads == [
        (64, 0, False),
        (6, 64, False),
    ]
    assert harness.graph_batches == [(first_set,), (second_set,)]
    assert harness.completion_reads == [
        ((1, 2), 64, 0, False),
        ((1, 2), 6, 64, False),
    ]


@pytest.mark.asyncio
async def test_sealed_rate_cap_crossing_fails_before_partial_page(
    monkeypatch,
):
    provider_set_id = _provider_set_id(1)
    harness = _IncrementalExpansionHarness(
        monkeypatch,
        [
            _rate_row(1, 1, price_key=1),
            _rate_row(1, 2, price_key=2),
        ],
        {provider_set_id: ()},
        sealed_cap_overrides={
            "maximum_provider_expansion_rate_rows": 1,
        },
    )

    with pytest.raises(PTG2ManifestArtifactError, match="rate-row cap"):
        await harness.select(target_count=2)

    assert harness.rate_reads == [(1, 0, False)]
    assert harness.graph_batches == [(provider_set_id,)]


@pytest.mark.asyncio
async def test_sealed_provider_set_cap_fails_before_graph_work(
    monkeypatch,
):
    harness = _IncrementalExpansionHarness(
        monkeypatch,
        [
            _rate_row(1, 1, price_key=1),
            _rate_row(2, 2, price_key=2),
        ],
        {
            _provider_set_id(1): (),
            _provider_set_id(2): (),
        },
        sealed_cap_overrides={
            "maximum_provider_expansion_provider_sets": 1,
        },
    )

    with pytest.raises(
        PTG2ManifestArtifactError,
        match="distinct-provider-set cap",
    ):
        await harness.select(target_count=2)

    assert harness.rate_reads == [(2, 0, False)]
    assert harness.graph_batches == []


@pytest.mark.asyncio
async def test_sealed_graph_batch_cap_fails_before_second_batch(
    monkeypatch,
):
    first_set = _provider_set_id(1)
    harness = _IncrementalExpansionHarness(
        monkeypatch,
        [
            _rate_row(
                1,
                1,
                price_key=1,
                provider_count=1,
            ),
            _rate_row(
                1,
                2,
                price_key=2,
                source_key=1,
                provider_count=1,
            ),
            _rate_row(
                2,
                3,
                price_key=3,
                provider_count=1,
            ),
        ],
        {
            first_set: (1_000_000_001,),
            _provider_set_id(2): (1_000_000_002,),
        },
        sealed_cap_overrides={
            "maximum_provider_expansion_graph_batches": 1,
        },
    )

    with pytest.raises(PTG2ManifestArtifactError, match="graph-batch cap"):
        await harness.select(target_count=3)

    assert harness.rate_reads == [(3, 0, False)]
    assert harness.graph_batches == [(first_set,)]


@pytest.mark.asyncio
async def test_provider_count_understatement_fails_before_partial_page(
    monkeypatch,
):
    harness = _IncrementalExpansionHarness(
        monkeypatch,
        [
            _rate_row(
                1,
                1,
                price_key=1,
                provider_count=1,
            )
        ],
        {
            _provider_set_id(1): (
                1_000_000_001,
                1_000_000_002,
            )
        },
    )

    with pytest.raises(
        PTG2ManifestArtifactError,
        match="authenticated provider count",
    ):
        await harness.select(target_count=2)


def test_snapshot_without_sealed_expansion_contract_fails_closed():
    with pytest.raises(
        PTG2ManifestArtifactError,
        match="missing sealed hot-prefix limits",
    ):
        serving._v4_provider_expansion_request_caps(
            _v4_tables(),
            target_count=25,
        )


@pytest.mark.asyncio
async def test_sealed_prefix_target_crossing_fails_before_rate_or_graph_work(
    monkeypatch,
):
    harness = _IncrementalExpansionHarness(
        monkeypatch,
        [_rate_row(1, 1, price_key=1)],
        {_provider_set_id(1): (1_000_000_001,)},
    )

    with pytest.raises(
        PTG2ManifestArtifactError,
        match="sealed hot-prefix target",
    ):
        await harness.select(target_count=202)

    assert harness.rate_reads == []
    assert harness.graph_batches == []
