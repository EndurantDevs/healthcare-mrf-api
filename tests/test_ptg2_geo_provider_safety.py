# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed budgets for geographic provider expansion."""

from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from tests import test_ptg2_geo_provider_expansion as geo
from tests.test_ptg2_geo_rate_prefix import _production_tables


_OTHER_PROVIDER_SET_ID = "22" * 16
_THIRD_PROVIDER_SET_ID = "33" * 16
_MEMBER_NPI = 1234567891


def _claimed_budget(tables):
    budget = serving._geo_rate_selection_budget(tables)
    budget.claim_rate_page(1)
    assert budget.claim_provider_sets({geo._PROVIDER_SET_ID: 1}) == 1
    return budget


def _completion_request(tables, budget, *, filtered_npis_by_set=None):
    rate_row = geo._rate_row(geo._PROVIDER_SET_ID, 1, 1)
    return serving._GeoProviderCompletionRequest(
        code_rows=[{"code_key": 7, "rate_count": 2}],
        serving_rows=[rate_row],
        selected_npis=(_MEMBER_NPI,),
        filtered_npis_by_set=(
            filtered_npis_by_set
            or {geo._PROVIDER_SET_ID: (_MEMBER_NPI,)}
        ),
        source_trace_set_hash=None,
        network_names=[],
        descending=False,
        is_source_exhausted=False,
        budget=budget,
        forward_limits=serving._v4_geo_rate_forward_limits(tables),
    )


@pytest.mark.asyncio
async def test_strict_geo_cost_respects_sealed_target(monkeypatch):
    """Reject oversized pagination before reading a geographic rate prefix."""

    merge_rows = AsyncMock()
    monkeypatch.setattr(serving, "_merge_manifest_code_variant_rows", merge_rows)
    tables = geo._geo_tables()

    with pytest.raises(serving.PTG2ManifestArtifactError, match="hot-prefix target"):
        await geo._select_geo(
            tables,
            rate_count=1,
            target_count=serving._v4_hot_prefix_limits(tables).target + 1,
            descending=False,
        )

    merge_rows.assert_not_awaited()


@pytest.mark.asyncio
async def test_strict_geo_cost_rejects_short_authenticated_rate_page(monkeypatch):
    """A declared rate row cannot disappear behind false exhaustion."""

    rate_row = geo._rate_row(geo._PROVIDER_SET_ID, 1, 1, 0)
    merge_rows = AsyncMock(side_effect=[[rate_row], []])
    monkeypatch.setattr(serving, "_merge_manifest_code_variant_rows", merge_rows)
    monkeypatch.setattr(
        serving,
        "_provider_npis_for_sets",
        AsyncMock(return_value={geo._PROVIDER_SET_ID: ()}),
    )
    monkeypatch.setattr(serving, "_membership_location_rows", AsyncMock(return_value=[]))

    with pytest.raises(serving.PTG2ManifestArtifactError, match="incomplete"):
        await geo._select_geo(
            geo._geo_tables(), rate_count=2, target_count=2, descending=False
        )

    assert merge_rows.await_count == 2


@pytest.mark.asyncio
async def test_geo_completion_respects_provider_set_cap(monkeypatch):
    """Reject completion sets before an unbounded rate read can start."""

    tables = geo._geo_tables(
        max_online_provider_expansion_provider_sets=1,
        max_online_provider_expansion_graph_batches=3,
    )
    request = _completion_request(tables, _claimed_budget(tables))
    monkeypatch.setattr(
        serving,
        "_provider_set_ids_for_keys",
        AsyncMock(
            return_value={
                1: geo._PROVIDER_SET_ID,
                2: _OTHER_PROVIDER_SET_ID,
                3: _THIRD_PROVIDER_SET_ID,
            }
        ),
    )

    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded):
        await serving._geo_completion_memberships(
            object(),
            tables,
            request,
            {_MEMBER_NPI: (1, 2, 3)},
            (1, 2, 3),
        )


@pytest.mark.asyncio
async def test_geo_completion_preflights_graph_batch(monkeypatch):
    """Do not begin reverse completion after its graph budget is spent."""

    tables = geo._geo_tables(max_online_provider_expansion_graph_batches=1)
    request = _completion_request(tables, _claimed_budget(tables))
    reverse_memberships = AsyncMock()
    monkeypatch.setattr(
        serving, "_v4_direct_npi_memberships", reverse_memberships
    )

    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded):
        await serving._geo_completion_provider_set_keys(object(), tables, request)

    reverse_memberships.assert_not_awaited()


@pytest.mark.asyncio
async def test_geo_completion_requires_every_ranked_membership(monkeypatch):
    """A duplicate NPI must retain every ranked rate-set membership."""

    tables = geo._geo_tables()
    request = _completion_request(
        tables,
        _claimed_budget(tables),
        filtered_npis_by_set={
            geo._PROVIDER_SET_ID: (_MEMBER_NPI,),
            _OTHER_PROVIDER_SET_ID: (_MEMBER_NPI,),
        },
    )
    monkeypatch.setattr(
        serving,
        "_provider_set_ids_for_keys",
        AsyncMock(return_value={1: geo._PROVIDER_SET_ID}),
    )

    with pytest.raises(serving.PTG2ManifestArtifactError, match="ranked membership"):
        await serving._geo_completion_memberships(
            object(), tables, request, {_MEMBER_NPI: (1,)}, (1,)
        )


@pytest.mark.asyncio
async def test_geo_completion_accounts_for_bounded_completion_rows(monkeypatch):
    """Charge completion rows against the same sealed forward budget."""

    tables = geo._geo_tables(max_online_provider_expansion_rate_rows=2)
    request = _completion_request(tables, _claimed_budget(tables))
    completion = AsyncMock(
        return_value=(request.serving_rows * 2, {1: geo._PROVIDER_SET_ID})
    )
    monkeypatch.setattr(serving, "_v4_pattern_completion_rows", completion)

    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded):
        await serving._geo_completion_rate_rows(
            object(), tables, request, (1,), {1: geo._PROVIDER_SET_ID}
        )

    completion_request = completion.await_args.args[2]
    assert completion_request.candidate_provider_set_keys == (1,)
    assert completion_request.maximum_occurrences == 2
    assert completion_request.scan_budget is request.forward_limits.scan_budget


@pytest.mark.asyncio
async def test_geo_matcher_fails_closed_on_unusable_location_rows(monkeypatch):
    """Reject unavailable or out-of-scope geo evidence and skip blank NPIs."""

    location_rows = AsyncMock(
        side_effect=[None, [{"npi": None}], [{"npi": _MEMBER_NPI + 1}]]
    )
    monkeypatch.setattr(serving, "_membership_location_rows", location_rows)
    tables = geo._geo_tables()
    provider_sets_by_npi = {_MEMBER_NPI: [geo._PROVIDER_SET_ID]}

    assert await serving._geo_matched_npis_by_set(
        object(), tables, {}, provider_sets_by_npi, {}
    ) is None
    assert await serving._geo_matched_npis_by_set(
        object(), tables, {}, provider_sets_by_npi, {}
    ) == {geo._PROVIDER_SET_ID: []}
    with pytest.raises(serving.PTG2ManifestArtifactError, match="escaped"):
        await serving._geo_matched_npis_by_set(
            object(), tables, {}, provider_sets_by_npi, {}
        )


@pytest.mark.asyncio
async def test_geo_batch_loader_propagates_unavailable_location_rows(monkeypatch):
    """Return unavailable without accepting an unverified provider set."""

    tables = geo._geo_tables()
    budget = serving._geo_rate_selection_budget(tables)
    monkeypatch.setattr(
        serving,
        "_exact_geo_rate_member_npis",
        AsyncMock(return_value={geo._PROVIDER_SET_ID: (_MEMBER_NPI,)}),
    )
    monkeypatch.setattr(
        serving, "_geo_matched_npis_by_set", AsyncMock(return_value=None)
    )

    loaded = await serving._is_geo_provider_expansion_batch_loaded(
        object(),
        tables,
        [geo._rate_row(geo._PROVIDER_SET_ID, 1, 1)],
        {"zip5": "60611"},
        budget,
        {},
        {},
    )

    assert loaded is False


@pytest.mark.asyncio
async def test_filtered_provider_prefix_handles_uncached_and_evicted_results(
    monkeypatch,
):
    """Return exact filtered NPIs with or without an eligible cache slot."""

    tables = geo._geo_tables()
    monkeypatch.setattr(
        serving,
        "_provider_npis_for_sets",
        AsyncMock(return_value={geo._PROVIDER_SET_ID: ()}),
    )
    monkeypatch.setattr(
        serving, "_filter_npis_by_taxonomy", AsyncMock(return_value=())
    )
    monkeypatch.setattr(
        serving, "_filtered_provider_prefix_cache_key", lambda *_args: None
    )
    assert await serving._filtered_provider_npis_for_expansion_set(
        object(), tables, geo._PROVIDER_SET_ID, {}, target_count=1
    ) == ()

    monkeypatch.setattr(
        serving,
        "_filtered_provider_prefix_cache_key",
        lambda *_args: (1, geo._PROVIDER_SET_ID, 1, "{}"),
    )
    monkeypatch.setattr(serving, "_PTG2_PROVIDER_NPI_PREFIX_CACHE_MAX_ENTRIES", 0)
    serving._PTG2_FILTERED_PROVIDER_PREFIX_CACHE.clear()
    assert await serving._filtered_provider_npis_for_expansion_set(
        object(), tables, geo._PROVIDER_SET_ID, {}, target_count=1
    ) == ()
    assert serving._PTG2_FILTERED_PROVIDER_PREFIX_CACHE == {}


def test_geo_batch_rejects_malformed_or_over_budget_rows():
    """Fail before graph work when an ordered set batch is not authentic."""

    tables = geo._geo_tables()
    with pytest.raises(serving.PTG2ManifestArtifactError, match="identity"):
        serving._next_geo_provider_expansion_batch(
            [{"provider_count": 1}], 0, serving._geo_rate_selection_budget(tables),
            maximum_possible_results=None,
        )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="provider count"):
        serving._next_geo_provider_expansion_batch(
            [{"provider_set_global_id_128": geo._PROVIDER_SET_ID}],
            0,
            serving._geo_rate_selection_budget(tables),
            maximum_possible_results=None,
        )
    known_budget = serving._geo_rate_selection_budget(tables)
    known_budget.provider_counts_by_id[geo._PROVIDER_SET_ID] = 2
    with pytest.raises(serving.PTG2ManifestArtifactError, match="disagree"):
        serving._next_geo_provider_expansion_batch(
            [geo._rate_row(geo._PROVIDER_SET_ID, 1, 1)], 0, known_budget,
            maximum_possible_results=None,
        )
    duplicate_rows = [
        geo._rate_row(geo._PROVIDER_SET_ID, 1, 1, provider_count)
        for provider_count in (1, 2)
    ]
    with pytest.raises(serving.PTG2ManifestArtifactError, match="disagree"):
        serving._next_geo_provider_expansion_batch(
            duplicate_rows, 0, serving._geo_rate_selection_budget(tables),
            maximum_possible_results=None,
        )
    full_budget = serving._geo_rate_selection_budget(tables)
    full_budget.candidate_members = full_budget.maximum_candidate_members
    with pytest.raises(serving.PTG2LocationScopeError):
        serving._next_geo_provider_expansion_batch(
            [geo._rate_row(geo._PROVIDER_SET_ID, 1, 1)], 0, full_budget,
            maximum_possible_results=None,
        )


@pytest.mark.asyncio
async def test_geo_completion_rejects_missing_graph_identities(monkeypatch):
    """Reject empty reverse membership and incomplete set-key resolution."""

    tables = geo._geo_tables()
    request = _completion_request(tables, _claimed_budget(tables))
    monkeypatch.setattr(
        serving,
        "_v4_direct_npi_memberships",
        AsyncMock(return_value={_MEMBER_NPI: ()}),
    )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="no provider sets"):
        await serving._geo_completion_provider_set_keys(object(), tables, request)

    monkeypatch.setattr(
        serving, "_provider_set_ids_for_keys", AsyncMock(return_value={})
    )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="unknown provider set"):
        await serving._geo_completion_memberships(
            object(), tables, request, {_MEMBER_NPI: (1,)}, (1,)
        )


@pytest.mark.asyncio
async def test_geo_completion_rejects_mismatched_rate_scope(monkeypatch):
    """Bind completion rows to the authenticated provider-code scope."""

    tables = geo._geo_tables()
    request = _completion_request(tables, _claimed_budget(tables))
    monkeypatch.setattr(
        serving,
        "_v4_pattern_completion_rows",
        AsyncMock(return_value=([], {2: _OTHER_PROVIDER_SET_ID})),
    )

    with pytest.raises(serving.PTG2ManifestArtifactError, match="provider-code scope"):
        await serving._geo_completion_rate_rows(
            object(), tables, request, (1,), {1: geo._PROVIDER_SET_ID}
        )


@pytest.mark.asyncio
async def test_geo_ranker_rejects_unsealed_set_and_closes_price_boundary():
    """Never load an unsealed geo set; stop after the completed price tie."""

    tables = geo._geo_tables()
    missing_set_request = serving._FilteredProviderExpansionRequest(
        row_data=[],
        args={},
        target_count=1,
        npis_by_set={},
        geo_budget=serving._geo_rate_selection_budget(tables),
    )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="sealed set batch"):
        await serving._provider_expansion_npis_for_row(
            object(), tables, missing_set_request,
            geo._rate_row(geo._PROVIDER_SET_ID, 1, 1),
        )

    boundary_request = serving._FilteredProviderExpansionRequest(
        row_data=[
            geo._rate_row(geo._PROVIDER_SET_ID, 1, 1),
            geo._rate_row(_OTHER_PROVIDER_SET_ID, 2, 2),
        ],
        args={},
        target_count=1,
        npis_by_set={geo._PROVIDER_SET_ID: (_MEMBER_NPI,)},
        complete_price_key_boundary=True,
    )
    rank_by_key, selected_npis, selected_sets = (
        await serving._rank_filtered_provider_expansion_prefix(
            object(), tables, boundary_request
        )
    )
    assert len(rank_by_key) == 1
    assert selected_npis == (_MEMBER_NPI,)
    assert selected_sets == (geo._PROVIDER_SET_ID,)


@pytest.mark.asyncio
@pytest.mark.parametrize("descending", [False, True])
async def test_strict_geo_sparse_scan_stays_within_graph_batch_cap(
    monkeypatch,
    descending,
):
    """Geometric widening keeps a 256-set sparse scan within its batch cap."""

    provider_set_ids = tuple(f"{number:032x}" for number in range(1, 257))
    npis_by_set = {
        provider_set_id: (1234567800 + index,)
        for index, provider_set_id in enumerate(provider_set_ids, start=1)
    }
    rate_rows = [
        geo._rate_row(provider_set_id, index, index)
        for index, provider_set_id in enumerate(provider_set_ids, start=1)
    ]
    _merge_rows, member_rows = geo._install_rate_scan(
        monkeypatch,
        rate_rows,
        npis_by_set,
        {provider_npis[0]: 1.0 for provider_npis in npis_by_set.values()},
    )
    monkeypatch.setattr(serving, "_membership_location_rows", AsyncMock(return_value=[]))

    selection = await geo._select_geo(
        _production_tables(),
        rate_count=len(rate_rows),
        target_count=2,
        descending=descending,
    )

    assert selection is not None and selection.exhausted is True
    assert selection.rank_by_key == {}
    assert member_rows.await_count < 64


@pytest.mark.asyncio
async def test_strict_geo_equal_price_boundary_batches_graph_reads(monkeypatch):
    """One wide equal-price continuation consumes one sealed graph batch."""

    provider_set_ids = tuple(f"{number:032x}" for number in range(1, 65))
    npis_by_set = {
        provider_set_id: (1234567000 + index,)
        for index, provider_set_id in enumerate(provider_set_ids, start=1)
    }
    rate_rows = [
        geo._rate_row(provider_set_id, index, 1)
        for index, provider_set_id in enumerate(provider_set_ids, start=1)
    ]
    _merge_rows, member_rows = geo._install_rate_scan(
        monkeypatch,
        rate_rows,
        npis_by_set,
        {provider_npis[0]: 1.0 for provider_npis in npis_by_set.values()},
    )
    geo._install_geo_completion(
        monkeypatch,
        provider_set_keys_by_npi={
            provider_npis[0]: (index,)
            for index, provider_npis in enumerate(npis_by_set.values(), start=1)
        },
        provider_set_id_by_key=dict(enumerate(provider_set_ids, start=1)),
        completion_rows=rate_rows,
    )
    geo._install_provider_enrichment(monkeypatch)

    selection = await geo._select_geo(
        geo._geo_tables(
            provider_expansion_rate_page_rows=64,
            max_online_provider_expansion_rate_rows=64,
            max_online_provider_expansion_provider_sets=64,
            max_online_provider_expansion_graph_batches=3,
        ),
        rate_count=len(rate_rows),
        target_count=1,
        descending=False,
    )

    assert selection is not None and selection.exhausted is True
    assert len(selection.rank_by_key) == len(rate_rows)
    assert member_rows.await_count == 2
