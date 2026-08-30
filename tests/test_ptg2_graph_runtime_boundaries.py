# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed boundary contracts for bounded provider graph traversal."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from tests.ptg2_serving_coverage_paydown_support import strict_v3_tables
from tests.ptg2_v4_provider_prefix_support import sealed_v4_hot_prefix


_PROVIDER_SET_ID = "01" * 16


def _explicit_scope(*provider_set_keys: int) -> serving._ExplicitNpiGraphScope:
    return serving._ExplicitNpiGraphScope(1234567890, provider_set_keys)


def _local_distance_tables():
    return strict_v3_tables(
        storage_generation="shared_blocks_v4",
        shared_block_layout="packed_snapshot_maps_v4",
        provider_graph_v4_hot_prefix=sealed_v4_hot_prefix(),
        provider_graph_v4_inferred_taxonomy_candidates={
            "max_online_inferred_taxonomy_retained_memberships": 10,
            "max_online_candidate_pattern_projection_members": 100,
            "max_online_inferred_taxonomy_graph_pages": 10,
            "max_online_inferred_taxonomy_graph_bytes": 10_000,
            "max_online_inferred_taxonomy_graph_batches": 10,
            "max_online_filtered_reverse_code_occurrences": 10,
            "max_online_filtered_reverse_code_sets": 10,
        },
    )


def _install_local_distance_reads(monkeypatch):
    location_limits: list[int] = []

    async def location_rows(*_args, limit, **_kwargs):
        location_limits.append(limit)
        if len(location_limits) == 1:
            return [{"npi": 1, "_ptg_source_exhausted": False}]
        return [
            {"npi": 1, "_ptg_source_exhausted": False},
            {"npi": 2, "_ptg_source_exhausted": False},
            {"npi": 3, "_ptg_source_exhausted": True},
        ]

    rate_sets = AsyncMock(
        side_effect=lambda *_args, provider_set_keys, **_kwargs: tuple(
            key for key in provider_set_keys if key in {10, 30, 31}
        )
    )
    replacement_by_name = {
        "_version_three_explicit_npi_graph_scope": AsyncMock(return_value=None),
        "_shared_rate_code_scope_rows": AsyncMock(return_value=[{"code_key": 7}]),
        "load_v4_graph_root": AsyncMock(
            return_value=SimpleNamespace(representation="pattern_v1")
        ),
        "_membership_location_rows": location_rows,
        "_v4_sets_by_npi": AsyncMock(
            side_effect=({1: (10,)}, {2: (20,), 3: (30, 31)})
        ),
        "_scoped_rate_provider_set_keys": rate_sets,
        "_graph_location_probe_batch_size": lambda *_args, **_kwargs: 1,
        "_ptg2_manifest_location_match_limit": lambda: 10,
    }
    for name, replacement in replacement_by_name.items():
        monkeypatch.setattr(serving, name, replacement)
    return location_limits, rate_sets


@pytest.mark.asyncio
@pytest.mark.parametrize("location_rows", [None, []])
async def test_paged_graph_candidates_preserve_unavailable_and_empty(
    monkeypatch,
    location_rows,
):
    """Distinguish an unavailable location reader from an exhausted empty one."""

    monkeypatch.setattr(
        serving,
        "_membership_location_rows",
        AsyncMock(return_value=location_rows),
    )

    candidates = await serving._paged_graph_candidates(
        object(),
        strict_v3_tables(),
        {},
        frozenset({7}),
        1,
    )

    if location_rows is None:
        assert candidates is None
    else:
        assert candidates == serving._GraphLocationCandidates([], {})


@pytest.mark.asyncio
async def test_paged_graph_candidates_return_proven_prefix(monkeypatch):
    """Return immediately once the ordered graph prefix proves the target."""

    monkeypatch.setattr(
        serving,
        "_membership_location_rows",
        AsyncMock(return_value=[{"npi": 1234567890}]),
    )
    enough = AsyncMock(return_value=True)
    monkeypatch.setattr(
        serving._GraphLocationProbeState,
        "has_enough_after_append",
        enough,
    )

    candidates = await serving._paged_graph_candidates(
        object(),
        strict_v3_tables(),
        {},
        frozenset({7}),
        1,
    )

    assert candidates == serving._GraphLocationCandidates([], {})
    enough.assert_awaited_once()


@pytest.mark.asyncio
async def test_paged_graph_candidates_stop_at_proven_source_exhaustion(
    monkeypatch,
):
    """Return collected candidates when the address source proves exhaustion."""

    monkeypatch.setattr(
        serving,
        "_membership_location_rows",
        AsyncMock(
            return_value=[
                {"npi": 1234567890, "_ptg_source_exhausted": True}
            ]
        ),
    )
    monkeypatch.setattr(
        serving._GraphLocationProbeState,
        "has_enough_after_append",
        AsyncMock(return_value=False),
    )

    candidates = await serving._paged_graph_candidates(
        object(),
        strict_v3_tables(),
        {},
        frozenset({7}),
        2,
    )

    assert candidates == serving._GraphLocationCandidates([], {})


@pytest.mark.asyncio
async def test_paged_graph_candidates_expand_after_empty_unexhausted_knn_probe(
    monkeypatch,
):
    """Do not mistake an unsupported nearest prefix for source exhaustion."""

    location_limits: list[int] = []

    async def location_rows(*_args, limit, **_kwargs):
        location_limits.append(limit)
        if len(location_limits) == 1:
            return [
                {
                    "_ptg_probe_empty": True,
                    "_ptg_source_exhausted": False,
                }
            ]
        return [{"npi": 1234567890, "_ptg_source_exhausted": True}]

    monkeypatch.setattr(serving, "_membership_location_rows", location_rows)
    monkeypatch.setattr(
        serving,
        "_shared_provider_set_keys_by_npi",
        AsyncMock(return_value={1234567890: {7}}),
    )

    candidates = await serving._paged_graph_candidates(
        object(),
        strict_v3_tables(),
        {},
        frozenset({7}),
        1,
    )

    assert candidates == serving._GraphLocationCandidates(
        [{"npi": 1234567890, "_ptg_source_exhausted": True}],
        {1234567890: {7}},
    )
    assert location_limits == [64, 256]


@pytest.mark.asyncio
async def test_paged_graph_candidates_reject_unproven_exhaustive_expansion(
    monkeypatch,
):
    """Fail closed after the first bounded exhaustive probe remains unproven."""

    monkeypatch.setattr(
        serving,
        "_membership_location_rows",
        AsyncMock(
            return_value=[
                {"npi": 1234567890, "_ptg_source_exhausted": False}
            ]
        ),
    )
    monkeypatch.setattr(
        serving._GraphLocationProbeState,
        "has_enough_after_append",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(
        serving, "_graph_location_probe_batch_size", lambda *_args, **_kwargs: 1
    )
    monkeypatch.setattr(serving, "_ptg2_manifest_location_match_limit", lambda: 1)

    with pytest.raises(
        serving.PTG2LocationScopeError,
        match="Narrow the ZIP radius",
    ) as raised:
        await serving._paged_graph_candidates(
            object(),
            strict_v3_tables(),
            {},
            frozenset({7}),
            2,
        )

    serving._membership_location_rows.assert_awaited_once()
    assert raised.value.error_code == "ptg2_location_scope_too_broad"


def test_graph_probe_state_preserves_unproven_bound_error():
    """Keep the legacy fail-closed error for ordinary bounded traversal."""

    with pytest.raises(
        serving.PTG2ManifestArtifactError,
        match="configured exactness bound",
    ):
        serving._GraphLocationProbeState().raise_unproven_bound()


@pytest.mark.asyncio
async def test_graph_rate_scope_distinguishes_broad_empty_and_unavailable(
    monkeypatch,
):
    """Route broad traversal and preserve exact-NPI empty/unavailable states."""

    broad_result = serving._GraphLocationCandidates([{"npi": 1}], {1: {7}})
    broad_lookup = AsyncMock(return_value=broad_result)
    monkeypatch.setattr(serving, "_graph_location_candidates", broad_lookup)

    broad = await serving._graph_candidates_for_rate_scope(
        object(), strict_v3_tables(), {}, frozenset({7}), 1, None
    )
    empty = await serving._graph_candidates_for_rate_scope(
        object(), strict_v3_tables(), {}, frozenset({8}), 1, _explicit_scope(7)
    )
    monkeypatch.setattr(
        serving,
        "_membership_location_rows",
        AsyncMock(return_value=None),
    )
    unavailable = await serving._graph_candidates_for_rate_scope(
        object(), strict_v3_tables(), {}, frozenset({7}), 1, _explicit_scope(7)
    )

    assert broad is broad_result
    assert empty == serving._GraphLocationCandidates([], {})
    assert unavailable is None


@pytest.mark.asyncio
async def test_graph_request_short_circuits_proven_empty_scopes(monkeypatch):
    """Avoid rate I/O for empty exact-NPI, caller, and code scopes."""

    rate_sets = AsyncMock(return_value=[])
    monkeypatch.setattr(serving, "_shared_rate_provider_set_keys", rate_sets)

    empty_exact = await serving._graph_candidates_for_request(
        object(),
        strict_v3_tables(),
        {},
        requested_code="00001",
        requested_system="CPT",
        plan_id="synthetic-plan",
        candidate_limit=1,
        explicit_npi_scope=_explicit_scope(),
    )
    empty_intersection = await serving._graph_candidates_for_request(
        object(),
        strict_v3_tables(),
        {},
        requested_code="00001",
        requested_system="CPT",
        plan_id="synthetic-plan",
        candidate_limit=1,
        provider_set_keys=(8,),
        explicit_npi_scope=_explicit_scope(7),
    )
    empty_rate = await serving._graph_candidates_for_request(
        object(),
        strict_v3_tables(),
        {},
        requested_code="00001",
        requested_system="CPT",
        plan_id="synthetic-plan",
        candidate_limit=1,
        explicit_npi_scope=_explicit_scope(7),
    )

    expected_empty = serving._GraphLocationCandidates([], {})
    assert empty_exact == expected_empty
    assert empty_intersection == expected_empty
    assert empty_rate == expected_empty
    rate_sets.assert_awaited_once()


@pytest.mark.asyncio
async def test_inferred_distance_intersects_only_nearby_v4_sets(monkeypatch):
    """Grow the ordered taxonomy prefix without materializing a national code scope."""

    location_limits, rate_sets = _install_local_distance_reads(monkeypatch)

    candidates = await serving._graph_candidates_for_request(
        object(),
        _local_distance_tables(),
        {
            "code": "73721",
            "code_system": "CPT",
            "lat": 41.9,
            "long": -87.65,
        },
        requested_code="73721",
        requested_system="CPT",
        plan_id="synthetic-plan",
        candidate_limit=2,
    )

    assert candidates == serving._GraphLocationCandidates(
        [
            {"npi": 1, "_ptg_source_exhausted": False},
            {"npi": 3, "_ptg_source_exhausted": True},
        ],
        {1: {10}, 3: {30, 31}},
        taxonomy_filtered=True,
    )
    assert location_limits == [1, 3]
    assert [
        call.kwargs["provider_set_keys"] for call in rate_sets.await_args_list
    ] == [(10,), (20, 30, 31)]


def test_local_distance_rejects_an_unstable_prefix():
    state = serving._LocalDistanceGraphState()
    assert serving._new_local_taxonomy_locations(
        [{"npi": 1}, {"npi": 2}], state
    ) == [{"npi": 1}, {"npi": 2}]
    with pytest.raises(serving.PTG2ManifestArtifactError, match="prefix changed"):
        serving._new_local_taxonomy_locations([{"npi": 1}, {"npi": 3}], state)


@pytest.mark.asyncio
async def test_local_distance_rejects_an_unproven_exactness_bound(monkeypatch):
    monkeypatch.setattr(
        serving,
        "_membership_location_rows",
        AsyncMock(return_value=[{"npi": 1, "_ptg_source_exhausted": False}]),
    )
    monkeypatch.setattr(
        serving, "_local_v4_memberships", AsyncMock(return_value={1: ()})
    )
    monkeypatch.setattr(serving, "_classify_local_code_sets", AsyncMock())
    request = serving._LocalDistanceGraphRequest(
        1, [{"code_key": 7}], serving._v4_geo_rate_forward_limits(_local_distance_tables())
    )

    with pytest.raises(serving.PTG2ManifestArtifactError, match="exactness bound"):
        await serving._scan_local_distance_graph(
            object(), _local_distance_tables(), {}, request, 1, 1
        )


@pytest.mark.asyncio
async def test_local_distance_rejects_code_sets_outside_the_requested_scope(monkeypatch):
    request = serving._LocalDistanceGraphRequest(
        1, [{"code_key": 7}], serving._v4_geo_rate_forward_limits(_local_distance_tables())
    )
    monkeypatch.setattr(
        serving, "_scoped_rate_provider_set_keys", AsyncMock(return_value=(99,))
    )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="escaped"):
        await serving._classify_local_code_sets(
            object(), _local_distance_tables(), {1: (10,)}, request,
            serving._LocalDistanceGraphState(),
        )


@pytest.mark.asyncio
async def test_graph_location_matches_preserve_invalid_unavailable_and_empty(
    monkeypatch,
):
    """Keep invalid selectors, unavailable graph state, and exact emptiness distinct."""

    assert await serving._graph_location_matches(
        object(), strict_v3_tables(), {"code": "00001"}, candidate_limit=1, plan_id=""
    ) is None
    assert await serving._graph_location_matches(
        object(), strict_v3_tables(), {}, candidate_limit=1, plan_id="synthetic-plan"
    ) is None
    candidates = AsyncMock(
        side_effect=(None, serving._GraphLocationCandidates([], {}))
    )
    monkeypatch.setattr(serving, "_graph_candidates_for_request", candidates)

    unavailable = await serving._graph_location_matches(
        object(),
        strict_v3_tables(),
        {"code_system": "CPT", "code": "00001"},
        candidate_limit=1,
        plan_id="synthetic-plan",
    )
    empty = await serving._graph_location_matches(
        object(),
        strict_v3_tables(),
        {"code_system": "CPT", "code": "00001"},
        candidate_limit=1,
        plan_id="synthetic-plan",
    )

    assert unavailable is None
    assert empty == (set(), {})


@pytest.mark.asyncio
async def test_manifest_location_window_is_bounded_and_exhaustive(monkeypatch):
    """Bound requested pages and prove exhaustive cardinality before returning."""

    monkeypatch.setattr(serving, "_ptg2_manifest_location_match_limit", lambda: 2)
    graph_matches = AsyncMock(return_value=None)
    monkeypatch.setattr(serving, "_graph_location_matches", graph_matches)

    assert await serving._ptg2_manifest_location_provider_matches(
        object(), strict_v3_tables(), {}, require_exhaustive=True
    ) is None
    assert graph_matches.await_args.kwargs["candidate_limit"] == 3

    graph_matches.return_value = ({_PROVIDER_SET_ID}, {_PROVIDER_SET_ID: [{"npi": 1}]})
    bounded = await serving._ptg2_manifest_location_provider_matches(
        object(), strict_v3_tables(), {}, candidate_limit=1
    )
    assert bounded == graph_matches.return_value
    assert graph_matches.await_args.kwargs["candidate_limit"] == 1

    with pytest.raises(serving.PTG2ManifestArtifactError, match="pagination exceeds"):
        await serving._ptg2_manifest_location_provider_matches(
            object(), strict_v3_tables(), {}, candidate_limit=3
        )

    graph_matches.return_value = (
        {_PROVIDER_SET_ID},
        {_PROVIDER_SET_ID: [{"npi": 1}, {"npi": 2}, {"npi": 3}]},
    )
    coverage = await serving._ptg2_manifest_location_provider_matches(
        object(),
        strict_v3_tables(),
        {},
        require_exhaustive=True,
        require_provider_set_coverage=True,
    )
    assert coverage == graph_matches.return_value
    assert graph_matches.await_args.kwargs["require_provider_set_coverage"] is True

    with pytest.raises(serving.PTG2ManifestArtifactError, match="traversal reached"):
        await serving._ptg2_manifest_location_provider_matches(
            object(), strict_v3_tables(), {}, require_exhaustive=True
        )
