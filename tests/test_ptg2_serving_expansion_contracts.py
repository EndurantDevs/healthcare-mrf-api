"""Behavioral contracts for bounded PTG provider expansion."""

from types import SimpleNamespace

import pytest

from api import ptg2_serving as serving


def _rate_row(**overrides):
    row_by_field = {
        "provider_set_global_id_128": "01" * 16,
        "serving_content_hash_128": "02" * 16,
        "reported_code_system": "CPT",
        "reported_code": "99213",
        "negotiation_arrangement": "ffs",
        "_ptg_provider_set_key": 1,
    }
    row_by_field.update(overrides)
    return row_by_field


def test_limited_graph_and_price_filters_stop_at_exact_boundaries():
    """Deduplication and filtering cannot overrun a requested provider prefix."""

    member_ids_by_set = {"set-a": ["already"], "set-b": []}
    seen_ids_by_set = {"set-a": {"already"}, "set-b": set()}
    serving._collect_limited_graph_batch(
        ("group-a", "group-b"),
        {
            "group-a": ("already", "new", "extra"),
            "group-b": ("already", "second"),
        },
        {"group-a": ["set-a", "set-b"], "group-b": ["set-b"]},
        member_ids_by_set,
        seen_ids_by_set,
        2,
    )
    assert member_ids_by_set == {
        "set-a": ["already", "new"],
        "set-b": ["already", "new"],
    }

    price_by_field = {
        "service_code": ["11"],
        "billing_code_modifier": ["25"],
        "negotiated_rate": "10.00",
    }
    assert not serving._is_price_filter_match(
        price_by_field,
        {"modifier": "26"},
    )
    assert not serving._is_price_filter_match(
        {"service_code": ["11"], "billing_code_modifier": ["25"]},
        {"rate": "10.00"},
    )
    prices = [price_by_field]
    assert serving._ptg2_manifest_filter_prices(prices, {}) is prices


def test_manifest_limit_configuration_uses_safe_defaults(monkeypatch):
    """Malformed environment values never create unbounded online work."""

    readers_by_environment_variable = {
        "HLTHPRT_PTG2_MANIFEST_LOCATION_MATCH_LIMIT": (
            serving._ptg2_manifest_location_match_limit,
            5000,
        ),
        "HLTHPRT_PTG2_MANIFEST_LOCATION_CANDIDATE_MULTIPLIER": (
            serving._ptg2_manifest_location_candidate_multiplier,
            2,
        ),
        "HLTHPRT_PTG2_MANIFEST_LOCATION_CANDIDATE_OVERFETCH_CAP": (
            serving._location_candidate_overfetch_cap,
            100,
        ),
        "HLTHPRT_PTG2_MANIFEST_LOCATION_CANDIDATE_FLOOR": (
            serving._ptg2_manifest_location_candidate_floor,
            100,
        ),
        "HLTHPRT_PTG2_MANIFEST_SQL_RATE_SCOPE_MAX_IDS": (
            serving._ptg2_sql_scope_limit,
            10000,
        ),
    }
    for variable_name, (reader, default) in (
        readers_by_environment_variable.items()
    ):
        monkeypatch.setenv(variable_name, "malformed")
        assert reader() == default

    assert (
        serving._membership_geo_sql(
            {"lat": "north", "long": "east"},
            uses_unified_addresses=False,
            parameter_map={},
        )
        is None
    )
    assert serving._graph_location_probe_batch_size(
        candidate_limit=200,
        taxonomy_filter_requested=True,
    ) == 2000


@pytest.mark.asyncio
async def test_empty_hydration_paths_avoid_io():
    """Empty hydration inputs return exact empty mappings and tuples."""

    serving_tables = SimpleNamespace(
        uses_shared_blocks=True,
        shared_snapshot_key=7,
    )
    assert await serving._version_three_prices_by_key(
        object(),
        serving_tables,
        (),
    ) == {}
    assert await serving._prices_for_price_sets(
        object(),
        serving_tables,
        (),
    ) == {}
    assert await serving._taxonomy_rows_for_npis(object(), ()) == {}
    assert await serving._filter_npis_by_taxonomy(
        object(),
        {},
        (),
        limit=5,
    ) == ()


def test_filter_fallbacks_and_cache_eviction_are_bounded(monkeypatch):
    """Fallback ordering and prefix caches retain their online work bounds."""

    assert serving._ptg2_row_address_key({"address_payload": {}}) is None
    assert serving._inferred_provider_taxonomy_rule({"code": "99213"}) is None
    assert serving._manifest_response_row_order_for_direction(
        {"price_key": 9},
        descending=True,
    )[0] == -9
    assert serving._next_version_three_code_batch_size(None) is None

    monkeypatch.setattr(
        serving,
        "resolve_provider_specialty_filter",
        lambda _args: SimpleNamespace(active=True),
    )
    assert serving._is_ptg2_provider_filter_requested({"specialty": "synthetic"})
    monkeypatch.setattr(
        serving,
        "provider_sex_exists_sql",
        lambda *_args, **_kwargs: "provider_sex_match",
    )
    monkeypatch.setattr(
        serving,
        "resolve_provider_specialty_filter",
        lambda _args: SimpleNamespace(active=False),
    )
    assert serving._membership_taxonomy_filters({}, {})[0] == "provider_sex_match"

    state = serving._V4NpiPrefixState.for_provider_sets(("01" * 16,))
    prefix_round = serving._V4GroupPrefixRound(
        {"01" * 16: ()},
        {"01" * 16: True},
    )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="exceeds"):
        serving._mark_v4_prefix_completion(
            prefix_round,
            state,
            {"01" * 16: 1},
        )

    serving_tables = SimpleNamespace(
        uses_shared_blocks=True,
        shared_snapshot_key=7,
    )
    monkeypatch.setattr(
        serving,
        "_PTG2_PROVIDER_NPI_PREFIX_CACHE_MAX_ENTRIES",
        0,
    )
    serving._PTG2_PROVIDER_NPI_PREFIX_CACHE.clear()
    serving._cache_provider_npi_prefix(
        serving_tables,
        "01" * 16,
        1,
        (1000000001,),
        is_complete=True,
    )
    assert not serving._PTG2_PROVIDER_NPI_PREFIX_CACHE


def test_provider_expansion_identity_and_budget_errors_fail_before_io():
    """Malformed rate identities and exhausted budgets reject before graph reads."""

    with pytest.raises(serving.PTG2ManifestArtifactError, match="occurrence"):
        serving._provider_expansion_key({}, npi=None)
    with pytest.raises(serving.PTG2ManifestArtifactError, match="provider-set identity"):
        serving._rank_provider_expansion_prefix([{}], {}, target_count=1)
    assert (
        serving._filtered_provider_prefix_cache_key(
            SimpleNamespace(shared_snapshot_key=None),
            "01" * 16,
            {},
            1,
        )
        is None
    )
    for raw_count, message in (("bad", "invalid"), (-1, "negative")):
        with pytest.raises(serving.PTG2ManifestArtifactError, match=message):
            serving._rate_row_provider_count({"provider_count": raw_count})

    caps = serving._V4ProviderExpansionRequestCaps(
        rate_page_rows=1,
        maximum_rate_rows=1,
        maximum_provider_sets=1,
        maximum_graph_batches=1,
    )
    budget = serving._V4ProviderExpansionBudget(caps)
    with pytest.raises(serving.PTG2ManifestArtifactError, match="empty or repeated"):
        budget.charge_provider_set_batch(())
    with pytest.raises(serving.PTG2ManifestArtifactError, match="no provider sets"):
        budget.charge_completion_provider_sets(())


def test_incremental_completion_rejects_membership_and_identity_drift():
    """Ranked memberships remain inside their exact CPT provider-set scope."""

    state = serving._IncrementalProviderExpansionState(target_count=1)
    state.selected_npis[1000000001] = None
    state.npis_by_set["01" * 16] = (1000000001,)
    with pytest.raises(serving.PTG2ManifestArtifactError, match="ranked membership"):
        serving._validate_incremental_completion_memberships(
            state,
            {1000000001: ()},
        )

    state.selected_provider_set_ids["01" * 16] = None
    state.row_data.append(_rate_row(_ptg_provider_set_key=1))
    with pytest.raises(serving.PTG2ManifestArtifactError, match="absent"):
        serving._incremental_completion_set_union(
            state,
            {},
            frozenset({2}),
        )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="inconsistent"):
        serving._incremental_completion_set_union(
            state,
            {1: "03" * 16},
            frozenset({1}),
        )

    with pytest.raises(serving.PTG2ManifestArtifactError, match="provider-set key"):
        serving._v4_rate_scope_set_ids(
            [_rate_row(_ptg_provider_set_key="bad")],
            None,
        )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="provider-set key"):
        serving._v4_pattern_candidate_prefix(
            [_rate_row(_ptg_provider_set_key=None)],
            {},
            {},
            target_count=1,
        )
