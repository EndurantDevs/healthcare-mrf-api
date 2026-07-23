from __future__ import annotations

from tests.ptg2_v4_orchestration_support import (
    AsyncMock,
    OrderedDict,
    SimpleNamespace,
    V4GraphRoot,
    _tables,
    pytest,
    serving,
)

async def _assert_empty_npi_layout(monkeypatch) -> None:
    assert await serving._v4_sets_by_npi(object(), _tables(), ()) == {}
    first_lookup = AsyncMock(return_value={1: ()})
    monkeypatch.setattr(
        serving,
        "v4_npi_keys_for_values",
        AsyncMock(return_value={1_111_111_111: 1}),
    )
    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        AsyncMock(return_value=V4GraphRoot(17, "pattern_v1", b"p" * 32)),
    )
    monkeypatch.setattr(serving, "lookup_v4_relation_members", first_lookup)
    assert await serving._v4_sets_by_npi(
        object(),
        _tables(),
        (1_111_111_111, 2_222_222_222),
    ) == {1_111_111_111: (), 2_222_222_222: ()}


async def _assert_pattern_npi_layout(monkeypatch) -> None:
    pattern_calls: list[str] = []

    async def pattern_lookup(*_args, **kwargs):
        relation = kwargs["relation"]
        pattern_calls.append(relation)
        if relation == "npi_patterns":
            return {1: (7,), 2: (8,)}
        if relation == "set_patterns":
            return {10: (7,), 11: (8,), 12: (9,)}
        raise AssertionError(relation)

    monkeypatch.setattr(
        serving,
        "v4_npi_keys_for_values",
        AsyncMock(
            return_value={
                1_111_111_111: 1,
                2_222_222_222: 2,
            }
        ),
    )
    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        AsyncMock(return_value=V4GraphRoot(17, "pattern_v1", b"p" * 32)),
    )
    monkeypatch.setattr(serving, "lookup_v4_relation_members", pattern_lookup)
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_member_prefixes",
        pattern_lookup,
    )

    async def pattern_manifest(*_args, **kwargs):
        if kwargs["relation"] == "pattern_sets":
            return SimpleNamespace(logical_member_count=100, owner_count=1)
        return SimpleNamespace(logical_member_count=3, owner_count=100)

    monkeypatch.setattr(
        serving,
        "load_v4_relation_manifest",
        pattern_manifest,
    )
    pattern_result = await serving._v4_sets_by_npi(
        object(),
        _tables(),
        (3_333_333_333, 2_222_222_222, 1_111_111_111),
        allowed_provider_set_keys=frozenset({10, 11, 12}),
    )
    assert pattern_result == {
        1_111_111_111: (10,),
        2_222_222_222: (11,),
        3_333_333_333: (),
    }
    assert pattern_calls == ["npi_patterns", "set_patterns"]


async def _assert_direct_npi_layout(monkeypatch) -> None:
    direct_calls: list[str] = []

    async def direct_lookup(*_args, **kwargs):
        relation = kwargs["relation"]
        direct_calls.append(relation)
        if relation == "npi_groups_exact":
            return {1: (7, 8)}
        if relation == "group_sets_direct":
            return {7: (10, 12), 8: (11, 12)}
        raise AssertionError(relation)

    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        AsyncMock(return_value=V4GraphRoot(17, "direct_v1", b"d" * 32)),
    )
    monkeypatch.setattr(
        serving,
        "v4_npi_keys_for_values",
        AsyncMock(return_value={1_111_111_111: 1}),
    )
    monkeypatch.setattr(serving, "lookup_v4_relation_members", direct_lookup)

    async def direct_manifest(*_args, **kwargs):
        if kwargs["relation"] == "group_sets_direct":
            return SimpleNamespace(logical_member_count=2, owner_count=100)
        return SimpleNamespace(logical_member_count=100, owner_count=1)

    monkeypatch.setattr(serving, "load_v4_relation_manifest", direct_manifest)
    direct_result = await serving._v4_sets_by_npi(
        object(),
        _tables(),
        (1_111_111_111, 2_222_222_222),
        allowed_provider_set_keys=frozenset({10, 11}),
    )
    assert direct_result == {
        1_111_111_111: (10, 11),
        2_222_222_222: (),
    }
    assert direct_calls == ["npi_groups_exact", "group_sets_direct"]


@pytest.mark.asyncio
async def test_v4_sets_by_npi_layouts(
    monkeypatch,
) -> None:
    """Resolve empty, pattern, reverse, and direct NPI membership layouts."""
    await _assert_empty_npi_layout(monkeypatch)
    await _assert_pattern_npi_layout(monkeypatch)
    await _assert_direct_npi_layout(monkeypatch)


@pytest.mark.asyncio
async def test_v4_sets_by_npi_unscoped_pattern_projection(monkeypatch) -> None:
    async def lookup(*_args, **kwargs):
        if kwargs["relation"] == "npi_patterns":
            return {1: (7, 8), 2: (8,)}
        if kwargs["relation"] == "pattern_sets":
            return {7: (10,), 8: (11, 12)}
        raise AssertionError(kwargs["relation"])

    monkeypatch.setattr(
        serving,
        "v4_npi_keys_for_values",
        AsyncMock(
            return_value={
                1_111_111_111: 1,
                2_222_222_222: 2,
            }
        ),
    )
    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        AsyncMock(return_value=V4GraphRoot(17, "pattern_v1", b"p" * 32)),
    )
    monkeypatch.setattr(serving, "lookup_v4_relation_members", lookup)
    assert await serving._v4_sets_by_npi(
        object(),
        _tables(),
        (1_111_111_111, 2_222_222_222),
    ) == {
        1_111_111_111: (10, 11, 12),
        2_222_222_222: (11, 12),
    }


async def _assert_shared_rate_provider_scope(monkeypatch) -> None:
    monkeypatch.setattr(
        serving,
        "_v4_sets_by_npi",
        AsyncMock(
            return_value={
                1_111_111_111: (7, 8),
                2_222_222_222: (),
            }
        ),
    )
    assert await serving._shared_provider_set_keys_by_npi(
        object(),
        _tables(),
        (),
        (7,),
    ) == {}
    assert await serving._shared_provider_set_keys_by_npi(
        object(),
        _tables(),
        (1_111_111_111,),
        (),
    ) == {}
    assert await serving._shared_provider_set_keys_by_npi(
        object(),
        _tables(),
        (1_111_111_111, 2_222_222_222),
        (7, 8),
    ) == {1_111_111_111: {7, 8}}


async def _assert_explicit_npi_rate_scopes(monkeypatch) -> None:
    monkeypatch.setattr(serving, "_require_strict_shared_v3", lambda _tables: None)
    assert await serving._version_three_explicit_npi_graph_scope(
        object(),
        _tables(),
        {"npi": None},
    ) is None

    monkeypatch.setattr(
        serving,
        "_ptg2_manifest_plan_code_values",
        lambda _args: ("plan", "CPT", "70553"),
    )
    monkeypatch.setattr(
        serving,
        "_shared_rate_provider_set_keys",
        AsyncMock(return_value=()),
    )
    empty_scope = await serving._version_three_explicit_npi_graph_scope(
        object(),
        _tables(),
        {"npi": 1_111_111_111, "market_type": "commercial"},
    )
    assert empty_scope == serving._ExplicitNpiGraphScope(1_111_111_111, ())

    serving._shared_rate_provider_set_keys.return_value = (7, 8)
    serving._v4_sets_by_npi.return_value = {1_111_111_111: (8,)}
    scoped = await serving._version_three_explicit_npi_graph_scope(
        object(),
        _tables(),
        {"npi": 1_111_111_111, "plan_market_type": "employer"},
    )
    assert scoped == serving._ExplicitNpiGraphScope(1_111_111_111, (8,))
    assert serving._v4_sets_by_npi.await_args.kwargs[
        "allowed_provider_set_keys"
    ] == frozenset({7, 8})

    monkeypatch.setattr(
        serving,
        "_ptg2_manifest_plan_code_values",
        lambda _args: None,
    )
    serving._v4_sets_by_npi.return_value = {1_111_111_111: (7,)}
    unscoped = await serving._version_three_explicit_npi_graph_scope(
        object(),
        _tables(),
        {"npi": 1_111_111_111},
    )
    assert unscoped == serving._ExplicitNpiGraphScope(1_111_111_111, (7,))
    assert serving._v4_sets_by_npi.await_args.kwargs[
        "allowed_provider_set_keys"
    ] is None


@pytest.mark.asyncio
async def test_v4_rate_scope_and_explicit_npi_orchestration(monkeypatch) -> None:
    """Apply rate-set scope to explicit NPI graph membership resolution."""
    await _assert_shared_rate_provider_scope(monkeypatch)
    await _assert_explicit_npi_rate_scopes(monkeypatch)


async def _assert_provider_set_dictionary(monkeypatch) -> None:
    monkeypatch.setattr(serving, "_require_strict_shared_v3", lambda _tables: None)
    monkeypatch.setattr(
        serving,
        "_v4_sets_by_npi",
        AsyncMock(return_value={1_111_111_111: (7, 8)}),
    )
    monkeypatch.setattr(
        serving,
        "_provider_set_ids_for_keys",
        AsyncMock(return_value={7: "77" * 16, 8: "88" * 16}),
    )
    assert await serving._provider_sets_from_membership_graph(
        object(),
        _tables(),
        1_111_111_111,
    ) == ("77" * 16, "88" * 16)
    serving._provider_set_ids_for_keys.return_value = {7: "77" * 16}
    with pytest.raises(serving.PTG2ManifestArtifactError, match="missing provider-set"):
        await serving._provider_sets_from_membership_graph(
            object(),
            _tables(),
            1_111_111_111,
        )


def _assert_selected_npi_cache_eviction(monkeypatch) -> None:
    monkeypatch.setattr(
        serving,
        "_PTG2_PROVIDER_SET_IDS_BY_NPI_CACHE",
        OrderedDict(),
    )
    monkeypatch.setattr(
        serving,
        "_PTG2_PROVIDER_NPI_PREFIX_CACHE_MAX_ENTRIES",
        2,
    )
    serving._cache_provider_set_ids_for_npis(
        17,
        {
            1: ("one",),
            2: ("two",),
            3: ("three",),
        },
    )
    cached, uncached = serving._cached_provider_set_ids_for_npis(
        17,
        (1, 2, 3, 4),
    )
    assert cached == {2: ("two",), 3: ("three",)}
    assert uncached == (1, 4)


async def _assert_selected_npi_cache_resolution(monkeypatch) -> None:
    monkeypatch.setattr(
        serving,
        "_PTG2_PROVIDER_SET_IDS_BY_NPI_CACHE",
        OrderedDict(),
    )
    assert await serving._provider_set_ids_for_selected_npis(
        object(),
        _tables(),
        (),
    ) == {}
    serving._v4_sets_by_npi.return_value = {
        1_111_111_111: (7,),
        2_222_222_222: (),
    }
    serving._provider_set_ids_for_keys.return_value = {7: "77" * 16}
    resolved = await serving._provider_set_ids_for_selected_npis(
        object(),
        _tables(),
        (1_111_111_111, 2_222_222_222),
    )
    assert resolved == {
        1_111_111_111: ("77" * 16,),
        2_222_222_222: (),
    }
    assert await serving._provider_set_ids_for_selected_npis(
        object(),
        _tables(),
        (1_111_111_111, 2_222_222_222),
    ) == resolved

    monkeypatch.setattr(
        serving,
        "_PTG2_PROVIDER_SET_IDS_BY_NPI_CACHE",
        OrderedDict(),
    )
    serving._provider_set_ids_for_keys.return_value = {}
    with pytest.raises(serving.PTG2ManifestArtifactError, match="missing provider-set"):
        await serving._provider_set_ids_for_selected_npis(
            object(),
            _tables(),
            (1_111_111_111,),
        )


@pytest.mark.asyncio
async def test_v4_provider_set_dictionary_and_selected_npi_cache(
    monkeypatch,
) -> None:
    """Resolve provider-set IDs and cache selected NPI membership entries."""
    await _assert_provider_set_dictionary(monkeypatch)
    _assert_selected_npi_cache_eviction(monkeypatch)
    await _assert_selected_npi_cache_resolution(monkeypatch)
