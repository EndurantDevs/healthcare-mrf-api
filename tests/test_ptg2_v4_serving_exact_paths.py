# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from api.ptg2_v4_graph import V4GraphRoot
from tests.ptg2_serving_coverage_paydown_support import FakeResult, FakeSession
from tests.ptg2_v4_provider_prefix_support import sealed_v4_hot_prefix


def _tables() -> serving.PTG2ServingTables:
    return serving.PTG2ServingTables(
        arch_version="postgres_binary_v3",
        shared_snapshot_key=17,
        storage_generation="shared_blocks_v4",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout="packed_snapshot_maps_v4",
        source_count=1,
        provider_graph_v4_hot_prefix=sealed_v4_hot_prefix(),
    )


@pytest.mark.asyncio
async def test_v4_projection_direct_empty_and_budget_guards(monkeypatch) -> None:
    direct_lookup = AsyncMock(return_value={3: (7, 8)})
    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        AsyncMock(return_value=V4GraphRoot(17, "direct_v1", b"d" * 32)),
    )
    monkeypatch.setattr(serving, "lookup_v4_relation_members", direct_lookup)

    assert await serving._v4_members_through_projection(
        object(),
        _tables(),
        owner_keys=(),
        direct_relation="group_sets_direct",
        projection_relation="group_patterns",
        projected_member_relation="pattern_sets",
    ) == {}
    assert await serving._v4_members_through_projection(
        object(),
        _tables(),
        owner_keys=(3, 3),
        direct_relation="group_sets_direct",
        projection_relation="group_patterns",
        projected_member_relation="pattern_sets",
        max_members=2,
    ) == {3: (7, 8)}
    assert direct_lookup.await_args.kwargs["relation"] == "group_sets_direct"
    assert direct_lookup.await_args.kwargs["max_members"] == 2

    with pytest.raises(serving.PTG2ManifestArtifactError, match="non-negative"):
        await serving._v4_members_through_projection(
            object(),
            _tables(),
            owner_keys=(3,),
            direct_relation="group_sets_direct",
            projection_relation="group_patterns",
            projected_member_relation="pattern_sets",
            max_members=-1,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("duplicate_owner_members", [False, True])
async def test_v4_projection_rejects_per_owner_and_aggregate_overflow(
    monkeypatch,
    duplicate_owner_members: bool,
) -> None:
    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        AsyncMock(return_value=V4GraphRoot(17, "pattern_v1", b"p" * 32)),
    )

    async def lookup(*_args, **kwargs):
        if duplicate_owner_members:
            return {7: (10, 11)}
        return {7: (10,), 8: (11,)}

    async def prefix_lookup(*_args, **_kwargs):
        return {1: (7,), 2: ((7,) if duplicate_owner_members else (8,))}

    monkeypatch.setattr(serving, "lookup_v4_relation_members", lookup)
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_member_prefixes",
        prefix_lookup,
    )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="exceeds max_members"):
        await serving._v4_members_through_projection(
            object(),
            _tables(),
            owner_keys=(1, 2),
            direct_relation="set_groups_direct",
            projection_relation="set_patterns",
            projected_member_relation="pattern_groups",
            max_members=1,
        )


def test_v4_reverse_cost_zero_edges_and_single_npi_projection() -> None:
    assert serving._is_v4_reverse_scope_cheaper(
        first_member_count=0,
        allowed_provider_set_count=99,
        forward_member_count=99,
        forward_owner_count=0,
        reverse_member_count=99,
        reverse_owner_count=0,
    )
    assert not serving._is_v4_reverse_scope_cheaper(
        first_member_count=1,
        allowed_provider_set_count=1,
        forward_member_count=1,
        forward_owner_count=0,
        reverse_member_count=1,
        reverse_owner_count=1,
    )
    assert serving._is_v4_reverse_scope_cheaper(
        first_member_count=1,
        allowed_provider_set_count=1,
        forward_member_count=1,
        forward_owner_count=1,
        reverse_member_count=1,
        reverse_owner_count=0,
    )
    assert serving._v4_sets_by_npi_reverse(
        normalized_npis=(1111111111,),
        npi_key_by_value={},
        first_members_by_npi_key={},
        first_members_by_allowed_set={},
        allowed_provider_sets=(3,),
    ) == {1111111111: ()}
    assert serving._v4_sets_by_npi_reverse(
        normalized_npis=(1111111111,),
        npi_key_by_value={1111111111: 4},
        first_members_by_npi_key={4: (7, 8)},
        first_members_by_allowed_set={3: (8,), 5: (9,)},
        allowed_provider_sets=(3, 5),
    ) == {1111111111: (3,)}


@pytest.mark.asyncio
async def test_v4_exact_npi_and_group_directions(monkeypatch) -> None:
    npi_member = serving._ptg2_npi_member_id(1234567890)
    first_group = "11" * 16
    second_group = "22" * 16
    monkeypatch.setattr(
        serving,
        "v4_npi_keys_for_values",
        AsyncMock(return_value={1234567890: 4}),
    )
    relation_lookup = AsyncMock(side_effect=[{4: (7, 8)}, {7: (1,), 8: (2,)}])
    monkeypatch.setattr(serving, "lookup_v4_relation_members", relation_lookup)
    monkeypatch.setattr(
        serving,
        "_shared_provider_group_ids_for_keys",
        AsyncMock(return_value={7: first_group, 8: second_group}),
    )
    monkeypatch.setattr(
        serving,
        "_shared_provider_group_keys_for_ids",
        AsyncMock(return_value={first_group: 7, second_group: 8}),
    )
    monkeypatch.setattr(
        serving,
        "v4_npi_values_for_keys",
        AsyncMock(return_value={1: 1234567890, 2: 2234567890}),
    )

    assert await serving._v4_shared_graph_members_many(
        object(), _tables(), "provider_npi_group", [npi_member], max_members=2
    ) == {npi_member: (first_group, second_group)}
    assert await serving._v4_shared_graph_members_many(
        object(),
        _tables(),
        "provider_group_npi",
        [first_group, second_group],
        max_members=2,
    ) == {
        first_group: (serving._ptg2_npi_member_id(1234567890),),
        second_group: (serving._ptg2_npi_member_id(2234567890),),
    }
    assert [call.kwargs["relation"] for call in relation_lookup.await_args_list] == [
        "npi_groups_exact",
        "group_npis_exact",
    ]


@pytest.mark.asyncio
async def test_v4_exact_directions_fail_closed_for_bad_owners_and_dictionaries(
    monkeypatch,
) -> None:
    with pytest.raises(serving.PTG2ManifestArtifactError, match="owner is malformed"):
        await serving._v4_shared_graph_members_many(
            object(), _tables(), "provider_npi_group", ["not-an-id"], max_members=1
        )

    npi_member = serving._ptg2_npi_member_id(1234567890)
    monkeypatch.setattr(
        serving,
        "v4_npi_keys_for_values",
        AsyncMock(return_value={1234567890: 4}),
    )
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_members",
        AsyncMock(return_value={4: (7,)}),
    )
    monkeypatch.setattr(
        serving,
        "_shared_provider_group_ids_for_keys",
        AsyncMock(return_value={}),
    )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="missing provider-group"):
        await serving._v4_shared_graph_members_many(
            object(), _tables(), "provider_npi_group", [npi_member], max_members=1
        )

    group_id = "33" * 16
    monkeypatch.setattr(
        serving,
        "_shared_provider_group_keys_for_ids",
        AsyncMock(return_value={group_id: 3}),
    )
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_members",
        AsyncMock(return_value={3: (9,)}),
    )
    monkeypatch.setattr(serving, "v4_npi_values_for_keys", AsyncMock(return_value={}))
    with pytest.raises(serving.PTG2ManifestArtifactError, match="missing NPI"):
        await serving._v4_shared_graph_members_many(
            object(), _tables(), "provider_group_npi", [group_id], max_members=1
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("name", "owner_id", "owner_key", "member_key", "expected_member"),
    [
        ("provider_inverted", "44" * 16, 4, 8, "55" * 16),
        ("provider_forward", "66" * 16, 6, 9, "77" * 16),
    ],
)
async def test_v4_factored_group_set_directions(
    monkeypatch,
    name: str,
    owner_id: str,
    owner_key: int,
    member_key: int,
    expected_member: str,
) -> None:
    monkeypatch.setattr(
        serving,
        "_shared_provider_group_keys_for_ids",
        AsyncMock(return_value={owner_id: owner_key}),
    )
    monkeypatch.setattr(
        serving,
        "_provider_set_keys_for_ids",
        AsyncMock(return_value={owner_id: owner_key}),
    )
    projection = AsyncMock(return_value={owner_key: (member_key,)})
    monkeypatch.setattr(serving, "_v4_members_through_projection", projection)
    monkeypatch.setattr(
        serving,
        "_provider_set_ids_for_keys",
        AsyncMock(return_value={member_key: expected_member}),
    )
    monkeypatch.setattr(
        serving,
        "_shared_provider_group_ids_for_keys",
        AsyncMock(return_value={member_key: expected_member}),
    )

    assert await serving._v4_shared_graph_members_many(
        object(), _tables(), name, [owner_id], max_members=1
    ) == {owner_id: (expected_member,)}
    if name == "provider_inverted":
        assert projection.await_args.kwargs["projection_relation"] == "group_patterns"
    else:
        assert projection.await_args.kwargs["projection_relation"] == "set_patterns"


@pytest.mark.asyncio
async def test_v4_factored_directions_enforce_support_and_total_budget(monkeypatch) -> None:
    owner_id = "88" * 16
    monkeypatch.setattr(
        serving,
        "_provider_set_keys_for_ids",
        AsyncMock(return_value={owner_id: 5}),
    )
    monkeypatch.setattr(
        serving,
        "_v4_members_through_projection",
        AsyncMock(return_value={5: (7, 8)}),
    )
    dictionary = AsyncMock(return_value={7: "99" * 16})
    monkeypatch.setattr(serving, "_shared_provider_group_ids_for_keys", dictionary)

    with pytest.raises(serving.PTG2ManifestArtifactError, match="missing support"):
        await serving._v4_shared_graph_members_many(
            object(), _tables(), "provider_forward", [owner_id], max_members=2
        )
    dictionary.return_value = {7: "99" * 16, 8: "aa" * 16}
    with pytest.raises(serving.PTG2ManifestArtifactError, match="exceeds max_members"):
        await serving._v4_shared_graph_members_many(
            object(), _tables(), "provider_forward", [owner_id], max_members=1
        )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="unsupported PTG V4"):
        await serving._v4_shared_graph_members_many(
            object(), _tables(), "unknown", [owner_id], max_members=None
        )


@pytest.mark.asyncio
async def test_v4_set_keys_resolve_group_ids_without_flat_storage(monkeypatch) -> None:
    projection = AsyncMock(return_value={3: (7, 8), 4: (8,)})
    dictionary = AsyncMock(return_value={7: "bb" * 16, 8: "cc" * 16})
    monkeypatch.setattr(serving, "_v4_members_through_projection", projection)
    monkeypatch.setattr(serving, "_shared_provider_group_ids_for_keys", dictionary)

    assert await serving._shared_group_ids_for_set_keys(object(), _tables(), ()) == ()
    assert await serving._shared_group_ids_for_set_keys(
        object(), _tables(), (4, 3, 4)
    ) == ("bb" * 16, "cc" * 16)
    assert projection.await_args.kwargs["projection_relation"] == "set_patterns"

    dictionary.return_value = {7: "bb" * 16}
    with pytest.raises(serving.PTG2ManifestArtifactError, match="missing provider-group"):
        await serving._shared_group_ids_for_set_keys(object(), _tables(), (3,))


@pytest.mark.asyncio
async def test_shared_graph_dispatches_v4_and_rejects_unknown_name(monkeypatch) -> None:
    delegated = AsyncMock(return_value={"owner": ("member",)})
    monkeypatch.setattr(serving, "_v4_shared_graph_members_many", delegated)
    assert await serving._shared_graph_members_many(
        object(), _tables(), "provider_forward", ["owner"], max_members=3
    ) == {"owner": ("member",)}
    assert delegated.await_args.kwargs["max_members"] == 3

    v3_tables = serving.PTG2ServingTables(
        arch_version="postgres_binary_v3",
        shared_snapshot_key=17,
        storage_generation="shared_blocks_v3",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout="dense_shared_blocks_v3",
        source_count=1,
    )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="unsupported PTG2"):
        await serving._shared_graph_members_many(
            object(), v3_tables, "unknown", ["owner"], max_members=None
        )


def test_shared_rate_scope_rejects_malformed_group_id() -> None:
    assert serving._ptg2_build_rate_scope(()) == serving._ManifestRateScope(
        group_ids=(), group_id_bytes=frozenset(), id_count=0
    )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="malformed"):
        serving._ptg2_build_rate_scope(("not-hex",))


@pytest.mark.asyncio
async def test_knn_planner_settings_are_applied_and_restored() -> None:
    session = FakeSession(
        [
            FakeResult(
                [
                    {
                        "plan_cache_mode": "auto",
                        "parallel_workers": "2",
                    }
                ]
            ),
            FakeResult(),
        ]
    )
    prior_settings = await serving._enable_serial_knn_planning(session)
    assert prior_settings == ("auto", "2")
    await serving._restore_knn_planning(session, prior_settings)
    assert len(session.calls) == 2
    assert session.calls[1][0][1] == {
        "plan_cache_mode": "auto",
        "parallel_workers": "2",
    }


@pytest.mark.asyncio
async def test_project_graph_candidates_enriches_and_validates_dictionary(
    monkeypatch,
) -> None:
    candidates = serving._GraphLocationCandidates(
        [
            {"npi": 11, "address_payload": {"first_line": "11 Main"}},
            {"npi": 12, "state": "IL"},
        ],
        {11: {3, 4}, 12: {4}},
    )
    provider_set_dictionary = AsyncMock(
        return_value={3: "31" * 16, 4: "41" * 16}
    )
    monkeypatch.setattr(
        serving, "_provider_set_ids_for_keys", provider_set_dictionary
    )
    monkeypatch.setattr(
        serving,
        "_enriched_provider_rows_for_npis",
        AsyncMock(return_value=[{"npi": 11, "provider_name": "Eleven"}]),
    )
    monkeypatch.setattr(
        serving,
        "_ptg2_address_serving_table",
        AsyncMock(return_value="mrf.npi_address"),
    )

    provider_set_ids, providers_by_set = await serving._project_graph_candidates(
        object(),
        _tables(),
        candidates,
        plan_id="plan-1",
        snapshot_id="snapshot-1",
        source_key=None,
    )
    assert provider_set_ids == {"31" * 16, "41" * 16}
    assert [
        provider_record["npi"]
        for provider_record in providers_by_set["41" * 16]
    ] == [11, 12]
    assert providers_by_set["31" * 16][0]["provider_name"] == "Eleven"

    provider_set_dictionary.return_value = {3: "31" * 16}
    with pytest.raises(serving.PTG2ManifestArtifactError, match="missing provider-set"):
        await serving._project_graph_candidates(
            object(),
            _tables(),
            candidates,
            plan_id="plan-1",
            snapshot_id=None,
            source_key="source-1",
        )


@pytest.mark.asyncio
async def test_v4_explicit_npi_scope_handles_unscoped_and_empty_rate_scope(
    monkeypatch,
) -> None:
    graph_lookup = AsyncMock(return_value={1234567890: (3, 5)})
    monkeypatch.setattr(serving, "_v4_sets_by_npi", graph_lookup)
    assert await serving._version_three_explicit_npi_graph_scope(
        object(), _tables(), {"npi": "1234567890"}
    ) == serving._ExplicitNpiGraphScope(1234567890, (3, 5))
    assert graph_lookup.await_args.kwargs["allowed_provider_set_keys"] is None

    monkeypatch.setattr(
        serving, "_shared_rate_provider_set_keys", AsyncMock(return_value=())
    )
    graph_lookup.reset_mock()
    assert await serving._version_three_explicit_npi_graph_scope(
        object(),
        _tables(),
        {"npi": "1234567890", "plan_id": "plan-1", "code": "70553"},
    ) == serving._ExplicitNpiGraphScope(1234567890, ())
    graph_lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_v4_provider_sets_from_membership_graph_are_dictionary_backed(
    monkeypatch,
) -> None:
    graph_lookup = AsyncMock(return_value={1234567890: (3, 5)})
    dictionary = AsyncMock(return_value={3: "31" * 16, 5: "51" * 16})
    monkeypatch.setattr(serving, "_v4_sets_by_npi", graph_lookup)
    monkeypatch.setattr(serving, "_provider_set_ids_for_keys", dictionary)

    assert await serving._provider_sets_from_membership_graph(
        object(), _tables(), 1234567890
    ) == ("31" * 16, "51" * 16)
    dictionary.return_value = {3: "31" * 16}
    with pytest.raises(serving.PTG2ManifestArtifactError, match="missing provider-set"):
        await serving._provider_sets_from_membership_graph(
            object(), _tables(), 1234567890
        )
