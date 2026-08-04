# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded V4 exact-NPI billing-association graph tests."""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from api.ptg2_v4_graph import V4GraphRoot
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
async def test_exact_billing_groups_intersect_direct_sets(monkeypatch) -> None:
    intersection = AsyncMock(return_value={3: (7,), 4: (8,)})
    entered_source_scopes: list[int] = []

    @contextmanager
    def source_scope(_hot_limits, provider_set_count):
        entered_source_scopes.append(provider_set_count)
        yield

    monkeypatch.setattr(serving, "_v4_source_scope", source_scope)
    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        AsyncMock(return_value=V4GraphRoot(17, "direct_v1", b"d" * 32)),
    )
    monkeypatch.setattr(serving, "lookup_v4_relation_intersections", intersection)

    assert await serving._v4_exact_groups_by_set(
        object(), _tables(), provider_set_keys=(4, 3), exact_group_keys=(8, 7)
    ) == {3: (7,), 4: (8,)}
    assert entered_source_scopes == [2]
    assert intersection.await_args.kwargs["relation"] == "set_groups_direct"
    assert intersection.await_args.kwargs["allowed_member_keys"] == (7, 8)


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_groups", [{3: (7,)}, {3: (9,), 4: (8,)}])
async def test_exact_billing_groups_reject_invalid_direct_scope(
    monkeypatch, invalid_groups
) -> None:
    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        AsyncMock(return_value=V4GraphRoot(17, "direct_v1", b"d" * 32)),
    )
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_intersections",
        AsyncMock(return_value=invalid_groups),
    )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="incomplete"):
        await serving._v4_exact_groups_by_set(
            object(), _tables(), provider_set_keys=(3, 4), exact_group_keys=(7, 8)
        )


@pytest.mark.asyncio
async def test_exact_billing_groups_intersect_pattern_and_component_sets(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        AsyncMock(return_value=V4GraphRoot(17, "pattern_v1", b"p" * 32)),
    )
    monkeypatch.setattr(
        serving,
        "_load_v4_pattern_set_group_sources",
        AsyncMock(
            return_value=serving._V4SetGroupSources(
                pattern_keys_by_set={3: (10,)}, component_keys_by_set={4: (20,)}
            )
        ),
    )

    async def intersection(*_args, **kwargs):
        return {10: (7,)} if kwargs["relation"] == "pattern_groups" else {20: (8,)}

    intersect = AsyncMock(side_effect=intersection)
    monkeypatch.setattr(serving, "lookup_v4_relation_intersections", intersect)

    assert await serving._v4_exact_groups_by_set(
        object(), _tables(), provider_set_keys=(3, 4), exact_group_keys=(7, 8)
    ) == {3: (7,), 4: (8,)}
    assert [call.kwargs["max_members"] for call in intersect.await_args_list] == [
        8192,
        8191,
    ]


@pytest.mark.asyncio
async def test_exact_billing_groups_reject_incomplete_pattern_source_scope(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        AsyncMock(return_value=V4GraphRoot(17, "pattern_v1", b"p" * 32)),
    )
    monkeypatch.setattr(
        serving,
        "_load_v4_pattern_set_group_sources",
        AsyncMock(
            return_value=serving._V4SetGroupSources(
                pattern_keys_by_set={3: (10,)}, component_keys_by_set={4: (20,)}
            )
        ),
    )

    async def incomplete_intersection(*_args, **kwargs):
        return {10: (7,)} if kwargs["relation"] == "pattern_groups" else {}

    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_intersections",
        AsyncMock(side_effect=incomplete_intersection),
    )

    with pytest.raises(serving.PTG2ManifestArtifactError, match="incomplete"):
        await serving._v4_exact_groups_by_set(
            object(), _tables(), provider_set_keys=(3, 4), exact_group_keys=(7, 8)
        )


@pytest.mark.asyncio
async def test_shared_pattern_fanout_cannot_exceed_association_edge_cap(
    monkeypatch,
) -> None:
    provider_set_keys = tuple(range(1, 2049))
    exact_group_keys = (7, 8, 9, 10, 11)
    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        AsyncMock(return_value=V4GraphRoot(17, "pattern_v1", b"p" * 32)),
    )
    monkeypatch.setattr(
        serving,
        "_load_v4_pattern_set_group_sources",
        AsyncMock(
            return_value=serving._V4SetGroupSources(
                pattern_keys_by_set={key: (30,) for key in provider_set_keys},
                component_keys_by_set={},
            )
        ),
    )
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_intersections",
        AsyncMock(return_value={30: exact_group_keys}),
    )

    with pytest.raises(serving.PTG2ManifestArtifactError, match="edge limit"):
        await serving._v4_exact_groups_by_set(
            object(),
            _tables(),
            provider_set_keys=provider_set_keys,
            exact_group_keys=exact_group_keys,
        )


@pytest.mark.asyncio
async def test_exact_npi_billing_map_keeps_only_witnessed_groups(monkeypatch) -> None:
    first_set, second_set = "11" * 16, "22" * 16
    first_group, second_group = "aa" * 16, "bb" * 16
    monkeypatch.setattr(
        serving,
        "_provider_set_keys_for_ids",
        AsyncMock(return_value={first_set: 3, second_set: 4}),
    )
    exact_groups = AsyncMock(return_value=(first_group, second_group))
    monkeypatch.setattr(serving, "_shared_graph_members_for_id", exact_groups)
    monkeypatch.setattr(
        serving,
        "_shared_provider_group_keys_for_ids",
        AsyncMock(return_value={first_group: 7, second_group: 8}),
    )
    monkeypatch.setattr(
        serving,
        "_v4_exact_groups_by_set",
        AsyncMock(return_value={3: (7,), 4: (8,)}),
    )
    sidecar = AsyncMock(
        return_value={
            first_group: {"provider_group_ref": first_group, "tax_identity_status": "matched_ein"},
            second_group: {"provider_group_ref": second_group, "tax_identity_status": "missing"},
        }
    )
    monkeypatch.setattr(serving, "load_provider_group_billing_associations", sidecar)

    associations_by_set = await serving._exact_npi_billing_associations_by_set(
        object(),
        _tables(),
        npi=1234567890,
        serving_rows=[
            {"provider_set_global_id_128": first_set},
            {"provider_set_global_id_128": second_set},
        ],
    )
    assert associations_by_set[first_set][0]["tax_identity_status"] == "matched_ein"
    assert associations_by_set[second_set][0]["tax_identity_status"] == "missing"
    assert exact_groups.await_args.kwargs["max_members"] == 2048
    assert sidecar.await_args.kwargs["provider_group_refs"] == {first_group, second_group}


@pytest.mark.asyncio
async def test_exact_npi_billing_map_rejects_unknown_set_and_empty_groups(
    monkeypatch,
) -> None:
    provider_set = "11" * 16
    set_dictionary = AsyncMock(return_value={})
    monkeypatch.setattr(serving, "_provider_set_keys_for_ids", set_dictionary)
    with pytest.raises(serving.PTG2ManifestArtifactError, match="provider set"):
        await serving._exact_npi_billing_associations_by_set(
            object(), _tables(), npi=1234567890,
            serving_rows=[{"provider_set_global_id_128": provider_set}],
        )

    set_dictionary.return_value = {provider_set: 3}
    monkeypatch.setattr(serving, "_shared_graph_members_for_id", AsyncMock(return_value=()))
    with pytest.raises(serving.PTG2ManifestArtifactError, match="no provider-group"):
        await serving._exact_npi_billing_associations_by_set(
            object(), _tables(), npi=1234567890,
            serving_rows=[{"provider_set_global_id_128": provider_set}],
        )


@pytest.mark.asyncio
async def test_exact_npi_billing_map_rejects_unknown_group_and_empty_witness(
    monkeypatch,
) -> None:
    provider_set, group_ref = "11" * 16, "aa" * 16
    monkeypatch.setattr(
        serving, "_provider_set_keys_for_ids", AsyncMock(return_value={provider_set: 3})
    )
    monkeypatch.setattr(
        serving, "_shared_graph_members_for_id", AsyncMock(return_value=(group_ref,))
    )
    group_dictionary = AsyncMock(return_value={})
    monkeypatch.setattr(serving, "_shared_provider_group_keys_for_ids", group_dictionary)
    with pytest.raises(serving.PTG2ManifestArtifactError, match="provider group"):
        await serving._exact_npi_billing_associations_by_set(
            object(), _tables(), npi=1234567890,
            serving_rows=[{"provider_set_global_id_128": provider_set}],
        )

    group_dictionary.return_value = {group_ref: 7}
    monkeypatch.setattr(
        serving, "_v4_exact_groups_by_set", AsyncMock(return_value={3: ()})
    )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="witness"):
        await serving._exact_npi_billing_associations_by_set(
            object(), _tables(), npi=1234567890,
            serving_rows=[{"provider_set_global_id_128": provider_set}],
        )


@pytest.mark.asyncio
async def test_exact_npi_billing_provider_set_scope_is_bounded() -> None:
    with pytest.raises(serving.PTG2ManifestArtifactError, match="provider-set limit"):
        await serving._exact_npi_billing_associations_by_set(
            object(),
            _tables(),
            npi=1234567890,
            serving_rows=[
                {"provider_set_global_id_128": f"{ordinal:032x}"}
                for ordinal in range(1, 2050)
            ],
        )


@pytest.mark.asyncio
async def test_exact_billing_provider_sets_require_ids_and_deduplicate(
    monkeypatch,
) -> None:
    with pytest.raises(serving.PTG2ManifestArtifactError, match="unknown provider set"):
        await serving._exact_provider_set_keys_by_id(object(), _tables(), [{}])

    provider_set = "11" * 16
    set_dictionary = AsyncMock(return_value={provider_set: 3})
    monkeypatch.setattr(serving, "_provider_set_keys_for_ids", set_dictionary)
    assert await serving._exact_provider_set_keys_by_id(
        object(),
        _tables(),
        [
            {"provider_set_global_id_128": provider_set},
            {"provider_set_global_id_128": provider_set},
        ],
    ) == {provider_set: 3}
    assert set_dictionary.await_args.args[2] == (provider_set,)


def test_exact_billing_group_dictionary_rejects_duplicate_keys() -> None:
    with pytest.raises(serving.PTG2ManifestArtifactError, match="inconsistent"):
        serving._exact_group_ids_by_set(
            {"11" * 16: 3},
            {"aa" * 16: 7, "bb" * 16: 7},
            {3: (7,)},
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("include_providers", "has_scope", "uses_v4"),
    [
        (False, True, True),
        (True, False, True),
        (True, True, False),
    ],
)
async def test_billing_associations_are_provider_expanded_exact_npi_v4_only(
    monkeypatch, include_providers, has_scope, uses_v4
) -> None:
    resolver = AsyncMock(return_value={"set": ({"status": "resolved"},)})
    monkeypatch.setattr(serving, "_exact_npi_billing_associations_by_set", resolver)
    tables = _tables()
    if not uses_v4:
        tables = serving.PTG2ServingTables(
            arch_version="postgres_binary_v3",
            shared_snapshot_key=17,
            storage_generation="shared_blocks_v3",
            cold_lookup_contract="ptg_v3_cold_v2",
            shared_block_layout="dense_shared_blocks_v3",
            source_count=1,
        )
    scope = serving._ExplicitNpiGraphScope(1234567890, (3,)) if has_scope else None
    assert await serving._billing_associations_for_exact_npi_request(
        object(), tables, include_providers=include_providers,
        explicit_npi_scope=scope, serving_rows=(
            {"provider_set_global_id_128": "11" * 16},
        ),
    ) == {}
    resolver.assert_not_awaited()


@pytest.mark.asyncio
async def test_billing_associations_delegate_for_provider_expanded_exact_npi_request(
    monkeypatch,
) -> None:
    expected_by_set = {"set": ({"status": "resolved"},)}
    resolver = AsyncMock(return_value=expected_by_set)
    monkeypatch.setattr(serving, "_exact_npi_billing_associations_by_set", resolver)
    scope = serving._ExplicitNpiGraphScope(1234567890, (3,))
    serving_rows = ({"provider_set_global_id_128": "11" * 16},)
    assert await serving._billing_associations_for_exact_npi_request(
        object(), _tables(), include_providers=True,
        explicit_npi_scope=scope, serving_rows=serving_rows,
    ) == expected_by_set
    assert resolver.await_args.kwargs == {
        "npi": 1234567890,
        "serving_rows": serving_rows,
    }
