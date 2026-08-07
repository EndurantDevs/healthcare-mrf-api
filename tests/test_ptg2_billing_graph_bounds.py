# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded exact billing graph projection tests."""

from unittest.mock import AsyncMock

import pytest

from api import ptg2_db_sidecars as sidecars
from api import ptg2_serving as serving
from api import ptg2_shared_blocks as shared_blocks
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_manifest_artifacts import ManifestReadLimitError

GROUP_REF = "aa" * 16


def _tables() -> PTG2ServingTables:
    return PTG2ServingTables(
        snapshot_id="ptg2:synthetic",
        arch_version="postgres_binary_v3",
        shared_snapshot_key=17,
        storage_generation="shared_blocks_v4",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout="packed_snapshot_maps_v4",
        source_count=2,
    )


@pytest.mark.asyncio
async def test_shared_graph_projection_cap_is_forwarded(monkeypatch) -> None:
    graph_read = AsyncMock(return_value={GROUP_REF: ()})
    monkeypatch.setattr(
        serving,
        "_shared_graph_members_many",
        graph_read,
    )
    assert await serving._shared_graph_members_by_id(
        object(),
        _tables(),
        "provider_group_npi",
        (GROUP_REF,),
        max_members=5,
        max_projection_members=9,
    ) == {GROUP_REF: ()}
    assert graph_read.await_args.kwargs == {
        "max_members": 5,
        "max_projection_members": 9,
    }


@pytest.mark.asyncio
async def test_manifest_group_lookup_forwards_member_cap(monkeypatch) -> None:
    graph_read = AsyncMock(return_value={GROUP_REF: ()})
    monkeypatch.setattr(serving, "_shared_graph_members_by_id", graph_read)
    session = object()
    serving_tables = _tables()

    assert await serving._manifest_sets_by_group(
        session,
        serving_tables,
        (GROUP_REF,),
        max_members=7,
    ) == {GROUP_REF: ()}
    graph_read.assert_awaited_once_with(
        session,
        serving_tables,
        "provider_inverted",
        (GROUP_REF,),
        max_members=7,
    )


def test_v4_projection_cap_applies_without_per_owner_cap() -> None:
    graph_read, read_options = serving._v4_group_npi_lookup(3, None, 9)

    assert graph_read is serving.lookup_v4_relation_members
    assert read_options == {"max_members": 9}


@pytest.mark.asyncio
async def test_legacy_projection_cap_rejects_combined_owner_fanout(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        serving,
        "_legacy_graph_owner_keys",
        AsyncMock(return_value={GROUP_REF: 1, "bb" * 16: 2}),
    )
    graph_read = AsyncMock(
        side_effect=ManifestReadLimitError(
            "shared PTG graph selection exceeds max_total_members"
        )
    )
    monkeypatch.setattr(
        serving,
        "lookup_shared_graph_members_from_db",
        graph_read,
    )
    member_id_lookup = AsyncMock()
    monkeypatch.setattr(serving, "_legacy_graph_member_ids", member_id_lookup)

    with pytest.raises(ManifestReadLimitError, match="max_total_members"):
        await serving._legacy_shared_graph_members_many(
            object(),
            _tables(),
            "provider_group_npi",
            [GROUP_REF, "bb" * 16],
            max_members=2,
            max_projection_members=2,
        )

    assert graph_read.await_args.kwargs == {
        "schema_name": serving.PTG2_SCHEMA,
        "max_members": 2,
        "max_total_members": 2,
    }
    member_id_lookup.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_limit", (-1, True, 1.5, "1"))
async def test_legacy_projection_cap_is_strict_before_owner_lookup(
    monkeypatch,
    invalid_limit,
) -> None:
    owner_lookup = AsyncMock()
    monkeypatch.setattr(serving, "_legacy_graph_owner_keys", owner_lookup)

    with pytest.raises(ValueError, match="non-negative integer"):
        await serving._legacy_shared_graph_members_many(
            object(),
            _tables(),
            "provider_group_npi",
            [GROUP_REF],
            max_members=2,
            max_projection_members=invalid_limit,
        )
    owner_lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_aggregate_cap_stops_before_graph_payload_fetch(
    monkeypatch,
) -> None:
    owner_records = AsyncMock(
        return_value=(
            {
                "owner_key": 1,
                "first_chunk": 0,
                "member_offset": 0,
                "member_count": 2,
                "selected_member_count": 2,
            },
            {
                "owner_key": 2,
                "first_chunk": 1,
                "member_offset": 0,
                "member_count": 1,
                "selected_member_count": 1,
            },
        )
    )
    payload_fetch = AsyncMock()
    monkeypatch.setattr(
        shared_blocks,
        "_shared_graph_owner_records",
        owner_records,
    )
    monkeypatch.setattr(shared_blocks, "fetch_shared_blocks", payload_fetch)

    with pytest.raises(
        shared_blocks.SharedGraphReadLimitError,
        match="max_total_members",
    ):
        await shared_blocks.fetch_shared_graph_members(
            object(),
            schema_name="mrf",
            snapshot_key=17,
            direction=shared_blocks.PTG2_V3_GRAPH_GROUP_TO_NPI,
            owner_keys=(1, 2),
            max_members=2,
            max_total_members=2,
        )

    owner_records.assert_awaited_once()
    payload_fetch.assert_not_awaited()


def test_graph_total_cap_accepts_equality_after_per_owner_prefixing() -> None:
    request = shared_blocks._shared_graph_read_request(
        schema_name="mrf",
        snapshot_key=17,
        direction=shared_blocks.PTG2_V3_GRAPH_GROUP_TO_NPI,
        owner_keys=(1, 2),
        max_members=2,
        max_total_members=3,
    )

    selection = shared_blocks._validated_graph_owner_selection(
        request,
        (
            {
                "owner_key": 1,
                "first_chunk": 0,
                "member_offset": 0,
                "member_count": 5,
                "selected_member_count": 2,
            },
            {
                "owner_key": 2,
                "first_chunk": 0,
                "member_offset": 16,
                "member_count": 1,
                "selected_member_count": 1,
            },
        ),
        maximum_raw_bytes=None,
    )

    assert selection.required_chunk_keys == {0}


def test_graph_zero_total_cap_accepts_only_empty_selected_projection() -> None:
    request = shared_blocks._shared_graph_read_request(
        schema_name="mrf",
        snapshot_key=17,
        direction=shared_blocks.PTG2_V3_GRAPH_GROUP_TO_NPI,
        owner_keys=(1,),
        max_members=0,
        max_total_members=0,
    )

    selection = shared_blocks._validated_graph_owner_selection(
        request,
        (
            {
                "owner_key": 1,
                "first_chunk": 0,
                "member_offset": 0,
                "member_count": 5,
                "selected_member_count": 0,
            },
        ),
        maximum_raw_bytes=None,
    )

    assert selection.required_chunk_keys == set()


@pytest.mark.asyncio
async def test_graph_wrapper_forwards_aggregate_cap(monkeypatch) -> None:
    graph_fetch = AsyncMock(return_value={1: (10,)})
    monkeypatch.setattr(sidecars, "fetch_shared_graph_members", graph_fetch)

    assert await sidecars.lookup_shared_graph_members_from_db(
        object(),
        17,
        shared_blocks.PTG2_V3_GRAPH_GROUP_TO_NPI,
        (1,),
        max_members=2,
        max_total_members=3,
    ) == {1: (10,)}
    assert graph_fetch.await_args.kwargs["max_members"] == 2
    assert graph_fetch.await_args.kwargs["max_total_members"] == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_limit", (-1, True, 1.5, "1"))
async def test_graph_total_cap_rejects_invalid_values_before_io(
    invalid_limit,
) -> None:
    session = AsyncMock()

    with pytest.raises(ValueError, match="non-negative integer"):
        await shared_blocks.fetch_shared_graph_members(
            session,
            schema_name="mrf",
            snapshot_key=17,
            direction=shared_blocks.PTG2_V3_GRAPH_GROUP_TO_NPI,
            owner_keys=(1,),
            max_total_members=invalid_limit,
        )
    session.execute.assert_not_awaited()
