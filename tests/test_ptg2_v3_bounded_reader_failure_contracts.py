from __future__ import annotations

from unittest.mock import AsyncMock, Mock

import pytest

from api import ptg2_db_serving_v3, ptg2_db_sidecars, ptg2_serving

from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
)

from api.ptg2_db_sidecars import (
    ForwardReadBudget,
    PTG2ManifestArtifactError,
    lookup_code_prefix_rows_from_db,
)

from api.ptg2_shared_blocks import (
    PTG2_V3_GRAPH_NPI_TO_GROUP,
    SharedBlockPayload,
    fetch_shared_graph_members,
    stream_shared_blocks,
)

from process.ptg_parts.ptg2_shared_blocks import shared_block_hash

from process.ptg_parts import ptg2_serving_binary_v3

from process.ptg_parts.ptg2_serving_binary_v3_types import (
    PTG2V3PriceAtomRecord,
)

from tests.test_ptg2_v3_bounded_readers import (
    _AsyncRows,
    _Rows,
    _Session,
    _StreamingSession,
    _aliased_forward_fragments,
    _assert_prefix_reference_reads,
    _assert_two_code_sparse_rows,
    _capture_forward_fanout,
    _dense_forward_fragment,
    _eager_ranked_prefix,
    _fragment,
    _fragment_row,
    _grouped_payload,
    _install_forward_fragment_reader,
    _patch_reference_lookups,
    _prefix_rows,
    _shard_block_key,
    _source_vector,
    _stored_row,
    _two_code_sparse_block_keys,
    _two_code_sparse_fragments,
    _uvarint,
)

@pytest.mark.asyncio
async def test_same_provider_continuation_rejects_decreasing_occurrences(
    monkeypatch,
):
    fragments = (
        _fragment(
            _grouped_payload(2, [(5, [(8, 1)])]),
            fragment_no=0,
            entry_count=1,
        ),
        _fragment(
            _grouped_payload(2, [(5, [(7, 0)])]),
            fragment_no=1,
            entry_count=1,
        ),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="not ordered"):
        await _prefix_rows(
            monkeypatch,
            fragments=fragments,
            limit=1,
            descending=False,
        )

@pytest.mark.asyncio
async def test_bounded_code_prefix_validates_later_fragments_after_heap_is_full(
    monkeypatch,
):
    fragments = (
        _fragment(
            _grouped_payload(2, [(3, [(0, 0)])]),
            fragment_no=0,
            entry_count=1,
        ),
        _fragment(
            _grouped_payload(2, [(5, [(1, 1)])]) + b"\x00",
            fragment_no=1,
            entry_count=1,
        ),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="trailing bytes"):
        await _prefix_rows(
            monkeypatch,
            fragments=fragments,
            limit=1,
            descending=False,
        )

@pytest.mark.asyncio
async def test_bounded_code_prefix_validates_unretained_price_keys(monkeypatch):
    fragments = (
        _fragment(
            _grouped_payload(2, [(3, [(0, 0)]), (5, [(99, 1)])]),
            entry_count=2,
        ),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="price key is out of range"):
        await _prefix_rows(
            monkeypatch,
            fragments=fragments,
            limit=1,
            descending=False,
            item_count=2,
        )

@pytest.mark.asyncio
async def test_bounded_code_prefix_charges_physical_work_across_reads(
    monkeypatch,
):
    fragment = _fragment(
        _grouped_payload(2, [(3, [(0, 0)]), (5, [(1, 1)])]),
        entry_count=2,
    )
    scan_budget = ForwardReadBudget(
        maximum_fragments=1,
        maximum_raw_payload_bytes=len(fragment.payload) * 2,
    )

    prefix_rows, _provider_counts, _dictionary = await _prefix_rows(
        monkeypatch,
        fragments=(fragment,),
        limit=1,
        descending=False,
        scan_budget=scan_budget,
    )
    assert len(prefix_rows) == 1
    assert scan_budget.fragment_count == 1
    assert scan_budget.raw_payload_bytes == len(fragment.payload)

    with pytest.raises(
        ptg2_db_sidecars.ForwardReadBudgetExceeded,
        match="physical scan budget",
    ):
        await _prefix_rows(
            monkeypatch,
            fragments=(fragment,),
            limit=1,
            descending=False,
            scan_budget=scan_budget,
        )

@pytest.mark.parametrize(
    "budget_values",
    ((True, 1), (1, True), (0, 1), (1, 0)),
)
def test_forward_read_budget_requires_positive_integer_limits(budget_values):
    with pytest.raises(PTG2ManifestArtifactError, match="budget is invalid"):
        ForwardReadBudget(*budget_values)

@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [0, -1, True])
async def test_bounded_code_prefix_requires_positive_integer_limit(limit):
    with pytest.raises(ValueError, match="limit must be positive"):
        await lookup_code_prefix_rows_from_db(
            object(),
            7,
            limit=limit,
            shared_snapshot_key=41,
            source_count=1,
            price_dictionary_item_count=1,
            price_dictionary_block_bytes=16,
        )

@pytest.mark.asyncio
async def test_code_shard_discovery_uses_exact_mapping_ranges():
    block_7_0 = _shard_block_key(7, 5)
    block_7_1 = _shard_block_key(7, 1025)
    block_9_0 = _shard_block_key(9, 5)
    session = _Session(
        [
            {"code_key": 7, "block_key": block_7_0},
            {"code_key": 7, "block_key": block_7_1},
            {"code_key": 9, "block_key": block_9_0},
        ]
    )

    keys_by_code = await ptg2_db_sidecars._discover_forward_shard_keys(
        session,
        shared_snapshot_key=41,
        schema_name="mrf",
        code_keys=(9, 7),
    )

    assert keys_by_code == {
        7: (block_7_0, block_7_1),
        9: (block_9_0,),
    }
    sql, params = session.calls[0]
    assert "mapping.block_key >=" in sql
    assert "mapping.block_key <" in sql
    assert "requested_code.code_key * :code_block_span" in sql
    assert params["object_kind"] == "by_code_provider_shard_v1"
    assert params["code_keys"] == (7, 9)
    assert params["code_block_span"] == 1 << 31
    assert params["shared_projection_generations"] == (
        "shared_blocks_v3",
        "shared_blocks_v4",
    )

@pytest.mark.asyncio
async def test_code_shard_discovery_rejects_unreachable_provider_shard():
    invalid_shard = (7 << 31) | 3_000_000
    session = _Session([{"code_key": 7, "block_key": invalid_shard}])

    with pytest.raises(PTG2ManifestArtifactError, match="invalid shard number"):
        await ptg2_db_sidecars._discover_forward_shard_keys(
            session,
            shared_snapshot_key=41,
            schema_name="mrf",
            code_keys=(7,),
        )

@pytest.mark.asyncio
async def test_code_existence_uses_provider_shard_range():
    session = _Session([True])

    exists = await ptg2_db_sidecars.has_serving_binary_code_block(
        session,
        7,
        shared_snapshot_key=41,
        schema_name="mrf",
    )

    assert exists is True
    sql, params = session.calls[0]
    assert "SELECT EXISTS" in sql
    assert "mapping.block_key >= :lower_bound" in sql
    assert "mapping.block_key < :upper_bound" in sql
    assert params["object_kind"] == "by_code_provider_shard_v1"
    assert params["lower_bound"] == 7 << 31
    assert params["upper_bound"] == 8 << 31
    assert params["shared_projection_generations"] == (
        "shared_blocks_v3",
        "shared_blocks_v4",
    )

@pytest.mark.asyncio
async def test_shared_block_stream_uses_server_side_iteration():
    stream_row = _stored_row(
        object_kind="by_code_provider_shard_v1",
        block_key=_shard_block_key(7, 0),
        fragment_no=0,
        payload=b"payload",
    )
    session = _StreamingSession([stream_row])

    fragments = [
        fragment
        async for fragment in stream_shared_blocks(
            session,
            schema_name="mrf",
            snapshot_key=41,
            object_kind="by_code_provider_shard_v1",
            block_keys=(_shard_block_key(7, 0),),
        )
    ]

    assert [fragment.payload for fragment in fragments] == [b"payload"]
    assert len(session.calls) == 1

@pytest.mark.asyncio
async def test_graph_member_limit_bounds_generate_series_and_decoded_bytes():
    all_members = (3, 9, 17, 25)
    member_payload = b"".join(
        member.to_bytes(4, "little", signed=False) for member in all_members
    )
    graph_row = _stored_row(
        object_kind="graph_npi_groups_v1",
        block_key=4,
        fragment_no=0,
        payload=member_payload,
        extra={
            "owner_key": 1234567890,
            "first_chunk": 4,
            "member_offset": 0,
            "member_count": len(all_members),
            "selected_member_count": 2,
        },
    )
    session = _Session([graph_row])

    members = await fetch_shared_graph_members(
        session,
        schema_name="mrf",
        snapshot_key=41,
        direction=PTG2_V3_GRAPH_NPI_TO_GROUP,
        owner_keys=(1234567890,),
        max_members=2,
    )

    assert members == {1234567890: all_members[:2]}
    sql, params = session.calls[0]
    assert sql.count("LEAST(owner.member_count, :max_members)") >= 2
    assert "generate_series" in sql
    assert params["max_members"] == 2

@pytest.mark.asyncio
async def test_graph_wrapper_threads_member_limit_into_storage_read(monkeypatch):
    graph_fetch = AsyncMock(return_value={7: (3, 5)})
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "fetch_shared_graph_members",
        graph_fetch,
    )

    session = object()
    members = await ptg2_db_sidecars.lookup_shared_graph_members_from_db(
        session,
        41,
        4,
        (7,),
        max_members=2,
    )

    assert members == {7: (3, 5)}
    graph_fetch.assert_awaited_once_with(
        session,
        schema_name="mrf",
        snapshot_key=41,
        direction=4,
        owner_keys=(7,),
        max_members=2,
    )
