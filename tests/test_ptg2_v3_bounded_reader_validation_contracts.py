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

def test_shared_price_atom_is_projected_once_per_hydration(monkeypatch):
    projection_calls = []

    def _project(price_atom, _dictionary_values, _constant_values):
        projection_calls.append(price_atom)
        return {"negotiated_rate": "12.34", "service_code": []}

    atom = object()
    monkeypatch.setattr(
        ptg2_serving,
        "_version_three_price_payload",
        _project,
    )

    copied_rows = ptg2_serving._version_three_price_rows(
        (1, 2),
        {1: (7,), 2: (7,)},
        {7: atom},
        {},
        {},
    )

    assert projection_calls == [atom]
    assert copied_rows[1][0] == copied_rows[2][0]
    assert copied_rows[1][0] is not copied_rows[2][0]
    copied_rows[1][0]["negotiated_rate"] = "changed"
    assert copied_rows[2][0]["negotiated_rate"] == "12.34"

    projection_calls.clear()
    shared_rows = ptg2_serving._version_three_price_rows(
        (1, 2),
        {1: (7,), 2: (7,)},
        {7: atom},
        {},
        {},
        copy_payloads=False,
    )

    assert projection_calls == [atom]
    assert shared_rows[1][0] is shared_rows[2][0]

@pytest.mark.parametrize(
    "source_filters_by_code",
    [
        {7: ()},
        {8: (0,)},
        {7: (False,)},
        {7: (-1,)},
        {7: (2,)},
    ],
)
@pytest.mark.asyncio
async def test_audit_forward_index_rejects_invalid_source_filters(
    source_filters_by_code,
):
    with pytest.raises(PTG2ManifestArtifactError, match="source filter"):
        await (
            ptg2_db_sidecars.lookup_forward_price_index_from_db(
                object(),
                (7,),
                provider_set_keys_by_code={7: (5,)},
                source_keys_by_code=source_filters_by_code,
                shared_snapshot_key=41,
                source_count=2,
                price_dictionary_item_count=128,
                price_dictionary_block_bytes=2048,
            )
        )

@pytest.mark.asyncio
async def test_sparse_batch_rejects_two_provider_filter_modes():
    with pytest.raises(PTG2ManifestArtifactError, match="one filter mode"):
        await ptg2_db_sidecars.lookup_forward_occurrences_batch_from_db(
            object(),
            (7,),
            provider_set_keys=(5,),
            provider_set_keys_by_code={7: (5,)},
            shared_snapshot_key=41,
            source_count=2,
            price_dictionary_item_count=128,
            price_dictionary_block_bytes=2048,
        )

@pytest.mark.asyncio
async def test_full_batch_discovers_multiple_code_ranges_once(monkeypatch):
    block_7 = _shard_block_key(7, 5)
    block_8 = _shard_block_key(8, 1025)
    discover = AsyncMock(return_value={7: (block_7,), 8: (block_8,)})
    fetch = AsyncMock(
        return_value=[
            _fragment_row(
                _fragment(
                    _grouped_payload(2, [(5, [(8, 0)])]),
                    entry_count=1,
                    block_key=block_7,
                )
            ),
            _fragment_row(
                _fragment(
                    _grouped_payload(2, [(1025, [(2, 1)])]),
                    entry_count=1,
                    block_key=block_8,
                )
            ),
        ]
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_discover_forward_shard_keys",
        discover,
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_shared_serving_binary_payload_rows_for_keys",
        fetch,
    )
    _patch_reference_lookups(monkeypatch)

    rows_by_code = await ptg2_db_sidecars.lookup_binary_code_batch_from_db(
        object(),
        (8, 7),
        shared_snapshot_key=41,
        source_count=2,
        price_dictionary_item_count=128,
        price_dictionary_block_bytes=2048,
    )

    assert [decoded_row.provider_set_key for decoded_row in rows_by_code[7]] == [5]
    assert [decoded_row.provider_set_key for decoded_row in rows_by_code[8]] == [1025]
    assert discover.await_args.kwargs["code_keys"] == (7, 8)
    assert fetch.await_args.kwargs["block_keys"] == (block_7, block_8)
    assert fetch.await_args.kwargs["require_all"] is True

@pytest.mark.asyncio
async def test_full_reader_accepts_provider_delta_reset_in_each_fragment(
    monkeypatch,
):
    block_key = _shard_block_key(7, 5)
    discover = AsyncMock(return_value={7: (block_key,)})
    fetch = AsyncMock(
        return_value=[
            _fragment_row(
                _fragment(
                    _grouped_payload(2, [(3, [(1, 0)])]),
                    fragment_no=0,
                    entry_count=1,
                    block_key=block_key,
                )
            ),
            _fragment_row(
                _fragment(
                    _grouped_payload(2, [(5, [(2, 1)])]),
                    fragment_no=1,
                    entry_count=1,
                    block_key=block_key,
                )
            ),
        ]
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_discover_forward_shard_keys",
        discover,
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_shared_serving_binary_payload_rows_for_keys",
        fetch,
    )
    _patch_reference_lookups(monkeypatch)

    decoded_rows = await ptg2_db_sidecars.lookup_serving_binary_by_code_from_db(
        object(),
        7,
        shared_snapshot_key=41,
        source_count=2,
        price_dictionary_item_count=128,
        price_dictionary_block_bytes=2048,
    )

    assert [decoded_row.provider_set_key for decoded_row in decoded_rows] == [3, 5]

@pytest.mark.asyncio
async def test_bounded_prefix_streams_all_shards_and_resets_fragment_numbers(
    monkeypatch,
):
    fragments = (
        _fragment(
            _grouped_payload(2, [(5, [(9, 0)])]),
            fragment_no=0,
            entry_count=1,
            block_key=_shard_block_key(7, 5),
        ),
        _fragment(
            _grouped_payload(2, [(1025, [(1, 1)])]),
            fragment_no=0,
            entry_count=1,
            block_key=_shard_block_key(7, 1025),
        ),
    )

    rows, _provider_counts, _dictionary = await _prefix_rows(
        monkeypatch,
        fragments=fragments,
        limit=2,
        descending=False,
    )

    assert [
        (row.provider_set_key, row.price_key, row.source_key) for row in rows
    ] == [(1025, 1, 1), (5, 9, 0)]

@pytest.mark.asyncio
async def test_bounded_prefix_rejects_provider_outside_logical_shard(monkeypatch):
    fragments = (
        _fragment(
            _grouped_payload(2, [(5, [(1, 0)])]),
            entry_count=1,
            block_key=_shard_block_key(7, 1025),
        ),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="outside its forward shard"):
        await _prefix_rows(
            monkeypatch,
            fragments=fragments,
            limit=1,
            descending=False,
        )

@pytest.mark.asyncio
async def test_bounded_prefix_rejects_block_key_from_another_code(monkeypatch):
    fragments = (
        _fragment(
            _grouped_payload(2, [(5, [(1, 0)])]),
            entry_count=1,
            block_key=_shard_block_key(8, 5),
        ),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="outside its code range"):
        await _prefix_rows(
            monkeypatch,
            fragments=fragments,
            limit=1,
            descending=False,
        )

@pytest.mark.asyncio
async def test_bounded_prefix_requires_fragment_zero_for_each_shard(monkeypatch):
    fragments = (
        _fragment(
            _grouped_payload(2, [(5, [(1, 0)])]),
            fragment_no=1,
            entry_count=1,
            block_key=_shard_block_key(7, 5),
        ),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="not contiguous"):
        await _prefix_rows(
            monkeypatch,
            fragments=fragments,
            limit=1,
            descending=False,
        )

@pytest.mark.asyncio
async def test_bounded_prefix_validates_source_metadata_in_every_shard(
    monkeypatch,
):
    fragments = (
        _fragment(
            _grouped_payload(2, [(5, [(1, 0)])]),
            entry_count=1,
            block_key=_shard_block_key(7, 5),
        ),
        _fragment(
            _grouped_payload(3, [(1025, [(2, 2)])]),
            entry_count=1,
            block_key=_shard_block_key(7, 1025),
        ),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="corrupt"):
        await _prefix_rows(
            monkeypatch,
            fragments=fragments,
            limit=1,
            descending=False,
        )

@pytest.mark.asyncio
async def test_bounded_prefix_rejects_materialized_row_without_dense_rank(
    monkeypatch,
):
    fragment = _fragment(
        _grouped_payload(2, [(5, [(1, 0)])]),
        entry_count=1,
        block_key=_shard_block_key(7, 5),
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_materialize_forward_rows",
        Mock(
            return_value=(
                ptg2_db_sidecars.PTG2ServingBinaryRow(
                    code_key=7,
                    provider_set_key=5,
                    provider_count=None,
                    price_set_global_id_128="0" * 32,
                    source_key=0,
                    price_key=1,
                ),
            )
        ),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="dense rank field"):
        await _prefix_rows(
            monkeypatch,
            fragments=(fragment,),
            limit=1,
            descending=False,
        )

@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("descending", "expected"),
    [
        (False, [(7, 2, 0), (9, 5, 0), (5, 8, 0)]),
        (True, [(5, 100, 1), (6, 100, 0), (7, 9, 1)]),
    ],
)
async def test_bounded_code_prefix_matches_eager_rank_and_reads_selected_refs(
    monkeypatch,
    descending,
    expected,
):
    """Verify bounded reads match eager ranking and resolve selected references."""

    entries = [
        (5, [(8, 0), (100, 1)]),
        (6, [(100, 0)]),
        (7, [(2, 0), (9, 1)]),
        (9, [(5, 0)]),
    ]
    encoded_payload = _grouped_payload(2, entries)
    fragments = (_fragment(encoded_payload, entry_count=len(entries)),)

    bounded_rows, provider_counts, dictionary = await _prefix_rows(
        monkeypatch,
        fragments=fragments,
        limit=3,
        descending=descending,
    )
    eager_prefix = _eager_ranked_prefix(encoded_payload, entries, descending)
    assert eager_prefix == expected
    _assert_prefix_reference_reads(
        bounded_rows,
        provider_counts,
        dictionary,
        expected,
    )

@pytest.mark.asyncio
async def test_same_provider_continuations_preserve_exact_occurrences(monkeypatch):
    fragments = (
        _fragment(
            _grouped_payload(2, [(5, [(1, 0), (2, 1)])]),
            fragment_no=0,
            entry_count=1,
        ),
        _fragment(
            _grouped_payload(2, [(5, [(2, 1), (3, 0)])]),
            fragment_no=1,
            entry_count=1,
        ),
    )
    expected_occurrences = [(5, 1, 0), (5, 2, 1), (5, 2, 1), (5, 3, 0)]

    eager_occurrences = ptg2_db_sidecars._decode_serving_binary_code_records(
        [_fragment_row(fragment) for fragment in fragments],
        provider_set_keys=None,
        expected_source_count=2,
    )
    prefix_rows, provider_count_lookup, price_dictionary_lookup = await _prefix_rows(
        monkeypatch,
        fragments=fragments,
        limit=len(expected_occurrences),
        descending=False,
    )

    assert eager_occurrences == expected_occurrences
    assert [
        (prefix_row.provider_set_key, prefix_row.price_key, prefix_row.source_key)
        for prefix_row in prefix_rows
    ] == expected_occurrences
    provider_count_lookup.assert_awaited_once()
    price_dictionary_lookup.assert_awaited_once()
