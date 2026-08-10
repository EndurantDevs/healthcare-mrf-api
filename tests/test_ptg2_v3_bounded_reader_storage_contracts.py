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
async def test_full_code_read_discovers_and_decodes_every_provider_shard(
    monkeypatch,
):
    block_zero = _shard_block_key(7, 5)
    block_one = _shard_block_key(7, 1025)
    fragments = (
        _fragment(
            _grouped_payload(2, [(5, [(8, 0)])]),
            entry_count=1,
            block_key=block_zero,
        ),
        _fragment(
            _grouped_payload(2, [(1025, [(2, 1)])]),
            entry_count=1,
            block_key=block_one,
        ),
    )
    discover = AsyncMock(return_value={7: (block_zero, block_one)})
    fetch = AsyncMock(
        return_value=[_fragment_row(fragment) for fragment in fragments]
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

    assert [
        (decoded_row.provider_set_key, decoded_row.price_key, decoded_row.source_key)
        for decoded_row in decoded_rows
    ] == [(5, 8, 0), (1025, 2, 1)]
    discover.assert_awaited_once()
    assert fetch.await_args.kwargs["artifact_kind"] == (
        "by_code_provider_shard_v1"
    )
    assert fetch.await_args.kwargs["block_keys"] == (block_zero, block_one)
    assert fetch.await_args.kwargs["require_all"] is True

@pytest.mark.asyncio
async def test_provider_filtered_read_computes_only_exact_sparse_shards(
    monkeypatch,
):
    block_zero = _shard_block_key(7, 5)
    block_two = _shard_block_key(7, 2050)
    discover = AsyncMock(
        side_effect=AssertionError("sparse provider reads must not discover a code range")
    )
    fetch = AsyncMock(
        return_value=[
            _fragment_row(
                _fragment(
                    _grouped_payload(2, [(5, [(8, 0)])]),
                    entry_count=1,
                    block_key=block_zero,
                )
            )
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

    filtered_rows = await ptg2_db_sidecars.lookup_serving_binary_by_code_from_db(
        object(),
        7,
        provider_set_keys=(5, 2050),
        shared_snapshot_key=41,
        source_count=2,
        price_dictionary_item_count=128,
        price_dictionary_block_bytes=2048,
    )

    assert [
        (filtered_row.provider_set_key, filtered_row.price_key)
        for filtered_row in filtered_rows
    ] == [(5, 8)]
    discover.assert_not_awaited()
    assert fetch.await_args.kwargs["block_keys"] == (block_zero, block_two)
    assert fetch.await_args.kwargs["require_all"] is False

@pytest.mark.asyncio
async def test_provider_filtered_read_uses_manifest_provider_shard_span(
    monkeypatch,
):
    provider_shard_span = 8192
    block_zero = _shard_block_key(7, 5, provider_shard_span)
    block_one = _shard_block_key(7, 8193, provider_shard_span)
    fetch = AsyncMock(
        return_value=[
            _fragment_row(
                _fragment(
                    _grouped_payload(2, [(5, [(8, 0)])]),
                    entry_count=1,
                    block_key=block_zero,
                )
            )
        ]
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_discover_forward_shard_keys",
        AsyncMock(
            side_effect=AssertionError(
                "sparse provider reads must not discover a code range"
            )
        ),
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_shared_serving_binary_payload_rows_for_keys",
        fetch,
    )
    _patch_reference_lookups(monkeypatch)

    filtered_rows = await ptg2_db_sidecars.lookup_serving_binary_by_code_from_db(
        object(),
        7,
        provider_set_keys=(5, 8193),
        shared_snapshot_key=41,
        source_count=2,
        price_dictionary_item_count=128,
        price_dictionary_block_bytes=2048,
        provider_shard_span=provider_shard_span,
    )

    assert [
        (filtered_row.provider_set_key, filtered_row.price_key)
        for filtered_row in filtered_rows
    ] == [
        (5, 8)
    ]
    assert fetch.await_args.kwargs["block_keys"] == (block_zero, block_one)

@pytest.mark.asyncio
async def test_sparse_batch_uses_each_codes_own_provider_shards(monkeypatch):
    block_7 = _shard_block_key(7, 5)
    block_8 = _shard_block_key(8, 1025)
    returned_fragments = [
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
    fetch = AsyncMock(return_value=returned_fragments)
    discover = AsyncMock(
        side_effect=AssertionError("per-code filters must not discover code ranges")
    )
    hydrate = AsyncMock(
        side_effect=AssertionError("occurrence reads must not hydrate labels")
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_shared_serving_binary_payload_rows_for_keys",
        fetch,
    )
    monkeypatch.setattr(ptg2_db_sidecars, "_discover_forward_shard_keys", discover)
    monkeypatch.setattr(ptg2_db_sidecars, "_lookup_forward_references", hydrate)

    occurrences_by_code = (
        await ptg2_db_sidecars.lookup_forward_occurrences_batch_from_db(
            object(),
            (8, 7),
            provider_set_keys_by_code={7: (5,), 8: (1025,)},
            shared_snapshot_key=41,
            source_count=2,
            price_dictionary_item_count=128,
            price_dictionary_block_bytes=2048,
        )
    )

    assert occurrences_by_code == {7: ((5, 8, 0),), 8: ((1025, 2, 1),)}
    assert fetch.await_args.kwargs["block_keys"] == (block_7, block_8)
    assert fetch.await_args.kwargs["require_all"] is False
    discover.assert_not_awaited()
    hydrate.assert_not_awaited()

@pytest.mark.asyncio
async def test_audit_forward_index_filters_sources_during_one_union_visit(
    monkeypatch,
):
    block_7 = _shard_block_key(7, 5)
    block_8 = _shard_block_key(8, 1025)
    fetch = AsyncMock(
        return_value=[
            _fragment_row(
                _fragment(
                    _grouped_payload(
                        2,
                        [(5, [(8, 0), (9, 1), (10, 0)])],
                    ),
                    entry_count=1,
                    block_key=block_7,
                )
            ),
            _fragment_row(
                _fragment(
                    _grouped_payload(2, [(1025, [(2, 0), (3, 1)])]),
                    entry_count=1,
                    block_key=block_8,
                )
            ),
        ]
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_shared_serving_binary_payload_rows_for_keys",
        fetch,
    )

    price_keys_by_occurrence = await (
        ptg2_db_sidecars.lookup_forward_price_index_from_db(
            object(),
            (8, 7),
            provider_set_keys_by_code={7: (5,), 8: (1025,)},
            source_keys_by_code={7: (1,), 8: (0,)},
            shared_snapshot_key=41,
            source_count=2,
            price_dictionary_item_count=128,
            price_dictionary_block_bytes=2048,
        )
    )

    assert price_keys_by_occurrence == {
        (7, 5, 1): (9,),
        (8, 1025, 0): (2,),
    }
    fetch.assert_awaited_once()
    assert fetch.await_args.kwargs["block_keys"] == (block_7, block_8)

@pytest.mark.asyncio
async def test_audit_forward_index_filters_exact_provider_source_pairs(
    monkeypatch,
):
    block_key = _shard_block_key(7, 5)
    fetch = AsyncMock(
        return_value=[
            _fragment_row(
                _fragment(
                    _grouped_payload(
                        2,
                        [
                            (5, [(8, 0), (9, 1)]),
                            (6, [(10, 0), (11, 1)]),
                        ],
                    ),
                    entry_count=2,
                    block_key=block_key,
                )
            )
        ]
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_shared_serving_binary_payload_rows_for_keys",
        fetch,
    )

    price_keys_by_occurrence = await (
        ptg2_db_sidecars.lookup_forward_price_index_from_db(
            object(),
            (7,),
            provider_set_keys_by_code={7: (5, 6)},
            occurrence_keys={(7, 5, 0), (7, 6, 1)},
            shared_snapshot_key=41,
            source_count=2,
            price_dictionary_item_count=128,
            price_dictionary_block_bytes=2048,
        )
    )

    assert price_keys_by_occurrence == {
        (7, 5, 0): (8,),
        (7, 6, 1): (11,),
    }
    assert (7, 5, 1) not in price_keys_by_occurrence
    assert (7, 6, 0) not in price_keys_by_occurrence

@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_filters_by_code",
    [None, {7: (5, 6)}],
)
async def test_audit_forward_exact_scope_fails_before_shard_io(
    monkeypatch,
    provider_filters_by_code,
):
    discover = AsyncMock(return_value={7: (_shard_block_key(7, 5),)})
    fetch = AsyncMock(return_value=[])
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
    read_options_by_name = {
        "occurrence_keys": {(7, 5, 0)},
        "shared_snapshot_key": 41,
        "source_count": 2,
        "price_dictionary_item_count": 128,
        "price_dictionary_block_bytes": 2048,
    }
    if provider_filters_by_code is not None:
        read_options_by_name["provider_set_keys_by_code"] = (
            provider_filters_by_code
        )

    with pytest.raises(PTG2ManifestArtifactError, match="provider scope"):
        await ptg2_db_sidecars.lookup_forward_price_index_from_db(
            object(),
            (7,),
            **read_options_by_name,
        )

    discover.assert_not_awaited()
    fetch.assert_not_awaited()

@pytest.mark.asyncio
async def test_audit_forward_exact_filter_spans_provider_shards(monkeypatch):
    provider_set_keys = (5, 1025)
    block_keys = tuple(_shard_block_key(7, key) for key in provider_set_keys)
    returned_fragments = [
        _fragment_row(
            _fragment(
                _grouped_payload(2, [(provider_key, [(provider_key, source_key)])]),
                entry_count=1,
                block_key=block_key,
            )
        )
        for source_key, (provider_key, block_key) in enumerate(
            zip(provider_set_keys, block_keys)
        )
    ]
    fetch = AsyncMock(return_value=returned_fragments)
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_shared_serving_binary_payload_rows_for_keys",
        fetch,
    )
    required_occurrences = {(7, 5, 0), (7, 1025, 1)}
    retention_budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=1024 * 1024
    )

    observed = await ptg2_db_sidecars.lookup_forward_price_index_from_db(
        object(),
        (7,),
        provider_set_keys_by_code={7: provider_set_keys},
        occurrence_keys=required_occurrences,
        shared_snapshot_key=41,
        source_count=2,
        price_dictionary_item_count=2048,
        price_dictionary_block_bytes=2048,
        retention_budget=retention_budget,
    )

    assert observed == {(7, 5, 0): (5,), (7, 1025, 1): (1025,)}
    assert fetch.await_args.kwargs["block_keys"] == block_keys
    assert retention_budget.retained_bytes == 2 * (256 + 48)
