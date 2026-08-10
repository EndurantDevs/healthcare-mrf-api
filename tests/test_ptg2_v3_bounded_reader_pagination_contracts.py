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
async def test_audit_forward_exact_filter_validates_unretained_rows(monkeypatch):
    block_key = _shard_block_key(7, 5)
    fetch = AsyncMock(
        return_value=[
            _fragment_row(
                _fragment(
                    _grouped_payload(2, [(5, [(8, 0), (999, 1)])]),
                    entry_count=1,
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

    with pytest.raises(PTG2ManifestArtifactError, match="price key"):
        await ptg2_db_sidecars.lookup_forward_price_index_from_db(
            object(),
            (7,),
            provider_set_keys_by_code={7: (5,)},
            occurrence_keys={(7, 5, 0)},
            shared_snapshot_key=41,
            source_count=2,
            price_dictionary_item_count=128,
            price_dictionary_block_bytes=2048,
        )

@pytest.mark.asyncio
async def test_audit_forward_exact_filter_prunes_dense_cross_product_during_visit(
    monkeypatch,
):
    """Retain 100 exact coordinates while validating all 10,000 rows once."""

    provider_set_keys = tuple(range(5, 105))
    source_keys = tuple(range(100))
    visit_spy = _install_forward_fragment_reader(
        monkeypatch,
        [_dense_forward_fragment(provider_set_keys, source_keys)],
    )

    broad_index = await ptg2_db_sidecars.lookup_forward_price_index_from_db(
        object(),
        (7,),
        provider_set_keys_by_code={7: provider_set_keys},
        source_keys_by_code={7: source_keys},
        shared_snapshot_key=41,
        source_count=len(source_keys),
        price_dictionary_item_count=128,
        price_dictionary_block_bytes=2048,
    )
    required_occurrences = {
        (7, provider_set_key, source_key)
        for provider_set_key, source_key in zip(provider_set_keys, source_keys)
    }
    exact_index = await ptg2_db_sidecars.lookup_forward_price_index_from_db(
        object(),
        (7,),
        provider_set_keys_by_code={7: provider_set_keys},
        occurrence_keys=required_occurrences,
        shared_snapshot_key=41,
        source_count=len(source_keys),
        price_dictionary_item_count=128,
        price_dictionary_block_bytes=2048,
    )

    assert len(broad_index) == 10_000
    assert exact_index == {
        occurrence_key: (occurrence_key[2] + 1,)
        for occurrence_key in required_occurrences
    }
    assert len(exact_index) == 100
    assert visit_spy.call_count == 2

@pytest.mark.asyncio
async def test_audit_forward_exact_alias_indexes_logical_views_once(monkeypatch):
    """Index aliased logical views before the single physical block parse."""

    code_keys = tuple(range(7, 71))
    source_count, returned_fragments, required_occurrences = (
        _aliased_forward_fragments(code_keys)
    )
    visit_spy = _install_forward_fragment_reader(monkeypatch, returned_fragments)
    captures = _capture_forward_fanout(monkeypatch)
    price_keys_by_occurrence = await (
        ptg2_db_sidecars.lookup_forward_price_index_from_db(
            object(),
            code_keys,
            provider_set_keys_by_code={code_key: (5,) for code_key in code_keys},
            occurrence_keys=required_occurrences,
            shared_snapshot_key=41,
            source_count=source_count,
            price_dictionary_item_count=128,
            price_dictionary_block_bytes=2048,
        )
    )

    assert price_keys_by_occurrence == {
        occurrence_key: (source_key + 1,)
        for source_key, occurrence_key in enumerate(sorted(required_occurrences))
    }
    assert visit_spy.call_count == 1
    assert len(captures) == 1
    assert captures[0].fallback_views == ()
    assert len(captures[0].exact_views_by_occurrence) == source_count
    assert sum(
        len(views) for views in captures[0].exact_views_by_occurrence.values()
    ) == source_count

@pytest.mark.asyncio
async def test_audit_forward_alias_parses_and_claims_physical_payload_once(
    monkeypatch,
):
    block_7 = _shard_block_key(7, 5)
    block_8 = _shard_block_key(8, 5)
    block_hash = b"h" * 32
    forward_block_bytes = _grouped_payload(2, [(5, [(8, 0), (9, 1)])])
    fetch = AsyncMock(
        return_value=[
            _fragment_row(
                _fragment(forward_block_bytes, entry_count=1, block_key=block_7),
                block_hash=block_hash,
            ),
            _fragment_row(
                _fragment(forward_block_bytes, entry_count=1, block_key=block_8),
                block_hash=block_hash,
            ),
        ]
    )
    original_visit = ptg2_db_sidecars._visit_serving_binary_by_code_record
    visit_spy = Mock(wraps=original_visit)
    claim = Mock()
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_shared_serving_binary_payload_rows_for_keys",
        fetch,
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_visit_serving_binary_by_code_record",
        visit_spy,
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "claim_shared_block_processing",
        claim,
    )

    price_keys_by_occurrence = await (
        ptg2_db_sidecars.lookup_forward_price_index_from_db(
            object(),
            (8, 7),
            provider_set_keys_by_code={7: (5,), 8: (5,)},
            source_keys_by_code={7: (0,), 8: (1,)},
            shared_snapshot_key=41,
            source_count=2,
            price_dictionary_item_count=128,
            price_dictionary_block_bytes=2048,
        )
    )

    assert price_keys_by_occurrence == {
        (7, 5, 0): (8,),
        (8, 5, 1): (9,),
    }
    visit_spy.assert_called_once()
    claim.assert_called_once_with(schema_name="mrf", block_hash=block_hash)

@pytest.mark.asyncio
async def test_audit_forward_alias_revalidates_each_logical_shard_bounds(
    monkeypatch,
):
    block_7 = _shard_block_key(7, 5)
    block_8 = _shard_block_key(8, 1025)
    block_hash = b"h" * 32
    forward_block_bytes = _grouped_payload(2, [(5, [(8, 0)])])
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_shared_serving_binary_payload_rows_for_keys",
        AsyncMock(
            return_value=[
                _fragment_row(
                    _fragment(forward_block_bytes, entry_count=1, block_key=block_7),
                    block_hash=block_hash,
                ),
                _fragment_row(
                    _fragment(forward_block_bytes, entry_count=1, block_key=block_8),
                    block_hash=block_hash,
                ),
            ]
        ),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="outside its forward shard"):
        await ptg2_db_sidecars.lookup_forward_price_index_from_db(
            object(),
            (7, 8),
            provider_set_keys_by_code={7: (5,), 8: (1025,)},
            source_keys_by_code={7: (0,), 8: (0,)},
            shared_snapshot_key=41,
            source_count=2,
            price_dictionary_item_count=128,
            price_dictionary_block_bytes=2048,
        )

@pytest.mark.asyncio
async def test_audit_forward_rejects_missing_physical_block_identity(monkeypatch):
    block_key = _shard_block_key(7, 5)
    fragment_fields_by_name = _fragment_row(
        _fragment(
            _grouped_payload(1, [(5, [(8, 0)])]),
            entry_count=1,
            block_key=block_key,
        )
    )
    fragment_fields_by_name.pop("_block_hash")
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_shared_serving_binary_payload_rows_for_keys",
        AsyncMock(return_value=[fragment_fields_by_name]),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="physical block identity"):
        await ptg2_db_sidecars.lookup_forward_price_index_from_db(
            object(),
            (7,),
            provider_set_keys_by_code={7: (5,)},
            source_keys_by_code={7: (0,)},
            shared_snapshot_key=41,
            source_count=1,
            price_dictionary_item_count=128,
            price_dictionary_block_bytes=2048,
        )

@pytest.mark.asyncio
async def test_audit_forward_index_deduplicates_continuation_prices(monkeypatch):
    block_key = _shard_block_key(7, 5)
    fetch = AsyncMock(
        return_value=[
            _fragment_row(
                _fragment(
                    _grouped_payload(2, [(5, [(8, 1)])]),
                    fragment_no=0,
                    entry_count=1,
                    block_key=block_key,
                )
            ),
            _fragment_row(
                _fragment(
                    _grouped_payload(2, [(5, [(8, 1), (9, 1)])]),
                    fragment_no=1,
                    entry_count=1,
                    block_key=block_key,
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
            (7,),
            provider_set_keys_by_code={7: (5,)},
            source_keys_by_code={7: (1,)},
            shared_snapshot_key=41,
            source_count=2,
            price_dictionary_item_count=128,
            price_dictionary_block_bytes=2048,
        )
    )

    assert price_keys_by_occurrence == {(7, 5, 1): (8, 9)}

@pytest.mark.asyncio
async def test_audit_forward_index_validates_filtered_out_occurrences(monkeypatch):
    block_key = _shard_block_key(7, 5)
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_shared_serving_binary_payload_rows_for_keys",
        AsyncMock(
            return_value=[
                _fragment_row(
                    _fragment(
                        _grouped_payload(2, [(5, [(9, 0), (8, 1)])]),
                        entry_count=1,
                        block_key=block_key,
                    )
                )
            ]
        ),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="not ordered"):
        await (
            ptg2_db_sidecars.lookup_forward_price_index_from_db(
                object(),
                (7,),
                provider_set_keys_by_code={7: (5,)},
                source_keys_by_code={7: (0,)},
                shared_snapshot_key=41,
                source_count=2,
                price_dictionary_item_count=128,
                price_dictionary_block_bytes=2048,
            )
        )

def test_forward_occurrence_price_vector_is_parsed_once(monkeypatch):
    original_reader = ptg2_db_sidecars.read_strict_uvarint
    read_offsets = []

    def _counted_reader(payload, offset):
        read_offsets.append(offset)
        return original_reader(payload, offset)

    monkeypatch.setattr(
        ptg2_db_sidecars,
        "read_strict_uvarint",
        _counted_reader,
    )
    fragment = _fragment_row(
        _fragment(
            _grouped_payload(2, [(5, [(8, 0), (9, 1), (10, 0)])]),
            entry_count=1,
        )
    )

    decoded = ptg2_db_sidecars._decode_serving_binary_code_records(
        (fragment,),
        provider_set_keys=(5,),
        expected_source_count=2,
        price_item_count=128,
    )

    assert decoded == [(5, 8, 0), (5, 9, 1), (5, 10, 0)]
    assert len(read_offsets) == 5

def test_price_membership_header_is_parsed_once(monkeypatch):
    encoded_memberships = ptg2_serving_binary_v3.encode_price_memberships(
        ((0, (7,)),),
        24,
    )
    original_header = ptg2_serving_binary_v3._price_membership_header
    header_spy = Mock(wraps=original_header)

    monkeypatch.setattr(
        ptg2_serving_binary_v3,
        "_price_membership_header",
        header_spy,
    )

    memberships = ptg2_db_serving_v3._decode_price_membership_block(
        encoded_memberships,
        block_key=0,
        entry_count=1,
        atom_key_bits=24,
        block_span=512,
        requested_price_keys={0},
    )

    assert memberships == {0: (7,)}
    header_spy.assert_called_once_with(encoded_memberships)

def test_price_atom_header_is_parsed_once(monkeypatch):
    price_atom = PTG2V3PriceAtomRecord("12.34", (None,))
    encoded_atoms = ptg2_serving_binary_v3.encode_price_atoms((price_atom,))
    original_header = ptg2_serving_binary_v3._price_atom_header
    header_spy = Mock(wraps=original_header)

    monkeypatch.setattr(
        ptg2_serving_binary_v3,
        "_price_atom_header",
        header_spy,
    )

    atoms = ptg2_db_sidecars._decode_price_atom_block(
        encoded_atoms,
        block_key=0,
        entry_count=1,
        block_span=512,
        requested_keys={0},
    )

    assert atoms == {0: price_atom}
    header_spy.assert_called_once_with(encoded_atoms)
