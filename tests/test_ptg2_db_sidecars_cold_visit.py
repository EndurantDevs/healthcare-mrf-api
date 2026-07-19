# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from unittest.mock import Mock

import pytest

from api import ptg2_db_sidecars as sidecars


def _uvarint(value: int) -> bytes:
    encoded = bytearray()
    remaining = int(value)
    while remaining >= 0x80:
        encoded.append((remaining & 0x7F) | 0x80)
        remaining >>= 7
    encoded.append(remaining)
    return bytes(encoded)


def _single_source_fragment(
    entries: tuple[tuple[int, tuple[int, ...]], ...],
) -> dict:
    return _forward_fragment(
        1,
        tuple(
            (
                provider_set_key,
                tuple((price_key, 0) for price_key in price_keys),
            )
            for provider_set_key, price_keys in entries
        ),
    )


def _source_vector(source_keys: tuple[int, ...], source_bits: int) -> bytes:
    encoded = bytearray((len(source_keys) * source_bits + 7) // 8)
    for source_index, source_key in enumerate(source_keys):
        for source_bit in range(source_bits):
            if source_key & (1 << source_bit):
                bit_offset = source_index * source_bits + source_bit
                encoded[bit_offset // 8] |= 1 << (bit_offset % 8)
    return bytes(encoded)


def _forward_fragment(
    source_count: int,
    entries: tuple[tuple[int, tuple[tuple[int, int], ...]], ...],
    *,
    fragment_no: int = 0,
    block_hash: bytes = b"h" * 32,
) -> dict:
    source_bits = 0 if source_count == 1 else (source_count - 1).bit_length()
    payload = bytearray((2, source_count, source_bits))
    previous_provider_set_key = 0
    for provider_set_key, occurrences in entries:
        payload.extend(_uvarint(provider_set_key - previous_provider_set_key))
        payload.extend(_uvarint(len(occurrences)))
        for price_key, _source_key in occurrences:
            payload.extend(_uvarint(price_key))
        payload.extend(
            _source_vector(
                tuple(source_key for _price_key, source_key in occurrences),
                source_bits,
            )
        )
        previous_provider_set_key = provider_set_key
    return {
        "block_key": sidecars._forward_provider_shard_block_key(7, 0),
        "block_no": fragment_no,
        "entry_count": len(entries),
        "_decoded_payload": bytes(payload),
        "_block_hash": block_hash,
    }


def _fragment_view(
    fragment_row: dict,
    *,
    fragment_no: int = 0,
    provider_filter: frozenset[int] | None = None,
    occurrence_filter: frozenset[tuple[int, int]] | None = None,
) -> sidecars._ForwardBatchFragmentView:
    return sidecars._ForwardBatchFragmentView(
        code_key=7,
        block_key=int(fragment_row["block_key"]),
        fragment_no=fragment_no,
        provider_key_min=0,
        provider_key_max=1024,
        provider_filter=provider_filter,
        source_filter=None,
        occurrence_filter=occurrence_filter,
        fragment_row=fragment_row,
    )


def test_single_source_occurrences_are_delivered_during_price_decode(monkeypatch):
    source_key_decoder = Mock(
        side_effect=AssertionError("zero-bit source keys must not be decoded")
    )
    monkeypatch.setattr(sidecars, "_dense_source_key_at", source_key_decoder)
    delivered_occurrence_list = []

    cursor, last_occurrence = sidecars._visit_forward_occurrences(
        b"\x02\x01\x02",
        0,
        source_count=1,
        source_bits=0,
        occurrence_consumer=(
            lambda price_key, source_key: delivered_occurrence_list.append(
                (price_key, source_key)
            )
        ),
        price_item_count=3,
    )

    assert cursor == 3
    assert last_occurrence == (2, 0)
    assert delivered_occurrence_list == [(1, 0), (2, 0)]
    source_key_decoder.assert_not_called()


def test_single_source_occurrences_reject_continuation_regression():
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="not ordered"):
        sidecars._visit_forward_occurrences(
            b"\x01\x01",
            0,
            source_count=1,
            source_bits=0,
            occurrence_consumer=None,
            previous_occurrence=(2, 0),
        )


def test_forward_fragment_skips_unselected_delivery_but_keeps_physical_first():
    fragment_row = _single_source_fragment(((5, (1,)), (6, (2,))))
    delivered_occurrence_list = []
    physical_first_list = []

    fragment_cursor, source_count = sidecars._visit_forward_fragment_unchecked(
        fragment_row,
        provider_filter={6},
        fragment_cursor=sidecars._ForwardFragmentCursor(),
        validation=sidecars._ForwardFragmentValidation(
            expected_source_count=1,
            price_item_count=3,
            provider_key_min=0,
            provider_key_max=1024,
        ),
        occurrence_consumer=(
            lambda *occurrence: delivered_occurrence_list.append(occurrence)
        ),
        first_occurrence_consumer=(
            lambda *occurrence: physical_first_list.append(occurrence)
        ),
    )

    assert source_count == 1
    assert fragment_cursor == sidecars._ForwardFragmentCursor(6, (2, 0))
    assert physical_first_list == [(5, 1, 0)]
    assert delivered_occurrence_list == [(6, 2, 0)]


def test_physical_forward_parse_retains_exact_provider_after_unmatched_first():
    fragment_row = _single_source_fragment(((5, (1,)), (6, (2,))))
    block_key = int(fragment_row["block_key"])
    view = _fragment_view(
        fragment_row,
        provider_filter=frozenset({6}),
        occurrence_filter=frozenset({(6, 0)}),
    )
    retained_by_coordinate = {(7, block_key, 0): []}

    parsed = sidecars._parse_physical_forward_fragment_once(
        (view,),
        sidecars._ForwardBatchOptions(
            shared_snapshot_key=1,
            source_count=1,
            price_dictionary_item_count=3,
            price_dictionary_block_bytes=32,
            occurrence_keys=((7, 6, 0),),
        ),
        3,
        retained_by_coordinate,
    )

    assert parsed.first_provider_set_key == 5
    assert parsed.first_occurrence == (1, 0)
    assert parsed.last_cursor == sidecars._ForwardFragmentCursor(6, (2, 0))
    assert retained_by_coordinate[(7, block_key, 0)] == [(6, 2, 0)]


def test_unselected_provider_price_keys_remain_fully_validated():
    fragment_row = _single_source_fragment(((5, (99,)), (6, (2,))))

    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="price key"):
        sidecars._visit_forward_fragment_unchecked(
            fragment_row,
            provider_filter={6},
            fragment_cursor=sidecars._ForwardFragmentCursor(),
            validation=sidecars._ForwardFragmentValidation(
                expected_source_count=1,
                price_item_count=3,
            ),
            occurrence_consumer=lambda *_occurrence: None,
        )


def test_selected_physical_first_is_captured_once(monkeypatch):
    fragment_row = _single_source_fragment(((5, (1,)),))
    view = _fragment_view(
        fragment_row,
        provider_filter=frozenset({5}),
        occurrence_filter=frozenset({(5, 0)}),
    )
    coordinate = (7, int(fragment_row["block_key"]), 0)
    capture = sidecars._forward_fanout_capture((view,), {coordinate: []})
    capture_first = Mock(wraps=capture.capture_first)
    capture.capture_first = capture_first
    monkeypatch.setattr(
        sidecars,
        "_forward_fanout_capture",
        Mock(return_value=capture),
    )

    sidecars._parse_physical_forward_fragment_once(
        (view,),
        sidecars._ForwardBatchOptions(1, 1, 3, 32),
        3,
        capture.retained_by_coordinate,
    )

    capture_first.assert_called_once_with(5, 1, 0)


def test_fanout_indexes_mixed_exact_and_fallback_provider_union():
    fragment_row = _single_source_fragment(((6, (1,)), (7, (2,))))
    block_key = int(fragment_row["block_key"])
    exact_view = _fragment_view(
        fragment_row,
        provider_filter=frozenset({6}),
        occurrence_filter=frozenset({(6, 0)}),
    )
    fallback_view = _fragment_view(
        fragment_row,
        fragment_no=1,
        provider_filter=frozenset({7}),
    )
    retained_by_coordinate = {(7, block_key, 0): [], (7, block_key, 1): []}

    capture = sidecars._forward_fanout_capture(
        (exact_view, fallback_view),
        retained_by_coordinate,
    )
    capture(6, 1, 0)
    capture(7, 2, 0)

    assert capture.provider_filter_set == frozenset({6, 7})
    assert retained_by_coordinate[(7, block_key, 0)] == [(6, 1, 0)]
    assert retained_by_coordinate[(7, block_key, 1)] == [(7, 2, 0)]

    unrestricted_view = _fragment_view(fragment_row, fragment_no=2)
    assert sidecars._forward_fanout_capture(
        (exact_view, unrestricted_view),
        {**retained_by_coordinate, (7, block_key, 2): []},
    ).provider_filter_set is None


def test_unselected_multi_source_rows_skip_fanout_but_stay_validated(
    monkeypatch,
):
    fragment_row = _forward_fragment(
        3,
        ((5, ((1, 0), (2, 1))), (6, ((3, 2),))),
    )
    occurrence_consumer = Mock()
    occurrence_visit = Mock(wraps=sidecars._visit_forward_occurrences)
    monkeypatch.setattr(
        sidecars,
        "_visit_forward_occurrences",
        occurrence_visit,
    )

    sidecars._visit_forward_fragment_unchecked(
        fragment_row,
        provider_filter=frozenset({6}),
        fragment_cursor=sidecars._ForwardFragmentCursor(),
        validation=sidecars._ForwardFragmentValidation(
            expected_source_count=3,
            price_item_count=4,
        ),
        occurrence_consumer=occurrence_consumer,
    )

    occurrence_consumer.assert_called_once_with(6, 3, 2)
    assert occurrence_visit.call_count == 2
    assert occurrence_visit.call_args_list[0].kwargs["occurrence_consumer"] is None
    assert occurrence_visit.call_args_list[1].kwargs["occurrence_consumer"] is not None

    corrupt_fragment_row = _forward_fragment(
        3,
        ((5, ((1, 3),)), (6, ((2, 0),))),
    )
    with pytest.raises(sidecars.PTG2SharedBlockError, match="source key"):
        sidecars._visit_forward_fragment_unchecked(
            corrupt_fragment_row,
            provider_filter=frozenset({6}),
            fragment_cursor=sidecars._ForwardFragmentCursor(),
            validation=sidecars._ForwardFragmentValidation(
                expected_source_count=3,
                price_item_count=4,
            ),
            occurrence_consumer=occurrence_consumer,
        )


def test_unselected_first_row_still_guards_two_fragment_continuation():
    first_row = _forward_fragment(
        1,
        ((6, ((5, 0),)),),
        block_hash=b"a" * 32,
    )
    second_row = _forward_fragment(
        1,
        ((6, ((4, 0),)), (7, ((6, 0),))),
        fragment_no=1,
        block_hash=b"b" * 32,
    )
    views = (
        _fragment_view(
            first_row,
            provider_filter=frozenset({6}),
            occurrence_filter=frozenset({(6, 0)}),
        ),
        _fragment_view(
            second_row,
            fragment_no=1,
            provider_filter=frozenset({7}),
            occurrence_filter=frozenset({(7, 0)}),
        ),
    )
    options = sidecars._ForwardBatchOptions(1, 1, 8, 32)
    parsed_by_identity, retained = (
        sidecars._parse_forward_batch_physical_fragments_once(
            views,
            options=options,
            price_item_count=8,
        )
    )

    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="not ordered"):
        sidecars._emit_forward_batch_logical_views(
            views,
            parsed_by_identity,
            retained,
            lambda *_occurrence: None,
        )
