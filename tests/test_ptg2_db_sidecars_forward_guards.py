# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_db_sidecars as sidecars
from api.ptg2_shared_blocks import (
    PTG2SharedBlockError,
    decode_dense_source_header,
    read_strict_uvarint,
)


def _encode_uvarint(integer: int) -> bytes:
    encoded_bytes = bytearray()
    remaining_integer = int(integer)
    while remaining_integer >= 0x80:
        encoded_bytes.append((remaining_integer & 0x7F) | 0x80)
        remaining_integer >>= 7
    encoded_bytes.append(remaining_integer)
    return bytes(encoded_bytes)


def _encode_source_vector(source_keys: tuple[int, ...], source_count: int) -> bytes:
    source_bits = 0 if source_count == 1 else (source_count - 1).bit_length()
    encoded_bytes = bytearray((len(source_keys) * source_bits + 7) // 8)
    bit_offset = 0
    for source_key in source_keys:
        for source_bit in range(source_bits):
            if source_key & (1 << source_bit):
                encoded_bytes[bit_offset // 8] |= 1 << (bit_offset % 8)
            bit_offset += 1
    return bytes(encoded_bytes)


def _grouped_forward_payload(
    source_count: int,
    entries: tuple[tuple[int, tuple[tuple[int, int], ...]], ...],
) -> bytes:
    source_bits = 0 if source_count == 1 else (source_count - 1).bit_length()
    payload_bytes = bytearray([2])
    payload_bytes.extend(_encode_uvarint(source_count))
    payload_bytes.append(source_bits)
    previous_provider_set_key = 0
    for provider_set_key, occurrences in entries:
        payload_bytes.extend(
            _encode_uvarint(provider_set_key - previous_provider_set_key)
        )
        payload_bytes.extend(_encode_uvarint(len(occurrences)))
        for price_key, _source_key in occurrences:
            payload_bytes.extend(_encode_uvarint(price_key))
        payload_bytes.extend(
            _encode_source_vector(
                tuple(source_key for _price_key, source_key in occurrences),
                source_count,
            )
        )
        previous_provider_set_key = provider_set_key
    return bytes(payload_bytes)


def _forward_fragment_row(
    payload_bytes: bytes,
    *,
    entry_count: int = 1,
    block_key: int | None = None,
) -> dict[str, object]:
    return {
        "block_key": (
            sidecars._forward_provider_shard_block_key(7, 0)
            if block_key is None
            else block_key
        ),
        "block_no": 0,
        "entry_count": entry_count,
        "_decoded_payload": payload_bytes,
        "_block_hash": b"h" * 32,
    }


def _batch_options(**overrides: object) -> sidecars._ForwardBatchOptions:
    options_by_name: dict[str, object] = {
        "shared_snapshot_key": 1,
        "source_count": 2,
        "price_dictionary_item_count": 128,
        "price_dictionary_block_bytes": 32,
    }
    options_by_name.update(overrides)
    return sidecars._ForwardBatchOptions(**options_by_name)


def test_forward_compatibility_decoders_preserve_dense_order():
    payload_bytes = _grouped_forward_payload(
        2,
        ((5, ((8, 0), (9, 1))),),
    )
    source_count, source_bits, cursor = decode_dense_source_header(
        payload_bytes,
        0,
        format_version=2,
        expected_source_count=2,
    )
    provider_delta, cursor = read_strict_uvarint(payload_bytes, cursor)

    decoded_occurrences, end_cursor = sidecars._decode_forward_occurrences(
        payload_bytes,
        cursor,
        source_count=source_count,
        source_bits=source_bits,
    )
    decoded_keys, last_provider_key, last_occurrence, observed_source_count = (
        sidecars._decode_serving_binary_by_code_record(
            _forward_fragment_row(payload_bytes),
            provider_filter={5},
            expected_source_count=2,
            previous_provider_set_key=None,
            previous_occurrence=None,
            price_item_count=128,
        )
    )

    assert provider_delta == 5
    assert decoded_occurrences == [(8, 0), (9, 1)]
    assert end_cursor == len(payload_bytes)
    assert decoded_keys == [(5, 8, 0), (5, 9, 1)]
    assert last_provider_key == 5
    assert last_occurrence == (9, 1)
    assert observed_source_count == 2


def test_forward_compatibility_decoder_preserves_manifest_errors():
    with pytest.raises(
        sidecars.PTG2ManifestArtifactError,
        match="invalid entry count",
    ):
        sidecars._decode_serving_binary_by_code_record(
            _forward_fragment_row(b"\x02\x02\x01", entry_count=0),
            provider_filter=None,
            expected_source_count=2,
            previous_provider_set_key=None,
            previous_occurrence=None,
        )


def test_forward_compatibility_decoder_wraps_unexpected_errors(monkeypatch):
    def _raise_unexpected_error(*_args, **_kwargs):
        raise RuntimeError("unexpected decoder failure")

    monkeypatch.setattr(
        sidecars,
        "_decode_serving_binary_by_code_record_unchecked",
        _raise_unexpected_error,
    )
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="corrupt"):
        sidecars._decode_serving_binary_by_code_record(
            {},
            provider_filter=None,
            expected_source_count=2,
            previous_provider_set_key=None,
            previous_occurrence=None,
        )


@pytest.mark.parametrize(
    ("source_vector", "source_bits", "expected_message"),
    (
        (b"", 2, "width is invalid"),
        (b"", 1, "is truncated"),
    ),
)
def test_dense_source_vector_guards(
    source_vector: bytes,
    source_bits: int,
    expected_message: str,
):
    with pytest.raises(PTG2SharedBlockError, match=expected_message):
        sidecars._dense_source_vector_view(
            source_vector,
            0,
            entry_count=1,
            source_count=2,
            source_bits=source_bits,
        )


def test_forward_occurrence_and_fragment_counts_must_be_positive():
    with pytest.raises(
        sidecars.PTG2ManifestArtifactError,
        match="occurrence count",
    ):
        sidecars._visit_forward_occurrences(
            b"\x00",
            0,
            source_count=2,
            source_bits=1,
            occurrence_consumer=lambda _price_key, _source_key: None,
        )

    with pytest.raises(
        sidecars.PTG2ManifestArtifactError,
        match="invalid entry count",
    ):
        sidecars._visit_forward_fragment_unchecked(
            _forward_fragment_row(b"\x02\x02\x01", entry_count=0),
            provider_filter=None,
            fragment_cursor=sidecars._ForwardFragmentCursor(),
            validation=sidecars._ForwardFragmentValidation(
                expected_source_count=2
            ),
            occurrence_consumer=lambda *_occurrence: None,
        )


def test_forward_fragment_rejects_provider_regression():
    payload_bytes = _grouped_forward_payload(2, ((5, ((8, 0),)),))
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="not ordered"):
        sidecars._visit_forward_fragment_unchecked(
            _forward_fragment_row(payload_bytes),
            provider_filter=None,
            fragment_cursor=sidecars._ForwardFragmentCursor(
                provider_set_key=6,
                occurrence=(8, 0),
            ),
            validation=sidecars._ForwardFragmentValidation(
                expected_source_count=2
            ),
            occurrence_consumer=lambda *_occurrence: None,
        )


@pytest.mark.parametrize(
    ("missing_provider_key", "missing_occurrence"),
    ((True, False), (False, True)),
)
def test_forward_decoder_requires_complete_ordering_state(
    monkeypatch,
    missing_provider_key: bool,
    missing_occurrence: bool,
):
    incomplete_cursor = sidecars._ForwardFragmentCursor(
        provider_set_key=None if missing_provider_key else 5,
        occurrence=None if missing_occurrence else (8, 0),
    )
    monkeypatch.setattr(
        sidecars,
        "_visit_forward_fragment_unchecked",
        lambda *_args, **_kwargs: (incomplete_cursor, 2),
    )
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="ordering state"):
        sidecars._decode_forward_fragment_unchecked(
            {},
            provider_filter=None,
            expected_source_count=2,
            previous_provider_set_key=None,
            previous_occurrence=None,
        )


@pytest.mark.parametrize(
    ("normalizer_name", "option_overrides", "code_keys", "expected_message"),
    (
        (
            "_normalized_batch_provider_filters",
            {"provider_set_keys_by_code": {7: (5,), "7": (6,)}},
            (7,),
            "duplicate code key",
        ),
        (
            "_normalized_batch_provider_filters",
            {"provider_set_keys_by_code": {7: (5,)}},
            (7, 8),
            "cover exactly",
        ),
        (
            "_normalized_batch_source_filters",
            {"source_keys_by_code": {7: (0,)}, "source_count": True},
            (7,),
            "valid source count",
        ),
        (
            "_normalized_batch_source_filters",
            {"source_keys_by_code": {7: (0,), "7": (1,)}},
            (7,),
            "duplicate code key",
        ),
        (
            "_normalized_batch_source_filters",
            {"source_keys_by_code": {7: ()}},
            (7,),
            "must not be empty",
        ),
        (
            "_normalized_batch_source_filters",
            {"source_keys_by_code": {7: (0,)}},
            (7, 8),
            "cover exactly",
        ),
        (
            "_normalized_batch_occurrence_filters",
            {"occurrence_keys": ((7, 5, 0),), "source_count": True},
            (7,),
            "valid source count",
        ),
        (
            "_normalized_batch_occurrence_filters",
            {"occurrence_keys": ((7, 5),)},
            (7,),
            "invalid coordinate",
        ),
        (
            "_normalized_batch_occurrence_filters",
            {"occurrence_keys": ((8, 5, 0),)},
            (7,),
            "unexpected code key",
        ),
        (
            "_normalized_batch_occurrence_filters",
            {"occurrence_keys": ((7, 5, 0),)},
            (7, 8),
            "cover exactly",
        ),
    ),
)
def test_batch_filter_normalizers_fail_closed(
    normalizer_name: str,
    option_overrides: dict[str, object],
    code_keys: tuple[int, ...],
    expected_message: str,
):
    normalizer = getattr(sidecars, normalizer_name)
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match=expected_message):
        normalizer(_batch_options(**option_overrides), code_keys)


def test_exact_occurrence_scope_and_shard_guards():
    sidecars._validate_forward_occurrence_scope(None, None, None)
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="source scope"):
        sidecars._validate_forward_occurrence_scope(
            frozenset({5}),
            frozenset({0}),
            frozenset({(5, 1)}),
        )
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="empty"):
        sidecars._forward_block_occurrence_filter(
            frozenset({(5, 0)}),
            1024,
            2048,
        )

    expected_block_key = sidecars._forward_provider_shard_block_key(7, 5)
    unexpected_block_key = sidecars._forward_provider_shard_block_key(7, 1025)
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="unexpected"):
        sidecars._validated_forward_rows_by_block(
            7,
            ({"block_key": unexpected_block_key},),
            (expected_block_key,),
            None,
        )


@pytest.mark.asyncio
async def test_forward_batch_empty_shard_paths_do_not_fetch(monkeypatch):
    discover_shards = AsyncMock(return_value={7: ()})
    fetch_fragments = AsyncMock()
    monkeypatch.setattr(
        sidecars,
        "_discover_forward_shard_keys",
        discover_shards,
    )
    monkeypatch.setattr(
        sidecars,
        "_shared_serving_binary_payload_rows_for_keys",
        fetch_fragments,
    )

    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="missing"):
        await sidecars._fetch_forward_batch_fragment_views(
            object(),
            (7,),
            _batch_options(),
            None,
            None,
        )
    assert await sidecars._fetch_forward_batch_fragment_views(
        object(),
        (7,),
        _batch_options(provider_set_keys_by_code={7: ()}),
        None,
        None,
    ) == ()
    fetch_fragments.assert_not_awaited()


@pytest.mark.asyncio
async def test_empty_batch_entry_points_do_not_read():
    read_options_by_name = {
        "shared_snapshot_key": 1,
        "source_count": 2,
        "price_dictionary_item_count": 128,
        "price_dictionary_block_bytes": 32,
    }
    assert await sidecars.lookup_binary_code_batch_from_db(
        object(), (), **read_options_by_name
    ) == {}
    assert await sidecars.lookup_forward_occurrences_batch_from_db(
        object(), (), **read_options_by_name
    ) == {}
    assert await sidecars.lookup_forward_price_index_from_db(
        object(), (), **read_options_by_name
    ) == {}
    assert await sidecars.lookup_price_ids_from_db(
        object(),
        (),
        shared_snapshot_key=1,
        price_dictionary_item_count=128,
        price_dictionary_block_bytes=32,
    ) == {}


@pytest.mark.asyncio
async def test_provider_count_dictionary_empty_success_and_missing_paths():
    empty_session = SimpleNamespace(execute=AsyncMock())
    assert await sidecars._shared_provider_counts_for_keys(
        empty_session,
        shared_snapshot_key=1,
        schema_name="mrf",
        provider_set_keys=(),
    ) == {}
    empty_session.execute.assert_not_awaited()

    populated_session = SimpleNamespace(
        execute=AsyncMock(
            return_value=({"provider_set_key": 5, "provider_count": 12},)
        )
    )
    assert await sidecars._shared_provider_counts_for_keys(
        populated_session,
        shared_snapshot_key=1,
        schema_name="mrf",
        provider_set_keys=(5,),
    ) == {5: 12}

    missing_session = SimpleNamespace(execute=AsyncMock(return_value=()))
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="missing"):
        await sidecars._shared_provider_counts_for_keys(
            missing_session,
            shared_snapshot_key=1,
            schema_name="mrf",
            provider_set_keys=(5,),
        )


@pytest.mark.asyncio
async def test_provided_provider_counts_are_filtered_and_complete():
    assert await sidecars._provider_counts_for_decoded_keys(
        object(),
        shared_snapshot_key=1,
        schema_name="mrf",
        decoded_keys=((5, 8, 0),),
        provider_counts_by_key={5: 12, 6: 20},
    ) == {5: 12}
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="missing"):
        await sidecars._provider_counts_for_decoded_keys(
            object(),
            shared_snapshot_key=1,
            schema_name="mrf",
            decoded_keys=((5, 8, 0),),
            provider_counts_by_key={6: 20},
        )
