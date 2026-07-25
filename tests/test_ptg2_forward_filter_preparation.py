# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepared provider-filter invariants for strict forward shard reads."""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from api import ptg2_db_sidecars as sidecars


def _encode_uvarint(integer: int) -> bytes:
    encoded_bytes = bytearray()
    remaining_integer = int(integer)
    while remaining_integer >= 0x80:
        encoded_bytes.append((remaining_integer & 0x7F) | 0x80)
        remaining_integer >>= 7
    encoded_bytes.append(remaining_integer)
    return bytes(encoded_bytes)


def _grouped_payload(
    provider_set_key: int,
    price_key: int,
    source_key: int,
) -> bytes:
    payload_bytes = bytearray([2, 2, 1])
    payload_bytes.extend(_encode_uvarint(provider_set_key))
    payload_bytes.extend(_encode_uvarint(1))
    payload_bytes.extend(_encode_uvarint(price_key))
    payload_bytes.append(source_key)
    return bytes(payload_bytes)


def _fragment_row(
    provider_set_key: int,
    price_key: int,
    source_key: int,
    block_key: int,
) -> dict[str, object]:
    return {
        "block_key": block_key,
        "block_no": 0,
        "entry_count": 1,
        "_decoded_payload": _grouped_payload(
            provider_set_key,
            price_key,
            source_key,
        ),
        "_block_hash": b"h" * 32,
    }


def test_forward_shards_preserve_filtered_multi_shard_rows():
    first_block_key = sidecars._forward_provider_shard_block_key(7, 5)
    second_block_key = sidecars._forward_provider_shard_block_key(7, 1025)
    fragment_rows = (
        _fragment_row(5, 8, 0, first_block_key),
        _fragment_row(1025, 9, 1, second_block_key),
    )
    options_by_name = {
        "code_key": 7,
        "expected_block_keys": (first_block_key, second_block_key),
        "expected_source_count": 2,
        "price_item_count": 128,
        "provider_shard_span": 1024,
    }

    unfiltered_rows = sidecars._decode_forward_shards_for_code(
        fragment_rows,
        provider_set_keys=None,
        **options_by_name,
    )
    filtered_rows = sidecars._decode_forward_shards_for_code(
        fragment_rows,
        provider_set_keys=(1025, 5, 1025),
        **options_by_name,
    )

    assert filtered_rows == unfiltered_rows == [(5, 8, 0), (1025, 9, 1)]


def test_forward_shards_prepare_provider_filter_once_for_every_shard(
    monkeypatch,
):
    first_block_key = sidecars._forward_provider_shard_block_key(7, 0)
    second_block_key = sidecars._forward_provider_shard_block_key(7, 1024)
    normalize_filter = Mock(wraps=sidecars._normalized_provider_set_filter)
    monkeypatch.setattr(
        sidecars,
        "_normalized_provider_set_filter",
        normalize_filter,
    )
    visited_filters = []

    def visit_fragment(_fragment_row, **kwargs):
        visited_filters.append(kwargs["provider_filter"])
        return sidecars._ForwardFragmentCursor(), 1

    monkeypatch.setattr(
        sidecars,
        "_visit_serving_binary_by_code_record",
        visit_fragment,
    )
    sidecars._visit_forward_shards_for_code(
        ({"block_key": first_block_key}, {"block_key": second_block_key}),
        options=sidecars._ForwardShardVisitOptions(
            code_key=7,
            expected_block_keys=(first_block_key, second_block_key),
            provider_set_keys=(1025, 5, 1025),
            expected_source_count=1,
            price_item_count=128,
        ),
        occurrence_consumer=lambda *_occurrence: None,
    )

    normalize_filter.assert_called_once_with((1025, 5, 1025))
    assert visited_filters == [frozenset({5, 1025})] * 2
    assert visited_filters[0] is visited_filters[1]


def test_forward_shards_validate_filter_before_reading_fragments():
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="out of range"):
        sidecars._visit_forward_shards_for_code(
            (),
            options=sidecars._ForwardShardVisitOptions(
                code_key=7,
                expected_block_keys=(),
                provider_set_keys=(True,),
                expected_source_count=1,
                price_item_count=128,
            ),
            occurrence_consumer=lambda *_occurrence: None,
        )
