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


def _shard_block_key(
    code_key: int,
    provider_set_key: int,
    provider_shard_span: int = 1024,
) -> int:
    return (int(code_key) << 31) | (
        int(provider_set_key) // int(provider_shard_span)
    )


def _uvarint(value: int) -> bytes:
    encoded = bytearray()
    remaining = int(value)
    while remaining >= 0x80:
        encoded.append((remaining & 0x7F) | 0x80)
        remaining >>= 7
    encoded.append(remaining)
    return bytes(encoded)


def _source_vector(source_keys: list[int], source_count: int) -> bytes:
    source_bits = 0 if source_count == 1 else (source_count - 1).bit_length()
    encoded = bytearray((len(source_keys) * source_bits + 7) // 8)
    bit_offset = 0
    for source_key in source_keys:
        for source_bit in range(source_bits):
            if source_key & (1 << source_bit):
                encoded[bit_offset // 8] |= 1 << (bit_offset % 8)
            bit_offset += 1
    return bytes(encoded)


def _grouped_payload(
    source_count: int,
    entries: list[tuple[int, list[tuple[int, int]]]],
) -> bytes:
    source_bits = 0 if source_count == 1 else (source_count - 1).bit_length()
    payload = bytearray([2])
    payload.extend(_uvarint(source_count))
    payload.append(source_bits)
    previous_provider_set_key = 0
    for provider_set_key, occurrences in entries:
        payload.extend(_uvarint(provider_set_key - previous_provider_set_key))
        payload.extend(_uvarint(len(occurrences)))
        for price_key, _source_key in occurrences:
            payload.extend(_uvarint(price_key))
        payload.extend(
            _source_vector(
                [source_key for _price_key, source_key in occurrences],
                source_count,
            )
        )
        previous_provider_set_key = provider_set_key
    return bytes(payload)


def _fragment(
    payload: bytes,
    *,
    fragment_no: int = 0,
    entry_count: int,
    block_key: int | None = None,
) -> SharedBlockPayload:
    return SharedBlockPayload(
        block_key=(
            _shard_block_key(7, 0) if block_key is None else block_key
        ),
        fragment_no=fragment_no,
        entry_count=entry_count,
        payload=payload,
    )


def _fragment_row(
    fragment: SharedBlockPayload,
    *,
    block_hash: bytes | None = None,
) -> dict:
    physical_block_hash = block_hash or shared_block_hash(
        format_version=2,
        object_kind="by_code_provider_shard_v1",
        codec="none",
        payload=fragment.payload,
    )
    fragment_fields_by_name = {
        "block_key": fragment.block_key,
        "block_no": fragment.fragment_no,
        "entry_count": fragment.entry_count,
        "_decoded_payload": fragment.payload,
        "_block_hash": physical_block_hash,
    }
    return fragment_fields_by_name


def _dense_forward_fragment(
    provider_set_keys: tuple[int, ...],
    source_keys: tuple[int, ...],
) -> dict:
    forward_block_bytes = _grouped_payload(
        len(source_keys),
        [
            (
                provider_set_key,
                [(source_key + 1, source_key) for source_key in source_keys],
            )
            for provider_set_key in provider_set_keys
        ],
    )
    return _fragment_row(
        _fragment(
            forward_block_bytes,
            entry_count=len(provider_set_keys),
            block_key=_shard_block_key(7, provider_set_keys[0]),
        )
    )


def _install_forward_fragment_reader(monkeypatch, returned_fragments):
    visit_spy = Mock(
        wraps=ptg2_db_sidecars._visit_serving_binary_by_code_record
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_shared_serving_binary_payload_rows_for_keys",
        AsyncMock(return_value=returned_fragments),
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_visit_serving_binary_by_code_record",
        visit_spy,
    )
    return visit_spy


def _capture_forward_fanout(monkeypatch):
    captures = []
    original_capture = ptg2_db_sidecars._forward_fanout_capture

    def capture_spy(*args):
        capture = original_capture(*args)
        captures.append(capture)
        return capture

    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_forward_fanout_capture",
        capture_spy,
    )
    return captures


def _aliased_forward_fragments(code_keys: tuple[int, ...]):
    source_count = len(code_keys)
    forward_block_bytes = _grouped_payload(
        source_count,
        [(5, [(source_key + 1, source_key) for source_key in range(source_count)])],
    )
    block_hash = b"x" * 32
    returned_fragments = [
        _fragment_row(
            _fragment(
                forward_block_bytes,
                entry_count=1,
                block_key=_shard_block_key(code_key, 5),
            ),
            block_hash=block_hash,
        )
        for code_key in code_keys
    ]
    required_occurrences = {
        (code_key, 5, source_key)
        for source_key, code_key in enumerate(code_keys)
    }
    return source_count, returned_fragments, required_occurrences


def _patch_reference_lookups(monkeypatch):
    provider_counts = AsyncMock(
        side_effect=lambda _session, **kwargs: {
            provider_set_key: provider_set_key * 10
            for provider_set_key, _price_key, _source_key in kwargs[
                "decoded_keys"
            ]
        }
    )
    dictionary = AsyncMock(
        side_effect=lambda _session, **kwargs: {
            price_key: f"{price_key:032x}" for price_key in kwargs["item_keys"]
        }
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_provider_counts_for_decoded_keys",
        provider_counts,
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_serving_binary_dictionary_values_for_keys",
        dictionary,
    )
    return provider_counts, dictionary


async def _prefix_rows(
    monkeypatch,
    *,
    fragments: tuple[SharedBlockPayload, ...],
    limit: int,
    descending: bool,
    item_count: int = 128,
    scan_budget: ForwardReadBudget | None = None,
):
    block_keys = tuple(sorted({fragment.block_key for fragment in fragments}))

    async def _stream(_session, **kwargs):
        assert kwargs["object_kind"] == "by_code_provider_shard_v1"
        assert kwargs["block_keys"] == block_keys
        assert kwargs["require_all"] is True
        for fragment in fragments:
            yield fragment

    provider_counts, dictionary = _patch_reference_lookups(monkeypatch)
    monkeypatch.setattr(ptg2_db_sidecars, "stream_shared_blocks", _stream)
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_discover_forward_shard_keys",
        AsyncMock(return_value={7: block_keys}),
    )
    prefix_rows = await lookup_code_prefix_rows_from_db(
        object(),
        7,
        limit=limit,
        descending=descending,
        shared_snapshot_key=41,
        source_count=2,
        price_dictionary_item_count=item_count,
        price_dictionary_block_bytes=2048,
        scan_budget=scan_budget,
    )
    return prefix_rows, provider_counts, dictionary


def _two_code_sparse_fragments():
    return [
        _fragment_row(
            _fragment(
                _grouped_payload(2, [(5, [(8, 0)])]),
                entry_count=1,
                block_key=_shard_block_key(7, 5),
            )
        ),
        _fragment_row(
            _fragment(
                _grouped_payload(2, [(1025, [(2, 1)])]),
                entry_count=1,
                block_key=_shard_block_key(8, 1025),
            )
        ),
    ]


def _two_code_sparse_block_keys():
    return tuple(
        sorted(
            {
                _shard_block_key(code_key, provider_set_key)
                for code_key in (7, 8)
                for provider_set_key in (5, 1025)
            }
        )
    )


def _assert_two_code_sparse_rows(rows_by_code):
    decoded_row_map = {
        code_key: [
            (row.provider_set_key, row.price_key)
            for row in rows_by_code[code_key]
        ]
        for code_key in (7, 8)
    }
    assert decoded_row_map == {7: [(5, 8)], 8: [(1025, 2)]}


@pytest.mark.asyncio
async def test_sparse_batch_reads_multiple_codes_from_exact_provider_shards(
    monkeypatch,
):
    """Ensure sparse multi-code reads fetch only the requested provider shards."""

    discover = AsyncMock(
        side_effect=AssertionError("sparse batch reads must not discover code ranges")
    )
    fetch = AsyncMock(return_value=_two_code_sparse_fragments())
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
        provider_set_keys=(5, 1025),
        shared_snapshot_key=41,
        source_count=2,
        price_dictionary_item_count=128,
        price_dictionary_block_bytes=2048,
    )

    _assert_two_code_sparse_rows(rows_by_code)
    assert fetch.await_args.kwargs["block_keys"] == _two_code_sparse_block_keys()
    assert fetch.await_args.kwargs["require_all"] is False
    discover.assert_not_awaited()


def _eager_ranked_prefix(encoded_payload, entries, descending):
    eager_rows = ptg2_db_sidecars._decode_serving_binary_code_records(
        [
            {
                "block_no": 0,
                "entry_count": len(entries),
                "_decoded_payload": encoded_payload,
            }
        ],
        provider_set_keys=None,
        expected_source_count=2,
    )
    return sorted(
        eager_rows,
        key=lambda item: (
            -item[1] if descending else item[1],
            item[0],
            item[2],
            item[0] * 10,
        ),
    )[:3]


def _assert_prefix_reference_reads(
    bounded_rows,
    provider_counts,
    dictionary,
    expected,
):
    actual_prefix_rows = [
        (
            bounded_row.provider_set_key,
            bounded_row.price_key,
            bounded_row.source_key,
        )
        for bounded_row in bounded_rows
    ]
    assert actual_prefix_rows == expected
    assert {
        (provider_set_key, price_key, source_key)
        for provider_set_key, price_key, source_key in provider_counts.await_args.kwargs[
            "decoded_keys"
        ]
    } == set(expected)
    assert set(dictionary.await_args.kwargs["item_keys"]) == {
        price_key for _provider_set_key, price_key, _source_key in expected
    }


class _Rows:
    def __init__(self, rows):
        self.rows = list(rows)

    def __iter__(self):
        return iter(self.rows)

    def scalar(self):
        return self.rows[0] if self.rows else None


class _Session:
    def __init__(self, rows):
        self.rows = list(rows)
        self.calls = []

    async def execute(self, statement, params):
        self.calls.append((str(statement), dict(params)))
        return _Rows(self.rows)


class _AsyncRows:
    def __init__(self, rows):
        self.rows = list(rows)

    def __aiter__(self):
        async def _rows():
            for row in self.rows:
                yield row

        return _rows()


class _StreamingSession:
    def __init__(self, rows):
        self.rows = list(rows)
        self.calls = []

    async def stream(self, statement, params):
        self.calls.append((str(statement), dict(params)))
        return _AsyncRows(self.rows)

    async def execute(self, _statement, _params):
        raise AssertionError("bounded block reads must use the streaming API")


def _stored_row(
    *,
    object_kind: str,
    block_key: int,
    fragment_no: int,
    payload: bytes,
    extra: dict | None = None,
):
    stored_row_map = {
        "object_kind": object_kind,
        "block_key": block_key,
        "fragment_no": fragment_no,
        "mapping_entry_count": 1,
        "format_version": 2,
        "codec": "none",
        "raw_byte_count": len(payload),
        "payload": payload,
        "block_hash": shared_block_hash(
            format_version=2,
            object_kind=object_kind,
            codec="none",
            payload=payload,
        ),
    }
    stored_row_map.update(extra or {})
    return stored_row_map
