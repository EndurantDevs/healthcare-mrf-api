from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from api import ptg2_db_sidecars
from api.ptg2_db_sidecars import (
    PTG2ManifestArtifactError,
    lookup_serving_binary_by_code_prefix_from_db,
)
from api.ptg2_shared_blocks import (
    PTG2_V3_GRAPH_NPI_TO_GROUP,
    SharedBlockPayload,
    fetch_shared_graph_members,
    stream_shared_blocks,
)
from process.ptg_parts.ptg2_shared_blocks import shared_block_hash


def _shard_block_key(code_key: int, provider_set_key: int) -> int:
    return (int(code_key) << 31) | (int(provider_set_key) // 1024)


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


def _fragment_row(fragment: SharedBlockPayload) -> dict:
    return {
        "block_key": fragment.block_key,
        "block_no": fragment.fragment_no,
        "entry_count": fragment.entry_count,
        "_decoded_payload": fragment.payload,
    }


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
    rows = await lookup_serving_binary_by_code_prefix_from_db(
        object(),
        7,
        limit=limit,
        descending=descending,
        shared_snapshot_key=41,
        source_count=2,
        price_dictionary_item_count=item_count,
        price_dictionary_block_bytes=2048,
    )
    return rows, provider_counts, dictionary


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
    fetch = AsyncMock(return_value=[_fragment_row(item) for item in fragments])
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

    rows = await ptg2_db_sidecars.lookup_serving_binary_by_code_from_db(
        object(),
        7,
        shared_snapshot_key=41,
        source_count=2,
        price_dictionary_item_count=128,
        price_dictionary_block_bytes=2048,
    )

    assert [
        (row.provider_set_key, row.price_key, row.source_key) for row in rows
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

    rows = await ptg2_db_sidecars.lookup_serving_binary_by_code_from_db(
        object(),
        7,
        provider_set_keys=(5, 2050),
        shared_snapshot_key=41,
        source_count=2,
        price_dictionary_item_count=128,
        price_dictionary_block_bytes=2048,
    )

    assert [(row.provider_set_key, row.price_key) for row in rows] == [(5, 8)]
    discover.assert_not_awaited()
    assert fetch.await_args.kwargs["block_keys"] == (block_zero, block_two)
    assert fetch.await_args.kwargs["require_all"] is False


@pytest.mark.asyncio
async def test_sparse_batch_reads_multiple_codes_from_exact_provider_shards(
    monkeypatch,
):
    """Ensure sparse multi-code reads fetch only the requested provider shards."""

    expected_block_keys = tuple(
        sorted(
            {
                _shard_block_key(code_key, provider_set_key)
                for code_key in (7, 8)
                for provider_set_key in (5, 1025)
            }
        )
    )
    returned_fragments = [
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
    discover = AsyncMock(
        side_effect=AssertionError("sparse batch reads must not discover code ranges")
    )
    fetch = AsyncMock(return_value=returned_fragments)
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

    assert [(row.provider_set_key, row.price_key) for row in rows_by_code[7]] == [
        (5, 8)
    ]
    assert [(row.provider_set_key, row.price_key) for row in rows_by_code[8]] == [
        (1025, 2)
    ]
    assert fetch.await_args.kwargs["block_keys"] == expected_block_keys
    assert fetch.await_args.kwargs["require_all"] is False
    discover.assert_not_awaited()


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

    assert [row.provider_set_key for row in rows_by_code[7]] == [5]
    assert [row.provider_set_key for row in rows_by_code[8]] == [1025]
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

    rows = await ptg2_db_sidecars.lookup_serving_binary_by_code_from_db(
        object(),
        7,
        shared_snapshot_key=41,
        source_count=2,
        price_dictionary_item_count=128,
        price_dictionary_block_bytes=2048,
    )

    assert [row.provider_set_key for row in rows] == [3, 5]


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
    entries = [
        (5, [(8, 0), (100, 1)]),
        (6, [(100, 0)]),
        (7, [(2, 0), (9, 1)]),
        (9, [(5, 0)]),
    ]
    payload = _grouped_payload(2, entries)
    fragments = (_fragment(payload, entry_count=len(entries)),)

    rows, provider_counts, dictionary = await _prefix_rows(
        monkeypatch,
        fragments=fragments,
        limit=3,
        descending=descending,
    )

    eager = ptg2_db_sidecars._decode_serving_binary_code_records(
        [{"block_no": 0, "entry_count": len(entries), "_decoded_payload": payload}],
        provider_set_keys=None,
        expected_source_count=2,
    )
    eager_prefix = sorted(
        eager,
        key=lambda item: (
            -item[1] if descending else item[1],
            item[0],
            item[2],
            item[0] * 10,
        ),
    )[:3]
    actual = [
        (row.provider_set_key, row.price_key, row.source_key) for row in rows
    ]

    assert actual == expected == eager_prefix
    assert {
        (provider_set_key, price_key, source_key)
        for provider_set_key, price_key, source_key in provider_counts.await_args.kwargs[
            "decoded_keys"
        ]
    } == set(expected)
    assert set(dictionary.await_args.kwargs["item_keys"]) == {
        price_key for _provider_set_key, price_key, _source_key in expected
    }


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
@pytest.mark.parametrize("limit", [0, -1, True])
async def test_bounded_code_prefix_requires_positive_integer_limit(limit):
    with pytest.raises(ValueError, match="limit must be positive"):
        await lookup_serving_binary_by_code_prefix_from_db(
            object(),
            7,
            limit=limit,
            shared_snapshot_key=41,
            source_count=1,
            price_dictionary_item_count=1,
            price_dictionary_block_bytes=16,
        )


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
    row = {
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
    row.update(extra or {})
    return row


@pytest.mark.asyncio
async def test_shared_block_stream_uses_server_side_iteration():
    row = _stored_row(
        object_kind="by_code_provider_shard_v1",
        block_key=_shard_block_key(7, 0),
        fragment_no=0,
        payload=b"payload",
    )
    session = _StreamingSession([row])

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
    payload = b"".join(
        member.to_bytes(4, "little", signed=False) for member in all_members
    )
    row = _stored_row(
        object_kind="graph_npi_groups_v1",
        block_key=4,
        fragment_no=0,
        payload=payload,
        extra={
            "owner_key": 1234567890,
            "first_chunk": 4,
            "member_offset": 0,
            "member_count": len(all_members),
            "selected_member_count": 2,
        },
    )
    session = _Session([row])

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
