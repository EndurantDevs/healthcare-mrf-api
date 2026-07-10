# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import zlib

import pytest

import api.ptg2_db_sidecars as db_sidecars
from api.ptg2_db_serving_v3 import (
    PTG2_SERVING_BINARY_V3_ATOM_KEY_BLOCK_SPAN,
    PTG2_SERVING_BINARY_V3_ATOM_PAYLOAD_KIND,
    PTG2_SERVING_BINARY_V3_PRICE_KEY_BLOCK_SPAN,
    PTG2_SERVING_BINARY_V3_PRICE_MEMBERSHIPS_KIND,
    PTG2_SERVING_BINARY_V3_PROVIDER_SET_CODES_KIND,
    PTG2_SERVING_BINARY_V3_PROVIDER_SET_KEY_BLOCK_SPAN,
    lookup_price_atom_memberships_from_db,
    lookup_price_atoms_from_db,
    lookup_provider_code_keys_from_db,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_serving_binary_v3 import (
    PTG2_V3_ATOM_KEY_24_BITS,
    PTG2_V3_ATOM_KEY_32_BITS,
    PTG2V3PriceAtomRecord,
    append_uvarint,
    encode_price_atoms,
    encode_price_memberships,
    encode_provider_code_set,
)


@pytest.fixture(autouse=True)
def clear_binary_block_cache():
    db_sidecars._BINARY_DICTIONARY_CACHE.clear()
    db_sidecars._BINARY_DICTIONARY_CACHE_STATE["byte_count"] = 0
    db_sidecars._BINARY_BLOCK_CACHE.clear()
    db_sidecars._BINARY_BLOCK_CACHE_STATE["byte_count"] = 0


class FakeResult:
    def __init__(self, result_rows):
        self.result_rows = list(result_rows)

    def __iter__(self):
        return iter(self.result_rows)


class FakeSession:
    def __init__(self, block_records_by_kind):
        self.block_records_by_kind = block_records_by_kind
        self.calls = []

    async def execute(self, _statement, params):
        query_params_by_name = dict(params)
        self.calls.append(query_params_by_name)
        result_rows = []
        for block_key in query_params_by_name.get("block_keys", []):
            result_rows.extend(
                {**record, "block_key": block_key}
                for record in self.block_records_by_kind.get(
                    (query_params_by_name["artifact_kind"], block_key),
                    [],
                )
            )
        return FakeResult(result_rows)


def _block_record(payload, *, block_no=0, entry_count=0, compressed=False):
    stored_payload = zlib.compress(payload) if compressed else payload
    return {
        "block_no": block_no,
        "entry_count": entry_count,
        "payload": stored_payload,
        "payload_compression": "zlib" if compressed else "none",
        "raw_payload_bytes": len(payload) if compressed else 0,
    }


def _fragmented_block(payload, *, entry_count, split_at, compressed=False):
    return [
        _block_record(payload[:split_at], block_no=0, entry_count=entry_count, compressed=compressed),
        _block_record(payload[split_at:], block_no=1, compressed=compressed),
    ]


def _provider_block(provider_set_key, code_keys):
    code_payload, _stats = encode_provider_code_set(code_keys)
    block_key = provider_set_key // PTG2_SERVING_BINARY_V3_PROVIDER_SET_KEY_BLOCK_SPAN
    payload = bytearray()
    append_uvarint(payload, 1)
    append_uvarint(payload, provider_set_key % PTG2_SERVING_BINARY_V3_PROVIDER_SET_KEY_BLOCK_SPAN)
    append_uvarint(payload, len(code_payload))
    payload.extend(code_payload)
    return block_key, bytes(payload)


def _dictionary_id(item_key):
    return int(item_key).to_bytes(16, "big")


def _dictionary_block(block_no, item_keys, *, compressed=False):
    raw_payload = b"".join(_dictionary_id(item_key) for item_key in item_keys)
    return {
        "block_no": block_no,
        "entry_count": len(item_keys),
        "payload": zlib.compress(raw_payload) if compressed else raw_payload,
        "payload_compression": "zlib" if compressed else "none",
        "raw_payload_bytes": len(raw_payload) if compressed else 0,
    }


class ChunkedDictionarySession:
    def __init__(self, dictionary_blocks):
        self.dictionary_blocks = list(dictionary_blocks)
        self.calls = []
        self.substring_item_keys = []
        self.payload_block_requests = []

    async def execute(self, statement, params):
        """Serve the metadata, substring, and selected-payload query shapes."""

        statement_text = str(statement)
        query_params_by_name = dict(params)
        self.calls.append((statement_text, query_params_by_name))
        if "octet_length(binary_block.payload) AS payload_bytes" in statement_text:
            return self._metadata_result()
        if "substring(" in statement_text:
            return self._substring_result(query_params_by_name)
        if "block_nos" in query_params_by_name:
            return self._payload_blocks_result(query_params_by_name)
        raise AssertionError(f"unexpected dictionary query: {statement_text}")

    def _metadata_result(self):
        return FakeResult(
            {
                "block_no": block["block_no"],
                "entry_count": block["entry_count"],
                "payload_compression": block["payload_compression"],
                "raw_payload_bytes": block["raw_payload_bytes"],
                "payload_bytes": len(block["payload"]),
            }
            for block in self.dictionary_blocks
        )

    def _substring_result(self, query_params_by_name):
        item_keys = tuple(query_params_by_name["item_keys"])
        entries_per_block = int(query_params_by_name["entries_per_block"])
        self.substring_item_keys.append(item_keys)
        value_rows = []
        blocks_by_number = {
            int(block["block_no"]): block for block in self.dictionary_blocks
        }
        for item_key in item_keys:
            block_no = item_key // entries_per_block
            item_offset = item_key % entries_per_block
            block = blocks_by_number[block_no]
            assert block["payload_compression"] == "none"
            value_rows.append(
                {
                    "item_key": item_key,
                    "item_value": block["payload"][item_offset * 16 : item_offset * 16 + 16],
                }
            )
        return FakeResult(value_rows)

    def _payload_blocks_result(self, query_params_by_name):
        requested_block_nos = tuple(query_params_by_name["block_nos"])
        self.payload_block_requests.append(requested_block_nos)
        return FakeResult(
            block
            for block in self.dictionary_blocks
            if block["block_no"] in requested_block_nos
        )


@pytest.mark.asyncio
async def test_chunked_dictionary_fetches_selected_blocks_once_across_boundaries():
    session = ChunkedDictionarySession(
        [
            _dictionary_block(0, (0, 1)),
            _dictionary_block(1, (2, 3)),
            _dictionary_block(2, (4,)),
        ]
    )

    values_by_key = await db_sidecars._serving_binary_dictionary_values_for_keys(
        session,
        "mrf.ptg2_v3_dictionary_slices",
        artifact_kind="by_code_price_dictionary",
        item_keys=(1, 2, 4),
    )

    assert values_by_key == {item_key: _dictionary_id(item_key).hex() for item_key in (1, 2, 4)}
    assert session.substring_item_keys == []
    assert session.payload_block_requests == [(0, 1, 2)]
    assert len(session.calls) == 2
    block_query = session.calls[1][0]
    assert "block_no = ANY(CAST(:block_nos AS integer[]))" in block_query
    assert "binary_block.payload" in block_query


@pytest.mark.asyncio
async def test_chunked_dictionary_fetches_only_selected_compressed_blocks(monkeypatch):
    dictionary_blocks = [
        _dictionary_block(0, (0, 1), compressed=True),
        _dictionary_block(1, (2, 3)),
        _dictionary_block(2, (4,), compressed=True),
    ]
    session = ChunkedDictionarySession(dictionary_blocks)
    decompressed_payloads = []
    original_decompress = db_sidecars.zlib.decompress

    def tracked_decompress(payload):
        decompressed_payloads.append(payload)
        return original_decompress(payload)

    monkeypatch.setattr(db_sidecars.zlib, "decompress", tracked_decompress)
    values_by_key = await db_sidecars._serving_binary_dictionary_values_for_keys(
        session,
        "mrf.ptg2_v3_dictionary_mixed_compression",
        artifact_kind="by_code_price_dictionary",
        item_keys=(0, 2, 4),
    )

    assert values_by_key == {item_key: _dictionary_id(item_key).hex() for item_key in (0, 2, 4)}
    assert session.substring_item_keys == []
    assert session.payload_block_requests == [(0, 1, 2)]
    assert decompressed_payloads == [dictionary_blocks[0]["payload"], dictionary_blocks[2]["payload"]]
    assert len(session.calls) == 2


@pytest.mark.asyncio
async def test_chunked_dictionary_batches_sparse_block_reads():
    dictionary_blocks = [
        _dictionary_block(block_no, (block_no,)) for block_no in range(65)
    ]
    session = ChunkedDictionarySession(dictionary_blocks)

    values_by_key = await db_sidecars._serving_binary_dictionary_values_for_keys(
        session,
        "mrf.ptg2_v3_dictionary_sparse_blocks",
        artifact_kind="by_code_price_dictionary",
        item_keys=range(65),
    )

    assert values_by_key == {
        item_key: _dictionary_id(item_key).hex() for item_key in range(65)
    }
    assert session.payload_block_requests == [tuple(range(64)), (64,)]


@pytest.mark.asyncio
async def test_chunked_dictionary_hint_accepts_full_uncompressed_raw_byte_count():
    dictionary_block = _dictionary_block(0, (0, 1))
    dictionary_block["raw_payload_bytes"] = len(dictionary_block["payload"])
    session = ChunkedDictionarySession([dictionary_block])

    values_by_key = await db_sidecars._serving_binary_dictionary_values_for_keys(
        session,
        "mrf.ptg2_v3_dictionary_hint_raw_bytes",
        artifact_kind="by_code_price_dictionary",
        item_keys=(1,),
        item_count_hint=2,
        block_bytes_hint=32,
        compressed_records_hint=0,
    )

    assert values_by_key == {1: _dictionary_id(1).hex()}
    assert session.payload_block_requests == [(0,)]
    assert len(session.calls) == 1


@pytest.mark.asyncio
async def test_chunked_dictionary_full_payload_concatenates_validated_blocks():
    session = ChunkedDictionarySession(
        [
            _dictionary_block(0, (0, 1)),
            _dictionary_block(1, (2, 3), compressed=True),
            _dictionary_block(2, (4,)),
        ]
    )

    dictionary_payload = await db_sidecars._serving_binary_dictionary_payload(
        session,
        "mrf.ptg2_v3_dictionary_full_payload",
        artifact_kind="by_code_price_dictionary",
    )

    assert dictionary_payload == b"".join(_dictionary_id(item_key) for item_key in range(5))
    assert session.payload_block_requests == [(0, 1, 2)]


@pytest.mark.parametrize(
    ("dictionary_blocks", "error_match"),
    [
        (
            [_dictionary_block(0, (0, 1)), _dictionary_block(2, (2,))],
            "non-contiguous",
        ),
        (
            [
                _dictionary_block(0, (0, 1)),
                _dictionary_block(1, (2,)),
                _dictionary_block(2, (3,)),
            ],
            "short non-final",
        ),
        (
            [_dictionary_block(0, (0, 1)), _dictionary_block(1, ())],
            "final block size",
        ),
        (
            [
                {
                    **_dictionary_block(0, (0, 1)),
                    "payload": _dictionary_id(0),
                }
            ],
            "payload byte count mismatch",
        ),
    ],
)
@pytest.mark.asyncio
async def test_chunked_dictionary_rejects_corrupt_metadata(dictionary_blocks, error_match):
    session = ChunkedDictionarySession(dictionary_blocks)

    with pytest.raises(PTG2ManifestArtifactError, match=error_match):
        await db_sidecars._serving_binary_dictionary_values_for_keys(
            session,
            "mrf.ptg2_v3_dictionary_corrupt_metadata",
            artifact_kind="by_code_price_dictionary",
            item_keys=(0,),
        )


@pytest.mark.asyncio
async def test_chunked_dictionary_rejects_corrupt_selected_compressed_payload():
    corrupt_block = _dictionary_block(0, (0,), compressed=True)
    corrupt_block["payload"] = b"not-zlib"
    session = ChunkedDictionarySession([corrupt_block])

    with pytest.raises(PTG2ManifestArtifactError, match="compressed payload is corrupt"):
        await db_sidecars._serving_binary_dictionary_values_for_keys(
            session,
            "mrf.ptg2_v3_dictionary_corrupt_payload",
            artifact_kind="by_code_price_dictionary",
            item_keys=(0,),
        )


@pytest.mark.asyncio
async def test_v3_readers_concatenate_fragments_at_canonical_block_spans():
    provider_set_key = PTG2_SERVING_BINARY_V3_PROVIDER_SET_KEY_BLOCK_SPAN + 3
    provider_block_key, provider_payload = _provider_block(provider_set_key, (3, 7))
    price_key = PTG2_SERVING_BINARY_V3_PRICE_KEY_BLOCK_SPAN + 5
    membership_payload = encode_price_memberships(((price_key, (4096, 4097)),), PTG2_V3_ATOM_KEY_24_BITS)
    atom_payload = encode_price_atoms(
        (
            PTG2V3PriceAtomRecord("10.00", (1, None)),
            PTG2V3PriceAtomRecord("20.00", (2, 3)),
        )
    )
    session = FakeSession(
        {
            (PTG2_SERVING_BINARY_V3_PROVIDER_SET_CODES_KIND, provider_block_key): _fragmented_block(
                provider_payload,
                entry_count=1,
                split_at=3,
            ),
            (PTG2_SERVING_BINARY_V3_PRICE_MEMBERSHIPS_KIND, 1): _fragmented_block(
                membership_payload,
                entry_count=1,
                split_at=4,
                compressed=True,
            ),
            (PTG2_SERVING_BINARY_V3_ATOM_PAYLOAD_KIND, 1): _fragmented_block(
                atom_payload,
                entry_count=2,
                split_at=5,
            ),
        }
    )

    assert await lookup_provider_code_keys_from_db(
        session,
        "mrf.ptg2_v3_blocks",
        [provider_set_key],
    ) == {provider_set_key: (3, 7)}
    assert await lookup_price_atom_memberships_from_db(
        session,
        "mrf.ptg2_v3_blocks",
        [price_key],
        atom_key_bits=24,
    ) == {price_key: (4096, 4097)}
    assert await lookup_price_atoms_from_db(
        session,
        "mrf.ptg2_v3_blocks",
        [PTG2_SERVING_BINARY_V3_ATOM_KEY_BLOCK_SPAN + 1],
        atom_key_bits=24,
    ) == {
        PTG2_SERVING_BINARY_V3_ATOM_KEY_BLOCK_SPAN + 1: PTG2V3PriceAtomRecord("20.00", (2, 3))
    }
    assert [call["block_keys"] for call in session.calls] == [[1], [1], [1]]


@pytest.mark.asyncio
async def test_v3_membership_reader_rejects_manifest_width_mismatch():
    payload = encode_price_memberships(((1, (2,)),), PTG2_V3_ATOM_KEY_24_BITS)
    session = FakeSession(
        {
            (PTG2_SERVING_BINARY_V3_PRICE_MEMBERSHIPS_KIND, 0): [
                _block_record(payload, entry_count=1)
            ]
        }
    )

    with pytest.raises(PTG2ManifestArtifactError, match="corrupt"):
        await lookup_price_atom_memberships_from_db(
            session,
            "mrf.ptg2_v3_blocks",
            [1],
            atom_key_bits=PTG2_V3_ATOM_KEY_32_BITS,
        )


@pytest.mark.asyncio
async def test_v3_reader_rejects_noncontiguous_or_corrupt_fragments():
    atom_payload = encode_price_atoms((PTG2V3PriceAtomRecord("10.00", (1,)),))
    session = FakeSession(
        {
            (PTG2_SERVING_BINARY_V3_ATOM_PAYLOAD_KIND, 0): [
                _block_record(atom_payload[:2], block_no=0, entry_count=1),
                _block_record(atom_payload[2:], block_no=2),
            ]
        }
    )

    with pytest.raises(PTG2ManifestArtifactError, match="non-contiguous"):
        await lookup_price_atoms_from_db(session, "mrf.ptg2_v3_blocks", [0])


@pytest.mark.asyncio
async def test_v3_reader_allows_zero_in_memory_cache_configuration(monkeypatch):
    provider_block_key, provider_payload = _provider_block(4, (7,))
    session = FakeSession(
        {
            (PTG2_SERVING_BINARY_V3_PROVIDER_SET_CODES_KIND, provider_block_key): [
                _block_record(provider_payload, entry_count=1)
            ]
        }
    )
    monkeypatch.setattr(db_sidecars, "_BINARY_BLOCK_CACHE_MAX_BYTES", 0)

    assert await lookup_provider_code_keys_from_db(session, "mrf.ptg2_v3_blocks", [4]) == {4: (7,)}
    assert await lookup_provider_code_keys_from_db(session, "mrf.ptg2_v3_blocks", [4]) == {4: (7,)}
    assert len(session.calls) == 2


@pytest.mark.asyncio
async def test_v3_reverse_batch_reader_preserves_forward_duplicate_price_keys(monkeypatch):
    forward_block_calls = []

    async def forward_blocks(_session, _table_name, *, artifact_kind, block_keys):
        forward_block_calls.append((artifact_kind, tuple(block_keys)))
        return [
            {"block_key": code_key, "entry_count": 1, "payload": bytes((code_key,))}
            for code_key in block_keys
        ]

    async def provider_counts(_session, _table_name):
        return {3: 5}

    def decode_forward_block(block_row, **kwargs):
        assert kwargs["provider_filter"] == {3}
        price_key = 10 if block_row["block_key"] == 7 else 11
        return [(3, 5, price_key), (3, 5, price_key)]

    async def price_identifiers(_session, _table_name, *, artifact_kind, item_keys):
        assert artifact_kind == "by_code_price_dictionary"
        assert set(item_keys) == {10, 11}
        return {10: "00000000000000000000000000000010", 11: "00000000000000000000000000000011"}

    monkeypatch.setattr(db_sidecars, "_serving_binary_payload_rows_for_keys", forward_blocks)
    monkeypatch.setattr(db_sidecars, "_serving_binary_provider_counts", provider_counts)
    monkeypatch.setattr(db_sidecars, "_decode_serving_binary_by_code_record", decode_forward_block)
    monkeypatch.setattr(db_sidecars, "_serving_binary_dictionary_values_for_keys", price_identifiers)

    forward_rows_by_code = await db_sidecars.lookup_binary_code_batch_from_db(
        object(),
        "mrf.ptg2_v3_blocks",
        [7, 8],
        provider_set_keys=[3],
    )

    assert [forward_entry.price_key for forward_entry in forward_rows_by_code[7]] == [10, 10]
    assert [forward_entry.price_key for forward_entry in forward_rows_by_code[8]] == [11, 11]
    assert forward_block_calls == [("by_code_grouped", (7, 8))]
