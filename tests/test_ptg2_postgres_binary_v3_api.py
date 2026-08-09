# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving
from api import ptg2_db_sidecars
from api.ptg2_db_sidecars import PTG2ServingBinaryRow


def _uvarint(value):
    encoded = bytearray()
    remaining = int(value)
    while remaining >= 0x80:
        encoded.append((remaining & 0x7F) | 0x80)
        remaining >>= 7
    encoded.append(remaining)
    return bytes(encoded)


def _source_vector(source_keys, source_count):
    source_bits = 0 if source_count == 1 else (source_count - 1).bit_length()
    encoded = bytearray((len(source_keys) * source_bits + 7) // 8)
    bit_offset = 0
    for source_key in source_keys:
        for source_bit in range(source_bits):
            if source_key & (1 << source_bit):
                encoded[bit_offset // 8] |= 1 << (bit_offset % 8)
            bit_offset += 1
    return source_bits, bytes(encoded)


def _provider_shard_payload(source_count, price_keys, source_keys, *, source_bits=None):
    expected_bits, encoded_sources = _source_vector(source_keys, source_count)
    return b"".join(
        [
            b"\x02",
            _uvarint(source_count),
            bytes([expected_bits if source_bits is None else source_bits]),
            _uvarint(3),
            _uvarint(len(price_keys)),
            *(_uvarint(price_key) for price_key in price_keys),
            encoded_sources,
        ]
    )


def _decode_provider_shard(payload, *, source_count, entry_count=1):
    return ptg2_db_sidecars._decode_serving_binary_code_records(
        [
            {
                "block_no": 0,
                "entry_count": entry_count,
                "_decoded_payload": payload,
            }
        ],
        provider_set_keys=None,
        expected_source_count=source_count,
    )


@pytest.mark.parametrize(
    ("source_count", "source_keys"),
    [(1, [0]), (2, [0, 1]), (256, [0, 255]), (257, [0, 256])],
)
def test_v3_provider_shard_decodes_exact_source_bit_width_boundaries(
    source_count,
    source_keys,
):
    price_keys = list(range(10, 10 + len(source_keys)))
    decoded = _decode_provider_shard(
        _provider_shard_payload(source_count, price_keys, source_keys),
        source_count=source_count,
    )

    assert decoded == [
        (3, price_key, source_key)
        for price_key, source_key in zip(price_keys, source_keys)
    ]


def test_v3_provider_shard_preserves_cross_source_duplicates_and_multiplicity():
    assert _decode_provider_shard(
        _provider_shard_payload(2, [10, 10, 10], [0, 0, 1]),
        source_count=2,
    ) == [(3, 10, 0), (3, 10, 0), (3, 10, 1)]


@pytest.mark.asyncio
async def test_forward_batch_decoder_receives_and_enforces_source_count(monkeypatch):
    block_key = 7 << 31
    block_hash = b"h" * 32
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_discover_forward_shard_keys",
        AsyncMock(return_value={7: (block_key,)}),
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_shared_serving_binary_payload_rows_for_keys",
        AsyncMock(
            return_value=[
                {
                    "block_key": block_key,
                    "block_no": 0,
                    "entry_count": 1,
                    "_decoded_payload": _provider_shard_payload(2, [0], [1]),
                    "_block_hash": block_hash,
                }
            ]
        ),
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_provider_counts_for_decoded_keys",
        AsyncMock(return_value={3: 4}),
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_serving_binary_dictionary_values_for_keys",
        AsyncMock(return_value={0: "0" * 31 + "a"}),
    )

    decoded = await ptg2_db_sidecars.lookup_binary_code_batch_from_db(
        object(),
        [7],
        shared_snapshot_key=41,
        source_count=2,
        price_dictionary_item_count=1,
        price_dictionary_block_bytes=64,
    )

    assert decoded[7][0].source_key == 1
    with pytest.raises(
        ptg2_db_sidecars.PTG2ManifestArtifactError,
        match="corrupt",
    ):
        await ptg2_db_sidecars.lookup_binary_code_batch_from_db(
            object(),
            [7],
            shared_snapshot_key=41,
            source_count=3,
            price_dictionary_item_count=1,
            price_dictionary_block_bytes=64,
        )


@pytest.mark.parametrize(
    "payload",
    [
        _provider_shard_payload(2, [10], [0], source_bits=8),
        _provider_shard_payload(2, [10], [0]) + b"\x00",
        _provider_shard_payload(2, [11, 10], [0, 1]),
        _provider_shard_payload(3, [10], [3]),
        _provider_shard_payload(2, [10], [0])[:-1] + b"\x80",
    ],
)
def test_v3_provider_shard_rejects_corrupt_source_vectors_and_trailing_bytes(payload):
    with pytest.raises(ptg2_db_sidecars.PTG2ManifestArtifactError, match="corrupt|ordered|trailing"):
        _decode_provider_shard(payload, source_count=2 if payload[1] == 2 else 3)


class FakeResult:
    def __init__(self, rows):
        self.rows = list(rows)

    def __iter__(self):
        return iter(self.rows)


class FakeSession:
    def __init__(self, rows=()):
        self.rows = list(rows)
        self.calls = []

    async def execute(self, statement, params):
        self.calls.append((statement, dict(params)))
        return FakeResult(self.rows)


def _version_three_tables(**table_overrides_by_key):
    table_kwargs_by_key = {
        "arch_version": "postgres_binary_v3",
        "storage": "manifest_snapshot",
        "shared_snapshot_key": 41,
        "storage_generation": "shared_blocks_v3",
        "cold_lookup_contract": "ptg_v3_cold_v2",
        "serving_table_layout": "lean_provider_key_v1",
        "shared_block_layout": "dense_shared_blocks_v3",
        "source_count": 2,
        "atom_key_bits": 24,
        "price_key_block_span": 512,
        "atom_key_block_span": 512,
        "price_dictionary_item_count": 8192,
        "price_dictionary_block_bytes": 65536,
    }
    table_kwargs_by_key.update(table_overrides_by_key)
    return ptg2_serving.PTG2ServingTables(**table_kwargs_by_key)


@pytest.mark.parametrize(
    ("provider_shard_span", "expected_hint"),
    ((None, None), (8192, 8192)),
)
def test_v3_forward_lookup_hints_preserve_legacy_and_manifest_spans(
    provider_shard_span,
    expected_hint,
):
    """Use the legacy lower-layer default unless the manifest declares a span."""

    lookup_hints_by_key = ptg2_serving._version_three_forward_lookup_hints(
        _version_three_tables(provider_shard_span=provider_shard_span)
    )

    assert lookup_hints_by_key.get("provider_shard_span") == expected_hint


@pytest.mark.asyncio
async def test_v3_forward_uses_existing_forward_rows_and_keeps_price_key(monkeypatch):
    async def forward_rows(_session, code_key, *, provider_set_keys=None, **dictionary_hints):
        assert code_key == 7
        assert provider_set_keys is None
        assert dictionary_hints["price_dictionary_item_count"] == 8192
        assert dictionary_hints["shared_snapshot_key"] == 41
        return (
            PTG2ServingBinaryRow(
                code_key=7,
                provider_set_key=3,
                provider_count=4,
                price_set_global_id_128="00000000000000000000000000000011",
                source_key=1,
                price_key=19,
            ),
        )

    async def provider_sets(_session, _tables, keys):
        assert keys == [3]
        return {3: "00000000000000000000000000000003"}

    monkeypatch.setattr(ptg2_serving, "lookup_serving_binary_by_code_from_db", forward_rows)
    monkeypatch.setattr(ptg2_serving, "_provider_set_ids_for_keys", provider_sets)

    serving_rows = await ptg2_serving._shared_rows_for_code(
        object(),
        _version_three_tables(),
        code_data={"code_key": 7, "plan_id": "plan", "reported_code_system": "CPT", "reported_code": "99213"},
        provider_set_keys=None,
        source_trace_set_hash=None,
        network_names=[],
    )

    assert serving_rows and serving_rows[0]["price_key"] == 19
    assert serving_rows[0]["source_key"] == 1


@pytest.mark.asyncio
async def test_v3_forward_raises_when_referenced_code_block_is_missing(monkeypatch):
    async def missing_forward_rows(
        _session, _code_key, *, provider_set_keys=None, **_dictionary_hints
    ):
        return ()

    async def has_referenced_code_block(_session, _code_key, **_kwargs):
        return False

    monkeypatch.setattr(ptg2_serving, "lookup_serving_binary_by_code_from_db", missing_forward_rows)
    monkeypatch.setattr(ptg2_serving, "serving_binary_code_block_exists", has_referenced_code_block)

    with pytest.raises(ptg2_serving.PTG2ManifestArtifactError, match="referenced code block"):
        await ptg2_serving._shared_rows_for_code(
            object(),
            _version_three_tables(),
            code_data={"code_key": 7},
            provider_set_keys=None,
            source_trace_set_hash=None,
            network_names=[],
        )


@pytest.mark.asyncio
async def test_v3_forward_raises_when_provider_dictionary_key_is_missing(monkeypatch):
    async def forward_rows(
        _session, _code_key, *, provider_set_keys=None, **_dictionary_hints
    ):
        return (
            PTG2ServingBinaryRow(
                code_key=7,
                provider_set_key=3,
                provider_count=1,
                price_set_global_id_128="00000000000000000000000000000011",
                source_key=0,
                price_key=19,
            ),
        )

    async def missing_provider_set(_session, _tables, _keys):
        return {}

    monkeypatch.setattr(ptg2_serving, "lookup_serving_binary_by_code_from_db", forward_rows)
    monkeypatch.setattr(ptg2_serving, "_provider_set_ids_for_keys", missing_provider_set)

    with pytest.raises(ptg2_serving.PTG2ManifestArtifactError, match="provider-set dictionary"):
        await ptg2_serving._shared_rows_for_code(
            object(),
            _version_three_tables(),
            code_data={"code_key": 7},
            provider_set_keys=None,
            source_trace_set_hash=None,
            network_names=[],
        )


_REVERSE_PROVIDER_SET_IDS = (
    "00000000000000000000000000000003",
    "00000000000000000000000000000004",
)


async def _stub_reverse_provider_keys(_session, _tables, provider_set_ids):
    assert tuple(provider_set_ids) == _REVERSE_PROVIDER_SET_IDS
    return {_REVERSE_PROVIDER_SET_IDS[0]: 3, _REVERSE_PROVIDER_SET_IDS[1]: 4}


async def _stub_reverse_provider_codes(_session, snapshot_key, provider_set_keys, *, schema_name=None):
    assert snapshot_key == 41
    assert schema_name == "mrf"
    assert tuple(provider_set_keys) == (3, 4)
    return {3: (7, 8), 4: (7,)}


async def _stub_reverse_code_metadata(_session, _tables, **kwargs):
    assert kwargs["code_keys"] == (7, 8)
    return [
        {"code_key": 7, "plan_id": "plan", "reported_code_system": "CPT", "reported_code": "99213"},
        {"code_key": 8, "plan_id": "plan", "reported_code_system": "CPT", "reported_code": "99214"},
    ]


async def _stub_reverse_forward_entries(
    _session, code_keys, *, provider_set_keys=None, **_dictionary_hints
):
    assert _dictionary_hints["shared_snapshot_key"] == 41
    assert tuple(code_keys) == (7, 8)
    assert tuple(provider_set_keys) == (3, 4)
    return {
        7: (
            PTG2ServingBinaryRow(7, 3, 2, "00000000000000000000000000000011", 0, 10),
            PTG2ServingBinaryRow(7, 4, 7, "00000000000000000000000000000012", 0, 11),
            PTG2ServingBinaryRow(7, 4, 7, "00000000000000000000000000000012", 1, 11),
        ),
        8: (
            PTG2ServingBinaryRow(8, 3, 5, "00000000000000000000000000000013", 0, 12),
        ),
    }


def _configure_version_three_reverse(monkeypatch):
    monkeypatch.setattr(ptg2_serving, "_provider_set_keys_for_ids", _stub_reverse_provider_keys)
    monkeypatch.setattr(ptg2_serving, "lookup_shared_provider_code_keys_from_db", _stub_reverse_provider_codes)
    monkeypatch.setattr(ptg2_serving, "_manifest_reverse_code_rows", _stub_reverse_code_metadata)
    monkeypatch.setattr(ptg2_serving, "lookup_binary_code_batch_from_db", _stub_reverse_forward_entries)
    monkeypatch.setattr(
        ptg2_serving,
        "has_shared_provider_pages_in_db",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_has_single_plan_page_order",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "lookup_shared_provider_pages_from_db",
        AsyncMock(return_value=None),
    )
