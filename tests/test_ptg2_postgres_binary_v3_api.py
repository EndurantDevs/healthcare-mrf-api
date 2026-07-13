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
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_provider_set_ids_for_keys", provider_sets)

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
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_provider_set_ids_for_keys", missing_provider_set)

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
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_provider_set_keys_for_ids", _stub_reverse_provider_keys)
    monkeypatch.setattr(ptg2_serving, "lookup_shared_provider_code_keys_from_db", _stub_reverse_provider_codes)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_code_rows_for_provider_reverse", _stub_reverse_code_metadata)
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


@pytest.mark.asyncio
async def test_v3_reverse_batches_forward_rows_and_preserves_duplicate_pagination(monkeypatch):
    _configure_version_three_reverse(monkeypatch)
    reverse_rows = await ptg2_serving._version_three_reverse_rows(
        object(),
        _version_three_tables(),
        ptg2_serving._VersionThreeReverseQuery(
            provider_set_ids=_REVERSE_PROVIDER_SET_IDS,
            requested_plan="plan",
            code_value="",
            code_system=None,
            q_text="",
            code_context=None,
            source_trace_set_hash=None,
            network_names=[],
            limit=2,
            offset=1,
            apply_window=True,
        ),
    )

    assert [(candidate["provider_count"], candidate["price_key"]) for candidate in reverse_rows] == [(7, 11), (2, 10)]


@pytest.mark.asyncio
async def test_v3_reverse_raises_when_provider_code_membership_is_missing(monkeypatch):
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_provider_set_keys_for_ids",
        _stub_reverse_provider_keys,
    )

    async def incomplete_provider_codes(_session, _snapshot_key, _provider_set_keys, **_kwargs):
        return {3: (7,)}

    monkeypatch.setattr(
        ptg2_serving,
        "lookup_shared_provider_code_keys_from_db",
        incomplete_provider_codes,
    )

    with pytest.raises(ptg2_serving.PTG2ManifestArtifactError, match="provider-code artifact"):
        await ptg2_serving._version_three_reverse_scope(
            object(),
            _version_three_tables(),
            ptg2_serving._VersionThreeReverseQuery(
                provider_set_ids=_REVERSE_PROVIDER_SET_IDS,
                requested_plan="plan",
                code_value="",
                code_system=None,
                q_text="",
                code_context=None,
                source_trace_set_hash=None,
                network_names=[],
                limit=25,
                offset=0,
                apply_window=True,
            ),
        )


class VersionThreeBatchHarness:
    def __init__(self, candidate_code_count=4096):
        self.candidate_code_keys = tuple(range(candidate_code_count))
        self.metadata_calls = []
        self.forward_code_batches = []

    async def provider_keys(self, _session, _tables, _provider_set_ids):
        return {_REVERSE_PROVIDER_SET_IDS[0]: 3}

    async def provider_codes(self, _session, _snapshot_key, _provider_set_keys, **_kwargs):
        return {3: self.candidate_code_keys}

    async def code_metadata(self, _session, _tables, **query_kwargs):
        requested_code = str(query_kwargs.get("code_value") or "")
        candidate_code_keys = query_kwargs.get("code_keys") or self.candidate_code_keys
        matching_code_keys = [
            code_key
            for code_key in candidate_code_keys
            if not requested_code or f"{code_key:05d}" == requested_code
        ]
        metadata_offset = int(query_kwargs.get("offset_rows") or 0)
        metadata_limit = query_kwargs.get("limit_rows")
        batch_code_keys = matching_code_keys[metadata_offset:]
        if metadata_limit is not None:
            batch_code_keys = batch_code_keys[: int(metadata_limit)]
        self.metadata_calls.append((metadata_limit, metadata_offset, len(batch_code_keys)))
        return [
            {
                "code_key": code_key,
                "plan_id": "plan",
                "reported_code_system": "CPT",
                "reported_code": f"{code_key:05d}",
            }
            for code_key in batch_code_keys
        ]

    def entries_for_code(self, code_key):
        price_set_id = f"{code_key + 1:032x}"
        forward_entry = PTG2ServingBinaryRow(
            code_key=code_key,
            provider_set_key=3,
            provider_count=(code_key % 7) + 1,
            price_set_global_id_128=price_set_id,
            source_key=code_key % 2,
            price_key=code_key,
        )
        return (forward_entry, forward_entry) if code_key % 10 == 0 else (forward_entry,)

    async def forward_entries(
        self, _session, code_keys, *, provider_set_keys=None, **_dictionary_hints
    ):
        assert tuple(provider_set_keys) == (3,)
        batch_code_keys = tuple(code_keys)
        self.forward_code_batches.append(batch_code_keys)
        return {code_key: self.entries_for_code(code_key) for code_key in batch_code_keys}

    def expected_candidates(self):
        return [
            (f"{code_key:05d}", code_key)
            for code_key in self.candidate_code_keys
            for _duplicate_index in range(2 if code_key % 10 == 0 else 1)
        ]

    def metadata_calls_for_candidate_count(self, candidate_count):
        seen_candidates = 0
        batch_start = 0
        batch_size = ptg2_serving._PTG2_VERSION_THREE_REVERSE_INITIAL_BATCH_SIZE
        expected_calls = []
        while batch_start < len(self.candidate_code_keys):
            batch_code_keys = self.candidate_code_keys[batch_start : batch_start + batch_size]
            expected_calls.append((batch_size, batch_start, len(batch_code_keys)))
            for code_key in batch_code_keys:
                seen_candidates += 2 if code_key % 10 == 0 else 1
            if seen_candidates >= candidate_count:
                break
            batch_start += len(batch_code_keys)
            batch_size = min(batch_size * 2, ptg2_serving._PTG2_VERSION_THREE_REVERSE_CODE_BATCH_SIZE)
        return expected_calls

    def install(self, monkeypatch):
        monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_provider_set_keys_for_ids", self.provider_keys)
        monkeypatch.setattr(ptg2_serving, "lookup_shared_provider_code_keys_from_db", self.provider_codes)
        monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_code_rows_for_provider_reverse", self.code_metadata)
        monkeypatch.setattr(ptg2_serving, "lookup_binary_code_batch_from_db", self.forward_entries)
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


def _batched_reverse_query(*, limit, offset=0, apply_window=True, code_value=""):
    return ptg2_serving._VersionThreeReverseQuery(
        provider_set_ids=(_REVERSE_PROVIDER_SET_IDS[0],),
        requested_plan="plan",
        code_value=code_value,
        code_system="CPT" if code_value else None,
        q_text="",
        code_context=None,
        source_trace_set_hash=None,
        network_names=[],
        limit=limit,
        offset=offset,
        apply_window=apply_window,
    )


def _candidate_identity(candidate):
    return candidate["reported_code"], candidate["price_key"]


@pytest.mark.asyncio
async def test_v3_shallow_page_reads_one_batch_from_thousands(monkeypatch):
    harness = VersionThreeBatchHarness()
    harness.install(monkeypatch)

    reverse_rows = await ptg2_serving._version_three_reverse_rows(
        object(),
        _version_three_tables(),
        _batched_reverse_query(limit=25),
    )

    assert [_candidate_identity(candidate) for candidate in reverse_rows] == harness.expected_candidates()[:25]
    expected_calls = harness.metadata_calls_for_candidate_count(25)
    assert harness.metadata_calls == expected_calls
    assert len(harness.forward_code_batches) == len(expected_calls)


@pytest.mark.asyncio
async def test_v3_deep_offset_matches_global_candidate_order(monkeypatch):
    harness = VersionThreeBatchHarness()
    harness.install(monkeypatch)
    offset = 1500
    limit = 25

    reverse_rows = await ptg2_serving._version_three_reverse_rows(
        object(),
        _version_three_tables(),
        _batched_reverse_query(limit=limit, offset=offset),
    )

    expected_candidates = harness.expected_candidates()[offset : offset + limit]
    assert [_candidate_identity(candidate) for candidate in reverse_rows] == expected_candidates
    expected_calls = harness.metadata_calls_for_candidate_count(offset + limit)
    assert harness.metadata_calls == expected_calls
    assert len(harness.forward_code_batches) == len(expected_calls)


@pytest.mark.asyncio
async def test_v3_price_filter_candidate_cap_matches_eager_prefix(monkeypatch):
    harness = VersionThreeBatchHarness()
    harness.install(monkeypatch)
    candidate_limit = 500

    reverse_rows = await ptg2_serving._version_three_reverse_rows(
        object(),
        _version_three_tables(),
        _batched_reverse_query(limit=candidate_limit, apply_window=False),
    )

    assert [_candidate_identity(candidate) for candidate in reverse_rows] == harness.expected_candidates()[:candidate_limit]
    expected_calls = harness.metadata_calls_for_candidate_count(candidate_limit)
    assert harness.metadata_calls == expected_calls
    assert len(harness.forward_code_batches) == len(expected_calls)


@pytest.mark.asyncio
async def test_v3_exact_code_uses_one_unbounded_batch(monkeypatch):
    harness = VersionThreeBatchHarness()
    harness.install(monkeypatch)

    reverse_rows = await ptg2_serving._version_three_reverse_rows(
        object(),
        _version_three_tables(),
        _batched_reverse_query(limit=25, code_value="02000"),
    )

    assert [_candidate_identity(candidate) for candidate in reverse_rows] == [("02000", 2000), ("02000", 2000)]
    assert harness.metadata_calls == [(None, 0, 1)]
    assert harness.forward_code_batches == [(2000,)]


@pytest.mark.asyncio
async def test_v3_exact_code_skips_reverse_membership_expansion(monkeypatch):
    harness = VersionThreeBatchHarness()
    harness.install(monkeypatch)
    reverse_memberships = AsyncMock(
        side_effect=AssertionError("exact code must not expand provider reverse memberships")
    )
    monkeypatch.setattr(
        ptg2_serving,
        "lookup_shared_provider_code_keys_from_db",
        reverse_memberships,
    )

    reverse_rows = await ptg2_serving._version_three_reverse_rows(
        object(),
        _version_three_tables(),
        _batched_reverse_query(limit=25, code_value="02000"),
    )

    assert [_candidate_identity(candidate) for candidate in reverse_rows] == [
        ("02000", 2000),
        ("02000", 2000),
    ]
    reverse_memberships.assert_not_awaited()
