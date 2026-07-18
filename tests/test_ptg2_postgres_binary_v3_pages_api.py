# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_db_serving_v3_pages, ptg2_db_sidecars, ptg2_serving
from api.ptg2_db_serving_v3_pages import PTG2V3PageRecord, PTG2V3ProviderPage
from api.ptg2_shared_blocks import SharedBlockPayload


PROVIDER_ID = "00000000000000000000000000000003"
PRICE_ID = "00000000000000000000000000000011"


def _uvarint(value):
    encoded = bytearray()
    remaining = int(value)
    while remaining >= 0x80:
        encoded.append((remaining & 0x7F) | 0x80)
        remaining >>= 7
    encoded.append(remaining)
    return bytes(encoded)


def _page_header(source_count, entry_count):
    source_bits = 0 if source_count == 1 else (source_count - 1).bit_length()
    return b"\x04" + _uvarint(source_count) + bytes([source_bits]) + _uvarint(entry_count)


def _source_vector(source_keys, source_count):
    source_bits = 0 if source_count == 1 else (source_count - 1).bit_length()
    encoded = bytearray((len(source_keys) * source_bits + 7) // 8)
    bit_offset = 0
    for source_key in source_keys:
        for source_bit in range(source_bits):
            if source_key & (1 << source_bit):
                encoded[bit_offset // 8] |= 1 << (bit_offset % 8)
            bit_offset += 1
    return bytes(encoded)


def test_page_decodes_aligned_source_keys():
    forward_payload = b"".join(
        [
            _page_header(2, 2),
            _uvarint(3),
            _uvarint(9),
            _uvarint(10),
            _uvarint(3),
            _uvarint(9),
            _uvarint(10),
            _source_vector([0, 1], 2),
        ]
    )
    forward = ptg2_db_serving_v3_pages._decode_code_page_block(
        forward_payload,
        code_key=7,
        entry_count=2,
        expected_source_count=2,
    )
    assert [
        (forward_row.price_key, forward_row.source_key)
        for forward_row in forward
    ] == [(10, 0), (10, 1)]

    provider_payload = b"".join(
        [
            _page_header(2, 1),
            _uvarint(0),
            _uvarint(9),
            _uvarint(2),
            _uvarint(2),
            _uvarint(7),
            _uvarint(10),
            _uvarint(0),
            _uvarint(10),
            _source_vector([0, 1], 2),
        ]
    )
    provider = ptg2_db_serving_v3_pages._decode_provider_page_block(
        provider_payload,
        block_key=3,
        entry_count=1,
        requested_provider_set_keys={3},
        expected_source_count=2,
    )
    assert [
        (provider_row.code_key, provider_row.price_key, provider_row.source_key)
        for provider_row in provider[3].entries
    ] == [
        (7, 10, 0),
        (7, 10, 1),
    ]

    selective = ptg2_db_serving_v3_pages._decode_provider_page_block(
        provider_payload,
        block_key=3,
        entry_count=1,
        requested_provider_set_keys={3},
        requested_code_keys={8},
        expected_source_count=2,
    )[3]
    assert selective.entries == ()
    assert selective.provider_count == 9
    assert selective.page_row_count == 2
    assert selective.last_code_key == 7


def test_page_rejects_bad_source_layout():
    payload = b"".join(
        [
            _page_header(2, 1),
            _uvarint(3),
            _uvarint(9),
            _uvarint(10),
            _source_vector([1], 2),
        ]
    )
    with pytest.raises(ptg2_serving.PTG2ManifestArtifactError, match="corrupt"):
        ptg2_db_serving_v3_pages._decode_code_page_block(
            payload,
            code_key=7,
            entry_count=1,
            expected_source_count=3,
        )
    with pytest.raises(ptg2_serving.PTG2ManifestArtifactError, match="corrupt"):
        ptg2_db_serving_v3_pages._decode_code_page_block(
            payload + b"\x00",
            code_key=7,
            entry_count=1,
            expected_source_count=2,
        )


@pytest.mark.asyncio
async def test_provider_page_bundle_fuses_metadata_and_selects_codes(monkeypatch):
    provider_payload = b"".join(
        [
            _page_header(2, 1),
            _uvarint(0),
            _uvarint(9),
            _uvarint(2),
            _uvarint(2),
            _uvarint(7),
            _uvarint(10),
            _uvarint(1),
            _uvarint(11),
            _source_vector([0, 1], 2),
        ]
    )
    session = AsyncMock()
    session.execute.return_value = [
        {
            "metadata_provider_set_key": 3,
            "provider_set_global_id_128": b"\x03" * 16,
            "metadata_provider_count": 9,
            "network_names": ["Network A"],
        }
    ]
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "validate_shared_block_row",
        lambda row, *, expected_kind: SharedBlockPayload(
            block_key=3,
            fragment_no=0,
            entry_count=1,
            payload=provider_payload,
        ),
    )

    bundle = await ptg2_db_sidecars.lookup_shared_provider_code_pages_from_db(
        session,
        41,
        (3,),
        (8,),
        source_count=2,
    )

    assert bundle is not None
    assert [entry.code_key for entry in bundle.pages_by_key[3].entries] == [8]
    assert bundle.provider_set_ids_by_key == {3: "03" * 16}
    assert bundle.network_names_by_key == {3: ("Network A",)}
    session.execute.assert_awaited_once()


class ScalarResult:
    def __init__(self, scalar_value):
        self.scalar_value = scalar_value

    def scalar(self):
        return self.scalar_value


class RecordingSession:
    def __init__(self, scalar_value):
        self.scalar_value = scalar_value
        self.calls = []

    async def execute(self, statement, params):
        self.calls.append((str(statement), dict(params)))
        return ScalarResult(self.scalar_value)


def serving_tables():
    return ptg2_serving.PTG2ServingTables(
        snapshot_id="logical-snapshot",
        arch_version="postgres_binary_v3",
        storage="manifest_snapshot",
        shared_snapshot_key=41,
        storage_generation="shared_blocks_v3",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout="dense_shared_blocks_v3",
        source_count=2,
        code_count=5,
        coverage_scope_id="c" * 64,
        plan_id="plan",
        plan_market_type="group",
        price_dictionary_item_count=1024,
        price_dictionary_block_bytes=65536,
    )


def reverse_query(limit=2):
    return ptg2_serving._VersionThreeReverseQuery(
        provider_set_ids=(PROVIDER_ID,),
        requested_plan="plan",
        code_value="",
        code_system=None,
        q_text="",
        code_context=None,
        source_trace_set_hash=None,
        network_names=[],
        limit=limit,
        offset=0,
        apply_window=True,
    )


@pytest.mark.asyncio
async def test_forward_page_materializes_without_full_code_lookup(monkeypatch):
    page_entries = (
        PTG2V3PageRecord(7, 3, 9, 10, 1),
    )
    monkeypatch.setattr(ptg2_serving, "lookup_shared_code_page_from_db", AsyncMock(return_value=page_entries))
    monkeypatch.setattr(
        ptg2_serving,
        "_provider_set_ids_for_keys",
        AsyncMock(return_value={3: PROVIDER_ID}),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "lookup_price_ids_from_db",
        AsyncMock(return_value={10: PRICE_ID}),
    )

    response_rows = await ptg2_serving._version_three_forward_page_rows(
        object(),
        serving_tables(),
        code_metadata={
            "code_key": 7,
            "plan_id": "plan",
            "reported_code_system": "CPT",
            "reported_code": "99213",
            "rate_count": 1,
        },
        source_trace_set_hash=None,
        network_names=[],
        limit=25,
        offset=0,
    )

    assert len(response_rows) == 1
    assert response_rows[0]["provider_set_global_id_128"] == PROVIDER_ID
    assert response_rows[0]["price_set_global_id_128"] == PRICE_ID
    assert response_rows[0]["price_key"] == 10


@pytest.mark.asyncio
async def test_forward_page_resolves_only_requested_window(monkeypatch):
    page_entries = tuple(
        PTG2V3PageRecord(
            code_key=7,
            provider_set_key=provider_set_key,
            provider_count=100 - provider_set_key,
            price_key=provider_set_key + 100,
            source_key=provider_set_key % 2,
        )
        for provider_set_key in range(1, 65)
    )
    provider_lookup = AsyncMock(return_value={1: PROVIDER_ID})
    price_lookup = AsyncMock(return_value={101: PRICE_ID})
    monkeypatch.setattr(
        ptg2_serving,
        "lookup_shared_code_page_from_db",
        AsyncMock(return_value=page_entries),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_provider_set_ids_for_keys",
        provider_lookup,
    )
    monkeypatch.setattr(ptg2_serving, "lookup_price_ids_from_db", price_lookup)

    response_rows = await ptg2_serving._version_three_forward_page_rows(
        object(),
        serving_tables(),
        code_metadata={"code_key": 7, "rate_count": 100},
        source_trace_set_hash=None,
        network_names=[],
        limit=1,
        offset=0,
    )

    assert len(response_rows) == 1
    assert provider_lookup.await_args.args[2] == {1}
    assert price_lookup.await_args.args[1] == {101}


@pytest.mark.asyncio
async def test_forward_page_rejects_underfilled_projection(monkeypatch):
    page_entries = tuple(
        PTG2V3PageRecord(7, provider_set_key, 1, provider_set_key, 0)
        for provider_set_key in range(1, 64)
    )
    monkeypatch.setattr(
        ptg2_serving,
        "lookup_shared_code_page_from_db",
        AsyncMock(return_value=page_entries),
    )

    with pytest.raises(ptg2_serving.PTG2ManifestArtifactError, match="invalid row count"):
        await ptg2_serving._version_three_forward_page_rows(
            object(),
            serving_tables(),
            code_metadata={"code_key": 7, "rate_count": 100},
            source_trace_set_hash=None,
            network_names=[],
            limit=1,
            offset=0,
        )


def install_reverse_page_stubs(monkeypatch, *, metadata_rows):
    page_record = PTG2V3PageRecord(
        code_key=7,
        provider_set_key=3,
        provider_count=9,
        price_key=10,
        source_key=1,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "has_shared_provider_pages_in_db",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_has_single_plan_page_order",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_provider_set_keys_for_ids",
        AsyncMock(return_value={PROVIDER_ID: 3}),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "lookup_shared_provider_pages_from_db",
        AsyncMock(return_value={3: PTG2V3ProviderPage((page_record,), 100)}),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_manifest_reverse_code_rows",
        AsyncMock(return_value=metadata_rows),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "lookup_price_ids_from_db",
        AsyncMock(return_value={10: PRICE_ID}),
    )


@pytest.mark.asyncio
async def test_reverse_page_avoids_full_forward_blocks(monkeypatch):
    install_reverse_page_stubs(
        monkeypatch,
        metadata_rows=[
            {
                "code_key": 7,
                "plan_id": "plan",
                "reported_code_system": "CPT",
                "reported_code": "99213",
            }
        ],
    )
    full_scope = AsyncMock(side_effect=AssertionError("full reverse scope should not run"))
    monkeypatch.setattr(ptg2_serving, "_version_three_reverse_scope", full_scope)

    rows = await ptg2_serving._version_three_reverse_rows(
        object(),
        serving_tables(),
        reverse_query(limit=1),
    )

    assert len(rows) == 1
    assert rows[0]["provider_set_global_id_128"] == PROVIDER_ID
    assert rows[0]["price_set_global_id_128"] == PRICE_ID
    full_scope.assert_not_awaited()


@pytest.mark.asyncio
async def test_reverse_page_reports_exact_projected_total(monkeypatch):
    install_reverse_page_stubs(
        monkeypatch,
        metadata_rows=[
            {
                "code_key": 7,
                "plan_id": "plan",
                "reported_code_system": "CPT",
                "reported_code": "99213",
            }
        ],
    )

    selection = await ptg2_serving._version_three_reverse_page_selection(
        object(),
        serving_tables(),
        reverse_query(limit=1),
    )

    assert selection is not None
    assert len(selection.rows) == 1
    assert selection.total_row_count == 100
    assert selection.is_total_exact
    assert not selection.exhausted


@pytest.mark.asyncio
async def test_reverse_page_rejects_projected_code_missing_from_metadata(monkeypatch):
    install_reverse_page_stubs(monkeypatch, metadata_rows=[])
    monkeypatch.setattr(
        ptg2_serving,
        "lookup_price_ids_from_db",
        AsyncMock(return_value={}),
    )

    with pytest.raises(ptg2_serving.PTG2ManifestArtifactError, match="unknown code"):
        await ptg2_serving._version_three_reverse_page_rows(
            object(),
            serving_tables(),
            reverse_query(limit=1),
        )


@pytest.mark.asyncio
async def test_reverse_page_falls_back_when_truncated_prefix_is_too_short(monkeypatch):
    install_reverse_page_stubs(
        monkeypatch,
        metadata_rows=[
            {
                "code_key": 7,
                "plan_id": "plan",
                "reported_code_system": "CPT",
                "reported_code": "99213",
            }
        ],
    )

    response_rows = await ptg2_serving._version_three_reverse_page_rows(
        object(),
        serving_tables(),
        reverse_query(limit=2),
    )

    assert response_rows is None


@pytest.mark.asyncio
async def test_reverse_page_falls_back_when_snapshot_contains_mixed_plan_order(monkeypatch):
    install_reverse_page_stubs(monkeypatch, metadata_rows=[])
    plan_order = AsyncMock(return_value=False)
    monkeypatch.setattr(
        ptg2_serving,
        "_has_single_plan_page_order",
        plan_order,
    )
    provider_lookup = AsyncMock(side_effect=AssertionError("mixed-plan page must not be read"))
    monkeypatch.setattr(
        ptg2_serving,
        "_provider_set_keys_for_ids",
        provider_lookup,
    )

    rows = await ptg2_serving._version_three_reverse_page_rows(
        object(),
        serving_tables(),
        reverse_query(limit=1),
    )

    assert rows is None
    plan_order.assert_awaited_once()
    provider_lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_unsupported_snapshot_contract_fails_before_page_queries(monkeypatch):
    availability = AsyncMock(side_effect=AssertionError("unsupported snapshot must fail closed"))
    plan_order = AsyncMock(side_effect=AssertionError("unsupported snapshot must skip plan aggregate"))
    provider_lookup = AsyncMock(side_effect=AssertionError("unsupported snapshot must skip provider lookup"))
    monkeypatch.setattr(ptg2_serving, "has_shared_provider_pages_in_db", availability)
    monkeypatch.setattr(ptg2_serving, "_has_single_plan_page_order", plan_order)
    monkeypatch.setattr(
        ptg2_serving,
        "_provider_set_keys_for_ids",
        provider_lookup,
    )

    unsupported_tables = replace(serving_tables(), arch_version="postgres_binary_v2")
    with pytest.raises(ptg2_serving.PTG2ManifestArtifactError, match="only postgres_binary_v3"):
        await ptg2_serving._version_three_reverse_page_rows(
            object(),
            unsupported_tables,
            reverse_query(limit=1),
        )

    availability.assert_not_awaited()
    plan_order.assert_not_awaited()
    provider_lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_plan_order_proof_uses_sealed_logical_scope_without_query():
    session = RecordingSession(True)

    assert await ptg2_serving._has_single_plan_page_order(
        session,
        serving_tables(),
        "plan",
        "group",
    )
    assert not await ptg2_serving._has_single_plan_page_order(
        session,
        serving_tables(),
        "other-plan",
        "group",
    )
    assert session.calls == []


@pytest.mark.asyncio
async def test_reverse_page_caps_provider_set_fanout(monkeypatch):
    availability = AsyncMock(return_value=True)
    plan_order = AsyncMock(side_effect=AssertionError("oversized page must skip plan aggregate"))
    provider_lookup = AsyncMock(side_effect=AssertionError("oversized page must skip provider lookup"))
    monkeypatch.setattr(ptg2_serving, "has_shared_provider_pages_in_db", availability)
    monkeypatch.setattr(ptg2_serving, "_has_single_plan_page_order", plan_order)
    monkeypatch.setattr(
        ptg2_serving,
        "_provider_set_keys_for_ids",
        provider_lookup,
    )
    oversized_query = replace(
        reverse_query(limit=1),
        provider_set_ids=tuple(f"{provider_index + 1:032x}" for provider_index in range(65)),
    )

    page_scope = await ptg2_serving._version_three_page_projection_scope(
        object(),
        serving_tables(),
        oversized_query,
    )

    assert page_scope is None
    availability.assert_awaited_once()
    plan_order.assert_not_awaited()
    provider_lookup.assert_not_awaited()
