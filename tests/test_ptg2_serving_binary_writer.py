# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import zlib

import pytest

from process.ptg_parts import ptg2_serving_binary as serving_binary_writer


@pytest.mark.asyncio
async def test_price_set_atom_writer_keeps_block_numbers_when_keys_reappear(monkeypatch):
    copied_records = []

    async def fake_copy_records(*, schema_name, table_name, records):
        copied_records.extend(records)

    monkeypatch.setattr(serving_binary_writer, "_copy_serving_binary_records", fake_copy_records)
    writer = serving_binary_writer._PriceSetAtomBlockWriter(
        schema_name="mrf",
        table_name="ptg2_serving_binary_test",
        max_payload_bytes=1024 * 1024,
        batch_size=100,
    )

    await writer.add_price_set(0, bytes.fromhex("000000000000000000000000000000a1"), ())
    await writer.add_price_set(
        serving_binary_writer._PRICE_SET_ATOM_BLOCK_SIZE,
        bytes.fromhex("000000000000000000000000000000a2"),
        (),
    )
    await writer.add_price_set(1, bytes.fromhex("000000000000000000000000000000a3"), ())
    await writer.finish()

    price_set_atom_blocks = [
        (block_key, block_no)
        for artifact_kind, block_key, block_no, *_ in copied_records
        if artifact_kind == serving_binary_writer.PTG2_SERVING_BINARY_PRICE_SET_ATOMS_KIND
    ]
    assert price_set_atom_blocks == [(0, 0), (1, 0), (0, 1)]


@pytest.mark.asyncio
async def test_price_key_map_query_reads_compression_columns_directly(monkeypatch):
    captured_query_map = {}
    first_price_set_id = bytes.fromhex("000000000000000000000000000000a1")
    second_price_set_id = bytes.fromhex("000000000000000000000000000000a2")
    raw_payload = first_price_set_id + second_price_set_id

    async def fake_first(sql, **params):
        captured_query_map["sql"] = sql
        captured_query_map["params"] = params
        return {
            "payload": zlib.compress(raw_payload),
            "payload_compression": "zlib",
            "raw_payload_bytes": len(raw_payload),
        }

    monkeypatch.setattr(serving_binary_writer.db, "first", fake_first)

    price_key_by_id = await serving_binary_writer._load_price_key_map(
        schema_name="mrf",
        table_name="ptg2_serving_binary_test",
    )

    assert price_key_by_id == {first_price_set_id: 0, second_price_set_id: 1}
    assert captured_query_map["params"] == {
        "artifact_kind": serving_binary_writer.PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND
    }
    assert "to_jsonb(binary_block)" not in captured_query_map["sql"]
    assert "COALESCE(payload_compression, 'none')" in captured_query_map["sql"]
    assert "raw_payload_bytes" in captured_query_map["sql"]
