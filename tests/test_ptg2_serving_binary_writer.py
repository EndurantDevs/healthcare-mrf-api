# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import zlib

import pytest

from process.ptg_parts import ptg2_serving_binary as serving_binary_writer


def test_natural_lean_stream_sql_joins_dictionary_tables():
    by_code_sql = serving_binary_writer._serving_binary_stream_sql(
        qualified_source='"mrf"."ptg2_manifest_stage_serving_token"',
        kind="by_code",
        source_layout=serving_binary_writer.PTG2_SERVING_BINARY_SOURCE_LAYOUT_NATURAL_LEAN,
        qualified_code_count_table='"mrf"."ptg2_code_count_token"',
        qualified_provider_set_dictionary_table='"mrf"."ptg2_provider_set_dict_token"',
    )
    reverse_sql = serving_binary_writer._serving_binary_stream_sql(
        qualified_source='"mrf"."ptg2_manifest_stage_serving_token"',
        kind="by_provider_set",
        source_layout=serving_binary_writer.PTG2_SERVING_BINARY_SOURCE_LAYOUT_NATURAL_LEAN,
        qualified_code_count_table='"mrf"."ptg2_code_count_token"',
        qualified_provider_set_dictionary_table='"mrf"."ptg2_provider_set_dict_token"',
    )

    assert 'FROM "mrf"."ptg2_manifest_stage_serving_token" serving' in by_code_sql
    assert 'JOIN "mrf"."ptg2_code_count_token" code_count' in by_code_sql
    assert 'JOIN "mrf"."ptg2_provider_set_dict_token" provider_set_dictionary' in by_code_sql
    assert "code_count.reported_code_system IS NOT DISTINCT FROM serving.reported_code_system" in by_code_sql
    assert "ORDER BY code_count.code_key, provider_set_dictionary.provider_set_key" in by_code_sql
    assert "ORDER BY provider_set_dictionary.provider_set_key, code_count.code_key" in reverse_sql


def test_natural_lean_stream_sql_requires_dictionary_tables():
    with pytest.raises(ValueError, match="dictionary tables"):
        serving_binary_writer._serving_binary_stream_sql(
            qualified_source='"mrf"."ptg2_manifest_stage_serving_token"',
            kind="by_code",
            source_layout=serving_binary_writer.PTG2_SERVING_BINARY_SOURCE_LAYOUT_NATURAL_LEAN,
        )


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
