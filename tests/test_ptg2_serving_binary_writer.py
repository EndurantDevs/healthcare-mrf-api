# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

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
async def test_price_set_atom_v2_writer_groups_sorted_ids_by_prefix(monkeypatch):
    copied_records = []

    async def fake_copy_records(*, schema_name, table_name, records):
        copied_records.extend(records)

    monkeypatch.setattr(serving_binary_writer, "_copy_serving_binary_records", fake_copy_records)
    writer = serving_binary_writer._PriceSetAtomPrefixBlockWriter(
        schema_name="mrf",
        table_name="ptg2_serving_binary_test",
        max_payload_bytes=1024 * 1024,
        batch_size=100,
    )
    first_atom_id = bytes.fromhex("000000000000000000000000000000c1")

    await writer.add_price_set(bytes.fromhex("000100000000000000000000000000a1"), (first_atom_id,))
    await writer.add_price_set(bytes.fromhex("000100000000000000000000000000a2"), ())
    await writer.add_price_set(bytes.fromhex("000200000000000000000000000000a3"), ())
    await writer.finish()

    v2_blocks = [
        (block_key, block_no, entry_count)
        for artifact_kind, block_key, block_no, entry_count, *_ in copied_records
        if artifact_kind == serving_binary_writer.PTG2_SERVING_BINARY_PRICE_SET_ATOMS_BY_ID_V2_KIND
    ]
    assert v2_blocks == [(1, 0, 2), (2, 0, 1)]
    assert writer.summary() == {
        "format": "price_set_atoms_by_id_v2",
        "artifact_kind": "price_set_atoms_by_id_v2",
        "id_prefix_bytes": 2,
        "id_bucket_count": 65536,
        "id_block_count": 2,
        "price_set_count": 3,
        "atom_ref_count": 1,
        "copied_records": 2,
    }
