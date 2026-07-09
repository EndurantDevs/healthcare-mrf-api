# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

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
