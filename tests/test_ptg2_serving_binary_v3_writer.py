# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import zlib

import pytest

from process.ptg_parts import ptg2_serving_binary_v3_writer as v3_writer
from process.ptg_parts.ptg2_serving_binary_v3 import (
    decode_price_atoms,
    decode_price_memberships,
    read_uvarint,
)


async def _ordered_rows(*source_rows):
    for source_row in source_rows:
        yield source_row


def _raw_payload(copied_record):
    binary_payload = copied_record[4]
    return zlib.decompress(binary_payload) if copied_record[5] == "zlib" else binary_payload


def _logical_payload(copied_records):
    return b"".join(_raw_payload(copied_record) for copied_record in copied_records)


def _decode_provider_block(block_payload):
    provider_count, cursor = read_uvarint(block_payload, 0)
    code_keys_by_relative_key = {}
    for _provider_index in range(provider_count):
        relative_key, cursor = read_uvarint(block_payload, cursor)
        code_bytes, cursor = read_uvarint(block_payload, cursor)
        code_end = cursor + code_bytes
        code_keys_by_relative_key[relative_key] = block_payload[cursor:code_end]
        cursor = code_end
    assert cursor == len(block_payload)
    return code_keys_by_relative_key


@pytest.mark.asyncio
async def test_provider_set_blocks_use_canonical_kind_span_and_fragments(monkeypatch):
    copied_records = []

    async def capture_copy(**copy_call):
        copied_records.extend(copy_call["records"])

    monkeypatch.setattr(v3_writer, "_copy_serving_binary_records", capture_copy)
    summary = await v3_writer.write_provider_set_code_blocks(
        schema_name="mrf",
        table_name="v3_blocks",
        ordered_rows=_ordered_rows((1023, 1), (1024, 2)),
        max_payload_bytes=5,
        copy_batch_size=1,
    )

    assert {record[0] for record in copied_records} == {"provider_set_codes_v3"}
    assert {record[1] for record in copied_records} == {0, 1}
    assert all(len(_raw_payload(record)) <= 5 for record in copied_records)
    assert all(record[3] == 0 for record in copied_records if record[2] > 0)
    assert summary["block_span"] == 1024
    assert summary["provider_set_count"] == 2


@pytest.mark.asyncio
async def test_membership_blocks_use_shared_price_keys_and_canonical_span(monkeypatch):
    copied_records = []

    async def capture_copy(**copy_call):
        copied_records.extend(copy_call["records"])

    monkeypatch.setattr(v3_writer, "_copy_serving_binary_records", capture_copy)
    summary = await v3_writer.write_price_atom_membership_blocks(
        schema_name="mrf",
        table_name="v3_blocks",
        ordered_rows=_ordered_rows((511, 1), (512, 2)),
        atom_key_bits=24,
        max_payload_bytes=1024,
    )

    assert {record[0] for record in copied_records} == {"price_set_atom_memberships_v3"}
    assert {record[1] for record in copied_records} == {0, 1}
    assert summary["block_span"] == 512
    assert summary["atom_key_bits"] == 24


@pytest.mark.asyncio
async def test_membership_fragments_concatenate_before_decode(monkeypatch):
    copied_records = []

    async def capture_copy(**copy_call):
        copied_records.extend(copy_call["records"])

    monkeypatch.setattr(v3_writer, "_copy_serving_binary_records", capture_copy)
    await v3_writer.write_price_atom_membership_blocks(
        schema_name="mrf",
        table_name="v3_blocks",
        ordered_rows=_ordered_rows((0, 1), (0, 2)),
        atom_key_bits=24,
        max_payload_bytes=4,
    )

    assert [record[2] for record in copied_records] == list(range(len(copied_records)))
    assert copied_records[0][3] == 1
    assert all(record[3] == 0 for record in copied_records[1:])
    assert all(len(_raw_payload(record)) <= 4 for record in copied_records)
    assert decode_price_memberships(_logical_payload(copied_records)) == {0: (1, 2)}


@pytest.mark.asyncio
async def test_atom_blocks_are_dense_with_snapshot_key_width(monkeypatch):
    copied_records = []

    async def capture_copy(**copy_call):
        copied_records.extend(copy_call["records"])

    monkeypatch.setattr(v3_writer, "_copy_serving_binary_records", capture_copy)
    summary = await v3_writer.write_dense_atom_payload_blocks(
        schema_name="mrf",
        table_name="v3_blocks",
        ordered_rows=_ordered_rows(
            *((atom_key, "10.25", (0,)) for atom_key in range(513))
        ),
        atom_key_bits=24,
        max_payload_bytes=1024,
    )

    assert {record[0] for record in copied_records} == {"price_atoms_v3"}
    assert {record[1] for record in copied_records} == {0, 1}
    assert summary["block_span"] == 512
    assert summary["atom_key_bits"] == 24


@pytest.mark.asyncio
async def test_reference_manifest_uses_v2_price_dictionary_without_writing_it(monkeypatch):
    captured_serving_artifacts = []

    async def capture_copy(**copy_invocation):
        captured_serving_artifacts.extend(copy_invocation["records"])

    monkeypatch.setattr(v3_writer, "_copy_serving_binary_records", capture_copy)
    reference_manifest = await v3_writer.write_v3_reference_blocks(
        schema_name="mrf",
        table_name="v3_blocks",
        provider_code_rows=_ordered_rows((0, 7)),
        price_membership_rows=_ordered_rows((0, 0)),
        atom_payload_rows=_ordered_rows((0, "9.99", (1, None))),
        price_set_count=1,
        atom_count=1,
        max_payload_bytes=1024,
    )

    assert reference_manifest["format"] == "postgres_binary_v3"
    assert reference_manifest["writer"] == "python_reference_unwired"
    assert reference_manifest["atom_key_bits"] == 24
    assert reference_manifest["forward_price_dictionary"] == {
        "artifact_kind": "by_code_price_dictionary",
        "price_set_count": 1,
    }
    assert {
        artifact_name: artifact_summary["block_span"]
        for artifact_name, artifact_summary in reference_manifest["artifacts"].items()
    } == {
        "provider_set_codes": 1024,
            "price_set_atom_memberships": 512,
            "price_atoms": 512,
    }
    assert {serving_artifact[0] for serving_artifact in captured_serving_artifacts} == {
        "provider_set_codes_v3",
        "price_set_atom_memberships_v3",
        "price_atoms_v3",
    }
    assert reference_manifest["storage"]["record_count"] == len(captured_serving_artifacts)


@pytest.mark.asyncio
async def test_reference_manifest_rejects_membership_outside_v2_dictionary(monkeypatch):
    async def capture_copy(**_copy_call):
        return None

    monkeypatch.setattr(v3_writer, "_copy_serving_binary_records", capture_copy)
    with pytest.raises(RuntimeError, match="by_code_price_dictionary"):
        await v3_writer.write_v3_reference_blocks(
            schema_name="mrf",
            table_name="v3_blocks",
            provider_code_rows=_ordered_rows(),
            price_membership_rows=_ordered_rows((1, 0)),
            atom_payload_rows=_ordered_rows((0, "9.99", (1,))),
            price_set_count=1,
            atom_count=1,
            max_payload_bytes=1024,
        )
