# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed edge coverage for shared PTG block contracts."""

from __future__ import annotations

import struct

import pytest

from process.ptg_parts import ptg2_shared_blocks as shared_blocks


_EMPTY_BINARY_COPY_STREAM = (
    shared_blocks._PG_BINARY_COPY_SIGNATURE + struct.pack(">IIh", 0, 0, -1)
)


def _mapping_record(reference: shared_blocks.SharedBlockReference) -> bytes:
    """Encode one mapping record for direct accumulator guard checks."""

    object_kind_bytes = reference.object_kind.encode("utf-8")
    return b"".join(
        (
            struct.pack(">I", len(object_kind_bytes)),
            object_kind_bytes,
            struct.pack(">q", reference.block_key),
            struct.pack(">i", reference.fragment_no),
            struct.pack(">q", reference.entry_count),
            reference.block_hash,
        )
    )


def test_shared_block_contract_fields_fail_closed():
    with pytest.raises(ValueError, match="format_version 2"):
        shared_blocks.shared_block_hash(
            format_version=1,
            object_kind="kind",
            codec="none",
            payload=b"",
        )
    with pytest.raises(ValueError, match="object_kind"):
        shared_blocks.SharedBlock("", 1, 0, 0, "none", 0, b"")
    with pytest.raises(ValueError, match="counts"):
        shared_blocks.SharedBlock("kind", 1, -1, 0, "none", 0, b"")
    with pytest.raises(ValueError, match="raw_byte_count"):
        shared_blocks.SharedBlock("kind", 1, 0, 0, "none", -1, b"")
    with pytest.raises(ValueError, match="codec"):
        shared_blocks.SharedBlock("kind", 1, 0, 0, "NONE", 0, b"")
    with pytest.raises(ValueError, match="size"):
        shared_blocks.SharedBlock("kind", 1, 0, 0, "none", 1, b"")


def test_mapping_copy_accumulator_rejects_invalid_lifecycle():
    active_accumulator = shared_blocks._SharedMappingBinaryCopyDigest()
    active_accumulator.begin_copy("kind")
    active_accumulator.feed(b"")
    with pytest.raises(RuntimeError, match="already active"):
        active_accumulator.begin_copy("kind")

    oversized_kind_accumulator = shared_blocks._SharedMappingBinaryCopyDigest()
    with pytest.raises(RuntimeError, match="object_kind exceeds"):
        oversized_kind_accumulator.begin_copy(
            "x" * (shared_blocks._MAPPING_COPY_MAX_KIND_BYTES + 1)
        )

    idle_accumulator = shared_blocks._SharedMappingBinaryCopyDigest()
    with pytest.raises(RuntimeError, match="while idle"):
        idle_accumulator.feed(b"x")
    with pytest.raises(RuntimeError, match="not active"):
        idle_accumulator.finish_copy()

    completed_accumulator = shared_blocks._SharedMappingBinaryCopyDigest()
    completed_accumulator.begin_copy("kind")
    completed_accumulator.feed(_EMPTY_BINARY_COPY_STREAM)
    with pytest.raises(RuntimeError, match="after its trailer"):
        completed_accumulator.feed(b"x")
    with pytest.raises(RuntimeError, match="contains no mappings"):
        completed_accumulator.finish_copy()

    unfinished_accumulator = shared_blocks._SharedMappingBinaryCopyDigest()
    unfinished_accumulator.begin_copy("kind")
    with pytest.raises(RuntimeError, match="still active"):
        unfinished_accumulator.finish()


def test_mapping_copy_accumulator_rejects_invalid_records():
    oversized_record_accumulator = shared_blocks._SharedMappingBinaryCopyDigest()
    oversized_record_accumulator.begin_copy("kind")
    with pytest.raises(RuntimeError, match="object_kind length"):
        oversized_record_accumulator._consume_mapping_record(
            struct.pack(">I", shared_blocks._MAPPING_COPY_MAX_KIND_BYTES + 1)
        )

    unexpected_reference = shared_blocks.SharedBlockReference(
        object_kind="other",
        block_key=1,
        fragment_no=0,
        entry_count=1,
        block_hash=b"a" * 32,
        raw_byte_count=1,
    )
    unexpected_record_accumulator = shared_blocks._SharedMappingBinaryCopyDigest()
    unexpected_record_accumulator.begin_copy("kind")
    with pytest.raises(RuntimeError, match="unexpected object_kind"):
        unexpected_record_accumulator._consume_mapping_record(
            _mapping_record(unexpected_reference)
        )

    with pytest.raises(RuntimeError, match="negative entry_count"):
        shared_blocks._non_negative_mapping_aggregate(-1, name="entry_count")


def test_shared_block_scalar_guards_fail_closed():
    assert shared_blocks._row_mapping((("key", 1),)) == {"key": 1}
    with pytest.raises(ValueError, match="32 bytes"):
        shared_blocks._advisory_lock_key(b"short")


@pytest.mark.asyncio
async def test_shared_block_runtime_short_circuits_invalid_inputs():
    empty_result = await shared_blocks.insert_shared_blocks(
        object(),
        schema_name="mrf",
        snapshot_key=1,
        blocks=(),
    )
    assert empty_result == shared_blocks.SharedBlockBatchResult((), 0, 0, 0)

    with pytest.raises(ValueError, match="support digest"):
        await shared_blocks.seal_shared_layout(
            object(),
            schema_name="mrf",
            snapshot_key=1,
            build_token="token",
            expected_summary=None,
            support_digest=b"short",
            layout_manifest={},
        )

    with pytest.raises(NotImplementedError, match="SQLAlchemy session"):
        await shared_blocks.summarize_shared_snapshot_mappings(
            object(),
            schema_name="mrf",
            snapshot_key=1,
        )
