# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Parity and regression tests for Rust columnar direct-stage encoding and PG binary COPY."""

from __future__ import annotations

import json
import struct
import time
from dataclasses import replace
from decimal import Decimal

import msgpack
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from process.custom_import import family_block_direct_stage as direct_stage
from process.custom_import.capture import CaptureError, capture_stream
from process.custom_import.capture_store import CaptureReceipt, SegmentedParquetPart
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_raw_key import RawFamilyKeyError
from process.custom_import.runner_codec import pack_hash
from process.custom_import.runner_types import CandidateRunnerError
from process.custom_import.segmented_capture_policy import SegmentedCapturePolicy
from tests.test_custom_import_build_source_bulk import _bulk_context
from tests.test_custom_import_snowflake_capture import _policy as _capture_policy

_PG_COPY_HEADER = b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0"
_PG_COPY_TRAILER = b"\xff\xff"
_DIRECT_COLUMN_KINDS = (
    "i16",
    "i16",
    "i16",
    "i64",
    "i64",
    "bytes",
    "str",
    "bytes",
    "str",
    "bytes",
    "str",
    "str",
    "str",
    "bytes",
    "str",
    "bytes",
    "bytes",
)


def _decode_pg_binary_field(col_kind: str, raw_field: bytes) -> object:
    """Decode one non-null PostgreSQL binary COPY field value."""
    if col_kind == "i16":
        return struct.unpack("!h", raw_field)[0]
    if col_kind == "i64":
        return struct.unpack("!q", raw_field)[0]
    if col_kind == "str":
        return raw_field.decode("utf-8")
    return bytes(raw_field)


def _decode_pg_binary_direct_rows(copy_bytes: bytes) -> list[tuple[object, ...]]:
    """Decode PostgreSQL binary COPY output for DIRECT_BUCKET_COLUMNS into Python tuples."""
    assert copy_bytes.startswith(_PG_COPY_HEADER)
    assert copy_bytes.endswith(_PG_COPY_TRAILER)
    offset = len(_PG_COPY_HEADER)
    end = len(copy_bytes) - len(_PG_COPY_TRAILER)
    decoded_rows: list[tuple[object, ...]] = []
    while offset < end:
        (col_count,) = struct.unpack_from("!h", copy_bytes, offset)
        offset += 2
        assert col_count == len(_DIRECT_COLUMN_KINDS)
        row_values: list[object] = []
        for col_kind in _DIRECT_COLUMN_KINDS:
            (field_len,) = struct.unpack_from("!i", copy_bytes, offset)
            offset += 4
            if field_len == -1:
                row_values.append(None)
                continue
            row_values.append(_decode_pg_binary_field(col_kind, copy_bytes[offset : offset + field_len]))
            offset += field_len
        decoded_rows.append(tuple(row_values))
    assert offset == end
    return decoded_rows


def _parquet_part(
    context,
    table: pa.Table,
    *,
    ordinal: int = 1,
    maximum_records: int = 100,
) -> tuple[SegmentedParquetPart, SegmentedCapturePolicy]:
    """Serialize an Arrow table into a verified SegmentedParquetPart for direct-stage testing."""
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd")
    payload_bytes = sink.getvalue().to_pybytes()
    base_policy = _capture_policy()
    policy = replace(
        base_policy,
        part_limits=replace(base_policy.part_limits, maximum_records=maximum_records),
    )
    sealed = capture_stream(
        pa.BufferReader(payload_bytes),
        context.stream,
        source_snapshot_token="snap-direct-rust-1",
        limits=policy.part_limits,
    )
    receipt = CaptureReceipt(
        stream_id=context.stream.stream_id,
        source_snapshot_token="snap-direct-rust-1",
        byte_count=len(payload_bytes),
        content_sha256="a" * 64,
        canonical_manifest='{"part_count":1}',
        manifest_sha256="b" * 64,
    )
    part = SegmentedParquetPart(
        receipt=receipt,
        ordinal=ordinal,
        capture=sealed,
        record_count=table.num_rows,
        arrow_byte_count=table.nbytes,
    )
    return part, policy


def _decoded_slice_by_bucket(
    part_slice: direct_stage.PreparedDirectPartSlice,
    pack_id: int,
) -> dict[int, list[tuple[object, ...]]]:
    """Materialize either Rust binary COPY chunks or Python tuples into comparable bucket rows."""
    finalize_fn = direct_stage.native_finalize_direct_stage_copy()
    rows_by_bucket: dict[int, list[tuple[object, ...]]] = {}
    for bucket_index in sorted(set(part_slice.binary_chunks_by_bucket) | set(part_slice.rows_by_bucket)):
        bucket_rows: list[tuple[object, ...]] = []
        binary_chunks = part_slice.binary_chunks_by_bucket.get(bucket_index)
        if binary_chunks:
            assert finalize_fn is not None
            copy_bytes = finalize_fn([(entry[0], entry[1]) for entry in binary_chunks], pack_id)
            bucket_rows.extend(_decode_pg_binary_direct_rows(copy_bytes))
        python_rows = part_slice.rows_by_bucket.get(bucket_index)
        if python_rows:
            bucket_rows.extend((*row[:3], pack_id, *row[4:]) for row in python_rows)
        rows_by_bucket[bucket_index] = bucket_rows
    return rows_by_bucket


def _require_native() -> None:
    if direct_stage.native_direct_stage_plan_factory() is None:
        pytest.skip("ptg2_address_canon direct-stage native capability is unavailable")


def _default_budget(
    *, start_row: int = 0, source_ordinal: int = 100, max_rows: int = 1000
) -> direct_stage.DirectSliceBudget:
    return direct_stage.DirectSliceBudget(
        part_row_ordinal=start_row,
        source_ordinal=source_ordinal,
        bucket_count=16,
        max_rows=max_rows,
        max_bytes=16 * 1024 * 1024,
        deadline=time.monotonic() + 60.0,
        soft_deadline=time.monotonic() + 30.0,
        is_final_part=True,
    )


def _msgpack_boundary_context():
    """Extend the synthetic root fixture with signed integer and optional text fields."""
    base_context = _bulk_context(0)
    definition_by_key = json.loads(base_context.request.definition.canonical)
    definition_by_key["schema"]["root"]["fields"].extend(
        [
            {"id": "signed_int", "slot": 10, "type": "integer", "nullable": True, "projection_slot": 3},
            {"id": "note", "slot": 11, "type": "string", "nullable": True},
        ]
    )
    definition_by_key["aliases"]["root_source"]["SIGNED_INT"] = "signed_int"
    definition_by_key["aliases"]["root_source"]["NOTE"] = "note"
    definition = CustomImportDefinition.from_mapping(definition_by_key)
    return replace(
        base_context,
        request=replace(base_context.request, definition=definition),
        stream=definition.source_streams[0],
    )


def _msgpack_boundary_table() -> pa.Table:
    """Build an Arrow table exercising every MessagePack integer and string length boundary."""
    boundary_ints = [
        -(1 << 63),
        -(1 << 31) - 1,
        -(1 << 31),
        -32_769,
        -32_768,
        -129,
        -128,
        -33,
        -32,
        -1,
        0,
        127,
        128,
        255,
        256,
        65_535,
        65_536,
        (1 << 32) - 1,
        1 << 32,
        (1 << 63) - 1,
        None,
    ]
    row_count = len(boundary_ints)
    notes = [
        ""
        if idx == 0
        else ("x" * 31 if idx == 1 else ("y" * 32 if idx == 2 else ("z" * 260 if idx == 3 else f"n{idx}")))
        for idx in range(row_count)
    ]
    return pa.table(
        {
            "npi": pa.array(["1992999874"] * row_count, type=pa.string()),
            "score": pa.array([Decimal(f"{idx}.050") for idx in range(row_count)], type=pa.decimal128(30, 12)),
            "enabled": pa.array([idx % 2 == 0 for idx in range(row_count)], type=pa.bool_()),
            "signed_int": pa.array(boundary_ints, type=pa.int64()),
            "note": pa.array(notes, type=pa.string()),
        }
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("stream_index", [0, 1])
async def test_rust_direct_stage_matches_python_oracle_byte_for_byte(stream_index: int) -> None:
    """Rust columnar direct-stage PG binary COPY matches Python oracle across accepted and rejected rows."""
    _require_native()
    context = _bulk_context(stream_index)
    if stream_index == 0:
        table = pa.table(
            {
                "npi": pa.array(
                    ["1992999874", "1992999876", None, "1234567893", "1003000126"],
                    type=pa.string(),
                ),
                "score": pa.array(
                    [Decimal("12.3400"), Decimal("1.00"), Decimal("5.0"), None, Decimal("-0.00")],
                    type=pa.decimal128(38, 12),
                ),
                "enabled": pa.array([True, False, True, True, False], type=pa.bool_()),
            }
        )
    else:
        table = pa.table(
            {
                "detail_npi": pa.array(["1992999874", None, "1234567893", "1003000126"], type=pa.large_string()),
                "detail_id": pa.array(["c1", "c2", None, 'c4\n"\\Ω'], type=pa.string()),
                "amount": pa.array(
                    [Decimal("9.50"), Decimal("1.0"), Decimal("2.0"), Decimal("1000000000000000000.0")],
                    type=pa.decimal128(38, 12),
                ),
            }
        )
    part, policy = _parquet_part(context, table)
    budget = _default_budget()
    direct_plan = direct_stage.compile_direct_plan(context)
    assert direct_plan is not None
    rust_slice = await direct_stage.prepare_direct_part_slice(context, part, policy, budget, direct_plan=direct_plan)
    python_slice = await direct_stage.prepare_direct_part_slice(context, part, policy, budget, direct_plan=None)

    assert rust_slice.binary_chunks_by_bucket and not rust_slice.rows_by_bucket
    assert python_slice.rows_by_bucket and not python_slice.binary_chunks_by_bucket
    assert rust_slice.row_count == python_slice.row_count == table.num_rows
    assert rust_slice.byte_count == python_slice.byte_count
    assert rust_slice.has_reached_eof is True
    assert python_slice.has_reached_eof is True

    pack_id = 42_000_001
    assert _decoded_slice_by_bucket(rust_slice, pack_id) == _decoded_slice_by_bucket(python_slice, pack_id)

    label = "root" if context.stream.record_kind == "root" else context.stream.child_collection
    rust_batch = direct_stage.merge_direct_slices([rust_slice])
    python_batch = direct_stage.merge_direct_slices([python_slice])
    assert direct_stage._compute_direct_pack_digest(label, rust_batch) == direct_stage._compute_direct_pack_digest(
        label, python_batch
    )


@pytest.mark.asyncio
async def test_rust_direct_stage_msgpack_and_budget_truncation_parity() -> None:
    """Every integer/string/boolean/null msgpack boundary and mid-batch budget cut matches Python."""
    _require_native()
    context = _msgpack_boundary_context()
    part, policy = _parquet_part(context, _msgpack_boundary_table())
    direct_plan = direct_stage.compile_direct_plan(context)
    assert direct_plan is not None

    for start_row, max_rows in ((0, 100), (5, 7), (12, 100)):
        budget = _default_budget(start_row=start_row, source_ordinal=500 + start_row, max_rows=max_rows)
        rust_slice = await direct_stage.prepare_direct_part_slice(
            context, part, policy, budget, direct_plan=direct_plan
        )
        python_slice = await direct_stage.prepare_direct_part_slice(context, part, policy, budget, direct_plan=None)
        assert rust_slice.row_count == python_slice.row_count
        assert rust_slice.byte_count == python_slice.byte_count
        assert rust_slice.has_reached_eof == python_slice.has_reached_eof
        rust_rows_by_bucket = _decoded_slice_by_bucket(rust_slice, 77)
        assert rust_rows_by_bucket == _decoded_slice_by_bucket(python_slice, 77)
        for bucket_rows in rust_rows_by_bucket.values():
            for row_tuple in bucket_rows:
                assert row_tuple[15] is not None
                unpacked = msgpack.unpackb(row_tuple[15], raw=False)
                assert isinstance(unpacked, list) and len(unpacked) == 5


@pytest.mark.asyncio
async def test_rust_direct_stage_enforces_arrow_byte_limit_in_single_pass() -> None:
    """Single-pass Parquet direct staging rejects parts exceeding maximum_part_arrow_bytes."""
    _require_native()
    context = _bulk_context(0)
    table = pa.table(
        {
            "npi": pa.array(["1992999874"] * 10, type=pa.string()),
            "score": pa.array([Decimal("1.25")] * 10, type=pa.decimal128(38, 12)),
            "enabled": pa.array([True] * 10, type=pa.bool_()),
        }
    )
    part, policy = _parquet_part(context, table)
    tight_policy = replace(policy, maximum_part_arrow_bytes=16)
    with pytest.raises(CandidateRunnerError, match="decoded part Arrow bytes exceed"):
        await direct_stage.prepare_direct_part_slice(context, part, tight_policy, _default_budget())


@pytest.mark.asyncio
async def test_rust_direct_stage_enforces_page_byte_and_record_limits() -> None:
    """Rust direct stage raises matching exceptions on page_byte_limit and maximum_record_bytes."""
    _require_native()
    context = _bulk_context(0)
    table = pa.table(
        {
            "npi": pa.array(["1992999874"], type=pa.string()),
            "score": pa.array([Decimal("1.25")], type=pa.decimal128(38, 12)),
            "enabled": pa.array([True], type=pa.bool_()),
        }
    )
    part, policy = _parquet_part(context, table)
    budget = _default_budget(source_ordinal=0)
    tight_record_policy = replace(policy, part_limits=replace(policy.part_limits, maximum_record_bytes=8))
    with pytest.raises(CaptureError, match="exceeds the byte limit"):
        await direct_stage.prepare_direct_part_slice(context, part, tight_record_policy, budget)

    tight_raw_context = replace(context, request=replace(context.request, page_byte_limit=16))
    with pytest.raises(RawFamilyKeyError, match="raw family key exceeds"):
        await direct_stage.prepare_direct_part_slice(tight_raw_context, part, policy, budget)

    tight_occ_context = replace(context, request=replace(context.request, page_byte_limit=120))
    with pytest.raises(CandidateRunnerError, match="one source occurrence exceeds"):
        await direct_stage.prepare_direct_part_slice(tight_occ_context, part, policy, budget)


def test_native_pack_hash_matches_python_pack_hash() -> None:
    """Native pack hash matches runner_codec.pack_hash across multi-chunk inputs."""
    _require_native()
    native_fn = direct_stage.native_pack_hash()
    assert native_fn is not None
    hashes = [bytes([idx]) * 32 for idx in (19, 3, 255, 0, 42, 3)]
    chunked = (b"".join(hashes[:2]), b"".join(hashes[2:]))
    assert native_fn("root", chunked) == pack_hash("root", hashes)
    assert native_fn("episodes", chunked) == pack_hash("episodes", hashes)
    assert native_fn("root", ()) == pack_hash("root", ())


def _sample_v2_family_fixture():
    from process.custom_import import runner_codec
    from process.custom_import.family_block_records import FamilyRecord
    from process.custom_import.family_block_v2_codec import pack_staged_values
    from tests.test_custom_import_snowflake_shared_capture import _shared_definition

    definition = _shared_definition()
    fields_by_col = runner_codec.fields_by_collection(definition)
    root_by_field = {"npi": "1003000126", "score": Decimal("42.5"), "enabled": True}
    child_values_list = [
        {"detail_npi": "1003000126", "detail_id": 'k1\n\t"\\\x01Ω', "amount": Decimal("12.34")},
        {"detail_npi": "1003000126", "detail_id": "k2", "amount": Decimal("0")},
    ]
    child_items = []
    for idx, child_values in enumerate(child_values_list, 1):
        child_key = runner_codec.child_key_document(definition, "details", child_values)
        child_hash = runner_codec.digest_text("child-key", child_key)
        child_items.append((child_hash, child_key, idx, child_values))
    child_items.sort(key=lambda entry: (entry[0], entry[1]))
    root_record = FamilyRecord(101, 7, 0, root_by_field, {})
    child_records = [FamilyRecord(200 + idx, 8, idx, vals, {}) for _h, _k, idx, vals in child_items]
    root_key = runner_codec.root_key_document(definition, root_by_field)
    root_payload_sha = runner_codec.digest_text(
        "root-payload", runner_codec.record_payload(definition.root_fields, root_by_field)
    )
    root_packed = pack_staged_values(definition.root_fields, root_by_field)
    rust_child_rows = [
        (200 + idx, 8, idx, pack_staged_values(fields_by_col["details"], vals), chash, ckey)
        for chash, ckey, idx, vals in child_items
    ]
    return definition, root_record, child_records, root_packed, root_key, root_payload_sha, rust_child_rows


def test_native_v2_family_encoder_matches_python_codec_byte_for_byte() -> None:
    """Rust compact v2 family hashing and framing matches encode_family_v2 across boundaries."""
    _require_native()
    from process.custom_import.family_block_codec import FamilyBlockLimits
    from process.custom_import.family_block_v2_codec import (
        compile_v2_family_plan,
        encode_family_v2,
        finalize_native_v2_family,
        verify_v2_fragments_and_records,
    )

    definition, root_record, child_records, root_packed, root_key, root_payload_sha, rust_child_rows = (
        _sample_v2_family_fixture()
    )
    rust_family = (101, 7, 0, root_packed, root_key, root_payload_sha, (rust_child_rows,))

    for fragment_bytes in (65_536, 128):
        limits = FamilyBlockLimits(max_spool_bytes=1_048_576, max_record_bytes=65_536, fragment_bytes=fragment_bytes)
        with encode_family_v2(
            definition,
            root_record,
            {"details": child_records},
            None,
            limits=limits,
            retained=False,
            total_child_count=2,
        ) as py_enc:
            plan = compile_v2_family_plan(definition, limits)
            assert plan is not None
            native_out = plan.encode_batch([rust_family])[0]
            nat_enc = finalize_native_v2_family(definition, root_payload_sha, 2, native_out, limits=limits)
            assert nat_enc is not None
            assert nat_enc.stored_manifest == py_enc.stored_manifest
            assert nat_enc.root_payload_sha256 == py_enc.root_payload_sha256
            assert nat_enc.root_scalar_count == py_enc.root_scalar_count
            assert nat_enc.child_scalar_count == py_enc.child_scalar_count
            assert list(nat_enc.indexed_fragments()) == list(py_enc.indexed_fragments())
            verified = verify_v2_fragments_and_records(
                definition, nat_enc.indexed_fragments(), nat_enc.stored_manifest, limits=limits
            )
            verified.close()

    plan = compile_v2_family_plan(definition, FamilyBlockLimits(max_spool_bytes=1_048_576, max_record_bytes=65_536))
    assert plan is not None
    with pytest.raises(ValueError, match="root payload hash differs"):
        plan.encode_batch([(101, 7, 0, root_packed, root_key, b"\x00" * 32, (rust_child_rows,))])
    with pytest.raises(ValueError, match="not ordered"):
        plan.encode_batch([(101, 7, 0, root_packed, root_key, root_payload_sha, (list(reversed(rust_child_rows)),))])


def test_native_v2_family_encoder_inline_root_parity() -> None:
    """Rust inline zero-child root encoding matches Python inline_storage_sha256 exactly."""
    _require_native()
    from process.custom_import import runner_codec
    from process.custom_import.family_block_codec import FamilyBlockLimits
    from process.custom_import.family_block_records import FamilyRecord
    from process.custom_import.family_block_v2_codec import (
        compile_v2_family_plan,
        encode_family_v2,
        finalize_native_v2_family,
        pack_staged_values,
    )
    from tests.test_custom_import_compact_v2_direct_postgres import _inline_root_definition
    from tests.test_custom_import_snowflake_shared_capture import _shared_definition

    definition = _inline_root_definition(_shared_definition())
    root_by_field = {"npi": "1003000126", "score": Decimal("1.5"), "enabled": False}
    root_record = FamilyRecord(301, 5, 0, root_by_field, {})
    root_key = runner_codec.root_key_document(definition, root_by_field)
    root_payload_sha = runner_codec.digest_text(
        "root-payload", runner_codec.record_payload(definition.root_fields, root_by_field)
    )
    root_packed = pack_staged_values(definition.root_fields, root_by_field)
    limits = FamilyBlockLimits(max_spool_bytes=1_048_576, max_record_bytes=65_536)

    with encode_family_v2(
        definition, root_record, {}, None, limits=limits, retained=False, total_child_count=0
    ) as py_enc:
        plan = compile_v2_family_plan(definition, limits)
        assert plan is not None
        native_out = plan.encode_batch([(301, 5, 0, root_packed, root_key, root_payload_sha, ())])[0]
        assert native_out[0] == "inline"
        nat_enc = finalize_native_v2_family(definition, root_payload_sha, 0, native_out, limits=limits)
        assert nat_enc is not None
        assert nat_enc.stored_manifest == py_enc.stored_manifest
        assert nat_enc.root_scalar_count == py_enc.root_scalar_count
        assert list(nat_enc.indexed_fragments()) == []


@pytest.mark.asyncio
async def test_direct_stage_nonmonotonic_field_slots_match_declared_order() -> None:
    """Direct staging encodes packed_slots and payload_sha256 in declared order when field slots are non-monotonic."""
    _require_native()
    from process.custom_import import runner_codec
    from process.custom_import.family_block_v2_codec import (
        extract_staged_membership_key,
        unpack_staged_values_and_scalars,
    )

    base_context = _bulk_context(0)
    definition_by_key = json.loads(base_context.request.definition.canonical)
    # Place a higher-numbered string slot BEFORE a lower-numbered numeric slot in declared order.
    definition_by_key["schema"]["root"]["fields"].insert(
        1, {"id": "panel", "slot": 40, "type": "string", "nullable": False, "projection_slot": 3}
    )
    definition_by_key["aliases"]["root_source"]["PANEL"] = "panel"
    definition = CustomImportDefinition.from_mapping(definition_by_key)
    stream_context = replace(
        base_context,
        request=replace(base_context.request, definition=definition),
        stream=definition.source_streams[0],
    )
    arrow_table = pa.table(
        {
            "npi": pa.array(["1003000126", "1234567893"], type=pa.string()),
            "score": pa.array([Decimal("42.5"), Decimal("0.0")], type=pa.decimal128(10, 2)),
            "enabled": pa.array([True, False], type=pa.bool_()),
            "panel": pa.array(["alpha", "beta"], type=pa.string()),
        }
    )
    part, policy = _parquet_part(stream_context, arrow_table)
    budget = _default_budget(max_rows=10)
    rust_slice = await direct_stage.prepare_direct_part_slice(stream_context, part, policy, budget)
    python_slice = await direct_stage.prepare_direct_part_slice(stream_context, part, policy, budget, direct_plan=None)

    rust_buckets = _decoded_slice_by_bucket(rust_slice, pack_id=77)
    python_buckets = _decoded_slice_by_bucket(python_slice, pack_id=77)
    assert rust_buckets == python_buckets
    staged_rows = [staged_entry for bucket_rows in rust_buckets.values() for staged_entry in bucket_rows]
    assert len(staged_rows) == 2
    expected_by_npi = {
        "1003000126": {"npi": "1003000126", "panel": "alpha", "score": Decimal("42.5"), "enabled": True},
        "1234567893": {"npi": "1234567893", "panel": "beta", "score": Decimal("0"), "enabled": False},
    }
    for staged_row in staged_rows:
        packed_slots = bytes(staged_row[15])
        payload_hash = bytes(staged_row[16])
        decoded_by_field, _scalars = unpack_staged_values_and_scalars(definition.root_fields, packed_slots)
        expected_fields = expected_by_npi[decoded_by_field["npi"]]
        assert decoded_by_field == expected_fields
        expected_hash = runner_codec.digest_text(
            "root-payload", runner_codec.record_payload(definition.root_fields, expected_fields)
        )
        assert payload_hash == expected_hash
        assert extract_staged_membership_key(packed_slots, len(definition.root_fields), ((1, "string"),)) == (
            expected_fields["panel"],
        )
