# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fast direct Parquet-to-bucketed-stage SOURCE streamer for compact v2 builds.

Streams sealed Parquet parts directly into unlogged, unindexed bucketed stage
partitions inside the isolated candidate snapshot schema, bypassing
``source_bulk_landing``, ``custom_import_build_occurrence``, and
``custom_import_root/child_revision`` staging altogether.
"""

from __future__ import annotations

import asyncio
import os
import time
from collections import defaultdict
from dataclasses import dataclass, replace
from decimal import Decimal

import msgpack
from sqlalchemy import Sequence, func, select, text
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.schema import CreateTable

from db.models.custom_import import CustomImportBuildStream, CustomImportCapture, CustomImportPack
from db.models.custom_import_family_block import CustomImportFamilyBlockLayout
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import import build_source as staging
from process.custom_import.capture import iter_parquet_source_batches, iter_records
from process.custom_import.continuation_runtime import producer_token_sha256
from process.custom_import.definition import CustomImportDefinition, Field, SourceStream
from process.custom_import.execution import LeaseGrant
from process.custom_import.family import (
    MAX_SCALAR_INTEGER,
    MIN_SCALAR_INTEGER,
    FamilyRejection,
    _canonical_child_key,
    _key,
    _parent_key,
    _record_error,
)
from process.custom_import.family_block_binding import FamilyBlockUnavailable, checked_binding
from process.custom_import.family_block_layout import (
    READ_CAPABILITIES,
    candidate_layout_statements,
    copy_columns,
)
from process.custom_import.family_block_query_input import query_input_table
from process.custom_import.family_block_v2_codec import (
    compiled_serializer,
    encode_slot_values,
)
from process.custom_import.family_block_write import _legacy_readers, _producer_scope
from process.custom_import.family_raw_key import raw_family_key_evidence
from process.custom_import.query_projection_initialize import _close_and_copy_acl
from process.custom_import.runner_codec import (
    child_key_document,
    digest_text,
    pack_hash,
    root_key_evidence_from_tuple,
)
from process.custom_import.runner_graph import rejection_model
from process.custom_import.runner_types import CandidateRunnerError, LeaseAuthorityLost
from process.custom_import.snowflake_bundle_replay import (
    _aggregate_parquet_arrow_bytes,
    _stream_fields,
    _validate_replay_partition_schema,
)
from process.custom_import.storage_layout import snapshot_schema

DIRECT_COMPACT_ENV = "HLTHPRT_CUSTOM_IMPORT_DIRECT_COMPACT"
DIRECT_PROGRESS_TABLE = "direct_stage_progress"
DIRECT_VOLATILE_GUARD_TABLE = "direct_stage_volatile_guard"
DIRECT_COMMITMENTS_TABLE = "direct_stage_commitments"
DIRECT_REJECTED_TABLE = "direct_stage_rejected_roots"
_PACK_IDS = Sequence("custom_import_pack_pack_id_seq", schema=CustomImportPack.__table__.schema)
_DIALECT = dialect()

DIRECT_BUCKET_COLUMNS = (
    "record_kind",
    "stream_slot",
    "collection_slot",
    "pack_id",
    "source_ordinal",
    "raw_key_sha256",
    "raw_key_canonical",
    "typed_key_sha256",
    "typed_key_canonical",
    "child_key_sha256",
    "child_key_canonical",
    "initial_code",
    "rejection_key",
    "rejection_hash",
    "rejection_evidence",
    "packed_slots",
    "payload_sha256",
)

_V1_FAMILY_CHECK = (
    "family_revision_id > 0 AND root_record_id > 0 AND root_revision_id > 0 AND "
    "entity_binding_id > 0 AND child_count >= 0 AND fragment_count > 0 AND "
    "raw_bytes > 0 AND stored_bytes > 0 AND membership_count > 0 AND root_source_ordinal >= 0"
)
_V2_FAMILY_CHECK = (
    "family_revision_id > 0 AND root_record_id > 0 AND root_revision_id > 0 AND "
    "entity_binding_id > 0 AND child_count >= 0 AND membership_count >= 0 AND root_source_ordinal >= 0 AND "
    "((fragment_count = 0 AND raw_bytes = 0 AND stored_bytes = 0 AND child_count = 0 AND NOT retained AND "
    "membership_count = 0) OR (fragment_count > 0 AND raw_bytes > 0 AND stored_bytes > 0))"
)


def direct_bucket_table_name(bucket_index: int) -> str:
    """Return the fixed unlogged bucket partition name inside the candidate schema."""
    return f"direct_stage_p_{bucket_index}"


def choose_bucket_count(total_rows: int) -> int:
    """Pick partition count so every in-memory bucket stays comfortably bounded."""
    if total_rows < 200_000:
        return 4
    if total_rows < 2_000_000:
        return 16
    if total_rows < 10_000_000:
        return 64
    return 128


def is_direct_compact_enabled() -> bool:
    """Return whether runtime configuration opted fresh snapshots into direct compact v2."""
    return os.environ.get(DIRECT_COMPACT_ENV, "").strip().lower() in {"1", "true", "yes"}


def is_direct_compact_eligible(build) -> bool:
    """Direct compact v2 handles fresh complete-scope snapshot builds."""
    return build.refresh_mode == "snapshot" and bool(build.complete_scope)


async def has_direct_stage(session, family_id: int) -> bool:
    """Return whether this isolated candidate was initialized in direct compact v2 mode."""
    if getattr(session, "scalar", None) is None:
        return False
    schema_name = snapshot_schema(family_id)
    qualified = f"{_DIALECT.identifier_preparer.quote_schema(schema_name)}.{DIRECT_PROGRESS_TABLE}"
    await staging._prepare_statement(session)
    return bool(
        await session.scalar(
            text("SELECT pg_catalog.to_regclass(:relation_name) IS NOT NULL"),
            {"relation_name": qualified},
        )
    )


async def require_direct_stage_intact(session, family_id: int) -> None:
    """Fail closed if unlogged direct staging tables were truncated by crash recovery."""
    schema_sql = _DIALECT.identifier_preparer.quote_schema(snapshot_schema(family_id))
    await staging._prepare_statement(session)
    sentinel = await session.scalar(text(f"SELECT id FROM {schema_sql}.{DIRECT_VOLATILE_GUARD_TABLE} WHERE id = 1"))
    if sentinel != 1:
        raise CandidateRunnerError("direct staging unlogged state was lost after crash recovery")


async def load_direct_bucket_count(session, family_id: int) -> int | None:
    """Return the initialized bucket count if direct staging is active, else None."""
    if not await has_direct_stage(session, family_id):
        return None
    await require_direct_stage_intact(session, family_id)
    schema_sql = _DIALECT.identifier_preparer.quote_schema(snapshot_schema(family_id))
    await staging._prepare_statement(session)
    return await session.scalar(text(f"SELECT bucket_count FROM {schema_sql}.{DIRECT_PROGRESS_TABLE} WHERE id = 1"))


async def _can_start_direct_stage(session, build) -> bool:
    if not is_direct_compact_eligible(build) or build.source_occurrence_count != 0:
        return False
    await staging._prepare_statement(session)
    started = await session.scalar(
        select(
            select(CustomImportBuildStream.stream_slot)
            .where(
                CustomImportBuildStream.build_id == build.build_id,
                (CustomImportBuildStream.next_part_ordinal != 1)
                | (CustomImportBuildStream.next_part_row_ordinal != 0)
                | (CustomImportBuildStream.next_source_ordinal != 0)
                | (CustomImportBuildStream.next_pack_ordinal != 0)
                | CustomImportBuildStream.replay_verified_at.is_not(None),
            )
            .exists()
        )
    )
    return not started


async def _create_compact_candidate_tables(session, parent, binding, definition: CustomImportDefinition) -> None:
    """Create compact v2 candidate tables and query input staging in the candidate schema."""
    for role, ddl_sql in candidate_layout_statements(binding):
        if role == "family":
            ddl_sql = ddl_sql.replace(_V1_FAMILY_CHECK, _V2_FAMILY_CHECK, 1)
        await staging._prepare_statement(session)
        await session.execute(text(ddl_sql))
        if role != "winner":
            table = binding.tables_by_role[role]
            readers = await _legacy_readers(session, READ_CAPABILITIES[role], table.schema)
            await _close_and_copy_acl(session, parent, table.name, readers)
    if definition.query.child_collection is not None:
        query_input = query_input_table(binding.family_id, binding.layout)
        await staging._prepare_statement(session)
        await session.execute(CreateTable(query_input))
        await _close_and_copy_acl(session, parent, query_input.name, ())


_PROGRESS_DDL_TEMPLATE = """
    CREATE TABLE {schema_sql}.{table_name} (
        id smallint PRIMARY KEY CHECK (id = 1),
        bucket_count integer NOT NULL CHECK (bucket_count > 0),
        next_bucket integer NOT NULL DEFAULT 0 CHECK (next_bucket >= 0),
        finalize_stage text NOT NULL DEFAULT 'buckets',
        reserved_generation_id bigint,
        accepted_family_count bigint NOT NULL DEFAULT 0,
        retained_family_count bigint NOT NULL DEFAULT 0,
        source_rejection_count bigint NOT NULL DEFAULT 0,
        total_child_count bigint NOT NULL DEFAULT 0,
        root_scalar_count bigint NOT NULL DEFAULT 0,
        child_scalar_count bigint NOT NULL DEFAULT 0,
        winner_count bigint NOT NULL DEFAULT 0,
        candidate_context_count bigint NOT NULL DEFAULT 0,
        last_profile_slot smallint,
        last_entity_binding_id bigint,
        last_context_key_sha256 bytea,
        projection_evidence_json text,
        collection_counts_json text NOT NULL DEFAULT '{{}}'
    )
"""
_VOLATILE_GUARD_DDL_TEMPLATE = """
    CREATE UNLOGGED TABLE {schema_sql}.{table_name} (
        id smallint PRIMARY KEY CHECK (id = 1)
    )
"""
_COMMITMENTS_DDL_TEMPLATE = """
    CREATE UNLOGGED TABLE {schema_sql}.{table_name} (
        family_revision_id bigint NOT NULL,
        root_record_id bigint NOT NULL,
        logical_key_sha256 bytea NOT NULL,
        key_contract_sha256 bytea NOT NULL,
        canonical_logical_key text NOT NULL,
        entity_binding_id bigint NOT NULL,
        family_sha256 bytea NOT NULL,
        child_count bigint NOT NULL,
        root_payload_sha256 bytea NOT NULL,
        root_source_ordinal bigint NOT NULL,
        retained boolean NOT NULL
    )
"""
_REJECTED_DDL_TEMPLATE = """
    CREATE UNLOGGED TABLE {schema_sql}.{table_name} (
        raw_key_sha256 bytea NOT NULL,
        typed_key_sha256 bytea,
        canonical_typed_key text
    )
"""


async def _create_stage_control_tables(session, parent, schema_sql: str, bucket_count: int) -> None:
    """Create logged progress table plus unlogged sentinel, commitment, and rejected-root tables."""
    ddl_specs = (
        (DIRECT_PROGRESS_TABLE, _PROGRESS_DDL_TEMPLATE),
        (DIRECT_VOLATILE_GUARD_TABLE, _VOLATILE_GUARD_DDL_TEMPLATE),
        (DIRECT_COMMITMENTS_TABLE, _COMMITMENTS_DDL_TEMPLATE),
        (DIRECT_REJECTED_TABLE, _REJECTED_DDL_TEMPLATE),
    )
    for tbl_name, template in ddl_specs:
        await staging._prepare_statement(session)
        await session.execute(text(template.format(schema_sql=schema_sql, table_name=tbl_name)))
        await _close_and_copy_acl(session, parent, tbl_name, ())
    await staging._prepare_statement(session)
    await session.execute(
        text(f"INSERT INTO {schema_sql}.{DIRECT_PROGRESS_TABLE}(id, bucket_count) VALUES (1, :bucket_count)"),
        {"bucket_count": bucket_count},
    )
    await staging._prepare_statement(session)
    await session.execute(text(f"INSERT INTO {schema_sql}.{DIRECT_VOLATILE_GUARD_TABLE}(id) VALUES (1)"))


async def _create_stage_bucket_tables(session, parent, schema_sql: str, bucket_count: int) -> None:
    """Create unlogged, unindexed hash-partition bucket tables."""
    for bucket_idx in range(bucket_count):
        bucket_name = direct_bucket_table_name(bucket_idx)
        bucket_ddl = f"""
            CREATE UNLOGGED TABLE {schema_sql}.{bucket_name} (
                record_kind smallint NOT NULL,
                stream_slot smallint NOT NULL,
                collection_slot smallint NOT NULL,
                pack_id bigint NOT NULL,
                source_ordinal bigint NOT NULL,
                raw_key_sha256 bytea,
                raw_key_canonical text,
                typed_key_sha256 bytea,
                typed_key_canonical text,
                child_key_sha256 bytea,
                child_key_canonical text,
                initial_code text,
                rejection_key text,
                rejection_hash bytea,
                rejection_evidence text,
                packed_slots bytea,
                payload_sha256 bytea
            )
        """
        await staging._prepare_statement(session)
        await session.execute(text(bucket_ddl))
        await _close_and_copy_acl(session, parent, bucket_name, ())


async def ensure_direct_stage(
    session, build, definition: CustomImportDefinition, registry, *, enable: bool = False
) -> tuple[int, int] | None:
    """Initialize compact v2 tables and unlogged stage buckets when opted in."""
    if getattr(session, "scalar", None) is None:
        return None
    family_id = await staging._resolve_build_snapshot(session, build.build_id)
    existing_buckets = await load_direct_bucket_count(session, family_id)
    if existing_buckets is not None:
        return family_id, existing_buckets
    if not (enable or is_direct_compact_enabled()) or not await _can_start_direct_stage(session, build):
        return None
    await staging._prepare_statement(session)
    if await session.get(CustomImportFamilyBlockLayout, family_id) is not None:
        return None
    await staging._prepare_statement(session)
    parent = await session.scalar(
        select(CustomImportSnapshotFamily)
        .where(CustomImportSnapshotFamily.family_id == family_id, *_producer_scope(build))
        .with_for_update()
    )
    if parent is None or parent.generation_id is not None:
        raise FamilyBlockUnavailable("direct compact candidate is missing or already assigned")
    await staging._prepare_statement(session)
    total_rows = int(
        await session.scalar(
            select(func.coalesce(func.sum(CustomImportCapture.committed_record_count), 0)).where(
                CustomImportCapture.capture_bundle_id == build.capture_bundle_id
            )
        )
        or 0
    )
    bucket_count = choose_bucket_count(total_rows)
    binding = checked_binding(family_id, definition, registry.child_collection_slots)
    await _create_compact_candidate_tables(session, parent, binding, definition)
    schema_sql = _DIALECT.identifier_preparer.quote_schema(snapshot_schema(family_id))
    await _create_stage_control_tables(session, parent, schema_sql, bucket_count)
    await _create_stage_bucket_tables(session, parent, schema_sql, bucket_count)
    session.add(
        CustomImportFamilyBlockLayout(
            family_id=family_id,
            layout_contract="custom-import/family-block-layout/v1",
            definition_sha256=bytes.fromhex(definition.digest),
            collection_slots=dict(registry.child_collection_slots),
            phase="loading",
        )
    )
    await staging._flush_page(session)
    return family_id, bucket_count


@dataclass(frozen=True)
class DirectSliceBudget:
    """Row, byte, and deadline bounds for one bounded direct part slice."""

    part_row_ordinal: int
    source_ordinal: int
    bucket_count: int
    max_rows: int
    max_bytes: int
    deadline: float
    soft_deadline: float
    is_final_part: bool


@dataclass(frozen=True)
class PreparedDirectPartSlice:
    """One bounded slice of a sealed part prepared for unlogged bucket COPY."""

    part_ordinal: int
    first_row: int
    row_count: int
    first_source: int
    has_reached_eof: bool
    is_final_part: bool
    valid_payload_hashes: tuple[bytes, ...]
    rows_by_bucket: dict[int, list[tuple[object, ...]]]
    byte_count: int = 0


@dataclass(frozen=True)
class PreparedDirectBatch:
    """One bounded batch ready to commit atomically to unlogged bucket partitions."""

    slices: tuple[PreparedDirectPartSlice, ...]
    total_rows: int
    valid_payload_hashes: tuple[bytes, ...]
    rows_by_bucket: dict[int, list[tuple[object, ...]]]


@dataclass(frozen=True)
class _DirectStreamEncoder:
    request: staging.SourceBuildRequest
    stream: SourceStream
    stream_slot: int
    collection_slot: int
    fields: tuple[Field, ...]
    fields_by_id_map: dict[str, Field]
    serializer: object
    bucket_count: int


def _normalize_integer_cell(field: Field, cell_val: object) -> object:
    if field.value_type != "integer" or not isinstance(cell_val, Decimal):
        return cell_val
    if not cell_val.is_finite() or cell_val != cell_val.to_integral_value():
        raise CandidateRunnerError("bundle replay integer result is invalid")
    integer_val = int(cell_val)
    if not MIN_SCALAR_INTEGER <= integer_val <= MAX_SCALAR_INTEGER:
        raise CandidateRunnerError("bundle replay integer result exceeds the signed 64-bit range")
    return integer_val


def _classify_direct_row(encoder: _DirectStreamEncoder, row_by_field: dict[str, object]) -> tuple[object, str | None]:
    definition = encoder.request.definition
    if encoder.stream.record_kind == "root":
        root_key = _key(row_by_field, definition.root_logical_key)
        code = (
            "root_key_missing"
            if root_key is None
            else _record_error(row_by_field, encoder.fields_by_id_map, entity_field=definition.entity_field)
        )
        return root_key, code
    collection = definition.collections_by_name[encoder.stream.child_collection]
    root_key = _parent_key(row_by_field, collection)
    code = "orphan_child" if root_key is None else _record_error(row_by_field, encoder.fields_by_id_map)
    child_key_tuple = _key(row_by_field, collection.child_key)
    if code is None and child_key_tuple is None:
        code = "child_key_missing"
    if code is None and _canonical_child_key(collection, child_key_tuple, encoder.fields_by_id_map) is None:
        code = "field_type_invalid"
    return root_key, code


def _encode_valid_direct_row(
    encoder: _DirectStreamEncoder,
    row_by_field: dict[str, object],
    typed_canonical: str | None,
) -> tuple[str | None, bytes | None, bytes, bytes, int]:
    slot_entries = encode_slot_values(encoder.fields, row_by_field)
    canonical_payload = encoder.serializer.serialize_from_slots(slot_entries)
    payload_hash = digest_text(f"{encoder.stream.record_kind}-payload", canonical_payload)
    packed_slots = msgpack.packb(slot_entries, use_bin_type=True)
    added_bytes = len(canonical_payload.encode("utf-8"))
    if encoder.stream.record_kind == "root":
        return None, None, packed_slots, payload_hash, added_bytes
    child_canonical = child_key_document(encoder.request.definition, encoder.stream.child_collection, row_by_field)
    child_hash = digest_text("child-key", child_canonical)
    added_bytes += len(child_canonical.encode("utf-8")) + (
        0 if typed_canonical is None else len(typed_canonical.encode("utf-8"))
    )
    return child_canonical, child_hash, packed_slots, payload_hash, added_bytes


def _encode_rejected_direct_row(
    request: staging.SourceBuildRequest, root_key, code: str
) -> tuple[str | None, bytes | None, str, int]:
    rejected_entry = rejection_model(
        request,
        LeaseGrant(request.execution_id, request.fence, request.build_deadline_at, "running"),
        0,
        FamilyRejection(root_key, code),
    )
    rejection_key = rejected_entry.canonical_root_key
    rejection_evidence = rejected_entry.canonical_evidence
    added_bytes = (0 if rejection_key is None else len(rejection_key.encode("utf-8"))) + (
        0 if rejection_evidence is None else len(rejection_evidence.encode("utf-8"))
    )
    return rejection_key, rejected_entry.root_key_sha256, rejection_evidence, added_bytes


def _prepare_direct_tuple(
    encoder: _DirectStreamEncoder,
    source_ordinal: int,
    row_by_field: dict[str, object],
) -> tuple[int, tuple[object, ...], bytes | None, int]:
    """Validate, hash, and route one decoded source row to its bucket COPY tuple."""
    request = encoder.request
    root_key, code = _classify_direct_row(encoder, row_by_field)
    raw = raw_family_key_evidence(root_key, maximum_canonical_bytes=request.page_byte_limit)
    typed = root_key_evidence_from_tuple(request.definition, root_key)
    if code is None and typed is None:
        code = "field_type_invalid"
    raw_canonical, raw_hash = (None, None) if raw is None else raw
    typed_canonical, typed_hash = (None, None) if typed is None else typed
    byte_count = (0 if raw_canonical is None else len(raw_canonical.encode("utf-8"))) + (
        0 if typed_canonical is None else len(typed_canonical.encode("utf-8"))
    )
    child_canonical = child_hash = packed_slots = payload_hash = None
    rejection_key = rejection_hash = rejection_evidence = None
    if code is None:
        child_canonical, child_hash, packed_slots, payload_hash, extra_bytes = _encode_valid_direct_row(
            encoder, row_by_field, typed_canonical
        )
    else:
        rejection_key, rejection_hash, rejection_evidence, extra_bytes = _encode_rejected_direct_row(
            request, root_key, code
        )
    byte_count += extra_bytes
    if byte_count > request.page_byte_limit:
        raise CandidateRunnerError("one source occurrence exceeds the admitted page byte limit")
    partition_key = typed_hash if typed_hash is not None else raw_hash
    bucket_idx = 0 if partition_key is None else int.from_bytes(partition_key[:2], "big") % encoder.bucket_count
    record_kind_code = 0 if encoder.stream.record_kind == "root" else 1
    staged_tuple = (
        record_kind_code,
        encoder.stream_slot,
        encoder.collection_slot,
        0,
        source_ordinal,
        raw_hash,
        raw_canonical,
        typed_hash,
        typed_canonical,
        child_hash,
        child_canonical,
        code,
        rejection_key,
        rejection_hash,
        rejection_evidence,
        packed_slots,
        payload_hash,
    )
    return bucket_idx, staged_tuple, payload_hash, byte_count


def _iter_parquet_row_dicts(part, stream: SourceStream, fields: tuple[Field, ...], policy):
    expected_ids = tuple(field.field_id for field in fields)
    int_indices = tuple((idx, field) for idx, field in enumerate(fields) if field.value_type == "integer")
    for batch in iter_parquet_source_batches(part.capture, stream, limits=policy.part_limits):
        col_lists = [batch.column(idx).to_pylist() for idx in range(batch.num_columns)]
        for col_idx, field in int_indices:
            col_lists[col_idx] = [_normalize_integer_cell(field, cell) for cell in col_lists[col_idx]]
        for row_idx in range(batch.num_rows):
            yield {expected_ids[col_idx]: col_lists[col_idx][row_idx] for col_idx in range(len(expected_ids))}


def _iter_part_row_dicts(part, stream: SourceStream, fields: tuple[Field, ...], policy):
    if stream.format == "parquet":
        yield from _iter_parquet_row_dicts(part, stream, fields, policy)
        return
    expected_ids = tuple(field.field_id for field in fields)
    for decoded in iter_records(part.capture, stream, limits=policy.part_limits):
        if tuple(decoded.values) != expected_ids:
            raise CandidateRunnerError("replayed fields differ from the retained stream")
        yield {field.field_id: _normalize_integer_cell(field, decoded.values[field.field_id]) for field in fields}


def verify_direct_part(context, part, policy, *, part_row_ordinal: int, source_ordinal: int) -> None:
    """Validate part schema, cursor bounds, and Arrow byte budget before staging."""
    fields = _stream_fields(context.request.definition, context.stream)
    _validate_replay_partition_schema(part.capture, fields=fields, limits=policy.part_limits)
    if part_row_ordinal > part.record_count or source_ordinal < part_row_ordinal:
        raise CandidateRunnerError("SOURCE cursor exceeds the retained part")
    arrow_limits = replace(
        policy.part_limits,
        maximum_decoded_bytes=max(policy.part_limits.maximum_decoded_bytes, policy.maximum_part_arrow_bytes),
    )
    actual_arrow = _aggregate_parquet_arrow_bytes(part.capture, limits=arrow_limits, decoded_bytes=0)
    if actual_arrow > policy.maximum_part_arrow_bytes:
        raise CandidateRunnerError("decoded part Arrow bytes exceed the admitted replay limit")


def _is_slice_budget_reached(taken_rows: int, taken_bytes: int, budget: DirectSliceBudget) -> bool:
    return bool(
        taken_rows
        and (
            taken_rows >= budget.max_rows
            or taken_bytes >= budget.max_bytes
            or (taken_rows % 1024 == 0 and time.monotonic() >= budget.soft_deadline)
        )
    )


def _build_direct_encoder(context, fields: tuple[Field, ...], bucket_count: int) -> _DirectStreamEncoder:
    return _DirectStreamEncoder(
        context.request,
        context.stream,
        context.stream_slot,
        context.collection_slot,
        fields,
        {field.field_id: field for field in fields},
        compiled_serializer(fields),
        bucket_count,
    )


async def prepare_direct_part_slice(
    context,
    part,
    policy,
    budget: DirectSliceBudget,
) -> PreparedDirectPartSlice:
    """Decode and validate one bounded part slice into per-bucket COPY tuples."""
    verify_direct_part(
        context, part, policy, part_row_ordinal=budget.part_row_ordinal, source_ordinal=budget.source_ordinal
    )
    fields = _stream_fields(context.request.definition, context.stream)
    encoder = _build_direct_encoder(context, fields, budget.bucket_count)
    rows_by_bucket: dict[int, list[tuple[object, ...]]] = defaultdict(list)
    valid_hashes: list[bytes] = []
    seen_rows = taken_rows = taken_bytes = 0
    has_reached_eof = True
    row_iter = _iter_part_row_dicts(part, context.stream, fields, policy)
    try:
        for row_by_field in row_iter:
            if seen_rows < budget.part_row_ordinal:
                seen_rows += 1
                continue
            if _is_slice_budget_reached(taken_rows, taken_bytes, budget):
                has_reached_eof = False
                break
            if taken_rows % 1024 == 0:
                if time.monotonic() >= budget.deadline:
                    raise LeaseAuthorityLost("SOURCE read deadline elapsed")
                await asyncio.sleep(0)
            bucket_idx, staged_tuple, payload_hash, row_bytes = _prepare_direct_tuple(
                encoder, budget.source_ordinal + taken_rows, row_by_field
            )
            if taken_rows > 0 and taken_bytes + row_bytes > budget.max_bytes:
                has_reached_eof = False
                break
            rows_by_bucket[bucket_idx].append(staged_tuple)
            if payload_hash is not None:
                valid_hashes.append(payload_hash)
            taken_rows += 1
            taken_bytes += row_bytes
            seen_rows += 1
        if has_reached_eof and seen_rows != part.record_count:
            raise CandidateRunnerError("decoded part record count differs from the sealed receipt")
    finally:
        if hasattr(row_iter, "close"):
            row_iter.close()
    return PreparedDirectPartSlice(
        part.ordinal,
        budget.part_row_ordinal,
        taken_rows,
        budget.source_ordinal,
        has_reached_eof,
        budget.is_final_part,
        tuple(valid_hashes),
        dict(rows_by_bucket),
        byte_count=taken_bytes,
    )


def merge_direct_slices(slices: list[PreparedDirectPartSlice]) -> PreparedDirectBatch:
    """Combine sequential slices into one bounded commit batch."""
    merged_by_bucket: dict[int, list[tuple[object, ...]]] = defaultdict(list)
    valid_hashes: list[bytes] = []
    total_rows = 0
    for part_slice in slices:
        total_rows += part_slice.row_count
        valid_hashes.extend(part_slice.valid_payload_hashes)
        for bucket_idx, bucket_rows in part_slice.rows_by_bucket.items():
            merged_by_bucket[bucket_idx].extend(bucket_rows)
    return PreparedDirectBatch(
        slices=tuple(slices),
        total_rows=total_rows,
        valid_payload_hashes=tuple(valid_hashes),
        rows_by_bucket=dict(merged_by_bucket),
    )


async def _copy_direct_pack_and_buckets(
    session,
    build,
    stream_row: CustomImportBuildStream,
    context,
    batch: PreparedDirectBatch,
    family_id: int,
) -> None:
    """Insert one pack record and binary-COPY prepared tuples into unlogged bucket tables."""
    schema_name = snapshot_schema(family_id)
    await staging._prepare_statement(session)
    pack_id = int(await session.scalar(select(_PACK_IDS.next_value())))
    await staging._prepare_statement(session)
    stamp = await session.scalar(select(func.clock_timestamp()))
    label = "root" if context.stream.record_kind == "root" else context.stream.child_collection
    pack_table = checked_binding(
        family_id, context.request.definition, context.registry.child_collection_slots
    ).tables_by_role["pack"]
    pack_by_col = {
        "pack_id": pack_id,
        "execution_id": build.execution_id,
        "dataset_id": build.dataset_id,
        "definition_revision_id": build.definition_revision_id,
        "schema_revision_id": build.schema_revision_id,
        "stream_slot": context.stream_slot,
        "pack_ordinal": stream_row.next_pack_ordinal,
        "capture_bundle_id": build.capture_bundle_id,
        "record_count": len(batch.valid_payload_hashes),
        "pack_sha256": pack_hash(label, batch.valid_payload_hashes),
        "producing_fence": build.producing_fence,
        "producing_token_sha256": bytes(producer_token_sha256(context.request)),
        "created_at": stamp,
    }
    pack_cols = copy_columns(pack_table)
    pack_cells = tuple(pack_by_col[col_name] for col_name in pack_cols)
    connection = await session.connection()
    raw_conn = await connection.get_raw_connection()
    await staging._prepare_statement(session)
    await raw_conn.driver_connection.copy_records_to_table(
        pack_table.name, schema_name=schema_name, columns=pack_cols, records=(pack_cells,)
    )
    for bucket_idx, staged_rows in sorted(batch.rows_by_bucket.items()):
        if not staged_rows:
            continue
        filled_rows = tuple((*staged_row[:3], pack_id, *staged_row[4:]) for staged_row in staged_rows)
        await staging._prepare_statement(session)
        status = await raw_conn.driver_connection.copy_records_to_table(
            direct_bucket_table_name(bucket_idx),
            schema_name=schema_name,
            columns=DIRECT_BUCKET_COLUMNS,
            records=filled_rows,
        )
        if status != f"COPY {len(filled_rows)}":
            raise FamilyBlockUnavailable("direct stage COPY count differs from prepared rows")
    stream_row.next_pack_ordinal += 1


async def commit_direct_source_batch(
    session,
    build,
    stream_row: CustomImportBuildStream,
    context,
    batch: PreparedDirectBatch,
    *,
    family_id: int,
) -> int:
    """Write pack, native binary COPY into bucket partitions, and advance stream cursor."""
    if build.phase != "source":
        raise CandidateRunnerError("source build is already frozen")
    if batch.total_rows > 0:
        await _copy_direct_pack_and_buckets(session, build, stream_row, context, batch, family_id)
    for part_slice in batch.slices:
        if part_slice.row_count > 0:
            stream_row.next_part_row_ordinal += part_slice.row_count
            stream_row.next_source_ordinal += part_slice.row_count
            build.source_occurrence_count += part_slice.row_count
        await staging._flush_page(session)
        if part_slice.has_reached_eof:
            await staging._call(
                session,
                "finish_custom_import_build_source_part",
                (
                    ("bigint", build.build_id),
                    ("smallint", context.stream_slot),
                    ("integer", part_slice.part_ordinal),
                ),
            )
            await session.refresh(stream_row)
    return batch.total_rows


async def freeze_direct_source_if_complete(session, build) -> None:
    """Freeze SOURCE to admission once every declared stream reached verified EOF."""
    await staging._prepare_statement(session)
    stream_rows = (
        await session.scalars(
            select(CustomImportBuildStream)
            .where(CustomImportBuildStream.build_id == build.build_id)
            .execution_options(populate_existing=True)
        )
    ).all()
    if stream_rows and all(stream_entry.replay_verified_at is not None for stream_entry in stream_rows):
        await staging._call(session, "freeze_custom_import_build_source", (("bigint", build.build_id),))
        await session.refresh(build)


async def replay_verified_direct_stage(session_factory, request):
    """Return the committed terminal staging result on a same-request retry after direct seal."""
    if not callable(session_factory):
        return None
    from db.models.custom_import import CustomImportBuildAttempt, CustomImportExecution, CustomImportGenerationSeal

    async with session_factory() as session, session.begin():
        await session.execute(text("SET TRANSACTION READ ONLY"))
        await staging._set_timeout(session, request.statement_timeout_ms)
        execution = await session.get(CustomImportExecution, request.execution_id)
        build = (
            await session.scalars(
                select(CustomImportBuildAttempt).where(
                    CustomImportBuildAttempt.execution_id == request.execution_id,
                    CustomImportBuildAttempt.producing_fence == request.fence,
                )
            )
        ).one_or_none()
        if (
            execution is None
            or build is None
            or build.phase != "verified"
            or build.generation_id is None
            or execution.state not in {"completed", "no_change"}
        ):
            return None
        staging._assert_build_identity(build, request, execution)
        seal = await session.get(CustomImportGenerationSeal, build.generation_id)
        family = (
            await session.scalars(
                select(CustomImportSnapshotFamily).where(
                    CustomImportSnapshotFamily.generation_id == build.generation_id
                )
            )
        ).one_or_none()
        layout = None if family is None else await session.get(CustomImportFamilyBlockLayout, family.family_id)
        if seal is None or layout is None or layout.phase != "logged":
            return None
        return staging.SourceBuildResult(
            build.build_id,
            request.execution_id,
            build.phase,
            build.source_occurrence_count,
            build.candidate_error_count,
        )


async def _commit_direct_part_slice(
    session_factory, request, build_id, slot, stream_context, direct_batch, family_id, expected_cursor
):
    async with staging._page_session(session_factory, request, build_id) as (write_session, build):
        await staging._prepare_statement(write_session)
        stream_row = (
            await write_session.scalars(
                select(CustomImportBuildStream)
                .where(CustomImportBuildStream.build_id == build_id, CustomImportBuildStream.stream_slot == slot)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).one()
        actual_cursor = (
            stream_row.next_part_ordinal,
            stream_row.next_part_row_ordinal,
            stream_row.next_source_ordinal,
        )
        if stream_row.replay_verified_at is not None or actual_cursor != expected_cursor:
            return False, actual_cursor, stream_row.replay_verified_at is not None
        await commit_direct_source_batch(
            write_session, build, stream_row, stream_context, direct_batch, family_id=family_id
        )
        return (
            True,
            (stream_row.next_part_ordinal, stream_row.next_part_row_ordinal, stream_row.next_source_ordinal),
            stream_row.replay_verified_at is not None,
        )


async def _replay_direct_part(
    session_factory,
    stream_context,
    part,
    policy,
    *,
    part_row_offset: int,
    source_start: int,
    direct_info: tuple[int, int],
    is_final: bool,
):
    """Stream one retained part into direct stage bucket partitions."""
    from process.custom_import.bulk_page_codec import MAX_BATCH_BYTES, MAX_BATCH_ROWS

    family_id, bucket_count = direct_info
    request = stream_context.request
    while True:
        started = time.monotonic()
        expected_cursor = (part.ordinal, part_row_offset, source_start + part_row_offset)
        budget = DirectSliceBudget(
            part_row_ordinal=part_row_offset,
            source_ordinal=source_start + part_row_offset,
            bucket_count=bucket_count,
            max_rows=MAX_BATCH_ROWS,
            max_bytes=MAX_BATCH_BYTES,
            deadline=started + request.lease_seconds,
            soft_deadline=started + request.lease_seconds / 2,
            is_final_part=is_final,
        )
        part_slice = await prepare_direct_part_slice(stream_context, part, policy, budget)
        commit_slice = replace(part_slice, has_reached_eof=False) if is_final else part_slice
        direct_batch = merge_direct_slices([commit_slice])
        if direct_batch.total_rows > 0 or commit_slice.has_reached_eof:
            is_committed, actual_cursor, is_verified = await _commit_direct_part_slice(
                session_factory,
                request,
                stream_context.build_id,
                stream_context.stream_slot,
                stream_context,
                direct_batch,
                family_id,
                expected_cursor,
            )
            if not is_committed:
                if is_verified or actual_cursor[0] > part.ordinal:
                    return
                if actual_cursor[0] != part.ordinal or actual_cursor[1] < part_row_offset:
                    raise CandidateRunnerError("direct SOURCE durable cursor moved behind replay")
                part_row_offset = actual_cursor[1]
                continue
        part_row_offset += part_slice.row_count
        if part_slice.has_reached_eof:
            return


async def replay_direct_source(session_factory, request, registry, build_id, bundle_id, policy, cursors, direct_info):
    """Stream sealed parts directly into unlogged bucketed stage partitions."""
    import json

    contexts, primary = [], None
    final_part_by_slot: dict[int, int] = {}
    starts_by_stream = dict.fromkeys(registry.stream_slots, 0)
    stream_by_id = {stream.stream_id: stream for stream in request.definition.source_streams}
    try:
        session = await staging._enter_context(contexts, session_factory())
        await staging._enter_context(contexts, session.begin())
        await staging._set_timeout(session, request.statement_timeout_ms)
        parts = await staging._enter_context(
            contexts,
            staging.open_segmented_parquet_parts(
                session,
                capture_bundle_id=bundle_id,
                dataset_id=request.dataset_id,
                definition_revision_id=request.definition_revision_id,
                schema_revision_id=request.schema_revision_id,
            ),
        )
        async for part in parts:
            stream_id = part.receipt.stream_id
            slot = registry.stream_slots[stream_id]
            cursor = cursors[slot]
            is_final = part.ordinal == json.loads(part.receipt.canonical_manifest)["part_count"]
            if part.ordinal >= cursor[0]:
                stream_context = staging._StreamContext(request, registry, build_id, stream_by_id[stream_id])
                await _replay_direct_part(
                    session_factory,
                    stream_context,
                    part,
                    policy,
                    part_row_offset=cursor[1] if part.ordinal == cursor[0] else 0,
                    source_start=starts_by_stream[stream_id],
                    direct_info=direct_info,
                    is_final=is_final,
                )
            starts_by_stream[stream_id] += part.record_count
            if is_final:
                final_part_by_slot[slot] = part.ordinal
    except BaseException as exc:
        primary = exc
    primary = await staging._close_contexts(contexts, primary)
    if primary is not None:
        raise primary
    if set(final_part_by_slot) != set(registry.stream_slots.values()):
        raise CandidateRunnerError("source replay lacks complete stream EOF")
    for slot, ordinal in sorted(final_part_by_slot.items()):
        await staging._finish_part(session_factory, request, build_id, slot, ordinal)
    async with staging._page_session(session_factory, request, build_id) as (session, _):
        await staging._call(session, "freeze_custom_import_build_source", (("bigint", build_id),))
