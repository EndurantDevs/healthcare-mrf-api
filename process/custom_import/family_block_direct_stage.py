# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fast direct Parquet-to-bucketed-stage SOURCE streamer for compact v2 builds.

Streams sealed Parquet parts directly into unlogged, unindexed bucketed stage
partitions inside the isolated candidate snapshot schema, bypassing
``source_bulk_landing``, ``custom_import_build_occurrence``, and
``custom_import_root/child_revision`` staging altogether.
"""

from __future__ import annotations

import asyncio
import importlib
import io
import os
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field, replace
from decimal import Decimal
from functools import lru_cache
from typing import Any

import msgpack
import pyarrow as pa
from sqlalchemy import Sequence, func, select, text
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.schema import CreateTable

from db.models.custom_import import (
    CustomImportBuildStream,
    CustomImportCapture,
    CustomImportCaptureParquetPart,
    CustomImportPack,
)
from db.models.custom_import_family_block import CustomImportFamilyBlockLayout
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import import build_source as staging
from process.custom_import.capture import (
    CaptureError,
    _nonnegative_parquet_integer,
    _validate_parquet_batch_schema,
    _validated_parquet_reader,
    iter_parquet_source_batches,
    iter_records,
    verify_capture,
)
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
from process.custom_import.family_raw_key import RawFamilyKeyError, raw_family_key_evidence
from process.custom_import.query_projection_initialize import _close_and_copy_acl
from process.custom_import.runner_codec import (
    child_key_document,
    digest_text,
    fields_by_collection,
    pack_hash,
    root_key_evidence_from_tuple,
)
from process.custom_import.runner_graph import rejection_model
from process.custom_import.runner_types import CandidateRunnerError, LeaseAuthorityLost
from process.custom_import.snowflake_bundle import SnowflakeBundleError
from process.custom_import.snowflake_bundle_replay import (
    _aggregate_parquet_arrow_bytes,
    _is_bundle_column_type_valid,
    _stream_fields,
    _validate_replay_partition_schema,
)
from process.custom_import.source_column_preparation import _column_window
from process.custom_import.source_preparation import MAX_FIELDS, _has_allocation_failure
from process.custom_import.storage_layout import snapshot_schema

_DIRECT_PARQUET_BATCH_ROWS = 16_384
_MAX_DIRECT_STAGE_ROWS = 65_536
_MAX_DIRECT_STAGE_INPUT_BYTES = 64 * 1024 * 1024
_COPY_TIMEOUT_REFRESH_SECONDS = 1.0

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
    ) WITH (autovacuum_enabled = false)
"""
_REJECTED_DDL_TEMPLATE = """
    CREATE UNLOGGED TABLE {schema_sql}.{table_name} (
        raw_key_sha256 bytea NOT NULL,
        typed_key_sha256 bytea,
        canonical_typed_key text
    ) WITH (autovacuum_enabled = false)
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
            ) WITH (autovacuum_enabled = false)
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
    valid_payload_hash_chunks: tuple[bytes, ...] = ()
    binary_chunks_by_bucket: dict[int, list[tuple[bytes, bytes, int]]] = field(default_factory=dict)


@dataclass(frozen=True)
class PreparedDirectBatch:
    """One bounded batch ready to commit atomically to unlogged bucket partitions."""

    slices: tuple[PreparedDirectPartSlice, ...]
    total_rows: int
    valid_payload_hashes: tuple[bytes, ...]
    rows_by_bucket: dict[int, list[tuple[object, ...]]]
    valid_payload_hash_chunks: tuple[bytes, ...] = ()
    binary_chunks_by_bucket: dict[int, list[tuple[bytes, bytes, int]]] = field(default_factory=dict)


@dataclass(frozen=True)
class DirectStagePlan:
    """Compiled native columnar layout paired with its Rust direct-stage plan."""

    layout: tuple[object, ...]
    parquet_names: tuple[str, ...]
    plan: object


_UNSET_DIRECT_PLAN = object()


def _declared_stream_fields(definition: CustomImportDefinition, stream: SourceStream) -> tuple[Field, ...]:
    """Return declared field order matching compact v2 slots and canonical record payloads."""
    if stream.record_kind == "root":
        return definition.root_fields
    return fields_by_collection(definition)[stream.child_collection]


def _native_capability(capability_name: str):
    """Look up an optional compiled capability in ``ptg2_address_canon``."""
    try:
        native_module = importlib.import_module("ptg2_address_canon")
    except ImportError as error:
        if _has_allocation_failure(error):
            raise
        return None
    capability = getattr(native_module, capability_name, None)
    return capability if callable(capability) else None


@lru_cache(maxsize=1)
def native_direct_stage_plan_factory():
    """Return the compiled direct-stage columnar plan factory when available."""
    return _native_capability("custom_import_direct_stage_plan_v1")


@lru_cache(maxsize=1)
def native_finalize_direct_stage_copy():
    """Return the compiled PG binary COPY pack_id stamper when available."""
    return _native_capability("custom_import_finalize_direct_stage_copy_v1")


@lru_cache(maxsize=1)
def native_pack_hash():
    """Return the compiled pack hash helper when available."""
    return _native_capability("custom_import_pack_hash_v1")


def compile_direct_plan(context) -> DirectStagePlan | None:
    """Compile one immutable Rust direct-stage layout in declared compact v2 slot order."""
    factory = native_direct_stage_plan_factory()
    if factory is None or native_finalize_direct_stage_copy() is None or sys.byteorder != "little":
        return None
    definition, stream = context.request.definition, context.stream
    parquet_fields = _stream_fields(definition, stream)
    declared_fields = _declared_stream_fields(definition, stream)
    if not 1 <= len(declared_fields) <= MAX_FIELDS or any(
        field_spec.value_type in {"date", "timestamp"} for field_spec in declared_fields
    ):
        return None
    index_by_field = {field_spec.field_id: index for index, field_spec in enumerate(declared_fields)}
    is_child = stream.record_kind == "child"
    if is_child:
        collection = definition.collections_by_name[stream.child_collection]
        fields_by_id = definition.fields_by_id
        if any(
            fields_by_id[key_part.root_field].value_type != fields_by_id[key_part.child_field].value_type
            for key_part in collection.parent_key
        ):
            return None
        root_keys = tuple(
            (key_part.root_field, index_by_field[key_part.child_field]) for key_part in collection.parent_key
        )
        child_keys = tuple(index_by_field[field_id] for field_id in collection.child_key)
        entity_index = None
    else:
        root_keys = tuple((field_id, index_by_field[field_id]) for field_id in definition.root_logical_key)
        child_keys = ()
        entity_index = index_by_field[definition.entity_field]
    if not 1 <= len(root_keys) <= 3 or (is_child and not 1 <= len(child_keys) <= 3):
        return None
    layout = (
        is_child,
        tuple(
            (field_spec.field_id, field_spec.value_type, field_spec.nullable, field_spec.projection_slot is not None)
            for field_spec in declared_fields
        ),
        root_keys,
        child_keys,
        entity_index,
    )
    parquet_names = tuple(field_spec.field_id for field_spec in parquet_fields)
    return DirectStagePlan(layout=layout, parquet_names=parquet_names, plan=factory(layout))


def _direct_column_descriptors(direct_plan: DirectStagePlan, record_batch: pa.RecordBatch):
    """Extract bounded immutable column buffer descriptors in declared compact v2 slot order."""
    if sys.byteorder != "little" or type(record_batch) is not pa.RecordBatch:
        return None
    if not 1 <= record_batch.num_rows <= _MAX_DIRECT_STAGE_ROWS or (
        tuple(record_batch.schema.names) != direct_plan.parquet_names
    ):
        return None
    index_by_parquet_name = {name: index for index, name in enumerate(direct_plan.parquet_names)}
    field_specs = direct_plan.layout[1]
    column_windows = tuple(
        _column_window(field_spec[0], record_batch.column(index_by_parquet_name[field_spec[0]]))
        for field_spec in field_specs
    )
    if any(column_window is None for column_window in column_windows):
        return None
    estimated_bytes = sum(
        128
        + len(column_window[0].encode())
        + len(column_window[1].encode())
        + sum(len(buffer_view) for buffer_view in column_window[6:] if buffer_view is not None)
        for column_window in column_windows
    )
    if estimated_bytes > _MAX_DIRECT_STAGE_INPUT_BYTES:
        return None
    return tuple(
        (
            *column_window[:6],
            *(None if buffer_view is None else bytes(buffer_view) for buffer_view in column_window[6:]),
        )
        for column_window in column_windows
    )


def _raise_direct_stage_error(category: str, code: str, ordinal: int) -> None:
    """Translate a Rust direct-stage row failure into the existing Python exception contract."""
    if category == "capture-error" and code == "record_byte_limit":
        raise CaptureError(f"record {ordinal} exceeds the byte limit")
    if category == "transport-error" and code == "integer_invalid":
        raise CandidateRunnerError("bundle replay integer result is invalid")
    if category == "transport-error" and code == "integer_range":
        raise CandidateRunnerError("bundle replay integer result exceeds the signed 64-bit range")
    if category == "preparation-error" and code == "raw_key_byte_limit":
        raise RawFamilyKeyError("raw family key exceeds the canonical byte limit")
    if category == "preparation-error" and code == "occurrence_byte_limit":
        raise CandidateRunnerError("one source occurrence exceeds the admitted page byte limit")
    raise CandidateRunnerError("direct stage column encoder returned an invalid error")


@dataclass(frozen=True)
class _DirectStreamEncoder:
    request: staging.SourceBuildRequest
    stream: SourceStream
    stream_slot: int
    collection_slot: int
    fields: tuple[Field, ...]
    parquet_fields: tuple[Field, ...]
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


def _build_direct_encoder(context, parquet_fields: tuple[Field, ...], bucket_count: int) -> _DirectStreamEncoder:
    declared_fields = _declared_stream_fields(context.request.definition, context.stream)
    return _DirectStreamEncoder(
        context.request,
        context.stream,
        context.stream_slot,
        context.collection_slot,
        declared_fields,
        parquet_fields,
        {field.field_id: field for field in declared_fields},
        compiled_serializer(declared_fields),
        bucket_count,
    )


def _validate_open_parquet_schema(schema: pa.Schema, labels: tuple[str, ...], fields: tuple[Field, ...]) -> None:
    """Validate an already-open Parquet reader's schema against declared stream fields."""
    expected_field_ids = tuple(field_spec.field_id for field_spec in fields)
    if labels != expected_field_ids:
        raise SnowflakeBundleError("bundle replay schema does not match the declared stream fields")
    fields_by_id = {field_spec.field_id: field_spec for field_spec in fields}
    if any(not _is_bundle_column_type_valid(column.type, fields_by_id[column.name]) for column in schema):
        raise SnowflakeBundleError("bundle replay schema type does not match the declared field")


def _stage_python_record_batch(
    encoder: _DirectStreamEncoder,
    record_batch: pa.RecordBatch,
    budget: DirectSliceBudget,
    *,
    taken_rows: int,
    taken_bytes: int,
    rows_by_bucket: dict[int, list[tuple[object, ...]]],
    valid_hashes: list[bytes],
) -> tuple[int, int, bool]:
    """Encode one Arrow RecordBatch through the Python oracle fallback path."""
    expected_ids = tuple(field_spec.field_id for field_spec in encoder.parquet_fields)
    int_indices = tuple(
        (column_index, field_spec)
        for column_index, field_spec in enumerate(encoder.parquet_fields)
        if field_spec.value_type == "integer"
    )
    column_lists = [record_batch.column(column_index).to_pylist() for column_index in range(record_batch.num_columns)]
    for column_index, field_spec in int_indices:
        column_lists[column_index] = [_normalize_integer_cell(field_spec, cell) for cell in column_lists[column_index]]
    for row_index in range(record_batch.num_rows):
        if _is_slice_budget_reached(taken_rows, taken_bytes, budget):
            return taken_rows, taken_bytes, True
        row_by_field = {
            expected_ids[column_index]: column_lists[column_index][row_index]
            for column_index in range(len(expected_ids))
        }
        bucket_index, staged_tuple, payload_hash, row_bytes = _prepare_direct_tuple(
            encoder, budget.source_ordinal + taken_rows, row_by_field
        )
        if taken_rows > 0 and taken_bytes + row_bytes > budget.max_bytes:
            return taken_rows, taken_bytes, True
        rows_by_bucket[bucket_index].append(staged_tuple)
        if payload_hash is not None:
            valid_hashes.append(payload_hash)
        taken_rows += 1
        taken_bytes += row_bytes
    return taken_rows, taken_bytes, False


def _iter_direct_parquet_batches(part, stream: SourceStream, policy):
    """Yield schema header then validated Arrow batches in one Parquet decode pass."""
    verify_capture(part.capture, stream, limits=policy.part_limits)
    batch_size = min(_DIRECT_PARQUET_BATCH_ROWS, policy.part_limits.maximum_records)
    emitted = logical_bytes = 0
    with _validated_parquet_reader(
        part.capture.payload,
        stream,
        policy.part_limits,
        expected_decoded_bytes=part.capture.manifest.decoded_bytes,
        expected_decoded_sha256=part.capture.manifest.decoded_sha256,
    ) as (parquet_reader, labels, expected_rows):
        yield ("header", parquet_reader.schema_arrow, labels, expected_rows)
        for record_batch in parquet_reader.iter_batches(
            batch_size=batch_size, use_threads=False, use_pandas_metadata=False
        ):
            _validate_parquet_batch_schema(record_batch, labels)
            logical_bytes += _nonnegative_parquet_integer(record_batch.nbytes)
            if logical_bytes > policy.part_limits.maximum_decoded_bytes:
                raise CaptureError("Parquet source payload exceeds the decoded-byte limit")
            if not record_batch.num_rows:
                continue
            if emitted + record_batch.num_rows > expected_rows:
                raise CaptureError("Parquet source payload has inconsistent row metadata")
            emitted += record_batch.num_rows
            yield ("batch", record_batch, logical_bytes)
        if emitted != expected_rows:
            raise CaptureError("Parquet source payload has inconsistent row metadata")


@dataclass
class _ParquetSliceAccumulator:
    """Mutable per-part accumulator keeping Parquet staging functions small and clear."""

    rows_by_bucket: dict[int, list[tuple[object, ...]]] = field(default_factory=lambda: defaultdict(list))
    binary_chunks_by_bucket: dict[int, list[tuple[bytes, bytes, int]]] = field(
        default_factory=lambda: defaultdict(list)
    )
    valid_hashes: list[bytes] = field(default_factory=list)
    valid_hash_chunks: list[bytes] = field(default_factory=list)
    encoder: _DirectStreamEncoder | None = None
    seen_rows: int = 0
    taken_rows: int = 0
    taken_bytes: int = 0
    has_reached_eof: bool = True


async def _try_native_record_batch(
    context,
    policy,
    budget: DirectSliceBudget,
    direct_plan: DirectStagePlan | None,
    work_batch: pa.RecordBatch,
    *,
    record_kind_code: int,
    taken_rows: int,
    taken_bytes: int,
):
    """Run the compiled Rust columnar direct-stage encoder off the event loop when eligible."""
    if direct_plan is None:
        return None
    descriptors = _direct_column_descriptors(direct_plan, work_batch)
    if descriptors is None:
        return None
    options = (
        record_kind_code,
        context.stream_slot,
        context.collection_slot,
        budget.source_ordinal + taken_rows,
        budget.bucket_count,
        context.request.page_byte_limit,
        policy.part_limits.maximum_record_bytes,
        budget.max_rows - taken_rows,
        budget.max_bytes - taken_bytes,
        taken_rows > 0,
    )
    return await asyncio.to_thread(direct_plan.plan.prepare_batch, descriptors, options)


def _apply_native_batch_outcome(acc: _ParquetSliceAccumulator, native_outcome) -> None:
    """Record one Rust batch outcome and mark EOF false when budget truncation stops the slice."""
    if native_outcome[0] == "error":
        _raise_direct_stage_error(native_outcome[1], native_outcome[2], acc.seen_rows + native_outcome[3] + 1)
    _, batch_taken, batch_bytes, stopped_by_budget, hash_bytes, bucket_entries = native_outcome
    acc.taken_rows += batch_taken
    acc.taken_bytes += batch_bytes
    acc.seen_rows += batch_taken
    if hash_bytes:
        acc.valid_hash_chunks.append(hash_bytes)
    for bucket_index, bucket_rows, copy_body, pack_id_offsets in bucket_entries:
        acc.binary_chunks_by_bucket[bucket_index].append((copy_body, pack_id_offsets, bucket_rows))
    if stopped_by_budget:
        acc.has_reached_eof = False


async def _stage_parquet_work_batch(
    context,
    policy,
    budget: DirectSliceBudget,
    fields: tuple[Field, ...],
    direct_plan: DirectStagePlan | None,
    work_batch: pa.RecordBatch,
    acc: _ParquetSliceAccumulator,
) -> None:
    """Stage one post-prefix Arrow RecordBatch via Rust or Python fallback."""
    record_kind_code = 0 if context.stream.record_kind == "root" else 1
    native_outcome = await _try_native_record_batch(
        context,
        policy,
        budget,
        direct_plan,
        work_batch,
        record_kind_code=record_kind_code,
        taken_rows=acc.taken_rows,
        taken_bytes=acc.taken_bytes,
    )
    if native_outcome is not None:
        _apply_native_batch_outcome(acc, native_outcome)
        return
    if acc.encoder is None:
        acc.encoder = _build_direct_encoder(context, fields, budget.bucket_count)
    previous_taken = acc.taken_rows
    acc.taken_rows, acc.taken_bytes, stopped_by_budget = _stage_python_record_batch(
        acc.encoder,
        work_batch,
        budget,
        taken_rows=acc.taken_rows,
        taken_bytes=acc.taken_bytes,
        rows_by_bucket=acc.rows_by_bucket,
        valid_hashes=acc.valid_hashes,
    )
    acc.seen_rows += acc.taken_rows - previous_taken
    if stopped_by_budget:
        acc.has_reached_eof = False


async def _handle_parquet_batch(
    context,
    policy,
    budget: DirectSliceBudget,
    fields: tuple[Field, ...],
    direct_plan: DirectStagePlan | None,
    record_batch: pa.RecordBatch,
    logical_bytes: int,
    acc: _ParquetSliceAccumulator,
) -> None:
    """Apply cursor skipping, budget checks, and staging to one decoded Arrow RecordBatch."""
    if logical_bytes > policy.maximum_part_arrow_bytes:
        raise CandidateRunnerError("decoded part Arrow bytes exceed the admitted replay limit")
    if acc.seen_rows + record_batch.num_rows <= budget.part_row_ordinal:
        acc.seen_rows += record_batch.num_rows
        return
    work_batch = record_batch
    if acc.seen_rows < budget.part_row_ordinal:
        skip_rows = budget.part_row_ordinal - acc.seen_rows
        work_batch = record_batch.slice(skip_rows)
        acc.seen_rows += skip_rows
    if acc.taken_rows and (
        acc.taken_rows >= budget.max_rows
        or acc.taken_bytes >= budget.max_bytes
        or time.monotonic() >= budget.soft_deadline
    ):
        acc.has_reached_eof = False
        return
    if time.monotonic() >= budget.deadline:
        raise LeaseAuthorityLost("SOURCE read deadline elapsed")
    await asyncio.sleep(0)
    await _stage_parquet_work_batch(context, policy, budget, fields, direct_plan, work_batch, acc)


async def _prepare_parquet_direct_slice(
    context,
    part,
    policy,
    budget: DirectSliceBudget,
    fields: tuple[Field, ...],
    direct_plan: DirectStagePlan | None,
) -> PreparedDirectPartSlice:
    """Decode a sealed Parquet part in one pass, encoding via Rust or Python fallback."""
    if budget.part_row_ordinal > part.record_count or budget.source_ordinal < budget.part_row_ordinal:
        raise CandidateRunnerError("SOURCE cursor exceeds the retained part")
    if part.arrow_byte_count > policy.maximum_part_arrow_bytes:
        raise CandidateRunnerError("decoded part Arrow bytes exceed the admitted replay limit")
    acc = _ParquetSliceAccumulator()
    batch_iter = _iter_direct_parquet_batches(part, context.stream, policy)
    try:
        for event_kind, first_payload, second_payload, *extra in batch_iter:
            if event_kind == "header":
                _validate_open_parquet_schema(first_payload, second_payload, fields)
                if extra[0] != part.record_count:
                    raise CandidateRunnerError("decoded part record count differs from the sealed receipt")
                continue
            await _handle_parquet_batch(
                context, policy, budget, fields, direct_plan, first_payload, second_payload, acc
            )
            if not acc.has_reached_eof:
                break
        if acc.has_reached_eof and acc.seen_rows != part.record_count:
            raise CandidateRunnerError("decoded part record count differs from the sealed receipt")
    finally:
        batch_iter.close()
    return PreparedDirectPartSlice(
        part.ordinal,
        budget.part_row_ordinal,
        acc.taken_rows,
        budget.source_ordinal,
        acc.has_reached_eof,
        budget.is_final_part,
        tuple(acc.valid_hashes),
        dict(acc.rows_by_bucket),
        byte_count=acc.taken_bytes,
        valid_payload_hash_chunks=tuple(acc.valid_hash_chunks),
        binary_chunks_by_bucket=dict(acc.binary_chunks_by_bucket),
    )


async def _prepare_record_direct_slice(
    context,
    part,
    policy,
    budget: DirectSliceBudget,
    fields: tuple[Field, ...],
) -> PreparedDirectPartSlice:
    """Decode a non-Parquet sealed part row by row into per-bucket COPY tuples."""
    verify_direct_part(
        context, part, policy, part_row_ordinal=budget.part_row_ordinal, source_ordinal=budget.source_ordinal
    )
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


async def prepare_direct_part_slice(
    context,
    part,
    policy,
    budget: DirectSliceBudget,
    *,
    direct_plan: DirectStagePlan | None | object = _UNSET_DIRECT_PLAN,
) -> PreparedDirectPartSlice:
    """Decode and validate one bounded part slice into per-bucket COPY buffers or tuples."""
    fields = _stream_fields(context.request.definition, context.stream)
    if context.stream.format == "parquet":
        resolved_plan = compile_direct_plan(context) if direct_plan is _UNSET_DIRECT_PLAN else direct_plan
        return await _prepare_parquet_direct_slice(context, part, policy, budget, fields, resolved_plan)
    return await _prepare_record_direct_slice(context, part, policy, budget, fields)


def merge_direct_slices(slices: list[PreparedDirectPartSlice]) -> PreparedDirectBatch:
    """Combine sequential slices into one bounded commit batch."""
    merged_by_bucket: dict[int, list[tuple[object, ...]]] = defaultdict(list)
    merged_binary_by_bucket: dict[int, list[tuple[bytes, bytes, int]]] = defaultdict(list)
    valid_hashes: list[bytes] = []
    valid_hash_chunks: list[bytes] = []
    total_rows = 0
    for part_slice in slices:
        total_rows += part_slice.row_count
        valid_hashes.extend(part_slice.valid_payload_hashes)
        valid_hash_chunks.extend(part_slice.valid_payload_hash_chunks)
        for bucket_idx, bucket_rows in part_slice.rows_by_bucket.items():
            merged_by_bucket[bucket_idx].extend(bucket_rows)
        for bucket_idx, binary_chunks in part_slice.binary_chunks_by_bucket.items():
            merged_binary_by_bucket[bucket_idx].extend(binary_chunks)
    return PreparedDirectBatch(
        slices=tuple(slices),
        total_rows=total_rows,
        valid_payload_hashes=tuple(valid_hashes),
        rows_by_bucket=dict(merged_by_bucket),
        valid_payload_hash_chunks=tuple(valid_hash_chunks),
        binary_chunks_by_bucket=dict(merged_binary_by_bucket),
    )


def _compute_direct_pack_digest(label: str, batch: PreparedDirectBatch) -> tuple[int, bytes]:
    """Compute pack record count and deterministic SHA-256 across native and fallback hashes."""
    if batch.valid_payload_hash_chunks and not batch.valid_payload_hashes:
        native_hash_fn = native_pack_hash()
        record_count = sum(len(hash_chunk) for hash_chunk in batch.valid_payload_hash_chunks) // 32
        if native_hash_fn is not None:
            return record_count, native_hash_fn(label, batch.valid_payload_hash_chunks)
    combined_hashes = list(batch.valid_payload_hashes)
    for hash_chunk in batch.valid_payload_hash_chunks:
        combined_hashes.extend(hash_chunk[offset : offset + 32] for offset in range(0, len(hash_chunk), 32))
    return len(combined_hashes), pack_hash(label, combined_hashes)


async def _refresh_copy_timeout_if_due(session, last_timeout_refresh: float) -> float:
    """Enforce the fenced deadline before each bucket COPY without redundant SET LOCAL calls."""
    window = session.info[staging._WINDOW]
    now = time.monotonic()
    if int((window.deadline - now) * 1_000) < 4:
        raise LeaseAuthorityLost("build page deadline elapsed")
    if now - last_timeout_refresh >= _COPY_TIMEOUT_REFRESH_SECONDS:
        await staging._prepare_statement(session)
        return time.monotonic()
    return last_timeout_refresh


@dataclass(frozen=True)
class _DirectPackCopyTarget:
    family_id: int
    schema_name: str
    pack_id: int
    stamp: object


async def _copy_direct_pack_row(
    driver_conn,
    build,
    stream_row: CustomImportBuildStream,
    context,
    batch: PreparedDirectBatch,
    pack_target: _DirectPackCopyTarget,
) -> None:
    """Copy the single batch pack metadata row into the candidate snapshot schema."""
    label = "root" if context.stream.record_kind == "root" else context.stream.child_collection
    pack_record_count, pack_digest = _compute_direct_pack_digest(label, batch)
    pack_table = checked_binding(
        pack_target.family_id, context.request.definition, context.registry.child_collection_slots
    ).tables_by_role["pack"]
    pack_by_col = {
        "pack_id": pack_target.pack_id,
        "execution_id": build.execution_id,
        "dataset_id": build.dataset_id,
        "definition_revision_id": build.definition_revision_id,
        "schema_revision_id": build.schema_revision_id,
        "stream_slot": context.stream_slot,
        "pack_ordinal": stream_row.next_pack_ordinal,
        "capture_bundle_id": build.capture_bundle_id,
        "record_count": pack_record_count,
        "pack_sha256": pack_digest,
        "producing_fence": build.producing_fence,
        "producing_token_sha256": bytes(producer_token_sha256(context.request)),
        "created_at": pack_target.stamp,
    }
    pack_cols = copy_columns(pack_table)
    pack_cells = tuple(pack_by_col[col_name] for col_name in pack_cols)
    await driver_conn.copy_records_to_table(
        pack_table.name, schema_name=pack_target.schema_name, columns=pack_cols, records=(pack_cells,)
    )


async def _copy_direct_bucket_partitions(
    session,
    driver_conn,
    batch: PreparedDirectBatch,
    *,
    schema_name: str,
    pack_id: int,
) -> None:
    """Stream prepared binary chunks or Python fallback rows into unlogged bucket tables."""
    finalize_copy_fn = native_finalize_direct_stage_copy()
    last_timeout_refresh = time.monotonic()
    bucket_indices = sorted(set(batch.binary_chunks_by_bucket) | set(batch.rows_by_bucket))
    for bucket_idx in bucket_indices:
        bucket_name = direct_bucket_table_name(bucket_idx)
        binary_chunks = batch.binary_chunks_by_bucket.get(bucket_idx)
        if binary_chunks:
            if finalize_copy_fn is None:
                raise FamilyBlockUnavailable("direct stage native binary COPY finalizer is unavailable")
            last_timeout_refresh = await _refresh_copy_timeout_if_due(session, last_timeout_refresh)
            bucket_rows = sum(entry[2] for entry in binary_chunks)
            copy_bytes = finalize_copy_fn([(entry[0], entry[1]) for entry in binary_chunks], pack_id)
            with io.BytesIO(copy_bytes) as copy_source:
                status = await driver_conn.copy_to_table(
                    bucket_name,
                    schema_name=schema_name,
                    columns=DIRECT_BUCKET_COLUMNS,
                    format="binary",
                    source=copy_source,
                )
                if status != f"COPY {bucket_rows}" or copy_source.tell() != len(copy_bytes):
                    raise FamilyBlockUnavailable("direct stage COPY count differs from prepared rows")
        staged_rows = batch.rows_by_bucket.get(bucket_idx)
        if staged_rows:
            last_timeout_refresh = await _refresh_copy_timeout_if_due(session, last_timeout_refresh)
            filled_rows = tuple((*staged_row[:3], pack_id, *staged_row[4:]) for staged_row in staged_rows)
            status = await driver_conn.copy_records_to_table(
                bucket_name,
                schema_name=schema_name,
                columns=DIRECT_BUCKET_COLUMNS,
                records=filled_rows,
            )
            if status != f"COPY {len(filled_rows)}":
                raise FamilyBlockUnavailable("direct stage COPY count differs from prepared rows")


async def _copy_direct_pack_and_buckets(
    session,
    build,
    stream_row: CustomImportBuildStream,
    context,
    batch: PreparedDirectBatch,
    family_id: int,
) -> None:
    """Insert one pack record and native binary COPY prepared rows into unlogged bucket tables."""
    schema_name = snapshot_schema(family_id)
    await staging._prepare_statement(session)
    pack_id_raw, stamp = (await session.execute(select(_PACK_IDS.next_value(), func.clock_timestamp()))).one()
    pack_id = int(pack_id_raw)
    connection = await session.connection()
    raw_conn = await connection.get_raw_connection()
    driver_conn = raw_conn.driver_connection
    await _copy_direct_pack_row(
        driver_conn,
        build,
        stream_row,
        context,
        batch,
        _DirectPackCopyTarget(family_id, schema_name, pack_id, stamp),
    )
    await _copy_direct_bucket_partitions(session, driver_conn, batch, schema_name=schema_name, pack_id=pack_id)
    stream_row.next_pack_ordinal += 1


async def _verify_direct_batch_parts(
    session,
    build,
    stream_row: CustomImportBuildStream,
    stream_slot: int,
    slices: tuple[PreparedDirectPartSlice, ...],
) -> None:
    """Validate contiguous slice boundaries and prove every earlier closed part in one query."""
    expected_part = stream_row.next_part_ordinal
    expected_row = stream_row.next_part_row_ordinal
    expected_source = stream_row.next_source_ordinal
    for slice_index, part_slice in enumerate(slices):
        if (
            part_slice.part_ordinal != expected_part
            or part_slice.first_row != expected_row
            or part_slice.first_source != expected_source
        ):
            raise CandidateRunnerError("direct SOURCE batch slices are not contiguous with the durable cursor")
        if slice_index + 1 < len(slices):
            if not part_slice.has_reached_eof:
                raise CandidateRunnerError("direct SOURCE batch earlier slice did not reach part EOF")
            expected_part += 1
            expected_row = 0
        expected_source += part_slice.row_count
    if len(slices) <= 1:
        return
    first_part = slices[0].part_ordinal
    last_part = slices[-1].part_ordinal
    await staging._prepare_statement(session)
    part_rows = (
        await session.execute(
            select(
                CustomImportCaptureParquetPart.part_ordinal,
                CustomImportCaptureParquetPart.record_count,
            ).where(
                CustomImportCaptureParquetPart.capture_bundle_id == build.capture_bundle_id,
                CustomImportCaptureParquetPart.stream_slot == stream_slot,
                CustomImportCaptureParquetPart.part_ordinal >= first_part,
                CustomImportCaptureParquetPart.part_ordinal <= last_part,
            )
        )
    ).all()
    records_by_ordinal = {int(ordinal): int(count) for ordinal, count in part_rows}
    if len(records_by_ordinal) != len(slices):
        raise CandidateRunnerError("custom_import_build_incomplete")
    for slice_index, part_slice in enumerate(slices):
        expected_end = part_slice.first_row + part_slice.row_count
        stored_count = records_by_ordinal.get(part_slice.part_ordinal)
        if stored_count is None:
            raise CandidateRunnerError("custom_import_build_incomplete")
        if slice_index + 1 < len(slices) and expected_end != stored_count:
            raise CandidateRunnerError("custom_import_build_incomplete")
        if slice_index + 1 == len(slices) and expected_end > stored_count:
            raise CandidateRunnerError("custom_import_build_incomplete")


async def commit_direct_source_batch(
    session,
    build,
    stream_row: CustomImportBuildStream,
    context,
    batch: PreparedDirectBatch,
    *,
    family_id: int,
) -> int:
    """Write pack, native binary COPY into bucket partitions, and advance stream cursor once."""
    if build.phase != "source":
        raise CandidateRunnerError("source build is already frozen")
    if not batch.slices:
        return 0
    await _verify_direct_batch_parts(session, build, stream_row, context.stream_slot, batch.slices)
    if batch.total_rows > 0:
        await _copy_direct_pack_and_buckets(session, build, stream_row, context, batch, family_id)
    last_slice = batch.slices[-1]
    stream_row.next_part_ordinal = last_slice.part_ordinal
    stream_row.next_part_row_ordinal = last_slice.first_row + last_slice.row_count
    if batch.total_rows > 0:
        stream_row.next_source_ordinal += batch.total_rows
        build.source_occurrence_count += batch.total_rows
    await staging._flush_page(session)
    if last_slice.has_reached_eof:
        await staging._call(
            session,
            "finish_custom_import_build_source_part",
            (
                ("bigint", build.build_id),
                ("smallint", context.stream_slot),
                ("integer", last_slice.part_ordinal),
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


_LOCAL_PREFETCH_MAX_PARTS = 64
_LOCAL_PREFETCH_MAX_COMPRESSED_BYTES = 16 * 1024 * 1024
_LOCAL_PREFETCH_MAX_ARROW_BYTES = 64 * 1024 * 1024


async def _load_local_stream_cursor(session_factory, request, build_id: int, slot: int):
    async with staging._page_session(session_factory, request, build_id) as (session, _):
        await staging._prepare_statement(session)
        stream_row = (
            await session.scalars(
                select(CustomImportBuildStream)
                .where(CustomImportBuildStream.build_id == build_id, CustomImportBuildStream.stream_slot == slot)
                .execution_options(populate_existing=True)
            )
        ).one()
        return (
            (stream_row.next_part_ordinal, stream_row.next_part_row_ordinal, stream_row.next_source_ordinal),
            stream_row.replay_verified_at is not None,
        )


@dataclass
class _LocalDirectReplayState:
    """Stream context and prefetch buffer for coalesced local direct replay."""

    session_factory: Any
    stream_context: Any
    policy: Any
    direct_plan: Any
    identity_by_field: dict[str, int]
    bucket_count: int
    headers: Any
    pending_parts: deque = field(default_factory=deque)


async def _fetch_local_part_window(state: _LocalDirectReplayState, start_part_ordinal: int, remaining_rows: int):
    from process.custom_import.capture_store import (
        CursorPartWindowLimits,
        CursorPartWindowRequest,
        load_segmented_cursor_part_window,
    )

    request = state.stream_context.request
    window_request = CursorPartWindowRequest(
        stream_slot=state.stream_context.stream_slot,
        start_part_ordinal=start_part_ordinal,
        limits=CursorPartWindowLimits(
            max_parts=_LOCAL_PREFETCH_MAX_PARTS,
            max_rows=max(1, remaining_rows),
            max_compressed_bytes=_LOCAL_PREFETCH_MAX_COMPRESSED_BYTES,
            max_arrow_bytes=_LOCAL_PREFETCH_MAX_ARROW_BYTES,
        ),
        **state.identity_by_field,
    )
    async with state.session_factory() as read_session:
        await staging._set_timeout(read_session, request.statement_timeout_ms)
        return await load_segmented_cursor_part_window(read_session, window_request, headers=state.headers)


async def _next_local_part(state: _LocalDirectReplayState, part_ordinal: int, remaining_rows: int):
    while state.pending_parts and state.pending_parts[0].ordinal < part_ordinal:
        state.pending_parts.popleft()
    if not state.pending_parts or state.pending_parts[0].ordinal != part_ordinal:
        state.pending_parts.clear()
        part_window, state.headers = await _fetch_local_part_window(state, part_ordinal, remaining_rows)
        state.pending_parts.extend(part_window)
    return state.pending_parts.popleft()


async def _verify_local_stream_metadata(state: _LocalDirectReplayState, deadline: float):
    from process.custom_import.capture_store import verify_segmented_stream_metadata

    async with state.session_factory() as read_session:
        await staging._set_timeout(read_session, state.stream_context.request.statement_timeout_ms)
        await verify_segmented_stream_metadata(
            read_session,
            stream_slot=state.stream_context.stream_slot,
            deadline=deadline,
            **state.identity_by_field,
        )


async def _prepare_local_direct_batch(state: _LocalDirectReplayState, cursor: tuple[int, int, int]):
    """Coalesce contiguous retained parts into one bounded direct-stage write batch."""
    from process.custom_import.bulk_page_codec import MAX_BATCH_BYTES, MAX_BATCH_ROWS

    request = state.stream_context.request
    started = time.monotonic()
    deadline = started + request.lease_seconds
    soft_deadline = started + request.lease_seconds / 2
    cur_part, cur_row, cur_source = cursor
    slices: list[PreparedDirectPartSlice] = []
    row_count = byte_count = 0

    while len(slices) < _LOCAL_PREFETCH_MAX_PARTS and (
        not slices
        or (
            row_count + request.page_row_limit <= MAX_BATCH_ROWS
            and byte_count + request.page_byte_limit <= MAX_BATCH_BYTES
        )
    ):
        if slices and time.monotonic() >= soft_deadline:
            break
        part = await _next_local_part(state, cur_part, MAX_BATCH_ROWS - row_count)
        is_final = part.ordinal == state.headers.part_count
        budget = DirectSliceBudget(
            part_row_ordinal=cur_row,
            source_ordinal=cur_source,
            bucket_count=state.bucket_count,
            max_rows=MAX_BATCH_ROWS - row_count,
            max_bytes=MAX_BATCH_BYTES - byte_count,
            deadline=deadline,
            soft_deadline=soft_deadline,
            is_final_part=is_final,
        )
        part_slice = await prepare_direct_part_slice(
            state.stream_context, part, state.policy, budget, direct_plan=state.direct_plan
        )
        slices.append(part_slice)
        row_count += part_slice.row_count
        byte_count += part_slice.byte_count
        if not part_slice.has_reached_eof or is_final:
            if is_final and part_slice.has_reached_eof:
                state.pending_parts.clear()
                await _verify_local_stream_metadata(state, deadline)
            break
        cur_part += 1
        cur_row = 0
        cur_source += part_slice.row_count
    return merge_direct_slices(slices)


async def _replay_direct_stream(
    session_factory,
    stream_context,
    bundle_id: int,
    policy,
    direct_info: tuple[int, int],
) -> None:
    """Replay one stream in coalesced 64-part batches into bucketed direct stage partitions."""
    from process.custom_import.capture_store import load_segmented_cursor_headers

    family_id, bucket_count = direct_info
    request = stream_context.request
    slot = stream_context.stream_slot
    cursor, is_verified = await _load_local_stream_cursor(session_factory, request, stream_context.build_id, slot)
    if is_verified:
        return
    identity_by_field = dict(
        capture_bundle_id=bundle_id,
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
    )
    async with session_factory() as read_session:
        await staging._set_timeout(read_session, request.statement_timeout_ms)
        headers = await load_segmented_cursor_headers(read_session, stream_slot=slot, **identity_by_field)
    state = _LocalDirectReplayState(
        session_factory=session_factory,
        stream_context=stream_context,
        policy=policy,
        direct_plan=compile_direct_plan(stream_context),
        identity_by_field=identity_by_field,
        bucket_count=bucket_count,
        headers=headers,
    )
    while not is_verified:
        direct_batch = await _prepare_local_direct_batch(state, cursor)
        is_committed, actual_cursor, is_verified = await _commit_direct_part_slice(
            session_factory,
            request,
            stream_context.build_id,
            slot,
            stream_context,
            direct_batch,
            family_id,
            cursor,
        )
        if not is_committed:
            state.pending_parts.clear()
            if not is_verified and (
                actual_cursor[0] < cursor[0] or (actual_cursor[0] == cursor[0] and actual_cursor[1] < cursor[1])
            ):
                raise CandidateRunnerError("direct SOURCE durable cursor moved behind replay")
        cursor = actual_cursor


async def replay_direct_source(session_factory, request, registry, build_id, bundle_id, policy, cursors, direct_info):
    """Stream sealed parts directly into unlogged bucketed stage partitions."""
    del cursors
    for stream in request.definition.source_streams:
        stream_context = staging._StreamContext(request, registry, build_id, stream)
        await _replay_direct_stream(session_factory, stream_context, bundle_id, policy, direct_info)
    async with staging._page_session(session_factory, request, build_id) as (session, _):
        await staging._call(session, "freeze_custom_import_build_source", (("bigint", build_id),))
