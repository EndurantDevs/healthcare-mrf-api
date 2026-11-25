# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Per-bucket O(N) admission and compact v2 encoding for direct builds."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from types import SimpleNamespace

from sqlalchemy import BigInteger, LargeBinary, Sequence, Text, any_, bindparam, column, func, literal, select, text
from sqlalchemy.dialects.postgresql import ARRAY, dialect, insert

from db.models.custom_import import CustomImportEntityBinding, CustomImportRootRecord
from process.custom_import import build_graph as graph
from process.custom_import import build_source as staging
from process.custom_import import materialization
from process.custom_import.bulk_page_codec import MAX_BATCH_ROWS
from process.custom_import.continuation_runtime import producer_token_sha256
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import has_child_membership
from process.custom_import.family_block_binding import FamilyBlockBinding, FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockLimits, FamilyBlockResourceLimit
from process.custom_import.family_block_direct_stage import (
    DIRECT_COMMITMENTS_TABLE,
    DIRECT_PROGRESS_TABLE,
    DIRECT_REJECTED_TABLE,
    direct_bucket_table_name,
)
from process.custom_import.family_block_layout import copy_columns
from process.custom_import.family_block_query_input import query_input_table
from process.custom_import.family_block_records import FamilyRecord
from process.custom_import.family_block_scalar_layout import pack_query_scalars, query_scalar_fields
from process.custom_import.family_block_v2_codec import (
    compiled_serializer,
    encode_family_v2,
    unpack_staged_values,
)
from process.custom_import.family_block_write import (
    CONTEXT_COPY_COLUMNS,
    BlockCopyBuffer,
    _copy_batch_rows,
)
from process.custom_import.runner_codec import fields_by_collection, root_key_contract_hash
from process.custom_import.runner_graph import entity_value_digest
from process.custom_import.storage_layout import snapshot_schema, snapshot_tables

_DIALECT = dialect()
_ROOT_TABLE = CustomImportRootRecord.__table__
_ENTITY_TABLE = CustomImportEntityBinding.__table__
_FAMILY_SEQ = Sequence("custom_import_family_revision_family_revision_id_seq", schema=_ROOT_TABLE.schema)
_ROOT_REV_SEQ = Sequence("custom_import_root_revision_root_revision_id_seq", schema=_ROOT_TABLE.schema)
_CHILD_REV_SEQ = Sequence("custom_import_child_revision_child_revision_id_seq", schema=_ROOT_TABLE.schema)
_REJECTION_SEQ = Sequence("custom_import_rejection_rejection_id_seq", schema=_ROOT_TABLE.schema)

_BUCKET_BATCH_ROW_BUDGET = 60_000
_BUCKET_DIRECT_SCAN_LIMIT = 32_768
_BUCKET_SLICE_KEY_LIMIT = 4_096
_COMMITMENT_COLUMNS = (
    "family_revision_id",
    "root_record_id",
    "logical_key_sha256",
    "key_contract_sha256",
    "canonical_logical_key",
    "entity_binding_id",
    "family_sha256",
    "child_count",
    "root_payload_sha256",
    "root_source_ordinal",
    "retained",
)
_REJECTED_ROOT_COLUMNS = ("raw_key_sha256", "typed_key_sha256", "canonical_typed_key")
_CANDIDATE_CODES = frozenset({"root_not_object", "root_key_missing", "child_not_object", "orphan_child"})
_UNKEYED_ROOT_CODES = frozenset({"root_not_object", "root_key_missing"})


@dataclass(frozen=True)
class DirectBucketOutcome:
    """Counters emitted by one completed bucket transaction."""

    buckets_processed: int
    staged_rows_processed: int
    accepted_families: int
    source_rejections: int
    candidate_errors: int
    rejections_inserted: int
    child_count: int
    root_scalar_count: int
    child_scalar_count: int
    children_by_slot: dict[int, int]


@dataclass(frozen=True)
class _AcceptedFamilyBundle:
    raw_hash: bytes
    typed_hash: bytes
    typed_canonical: str
    root_entry: dict[str, object]
    root_values_by_field: dict[str, object]
    children_by_slot: dict[int, list[tuple[dict[str, object], dict[str, object]]]]


@dataclass(frozen=True)
class _RejectionEmit:
    pack_id: int
    root_key_sha256: bytes | None
    canonical_root_key: str | None
    collection_slot: int | None
    source_ordinal: int
    code: str
    canonical_evidence: str


@dataclass(frozen=True)
class _AcceptedWriteContext:
    request: staging.SourceBuildRequest
    build: object
    registry: object
    binding: FamilyBlockBinding
    query_collection: str | None
    query_root_fields: tuple
    query_child_fields: tuple
    fields_by_name_map: dict
    serializers_by_name_map: dict
    tables_by_role: dict
    key_contract_hash: bytes
    limits: FamilyBlockLimits


def default_direct_limits() -> FamilyBlockLimits:
    """Bounded encoding limits shared across compact v2 bucket encoding and spot checks."""
    return FamilyBlockLimits(max_spool_bytes=512 * 1024 * 1024, max_record_bytes=268_435_456)


async def _allocate_sequence_ids(session, sequence: Sequence, count_needed: int) -> list[int]:
    if count_needed <= 0:
        return []
    await staging._prepare_statement(session)
    return list(
        (await session.scalars(select(sequence.next_value()).select_from(func.generate_series(1, count_needed)))).all()
    )


async def _intern_root_records(
    session,
    dataset_id: int,
    key_contract_sha256: bytes,
    keys_by_hash: dict[bytes, str],
) -> dict[bytes, int]:
    if not keys_by_hash:
        return {}
    hashes = tuple(keys_by_hash)
    canonicals = tuple(keys_by_hash[key_hash] for key_hash in hashes)
    input_rows = (
        func.unnest(
            bindparam("root_hashes", hashes, type_=ARRAY(LargeBinary)),
            bindparam("root_keys", canonicals, type_=ARRAY(Text)),
        )
        .table_valued(column("logical_key_sha256", LargeBinary), column("canonical_logical_key", Text))
        .render_derived(name="direct_root_inputs")
    )
    cols = ("dataset_id", "key_contract_sha256", "canonical_logical_key", "logical_key_sha256")
    insert_stmt = insert(_ROOT_TABLE).from_select(
        cols,
        select(
            literal(dataset_id, BigInteger),
            literal(key_contract_sha256, LargeBinary),
            input_rows.c.canonical_logical_key,
            input_rows.c.logical_key_sha256,
        ),
        include_defaults=False,
    )
    await staging._prepare_statement(session)
    await session.execute(insert_stmt.on_conflict_do_nothing(index_elements=cols[:2] + (cols[3],)))
    await staging._prepare_statement(session)
    found_rows = (
        await session.execute(
            select(
                _ROOT_TABLE.c.logical_key_sha256, _ROOT_TABLE.c.root_record_id, _ROOT_TABLE.c.canonical_logical_key
            ).where(
                _ROOT_TABLE.c.dataset_id == dataset_id,
                _ROOT_TABLE.c.key_contract_sha256 == key_contract_sha256,
                _ROOT_TABLE.c.logical_key_sha256
                == any_(bindparam("lookup_root_hashes", hashes, type_=ARRAY(LargeBinary))),
            )
        )
    ).all()
    ids_by_hash: dict[bytes, int] = {}
    for logical_hash, root_id, canonical_key in found_rows:
        hash_bytes = bytes(logical_hash)
        if keys_by_hash.get(hash_bytes) != canonical_key:
            raise FamilyBlockUnavailable("direct root key digest collision")
        ids_by_hash[hash_bytes] = int(root_id)
    if len(ids_by_hash) != len(keys_by_hash):
        raise FamilyBlockUnavailable("direct root record interning is incomplete")
    return ids_by_hash


async def _copy_interned_entities(
    session,
    binding: FamilyBlockBinding,
    dataset_id: int,
    entity_ids: tuple[int, ...],
) -> None:
    target_entity = binding.tables_by_role["entity_binding"]
    entity_cols = copy_columns(target_entity)
    copy_stmt = insert(target_entity).from_select(
        entity_cols,
        select(*(_ENTITY_TABLE.c[col_name] for col_name in entity_cols)).where(
            _ENTITY_TABLE.c.dataset_id == dataset_id,
            _ENTITY_TABLE.c.entity_binding_id
            == any_(bindparam("copy_entity_ids", entity_ids, type_=ARRAY(BigInteger))),
        ),
        include_defaults=False,
    )
    await staging._prepare_statement(session)
    await session.execute(copy_stmt.on_conflict_do_nothing(index_elements=[target_entity.c.entity_binding_id]))


async def _intern_and_copy_entities(
    session,
    binding: FamilyBlockBinding,
    dataset_id: int,
    entity_values: set[str],
) -> dict[str, int]:
    if not entity_values:
        return {}
    ordered_entities = tuple(sorted(entity_values))
    entity_hashes = tuple(entity_value_digest(entity_text) for entity_text in ordered_entities)
    input_rows = (
        func.unnest(
            bindparam("entity_texts", ordered_entities, type_=ARRAY(Text)),
            bindparam("entity_hashes", entity_hashes, type_=ARRAY(LargeBinary)),
        )
        .table_valued(column("canonical_value", Text), column("value_sha256", LargeBinary))
        .render_derived(name="direct_entity_inputs")
    )
    cols = ("dataset_id", "adapter_id", "canonical_value", "value_sha256")
    insert_stmt = insert(_ENTITY_TABLE).from_select(
        cols,
        select(
            literal(dataset_id, BigInteger),
            literal("npi", Text),
            input_rows.c.canonical_value,
            input_rows.c.value_sha256,
        ),
        include_defaults=False,
    )
    await staging._prepare_statement(session)
    await session.execute(insert_stmt.on_conflict_do_nothing(index_elements=cols[:3]))
    await staging._prepare_statement(session)
    found_rows = (
        await session.execute(
            select(_ENTITY_TABLE.c.canonical_value, _ENTITY_TABLE.c.entity_binding_id).where(
                _ENTITY_TABLE.c.dataset_id == dataset_id,
                _ENTITY_TABLE.c.adapter_id == "npi",
                _ENTITY_TABLE.c.canonical_value
                == any_(bindparam("lookup_entities", ordered_entities, type_=ARRAY(Text))),
            )
        )
    ).all()
    ids_by_entity = {canonical_text: int(entity_id) for canonical_text, entity_id in found_rows}
    if len(ids_by_entity) != len(ordered_entities):
        raise FamilyBlockUnavailable("direct entity binding interning is incomplete")
    await _copy_interned_entities(session, binding, dataset_id, tuple(ids_by_entity.values()))
    return ids_by_entity


def _emit_from_staged(staged: dict[str, object], code: str) -> _RejectionEmit:
    uses_initial = staged["initial_code"] == code and staged["rejection_evidence"] is not None
    typed_hash = staged["typed_key_sha256"]
    return _RejectionEmit(
        pack_id=int(staged["pack_id"]),
        root_key_sha256=(
            (None if staged["rejection_hash"] is None else bytes(staged["rejection_hash"]))
            if uses_initial
            else (None if typed_hash is None else bytes(typed_hash))
        ),
        canonical_root_key=staged["rejection_key"] if uses_initial else staged["typed_key_canonical"],
        collection_slot=None if staged["record_kind"] == 0 else int(staged["collection_slot"]),
        source_ordinal=int(staged["source_ordinal"]),
        code=code,
        canonical_evidence=staged["rejection_evidence"] if uses_initial else "{}",
    )


def _partition_bucket_rows(staged_mappings: list[dict[str, object]]):
    unkeyed_rows: list[dict[str, object]] = []
    roots_by_raw: dict[bytes, list[dict[str, object]]] = defaultdict(list)
    children_by_raw: dict[bytes, list[dict[str, object]]] = defaultdict(list)
    raw_text_by_hash: dict[bytes, str] = {}
    typed_counts_by_hash: dict[bytes, int] = defaultdict(int)
    for staged in staged_mappings:
        raw_hash = staged["raw_key_sha256"]
        if raw_hash is None:
            unkeyed_rows.append(staged)
            continue
        raw_bytes = bytes(raw_hash)
        raw_text = staged["raw_key_canonical"]
        existing_raw = raw_text_by_hash.get(raw_bytes)
        if existing_raw is not None and existing_raw != raw_text:
            raise FamilyBlockUnavailable("raw key digest collision")
        raw_text_by_hash[raw_bytes] = raw_text
        if staged["record_kind"] == 0:
            roots_by_raw[raw_bytes].append(staged)
            typed_hash = staged["typed_key_sha256"]
            if typed_hash is not None:
                typed_counts_by_hash[bytes(typed_hash)] += 1
        else:
            children_by_raw[raw_bytes].append(staged)
    return unkeyed_rows, roots_by_raw, children_by_raw, typed_counts_by_hash


def _collect_unkeyed_and_orphan_rejections(
    unkeyed_rows: list[dict[str, object]],
    roots_by_raw: dict[bytes, list[dict[str, object]]],
    children_by_raw: dict[bytes, list[dict[str, object]]],
) -> tuple[list[_RejectionEmit], int, int]:
    rejection_emits: list[_RejectionEmit] = []
    source_rejections = candidate_errors = 0
    for unkeyed in unkeyed_rows:
        code = unkeyed["initial_code"]
        if code not in _CANDIDATE_CODES:
            raise FamilyBlockUnavailable("unkeyed source row lacks a candidate error code")
        candidate_errors += 1
        if code in _UNKEYED_ROOT_CODES:
            source_rejections += 1
        rejection_emits.append(
            _RejectionEmit(
                pack_id=int(unkeyed["pack_id"]),
                root_key_sha256=None if unkeyed["rejection_hash"] is None else bytes(unkeyed["rejection_hash"]),
                canonical_root_key=unkeyed["rejection_key"],
                collection_slot=None if unkeyed["record_kind"] == 0 else int(unkeyed["collection_slot"]),
                source_ordinal=int(unkeyed["source_ordinal"]),
                code=code,
                canonical_evidence=unkeyed["rejection_evidence"] or "{}",
            )
        )
    for raw_bytes, orphan_children in children_by_raw.items():
        if raw_bytes in roots_by_raw:
            continue
        for child_entry in orphan_children:
            initial_code = child_entry["initial_code"]
            effective_code = "child_not_object" if initial_code == "child_not_object" else "orphan_child"
            candidate_errors += 1
            rejection_emits.append(
                _RejectionEmit(
                    pack_id=int(child_entry["pack_id"]),
                    root_key_sha256=None
                    if child_entry["typed_key_sha256"] is None
                    else bytes(child_entry["typed_key_sha256"]),
                    canonical_root_key=child_entry["typed_key_canonical"],
                    collection_slot=int(child_entry["collection_slot"]),
                    source_ordinal=int(child_entry["source_ordinal"]),
                    code=effective_code,
                    canonical_evidence=child_entry["rejection_evidence"] if effective_code == initial_code else "{}",
                )
            )
    return rejection_emits, source_rejections, candidate_errors


def _admit_family_children(
    family_children: list[dict[str, object]],
    collapse_by_slot_map: dict[int, bool],
) -> tuple[
    dict[tuple[int, bytes], dict[str, object]], set[str], list[_RejectionEmit], list[_RejectionEmit], int | None
]:
    accepted_by_slot_and_key: dict[tuple[int, bytes], dict[str, object]] = {}
    key_text_by_slot_and_hash: dict[tuple[int, bytes], str] = {}
    child_codes: set[str] = set()
    family_rejections: list[_RejectionEmit] = []
    candidate_emits: list[_RejectionEmit] = []
    earliest_child_slot: int | None = None
    for child_entry in family_children:
        initial_code = child_entry["initial_code"]
        if initial_code in ("child_not_object", "orphan_child"):
            candidate_emits.append(_emit_from_staged(child_entry, initial_code))
            continue
        if initial_code is not None:
            child_codes.add(initial_code)
            family_rejections.append(_emit_from_staged(child_entry, initial_code))
            child_stream_slot = int(child_entry["stream_slot"])
            if earliest_child_slot is None or child_stream_slot < earliest_child_slot:
                earliest_child_slot = child_stream_slot
            continue
        slot = int(child_entry["collection_slot"])
        slot_key = (slot, bytes(child_entry["child_key_sha256"]))
        child_key_text = child_entry["child_key_canonical"]
        seen_text = key_text_by_slot_and_hash.get(slot_key)
        if seen_text is not None and seen_text != child_key_text:
            raise FamilyBlockUnavailable("child key digest collision")
        key_text_by_slot_and_hash[slot_key] = child_key_text
        previous = accepted_by_slot_and_key.get(slot_key)
        if previous is None:
            accepted_by_slot_and_key[slot_key] = child_entry
            continue
        is_identical = (
            collapse_by_slot_map.get(slot, False)
            and bytes(previous["payload_sha256"]) == bytes(child_entry["payload_sha256"])
            and bytes(previous["packed_slots"]) == bytes(child_entry["packed_slots"])
        )
        if is_identical:
            if int(child_entry["source_ordinal"]) > int(previous["source_ordinal"]):
                accepted_by_slot_and_key[slot_key] = child_entry
        else:
            child_codes.add("duplicate_child_key")
            family_rejections.append(_emit_from_staged(child_entry, "duplicate_child_key"))
            child_stream_slot = int(child_entry["stream_slot"])
            if earliest_child_slot is None or child_stream_slot < earliest_child_slot:
                earliest_child_slot = child_stream_slot
    return accepted_by_slot_and_key, child_codes, family_rejections, candidate_emits, earliest_child_slot


def _unpack_and_check_membership(
    definition: CustomImportDefinition,
    registry,
    fields_by_slot_map: dict[int, tuple],
    accepted_by_slot_and_key: dict[tuple[int, bytes], dict[str, object]],
) -> tuple[dict[int, list[tuple[dict[str, object], dict[str, object]]]], bool]:
    unpacked_by_slot: dict[int, list[tuple[dict[str, object], dict[str, object]]]] = defaultdict(list)
    for (slot, _child_hash), child_entry in accepted_by_slot_and_key.items():
        child_vals = unpack_staged_values(fields_by_slot_map[slot], bytes(child_entry["packed_slots"]))
        unpacked_by_slot[slot].append((child_entry, child_vals))
    if not definition.child_memberships:
        return unpacked_by_slot, True
    children_by_name = {
        name: tuple(child_vals for _entry, child_vals in unpacked_by_slot.get(slot, ()))
        for name, slot in registry.child_collection_slots.items()
    }
    return unpacked_by_slot, has_child_membership(definition, children_by_name)


def _collect_root_rejections(
    root_entries: list[dict[str, object]], earliest_child_slot: int | None
) -> tuple[set[str], list[_RejectionEmit]]:
    first_root = root_entries[0]
    is_blocked_by_child = earliest_child_slot is not None and earliest_child_slot < int(first_root["stream_slot"])
    family_codes: set[str] = set()
    family_rejections: list[_RejectionEmit] = []
    if first_root["initial_code"] is not None:
        family_codes.add(first_root["initial_code"])
    for root_entry in root_entries:
        if root_entry["initial_code"] is not None:
            family_rejections.append(_emit_from_staged(root_entry, root_entry["initial_code"]))
    if len(root_entries) > 1:
        family_codes.add("duplicate_root_key")
        for dup_root in root_entries:
            if dup_root["initial_code"] is None and not (dup_root is first_root and is_blocked_by_child):
                family_rejections.append(_emit_from_staged(dup_root, "duplicate_root_key"))
    return family_codes, family_rejections


def _admit_single_family(
    definition: CustomImportDefinition,
    registry,
    fields_by_slot_map: dict[int, tuple],
    collapse_by_slot_map: dict[int, bool],
    raw_bytes: bytes,
    root_entries: list[dict[str, object]],
    family_children: list[dict[str, object]],
    dup_typed_hashes: set[bytes],
) -> tuple[_AcceptedFamilyBundle | None, list[_RejectionEmit], int, int]:
    """Admit or reject one keyed root family and its staged children."""
    root_entries.sort(key=lambda entry: int(entry["source_ordinal"]))
    first_root = root_entries[0]
    accepted_by_slot_and_key, child_codes, child_rejections, candidate_emits, earliest_child_slot = (
        _admit_family_children(family_children, collapse_by_slot_map)
    )
    family_codes, family_rejections = _collect_root_rejections(root_entries, earliest_child_slot)
    family_codes.update(child_codes)
    family_rejections.extend(child_rejections)
    unpacked_by_slot: dict[int, list[tuple[dict[str, object], dict[str, object]]]] = {}
    if not family_codes:
        unpacked_by_slot, has_membership = _unpack_and_check_membership(
            definition, registry, fields_by_slot_map, accepted_by_slot_and_key
        )
        if not has_membership:
            family_codes.add("child_membership_missing")
            family_rejections.append(_emit_from_staged(first_root, "child_membership_missing"))
    typed_hash = None if first_root["typed_key_sha256"] is None else bytes(first_root["typed_key_sha256"])
    is_typed_dup = (
        not family_codes and len(root_entries) == 1 and typed_hash is not None and typed_hash in dup_typed_hashes
    )
    if is_typed_dup:
        family_rejections.append(_emit_from_staged(first_root, "duplicate_root_key"))
    if family_codes or is_typed_dup:
        return (
            None,
            [*candidate_emits, *family_rejections],
            len(family_codes) if family_codes else 1,
            len(candidate_emits),
        )
    if typed_hash is None or first_root["packed_slots"] is None:
        raise FamilyBlockUnavailable("accepted root lacks typed identity or payload")
    root_values_by_field = unpack_staged_values(definition.root_fields, bytes(first_root["packed_slots"]))
    bundle = _AcceptedFamilyBundle(
        raw_hash=raw_bytes,
        typed_hash=typed_hash,
        typed_canonical=first_root["typed_key_canonical"],
        root_entry=first_root,
        root_values_by_field=root_values_by_field,
        children_by_slot=dict(unpacked_by_slot),
    )
    return bundle, candidate_emits, 0, len(candidate_emits)


def _bucket_admission_maps(definition: CustomImportDefinition, registry):
    fields_by_slot_map = {
        registry.child_collection_slots[name]: col_fields
        for name, col_fields in fields_by_collection(definition).items()
    }
    collapse_by_slot_map = {
        registry.child_collection_slots[stream.child_collection]: stream.duplicate_policy == "collapse_identical"
        for stream in definition.source_streams
        if stream.child_collection is not None
    }
    return fields_by_slot_map, collapse_by_slot_map


def _admit_bucket_rows(
    definition: CustomImportDefinition,
    registry,
    staged_mappings: list[dict[str, object]],
    *,
    external_dup_typed_hashes: set[bytes] | None = None,
) -> tuple[list[_AcceptedFamilyBundle], list[_RejectionEmit], list[tuple[bytes, bytes | None, str | None]], int, int]:
    """Run exact O(N) family admission on a bounded slice of staged rows in one bucket."""
    fields_by_slot_map, collapse_by_slot_map = _bucket_admission_maps(definition, registry)
    unkeyed_rows, roots_by_raw, children_by_raw, typed_counts_by_hash = _partition_bucket_rows(staged_mappings)
    dup_typed_hashes = (
        external_dup_typed_hashes
        if external_dup_typed_hashes is not None
        else {typed_key for typed_key, count_val in typed_counts_by_hash.items() if count_val > 1}
    )
    rejection_emits, source_rejections, candidate_errors = _collect_unkeyed_and_orphan_rejections(
        unkeyed_rows, roots_by_raw, children_by_raw
    )
    accepted_bundles: list[_AcceptedFamilyBundle] = []
    rejected_root_entries: list[tuple[bytes, bytes | None, str | None]] = []
    for raw_bytes, root_entries in roots_by_raw.items():
        bundle, emits, src_rej, cand_err = _admit_single_family(
            definition,
            registry,
            fields_by_slot_map,
            collapse_by_slot_map,
            raw_bytes,
            root_entries,
            children_by_raw.get(raw_bytes, []),
            dup_typed_hashes,
        )
        rejection_emits.extend(emits)
        source_rejections += src_rej
        candidate_errors += cand_err
        if bundle is not None:
            accepted_bundles.append(bundle)
        else:
            rejected_root_entries.append(
                (
                    raw_bytes,
                    None if root_entries[0]["typed_key_sha256"] is None else bytes(root_entries[0]["typed_key_sha256"]),
                    root_entries[0]["typed_key_canonical"],
                )
            )
    return accepted_bundles, rejection_emits, rejected_root_entries, source_rejections, candidate_errors


async def _copy_extra_table(
    session, schema_name: str, table_name: str, columns: tuple[str, ...], batch_rows: list
) -> None:
    if not batch_rows:
        return
    connection = await session.connection()
    raw_conn = await connection.get_raw_connection()
    await staging._prepare_statement(session)
    status = await raw_conn.driver_connection.copy_records_to_table(
        table_name,
        schema_name=schema_name,
        columns=columns,
        records=batch_rows,
    )
    if status != f"COPY {len(batch_rows)}":
        raise FamilyBlockUnavailable(f"direct COPY count mismatch on {table_name}")


def _scalars_dict(fields, values_by_field: dict[str, object]) -> dict[str, materialization.TypedScalar]:
    return {
        field.field_id: materialization._typed_scalar(field, values_by_field[field.field_id])
        for field in fields
        if field.projection_slot is not None and field.field_id in values_by_field
    }


def _build_sorted_family_children(
    write_ctx: _AcceptedWriteContext,
    bundle: _AcceptedFamilyBundle,
    child_rev_ids,
    children_by_slot: dict[int, int],
) -> tuple[dict[str, list[FamilyRecord]], list[tuple[int, int, bytes, str, FamilyRecord]], int]:
    sorted_children_by_name: dict[str, list[FamilyRecord]] = {}
    query_child_entries: list[tuple[int, int, bytes, str, FamilyRecord]] = []
    family_child_count = 0
    for collection_name, slot in write_ctx.binding.layout.collection_slots.items():
        col_fields = write_ctx.fields_by_name_map[collection_name]
        col_serializer = write_ctx.serializers_by_name_map[collection_name]
        ordered_items = [
            (
                (
                    bytes(child_entry["child_key_sha256"]),
                    child_entry["child_key_canonical"],
                    col_serializer.serialize_payload(child_values),
                ),
                child_entry,
                child_values,
            )
            for child_entry, child_values in bundle.children_by_slot.get(slot, ())
        ]
        ordered_items.sort(key=lambda entry: entry[0])
        built_records: list[FamilyRecord] = []
        for _sort_key, child_entry, child_values in ordered_items:
            child_rev_id = next(child_rev_ids)
            child_record = FamilyRecord(
                revision_id=child_rev_id,
                pack_id=int(child_entry["pack_id"]),
                source_ordinal=int(child_entry["source_ordinal"]),
                values_by_field=child_values,
                scalars=_scalars_dict(col_fields, child_values),
            )
            built_records.append(child_record)
            if collection_name == write_ctx.query_collection:
                query_child_entries.append(
                    (
                        child_rev_id,
                        slot,
                        bytes(child_entry["child_key_sha256"]),
                        child_entry["child_key_canonical"],
                        child_record,
                    )
                )
        sorted_children_by_name[collection_name] = built_records
        if built_records:
            children_by_slot[slot] += len(built_records)
            family_child_count += len(built_records)
    return sorted_children_by_name, query_child_entries, family_child_count


async def _append_copy_role(session, tables_by_role: dict, pending: BlockCopyBuffer, role: str, cells: tuple) -> None:
    if ready := pending.append(role, cells):
        await _copy_batch_rows(session, tables_by_role, ready)


async def _emit_family_contexts(
    session,
    write_ctx: _AcceptedWriteContext,
    pending: BlockCopyBuffer,
    family_identity,
    root_values_by_field: dict[str, object],
    query_child_entries: list[tuple[int, int, bytes, str, FamilyRecord]],
) -> None:
    definition = write_ctx.request.definition
    root_candidate = graph._candidate(definition, family_identity, root_values_by_field)
    for context_by_col in graph._context_values(
        write_ctx.request, write_ctx.registry, write_ctx.build.build_id, root_candidate
    ):
        await _append_copy_role(
            session,
            write_ctx.tables_by_role,
            pending,
            "candidate_context",
            tuple(context_by_col[col_name] for col_name in CONTEXT_COPY_COLUMNS),
        )
    for child_rev_id, slot, child_hash, child_canonical, child_record in query_child_entries:
        await _append_copy_role(
            session,
            write_ctx.tables_by_role,
            pending,
            "query_input",
            (
                child_rev_id,
                family_identity.family_revision_id,
                slot,
                child_hash,
                child_canonical,
                *pack_query_scalars(write_ctx.query_child_fields, child_record.scalars),
            ),
        )
        child_identity = SimpleNamespace(
            child_revision_id=child_rev_id, collection_slot=slot, child_key_sha256=child_hash
        )
        child_candidate = graph._candidate(
            definition, family_identity, root_values_by_field, child_identity, child_record.values_by_field
        )
        for context_by_col in graph._context_values(
            write_ctx.request, write_ctx.registry, write_ctx.build.build_id, child_candidate
        ):
            await _append_copy_role(
                session,
                write_ctx.tables_by_role,
                pending,
                "candidate_context",
                tuple(context_by_col[col_name] for col_name in CONTEXT_COPY_COLUMNS),
            )


def _accepted_family_cells(
    ids_tuple: tuple[int, int, int, int],
    encoded,
    source_ordinal: int,
    query_root_fields: tuple,
    root_scalars_by_field: dict,
) -> tuple[object, ...]:
    family_id, root_rev_id, root_record_id, entity_binding_id = ids_tuple
    manifest = encoded.stored_manifest
    return (
        family_id,
        root_record_id,
        root_rev_id,
        entity_binding_id,
        manifest.family_sha256,
        manifest.storage_sha256,
        manifest.retained,
        manifest.child_count,
        manifest.fragment_count,
        manifest.raw_bytes,
        manifest.stored_bytes,
        manifest.membership_count,
        encoded.root_payload_sha256,
        source_ordinal,
        *pack_query_scalars(query_root_fields, root_scalars_by_field),
    )


async def _emit_accepted_family(
    session,
    write_ctx: _AcceptedWriteContext,
    pending: BlockCopyBuffer,
    ids_tuple: tuple[int, int, int, int],
    bundle: _AcceptedFamilyBundle,
    child_rev_ids,
    children_by_slot: dict[int, int],
) -> tuple[int, int, tuple[object, ...]]:
    """Encode one accepted family into compact v2 COPY buffers and return commitment row."""
    family_id, root_rev_id, root_record_id, entity_binding_id = ids_tuple
    definition = write_ctx.request.definition
    root_scalars_by_field = _scalars_dict(definition.root_fields, bundle.root_values_by_field)
    root_record = FamilyRecord(
        revision_id=root_rev_id,
        pack_id=int(bundle.root_entry["pack_id"]),
        source_ordinal=int(bundle.root_entry["source_ordinal"]),
        values_by_field=bundle.root_values_by_field,
        scalars=root_scalars_by_field,
    )
    sorted_children_by_name, query_child_entries, family_child_count = _build_sorted_family_children(
        write_ctx, bundle, child_rev_ids, children_by_slot
    )
    with encode_family_v2(
        definition,
        root_record,
        sorted_children_by_name,
        None,
        limits=write_ctx.limits,
        retained=False,
        total_child_count=family_child_count,
    ) as encoded:
        manifest = encoded.stored_manifest
        family_cells = _accepted_family_cells(
            ids_tuple, encoded, root_record.source_ordinal, write_ctx.query_root_fields, root_scalars_by_field
        )
        await _append_copy_role(session, write_ctx.tables_by_role, pending, "family", family_cells)
        for directory, frame in encoded.indexed_fragments():
            frag_cells = (family_id, directory[0], directory[1], frame, *directory[2:])
            await _append_copy_role(session, write_ctx.tables_by_role, pending, "fragment", frag_cells)
        family_identity = SimpleNamespace(
            family_revision_id=family_id, entity_binding_id=entity_binding_id, family_sha256=manifest.family_sha256
        )
        await _emit_family_contexts(
            session, write_ctx, pending, family_identity, bundle.root_values_by_field, query_child_entries
        )
        commitment = (
            family_id,
            root_record_id,
            bundle.typed_hash,
            write_ctx.key_contract_hash,
            bundle.typed_canonical,
            entity_binding_id,
            manifest.family_sha256,
            manifest.child_count,
            encoded.root_payload_sha256,
            root_record.source_ordinal,
            False,
        )
        return encoded.root_scalar_count, encoded.child_scalar_count, commitment


def _build_accepted_write_context(
    request: staging.SourceBuildRequest,
    build,
    registry,
    binding: FamilyBlockBinding,
    key_contract_hash: bytes,
    limits: FamilyBlockLimits,
) -> _AcceptedWriteContext:
    definition = request.definition
    query_collection = definition.query.child_collection
    fields_by_name_map = fields_by_collection(definition)
    tables_by_role = dict(binding.tables_by_role)
    if query_collection is not None:
        tables_by_role["query_input"] = query_input_table(binding.family_id, binding.layout)
    tables_by_role["candidate_context"] = snapshot_tables(binding.family_id)["custom_import_build_candidate_context"]
    return _AcceptedWriteContext(
        request=request,
        build=build,
        registry=registry,
        binding=binding,
        query_collection=query_collection,
        query_root_fields=query_scalar_fields(definition),
        query_child_fields=query_scalar_fields(definition, query_collection) if query_collection is not None else (),
        fields_by_name_map=fields_by_name_map,
        serializers_by_name_map={name: compiled_serializer(cols) for name, cols in fields_by_name_map.items()},
        tables_by_role=tables_by_role,
        key_contract_hash=key_contract_hash,
        limits=limits,
    )


@dataclass(frozen=True)
class _AllocatedBundleIds:
    root_ids_by_hash: dict[bytes, int]
    entity_ids_by_text: dict[str, int]
    total_children: int
    family_ids: list[int]
    root_rev_ids: list[int]
    child_rev_ids: object


async def _intern_and_allocate_bundles(
    session,
    definition: CustomImportDefinition,
    build,
    binding: FamilyBlockBinding,
    accepted_bundles: list[_AcceptedFamilyBundle],
    key_contract_hash: bytes,
) -> _AllocatedBundleIds:
    root_ids_by_hash = await _intern_root_records(
        session,
        build.dataset_id,
        key_contract_hash,
        {bundle.typed_hash: bundle.typed_canonical for bundle in accepted_bundles},
    )
    entity_ids_by_text = await _intern_and_copy_entities(
        session,
        binding,
        build.dataset_id,
        {bundle.root_values_by_field[definition.entity_field] for bundle in accepted_bundles},
    )
    total_children = sum(
        sum(len(slot_children) for slot_children in bundle.children_by_slot.values()) for bundle in accepted_bundles
    )
    family_ids = await _allocate_sequence_ids(session, _FAMILY_SEQ, len(accepted_bundles))
    root_rev_ids = await _allocate_sequence_ids(session, _ROOT_REV_SEQ, len(accepted_bundles))
    child_rev_ids = iter(await _allocate_sequence_ids(session, _CHILD_REV_SEQ, total_children))
    return _AllocatedBundleIds(
        root_ids_by_hash, entity_ids_by_text, total_children, family_ids, root_rev_ids, child_rev_ids
    )


async def _write_accepted_bundles(
    session,
    request: staging.SourceBuildRequest,
    build,
    registry,
    binding: FamilyBlockBinding,
    accepted_bundles: list[_AcceptedFamilyBundle],
    *,
    limits: FamilyBlockLimits,
) -> tuple[int, int, int, dict[int, int]]:
    """Intern identities, allocate sequence IDs, and COPY accepted compact v2 families."""
    if not accepted_bundles:
        return 0, 0, 0, {}
    definition = request.definition
    schema_name = snapshot_schema(binding.family_id)
    key_contract_hash = root_key_contract_hash(definition)
    allocated = await _intern_and_allocate_bundles(
        session, definition, build, binding, accepted_bundles, key_contract_hash
    )
    write_ctx = _build_accepted_write_context(request, build, registry, binding, key_contract_hash, limits)
    pending = BlockCopyBuffer()
    commitment_rows: list[tuple[object, ...]] = []
    root_scalar_total = child_scalar_total = 0
    children_by_slot: dict[int, int] = defaultdict(int)
    for idx, bundle in enumerate(accepted_bundles):
        ids_tuple = (
            allocated.family_ids[idx],
            allocated.root_rev_ids[idx],
            allocated.root_ids_by_hash[bundle.typed_hash],
            allocated.entity_ids_by_text[bundle.root_values_by_field[definition.entity_field]],
        )
        root_sc_n, child_sc_n, commitment = await _emit_accepted_family(
            session, write_ctx, pending, ids_tuple, bundle, allocated.child_rev_ids, children_by_slot
        )
        root_scalar_total += root_sc_n
        child_scalar_total += child_sc_n
        commitment_rows.append(commitment)
        if len(commitment_rows) >= MAX_BATCH_ROWS:
            await _copy_extra_table(
                session, schema_name, DIRECT_COMMITMENTS_TABLE, _COMMITMENT_COLUMNS, commitment_rows
            )
            commitment_rows.clear()
    if remaining := pending.take():
        await _copy_batch_rows(session, write_ctx.tables_by_role, remaining)
    if commitment_rows:
        await _copy_extra_table(session, schema_name, DIRECT_COMMITMENTS_TABLE, _COMMITMENT_COLUMNS, commitment_rows)
    return allocated.total_children, root_scalar_total, child_scalar_total, dict(children_by_slot)


async def _write_bucket_rejections(
    session,
    request: staging.SourceBuildRequest,
    build,
    binding: FamilyBlockBinding,
    rejection_emits: list[_RejectionEmit],
    rejected_root_entries: list[tuple[bytes, bytes | None, str | None]],
) -> int:
    schema_name = snapshot_schema(binding.family_id)
    if rejected_root_entries:
        await _copy_extra_table(
            session, schema_name, DIRECT_REJECTED_TABLE, _REJECTED_ROOT_COLUMNS, rejected_root_entries
        )
    if not rejection_emits:
        return 0
    rejection_ids = await _allocate_sequence_ids(session, _REJECTION_SEQ, len(rejection_emits))
    await staging._prepare_statement(session)
    stamp = await session.scalar(select(func.clock_timestamp()))
    rejection_table = binding.tables_by_role["rejection"]
    rejection_cols = copy_columns(rejection_table)
    token_hash = bytes(producer_token_sha256(request))
    next_ordinal = int(build.next_rejection_ordinal)
    copy_tuples = []
    for offset, (rejection_id, emit) in enumerate(zip(rejection_ids, rejection_emits, strict=True)):
        row_by_col = {
            "rejection_id": rejection_id,
            "execution_id": build.execution_id,
            "rejection_ordinal": next_ordinal + offset,
            "dataset_id": build.dataset_id,
            "definition_revision_id": build.definition_revision_id,
            "schema_revision_id": build.schema_revision_id,
            "pack_id": emit.pack_id,
            "root_key_sha256": emit.root_key_sha256,
            "canonical_root_key": emit.canonical_root_key,
            "collection_slot": emit.collection_slot,
            "source_ordinal": emit.source_ordinal,
            "code": emit.code,
            "field_slot": None,
            "canonical_evidence": emit.canonical_evidence,
            "producing_fence": build.producing_fence,
            "producing_token_sha256": token_hash,
            "created_at": stamp,
        }
        copy_tuples.append(tuple(row_by_col[col_name] for col_name in rejection_cols))
    for offset in range(0, len(copy_tuples), MAX_BATCH_ROWS):
        await _copy_extra_table(
            session, schema_name, rejection_table.name, rejection_cols, copy_tuples[offset : offset + MAX_BATCH_ROWS]
        )
    return len(copy_tuples)


async def _check_bucket_family_budget(session, schema_sql: str, bucket_name: str, max_family_bytes: int) -> int:
    """Enforce per-family spool budget in SQL before loading staged rows into Python."""
    await staging._prepare_statement(session)
    oversized = await session.scalar(
        text(
            f"""
            SELECT raw_key_sha256
            FROM {schema_sql}.{bucket_name}
            WHERE raw_key_sha256 IS NOT NULL
            GROUP BY raw_key_sha256
            HAVING sum(
                coalesce(octet_length(packed_slots), 0)::bigint
                + coalesce(octet_length(raw_key_canonical), 0)::bigint
                + coalesce(octet_length(typed_key_canonical), 0)::bigint
                + coalesce(octet_length(child_key_canonical), 0)::bigint
                + coalesce(octet_length(rejection_evidence), 0)::bigint
            ) > :max_family_bytes
            LIMIT 1
            """
        ),
        {"max_family_bytes": max_family_bytes},
    )
    if oversized is not None:
        raise FamilyBlockResourceLimit("staged family exceeds the admission spool budget")
    await staging._prepare_statement(session)
    return int(await session.scalar(text(f"SELECT count(*) FROM {schema_sql}.{bucket_name}")) or 0)


async def _admit_and_copy_slice(
    session,
    request: staging.SourceBuildRequest,
    build,
    registry,
    binding: FamilyBlockBinding,
    slice_rows: list[dict[str, object]],
    *,
    limits: FamilyBlockLimits,
    external_dup_typed_hashes: set[bytes] | None = None,
) -> tuple[int, int, int, int, int, int, int, dict[int, int]]:
    if not slice_rows:
        return 0, 0, 0, 0, 0, 0, 0, {}
    accepted_bundles, rejection_emits, rejected_roots, src_rej, cand_err = _admit_bucket_rows(
        request.definition, registry, slice_rows, external_dup_typed_hashes=external_dup_typed_hashes
    )
    inserted_rej = await _write_bucket_rejections(session, request, build, binding, rejection_emits, rejected_roots)
    build.next_rejection_ordinal += inserted_rej
    children_n, root_sc_n, child_sc_n, slot_counts_by_slot = await _write_accepted_bundles(
        session, request, build, registry, binding, accepted_bundles, limits=limits
    )
    return (
        len(accepted_bundles),
        src_rej,
        cand_err,
        inserted_rej,
        children_n,
        root_sc_n,
        child_sc_n,
        slot_counts_by_slot,
    )


async def _load_dup_typed_hashes(session, schema_sql: str, bucket_name: str) -> set[bytes]:
    await staging._prepare_statement(session)
    return {
        bytes(dup_hash)
        for dup_hash in (
            await session.scalars(
                text(
                    f"SELECT typed_key_sha256 FROM {schema_sql}.{bucket_name} "
                    "WHERE record_kind = 0 AND typed_key_sha256 IS NOT NULL "
                    "GROUP BY typed_key_sha256 HAVING count(*) > 1"
                )
            )
        ).all()
    }


async def _fetch_bucket_key_page(session, schema_sql: str, bucket_name: str, after_key: bytes | None) -> list[bytes]:
    key_where = "raw_key_sha256 IS NOT NULL" if after_key is None else "raw_key_sha256 > :after_key"
    key_params_by_name: dict[str, object] = {"key_limit": _BUCKET_SLICE_KEY_LIMIT}
    if after_key is not None:
        key_params_by_name["after_key"] = after_key
    await staging._prepare_statement(session)
    return [
        bytes(raw_key)
        for raw_key in (
            await session.scalars(
                text(
                    f"SELECT DISTINCT raw_key_sha256 FROM {schema_sql}.{bucket_name} "
                    f"WHERE {key_where} ORDER BY raw_key_sha256 LIMIT :key_limit"
                ),
                key_params_by_name,
            )
        ).all()
    ]


async def _fetch_bucket_rows(
    session, schema_sql: str, bucket_name: str, raw_keys: list[bytes] | None = None
) -> list[dict[str, object]]:
    await staging._prepare_statement(session)
    if raw_keys is None:
        stmt = text(
            f"SELECT * FROM {schema_sql}.{bucket_name} WHERE raw_key_sha256 IS NULL ORDER BY source_ordinal, stream_slot"
        )
    else:
        stmt = text(
            f"SELECT * FROM {schema_sql}.{bucket_name} "
            "WHERE raw_key_sha256 = ANY(:raw_keys) ORDER BY source_ordinal, stream_slot"
        ).bindparams(bindparam("raw_keys", raw_keys, type_=ARRAY(LargeBinary)))
    return [dict(mapping) for mapping in (await session.execute(stmt)).mappings().all()]


def _merge_slice_totals(totals: list[int], counts_by_slot: dict[int, int], slice_stats) -> None:
    for idx in range(7):
        totals[idx] += slice_stats[idx]
    for slot, count_val in slice_stats[7].items():
        counts_by_slot[slot] = counts_by_slot.get(slot, 0) + count_val


async def _process_paged_bucket(
    session,
    request: staging.SourceBuildRequest,
    build,
    registry,
    binding: FamilyBlockBinding,
    schema_sql: str,
    bucket_name: str,
    *,
    limits: FamilyBlockLimits,
) -> tuple[int, int, int, int, int, int, int, dict[int, int]]:
    """Page large buckets by raw_key_sha256 so memory stays bounded per family slice."""
    dup_typed_hashes = await _load_dup_typed_hashes(session, schema_sql, bucket_name)
    await staging._prepare_statement(session)
    await session.execute(text(f"CREATE INDEX ON {schema_sql}.{bucket_name} (raw_key_sha256)"))
    totals = [0, 0, 0, 0, 0, 0, 0]
    counts_by_slot: dict[int, int] = {}
    unkeyed_rows = await _fetch_bucket_rows(session, schema_sql, bucket_name, raw_keys=None)
    if unkeyed_rows:
        stats = await _admit_and_copy_slice(
            session,
            request,
            build,
            registry,
            binding,
            unkeyed_rows,
            limits=limits,
            external_dup_typed_hashes=dup_typed_hashes,
        )
        _merge_slice_totals(totals, counts_by_slot, stats)
    after_key: bytes | None = None
    while True:
        raw_keys = await _fetch_bucket_key_page(session, schema_sql, bucket_name, after_key)
        if not raw_keys:
            break
        slice_rows = await _fetch_bucket_rows(session, schema_sql, bucket_name, raw_keys=raw_keys)
        stats = await _admit_and_copy_slice(
            session,
            request,
            build,
            registry,
            binding,
            slice_rows,
            limits=limits,
            external_dup_typed_hashes=dup_typed_hashes,
        )
        _merge_slice_totals(totals, counts_by_slot, stats)
        after_key = raw_keys[-1]
        if len(raw_keys) < _BUCKET_SLICE_KEY_LIMIT:
            break
    return totals[0], totals[1], totals[2], totals[3], totals[4], totals[5], totals[6], counts_by_slot


async def _process_one_bucket(
    session,
    request: staging.SourceBuildRequest,
    build,
    registry,
    binding: FamilyBlockBinding,
    schema_sql: str,
    bucket_name: str,
    *,
    limits: FamilyBlockLimits,
) -> DirectBucketOutcome:
    """Read, admit, COPY, and drop one unlogged stage bucket table within bounded memory."""
    total_rows = await _check_bucket_family_budget(session, schema_sql, bucket_name, limits.max_spool_bytes)
    stats: tuple[int, int, int, int, int, int, int, dict[int, int]] = (0, 0, 0, 0, 0, 0, 0, {})
    if 0 < total_rows <= _BUCKET_DIRECT_SCAN_LIMIT:
        await staging._prepare_statement(session)
        bucket_rows = [
            dict(mapping)
            for mapping in (
                await session.execute(
                    text(f"SELECT * FROM {schema_sql}.{bucket_name} ORDER BY source_ordinal, stream_slot")
                )
            )
            .mappings()
            .all()
        ]
        stats = await _admit_and_copy_slice(session, request, build, registry, binding, bucket_rows, limits=limits)
    elif total_rows > _BUCKET_DIRECT_SCAN_LIMIT:
        stats = await _process_paged_bucket(
            session, request, build, registry, binding, schema_sql, bucket_name, limits=limits
        )
    await staging._prepare_statement(session)
    await session.execute(text(f"DROP TABLE {schema_sql}.{bucket_name}"))
    return DirectBucketOutcome(1, total_rows, *stats)


async def _save_bucket_progress(
    session,
    schema_sql: str,
    progress_by_col: dict[str, object],
    outcome: DirectBucketOutcome,
    next_bucket: int,
    bucket_count: int,
) -> None:
    counts_by_slot = {
        int(slot_key): int(count_val)
        for slot_key, count_val in json.loads(progress_by_col["collection_counts_json"]).items()
    }
    for slot, count_val in outcome.children_by_slot.items():
        counts_by_slot[slot] = counts_by_slot.get(slot, 0) + count_val
    next_stage = "f1" if next_bucket >= bucket_count else "buckets"
    await staging._prepare_statement(session)
    await session.execute(
        text(
            f"""
            UPDATE {schema_sql}.{DIRECT_PROGRESS_TABLE}
            SET next_bucket = :next_bucket,
                finalize_stage = :next_stage,
                accepted_family_count = accepted_family_count + :accepted_n,
                source_rejection_count = source_rejection_count + :src_rej_n,
                total_child_count = total_child_count + :child_n,
                root_scalar_count = root_scalar_count + :root_sc_n,
                child_scalar_count = child_scalar_count + :child_sc_n,
                collection_counts_json = :counts_json
            WHERE id = 1
            """
        ),
        {
            "next_bucket": next_bucket,
            "next_stage": next_stage,
            "accepted_n": outcome.accepted_families,
            "src_rej_n": outcome.source_rejections,
            "child_n": outcome.child_count,
            "root_sc_n": outcome.root_scalar_count,
            "child_sc_n": outcome.child_scalar_count,
            "counts_json": json.dumps(
                {str(slot_key): count_val for slot_key, count_val in sorted(counts_by_slot.items())}
            ),
        },
    )


async def process_direct_buckets(
    session,
    request: staging.SourceBuildRequest,
    build,
    registry,
    binding: FamilyBlockBinding,
    progress_by_col: dict[str, object],
    *,
    limits: FamilyBlockLimits,
) -> DirectBucketOutcome:
    """Process one or more bounded buckets, drop them, and advance progress."""
    schema_sql = _DIALECT.identifier_preparer.quote_schema(snapshot_schema(binding.family_id))
    bucket_count = int(progress_by_col["bucket_count"])
    next_bucket = int(progress_by_col["next_bucket"])
    totals = [0, 0, 0, 0, 0, 0, 0, 0, 0]
    merged_by_slot: dict[int, int] = defaultdict(int)
    while next_bucket < bucket_count:
        step = await _process_one_bucket(
            session, request, build, registry, binding, schema_sql, direct_bucket_table_name(next_bucket), limits=limits
        )
        next_bucket += 1
        step_values = (
            step.buckets_processed,
            step.staged_rows_processed,
            step.accepted_families,
            step.source_rejections,
            step.candidate_errors,
            step.rejections_inserted,
            step.child_count,
            step.root_scalar_count,
            step.child_scalar_count,
        )
        for idx, delta in enumerate(step_values):
            totals[idx] += delta
        for slot, count_val in step.children_by_slot.items():
            merged_by_slot[slot] += count_val
        if totals[1] >= _BUCKET_BATCH_ROW_BUDGET:
            break
    outcome = DirectBucketOutcome(*totals, children_by_slot=dict(merged_by_slot))
    await _save_bucket_progress(session, schema_sql, progress_by_col, outcome, next_bucket, bucket_count)
    return outcome
