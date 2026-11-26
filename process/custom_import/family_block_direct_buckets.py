# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Per-bucket O(N) admission and compact v2 encoding for direct builds."""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from dataclasses import dataclass
from types import SimpleNamespace

from sqlalchemy import BigInteger, LargeBinary, Sequence, Text, any_, bindparam, column, func, literal, select, text
from sqlalchemy.dialects.postgresql import ARRAY, dialect, insert

from db.models.custom_import import CustomImportEntityBinding, CustomImportRootRecord
from process.custom_import import build_source as staging
from process.custom_import import materialization
from process.custom_import.bulk_page_codec import MAX_BATCH_ROWS
from process.custom_import.continuation_runtime import producer_token_sha256
from process.custom_import.definition import CustomImportDefinition
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
    compile_v2_family_plan,
    encode_family_v2,
    extract_staged_membership_key,
    finalize_native_v2_family,
    pack_v2_slot_query_scalars,
    unpack_staged_values_and_scalars,
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
_BUCKET_DIRECT_SCAN_LIMIT = 131_072
_BUCKET_SLICE_KEY_LIMIT = 8_192
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

_STAGE_SELECT_SQL = (
    "record_kind, stream_slot, collection_slot, pack_id, source_ordinal, "
    "raw_key_sha256, raw_key_canonical, typed_key_sha256, typed_key_canonical, "
    "child_key_sha256, child_key_canonical, payload_sha256, packed_slots, "
    "initial_code, rejection_hash, rejection_key, rejection_evidence"
)
_COL_KIND = 0
_COL_STREAM_SLOT = 1
_COL_COLLECTION_SLOT = 2
_COL_PACK_ID = 3
_COL_SOURCE_ORDINAL = 4
_COL_RAW_HASH = 5
_COL_RAW_CANON = 6
_COL_TYPED_HASH = 7
_COL_TYPED_CANON = 8
_COL_CHILD_HASH = 9
_COL_CHILD_CANON = 10
_COL_PAYLOAD_HASH = 11
_COL_PACKED_SLOTS = 12
_COL_INITIAL_CODE = 13
_COL_REJ_HASH = 14
_COL_REJ_KEY = 15
_COL_REJ_EVIDENCE = 16


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
    root_entry: tuple
    root_values_by_field: dict[str, object]
    root_scalars_by_field: dict[str, materialization.TypedScalar]
    raw_children_by_slot: dict[int, list[tuple]]
    unpacked_children_by_slot: dict[int, dict[int, dict[str, object]]] | None


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
    query_child_slot_specs: tuple[tuple[int, object], ...]
    fields_by_name_map: dict
    sorted_collections: tuple[tuple[str, int], ...]
    tables_by_role: dict
    key_contract_hash: bytes
    limits: FamilyBlockLimits
    v2_plan: object
    winner_contract: object
    root_profiles: tuple[tuple[int, object], ...]
    child_profiles: tuple[tuple[int, object, int], ...]
    context_cache: dict[tuple[int, int, tuple], tuple[str, bytes]]


@dataclass(frozen=True)
class _PreparedFamilyWrite:
    ids_tuple: tuple[int, int, int, int]
    bundle: _AcceptedFamilyBundle
    sorted_children_by_col: list[tuple[str, int, list[tuple[int, tuple]]]]
    family_child_count: int


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


async def _allocate_family_and_root_ids(session, count_needed: int) -> tuple[list[int], list[int]]:
    if count_needed <= 0:
        return [], []
    await staging._prepare_statement(session)
    id_pairs = (
        await session.execute(
            select(_FAMILY_SEQ.next_value(), _ROOT_REV_SEQ.next_value()).select_from(
                func.generate_series(1, count_needed)
            )
        )
    ).all()
    return [int(pair[0]) for pair in id_pairs], [int(pair[1]) for pair in id_pairs]


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


def _emit_from_staged(staged: tuple, code: str) -> _RejectionEmit:
    uses_initial = staged[_COL_INITIAL_CODE] == code and staged[_COL_REJ_EVIDENCE] is not None
    typed_hash = staged[_COL_TYPED_HASH]
    rej_hash = staged[_COL_REJ_HASH]
    return _RejectionEmit(
        pack_id=int(staged[_COL_PACK_ID]),
        root_key_sha256=(
            (None if rej_hash is None else bytes(rej_hash))
            if uses_initial
            else (None if typed_hash is None else bytes(typed_hash))
        ),
        canonical_root_key=staged[_COL_REJ_KEY] if uses_initial else staged[_COL_TYPED_CANON],
        collection_slot=None if staged[_COL_KIND] == 0 else int(staged[_COL_COLLECTION_SLOT]),
        source_ordinal=int(staged[_COL_SOURCE_ORDINAL]),
        code=code,
        canonical_evidence=staged[_COL_REJ_EVIDENCE] if uses_initial else "{}",
    )


def _partition_bucket_rows(staged_rows: list[tuple]):
    unkeyed_rows: list[tuple] = []
    roots_by_raw: dict[bytes, list[tuple]] = defaultdict(list)
    children_by_raw: dict[bytes, list[tuple]] = defaultdict(list)
    raw_text_by_hash: dict[bytes, str] = {}
    typed_counts_by_hash: dict[bytes, int] = defaultdict(int)
    for staged in staged_rows:
        raw_hash = staged[_COL_RAW_HASH]
        if raw_hash is None:
            unkeyed_rows.append(staged)
            continue
        raw_bytes = bytes(raw_hash)
        raw_text = staged[_COL_RAW_CANON]
        existing_raw = raw_text_by_hash.get(raw_bytes)
        if existing_raw is not None and existing_raw != raw_text:
            raise FamilyBlockUnavailable("raw key digest collision")
        raw_text_by_hash[raw_bytes] = raw_text
        if staged[_COL_KIND] == 0:
            roots_by_raw[raw_bytes].append(staged)
            typed_hash = staged[_COL_TYPED_HASH]
            if typed_hash is not None:
                typed_counts_by_hash[bytes(typed_hash)] += 1
        else:
            children_by_raw[raw_bytes].append(staged)
    return unkeyed_rows, roots_by_raw, children_by_raw, typed_counts_by_hash


def _collect_unkeyed_and_orphan_rejections(
    unkeyed_rows: list[tuple],
    roots_by_raw: dict[bytes, list[tuple]],
    children_by_raw: dict[bytes, list[tuple]],
) -> tuple[list[_RejectionEmit], int, int]:
    rejection_emits: list[_RejectionEmit] = []
    source_rejections = candidate_errors = 0
    for unkeyed in unkeyed_rows:
        code = unkeyed[_COL_INITIAL_CODE]
        if code not in _CANDIDATE_CODES:
            raise FamilyBlockUnavailable("unkeyed source row lacks a candidate error code")
        candidate_errors += 1
        if code in _UNKEYED_ROOT_CODES:
            source_rejections += 1
        rej_hash = unkeyed[_COL_REJ_HASH]
        rejection_emits.append(
            _RejectionEmit(
                pack_id=int(unkeyed[_COL_PACK_ID]),
                root_key_sha256=None if rej_hash is None else bytes(rej_hash),
                canonical_root_key=unkeyed[_COL_REJ_KEY],
                collection_slot=None if unkeyed[_COL_KIND] == 0 else int(unkeyed[_COL_COLLECTION_SLOT]),
                source_ordinal=int(unkeyed[_COL_SOURCE_ORDINAL]),
                code=code,
                canonical_evidence=unkeyed[_COL_REJ_EVIDENCE] or "{}",
            )
        )
    for raw_bytes, orphan_children in children_by_raw.items():
        if raw_bytes in roots_by_raw:
            continue
        for child_entry in orphan_children:
            initial_code = child_entry[_COL_INITIAL_CODE]
            effective_code = "child_not_object" if initial_code == "child_not_object" else "orphan_child"
            candidate_errors += 1
            typed_hash = child_entry[_COL_TYPED_HASH]
            rejection_emits.append(
                _RejectionEmit(
                    pack_id=int(child_entry[_COL_PACK_ID]),
                    root_key_sha256=None if typed_hash is None else bytes(typed_hash),
                    canonical_root_key=child_entry[_COL_TYPED_CANON],
                    collection_slot=int(child_entry[_COL_COLLECTION_SLOT]),
                    source_ordinal=int(child_entry[_COL_SOURCE_ORDINAL]),
                    code=effective_code,
                    canonical_evidence=child_entry[_COL_REJ_EVIDENCE] if effective_code == initial_code else "{}",
                )
            )
    return rejection_emits, source_rejections, candidate_errors


def _admit_family_children(
    family_children: list[tuple],
    collapse_by_slot_map: dict[int, bool],
) -> tuple[dict[tuple[int, bytes], tuple], set[str], list[_RejectionEmit], list[_RejectionEmit], int | None]:
    accepted_by_slot_and_key: dict[tuple[int, bytes], tuple] = {}
    key_text_by_slot_and_hash: dict[tuple[int, bytes], str] = {}
    child_codes: set[str] = set()
    family_rejections: list[_RejectionEmit] = []
    candidate_emits: list[_RejectionEmit] = []
    earliest_child_slot: int | None = None
    for child_entry in family_children:
        initial_code = child_entry[_COL_INITIAL_CODE]
        if initial_code in ("child_not_object", "orphan_child"):
            candidate_emits.append(_emit_from_staged(child_entry, initial_code))
            continue
        if initial_code is not None:
            child_codes.add(initial_code)
            family_rejections.append(_emit_from_staged(child_entry, initial_code))
            child_stream_slot = int(child_entry[_COL_STREAM_SLOT])
            if earliest_child_slot is None or child_stream_slot < earliest_child_slot:
                earliest_child_slot = child_stream_slot
            continue
        slot = int(child_entry[_COL_COLLECTION_SLOT])
        slot_key = (slot, bytes(child_entry[_COL_CHILD_HASH]))
        child_key_text = child_entry[_COL_CHILD_CANON]
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
            and bytes(previous[_COL_PAYLOAD_HASH]) == bytes(child_entry[_COL_PAYLOAD_HASH])
            and bytes(previous[_COL_PACKED_SLOTS]) == bytes(child_entry[_COL_PACKED_SLOTS])
        )
        if is_identical:
            if int(child_entry[_COL_SOURCE_ORDINAL]) > int(previous[_COL_SOURCE_ORDINAL]):
                accepted_by_slot_and_key[slot_key] = child_entry
        else:
            child_codes.add("duplicate_child_key")
            family_rejections.append(_emit_from_staged(child_entry, "duplicate_child_key"))
            child_stream_slot = int(child_entry[_COL_STREAM_SLOT])
            if earliest_child_slot is None or child_stream_slot < earliest_child_slot:
                earliest_child_slot = child_stream_slot
    return accepted_by_slot_and_key, child_codes, family_rejections, candidate_emits, earliest_child_slot


@dataclass(frozen=True)
class _ChildMembershipSpec:
    outer_slot: int
    outer_len: int
    outer_key_specs: tuple[tuple[int, str], ...]
    inner_slot: int
    inner_len: int
    inner_key_specs: tuple[tuple[int, str], ...]


def _group_and_check_membership(
    membership_specs: tuple[_ChildMembershipSpec, ...],
    accepted_by_slot_and_key: dict[tuple[int, bytes], tuple],
) -> tuple[dict[int, list[tuple]], bool]:
    raw_by_slot: dict[int, list[tuple]] = defaultdict(list)
    for (slot, _child_hash), child_entry in accepted_by_slot_and_key.items():
        raw_by_slot[slot].append(child_entry)
    for spec in membership_specs:
        outer_keys: set[tuple[object, ...]] = set()
        for outer_entry in raw_by_slot.get(spec.outer_slot, ()):
            outer_key = extract_staged_membership_key(
                bytes(outer_entry[_COL_PACKED_SLOTS]), spec.outer_len, spec.outer_key_specs
            )
            if outer_key is None:
                return dict(raw_by_slot), False
            outer_keys.add(outer_key)
        for inner_entry in raw_by_slot.get(spec.inner_slot, ()):
            inner_key = extract_staged_membership_key(
                bytes(inner_entry[_COL_PACKED_SLOTS]), spec.inner_len, spec.inner_key_specs
            )
            if inner_key is None or inner_key not in outer_keys:
                return dict(raw_by_slot), False
    return dict(raw_by_slot), True


def _collect_root_rejections(
    root_entries: list[tuple], earliest_child_slot: int | None
) -> tuple[set[str], list[_RejectionEmit]]:
    first_root = root_entries[0]
    is_blocked_by_child = earliest_child_slot is not None and earliest_child_slot < int(first_root[_COL_STREAM_SLOT])
    family_codes: set[str] = set()
    family_rejections: list[_RejectionEmit] = []
    if first_root[_COL_INITIAL_CODE] is not None:
        family_codes.add(first_root[_COL_INITIAL_CODE])
    for root_entry in root_entries:
        if root_entry[_COL_INITIAL_CODE] is not None:
            family_rejections.append(_emit_from_staged(root_entry, root_entry[_COL_INITIAL_CODE]))
    if len(root_entries) > 1:
        family_codes.add("duplicate_root_key")
        for dup_root in root_entries:
            if dup_root[_COL_INITIAL_CODE] is None and not (dup_root is first_root and is_blocked_by_child):
                family_rejections.append(_emit_from_staged(dup_root, "duplicate_root_key"))
    return family_codes, family_rejections


def _admit_single_family(
    definition: CustomImportDefinition,
    collapse_by_slot_map: dict[int, bool],
    membership_specs: tuple[_ChildMembershipSpec, ...],
    raw_bytes: bytes,
    root_entries: list[tuple],
    family_children: list[tuple],
    dup_typed_hashes: set[bytes],
) -> tuple[_AcceptedFamilyBundle | None, list[_RejectionEmit], int, int]:
    """Admit or reject one keyed root family and its staged children."""
    root_entries.sort(key=lambda entry: int(entry[_COL_SOURCE_ORDINAL]))
    first_root = root_entries[0]
    accepted_by_slot_and_key, child_codes, child_rejections, candidate_emits, earliest_child_slot = (
        _admit_family_children(family_children, collapse_by_slot_map)
    )
    family_codes, family_rejections = _collect_root_rejections(root_entries, earliest_child_slot)
    family_codes.update(child_codes)
    family_rejections.extend(child_rejections)
    raw_by_slot: dict[int, list[tuple]] = {}
    if not family_codes:
        raw_by_slot, has_membership = _group_and_check_membership(membership_specs, accepted_by_slot_and_key)
        if not has_membership:
            family_codes.add("child_membership_missing")
            family_rejections.append(_emit_from_staged(first_root, "child_membership_missing"))
    typed_hash = None if first_root[_COL_TYPED_HASH] is None else bytes(first_root[_COL_TYPED_HASH])
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
    if typed_hash is None or first_root[_COL_PACKED_SLOTS] is None:
        raise FamilyBlockUnavailable("accepted root lacks typed identity or payload")
    root_values_by_field, root_scalars_by_field = unpack_staged_values_and_scalars(
        definition.root_fields, bytes(first_root[_COL_PACKED_SLOTS])
    )
    bundle = _AcceptedFamilyBundle(
        raw_hash=raw_bytes,
        typed_hash=typed_hash,
        typed_canonical=first_root[_COL_TYPED_CANON],
        root_entry=first_root,
        root_values_by_field=root_values_by_field,
        root_scalars_by_field=root_scalars_by_field,
        raw_children_by_slot=raw_by_slot,
        unpacked_children_by_slot=None,
    )
    return bundle, candidate_emits, 0, len(candidate_emits)


def _bucket_admission_maps(
    definition: CustomImportDefinition, registry
) -> tuple[dict[int, bool], tuple[_ChildMembershipSpec, ...]]:
    fields_by_name = fields_by_collection(definition)
    collapse_by_slot_map = {
        registry.child_collection_slots[stream.child_collection]: stream.duplicate_policy == "collapse_identical"
        for stream in definition.source_streams
        if stream.child_collection is not None
    }
    membership_specs: list[_ChildMembershipSpec] = []
    for membership in definition.child_memberships:
        outer_fields = fields_by_name[membership.outer_collection]
        inner_fields = fields_by_name[membership.inner_collection]
        outer_by_id = {field_spec.field_id: (idx, field_spec.value_type) for idx, field_spec in enumerate(outer_fields)}
        inner_by_id = {field_spec.field_id: (idx, field_spec.value_type) for idx, field_spec in enumerate(inner_fields)}
        membership_specs.append(
            _ChildMembershipSpec(
                outer_slot=registry.child_collection_slots[membership.outer_collection],
                outer_len=len(outer_fields),
                outer_key_specs=tuple(outer_by_id[outer_field] for outer_field, _ in membership.key_mapping),
                inner_slot=registry.child_collection_slots[membership.inner_collection],
                inner_len=len(inner_fields),
                inner_key_specs=tuple(inner_by_id[inner_field] for _, inner_field in membership.key_mapping),
            )
        )
    return collapse_by_slot_map, tuple(membership_specs)


def _admit_bucket_rows(
    definition: CustomImportDefinition,
    registry,
    staged_rows: list[tuple],
    *,
    external_dup_typed_hashes: set[bytes] | None = None,
) -> tuple[list[_AcceptedFamilyBundle], list[_RejectionEmit], list[tuple[bytes, bytes | None, str | None]], int, int]:
    """Run exact O(N) family admission on a bounded slice of staged rows in one bucket."""
    collapse_by_slot_map, membership_specs = _bucket_admission_maps(definition, registry)
    unkeyed_rows, roots_by_raw, children_by_raw, typed_counts_by_hash = _partition_bucket_rows(staged_rows)
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
            collapse_by_slot_map,
            membership_specs,
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
            first_typed = root_entries[0][_COL_TYPED_HASH]
            rejected_root_entries.append(
                (raw_bytes, None if first_typed is None else bytes(first_typed), root_entries[0][_COL_TYPED_CANON])
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


async def _append_copy_role(session, tables_by_role: dict, pending: BlockCopyBuffer, role: str, cells: tuple) -> None:
    if ready := pending.append(role, cells):
        await _copy_batch_rows(session, tables_by_role, ready)


def _cached_profile_context(
    write_ctx: _AcceptedWriteContext,
    profile_slot: int,
    profile,
    scope_slot: int,
    scalar_values_by_field: dict[str, materialization.TypedScalar],
    candidate_ref,
) -> tuple[str, bytes]:
    fields_by_id = write_ctx.winner_contract.fields_by_id
    normalized = materialization._NormalizedWinnerCandidate(
        candidate=candidate_ref, values_by_field=scalar_values_by_field
    )
    materialization._validate_selection_values(profile, normalized, fields_by_id)
    dimension_keys = tuple(
        None if (sc := scalar_values_by_field.get(fid)) is None else (sc.value_state, sc.logical_value)
        for fid in profile.context_dimensions
    )
    cache_key = (profile_slot, scope_slot, dimension_keys)
    cached = write_ctx.context_cache.get(cache_key)
    if cached is not None:
        return cached
    result = materialization._context_key(profile, normalized, fields_by_id)
    write_ctx.context_cache[cache_key] = result
    return result


def _query_root_scalars_map(
    write_ctx: _AcceptedWriteContext, bundle: _AcceptedFamilyBundle
) -> dict[str, materialization.TypedScalar]:
    definition = write_ctx.request.definition
    fields_by_id = write_ctx.winner_contract.fields_by_id
    root_scalars = bundle.root_scalars_by_field
    root_values = bundle.root_values_by_field
    return {
        fid: (
            root_scalars[fid]
            if fid in root_scalars
            else materialization._typed_scalar(fields_by_id[fid], root_values[fid])
        )
        for fid in definition.query.root_fields
        if fid in root_values
    }


async def _emit_family_contexts(
    session,
    write_ctx: _AcceptedWriteContext,
    pending: BlockCopyBuffer,
    family_identity,
    prepared: _PreparedFamilyWrite,
) -> None:
    build_id = write_ctx.build.build_id
    bundle = prepared.bundle
    root_query_scalars = _query_root_scalars_map(write_ctx, bundle)
    if write_ctx.root_profiles:
        root_ref = SimpleNamespace(context_collection_slot=0)
        for profile_slot, profile in write_ctx.root_profiles:
            canonical_key, key_digest = _cached_profile_context(
                write_ctx, profile_slot, profile, 0, root_query_scalars, root_ref
            )
            await _append_copy_role(
                session,
                write_ctx.tables_by_role,
                pending,
                "candidate_context",
                (
                    build_id,
                    profile_slot,
                    family_identity.entity_binding_id,
                    family_identity.family_revision_id,
                    0,
                    None,
                    canonical_key,
                    key_digest,
                ),
            )
    if write_ctx.query_collection is None:
        return
    await _emit_query_child_rows_and_contexts(
        session, write_ctx, pending, family_identity, prepared, root_query_scalars
    )


async def _emit_one_query_child_contexts(
    session,
    write_ctx: _AcceptedWriteContext,
    pending: BlockCopyBuffer,
    family_identity,
    slot: int,
    child_rev_id: int,
    combined_scalars_by_field: dict[str, materialization.TypedScalar],
) -> None:
    """Emit child-scoped candidate contexts for one queried child record."""
    build_id = write_ctx.build.build_id
    child_ref = SimpleNamespace(context_collection_slot=slot)
    for profile_slot, profile, profile_col_slot in write_ctx.child_profiles:
        if profile_col_slot != slot:
            continue
        canonical_key, key_digest = _cached_profile_context(
            write_ctx, profile_slot, profile, slot, combined_scalars_by_field, child_ref
        )
        await _append_copy_role(
            session,
            write_ctx.tables_by_role,
            pending,
            "candidate_context",
            (
                build_id,
                profile_slot,
                family_identity.entity_binding_id,
                family_identity.family_revision_id,
                slot,
                child_rev_id,
                canonical_key,
                key_digest,
            ),
        )


def _merge_child_query_scalars(
    write_ctx: _AcceptedWriteContext,
    root_query_scalars: dict[str, materialization.TypedScalar],
    child_vals_by_field: dict[str, object] | None,
    child_scalars_by_field: dict[str, materialization.TypedScalar],
) -> dict[str, materialization.TypedScalar]:
    fields_by_id = write_ctx.winner_contract.fields_by_id
    combined_by_field = dict(root_query_scalars)
    for fid in write_ctx.request.definition.query.child_fields:
        if fid in child_scalars_by_field:
            combined_by_field[fid] = child_scalars_by_field[fid]
        elif child_vals_by_field is not None and fid in child_vals_by_field:
            combined_by_field[fid] = materialization._typed_scalar(fields_by_id[fid], child_vals_by_field[fid])
    return combined_by_field


async def _emit_query_child_rows_and_contexts(
    session,
    write_ctx: _AcceptedWriteContext,
    pending: BlockCopyBuffer,
    family_identity,
    prepared: _PreparedFamilyWrite,
    root_query_scalars: dict[str, materialization.TypedScalar],
) -> None:
    """Copy queried child rows and emit child-scoped contexts when configured."""
    query_col_name = write_ctx.query_collection
    col_fields = write_ctx.fields_by_name_map[query_col_name]
    for col_name, slot, ordered_children in prepared.sorted_children_by_col:
        if col_name != query_col_name:
            continue
        for child_rev_id, child_entry in ordered_children:
            packed = bytes(child_entry[_COL_PACKED_SLOTS])
            if write_ctx.child_profiles:
                child_vals_by_field, child_scalars_by_field = unpack_staged_values_and_scalars(col_fields, packed)
                packed_child_scalars = pack_query_scalars(
                    write_ctx.query_child_fields, child_scalars_by_field, validated=True
                )
            else:
                child_vals_by_field = child_scalars_by_field = None
                packed_child_scalars = pack_v2_slot_query_scalars(col_fields, write_ctx.query_child_slot_specs, packed)
            await _append_copy_role(
                session,
                write_ctx.tables_by_role,
                pending,
                "query_input",
                (
                    child_rev_id,
                    family_identity.family_revision_id,
                    slot,
                    bytes(child_entry[_COL_CHILD_HASH]),
                    child_entry[_COL_CHILD_CANON],
                    *packed_child_scalars,
                ),
            )
            if write_ctx.child_profiles and child_scalars_by_field is not None:
                combined_by_field = _merge_child_query_scalars(
                    write_ctx, root_query_scalars, child_vals_by_field, child_scalars_by_field
                )
                await _emit_one_query_child_contexts(
                    session, write_ctx, pending, family_identity, slot, child_rev_id, combined_by_field
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
        *pack_query_scalars(query_root_fields, root_scalars_by_field, validated=True),
    )


def _encode_family_python_fallback(write_ctx: _AcceptedWriteContext, prepared: _PreparedFamilyWrite):
    definition = write_ctx.request.definition
    bundle = prepared.bundle
    _family_id, root_rev_id, _root_record_id, _entity_binding_id = prepared.ids_tuple
    root_record = FamilyRecord(
        revision_id=root_rev_id,
        pack_id=int(bundle.root_entry[_COL_PACK_ID]),
        source_ordinal=int(bundle.root_entry[_COL_SOURCE_ORDINAL]),
        values_by_field=bundle.root_values_by_field,
        scalars=bundle.root_scalars_by_field,
    )
    sorted_children_by_name: dict[str, list[FamilyRecord]] = {}
    for col_name, slot, ordered_children in prepared.sorted_children_by_col:
        col_fields = write_ctx.fields_by_name_map[col_name]
        pre_unpacked = None if bundle.unpacked_children_by_slot is None else bundle.unpacked_children_by_slot.get(slot)
        child_records: list[FamilyRecord] = []
        for child_rev_id, child_entry in ordered_children:
            if pre_unpacked is not None and id(child_entry) in pre_unpacked:
                child_vals = pre_unpacked[id(child_entry)]
                child_scalars = _scalars_dict(col_fields, child_vals)
            else:
                child_vals, child_scalars = unpack_staged_values_and_scalars(
                    col_fields, bytes(child_entry[_COL_PACKED_SLOTS])
                )
            child_records.append(
                FamilyRecord(
                    revision_id=child_rev_id,
                    pack_id=int(child_entry[_COL_PACK_ID]),
                    source_ordinal=int(child_entry[_COL_SOURCE_ORDINAL]),
                    values_by_field=child_vals,
                    scalars=child_scalars,
                )
            )
        sorted_children_by_name[col_name] = child_records
    return encode_family_v2(
        definition,
        root_record,
        sorted_children_by_name,
        None,
        limits=write_ctx.limits,
        retained=False,
        total_child_count=prepared.family_child_count,
    )


def _prepare_one_family_write(
    write_ctx: _AcceptedWriteContext,
    bundle: _AcceptedFamilyBundle,
    ids_tuple: tuple[int, int, int, int],
    child_rev_ids,
    children_by_slot: dict[int, int],
) -> tuple[_PreparedFamilyWrite, tuple | None]:
    """Order one family's child collections and build its native batch encoder input."""
    sorted_cols: list[tuple[str, int, list[tuple[int, tuple]]]] = []
    native_cols: list[list[tuple]] = []
    family_child_count = 0
    for col_name, slot in write_ctx.sorted_collections:
        raw_children = bundle.raw_children_by_slot.get(slot)
        if not raw_children:
            sorted_cols.append((col_name, slot, []))
            native_cols.append([])
            continue
        ordered_entries = sorted(
            raw_children, key=lambda entry: (bytes(entry[_COL_CHILD_HASH]), entry[_COL_CHILD_CANON])
        )
        indexed_child_rows: list[tuple[int, tuple]] = []
        native_child_rows: list[tuple] = []
        for child_entry in ordered_entries:
            child_rev_id = next(child_rev_ids)
            indexed_child_rows.append((child_rev_id, child_entry))
            native_child_rows.append(
                (
                    child_rev_id,
                    int(child_entry[_COL_PACK_ID]),
                    int(child_entry[_COL_SOURCE_ORDINAL]),
                    bytes(child_entry[_COL_PACKED_SLOTS]),
                    bytes(child_entry[_COL_CHILD_HASH]),
                    child_entry[_COL_CHILD_CANON],
                )
            )
        sorted_cols.append((col_name, slot, indexed_child_rows))
        native_cols.append(native_child_rows)
        children_by_slot[slot] += len(indexed_child_rows)
        family_child_count += len(indexed_child_rows)
    prepared = _PreparedFamilyWrite(ids_tuple, bundle, sorted_cols, family_child_count)
    if write_ctx.v2_plan is None:
        return prepared, None
    root_entry = bundle.root_entry
    native_input = (
        ids_tuple[1],
        int(root_entry[_COL_PACK_ID]),
        int(root_entry[_COL_SOURCE_ORDINAL]),
        bytes(root_entry[_COL_PACKED_SLOTS]),
        bundle.typed_canonical,
        bytes(root_entry[_COL_PAYLOAD_HASH]),
        tuple(native_cols),
    )
    return prepared, native_input


def _prepare_slice_family_writes(
    write_ctx: _AcceptedWriteContext,
    accepted_bundles: list[_AcceptedFamilyBundle],
    allocated: _AllocatedBundleIds,
    children_by_slot: dict[int, int],
) -> tuple[list[_PreparedFamilyWrite], list[tuple]]:
    """Assign allocated identities and sort children for every accepted family in a slice."""
    entity_field = write_ctx.request.definition.entity_field
    prepared_list: list[_PreparedFamilyWrite] = []
    native_inputs: list[tuple] = []
    for idx, bundle in enumerate(accepted_bundles):
        ids_tuple = (
            allocated.family_ids[idx],
            allocated.root_rev_ids[idx],
            allocated.root_ids_by_hash[bundle.typed_hash],
            allocated.entity_ids_by_text[bundle.root_values_by_field[entity_field]],
        )
        prepared, native_input = _prepare_one_family_write(
            write_ctx, bundle, ids_tuple, allocated.child_rev_ids, children_by_slot
        )
        prepared_list.append(prepared)
        if native_input is not None:
            native_inputs.append(native_input)
    return prepared_list, native_inputs


async def _emit_prepared_family(
    session,
    write_ctx: _AcceptedWriteContext,
    pending: BlockCopyBuffer,
    prepared: _PreparedFamilyWrite,
    native_entry,
) -> tuple[int, int, tuple[object, ...]]:
    """Copy one encoded compact v2 family into output buffers and return its commitment row."""
    family_id, _root_rev_id, root_record_id, entity_binding_id = prepared.ids_tuple
    bundle = prepared.bundle
    definition = write_ctx.request.definition
    encoded = (
        finalize_native_v2_family(
            definition,
            bytes(bundle.root_entry[_COL_PAYLOAD_HASH]),
            prepared.family_child_count,
            native_entry,
            limits=write_ctx.limits,
        )
        if native_entry is not None
        else None
    )
    if encoded is None:
        encoded = _encode_family_python_fallback(write_ctx, prepared)
    with encoded:
        manifest = encoded.stored_manifest
        root_source_ordinal = int(bundle.root_entry[_COL_SOURCE_ORDINAL])
        family_cells = _accepted_family_cells(
            prepared.ids_tuple, encoded, root_source_ordinal, write_ctx.query_root_fields, bundle.root_scalars_by_field
        )
        await _append_copy_role(session, write_ctx.tables_by_role, pending, "family", family_cells)
        for directory, frame in encoded.indexed_fragments():
            frag_cells = (family_id, directory[0], directory[1], frame, *directory[2:])
            await _append_copy_role(session, write_ctx.tables_by_role, pending, "fragment", frag_cells)
        family_identity = SimpleNamespace(
            family_revision_id=family_id, entity_binding_id=entity_binding_id, family_sha256=manifest.family_sha256
        )
        await _emit_family_contexts(session, write_ctx, pending, family_identity, prepared)
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
            root_source_ordinal,
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
    profile_scopes = materialization._profile_scopes(definition, registry.child_collection_slots)
    winner_contract = materialization._winner_candidate_contract(definition, profile_scopes)
    root_profiles = tuple(
        (slot, profile)
        for slot, (profile, scope) in enumerate(zip(definition.selection_profiles, profile_scopes, strict=True), 1)
        if scope.collection_slot == 0
    )
    child_profiles = tuple(
        (slot, profile, scope.collection_slot)
        for slot, (profile, scope) in enumerate(zip(definition.selection_profiles, profile_scopes, strict=True), 1)
        if scope.collection_slot != 0
    )
    query_child_fields = query_scalar_fields(definition, query_collection) if query_collection is not None else ()
    query_child_slot_specs: tuple[tuple[int, object], ...] = ()
    if query_collection is not None and query_child_fields:
        col_index_by_id = {field.field_id: idx for idx, field in enumerate(fields_by_name_map[query_collection])}
        query_child_slot_specs = tuple((col_index_by_id[field.field_id], field) for field in query_child_fields)
    return _AcceptedWriteContext(
        request=request,
        build=build,
        registry=registry,
        binding=binding,
        query_collection=query_collection,
        query_root_fields=query_scalar_fields(definition),
        query_child_fields=query_child_fields,
        query_child_slot_specs=query_child_slot_specs,
        fields_by_name_map=fields_by_name_map,
        sorted_collections=tuple(sorted(binding.layout.collection_slots.items())),
        tables_by_role=tables_by_role,
        key_contract_hash=key_contract_hash,
        limits=limits,
        v2_plan=compile_v2_family_plan(definition, limits),
        winner_contract=winner_contract,
        root_profiles=root_profiles,
        child_profiles=child_profiles,
        context_cache={},
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
        sum(len(slot_children) for slot_children in bundle.raw_children_by_slot.values()) for bundle in accepted_bundles
    )
    family_ids, root_rev_ids = await _allocate_family_and_root_ids(session, len(accepted_bundles))
    child_rev_ids = iter(await _allocate_sequence_ids(session, _CHILD_REV_SEQ, total_children))
    return _AllocatedBundleIds(
        root_ids_by_hash, entity_ids_by_text, total_children, family_ids, root_rev_ids, child_rev_ids
    )


async def _copy_prepared_slice_families(
    session,
    write_ctx: _AcceptedWriteContext,
    schema_name: str,
    prepared_list: list[_PreparedFamilyWrite],
    native_entries,
) -> tuple[int, int]:
    """COPY all encoded families, fragments, contexts, and commitments for one bucket slice."""
    pending = BlockCopyBuffer()
    commitment_rows: list[tuple[object, ...]] = []
    root_scalar_total = child_scalar_total = 0
    for idx, prepared in enumerate(prepared_list):
        native_entry = None if native_entries is None else native_entries[idx]
        root_sc_n, child_sc_n, commitment = await _emit_prepared_family(
            session, write_ctx, pending, prepared, native_entry
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
    return root_scalar_total, child_scalar_total


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
    key_contract_hash = root_key_contract_hash(request.definition)
    allocated = await _intern_and_allocate_bundles(
        session, request.definition, build, binding, accepted_bundles, key_contract_hash
    )
    write_ctx = _build_accepted_write_context(request, build, registry, binding, key_contract_hash, limits)
    children_by_slot: dict[int, int] = defaultdict(int)
    prepared_list, native_inputs = _prepare_slice_family_writes(
        write_ctx, accepted_bundles, allocated, children_by_slot
    )
    native_entries = (
        await asyncio.to_thread(write_ctx.v2_plan.encode_batch, native_inputs)
        if write_ctx.v2_plan is not None and native_inputs
        else None
    )
    root_scalar_total, child_scalar_total = await _copy_prepared_slice_families(
        session, write_ctx, snapshot_schema(binding.family_id), prepared_list, native_entries
    )
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
    slice_rows: list[tuple],
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
) -> list[tuple]:
    await staging._prepare_statement(session)
    if raw_keys is None:
        stmt = text(
            f"SELECT {_STAGE_SELECT_SQL} FROM {schema_sql}.{bucket_name} "
            "WHERE raw_key_sha256 IS NULL ORDER BY source_ordinal, stream_slot"
        )
    else:
        stmt = text(
            f"SELECT {_STAGE_SELECT_SQL} FROM {schema_sql}.{bucket_name} "
            "WHERE raw_key_sha256 = ANY(:raw_keys) ORDER BY source_ordinal, stream_slot"
        ).bindparams(bindparam("raw_keys", raw_keys, type_=ARRAY(LargeBinary)))
    return list((await session.execute(stmt)).tuples().all())


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
        bucket_rows = list(
            (
                await session.execute(
                    text(
                        f"SELECT {_STAGE_SELECT_SQL} FROM {schema_sql}.{bucket_name} ORDER BY source_ordinal, stream_slot"
                    )
                )
            )
            .tuples()
            .all()
        )
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
