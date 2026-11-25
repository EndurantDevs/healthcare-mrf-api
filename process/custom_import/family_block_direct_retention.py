# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retained-family carry-forward for direct compact v2 snapshot builds."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

from sqlalchemy import BigInteger, LargeBinary, Sequence, any_, bindparam, func, select, text
from sqlalchemy.dialects.postgresql import ARRAY, dialect
from sqlalchemy.dialects.postgresql import insert as pg_insert

from db.models.custom_import import (
    CustomImportChildRevision,
    CustomImportEntityBinding,
    CustomImportFamilyChild,
    CustomImportFamilyRevision,
    CustomImportGeneration,
    CustomImportGenerationFamily,
    CustomImportPack,
    CustomImportRootRecord,
    CustomImportRootRevision,
)
from db.models.custom_import_family_block import CustomImportFamilyBlockLayout
from process.custom_import import build_graph as graph
from process.custom_import import build_source as staging
from process.custom_import import runner_codec
from process.custom_import.continuation_runtime import producer_token_sha256
from process.custom_import.family_block_binding import FamilyBlockBinding, FamilyBlockUnavailable, checked_binding
from process.custom_import.family_block_direct_buckets import (
    _COMMITMENT_COLUMNS,
    _copy_extra_table,
    _scalars_dict,
)
from process.custom_import.family_block_direct_stage import DIRECT_COMMITMENTS_TABLE, DIRECT_REJECTED_TABLE
from process.custom_import.family_block_layout import copy_columns
from process.custom_import.family_block_query_input import query_input_table
from process.custom_import.family_block_read import _directory, verified_family
from process.custom_import.family_block_records import FamilyRecord
from process.custom_import.family_block_scalar_layout import pack_query_scalars, query_scalar_fields
from process.custom_import.family_block_v2_codec import (
    encode_family_v2,
    verify_inline_root_family,
    verify_v2_fragments_and_records,
)
from process.custom_import.family_block_write import CONTEXT_COPY_COLUMNS, BlockCopyBuffer, _copy_batch_rows
from process.custom_import.storage_layout import snapshot_schema, snapshot_tables

_DIALECT = dialect()
_PACK_SEQ = Sequence("custom_import_pack_pack_id_seq", schema=CustomImportPack.__table__.schema)
_RETENTION_PAGE_SIZE = 256


@dataclass(frozen=True)
class _RetentionContext:
    request: staging.SourceBuildRequest
    build: object
    registry: object
    binding: FamilyBlockBinding
    limits: object


@dataclass(frozen=True)
class _RetainedFamilyInput:
    family_revision_id: int
    root_record_id: int
    entity_binding_id: int
    inherited_family_sha256: bytes
    root_record: FamilyRecord
    children_by_name: dict[str, list[FamilyRecord]]
    logical_key_sha256: bytes
    canonical_logical_key: str


async def _copy_base_entities(
    session, binding: FamilyBlockBinding, dataset_id: int, entity_ids: tuple[int, ...]
) -> None:
    if not entity_ids:
        return
    entity_tbl = CustomImportEntityBinding.__table__
    target_entity = binding.tables_by_role["entity_binding"]
    cols = copy_columns(target_entity)
    stmt = pg_insert(target_entity).from_select(
        cols,
        select(*(entity_tbl.c[col_name] for col_name in cols)).where(
            entity_tbl.c.dataset_id == dataset_id,
            entity_tbl.c.entity_binding_id == any_(bindparam("ret_eids", entity_ids, type_=ARRAY(BigInteger))),
        ),
        include_defaults=False,
    )
    await staging._prepare_statement(session)
    await session.execute(stmt.on_conflict_do_nothing(index_elements=[target_entity.c.entity_binding_id]))


def _retained_pack_hashes_and_records(
    definition, collection_name: str | None, family_records: list[FamilyRecord], pack_id: int
):
    domain = "root-payload" if collection_name is None else "child-payload"
    col_fields = (
        definition.root_fields
        if collection_name is None
        else runner_codec.fields_by_collection(definition)[collection_name]
    )
    payload_hashes: list[bytes] = []
    reassigned_records: list[FamilyRecord] = []
    for idx, rec in enumerate(family_records):
        payload_hash = (
            rec.sparse.payload_sha256
            if rec.sparse is not None
            else runner_codec.digest_text(domain, runner_codec.record_payload(col_fields, rec.values_by_field))
        )
        payload_hashes.append(payload_hash)
        reassigned_records.append(
            FamilyRecord(rec.revision_id, pack_id, idx, rec.values_by_field, rec.scalars, rec.sparse)
        )
    return payload_hashes, reassigned_records


async def _assign_retained_pack(
    session,
    ctx: _RetentionContext,
    collection_name: str | None,
    family_records: list[FamilyRecord],
    stamp,
) -> list[FamilyRecord]:
    """Allocate a fresh build-scoped pack for retained root or child records."""
    if not family_records:
        return []
    definition = ctx.request.definition
    stream_slot = next(
        ctx.registry.stream_slots[stream.stream_id]
        for stream in definition.source_streams
        if stream.child_collection == collection_name
    )
    pack_table = ctx.binding.tables_by_role["pack"]
    pack_cols = copy_columns(pack_table)
    await staging._prepare_statement(session)
    pack_id = int(await session.scalar(select(_PACK_SEQ.next_value())))
    await staging._prepare_statement(session)
    next_pack_ord = int(
        await session.scalar(
            select(func.coalesce(func.max(pack_table.c.pack_ordinal) + 1, 0)).where(
                pack_table.c.stream_slot == stream_slot
            )
        )
    )
    payload_hashes, reassigned_records = _retained_pack_hashes_and_records(
        definition, collection_name, family_records, pack_id
    )
    label = "root" if collection_name is None else collection_name
    pack_by_col = {
        "pack_id": pack_id,
        "execution_id": ctx.build.execution_id,
        "dataset_id": ctx.build.dataset_id,
        "definition_revision_id": ctx.build.definition_revision_id,
        "schema_revision_id": ctx.build.schema_revision_id,
        "stream_slot": stream_slot,
        "pack_ordinal": next_pack_ord,
        "capture_bundle_id": ctx.build.capture_bundle_id,
        "record_count": len(payload_hashes),
        "pack_sha256": runner_codec.pack_hash(label, payload_hashes),
        "producing_fence": ctx.build.producing_fence,
        "producing_token_sha256": bytes(producer_token_sha256(ctx.request)),
        "created_at": stamp,
    }
    await _copy_extra_table(
        session,
        snapshot_schema(ctx.binding.family_id),
        pack_table.name,
        pack_cols,
        [tuple(pack_by_col[col_name] for col_name in pack_cols)],
    )
    return reassigned_records


async def _append_retained_role(
    session, tables_by_role: dict, pending: BlockCopyBuffer, role: str, cells: tuple
) -> None:
    if batch := pending.append(role, cells):
        await _copy_batch_rows(session, tables_by_role, batch)


async def _emit_retained_query_children(
    session,
    ctx: _RetentionContext,
    tables_by_role: dict,
    pending: BlockCopyBuffer,
    family_identity,
    root_values_map: dict,
    assigned_children: list[FamilyRecord],
) -> None:
    """Append retained query-collection children to query_input and candidate_context."""
    definition = ctx.request.definition
    query_collection = definition.query.child_collection
    query_slot = ctx.binding.layout.collection_slots[query_collection]
    query_child_fields = query_scalar_fields(definition, query_collection)
    collection_fields = runner_codec.fields_by_collection(definition)[query_collection]
    for child_rec in assigned_children:
        child_vals = (
            child_rec.values_by_field
            if child_rec.values_by_field is not None
            else runner_codec.payload_values(
                collection_fields, child_rec.sparse.payload_text, label="retained child payload"
            )
        )
        child_key = (
            child_rec.sparse.key_text
            if child_rec.sparse is not None
            else runner_codec.child_key_document(definition, query_collection, child_vals)
        )
        child_hash = (
            child_rec.sparse.key_sha256
            if child_rec.sparse is not None
            else runner_codec.digest_text("child-key", child_key)
        )
        await _append_retained_role(
            session,
            tables_by_role,
            pending,
            "query_input",
            (
                child_rec.revision_id,
                family_identity.family_revision_id,
                query_slot,
                child_hash,
                child_key,
                *pack_query_scalars(query_child_fields, child_rec.scalars),
            ),
        )
        child_identity = SimpleNamespace(
            child_revision_id=child_rec.revision_id, collection_slot=query_slot, child_key_sha256=child_hash
        )
        child_candidate = graph._candidate(definition, family_identity, root_values_map, child_identity, child_vals)
        for context_by_col in graph._context_values(ctx.request, ctx.registry, ctx.build.build_id, child_candidate):
            await _append_retained_role(
                session,
                tables_by_role,
                pending,
                "candidate_context",
                tuple(context_by_col[col_name] for col_name in CONTEXT_COPY_COLUMNS),
            )


async def _copy_retained_contexts(
    session,
    ctx: _RetentionContext,
    tables_by_role: dict,
    pending: BlockCopyBuffer,
    family_identity,
    root_values_map: dict,
    assigned_children_by_name: dict[str, list[FamilyRecord]],
) -> None:
    definition = ctx.request.definition
    root_candidate = graph._candidate(definition, family_identity, root_values_map)
    for context_by_col in graph._context_values(ctx.request, ctx.registry, ctx.build.build_id, root_candidate):
        await _append_retained_role(
            session,
            tables_by_role,
            pending,
            "candidate_context",
            tuple(context_by_col[col_name] for col_name in CONTEXT_COPY_COLUMNS),
        )
    query_collection = definition.query.child_collection
    if query_collection is not None:
        await _emit_retained_query_children(
            session,
            ctx,
            tables_by_role,
            pending,
            family_identity,
            root_values_map,
            assigned_children_by_name[query_collection],
        )


def _retained_family_cells(
    definition, retained: _RetainedFamilyInput, assigned_root: FamilyRecord, encoded
) -> tuple[object, ...]:
    manifest = encoded.stored_manifest
    return (
        retained.family_revision_id,
        retained.root_record_id,
        assigned_root.revision_id,
        retained.entity_binding_id,
        manifest.family_sha256,
        manifest.storage_sha256,
        True,
        manifest.child_count,
        manifest.fragment_count,
        manifest.raw_bytes,
        manifest.stored_bytes,
        0,
        encoded.root_payload_sha256,
        assigned_root.source_ordinal,
        *pack_query_scalars(query_scalar_fields(definition), assigned_root.scalars),
    )


def _retained_commitment_row(
    definition, retained: _RetainedFamilyInput, assigned_root: FamilyRecord, encoded
) -> tuple[object, ...]:
    manifest = encoded.stored_manifest
    return (
        retained.family_revision_id,
        retained.root_record_id,
        retained.logical_key_sha256,
        runner_codec.root_key_contract_hash(definition),
        retained.canonical_logical_key,
        retained.entity_binding_id,
        manifest.family_sha256,
        manifest.child_count,
        encoded.root_payload_sha256,
        assigned_root.source_ordinal,
        True,
    )


async def _copy_retained_encoded_family(
    session,
    ctx: _RetentionContext,
    retained: _RetainedFamilyInput,
    assigned_root: FamilyRecord,
    assigned_children_by_name: dict[str, list[FamilyRecord]],
) -> tuple[int, int, int]:
    """Encode and COPY one retained family, its fragments, contexts, and commitment row."""
    definition = ctx.request.definition
    tables_by_role = dict(ctx.binding.tables_by_role)
    if definition.query.child_collection is not None:
        tables_by_role["query_input"] = query_input_table(ctx.binding.family_id, ctx.binding.layout)
    tables_by_role["candidate_context"] = snapshot_tables(ctx.binding.family_id)[
        "custom_import_build_candidate_context"
    ]
    root_values_map = (
        assigned_root.values_by_field
        if assigned_root.values_by_field is not None
        else runner_codec.payload_values(
            definition.root_fields, assigned_root.sparse.payload_text, label="retained root payload"
        )
    )
    pending = BlockCopyBuffer()
    with encode_family_v2(
        definition,
        assigned_root,
        assigned_children_by_name,
        retained.inherited_family_sha256,
        limits=ctx.limits,
        retained=True,
    ) as encoded:
        manifest = encoded.stored_manifest
        family_cells = _retained_family_cells(definition, retained, assigned_root, encoded)
        await _append_retained_role(session, tables_by_role, pending, "family", family_cells)
        for directory, frame in encoded.indexed_fragments():
            frag_cells = (retained.family_revision_id, directory[0], directory[1], frame, *directory[2:])
            await _append_retained_role(session, tables_by_role, pending, "fragment", frag_cells)
        family_identity = SimpleNamespace(
            family_revision_id=retained.family_revision_id,
            entity_binding_id=retained.entity_binding_id,
            family_sha256=manifest.family_sha256,
        )
        await _copy_retained_contexts(
            session, ctx, tables_by_role, pending, family_identity, root_values_map, assigned_children_by_name
        )
        if remaining := pending.take():
            await _copy_batch_rows(session, tables_by_role, remaining)
        commitment_row = _retained_commitment_row(definition, retained, assigned_root, encoded)
        await _copy_extra_table(
            session,
            snapshot_schema(ctx.binding.family_id),
            DIRECT_COMMITMENTS_TABLE,
            _COMMITMENT_COLUMNS,
            [commitment_row],
        )
        return manifest.child_count, encoded.root_scalar_count, encoded.child_scalar_count


async def _write_retained_bundle(
    session,
    ctx: _RetentionContext,
    retained: _RetainedFamilyInput,
) -> tuple[int, int, int, dict[int, int]]:
    """Assign fresh build packs and COPY one retained family into compact v2 tables."""
    await staging._prepare_statement(session)
    stamp = await session.scalar(select(func.clock_timestamp()))
    assigned_roots = await _assign_retained_pack(session, ctx, None, [retained.root_record], stamp)
    assigned_children_by_name: dict[str, list[FamilyRecord]] = {}
    counts_by_slot: dict[int, int] = {}
    for collection_name, slot in ctx.binding.layout.collection_slots.items():
        assigned_list = await _assign_retained_pack(
            session, ctx, collection_name, retained.children_by_name.get(collection_name, []), stamp
        )
        assigned_children_by_name[collection_name] = assigned_list
        if assigned_list:
            counts_by_slot[slot] = len(assigned_list)
    child_n, root_sc_n, child_sc_n = await _copy_retained_encoded_family(
        session, ctx, retained, assigned_roots[0], assigned_children_by_name
    )
    return child_n, root_sc_n, child_sc_n, counts_by_slot


async def _read_compact_base_records(
    session,
    definition,
    base_binding: FamilyBlockBinding,
    base_entry: dict,
    *,
    limits,
) -> tuple[FamilyRecord, dict[str, list[FamilyRecord]]]:
    if int(base_entry["fragment_count"]) == 0:
        values_by_field, scalars_by_field = verify_inline_root_family(
            definition, base_entry, entity_canonical_value=base_entry["entity_canonical_value"]
        )
        root_rec = FamilyRecord(
            int(base_entry["root_revision_id"]),
            1,
            int(base_entry["root_source_ordinal"]),
            values_by_field,
            scalars_by_field,
        )
        return root_rec, {name: [] for name in base_binding.layout.collection_slots}
    if int(base_entry["membership_count"]) == 0:
        frag_tbl = base_binding.tables_by_role["fragment"]
        await staging._prepare_statement(session)
        frag_rows = (
            (
                await session.execute(
                    select(frag_tbl)
                    .where(frag_tbl.c.family_revision_id == base_entry["family_revision_id"])
                    .order_by(frag_tbl.c.fragment_ordinal)
                )
            )
            .mappings()
            .all()
        )
        with verify_v2_fragments_and_records(
            definition,
            ((_directory(frag), frag["encoded_fragment"]) for frag in frag_rows),
            base_binding.manifest(base_entry),
            limits=limits,
        ) as verified:
            root_entries = list(verified.records(None))
            return root_entries[0], {
                name: list(verified.records(name)) for name in base_binding.layout.collection_slots
            }
    with await verified_family(session, base_binding, base_entry, limits=limits) as verified:
        root_entries = list(verified.records(None))
        return root_entries[0], {name: list(verified.records(name)) for name in base_binding.layout.collection_slots}


def _make_retained_input(
    base_entry: dict,
    root_meta_by_id: dict[int, tuple[bytes, str]],
    root_rec: FamilyRecord,
    children_by_name: dict[str, list[FamilyRecord]],
) -> _RetainedFamilyInput:
    lhash, lkey = root_meta_by_id[int(base_entry["root_record_id"])]
    return _RetainedFamilyInput(
        family_revision_id=int(base_entry["family_revision_id"]),
        root_record_id=int(base_entry["root_record_id"]),
        entity_binding_id=int(base_entry["entity_binding_id"]),
        inherited_family_sha256=bytes(base_entry["family_sha256"]),
        root_record=root_rec,
        children_by_name=children_by_name,
        logical_key_sha256=lhash,
        canonical_logical_key=lkey,
    )


def _accumulate_retained_counts(
    totals: list[int], counts_by_slot: dict[int, int], bundle_counts: tuple[int, int, int, dict[int, int]]
) -> None:
    child_n, root_sc, child_sc, slots_by_slot = bundle_counts
    totals[0] += 1
    totals[1] += child_n
    totals[2] += root_sc
    totals[3] += child_sc
    for slot, count_val in slots_by_slot.items():
        counts_by_slot[slot] = counts_by_slot.get(slot, 0) + count_val


async def _carry_from_compact_base(
    session,
    ctx: _RetentionContext,
    base_family_id: int,
    root_meta_by_id: dict[int, tuple[bytes, str]],
) -> tuple[int, int, int, int, dict[int, int]]:
    """Carry rejected roots forward from a logged compact v1 or v2 base snapshot in bounded pages."""
    definition = ctx.request.definition
    base_binding = checked_binding(base_family_id, definition, ctx.registry.child_collection_slots)
    base_family_tbl = base_binding.tables_by_role["family"]
    base_entity_tbl = base_binding.tables_by_role["entity_binding"]
    root_ids = tuple(sorted(root_meta_by_id))
    totals = [0, 0, 0, 0]
    counts_by_slot: dict[int, int] = {}
    for page_start in range(0, len(root_ids), _RETENTION_PAGE_SIZE):
        page_ids = root_ids[page_start : page_start + _RETENTION_PAGE_SIZE]
        await staging._prepare_statement(session)
        base_entries = (
            (
                await session.execute(
                    select(base_family_tbl, base_entity_tbl.c.canonical_value.label("entity_canonical_value"))
                    .join(base_entity_tbl, base_entity_tbl.c.entity_binding_id == base_family_tbl.c.entity_binding_id)
                    .where(
                        base_family_tbl.c.root_record_id
                        == any_(bindparam("ret_rids", page_ids, type_=ARRAY(BigInteger)))
                    )
                    .order_by(base_family_tbl.c.root_record_id)
                )
            )
            .mappings()
            .all()
        )
        if not base_entries:
            continue
        await _copy_base_entities(
            session,
            ctx.binding,
            ctx.build.dataset_id,
            tuple({int(entry["entity_binding_id"]) for entry in base_entries}),
        )
        for base_entry in base_entries:
            root_rec, children_by_name = await _read_compact_base_records(
                session, definition, base_binding, base_entry, limits=ctx.limits
            )
            retained_input = _make_retained_input(base_entry, root_meta_by_id, root_rec, children_by_name)
            bundle_counts = await _write_retained_bundle(session, ctx, retained_input)
            _accumulate_retained_counts(totals, counts_by_slot, bundle_counts)
    return totals[0], totals[1], totals[2], totals[3], counts_by_slot


def _legacy_base_tables(base_family_id: int | None) -> tuple:
    if base_family_id is not None:
        base_by_name = snapshot_tables(base_family_id)
        return (
            base_by_name[CustomImportGenerationFamily.__tablename__],
            base_by_name[CustomImportFamilyRevision.__tablename__],
            base_by_name[CustomImportRootRevision.__tablename__],
            base_by_name[CustomImportFamilyChild.__tablename__],
            base_by_name[CustomImportChildRevision.__tablename__],
        )
    return (
        CustomImportGenerationFamily.__table__,
        CustomImportFamilyRevision.__table__,
        CustomImportRootRevision.__table__,
        CustomImportFamilyChild.__table__,
        CustomImportChildRevision.__table__,
    )


async def _read_legacy_base_children(
    session,
    fc_tbl,
    cr_tbl,
    family_revision_id: int,
    binding: FamilyBlockBinding,
    fields_by_name_map: dict,
) -> dict[str, list[FamilyRecord]]:
    name_by_slot_map = {slot: name for name, slot in binding.layout.collection_slots.items()}
    await staging._prepare_statement(session)
    child_entries = (
        (
            await session.execute(
                select(
                    cr_tbl.c.child_revision_id,
                    cr_tbl.c.collection_slot,
                    cr_tbl.c.source_ordinal,
                    cr_tbl.c.child_key_sha256,
                    cr_tbl.c.canonical_child_key,
                    cr_tbl.c.canonical_payload,
                )
                .select_from(fc_tbl)
                .join(cr_tbl, cr_tbl.c.child_revision_id == fc_tbl.c.child_revision_id)
                .where(fc_tbl.c.family_revision_id == family_revision_id)
                .order_by(
                    cr_tbl.c.collection_slot,
                    cr_tbl.c.child_key_sha256,
                    cr_tbl.c.canonical_child_key,
                    cr_tbl.c.canonical_payload,
                )
            )
        )
        .mappings()
        .all()
    )
    children_by_name: dict[str, list[FamilyRecord]] = {name: [] for name in binding.layout.collection_slots}
    for child_entry in child_entries:
        col_name = name_by_slot_map[int(child_entry["collection_slot"])]
        col_fields = fields_by_name_map[col_name]
        child_vals = runner_codec.payload_values(
            col_fields, child_entry["canonical_payload"], label="retained legacy child"
        )
        children_by_name[col_name].append(
            FamilyRecord(
                int(child_entry["child_revision_id"]),
                1,
                int(child_entry["source_ordinal"]),
                child_vals,
                _scalars_dict(col_fields, child_vals),
            )
        )
    return children_by_name


async def _query_legacy_base_page(session, ctx: _RetentionContext, gf_tbl, fr_tbl, rr_tbl, page_ids: tuple[int, ...]):
    await staging._prepare_statement(session)
    return (
        (
            await session.execute(
                select(
                    fr_tbl.c.family_revision_id,
                    fr_tbl.c.root_record_id,
                    fr_tbl.c.root_revision_id,
                    fr_tbl.c.entity_binding_id,
                    fr_tbl.c.family_sha256,
                    rr_tbl.c.canonical_payload,
                    rr_tbl.c.source_ordinal,
                )
                .select_from(gf_tbl)
                .join(fr_tbl, fr_tbl.c.family_revision_id == gf_tbl.c.family_revision_id)
                .join(rr_tbl, rr_tbl.c.root_revision_id == fr_tbl.c.root_revision_id)
                .where(
                    gf_tbl.c.generation_id == ctx.build.base_generation_id,
                    gf_tbl.c.dataset_id == ctx.build.dataset_id,
                    gf_tbl.c.schema_revision_id == ctx.build.schema_revision_id,
                    gf_tbl.c.root_record_id == any_(bindparam("ret_leg_rids", page_ids, type_=ARRAY(BigInteger))),
                )
                .order_by(gf_tbl.c.root_record_id)
            )
        )
        .mappings()
        .all()
    )


async def _carry_from_legacy_base(
    session,
    ctx: _RetentionContext,
    base_family_id: int | None,
    root_meta_by_id: dict[int, tuple[bytes, str]],
) -> tuple[int, int, int, int, dict[int, int]]:
    """Carry rejected roots forward from a legacy base generation in bounded pages."""
    definition = ctx.request.definition
    gf_tbl, fr_tbl, rr_tbl, fc_tbl, cr_tbl = _legacy_base_tables(base_family_id)
    root_ids = tuple(sorted(root_meta_by_id))
    fields_by_name_map = runner_codec.fields_by_collection(definition)
    totals = [0, 0, 0, 0]
    counts_by_slot: dict[int, int] = {}
    for page_start in range(0, len(root_ids), _RETENTION_PAGE_SIZE):
        base_entries = await _query_legacy_base_page(
            session, ctx, gf_tbl, fr_tbl, rr_tbl, root_ids[page_start : page_start + _RETENTION_PAGE_SIZE]
        )
        if not base_entries:
            continue
        entity_ids = tuple({int(entry["entity_binding_id"]) for entry in base_entries})
        await _copy_base_entities(session, ctx.binding, ctx.build.dataset_id, entity_ids)
        for base_entry in base_entries:
            root_vals = runner_codec.payload_values(
                definition.root_fields, base_entry["canonical_payload"], label="retained legacy root"
            )
            root_rec = FamilyRecord(
                int(base_entry["root_revision_id"]),
                1,
                int(base_entry["source_ordinal"]),
                root_vals,
                _scalars_dict(definition.root_fields, root_vals),
            )
            children_by_name = await _read_legacy_base_children(
                session, fc_tbl, cr_tbl, int(base_entry["family_revision_id"]), ctx.binding, fields_by_name_map
            )
            retained_input = _make_retained_input(base_entry, root_meta_by_id, root_rec, children_by_name)
            _accumulate_retained_counts(
                totals, counts_by_slot, await _write_retained_bundle(session, ctx, retained_input)
            )
    return totals[0], totals[1], totals[2], totals[3], counts_by_slot


async def _load_rejected_root_meta(session, build, definition, family_id: int) -> dict[int, tuple[bytes, str]]:
    schema_sql = _DIALECT.identifier_preparer.quote_schema(snapshot_schema(family_id))
    await staging._prepare_statement(session)
    rejected_hashes = [
        bytes(row_hash)
        for row_hash in (
            await session.scalars(
                text(
                    f"SELECT DISTINCT typed_key_sha256 FROM {schema_sql}.{DIRECT_REJECTED_TABLE} "
                    "WHERE typed_key_sha256 IS NOT NULL"
                )
            )
        ).all()
    ]
    if not rejected_hashes:
        return {}
    root_tbl = CustomImportRootRecord.__table__
    await staging._prepare_statement(session)
    root_rows = (
        await session.execute(
            select(root_tbl.c.root_record_id, root_tbl.c.logical_key_sha256, root_tbl.c.canonical_logical_key).where(
                root_tbl.c.dataset_id == build.dataset_id,
                root_tbl.c.key_contract_sha256 == runner_codec.root_key_contract_hash(definition),
                root_tbl.c.logical_key_sha256
                == any_(bindparam("rej_hashes", rejected_hashes, type_=ARRAY(LargeBinary))),
            )
        )
    ).all()
    return {int(rid): (bytes(lhash), lkey) for rid, lhash, lkey in root_rows}


async def carry_retained_families(
    session,
    request: staging.SourceBuildRequest,
    build,
    registry,
    binding: FamilyBlockBinding,
    *,
    limits,
) -> tuple[int, int, int, int, dict[int, int]]:
    """Carry rejected source roots forward from base generation (compact v1/v2 or legacy)."""
    if build.base_generation_id is None:
        return 0, 0, 0, 0, {}
    await staging._prepare_statement(session)
    base_gen = await session.get(CustomImportGeneration, build.base_generation_id)
    if (
        base_gen is None
        or base_gen.dataset_id != build.dataset_id
        or base_gen.schema_revision_id != build.schema_revision_id
    ):
        return 0, 0, 0, 0, {}
    root_meta_by_id = await _load_rejected_root_meta(session, build, request.definition, binding.family_id)
    if not root_meta_by_id:
        return 0, 0, 0, 0, {}
    base_family_id = (
        await staging._call(session, "resolve_custom_import_build_base_snapshot", (("bigint", build.build_id),))
    ).scalar_one()
    await staging._prepare_statement(session)
    base_layout = None if base_family_id is None else await session.get(CustomImportFamilyBlockLayout, base_family_id)
    if base_layout is not None and base_layout.phase != "logged":
        raise FamilyBlockUnavailable("compact base layout must be logged before retention")
    ctx = _RetentionContext(request=request, build=build, registry=registry, binding=binding, limits=limits)
    if base_layout is not None:
        return await _carry_from_compact_base(session, ctx, base_family_id, root_meta_by_id)
    return await _carry_from_legacy_base(session, ctx, base_family_id, root_meta_by_id)
