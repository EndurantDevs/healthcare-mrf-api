# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Carry the few rejected families selected from a sealed compact base."""

from dataclasses import dataclass, replace
from itertools import chain, islice
from types import SimpleNamespace

from sqlalchemy import Sequence, func, select
from sqlalchemy.util.concurrency import await_

from db.models.custom_import import CustomImportBuildFamily, CustomImportBuildStream, CustomImportPack
from process.custom_import import runner_codec
from process.custom_import.build_source import _flush_page, _page_session, _prepare_statement
from process.custom_import.bulk_page_codec import MAX_BATCH_BYTES, MAX_BATCH_ROWS
from process.custom_import.family_block_binding import FamilyBlockUnavailable, checked_binding
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_identity import _ENTITY, _ROOT, _copy_identities
from process.custom_import.family_block_layout import copy_columns
from process.custom_import.family_block_read import verified_family
from process.custom_import.family_block_scalar_layout import query_scalar_fields
from process.custom_import.family_block_source import _stage_child, _stage_contexts
from process.custom_import.family_block_write import (
    AllocatedBlockFamily,
    AllocatedChildCollection,
    BlockCopyBuffer,
    _batch_tables,
    _check_batch,
    _copy_batch_rows,
    _lock_open_layout,
    pack_family_rows,
)
from process.custom_import.storage_layout import snapshot_tables

_PACK_IDS = Sequence("custom_import_pack_pack_id_seq", schema=CustomImportPack.__table__.schema)
_PACK_ROWS = 1024


def _remaining_limits(limits, resident_bytes):
    available = limits.max_spool_bytes - resident_bytes
    if available < max(limits.max_record_bytes, limits.memory_bytes, limits.fragment_bytes):
        raise FamilyBlockResourceLimit("retained spool exceeds the shared budget")
    return replace(limits, max_spool_bytes=available)


@dataclass(frozen=True)
class _RetainedPage:
    request: object
    registry: object
    binding: object
    base: object
    build: object
    cursor_by_slot: dict
    stamp: object
    limits: object


def _retained_records(page, verified, collection, pack_ids, append, *, observe=None):
    """Reassign records to bounded packs under the current producer scope."""
    stream_slot = next(
        page.registry.stream_slots[stream.stream_id]
        for stream in page.request.definition.source_streams
        if stream.child_collection == collection
    )
    cursor = page.cursor_by_slot[stream_slot]
    columns = copy_columns(page.binding.tables_by_role["pack"])
    producer_by_column = {
        name: getattr(page.build, name)
        for name in (
            "execution_id",
            "dataset_id",
            "definition_revision_id",
            "schema_revision_id",
            "capture_bundle_id",
            "producing_fence",
        )
    }
    producer_by_column["producing_token_sha256"] = bytes(page.build.producing_token_sha256)
    label = collection if collection is not None else "root"
    domain = "root-payload" if collection is None else "child-payload"
    incoming = iter(verified.decoded_records(collection))
    while first := next(incoming, None):
        pack_id, hashes = next(pack_ids), []
        for ordinal, (original, _wire, parts) in enumerate(chain((first,), islice(incoming, _PACK_ROWS - 1))):
            hashes.append(runner_codec.digest_text(domain, parts[2]))
            assigned = replace(original, pack_id=pack_id, source_ordinal=ordinal)
            if observe is not None:
                observe(assigned, parts)
            yield assigned
        pack_by_column = producer_by_column | {
            "created_at": page.stamp,
            "pack_id": pack_id,
            "stream_slot": stream_slot,
            "pack_ordinal": cursor.next_pack_ordinal,
            "record_count": len(hashes),
            "pack_sha256": runner_codec.pack_hash(label, hashes),
        }
        append("pack", tuple(pack_by_column[name] for name in columns))
        cursor.next_pack_ordinal += 1


def _repack(page, verified, family_row, counts_by_slot, pack_ids, append):
    """Encode unchanged source values and stage the current query fields."""
    definition = page.request.definition
    query = definition.query.child_collection
    query_fields = query_scalar_fields(definition, query) if query is not None else ()
    slots = page.registry.child_collection_slots
    family_id = family_row["family_revision_id"]
    identity = SimpleNamespace(
        family_revision_id=family_id,
        entity_binding_id=family_row["entity_binding_id"],
        family_sha256=bytes(family_row["family_sha256"]),
    )
    packed_roots = tuple(_retained_records(page, verified, None, pack_ids, append))
    (root,) = packed_roots
    if root.revision_id != family_row["root_revision_id"]:
        raise FamilyBlockUnavailable("retained compact root differs from its family")
    root_values = root.values_by_field
    if root_values is None:
        root_values = runner_codec.payload_values(
            definition.root_fields, root.sparse.payload_text, label="retained compact payload"
        )
    root_scope = SimpleNamespace(plan=SimpleNamespace(build_id=page.build.build_id), values=root_values)
    _stage_contexts(append, page.request, page.registry, root_scope, identity)

    def _stage_projection(child_record, parts):
        key_hash, key, _ = parts
        child = SimpleNamespace(
            child_revision_id=child_record.revision_id,
            collection_slot=slots[query],
            child_key_sha256=key_hash,
            canonical_child_key=key,
        )
        _stage_child(append, page.request, page.registry, root_scope, identity, query_fields, child, child_record)

    children_by_collection = {
        name: AllocatedChildCollection(
            counts_by_slot.get(slot, 0),
            _retained_records(
                page, verified, name, pack_ids, append, observe=_stage_projection if name == query else None
            ),
        )
        for name, slot in slots.items()
    }
    return AllocatedBlockFamily(
        family_id,
        family_row["root_record_id"],
        identity.entity_binding_id,
        identity.family_sha256,
        root,
        children_by_collection,
    )


async def _retained_collection_counts(session, runs, family_row):
    await _prepare_statement(session)
    counts_by_slot = {
        slot: int(count)
        for slot, count in (
            await session.execute(
                select(runs.c.collection_slot, func.sum(runs.c.record_count))
                .where(runs.c.family_revision_id == family_row["family_revision_id"])
                .group_by(runs.c.collection_slot)
            )
        ).all()
    }
    if counts_by_slot.get(0) != 1 or sum(counts_by_slot.values()) != family_row["child_count"] + 1:
        raise FamilyBlockUnavailable("retained compact family has an invalid census")
    return counts_by_slot


async def _copy_family(session, page, family_row):
    counts_by_slot = await _retained_collection_counts(session, page.base.tables_by_role["revision_run"], family_row)
    pack_count = sum((count + _PACK_ROWS - 1) // _PACK_ROWS for count in counts_by_slot.values())
    await _prepare_statement(session)
    pack_ids = iter(
        (await session.scalars(select(_PACK_IDS.next_value()).select_from(func.generate_series(1, pack_count)))).all()
    )
    binding = page.binding
    await _copy_identities(
        session, binding.tables_by_role["root_record"], _ROOT, (family_row["root_record_id"],), page.build.dataset_id
    )
    await _copy_identities(
        session,
        binding.tables_by_role["entity_binding"],
        _ENTITY,
        (family_row["entity_binding_id"],),
        page.build.dataset_id,
    )
    await _prepare_statement(session)
    with await verified_family(session, page.base, family_row, limits=page.limits) as verified:
        output_limits = _remaining_limits(page.limits, family_row["raw_bytes"])

        def _encode(_sync):
            pending = BlockCopyBuffer()
            scalar_counts = []

            def _write(batch):
                tables = _batch_tables(binding.family_id, binding.layout, batch, True)
                _check_batch(tables, batch, row_limit=MAX_BATCH_ROWS, byte_limit=MAX_BATCH_BYTES)
                await_(_copy_batch_rows(session, tables, batch))

            def _append(role, cells):
                if batch := pending.append(role, cells):
                    _write(batch)

            family = _repack(page, verified, family_row, counts_by_slot, pack_ids, _append)
            for role, cells in pack_family_rows(
                binding.layout,
                family,
                retained=True,
                limits=output_limits,
                account=lambda *counts: scalar_counts.extend(counts),
            ):
                _append(role, cells)
            if next(pack_ids, None) is not None:
                raise FamilyBlockUnavailable("retained compact pack allocation differs")
            if batch := pending.take():
                _write(batch)
            return tuple(scalar_counts)

        return await session.run_sync(_encode)


async def copy_retained_families(session_factory, request, registry, build_id, binding, base_family, limits):
    """Read only selected base blocks and write bounded candidate pages."""
    base = checked_binding(base_family.family_id, request.definition, registry.child_collection_slots)
    family = base.tables_by_role["family"]
    plan = snapshot_tables(binding.family_id)[CustomImportBuildFamily.__tablename__]
    stream = CustomImportBuildStream
    roots = children = after = 0
    while True:
        async with _page_session(session_factory, request, build_id) as (session, build):
            await _lock_open_layout(session, build, binding.family_id, binding.layout)
            await _prepare_statement(session)
            page = (
                (
                    await session.execute(
                        select(family)
                        .join(
                            plan,
                            (
                                (plan.c.base_family_revision_id == family.c.family_revision_id)
                                & (plan.c.root_record_id == family.c.root_record_id)
                            ),
                        )
                        .where(
                            plan.c.build_id == build_id,
                            plan.c.selection_kind == "retained",
                            plan.c.root_record_id > after,
                        )
                        .order_by(plan.c.root_record_id)
                        .limit(request.page_row_limit)
                    )
                )
                .mappings()
                .all()
            )
            if not page:
                return roots, children
            if (base_family.generation_id, base_family.definition_revision_id, base_family.schema_revision_id) != (
                build.base_generation_id,
                build.definition_revision_id,
                build.schema_revision_id,
            ):
                raise FamilyBlockUnavailable("retained compact families require the current schema")
            await _prepare_statement(session)
            cursor_by_slot = {
                entry.stream_slot: entry
                for entry in (await session.scalars(select(stream).where(stream.build_id == build_id))).all()
            }
            await _prepare_statement(session)
            stamp = await session.scalar(select(func.clock_timestamp()))
            scope = _RetainedPage(request, registry, binding, base, build, cursor_by_slot, stamp, limits)
            for family_row in page:
                root_count, child_count = await _copy_family(session, scope, family_row)
                roots += root_count
                children += child_count
            await _flush_page(session)
            after = page[-1]["root_record_id"]
