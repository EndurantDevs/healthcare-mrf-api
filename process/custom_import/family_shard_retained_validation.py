# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compare pinned originals with complete retained assignments, never native output.

Accepted IDs prove allocation only. This comparison independently joins every
original family, record, scalar declaration and provenance frame to those IDs.
Stored candidate families, native COPY and winner contexts still require proof.
"""

from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime
from itertools import groupby, zip_longest

import msgpack

from process.custom_import import family_shard_retained_allocate as allocation
from process.custom_import import family_shard_retained_homes as homes
from process.custom_import import runner_codec as codec
from process.custom_import.build_source import _source_cleanup_failure
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_records import (
    _fields,
    _integer,
    _scalar_wire,
    _scalars_from_wire,
    _text,
    record_from_wire,
    record_parts,
)
from process.custom_import.family_block_validate import _equal_native
from process.custom_import.family_shard_base_values import read_retained_input
from process.custom_import.family_shard_facts import _sorted
from process.custom_import.family_shard_output import _pack_row, _unpack_row
from process.custom_import.family_shard_plan import PlanWork, _cpu, _plan, _plan_cells, _Scratch, read_family_plan
from process.custom_import.family_shard_retained_output import read_retained_destination

_MISSING = object()
_ROLES = ("family", "root", "child", "scalar", "pack", "origin")


@dataclass(frozen=True)
class RetainedOriginalComparison:
    """Complete owner-spool equality; no native or stored-output acceptance."""

    scope: tuple
    destination_sha256: bytes
    counts: tuple
    root_scalar_count: int
    child_scalar_count: int
    unproven_boundaries: tuple = field(
        default=("native_output", "stored_output_families", "winner_contexts"), init=False
    )


def _row(key, value):
    return key, _pack_row(value)


def _collection_names(slots):
    return {0: None} | {slot: name for name, slot in slots}


def _evidence(cells, scalars, lineage, collection, order):
    # Native scalar scale/offset is semantic; payload/key spelling is byte exact.
    scalar_cells = tuple(
        (name, scalar.field_type, scalar.value_state, scalar.logical_value) for name, scalar in sorted(scalars.items())
    )
    return (
        tuple(cells[:3]),
        cells[3],
        codec.digest_text(("root" if collection is None else "child") + "-payload", cells[3]),
        order,
        cells[5],
        scalar_cells,
        None if lineage is None else _unpack_row(lineage),
    )


def _record_evidence(definition, collection, wire, lineage):
    cells = msgpack.unpackb(wire, raw=False)
    record, parts = record_from_wire(definition, collection, cells, retained=True)
    order = () if collection is None else parts
    return record, parts, _evidence(cells, record.scalars, lineage, collection, order)


def _outer_record(definition, collection, wire):
    cells = msgpack.unpackb(wire, raw=False)
    if type(cells) is not list or len(cells) != 6:
        raise FamilyBlockUnavailable("retained comparison record shape differs")
    for index, label in enumerate(("revision identity", "pack identity", "source ordinal")):
        _integer(cells[index], label, zero=index == 2)
    _text(cells[3])
    fields_by_name = {field.field_id: field for field in _fields(definition, collection)}
    scalars = _scalars_from_wire(cells[4], fields_by_name)
    if _scalar_wire(scalars, fields_by_name) != cells[4]:
        raise FamilyBlockUnavailable("retained comparison scalar encoding differs")
    return cells, scalars


def _selected_families(context, work):
    with closing(read_family_plan(context.final_plan, check=work.current)) as plans:
        for plan in plans:
            if plan.selection_kind != "retained":
                continue
            base = plan.base_header
            if base is None:
                raise FamilyBlockUnavailable("retained comparison plan has no original family")
            yield _row(
                ("selected_family", base.family_revision_id),
                (
                    (plan.typed_key[1], plan.typed_key[0]),
                    plan.root_record_id,
                    _plan_cells(plan),
                    base.family_revision_id,
                    base.entity_binding_id,
                    base.family_sha256,
                    base.child_count,
                ),
            )


def _provenance(role, packed):
    native = _unpack_row(packed)
    if role == "pack":
        key = ("pack", native["pack_pack_id"])
    else:
        key = ("origin", native["family_revision_id"], native["kind"], native["revision_id"])
    return _row(key, native)


def _original_rows(context, work):
    descriptor = context.retained_input
    definition = (
        CustomImportDefinition.from_json(descriptor.definition_canonical) if descriptor.definition_canonical else None
    )
    names = _collection_names(descriptor.collection_slots)
    yield from _selected_families(context, work)
    with closing(read_retained_input(descriptor, check=work.current)) as entries:
        for entry in entries:
            if entry[0] == "family":
                identity, header, native = entry[1]
                yield _row(("original_family", identity), (header, _unpack_row(native)))
                continue
            if entry[0] == "record":
                _kind, family, slot, wire, lineage = entry
                if definition is None or type(slot) is not int or slot not in names:
                    raise FamilyBlockUnavailable("retained comparison original collection differs")
                record, _parts, evidence = _record_evidence(definition, names[slot], wire, lineage)
                yield _row(("record", family, slot, record.revision_id), evidence)
                continue
            if entry[0] in {"pack", "origin"}:
                yield _provenance(*entry)


def _family_rows(entry):
    _kind, order, root, plan_cells, old, fresh, entity, digest, count, original = entry
    plan = _plan(plan_cells)
    if plan.selection_kind != "retained" or plan.base_header is None:
        raise FamilyBlockUnavailable("retained comparison destination plan differs")
    _integer(fresh, "retained assigned family")
    if type(original) is tuple:
        if len(original) != 2 or type(original[0]) is not int or original[0] != root:
            raise FamilyBlockUnavailable("retained comparison original root wrapper differs")
        original = original[1]
    yield _row(("selected_family", old), (order, root, plan_cells, old, entity, digest, count))
    yield _row(("original_family", old), (tuple(vars(plan.base_header).values()), _unpack_row(original)))


def _assigned_record(context, entry, family, previous):
    _kind, order, slot, declared_order, original, destination, lineage = entry
    names = _collection_names(tuple(context.common.layout.collection_slots.items()))
    if family is None or order != family[1] or type(slot) is not int or slot not in names:
        raise FamilyBlockUnavailable("retained comparison record has no selected family")
    definition, collection = context.common.request.definition, names[slot]
    before, before_scalars = _outer_record(definition, collection, original)
    after, after_scalars = _outer_record(definition, collection, destination)
    expected_order = () if slot == 0 else declared_order
    if before[0] == after[0] or not _equal_native(
        (before[3], before[5], before_scalars), (after[3], after[5], after_scalars)
    ):
        raise FamilyBlockUnavailable("retained comparison changes original content")
    order_key = slot, expected_order
    if declared_order != expected_order or previous is not None and order_key < previous:
        raise FamilyBlockUnavailable("retained comparison record order differs")
    if slot == 0 and after[2] != 0:
        raise FamilyBlockUnavailable("retained comparison root ordinal differs")
    evidence = _evidence(before, before_scalars, lineage, collection, expected_order)
    return tuple(before[:3]), tuple(after[:3]), evidence, order_key


def _destination_rows(context, destination, work, members, packs, ordinals):
    family, previous_family, previous_record = None, None, None
    roots = children = 0
    with (
        members.open("xb") as member_output,
        packs.open("xb") as pack_output,
        ordinals.open("xb") as ordinal_output,
        closing(read_retained_destination(destination, check=work.current)) as entries,
    ):
        for entry in entries:
            kind = entry[0]
            if kind == "family":
                _family_eof(family, roots, children)
                family, roots, children, previous_record = entry, 0, 0, None
                if previous_family is not None and family[1] <= previous_family:
                    raise FamilyBlockUnavailable("retained comparison family order repeats")
                previous_family = family[1]
                yield from _family_rows(entry)
                continue
            if kind == "record":
                before, after, evidence, previous_record = _assigned_record(context, entry, family, previous_record)
                slot, old_family = entry[2], family[4]
                roots, children = roots + (slot == 0), children + (slot != 0)
                work.frame(
                    member_output,
                    (after[1], after[2], old_family, slot, evidence[2]),
                    members,
                )
                yield _row(("record", old_family, slot, before[0]), evidence)
                continue
            if kind == "original":
                yield _provenance(entry[1], entry[2])
                continue
            if kind == "copy" and entry[1] == "pack":
                native_by_name = dict(zip(context.common.columns_by_role["pack"], _unpack_row(entry[2]), strict=True))
                work.frame(pack_output, (native_by_name["pack_id"], entry[2]), packs)
                work.frame(ordinal_output, (native_by_name["stream_slot"], native_by_name["pack_ordinal"]), ordinals)
        _family_eof(family, roots, children)


def _family_eof(family, roots, children):
    if family is not None and (roots != 1 or children != family[8]):
        raise FamilyBlockUnavailable("retained comparison complete family census differs")


def _compare(work, expected, actual):
    previous, counts = None, dict.fromkeys((*_ROLES, "root_scalar", "child_scalar"), 0)
    with (
        closing(_sorted(work, expected, lambda row: row[0])) as originals,
        closing(_sorted(work, actual, lambda row: row[0])) as destinations,
    ):
        for old, fresh in zip_longest(originals, destinations, fillvalue=_MISSING):
            work.current()
            if old is _MISSING or fresh is _MISSING or old[0] != fresh[0]:
                raise FamilyBlockUnavailable("retained original/destination complete membership differs")
            key, value = old
            native = _unpack_row(value)
            if key == previous or not _equal_native(native, _unpack_row(fresh[1])):
                raise FamilyBlockUnavailable("retained original/destination semantic evidence differs")
            previous = key
            role = key[0]
            if role == "selected_family":
                counts["family"] += 1
                continue
            if role == "record":
                scope = "root" if key[2] == 0 else "child"
                counts[scope] += 1
                counts["scalar"] += len(native[5])
                counts[scope + "_scalar"] += len(native[5])
                continue
            if role in {"pack", "origin"}:
                counts[role] += 1
    return counts


def _pack_members(context, work, members):
    names = _collection_names(tuple(context.common.layout.collection_slots.items()))
    with closing(_sorted(work, work.rows(members), lambda row: row[:2])) as records:
        for identity, group in groupby(records, lambda row: row[0]):
            hashes, scope = [], None
            for _pack, ordinal, family, slot, digest in group:
                if ordinal != len(hashes) or scope is not None and scope != (family, slot):
                    raise FamilyBlockUnavailable("retained pack member position or family differs")
                if (len(hashes) + 1) * 128 > work.limits.memory_bytes:
                    raise FamilyBlockResourceLimit("retained pack hash inputs exceed the caller memory budget")
                hashes.append(digest)
                scope = family, slot
                work.current()
            collection = names[scope[1]]
            if collection is None and len(hashes) != 1:
                raise FamilyBlockUnavailable("retained root pack has multiple records")
            yield identity, collection, len(hashes), codec.pack_hash(collection or "root", hashes)


def _pack_producer(context):
    request = context.common.request
    return dict(
        execution_id=request.execution_id,
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        producing_fence=request.fence,
        producing_token_sha256=allocation.producer_token_sha256(request),
        capture_bundle_id=dict(context.common.source.source.scope[0])["capture_bundle_id"],
        created_at=datetime.fromisoformat(context.common_output.proof_inputs["owner_created_at"]),
    )


def _compare_packs(context, destination, work, members, packs, ordinals):
    slots_by_stream = dict(context.common.source.source.stream_slots)
    producer, previous = _pack_producer(context), None
    with closing(_pack_members(context, work, members)) as groups:
        with closing(_sorted(work, work.rows(packs), lambda row: row[0])) as copies:
            for expected, copied in zip_longest(groups, copies, fillvalue=_MISSING):
                if expected is _MISSING or copied is _MISSING or expected[0] != copied[0] or copied[0] == previous:
                    raise FamilyBlockUnavailable("retained COPY pack complete membership differs")
                identity, collection, count, digest = expected
                stream = next(
                    declaration
                    for declaration in context.common.request.definition.source_streams
                    if declaration.child_collection == collection
                )
                native_by_name = dict(zip(context.common.columns_by_role["pack"], _unpack_row(copied[1]), strict=True))
                expected_by_name = dict(
                    producer,
                    pack_id=identity,
                    stream_slot=slots_by_stream[stream.stream_id],
                    record_count=count,
                    pack_sha256=digest,
                )
                if any(
                    not _equal_native(native_by_name[name], expected_cell)
                    for name, expected_cell in expected_by_name.items()
                ):
                    raise FamilyBlockUnavailable("retained COPY pack content or producer differs")
                previous = identity
    _pack_ordinals(destination, work, ordinals)


def _pack_ordinals(destination, work, ordinals):
    tails_by_stream = dict(destination.source_pack_prefix)
    with closing(_sorted(work, work.rows(ordinals), lambda row: row)) as ordinal_rows:
        for slot, ordinal in ordinal_rows:
            if (
                type(slot) is not int
                or slot not in tails_by_stream
                or type(ordinal) is not int
                or not 0 <= ordinal < 2**31
                or ordinal != tails_by_stream[slot]
            ):
                raise FamilyBlockUnavailable("retained COPY pack stream ordinal differs")
            tails_by_stream[slot] += 1
    if tuple(sorted(tails_by_stream.items())) != destination.destination_pack_tail:
        raise FamilyBlockUnavailable("retained COPY pack complete tail differs")


def _stage(context, destination, work, limits):
    home_ids, _counts, _prefix = homes._stage(context, destination, work, limits)
    with closing(work.rows(home_ids)) as identities:
        for _identity in identities:
            work.current()
    work.remove(home_ids)
    members, packs, ordinals = work.path(), work.path(), work.path()
    counts = _compare(
        work,
        _original_rows(context, work),
        _destination_rows(context, destination, work, members, packs, ordinals),
    )
    if {role: counts[role] for role in _ROLES} != context.retained_input.counts_by_role:
        raise FamilyBlockUnavailable("retained comparison original complete census differs")
    _compare_packs(context, destination, work, members, packs, ordinals)
    return tuple((role, counts[role]) for role in _ROLES), counts["root_scalar"], counts["child_scalar"]


async def verify_retained_destination(context, destination, *, work):
    """Exhaust independent originals and destination spools under the live base pin.

    No prefix, ID ledger or returned comparison proves stored output or winners.
    The owner must retain the original artifacts through that later native proof.
    """
    scope = homes._destination_scope(context, destination, work)
    scratch = _Scratch(PlanWork(work.root, work.limits.spool, work.register_cleanup, work.check))
    primary, comparison = None, None
    try:
        await allocation._check_base(context)
        counts, root_scalars, child_scalars = await _cpu(scratch, _stage, context, destination, scratch, work.limits)
        await allocation._check_base(context)
        scratch.current()
        comparison = RetainedOriginalComparison(scope, destination.sha256, counts, root_scalars, child_scalars)
    except BaseException as error:
        primary = error
    try:
        scratch.close()
    except BaseException as error:
        primary = _source_cleanup_failure(primary, error)
    if primary is not None:
        raise primary
    return comparison
