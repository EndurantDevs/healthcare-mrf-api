# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Register exact retained destination IDs before GRAPH opens a generation.

Homes route observed revisions; they do not establish candidate completeness.
The final SOURCE-plus-retained home comparison remains the owner's obligation.
"""

from collections import deque
from contextlib import closing
from dataclasses import dataclass
from itertools import zip_longest

from process.custom_import import family_shard_retained_allocate as allocation
from process.custom_import.build_source import _page_session, _source_cleanup_failure
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_records import _integer
from process.custom_import.family_shard_admission import read_admission_occurrences
from process.custom_import.family_shard_base_values import read_retained_input
from process.custom_import.family_shard_facts import _sorted
from process.custom_import.family_shard_homes import _append_batch, _batches
from process.custom_import.family_shard_output import _pack_row, _unpack_row
from process.custom_import.family_shard_plan import PlanWork, _cpu, _Scratch, read_family_plan
from process.custom_import.family_shard_retained_output import RetainedDestinationDescriptor, read_retained_destination

_KINDS = {"root_revision": 1, "child_revision": 2, "family_revision": 3, "pack": 4}
_COUNTS = ("root", "child", "family", "pack")
_ROLES = ("root_record", "entity_binding", "pack")


@dataclass(frozen=True)
class RetainedHomeCounts:
    """Retained-only accounting, separate from final candidate verification."""

    root_count: int
    child_count: int
    inserted_root_count: int
    inserted_child_count: int


def _destination_scope(context, destination, work):
    scope = allocation._scope(context, work)
    if (
        type(destination) is not RetainedDestinationDescriptor
        or _pack_row(destination.scope) != _pack_row(scope)
        or destination.definition_sha256 != context.common.request.definition.digest
        or destination.collection_slots != tuple(sorted(context.common.layout.collection_slots.items()))
        or destination.columns_by_role != tuple((role, context.common.columns_by_role[role]) for role in _ROLES)
    ):
        raise FamilyBlockUnavailable("retained homes destination original scope differs")
    return scope


def _originals(context, work, limits):
    prefix = allocation._source_prefix(context, work)
    with closing(
        context.common_output.batches(
            maximum_rows=limits.maximum_batch_records,
            maximum_bytes=min(limits.maximum_batch_bytes, work.limits.memory_bytes),
        )
    ) as batches:
        for _batch in batches:
            work.current()
    count = 0
    with closing(read_admission_occurrences(context.admission, check=work.current)) as occurrences:
        for _occurrence in occurrences:
            count += 1
    if count != context.common.source.source.counts[0]:
        raise FamilyBlockUnavailable("retained homes original SOURCE/admission census differs")
    retained = 0
    with closing(read_family_plan(context.final_plan, check=work.current)) as plans:
        for plan in plans:
            retained += plan.selection_kind == "retained"
    if retained != context.final_plan.counts_by_kind["retained"]:
        raise FamilyBlockUnavailable("retained homes original selection census differs")
    with closing(read_retained_input(context.retained_input, check=work.current)) as entries:
        deque(entries, maxlen=0)
    return prefix


def _allocation(entry):
    _label, attempt, ordinal, name, identity, accepted = entry
    if name not in _KINDS or type(accepted) is not bool:
        raise FamilyBlockUnavailable("retained home observed allocation shape differs")
    _integer(attempt, "retained home allocation attempt", zero=True)
    _integer(ordinal, "retained home allocation ordinal", zero=True)
    _integer(identity, "retained home observed revision")
    return _KINDS[name], identity, accepted


def _assignments(context, destination, work, observed, assigned):
    names = {0: None} | {slot: name for name, slot in destination.collection_slots}
    with (
        observed.open("xb") as observations,
        assigned.open("xb") as assignments,
        closing(read_retained_destination(destination, check=work.current)) as entries,
    ):
        for entry in entries:
            kind = entry[0]
            pair = None
            match kind:
                case "allocation":
                    work.frame(observations, _allocation(entry), observed)
                case "family":
                    pair = 3, entry[5]
                case "record":
                    slot = entry[2]
                    if type(slot) is not int or slot not in names:
                        raise FamilyBlockUnavailable("retained home record collection differs")
                    revision_records = (
                        allocation._record(context.common.request.definition, names[slot], wire, retained=True)
                        for wire in entry[4:6]
                    )
                    before_record, after_record = revision_records
                    if before_record.revision_id == after_record.revision_id:
                        raise FamilyBlockUnavailable("retained home record reuses original revision")
                    pair = 1 if slot == 0 else 2, after_record.revision_id
                case "copy" if entry[1] == "pack":
                    allocation_by_role = dict(
                        zip(context.common.columns_by_role["pack"], _unpack_row(entry[2]), strict=True)
                    )
                    pair = 4, allocation_by_role["pack_id"]
            if pair is not None:
                _integer(pair[1], "retained home assigned revision")
                work.frame(assignments, pair, assigned)


def _accepted(work, path):
    previous = None
    with closing(_sorted(work, work.rows(path), lambda row: row[:2])) as entries:
        for kind, identity, accepted in entries:
            pair = kind, identity
            if pair == previous:
                raise FamilyBlockUnavailable("retained home observed identity repeats")
            previous = pair
            if accepted:
                yield pair


def _matching_ids(work, assigned, observed, expected):
    counts, previous = [0] * len(_COUNTS), None
    with (
        closing(_sorted(work, work.rows(assigned), lambda pair: pair)) as assignments,
        closing(_accepted(work, observed)) as accepted,
    ):
        for assignment, observation in zip_longest(assignments, accepted):
            work.current()
            if assignment != observation or assignment is None:
                raise FamilyBlockUnavailable("retained home accepted/assigned identity bijection differs")
            if assignment == previous:
                raise FamilyBlockUnavailable("retained home assigned identity repeats")
            previous = assignment
            kind, _identity = assignment
            counts[kind - 1] += 1
            if kind in (1, 2):
                yield assignment
    if tuple(counts) != expected:
        raise FamilyBlockUnavailable("retained home accepted identity census differs")


def _stage(context, destination, work, limits):
    prefix = _originals(context, work, limits)
    if destination.source_pack_prefix != prefix:
        raise FamilyBlockUnavailable("retained homes SOURCE pack prefix differs")
    original = context.retained_input.counts_by_role
    counts_by_role = dict(destination.counts)
    for name, count in counts_by_role.items():
        _integer(count, "retained home " + name + " count", zero=True)
    if any(counts_by_role[name] != original[name] for name in ("family", "root", "child", "scalar")) or (
        counts_by_role["original_pack"],
        counts_by_role["original_origin"],
    ) != (original["pack"], original["origin"]):
        raise FamilyBlockUnavailable("retained homes original/destination census differs")
    observed, assigned = work.path(), work.path()
    _assignments(context, destination, work, observed, assigned)
    path = work.write(_matching_ids(work, assigned, observed, tuple(counts_by_role[name] for name in _COUNTS)))
    work.remove(observed)
    work.remove(assigned)
    return path, (counts_by_role["root"], counts_by_role["child"]), prefix


async def _register_batch(factory, context, destination, owner_work, prefix, batch, check):
    _destination_scope(context, destination, owner_work)
    check()
    await allocation._check_base(context)
    async with _page_session(factory, context.common.request, context.build_id) as (session, _build):
        await allocation._guard(session, context, prefix)
        check()
        inserted = await _append_batch(session, context.common.source.snapshot_family_id, batch)
        await allocation._check_base(context)
        check()
        return inserted


async def register_retained_homes(factory, context, destination, *, work):
    """Append only missing accepted destination IDs after complete original EOF.

    Each batch reacquires the current GRAPH owner and live base, including empty
    input. Committed batches survive interruption; a retry verifies every closed
    input again and registers only the missing observed IDs, never sequence gaps.
    """
    _destination_scope(context, destination, work)
    scratch = _Scratch(PlanWork(work.root, work.limits.spool, work.register_cleanup, work.check))
    primary, result = None, None
    try:
        await allocation._check_base(context)
        path, counts, prefix = await _cpu(scratch, _stage, context, destination, scratch, work.limits)
        inserted_roots = inserted_children = 0
        with closing(_batches(scratch, path, work.limits)) as batches:
            while (batch := await _cpu(scratch, next, batches, None)) is not None:
                inserted = await _register_batch(factory, context, destination, work, prefix, batch, scratch.current)
                inserted_roots += inserted[0]
                inserted_children += inserted[1]
        result = RetainedHomeCounts(*counts, inserted_roots, inserted_children)
    except BaseException as error:
        primary = error
    try:
        scratch.close()
    except BaseException as error:
        primary = _source_cleanup_failure(primary, error)
    if primary is not None:
        raise primary
    return result
