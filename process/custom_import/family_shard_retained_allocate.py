# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Owner-only retained destination preparation from independent closed originals.

Existing native sequences allocate every observed ID. Logical pack membership
and positions remain the original legacy page decisions; this module never
advances stream counters, runs COPY, manufactures verification or publishes.
"""

import struct
from collections import deque
from contextlib import ExitStack, closing
from dataclasses import dataclass, replace
from types import SimpleNamespace

from sqlalchemy import select

from db.models import custom_import as models
from process.custom_import import build_graph as graph
from process.custom_import import family_shard_retained_pages as pages
from process.custom_import import runner_codec as codec
from process.custom_import.build_source import _WINDOW, _drain_cleanup, _page_session
from process.custom_import.bulk_page_codec import MAX_BATCH_BYTES, MAX_BATCH_ROWS, LandingRow
from process.custom_import.continuation_runtime import producer_token_sha256
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_materialization import logical_child, logical_root, logical_scalars
from process.custom_import.family_block_records import record_parts
from process.custom_import.family_block_store import _candidate, _execute
from process.custom_import.family_shard_admission import AdmissionOutputDescriptor, read_admission_occurrences
from process.custom_import.family_shard_allocate import SourceAllocationLimits, read_source_allocations
from process.custom_import.family_shard_base import PinnedBase, _read_deadline, _require_deadline
from process.custom_import.family_shard_base_values import (
    RetainedInputDescriptor,
    RetainedWork,
    read_retained_input,
)
from process.custom_import.family_shard_base_values import (
    _scope as retained_scope,
)
from process.custom_import.family_shard_common import SourceCommonContext, SourceCommonOutput
from process.custom_import.family_shard_facts import SourceFactLimits, _sorted
from process.custom_import.family_shard_graph_control import _plan_scope
from process.custom_import.family_shard_output import AllocatedFamily, _pack_row, _unpack_row
from process.custom_import.family_shard_plan import (
    BaseFamilyHeader,
    FinalPlanDescriptor,
    PlanWork,
    _cpu,
    _plan,
    _plan_cells,
    _Scratch,
    read_family_plan,
)
from process.custom_import.family_shard_retained_output import (
    RetainedDestinationDescriptor as RetainedDestinationDescriptor,
)
from process.custom_import.family_shard_retained_output import (
    _assigned_wire as _assigned_wire,
)
from process.custom_import.family_shard_retained_output import (
    _dictionary_columns,
    _original_root,
    _Output,
    _record,
    _wire,
    _write_complete,
)
from process.custom_import.family_shard_retained_output import (
    _unique_observed as _unique_observed,
)
from process.custom_import.family_shard_retained_output import (
    cleanup_retained_destination as cleanup_retained_destination,
)
from process.custom_import.family_shard_retained_output import (
    read_retained_destination as read_retained_destination,
)
from process.custom_import.family_shard_retained_output import (
    retained_destination_families as retained_destination_families,
)
from process.custom_import.family_shard_retained_reservations import RetainedControlWindow, RetainedReservations
from process.custom_import.family_shard_selection import _record_values
from process.custom_import.family_shard_source_values import source_family_inputs
from process.custom_import.runner_registry import load_registry
from process.custom_import.runner_types import CandidateRunnerError

_CONTRACT = "custom-import/retained-destination/v1"
_FILE = "retained-destination.mpk"
_LENGTH = struct.Struct("!Q")
_ROLES = ("root_record", "entity_binding", "pack")
_RAW_PARENT_INDEX = LandingRow._fields[1:].index("raw_parent_key_canonical")


@dataclass(frozen=True)
class RetainedAllocationContext:
    """All original owners remain live; no ID/header substitutes for their proof."""

    common: SourceCommonContext
    build_id: int
    admission: AdmissionOutputDescriptor
    final_plan: FinalPlanDescriptor
    retained_input: RetainedInputDescriptor
    base: PinnedBase | None
    common_output: SourceCommonOutput
    events: object = None


@dataclass(frozen=True)
class RetainedAllocationWork:
    root: object
    limits: SourceAllocationLimits
    register_cleanup: object
    check: object = None

    def __post_init__(self):
        if type(self.limits) is not SourceAllocationLimits:
            raise ValueError("retained allocation requires its existing owner resource limits")
        PlanWork(self.root, self.limits.spool, self.register_cleanup, self.check)


def _require_common_provenance(context):
    proof = context.common_output.proof_inputs
    source_plan, request = context.common.source, context.common.request
    if (
        proof["source_plan_sha256"],
        proof["allocation_sha256"],
        proof["family_id"],
        proof["execution_id"],
        proof["producing_fence"],
        proof["producing_token_sha256"],
        proof["complete_scope"],
        proof["source_records"],
        proof["source_initial_rejections"],
    ) != (
        source_plan.source.sha256.hex(),
        source_plan.sha256.hex(),
        source_plan.snapshot_family_id,
        request.execution_id,
        request.fence,
        producer_token_sha256(request).hex(),
        request.complete_scope,
        source_plan.source.counts[0],
        source_plan.source.counts[4],
    ):
        raise FamilyBlockUnavailable("retained allocation SOURCE common provenance differs")


def _scope(context, work):
    """Resolve retained allocation scope without relaxing original owner authority."""
    if type(context) is not RetainedAllocationContext or type(work) is not RetainedAllocationWork:
        raise ValueError("retained allocation requires explicit typed original owners")
    if type(context.build_id) is not int or not 0 < context.build_id < 2**63:
        raise ValueError("retained allocation build identity differs")
    if (
        type(context.common_output) is not SourceCommonOutput
        or type(context.retained_input) is not RetainedInputDescriptor
    ):
        raise FamilyBlockUnavailable("retained allocation requires closed original/common outputs")
    _plan_scope(context.common, context.final_plan, context.admission)
    original = context.retained_input
    identity = retained_scope(
        context.base, context.final_plan, RetainedWork(work.root, work.limits.spool, work.register_cleanup, work.check)
    )
    # No-base inputs have no original definition; _plan_scope binds current SOURCE.
    current_definition = "" if context.base is None else context.common.request.definition.digest
    if (
        original.current_definition_sha256 != current_definition
        or original.plan_sha256 != context.final_plan.sha256
        or original.base_identity != identity
        or (original.counts_by_role["family"] != context.final_plan.counts_by_kind["retained"])
        or (
            context.base is None
            and (
                original.definition_canonical != ""
                or original.collection_slots != ()
                or original.origin_scope is not None
                or original.counts
                != tuple((role, 0) for role in ("family", "root", "child", "scalar", "pack", "origin"))
                or any(type(count) is not int for _role, count in original.counts)
            )
        )
    ):
        raise FamilyBlockUnavailable("retained allocation original-input scope differs")
    _require_common_provenance(context)
    source_plan = context.common.source
    return (
        context.build_id,
        source_plan.snapshot_family_id,
        context.final_plan.request_scope,
        source_plan.source.sha256,
        source_plan.sha256,
        context.admission.sha256,
        context.final_plan.sha256,
        original.sha256,
        context.common_output.sha256,
        identity,
    )


def _spool_limits(limits):
    # SOURCE's borrowed adapter owns its own complete external pass. Reserve
    # half for it and half for this consumer, so their simultaneous disk use is
    # explicitly bounded by the caller's one total new-work budget.
    half = limits.max_spool_bytes // 2
    if max(limits.max_record_bytes, limits.memory_bytes) > half:
        raise FamilyBlockResourceLimit("retained allocation cannot reserve both accounted input passes")
    return SourceFactLimits(half, limits.max_record_bytes, limits.memory_bytes)


def _model_cells(model):
    return {column.name: getattr(model, column.name) for column in model.__table__.columns}


def _stage_common(context, work, path_by_kind):
    counts_by_role = dict.fromkeys(context.common_output.role_counts, 0)
    with ExitStack() as stack:
        files_by_role = {role: stack.enter_context(path_by_kind["common_" + role].open("xb")) for role in _ROLES[:2]}
        batches = stack.enter_context(
            closing(
                context.common_output.batches(
                    maximum_rows=4096,
                    maximum_bytes=work.limits.max_record_bytes,
                )
            )
        )
        for batch in batches:
            work.current()
            for role, rows in batch.items():
                counts_by_role[role] += len(rows)
                if role not in files_by_role:
                    continue
                names = _dictionary_columns(context, role)
                identity = "root_record_id" if role == "root_record" else "entity_binding_id"
                for row in rows:
                    native_by_name = dict(zip(names, row, strict=True))
                    work.frame(
                        files_by_role[role],
                        (native_by_name[identity], _pack_row(native_by_name)),
                        path_by_kind["common_" + role],
                    )
    if counts_by_role != context.common_output.role_counts:
        raise FamilyBlockUnavailable("retained allocation SOURCE common complete EOF differs")


def _source_prefix(context, work):
    counts_by_stream = dict.fromkeys((slot for slot, _count in context.common.source.source.source_counts), 0)
    source_records = source_packs = 0
    with closing(read_source_allocations(context.common.source, check=work.current)) as packs:
        for pack, _pack_id, _roots, _revisions, _rejections in packs:
            work.current()
            ordinal = counts_by_stream.get(pack.stream_slot, 0)
            if pack.pack_ordinal != ordinal:
                raise FamilyBlockUnavailable("retained allocation SOURCE pack prefix differs")
            counts_by_stream[pack.stream_slot] = ordinal + 1
            source_records += len(pack.records)
            source_packs += 1
    if (source_records, source_packs) != context.common.source.source.counts[:2]:
        raise FamilyBlockUnavailable("retained allocation SOURCE complete EOF differs")
    return tuple(sorted(counts_by_stream.items()))


def _family_header(plan, allocated, child_count):
    return (
        plan.root_record_id,
        plan.selection_kind,
        _plan_cells(plan),
        allocated.family_revision_id,
        allocated.root_record_id,
        allocated.entity_binding_id,
        allocated.expected_family_sha256,
        child_count,
    )


def _source_family(context, work, output_by_kind, plan, allocated):
    definition = context.common.request.definition
    root_id = allocated.root_record_id
    root_wire = _wire(definition, None, allocated.root, False)
    raw = plan.source_root.landing[_RAW_PARENT_INDEX]
    work.frame(
        output_by_kind["root"][1],
        (root_id, root_wire, 0 if raw is None else len(raw.encode("utf-8"))),
        output_by_kind["root"][0],
    )
    count = 0
    for collection, factory in allocated.children_by_collection.items():
        with closing(factory()) as original_records, closing(factory.original_occurrences()) as originals:
            for original_record, occurrence in zip(original_records, originals, strict=True):
                work.current()
                if (original_record.revision_id, original_record.pack_id, original_record.source_ordinal) != (
                    occurrence.revision_id,
                    occurrence.pack_id,
                    occurrence.coordinate[1],
                ):
                    raise FamilyBlockUnavailable("SOURCE cost record/original occurrence differs")
                raw = occurrence.landing[_RAW_PARENT_INDEX]
                wire = _wire(definition, collection, original_record, False)
                key = record_parts(definition, collection, original_record, retained=False)[1]
                order = collection, key[0], original_record.revision_id
                work.frame(
                    output_by_kind["child"][1],
                    (
                        root_id,
                        "source",
                        order,
                        context.common.layout.collection_slots[collection],
                        wire,
                        0 if raw is None else len(raw.encode("utf-8")),
                        None,
                    ),
                    output_by_kind["child"][0],
                )
                count += 1
    work.frame(output_by_kind["family"][1], _family_header(plan, allocated, count), output_by_kind["family"][0])


def _stage_sources(context, work, output_by_kind):
    source_work = PlanWork(work.root / "source-inputs", work.limits, lambda _path: None, work.current)
    # The borrowed SOURCE adapter owns an independent half-sized scratch.
    # Reserve its entire bound while alive in the shared journal budget.
    budget = getattr(work, "budget", None)
    reservation = work.limits.max_spool_bytes if budget is not None else 0
    if budget is not None:
        budget.charge(reservation)
    try:
        with source_family_inputs(context.common, context.admission, context.final_plan, work=source_work) as families:
            for plan, allocated in families:
                _source_family(context, work, output_by_kind, plan, allocated)
    finally:
        if budget is not None:
            budget.release(reservation)


def _retained_headers(context, work, path):
    with path.open("xb") as output, closing(read_family_plan(context.final_plan, check=work.current)) as plans:
        for plan in plans:
            if plan.selection_kind == "retained":
                work.frame(output, (plan.base_header.family_revision_id, _plan_cells(plan)), path)


def _retained_family(plan, header, root):
    return AllocatedFamily(
        header.family_revision_id, header.root_record_id, header.entity_binding_id, root, {}, header.family_sha256, True
    )


def _retained_entries(context, work, outputs_by_role, selected, entries):
    pending = work.path()
    current, families = next(selected, None), None
    with pending.open("xb") as headers, ExitStack() as stack:
        for entry in entries:
            if entry[0] == "family":
                identity, cells, original = entry[1]
                if current is None or identity != current[0]:
                    raise FamilyBlockUnavailable("retained allocation final/original family bijection differs")
                work.frame(headers, (identity, cells, current[1], original), pending)
                current = next(selected, None)
                continue
            if entry[0] == "record":
                headers.flush()
                if families is None:
                    families = stack.enter_context(closing(_RetainedFamilies(work, pending)))
                cells, plan_cells, original = families.for_id(entry[1])
                _stage_retained_record(
                    context, work, outputs_by_role, entry, BaseFamilyHeader(*cells), _plan(plan_cells), original
                )
                continue
            if entry[0] in ("pack", "origin"):
                work.frame(outputs_by_role["original"][1], entry, outputs_by_role["original"][0])
    if current is not None:
        raise FamilyBlockUnavailable("retained allocation selected family is missing")
    work.remove(pending)


def _stage_retained(context, work, output_by_kind):
    selected_path = work.path()
    _retained_headers(context, work, selected_path)
    with closing(_sorted(work, work.rows(selected_path), lambda row: row[0])) as selected:
        with closing(read_retained_input(context.retained_input, check=work.current)) as entries:
            _retained_entries(context, work, output_by_kind, selected, entries)
    work.remove(selected_path)


class _RetainedFamilies:
    def __init__(self, work, path):
        self.rows = work.rows(path)
        self.current = next(self.rows, None)

    def for_id(self, identity):
        """Borrow one ordered complete original family header."""
        while self.current is not None and self.current[0] < identity:
            self.current = next(self.rows, None)
        if self.current is None or self.current[0] != identity:
            raise FamilyBlockUnavailable("retained allocation record lacks its original family")
        return self.current[1:]

    def close(self):
        """Close only this accounted original header cursor."""
        self.rows.close()


def _stage_retained_record(context, work, output_by_kind, entry, header, plan, original):
    names_by_slot = {slot: name for name, slot in context.retained_input.collection_slots}
    definition = context.common.request.definition
    _kind, identity, slot, wire, lineage = entry
    collection = None if slot == 0 else names_by_slot[slot]
    record = _record(definition, collection, wire, retained=True)
    if slot == 0:
        allocated = _retained_family(plan, header, record)
        work.frame(output_by_kind["root"][1], (header.root_record_id, wire, 0, lineage), output_by_kind["root"][0])
        work.frame(
            output_by_kind["family"][1],
            _family_header(plan, allocated, header.child_count),
            output_by_kind["family"][0],
        )
        work.frame(
            output_by_kind["base_family"][1], (header.root_record_id, original), output_by_kind["base_family"][0]
        )
    else:
        work.frame(
            output_by_kind["child"][1],
            (header.root_record_id, "retained", (slot, record.revision_id), slot, wire, 0, lineage),
            output_by_kind["child"][0],
        )


def _stage_inputs(context, work):
    roles = ("family", "root", "child", "base_family", "original")
    path_by_kind = {role: work.path() for role in (*roles, "common_root_record", "common_entity_binding")}
    _stage_common(context, work, path_by_kind)
    source_prefix = _source_prefix(context, work)
    with ExitStack() as stack:
        outputs_by_role = {
            role: (path_by_kind[role], stack.enter_context(path_by_kind[role].open("xb"))) for role in roles
        }
        if context.final_plan.counts_by_kind["retained"]:
            _stage_sources(context, work, outputs_by_role)
            _stage_retained(context, work, outputs_by_role)
        else:
            _drain_empty_inputs(context, work)
    return path_by_kind, source_prefix


def _drain_empty_inputs(context, work):
    # With no retained destination, SOURCE costs cannot affect pack membership.
    # Keep every independent closed input EOF, but do not reconstruct its values.
    count = 0
    with closing(read_admission_occurrences(context.admission, check=work.current)) as occurrences:
        for _original in occurrences:
            count += 1
    if count != context.common.source.source.counts[0]:
        raise FamilyBlockUnavailable("empty retained allocation SOURCE/admission census differs")
    with closing(read_family_plan(context.final_plan, check=work.current)) as plans:
        for plan in plans:
            if plan.selection_kind == "retained":
                raise FamilyBlockUnavailable("empty retained allocation contains a selected base family")
    with closing(read_retained_input(context.retained_input, check=work.current)) as entries:
        for entry in entries:
            if entry[0] != "complete":
                raise FamilyBlockUnavailable("empty retained allocation contains original retained values")


def _ordered_file(work, path, key):
    with closing(_sorted(work, work.rows(path), key)) as entries:
        result = work.write(entries)
    work.remove(path)
    return result


def _index_children(work, path):
    ordered = _sorted(work, work.rows(path), lambda row: row[:3])
    indexed, ranges = work.path(), work.path()
    previous = None
    start, count = 0, 0
    with closing(ordered), indexed.open("xb") as target, ranges.open("xb") as index:
        for entry in ordered:
            if previous is not None and previous[0] != entry[0]:
                work.frame(index, (previous[0], start, target.tell(), count), ranges)
                start, count = target.tell(), 0
            work.frame(target, entry, indexed)
            count += 1
            previous = entry
        if previous is not None:
            work.frame(index, (previous[0], start, target.tell(), count), ranges)
    work.remove(path)
    return indexed, ranges


def _unique_dictionary(work, path):
    ordered = _sorted(work, work.rows(path), lambda row: row[0])
    output, previous = work.path(), None
    with closing(ordered), output.open("xb") as target:
        for entry in ordered:
            if previous is not None and previous[0] == entry[0]:
                if previous != entry:
                    raise FamilyBlockUnavailable("original common dictionary identity conflicts")
                continue
            work.frame(target, entry, output)
            previous = entry
    work.remove(path)
    return output


def _organize(work, path_by_kind):
    for role in ("family", "root", "base_family"):
        path_by_kind[role] = _ordered_file(work, path_by_kind[role], lambda row: row[0])
    for role in _ROLES[:2]:
        path_by_kind["common_" + role] = _unique_dictionary(work, path_by_kind["common_" + role])
    path_by_kind["children"], path_by_kind["ranges"] = _index_children(work, path_by_kind.pop("child"))
    return path_by_kind


class _GroupedRows:
    def __init__(self, work, path):
        self.rows = work.rows(path)
        self.current = next(self.rows, None)

    def get(self, identity, *, required=False):
        """Compare one immutable ordered owner stream in both directions."""
        if self.current is not None and self.current[0] < identity:
            raise FamilyBlockUnavailable("retained allocation input has an unowned prefix")
        if self.current is None or self.current[0] != identity:
            if required:
                raise FamilyBlockUnavailable("retained allocation input row is missing")
            return None
        current, self.current = self.current, next(self.rows, None)
        if self.current is not None and self.current[0] == identity:
            raise FamilyBlockUnavailable("retained allocation input identity repeats")
        return current

    def finish(self):
        """No orphan input suffix may authorize an allocation artifact."""
        if self.current is not None:
            raise FamilyBlockUnavailable("retained allocation input has an extra suffix")

    def close(self):
        """Close only this borrowed immutable file iterator."""
        self.rows.close()


def _headers(context, work, path_by_kind):
    previous, counts_by_kind = None, dict(source=0, retained=0)
    with ExitStack() as stack:
        roots = stack.enter_context(closing(_GroupedRows(work, path_by_kind["root"])))
        bases = stack.enter_context(closing(_GroupedRows(work, path_by_kind["base_family"])))
        ranges = stack.enter_context(closing(_GroupedRows(work, path_by_kind["ranges"])))
        families = stack.enter_context(closing(work.rows(path_by_kind["family"])))
        for cells in families:
            root_id, kind = cells[:2]
            if previous is not None and root_id <= previous:
                raise FamilyBlockUnavailable("retained allocation family root order differs")
            root = roots.get(root_id, required=True)
            original = bases.get(root_id, required=kind == "retained")
            interval = ranges.get(root_id)
            if (0 if interval is None else interval[3]) != cells[7]:
                raise FamilyBlockUnavailable("retained allocation family child range/count differs")
            counts_by_kind[kind] += 1
            yield cells, root, original, interval
            previous = root_id
        for cursor in (roots, bases, ranges):
            cursor.finish()
    if any(counts_by_kind[kind] != context.final_plan.counts_by_kind[kind] for kind in counts_by_kind):
        raise FamilyBlockUnavailable("retained allocation original selected family EOF differs")


def _header_batches(context, work, path_by_kind, limits):
    batch, size = [], 0
    with closing(_headers(context, work, path_by_kind)) as families:
        for header in families:
            charge = 1024 + 8 * len(_pack_row(header))
            if charge > limits.maximum_batch_bytes or charge > work.limits.memory_bytes:
                raise FamilyBlockResourceLimit("retained family metadata exceeds the owner buffer")
            if batch and (
                len(batch) == min(4096, limits.maximum_batch_records)
                or size + charge > min(limits.maximum_batch_bytes, work.limits.memory_bytes)
            ):
                yield tuple(batch)
                batch, size = [], 0
            batch.append(header)
            size += charge
        if batch:
            yield tuple(batch)


async def _check_base(context):
    if context.base is not None:
        await context.base.check()


async def _guard(session, context, prefix):
    request = context.common.request
    _, binding = await _candidate(
        session, request, context.common.source.snapshot_family_id, context.common.layout, {"loading"}
    )
    build = (
        await _execute(
            session,
            select(models.CustomImportBuildAttempt)
            .where(models.CustomImportBuildAttempt.build_id == context.build_id)
            .execution_options(populate_existing=True),
        )
    ).scalar_one_or_none()
    if (
        build is None
        or build.phase != "graph"
        or build.source_frozen_at is None
        or build.candidate_error_count != 0
        or build.source_occurrence_count != context.common.source.source.counts[0]
        or build.capture_bundle_id != dict(context.common.source.source.scope[0])["capture_bundle_id"]
        or any(
            getattr(build, name) is not None
            for name in ("graph_frozen_at", "output_frozen_at", "generation_id", "verified_at")
        )
    ):
        raise FamilyBlockUnavailable("retained allocation requires the current unfrozen GRAPH owner")
    streams = (
        (
            await _execute(
                session,
                select(models.CustomImportBuildStream)
                .where(models.CustomImportBuildStream.build_id == context.build_id)
                .order_by(models.CustomImportBuildStream.stream_slot),
            )
        )
        .scalars()
        .all()
    )
    actual_pack_rows = tuple((stream.stream_slot, stream.next_pack_ordinal) for stream in streams)
    if actual_pack_rows != prefix or any(stream.replay_verified_at is None for stream in streams):
        raise FamilyBlockUnavailable("retained allocation SOURCE pack counters changed")
    return binding


async def _lookup_dictionaries(factory, context, batch, prefix):
    root_ids = tuple(header[0][0] for header in batch)
    entity_ids = tuple(sorted({header[0][5] for header in batch}))
    await _check_base(context)
    async with _page_session(factory, context.common.request, context.build_id) as (session, _build):
        await _guard(session, context, prefix)
        root_rows = (
            (
                await _execute(
                    session,
                    select(models.CustomImportRootRecord)
                    .where(models.CustomImportRootRecord.root_record_id.in_(root_ids))
                    .order_by(models.CustomImportRootRecord.root_record_id),
                )
            )
            .scalars()
            .all()
        )
        entity_rows = (
            (
                await _execute(
                    session,
                    select(models.CustomImportEntityBinding)
                    .where(models.CustomImportEntityBinding.entity_binding_id.in_(entity_ids))
                    .order_by(models.CustomImportEntityBinding.entity_binding_id),
                )
            )
            .scalars()
            .all()
        )
        if (
            tuple(native_row.root_record_id for native_row in root_rows) != root_ids
            or tuple(native_row.entity_binding_id for native_row in entity_rows) != entity_ids
        ):
            raise FamilyBlockUnavailable("retained allocation global dictionary census differs")
        dictionaries = (
            tuple((_model_cells(root),) for root in root_rows),
            tuple(_model_cells(entity) for entity in entity_rows),
        )
    await _check_base(context)
    return dictionaries


def _checked_dictionary(context, header, root, entity):
    cells = header[0]
    plan = _plan(cells[2])
    request = context.common.request
    if (
        root["root_record_id"],
        root["dataset_id"],
        bytes(root["key_contract_sha256"]),
        root["canonical_logical_key"],
        bytes(root["logical_key_sha256"]),
    ) != (cells[0], request.dataset_id, codec.root_key_contract_hash(request.definition), *plan.typed_key):
        raise FamilyBlockUnavailable("retained allocation exact global root differs")
    if entity["entity_binding_id"] != cells[5] or entity["dataset_id"] != request.dataset_id:
        raise FamilyBlockUnavailable("retained allocation exact global entity differs")
    if cells[1] == "source":
        record = _record(request.definition, None, header[1][1], retained=False)
        value = record.values_by_field[request.definition.entity_field]
        if (
            entity["adapter_id"] != "npi"
            or entity["canonical_value"] != value
            or bytes(entity["value_sha256"]) != codec.digest_text("entity:npi", value)
        ):
            raise FamilyBlockUnavailable("retained allocation SOURCE entity bytes differ")


def _merge_global_dictionaries(context, work, batch, dictionaries, target, path):
    roots, entities = dictionaries
    entities_by_id = {row["entity_binding_id"]: row for row in entities}
    for header, (root,) in zip(batch, roots, strict=True):
        entity = entities_by_id[header[0][5]]
        _checked_dictionary(context, header, root, entity)
        work.frame(target, (*header, _pack_row(root), _pack_row(entity)), path)


async def _dictionary_views(factory, context, work, path_by_kind, prefix, limits):
    path = work.path()
    with path.open("xb") as target:
        for batch in _header_batches(context, work, path_by_kind, limits):
            observed = await _lookup_dictionaries(factory, context, batch, prefix)
            await _cpu(work, _merge_global_dictionaries, context, work, batch, observed, target, path)
    return path


def _base_cost_family(context, original):
    if original is None:
        return None
    native, _member = _unpack_row(original[1])
    return models.CustomImportFamilyRevision(
        **{
            name: native[name]
            for name in (
                "family_revision_id",
                "root_record_id",
                "root_revision_id",
                "entity_binding_id",
                "family_sha256",
                "child_count",
            )
        },
        dataset_id=context.common.request.dataset_id,
        schema_revision_id=context.common.request.schema_revision_id,
        producing_execution_id=context.base.generation.execution_id,
        producing_fence=context.base.generation.producing_fence,
        producing_token_sha256=bytes(context.base.generation.producing_token_sha256),
    )


def _legacy_input(context, cells):
    metadata, root_cells, original, interval, root_data, entity_data = cells
    plan = _plan(metadata[2])
    definition, kind = context.common.request.definition, metadata[1]
    original_root = _record(definition, None, root_cells[1], retained=kind == "retained")
    global_root = models.CustomImportRootRecord(**_unpack_row(root_data))
    entity = models.CustomImportEntityBinding(**_unpack_row(entity_data))
    root = logical_root(
        SimpleNamespace(layout=context.common.layout), context.common.request, global_root, original_root
    )
    pending = models.CustomImportBuildFamily(
        build_id=context.build_id,
        root_record_id=metadata[0],
        root_key_sha256=plan.typed_key[1],
        selection_kind=kind,
        base_family_revision_id=None if plan.base_header is None else plan.base_header.family_revision_id,
        source_root_occurrence_id=None,
        family_revision_id=None,
        attached_child_count=0,
        last_child_collection_slot=None,
        last_child_key_sha256=None,
        last_input_child_revision_id=None,
    )
    family_input = graph._FamilyInput(
        pending,
        root,
        global_root,
        _record_values(definition, None, original_root),
        metadata[6],
        metadata[7],
        metadata[5] if kind == "retained" else None,
    )
    # Preserve both selected root lanes, before the original reducer drops its NULL lane.
    pending_models = (
        pending,
        root if kind == "source" else None,
        root if kind == "retained" else None,
        global_root,
        _base_cost_family(context, original),
        entity if kind == "retained" else None,
        None,
    )
    return pages.LegacyFamilyView(
        family_input,
        pending_models,
        0 if interval is None else interval[1],
        0 if interval is None else interval[2],
        cells,
        1024 + 8 * len(_pack_row(cells)),
    )


@dataclass(frozen=True)
class _ChildOriginal:
    root_id: int
    kind: str
    collection_slot: int
    record: object
    model: object
    raw_parent_bytes: int
    lineage: object
    end: int


def _child_original(context, view, cells, end):
    root_id, kind, _order, slot, wire, raw_bytes, lineage = cells
    if root_id != view.root_id or kind != view.source.plan.selection_kind:
        raise FamilyBlockUnavailable("retained allocation child range belongs to another original family")
    names_by_slot = {candidate: name for name, candidate in context.common.layout.collection_slots.items()}
    collection = names_by_slot[slot]
    record = _record(context.common.request.definition, collection, wire, retained=kind == "retained")
    binding = SimpleNamespace(layout=context.common.layout)
    scope = context.common.request if kind == "source" else context.base.generation
    model = logical_child(binding, scope, view.source.record, collection, record)
    return _ChildOriginal(
        root_id, kind, slot, record, model, raw_bytes, None if lineage is None else _unpack_row(lineage), end
    )


class _ChildReader:
    def __init__(self, context, work, path, views):
        self.context, self.work = context, work
        self.file = path.open("rb")
        self.views_by_id = {view.root_id: view for view in views}
        self.offsets_by_id = {view.root_id: view.child_start for view in views}
        self.preview_by_key = {}
        self.owned_bytes = sum(view.owned_bytes for view in views)
        self.buffer_bytes = self.owned_bytes

    def candidates(self, states, kind, maximum):
        """Borrow a bounded old root/collection/key prefix; no cursor commits here."""
        self.preview_by_key = {}
        count, owned_bytes = 0, self.owned_bytes
        self.buffer_bytes = owned_bytes
        for state in states:
            if state.complete or state.current.selection_kind != kind:
                continue
            view = self.views_by_id[state.current.root_record_id]
            offset = self.offsets_by_id[view.root_id]
            self.file.seek(offset)
            while self.file.tell() < view.child_end and count < maximum:
                self.work.current()
                entry = next(self.work.read(self.file), None)
                if entry is None:
                    raise FamilyBlockUnavailable("retained allocation original child range ended early")
                owned_bytes += 1024 + 8 * len(_pack_row(entry))
                self.buffer_bytes = owned_bytes
                if owned_bytes > self.work.limits.memory_bytes:
                    raise FamilyBlockResourceLimit("legacy child prefix exceeds the owner buffer")
                original = _child_original(self.context, view, entry, self.file.tell())
                key = view.root_id, original.record.revision_id
                if key in self.preview_by_key:
                    raise FamilyBlockUnavailable("retained allocation child identity repeats in a prefix")
                self.preview_by_key[key] = original
                count += 1
                # The last old read column is not native authority. Pure page
                # grouping ignores it; all original input scope is checked above.
                yield view.root_id, original.model, None
            if count == maximum:
                return
            if offset == view.child_end and not state.complete:
                raise FamilyBlockUnavailable("retained allocation original family lacks remaining children")

    def accept(self, attempt):
        """Advance only local file positions after the exact old work decision."""
        for group in attempt.groups:
            if group.children:
                key = group.state.current.root_record_id, group.children[-1].child_revision_id
                self.offsets_by_id[key[0]] = self.preview_by_key[key].end

    def original(self, root_id, revision_id):
        """Return only an original already admitted in this bounded preview."""
        return self.preview_by_key[(root_id, revision_id)]

    def close(self):
        """Release the exact borrowed original file descriptor."""
        self.file.close()


def _scalar_models(context, root_id, collection, record, *, revision_id=None):
    binding = SimpleNamespace(layout=context.common.layout)
    original = record if revision_id is None else replace(record, revision_id=revision_id)
    return tuple(logical_scalars(binding, context.common.request, root_id, collection, original))


def _byte_size(text):
    return 0 if text is None else len(text.encode("utf-8"))


def _cost_upper(page):
    """Conservative native JSON bound; it never substitutes a different decision."""
    work, model = page.work_bytes, page.model_bytes
    for scalar in page.scalars:
        # Six nullable lanes, bounded BIGINT/slot digits, ISO date/timestamp and
        # NUMERIC(30,12) formatting fit the fixed reservation. Native JSON escapes
        # UTF-8 control bytes by at most sixfold. Ambiguous pages use native SQL.
        work += 2048 + 6 * _byte_size(scalar.string_value)
        model += _byte_size(scalar.field_type) + _byte_size(scalar.value_state) + _byte_size(scalar.string_value)
    return page.work_rows + len(page.scalars), work, model


def _context_keys(arguments, identity_index, key_index):
    keys_by_identity = {}
    for identity, canonical in zip(arguments[identity_index][1], arguments[key_index][1], strict=True):
        keys_by_identity.setdefault(identity, []).append(canonical)
    return keys_by_identity


def _merge_cost_pages(ordinal, contributions):
    from process.custom_import.family_shard_work_cost import WorkCostPage

    counters, scalar_rows = [0, 0, 0, 0], []
    for contribution in contributions:
        for index, name in enumerate(("work_rows", "context_count", "work_bytes", "model_bytes")):
            counters[index] += getattr(contribution, name)
        scalar_rows.extend(contribution.scalars)
    return WorkCostPage(ordinal, *counters, tuple(scalar_rows))


def _root_contribution(context, family_input, observed, keys):
    from process.custom_import.family_shard_work_cost import WorkCostPage

    root, dictionary = family_input.root, family_input.record
    entity = observed["entity"]
    identity_bytes = (
        _byte_size(dictionary.canonical_logical_key)
        + 64
        + _byte_size(entity["adapter_id"])
        + _byte_size(entity["canonical_value"])
        + 32
    )
    work_bytes = (
        _byte_size(root.canonical_payload)
        + _byte_size(dictionary.canonical_logical_key)
        + identity_bytes
        + sum(_byte_size(key) for key in keys)
    )
    model_bytes = 172 + _byte_size(root.canonical_payload) + identity_bytes + sum(_byte_size(key) + 32 for key in keys)
    scalars = _scalar_models(
        context,
        family_input.plan.root_record_id,
        None,
        observed["original_root"],
        revision_id=observed["root_revision_id"],
    )
    return WorkCostPage(0, 6 + len(keys), len(keys), work_bytes, model_bytes, scalars)


def _retained_root_costs(context, chunk, arguments, assignments):
    keys_by_root = _context_keys(arguments, 10, 12)
    cost_pages, offset = [], 0
    for ordinal, size in enumerate(arguments[-1][1]):
        selected_inputs = chunk[offset : offset + size]
        offset += size
        contributions = (
            _root_contribution(
                context,
                family_input,
                assignments[family_input.plan.root_record_id],
                keys_by_root.get(family_input.plan.root_record_id, ()),
            )
            for family_input in selected_inputs
        )
        cost_pages.append(_merge_cost_pages(ordinal, contributions))
    return tuple(cost_pages)


def _one_child_cost(context, original, destination, keys, dictionary):
    from process.custom_import.family_shard_work_cost import WorkCostPage

    child = original.model
    collection = next(
        name for name, slot in context.common.layout.collection_slots.items() if slot == original.collection_slot
    )
    scalars = _scalar_models(context, original.root_id, collection, original.record, revision_id=destination)
    text_bytes = sum(
        _byte_size(cell) for cell in (child.canonical_payload, child.canonical_parent_key, child.canonical_child_key)
    )
    work_bytes = text_bytes + _byte_size(dictionary.canonical_logical_key) + sum(_byte_size(key) for key in keys)
    model_bytes = sum(_byte_size(key) + 32 for key in keys)
    if original.kind == "retained":
        model_bytes += 141 + text_bytes
    else:
        work_bytes += original.raw_parent_bytes
    return WorkCostPage(0, 3 + len(keys), len(keys), work_bytes, model_bytes, scalars)


def _group_child_costs(context, groups, keys_by_child, reader, destinations):
    from process.custom_import.family_shard_work_cost import WorkCostPage

    for group in groups:
        root_id = group.state.current.root_record_id
        if group.children and group.state.current.selection_kind == "retained":
            yield WorkCostPage(0, 2, 0, 0, 64, ())
        for child in group.children:
            original = reader.original(root_id, child.child_revision_id)
            destination = (
                child.child_revision_id
                if original.kind == "source"
                else destinations[(root_id, child.child_revision_id)]
            )
            yield _one_child_cost(
                context,
                original,
                destination,
                keys_by_child.get(child.child_revision_id, ()),
                group.state.source.record,
            )


def _child_costs(context, attempt, reader, destinations):
    cost_pages = []
    context_index = 11 if attempt.kind == "retained" else 22
    for ordinal, (groups, arguments) in enumerate(attempt.pages):
        keys_by_child = _context_keys(arguments, context_index, context_index + 2)
        contributions = _group_child_costs(context, groups, keys_by_child, reader, destinations)
        cost_pages.append(_merge_cost_pages(ordinal, contributions))
    return tuple(cost_pages)


def _is_cost_allowed(request, kind, measured, *, roots=False):
    if any(max(page.work_bytes, page.model_bytes) > request.page_byte_limit for page in measured):
        return False
    if kind == "retained" and any(
        page.work_rows > request.page_row_limit or (roots and page.context_count > 256) for page in measured
    ):
        return False
    if kind == "retained" and sum(page.work_rows for page in measured) > MAX_BATCH_ROWS:
        return False
    return max(sum(page.work_bytes for page in measured), sum(page.model_bytes for page in measured)) <= MAX_BATCH_BYTES


async def _is_measured_work_allowed(factory, context, prefix, work_pages, limits, *, control, kind, roots=False):
    from process.custom_import.family_shard_work_cost import MeasuredWorkPage

    conservative_pages = []
    for page in work_pages:
        rows, byte_count, model_bytes = _cost_upper(page)
        conservative_pages.append(
            MeasuredWorkPage(page.page_ordinal, rows, page.context_count, byte_count, model_bytes)
        )
    conservative_pages = tuple(conservative_pages)
    if _is_cost_allowed(context.common.request, kind, conservative_pages, roots=roots):
        return True
    await _check_base(context)
    measured = await control.cost(factory, work_pages, limits)
    await _check_base(context)
    return _is_cost_allowed(context.common.request, kind, measured, roots=roots)


async def _observe_ids(factory, context, prefix, kinds, limits, *, remembered=None, journal=None, cursor=()):
    from process.custom_import.family_shard_retained_journal import _Run

    if (
        type(journal) is not _Run
        or journal.context is not context
        or journal.factory is not factory
        or journal.limits != limits
    ):
        raise ValueError("retained native IDs require their original protected journal run")
    return await journal.observe(prefix, kinds, remembered, cursor=cursor)


def _root_assignments(context, views, identities):
    result, iterator = {}, iter(identities)
    for view in views:
        result[view.root_id] = dict(
            family_id=next(iterator),
            root_revision_id=next(iterator),
            pack_id=next(iterator),
            original_root=_original_root(context, view),
            entity=_unpack_row(view.original_cells[-1]),
        )
        if result[view.root_id]["root_revision_id"] == view.source.root.root_revision_id:
            raise FamilyBlockUnavailable("retained root destination reuses its original revision")
    if next(iterator, None) is not None:
        raise FamilyBlockUnavailable("retained root allocation has trailing identities")
    return result


async def _start_retained(factory, context, work, registry, views, output, bank):
    control = bank.control
    by_root = {view.root_id: view for view in views if view.source.plan.selection_kind == "retained"}
    pending = deque(
        batch
        for batch in pages.root_attempts(context.common.request, registry, tuple(view.source for view in views))
        if batch[1][0].plan.selection_kind == "retained"
    )
    states = []
    while pending:
        work.current()
        _name, chunk, arguments = pending.popleft()
        selected_views = tuple(by_root[family_input.plan.root_record_id] for family_input in chunk)
        kinds = ("family_revision", "root_revision", "pack") * len(chunk)
        identities = await bank.take(kinds, held_bytes=sum(view.owned_bytes for view in views))
        assignments = _root_assignments(context, selected_views, identities)
        costs = _retained_root_costs(context, chunk, arguments, assignments)
        await control.checkpoint(rows=len(chunk), byte_count=sum(page.model_bytes for page in costs))
        allowed = await _is_measured_work_allowed(
            factory, context, output.prefix, costs, bank.limits, control=control, kind="retained", roots=True
        )
        await control.checkpoint()
        output.observed(kinds, identities, allowed)
        if not allowed:
            pending.extendleft(reversed(pages.split_root_attempt(context.common.request, registry, chunk)))
            continue
        for view in selected_views:
            observed = assignments[view.root_id]
            output.root(view, observed, registry)
            states.append(
                pages.started_view(
                    view.source,
                    family_id=observed["family_id"],
                    revision_id=observed["root_revision_id"],
                    entity_id=view.source.entity_binding_id,
                )
            )
    return tuple(states)


def _child_id_kinds(attempt):
    return ("child_revision",) * attempt.consumed + ("pack",) * sum(
        bool(group.children) for groups, _arguments in attempt.pages for group in groups
    )


def _child_assignments(attempt, identities):
    iterator = iter(identities)
    destinations_by_key = {}
    for group in (group for groups, _arguments in attempt.pages for group in groups):
        for child in group.children:
            key = group.state.current.root_record_id, child.child_revision_id
            if key in destinations_by_key:
                raise FamilyBlockUnavailable("retained child input identity repeats")
            destinations_by_key[key] = next(iterator)
            if destinations_by_key[key] == child.child_revision_id:
                raise FamilyBlockUnavailable("retained child destination reuses its original revision")
    packs = tuple(next(iterator) for groups, _arguments in attempt.pages for group in groups if group.children)
    if next(iterator, None) is not None:
        raise FamilyBlockUnavailable("retained child allocation has trailing identities")
    return destinations_by_key, packs


def _emit_children(context, attempt, reader, destinations, packs, output, registry):
    pack_ids = iter(packs)
    names_by_slot = {slot: name for name, slot in context.common.layout.collection_slots.items()}
    for groups, _arguments in attempt.pages:
        for group in groups:
            if not group.children:
                continue
            root_id, pack_id = group.state.current.root_record_id, next(pack_ids)
            view = reader.views_by_id[root_id]
            slots = {child.collection_slot for child in group.children}
            if len(slots) != 1:
                raise FamilyBlockUnavailable("retained logical pack contains multiple collections")
            slot = slots.pop()
            payload_hashes = tuple(bytes(child.payload_sha256) for child in group.children)
            stream = next(
                stream
                for stream in context.common.request.definition.source_streams
                if stream.child_collection == names_by_slot[slot]
            )
            output.pack(pack_id, registry.stream_slots[stream.stream_id], names_by_slot[slot], payload_hashes)
            for ordinal, child in enumerate(group.children):
                original = reader.original(root_id, child.child_revision_id)
                destination = replace(
                    original.record,
                    revision_id=destinations[(root_id, child.child_revision_id)],
                    pack_id=pack_id,
                    source_ordinal=ordinal,
                )
                output.record(view, slot, original.record, destination, original.lineage)
    if next(pack_ids, None) is not None:
        raise FamilyBlockUnavailable("retained child pack allocation has trailing identities")


def _preview_child_attempt(context, work, registry, inputs, states, reader, row_limits, kind):
    try:
        cap = pages.physical_child_limit(context.common.request, registry, states, row_limits)
        candidates = reader.candidates(states, kind, cap)
        attempt = pages.child_attempt(
            context.common.request, registry, inputs, states, kind, candidates, row_limits, check=work.current
        )
    except CandidateRunnerError as error:
        if not graph._is_child_page_bound_error(error):
            raise
        return pages.smaller_limits(row_limits), None
    return row_limits, attempt


async def _consume_children(factory, context, work, state, output, bank, control):
    registry, views, states, child_path = state
    inputs = tuple(view.source for view in views)
    row_limits = pages.initial_limits(context.common.request, registry)
    with closing(_ChildReader(context, work, child_path, views)) as reader:
        while any(not current.complete for current in states):
            for kind in ("source", "retained"):
                if not any(not current.complete and current.current.selection_kind == kind for current in states):
                    continue
                row_limits, attempt = _preview_child_attempt(
                    context, work, registry, inputs, states, reader, row_limits, kind
                )
                if attempt is None:
                    await control.checkpoint(rows=1)
                    continue
                kinds = _child_id_kinds(attempt) if kind == "retained" else ()
                identities = await bank.take(kinds, held_bytes=reader.buffer_bytes) if kinds else ()
                destinations, packs = _child_assignments(attempt, identities) if kinds else ({}, ())
                costs = _child_costs(context, attempt, reader, destinations)
                await control.checkpoint(rows=attempt.consumed, byte_count=sum(page.model_bytes for page in costs))
                allowed = await _is_measured_work_allowed(
                    factory, context, output.prefix, costs, bank.limits, control=control, kind=kind
                )
                await control.checkpoint()
                if kinds:
                    output.observed(kinds, identities, allowed)
                if not allowed:
                    row_limits = pages.smaller_limits(row_limits, attempt)
                    continue
                if kind == "retained":
                    _emit_children(context, attempt, reader, destinations, packs, output, registry)
                reader.accept(attempt)
                states = pages.advance_preview(states, attempt)
        if any(reader.offsets_by_id[view.root_id] != view.child_end for view in views):
            raise FamilyBlockUnavailable("retained allocation child stream did not reach exact EOF")


def _view_cohorts(context, work, registry, path):
    with closing(work.rows(path)) as entries:
        views = (_legacy_input(context, cells) for cells in entries)
        yield from pages.root_cohorts(
            context.common.request, registry, views, check=work.current, memory_bytes=work.limits.memory_bytes
        )


async def _allocate_cohorts(factory, context, work, inputs, output, limits, control):
    paths, prefix, dictionary_path = inputs
    async with _page_session(factory, context.common.request, context.build_id) as (session, _build):
        await _guard(session, context, prefix)
        registry = await load_registry(session, context.common.request)
        window = session.info[_WINDOW]
    control.remembered(window)
    with (
        RetainedReservations(work, limits, control) as bank,
        closing(_view_cohorts(context, work, registry, dictionary_path)) as cohorts,
    ):
        for views in cohorts:
            await control.checkpoint()
            if sum(1024 + 8 * len(_pack_row(view.original_cells)) for view in views) > work.limits.memory_bytes:
                raise FamilyBlockResourceLimit("legacy physical root cohort exceeds the owner buffer")
            retained_views = tuple(view for view in views if view.source.plan.selection_kind == "retained")
            states = (
                list(await _start_retained(factory, context, work, registry, views, output, bank))
                if retained_views
                else []
            )
            for view in views:
                if view.source.plan.selection_kind == "source":
                    original = _original_root(context, view)
                    states.append(
                        pages.started_view(
                            view.source,
                            family_id=view.original_cells[0][3],
                            revision_id=original.revision_id,
                            entity_id=view.original_cells[0][5],
                        )
                    )
            states.sort(key=lambda current: current.current.root_record_id)
            await _consume_children(
                factory,
                context,
                work,
                (registry, views, tuple(states), paths["children"]),
                output,
                bank,
                control,
            )
        await bank.finish(output)


def _retained_dictionaries(work, path, role):
    position = -2 if role == "root_record" else -1
    identity = "root_record_id" if role == "root_record" else "entity_binding_id"
    with closing(work.rows(path)) as families:
        for cells in families:
            if cells[0][1] == "retained":
                packed = cells[position]
                yield _unpack_row(packed)[identity], packed


def _dictionary_role_difference(work, paths, dictionary_path, output, role):
    ordered = _sorted(work, _retained_dictionaries(work, dictionary_path, role), lambda row: row[0])
    previous = None
    with closing(ordered), closing(_GroupedRows(work, paths["common_" + role])) as source:
        for dictionary_row in ordered:
            if previous is not None and dictionary_row[0] == previous[0]:
                if previous != dictionary_row:
                    raise FamilyBlockUnavailable("retained stable dictionary ID has conflicting bytes")
                continue
            while source.current is not None and source.current[0] < dictionary_row[0]:
                source.current = next(source.rows, None)
            common = source.get(dictionary_row[0])
            if common is not None and common != dictionary_row:
                raise FamilyBlockUnavailable("retained dictionary conflicts with independent SOURCE output")
            if common is None:
                output.copy(role, _unpack_row(dictionary_row[1]))
            previous = dictionary_row
        while source.current is not None:
            source.current = next(source.rows, None)


def _dictionary_difference(work, path_by_kind, dictionary_path, output):
    for role in _ROLES[:2]:
        _dictionary_role_difference(work, path_by_kind, dictionary_path, output, role)


async def _produce_retained_destination(factory, context, *, work, observe, scratch=None):
    """Transform exact originals and supplied protected IDs, without COPY.

    The live base context and completed source/common/admission/plan artifacts
    remain independently caller-owned. This producer never calls sequences.
    A supplied scratch remains with its journal owner on failure; standalone
    pure fixtures close their own provisional scratch. Counters are untouched.
    SOURCE and final destination home registration/verification remain mandatory
    caller-owned operations; this artifact never substitutes for their evidence.
    """
    scope = _scope(context, work)
    has_retained = bool(context.final_plan.counts_by_kind["retained"])
    limits = _spool_limits(work.limits.spool) if has_retained else work.limits.spool
    deadline = _read_deadline(context.common.request)

    def current():
        """Retain cancellation and the original immutable request deadline."""
        if work.check is not None:
            work.check()
        _require_deadline(deadline)

    is_owned_scratch = scratch is None
    if is_owned_scratch:
        scratch = _Scratch(PlanWork(work.root, limits, work.register_cleanup, current))
    try:
        await _check_base(context)
        paths, prefix = await _cpu(scratch, _stage_inputs, context, scratch)
        if has_retained:

            async def observed(kinds, remembered, *, cursor=()):
                """Refresh complete owner authority before returning actual IDs."""
                return await observe(prefix, kinds, remembered, cursor=cursor)

            control = RetainedControlWindow(scratch, work.limits, observed)
            await control.checkpoint()
            await _cpu(scratch, _organize, scratch, paths)
            await control.checkpoint()
            dictionary_path = await _dictionary_views(factory, context, scratch, paths, prefix, work.limits)
        with _Output(context, scratch, prefix) as output:
            if has_retained:
                await control.checkpoint()
                await _cpu(scratch, _dictionary_difference, scratch, paths, dictionary_path, output)
                await control.checkpoint()
                await _allocate_cohorts(
                    factory, context, scratch, (paths, prefix, dictionary_path), output, work.limits, control
                )
        if (output.counts["family"], output.counts["root"]) != (context.final_plan.counts_by_kind["retained"],) * 2:
            raise FamilyBlockUnavailable("retained destination selected family census differs")
        descriptor = await _cpu(scratch, _write_complete, context, scope, scratch, paths, output)
        await _check_base(context)
        async with _page_session(factory, context.common.request, context.build_id) as (session, _build):
            await _guard(session, context, prefix)
        return descriptor
    except BaseException as error:
        if is_owned_scratch:
            primary = await _drain_cleanup(_cpu(scratch, scratch.close), error)
            raise primary
        raise


async def allocate_retained_destination(factory, context, *, work, journal):
    """Protect exact destination vectors with the original warm SOURCE owner.

    Original completed events and journal custody are mandatory; unknown replies
    reconcile native heads. Neither pending flags nor files authorize new IDs.
    """
    from process.custom_import.family_shard_retained_journal import journalled_retained_destination

    return await journalled_retained_destination(factory, context, work=work, journal=journal)
