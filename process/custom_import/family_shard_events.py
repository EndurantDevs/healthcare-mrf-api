# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Observed admission rejection IDs, exact COPY cells and resolved-target replay.

SOURCE events keep their existing IDs and never enter this COPY stream. The
closed output remains provisional until the owner finishes every source join.
"""

import hashlib
import itertools
import os
import shutil
import stat
import struct
from contextlib import closing
from dataclasses import astuple, dataclass, replace
from datetime import datetime
from pathlib import Path

from sqlalchemy import select

from db.models.custom_import import CustomImportExecution, CustomImportRejection
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_records import _integer
from process.custom_import.family_block_store import _candidate, _execute, _size
from process.custom_import.family_shard_admission import (
    AdmissionEvent,
    AdmissionOutputDescriptor,
    AdmissionResolution,
    read_admission_events,
    read_admission_resolutions,
)
from process.custom_import.family_shard_allocate import (
    _checked_source,
    _checked_source_provenance,
    read_source_allocations,
)
from process.custom_import.family_shard_facts import _sorted
from process.custom_import.family_shard_ids import _allocate_record_ids
from process.custom_import.family_shard_output import _pack_row, _unpack_row

_CONTRACT = "custom-import/admission-event-ids/v1"
_FILE = "events.mpk"
_LENGTH = struct.Struct("!Q")
_COLUMNS = tuple(CustomImportRejection.__table__.c.keys())


@dataclass(frozen=True)
class AdmissionEventsDescriptor:
    """Picklable custody, exact input scope and actual completed event census."""

    root: str
    device: int
    inode: int
    file_device: int
    file_inode: int
    file_bytes: int
    sha256: bytes
    scope: tuple
    owner_created_at: str
    counts: tuple
    max_frame_bytes: int

    @property
    def role_counts(self):
        """Count only new admission COPY rows; source IDs are evidence, not rows."""
        return {"rejection": self.counts[1]}


def _scope(request, layout, allocated_source, admission):
    _checked_source_provenance(request, allocated_source.source)
    if type(admission) is not AdmissionOutputDescriptor or (
        admission.source_sha256,
        admission.allocation_sha256,
        admission.source_scope,
        admission.snapshot_family_id,
        admission.definition_sha256,
        admission.complete_scope,
    ) != (
        allocated_source.source.sha256,
        allocated_source.sha256,
        allocated_source.source.scope,
        allocated_source.snapshot_family_id,
        request.definition.digest,
        request.complete_scope,
    ):
        raise FamilyBlockUnavailable("admission event input scope differs")
    declarations = admission.worker_inputs
    if (
        type(request.complete_scope) is not bool
        or type(admission.complete_scope) is not bool
        or layout.definition.digest != request.definition.digest
        or dict(layout.collection_slots) != dict(allocated_source.source.collection_slots)
        or tuple(layout.tables(allocated_source.snapshot_family_id)["rejection"].c.keys()) != _COLUMNS
        or type(declarations) is not tuple
        or not 0 < len(declarations) <= 4096
        or admission.counts_by_kind["workers"] != len(declarations)
        or admission.counts_by_kind["records"] != allocated_source.source.counts[0]
        or admission.counts_by_kind["source_events"] != allocated_source.source.counts[4]
    ):
        raise FamilyBlockUnavailable("admission event definition or cohort differs")
    for shard, declaration in enumerate(declarations):
        if (
            type(declaration) is not tuple
            or len(declaration) != 3
            or type(declaration[0]) is not int
            or declaration[0] != shard
            or any(type(digest) is not bytes or len(digest) != 32 for digest in declaration[1:])
        ):
            raise FamilyBlockUnavailable("admission event worker commitments differ")
    return (
        allocated_source.source.sha256,
        allocated_source.sha256,
        admission.sha256,
        allocated_source.source.scope,
        allocated_source.snapshot_family_id,
        layout.digest,
        request.definition.digest,
        request.complete_scope,
        declarations,
    )


def _originals(source, check=None):
    with closing(read_source_allocations(source, check=check)) as packs:
        for pack, pack_id, roots, _revisions, rejections in packs:
            ordinals = pack.initial_rejection_ordinals
            for index, (record, landing) in enumerate(zip(pack.records, pack.landing.records, strict=True)):
                yield (
                    (record.stream_slot, record.source_ordinal),
                    pack_id,
                    roots[index],
                    rejections[index],
                    ordinals[index],
                    pack.collection_slot or None,
                    record.typed_parent_key,
                    landing,
                )


def _original_event(original, event):
    coordinate, pack, root, rejection, ordinal, collection, key, landing = original
    if event.origin == "source":
        expected = AdmissionEvent(
            "source",
            ordinal,
            coordinate,
            landing.rejection_code,
            rejection,
            pack,
            root,
            collection,
            coordinate[1],
            landing.rejection_root_key_canonical,
            landing.rejection_root_key_sha256,
            landing.canonical_evidence,
        )
        if rejection is None or event != expected:
            raise FamilyBlockUnavailable("admission SOURCE event differs from original native allocation")
    elif event.origin != "admission" or (
        event.rejection_id,
        event.pack_id,
        event.root_record_id,
        event.collection_slot,
        event.source_ordinal,
        event.canonical_root_key,
        event.root_key_sha256,
        event.canonical_evidence,
    ) != (
        None,
        pack,
        root,
        collection,
        coordinate[1] or None,
        None if key is None else key[0],
        None if key is None else key[1],
        "{}",
    ):
        raise FamilyBlockUnavailable("admission added event differs from original typed SOURCE evidence")


def _ordered_input_events(admission):
    counts_by_origin, previous_by_origin = dict(source=0, admission=0), {}
    is_admission = False
    with closing(read_admission_events(admission)) as events:
        for ordinal, event in enumerate(events):
            if (
                event.ordinal != ordinal
                or event.origin not in counts_by_origin
                or not isinstance(event.code, str)
                or not event.code
                or (is_admission and event.origin == "source")
                or (event.origin in previous_by_origin and event.coordinate <= previous_by_origin[event.origin])
            ):
                raise FamilyBlockUnavailable("admission global event ordering differs")
            is_admission |= event.origin == "admission"
            previous_by_origin[event.origin] = event.coordinate
            counts_by_origin[event.origin] += 1
            yield astuple(event)
    if tuple(counts_by_origin.values()) != (
        admission.counts_by_kind["source_events"],
        admission.counts_by_kind["admission_events"],
    ):
        raise FamilyBlockUnavailable("admission input event census differs")


def _validated_events(work, source, admission):
    ordered = _sorted(work, _ordered_input_events(admission), lambda cells: (cells[2], cells[0]))
    with closing(ordered), closing(_originals(source, work.current)) as originals:
        groups = itertools.groupby(ordered, lambda cells: cells[2])
        current = next(groups, None)
        for original in originals:
            coordinate, initial = original[0], original[3]
            if current is not None and current[0] < coordinate:
                raise FamilyBlockUnavailable("admission event has no original SOURCE coordinate")
            has_source = False
            if current is not None and current[0] == coordinate:
                for cells in current[1]:
                    event = AdmissionEvent(*cells)
                    _original_event(original, event)
                    has_source |= event.origin == "source"
                    yield cells
                current = next(groups, None)
            if has_source != (initial is not None):
                raise FamilyBlockUnavailable("admission SOURCE event coverage differs")
        if current is not None:
            raise FamilyBlockUnavailable("admission event has an unknown SOURCE coordinate")


def _stage_events(work, source, admission):
    with closing(_sorted(work, _validated_events(work, source, admission), lambda cells: cells[1])) as rows:
        return work.write(rows)


async def _owner(session, request, layout, source, count):
    """Observe values in the journal caller's existing owner transaction."""
    capture = _checked_source(request, source.source)
    await _candidate(session, request, source.snapshot_family_id, layout, {"loading"})
    actual = (
        await _execute(
            session,
            select(CustomImportExecution.capture_bundle_id, CustomImportExecution.source_binding_revision_id).where(
                CustomImportExecution.execution_id == request.execution_id
            ),
        )
    ).one()
    if tuple(actual) != (capture["capture_bundle_id"], capture["source_binding_revision_id"]):
        raise FamilyBlockUnavailable("admission event current capture binding differs")
    stamp = (await _execute(session, "SELECT pg_catalog.transaction_timestamp()", {})).scalar_one()
    if not isinstance(stamp, datetime) or stamp.tzinfo is None or stamp.utcoffset() is None:
        raise FamilyBlockUnavailable("admission event owner timestamp differs")
    return stamp, await _allocate_record_ids(session, ("rejection",) * count) if count else ()


def _pending_batches(work, path, limits):
    batch, size = [], 0
    with closing(work.rows(path)) as rows:
        for cells in rows:
            if cells[0] != "admission":
                continue
            charge = len(_pack_row(cells)) + 128
            if charge > limits.maximum_batch_bytes:
                raise FamilyBlockResourceLimit("admission event exceeds the native allocation batch budget")
            if batch and (len(batch) == limits.maximum_batch_records or size + charge > limits.maximum_batch_bytes):
                yield tuple(batch)
                batch, size = [], 0
            batch.append(cells)
            size += charge
        if batch:
            yield tuple(batch)


def _split_events(work, path):
    sources, pending = work.path(), work.path()
    with closing(work.rows(path)) as rows, sources.open("xb") as source_output, pending.open("xb") as pending_output:
        for cells in rows:
            target, destination = (source_output, sources) if cells[0] == "source" else (pending_output, pending)
            work.frame(target, cells, destination)
    return sources, pending


def _append_ids(work, path, batch, identities):
    if len(identities) != len(batch):
        raise FamilyBlockUnavailable("admission native allocation census differs")
    with path.open("ab") as output:
        for cells, identifier in zip(batch, identities, strict=True):
            _integer(identifier, "admission native rejection")
            work.frame(output, astuple(replace(AdmissionEvent(*cells), rejection_id=identifier)), path)


def _unique_ids(work, path):
    with closing(_sorted(work, work.rows(path), lambda cells: cells[4])) as rows:
        previous = None
        for cells in rows:
            _integer(cells[4], "completed admission rejection")
            if cells[4] == previous:
                raise FamilyBlockUnavailable("completed SOURCE/admission rejection ID repeats")
            previous = cells[4]


def _resolved_rows(work, path, admission):
    def requests():
        """Keep resolution sorting primitive while preserving its exact original event."""
        with closing(read_admission_resolutions(admission)) as resolutions:
            for resolution in resolutions:
                yield resolution.target, astuple(resolution.event)

    ordered = _sorted(work, requests(), lambda row: (row[1][1], row[0]))
    with closing(ordered), closing(work.rows(path)) as events:
        current = next(events, None)
        for target, cells in ordered:
            resolution = AdmissionResolution(target, AdmissionEvent(*cells))
            while current is not None and current[1] < resolution.event.ordinal:
                current = next(events, None)
            if current is None or current[1] != resolution.event.ordinal:
                raise FamilyBlockUnavailable("admission resolved target has no allocated event")
            actual = AdmissionEvent(*current)
            if replace(actual, rejection_id=resolution.event.rejection_id) != resolution.event:
                raise FamilyBlockUnavailable("admission resolved target event evidence differs")
            yield resolution.target, astuple(actual)


def _copy_cells(scope, created_at, event):
    capture_by_field = dict(scope[3][0])
    values_by_name = dict(
        rejection_id=event.rejection_id,
        execution_id=scope[3][1],
        rejection_ordinal=event.ordinal,
        dataset_id=capture_by_field["dataset_id"],
        definition_revision_id=capture_by_field["definition_revision_id"],
        schema_revision_id=capture_by_field["schema_revision_id"],
        pack_id=event.pack_id,
        root_key_sha256=event.root_key_sha256,
        canonical_root_key=event.canonical_root_key,
        collection_slot=event.collection_slot,
        source_ordinal=event.source_ordinal,
        code=event.code,
        field_slot=None,
        canonical_evidence=event.canonical_evidence,
        producing_fence=scope[3][2],
        producing_token_sha256=scope[3][3],
        created_at=datetime.fromisoformat(created_at),
    )
    return tuple(values_by_name[name] for name in _COLUMNS)


def _complete(work, events, admission, scope, stamp):
    _unique_ids(work, events)
    path, counts = work.path(), [0, 0, 0]
    with path.open("xb") as output, closing(work.rows(events)) as event_rows:
        work.frame(output, ("header", _CONTRACT, scope, stamp), path)
        for cells in event_rows:
            event = AdmissionEvent(*cells)
            copy_payload = _pack_row(_copy_cells(scope, stamp, event)) if event.origin == "admission" else None
            work.frame(output, ("event", cells, copy_payload), path)
            counts[int(event.origin == "admission")] += 1
        ordered = _sorted(work, _resolved_rows(work, events, admission), lambda row: row[0])
        with closing(ordered):
            previous = None
            for target_coordinate, cells in ordered:
                if previous is not None and target_coordinate <= previous:
                    raise FamilyBlockUnavailable("admission resolved target repeats")
                work.frame(output, ("resolution", target_coordinate, cells), path)
                previous = target_coordinate
                counts[2] += 1
        expected = admission.counts_by_kind
        if tuple(counts) != (expected["source_events"], expected["admission_events"], expected["resolutions"]):
            raise FamilyBlockUnavailable("completed admission event census differs")
        work.frame(output, ("complete", tuple(counts)), path)
        output.flush()
        os.fsync(output.fileno())
    work.remove(events)
    return _seal_output(work, path, scope, stamp, counts)


def _seal_output(work, path, scope, stamp, counts):
    ready = work.root / _FILE
    path.rename(ready)
    work.paths[ready] = work.paths.pop(path)
    ready.chmod(0o400)
    with ready.open("rb") as source:
        digest = hashlib.file_digest(source, "sha256").digest()
        os.fsync(source.fileno())
    directory = os.open(work.root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    observed = ready.stat()
    return AdmissionEventsDescriptor(
        str(work.root),
        work.identity.st_dev,
        work.identity.st_ino,
        observed.st_dev,
        observed.st_ino,
        observed.st_size,
        digest,
        scope,
        stamp,
        tuple(counts),
        work.limits.max_record_bytes,
    )


async def allocate_admission_events(session_factory, request, layout, allocated_source, admission, *, journal, limits):
    """Protect actual owner timestamps and added IDs before exposing COPY cells.

    Original SOURCE journal custody is mandatory. Unknown replies reconcile its
    fresh native head; neither pending flags nor files authorize replacement IDs.
    """
    scope = _scope(request, layout, allocated_source, admission)
    from process.custom_import.family_shard_events_journal import journalled_admission_events

    return await journalled_admission_events(
        session_factory, request, layout, allocated_source, admission, scope, journal=journal, limits=limits
    )


def _owned_file(descriptor):
    if type(descriptor) is not AdmissionEventsDescriptor:
        raise ValueError("admission events descriptor differs")
    root = Path(descriptor.root)
    directory, path = root.lstat(), root / _FILE
    observed = path.lstat()
    if (
        not stat.S_ISDIR(directory.st_mode)
        or (directory.st_dev, directory.st_ino) != (descriptor.device, descriptor.inode)
        or not stat.S_ISREG(observed.st_mode)
        or (observed.st_dev, observed.st_ino, observed.st_size, stat.S_IMODE(observed.st_mode))
        != (descriptor.file_device, descriptor.file_inode, descriptor.file_bytes, 0o400)
        or {entry.name for entry in root.iterdir()} != {_FILE}
    ):
        raise ValueError("admission events file custody differs")
    return path


def _entries(descriptor):
    path = _owned_file(descriptor)
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as source:
        opened = os.fstat(source.fileno())
        if (opened.st_dev, opened.st_ino, opened.st_size) != (
            descriptor.file_device,
            descriptor.file_inode,
            descriptor.file_bytes,
        ):
            raise ValueError("admission events open custody differs")
        digest = hashlib.sha256()
        while prefix := source.read(8):
            if len(prefix) != 8:
                raise ValueError("admission events frame is truncated")
            size = _LENGTH.unpack(prefix)[0]
            if not 0 < size <= descriptor.max_frame_bytes:
                raise ValueError("admission events frame exceeds the resource budget")
            payload = source.read(size)
            if len(payload) != size:
                raise ValueError("admission events payload is truncated")
            digest.update(prefix)
            digest.update(payload)
            yield _unpack_row(payload)
        if digest.digest() != descriptor.sha256:
            raise ValueError("admission events complete EOF bytes differ")


def _check_event(descriptor, entry, counts):
    event = AdmissionEvent(*entry[1])
    _integer(event.rejection_id, "replayed admission rejection")
    if event.ordinal != sum(counts[:2]) or event.origin not in {"source", "admission"}:
        raise ValueError("admission events replay ordering differs")
    if event.origin == "source":
        if counts[1] or entry[2] is not None:
            raise ValueError("admission events SOURCE replay differs")
        counts[0] += 1
    else:
        if _unpack_row(entry[2]) != _copy_cells(descriptor.scope, descriptor.owner_created_at, event):
            raise ValueError("admission rejection COPY cells differ")
        counts[1] += 1


class _ReplayCensus:
    def __init__(self, descriptor):
        self.descriptor, self.counts = descriptor, [0, 0, 0]
        self.previous, self.is_resolution, self.is_complete = None, False, False

    def accept(self, entry):
        """Check one section/ordinal transition before exposing its provisional row."""
        if self.is_complete:
            raise ValueError("admission events have trailing rows")
        if entry[0] == "complete":
            if entry != ("complete", self.descriptor.counts):
                raise ValueError("admission events completion differs")
            self.is_complete = True
            return
        if entry[0] == "event" and not self.is_resolution:
            _check_event(self.descriptor, entry, self.counts)
            return
        if entry[0] == "resolution":
            target_coordinate = entry[1]
            if self.previous is not None and target_coordinate <= self.previous:
                raise ValueError("admission resolution replay ordering differs")
            _integer(AdmissionEvent(*entry[2]).rejection_id, "replayed resolved rejection")
            self.previous, self.is_resolution = target_coordinate, True
            self.counts[2] += 1
            return
        raise ValueError("admission events section differs")


def _checked_entries(descriptor):
    census = _ReplayCensus(descriptor)
    with closing(_entries(descriptor)) as entries:
        if next(entries, None) != ("header", _CONTRACT, descriptor.scope, descriptor.owner_created_at):
            raise ValueError("admission events header differs")
        for entry in entries:
            census.accept(entry)
            yield entry
    if not census.is_complete or tuple(census.counts) != descriptor.counts:
        raise ValueError("admission events require complete EOF/census")


def read_allocated_events(descriptor):
    """Replay unchanged SOURCE events and added events with observed native IDs."""
    with closing(_checked_entries(descriptor)) as entries:
        for entry in entries:
            if entry[0] == "event":
                yield AdmissionEvent(*entry[1])


def read_event_resolutions(descriptor):
    """Replay exact earliest targets with their actual SOURCE/admission rejection ID."""
    with closing(_checked_entries(descriptor)) as entries:
        for entry in entries:
            if entry[0] == "resolution":
                yield AdmissionResolution(entry[1], AdmissionEvent(*entry[2]))


def admission_event_batches(descriptor, *, maximum_rows, maximum_bytes):
    """Emit ONLY new rejection rows; EOF remains mandatory before owner acceptance."""
    if any(type(bound) is not int or bound <= 0 for bound in (maximum_rows, maximum_bytes)):
        raise ValueError("admission event COPY budgets must be positive")
    rows, size = [], 0
    with closing(_checked_entries(descriptor)) as entries:
        for entry in entries:
            if entry[0] != "event" or entry[2] is None:
                continue
            row = _unpack_row(entry[2])
            charge = 2 + sum(_size(cell) for cell in row)
            if charge > maximum_bytes:
                raise FamilyBlockResourceLimit("admission rejection exceeds the COPY byte budget")
            if rows and (len(rows) == maximum_rows or size + charge > maximum_bytes):
                yield {"rejection": tuple(rows)}
                rows, size = [], 0
            rows.append(row)
            size += charge
        if rows:
            yield {"rejection": tuple(rows)}


def cleanup_admission_events(descriptor):
    """Remove only the unchanged exact completed root after terminal owner checks."""
    _owned_file(descriptor)
    shutil.rmtree(descriptor.root)
