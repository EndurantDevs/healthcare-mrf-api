# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Completed owner SOURCE/admission evidence; no selection, IDs or authority minted.

Original SOURCE plans independently regenerate the existing complete facts.
Every worker row is compared in both directions. Candidate blocks are never an
input oracle. Replay prefixes remain provisional until file SHA, census and EOF.
"""

import hashlib
import os
import shutil
import stat
import struct
from collections.abc import Sequence
from contextlib import ExitStack, closing
from dataclasses import asdict, dataclass
from pathlib import Path
from types import SimpleNamespace

from process.custom_import import family_shard_build as kernel
from process.custom_import.bulk_page_codec import LandingRow
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_shard_allocate import AllocatedSourcePlan, read_source_allocations
from process.custom_import.family_shard_facts import (
    SourceFactLimits,
    _decision_from_document,
    _family_order,
    _prepared_families,
    _routed_records,
    _sorted,
    _Work,
)
from process.custom_import.family_shard_input_records import decode_record, encode_record
from process.custom_import.family_shard_output import _pack_row, _unpack_row
from process.custom_import.family_shard_route import RoutedShard
from process.custom_import.family_shard_source_plan import read_source_plan
from process.custom_import.family_shard_worker import SourceShardOutput, _audit_family

_CONTRACT = "custom-import/admission-output/v1"
_FILE = "admission.mpk"
_LENGTH = struct.Struct("!Q")
_WORKER_COUNTS = ("records", "families", "source", "absent", "error", "candidate_errors", "fatal_families")
_SECTIONS = ("worker", "family", "occurrence", "event", "resolution")


@dataclass(frozen=True)
class AdmissionWork:
    """Caller-owned operational scratch; exhaustion never rejects a logical row."""

    root: object
    limits: SourceFactLimits
    register_cleanup: object
    check: object = None

    def __post_init__(self):
        if type(self.limits) is not SourceFactLimits or not callable(self.register_cleanup):
            raise ValueError("admission requires explicit accounted work")
        if self.check is not None and not callable(self.check):
            raise ValueError("admission cancellation check differs")


@dataclass(frozen=True)
class AdmissionOccurrence:
    coordinate: tuple
    family_key: tuple | None
    encoded_record: bytes
    pack_id: int
    root_record_id: int | None
    revision_id: int | None
    initial_rejection_id: int | None
    initial_rejection_ordinal: int | None
    collection_slot: int
    landing: tuple
    facts_by_name: dict
    decision_by_name: dict
    worker_shard_id: int

    @property
    def record(self):
        """Recover exact lexical/typed original values without payload inference."""
        return decode_record(self.encoded_record)

    @property
    def decision(self):
        """Recover the existing kernel decision, including every original target."""
        return _decision_from_document(self.decision_by_name)


@dataclass(frozen=True)
class _OriginalOccurrence:
    coordinate: tuple
    encoded_record: bytes
    pack_id: int
    root_record_id: int | None
    revision_id: int | None
    rejection_id: int | None
    rejection_ordinal: int | None
    collection_slot: int
    landing: tuple


@dataclass(frozen=True)
class AdmissionEvent:
    origin: str
    ordinal: int
    coordinate: tuple
    code: str
    rejection_id: int | None
    pack_id: int
    root_record_id: int | None
    collection_slot: int | None
    source_ordinal: int | None
    canonical_root_key: str | None
    root_key_sha256: bytes | None
    canonical_evidence: str


@dataclass(frozen=True)
class AdmissionResolution:
    target: tuple
    event: AdmissionEvent


@dataclass(frozen=True)
class AdmissionOutputDescriptor:
    """Picklable closed custody and provisional complete-source audit identity."""

    root: str
    device: int
    inode: int
    file_device: int
    file_inode: int
    file_bytes: int
    sha256: bytes
    source_sha256: bytes
    allocation_sha256: bytes
    source_scope: tuple
    snapshot_family_id: int
    definition_sha256: str
    complete_scope: bool
    worker_inputs: tuple
    counts: tuple
    max_frame_bytes: int

    @property
    def counts_by_kind(self):
        """Return audit counts, never native structural acceptance or base selection."""
        return dict(self.counts)


class _Scratch(_Work):
    def __init__(self, plan):
        super().__init__(plan.root, plan.limits, plan.register_cleanup, check=plan.check)
        self.check = plan.check

    def current(self):
        """Apply the same owner cancellation/deadline throughout external passes."""
        if self.check is not None:
            self.check()


def _coordinate(record):
    return record.stream_slot, record.source_ordinal


def _typed_order(key):
    return (1, b"", "") if key is None else (0, key[1], key[0])


def _scope(source, definition, workers, work, complete_scope):
    if (
        type(source) is not AllocatedSourcePlan
        or type(definition) is not CustomImportDefinition
        or type(work) is not AdmissionWork
    ):
        raise ValueError("admission requires its complete typed source, definition and work")
    if not isinstance(workers, Sequence) or isinstance(workers, (str, bytes)) or not 0 < len(workers) <= 4096:
        raise ValueError("admission requires the complete bounded worker cohort")
    if type(complete_scope) is not bool or any(type(worker) is not SourceShardOutput for worker in workers):
        raise ValueError("admission worker or complete-scope declaration differs")
    identity_by_name = dict(source.source.scope[0])
    if (identity_by_name["definition_sha256"], identity_by_name["schema_sha256"]) != (
        definition.digest,
        definition.schema_digest,
    ):
        raise ValueError("admission source definition differs")
    expected_streams = tuple((stream.stream_id, slot) for slot, stream in enumerate(definition.source_streams, 1))
    if tuple(sorted(source.source.stream_slots)) != tuple(sorted(expected_streams)):
        raise ValueError("admission SOURCE stream declaration differs")
    collection_by_name = dict(source.source.collection_slots)
    if set(collection_by_name) != set(definition.collections_by_name):
        raise ValueError("admission SOURCE collection declaration differs")
    return identity_by_name


def _source_records(source, work):
    with closing(read_source_plan(source.source, check=work.current)) as packs:
        for pack in packs:
            for record in pack.records:
                work.current()
                yield record


def _reference(work, source, definition, complete_scope):
    path = work.path()
    counts_by_kind = dict.fromkeys(_WORKER_COUNTS, 0)
    # Reuse the exact complete raw reconciliation and typed-family fact passes,
    # on the SAME accounted Work so nested external sorts share its disk budget.
    with closing(_source_records(source, work)) as records:
        # A constant bucket preserves the unsharded reference's family order.
        ordered = _sorted(
            work,
            _routed_records(work, records, source.source.counts[0], route_merge=True),
            lambda row: (0, _family_order(row)),
            route_key="family",
        )
        with closing(_prepared_families(work, definition, ordered)) as families, path.open("xb") as output:
            plan = SimpleNamespace(complete_scope=complete_scope, family_ids={}, record_ids={})
            for family in families:
                _audit_family(work, output, path, plan, family, counts_by_kind)
    if counts_by_kind["records"] != source.source.counts[0]:
        raise ValueError("admission reference complete occurrence census differs")
    return path, counts_by_kind


def _worker_header(allocated_source, definition, header_by_name, count, complete_scope):
    if set(header_by_name) != {
        "shard",
        "dataset_id",
        "schema_revision_id",
        "snapshot_family_id",
        "metadata_bytes",
        "metadata_sha256",
        "complete_scope",
    }:
        raise ValueError("admission worker header declaration differs")
    identity_by_name = dict(allocated_source.source.scope[0])
    if type(header_by_name["complete_scope"]) is not bool or any(
        type(header_by_name[name]) is not int or header_by_name[name] <= 0
        for name in ("dataset_id", "schema_revision_id", "snapshot_family_id")
    ):
        raise ValueError("admission worker native scope types differ")
    shard = RoutedShard(**header_by_name["shard"])
    if (
        header_by_name["dataset_id"],
        header_by_name["schema_revision_id"],
        header_by_name["snapshot_family_id"],
        header_by_name["complete_scope"],
    ) != (
        identity_by_name["dataset_id"],
        identity_by_name["schema_revision_id"],
        allocated_source.snapshot_family_id,
        complete_scope,
    ):
        raise ValueError("admission worker native scope differs")
    if (
        type(shard.shard_id) is not int
        or not 0 <= shard.shard_id < count
        or type(shard.shard_count) is not int
        or shard.shard_count != count
        or shard.partition_sha256 != allocated_source.source.partition_sha256
        or shard.definition_sha256 != definition.digest
        or type(shard.records) is not int
        or shard.records < 0
    ):
        raise ValueError("admission worker routed scope differs")
    digest = header_by_name["metadata_sha256"]
    if (
        type(digest) is not bytes
        or len(digest) != 32
        or type(header_by_name["metadata_bytes"]) is not int
        or header_by_name["metadata_bytes"] <= 0
    ):
        raise ValueError("admission worker metadata commitment differs")
    return shard


def _family_header(header_by_name):
    if set(header_by_name) != {
        "typed_key",
        "selection",
        "selection_problem",
        "source_root",
        "source_facts",
        "root_resolution",
        "candidate_errors",
        "fatal_problem",
        "fatal_order",
        "family_ids",
    }:
        raise ValueError("admission worker family declaration differs")
    identities = header_by_name["family_ids"]
    if identities is not None and (
        type(identities) is not tuple
        or len(identities) != 2
        or any(type(identifier) is not int or not 0 < identifier < 2**63 for identifier in identities)
    ):
        raise ValueError("admission provisional family reservation differs")
    if header_by_name["selection"] == "source" and identities is None:
        raise ValueError("admission source family lacks its provisional reservation")
    if header_by_name["selection"] not in ("source", "absent", "error"):
        raise ValueError("admission provisional selection differs")


def _worker_rows(work, allocated_source, definition, worker, count, complete_scope):
    counters = dict.fromkeys(_WORKER_COUNTS, 0)
    with closing(worker.audit_rows()) as audit_entries:
        first = next(audit_entries, None)
        if type(first) is not tuple or len(first) != 2 or first[0] != "worker":
            raise ValueError("admission worker lacks original header")
        shard = _worker_header(allocated_source, definition, first[1], count, complete_scope)
        receipt = worker.receipt
        if any(type(digest) is not bytes or len(digest) != 32 for digest in (worker.sha256, receipt["result_sha256"])):
            raise ValueError("admission worker audit/output commitment differs")
        if (receipt["shard_id"], receipt["partition_sha256"], receipt["source_record_count"]) != (
            shard.shard_id,
            bytes.fromhex(allocated_source.source.partition_sha256),
            shard.records,
        ):
            raise ValueError("admission worker output receipt scope differs")
        yield "worker", shard.shard_id, first[1]
        family_key = previous = None
        is_family = is_complete = False
        for entry in audit_entries:
            work.current()
            if is_complete:
                raise ValueError("admission worker has trailing audit rows")
            match entry[0]:
                case "family":
                    header_by_name = entry[1]
                    _family_header(header_by_name)
                    order = _typed_order(header_by_name["typed_key"])
                    if previous is not None and order <= previous:
                        raise ValueError("admission worker family order repeats")
                    previous, family_key, is_family = order, header_by_name["typed_key"], True
                    counters["families"] += 1
                    counters[header_by_name["selection"]] += 1
                    counters["candidate_errors"] += header_by_name["candidate_errors"]
                    counters["fatal_families"] += int(header_by_name["fatal_problem"] is not None)
                    yield "family", shard.shard_id, header_by_name
                case "occurrence":
                    if not is_family or len(entry) != 6 or entry[1] != family_key:
                        raise ValueError("admission worker occurrence family differs")
                    counters["records"] += 1
                    yield "occurrence", shard.shard_id, *entry[1:]
                case "eof":
                    if set(entry[1]) != set(_WORKER_COUNTS) or any(
                        type(counter_value) is not int or counter_value < 0 for counter_value in entry[1].values()
                    ):
                        raise ValueError("admission worker EOF count types differ")
                    if entry != ("eof", counters) or worker.counts != counters or counters["records"] != shard.records:
                        raise ValueError("admission worker complete census differs")
                    if worker.status != ("refused" if counters["error"] else "source_eof"):
                        raise ValueError("admission worker status differs from full audit")
                    is_complete = True
                case _:
                    raise ValueError("admission worker audit kind differs")
        if not is_complete:
            raise ValueError("admission worker audit EOF is missing")


def _workers(work, source, definition, workers, complete_scope):
    path = work.path()
    header_path = work.path()
    declarations, seen = [], set()
    with path.open("xb") as output, header_path.open("xb") as header_output:
        for worker in workers:
            with closing(_worker_rows(work, source, definition, worker, len(workers), complete_scope)) as rows:
                header = next(rows)
                shard = header[1]
                if shard in seen:
                    raise ValueError("admission worker cohort repeats a shard")
                seen.add(shard)
                declarations.append((shard, worker.sha256, worker.receipt["result_sha256"]))
                work.frame(header_output, header, header_path)
                for entry in rows:
                    work.frame(output, entry, path)
    if seen != set(range(len(workers))):
        raise ValueError("admission worker cohort is incomplete")
    return path, header_path, tuple(sorted(declarations))


def _family_entries(work, path, *, worker):
    for entry in work.rows(path):
        if entry[0] == "family":
            header_by_name = entry[2] if worker else entry[1]
            yield _typed_order(header_by_name["typed_key"]), (entry[1], header_by_name) if worker else header_by_name


def _families(work, expected, observed):
    path = work.path()
    left = _sorted(work, _family_entries(work, expected, worker=False), lambda entry: entry[0])
    right = _sorted(work, _family_entries(work, observed, worker=True), lambda entry: entry[0])
    with closing(left), closing(right), path.open("xb") as output:
        previous = None
        for order, original in left:
            actual = next(right, None)
            if order == previous or actual is None or actual[0] != order:
                raise ValueError("admission worker family coverage differs")
            shard, header_by_name = actual[1]
            if _pack_row(header_by_name | {"family_ids": None}) != _pack_row(original):
                raise ValueError("admission worker complete family facts differ")
            work.frame(output, ("family", shard, header_by_name), path)
            previous = order
        if next(right, None) is not None:
            raise ValueError("admission worker has extra families")
    return path


def _occurrence_entries(work, path, *, worker):
    for entry in work.rows(path):
        if entry[0] == "occurrence":
            encoded = entry[3] if worker else entry[2]
            yield _coordinate(decode_record(encoded)), entry[1:]


def _source_entries(source, definition, check=None):
    slot_by_name = dict(source.source.collection_slots)
    with closing(read_source_allocations(source, check=check)) as packs:
        for pack, pack_id, roots, revisions, rejections in packs:
            stream = definition.source_streams[pack.stream_slot - 1]
            expected_slot = 0 if stream.child_collection is None else slot_by_name[stream.child_collection]
            if pack.collection_slot != expected_slot:
                raise ValueError("admission actual registry collection slot differs")
            rejection_ordinals = pack.initial_rejection_ordinals
            for index, (record, landing) in enumerate(zip(pack.records, pack.landing.records, strict=True)):
                if record.rejection_code != landing.rejection_code:
                    raise ValueError("admission original SOURCE rejection differs")
                yield _OriginalOccurrence(
                    _coordinate(record),
                    encode_record(record),
                    pack_id,
                    roots[index],
                    revisions[index],
                    rejections[index],
                    rejection_ordinals[index],
                    pack.collection_slot,
                    tuple(landing[1:]),
                )


def _source_event(occurrence, origin, code):
    landing_by_name = dict(zip(LandingRow._fields[1:], occurrence.landing, strict=True))
    is_source = origin == "source"
    key = occurrence.record.typed_parent_key if occurrence.root_record_id is not None else None
    return (
        origin,
        occurrence.coordinate,
        code,
        occurrence.initial_rejection_id if is_source else None,
        occurrence.initial_rejection_ordinal if is_source else None,
        occurrence.pack_id,
        occurrence.root_record_id,
        occurrence.collection_slot or None,
        occurrence.coordinate[1] if is_source else (occurrence.coordinate[1] or None),
        landing_by_name["rejection_root_key_canonical"] if is_source else (None if key is None else key[0]),
        landing_by_name["rejection_root_key_sha256"] if is_source else (None if key is None else key[1]),
        landing_by_name["canonical_evidence"] if is_source else "{}",
    )


def _joined(work, allocated_source, definition, expected, observed, worker_count):
    paths_by_kind = {kind: work.path() for kind in ("occurrence", "source", "admission", "target")}
    left = _sorted(work, _occurrence_entries(work, expected, worker=False), lambda entry: entry[0])
    right = _sorted(work, _occurrence_entries(work, observed, worker=True), lambda entry: entry[0])
    with ExitStack() as stack:
        files_by_kind = {kind: stack.enter_context(path.open("xb")) for kind, path in paths_by_kind.items()}
        stack.enter_context(closing(left))
        stack.enter_context(closing(right))
        originals = stack.enter_context(closing(_source_entries(allocated_source, definition, work.current)))
        previous = None
        for original in originals:
            reference, actual = next(left, None), next(right, None)
            if previous is not None and original.coordinate <= previous:
                raise ValueError("admission original source order repeats")
            occurrence = _join_occurrence(original, reference, actual, worker_count)
            work.frame(
                files_by_kind["occurrence"],
                ("occurrence", tuple(vars(occurrence).values())),
                paths_by_kind["occurrence"],
            )
            _occurrence_events(work, files_by_kind, paths_by_kind, occurrence)
            previous = original.coordinate
        if next(left, None) is not None or next(right, None) is not None:
            raise ValueError("admission worker/reference has extra occurrences")
    return paths_by_kind


def _join_occurrence(original, reference, actual, worker_count):
    if reference is None or actual is None or reference[0] != original.coordinate or actual[0] != original.coordinate:
        raise ValueError("admission complete occurrence coverage differs")
    key, encoded, facts, decision, _unused = reference[1]
    shard, actual_key, actual_encoded, actual_facts, actual_decision, native = actual[1]
    expected_native = (
        None if original.revision_id is None else (original.revision_id, original.pack_id, original.root_record_id)
    )
    if (
        original.encoded_record != encoded
        or original.encoded_record != actual_encoded
        or actual_key != key
        or native != expected_native
        or _pack_row((actual_facts, actual_decision)) != _pack_row((facts, decision))
    ):
        raise ValueError("admission worker original bytes, native IDs, facts or decision differ")
    if shard != (0 if key is None else int.from_bytes(key[1][:8], "big") % worker_count):
        raise ValueError("admission worker effective family routing differs")
    return AdmissionOccurrence(
        original.coordinate,
        key,
        original.encoded_record,
        original.pack_id,
        original.root_record_id,
        original.revision_id,
        original.rejection_id,
        original.rejection_ordinal,
        original.collection_slot,
        original.landing,
        facts,
        decision,
        shard,
    )


def _occurrence_events(work, files_by_kind, paths_by_kind, occurrence):
    record, decision = occurrence.record, occurrence.decision
    if record.rejection_code is not None:
        work.frame(
            files_by_kind["source"], _source_event(occurrence, "source", record.rejection_code), paths_by_kind["source"]
        )
    if decision.event is not None and decision.event.origin == "admission":
        work.frame(
            files_by_kind["admission"],
            _source_event(occurrence, "admission", decision.event.code),
            paths_by_kind["admission"],
        )
    for target in decision.targets:
        work.frame(
            files_by_kind["target"],
            (target.occurrence, target.event.occurrence, target.event.origin, target.event.code),
            paths_by_kind["target"],
        )


class _EventCursor:
    def __init__(self, work, path):
        self.work, self.path, self.current = work, path, None

    def events(self):
        """Borrow one exact source/admission event body at a time for the kernel."""
        for body in self.work.rows(self.path):
            self.current = body
            yield kernel.RejectionEvent(body[0], body[1], body[2])


def _event_key(event):
    return event.origin, event.coordinate, event.code


def _number_events(work, paths_by_kind, initial_count):
    path = work.path()
    cursors_by_origin = {origin: _EventCursor(work, paths_by_kind[origin]) for origin in ("source", "admission")}
    counts_by_origin = dict(source=0, admission=0)
    with ExitStack() as stack:
        streams_by_origin = {
            origin: stack.enter_context(closing(cursor.events())) for origin, cursor in cursors_by_origin.items()
        }
        output = stack.enter_context(path.open("xb"))
        for ordinal, event in kernel.rejection_ordinals(streams_by_origin["source"], streams_by_origin["admission"]):
            body = cursors_by_origin[event.origin].current
            if event.origin == "source" and (body[3] is None or body[4] != ordinal):
                raise ValueError("admission initial SOURCE IDs/ordinals differ")
            numbered = AdmissionEvent(body[0], ordinal, body[1], body[2], body[3], *body[5:])
            work.frame(output, ("event", tuple(vars(numbered).values())), path)
            counts_by_origin[event.origin] += 1
    if counts_by_origin["source"] != initial_count:
        raise ValueError("admission original initial rejection census differs")
    return path, counts_by_origin


def _first_targets(work, path):
    ordered = _sorted(work, work.rows(path), lambda row: (row[0], row[1]))
    with closing(ordered):
        targets = (kernel.RejectionTarget(row[0], kernel.RejectionEvent(row[2], row[1], row[3])) for row in ordered)
        for target in kernel.first_resolutions(targets):
            yield (target.event.origin, target.event.occurrence, target.event.code), target.occurrence


def _events_by_key(work, path):
    for _kind, cells in work.rows(path):
        event = AdmissionEvent(*cells)
        yield _event_key(event), cells


def _resolutions(work, paths_by_kind, events):
    first = _sorted(work, _first_targets(work, paths_by_kind["target"]), lambda row: row[0])
    numbered = _sorted(work, _events_by_key(work, events), lambda row: row[0])

    def joined():
        """Bind each earliest target to an actual initial or pending admission event."""
        with closing(first), closing(numbered):
            current = next(numbered, None)
            for event_key, target in first:
                while current is not None and current[0] < event_key:
                    current = next(numbered, None)
                if current is None or current[0] != event_key:
                    raise ValueError("admission resolved target has no original event")
                yield target, current[1]

    ordered = _sorted(work, joined(), lambda row: row[0])
    path, count = work.path(), 0
    with closing(ordered), closing(work.rows(paths_by_kind["occurrence"])) as occurrences, path.open("xb") as output:
        current = next(occurrences, None)
        previous = None
        for target, event in ordered:
            while current is not None and current[1][0] < target:
                current = next(occurrences, None)
            if current is None or current[1][0] != target or previous == target:
                raise ValueError("admission resolved target occurrence is missing or repeats")
            work.frame(output, ("resolution", target, event), path)
            previous, count = target, count + 1
    return path, count


def _digest_file(source, maximum, check):
    digest = hashlib.sha256()
    while True:
        if check is not None:
            check()
        block = source.read(min(65536, maximum))
        if not block:
            return digest.digest()
        digest.update(block)


def _seal(work, path):
    ready = work.root / _FILE
    os.chmod(path, 0o400)
    with path.open("rb") as source:
        os.fsync(source.fileno())
        digest = _digest_file(source, work.limits.max_record_bytes, work.check)
    os.link(path, ready, follow_symlinks=False)
    path.unlink()
    work.paths[ready] = work.paths.pop(path)
    directory = os.open(work.root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return ready, digest


def _header(source, definition, workers, complete_scope):
    return (
        _CONTRACT,
        source.source.sha256,
        source.sha256,
        source.source.scope,
        source.snapshot_family_id,
        definition.digest,
        complete_scope,
        workers,
    )


def build_admission_output(allocated_source, definition, workers, *, work, complete_scope):
    """Independently rederive and exactly join ALL source/worker evidence.

    This adds a complete global fact/external-sort pass to pipeline cost. It
    never chooses retained content, allocates admission IDs or records a proof.
    Caller-owned worker/source artifacts remain open and unchanged after return.
    """
    _scope(allocated_source, definition, workers, work, complete_scope)
    scratch = _Scratch(work)
    try:
        reference, counts_by_kind = _reference(scratch, allocated_source, definition, complete_scope)
        observed, headers, inputs = _workers(scratch, allocated_source, definition, workers, complete_scope)
        family_path = _families(scratch, reference, observed)
        paths_by_kind = _joined(scratch, allocated_source, definition, reference, observed, len(workers))
        event_path, event_counts = _number_events(scratch, paths_by_kind, allocated_source.source.counts[4])
        resolution_path, resolutions = _resolutions(scratch, paths_by_kind, event_path)
        counts_by_kind |= {
            "workers": len(workers),
            "source_events": event_counts["source"],
            "admission_events": event_counts["admission"],
            "resolutions": resolutions,
        }
        counts = tuple(counts_by_kind.items())
        sections = (
            ("worker", headers),
            ("family", family_path),
            ("occurrence", paths_by_kind["occurrence"]),
            ("event", event_path),
            ("resolution", resolution_path),
        )
        path = _completed_file(scratch, _header(allocated_source, definition, inputs, complete_scope), sections, counts)
        return _completed_descriptor(scratch, path, allocated_source, definition, complete_scope, inputs, counts)
    except BaseException:
        scratch.close()
        raise


def _completed_file(work, header, sections, counts):
    path = work.path()
    with path.open("xb") as output:
        work.frame(output, ("header", *header), path)
        for kind, section_path in sections:
            entries = work.rows(section_path)
            if kind == "worker":
                entries = _sorted(work, entries, lambda entry: entry[1])
            _copy_section(work, output, path, kind, entries)
        work.frame(output, ("complete", counts), path)
    for temporary in tuple(work.paths):
        if temporary != path:
            work.remove(temporary)
    return path


def _copy_section(work, output, path, kind, entries):
    with closing(entries):
        for entry in entries:
            if entry[0] != kind:
                raise ValueError("admission completed section kind differs")
            work.frame(output, entry, path)


def _completed_descriptor(work, path, source, definition, complete_scope, inputs, counts):
    ready, digest = _seal(work, path)
    info = ready.stat()
    return AdmissionOutputDescriptor(
        str(work.root),
        work.identity.st_dev,
        work.identity.st_ino,
        info.st_dev,
        info.st_ino,
        info.st_size,
        digest,
        source.source.sha256,
        source.sha256,
        source.source.scope,
        source.snapshot_family_id,
        definition.digest,
        complete_scope,
        inputs,
        counts,
        work.limits.max_record_bytes,
    )


def _owned_file(descriptor):
    if type(descriptor) is not AdmissionOutputDescriptor:
        raise ValueError("admission descriptor differs")
    root = Path(descriptor.root)
    directory, path = root.lstat(), root / _FILE
    observed = path.lstat()
    if not stat.S_ISDIR(directory.st_mode) or (directory.st_dev, directory.st_ino) != (
        descriptor.device,
        descriptor.inode,
    ):
        raise ValueError("admission root custody differs")
    if (
        not stat.S_ISREG(observed.st_mode)
        or (observed.st_dev, observed.st_ino, observed.st_size)
        != (descriptor.file_device, descriptor.file_inode, descriptor.file_bytes)
        or {entry.name for entry in root.iterdir()} != {_FILE}
    ):
        raise ValueError("admission completed file custody differs")
    return path


def _entries(descriptor, check):
    path = _owned_file(descriptor)
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as source:
        observed = os.fstat(source.fileno())
        if (observed.st_dev, observed.st_ino, observed.st_size) != (
            descriptor.file_device,
            descriptor.file_inode,
            descriptor.file_bytes,
        ):
            raise ValueError("admission open file custody differs")
        digest = hashlib.sha256()
        while prefix := source.read(8):
            if check is not None:
                check()
            if len(prefix) != 8:
                raise ValueError("admission frame prefix is truncated")
            size = _LENGTH.unpack(prefix)[0]
            if not 0 < size <= descriptor.max_frame_bytes:
                raise ValueError("admission frame exceeds operational budget")
            packed = source.read(size)
            if len(packed) != size:
                raise ValueError("admission frame is truncated")
            digest.update(prefix)
            digest.update(packed)
            yield _unpack_row(packed)
        if digest.digest() != descriptor.sha256:
            raise ValueError("admission complete EOF bytes differ")


def _checked_entries(descriptor, check):
    expected_header = (
        "header",
        _CONTRACT,
        descriptor.source_sha256,
        descriptor.allocation_sha256,
        descriptor.source_scope,
        descriptor.snapshot_family_id,
        descriptor.definition_sha256,
        descriptor.complete_scope,
        descriptor.worker_inputs,
    )
    counts_by_section = dict.fromkeys(_SECTIONS, 0)
    previous_section, previous_key, is_complete = -1, None, False
    with closing(_entries(descriptor, check)) as entries:
        if next(entries, None) != expected_header:
            raise ValueError("admission complete header differs")
        for entry in entries:
            if is_complete:
                raise ValueError("admission has trailing output records")
            if entry[0] == "complete":
                if entry != ("complete", descriptor.counts):
                    raise ValueError("admission completion census differs")
                is_complete = True
                continue
            section = _SECTIONS.index(entry[0])
            if section < previous_section:
                raise ValueError("admission output section order differs")
            key = _entry_key(entry)
            if section == previous_section and key <= previous_key:
                raise ValueError("admission output occurrence/event order repeats")
            previous_section, previous_key = section, key
            counts_by_section[entry[0]] += 1
            yield entry
    counts = descriptor.counts_by_kind
    expected_by_section = {
        "worker": counts["workers"],
        "family": counts["families"],
        "occurrence": counts["records"],
        "event": counts["source_events"] + counts["admission_events"],
        "resolution": counts["resolutions"],
    }
    if not is_complete or counts_by_section != expected_by_section:
        raise ValueError("admission output incomplete EOF/section coverage")


def _entry_key(entry):
    match entry[0]:
        case "worker":
            return entry[1]
        case "family":
            return _typed_order(entry[2]["typed_key"])
        case "occurrence":
            return entry[1][0]
        case "event":
            return entry[1][1]
        case "resolution":
            return entry[1]
    raise ValueError("admission output kind differs")


def read_admission_occurrences(descriptor, *, check=None):
    """Replay every original occurrence, facts and decisions; require full EOF."""
    with closing(_checked_entries(descriptor, check)) as entries:
        for entry in entries:
            if entry[0] == "occurrence":
                yield AdmissionOccurrence(*entry[1])


def read_admission_families(descriptor, *, check=None):
    """Replay preserved provisional headers; never final base-aware selection."""
    with closing(_checked_entries(descriptor, check)) as entries:
        for entry in entries:
            if entry[0] == "family":
                yield entry[1], entry[2]


def read_admission_events(descriptor, *, check=None):
    """Replay SOURCE IDs first, then pending admission coordinates and codes."""
    with closing(_checked_entries(descriptor, check)) as entries:
        for entry in entries:
            if entry[0] == "event":
                yield AdmissionEvent(*entry[1])


def read_admission_resolutions(descriptor, *, check=None):
    """Replay each exact earliest target resolution, with its original event."""
    with closing(_checked_entries(descriptor, check)) as entries:
        for entry in entries:
            if entry[0] == "resolution":
                yield AdmissionResolution(entry[1], AdmissionEvent(*entry[2]))


def cleanup_admission_output(descriptor):
    """Remove only the exact caller-registered completed admission root."""
    _owned_file(descriptor)
    shutil.rmtree(descriptor.root)
