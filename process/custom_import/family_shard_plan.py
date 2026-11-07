# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded original base-aware family selection over completed owner evidence."""

import asyncio
import hashlib
import os
import shutil
import stat
import struct
import threading
from contextlib import ExitStack, aclosing, closing
from dataclasses import dataclass
from pathlib import Path

from process.custom_import import family_shard_build as kernel
from process.custom_import.build_source import SourceBuildRequest
from process.custom_import.continuation_runtime import producer_token_sha256
from process.custom_import.family import CandidateRejected
from process.custom_import.family_shard_admission import (
    AdmissionEvent,
    AdmissionOccurrence,
    AdmissionOutputDescriptor,
    AdmissionResolution,
    _checked_entries,
)
from process.custom_import.family_shard_facts import SourceFactLimits, _sorted, _Work
from process.custom_import.family_shard_output import _input_iterator, _pack_row, _unpack_row
from process.custom_import.runner_codec import digest_text
from process.custom_import.runner_types import CancellationRequested


@dataclass(frozen=True)
class BaseFamilyHeader:
    """Original pinned base identity; this value alone grants no origin authority.

    A NULL snapshot is only an explicitly resolved canonical legacy base. The
    reader validates its key contract against the actual base definition; this
    consumer never canonicalizes it under the replacement definition.
    """

    typed_key: tuple
    root_record_id: int
    family_revision_id: int
    root_revision_id: int
    entity_binding_id: int
    family_sha256: bytes
    child_count: int
    base_generation_id: int
    base_snapshot_family_id: int | None


_CONTRACT = "custom-import/final-family-plan/v1"
_FILE = "family-plan.mpk"
_LENGTH = struct.Struct("!Q")


@dataclass(frozen=True)
class PlanWork:
    """Accounted owner scratch; limits are resource budgets, never input caps."""

    root: object
    limits: SourceFactLimits
    register_cleanup: object
    check: object = None

    def __post_init__(self):
        if type(self.limits) is not SourceFactLimits or not callable(self.register_cleanup):
            raise ValueError("family plan requires explicit accounted work")
        if self.check is not None and not callable(self.check):
            raise ValueError("family plan cancellation check differs")


@dataclass(frozen=True)
class FinalFamilyPlan:
    """Original source/base evidence and selection; retained destination IDs unset."""

    typed_key: tuple
    selection_kind: str
    source_header: dict | None
    source_root: AdmissionOccurrence | None
    source_metadata: tuple | None
    base_header: BaseFamilyHeader | None
    source_resolution: AdmissionResolution | None

    @property
    def root_record_id(self):
        """Use an observed stable dictionary ID, never a newly invented identity."""
        return self.source_root.root_record_id if self.source_root is not None else self.base_header.root_record_id


@dataclass(frozen=True)
class FinalPlanDescriptor:
    """Closed file custody and declarations; no native pin/proof is minted."""

    root: str
    device: int
    inode: int
    file_device: int
    file_inode: int
    file_bytes: int
    sha256: bytes
    admission_sha256: bytes
    request_scope: tuple
    base_scope: tuple | None
    counts: tuple
    max_frame_bytes: int

    @property
    def counts_by_kind(self):
        """Return union/selection/audit counts, not BuildVerification counts."""
        return dict(self.counts)


class _Scratch(_Work):
    def __init__(self, plan):
        self.stop, self.owner_check = threading.Event(), plan.check
        super().__init__(plan.root, plan.limits, plan.register_cleanup, check=self.current)

    def current(self):
        """Keep CPU merge cancellation and the existing owner deadline responsive."""
        if self.stop.is_set():
            raise CancellationRequested("family plan task was cancelled")
        if self.owner_check is not None:
            self.owner_check()


async def _joined_cpu(task):
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
        except BaseException:
            break
    if not task.cancelled():
        task.exception()


async def _cpu(work, operation, *arguments):
    task = asyncio.create_task(asyncio.to_thread(operation, *arguments))
    try:
        return await asyncio.shield(task)
    except BaseException:
        work.stop.set()
        await _joined_cpu(task)
        raise


def _positive(identifier, label):
    if type(identifier) is not int or not 0 < identifier < 2**63:
        raise ValueError(f"family plan {label} differs")


def _key_order(typed_key):
    if (
        type(typed_key) is not tuple
        or len(typed_key) != 2
        or type(typed_key[0]) is not str
        or not typed_key[0]
        or type(typed_key[1]) is not bytes
        or len(typed_key[1]) != 32
        or digest_text("root-key", typed_key[0]) != typed_key[1]
    ):
        raise ValueError("family plan exact typed-key evidence differs")
    return typed_key[1], typed_key[0]


def _scope(request, admission, base_scope, work):
    if (
        type(request) is not SourceBuildRequest
        or type(admission) is not AdmissionOutputDescriptor
        or type(work) is not PlanWork
    ):
        raise ValueError("family plan requires its original typed owner inputs")
    identity_by_name = dict(admission.source_scope[0])
    if (
        admission.definition_sha256 != request.definition.digest
        or admission.complete_scope != request.complete_scope
        or tuple(identity_by_name[name] for name in ("dataset_id", "definition_revision_id", "schema_revision_id"))
        != (request.dataset_id, request.definition_revision_id, request.schema_revision_id)
        or admission.source_scope[1:]
        != (
            request.execution_id,
            request.fence,
            producer_token_sha256(request),
            request.page_row_limit,
            request.page_byte_limit,
        )
    ):
        raise ValueError("family plan SOURCE/admission producer scope differs")
    if request.expected_base_generation_id is None:
        if base_scope is not None:
            raise ValueError("family plan unexpected pinned base scope")
    else:
        if (
            type(base_scope) is not tuple
            or len(base_scope) != 2
            or base_scope[0] != request.expected_base_generation_id
        ):
            raise ValueError("family plan requires explicit actual pinned base scope")
        _positive(base_scope[0], "base generation")
        if base_scope[1] is not None:
            _positive(base_scope[1], "base snapshot")
    return (
        request.dataset_id,
        request.definition_revision_id,
        request.schema_revision_id,
        request.execution_id,
        request.fence,
        producer_token_sha256(request),
        request.expected_base_generation_id,
        request.expected_pointer_version,
        request.definition.refresh_mode,
        request.complete_scope,
    )


def _stage_family(work, paths_by_kind, files_by_kind, summary_by_kind, entry, failures):
    header = entry[2]
    summary_by_kind["candidate_errors"] += header["candidate_errors"]
    summary_by_kind["fatal_families"] += int(header["fatal_problem"] is not None)
    if header["fatal_problem"] is not None and (failures[0] is None or header["fatal_order"] < failures[0][0]):
        failures[0] = header["fatal_order"], header["fatal_problem"]
    if failures[1] is None and header["selection"] == "error":
        failures[1] = header["selection_problem"]
    kind = "invalid" if header["typed_key"] is None else "family"
    work.frame(files_by_kind[kind], entry, paths_by_kind[kind])
    summary_by_kind["invalid_families" if kind == "invalid" else "source_families"] += 1


def _stage_entry(work, paths_by_kind, files_by_kind, summary_by_kind, entry, failures):
    if entry[0] == "family":
        _stage_family(work, paths_by_kind, files_by_kind, summary_by_kind, entry, failures)
    elif entry[0] == "occurrence":
        _stage_occurrence(work, paths_by_kind, files_by_kind, entry)
        summary_by_kind["invalid_occurrences"] += int(AdmissionOccurrence(*entry[1]).record.typed_parent_key is None)
    elif entry[0] == "resolution":
        work.frame(files_by_kind["resolution"], entry, paths_by_kind["resolution"])


def _stage_admission(work, admission):
    paths_by_kind = {kind: work.path() for kind in ("family", "root", "valid", "resolution", "invalid")}
    summary_by_kind = dict(
        source_families=0, invalid_families=0, invalid_occurrences=0, candidate_errors=0, fatal_families=0
    )
    failures = [None, None]
    with ExitStack() as stack:
        files_by_kind = {kind: stack.enter_context(path.open("xb")) for kind, path in paths_by_kind.items()}
        entries = stack.enter_context(closing(_checked_entries(admission, work.current)))
        for entry in entries:
            _stage_entry(work, paths_by_kind, files_by_kind, summary_by_kind, entry, failures)
    return paths_by_kind, summary_by_kind, *failures


def _stage_occurrence(work, paths_by_kind, files_by_kind, entry):
    occurrence = AdmissionOccurrence(*entry[1])
    record = occurrence.record
    if record.typed_parent_key is None:
        work.frame(files_by_kind["invalid"], entry, paths_by_kind["invalid"])
    if record.collection_slot == 0 and record.typed_parent_key is not None:
        work.frame(files_by_kind["root"], (occurrence.coordinate, entry[1]), paths_by_kind["root"])
        if occurrence.revision_id is not None:
            work.frame(
                files_by_kind["valid"],
                (_key_order(record.typed_parent_key), occurrence.root_record_id),
                paths_by_kind["valid"],
            )


def _metadata(work, source_metadata):
    path, count = work.path(), 0
    previous = None
    with closing(_input_iterator(source_metadata)) as assignments, path.open("xb") as output:
        for typed_key, identities in assignments:
            order = _key_order(typed_key)
            if previous is not None and order <= previous:
                raise ValueError("family plan source metadata repeats or is unordered")
            if type(identities) is not tuple or len(identities) != 2:
                raise ValueError("family plan source metadata assignment differs")
            for identifier in identities:
                _positive(identifier, "source reservation")
            work.frame(output, (order, identities), path)
            previous, count = order, count + 1
    return path, count


def _stage_source(work, admission, source_metadata):
    staged = _stage_admission(work, admission)
    metadata_path, count = _metadata(work, source_metadata)
    staged[1]["metadata_rows"] = count
    return *staged, metadata_path


def _base_header(header, base_scope):
    if type(header) is not BaseFamilyHeader or base_scope is None:
        raise ValueError("family plan base header requires its actual pinned scope")
    if (header.base_generation_id, header.base_snapshot_family_id) != base_scope:
        raise ValueError("family plan base header pin/snapshot differs")
    for identifier in (
        header.root_record_id,
        header.family_revision_id,
        header.root_revision_id,
        header.entity_binding_id,
    ):
        _positive(identifier, "base identity")
    if (
        type(header.family_sha256) is not bytes
        or len(header.family_sha256) != 32
        or type(header.child_count) is not int
        or header.child_count < 0
    ):
        raise ValueError("family plan original base counts/digest differ")
    return _key_order(header.typed_key)


async def _stage_base(work, base_headers, base_scope):
    if base_scope is not None and base_headers is None:
        raise ValueError("family plan requires the actual pinned base header stream")
    if base_headers is None:
        return work.write(()), 0
    path, count, previous = work.path(), 0, None
    with path.open("xb") as output:
        iterator = base_headers.__aiter__()
        async with aclosing(iterator):
            async for header in iterator:
                work.current()
                order = _base_header(header, base_scope)
                if previous is not None and order <= previous:
                    raise ValueError("family plan base keys repeat or are unordered")
                work.frame(output, (order, tuple(vars(header).values())), path)
                previous, count = order, count + 1
    return path, count


def _source_requests(work, path):
    for _kind, shard, header in work.rows(path):
        if header["source_root"] is None:
            raise ValueError("family plan typed source family lacks its original root")
        yield header["source_root"], (shard, header)


def _root_resolution(target, entry):
    if entry is None or entry[1] != target:
        return None
    return AdmissionResolution(target, AdmissionEvent(*entry[2]))


def _expected_resolution(header):
    original = header["root_resolution"]
    if original is None:
        return None
    return kernel.RejectionTarget(original["occurrence"], kernel.RejectionEvent(**original["event"]))


def _recover_root(header, occurrence, resolution):
    record = occurrence.record
    if (
        record.collection_slot != 0
        or record.typed_parent_key != header["typed_key"]
        or occurrence.coordinate != header["source_root"]
        or occurrence.family_key != header["typed_key"]
        or _pack_row(occurrence.facts_by_name) != _pack_row(header["source_facts"])
    ):
        raise ValueError("family plan original source-root bytes/identity/facts differ")
    _positive(occurrence.root_record_id, "observed source root")
    target = (
        None
        if resolution is None
        else kernel.RejectionTarget(
            resolution.target,
            kernel.RejectionEvent(resolution.event.origin, resolution.event.coordinate, resolution.event.code),
        )
    )
    if target != _expected_resolution(header):
        raise ValueError("family plan earliest source-root resolution differs")


def _source_roots(work, paths_by_kind):
    path = work.path()
    requested = _sorted(work, _source_requests(work, paths_by_kind["family"]), lambda entry: entry[0])
    with (
        closing(requested),
        closing(work.rows(paths_by_kind["root"])) as originals,
        closing(work.rows(paths_by_kind["resolution"])) as resolutions,
        path.open("xb") as output,
    ):
        current, resolved = next(originals, None), next(resolutions, None)
        for coordinate, (shard, header) in requested:
            while current is not None and current[0] < coordinate:
                current = next(originals, None)
            while resolved is not None and resolved[1] < coordinate:
                resolved = next(resolutions, None)
            if current is None or current[0] != coordinate:
                raise ValueError("family plan source-root occurrence is missing")
            occurrence = AdmissionOccurrence(*current[1])
            resolution = _root_resolution(coordinate, resolved)
            _recover_root(header, occurrence, resolution)
            work.frame(
                output,
                (
                    _key_order(header["typed_key"]),
                    shard,
                    header,
                    current[1],
                    None if resolution is None else (resolution.target, tuple(vars(resolution.event).values())),
                ),
                path,
            )
    return path


def _valid_roots(work, path):
    ordered = _sorted(work, work.rows(path), lambda entry: entry)
    with closing(ordered):
        previous = None
        for order, identity in ordered:
            if previous is not None and previous[0] == order:
                if previous[1] != identity:
                    raise ValueError("family plan same source key rebinds its native root")
                continue
            _positive(identity, "valid original root")
            yield order, identity
            previous = order, identity


def _sources(work, paths_by_kind, metadata_path):
    recovered = _source_roots(work, paths_by_kind)
    ordered = _sorted(work, work.rows(recovered), lambda entry: entry[0])
    valid = _valid_roots(work, paths_by_kind["valid"])
    with closing(ordered), closing(valid), closing(work.rows(metadata_path)) as metadata:
        root, assignment = next(valid, None), next(metadata, None)
        for order, _shard, header, original, resolution in ordered:
            if (root is not None and root[0] < order) or (assignment is not None and assignment[0] < order):
                raise ValueError("family plan metadata/root has no source family")
            identities = None
            if root is not None and root[0] == order:
                if (
                    assignment is None
                    or assignment[0] != order
                    or assignment[1] != header["family_ids"]
                    or original[4] != root[1]
                ):
                    raise ValueError("family plan worker/source native reservations differ")
                identities = assignment[1]
                root, assignment = next(valid, None), next(metadata, None)
            elif header["family_ids"] is not None or (assignment is not None and assignment[0] == order):
                raise ValueError("family plan reservation lacks an initially valid original root")
            yield order, header, original, identities, resolution
        if root is not None or assignment is not None:
            raise ValueError("family plan source metadata has extra original keys")


def _selection(request, source, base):
    header = None if source is None else source[1]
    original = None if source is None else AdmissionOccurrence(*source[2])
    facts = None if header is None else kernel.SourceAdmissionFacts(**header["source_facts"])
    resolution = None if header is None else _expected_resolution(header)
    selection = kernel.family_selection(
        None if original is None else original.record,
        resolution,
        has_base=base is not None,
        refresh_mode=request.definition.refresh_mode,
        complete_scope=request.complete_scope,
        source_facts=facts,
    )
    if source is not None and base is not None and original.root_record_id != base.root_record_id:
        raise ValueError("family plan source/base dictionary identity rebinds")
    if selection == "source" and (source[3] is None or original.revision_id is None):
        raise ValueError("family plan source selection lacks actual observed native IDs")
    typed_key = header["typed_key"] if source is not None else base.typed_key
    resolved = (
        None
        if source is None or source[4] is None
        else AdmissionResolution(source[4][0], AdmissionEvent(*source[4][1]))
    )
    return FinalFamilyPlan(
        typed_key, selection, header, original, None if source is None else source[3], base, resolved
    )


def _plan_cells(plan):
    return (
        plan.typed_key,
        plan.selection_kind,
        plan.source_header,
        None if plan.source_root is None else tuple(vars(plan.source_root).values()),
        plan.source_metadata,
        None if plan.base_header is None else tuple(vars(plan.base_header).values()),
        None
        if plan.source_resolution is None
        else (plan.source_resolution.target, tuple(vars(plan.source_resolution.event).values())),
    )


def _plan(cells):
    return FinalFamilyPlan(
        cells[0],
        cells[1],
        cells[2],
        None if cells[3] is None else AdmissionOccurrence(*cells[3]),
        cells[4],
        None if cells[5] is None else BaseFamilyHeader(*cells[5]),
        None if cells[6] is None else AdmissionResolution(cells[6][0], AdmissionEvent(*cells[6][1])),
    )


def _union(work, request, source, base_path):
    with closing(source), closing(work.rows(base_path)) as base_entries:
        current, prior, previous = next(source, None), next(base_entries, None), None
        while current is not None or prior is not None:
            order = prior[0] if current is None else current[0] if prior is None else min(current[0], prior[0])
            if previous is not None and order[0] == previous[0] and order[1] != previous[1]:
                raise ValueError("family plan root-key digest collision")
            selected_source = current if current is not None and current[0] == order else None
            base = BaseFamilyHeader(*prior[1]) if prior is not None and prior[0] == order else None
            yield _selection(request, selected_source, base)
            if selected_source is not None:
                current = next(source, None)
            if base is not None:
                prior = next(base_entries, None)
            previous = order


def _identity_entries(work, paths_by_kind, base_path, metadata_path):
    for _coordinate, cells in work.rows(paths_by_kind["root"]):
        root = AdmissionOccurrence(*cells)
        yield ("root", root.root_record_id), root.record.typed_parent_key
    for _order, cells in work.rows(base_path):
        base = BaseFamilyHeader(*cells)
        for kind, identity in (
            ("root", base.root_record_id),
            ("base_family", base.family_revision_id),
            ("base_revision", base.root_revision_id),
        ):
            yield (kind, identity), base.typed_key
    for order, identities in work.rows(metadata_path):
        yield ("source_family", identities[0]), (order[1], order[0])


def _identities(work, paths_by_kind, base_path, metadata_path):
    entries = _sorted(work, _identity_entries(work, paths_by_kind, base_path, metadata_path), lambda entry: entry)
    with closing(entries):
        previous = None
        for identity, typed_key in entries:
            if previous is not None and previous[0] == identity and previous[1] != typed_key:
                raise ValueError("family plan native identity belongs to different exact keys")
            previous = identity, typed_key


def _error_guards(request, admission, staged):
    _paths, counts, fatal, error, _metadata = staged
    if fatal is not None:
        raise ValueError(fatal[1])
    if counts["candidate_errors"] or admission.counts_by_kind["candidate_errors"]:
        raise CandidateRejected("source candidate has occurrence errors")
    kernel.family_selection(
        None, None, has_base=False, refresh_mode=request.definition.refresh_mode, complete_scope=request.complete_scope
    )
    if error is not None:
        raise CandidateRejected(error)


def _sha256(source, maximum, check):
    digest = hashlib.sha256()
    while True:
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
        digest = _sha256(source, work.limits.max_record_bytes, work.current)
    os.link(path, ready, follow_symlinks=False)
    path.unlink()
    work.paths[ready] = work.paths.pop(path)
    directory = os.open(work.root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return ready, digest


def _finish(work, request, admission, staged, base, request_scope, base_scope):
    _error_guards(request, admission, staged)
    paths_by_kind, counts_by_kind, _fatal, _error, metadata_path = staged
    base_path, base_count = base
    _identities(work, paths_by_kind, base_path, metadata_path)
    path = work.path()
    counts_by_kind |= dict(base_families=base_count, source=0, retained=0, absent=0, families=0)
    with path.open("xb") as output:
        work.frame(output, ("header", _CONTRACT, admission.sha256, request_scope, base_scope), path)
        union = _union(work, request, _sources(work, paths_by_kind, metadata_path), base_path)
        with closing(union):
            for plan in union:
                counts_by_kind[plan.selection_kind] += 1
                counts_by_kind["families"] += 1
                work.frame(output, ("family", _plan_cells(plan)), path)
        for entry in work.rows(paths_by_kind["invalid"]):
            work.frame(output, ("invalid", entry), path)
        counts = tuple(counts_by_kind.items())
        work.frame(output, ("complete", counts), path)
    for temporary in tuple(work.paths):
        if temporary != path:
            work.remove(temporary)
    ready, digest = _seal(work, path)
    info = ready.stat()
    return FinalPlanDescriptor(
        str(work.root),
        work.identity.st_dev,
        work.identity.st_ino,
        info.st_dev,
        info.st_ino,
        info.st_size,
        digest,
        admission.sha256,
        request_scope,
        base_scope,
        counts,
        work.limits.max_record_bytes,
    )


async def build_family_plan(request, admission, base_headers, source_metadata, *, work, base_scope):
    """Consume completed source, metadata and ONE native async base header stream.

    Keep the caller's pinned-base context open through this call and later full
    retained-value copy. Its own verified EOF/counts/pin grant origin authority;
    no scope tuple, header or output descriptor can grant that authority.
    """
    request_scope = _scope(request, admission, base_scope, work)
    scratch = _Scratch(work)
    try:
        staged = await _cpu(scratch, _stage_source, scratch, admission, source_metadata)
        base = await _stage_base(scratch, base_headers, base_scope)
        return await _cpu(scratch, _finish, scratch, request, admission, staged, base, request_scope, base_scope)
    except BaseException:
        scratch.close()
        raise


def _owned_file(descriptor):
    if type(descriptor) is not FinalPlanDescriptor:
        raise ValueError("family plan descriptor differs")
    root = Path(descriptor.root)
    directory, path = root.lstat(), root / _FILE
    observed = path.lstat()
    if not stat.S_ISDIR(directory.st_mode) or (directory.st_dev, directory.st_ino) != (
        descriptor.device,
        descriptor.inode,
    ):
        raise ValueError("family plan root custody differs")
    if (
        not stat.S_ISREG(observed.st_mode)
        or (observed.st_dev, observed.st_ino, observed.st_size)
        != (descriptor.file_device, descriptor.file_inode, descriptor.file_bytes)
        or {entry.name for entry in root.iterdir()} != {_FILE}
    ):
        raise ValueError("family plan file custody differs")
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
            raise ValueError("family plan open custody differs")
        digest = hashlib.sha256()
        while prefix := source.read(8):
            if check is not None:
                check()
            if len(prefix) != 8:
                raise ValueError("family plan frame prefix differs")
            size = _LENGTH.unpack(prefix)[0]
            if not 0 < size <= descriptor.max_frame_bytes:
                raise ValueError("family plan frame exceeds resource budget")
            packed = source.read(size)
            if len(packed) != size:
                raise ValueError("family plan frame is truncated")
            digest.update(prefix)
            digest.update(packed)
            yield _unpack_row(packed)
        if digest.digest() != descriptor.sha256:
            raise ValueError("family plan complete EOF bytes differ")


def _plan_census(cells, previous, counts_by_kind, is_invalid):
    plan = _plan(cells)
    order = _key_order(plan.typed_key)
    if (
        is_invalid
        or (previous is not None and order <= previous)
        or plan.selection_kind not in ("source", "retained", "absent")
    ):
        raise ValueError("family plan key/selection order differs")
    counts_by_kind["families"] += 1
    counts_by_kind[plan.selection_kind] += 1
    return order


def _plan_entry(entry, descriptor, counts_by_kind, state):
    previous, is_invalid, is_complete = state
    if is_complete:
        raise ValueError("family plan has trailing records")
    if entry[0] == "family":
        return _plan_census(entry[1], previous, counts_by_kind, is_invalid), is_invalid, False
    if entry[0] == "invalid":
        counts_by_kind["invalid_families" if entry[1][0] == "family" else "invalid_occurrences"] += 1
        return previous, True, False
    if entry != ("complete", descriptor.counts):
        raise ValueError("family plan completion differs")
    return previous, is_invalid, True


def _checked_plan_entries(descriptor, check):
    previous = None
    is_invalid = is_complete = False
    counts_by_kind = dict(families=0, source=0, retained=0, absent=0, invalid_families=0, invalid_occurrences=0)
    with closing(_entries(descriptor, check)) as entries:
        if next(entries, None) != (
            "header",
            _CONTRACT,
            descriptor.admission_sha256,
            descriptor.request_scope,
            descriptor.base_scope,
        ):
            raise ValueError("family plan header differs")
        for entry in entries:
            previous, is_invalid, is_complete = _plan_entry(
                entry, descriptor, counts_by_kind, (previous, is_invalid, is_complete)
            )
            yield entry
    expected_by_kind = descriptor.counts_by_kind
    if not is_complete or any(expected_by_kind[name] != count for name, count in counts_by_kind.items()):
        raise ValueError("family plan incomplete EOF/census")


def read_family_plan(descriptor, *, check=None):
    """Replay every source/retained/absent union key; require complete EOF."""
    with closing(_checked_plan_entries(descriptor, check)) as entries:
        for entry in entries:
            if entry[0] == "family":
                yield _plan(entry[1])


def read_invalid_plan_audit(descriptor, *, check=None):
    """Preserve invalid-key family/occurrence audit separately from typed union."""
    with closing(_checked_plan_entries(descriptor, check)) as entries:
        for entry in entries:
            if entry[0] == "invalid":
                yield entry[1]


def cleanup_family_plan(descriptor):
    """Remove only the exact registered completed plan root after use."""
    _owned_file(descriptor)
    shutil.rmtree(descriptor.root)
