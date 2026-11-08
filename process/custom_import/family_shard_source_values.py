# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded SOURCE family inputs from closed admission and final-selection evidence.

Values and scalar projections come from original source occurrences. Retained
selections remain a separate input path; no candidate table is an input oracle.
"""

import hashlib
import itertools
import os
import stat
from contextlib import ExitStack, closing, contextmanager
from dataclasses import astuple, replace
from pathlib import Path
from types import MappingProxyType

from process.custom_import import runner_codec as codec
from process.custom_import.family import _is_valid_npi
from process.custom_import.family_block_records import FamilyRecord, _integer
from process.custom_import.family_shard_admission import AdmissionOccurrence, read_admission_occurrences
from process.custom_import.family_shard_admission import _owned_file as _admission_file
from process.custom_import.family_shard_allocate import _NAME as _ALLOCATION_FILE
from process.custom_import.family_shard_allocate import _owned_root as _allocation_root
from process.custom_import.family_shard_base import _read_deadline, _require_deadline
from process.custom_import.family_shard_common import SourceCommonContext
from process.custom_import.family_shard_events import _scope as _source_scope
from process.custom_import.family_shard_facts import _sorted
from process.custom_import.family_shard_output import AllocatedFamily, _pack_row
from process.custom_import.family_shard_plan import (
    FinalPlanDescriptor,
    PlanWork,
    _key_order,
    _plan,
    _plan_cells,
    _scope,
    _Scratch,
    read_family_plan,
)
from process.custom_import.family_shard_plan import (
    _owned_file as _selection_file,
)
from process.custom_import.family_shard_source_plan import _owned_file as _source_file
from process.custom_import.materialization import (
    ChildScalarTarget,
    RootScalarTarget,
    project_child_scalars,
    project_root_scalars,
)


def _checked_scope(context, admission, final_plan, work):
    if (
        type(context) is not SourceCommonContext
        or type(final_plan) is not FinalPlanDescriptor
        or type(work) is not PlanWork
    ):
        raise ValueError("source values require explicit original context and accounted plan work")
    _source_scope(context.request, context.layout, context.source, admission)
    expected = _scope(context.request, admission, final_plan.base_scope, work)
    if final_plan.admission_sha256 != admission.sha256 or final_plan.request_scope != expected:
        raise ValueError("source values final-plan scope differs")


def _family_order(key):
    if key is None:
        return 1, b"", ""
    if (
        type(key) is not tuple
        or len(key) != 2
        or type(key[0]) is not str
        or type(key[1]) is not bytes
        or len(key[1]) != 32
    ):
        raise ValueError("source values effective family key differs")
    return 0, key[1], key[0]


def _collection(context, occurrence, record=None):
    record, definition = occurrence.record if record is None else record, context.request.definition
    if not 1 <= record.stream_slot <= len(definition.source_streams) or occurrence.coordinate != (
        record.stream_slot,
        record.source_ordinal,
    ):
        raise ValueError("source values occurrence stream or coordinate differs")
    collection = definition.source_streams[record.stream_slot - 1].child_collection
    ordinal = (
        0
        if collection is None
        else next(index for index, item in enumerate(definition.child_collections, 1) if item.name == collection)
    )
    native_slot = 0 if collection is None else context.layout.collection_slots[collection]
    if record.collection_slot != ordinal or occurrence.collection_slot != native_slot:
        raise ValueError("source values native collection scope differs")
    return collection


def _all_occurrences(context, admission, work):
    with closing(read_admission_occurrences(admission, check=work.current)) as occurrences:
        for occurrence in occurrences:
            _collection(context, occurrence)
            yield _family_order(occurrence.family_key), occurrence.coordinate, tuple(vars(occurrence).values())


def _selected_plans(final_plan, work):
    count = 0
    with closing(read_family_plan(final_plan, check=work.current)) as plans:
        for plan in plans:
            if plan.selection_kind == "source":
                count += 1
                yield _plan_cells(plan)
    if count != final_plan.counts_by_kind["source"]:
        raise ValueError("source values selected-family census differs")


def _native_ids(occurrence, root):
    for identifier in (occurrence.revision_id, occurrence.pack_id, occurrence.root_record_id):
        _integer(identifier, "source input native identity")
    if occurrence.root_record_id != root or occurrence.initial_rejection_id is not None:
        raise ValueError("source values initially valid native identity differs")


def _values(context, occurrence, record=None):
    definition = context.request.definition
    record = occurrence.record if record is None else record
    return record.normalized_values(definition, definition.source_streams[record.stream_slot - 1])


def _record(context, occurrence, collection):
    values = _values(context, occurrence)
    scope = context.request.dataset_id, context.request.schema_revision_id, occurrence.root_record_id
    if collection is None:
        projected = project_root_scalars(
            context.request.definition, root_target=RootScalarTarget(*scope, occurrence.revision_id), root_values=values
        )
    else:
        projected = project_child_scalars(
            context.request.definition,
            collection=collection,
            child_target=ChildScalarTarget(*scope, context.layout.collection_slots[collection], occurrence.revision_id),
            child_values=values,
            child_collection_slots=context.layout.collection_slots,
        )
    return FamilyRecord(
        occurrence.revision_id,
        occurrence.pack_id,
        occurrence.coordinate[1],
        MappingProxyType(values),
        MappingProxyType({projection.field_id: projection.scalar for projection in projected}),
    )


def _source_plan(context, plan, *, check_values=True):
    root, header = plan.source_root, plan.source_header
    if root is None or header is None or plan.source_resolution is not None or plan.source_metadata is None:
        raise ValueError("source values selection lacks its original un-rejected root")
    if (
        root.family_key != plan.typed_key
        or root.record.typed_parent_key != plan.typed_key
        or _collection(context, root) is not None
        or root.decision.code is not None
        or header["typed_key"] != plan.typed_key
        or header["source_root"] != root.coordinate
        or header["family_ids"] != plan.source_metadata
        or header["selection"] != "source"
        or header["root_resolution"] is not None
        or header["fatal_problem"] is not None
        or header["candidate_errors"]
        or _pack_row(header["source_facts"]) != _pack_row(root.facts_by_name)
    ):
        raise ValueError("source values selected root/header identity differs")
    _key_order(plan.typed_key)
    if type(plan.source_metadata) is not tuple or len(plan.source_metadata) != 2:
        raise ValueError("source values selected family/entity IDs differ")
    for identifier in plan.source_metadata:
        _integer(identifier, "source input family/entity")
    _native_ids(root, root.root_record_id)
    if not check_values:
        return
    root_values = _values(context, root)
    entity = root_values.get(context.request.definition.entity_field)
    if (
        codec.root_key_document(context.request.definition, root_values) != plan.typed_key[0]
        or not isinstance(entity, str)
        or not _is_valid_npi(entity)
    ):
        raise ValueError("source values root key/schema/entity differs")


class _SourceChildren:
    def __init__(self, family, collection):
        self.family, self.collection = family, collection

    def __call__(self):
        """Return fresh values/scalars for this selected collection while current."""
        return self.family.children(self.collection)

    def original_occurrences(self):
        """Borrow exact selected SOURCE evidence in the same native record order."""
        return self.family.original_occurrences(self.collection)

    def records_with_occurrences(self):
        """Pair independent landing custody with its one existing record decode."""
        return self.family.records_with_occurrences(self.collection)


class _SourceFamily:
    def __init__(self, context, plan, group, work, fields_by_collection):
        self.context, self.plan, self.work = context, plan, work
        self.fields_by_collection = fields_by_collection
        self.paths_by_collection, self.readers = {}, set()
        self.is_active = True
        self.has_root = False
        try:
            _source_plan(context, plan)
            self.root = _record(context, plan.source_root, None)
            self._prepare(group)
        except BaseException:
            self.close()
            raise

    def _accepted_child_order(self, occurrence, collection, record):
        if record.rejection_code is None:
            _native_ids(occurrence, self.plan.root_record_id)
        if occurrence.decision_by_name["code"] is not None:
            return None
        facts = occurrence.facts_by_name
        if facts["collapse_identical"] and facts["last_child"] != occurrence.coordinate:
            return None
        if record.typed_parent_key != self.plan.typed_key or record.child_key is None:
            raise ValueError("source values accepted child parent/key differs")
        values = _values(self.context, occurrence, record)
        if codec.child_key_document(self.context.request.definition, collection, values) != record.child_key:
            raise ValueError("source values accepted child key bytes differ")
        fields = self.fields_by_collection[collection]
        return codec.digest_text("child-key", record.child_key), record.child_key, codec.record_payload(fields, values)

    def _observe_root(self, occurrence, cells):
        if occurrence.coordinate == self.plan.source_root.coordinate:
            if self.has_root or _pack_row(cells) != _pack_row(tuple(vars(self.plan.source_root).values())):
                raise ValueError("source values original root bytes/native IDs differ")
            self.has_root = True
        elif occurrence.decision.code is None:
            raise ValueError("source values has another accepted root")

    def _prepare(self, group):
        unsorted_paths_by_collection = {name: self.work.path() for name in self.context.layout.collection_slots}
        self.paths_by_collection.update(unsorted_paths_by_collection)
        with ExitStack() as stack:
            files_by_collection = {
                name: stack.enter_context(path.open("xb")) for name, path in unsorted_paths_by_collection.items()
            }
            for _order, _coordinate, cells in group:
                self.work.current()
                occurrence = AdmissionOccurrence(*cells)
                record = occurrence.record
                collection = _collection(self.context, occurrence, record)
                if collection is None:
                    self._observe_root(occurrence, cells)
                    continue
                order = self._accepted_child_order(occurrence, collection, record)
                if order is not None:
                    self.work.frame(
                        files_by_collection[collection], (order, cells), unsorted_paths_by_collection[collection]
                    )
        if not self.has_root:
            raise ValueError("source values selected original root is missing")
        for name, path in unsorted_paths_by_collection.items():
            with closing(_sorted(self.work, self.work.rows(path), lambda row: row[0])) as rows:
                self.paths_by_collection[name] = self.work.write(rows)
            self.work.remove(path)

    def _rows(self, collection):
        if not self.is_active:
            raise ValueError("source family input is no longer active")
        reader = self.work.rows(self.paths_by_collection[collection])
        self.readers.add(reader)
        try:
            while True:
                if not self.is_active:
                    raise ValueError("source family input is no longer active")
                self.work.current()
                row = next(reader, None)
                if row is None:
                    return
                yield row
        finally:
            reader.close()
            self.readers.discard(reader)

    def children(self, collection):
        """Fresh independent records; factories remain borrowed until the next family."""
        with closing(self.records_with_occurrences(collection)) as records:
            for _occurrence, record in records:
                yield record

    def records_with_occurrences(self, collection):
        """Read each original frame once, retaining exact sidecar and record order."""
        with closing(self._rows(collection)) as rows:
            for _order, cells in rows:
                occurrence = AdmissionOccurrence(*cells)
                yield occurrence, _record(self.context, occurrence, collection)

    def original_occurrences(self, collection):
        """Keep raw parent/landing bytes independent of normalized record values."""
        with closing(self._rows(collection)) as rows:
            for _order, cells in rows:
                yield AdmissionOccurrence(*cells)

    def _documents(self, collection):
        with closing(self._rows(collection)) as rows:
            for order, _cells in rows:
                yield order

    def allocated(self):
        """Use the original ordered hash function and observed final-plan IDs."""
        definition = self.context.request.definition
        digest = codec.new_family_hash_ordered(
            definition,
            self.root.values_by_field,
            {name: self._documents(name) for name in self.paths_by_collection},
        )
        return AllocatedFamily(
            self.plan.source_metadata[0],
            self.plan.root_record_id,
            self.plan.source_metadata[1],
            self.root,
            MappingProxyType({name: _SourceChildren(self, name) for name in self.paths_by_collection}),
            digest,
        )

    def close(self):
        """Close active readers before removing only this family's temporary paths."""
        self.is_active = False
        for reader in tuple(self.readers):
            reader.close()
        self.readers.clear()
        for path in self.paths_by_collection.values():
            if path in self.work.paths and path.exists():
                self.work.remove(path)


def _occurrence_groups(context, selected, occurrences, work, read_rows):
    read_rows = work.rows if read_rows is None else read_rows
    with closing(read_rows(selected)) as plans, closing(read_rows(occurrences)) as rows:
        groups = itertools.groupby(rows, lambda row: row[0])
        current = next(groups, None)
        for cells in plans:
            plan = _plan(cells)
            order = _family_order(plan.typed_key)
            while current is not None and current[0] < order:
                for _unused in current[1]:
                    work.current()
                current = next(groups, None)
            if current is None or current[0] != order:
                raise ValueError("source values selected family has no original occurrences")
            yield plan, current[1]
            current = next(groups, None)
        for _order, group in () if current is None else itertools.chain((current,), groups):
            for _unused in group:
                work.current()


def _families(context, selected, occurrences, work, fields_by_collection, *, read_rows=None, selection=None):
    read_rows = work.rows if read_rows is None else read_rows
    decisions = iter(()) if selection is None else selection.source_decisions()
    try:
        with closing(_occurrence_groups(context, selected, occurrences, work, read_rows)) as groups:
            for plan, group in groups:
                if not _keep_source(plan, group, selection, decisions, work):
                    continue
                family = _SourceFamily(context, plan, group, work, fields_by_collection)
                try:
                    yield plan, family.allocated()
                finally:
                    family.close()
        if next(decisions, None) is not None:
            raise ValueError("prepared source audit has trailing selections")
    finally:
        close = getattr(decisions, "close", None)
        if close is not None:
            close()


def _keep_source(plan, group, selection, decisions, work):
    if selection is None:
        return True
    key, keep = next(decisions, (None, None))
    if key != plan.typed_key:
        raise ValueError("prepared source audit selection differs")
    if not keep:
        for _unused in group:
            work.current()
    return keep


def _input_signature(context, admission, final_plan, work):
    _checked_scope(context, admission, final_plan, work)
    request = context.request
    return (
        tuple(vars(context.source.source).items()),
        tuple((name, cell) for name, cell in vars(context.source).items() if name != "source"),
        tuple(vars(admission).items()),
        tuple(vars(final_plan).items()),
        context.layout.digest,
        tuple(sorted(context.layout.collection_slots.items())),
        _scope(request, admission, final_plan.base_scope, work),
        request.build_deadline_at,
        request.authorization_expires_at,
        request.operation_deadline,
    )


def _file_stamp(path):
    observed = path.lstat()
    if not stat.S_ISREG(observed.st_mode) or stat.S_IMODE(observed.st_mode) != 0o400:
        raise ValueError("prepared source file custody differs")
    return tuple(getattr(observed, "st_" + name) for name in ("dev", "ino", "size", "mtime_ns", "ctime_ns", "mode"))


def _input_stamps(context, admission, final_plan):
    allocated = context.source
    root = _allocation_root(allocated)
    if {entry.name for entry in root.iterdir()} != {_ALLOCATION_FILE}:
        raise ValueError("prepared source allocation file census differs")
    paths = (
        _source_file(allocated.source),
        root / _ALLOCATION_FILE,
        _admission_file(admission),
        _selection_file(final_plan),
    )
    if _file_stamp(paths[1])[:3] != (allocated.file_device, allocated.file_inode, allocated.file_bytes):
        raise ValueError("prepared source allocation custody differs")
    return tuple((path, _file_stamp(path)) for path in paths)


def _staged_digest(path, work):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while payload := source.read(min(work.limits.memory_bytes, 65536)):
            work.current()
            digest.update(payload)
    work.current()
    return digest.digest()


class _DigestInput:
    def __init__(self, source):
        self.source, self.digest = source, hashlib.sha256()

    def read(self, size):
        """Hash exactly the framed bytes consumed by the existing bounded reader."""
        payload = self.source.read(size)
        self.digest.update(payload)
        return payload


class _SourceReplay:
    def __init__(self, owner, observations=False, selection=None):
        self.owner, self.iterator, self.is_closed = owner, owner._replay(observations, selection), False

    def __iter__(self):
        return self

    def __next__(self):
        if self.is_closed:
            raise StopIteration
        try:
            return next(self.iterator)
        except BaseException:
            self.close()
            raise

    def close(self):
        """Close even an unstarted replay and release its exclusive borrowed slot."""
        if not self.is_closed:
            self.is_closed = True
            try:
                self.iterator.close()
            finally:
                self.owner._active = None
                self.owner._replay_check = None


class PreparedSourceFamilies:
    """One owned preparation; fresh sequential replays are borrowed until close.

    Original descriptor digests and complete admission/plan reads bind the two
    staged files. This is original-input preparation, never candidate acceptance.
    """

    def __init__(self, context, admission, final_plan, *, work):
        self.context, self.admission, self.final_plan, self.policy = context, admission, final_plan, work
        self._replay_check = None
        self._observation_token = None
        self.signature = _input_signature(context, admission, final_plan, work)
        self.inputs = _input_stamps(context, admission, final_plan)
        deadline = _read_deadline(context.request)

        def current():
            """Keep every replay inside the original deadline and owner stop check."""
            _require_deadline(deadline)
            if work.check is not None:
                work.check()
            if self._replay_check is not None:
                self._replay_check()

        current()
        self.work, self._active, self.is_closed = _Scratch(replace(work, check=current)), None, False
        try:
            self.selected = self.work.write(_selected_plans(final_plan, self.work))
            with closing(
                _sorted(self.work, _all_occurrences(context, admission, self.work), lambda row: row[:2])
            ) as ordered_occurrences:
                self.occurrences = self.work.write(ordered_occurrences)
            self.staged_by_path = {}
            for path in (self.selected, self.occurrences):
                path.chmod(0o400)
                self.staged_by_path[path] = _file_stamp(path), _staged_digest(path, self.work)
            self.fields_by_collection = codec.fields_by_collection(context.request.definition)
            self.check_scope(context, admission, final_plan)
        except BaseException:
            self.work.close()
            raise

    def check_scope(self, context, admission, final_plan):
        """Check captured provenance and current custody; no EOF proof is returned."""
        if self.is_closed:
            raise ValueError("prepared source families are closed")
        self.work.current()
        if self.signature != _input_signature(
            context, admission, final_plan, self.policy
        ) or self.inputs != _input_stamps(context, admission, final_plan):
            raise ValueError("prepared source original scope or custody differs")
        observed = self.work.root.lstat()
        if not stat.S_ISDIR(observed.st_mode) or (observed.st_dev, observed.st_ino) != (
            self.work.identity.st_dev,
            self.work.identity.st_ino,
        ):
            raise ValueError("prepared source work custody differs")
        if any(_file_stamp(path) != stamp for path, (stamp, _digest) in self.staged_by_path.items()):
            raise ValueError("prepared source staged custody differs")

    def _rows(self, path):
        stamp, digest = self.staged_by_path[path]
        with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as source:
            if (
                _file_stamp(path) != stamp
                or (os.fstat(source.fileno()).st_dev, os.fstat(source.fileno()).st_ino) != stamp[:2]
            ):
                raise ValueError("prepared source open custody differs")
            checked = _DigestInput(source)
            yield from self.work.read(checked)
            self.work.current()
            if checked.digest.digest() != digest or _file_stamp(path) != stamp:
                raise ValueError("prepared source complete EOF bytes differ")

    def _replay(self, observations=False, selection=None):
        self.check_scope(self.context, self.admission, self.final_plan)
        if observations:
            yield from self._observed_families()
            self.check_scope(self.context, self.admission, self.final_plan)
            return
        with closing(
            _families(
                self.context,
                self.selected,
                self.occurrences,
                self.work,
                self.fields_by_collection,
                read_rows=self._rows,
                selection=selection,
            )
        ) as families:
            yield from families
        self.check_scope(self.context, self.admission, self.final_plan)

    def _observed_families(self):
        with closing(
            _occurrence_groups(self.context, self.selected, self.occurrences, self.work, self._rows)
        ) as groups:
            for plan, occurrence_rows in groups:
                _source_plan(self.context, plan, check_values=False)
                token = self._observation_token = object()
                eof_flags = [False]
                try:
                    yield plan, self._observation_rows(occurrence_rows, token, eof_flags)
                    if not eof_flags[0]:
                        raise ValueError("original observations require complete family EOF")
                finally:
                    self._observation_token = None

    def _observation_rows(self, rows, token, eof_flags):
        while True:
            if self.is_closed or self._observation_token is not token:
                raise ValueError("original occurrence observation is no longer active")
            self.work.current()
            row = next(rows, None)
            if row is None:
                eof_flags[0] = True
                return
            yield AdmissionOccurrence(*row[2])

    def families(self, *, check=None, selection=None):
        """Borrow a fresh iterator with an optional requester stop check until close."""
        if selection is not None:
            from process.custom_import.family_shard_semantic_audit import SourceSemanticSelection

            if type(selection) is not SourceSemanticSelection:
                raise ValueError("prepared source requires its typed semantic selection")
            selection.check_scope(self.context, self.admission, self.final_plan, self)
        return self._borrow(check, selection=selection)

    def observations(self, *, check=None):
        """Borrow original occurrence groups before constructing any logical family."""
        return self._borrow(check, observations=True)

    def _borrow(self, check, *, observations=False, selection=None):
        if check is not None and not callable(check):
            raise ValueError("prepared source replay cancellation check differs")
        self.check_scope(self.context, self.admission, self.final_plan)
        if self._active is not None:
            raise ValueError("prepared source already has an active replay")
        self._replay_check = check
        try:
            self.work.current()
            self._active = _SourceReplay(self, observations, selection)
        except BaseException:
            self._replay_check = None
            raise
        return self._active

    def close(self):
        """Invalidate borrowed child readers before removing this exact work root."""
        if not self.is_closed:
            self.is_closed = True
            try:
                if self._active is not None:
                    self._active.close()
            finally:
                self.work.close()


def prepared_source_descriptor(prepared):
    """Retain only closed original files; this descriptor grants no native authority."""
    from process.custom_import.family_shard_source_checkpoint import _directory, _hash_file, _sync

    if type(prepared) is not PreparedSourceFamilies or prepared._active is not None:
        raise ValueError("prepared SOURCE custody requires its idle original producer")
    prepared.check_scope(prepared.context, prepared.admission, prepared.final_plan)
    files = []
    for role, path in (("selected", prepared.selected), ("occurrences", prepared.occurrences)):
        stamp, digest = prepared.staged_by_path[path]
        if _hash_file(path, check=prepared.work.current)[1] != digest:
            raise ValueError("prepared SOURCE custody bytes differ")
        with path.open("rb") as original:
            os.fsync(original.fileno())
        files.append((role, path.name, stamp, digest))
    _sync(prepared.selected.parent)
    original = getattr(prepared, "_original_descriptor", None)
    if original is not None:
        if tuple(files) != original[-2:]:
            raise ValueError("prepared SOURCE original roles changed")
        return original
    return (
        str(prepared.work.root),
        _directory(prepared.work.root),
        astuple(prepared.policy.limits),
        _pack_row(prepared.signature),
        tuple((str(path), stamp) for path, stamp in prepared.inputs),
        *files,
    )


def _restore_prepared_files(prepared, root, selected, occurrences, descriptor):
    from process.custom_import.family_shard_source_checkpoint import _hash_file

    for _role, name, stamp, digest in (selected, occurrences):
        if type(name) is not str or Path(name).name != name:
            raise ValueError("prepared SOURCE filename differs")
        path = root / name
        if _file_stamp(path) != stamp or _hash_file(path, check=prepared.work.current)[1] != digest:
            raise ValueError("prepared SOURCE retained file differs")
        prepared.staged_by_path[path] = stamp, digest
    prepared.selected, prepared.occurrences = root / selected[1], root / occurrences[1]
    if prepared.selected == prepared.occurrences or {path.name for path in root.iterdir()} != {
        selected[1],
        occurrences[1],
    }:
        raise ValueError("prepared SOURCE complete file census differs")
    prepared.fields_by_collection = codec.fields_by_collection(prepared.context.request.definition)
    prepared.check_scope(prepared.context, prepared.admission, prepared.final_plan)
    for path in prepared.staged_by_path:
        with closing(prepared._rows(path)) as original_rows:
            for _row in original_rows:
                prepared.work.current()
    prepared._original_descriptor = descriptor


def reopen_prepared_source(context, admission, final_plan, descriptor, *, work, replay_work):
    """Borrow the exact two committed files, never rerun original preparation."""
    from process.custom_import.family_shard_source_checkpoint import _directory

    root, directory, limits, signature, inputs, selected, occurrences = descriptor
    root = Path(root)
    if type(work) is not PlanWork or type(replay_work) is not PlanWork:
        raise ValueError("prepared SOURCE requires original and replay PlanWork")
    if root != Path(work.root) or astuple(work.limits) != limits or _directory(root) != directory:
        raise ValueError("prepared SOURCE reopening scope differs")
    if any(actual > ceiling for actual, ceiling in zip(astuple(replay_work.limits), limits, strict=True)):
        raise ValueError("prepared SOURCE replay exceeds its original resource bounds")

    if selected[0] != "selected" or occurrences[0] != "occurrences" or Path(replay_work.root) == root:
        raise ValueError("prepared SOURCE original file roles or replay work differ")
    prepared = object.__new__(PreparedSourceFamilies)
    prepared.context, prepared.admission, prepared.final_plan, prepared.policy = context, admission, final_plan, work
    prepared.signature = _input_signature(context, admission, final_plan, work)
    prepared.inputs = _input_stamps(context, admission, final_plan)
    if (
        _pack_row(prepared.signature) != signature
        or tuple((str(path), stamp) for path, stamp in prepared.inputs) != inputs
    ):
        raise ValueError("prepared SOURCE original inputs changed")
    prepared._replay_check = prepared._observation_token = prepared._active = None
    prepared.is_closed = False
    deadline = _read_deadline(context.request)

    def current():
        """Keep replay within the original deadline and both owning stop checks."""
        _require_deadline(deadline)
        if work.check is not None:
            work.check()
        if replay_work.check is not None:
            replay_work.check()
        if prepared._replay_check is not None:
            prepared._replay_check()

    current()
    prepared.work = _Scratch(replace(replay_work, check=current))
    prepared.staged_by_path = {}
    try:
        _restore_prepared_files(prepared, root, selected, occurrences, descriptor)
    except BaseException:
        prepared.work.close()
        raise
    return prepared


@contextmanager
def prepared_source_family_inputs(context, admission, final_plan, *, work):
    """Prepare bounded original SOURCE ordering once for sequential full replays."""
    prepared = PreparedSourceFamilies(context, admission, final_plan, work=work)
    try:
        yield prepared
    finally:
        prepared.close()


@contextmanager
def source_family_inputs(context, admission, final_plan, *, work):
    """Consume closed input EOF before yielding SOURCE values; no proof is minted.

    Run this CPU/file adapter off the coordinator event loop. One family's
    child factories remain valid until advancing or closing this context.
    Original source, admission and plan artifacts remain caller-owned.
    """
    with prepared_source_family_inputs(context, admission, final_plan, work=work) as prepared:
        with closing(prepared.families()) as families:
            yield families
