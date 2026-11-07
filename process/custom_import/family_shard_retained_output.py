# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure closed retained destination assignments and provisional owner COPY replay."""

import hashlib
import os
import shutil
import stat
import struct
from contextlib import closing, contextmanager
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path

import msgpack

from process.custom_import import runner_codec as codec
from process.custom_import.continuation_runtime import producer_token_sha256
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_records import FamilyRecord, _integer, record_from_wire, record_parts
from process.custom_import.family_shard_facts import _sorted
from process.custom_import.family_shard_output import AllocatedFamily, _pack_row, _unpack_row
from process.custom_import.family_shard_plan import PlanWork, _plan, _Scratch

_CONTRACT = "custom-import/retained-destination/v1"
_FILE = "retained-destination.mpk"
_LENGTH = struct.Struct("!Q")
_ROLES = ("root_record", "entity_binding", "pack")


def _wire(definition, collection, record, retained):
    return msgpack.packb(record_parts(definition, collection, record, retained=retained)[0], use_bin_type=True)


def _record(definition, collection, wire, *, retained):
    return record_from_wire(definition, collection, msgpack.unpackb(wire, raw=False), retained=retained)[0]


def _dictionary_columns(context, role):
    return tuple(context.common.columns_by_role[role])


@dataclass(frozen=True)
class RetainedDestinationDescriptor:
    """Closed owner assignments, not verification, publication or COPY authority."""

    root: str
    device: int
    inode: int
    file_device: int
    file_inode: int
    file_bytes: int
    sha256: bytes
    scope: tuple
    source_pack_prefix: tuple
    destination_pack_tail: tuple
    counts: tuple
    columns_by_role: tuple
    definition_sha256: str
    collection_slots: tuple
    max_frame_bytes: int

    @property
    def role_counts(self):
        """Return common-role COPY census, without granting candidate completeness."""
        return {role: dict(self.counts)[role] for role in _ROLES}

    def batches(self, *, maximum_rows, maximum_bytes):
        """Replay common COPY rows; completed EOF is mandatory for acceptance."""
        if any(type(bound) is not int or bound <= 0 for bound in (maximum_rows, maximum_bytes)):
            raise ValueError("retained COPY replay budgets differ")
        batch, count, size = {}, 0, 0
        with closing(read_retained_destination(self)) as entries:
            for entry in entries:
                if entry[0] != "copy":
                    continue
                role, packed = entry[1:]
                row = _unpack_row(packed)
                charge = 128 + len(_pack_row(row))
                if charge > maximum_bytes:
                    raise FamilyBlockResourceLimit("retained COPY row exceeds the caller buffer")
                if count and (count == maximum_rows or size + charge > maximum_bytes):
                    yield {name: tuple(rows) for name, rows in batch.items()}
                    batch, count, size = {}, 0, 0
                batch.setdefault(role, []).append(row)
                count, size = count + 1, size + charge
        if count:
            yield {name: tuple(rows) for name, rows in batch.items()}


def _original_root(context, view):
    cells = view.original_cells
    return _record(
        context.common.request.definition, None, cells[1][1], retained=view.source.plan.selection_kind == "retained"
    )


def _family_order(view):
    return bytes(view.source.record.logical_key_sha256), view.source.record.canonical_logical_key


def _producer(context):
    request = context.common.request
    return dict(
        execution_id=request.execution_id,
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        producing_fence=request.fence,
        producing_token_sha256=producer_token_sha256(request),
    )


class _Output:
    def __init__(self, context, work, prefix):
        self.context, self.work = context, work
        self.prefix, self.tail = prefix, dict(prefix)
        self.paths = {name: work.path() for name in ("family", "record", "copy", "allocation")}
        self.files = {}
        self.counts = dict(
            family=0,
            root=0,
            child=0,
            scalar=0,
            root_record=0,
            entity_binding=0,
            pack=0,
            allocation=0,
            original_pack=0,
            original_origin=0,
        )
        self.stamp = datetime.fromisoformat(context.common_output.proof_inputs["owner_created_at"])
        if self.stamp.tzinfo is None or self.stamp.utcoffset() is None:
            raise FamilyBlockUnavailable("retained allocation owner timestamp differs")
        self.attempt = 0

    def __enter__(self):
        try:
            for name, path in self.paths.items():
                self.files[name] = path.open("xb")
        except BaseException:
            self.__exit__()
            raise
        return self

    def __exit__(self, *_error):
        for target in self.files.values():
            target.close()

    def emit(self, kind, row):
        """Append one bounded provisional frame to the accounted owned spool."""
        self.work.frame(self.files[kind], row, self.paths[kind])

    def observed(self, kinds, identities, accepted):
        """Preserve each observed native ID, including rejected-attempt sequence gaps."""
        for ordinal, (kind, identity) in enumerate(zip(kinds, identities, strict=True)):
            self.emit("allocation", (self.attempt, ordinal, kind, identity, accepted))
            self.counts["allocation"] += 1
        self.attempt += 1

    def copy(self, role, values):
        """Append exactly the registered common-role physical column tuple."""
        columns = _dictionary_columns(self.context, role)
        if set(values) != set(columns):
            raise FamilyBlockUnavailable("retained COPY native columns differ")
        self.emit("copy", (role, _pack_row(tuple(values[name] for name in columns))))
        self.counts[role] += 1

    def pack(self, pack_id, stream_slot, label, hashes):
        """Preserve the original pack hash and per-stream ordinal after SOURCE."""
        ordinal = self.tail[stream_slot]
        if not 0 <= ordinal < 2**31:
            raise FamilyBlockUnavailable("retained pack ordinal exceeds the native boundary")
        self.copy(
            "pack",
            dict(
                _producer(self.context),
                pack_id=pack_id,
                stream_slot=stream_slot,
                pack_ordinal=ordinal,
                capture_bundle_id=dict(self.context.common.source.source.scope[0])["capture_bundle_id"],
                record_count=len(hashes),
                pack_sha256=codec.pack_hash(label, hashes),
                created_at=self.stamp,
            ),
        )
        self.tail[stream_slot] = ordinal + 1

    def root(self, view, assignment, registry):
        """Map one original retained root to its observed destination and logical pack."""
        original = assignment["original_root"]
        destination = replace(
            original, revision_id=assignment["root_revision_id"], pack_id=assignment["pack_id"], source_ordinal=0
        )
        order, plan = _family_order(view), view.original_cells[0][2]
        self.emit(
            "family",
            (
                order,
                view.root_id,
                plan,
                view.source.plan.base_family_revision_id,
                assignment["family_id"],
                view.source.entity_binding_id,
                view.source.family_sha256,
                view.source.child_count,
                view.original_cells[2],
            ),
        )
        lineage = view.original_cells[1][3]
        self.record(view, 0, original, destination, _unpack_row(lineage))
        self.pack(assignment["pack_id"], registry.root_stream_slot, "root", (bytes(view.source.root.payload_sha256),))
        self.counts["family"] += 1

    def record(self, view, slot, original, destination, lineage):
        """Preserve original lexical/scalar evidence alongside the destination bijection."""
        collection = (
            None
            if slot == 0
            else next(name for name, number in self.context.common.layout.collection_slots.items() if number == slot)
        )
        definition = self.context.common.request.definition
        wire, parts = record_parts(definition, collection, original, retained=True)
        order = () if slot == 0 else parts
        self.emit(
            "record",
            (
                _family_order(view),
                slot,
                order,
                msgpack.packb(wire, use_bin_type=True),
                _destination_ids(original, destination),
                None if lineage is None else _pack_row(lineage),
            ),
        )
        self.counts["root" if slot == 0 else "child"] += 1
        self.counts["scalar"] += len(original.scalars)


def _destination_ids(original, destination):
    if type(destination) is not FamilyRecord or (
        destination.values_by_field is not original.values_by_field
        or destination.scalars is not original.scalars
        or destination.sparse is not original.sparse
    ):
        raise FamilyBlockUnavailable("retained assignment may replace only native identity cells")
    cells = destination.revision_id, destination.pack_id, destination.source_ordinal
    for ordinal, identity in enumerate(cells):
        _integer(identity, "retained destination identity", zero=ordinal == 2)
    if cells[0] == original.revision_id:
        raise FamilyBlockUnavailable("retained destination reuses its original revision")
    return cells


def _assigned_wire(original_wire, identities):
    if type(identities) is not tuple or len(identities) != 3:
        raise ValueError("retained destination identity tuple differs")
    for ordinal, identity in enumerate(identities):
        _integer(identity, "retained destination identity", zero=ordinal == 2)
    original = msgpack.unpackb(original_wire, raw=False)
    if type(original) is not list or len(original) != 6 or original[0] == identities[0]:
        raise ValueError("retained original wire/identity differs")
    return msgpack.packb([*identities, *original[3:]], use_bin_type=True)


def _unique_observed(work, output):
    previous = None
    accepted_by_kind = dict(family_revision=0, root_revision=0, child_revision=0, pack=0)
    ordered = _sorted(work, work.rows(output.paths["allocation"]), lambda row: (row[2], row[3]))
    with closing(ordered):
        for row in ordered:
            key = row[2], row[3]
            if key == previous or type(row[3]) is not int or not 0 < row[3] < 2**63 or type(row[4]) is not bool:
                raise FamilyBlockUnavailable("retained observed native identity repeats or differs")
            if row[4]:
                accepted_by_kind[row[2]] += 1
            previous = key
    if tuple(accepted_by_kind.values()) != tuple(output.counts[name] for name in ("family", "root", "child", "pack")):
        raise FamilyBlockUnavailable("retained accepted native ID census differs from assignments")


def _check_record_bijections(work, output):
    originals, destinations = work.path(), work.path()
    with (
        originals.open("xb") as old,
        destinations.open("xb") as new,
        closing(work.rows(output.paths["record"])) as rows,
    ):
        for row in rows:
            kind = "root" if row[1] == 0 else "child"
            before, after = msgpack.unpackb(row[3], raw=False), row[4]
            if before[0] == after[0]:
                raise FamilyBlockUnavailable("retained original/destination content or identity differs")
            work.frame(old, (kind, before[0]), originals)
            work.frame(new, (kind, after[0]), destinations)
    for path in (originals, destinations):
        previous = None
        with closing(_sorted(work, work.rows(path), lambda row: row)) as rows:
            for row in rows:
                if row == previous:
                    raise FamilyBlockUnavailable("retained original/destination revision bijection repeats")
                previous = row
        work.remove(path)


def _copy_family_sections(work, output, target, path):
    ordered = _sorted(work, work.rows(output.paths["family"]), lambda row: row[0])
    records = _sorted(work, work.rows(output.paths["record"]), lambda row: row[:3])
    previous = None
    with closing(ordered), closing(records):
        current = next(records, None)
        for family in ordered:
            if previous is not None and family[0] <= previous:
                raise FamilyBlockUnavailable("retained output family order repeats")
            work.frame(target, ("family", *family), path)
            count = roots = 0
            while current is not None and current[0] == family[0]:
                work.frame(target, ("record", *current), path)
                roots += current[1] == 0
                count += current[1] != 0
                current = next(records, None)
            if roots != 1 or count != family[7]:
                raise FamilyBlockUnavailable("retained destination family record EOF differs")
            previous = family[0]
        if current is not None:
            raise FamilyBlockUnavailable("retained destination record lacks its original family")


def _write_complete(context, scope, work, paths, output):
    _unique_observed(work, output)
    _check_record_bijections(work, output)
    path = work.path()
    with closing(work.rows(paths["original"])) as originals:
        for original in originals:
            output.counts["original_" + original[0]] += 1
    for role in ("pack", "origin"):
        if output.counts["original_" + role] != context.retained_input.counts_by_role[role]:
            raise FamilyBlockUnavailable("retained original provenance census differs")
    counts = tuple(output.counts.items())
    pack_tail_rows = tuple(sorted(output.tail.items()))
    with path.open("xb") as spool_output:
        work.frame(
            spool_output,
            (
                "header",
                _CONTRACT,
                scope,
                output.prefix,
                context.common.request.definition.digest,
                tuple(sorted(context.common.layout.collection_slots.items())),
            ),
            path,
        )
        _copy_family_sections(work, output, spool_output, path)
        for kind, staged in (
            ("copy", output.paths["copy"]),
            ("original", paths["original"]),
            ("allocation", output.paths["allocation"]),
        ):
            with closing(work.rows(staged)) as staged_rows:
                for staged_row in staged_rows:
                    work.frame(spool_output, (kind, *staged_row), path)
        work.frame(spool_output, ("complete", counts, pack_tail_rows), path)
        spool_output.flush()
        os.fsync(spool_output.fileno())
    return _freeze_file(context, scope, work, output, path, counts, pack_tail_rows)


def _freeze_file(context, scope, work, output, path, counts, pack_tail_rows):
    for obsolete in tuple(work.paths):
        if obsolete != path and obsolete.exists():
            work.remove(obsolete)
    ready = work.root / _FILE
    path.rename(ready)
    ready.chmod(0o400)
    with ready.open("rb") as spool_input:
        checksum = hashlib.sha256()
        while block := spool_input.read(min(work.limits.max_record_bytes, 1024**2)):
            work.current()
            checksum.update(block)
        digest = checksum.digest()
    directory = os.open(work.root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    observed = ready.stat()
    return RetainedDestinationDescriptor(
        str(work.root),
        work.identity.st_dev,
        work.identity.st_ino,
        observed.st_dev,
        observed.st_ino,
        observed.st_size,
        digest,
        scope,
        output.prefix,
        pack_tail_rows,
        counts,
        tuple((role, _dictionary_columns(context, role)) for role in _ROLES),
        context.common.request.definition.digest,
        tuple(sorted(context.common.layout.collection_slots.items())),
        work.limits.max_record_bytes,
    )


def _owned_destination(descriptor):
    if type(descriptor) is not RetainedDestinationDescriptor:
        raise ValueError("retained destination descriptor differs")
    root = Path(descriptor.root)
    directory, path = root.lstat(), root / _FILE
    observed = path.lstat()
    if not stat.S_ISDIR(directory.st_mode) or (directory.st_dev, directory.st_ino) != (
        descriptor.device,
        descriptor.inode,
    ):
        raise ValueError("retained destination root custody differs")
    if not stat.S_ISREG(observed.st_mode) or (
        observed.st_dev,
        observed.st_ino,
        observed.st_size,
        stat.S_IMODE(observed.st_mode),
    ) != (descriptor.file_device, descriptor.file_inode, descriptor.file_bytes, 0o400):
        raise ValueError("retained destination file custody differs")
    if {entry.name for entry in root.iterdir()} != {_FILE}:
        raise ValueError("retained destination file census differs")
    return path


def _destination_entries(descriptor, check):
    path = _owned_destination(descriptor)
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as source:
        observed = os.fstat(source.fileno())
        if (observed.st_dev, observed.st_ino, observed.st_size) != (
            descriptor.file_device,
            descriptor.file_inode,
            descriptor.file_bytes,
        ):
            raise ValueError("retained destination open custody differs")
        digest = hashlib.sha256()
        while prefix := source.read(_LENGTH.size):
            if check is not None:
                check()
            if len(prefix) != _LENGTH.size:
                raise ValueError("retained destination frame prefix is truncated")
            size = _LENGTH.unpack(prefix)[0]
            if not 0 < size <= descriptor.max_frame_bytes:
                raise FamilyBlockResourceLimit("retained destination frame exceeds its resource budget")
            payload = source.read(size)
            if len(payload) != size:
                raise ValueError("retained destination frame is truncated")
            digest.update(prefix)
            digest.update(payload)
            yield msgpack.unpackb(payload, raw=False, use_list=False, strict_map_key=True)
        if digest.digest() != descriptor.sha256:
            raise ValueError("retained destination complete EOF digest differs")


def _destination_census(entry, counts):
    kind = entry[0]
    if kind in ("family", "allocation"):
        counts[kind] += 1
        return
    if kind == "record":
        counts["root" if entry[2] == 0 else "child"] += 1
        counts["scalar"] += len(msgpack.unpackb(entry[4], raw=False)[4])
        return
    if kind == "copy" and entry[1] in _ROLES:
        counts[entry[1]] += 1
        return
    if kind == "original" and entry[1] in ("pack", "origin"):
        counts["original_" + entry[1]] += 1
        return
    raise ValueError("retained destination record kind differs")


def read_retained_destination(descriptor, *, check=None):
    """Owner-only streaming replay; a prefix cannot establish completed receipt."""
    counts = dict.fromkeys(dict(descriptor.counts), 0)
    is_complete = False
    with closing(_destination_entries(descriptor, check)) as entries:
        if next(entries, None) != (
            "header",
            _CONTRACT,
            descriptor.scope,
            descriptor.source_pack_prefix,
            descriptor.definition_sha256,
            descriptor.collection_slots,
        ):
            raise ValueError("retained destination scope header differs")
        for entry in entries:
            if is_complete:
                raise ValueError("retained destination has trailing records")
            if entry[0] == "complete":
                if (
                    entry != ("complete", descriptor.counts, descriptor.destination_pack_tail)
                    or tuple(counts.items()) != descriptor.counts
                ):
                    raise ValueError("retained destination complete census differs")
                is_complete = True
            else:
                _destination_census(entry, counts)
                if entry[0] == "record":
                    entry = (*entry[:5], _assigned_wire(entry[4], entry[5]), entry[6])
                yield entry
    if not is_complete:
        raise ValueError("retained destination complete footer is missing")


def cleanup_retained_destination(descriptor):
    """Remove only the exact task-owned closed assignment directory."""
    _owned_destination(descriptor)
    shutil.rmtree(descriptor.root)


class _RetainedChildren:
    def __init__(self, owner, collection, destination):
        self.owner, self.collection, self.destination = owner, collection, destination

    def __call__(self):
        return self.owner.children(self.collection, self.destination)


class _FamilyReplay:
    def __init__(self, definition, work, header, collection_slots):
        self.definition, self.work, self.header = definition, work, header
        self.paths_by_slot, self.readers = {}, set()
        self.roots = None
        self.is_active = True
        self.names_by_slot = {slot: name for name, slot in collection_slots}
        self.child_count = 0

    def append(self, entry):
        """Spool only this current family with original and destination cells intact."""
        _kind, order, slot, _sort, original, destination, lineage = entry
        if order != self.header[1]:
            raise ValueError("retained family replay contains another key")
        if slot == 0:
            if self.roots is not None:
                raise ValueError("retained family replay repeats its root")
            self.roots = tuple(_record(self.definition, None, wire, retained=True) for wire in (original, destination))
            return
        if slot not in self.names_by_slot:
            raise ValueError("retained family replay collection differs")
        if slot not in self.paths_by_slot:
            self.paths_by_slot[slot] = self.work.path()
            self.paths_by_slot[slot].touch(mode=0o600, exist_ok=False)
        path = self.paths_by_slot[slot]
        self.child_count += 1
        with path.open("ab") as target:
            self.work.frame(target, (original, destination, lineage), path)

    def children(self, collection, destination):
        """Borrow fresh records only while their current accounted family remains live."""
        if not self.is_active:
            raise ValueError("retained child factory outlived its current family")
        slot = next(slot for slot, name in self.names_by_slot.items() if name == collection)
        path = self.paths_by_slot.get(slot)
        if path is None:
            return
        reader = self.work.rows(path)
        self.readers.add(reader)
        try:
            for row in reader:
                if not self.is_active:
                    raise ValueError("retained child factory outlived its current family")
                yield _record(self.definition, collection, row[1 if destination else 0], retained=True)
        finally:
            reader.close()
            self.readers.discard(reader)

    def allocated(self, destination):
        """Return original or destination inputs without granting base or COPY authority."""
        if self.roots is None:
            raise ValueError("retained family replay lacks its original/destination roots")
        _kind, _order, root_id, plan, old_family, new_family, entity, digest, count, _original = self.header
        if self.child_count != count:
            raise ValueError("retained family replay child census differs")
        factories_by_collection = {
            self.names_by_slot[slot]: _RetainedChildren(self, self.names_by_slot[slot], destination)
            for slot in self.names_by_slot
        }
        return _plan(plan), AllocatedFamily(
            new_family if destination else old_family,
            root_id,
            entity,
            self.roots[int(destination)],
            factories_by_collection,
            digest,
            True,
        )

    def close(self):
        """Close active readers and remove only this current family owned spool."""
        self.is_active = False
        for reader in tuple(self.readers):
            reader.close()
        for path in self.paths_by_slot.values():
            if path.exists():
                self.work.remove(path)


def _replay_families(descriptor, definition, scratch):
    with closing(read_retained_destination(descriptor, check=scratch.current)) as entries:
        current = next(entries, None)
        while current is not None:
            if current[0] != "family":
                for _unused in entries:
                    scratch.current()
                break
            family = _FamilyReplay(definition, scratch, current, descriptor.collection_slots)
            try:
                current = next(entries, None)
                while current is not None and current[0] == "record":
                    family.append(current)
                    current = next(entries, None)
                plan, original = family.allocated(False)
                destination_plan, destination = family.allocated(True)
                if destination_plan != plan:
                    raise ValueError("retained destination original/plan differs")
                yield plan, original, destination
            finally:
                family.close()


@contextmanager
def retained_destination_families(descriptor, definition, *, work):
    """Borrow independent original/destination families, only until next/close.

    This is an owner-only provisional COPY input. The caller must consume the
    entire iterator to EOF; accepted COPY receipts remain transaction-bound.
    The original live pinned base context remains a separate caller obligation.
    """
    if type(work) is not PlanWork or definition.digest != descriptor.definition_sha256:
        raise ValueError("retained family replay work/definition differs")
    scratch = _Scratch(work)
    try:
        with closing(_replay_families(descriptor, definition, scratch)) as families:
            yield families
    finally:
        scratch.close()
