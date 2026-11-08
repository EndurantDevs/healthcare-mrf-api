# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Owner-allocated family identities and globally ordered query-key metadata."""

import hashlib
import itertools
import json
import os
import stat
from contextlib import asynccontextmanager, closing, contextmanager
from dataclasses import dataclass, field
from pathlib import Path

from process.custom_import.build_source import _drain_cleanup
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_keys import iter_key_dictionary
from process.custom_import.family_block_records import _integer
from process.custom_import.family_block_validate import check_key_dictionary
from process.custom_import.family_shard_allocate import (
    _checked_source,
    _checked_source_provenance,
    read_source_allocations,
)
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded, _sorted
from process.custom_import.family_shard_identity import GlobalIdentityKey, intern_global_identities
from process.custom_import.family_shard_ids import _allocate_record_ids
from process.custom_import.family_shard_output import _input_iterator
from process.custom_import.family_shard_plan import PlanWork, _cpu, _key_order, _Scratch
from process.custom_import.family_shard_tasks import _BorrowedWork

_METADATA_CONTRACT = "custom-import/owner-source-metadata/v1"
_DESCRIPTOR = "owner-metadata.json"
_FILES = {"family": "families.mpk", "query": "query-keys.mpk"}


@dataclass(frozen=True)
class _WrittenMetadata:
    path: object
    count: int
    sha256: bytes
    stamp: tuple


class _MetadataOutput:
    def __init__(self, output):
        self.output, self.digest = output, hashlib.sha256()

    def write(self, chunk):
        """Hash exactly the framed bytes accepted by the existing writer."""
        written = self.output.write(chunk)
        if written != len(chunk):
            raise OSError("owner metadata write is incomplete")
        self.digest.update(chunk)
        return written


def _source_family_keys(source, definition, check):
    with closing(read_source_allocations(source, check=check)) as packs:
        for pack, _pack_id, root_ids, revision_ids, _rejection_ids in packs:
            check()
            if pack.collection_slot:
                continue
            for index, record in enumerate(pack.records):
                check()
                if revision_ids[index] is None:
                    continue
                values = record.normalized_values(definition, definition.source_streams[record.stream_slot - 1])
                yield (
                    record.typed_parent_key,
                    (record.stream_slot, record.source_ordinal),
                    root_ids[index],
                    values[definition.entity_field],
                )


def _source_query_keys(source, definition, check):
    with closing(read_source_allocations(source, check=check)) as packs:
        for pack, _pack_id, _root_ids, revision_ids, _rejection_ids in packs:
            check()
            stream = definition.source_streams[pack.stream_slot - 1]
            if not pack.collection_slot or stream.child_collection != definition.query.child_collection:
                continue
            for index, landing in enumerate(pack.landing.records):
                check()
                if revision_ids[index] is not None:
                    yield landing.child_key_sha256, landing.canonical_child_key


def _unique_families(work, source, definition):
    ordered = _sorted(
        work, _source_family_keys(source, definition, work.current), lambda row: (row[0][1], row[0][0], row[1])
    )
    with closing(ordered):
        for _key, entries in itertools.groupby(ordered, lambda row: row[0]):
            # Duplicate roots are rejected by the complete facts reducer. One
            # provisional identity here grants neither admission nor membership.
            yield next(entries)


def _family_batches(entries, limits):
    batch, size = [], 0
    with closing(entries):
        for entry in entries:
            charge = len(entry[0][0].encode("utf-8")) + len(entry[3].encode("utf-8")) + 128
            if charge > limits.maximum_batch_bytes:
                raise FamilyBlockResourceLimit("family identity exceeds the owner batch budget")
            if batch and (len(batch) == limits.maximum_batch_records or size + charge > limits.maximum_batch_bytes):
                yield tuple(batch)
                batch, size = [], 0
            batch.append(entry)
            size += charge
        if batch:
            yield tuple(batch)
        else:
            yield ()


async def _allocate_family_batch(session, request, batch, limits):
    """Observe native IDs inside the caller's already authorized transaction."""
    if not batch:
        return ()
    identities = await intern_global_identities(
        session,
        request.definition,
        request.dataset_id,
        tuple(GlobalIdentityKey(key[0], key[1], entity) for key, _position, _root, entity in batch),
        maximum_rows=limits.maximum_batch_records,
        maximum_bytes=limits.maximum_batch_bytes,
    )
    if any(identity.root_record_id != entry[2] for identity, entry in zip(identities, batch, strict=True)):
        raise FamilyBlockUnavailable("family root dictionary differs from SOURCE allocations")
    family_ids = await _allocate_record_ids(session, ("family_revision",) * len(batch))
    return tuple(
        (entry[0], (family_id, identity.entity_binding_id))
        for entry, family_id, identity in zip(batch, family_ids, identities, strict=True)
    )


@dataclass(frozen=True)
class OwnerSourceMetadata:
    work: object
    family_path: object
    query_path: object
    _scope: tuple = field(default=(), init=False, repr=False)
    _written: tuple = field(default=(), init=False, repr=False)
    _reader: object = field(default=None, init=False, repr=False)

    @property
    def scope(self):
        """Expose immutable provenance, never native authority or completion proof."""
        return self._scope

    def family_ids(self):
        """Replay actual native family/entity identities for task preparation."""
        return self.work.rows(self.family_path) if self._reader is None else self._reader.rows("family")

    def query_keys(self):
        """Replay (hash,text,rank,variant) using one completed global order."""
        rows = self.work.rows(self.query_path) if self._reader is None else self._reader.rows("query")
        with closing(rows):
            yield from ((digest, canonical, rank, variant) for rank, variant, digest, canonical in rows)


def _query_keys(work, source, definition, retained):
    with (
        closing(_source_query_keys(source, definition, work.current)) as fresh,
        closing(_input_iterator(retained)) as previous,
    ):
        yield from fresh
        for entry in previous:
            work.current()
            yield entry


def _write_assignments(work, output, path, assignments):
    count = 0
    for assignment in assignments:
        work.frame(output, assignment, path)
        count += 1
    return count


async def _write_families(request, source, work, limits, allocator):
    path = work.path()
    batches = _family_batches(_unique_families(work, source, request.definition), limits)
    primary, count = None, 0
    try:
        with path.open("xb") as target:
            output = _MetadataOutput(target)
            while (batch := await _cpu(work, next, batches, None)) is not None:
                work.current()
                assignments = await allocator(batch)
                count += await _cpu(work, _write_assignments, work, output, path, assignments)
    except BaseException as error:
        primary = error
    primary = await _drain_cleanup(_cpu(work, batches.close), primary)
    if primary is not None:
        raise primary
    return _WrittenMetadata(path, count, output.digest.digest(), _metadata_stamp(path))


def _write_query_keys(work, source, definition, retained):
    work.current()
    keys = _query_keys(work, source, definition, retained)
    with closing(_sorted(work, keys, lambda key: key)) as ordered:
        path = work.path()
        with path.open("xb") as target, closing(iter_key_dictionary(ordered)) as assignments:
            output = _MetadataOutput(target)
            count = _write_assignments(work, output, path, assignments)
    work.current()
    return _WrittenMetadata(path, count, output.digest.digest(), _metadata_stamp(path))


@asynccontextmanager
async def owner_source_metadata(session_factory, request, layout, source, *, journal, limits, retained_query_keys=()):
    """Keep bounded metadata until task, native COPY and owner validation finish.

    Family reservations include initially valid duplicate/rejected SOURCE roots;
    only the complete worker facts can select them. Retained query keys require
    independently pinned original provenance; they never grant a retained copy.
    The journal owns its fresh retained attempt and commit reconciliation.
    """
    from process.custom_import.family_shard_metadata_journal import journalled_owner_metadata

    async with journalled_owner_metadata(
        session_factory,
        request,
        layout,
        source,
        journal=journal,
        limits=limits,
        retained_query_keys=retained_query_keys,
    ) as metadata:
        yield metadata


def owner_metadata_scope(request, source):
    """Derive immutable original scope; this does not authorize a resumed owner."""
    capture = _checked_source_provenance(request, source.source)
    return (
        ("capture", tuple(sorted(capture.items()))),
        ("execution_id", request.execution_id),
        ("producing_fence", request.fence),
        ("snapshot_family_id", source.snapshot_family_id),
        ("source_plan_sha256", source.source.sha256.hex()),
        ("allocation_sha256", source.sha256.hex()),
        ("collection_slots", source.source.collection_slots),
        ("base_generation_id", request.expected_base_generation_id),
        ("base_pointer_version", request.expected_pointer_version),
        ("complete_scope", request.complete_scope),
    )


@dataclass(frozen=True)
class OwnerMetadataSeal:
    """File commitment for a protected checkpoint, never native resume authority."""

    root: str
    descriptor_sha256: bytes
    scope: tuple
    counts: tuple


def _metadata_stamp(path):
    observed = path.lstat()
    if not stat.S_ISREG(observed.st_mode):
        raise ValueError("owner metadata file type differs")
    return _file_stamp(observed)


def _file_stamp(observed):
    return tuple(
        getattr(observed, "st_" + name) for name in ("dev", "ino", "mode", "uid", "size", "mtime_ns", "ctime_ns")
    )


def _directory_stamp(observed):
    if not stat.S_ISDIR(observed.st_mode) or stat.S_IMODE(observed.st_mode) != 0o700:
        raise ValueError("owner metadata directory type or mode differs")
    return observed.st_dev, observed.st_ino, observed.st_uid, observed.st_mode


def _metadata_work(root, work):
    if type(work) is not PlanWork:
        raise ValueError("owner metadata needs explicit accounted work")
    retained, temporary = Path(root).absolute(), Path(work.root).absolute()
    if (
        retained.resolve() == temporary.resolve()
        or retained.resolve() in temporary.resolve().parents
        or temporary.resolve() in retained.resolve().parents
    ):
        raise ValueError("owner metadata retained and temporary roots overlap")
    return retained


def _check_original(metadata, request, source):
    from process.custom_import.family_shard_metadata_journal import MetadataJournalScratch

    if (
        type(metadata) is not OwnerSourceMetadata
        or type(metadata.work) is not MetadataJournalScratch
        or metadata._reader is not None
    ):
        raise ValueError("owner metadata sealing requires its original producer")
    metadata.work.current()
    if metadata.scope != owner_metadata_scope(request, source) or len(metadata._written) != 2:
        raise ValueError("owner metadata original producer scope differs")
    if tuple(entry.path for entry in metadata._written) != (metadata.family_path, metadata.query_path):
        raise ValueError("owner metadata original producer files differ")
    observed = metadata.work.root.lstat()
    if _directory_stamp(observed)[:2] != (metadata.work.identity.st_dev, metadata.work.identity.st_ino):
        raise ValueError("owner metadata original directory custody differs")
    if {path.name for path in metadata.work.root.iterdir()} != {metadata.family_path.name, metadata.query_path.name}:
        raise ValueError("owner metadata original file census differs")
    for entry in metadata._written:
        if type(entry) is not _WrittenMetadata or _metadata_stamp(entry.path) != entry.stamp:
            raise ValueError("owner metadata original file custody differs")


def _copy_metadata(directory, name, written, work):
    work.current()
    with os.fdopen(os.open(written.path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as source:
        if _file_stamp(os.fstat(source.fileno())) != written.stamp:
            raise ValueError("owner metadata original open custody differs")
        digest = hashlib.sha256()
        descriptor = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory)
        with os.fdopen(descriptor, "wb") as target:
            while chunk := source.read(min(65536, work.limits.memory_bytes)):
                work.current()
                work.charge(len(chunk))
                target.write(chunk)
                digest.update(chunk)
            target.flush()
            os.fsync(target.fileno())
            os.fchmod(target.fileno(), 0o400)
            os.fsync(target.fileno())
            stamp = _file_stamp(os.fstat(target.fileno()))
        if (
            _file_stamp(os.fstat(source.fileno())) != written.stamp
            or _metadata_stamp(written.path) != written.stamp
            or digest.digest() != written.sha256
        ):
            raise ValueError("owner metadata original bytes changed during sealing")
    return {"name": name, "stamp": stamp, "count": written.count, "sha256": digest.hexdigest()}


def _metadata_document(directory, scope, files):
    return {
        "contract": _METADATA_CONTRACT,
        "scope": scope,
        "directory": _directory_stamp(os.fstat(directory)),
        "files": files,
    }


def _publish_metadata(directory, document, work):
    contents = json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    if len(contents) > work.limits.max_record_bytes:
        raise FamilyBlockResourceLimit("owner metadata descriptor exceeds the resource budget")
    work.current()
    work.charge(len(contents))
    descriptor = os.open(_DESCRIPTOR, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory)
    with os.fdopen(descriptor, "wb") as target:
        target.write(contents)
        target.flush()
        os.fsync(target.fileno())
        os.fchmod(target.fileno(), 0o400)
        os.fsync(target.fileno())
    os.fsync(directory)
    work.current()
    return hashlib.sha256(contents).digest()


def _sync_metadata_parent(root, document, work):
    work.current()
    parent = os.open(root.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        opened = os.fstat(parent)
        if _directory_stamp(os.stat(root.name, dir_fd=parent, follow_symlinks=False)) != tuple(document["directory"]):
            raise ValueError("owner metadata retained directory entry differs")
        os.fsync(parent)
        current = root.parent.lstat()
        if not stat.S_ISDIR(current.st_mode) or (current.st_dev, current.st_ino) != (opened.st_dev, opened.st_ino):
            raise ValueError("owner metadata parent changed during sealing")
        work.current()
    finally:
        os.close(parent)


def seal_owner_metadata(metadata, request, allocated_source, *, root, register_cleanup, work):
    """Copy original native IDs, then seal actual typed EOF under retained custody.

    Run off the owner event loop. The existing producer manager still cleans its
    own files. The registered retained root is caller-owned even on failure; an
    uncertain fsync is never a successful seal or permission to reallocate IDs.
    """
    root = _metadata_work(root, work)
    _check_original(metadata, request, allocated_source)
    if root == metadata.work.root.resolve() or metadata.work.root.resolve() in root.resolve().parents:
        raise ValueError("owner metadata retained root overlaps the original producer")
    if not callable(register_cleanup) or root.exists() or root.is_symlink():
        raise ValueError("owner metadata needs a fresh registered retained root")
    with closing(_Scratch(work)) as scratch:
        register_cleanup(root)
        root.mkdir(mode=0o700)
        directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            files_by_role = {
                role: _copy_metadata(directory, name, written, scratch)
                for (role, name), written in zip(_FILES.items(), metadata._written, strict=True)
            }
            document = _metadata_document(directory, metadata.scope, files_by_role)
            with closing(_MetadataReader(root, directory, document, scratch, descriptor_present=False)) as reader:
                directory = None
                _typed_metadata_eof(reader)
                _check_original(metadata, request, allocated_source)
                digest = _publish_metadata(reader.directory, document, scratch)
                _sync_metadata_parent(root, document, scratch)
                reader.check(descriptor_present=True)
        finally:
            if directory is not None:
                os.close(directory)
    return OwnerMetadataSeal(
        str(root), digest, metadata.scope, tuple((role, files_by_role[role]["count"]) for role in _FILES)
    )


class _MetadataInput:
    def __init__(self, source):
        self.source, self.digest = source, hashlib.sha256()

    def read(self, size):
        """Hash the exact complete framed bytes consumed by the bounded reader."""
        chunk = self.source.read(size)
        self.digest.update(chunk)
        return chunk


class _MetadataReader:
    def __init__(self, root, directory, document, work, *, descriptor_present=True):
        self.root, self.directory, self.document, self.work = root, directory, document, work
        self.descriptor_present, self.readers, self.is_closed = descriptor_present, set(), False

    def check(self, *, descriptor_present=None):
        """Check retained directory custody and the complete fixed file census."""
        if self.is_closed:
            raise ValueError("reopened owner metadata is closed")
        self.work.current()
        expected_ids = tuple(self.document["directory"])
        if (
            _directory_stamp(self.root.lstat()) != expected_ids
            or _directory_stamp(os.fstat(self.directory)) != expected_ids
        ):
            raise ValueError("owner metadata retained directory custody differs")
        names = set(_FILES.values())
        has_descriptor = self.descriptor_present if descriptor_present is None else descriptor_present
        if has_descriptor:
            names.add(_DESCRIPTOR)
        if set(os.listdir(self.directory)) != names:
            raise ValueError("owner metadata retained file census differs")

    def rows(self, role):
        """Borrow one digest-checked stream only while this reopened owner is live."""
        self.check()

        def borrowed():
            """Release this reader after complete EOF or early close."""
            try:
                yield from self._rows(role)
            finally:
                self.readers.discard(reader)

        reader = borrowed()
        self.readers.add(reader)
        return reader

    def _rows(self, role):
        self.check()
        entry = self.document["files"][role]
        opened = os.open(entry["name"], os.O_RDONLY | os.O_NOFOLLOW, dir_fd=self.directory)
        with os.fdopen(opened, "rb") as source:
            if _file_stamp(os.fstat(source.fileno())) != tuple(entry["stamp"]):
                raise ValueError("owner metadata retained file custody differs")
            checked, count = _MetadataInput(source), 0
            for cells in self.work.read(checked):
                self.work.current()
                count += 1
                yield cells
            if (
                count != entry["count"]
                or checked.digest.hexdigest() != entry["sha256"]
                or _file_stamp(os.fstat(source.fileno())) != tuple(entry["stamp"])
            ):
                raise ValueError("owner metadata requires exact complete file EOF")
        observed = os.stat(entry["name"], dir_fd=self.directory, follow_symlinks=False)
        if _file_stamp(observed) != tuple(entry["stamp"]):
            raise ValueError("owner metadata retained path custody differs")
        self.check()

    def close(self):
        """Drain borrowed file handles before releasing this directory handle."""
        if self.is_closed:
            return
        self.is_closed = True
        try:
            for reader in tuple(self.readers):
                reader.close()
        finally:
            self.readers.clear()
            os.close(self.directory)

    def _check_files(self):
        self.check()
        for entry in self.document["files"].values():
            if _metadata_stamp(self.root / entry["name"]) != tuple(entry["stamp"]):
                raise ValueError("owner metadata retained files changed during borrowing")


def _typed_metadata_eof(reader):
    previous = None
    with closing(reader.rows("family")) as families:
        for entry in families:
            if type(entry) is not tuple or len(entry) != 2 or type(entry[1]) is not tuple or len(entry[1]) != 2:
                raise ValueError("owner metadata family cells differ")
            order = _key_order(entry[0])
            if previous is not None and order <= previous:
                raise ValueError("owner metadata family keys repeat or are unordered")
            for identifier in entry[1]:
                _integer(identifier, "owner metadata native identity")
            previous = order
    with closing(reader.rows("query")) as keys:
        check_key_dictionary(keys)


def _read_metadata_document(root, directory, descriptor_sha256, expected_scope, work):
    if (
        type(descriptor_sha256) is not bytes
        or len(descriptor_sha256) != 32
        or type(expected_scope) is not tuple
        or not expected_scope
    ):
        raise ValueError("owner metadata requires its protected descriptor commitment and actual scope")
    descriptor = os.open(_DESCRIPTOR, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
    with os.fdopen(descriptor, "rb") as descriptor_stream:
        stamp = os.fstat(descriptor_stream.fileno())
        if not stat.S_ISREG(stamp.st_mode) or stat.S_IMODE(stamp.st_mode) != 0o400:
            raise ValueError("owner metadata descriptor type or mode differs")
        before = _file_stamp(stamp)
        contents = descriptor_stream.read(work.limits.max_record_bytes + 1)
        if len(contents) > work.limits.max_record_bytes:
            raise FamilyBlockResourceLimit("owner metadata descriptor exceeds its resource budget")
        if hashlib.sha256(contents).digest() != descriptor_sha256:
            raise ValueError("owner metadata protected descriptor commitment differs")
        if (
            _file_stamp(os.fstat(descriptor_stream.fileno())) != before
            or _file_stamp(os.stat(_DESCRIPTOR, dir_fd=directory, follow_symlinks=False)) != before
        ):
            raise ValueError("owner metadata descriptor changed during replay")
    work.charge(len(contents))
    document = json.loads(contents)
    if (
        set(document) != {"contract", "scope", "directory", "files"}
        or document["contract"] != _METADATA_CONTRACT
        or document["scope"] != json.loads(json.dumps(expected_scope))
    ):
        raise ValueError("owner metadata descriptor scope differs")
    directory_ids = document["directory"]
    if (
        type(directory_ids) is not list
        or len(directory_ids) != 4
        or any(type(identifier) is not int or identifier < 0 for identifier in directory_ids)
    ):
        raise ValueError("owner metadata descriptor directory identity differs")
    if tuple(directory_ids) != _directory_stamp(root.lstat()) or tuple(directory_ids) != _directory_stamp(
        os.fstat(directory)
    ):
        raise ValueError("owner metadata protected directory custody differs")
    _check_metadata_entries(document["files"], work)
    return document


def _check_metadata_entries(files, work):
    if type(files) is not dict or files.keys() != _FILES.keys():
        raise ValueError("owner metadata descriptor file roles differ")
    for role, name in _FILES.items():
        entry = files[role]
        if type(entry) is not dict or set(entry) != {"name", "stamp", "count", "sha256"} or entry["name"] != name:
            raise ValueError("owner metadata relative file binding differs")
        stamp = entry["stamp"]
        if (
            type(stamp) is not list
            or len(stamp) != 7
            or any(type(value) is not int or value < 0 for value in stamp)
            or not stat.S_ISREG(stamp[2])
            or stat.S_IMODE(stamp[2]) != 0o400
        ):
            raise ValueError("owner metadata descriptor file identity differs")
        _integer(entry["count"], "owner metadata EOF count", zero=True)
        digest = entry["sha256"]
        if type(digest) is not str or len(digest) != 64 or bytes.fromhex(digest).hex() != digest:
            raise ValueError("owner metadata descriptor file digest differs")
        work.charge(stamp[4])


class _MetadataReadWork(_BorrowedWork):
    """Account immutable borrowed bytes without owning a writable directory or cleanup."""

    def __init__(self, root, work):
        super().__init__(root, _directory_stamp(root.lstat())[:2], work.limits)
        self._io_check = work.check
        self.current()
        filesystem = os.statvfs(root)
        unit = max(512, filesystem.f_frsize or filesystem.f_bsize)
        self.remaining_bytes = work.limits.max_spool_bytes - 2 * unit
        self.charge(0)

    def current(self):
        """Keep the original caller's lock, cancellation and deadline checks live."""
        if self._io_check is not None:
            self._io_check()

    def charge(self, count):
        """Preserve the former scratch reservation and complete retained-byte ceiling."""
        self.current()
        if count > self.remaining_bytes:
            raise SourceFactBudgetExceeded("owner metadata borrowed bytes exceed the original resource budget")
        self.remaining_bytes -= count


@contextmanager
def reopen_owner_metadata(root, *, expected_scope, descriptor_sha256, work):
    """Borrow sealed files only after the caller verifies their native checkpoint.

    The SHA is the named metadata descriptor commitment from that protected
    checkpoint, not its journal head. This reader supplies no native authority.
    Use these same sealed paths before creating dependent task fingerprints.
    """
    root = _metadata_work(root, work)
    with closing(_MetadataReadWork(root, work)) as scratch:
        directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            document = _read_metadata_document(root, directory, descriptor_sha256, expected_scope, scratch)
            with closing(_MetadataReader(root, directory, document, scratch)) as reader:
                directory = None
                _typed_metadata_eof(reader)
                metadata = OwnerSourceMetadata(scratch, root / _FILES["family"], root / _FILES["query"])
                object.__setattr__(metadata, "_scope", expected_scope)
                object.__setattr__(metadata, "_reader", reader)
                yield metadata
                reader._check_files()
        finally:
            if directory is not None:
                os.close(directory)
