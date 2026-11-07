# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Owner-bound immutable custody records; no allocation or verification proof.

Append inside the allocation's existing page transaction, before returning IDs.
Its result is provisional until a fresh native read confirms the committed head.
Uncommitted files remain owned evidence; this module never deletes a journal.
"""

import hashlib
import os
import re
import stat
import struct
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import msgpack

from db.models.custom_import import CustomImportBuildAttempt, CustomImportCaptureBundle, CustomImportExecution
from process.custom_import import family_block_store as store
from process.custom_import.build_source import _WINDOW, SourceBuildRequest, _assert_build_identity, _page_session
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_layout import FAMILY_BLOCK_LAYOUT_CONTRACT, FamilyBlockLayout
from process.custom_import.family_shard_facts import SourceFactLimits

_CONTRACT = "custom-import/owner-checkpoint/v1"
_OWNER = "owner.mpk"
_LENGTH = struct.Struct("!Q")
_KINDS = {"source_allocation", "family_allocation", "admission_allocation", "retained_allocation", "artifact"}
_NAME = re.compile(r"[a-z][a-z0-9_]{0,47}\Z")


def _integer(value, *, zero=False):
    if type(value) is not int or not (0 if zero else 1) <= value < 2**63:
        raise ValueError("owner checkpoint integer differs")


def _digest(value):
    if type(value) is not bytes or len(value) != 32:
        raise ValueError("owner checkpoint digest differs")


@dataclass(frozen=True)
class OwnerCheckpointHead:
    revision: int
    sha256: bytes

    def __post_init__(self):
        _integer(self.revision)
        _digest(self.sha256)


@dataclass(frozen=True)
class OwnerCheckpointRecord:
    """Bounded caller observations; payload uses that caller's explicit wire format.

    Payload is opaque custody data, never Python serialization or an authority
    token. The consuming allocation/artifact module still validates its meaning.
    """

    kind: str
    ordinal: int
    input_sha256: bytes
    commitments: tuple = ()
    payload: bytes = b""

    def __post_init__(self):
        if type(self.kind) is not str or self.kind not in _KINDS or type(self.payload) is not bytes:
            raise ValueError("owner checkpoint record kind or payload differs")
        _integer(self.ordinal, zero=True)
        _digest(self.input_sha256)
        if type(self.commitments) is not tuple or len(self.commitments) > 32:
            raise ValueError("owner checkpoint commitment budget differs")
        previous = ""
        for item in self.commitments:
            if type(item) is not tuple or len(item) != 2:
                raise ValueError("owner checkpoint commitment shape differs")
            name, digest = item
            if type(name) is not str or _NAME.fullmatch(name) is None or name <= previous:
                raise ValueError("owner checkpoint commitment names differ")
            _digest(digest)
            previous = name


@dataclass(frozen=True)
class PreparedOwnerCheckpoint:
    """A prepared file/head inside an uncommitted transaction, never a commit receipt."""

    head: OwnerCheckpointHead
    previous: OwnerCheckpointHead | None


@dataclass(frozen=True)
class CommittedOwnerCheckpoint:
    """Fresh native head and file custody; consumers must exhaust records for full EOF."""

    root: str
    scope: tuple
    head: OwnerCheckpointHead | None
    owner_sha256: bytes | None
    limits: SourceFactLimits

    def records(self, *, check=None):
        """Read the exact committed chain newest first, without loading all payloads."""
        if check is not None and not callable(check):
            raise ValueError("owner checkpoint cancellation check differs")
        if self.head is None:
            return
        owner_sha, owner_bytes = _owner_file(self.root, self.scope, self.limits, create=False)
        if owner_sha != self.owner_sha256:
            raise FamilyBlockUnavailable("owner checkpoint directory commitment differs")
        head, expected_bytes = self.head, None
        while head is not None:
            if check is not None:
                check()
            previous, prior_bytes, record, total = _entry(self.root, head, owner_sha, self.limits)
            if expected_bytes is not None and total != expected_bytes:
                raise FamilyBlockUnavailable("owner checkpoint chain byte census differs")
            yield head, record
            head, expected_bytes = previous, prior_bytes
        if expected_bytes != owner_bytes:
            raise FamilyBlockUnavailable("owner checkpoint chain lacks original EOF")


def _head(descriptor):
    revision, digest = descriptor.owner_checkpoint_revision, descriptor.owner_checkpoint_sha256
    if revision is None and digest is None:
        return None
    if revision is None or digest is None:
        raise FamilyBlockUnavailable("owner checkpoint native head is partial")
    return OwnerCheckpointHead(revision, bytes(digest))


def _capture_scope(capture, request, build):
    if capture is None or (
        capture.capture_bundle_id,
        capture.dataset_id,
        capture.definition_revision_id,
        capture.schema_revision_id,
        capture.capture_state,
    ) != (
        build.capture_bundle_id,
        request.dataset_id,
        request.definition_revision_id,
        request.schema_revision_id,
        "sealed",
    ):
        raise FamilyBlockUnavailable("owner checkpoint sealed capture differs")
    hashes = tuple(
        bytes(getattr(capture, name)) for name in ("manifest_sha256", "source_request_sha256", "statement_sha256")
    )
    for digest in hashes:
        _digest(digest)
    binding = capture.source_binding_revision_id
    binding_sha = None if capture.source_binding_sha256 is None else bytes(capture.source_binding_sha256)
    if (binding is None) != (binding_sha is None):
        raise FamilyBlockUnavailable("owner checkpoint capture binding differs")
    if binding_sha is not None:
        _integer(binding)
        _digest(binding_sha)
    return capture.capture_bundle_id, binding, binding_sha, capture.payload_contract, *hashes


async def _native_scope(session, request, build_id, family_id, layout, *, writing):
    if type(request) is not SourceBuildRequest or type(layout) is not FamilyBlockLayout:
        raise ValueError("owner checkpoint requires the current typed request and layout")
    _integer(build_id)
    _integer(family_id)
    if not session.in_transaction() or _WINDOW not in session.info:
        raise FamilyBlockUnavailable("owner checkpoint requires the caller's existing page transaction")
    await store._fresh_commit_authority(session, request)
    build = await session.get(CustomImportBuildAttempt, build_id, with_for_update=True, populate_existing=True)
    execution = await session.get(CustomImportExecution, request.execution_id, populate_existing=True)
    if build is None or execution is None or build.build_id != build_id:
        raise FamilyBlockUnavailable("owner checkpoint native build is missing")
    _assert_build_identity(build, request, execution)
    phases = {"loading"} if writing else {"loading", "closed", "verified", "logged"}
    descriptor, binding = await store._candidate(session, request, family_id, layout, phases)
    if (
        descriptor.family_id != family_id
        or descriptor.layout_contract != FAMILY_BLOCK_LAYOUT_CONTRACT
        or bytes(descriptor.definition_sha256) != bytes.fromhex(request.definition.digest)
        or descriptor.collection_slots != dict(layout.collection_slots)
    ):
        raise FamilyBlockUnavailable("owner checkpoint native layout differs")
    capture = await session.get(CustomImportCaptureBundle, build.capture_bundle_id, populate_existing=True)
    return descriptor, _scope_cells(build, capture, request, layout, binding, family_id)


async def bulk_checkpoint_scope(session, request, build_id, family_id, layout, descriptor, binding):
    """Observe file scope inside the caller's checked bulk window; never authorize a write."""
    if type(request) is not SourceBuildRequest or type(layout) is not FamilyBlockLayout:
        raise ValueError("owner checkpoint requires the current typed request and layout")
    _integer(build_id)
    _integer(family_id)
    if not session.in_transaction() or _WINDOW not in session.info:
        raise FamilyBlockUnavailable("owner checkpoint requires a bounded bulk transaction")
    if (
        descriptor.family_id != family_id
        or descriptor.layout_contract != FAMILY_BLOCK_LAYOUT_CONTRACT
        or bytes(descriptor.definition_sha256) != bytes.fromhex(request.definition.digest)
        or descriptor.collection_slots != dict(layout.collection_slots)
    ):
        raise FamilyBlockUnavailable("owner checkpoint native layout differs")
    build = await session.get(CustomImportBuildAttempt, build_id, populate_existing=True)
    execution = await session.get(CustomImportExecution, request.execution_id, populate_existing=True)
    if build is None or execution is None or (build.build_id, build.phase) != (build_id, "source"):
        raise FamilyBlockUnavailable("SOURCE allocation requires its current SOURCE build phase")
    _assert_build_identity(build, request, execution)
    capture = await session.get(CustomImportCaptureBundle, build.capture_bundle_id, populate_existing=True)
    return _scope_cells(build, capture, request, layout, binding, family_id)


def _scope_cells(build, capture, request, layout, binding, family_id):
    return (
        build.build_id,
        family_id,
        build.dataset_id,
        build.definition_revision_id,
        build.schema_revision_id,
        build.execution_id,
        build.producing_fence,
        bytes(build.producing_token_sha256),
        bytes(build.request_identity_sha256),
        build.base_generation_id,
        build.base_pointer_version,
        build.complete_scope,
        build.refresh_mode,
        build.page_row_limit,
        build.page_byte_limit,
        build.statement_timeout_ms,
        build.build_deadline_at.isoformat(),
        request.definition.digest,
        request.definition.schema_digest,
        layout.digest,
        tuple(sorted(layout.collection_slots.items())),
        binding.owner_oid,
        tuple(sorted(binding.oids_by_role.items())),
        _capture_scope(capture, request, build),
    )


def _file_stamp(observed):
    return tuple(
        getattr(observed, "st_" + name)
        for name in ("dev", "ino", "uid", "mode", "size", "mtime_ns", "ctime_ns", "nlink")
    )


@contextmanager
def _directory(root):
    root = Path(root).absolute()
    observed = root.lstat()
    if not stat.S_ISDIR(observed.st_mode) or observed.st_uid != os.geteuid() or stat.S_IMODE(observed.st_mode) != 0o700:
        raise FamilyBlockUnavailable("owner checkpoint root custody differs")
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (observed.st_dev, observed.st_ino):
            raise FamilyBlockUnavailable("owner checkpoint opened root differs")
        yield descriptor, (str(root), observed.st_dev, observed.st_ino, observed.st_uid, 0o700)
        current = root.lstat()
        if (current.st_dev, current.st_ino, current.st_uid, current.st_mode) != (
            observed.st_dev,
            observed.st_ino,
            observed.st_uid,
            observed.st_mode,
        ):
            raise FamilyBlockUnavailable("owner checkpoint root changed during access")
    finally:
        os.close(descriptor)


def _frame_limit(limits):
    if type(limits) is not SourceFactLimits:
        raise ValueError("owner checkpoint requires existing bounded file limits")
    return min(limits.max_record_bytes, limits.memory_bytes // 4)


def _read_file(root, name, expected, limits):
    with _directory(root) as (directory, _identity):
        with os.fdopen(os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory), "rb") as checkpoint_stream:
            before = os.fstat(checkpoint_stream.fileno())
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_uid != os.geteuid()
                or stat.S_IMODE(before.st_mode) != 0o400
                or before.st_nlink != 1
            ):
                raise FamilyBlockUnavailable("owner checkpoint file custody differs")
            prefix = checkpoint_stream.read(8)
            if len(prefix) != 8 or not 0 < (size := _LENGTH.unpack(prefix)[0]) <= _frame_limit(limits):
                raise FamilyBlockUnavailable("owner checkpoint frame is truncated or exceeds its budget")
            frame_payload = checkpoint_stream.read(size)
            if len(frame_payload) != size or checkpoint_stream.read(1):
                raise FamilyBlockUnavailable("owner checkpoint file lacks exact EOF")
            digest = hashlib.sha256(prefix + frame_payload).digest()
            if expected is not None and digest != expected:
                raise FamilyBlockUnavailable("owner checkpoint file SHA differs")
            decoded = msgpack.unpackb(
                frame_payload,
                raw=False,
                use_list=False,
                strict_map_key=True,
                max_array_len=256,
                max_map_len=0,
                max_str_len=4096,
                max_bin_len=_frame_limit(limits),
            )
            identity = before.st_dev, before.st_ino, before.st_uid, 0o400
            if type(decoded) is not tuple or len(decoded) != 2 or decoded[0] != identity:
                raise FamilyBlockUnavailable("owner checkpoint original file identity differs")
            checkpoint_stream.seek(0)
            if hashlib.file_digest(checkpoint_stream, "sha256").digest() != digest or _file_stamp(
                os.fstat(checkpoint_stream.fileno())
            ) != _file_stamp(before):
                raise FamilyBlockUnavailable("owner checkpoint file changed during reading")
            if _file_stamp(os.stat(name, dir_fd=directory, follow_symlinks=False)) != _file_stamp(before):
                raise FamilyBlockUnavailable("owner checkpoint path changed during reading")
            return decoded[1], digest, before.st_size


def _write_file(root, document, limits, name):
    with _directory(root) as (directory, _identity):
        temporary = ".pending-" + uuid4().hex
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory)
        try:
            with os.fdopen(descriptor, "wb") as target:
                observed = os.fstat(target.fileno())
                payload = msgpack.packb(
                    ((observed.st_dev, observed.st_ino, observed.st_uid, 0o400), document), use_bin_type=True
                )
                if len(payload) > _frame_limit(limits):
                    raise ValueError("owner checkpoint record exceeds its frame/memory budget")
                wire = _LENGTH.pack(len(payload)) + payload
                target.write(wire)
                target.flush()
                os.fchmod(target.fileno(), 0o400)
                os.fsync(target.fileno())
            digest = hashlib.sha256(wire).digest()
            os.link(temporary, name(digest), src_dir_fd=directory, dst_dir_fd=directory, follow_symlinks=False)
            os.unlink(temporary, dir_fd=directory)
            os.fsync(directory)
            return digest, len(wire)
        except BaseException:
            with suppress(FileNotFoundError):
                os.unlink(temporary, dir_fd=directory)
            raise


def _owner_file(root, scope, limits, *, create):
    with _directory(root) as (directory, identity):
        expected = _CONTRACT, "owner", scope, identity
        try:
            os.stat(_OWNER, dir_fd=directory, follow_symlinks=False)
        except FileNotFoundError:
            with os.scandir(directory) as entries:
                if not create or next(entries, None) is not None:
                    raise FamilyBlockUnavailable("owner checkpoint directory has no original owner record")
            _write_file(root, expected, limits, lambda _digest: _OWNER)
        document, digest, size = _read_file(root, _OWNER, None, limits)
        if document != expected:
            raise FamilyBlockUnavailable("owner checkpoint directory belongs to another native scope")
        return digest, size


def _filename(head):
    return f"checkpoint-{head.revision:020d}-{head.sha256.hex()}.mpk"


def _record_cells(record):
    return record.kind, record.ordinal, record.input_sha256, record.commitments, record.payload


def _entry(root, head, owner_sha, limits):
    document, _digest, size = _read_file(root, _filename(head), head.sha256, limits)
    if type(document) is not tuple or len(document) != 7 or document[:3] != (_CONTRACT, owner_sha, head.revision):
        raise FamilyBlockUnavailable("owner checkpoint record header differs")
    previous_sha, prior_bytes, cells, complete = document[3:]
    if complete != "complete" or type(cells) is not tuple or len(cells) != 5:
        raise FamilyBlockUnavailable("owner checkpoint record completion differs")
    _integer(prior_bytes)
    if prior_bytes + size > limits.max_spool_bytes:
        raise FamilyBlockUnavailable("owner checkpoint committed chain exceeds its budget")
    if head.revision == 1:
        if previous_sha is not None:
            raise FamilyBlockUnavailable("owner checkpoint first predecessor differs")
        previous = None
    else:
        previous = OwnerCheckpointHead(head.revision - 1, previous_sha)
    return previous, prior_bytes, OwnerCheckpointRecord(*cells), prior_bytes + size


async def append_owner_checkpoint(session, request, build_id, family_id, layout, *, storage, record, expected_head):
    """Prepare an exact append in the caller's allocation transaction; never commit.

    Fresh reconciliation supplies expected_head. Only the exact immediate replay
    is accepted after an uncertain commit. Allocation callers must retain all
    observations until reconciliation and must not allocate on ambiguous state.
    """
    if type(storage) is not CommittedOwnerCheckpoint:
        raise ValueError("owner checkpoint requires its committed storage descriptor")
    root, limits = storage.root, storage.limits
    if type(record) is not OwnerCheckpointRecord or (
        expected_head is not None and type(expected_head) is not OwnerCheckpointHead
    ):
        raise ValueError("owner checkpoint requires typed record and previous head")
    _frame_limit(limits)
    descriptor, scope = await _native_scope(session, request, build_id, family_id, layout, writing=True)
    prepared = prepare_checkpoint_append(root, scope, record, _head(descriptor), expected_head, limits)
    descriptor.owner_checkpoint_revision = prepared.head.revision
    descriptor.owner_checkpoint_sha256 = prepared.head.sha256
    return prepared


def prepare_checkpoint_append(root, scope, checkpoint_record, actual, expected_head, limits):
    """Prepare immutable files only; this result grants no native commit authority."""
    if type(checkpoint_record) is not OwnerCheckpointRecord or (
        expected_head is not None and type(expected_head) is not OwnerCheckpointHead
    ):
        raise ValueError("owner checkpoint requires typed checkpoint_record and previous head")
    _frame_limit(limits)
    owner_sha, owner_bytes = _owner_file(root, scope, limits, create=actual is None)
    prior_bytes = owner_bytes
    if actual is not None:
        previous, _bytes, old_record, prior_bytes = _entry(root, actual, owner_sha, limits)
        if actual != expected_head:
            if previous == expected_head and old_record == checkpoint_record:
                return PreparedOwnerCheckpoint(actual, previous)
            raise FamilyBlockUnavailable("owner checkpoint append conflicts with the protected head")
    elif expected_head is not None:
        raise FamilyBlockUnavailable("owner checkpoint protected head disappeared")
    revision = 1 if actual is None else actual.revision + 1
    _integer(revision)
    document = (
        _CONTRACT,
        owner_sha,
        revision,
        None if actual is None else actual.sha256,
        prior_bytes,
        _record_cells(checkpoint_record),
        "complete",
    )
    # Reserve the maximum frame before creating any file for this append.
    if prior_bytes + len(msgpack.packb(document, use_bin_type=True)) + 128 > limits.max_spool_bytes:
        raise ValueError("owner checkpoint append exceeds its chain budget")
    digest, _size = _write_file(root, document, limits, lambda digest: _filename(OwnerCheckpointHead(revision, digest)))
    prepared = OwnerCheckpointHead(revision, digest)
    return PreparedOwnerCheckpoint(prepared, actual)


async def read_owner_checkpoint(factory, request, build_id, family_id, layout, *, root, limits):
    """Return only a fresh committed native head, with original directory/file custody.

    Terminal native replay precedes this API. It grants neither a new processing
    epoch nor permission to alter closed candidates or remove retained files.
    """
    _frame_limit(limits)
    async with _page_session(factory, request, build_id) as (session, _build):
        descriptor, scope = await _native_scope(session, request, build_id, family_id, layout, writing=False)
        head = _head(descriptor)
        with _directory(root) as (directory, _identity), os.scandir(directory) as entries:
            is_empty = next(entries, None) is None
        owner_sha = None
        if head is not None or not is_empty:
            owner_sha, _bytes = _owner_file(root, scope, limits, create=False)
        if head is not None:
            _entry(root, head, owner_sha, limits)
        result = CommittedOwnerCheckpoint(str(Path(root).absolute()), scope, head, owner_sha, limits)
    return result
