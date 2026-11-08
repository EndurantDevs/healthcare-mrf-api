# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded provisional winner COPY cells and every original candidate context.

Only a caller's already validated immutable generation streams are accepted.
A completed spool proves exhaustion of those streams, not their membership or
completeness against the database. Owner closure still compares that evidence.
"""

import hashlib
import os
import shutil
import stat
import struct
from contextlib import ExitStack, closing
from dataclasses import astuple, dataclass
from pathlib import Path

from db.models.custom_import import CustomImportWinner
from process.custom_import import materialization as selection
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_shard_facts import SourceFactLimits, _Work
from process.custom_import.family_shard_output import _pack_row, _unpack_row
from process.custom_import.family_shard_selection import _profile_inputs, iter_shard_profile_winners
from process.custom_import.materialization import GenerationIdentity, ValidatedWinnerCandidateStream, WinnerCandidate

_CONTRACT = "custom-import/winner-output/v1"
_FILE = "winners.mpk"
_LENGTH = struct.Struct("!Q")
_COLUMNS = tuple(CustomImportWinner.__table__.c.keys())
_ROLE_DOMAIN = b"custom-import/winner-copy-rows/v1\x00"
_STREAM_DOMAIN = b"custom-import/winner-candidate-stream/v1\x00"


@dataclass(frozen=True)
class WinnerWork:
    """Existing caller-owned scratch policy; limits are operational, not input caps."""

    root: object
    limits: SourceFactLimits
    register_cleanup: object
    check: object = None

    def __post_init__(self):
        if type(self.limits) is not SourceFactLimits or not callable(self.register_cleanup):
            raise ValueError("winner output requires explicit accounted work")
        if self.check is not None and not callable(self.check):
            raise ValueError("winner output cancellation check differs")


@dataclass(frozen=True)
class WinnerOutputDescriptor:
    """Picklable completed-file custody, declarations and provisional role evidence."""

    root: str
    device: int
    inode: int
    file_device: int
    file_inode: int
    file_bytes: int
    sha256: bytes
    layout_sha256: str
    definition_sha256: str
    generation: tuple
    profile_slots: tuple
    profile_scopes: tuple
    profiles: tuple
    winner_count: int
    winner_rows_sha256: bytes
    max_frame_bytes: int

    @property
    def role_counts(self):
        """Only winner COPY count; never a replacement for raw context coverage."""
        return {"winner": self.winner_count}


def _check(work):
    if work.check is not None:
        work.check()


def _feed(digest, cells):
    packed = _pack_row(cells)
    digest.update(_LENGTH.pack(len(packed)))
    digest.update(packed)


def _candidate_cells(candidate):
    return (
        candidate.entity_binding_id,
        candidate.family_revision_id,
        bytes(candidate.family_sha256),
        candidate.context_collection_slot,
        candidate.context_child_revision_id,
        None if candidate.context_child_key_sha256 is None else bytes(candidate.context_child_key_sha256),
        tuple(candidate.values_by_field.items()),
    )


def _candidate_from_cells(cells):
    if type(cells) is not tuple or len(cells) != 7:
        raise ValueError("winner candidate audit shape differs")
    fields = cells[-1]
    if type(fields) is not tuple or len(dict(fields)) != len(fields):
        raise ValueError("winner candidate audit fields repeat")
    return WinnerCandidate(*cells[:-1], dict(fields))


def _winner_cells(winner, definition, scopes):
    single = selection.WinnerMaterialization(
        winner.generation,
        len(definition.selection_profiles),
        tuple(scope.collection_slot for scope in scopes),
        (winner,),
    )
    (model,) = selection.winner_materialization_models(single)
    return tuple(getattr(model, name) for name in _COLUMNS)


class _ObservedStream:
    def __init__(self, stream, profile_slot, shard, scope, emit, work):
        self.stream, self.profile_slot, self.shard = stream, profile_slot, shard
        self.scope, self.emit, self.work = scope, emit, work
        self.count = self.matching = 0
        self.digest = hashlib.sha256(_STREAM_DOMAIN)
        self.is_complete = False

    def candidates(self):
        """Record every consumed raw candidate, including losing and other scopes."""
        for candidate in self.stream.consume():
            _check(self.work)
            yield candidate
            # Existing reducer validation runs first. Failed prefixes never publish.
            cells = _candidate_cells(candidate)
            self.emit(("candidate", self.profile_slot, self.shard, self.count, _pack_row(cells)))
            _feed(self.digest, cells)
            self.count += 1
            self.matching += int(candidate.context_collection_slot == self.scope.collection_slot)
            _check(self.work)
        self.is_complete = True

    def census(self):
        """Require actual EOF before publishing this exact stream's evidence."""
        if not self.is_complete:
            raise ValueError("winner candidate stream is incomplete")
        return self.shard, self.count, self.matching, self.digest.digest()


def _profile_rows(layout, generation, slot, factory, emit, work, role_digest):
    with factory(slot) as streams:
        scopes, _contract = _profile_inputs(layout.definition, generation, slot, streams, layout.collection_slots)
        emit(("profile", slot, len(streams)))
        observed_streams = [
            _ObservedStream(stream, slot, shard, scopes[slot - 1], emit, work) for shard, stream in enumerate(streams)
        ]
        count = 0
        with ExitStack() as stack:
            wrapped_streams = tuple(
                ValidatedWinnerCandidateStream(generation, stack.enter_context(closing(observer.candidates())))
                for observer in observed_streams
            )
            winners = iter_shard_profile_winners(
                layout.definition,
                generation=generation,
                profile_slot=slot,
                shards=wrapped_streams,
                child_collection_slots=layout.collection_slots,
            )
            with closing(winners):
                for winner in winners:
                    _check(work)
                    cells = _winner_cells(winner, layout.definition, scopes)
                    emit(("winner", slot, cells))
                    _feed(role_digest, ("winner", cells))
                    count += 1
        census = slot, tuple(observer.census() for observer in observed_streams), count
        emit(("profile_complete", *census))
        return census


def _inputs(layout, generation, factory, work, profile_slots):
    if (
        type(layout) is not FamilyBlockLayout
        or type(generation) is not GenerationIdentity
        or type(work) is not WinnerWork
    ):
        raise ValueError("winner output requires its typed layout, native identity and accounted work")
    if not callable(factory):
        raise ValueError("winner output requires an owner profile-stream context factory")
    slots = tuple(range(1, len(layout.definition.selection_profiles) + 1)) if profile_slots is None else profile_slots
    if type(slots) is not tuple or any(type(slot) is not int for slot in slots):
        raise ValueError("winner output profile declaration differs")
    if slots != tuple(sorted(set(slots))) or any(
        not 1 <= slot <= len(layout.definition.selection_profiles) for slot in slots
    ):
        raise ValueError("winner output profile declaration differs")
    if tuple(layout.tables(1)["winner"].c.keys()) != _COLUMNS:
        raise ValueError("winner native COPY role columns differ")
    return slots


def _file_sha256(source, maximum_frame_bytes, check):
    digest = hashlib.sha256()
    while True:
        if check is not None:
            check()
        block = source.read(min(65536, maximum_frame_bytes))
        if not block:
            return digest.digest()
        digest.update(block)


def _seal_file(work, path, owner_work):
    ready = work.root / _FILE
    os.chmod(path, 0o400)
    with path.open("rb") as source:
        os.fsync(source.fileno())
        digest = _file_sha256(source, work.limits.max_record_bytes, lambda: _check(owner_work))
    os.link(path, ready, follow_symlinks=False)
    path.unlink()
    work.paths[ready] = work.paths.pop(path)
    directory = os.open(work.root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return ready, digest


def _completed_output(scratch, ready, digest, layout, generation, profiles, profile_scopes, role_digest):
    info = ready.stat()
    return WinnerOutputDescriptor(
        str(scratch.root),
        scratch.identity.st_dev,
        scratch.identity.st_ino,
        info.st_dev,
        info.st_ino,
        info.st_size,
        digest,
        layout.digest,
        layout.definition.digest,
        astuple(generation),
        tuple(slot for slot, _scope in profile_scopes),
        profile_scopes,
        tuple(profiles),
        sum(profile[2] for profile in profiles),
        role_digest.digest(),
        scratch.limits.max_record_bytes,
    )


def build_winner_output(layout, generation, profile_streams, *, work, profile_slots=None):
    """Complete each profile once, then atomically expose a closed owned spool.

    ``profile_streams(slot)`` borrows a bounded sequence of fresh validated
    streams inside the caller's context manager. It owns closing source cursors
    on success and failure. The default covers every profile; explicit slots are
    declared in the descriptor and never imply all-profile acceptance.
    """
    slots = _inputs(layout, generation, profile_streams, work, profile_slots)
    scopes = selection._profile_scopes(layout.definition, layout.collection_slots)
    profile_scopes = tuple((slot, scopes[slot - 1].collection_slot) for slot in slots)
    scratch = _Work(work.root, work.limits, work.register_cleanup)
    path = scratch.path()
    profiles, role_digest = [], hashlib.sha256(_ROLE_DOMAIN)
    try:
        with path.open("xb") as output:

            def emit(cells):
                """Charge complete frames to the existing owner scratch budget."""
                _check(work)
                scratch.frame(output, cells, path)
                _check(work)

            emit(
                (
                    "header",
                    _CONTRACT,
                    layout.digest,
                    layout.definition.digest,
                    astuple(generation),
                    slots,
                    profile_scopes,
                    _COLUMNS,
                )
            )
            for slot in slots:
                profiles.append(_profile_rows(layout, generation, slot, profile_streams, emit, work, role_digest))
            count = sum(profile[2] for profile in profiles)
            emit(("complete", tuple(profiles), count, role_digest.digest()))
        ready, digest = _seal_file(scratch, path, work)
        _check(work)
        return _completed_output(scratch, ready, digest, layout, generation, profiles, profile_scopes, role_digest)
    except BaseException:
        scratch.close()
        raise


def _owned_file(descriptor):
    if type(descriptor) is not WinnerOutputDescriptor:
        raise ValueError("winner output descriptor differs")
    root = Path(descriptor.root)
    observed = root.lstat()
    if not stat.S_ISDIR(observed.st_mode) or (observed.st_dev, observed.st_ino) != (
        descriptor.device,
        descriptor.inode,
    ):
        raise ValueError("winner output root custody differs")
    path = root / _FILE
    observed = path.lstat()
    if (
        not stat.S_ISREG(observed.st_mode)
        or (observed.st_dev, observed.st_ino, observed.st_size)
        != (descriptor.file_device, descriptor.file_inode, descriptor.file_bytes)
        or {entry.name for entry in root.iterdir()} != {_FILE}
    ):
        raise ValueError("winner output file custody differs")
    return path


def _file_entries(descriptor, check):
    path = _owned_file(descriptor)
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as source:
        observed = os.fstat(source.fileno())
        if (observed.st_dev, observed.st_ino, observed.st_size) != (
            descriptor.file_device,
            descriptor.file_inode,
            descriptor.file_bytes,
        ):
            raise ValueError("winner output open custody differs")
        digest = hashlib.sha256()
        while prefix := source.read(8):
            if check is not None:
                check()
            if len(prefix) != 8:
                raise ValueError("winner output frame prefix is truncated")
            size = _LENGTH.unpack(prefix)[0]
            if not 0 < size <= descriptor.max_frame_bytes:
                raise ValueError("winner output frame exceeds its resource budget")
            packed = source.read(size)
            if len(packed) != size:
                raise ValueError("winner output frame is truncated")
            digest.update(prefix)
            digest.update(packed)
            yield _unpack_row(packed)
        if digest.digest() != descriptor.sha256:
            raise ValueError("winner output bytes differ at complete EOF")


class _ReplayCensus:
    def __init__(self, descriptor):
        self.descriptor, self.profile = descriptor, None
        self.completed = []
        self.winners = 0
        self.role_digest = hashlib.sha256(_ROLE_DOMAIN)
        self.is_complete = False

    def accept(self, entry):
        """Validate every profile, raw occurrence and COPY row before complete EOF."""
        if self.is_complete:
            raise ValueError("winner output contains trailing records")
        match entry[0]:
            case "profile":
                self._begin(entry)
            case "candidate":
                self._candidate(entry)
            case "winner":
                self._winner(entry)
            case "profile_complete":
                self._end(entry)
            case "complete":
                if self.profile is not None or entry != (
                    "complete",
                    self.descriptor.profiles,
                    self.descriptor.winner_count,
                    self.descriptor.winner_rows_sha256,
                ):
                    raise ValueError("winner output completion differs")
                if (
                    tuple(self.completed) != self.descriptor.profiles
                    or self.role_digest.digest() != self.descriptor.winner_rows_sha256
                ):
                    raise ValueError("winner output role proof differs")
                self.is_complete = True
            case _:
                raise ValueError("winner output record kind differs")

    def _begin(self, entry):
        _, slot, streams = entry
        if (
            self.profile is not None
            or len(self.completed) >= len(self.descriptor.profile_slots)
            or slot != self.descriptor.profile_slots[len(self.completed)]
        ):
            raise ValueError("winner output profile order differs")
        if type(streams) is not int or streams < 0:
            raise ValueError("winner output stream declaration differs")
        if streams != len(self.descriptor.profiles[len(self.completed)][1]):
            raise ValueError("winner output stream census differs")
        self.profile = slot, [[0, 0, hashlib.sha256(_STREAM_DOMAIN)] for _ in range(streams)], 0

    def _candidate(self, entry):
        _, slot, shard, ordinal, packed = entry
        if (
            self.profile is None
            or slot != self.profile[0]
            or type(shard) is not int
            or not 0 <= shard < len(self.profile[1])
        ):
            raise ValueError("winner candidate profile/shard differs")
        census = self.profile[1][shard]
        if type(ordinal) is not int or ordinal != census[0]:
            raise ValueError("winner candidate occurrence sequence differs")
        cells = _unpack_row(packed)
        candidate = _candidate_from_cells(cells)
        _feed(census[2], cells)
        census[0] += 1
        scope = dict(self.descriptor.profile_scopes)[slot]
        census[1] += int(candidate.context_collection_slot == scope)

    def _winner(self, entry):
        _, slot, cells = entry
        if self.profile is None or slot != self.profile[0] or len(cells) != len(_COLUMNS):
            raise ValueError("winner COPY row scope differs")
        by_name = dict(zip(_COLUMNS, cells, strict=True))
        if (
            tuple(
                by_name[name]
                for name in ("generation_id", "dataset_id", "definition_revision_id", "schema_revision_id")
            )
            != self.descriptor.generation
            or by_name["profile_slot"] != slot
        ):
            raise ValueError("winner COPY row generation differs")
        _feed(self.role_digest, ("winner", cells))
        self.profile = self.profile[0], self.profile[1], self.profile[2] + 1
        self.winners += 1

    def _end(self, entry):
        _, slot, streams, winners = entry
        if (
            self.profile is None
            or slot != self.profile[0]
            or winners != self.profile[2]
            or len(streams) != len(self.profile[1])
        ):
            raise ValueError("winner profile completion census differs")
        for shard, (observed, declared) in enumerate(zip(self.profile[1], streams, strict=True)):
            if declared != (shard, observed[0], observed[1], observed[2].digest()):
                raise ValueError("winner candidate stream proof differs")
        self.completed.append((slot, streams, winners))
        self.profile = None


def _checked_entries(descriptor, check=None):
    census = _ReplayCensus(descriptor)
    with closing(_file_entries(descriptor, check)) as entries:
        if next(entries, None) != (
            "header",
            _CONTRACT,
            descriptor.layout_sha256,
            descriptor.definition_sha256,
            descriptor.generation,
            descriptor.profile_slots,
            descriptor.profile_scopes,
            _COLUMNS,
        ):
            raise ValueError("winner output header differs")
        for entry in entries:
            census.accept(entry)
            yield entry
    if not census.is_complete or census.winners != descriptor.winner_count:
        raise ValueError("winner output is incomplete")


def read_winner_rows(descriptor, *, check=None):
    """Replay bounded exact native COPY cells; only full EOF proves the role count."""
    with closing(_checked_entries(descriptor, check)) as entries:
        for entry in entries:
            if entry[0] == "winner":
                yield entry[2]


def read_winner_candidates(descriptor, *, check=None):
    """Replay ALL raw contexts as (profile, shard, ordinal, original candidate)."""
    with closing(_checked_entries(descriptor, check)) as entries:
        for entry in entries:
            if entry[0] == "candidate":
                yield (*entry[1:4], _candidate_from_cells(_unpack_row(entry[4])))


def cleanup_winner_output(descriptor):
    """Remove only the exact registered completed output, preserving foreign state."""
    _owned_file(descriptor)
    shutil.rmtree(descriptor.root)
