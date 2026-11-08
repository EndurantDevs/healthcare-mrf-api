# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact SOURCE ID vectors under the existing protected owner checkpoint head.

Vector files are written before the native transaction commits its checkpoint.
Neither a file nor this live cleanup owner proves commit. Unknown responses need
fresh native reconciliation; all partial files remain owned until terminal join.
"""

import hashlib
import os
import re
import shutil
import stat
import threading
from contextlib import closing, contextmanager
from dataclasses import astuple, dataclass, field, replace
from pathlib import Path
from uuid import uuid4

import msgpack

from process.custom_import import family_shard_checkpoint as checkpoint_api
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_shard_checkpoint import OwnerCheckpointRecord
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded, SourceFactLimits, _sorted, _Work
from process.custom_import.runner_types import CancellationRequested

_CONTRACT = "custom-import/source-allocation-vectors/v1"
_OWNER_SLOTS = {"base-keys", "final-plan", "retained", "output", "finality"}
_SOURCE_SLOTS = {"ready-read", "metadata", "admission", "counts", "homes"}
_RESUME_NAME = re.compile(r"resume-[0-9a-f]{32}\Z")


@dataclass(frozen=True)
class SourceOwnerCustody:
    """Protected directory identities and admitted budgets, never a replacement lease."""

    directories: tuple
    lock_identity: tuple
    profile: tuple

    def __post_init__(self):
        if type(self.directories) is not tuple or len(self.directories) != 3:
            raise ValueError("SOURCE owner directory scope differs")
        for path, identity in self.directories:
            if type(path) is not str or not Path(path).is_absolute() or "\0" in path:
                raise ValueError("SOURCE owner directory path differs")
            if type(identity) is not tuple or len(identity) != 4:
                raise ValueError("SOURCE owner directory identity differs")
            for value in identity:
                _integer(value, zero=True)
        root_paths = tuple(Path(path) for path, _identity in self.directories)
        if root_paths[1] != root_paths[0] / "source" or root_paths[2] != root_paths[1] / "allocation-journal":
            raise ValueError("SOURCE owner directory hierarchy differs")
        if type(self.lock_identity) is not tuple or len(self.lock_identity) != 2:
            raise ValueError("SOURCE owner lifetime lock identity differs")
        for value in self.lock_identity:
            _integer(value)
        _check_profile(self.profile)
        if len(self.profile) != 3 or type(self.profile[0]) is not tuple or len(self.profile[0]) != 3:
            raise ValueError("SOURCE owner job budget profile differs")
        if type(self.profile[2]) is not tuple or len(self.profile[2]) != 6:
            raise ValueError("SOURCE owner stage budget profile differs")

    def current(self, root, profile):
        """Compare protected identities before any directory is adopted."""
        if Path(root).absolute() != Path(self.directories[0][0]) or self.profile != profile:
            raise FamilyBlockUnavailable("SOURCE owner root or resource profile differs")
        for path, identity in self.directories:
            if _directory(Path(path)) != identity:
                raise FamilyBlockUnavailable("SOURCE owner directory custody changed")
        observed = (Path(root) / ".owner-job.lock").lstat()
        if (observed.st_dev, observed.st_ino) != self.lock_identity:
            raise FamilyBlockUnavailable("SOURCE owner lifetime lock was replaced")


def _check_profile(profile, depth=0):
    if type(profile) is not tuple or not profile or len(profile) > 16 or depth > 5:
        raise ValueError("SOURCE owner resource profile differs")
    for value in profile:
        if type(value) is tuple:
            _check_profile(value, depth + 1)
            continue
        if value is None:
            continue
        if type(value) is str:
            if not value or len(value) > 4096 or "\0" in value:
                raise ValueError("SOURCE owner spool directory differs")
            continue
        _integer(value)


def source_owner_custody(owner_lock, source_root, journal, profile):
    """Capture the existing held owner, SOURCE and journal directories before launch."""
    from process.custom_import.family_shard_owner_lock import OwnerJobLock

    if type(owner_lock) is not OwnerJobLock or type(journal) is not SourceAllocationJournal:
        raise ValueError("SOURCE custody requires its held owner and journal")
    owner_lock.check()
    journal.current()
    roots = (owner_lock.root, Path(source_root).absolute(), journal.root)
    custody = SourceOwnerCustody(
        tuple((str(path), _directory(path)) for path in roots), owner_lock.file_identity, profile
    )
    custody.current(owner_lock.root, profile)
    return custody


def _census_stamp(observed):
    return tuple(
        getattr(observed, "st_" + name) for name in ("dev", "ino", "uid", "mode", "size", "mtime_ns", "ctime_ns")
    )


@contextmanager
def _census_directory(parent, name):
    observed = os.stat(name, dir_fd=parent, follow_symlinks=False)
    if not stat.S_ISDIR(observed.st_mode) or observed.st_uid != os.getuid() or stat.S_IMODE(observed.st_mode) != 0o700:
        raise FamilyBlockUnavailable("retained work directory owner or type differs")
    descriptor = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent)
    try:
        if _census_stamp(os.fstat(descriptor)) != _census_stamp(observed):
            raise FamilyBlockUnavailable("retained work directory changed during census")
        yield descriptor, observed
        if _census_stamp(os.fstat(descriptor)) != _census_stamp(observed) or _census_stamp(
            os.stat(name, dir_fd=parent, follow_symlinks=False)
        ) != _census_stamp(observed):
            raise FamilyBlockUnavailable("retained work directory changed during census")
    finally:
        os.close(descriptor)


def _entry_cost(observed, unit):
    # Charge rounded data/allocation blocks plus one allocation unit per entry.
    # In particular, empty files and directories never have zero retained cost.
    size = max(observed.st_size, observed.st_blocks * 512)
    return ((size + unit - 1) // unit + 1) * unit


def _filesystem_unit(directory):
    filesystem = os.fstatvfs(directory)
    return max(512, filesystem.f_frsize or filesystem.f_bsize)


def _census_tree(parent, name, budget, unit, check, depth=0):
    if depth > 3:
        raise FamilyBlockUnavailable("retained work directory nesting differs")
    check()
    with _census_directory(parent, name) as (directory, observed):
        budget.charge(_entry_cost(observed, unit))
        with os.scandir(directory) as entries:
            for entry in entries:
                check()
                observed = entry.stat(follow_symlinks=False)
                if stat.S_ISDIR(observed.st_mode):
                    _census_tree(directory, entry.name, budget, unit, check, depth + 1)
                    continue
                if not stat.S_ISREG(observed.st_mode) or observed.st_uid != os.getuid() or observed.st_nlink != 1:
                    raise FamilyBlockUnavailable("retained work file owner, link or type differs")
                budget.charge(_entry_cost(observed, unit))


def _attempt_slot_cost(directory, name, slot, budget, unit, check):
    with _census_directory(directory, name) as (attempt, observed):
        budget.charge(_entry_cost(observed, unit))
        try:
            os.stat(slot, dir_fd=attempt, follow_symlinks=False)
        except FileNotFoundError:
            return
        _census_tree(attempt, slot, budget, unit, check)


def _source_slot_cost(directory, slot, budget, unit, check):
    with os.scandir(directory) as entries:
        for entry in entries:
            check()
            if entry.name == slot:
                _census_tree(directory, slot, budget, unit, check)
                continue
            if _RESUME_NAME.fullmatch(entry.name):
                _attempt_slot_cost(directory, entry.name, slot, budget, unit, check)


def _owner_slot_cost(directory, slot, budget, unit, check):
    with os.scandir(directory) as entries:
        for entry in entries:
            check()
            if re.fullmatch(slot + r"(?:-[0-9a-f]{32})?", entry.name):
                _census_tree(directory, entry.name, budget, unit, check)


def _work_scope(custody, proposed, slot, limits):
    owner_root, source_root = Path(custody.directories[0][0]), Path(custody.directories[1][0])
    if (
        slot in _OWNER_SLOTS
        and proposed.parent == owner_root
        and re.fullmatch(slot + r"(?:-[0-9a-f]{32})?", proposed.name)
    ):
        expected, parent = custody.profile[0], owner_root
    elif (
        slot in _SOURCE_SLOTS
        and proposed.name == slot
        and (
            proposed.parent == source_root
            or (proposed.parent.parent == source_root and _RESUME_NAME.fullmatch(proposed.parent.name))
        )
    ):
        expected, parent = custody.profile[2][2], source_root
    else:
        raise FamilyBlockUnavailable("retained work logical slot differs")
    if astuple(limits) != expected:
        raise FamilyBlockUnavailable("retained work original allowance differs")
    return parent


def retained_work_limits(custody, owner_lock, proposed, slot, limits, check):
    """Deduct retained slot attempts before creation; keep the signed profile unchanged.

    This is a per-slot retained-work bound, not a simultaneous node-disk quota.
    No scanned file is adopted or deleted and no new allowance is introduced.
    """
    from process.custom_import.family_shard_owner_lock import OwnerJobLock

    if (
        type(custody) is not SourceOwnerCustody
        or type(owner_lock) is not OwnerJobLock
        or type(limits) is not SourceFactLimits
    ):
        raise ValueError("retained work requires original typed custody and limits")
    proposed = Path(proposed).absolute()
    parent = _work_scope(custody, proposed, slot, limits)

    def current():
        """Keep the original owner, cancellation and deadline active during the census."""
        check()
        owner_lock.check()
        custody.current(owner_lock.root, custody.profile)

    current()
    with _census_directory(None, str(parent)) as (directory, observed):
        unit = _filesystem_unit(directory)
        budget = _JournalBudget(limits.max_spool_bytes, _entry_cost(observed, unit) + 2 * unit)
        if slot in _OWNER_SLOTS:
            _owner_slot_cost(directory, slot, budget, unit, current)
        else:
            _source_slot_cost(directory, slot, budget, unit, current)
            if proposed.parent != parent:
                try:
                    os.stat(proposed.parent.name, dir_fd=directory, follow_symlinks=False)
                except FileNotFoundError:
                    budget.charge(2 * unit)
    remaining = limits.max_spool_bytes - budget.used
    if remaining < max(limits.max_record_bytes, limits.memory_bytes):
        raise SourceFactBudgetExceeded("retained work leaves insufficient original spool allowance")
    current()
    return replace(limits, max_spool_bytes=remaining)


class _JournalBudget:
    def __init__(self, maximum, used, unit=0):
        self.maximum, self.used, self.unit = maximum, used, unit
        self.ensure(0)

    def ensure(self, count):
        """Refuse allocation beyond the existing aggregate journal disk bound."""
        if self.used + count > self.maximum:
            raise SourceFactBudgetExceeded("SOURCE journal aggregate spool budget is exhausted")

    def charge(self, count):
        """Charge persistent journal bytes under the existing aggregate disk bound."""
        self.ensure(count)
        self.used += count

    def release(self, count):
        """Release the exact recorded bytes after owned artifact removal."""
        if not 0 <= count <= self.used:
            raise FamilyBlockUnavailable("SOURCE journal byte accounting differs")
        self.used -= count


class SourceAllocationWork(_Work):
    """All files under one journal share its reconciled aggregate byte budget."""

    def __init__(self, journal, root, limits, *, check=None, register_cleanup=None):
        self.budget = journal.budget()
        if self.budget.unit <= 0:
            raise FamilyBlockUnavailable("SOURCE journal allocation units require fresh reconciliation")
        self.budget.ensure(2 * self.budget.unit + max(limits.max_record_bytes, limits.memory_bytes))
        self._overhead = 0
        super().__init__(
            root,
            limits,
            journal.work.register_cleanup if register_cleanup is None else register_cleanup,
            check=journal.current if check is None else check,
        )

    def _reserve_entry(self):
        if self._entry_charge != 2 * self.budget.unit:
            raise FamilyBlockUnavailable("SOURCE journal work allocation units differ")
        self.budget.ensure(self._entry_charge + self.limits.max_record_bytes)
        super()._reserve_entry()
        self.budget.charge(self._entry_charge)
        self._overhead += self._entry_charge

    def _release_entry(self, raw_bytes=0):
        super()._release_entry(raw_bytes)
        self.budget.release(self._entry_charge + raw_bytes)
        self._overhead -= self._entry_charge

    def charge(self, count):
        """Charge owned SOURCE scratch before allocating additional bytes."""
        self.budget.ensure(count)
        super().charge(count)
        self.budget.charge(count)

    def remove(self, path):
        """Release one exact owned scratch path and its recorded byte charge."""
        size = self.paths[path]
        super().remove(path)
        self.budget.release(size)

    def close(self):
        """Remove owned SOURCE scratch and release its exact existing charge."""
        remaining = self.used + self._overhead
        super().close()
        self.budget.release(remaining)
        self.used = self._overhead = 0

    def _confirm_removed(self, path):
        _removed(path)


def _removed(path):
    try:
        path.lstat()
    except FileNotFoundError:
        return
    raise FamilyBlockUnavailable("SOURCE journal removal is not confirmed; retained charge remains")


class _VectorFrames(_Work):
    """Borrow bounded framing/accounting without acquiring directory cleanup."""

    def __init__(self, journal):
        self.limits, self._io_check = journal.work.limits, journal.current
        self.budget = journal._budget
        self.used = self.peak_bytes = 0
        self._entry_bytes = 0  # Its entry was already reserved by the journal before open.

    def charge(self, count):
        """Account encoded vector bytes before writing the bounded frame."""
        if self.budget is None:
            raise FamilyBlockUnavailable("SOURCE journal requires fresh aggregate reconciliation")
        self.budget.ensure(count)
        super().charge(count)
        self.budget.charge(count)


def _integer(value, *, zero=False):
    if type(value) is not int or not int(not zero) <= value < 2**63:
        raise FamilyBlockUnavailable("SOURCE journal integer differs")


def _directory(path):
    observed = path.lstat()
    if not stat.S_ISDIR(observed.st_mode) or observed.st_uid != os.getuid() or stat.S_IMODE(observed.st_mode) != 0o700:
        raise FamilyBlockUnavailable("SOURCE journal directory owner or mode differs")
    return observed.st_dev, observed.st_ino, observed.st_uid, observed.st_mode


def _stamp(observed):
    if (
        not stat.S_ISREG(observed.st_mode)
        or observed.st_uid != os.getuid()
        or stat.S_IMODE(observed.st_mode) != 0o400
        or observed.st_nlink != 1
    ):
        raise FamilyBlockUnavailable("SOURCE vector file owner or mode differs")
    return tuple(
        getattr(observed, "st_" + name) for name in ("dev", "ino", "uid", "mode", "size", "mtime_ns", "ctime_ns")
    )


def _sync(root):
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@dataclass
class SourceAllocationJournal:
    """Live file custody, not native authority or a serialized request/session."""

    build_id: int
    work: object
    control_limits: SourceFactLimits
    identity: tuple
    stop: object = field(default_factory=threading.Event, init=False, repr=False)
    busy: bool = field(default=False, init=False)
    _budget: object = field(default=None, init=False, repr=False)

    @property
    def root(self):
        """Return the exact protected journal root owned by this SOURCE attempt."""
        return Path(self.work.root).absolute()

    @property
    def checkpoints(self):
        """Return the immutable original SOURCE checkpoint sequence."""
        return self.root / "checkpoints"

    def current(self):
        """Check original owner authority and journal identity before further work."""
        if self.stop.is_set():
            raise CancellationRequested("SOURCE allocation journal was stopped")
        if self.work.check is not None:
            self.work.check()
        if _directory(self.root) != self.identity:
            raise FamilyBlockUnavailable("SOURCE journal root custody differs")

    def budget(self):
        """Expose the existing aggregate journal byte budget."""
        if self._budget is None:
            raise FamilyBlockUnavailable("SOURCE journal requires fresh aggregate reconciliation")
        return self._budget

    def reconcile_budget(self):
        """Count retained allocation and entry costs once, including empty attempts."""
        self.current()
        self._budget = None
        with _census_directory(None, str(self.root)) as (directory, _observed):
            unit = _filesystem_unit(directory)
            budget = _JournalBudget(self.work.limits.max_spool_bytes, 0, unit)
            _census_tree(None, str(self.root), budget, unit, self.current)
        self._budget = budget
        self.current()

    def close_terminal(self):
        """Caller must stop/drain all readers and decide terminal custody first."""
        if self.busy:
            raise FamilyBlockUnavailable("SOURCE allocation must drain before journal cleanup")
        if _directory(self.root) != self.identity:
            raise FamilyBlockUnavailable("SOURCE journal cleanup custody differs")
        shutil.rmtree(self.root)


def create_source_allocation_journal(build_id, *, work, control_limits):
    """Register a fresh owner before creating any retained journal/vector files."""
    from process.custom_import.family_shard_plan import PlanWork

    _integer(build_id)
    if type(work) is not PlanWork or type(control_limits) is not SourceFactLimits:
        raise ValueError("SOURCE journal requires explicit data and control budgets")
    root = Path(work.root).absolute()
    if work.check is not None:
        work.check()
    if root.exists() or root.is_symlink():
        raise FileExistsError("SOURCE journal already exists; retained custody is required")
    work.register_cleanup(root)
    root.mkdir(mode=0o700)
    for name in ("checkpoints", "vectors", "outputs"):
        (root / name).mkdir(mode=0o700)
    _sync(root)
    _sync(root.parent)
    return SourceAllocationJournal(build_id, work, control_limits, _directory(root))


async def reopen_source_allocation_journal(factory, context, custody, owner_lock, *, work, control_limits, head):
    """Reauthenticate an existing protected head; never create or reset retained files."""
    from process.custom_import.family_shard_owner_lock import OwnerJobLock
    from process.custom_import.family_shard_plan import PlanWork

    if type(custody) is not SourceOwnerCustody or type(owner_lock) is not OwnerJobLock or type(work) is not PlanWork:
        raise ValueError("SOURCE journal reopening requires protected owner custody")
    owner_lock.check()
    custody.current(owner_lock.root, custody.profile)
    if (
        owner_lock.file_identity != custody.lock_identity
        or str(Path(work.root).absolute()) != custody.directories[2][0]
    ):
        raise FamilyBlockUnavailable("SOURCE journal reopening belongs to another owner")
    if astuple(work.limits) != custody.profile[2][1][0] or astuple(control_limits) != custody.profile[2][5]:
        raise FamilyBlockUnavailable("SOURCE journal original budgets differ")
    snapshot = await checkpoint_api.read_owner_checkpoint(
        factory,
        context.request,
        context.build_id,
        context.family_id,
        context.layout,
        root=Path(work.root) / "checkpoints",
        limits=control_limits,
    )
    if head is None or snapshot.head != head:
        raise FamilyBlockUnavailable("SOURCE protected head changed during owner reopening")
    journal = SourceAllocationJournal(context.build_id, work, control_limits, custody.directories[2][1])
    journal.current()
    if not {"checkpoints", "vectors", "outputs"} <= {path.name for path in journal.root.iterdir()}:
        raise FamilyBlockUnavailable("SOURCE journal directory census is incomplete")
    for name in ("checkpoints", "vectors", "outputs"):
        _directory(journal.root / name)
    journal.reconcile_budget()
    work.register_cleanup(journal.root)
    return journal, snapshot


async def _custody_snapshot(factory, context, journal):
    return await checkpoint_api.read_owner_checkpoint(
        factory,
        context.request,
        context.build_id,
        context.family_id,
        context.layout,
        root=journal.checkpoints,
        limits=journal.control_limits,
    )


async def commit_source_custody(factory, context, journal, checkpoint_record):
    """Commit one prepared custody record, reconciling an exact lost acknowledgement."""
    snapshot = await _custody_snapshot(factory, context, journal)
    previous, prepared = snapshot.head, None
    with closing(snapshot.records(check=journal.current)) as checkpoint_records:
        latest = next(checkpoint_records, None)
    if latest is not None and latest[1] == checkpoint_record:
        return snapshot.head
    journal.reconcile_budget()
    failure = None
    try:
        async with checkpoint_api._page_session(factory, context.request, context.build_id) as (session, build):
            if build is None or build.phase != "source":
                raise FamilyBlockUnavailable("SOURCE custody can only advance its original SOURCE phase")
            reserved = reserve_checkpoint(journal)
            prepared = await checkpoint_api.append_owner_checkpoint(
                session,
                context.request,
                context.build_id,
                context.family_id,
                context.layout,
                storage=snapshot,
                record=checkpoint_record,
                expected_head=previous,
            )
            finish_checkpoint_charge(journal, reserved, prepared)
    except Exception as error:
        failure = error
    try:
        confirmed = await _custody_snapshot(factory, context, journal)
    except BaseException as error:
        raise FamilyBlockUnavailable("SOURCE custody commit is unknown; retain every original") from error
    if confirmed.head == previous and failure is not None:
        raise failure
    with closing(confirmed.records(check=journal.current)) as checkpoint_records:
        latest, prior = next(checkpoint_records, None), next(checkpoint_records, None)
    if (
        latest is None
        or latest[1] != checkpoint_record
        or latest[0].revision != (1 if previous is None else previous.revision + 1)
        or (None if prior is None else prior[0]) != previous
        or (failure is None and (prepared is None or confirmed.head != prepared.head))
    ):
        raise FamilyBlockUnavailable("SOURCE custody lacks its exact committed successor")
    return confirmed.head


def input_sha256(plan, family_id, ordinal, packs):
    """Bind exact original pack coordinates and commitments, not sequence ranges."""
    return hashlib.sha256(
        msgpack.packb(
            (
                _CONTRACT,
                plan.sha256,
                family_id,
                ordinal,
                tuple(
                    (
                        pack.pack_index,
                        pack.stream_slot,
                        pack.collection_slot,
                        len(pack.records),
                        pack.pack_sha256,
                        pack.landing.manifest_sha256,
                    )
                    for pack in packs
                ),
            ),
            use_bin_type=True,
        )
    ).digest()


def reserve_checkpoint(journal):
    """Reserve core's maximum new owner/entry frames before it creates files."""
    is_owner_missing = not (journal.checkpoints / checkpoint_api._OWNER).exists()
    maximum = (1 + int(is_owner_missing)) * (
        checkpoint_api._frame_limit(journal.control_limits) + 8 + 2 * journal.budget().unit
    )
    journal.budget().charge(maximum)
    return maximum, is_owner_missing


def finish_checkpoint_charge(journal, reservation, prepared):
    """Only a successful append has exact names; failures retain the full charge."""
    maximum, owner_created = reservation
    actual = _entry_cost((journal.checkpoints / checkpoint_api._filename(prepared.head)).lstat(), journal.budget().unit)
    if owner_created:
        actual += _entry_cost((journal.checkpoints / checkpoint_api._OWNER).lstat(), journal.budget().unit)
    if actual > maximum:
        raise FamilyBlockUnavailable("SOURCE checkpoint exceeded its reserved frame budget")
    journal.budget().release(maximum - actual)


def reserve_vector(journal, payload_bytes):
    """Reserve a vector entry and allocation rounding before its first open."""
    budget = journal.budget()
    overhead = 2 * budget.unit
    budget.ensure(payload_bytes + overhead)
    budget.charge(overhead)


@contextmanager
def replay_records(journal, checkpoint, plan):
    """Exhaust the committed chain once and externally order small descriptors."""
    journal.current()
    scratch = SourceAllocationWork(journal, journal.root / ("scan-" + uuid4().hex), journal.control_limits)

    def records():
        """Replay the exact original SOURCE record sequence with its EOF guards."""
        for _head, record in checkpoint.records(check=journal.current):
            if record.kind == "source_allocation":
                if dict(record.commitments).get("source_plan") != plan.sha256:
                    raise FamilyBlockUnavailable("SOURCE journal belongs to a different original plan")
                yield record.ordinal, record.input_sha256, record.commitments, record.payload

    rows = _sorted(scratch, records(), lambda row: row[0])
    try:
        yield rows
    finally:
        try:
            rows.close()
        finally:
            scratch.close()


def write_vectors(journal, plan, family_id, ordinal, packs, allocation_rows):
    """Prepare immutable bytes before the caller appends in the SAME transaction."""
    journal.current()
    expected = input_sha256(plan, family_id, ordinal, packs)
    root = journal.root / "vectors"
    directory = _directory(root)
    name = "vectors-" + uuid4().hex + ".mpk"
    path = root / name
    source_records = sum(len(pack.records) for pack in packs)
    header = (_CONTRACT, plan.sha256, family_id, ordinal, expected, len(packs), source_records)
    # An exhausted budget cannot create another retained zero-byte attempt.
    reserve_vector(
        journal,
        sum(
            len(msgpack.packb(source_row, use_bin_type=True)) + 8
            for source_row in (header, *allocation_rows, ("end", len(packs), source_records))
        ),
    )
    # _Work's bounded framed encoder is used without transferring cleanup custody.
    writer = _VectorFrames(journal)
    with path.open("xb") as source_target:
        writer.frame(source_target, header)
        for source_row in allocation_rows:
            writer.frame(source_target, source_row)
        writer.frame(source_target, ("end", len(packs), source_records))
        source_target.flush()
        os.fchmod(source_target.fileno(), 0o400)
        os.fsync(source_target.fileno())
        stamp = _stamp(os.fstat(source_target.fileno()))
    _sync(root)
    with path.open("rb") as source_source:
        digest = hashlib.file_digest(source_source, "sha256").digest()
    source_payload = msgpack.packb(
        (
            _CONTRACT,
            str(journal.root),
            journal.identity,
            directory,
            name,
            stamp,
            digest,
            family_id,
            len(packs),
            source_records,
        ),
        use_bin_type=True,
    )
    return OwnerCheckpointRecord(
        "source_allocation",
        ordinal,
        expected,
        (("allocation_vectors", digest), ("source_plan", plan.sha256)),
        source_payload,
    )


def _checked_vector_cells(journal, checkpoint, plan, family_id, ordinal, packs, expected):
    if checkpoint.kind != "source_allocation" or checkpoint.ordinal != ordinal or checkpoint.input_sha256 != expected:
        raise FamilyBlockUnavailable("SOURCE journal original batch input differs")
    cells = msgpack.unpackb(
        checkpoint.payload,
        raw=False,
        use_list=False,
        strict_map_key=True,
        max_array_len=256,
        max_map_len=0,
        max_str_len=4096,
        max_bin_len=journal.control_limits.max_record_bytes,
    )
    if type(cells) is not tuple or len(cells) != 10:
        raise FamilyBlockUnavailable("SOURCE journal vector descriptor differs")
    contract, root, identity, directory, name, stamp, digest, candidate, count, source_records = cells
    for source_values, length in ((identity, 4), (directory, 4), (stamp, 7)):
        if type(source_values) is not tuple or len(source_values) != length:
            raise FamilyBlockUnavailable("SOURCE journal custody tuple differs")
        for source_value in source_values:
            _integer(source_value, zero=True)
    for source_value in (candidate, count, source_records):
        _integer(source_value, zero=True)
    if (
        contract != _CONTRACT
        or root != str(journal.root)
        or identity != journal.identity
        or directory != _directory(journal.root / "vectors")
        or type(name) is not str
        or Path(name).name != name
        or name in ("", ".", "..")
        or type(digest) is not bytes
        or len(digest) != 32
        or candidate != family_id
        or count != len(packs)
        or source_records != sum(len(pack.records) for pack in packs)
        or checkpoint.commitments != (("allocation_vectors", digest), ("source_plan", plan.sha256))
        or stamp[4] > journal.work.limits.max_spool_bytes
    ):
        raise FamilyBlockUnavailable("SOURCE journal vector custody or input census differs")
    return cells


def _read_vector_file(journal, plan, ordinal, packs, validate, cells, expected):
    directory, name, stamp, digest, family_id, count, source_records = cells[3:]
    path = journal.root / "vectors" / name
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as vector_stream:
        if _stamp(path.lstat()) != stamp or _stamp(os.fstat(vector_stream.fileno())) != stamp:
            raise FamilyBlockUnavailable("SOURCE vector original file identity differs")
        if hashlib.file_digest(vector_stream, "sha256").digest() != digest:
            raise FamilyBlockUnavailable("SOURCE vector file SHA differs")
        vector_stream.seek(0)
        reader = _VectorFrames(journal)
        with closing(reader.read(vector_stream)) as entries:
            header = next(entries, None)
            if type(header) is not tuple or len(header) != 7:
                raise FamilyBlockUnavailable("SOURCE vector file header shape differs")
            for source_value in (header[2], header[3], header[5], header[6]):
                _integer(source_value, zero=True)
            if header != (_CONTRACT, plan.sha256, family_id, ordinal, expected, count, source_records):
                raise FamilyBlockUnavailable("SOURCE vector file header differs")
            source_rows = []
            for pack in packs:
                source_row = next(entries, None)
                validate(pack, source_row)
                source_rows.append(source_row)
            footer = next(entries, None)
            if type(footer) is not tuple or len(footer) != 3:
                raise FamilyBlockUnavailable("SOURCE vector footer shape differs")
            _integer(footer[1], zero=True)
            _integer(footer[2], zero=True)
            if footer != ("end", count, source_records) or next(entries, None) is not None:
                raise FamilyBlockUnavailable("SOURCE vector typed EOF differs")
        vector_stream.seek(0)
        if (
            hashlib.file_digest(vector_stream, "sha256").digest() != digest
            or _stamp(os.fstat(vector_stream.fileno())) != stamp
            or _stamp(path.lstat()) != stamp
            or _directory(journal.root / "vectors") != directory
        ):
            raise FamilyBlockUnavailable("SOURCE vector file changed during replay")
    journal.current()
    return tuple(source_rows)


def read_vectors(journal, record, plan, family_id, ordinal, packs, validate):
    """Use only a protected committed record; replay complete typed vector EOF."""
    journal.current()
    expected = input_sha256(plan, family_id, ordinal, packs)
    cells = _checked_vector_cells(journal, record, plan, family_id, ordinal, packs, expected)
    return _read_vector_file(journal, plan, ordinal, packs, validate, cells, expected)
