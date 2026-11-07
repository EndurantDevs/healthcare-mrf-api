# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Typed OUTPUT control and real owned-file accounting, without native authority.

Snapshot/profile/lock observations below are explicitly declared host doubles.
Producer cells stand for separately checked descriptors, never native commit or
recovery evidence. Filesystem custody/accounting cases use actual temporary files.
"""

import asyncio
import hashlib
import os
from dataclasses import astuple, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.custom_import import family_shard_output_checkpoint as output
from process.custom_import import family_shard_retained_checkpoint as retained
from process.custom_import import family_shard_retained_storage_validation as stored_validation
from process.custom_import import family_shard_retained_validation as original_validation
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_shard_checkpoint import OwnerCheckpointHead, OwnerCheckpointRecord
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded, SourceFactLimits
from process.custom_import.family_shard_output import _pack_row
from process.custom_import.family_shard_plan import PlanWork
from tests.test_custom_import_family_shard_cold_retained import _bundle as retained_bundle
from tests.test_custom_import_family_shard_cold_retained import _snapshot as retained_snapshot

_LIMITS = SourceFactLimits(16 * 1024**2, 65536, 65536)


def _cells(stage):
    """Closed control shapes only; these opaque producer cells grant no proof."""
    artifacts = tuple(
        ("declared-host-producer", index) if index < output._REQUIRED[stage] else None for index in range(6)
    )
    return (
        output._CONTRACT,
        b"r" * 32,
        retained._bundle(retained_bundle(len(retained._STAGES) - 1), _LIMITS),
        ("/declared-owner/output", (1, 2, 0o700, 501), astuple(_LIMITS)),
        (("admission",), ("events",), (1, 0), (1, 2, 1, 2), ("common",), ("events",), "graph"),
        artifacts,
        ((16 * 1024**2, 65536, 65536), 1024, ("declared-host-source-profile",)),
        ("/declared-owner", (1, 2)),
        stage,
    )


def _record(cells):
    payload = _pack_row(cells)
    digest = hashlib.sha256(payload).digest()
    stage = cells[-1] if type(cells[-1]) is int and cells[-1] >= 0 else 0
    ready = cells[1] if type(cells[1]) is bytes and len(cells[1]) == 32 else b"r" * 32
    return OwnerCheckpointRecord(
        "artifact", stage, digest, (("output_bundle", digest), ("source_ready", ready)), payload
    )


def _snapshot(records):
    """An explicit chain-reader double, not a database-protected native head."""
    rows = tuple(
        (OwnerCheckpointHead(index + 1, hashlib.sha256(str(index).encode() + record.payload).digest()), record)
        for index, record in reversed(tuple(enumerate(reversed(records))))
    )

    def read(*, check=None):
        for row in rows:
            if check is not None:
                check()
            yield row

    return SimpleNamespace(
        head=None if not rows else rows[0][0],
        limits=_LIMITS,
        root="/declared-owner/source/checkpoints",
        scope=("declared-host-native-scope",),
        records=read,
    )


def _chain(stage):
    _prefix, rows = retained_snapshot(len(retained._STAGES) - 1)
    return [_record(_cells(index)) for index in range(stage, -1, -1)] + [record for _head, record in rows]


@pytest.mark.parametrize("stage", range(len(output._STAGES)))
def test_output_suffix_preserves_original_prefix_and_full_head(stage):
    records = _chain(stage)
    snapshot = _snapshot(records)
    checks = []
    view = output.OriginalPrefix(snapshot)
    observed_records = tuple(view.records(check=lambda: checks.append(True)))
    assert view.snapshot is snapshot and view.head is snapshot.head and view.limits is snapshot.limits
    assert [record for _head, record in observed_records] == records[stage + 1 :]
    assert len(checks) == len(records)
    assert output.latest(snapshot, lambda: None) == _cells(stage)
    assert retained.latest(view, lambda: None) == _cells(stage)[2]


@pytest.mark.parametrize("fault", ("kind", "ordinal", "input_hash", "commitments"))
def test_output_bundle_authenticates_original_record_envelope(fault):
    record = _record(_cells(0))
    changes_by_fault = {
        "kind": {"kind": "source_allocation"},
        "ordinal": {"ordinal": 1},
        "input_hash": {"input_sha256": b"x" * 32},
        "commitments": {"commitments": (("output_bundle", b"x" * 32), ("source_ready", b"r" * 32))},
    }
    with pytest.raises(FamilyBlockUnavailable):
        output._bundle(replace(record, **changes_by_fault[fault]))


@pytest.mark.parametrize(
    "fault", ("contract", "length", "stage_bool", "stage_range", "tail", "source", "artifacts", "profile", "owner")
)
def test_output_bundle_refuses_malformed_typed_cells(fault):
    cells = list(_cells(0))
    if fault == "contract":
        cells[0] = "unsupported-output-contract"
    elif fault == "length":
        cells.pop(3)
    elif fault == "stage_bool":
        cells[8] = True
    elif fault == "stage_range":
        cells[8] = len(output._STAGES)
    else:
        index = {"tail": 2, "source": 4, "artifacts": 5, "profile": 6, "owner": 7}[fault]
        cells[index] = None
    with pytest.raises(FamilyBlockUnavailable):
        output._bundle(_record(tuple(cells)))


@pytest.mark.parametrize(
    "stage,fault",
    [(stage, "missing") for stage, required in enumerate(output._REQUIRED) if required]
    + [(stage, "premature") for stage, required in enumerate(output._REQUIRED) if required < 6],
)
def test_output_bundle_requires_only_the_exact_completed_stage_fields(stage, fault):
    cells = list(_cells(stage))
    artifacts = list(cells[5])
    required = output._REQUIRED[stage]
    if fault == "missing":
        artifacts[required - 1] = None
    else:
        artifacts[required] = ("uncompleted-host-descriptor",)
    cells[5] = tuple(artifacts)
    with pytest.raises(FamilyBlockUnavailable):
        output._bundle(_record(tuple(cells)))


@pytest.mark.parametrize(
    "fault",
    (
        "duplicate",
        "gap",
        "order",
        "prefix_interruption",
        "missing_inputs",
        "missing_prefix",
        "completed_field",
        "original_scope",
    ),
)
def test_output_suffix_rejects_incomplete_or_rebound_history(fault):
    records = _chain(5)
    if fault == "duplicate":
        records.insert(3, records[3])
    if fault == "gap":
        records.pop(3)
    if fault == "order":
        records[2], records[3] = records[3], records[2]
    if fault == "prefix_interruption":
        records.insert(2, records[-1])
    if fault == "missing_inputs":
        records.pop(5)
    if fault == "missing_prefix":
        records = records[:6]
    if fault in {"completed_field", "original_scope"}:
        cells = list(_cells(5))
        if fault == "completed_field":
            artifacts = list(cells[5])
            artifacts[0] = ("changed-completed-generation",)
            cells[5] = tuple(artifacts)
        else:
            cells[7] = ("/different-owner", (1, 2))
        records[0] = _record(tuple(cells))
    snapshot = _snapshot(records)
    with pytest.raises(FamilyBlockUnavailable):
        tuple(output.OriginalPrefix(snapshot).records())
    with pytest.raises(FamilyBlockUnavailable):
        output.latest(snapshot, lambda: None)


def test_unknown_artifacts_remain_visible_to_original_prefix_validator():
    unknown = OwnerCheckpointRecord("artifact", 0, b"u" * 32, (("unknown_artifact", b"u" * 32),), b"opaque")
    records = _chain(0)
    records.insert(1, unknown)
    prefix_records = [record for _head, record in output.OriginalPrefix(_snapshot(records)).records()]
    assert prefix_records[0] is unknown and prefix_records[1:] == records[2:]
    assert output.latest(_snapshot([unknown, records[-1]]), lambda: None) is None


@pytest.mark.parametrize("fault", ("ready", "homes", "boolean_count"))
def test_bundle_keeps_exact_retained_ready_homes_and_source_outcome_join(fault):
    cells = list(_cells(0))
    if fault == "ready":
        cells[1] = b"x" * 32
    else:
        source_cells = list(cells[4])
        source_cells[3 if fault == "homes" else 2] = (1, 2, 0, 2) if fault == "homes" else (True, 0)
        cells[4] = tuple(source_cells)
    with pytest.raises(FamilyBlockUnavailable):
        output._bundle(_record(tuple(cells)))


def _owner(tmp_path):
    root = tmp_path / "output"
    root.mkdir(mode=0o700)
    checks, registered_roots = [], []
    identity = root.stat()

    def current():
        checks.append("current")

    owner = SimpleNamespace(
        root=root,
        limits=_LIMITS,
        current=current,
        _root_identity=(identity.st_dev, identity.st_ino),
        source=SimpleNamespace(
            owner_profile=("declared-host-owner-profile",),
            owner_lock=SimpleNamespace(check=lambda: checks.append("lock")),
        ),
        work=PlanWork(root, _LIMITS, registered_roots.append, current),
    )
    return owner, checks, registered_roots


def _persistent_files(owner):
    members = owner.root / "members"
    members.mkdir(mode=0o700)
    member_file = members / "output.mpk"
    member_file.write_bytes(b"closed-member-file")
    member_file.chmod(0o400)
    failed = owner.root / ("proof-" + "a" * 32)
    failed.mkdir(mode=0o700)
    verification = failed / "verification"
    verification.mkdir(mode=0o700)
    incomplete = verification / "partial.mpk"
    incomplete.write_bytes(b"failed-proof" * 5000)
    return member_file, incomplete


def _completion_job(owner):
    """Keep only finality transport and stage selection as declared host doubles."""
    owner.inputs = object()
    return SimpleNamespace(
        counts_unavailable=False,
        source_counts=(),
        is_rejected=False,
        output=None,
        retained=SimpleNamespace(context=object()),
        output_stage=owner,
        source_limits=SimpleNamespace(allocation=object()),
        reader_activation=None,
        root=owner.root.parent,
        work=owner.work,
        current=owner.current,
    )


def _empty_output_inventory(owner):
    """Run the real strict census without admitting any temporary proof parent."""
    from process.custom_import import family_shard_retirement_files as inventory
    from process.custom_import.family_shard_retirement_policy import RetiredFilePolicy

    root = owner.root.parent
    policy = RetiredFilePolicy(root)
    policy.add(owner.root, identity=inventory._directory(owner.root))
    context = SimpleNamespace(
        root=root,
        root_identity=inventory._directory(root),
        lock_identity=owner.source.owner_lock.file_identity,
        maximum_bytes=_LIMITS.max_spool_bytes,
    )
    return inventory._census(context, policy, owner.source.owner_lock.check)


@pytest.mark.asyncio
@pytest.mark.parametrize("failed", [False, True], ids=["completed", "interrupted"])
async def test_completion_removes_empty_proof_parent_before_strict_terminal_census(tmp_path, monkeypatch, failed):
    from process.custom_import import family_shard_owner_job as owners
    from process.custom_import.family_shard_facts import _Work
    from process.custom_import.family_shard_owner_lock import acquire_owner_job_lock

    owner, _checks, _registered = _owner(tmp_path)
    identity = tmp_path.stat()
    with acquire_owner_job_lock(tmp_path, identity=(identity.st_dev, identity.st_ino)) as lock:
        owner.source.owner_lock = lock
        job = _completion_job(owner)
        failure = RuntimeError("synthetic finality interruption")

        async def complete(_factory, _context, _inputs, *, work, **_keywords):
            for name in ("verification", "finality"):
                scratch = _Work(work.root / name, work.limits, work.register_cleanup)
                scratch.write(((name,),))
                scratch.close()
            if failed:
                raise failure
            return SimpleNamespace(no_change=object())

        monkeypatch.setattr(owners, "ShardOutputStage", SimpleNamespace)
        monkeypatch.setattr(owners, "complete_closed_shard_build", complete)
        if failed:
            with pytest.raises(RuntimeError) as observed:
                await owners._finish(None, job)
            assert observed.value is failure
        else:
            await owners._finish(None, job)
            assert job.complete and job.publication is job.output.no_change
        entries = _empty_output_inventory(owner)
        assert {entry[0] for entry in entries} == {".owner-job.lock", "output"}


def test_remaining_charges_real_persistent_and_failed_proof_allocations(tmp_path):
    owner, _checks, _registered = _owner(tmp_path)
    empty = output.remaining(owner)
    files = _persistent_files(owner)
    remaining = output.remaining(owner)
    filesystem = os.statvfs(owner.root)
    unit = max(512, filesystem.f_frsize or filesystem.f_bsize)
    minimum_file_charge = sum(max(path.stat().st_size, path.stat().st_blocks * 512) + unit for path in files)
    assert empty.max_spool_bytes - remaining.max_spool_bytes >= minimum_file_charge + 3 * unit
    assert (remaining.max_record_bytes, remaining.memory_bytes) == (_LIMITS.max_record_bytes, _LIMITS.memory_bytes)
    assert all(path.stat().st_uid == os.getuid() for path in files)


def test_members_work_charges_live_selected_tree_and_refuses_unknown_sibling(tmp_path):
    from process.custom_import.family_shard_output_stage import _work

    owner, _checks, _registered = _owner(tmp_path)
    originals = _persistent_files(owner)
    before = output.remaining(owner)
    selected = owner.root / "selected"
    selected.mkdir(mode=0o700)
    nested = selected / "sort"
    nested.mkdir(mode=0o700)
    partial = nested / "selected.mpk"
    partial.write_bytes(b"selected-original" * 8192)
    original_bytes_by_key = {path: path.read_bytes() for path in (*originals, partial)}

    members = _work(owner, "members")
    filesystem = os.statvfs(owner.root)
    unit = max(512, filesystem.f_frsize or filesystem.f_bsize)
    minimum_charge = max(partial.stat().st_size, partial.stat().st_blocks * 512) + 3 * unit
    assert before.max_spool_bytes - members.limits.max_spool_bytes >= minimum_charge
    assert (members.limits.max_record_bytes, members.limits.memory_bytes) == (
        _LIMITS.max_record_bytes,
        _LIMITS.memory_bytes,
    )

    (owner.root / "selected-other").mkdir(mode=0o700)
    with pytest.raises(FamilyBlockUnavailable, match="unknown producer or failed attempt"):
        _work(owner, "members")
    assert {path: path.read_bytes() for path in original_bytes_by_key} == original_bytes_by_key


def test_low_slack_refuses_new_proof_before_registration_or_creation(tmp_path):
    owner, _checks, registered_roots = _owner(tmp_path)
    files = _persistent_files(owner)
    prior_by_path = {str(path): path.read_bytes() for path in files}
    charged = owner.limits.max_spool_bytes - output.remaining(owner).max_spool_bytes
    threshold = max(owner.limits.memory_bytes, owner.limits.max_record_bytes)
    owner.limits = replace(owner.limits, max_spool_bytes=charged + threshold - 1)
    with pytest.raises(SourceFactBudgetExceeded), output.proof_work(owner):
        pytest.fail("exhausted proof work must not begin")
    assert registered_roots == [] and {str(path): path.read_bytes() for path in files} == prior_by_path
    owner.limits = replace(owner.limits, max_spool_bytes=charged + threshold)
    assert output.remaining(owner).max_spool_bytes == threshold


def test_proof_work_registers_fresh_root_after_charging_prior_attempts(tmp_path):
    owner, checks, registered_roots = _owner(tmp_path)
    files = _persistent_files(owner)
    before_by_path = {str(path): path.read_bytes() for path in files}
    expected = output.remaining(owner)
    registrations = []

    def register(path):
        registrations.append((path, path.exists()))
        registered_roots.append(path)

    owner.work = replace(owner.work, register_cleanup=register)
    with output.proof_work(owner) as first, output.proof_work(owner) as second:
        assert first.root != second.root and first.root.parent == second.root.parent == owner.root
        assert first.limits == expected and second.limits.max_spool_bytes < first.limits.max_spool_bytes
        assert registrations == [(first.root, False), (second.root, False)]
        assert all(path.is_dir() and path.stat().st_mode & 0o777 == 0o700 for path in registered_roots)
    assert not first.root.exists() and not second.root.exists()
    assert {str(path): path.read_bytes() for path in files} == before_by_path
    output.remove_root(owner)
    assert "lock" in checks and not owner.root.exists()


@pytest.mark.parametrize("failure", [RuntimeError("proof failed"), asyncio.CancelledError("proof canceled")])
def test_interrupted_proof_removes_only_its_empty_parent(tmp_path, failure):
    owner, _checks, _registered = _owner(tmp_path)
    originals = _persistent_files(owner)
    original_bytes_by_path = {path: path.read_bytes() for path in originals}
    with pytest.raises(type(failure)) as observed, output.proof_work(owner) as work:
        raise failure
    assert observed.value is failure and not work.root.exists()
    assert {path: path.read_bytes() for path in originals} == original_bytes_by_path


@pytest.mark.parametrize("fault", ["nonempty", "replaced_proof", "replaced_output", "missing_proof"])
def test_proof_cleanup_refuses_unknown_or_replaced_paths(tmp_path, fault):
    owner, _checks, _registered = _owner(tmp_path)
    with pytest.raises((OSError, FamilyBlockUnavailable)), output.proof_work(owner) as work:
        if fault == "nonempty":
            original = work.root / "unknown.mpk"
            original.write_bytes(b"preserved failed proof")
        elif fault == "replaced_proof":
            original = work.root.with_name("detached-proof")
            work.root.rename(original)
            work.root.mkdir(mode=0o700)
        elif fault == "replaced_output":
            original = owner.root.with_name("detached-output")
            owner.root.rename(original)
            owner.root.mkdir(mode=0o700)
            work.root.mkdir(mode=0o700)
        else:
            work.root.rmdir()
    if fault != "missing_proof":
        assert original.exists() and work.root.exists()
    if fault == "nonempty":
        assert original.read_bytes() == b"preserved failed proof"


def test_proof_cleanup_preserves_primary_failure_and_nonempty_evidence(tmp_path):
    owner, _checks, _registered = _owner(tmp_path)
    failure = RuntimeError("original proof failure")
    with pytest.raises(RuntimeError) as observed, output.proof_work(owner) as work:
        evidence = work.root / "partial.mpk"
        evidence.write_bytes(b"preserved partial proof")
        raise failure
    assert observed.value is failure and isinstance(failure.__cause__, OSError)
    assert failure._custom_import_retry_blocked
    assert evidence.read_bytes() == b"preserved partial proof"


def test_proof_cleanup_requires_original_lifetime_lock(tmp_path):
    owner, checks, _registered = _owner(tmp_path)

    def stopped():
        raise FamilyBlockUnavailable("original lifetime lock changed")

    with pytest.raises(FamilyBlockUnavailable, match="lifetime lock"), output.proof_work(owner) as work:
        owner.source.owner_lock.check = stopped
    assert work.root.is_dir() and "current" in checks


@pytest.mark.parametrize(
    "stage,configured", [(stage, False) for stage in range(4)] + [(4, False), (4, True), (5, False), (5, True)]
)
def test_committed_stage_requires_exact_original_producer_children(tmp_path, stage, configured):
    owner, _checks, _registered = _owner(tmp_path)
    cells = list(_cells(stage))
    if stage >= 4:
        artifacts = list(cells[5])
        artifacts[5] = (True, ("host-projection",)) if configured else (False,)
        cells[5] = tuple(artifacts)
    expected_names = set()
    if stage >= 2:
        expected_names.add("source-families")
    if stage >= 3:
        expected_names.update(("members", "members-winners"))
    if configured:
        expected_names.add("projection")
    for name in expected_names | {"proof-" + "a" * 32, "source-replay-" + "b" * 32}:
        (owner.root / name).mkdir(mode=0o700)
    output.check_stage_children(owner, tuple(cells))
    for name in expected_names:
        (owner.root / name).rmdir()
        with pytest.raises(FamilyBlockUnavailable):
            output.check_stage_children(owner, tuple(cells))
        (owner.root / name).mkdir(mode=0o700)


def test_unanchored_member_return_refuses_before_any_replacement_work(tmp_path):
    owner, _checks, registered_roots = _owner(tmp_path)
    for name in ("source-families", "members", "members-winners"):
        directory = owner.root / name
        directory.mkdir(mode=0o700)
        (directory / "original.mpk").write_bytes(b"unanchored-original")
    with pytest.raises(FamilyBlockUnavailable, match="exact committed custody"):
        output.check_stage_children(owner, _cells(2))
    assert registered_roots == [] and all(
        (owner.root / name / "original.mpk").read_bytes() == b"unanchored-original"
        for name in ("members", "members-winners")
    )


def _damage_output_shape(owner, tmp_path, outside, fault):
    if fault == "unknown":
        (owner.root / "unexpected").mkdir(mode=0o700)
        return
    if fault == "root_symlink":
        original = owner.root.with_name("original-output")
        owner.root.rename(original)
        owner.root.symlink_to(original, target_is_directory=True)
        return
    if fault == "root_inode":
        owner.root.rename(owner.root.with_name("original-output"))
        owner.root.mkdir(mode=0o700)
        return
    if fault == "root_mode":
        owner.root.chmod(0o755)
        return
    if fault == "file_as_child":
        (owner.root / "members").write_bytes(b"not-a-directory")
        return
    members = owner.root / "members"
    if fault == "child_symlink":
        members.symlink_to(tmp_path, target_is_directory=True)
        return
    members.mkdir(mode=0o755 if fault == "child_mode" else 0o700)
    output_file = members / "output.mpk"
    if fault == "file_symlink":
        output_file.symlink_to(outside)
    elif fault == "file_hardlink":
        output_file.hardlink_to(outside)
    else:
        output_file.write_bytes(b"original")


@pytest.mark.parametrize("operation", (output.remaining, output.remove_root), ids=("remaining", "remove_root"))
@pytest.mark.parametrize(
    "fault",
    (
        "unknown",
        "root_symlink",
        "root_inode",
        "root_mode",
        "child_symlink",
        "file_symlink",
        "file_hardlink",
        "child_mode",
        "file_as_child",
    ),
)
def test_output_census_refuses_changed_or_unowned_shape_without_removing_files(tmp_path, operation, fault):
    owner, _checks, _registered = _owner(tmp_path)
    outside = tmp_path / "preserve"
    outside.write_bytes(b"outside-original")
    _damage_output_shape(owner, tmp_path, outside, fault)
    with pytest.raises((FamilyBlockUnavailable, ValueError)):
        operation(owner)
    assert owner.root.exists() and outside.read_bytes() == b"outside-original"


def test_terminal_removal_requires_original_live_lock_before_census(tmp_path):
    owner, _checks, _registered = _owner(tmp_path)
    files = _persistent_files(owner)
    before_by_path = {str(path): path.read_bytes() for path in files}

    def lost():
        raise FamilyBlockUnavailable("declared host lock lost")

    owner.source.owner_lock.check = lost
    with pytest.raises(FamilyBlockUnavailable, match="lock lost"):
        output.remove_root(owner)
    assert {str(path): path.read_bytes() for path in files} == before_by_path


def test_terminal_real_root_cleanup_keeps_live_lock_after_processing_expiry(tmp_path):
    owner, checks, _registered = _owner(tmp_path)
    _persistent_files(owner)

    def expired():
        raise TimeoutError("processing deadline elapsed")

    owner.current = expired
    output.remove_root(owner)
    assert "lock" in checks and not owner.root.exists()


@pytest.mark.parametrize("operation", (output.remaining, output.remove_root), ids=("remaining", "remove_root"))
@pytest.mark.parametrize("location", ("root", "file"))
def test_actual_owned_metadata_is_rejected_under_wrong_host_uid_view(tmp_path, monkeypatch, operation, location):
    """An explicit effective-UID guard double; no filesystem ownership is changed."""
    owner, _checks, _registered = _owner(tmp_path)
    members = owner.root / "members"
    members.mkdir(mode=0o700)
    original = members / "output.mpk"
    original.write_bytes(b"original-member")
    real_uid = os.getuid()
    calls = []

    def wrong_uid():
        calls.append(True)
        return real_uid + 1 if location == "root" or len(calls) >= 4 else real_uid

    monkeypatch.setattr(output.os, "getuid", wrong_uid)
    with pytest.raises((ValueError, FamilyBlockUnavailable)):
        operation(owner)
    assert original.read_bytes() == b"original-member" and original.stat().st_uid == real_uid
    if location == "file":
        assert len(calls) >= 4


def _retained_proof_cells(fault):

    counts = (("family", 1), ("root", 1), ("child", 1), ("scalar", 2), ("pack", 1), ("origin", 2))
    original = original_validation.RetainedOriginalComparison(("scope",), b"d" * 32, counts, 1, 1)
    stored = stored_validation.RetainedStoredComparison(
        original.scope, original.destination_sha256, b"i" * 32, ("output",), counts, 1, 1, 1, 1, b"s" * 32
    )
    expected_original, expected_stored = original, stored
    if fault == "original-digest":
        expected_original = replace(original, destination_sha256=b"x" * 32)
    elif fault == "original-counts":
        expected_original = replace(original, root_scalar_count=2)
    elif fault == "stored-digest":
        expected_stored = replace(stored, sha256=b"x" * 32)
    artifacts = [None] * 6
    artifacts[3], artifacts[5] = retained._init_cells(expected_original), retained._init_cells(expected_stored)
    cells = (None, None, (None, None, None, None, None, tuple(artifacts)))
    return original, stored, cells


@pytest.mark.parametrize("fault", (None, "original-digest", "original-counts", "stored-digest"))
async def test_reopened_retained_proofs_compare_every_persisted_constructor_field(tmp_path, monkeypatch, fault):
    from process.custom_import.family_shard_allocate import SourceAllocationLimits

    original, stored, cells = _retained_proof_cells(fault)
    producer = SimpleNamespace(context=object(), destination=object(), encoded=object(), original=None, stored=None)

    def query_keys():
        return iter(())

    owner = SimpleNamespace(
        root=tmp_path,
        work=SimpleNamespace(register_cleanup=lambda _path: None),
        current=lambda: None,
        source=SimpleNamespace(
            limits=SimpleNamespace(allocation=SourceAllocationLimits(_LIMITS, 2, 65536)),
            metadata=SimpleNamespace(query_keys=query_keys),
        ),
        retained=producer,
    )
    verify_original, verify_stored = AsyncMock(return_value=original), AsyncMock(return_value=stored)
    monkeypatch.setattr(output, "remaining", lambda _owner: _LIMITS)
    monkeypatch.setattr(original_validation, "verify_retained_destination", verify_original)
    monkeypatch.setattr(stored_validation, "prove_retained_output", verify_stored)

    if fault is None:
        await output._recheck_retained(owner, cells)
        assert producer.original is original and producer.stored is stored
    else:
        label = "storage" if fault == "stored-digest" else "original"
        with pytest.raises(FamilyBlockUnavailable, match=f"retained {label} comparison changed"):
            await output._recheck_retained(owner, cells)
        assert producer.original is producer.stored is None
    verify_original.assert_awaited_once()
    assert verify_original.await_args.args == (producer.context, producer.destination)
    if fault in ("original-digest", "original-counts"):
        verify_stored.assert_not_awaited()
    else:
        verify_stored.assert_awaited_once()
        assert verify_stored.await_args.args == (
            producer.context,
            producer.destination,
            original,
            producer.encoded,
            query_keys,
        )
