# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Complete SOURCE allocation coordinates, bounded authority and failure cleanup."""

import asyncio
import hashlib
import os
import pickle
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from process.custom_import import family_shard_allocate as allocation
from process.custom_import import family_shard_checkpoint as checkpoint
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded, SourceFactLimits
from process.custom_import.family_shard_input import cleanup_spool
from process.custom_import.family_shard_plan import PlanWork
from process.custom_import.family_shard_source_journal import create_source_allocation_journal
from process.custom_import.family_shard_source_plan import cleanup_source_plan, read_source_plan
from tests.test_custom_import_family_shard_source_plan import _build, _case, _child, _root

pytestmark = pytest.mark.asyncio


class _Boundary(list):
    pass


def _install_allocation_pages(monkeypatch, events, plan):
    root_ids_by_key = {}

    @asynccontextmanager
    async def page(factory, request, build_id):
        assert type(build_id) is int
        if events.build_id is None:
            events.build_id = build_id
        assert build_id == events.build_id
        events.append("authority")
        session = SimpleNamespace(
            descriptor=SimpleNamespace(**events.committed), roots=dict(root_ids_by_key), writing=False
        )
        yield session, SimpleNamespace(build_id=build_id, phase=events.phase)
        if not session.writing and events.fail_read:
            events.fail_read = False
            raise ConnectionError("synthetic reconciliation read unavailable")
        if session.writing and events.failure == "before":
            raise RuntimeError("synthetic changed lease before commit")
        events.committed = vars(session.descriptor).copy()
        root_ids_by_key.update(session.roots)
        events.append("commit")
        if session.writing and events.failure in {"after", "unknown"}:
            events.fail_read = events.failure == "unknown"
            events.failure = None
            raise ConnectionError("synthetic committed allocation reply lost")

    async def native_scope(session, request, build_id, family_id, layout, *, writing):
        allocation._checked_source(request, plan)
        assert build_id == events.build_id and family_id == 17
        session.writing = session.writing or writing
        if writing:
            events.append("candidate")
        return session.descriptor, (build_id, family_id, plan.sha256, plan.scope)

    @asynccontextmanager
    async def window(factory, request, family_id, layout, journal, transition):
        async with page(factory, request, journal.build_id) as (session, build):
            if build.phase != "source":
                raise FamilyBlockUnavailable("SOURCE allocation requires its current SOURCE build phase")
            descriptor, scope = await checkpoint._native_scope(
                session, request, journal.build_id, family_id, layout, writing=True
            )
            yield session, descriptor, scope
            descriptor.owner_checkpoint_revision = transition.values["owner_checkpoint_revision"]
            descriptor.owner_checkpoint_sha256 = transition.values["owner_checkpoint_sha256"]

    monkeypatch.setattr(allocation, "_source_window_session", window)
    monkeypatch.setattr(allocation, "_page_session", page)
    monkeypatch.setattr(checkpoint, "_page_session", page)
    monkeypatch.setattr(checkpoint, "_native_scope", native_scope)
    return root_ids_by_key


def _install_allocation_ids(monkeypatch, events):
    next_id_by_kind = {}

    async def execute(*args):
        return SimpleNamespace(one=lambda: events.capture_scope)

    async def roots(session, definition, dataset, keys, **kwargs):
        return tuple(session.roots.setdefault(key, 1001 + 17 * len(session.roots)) for key in keys)

    async def allocate(session, kinds):
        events.allocations.append(tuple(kinds))
        native_ids = []
        for kind in kinds:
            next_id_by_kind[kind] = next_id_by_kind.get(kind, 40) + 17
            native_ids.append(next_id_by_kind[kind])
        return tuple(native_ids)

    async def sequences(session, kinds):
        return {"source_root": 10, **{kind: index + 20 for index, kind in enumerate(kinds)}}

    async def bound_ids(session, kinds, sequences):
        return await allocate(session, kinds)

    monkeypatch.setattr(allocation, "source_sequence_bindings", sequences)
    monkeypatch.setattr(allocation, "allocate_bound_ids", bound_ids)
    monkeypatch.setattr(allocation, "_execute", execute)
    monkeypatch.setattr(allocation, "intern_root_records", roots)
    monkeypatch.setattr(allocation, "_allocate_record_ids", allocate)


def _native_boundary(monkeypatch, plan, *, lost_commit=False):
    events = _Boundary()
    events.committed = dict(owner_checkpoint_revision=None, owner_checkpoint_sha256=None)
    events.failure = "before" if lost_commit else None
    events.fail_read = False
    events.allocations = []
    events.build_id = None
    events.phase = "source"
    capture_by_field = dict(plan.scope[0])
    events.capture_scope = (
        capture_by_field["capture_bundle_id"],
        capture_by_field["source_binding_revision_id"],
        capture_by_field["source_binding_revision_id"],
        bytes.fromhex(capture_by_field["manifest_sha256"]),
        None
        if capture_by_field["source_binding_sha256"] is None
        else bytes.fromhex(capture_by_field["source_binding_sha256"]),
    )

    _install_allocation_pages(monkeypatch, events, plan)
    _install_allocation_ids(monkeypatch, events)
    return events


def _journal(tmp_path, register=None, *, facts=None, control=None):
    facts = facts or SourceFactLimits(16 * 1024**2, 1024**2, 4096)
    return create_source_allocation_journal(
        23,
        work=PlanWork(tmp_path / "allocated", facts, register or (lambda _path: None)),
        control_limits=control or SourceFactLimits(16 * 1024**2, 1024**2, 256 * 1024),
    )


def _journal_for_plan(tmp_path, plan, maximum=32, register=None):
    batches = tuple(allocation._pack_batches(plan, maximum, 1024**2))
    charges = tuple(
        max(
            sum(pack.landing.byte_count + 512 * len(pack.records) for pack in packs),
            sum(
                len(record.typed_parent_key[0].encode("utf-8")) + 56
                for pack in packs
                for record in pack.records
                if record.typed_parent_key is not None
            )
            + 64 * sum(1 + len(pack.records) for pack in packs),
        )
        for packs in batches
    )
    memory = max(4096, 4 * min(1024**2, sum(charges)))
    return _journal(tmp_path, register, facts=SourceFactLimits(16 * 1024**2, 1024**2, memory))


async def _allocate(tmp_path, request, plan, *, maximum=32, register=None, journal=None):
    allocation._checked_source(request, plan)
    journal = journal or _journal_for_plan(tmp_path, plan, maximum, register)
    return await allocation.allocate_source_plan(
        None,
        request,
        17,
        None,
        plan,
        journal=journal,
        limits=allocation.SourceAllocationLimits(journal.work.limits, maximum, 1024**2),
    )


async def test_source_pages_share_authority_and_preserve_native_ids(tmp_path, monkeypatch):
    case = await _case(
        tmp_path,
        {
            "root_source": [[_root(), _root(), _root(npi=None), _root("1234567893")]],
            "detail_source": [[_child(), _child(), _child("orphan-parent"), _child(key=None)]],
        },
        row_limit=2,
    )
    plan = _build(tmp_path, case)
    events = _native_boundary(monkeypatch, plan)
    try:
        registered_paths = []
        allocated = await _allocate(tmp_path, case[1], plan, register=registered_paths.append)
        try:
            assert registered_paths[0] == tmp_path / "allocated" and Path(allocated.root) in registered_paths
            assert events.count("candidate") == 1 and len(events.allocations) == 1
            assert pickle.loads(pickle.dumps(allocated)) == allocated
            allocated_packs = list(allocation.read_source_allocations(allocated))
            assert len(allocated_packs) == plan.counts[1] == 4
            expected_packs = list(read_source_plan(plan))
            assert [allocated_pack[0] for allocated_pack in allocated_packs] == expected_packs
            assert [allocated_pack[1] for allocated_pack in allocated_packs] == [57, 74, 91, 108]
            by_kind = {1: [], 2: [], None: []}
            for pack, pack_id, roots, revisions, rejections in allocated_packs:
                for index, kind in enumerate(pack.allocation_kinds):
                    by_kind[kind].append((rejections if kind is None else revisions)[index])
                    assert (roots[index] is None) == (pack.records[index].typed_parent_key is None)
                    assert (revisions[index] is None) == (kind is None)
                    assert (rejections[index] is not None) == (kind is None)
            assert {kind: len(native_ids) for kind, native_ids in by_kind.items()} == {1: 3, 2: 3, None: 2}
            assert all(
                native_ids == [57 + 17 * index for index in range(len(native_ids))] for native_ids in by_kind.values()
            )
        finally:
            allocation.cleanup_source_allocations(allocated)
    finally:
        cleanup_source_plan(plan)
        cleanup_spool(case[0])


async def test_empty_complete_source_still_checks_current_owner_authority(tmp_path, monkeypatch):
    case = await _case(tmp_path, {"root_source": [[]], "detail_source": [[]]})
    plan = _build(tmp_path, case)
    events = _native_boundary(monkeypatch, plan)
    try:
        allocated = await _allocate(tmp_path, case[1], plan)
        try:
            assert events.count("candidate") == 1 and events.allocations == []
            assert list(allocation.read_source_allocations(allocated)) == []
        finally:
            allocation.cleanup_source_allocations(allocated)
    finally:
        cleanup_source_plan(plan)
        cleanup_spool(case[0])


@pytest.mark.parametrize("stop_pass", [1, 2])
async def test_original_allocation_hash_passes_honor_stop_and_preserve_input(tmp_path, monkeypatch, stop_pass):
    case = await _case(tmp_path)
    plan = _build(tmp_path, case)
    _native_boundary(monkeypatch, plan)
    allocated = await _allocate(tmp_path, case[1], plan)
    expected_packs = list(allocation.read_source_allocations(allocated))
    original, scans, seen = allocation._plan_digest, [], []
    failure = InterruptedError("synthetic original allocation hash stopped")

    def digest(source_file, check):
        scans.append(source_file)
        return original(source_file, check)

    def current():
        if len(scans) == stop_pass and scans[-1].tell() > 0:
            raise failure

    monkeypatch.setattr(allocation, "_plan_digest", digest)
    try:
        with pytest.raises(InterruptedError, match="original allocation hash stopped") as raised:
            for pack in allocation.read_source_allocations(allocated, check=current):
                seen.append(pack)
        assert raised.value is failure and all(source_file.closed for source_file in scans)
        assert seen == ([] if stop_pass == 1 else expected_packs)
        assert list(allocation.read_source_allocations(allocated)) == expected_packs
        with pytest.raises(ValueError, match="cancellation check"):
            list(allocation.read_source_allocations(allocated, check=False))
    finally:
        allocation.cleanup_source_allocations(allocated)
        cleanup_source_plan(plan)
        cleanup_spool(case[0])


@pytest.mark.parametrize("failure", ["commit", "late_source", "changed_bytes", "changed_scope"])
async def test_incomplete_or_changed_allocation_never_becomes_worker_input(tmp_path, monkeypatch, failure):
    case = await _case(tmp_path, {"root_source": [[_root()] * 4], "detail_source": [[_child()] * 3]})
    plan = _build(tmp_path, case)
    _native_boundary(monkeypatch, plan, lost_commit=failure == "commit")
    journal = _journal_for_plan(tmp_path, plan, 2) if failure == "late_source" else None
    if failure == "late_source":

        def interrupted(plan, **kwargs):
            for index, pack in enumerate(read_source_plan(plan)):
                if index == 2:
                    raise RuntimeError("synthetic late source loss")
                yield pack

        monkeypatch.setattr(allocation, "read_source_plan", interrupted)
    allocated = None
    try:
        if failure in ("commit", "late_source"):
            with pytest.raises(RuntimeError):
                await _allocate(tmp_path, case[1], plan, maximum=2, journal=journal)
            assert (tmp_path / "allocated").exists()
            assert (tmp_path / "allocated" / "vectors").is_dir()
        else:
            allocated = await _allocate(tmp_path, case[1], plan)
            if failure == "changed_bytes":
                path = Path(allocated.root) / "source-ids.mpk"
                path.chmod(0o600)
                original_bytes = path.read_bytes()
                path.write_bytes(original_bytes[:-1] + bytes((original_bytes[-1] ^ 1,)))
                path.chmod(0o400)
            else:
                allocated = replace(allocated, snapshot_family_id=19)
            with pytest.raises(ValueError):
                list(allocation.read_source_allocations(allocated))
    finally:
        if allocated is not None:
            allocation.cleanup_source_allocations(allocated)
        cleanup_source_plan(plan)
        cleanup_spool(case[0])


async def test_wrong_producer_scope_is_refused_before_creating_resources(tmp_path):
    case = await _case(tmp_path)
    plan = _build(tmp_path, case)
    try:
        with pytest.raises(FamilyBlockUnavailable, match="scope"):
            await _allocate(tmp_path, replace(case[1], fence=case[1].fence + 1), plan)
        assert not (tmp_path / "allocated").exists()
    finally:
        cleanup_source_plan(plan)
        cleanup_spool(case[0])


async def _recovery_case(tmp_path, monkeypatch, *, windowed=False):
    case = await _case(tmp_path, {"root_source": [[_root()] * 4], "detail_source": [[_child()] * 3]})
    plan = _build(tmp_path, case)
    boundary = _native_boundary(monkeypatch, plan)
    if not windowed:
        # Retain single-batch loss boundaries, while separate tests exercise the
        # production multi-record window and atomic suffix reconciliation.
        monkeypatch.setattr(
            allocation, "_allocation_windows", lambda batches, limits, check: ((batch,) for batch in batches)
        )
    return case, plan, boundary, _journal_for_plan(tmp_path, plan, 2)


def _close_recovery(case, plan, journal, allocated=None):
    if allocated is not None:
        allocation.cleanup_source_allocations(allocated)
    journal.close_terminal()
    cleanup_source_plan(plan)
    cleanup_spool(case[0])


async def test_committed_lost_reply_returns_exact_vectors_without_reallocating(tmp_path, monkeypatch):
    case, plan, boundary, journal = await _recovery_case(tmp_path, monkeypatch)
    boundary.failure = "after"
    allocated = None
    try:
        allocated = await _allocate(tmp_path, case[1], plan, maximum=2, journal=journal)
        rows = list(allocation.read_source_allocations(allocated))
        assert [row[1] for row in rows] == [57, 74, 91, 108]
        assert len(boundary.allocations) == 4
        assert boundary.committed["owner_checkpoint_revision"] == 4
        assert len(tuple((journal.root / "vectors").iterdir())) == 4
    finally:
        _close_recovery(case, plan, journal, allocated)


@pytest.mark.parametrize("reopen_owner", (False, True))
async def test_unknown_commit_keeps_files_then_reuses_freshly_protected_prefix(tmp_path, monkeypatch, reopen_owner):
    case, plan, boundary, journal = await _recovery_case(tmp_path, monkeypatch)
    boundary.failure = "unknown"
    allocated = None
    try:
        with pytest.raises(FamilyBlockUnavailable, match="commit is unknown"):
            await _allocate(tmp_path, case[1], plan, maximum=2, journal=journal)
        assert not journal.busy and len(boundary.allocations) == 1
        assert boundary.committed["owner_checkpoint_revision"] == 1
        old_outputs = tuple((journal.root / "outputs").iterdir())
        old_vectors_by_key = {path.name: path.read_bytes() for path in (journal.root / "vectors").iterdir()}
        if reopen_owner:
            # No pending ID vector, transaction or result cache is transferred.
            journal = allocation.SourceAllocationJournal(
                journal.build_id, journal.work, journal.control_limits, journal.identity
            )
        allocated = await _allocate(tmp_path, case[1], plan, maximum=2, journal=journal)
        assert [row[1] for row in allocation.read_source_allocations(allocated)] == [57, 74, 91, 108]
        assert len(boundary.allocations) == 4
        assert all(path.exists() for path in old_outputs)
        assert all(
            (journal.root / "vectors" / name).read_bytes() == payload for name, payload in old_vectors_by_key.items()
        )
        assert len(tuple((journal.root / "outputs").iterdir())) == 2
    finally:
        _close_recovery(case, plan, journal, allocated)


async def test_freshly_confirmed_rollback_keeps_gaps_and_never_reuses_provisional_vector(tmp_path, monkeypatch):
    case, plan, boundary, journal = await _recovery_case(tmp_path, monkeypatch)
    boundary.failure = "before"
    allocated = None
    try:
        with pytest.raises(RuntimeError, match="before commit"):
            await _allocate(tmp_path, case[1], plan, maximum=2, journal=journal)
        assert boundary.committed["owner_checkpoint_revision"] is None
        assert len(tuple((journal.root / "vectors").iterdir())) == 1
        boundary.failure = None
        allocated = await _allocate(tmp_path, case[1], plan, maximum=2, journal=journal)
        assert next(allocation.read_source_allocations(allocated))[1] == 74
        assert len(boundary.allocations) == 5
        assert boundary.committed["owner_checkpoint_revision"] == 4
        assert len(tuple((journal.root / "vectors").iterdir())) == 5
    finally:
        _close_recovery(case, plan, journal, allocated)


@pytest.mark.parametrize("damage", ("batch", "plan", "missing", "bytes", "symlink", "mode"))
async def test_replay_refuses_changed_inputs_or_vectors_before_new_native_ids(tmp_path, monkeypatch, damage):
    case, plan, boundary, journal = await _recovery_case(tmp_path, monkeypatch)
    boundary.failure = "unknown"
    try:
        with pytest.raises(FamilyBlockUnavailable, match="commit is unknown"):
            await _allocate(tmp_path, case[1], plan, maximum=2, journal=journal)
        vector = next((journal.root / "vectors").iterdir())
        current_plan = plan
        maximum = 2
        match damage:
            case "batch":
                maximum = 4
            case "plan":
                current_plan = replace(plan, sha256=b"x" * 32)
            case "missing":
                vector.unlink()
            case "symlink":
                vector.unlink()
                vector.symlink_to(Path(plan.root) / "source-plan.mpk")
            case _:
                vector.chmod(0o600)
                if damage == "bytes":
                    vector.write_bytes(vector.read_bytes() + b"x")
                    vector.chmod(0o400)
        with pytest.raises((ValueError, OSError, FamilyBlockUnavailable)):
            await _allocate(tmp_path, case[1], current_plan, maximum=maximum, journal=journal)
        assert len(boundary.allocations) == 1 and boundary.committed["owner_checkpoint_revision"] == 1
        assert Path(plan.root).exists() and (journal.root / "checkpoints").exists()
    finally:
        _close_recovery(case, plan, journal)


async def test_cancel_after_append_keeps_uncommitted_evidence_and_requires_fresh_read(tmp_path, monkeypatch):
    case, plan, boundary, journal = await _recovery_case(tmp_path, monkeypatch)
    original = allocation.checkpoint_api.prepare_checkpoint_append
    allocated = None

    def cancelled(*args, **kwargs):
        original(*args, **kwargs)
        raise asyncio.CancelledError

    monkeypatch.setattr(allocation.checkpoint_api, "prepare_checkpoint_append", cancelled)
    try:
        with pytest.raises(asyncio.CancelledError):
            await _allocate(tmp_path, case[1], plan, maximum=2, journal=journal)
        assert boundary.committed["owner_checkpoint_revision"] is None
        assert len(tuple((journal.root / "vectors").iterdir())) == 1
        assert not journal.busy and Path(plan.root).exists()
        monkeypatch.setattr(allocation.checkpoint_api, "prepare_checkpoint_append", original)
        allocated = await _allocate(tmp_path, case[1], plan, maximum=2, journal=journal)
        assert next(allocation.read_source_allocations(allocated))[1] == 74
        assert len(boundary.allocations) == 5
    finally:
        _close_recovery(case, plan, journal, allocated)


async def test_continuation_cannot_create_or_replay_fresh_source_allocations(tmp_path, monkeypatch):
    from process.custom_import.continuation_runtime import BuildContinuationContext

    case, plan, boundary, journal = await _recovery_case(tmp_path, monkeypatch)
    try:
        request = replace(case[1], continuation=BuildContinuationContext(9, case[1].fence, b"p" * 32))
        with pytest.raises(FamilyBlockUnavailable, match="original complete"):
            await _allocate(tmp_path, request, plan, maximum=2, journal=journal)
        assert not boundary.allocations and not tuple((journal.root / "vectors").iterdir())
    finally:
        _close_recovery(case, plan, journal)


async def test_completed_source_provenance_keeps_producer_while_original_writer_refuses_continuation(
    tmp_path, monkeypatch
):
    from process.custom_import.continuation_runtime import BuildContinuationContext, producer_token_sha256

    case, plan, boundary, journal = await _recovery_case(tmp_path, monkeypatch)
    try:
        request = replace(
            case[1],
            lease_token=b"n" * 32,
            continuation=BuildContinuationContext(9, 7, producer_token_sha256(case[1])),
        )
        assert allocation._checked_source_provenance(request, plan) == dict(plan.scope[0])
        with pytest.raises(FamilyBlockUnavailable, match="original complete"):
            allocation._checked_source(request, plan)
        wrong = replace(request, continuation=BuildContinuationContext(9, 7, b"x" * 32))
        with pytest.raises(FamilyBlockUnavailable, match="scope differs"):
            allocation._checked_source_provenance(wrong, plan)
        assert not boundary.allocations and not tuple((journal.root / "vectors").iterdir())
    finally:
        _close_recovery(case, plan, journal)


@pytest.mark.parametrize("field", (3, 4))
async def test_current_native_capture_hashes_match_before_any_id_allocation(tmp_path, monkeypatch, field):
    case, plan, boundary, journal = await _recovery_case(tmp_path, monkeypatch)
    changed_items = list(boundary.capture_scope)
    changed_items[field] = b"x" * 32
    boundary.capture_scope = tuple(changed_items)
    try:
        with pytest.raises(FamilyBlockUnavailable, match="capture binding"):
            await _allocate(tmp_path, case[1], plan, maximum=2, journal=journal)
        assert not boundary.allocations and boundary.committed["owner_checkpoint_revision"] is None
        assert not tuple((journal.root / "vectors").iterdir())
    finally:
        _close_recovery(case, plan, journal)


async def test_replay_does_not_allocate_new_ids_after_native_source_phase_closes(tmp_path, monkeypatch):
    case, plan, boundary, journal = await _recovery_case(tmp_path, monkeypatch)
    boundary.failure = "unknown"
    try:
        with pytest.raises(FamilyBlockUnavailable, match="commit is unknown"):
            await _allocate(tmp_path, case[1], plan, maximum=2, journal=journal)
        boundary.phase = "admission"
        with pytest.raises(FamilyBlockUnavailable, match="current SOURCE build phase"):
            await _allocate(tmp_path, case[1], plan, maximum=2, journal=journal)
        assert len(boundary.allocations) == 1 and boundary.committed["owner_checkpoint_revision"] == 1
    finally:
        _close_recovery(case, plan, journal)


async def test_stale_protected_head_refuses_before_any_native_id_allocation(tmp_path, monkeypatch):
    case, plan, boundary, journal = await _recovery_case(tmp_path, monkeypatch)
    original_scope = checkpoint._native_scope

    async def advanced(*args, **kwargs):
        descriptor, scope = await original_scope(*args, **kwargs)
        if kwargs["writing"]:
            descriptor.owner_checkpoint_revision = 1
            descriptor.owner_checkpoint_sha256 = b"x" * 32
        return descriptor, scope

    monkeypatch.setattr(checkpoint, "_native_scope", advanced)
    try:
        with pytest.raises(FamilyBlockUnavailable, match="before native allocation"):
            await _allocate(tmp_path, case[1], plan, maximum=2, journal=journal)
        assert not boundary.allocations and boundary.committed["owner_checkpoint_revision"] is None
        assert not tuple((journal.root / "vectors").iterdir())
    finally:
        _close_recovery(case, plan, journal)


def _journal_files(root):
    return {
        str(path.relative_to(root)): (path.stat().st_size, hashlib.sha256(path.read_bytes()).digest())
        for path in root.rglob("*")
        if path.is_file()
    }


def _journal_cost(root):
    filesystem = os.statvfs(root)
    unit = max(512, filesystem.f_frsize or filesystem.f_bsize)
    observed_entries = [path.lstat() for path in (root, *root.rglob("*"))]
    cost = sum(
        ((max(entry.st_size, entry.st_blocks * 512) + unit - 1) // unit + 1) * unit for entry in observed_entries
    )
    return cost, unit


@pytest.mark.parametrize("overage", (0, 1))
async def test_aggregate_budget_bounds_many_batches_and_preserves_exhausted_retry(tmp_path, monkeypatch, overage):
    case = await _case(tmp_path, {"root_source": [[_root()] * 40], "detail_source": [[_child()] * 40]})
    plan = _build(tmp_path, case)
    boundary = _native_boundary(monkeypatch, plan)
    maximum = 128 * 1024
    journal = _journal(
        tmp_path, facts=SourceFactLimits(maximum, 4096, 16384), control=SourceFactLimits(maximum, 4096, 16384)
    )
    original_census, censuses = journal.reconcile_budget, []

    def census():
        censuses.append(True)
        return original_census()

    monkeypatch.setattr(journal, "reconcile_budget", census)
    try:
        with pytest.raises(SourceFactBudgetExceeded, match="aggregate"):
            await _allocate(tmp_path, case[1], plan, maximum=2, journal=journal)
        assert len(boundary.allocations) > 1 and boundary.committed["owner_checkpoint_revision"] > 1
        files = _journal_files(journal.root)
        actual = sum(size for size, _digest in files.values())
        assert actual <= journal.budget().used <= maximum
        assert len(tuple((journal.root / "vectors").iterdir())) > 1
        assert len(censuses) == 1  # Never one full directory census per batch.

        # Model a retained provisional file that consumed the final allocation units
        # before process loss. It is neither deleted nor mistaken for proof.
        padding = journal.root / "vectors" / "held-provisional.mpk"
        retained_cost, unit = _journal_cost(journal.root)
        assert maximum - retained_cost >= unit
        padding.write_bytes(b"x" * (maximum - retained_cost - unit + overage))
        padding.chmod(0o400)
        original_files = _journal_files(journal.root)
        original_paths = {str(path.relative_to(journal.root)) for path in journal.root.rglob("*")}
        original_allocations = len(boundary.allocations)
        for _attempt in range(2):
            with pytest.raises(SourceFactBudgetExceeded, match="aggregate"):
                await _allocate(tmp_path, case[1], plan, maximum=2, journal=journal)
            if overage:
                with pytest.raises(FamilyBlockUnavailable, match="fresh aggregate"):
                    journal.budget()
            else:
                assert journal.budget().used == maximum
            assert _journal_files(journal.root) == original_files
            assert {str(path.relative_to(journal.root)) for path in journal.root.rglob("*")} == original_paths
            assert len(boundary.allocations) == original_allocations
        assert len(censuses) == 3
    finally:
        _close_recovery(case, plan, journal)


async def test_checkpoint_provisional_charge_is_reconciled_without_deleting_files(tmp_path, monkeypatch):
    case, plan, boundary, journal = await _recovery_case(tmp_path, monkeypatch)
    original_append = allocation.checkpoint_api.prepare_checkpoint_append

    def fail_after_file(*args, **kwargs):
        original_append(*args, **kwargs)
        raise RuntimeError("synthetic append response lost before transaction commit")

    monkeypatch.setattr(allocation.checkpoint_api, "prepare_checkpoint_append", fail_after_file)
    allocated = None
    try:
        with pytest.raises(RuntimeError, match="append response lost"):
            await _allocate(tmp_path, case[1], plan, maximum=2, journal=journal)
        held = _journal_files(journal.root)
        actual = sum(size for size, _digest in held.values())
        assert actual < journal.budget().used <= journal.work.limits.max_spool_bytes
        assert boundary.committed["owner_checkpoint_revision"] is None
        monkeypatch.setattr(allocation.checkpoint_api, "prepare_checkpoint_append", original_append)
        allocated = await _allocate(tmp_path, case[1], plan, maximum=2, journal=journal)
        after = _journal_files(journal.root)
        assert all(after[name] == evidence for name, evidence in held.items())
        retained_cost, _unit = _journal_cost(journal.root)
        assert sum(size for size, _digest in after.values()) < retained_cost <= journal.budget().used
        journal.reconcile_budget()
        assert journal.budget().used == retained_cost
        assert len(boundary.allocations) == 5
    finally:
        _close_recovery(case, plan, journal, allocated)
