# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real producer files survive original cleanup; only native transport is doubled."""

import asyncio
import hashlib
import os
import shutil
import threading
from contextlib import asynccontextmanager
from dataclasses import astuple, replace
from pathlib import Path
from types import SimpleNamespace

import msgpack
import pytest

from process.custom_import import family_shard_allocate as allocation
from process.custom_import import family_shard_source_checkpoint as checkpoint
from process.custom_import import family_shard_source_journal as journal_api
from process.custom_import import family_shard_source_stage as stage
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_shard_capture import PreparedCaptureSource
from process.custom_import.family_shard_input import ShardInputLimits, cleanup_spool
from process.custom_import.family_shard_metadata import reopen_owner_metadata
from process.custom_import.family_shard_owner_lock import acquire_owner_job_lock
from process.custom_import.family_shard_plan import PlanWork
from process.custom_import.family_shard_route import route_capture
from process.custom_import.family_shard_source_plan import cleanup_source_plan
from process.custom_import.family_shard_source_stage import SourceStageContext
from process.custom_import.family_shard_tasks import _prepared_sha256
from tests import test_custom_import_family_shard_source_plan as source_fixture
from tests.test_custom_import_family_shard_allocate import _allocate, _journal_for_plan, _native_boundary
from tests.test_custom_import_family_shard_metadata import _owner, _work
from tests.test_custom_import_family_shard_source_plan import _build, _case, _child, _root
from tests.test_custom_import_family_shard_source_stage import _owner as stage_owner
from tests.test_custom_import_family_shard_worker import _limits as worker_limits
from tests.test_custom_import_family_shard_worker import _projected_definition

pytestmark = pytest.mark.asyncio


@asynccontextmanager
async def _canonical_owner(tmp_path, monkeypatch):
    root = tmp_path / "owner"
    root.mkdir(mode=0o700)
    lock = acquire_owner_job_lock(root, identity=(root.stat().st_dev, root.stat().st_ino))
    template = stage_owner(root, owner_lock=lock)
    limits = replace(
        template.limits,
        allocation=replace(
            template.limits.allocation, spool=replace(template.limits.allocation.spool, memory_bytes=1024**2)
        ),
    )
    profile = astuple(limits.facts), 1024
    owner = stage.SourceAdmissionStage(
        replace(template.context, owner_profile=profile),
        lambda: iter(()),
        root=root / "source",
        register_cleanup=lambda _path: None,
        limits=limits,
        current=lambda: None,
        owner_lock=lock,
    )
    owner._ensure_root()
    ready_root = stage._producer_root(owner)
    partition = source_fixture._partition

    async def original_partition(*args, **kwargs):
        return await partition(*args, **kwargs, root=ready_root / "input")

    monkeypatch.setattr(source_fixture, "_partition", original_partition)
    try:
        case = await _case(tmp_path, definition=_projected_definition())
        owner.context = replace(_stage_context(case), owner_profile=profile)
        plan = source_fixture.planner.build_source_plan(
            case[0],
            case[1],
            case[2],
            case[3],
            root=ready_root / "plan",
            register_cleanup=lambda _path: None,
            limits=limits.preparation.plan,
        )
        _native_boundary(monkeypatch, plan)
        journal = journal_api.create_source_allocation_journal(
            23,
            work=PlanWork(owner.root / "allocation-journal", limits.allocation.spool, lambda _path: None),
            control_limits=limits.checkpoint,
        )
        allocated = await _allocate(tmp_path, case[1], plan, journal=journal)
        _owner(monkeypatch, allocated, journal)
        owner.prepared, owner.allocated = PreparedCaptureSource(case[4], case[0], plan), allocated
        owner.allocation_journal = journal
        yield owner, case
    finally:
        await owner._stack.aclose()
        lock.close()
        if root.exists():
            shutil.rmtree(root)


async def test_actual_owner_reuses_canonical_files_and_cold_reopens_the_committed_census(tmp_path, monkeypatch):
    async with _canonical_owner(tmp_path, monkeypatch) as (owner, case):
        original_partition, original_plan = owner.prepared.partition, owner.prepared.plan
        copy_tree, copied_roles = checkpoint._copy_tree, []

        def observe_copy(source, target, *args):
            copied_roles.append(target.name)
            return copy_tree(source, target, *args)

        monkeypatch.setattr(checkpoint, "_copy_tree", observe_copy)
        await stage._prepare_tasks(None, owner)
        assert copied_roles == ["allocations"]
        assert owner.prepared.partition == original_partition and owner.prepared.plan == original_plan
        assert not any((owner.root / role).exists() for role in ("input", "plan", "route"))
        snapshot = await journal_api._custody_snapshot(None, owner.context, owner.allocation_journal)
        ready_record, custody, outputs, common = checkpoint.source_owner_checkpoint(
            snapshot, owner.owner_lock.root, stage.owner_source_profile(owner), owner.current
        )
        assert ready_record == owner.ready_record and outputs == () and common is None
        observed = _observed(owner.ready)
        await owner._stack.aclose()
        owner.owner_lock.close()
        root = owner.root.parent
        replacement = acquire_owner_job_lock(root, identity=(root.stat().st_dev, root.stat().st_ino))
        try:
            custody.current(root, stage.owner_source_profile(owner))

            async def load(*_args):
                return case[4]

            monkeypatch.setattr(checkpoint, "_load_retained_source", load)
            reopened = await checkpoint.reopen_source_ready(
                None,
                owner.context,
                root=owner.root / "ready",
                checkpoint_root=owner.allocation_journal.checkpoints,
                limits=owner.limits.facts,
                worker_limits=owner.limits.worker,
                metadata_work=_scratch(tmp_path, "cold-read"),
                checkpoint_limits=owner.limits.checkpoint,
            )
            assert _observed(reopened) == observed
            _consume_canonical_inventory(owner, snapshot, replacement, tmp_path, monkeypatch)
        finally:
            replacement.close()


def _consume_canonical_inventory(owner, snapshot, replacement, tmp_path, monkeypatch):
    """Join real protected files to the inventory consumer with synthetic retirement authority."""
    from process.custom_import import family_shard_retirement_files as retirement_files
    from process.custom_import import family_shard_retirement_policy as retirement

    root = replacement.root
    policy, _custody = retirement.original_policy(snapshot, root, owner.limits, 1024, replacement.check)
    state = tmp_path / "retirement"
    state.mkdir(mode=0o700)
    context = retirement_files.RetirementFiles(
        root,
        state,
        "a" * 64,
        retirement_files._directory(root),
        retirement_files._directory(state),
        replacement.file_identity,
        64 * 1024**2,
    )
    entries, digest = retirement_files.seal_inventory(context, policy, replacement.check)
    assert any(entry[0] == "source/ready/input/storage.json" for entry in entries)
    _partial_retirement(monkeypatch, retirement_files, context, replacement, digest)
    replacement.remove_terminal_root()


def _partial_retirement(monkeypatch, files, context, lock, digest):
    """Exercise exact surviving-file consumption after a synthetic unlink crash."""
    original = files._remove_entry
    removed_paths = []

    def fail_after_unlink(*args):
        original(*args)
        removed_paths.append(args[1][0])
        raise RuntimeError("synthetic post-unlink failure")

    with monkeypatch.context() as patch:
        patch.setattr(files, "_remove_entry", fail_after_unlink)
        with pytest.raises(RuntimeError, match="post-unlink"):
            files.consume_inventory(context, lock.check)
    assert len(removed_paths) == 1
    assert files.consume_inventory(context, lock.check) == digest


@pytest.mark.parametrize("damage", ("parent", "unknown", "closed_lock", "same_bytes_inode"))
async def test_canonical_ready_requires_its_exact_live_producer_custody(tmp_path, monkeypatch, damage):
    async with _canonical_owner(tmp_path, monkeypatch) as (owner, _case_value):
        if damage == "parent":
            owner.ready_directory = (
                *owner.ready_directory[:1],
                owner.ready_directory[1] + 1,
                *owner.ready_directory[2:],
            )
        elif damage == "unknown":
            (owner.root / "ready/foreign").write_text("preserve")
        elif damage == "closed_lock":
            owner.owner_lock.close()
        else:
            path = Path(owner.prepared.plan.root) / "source-plan.mpk"
            payload = path.read_bytes()
            original = path.rename(tmp_path / "original-source-plan")
            path.write_bytes(payload)
            path.chmod(0o400)
            assert path.stat().st_ino != original.stat().st_ino
        with pytest.raises((ValueError, OSError)):
            await stage._prepare_tasks(None, owner)
        assert not (owner.root / "ready/source-ready.mpk").exists()


@pytest.mark.parametrize("role", ("input", "plan", "routed"))
async def test_canonical_ready_rejects_late_same_byte_producer_replacement(tmp_path, monkeypatch, role):
    async with _canonical_owner(tmp_path, monkeypatch) as (owner, _case_value):
        prepare = checkpoint.prepare_owned_source_ready

        def replace_after_production(actual_owner, *args, **kwargs):
            assert actual_owner.metadata is not None and actual_owner.routed is not None
            root = actual_owner.root / "ready" / role
            path = (
                root
                if role == "input"
                else root / ("source-plan.mpk" if role == "plan" else actual_owner.routed[0].name)
            )
            original_inode = path.stat().st_ino
            original = tmp_path / "original-producer"
            path.rename(original)
            if path == root:
                shutil.copytree(original, path)
            else:
                shutil.copy2(original, path)
            assert path.stat().st_ino != original_inode
            return prepare(actual_owner, *args, **kwargs)

        monkeypatch.setattr(checkpoint, "prepare_owned_source_ready", replace_after_production)
        with pytest.raises((ValueError, OSError)):
            await stage._prepare_tasks(None, owner)
        assert not (owner.root / "ready/source-ready.mpk").exists()


def _limits():
    return worker_limits().facts


def _scratch(tmp_path, name="scratch", *, check=None):
    return PlanWork(tmp_path / name, _limits(), lambda _path: None, check)


@asynccontextmanager
async def _originals(tmp_path, monkeypatch, *, empty=False, shard_count=None, repeats=1):
    source_rows = (
        {"root_source": [[]], "detail_source": [[]]}
        if empty
        else {
            "root_source": [[_root(), _root("1234567893")] * repeats],
            "detail_source": [[_child(), _child(key="b"), _child("orphan", "c")] * repeats],
        }
    )
    if shard_count is not None or repeats > 1:
        original_partition = source_fixture._partition

        async def partition(*args, **kwargs):
            return await original_partition(
                *args, **kwargs, limits=ShardInputLimits(32 * 1024**2, shard_count or 4, 65536 if repeats > 1 else 1024)
            )

        monkeypatch.setattr(source_fixture, "_partition", partition)
    case = await _case(
        tmp_path,
        source_rows,
        definition=_projected_definition(),
        row_limit=32 if repeats > 1 else 2,
        byte_limit=131072 if repeats > 1 else 16384,
    )
    plan = _build(tmp_path, case)
    _native_boundary(monkeypatch, plan)
    journal = _journal_for_plan(tmp_path, plan)
    allocated = await _allocate(tmp_path, case[1], plan, journal=journal)
    _owner(monkeypatch, allocated, journal)
    try:
        with route_capture(
            case[0],
            case[1].definition,
            root=tmp_path / "route",
            register_cleanup=lambda _path: None,
            limits=_limits(),
        ) as shards:
            async with _work(tmp_path, case[1], allocated, journal) as metadata:
                yield case, PreparedCaptureSource(case[4], case[0], plan), allocated, shards, metadata
    finally:
        allocation.cleanup_source_allocations(allocated)
        journal.close_terminal()
        cleanup_source_plan(plan)
        cleanup_spool(case[0])


def _stage_context(case, *, request=None, build_id=23, family_id=17):
    """Use the fixture's existing request, registry and native transport identities."""
    request = case[1] if request is None else request
    layout = FamilyBlockLayout(request.definition, case[2].child_collection_slots)
    return SourceStageContext(request, case[2], layout, family_id, build_id)


def _prepare(tmp_path, originals, *, registered=None, check=None):
    case, prepared, source, shards, metadata = originals
    return checkpoint.prepare_source_ready(
        prepared,
        source,
        shards,
        metadata,
        _stage_context(case),
        PlanWork(tmp_path / "retained", _limits(), (registered if registered is not None else []).append),
        worker_limits=worker_limits(),
        metadata_work=_scratch(tmp_path, check=check),
    )


def _native_transport(monkeypatch, case):
    """Return a provisional append; the explicit commit alone makes it visible."""
    state_by_field = dict(
        pending=None,
        committed=None,
        reads=0,
        headers=0,
        storage=SimpleNamespace(root="native-journal", limits=_limits()),
    )

    async def append(session, request, build_id, family_id, layout, *, storage, record, expected_head):
        assert request is case[1] and family_id == 17 and build_id == 23
        assert expected_head is None and session == "owner-transaction"
        state_by_field["pending"] = record
        return "provisional-head"

    async def read(factory, request, build_id, family_id, layout, *, root, limits):
        assert request == case[1] and build_id == 23 and family_id == 17
        state_by_field["reads"] += 1

        def records(*, check=None):
            if check is not None:
                check()
            if state_by_field["committed"] is not None:
                yield "protected-head", state_by_field["committed"]
            state_by_field["exhausted"] = True

        return SimpleNamespace(records=records)

    async def load(factory, request, build_id, family_id, layout):
        assert request == case[1] and build_id == 23 and family_id == 17
        assert type(layout) is FamilyBlockLayout and layout.definition.digest == request.definition.digest
        state_by_field["headers"] += 1
        case[4].headers(request.definition)
        return case[4]

    monkeypatch.setattr(checkpoint, "append_owner_checkpoint", append)
    monkeypatch.setattr(checkpoint, "read_owner_checkpoint", read)
    monkeypatch.setattr(checkpoint, "_load_retained_source", load)
    return state_by_field


async def _anchor(ready, case, state):
    prepared = await checkpoint.append_source_ready_checkpoint(
        "owner-transaction",
        _stage_context(case),
        ready,
        storage=state["storage"],
        expected_head=None,
    )
    assert prepared == "provisional-head"
    state["committed"] = state["pending"]


async def _reopen(tmp_path, case, **options):
    return await checkpoint.reopen_source_ready(
        None,
        _stage_context(case, request=options.pop("request", case[1])),
        root=options.pop("root", tmp_path / "retained"),
        checkpoint_root="native-journal",
        limits=_limits(),
        worker_limits=worker_limits(),
        metadata_work=_scratch(tmp_path, "reopen-scratch"),
        **options,
    )


def _observed(ready):
    return tuple(ready.tasks.load_plan(index, worker_limits()) for index in range(len(ready.tasks.files)))


def _damage_ready_plan(ready, damage):
    plan = ready.allocated.source
    plan_file = Path(plan.root) / "source-plan.mpk"
    plan_bytes = plan_file.read_bytes()
    if damage == "bytes":
        plan_bytes = plan_bytes[:-1] + bytes((plan_bytes[-1] ^ 1,))
    else:
        position = 0
        while position < len(plan_bytes):
            footer_start = position
            position += 8 + source_fixture.planner._LENGTH.unpack(plan_bytes[position : position + 8])[0]
        plan_bytes = plan_bytes[:footer_start]
        plan = replace(plan, file_bytes=len(plan_bytes), sha256=hashlib.sha256(plan_bytes).digest())
        vectors = Path(ready.allocated.root) / "source-ids.mpk"
        allocated_bytes = vectors.read_bytes()
        size = allocation._LENGTH.unpack(allocated_bytes[:8])[0]
        header = msgpack.unpackb(allocated_bytes[8 : 8 + size], raw=False, use_list=False)
        header = msgpack.packb((*header[:2], plan.sha256, header[3]), use_bin_type=True)
        allocated_bytes = allocation._LENGTH.pack(len(header)) + header + allocated_bytes[8 + size :]
        vectors.chmod(0o600)
        vectors.write_bytes(allocated_bytes)
        vectors.chmod(0o400)
        ready = replace(
            ready,
            prepared=replace(ready.prepared, plan=plan),
            allocated=replace(ready.allocated, source=plan, sha256=hashlib.sha256(allocated_bytes).digest()),
        )
    plan_file.chmod(0o600)
    plan_file.write_bytes(plan_bytes)
    plan_file.chmod(0o400)
    return ready


@pytest.mark.parametrize("damage", (None, "bytes", "footer"))
async def test_ready_plan_single_replay(tmp_path, monkeypatch, damage):
    async with _originals(tmp_path, monkeypatch) as originals:
        ready = _prepare(tmp_path, originals)
        try:
            if damage is not None:
                ready = _damage_ready_plan(ready, damage)
            plan = ready.allocated.source
            calls, completed, checks = [], [], []
            work = _scratch(tmp_path, "eof-scratch", check=lambda: checks.append(None))
            original_entries = source_fixture.planner._entries

            def once(current, *, check=None):
                calls.append(current)
                assert calls == [plan], "SOURCE-ready must not traverse the full plan twice"
                assert check is work.check
                yield from original_entries(current, check=check)
                completed.append(current)

            monkeypatch.setattr(source_fixture.planner, "_entries", once)
            if damage is None:
                assert checkpoint._validate_ready_eof(ready, originals[0][1], work) == plan.counts[0]
                assert completed == [plan]
            else:
                message = "SOURCE plan bytes differ" if damage == "bytes" else "SOURCE plan is incomplete"
                with pytest.raises(ValueError, match=message):
                    checkpoint._validate_ready_eof(ready, originals[0][1], work)
                assert completed == []
            assert calls == [plan] and checks
        finally:
            shutil.rmtree(ready.root)


async def test_ready_reopens_exact_ids_and_files_after_original_owners_close(tmp_path, monkeypatch):
    registered_paths = []
    async with _originals(tmp_path, monkeypatch) as originals:
        case, _prepared, original_source, shards, metadata = originals
        expected_ids = list(allocation.read_source_allocations(original_source))
        expected_metadata = list(metadata.family_ids()), list(metadata.query_keys())
        ready = _prepare(tmp_path, originals, registered=registered_paths)
        state = _native_transport(monkeypatch, case)
        plans, scope_sha = _observed(ready), _prepared_sha256(ready.tasks)
        await _anchor(ready, case, state)
        original_paths = (
            Path(original_source.root),
            Path(original_source.source.root),
            Path(shards[0].root),
            metadata.work.root,
        )
        assert all(path.exists() for path in original_paths)
    assert all(not path.exists() for path in original_paths)

    async def forbid(*_arguments, **_keywords):
        pytest.fail("reopening cannot recapture or allocate replacement IDs")

    monkeypatch.setattr(allocation, "allocate_source_plan", forbid)
    monkeypatch.setattr(allocation, "_allocate_record_ids", forbid)
    reopened = await _reopen(tmp_path, case)
    assert _observed(reopened) == plans
    assert list(allocation.read_source_allocations(reopened.allocated)) == expected_ids
    assert _prepared_sha256(reopened.tasks) == scope_sha
    assert reopened.descriptor_sha256 == ready.descriptor_sha256
    with reopen_owner_metadata(
        reopened.metadata.root,
        expected_scope=reopened.metadata.scope,
        descriptor_sha256=reopened.metadata.descriptor_sha256,
        work=_scratch(tmp_path, "inspect-scratch"),
    ) as metadata:
        assert (list(metadata.family_ids()), list(metadata.query_keys())) == expected_metadata
    reopened.tasks.close()
    assert all((Path(ready.root) / role).exists() for role in checkpoint._ROLES)
    assert state["exhausted"] and state["headers"] == 1
    assert tmp_path / "retained" in registered_paths
    shutil.rmtree(ready.root)


async def test_compressed_ready_fits_the_same_aggregate_cap_that_rejects_raw_copies(tmp_path, monkeypatch):
    from process.custom_import.family_shard_facts import _pack
    from process.custom_import.family_shard_input import manifest_document
    from process.custom_import.family_shard_route import read_routed_shard
    from process.custom_import.family_shard_tasks import _BorrowedWork

    limits = replace(_limits(), max_spool_bytes=4 * 1024**2)
    async with _originals(tmp_path, monkeypatch, repeats=1000) as originals:
        case, prepared, allocated, shards, metadata = originals
        borrowed = _BorrowedWork(Path(shards[0].root), (shards[0].root_device, shards[0].root_inode), _limits())
        routed_bytes = sum(
            len(_pack(routed_row)) + 8 for shard in shards for routed_row in read_routed_shard(shard, borrowed)
        )
        raw_role_bytes = (
            manifest_document(prepared.partition)["spool_bytes"],
            prepared.plan.file_bytes,
            allocated.file_bytes,
            routed_bytes,
        )
        assert max(raw_role_bytes) < limits.max_spool_bytes < sum(raw_role_bytes), raw_role_bytes
        ready = checkpoint.prepare_source_ready(
            prepared,
            allocated,
            shards,
            metadata,
            _stage_context(case),
            PlanWork(tmp_path / "retained", limits, lambda _path: None),
            worker_limits=worker_limits(),
            metadata_work=PlanWork(tmp_path / "ready-scratch", limits, lambda _path: None),
        )
        actual_bytes = sum(
            path.stat().st_size for role in checkpoint._ROLES for path in (Path(ready.root) / role).iterdir()
        )
        assert actual_bytes <= limits.max_spool_bytes
        assert sum(plan.shard.records for plan in _observed(ready)) == 5000
    shutil.rmtree(ready.root)


@pytest.mark.parametrize("empty", (True, False))
async def test_seal_has_bounded_frames_and_only_primitive_custody(tmp_path, monkeypatch, empty):
    async with _originals(tmp_path, monkeypatch, empty=empty) as originals:
        ready = _prepare(tmp_path, originals)
        with (Path(ready.root) / checkpoint._DESCRIPTOR).open("rb") as descriptor:
            rows = tuple(checkpoint._read_frames(descriptor, _limits(), None))
        assert rows[-1][0] == "end"
        assert len([row for row in rows if row[0] == "task"]) == len(ready.shards)
        assert (
            hashlib.sha256((Path(ready.root) / checkpoint._DESCRIPTOR).read_bytes()).digest() == ready.descriptor_sha256
        )
        payload = repr(rows)
        assert all(name not in payload for name in ("lease_token", "snapshot_token", "deadline", "session", "verified"))
        assert ready.tasks.counts["records"] == (0 if empty else 5)
        assert len(_observed(ready)) == len(ready.shards)
    shutil.rmtree(ready.root)


async def test_provisional_uncommitted_descriptor_never_reopens(tmp_path, monkeypatch):
    async with _originals(tmp_path, monkeypatch) as originals:
        case = originals[0]
        ready = _prepare(tmp_path, originals)
        state = _native_transport(monkeypatch, case)
        await checkpoint.append_source_ready_checkpoint(
            "owner-transaction",
            _stage_context(case, build_id=23, family_id=17),
            ready,
            storage=state["storage"],
            expected_head=None,
        )
        with pytest.raises(ValueError, match="no protected completed"):
            await _reopen(tmp_path, case)
        assert state["headers"] == 0
        assert Path(originals[2].root).exists()
    shutil.rmtree(ready.root)


@pytest.mark.parametrize("damage", ("bytes", "missing", "mode", "symlink", "extra", "root", "descriptor"))
async def test_protected_open_refuses_changed_or_incomplete_custody(tmp_path, monkeypatch, damage):
    async with _originals(tmp_path, monkeypatch) as originals:
        case = originals[0]
        ready = _prepare(tmp_path, originals)
        state = _native_transport(monkeypatch, case)
        await _anchor(ready, case, state)
    root, options = Path(ready.root), {}
    target_path = root / "tasks" / ready.tasks.files[0].name
    match damage:
        case "bytes":
            target_path.chmod(0o600)
            target_path.write_bytes(target_path.read_bytes() + b"x")
            target_path.chmod(0o400)
        case "missing":
            target_path.unlink()
        case "mode":
            target_path.chmod(0o600)
        case "symlink":
            target_path.unlink()
            target_path.symlink_to(root / "source-ready.mpk")
        case "extra":
            (root / "routed" / "extra").write_bytes(b"x")
        case "root":
            options["root"] = tmp_path / "other-root"
            shutil.copytree(root, options["root"])
        case _:
            target_path = root / checkpoint._DESCRIPTOR
            target_path.chmod(0o600)
            target_path.write_bytes(target_path.read_bytes()[:-1])
            target_path.chmod(0o400)
    with pytest.raises((ValueError, FileNotFoundError)):
        await _reopen(tmp_path, case, **options)
    assert not (tmp_path / "reopen-scratch").exists()
    shutil.rmtree(root)
    if options:
        shutil.rmtree(options["root"])


@pytest.mark.parametrize("damage", ("capture", "definition", "cohort", "family", "prepared", "duplicate"))
async def test_fresh_scope_and_complete_cohort_are_required(tmp_path, monkeypatch, damage):
    async with _originals(tmp_path, monkeypatch) as originals:
        case = originals[0]
        ready = _prepare(tmp_path, originals)
        state = _native_transport(monkeypatch, case)
        await _anchor(ready, case, state)
        match damage:
            case "capture":

                async def changed_capture(*_args):
                    return replace(case[4], identity=replace(case[4].identity, capture_bundle_id=999))

                monkeypatch.setattr(checkpoint, "_load_retained_source", changed_capture)
            case "prepared":
                source_record = state["committed"]
                state["committed"] = replace(
                    source_record, commitments=(source_record.commitments[0], ("prepared_sha256", b"x" * 32))
                )
            case "duplicate":

                async def duplicated(*_args, **_kwargs):
                    return SimpleNamespace(records=lambda **_options: iter(((None, state["committed"]),) * 2))

                monkeypatch.setattr(checkpoint, "read_owner_checkpoint", duplicated)
            case _:
                original_read = checkpoint._read_descriptor

                def changed(*args):
                    reopened = original_read(*args)
                    match damage:
                        case "definition":
                            reopened.tasks.definition_canonical += " "
                        case "cohort":
                            return replace(reopened, shards=reopened.shards[:-1])
                        case _:
                            return replace(reopened, allocated=replace(reopened.allocated, snapshot_family_id=999))
                    return reopened

                monkeypatch.setattr(checkpoint, "_read_descriptor", changed)
        with pytest.raises(ValueError):
            await _reopen(tmp_path, case)
        assert all(Path(path).exists() for path in (originals[2].root, originals[4].work.root))
    shutil.rmtree(ready.root)


async def test_failed_partial_seal_keeps_original_owners_and_cannot_reopen(tmp_path, monkeypatch):
    registered, checks = [], []

    def cancel():
        checks.append(True)
        if len(checks) == 4:
            raise RuntimeError("synthetic cancellation")

    async with _originals(tmp_path, monkeypatch) as originals:
        case = originals[0]
        state = _native_transport(monkeypatch, case)
        with pytest.raises(RuntimeError, match="cancellation"):
            _prepare(tmp_path, originals, registered=registered, check=cancel)
        assert registered[0] == tmp_path / "retained"
        assert Path(originals[2].root).exists() and list(originals[4].family_ids())
        assert not (tmp_path / "retained" / checkpoint._DESCRIPTOR).exists()
        with pytest.raises(ValueError, match="no protected completed"):
            await _reopen(tmp_path, case)
        assert state["headers"] == 0
    shutil.rmtree(tmp_path / "retained")


async def test_checkpoint_payload_is_fixed_size_for_large_cohorts(tmp_path, monkeypatch):
    async with _originals(tmp_path, monkeypatch, shard_count=256) as originals:
        ready = _prepare(tmp_path, originals)
        state = _native_transport(monkeypatch, originals[0])
        await _anchor(ready, originals[0], state)
        record = state["committed"]
        decoded = msgpack.unpackb(record.payload, raw=False, use_list=False)
        assert len(decoded) == 4 and len(record.payload) < 1024
        assert tuple(name for name, _digest in record.commitments) == ("descriptor_sha256", "prepared_sha256")
        assert all(type(cell) in (str, tuple) for cell in decoded)
        assert len(ready.tasks.files) == 256
        assert (Path(ready.root) / checkpoint._DESCRIPTOR).stat().st_size > _limits().memory_bytes
        reopened = await _reopen(tmp_path, originals[0])
        assert _observed(reopened) == _observed(ready)
    shutil.rmtree(ready.root)


async def test_cancelled_reopening_stops_and_drains_reader_before_cleanup(tmp_path, monkeypatch):
    async with _originals(tmp_path, monkeypatch) as originals:
        ready = _prepare(tmp_path, originals)
        state = _native_transport(monkeypatch, originals[0])
        await _anchor(ready, originals[0], state)
        entered, exited = threading.Event(), threading.Event()

        def wait_for_cancel(root, capture, request, record, limits, check):
            entered.set()
            try:
                while True:
                    check()
                    exited.wait(0.001)
            finally:
                exited.set()

        monkeypatch.setattr(checkpoint, "_read_descriptor", wait_for_cancel)
        opening = asyncio.create_task(_reopen(tmp_path, originals[0]))
        assert await asyncio.to_thread(entered.wait, 5)
        opening.cancel()
        with pytest.raises(asyncio.CancelledError):
            await opening
        assert exited.is_set()
        assert Path(ready.root).exists() and Path(originals[2].root).exists()
        assert not (tmp_path / "reopen-scratch").exists()
    shutil.rmtree(ready.root)


@pytest.mark.parametrize("field", ("plan_count", "file_size", "task_size"))
async def test_authenticated_descriptor_still_requires_typed_scalars(tmp_path, monkeypatch, field):
    async with _originals(tmp_path, monkeypatch, empty=True) as originals:
        ready = _prepare(tmp_path, originals)
        state = _native_transport(monkeypatch, originals[0])
        await _anchor(ready, originals[0], state)
        path = Path(ready.root) / checkpoint._DESCRIPTOR
        with path.open("rb") as original_source:
            source_rows = list(checkpoint._read_frames(original_source, _limits(), None))
        match field:
            case "plan_count":
                header, plan = list(source_rows[0]), list(source_rows[0][5])
                plan[11] = (False, *plan[11][1:])
                header[5] = tuple(plan)
                source_rows[0] = tuple(header)
            case "file_size":
                index = next(
                    index
                    for index, source_row_items in enumerate(source_rows)
                    if source_row_items[0] == "file" and source_row_items[3][-1] == 0
                )
                source_row_items = list(source_rows[index])
                source_row_items[3] = (*source_row_items[3][:-1], False)
                source_rows[index] = tuple(source_row_items)
            case _:
                index = next(
                    index for index, source_row_items in enumerate(source_rows) if source_row_items[0] == "task"
                )
                source_row_items = list(source_rows[index])
                source_row_items[2] = (*source_row_items[2][:3], False, source_row_items[2][4])
                source_rows[index] = tuple(source_row_items)
        path.chmod(0o600)
        with path.open("wb") as target_path:
            for source_row_items in source_rows:
                checkpoint._frame(target_path, source_row_items, _limits())
        path.chmod(0o400)
        stamp, digest = checkpoint._hash_file(path)
        # The protected transport supplies this commitment; bytes alone do not
        # excuse invalid scalar types, even when False compares equal to zero.
        source_record = state["committed"]
        state["committed"] = replace(
            source_record,
            input_sha256=digest,
            commitments=(("descriptor_sha256", digest), source_record.commitments[1]),
            payload=msgpack.packb(
                (checkpoint._CONTRACT, ready.root, checkpoint._directory(path.parent), stamp), use_bin_type=True
            ),
        )
        with pytest.raises(ValueError, match="integer"):
            await _reopen(tmp_path, originals[0])
    shutil.rmtree(ready.root)
