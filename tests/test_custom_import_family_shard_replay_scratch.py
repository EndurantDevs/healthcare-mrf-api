# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real disposable files and process exit; native authority transport is doubled."""

import os
import subprocess
import sys
import threading
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import astuple, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.custom_import import family_shard_output_checkpoint as output
from process.custom_import import family_shard_owner_job as jobs
from process.custom_import import family_shard_retirement_policy as retirement
from process.custom_import import family_shard_source_values as values
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded, SourceFactLimits
from process.custom_import.family_shard_owner_lock import acquire_owner_job_lock
from process.custom_import.family_shard_plan import PlanWork, _Scratch
from tests.test_custom_import_family_shard_source_values import _case, _prepared_rows

_LIMITS = SourceFactLimits(16 * 1024**2, 65536, 65536)


@pytest.fixture
def replay_owner(tmp_path):
    root = tmp_path / "owner"
    root.mkdir(mode=0o700)
    observed = root.stat()
    lock = acquire_owner_job_lock(root, identity=(observed.st_dev, observed.st_ino))
    stage = root / "output"
    stage.mkdir(mode=0o700)
    observed = stage.stat()
    registered_roots = []
    owner = SimpleNamespace(
        root=stage,
        limits=_LIMITS,
        _root_identity=(observed.st_dev, observed.st_ino),
        current=lock.check,
        stop=threading.Event(),
        source=SimpleNamespace(owner_lock=lock, owner_profile=("synthetic-owner-profile",)),
        _stack=AsyncExitStack(),
    )
    owner.work = PlanWork(stage, _LIMITS, registered_roots.append, owner.current)
    try:
        yield owner
    finally:
        lock.close()


def _abandoned(owner):
    root = owner.root / output._REPLAY_SLOT
    root.mkdir(mode=0o700)
    path = root / "run-000000000.mpk"
    path.write_bytes(b"incomplete disposable frame")
    path.chmod(0o600)
    return root, path


@pytest.mark.parametrize("empty", [False, True])
def test_process_death_leaves_only_exact_recoverable_disposable_slot(replay_owner, empty):
    root = replay_owner.root / output._REPLAY_SLOT
    program = """
import os, sys
from process.custom_import.family_shard_facts import SourceFactLimits
from process.custom_import.family_shard_plan import PlanWork, _Scratch
scratch = _Scratch(PlanWork(sys.argv[1], SourceFactLimits(1048576, 4096, 4096), lambda path: None))
if sys.argv[2] != 'empty':
    scratch.write([('disposable', 1), ('disposable', 2)])
os._exit(73)
"""
    child = subprocess.run([sys.executable, "-B", "-c", program, str(root), "empty" if empty else "rows"], timeout=15)
    assert child.returncode == 73 and root.is_dir()
    if not empty:
        assert list(root.glob("run-*.mpk"))
    work = output._replay_work(replay_owner)
    assert work.root == root and not root.exists()
    scratch = _Scratch(work)
    try:
        path = scratch.write([("fresh", 3)])
        assert list(scratch.rows(path)) == [("fresh", 3)]
    finally:
        scratch.close()
    assert not root.exists()


@pytest.mark.parametrize("fault", ["unknown", "directory", "symlink", "hardlink", "mode"])
def test_complete_census_refuses_unsafe_scratch_before_any_unlink(replay_owner, fault):
    root, original = _abandoned(replay_owner)
    unsafe = root / "run-000000001.mpk"
    if fault == "unknown":
        unsafe = root / "unrecorded-original.mpk"
        unsafe.write_bytes(b"retain")
    elif fault == "directory":
        unsafe.mkdir(mode=0o700)
    elif fault == "symlink":
        unsafe.symlink_to(original)
    elif fault == "hardlink":
        os.link(original, unsafe)
    else:
        original.chmod(0o666)
    with pytest.raises(FamilyBlockUnavailable):
        output._replay_work(replay_owner)
    assert original.read_bytes() == b"incomplete disposable frame" and root.is_dir()


def test_same_bytes_new_inode_and_new_child_refuse_saved_census(replay_owner):
    root, original = _abandoned(replay_owner)
    identity, files = output._replay_census(replay_owner, root)
    replacement = root / "replacement"
    replacement.write_bytes(original.read_bytes())
    replacement.chmod(0o600)
    replacement.replace(original)
    with pytest.raises(FamilyBlockUnavailable):
        output._remove_replay(replay_owner, root, identity, files)
    assert original.exists()
    identity, files = output._replay_census(replay_owner, root)
    (root / "run-000000001.mpk").write_bytes(b"late")
    with pytest.raises(FamilyBlockUnavailable):
        output._remove_replay(replay_owner, root, identity, files)
    assert original.exists()


def test_partial_unlink_failure_is_recoverable_without_touching_originals(replay_owner, monkeypatch):
    root, _first = _abandoned(replay_owner)
    (root / "run-000000001.mpk").write_bytes(b"another incomplete frame")
    source = replay_owner.root / "source-families"
    source.mkdir(mode=0o700)
    original = source / "run-000000000.mpk"
    original.write_bytes(b"immutable original")
    before = original.stat(), original.read_bytes()
    unlink = os.unlink
    removed_paths = []

    def interrupted(name, **options):
        unlink(name, **options)
        removed_paths.append(name)
        raise OSError("interrupted after one unlink")

    monkeypatch.setattr(os, "unlink", interrupted)
    with pytest.raises(OSError, match="after one unlink"):
        output._replay_work(replay_owner)
    assert len(removed_paths) == 1 and root.exists()
    monkeypatch.setattr(os, "unlink", unlink)
    output._replay_work(replay_owner)
    assert not root.exists() and (original.stat(), original.read_bytes()) == before


@pytest.mark.parametrize("budget", ["memory", "disk"])
def test_abandoned_files_are_bounded_before_any_removal(replay_owner, budget):
    root, path = _abandoned(replay_owner)
    if budget == "memory":
        replay_owner.limits = replace(replay_owner.limits, memory_bytes=64)
    else:
        replay_owner.limits = SourceFactLimits(65536, 4096, 4096)
        path.write_bytes(b"x" * 65536)
    before = path.read_bytes()
    with pytest.raises(SourceFactBudgetExceeded):
        output._replay_work(replay_owner)
    assert root.exists() and path.read_bytes() == before


@pytest.mark.parametrize("mode", [0o400, 0o600, 0o644])
def test_exact_disposable_writer_modes_can_be_reset(replay_owner, mode):
    root, path = _abandoned(replay_owner)
    path.chmod(mode)
    output._replay_work(replay_owner)
    assert not root.exists()


def test_replaced_output_root_cannot_remove_original_scratch(replay_owner):
    root, path = _abandoned(replay_owner)
    previous = replay_owner.root.with_name("old-output")
    replay_owner.root.rename(previous)
    replay_owner.root.mkdir(mode=0o700)
    with pytest.raises(FamilyBlockUnavailable, match="identity"):
        output._replay_work(replay_owner)
    assert (previous / root.name / path.name).exists()


def test_old_uuid_and_unknown_sibling_remain_outside_retirement_policy(replay_owner):
    original = retirement.RetiredFilePolicy(replay_owner.source.owner_lock.root)
    original.work(replay_owner.root / output._REPLAY_SLOT)
    assert original.is_directory("output/source-replay")
    assert not original.is_directory("output/source-replay-" + "a" * 32)
    assert not original.is_directory("output/source-replay-other")
    historical = replay_owner.root / ("source-replay-" + "a" * 32)
    historical.mkdir(mode=0o700)
    preserved = historical / "run-000000000.mpk"
    preserved.write_bytes(b"historical private scratch")
    output._replay_work(replay_owner)
    assert preserved.read_bytes() == b"historical private scratch"
    (replay_owner.root / "unknown").mkdir(mode=0o700)
    root, path = _abandoned(replay_owner)
    with pytest.raises(FamilyBlockUnavailable, match="unknown"):
        output._replay_work(replay_owner)
    assert root.exists() and path.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["lease", "cancel", "deadline"])
async def test_native_authority_failure_precedes_any_scratch_reset(replay_owner, monkeypatch, fault):
    root, path = _abandoned(replay_owner)
    source = replay_owner.root / "source-families"
    source.mkdir(mode=0o700)
    artifact = str(source), output._directory(source), astuple(_LIMITS)
    replay_owner.source.context = SimpleNamespace(request=object(), build_id=11)

    @asynccontextmanager
    async def authority(*_args):
        yield object(), object()

    guard = AsyncMock(side_effect=FamilyBlockUnavailable("native " + fault))
    monkeypatch.setattr(output, "_page_session", authority)
    monkeypatch.setattr(output, "_native", guard)
    with pytest.raises(FamilyBlockUnavailable, match="native " + fault):
        await output._reopen_prepared(None, replay_owner, artifact)
    guard.assert_awaited_once()
    assert root.exists() and path.exists()


@pytest.mark.asyncio
async def test_original_prepared_files_survive_cold_replay_and_terminal_census(replay_owner, tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        work = PlanWork(replay_owner.root / "source-families", output.remaining(replay_owner), lambda _path: None)
        with values.prepared_source_family_inputs(native.context, native.admission, native.plan, work=work) as original:
            descriptor = values.prepared_source_descriptor(original)
            expected = _prepared_rows(original)
            originals_by_path = {
                path: (path.stat(), path.read_bytes()) for path in (original.selected, original.occurrences)
            }
            replay_owner.source.context = SimpleNamespace(request=native.context.request, build_id=11)
            replay_owner.source.admission = native.admission
            replay_owner.retained = SimpleNamespace(
                context=SimpleNamespace(common=native.context), final_plan=native.plan
            )
            root, _path = _abandoned(replay_owner)

            @asynccontextmanager
            async def authority(*_args):
                yield object(), object()

            monkeypatch.setattr(output, "_page_session", authority)
            monkeypatch.setattr(output, "_native", AsyncMock())
            await output._reopen_prepared(None, replay_owner, descriptor)
            assert root.is_dir() and _prepared_rows(replay_owner.prepared) == expected
            assert values.prepared_source_descriptor(replay_owner.prepared) == descriptor
            assert {path: (path.stat(), path.read_bytes()) for path in originals_by_path} == originals_by_path
            policy = retirement.RetiredFilePolicy(replay_owner.source.owner_lock.root)
            cells = [None] * 8
            cells[3] = str(replay_owner.root), output._directory(replay_owner.root)
            cells[4], cells[5] = (), (None, descriptor, None, None, None, None)
            cells[7] = (str(policy.root),)
            retirement._output_slots(policy, cells)
            assert policy.is_directory("output/source-replay")
            assert policy.file_modes("output/source-replay/run-000000000.mpk") == (0o400, 0o600, 0o644)
            assert policy.file_modes("output/source-families/run-000000000.mpk") == (0o400, 0o600)
            assert policy.names("output/source-replay", ["run-000000000.mpk", "foreign"]) == {"run-000000000.mpk"}
            await replay_owner._stack.aclose()
            assert not root.exists()
            assert {path: (path.stat(), path.read_bytes()) for path in originals_by_path} == originals_by_path


@pytest.mark.asyncio
async def test_protected_cold_owner_keeps_pin_without_reopening_key_spool(tmp_path, monkeypatch):
    entered, exited = [], []
    base = object()

    @asynccontextmanager
    async def pin(*_args, **_kwargs):
        entered.append(True)
        try:
            yield base
        finally:
            exited.append(True)

    def keys(*_args, **_kwargs):
        pytest.fail("protected ready metadata must not stage a duplicate base-key spool")

    job = SimpleNamespace(
        request=object(),
        work=SimpleNamespace(limits=_LIMITS),
        base=None,
        base_context=None,
        base_keys=None,
        _source_custody=object(),
        source=object(),
        _stack=AsyncExitStack(),
    )
    monkeypatch.setattr(jobs, "pinned_base", pin)
    monkeypatch.setattr(jobs, "pinned_base_query_keys", keys)
    await jobs._open_originals(None, job)
    await jobs._open_originals(None, job)
    assert job.base is base and job.base_keys is None and entered == [True]
    await job._stack.aclose()
    assert exited == [True] and not list(tmp_path.iterdir())
