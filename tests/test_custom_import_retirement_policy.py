# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Actual producer files admit only fixed slots and protected recorded roots."""

import os
import shutil

import pytest

from process.custom_import import family_shard_retirement_files as files
from process.custom_import import family_shard_retirement_policy as policy
from process.custom_import import family_shard_source_stage as source
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from tests.test_custom_import_family_shard_cold_source import ready_boundary


def _inventory_context(boundary, original):
    """Use actual producer directories with an explicitly synthetic native digest."""
    state = boundary.root.parent / "retirement"
    state.mkdir(mode=0o700)
    return files.RetirementFiles(
        boundary.root,
        state,
        "a" * 64,
        files._directory(boundary.root),
        files._directory(state),
        original.lock_identity,
        128 * 1024**2,
    )


@pytest.mark.asyncio
async def test_original_source_ready_and_recorded_output_form_closed_file_inventory(ready_boundary):
    boundary, output = ready_boundary
    await source._anchor_output(None, boundary.job.source, output)
    snapshot = await boundary.protected()
    admitted, original = policy.original_policy(
        snapshot, boundary.root, boundary.job.source_limits, boundary.job.projection_work_mem_mb, boundary.job.current
    )
    context = _inventory_context(boundary, original)
    for path, directories, names in os.walk(boundary.root):
        relative = str(files.Path(path).relative_to(boundary.root))
        relative = "" if relative == "." else relative
        assert set(directories + names) == admitted.names(relative, directories + names), relative
    entries = files._census(context, admitted, boundary.job.current)
    assert any(entry[0].startswith("source/closed-output/") for entry in entries)
    assert any(entry[0].startswith("source/allocation-journal/checkpoints/") for entry in entries)
    assert admitted.is_directory("source/closed-output")
    assert not admitted.is_directory("source/closed-output-unknown")


@pytest.mark.asyncio
@pytest.mark.parametrize("removed", ["ready_file", "recorded_output"])
async def test_unsealed_partial_producer_cannot_create_retirement_inventory(ready_boundary, removed):
    boundary, output = ready_boundary
    await source._anchor_output(None, boundary.job.source, output)
    snapshot = await boundary.protected()
    if removed == "ready_file":
        next((boundary.root / "source/ready/input").glob("shard-*.mpk")).unlink()
    else:
        shutil.rmtree(output._work.root)
    with pytest.raises((ValueError, FamilyBlockUnavailable, FileNotFoundError)):
        policy.original_policy(
            snapshot,
            boundary.root,
            boundary.job.source_limits,
            boundary.job.projection_work_mem_mb,
            boundary.job.current,
        )
    assert (boundary.root / "source/allocation-journal/checkpoints").is_dir()
    boundary.job._owner_lock.check()


@pytest.mark.asyncio
async def test_sealed_inventory_resumes_after_original_checkpoint_cleanup(ready_boundary, monkeypatch):
    boundary, output = ready_boundary
    await source._anchor_output(None, boundary.job.source, output)
    admitted, original = policy.original_policy(
        await boundary.protected(),
        boundary.root,
        boundary.job.source_limits,
        boundary.job.projection_work_mem_mb,
        boundary.job.current,
    )
    context = _inventory_context(boundary, original)
    entries, digest = files.seal_inventory(context, admitted, boundary.job._owner_lock.check)
    await boundary.job.source._stack.aclose()
    await boundary.job._keys_stack.aclose()
    await boundary.job._stack.aclose()
    assert not (boundary.root / "source/allocation-journal").exists()
    remove = files._remove_entry

    def interrupted(context, entry, by_path, check):
        remove(context, entry, by_path, check)
        if entry[0].startswith("source/ready/input/shard-"):
            raise OSError("synthetic interruption after inventoried unlink")

    monkeypatch.setattr(files, "_remove_entry", interrupted)
    with pytest.raises(OSError, match="after inventoried unlink"):
        files.consume_inventory(context, boundary.job._owner_lock.check)
    assert files.read_inventory(context) == (entries, digest)
    monkeypatch.setattr(files, "_remove_entry", remove)
    assert files.consume_inventory(context, boundary.job._owner_lock.check) == digest
    assert set(os.listdir(boundary.root)) == {".owner-job.lock"}
    # The retired consumer has closed every SOURCE artifact; release the live test owner.
    boundary.job.source._closed = True
    await boundary.job.close_terminal()
    assert not boundary.root.exists()


def test_matching_file_form_cannot_adopt_unknown_directory(tmp_path):
    root = tmp_path / "owner"
    root.mkdir(mode=0o700)
    admitted = policy.RetiredFilePolicy(root)
    admitted.work(root / "source")
    assert "run-000000001.mpk" in admitted.names("source", ["run-000000001.mpk"])
    assert not admitted.is_directory("source/run-000000001.mpk")
    unknown = root / "borrow-" / "output.mpk"
    assert not admitted.is_directory(str(unknown.parent.relative_to(root)))


@pytest.mark.parametrize("name", ["../foreign", "/foreign", "source/../../foreign"])
def test_policy_cannot_admit_external_or_parent_traversal_roots(tmp_path, name):
    root = tmp_path / "owner"
    root.mkdir(mode=0o700)
    admitted = policy.RetiredFilePolicy(root)
    with pytest.raises(FamilyBlockUnavailable):
        admitted.work(root / name)


@pytest.mark.asyncio
@pytest.mark.parametrize("problem", ["uncommitted_root", "changed_original", "changed_copy"])
async def test_real_source_copy_proof_refuses_unknown_or_changed_originals(ready_boundary, problem):
    boundary, output = ready_boundary
    await source._anchor_output(None, boundary.job.source, output)
    journal = boundary.root / "source/allocation-journal/outputs"
    if problem == "uncommitted_root":
        (journal / ("metadata-" + "f" * 32)).mkdir(mode=0o700)
    elif problem == "changed_original":
        original = next(journal.glob("metadata-*"))
        next(original.iterdir()).write_bytes(b"different")
    else:
        copied = boundary.root / "source/ready/metadata/families.mpk"
        copied.chmod(0o600)
        copied.write_bytes(b"different")
    with pytest.raises((ValueError, FamilyBlockUnavailable)):
        policy.original_policy(
            await boundary.protected(),
            boundary.root,
            boundary.job.source_limits,
            boundary.job.projection_work_mem_mb,
            boundary.job.current,
        )
    assert (boundary.root / ".owner-job.lock").exists()


async def _bind_completed(boundary, output, monkeypatch):
    """Use real committed files; only terminal database transport is doubled."""
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from process.custom_import import family_shard_terminal_cleanup as terminal
    from tests.test_control_owner_handoff import _profile

    await source._anchor_output(None, boundary.job.source, output)
    snapshot = await boundary.protected()
    profile, parent = _profile()
    native = SimpleNamespace(scope=snapshot.scope, head=snapshot.head, sha256="a" * 64)
    read_native = AsyncMock(return_value=native)
    monkeypatch.setattr(terminal, "read_terminal_scope", read_native)
    boundary.job._terminal_custody = terminal.TerminalCustody(parent, profile, lambda: None)
    boundary.job._terminal_inventory = None
    boundary.job.complete = True
    return native, read_native


def _retired_census_context(original, native):
    """Preserve exact producer filesystem identities while binding actual retirement separately."""
    return files.RetirementFiles(
        original.root,
        original.state,
        native.sha256,
        original.root_identity,
        original.state_identity,
        original.lock_identity,
        original.maximum_bytes,
    )


@pytest.mark.asyncio
async def test_owner_seals_before_first_destructive_callback_and_retirement_resumes_partial_files(
    ready_boundary, monkeypatch
):
    from types import SimpleNamespace

    from process.custom_import import family_shard_handoff as handoff
    from process.custom_import import family_shard_retirement_cli as cli
    from process.custom_import import family_shard_retirement_handoff as custody
    from process.custom_import import family_shard_terminal_files as producer

    boundary, output = ready_boundary
    job = boundary.job
    native, read_native = await _bind_completed(boundary, output, monkeypatch)
    stopped_workers = []

    async def stop(*, terminate):
        assert terminate
        stopped_workers.append(True)

    job.source.runs.append(SimpleNamespace(stop=stop))

    async def interrupted():
        context = job._terminal_inventory
        assert context is not None, "a durable producer census must precede every destructive callback"
        assert stopped_workers, "workers must join before the first destructive callback"
        assert producer.read(context)[0]
        await job.source._stack.aclose()
        await job._keys_stack.aclose()
        await job._stack.aclose()
        assert not (boundary.root / "source/ready/input").exists()
        raise OSError("synthetic cleanup interruption after journal and input deletion")

    job.output_stage = SimpleNamespace(_busy=False, close_terminal=interrupted)
    try:
        with pytest.raises(OSError, match="after journal and input deletion"):
            await job.close_terminal()
        assert not (boundary.root / "source/allocation-journal").exists()
        original = job._terminal_inventory
        context = _retired_census_context(original, native)
        # This fixture's mount slot is named job. Only mount/native transport is
        # doubled; actual producer policy, sealed entries and CLI consumption run.
        monkeypatch.setattr(custody, "context", lambda *_args, **_kwargs: context)
        monkeypatch.setattr(custody, "identity", lambda _document: "b" * 64)
        monkeypatch.setattr(handoff, "OWNER_PARENT", boundary.root.parent)
        job._owner_lock.close()
        bound = job._terminal_custody
        cli._consume({}, native, job.source_limits, job.projection_work_mem_mb, bound.parent, bound.profile)
        assert not boundary.root.exists() and (context.state / "completion.json").exists()
        read_native.assert_awaited_once()
        job._closed = True
    finally:
        job.output_stage = None
        job.complete = False
        job._terminal_custody = None


@pytest.mark.asyncio
async def test_completed_owner_removes_census_only_after_exact_root_cleanup(ready_boundary, monkeypatch):
    boundary, output = ready_boundary
    await _bind_completed(boundary, output, monkeypatch)
    await boundary.job.close_terminal()
    assert not boundary.root.exists()
    assert not (boundary.root.parent / "retirement").exists()
    assert boundary.job._closed


@pytest.mark.asyncio
async def test_incomplete_terminal_cancellation_keeps_existing_same_process_cleanup(ready_boundary, monkeypatch):
    boundary, output = ready_boundary
    _native, read_native = await _bind_completed(boundary, output, monkeypatch)
    boundary.job.complete = False
    await boundary.job.close_terminal()
    read_native.assert_not_awaited()
    assert not boundary.root.exists() and not (boundary.root.parent / "retirement").exists()


@pytest.mark.asyncio
async def test_failed_worker_drain_cannot_seal_or_begin_destructive_cleanup(ready_boundary, monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    boundary, output = ready_boundary
    _native, read_native = await _bind_completed(boundary, output, monkeypatch)
    job = boundary.job
    worker = SimpleNamespace(stop=AsyncMock(side_effect=OSError("synthetic join failure")))
    job.source.runs.append(worker)
    closed = AsyncMock()
    job.output_stage = SimpleNamespace(_busy=False, close_terminal=closed)
    try:
        with pytest.raises(OSError, match="join failure"):
            await job.close_terminal()
        read_native.assert_not_awaited()
        closed.assert_not_awaited()
        assert (boundary.root / "source/allocation-journal/checkpoints").is_dir()
        assert not (boundary.root.parent / "retirement").exists()
    finally:
        job.source.runs.remove(worker)
        job.output_stage = None
        job.complete = False
        job._terminal_custody = None
