# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact retired inventory refuses foreign entries and survives partial unlink."""

import os
from dataclasses import replace

import pytest

from process.custom_import import family_shard_retirement_files as files
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_shard_owner_lock import acquire_owner_job_lock


class _Policy:
    def file_modes(self, relative):
        return (0o400, 0o600)

    def is_directory(self, relative):
        return relative in {"source", "source/ready", "source/allocation-journal"}

    def names(self, relative, names):
        allowed_by_directory = {
            "": {".owner-job.lock", "source"},
            "source": {"ready", "allocation-journal"},
            "source/ready": {"source-ready.mpk"},
            "source/allocation-journal": {"owner.mpk"},
        }
        return set(names) & allowed_by_directory.get(relative, set())


@pytest.fixture
def retired(tmp_path):
    root, state = tmp_path / "owner", tmp_path / "retirement"
    root.mkdir(mode=0o700)
    state.mkdir(mode=0o700)
    lock = acquire_owner_job_lock(root, identity=(root.stat().st_dev, root.stat().st_ino))
    for name in ("source", "source/ready", "source/allocation-journal"):
        (root / name).mkdir(mode=0o700)
    for name in ("source/ready/source-ready.mpk", "source/allocation-journal/owner.mpk"):
        path = root / name
        path.write_bytes(name.encode())
        path.chmod(0o400)
    context = files.RetirementFiles(
        root, state, "a" * 64, files._directory(root), files._directory(state), lock.file_identity, 1024**2
    )
    try:
        yield context, lock
    finally:
        lock.close()


def test_inventory_precedes_any_unlink_and_preserves_descriptors_until_last(retired, monkeypatch):
    context, lock = retired
    entries, digest = files.seal_inventory(context, _Policy(), lock.check)
    assert len(entries) == 6 and len(digest) == 32
    original = files._remove_entry
    removed_paths = []

    def remove(context, entry, by_path, check):
        assert files.read_inventory(context) == (entries, digest)
        removed_paths.append(entry[0])
        return original(context, entry, by_path, check)

    monkeypatch.setattr(files, "_remove_entry", remove)
    assert files.consume_inventory(context, lock.check) == digest
    assert removed_paths[:2] == ["source/ready/source-ready.mpk", "source/allocation-journal/owner.mpk"]
    assert set(os.listdir(context.root)) == {".owner-job.lock"}
    lock.remove_terminal_root()
    assert not context.root.exists()


@pytest.mark.parametrize("problem", ["unknown_file", "unknown_root", "symlink", "hardlink", "wrong_mode", "budget"])
def test_census_refuses_every_unknown_or_foreign_entry_before_seal(retired, problem):
    context, lock = retired
    path = context.root / "source/ready/source-ready.mpk"
    match problem:
        case "unknown_file":
            (path.parent / "unexpected.mpk").write_bytes(b"retain")
        case "unknown_root":
            (context.root / "borrow-deadbeef").mkdir(mode=0o700)
        case "symlink":
            path.unlink()
            path.symlink_to(context.root.parent / "foreign")
        case "hardlink":
            os.link(path, context.root.parent / "foreign")
        case "wrong_mode":
            path.chmod(0o644)
        case _:
            context = replace(context, maximum_bytes=1)
    with pytest.raises(Exception):
        files.seal_inventory(context, _Policy(), lock.check)
    assert not list(context.state.iterdir())
    assert (context.root / "source/allocation-journal/owner.mpk").exists()


def test_partial_retry_uses_same_sealed_inventory_and_missing_entries_only(retired, monkeypatch):
    context, lock = retired
    entries, digest = files.seal_inventory(context, _Policy(), lock.check)
    original = files._remove_entry
    removed_entries = []

    def interrupted(context, entry, by_path, check):
        original(context, entry, by_path, check)
        removed_entries.append(entry)
        if len(removed_entries) == 1:
            raise ConnectionError("synthetic stop after unlink")

    monkeypatch.setattr(files, "_remove_entry", interrupted)
    with pytest.raises(ConnectionError):
        files.consume_inventory(context, lock.check)
    assert files.read_inventory(context) == (entries, digest)
    monkeypatch.setattr(files, "_remove_entry", original)
    assert files.consume_inventory(context, lock.check) == digest
    assert set(os.listdir(context.root)) == {".owner-job.lock"}


@pytest.mark.parametrize(
    "problem", ["changed_file", "extra_file", "changed_directory", "changed_state", "native_identity"]
)
def test_retry_refuses_drift_before_deleting_any_other_entry(retired, problem):
    context, lock = retired
    files.seal_inventory(context, _Policy(), lock.check)
    target = context.root / "source/ready/source-ready.mpk"
    if problem == "changed_file":
        target.chmod(0o600)
        target.write_bytes(b"different")
    elif problem == "extra_file":
        (target.parent / "new").write_bytes(b"unknown")
    elif problem == "changed_directory":
        target.parent.rename(target.parent.with_name("old"))
        target.parent.mkdir(mode=0o700)
    elif problem == "changed_state":
        context.state.rename(context.state.with_name("old"))
        context.state.mkdir(mode=0o700)
    else:
        context = replace(context, native_sha256="b" * 64)
    with pytest.raises(Exception):
        files.consume_inventory(context, lock.check)
    assert (context.root / "source/allocation-journal/owner.mpk").exists()


def test_unsealed_inventory_is_retained_and_never_authorizes_deletion(retired):
    context, lock = retired
    (context.state / "inventory.mpk").write_bytes(b"partial")
    (context.state / "inventory.mpk").chmod(0o600)
    with pytest.raises(Exception):
        files.seal_inventory(context, _Policy(), lock.check)
    assert (context.root / "source/ready/source-ready.mpk").exists()


@pytest.fixture
def producer(retired):
    """Synthetic committed scope with real protected files and original lifetime lock."""
    from types import SimpleNamespace

    from process.custom_import import family_shard_terminal_files as terminal
    from tests.test_control_owner_handoff import _profile

    context, lock = retired
    profile, parent = _profile()
    native = SimpleNamespace(scope=(1, b"scope"), head=SimpleNamespace(revision=3, sha256=b"h" * 32), sha256="a" * 64)
    original = terminal.original_context(context.root, parent, profile, native, lock)
    return SimpleNamespace(
        context=context, lock=lock, parent=parent, profile=profile, native=native, original=original, api=terminal
    )


@pytest.mark.parametrize("name", ["producer-inventory.mpk", "producer-sealed.mpk"])
@pytest.mark.parametrize("boundary", ["pending", "renamed"])
def test_producer_torn_seal_requires_full_original_reproof(producer, monkeypatch, name, boundary):
    value = producer
    write = files._atomic_write

    def interrupted(directory, target, content):
        if target == name:
            if boundary == "pending":
                files._write_once(directory, target + ".pending", content[:7])
            else:
                write(directory, target, content)
            raise OSError("synthetic producer seal interruption")
        write(directory, target, content)

    monkeypatch.setattr(files, "_atomic_write", interrupted)
    with pytest.raises(OSError):
        value.api.seal(value.original, _Policy(), value.lock.check)
    assert (value.context.root / "source/ready/source-ready.mpk").exists()
    monkeypatch.setattr(files, "_atomic_write", write)
    entries, digest = value.api.seal(value.original, _Policy(), value.lock.check)
    assert value.api.read(value.original) == (entries, digest)
    assert set(os.listdir(value.context.state)) == {value.api.INVENTORY, value.api.SEALED}


@pytest.mark.parametrize("changed", ["parent", "profile", "scope", "head", "root", "state", "lock", "budget", "native"])
def test_producer_rebind_refuses_every_changed_custody_join(producer, changed):
    from copy import deepcopy
    from types import SimpleNamespace

    custody_case = producer
    custody_case.api.seal(custody_case.original, _Policy(), custody_case.lock.check)
    parent, profile, native = (
        deepcopy(custody_case.parent),
        deepcopy(custody_case.profile),
        deepcopy(custody_case.native),
    )
    context = custody_case.context
    match changed:
        case "parent":
            parent["descriptor"]["controller_execution_id"] = "another-execution"
        case "profile":
            profile["work_mem_mb"] += 1
        case "scope":
            native.scope = (2, b"scope")
        case "head":
            native.head = SimpleNamespace(revision=4, sha256=b"j" * 32)
        case "root":
            context = replace(context, root_identity=(context.root_identity[0], 0, *context.root_identity[2:]))
        case "state":
            context = replace(context, state_identity=(context.state_identity[0], 0, *context.state_identity[2:]))
        case "lock":
            context = replace(context, lock_identity=(context.lock_identity[0], 0))
        case "native":
            context = replace(context, native_sha256="b" * 64)
        case _:
            context = replace(context, maximum_bytes=1)
    with pytest.raises((ValueError, OSError, FamilyBlockUnavailable)):
        custody_case.api.rebind(context, parent, profile, native, custody_case.lock.check)
    assert (custody_case.context.root / "source/ready/source-ready.mpk").exists()
    assert not (custody_case.context.state / "sealed").exists()


@pytest.mark.parametrize("name", ["inventory.mpk", "sealed"])
@pytest.mark.parametrize("boundary", ["pending", "renamed"])
def test_torn_retirement_rebind_keeps_producer_proof_and_resumes(producer, monkeypatch, name, boundary):
    value = producer
    entries, _digest = value.api.seal(value.original, _Policy(), value.lock.check)
    (value.context.root / "source/ready/source-ready.mpk").unlink()
    write = files._atomic_write

    def interrupted(directory, target, content):
        if target == name:
            if boundary == "pending":
                files._write_once(directory, target + ".pending", content[:7])
            else:
                write(directory, target, content)
            raise OSError("synthetic rebind interruption")
        write(directory, target, content)

    monkeypatch.setattr(files, "_atomic_write", interrupted)
    with pytest.raises(OSError):
        value.api.rebind(value.context, value.parent, value.profile, value.native, value.lock.check)
    assert value.api.read(value.original)[0] == entries
    monkeypatch.setattr(files, "_atomic_write", write)
    if not value.api.is_rebind_resumed(value.context, value.lock.check):
        value.api.rebind(value.context, value.parent, value.profile, value.native, value.lock.check)
    assert files.read_inventory(value.context)[0] == entries
    assert set(os.listdir(value.context.state)) == {"inventory.mpk", "sealed"}
    files.consume_inventory(value.context, value.lock.check)
    assert set(os.listdir(value.context.root)) == {".owner-job.lock"}


@pytest.mark.parametrize("name", ["producer-inventory.mpk", "producer-sealed.mpk"])
def test_rebind_cleanup_failure_uses_durable_actual_retirement_inventory(producer, monkeypatch, name):
    value = producer
    value.api.seal(value.original, _Policy(), value.lock.check)
    unlink = os.unlink

    def interrupted(target, *args, **kwargs):
        unlink(target, *args, **kwargs)
        if target == name:
            raise OSError("synthetic evidence unlink interruption")

    monkeypatch.setattr(os, "unlink", interrupted)
    with pytest.raises(OSError):
        value.api.rebind(value.context, value.parent, value.profile, value.native, value.lock.check)
    monkeypatch.setattr(os, "unlink", unlink)
    assert value.api.is_rebind_resumed(value.context, value.lock.check)
    files.consume_inventory(value.context, value.lock.check)
    assert set(os.listdir(value.context.root)) == {".owner-job.lock"}


@pytest.mark.parametrize("boundary", ["drained", "lock_unlinked", "root_removed", "inventory_removed", "seal_removed"])
def test_producer_final_removal_crashes_preserve_exact_retirement_recovery(producer, monkeypatch, boundary):
    custody_case = producer
    entries, _digest = custody_case.api.seal(custody_case.original, _Policy(), custody_case.lock.check)
    files._consume_entries(custody_case.original, entries, custody_case.lock.check)
    custody_case.api.mark_drained(custody_case.original, custody_case.lock.check)
    if boundary == "lock_unlinked":
        os.unlink(custody_case.context.root / ".owner-job.lock")
        custody_case.lock.close()
    elif boundary in {"root_removed", "inventory_removed", "seal_removed"}:
        custody_case.lock.remove_terminal_root()
    if boundary in {"inventory_removed", "seal_removed"}:
        unlink = os.unlink
        target_name = custody_case.api.INVENTORY if boundary == "inventory_removed" else custody_case.api.SEALED

        def interrupted(target, *args, **kwargs):
            unlink(target, *args, **kwargs)
            if target == target_name:
                raise OSError("synthetic evidence removal interruption")

        monkeypatch.setattr(os, "unlink", interrupted)
        with pytest.raises(OSError):
            custody_case.api.finish(custody_case.original)
        monkeypatch.setattr(os, "unlink", unlink)
    if boundary == "seal_removed":
        assert not list(custody_case.context.state.iterdir())
        custody_case.api.finish(custody_case.original)
        assert not custody_case.context.state.exists()
        return
    check = custody_case.lock.check if boundary == "drained" else lambda: None
    custody_case.api.rebind(custody_case.context, custody_case.parent, custody_case.profile, custody_case.native, check)
    assert not set(os.listdir(custody_case.context.state)) & {custody_case.api.INVENTORY, custody_case.api.SEALED}
    if boundary != "drained":
        assert (custody_case.context.state / "drained").exists()


def test_producer_metadata_is_charged_inside_the_existing_disk_allowance(producer):
    value = producer
    _entries, owned_bytes = files._census_entries(value.original, _Policy(), value.lock.check)
    context = replace(value.original, maximum_bytes=owned_bytes + 1)
    with pytest.raises(FamilyBlockUnavailable):
        value.api.seal(context, _Policy(), value.lock.check)
    assert not list(context.state.iterdir())
    assert (context.root / "source/ready/source-ready.mpk").exists()


def test_extra_producer_state_file_refuses_before_any_evidence_or_data_removal(producer):
    value = producer
    value.api.seal(value.original, _Policy(), value.lock.check)
    unknown = value.context.state / "unexpected"
    unknown.write_bytes(b"retain")
    unknown.chmod(0o600)
    with pytest.raises(FamilyBlockUnavailable):
        value.api.rebind(value.context, value.parent, value.profile, value.native, value.lock.check)
    assert unknown.exists() and (value.context.state / value.api.SEALED).exists()
    assert (value.context.root / "source/ready/source-ready.mpk").exists()


def test_original_root_and_state_cannot_move_together_under_a_replacement_parent(producer):
    value = producer
    value.api.seal(value.original, _Policy(), value.lock.check)
    replacement = value.context.root.parent / "replacement"
    replacement.mkdir(mode=0o700)
    value.context.root.rename(replacement / "owner")
    value.context.state.rename(replacement / "retirement")
    moved = replace(value.context, root=replacement / "owner", state=replacement / "retirement")
    with pytest.raises(FamilyBlockUnavailable):
        value.api.rebind(moved, value.parent, value.profile, value.native, lambda: None)
    assert (moved.root / "source/ready/source-ready.mpk").exists()
    assert not (moved.state / "sealed").exists()
