# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Terminal parent cleanup consumes only empty, authenticated attempt locks."""

import os

import pytest

from api import custom_import_owner_worker as owner
from process.custom_import import family_shard_handoff as handoff
from process.custom_import import family_shard_owner_attempt_files as files
from tests.owner_attempt_support import recovery


def test_terminal_parent_cleanup_removes_only_lock_created_by_unsent_check(tmp_path, monkeypatch):
    custody = recovery(tmp_path, monkeypatch)
    receipt = owner.custody_receipt(custody.parent)
    lock = files.attempt_lock(custody.root)
    lock.close()
    assert set(os.listdir(custody.root / "attempts")) == {files._NAME}
    owner.cleanup(custody.parent, receipt)
    assert not custody.root.exists()
    owner.cleanup(custody.parent, receipt)


@pytest.mark.parametrize("state", ["child", "unknown", "symlink", "held", "changed_lock", "changed_parent"])
def test_ambiguous_attempt_custody_keeps_original_inputs(tmp_path, monkeypatch, state):
    custody = recovery(tmp_path, monkeypatch)
    receipt = owner.custody_receipt(custody.parent)
    lock = files.attempt_lock(custody.root)
    if state != "held":
        lock.close()
    attempt_root = custody.root / "attempts"
    match state:
        case "child":
            (attempt_root / ("e" * 64)).mkdir(mode=0o700)
        case "unknown":
            (attempt_root / "unknown").touch()
        case "symlink":
            (attempt_root / ("e" * 64)).symlink_to(tmp_path)
        case "changed_lock":
            (attempt_root / files._NAME).chmod(0o400)
        case "changed_parent":
            receipt = receipt | {"root_inode": receipt["root_inode"] + 1}
    try:
        with pytest.raises((ValueError, OSError)):
            owner.cleanup(custody.parent, receipt)
    finally:
        lock.close()
    assert (custody.root / "bearer").read_bytes() == b"x" * 43
    assert (custody.root / "committed").exists()
    assert attempt_root.exists()


def test_nonempty_work_is_refused_before_attempt_lock_removal(tmp_path, monkeypatch):
    custody = recovery(tmp_path, monkeypatch)
    lock = files.attempt_lock(custody.root)
    lock.close()
    (custody.root / "work" / "owner").mkdir(mode=0o700)
    with pytest.raises(ValueError):
        owner.cleanup(custody.parent, owner.custody_receipt(custody.parent))
    assert (custody.root / "attempts" / files._NAME).exists()
