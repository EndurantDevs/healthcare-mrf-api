# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""CMS private acquisition never widens access to existing retained releases."""

import os
import stat
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_npd as cms


@pytest.fixture
def artifact_root(monkeypatch):
    # The durable-root guard rejects system temporary directories.
    with TemporaryDirectory(prefix=".cms-artifact-test-", dir=Path.cwd()) as directory:
        root = Path(directory) / "artifacts"
        root.mkdir()
        monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT", str(root))
        monkeypatch.setattr(cms, "_is_isolated_intake", lambda: True)
        yield root


def test_private_root_preserves_sealed_release_and_reuses_only_its_own_directory(artifact_root):
    sealed = artifact_root / "cms-npd" / "releases" / ("a" * 64)
    sealed.mkdir(parents=True)
    manifest = sealed / "manifest.json"
    manifest.write_bytes(b"sealed CMS release\n")
    manifest.chmod(0o600)
    before = manifest.stat()
    parent_before = artifact_root.stat()

    selected = cms.durable_artifact_root()

    assert selected == artifact_root / "cms-source-worker"
    observed = selected.lstat()
    assert observed.st_uid == os.geteuid()
    assert stat.S_IMODE(observed.st_mode) & 0o777 == 0o700
    assert cms.durable_artifact_root().stat().st_ino == observed.st_ino
    assert manifest.read_bytes() == b"sealed CMS release\n"
    after = manifest.stat()
    for field in ("st_ino", "st_uid", "st_gid", "st_mode", "st_size", "st_mtime_ns", "st_ctime_ns"):
        assert getattr(after, field) == getattr(before, field)
    assert artifact_root.stat().st_uid == parent_before.st_uid
    assert artifact_root.stat().st_gid == parent_before.st_gid
    assert artifact_root.stat().st_mode == parent_before.st_mode


def test_legacy_cms_keeps_existing_root_without_creating_a_worker_directory(artifact_root, monkeypatch):
    monkeypatch.setattr(cms, "_is_isolated_intake", lambda: False)
    assert cms.durable_artifact_root() == artifact_root
    assert list(artifact_root.iterdir()) == []


@pytest.mark.parametrize("kind", ["symlink", "file", "foreign-owner", "group-readable", "not-writable"])
def test_private_root_refuses_unowned_or_shared_paths_without_repair(artifact_root, monkeypatch, kind):
    target = artifact_root / "cms-source-worker"
    if kind == "file":
        target.write_bytes(b"existing")
    elif kind == "symlink":
        other = artifact_root / "other"
        other.mkdir()
        target.symlink_to(other, target_is_directory=True)
    else:
        target.mkdir(mode=0o700)
        if kind == "foreign-owner":
            owner = os.geteuid()
            monkeypatch.setattr(cms.os, "geteuid", lambda: owner + 1)
        elif kind == "group-readable":
            target.chmod(0o750)
        else:
            target.chmod(0o500)
    before = target.lstat()
    with pytest.raises(ValueError, match="cms_npd_worker_artifact_root_unsafe"):
        cms.durable_artifact_root()
    assert target.lstat() == before


def test_unwritable_parent_fails_before_acquisition_without_chmod(artifact_root, monkeypatch):
    original_mkdir = Path.mkdir

    def denied(path, *args, **kwargs):
        if path == artifact_root / "cms-source-worker":
            raise PermissionError("private fixture denies creation")
        return original_mkdir(path, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", denied)
    with pytest.raises(ValueError, match="cms_npd_worker_artifact_root_unavailable"):
        cms.durable_artifact_root()
    assert list(artifact_root.iterdir()) == []


@pytest.mark.asyncio
async def test_isolated_intake_passes_only_the_private_root_to_acquisition(artifact_root, monkeypatch):
    acquire = AsyncMock(return_value={"status": "validated"})
    monkeypatch.setattr(cms, "_run_upstream_check", acquire)
    context, task = {}, {"source_ids": ["cms-npd"], "import_resources": True, "full_refresh": True}
    result = await cms._execute_intake(context, task, "run-synthetic")
    assert result == {"status": "validated"}
    acquire.assert_awaited_once_with(context, task, "run-synthetic", artifact_root / "cms-source-worker")


@pytest.mark.asyncio
async def test_isolated_intake_refuses_shared_root_before_upstream_io(artifact_root, monkeypatch):
    target = artifact_root / "cms-source-worker"
    target.mkdir(mode=0o750)
    acquire = AsyncMock()
    monkeypatch.setattr(cms, "_run_upstream_check", acquire)
    with pytest.raises(ValueError, match="cms_npd_worker_artifact_root_unsafe"):
        await cms._execute_intake(
            {}, {"source_ids": ["cms-npd"], "import_resources": True, "full_refresh": True}, "run-synthetic"
        )
    acquire.assert_not_awaited()
