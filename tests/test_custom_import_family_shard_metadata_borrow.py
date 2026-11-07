# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Actual sealed metadata remains borrowed; database transport is doubled."""

import os
from dataclasses import replace
from pathlib import Path

import pytest

from process.custom_import import family_shard_metadata as metadata
from process.custom_import import family_shard_source_stage as source
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded, SourceFactLimits
from process.custom_import.family_shard_owner_lock import acquire_owner_job_lock
from process.custom_import.family_shard_plan import PlanWork
from tests.test_custom_import_family_shard_cold_source import ready_boundary
from tests.test_custom_import_family_shard_metadata import _owner, _reopen, _seal, _source, _work


def test_borrowed_budget_owns_no_cleanup_and_keeps_lock_check(tmp_path):
    root = tmp_path / "original"
    root.mkdir(mode=0o700)
    observed = root.stat()
    lock = acquire_owner_job_lock(root, identity=(observed.st_dev, observed.st_ino))
    original = root / "immutable"
    original.write_bytes(b"original")
    original.chmod(0o400)
    work = PlanWork(tmp_path / "unused", SourceFactLimits(65536, 4096, 4096), lambda _path: pytest.fail(), lock.check)
    try:
        reader = metadata._MetadataReadWork(root, work)
        filesystem = os.statvfs(root)
        unit = max(512, filesystem.f_frsize or filesystem.f_bsize)
        assert reader.remaining_bytes == work.limits.max_spool_bytes - 2 * unit
        reader.charge(reader.remaining_bytes)
        with pytest.raises(SourceFactBudgetExceeded):
            reader.charge(1)
        reader.close()
        assert original.read_bytes() == b"original" and not work.root.exists()
        lock.close()
        with pytest.raises(ValueError, match="closed"):
            reader.current()
    finally:
        lock.close()


@pytest.mark.asyncio
async def test_borrowed_metadata_creates_no_work_and_preserves_originals(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch, query_child=True) as (request, allocated, journal):
        _owner(monkeypatch, allocated, journal)
        async with _work(tmp_path, request, allocated, journal) as original:
            sealed = _seal(tmp_path, original, request, allocated, [])
            expected = list(original.family_ids()), list(original.query_keys())
        root = Path(sealed.root)
        before_by_path = {path: (path.stat(), path.read_bytes()) for path in root.iterdir()}
        registered_paths = []
        work = PlanWork(tmp_path / "unused", SourceFactLimits(16 * 1024**2, 1024**2, 4096), registered_paths.append)
        with _reopen(tmp_path, sealed, work=work) as borrowed:
            assert (list(borrowed.family_ids()), list(borrowed.query_keys())) == expected
            assert not work.root.exists() and registered_paths == []
            borrowed.work.close()
            assert list(borrowed.family_ids()) == expected[0]
        assert {path: (path.stat(), path.read_bytes()) for path in before_by_path} == before_by_path
        with pytest.raises(SourceFactBudgetExceeded):
            with _reopen(tmp_path, sealed, work=replace(work, limits=SourceFactLimits(4096, 4096, 4096))):
                pytest.fail("borrowed bytes cannot exceed their original allowance")
        assert {path: (path.stat(), path.read_bytes()) for path in before_by_path} == before_by_path
        assert not work.root.exists() and registered_paths == []


@pytest.mark.asyncio
async def test_source_ready_cold_metadata_creates_no_resume_parent(ready_boundary):
    boundary, _output = ready_boundary
    original = boundary.job.source
    reopened = source.SourceAdmissionStage(
        original.context,
        original.base_keys,
        root=original.root,
        register_cleanup=original.register_cleanup,
        limits=original.limits,
        current=original._current,
        owner_lock=original.owner_lock,
    )
    source.bind_source_custody(reopened, original.custody)
    original_paths = set(boundary.root.rglob("*"))
    try:
        await source._reopen_prepared(None, reopened)
        reopened.allocation_journal = original.allocation_journal
        reopened.metadata = source._reopen_metadata(reopened, await boundary.protected())
        assert reopened.work_root == original.root
        assert set(boundary.root.rglob("*")) == original_paths
        assert list(reopened.metadata.family_ids()) == list(original.metadata.family_ids())
        assert list(reopened.metadata.query_keys()) == list(original.metadata.query_keys())
    finally:
        await reopened._stack.aclose()
    assert set(boundary.root.rglob("*")) == original_paths
