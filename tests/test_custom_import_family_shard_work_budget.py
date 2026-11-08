# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Scratch entry reservations preserve raw framing and exact cleanup custody."""

import os
import shutil
from contextlib import closing
from pathlib import Path

import pytest

from process.custom_import import family_shard_facts as facts
from process.custom_import import family_shard_source_journal as journal_api
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded, SourceFactLimits
from process.custom_import.runner_types import CancellationRequested
from tests.test_custom_import_family_shard_recovery_budget import retained as retained


def _work(tmp_path, *, units=128, check=None):
    filesystem = os.statvfs(tmp_path)
    unit = max(512, filesystem.f_frsize or filesystem.f_bsize)
    limits = SourceFactLimits(units * unit, 4 * unit, unit)
    return facts._Work(tmp_path / "work", limits, lambda _path: None, check=check)


def test_empty_paths_consume_allowance_before_returning_another_name(tmp_path):
    work = _work(tmp_path, units=12)
    try:
        while True:
            previous = dict(work.paths), work.remaining_bytes
            try:
                path = work.path()
            except SourceFactBudgetExceeded:
                assert (work.paths, work.remaining_bytes) == previous
                break
            path.touch()
        assert len(work.paths) == 5 and work.used == 0 and work.remaining_bytes == 0
        unit = work._entry_charge // 2
        actual = sum(journal_api._entry_cost(path.lstat(), unit) for path in (work.root, *work.paths))
        assert actual <= work.limits.max_spool_bytes
    finally:
        work.close()


def test_closed_rename_and_sort_keep_raw_bytes_and_entry_credit(tmp_path):
    work = _work(tmp_path, units=1024)
    try:
        values = [(index % 13, index) for index in range(255, -1, -1)]
        with closing(facts._sorted(work, values, key=lambda row: row)) as rows:
            path = work.write(rows)
        raw_used, remaining = work.used, work.remaining_bytes
        assert list(work.rows(path)) == sorted(values)
        ready = path.with_name("closed-plan.mpk")
        path.rename(ready)
        work.paths[ready] = work.paths.pop(path)
        assert work.used == raw_used and work.remaining_bytes == remaining
        work.remove(ready)
        assert not work.paths and work.used == 0
        assert work.remaining_bytes == work.limits.max_spool_bytes - work._entry_charge
    finally:
        work.close()


def test_replay_spills_and_close_releases_only_its_entry_and_raw_bytes(tmp_path):
    work = _work(tmp_path)
    try:
        before = work.remaining_bytes
        replay = facts._Replay(work)
        values = [("opaque-source-bytes", b"x" * 1000)] * 9
        for value in values:
            replay.append(value)
        assert replay.file._rolled
        assert list(replay.rows()) == values
        assert before - work.remaining_bytes == replay.size + work._entry_charge
        replay.close()
        assert work.remaining_bytes == before and work.used == 0
        replay.close()
        assert work.remaining_bytes == before
    finally:
        work.close()


def test_failed_replay_open_keeps_provisional_entry_reserved(tmp_path, monkeypatch):
    work = _work(tmp_path)
    before = work.remaining_bytes

    def fail_open(**_keywords):
        raise OSError("injected replay open failure")

    monkeypatch.setattr(facts.tempfile, "SpooledTemporaryFile", fail_open)
    try:
        with pytest.raises(OSError, match="replay open"):
            facts._Replay(work)
        assert work.remaining_bytes == before - work._entry_charge and work.used == 0
    finally:
        work.close()


def test_failed_path_open_keeps_its_unwritten_entry_reserved(tmp_path, monkeypatch):
    work = _work(tmp_path)
    before = work.remaining_bytes

    def refuse_open(_path, *_arguments, **_keywords):
        raise OSError("injected file open failure")

    try:
        with monkeypatch.context() as patch:
            patch.setattr(Path, "open", refuse_open)
            with pytest.raises(OSError, match="file open"):
                work.write([("never written", 1)])
        assert work.remaining_bytes == before - work._entry_charge
        assert work.used == 0 and len(work.paths) == 1
        assert not tuple(work.root.iterdir())
    finally:
        work.close()


def test_partial_mkdir_keeps_registered_root_and_entry_custody(tmp_path, monkeypatch):
    root = tmp_path / "partial"
    unit = max(512, os.statvfs(tmp_path).f_frsize)
    work = facts._Work.__new__(facts._Work)
    registered_paths = []
    original = Path.mkdir

    def partial(path, *arguments, **keywords):
        original(path, *arguments, **keywords)
        raise OSError("injected response loss after mkdir")

    try:
        with monkeypatch.context() as patch:
            patch.setattr(Path, "mkdir", partial)
            with pytest.raises(OSError, match="after mkdir"):
                facts._Work.__init__(work, root, SourceFactLimits(32 * unit, unit, unit), registered_paths.append)
        assert registered_paths == [root] and root.is_dir()
        assert work._entry_bytes == 2 * unit and work.used == 0
    finally:
        if root.exists():
            shutil.rmtree(root)


def test_unconfirmed_replay_close_retains_entry_until_exact_retry(tmp_path, monkeypatch):
    work = _work(tmp_path)
    before = work.remaining_bytes

    class File:
        closed = False
        refuse = True

        def close(self):
            if not self.refuse:
                self.closed = True

    file = File()
    monkeypatch.setattr(facts.tempfile, "SpooledTemporaryFile", lambda **_keywords: file)
    try:
        replay = facts._Replay(work)
        with pytest.raises(ValueError, match="unconfirmed"):
            replay.close()
        assert work.remaining_bytes == before - work._entry_charge
        file.refuse = False
        replay.close()
        assert work.remaining_bytes == before
    finally:
        work.close()


@pytest.mark.parametrize("damage", ["unlink", "directory"])
def test_unconfirmed_removal_retains_raw_and_entry_charge(tmp_path, monkeypatch, damage):
    work = _work(tmp_path)
    path = work.write([("original bytes", 42)])
    before = work.remaining_bytes, work.used, dict(work.paths)
    with monkeypatch.context() as patch:
        if damage == "unlink":
            patch.setattr(Path, "unlink", lambda _path: None)

            def operation():
                work.remove(path)

        else:
            patch.setattr(facts.shutil, "rmtree", lambda _path: None)
            operation = work.close
        with pytest.raises(ValueError, match="unconfirmed"):
            operation()
    assert (work.remaining_bytes, work.used, work.paths) == before
    work.close()


def test_cancellation_refuses_before_new_entry_is_reserved(tmp_path):
    is_stopped = False

    def current():
        if is_stopped:
            raise CancellationRequested("injected cancellation before new file")

    work = _work(tmp_path, check=current)
    try:
        before = work.remaining_bytes, dict(work.paths)
        is_stopped = True
        with pytest.raises(CancellationRequested, match="before new file"):
            work.path()
        assert (work.remaining_bytes, work.paths) == before
    finally:
        work.close()


def test_entry_exhaustion_precedes_directory_registration(tmp_path):
    registered_paths = []
    unit = max(512, os.statvfs(tmp_path).f_frsize)
    with pytest.raises(SourceFactBudgetExceeded, match="entry creation"):
        facts._Work(tmp_path / "never-created", SourceFactLimits(unit, unit, unit), registered_paths.append)
    assert not registered_paths and not (tmp_path / "never-created").exists()


def test_original_raw_singleton_guard_is_unchanged(tmp_path):
    work = _work(tmp_path)
    try:
        path = work.path()
        remaining = work.remaining_bytes
        with path.open("xb") as target, pytest.raises(SourceFactBudgetExceeded, match="singleton"):
            work.frame(target, b"x" * work.limits.max_record_bytes, path)
        assert work.remaining_bytes == remaining and work.used == 0
    finally:
        work.close()


def test_journal_aggregate_entry_reservation_is_not_double_charged(retained):
    retained.journal.reconcile_budget()
    before = retained.journal.budget().used
    work = journal_api.SourceAllocationWork(
        retained.journal, retained.journal.root / "outputs" / "new-work", retained.limits
    )
    try:
        path = work.write([("source row", 7)])
        expected = before + work.used + 2 * work._entry_charge
        assert retained.journal.budget().used == expected
        work.remove(path)
        assert retained.journal.budget().used == before + work._entry_charge
        with facts._Replay(work) as replay:
            replay.append(("journal replay", 3))
            assert retained.journal.budget().used == before + replay.size + 2 * work._entry_charge
        assert retained.journal.budget().used == before + work._entry_charge
    finally:
        work.close()
    assert retained.journal.budget().used == before
