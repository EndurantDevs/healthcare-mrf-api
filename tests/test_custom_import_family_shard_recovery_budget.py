# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real private files and held locks; resource accounting supplies no native authority."""

import os
import shutil
from dataclasses import astuple, replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from process.custom_import import family_shard_facts as facts
from process.custom_import import family_shard_metadata_journal as metadata_journal
from process.custom_import import family_shard_owner_job as owners
from process.custom_import import family_shard_source_journal as journal_api
from process.custom_import import family_shard_source_stage as stage
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded, SourceFactLimits
from process.custom_import.family_shard_owner_lock import acquire_owner_job_lock
from process.custom_import.family_shard_plan import PlanWork
from process.custom_import.runner_types import CancellationRequested
from tests.test_custom_import_family_shard_source_stage import _owner


@pytest.fixture
def retained(tmp_path):
    root = tmp_path / "job"
    root.mkdir(mode=0o700)
    lock = acquire_owner_job_lock(root, identity=(root.stat().st_dev, root.stat().st_ino))
    with journal_api._census_directory(None, str(root)) as (directory, _observed):
        unit = journal_api._filesystem_unit(directory)
    limits = SourceFactLimits(64 * unit, unit, 2 * unit)
    template, registered = _owner(root), []
    source_limits = replace(
        template.limits, facts=limits, allocation=replace(template.limits.allocation, spool=limits), checkpoint=limits
    )
    context = replace(template.context, owner_profile=(astuple(limits), 1024))
    source_owner = stage.SourceAdmissionStage(
        context,
        lambda: iter(()),
        root=root / "source",
        register_cleanup=registered.append,
        limits=source_limits,
        current=lambda: None,
        owner_lock=lock,
    )
    source_owner._ensure_root()
    journal = journal_api.create_source_allocation_journal(
        context.build_id,
        work=PlanWork(source_owner.root / "allocation-journal", limits, registered.append),
        control_limits=limits,
    )
    custody = journal_api.source_owner_custody(
        lock, source_owner.root, journal, stage.owner_source_profile(source_owner)
    )
    stage.bind_source_custody(source_owner, custody)
    job = owners.OwnerShardJob(
        context.request, work=PlanWork(root, limits, registered.append), source_limits=source_limits
    )
    job._owner_lock, job._source_custody = lock, custody
    try:
        yield SimpleNamespace(
            root=root,
            lock=lock,
            unit=unit,
            limits=limits,
            source=source_owner,
            journal=journal,
            custody=custody,
            job=job,
            registered=registered,
        )
    finally:
        lock.check()
        shutil.rmtree(source_owner.root)
        for path in root.iterdir():
            if path.name != ".owner-job.lock":
                shutil.rmtree(path) if path.is_dir() and not path.is_symlink() else path.unlink()
        lock.remove_terminal_root()


def _remaining(retained, slot="final-plan"):
    return journal_api.retained_work_limits(
        retained.custody,
        retained.lock,
        retained.root / (slot + "-" + "a" * 32),
        slot,
        retained.limits,
        retained.lock.check,
    )


def test_census_uses_allocation_unit(monkeypatch):
    monkeypatch.setattr(journal_api.os, "fstatvfs", lambda _descriptor: SimpleNamespace(f_frsize=4096, f_bsize=1024**2))
    assert journal_api._filesystem_unit(1) == 4096


def test_retained_cost_includes_empty_files_and_allocations(retained):
    original = _remaining(retained)
    previous = retained.root / "final-plan"
    previous.mkdir(mode=0o700)
    (previous / "empty.mpk").touch()
    large = previous / "data.mpk"
    large.write_bytes(b"x" * (retained.unit + 1))
    reduced = _remaining(retained)
    assert reduced.max_spool_bytes < original.max_spool_bytes - 3 * retained.unit
    assert (reduced.max_record_bytes, reduced.memory_bytes) == (
        retained.limits.max_record_bytes,
        retained.limits.memory_bytes,
    )
    assert retained.job.work.limits == retained.limits
    assert retained.custody.profile == stage.owner_source_profile(retained.source)
    unknown = retained.root / "unrelated"
    unknown.mkdir(mode=0o700)
    (unknown / "preserve").write_text("unrelated contents")
    assert _remaining(retained) == reduced
    work = owners._work(retained.job, "final-plan")
    assert work.root != previous and not Path(work.root).exists()
    assert work.limits == reduced and (unknown / "preserve").read_text() == "unrelated contents"


def test_empty_resume_attempts_exhaust_before_mkdir(retained):
    attempt_roots = []
    for _attempt in range(64):
        before = set(retained.source.root.iterdir()), tuple(retained.registered)
        try:
            stage._reopen_work_root(retained.source)
        except SourceFactBudgetExceeded:
            assert (set(retained.source.root.iterdir()), tuple(retained.registered)) == before
            break
        attempt_roots.append(retained.source.work_root)
        assert not tuple(retained.source.work_root.iterdir())
    else:
        pytest.fail("empty abandoned attempts never exhausted their original allowance")
    assert 1 < len(attempt_roots) < 64 and all(path.is_dir() for path in attempt_roots)
    assert retained.source.limits.facts == retained.limits
    retained.lock.check()


def test_source_slot_counts_prior_and_partial_attempts(retained):
    before = stage._work(retained.source, "admission").limits
    old = retained.source.root / "admission"
    old.mkdir(mode=0o700)
    (old / "run.mpk").write_bytes(b"original")
    stage._reopen_work_root(retained.source)
    partial = retained.source.work_root / "admission"
    partial.mkdir(mode=0o700)
    (partial / "empty.mpk").touch()
    stage._reopen_work_root(retained.source)
    work = stage._work(retained.source, "admission")
    assert work.limits.max_spool_bytes < before.max_spool_bytes
    assert not work.root.exists() and old.is_dir() and partial.is_dir()
    assert retained.custody.profile == stage.owner_source_profile(retained.source)


@pytest.mark.parametrize("damage", ("symlink", "hardlink", "fifo", "mode", "nesting"))
def test_unsafe_retained_entries_refuse_without_cleanup(retained, tmp_path, damage):
    old = retained.root / "final-plan"
    old.mkdir(mode=0o700)
    outside = tmp_path / "outside"
    outside.write_text("preserve")
    match damage:
        case "symlink":
            (old / "entry").symlink_to(outside)
        case "hardlink":
            os.link(outside, old / "entry")
        case "fifo":
            os.mkfifo(old / "entry", 0o600)
        case "mode":
            old.chmod(0o755)
        case "nesting":
            nested = old
            for _depth in range(4):
                nested = nested / "nested"
                nested.mkdir(mode=0o700)
    with pytest.raises(FamilyBlockUnavailable):
        _remaining(retained)
    assert old.exists() and outside.read_text() == "preserve"
    assert not (retained.root / ("final-plan-" + "a" * 32)).exists()


@pytest.mark.parametrize("change", ("profile", "slot", "lock"))
def test_census_requires_original_scope(retained, monkeypatch, change):
    limits, slot = retained.limits, "final-plan"
    if change == "profile":
        limits = replace(limits, max_spool_bytes=limits.max_spool_bytes + 1)
    elif change == "slot":
        slot = "unknown"
    else:

        def unavailable():
            raise FamilyBlockUnavailable("injected unavailable lifetime lock")

        monkeypatch.setattr(retained.lock, "check", unavailable)
    try:
        with pytest.raises(FamilyBlockUnavailable):
            journal_api.retained_work_limits(
                retained.custody, retained.lock, retained.root / slot, slot, limits, lambda: None
            )
    finally:
        monkeypatch.undo()


def _journal_work(retained, name):
    return metadata_journal.MetadataJournalScratch(
        retained.journal,
        PlanWork(retained.journal.root / "outputs" / name, retained.limits, retained.registered.append),
    )


def test_journal_empty_attempts_are_charged_before_creation(retained):
    attempts = []
    for ordinal in range(64):
        retained.journal.reconcile_budget()
        root = retained.journal.root / "outputs" / str(ordinal)
        before_registrations = tuple(retained.registered)
        try:
            work = _journal_work(retained, str(ordinal))
        except SourceFactBudgetExceeded:
            assert not root.exists() and tuple(retained.registered) == before_registrations
            break
        empty = work.path()
        empty.touch()
        attempts.append(root)
    else:
        pytest.fail("empty journal attempts never consumed their retained allocation cost")
    assert attempts and all(path.is_dir() for path in attempts)


def test_partial_creation_retains_charge(retained, monkeypatch):
    retained.journal.reconcile_budget()
    original, before = facts._Work.__init__, retained.journal.budget().used

    def partial(work, *arguments, **keywords):
        original(work, *arguments, **keywords)
        raise RuntimeError("injected failure after mkdir")

    monkeypatch.setattr(facts._Work, "__init__", partial)
    with pytest.raises(RuntimeError, match="after mkdir"):
        _journal_work(retained, "partial")
    assert (retained.journal.root / "outputs" / "partial").is_dir()
    assert retained.journal.budget().used > before
    retained.journal.reconcile_budget()
    assert retained.journal.budget().used > before


@pytest.mark.parametrize("failure", ("cancel", "cleanup", "unconfirmed"))
def test_credit_requires_confirmed_removal(retained, monkeypatch, failure):
    retained.journal.reconcile_budget()
    before = retained.journal.budget().used
    work = _journal_work(retained, "active")
    work.write((("genuine frame",),))
    if failure == "cancel":
        work.stop.set()
        with pytest.raises(CancellationRequested):
            work.write((("never framed",),))
    else:

        def refuse(_path):
            if failure == "cleanup":
                raise PermissionError("injected cleanup refusal")

        monkeypatch.setattr(facts.shutil, "rmtree", refuse)
        charged = retained.journal.budget().used
        with pytest.raises(PermissionError if failure == "cleanup" else FamilyBlockUnavailable):
            work.close()
        assert retained.journal.budget().used == charged and work.root.is_dir()
        monkeypatch.undo()
    assert retained.journal.budget().used > before
    work.close()
    assert not work.root.exists() and retained.journal.budget().used == before
