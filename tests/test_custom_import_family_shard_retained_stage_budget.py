# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retained stage-boundary limits use real coexisting spools and closed custody.

Native guard responses and encoding values are transport doubles where stated;
actual Work creation, frame charges, cleanup and source constructor remain real.
"""

import hashlib
from contextlib import asynccontextmanager, closing
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from process.custom_import import family_shard_facts as facts
from process.custom_import import family_shard_retained_checkpoint as custody
from process.custom_import import family_shard_retained_encoding as encoding
from process.custom_import import family_shard_retained_stage as stage
from process.custom_import import family_shard_source_stage as source_api
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockLimits
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded, SourceFactLimits
from process.custom_import.family_shard_output import FamilyOutputWork
from process.custom_import.family_shard_owner_lock import acquire_owner_job_lock
from process.custom_import.family_shard_plan import FinalPlanDescriptor, PlanWork, _Scratch
from process.custom_import.family_shard_source_journal import create_source_allocation_journal
from tests import test_custom_import_family_shard_cold_retained as controls
from tests.test_custom_import_family_shard_retained_stage import _ready_source

_MIB = 1024**2


def _budget_owner(root, maximum=2 * _MIB):
    root.mkdir(mode=0o700)
    observed = root.lstat()
    return SimpleNamespace(
        root=root,
        _root_identity=(observed.st_dev, observed.st_ino),
        _closed_costs={},
        retained_input=None,
        encoded=None,
        current=lambda: None,
        work=PlanWork(root, SourceFactLimits(maximum, 4096, 65536), lambda _path: None),
    )


def _write_bytes(work, target, total, path):
    while target.tell() < total:
        work.frame(target, b"x" * min(4000, total - target.tell()), path)


def _closed_input(owner, total):
    work = _Scratch(PlanWork(owner.root / "input", custody.retained_limits(owner), lambda _path: None))
    temporary = work.path()
    with temporary.open("xb") as stream:
        _write_bytes(work, stream, total, temporary)
    path = work.root / "retained-input.mpk"
    temporary.rename(path)
    work.paths[path] = work.paths.pop(temporary)
    path.chmod(0o400)
    observed = path.lstat()
    owner.retained_input = SimpleNamespace(
        root=str(work.root),
        device=work.identity.st_dev,
        inode=work.identity.st_ino,
        file_device=observed.st_dev,
        file_inode=observed.st_ino,
        file_bytes=observed.st_size,
        sha256=hashlib.sha256(path.read_bytes()).digest(),
    )
    return work, path


def test_closed_input_and_live_child_share_one_allowance(tmp_path):
    owner = _budget_owner(tmp_path / "retained")
    input_work, path = _closed_input(owner, 5 * _MIB // 4)
    allowance = custody.retained_limits(owner)
    assert allowance.max_spool_bytes < 3 * _MIB // 4
    child = _Scratch(stage._fact_work(owner, "stored"))
    output = child.path()
    try:
        with output.open("xb") as stream:
            while stream.tell() < 3 * _MIB // 4:
                before = stream.tell()
                try:
                    child.frame(stream, b"y" * 4000, output)
                except SourceFactBudgetExceeded:
                    assert stream.tell() == before
                    break
            else:
                pytest.fail("coexisting retained work exceeded its one original allowance")
        assert path.stat().st_size >= 5 * _MIB // 4
        assert output.stat().st_size < 3 * _MIB // 4
        assert child.used + child._entry_bytes <= allowance.max_spool_bytes
    finally:
        child.close()
    assert custody.retained_limits(owner) == allowance
    owner.retained_input = None
    input_work.close()
    owner.root.rmdir()


def test_failed_child_cleanup_refuses_fresh_allowance_and_retains_original(tmp_path, monkeypatch):
    owner = _budget_owner(tmp_path / "retained")
    input_work, path = _closed_input(owner, 5 * _MIB // 4)
    child = _Scratch(stage._fact_work(owner, "stored"))
    output = child.write((b"preserve failed child",))
    original = facts.shutil.rmtree

    def failed_cleanup(root):
        assert root == child.root
        raise OSError("cleanup interrupted")

    with monkeypatch.context() as patch:
        patch.setattr(facts.shutil, "rmtree", failed_cleanup)
        with pytest.raises(OSError, match="interrupted"):
            child.close()
    assert output.exists() and path.exists()
    with pytest.raises(FamilyBlockUnavailable, match="cleanup is incomplete"):
        stage._fact_work(owner, "next")
    assert not (owner.root / "next").exists()
    assert facts.shutil.rmtree is original
    child.close()
    assert custody.retained_limits(owner).max_spool_bytes < 3 * _MIB // 4
    owner.retained_input = None
    input_work.close()
    owner.root.rmdir()


def _nested_encoding_transport(monkeypatch, scratch, lookup, limits):
    """Stub values while retaining actual nested Work growth and cleanup."""
    allocations = []
    baseline = scratch.used + scratch._entry_bytes

    def prepare(*_arguments):
        return lookup, {"replay": _MIB // 4, "frame": 4096}

    class Keys:
        def __init__(self, *_arguments):
            self.closed = False

        def close(self):
            self.closed = True

    def families(_context, _destination, outer, _keys, replay_limits):
        assert outer.remaining_bytes == 0
        replay = _Scratch(PlanWork(outer.root / "family-replay", replay_limits, lambda _path: None))
        try:
            replay.write((b"r" * 4000 for _ in range(40)))
            allocations.append((replay.limits.max_spool_bytes, replay.used + replay._entry_bytes))
            yield object()
        finally:
            replay.close()

    def output(_layout, _family_id, families, _keys, *, work, **_options):
        actual = facts._Work(
            work.root, SourceFactLimits(work.limits.max_spool_bytes, 4096, 65536), work.register_cleanup
        )
        try:
            with closing(families):
                for _family in families:
                    target = actual.path()
                    with target.open("xb") as stream:
                        _write_bytes(actual, stream, _MIB // 4, target)
                    assert actual.limits.max_spool_bytes + allocations[0][0] + baseline == limits.max_spool_bytes
                    assert actual.used + actual._entry_bytes + allocations[0][1] + baseline <= limits.max_spool_bytes
            allocations.append((actual.limits.max_spool_bytes, actual.used + actual._entry_bytes))
            return SimpleNamespace(close=actual.close)
        except BaseException:
            actual.close()
            raise

    monkeypatch.setattr(encoding, "_prepare", prepare)
    monkeypatch.setattr(encoding, "_FamilyKeys", Keys)
    monkeypatch.setattr(encoding, "_families", families)
    monkeypatch.setattr(encoding, "build_family_output", output)
    return allocations


def test_encoding_reserves_replay_and_output_future_growth_together(tmp_path, monkeypatch):
    owner = _budget_owner(tmp_path / "retained")
    input_work, path = _closed_input(owner, _MIB // 4)
    limits = custody.retained_limits(owner)
    scratch = _Scratch(PlanWork(owner.root / "encoded-retained-inputs", limits, lambda _path: None))
    lookup = scratch.write((b"lookup",))
    allocations = _nested_encoding_transport(monkeypatch, scratch, lookup, limits)

    context = SimpleNamespace(common=SimpleNamespace(layout=object(), source=SimpleNamespace(snapshot_family_id=17)))
    output_work = FamilyOutputWork(
        owner.root / "encoded", FamilyBlockLimits(limits.max_spool_bytes, 4096, 4096, 65536), lambda _path: None
    )
    encoded = encoding._encode(context, object(), object(), object(), output_work, scratch, 3, b"p" * 32)
    try:
        assert len(allocations) == 2 and all(used <= maximum for maximum, used in allocations)
        assert (
            sum(maximum for maximum, _used in allocations) + scratch.used + scratch._entry_bytes
            == limits.max_spool_bytes
        )
        assert path.exists()
    finally:
        scratch.close()
        encoded.close()
        owner.retained_input = None
        input_work.close()
        owner.root.rmdir()


async def test_completed_encoded_and_stored_inputs_do_not_request_new_output_headroom(tmp_path, monkeypatch):
    allocation_limits = stage.allocation.SourceAllocationLimits(SourceFactLimits(32 * _MIB, _MIB, _MIB), 32, _MIB)
    owner = SimpleNamespace(
        root=tmp_path / "retained",
        context=object(),
        current=lambda: None,
        encoded=object(),
        stored=object(),
        work=PlanWork(tmp_path / "retained", SourceFactLimits(8 * _MIB, _MIB, _MIB), lambda _path: None),
        _checkpoint_stage=6,
        source=SimpleNamespace(
            owner_profile=("original",),
            limits=SimpleNamespace(
                allocation=allocation_limits,
                worker=SimpleNamespace(output=FamilyBlockLimits(64 * _MIB, _MIB, 65536, 16 * _MIB)),
            ),
        ),
    )

    async def base(_context):
        assert _context is owner.context

    def forbidden(*_arguments, **_options):
        pytest.fail("completed retained output requested fresh producer resources")

    monkeypatch.setattr(stage.allocation, "_check_base", base)
    monkeypatch.setattr(stage, "_output_work", forbidden)
    monkeypatch.setattr(stage, "_fact_work", forbidden)
    monkeypatch.setattr(stage, "build_retained_output", forbidden)
    await stage._encode_retained(owner)
    assert stage._allocation_work(owner, "original", writing=False).limits == allocation_limits
    assert not owner.root.exists()


def _phase_source(tmp_path):
    source = _ready_source(tmp_path)
    source.phase = None
    source.allocated.source.counts = (7,)
    source.allocation_journal = create_source_allocation_journal(
        source.context.build_id,
        work=PlanWork(source.root / "journal", source.limits.allocation.spool, lambda _path: None),
        control_limits=source.limits.facts,
    )
    owner_lock = acquire_owner_job_lock(tmp_path, identity=(tmp_path.stat().st_dev, tmp_path.stat().st_ino))
    source.owner_lock = owner_lock
    plan = FinalPlanDescriptor(str(tmp_path / "plan"), 1, 1, 1, 1, 0, b"f" * 32, b"a" * 32, (), None, (), 1024)
    work = PlanWork(tmp_path / "retained", source.limits.facts, lambda _path: None)
    return source, owner_lock, plan, work


@pytest.mark.parametrize("native", ["graph", "admission", "changed"])
async def test_fresh_native_phase_restores_real_source_before_retained_construction(tmp_path, monkeypatch, native):
    """Only an authenticated fresh GRAPH response can restore the real constructor input."""
    source_owner, owner_lock, plan, work = _phase_source(tmp_path)
    snapshot, _records = controls._snapshot(1)
    cells = custody.latest(snapshot, source_owner.current)
    calls = []

    async def protected(*_arguments):
        calls.append("protected")
        return snapshot

    async def observed(*_arguments):
        calls.append("observed")
        return SimpleNamespace(phase="graph" if native == "changed" else native)

    @asynccontextmanager
    async def native_page(*_arguments):
        yield (
            object(),
            SimpleNamespace(
                phase="admission" if native == "changed" else native,
                source_frozen_at=datetime(2030, 1, 1, tzinfo=UTC),
                source_occurrence_count=7,
            ),
        )

    async def candidate(*_arguments):
        return object(), object()

    monkeypatch.setattr(custody.journal_api, "_custody_snapshot", protected)
    monkeypatch.setattr(source_api, "_snapshot", observed)
    monkeypatch.setattr(source_api, "_page_session", native_page)
    monkeypatch.setattr(source_api.store, "_candidate", candidate)
    try:
        with pytest.raises(ValueError, match="completed SOURCE"):
            stage.RetainedFamilyStage(source_owner, plan, None, work=work)
        if native == "graph":
            await custody._restore_source_phase(None, source_owner, cells)
            owner = stage.RetainedFamilyStage(source_owner, plan, None, work=work)
            assert owner.source is source_owner and source_owner.phase == "graph"
        else:
            with pytest.raises(FamilyBlockUnavailable, match="GRAPH phase|phase changed"):
                await custody._restore_source_phase(None, source_owner, cells)
            assert source_owner.phase is None and not work.root.exists()
        assert calls == ["protected", "observed"]
    finally:
        source_owner.allocation_journal.close_terminal()
        await source_owner.close_terminal()
        owner_lock.close()
        (tmp_path / ".owner-job.lock").unlink()


def test_partial_retained_vectors_refuse_before_original_input_is_missing():
    snapshot, records = controls._snapshot(1, partial=True)
    incomplete_records = tuple(pair for pair in records if pair[1] != controls._bundle(1))
    snapshot.records = lambda **_options: (row for row in incomplete_records)
    with pytest.raises(FamilyBlockUnavailable, match="original input custody"):
        custody.latest(snapshot, None)
