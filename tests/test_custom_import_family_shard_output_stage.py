# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""OUTPUT coordinator tests with explicit producer/native transport doubles.

These tests inject lost replies and cancellation. They do not establish native
storage, original semantic verification, generation finality or API acceptance.
"""

import asyncio
import threading
from collections import Counter
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from process.custom_import import family_shard_output_stage as stage
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_shard_allocate import SourceAllocationLimits
from process.custom_import.family_shard_facts import SourceFactLimits
from process.custom_import.family_shard_plan import PlanWork
from tests.test_custom_import_build_source import _request


class _Output:
    def __init__(self, layout, shard, closed):
        self.receipt = stage.store.ShardOutputProof(layout, shard, b"p" * 32, 0).receipt()
        self.closed, self.events = False, closed

    def batches(self, **_limits):
        if self.closed:
            raise ValueError("closed synthetic output")
        yield {}

    def close(self):
        self.closed = True
        self.events.append(self.receipt["shard_id"])


class _Pipeline:
    def __init__(self, tmp_path, configured):
        request = _request()
        layout = FamilyBlockLayout(request.definition, {"details": 7})
        limits = SourceFactLimits(64 * 1024**2, 1024**2, 4 * 1024**2)
        common = SimpleNamespace(
            request=request,
            layout=layout,
            source=SimpleNamespace(
                snapshot_family_id=17,
                source=SimpleNamespace(partition_sha256=(b"p" * 32).hex(), scope=((("capture_bundle_id", 8),),)),
            ),
        )
        source_owner = SimpleNamespace(
            context=SimpleNamespace(request=request, build_id=11),
            workers=tuple(object() for _index in range(4)),
            admission=object(),
            limits=SimpleNamespace(allocation=SourceAllocationLimits(limits, 32, 1024**2)),
            metadata=SimpleNamespace(query_keys=lambda: iter(())),
            events=object(),
            common_receipt={"shard_id": 4},
            event_receipt={"shard_id": 5},
        )
        # Only the coordinator is real; all producer and native boundaries below are doubled.
        retained = object.__new__(stage.RetainedFamilyStage)
        retained.source, retained.complete, retained.final_plan = source_owner, True, object()
        retained.context = SimpleNamespace(common=common, build_id=11)
        retained.current = lambda: None
        retained.destination, retained.original, retained.encoded, retained.stored = (object() for _index in range(4))
        retained.common_receipt = {"shard_id": 6}
        self.owner = stage.ShardOutputStage(retained, work=PlanWork(tmp_path / "output", limits, lambda _p: None))
        self.phase, self.storage, self.indexed, self.configured = "graph", "loading", False, configured
        self.calls, self.closed, self.committed = Counter(), [], {}
        self.fail, self.block = None, False
        self.started, self.release = threading.Event(), threading.Event()
        self.members, self.winners = _Output(layout, 8, self.closed), object()
        self.projected = _Output(layout, 10, self.closed) if configured else None

    def _maybe_fail(self, boundary):
        if self.fail == boundary:
            self.fail = None
            raise OSError("synthetic lost " + boundary + " reply")

    async def snapshot(self, *_args):
        return SimpleNamespace(phase=self.phase)

    async def loading(self, *_args):
        return self.storage

    async def open(self, *_args, **_kwargs):
        assert self.storage == "loading"
        self.calls["open"] += 1
        self.phase = "output"
        self._maybe_fail("open")
        return 29

    @contextmanager
    def prepared(self, *_args, **_kwargs):
        self.calls["prepared"] += 1
        try:
            yield object()
        finally:
            self.closed.append("prepared")

    def members_output(self, _owner):
        self.calls["members"] += 1
        if self.block:
            self.started.set()
            if not self.release.wait(5):
                raise RuntimeError("test member producer deadline")
        return self.members, self.winners

    def winner_batches(self, *_args):
        yield {}

    async def copy(self, _factory, _request, _family, _layout, batches, receipt, **_limits):
        shard = receipt["shard_id"]
        self.calls["copy-" + str(shard)] += 1
        if shard in self.committed:
            assert self.committed[shard] == receipt
            return receipt
        assert self.storage == "loading"
        assert list(batches) == [{}]
        self.committed[shard] = receipt
        self._maybe_fail("copy")
        return receipt

    async def projection(self, *_args, **_kwargs):
        self.calls["projection"] += 1
        assert self.storage == "loading"
        return self.projected

    async def close(self, *_args):
        self.calls["close"] += 1
        self.storage = "closed"
        self._maybe_fail("close")

    async def indexes(self, *_args):
        assert self.storage == "closed"
        if not self.indexed:
            self.calls["index-create"] += 1
            self.indexed = True
            self._maybe_fail("indexes")

    async def prove(self, *_args):
        self.calls["prove"] += 1
        assert self.storage == "closed" and self.indexed
        self._maybe_fail("prove")
        return object()

    async def manifest(self, *_args):
        self.calls["manifest"] += 1
        self._maybe_fail("manifest")

    async def complete(self, *_args, **_kwargs):
        self.calls["complete"] += 1
        self.phase = "verifying"
        self._maybe_fail("complete")

    def install(self, monkeypatch):
        for name, method in (
            ("_snapshot", self.snapshot),
            ("_loading_phase", self.loading),
            ("open_shard_output", self.open),
            ("prepared_source_family_inputs", self.prepared),
            ("_prepare_members", self.members_output),
            ("_winner_batches", self.winner_batches),
            ("stage_shard_query_projection", self.projection),
            ("_closed_projection", self.prove),
            ("complete_shard_query_projection", self.manifest),
            ("complete_shard_output", self.complete),
        ):
            monkeypatch.setattr(stage, name, method)
        monkeypatch.setattr(stage.store, "copy_shard", self.copy)
        monkeypatch.setattr(stage.store, "close_candidate", self.close)
        monkeypatch.setattr(stage.store, "prepare_indexes", self.indexes)
        monkeypatch.setattr(stage, "cleanup_winner_output", lambda _output: self.closed.append("winners"))


@pytest.mark.parametrize("configured", [False, True])
async def test_complete_original_inputs_and_unique_auxiliary_receipts(tmp_path, monkeypatch, configured):
    pipeline = _Pipeline(tmp_path, configured)
    pipeline.install(monkeypatch)
    owner = await stage.advance_shard_output(None, pipeline.owner)
    assert owner.inputs.workers == owner.source.workers
    assert owner.inputs.source_families is owner.prepared
    assert owner.inputs.members is pipeline.members and owner.inputs.winners is pipeline.winners
    assert owner.inputs.projection_output is pipeline.projected
    assert sorted(pipeline.committed) == ([8, 9, 10] if configured else [8, 9])
    assert pipeline.phase == "verifying" and pipeline.calls["index-create"] == 1
    await owner.close_terminal()
    assert not owner.root.exists()
    assert set(pipeline.closed) == ({8, 10, "winners", "prepared"} if configured else {8, "winners", "prepared"})


@pytest.mark.parametrize("boundary", ["open", "copy", "close", "indexes", "prove", "manifest", "complete"])
async def test_uncertain_commits_resume_with_same_originals(tmp_path, monkeypatch, boundary):
    pipeline = _Pipeline(tmp_path, True)
    pipeline.install(monkeypatch)
    pipeline.fail = boundary
    with pytest.raises(OSError, match=boundary):
        await stage.advance_shard_output(None, pipeline.owner)
    prepared, members = pipeline.owner.prepared, pipeline.owner.member_winners
    assert not pipeline.closed
    owner = await stage.advance_shard_output(None, pipeline.owner)
    assert pipeline.phase == "verifying"
    if prepared is not None:
        assert owner.prepared is prepared
    if members is not None:
        assert owner.member_winners is members
    assert pipeline.calls["prepared"] == pipeline.calls["members"] == pipeline.calls["index-create"] == 1
    assert owner.inputs is not None
    await owner.close_terminal()


async def test_cancellation_drains_and_retains_completed_cpu_output(tmp_path, monkeypatch):
    pipeline = _Pipeline(tmp_path, False)
    pipeline.install(monkeypatch)
    pipeline.block = True
    task = asyncio.create_task(stage.advance_shard_output(None, pipeline.owner))
    assert await asyncio.to_thread(pipeline.started.wait, 5)
    with pytest.raises(ValueError, match="drain"):
        await pipeline.owner.close_terminal()
    task.cancel()
    pipeline.release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert pipeline.owner.member_winners == (pipeline.members, pipeline.winners)
    assert not pipeline.closed
    await stage.advance_shard_output(None, pipeline.owner)
    assert pipeline.calls["members"] == 1
    await pipeline.owner.close_terminal()


@pytest.mark.parametrize("phase", ["verified", "rejected", "source"])
async def test_wrong_native_phase_does_not_write_or_create_producers(tmp_path, monkeypatch, phase):
    pipeline = _Pipeline(tmp_path, False)
    pipeline.install(monkeypatch)
    pipeline.phase = phase
    with pytest.raises(stage.FamilyBlockUnavailable, match="immutable phase"):
        await stage.advance_shard_output(None, pipeline.owner)
    assert not pipeline.calls and pipeline.owner.inputs is None
    await pipeline.owner.close_terminal()


async def test_closed_candidate_without_original_outputs_refuses_regeneration(tmp_path, monkeypatch):
    pipeline = _Pipeline(tmp_path, False)
    pipeline.install(monkeypatch)
    pipeline.phase, pipeline.storage = "output", "closed"
    with pytest.raises(stage.FamilyBlockUnavailable, match="original loading artifacts"):
        await stage.advance_shard_output(None, pipeline.owner)
    assert not pipeline.calls
    await pipeline.owner.close_terminal()
