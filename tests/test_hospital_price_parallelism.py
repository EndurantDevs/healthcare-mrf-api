# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Cross-run hospital resource and immutable-content coordination proof."""

from __future__ import annotations

import asyncio
import contextlib
import subprocess
import sys
import threading
from types import SimpleNamespace

import pytest

from process.ptg_parts import artifacts as ptg_artifacts
from process.ptg_parts.artifacts import PTG2ArtifactStore
from tests.hospital_price_orchestration_support import (
    ROOT,
    ArtifactStore,
    orchestrator_module,
)


@pytest.mark.asyncio
async def test_resource_slot_yields_and_releases():
    orchestrator = orchestrator_module()

    class Lock:
        releases = 0

        def try_acquire(self):
            return self

        def release(self):
            self.releases += 1

    lock = Lock()
    store = ArtifactStore()
    lock_names: list[tuple[str, ...]] = []
    store.named_lock = lambda *names: lock_names.append(names) or lock

    async with orchestrator._hospital_resource_slot(store, "fetch", 1):
        assert lock.releases == 0
    assert lock_names == [("hospital-price", "fetch-slot-000")]
    assert lock.releases == 1


@pytest.mark.asyncio
async def test_resource_slot_rotates_scan_start():
    orchestrator = orchestrator_module()
    acquired_keys: list[str] = []

    class Lock:
        def __init__(self, name: str) -> None:
            self.name = name

        def try_acquire(self):
            acquired_keys.append(self.name)
            return self

        def release(self):
            return None

    store = ArtifactStore()
    store.named_lock = lambda _namespace, key: Lock(key)
    for _unused in range(3):
        async with orchestrator._hospital_resource_slot(store, "load", 3):
            assert acquired_keys
    assert len(acquired_keys) == len(set(acquired_keys)) == 3


@pytest.mark.asyncio
async def test_two_runs_share_one_global_fetch_slot(tmp_path):
    orchestrator = orchestrator_module()
    first_entered, release_first, second_entered = (
        asyncio.Event(), asyncio.Event(), asyncio.Event()
    )

    async def first_run() -> None:
        async with orchestrator._hospital_resource_slot(
            PTG2ArtifactStore(tmp_path), "fetch", 1
        ):
            first_entered.set()
            await release_first.wait()

    async def second_run() -> None:
        await first_entered.wait()
        async with orchestrator._hospital_resource_slot(
            PTG2ArtifactStore(tmp_path), "fetch", 1
        ):
            second_entered.set()

    first, second = asyncio.create_task(first_run()), asyncio.create_task(second_run())
    try:
        await asyncio.wait_for(first_entered.wait(), timeout=0.5)
        await asyncio.sleep(0.05)
        assert not second_entered.is_set()
        release_first.set()
        await asyncio.wait_for(asyncio.gather(first, second), timeout=1)
    finally:
        release_first.set()
        for run in (first, second):
            if not run.done():
                run.cancel()
        await asyncio.gather(first, second, return_exceptions=True)
    assert second_entered.is_set()


def _lock_holder(tmp_path, lock_name: str, suffix: str = ""):
    return subprocess.Popen(
        [
            sys.executable,
            "-u",
            "-c",
            (
                "import sys, time\n"
                "from process.ptg_parts.artifacts import PTG2ArtifactStore\n"
                "lock = PTG2ArtifactStore(sys.argv[1]).named_lock("
                "'hospital-price', sys.argv[2] + sys.argv[3])\n"
                "lock.acquire()\nprint('held', flush=True)\ntime.sleep(30)\n"
            ),
            str(tmp_path),
            lock_name,
            suffix,
        ],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


@pytest.mark.asyncio
async def test_fetch_slot_is_cross_process_and_crash_releasing(tmp_path):
    orchestrator = orchestrator_module()
    child = _lock_holder(tmp_path, "fetch-slot-000")
    entered = asyncio.Event()

    async def wait_for_slot() -> None:
        async with orchestrator._hospital_resource_slot(
            PTG2ArtifactStore(tmp_path), "fetch", 1
        ):
            entered.set()

    waiter = None
    try:
        assert child.stdout is not None and child.stdout.readline() == "held\n"
        waiter = asyncio.create_task(wait_for_slot())
        await asyncio.sleep(0.05)
        assert not entered.is_set()
        child.kill()
        child.wait(timeout=2)
        await asyncio.wait_for(waiter, timeout=1)
    finally:
        if waiter is not None and not waiter.done():
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
        if child.poll() is None:
            child.kill()
            child.wait(timeout=2)
    assert entered.is_set()


@pytest.mark.asyncio
async def test_digest_lock_wait_is_cross_process_and_cancellable(tmp_path):
    orchestrator = orchestrator_module()
    digest = "a" * 64
    child = _lock_holder(tmp_path, "digest-", digest)
    entered = asyncio.Event()

    async def wait_for_digest() -> None:
        async with orchestrator._hospital_digest_lock(
            PTG2ArtifactStore(tmp_path), digest
        ):
            entered.set()

    waiter = None
    try:
        assert child.stdout is not None and child.stdout.readline() == "held\n"
        waiter = asyncio.create_task(wait_for_digest())
        await asyncio.sleep(0.05)
        assert not entered.is_set()
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(waiter, timeout=0.5)
    finally:
        if waiter is not None and not waiter.done():
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
        if child.poll() is None:
            child.kill()
            child.wait(timeout=2)
    async with orchestrator._hospital_digest_lock(PTG2ArtifactStore(tmp_path), digest):
        assert child.poll() is not None


@pytest.mark.asyncio
async def test_cancelled_resource_slot_acquire_does_not_release_unheld_lock():
    orchestrator = orchestrator_module()
    acquire_started, allow_acquire = threading.Event(), threading.Event()

    class Lock:
        releases = 0

        def try_acquire(self):
            acquire_started.set()
            assert allow_acquire.wait(timeout=1)
            return None

        def release(self):
            self.releases += 1

    lock = Lock()
    store = ArtifactStore()
    store.named_lock = lambda *_args: lock
    operation = asyncio.create_task(
        orchestrator._hospital_resource_slot(store, "fetch", 1).__aenter__()
    )
    assert await asyncio.to_thread(acquire_started.wait, 1)
    operation.cancel()
    allow_acquire.set()
    with pytest.raises(asyncio.CancelledError):
        await operation
    assert lock.releases == 0


def test_resource_slot_supports_nonblocking_capacity_poll(tmp_path, monkeypatch):
    store = PTG2ArtifactStore(tmp_path)
    held = store.named_lock("hospital-price", "fetch-slot-000")
    waiting = store.named_lock("hospital-price", "fetch-slot-000")
    held.acquire()
    try:
        assert waiting.try_acquire() is None
    finally:
        held.release()
    assert waiting.try_acquire() is waiting
    waiting.release()
    unavailable = store.named_lock("hospital-price", "load-slot-000")

    def unavailable_flock(_fd, _operation):
        raise BlockingIOError

    with monkeypatch.context() as patch:
        patch.setattr(ptg_artifacts.fcntl, "flock", unavailable_flock)
        assert unavailable.try_acquire() is None
    assert unavailable.try_acquire() is unavailable
    unavailable.release()


@pytest.mark.asyncio
async def test_cancelled_resource_slot_wait_does_not_wait_for_current_work(tmp_path):
    orchestrator = orchestrator_module()
    store = PTG2ArtifactStore(tmp_path)
    held = store.named_lock("hospital-price", "fetch-slot-000")
    held.acquire()
    entered = asyncio.Event()

    async def wait_for_capacity() -> None:
        async with orchestrator._hospital_resource_slot(store, "fetch", 1):
            entered.set()

    waiting = asyncio.create_task(wait_for_capacity())
    try:
        await asyncio.sleep(0.05)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(waiting, timeout=0.5)
    finally:
        held.release()
    assert not entered.is_set()


@pytest.mark.asyncio
async def test_independent_runs_lock_digest_and_recheck_before_parse(tmp_path, monkeypatch):
    """Parse one immutable digest once across concurrent import runs."""

    orchestrator = orchestrator_module()
    source_path = tmp_path / "hospital-mrf-source-test" / "raw" / "source.json"
    source_path.parent.mkdir(parents=True)
    source_path.write_text("{}")
    raw = SimpleNamespace(raw_sha256="a" * 64, raw_path=str(source_path), byte_count=2, head=None)
    first_parse_started, allow_first_parse = asyncio.Event(), asyncio.Event()
    content_staged = asyncio.Event()
    parse_calls, slot_entries, capacity_checks = [], [], []
    @contextlib.asynccontextmanager
    async def resource_slot(_store, resource, slot_count):
        assert (resource, slot_count) == ("load", 2)
        slot_entries.append(None)
        yield
    def require_capacity(*_args):
        capacity_checks.append(None)
    async def parse(*_args):
        assert len(capacity_checks) == 1
        parse_calls.append(None)
        if len(parse_calls) == 1:
            first_parse_started.set()
            await allow_first_parse.wait()
        return SimpleNamespace(version_id="version")
    async def has_existing_version(*_args):
        return content_staged.is_set()

    async def stage_content(*_args):
        content_staged.set()
    for name, collaborator in (
        ("has_existing_version", has_existing_version), ("_hospital_resource_slot", resource_slot),
        ("run_native_parser", parse), ("stage_content", stage_content),
        ("_require_disk_capacity", require_capacity),
    ):
        monkeypatch.setattr(orchestrator, name, collaborator)
    pipelines = [({}, {}), ({}, {})]

    async def ingest(index: int):
        locks, errors = pipelines[index]
        return await orchestrator._content_ingest_error(
            {}, {}, ArtifactStore(tmp_path), raw, locks, errors, (2048, 1024, 1),
            slot_count=2,
        )

    first = asyncio.create_task(ingest(0))
    await asyncio.wait_for(first_parse_started.wait(), timeout=1)
    second = asyncio.create_task(ingest(1))
    try:
        await asyncio.sleep(0.05)
        assert len(parse_calls) == 1
        allow_first_parse.set()
        assert await asyncio.gather(first, second) == [(None, None), (None, None)]
    finally:
        allow_first_parse.set()
        for task in (first, second):
            if not task.done():
                task.cancel()
        await asyncio.gather(first, second, return_exceptions=True)
    assert (len(parse_calls), len(slot_entries), len(capacity_checks)) == (1, 1, 2)


@pytest.mark.asyncio
async def test_two_selected_refresh_runs_overlap_without_a_run_lock(
    tmp_path, monkeypatch
):
    orchestrator = orchestrator_module()
    store = PTG2ArtifactStore(tmp_path)
    both_started, release = asyncio.Event(), asyncio.Event()
    started_run_ids: list[str] = []
    def selected_registry(params, **_kwargs):
        hospital_id = params["hospital_id"]
        return ({
            "hospital_id": hospital_id,
            "name": hospital_id,
            "cms_hpt_url": f"https://{hospital_id}/locator",
        },)

    monkeypatch.setattr(
        orchestrator, "selected_hospital_hpt_registry", selected_registry
    )
    monkeypatch.setattr(orchestrator, "_hospital_price_artifact_store", lambda: store)
    monkeypatch.setattr(orchestrator, "positive_env", lambda _name, default: default)

    async def run_import(_ctx, task, *_args):
        started_run_ids.append(task["run_id"])
        if len(started_run_ids) == 2:
            both_started.set()
        await release.wait()
        return {"published": 1}

    async def guard(_ctx, _task, operation, *_args):
        return await operation

    monkeypatch.setattr(orchestrator, "_run_import", run_import)
    monkeypatch.setattr(orchestrator, "_guard_cancellation", guard)
    runs = tuple(
        asyncio.create_task(
            orchestrator.refresh_hospital_prices(
                {}, {"hospital_id": hospital_id, "run_id": run_id}
            )
        )
        for hospital_id, run_id in (("a", "run-a"), ("b", "run-b"))
    )
    try:
        await asyncio.wait_for(both_started.wait(), timeout=0.5)
    finally:
        release.set()
        run_results = await asyncio.gather(*runs)
    assert set(started_run_ids) == {"run-a", "run-b"}
    assert run_results == [{"published": 1}, {"published": 1}]
