# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Manifest COPY workers must finish before their caller cleans up inputs."""

import asyncio
import importlib
from pathlib import Path

import pytest

ptg = importlib.import_module("process.ptg")


@pytest.mark.asyncio
@pytest.mark.parametrize("copy_tasks", [1, 2, 5])
async def test_manifest_copy_preserves_concurrency(copy_tasks):
    active_count_by_state = {"current": 0, "maximum": 0}
    copied_paths = []
    paths = [Path(str(index)) for index in range(3)]

    async def copy_file(path, **_kwargs):
        active_count_by_state["current"] += 1
        active_count_by_state["maximum"] = max(active_count_by_state.values())
        await asyncio.sleep(0)
        copied_paths.append(path)
        active_count_by_state["current"] -= 1

    await ptg._copy_manifest_paths(
        paths,
        target_table="stage",
        copy_func=copy_file,
        progress_callback=lambda _count: None,
        copy_tasks=copy_tasks,
    )

    assert copied_paths == paths
    assert active_count_by_state == {"current": 0, "maximum": min(copy_tasks, len(paths))}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_type", [RuntimeError, ptg.StaleMetadataFenceError, asyncio.CancelledError]
)
async def test_manifest_copy_failure_drains(error_type, caplog):
    sibling_started = asyncio.Event()
    sibling_stopped = asyncio.Event()
    release_sibling = asyncio.Event()
    copy_workers = []
    failure = error_type("injected COPY failure")

    async def copy_file(path, **_kwargs):
        copy_workers.append(asyncio.current_task())
        if path.name == "failing":
            await sibling_started.wait()
            raise failure
        sibling_started.set()
        try:
            await release_sibling.wait()
        finally:
            await asyncio.sleep(0)
            sibling_stopped.set()

    try:
        with pytest.raises(error_type) as raised:
            await ptg._copy_manifest_paths(
                [Path("failing"), Path("sibling")],
                target_table="stage",
                copy_func=copy_file,
                progress_callback=lambda _count: None,
                copy_tasks=2,
            )
        assert raised.value is failure
        assert sibling_stopped.is_set(), "input cleanup would race the sibling COPY"
        assert all(worker.done() for worker in copy_workers)
        await asyncio.sleep(0)
        assert "exception was never retrieved" not in caplog.text
    finally:
        release_sibling.set()
        await asyncio.gather(*copy_workers, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("is_repeated_cancel", [False, True])
async def test_manifest_copy_cancellation_drains(is_repeated_cancel, caplog):
    started_paths = set()
    cleaning_paths = set()
    stopped_paths = set()
    all_started = asyncio.Event()
    all_cleaning = asyncio.Event()
    release_copies = asyncio.Event()
    release_cleanup = asyncio.Event()

    async def copy_file(path, **_kwargs):
        started_paths.add(path)
        if len(started_paths) == 2:
            all_started.set()
        try:
            await release_copies.wait()
        finally:
            cleaning_paths.add(path)
            if len(cleaning_paths) == 2:
                all_cleaning.set()
            await release_cleanup.wait()
            stopped_paths.add(path)

    copy_task = asyncio.create_task(
        ptg._copy_manifest_paths(
            [Path("one"), Path("two")],
            target_table="stage",
            copy_func=copy_file,
            progress_callback=lambda _count: None,
            copy_tasks=2,
        )
    )
    try:
        await asyncio.wait_for(all_started.wait(), timeout=1)
        copy_task.cancel()
        await asyncio.wait_for(all_cleaning.wait(), timeout=1)
        if is_repeated_cancel:
            copy_task.cancel()
            await asyncio.sleep(0)
        release_cleanup.set()
        with pytest.raises(asyncio.CancelledError):
            await copy_task
        assert stopped_paths == started_paths
        await asyncio.sleep(0)
        assert "exception was never retrieved" not in caplog.text
        assert "exception in shielded future" not in caplog.text
    finally:
        release_copies.set()
        release_cleanup.set()
        await asyncio.gather(copy_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_copy_drain_survives_cancellation():
    sibling_started = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()
    copy_workers = []
    failure = RuntimeError("injected COPY failure")

    async def copy_file(path, **_kwargs):
        copy_workers.append(asyncio.current_task())
        if path.name == "failing":
            await sibling_started.wait()
            raise failure
        sibling_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleanup_started.set()
            await release_cleanup.wait()
            cleanup_finished.set()

    copy_task = asyncio.create_task(
        ptg._copy_manifest_paths(
            [Path("failing"), Path("sibling")],
            target_table="stage",
            copy_func=copy_file,
            progress_callback=lambda _count: None,
            copy_tasks=2,
        )
    )
    try:
        await asyncio.wait_for(cleanup_started.wait(), timeout=1)
        for _attempt in range(2):
            copy_task.cancel()
            await asyncio.sleep(0)
        assert not copy_task.done()
        release_cleanup.set()
        with pytest.raises(RuntimeError) as raised:
            await copy_task
        assert raised.value is failure
        assert cleanup_finished.is_set()
        assert all(worker.done() for worker in copy_workers)
    finally:
        release_cleanup.set()
        for worker in copy_workers:
            worker.cancel()
        await asyncio.gather(copy_task, *copy_workers, return_exceptions=True)
