# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused branch coverage for hospital-price worker orchestration."""

from __future__ import annotations

import asyncio
import contextlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.hospital_price_orchestration_support import (
    ArtifactStore,
    Attempt,
    DownloadedSource,
    configure_incomplete_import,
    orchestrator_module,
)


@pytest.mark.asyncio
async def test_start_attempts_handles_empty_and_active_candidates(monkeypatch):
    orchestrator = orchestrator_module()
    run_attempts = []
    assert await orchestrator._start_attempts([], run_attempts, "owner", 30) == (
        (), 0,
    )

    candidates = (
        SimpleNamespace(
            hospital_id="a", hospital_name="A", source_url="https://a/mrf",
            locator_name="A locator", locator_url="https://a/locator",
        ),
        SimpleNamespace(
            hospital_id="b", hospital_name="B", source_url="https://b/mrf",
            locator_name=None, locator_url="https://b/locator",
        ),
    )
    monkeypatch.setattr(
        orchestrator,
        "admit_attempts",
        AsyncMock(return_value=(("a", "attempt-a", 4),)),
    )

    attempts, active = await orchestrator._start_attempts(
        candidates, run_attempts, "owner", 30
    )

    assert [(attempt.hospital_id, attempt.expected_generation) for attempt in attempts] == [
        ("a", 4)
    ]
    assert run_attempts == list(attempts)
    assert attempts[0].locator_url == "https://a/locator"
    assert active == 1


@pytest.mark.asyncio
async def test_new_content_is_staged_after_native_parse(tmp_path, monkeypatch):
    orchestrator = orchestrator_module()
    source_path = tmp_path / "hospital-mrf-source-test" / "raw" / "source.json"
    source_path.parent.mkdir(parents=True)
    source_path.write_text("{}")
    raw = SimpleNamespace(
        raw_sha256="a" * 64, raw_path=str(source_path), byte_count=2
    )
    receipt = SimpleNamespace(version_id="version")
    monkeypatch.setattr(
        orchestrator, "has_existing_version", AsyncMock(return_value=False)
    )
    monkeypatch.setattr(
        orchestrator, "run_native_parser", AsyncMock(return_value=receipt)
    )
    stage = AsyncMock()
    monkeypatch.setattr(orchestrator, "stage_content", stage)

    assert await orchestrator._ensure_content(
        {}, {}, ArtifactStore(tmp_path), raw, 2048, 1024
    ) == orchestrator.hospital_price_version_id(raw.raw_sha256)
    stage.assert_awaited_once_with(receipt, raw)


@pytest.mark.asyncio
async def test_publish_download_handles_empty_success_cancel_and_failure(monkeypatch):
    orchestrator = orchestrator_module()
    attempt = Attempt("one", "a", "A", "https://a/mrf", 0)
    empty_download = DownloadedSource("https://a/mrf", None, (attempt,))
    assert await orchestrator._publish_download(
        {}, {}, empty_download, "version"
    ) == (0, 0, 0, 0)

    raw = SimpleNamespace(raw_sha256="a" * 64)
    downloaded_source = DownloadedSource("https://a/mrf", raw, (attempt,))
    publish = AsyncMock(return_value=(1, 2, 3))
    fail = AsyncMock(return_value=1)
    monkeypatch.setattr(orchestrator, "publish_existing", publish)
    monkeypatch.setattr(orchestrator, "_fail_attempts", fail)
    assert await orchestrator._publish_download(
        {}, {}, downloaded_source, "version"
    ) == (1, 2, 3, 0)

    publish.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await orchestrator._publish_download({}, {}, downloaded_source, "version")
    fail.assert_awaited_with((attempt,), "cancelled", "hospital import cancelled")

    publish.side_effect = ValueError("broken")
    monkeypatch.setattr(orchestrator, "error_details", lambda exc: ("invalid", str(exc)))
    assert await orchestrator._publish_download(
        {}, {}, downloaded_source, "version"
    ) == (0, 0, 0, 1)
    fail.assert_awaited_with((attempt,), "invalid", "broken")


@pytest.mark.asyncio
async def test_resolve_attempts_groups_sources_and_initial_errors(monkeypatch):
    orchestrator = orchestrator_module()
    candidates = (
        SimpleNamespace(
            hospital_id="bad", initial_error_code="locator",
            initial_error_detail=None,
        ),
        SimpleNamespace(
            hospital_id="good", initial_error_code=None,
            initial_error_detail=None,
        ),
    )
    attempts = (
        Attempt("one", "bad", "Bad", "https://bad/mrf", 0),
        Attempt("two", "good", "Good", "https://good/mrf", 0),
    )
    monkeypatch.setattr(
        orchestrator, "candidates_from_locators", lambda _results: candidates
    )
    monkeypatch.setattr(
        orchestrator, "_start_attempts", AsyncMock(return_value=(attempts, 1))
    )
    fail = AsyncMock(return_value=1)
    monkeypatch.setattr(orchestrator, "_fail_attempts", fail)

    attempts_by_url, active, failed = await orchestrator._resolve_attempts(
        (), [], "owner", 30
    )

    assert attempts_by_url == {"https://good/mrf": [attempts[1]]}
    assert (active, failed) == (1, 1)
    fail.assert_awaited_once_with([attempts[0]], "locator", "locator")


@pytest.mark.asyncio
async def test_successful_content_ingest_is_cached_by_digest(monkeypatch):
    orchestrator = orchestrator_module()
    ensure = AsyncMock()
    monkeypatch.setattr(orchestrator, "_ensure_content", ensure)
    raw = SimpleNamespace(raw_sha256="a" * 64)
    locks_by_digest, errors_by_digest = {}, {}

    for _unused in range(2):
        assert await orchestrator._content_ingest_error(
            {}, {}, ArtifactStore(), raw, locks_by_digest, errors_by_digest, 2048, 1024
        ) == (None, None)
    ensure.assert_awaited_once()


@pytest.mark.asyncio
async def test_load_worker_acknowledges_download_failures_and_publishes(
    tmp_path, monkeypatch
):
    orchestrator = orchestrator_module()
    attempt = Attempt("one", "a", "A", "https://a/mrf", 0)
    pending_acknowledgement = asyncio.get_running_loop().create_future()
    completed_acknowledgement = asyncio.get_running_loop().create_future()
    completed_acknowledgement.set_result(None)
    raw_path = tmp_path / "hospital-mrf-source-test" / "raw" / "source.json"
    raw_path.parent.mkdir(parents=True)
    raw_path.write_bytes(b"{}")
    raw = SimpleNamespace(raw_sha256="a" * 64, raw_path=str(raw_path))
    downloads = asyncio.Queue()
    for queued_download in (
        (
            DownloadedSource(
                "https://a/mrf", None, (attempt,), "download", "bad"
            ),
            pending_acknowledgement,
        ),
        (
            DownloadedSource(
                "https://a/mrf", None, (attempt,), "download", "bad"
            ),
            completed_acknowledgement,
        ),
        (
            DownloadedSource("https://a/mrf", raw, (attempt,)),
            completed_acknowledgement,
        ),
        None,
    ):
        downloads.put_nowait(queued_download)
    metrics_by_name = {
        "processed": 0, "published": 0, "superseded": 0,
        "unchanged": 0, "failed": 0,
    }
    monkeypatch.setattr(orchestrator, "_fail_attempts", AsyncMock(return_value=1))
    monkeypatch.setattr(
        orchestrator, "_content_ingest_error", AsyncMock(return_value=(None, None))
    )
    monkeypatch.setattr(
        orchestrator, "_publish_download", AsyncMock(return_value=(1, 0, 0, 0))
    )
    monkeypatch.setattr(orchestrator, "_progress", lambda *_args: None)

    await orchestrator._load_worker(
        {}, {}, ArtifactStore(tmp_path), downloads,
        ({}, {}, metrics_by_name), (None, 0, 3),
        2048, 1024,
    )

    assert pending_acknowledgement.done()
    assert not raw_path.exists()
    assert metrics_by_name == {
        "processed": 3, "published": 1, "superseded": 0,
        "unchanged": 0, "failed": 2,
    }


@pytest.mark.asyncio
async def test_bulk_import_returns_complete_selected_cohort(tmp_path, monkeypatch):
    orchestrator = orchestrator_module()
    hospitals = (
        {"hospital_id": "a", "name": "A", "cms_hpt_url": "https://a/locator"},
        {"hospital_id": "b", "name": "B", "cms_hpt_url": "https://b/locator"},
    )
    configure_incomplete_import(
        orchestrator,
        monkeypatch,
        0,
        {
            "processed": 2, "published": 2, "superseded": 0,
            "unchanged": 0, "failed": 0, "contents": 1,
        },
    )
    collect = AsyncMock(return_value=0)
    monkeypatch.setattr(
        orchestrator, "garbage_collect_superseded_versions", collect
    )

    metrics_by_name = await orchestrator._run_import(
        {}, {}, hospitals, ArtifactStore(tmp_path), [], "owner", 30
    )

    assert metrics_by_name["selected"] == metrics_by_name["published"] == 2
    collect.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_finish_failed_attempts_drains_cleanup_after_cancel(monkeypatch):
    orchestrator = orchestrator_module()
    cleanup_started, allow_cleanup, cleanup_finished = (
        asyncio.Event(), asyncio.Event(), asyncio.Event()
    )

    async def fail(*_args):
        cleanup_started.set()
        await allow_cleanup.wait()
        cleanup_finished.set()

    monkeypatch.setattr(orchestrator, "_fail_attempts", fail)
    cleanup = asyncio.create_task(
        orchestrator._finish_failed_attempts([], "cancelled", "cancelled")
    )
    await asyncio.wait_for(cleanup_started.wait(), timeout=1)
    cleanup.cancel()
    await asyncio.sleep(0)
    allow_cleanup.set()

    await asyncio.wait_for(cleanup, timeout=1)
    assert cleanup_finished.is_set()


@pytest.mark.asyncio
async def test_worker_and_cli_delegate_to_refresh(monkeypatch):
    orchestrator = orchestrator_module()
    refresh = AsyncMock(return_value={"published": 1})
    monkeypatch.setattr(orchestrator, "refresh_hospital_prices", refresh)
    assert await orchestrator.process_data({}, {"hospital_id": "a"}) == {
        "published": 1
    }
    refresh.assert_awaited_once_with({}, {"hospital_id": "a"})

    process = AsyncMock(return_value={"published": 2})
    monkeypatch.setattr(orchestrator, "process_data", process)
    assert await orchestrator.main(hospital_ids=("a", "b")) == {"published": 2}
    process.assert_awaited_once_with(
        {},
        {"hospital_id": None, "hospital_ids": ["a", "b"], "all_hospitals": False},
    )


@pytest.mark.asyncio
async def test_refresh_lifecycle_succeeds_and_rejects_long_owner(monkeypatch):
    orchestrator = orchestrator_module()
    hospital_by_field = {
        "hospital_id": "a", "name": "A", "cms_hpt_url": "https://a/locator"
    }
    monkeypatch.setattr(
        orchestrator,
        "selected_hospital_hpt_registry",
        lambda *_args, **_kwargs: (hospital_by_field,),
    )
    monkeypatch.setattr(orchestrator, "ensure_database", AsyncMock())
    monkeypatch.setattr(orchestrator, "positive_env", lambda _name, default: default)

    @contextlib.asynccontextmanager
    async def resource_lock(_store):
        yield

    monkeypatch.setattr(orchestrator, "_hospital_resource_lock", resource_lock)
    run_import = AsyncMock(return_value={"published": 1})
    monkeypatch.setattr(orchestrator, "_run_import", run_import)

    async def guard(_ctx, _task, operation, *_args):
        return await operation

    monkeypatch.setattr(orchestrator, "_guard_cancellation", guard)

    assert await orchestrator.refresh_hospital_prices(
        {}, {"hospital_id": "a", "run_id": "run"}
    ) == {"published": 1}

    with pytest.raises(ValueError, match="run owner is invalid"):
        await orchestrator.refresh_hospital_prices(
            {}, {"hospital_id": "a", "run_id": "x" * 128}
        )


@pytest.mark.asyncio
async def test_refresh_sweeps_under_resource_lock_before_database_network(
    tmp_path, monkeypatch
):
    orchestrator = orchestrator_module()
    store = ArtifactStore(tmp_path)
    stale_root = tmp_path / "hospital-mrf-source-stale"
    (stale_root / "raw").mkdir(parents=True)
    (stale_root / "raw" / "source.json").write_bytes(b"{}")
    lock_held = asyncio.Event()

    monkeypatch.setattr(
        orchestrator, "selected_hospital_hpt_registry", lambda *_args, **_kwargs: ()
    )
    monkeypatch.setattr(orchestrator, "_hospital_price_artifact_store", lambda: store)
    monkeypatch.setattr(orchestrator, "positive_env", lambda _name, default: default)

    @contextlib.asynccontextmanager
    async def resource_lock(_store):
        lock_held.set()
        try:
            yield
        finally:
            lock_held.clear()

    async def ensure_database(_migrate):
        assert lock_held.is_set()
        assert not stale_root.exists()
        raise RuntimeError("stop after ordered sweep")

    async def guard(_ctx, _task, operation, *_args):
        return await operation

    monkeypatch.setattr(orchestrator, "_hospital_resource_lock", resource_lock)
    monkeypatch.setattr(orchestrator, "ensure_database", ensure_database)
    monkeypatch.setattr(orchestrator, "_guard_cancellation", guard)

    with pytest.raises(RuntimeError, match="stop after ordered sweep"):
        await orchestrator.refresh_hospital_prices({}, {"hospital_id": "a"})
    assert not lock_held.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "expected_cleanup"),
    (
        (asyncio.CancelledError(), ("cancelled", "hospital import cancelled")),
        (ValueError("broken"), ("runtime", "broken")),
    ),
)
async def test_refresh_lifecycle_finishes_attempts_on_failure(
    monkeypatch, failure, expected_cleanup
):
    orchestrator = orchestrator_module()
    events: list[str] = []
    hospital_by_field = {
        "hospital_id": "a", "name": "A", "cms_hpt_url": "https://a/locator"
    }
    monkeypatch.setattr(
        orchestrator,
        "selected_hospital_hpt_registry",
        lambda *_args, **_kwargs: (hospital_by_field,),
    )
    monkeypatch.setattr(orchestrator, "ensure_database", AsyncMock())
    monkeypatch.setattr(orchestrator, "positive_env", lambda _name, default: default)
    monkeypatch.setattr(orchestrator, "error_details", lambda exc: ("runtime", str(exc)))
    async def finish_attempts(*_args):
        events.append("attempts_terminal")
        raise RuntimeError("attempt cleanup failed")

    finish = AsyncMock(side_effect=finish_attempts)
    monkeypatch.setattr(orchestrator, "_finish_failed_attempts", finish)

    @contextlib.asynccontextmanager
    async def resource_lock(_store):
        events.append("lock")
        try:
            yield
        finally:
            events.append("unlock")

    async def run_import(_ctx, _task, _hospitals, _store, attempts, *_args):
        attempts.append(SimpleNamespace(attempt_id="attempt"))
        events.append("workers_drained")
        raise failure

    async def guard(_ctx, _task, operation, *_args):
        return await operation

    async def collect():
        events.append("gc")
        raise RuntimeError("gc failed")

    monkeypatch.setattr(orchestrator, "_hospital_resource_lock", resource_lock)
    monkeypatch.setattr(orchestrator, "_run_import", run_import)
    monkeypatch.setattr(orchestrator, "_guard_cancellation", guard)
    monkeypatch.setattr(orchestrator, "garbage_collect_superseded_versions", collect)

    with pytest.raises(type(failure)) as caught:
        await orchestrator.refresh_hospital_prices({}, {"hospital_id": "a"})
    assert caught.value is failure
    finish.assert_awaited_once_with([SimpleNamespace(attempt_id="attempt")], *expected_cleanup)
    assert events == ["lock", "workers_drained", "attempts_terminal", "gc", "unlock"]
