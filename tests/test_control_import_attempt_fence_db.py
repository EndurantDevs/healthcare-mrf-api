# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio

import pytest
from sqlalchemy import select

from api import control, control_imports
from db.models import ImportRun, db
from process import control_lifecycle, ptg_control
from process.control_lifecycle import mark_control_run
from process.ptg_parts.frozen_rate_files import (
    FrozenRateFileValidationError,
)
from process.live_progress import (
    reset_live_progress_context,
    set_live_progress_context,
)
from tests.test_control_imports_db import (
    _drop_import_run_schema,
    _reset_import_run_schema,
)
from tests.ptg_frozen_test_support import protected_control_payload


pytestmark = [
    pytest.mark.asyncio(loop_scope="module"),
    pytest.mark.filterwarnings(
        "ignore:coroutine 'Connection._cancel' was never awaited:RuntimeWarning"
    ),
]


async def _insert_attempt_fence_run(
    run_id: str = "run_attempt_fence",
) -> None:
    await db.execute(
        control_imports.insert(ImportRun).values(
            run_id=run_id,
            engine=control_imports.ENGINE_NAME,
            importer="ptg",
            family="pricing",
            status="queued",
            phase_detail="queued",
            params={},
            created_at=control_imports.utc_now(),
            heartbeat_at=control_imports.utc_now(),
            progress={
                "unit": "run",
                "total": 1,
                "done": 0,
                "pct": 0,
                "message": "queued",
            },
        )
    )


class _FrozenValidationFailureHarness:
    def __init__(self, stop_live_heartbeat) -> None:
        self.mark_results: list[tuple[str, bool, str, str]] = []
        self.heartbeat_events: list[str] = []
        self.flushed_run_ids: list[str] = []
        self._stop_live_heartbeat = stop_live_heartbeat
        self._thread_heartbeat_token = object()

    async def no_stale_run(self, _run_id):
        return None

    async def mark_with_real_fence(self, *args, **kwargs):
        accepted = await mark_control_run(*args, **kwargs)
        self.mark_results.append(
            (
                kwargs["status"],
                accepted,
                kwargs["attempt_id"],
                kwargs["attempt_started_at"],
            )
        )
        return accepted

    async def idle_live_heartbeat(self, *_args, **_kwargs):
        await asyncio.Event().wait()

    def start_thread_heartbeat(self, *_args, **_kwargs):
        self.heartbeat_events.append("thread-start")
        return self._thread_heartbeat_token

    def stop_thread_heartbeat(self, stop_token):
        if stop_token is None:
            self.heartbeat_events.append("thread-stop-empty")
            return
        assert stop_token is self._thread_heartbeat_token
        self.heartbeat_events.append("thread-stop")

    async def stop_live_heartbeat(self, heartbeat_task):
        self.heartbeat_events.append(
            "live-stop" if heartbeat_task is not None else "live-stop-empty"
        )
        await self._stop_live_heartbeat(heartbeat_task)

    def fail_if_lane_selected(self, _params_by_name):
        raise AssertionError("frozen validation failure selected a lane")

    async def fail_if_engine_started(self, **_kwargs):
        raise AssertionError("frozen validation failure started the engine")

    async def flush_status(self, run_id) -> None:
        self.flushed_run_ids.append(run_id)


def _install_frozen_failure_harness(
    monkeypatch,
    harness: _FrozenValidationFailureHarness,
) -> None:
    async def admit_worker_start(*_args, **_kwargs):
        return None

    monkeypatch.setattr(
        ptg_control,
        "guard_ptg_worker_start",
        admit_worker_start,
    )
    monkeypatch.setattr(
        ptg_control,
        "_stale_ptg_job_result",
        harness.no_stale_run,
    )
    monkeypatch.setattr(
        ptg_control,
        "mark_control_run",
        harness.mark_with_real_fence,
    )
    monkeypatch.setattr(
        ptg_control,
        "_live_progress_heartbeat",
        harness.idle_live_heartbeat,
    )
    monkeypatch.setattr(
        ptg_control,
        "_start_threaded_ptg_heartbeat",
        harness.start_thread_heartbeat,
    )
    monkeypatch.setattr(
        ptg_control,
        "_stop_threaded_ptg_heartbeat",
        harness.stop_thread_heartbeat,
    )
    monkeypatch.setattr(
        ptg_control,
        "_stop_live_progress_heartbeat",
        harness.stop_live_heartbeat,
    )
    monkeypatch.setattr(
        ptg_control,
        "_assert_expected_lane",
        harness.fail_if_lane_selected,
    )
    monkeypatch.setattr(
        ptg_control,
        "ptg_main",
        harness.fail_if_engine_started,
    )
    monkeypatch.setattr(
        ptg_control,
        "_flush_terminal_status_events",
        harness.flush_status,
    )


async def _apply_fenced_attempt_transitions() -> tuple[str, str]:
    newer_id = "run_attempt_fence:newer"
    newer_started_at = "2026-07-23T12:00:00.000000+00:00"
    older_id = "run_attempt_fence:older"
    older_started_at = "2026-07-23T11:00:00.000000+00:00"

    assert await mark_control_run(
        "run_attempt_fence",
        status="running",
        phase_detail="newer running",
        progress_message="running",
        attempt_id=newer_id,
        attempt_started_at=newer_started_at,
    )
    await _apply_nested_progress(newer_id, newer_started_at)
    await _reject_older_attempt(older_id, older_started_at)
    assert await mark_control_run(
        "run_attempt_fence",
        status="succeeded",
        phase_detail="newer succeeded",
        progress_message="succeeded",
        attempt_id=newer_id,
        attempt_started_at=newer_started_at,
    )
    await _reject_terminal_restarts()
    return newer_id, newer_started_at


async def _apply_nested_progress(
    attempt_id: str,
    attempt_started_at: str,
) -> None:
    live_token = set_live_progress_context(
        run_id="run_attempt_fence",
        attempt_id=attempt_id,
        attempt_started_at=attempt_started_at,
    )
    try:
        assert await mark_control_run(
            "run_attempt_fence",
            status="running",
            phase_detail="nested target progress",
            progress_message="working",
        )
    finally:
        reset_live_progress_context(live_token)


async def _reject_older_attempt(
    attempt_id: str,
    attempt_started_at: str,
) -> None:
    for status, detail, message in (
        ("running", "older delayed start", "running"),
        ("failed", "older delayed terminal", "failed"),
    ):
        assert not await mark_control_run(
            "run_attempt_fence",
            status=status,
            phase_detail=detail,
            progress_message=message,
            attempt_id=attempt_id,
            attempt_started_at=attempt_started_at,
        )


async def _reject_terminal_restarts() -> None:
    assert not await mark_control_run(
        "run_attempt_fence",
        status="running",
        phase_detail="late retry start after terminal",
        progress_message="running",
        attempt_id="run_attempt_fence:late",
        attempt_started_at="2026-07-23T13:00:00.000000+00:00",
    )
    assert not await mark_control_run(
        "run_attempt_fence",
        status="running",
        phase_detail="legacy late start after terminal",
        progress_message="running",
    )


async def test_postgres_attempt_claim_and_terminal_updates_are_fenced(
    monkeypatch,
):
    """Prove older start and terminal writes affect zero PostgreSQL rows."""

    await _reset_import_run_schema()
    try:
        monkeypatch.setattr(
            control_lifecycle,
            "write_live_progress",
            lambda **_payload: True,
        )
        await _insert_attempt_fence_run()
        newer_id, newer_started_at = await _apply_fenced_attempt_transitions()

        stored_run = (
            await db.execute(
                select(ImportRun).where(
                    ImportRun.run_id == "run_attempt_fence"
                )
            )
        ).scalar_one()
        assert stored_run.status == "succeeded"
        assert stored_run.progress["attempt_id"] == newer_id
        assert stored_run.progress["attempt_started_at"] == newer_started_at
    finally:
        await _drop_import_run_schema()


async def _raise_frozen_validation_failure(run_id: str) -> None:
    protected_request = control._validated_control_import_payload(
        protected_control_payload()
    )
    with pytest.raises(
        FrozenRateFileValidationError,
        match="outer and nested",
    ):
        await ptg_control.ptg_control_start(
            {},
            {
                "run_id": run_id,
                "source_file_import_id": "drifted-source-file-import",
                "import_id": protected_request["import_id"],
                "params": protected_request["params"],
            },
        )


async def _assert_frozen_failure_persisted(
    run_id: str,
    harness: _FrozenValidationFailureHarness,
) -> None:
    stored_run = (
        await db.execute(
            select(ImportRun).where(ImportRun.run_id == run_id)
        )
    ).scalar_one()
    assert [
        (status, accepted)
        for status, accepted, _attempt_id, _started_at
        in harness.mark_results
    ] == [("running", True), ("failed", True)]
    assert harness.mark_results[0][2:] == harness.mark_results[1][2:]
    assert harness.heartbeat_events == [
        "thread-start",
        "thread-stop",
        "live-stop",
    ]
    assert harness.flushed_run_ids == [run_id]
    assert stored_run.status == "failed"
    assert stored_run.phase_detail == "ptg import failed"
    assert stored_run.error == {
        "code": "ptg_frozen_rate_file_contract_failed",
        "message": (
            "protected outer and nested source_file_import_id and "
            "import_id must all match"
        ),
        "retryable": False,
    }
    assert stored_run.progress["attempt_id"] == harness.mark_results[0][2]
    assert stored_run.finished_at is not None


async def test_frozen_validation_failure_claims_then_persists_terminal_attempt(
    monkeypatch,
):
    """Use the real PostgreSQL attempt fence for claim and terminal failure."""

    run_id = "run_frozen_validation_failure"
    await _reset_import_run_schema()
    try:
        monkeypatch.setattr(
            control_lifecycle,
            "write_live_progress",
            lambda **_payload: True,
        )
        monkeypatch.setattr(
            control_lifecycle,
            "read_live_progress",
            lambda _run_id: None,
        )
        harness = _FrozenValidationFailureHarness(
            ptg_control._stop_live_progress_heartbeat
        )
        _install_frozen_failure_harness(monkeypatch, harness)
        await _insert_attempt_fence_run(run_id)
        await _raise_frozen_validation_failure(run_id)
        await _assert_frozen_failure_persisted(run_id, harness)
    finally:
        await _drop_import_run_schema()
