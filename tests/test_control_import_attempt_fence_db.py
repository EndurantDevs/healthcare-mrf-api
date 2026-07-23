# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import pytest
from sqlalchemy import select

from api import control_imports
from db.models import ImportRun, db
from process import control_lifecycle
from process.control_lifecycle import mark_control_run
from process.live_progress import (
    reset_live_progress_context,
    set_live_progress_context,
)
from tests.test_control_imports_db import (
    _drop_import_run_schema,
    _reset_import_run_schema,
)


pytestmark = [
    pytest.mark.asyncio(loop_scope="module"),
    pytest.mark.filterwarnings(
        "ignore:coroutine 'Connection._cancel' was never awaited:RuntimeWarning"
    ),
]


async def _insert_attempt_fence_run() -> None:
    await db.execute(
        control_imports.insert(ImportRun).values(
            run_id="run_attempt_fence",
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
