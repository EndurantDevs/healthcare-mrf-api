# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Attempt-scoped heartbeat and stale-job helpers for controlled PTG runs."""

from __future__ import annotations

import os
import threading
from typing import Any

from db.models import db
from process.live_progress import write_live_progress


PTG_CONTROL_HEARTBEAT_SOURCE = "engine-heartbeat"
_TERMINAL_RUN_STATUSES = {
    "succeeded",
    "failed",
    "canceled",
    "cancelled",
    "dead_letter",
}


def _start_threaded_ptg_heartbeat(
    run_id: str,
    started_at: str,
    *,
    attempt_id: str | None = None,
) -> threading.Event:
    """Start the thread heartbeat used while the scanner blocks its event loop."""

    stop_event = threading.Event()
    interval = float(
        os.getenv("HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS", "15")
    )
    if interval <= 0:
        return stop_event

    def _heartbeat() -> None:
        while not stop_event.wait(interval):
            write_live_progress(
                run_id=run_id,
                importer="ptg",
                status="running",
                phase="ptg import running",
                unit="run",
                done=0,
                total=1,
                pct=0,
                message="running",
                started_at=started_at,
                attempt_id=attempt_id,
                attempt_started_at=started_at,
                source=PTG_CONTROL_HEARTBEAT_SOURCE,
                confidence="heartbeat",
                publish_event=False,
            )

    heartbeat_thread = threading.Thread(
        target=_heartbeat,
        name=f"ptg-heartbeat-{run_id[:12]}",
        daemon=True,
    )
    heartbeat_thread.start()
    return stop_event


def _stop_threaded_ptg_heartbeat(
    stop_event: threading.Event | None,
) -> None:
    """Request shutdown of a scanner heartbeat thread when one was started."""

    if stop_event is not None:
        stop_event.set()


async def _stale_ptg_job_result(run_id: str) -> dict[str, Any] | None:
    """Return the skip response for a terminal run, otherwise allow execution."""

    if not run_id:
        return None
    database_row = await db.first(
        """
        SELECT ir.status
          FROM mrf.import_run ir
         WHERE ir.run_id = :run_id
         LIMIT 1
        """,
        run_id=run_id,
    )
    if database_row is None:
        return None
    run_status = str(database_row[0] or "").strip().lower()
    if run_status in _TERMINAL_RUN_STATUSES:
        return {
            "status": "skipped",
            "run_id": run_id,
            "reason": f"run_{run_status}",
        }
    return None
