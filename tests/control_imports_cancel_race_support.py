# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from sqlalchemy import update

from api import control_imports
from db.models import ImportRun, db


async def insert_queued_cancel_race_run() -> None:
    """Insert a queued run that can finish while cancellation is signaling."""
    now = control_imports.utc_now()
    await db.execute(
        control_imports.insert(ImportRun).values(
            run_id="run_cancel_terminal_race",
            engine=control_imports.ENGINE_NAME,
            importer="provider-directory-fhir",
            family="provider",
            status="queued",
            phase_detail="queued",
            params={},
            created_at=now,
            heartbeat_at=now,
            progress={
                "unit": "run",
                "total": 1,
                "done": 0,
                "pct": 0,
                "message": "queued",
            },
            metrics={"enqueue_adapter": "arq_single_job"},
        )
    )


async def finish_cancel_race_run(_run, **_options) -> dict:
    """Commit terminal worker truth during the cancellation signaling window."""
    finished_at = control_imports.utc_now()
    await db.execute(
        update(ImportRun)
        .where(ImportRun.run_id == "run_cancel_terminal_race")
        .values(
            status="succeeded",
            phase_detail="worker succeeded",
            heartbeat_at=finished_at,
            finished_at=finished_at,
            progress={
                "unit": "run",
                "total": 1,
                "done": 1,
                "pct": 100,
                "message": "succeeded",
            },
            metrics={"rows": 10},
        )
    )
    return {
        "redis": True,
        "removed": False,
        "cancel_flag": {"redis": True},
        "kubernetes": {"enabled": True, "deleted": 0, "items": []},
    }
