# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from typing import Any


class ImportCancelledError(RuntimeError):
    """Raised when the control-plane cancel flag is set for a run."""


def run_id_from_task(task: dict[str, Any] | None) -> str | None:
    if not isinstance(task, dict):
        return None
    run_id = str(task.get("run_id") or "").strip()
    return run_id or None


async def raise_if_cancelled(ctx: dict[str, Any], task: dict[str, Any] | None) -> None:
    run_id = run_id_from_task(task)
    if not run_id:
        return
    redis = ctx.get("redis")
    if redis is None:
        return
    value = await redis.get(f"cancel:{run_id}")
    if value in {b"1", "1", 1, True}:
        raise ImportCancelledError(f"import run {run_id} was cancelled")
