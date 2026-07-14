# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import os
import re
from typing import Any

from sanic import Blueprint, response
from sqlalchemy import func, select

from api.control_imports import ACTIVE_STATUSES, node_health
from db.models import ImportRun, db

blueprint = Blueprint("metrics")


@blueprint.get("/metrics")
async def prometheus_metrics(request):
    """Serve authenticated Prometheus metrics for the API node."""
    if _metrics_require_auth():
        from api.control import _require_control_auth

        _require_control_auth(request)
    return response.text(await render_prometheus_metrics(), content_type="text/plain; version=0.0.4")


async def render_prometheus_metrics() -> str:
    """Render current node and importer state in Prometheus format."""
    health = await node_health()
    status_counts, importer_counts = await _active_run_counts()
    lines = [
        "# HELP hp_mrf_api_node_health Engine node health status, 1 for ok and 0 for degraded.",
        "# TYPE hp_mrf_api_node_health gauge",
        _metric(
            "hp_mrf_api_node_health",
            1 if health.get("status") == "ok" else 0,
            node_id=str(health.get("node_id") or "unknown"),
            status=str(health.get("status") or "unknown"),
        ),
        "# HELP hp_mrf_api_active_runs_by_status Active engine import runs grouped by status.",
        "# TYPE hp_mrf_api_active_runs_by_status gauge",
    ]
    for status, count in sorted(status_counts.items()):
        lines.append(_metric("hp_mrf_api_active_runs_by_status", count, status=status))
    lines.extend(
        [
            "# HELP hp_mrf_api_active_runs_by_importer Active engine import runs grouped by importer.",
            "# TYPE hp_mrf_api_active_runs_by_importer gauge",
        ]
    )
    for importer, count in sorted(importer_counts.items()):
        lines.append(_metric("hp_mrf_api_active_runs_by_importer", count, importer=importer))
    disk = health.get("disk") if isinstance(health.get("disk"), dict) else {}
    if isinstance(disk.get("free"), (int, float)):
        lines.append(_metric("hp_mrf_api_disk_free_bytes", disk["free"], node_id=str(health.get("node_id") or "unknown")))
    queue_depth = health.get("queue_depth") if isinstance(health.get("queue_depth"), dict) else {}
    for queue, depth in sorted(queue_depth.items()):
        if isinstance(depth, (int, float)):
            lines.append(_metric("hp_mrf_api_queue_depth", depth, queue=str(queue)))
    workers = health.get("workers") if isinstance(health.get("workers"), dict) else {}
    for queue, item in sorted(workers.items()):
        if isinstance(item, dict):
            lines.append(_metric("hp_mrf_api_worker_running", 1 if item.get("running") else 0, queue=str(queue)))
    return "\n".join(lines) + "\n"


async def _active_run_counts() -> tuple[dict[str, int], dict[str, int]]:
    stmt = (
        select(ImportRun.status, ImportRun.importer, func.count().label("count"))
        .where(ImportRun.status.in_(ACTIVE_STATUSES))
        .group_by(ImportRun.status, ImportRun.importer)
    )
    rows = (await db.execute(stmt)).all()
    by_status: dict[str, int] = {}
    by_importer: dict[str, int] = {}
    for status, importer, count in rows:
        parsed_count = int(count or 0)
        by_status[str(status)] = by_status.get(str(status), 0) + parsed_count
        by_importer[str(importer)] = by_importer.get(str(importer), 0) + parsed_count
    return by_status, by_importer


def _metrics_require_auth() -> bool:
    return str(os.getenv("HLTHPRT_METRICS_REQUIRE_AUTH", "1")).strip().lower() not in {"0", "false", "no"}


def _metric(name: str, value: int | float, **labels: str) -> str:
    if labels:
        label_text = ",".join(f'{_label_name(key)}="{_label_value(value)}"' for key, value in sorted(labels.items()))
        return f"{name}{{{label_text}}} {float(value):.6f}"
    return f"{name} {float(value):.6f}"


def _label_name(value: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9_]", "_", str(value))
    return text or "label"


def _label_value(value: Any) -> str:
    return str(value or "").replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')
