# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import os
import re
from typing import Any

from sanic import Blueprint, response
from sqlalchemy import func, select

from api.control_imports import ACTIVE_STATUSES, node_health
from api.ptg2_v4_graph import v4_graph_metrics_snapshot
from api.runtime_identity import runtime_identity
from db.models import ImportRun, db

blueprint = Blueprint("metrics")

_V4_GRAPH_COUNTER_METRICS = (
    ("database_blocks", "hp_mrf_ptg_v4_graph_database_blocks_total"),
    ("cache_hit_bytes", "hp_mrf_ptg_v4_graph_cache_hit_bytes_total"),
    ("logical_lookups", "hp_mrf_ptg_v4_graph_logical_lookups_total"),
    ("bitmap_owner_hits", "hp_mrf_ptg_v4_graph_bitmap_owner_hits_total"),
    (
        "component_fallback_sets",
        "hp_mrf_ptg_v4_graph_component_fallback_sets_total",
    ),
    ("hot_prefix_requests", "hp_mrf_ptg_v4_graph_hot_prefix_requests_total"),
    ("cold_exact_requests", "hp_mrf_ptg_v4_graph_cold_exact_requests_total"),
    (
        "npi_prefix_override_sets",
        "hp_mrf_ptg_v4_graph_npi_prefix_override_sets_total",
    ),
    ("hot_group_npi_members", "hp_mrf_ptg_v4_graph_hot_group_npi_members_total"),
    (
        "hot_group_npi_locator_pages",
        "hp_mrf_ptg_v4_graph_hot_group_npi_locator_pages_total",
    ),
    (
        "hot_group_npi_member_pages",
        "hp_mrf_ptg_v4_graph_hot_group_npi_member_pages_total",
    ),
    ("hot_group_npi_bytes", "hp_mrf_ptg_v4_graph_hot_group_npi_bytes_total"),
    ("hot_group_npi_batches", "hp_mrf_ptg_v4_graph_hot_group_npi_batches_total"),
    (
        "hot_npi_dictionary_reads",
        "hp_mrf_ptg_v4_graph_hot_npi_dictionary_reads_total",
    ),
    (
        "provider_expansion_rate_rows",
        "hp_mrf_ptg_v4_provider_expansion_rate_rows_total",
    ),
    (
        "provider_expansion_provider_sets",
        "hp_mrf_ptg_v4_provider_expansion_provider_sets_total",
    ),
    (
        "provider_expansion_graph_batches",
        "hp_mrf_ptg_v4_provider_expansion_graph_batches_total",
    ),
    (
        "provider_expansion_rejections",
        "hp_mrf_ptg_v4_provider_expansion_rejections_total",
    ),
)


@blueprint.get("/metrics")
async def prometheus_metrics(request):
    """Serve authenticated Prometheus metrics for the API node."""
    if _is_metrics_auth_required():
        from api.control import _require_control_auth

        _require_control_auth(request)
    return response.text(await render_prometheus_metrics(), content_type="text/plain; version=0.0.4")


async def render_prometheus_metrics() -> str:
    """Render current node and importer state in Prometheus format."""
    health = await node_health()
    status_counts, importer_counts = await _active_run_counts()
    lines = _engine_metric_lines(health, status_counts, importer_counts)
    lines.extend(_runtime_metric_lines(health))
    lines.extend(_process_identity_metric_lines(runtime_identity()))
    lines.extend(_v4_graph_metric_lines(v4_graph_metrics_snapshot()))
    return "\n".join(lines) + "\n"


def _engine_metric_lines(
    health: dict[str, Any],
    status_counts: dict[str, int],
    importer_counts: dict[str, int],
) -> list[str]:
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
    return lines


def _runtime_metric_lines(health: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    disk_stats = health.get("disk") if isinstance(health.get("disk"), dict) else {}
    if isinstance(disk_stats.get("free"), (int, float)):
        lines.append(
            _metric(
                "hp_mrf_api_disk_free_bytes",
                disk_stats["free"],
                node_id=str(health.get("node_id") or "unknown"),
            )
        )
    queue_depth_by_name = (
        health.get("queue_depth") if isinstance(health.get("queue_depth"), dict) else {}
    )
    for queue_name, depth in sorted(queue_depth_by_name.items()):
        if isinstance(depth, (int, float)):
            lines.append(
                _metric(
                    "hp_mrf_api_queue_depth",
                    depth,
                    queue=str(queue_name),
                )
            )
    worker_state_by_queue = (
        health.get("workers") if isinstance(health.get("workers"), dict) else {}
    )
    for queue_name, worker_state in sorted(worker_state_by_queue.items()):
        if isinstance(worker_state, dict):
            lines.append(
                _metric(
                    "hp_mrf_api_worker_running",
                    1 if worker_state.get("running") else 0,
                    queue=str(queue_name),
                )
            )
    return lines


def _process_identity_metric_lines(identity_by_field: dict[str, str]) -> list[str]:
    return [
        "# HELP hp_mrf_api_process_identity_info Exact API process and image identity for direct-pod canaries.",
        "# TYPE hp_mrf_api_process_identity_info gauge",
        _metric(
            "hp_mrf_api_process_identity_info",
            1,
            identity=identity_by_field["process_identity"],
            image=identity_by_field["image_identity"],
            started_at=identity_by_field["process_started_at"],
        ),
    ]


def _v4_graph_metric_lines(graph_metrics_by_name: dict[str, Any]) -> list[str]:
    lines = [
        "# HELP hp_mrf_ptg_v4_graph_database_bytes_per_request Bytes fetched from PostgreSQL for packed V4 graph pages per API request.",
        "# TYPE hp_mrf_ptg_v4_graph_database_bytes_per_request histogram",
    ]
    bucket_count_by_upper_bound = graph_metrics_by_name["buckets"]
    for upper_bound, count in sorted(bucket_count_by_upper_bound.items()):
        lines.append(
            _metric(
                "hp_mrf_ptg_v4_graph_database_bytes_per_request_bucket",
                count,
                le=str(upper_bound),
            )
        )
    lines.append(
        _metric(
            "hp_mrf_ptg_v4_graph_database_bytes_per_request_bucket",
            graph_metrics_by_name["infinite_bucket_count"],
            le="+Inf",
        )
    )
    lines.append(
        _metric(
            "hp_mrf_ptg_v4_graph_database_bytes_per_request_sum",
            graph_metrics_by_name["database_bytes"],
        )
    )
    lines.append(
        _metric(
            "hp_mrf_ptg_v4_graph_database_bytes_per_request_count",
            graph_metrics_by_name["request_count"],
        )
    )
    lines.extend(
        _metric(metric_name, graph_metrics_by_name[source_name])
        for source_name, metric_name in _V4_GRAPH_COUNTER_METRICS
    )
    return lines


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


def _is_metrics_auth_required() -> bool:
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
