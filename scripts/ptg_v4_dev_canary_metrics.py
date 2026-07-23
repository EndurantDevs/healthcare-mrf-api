"""Prometheus parsing and bounded graph-read gates for the PTG V4 canary."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any, Mapping


GRAPH_METRIC_BY_FIELD = {
    "request_count": "hp_mrf_ptg_v4_graph_database_bytes_per_request_count",
    "database_bytes": "hp_mrf_ptg_v4_graph_database_bytes_per_request_sum",
    "database_blocks": "hp_mrf_ptg_v4_graph_database_blocks_total",
    "logical_lookups": "hp_mrf_ptg_v4_graph_logical_lookups_total",
    "component_fallbacks": "hp_mrf_ptg_v4_graph_component_fallback_sets_total",
    "provider_expansion_rate_rows": (
        "hp_mrf_ptg_v4_provider_expansion_rate_rows_total"
    ),
    "provider_expansion_provider_sets": (
        "hp_mrf_ptg_v4_provider_expansion_provider_sets_total"
    ),
    "provider_expansion_graph_batches": (
        "hp_mrf_ptg_v4_provider_expansion_graph_batches_total"
    ),
    "provider_expansion_rejections": (
        "hp_mrf_ptg_v4_provider_expansion_rejections_total"
    ),
}
_PROMETHEUS_SAMPLE_PATTERN = re.compile(
    r"^([A-Za-z_:][A-Za-z0-9_:]*)(?:\{[^}]*\})?\s+"
    r"([-+]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][-+]?[0-9]+)?)$"
)
_PROCESS_IDENTITY_PATTERN = re.compile(
    r'^hp_mrf_api_process_identity_info\{'
    r'identity="([0-9a-f]{64})",'
    r'image="([^"\\]+)",'
    r'started_at="([^"\\]+)"'
    r'\}\s+1(?:\.0+)?$'
)


@dataclass(frozen=True)
class GraphReadLimits:
    """Optional reviewed maxima plus required direct-pod traversal shape."""

    database_bytes: int | None
    database_blocks: int | None
    logical_lookups: int | None
    minimum_request_scopes: int
    maximum_request_scopes: int
    component_fallback_mode: str


def parse_graph_metrics(metric_text: str) -> dict[str, float]:
    """Extract the unlabelled V4 counters needed for one direct-pod delta."""

    metric_name_set = set(GRAPH_METRIC_BY_FIELD.values())
    metric_value_by_name: dict[str, float] = {}
    for raw_line in str(metric_text).splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = _PROMETHEUS_SAMPLE_PATTERN.fullmatch(line)
        if match is None or match.group(1) not in metric_name_set:
            continue
        metric_name = match.group(1)
        metric_value = float(match.group(2))
        if not math.isfinite(metric_value) or metric_name in metric_value_by_name:
            raise ValueError("V4 Prometheus evidence is duplicated or non-finite")
        metric_value_by_name[metric_name] = metric_value
    missing_names = metric_name_set - set(metric_value_by_name)
    if missing_names:
        raise ValueError("V4 Prometheus evidence lacks required counters")
    return {
        field_name: metric_value_by_name[metric_name]
        for field_name, metric_name in GRAPH_METRIC_BY_FIELD.items()
    }


def parse_runtime_identity_metric(metric_text: str) -> dict[str, str]:
    """Extract exactly one process/image identity tuple from Prometheus text."""

    identities = [
        match.groups()
        for raw_line in str(metric_text).splitlines()
        if (
            match := _PROCESS_IDENTITY_PATTERN.fullmatch(raw_line.strip())
        )
    ]
    if len(identities) != 1 or not all(identities[0]):
        raise ValueError("Prometheus evidence lacks one runtime identity")
    process_identity, image_identity, process_started_at = identities[0]
    return {
        "process_identity": process_identity,
        "image_identity": image_identity,
        "process_started_at": process_started_at,
    }


def evaluate_graph_metric_delta(
    before_by_field: Mapping[str, float],
    after_by_field: Mapping[str, float],
    *,
    limits: GraphReadLimits,
) -> dict[str, Any]:
    """Require one observed request and bounded graph I/O counter deltas."""

    failures: list[str] = []
    deltas_by_field = {
        field_name: float(after_by_field.get(field_name, 0))
        - float(before_by_field.get(field_name, 0))
        for field_name in GRAPH_METRIC_BY_FIELD
    }
    request_count = deltas_by_field["request_count"]
    if not limits.minimum_request_scopes <= request_count <= limits.maximum_request_scopes:
        failures.append("direct-pod V4 request_count delta is outside its bound")
    maximum_by_field = {
        "database_bytes": limits.database_bytes,
        "database_blocks": limits.database_blocks,
        "logical_lookups": limits.logical_lookups,
    }
    for field_name, maximum in maximum_by_field.items():
        delta = deltas_by_field[field_name]
        if delta < 0 or (maximum is not None and delta > maximum):
            failures.append(f"direct-pod {field_name} delta exceeds its bound")
    _validate_component_fallback_delta(
        deltas_by_field["component_fallbacks"],
        mode=limits.component_fallback_mode,
        failures=failures,
    )
    return {
        "passed": not failures,
        "failures": failures,
        "deltas": deltas_by_field,
        "maximums": maximum_by_field,
        "minimum_request_scopes": limits.minimum_request_scopes,
        "maximum_request_scopes": limits.maximum_request_scopes,
        "component_fallback_mode": limits.component_fallback_mode,
    }


def _validate_component_fallback_delta(
    fallback_delta: float,
    *,
    mode: str,
    failures: list[str],
) -> None:
    """Validate required, forbidden, or explicitly allowed fallback behavior."""

    if mode not in {"required", "forbidden", "allowed"}:
        failures.append("component fallback mode is invalid")
    if mode == "required" and fallback_delta < 1:
        failures.append("overflow traversal did not exercise component fallback")
    if mode == "forbidden" and fallback_delta != 0:
        failures.append("ordinary traversal unexpectedly exercised component fallback")
