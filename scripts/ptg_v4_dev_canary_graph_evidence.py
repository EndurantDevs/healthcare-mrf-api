"""Evidence aggregation and hard-envelope checks for PTG V4 graph reads."""

from __future__ import annotations

import math
from typing import Any, Mapping, Sequence


MINIMUM_PUBLIC_COLD_SAMPLES = 20
MINIMUM_INTERNAL_PHASE_SAMPLES = 20
GRAPH_IO_FIELDS = (
    "database_bytes",
    "database_blocks",
    "logical_lookups",
)
INTERNAL_MAXIMUM_DATABASE_BYTES = 8_028_416
INTERNAL_MAXIMUM_DATABASE_BLOCKS = 416
INTERNAL_MAXIMUM_LOGICAL_LOOKUPS = 7
INTERNAL_MAXIMUM_GRAPH_PAGES = 208
INTERNAL_MAXIMUM_SOURCE_PAGES = 64
INTERNAL_MAXIMUM_LOCATOR_PAGES = 16
INTERNAL_MAXIMUM_MEMBER_PAGES = 128
INTERNAL_HARD_MAXIMUMS = {
    "database_bytes": INTERNAL_MAXIMUM_DATABASE_BYTES,
    "database_blocks": INTERNAL_MAXIMUM_DATABASE_BLOCKS,
    "logical_lookups": INTERNAL_MAXIMUM_LOGICAL_LOOKUPS,
    "graph_pages": INTERNAL_MAXIMUM_GRAPH_PAGES,
    "source_pages": INTERNAL_MAXIMUM_SOURCE_PAGES,
    "locator_pages": INTERNAL_MAXIMUM_LOCATOR_PAGES,
    "member_pages": INTERNAL_MAXIMUM_MEMBER_PAGES,
}


def collect_graph_read_measurement(
    *,
    public_cold_samples: Sequence[Mapping[str, Any]],
    public_warm_graph_evidence: Mapping[str, Any],
    internal_owner_evidence: Mapping[str, Any],
    provenance: Mapping[str, str],
) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    """Aggregate every measured public and internal graph-read series."""

    failures: list[str] = []
    public_by_field = _public_measurement(
        public_cold_samples,
        public_warm_graph_evidence,
        provenance,
        failures,
    )
    internal_by_field = _internal_measurement(
        internal_owner_evidence,
        provenance,
        failures,
    )
    return public_by_field, internal_by_field, failures


def _public_measurement(
    cold_samples: Sequence[Mapping[str, Any]],
    warm_graph_evidence: Mapping[str, Any],
    provenance: Mapping[str, str],
    failures: list[str],
) -> dict[str, Any]:
    """Summarize all fresh-process reads and the wrapped warm request."""

    if len(cold_samples) < MINIMUM_PUBLIC_COLD_SAMPLES:
        failures.append(
            "graph-read measurement lacks 20 public cold-process samples"
        )
    cold_deltas = [
        _public_delta(
            sample.get("graph_read_evidence"),
            label=f"public cold sample {sample_index}",
            failures=failures,
        )
        for sample_index, sample in enumerate(cold_samples)
    ]
    for sample in cold_samples:
        if (
            sample.get("snapshot_id") != provenance["snapshot_id"]
            or sample.get("reference_snapshot_id")
            != provenance["reference_snapshot_id"]
            or sample.get("image_identity") != provenance["image_identity"]
        ):
            failures.append(
                "public cold graph evidence differs from accepted provenance"
            )
    warm_delta = _public_delta(
        warm_graph_evidence,
        label="public warm wrapped request",
        failures=failures,
    )
    warm_runtime = _mapping(warm_graph_evidence.get("runtime_identity"))
    if warm_runtime.get("image_identity") != provenance["image_identity"]:
        failures.append(
            "public warm graph evidence differs from accepted provenance"
        )
    cold_summary = _series_summary(cold_deltas)
    warm_summary = _series_summary([warm_delta])
    return {
        "cold": cold_summary,
        "warm": warm_summary,
        "maximums": _field_maximums([*cold_deltas, warm_delta]),
    }


def _public_delta(
    evidence: Any,
    *,
    label: str,
    failures: list[str],
) -> dict[str, int]:
    graph_evidence = _mapping(evidence)
    if graph_evidence.get("passed") is not True:
        failures.append(f"{label} graph evidence did not pass collection checks")
    deltas = _mapping(graph_evidence.get("deltas"))
    if _counter(deltas.get("request_count"), label=label, failures=failures) != 1:
        failures.append(f"{label} does not contain exactly one request scope")
    return {
        field_name: _counter(
            deltas.get(field_name),
            label=f"{label} {field_name}",
            failures=failures,
        )
        for field_name in GRAPH_IO_FIELDS
    }


def _internal_measurement(
    evidence: Mapping[str, Any],
    provenance: Mapping[str, str],
    failures: list[str],
) -> dict[str, Any]:
    """Summarize both compiler-selected owners and enforce the hard envelope."""

    if (
        evidence.get("snapshot_id") != provenance["snapshot_id"]
        or evidence.get("reference_snapshot_id")
        != provenance["reference_snapshot_id"]
        or evidence.get("image_identity") != provenance["image_identity"]
    ):
        failures.append(
            "internal graph evidence differs from accepted provenance"
        )
    owners = [
        dict(owner)
        for owner in evidence.get("owners", [])
        if isinstance(owner, Mapping)
    ]
    expected_roles = {"overall_worst", "worst_online_non_override"}
    if {str(owner.get("role") or "") for owner in owners} != expected_roles:
        failures.append(
            "internal graph evidence lacks both compiler-selected owners"
        )
    owner_reports: list[dict[str, Any]] = []
    all_deltas: list[dict[str, int]] = []
    for owner in owners:
        owner_report, owner_deltas = _internal_owner_measurement(
            owner,
            failures,
        )
        owner_reports.append(owner_report)
        all_deltas.extend(owner_deltas)
    return {
        "owners": owner_reports,
        "maximums": _field_maximums(all_deltas),
        "hard_maximums": dict(INTERNAL_HARD_MAXIMUMS),
    }


def _internal_owner_measurement(
    owner: Mapping[str, Any],
    failures: list[str],
) -> tuple[dict[str, Any], list[dict[str, int]]]:
    role = str(owner.get("role") or "unknown")
    source_pages = _owner_source_pages(owner, failures)
    all_deltas: list[dict[str, int]] = []
    report_by_phase: dict[str, Any] = {}
    for phase_name in ("cold", "warm"):
        phase_by_field = _mapping(owner.get(phase_name))
        raw_delta_rows = phase_by_field.get("metric_deltas")
        metric_deltas = (
            list(raw_delta_rows)
            if isinstance(raw_delta_rows, Sequence)
            and not isinstance(raw_delta_rows, (str, bytes))
            else []
        )
        if len(metric_deltas) < MINIMUM_INTERNAL_PHASE_SAMPLES:
            failures.append(
                f"{role} {phase_name} graph measurement lacks 20 samples"
            )
        normalized_deltas = [
            _internal_delta(
                raw_delta,
                label=f"{role} {phase_name} sample {sample_index}",
                source_pages=source_pages,
                failures=failures,
            )
            for sample_index, raw_delta in enumerate(metric_deltas)
        ]
        report_by_phase[phase_name] = _series_summary(normalized_deltas)
        all_deltas.extend(normalized_deltas)
    return (
        {
            "role": role,
            **report_by_phase,
            "maximums": _field_maximums(all_deltas),
            "source_pages": source_pages,
        },
        all_deltas,
    )


def _owner_source_pages(
    owner: Mapping[str, Any],
    failures: list[str],
) -> int:
    if owner.get("mode") == "prefix_override":
        return 0
    compiler_work = _mapping(owner.get("compiler_work"))
    observed = _mapping(compiler_work.get("observed"))
    return _counter(
        observed.get("source_pages"),
        label=f"{owner.get('role')} source pages",
        failures=failures,
    )


def _internal_delta(
    raw_delta: Any,
    *,
    label: str,
    source_pages: int,
    failures: list[str],
) -> dict[str, int]:
    delta = _mapping(raw_delta)
    request_count = _counter(
        delta.get("request_count"),
        label=f"{label} request count",
        failures=failures,
    )
    if request_count != 1:
        failures.append(f"{label} does not contain exactly one request scope")
    normalized_by_field = {
        field_name: _counter(
            delta.get(field_name),
            label=f"{label} {field_name}",
            failures=failures,
        )
        for field_name in GRAPH_IO_FIELDS
    }
    locator_pages = _counter(
        delta.get("hot_group_npi_locator_pages"),
        label=f"{label} locator pages",
        failures=failures,
    )
    member_pages = _counter(
        delta.get("hot_group_npi_member_pages"),
        label=f"{label} member pages",
        failures=failures,
    )
    graph_pages = source_pages + locator_pages + member_pages
    observed_hard_by_field = {
        **normalized_by_field,
        "graph_pages": graph_pages,
        "source_pages": source_pages,
        "locator_pages": locator_pages,
        "member_pages": member_pages,
    }
    if any(
        observed_hard_by_field[field_name] > maximum
        for field_name, maximum in INTERNAL_HARD_MAXIMUMS.items()
    ):
        failures.append(f"{label} exceeds the internal graph-read hard envelope")
    return {
        **normalized_by_field,
        "graph_pages": graph_pages,
    }


def _series_summary(deltas: Sequence[Mapping[str, int]]) -> dict[str, Any]:
    """Return only sample count and maxima for redacted read evidence."""

    return {
        "sample_count": len(deltas),
        "maximums": _field_maximums(deltas),
    }


def _field_maximums(
    deltas: Sequence[Mapping[str, int]],
) -> dict[str, int]:
    """Return per-dimension maxima for one or more read samples."""

    return {
        field_name: max(
            (int(delta.get(field_name) or 0) for delta in deltas),
            default=0,
        )
        for field_name in GRAPH_IO_FIELDS
    }

def _counter(value: Any, *, label: str, failures: list[str]) -> int:
    if isinstance(value, bool):
        failures.append(f"{label} counter is invalid")
        return 0
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        failures.append(f"{label} counter is invalid")
        return 0
    if not math.isfinite(numeric) or numeric < 0 or not numeric.is_integer():
        failures.append(f"{label} counter is invalid")
        return 0
    return int(numeric)


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


__all__ = [
    "GRAPH_IO_FIELDS",
    "INTERNAL_HARD_MAXIMUMS",
    "INTERNAL_MAXIMUM_DATABASE_BLOCKS",
    "INTERNAL_MAXIMUM_DATABASE_BYTES",
    "INTERNAL_MAXIMUM_GRAPH_PAGES",
    "INTERNAL_MAXIMUM_LOCATOR_PAGES",
    "INTERNAL_MAXIMUM_LOGICAL_LOOKUPS",
    "INTERNAL_MAXIMUM_MEMBER_PAGES",
    "INTERNAL_MAXIMUM_SOURCE_PAGES",
    "collect_graph_read_measurement",
]
