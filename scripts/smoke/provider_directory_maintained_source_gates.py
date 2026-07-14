#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Gate maintained Provider Directory sources using explicit observation JSON.

Each observation file must contain either a JSON list or an object with an
``observations`` list. Every observation identifies one maintained source with
``entry_id`` and ``source_id``. Acquisition observations also carry
``dataset_id``, ``is_current``, and ``terminal_state``. Downstream observation
files must use that exact dataset ID; they cannot rely on catalog documentation
or a prior snapshot for readiness.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCES_PATH = ROOT / "specs" / "provider_directory_profile_sources.json"
STATUS_READY = "ready"
STATUS_NOT_READY = "not_ready"
STATUS_FAILED = "failed"
STATUS_UNKNOWN = "unknown"
STATUS_VALUES = (STATUS_READY, STATUS_NOT_READY, STATUS_FAILED, STATUS_UNKNOWN)
TERMINAL_SUCCESS = {"complete", "completed", "pass", "passed", "ready", "success", "succeeded"}
TERMINAL_FAILURE = {"error", "failed", "failure"}
TERMINAL_PENDING = {"active", "pending", "running", "started"}


class ObservationValidationError(ValueError):
    """Raised when an observation file cannot prove one source-scoped result."""


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise ObservationValidationError(f"cannot read {path}: {error}") from error
    except json.JSONDecodeError as error:
        raise ObservationValidationError(f"invalid JSON in {path}: {error.msg}") from error


def _observation_list(document: Any, *, label: str) -> list[dict[str, Any]]:
    candidate_list = document.get("observations") if isinstance(document, dict) else document
    if not isinstance(candidate_list, list):
        raise ObservationValidationError(f"{label} must be a JSON list or object with observations")
    if not all(isinstance(observation, dict) for observation in candidate_list):
        raise ObservationValidationError(f"{label} observations must be JSON objects")
    return candidate_list


def load_maintained_sources(path: Path = DEFAULT_SOURCES_PATH) -> list[dict[str, str]]:
    """Load the ordered maintained source registry, rejecting inconsistent IDs."""
    document = _load_json(path)
    matrix = document.get("verification_matrix") if isinstance(document, dict) else None
    source_list = matrix.get("sources") if isinstance(matrix, dict) else None
    if not isinstance(source_list, list):
        raise ObservationValidationError(f"{path} lacks verification_matrix.sources")
    maintained_sources = [
        {"entry_id": str(source.get("entry_id") or ""), "source_id": str(source.get("source_id") or "")}
        for source in source_list
        if isinstance(source, dict)
    ]
    entry_id_list = [source["entry_id"] for source in maintained_sources]
    source_id_list = [source["source_id"] for source in maintained_sources]
    if not maintained_sources or "" in entry_id_list or "" in source_id_list:
        raise ObservationValidationError(f"{path} has incomplete maintained source identities")
    if len(entry_id_list) != len(set(entry_id_list)) or len(source_id_list) != len(set(source_id_list)):
        raise ObservationValidationError(f"{path} has duplicate maintained source identities")
    return maintained_sources


def load_source_observations(
    path: Path,
    *,
    label: str,
    maintained_sources: list[dict[str, str]],
) -> dict[str, dict[str, Any]]:
    """Load exactly one explicit observation for every maintained source."""
    expected_source_id_by_entry = {
        maintained_source["entry_id"]: maintained_source["source_id"]
        for maintained_source in maintained_sources
    }
    observation_by_entry: dict[str, dict[str, Any]] = {}
    for observation in _observation_list(_load_json(path), label=label):
        entry_id = str(observation.get("entry_id") or "")
        source_id = str(observation.get("source_id") or "")
        if entry_id not in expected_source_id_by_entry:
            raise ObservationValidationError(f"{label} has unknown entry_id {entry_id!r}")
        if source_id != expected_source_id_by_entry[entry_id]:
            raise ObservationValidationError(
                f"{label} source_id mismatch for {entry_id}: expected "
                f"{expected_source_id_by_entry[entry_id]!r}, got {source_id!r}"
            )
        if entry_id in observation_by_entry:
            raise ObservationValidationError(f"{label} has duplicate observation for {entry_id}")
        observation_by_entry[entry_id] = observation
    missing_entry_id_list = [
        maintained_source["entry_id"]
        for maintained_source in maintained_sources
        if maintained_source["entry_id"] not in observation_by_entry
    ]
    if missing_entry_id_list:
        raise ObservationValidationError(f"{label} misses maintained sources: {', '.join(missing_entry_id_list)}")
    return observation_by_entry


def _dataset_id(observation: dict[str, Any]) -> str | None:
    value = observation.get("dataset_id")
    if value in (None, ""):
        return None
    if not isinstance(value, str):
        raise ObservationValidationError("dataset_id must be a string or null")
    return value


def validate_dataset_alignment(
    acquisition_by_entry: dict[str, dict[str, Any]],
    observation_maps_by_label: dict[str, dict[str, dict[str, Any]]],
) -> None:
    """Reject downstream evidence that is stale relative to its source dataset."""
    for label, observation_by_entry in observation_maps_by_label.items():
        for entry_id, observation in observation_by_entry.items():
            acquisition_dataset_id = _dataset_id(acquisition_by_entry[entry_id])
            observed_dataset_id = _dataset_id(observation)
            if observed_dataset_id != acquisition_dataset_id:
                raise ObservationValidationError(
                    f"{label} dataset_id mismatch for {entry_id}: expected "
                    f"{acquisition_dataset_id!r}, got {observed_dataset_id!r}"
                )


def _explicit_status(observation: dict[str, Any], field_names: tuple[str, ...]) -> str:
    for field_name in field_names:
        value = observation.get(field_name)
        if isinstance(value, bool):
            return STATUS_READY if value else STATUS_NOT_READY
        if isinstance(value, str):
            normalized = value.strip().lower().replace("-", "_")
            if normalized in {"pass", "passed", "ready", "success", "succeeded"}:
                return STATUS_READY
            if normalized in {"fail", "failed", "error", "failure"}:
                return STATUS_FAILED
            if normalized in {"not_ready", "not_observed", "missing", "unavailable"}:
                return STATUS_NOT_READY
            if normalized in {"unknown", "inconclusive"}:
                return STATUS_UNKNOWN
    return STATUS_UNKNOWN


def _metric(observation: dict[str, Any], field_names: tuple[str, ...]) -> int | float | None:
    for field_name in field_names:
        value = observation.get(field_name)
        if isinstance(value, bool):
            raise ObservationValidationError(f"{field_name} must be numeric, not boolean")
        if isinstance(value, (int, float)):
            if value < 0:
                raise ObservationValidationError(f"{field_name} must not be negative")
            return value
    return None


def acquisition_status(observation: dict[str, Any]) -> str:
    """Evaluate terminal, current acquisition evidence without static fallbacks."""
    terminal_state = str(observation.get("terminal_state") or observation.get("acquisition_state") or "").lower()
    if terminal_state in TERMINAL_FAILURE:
        return STATUS_FAILED
    if terminal_state in TERMINAL_PENDING:
        return STATUS_NOT_READY
    if terminal_state not in TERMINAL_SUCCESS:
        return STATUS_UNKNOWN
    if observation.get("is_current") is not True:
        return STATUS_NOT_READY
    return STATUS_READY if _dataset_id(observation) else STATUS_UNKNOWN


def artifact_status(observation: dict[str, Any]) -> str:
    """Evaluate explicit mapped artifact counts."""
    explicit_state = _explicit_status(observation, ("artifact_status", "status", "mapped_artifacts"))
    if explicit_state == STATUS_FAILED:
        return STATUS_FAILED
    mapped_count = _metric(observation, ("mapped_artifact_count", "artifact_count"))
    if mapped_count is None:
        return explicit_state
    return STATUS_READY if mapped_count > 0 else STATUS_NOT_READY


def unified_status(observation: dict[str, Any]) -> tuple[str, dict[str, int | float | None]]:
    """Require observed source, phone, and geographic evidence metrics."""
    metric_by_name = {
        "source_evidence_count": _metric(observation, ("source_evidence_count", "source_count")),
        "phone_evidence_count": _metric(observation, ("phone_evidence_count", "phone_count")),
        "geo_evidence_count": _metric(observation, ("geo_evidence_count", "geocode_evidence_count", "geo_count")),
    }
    explicit_state = _explicit_status(observation, ("unified_status", "status"))
    if explicit_state == STATUS_FAILED:
        return STATUS_FAILED, metric_by_name
    if any(metric is None for metric in metric_by_name.values()):
        return STATUS_UNKNOWN, metric_by_name
    return (STATUS_READY if all(metric > 0 for metric in metric_by_name.values()) else STATUS_NOT_READY), metric_by_name


def api_status(observation: dict[str, Any]) -> str:
    """Require explicit NPI profile and source-evidence API observations."""
    profile_state = _explicit_status(
        observation,
        ("profile_status", "profile_api_status", "profile_observed", "npi_profile_observed"),
    )
    evidence_state = _explicit_status(
        observation,
        ("evidence_status", "evidence_api_status", "evidence_observed", "npi_evidence_observed"),
    )
    state_list = (profile_state, evidence_state)
    if STATUS_FAILED in state_list:
        return STATUS_FAILED
    if STATUS_UNKNOWN in state_list:
        return STATUS_UNKNOWN
    return STATUS_READY if state_list == (STATUS_READY, STATUS_READY) else STATUS_NOT_READY


def latency_status(observation: dict[str, Any], *, latency_slo_ms: float) -> tuple[str, dict[str, int | float | None]]:
    """Require explicit search and detail latency measurements within the SLO."""
    latency_by_name = {
        "search_latency_ms": _metric(observation, ("search_latency_ms",)),
        "detail_latency_ms": _metric(observation, ("detail_latency_ms",)),
    }
    endpoint_state_list = (
        _explicit_status(observation, ("search_status",)),
        _explicit_status(observation, ("detail_status",)),
    )
    if STATUS_FAILED in endpoint_state_list or _explicit_status(observation, ("status",)) == STATUS_FAILED:
        return STATUS_FAILED, latency_by_name
    if any(latency is None for latency in latency_by_name.values()):
        return STATUS_UNKNOWN, latency_by_name
    return (
        STATUS_READY if all(latency <= latency_slo_ms for latency in latency_by_name.values()) else STATUS_NOT_READY,
        latency_by_name,
    )


def _source_status(status_by_check: dict[str, str]) -> str:
    for status in (STATUS_FAILED, STATUS_UNKNOWN, STATUS_NOT_READY):
        if status in status_by_check.values():
            return status
    return STATUS_READY


def build_report(
    *,
    maintained_sources: list[dict[str, str]],
    acquisition_by_entry: dict[str, dict[str, Any]],
    artifacts_by_entry: dict[str, dict[str, Any]],
    unified_by_entry: dict[str, dict[str, Any]],
    api_by_entry: dict[str, dict[str, Any]],
    latency_by_entry: dict[str, dict[str, Any]],
    latency_slo_ms: float,
) -> dict[str, Any]:
    """Build a deterministic maintained-source readiness report from observations."""
    validate_dataset_alignment(
        acquisition_by_entry,
        {
            "artifacts": artifacts_by_entry,
            "unified": unified_by_entry,
            "api": api_by_entry,
            "latency": latency_by_entry,
        },
    )
    row_list: list[dict[str, Any]] = []
    for maintained_source in maintained_sources:
        entry_id = maintained_source["entry_id"]
        unified_check, unified_metrics = unified_status(unified_by_entry[entry_id])
        latency_check, latency_metrics = latency_status(latency_by_entry[entry_id], latency_slo_ms=latency_slo_ms)
        status_by_check = {
            "current_terminal_acquisition": acquisition_status(acquisition_by_entry[entry_id]),
            "mapped_artifacts": artifact_status(artifacts_by_entry[entry_id]),
            "unified_source_evidence": unified_check,
            "npi_profile_evidence_api": api_status(api_by_entry[entry_id]),
            "search_detail_latency": latency_check,
        }
        row_list.append(
            {
                "entry_id": entry_id,
                "source_id": maintained_source["source_id"],
                "dataset_id": _dataset_id(acquisition_by_entry[entry_id]),
                "status": _source_status(status_by_check),
                "statuses": status_by_check,
                "unified_metrics": unified_metrics,
                "latency_metrics": latency_metrics,
            }
        )
    status_count_by_name = {
        status: sum(source_report["status"] == status for source_report in row_list)
        for status in STATUS_VALUES
    }
    return {
        "schema_version": 1,
        "maintained_source_count": len(row_list),
        "latency_slo_ms": latency_slo_ms,
        "ready": status_count_by_name[STATUS_READY] == len(row_list),
        "summary": {"source_status_counts": status_count_by_name},
        "sources": row_list,
    }


def format_table(report: dict[str, Any]) -> str:
    """Render one stable, human-readable row for each maintained source."""
    header_list = ["source", "acquisition", "artifacts", "unified", "npi api", "latency"]
    display_rows = [
        [
            row["entry_id"],
            row["statuses"]["current_terminal_acquisition"],
            row["statuses"]["mapped_artifacts"],
            row["statuses"]["unified_source_evidence"],
            row["statuses"]["npi_profile_evidence_api"],
            row["statuses"]["search_detail_latency"],
        ]
        for row in report["sources"]
    ]
    width_list = [max(len(header), *(len(row[index]) for row in display_rows)) for index, header in enumerate(header_list)]
    format_row = lambda row: " | ".join(value.ljust(width_list[index]) for index, value in enumerate(row))
    divider = "-+-".join("-" * width for width in width_list)
    return "\n".join([format_row(header_list), divider, *(format_row(row) for row in display_rows)])


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse explicit observation files and output controls."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sources", type=Path, default=DEFAULT_SOURCES_PATH)
    parser.add_argument("--acquisition-observations", type=Path, required=True)
    parser.add_argument("--artifact-observations", type=Path, required=True)
    parser.add_argument("--unified-observations", type=Path, required=True)
    parser.add_argument("--api-observations", type=Path, required=True)
    parser.add_argument("--latency-observations", type=Path, required=True)
    parser.add_argument("--latency-slo-ms", type=float, default=40.0)
    parser.add_argument("--format", choices=("table", "json", "both"), default="table")
    parser.add_argument("--strict", action="store_true", help="fail unless all five checks are ready for all sources")
    args = parser.parse_args(argv)
    if args.latency_slo_ms < 0:
        parser.error("--latency-slo-ms must not be negative")
    return args


def run_gate(argv: list[str] | None = None) -> int:
    """Run the maintained-source gate, returning strict readiness when requested."""
    args = parse_args(argv)
    try:
        maintained_sources = load_maintained_sources(args.sources)
        acquisition_by_entry = load_source_observations(args.acquisition_observations, label="acquisition", maintained_sources=maintained_sources)
        artifacts_by_entry = load_source_observations(args.artifact_observations, label="artifacts", maintained_sources=maintained_sources)
        unified_by_entry = load_source_observations(args.unified_observations, label="unified", maintained_sources=maintained_sources)
        api_by_entry = load_source_observations(args.api_observations, label="api", maintained_sources=maintained_sources)
        latency_by_entry = load_source_observations(args.latency_observations, label="latency", maintained_sources=maintained_sources)
        report = build_report(
            maintained_sources=maintained_sources,
            acquisition_by_entry=acquisition_by_entry,
            artifacts_by_entry=artifacts_by_entry,
            unified_by_entry=unified_by_entry,
            api_by_entry=api_by_entry,
            latency_by_entry=latency_by_entry,
            latency_slo_ms=args.latency_slo_ms,
        )
    except ObservationValidationError as error:
        if args.format == "json":
            print(json.dumps({"error": str(error)}, sort_keys=True))
        else:
            print(f"observation error: {error}", file=sys.stderr)
        return 2
    if args.format in {"json", "both"}:
        print(json.dumps(report, indent=2, sort_keys=True))
    if args.format in {"table", "both"}:
        print(format_table(report))
    return 1 if args.strict and not report["ready"] else 0


if __name__ == "__main__":
    raise SystemExit(run_gate())
