"""Produce debt diagnostics from the exact report staged for the ratchet."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from coverage_growth import (
    calculate_required_debt_reduction,
    collect_growth_evidence,
    load_growth_policy,
)
from coverage_reports import (
    CoverageRatchetError,
    _collect_report,
    _is_path_in_scope,
    _read_json,
    _relative_report_path,
)


def _metric_diagnostic(
    report_name: str,
    metric_name: str,
    current_metric: dict[str, int],
    reference_metric: dict[str, int],
    policy: dict[str, Any],
    changed_line_count: int,
) -> dict[str, int]:
    """Return the debt cap and margin that the staged ratchet evaluates."""

    current_missing = current_metric["total"] - current_metric["covered"]
    reference_missing = reference_metric["total"] - reference_metric["covered"]
    target_percent = policy["target_percent_by_metric"][metric_name]
    is_at_target = (
        current_metric["covered"] * 100 >= target_percent * current_metric["total"]
    )
    required_reduction = 0
    if changed_line_count and not is_at_target:
        required_reduction = calculate_required_debt_reduction(
            reference_metric,
            target_percent,
            policy["debt_reduction_percent"],
            changed_line_count,
            policy["changed_line_divisor"],
        )
    cap = reference_missing - required_reduction
    return {
        "base_missing": reference_missing,
        "current_missing": current_missing,
        "effective_missing_cap": cap,
        "margin": cap - current_missing,
        "required_growth_reduction": required_reduction,
        "target_percent": target_percent,
    }


def _missing_branch_arcs(
    root: Path,
    report_path: Path,
    report_config: dict[str, Any],
) -> dict[str, list[list[int]]]:
    """Return sorted in-scope missing arcs for a coverage.py report."""

    report_document = _read_json(report_path)
    raw_files = report_document.get("files")
    if not isinstance(raw_files, dict):
        return {}
    arcs_by_path: dict[str, list[list[int]]] = {}
    for raw_path, payload in raw_files.items():
        if not isinstance(raw_path, str) or not isinstance(payload, dict):
            continue
        relative_path = _relative_report_path(root, raw_path)
        if relative_path is None or not _is_path_in_scope(relative_path, report_config):
            continue
        raw_arcs = payload.get("missing_branches")
        arcs = [arc for arc in raw_arcs if _is_branch_arc(arc)] if isinstance(raw_arcs, list) else []
        if arcs:
            arcs_by_path[relative_path] = sorted(arcs)
    return arcs_by_path


def _is_branch_arc(value: object) -> bool:
    """Accept only the two-integer arc records emitted by coverage.py."""

    return (
        isinstance(value, list)
        and len(value) == 2
        and all(isinstance(line_number, int) for line_number in value)
    )


def build_forecast_diagnostics(
    root: Path,
    base_sha: str,
    head_sha: str,
    candidate_baseline: dict[str, Any],
    reference_baseline: dict[str, Any],
    report_name: str,
    report_path: Path,
) -> dict[str, Any]:
    """Summarize the same report snapshot and base policy fed to the gate."""

    candidate_config = candidate_baseline["reports"][report_name]
    reference_config = reference_baseline["reports"][report_name]
    candidate_path = Path(candidate_config["path"]).resolve()
    if candidate_path != report_path.resolve():
        raise CoverageRatchetError(
            "forecast diagnostics report path differs from the staged ratchet input"
        )
    changed_by_report, exclusion_errors = collect_growth_evidence(
        root,
        base_sha,
        candidate_baseline,
        [report_name],
    )
    snapshot = _collect_report(root, report_name, candidate_config)
    if snapshot.metric_by_name != candidate_config["metrics"]:
        raise CoverageRatchetError(
            "staged coverage metrics differ from the measured report"
        )
    policy = load_growth_policy(report_name, candidate_config)
    metric_diagnostics_by_name: dict[str, dict[str, int]] = {}
    for metric_name, reference_metric in reference_config["metrics"].items():
        current_metric = snapshot.metric_by_name[metric_name]
        metric_diagnostics_by_name[metric_name] = _metric_diagnostic(
            report_name,
            metric_name,
            current_metric,
            reference_metric,
            policy,
            changed_by_report[report_name],
        )
    report_diagnostics_by_name: dict[str, Any] = {
        "changed_source_lines": changed_by_report[report_name],
        "exclusion_errors": exclusion_errors,
        "metrics": metric_diagnostics_by_name,
        "source_files": sorted(snapshot.files),
    }
    if candidate_config.get("format") == "coverage.py":
        report_diagnostics_by_name["missing_branch_arcs"] = _missing_branch_arcs(
            root,
            report_path,
            candidate_config,
        )
    return {
        "schema_version": 1,
        "base_sha": base_sha,
        "head_sha": head_sha,
        "reports": {report_name: report_diagnostics_by_name},
    }
