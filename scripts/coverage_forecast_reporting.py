"""Produce compact diagnostics from CI-equivalent coverage inputs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from coverage_forecast_artifacts import CoverageForecastError
from coverage_growth import collect_diff_coverage
from coverage_reports import CoverageRatchetError, _collect_report, _metric


def _validated_metric(metric_value: Any, label: str) -> dict[str, int]:
    if not isinstance(metric_value, dict):
        raise CoverageForecastError(f"{label}: metric input is malformed")
    try:
        return _metric(metric_value.get("covered"), metric_value.get("total"), label)
    except CoverageRatchetError as exc:
        raise CoverageForecastError(str(exc)) from exc


def _metric_details(
    report_name: str,
    candidate_config: dict[str, Any],
    reference_config: dict[str, Any],
) -> dict[str, dict[str, int | float]]:
    candidate_metrics = candidate_config.get("metrics")
    reference_metrics = reference_config.get("metrics")
    if not isinstance(candidate_metrics, dict) or not isinstance(reference_metrics, dict):
        raise CoverageForecastError(f"{report_name}: baseline metrics are malformed")
    details_by_name: dict[str, dict[str, int | float]] = {}
    for metric_name, candidate_value in candidate_metrics.items():
        current = _validated_metric(candidate_value, f"{report_name}.{metric_name}")
        reference = _validated_metric(
            reference_metrics.get(metric_name),
            f"{report_name}.{metric_name} reference baseline",
        )
        details_by_name[metric_name] = {
            "current_covered": current["covered"],
            "current_total": current["total"],
            "current_percent": 100.0 * current["covered"] / current["total"],
            "reference_covered": reference["covered"],
            "reference_total": reference["total"],
            "reference_percent": 100.0
            * reference["covered"]
            / reference["total"],
        }
    return details_by_name


def _build_diagnostics(
    root: Path,
    base_sha: str,
    head_sha: str,
    candidate_baseline: dict[str, Any],
    reference_baseline: dict[str, Any],
    report_paths: dict[str, Path],
) -> dict[str, Any]:
    candidate_reports = candidate_baseline.get("reports")
    reference_reports = reference_baseline.get("reports")
    if not isinstance(candidate_reports, dict) or not isinstance(reference_reports, dict):
        raise CoverageForecastError("coverage baseline reports are malformed")
    report_names = list(report_paths)
    try:
        diff_by_report, diff_errors = collect_diff_coverage(
            root,
            base_sha,
            candidate_baseline,
            report_names,
        )
    except CoverageRatchetError as exc:
        raise CoverageForecastError(str(exc)) from exc
    reports_by_name: dict[str, Any] = {}
    for report_name, report_path in report_paths.items():
        candidate_config = candidate_reports.get(report_name)
        reference_config = reference_reports.get(report_name)
        if not isinstance(candidate_config, dict) or not isinstance(
            reference_config, dict
        ):
            raise CoverageForecastError(f"{report_name}: baseline report mismatch")
        if Path(str(candidate_config.get("path"))).resolve() != report_path.resolve():
            raise CoverageRatchetError(
                "forecast diagnostics report path differs from the staged ratchet input"
            )
        snapshot = _collect_report(root, report_name, candidate_config)
        if snapshot.metric_by_name != candidate_config.get("metrics"):
            raise CoverageRatchetError(
                "staged coverage metrics differ from the measured report"
            )
        reports_by_name[report_name] = {
            "diff_coverage": diff_by_report[report_name],
            "metrics": _metric_details(
                report_name,
                candidate_config,
                reference_config,
            ),
            "source_files": sorted(snapshot.files),
        }
    return {
        "schema_version": 1,
        "base_sha": base_sha,
        "head_sha": head_sha,
        "reports": reports_by_name,
        "policy_errors": diff_errors,
    }


def build_forecast_diagnostics(
    root: Path,
    base_sha: str,
    head_sha: str,
    candidate_baseline: dict[str, Any],
    reference_baseline: dict[str, Any],
    report_name: str,
    report_path: Path,
) -> dict[str, Any]:
    """Build diagnostics for the legacy one-report forecast commands."""

    return _build_diagnostics(
        root,
        base_sha,
        head_sha,
        candidate_baseline,
        reference_baseline,
        {report_name: report_path},
    )


def build_combined_forecast_diagnostics(
    root: Path,
    base_sha: str,
    head_sha: str,
    candidate_baseline: dict[str, Any],
    reference_baseline: dict[str, Any],
    report_paths: dict[str, Path],
) -> dict[str, Any]:
    """Build one diagnostic covering both canonical language reports."""

    return _build_diagnostics(
        root,
        base_sha,
        head_sha,
        candidate_baseline,
        reference_baseline,
        report_paths,
    )
