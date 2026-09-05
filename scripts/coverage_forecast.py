#!/usr/bin/env python3
"""Forecast the healthcare CI coverage ratchets from verified artifacts."""

from __future__ import annotations

import argparse
from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Any, Sequence

from coverage_forecast_artifacts import (
    CoverageForecastError,
    _baseline,
    base_baseline,
    reference_baseline as load_reference_baseline,
    report_provenance_name,
    resolve_forecast_base,
    verify_report_artifact,
    write_report_provenance,
    write_shard_provenance,
)
from coverage_forecast_combine import combine_python_coverage
from coverage_forecast_reporting import (
    build_combined_forecast_diagnostics,
    build_forecast_diagnostics,
)
from coverage_ratchet import _compare_baselines
from coverage_reports import CoverageRatchetError, _collect_report


def _write_json(path: Path, document: dict[str, Any]) -> None:
    """Write one deterministic forecast artifact."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _base_baseline(root: Path, base_sha: str, output_path: Path) -> dict[str, Any]:
    """Load the ratchet reference only from the exact requested target base."""

    baseline = base_baseline(root, base_sha)
    _write_json(output_path, baseline)
    return baseline


def _selected_baseline(baseline: dict[str, Any], report_name: str) -> dict[str, Any]:
    """Keep ratchet comparisons scoped to the report being forecast."""

    selected = deepcopy(baseline)
    reports = selected.get("reports")
    if not isinstance(reports, dict) or report_name not in reports:
        raise CoverageForecastError(f"coverage baseline is missing {report_name}")
    selected["reports"] = {report_name: reports[report_name]}
    return selected


def _with_report_path(
    baseline: dict[str, Any], report_name: str, report_path: Path
) -> dict[str, Any]:
    """Point a temporary ratchet copy at its exact generated report."""

    staged = _selected_baseline(baseline, report_name)
    staged["reports"][report_name]["path"] = str(report_path.resolve())
    return staged


def _with_report_snapshot(
    root: Path,
    baseline: dict[str, Any],
    report_name: str,
    report_path: Path,
) -> dict[str, Any]:
    """Stage current metrics and files from the exact CI report for the gate."""

    staged = _with_report_path(baseline, report_name, report_path)
    config = staged["reports"][report_name]
    snapshot = _collect_report(root, report_name, config, enforce_baseline_files=False)
    config["metrics"] = deepcopy(snapshot.metric_by_name)
    config["files"] = sorted(snapshot.files)
    return staged


def _with_report_snapshots(
    root: Path,
    baseline: dict[str, Any],
    report_paths: dict[str, Path],
) -> dict[str, Any]:
    """Stage all measured report paths, metrics, and source-file sets."""

    staged = deepcopy(baseline)
    reports = staged.get("reports")
    if not isinstance(reports, dict) or set(reports) != set(report_paths):
        raise CoverageForecastError("coverage reports differ from the measured report set")
    for report_name, report_path in report_paths.items():
        config = reports[report_name]
        config["path"] = str(report_path.resolve())
        snapshot = _collect_report(
            root,
            report_name,
            config,
            enforce_baseline_files=False,
        )
        config["metrics"] = deepcopy(snapshot.metric_by_name)
        config["files"] = sorted(snapshot.files)
    return staged


def _with_report_paths(
    baseline: dict[str, Any],
    report_paths: dict[str, Path],
) -> dict[str, Any]:
    """Point all reference configs at the exact reports used by the gate."""

    staged = deepcopy(baseline)
    reports = staged.get("reports")
    if not isinstance(reports, dict) or set(reports) != set(report_paths):
        raise CoverageForecastError("coverage reports differ from the reference report set")
    for report_name, report_path in report_paths.items():
        reports[report_name]["path"] = str(report_path.resolve())
    return staged


def _policy_projection(
    candidate: dict[str, Any], reference: dict[str, Any], report_name: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Compare policy/scope without mistaking stale committed metrics for CI data."""

    projected_candidate = _selected_baseline(candidate, report_name)
    projected_reference = _selected_baseline(reference, report_name)
    candidate_config = projected_candidate["reports"][report_name]
    reference_config = projected_reference["reports"][report_name]
    candidate_config["metrics"] = deepcopy(reference_config["metrics"])
    candidate_config["files"] = deepcopy(reference_config["files"])
    return projected_candidate, projected_reference


def _write_staged_baselines(
    root: Path,
    temporary_directory: Path,
    candidate: dict[str, Any],
    reference: dict[str, Any],
    report_name: str,
    report_path: Path,
) -> tuple[dict[str, Any], dict[str, Any], Path, Path]:
    """Persist report-derived candidate and exact-base reference copies."""

    candidate_staged = _with_report_snapshot(root, candidate, report_name, report_path)
    reference_staged = _with_report_path(reference, report_name, report_path)
    candidate_path = temporary_directory / "candidate-baseline.json"
    reference_path = temporary_directory / "reference-baseline.json"
    _write_json(candidate_path, candidate_staged)
    _write_json(reference_path, reference_staged)
    return candidate_staged, reference_staged, candidate_path, reference_path


def _write_all_staged_baselines(
    root: Path,
    temporary_directory: Path,
    candidate: dict[str, Any],
    reference: dict[str, Any],
    report_paths: dict[str, Path],
) -> tuple[dict[str, Any], dict[str, Any], Path, Path]:
    """Persist the two-report measured candidate and exact-base reference."""

    candidate_staged = _with_report_snapshots(root, candidate, report_paths)
    reference_staged = _with_report_paths(reference, report_paths)
    candidate_path = temporary_directory / "candidate-baseline.json"
    reference_path = temporary_directory / "reference-baseline.json"
    _write_json(candidate_path, candidate_staged)
    _write_json(reference_path, reference_staged)
    return candidate_staged, reference_staged, candidate_path, reference_path


def _run_ratchet(
    root: Path,
    candidate_path: Path,
    reference_path: Path,
    base_sha: str,
    report_name: str | None,
) -> subprocess.CompletedProcess[str]:
    """Call the production ratchet once using only staged temporary copies."""

    command = [
        sys.executable,
        "scripts/coverage_ratchet.py",
        "--baseline",
        str(candidate_path),
        "--reference-baseline",
        str(reference_path),
        "--changed-since",
        base_sha,
    ]
    if report_name is not None:
        command.extend(("--report", report_name))
    return subprocess.run(
        command,
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )


def _ratchet_errors(result: subprocess.CompletedProcess[str]) -> list[str]:
    """Extract individual stable production-gate errors for the forecast artifact."""

    return [
        line.removeprefix("ERROR: ")
        for line in result.stdout.splitlines()
        if line.startswith("ERROR: ")
    ]


def _print_summary(document: dict[str, Any], output_path: Path | None) -> None:
    """Print the small actionable portion while retaining detailed diagnostics."""

    print(
        "coverage forecast: "
        f"base={document['base_sha']} head={document['head_sha']}"
    )
    for report_name, report in document["reports"].items():
        diff_coverage = report["diff_coverage"]
        print(
            f"coverage forecast {report_name} diff: "
            f"{diff_coverage['percent']:.2f}% "
            f"({diff_coverage['covered']}/{diff_coverage['total']}; "
            f"threshold {diff_coverage['threshold']}%)"
        )
        for metric_name, metric in report["metrics"].items():
            print(
                f"coverage forecast {report_name}.{metric_name}: "
                f"current={metric['current_percent']:.4f}% "
                f"base={metric['reference_percent']:.4f}%"
            )
    if output_path is not None:
        print(f"coverage forecast diagnostics: {output_path}")


def _forecast_one_report(
    root: Path,
    base_revision: str,
    report_name: str,
    report_path: Path,
    output_path: Path | None,
    inputs: dict[str, Any],
) -> int:
    """Run one report's policy preflight and real ratchet against report data."""

    base_sha, head_sha = resolve_forecast_base(root, base_revision)
    candidate_baseline = _baseline(root)
    with tempfile.TemporaryDirectory(prefix="healthcare-coverage-forecast-") as raw_temp:
        temporary_directory = Path(raw_temp)
        reference_baseline = _base_baseline(
            root, base_sha, temporary_directory / "base.json"
        )
        policy_candidate, policy_reference = _policy_projection(
            candidate_baseline, reference_baseline, report_name
        )
        policy_errors = _compare_baselines(policy_candidate, policy_reference)
        if policy_errors:
            raise CoverageForecastError("; ".join(policy_errors))
        staged_candidate, staged_reference, candidate_path, reference_path = (
            _write_staged_baselines(
                root,
                temporary_directory,
                candidate_baseline,
                reference_baseline,
                report_name,
                report_path,
            )
        )
        ratchet_result = _run_ratchet(
            root, candidate_path, reference_path, base_sha, report_name
        )
        document = build_forecast_diagnostics(
            root,
            base_sha,
            head_sha,
            staged_candidate,
            staged_reference,
            report_name,
            report_path,
        )
    document["inputs"] = inputs
    document["ratchet_errors"] = _ratchet_errors(ratchet_result)
    document["ratchet_exit_code"] = ratchet_result.returncode
    if output_path is not None:
        _write_json(output_path, document)
    if ratchet_result.stdout:
        print(ratchet_result.stdout, end="")
    if ratchet_result.stderr:
        print(ratchet_result.stderr, file=sys.stderr, end="")
    _print_summary(document, output_path)
    return ratchet_result.returncode


def forecast_python(
    root: Path,
    base_revision: str,
    main_artifacts: Path,
    capacity_artifacts: Path,
    postgres_artifacts: Path,
    output_path: Path | None,
) -> int:
    """Verify all Python producers then forecast the combined CI report."""

    base_sha, head_sha = resolve_forecast_base(root, base_revision)
    with tempfile.TemporaryDirectory(prefix="healthcare-coverage-combine-") as raw_temp:
        temporary_directory = Path(raw_temp)
        report_path, input_names = combine_python_coverage(
            root,
            temporary_directory,
            main_artifacts,
            capacity_artifacts,
            postgres_artifacts,
            base_sha,
            head_sha,
        )
        return _forecast_one_report(
            root,
            base_sha,
            "python",
            report_path,
            output_path,
            {"producer_files": input_names},
        )


def forecast_rust(
    root: Path,
    base_revision: str,
    artifacts: Path,
    output_path: Path | None,
) -> int:
    """Verify the Rust report artifact before forecasting its separate ratchet."""

    base_sha, head_sha = resolve_forecast_base(root, base_revision)
    report_path = verify_report_artifact(root, artifacts, "rust", base_sha, head_sha)
    return _forecast_one_report(
        root,
        base_sha,
        "rust",
        report_path,
        output_path,
        {"provenance": report_provenance_name("rust")},
    )


def _write_measured_baseline(
    output_path: Path,
    measured_candidate: dict[str, Any],
    configured_candidate: dict[str, Any],
    head_sha: str,
) -> None:
    """Write one source-bound baseline from both measured reports."""

    artifact_baseline = deepcopy(measured_candidate)
    artifact_baseline["source_sha"] = head_sha
    artifact_baseline["machine_artifact_required"] = True
    measured_reports = artifact_baseline.get("reports")
    configured_reports = configured_candidate.get("reports")
    if not isinstance(measured_reports, dict) or not isinstance(
        configured_reports, dict
    ):
        raise CoverageForecastError("coverage baseline reports are malformed")
    for report_name, report_config in measured_reports.items():
        configured = configured_reports.get(report_name)
        if not isinstance(report_config, dict) or not isinstance(configured, dict):
            raise CoverageForecastError(f"{report_name}: baseline report mismatch")
        report_config["path"] = configured["path"]
    _write_json(output_path, artifact_baseline)


def forecast_coverage(
    root: Path,
    base_revision: str,
    main_artifacts: Path,
    capacity_artifacts: Path,
    postgres_artifacts: Path,
    rust_artifacts: Path,
    output_path: Path | None,
    reference_artifact_path: Path | None,
    baseline_output_path: Path | None,
) -> int:
    """Validate both language reports and run one canonical policy gate."""

    base_sha, head_sha = resolve_forecast_base(root, base_revision)
    configured_candidate = _baseline(root)
    reference = load_reference_baseline(root, base_sha, reference_artifact_path)
    with tempfile.TemporaryDirectory(prefix="healthcare-coverage-combine-") as raw_temp:
        temporary_directory = Path(raw_temp)
        python_report, input_names = combine_python_coverage(
            root,
            temporary_directory / "python",
            main_artifacts,
            capacity_artifacts,
            postgres_artifacts,
            base_sha,
            head_sha,
        )
        rust_report = verify_report_artifact(
            root,
            rust_artifacts,
            "rust",
            base_sha,
            head_sha,
        )
        report_paths = {"python": python_report, "rust": rust_report}
        measured_candidate, staged_reference, candidate_path, reference_path = (
            _write_all_staged_baselines(
                root,
                temporary_directory,
                configured_candidate,
                reference,
                report_paths,
            )
        )
        ratchet_result = _run_ratchet(
            root,
            candidate_path,
            reference_path,
            base_sha,
            None,
        )
        document = build_combined_forecast_diagnostics(
            root,
            base_sha,
            head_sha,
            measured_candidate,
            staged_reference,
            report_paths,
        )
        if baseline_output_path is not None:
            _write_measured_baseline(
                baseline_output_path,
                measured_candidate,
                configured_candidate,
                head_sha,
            )
    document["inputs"] = {
        "python_producer_files": input_names,
        "rust_provenance": report_provenance_name("rust"),
    }
    document["ratchet_errors"] = _ratchet_errors(ratchet_result)
    document["ratchet_exit_code"] = ratchet_result.returncode
    if output_path is not None:
        _write_json(output_path, document)
    if ratchet_result.stdout:
        print(ratchet_result.stdout, end="")
    if ratchet_result.stderr:
        print(ratchet_result.stderr, file=sys.stderr, end="")
    _print_summary(document, output_path)
    return ratchet_result.returncode


def _parse_arguments(raw_arguments: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the narrow CI producer and forecast commands."""

    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    shard = commands.add_parser("write-shard-provenance")
    shard.add_argument("--base", required=True)
    shard.add_argument("--kind", required=True)
    shard.add_argument("--shard", required=True)
    shard.add_argument("--coverage", type=Path, required=True)
    shard.add_argument("--output", type=Path, required=True)
    report = commands.add_parser("write-report-provenance")
    report.add_argument("--base", required=True)
    report.add_argument("--report-name", required=True)
    report.add_argument("--report", type=Path, required=True)
    report.add_argument("--output", type=Path, required=True)
    report.add_argument("--cargo-llvm-cov-version")
    report.add_argument("--rust-version")
    python = commands.add_parser("forecast-python")
    python.add_argument("--base", required=True)
    python.add_argument("--main-artifacts", type=Path, required=True)
    python.add_argument("--capacity-artifacts", type=Path, required=True)
    python.add_argument("--postgres-artifacts", type=Path, required=True)
    python.add_argument("--output", type=Path)
    rust = commands.add_parser("forecast-rust")
    rust.add_argument("--base", required=True)
    rust.add_argument("--artifacts", type=Path, required=True)
    rust.add_argument("--output", type=Path)
    combined = commands.add_parser("forecast")
    combined.add_argument("--base", required=True)
    combined.add_argument("--main-artifacts", type=Path, required=True)
    combined.add_argument("--capacity-artifacts", type=Path, required=True)
    combined.add_argument("--postgres-artifacts", type=Path, required=True)
    combined.add_argument("--rust-artifacts", type=Path, required=True)
    combined.add_argument("--reference-baseline", type=Path)
    combined.add_argument("--baseline-output", type=Path)
    combined.add_argument("--output", type=Path)
    return parser.parse_args(raw_arguments)


def _write_error(output_path: Path | None, error: Exception) -> None:
    """Persist one fail-closed diagnostic when a forecast cannot be reproduced."""

    if output_path is not None:
        _write_json(output_path, {"schema_version": 1, "error": str(error)})


def run_coverage_forecast_cli(raw_arguments: Sequence[str] | None = None) -> int:
    """Run one narrowly specified CI coverage forecast operation."""

    arguments = _parse_arguments(raw_arguments)
    root = Path.cwd()
    try:
        if arguments.command == "write-shard-provenance":
            write_shard_provenance(
                root,
                arguments.kind,
                arguments.shard,
                arguments.coverage,
                arguments.base,
                arguments.output,
            )
            return 0
        if arguments.command == "write-report-provenance":
            write_report_provenance(
                root,
                arguments.report_name,
                arguments.report,
                arguments.base,
                arguments.output,
                arguments.cargo_llvm_cov_version,
                arguments.rust_version,
            )
            return 0
        if arguments.command == "forecast-python":
            return forecast_python(
                root,
                arguments.base,
                arguments.main_artifacts,
                arguments.capacity_artifacts,
                arguments.postgres_artifacts,
                arguments.output,
            )
        if arguments.command == "forecast-rust":
            return forecast_rust(
                root,
                arguments.base,
                arguments.artifacts,
                arguments.output,
            )
        return forecast_coverage(
            root,
            arguments.base,
            arguments.main_artifacts,
            arguments.capacity_artifacts,
            arguments.postgres_artifacts,
            arguments.rust_artifacts,
            arguments.output,
            arguments.reference_baseline,
            arguments.baseline_output,
        )
    except (CoverageForecastError, CoverageRatchetError) as error:
        _write_error(getattr(arguments, "output", None), error)
        print(f"ERROR: {error}")
        return 2


def main(raw_arguments: Sequence[str] | None = None) -> int:
    """Provide the conventional small executable entry point."""

    return run_coverage_forecast_cli(raw_arguments)


if __name__ == "__main__":
    raise SystemExit(main())
