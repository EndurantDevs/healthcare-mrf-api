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
    BASELINE_NAME,
    CoverageForecastError,
    _baseline,
    report_provenance_name,
    resolve_forecast_base,
    verify_report_artifact,
    write_report_provenance,
    write_shard_provenance,
)
from coverage_forecast_combine import combine_python_coverage
from coverage_forecast_reporting import build_forecast_diagnostics
from coverage_ratchet import _compare_baselines, _load_baseline
from coverage_reports import CoverageRatchetError, _collect_report


def _write_json(path: Path, document: dict[str, Any]) -> None:
    """Write one deterministic forecast artifact."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _base_baseline(root: Path, base_sha: str, output_path: Path) -> dict[str, Any]:
    """Load the ratchet reference only from the exact requested target base."""

    try:
        completed = subprocess.run(
            ["git", "show", f"{base_sha}:{BASELINE_NAME}"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise CoverageForecastError("target base has no readable coverage baseline") from exc
    output_path.write_text(completed.stdout, encoding="utf-8")
    return _load_baseline(output_path)


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


def _run_ratchet(
    root: Path,
    candidate_path: Path,
    reference_path: Path,
    base_sha: str,
    report_name: str,
) -> subprocess.CompletedProcess[str]:
    """Call the production ratchet once using only staged temporary copies."""

    return subprocess.run(
        [
            sys.executable,
            "scripts/coverage_ratchet.py",
            "--baseline",
            str(candidate_path),
            "--reference-baseline",
            str(reference_path),
            "--changed-since",
            base_sha,
            "--report",
            report_name,
        ],
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

    report = next(iter(document["reports"].values()))
    print(
        "coverage forecast: "
        f"base={document['base_sha']} head={document['head_sha']} "
        f"changed_source_lines={report['changed_source_lines']}"
    )
    for name, metric in report["metrics"].items():
        print(
            f"coverage forecast {name}: missing={metric['current_missing']} "
            f"cap={metric['effective_missing_cap']} margin={metric['margin']}"
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
        return forecast_rust(root, arguments.base, arguments.artifacts, arguments.output)
    except (CoverageForecastError, CoverageRatchetError) as error:
        _write_error(getattr(arguments, "output", None), error)
        print(f"ERROR: {error}")
        return 2


def main(raw_arguments: Sequence[str] | None = None) -> int:
    """Provide the conventional small executable entry point."""

    return run_coverage_forecast_cli(raw_arguments)


if __name__ == "__main__":
    raise SystemExit(main())
