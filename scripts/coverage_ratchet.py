#!/usr/bin/env python3
"""Enforce versioned coverage ratios and changed-line coverage."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from coverage_growth import collect_diff_coverage, compare_diff_policy, load_diff_threshold
from coverage_ratchet_self_test import run_self_test
from coverage_reports import CoverageRatchetError, Metric, _collect_report, _metric, _read_json

SCHEMA_VERSION = 1


def _load_baseline(path: Path) -> dict[str, Any]:
    baseline = _read_json(path)
    if baseline.get("schema_version") != SCHEMA_VERSION:
        raise CoverageRatchetError(f"{path} must use schema_version {SCHEMA_VERSION}")
    reports = baseline.get("reports")
    if not isinstance(reports, dict) or not reports:
        raise CoverageRatchetError(f"{path} must define at least one report")
    return baseline


def _percent(metric: Metric) -> float:
    return 100.0 * metric["covered"] / metric["total"]


def _compare_metric(label: str, current: Metric, minimum: Metric) -> list[str]:
    current = _metric(current.get("covered"), current.get("total"), label)
    minimum = _metric(minimum.get("covered"), minimum.get("total"), label)
    if current["covered"] * minimum["total"] < minimum["covered"] * current["total"]:
        return [
            f"{label}: coverage fell to {_percent(current):.4f}% "
            f"below {_percent(minimum):.4f}%"
        ]
    return []


def _compare_policy(
    report_name: str,
    candidate: Any,
    reference: Any,
    allow_tool_version_transition: bool = False,
) -> list[str]:
    if not isinstance(candidate, dict) or not isinstance(reference, dict):
        return [f"{report_name}: baseline measurement policy is malformed"]
    errors: list[str] = []
    for field in (
        "all",
        "all_targets",
        "branch",
        "include_namespace_packages",
        "workspace",
    ):
        if reference.get(field) is True and candidate.get(field) is not True:
            errors.append(f"{report_name}: measurement policy disabled {field}")
    for field in ("features", "source_dirs", "source_pkgs", "tests"):
        if not set(reference.get(field, [])).issubset(set(candidate.get(field, []))):
            errors.append(f"{report_name}: measurement policy narrowed {field}")
    if not set(candidate.get("test_deselections", [])).issubset(
        set(reference.get("test_deselections", []))
    ):
        errors.append(f"{report_name}: measurement policy added test deselections")
    for field in ("manifest", "source"):
        if reference.get(field) != candidate.get(field):
            errors.append(f"{report_name}: measurement policy changed {field}")
    for field in ("c8", "cargo_llvm_cov", "coverage", "pytest", "rust"):
        is_transition_upgrade = (
            allow_tool_version_transition
            and field == "coverage"
            and reference.get(field) == "7.15.2"
            and candidate.get(field) == "7.16.0"
        )
        if (
            not is_transition_upgrade
            and field in reference
            and reference.get(field) != candidate.get(field)
        ):
            errors.append(f"{report_name}: measurement policy changed {field}")
    return errors


def _compare_scope(
    report_name: str,
    candidate: dict[str, Any],
    reference: dict[str, Any],
    allow_tool_version_transition: bool = False,
) -> list[str]:
    candidate_scope = candidate.get("scope")
    reference_scope = reference.get("scope")
    if not isinstance(candidate_scope, dict) or not isinstance(reference_scope, dict):
        return [f"{report_name}: baseline scope is malformed"]
    errors: list[str] = []
    if not set(reference_scope.get("include", [])).issubset(
        set(candidate_scope.get("include", []))
    ):
        errors.append(f"{report_name}: baseline source scope was narrowed")
    if not set(candidate_scope.get("exclude", [])).issubset(
        set(reference_scope.get("exclude", []))
    ):
        errors.append(f"{report_name}: baseline exclusions were expanded")
    errors.extend(
        _compare_policy(
            report_name,
            candidate_scope.get("policy", {}),
            reference_scope.get("policy", {}),
            allow_tool_version_transition,
        )
    )
    return errors


def _compare_baselines(
    candidate: dict[str, Any],
    reference: dict[str, Any],
    _changed_line_by_report: dict[str, int] | None = None,
) -> list[str]:
    """Compare policy and ratio floors without imposing an uncovered-count cap."""

    errors: list[str] = []
    if reference.get("machine_artifact_required") is True and candidate.get(
        "machine_artifact_required"
    ) is not True:
        errors.append("coverage baseline machine-artifact requirement was removed")
    candidate_reports = candidate["reports"]
    allow_tool_version_transition = (
        reference.get("machine_artifact_required") is not True
        and candidate.get("machine_artifact_required") is True
    )
    for report_name, old_config in reference["reports"].items():
        new_config = candidate_reports.get(report_name)
        if not isinstance(new_config, dict) or not isinstance(old_config, dict):
            errors.append(f"{report_name}: baseline report was removed")
            continue
        for field in ("format", "path"):
            if new_config.get(field) != old_config.get(field):
                errors.append(f"{report_name}: baseline {field} changed")
        errors.extend(
            _compare_scope(
                report_name,
                new_config,
                old_config,
                allow_tool_version_transition,
            )
        )
        errors.extend(compare_diff_policy(report_name, new_config, old_config))
        old_metrics = old_config.get("metrics")
        new_metrics = new_config.get("metrics")
        if not isinstance(old_metrics, dict) or not isinstance(new_metrics, dict):
            errors.append(f"{report_name}: baseline metrics are malformed")
            continue
        for metric_name, old_metric in old_metrics.items():
            new_metric = new_metrics.get(metric_name)
            if not isinstance(new_metric, dict):
                errors.append(f"{report_name}.{metric_name}: baseline metric was removed")
                continue
            errors.extend(
                _compare_metric(
                    f"{report_name}.{metric_name} baseline",
                    new_metric,
                    old_metric,
                )
            )
    return errors


def _write_baseline(
    path: Path,
    baseline: dict[str, Any],
    root: Path,
    selected_names: list[str],
) -> None:
    for report_name in selected_names:
        config = baseline["reports"][report_name]
        snapshot = _collect_report(root, report_name, config, enforce_baseline_files=False)
        config["metrics"] = snapshot.metric_by_name
        config["files"] = sorted(snapshot.files)
    path.write_text(json.dumps(baseline, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _run_self_test() -> None:
    run_self_test(_compare_metric, _compare_baselines, _collect_report)


def _parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", default="test-coverage-baseline.json")
    parser.add_argument("--reference-baseline")
    parser.add_argument("--changed-since")
    parser.add_argument("--report", action="append", dest="report_names")
    parser.add_argument("--write-baseline", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def _check_current_report(
    root: Path,
    report_name: str,
    config: dict[str, Any],
) -> list[str]:
    snapshot = _collect_report(root, report_name, config)
    minimum_metrics = config.get("metrics")
    if not isinstance(minimum_metrics, dict) or not minimum_metrics:
        raise CoverageRatchetError(
            f"{report_name}: baseline metrics are missing; use --write-baseline"
        )
    errors: list[str] = []
    for metric_name, minimum in minimum_metrics.items():
        current = snapshot.metric_by_name.get(metric_name)
        if current is None:
            errors.append(f"{report_name}.{metric_name}: current metric is missing")
            continue
        print(
            f"{report_name}.{metric_name}: {_percent(current):.4f}% "
            f"({current['covered']}/{current['total']})"
        )
        errors.extend(_compare_metric(f"{report_name}.{metric_name}", current, minimum))
    return errors


def _execute_gate(args: argparse.Namespace) -> int:
    root = Path.cwd()
    baseline_path = root / args.baseline
    baseline = _load_baseline(baseline_path)
    selected_names = args.report_names or list(baseline["reports"])
    unknown_names = sorted(set(selected_names) - set(baseline["reports"]))
    if unknown_names:
        raise CoverageRatchetError(f"unknown baseline reports: {', '.join(unknown_names)}")
    if args.write_baseline:
        _write_baseline(baseline_path, baseline, root, selected_names)
        baseline = _load_baseline(baseline_path)
    for report_name in selected_names:
        load_diff_threshold(report_name, baseline["reports"][report_name])
    errors: list[str] = []
    if args.reference_baseline:
        if not args.changed_since:
            raise CoverageRatchetError("--reference-baseline requires --changed-since")
        reference = _load_baseline(Path(args.reference_baseline))
        errors.extend(_compare_baselines(baseline, reference))
        diff_by_report, diff_errors = collect_diff_coverage(
            root,
            args.changed_since,
            baseline,
            selected_names,
        )
        errors.extend(diff_errors)
        for report_name, diff_coverage in diff_by_report.items():
            print(
                f"{report_name} diff coverage: {diff_coverage['percent']:.2f}% "
                f"({diff_coverage['covered']}/{diff_coverage['total']} executable "
                f"changed lines; threshold {diff_coverage['threshold']}%)"
            )
    elif args.changed_since:
        raise CoverageRatchetError("--changed-since requires --reference-baseline")
    for report_name in selected_names:
        errors.extend(_check_current_report(root, report_name, baseline["reports"][report_name]))
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print("Test coverage satisfies ratio and changed-line coverage policy.")
    return 0


def run_coverage_ratchet() -> int:
    """Run the self-test, baseline writer, or configured coverage gate."""

    args = _parse_arguments()
    if args.self_test:
        _run_self_test()
        return 0
    try:
        return _execute_gate(args)
    except CoverageRatchetError as exc:
        print(f"ERROR: {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(run_coverage_ratchet())
