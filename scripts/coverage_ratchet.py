#!/usr/bin/env python3
"""Enforce versioned, exact test-coverage floors across coverage tools."""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path
from typing import Any

from coverage_reports import (
    CoverageRatchetError,
    Metric,
    _collect_report,
    _metric,
    _read_json,
)
from coverage_growth import (
    build_growth_policy_test_baseline,
    collect_growth_evidence,
    compare_growth_metric,
    compare_growth_policy,
    load_growth_policy,
    run_exclusion_guard_self_test,
    run_growth_helper_self_test,
)

SCHEMA_VERSION = 1


def _load_baseline(path: Path) -> dict[str, Any]:
    baseline = _read_json(path)
    if baseline.get("schema_version") != SCHEMA_VERSION:
        raise CoverageRatchetError(
            f"{path} must use schema_version {SCHEMA_VERSION}"
        )
    reports = baseline.get("reports")
    if not isinstance(reports, dict) or not reports:
        raise CoverageRatchetError(f"{path} must define at least one report")
    return baseline


def _percent(metric: Metric) -> float:
    return 100.0 * metric["covered"] / metric["total"]


def _compare_metric(label: str, current: Metric, minimum: Metric) -> list[str]:
    current = _metric(current.get("covered"), current.get("total"), label)
    minimum = _metric(minimum.get("covered"), minimum.get("total"), label)
    errors: list[str] = []
    if current["covered"] * minimum["total"] < minimum["covered"] * current["total"]:
        errors.append(
            f"{label}: coverage fell to {_percent(current):.4f}% "
            f"below {_percent(minimum):.4f}%"
        )
    current_missing = current["total"] - current["covered"]
    minimum_missing = minimum["total"] - minimum["covered"]
    if current_missing > minimum_missing:
        errors.append(
            f"{label}: uncovered debt rose to {current_missing} above {minimum_missing}"
        )
    return errors


def _compare_scope(
    report_name: str,
    candidate: dict[str, Any],
    reference: dict[str, Any],
) -> list[str]:
    errors: list[str] = []
    candidate_scope = candidate.get("scope")
    reference_scope = reference.get("scope")
    if not isinstance(candidate_scope, dict) or not isinstance(reference_scope, dict):
        return [f"{report_name}: baseline scope is malformed"]
    old_includes = set(reference_scope.get("include", []))
    new_includes = set(candidate_scope.get("include", []))
    old_excludes = set(reference_scope.get("exclude", []))
    new_excludes = set(candidate_scope.get("exclude", []))
    if not old_includes.issubset(new_includes):
        errors.append(f"{report_name}: baseline source scope was narrowed")
    if not new_excludes.issubset(old_excludes):
        errors.append(f"{report_name}: baseline exclusions were expanded")
    errors.extend(
        _compare_policy(
            report_name,
            candidate_scope.get("policy", {}),
            reference_scope.get("policy", {}),
        )
    )
    return errors


def _compare_policy(
    report_name: str,
    candidate_policy: Any,
    reference_policy: Any,
) -> list[str]:
    if not isinstance(candidate_policy, dict) or not isinstance(reference_policy, dict):
        return [f"{report_name}: baseline measurement policy is malformed"]
    errors: list[str] = []
    for field in ("all", "all_targets", "branch", "include_namespace_packages", "workspace"):
        if reference_policy.get(field) is True and candidate_policy.get(field) is not True:
            errors.append(f"{report_name}: measurement policy disabled {field}")
    for field in ("features", "source_dirs", "source_pkgs", "tests"):
        old_values = set(reference_policy.get(field, []))
        new_values = set(candidate_policy.get(field, []))
        if not old_values.issubset(new_values):
            errors.append(f"{report_name}: measurement policy narrowed {field}")
    old_deselections = set(reference_policy.get("test_deselections", []))
    new_deselections = set(candidate_policy.get("test_deselections", []))
    if not new_deselections.issubset(old_deselections):
        errors.append(f"{report_name}: measurement policy added test deselections")
    for field in ("manifest", "source"):
        if reference_policy.get(field) != candidate_policy.get(field):
            errors.append(f"{report_name}: measurement policy changed {field}")
    for field in ("c8", "cargo_llvm_cov", "coverage", "pytest", "rust"):
        if field in reference_policy and (
            reference_policy.get(field) != candidate_policy.get(field)
        ):
            errors.append(f"{report_name}: measurement policy changed {field}")
    return errors


def _compare_baselines(
    candidate: dict[str, Any],
    reference: dict[str, Any],
    changed_line_by_report: dict[str, int] | None = None,
) -> list[str]:
    errors: list[str] = []
    changed_line_by_report = changed_line_by_report or {}
    candidate_reports = candidate["reports"]
    for report_name, old_config in reference["reports"].items():
        new_config = candidate_reports.get(report_name)
        if not isinstance(new_config, dict):
            errors.append(f"{report_name}: baseline report was removed")
            continue
        for field in ("format", "path"):
            if new_config.get(field) != old_config.get(field):
                errors.append(f"{report_name}: baseline {field} changed")
        errors.extend(_compare_scope(report_name, new_config, old_config))
        errors.extend(compare_growth_policy(report_name, new_config, old_config))
        if not set(old_config.get("files", [])).issubset(
            set(new_config.get("files", []))
        ):
            errors.append(f"{report_name}: baseline source files were removed")
        old_metric_by_name = old_config.get("metrics")
        new_metric_by_name = new_config.get("metrics")
        if not isinstance(old_metric_by_name, dict) or not isinstance(
            new_metric_by_name, dict
        ):
            errors.append(f"{report_name}: baseline metrics are malformed")
            continue
        for metric_name, old_metric in old_metric_by_name.items():
            new_metric = new_metric_by_name.get(metric_name)
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
            changed_line_count = changed_line_by_report.get(report_name, 0)
            if changed_line_count:
                growth_policy_by_field = load_growth_policy(report_name, new_config)
                errors.extend(
                    compare_growth_metric(
                        f"{report_name}.{metric_name} baseline",
                        new_metric,
                        old_metric,
                        growth_policy_by_field["target_percent_by_metric"][metric_name],
                        growth_policy_by_field,
                        changed_line_count,
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
        snapshot = _collect_report(
            root, report_name, config, enforce_baseline_files=False
        )
        config["metrics"] = snapshot.metric_by_name
        config["files"] = sorted(snapshot.files)
    path.write_text(
        json.dumps(baseline, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _require_self_test(condition: bool, message: str) -> None:
    if not condition:
        raise CoverageRatchetError(f"coverage ratchet self-test failed: {message}")


def _assert_metric_behavior() -> None:
    exact_metric_map = {"covered": 80, "total": 100}
    _require_self_test(
        not _compare_metric("exact", exact_metric_map, exact_metric_map),
        "exact metric",
    )
    _require_self_test(
        not _compare_metric(
            "better", {"covered": 90, "total": 100}, exact_metric_map
        ),
        "improved metric",
    )
    _require_self_test(
        bool(_compare_metric("ratio", {"covered": 79, "total": 100}, exact_metric_map)),
        "ratio regression",
    )
    _require_self_test(
        bool(
            _compare_metric(
                "debt", {"covered": 160, "total": 200}, exact_metric_map
            )
        ),
        "uncovered debt regression",
    )


def _assert_parser_behavior() -> None:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        (root / "sample.py").write_text("value = 1\n", encoding="utf-8")
        report_document_map = {
            "files": {
                "sample.py": {
                    "summary": {
                        "covered_lines": 8,
                        "num_statements": 10,
                        "covered_branches": 3,
                        "num_branches": 4,
                    }
                }
            }
        }
        (root / "coverage.json").write_text(
            json.dumps(report_document_map), encoding="utf-8"
        )
        snapshot = _collect_report(
            root,
            "python",
            {
                "format": "coverage.py",
                "path": "coverage.json",
                "scope": {"include": ["*.py"], "exclude": []},
            },
        )
        _require_self_test(
            snapshot.metric_by_name
            == {
                "lines": {"covered": 8, "total": 10},
                "branches": {"covered": 3, "total": 4},
            },
            "coverage.py parser",
        )


def _assert_reference_behavior() -> None:
    exact_metric_map = {"covered": 80, "total": 100}
    reference_baseline_map = {
        "reports": {
            "python": {
                "format": "coverage.py",
                "path": "coverage.json",
                "scope": {"include": ["*.py"], "exclude": [], "policy": {}},
                "files": ["sample.py"],
                "metrics": {"lines": exact_metric_map},
                "growth": {
                    "changed_line_divisor": 10,
                    "debt_reduction_percent": 1,
                    "target_percent_by_metric": {"lines": 95},
                },
            }
        }
    }
    _require_self_test(
        not _compare_baselines(reference_baseline_map, reference_baseline_map),
        "unchanged baseline",
    )
    lowered_baseline_map = json.loads(json.dumps(reference_baseline_map))
    lowered_baseline_map["reports"]["python"]["metrics"]["lines"] = {
        "covered": 79,
        "total": 100,
    }
    _require_self_test(
        bool(_compare_baselines(lowered_baseline_map, reference_baseline_map)),
        "lowered baseline",
    )
    narrowed_baseline_map = json.loads(json.dumps(reference_baseline_map))
    narrowed_baseline_map["reports"]["python"]["scope"]["include"] = []
    _require_self_test(
        bool(_compare_baselines(narrowed_baseline_map, reference_baseline_map)),
        "narrowed scope",
    )


def _assert_growth_behavior() -> None:
    reference_baseline_by_field = build_growth_policy_test_baseline()
    unchanged_baseline_by_field = json.loads(json.dumps(reference_baseline_by_field))
    _require_self_test(
        bool(
            _compare_baselines(
                unchanged_baseline_by_field,
                reference_baseline_by_field,
                {"python": 5},
            )
        ),
        "changed source requires debt reduction",
    )
    improved_baseline_by_field = json.loads(json.dumps(reference_baseline_by_field))
    improved_baseline_by_field["reports"]["python"]["metrics"]["lines"] = {
        "covered": 81,
        "total": 100,
    }
    _require_self_test(
        not _compare_baselines(
            improved_baseline_by_field,
            reference_baseline_by_field,
            {"python": 5},
        ),
        "one-unit debt reduction for a small change",
    )
    target_baseline_by_field = json.loads(json.dumps(reference_baseline_by_field))
    target_baseline_by_field["reports"]["python"]["metrics"]["lines"] = {
        "covered": 95,
        "total": 100,
    }
    _require_self_test(
        not _compare_baselines(
            target_baseline_by_field,
            target_baseline_by_field,
            {"python": 100},
        ),
        "growth stops at the configured target",
    )
    weakened_baseline_by_field = json.loads(json.dumps(reference_baseline_by_field))
    weakened_baseline_by_field["reports"]["python"]["growth"][
        "target_percent_by_metric"
    ]["lines"] = 90
    _require_self_test(
        bool(
            _compare_baselines(
                weakened_baseline_by_field,
                reference_baseline_by_field,
            )
        ),
        "growth target weakening",
    )


def _run_self_test() -> None:
    _assert_metric_behavior()
    _assert_parser_behavior()
    _assert_reference_behavior()
    _assert_growth_behavior()
    run_growth_helper_self_test()
    run_exclusion_guard_self_test()
    print("coverage ratchet self-test passed")


def _parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", default="test-coverage-baseline.json")
    parser.add_argument("--reference-baseline")
    parser.add_argument(
        "--changed-since",
        help="Git revision used to measure changed production-source lines",
    )
    parser.add_argument(
        "--report",
        action="append",
        dest="report_names",
        help="check only the named report; repeat for multiple reports",
    )
    parser.add_argument("--write-baseline", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def _check_current_report(
    root: Path,
    report_name: str,
    config: dict[str, Any],
) -> list[str]:
    snapshot = _collect_report(root, report_name, config)
    minimum_metric_by_name = config.get("metrics")
    if not isinstance(minimum_metric_by_name, dict) or not minimum_metric_by_name:
        raise CoverageRatchetError(
            f"{report_name}: baseline metrics are missing; use --write-baseline"
        )
    errors: list[str] = []
    for metric_name, minimum in minimum_metric_by_name.items():
        current = snapshot.metric_by_name.get(metric_name)
        if current is None:
            errors.append(f"{report_name}.{metric_name}: current metric is missing")
            continue
        print(
            f"{report_name}.{metric_name}: {_percent(current):.4f}% "
            f"({current['covered']}/{current['total']}, "
            f"missing {current['total'] - current['covered']})"
        )
        errors.extend(
            _compare_metric(f"{report_name}.{metric_name}", current, minimum)
        )
    return errors


def _execute_gate(args: argparse.Namespace) -> int:
    root = Path.cwd()
    baseline_path = root / args.baseline
    baseline = _load_baseline(baseline_path)
    selected_names = args.report_names or list(baseline["reports"])
    unknown_names = sorted(set(selected_names) - set(baseline["reports"]))
    if unknown_names:
        raise CoverageRatchetError(
            f"unknown baseline reports: {', '.join(unknown_names)}"
        )
    if args.write_baseline:
        _write_baseline(baseline_path, baseline, root, selected_names)
        baseline = _load_baseline(baseline_path)
    for report_name in selected_names:
        load_growth_policy(report_name, baseline["reports"][report_name])
    errors: list[str] = []
    changed_line_by_report: dict[str, int] = {}
    if args.reference_baseline:
        if not args.changed_since:
            raise CoverageRatchetError(
                "--reference-baseline requires --changed-since"
            )
        reference = _load_baseline(Path(args.reference_baseline))
        changed_line_by_report, exclusion_errors = collect_growth_evidence(
            root,
            args.changed_since,
            baseline,
            selected_names,
        )
        errors.extend(exclusion_errors)
        errors.extend(
            _compare_baselines(baseline, reference, changed_line_by_report)
        )
    elif args.changed_since:
        raise CoverageRatchetError(
            "--changed-since requires --reference-baseline"
        )
    for report_name in selected_names:
        changed_line_count = changed_line_by_report.get(report_name, 0)
        if changed_line_count:
            print(
                f"{report_name}: {changed_line_count} changed production-source "
                "lines require coverage debt reduction"
            )
        elif args.changed_since:
            print(f"{report_name}: no production-source changes; no debt paydown required")
    for report_name in selected_names:
        errors.extend(
            _check_current_report(root, report_name, baseline["reports"][report_name])
        )
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print("Test coverage satisfies the versioned no-regression and growth policy.")
    return 0


def run_coverage_ratchet() -> int:
    """Run self-tests, update a baseline, or enforce the current baseline."""
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
