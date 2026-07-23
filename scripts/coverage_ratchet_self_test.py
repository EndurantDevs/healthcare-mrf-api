"""Executable behavior checks for the coverage ratchet."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any, Callable

from coverage_growth import (
    build_growth_policy_test_baseline,
    run_exclusion_guard_self_test,
    run_growth_helper_self_test,
)
from coverage_reports import CoverageRatchetError


CompareMetric = Callable[[str, dict[str, int], dict[str, int]], list[str]]
CompareBaselines = Callable[
    [dict[str, Any], dict[str, Any], dict[str, int] | None],
    list[str],
]
CollectReport = Callable[..., Any]


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise CoverageRatchetError(f"coverage ratchet self-test failed: {message}")


def _assert_metric_behavior(compare_metric: CompareMetric) -> None:
    exact_metric_by_field = {"covered": 80, "total": 100}
    _require(
        not compare_metric("exact", exact_metric_by_field, exact_metric_by_field),
        "exact metric",
    )
    _require(
        not compare_metric(
            "better",
            {"covered": 90, "total": 100},
            exact_metric_by_field,
        ),
        "improved metric",
    )
    _require(
        bool(
            compare_metric(
                "ratio",
                {"covered": 79, "total": 100},
                exact_metric_by_field,
            )
        ),
        "ratio regression",
    )
    _require(
        bool(
            compare_metric(
                "debt",
                {"covered": 160, "total": 200},
                exact_metric_by_field,
            )
        ),
        "uncovered debt regression",
    )


def _assert_parser_behavior(collect_report: CollectReport) -> None:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        (root / "sample.py").write_text("value = 1\n", encoding="utf-8")
        report_by_field = {
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
            json.dumps(report_by_field),
            encoding="utf-8",
        )
        snapshot = collect_report(
            root,
            "python",
            {
                "format": "coverage.py",
                "path": "coverage.json",
                "scope": {"include": ["*.py"], "exclude": []},
            },
        )
        _require(
            snapshot.metric_by_name
            == {
                "lines": {"covered": 8, "total": 10},
                "branches": {"covered": 3, "total": 4},
            },
            "coverage.py parser",
        )


def _reference_baseline() -> dict[str, Any]:
    exact_metric_by_field = {"covered": 80, "total": 100}
    return {
        "reports": {
            "python": {
                "format": "coverage.py",
                "path": "coverage.json",
                "scope": {"include": ["*.py"], "exclude": [], "policy": {}},
                "files": ["sample.py"],
                "metrics": {"lines": exact_metric_by_field},
                "growth": {
                    "changed_line_divisor": 10,
                    "debt_reduction_percent": 1,
                    "target_percent_by_metric": {"lines": 95},
                },
            }
        }
    }


def _one_point_reset(
    reference_by_field: dict[str, Any],
) -> dict[str, Any]:
    reset_by_field = json.loads(json.dumps(reference_by_field))
    reset_by_field["reports"]["python"]["metrics"]["lines"] = {
        "covered": 79,
        "total": 100,
    }
    reset_by_field["reports"]["python"]["one_time_floor_reset"] = {
        "maximum_drop_basis_points": 100,
        "reason": "test migration",
        "reference_metrics": reference_by_field["reports"]["python"]["metrics"],
    }
    return reset_by_field


def _assert_exact_and_reset_behavior(compare_baselines: CompareBaselines) -> None:
    reference_by_field = _reference_baseline()
    _require(
        not compare_baselines(reference_by_field, reference_by_field, None),
        "unchanged baseline",
    )
    lowered_by_field = json.loads(json.dumps(reference_by_field))
    lowered_by_field["reports"]["python"]["metrics"]["lines"] = {
        "covered": 79,
        "total": 100,
    }
    _require(
        bool(compare_baselines(lowered_by_field, reference_by_field, None)),
        "lowered baseline",
    )
    _require(
        not compare_baselines(
            _one_point_reset(reference_by_field),
            reference_by_field,
            None,
        ),
        "anchored one-point floor reset",
    )


def _assert_mixed_reset_growth(compare_baselines: CompareBaselines) -> None:
    reference_by_field = _reference_baseline()
    reference_by_field["reports"]["python"]["metrics"]["branches"] = {
        "covered": 80,
        "total": 100,
    }
    reference_by_field["reports"]["python"]["growth"][
        "target_percent_by_metric"
    ]["branches"] = 95
    reset_by_field = _one_point_reset(reference_by_field)
    errors = compare_baselines(
        reset_by_field,
        reference_by_field,
        {"python": 5},
    )
    _require(
        any(
            "python.branches baseline: uncovered debt must fall" in error
            for error in errors
        ),
        "reset does not bypass growth for unchanged metrics",
    )


def _assert_invalid_reset_behavior(compare_baselines: CompareBaselines) -> None:
    reference_by_field = _reference_baseline()
    reset_by_field = _one_point_reset(reference_by_field)
    repeated_by_field = json.loads(json.dumps(reset_by_field))
    repeated_by_field["reports"]["python"]["metrics"]["lines"] = {
        "covered": 78,
        "total": 100,
    }
    _require(
        bool(compare_baselines(repeated_by_field, reset_by_field, None)),
        "one-time floor reset cannot repeat",
    )
    _require(
        bool(compare_baselines(repeated_by_field, reference_by_field, None)),
        "floor reset beyond one point",
    )
    unused_by_field = _one_point_reset(reference_by_field)
    unused_by_field["reports"]["python"]["metrics"]["lines"] = {
        "covered": 80,
        "total": 100,
    }
    _require(
        bool(compare_baselines(unused_by_field, reference_by_field, None)),
        "unused reset marker",
    )
    narrowed_by_field = json.loads(json.dumps(reference_by_field))
    narrowed_by_field["reports"]["python"]["scope"]["include"] = []
    _require(
        bool(compare_baselines(narrowed_by_field, reference_by_field, None)),
        "narrowed scope",
    )


def _assert_growth_behavior(compare_baselines: CompareBaselines) -> None:
    reference_by_field = build_growth_policy_test_baseline()
    unchanged_by_field = json.loads(json.dumps(reference_by_field))
    _require(
        bool(
            compare_baselines(
                unchanged_by_field,
                reference_by_field,
                {"python": 5},
            )
        ),
        "changed source requires debt reduction",
    )
    improved_by_field = json.loads(json.dumps(reference_by_field))
    improved_by_field["reports"]["python"]["metrics"]["lines"] = {
        "covered": 81,
        "total": 100,
    }
    _require(
        not compare_baselines(
            improved_by_field,
            reference_by_field,
            {"python": 5},
        ),
        "one-unit debt reduction for a small change",
    )
    target_by_field = json.loads(json.dumps(reference_by_field))
    target_by_field["reports"]["python"]["metrics"]["lines"] = {
        "covered": 95,
        "total": 100,
    }
    _require(
        not compare_baselines(
            target_by_field,
            target_by_field,
            {"python": 100},
        ),
        "growth stops at the configured target",
    )
    weakened_by_field = json.loads(json.dumps(reference_by_field))
    weakened_by_field["reports"]["python"]["growth"][
        "target_percent_by_metric"
    ]["lines"] = 90
    _require(
        bool(compare_baselines(weakened_by_field, reference_by_field, None)),
        "growth target weakening",
    )


def run_self_test(
    compare_metric: CompareMetric,
    compare_baselines: CompareBaselines,
    collect_report: CollectReport,
) -> None:
    """Exercise ratchet parsing, floors, resets, and growth policy."""
    _assert_metric_behavior(compare_metric)
    _assert_parser_behavior(collect_report)
    _assert_exact_and_reset_behavior(compare_baselines)
    _assert_mixed_reset_growth(compare_baselines)
    _assert_invalid_reset_behavior(compare_baselines)
    _assert_growth_behavior(compare_baselines)
    run_growth_helper_self_test()
    run_exclusion_guard_self_test()
    print("coverage ratchet self-test passed")
