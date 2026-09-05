"""Executable behavior checks for the coverage ratchet."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any, Callable

from coverage_growth import (
    build_diff_policy_test_baseline,
    run_diff_helper_self_test,
    run_exclusion_guard_self_test,
)
from coverage_reports import CoverageRatchetError

CompareMetric = Callable[[str, dict[str, int], dict[str, int]], list[str]]
CompareBaselines = Callable[..., list[str]]
CollectReport = Callable[..., Any]


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise CoverageRatchetError(f"coverage ratchet self-test failed: {message}")


def _assert_metric_behavior(compare_metric: CompareMetric) -> None:
    floor = {"covered": 80, "total": 100}
    _require(not compare_metric("exact", floor, floor), "exact metric")
    _require(
        not compare_metric("larger", {"covered": 160, "total": 200}, floor),
        "equal ratio with more uncovered lines",
    )
    _require(
        bool(compare_metric("ratio", {"covered": 79, "total": 100}, floor)),
        "ratio regression",
    )


def _assert_parser_behavior(collect_report: CollectReport) -> None:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        (root / "sample.py").write_text("value = 1\n", encoding="utf-8")
        (root / "coverage.json").write_text(
            json.dumps(
                {
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
            ),
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


def _assert_reference_behavior(compare_baselines: CompareBaselines) -> None:
    baseline = build_diff_policy_test_baseline()
    _require(not compare_baselines(baseline, baseline), "unchanged baseline")
    expanded_exclusions = json.loads(json.dumps(baseline))
    expanded_exclusions["reports"]["python"]["growth"]["diff_exclude"] = [
        "generated.py"
    ]
    _require(
        bool(compare_baselines(expanded_exclusions, baseline)),
        "new diff exclusion",
    )
    regressed = json.loads(json.dumps(baseline))
    regressed["reports"]["python"]["metrics"]["lines"] = {
        "covered": 79,
        "total": 100,
    }
    _require(bool(compare_baselines(regressed, baseline)), "ratio regression")
    larger = json.loads(json.dumps(baseline))
    larger["reports"]["python"]["metrics"]["lines"] = {
        "covered": 160,
        "total": 200,
    }
    _require(
        not compare_baselines(larger, baseline),
        "equal ratio with more uncovered lines",
    )
    weakened = json.loads(json.dumps(baseline))
    weakened["reports"]["python"]["growth"]["diff_coverage_percent"] = 84
    _require(bool(compare_baselines(weakened, baseline)), "weakened diff policy")

    legacy = json.loads(json.dumps(baseline))
    legacy["reports"]["python"]["scope"]["policy"]["coverage"] = "7.15.2"
    transition = json.loads(json.dumps(legacy))
    transition["machine_artifact_required"] = True
    transition["reports"]["python"]["scope"]["policy"]["coverage"] = "7.16.0"
    _require(
        not compare_baselines(transition, legacy),
        "machine artifact bootstrap",
    )
    marker_removed = json.loads(json.dumps(transition))
    marker_removed.pop("machine_artifact_required")
    _require(
        bool(compare_baselines(marker_removed, transition)),
        "machine artifact marker removal",
    )
    version_changed = json.loads(json.dumps(transition))
    version_changed["reports"]["python"]["scope"]["policy"]["coverage"] = "7.17.0"
    _require(
        bool(compare_baselines(version_changed, transition)),
        "post-bootstrap coverage version change",
    )


def run_self_test(
    compare_metric: CompareMetric,
    compare_baselines: CompareBaselines,
    collect_report: CollectReport,
) -> None:
    """Exercise ratio floors, report parsing, and changed-line policy."""

    _assert_metric_behavior(compare_metric)
    _assert_parser_behavior(collect_report)
    _assert_reference_behavior(compare_baselines)
    run_diff_helper_self_test()
    run_exclusion_guard_self_test()
    print("coverage ratchet self-test passed")
