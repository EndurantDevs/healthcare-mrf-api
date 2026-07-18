"""Calculate target-aware coverage debt paydown for changed source code."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

from coverage_reports import (
    CoverageRatchetError,
    Metric,
    _is_path_in_scope,
    _metric,
)

COVERAGE_EXCLUSION_MARKERS = (
    "c8ignore",
    "coverage(off)",
    "istanbulignore",
    "node:coverageignore",
    "pragma:nobranch",
    "pragma:nocover",
)


def load_growth_policy(
    report_name: str,
    report_config_by_field: dict[str, Any],
) -> dict[str, Any]:
    """Validate and return one report's versioned growth policy."""
    growth_policy_by_field = report_config_by_field.get("growth")
    if not isinstance(growth_policy_by_field, dict):
        raise CoverageRatchetError(f"{report_name}: coverage growth policy is missing")
    debt_reduction_percent = growth_policy_by_field.get("debt_reduction_percent")
    changed_line_divisor = growth_policy_by_field.get("changed_line_divisor")
    target_percent_by_metric = growth_policy_by_field.get(
        "target_percent_by_metric"
    )
    if not isinstance(debt_reduction_percent, int) or not 1 <= debt_reduction_percent <= 100:
        raise CoverageRatchetError(
            f"{report_name}: debt_reduction_percent must be an integer from 1 to 100"
        )
    if not isinstance(changed_line_divisor, int) or changed_line_divisor <= 0:
        raise CoverageRatchetError(
            f"{report_name}: changed_line_divisor must be a positive integer"
        )
    metric_by_name = report_config_by_field.get("metrics")
    if not isinstance(metric_by_name, dict) or not isinstance(
        target_percent_by_metric,
        dict,
    ):
        raise CoverageRatchetError(f"{report_name}: coverage growth targets are malformed")
    if set(target_percent_by_metric) != set(metric_by_name):
        raise CoverageRatchetError(
            f"{report_name}: coverage growth targets must match the tracked metrics"
        )
    if not all(
        isinstance(target_percent, int) and 1 <= target_percent <= 100
        for target_percent in target_percent_by_metric.values()
    ):
        raise CoverageRatchetError(
            f"{report_name}: coverage growth targets must be integer percentages"
        )
    return growth_policy_by_field


def compare_growth_policy(
    report_name: str,
    candidate_report_by_field: dict[str, Any],
    reference_report_by_field: dict[str, Any],
) -> list[str]:
    """Reject attempts to weaken an established growth policy."""
    current_growth_by_field = load_growth_policy(
        report_name,
        candidate_report_by_field,
    )
    previous_growth_by_field = reference_report_by_field.get("growth")
    if previous_growth_by_field is None:
        return []
    if not isinstance(previous_growth_by_field, dict):
        return [f"{report_name}: base coverage growth policy is malformed"]
    previous_growth_by_field = load_growth_policy(
        report_name,
        reference_report_by_field,
    )
    error_messages: list[str] = []
    if (
        current_growth_by_field["debt_reduction_percent"]
        < previous_growth_by_field["debt_reduction_percent"]
    ):
        error_messages.append(
            f"{report_name}: coverage debt reduction rate was lowered"
        )
    if (
        current_growth_by_field["changed_line_divisor"]
        > previous_growth_by_field["changed_line_divisor"]
    ):
        error_messages.append(
            f"{report_name}: coverage changed-line divisor was weakened"
        )
    previous_target_by_metric = previous_growth_by_field["target_percent_by_metric"]
    current_target_by_metric = current_growth_by_field["target_percent_by_metric"]
    for metric_name, previous_target in previous_target_by_metric.items():
        if current_target_by_metric.get(metric_name, 0) < previous_target:
            error_messages.append(
                f"{report_name}.{metric_name}: coverage target was lowered"
            )
    return error_messages


def calculate_required_debt_reduction(
    base_metric: Metric,
    target_percent: int,
    debt_reduction_percent: int,
    changed_line_count: int,
    changed_line_divisor: int,
) -> int:
    """Return the bounded number of uncovered units a PR must remove."""
    base_metric = _metric(
        base_metric.get("covered"),
        base_metric.get("total"),
        "base metric",
    )
    if base_metric["covered"] * 100 >= target_percent * base_metric["total"]:
        return 0
    base_missing_count = base_metric["total"] - base_metric["covered"]
    target_covered_count = (
        target_percent * base_metric["total"] + 99
    ) // 100
    target_gap_count = target_covered_count - base_metric["covered"]
    percentage_reduction = (
        base_missing_count * debt_reduction_percent + 99
    ) // 100
    changed_line_reduction = max(
        1,
        (changed_line_count + changed_line_divisor - 1) // changed_line_divisor,
    )
    return min(percentage_reduction, changed_line_reduction, target_gap_count)


def compare_growth_metric(
    label: str,
    candidate_metric: Metric,
    reference_metric: Metric,
    target_percent: int,
    growth_policy_by_field: dict[str, Any],
    changed_line_count: int,
) -> list[str]:
    """Check one candidate metric for its required debt reduction."""
    candidate_metric = _metric(
        candidate_metric.get("covered"),
        candidate_metric.get("total"),
        label,
    )
    reference_metric = _metric(
        reference_metric.get("covered"),
        reference_metric.get("total"),
        label,
    )
    if (
        candidate_metric["covered"] * 100
        >= target_percent * candidate_metric["total"]
    ):
        return []
    required_reduction = calculate_required_debt_reduction(
        reference_metric,
        target_percent,
        growth_policy_by_field["debt_reduction_percent"],
        changed_line_count,
        growth_policy_by_field["changed_line_divisor"],
    )
    if not required_reduction:
        return []
    previous_missing_count = reference_metric["total"] - reference_metric["covered"]
    current_missing_count = candidate_metric["total"] - candidate_metric["covered"]
    required_maximum = previous_missing_count - required_reduction
    if current_missing_count <= required_maximum:
        return []
    return [
        f"{label}: uncovered debt must fall by {required_reduction} to "
        f"{required_maximum} or less on {changed_line_count} changed source lines "
        f"(target {target_percent}%)"
    ]


def count_changed_lines_from_numstat(
    diff_text: str,
    baseline_by_field: dict[str, Any],
    selected_report_names: list[str],
) -> dict[str, int]:
    """Count changed in-scope source lines from Git numstat text."""
    changed_line_count_by_report = {
        report_name: 0 for report_name in selected_report_names
    }
    record_separator = "\0" if "\0" in diff_text else "\n"
    for raw_line in diff_text.split(record_separator):
        fields = raw_line.split("\t", 2)
        if len(fields) != 3 or fields[0] == "-" or fields[1] == "-":
            continue
        added_line_count, removed_line_count, relative_path = fields
        changed_line_count = int(added_line_count) + int(removed_line_count)
        for report_name in selected_report_names:
            report_config_by_field = baseline_by_field["reports"][report_name]
            if _is_path_in_scope(relative_path, report_config_by_field):
                changed_line_count_by_report[report_name] += changed_line_count
    return changed_line_count_by_report


def count_changed_lines(
    root: Path,
    base_revision: str,
    baseline_by_field: dict[str, Any],
    selected_report_names: list[str],
) -> dict[str, int]:
    """Run Git numstat and count changed source lines for each report."""
    command_parts = [
        "git",
        "diff",
        "--numstat",
        "-z",
        "--no-renames",
        f"{base_revision}...HEAD",
        "--",
    ]
    try:
        completed_process = subprocess.run(
            command_parts,
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise CoverageRatchetError(
            f"could not measure changed source lines from {base_revision}"
        ) from exc
    return count_changed_lines_from_numstat(
        completed_process.stdout,
        baseline_by_field,
        selected_report_names,
    )


def find_added_exclusion_directives_in_diff(
    diff_text: str,
    baseline_by_field: dict[str, Any],
    selected_report_names: list[str],
) -> list[str]:
    """Return new in-scope source directives that suppress coverage."""
    error_messages: list[str] = []
    current_relative_path: str | None = None
    for raw_line in diff_text.splitlines():
        if raw_line.startswith("+++ "):
            header_path = raw_line[4:]
            current_relative_path = (
                header_path[2:] if header_path.startswith("b/") else None
            )
            continue
        if not current_relative_path or not raw_line.startswith("+"):
            continue
        compact_added_line = "".join(raw_line[1:].lower().split())
        if not any(
            marker in compact_added_line for marker in COVERAGE_EXCLUSION_MARKERS
        ):
            continue
        for report_name in selected_report_names:
            report_config_by_field = baseline_by_field["reports"][report_name]
            if _is_path_in_scope(current_relative_path, report_config_by_field):
                error_messages.append(
                    f"{report_name}: new coverage exclusion directive in "
                    f"{current_relative_path}"
                )
    return error_messages


def find_added_exclusion_directives(
    root: Path,
    base_revision: str,
    baseline_by_field: dict[str, Any],
    selected_report_names: list[str],
) -> list[str]:
    """Inspect the Git diff for newly added coverage suppressions."""
    command_parts = [
        "git",
        "diff",
        "--unified=0",
        "--no-renames",
        f"{base_revision}...HEAD",
        "--",
    ]
    try:
        completed_process = subprocess.run(
            command_parts,
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise CoverageRatchetError(
            f"could not inspect coverage exclusions from {base_revision}"
        ) from exc
    return find_added_exclusion_directives_in_diff(
        completed_process.stdout,
        baseline_by_field,
        selected_report_names,
    )


def collect_growth_evidence(
    root: Path,
    base_revision: str,
    baseline_by_field: dict[str, Any],
    selected_report_names: list[str],
) -> tuple[dict[str, int], list[str]]:
    """Collect changed-line counts and coverage-exclusion errors."""
    changed_line_count_by_report = count_changed_lines(
        root,
        base_revision,
        baseline_by_field,
        selected_report_names,
    )
    exclusion_errors = find_added_exclusion_directives(
        root,
        base_revision,
        baseline_by_field,
        selected_report_names,
    )
    return changed_line_count_by_report, exclusion_errors


def build_growth_policy_test_baseline() -> dict[str, Any]:
    """Build the compact baseline fixture used by the gate self-test."""
    return {
        "reports": {
            "python": {
                "format": "coverage.py",
                "path": "coverage.json",
                "scope": {"include": ["*.py"], "exclude": [], "policy": {}},
                "files": ["sample.py"],
                "metrics": {"lines": {"covered": 80, "total": 100}},
                "growth": {
                    "changed_line_divisor": 10,
                    "debt_reduction_percent": 1,
                    "target_percent_by_metric": {"lines": 95},
                },
            }
        }
    }


def run_growth_helper_self_test() -> None:
    """Exercise bounded debt math and changed-source classification."""
    required_reduction = calculate_required_debt_reduction(
        {"covered": 100, "total": 1000},
        95,
        1,
        5000,
        10,
    )
    if required_reduction != 9:
        raise CoverageRatchetError(
            "coverage ratchet self-test failed: full one-percent debt reduction"
        )
    near_target_reduction = calculate_required_debt_reduction(
        {"covered": 9499, "total": 10000},
        95,
        1,
        5000,
        10,
    )
    if near_target_reduction != 1:
        raise CoverageRatchetError(
            "coverage ratchet self-test failed: target gap caps debt reduction"
        )
    target_metric_errors = compare_growth_metric(
        "target",
        {"covered": 380, "total": 400},
        {"covered": 80, "total": 100},
        95,
        {
            "changed_line_divisor": 10,
            "debt_reduction_percent": 1,
        },
        5,
    )
    if target_metric_errors:
        raise CoverageRatchetError(
            "coverage ratchet self-test failed: reaching target stops paydown"
        )
    changed_line_count_by_report = count_changed_lines_from_numstat(
        "3\t2\tsample.py\n8\t1\tdocs/guide.md\n-\t-\tasset.bin\n",
        build_growth_policy_test_baseline(),
        ["python"],
    )
    if changed_line_count_by_report != {"python": 5}:
        raise CoverageRatchetError(
            "coverage ratchet self-test failed: source-line classification"
        )


def run_exclusion_guard_self_test() -> None:
    """Exercise rejection of newly added coverage suppressions."""
    exclusion_errors = find_added_exclusion_directives_in_diff(
        "diff --git a/sample.py b/sample.py\n"
        "+++ b/sample.py\n"
        "+value = fallback()  # pragma: no cover\n"
        "diff --git a/docs/guide.md b/docs/guide.md\n"
        "+++ b/docs/guide.md\n"
        "+document c8 ignore behavior\n",
        build_growth_policy_test_baseline(),
        ["python"],
    )
    if exclusion_errors != [
        "python: new coverage exclusion directive in sample.py"
    ]:
        raise CoverageRatchetError(
            "coverage ratchet self-test failed: exclusion directive guard"
        )
