"""Command-line behavior for readability budget checks."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from pathlib import Path
from typing import Any

from .source_files import collect_issues

DEFAULT_CONFIG = "readability-budget.json"
DEFAULT_BASELINE = "readability-baseline.json"
ONE_TIME_DEBT_RESET_BASIS_POINTS = 200
ONE_TIME_DEBT_RESET_FIELD = "one_time_debt_reset"
PROTECTED_NEW_ISSUE_CATEGORIES = frozenset(
    {
        "builtin_shadowing",
        "confusable_function_names",
        "global_state_usage",
        "inline_suppressions",
        "pass_placeholders",
        "syntax_errors",
    }
)


def build_snapshot(repo_root: Path, config: dict[str, Any]) -> dict[str, Any]:
    """Build the deterministic readability snapshot used for gating."""
    issues_by_category = collect_issues(repo_root, config)
    return {
        "version": 1,
        "rules": _rules_snapshot(config),
        "thresholds": config["thresholds"],
        "issues": {
            category: [issue.to_json() for issue in values]
            for category, values in sorted(issues_by_category.items())
        },
        "issue_counts": {
            category: len(values)
            for category, values in sorted(issues_by_category.items())
        },
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse CLI flags for checking or refreshing readability baselines."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--baseline", default=DEFAULT_BASELINE)
    parser.add_argument("--write-baseline", action="store_true")
    parser.add_argument(
        "--ratchet-baseline",
        type=Path,
        help="Base-branch readability snapshot used to enforce debt reduction",
    )
    parser.add_argument(
        "--required-reduction-percent",
        type=float,
        default=1.0,
        help="Minimum total debt reduction required by the ratchet (default: 1; 0 prevents net growth)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the readability budget check and return a process exit code."""
    args = parse_args(argv or sys.argv[1:])
    repo_root = args.repo_root.resolve()
    config_path = repo_root / args.config
    baseline_path = repo_root / args.baseline
    config = _load_json(config_path)
    snapshot = build_snapshot(repo_root, config)
    _print_summary(snapshot)
    if args.write_baseline:
        reset_marker = None
        if baseline_path.exists():
            reset_marker = _load_json(baseline_path).get(ONE_TIME_DEBT_RESET_FIELD)
        baseline_snapshot = _baseline_snapshot(snapshot)
        if reset_marker is not None:
            baseline_snapshot[ONE_TIME_DEBT_RESET_FIELD] = reset_marker
        baseline_path.write_text(
            json.dumps(baseline_snapshot, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(f"Wrote baseline: {baseline_path.relative_to(repo_root)}")
        return 0
    return _check_snapshot(repo_root, args, snapshot, baseline_path)


def _check_snapshot(
    repo_root: Path,
    args: argparse.Namespace,
    snapshot: dict[str, Any],
    baseline_path: Path,
) -> int:
    if not baseline_path.exists():
        print(f"Baseline is missing: {baseline_path.relative_to(repo_root)}", file=sys.stderr)
        return 2
    baseline = _load_json(baseline_path)
    if baseline.get("rules", {"thresholds": baseline.get("thresholds")}) != snapshot.get("rules"):
        print("Readability rules changed; regenerate the baseline intentionally.", file=sys.stderr)
        return 2
    if args.ratchet_baseline:
        ratchet_baseline_path = _resolve_repo_path(repo_root, args.ratchet_baseline)
        return _check_readability_ratchet(
            snapshot,
            baseline,
            ratchet_baseline_path,
            args.required_reduction_percent,
        )
    new_by_category = _new_issues(snapshot, baseline)
    if new_by_category:
        _print_new_issues(new_by_category)
        return 1
    print("No new readability debt relative to baseline.")
    return 0


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _resolve_repo_path(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _check_readability_ratchet(
    snapshot: dict[str, Any],
    baseline: dict[str, Any],
    ratchet_baseline_path: Path,
    required_reduction_percent: float,
) -> int:
    """Require a synchronized snapshot with less debt than the base branch."""
    if not 0 <= required_reduction_percent <= 100:
        print("Readability reduction percent must be between 0 and 100.", file=sys.stderr)
        return 2
    if not ratchet_baseline_path.exists():
        print(f"Ratchet baseline is missing: {ratchet_baseline_path}", file=sys.stderr)
        return 2

    ratchet_baseline = _load_json(ratchet_baseline_path)
    if not _has_compatible_ratchet_rules_by_section(ratchet_baseline, snapshot):
        print("Readability ratchet rules differ from the base branch.", file=sys.stderr)
        return 2
    reset_active, reset_error = _one_time_debt_reset(baseline, ratchet_baseline)
    if reset_error:
        print(reset_error, file=sys.stderr)
        return 2

    new_by_category = _new_issues(snapshot, ratchet_baseline)
    protected_new_issues_by_category = {
        category: issues
        for category, issues in new_by_category.items()
        if category in PROTECTED_NEW_ISSUE_CATEGORIES
    }
    if protected_new_issues_by_category:
        _print_new_issues(protected_new_issues_by_category)
        return 1
    if not _is_baseline_synchronized(baseline, snapshot):
        print(
            "Readability baseline is not synchronized; run scripts/readability_budget.py --write-baseline.",
            file=sys.stderr,
        )
        return 1

    base_total = _total_issue_count(ratchet_baseline)
    current_total = _total_issue_count(snapshot)
    if reset_active:
        required_reduction = 0
        target_total = (
            base_total * (10_000 + ONE_TIME_DEBT_RESET_BASIS_POINTS)
        ) // 10_000
        print(
            "Readability ratchet: "
            f"base={base_total} current={current_total} target<={target_total} "
            f"one_time_reset={ONE_TIME_DEBT_RESET_BASIS_POINTS / 100:.2f}%"
        )
    else:
        required_reduction = (
            math.ceil(base_total * required_reduction_percent / 100)
            if base_total
            else 0
        )
        target_total = max(0, base_total - required_reduction)
        print(
            "Readability ratchet: "
            f"base={base_total} current={current_total} target<={target_total} "
            f"reduction={required_reduction_percent:g}%"
        )
    ordinary_new_issue_count = _ordinary_new_issue_count(new_by_category)
    if ordinary_new_issue_count:
        if reset_active:
            print(
                "Readability ratchet: "
                f"{ordinary_new_issue_count} replacement finding(s) are bounded "
                "by the one-time reset ceiling."
            )
        else:
            print(
                "Readability ratchet: "
                f"{ordinary_new_issue_count} replacement finding(s) must be offset "
                "by the required net debt reduction."
            )
    if current_total > target_total:
        if reset_active:
            print(
                "Readability debt exceeds the one-time base-anchored "
                f"{ONE_TIME_DEBT_RESET_BASIS_POINTS / 100:.2f}% reset.",
                file=sys.stderr,
            )
        else:
            print(
                f"Readability debt must decrease by at least {required_reduction} finding(s).",
                file=sys.stderr,
            )
        return 1
    print("Readability ratchet satisfied.")
    return 0


def _ordinary_new_issue_count(new_issues_by_category: dict[str, list[dict[str, Any]]]) -> int:
    return sum(
        len(issues)
        for category, issues in new_issues_by_category.items()
        if category not in PROTECTED_NEW_ISSUE_CATEGORIES
    )


def _is_baseline_synchronized(baseline: dict[str, Any], snapshot: dict[str, Any]) -> bool:
    expected_baseline = _baseline_snapshot(snapshot)
    return (
        baseline.get("issue_counts") == expected_baseline["issue_counts"]
        and baseline.get("issue_ids") == expected_baseline["issue_ids"]
    )


def _total_issue_count(snapshot: dict[str, Any]) -> int:
    return sum(int(value) for value in snapshot.get("issue_counts", {}).values())


def _baseline_anchor_sha256(baseline: dict[str, Any]) -> str:
    """Return the stable identity used to anchor a one-time debt reset."""

    payload = {
        key: baseline.get(key)
        for key in ("version", "rules", "thresholds", "issue_counts", "issue_ids")
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _one_time_debt_reset(
    candidate: dict[str, Any],
    reference: dict[str, Any],
) -> tuple[bool, str | None]:
    """Validate a reset introduced against one exact base snapshot."""

    candidate_reset = candidate.get(ONE_TIME_DEBT_RESET_FIELD)
    reference_reset = reference.get(ONE_TIME_DEBT_RESET_FIELD)
    if reference_reset is not None:
        if candidate_reset != reference_reset:
            return False, "Established one-time readability reset marker changed."
        return False, None
    if candidate_reset is None:
        return False, None
    if not isinstance(candidate_reset, dict):
        return False, "One-time readability reset marker is malformed."
    if set(candidate_reset) != {
        "maximum_increase_basis_points",
        "reason",
        "reference_baseline_sha256",
        "reference_total",
    }:
        return False, "One-time readability reset fields are malformed."
    if (
        candidate_reset.get("maximum_increase_basis_points")
        != ONE_TIME_DEBT_RESET_BASIS_POINTS
    ):
        return (
            False,
            "One-time readability reset must be exactly "
            f"{ONE_TIME_DEBT_RESET_BASIS_POINTS} basis points.",
        )
    if not str(candidate_reset.get("reason") or "").strip():
        return False, "One-time readability reset reason is missing."
    if candidate_reset.get("reference_total") != _total_issue_count(reference):
        return False, "One-time readability reset has a stale base total."
    if candidate_reset.get("reference_baseline_sha256") != _baseline_anchor_sha256(
        reference
    ):
        return False, "One-time readability reset is not anchored to the base snapshot."
    return True, None


def _rules_snapshot(config: dict[str, Any]) -> dict[str, Any]:
    return {
        "readability": config.get("readability", {}),
        "thresholds": config.get("thresholds", {}),
    }


def _has_compatible_ratchet_rules_by_section(
    ratchet_baseline: dict[str, Any],
    snapshot: dict[str, Any],
) -> bool:
    """Allow the one-time confusable-name rule migration, then require exact rules."""

    ratchet_rules_by_section = ratchet_baseline.get(
        "rules",
        {"thresholds": ratchet_baseline.get("thresholds")},
    )
    current_rules_by_section = snapshot.get("rules")
    if ratchet_rules_by_section == current_rules_by_section:
        return True
    if "confusable_function_names" in ratchet_baseline.get("issue_counts", {}):
        return False
    if not isinstance(current_rules_by_section, dict):
        return False
    current_readability_by_name = dict(current_rules_by_section.get("readability", {}))
    exceptions = current_readability_by_name.pop("confusable_function_name_exceptions", None)
    if not isinstance(exceptions, list):
        return False
    migrated_rules_by_section = dict(current_rules_by_section)
    migrated_rules_by_section["readability"] = current_readability_by_name
    return ratchet_rules_by_section == migrated_rules_by_section


def _baseline_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {
        "version": snapshot.get("version", 1),
        "rules": snapshot.get("rules", {}),
        "thresholds": snapshot.get("thresholds", {}),
        "issue_counts": snapshot.get("issue_counts", {}),
        "issue_ids": {
            category: sorted(issue["id"] for issue in issues)
            for category, issues in snapshot.get("issues", {}).items()
        },
    }


def _issue_ids(snapshot: dict[str, Any], category: str) -> set[str]:
    issue_ids = snapshot.get("issue_ids", {}).get(category)
    if issue_ids is not None:
        return set(issue_ids)
    return {issue["id"] for issue in snapshot.get("issues", {}).get(category, [])}


def _comparison_issue_id(category: str, issue_id: str) -> str:
    """Return a stable issue identity for protected line-based findings."""

    if category == "global_state_usage":
        prefix, separator, names = issue_id.rpartition(":")
        identity, line_separator, line = prefix.rpartition(":")
        if separator and line_separator and line.isdigit():
            return f"{identity}:{names}"
    elif category == "pass_placeholders":
        identity, separator, line = issue_id.rpartition(":")
        if separator and line.isdigit():
            return identity
    return issue_id


def _new_issues(current: dict[str, Any], baseline: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    new_by_category: dict[str, list[dict[str, Any]]] = {}
    for category, current_issues in current.get("issues", {}).items():
        baseline_ids = {
            _comparison_issue_id(category, issue_id)
            for issue_id in _issue_ids(baseline, category)
        }
        new_items = [
            issue
            for issue in current_issues
            if _comparison_issue_id(category, issue["id"]) not in baseline_ids
        ]
        if new_items:
            new_by_category[category] = new_items
    return new_by_category


def _print_summary(snapshot: dict[str, Any]) -> None:
    print("Readability budget summary:")
    for category, count in sorted(snapshot["issue_counts"].items()):
        print(f"  {category}: {count}")


def _print_new_issues(new_by_category: dict[str, list[dict[str, Any]]]) -> None:
    print("New readability debt found:")
    for category, issues in sorted(new_by_category.items()):
        print(f"  {category}: {len(issues)}")
        for issue in issues[:20]:
            _print_issue(issue)
        if len(issues) > 20:
            print(f"    ... {len(issues) - 20} more")


def _print_issue(issue: dict[str, Any]) -> None:
    location = f"{issue['path']}:{issue.get('line', 1)}"
    detail = (
        issue.get("function")
        or issue.get("class")
        or issue.get("name")
        or issue.get("reason")
        or issue.get("pattern")
        or issue.get("lines")
        or issue.get("depth")
    )
    print(f"    {location} {detail}")
