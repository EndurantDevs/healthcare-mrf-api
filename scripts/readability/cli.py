"""Command-line behavior for readability budget checks."""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

from .source_files import collect_issues

DEFAULT_CONFIG = "readability-budget.json"
DEFAULT_BASELINE = "readability-baseline.json"
PROTECTED_NEW_ISSUE_CATEGORIES = frozenset(
    {
        "builtin_shadowing",
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
        baseline_path.write_text(
            json.dumps(_baseline_snapshot(snapshot), indent=2, sort_keys=True) + "\n",
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
    ratchet_rules = ratchet_baseline.get("rules", {"thresholds": ratchet_baseline.get("thresholds")})
    if ratchet_rules != snapshot.get("rules"):
        print("Readability ratchet rules differ from the base branch.", file=sys.stderr)
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
    required_reduction = math.ceil(base_total * required_reduction_percent / 100) if base_total else 0
    target_total = max(0, base_total - required_reduction)
    print(
        "Readability ratchet: "
        f"base={base_total} current={current_total} target<={target_total} "
        f"reduction={required_reduction_percent:g}%"
    )
    ordinary_new_issue_count = _ordinary_new_issue_count(new_by_category)
    if ordinary_new_issue_count:
        print(
            "Readability ratchet: "
            f"{ordinary_new_issue_count} replacement finding(s) must be offset "
            "by the required net debt reduction."
        )
    if current_total > target_total:
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


def _rules_snapshot(config: dict[str, Any]) -> dict[str, Any]:
    return {
        "readability": config.get("readability", {}),
        "thresholds": config.get("thresholds", {}),
    }


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


def _new_issues(current: dict[str, Any], baseline: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    new_by_category: dict[str, list[dict[str, Any]]] = {}
    for category, current_issues in current.get("issues", {}).items():
        baseline_ids = _issue_ids(baseline, category)
        new_items = [issue for issue in current_issues if issue["id"] not in baseline_ids]
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
