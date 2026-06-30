#!/usr/bin/env python3
"""Measure readability debt and fail when new debt is introduced."""

from __future__ import annotations

import argparse
import ast
import fnmatch
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


DEFAULT_CONFIG = "readability-budget.json"
DEFAULT_BASELINE = "readability-baseline.json"
NESTING_NODES = (
    ast.If,
    ast.For,
    ast.AsyncFor,
    ast.While,
    ast.With,
    ast.AsyncWith,
    ast.Try,
    ast.Match,
)
SCOPE_NODES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)


@dataclass(frozen=True)
class Issue:
    category: str
    identifier: str
    path: str
    detail: dict[str, Any]

    def to_json(self) -> dict[str, Any]:
        payload = {"id": self.identifier, "path": self.path}
        payload.update(self.detail)
        return payload


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _compile_suppression_patterns(config: dict[str, Any]) -> list[tuple[str, re.Pattern[str]]]:
    patterns = []
    for pattern in config.get("inline_suppression_patterns", []):
        patterns.append((pattern["name"], re.compile(pattern["pattern"])))
    return patterns


def _matches_any(path: str, patterns: Iterable[str]) -> bool:
    return any(fnmatch.fnmatch(path, pattern) for pattern in patterns)


def _iter_source_files(repo_root: Path, config: dict[str, Any]) -> list[Path]:
    roots = config.get("source_roots", [])
    exclude_globs = config.get("exclude_globs", [])
    include_suffixes = tuple(config.get("include_suffixes", [".py"]))
    files: list[Path] = []
    for source_root in roots:
        root_path = repo_root / source_root
        if root_path.is_file():
            candidates = [root_path]
        elif root_path.is_dir():
            candidates = [path for path in root_path.rglob("*") if path.is_file()]
        else:
            continue
        for path in candidates:
            relative = path.relative_to(repo_root).as_posix()
            if not path.name.endswith(include_suffixes):
                continue
            if _matches_any(relative, exclude_globs):
                continue
            files.append(path)
    return sorted(set(files))


def _line_count(path: Path) -> int:
    with path.open("r", encoding="utf-8") as handle:
        return sum(1 for _ in handle)


def _suppression_fingerprint(path: str, pattern_name: str, line: str) -> str:
    normalized = " ".join(line.strip().split())
    digest = hashlib.sha1(f"{path}:{pattern_name}:{normalized}".encode("utf-8")).hexdigest()[:12]
    return f"inline_suppression:{path}:{pattern_name}:{digest}"


def _find_inline_suppressions(
    repo_root: Path,
    path: Path,
    patterns: list[tuple[str, re.Pattern[str]]],
) -> list[Issue]:
    relative = path.relative_to(repo_root).as_posix()
    issues: list[Issue] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            for pattern_name, pattern in patterns:
                if pattern.search(line):
                    issues.append(
                        Issue(
                            "inline_suppressions",
                            _suppression_fingerprint(relative, pattern_name, line),
                            relative,
                            {
                                "line": line_number,
                                "pattern": pattern_name,
                                "text": line.strip(),
                            },
                        )
                    )
    return issues


def _max_nesting_depth(node: ast.AST, depth: int = 0) -> int:
    if isinstance(node, SCOPE_NODES):
        return depth
    current_depth = depth + 1 if isinstance(node, NESTING_NODES) else depth
    max_depth = current_depth
    for child in ast.iter_child_nodes(node):
        max_depth = max(max_depth, _max_nesting_depth(child, current_depth))
    return max_depth


class FunctionVisitor(ast.NodeVisitor):
    def __init__(self, repo_root: Path, path: Path, thresholds: dict[str, int]) -> None:
        self.repo_root = repo_root
        self.path = path
        self.thresholds = thresholds
        self.issues: list[Issue] = []
        self.scope: list[str] = []

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.scope.append(node.name)
        self.generic_visit(node)
        self.scope.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._visit_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._visit_function(node)

    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        relative = self.path.relative_to(self.repo_root).as_posix()
        qualified_name = ".".join([*self.scope, node.name])
        function_lines = node.end_lineno - node.lineno + 1 if node.end_lineno else 0
        max_function_lines = self.thresholds["max_function_lines"]
        if function_lines > max_function_lines:
            self.issues.append(
                Issue(
                    "long_functions",
                    f"long_function:{relative}:{qualified_name}",
                    relative,
                    {
                        "function": qualified_name,
                        "line": node.lineno,
                        "lines": function_lines,
                        "limit": max_function_lines,
                    },
                )
            )
        max_nesting_depth = max((_max_nesting_depth(child) for child in node.body), default=0)
        max_allowed_depth = self.thresholds["max_nesting_depth"]
        if max_nesting_depth > max_allowed_depth:
            self.issues.append(
                Issue(
                    "deep_nesting",
                    f"deep_nesting:{relative}:{qualified_name}",
                    relative,
                    {
                        "function": qualified_name,
                        "line": node.lineno,
                        "depth": max_nesting_depth,
                        "limit": max_allowed_depth,
                    },
                )
            )
        self.scope.append(node.name)
        self.generic_visit(node)
        self.scope.pop()


def _analyze_file(repo_root: Path, path: Path, thresholds: dict[str, int]) -> list[Issue]:
    relative = path.relative_to(repo_root).as_posix()
    issues: list[Issue] = []
    file_lines = _line_count(path)
    max_file_lines = thresholds["max_file_lines"]
    if file_lines > max_file_lines:
        issues.append(
            Issue(
                "long_files",
                f"long_file:{relative}",
                relative,
                {"lines": file_lines, "limit": max_file_lines},
            )
        )
    if path.suffix != ".py":
        return issues
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError as exc:
        return [
            Issue(
                "syntax_errors",
                f"syntax_error:{relative}:{exc.lineno}:{exc.offset}",
                relative,
                {"line": exc.lineno, "offset": exc.offset, "message": exc.msg},
            )
        ]
    visitor = FunctionVisitor(repo_root, path, thresholds)
    visitor.visit(tree)
    issues.extend(visitor.issues)
    return issues


def collect_issues(repo_root: Path, config: dict[str, Any]) -> dict[str, list[Issue]]:
    thresholds = config["thresholds"]
    patterns = _compile_suppression_patterns(config)
    issues: dict[str, list[Issue]] = {
        "long_files": [],
        "long_functions": [],
        "deep_nesting": [],
        "inline_suppressions": [],
        "syntax_errors": [],
    }
    for path in _iter_source_files(repo_root, config):
        for issue in _analyze_file(repo_root, path, thresholds):
            issues[issue.category].append(issue)
        issues["inline_suppressions"].extend(_find_inline_suppressions(repo_root, path, patterns))
    return {category: sorted(values, key=lambda issue: issue.identifier) for category, values in issues.items()}


def build_snapshot(repo_root: Path, config: dict[str, Any]) -> dict[str, Any]:
    issues = collect_issues(repo_root, config)
    return {
        "version": 1,
        "thresholds": config["thresholds"],
        "issues": {
            category: [issue.to_json() for issue in values]
            for category, values in sorted(issues.items())
        },
        "issue_counts": {
            category: len(values)
            for category, values in sorted(issues.items())
        },
    }


def _issue_ids(snapshot: dict[str, Any], category: str) -> set[str]:
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
            location = f"{issue['path']}:{issue.get('line', 1)}"
            detail = issue.get("function") or issue.get("pattern") or issue.get("lines") or issue.get("depth")
            print(f"    {location} {detail}")
        if len(issues) > 20:
            print(f"    ... {len(issues) - 20} more")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--baseline", default=DEFAULT_BASELINE)
    parser.add_argument("--write-baseline", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    repo_root = args.repo_root.resolve()
    config_path = repo_root / args.config
    baseline_path = repo_root / args.baseline
    config = _load_json(config_path)
    snapshot = build_snapshot(repo_root, config)
    _print_summary(snapshot)
    if args.write_baseline:
        baseline_path.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"Wrote baseline: {baseline_path.relative_to(repo_root)}")
        return 0
    if not baseline_path.exists():
        print(f"Baseline is missing: {baseline_path.relative_to(repo_root)}", file=sys.stderr)
        return 2
    baseline = _load_json(baseline_path)
    if baseline.get("thresholds") != snapshot.get("thresholds"):
        print("Readability thresholds changed; regenerate the baseline intentionally.", file=sys.stderr)
        return 2
    new_by_category = _new_issues(snapshot, baseline)
    if new_by_category:
        _print_new_issues(new_by_category)
        return 1
    print("No new readability debt relative to baseline.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
