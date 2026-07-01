"""Source file discovery and per-file readability checks."""

from __future__ import annotations

import ast
import hashlib
import re
from pathlib import Path
from typing import Any

from .config import (DEFAULT_COMMENT_NOISE_PATTERNS, compile_suppression_patterns, is_matching_path_pattern,
                     readability_options, threshold)
from .function_visitor import FunctionVisitor
from .model import DEFAULT_ISSUE_CATEGORIES, Issue


def collect_issues(repo_root: Path, config: dict[str, Any]) -> dict[str, list[Issue]]:
    """Collect readability findings grouped by rule category."""
    patterns = compile_suppression_patterns(config)
    issues_by_category: dict[str, list[Issue]] = {category: [] for category in DEFAULT_ISSUE_CATEGORIES}
    for path in _iter_source_files(repo_root, config):
        for issue in _analyze_file(repo_root, path, config):
            issues_by_category[issue.category].append(issue)
        issues_by_category["inline_suppressions"].extend(_find_inline_suppressions(repo_root, path, patterns))
        if path.suffix == ".py":
            issues_by_category["comment_noise"].extend(_find_comment_noise(repo_root, path, config))
    return {category: sorted(values, key=lambda issue: issue.identifier) for category, values in issues_by_category.items()}


def _iter_source_files(repo_root: Path, config: dict[str, Any]) -> list[Path]:
    roots = config.get("source_roots", [])
    exclude_globs = config.get("exclude_globs", [])
    include_suffixes = tuple(config.get("include_suffixes", [".py"]))
    files: list[Path] = []
    for source_root in roots:
        root_path = repo_root / source_root
        candidates = _candidate_files(root_path)
        for path in candidates:
            relative = path.relative_to(repo_root).as_posix()
            if not path.name.endswith(include_suffixes):
                continue
            if is_matching_path_pattern(relative, exclude_globs):
                continue
            files.append(path)
    return sorted(set(files))


def _candidate_files(root_path: Path) -> list[Path]:
    if root_path.is_file():
        return [root_path]
    if root_path.is_dir():
        return [path for path in root_path.rglob("*") if path.is_file()]
    return []


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


def _find_comment_noise(repo_root: Path, path: Path, config: dict[str, Any]) -> list[Issue]:
    relative = path.relative_to(repo_root).as_posix()
    patterns = [
        re.compile(pattern, re.IGNORECASE)
        for pattern in readability_options(config).get("comment_noise_patterns", DEFAULT_COMMENT_NOISE_PATTERNS)
    ]
    issues: list[Issue] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            issue = _comment_noise_issue(relative, line_number, line, patterns)
            if issue:
                issues.append(issue)
    return issues


def _comment_noise_issue(
    relative: str,
    line_number: int,
    line: str,
    patterns: list[re.Pattern[str]],
) -> Issue | None:
    stripped = line.strip()
    if not stripped.startswith("#"):
        return None
    comment = stripped.lstrip("#").strip()
    if not comment or comment.startswith(("!", "-", "Licensed", "Copyright")):
        return None
    if not any(pattern.search(comment) for pattern in patterns):
        return None
    digest = hashlib.sha1(f"{relative}:{line_number}:{comment}".encode("utf-8")).hexdigest()[:12]
    return Issue(
        "comment_noise",
        f"comment_noise:{relative}:{digest}",
        relative,
        {"line": line_number, "text": comment},
    )


def _analyze_file(repo_root: Path, path: Path, config: dict[str, Any]) -> list[Issue]:
    relative = path.relative_to(repo_root).as_posix()
    issues = _file_size_issues(relative, path, config)
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
    visitor = FunctionVisitor(repo_root, path, config)
    visitor.visit(tree)
    issues.extend(visitor.issues)
    return issues


def _file_size_issues(relative: str, path: Path, config: dict[str, Any]) -> list[Issue]:
    file_lines = _line_count(path)
    max_file_lines = threshold(config, "max_file_lines", 500)
    if file_lines <= max_file_lines:
        return []
    return [
        Issue(
            "long_files",
            f"long_file:{relative}",
            relative,
            {"lines": file_lines, "limit": max_file_lines},
        )
    ]
