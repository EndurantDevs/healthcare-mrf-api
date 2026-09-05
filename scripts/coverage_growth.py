"""Measure coverage of executable product lines changed by a pull request."""

from __future__ import annotations

import ast
import json
import re
import subprocess
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any

from coverage_reports import (
    CoverageRatchetError,
    _is_path_in_scope,
    _read_json,
    _relative_report_path,
)

COVERAGE_EXCLUSION_MARKERS = (
    "c8ignore",
    "coverage(off)",
    "istanbulignore",
    "node:coverageignore",
    "pragma:nobranch",
    "pragma:nocover",
)
_HUNK_HEADER = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@")


def _path_from_diff_header(raw_path: str) -> str | None:
    """Decode one Git patch path without losing spaces or quoted UTF-8."""

    if raw_path.startswith('"'):
        try:
            decoded_path = ast.literal_eval(raw_path.rstrip("\t"))
        except (SyntaxError, ValueError) as exc:
            raise CoverageRatchetError("Git diff contains a malformed quoted path") from exc
        if not isinstance(decoded_path, str):
            raise CoverageRatchetError("Git diff contains a non-string path")
        try:
            decoded_path = decoded_path.encode("latin-1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            decoded_path = str(decoded_path)
    else:
        decoded_path = raw_path.split("\t", 1)[0]
    if decoded_path == "/dev/null":
        return None
    if not decoded_path.startswith("b/"):
        raise CoverageRatchetError("Git diff new path does not start with b/")
    return decoded_path[2:]


def load_diff_threshold(
    report_name: str,
    report_config_by_field: dict[str, Any],
) -> int:
    """Validate the zero-paydown policy and return its diff threshold."""

    policy = report_config_by_field.get("growth")
    if not isinstance(policy, dict):
        raise CoverageRatchetError(f"{report_name}: coverage growth policy is missing")
    if policy.get("debt_reduction_percent") != 0:
        raise CoverageRatchetError(f"{report_name}: debt_reduction_percent must be 0")
    threshold = policy.get("diff_coverage_percent")
    if type(threshold) is not int or not 1 <= threshold <= 100:
        raise CoverageRatchetError(
            f"{report_name}: diff_coverage_percent must be an integer from 1 to 100"
        )
    diff_exclude = policy.get("diff_exclude", [])
    if not isinstance(diff_exclude, list) or not all(
        isinstance(pattern, str) and pattern for pattern in diff_exclude
    ):
        raise CoverageRatchetError(
            f"{report_name}: diff_exclude must be a string list"
        )
    return threshold


def compare_diff_policy(
    report_name: str,
    candidate_report_by_field: dict[str, Any],
    reference_report_by_field: dict[str, Any],
) -> list[str]:
    """Reject lowering an established diff-coverage threshold."""

    current = load_diff_threshold(report_name, candidate_report_by_field)
    previous_policy = reference_report_by_field.get("growth")
    if not isinstance(previous_policy, dict):
        return []
    previous = previous_policy.get("diff_coverage_percent")
    if previous is None:
        return []
    if type(previous) is not int or not 1 <= previous <= 100:
        return [f"{report_name}: base diff coverage policy is malformed"]
    if current < previous:
        return [f"{report_name}: diff coverage threshold was lowered"]
    candidate_excludes = set(candidate_report_by_field["growth"].get("diff_exclude", []))
    reference_excludes = set(previous_policy.get("diff_exclude", []))
    if not candidate_excludes.issubset(reference_excludes):
        return [f"{report_name}: diff coverage exclusions were expanded"]
    return []


def changed_lines_from_diff(diff_text: str) -> dict[str, set[int]]:
    """Return new-side line numbers from a unified-zero Git diff."""

    changed_by_path: dict[str, set[int]] = {}
    current_path: str | None = None
    current_line: int | None = None
    for raw_line in diff_text.splitlines():
        if raw_line.startswith("diff --git "):
            current_path = None
            current_line = None
            continue
        if current_line is None and raw_line.startswith("+++ "):
            current_path = _path_from_diff_header(raw_line[4:])
            continue
        hunk_header = _HUNK_HEADER.match(raw_line)
        if hunk_header:
            current_line = int(hunk_header.group(1))
            continue
        if current_line is None:
            continue
        if raw_line.startswith("+"):
            if current_path is not None:
                changed_by_path.setdefault(current_path, set()).add(current_line)
            current_line += 1
            continue
        if not raw_line.startswith(("-", "\\")):
            current_line += 1
    return changed_by_path


def _git_diff(root: Path, base_revision: str) -> str:
    command_parts = [
        "git",
        "-c",
        "core.quotePath=false",
        "diff",
        "--unified=0",
        "--no-color",
        "--no-ext-diff",
        "--no-renames",
        f"{base_revision}...HEAD",
        "--",
    ]
    try:
        return subprocess.run(
            command_parts,
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError) as exc:
        raise CoverageRatchetError(
            f"could not inspect changed source lines from {base_revision}"
        ) from exc


def _line_set(raw_lines: Any, label: str) -> set[int]:
    if not isinstance(raw_lines, list) or not all(
        type(line) is int and line > 0 for line in raw_lines
    ):
        raise CoverageRatchetError(f"{label} must be a list of positive line numbers")
    return set(raw_lines)


def _coveragepy_line_sets(payload: dict[str, Any], label: str) -> tuple[set[int], set[int]]:
    executed = _line_set(payload.get("executed_lines"), f"{label} executed_lines")
    missing = _line_set(payload.get("missing_lines"), f"{label} missing_lines")
    if executed & missing:
        raise CoverageRatchetError(f"{label} coverage line sets overlap")
    return executed | missing, executed


def _llvm_segment(raw_segment: Any, label: str) -> tuple[int, int, int, bool, bool]:
    """Validate the stable six-field llvm-cov export segment."""

    if not isinstance(raw_segment, list) or len(raw_segment) != 6:
        raise CoverageRatchetError(f"{label} LLVM segment must contain six fields")
    line, column, count, has_count, is_region_entry, is_gap = raw_segment
    if (
        type(line) is not int
        or line <= 0
        or type(column) is not int
        or column <= 0
        or type(count) is not int
        or count < 0
        or type(has_count) is not bool
        or type(is_region_entry) is not bool
        or type(is_gap) is not bool
    ):
        raise CoverageRatchetError(f"{label} LLVM segment is malformed")
    return line, column, count, has_count, is_gap


def _llvm_line_sets(raw_segments: Any, label: str) -> tuple[set[int], set[int]]:
    """Expand llvm-cov's half-open coverage segments into line sets."""

    if not isinstance(raw_segments, list):
        raise CoverageRatchetError(f"{label} LLVM segments must be a list")
    segments = [
        _llvm_segment(raw_segment, f"{label}[{index}]")
        for index, raw_segment in enumerate(raw_segments)
    ]
    executable: set[int] = set()
    covered: set[int] = set()
    for index, segment in enumerate(segments):
        line, column, count, has_count, is_gap = segment
        if index + 1 == len(segments):
            if has_count and not is_gap:
                raise CoverageRatchetError(f"{label} LLVM segments are unterminated")
            continue
        next_line, next_column, *_ = segments[index + 1]
        if (next_line, next_column) < (line, column):
            raise CoverageRatchetError(f"{label} LLVM segments are not ordered")
        if not has_count or is_gap or (next_line, next_column) == (line, column):
            continue
        final_line = next_line - (next_column == 1)
        if final_line < line:
            continue
        lines = set(range(line, final_line + 1))
        executable.update(lines)
        if count > 0:
            covered.update(lines)
    return executable, covered


def _coveragepy_files(
    root: Path,
    report_config: dict[str, Any],
) -> dict[str, tuple[set[int], set[int]]]:
    report_path = report_config.get("path")
    if not isinstance(report_path, str) or not report_path:
        raise CoverageRatchetError("coverage report path is required")
    raw_files = _read_json(root / report_path).get("files")
    if not isinstance(raw_files, dict):
        raise CoverageRatchetError("coverage.py report has no files object")
    coverage_by_path: dict[str, tuple[set[int], set[int]]] = {}
    for raw_path, payload in raw_files.items():
        if not isinstance(raw_path, str) or not isinstance(payload, dict):
            raise CoverageRatchetError("coverage.py file record is malformed")
        relative_path = _relative_report_path(root, raw_path)
        if relative_path is None or not _is_path_in_scope(relative_path, report_config):
            continue
        if relative_path in coverage_by_path:
            raise CoverageRatchetError(f"duplicate coverage.py file: {relative_path}")
        coverage_by_path[relative_path] = _coveragepy_line_sets(
            payload, relative_path
        )
    return coverage_by_path


def _llvm_files(
    root: Path,
    report_config: dict[str, Any],
) -> dict[str, tuple[set[int], set[int]]]:
    report_path = report_config.get("path")
    if not isinstance(report_path, str) or not report_path:
        raise CoverageRatchetError("coverage report path is required")
    report_runs = _read_json(root / report_path).get("data")
    if not isinstance(report_runs, list) or not report_runs:
        raise CoverageRatchetError("LLVM report has no data")
    coverage_by_path: dict[str, tuple[set[int], set[int]]] = {}
    for run_index, report_run in enumerate(report_runs):
        if not isinstance(report_run, dict) or not isinstance(
            report_run.get("files"), list
        ):
            raise CoverageRatchetError("LLVM report files are malformed")
        for file_index, file_record in enumerate(report_run["files"]):
            if not isinstance(file_record, dict) or not isinstance(
                file_record.get("filename"), str
            ):
                raise CoverageRatchetError("LLVM file record is malformed")
            relative_path = _relative_report_path(root, file_record["filename"])
            if relative_path is None or not _is_path_in_scope(
                relative_path, report_config
            ):
                continue
            if relative_path in coverage_by_path:
                raise CoverageRatchetError(f"duplicate LLVM file: {relative_path}")
            coverage_by_path[relative_path] = _llvm_line_sets(
                file_record.get("segments"),
                f"LLVM data[{run_index}].files[{file_index}].segments",
            )
    return coverage_by_path


def _coverage_files(
    root: Path,
    report_config: dict[str, Any],
) -> dict[str, tuple[set[int], set[int]]]:
    report_format = report_config.get("format")
    if report_format == "coverage.py":
        return _coveragepy_files(root, report_config)
    if report_format == "llvm-cov":
        return _llvm_files(root, report_config)
    raise CoverageRatchetError(
        f"diff coverage does not support report format {report_format!r}"
    )


def _report_diff_coverage(
    root: Path,
    report_name: str,
    report_config: dict[str, Any],
    changed_by_path: dict[str, set[int]],
) -> dict[str, Any]:
    threshold = load_diff_threshold(report_name, report_config)
    report_format = report_config.get("format")
    coverage_by_path = _coverage_files(root, report_config)
    covered_locations: set[str] = set()
    uncovered_locations: set[str] = set()
    changed_count = 0
    for relative_path, changed_lines in changed_by_path.items():
        if not _is_path_in_scope(relative_path, report_config):
            continue
        if report_format == "llvm-cov":
            # cargo-llvm-cov 0.8.7 omits these test paths from its report by default.
            path = PurePosixPath(relative_path)
            if (
                any(part in {"tests", "examples", "benches"} for part in path.parts)
                or path.name == "tests.rs"
                or path.name.endswith(("_tests.rs", "-tests.rs"))
            ):
                continue
        diff_exclude = report_config["growth"].get("diff_exclude", [])
        if any(PurePosixPath(relative_path).full_match(pattern) for pattern in diff_exclude):
            continue
        changed_count += len(changed_lines)
        line_sets = coverage_by_path.get(relative_path)
        if line_sets is None:
            raise CoverageRatchetError(
                f"{report_name}: changed source file is absent from coverage: "
                f"{relative_path}"
            )
        executable, covered = line_sets
        covered_locations.update(
            f"{relative_path}:{line}" for line in changed_lines & covered
        )
        uncovered_locations.update(
            f"{relative_path}:{line}"
            for line in changed_lines & (executable - covered)
        )
    total = len(covered_locations) + len(uncovered_locations)
    return {
        "changed": changed_count,
        "covered": len(covered_locations),
        "total": total,
        "percent": 100.0 if not total else 100.0 * len(covered_locations) / total,
        "threshold": threshold,
        "uncovered_lines": sorted(uncovered_locations),
    }


def _diff_coverage_errors(report_name: str, result: dict[str, Any]) -> list[str]:
    if result["covered"] * 100 >= result["threshold"] * result["total"]:
        return []
    errors = [
        f"{report_name}: diff coverage {result['percent']:.2f}% is below "
        f"{result['threshold']}% ({result['covered']}/{result['total']})"
    ]
    errors.extend(
        f"{report_name}: uncovered changed line {location}"
        for location in result["uncovered_lines"]
    )
    return errors


def collect_diff_coverage(
    root: Path,
    base_revision: str,
    baseline_by_field: dict[str, Any],
    selected_report_names: list[str],
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Measure executable changed lines and return all policy errors."""

    diff_text = _git_diff(root, base_revision)
    changed_by_path = changed_lines_from_diff(diff_text)
    result_by_report: dict[str, dict[str, Any]] = {}
    errors = find_added_exclusion_directives_in_diff(
        diff_text,
        baseline_by_field,
        selected_report_names,
    )
    for report_name in selected_report_names:
        report_config = baseline_by_field["reports"].get(report_name)
        if not isinstance(report_config, dict):
            raise CoverageRatchetError(f"unknown baseline report: {report_name}")
        result = _report_diff_coverage(
            root,
            report_name,
            report_config,
            changed_by_path,
        )
        result_by_report[report_name] = result
        errors.extend(_diff_coverage_errors(report_name, result))
    return result_by_report, errors


def find_added_exclusion_directives_in_diff(
    diff_text: str,
    baseline_by_field: dict[str, Any],
    selected_report_names: list[str],
) -> list[str]:
    """Return new in-scope source directives that suppress coverage."""

    errors: list[str] = []
    current_path: str | None = None
    is_in_hunk = False
    for raw_line in diff_text.splitlines():
        if raw_line.startswith("diff --git "):
            current_path = None
            is_in_hunk = False
            continue
        if not is_in_hunk and raw_line.startswith("+++ "):
            current_path = _path_from_diff_header(raw_line[4:])
            continue
        if raw_line.startswith("@@ "):
            is_in_hunk = True
            continue
        if not current_path or not is_in_hunk or not raw_line.startswith("+"):
            continue
        compact_line = "".join(raw_line[1:].lower().split())
        if not any(marker in compact_line for marker in COVERAGE_EXCLUSION_MARKERS):
            continue
        for report_name in selected_report_names:
            report_config = baseline_by_field["reports"][report_name]
            if _is_path_in_scope(current_path, report_config):
                errors.append(
                    f"{report_name}: new coverage exclusion directive in {current_path}"
                )
    return errors


def find_added_exclusion_directives(
    root: Path,
    base_revision: str,
    baseline_by_field: dict[str, Any],
    selected_report_names: list[str],
) -> list[str]:
    """Inspect Git for newly added in-scope coverage suppressions."""

    return find_added_exclusion_directives_in_diff(
        _git_diff(root, base_revision),
        baseline_by_field,
        selected_report_names,
    )


def build_diff_policy_test_baseline() -> dict[str, Any]:
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
                    "debt_reduction_percent": 0,
                    "diff_coverage_percent": 85,
                },
            }
        }
    }


def _assert_unusual_diff_paths() -> None:
    unusual_path_diff = (
        'diff --git "a/caf\\303\\251.py" "b/caf\\303\\251.py"\n'
        '+++ "b/caf\\303\\251.py"\n'
        "@@ -0,0 +1 @@\n"
        "+covered = 1\n"
        "diff --git a/space name.py b/space name.py\n"
        "+++ b/space name.py\t\n"
        "@@ -0,0 +1 @@\n"
        "+covered = 1\n"
    )
    if changed_lines_from_diff(unusual_path_diff) != {
        "café.py": {1},
        "space name.py": {1},
    }:
        raise CoverageRatchetError(
            "coverage ratchet self-test failed: quoted Git paths"
        )


def run_diff_helper_self_test() -> None:
    """Exercise parsing, executable-line selection, and language boundaries."""

    added_lines = "\n".join(f"+line {line}" for line in range(1, 22))
    diff_text = (
        "diff --git a/sample.py b/sample.py\n"
        "+++ b/sample.py\n"
        "@@ -0,0 +1,21 @@\n"
        f"{added_lines}\n"
    )
    changed = changed_lines_from_diff(diff_text)
    if changed != {"sample.py": set(range(1, 22))}:
        raise CoverageRatchetError(
            "coverage ratchet self-test failed: changed-line parser"
        )
    _assert_unusual_diff_paths()
    with tempfile.TemporaryDirectory() as raw_directory:
        root = Path(raw_directory)
        (root / "coverage.json").write_text(
            json.dumps(
                {
                    "files": {
                        "sample.py": {
                            "executed_lines": list(range(1, 18)),
                            "missing_lines": [18, 19, 20],
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        passing = _report_diff_coverage(
            root,
            "python",
            build_diff_policy_test_baseline()["reports"]["python"],
            changed,
        )
    failing = {
        "covered": 16,
        "total": 20,
        "percent": 80.0,
        "threshold": 85,
        "uncovered_lines": ["sample.py:17"],
    }
    if passing["changed"] != 21 or passing["total"] != 20:
        raise CoverageRatchetError(
            "coverage ratchet self-test failed: executable changed lines"
        )
    if _diff_coverage_errors("python", passing) or not _diff_coverage_errors(
        "python", failing
    ):
        raise CoverageRatchetError(
            "coverage ratchet self-test failed: Python diff coverage boundary"
        )
    rust_executable, rust_covered = _llvm_line_sets(
        [[1, 1, 1, True, True, False], [5, 1, 0, False, False, False]],
        "rust",
    )
    if rust_executable != {1, 2, 3, 4} or rust_covered != rust_executable:
        raise CoverageRatchetError(
            "coverage ratchet self-test failed: LLVM segment expansion"
        )
    rust_boundary = {
        "covered": 16,
        "total": 20,
        "percent": 80.0,
        "threshold": 80,
        "uncovered_lines": [],
    }
    if _diff_coverage_errors("rust", rust_boundary):
        raise CoverageRatchetError(
            "coverage ratchet self-test failed: Rust diff coverage boundary"
        )
    pure_deletion = (
        "diff --git a/sample.py b/sample.py\n"
        "+++ b/sample.py\n"
        "@@ -1 +0,0 @@\n"
        "-old\n"
    )
    if changed_lines_from_diff(pure_deletion):
        raise CoverageRatchetError(
            "coverage ratchet self-test failed: pure deletion exemption"
        )


def run_exclusion_guard_self_test() -> None:
    """Exercise rejection of newly added coverage suppressions."""

    errors = find_added_exclusion_directives_in_diff(
        "diff --git a/sample.py b/sample.py\n"
        "+++ b/sample.py\n"
        "@@ -0,0 +1 @@\n"
        "+value = fallback()  # pragma: no cover\n"
        "diff --git a/docs/guide.md b/docs/guide.md\n"
        "+++ b/docs/guide.md\n"
        "@@ -0,0 +1 @@\n"
        "+document c8 ignore behavior\n",
        build_diff_policy_test_baseline(),
        ["python"],
    )
    if errors != ["python: new coverage exclusion directive in sample.py"]:
        raise CoverageRatchetError(
            "coverage ratchet self-test failed: exclusion directive guard"
        )
