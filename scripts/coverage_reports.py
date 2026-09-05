"""Normalize scoped coverage reports for the test-coverage ratchet."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Callable

Metric = dict[str, int]
MetricByName = dict[str, Metric]


class CoverageRatchetError(ValueError):
    """Raised when a coverage report or baseline is malformed."""


@dataclass(frozen=True)
class _ReportSnapshot:
    """Normalized metrics and source files from one coverage report."""

    metric_by_name: MetricByName
    files: frozenset[str]


def _read_json(path: Path) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise CoverageRatchetError(f"missing coverage file: {path}") from exc
    except json.JSONDecodeError as exc:
        raise CoverageRatchetError(f"invalid JSON in {path}: {exc}") from exc
    if not isinstance(document, dict):
        raise CoverageRatchetError(f"expected a JSON object in {path}")
    return document


def _metric(covered: Any, total: Any, label: str) -> Metric:
    if not isinstance(covered, int) or not isinstance(total, int):
        raise CoverageRatchetError(f"{label} counts must be integers")
    if total <= 0 or covered < 0 or covered > total:
        raise CoverageRatchetError(
            f"{label} counts are invalid: covered={covered}, total={total}"
        )
    return {"covered": covered, "total": total}


def _scope_patterns(config: dict[str, Any]) -> tuple[list[str], list[str]]:
    scope = config.get("scope")
    if not isinstance(scope, dict):
        raise CoverageRatchetError("report scope must be an object")
    include_patterns = scope.get("include")
    exclude_patterns = scope.get("exclude", [])
    if (
        not isinstance(include_patterns, list)
        or not include_patterns
        or not all(
            isinstance(pattern, str) and pattern for pattern in include_patterns
        )
    ):
        raise CoverageRatchetError("report scope include must be a non-empty string list")
    if not isinstance(exclude_patterns, list) or not all(
        isinstance(pattern, str) and pattern for pattern in exclude_patterns
    ):
        raise CoverageRatchetError("report scope exclude must be a string list")
    return include_patterns, exclude_patterns


def _is_path_in_scope(relative_path: str, config: dict[str, Any]) -> bool:
    include_patterns, exclude_patterns = _scope_patterns(config)
    candidate = PurePosixPath(relative_path)
    is_included = any(candidate.full_match(pattern) for pattern in include_patterns)
    is_excluded = any(candidate.full_match(pattern) for pattern in exclude_patterns)
    return is_included and not is_excluded


def _relative_report_path(root: Path, raw_path: str) -> str | None:
    candidate = Path(raw_path)
    if candidate.is_absolute():
        try:
            candidate = candidate.resolve().relative_to(root.resolve())
        except ValueError:
            return None
    return candidate.as_posix().removeprefix("./")


def _discover_scope_files(root: Path, config: dict[str, Any]) -> frozenset[str]:
    include_patterns, _ = _scope_patterns(config)
    files: set[str] = set()
    for pattern in include_patterns:
        for candidate in root.glob(pattern):
            if candidate.is_file():
                relative_path = candidate.relative_to(root).as_posix()
                if _is_path_in_scope(relative_path, config):
                    files.add(relative_path)
    if not files:
        raise CoverageRatchetError("report scope matches no repository files")
    return frozenset(files)


def _coveragepy_snapshot(
    report_document: dict[str, Any],
    root: Path,
    config: dict[str, Any],
) -> _ReportSnapshot:
    raw_summary_by_file = report_document.get("files")
    if not isinstance(raw_summary_by_file, dict):
        raise CoverageRatchetError("coverage.py report has no files object")
    summary_by_file: dict[str, dict[str, Any]] = {}
    for raw_path, file_payload in raw_summary_by_file.items():
        if not isinstance(raw_path, str) or not isinstance(file_payload, dict):
            raise CoverageRatchetError("coverage.py file record is malformed")
        relative_path = _relative_report_path(root, raw_path)
        if relative_path is None:
            continue
        file_summary = file_payload.get("summary")
        if _is_path_in_scope(relative_path, config) and isinstance(
            file_summary, dict
        ):
            summary_by_file[relative_path] = file_summary
    if not summary_by_file:
        raise CoverageRatchetError("coverage.py report contains no in-scope files")
    metric_by_name = {
        "lines": _metric(
            sum(
                file_summary.get("covered_lines", 0)
                for file_summary in summary_by_file.values()
            ),
            sum(
                file_summary.get("num_statements", 0)
                for file_summary in summary_by_file.values()
            ),
            "lines",
        )
    }
    branch_total = sum(
        file_summary.get("num_branches", 0)
        for file_summary in summary_by_file.values()
    )
    if branch_total:
        metric_by_name["branches"] = _metric(
            sum(
                file_summary.get("covered_branches", 0)
                for file_summary in summary_by_file.values()
            ),
            branch_total,
            "branches",
        )
    return _ReportSnapshot(metric_by_name, frozenset(summary_by_file))


def _istanbul_snapshot(
    report_document: dict[str, Any],
    root: Path,
    config: dict[str, Any],
) -> _ReportSnapshot:
    summary_by_file: dict[str, dict[str, Any]] = {}
    for raw_path, file_summary in report_document.items():
        if raw_path == "total":
            continue
        if not isinstance(raw_path, str) or not isinstance(file_summary, dict):
            raise CoverageRatchetError("Istanbul file record is malformed")
        relative_path = _relative_report_path(root, raw_path)
        if relative_path is None:
            continue
        if _is_path_in_scope(relative_path, config):
            summary_by_file[relative_path] = file_summary
    if not summary_by_file:
        raise CoverageRatchetError("Istanbul summary contains no in-scope files")
    metric_by_name: MetricByName = {}
    for metric_name in ("lines", "branches", "functions", "statements"):
        metric_summaries = [
            file_summary.get(metric_name)
            for file_summary in summary_by_file.values()
        ]
        if metric_summaries and all(
            isinstance(metric_summary, dict) for metric_summary in metric_summaries
        ):
            total = sum(
                metric_summary.get("total", 0)
                for metric_summary in metric_summaries
            )
            if total:
                metric_by_name[metric_name] = _metric(
                    sum(
                        metric_summary.get("covered", 0)
                        for metric_summary in metric_summaries
                    ),
                    total,
                    metric_name,
                )
    return _ReportSnapshot(metric_by_name, frozenset(summary_by_file))


def _llvm_snapshot(
    report_document: dict[str, Any],
    root: Path,
    config: dict[str, Any],
) -> _ReportSnapshot:
    report_runs = report_document.get("data")
    if (
        not isinstance(report_runs, list)
        or not report_runs
        or not isinstance(report_runs[0], dict)
    ):
        raise CoverageRatchetError("LLVM report has no data totals")
    raw_files = report_runs[0].get("files")
    if not isinstance(raw_files, list):
        raise CoverageRatchetError("LLVM report files are malformed")
    summary_by_file: dict[str, dict[str, Any]] = {}
    for file_record in raw_files:
        if not isinstance(file_record, dict) or not isinstance(
            file_record.get("filename"), str
        ):
            raise CoverageRatchetError("LLVM file record is malformed")
        relative_path = _relative_report_path(root, file_record["filename"])
        file_summary = file_record.get("summary")
        if (
            relative_path is not None
            and _is_path_in_scope(relative_path, config)
            and isinstance(file_summary, dict)
        ):
            summary_by_file[relative_path] = file_summary
    if not summary_by_file:
        raise CoverageRatchetError("LLVM report contains no in-scope files")
    metric_by_name: MetricByName = {}
    for metric_name in ("lines", "functions", "regions", "branches"):
        metric_summaries = [
            file_summary.get(metric_name)
            for file_summary in summary_by_file.values()
        ]
        total = sum(
            metric_summary.get("count", 0)
            for metric_summary in metric_summaries
            if isinstance(metric_summary, dict)
        )
        if total:
            metric_by_name[metric_name] = _metric(
                sum(
                    metric_summary.get("covered", 0)
                    for metric_summary in metric_summaries
                    if isinstance(metric_summary, dict)
                ),
                total,
                metric_name,
            )
    return _ReportSnapshot(metric_by_name, frozenset(summary_by_file))


Parser = Callable[[dict[str, Any], Path, dict[str, Any]], _ReportSnapshot]
PARSER_BY_FORMAT: dict[str, Parser] = {
    "coverage.py": _coveragepy_snapshot,
    "istanbul-summary": _istanbul_snapshot,
    "llvm-cov": _llvm_snapshot,
}

_DOCS_START = "<!-- coverage-baseline:start -->"
_DOCS_END = "<!-- coverage-baseline:end -->"


def _collect_report(
    root: Path,
    report_name: str,
    config: dict[str, Any],
    enforce_baseline_files: bool = True,
) -> _ReportSnapshot:
    report_format = config.get("format")
    report_path = config.get("path")
    if report_format not in PARSER_BY_FORMAT:
        raise CoverageRatchetError(
            f"{report_name}: unsupported format {report_format!r}"
        )
    if not isinstance(report_path, str) or not report_path:
        raise CoverageRatchetError(f"{report_name}: report path is required")
    snapshot = PARSER_BY_FORMAT[report_format](
        _read_json(root / report_path), root, config
    )
    baseline_files = config.get("files")
    if enforce_baseline_files and baseline_files is not None:
        if not isinstance(baseline_files, list) or not all(
            isinstance(file_path, str) for file_path in baseline_files
        ):
            raise CoverageRatchetError(f"{report_name}: baseline files are malformed")
        missing_files = sorted(set(baseline_files) - snapshot.files)
        if missing_files:
            raise CoverageRatchetError(
                f"{report_name}: report dropped in-scope files: "
                + ", ".join(missing_files)
            )
    if report_format != "llvm-cov":
        missing_files = sorted(_discover_scope_files(root, config) - snapshot.files)
        if missing_files:
            raise CoverageRatchetError(
                f"{report_name}: coverage command omitted source files: "
                + ", ".join(missing_files)
            )
    return snapshot


def _baseline_docs(baseline: dict[str, Any]) -> str:
    """Render the tracked current-baseline tables deterministically."""

    reports_by_name = baseline.get("reports")
    if not isinstance(reports_by_name, dict) or not reports_by_name:
        raise CoverageRatchetError("coverage baseline has no reports")
    lines = [_DOCS_START]
    for report_name, config in reports_by_name.items():
        if not isinstance(config, dict):
            raise CoverageRatchetError(f"{report_name}: baseline report is malformed")
        metrics_by_name = config.get("metrics")
        if not isinstance(metrics_by_name, dict) or not metrics_by_name:
            raise CoverageRatchetError(f"{report_name}: baseline metrics are missing")
        if len(reports_by_name) > 1:
            lines.extend((f"### {report_name.replace('_', ' ').title()}", ""))
        lines.extend(("| Metric | Covered / total | Coverage |", "| --- | ---: | ---: |"))
        for metric_name, raw_metric in metrics_by_name.items():
            if not isinstance(raw_metric, dict):
                raise CoverageRatchetError(
                    f"{report_name}.{metric_name}: baseline metric is malformed"
                )
            metric = _metric(
                raw_metric.get("covered"),
                raw_metric.get("total"),
                f"{report_name}.{metric_name}",
            )
            lines.append(
                f"| {metric_name.replace('_', ' ').title()} | "
                f"{metric['covered']:,} / {metric['total']:,} | "
                f"{100 * metric['covered'] / metric['total']:.2f}% |"
            )
        lines.append("")
    lines.append(_DOCS_END)
    return "\n".join(lines)


def _updated_docs(document: str, baseline: dict[str, Any]) -> str:
    if document.count(_DOCS_START) != 1 or document.count(_DOCS_END) != 1:
        raise CoverageRatchetError(
            "coverage docs must contain one generated table marker pair"
        )
    if document.index(_DOCS_START) > document.index(_DOCS_END):
        raise CoverageRatchetError(
            "coverage docs generated table markers are out of order"
        )
    prefix, remainder = document.split(_DOCS_START, 1)
    _, suffix = remainder.split(_DOCS_END, 1)
    return prefix + _baseline_docs(baseline) + suffix


def check_baseline_docs(path: Path, baseline: dict[str, Any]) -> None:
    """Fail when the generated documentation tables differ from a baseline."""

    current = path.read_text(encoding="utf-8")
    if current != _updated_docs(current, baseline):
        raise CoverageRatchetError(
            f"{path} current-baseline table is stale; run coverage_reports.py --write-docs"
        )


def _run_docs_cli() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--baseline", type=Path, default=Path("test-coverage-baseline.json")
    )
    parser.add_argument("--docs", type=Path, default=Path("docs/test-coverage.md"))
    parser.add_argument("--write-docs", action="store_true")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if not args.write_docs and not args.check:
        parser.error("one of --write-docs or --check is required")
    try:
        baseline = _read_json(args.baseline)
        if args.write_docs:
            current = args.docs.read_text(encoding="utf-8")
            args.docs.write_text(_updated_docs(current, baseline), encoding="utf-8")
            print(f"Wrote the coverage baseline table to {args.docs}.")
        if args.check:
            check_baseline_docs(args.docs, baseline)
            print("Coverage documentation matches the baseline.")
    except (CoverageRatchetError, FileNotFoundError) as exc:
        print(f"ERROR: {exc}")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(_run_docs_cli())
