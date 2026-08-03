"""Rebuild healthcare's Python report from its fixed producer topology."""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys

from coverage_forecast_artifacts import CoverageForecastError, verify_shard_artifacts


def _run_coverage(root: Path, arguments: list[str]) -> None:
    """Run the same coverage.py subcommand used by the CI aggregation job."""

    completed = subprocess.run(
        [sys.executable, "-m", "coverage", *arguments],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode:
        detail = completed.stderr.strip() or completed.stdout.strip() or "no detail"
        raise CoverageForecastError(f"coverage {arguments[0]} failed: {detail}")


def _verified_coverage_paths(
    root: Path,
    main_artifacts: Path,
    capacity_artifacts: Path,
    postgres_artifacts: Path,
    base_sha: str,
    head_sha: str,
) -> list[Path]:
    """Return only the eight valid coverage data files required by healthcare CI."""

    return [
        *verify_shard_artifacts(root, main_artifacts, "main", base_sha, head_sha),
        *verify_shard_artifacts(
            root, capacity_artifacts, "capacity", base_sha, head_sha
        ),
        *verify_shard_artifacts(root, postgres_artifacts, "postgres", base_sha, head_sha),
    ]


def combine_python_coverage(
    root: Path,
    temporary_directory: Path,
    main_artifacts: Path,
    capacity_artifacts: Path,
    postgres_artifacts: Path,
    base_sha: str,
    head_sha: str,
) -> tuple[Path, dict[str, list[str]]]:
    """Rebuild the one Python report from all and only verified producer files."""

    coverage_paths = _verified_coverage_paths(
        root,
        main_artifacts,
        capacity_artifacts,
        postgres_artifacts,
        base_sha,
        head_sha,
    )
    data_path = temporary_directory / ".coverage"
    report_path = temporary_directory / "test-coverage-python.json"
    _run_coverage(
        root,
        ["combine", "--data-file", str(data_path), *map(str, coverage_paths)],
    )
    _run_coverage(
        root,
        [
            "json",
            "--data-file",
            str(data_path),
            "--rcfile=test-coverage.ini",
            "-o",
            str(report_path),
        ],
    )
    return report_path, {
        "main": [path.name for path in coverage_paths[:4]],
        "capacity": [coverage_paths[4].name],
        "postgres": [path.name for path in coverage_paths[5:]],
    }
