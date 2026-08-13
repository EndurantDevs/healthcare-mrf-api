"""Coverage artifacts remain portable across hosted and container roots."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys


def _config_text() -> str:
    return (
        "[run]\nbranch = True\nrelative_files = True\nsource = .\n\n"
        "[report]\ninclude = main.py\n"
    )


def _run_coverage(root: Path, arguments: list[str]) -> None:
    subprocess.run(
        [sys.executable, "-m", "coverage", *arguments],
        cwd=root,
        check=True,
    )


def _write_producer(root: Path) -> Path:
    root.mkdir()
    (root / "main.py").write_text("value = 1\n", encoding="utf-8")
    config_path = root / "coverage.ini"
    config_path.write_text(_config_text(), encoding="utf-8")
    data_path = root / ".coverage"
    _run_coverage(
        root,
        [
            "run",
            f"--rcfile={config_path}",
            f"--data-file={data_path}",
            "main.py",
        ],
    )
    return data_path


def _combined_report(root: Path, producer_paths: list[Path]) -> dict:
    root.mkdir()
    (root / "main.py").write_text("value = 1\n", encoding="utf-8")
    config_path = root / "coverage.ini"
    config_path.write_text(_config_text(), encoding="utf-8")
    data_path = root / ".coverage"
    report_path = root / "coverage.json"
    _run_coverage(
        root,
        ["combine", "--data-file", str(data_path), *map(str, producer_paths)],
    )
    _run_coverage(
        root,
        [
            "json",
            "--data-file",
            str(data_path),
            f"--rcfile={config_path}",
            "-o",
            str(report_path),
        ],
    )
    return json.loads(report_path.read_text(encoding="utf-8"))


def test_relative_coverage_combines_different_runner_roots(tmp_path: Path) -> None:
    repository_config = Path(__file__).resolve().parents[1] / "test-coverage.ini"
    assert "relative_files = True" in repository_config.read_text(encoding="utf-8")
    producer_paths = [
        _write_producer(tmp_path / "hosted"),
        _write_producer(tmp_path / "container"),
    ]

    report = _combined_report(tmp_path / "aggregate", producer_paths)

    assert set(report["files"]) == {"main.py"}


def test_public_evidence_coverage_scope_is_ratcheted() -> None:
    repository_root = Path(__file__).resolve().parents[1]
    coverage_config = (repository_root / "test-coverage.ini").read_text(
        encoding="utf-8"
    )
    baseline = json.loads(
        (repository_root / "test-coverage-baseline.json").read_text(encoding="utf-8")
    )
    python_scope = baseline["reports"]["python"]["scope"]
    workflow = (repository_root / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )
    prepush = (repository_root / "scripts" / "ci" / "prepush").read_text(
        encoding="utf-8"
    )

    assert "    public_evidence\n" in coverage_config
    assert "    public_evidence/*.py\n" in coverage_config
    assert "    public_evidence/**/*.py\n" in coverage_config
    assert "public_evidence" in python_scope["policy"]["source_dirs"]
    assert "public_evidence/*.py" in python_scope["include"]
    assert "public_evidence/**/*.py" in python_scope["include"]
    assert workflow.count("--cov=public_evidence") == 3
    assert "run: scripts/ci/prepush quality" in workflow
    assert "compileall api process db public_evidence scripts main.py" in prepush
