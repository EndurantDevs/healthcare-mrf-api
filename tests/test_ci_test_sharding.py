"""Contracts for deterministic CI pytest sharding."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPOSITORY_ROOT / "scripts" / "ci" / "shard_pytest_nodeids.py"
SPEC = importlib.util.spec_from_file_location("shard_pytest_nodeids", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
SHARDER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = SHARDER
SPEC.loader.exec_module(SHARDER)


def test_two_shards_are_sorted_disjoint_and_exact_once() -> None:
    nodeids = [
        "tests/test_alpha.py::test_one",
        "tests/test_alpha.py::test_two",
        "tests/test_beta.py::test_three",
        "tests/test_beta.py::test_four",
        "tests/test_gamma.py::test_five",
    ]

    first = SHARDER.select_nodeids(nodeids, shard_count=2, shard_index=0)
    second = SHARDER.select_nodeids(nodeids, shard_count=2, shard_index=1)

    assert first == sorted(first)
    assert second == sorted(second)
    assert set(first).isdisjoint(second)
    assert sorted((*first, *second)) == sorted(nodeids)


def test_collection_command_has_the_hard_test_process_limit() -> None:
    command = SHARDER.pytest_collection_command(["--ignore", "tests/capacity.py"])

    assert command[:3] == ["timeout", "--foreground", "295s"]
    assert command[3:7] == [sys.executable, "-m", "pytest", "--collect-only"]
    assert command[-2:] == ["--ignore", "tests/capacity.py"]


def test_cli_collects_and_assigns_each_temporary_test_once(tmp_path: Path) -> None:
    test_root = tmp_path / "tests"
    test_root.mkdir()
    (test_root / "test_sample.py").write_text(
        "def test_one():\n    assert True\n\ndef test_two():\n    assert True\n",
        encoding="utf-8",
    )
    outputs = [tmp_path / f"shard-{index}.txt" for index in range(2)]

    for index, output in enumerate(outputs):
        subprocess.run(
            [
                sys.executable,
                str(SCRIPT_PATH),
                "--shard-count",
                "2",
                "--shard-index",
                str(index),
                "--output",
                str(output),
                "--",
                str(test_root),
            ],
            check=True,
            cwd=tmp_path,
        )

    assigned_nodeids = [
        nodeid
        for output in outputs
        for nodeid in output.read_text(encoding="utf-8").splitlines()
    ]
    assert sorted(assigned_nodeids) == [
        "tests/test_sample.py::test_one",
        "tests/test_sample.py::test_two",
    ]


def test_workflow_uses_four_unique_main_coverage_artifacts_and_timeouts() -> None:
    workflow = (REPOSITORY_ROOT / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )

    assert "shard-index: [0, 1, 2, 3]" in workflow
    assert "--shard-count 4" in workflow
    assert "scripts/ci/shard_pytest_nodeids.py" in workflow
    assert "mrf-python-coverage-main-${{ matrix.shard-index }}" in workflow
    assert "pattern: mrf-python-coverage-main-*" in workflow
    assert "name: postgres18 postgis tests (${{ matrix.shard }})" in workflow
    assert "mrf-python-coverage-postgres-${{ matrix.shard }}" in workflow
    assert "pattern: mrf-python-coverage-postgres-*" in workflow
    assert "if: matrix.shard == 'core'" in workflow
    assert "if: matrix.shard == 'provider-directory'" in workflow
    assert "python -m pytest -q -n 1 --dist loadscope" in workflow
    assert (
        "cargo build --locked --bins --manifest-path "
        "support/ptg2_scanner/Cargo.toml &"
    ) in workflow
    assert "timeout --foreground 295s python -m pytest" in workflow
    assert "timeout --foreground 295s cargo llvm-cov" in workflow
    for workflow_line in workflow.splitlines():
        if "python -m pytest" in workflow_line or "cargo llvm-cov" in workflow_line:
            assert "timeout --foreground 295s" in workflow_line
