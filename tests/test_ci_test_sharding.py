"""Contracts for deterministic CI pytest sharding."""

from __future__ import annotations

import hashlib
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
    command = SHARDER.collection_command(["--ignore", "tests/capacity.py"])

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

    assigned_nodeids = [nodeid for output in outputs for nodeid in output.read_text(encoding="utf-8").splitlines()]
    assert sorted(assigned_nodeids) == [
        "tests/test_sample.py::test_one",
        "tests/test_sample.py::test_two",
    ]


def test_registry_cost_swap_preserves_all_other_assignments_and_exact_once() -> None:
    """Move one expensive proof while making room without changing other work."""
    publication = (
        "tests/test_network_cms_registry_complete_publication_postgres.py::test_complete_publication_and_retained_pair"
    )
    swapped_files = (
        "tests/test_network_registry_cms_prepared_pair_postgres.py",
        "tests/test_cms_publication_source_session_postgres.py",
    )
    nodeids = [publication, "tests/test_unrelated.py::test_one"]
    nodeids.extend(f"{path}::test_case[{index}]" for path in swapped_files for index in range(100))
    shards = [SHARDER.select_nodeids(nodeids, shard_count=8, shard_index=index) for index in range(8)]
    assert publication in shards[6]
    assert sorted(node for shard in shards for node in shard) == sorted(nodeids)
    for node in nodeids:
        legacy = int.from_bytes(hashlib.sha256(node.encode()).digest(), byteorder="big") % 8
        expected = (
            6 if node == publication else (5 if legacy == 6 and node.split("::", 1)[0] in swapped_files else legacy)
        )
        assert SHARDER.shard_index_for_nodeid(node, 8) == expected
        for count in (1, 2, 4):
            assert SHARDER.shard_index_for_nodeid(node, count) == (
                int.from_bytes(hashlib.sha256(node.encode()).digest(), byteorder="big") % count
            )
