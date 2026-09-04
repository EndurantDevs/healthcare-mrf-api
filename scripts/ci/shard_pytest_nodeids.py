"""Write one deterministic exact-once pytest node-id shard."""

from __future__ import annotations

import argparse
import hashlib
import subprocess
import sys
from pathlib import Path
from typing import Sequence


TEST_PROCESS_TIMEOUT_SECONDS = "295s"


def pytest_addoption(parser) -> None:
    """Allow CI workers to select their shard during the execution collection."""

    parser.addoption("--ci-shard-count", type=int, default=4)
    parser.addoption("--ci-shard-index", type=int)


def pytest_collection_modifyitems(config, items) -> None:
    """Keep the same sorted, exact-once assignment without a collect-only run."""

    import pytest

    shard_index = config.getoption("ci_shard_index")
    if shard_index is None:
        return
    shard_count = config.getoption("ci_shard_count")
    if shard_count < 1 or not 0 <= shard_index < shard_count:
        raise pytest.UsageError("CI shard index must be in [0, positive shard count)")
    nodeids = [item.nodeid for item in items]
    if len(nodeids) != len(set(nodeids)):
        raise pytest.UsageError("pytest collection returned duplicate node IDs")
    selected_ids = set(select_nodeids(nodeids, shard_count=shard_count, shard_index=shard_index))
    if not selected_ids:
        raise pytest.UsageError("selected shard has no node IDs")
    config.hook.pytest_deselected(items=[item for item in items if item.nodeid not in selected_ids])
    items[:] = sorted(
        (item for item in items if item.nodeid in selected_ids), key=lambda item: item.nodeid
    )


def parse_arguments(arguments: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse and validate one deterministic shard request."""

    parser = argparse.ArgumentParser(
        description="Collect pytest node IDs and write one deterministic shard."
    )
    parser.add_argument("--shard-count", type=int, required=True)
    parser.add_argument("--shard-index", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("pytest_arguments", nargs=argparse.REMAINDER)
    parsed = parser.parse_args(arguments)
    if parsed.shard_count < 1:
        parser.error("--shard-count must be positive")
    if not 0 <= parsed.shard_index < parsed.shard_count:
        parser.error("--shard-index must be in [0, --shard-count)")
    if parsed.pytest_arguments[:1] == ["--"]:
        parsed.pytest_arguments = parsed.pytest_arguments[1:]
    return parsed


def collection_command(pytest_arguments: Sequence[str]) -> list[str]:
    """Build the bounded pytest collection command."""

    return [
        "timeout",
        "--foreground",
        TEST_PROCESS_TIMEOUT_SECONDS,
        sys.executable,
        "-m",
        "pytest",
        "--collect-only",
        "-q",
        *pytest_arguments,
    ]


def collect_nodeids(pytest_arguments: Sequence[str]) -> list[str]:
    """Collect unique sorted repository test node IDs."""

    completed = subprocess.run(
        collection_command(pytest_arguments),
        check=True,
        capture_output=True,
        text=True,
    )
    nodeids = sorted(
        line
        for line in completed.stdout.splitlines()
        if line.startswith("tests/") and "::" in line
    )
    if not nodeids:
        raise ValueError("pytest collection returned no node IDs")
    if len(nodeids) != len(set(nodeids)):
        raise ValueError("pytest collection returned duplicate node IDs")
    return nodeids


def shard_index_for_nodeid(nodeid: str, shard_count: int) -> int:
    """Map one node ID to a stable shard index."""

    digest = hashlib.sha256(nodeid.encode("utf-8")).digest()
    return int.from_bytes(digest, byteorder="big") % shard_count


def select_nodeids(
    nodeids: Sequence[str],
    *,
    shard_count: int,
    shard_index: int,
) -> list[str]:
    """Return the sorted node IDs assigned to one shard."""

    return [
        nodeid
        for nodeid in sorted(nodeids)
        if shard_index_for_nodeid(nodeid, shard_count) == shard_index
    ]


def write_nodeids(output: Path, nodeids: Sequence[str]) -> None:
    """Write one pytest response file with one node ID per line."""

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("".join(f"{nodeid}\n" for nodeid in nodeids), encoding="utf-8")


def main(arguments: Sequence[str] | None = None) -> int:
    """Collect, select, and write one requested shard."""

    parsed = parse_arguments(arguments)
    nodeids = collect_nodeids(parsed.pytest_arguments)
    selected = select_nodeids(
        nodeids,
        shard_count=parsed.shard_count,
        shard_index=parsed.shard_index,
    )
    if not selected:
        raise ValueError("selected shard has no node IDs")
    write_nodeids(parsed.output, selected)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
