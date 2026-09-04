"""Exercise CI sharding inside real pytest collection and xdist workers."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

import pytest

from scripts.ci.shard_pytest_nodeids import select_nodeids


ROOT = Path(__file__).resolve().parents[1]


def _run_pytest(tmp_path: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    """Keep the synthetic suite outside the repository's autouse fixtures."""

    (tmp_path / "pytest.ini").write_text("[pytest]\n", encoding="utf-8")
    return subprocess.run(
        [
            sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
            "-p", "scripts.ci.shard_pytest_nodeids", *arguments,
        ],
        cwd=tmp_path,
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join((str(ROOT), os.environ.get("PYTHONPATH", ""))),
            "PYTEST_ADDOPTS": "",
        },
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


@pytest.mark.parametrize(("workers", "distribution"), [(1, "loadscope"), (4, "worksteal")])
def test_xdist_shards_execute_the_legacy_sorted_assignment_once(
    tmp_path: Path, workers: int, distribution: str,
) -> None:
    """Require identical worker collections and exact-once execution."""

    (tmp_path / "conftest.py").write_text(
        "from pathlib import Path\n"
        "def pytest_collection_finish(session):\n"
        "    if hasattr(session.config, 'workerinput'):\n"
        "        worker = session.config.workerinput['workerid']\n"
        "        Path(f'collected-{worker}.txt').write_text('\\n'.join(item.nodeid for item in session.items))\n"
        "def pytest_runtest_call(item):\n"
        "    worker = item.config.workerinput['workerid']\n"
        "    with Path(f'observed-{worker}.txt').open('a') as stream:\n"
        "        stream.write(item.nodeid + '\\n')\n",
        encoding="utf-8",
    )
    (tmp_path / "test_sample.py").write_text(
        "import pytest\n"
        "def test_zeta(): pass\n"
        "@pytest.mark.parametrize('value', range(16, 0, -1))\n"
        "def test_values(value): assert value > 0\n"
        "def test_alpha(): pass\n",
        encoding="utf-8",
    )
    nodeids = [
        "test_sample.py::test_zeta", "test_sample.py::test_alpha",
        *(f"test_sample.py::test_values[{sample_number}]" for sample_number in range(16, 0, -1)),
    ]
    executed_ids = []
    for shard in range(2):
        pytest_result = _run_pytest(
            tmp_path, "-n", str(workers), "--dist", distribution,
            "--ci-shard-count", "2", "--ci-shard-index", str(shard),
        )
        assert pytest_result.returncode == 0, pytest_result.stdout + pytest_result.stderr
        expected = select_nodeids(nodeids, shard_count=2, shard_index=shard)
        collections = list(tmp_path.glob("collected-*.txt"))
        assert len(collections) == workers
        for path in collections:
            assert path.read_text(encoding="utf-8").splitlines() == expected
            path.unlink()
        observed_ids = []
        for path in sorted(tmp_path.glob("observed-*.txt")):
            observed_ids.extend(path.read_text(encoding="utf-8").splitlines())
            path.unlink()
        assert sorted(observed_ids) == expected
        if workers == 1:
            assert observed_ids == expected
        executed_ids.extend(observed_ids)
    assert sorted(executed_ids) == sorted(nodeids)


@pytest.mark.parametrize(
    ("source", "arguments", "error"),
    [
        ("def test_one(): pass\n", ("--ci-shard-count", "0"), "positive shard count"),
        ("def test_one(): pass\n", ("--ci-shard-index", "-1"), "positive shard count"),
        ("def test_one(): pass\n", ("--ci-shard-index", "1"), "positive shard count"),
        ("def test_one(): pass\n", ("--keep-duplicates", "test_sample.py", "test_sample.py"), "duplicate node IDs"),
        ("", (), "selected shard has no node IDs"),
        ("raise RuntimeError('broken collection')\n", (), "broken collection"),
        ("def test_one(): assert False, 'broken test'\n", ("-n", "1"), "broken test"),
    ],
)
def test_shard_errors_never_pass(
    tmp_path: Path, source: str, arguments: tuple[str, ...], error: str,
) -> None:
    """Reject invalid selections and preserve collection and execution failures."""

    (tmp_path / "test_sample.py").write_text(source, encoding="utf-8")
    result = _run_pytest(
        tmp_path, "--ci-shard-count", "1", "--ci-shard-index", "0", *arguments,
    )
    assert result.returncode != 0
    assert error in result.stdout + result.stderr
