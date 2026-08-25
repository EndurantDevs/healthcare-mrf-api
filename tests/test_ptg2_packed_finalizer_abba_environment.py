from __future__ import annotations

import hashlib
import subprocess

import pytest

from scripts.research import ptg2_packed_finalizer_abba_environment as environment


def _write_cgroup(path, *, cpu: str, memory: str, cpus: str = "") -> None:
    path.mkdir(parents=True, exist_ok=True)
    (path / "cpu.max").write_text(cpu, encoding="ascii")
    (path / "memory.max").write_text(memory, encoding="ascii")
    (path / "cpuset.cpus.effective").write_text(cpus, encoding="ascii")


def test_cgroup_receipt_uses_the_effective_nested_limits(tmp_path):
    root = tmp_path / "cgroup"
    _write_cgroup(root, cpu="max 100000", memory="max")
    _write_cgroup(
        root / "bench",
        cpu="800000 100000",
        memory=str(environment.EXPECTED_MEMORY_BYTES),
        cpus="0-7",
    )

    receipt = environment._cgroup_resource_receipt("0::/bench\n", root)

    assert receipt == {
        "cgroup_path": "/bench",
        "cgroup_cpu_limit": 8.0,
        "cgroup_memory_bytes": environment.EXPECTED_MEMORY_BYTES,
    }


def test_cpu_identity_rejects_emulated_x86():
    native = environment._cpu_identity_receipt(
        "vendor_id : GenuineIntel\nmodel name : Intel Xeon\n"
    )
    emulated = environment._cpu_identity_receipt(
        "vendor_id : GenuineIntel\nmodel name : QEMU Virtual CPU\n"
    )

    assert native["cpuinfo_is_native_x86"] is True
    assert emulated["cpuinfo_is_native_x86"] is False


def test_source_identity_binds_source_tree_and_selected_scanner(monkeypatch, tmp_path):
    root = tmp_path / "repo"
    harness = root / "scripts/research/benchmark.py"
    tracked = root / "tracked.py"
    harness.parent.mkdir(parents=True)
    harness.write_text("pass\n", encoding="utf-8")
    tracked.write_text("before\n", encoding="utf-8")
    subprocess.run(("git", "init", "-q"), cwd=root, check=True)
    subprocess.run(("git", "config", "user.email", "test@example.invalid"), cwd=root, check=True)
    subprocess.run(("git", "config", "user.name", "Test"), cwd=root, check=True)
    subprocess.run(("git", "add", "."), cwd=root, check=True)
    subprocess.run(("git", "commit", "-qm", "fixture"), cwd=root, check=True)
    tracked.write_text("after\n", encoding="utf-8")
    untracked = root / "new.py"
    untracked.write_text("new\n", encoding="utf-8")
    scanner = tmp_path / "ptg2_scanner"
    scanner.write_bytes(b"scanner-v1")
    monkeypatch.setattr(
        environment,
        "_ptg2_rust_scanner_binary",
        lambda: scanner,
    )

    receipt = environment.capture_source_identity(harness)
    diff = subprocess.run(
        ("git", "diff", "--binary", "HEAD", "--"),
        cwd=root,
        check=True,
        capture_output=True,
    ).stdout

    assert receipt["tracked_diff_sha256"] == hashlib.sha256(diff).hexdigest()
    assert receipt["untracked_files"] == {
        "new.py": hashlib.sha256(untracked.read_bytes()).hexdigest()
    }
    assert receipt["scanner_binary"] == {
        "path": str(scanner.resolve()),
        "profile": "custom",
        "byte_count": len(b"scanner-v1"),
        "sha256": hashlib.sha256(b"scanner-v1").hexdigest(),
        "is_amd64_elf": False,
    }

    environment.assert_source_identity_unchanged(harness, receipt)
    untracked.write_text("changed\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="source identity changed"):
        environment.assert_source_identity_unchanged(harness, receipt)
    untracked.write_text("new\n", encoding="utf-8")
    scanner.write_bytes(b"scanner-v2")
    with pytest.raises(RuntimeError, match="source identity changed"):
        environment.assert_source_identity_unchanged(harness, receipt)


def test_process_affinity_parser_requires_the_exact_allowed_set():
    status = "Name:\tpostgres\nCpus_allowed_list:\t2-5,8-11\n"

    assert environment._status_affinity_cpu_count(status) == 8
    with pytest.raises(RuntimeError, match="affinity is unavailable"):
        environment._status_affinity_cpu_count("Name:\tpostgres\n")
