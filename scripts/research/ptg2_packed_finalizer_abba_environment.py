"""Fail-closed source, database, and host identity for the ABBA screen."""

from __future__ import annotations

import hashlib
import os
import platform
import re
import subprocess
from pathlib import Path, PurePosixPath
from typing import Any, Mapping

from sqlalchemy.engine import make_url

from db.connection import db
from process.ptg_parts.rust_scanner import (
    _ptg2_rust_scanner_binary,
    _ptg2_scanner_binary_profile,
)


OPT_IN_ENV = "HLTHPRT_PTG_PACKED_FINALIZER_ABBA"
DSN_ENV = "HLTHPRT_PTG_PACKED_FINALIZER_ABBA_POSTGRES_DSN"
EXPECTED_MEMORY_BYTES = 24 * 1024**3
DATABASE_RE = re.compile(r"^ptg_packed_finalizer_(?:bench|test)_[a-z0-9_]+$")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def capture_source_identity(harness_path: Path) -> dict[str, Any]:
    """Bind the benchmark to the complete committed and dirty source tree."""

    root = harness_path.resolve().parents[2]
    scanner_binary = _scanner_binary_receipt()
    head = subprocess.run(
        ("git", "rev-parse", "HEAD"),
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    tracked_diff = subprocess.run(
        ("git", "diff", "--binary", "HEAD", "--"),
        cwd=root,
        check=True,
        capture_output=True,
    ).stdout
    untracked_output = subprocess.run(
        ("git", "ls-files", "--others", "--exclude-standard", "-z"),
        cwd=root,
        check=True,
        capture_output=True,
    ).stdout
    untracked_paths = tuple(
        Path(os.fsdecode(path_bytes))
        for path_bytes in untracked_output.split(b"\0")
        if path_bytes
    )
    return {
        "git_head": head,
        "tracked_diff_sha256": hashlib.sha256(tracked_diff).hexdigest(),
        "untracked_files": {
            str(path): _sha256_file(root / path)
            for path in sorted(untracked_paths, key=str)
        },
        "scanner_binary": scanner_binary,
    }


def _scanner_binary_receipt() -> dict[str, Any]:
    scanner_binary = _ptg2_rust_scanner_binary()
    if scanner_binary is None or not scanner_binary.is_file():
        raise RuntimeError("ABBA Rust scanner binary is unavailable")
    scanner_binary = scanner_binary.resolve()
    return {
        "path": str(scanner_binary),
        "profile": _ptg2_scanner_binary_profile(scanner_binary),
        "byte_count": scanner_binary.stat().st_size,
        "sha256": _sha256_file(scanner_binary),
        "is_amd64_elf": _is_amd64_elf(scanner_binary),
    }


def assert_source_identity_unchanged(
    harness_path: Path,
    expected_identity: Mapping[str, Any],
) -> None:
    """Reject a source-tree change at any benchmark arm boundary."""

    if capture_source_identity(harness_path) != expected_identity:
        raise RuntimeError("ABBA source identity changed during the run")


def configure_database(dsn: str) -> dict[str, Any]:
    """Configure only an explicitly disposable localhost PostgreSQL database."""

    url = make_url(dsn)
    database_name = str(url.database or "")
    host = str(url.host or "")
    if (
        not url.drivername.startswith("postgresql")
        or host not in {"127.0.0.1", "localhost", "::1"}
        or int(url.port or 5432) != 5440
        or not url.username
        or not DATABASE_RE.fullmatch(database_name)
    ):
        raise ValueError(f"{DSN_ENV} must identify local port 5440 and a disposable database")
    os.environ.update(
        {
            "HLTHPRT_DB_DRIVER": "asyncpg",
            "HLTHPRT_DB_HOST": host,
            "HLTHPRT_DB_PORT": str(url.port or 5432),
            "HLTHPRT_DB_USER": str(url.username),
            "HLTHPRT_DB_PASSWORD": str(url.password or ""),
            "HLTHPRT_DB_DATABASE": database_name,
            "HLTHPRT_DB_POOL_MIN_SIZE": "1",
            "HLTHPRT_DB_POOL_MAX_SIZE": "8",
        }
    )
    return {"host": host, "port": int(url.port or 5432), "database": database_name}


def _cgroup_relative_path(proc_cgroup: str) -> PurePosixPath:
    lines = [line for line in proc_cgroup.splitlines() if line]
    if len(lines) != 1 or not lines[0].startswith("0::/"):
        raise RuntimeError("ABBA requires one cgroup v2 hierarchy")
    relative = PurePosixPath(lines[0][3:])
    if ".." in relative.parts:
        raise RuntimeError("ABBA cgroup path is unsafe")
    return relative


def _cgroup_paths(root: Path, relative: PurePosixPath) -> tuple[Path, ...]:
    paths = [root]
    for part in relative.parts[1:]:
        paths.append(paths[-1] / part)
    return tuple(paths)


def _finite_limit_value(raw_value: str) -> int | None:
    raw_value = raw_value.strip()
    return None if raw_value == "max" else int(raw_value)


def _cpu_quota_value(raw_value: str) -> float | None:
    quota, period = raw_value.split()
    return None if quota == "max" else int(quota) / int(period)


def _cpu_set_size_value(raw_value: str | None) -> int | None:
    raw_value = str(raw_value or "").strip()
    if not raw_value:
        return None
    cpu_ids: set[int] = set()
    for item in raw_value.split(","):
        first, separator, last = item.partition("-")
        start = int(first)
        stop = int(last) if separator else start
        if stop < start:
            raise ValueError("cgroup CPU set is invalid")
        cpu_ids.update(range(start, stop + 1))
    return len(cpu_ids)


def _cgroup_resource_receipt(proc_cgroup: str, root: Path) -> dict[str, Any]:
    relative = _cgroup_relative_path(proc_cgroup)
    paths = _cgroup_paths(root, relative)
    cpu_limits: list[float] = []
    memory_limits: list[int] = []
    for path in paths:
        try:
            cpu_raw = (path / "cpu.max").read_text(encoding="ascii")
        except OSError:
            cpu_raw = None
        try:
            memory_raw = (path / "memory.max").read_text(encoding="ascii")
        except OSError:
            memory_raw = None
        cpu_limit = _cpu_quota_value(cpu_raw) if cpu_raw is not None else None
        memory_limit = (
            _finite_limit_value(memory_raw) if memory_raw is not None else None
        )
        if cpu_limit is not None:
            cpu_limits.append(cpu_limit)
        if memory_limit is not None:
            memory_limits.append(memory_limit)
    try:
        cpu_set = (paths[-1] / "cpuset.cpus.effective").read_text(
            encoding="ascii"
        )
    except OSError:
        cpu_set = None
    cpu_set_size = _cpu_set_size_value(cpu_set)
    if cpu_set_size is not None:
        cpu_limits.append(float(cpu_set_size))
    return {
        "cgroup_path": str(relative),
        "cgroup_cpu_limit": min(cpu_limits) if cpu_limits else None,
        "cgroup_memory_bytes": min(memory_limits) if memory_limits else None,
    }


def _is_amd64_elf_header(header: bytes) -> bool:
    if len(header) != 20 or header[:5] != b"\x7fELF\x02":
        return False
    byte_order = "little" if header[5] == 1 else "big" if header[5] == 2 else ""
    return bool(byte_order) and int.from_bytes(header[18:20], byte_order) == 62


def _is_amd64_elf(path: Path) -> bool:
    with path.open("rb") as binary:
        return _is_amd64_elf_header(binary.read(20))


def _cpu_identity_receipt(cpuinfo: str) -> dict[str, Any]:
    lowered = cpuinfo.lower()
    has_x86_vendor = "genuineintel" in lowered or "authenticamd" in lowered
    is_emulated = any(token in lowered for token in ("qemu", "tcg", "bochs"))
    model_names = sorted(
        {
            line.partition(":")[2].strip()
            for line in cpuinfo.splitlines()
            if line.lower().startswith("model name")
        }
    )
    return {
        "cpuinfo_sha256": hashlib.sha256(cpuinfo.encode("utf-8")).hexdigest(),
        "cpu_model_names": model_names,
        "cpuinfo_is_native_x86": has_x86_vendor and not is_emulated,
    }


def _local_affinity_cpu_count() -> int | None:
    get_affinity = getattr(os, "sched_getaffinity", None)
    return len(get_affinity(0)) if callable(get_affinity) else None


def _status_affinity_cpu_count(status: str) -> int:
    for line in status.splitlines():
        if line.startswith("Cpus_allowed_list:"):
            cpu_count = _cpu_set_size_value(line.partition(":")[2])
            if cpu_count is not None:
                return cpu_count
    raise RuntimeError("ABBA process affinity is unavailable")


def _local_resource_receipt() -> dict[str, Any]:
    local_cgroup = Path("/proc/self/cgroup").read_text(encoding="ascii")
    cpu_identity = _cpu_identity_receipt(
        Path("/proc/cpuinfo").read_text(encoding="utf-8", errors="replace")
    )
    return {
        "architecture": platform.machine().lower(),
        **_cgroup_resource_receipt(local_cgroup, Path("/sys/fs/cgroup")),
        **cpu_identity,
        "process_is_amd64_elf": _is_amd64_elf(Path("/proc/self/exe")),
        "process_affinity_cpu_count": _local_affinity_cpu_count(),
    }


async def _postgres_file(
    connection: Any,
    path: str,
    *,
    binary: bool = False,
    missing_ok: bool = False,
) -> bytes | str | None:
    function = "pg_read_binary_file" if binary else "pg_read_file"
    file_row = await connection.first(
        f"SELECT {function}(:path, 0, :length, :missing_ok)",
        path=path,
        length=20 if binary else 16 * 1024 * 1024,
        missing_ok=missing_ok,
    )
    return file_row[0]


async def _postgres_resource_receipt(
    connection: Any,
    backend_pid: int,
) -> dict[str, Any]:
    proc_cgroup = str(await _postgres_file(connection, "/proc/self/cgroup"))
    relative = _cgroup_relative_path(proc_cgroup)
    paths = _cgroup_paths(Path("/sys/fs/cgroup"), relative)
    cpu_limits: list[float] = []
    memory_limits: list[int] = []
    for path in paths:
        cpu_raw = await _postgres_file(
            connection, str(path / "cpu.max"), missing_ok=True
        )
        memory_raw = await _postgres_file(
            connection, str(path / "memory.max"), missing_ok=True
        )
        cpu_limit = _cpu_quota_value(str(cpu_raw)) if cpu_raw is not None else None
        memory_limit = (
            _finite_limit_value(str(memory_raw)) if memory_raw is not None else None
        )
        if cpu_limit is not None:
            cpu_limits.append(cpu_limit)
        if memory_limit is not None:
            memory_limits.append(memory_limit)
    cpu_set_size = _cpu_set_size_value(
        await _postgres_file(
            connection,
            str(paths[-1] / "cpuset.cpus.effective"),
            missing_ok=True,
        )
    )
    if cpu_set_size is not None:
        cpu_limits.append(float(cpu_set_size))
    cpuinfo = str(await _postgres_file(connection, "/proc/cpuinfo"))
    executable_header = bytes(
        await _postgres_file(connection, "/proc/self/exe", binary=True) or b""
    )[:20]
    process_status = str(await _postgres_file(connection, "/proc/self/status"))
    return {
        "backend_pid": backend_pid,
        "cgroup_path": str(relative),
        "cgroup_cpu_limit": min(cpu_limits) if cpu_limits else None,
        "cgroup_memory_bytes": min(memory_limits) if memory_limits else None,
        **_cpu_identity_receipt(cpuinfo),
        "process_is_amd64_elf": _is_amd64_elf_header(executable_header),
        "process_affinity_cpu_count": _status_affinity_cpu_count(process_status),
    }


async def verify_benchmark_environment(
    dsn_identity: Mapping[str, Any],
) -> dict[str, Any]:
    """Verify the benchmark host and PostgreSQL environment fail closed."""

    async with db.acquire() as connection:
        postgres_settings_row = await connection.first(
            "SELECT version(), current_setting('server_version_num'), "
            "current_setting('max_connections'), pg_backend_pid()"
        )
        postgres = await _postgres_resource_receipt(
            connection,
            int(postgres_settings_row[3]),
        )
    local = _local_resource_receipt()
    scanner_binary = _scanner_binary_receipt()
    server_version_num = int(postgres_settings_row[1])
    postgres_version = str(postgres_settings_row[0])
    is_matching_environment = (
        local["architecture"] in {"amd64", "x86_64"}
        and local["cpuinfo_is_native_x86"]
        and local["process_is_amd64_elf"]
        and local["cgroup_cpu_limit"] == 8
        and local["cgroup_memory_bytes"] == EXPECTED_MEMORY_BYTES
        and local["process_affinity_cpu_count"] == 8
        and scanner_binary["profile"] == "release"
        and scanner_binary["is_amd64_elf"]
        and postgres["cpuinfo_is_native_x86"]
        and postgres["process_is_amd64_elf"]
        and postgres["cgroup_cpu_limit"] == 8
        and postgres["cgroup_memory_bytes"] == EXPECTED_MEMORY_BYTES
        and postgres["process_affinity_cpu_count"] == 8
        and 180000 <= server_version_num < 190000
        and any(architecture in postgres_version.lower() for architecture in ("amd64", "x86_64"))
    )
    receipt_by_field = {
        "dsn_identity": dict(dsn_identity),
        "postgres_version": postgres_version,
        "postgres_version_num": server_version_num,
        "postgres_max_connections": int(postgres_settings_row[2]),
        "runner": local,
        "scanner_binary": scanner_binary,
        "postgres_backend": postgres,
        "matches_native_amd64_postgres18_8cpu_24gib": is_matching_environment,
    }
    if not is_matching_environment:
        raise RuntimeError("ABBA environment is not native amd64 PostgreSQL 18 at 8 CPU/24 GiB")
    return receipt_by_field


__all__ = (
    "DSN_ENV",
    "OPT_IN_ENV",
    "assert_source_identity_unchanged",
    "capture_source_identity",
    "configure_database",
    "verify_benchmark_environment",
)
