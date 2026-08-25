#!/usr/bin/env python3
"""Coordinator-authorized nonproduction adapter for the local ABBA screen."""

from __future__ import annotations

import hashlib
import os
import platform
import subprocess
import sys
from pathlib import Path
from typing import Any, Mapping

from db.connection import db
from scripts.research import ptg2_packed_finalizer_abba as abba


LOCAL_SCREEN_OPT_IN_ENV = "HLTHPRT_PTG_PACKED_FINALIZER_ABBA_LOCAL_SCREEN"


def _sysctl(name: str) -> str:
    return subprocess.run(
        ("sysctl", "-n", name),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


async def _verify_local_screen(
    dsn_identity: Mapping[str, Any],
) -> dict[str, Any]:
    async with db.acquire() as connection:
        settings_row = await connection.first(
            "SELECT version(), current_setting('server_version_num'), "
            "current_setting('max_connections'), pg_backend_pid(), "
            "current_setting('data_directory'), inet_server_addr()::text, "
            "inet_server_port(), pg_postmaster_start_time(), "
            "current_setting('shared_buffers'), current_setting('work_mem'), "
            "current_setting('maintenance_work_mem'), "
            "current_setting('max_parallel_maintenance_workers'), "
            "current_setting('max_wal_size'), "
            "current_setting('synchronous_commit')"
        )
    server_version_num = int(settings_row[1])
    if not 180000 <= server_version_num < 190000:
        raise RuntimeError("local ABBA screen requires PostgreSQL 18")
    wrapper_path = Path(__file__)
    return {
        "environment_scope": "coordinator_authorized_local_nonproduction_screen",
        "production_acceptance": False,
        "resource_limits_verified": False,
        "matches_native_amd64_postgres18_8cpu_24gib": False,
        "dsn_identity": dict(dsn_identity),
        "postgres_version": str(settings_row[0]),
        "postgres_version_num": server_version_num,
        "postgres_max_connections": int(settings_row[2]),
        "postgres_backend": {
            "backend_pid": int(settings_row[3]),
            "resource_limits_verified": False,
        },
        "postgres_instance": {
            "data_directory": str(settings_row[4]),
            "server_address": str(settings_row[5]),
            "server_port": int(settings_row[6]),
            "postmaster_started_at": str(settings_row[7]),
        },
        "postgres_settings": {
            "shared_buffers": str(settings_row[8]),
            "work_mem": str(settings_row[9]),
            "maintenance_work_mem": str(settings_row[10]),
            "max_parallel_maintenance_workers": str(settings_row[11]),
            "max_wal_size": str(settings_row[12]),
            "synchronous_commit": str(settings_row[13]),
        },
        "runner": {
            "architecture": platform.machine().lower(),
            "platform": platform.platform(),
            "cpu_model": _sysctl("machdep.cpu.brand_string"),
            "logical_cpu_count": os.cpu_count(),
            "physical_memory_bytes": int(_sysctl("hw.memsize")),
        },
        "wrapper_sha256": hashlib.sha256(wrapper_path.read_bytes()).hexdigest(),
    }


def main() -> int:
    """Run the explicitly opted-in local synthetic benchmark adapter."""

    if os.getenv(LOCAL_SCREEN_OPT_IN_ENV) != "1":
        raise RuntimeError(f"set {LOCAL_SCREEN_OPT_IN_ENV}=1 for this local screen")
    if "--artifacts" in sys.argv or "--source-receipt" in sys.argv:
        raise RuntimeError("local ABBA screen accepts synthetic inputs only")
    abba.verify_benchmark_environment = _verify_local_screen
    return abba.main()


if __name__ == "__main__":
    raise SystemExit(main())
