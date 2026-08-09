# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resource and query-plan instrumentation for opt-in NPPES scale proof."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
from urllib.parse import urlsplit

import asyncpg

from tests.public_evidence_nppes_admission_postgres_support import qualified


SCALE_TABLES = (
    "public_evidence_source_identity",
    "public_evidence_source_release",
    "public_evidence_source_record",
    "public_evidence_record",
    "public_evidence_record_source_link",
    "public_evidence_npi_enumeration",
    "public_evidence_nppes_registry_admission",
    "public_evidence_nppes_registry_admission_seal",
    "public_evidence_nppes_registry_member",
)
STAGE_TABLES = (
    "nppes_stage_source_record",
    "nppes_stage_member",
    "nppes_stage_common",
    "nppes_stage_source_link",
    "nppes_stage_typed",
)


def machine_preflight(root: Path) -> dict[str, int]:
    """Require the minimum physical memory and disposable disk headroom."""

    page_size = os.sysconf("SC_PAGE_SIZE")
    page_count = os.sysconf("SC_PHYS_PAGES")
    physical_memory_bytes = int(page_size * page_count)
    free_disk_bytes = shutil.disk_usage(root).free
    assert physical_memory_bytes >= 4 * 1024**3
    assert free_disk_bytes >= 40 * 1024**3
    return {
        "physical_memory_bytes": physical_memory_bytes,
        "free_disk_bytes": free_disk_bytes,
    }


def require_loopback_test_database() -> None:
    """Reject accidental use of a remote or production PostgreSQL service."""

    raw_dsn = os.getenv("HLTHPRT_PUBLIC_EVIDENCE_STORAGE_POSTGRES_DSN", "")
    parsed_dsn = urlsplit(raw_dsn)
    if parsed_dsn.hostname not in {"127.0.0.1", "localhost", "::1"}:
        raise AssertionError("scale proof requires a loopback PostgreSQL database")


async def database_settings(connection: asyncpg.Connection) -> dict[str, object]:
    """Return and validate the bounded PostgreSQL durability fingerprint."""

    setting_names = (
        "server_version_num",
        "fsync",
        "full_page_writes",
        "synchronous_commit",
        "shared_buffers",
        "work_mem",
        "wal_compression",
        "jit",
        "track_io_timing",
    )
    settings_by_name = {
        setting_name: await connection.fetchval(
            "SELECT current_setting($1)", setting_name
        )
        for setting_name in setting_names
    }
    assert 180_000 <= int(settings_by_name["server_version_num"]) < 190_000
    assert settings_by_name["fsync"] == "on"
    assert settings_by_name["full_page_writes"] == "on"
    assert settings_by_name["synchronous_commit"] == "on"
    active_writers = await connection.fetchval(
        "SELECT count(*) FROM pg_catalog.pg_stat_activity "
        "WHERE datname=current_database() AND pid<>pg_backend_pid() "
        "AND state<>'idle'"
    )
    assert active_writers == 0
    return settings_by_name


async def database_counters(connection: asyncpg.Connection) -> dict[str, object]:
    """Read database I/O counters without exposing connection identity."""

    await connection.execute("SELECT pg_stat_clear_snapshot()")
    database_stats = await connection.fetchrow(
        "SELECT temp_files, temp_bytes, blk_read_time, blk_write_time "
        "FROM pg_catalog.pg_stat_database WHERE datname=current_database()"
    )
    checkpointer_stats = await connection.fetchrow(
        "SELECT num_timed, num_requested, num_done, write_time, sync_time, "
        "buffers_written FROM pg_catalog.pg_stat_checkpointer"
    )
    assert database_stats is not None and checkpointer_stats is not None
    return {**dict(database_stats), **dict(checkpointer_stats)}


async def cluster_lsn_delta(connection: asyncpg.Connection, before_lsn: str) -> int:
    """Return a diagnostic cluster-wide WAL delta from one exact LSN."""

    return await connection.fetchval(
        "SELECT pg_wal_lsn_diff("
        "pg_current_wal_insert_lsn(), $1::text::pg_lsn)::bigint",
        before_lsn,
    )


async def backend_wal_snapshot(
    connection: asyncpg.Connection,
    observer: asyncpg.Connection,
) -> dict[str, int]:
    """Flush and read WAL counters owned by exactly one live backend."""

    backend_pid = await connection.fetchval("SELECT pg_backend_pid()")
    await connection.execute("SELECT pg_stat_force_next_flush()")
    await observer.execute("SELECT pg_stat_clear_snapshot()")
    backend_wal = await observer.fetchrow(
        "SELECT wal_records, wal_fpi, wal_bytes "
        "FROM pg_catalog.pg_stat_get_backend_wal($1)",
        backend_pid,
    )
    assert backend_wal is not None
    return {
        "wal_records": int(backend_wal["wal_records"]),
        "wal_fpi": int(backend_wal["wal_fpi"]),
        "wal_bytes": int(backend_wal["wal_bytes"]),
    }


def backend_wal_delta(
    before_by_name: dict[str, int],
    after_by_name: dict[str, int],
) -> dict[str, int]:
    """Return exact monotonic per-backend WAL counter differences."""

    delta_by_name = {
        metric_name: after_by_name[metric_name] - before_by_name[metric_name]
        for metric_name in before_by_name
    }
    assert all(metric_value >= 0 for metric_value in delta_by_name.values())
    return delta_by_name


async def relation_sizes(
    connection: asyncpg.Connection,
    schema_name: str,
) -> dict[str, dict[str, int]]:
    """Return heap, index, and total bytes for every admitted table."""

    sizes_by_table: dict[str, dict[str, int]] = {}
    for table_name in SCALE_TABLES:
        relation_name = f"{schema_name}.{table_name}"
        size_row = await connection.fetchrow(
            "SELECT pg_relation_size($1::regclass)::bigint AS heap_bytes, "
            "pg_indexes_size($1::regclass)::bigint AS index_bytes, "
            "pg_total_relation_size($1::regclass)::bigint AS total_bytes",
            relation_name,
        )
        sizes_by_table[table_name] = dict(size_row)
    return sizes_by_table


async def stage_total_size(connection: asyncpg.Connection) -> int:
    """Return peak total bytes for the five current temporary stage tables."""

    total_bytes = 0
    for stage_table in STAGE_TABLES:
        total_bytes += await connection.fetchval(
            "SELECT pg_total_relation_size(to_regclass($1))::bigint",
            f"pg_temp.{stage_table}",
        )
    return total_bytes


def _plan_summary(raw_plan: object, row_count: int) -> dict[str, object]:
    plan_payload = json.loads(raw_plan) if type(raw_plan) is str else raw_plan
    root = plan_payload[0]
    node_summaries: list[dict[str, object]] = []

    def visit(plan_node: dict[str, object]) -> None:
        actual_rows = int(plan_node.get("Actual Rows", 0))
        actual_loops = int(plan_node.get("Actual Loops", 0))
        rows_removed = sum(
            int(metric_value)
            for key, metric_value in plan_node.items()
            if key.startswith("Rows Removed")
        )
        node_work = (actual_rows + rows_removed) * actual_loops
        assert node_work <= 4 * row_count, (
            plan_node.get("Node Type"),
            actual_rows,
            actual_loops,
            rows_removed,
            node_work,
        )
        node_summaries.append(
            {
                "node_type": plan_node.get("Node Type"),
                "actual_rows": actual_rows,
                "actual_loops": actual_loops,
                "node_work": node_work,
                "shared_hit_blocks": int(plan_node.get("Shared Hit Blocks", 0)),
                "shared_read_blocks": int(plan_node.get("Shared Read Blocks", 0)),
                "temp_written_blocks": int(plan_node.get("Temp Written Blocks", 0)),
            }
        )
        child_nodes = plan_node.get("Plans", ())
        if type(child_nodes) in {list, tuple}:
            for child_node in child_nodes:
                if type(child_node) is dict:
                    visit(child_node)

    visit(root["Plan"])
    return {
        "planning_ms": float(root["Planning Time"]),
        "execution_ms": float(root["Execution Time"]),
        "nodes": node_summaries,
    }


async def explain_probes(
    connection: asyncpg.Connection,
    schema_name: str,
    admission_ref: str,
    row_count: int,
) -> dict[str, object]:
    """Run the bounded Merkle, source-parity, and projected-topology plans."""

    member_table = qualified(schema_name, "public_evidence_nppes_registry_member")
    source_table = qualified(schema_name, "public_evidence_source_record")
    common_table = qualified(schema_name, "public_evidence_record")
    link_table = qualified(schema_name, "public_evidence_record_source_link")
    typed_table = qualified(schema_name, "public_evidence_npi_enumeration")
    merkle_function = qualified(schema_name, "public_evidence_nppes_merkle_root")
    explain_prefix = (
        "EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS, TIMING OFF, "
        "SUMMARY ON, FORMAT JSON) "
    )
    queries_by_name = {
        "member_merkle": (
            "SELECT count(*), min(source_row_ordinal), max(source_row_ordinal), "
            f"{merkle_function}(source_row_ordinal, leaf_sha256 "
            f"ORDER BY source_row_ordinal) FROM {member_table} WHERE admission_ref=$1"
        ),
        "member_source_parity": (
            f"SELECT count(*) FROM {member_table} member_row "
            f"JOIN {source_table} source_row "
            "ON source_row.source_record_ref=member_row.source_record_ref "
            "AND source_row.nppes_admission_ref=member_row.admission_ref "
            "WHERE member_row.admission_ref=$1"
        ),
        "projected_topology": (
            f"SELECT count(*) FROM {member_table} member_row "
            f"JOIN {source_table} source_row ON source_row.source_record_ref="
            "member_row.source_record_ref AND source_row.nppes_admission_ref="
            "member_row.admission_ref "
            f"JOIN {common_table} common_row "
            "ON common_row.evidence_ref=member_row.evidence_ref "
            f"JOIN {link_table} link_row ON link_row.evidence_ref=member_row.evidence_ref "
            f"JOIN {typed_table} typed_row "
            "ON typed_row.evidence_ref=member_row.evidence_ref "
            "WHERE member_row.admission_ref=$1 "
            "AND member_row.projection_state='projected_v1'"
        ),
    }
    return {
        probe_name: _plan_summary(
            await connection.fetchval(explain_prefix + probe_query, admission_ref),
            row_count,
        )
        for probe_name, probe_query in queries_by_name.items()
    }


__all__ = (
    "backend_wal_delta",
    "backend_wal_snapshot",
    "cluster_lsn_delta",
    "database_counters",
    "database_settings",
    "explain_probes",
    "machine_preflight",
    "relation_sizes",
    "require_loopback_test_database",
    "stage_total_size",
)
