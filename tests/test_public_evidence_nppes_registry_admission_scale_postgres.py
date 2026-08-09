# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Opt-in PostgreSQL scale proof for the complete NPPES archive writer."""

from __future__ import annotations

import json
import os
from pathlib import Path
import resource
import sys
import time
from typing import NamedTuple

import asyncpg
import pytest

from process import nppes_public_evidence_writer as writer
from process.nppes_public_evidence_replay import prepare_nppes_registry_replay
from tests.nppes_admission_scale_postgres_support import (
    backend_wal_delta,
    backend_wal_snapshot,
    cluster_lsn_delta,
    database_counters,
    database_settings,
    explain_probes,
    machine_preflight,
    relation_sizes,
    require_loopback_test_database,
    stage_total_size,
)
from tests.nppes_public_evidence_process_support import prepared_sized_archive
from tests.public_evidence_nppes_admission_postgres_support import (
    ConnectionDatabase,
    nppes_admission_schema,
    qualified,
    required_config,
)
from tests.public_evidence_storage_postgres_support import connect


class _DatabaseTrial(NamedTuple):
    settings_by_name: dict[str, object]
    before_counters: dict[str, object]
    after_counters: dict[str, object]
    relation_sizes: dict[str, dict[str, int]]
    admission_backend_wal: dict[str, int]
    idempotent_backend_wal: dict[str, int]
    explain_by_name: dict[str, object]
    projected_count: int
    excluded_count: int


def _requested_row_count() -> int:
    raw_value = os.getenv("HLTHPRT_NPPES_ADMISSION_SCALE_ROWS", "")
    if not raw_value:
        pytest.skip("NPPES admission scale proof is opt-in")
    if not raw_value.isascii() or not raw_value.isdigit():
        raise AssertionError("scale row count must be an ASCII integer")
    row_count = int(raw_value)
    if row_count not in {1_000, 100_000, 1_000_000}:
        raise AssertionError("scale row count must be 1000, 100000, or 1000000")
    return row_count


def _benchmark_archive_spec(row_count: int) -> tuple[str, str]:
    archive_by_size = {
        1_000: ("NPPES_Data_Dissemination_May_2026_V2.zip", "20260531"),
        100_000: ("NPPES_Data_Dissemination_June_2026_V2.zip", "20260630"),
        1_000_000: ("NPPES_Data_Dissemination_July_2026_V2.zip", "20260712"),
    }
    return archive_by_size[row_count]


def _peak_rss_bytes() -> int:
    peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(peak_rss if sys.platform == "darwin" else peak_rss * 1024)


async def _set_benchmark_wal_compression(connection: asyncpg.Connection) -> None:
    """Apply one allowlisted test-only WAL compression setting when requested."""

    requested_value = os.getenv(
        "HLTHPRT_NPPES_ADMISSION_SCALE_WAL_COMPRESSION", ""
    )
    if not requested_value:
        return
    if requested_value not in {"on", "pglz", "lz4", "zstd"}:
        raise AssertionError("unsupported scale WAL compression setting")
    await connection.execute(f"SET wal_compression TO '{requested_value}'")


def _install_phase_instrumentation(
    monkeypatch: pytest.MonkeyPatch,
    phase_seconds_by_name: dict[str, float],
    phase_values_by_name: dict[str, int],
) -> None:
    original_create = writer._create_stages
    original_stage = writer._stage_complete_replay
    original_finalize = writer._finalize
    original_cleanup = writer._drain_stage_cleanup

    async def timed_create(*args, **kwargs):
        started_at = time.perf_counter()
        result = await original_create(*args, **kwargs)
        phase_seconds_by_name["create_stages"] = time.perf_counter() - started_at
        return result

    async def timed_stage(*args, **kwargs):
        started_at = time.perf_counter()
        result = await original_stage(*args, **kwargs)
        phase_seconds_by_name["stage_replay"] = time.perf_counter() - started_at
        return result

    async def timed_finalize(connection, *args, **kwargs):
        phase_values_by_name["stage_peak_bytes"] = await stage_total_size(connection)
        before_lsn = await connection.fetchval(
            "SELECT pg_current_wal_insert_lsn()::text"
        )
        started_at = time.perf_counter()
        result = await original_finalize(connection, *args, **kwargs)
        phase_seconds_by_name["finalize"] = time.perf_counter() - started_at
        phase_values_by_name["finalize_cluster_lsn_bytes"] = await cluster_lsn_delta(
            connection, before_lsn
        )
        return result

    async def timed_cleanup(*args, **kwargs):
        started_at = time.perf_counter()
        result = await original_cleanup(*args, **kwargs)
        phase_seconds_by_name["cleanup"] = time.perf_counter() - started_at
        return result

    monkeypatch.setattr(writer, "_create_stages", timed_create)
    monkeypatch.setattr(writer, "_stage_complete_replay", timed_stage)
    monkeypatch.setattr(writer, "_finalize", timed_finalize)
    monkeypatch.setattr(writer, "_drain_stage_cleanup", timed_cleanup)


async def _assert_admitted_counts(
    connection: asyncpg.Connection,
    schema_name: str,
    admission_ref: str,
    row_count: int,
) -> tuple[int, int]:
    entity_missing = (row_count + 1) // 100
    effective_missing = row_count // 100
    projected_count = row_count - entity_missing - effective_missing
    admission_counts = await connection.fetchrow(
        f"SELECT source_record_count, projected_record_count, excluded_record_count, "
        "effective_start_not_disclosed_count, entity_type_not_disclosed_count "
        f"FROM {qualified(schema_name, 'public_evidence_nppes_registry_admission')} "
        "WHERE admission_ref=$1",
        admission_ref,
    )
    assert tuple(admission_counts) == (
        row_count,
        projected_count,
        entity_missing + effective_missing,
        effective_missing,
        entity_missing,
    )
    table_count_by_name = {
        "public_evidence_source_record": ("nppes_admission_ref", row_count),
        "public_evidence_nppes_registry_member": ("admission_ref", row_count),
        "public_evidence_record": ("nppes_admission_ref", projected_count),
        "public_evidence_record_source_link": (
            "nppes_admission_ref",
            projected_count,
        ),
        "public_evidence_npi_enumeration": (
            "nppes_admission_ref",
            projected_count,
        ),
    }
    for table_name, (admission_column, expected_count) in table_count_by_name.items():
        assert await connection.fetchval(
            f"SELECT count(*) FROM {qualified(schema_name, table_name)} "
            f"WHERE {admission_column}=$1",
            admission_ref,
        ) == expected_count
    assert await connection.fetchval(
        f"SELECT count(*) FROM "
        f"{qualified(schema_name, 'public_evidence_nppes_registry_admission_seal')} "
        "WHERE admission_ref=$1",
        admission_ref,
    ) == 1
    return projected_count, entity_missing + effective_missing


async def _measure_idempotent_replay(
    connection: asyncpg.Connection,
    observer: asyncpg.Connection,
    schema_name: str,
    prepared_replay: object,
    phase_seconds_by_name: dict[str, float],
) -> tuple[dict[str, int], dict[str, dict[str, int]]]:
    """Require a database-enforced read-only replay and unchanged sizes."""

    sizes_before_replay = await relation_sizes(connection, schema_name)
    before_backend_wal = await backend_wal_snapshot(connection, observer)
    idempotent_started_at = time.perf_counter()
    async with connection.transaction(readonly=True):
        replay_receipt = await writer.admit_nppes_registry_archive(
            prepared_replay,
            required_config(),
            schema=schema_name,
            database=ConnectionDatabase(connection),
        )
    phase_seconds_by_name["idempotent_replay"] = (
        time.perf_counter() - idempotent_started_at
    )
    assert replay_receipt.write_state == "already_present"
    idempotent_backend_wal = backend_wal_delta(
        before_backend_wal,
        await backend_wal_snapshot(connection, observer),
    )
    sizes_after_replay = await relation_sizes(connection, schema_name)
    assert sizes_after_replay == sizes_before_replay
    return idempotent_backend_wal, sizes_after_replay


async def _admit_scale_replay(
    connection: asyncpg.Connection,
    observer: asyncpg.Connection,
    schema_name: str,
    prepared_replay: object,
    phase_seconds_by_name: dict[str, float],
) -> tuple[object, dict[str, int]]:
    """Measure the first exact insertion and its backend-local WAL."""

    before_admission_wal = await backend_wal_snapshot(connection, observer)
    admission_started_at = time.perf_counter()
    admission_receipt = await writer.admit_nppes_registry_archive(
        prepared_replay,
        required_config(),
        schema=schema_name,
        database=ConnectionDatabase(connection),
    )
    phase_seconds_by_name["admission"] = time.perf_counter() - admission_started_at
    assert admission_receipt.write_state == "inserted"
    admission_backend_wal = backend_wal_delta(
        before_admission_wal,
        await backend_wal_snapshot(connection, observer),
    )
    return admission_receipt, admission_backend_wal


async def _run_database_trial(
    prepared_replay: object,
    row_count: int,
    phase_seconds_by_name: dict[str, float],
) -> _DatabaseTrial:
    """Run one fresh-schema admission, exact replay, and read-only plan proof."""

    async with nppes_admission_schema() as schema_context:
        _engine, database_url, schema_name, _migration = schema_context
        connection = await connect(database_url)
        observer = None
        try:
            await _set_benchmark_wal_compression(connection)
            settings_by_name = await database_settings(connection)
            observer = await connect(database_url)
            before_counters = await database_counters(connection)
            admission_receipt, admission_backend_wal = await _admit_scale_replay(
                connection,
                observer,
                schema_name,
                prepared_replay,
                phase_seconds_by_name,
            )
            projected_count, excluded_count = await _assert_admitted_counts(
                connection,
                schema_name,
                admission_receipt.admission_ref,
                row_count,
            )
            idempotent_backend_wal, relation_sizes = await _measure_idempotent_replay(
                connection,
                observer,
                schema_name,
                prepared_replay,
                phase_seconds_by_name,
            )
            explain_by_name = await explain_probes(
                connection,
                schema_name,
                admission_receipt.admission_ref,
                row_count,
            )
            await connection.execute("SELECT pg_stat_force_next_flush()")
            after_counters = await database_counters(connection)
        finally:
            if observer is not None:
                await observer.close()
            await connection.close()
    return _DatabaseTrial(
        settings_by_name,
        before_counters,
        after_counters,
        relation_sizes,
        admission_backend_wal,
        idempotent_backend_wal,
        explain_by_name,
        projected_count,
        excluded_count,
    )


def _checkpointer_delta(trial: _DatabaseTrial) -> dict[str, object]:
    """Return bounded checkpointer counter differences for one trial."""

    return {
        metric_name: trial.after_counters[metric_name]
        - trial.before_counters[metric_name]
        for metric_name in (
            "num_timed",
            "num_requested",
            "num_done",
            "write_time",
            "sync_time",
            "buffers_written",
        )
    }


def _build_scale_report(
    row_count: int,
    prepared_archive: object,
    trial: _DatabaseTrial,
    phase_seconds_by_name: dict[str, float],
    phase_values_by_name: dict[str, int],
    machine: dict[str, int],
    total_seconds: float,
) -> dict[str, object]:
    """Apply scale ceilings and build one value-safe machine-readable report."""

    committed_total_bytes = sum(
        table_sizes["total_bytes"] for table_sizes in trial.relation_sizes.values()
    )
    peak_rss_bytes = _peak_rss_bytes()
    temp_bytes = trial.after_counters["temp_bytes"] - trial.before_counters["temp_bytes"]
    assert peak_rss_bytes <= 2 * 1024**3
    assert phase_values_by_name["stage_peak_bytes"] <= 8 * 1024 * row_count
    assert committed_total_bytes <= 10 * 1024 * row_count
    assert trial.admission_backend_wal["wal_bytes"] <= 24 * 1024 * row_count
    assert temp_bytes <= 4 * 1024 * row_count
    assert total_seconds <= 90 * 60
    assert phase_seconds_by_name["admission"] <= 60 * 60
    return {
        "row_count": row_count,
        "benchmark_scope": "six_field_registry_admission_and_validator_v1",
        "projected_count": trial.projected_count,
        "excluded_count": trial.excluded_count,
        "archive_bytes": prepared_archive.retained.artifact_byte_count,
        "primary_csv_bytes": next(
            member.uncompressed_size
            for member in prepared_archive.layout.members
            if member.name == prepared_archive.layout.primary_member_name
        ),
        "phase_seconds": phase_seconds_by_name,
        "phase_values": phase_values_by_name,
        "total_seconds": total_seconds,
        "peak_rss_bytes": peak_rss_bytes,
        "committed_total_bytes": committed_total_bytes,
        "admission_backend_wal": trial.admission_backend_wal,
        "idempotent_backend_wal": trial.idempotent_backend_wal,
        "idempotent_read_only": True,
        "checkpointer_delta": _checkpointer_delta(trial),
        "temp_bytes": temp_bytes,
        "settings": trial.settings_by_name,
        "machine": machine,
        "relation_sizes": trial.relation_sizes,
        "explain": trial.explain_by_name,
    }


@pytest.mark.asyncio
async def test_nppes_registry_admission_scale(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Measure six-field admission and validation through the public writer."""

    row_count = _requested_row_count()
    require_loopback_test_database()
    machine = machine_preflight(tmp_path)
    archive_name, primary_end = _benchmark_archive_spec(row_count)
    phase_seconds_by_name: dict[str, float] = {}
    phase_values_by_name: dict[str, int] = {}
    _install_phase_instrumentation(
        monkeypatch,
        phase_seconds_by_name,
        phase_values_by_name,
    )
    total_started_at = time.perf_counter()
    archive_started_at = time.perf_counter()
    prepared_archive = prepared_sized_archive(
        tmp_path, archive_name, primary_end, row_count
    )
    phase_seconds_by_name["archive_generation"] = (
        time.perf_counter() - archive_started_at
    )
    replay_started_at = time.perf_counter()
    prepared_replay = await prepare_nppes_registry_replay(
        prepared_archive,
        required_config(),
    )
    phase_seconds_by_name["prepare_replay"] = time.perf_counter() - replay_started_at
    trial = await _run_database_trial(
        prepared_replay,
        row_count,
        phase_seconds_by_name,
    )
    report_by_name = _build_scale_report(
        row_count,
        prepared_archive,
        trial,
        phase_seconds_by_name,
        phase_values_by_name,
        machine,
        time.perf_counter() - total_started_at,
    )
    print("NPPES_ADMISSION_SCALE_RESULT=" + json.dumps(report_by_name, sort_keys=True))
