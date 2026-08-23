# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic end-to-end benchmark for authenticated PTG V4 NPI extraction."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import time
import uuid

import asyncpg

sys.path.insert(0, str(Path(__file__).parents[1]))

from process.ptg_parts import ptg2_v4_graph_compiler as compiler
from tests.ptg2_v4_graph_compiler_test_support import (
    _DENSE_FORMAT,
    _global,
    _write_npi_scope,
    compiler_fixture,
)


BASE_NPI = 1_000_000_000
PG_COPY_HEADER = b"PGCOPY\n\xff\r\n\0" + struct.pack(">II", 0, 0)


def _inputs() -> tuple[Path, Path, str, int, int, int, int]:
    event_path = Path(os.environ["ENDURANT_BENCHMARK_EVENT_PATH"])
    root = Path(__file__).parents[1].resolve()
    binary = root / "support/ptg2_scanner/target/release/ptg2_provider_graph_v4"
    dsn = os.getenv(
        "HLTHPRT_PTG_NPI_SCOPE_BENCHMARK_DSN",
        "postgresql://127.0.0.1:5440/postgres",
    )
    shards = int(os.getenv("HLTHPRT_PTG_NPI_SCOPE_BENCHMARK_SHARDS", "192"))
    rows_per_shard = int(
        os.getenv("HLTHPRT_PTG_NPI_SCOPE_BENCHMARK_ROWS_PER_SHARD", "40000")
    )
    universe = int(os.getenv("HLTHPRT_PTG_NPI_SCOPE_BENCHMARK_UNIVERSE", "1500000"))
    samples = int(os.getenv("HLTHPRT_PTG_NPI_SCOPE_BENCHMARK_SAMPLES", "3"))
    if (
        "127.0.0.1:5440/" not in dsn
        or shards < 2
        or rows_per_shard < 1
        or universe < rows_per_shard
        or BASE_NPI + universe > 9_999_999_999
        or samples < 3
        or samples % 2 == 0
    ):
        raise RuntimeError("invalid isolated PTG NPI-scope benchmark inputs")
    return event_path, binary, dsn, shards, rows_per_shard, universe, samples


def _write_dense_reciprocal(
    path: Path,
    *,
    shard_id: str,
    npis: range,
) -> dict[str, object]:
    payload = bytearray(b"PTG2MNDS")
    payload.extend(struct.pack("<IQQ", 1, len(npis), 1))
    for offset, npi in enumerate(npis):
        payload.extend(bytes(8))
        payload.extend(npi.to_bytes(8, "big"))
        payload.extend(struct.pack("<QI", offset, 1))
    payload.extend(_global(3, 1))
    payload.extend(bytes(4 * len(npis)))
    path.write_bytes(payload)
    return {
        "name": "provider_npi_group",
        "source_shard_id": shard_id,
        "path": str(path),
        "record_format": _DENSE_FORMAT,
        "sha256": hashlib.sha256(payload).hexdigest(),
        "byte_count": len(payload),
        "owner_count": len(npis),
        "member_count": len(npis),
        "member_global_count": 1,
    }


def _merged_intervals(intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    merged: list[tuple[int, int]] = []
    for start, end in sorted(intervals):
        if merged and start <= merged[-1][1] + 1:
            merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
        else:
            merged.append((start, end))
    return merged


def _expected_output(intervals: list[tuple[int, int]]) -> dict[str, object]:
    digest = hashlib.sha256(PG_COPY_HEADER)
    key = 0
    npi_sum = 0
    for start, end in intervals:
        for npi in range(start, end + 1):
            digest.update(struct.pack(">hIiIq", 2, 4, key, 8, npi))
            key += 1
            npi_sum += npi
    digest.update(struct.pack(">h", -1))
    return {
        "row_count": key,
        "byte_count": len(PG_COPY_HEADER) + key * 22 + 2,
        "sha256": digest.hexdigest(),
        "min_npi": intervals[0][0],
        "max_npi": intervals[-1][1],
        "npi_sum": str(npi_sum),
    }


def _workload(
    root: Path,
    *,
    shard_count: int,
    rows_per_shard: int,
    universe: int,
) -> tuple[tuple[dict[str, object], ...], dict[str, object]]:
    artifacts: list[dict[str, object]] = []
    intervals: list[tuple[int, int]] = []
    start_modulus = universe - rows_per_shard + 1
    for index in range(shard_count):
        shard_id = f"synthetic-{index:04d}"
        shard_root = root / shard_id
        shard_artifacts, _provider_map = compiler_fixture(
            shard_root,
            shard_id=shard_id,
        )
        start = BASE_NPI + ((index * 7_919) % start_modulus)
        npis = range(start, start + rows_per_shard)
        reciprocal = _write_dense_reciprocal(
            shard_root / "npi-group.sidecar",
            shard_id=shard_id,
            npis=npis,
        )
        scope = _write_npi_scope(
            shard_root / "npi-scope.copy",
            shard_id=shard_id,
            reciprocal=reciprocal,
            npis=list(npis),
        )
        artifacts.extend(
            (
                reciprocal
                if entry["name"] == "provider_npi_group"
                else scope if entry["name"] == "provider_npi_scope" else entry
            )
            for entry in shard_artifacts
        )
        intervals.append((start, start + rows_per_shard - 1))
    expected = _expected_output(_merged_intervals(intervals))
    expected["source_row_count"] = shard_count * rows_per_shard
    return tuple(artifacts), expected


async def _quiet_progress(**_payload: object) -> None:
    return None


def _measure_sync(original, seconds: dict[str, float], name: str, *args, **kwargs):
    started = time.perf_counter()
    try:
        return original(*args, **kwargs)
    finally:
        seconds[name] = time.perf_counter() - started


async def _measured_npi_scope_process(
    original,
    seconds: list[float],
    *args,
    **kwargs,
) -> None:
    started = time.perf_counter()
    try:
        await original(*args, **kwargs)
    finally:
        seconds.append(time.perf_counter() - started)


async def _postgres_proof(
    dsn: str,
    copy_path: Path,
) -> tuple[dict[str, object], float]:
    connection = await asyncpg.connect(dsn)
    schema = f"ptg_npi_scope_bench_{uuid.uuid4().hex}"
    quoted_schema = '"' + schema + '"'
    try:
        await connection.execute(f"CREATE SCHEMA {quoted_schema}")
        await connection.execute(
            f"CREATE UNLOGGED TABLE {quoted_schema}.scope "
            "(key integer NOT NULL, npi bigint NOT NULL)"
        )
        started = time.perf_counter()
        with copy_path.open("rb") as source:
            await connection.copy_to_table(
                "scope",
                source=source,
                schema_name=schema,
                columns=("key", "npi"),
                format="binary",
            )
        copied_seconds = time.perf_counter() - started
        row = await connection.fetchrow(
            f"""
            SELECT count(*)::bigint AS row_count,
                   min(key)::bigint AS min_key,
                   max(key)::bigint AS max_key,
                   min(npi)::bigint AS min_npi,
                   max(npi)::bigint AS max_npi,
                   sum(npi::numeric)::text AS npi_sum,
                   count(*) FILTER (
                       WHERE key <> ordinal OR (previous_npi IS NOT NULL AND npi <= previous_npi)
                   )::bigint AS sequence_violations
              FROM (
                    SELECT key,
                           npi,
                           row_number() OVER (ORDER BY key) - 1 AS ordinal,
                           lag(npi) OVER (ORDER BY key) AS previous_npi
                      FROM {quoted_schema}.scope
                   ) AS ordered_scope
            """
        )
        return dict(row), copied_seconds
    finally:
        try:
            await connection.execute(f"DROP SCHEMA IF EXISTS {quoted_schema} CASCADE")
            if await connection.fetchval(
                "SELECT to_regnamespace($1) IS NOT NULL", schema
            ):
                raise RuntimeError("benchmark PostgreSQL schema cleanup failed")
        finally:
            await connection.close()


async def _sample(
    root: Path,
    *,
    binary: Path,
    dsn: str,
    artifacts: tuple[dict[str, object], ...],
    expected: dict[str, object],
    sample: int,
) -> dict[str, object]:
    output = root / f"scope-{sample}.copy"
    original_progress = compiler._emit_npi_scope_progress
    original_process = compiler._run_npi_scope_process
    original_normalize = compiler._normalized_source_npi_scope_entries
    original_manifest = compiler._build_v4_graph_manifest_shards
    phase_seconds: dict[str, float] = {}
    process_seconds: list[float] = []
    compiler._emit_npi_scope_progress = _quiet_progress
    compiler._run_npi_scope_process = (
        lambda *args, **kwargs: _measured_npi_scope_process(
            original_process,
            process_seconds,
            *args,
            **kwargs,
        )
    )
    compiler._normalized_source_npi_scope_entries = (
        lambda *args, **kwargs: _measure_sync(
            original_normalize,
            phase_seconds,
            "source_scope_rebuild_seconds",
            *args,
            **kwargs,
        )
    )
    compiler._build_v4_graph_manifest_shards = lambda *args, **kwargs: _measure_sync(
        original_manifest,
        phase_seconds,
        "manifest_auth_seconds",
        *args,
        **kwargs,
    )
    preparation = None
    started = time.perf_counter()
    try:
        preparation = await compiler.prepare_v4_npi_scope(
            graph_artifact_entries=artifacts,
            output_path=output,
            binary_path=binary,
        )
        preparation_seconds = time.perf_counter() - started
        if len(process_seconds) != 1 or set(phase_seconds) != {
            "source_scope_rebuild_seconds",
            "manifest_auth_seconds",
        }:
            raise RuntimeError("benchmark did not observe every NPI-scope phase once")
        with preparation.copy_path.open("rb") as output_file:
            observed_sha256 = hashlib.file_digest(output_file, "sha256").hexdigest()
        if (
            preparation.manifest["row_count"] != expected["row_count"]
            or preparation.manifest["source_owner_count"]
            != expected["source_row_count"]
            or preparation.manifest["output_byte_count"] != expected["byte_count"]
            or preparation.manifest["output_sha256"] != expected["sha256"]
            or observed_sha256 != expected["sha256"]
        ):
            raise RuntimeError("NPI-scope extraction changed exact output bytes")
        postgres, postgres_copy_seconds = await _postgres_proof(
            dsn,
            preparation.copy_path,
        )
        if (
            postgres["row_count"] != expected["row_count"]
            or postgres["min_key"] != 0
            or postgres["max_key"] != expected["row_count"] - 1
            or postgres["min_npi"] != expected["min_npi"]
            or postgres["max_npi"] != expected["max_npi"]
            or postgres["npi_sum"] != expected["npi_sum"]
            or postgres["sequence_violations"] != 0
        ):
            raise RuntimeError("PostgreSQL binary COPY changed NPI-scope semantics")
        return {
            "schema_version": 1,
            "correctness": {
                "source_row_count": expected["source_row_count"],
                "output_row_count": expected["row_count"],
                "output_byte_count": expected["byte_count"],
                "output_sha256": expected["sha256"],
                "input_sha256": preparation.manifest["input_sha256"],
                "postgres_min_npi": postgres["min_npi"],
                "postgres_max_npi": postgres["max_npi"],
                "postgres_npi_sum": postgres["npi_sum"],
                "postgres_sequence_violations": postgres["sequence_violations"],
                "postgres_schema_cleaned": True,
            },
            "metrics": {
                "npi_scope_seconds": process_seconds[0],
                "preparation_seconds": preparation_seconds,
                **phase_seconds,
                "postgres_copy_seconds": postgres_copy_seconds,
            },
        }
    finally:
        compiler._emit_npi_scope_progress = original_progress
        compiler._run_npi_scope_process = original_process
        compiler._normalized_source_npi_scope_entries = original_normalize
        compiler._build_v4_graph_manifest_shards = original_manifest
        if preparation is not None:
            preparation.cleanup()


async def _run() -> None:
    event_path, binary, dsn, shards, rows, universe, sample_count = _inputs()
    root = Path(__file__).parents[1].resolve()
    subprocess.run(
        [
            "cargo",
            "build",
            "--locked",
            "--offline",
            "--release",
            "--bin",
            "ptg2_provider_graph_v4",
            "--manifest-path",
            "support/ptg2_scanner/Cargo.toml",
        ],
        cwd=root,
        check=True,
    )
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError("release PTG V4 graph compiler is unavailable")
    with tempfile.TemporaryDirectory(prefix="ptg-npi-scope-benchmark-") as temporary:
        temporary_root = Path(temporary)
        artifacts, expected = _workload(
            temporary_root / "source",
            shard_count=shards,
            rows_per_shard=rows,
            universe=universe,
        )
        samples = [
            await _sample(
                temporary_root,
                binary=binary,
                dsn=dsn,
                artifacts=artifacts,
                expected=expected,
                sample=sample,
            )
            for sample in range(sample_count)
        ]
    correctness = samples[0]["correctness"]
    if any(sample["correctness"] != correctness for sample in samples[1:]):
        raise RuntimeError("benchmark correctness changed between samples")
    trim = sample_count // 2
    metrics = {
        name: sorted(sample["metrics"][name] for sample in samples)[trim]
        for name in samples[0]["metrics"]
    }
    event_path.write_text(
        json.dumps(
            {"schema_version": 1, "correctness": correctness, "metrics": metrics},
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    asyncio.run(_run())
