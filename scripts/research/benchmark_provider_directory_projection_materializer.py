#!/usr/bin/env python3
"""Benchmark four native FHIR projection shards through durable PostgreSQL COPY."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import resource
import statistics
import sys
import time
import uuid

import asyncpg

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from process.provider_directory_projection_db import (
    set_local_projection_synchronous_commit,
    set_local_projection_wal_compression,
)
from process.provider_directory_projection_native import (
    run_native_projection_command,
)
from process.provider_directory_projection_types import (
    ProjectionRetainedChildLease,
    ProjectionShardClaim,
    ProjectionStage,
)
from scripts.research.provider_directory_projection_benchmark_support import (
    build_benchmark_coordinates,
    projection_stage_ddl,
    retained_payload as build_retained_payload,
)


TRIAL_COUNT = 8
SHARD_COUNT = 4
ROWS_PER_SHARD = 100_000
NATIVE_THREADS = 1
MINIMUM_ROWS_PER_SECOND = 100_000.0
DISPOSABLE_DATABASE = re.compile(r"^ptg2_v3_lifecycle_test_[a-z0-9_]+$")
SAFE_SCHEMA = re.compile(r"^pd_projection_bench_[a-z0-9_]+$")


@dataclass(frozen=True)
class BenchmarkShard:
    """Exact retained bytes and claim coordinates for one independent shard."""

    payload: bytes
    claim: ProjectionShardClaim
    child: ProjectionRetainedChildLease
    stage: ProjectionStage


class _AsyncpgProjectionSettings:
    """Expose the production setting-helper interface over asyncpg."""

    def __init__(self, connection):
        self._connection = connection

    async def status(self, statement: str):
        """Execute one production setting statement."""

        return await self._connection.execute(statement)

    async def scalar(self, statement: str):
        """Return one scalar production setting result."""

        return await self._connection.fetchval(statement)


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scanner", required=True, type=Path)
    parser.add_argument(
        "--postgres-dsn",
        default=os.getenv("HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_POSTGRES_DSN"),
    )
    return parser.parse_args()


def _validate_args(args: argparse.Namespace) -> None:
    if not args.scanner.is_file() or args.scanner.parent.name != "release":
        raise SystemExit("--scanner must be an existing target/release binary")
    if not args.postgres_dsn:
        raise SystemExit("provide --postgres-dsn or the projection PostgreSQL DSN env")


async def _validated_database_name(dsn: str) -> str:
    connection = await asyncpg.connect(dsn)
    try:
        database_name = await connection.fetchval("SELECT current_database()")
    finally:
        await connection.close()
    if not DISPOSABLE_DATABASE.fullmatch(str(database_name)):
        raise SystemExit("benchmark requires a ptg2_v3_lifecycle_test_* database")
    return str(database_name)


async def _prepare_schema(pool, schema: str) -> tuple[ProjectionStage, ...]:
    if not SAFE_SCHEMA.fullmatch(schema):
        raise SystemExit("generated benchmark schema failed its safety contract")
    async with pool.acquire() as connection:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        stages = []
        for shard_ordinal in range(SHARD_COUNT):
            relation = f"stage_{shard_ordinal}"
            await connection.execute(projection_stage_ddl(schema, relation))
            relation_oid = await connection.fetchval(
                "SELECT to_regclass($1)::oid::bigint", f"{schema}.{relation}"
            )
            stages.append(ProjectionStage(schema, relation, int(relation_oid)))
        await connection.execute(
            f'''CREATE TABLE "{schema}".checkpoint (
                partition_id varchar(64) PRIMARY KEY,
                canonical_row_sha256 varchar(64) NOT NULL,
                resource_count bigint NOT NULL,
                completed_at timestamptz NOT NULL DEFAULT clock_timestamp()
            )'''
        )
    return tuple(stages)


async def _assert_stage_shape(pool, stages: tuple[ProjectionStage, ...]) -> None:
    async with pool.acquire() as connection:
        for stage in stages:
            catalog = await connection.fetchrow(
                "SELECT relpersistence, (SELECT count(*) FROM pg_index "
                "WHERE indrelid = $1::oid) FROM pg_class WHERE oid = $1::oid",
                stage.relation_oid,
            )
            persistence = catalog[0]
            if isinstance(persistence, bytes):
                persistence = persistence.decode("ascii")
            if (persistence, catalog[1]) != ("p", 0):
                raise RuntimeError("benchmark stage is not logged and index-free")


async def _clear_trial(pool, schema: str, stages) -> None:
    relations = ",".join(
        f'"{stage.schema}"."{stage.relation}"' for stage in stages
    )
    async with pool.acquire() as connection:
        await connection.execute(f"TRUNCATE {relations}, \"{schema}\".checkpoint")


async def _configure_durable_transaction(connection) -> str:
    settings_database = _AsyncpgProjectionSettings(connection)
    await set_local_projection_synchronous_commit(settings_database, "on")
    await set_local_projection_wal_compression(settings_database)
    setting_map = {
        setting_name: await connection.fetchval(
            "SELECT current_setting($1)", setting_name
        )
        for setting_name in ("synchronous_commit", "wal_compression")
    }
    if setting_map["synchronous_commit"] != "on":
        raise RuntimeError("durable transaction settings are not active")
    return setting_map["wal_compression"]


async def _native_shard_copy(connection, scanner: Path, shard: BenchmarkShard):
    async def retained_bytes():
        """Yield one exact in-memory retained block without source calls."""

        yield shard.payload

    return await run_native_projection_command(
        shard.claim,
        shard.child,
        shard.stage,
        retained_bytes(),
        framing="ndjson",
        transaction=connection,
        materializer_workers=SHARD_COUNT,
        native_threads=NATIVE_THREADS,
        executable=str(scanner),
    )


async def _checkpoint_shard(connection, shard, native_result) -> None:
    census_record = await connection.fetchrow(
        f'''WITH staged_rows AS (
                SELECT resource_type, resource_id
                  FROM "{shard.stage.schema}"."{shard.stage.relation}"
                 WHERE physical_projection_id = $1
                   AND proof_partition_id = $2
            ), resource_counts AS (
                SELECT resource_type, count(*)::bigint AS resource_count
                  FROM staged_rows GROUP BY resource_type
            )
            SELECT count(*)::bigint,
                   min(ARRAY[
                       resource_type COLLATE "C", resource_id COLLATE "C"
                   ]),
                   max(ARRAY[
                       resource_type COLLATE "C", resource_id COLLATE "C"
                   ]),
                   COALESCE((
                       SELECT jsonb_object_agg(resource_type, resource_count)
                         FROM resource_counts
                   ), '{{}}'::jsonb)::text
              FROM staged_rows''',
        shard.claim.recipe_lease.recipe.recipe_id,
        shard.claim.shard.partition_id,
    )
    expected_census = (
        native_result.record_count,
        list(native_result.shard_proof.first_identity),
        list(native_result.shard_proof.last_identity),
    )
    if tuple(census_record[:3]) != expected_census:
        raise RuntimeError("staged census does not match native proof")
    if json.loads(census_record[3]) != native_result.shard_proof.proof["resource_counts"]:
        raise RuntimeError("staged resource census does not match native proof")
    await connection.execute(
        f'''INSERT INTO "{shard.stage.schema}".checkpoint (
                partition_id, canonical_row_sha256, resource_count
            ) VALUES ($1, $2, $3)''',
        shard.claim.shard.partition_id,
        native_result.shard_proof.canonical_row_sha256,
        native_result.record_count,
    )


async def _run_shard(pool, scanner: Path, shard: BenchmarkShard):
    """Run one native COPY and commit its SQL census checkpoint atomically."""

    async with pool.acquire() as connection:
        async with connection.transaction():
            await _configure_durable_transaction(connection)
            native_result = await _native_shard_copy(connection, scanner, shard)
            await _checkpoint_shard(connection, shard, native_result)
    return native_result


async def _wal_snapshot(pool) -> dict[str, int]:
    async with pool.acquire() as connection:
        wal_record = await connection.fetchrow(
            "SELECT wal_records, wal_fpi, wal_bytes::bigint FROM pg_stat_wal"
        )
        checkpointer_record = await connection.fetchrow(
            "SELECT num_timed, num_requested, num_done, buffers_written "
            "FROM pg_stat_checkpointer"
        )
    return {
        "wal_records": int(wal_record[0]),
        "wal_fpi": int(wal_record[1]),
        "wal_bytes": int(wal_record[2]),
        "checkpoints_timed": int(checkpointer_record[0]),
        "checkpoints_requested": int(checkpointer_record[1]),
        "checkpoints_done": int(checkpointer_record[2]),
        "checkpoint_buffers_written": int(checkpointer_record[3]),
    }


async def _run_trial(pool, scanner, schema, shards, trial_ordinal):
    await _clear_trial(pool, schema, tuple(shard.stage for shard in shards))
    before_wal_map = await _wal_snapshot(pool)
    started = time.perf_counter()
    native_results = await asyncio.gather(
        *(_run_shard(pool, scanner, shard) for shard in shards)
    )
    wall_seconds = time.perf_counter() - started
    after_wal_map = await _wal_snapshot(pool)
    row_count = sum(native_result.record_count for native_result in native_results)
    rate = row_count / wall_seconds
    async with pool.acquire() as connection:
        checkpoint_count = await connection.fetchval(
            f'SELECT count(*) FROM "{schema}".checkpoint'
        )
    if checkpoint_count != SHARD_COUNT:
        raise RuntimeError("transactional checkpoint count mismatch")
    trial_report_map = {
        "record_kind": "provider_directory_projection_benchmark_trial",
        "trial": trial_ordinal,
        "row_count": row_count,
        "wall_seconds": wall_seconds,
        "rows_per_second": rate,
        "maximum_native_seconds": max(
            native_result.timings_seconds["total_before_stdout_seconds"]
            for native_result in native_results
        ),
        "postgres_delta": {
            metric_name: after_wal_map[metric_name] - before_wal_map[metric_name]
            for metric_name in before_wal_map
        },
    }
    print(json.dumps(trial_report_map, separators=(",", ":")), flush=True)
    return trial_report_map


def _maximum_child_rss_bytes() -> int:
    maximum_rss = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    return int(maximum_rss if sys.platform == "darwin" else maximum_rss * 1024)


async def _runtime_evidence(pool) -> dict[str, object]:
    async with pool.acquire() as connection:
        async with connection.transaction():
            transaction_wal_compression = await _configure_durable_transaction(
                connection
            )
        setting_records = await connection.fetch(
            "SELECT name, setting FROM pg_settings WHERE name = ANY($1::text[])",
            [
                "checkpoint_timeout",
                "fsync",
                "full_page_writes",
                "max_wal_size",
                "shared_buffers",
                "synchronous_commit",
                "wal_compression",
            ],
        )
        setting_map = dict(setting_records)
        if any(
            setting_map.get(setting_name) != "on"
            for setting_name in ("fsync", "full_page_writes", "synchronous_commit")
        ):
            raise RuntimeError("PostgreSQL durable settings are not enabled")
        return {
            "logical_cpu_count": os.cpu_count(),
            "platform": f"{sys.platform}-{platform.machine()}",
            "postgres_version": await connection.fetchval(
                "SHOW server_version"
            ),
            "postgres_settings": setting_map,
            "transaction_wal_compression": transaction_wal_compression,
        }


def _benchmark_shards(stages: tuple[ProjectionStage, ...]) -> tuple[BenchmarkShard, ...]:
    benchmark_shards = []
    for shard_ordinal in range(SHARD_COUNT):
        shard_payload = build_retained_payload(shard_ordinal, ROWS_PER_SHARD)
        claim, child = build_benchmark_coordinates(
            shard_payload,
            shard_ordinal,
            ROWS_PER_SHARD,
        )
        benchmark_shards.append(
            BenchmarkShard(
                payload=shard_payload,
                claim=claim,
                child=child,
                stage=stages[shard_ordinal],
            )
        )
    return tuple(benchmark_shards)


async def _run_benchmark(args: argparse.Namespace) -> dict:
    await _validated_database_name(args.postgres_dsn)
    pool = await asyncpg.create_pool(args.postgres_dsn, min_size=SHARD_COUNT, max_size=SHARD_COUNT)
    schema = f"pd_projection_bench_{uuid.uuid4().hex[:12]}"
    try:
        stages = await _prepare_schema(pool, schema)
        await _assert_stage_shape(pool, stages)
        runtime_evidence_map = await _runtime_evidence(pool)
        benchmark_shards = _benchmark_shards(stages)
        reports = [
            await _run_trial(
                pool,
                args.scanner,
                schema,
                benchmark_shards,
                trial_ordinal,
            )
            for trial_ordinal in range(1, TRIAL_COUNT + 1)
        ]
    finally:
        async with pool.acquire() as connection:
            await connection.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await pool.close()
    rates = [report["rows_per_second"] for report in reports]
    walls = [report["wall_seconds"] for report in reports]
    return {
        "record_kind": "provider_directory_projection_benchmark_summary",
        "contract_id": "healthporta.provider-directory.native-copy-throughput.v1",
        "input_kind": "local_synthetic_retained_ndjson",
        "scope": "mixed_eight_resource_hot_path_not_external_workload",
        "external_source_calls": 0,
        "trials": TRIAL_COUNT,
        "workers": SHARD_COUNT,
        "scanner_profile": "release",
        "scanner_sha256": _sha256(args.scanner.read_bytes()),
        "native_threads_per_worker": NATIVE_THREADS,
        "rows_per_shard": ROWS_PER_SHARD,
        "rows_per_trial": SHARD_COUNT * ROWS_PER_SHARD,
        "retained_bytes_per_shard": [
            len(benchmark_shard.payload)
            for benchmark_shard in benchmark_shards
        ],
        "maximum_child_rss_bytes": _maximum_child_rss_bytes(),
        "runtime": runtime_evidence_map,
        "rows_per_second": {
            "minimum": min(rates),
            "median": statistics.median(rates),
            "maximum": max(rates),
        },
        "wall_seconds": {
            "minimum": min(walls),
            "median": statistics.median(walls),
            "maximum": max(walls),
        },
        "minimum_required_rows_per_second": MINIMUM_ROWS_PER_SECOND,
        "passed": min(rates) >= MINIMUM_ROWS_PER_SECOND,
    }


def main() -> None:
    """Run exactly eight fresh durable trials and enforce the throughput gate."""

    args = _parse_args()
    _validate_args(args)
    summary = asyncio.run(_run_benchmark(args))
    print(json.dumps(summary, separators=(",", ":")), flush=True)
    if not summary["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
