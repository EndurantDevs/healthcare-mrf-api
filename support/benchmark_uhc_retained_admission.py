#!/usr/bin/env python3
"""Benchmark fresh-output UHC retention through committed PostgreSQL admission."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import statistics
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from alembic.migration import MigrationContext
from alembic.operations import Operations
import asyncpg
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from process.uhc_retained_source_registry import admit_retained_source
from support.uhc_retained_benchmark_catalog import (
    benchmark_source_binding,
    insert_benchmark_catalog,
)

MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / "20260720130000_uhc_retained_artifact_admission.py"
)
_SAFE_SCHEMA_RE = re.compile(r"mrf_uhc_bench_[a-z0-9_]{8,40}")
_DISPOSABLE_DATABASE_RE = re.compile(
    r"uhc_retained_admission_(?:test|benchmark)_[a-z0-9_]{8,}"
)
_DEFAULT_MIN_ROWS_PER_SECOND = 100_000.0


class BenchmarkFailure(RuntimeError):
    """Raised when the benchmark cannot prove its correctness or speed gate."""


@dataclass(frozen=True)
class _TrialRequest:
    dsn: str
    source_path: Path
    artifact_sha256: str
    byte_count: int
    collection_kind: str
    artifact_root_base: Path
    range_count: int
    trial_ordinal: int
    keep_artifacts: bool


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("provider_path", type=Path)
    parser.add_argument("--plan-path", type=Path)
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--scanner-bin", type=Path, required=True)
    parser.add_argument("--artifact-root-base", type=Path, required=True)
    parser.add_argument("--range-count", type=int, default=4)
    parser.add_argument("--trials", type=int, default=8)
    parser.add_argument(
        "--minimum-rows-per-second",
        type=float,
        default=_DEFAULT_MIN_ROWS_PER_SECOND,
    )
    parser.add_argument("--keep-artifacts", action="store_true")
    return parser


def _source_identity(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    byte_count = 0
    with path.open("rb") as source_file:
        while chunk := source_file.read(1024 * 1024):
            digest.update(chunk)
            byte_count += len(chunk)
    if byte_count <= 0:
        raise BenchmarkFailure(f"source is empty: {path}")
    return digest.hexdigest(), byte_count


def _validate_dsn(dsn: str) -> None:
    database_url = make_url(dsn)
    database_name = str(database_url.database or "")
    if (
        not database_url.drivername.startswith("postgresql")
        or not database_url.host
        or not database_url.username
        or not _DISPOSABLE_DATABASE_RE.fullmatch(database_name)
    ):
        raise BenchmarkFailure(
            "benchmark DSN must name an explicit disposable PostgreSQL database"
        )


def _load_migration():
    spec = importlib.util.spec_from_file_location(
        "uhc_retained_benchmark_migration",
        MIGRATION_PATH,
    )
    if spec is None or spec.loader is None:
        raise BenchmarkFailure("UHC admission migration could not be loaded")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _upgrade_on_connection(sync_connection, migration) -> None:
    migration_context = MigrationContext.configure(sync_connection)
    migration.op = Operations(migration_context)
    migration.upgrade()


async def _prepare_schema(dsn: str, schema: str) -> None:
    if not _SAFE_SCHEMA_RE.fullmatch(schema):
        raise BenchmarkFailure(f"unsafe benchmark schema: {schema}")
    engine = create_async_engine(make_url(dsn).set(drivername="postgresql+asyncpg"))
    migration = _load_migration()
    try:
        async with engine.begin() as connection:
            await connection.exec_driver_sql(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            await connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
            await connection.exec_driver_sql(
                f'''CREATE TABLE "{schema}"."provider_directory_uhc_catalog_set" (
                        catalog_set_sha256 varchar(64) PRIMARY KEY
                    )'''
            )
            await connection.exec_driver_sql(
                f'''CREATE TABLE "{schema}"."provider_directory_uhc_catalog_file" (
                        catalog_set_sha256 varchar(64) NOT NULL,
                        file_id varchar(64) NOT NULL,
                        family varchar(8) NOT NULL,
                        collection_kind varchar(32) NOT NULL,
                        file_name varchar(256) NOT NULL,
                        source_url text NOT NULL,
                        catalog_modified_at varchar(64) NOT NULL,
                        catalog_entry_sha256 varchar(64) NOT NULL,
                        size_bytes bigint,
                        availability varchar(32) NOT NULL,
                        catalog_support varchar(32) NOT NULL,
                        PRIMARY KEY (catalog_set_sha256, file_id),
                        FOREIGN KEY (catalog_set_sha256)
                            REFERENCES "{schema}".
                            "provider_directory_uhc_catalog_set"
                            (catalog_set_sha256)
                    )'''
            )
            await connection.run_sync(
                lambda sync_connection: _upgrade_on_connection(
                    sync_connection,
                    migration,
                )
            )
    finally:
        await engine.dispose()


async def _drop_schema(dsn: str, schema: str) -> None:
    if not _SAFE_SCHEMA_RE.fullmatch(schema):
        raise BenchmarkFailure(f"unsafe benchmark schema: {schema}")
    connection = await asyncpg.connect(dsn)
    try:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await connection.close()


async def _verify_committed_counts(
    connection: asyncpg.Connection,
    schema: str,
    range_count: int,
) -> dict[str, int]:
    counts = await connection.fetchrow(
        f'''SELECT
            (SELECT count(*) FROM "{schema}".
                provider_directory_uhc_raw_artifact) AS raw_count,
            (SELECT count(*) FROM "{schema}".
                provider_directory_uhc_raw_layout) AS layout_count,
            (SELECT count(*) FROM "{schema}".
                provider_directory_uhc_source_binding) AS binding_count,
            (SELECT count(*) FROM "{schema}".
                provider_directory_uhc_raw_range) AS range_count,
            (SELECT count(*) FROM "{schema}".
                provider_directory_uhc_artifact_reference) AS reference_count'''
    )
    expected_count_map = {
        "raw_count": 1,
        "layout_count": 1,
        "binding_count": 1,
        "range_count": range_count,
        "reference_count": 2,
    }
    if dict(counts) != expected_count_map:
        raise BenchmarkFailure(
            f"committed proof counts differ: {dict(counts)} != {expected_count_map}"
        )
    return expected_count_map


async def _execute_trial(
    request: _TrialRequest,
    *,
    schema: str,
    connection: asyncpg.Connection,
) -> dict[str, object]:
    binding = benchmark_source_binding(
        source_path=request.source_path,
        artifact_sha256=request.artifact_sha256,
        byte_count=request.byte_count,
        collection_kind=request.collection_kind,
        trial_label=f"{request.collection_kind}:{schema.removeprefix('mrf_uhc_bench_')}",
    )
    await insert_benchmark_catalog(connection, schema, binding)
    started = time.perf_counter()
    verified_source = await admit_retained_source(
        connection,
        binding=binding,
        source_path=request.source_path,
        expected_sha256=request.artifact_sha256,
        expected_byte_count=request.byte_count,
        range_count=request.range_count,
    )
    committed_seconds = time.perf_counter() - started
    expected_count_map = await _verify_committed_counts(
        connection,
        schema,
        request.range_count,
    )
    if verified_source.raw_reused or verified_source.manifest_reused:
        raise BenchmarkFailure("fresh-output trial unexpectedly reused retained artifacts")
    rows_per_second = verified_source.raw_artifact.record_count / committed_seconds
    return {
        "trial": request.trial_ordinal,
        "collection_kind": request.collection_kind,
        "record_count": verified_source.raw_artifact.record_count,
        "committed_seconds": committed_seconds,
        "rows_per_second": rows_per_second,
        "native_timings_seconds": dict(verified_source.timings_seconds),
        "raw_reused": verified_source.raw_reused,
        "manifest_reused": verified_source.manifest_reused,
        "database_counts": expected_count_map,
    }


async def _cleanup_trial(
    request: _TrialRequest,
    *,
    schema: str,
    trial_root: Path,
    connection: asyncpg.Connection | None,
    has_schema_creation_started: bool,
    primary_error: BaseException | None,
) -> None:
    cleanup_errors = []
    if connection is not None:
        try:
            await connection.close()
        except BaseException as error:
            cleanup_errors.append(f"close connection: {error}")
    if has_schema_creation_started:
        try:
            await _drop_schema(request.dsn, schema)
        except BaseException as error:
            cleanup_errors.append(f"drop schema {schema}: {error}")
    if not request.keep_artifacts and trial_root.exists():
        try:
            shutil.rmtree(trial_root)
        except BaseException as error:
            cleanup_errors.append(f"remove artifact root {trial_root}: {error}")
    if not cleanup_errors:
        return
    cleanup_message = "; ".join(cleanup_errors)
    if primary_error is not None:
        primary_error.add_note(f"benchmark cleanup also failed: {cleanup_message}")
        return
    raise BenchmarkFailure(f"benchmark cleanup failed: {cleanup_message}")


async def _run_trial(request: _TrialRequest) -> dict[str, object]:
    nonce = hashlib.sha256(
        f"{os.getpid()}:{request.collection_kind}:{request.trial_ordinal}".encode()
    ).hexdigest()[:12]
    schema = f"mrf_uhc_bench_{nonce}"
    trial_root = request.artifact_root_base / f"{request.collection_kind}-{nonce}"
    trial_root.mkdir(parents=True, exist_ok=False)
    os.environ["HLTHPRT_DB_SCHEMA"] = schema
    os.environ["HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT"] = str(trial_root)
    connection = None
    has_schema_creation_started = False
    primary_error: BaseException | None = None
    try:
        has_schema_creation_started = True
        await _prepare_schema(request.dsn, schema)
        connection = await asyncpg.connect(request.dsn)
        return await _execute_trial(
            request,
            schema=schema,
            connection=connection,
        )
    except BaseException as error:
        primary_error = error
        raise
    finally:
        await _cleanup_trial(
            request,
            schema=schema,
            trial_root=trial_root,
            connection=connection,
            has_schema_creation_started=has_schema_creation_started,
            primary_error=primary_error,
        )


def _distribution(values: list[float]) -> dict[str, float]:
    """Return the complete min/median/max evidence for one measured phase."""

    return {
        "minimum": min(values),
        "median": statistics.median(values),
        "maximum": max(values),
    }


def _phase_distributions(
    trials: list[dict[str, object]],
) -> dict[str, dict[str, float]]:
    """Summarize native phases and measured post-native admission overhead."""

    timing_maps = [
        trial["native_timings_seconds"]
        for trial in trials
        if isinstance(trial["native_timings_seconds"], dict)
    ]
    if len(timing_maps) != len(trials):
        raise BenchmarkFailure("native timing evidence is incomplete")
    common_names = set(timing_maps[0])
    for timing_map in timing_maps[1:]:
        common_names.intersection_update(timing_map)
    phase_distribution_map = {
        f"native_{name}_seconds": _distribution(
            [float(timing_map[name]) for timing_map in timing_maps]
        )
        for name in sorted(common_names)
    }
    if "total" not in common_names:
        raise BenchmarkFailure("native total timing evidence is missing")
    phase_distribution_map["full_committed_seconds"] = _distribution(
        [float(trial["committed_seconds"]) for trial in trials]
    )
    phase_distribution_map["subprocess_python_database_overhead_seconds"] = (
        _distribution(
            [
                float(trial["committed_seconds"])
                - float(timing_map["total"])
                for trial, timing_map in zip(trials, timing_maps, strict=True)
            ]
        )
    )
    return phase_distribution_map


def _validate_benchmark_inputs(
    args: argparse.Namespace,
) -> tuple[Path, Path, Path]:
    provider_path = args.provider_path.resolve()
    scanner_binary = args.scanner_bin.resolve()
    artifact_root_base = args.artifact_root_base.resolve()
    if not provider_path.is_file():
        raise BenchmarkFailure(f"provider sample is missing: {provider_path}")
    if not scanner_binary.is_file() or not os.access(scanner_binary, os.X_OK):
        raise BenchmarkFailure(f"native scanner is unavailable: {scanner_binary}")
    _validate_dsn(args.dsn)
    if args.trials < 8 or args.range_count < 4 or args.range_count > 256:
        raise BenchmarkFailure("benchmark trials or range count is invalid")
    if args.minimum_rows_per_second <= 0:
        raise BenchmarkFailure("minimum rows/sec must be positive")
    artifact_root_base.mkdir(parents=True, exist_ok=True)
    os.environ["HLTHPRT_PTG2_RUST_SCANNER_BIN"] = str(scanner_binary)
    return provider_path, scanner_binary, artifact_root_base


async def _run_provider_trials(
    args: argparse.Namespace,
    *,
    provider_path: Path,
    artifact_root_base: Path,
) -> tuple[list[dict[str, object]], str, int]:
    provider_sha256, provider_bytes = _source_identity(provider_path)
    provider_trials = []
    for trial_ordinal in range(args.trials):
        provider_trials.append(
            await _run_trial(
                _TrialRequest(
                    dsn=args.dsn,
                    source_path=provider_path,
                    artifact_sha256=provider_sha256,
                    byte_count=provider_bytes,
                    collection_kind="provider_membership",
                    artifact_root_base=artifact_root_base,
                    range_count=args.range_count,
                    trial_ordinal=trial_ordinal,
                    keep_artifacts=args.keep_artifacts,
                )
            )
        )
    return provider_trials, provider_sha256, provider_bytes


async def _run_plan_trial(
    args: argparse.Namespace,
    artifact_root_base: Path,
) -> dict[str, object] | None:
    if args.plan_path is None:
        return None
    plan_path = args.plan_path.resolve()
    if not plan_path.is_file():
        raise BenchmarkFailure(f"plan sample is missing: {plan_path}")
    plan_sha256, plan_bytes = _source_identity(plan_path)
    plan_trial = await _run_trial(
        _TrialRequest(
            dsn=args.dsn,
            source_path=plan_path,
            artifact_sha256=plan_sha256,
            byte_count=plan_bytes,
            collection_kind="plan_reference",
            artifact_root_base=artifact_root_base,
            range_count=args.range_count,
            trial_ordinal=args.trials,
            keep_artifacts=args.keep_artifacts,
        )
    )
    if plan_trial["record_count"] != 24:
        raise BenchmarkFailure(
            "real UHC plan proof must contain exactly 24 retained records"
        )
    return plan_trial


async def _run_benchmark(args: argparse.Namespace) -> dict[str, object]:
    provider_path, _scanner_binary, artifact_root_base = _validate_benchmark_inputs(
        args
    )
    provider_trials, _provider_sha256, provider_bytes = await _run_provider_trials(
        args,
        provider_path=provider_path,
        artifact_root_base=artifact_root_base,
    )
    rates = [float(trial["rows_per_second"]) for trial in provider_trials]
    report_map: dict[str, object] = {
        "record_kind": "uhc_retained_admission_benchmark",
        "provider_byte_count": provider_bytes,
        "range_count": args.range_count,
        "minimum_rows_per_second_gate": args.minimum_rows_per_second,
        "full_fresh_output_committed": {
            "cache_condition": "fresh_output_source_page_cache_warm_after_prehash",
            "minimum_rows_per_second": min(rates),
            "median_rows_per_second": statistics.median(rates),
            "maximum_rows_per_second": max(rates),
            "phase_seconds": _phase_distributions(provider_trials),
            "trials": provider_trials,
        },
    }
    plan_trial = await _run_plan_trial(args, artifact_root_base)
    if plan_trial is not None:
        report_map["plan_reference"] = plan_trial
    report_map["gate_passed"] = min(rates) >= args.minimum_rows_per_second
    return report_map


def main() -> int:
    """Run the committed admission benchmark and emit one JSON report."""

    args = _parser().parse_args()
    try:
        report_map = asyncio.run(_run_benchmark(args))
    except BenchmarkFailure as error:
        print(json.dumps({"record_kind": "uhc_retained_admission_failure", "error": str(error)}))
        return 1
    print(json.dumps(report_map, sort_keys=True))
    return 0 if report_map["gate_passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
