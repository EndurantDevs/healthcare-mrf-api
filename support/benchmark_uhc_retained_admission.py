#!/usr/bin/env python3
"""Benchmark cold native UHC retention through committed PostgreSQL admission."""

from __future__ import annotations

import argparse
import asyncio
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

from process.uhc_retained_registry_contract import SourceBinding
from process.uhc_retained_source_registry import (
    _expected_catalog_file_hash_pair,
    admit_retained_source,
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


def _binding(
    *,
    source_path: Path,
    artifact_sha256: str,
    byte_count: int,
    collection_kind: str,
    trial_label: str,
) -> SourceBinding:
    file_name = source_path.name
    category = "plans" if collection_kind == "plan_reference" else "providers"
    source_url = (
        f"https://providermrf.uhc.com/api/stream/ui/ifp/{category}/{file_name}"
    )
    catalog_modified_at = "2026-07-20T08:00:00Z"
    catalog_entry_sha256, source_file_id = _expected_catalog_file_hash_pair(
        family="ifp",
        collection_kind=collection_kind,
        file_name=file_name,
        source_url=source_url,
        catalog_modified_at=catalog_modified_at,
        size_bytes=byte_count,
    )
    return SourceBinding(
        catalog_set_sha256=hashlib.sha256(trial_label.encode()).hexdigest(),
        source_file_id=source_file_id,
        family="ifp",
        collection_kind=collection_kind,
        file_name=file_name,
        source_url=source_url,
        catalog_modified_at=catalog_modified_at,
        size_bytes=byte_count,
        catalog_entry_sha256=catalog_entry_sha256,
        artifact_sha256=artifact_sha256,
    )


async def _insert_catalog(
    connection: asyncpg.Connection,
    schema: str,
    binding: SourceBinding,
) -> None:
    await connection.execute(
        f'''INSERT INTO "{schema}"."provider_directory_uhc_catalog_set"
                (catalog_set_sha256) VALUES ($1)''',
        binding.catalog_set_sha256,
    )
    await connection.execute(
        f'''INSERT INTO "{schema}"."provider_directory_uhc_catalog_file" (
                catalog_set_sha256, file_id, family, collection_kind,
                file_name, source_url, catalog_modified_at,
                catalog_entry_sha256, size_bytes,
                availability, catalog_support
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9,
                      'published', 'cataloged')''',
        binding.catalog_set_sha256,
        binding.source_file_id,
        binding.family,
        binding.collection_kind,
        binding.file_name,
        binding.source_url,
        binding.catalog_modified_at,
        binding.catalog_entry_sha256,
        binding.size_bytes,
    )


async def _run_trial(
    *,
    dsn: str,
    source_path: Path,
    artifact_sha256: str,
    byte_count: int,
    collection_kind: str,
    artifact_root_base: Path,
    range_count: int,
    trial_ordinal: int,
    keep_artifacts: bool,
) -> dict[str, object]:
    nonce = hashlib.sha256(
        f"{os.getpid()}:{collection_kind}:{trial_ordinal}".encode()
    ).hexdigest()[:12]
    schema = f"mrf_uhc_bench_{nonce}"
    trial_root = artifact_root_base / f"{collection_kind}-{nonce}"
    trial_root.mkdir(parents=True, exist_ok=False)
    os.environ["HLTHPRT_DB_SCHEMA"] = schema
    os.environ["HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT"] = str(trial_root)
    connection = None
    schema_attempted = False
    primary_error: BaseException | None = None
    try:
        schema_attempted = True
        await _prepare_schema(dsn, schema)
        connection = await asyncpg.connect(dsn)
        binding = _binding(
            source_path=source_path,
            artifact_sha256=artifact_sha256,
            byte_count=byte_count,
            collection_kind=collection_kind,
            trial_label=f"{collection_kind}:{nonce}",
        )
        await _insert_catalog(connection, schema, binding)
        started = time.perf_counter()
        source = await admit_retained_source(
            connection,
            binding=binding,
            source_path=source_path,
            expected_sha256=artifact_sha256,
            expected_byte_count=byte_count,
            range_count=range_count,
        )
        committed_seconds = time.perf_counter() - started
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
        expected_counts = {
            "raw_count": 1,
            "layout_count": 1,
            "binding_count": 1,
            "range_count": range_count,
            "reference_count": 2,
        }
        if dict(counts) != expected_counts:
            raise BenchmarkFailure(
                f"committed proof counts differ: {dict(counts)} != {expected_counts}"
            )
        if source.raw_reused or source.manifest_reused:
            raise BenchmarkFailure("cold trial unexpectedly reused retained artifacts")
        rows_per_second = source.raw_artifact.record_count / committed_seconds
        return {
            "trial": trial_ordinal,
            "collection_kind": collection_kind,
            "record_count": source.raw_artifact.record_count,
            "committed_seconds": committed_seconds,
            "rows_per_second": rows_per_second,
            "native_timings_seconds": dict(source.timings_seconds),
            "raw_reused": source.raw_reused,
            "manifest_reused": source.manifest_reused,
            "database_counts": expected_counts,
        }
    except BaseException as error:
        primary_error = error
        raise
    finally:
        cleanup_errors = []
        if connection is not None:
            try:
                await connection.close()
            except BaseException as error:
                cleanup_errors.append(f"close connection: {error}")
        if schema_attempted:
            try:
                await _drop_schema(dsn, schema)
            except BaseException as error:
                cleanup_errors.append(f"drop schema {schema}: {error}")
        if not keep_artifacts and trial_root.exists():
            try:
                shutil.rmtree(trial_root)
            except BaseException as error:
                cleanup_errors.append(f"remove artifact root {trial_root}: {error}")
        if cleanup_errors:
            cleanup_message = "; ".join(cleanup_errors)
            if primary_error is not None:
                primary_error.add_note(f"benchmark cleanup also failed: {cleanup_message}")
            else:
                raise BenchmarkFailure(
                    f"benchmark cleanup failed: {cleanup_message}"
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
    phase_distributions = {
        f"native_{name}_seconds": _distribution(
            [float(timing_map[name]) for timing_map in timing_maps]
        )
        for name in sorted(common_names)
    }
    if "total" not in common_names:
        raise BenchmarkFailure("native total timing evidence is missing")
    phase_distributions["full_committed_seconds"] = _distribution(
        [float(trial["committed_seconds"]) for trial in trials]
    )
    phase_distributions["subprocess_python_database_overhead_seconds"] = (
        _distribution(
            [
                float(trial["committed_seconds"])
                - float(timing_map["total"])
                for trial, timing_map in zip(trials, timing_maps, strict=True)
            ]
        )
    )
    return phase_distributions


async def _run(args: argparse.Namespace) -> dict[str, object]:
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
    provider_sha256, provider_bytes = _source_identity(provider_path)
    provider_trials = []
    for trial_ordinal in range(args.trials):
        provider_trials.append(
            await _run_trial(
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
    rates = [float(trial["rows_per_second"]) for trial in provider_trials]
    report: dict[str, object] = {
        "record_kind": "uhc_retained_admission_benchmark",
        "provider_path": str(provider_path),
        "provider_sha256": provider_sha256,
        "provider_byte_count": provider_bytes,
        "range_count": args.range_count,
        "minimum_rows_per_second_gate": args.minimum_rows_per_second,
        "full_cold_committed": {
            "cache_condition": "fresh_output_source_page_cache_warm_after_prehash",
            "minimum_rows_per_second": min(rates),
            "median_rows_per_second": statistics.median(rates),
            "maximum_rows_per_second": max(rates),
            "phase_seconds": _phase_distributions(provider_trials),
            "trials": provider_trials,
        },
    }
    if args.plan_path is not None:
        plan_path = args.plan_path.resolve()
        if not plan_path.is_file():
            raise BenchmarkFailure(f"plan sample is missing: {plan_path}")
        plan_sha256, plan_bytes = _source_identity(plan_path)
        plan_trial = await _run_trial(
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
        if plan_trial["record_count"] != 24:
            raise BenchmarkFailure(
                "real UHC plan proof must contain exactly 24 retained records"
            )
        report["plan_reference"] = plan_trial
    report["gate_passed"] = min(rates) >= args.minimum_rows_per_second
    return report


def main() -> int:
    args = _parser().parse_args()
    try:
        report = asyncio.run(_run(args))
    except BenchmarkFailure as error:
        print(json.dumps({"record_kind": "uhc_retained_admission_failure", "error": str(error)}))
        return 1
    print(json.dumps(report, sort_keys=True))
    return 0 if report["gate_passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
