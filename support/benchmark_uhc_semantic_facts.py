#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Benchmark bounded native UHC semantic transform through durable COPY."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import re
import statistics
import time
import uuid
from typing import Any, AsyncIterator, Mapping

import asyncpg

from process.uhc_semantic_build_store import (
    UHC_SEMANTIC_COPY_COLUMNS,
    UhcSemanticBuildClaim,
    UhcSemanticBuildIdentity,
    _stage_create_sql,
    _stage_index_sql,
    _validate_native_report,
)
from process.uhc_semantic_evidence import summarize_uhc_npi_evidence
from process.uhc_semantic_stage_verifier import verify_uhc_semantic_stage
from support.uhc_semantic_benchmark_quarantine import (
    benchmark_proof_identity as _proof_identity,
    benchmark_quarantine_source,
)


_SAFE_IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--native", type=Path, required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--catalog-set-sha256", required=True)
    parser.add_argument("--artifact-sha256", required=True)
    parser.add_argument("--artifact-byte-count", type=int, required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--range-set-sha256", required=True)
    parser.add_argument("--record-count", type=int, required=True)
    parser.add_argument("--range-count", type=int, required=True)
    parser.add_argument("--producer-build-id", required=True)
    parser.add_argument("--source-file-id", required=True)
    parser.add_argument("--source-binding-id", required=True)
    parser.add_argument(
        "--collection-kind",
        choices=("provider_membership", "plan_reference"),
        required=True,
    )
    parser.add_argument("--dsn", default=os.getenv("HLTHPRT_UHC_BENCHMARK_DSN"))
    parser.add_argument("--schema", default="public")
    parser.add_argument("--trials", type=int, default=8)
    parser.add_argument("--parallelism", type=int, default=2)
    parser.add_argument("--native-workers", type=int, default=4)
    parser.add_argument("--require-target", action="store_true")
    return parser.parse_args()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(4 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _quoted(identifier: str) -> str:
    if _SAFE_IDENTIFIER_RE.fullmatch(identifier) is None:
        raise ValueError("unsafe PostgreSQL identifier")
    return f'"{identifier}"'


def _stage_ref(schema: str, relation: str) -> str:
    return f"{_quoted(schema)}.{_quoted(relation)}"


def _native_command(
    arguments: argparse.Namespace,
    encoder_sha256: str,
) -> tuple[list[str], UhcSemanticBuildIdentity]:
    identity = UhcSemanticBuildIdentity(
        catalog_set_sha256=arguments.catalog_set_sha256,
        source_file_id=arguments.source_file_id,
        artifact_sha256=arguments.artifact_sha256,
        raw_contract_version=2,
        raw_range_count=arguments.range_count,
        manifest_sha256=arguments.manifest_sha256,
        range_set_sha256=arguments.range_set_sha256,
        raw_record_count=arguments.record_count,
        raw_producer_build_id=arguments.producer_build_id,
        collection_kind=arguments.collection_kind,
        encoder_sha256=encoder_sha256,
    )
    return (
        [
            str(arguments.native),
            "--input",
            str(arguments.input),
            "--manifest",
            str(arguments.manifest),
            "--output",
            "-",
            "--artifact-sha256",
            arguments.artifact_sha256,
            "--artifact-byte-count",
            str(arguments.artifact_byte_count),
            "--manifest-sha256",
            arguments.manifest_sha256,
            "--range-set-sha256",
            arguments.range_set_sha256,
            "--record-count",
            str(arguments.record_count),
            "--range-count",
            str(arguments.range_count),
            "--source-file-id",
            arguments.source_file_id,
            "--source-binding-id",
            arguments.source_binding_id,
            "--collection-kind",
            arguments.collection_kind,
            "--workers",
            str(arguments.native_workers),
        ],
        identity,
    )


async def _stdout_chunks(process: asyncio.subprocess.Process) -> AsyncIterator[bytes]:
    assert process.stdout is not None
    while chunk := await process.stdout.read(4 * 1024 * 1024):
        yield chunk


async def _terminate(process: asyncio.subprocess.Process | None) -> None:
    if process is None or process.returncode is not None:
        return
    process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout=5)
    except TimeoutError:
        process.kill()
        await process.wait()


async def _trial(
    arguments: argparse.Namespace,
    trial: int,
    command: list[str],
    identity: UhcSemanticBuildIdentity,
) -> dict[str, Any]:
    """Run and verify one isolated semantic landing benchmark trial."""

    connection = await asyncpg.connect(arguments.dsn)
    relation = f"provider_directory_uhc_bench_{uuid.uuid4().hex[:18]}"
    stage_ref = _stage_ref(arguments.schema, relation)
    claim = UhcSemanticBuildClaim(
        semantic_build_id=hashlib.sha256(relation.encode()).hexdigest(),
        lease_token="benchmark",
        attempt_count=1,
        stage_schema=arguments.schema,
        stage_relation=relation,
        sealed_reuse=False,
    )
    process = None
    try:
        await connection.execute(_stage_create_sql(stage_ref))
        landing_started = time.perf_counter()
        process, native_report_by_field = await _copy_native_trial(
            connection,
            relation,
            arguments.schema,
            command,
            identity,
        )
        landing_finished = time.perf_counter()
        verifier_report, evidence = await _verify_trial_stage(
            connection,
            claim,
            identity,
            native_report_by_field,
            arguments,
        )
        full_finished = time.perf_counter()
        return _trial_result(
            arguments=arguments,
            trial=trial,
            native_report_by_field=native_report_by_field,
            verifier_report=verifier_report,
            evidence=evidence,
            landing_started=landing_started,
            landing_finished=landing_finished,
            full_finished=full_finished,
        )
    finally:
        await _terminate(process)
        await connection.execute(f"DROP TABLE IF EXISTS {stage_ref}")
        await connection.close()


async def _verify_trial_stage(
    connection: asyncpg.Connection,
    claim: UhcSemanticBuildClaim,
    identity: UhcSemanticBuildIdentity,
    native_report_by_field: Mapping[str, Any],
    arguments: argparse.Namespace,
) -> tuple[dict[str, Any], Any]:
    async with connection.transaction():
        for statement in _stage_index_sql(claim):
            await connection.execute(statement)
        await connection.execute(
            f"ANALYZE {_stage_ref(arguments.schema, claim.stage_relation)}"
        )
    verifier_report = await verify_uhc_semantic_stage(
        connection,
        claim,
        identity,
        native_report_by_field,
        quarantine_source=benchmark_quarantine_source(arguments),
    )
    evidence = await summarize_uhc_npi_evidence(
        connection,
        f"{arguments.schema}.{claim.stage_relation}",
        expected_evidence_count=native_report_by_field["evidence_count"],
    )
    return verifier_report, evidence


async def _copy_native_trial(
    connection: asyncpg.Connection,
    relation: str,
    schema: str,
    command: list[str],
    identity: UhcSemanticBuildIdentity,
) -> tuple[asyncio.subprocess.Process, dict[str, Any]]:
    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    assert process.stderr is not None
    stderr_task = asyncio.create_task(process.stderr.read())
    async with connection.transaction():
        await connection.copy_to_table(
            relation,
            schema_name=schema,
            columns=UHC_SEMANTIC_COPY_COLUMNS,
            source=_stdout_chunks(process),
            format="binary",
        )
        return_code = await process.wait()
        stderr = await stderr_task
        if return_code:
            raise RuntimeError(
                f"bounded UHC native encoder failed ({return_code}): "
                + stderr.decode(errors="replace")
            )
        native_report_by_field = json.loads(stderr)
        _validate_native_report(identity, native_report_by_field)
    return process, native_report_by_field


def _trial_result(
    *,
    arguments: argparse.Namespace,
    trial: int,
    native_report_by_field: dict[str, Any],
    verifier_report: dict[str, Any],
    evidence: Any,
    landing_started: float,
    landing_finished: float,
    full_finished: float,
) -> dict[str, Any]:
    landing_elapsed = landing_finished - landing_started
    full_elapsed = full_finished - landing_started
    return {
        "trial": trial,
        "input_records": arguments.record_count,
        "copy_rows": int(native_report_by_field["fact_count"])
        + int(native_report_by_field["evidence_count"]),
        "landing_started": landing_started,
        "landing_finished": landing_finished,
        "full_finished": full_finished,
        "landing_elapsed_seconds": landing_elapsed,
        "full_elapsed_seconds": full_elapsed,
        "landing_input_rows_per_second": arguments.record_count / landing_elapsed,
        "full_input_rows_per_second": arguments.record_count / full_elapsed,
        "proof_identity": _proof_identity(native_report_by_field),
        "fact_set_sha256": native_report_by_field["fact_set_sha256"],
        "evidence_identity_set_sha256": native_report_by_field[
            "evidence_identity_set_sha256"
        ],
        "verifier_sha256": verifier_report["verifier_sha256"],
        "distinct_npis": evidence.distinct_npis,
        "duplicate_npi_groups": evidence.duplicate_npi_groups,
        "conflicting_npi_groups": evidence.conflicting_npi_groups,
        "conflict_counts": evidence.conflict_counts,
        "peak_worker_reserved_bytes": native_report_by_field[
            "peak_worker_reserved_bytes"
        ],
    }


def _public_result(result: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in result.items()
        if key not in {"landing_started", "landing_finished", "full_finished"}
    }


def _validate_benchmark_arguments(arguments: argparse.Namespace) -> None:
    if not arguments.dsn:
        raise ValueError("--dsn or HLTHPRT_UHC_BENCHMARK_DSN is required")
    if (
        arguments.trials < 1
        or arguments.parallelism < 1
        or arguments.parallelism > arguments.trials
        or not 1 <= arguments.native_workers <= 64
    ):
        raise ValueError("invalid benchmark concurrency")
    if arguments.artifact_byte_count <= 0 or arguments.record_count <= 0:
        raise ValueError("artifact and record counts must be positive")
    if not 4 <= arguments.range_count <= 256:
        raise ValueError("range count must be in 4..=256")
    if arguments.source_binding_id != (
        f"{arguments.catalog_set_sha256}/{arguments.source_file_id}"
    ):
        raise ValueError("source binding ID does not match catalog and source")
    _quoted(arguments.schema)


async def _run_trial_wave(
    arguments: argparse.Namespace,
    start: int,
    command: list[str],
    identity: UhcSemanticBuildIdentity,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    trial_numbers = list(
        range(
            start + 1,
            min(start + arguments.parallelism, arguments.trials) + 1,
        )
    )
    scheduler_started = time.perf_counter()
    wave_trial_reports = await asyncio.gather(
        *(
            _trial(arguments, trial, command, identity)
            for trial in trial_numbers
        )
    )
    scheduler_finished = time.perf_counter()
    input_record_count = sum(
        trial_report["input_records"] for trial_report in wave_trial_reports
    )
    landing_wall = max(
        trial_report["landing_finished"] for trial_report in wave_trial_reports
    ) - min(
        trial_report["landing_started"] for trial_report in wave_trial_reports
    )
    full_wall = max(
        trial_report["full_finished"] for trial_report in wave_trial_reports
    ) - min(
        trial_report["landing_started"] for trial_report in wave_trial_reports
    )
    aggregate_by_field = {
        "trials": trial_numbers,
        "scheduler_wall_seconds": scheduler_finished - scheduler_started,
        "landing_wall_seconds": landing_wall,
        "full_wall_seconds": full_wall,
        "landing_aggregate_input_rows_per_second": input_record_count
        / landing_wall,
        "full_aggregate_input_rows_per_second": input_record_count / full_wall,
    }
    return wave_trial_reports, aggregate_by_field


def _rate_summary(rate_values: list[float]) -> dict[str, float]:
    return {
        "minimum": min(rate_values),
        "median": statistics.median(rate_values),
        "maximum": max(rate_values),
    }


def _benchmark_summary(
    trial_reports: list[dict[str, Any]],
    aggregate_wave_reports: list[dict[str, Any]],
    encoder_sha256: str,
) -> dict[str, Any]:
    landing_rates = [
        trial_report["landing_input_rows_per_second"]
        for trial_report in trial_reports
    ]
    full_rates = [
        trial_report["full_input_rows_per_second"]
        for trial_report in trial_reports
    ]
    landing_aggregate_rates = [
        wave_report["landing_aggregate_input_rows_per_second"]
        for wave_report in aggregate_wave_reports
    ]
    full_aggregate_rates = [
        wave_report["full_aggregate_input_rows_per_second"]
        for wave_report in aggregate_wave_reports
    ]
    proof_identities = {
        trial_report["proof_identity"] for trial_report in trial_reports
    }
    return {
        "trials": [
            _public_result(trial_report) for trial_report in trial_reports
        ],
        "waves": aggregate_wave_reports,
        "landing_input_rows_per_second": _rate_summary(landing_rates),
        "full_input_rows_per_second": _rate_summary(full_rates),
        "landing_aggregate_input_rows_per_second": _rate_summary(
            landing_aggregate_rates
        ),
        "full_aggregate_input_rows_per_second": _rate_summary(
            full_aggregate_rates
        ),
        "target_rows_per_second": 100_000,
        "landing_aggregate_target_met": min(landing_aggregate_rates) >= 100_000,
        "full_aggregate_target_met": min(full_aggregate_rates) >= 100_000,
        "deterministic_proofs": len(proof_identities) == 1,
        "encoder_sha256": encoder_sha256,
    }


async def _run_benchmark(arguments: argparse.Namespace) -> dict[str, Any]:
    """Run bounded benchmark waves and aggregate deterministic evidence."""

    _validate_benchmark_arguments(arguments)
    encoder_sha256 = _file_sha256(arguments.native)
    command, identity = _native_command(arguments, encoder_sha256)
    identity.validate()
    trial_reports = []
    aggregate_wave_reports = []
    for start in range(0, arguments.trials, arguments.parallelism):
        wave_trial_reports, aggregate_by_field = await _run_trial_wave(
            arguments,
            start,
            command,
            identity,
        )
        trial_reports.extend(wave_trial_reports)
        aggregate_wave_reports.append(aggregate_by_field)
    return _benchmark_summary(
        trial_reports,
        aggregate_wave_reports,
        encoder_sha256,
    )


def main() -> None:
    """Run the retained UHC semantic benchmark CLI."""

    arguments = _arguments()
    result = asyncio.run(_run_benchmark(arguments))
    print(json.dumps(result, sort_keys=True))
    if arguments.require_target and not result["landing_aggregate_target_met"]:
        raise SystemExit(3)


if __name__ == "__main__":
    main()
