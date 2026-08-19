# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import hashlib
import importlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
from types import SimpleNamespace
from typing import Any, Awaitable, Callable
from unittest.mock import patch


def _required_inputs(repo_root: Path) -> tuple[Path, str, Path, str]:
    event_value = os.getenv("ENDURANT_BENCHMARK_EVENT_PATH", "")
    catalog_hash = os.getenv(
        "HLTHPRT_PROVIDER_DIRECTORY_BENCHMARK_CATALOG_SHA256",
        "",
    )
    database_name = os.getenv("HLTHPRT_DB_DATABASE", "")
    binary_value = os.getenv("HLTHPRT_UHC_SEMANTIC_BIN", "")
    if not event_value:
        raise RuntimeError("ENDURANT_BENCHMARK_EVENT_PATH is required")
    if re.fullmatch(r"[0-9a-f]{64}", catalog_hash) is None:
        raise RuntimeError(
            "HLTHPRT_PROVIDER_DIRECTORY_BENCHMARK_CATALOG_SHA256 must be exact"
        )
    if re.fullmatch(
        r"[a-z0-9_]*benchmark[a-z0-9_]*_test", database_name
    ) is None or os.getenv("HLTHPRT_DB_DATABASE_OVERRIDE"):
        raise RuntimeError(
            "HLTHPRT_DB_DATABASE must name the disposable benchmark test DB"
        )
    if not binary_value:
        raise RuntimeError("HLTHPRT_UHC_SEMANTIC_BIN is required")
    binary = Path(binary_value)
    if not binary.is_absolute():
        binary = repo_root / binary
    binary = Path(os.path.abspath(binary))
    expected_binary = repo_root / "support/ptg2_scanner/target/release/uhc_semantic_facts"
    if binary != expected_binary:
        raise RuntimeError(
            "HLTHPRT_UHC_SEMANTIC_BIN must be the repository release executable"
        )
    return Path(event_value), catalog_hash, binary, database_name


def _file_sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _build_release_encoder(repo_root: Path, binary: Path) -> None:
    subprocess.run(
        [
            "cargo",
            "build",
            "--locked",
            "--offline",
            "--release",
            "--bin",
            "uhc_semantic_facts",
            "--manifest-path",
            "support/ptg2_scanner/Cargo.toml",
        ],
        cwd=repo_root,
        check=True,
    )
    if binary.resolve(strict=True) != binary or not os.access(binary, os.X_OK):
        raise RuntimeError("repository release semantic executable is unavailable")


async def _is_stage_absent(importer, stage) -> bool:
    async with importer.db.acquire_driver() as connection:
        for relation in (*stage.auxiliary_relations, stage.resource_relation):
            if (
                await connection.fetchval(
                    "SELECT to_regclass($1)::text",
                    f'"{stage.schema}"."{relation}"',
                )
                is not None
            ):
                return False
    return True


@dataclass
class _BenchmarkObservation:
    acquire_current_files: Callable[..., Awaitable[Any]]
    build_publication_stage: Callable[..., Awaitable[Any]]
    publish_or_replay_candidate: Callable[..., Awaitable[Any]]
    cleanup_canonical_stage: Callable[..., Awaitable[Any]]
    start_semantic_encoder: Callable[..., Awaitable[Any]]
    seconds_by_phase: dict[str, list[float]] = field(
        default_factory=lambda: {
            name: [] for name in (
                "acquisition", "post_validation_to_publication", "semantic",
                "publication", "cleanup",
            )
        },
    )
    canonical_stages: list[Any] = field(default_factory=list)
    publication_records: list[tuple[bool, Any, dict[str, Any]]] = field(default_factory=list)
    publication_start_times: list[float] = field(default_factory=list)
    encoder_source_file_ids: list[str] = field(default_factory=list)
    touched_redis_paths: set[str] = field(default_factory=set)

    async def measure(self, phase_name: str, awaitable: Awaitable[Any]) -> Any:
        started_at = time.perf_counter()
        try:
            return await awaitable
        finally:
            self.seconds_by_phase[phase_name].append(time.perf_counter() - started_at)

    async def acquire_files(self, *args, **kwargs):
        acquired_files = await self.measure("acquisition", self.acquire_current_files(*args, **kwargs))
        self.publication_start_times.append(time.perf_counter())
        return acquired_files

    async def build_stage(self, *args, **kwargs):
        canonical_stage = await self.measure("semantic", self.build_publication_stage(*args, **kwargs))
        self.canonical_stages.append(canonical_stage)
        return canonical_stage

    async def publish_candidate(self, context, params, run_id, candidate, stage):
        publication_map = await self.measure(
            "publication",
            self.publish_or_replay_candidate(context, params, run_id, candidate, stage),
        )
        publication_index = len(self.publication_records)
        if publication_index >= len(self.publication_start_times):
            raise RuntimeError("benchmark post-validation timing is incomplete")
        self.seconds_by_phase["post_validation_to_publication"].append(
            time.perf_counter() - self.publication_start_times[publication_index]
        )
        self.publication_records.append(
            (candidate.already_published, candidate, dict(publication_map))
        )
        return publication_map

    async def cleanup_stage(self, *args, **kwargs):
        return await self.measure("cleanup", self.cleanup_canonical_stage(*args, **kwargs))

    async def reject_source_call(self, *_args, **_kwargs):
        raise RuntimeError("benchmark refuses external retained-source calls")

    def reject_control_throttle(self, *_args, **_kwargs):
        self.touched_redis_paths.add("control_throttle")
        raise RuntimeError("benchmark refuses Redis throttle calls")

    def reject_live_progress(self, *_args, **_kwargs):
        self.touched_redis_paths.add("live_progress")
        raise RuntimeError("benchmark refuses Redis live-progress calls")

    async def start_encoder(self, binary_path, admitted):
        self.encoder_source_file_ids.append(admitted.source_file_id)
        return await self.start_semantic_encoder(binary_path, admitted)


async def _assert_fresh_benchmark_state(
    importer,
    retained_dataset,
    *,
    catalog_hash: str,
    encoder_digest: str,
    run_id: str,
) -> None:
    run_ref = importer._qt(importer._schema(), importer.ImportRun.__tablename__)
    if await importer.db.scalar(
        f"SELECT EXISTS (SELECT 1 FROM {run_ref} WHERE run_id=:run_id);",
        run_id=run_id,
    ):
        raise RuntimeError("benchmark ImportRun identity already exists")
    async with importer.db.acquire_driver() as connection:
        admitted_set = await retained_dataset.load_complete_admitted_uhc_catalog_set(
            connection,
            catalog_hash,
        )
        for admitted in admitted_set.files:
            if await retained_dataset.load_sealed_uhc_semantic_build(
                connection,
                admitted.semantic_identity(encoder_digest),
            ):
                raise RuntimeError(
                    "benchmark requires fresh semantic builds for this encoder"
                )


def _benchmark_modules():
    return SimpleNamespace(
        importer=importlib.import_module("process.provider_directory_fhir"),
        acquisition=importlib.import_module("process.uhc_official_file_acquisition"),
        control_lifecycle=importlib.import_module("process.control_lifecycle"),
        repair_params=importlib.import_module("tests.test_uhc_provider_file_admission"),
        retained_dataset=importlib.import_module("process.uhc_retained_dataset"),
    )


async def _prepare_benchmark(
    modules,
    catalog_hash: str,
    encoder_digest: str,
    database_name: str,
) -> tuple[dict[str, Any], dict[str, Any], str]:
    importer = modules.importer
    if await importer.db.scalar("SELECT current_database();") != database_name:
        raise RuntimeError(
            "connected database differs from the disposable benchmark DB"
        )
    observed_at = importer._now()
    endpoint_row = importer._uhc_official_file_endpoint_row(observed_at=observed_at)
    source_row = importer._uhc_official_file_source_row(
        endpoint_row["endpoint_id"], observed_at=observed_at
    )
    run_id = f"pd-benchmark-{catalog_hash[:48]}"
    dataset_id = importer._uhc_retained_candidate_id(
        endpoint_row["endpoint_id"], run_id
    )
    await _assert_fresh_benchmark_state(
        importer,
        modules.retained_dataset,
        catalog_hash=catalog_hash,
        encoder_digest=encoder_digest,
        run_id=run_id,
    )
    if await importer._endpoint_dataset_state(dataset_id):
        raise RuntimeError(
            "benchmark requires a freshly reset disposable database clone"
        )
    await importer._upsert_provider_directory_source_rows([source_row])
    import_task = modules.repair_params._valid_repair_params(
        uhc_catalog_set_sha256=catalog_hash
    )
    return source_row, import_task, run_id


async def _run_benchmark_imports(modules, source_row, import_task, run_id):
    importer = modules.importer
    observations = _BenchmarkObservation(
        importer._acquire_current_uhc_official_file_set,
        importer._build_uhc_publication_stage,
        importer._publish_or_replay_uhc_candidate,
        importer.cleanup_uhc_canonical_stage,
        modules.retained_dataset._start_semantic_encoder,
    )

    async def invoke_import():
        return await importer._import_uhc_official_file_source_group(
            [source_row],
            list(importer.UHC_SUPPORTED_RESOURCES),
            ctx={"context": {}},
            task=import_task,
            run_id=run_id,
        )

    with (
        patch.object(modules.acquisition, "_download_file", observations.reject_source_call),
        patch.object(importer, "refresh_uhc_provider_file_catalog", observations.reject_source_call),
        patch.object(
            modules.control_lifecycle,
            "_control_run_db_throttle_client",
            observations.reject_control_throttle,
        ),
        patch.object(modules.control_lifecycle, "write_live_progress", observations.reject_live_progress),
        patch.object(importer, "_acquire_current_uhc_official_file_set", observations.acquire_files),
        patch.object(importer, "_build_uhc_publication_stage", observations.build_stage),
        patch.object(importer, "_publish_or_replay_uhc_candidate", observations.publish_candidate),
        patch.object(importer, "cleanup_uhc_canonical_stage", observations.cleanup_stage),
        patch.object(modules.retained_dataset, "_start_semantic_encoder", observations.start_encoder),
    ):
        started_at = time.perf_counter()
        first_import_result = await invoke_import()
        pipeline_seconds = time.perf_counter() - started_at
        first_encoder_source_file_ids = tuple(observations.encoder_source_file_ids)
        replay_started_at = time.perf_counter()
        replay_import_result = await invoke_import()
        replay_pipeline_seconds = time.perf_counter() - replay_started_at
    return (
        observations,
        first_import_result,
        replay_import_result,
        first_encoder_source_file_ids,
        pipeline_seconds,
        replay_pipeline_seconds,
    )


def _publication_replay_evidence(
    observations,
    first_import_result,
    replay_import_result,
    catalog_hash,
    encoder_digest,
):
    if observations.touched_redis_paths:
        raise RuntimeError(
            "benchmark touched Redis paths: "
            + ", ".join(sorted(observations.touched_redis_paths))
        )
    if len(observations.canonical_stages) != 2 or len(observations.publication_records) != 2:
        raise RuntimeError("benchmark phase observations are incomplete")
    first_replayed, candidate, publication_map = observations.publication_records[0]
    replayed, replay_candidate, replay_publication_map = observations.publication_records[1]
    first_stage, replay_stage = observations.canonical_stages
    build_keys = (
        "catalog_set_sha256",
        "input_set_sha256",
        "semantic_set_sha256",
        "input_sha256",
        "encoder_digest",
    )
    build_identity_by_name = {
        key: first_stage.summary_input[key] for key in build_keys
    }
    replay_identity_by_name = {
        key: replay_stage.summary_input[key] for key in build_keys
    }
    if (
        first_replayed
        or not replayed
        or candidate.dataset_id != replay_candidate.dataset_id
        or publication_map != replay_publication_map
        or first_import_result[2] != replay_import_result[2]
        or build_identity_by_name != replay_identity_by_name
        or build_identity_by_name["catalog_set_sha256"] != catalog_hash
        or build_identity_by_name["encoder_digest"] != encoder_digest
    ):
        raise RuntimeError("fresh publication and replay invariants differ")
    input_identity_by_name = {
        key: build_identity_by_name[key]
        for key in ("catalog_set_sha256", "input_set_sha256")
    }
    return candidate, publication_map, first_stage, replay_stage, input_identity_by_name


def _retained_acquisition_stats(
    observations, first_import_result, first_encoder_source_file_ids
):
    stats_by_name = next(iter(first_import_result[4].values()))
    if (
        stats_by_name["official_files_downloaded"] != 0
        or stats_by_name["official_files_reused"]
        != stats_by_name["official_catalog_files"]
        or len(first_encoder_source_file_ids)
        != stats_by_name["official_catalog_files"]
        or len(set(first_encoder_source_file_ids))
        != stats_by_name["official_catalog_files"]
        or len(observations.encoder_source_file_ids)
        != len(first_encoder_source_file_ids)
    ):
        raise RuntimeError(
            "benchmark catalog retention or encoder execution is incomplete"
        )
    return stats_by_name


async def _assert_publication_state(
    importer, candidate, publication_map, first_stage, replay_stage
) -> None:
    dataset_ref = importer._qt(
        importer._schema(), importer.ProviderDirectoryEndpointDataset.__tablename__
    )
    current_pointer = await importer.db.first(
        f"SELECT count(*) AS row_count, max(dataset_id) AS dataset_id "
        f"FROM {dataset_ref} WHERE endpoint_id=:endpoint_id AND is_current=true;",
        endpoint_id=candidate.endpoint_id,
    )
    current_pointer_map = dict(current_pointer._mapping)
    if not await _is_stage_absent(
        importer, first_stage
    ) or not await _is_stage_absent(importer, replay_stage):
        raise RuntimeError("canonical-stage cleanup is incomplete")
    post_cleanup_publication_map = await importer._assert_final_uhc_publication(
        candidate
    )
    if current_pointer_map != {
        "row_count": 1,
        "dataset_id": candidate.dataset_id,
    } or post_cleanup_publication_map != publication_map:
        raise RuntimeError("publication pointer or post-cleanup proof is incomplete")


def _benchmark_event_map(
    observations,
    stats_by_name,
    input_identity_by_name,
    publication_map,
    first_stage,
    pipeline_seconds,
    replay_pipeline_seconds,
):
    return {
        "schema_version": 1,
        "correctness": {
            "input_identity": input_identity_by_name,
            "retained_acquisition": {
                "file_count": stats_by_name["official_catalog_files"],
                "downloaded_file_count": 0,
                "reused_file_count": stats_by_name["official_files_reused"],
            },
            "dataset": {
                "dataset_hash": publication_map["dataset_hash"],
                "resource_count": publication_map["resource_count"],
                "resource_counts": publication_map["resource_counts"],
            },
            "publication": {
                "status": publication_map["status"],
                "is_current": publication_map["is_current"],
                "committed_receipt_matches": True,
                "single_current_pointer": True,
                "fresh_then_replay": True,
            },
            "cleanup": {"canonical_stages_removed": True},
        },
        "metrics": {
            "pipeline_seconds": pipeline_seconds,
            "post_validation_to_publication_seconds": observations.seconds_by_phase[
                "post_validation_to_publication"
            ][0],
            "acquisition_seconds": observations.seconds_by_phase["acquisition"][0],
            "semantic_build_seconds": observations.seconds_by_phase["semantic"][0],
            "canonical_materialization_seconds": first_stage.phase_metrics[
                "canonical_materialization_seconds"
            ],
            "publication_seconds": observations.seconds_by_phase["publication"][0],
            "cleanup_seconds": observations.seconds_by_phase["cleanup"][0],
            "replay_pipeline_seconds": replay_pipeline_seconds,
        },
    }


async def _execute_benchmark(modules, catalog_hash, encoder_digest, database_name):
    source_row, import_task, run_id = await _prepare_benchmark(
        modules, catalog_hash, encoder_digest, database_name
    )
    (
        observations,
        first_import_result,
        replay_import_result,
        first_encoder_source_file_ids,
        pipeline_seconds,
        replay_pipeline_seconds,
    ) = await _run_benchmark_imports(modules, source_row, import_task, run_id)
    candidate, publication_map, first_stage, replay_stage, input_identity_by_name = (
        _publication_replay_evidence(
            observations,
            first_import_result,
            replay_import_result,
            catalog_hash,
            encoder_digest,
        )
    )
    stats_by_name = _retained_acquisition_stats(
        observations, first_import_result, first_encoder_source_file_ids
    )
    await _assert_publication_state(
        modules.importer, candidate, publication_map, first_stage, replay_stage
    )
    return _benchmark_event_map(
        observations,
        stats_by_name,
        input_identity_by_name,
        publication_map,
        first_stage,
        pipeline_seconds,
        replay_pipeline_seconds,
    )


async def _benchmark() -> None:
    """Measure retained local input through durable source publication and replay."""
    repo_root = Path(__file__).resolve().parents[1]
    event_path, catalog_hash, binary, database_name = _required_inputs(repo_root)
    _build_release_encoder(repo_root, binary)
    encoder_digest = _file_sha256(binary)
    modules = _benchmark_modules()
    await modules.importer.db.connect()
    try:
        benchmark_event_map = await _execute_benchmark(
            modules, catalog_hash, encoder_digest, database_name
        )
    finally:
        await modules.importer.db.disconnect()
    event_path.write_text(
        json.dumps(benchmark_event_map, sort_keys=True) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    asyncio.run(_benchmark())
