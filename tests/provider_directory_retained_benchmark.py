# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
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
    expected_binary = (
        repo_root
        / "support"
        / "ptg2_scanner"
        / "target"
        / "release"
        / "uhc_semantic_facts"
    )
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


async def _stage_is_absent(importer, stage) -> bool:
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


async def _benchmark() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    event_path, catalog_hash, binary, database_name = _required_inputs(repo_root)
    _build_release_encoder(repo_root, binary)
    encoder_digest = _file_sha256(binary)
    importer = importlib.import_module("process.provider_directory_fhir")
    acquisition = importlib.import_module("process.uhc_official_file_acquisition")
    control_lifecycle = importlib.import_module("process.control_lifecycle")
    repair_params = importlib.import_module("tests.test_uhc_provider_file_admission")
    retained_dataset = importlib.import_module("process.uhc_retained_dataset")
    await importer.db.connect()
    try:
        if await importer.db.scalar("SELECT current_database();") != database_name:
            raise RuntimeError(
                "connected database differs from the disposable benchmark DB"
            )
        observed_at = importer._now()
        endpoint = importer._uhc_official_file_endpoint_row(observed_at=observed_at)
        source = importer._uhc_official_file_source_row(
            endpoint["endpoint_id"], observed_at=observed_at
        )
        run_id = f"pd-benchmark-{catalog_hash[:48]}"
        dataset_id = importer._uhc_retained_candidate_id(
            endpoint["endpoint_id"], run_id
        )
        await _assert_fresh_benchmark_state(
            importer,
            retained_dataset,
            catalog_hash=catalog_hash,
            encoder_digest=encoder_digest,
            run_id=run_id,
        )
        if await importer._endpoint_dataset_state(dataset_id):
            raise RuntimeError(
                "benchmark requires a freshly reset disposable database clone"
            )
        await importer._upsert_provider_directory_source_rows([source])
        task = repair_params._valid_repair_params(uhc_catalog_set_sha256=catalog_hash)
        timings = {
            name: []
            for name in (
                "acquisition",
                "post_validation_to_publication",
                "semantic",
                "publication",
                "cleanup",
            )
        }
        stages = []
        publications = []
        post_validation_started_at = []
        encoder_source_file_ids = []
        redis_paths_touched = set()

        async def timed(name, awaitable):
            started = time.perf_counter()
            try:
                return await awaitable
            finally:
                timings[name].append(time.perf_counter() - started)

        original_acquire = importer._acquire_current_uhc_official_file_set
        original_build = importer._build_uhc_publication_stage
        original_publish = importer._publish_or_replay_uhc_candidate
        original_cleanup = importer.cleanup_uhc_canonical_stage
        original_start_encoder = retained_dataset._start_semantic_encoder

        async def timed_acquire(*args, **kwargs):
            result = await timed("acquisition", original_acquire(*args, **kwargs))
            post_validation_started_at.append(time.perf_counter())
            return result

        async def timed_build(*args, **kwargs):
            stage = await timed("semantic", original_build(*args, **kwargs))
            stages.append(stage)
            return stage

        async def timed_publish(ctx, params, root_run_id, candidate, stage):
            result = await timed(
                "publication",
                original_publish(ctx, params, root_run_id, candidate, stage),
            )
            publication_index = len(publications)
            if publication_index >= len(post_validation_started_at):
                raise RuntimeError("benchmark post-validation timing is incomplete")
            timings["post_validation_to_publication"].append(
                time.perf_counter() - post_validation_started_at[publication_index]
            )
            publications.append((candidate.already_published, candidate, dict(result)))
            return result

        async def timed_cleanup(*args, **kwargs):
            return await timed("cleanup", original_cleanup(*args, **kwargs))

        async def reject_source_call(*_args, **_kwargs):
            raise RuntimeError("benchmark refuses external retained-source calls")

        def reject_control_throttle(*_args, **_kwargs):
            redis_paths_touched.add("control_throttle")
            raise RuntimeError("benchmark refuses Redis throttle calls")

        def reject_live_progress(*_args, **_kwargs):
            redis_paths_touched.add("live_progress")
            raise RuntimeError("benchmark refuses Redis live-progress calls")

        async def record_start_encoder(binary_path, admitted):
            encoder_source_file_ids.append(admitted.source_file_id)
            return await original_start_encoder(binary_path, admitted)

        async def invoke():
            return await importer._import_uhc_official_file_source_group(
                [source],
                list(importer.UHC_SUPPORTED_RESOURCES),
                ctx={"context": {}},
                task=task,
                run_id=run_id,
            )

        with (
            patch.object(acquisition, "_download_file", reject_source_call),
            patch.object(
                importer, "refresh_uhc_provider_file_catalog", reject_source_call
            ),
            patch.object(
                control_lifecycle,
                "_control_run_db_throttle_client",
                reject_control_throttle,
            ),
            patch.object(
                control_lifecycle,
                "write_live_progress",
                reject_live_progress,
            ),
            patch.object(
                importer, "_acquire_current_uhc_official_file_set", timed_acquire
            ),
            patch.object(importer, "_build_uhc_publication_stage", timed_build),
            patch.object(importer, "_publish_or_replay_uhc_candidate", timed_publish),
            patch.object(importer, "cleanup_uhc_canonical_stage", timed_cleanup),
            patch.object(
                retained_dataset, "_start_semantic_encoder", record_start_encoder
            ),
        ):
            started = time.perf_counter()
            first_result = await invoke()
            pipeline_seconds = time.perf_counter() - started
            first_encoder_source_file_ids = tuple(encoder_source_file_ids)
            replay_started = time.perf_counter()
            replay_result = await invoke()
            replay_pipeline_seconds = time.perf_counter() - replay_started

        if redis_paths_touched:
            raise RuntimeError(
                "benchmark touched Redis paths: "
                + ", ".join(sorted(redis_paths_touched))
            )
        if len(stages) != 2 or len(publications) != 2:
            raise RuntimeError("benchmark phase observations are incomplete")
        first_replayed, candidate, publication = publications[0]
        replayed, replay_candidate, replay_publication = publications[1]
        first_stage, replay_stage = stages
        build_identity = {
            key: first_stage.summary_input[key]
            for key in (
                "catalog_set_sha256",
                "input_set_sha256",
                "semantic_set_sha256",
                "input_sha256",
                "encoder_digest",
            )
        }
        input_identity = {
            key: build_identity[key]
            for key in ("catalog_set_sha256", "input_set_sha256")
        }
        if (
            first_replayed
            or not replayed
            or candidate.dataset_id != replay_candidate.dataset_id
            or publication != replay_publication
            or first_result[2] != replay_result[2]
            or build_identity
            != {key: replay_stage.summary_input[key] for key in build_identity}
            or build_identity["catalog_set_sha256"] != catalog_hash
            or build_identity["encoder_digest"] != encoder_digest
        ):
            raise RuntimeError("fresh publication and replay invariants differ")
        stats = next(iter(first_result[4].values()))
        if (
            stats["official_files_downloaded"] != 0
            or stats["official_files_reused"] != stats["official_catalog_files"]
            or len(first_encoder_source_file_ids)
            != stats["official_catalog_files"]
            or len(set(first_encoder_source_file_ids))
            != stats["official_catalog_files"]
            or len(encoder_source_file_ids) != len(first_encoder_source_file_ids)
        ):
            raise RuntimeError(
                "benchmark catalog retention or encoder execution is incomplete"
            )
        dataset_ref = importer._qt(
            importer._schema(), importer.ProviderDirectoryEndpointDataset.__tablename__
        )
        pointer = await importer.db.first(
            f"SELECT count(*) AS row_count, max(dataset_id) AS dataset_id "
            f"FROM {dataset_ref} WHERE endpoint_id=:endpoint_id AND is_current=true;",
            endpoint_id=candidate.endpoint_id,
        )
        pointer_map = dict(pointer._mapping)
        first_stage_absent = await _stage_is_absent(importer, first_stage)
        replay_stage_absent = await _stage_is_absent(importer, replay_stage)
        if not first_stage_absent or not replay_stage_absent:
            raise RuntimeError("canonical-stage cleanup is incomplete")
        post_cleanup_publication = await importer._assert_final_uhc_publication(
            candidate
        )
        if pointer_map != {
            "row_count": 1,
            "dataset_id": candidate.dataset_id,
        } or post_cleanup_publication != publication:
            raise RuntimeError(
                "publication pointer or post-cleanup proof is incomplete"
            )
        payload = {
            "schema_version": 1,
            "correctness": {
                "input_identity": input_identity,
                "retained_acquisition": {
                    "file_count": stats["official_catalog_files"],
                    "downloaded_file_count": 0,
                    "reused_file_count": stats["official_files_reused"],
                },
                "dataset": {
                    "dataset_hash": publication["dataset_hash"],
                    "resource_count": publication["resource_count"],
                    "resource_counts": publication["resource_counts"],
                },
                "publication": {
                    "status": publication["status"],
                    "is_current": publication["is_current"],
                    "committed_receipt_matches": True,
                    "single_current_pointer": True,
                    "fresh_then_replay": True,
                },
                "cleanup": {"canonical_stages_removed": True},
            },
            "metrics": {
                "pipeline_seconds": pipeline_seconds,
                "post_validation_to_publication_seconds": timings[
                    "post_validation_to_publication"
                ][0],
                "acquisition_seconds": timings["acquisition"][0],
                "semantic_build_seconds": timings["semantic"][0],
                "canonical_materialization_seconds": first_stage.phase_metrics[
                    "canonical_materialization_seconds"
                ],
                "publication_seconds": timings["publication"][0],
                "cleanup_seconds": timings["cleanup"][0],
                "replay_pipeline_seconds": replay_pipeline_seconds,
            },
        }
    finally:
        await importer.db.disconnect()
    event_path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    asyncio.run(_benchmark())
