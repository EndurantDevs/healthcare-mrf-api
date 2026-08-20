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


MAXIMUM_PUBLICATION_SECONDS = 1_800
MINIMUM_RESOURCES_PER_SECOND = 18_920


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
        repo_root / "support/ptg2_scanner/target/release/uhc_semantic_facts"
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


async def _relation_snapshot(
    importer, relation_names: tuple[str, ...]
) -> dict[str, dict[str, Any]]:
    async with importer.db.acquire_driver() as connection:
        rows = await connection.fetch(
            """
            SELECT relation.relname AS relation_name,
                   relation.oid::bigint AS relation_oid,
                   relation.relpersistence::text AS persistence,
                   count(index_state.indexrelid)::bigint AS index_count,
                   COALESCE(
                       bool_and(
                           index_state.indisvalid
                           AND index_state.indisready
                           AND index_state.indislive
                       ),
                       false
                   ) AS indexes_valid_ready_live
              FROM pg_catalog.pg_class AS relation
              JOIN pg_catalog.pg_namespace AS namespace
                ON namespace.oid = relation.relnamespace
              LEFT JOIN pg_catalog.pg_index AS index_state
                ON index_state.indrelid = relation.oid
             WHERE namespace.nspname = $1
               AND relation.relname = ANY($2::text[])
               AND relation.relkind IN ('r', 'p')
             GROUP BY relation.relname, relation.oid, relation.relpersistence;
            """,
            importer._schema(),
            list(relation_names),
        )
    return {
        row["relation_name"]: {
            "oid": int(row["relation_oid"]),
            "persistence": row["persistence"],
            "index_count": int(row["index_count"]),
            "indexes_valid_ready_live": bool(row["indexes_valid_ready_live"]),
        }
        for row in rows
    }


async def _live_relation_snapshot(importer) -> dict[str, dict[str, Any]]:
    relation_names = tuple(
        model.__tablename__
        for model in (
            importer.ProviderDirectoryEndpointDataset,
            importer.ProviderDirectoryDatasetResource,
            importer.ProviderDirectoryDatasetNetworkPlan,
            importer.ProviderDirectoryDatasetAffiliationOrganization,
        )
    )
    snapshot = await _relation_snapshot(importer, relation_names)
    if set(snapshot) != set(relation_names) or any(
        relation["persistence"] != "p"
        or relation["index_count"] < 1
        or relation["indexes_valid_ready_live"] is not True
        for relation in snapshot.values()
    ):
        raise RuntimeError("benchmark live relation persistence is invalid")
    return snapshot


async def _stage_relation_snapshot(importer, stage) -> dict[str, dict[str, Any]]:
    relation_names = (*stage.auxiliary_relations, stage.resource_relation)
    snapshot = await _relation_snapshot(importer, relation_names)
    if set(snapshot) != set(relation_names) or any(
        relation["persistence"] != "u" for relation in snapshot.values()
    ):
        raise RuntimeError("benchmark canonical-stage persistence is invalid")
    return snapshot


@dataclass
class _BenchmarkObservation:
    acquire_current_files: Callable[..., Awaitable[Any]]
    build_publication_stage: Callable[..., Awaitable[Any]]
    publish_or_replay_candidate: Callable[..., Awaitable[Any]]
    cleanup_canonical_stage: Callable[..., Awaitable[Any]]
    start_semantic_encoder: Callable[..., Awaitable[Any]]
    capture_stage_relations: Callable[[Any], Awaitable[dict[str, dict[str, Any]]]]
    seconds_by_phase: dict[str, list[float]] = field(
        default_factory=lambda: {
            name: []
            for name in (
                "acquisition",
                "post_validation_to_publication",
                "semantic",
                "publication",
                "cleanup",
            )
        },
    )
    canonical_stages: list[Any] = field(default_factory=list)
    stage_relation_snapshots: list[dict[str, dict[str, Any]]] = field(
        default_factory=list
    )
    publication_records: list[tuple[bool, Any, dict[str, Any]]] = field(
        default_factory=list
    )
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
        acquired_files = await self.measure(
            "acquisition", self.acquire_current_files(*args, **kwargs)
        )
        self.publication_start_times.append(time.perf_counter())
        return acquired_files

    async def build_stage(self, *args, **kwargs):
        canonical_stage = await self.measure(
            "semantic", self.build_publication_stage(*args, **kwargs)
        )
        self.canonical_stages.append(canonical_stage)
        self.stage_relation_snapshots.append(
            await self.capture_stage_relations(canonical_stage)
        )
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
        return await self.measure(
            "cleanup", self.cleanup_canonical_stage(*args, **kwargs)
        )

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
        acquire_current_files=importer._acquire_current_uhc_official_file_set,
        build_publication_stage=importer._build_uhc_publication_stage,
        publish_or_replay_candidate=importer._publish_or_replay_uhc_candidate,
        cleanup_canonical_stage=importer.cleanup_uhc_canonical_stage,
        start_semantic_encoder=modules.retained_dataset._start_semantic_encoder,
        capture_stage_relations=lambda stage: _stage_relation_snapshot(importer, stage),
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
        patch.object(
            modules.acquisition, "_download_file", observations.reject_source_call
        ),
        patch.object(
            importer,
            "refresh_uhc_provider_file_catalog",
            observations.reject_source_call,
        ),
        patch.object(
            modules.control_lifecycle,
            "_control_run_db_throttle_client",
            observations.reject_control_throttle,
        ),
        patch.object(
            modules.control_lifecycle,
            "write_live_progress",
            observations.reject_live_progress,
        ),
        patch.object(
            importer,
            "_acquire_current_uhc_official_file_set",
            observations.acquire_files,
        ),
        patch.object(
            importer, "_build_uhc_publication_stage", observations.build_stage
        ),
        patch.object(
            importer, "_publish_or_replay_uhc_candidate", observations.publish_candidate
        ),
        patch.object(
            importer, "cleanup_uhc_canonical_stage", observations.cleanup_stage
        ),
        patch.object(
            modules.retained_dataset,
            "_start_semantic_encoder",
            observations.start_encoder,
        ),
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
    if (
        len(observations.canonical_stages) != 2
        or len(observations.publication_records) != 2
    ):
        raise RuntimeError("benchmark phase observations are incomplete")
    first_replayed, candidate, publication_map = observations.publication_records[0]
    replayed, replay_candidate, replay_publication_map = (
        observations.publication_records[1]
    )
    first_stage, replay_stage = observations.canonical_stages
    build_identity_by_name = dict(first_stage.summary_input)
    replay_identity_by_name = dict(replay_stage.summary_input)
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
    return candidate, publication_map, first_stage, replay_stage, build_identity_by_name


def _retained_acquisition_stats(
    observations, first_import_result, first_encoder_source_file_ids
):
    stats_by_name = next(iter(first_import_result[4].values()))
    if (
        stats_by_name["official_files_downloaded"] != 0
        or stats_by_name["official_files_reused"]
        != stats_by_name["official_catalog_files"]
        or len(first_encoder_source_file_ids) != stats_by_name["official_catalog_files"]
        or len(set(first_encoder_source_file_ids))
        != stats_by_name["official_catalog_files"]
        or len(observations.encoder_source_file_ids)
        != len(first_encoder_source_file_ids)
    ):
        raise RuntimeError(
            "benchmark catalog retention or encoder execution is incomplete"
        )
    return stats_by_name


def _stage_lifecycle_proof(observations) -> dict[str, Any]:
    snapshots = observations.stage_relation_snapshots
    if len(snapshots) != 2 or any(not snapshot for snapshot in snapshots):
        raise RuntimeError("benchmark canonical-stage lineage is incomplete")
    stage_oid_sets = [
        {relation["oid"] for relation in snapshot.values()} for snapshot in snapshots
    ]
    if stage_oid_sets[0].intersection(stage_oid_sets[1]):
        raise RuntimeError("benchmark canonical-stage OID was reused")
    return {
        "private_stages_unlogged": True,
        "private_stage_oids_distinct": True,
    }


def _live_relation_lifecycle_proof(
    before: dict[str, dict[str, Any]],
    after: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if before != after:
        raise RuntimeError("benchmark live relation lineage changed")
    return {
        "live_relation_names": sorted(after),
        "live_relation_oids_unchanged": True,
        "live_relations_permanent": True,
        "live_indexes_valid_ready": True,
    }


async def _publication_correctness_proof(importer, candidate, first_stage):
    state = await importer._endpoint_dataset_state(candidate.dataset_id)
    proof = importer.validate_uhc_final_publication(
        state,
        importer.UhcFinalPublicationExpectation(
            source_id=candidate.source_ids[0],
            dataset_id=candidate.dataset_id,
            endpoint_id=candidate.endpoint_id,
            acquisition_root_run_id=candidate.acquisition_root_run_id or "",
            selected_resources=tuple(candidate.selected_resources),
            semantic_contract_id=first_stage.summary_input["semantic_contract_id"],
            catalog_set_sha256=first_stage.summary_input["catalog_set_sha256"],
        ),
    )
    dataset_ref = importer._qt(
        importer._schema(), importer.ProviderDirectoryEndpointDataset.__tablename__
    )
    receipt_row = await importer.db.first(
        f"""
        SELECT publication_metadata_sha256,
               content_proof_admission_version,
               content_proof_admission_kind,
               content_proof_admission_sha256,
               content_proof_resource_types,
               ({importer._artifact_admission_seal_shape_valid_sql('dataset')})
                   AS seal_valid
          FROM {dataset_ref} AS dataset
         WHERE dataset.dataset_id = :dataset_id;
        """,
        dataset_id=candidate.dataset_id,
    )
    receipt = dict(receipt_row._mapping) if receipt_row is not None else {}
    canonical_proof = proof.canonical_proof
    if (
        proof.summary_input != first_stage.summary_input
        or receipt.get("seal_valid") is not True
        or receipt.get("content_proof_admission_version")
        != importer.ADMISSION_SEAL_VERSION
        or receipt.get("content_proof_admission_kind")
        != importer.ADMISSION_KIND_UHC_CANONICAL
        or receipt.get("content_proof_admission_sha256")
        != canonical_proof["proof_sha256"]
        or tuple(receipt.get("content_proof_resource_types") or ())
        != tuple(sorted(proof.resource_counts))
    ):
        raise RuntimeError("benchmark committed publication receipt is invalid")
    npi_evidence = canonical_proof["npi_evidence"]
    return {
        "lineage": {
            "source_id": candidate.source_ids[0],
            "endpoint_id": candidate.endpoint_id,
            "dataset_id": candidate.dataset_id,
            "acquisition_root_run_id": candidate.acquisition_root_run_id,
            "import_run_id": candidate.import_run_id,
            "selected_resources": list(candidate.selected_resources),
        },
        "canonical_proof": {
            field_name: canonical_proof[field_name]
            for field_name in (
                "dataset_hash",
                "resource_count",
                "resource_counts",
                "resource_hashes",
                "materialization_sha256",
                "shard_set_sha256",
                "proof_sha256",
            )
        }
        | {
            "npi_evidence_proof_sha256": npi_evidence["proof_sha256"],
            "npi_evidence_shard_set_sha256": npi_evidence["shard_set_sha256"],
        },
        "receipt": {
            "publication_metadata_sha256": receipt["publication_metadata_sha256"],
            "content_proof_admission_version": receipt[
                "content_proof_admission_version"
            ],
            "content_proof_admission_kind": receipt["content_proof_admission_kind"],
            "content_proof_admission_sha256": receipt["content_proof_admission_sha256"],
            "content_proof_resource_types": list(
                receipt["content_proof_resource_types"]
            ),
            "publication_identity": proof.publication_identity,
        },
        "source_summary": {
            field_name: proof.source_summary[field_name]
            for field_name in (
                "contract_id",
                "contract_version",
                "source_ids",
                "endpoint_id",
                "dataset_id",
                "acquisition_root_run_id",
                "semantic_contract_id",
                "summary_sha256",
            )
        },
    }


async def _assert_publication_state(
    importer,
    observations,
    candidate,
    publication_map,
    first_stage,
    replay_stage,
    live_relations_before,
    live_relations_after,
) -> tuple[dict[str, Any], dict[str, Any]]:
    dataset_ref = importer._qt(
        importer._schema(), importer.ProviderDirectoryEndpointDataset.__tablename__
    )
    current_pointer = await importer.db.first(
        f"SELECT count(*) AS row_count, max(dataset_id) AS dataset_id "
        f"FROM {dataset_ref} WHERE endpoint_id=:endpoint_id AND is_current=true;",
        endpoint_id=candidate.endpoint_id,
    )
    current_pointer_map = dict(current_pointer._mapping)
    if not await _is_stage_absent(importer, first_stage) or not await _is_stage_absent(
        importer, replay_stage
    ):
        raise RuntimeError("canonical-stage cleanup is incomplete")
    post_cleanup_publication_map = await importer._assert_final_uhc_publication(
        candidate
    )
    if (
        current_pointer_map
        != {
            "row_count": 1,
            "dataset_id": candidate.dataset_id,
        }
        or post_cleanup_publication_map != publication_map
    ):
        raise RuntimeError("publication pointer or post-cleanup proof is incomplete")
    cleanup_proof = {
        "canonical_stages_removed": True,
        **_stage_lifecycle_proof(observations),
        **_live_relation_lifecycle_proof(
            live_relations_before,
            live_relations_after,
        ),
    }
    return (
        await _publication_correctness_proof(importer, candidate, first_stage),
        cleanup_proof,
    )


async def _assert_database_quiescent(modules, database_name: str) -> dict[str, Any]:
    connection = await modules.importer.asyncpg.connect(
        host=os.getenv("HLTHPRT_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("HLTHPRT_DB_PORT", "5432")),
        user=os.getenv("HLTHPRT_DB_USER", "postgres"),
        password=os.getenv("HLTHPRT_DB_PASSWORD", ""),
        database=database_name,
        timeout=10,
    )
    try:
        row = await connection.fetchrow(
            """
            SELECT (
                       SELECT count(*)
                         FROM pg_catalog.pg_stat_activity AS activity
                        WHERE activity.datname = current_database()
                          AND activity.backend_type = 'client backend'
                          AND activity.pid <> pg_backend_pid()
                   )::bigint AS other_client_sessions,
                   (
                       SELECT count(*)
                         FROM pg_catalog.pg_locks AS lock_state
                         JOIN pg_catalog.pg_stat_activity AS activity
                           ON activity.pid = lock_state.pid
                        WHERE activity.datname = current_database()
                          AND lock_state.pid <> pg_backend_pid()
                          AND lock_state.granted IS NOT TRUE
                   )::bigint AS ungranted_locks;
            """
        )
    finally:
        await connection.close()
    counts = {
        "other_client_sessions": int(row["other_client_sessions"]),
        "ungranted_locks": int(row["ungranted_locks"]),
    }
    if any(counts.values()):
        raise RuntimeError("benchmark database did not quiesce")
    return {**counts, "database_quiescent": True}


def _benchmark_event_map(
    observations,
    stats_by_name,
    input_identity_by_name,
    publication_map,
    publication_proof,
    cleanup_proof,
    first_stage,
    pipeline_seconds,
    replay_pipeline_seconds,
):
    post_validation_to_publication_seconds = observations.seconds_by_phase[
        "post_validation_to_publication"
    ][0]
    return {
        "schema_version": 1,
        "correctness": {
            "performance_contract": {
                "maximum_publication_seconds": MAXIMUM_PUBLICATION_SECONDS,
                "minimum_resources_per_second": MINIMUM_RESOURCES_PER_SECOND,
            },
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
                "canonical_proof": publication_proof["canonical_proof"],
            },
            "publication": {
                "status": publication_map["status"],
                "is_current": publication_map["is_current"],
                "committed_receipt_matches": True,
                "single_current_pointer": True,
                "fresh_then_replay": True,
                "lineage": publication_proof["lineage"],
                "receipt": publication_proof["receipt"],
                "source_summary": publication_proof["source_summary"],
            },
            "cleanup": cleanup_proof,
        },
        "metrics": {
            "pipeline_seconds": pipeline_seconds,
            "post_validation_to_publication_seconds": (
                post_validation_to_publication_seconds
            ),
            "resources_per_second": publication_map["resource_count"]
            / post_validation_to_publication_seconds,
            "acquisition_seconds": observations.seconds_by_phase["acquisition"][0],
            "semantic_build_seconds": observations.seconds_by_phase["semantic"][0],
            "canonical_materialization_seconds": first_stage.phase_metrics[
                "canonical_materialization_seconds"
            ],
            "fact_decode_copy_seconds": first_stage.phase_metrics[
                "fact_decode_copy_seconds"
            ],
            "plan_materialize_copy_seconds": first_stage.phase_metrics[
                "plan_materialize_copy_seconds"
            ],
            "identity_proof_merge_seconds": first_stage.phase_metrics[
                "identity_proof_merge_seconds"
            ],
            "deferred_index_seconds": first_stage.phase_metrics[
                "deferred_index_seconds"
            ],
            "npi_merge_summary_seconds": first_stage.phase_metrics[
                "npi_merge_summary_seconds"
            ],
            "canonical_rows_per_second": first_stage.phase_metrics[
                "canonical_rows_per_second"
            ],
            "npi_evidence_rows_per_second": first_stage.phase_metrics[
                "npi_evidence_rows_per_second"
            ],
            "publication_seconds": observations.seconds_by_phase["publication"][0],
            "cleanup_seconds": observations.seconds_by_phase["cleanup"][0],
            "replay_pipeline_seconds": replay_pipeline_seconds,
        },
    }


async def _execute_benchmark(modules, catalog_hash, encoder_digest, database_name):
    live_relations_before = await _live_relation_snapshot(modules.importer)
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
    live_relations_after = await _live_relation_snapshot(modules.importer)
    publication_proof, cleanup_proof = await _assert_publication_state(
        modules.importer,
        observations,
        candidate,
        publication_map,
        first_stage,
        replay_stage,
        live_relations_before,
        live_relations_after,
    )
    return _benchmark_event_map(
        observations,
        stats_by_name,
        input_identity_by_name,
        publication_map,
        publication_proof,
        cleanup_proof,
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
    benchmark_event_map["correctness"]["cleanup"].update(
        await _assert_database_quiescent(modules, database_name)
    )
    event_path.write_text(
        json.dumps(benchmark_event_map, sort_keys=True) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    asyncio.run(_benchmark())
