# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import os
from typing import Any


MAXIMUM_PUBLICATION_SECONDS = 1_800
MINIMUM_RESOURCES_PER_SECOND = 18_920

_STAGE_METRIC_NAMES = (
    "canonical_materialization_seconds",
    "fact_decode_copy_seconds",
    "plan_materialize_copy_seconds",
    "identity_proof_merge_seconds",
    "deferred_index_seconds",
    "npi_merge_summary_seconds",
    "canonical_rows_per_second",
    "npi_evidence_rows_per_second",
)


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
        relation_rows = await connection.fetch(
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
        relation_row["relation_name"]: {
            "oid": int(relation_row["relation_oid"]),
            "persistence": relation_row["persistence"],
            "index_count": int(relation_row["index_count"]),
            "indexes_valid_ready_live": bool(relation_row["indexes_valid_ready_live"]),
        }
        for relation_row in relation_rows
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


async def _final_publication_proof(importer, candidate, first_stage):
    state = await importer._endpoint_dataset_state(candidate.dataset_id)
    return importer.validate_uhc_final_publication(
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


async def _committed_receipt_by_name(importer, candidate) -> dict[str, Any]:
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
    return dict(receipt_row._mapping) if receipt_row is not None else {}


def _assert_receipt_matches(
    importer, proof, first_stage, receipt_by_name: dict[str, Any]
) -> None:
    canonical_proof_by_name = proof.canonical_proof
    if (
        proof.summary_input != first_stage.summary_input
        or receipt_by_name.get("seal_valid") is not True
        or receipt_by_name.get("content_proof_admission_version")
        != importer.ADMISSION_SEAL_VERSION
        or receipt_by_name.get("content_proof_admission_kind")
        != importer.ADMISSION_KIND_UHC_CANONICAL
        or receipt_by_name.get("content_proof_admission_sha256")
        != canonical_proof_by_name["proof_sha256"]
        or tuple(receipt_by_name.get("content_proof_resource_types") or ())
        != tuple(sorted(proof.resource_counts))
    ):
        raise RuntimeError("benchmark committed publication receipt is invalid")


def _canonical_proof_by_name(proof) -> dict[str, Any]:
    canonical_proof = proof.canonical_proof
    npi_evidence_by_name = canonical_proof["npi_evidence"]
    return {
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
    } | {
        "npi_evidence_proof_sha256": npi_evidence_by_name["proof_sha256"],
        "npi_evidence_shard_set_sha256": npi_evidence_by_name["shard_set_sha256"],
    }


def _publication_proof_by_name(
    candidate, proof, receipt_by_name: dict[str, Any]
) -> dict[str, Any]:
    return {
        "lineage": {
            "source_id": candidate.source_ids[0],
            "endpoint_id": candidate.endpoint_id,
            "dataset_id": candidate.dataset_id,
            "acquisition_root_run_id": candidate.acquisition_root_run_id,
            "import_run_id": candidate.import_run_id,
            "selected_resources": list(candidate.selected_resources),
        },
        "canonical_proof": _canonical_proof_by_name(proof),
        "receipt": {
            "publication_metadata_sha256": receipt_by_name[
                "publication_metadata_sha256"
            ],
            "content_proof_admission_version": receipt_by_name[
                "content_proof_admission_version"
            ],
            "content_proof_admission_kind": receipt_by_name[
                "content_proof_admission_kind"
            ],
            "content_proof_admission_sha256": receipt_by_name[
                "content_proof_admission_sha256"
            ],
            "content_proof_resource_types": list(
                receipt_by_name["content_proof_resource_types"]
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


async def _publication_correctness_proof(importer, candidate, first_stage):
    proof = await _final_publication_proof(importer, candidate, first_stage)
    receipt_by_name = await _committed_receipt_by_name(importer, candidate)
    _assert_receipt_matches(importer, proof, first_stage, receipt_by_name)
    return _publication_proof_by_name(candidate, proof, receipt_by_name)


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
    expected_pointer_map = {"row_count": 1, "dataset_id": candidate.dataset_id}
    if (
        current_pointer_map != expected_pointer_map
        or post_cleanup_publication_map != publication_map
    ):
        raise RuntimeError("publication pointer or post-cleanup proof is incomplete")
    cleanup_proof_by_name = {
        "canonical_stages_removed": True,
        **_stage_lifecycle_proof(observations),
        **_live_relation_lifecycle_proof(
            live_relations_before,
            live_relations_after,
        ),
    }
    return (
        await _publication_correctness_proof(importer, candidate, first_stage),
        cleanup_proof_by_name,
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
        state_row = await connection.fetchrow(
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
    counts_by_name = {
        "other_client_sessions": int(state_row["other_client_sessions"]),
        "ungranted_locks": int(state_row["ungranted_locks"]),
    }
    if any(counts_by_name.values()):
        raise RuntimeError("benchmark database did not quiesce")
    return {**counts_by_name, "database_quiescent": True}


def _correctness_by_name(
    observations,
    stats_by_name,
    input_identity_by_name,
    publication_map,
    publication_proof_by_name,
    cleanup_proof_by_name,
) -> dict[str, Any]:
    return {
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
            "canonical_proof": publication_proof_by_name["canonical_proof"],
        },
        "publication": {
            "status": publication_map["status"],
            "is_current": publication_map["is_current"],
            "committed_receipt_matches": True,
            "single_current_pointer": True,
            "fresh_then_replay": True,
            "lineage": publication_proof_by_name["lineage"],
            "receipt": publication_proof_by_name["receipt"],
            "source_summary": publication_proof_by_name["source_summary"],
        },
        "cleanup": cleanup_proof_by_name,
    }


def _metrics_by_name(
    observations,
    publication_map,
    first_stage,
    pipeline_seconds_by_name,
) -> dict[str, float]:
    publication_seconds = observations.seconds_by_phase[
        "post_validation_to_publication"
    ][0]
    stage_metrics_by_name = {
        name: first_stage.phase_metrics[name] for name in _STAGE_METRIC_NAMES
    }
    return {
        "pipeline_seconds": pipeline_seconds_by_name["fresh"],
        "post_validation_to_publication_seconds": publication_seconds,
        "resources_per_second": publication_map["resource_count"] / publication_seconds,
        "acquisition_seconds": observations.seconds_by_phase["acquisition"][0],
        "semantic_build_seconds": observations.seconds_by_phase["semantic"][0],
        **stage_metrics_by_name,
        "publication_seconds": observations.seconds_by_phase["publication"][0],
        "cleanup_seconds": observations.seconds_by_phase["cleanup"][0],
        "replay_pipeline_seconds": pipeline_seconds_by_name["replay"],
    }


def _benchmark_event_map(
    observations,
    stats_by_name,
    input_identity_by_name,
    publication_map,
    publication_proof_by_name,
    cleanup_proof_by_name,
    first_stage,
    pipeline_seconds_by_name,
):
    return {
        "schema_version": 1,
        "correctness": _correctness_by_name(
            observations,
            stats_by_name,
            input_identity_by_name,
            publication_map,
            publication_proof_by_name,
            cleanup_proof_by_name,
        ),
        "metrics": _metrics_by_name(
            observations,
            publication_map,
            first_stage,
            pipeline_seconds_by_name,
        ),
    }
