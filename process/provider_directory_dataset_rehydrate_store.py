# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Durable checkpoints and exact output proof for dataset rehydration."""

from __future__ import annotations

import json

from process.provider_directory_dataset_rehydrate_types import (
    CANONICAL_TABLE,
    CHECKPOINT_TABLE,
    DATASET_RESOURCE_TABLE,
    SOURCE_EDGE_TABLE,
    DatasetRehydrationError,
    RehydrationCheckpoint,
    ResourceContext,
    ResourceProof,
    _clean_text,
    _record_fields,
    _table_ref,
)
from process.provider_directory_dataset_rehydrate_scope import _json_object


async def _load_checkpoint(
    context: ResourceContext,
) -> RehydrationCheckpoint | None:
    """Load and scope-check one durable resource checkpoint."""
    checkpoint_record = await context.runtime.database.first(
        f"""SELECT endpoint_id, dataset_hash, state, last_resource_id,
                   expected_input_count, input_count, mapped_count,
                   rejected_count, evidence_json, error
              FROM {_table_ref(context.runtime.schema, CHECKPOINT_TABLE)}
             WHERE source_id=:source_id AND dataset_id=:dataset_id
               AND acquisition_root_run_id=:root_run_id
               AND resource_type=:resource_type;""",
        **_checkpoint_key(context),
    )
    if checkpoint_record is None:
        return None
    checkpoint_fields = _record_fields(checkpoint_record)
    if (
        _clean_text(checkpoint_fields.get("endpoint_id")) != context.scope.endpoint_id
        or _clean_text(checkpoint_fields.get("dataset_hash"))
        != context.scope.dataset_hash
    ):
        raise DatasetRehydrationError(
            "provider_directory_dataset_rehydrate_checkpoint_scope_mismatch"
        )
    return RehydrationCheckpoint(
        state=_clean_text(checkpoint_fields.get("state")),
        last_resource_id=_clean_text(
            checkpoint_fields.get("last_resource_id")
        )
        or None,
        expected_count=int(checkpoint_fields.get("expected_input_count") or 0),
        input_count=int(checkpoint_fields.get("input_count") or 0),
        mapped_count=int(checkpoint_fields.get("mapped_count") or 0),
        rejected_count=int(checkpoint_fields.get("rejected_count") or 0),
        evidence_by_name=_json_object(checkpoint_fields.get("evidence_json")),
        error=_clean_text(checkpoint_fields.get("error")) or None,
    )


async def _save_checkpoint(
    context: ResourceContext,
    checkpoint: RehydrationCheckpoint,
) -> None:
    """Insert or replace scope-fenced progress for one resource type."""
    await context.runtime.database.status(
        f"""INSERT INTO {_table_ref(context.runtime.schema, CHECKPOINT_TABLE)} (
                source_id, dataset_id, acquisition_root_run_id, resource_type,
                endpoint_id, dataset_hash, owner_run_id, state,
                last_resource_id, expected_input_count, input_count,
                mapped_count, rejected_count, evidence_json, error,
                created_at, started_at, updated_at, completed_at
            ) VALUES (
                :source_id, :dataset_id, :root_run_id, :resource_type,
                :endpoint_id, :dataset_hash, :owner_run_id,
                CAST(:state AS varchar),
                :last_resource_id, :expected_count, :input_count,
                :mapped_count, :rejected_count, CAST(:evidence_json AS jsonb),
                :error, now(), now(), now(),
                CASE WHEN CAST(:state AS varchar)='complete' THEN now() END
            ) ON CONFLICT (
                source_id, dataset_id, acquisition_root_run_id, resource_type
            ) DO UPDATE SET
                endpoint_id=EXCLUDED.endpoint_id,
                dataset_hash=EXCLUDED.dataset_hash,
                owner_run_id=EXCLUDED.owner_run_id,
                state=EXCLUDED.state,
                last_resource_id=EXCLUDED.last_resource_id,
                expected_input_count=EXCLUDED.expected_input_count,
                input_count=EXCLUDED.input_count,
                mapped_count=EXCLUDED.mapped_count,
                rejected_count=EXCLUDED.rejected_count,
                evidence_json=EXCLUDED.evidence_json,
                error=EXCLUDED.error,
                updated_at=now(),
                completed_at=CASE
                    WHEN EXCLUDED.state='complete' THEN now() ELSE NULL END;""",
        **_checkpoint_key(context),
        endpoint_id=context.scope.endpoint_id,
        dataset_hash=context.scope.dataset_hash,
        owner_run_id=context.request.owner_run_id,
        state=checkpoint.state,
        last_resource_id=checkpoint.last_resource_id,
        expected_count=checkpoint.expected_count,
        input_count=checkpoint.input_count,
        mapped_count=checkpoint.mapped_count,
        rejected_count=checkpoint.rejected_count,
        evidence_json=json.dumps(checkpoint.evidence_by_name, sort_keys=True),
        error=checkpoint.error,
    )


def _checkpoint_key(context: ResourceContext) -> dict[str, str]:
    """Return SQL parameters identifying one resource checkpoint."""
    return {
        "source_id": context.scope.source_id,
        "dataset_id": context.scope.dataset_id,
        "root_run_id": context.scope.acquisition_root_run_id,
        "resource_type": context.resource_type,
    }


async def _load_resource_proof(context: ResourceContext) -> ResourceProof:
    """Count exact typed, canonical-hash, and source-edge output membership."""
    proof_record = await context.runtime.database.first(
        _resource_proof_sql(context),
        source_id=context.scope.source_id,
        dataset_id=context.scope.dataset_id,
        resource_type=context.resource_type,
        root_run_id=context.scope.acquisition_root_run_id,
        canonical_base=context.scope.canonical_api_base,
    )
    proof_fields = _record_fields(proof_record)
    return ResourceProof(
        input_count=int(proof_fields.get("input_count") or 0),
        typed_count=int(proof_fields.get("typed_count") or 0),
        typed_extra_count=int(proof_fields.get("typed_extra_count") or 0),
        canonical_hash_count=int(proof_fields.get("canonical_hash_count") or 0),
        canonical_extra_count=int(proof_fields.get("canonical_extra_count") or 0),
        source_edge_count=int(proof_fields.get("source_edge_count") or 0),
        source_edge_extra_count=int(
            proof_fields.get("source_edge_extra_count") or 0
        ),
    )


def _resource_proof_sql(context: ResourceContext) -> str:
    """Build the independent exact-membership proof query."""
    schema = context.runtime.schema
    typed_table = _table_ref(schema, context.model.__tablename__)
    dataset_table = _table_ref(schema, DATASET_RESOURCE_TABLE)
    canonical_table = _table_ref(schema, CANONICAL_TABLE)
    source_edge_table = _table_ref(schema, SOURCE_EDGE_TABLE)
    return f"""
        SELECT count(*)::bigint AS input_count,
               count(typed.resource_id) FILTER (
                   WHERE typed.last_seen_run_id=:root_run_id
               )::bigint AS typed_count,
               GREATEST((
                   SELECT count(*) FROM {typed_table} typed_extra
                    WHERE typed_extra.source_id=:source_id
                      AND typed_extra.last_seen_run_id=:root_run_id
               ) - count(typed.resource_id) FILTER (
                   WHERE typed.last_seen_run_id=:root_run_id
               ), 0)::bigint AS typed_extra_count,
               count(canonical.resource_id) FILTER (
                   WHERE canonical.last_seen_run_id=:root_run_id
                     AND canonical.payload_hash=dataset.payload_hash
               )::bigint AS canonical_hash_count,
               (SELECT count(*) FROM {canonical_table} canonical_extra
                 WHERE canonical_extra.canonical_api_base=:canonical_base
                   AND canonical_extra.resource_type=:resource_type
                   AND canonical_extra.last_seen_run_id=:root_run_id
                   AND NOT EXISTS (
                       SELECT 1 FROM {dataset_table} retained_resource
                        WHERE retained_resource.dataset_id=:dataset_id
                          AND retained_resource.resource_type=:resource_type
                          AND retained_resource.resource_id=
                              canonical_extra.resource_id
                   ))::bigint AS canonical_extra_count,
               count(source_edge.resource_id) FILTER (
                   WHERE source_edge.last_seen_run_id=:root_run_id
               )::bigint AS source_edge_count,
               GREATEST((
                   SELECT count(*) FROM {source_edge_table} edge_extra
                    WHERE edge_extra.source_id=:source_id
                      AND edge_extra.resource_type=:resource_type
                      AND edge_extra.last_seen_run_id=:root_run_id
               ) - count(source_edge.resource_id) FILTER (
                   WHERE source_edge.last_seen_run_id=:root_run_id
               ), 0)::bigint AS source_edge_extra_count
          FROM {dataset_table} dataset
          LEFT JOIN {typed_table} typed
            ON typed.source_id=:source_id
           AND typed.resource_id=dataset.resource_id
          LEFT JOIN {canonical_table} canonical
            ON canonical.canonical_api_base=:canonical_base
           AND canonical.resource_type=dataset.resource_type
           AND canonical.resource_id=dataset.resource_id
          LEFT JOIN {source_edge_table} source_edge
            ON source_edge.source_id=:source_id
           AND source_edge.canonical_api_base=:canonical_base
           AND source_edge.resource_type=dataset.resource_type
           AND source_edge.resource_id=dataset.resource_id
         WHERE dataset.dataset_id=:dataset_id
           AND dataset.resource_type=:resource_type;"""


def _is_proof_complete(
    checkpoint: RehydrationCheckpoint,
    proof: ResourceProof,
) -> bool:
    """Return whether checkpoint and independent proof exactly agree."""
    expected_count = checkpoint.expected_count
    return (
        checkpoint.input_count == expected_count
        and checkpoint.mapped_count == expected_count
        and checkpoint.rejected_count == 0
        and proof.input_count == expected_count
        and proof.typed_count == expected_count
        and proof.typed_extra_count == 0
        and proof.canonical_hash_count == expected_count
        and proof.canonical_extra_count == 0
        and proof.source_edge_count == expected_count
        and proof.source_edge_extra_count == 0
    )
