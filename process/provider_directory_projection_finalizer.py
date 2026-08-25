# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Atomically prove, index, attach, and seal one completed projection stage."""

from __future__ import annotations

from typing import Any, Mapping

from db.connection import db
from process.provider_directory_projection_contract import (
    projection_reducer_proof,
    reduced_physical_projection_proof,
)
from process.provider_directory_projection_db import (
    json_value,
    locked_active_recipe,
    recipe_database_identity,
    row_mapping,
    set_local_projection_action,
    set_local_projection_maintenance_work_mem,
    set_local_projection_synchronous_commit,
    table_ref,
)
from process.provider_directory_projection_finalizer_proof import (
    assert_no_live_children,
    completed_shards,
    materialize_winner_set,
    prepare_immutable_stage,
    projection_stage,
    verify_stage_shards,
)
from process.provider_directory_projection_finalizer_semantic import semantic_proof
from process.provider_directory_projection_lease import heartbeat_projection_lease
from process.provider_directory_projection_types import (
    PROJECTION_CONTENT_HASH_CONTRACT_ID,
    PhysicalProjectionProof,
    PreparedProjectionStage,
    ProjectionLease,
    ProviderDirectoryProjectionError,
    ProviderDirectoryProjectionLeaseLost,
    required_hash,
    stable_json,
)


_MAX_RETAIN_SECONDS = 365 * 24 * 60 * 60


async def _mark_proof_ready(
    connection: Any,
    schema: str,
    lease: ProjectionLease,
    proof: PhysicalProjectionProof,
) -> None:
    await set_local_projection_action(
        connection,
        "proof_ready",
        recipe_id=lease.recipe.recipe_id,
        recipe_attempt=lease.attempt,
        recipe_lease_token=lease.lease_token,
    )
    updated_count = await connection.status(
        f"""
        UPDATE {table_ref(schema, 'provider_directory_projection_recipe')}
           SET status = 'proof_ready',
               prepared_proof_json = CAST(:proof_json AS jsonb),
               updated_at = clock_timestamp()
         WHERE recipe_id = :recipe_id AND attempt = :attempt
           AND status = 'building' AND lease_token = :lease_token
           AND lease_expires_at > clock_timestamp();
        """,
        proof_json=stable_json(proof.proof),
        recipe_id=lease.recipe.recipe_id,
        attempt=lease.attempt,
        lease_token=lease.lease_token,
    )
    if updated_count != 1:
        raise ProviderDirectoryProjectionLeaseLost(
            "provider_directory_projection_lease_lost"
        )


def _physical_value_by_name(
    lease: ProjectionLease,
    prepared: PreparedProjectionStage,
    proof: PhysicalProjectionProof,
    retain_seconds: int,
) -> dict[str, Any]:
    return {
        "physical_projection_id": proof.physical_projection_id,
        "canonical_row_sha256": proof.canonical_row_sha256,
        "content_hash_contract_id": PROJECTION_CONTENT_HASH_CONTRACT_ID,
        "decoder_contract_id": lease.recipe.decoder_contract_id,
        "input_set_sha256": lease.recipe.input_set_sha256,
        "transform_contract_id": lease.recipe.transform_contract_id,
        "scope_contract_id": lease.recipe.scope_contract_id,
        "transform_context_hash": lease.recipe.transform_context_hash,
        "transform_context_json": stable_json(lease.recipe.transform_context),
        "dataset_hash": proof.dataset_hash,
        "resource_profile_hash": lease.recipe.resource_profile_hash,
        "selected_resources_json": stable_json(lease.recipe.selected_resources),
        "required_resources_json": stable_json(lease.recipe.required_resources),
        "resource_count": proof.resource_count,
        "resource_counts_json": stable_json(proof.resource_counts),
        "proof_json": stable_json(proof.proof),
        "storage_schema": prepared.stage.schema,
        "storage_relation": prepared.stage.relation,
        "storage_relation_oid": prepared.stage.relation_oid,
        "storage_trigger_oid": prepared.storage_trigger_oid,
        "retain_seconds": retain_seconds,
    }


async def _insert_physical_projection(
    connection: Any,
    schema: str,
    value_by_name: Mapping[str, Any],
) -> int:
    return await connection.status(
        f"""
        WITH seal_clock AS (
            SELECT clock_timestamp() AS sealed_at
        )
        INSERT INTO {table_ref(schema, 'provider_directory_physical_projection')} (
            physical_projection_id, canonical_row_sha256,
            content_hash_contract_id, decoder_contract_id, input_set_sha256,
            transform_contract_id, scope_contract_id, transform_context_hash,
            transform_context_json, dataset_hash, resource_profile_hash,
            selected_resources_json, required_resources_json, resource_count,
            resource_counts_json, proof_json, storage_schema, storage_relation,
            storage_relation_oid, storage_trigger_oid, status, created_at,
            sealed_at, retain_until
        ) SELECT
            :physical_projection_id, :canonical_row_sha256,
            :content_hash_contract_id, :decoder_contract_id, :input_set_sha256,
            :transform_contract_id, :scope_contract_id, :transform_context_hash,
            CAST(:transform_context_json AS jsonb), :dataset_hash,
            :resource_profile_hash, CAST(:selected_resources_json AS jsonb),
            CAST(:required_resources_json AS jsonb), :resource_count,
            CAST(:resource_counts_json AS jsonb), CAST(:proof_json AS jsonb),
            :storage_schema, :storage_relation, :storage_relation_oid,
            :storage_trigger_oid, 'sealed', seal_clock.sealed_at,
            seal_clock.sealed_at,
            seal_clock.sealed_at + make_interval(secs => :retain_seconds)
          FROM seal_clock;
        """,
        **value_by_name,
    )


async def _insert_source_summary(
    connection: Any,
    schema: str,
    proof: PhysicalProjectionProof,
) -> int:
    source_summary_map = proof.proof["source_summary"]
    return await connection.status(
        f"""
        INSERT INTO {table_ref(schema, 'provider_directory_physical_projection_source_summary')} (
            physical_projection_id, canonical_row_sha256, dataset_hash,
            resource_count, resource_counts_json, proof_json, created_at
        ) VALUES (
            :physical_projection_id, :canonical_row_sha256, :dataset_hash,
            :resource_count, CAST(:resource_counts_json AS jsonb),
            CAST(:proof_json AS jsonb), now()
        );
        """,
        physical_projection_id=proof.physical_projection_id,
        canonical_row_sha256=source_summary_map["canonical_row_sha256"],
        dataset_hash=source_summary_map["dataset_hash"],
        resource_count=source_summary_map["resource_count"],
        resource_counts_json=stable_json(source_summary_map["resource_counts"]),
        proof_json=stable_json(source_summary_map),
    )


async def _insert_partitions(
    connection: Any,
    schema: str,
    proof: PhysicalProjectionProof,
) -> int:
    return await connection.status(
        f"""
        INSERT INTO {table_ref(schema, 'provider_directory_physical_projection_partition')} (
            physical_projection_id, proof_partition_id, partition_ordinal,
            resource_type, canonical_row_sha256, resource_count,
            proof_json, created_at
        )
        SELECT :physical_projection_id, shard ->> 'partition_id',
               (shard ->> 'partition_ordinal')::integer,
               shard ->> 'resource_type', shard ->> 'canonical_row_sha256',
               (shard ->> 'resource_count')::bigint, shard, now()
          FROM jsonb_array_elements(CAST(:shards_json AS jsonb)) AS shard;
        """,
        physical_projection_id=proof.physical_projection_id,
        shards_json=stable_json(proof.proof["raw_shards"]),
    )


async def _insert_projection_catalog(
    connection: Any,
    schema: str,
    lease: ProjectionLease,
    prepared: PreparedProjectionStage,
    proof: PhysicalProjectionProof,
    retain_seconds: int,
) -> None:
    inserted_count = await _insert_physical_projection(
        connection,
        schema,
        _physical_value_by_name(lease, prepared, proof, retain_seconds),
    )
    summary_count = await _insert_source_summary(connection, schema, proof)
    partition_count = await _insert_partitions(connection, schema, proof)
    if (
        inserted_count != 1
        or summary_count != 1
        or partition_count != len(proof.proof["raw_shards"])
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_finalizer_catalog_mismatch"
        )


async def _seal(
    connection: Any,
    schema: str,
    lease: ProjectionLease,
    prepared: PreparedProjectionStage,
    proof: PhysicalProjectionProof,
    retain_seconds: int,
) -> None:
    projection_id = required_hash(proof.physical_projection_id, "physical_projection_id")
    await set_local_projection_action(
        connection,
        "seal",
        recipe_id=lease.recipe.recipe_id,
        recipe_attempt=lease.attempt,
        recipe_lease_token=lease.lease_token,
        physical_projection_id=proof.physical_projection_id,
    )
    await _insert_projection_catalog(
        connection, schema, lease, prepared, proof, retain_seconds
    )
    await connection.status(
        f"ALTER TABLE {table_ref(schema, 'provider_directory_physical_projection_resource')} "
        f"ATTACH PARTITION {table_ref(prepared.stage.schema, prepared.stage.relation)} "
        f"FOR VALUES IN ('{projection_id}');"
    )
    sealed_count = await connection.status(
        f"""
        WITH seal_clock AS (SELECT clock_timestamp() AS sealed_at),
        physical_seal AS (
            UPDATE {table_ref(schema, 'provider_directory_physical_projection')} AS physical
               SET sealed_at = seal_clock.sealed_at,
                   retain_until = seal_clock.sealed_at + make_interval(secs => :retain_seconds)
              FROM seal_clock
             WHERE physical.physical_projection_id = :projection_id AND physical.status = 'sealed'
         RETURNING physical.sealed_at
        )
        UPDATE {table_ref(schema, 'provider_directory_projection_recipe')}
           SET status = 'sealed', physical_projection_id = :projection_id,
               lease_token = NULL, lease_expires_at = NULL,
               lease_heartbeat_at = NULL,
               sealed_at = physical_seal.sealed_at,
               updated_at = physical_seal.sealed_at
          FROM physical_seal
         WHERE recipe_id = :recipe_id AND attempt = :attempt
           AND status = 'proof_ready' AND lease_token = :lease_token
           AND lease_expires_at > clock_timestamp();
        """,
        projection_id=proof.physical_projection_id,
        recipe_id=lease.recipe.recipe_id,
        attempt=lease.attempt,
        lease_token=lease.lease_token,
        retain_seconds=retain_seconds,
    )
    if sealed_count != 1:
        raise ProviderDirectoryProjectionLeaseLost(
            "provider_directory_projection_lease_lost"
        )


async def _sealed_replay(
    connection: Any,
    schema: str,
    lease: ProjectionLease,
    recipe_fields: Mapping[str, Any],
) -> PhysicalProjectionProof | None:
    if recipe_fields.get("status") != "sealed":
        return None
    if (
        int(recipe_fields.get("attempt") or 0) != lease.attempt
        or recipe_database_identity(recipe_fields) != lease.recipe.identity_payload
        or recipe_fields.get("physical_projection_id") != lease.recipe.recipe_id
    ):
        raise ProviderDirectoryProjectionLeaseLost(
            "provider_directory_projection_lease_lost"
        )
    physical_fields = row_mapping(
        await connection.first(
            f"""
            SELECT canonical_row_sha256, dataset_hash, resource_count,
                   resource_counts_json, proof_json
              FROM {table_ref(schema, 'provider_directory_physical_projection')}
             WHERE physical_projection_id = :projection_id AND status = 'sealed'
             FOR SHARE;
            """,
            projection_id=lease.recipe.recipe_id,
        )
    )
    proof_map = json_value(physical_fields.get("proof_json"))
    resource_counts = json_value(physical_fields.get("resource_counts_json"))
    if (
        not isinstance(proof_map, Mapping)
        or not isinstance(resource_counts, Mapping)
        or proof_map.get("physical_projection_id") != lease.recipe.recipe_id
        or proof_map.get("canonical_row_sha256")
        != physical_fields.get("canonical_row_sha256")
        or proof_map.get("dataset_hash") != physical_fields.get("dataset_hash")
        or proof_map.get("resource_count") != physical_fields.get("resource_count")
        or proof_map.get("resource_counts") != resource_counts
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_finalizer_replay_invalid"
        )
    return PhysicalProjectionProof(
        physical_projection_id=lease.recipe.recipe_id,
        canonical_row_sha256=str(physical_fields["canonical_row_sha256"]),
        dataset_hash=str(physical_fields["dataset_hash"]),
        resource_count=int(physical_fields["resource_count"]),
        resource_counts={str(name): int(count) for name, count in resource_counts.items()},
        proof=dict(proof_map),
    )


async def _prepared_projection(
    connection: Any,
    schema: str,
    lease: ProjectionLease,
    recipe_fields: Mapping[str, Any],
) -> PreparedProjectionStage:
    stage = projection_stage(recipe_fields)
    await connection.status(
        f"LOCK TABLE {table_ref(stage.schema, stage.relation)} IN SHARE MODE;"
    )
    await assert_no_live_children(connection, schema, lease)
    shard_proofs = await completed_shards(
        connection,
        schema,
        lease,
        int(recipe_fields.get("partition_count") or 0),
    )
    await verify_stage_shards(connection, stage, lease, shard_proofs)
    await materialize_winner_set(connection, stage, lease)
    outcome_proof, dataset_hash, observed_count_by_type = await semantic_proof(
        connection, stage, lease
    )
    resource_count_by_type = {
        resource_type: observed_count_by_type.get(resource_type, 0)
        for resource_type in lease.recipe.selected_resources
    }
    proof = reduced_physical_projection_proof(
        lease.recipe,
        shard_proofs,
        dataset_hash=dataset_hash,
        canonical_row_sha256=outcome_proof.canonical_row_sha256,
        resource_counts=resource_count_by_type,
        reducer_proof=projection_reducer_proof(
            outcome_proof, resource_count_by_type
        ),
        outcome_proof=outcome_proof,
    )
    prepared = await prepare_immutable_stage(connection, schema, stage, proof)
    await connection.status(f"ANALYZE {table_ref(stage.schema, stage.relation)};")
    return prepared


async def finalize_projection(
    lease: ProjectionLease,
    *,
    retain_seconds: int,
    database: Any = db,
    schema: str = "mrf",
) -> PhysicalProjectionProof:
    """Seal one complete stage or return its exact already-sealed proof."""

    if (
        type(lease) is not ProjectionLease
        or type(retain_seconds) is not int
        or not 0 <= retain_seconds <= _MAX_RETAIN_SECONDS
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_finalizer_input_invalid"
        )
    recipe_table = table_ref(schema, "provider_directory_projection_recipe")
    async with database.acquire() as connection:
        recipe_fields = row_mapping(
            await connection.first(
                f"SELECT * FROM {recipe_table} WHERE recipe_id = :recipe_id FOR UPDATE;",
                recipe_id=lease.recipe.recipe_id,
            )
        )
        replay = await _sealed_replay(connection, schema, lease, recipe_fields)
        if replay is not None:
            return replay
        recipe_fields = await locked_active_recipe(
            lease,
            database=connection,
            schema=schema,
        )
        await heartbeat_projection_lease(
            lease,
            lease_seconds=3600,
            database=connection,
            schema=schema,
        )
        await set_local_projection_synchronous_commit(connection, "on")
        await set_local_projection_maintenance_work_mem(connection)
        prepared = await _prepared_projection(
            connection, schema, lease, recipe_fields
        )
        await _mark_proof_ready(connection, schema, lease, prepared.proof)
        await _seal(
            connection, schema, lease, prepared, prepared.proof, retain_seconds
        )
        return prepared.proof


__all__ = ("finalize_projection",)
