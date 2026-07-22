# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable acquisition attestations bound many-to-one to physical recipes."""

from __future__ import annotations

import secrets
from typing import Any, Mapping, Sequence

from db.connection import db
from process.provider_directory_projection_admission_contract import (
    PROJECTION_ADMISSION_CONTRACT_ID,
    admission_registration_inputs,
    projection_admission_consumer_id,
    projection_admission_identity,
)
from process.provider_directory_projection_db import (
    json_value,
    recipe_database_identity,
    row_mapping,
    set_local_projection_action,
    table_ref,
)
from process.provider_directory_projection_types import (
    ProjectionAdmissionIdentity,
    ProjectionAdmissionInputBlock,
    ProjectionAdmissionTerminalZero,
    ProjectionLease,
    ProjectionRecipeIdentity,
    ProjectionShardSpec,
    ProviderDirectoryProjectionBusy,
    ProviderDirectoryProjectionError,
    stable_json,
)
from process.provider_directory_projection_workset import (
    _stored_workset,
)


_AdmissionRegistration = tuple[
    ProjectionAdmissionIdentity,
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]


def _admission_database_identity(database_fields: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "admission_id": database_fields.get("admission_id"),
        "recipe_id": database_fields.get("recipe_id"),
        "acquisition_adapter_id": database_fields.get("acquisition_adapter_id"),
        "source_scope_hash": database_fields.get("source_scope_hash"),
        "source_ids": json_value(database_fields.get("source_ids_json")),
        "completeness_manifest_hash": database_fields.get(
            "completeness_manifest_hash"
        ),
        "completeness_manifest": json_value(
            database_fields.get("completeness_manifest_json")
        ),
        "retained_campaign_id": database_fields.get("retained_campaign_id"),
        "retained_campaign_sha256": database_fields.get(
            "retained_campaign_sha256"
        ),
        "retained_consumer_recipe_id": database_fields.get(
            "retained_consumer_recipe_id"
        ),
        "claim_generation": database_fields.get("claim_generation"),
        "input_block_set_sha256": database_fields.get("input_block_set_sha256"),
        "input_block_count": database_fields.get("input_block_count"),
        "binding_count": database_fields.get("binding_count"),
        "binding_set_sha256": database_fields.get("binding_set_sha256"),
        "stream_set_sha256": database_fields.get("stream_set_sha256"),
        "stream_count": database_fields.get("stream_count"),
    }


def _admission_identity_fields(identity: ProjectionAdmissionIdentity) -> dict[str, Any]:
    return {
        "admission_id": identity.admission_id,
        "recipe_id": identity.recipe_id,
        "acquisition_adapter_id": identity.acquisition_adapter_id,
        "source_scope_hash": identity.source_scope_hash,
        "source_ids": list(identity.source_ids),
        "completeness_manifest_hash": identity.completeness_manifest_hash,
        "completeness_manifest": dict(identity.completeness_manifest),
        "retained_campaign_id": identity.retained_campaign_id,
        "retained_campaign_sha256": identity.retained_campaign_sha256,
        "retained_consumer_recipe_id": identity.retained_consumer_recipe_id,
        "claim_generation": identity.claim_generation,
        "input_block_set_sha256": identity.input_block_set_sha256,
        "input_block_count": identity.input_block_count,
        "binding_count": identity.binding_count,
        "binding_set_sha256": identity.binding_set_sha256,
        "stream_set_sha256": identity.stream_set_sha256,
        "stream_count": identity.stream_count,
    }


async def _insert_admission(
    database: Any,
    table: str,
    identity: ProjectionAdmissionIdentity,
    lease_token: str,
    lease_seconds: int,
) -> None:
    await database.status(
        f"""
        INSERT INTO {table} (
            admission_id, recipe_id, acquisition_adapter_id,
            source_scope_hash, source_ids_json, completeness_manifest_hash,
            completeness_manifest_json, retained_campaign_id,
            retained_campaign_sha256, retained_consumer_recipe_id,
            claim_generation, input_block_set_sha256, input_block_count,
            binding_count, binding_set_sha256, stream_set_sha256, stream_count,
            status, attempt, lease_token, lease_expires_at,
            lease_heartbeat_at, created_at, updated_at
        ) VALUES (
            :admission_id, :recipe_id, :acquisition_adapter_id,
            :source_scope_hash, CAST(:source_ids_json AS jsonb),
            :completeness_manifest_hash,
            CAST(:completeness_manifest_json AS jsonb),
            :retained_campaign_id, :retained_campaign_sha256,
            :retained_consumer_recipe_id, :claim_generation,
            :input_block_set_sha256, :input_block_count,
            :binding_count, :binding_set_sha256,
            :stream_set_sha256, :stream_count,
            'building', 1, :lease_token,
            now() + make_interval(secs => :lease_seconds),
            now(), now(), now()
        ) ON CONFLICT (admission_id) DO NOTHING;
        """,
        admission_id=identity.admission_id,
        recipe_id=identity.recipe_id,
        acquisition_adapter_id=identity.acquisition_adapter_id,
        source_scope_hash=identity.source_scope_hash,
        source_ids_json=stable_json(identity.source_ids),
        completeness_manifest_hash=identity.completeness_manifest_hash,
        completeness_manifest_json=stable_json(identity.completeness_manifest),
        retained_campaign_id=identity.retained_campaign_id,
        retained_campaign_sha256=identity.retained_campaign_sha256,
        retained_consumer_recipe_id=identity.retained_consumer_recipe_id,
        claim_generation=identity.claim_generation,
        input_block_set_sha256=identity.input_block_set_sha256,
        input_block_count=identity.input_block_count,
        binding_count=identity.binding_count,
        binding_set_sha256=identity.binding_set_sha256,
        stream_set_sha256=identity.stream_set_sha256,
        stream_count=identity.stream_count,
        lease_token=lease_token,
        lease_seconds=lease_seconds,
    )


async def _insert_admission_mappings(
    database: Any,
    schema: str,
    mappings: Sequence[Mapping[str, Any]],
) -> None:
    await database.status(
        f"""
        INSERT INTO {table_ref(schema, 'provider_directory_projection_admission_input_block')} (
            admission_id, recipe_id, binding_id, block_id, retained_campaign_id,
            retained_consumer_recipe_id, retained_source_item_id,
            retained_artifact_sha256, retained_layout_sha256,
            retained_range_ordinal, claim_generation, resource_type,
            partition_key_hash, source_partition_ordinal, created_at
        )
        SELECT mapping.admission_id, mapping.recipe_id, mapping.binding_id,
               mapping.block_id,
               mapping.retained_campaign_id,
               mapping.retained_consumer_recipe_id,
               mapping.retained_source_item_id,
               mapping.retained_artifact_sha256,
               mapping.retained_layout_sha256,
               mapping.retained_range_ordinal,
               mapping.claim_generation, mapping.resource_type,
               mapping.partition_key_hash, mapping.source_partition_ordinal,
               now()
          FROM jsonb_to_recordset(CAST(:mappings_json AS jsonb)) AS mapping(
               admission_id varchar(64), recipe_id varchar(64),
               binding_id varchar(64), block_id varchar(64),
               retained_campaign_id varchar(64),
               retained_consumer_recipe_id varchar(128),
               retained_source_item_id varchar(64),
               retained_artifact_sha256 varchar(64),
               retained_layout_sha256 varchar(64),
               retained_range_ordinal integer, claim_generation bigint,
               resource_type varchar(64), partition_key_hash varchar(64),
               source_partition_ordinal integer);
        """,
        mappings_json=stable_json(mappings),
    )


async def _insert_admission_streams(
    database: Any,
    schema: str,
    streams: Sequence[Mapping[str, Any]],
) -> None:
    """Persist exact terminal-zero retained stream bindings."""

    await database.status(
        f"""
        INSERT INTO {table_ref(schema, 'provider_directory_projection_admission_stream')} (
            admission_id, recipe_id, binding_id, retained_campaign_id,
            retained_source_item_id, claim_generation, resource_type,
            partition_key_hash, source_partition_ordinal,
            stream_identity_sha256, stream_ordinal,
            terminal_sequence_ordinal, terminal_proof_sha256, created_at
        )
        SELECT stream.admission_id, stream.recipe_id, stream.binding_id,
               stream.retained_campaign_id, stream.retained_source_item_id,
               stream.claim_generation, stream.resource_type,
               stream.partition_key_hash, stream.source_partition_ordinal,
               stream.stream_identity_sha256, stream.stream_ordinal,
               stream.terminal_sequence_ordinal,
               stream.terminal_proof_sha256, now()
          FROM jsonb_to_recordset(CAST(:streams_json AS jsonb)) AS stream(
               admission_id varchar(64), recipe_id varchar(64),
               binding_id varchar(64), retained_campaign_id varchar(64),
               retained_source_item_id varchar(64), claim_generation bigint,
               resource_type varchar(64), partition_key_hash varchar(64),
               source_partition_ordinal integer,
               stream_identity_sha256 varchar(64), stream_ordinal integer,
               terminal_sequence_ordinal integer,
               terminal_proof_sha256 varchar(64));
        """,
        streams_json=stable_json(streams),
    )


async def _validated_recipe_attempt(
    database: Any,
    schema: str,
    recipe: ProjectionRecipeIdentity,
    identity: ProjectionAdmissionIdentity,
    lease_token: str,
    expected_blocks: Sequence[Mapping[str, Any]],
    expected_shards: Sequence[Mapping[str, Any]],
) -> int:
    recipe_fields = row_mapping(
        await database.first(
            f"SELECT * FROM {table_ref(schema, 'provider_directory_projection_recipe')} "
            "WHERE recipe_id = :recipe_id FOR SHARE;",
            recipe_id=identity.recipe_id,
        )
    )
    recipe_attempt = int(recipe_fields.get("attempt") or 0)
    if (
        recipe_fields.get("status") not in {"building", "proof_ready", "sealed"}
        or recipe_database_identity(recipe_fields) != recipe.identity_payload
        or recipe_fields.get("workset_registered_at") is None
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_admission_recipe_invalid"
        )
    stored_blocks, stored_shards = await _stored_workset(
        database,
        schema,
        ProjectionLease(recipe.physical, recipe_attempt, lease_token),
    )
    if stored_blocks != expected_blocks or stored_shards != expected_shards:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_admission_workset_mismatch"
        )
    return recipe_attempt


async def _claim_admission_attempt(
    database: Any,
    admission_table: str,
    identity: ProjectionAdmissionIdentity,
    recipe_attempt: int,
    lease_token: str,
    lease_seconds: int,
) -> int | None:
    """Insert, reuse, or reclaim one exact admission lease generation."""

    admission_fields = await _locked_admission_fields(
        database,
        admission_table,
        identity,
        recipe_attempt,
        lease_token,
        lease_seconds,
    )
    if admission_fields.get("status") == "sealed":
        return None
    if admission_fields.get("status") != "building":
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_admission_not_reclaimable"
        )
    admission_attempt = int(admission_fields.get("attempt") or 0)
    if admission_fields.get("lease_token") == lease_token:
        return admission_attempt
    lease_expired = await database.scalar(
        "SELECT :lease_expires_at <= now();",
        lease_expires_at=admission_fields.get("lease_expires_at"),
    )
    if lease_expired is not True:
        raise ProviderDirectoryProjectionBusy(
            "provider_directory_projection_admission_busy"
        )
    await _reclaim_admission(
        database,
        admission_table,
        identity,
        recipe_attempt,
        admission_attempt,
        lease_token,
        lease_seconds,
    )
    return admission_attempt


async def _locked_admission_fields(
    database: Any,
    admission_table: str,
    identity: ProjectionAdmissionIdentity,
    recipe_attempt: int,
    lease_token: str,
    lease_seconds: int,
) -> dict[str, Any]:
    """Insert if absent, then lock and verify one admission identity."""

    await set_local_projection_action(
        database,
        "admission_insert",
        recipe_id=identity.recipe_id,
        recipe_attempt=recipe_attempt,
        admission_id=identity.admission_id,
        admission_attempt=1,
        admission_lease_token=lease_token,
    )
    await _insert_admission(
        database,
        admission_table,
        identity,
        lease_token,
        lease_seconds,
    )
    admission_fields = row_mapping(
        await database.first(
            f"SELECT * FROM {admission_table} "
            "WHERE admission_id = :admission_id FOR UPDATE;",
            admission_id=identity.admission_id,
        )
    )
    if _admission_database_identity(admission_fields) != _admission_identity_fields(
        identity
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_admission_identity_collision"
        )
    return admission_fields


async def _reclaim_admission(
    database: Any,
    admission_table: str,
    identity: ProjectionAdmissionIdentity,
    recipe_attempt: int,
    admission_attempt: int,
    lease_token: str,
    lease_seconds: int,
) -> None:
    """Fence an expired admission lease to one replacement token."""

    await set_local_projection_action(
        database,
        "admission_reclaim",
        recipe_id=identity.recipe_id,
        recipe_attempt=recipe_attempt,
        admission_id=identity.admission_id,
        admission_attempt=admission_attempt,
        admission_lease_token=lease_token,
    )
    updated_count = await database.status(
        f"""
        UPDATE {admission_table}
           SET lease_token = :lease_token,
               lease_expires_at = now() + make_interval(secs => :lease_seconds),
               lease_heartbeat_at = now(), updated_at = now()
         WHERE admission_id = :admission_id AND status = 'building';
        """,
        admission_id=identity.admission_id,
        lease_token=lease_token,
        lease_seconds=lease_seconds,
    )
    if updated_count != 1:
        raise ProviderDirectoryProjectionBusy(
            "provider_directory_projection_admission_busy"
        )


async def _ensure_admission_mappings(
    database: Any,
    schema: str,
    identity: ProjectionAdmissionIdentity,
    mappings: Sequence[Mapping[str, Any]],
    recipe_attempt: int,
    admission_attempt: int,
    lease_token: str,
) -> None:
    if not mappings:
        return
    mapping_table = table_ref(
        schema,
        "provider_directory_projection_admission_input_block",
    )
    stored_mappings = [
        row_mapping(mapping_row)
        for mapping_row in await database.all(
            f"""
            SELECT admission_id, recipe_id, binding_id, block_id,
                   retained_campaign_id, retained_consumer_recipe_id,
                   retained_source_item_id, retained_artifact_sha256,
                   retained_layout_sha256, retained_range_ordinal,
                   claim_generation, resource_type, partition_key_hash,
                   source_partition_ordinal
              FROM {mapping_table}
             WHERE admission_id = :admission_id
             ORDER BY binding_id;
            """,
            admission_id=identity.admission_id,
        )
    ]
    if stored_mappings and stored_mappings != mappings:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_admission_mapping_mismatch"
        )
    if stored_mappings:
        return
    await set_local_projection_action(
        database,
        "admission_map",
        recipe_id=identity.recipe_id,
        recipe_attempt=recipe_attempt,
        admission_id=identity.admission_id,
        admission_attempt=admission_attempt,
        admission_lease_token=lease_token,
    )
    await _insert_admission_mappings(database, schema, mappings)


async def _ensure_admission_streams(
    database: Any,
    schema: str,
    identity: ProjectionAdmissionIdentity,
    streams: Sequence[Mapping[str, Any]],
    recipe_attempt: int,
    admission_attempt: int,
    lease_token: str,
) -> None:
    """Create one immutable terminal stream set or verify its exact reuse."""

    if not streams:
        return
    stream_table = table_ref(
        schema,
        "provider_directory_projection_admission_stream",
    )
    stored_streams = [
        row_mapping(stream_row)
        for stream_row in await database.all(
            f"""
            SELECT admission_id, recipe_id, binding_id,
                   retained_campaign_id, retained_source_item_id,
                   claim_generation, resource_type, partition_key_hash,
                   source_partition_ordinal, stream_identity_sha256,
                   stream_ordinal, terminal_sequence_ordinal,
                   terminal_proof_sha256
              FROM {stream_table}
             WHERE admission_id = :admission_id
             ORDER BY binding_id;
            """,
            admission_id=identity.admission_id,
        )
    ]
    if stored_streams and stored_streams != streams:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_admission_stream_mismatch"
        )
    if stored_streams:
        return
    await set_local_projection_action(
        database,
        "admission_map",
        recipe_id=identity.recipe_id,
        recipe_attempt=recipe_attempt,
        admission_id=identity.admission_id,
        admission_attempt=admission_attempt,
        admission_lease_token=lease_token,
    )
    await _insert_admission_streams(database, schema, streams)


async def _seal_admission(
    database: Any,
    admission_table: str,
    identity: ProjectionAdmissionIdentity,
    recipe_attempt: int,
    admission_attempt: int,
    lease_token: str,
) -> None:
    await set_local_projection_action(
        database,
        "admission_seal",
        recipe_id=identity.recipe_id,
        recipe_attempt=recipe_attempt,
        admission_id=identity.admission_id,
        admission_attempt=admission_attempt,
        admission_lease_token=lease_token,
    )
    sealed_count = await database.status(
        f"""
        UPDATE {admission_table}
           SET status = 'sealed', lease_token = NULL,
               lease_expires_at = NULL, lease_heartbeat_at = NULL,
               sealed_at = now(), updated_at = now()
         WHERE admission_id = :admission_id AND status = 'building'
           AND lease_token = :lease_token;
        """,
        admission_id=identity.admission_id,
        lease_token=lease_token,
    )
    if sealed_count != 1:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_admission_seal_failed"
        )


async def _register_admission_transaction(
    recipe: ProjectionRecipeIdentity,
    registration: _AdmissionRegistration,
    lease_seconds: int,
    database: Any,
    schema: str,
) -> str:
    """Persist one already-validated admission under exact lease fences."""

    identity, block_fields, shard_fields, mappings, streams = registration
    admission_table = table_ref(schema, "provider_directory_projection_admission")
    lease_token = secrets.token_hex(32)
    async with database.transaction():
        recipe_attempt = await _validated_recipe_attempt(
            database,
            schema,
            recipe,
            identity,
            lease_token,
            block_fields,
            shard_fields,
        )
        admission_attempt = await _claim_admission_attempt(
            database,
            admission_table,
            identity,
            recipe_attempt,
            lease_token,
            lease_seconds,
        )
        if admission_attempt is None:
            return identity.admission_id
        await _ensure_admission_mappings(
            database,
            schema,
            identity,
            mappings,
            recipe_attempt,
            admission_attempt,
            lease_token,
        )
        await _ensure_admission_streams(
            database,
            schema,
            identity,
            streams,
            recipe_attempt,
            admission_attempt,
            lease_token,
        )
        await _seal_admission(
            database,
            admission_table,
            identity,
            recipe_attempt,
            admission_attempt,
            lease_token,
        )
    return identity.admission_id


async def register_projection_admission(
    recipe: ProjectionRecipeIdentity,
    blocks: Sequence[ProjectionAdmissionInputBlock],
    shards: Sequence[ProjectionShardSpec],
    *,
    claim_generation: int,
    terminal_zeros: Sequence[ProjectionAdmissionTerminalZero] = (),
    lease_seconds: int = 900,
    database: Any = db,
    schema: str = "mrf",
) -> str:
    """Validate, map, and seal one immutable acquisition admission."""

    registration = admission_registration_inputs(
        recipe,
        blocks,
        shards,
        claim_generation,
        lease_seconds,
        terminal_zeros=terminal_zeros,
    )
    return await _register_admission_transaction(
        recipe,
        registration,
        lease_seconds,
        database,
        schema,
    )


__all__ = [
    "PROJECTION_ADMISSION_CONTRACT_ID",
    "projection_admission_consumer_id",
    "projection_admission_identity",
    "register_projection_admission",
]
