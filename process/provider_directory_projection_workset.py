# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Content-stable input manifests and lease-fenced projection shard work."""

from __future__ import annotations

import secrets
from typing import Any, Mapping, Sequence

from db.connection import db
from process.provider_directory_projection_contract import (
    projection_input_block,
    projection_input_set_sha256,
    projection_shard_spec,
    validated_physical_projection_recipe_identity,
)
from process.provider_directory_projection_db import (
    json_value,
    locked_active_recipe,
    row_mapping,
    set_local_projection_action,
    shared_active_recipe,
    table_ref,
)
from process.provider_directory_projection_types import (
    HASH_PATTERN,
    PROJECTION_MIXED_RESOURCE_TYPE,
    ProjectionInputBlock,
    ProjectionLease,
    PhysicalProjectionRecipeIdentity,
    ProjectionRecipeIdentity,
    ProjectionShardClaim,
    ProjectionShardSpec,
    ProviderDirectoryProjectionError,
    ProviderDirectoryProjectionLeaseLost,
    stable_hash,
    stable_json,
)


def _block_fields(
    block: ProjectionInputBlock,
    block_ordinal: int | None = None,
) -> dict[str, Any]:
    """Return only source-neutral physical input-block identity fields."""

    return {
        "block_id": block.block_id,
        **({"block_ordinal": block_ordinal} if block_ordinal is not None else {}),
        "upstream_artifact_id": block.upstream_artifact_id,
        "source_object_id": block.source_object_id,
        "block_kind": block.block_kind,
        "input_contract_id": block.input_contract_id,
        "record_start": block.record_start,
        "record_count": block.record_count,
        "content_sha256": block.content_sha256,
        "payload_sha256": block.payload_sha256,
        "payload_bytes": block.payload_bytes,
        "summary_json": dict(block.summary),
        "block_proof_sha256": block.block_proof_sha256,
    }


def _shard_fields(shard: ProjectionShardSpec) -> dict[str, Any]:
    return {
        "partition_id": shard.partition_id,
        "partition_ordinal": shard.partition_ordinal,
        "partition_key": shard.partition_key,
        "input_block_id": shard.input_block_id,
        "resource_type": shard.resource_type,
        "input_sha256": shard.input_sha256,
    }


def _validated_block_fields(block: ProjectionInputBlock) -> dict[str, Any]:
    """Rebuild one block so forged dataclass values cannot cross registration."""

    if type(block) is not ProjectionInputBlock or not isinstance(block.summary, Mapping):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_workset_invalid"
        )
    if any(
        type(summary_key) is not str or type(summary_count) is not int
        for summary_key, summary_count in block.summary.items()
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_workset_invalid"
        )
    rebuilt_block = projection_input_block(
        upstream_artifact_id=block.upstream_artifact_id,
        source_object_id=block.source_object_id,
        block_kind=block.block_kind,
        input_contract_id=block.input_contract_id,
        record_start=block.record_start,
        record_count=block.record_count,
        content_sha256=block.content_sha256,
        payload_sha256=block.payload_sha256,
        payload_bytes=block.payload_bytes,
        summary=block.summary,
    )
    if block != rebuilt_block:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_workset_invalid"
        )
    block_fields = _block_fields(block)
    if block_fields != _block_fields(rebuilt_block):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_workset_invalid"
        )
    return block_fields


def _validated_shard_fields(
    recipe: ProjectionRecipeIdentity | PhysicalProjectionRecipeIdentity,
    shard: ProjectionShardSpec,
    block: ProjectionInputBlock,
) -> dict[str, Any]:
    """Rebuild one shard and bind its input hash to the exact retained block."""

    if (
        type(shard) is not ProjectionShardSpec
        or shard.input_sha256 != block.content_sha256
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_workset_invalid"
        )
    rebuilt_shard = projection_shard_spec(
        recipe=recipe,
        partition_ordinal=shard.partition_ordinal,
        input_block=block,
    )
    shard_fields = _shard_fields(shard)
    if shard_fields != _shard_fields(rebuilt_shard):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_workset_invalid"
        )
    return shard_fields


def _validate_workset_identity_census(
    blocks: Sequence[ProjectionInputBlock],
    block_fields: Sequence[Mapping[str, Any]],
    shard_fields: Sequence[Mapping[str, Any]],
) -> None:
    """Require unique physical blocks, shards, ordinals, and coordinates."""

    block_ids = {block["block_id"] for block in block_fields}
    block_ordinals = {block["block_ordinal"] for block in block_fields}
    shard_ids = {shard["partition_id"] for shard in shard_fields}
    shard_block_ids = {shard["input_block_id"] for shard in shard_fields}
    shard_coordinates = {
        (shard["resource_type"], shard["partition_ordinal"])
        for shard in shard_fields
    }
    invalid_census = (
        len(block_ids) != len(block_fields)
        or len(block_ordinals) != len(block_fields)
        or len(shard_ids) != len(shard_fields)
        or len(shard_coordinates) != len(shard_fields)
        or len(block_ids) != len(blocks)
        or shard_block_ids != block_ids
        or len(shard_fields) != len(block_fields)
        or any(shard["input_block_id"] not in block_ids for shard in shard_fields)
    )
    if invalid_census:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_workset_invalid"
        )


def _validate_workset_resource_coverage(
    recipe: PhysicalProjectionRecipeIdentity,
    shard_fields: Sequence[Mapping[str, Any]],
) -> None:
    """Require the shard set to cover the recipe's declared resources."""

    if not shard_fields or any(
        shard["resource_type"] != PROJECTION_MIXED_RESOURCE_TYPE
        for shard in shard_fields
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_workset_resource_mismatch"
        )


def _normalized_workset(
    recipe: ProjectionRecipeIdentity | PhysicalProjectionRecipeIdentity,
    blocks: Sequence[ProjectionInputBlock],
    shards: Sequence[ProjectionShardSpec],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str, str]:
    """Validate and hash one deterministic block and shard census."""

    recipe = validated_physical_projection_recipe_identity(recipe)
    if any(type(block) is not ProjectionInputBlock for block in blocks) or any(
        type(shard) is not ProjectionShardSpec for shard in shards
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_workset_invalid"
        )
    sorted_blocks = sorted(blocks, key=lambda block: block.block_id)
    block_by_id = {block.block_id: block for block in sorted_blocks}
    block_ordinal_by_id = {
        block.block_id: block_ordinal
        for block_ordinal, block in enumerate(sorted_blocks)
    }
    block_fields = [
        {**_validated_block_fields(block), "block_ordinal": block_ordinal}
        for block_ordinal, block in enumerate(sorted_blocks)
    ]
    shard_fields = []
    for shard in shards:
        input_block = block_by_id.get(shard.input_block_id)
        if input_block is None or shard.partition_ordinal != block_ordinal_by_id.get(
            shard.input_block_id
        ):
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_workset_invalid"
            )
        shard_fields.append(_validated_shard_fields(recipe, shard, input_block))
    shard_fields.sort(
        key=lambda shard: (shard["partition_ordinal"], shard["partition_id"])
    )
    _validate_workset_identity_census(blocks, block_fields, shard_fields)
    _validate_workset_resource_coverage(recipe, shard_fields)
    return (
        block_fields,
        shard_fields,
        projection_input_set_sha256(
            blocks,
            decoder_contract_id=recipe.decoder_contract_id,
        ),
        stable_hash(
            shard_fields,
            domain="provider-directory-projection-shard-set-v1",
        ),
    )


async def _stored_workset(
    database: Any,
    schema: str,
    lease: ProjectionLease,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    block_rows = await database.all(
        f"""
        SELECT block_id, block_ordinal, upstream_artifact_id, source_object_id,
               block_kind, input_contract_id, record_start, record_count,
               content_sha256, payload_sha256, payload_bytes, summary_json,
               block_proof_sha256
          FROM {table_ref(schema, 'provider_directory_projection_input_block')}
         WHERE recipe_id = :recipe_id ORDER BY block_ordinal, block_id;
        """,
        recipe_id=lease.recipe.recipe_id,
    )
    shard_rows = await database.all(
        f"""
        SELECT partition_id, partition_ordinal, partition_key, input_block_id,
               resource_type, input_sha256
          FROM {table_ref(schema, 'provider_directory_projection_proof_shard')}
         WHERE recipe_id = :recipe_id AND attempt = :attempt
         ORDER BY partition_ordinal, partition_id;
        """,
        recipe_id=lease.recipe.recipe_id,
        attempt=lease.attempt,
    )
    stored_blocks = []
    for block_row in block_rows:
        block_fields = row_mapping(block_row)
        block_fields["summary_json"] = json_value(block_fields["summary_json"])
        stored_blocks.append(block_fields)
    return stored_blocks, [row_mapping(shard_row) for shard_row in shard_rows]


async def _insert_workset(
    database: Any,
    schema: str,
    lease: ProjectionLease,
    block_fields: Sequence[Mapping[str, Any]],
    shard_fields: Sequence[Mapping[str, Any]],
) -> None:
    """Insert a validated workset while its recipe transaction is locked."""

    await database.status(
        f"""
        INSERT INTO {table_ref(schema, 'provider_directory_projection_input_block')} (
            recipe_id, block_id, block_ordinal, upstream_artifact_id,
            source_object_id, block_kind, input_contract_id,
            record_start, record_count, content_sha256, payload_sha256,
            payload_bytes, summary_json, block_proof_sha256, created_at
        )
        SELECT :recipe_id, block.block_id, block.block_ordinal,
               block.upstream_artifact_id, block.source_object_id,
               block.block_kind, block.input_contract_id,
               block.record_start, block.record_count, block.content_sha256,
               block.payload_sha256, block.payload_bytes, block.summary_json,
               block.block_proof_sha256, now()
          FROM jsonb_to_recordset(CAST(:blocks_json AS jsonb)) AS block(
               block_id varchar(64), block_ordinal integer,
               upstream_artifact_id varchar(64), source_object_id varchar(64),
               block_kind varchar(64), input_contract_id varchar(128),
               record_start bigint, record_count bigint,
               content_sha256 varchar(64), payload_sha256 varchar(64),
               payload_bytes bigint, summary_json jsonb,
               block_proof_sha256 varchar(64));
        """,
        recipe_id=lease.recipe.recipe_id,
        blocks_json=stable_json(block_fields),
    )
    await database.status(
        f"""
        INSERT INTO {table_ref(schema, 'provider_directory_projection_proof_shard')} (
            recipe_id, attempt, partition_id, partition_ordinal,
            partition_key, input_block_id, resource_type, input_sha256,
            status, partition_attempt, created_at
        )
        SELECT :recipe_id, :attempt, shard.partition_id,
               shard.partition_ordinal, shard.partition_key,
               shard.input_block_id, shard.resource_type, shard.input_sha256,
               'pending', 0, now()
          FROM jsonb_to_recordset(CAST(:shards_json AS jsonb)) AS shard(
               partition_id varchar(64), partition_ordinal integer,
               partition_key varchar(64), input_block_id varchar(64),
               resource_type varchar(64), input_sha256 varchar(64));
        """,
        recipe_id=lease.recipe.recipe_id,
        attempt=lease.attempt,
        shards_json=stable_json(shard_fields),
    )


async def _is_registered_workset_replay(
    database: Any,
    schema: str,
    lease: ProjectionLease,
    recipe_fields: Mapping[str, Any],
    block_fields: Sequence[Mapping[str, Any]],
    shard_fields: Sequence[Mapping[str, Any]],
) -> bool:
    """Return true only for an exact replay of an existing workset."""

    if recipe_fields.get("workset_registered_at") is None:
        return False
    stored_blocks, stored_shards = await _stored_workset(database, schema, lease)
    if stored_blocks != block_fields or stored_shards != shard_fields:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_workset_replay_mismatch"
        )
    return True


async def _seal_registered_workset(
    database: Any,
    schema: str,
    lease: ProjectionLease,
    block_fields: Sequence[Mapping[str, Any]],
    shard_fields: Sequence[Mapping[str, Any]],
    block_set_hash: str,
    shard_set_hash: str,
) -> None:
    """Persist one new immutable workset under the active recipe lease."""

    await set_local_projection_action(
        database,
        "workset_register",
        recipe_id=lease.recipe.recipe_id,
        recipe_attempt=lease.attempt,
        recipe_lease_token=lease.lease_token,
    )
    await _insert_workset(database, schema, lease, block_fields, shard_fields)
    updated_count = await database.status(
        f"""
        UPDATE {table_ref(schema, 'provider_directory_projection_recipe')}
           SET input_block_set_sha256 = :block_set_hash,
               input_block_count = :block_count,
               partition_set_sha256 = :shard_set_hash,
               partition_count = :shard_count,
               workset_registered_at = now(), updated_at = now()
         WHERE recipe_id = :recipe_id AND attempt = :attempt
           AND lease_token = :lease_token
           AND workset_registered_at IS NULL;
        """,
        block_set_hash=block_set_hash,
        block_count=len(block_fields),
        shard_set_hash=shard_set_hash,
        shard_count=len(shard_fields),
        recipe_id=lease.recipe.recipe_id,
        attempt=lease.attempt,
        lease_token=lease.lease_token,
    )
    if updated_count != 1:
        raise ProviderDirectoryProjectionLeaseLost(
            "provider_directory_projection_lease_lost"
        )


async def register_projection_workset(
    lease: ProjectionLease,
    blocks: Sequence[ProjectionInputBlock],
    shards: Sequence[ProjectionShardSpec],
    *,
    database: Any = db,
    schema: str = "mrf",
) -> None:
    """Atomically register or exactly replay one immutable retained workset."""

    block_fields, shard_fields, block_set_hash, shard_set_hash = _normalized_workset(
        lease.recipe,
        blocks,
        shards,
    )
    if block_set_hash != lease.recipe.input_set_sha256:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_input_manifest_hash_mismatch"
        )
    async with database.transaction():
        recipe_fields = await locked_active_recipe(
            lease,
            database=database,
            schema=schema,
        )
        if await _is_registered_workset_replay(
            database,
            schema,
            lease,
            recipe_fields,
            block_fields,
            shard_fields,
        ):
            return
        await _seal_registered_workset(
            database,
            schema,
            lease,
            block_fields,
            shard_fields,
            block_set_hash,
            shard_set_hash,
        )


def _shard_claim(
    lease: ProjectionLease,
    admission_id: str,
    shard_fields: Mapping[str, Any],
) -> ProjectionShardClaim:
    shard = ProjectionShardSpec(
        partition_id=str(shard_fields["partition_id"]),
        partition_ordinal=int(shard_fields["partition_ordinal"]),
        partition_key=str(shard_fields["partition_key"]),
        input_block_id=str(shard_fields["input_block_id"]),
        resource_type=str(shard_fields["resource_type"]),
        input_sha256=str(shard_fields["input_sha256"]),
    )
    return ProjectionShardClaim(
        recipe_lease=lease,
        admission_id=admission_id,
        shard=shard,
        partition_attempt=int(shard_fields["partition_attempt"]),
        lease_token=str(shard_fields["lease_token"]),
    )


async def _assert_sealed_admission(
    database: Any,
    schema: str,
    lease: ProjectionLease,
    admission_id: str,
) -> None:
    admission_row = await database.first(
        f"""
        SELECT admission_id
          FROM {table_ref(schema, 'provider_directory_projection_admission')}
         WHERE admission_id = :admission_id
           AND recipe_id = :recipe_id
           AND status = 'sealed'
         FOR SHARE;
        """,
        admission_id=admission_id,
        recipe_id=lease.recipe.recipe_id,
    )
    if admission_row is None:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_admission_not_sealed"
        )


async def _claimable_shard_row(
    database: Any,
    shard_table: str,
    lease: ProjectionLease,
    partition_id: str | None,
) -> dict[str, Any]:
    candidate_row = await database.first(
        f"""
        SELECT partition_id, partition_attempt
          FROM {shard_table}
         WHERE recipe_id = :recipe_id AND attempt = :attempt
           AND (CAST(:partition_id AS varchar(64)) IS NULL
                OR partition_id = CAST(:partition_id AS varchar(64)))
           AND (status = 'pending' OR (
               status = 'building' AND (
                   lease_expires_at <= now()
                   OR recipe_lease_token <> :recipe_lease_token
               )
           ))
         ORDER BY partition_ordinal, partition_id
         FOR UPDATE SKIP LOCKED LIMIT 1;
        """,
        recipe_id=lease.recipe.recipe_id,
        attempt=lease.attempt,
        partition_id=partition_id,
        recipe_lease_token=lease.lease_token,
    )
    return row_mapping(candidate_row)


async def _claim_shard_row(
    database: Any,
    shard_table: str,
    lease: ProjectionLease,
    candidate_row: Mapping[str, Any],
    token: str,
    lease_seconds: int,
) -> Any:
    await set_local_projection_action(
        database,
        "shard_claim",
        recipe_id=lease.recipe.recipe_id,
        recipe_attempt=lease.attempt,
        recipe_lease_token=lease.lease_token,
        partition_id=str(candidate_row["partition_id"]),
        partition_attempt=int(candidate_row["partition_attempt"]),
        shard_lease_token=token,
    )
    return await database.first(
        f"""
        UPDATE {shard_table} AS shard
           SET status = 'building',
               partition_attempt = partition_attempt + 1,
               recipe_lease_token = :recipe_lease_token,
               lease_token = :lease_token,
               lease_expires_at = now() + make_interval(secs => :lease_seconds),
               lease_heartbeat_at = now()
         WHERE shard.recipe_id = :recipe_id
           AND shard.attempt = :attempt
           AND shard.partition_id = :claimed_partition_id
           AND shard.partition_attempt = :partition_attempt
           AND (shard.status = 'pending' OR (
               shard.status = 'building' AND (
                   shard.lease_expires_at <= now()
                   OR shard.recipe_lease_token <> :recipe_lease_token
               )
           ))
        RETURNING shard.*;
        """,
        recipe_id=lease.recipe.recipe_id,
        attempt=lease.attempt,
        claimed_partition_id=candidate_row["partition_id"],
        partition_attempt=candidate_row["partition_attempt"],
        recipe_lease_token=lease.lease_token,
        lease_token=token,
        lease_seconds=lease_seconds,
    )


async def claim_projection_shard(
    lease: ProjectionLease,
    *,
    admission_id: str,
    lease_seconds: int = 300,
    partition_id: str | None = None,
    database: Any = db,
    schema: str = "mrf",
) -> ProjectionShardClaim | None:
    """Claim one pending, expired, or superseded shard with SKIP LOCKED."""

    if not 30 <= lease_seconds <= 3600:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_shard_lease_seconds_invalid"
        )
    if type(admission_id) is not str or HASH_PATTERN.fullmatch(admission_id) is None:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_admission_id_invalid"
        )
    if partition_id is not None and (
        type(partition_id) is not str
        or HASH_PATTERN.fullmatch(partition_id) is None
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_partition_id_invalid"
        )
    shard_table = table_ref(schema, "provider_directory_projection_proof_shard")
    token = secrets.token_hex(32)
    async with database.transaction():
        recipe_fields = await shared_active_recipe(
            lease,
            database=database,
            schema=schema,
        )
        if recipe_fields.get("workset_registered_at") is None:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_workset_missing"
            )
        await _assert_sealed_admission(
            database,
            schema,
            lease,
            admission_id,
        )
        candidate_row = await _claimable_shard_row(
            database,
            shard_table,
            lease,
            partition_id,
        )
        if not candidate_row:
            return None
        claimed_row = await _claim_shard_row(
            database,
            shard_table,
            lease,
            candidate_row,
            token,
            lease_seconds,
        )
    return (
        _shard_claim(lease, admission_id, row_mapping(claimed_row))
        if claimed_row
        else None
    )


async def heartbeat_projection_shard(
    claim: ProjectionShardClaim,
    *,
    lease_seconds: int = 300,
    database: Any = db,
    schema: str = "mrf",
) -> None:
    """Extend only the exact shard generation held by this worker."""

    validated_physical_projection_recipe_identity(claim.recipe_lease.recipe)
    if not 30 <= lease_seconds <= 3600:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_shard_lease_seconds_invalid"
        )
    async with database.transaction():
        await set_local_projection_action(
            database,
            "shard_heartbeat",
            recipe_id=claim.recipe_lease.recipe.recipe_id,
            recipe_attempt=claim.recipe_lease.attempt,
            recipe_lease_token=claim.recipe_lease.lease_token,
            partition_id=claim.shard.partition_id,
            partition_attempt=claim.partition_attempt,
            shard_lease_token=claim.lease_token,
        )
        updated_count = await database.status(
            f"""
            UPDATE {table_ref(schema, 'provider_directory_projection_proof_shard')}
               SET lease_expires_at = now() + make_interval(secs => :lease_seconds),
                   lease_heartbeat_at = now()
             WHERE recipe_id = :recipe_id AND attempt = :attempt
               AND partition_id = :partition_id AND status = 'building'
               AND partition_attempt = :partition_attempt
               AND recipe_lease_token = :recipe_lease_token
               AND lease_token = :lease_token AND lease_expires_at > now();
            """,
            recipe_id=claim.recipe_lease.recipe.recipe_id,
            attempt=claim.recipe_lease.attempt,
            partition_id=claim.shard.partition_id,
            partition_attempt=claim.partition_attempt,
            recipe_lease_token=claim.recipe_lease.lease_token,
            lease_token=claim.lease_token,
            lease_seconds=lease_seconds,
        )
    if updated_count != 1:
        raise ProviderDirectoryProjectionLeaseLost(
            "provider_directory_projection_shard_lease_lost"
        )


__all__ = [
    "claim_projection_shard",
    "heartbeat_projection_shard",
    "register_projection_workset",
]
