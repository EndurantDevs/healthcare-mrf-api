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
    validated_projection_completeness_manifest,
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
    PROJECTION_MIXED_RESOURCE_TYPE,
    ProjectionCompletenessManifest,
    ProjectionInputBlock,
    ProjectionLease,
    ProjectionShardClaim,
    ProjectionShardSpec,
    ProviderDirectoryProjectionError,
    ProviderDirectoryProjectionLeaseLost,
    stable_hash,
    stable_json,
)


def _block_fields(block: ProjectionInputBlock) -> dict[str, Any]:
    return {
        "block_id": block.block_id,
        "block_ordinal": block.block_ordinal,
        "upstream_artifact_id": block.upstream_artifact_id,
        "source_object_id": block.source_object_id,
        "block_kind": block.block_kind,
        "input_contract_id": block.input_contract_id,
        "source_partition_ordinal": block.source_partition_ordinal,
        "record_start": block.record_start,
        "record_count": block.record_count,
        "content_sha256": block.content_sha256,
        "payload_sha256": block.payload_sha256,
        "payload_bytes": block.payload_bytes,
        "summary_json": dict(block.summary),
        "retained_campaign_id": block.retained_campaign_id,
        "retained_campaign_sha256": block.retained_campaign_sha256,
        "retained_source_item_id": block.retained_source_item_id,
        "retained_range_ordinal": block.retained_range_ordinal,
        "block_proof_sha256": block.block_proof_sha256,
    }


_RETAINED_BINDING_FIELDS = (
    "retained_campaign_id",
    "retained_campaign_sha256",
    "retained_source_item_id",
    "retained_range_ordinal",
)


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
        block_ordinal=block.block_ordinal,
        upstream_artifact_id=block.upstream_artifact_id,
        source_object_id=block.source_object_id,
        block_kind=block.block_kind,
        input_contract_id=block.input_contract_id,
        source_partition_ordinal=block.source_partition_ordinal,
        record_start=block.record_start,
        record_count=block.record_count,
        content_sha256=block.content_sha256,
        payload_sha256=block.payload_sha256,
        payload_bytes=block.payload_bytes,
        summary=block.summary,
        retained_campaign_id=block.retained_campaign_id,
        retained_campaign_sha256=block.retained_campaign_sha256,
        retained_source_item_id=block.retained_source_item_id,
        retained_range_ordinal=block.retained_range_ordinal,
    )
    block_fields = _block_fields(block)
    if block_fields != _block_fields(rebuilt_block):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_workset_invalid"
        )
    return block_fields


def _validated_shard_fields(
    lease: ProjectionLease,
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
        recipe=lease.recipe,
        partition_ordinal=shard.partition_ordinal,
        partition_key=shard.partition_key,
        input_block=block,
        resource_type=shard.resource_type,
        input_sha256=block.content_sha256,
    )
    shard_fields = _shard_fields(shard)
    if shard_fields != _shard_fields(rebuilt_shard):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_workset_invalid"
        )
    return shard_fields


def _validate_partition_census(
    completeness: Mapping[str, Any],
    block_by_id: Mapping[str, ProjectionInputBlock],
    shards: Sequence[ProjectionShardSpec],
) -> None:
    """Bind every terminal coordinate to its exact retained block aggregate."""

    terminal_by_coordinate = {
        (
            partition["resource_type"],
            partition["partition_key_hash"],
            partition["source_partition_ordinal"],
        ): partition
        for partition in completeness["terminal_partitions"]
    }
    blocks_by_coordinate: dict[tuple[str, str, int], list[ProjectionInputBlock]] = {}
    referenced_block_ids: set[str] = set()
    for shard in shards:
        input_block = block_by_id.get(shard.input_block_id)
        if input_block is None or shard.input_block_id in referenced_block_ids:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_workset_partition_census_mismatch"
            )
        referenced_block_ids.add(shard.input_block_id)
        coordinate = (
            shard.resource_type,
            shard.partition_key,
            input_block.source_partition_ordinal,
        )
        if coordinate not in terminal_by_coordinate:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_workset_partition_census_mismatch"
            )
        blocks_by_coordinate.setdefault(coordinate, []).append(input_block)
    if referenced_block_ids != set(block_by_id):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_workset_partition_census_mismatch"
        )
    for coordinate, terminal_partition in terminal_by_coordinate.items():
        coordinate_blocks = blocks_by_coordinate.get(coordinate, [])
        observed_count_by_field = {
            "block_count": len(coordinate_blocks),
            "row_count": sum(block.record_count for block in coordinate_blocks),
            "byte_count": sum(block.payload_bytes for block in coordinate_blocks),
        }
        expected_count_by_field = {
            field_name: terminal_partition[field_name]
            for field_name in ("block_count", "row_count", "byte_count")
        }
        if observed_count_by_field != expected_count_by_field:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_workset_partition_census_mismatch"
            )


def _normalized_workset(
    lease: ProjectionLease,
    blocks: Sequence[ProjectionInputBlock],
    shards: Sequence[ProjectionShardSpec],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str, str]:
    """Validate and hash one deterministic block and shard census."""

    completeness_manifest = validated_projection_completeness_manifest(
        ProjectionCompletenessManifest(
            lease.recipe.completeness_manifest_hash,
            lease.recipe.completeness_manifest,
        )
    )
    if any(type(block) is not ProjectionInputBlock for block in blocks) or any(
        type(shard) is not ProjectionShardSpec for shard in shards
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_workset_invalid"
        )
    block_by_id = {block.block_id: block for block in blocks}
    block_fields = sorted(
        (_validated_block_fields(block) for block in blocks),
        key=lambda block: (block["block_ordinal"], block["block_id"]),
    )
    shard_fields = []
    for shard in shards:
        input_block = block_by_id.get(shard.input_block_id)
        if input_block is None:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_workset_invalid"
            )
        shard_fields.append(_validated_shard_fields(lease, shard, input_block))
    shard_fields.sort(
        key=lambda shard: (shard["partition_ordinal"], shard["partition_id"])
    )
    block_ids = {block["block_id"] for block in block_fields}
    block_ordinals = {block["block_ordinal"] for block in block_fields}
    shard_ids = {shard["partition_id"] for shard in shard_fields}
    shard_coordinates = {
        (shard["resource_type"], shard["partition_ordinal"])
        for shard in shard_fields
    }
    if (
        len(block_ids) != len(block_fields)
        or len(block_ordinals) != len(block_fields)
        or len(shard_ids) != len(shard_fields)
        or len(shard_coordinates) != len(shard_fields)
        or len(block_by_id) != len(blocks)
        or any(shard["input_block_id"] not in block_ids for shard in shard_fields)
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_workset_invalid"
        )
    shard_resources = {shard["resource_type"] for shard in shard_fields}
    declared_resources = shard_resources - {PROJECTION_MIXED_RESOURCE_TYPE}
    if not declared_resources.issubset(lease.recipe.selected_resources) or (
        PROJECTION_MIXED_RESOURCE_TYPE not in shard_resources
        and not set(lease.recipe.required_resources).issubset(declared_resources)
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_workset_resource_mismatch"
        )
    _validate_partition_census(
        completeness_manifest.proof,
        block_by_id,
        shards,
    )
    return (
        block_fields,
        shard_fields,
        projection_input_set_sha256(
            blocks,
            decoder_contract_id=lease.recipe.decoder_contract_id,
            completeness_manifest=completeness_manifest,
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
               block_kind, input_contract_id, source_partition_ordinal,
               record_start, record_count,
               content_sha256, payload_sha256, payload_bytes, summary_json,
               retained_campaign_id, retained_campaign_sha256,
               retained_source_item_id, retained_range_ordinal,
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
            source_partition_ordinal,
            record_start, record_count, content_sha256, payload_sha256,
            payload_bytes, summary_json, retained_campaign_id,
            retained_campaign_sha256, retained_source_item_id,
            retained_range_ordinal,
            block_proof_sha256, created_at
        )
        SELECT :recipe_id, block.block_id, block.block_ordinal,
               block.upstream_artifact_id, block.source_object_id,
               block.block_kind, block.input_contract_id,
               block.source_partition_ordinal,
               block.record_start, block.record_count, block.content_sha256,
               block.payload_sha256, block.payload_bytes, block.summary_json,
               block.retained_campaign_id, block.retained_campaign_sha256,
               block.retained_source_item_id, block.retained_range_ordinal,
               block.block_proof_sha256, now()
          FROM jsonb_to_recordset(CAST(:blocks_json AS jsonb)) AS block(
               block_id varchar(64), block_ordinal integer,
               upstream_artifact_id varchar(64), source_object_id varchar(64),
               block_kind varchar(64), input_contract_id varchar(128),
               source_partition_ordinal integer,
               record_start bigint, record_count bigint,
               content_sha256 varchar(64), payload_sha256 varchar(64),
               payload_bytes bigint, summary_json jsonb,
               retained_campaign_id varchar(64),
               retained_campaign_sha256 varchar(64),
               retained_source_item_id varchar(64),
               retained_range_ordinal integer,
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


def _block_identity_without_retained_binding(
    block_fields: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        key: value
        for key, value in block_fields.items()
        if key not in _RETAINED_BINDING_FIELDS
    }


async def _refresh_workset_retained_bindings(
    database: Any,
    schema: str,
    lease: ProjectionLease,
    requested_blocks: Sequence[Mapping[str, Any]],
) -> None:
    requested_by_id = {block["block_id"]: block for block in requested_blocks}
    for block in requested_by_id.values():
        updated_count = await database.status(
            f"""
            UPDATE {table_ref(schema, 'provider_directory_projection_input_block')}
               SET retained_campaign_id = :retained_campaign_id,
                   retained_campaign_sha256 = :retained_campaign_sha256,
                   retained_source_item_id = :retained_source_item_id,
                   retained_range_ordinal = :retained_range_ordinal
             WHERE recipe_id = :recipe_id AND block_id = :block_id;
            """,
            retained_campaign_id=block["retained_campaign_id"],
            retained_campaign_sha256=block["retained_campaign_sha256"],
            retained_source_item_id=block["retained_source_item_id"],
            retained_range_ordinal=block["retained_range_ordinal"],
            recipe_id=lease.recipe.recipe_id,
            block_id=block["block_id"],
        )
        if updated_count != 1:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_retained_binding_refresh_failed"
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
        lease,
        blocks,
        shards,
    )
    if block_set_hash != lease.recipe.input_set_sha256:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_input_manifest_hash_mismatch"
        )
    recipe_table = table_ref(schema, "provider_directory_projection_recipe")
    async with database.transaction():
        recipe_fields = await locked_active_recipe(
            lease,
            database=database,
            schema=schema,
        )
        if recipe_fields.get("workset_registered_at") is not None:
            stored_blocks, stored_shards = await _stored_workset(
                database, schema, lease
            )
            if [
                _block_identity_without_retained_binding(block)
                for block in stored_blocks
            ] != [
                _block_identity_without_retained_binding(block)
                for block in block_fields
            ] or stored_shards != shard_fields:
                raise ProviderDirectoryProjectionError(
                    "provider_directory_projection_workset_replay_mismatch"
                )
            await set_local_projection_action(
                database,
                "retained_rebind",
                recipe_id=lease.recipe.recipe_id,
                recipe_attempt=lease.attempt,
                recipe_lease_token=lease.lease_token,
            )
            await _refresh_workset_retained_bindings(
                database,
                schema,
                lease,
                block_fields,
            )
            return
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
            UPDATE {recipe_table}
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


def _shard_claim(
    lease: ProjectionLease,
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
        shard=shard,
        partition_attempt=int(shard_fields["partition_attempt"]),
        lease_token=str(shard_fields["lease_token"]),
    )


async def claim_projection_shard(
    lease: ProjectionLease,
    *,
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
        candidate_row = row_mapping(
            await database.first(
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
        )
        if not candidate_row:
            return None
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
        claimed_row = await database.first(
            f"""
            UPDATE {shard_table} AS shard
               SET status = 'building',
                   partition_attempt = partition_attempt + 1,
                   recipe_lease_token = :recipe_lease_token,
                   lease_token = :lease_token,
                   lease_expires_at = now() +
                       make_interval(secs => :lease_seconds),
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
    return _shard_claim(lease, row_mapping(claimed_row)) if claimed_row else None


async def heartbeat_projection_shard(
    claim: ProjectionShardClaim,
    *,
    lease_seconds: int = 300,
    database: Any = db,
    schema: str = "mrf",
) -> None:
    """Extend only the exact shard generation held by this worker."""

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


async def _assert_active_retained_binding(
    database: Any,
    schema: str,
    recipe_id: str,
    block_fields: Mapping[str, Any],
) -> None:
    """Hold the exact live retained claim while a worker opens its bytes."""

    range_ordinal = block_fields.get("retained_range_ordinal")
    range_join = ""
    range_predicate = """
       AND :record_start = 0
       AND :record_count = layout.artifact_record_count
       AND :payload_sha256 = artifact.artifact_sha256
       AND :payload_bytes = artifact.artifact_byte_count
       AND :content_sha256 = layout.manifest_sha256
    """
    locked_relations = "campaign, consumer, reference, campaign_item, artifact, layout"
    if range_ordinal is not None:
        range_join = f"""
          JOIN {table_ref(schema, 'provider_directory_retained_artifact_range')}
               AS retained_range
            ON retained_range.layout_sha256 = layout.layout_sha256
           AND retained_range.range_ordinal = :retained_range_ordinal
        """
        range_predicate = """
       AND retained_range.artifact_sha256 = :upstream_artifact_id
       AND retained_range.record_start = :record_start
       AND retained_range.record_count = :record_count
       AND retained_range.raw_sha256 = :payload_sha256
       AND retained_range.raw_byte_count = :payload_bytes
       AND retained_range.canonical_sha256 = :content_sha256
        """
        locked_relations += ", retained_range"
    retained_row = await database.first(
        f"""
        SELECT 1
          FROM {table_ref(schema, 'provider_directory_retained_artifact_campaign')}
               AS campaign
          JOIN {table_ref(schema, 'provider_directory_retained_artifact_consumer')}
               AS consumer
            ON consumer.campaign_id = campaign.campaign_id
           AND consumer.consumer_recipe_id = :recipe_id
          JOIN {table_ref(schema, 'provider_directory_retained_artifact_consumer_reference')}
               AS reference
            ON reference.campaign_id = consumer.campaign_id
           AND reference.consumer_recipe_id = consumer.consumer_recipe_id
          JOIN {table_ref(schema, 'provider_directory_retained_artifact_campaign_item')}
               AS campaign_item
            ON campaign_item.campaign_id = reference.campaign_id
           AND campaign_item.source_item_id = reference.source_item_id
          JOIN {table_ref(schema, 'provider_directory_retained_artifact')}
               AS artifact
            ON artifact.artifact_sha256 = reference.artifact_sha256
          JOIN {table_ref(schema, 'provider_directory_retained_artifact_layout')}
               AS layout
            ON layout.layout_sha256 = reference.layout_sha256
           AND layout.artifact_sha256 = reference.artifact_sha256
          {range_join}
         WHERE campaign.campaign_id = :retained_campaign_id
           AND campaign.state = 'complete' AND campaign.complete
           AND campaign.campaign_sha256 = :retained_campaign_sha256
           AND campaign.released_at IS NULL
           AND consumer.claimed_campaign_sha256 = campaign.campaign_sha256
           AND consumer.released_at IS NULL
           AND reference.claim_generation = consumer.claim_generation
           AND reference.released_at IS NULL
           AND reference.source_item_id = :retained_source_item_id
           AND reference.artifact_sha256 = :upstream_artifact_id
           AND reference.layout_sha256 = :source_object_id
           AND campaign_item.status = 'admitted'
           AND campaign_item.artifact_sha256 = reference.artifact_sha256
           AND campaign_item.layout_sha256 = reference.layout_sha256
           AND artifact.registry_status = 'verified'
           AND artifact.released_at IS NULL
           AND layout.registry_status = 'verified'
           AND layout.released_at IS NULL
          {range_predicate}
         FOR SHARE OF {locked_relations};
        """,
        recipe_id=recipe_id,
        **{
            field_name: block_fields.get(field_name)
            for field_name in (
                "content_sha256",
                "payload_bytes",
                "payload_sha256",
                "record_count",
                "record_start",
                "retained_campaign_id",
                "retained_campaign_sha256",
                "retained_range_ordinal",
                "retained_source_item_id",
                "source_object_id",
                "upstream_artifact_id",
            )
        },
    )
    if retained_row is None:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_retained_binding_invalid"
        )


async def load_projection_input_block(
    claim: ProjectionShardClaim,
    *,
    database: Any = db,
    schema: str = "mrf",
) -> dict[str, Any]:
    """Load the bounded immutable descriptor for one claimed shard."""

    async with database.transaction():
        await shared_active_recipe(
            claim.recipe_lease,
            database=database,
            schema=schema,
        )
        block_fields = row_mapping(
            await database.first(
                f"""
                SELECT block.*
                  FROM {table_ref(schema, 'provider_directory_projection_proof_shard')}
                       AS shard
                  JOIN {table_ref(schema, 'provider_directory_projection_input_block')}
                       AS block
                    ON block.recipe_id = shard.recipe_id
                   AND block.block_id = shard.input_block_id
                 WHERE shard.recipe_id = :recipe_id
                   AND shard.attempt = :attempt
                   AND shard.partition_id = :partition_id
                   AND shard.partition_attempt = :partition_attempt
                   AND shard.status = 'building'
                   AND shard.recipe_lease_token = :recipe_lease_token
                   AND shard.lease_token = :shard_lease_token
                   AND shard.lease_expires_at > clock_timestamp()
                   AND block.block_id = :block_id
                 FOR SHARE OF shard, block;
                """,
                recipe_id=claim.recipe_lease.recipe.recipe_id,
                attempt=claim.recipe_lease.attempt,
                partition_id=claim.shard.partition_id,
                partition_attempt=claim.partition_attempt,
                recipe_lease_token=claim.recipe_lease.lease_token,
                shard_lease_token=claim.lease_token,
                block_id=claim.shard.input_block_id,
            )
        )
        if block_fields:
            await _assert_active_retained_binding(
                database,
                schema,
                claim.recipe_lease.recipe.recipe_id,
                block_fields,
            )
    if not block_fields:
        raise ProviderDirectoryProjectionLeaseLost(
            "provider_directory_projection_shard_lease_lost"
        )
    block_fields["summary_json"] = json_value(block_fields["summary_json"])
    return block_fields


__all__ = [
    "claim_projection_shard",
    "heartbeat_projection_shard",
    "load_projection_input_block",
    "register_projection_workset",
]
