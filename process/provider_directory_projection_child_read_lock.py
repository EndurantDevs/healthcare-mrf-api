# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Deterministic dependency locks for projection child retained reads."""

from __future__ import annotations

from typing import Any

from process.provider_directory_projection_child_read_contract import (
    validated_child_shard_claim,
)
from process.provider_directory_projection_db import (
    recipe_database_identity,
    row_mapping,
    table_ref,
)
from process.provider_directory_projection_types import (
    ProjectionShardClaim,
    ProviderDirectoryProjectionLeaseLost,
)


def _lease_lost() -> ProviderDirectoryProjectionLeaseLost:
    return ProviderDirectoryProjectionLeaseLost(
        "provider_directory_projection_child_dependency_lost"
    )


async def _lock_recipe(
    database: Any,
    schema: str,
    claim: ProjectionShardClaim,
    allow_completed: bool,
) -> None:
    recipe = row_mapping(
        await database.first(
            f"""
            SELECT *
              FROM {table_ref(schema, 'provider_directory_projection_recipe')}
             WHERE recipe_id = :recipe_id AND attempt = :attempt
             FOR SHARE;
            """,
            recipe_id=claim.recipe_lease.recipe.recipe_id,
            attempt=claim.recipe_lease.attempt,
        )
    )
    active_lease = bool(
        recipe
        and recipe.get("status") in {"building", "proof_ready"}
        and recipe_database_identity(recipe)
        == claim.recipe_lease.recipe.identity_payload
        and recipe.get("lease_token") == claim.recipe_lease.lease_token
        and await database.scalar(
            "SELECT :lease_expires_at > now();",
            lease_expires_at=recipe.get("lease_expires_at"),
        )
    )
    sealed_recipe = bool(
        allow_completed
        and recipe.get("status") == "sealed"
        and recipe_database_identity(recipe)
        == claim.recipe_lease.recipe.identity_payload
    )
    if not active_lease and not sealed_recipe:
        raise _lease_lost()


async def _lock_admission(
    database: Any,
    schema: str,
    claim: ProjectionShardClaim,
) -> dict[str, Any]:
    admission = row_mapping(
        await database.first(
            f"""
            SELECT admission_id, recipe_id, retained_campaign_id,
                   retained_campaign_sha256, retained_consumer_recipe_id,
                   claim_generation, status
              FROM {table_ref(schema, 'provider_directory_projection_admission')}
             WHERE admission_id = :admission_id AND recipe_id = :recipe_id
             FOR SHARE;
            """,
            admission_id=claim.admission_id,
            recipe_id=claim.recipe_lease.recipe.recipe_id,
        )
    )
    if not admission or admission.get("status") != "sealed":
        raise _lease_lost()
    return admission


async def _lock_shard(
    database: Any,
    schema: str,
    claim: ProjectionShardClaim,
    allow_completed: bool,
) -> dict[str, Any]:
    shard = row_mapping(
        await database.first(
            f"""
            SELECT *
              FROM {table_ref(schema, 'provider_directory_projection_proof_shard')}
             WHERE recipe_id = :recipe_id AND attempt = :attempt
               AND partition_id = :partition_id
             FOR UPDATE;
            """,
            recipe_id=claim.recipe_lease.recipe.recipe_id,
            attempt=claim.recipe_lease.attempt,
            partition_id=claim.shard.partition_id,
        )
    )
    exact_identity = bool(
        shard
        and shard.get("partition_attempt") == claim.partition_attempt
        and shard.get("partition_ordinal") == claim.shard.partition_ordinal
        and shard.get("partition_key") == claim.shard.partition_key
        and shard.get("input_block_id") == claim.shard.input_block_id
        and shard.get("resource_type") == claim.shard.resource_type
        and shard.get("input_sha256") == claim.shard.input_sha256
    )
    active_shard = bool(
        exact_identity
        and shard.get("status") == "building"
        and shard.get("recipe_lease_token") == claim.recipe_lease.lease_token
        and shard.get("lease_token") == claim.lease_token
        and await database.scalar(
            "SELECT :lease_expires_at > now();",
            lease_expires_at=shard.get("lease_expires_at"),
        )
    )
    completed_shard = bool(
        allow_completed and exact_identity and shard.get("status") == "complete"
    )
    if not active_shard and not completed_shard:
        raise _lease_lost()
    return shard


async def _lock_binding(
    database: Any,
    schema: str,
    claim: ProjectionShardClaim,
) -> dict[str, Any]:
    binding = row_mapping(
        await database.first(
            f"""
            SELECT binding.*, block.record_count, block.content_sha256,
                   block.payload_sha256, block.payload_bytes
              FROM {table_ref(schema, 'provider_directory_projection_admission_input_block')}
                   AS binding
              JOIN {table_ref(schema, 'provider_directory_projection_input_block')}
                   AS block
                ON block.recipe_id = binding.recipe_id
               AND block.block_id = binding.block_id
             WHERE binding.admission_id = :admission_id
               AND binding.recipe_id = :recipe_id
               AND binding.block_id = :block_id
             ORDER BY binding.binding_id
             LIMIT 1 FOR SHARE OF binding, block;
            """,
            admission_id=claim.admission_id,
            recipe_id=claim.recipe_lease.recipe.recipe_id,
            block_id=claim.shard.input_block_id,
        )
    )
    if not binding or binding.get("content_sha256") != claim.shard.input_sha256:
        raise _lease_lost()
    return binding


async def _lock_retained_campaign(
    database: Any,
    schema: str,
    admission: dict[str, Any],
) -> dict[str, Any]:
    return row_mapping(
        await database.first(
            f"""
            SELECT campaign_id, campaign_sha256, state, complete, released_at
              FROM {table_ref(schema, 'provider_directory_retained_artifact_campaign')}
             WHERE campaign_id = :campaign_id FOR SHARE;
            """,
            campaign_id=admission["retained_campaign_id"],
        )
    )


async def _lock_retained_consumer(
    database: Any,
    schema: str,
    admission: dict[str, Any],
) -> dict[str, Any]:
    return row_mapping(
        await database.first(
            f"""
            SELECT claimed_campaign_sha256, claim_generation, released_at
              FROM {table_ref(schema, 'provider_directory_retained_artifact_consumer')}
             WHERE campaign_id = :campaign_id
               AND consumer_recipe_id = :consumer_recipe_id
             FOR SHARE;
            """,
            campaign_id=admission["retained_campaign_id"],
            consumer_recipe_id=admission["retained_consumer_recipe_id"],
        )
    )


async def _lock_retained_reference(
    database: Any,
    schema: str,
    admission: dict[str, Any],
    binding: dict[str, Any],
) -> dict[str, Any]:
    return row_mapping(
        await database.first(
            f"""
            SELECT artifact_sha256, layout_sha256, claim_generation, released_at
              FROM {table_ref(schema, 'provider_directory_retained_artifact_consumer_reference')}
             WHERE campaign_id = :campaign_id
               AND consumer_recipe_id = :consumer_recipe_id
               AND source_item_id = :source_item_id
             FOR SHARE;
            """,
            campaign_id=admission["retained_campaign_id"],
            consumer_recipe_id=admission["retained_consumer_recipe_id"],
            source_item_id=binding["retained_source_item_id"],
        )
    )


def _has_exact_parent_claim(
    campaign: dict[str, Any],
    consumer: dict[str, Any],
    reference: dict[str, Any],
    admission: dict[str, Any],
    binding: dict[str, Any],
) -> bool:
    campaign_sha256 = admission["retained_campaign_sha256"]
    claim_generation = admission["claim_generation"]
    return bool(
        campaign
        and campaign.get("state") == "complete"
        and campaign.get("complete") is True
        and campaign.get("campaign_sha256") == campaign_sha256
        and campaign.get("released_at") is None
        and consumer
        and consumer.get("claimed_campaign_sha256") == campaign_sha256
        and consumer.get("claim_generation") == claim_generation
        and consumer.get("released_at") is None
        and reference
        and reference.get("claim_generation") == claim_generation
        and reference.get("released_at") is None
        and reference.get("artifact_sha256")
        == binding["retained_artifact_sha256"]
        and reference.get("layout_sha256") == binding["retained_layout_sha256"]
    )


async def _lock_campaign_and_parent(
    database: Any,
    schema: str,
    admission: dict[str, Any],
    binding: dict[str, Any],
) -> None:
    """Lock and verify a retained campaign, consumer, and reference."""

    campaign = await _lock_retained_campaign(database, schema, admission)
    consumer = await _lock_retained_consumer(database, schema, admission)
    reference = await _lock_retained_reference(
        database,
        schema,
        admission,
        binding,
    )
    if not _has_exact_parent_claim(
        campaign,
        consumer,
        reference,
        admission,
        binding,
    ):
        raise _lease_lost()


async def _lock_artifact_layout(
    database: Any,
    schema: str,
    binding: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    artifact = row_mapping(
        await database.first(
            f"""
            SELECT artifact_byte_count, registry_status, released_at
              FROM {table_ref(schema, 'provider_directory_retained_artifact')}
             WHERE artifact_sha256 = :artifact_sha256 FOR SHARE;
            """,
            artifact_sha256=binding["retained_artifact_sha256"],
        )
    )
    layout = row_mapping(
        await database.first(
            f"""
            SELECT artifact_record_count, manifest_sha256,
                   registry_status, released_at
              FROM {table_ref(schema, 'provider_directory_retained_artifact_layout')}
             WHERE layout_sha256 = :layout_sha256
               AND artifact_sha256 = :artifact_sha256 FOR SHARE;
            """,
            layout_sha256=binding["retained_layout_sha256"],
            artifact_sha256=binding["retained_artifact_sha256"],
        )
    )
    layout_range_by_field: dict[str, Any] = {}
    if binding.get("retained_range_ordinal") is not None:
        layout_range_by_field = row_mapping(
            await database.first(
                f"""
                SELECT raw_byte_start, raw_byte_count, raw_sha256,
                       record_count, canonical_sha256
                  FROM {table_ref(schema, 'provider_directory_retained_artifact_range')}
                 WHERE layout_sha256 = :layout_sha256
                   AND range_ordinal = :range_ordinal FOR SHARE;
                """,
                layout_sha256=binding["retained_layout_sha256"],
                range_ordinal=binding["retained_range_ordinal"],
            )
        )
    if (
        not artifact
        or artifact.get("registry_status") != "verified"
        or artifact.get("released_at") is not None
        or not layout
        or layout.get("registry_status") != "verified"
        or layout.get("released_at") is not None
        or (
            binding.get("retained_range_ordinal") is not None
            and not layout_range_by_field
        )
    ):
        raise _lease_lost()
    return artifact, layout, layout_range_by_field


def _validate_block_mapping(
    descriptor_by_field: dict[str, Any],
    binding: dict[str, Any],
    layout: dict[str, Any],
    layout_range_by_field: dict[str, Any],
    is_full_artifact: bool,
) -> None:
    expected_block_fields = (
        descriptor_by_field["expected_byte_count"],
        descriptor_by_field["expected_record_count"],
        descriptor_by_field["expected_payload_sha256"],
    )
    actual_block_fields = (
        binding["payload_bytes"],
        binding["record_count"],
        binding["payload_sha256"],
    )
    expected_input_sha256 = (
        layout["manifest_sha256"]
        if is_full_artifact
        else layout_range_by_field["canonical_sha256"]
    )
    if (
        expected_block_fields != actual_block_fields
        or expected_input_sha256 != binding["content_sha256"]
    ):
        raise _lease_lost()


def _binding_descriptor(
    admission: dict[str, Any],
    binding: dict[str, Any],
    artifact: dict[str, Any],
    layout: dict[str, Any],
    layout_range_by_field: dict[str, Any],
) -> dict[str, Any]:
    is_full_artifact = binding.get("retained_range_ordinal") is None
    descriptor_by_field = {
        "binding_id": binding["binding_id"],
        "block_id": binding["block_id"],
        "retained_campaign_id": admission["retained_campaign_id"],
        "retained_campaign_sha256": admission["retained_campaign_sha256"],
        "retained_consumer_recipe_id": admission["retained_consumer_recipe_id"],
        "retained_claim_generation": admission["claim_generation"],
        "retained_source_item_id": binding["retained_source_item_id"],
        "retained_artifact_sha256": binding["retained_artifact_sha256"],
        "retained_layout_sha256": binding["retained_layout_sha256"],
        "retained_range_ordinal": binding.get("retained_range_ordinal"),
        "artifact_byte_count": artifact["artifact_byte_count"],
        "raw_byte_start": (
            0 if is_full_artifact else layout_range_by_field["raw_byte_start"]
        ),
        "expected_byte_count": (
            artifact["artifact_byte_count"]
            if is_full_artifact
            else layout_range_by_field["raw_byte_count"]
        ),
        "expected_record_count": (
            layout["artifact_record_count"]
            if is_full_artifact
            else layout_range_by_field["record_count"]
        ),
        "input_sha256": binding["content_sha256"],
        "expected_payload_sha256": (
            binding["retained_artifact_sha256"]
            if is_full_artifact
            else layout_range_by_field["raw_sha256"]
        ),
    }
    _validate_block_mapping(
        descriptor_by_field,
        binding,
        layout,
        layout_range_by_field,
        is_full_artifact,
    )
    return descriptor_by_field


async def locked_child_read_dependencies(
    database: Any,
    schema: str,
    claim: ProjectionShardClaim,
    *,
    allow_completed: bool = False,
) -> dict[str, Any]:
    """Lock recipe, admission, shard, binding, and parent in one order."""

    claim = validated_child_shard_claim(claim)
    await _lock_recipe(database, schema, claim, allow_completed)
    admission = await _lock_admission(database, schema, claim)
    await _lock_shard(database, schema, claim, allow_completed)
    binding = await _lock_binding(database, schema, claim)
    await _lock_campaign_and_parent(database, schema, admission, binding)
    artifact, layout, layout_range = await _lock_artifact_layout(
        database,
        schema,
        binding,
    )
    return _binding_descriptor(
        admission,
        binding,
        artifact,
        layout,
        layout_range,
    )


__all__ = ("locked_child_read_dependencies",)
