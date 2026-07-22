# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Low-level persistence operations for projection child read leases."""

from __future__ import annotations

from typing import Any, Mapping

from process.provider_directory_projection_child_read_contract import (
    child_lease_database_identity,
    validated_child_shard_claim,
)
from process.provider_directory_projection_db import row_mapping, table_ref
from process.provider_directory_projection_types import (
    ProjectionRetainedChildLease,
    ProjectionShardClaim,
    ProviderDirectoryProjectionError,
    required_hash,
)


CHILD_READ_ACTIONS = frozenset({"claim", "heartbeat", "release", "verify"})


def _validated_lease_seconds(lease_seconds: int) -> int:
    if type(lease_seconds) is not int or not 30 <= lease_seconds <= 3600:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_child_lease_seconds_invalid"
        )
    return lease_seconds


async def _set_child_action(
    database: Any,
    action: str,
    claim: ProjectionShardClaim,
    child_generation: int,
    child_lease_token: str,
) -> None:
    claim = validated_child_shard_claim(claim)
    if (
        action not in CHILD_READ_ACTIONS
        or type(child_generation) is not int
        or child_generation < 1
        or required_hash(child_lease_token, "child_lease_token")
        != child_lease_token
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_child_action_invalid"
        )
    setting_by_name = {
        "action": action,
        "recipe_id": claim.recipe_lease.recipe.recipe_id,
        "recipe_attempt": str(claim.recipe_lease.attempt),
        "recipe_lease_token": claim.recipe_lease.lease_token,
        "partition_id": claim.shard.partition_id,
        "partition_attempt": str(claim.partition_attempt),
        "shard_lease_token": claim.lease_token,
        "generation": str(child_generation),
        "lease_token": child_lease_token,
    }
    for setting_name, setting_value in setting_by_name.items():
        stored_value = await database.scalar(
            "SELECT set_config("
            f"'healthporta.provider_directory_projection_child_{setting_name}', "
            ":setting_value, true);",
            setting_value=setting_value,
        )
        if stored_value != setting_value:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_child_action_setting_failed"
            )


def _claim_values_by_column(
    claim: ProjectionShardClaim,
    descriptor_by_field: Mapping[str, Any],
    child_generation: int,
    child_lease_token: str,
    lease_seconds: int,
) -> dict[str, Any]:
    return {
        "recipe_id": claim.recipe_lease.recipe.recipe_id,
        "recipe_attempt": claim.recipe_lease.attempt,
        "admission_id": claim.admission_id,
        "partition_id": claim.shard.partition_id,
        "partition_attempt": claim.partition_attempt,
        "recipe_lease_token": claim.recipe_lease.lease_token,
        "shard_lease_token": claim.lease_token,
        **dict(descriptor_by_field),
        "child_generation": child_generation,
        "child_lease_token": child_lease_token,
        "lease_seconds": lease_seconds,
    }


async def _locked_child_row(
    database: Any,
    schema: str,
    claim: ProjectionShardClaim,
) -> dict[str, Any]:
    return row_mapping(
        await database.first(
            f"""
            SELECT *
              FROM {table_ref(schema, 'provider_directory_projection_child_read_lease')}
             WHERE recipe_id = :recipe_id
               AND recipe_attempt = :recipe_attempt
               AND partition_id = :partition_id
             FOR UPDATE;
            """,
            recipe_id=claim.recipe_lease.recipe.recipe_id,
            recipe_attempt=claim.recipe_lease.attempt,
            partition_id=claim.shard.partition_id,
        )
    )


async def _has_live_current_child(
    database: Any,
    child_row: Mapping[str, Any],
    claim: ProjectionShardClaim,
) -> bool:
    has_same_shard_generation = bool(
        child_row.get("partition_attempt") == claim.partition_attempt
        and child_row.get("recipe_lease_token") == claim.recipe_lease.lease_token
        and child_row.get("shard_lease_token") == claim.lease_token
    )
    return bool(
        has_same_shard_generation
        and child_row.get("status") in {"active", "verified"}
        and await database.scalar(
            "SELECT :lease_expires_at > now();",
            lease_expires_at=child_row.get("lease_expires_at"),
        )
    )


async def _insert_child(
    database: Any,
    table: str,
    claim_values_by_column: Mapping[str, Any],
) -> dict[str, Any]:
    columns = (
        "recipe_id",
        "recipe_attempt",
        "admission_id",
        "partition_id",
        "partition_attempt",
        "recipe_lease_token",
        "shard_lease_token",
        "binding_id",
        "block_id",
        "retained_campaign_id",
        "retained_campaign_sha256",
        "retained_consumer_recipe_id",
        "retained_claim_generation",
        "retained_source_item_id",
        "retained_artifact_sha256",
        "retained_layout_sha256",
        "retained_range_ordinal",
        "artifact_byte_count",
        "raw_byte_start",
        "expected_byte_count",
        "expected_record_count",
        "input_sha256",
        "expected_payload_sha256",
        "child_generation",
        "child_lease_token",
    )
    column_sql = ", ".join(columns)
    value_sql = ", ".join(f":{column}" for column in columns)
    inserted_child_row = await database.first(
        f"""
        INSERT INTO {table} (
            {column_sql}, status, lease_expires_at, lease_heartbeat_at,
            created_at, updated_at
        ) VALUES (
            {value_sql}, 'active',
            now() + make_interval(secs => :lease_seconds), now(), now(), now()
        ) RETURNING *;
        """,
        **dict(claim_values_by_column),
    )
    return row_mapping(inserted_child_row)


async def _reclaim_child(
    database: Any,
    table: str,
    claim_values_by_column: Mapping[str, Any],
    previous_generation: int,
) -> dict[str, Any]:
    mutable_columns = (
        "admission_id",
        "partition_attempt",
        "recipe_lease_token",
        "shard_lease_token",
        "binding_id",
        "block_id",
        "retained_campaign_id",
        "retained_campaign_sha256",
        "retained_consumer_recipe_id",
        "retained_claim_generation",
        "retained_source_item_id",
        "retained_artifact_sha256",
        "retained_layout_sha256",
        "retained_range_ordinal",
        "artifact_byte_count",
        "raw_byte_start",
        "expected_byte_count",
        "expected_record_count",
        "input_sha256",
        "expected_payload_sha256",
        "child_generation",
        "child_lease_token",
    )
    assignments = ",\n".join(
        f"{column} = :{column}" for column in mutable_columns
    )
    reclaimed_child_row = await database.first(
        f"""
        UPDATE {table}
           SET {assignments}, status = 'active',
               lease_expires_at = now() + make_interval(secs => :lease_seconds),
               lease_heartbeat_at = now(),
               verified_byte_count = NULL, verified_record_count = NULL,
               verified_input_sha256 = NULL,
               verified_payload_sha256 = NULL, verified_at = NULL,
               released_at = NULL, updated_at = now()
         WHERE recipe_id = :recipe_id
           AND recipe_attempt = :recipe_attempt
           AND partition_id = :partition_id
           AND child_generation = :previous_generation
        RETURNING *;
        """,
        **dict(claim_values_by_column),
        previous_generation=previous_generation,
    )
    return row_mapping(reclaimed_child_row)


async def _verify_child_row(
    database: Any,
    schema: str,
    lease: ProjectionRetainedChildLease,
    byte_count: int,
    record_count: int,
    input_sha256: str,
    payload_sha256: str,
) -> int:
    return await database.status(
        f"""
        UPDATE {table_ref(schema, 'provider_directory_projection_child_read_lease')}
           SET status = 'verified', verified_byte_count = :byte_count,
               verified_record_count = :record_count,
               verified_input_sha256 = :verified_input_sha256,
               verified_payload_sha256 = :payload_sha256,
               verified_at = now(), updated_at = now()
         WHERE recipe_id = :recipe_id
           AND recipe_attempt = :recipe_attempt
           AND partition_id = :partition_id
           AND child_generation = :child_generation
           AND child_lease_token = :child_lease_token
           AND status = 'active' AND lease_expires_at > now();
        """,
        **child_lease_database_identity(lease),
        byte_count=byte_count,
        record_count=record_count,
        verified_input_sha256=input_sha256,
        payload_sha256=payload_sha256,
    )


__all__ = (
    "_claim_values_by_column",
    "_has_live_current_child",
    "_insert_child",
    "_locked_child_row",
    "_reclaim_child",
    "_set_child_action",
    "_validated_lease_seconds",
    "_verify_child_row",
)
