# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Short-transaction recipe claim and reclaim operations."""

from __future__ import annotations

import secrets
from typing import Any

from db.connection import db
from process.provider_directory_projection_contract import (
    validated_physical_projection_recipe_identity,
)
from process.provider_directory_projection_db import (
    recipe_database_identity,
    row_mapping,
    set_local_projection_action,
    table_ref,
)
from process.provider_directory_projection_types import (
    ProjectionClaim,
    ProjectionLease,
    PhysicalProjectionRecipeIdentity,
    ProjectionRecipeIdentity,
    ProviderDirectoryProjectionBusy,
    ProviderDirectoryProjectionError,
    ProviderDirectoryProjectionLeaseLost,
    stable_json,
)


async def _insert_recipe_if_missing(
    database: Any,
    recipe_table: str,
    recipe: PhysicalProjectionRecipeIdentity,
    lease_token: str,
    lease_seconds: int,
) -> None:
    await database.status(
        f"""
        INSERT INTO {recipe_table} (
            recipe_id, decoder_contract_id, input_set_sha256,
            transform_contract_id, scope_contract_id, transform_context_hash,
            transform_context_json, resource_profile_hash,
            selected_resources_json, required_resources_json, status,
            attempt, lease_token, lease_expires_at, lease_heartbeat_at,
            created_at, updated_at
        ) VALUES (
            :recipe_id, :decoder_contract_id, :input_set_sha256,
            :transform_contract_id, :scope_contract_id, :transform_context_hash,
            CAST(:transform_context_json AS jsonb),
            :resource_profile_hash, CAST(:selected_resources_json AS jsonb),
            CAST(:required_resources_json AS jsonb), 'building', 1,
            :lease_token, now() + make_interval(secs => :lease_seconds),
            now(), now(), now()
        ) ON CONFLICT (recipe_id) DO NOTHING;
        """,
        recipe_id=recipe.recipe_id,
        decoder_contract_id=recipe.decoder_contract_id,
        input_set_sha256=recipe.input_set_sha256,
        transform_contract_id=recipe.transform_contract_id,
        scope_contract_id=recipe.scope_contract_id,
        transform_context_hash=recipe.transform_context_hash,
        transform_context_json=stable_json(recipe.transform_context),
        resource_profile_hash=recipe.resource_profile_hash,
        selected_resources_json=stable_json(recipe.selected_resources),
        required_resources_json=stable_json(recipe.required_resources),
        lease_token=lease_token,
        lease_seconds=lease_seconds,
    )


async def _locked_recipe(
    database: Any,
    recipe_table: str,
    recipe: PhysicalProjectionRecipeIdentity,
) -> dict[str, Any]:
    recipe_fields = row_mapping(
        await database.first(
            f"SELECT * FROM {recipe_table} " "WHERE recipe_id = :recipe_id FOR UPDATE;",
            recipe_id=recipe.recipe_id,
        )
    )
    if recipe_database_identity(recipe_fields) != recipe.identity_payload:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_recipe_identity_collision"
        )
    return recipe_fields


def _projection_lease(
    recipe: PhysicalProjectionRecipeIdentity,
    recipe_fields: dict[str, Any],
    lease_token: str,
) -> ProjectionLease:
    stage_oid = recipe_fields.get("stage_relation_oid")
    return ProjectionLease(
        recipe=recipe,
        attempt=int(recipe_fields.get("attempt") or 0),
        lease_token=lease_token,
        stage_schema=recipe_fields.get("stage_schema"),
        stage_relation=recipe_fields.get("stage_relation"),
        stage_relation_oid=int(stage_oid) if stage_oid is not None else None,
    )


async def _claim_locked_recipe(
    database: Any,
    recipe_table: str,
    recipe: PhysicalProjectionRecipeIdentity,
    recipe_fields: dict[str, Any],
    lease_token: str,
    lease_seconds: int,
) -> ProjectionClaim:
    """Fence one live build; reuse and failed-state recovery are separate."""

    if recipe_fields.get("status") == "sealed":
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_admission_resolution_required"
        )
    recipe_status = recipe_fields.get("status")
    if recipe_status not in {"building", "proof_ready", "failed"}:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_recipe_not_reclaimable"
        )
    if recipe_status == "failed":
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_failure_classification_required"
        )
    is_inserted_token = recipe_fields.get("lease_token") == lease_token
    is_lease_expired = bool(
        await database.scalar(
            "SELECT :lease_expires_at <= now();",
            lease_expires_at=recipe_fields.get("lease_expires_at"),
        )
    )
    if not is_inserted_token and not is_lease_expired:
        raise ProviderDirectoryProjectionBusy(
            "provider_directory_projection_recipe_busy"
        )
    if not is_inserted_token:
        await set_local_projection_action(
            database,
            "recipe_reclaim",
            recipe_id=recipe.recipe_id,
            recipe_attempt=int(recipe_fields.get("attempt") or 0),
            recipe_lease_token=lease_token,
        )
        await database.status(
            f"""
            UPDATE {recipe_table}
               SET status = 'building', lease_token = :lease_token,
                   lease_expires_at = now() + make_interval(secs => :lease_seconds),
                   lease_heartbeat_at = now(), updated_at = now()
             WHERE recipe_id = :recipe_id;
            """,
            recipe_id=recipe.recipe_id,
            lease_token=lease_token,
            lease_seconds=lease_seconds,
        )
    return ProjectionClaim(_projection_lease(recipe, recipe_fields, lease_token))


async def claim_projection_recipe(
    recipe: ProjectionRecipeIdentity | PhysicalProjectionRecipeIdentity,
    *,
    lease_seconds: int = 900,
    database: Any = db,
    schema: str = "mrf",
) -> ProjectionClaim:
    """Claim a live build; failed and sealed recipes require scoped resolution."""

    recipe = validated_physical_projection_recipe_identity(recipe)
    if lease_seconds < 30 or lease_seconds > 86_400:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_lease_seconds_invalid"
        )
    lease_token = secrets.token_hex(32)
    recipe_table = table_ref(schema, "provider_directory_projection_recipe")
    async with database.transaction():
        await set_local_projection_action(
            database,
            "recipe_insert",
            recipe_id=recipe.recipe_id,
            recipe_attempt=1,
            recipe_lease_token=lease_token,
        )
        await _insert_recipe_if_missing(
            database,
            recipe_table,
            recipe,
            lease_token,
            lease_seconds,
        )
        recipe_fields = await _locked_recipe(database, recipe_table, recipe)
        return await _claim_locked_recipe(
            database,
            recipe_table,
            recipe,
            recipe_fields,
            lease_token,
            lease_seconds,
        )


async def heartbeat_projection_lease(
    lease: ProjectionLease,
    *,
    lease_seconds: int = 900,
    database: Any = db,
    schema: str = "mrf",
) -> None:
    """Extend a live recipe lease without opening a long transaction."""

    validated_physical_projection_recipe_identity(lease.recipe)
    if lease_seconds < 30 or lease_seconds > 86_400:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_lease_seconds_invalid"
        )
    recipe_table = table_ref(schema, "provider_directory_projection_recipe")
    async with database.transaction():
        await set_local_projection_action(
            database,
            "recipe_heartbeat",
            recipe_id=lease.recipe.recipe_id,
            recipe_attempt=lease.attempt,
            recipe_lease_token=lease.lease_token,
        )
        updated_count = await database.status(
            f"""
            UPDATE {recipe_table}
               SET lease_expires_at = now() + make_interval(secs => :lease_seconds),
                   lease_heartbeat_at = now(), updated_at = now()
             WHERE recipe_id = :recipe_id AND attempt = :attempt
               AND status IN ('building', 'proof_ready')
               AND lease_token = :lease_token AND lease_expires_at > now();
            """,
            recipe_id=lease.recipe.recipe_id,
            attempt=lease.attempt,
            lease_token=lease.lease_token,
            lease_seconds=lease_seconds,
        )
    if updated_count != 1:
        raise ProviderDirectoryProjectionLeaseLost(
            "provider_directory_projection_lease_lost"
        )
