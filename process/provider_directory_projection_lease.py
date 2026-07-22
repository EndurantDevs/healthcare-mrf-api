# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Short-transaction recipe claim and reclaim operations."""

from __future__ import annotations

import secrets
from typing import Any

from db.connection import db
from process.provider_directory_projection_db import (
    recipe_database_identity,
    row_mapping,
    set_local_projection_action,
    table_ref,
)
from process.provider_directory_projection_types import (
    ProjectionClaim,
    ProjectionLease,
    ProjectionRecipeIdentity,
    ProviderDirectoryProjectionBusy,
    ProviderDirectoryProjectionError,
    ProviderDirectoryProjectionLeaseLost,
    required_hash,
    stable_json,
)


async def _insert_recipe_if_missing(
    database: Any,
    recipe_table: str,
    recipe: ProjectionRecipeIdentity,
    lease_token: str,
    lease_seconds: int,
) -> None:
    await database.status(
        f"""
        INSERT INTO {recipe_table} (
            recipe_id, decoder_contract_id, input_set_sha256,
            transform_contract_id, scope_contract_id, transform_context_hash,
            transform_context_json, completeness_manifest_hash,
            completeness_manifest_json, resource_profile_hash,
            selected_resources_json, required_resources_json, status,
            attempt, lease_token, lease_expires_at, lease_heartbeat_at,
            created_at, updated_at
        ) VALUES (
            :recipe_id, :decoder_contract_id, :input_set_sha256,
            :transform_contract_id, :scope_contract_id, :transform_context_hash,
            CAST(:transform_context_json AS jsonb),
            :completeness_manifest_hash,
            CAST(:completeness_manifest_json AS jsonb),
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
        completeness_manifest_hash=recipe.completeness_manifest_hash,
        completeness_manifest_json=stable_json(recipe.completeness_manifest),
        resource_profile_hash=recipe.resource_profile_hash,
        selected_resources_json=stable_json(recipe.selected_resources),
        required_resources_json=stable_json(recipe.required_resources),
        lease_token=lease_token,
        lease_seconds=lease_seconds,
    )


async def _locked_recipe(
    database: Any,
    recipe_table: str,
    recipe: ProjectionRecipeIdentity,
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
    recipe: ProjectionRecipeIdentity,
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
    recipe: ProjectionRecipeIdentity,
    recipe_fields: dict[str, Any],
    lease_token: str,
    lease_seconds: int,
) -> ProjectionClaim:
    """Return a reusable projection or fence one live recipe generation."""

    if recipe_fields.get("status") == "sealed":
        if (
            recipe_fields.get("workset_registered_at") is None
            or recipe_fields.get("input_block_set_sha256") != recipe.input_set_sha256
        ):
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_sealed_workset_missing"
            )
        return ProjectionClaim(
            lease=None,
            physical_projection_id=required_hash(
                recipe_fields.get("physical_projection_id"),
                "physical_projection_id",
            ),
        )
    if recipe_fields.get("status") not in {"building", "proof_ready"}:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_recipe_not_reclaimable"
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
    return ProjectionClaim(
        lease=_projection_lease(recipe, recipe_fields, lease_token),
        physical_projection_id=None,
    )


async def claim_projection_recipe(
    recipe: ProjectionRecipeIdentity,
    *,
    lease_seconds: int = 900,
    database: Any = db,
    schema: str = "mrf",
) -> ProjectionClaim:
    """Claim or reclaim one recipe without discarding completed proof shards."""

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
