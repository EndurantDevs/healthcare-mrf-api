# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Database catalog and lease helpers shared by projection phases."""

from __future__ import annotations

import json
import os
from typing import Any, Mapping

from process.provider_directory_projection_contract import (
    validated_physical_projection_recipe_identity,
)
from process.provider_directory_projection_types import (
    HASH_PATTERN,
    IDENTIFIER_PATTERN,
    PROJECTION_RECIPE_CONTRACT_ID,
    PreparedProjectionStage,
    ProjectionLease,
    ProviderDirectoryProjectionError,
    ProviderDirectoryProjectionLeaseLost,
)


PROJECTION_DATABASE_ACTIONS = frozenset(
    {
        "admission_heartbeat",
        "admission_insert",
        "admission_map",
        "admission_reclaim",
        "admission_seal",
        "fail",
        "gc",
        "reference_heartbeat",
        "reference_insert",
        "reference_reclaim",
        "reference_release",
        "recipe_heartbeat",
        "recipe_insert",
        "recipe_reclaim",
        "shard_claim",
        "shard_complete",
        "shard_heartbeat",
        "stage_bind",
        "workset_register",
    }
)


def row_mapping(database_row: Any) -> dict[str, Any]:
    """Return a plain mapping for SQLAlchemy and compatibility rows."""

    if database_row is None:
        return {}
    database_mapping = getattr(database_row, "_mapping", database_row)
    return dict(database_mapping)


def json_value(database_value: Any) -> Any:
    """Decode JSON only when a driver returned serialized text."""

    if isinstance(database_value, str):
        return json.loads(database_value)
    return database_value


def quoted_identifier(identifier: str) -> str:
    """Quote one already validated PostgreSQL identifier."""

    if IDENTIFIER_PATTERN.fullmatch(identifier) is None:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_identifier_invalid"
        )
    return f'"{identifier}"'


def table_ref(schema: str, relation: str) -> str:
    """Return a safely quoted schema-qualified relation or function."""

    return f"{quoted_identifier(schema)}.{quoted_identifier(relation)}"


def projection_maintenance_work_mem_mb() -> int:
    """Return the bounded memory budget for one post-COPY DDL operation."""

    raw_value = os.getenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_MAINTENANCE_WORK_MEM_MB",
        "256",
    )
    try:
        memory_mb = int(raw_value)
    except ValueError as error:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_maintenance_work_mem_invalid"
        ) from error
    if not 64 <= memory_mb <= 1024:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_maintenance_work_mem_invalid"
        )
    return memory_mb


async def set_local_projection_maintenance_work_mem(database: Any) -> None:
    """Keep bounded index/attach sorts in memory for the current transaction."""

    memory_mb = projection_maintenance_work_mem_mb()
    await database.status(f"SET LOCAL maintenance_work_mem = '{memory_mb}MB';")


async def set_local_projection_synchronous_commit(
    database: Any,
    value: str,
) -> None:
    """Set and verify the projection transaction's WAL commit boundary."""

    if value not in {"off", "on"}:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_durability_invalid"
        )
    await database.status(f"SET LOCAL synchronous_commit = {value};")
    current_value = await database.scalar(
        "SELECT current_setting('synchronous_commit');"
    )
    if current_value != value:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_durability_invalid"
        )


async def set_local_projection_action(
    database: Any,
    action: str,
    *,
    recipe_id: str,
    recipe_attempt: int,
    recipe_lease_token: str | None = None,
    partition_id: str | None = None,
    partition_attempt: int | None = None,
    shard_lease_token: str | None = None,
    physical_projection_id: str | None = None,
    reference_owner_kind: str | None = None,
    reference_owner_id: str | None = None,
    reference_identity_hash: str | None = None,
    reference_lease_token: str | None = None,
    previous_reference_lease_token: str | None = None,
    admission_id: str | None = None,
    admission_attempt: int | None = None,
    admission_lease_token: str | None = None,
) -> None:
    """Fence one exact projection database transition in this transaction."""

    if action not in PROJECTION_DATABASE_ACTIONS:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_action_invalid"
        )
    hash_values = (recipe_id,)
    optional_hash_values = (
        recipe_lease_token,
        partition_id,
        shard_lease_token,
        physical_projection_id,
        reference_identity_hash,
        reference_lease_token,
        previous_reference_lease_token,
        admission_id,
        admission_lease_token,
    )
    if (
        any(HASH_PATTERN.fullmatch(value) is None for value in hash_values)
        or any(
            candidate_hash is not None
            and HASH_PATTERN.fullmatch(candidate_hash) is None
            for candidate_hash in optional_hash_values
        )
        or recipe_attempt < 1
        or (partition_attempt is not None and partition_attempt < 0)
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_action_identity_invalid"
        )
    reference_actions = {
        "reference_heartbeat",
        "reference_insert",
        "reference_reclaim",
        "reference_release",
    }
    reference_values = (
        reference_owner_kind,
        reference_owner_id,
        reference_identity_hash,
        reference_lease_token,
        previous_reference_lease_token,
    )
    admission_actions = {
        "admission_heartbeat",
        "admission_insert",
        "admission_map",
        "admission_reclaim",
        "admission_seal",
    }
    admission_values = (
        admission_id,
        admission_attempt,
        admission_lease_token,
    )
    if action in admission_actions:
        if (
            admission_id is None
            or admission_attempt is None
            or admission_attempt < 1
            or admission_lease_token is None
            or recipe_lease_token is not None
            or partition_id is not None
            or partition_attempt is not None
            or shard_lease_token is not None
            or physical_projection_id is not None
            or any(value is not None for value in reference_values)
        ):
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_action_identity_invalid"
            )
    elif action in reference_actions:
        reference_identity_is_valid = all(
            (
                physical_projection_id is not None,
                reference_owner_kind in {"dataset", "build", "artifact"},
                isinstance(reference_owner_id, str),
                bool(reference_owner_id),
                len(reference_owner_id or "") <= 128,
                reference_identity_hash is not None,
                reference_lease_token is not None,
                recipe_lease_token is None,
                partition_id is None,
                partition_attempt is None,
                shard_lease_token is None,
            )
        )
        previous_token_is_valid = (
            previous_reference_lease_token is not None
            if action == "reference_reclaim"
            else previous_reference_lease_token is None
        )
        if not reference_identity_is_valid or not previous_token_is_valid:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_action_identity_invalid"
            )
    elif any(reference_value is not None for reference_value in reference_values) or any(
        admission_value is not None for admission_value in admission_values
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_action_identity_invalid"
        )
    setting_by_name = {
        "action": action,
        "recipe_id": recipe_id,
        "recipe_attempt": str(recipe_attempt),
        "recipe_lease_token": recipe_lease_token or "",
        "partition_id": partition_id or "",
        "partition_attempt": (
            "" if partition_attempt is None else str(partition_attempt)
        ),
        "shard_lease_token": shard_lease_token or "",
        "physical_id": physical_projection_id or "",
        "reference_owner_kind": reference_owner_kind or "",
        "reference_owner_id": reference_owner_id or "",
        "reference_identity_hash": reference_identity_hash or "",
        "reference_lease_token": reference_lease_token or "",
        "reference_previous_lease_token": (
            previous_reference_lease_token or ""
        ),
        "admission_id": admission_id or "",
        "admission_attempt": (
            "" if admission_attempt is None else str(admission_attempt)
        ),
        "admission_lease_token": admission_lease_token or "",
    }
    for setting_name, setting_value in setting_by_name.items():
        stored_value = await database.scalar(
            "SELECT set_config("
            f"'healthporta.provider_directory_projection_{setting_name}', "
            ":setting_value, true);",
            setting_value=setting_value,
        )
        if stored_value != setting_value:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_action_setting_failed"
            )


async def set_local_projection_wal_compression(database: Any) -> str | None:
    """Enable and return the best WAL compression mode available to this role."""

    mode = (
        os.getenv(
            "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_WAL_COMPRESSION",
            "auto",
        )
        .strip()
        .lower()
    )
    if mode not in {"auto", "off", "on"}:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_wal_compression_invalid"
        )
    if mode == "off":
        return None
    server_version = int(
        await database.scalar("SELECT current_setting('server_version_num')::integer;")
        or 0
    )
    can_set_wal_compression = server_version >= 150000 and (
        await database.scalar(
            "SELECT has_parameter_privilege(current_user, " "'wal_compression', 'SET');"
        )
        is True
    )
    if not can_set_wal_compression:
        if mode == "on":
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_wal_compression_unavailable"
            )
        return None
    supports_zstd = (
        await database.scalar(
            "SELECT COALESCE('zstd' = ANY(enumvals), false) "
            "FROM pg_settings WHERE name = 'wal_compression';"
        )
        is True
    )
    algorithm = "zstd" if supports_zstd else "on"
    await database.status(f"SET LOCAL wal_compression = {algorithm};")
    if await database.scalar("SELECT current_setting('wal_compression');") == "off":
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_wal_compression_unavailable"
        )
    return algorithm


def recipe_database_identity(database_fields: Mapping[str, Any]) -> dict[str, Any]:
    """Project one stored recipe into its canonical identity fields."""

    return {
        "contract_id": PROJECTION_RECIPE_CONTRACT_ID,
        "decoder_contract_id": database_fields.get("decoder_contract_id"),
        "input_set_sha256": database_fields.get("input_set_sha256"),
        "transform_contract_id": database_fields.get("transform_contract_id"),
        "scope_contract_id": database_fields.get("scope_contract_id"),
        "transform_context_hash": database_fields.get("transform_context_hash"),
        "transform_context": json_value(database_fields.get("transform_context_json")),
        "resource_profile_hash": database_fields.get("resource_profile_hash"),
        "selected_resources": json_value(
            database_fields.get("selected_resources_json")
        ),
        "required_resources": json_value(
            database_fields.get("required_resources_json")
        ),
    }


async def relation_oid(
    database: Any,
    schema: str,
    relation: str,
) -> int | None:
    """Resolve a relation name to its current PostgreSQL OID."""

    current_oid = await database.scalar(
        "SELECT to_regclass(:qualified_relation)::oid::bigint;",
        qualified_relation=f"{schema}.{relation}",
    )
    return int(current_oid) if current_oid is not None else None


async def locked_active_recipe(
    lease: ProjectionLease,
    *,
    database: Any,
    schema: str,
) -> dict[str, Any]:
    """Lock one recipe and prove the caller still owns its live lease."""

    validated_physical_projection_recipe_identity(lease.recipe)
    recipe_fields = row_mapping(
        await database.first(
            f"SELECT * FROM {table_ref(schema, 'provider_directory_projection_recipe')} "
            "WHERE recipe_id = :recipe_id FOR UPDATE;",
            recipe_id=lease.recipe.recipe_id,
        )
    )
    is_live = bool(
        await database.scalar(
            "SELECT :lease_expires_at > now();",
            lease_expires_at=recipe_fields.get("lease_expires_at"),
        )
    )
    if (
        recipe_fields.get("status") not in {"building", "proof_ready"}
        or int(recipe_fields.get("attempt") or 0) != lease.attempt
        or recipe_fields.get("lease_token") != lease.lease_token
        or recipe_database_identity(recipe_fields) != lease.recipe.identity_payload
        or not is_live
    ):
        raise ProviderDirectoryProjectionLeaseLost(
            "provider_directory_projection_lease_lost"
        )
    return recipe_fields


async def shared_active_recipe(
    lease: ProjectionLease,
    *,
    database: Any,
    schema: str,
) -> dict[str, Any]:
    """Hold a shared recipe fence while one independent shard uses COPY."""

    validated_physical_projection_recipe_identity(lease.recipe)
    recipe_fields = row_mapping(
        await database.first(
            f"SELECT * FROM {table_ref(schema, 'provider_directory_projection_recipe')} "
            "WHERE recipe_id = :recipe_id FOR SHARE;",
            recipe_id=lease.recipe.recipe_id,
        )
    )
    is_live = bool(
        await database.scalar(
            "SELECT :lease_expires_at > now();",
            lease_expires_at=recipe_fields.get("lease_expires_at"),
        )
    )
    if (
        recipe_fields.get("status") != "building"
        or int(recipe_fields.get("attempt") or 0) != lease.attempt
        or recipe_fields.get("lease_token") != lease.lease_token
        or recipe_database_identity(recipe_fields) != lease.recipe.identity_payload
        or not is_live
    ):
        raise ProviderDirectoryProjectionLeaseLost(
            "provider_directory_projection_lease_lost"
        )
    return recipe_fields


async def assert_stage_trigger(
    database: Any,
    prepared: PreparedProjectionStage,
) -> None:
    """Verify the exact immutable stage trigger by OID and catalog identity."""

    stages = [(prepared.stage, prepared.storage_trigger_oid)]
    for stage, expected_trigger_oid in stages:
        trigger_fields = row_mapping(
            await database.first(
                """
            SELECT trigger_record.oid::bigint AS trigger_oid,
                   relation_record.relname AS relation_name,
                   relation_schema.nspname AS relation_schema,
                   relation_record.relkind AS relation_kind,
                   function_record.proname AS function_name,
                   function_schema.nspname AS function_schema,
                   trigger_record.tgenabled, trigger_record.tgtype,
                   trigger_record.tgqual IS NULL AS has_no_when,
                   trigger_record.tgnargs,
                   trigger_record.tgoldtable,
                   trigger_record.tgnewtable,
                   trigger_record.tgattr::text AS trigger_columns,
                   pg_get_triggerdef(trigger_record.oid, true) AS trigger_definition
              FROM pg_trigger AS trigger_record
              JOIN pg_class AS relation_record
                ON relation_record.oid = trigger_record.tgrelid
              JOIN pg_namespace AS relation_schema
                ON relation_schema.oid = relation_record.relnamespace
              JOIN pg_proc AS function_record
                ON function_record.oid = trigger_record.tgfoid
              JOIN pg_namespace AS function_schema
                ON function_schema.oid = function_record.pronamespace
             WHERE trigger_record.tgrelid = CAST(:relation_oid AS oid)
               AND trigger_record.tgname =
                   'provider_directory_projection_stage_immutable'
               AND NOT trigger_record.tgisinternal;
            """,
                relation_oid=stage.relation_oid,
            )
        )
        trigger_enabled = trigger_fields.get("tgenabled")
        if isinstance(trigger_enabled, bytes):
            trigger_enabled = trigger_enabled.decode("ascii")
        definition = str(trigger_fields.get("trigger_definition") or "").upper()
        is_exact = all(
            (
                int(trigger_fields.get("trigger_oid") or 0)
                == int(expected_trigger_oid or 0),
                trigger_fields.get("relation_schema") == stage.schema,
                trigger_fields.get("relation_name") == stage.relation,
                trigger_fields.get("relation_kind") in {"r", "p"},
                trigger_fields.get("function_schema") == stage.schema,
                trigger_fields.get("function_name")
                == "reject_provider_directory_projection_stage_mutation",
                trigger_enabled == "O",
                int(trigger_fields.get("tgtype") or 0) == 31,
                trigger_fields.get("has_no_when") is True,
                int(trigger_fields.get("tgnargs") or 0) == 0,
                trigger_fields.get("tgoldtable") is None,
                trigger_fields.get("tgnewtable") is None,
                str(trigger_fields.get("trigger_columns") or "") == "",
                all(
                    token in definition
                    for token in (
                        "BEFORE",
                        "INSERT",
                        "UPDATE",
                        "DELETE",
                        "FOR EACH ROW",
                    )
                ),
            )
        )
        if not is_exact:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_stage_trigger_mismatch"
            )
