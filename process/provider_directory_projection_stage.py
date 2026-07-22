# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Logged, index-free staging and bounded PostgreSQL binary COPY."""

from __future__ import annotations

import inspect
import os
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from db.connection import db
from process.provider_directory_projection_db import (
    locked_active_recipe,
    relation_oid,
    row_mapping,
    set_local_projection_action,
    table_ref,
)
from process.provider_directory_projection_types import (
    ProjectionLease,
    ProjectionProofShard,
    ProjectionShardClaim,
    ProjectionStage,
    ProviderDirectoryProjectionError,
    ProviderDirectoryProjectionLeaseLost,
)


STAGE_COPY_COLUMNS = (
    "physical_projection_id",
    "resource_type",
    "resource_id",
    "proof_partition_id",
    "payload_hash",
    "payload_json",
    "source_rank",
    "summary_npi",
    "summary_address_count",
    "summary_addressed_location",
    "summary_geocoded_location",
    "summary_network_link_count",
    "summary_affiliation_link_count",
    "active",
    "effective_start",
    "effective_end",
    "observed_at",
    "profile_evidence_json",
)
_STAGE_RELATION_PREFIX = "pd_projection_stage_"


def projection_copy_batch_size() -> int:
    """Return a bounded row count for each asyncpg binary COPY call."""

    raw_value = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_COPY_BATCH_ROWS", "4096")
    try:
        batch_size = int(raw_value)
    except ValueError as error:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_copy_batch_rows_invalid"
        ) from error
    if not 128 <= batch_size <= 16_384:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_copy_batch_rows_invalid"
        )
    return batch_size


def _stage_relation_name(lease: ProjectionLease) -> str:
    return f"{_STAGE_RELATION_PREFIX}{lease.recipe.recipe_id[:32]}"


async def _validate_bound_stage(
    database: Any,
    schema: str,
    stage: ProjectionStage,
) -> None:
    current_oid = await relation_oid(database, stage.schema, stage.relation)
    stage_is_valid = await database.scalar(
        f"""
        SELECT {table_ref(schema, 'provider_directory_projection_stage_matches_parent')}(
                   :stage_schema, :stage_relation, :stage_relation_oid,
                   'provider_directory_physical_projection_resource', false
               )
           AND EXISTS (
                   SELECT 1 FROM pg_class
                    WHERE oid = CAST(:stage_relation_oid AS oid)
                      AND relpersistence = 'p'
               )
           AND NOT EXISTS (
                   SELECT 1 FROM pg_index
                    WHERE indrelid = CAST(:stage_relation_oid AS oid)
               )
           AND NOT EXISTS (
                   SELECT 1 FROM pg_trigger
                    WHERE tgrelid = CAST(:stage_relation_oid AS oid)
                      AND NOT tgisinternal
               )
           AND NOT EXISTS (
                   SELECT 1 FROM pg_rewrite
                    WHERE ev_class = CAST(:stage_relation_oid AS oid)
               );
        """,
        stage_schema=stage.schema,
        stage_relation=stage.relation,
        stage_relation_oid=stage.relation_oid,
    )
    if current_oid != stage.relation_oid or stage_is_valid is not True:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_stage_invalid"
        )


def _bound_projection_stage(recipe_fields: dict[str, Any]) -> ProjectionStage | None:
    bound_entries = (
        recipe_fields.get("stage_schema"),
        recipe_fields.get("stage_relation"),
        recipe_fields.get("stage_relation_oid"),
    )
    if not any(bound_entry is not None for bound_entry in bound_entries):
        return None
    if not all(bound_entry is not None for bound_entry in bound_entries):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_stage_binding_invalid"
        )
    return ProjectionStage(
        schema=str(bound_entries[0]),
        relation=str(bound_entries[1]),
        relation_oid=int(bound_entries[2]),
    )


async def _create_projection_stage(
    lease: ProjectionLease,
    database: Any,
    schema: str,
) -> ProjectionStage:
    relation = _stage_relation_name(lease)
    if await relation_oid(database, schema, relation) is not None:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_stage_name_conflict"
        )
    await database.status(
        f"""
        CREATE TABLE {table_ref(schema, relation)} (
            LIKE {table_ref(schema, 'provider_directory_physical_projection_resource')}
            INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE
        );
        """
    )
    await database.status(
        f"""
        ALTER TABLE {table_ref(schema, relation)}
            ALTER COLUMN physical_projection_id
            SET DEFAULT '{lease.recipe.recipe_id}',
            ADD CONSTRAINT provider_directory_projection_partition_bound
            CHECK (physical_projection_id = '{lease.recipe.recipe_id}');
        """
    )
    created_oid = await relation_oid(database, schema, relation)
    if created_oid is None:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_stage_create_failed"
        )
    stage = ProjectionStage(schema=schema, relation=relation, relation_oid=created_oid)
    await _validate_bound_stage(database, schema, stage)
    return stage


async def _bind_projection_stage(
    lease: ProjectionLease,
    stage: ProjectionStage,
    database: Any,
    schema: str,
) -> None:
    await set_local_projection_action(
        database,
        "stage_bind",
        recipe_id=lease.recipe.recipe_id,
        recipe_attempt=lease.attempt,
        recipe_lease_token=lease.lease_token,
    )
    updated_count = await database.status(
        f"""
        UPDATE {table_ref(schema, 'provider_directory_projection_recipe')}
           SET stage_schema = :stage_schema, stage_relation = :stage_relation,
               stage_relation_oid = :stage_relation_oid,
               updated_at = clock_timestamp()
         WHERE recipe_id = :recipe_id AND attempt = :attempt
           AND status = 'building' AND lease_token = :lease_token
           AND lease_expires_at > now()
           AND stage_schema IS NULL AND stage_relation IS NULL
           AND stage_relation_oid IS NULL;
        """,
        stage_schema=stage.schema,
        stage_relation=stage.relation,
        stage_relation_oid=stage.relation_oid,
        recipe_id=lease.recipe.recipe_id,
        attempt=lease.attempt,
        lease_token=lease.lease_token,
    )
    if updated_count != 1:
        raise ProviderDirectoryProjectionLeaseLost(
            "provider_directory_projection_lease_lost"
        )


async def ensure_projection_stage(
    lease: ProjectionLease,
    *,
    database: Any = db,
    schema: str = "mrf",
) -> ProjectionStage:
    """Create and immutably bind one durable recipe-owned stage."""

    async with database.transaction():
        recipe_fields = await locked_active_recipe(
            lease,
            database=database,
            schema=schema,
        )
        stage = _bound_projection_stage(recipe_fields)
        if stage is not None:
            await _validate_bound_stage(database, schema, stage)
            return stage
        if recipe_fields.get("workset_registered_at") is None:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_workset_missing"
            )
        stage = await _create_projection_stage(lease, database, schema)
        await _bind_projection_stage(lease, stage, database, schema)
        return stage


async def _copy_driver(transaction: Any) -> Any:
    if any(
        getattr(transaction, method_name, None) is not None
        for method_name in ("copy_records_to_table", "copy_to_table")
    ):
        return transaction
    raw_connection = getattr(transaction, "raw_connection", None)
    if raw_connection is None:
        connection_method = getattr(transaction, "connection", None)
        if connection_method is None:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_copy_driver_missing"
            )
        sql_connection = connection_method()
        if inspect.isawaitable(sql_connection):
            sql_connection = await sql_connection
        raw_connection = await sql_connection.get_raw_connection()
    return getattr(raw_connection, "driver_connection", raw_connection)


async def copy_projection_stage_records(
    stage: ProjectionStage,
    copy_rows: Sequence[tuple[Any, ...]],
    *,
    transaction: Any,
) -> int:
    """Binary-COPY one bounded record batch inside the caller's transaction."""

    if type(stage) is not ProjectionStage or stage.relation_oid < 1:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_stage_invalid"
        )
    if not copy_rows or len(copy_rows) > projection_copy_batch_size() or any(
        type(copy_row) is not tuple or len(copy_row) != len(STAGE_COPY_COLUMNS)
        for copy_row in copy_rows
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_copy_batch_invalid"
        )
    driver_connection = await _copy_driver(transaction)
    copy_method = getattr(driver_connection, "copy_records_to_table", None)
    if copy_method is None:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_copy_driver_missing"
        )
    status = await copy_method(
        stage.relation,
        schema_name=stage.schema,
        columns=STAGE_COPY_COLUMNS,
        records=copy_rows,
    )
    copied_count = len(copy_rows)
    if isinstance(status, str):
        try:
            copied_count = int(status.rsplit(" ", 1)[-1])
        except ValueError as error:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_copy_status_invalid"
            ) from error
    if copied_count != len(copy_rows):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_copy_count_mismatch"
        )
    return copied_count


async def copy_projection_stage_binary_stream(
    stage: ProjectionStage,
    copy_source: Any,
    *,
    transaction: Any,
) -> int:
    """Forward one bounded native PostgreSQL binary COPY stream unchanged."""

    if type(stage) is not ProjectionStage or stage.relation_oid < 1:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_stage_invalid"
        )
    driver_connection = await _copy_driver(transaction)
    copy_method = getattr(driver_connection, "copy_to_table", None)
    if copy_method is None:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_copy_driver_missing"
        )
    status = await copy_method(
        stage.relation,
        source=copy_source,
        schema_name=stage.schema,
        columns=STAGE_COPY_COLUMNS,
        format="binary",
    )
    if not isinstance(status, str) or not status.startswith("COPY "):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_copy_status_invalid"
        )
    try:
        copied_count = int(status.removeprefix("COPY "))
    except ValueError as error:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_copy_status_invalid"
        ) from error
    if copied_count < 0:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_copy_status_invalid"
        )
    return copied_count


async def prepare_projection_stage_partition(
    stage: ProjectionStage,
    claim: ProjectionShardClaim,
    *,
    database: Any,
) -> None:
    """Prime the owned transaction and remove only this retry's orphan rows.

    ROW EXCLUSIVE lets COPY workers share the stage while excluding transform
    hooks and destructive DDL through the digest, census, and checkpoint.
    """

    if type(stage) is not ProjectionStage or type(claim) is not ProjectionShardClaim:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_stage_partition_invalid"
        )
    await database.status(
        f"LOCK TABLE {table_ref(stage.schema, stage.relation)} "
        "IN ROW EXCLUSIVE MODE;"
    )
    await _validate_bound_stage(database, stage.schema, stage)
    if claim.partition_attempt > 1:
        await database.status(
            f"""
            DELETE FROM {table_ref(stage.schema, stage.relation)}
             WHERE physical_projection_id = :physical_projection_id
               AND proof_partition_id = :proof_partition_id;
            """,
            physical_projection_id=claim.recipe_lease.recipe.recipe_id,
            proof_partition_id=claim.shard.partition_id,
        )


@dataclass(frozen=True)
class _ProjectionStageCensus:
    resource_count: int
    resource_count_map: dict[str, int]
    first_identity: tuple[str, str] | None
    last_identity: tuple[str, str] | None


def _stage_identity(candidate: Any) -> tuple[str, str] | None:
    if candidate is None:
        return None
    if not isinstance(candidate, (list, tuple)) or len(candidate) != 2:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_stage_census_invalid"
        )
    return str(candidate[0]), str(candidate[1])


def _stage_resource_count_map(
    candidate: Any,
    selected_resources: Sequence[str],
) -> dict[str, int]:
    if not isinstance(candidate, Mapping) or any(
        not isinstance(resource_type, str) or type(count) is not int or count < 0
        for resource_type, count in candidate.items()
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_stage_census_invalid"
        )
    resource_count_map = dict(candidate)
    for resource_type in selected_resources:
        resource_count_map.setdefault(resource_type, 0)
    return resource_count_map


async def _stage_partition_census(
    stage: ProjectionStage,
    claim: ProjectionShardClaim,
    database: Any,
) -> _ProjectionStageCensus:
    census_row = await database.first(
        f"""
        WITH staged_rows AS (
            SELECT resource_type, resource_id
              FROM {table_ref(stage.schema, stage.relation)}
             WHERE physical_projection_id = :physical_projection_id
               AND proof_partition_id = :proof_partition_id
        ), resource_counts AS (
            SELECT resource_type, count(*)::bigint AS resource_count
              FROM staged_rows GROUP BY resource_type
        )
        SELECT count(*)::bigint AS resource_count,
               min(ARRAY[
                   resource_type COLLATE "C", resource_id COLLATE "C"
               ]) AS first_identity,
               max(ARRAY[
                   resource_type COLLATE "C", resource_id COLLATE "C"
               ]) AS last_identity,
               COALESCE((
                   SELECT jsonb_object_agg(resource_type, resource_count)
                     FROM resource_counts
               ), '{{}}'::jsonb) AS resource_count_map
          FROM staged_rows;
        """,
        physical_projection_id=claim.recipe_lease.recipe.recipe_id,
        proof_partition_id=claim.shard.partition_id,
    )
    if census_row is None:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_stage_census_invalid"
        )
    census_row_map = row_mapping(census_row)
    resource_count = census_row_map.get("resource_count")
    if type(resource_count) is not int or resource_count < 0:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_stage_census_invalid"
        )
    return _ProjectionStageCensus(
        resource_count,
        _stage_resource_count_map(
            census_row_map.get("resource_count_map"),
            claim.recipe_lease.recipe.selected_resources,
        ),
        _stage_identity(census_row_map.get("first_identity")),
        _stage_identity(census_row_map.get("last_identity")),
    )


async def assert_projection_stage_partition(
    stage: ProjectionStage,
    claim: ProjectionShardClaim,
    shard_proof: ProjectionProofShard,
    *,
    database: Any,
) -> None:
    """Require the exact staged census on the caller's owned transaction."""

    await _validate_bound_stage(database, stage.schema, stage)
    census = await _stage_partition_census(stage, claim, database)
    if (
        census.resource_count != shard_proof.resource_count
        or census.resource_count_map != shard_proof.proof.get("resource_counts")
        or census.first_identity != shard_proof.first_identity
        or census.last_identity != shard_proof.last_identity
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_stage_census_mismatch"
        )


__all__ = (
    "STAGE_COPY_COLUMNS",
    "assert_projection_stage_partition",
    "copy_projection_stage_binary_stream",
    "copy_projection_stage_records",
    "ensure_projection_stage",
    "prepare_projection_stage_partition",
    "projection_copy_batch_size",
)
