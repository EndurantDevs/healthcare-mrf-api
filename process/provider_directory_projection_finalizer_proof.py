# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded proof and index work for atomic projection finalization."""

from __future__ import annotations

import hashlib
import json
from typing import Any, AsyncIterator, Mapping

from process.provider_directory_projection_contract import (
    canonical_row_line,
    claimed_projection_proof_shard,
)
from process.provider_directory_projection_db import (
    assert_stage_trigger,
    json_value,
    quoted_identifier,
    row_mapping,
    table_ref,
)
from process.provider_directory_projection_stage import _validate_bound_stage
from process.provider_directory_projection_types import (
    PhysicalProjectionProof,
    PreparedProjectionStage,
    ProjectionLease,
    ProjectionProofShard,
    ProjectionShardClaim,
    ProjectionShardSpec,
    ProjectionStage,
    ProviderDirectoryProjectionError,
)


_CURSOR_PREFETCH = 4096
_SHARD_ORDER_FIELDS = (
    "proof_partition_id",
    "resource_type",
    "resource_id",
    "source_rank",
    "payload_hash",
)


async def _cursor(
    connection: Any,
    query: str,
    *arguments: Any,
) -> AsyncIterator[Mapping[str, Any]]:
    driver = getattr(
        connection.raw_connection,
        "driver_connection",
        connection.raw_connection,
    )
    cursor = getattr(driver, "cursor", None)
    if cursor is None:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_finalizer_driver_missing"
        )
    async for record in cursor(query, *arguments, prefetch=_CURSOR_PREFETCH):
        yield dict(record)


def projection_stage(recipe_fields: Mapping[str, Any]) -> ProjectionStage:
    """Return the exact bound stage from one locked recipe row."""

    try:
        stage = ProjectionStage(
            schema=str(recipe_fields["stage_schema"]),
            relation=str(recipe_fields["stage_relation"]),
            relation_oid=int(recipe_fields["stage_relation_oid"]),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_stage_binding_invalid"
        ) from error
    if not stage.schema or not stage.relation or stage.relation_oid < 1:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_stage_binding_invalid"
        )
    return stage


def _claim_for_shard(
    lease: ProjectionLease,
    shard_fields: Mapping[str, Any],
) -> ProjectionShardClaim:
    return ProjectionShardClaim(
        recipe_lease=lease,
        admission_id="0" * 64,
        shard=ProjectionShardSpec(
            partition_id=str(shard_fields["partition_id"]),
            partition_ordinal=int(shard_fields["partition_ordinal"]),
            partition_key=str(shard_fields["partition_key"]),
            input_block_id=str(shard_fields["input_block_id"]),
            resource_type=str(shard_fields["resource_type"]),
            input_sha256=str(shard_fields["input_sha256"]),
        ),
        partition_attempt=int(shard_fields["partition_attempt"]),
        lease_token="0" * 64,
    )


async def completed_shards(
    connection: Any,
    schema: str,
    lease: ProjectionLease,
    partition_count: int,
) -> tuple[ProjectionProofShard, ...]:
    """Load and revalidate every completed shard proof."""

    shard_rows = await connection.all(
        f"""
        SELECT partition_id, partition_ordinal, partition_key, input_block_id,
               resource_type, input_sha256, status, partition_attempt,
               proof_json
          FROM {table_ref(schema, 'provider_directory_projection_proof_shard')}
         WHERE recipe_id = :recipe_id AND attempt = :attempt
         ORDER BY resource_type, partition_ordinal, partition_id;
        """,
        recipe_id=lease.recipe.recipe_id,
        attempt=lease.attempt,
    )
    if (
        type(partition_count) is not int
        or partition_count < 1
        or len(shard_rows) != partition_count
        or any(
            row_mapping(shard_record).get("status") != "complete"
            for shard_record in shard_rows
        )
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_finalizer_shards_incomplete"
        )
    shards = []
    for shard_row in shard_rows:
        shard_fields = row_mapping(shard_row)
        shards.append(
            claimed_projection_proof_shard(
                json_value(shard_fields.get("proof_json")),
                claim=_claim_for_shard(lease, shard_fields),
            )
        )
    return tuple(shards)


async def assert_no_live_children(
    connection: Any,
    schema: str,
    lease: ProjectionLease,
) -> None:
    """Require every retained child lease to be released."""

    live_count = await connection.scalar(
        f"""
        SELECT count(*)::bigint
          FROM {table_ref(schema, 'provider_directory_projection_child_read_lease')}
         WHERE recipe_id = :recipe_id AND recipe_attempt = :attempt
           AND status <> 'released';
        """,
        recipe_id=lease.recipe.recipe_id,
        attempt=lease.attempt,
    )
    if int(live_count or 0):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_finalizer_child_live"
        )


def _row_identity(row: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return tuple(str(row[field]) for field in _SHARD_ORDER_FIELDS[1:])


def _assert_native_source_rank(
    shard: ProjectionProofShard,
    row: Mapping[str, Any],
) -> None:
    source_rank = row.get("source_rank")
    expected_prefix = f"{shard.partition_ordinal:020d}:{row.get('payload_hash')}:"
    input_ordinal = (
        source_rank.removeprefix(expected_prefix)
        if type(source_rank) is str
        else ""
    )
    if (
        type(source_rank) is not str
        or not source_rank.startswith(expected_prefix)
        or len(input_ordinal) != 20
        or not input_ordinal.isascii()
        or not input_ordinal.isdigit()
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_source_rank_invalid"
        )


def _assert_partition_digest(
    shard: ProjectionProofShard,
    digest: Any,
    count: int,
    first_identity: tuple[str, str] | None,
    last_identity: tuple[str, str] | None,
) -> None:
    if (
        digest.hexdigest() != shard.canonical_row_sha256
        or count != shard.resource_count
        or first_identity != shard.first_identity
        or last_identity != shard.last_identity
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_finalizer_shard_mismatch"
        )


def _stage_shard_query(stage: ProjectionStage) -> str:
    return f"""
        SELECT {', '.join(_SHARD_ORDER_FIELDS)},
               summary_npi, summary_address_count,
               summary_addressed_location, summary_geocoded_location,
               summary_network_link_count, summary_affiliation_link_count,
               profile_evidence_json::text AS profile_evidence_json
          FROM {table_ref(stage.schema, stage.relation)}
         WHERE physical_projection_id = $1
         ORDER BY proof_partition_id, resource_type COLLATE "C",
                  resource_id COLLATE "C", source_rank COLLATE "C",
                  payload_hash COLLATE "C";
    """


async def verify_stage_shards(
    connection: Any,
    stage: ProjectionStage,
    lease: ProjectionLease,
    shards: tuple[ProjectionProofShard, ...],
) -> None:
    """Recompute every staged shard digest from one locked snapshot."""

    shard_by_id = {shard.partition_id: shard for shard in shards}
    observed_ids: set[str] = set()
    current_shard: ProjectionProofShard | None = None
    digest, count = hashlib.sha256(), 0
    first_identity: tuple[str, str] | None = None
    last_identity: tuple[str, str] | None = None
    previous_row_identity: tuple[str, str, str, str] | None = None
    async for stage_record in _cursor(
        connection, _stage_shard_query(stage), lease.recipe.recipe_id
    ):
        partition_id = str(stage_record["proof_partition_id"])
        if current_shard is None or partition_id != current_shard.partition_id:
            if current_shard is not None:
                _assert_partition_digest(
                    current_shard, digest, count, first_identity, last_identity
                )
            current_shard = shard_by_id.get(partition_id)
            if current_shard is None or partition_id in observed_ids:
                raise ProviderDirectoryProjectionError(
                    "provider_directory_projection_finalizer_shard_mismatch"
                )
            observed_ids.add(partition_id)
            digest, count = hashlib.sha256(), 0
            first_identity = last_identity = None
            previous_row_identity = None
        stage_record["profile_evidence_json"] = (
            json.loads(stage_record["profile_evidence_json"])
            if stage_record["profile_evidence_json"] is not None
            else None
        )
        _assert_native_source_rank(current_shard, stage_record)
        identity = _row_identity(stage_record)
        if previous_row_identity is not None and identity <= previous_row_identity:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_rows_not_strictly_sorted"
            )
        if count:
            digest.update(b"\n")
        digest.update(canonical_row_line(stage_record))
        resource_identity = identity[:2]
        first_identity = first_identity or resource_identity
        last_identity = resource_identity
        previous_row_identity = identity
        count += 1
    if current_shard is not None:
        _assert_partition_digest(
            current_shard, digest, count, first_identity, last_identity
        )
    if observed_ids != set(shard_by_id):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_finalizer_shard_mismatch"
        )


async def materialize_winner_set(
    connection: Any,
    stage: ProjectionStage,
    lease: ProjectionLease,
) -> None:
    """Keep the minimum deterministic rank/hash for each resource identity."""

    relation_ref = table_ref(stage.schema, stage.relation)
    await connection.status(
        f"""
        WITH ranked_rows AS (
            SELECT ctid,
                   row_number() OVER (
                       PARTITION BY resource_type, resource_id
                       ORDER BY source_rank COLLATE "C", payload_hash COLLATE "C"
                   ) AS source_ordinal
              FROM {relation_ref}
             WHERE physical_projection_id = :projection_id
        )
        DELETE FROM {relation_ref} AS staged_resource
         USING ranked_rows
         WHERE staged_resource.ctid = ranked_rows.ctid
           AND ranked_rows.source_ordinal > 1;
        """,
        projection_id=lease.recipe.recipe_id,
    )


async def prepare_immutable_stage(
    connection: Any,
    schema: str,
    stage: ProjectionStage,
    proof: PhysicalProjectionProof,
) -> PreparedProjectionStage:
    """Build final indexes and install the exact immutable trigger."""

    await _validate_bound_stage(connection, schema, stage)
    relation_ref = table_ref(stage.schema, stage.relation)
    await connection.status(
        f"CREATE UNIQUE INDEX {quoted_identifier(stage.relation + '_key')} "
        f"ON {relation_ref} (physical_projection_id, resource_type, resource_id);"
    )
    await connection.status(
        f"CREATE INDEX {quoted_identifier(stage.relation + '_identity')} "
        f"ON {relation_ref} (resource_type, resource_id, physical_projection_id);"
    )
    await connection.status(
        "CREATE TRIGGER provider_directory_projection_stage_immutable "
        f"BEFORE INSERT OR UPDATE OR DELETE ON {relation_ref} "
        "FOR EACH ROW EXECUTE FUNCTION "
        f"{table_ref(stage.schema, 'reject_provider_directory_projection_stage_mutation')}();"
    )
    trigger_oid = await connection.scalar(
        """
        SELECT oid::bigint FROM pg_trigger
         WHERE tgrelid = CAST(:relation_oid AS oid)
           AND tgname = 'provider_directory_projection_stage_immutable'
           AND NOT tgisinternal;
        """,
        relation_oid=stage.relation_oid,
    )
    if trigger_oid is None:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_stage_trigger_invalid"
        )
    prepared = PreparedProjectionStage(
        stage,
        int(trigger_oid),
        proof,
    )
    await assert_stage_trigger(connection, prepared)
    return prepared
