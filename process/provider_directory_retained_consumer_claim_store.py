# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Claim, heartbeat, release, and immutable consumer handoff."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import asyncpg

from process.provider_directory_retained_artifact_contract import (
    RetainedArtifactError,
    RetainedCampaignIncomplete,
    RetainedCampaignMismatch,
    SealedRetainedCampaign,
    require_digest,
    require_positive_int,
    require_safe_id,
    sealed_campaign_digest,
)
from process.provider_directory_retained_handoff import (
    ClaimSnapshot,
    ClaimTables,
    MAX_CLAIM_ITEMS,
    claim_tables,
    locked_claim_snapshot,
    sealed_campaign_handoff,
)
from process.provider_directory_retained_store_support import (
    database_record,
    database_record_list,
    database_schema,
    database_table,
)


def _verified_claim_digest(claim_snapshot: ClaimSnapshot) -> str:
    recalculated_digest = sealed_campaign_digest(
        campaign_row=claim_snapshot.campaign_record_by_field,
        item_rows=claim_snapshot.campaign_item_states,
        stream_rows=claim_snapshot.stream_states,
        artifact_rows=claim_snapshot.artifact_states,
        layout_rows=claim_snapshot.layout_states,
        range_rows=claim_snapshot.layout_range_states,
    )
    if recalculated_digest != claim_snapshot.campaign_record_by_field.get(
        "campaign_sha256"
    ):
        raise RetainedCampaignMismatch("sealed_campaign_digest_mismatch")
    return recalculated_digest


async def _upsert_consumer_claim(
    connection: asyncpg.Connection,
    tables: ClaimTables,
    campaign_id: str,
    consumer_recipe_id: str,
    campaign_sha256: str,
) -> tuple[int, bool]:
    previous_claim = database_record(
        await connection.fetchrow(
            f"""SELECT claimed_campaign_sha256, claim_generation, released_at
                   FROM {tables.consumer}
                  WHERE campaign_id=$1 AND consumer_recipe_id=$2
                  FOR UPDATE;""",
            campaign_id,
            consumer_recipe_id,
        )
    )
    claim_generation = await connection.fetchval(
        f"""INSERT INTO {tables.consumer} (
                campaign_id, consumer_recipe_id,
                claimed_campaign_sha256, claim_generation,
                claimed_at, heartbeat_at
            ) VALUES ($1, $2, $3, 1, now(), now())
            ON CONFLICT (campaign_id, consumer_recipe_id) DO UPDATE
                SET claim_generation=CASE
                        WHEN {tables.consumer}.released_at IS NULL
                        THEN {tables.consumer}.claim_generation
                        ELSE {tables.consumer}.claim_generation + 1
                    END,
                    claimed_at=CASE
                        WHEN {tables.consumer}.released_at IS NULL
                        THEN {tables.consumer}.claimed_at ELSE now()
                    END,
                    heartbeat_at=now(), released_at=NULL
              WHERE {tables.consumer}.claimed_campaign_sha256=
                    EXCLUDED.claimed_campaign_sha256
            RETURNING claim_generation;""",
        campaign_id,
        consumer_recipe_id,
        campaign_sha256,
    )
    if type(claim_generation) is not int or claim_generation < 1:
        raise RetainedCampaignMismatch("consumer_claim_identity_mismatch")
    return claim_generation, previous_claim.get("released_at") is not None


async def _upsert_consumer_references(
    connection: asyncpg.Connection,
    tables: ClaimTables,
    campaign_id: str,
    consumer_recipe_id: str,
    claim_generation: int,
    allow_reopen: bool,
    admitted_item_states: Sequence[Mapping[str, Any]],
) -> None:
    if len(admitted_item_states) > MAX_CLAIM_ITEMS:
        raise RetainedCampaignIncomplete("retained_consumer_refs_unbounded")
    await connection.execute(
        f"""INSERT INTO {tables.consumer_reference} (
                campaign_id, consumer_recipe_id, source_item_id,
                artifact_sha256, layout_sha256, claim_generation,
                created_at, released_at
            ) SELECT $1, $2, reference.source_item_id,
                     reference.artifact_sha256, reference.layout_sha256,
                     $6, now(), NULL
                FROM unnest($3::text[], $4::text[], $5::text[])
                     AS reference(source_item_id, artifact_sha256, layout_sha256)
            ON CONFLICT (
                campaign_id, consumer_recipe_id, source_item_id
            ) DO UPDATE SET
                artifact_sha256=EXCLUDED.artifact_sha256,
                layout_sha256=EXCLUDED.layout_sha256,
                claim_generation=EXCLUDED.claim_generation,
                created_at=now(), released_at=NULL
              WHERE $7 AND
                    {tables.consumer_reference}.released_at IS NOT NULL;""",
        campaign_id,
        consumer_recipe_id,
        [state["source_item_id"] for state in admitted_item_states],
        [state["artifact_sha256"] for state in admitted_item_states],
        [state["layout_sha256"] for state in admitted_item_states],
        claim_generation,
        allow_reopen,
    )
    await _verify_consumer_references(
        connection,
        tables,
        campaign_id,
        consumer_recipe_id,
        claim_generation,
        admitted_item_states,
    )


async def _verify_consumer_references(
    connection: asyncpg.Connection,
    tables: ClaimTables,
    campaign_id: str,
    consumer_recipe_id: str,
    claim_generation: int,
    admitted_item_states: Sequence[Mapping[str, Any]],
) -> None:
    reference_states = database_record_list(
        await connection.fetch(
            f"""SELECT source_item_id, artifact_sha256, layout_sha256,
                        claim_generation, released_at
                   FROM {tables.consumer_reference}
                  WHERE campaign_id=$1 AND consumer_recipe_id=$2
                  ORDER BY source_item_id LIMIT {MAX_CLAIM_ITEMS + 1}
                  FOR UPDATE;""",
            campaign_id,
            consumer_recipe_id,
        )
    )
    expected_reference_identities = sorted(
        (
            campaign_item_state["source_item_id"],
            campaign_item_state["artifact_sha256"],
            campaign_item_state["layout_sha256"],
            claim_generation,
            None,
        )
        for campaign_item_state in admitted_item_states
    )
    actual_reference_identities = [
        (
            reference_state["source_item_id"],
            reference_state["artifact_sha256"],
            reference_state["layout_sha256"],
            reference_state["claim_generation"],
            reference_state["released_at"],
        )
        for reference_state in reference_states
    ]
    if actual_reference_identities != expected_reference_identities:
        raise RetainedCampaignMismatch("consumer_artifact_refs_mismatch")


async def claim_sealed_retained_campaign(
    connection: asyncpg.Connection,
    *,
    campaign_id: str,
    consumer_recipe_id: str,
) -> SealedRetainedCampaign:
    """Verify one complete campaign and insert all immutable refs atomically."""

    require_digest(campaign_id, "campaign_id")
    require_safe_id(consumer_recipe_id, "consumer_recipe_id")
    tables = claim_tables()
    async with connection.transaction():
        claim_snapshot = await locked_claim_snapshot(
            connection,
            campaign_id,
            tables,
        )
        campaign_sha256 = _verified_claim_digest(claim_snapshot)
        claim_generation, allow_reopen = await _upsert_consumer_claim(
            connection,
            tables,
            campaign_id,
            consumer_recipe_id,
            campaign_sha256,
        )
        await _upsert_consumer_references(
            connection,
            tables,
            campaign_id,
            consumer_recipe_id,
            claim_generation,
            allow_reopen,
            claim_snapshot.admitted_item_states,
        )
        return sealed_campaign_handoff(
            claim_snapshot,
            consumer_claim_generation=claim_generation,
        )


async def heartbeat_retained_campaign_consumer(
    connection: asyncpg.Connection,
    *,
    campaign_id: str,
    consumer_recipe_id: str,
    claimed_campaign_sha256: str,
    consumer_claim_generation: int,
) -> None:
    """Heartbeat one exact active claim without exposing storage internals."""

    require_digest(campaign_id, "campaign_id")
    require_safe_id(consumer_recipe_id, "consumer_recipe_id")
    require_digest(claimed_campaign_sha256, "claimed_campaign_sha256")
    require_positive_int(consumer_claim_generation, "consumer_claim_generation")
    async with connection.transaction():
        await _lock_consumer_campaign(connection, campaign_id)
        update_status = await connection.execute(
            f"""UPDATE {database_table('provider_directory_retained_artifact_consumer')}
                   SET heartbeat_at=now()
                 WHERE campaign_id=$1 AND consumer_recipe_id=$2
                   AND claimed_campaign_sha256=$3 AND claim_generation=$4
                   AND released_at IS NULL;""",
            campaign_id,
            consumer_recipe_id,
            claimed_campaign_sha256,
            consumer_claim_generation,
        )
    if update_status != "UPDATE 1":
        raise RetainedCampaignMismatch("consumer_claim_identity_mismatch")


async def release_retained_campaign_consumer(
    connection: asyncpg.Connection,
    *,
    campaign_id: str,
    consumer_recipe_id: str,
    claimed_campaign_sha256: str,
    consumer_claim_generation: int,
) -> None:
    """Release one consumer's refs without affecting other consumers."""

    require_digest(campaign_id, "campaign_id")
    require_safe_id(consumer_recipe_id, "consumer_recipe_id")
    require_digest(claimed_campaign_sha256, "claimed_campaign_sha256")
    require_positive_int(consumer_claim_generation, "consumer_claim_generation")
    tables = claim_tables()
    async with connection.transaction():
        await _lock_projection_release_dependencies(
            connection,
            campaign_id,
            consumer_recipe_id,
        )
        await _lock_consumer_campaign(connection, campaign_id)
        consumer_state_by_field = database_record(
            await connection.fetchrow(
                f"""SELECT * FROM {tables.consumer}
                      WHERE campaign_id=$1 AND consumer_recipe_id=$2
                      FOR UPDATE;""",
                campaign_id,
                consumer_recipe_id,
            )
        )
        if not consumer_state_by_field:
            raise RetainedArtifactError("consumer_claim_not_found")
        if (
            consumer_state_by_field.get("claimed_campaign_sha256")
            != claimed_campaign_sha256
            or consumer_state_by_field.get("claim_generation")
            != consumer_claim_generation
        ):
            raise RetainedCampaignMismatch("consumer_claim_identity_mismatch")
        if consumer_state_by_field.get("released_at") is not None:
            return
        await _release_consumer_rows(
            connection,
            tables,
            campaign_id,
            consumer_recipe_id,
            consumer_claim_generation,
        )


async def _has_projection_release_relations(
    connection: asyncpg.Connection,
) -> bool:
    """Return whether the optional projection owner relations exist."""

    schema = database_schema()
    return bool(
        await connection.fetchval(
            "SELECT to_regclass($1) IS NOT NULL "
            "AND to_regclass($2) IS NOT NULL;",
            f"{schema}.provider_directory_projection_admission",
            f"{schema}.provider_directory_projection_recipe",
        )
    )


async def _projection_release_candidate_records(
    connection: asyncpg.Connection,
    admission_table: str,
    campaign_id: str,
    consumer_recipe_id: str,
) -> list[dict[str, Any]]:
    return database_record_list(
        await connection.fetch(
            f"""SELECT admission_id, recipe_id
                   FROM {admission_table}
                  WHERE retained_campaign_id=$1
                    AND retained_consumer_recipe_id=$2
                  ORDER BY recipe_id NULLS FIRST, admission_id;""",
            campaign_id,
            consumer_recipe_id,
        )
    )


async def _locked_projection_recipe_records(
    connection: asyncpg.Connection,
    recipe_table: str,
    recipe_ids: list[str],
) -> list[dict[str, Any]]:
    if not recipe_ids:
        return []
    return database_record_list(
        await connection.fetch(
            f"""SELECT recipe_id, status
                   FROM {recipe_table}
                  WHERE recipe_id=ANY($1::text[])
                  ORDER BY recipe_id FOR SHARE;""",
            recipe_ids,
        )
    )


async def _locked_projection_admission_records(
    connection: asyncpg.Connection,
    admission_table: str,
    campaign_id: str,
    consumer_recipe_id: str,
) -> list[dict[str, Any]]:
    return database_record_list(
        await connection.fetch(
            f"""SELECT admission_id, recipe_id, status
                   FROM {admission_table}
                  WHERE retained_campaign_id=$1
                    AND retained_consumer_recipe_id=$2
                  ORDER BY recipe_id NULLS FIRST, admission_id FOR SHARE;""",
            campaign_id,
            consumer_recipe_id,
        )
    )


def _is_projection_release_live(
    candidate_records: list[dict[str, Any]],
    recipe_records: list[dict[str, Any]],
    admission_records: list[dict[str, Any]],
    recipe_ids: list[str],
) -> bool:
    candidate_identity_list = [
        (candidate.get("admission_id"), candidate.get("recipe_id"))
        for candidate in candidate_records
    ]
    locked_identity_list = [
        (admission.get("admission_id"), admission.get("recipe_id"))
        for admission in admission_records
    ]
    return bool(
        locked_identity_list != candidate_identity_list
        or {recipe.get("recipe_id") for recipe in recipe_records}
        != set(recipe_ids)
        or any(
            admission.get("status") != "released"
            for admission in admission_records
        )
        or any(
            recipe.get("status") in {"building", "proof_ready"}
            for recipe in recipe_records
        )
    )


async def _lock_projection_release_dependencies(
    connection: asyncpg.Connection,
    campaign_id: str,
    consumer_recipe_id: str,
) -> None:
    """Lock projection owners before their retained parent is released."""

    if not await _has_projection_release_relations(connection):
        return
    admission_table = database_table("provider_directory_projection_admission")
    candidate_records = await _projection_release_candidate_records(
        connection, admission_table, campaign_id, consumer_recipe_id
    )
    if not candidate_records:
        return
    recipe_ids = sorted({
        str(candidate["recipe_id"])
        for candidate in candidate_records
        if candidate.get("recipe_id") is not None
    })
    recipe_records = await _locked_projection_recipe_records(
        connection,
        database_table("provider_directory_projection_recipe"),
        recipe_ids,
    )
    admission_records = await _locked_projection_admission_records(
        connection, admission_table, campaign_id, consumer_recipe_id
    )
    if _is_projection_release_live(
        candidate_records,
        recipe_records,
        admission_records,
        recipe_ids,
    ):
        raise RetainedArtifactError(
            "provider_directory_projection_retained_parent_live"
        )


async def _lock_consumer_campaign(
    connection: asyncpg.Connection,
    campaign_id: str,
) -> None:
    campaign_exists = await connection.fetchval(
        f"""SELECT TRUE
               FROM {database_table('provider_directory_retained_artifact_campaign')}
              WHERE campaign_id=$1 FOR SHARE;""",
        campaign_id,
    )
    if campaign_exists is not True:
        raise RetainedArtifactError("consumer_campaign_not_found")


async def _release_consumer_rows(
    connection: asyncpg.Connection,
    tables: ClaimTables,
    campaign_id: str,
    consumer_recipe_id: str,
    consumer_claim_generation: int,
) -> None:
    await connection.execute(
        f"""UPDATE {tables.consumer_reference} SET released_at=now()
              WHERE campaign_id=$1 AND consumer_recipe_id=$2
                AND claim_generation=$3 AND released_at IS NULL;""",
        campaign_id,
        consumer_recipe_id,
        consumer_claim_generation,
    )
    await connection.execute(
        f"""UPDATE {tables.consumer} SET released_at=now()
              WHERE campaign_id=$1 AND consumer_recipe_id=$2
                AND claim_generation=$3;""",
        campaign_id,
        consumer_recipe_id,
        consumer_claim_generation,
    )
