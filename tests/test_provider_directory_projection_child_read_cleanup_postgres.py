# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Adversarial PostgreSQL cleanup proofs for completed projection children."""

from __future__ import annotations

import pytest

import process.provider_directory_projection_child_read as child_read
from process.provider_directory_physical_projection import claim_projection_shard
from process.provider_directory_projection_contract import projection_proof_shard
from process.provider_directory_projection_db import set_local_projection_action
from process.provider_directory_projection_workset import complete_projection_shard
from tests.provider_directory_projection_foundation_postgres_support import (
    projection_foundation_postgres,
)
from tests.test_provider_directory_physical_projection import _rows
from tests.test_provider_directory_projection_foundation_postgres import (
    _registered_projection_context,
)


async def _expire_recipe_after_shard_commit(postgres, lease) -> None:
    """Move one still-owned recipe just past expiry through its guarded action."""

    database = postgres.database
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
            UPDATE "{postgres.schema}".provider_directory_projection_recipe
               SET lease_expires_at = clock_timestamp()
                                      + interval '500 milliseconds',
                   lease_heartbeat_at = clock_timestamp(),
                   updated_at = clock_timestamp()
             WHERE recipe_id = :recipe_id AND attempt = :attempt
               AND status = 'building' AND lease_token = :lease_token;
            """,
            recipe_id=lease.recipe.recipe_id,
            attempt=lease.attempt,
            lease_token=lease.lease_token,
        )
    assert updated_count == 1
    await database.scalar("SELECT pg_sleep(0.75);")
    assert await database.scalar(
        f"""
        SELECT lease_expires_at <= clock_timestamp()
          FROM "{postgres.schema}".provider_directory_projection_recipe
         WHERE recipe_id = :recipe_id;
        """,
        recipe_id=lease.recipe.recipe_id,
    ) is True


async def _verify_and_complete_child(postgres, claim, child_lease, proof) -> None:
    database_options_map = {
        "database": postgres.database,
        "schema": postgres.schema,
    }
    await child_read.verify_projection_child_read_lease(
        child_lease,
        byte_count=child_lease.expected_byte_count,
        record_count=child_lease.expected_record_count,
        input_sha256=child_lease.input_sha256,
        payload_sha256=child_lease.expected_payload_sha256,
        **database_options_map,
    )
    await complete_projection_shard(
        claim,
        proof,
        child_lease=child_lease,
        **database_options_map,
    )


async def _registered_cleanup_case(postgres):
    """Build one claimed shard and its exact completion proof."""

    await postgres.upgrade()
    await postgres.upgrade_child_read()
    lease, retained, _block, shard, admission_id = (
        await _registered_projection_context(
            postgres,
            "completed-child-cleanup-expiry",
        )
    )
    claim = await claim_projection_shard(
        lease,
        admission_id=admission_id,
        database=postgres.database,
        schema=postgres.schema,
    )
    assert claim is not None and claim.shard == shard
    proof = projection_proof_shard(
        _rows(retained.family, "completed-child-cleanup-expiry"),
        recipe=lease.recipe,
        attempt=lease.attempt,
        partition_ordinal=claim.shard.partition_ordinal,
        resource_type=claim.shard.resource_type,
        input_sha256=claim.shard.input_sha256,
        partition_id=claim.shard.partition_id,
        partition_attempt=claim.partition_attempt,
    )
    return lease, claim, proof


async def _assert_completed_shard(postgres, lease, claim) -> None:
    assert await postgres.database.scalar(
        f"""
        SELECT status = 'complete'
          FROM "{postgres.schema}".provider_directory_projection_proof_shard
         WHERE recipe_id = :recipe_id AND attempt = :attempt
           AND partition_id = :partition_id;
        """,
        recipe_id=lease.recipe.recipe_id,
        attempt=lease.attempt,
        partition_id=claim.shard.partition_id,
    ) is True


async def _cleanup_lifecycle_state(postgres, lease, claim):
    return await postgres.database.first(
        f"""
        SELECT child.status, child.released_at IS NOT NULL,
               shard.status, recipe.status,
               recipe.lease_expires_at <= clock_timestamp()
          FROM "{postgres.schema}".
               provider_directory_projection_child_read_lease AS child
          JOIN "{postgres.schema}".
               provider_directory_projection_proof_shard AS shard
            ON shard.recipe_id = child.recipe_id
           AND shard.attempt = child.recipe_attempt
           AND shard.partition_id = child.partition_id
          JOIN "{postgres.schema}".
               provider_directory_projection_recipe AS recipe
            ON recipe.recipe_id = child.recipe_id
         WHERE child.recipe_id = :recipe_id
           AND child.partition_id = :partition_id;
        """,
        recipe_id=lease.recipe.recipe_id,
        partition_id=claim.shard.partition_id,
    )


@pytest.mark.asyncio
async def test_completed_child_cleanup_survives_recipe_lease_expiry(
    monkeypatch,
) -> None:
    """A post-commit recipe expiry cannot strand the verified child lease."""

    async with projection_foundation_postgres(monkeypatch) as postgres:
        lease, claim, proof = await _registered_cleanup_case(postgres)
        async with child_read.claimed_projection_child_read_lease(
            claim,
            database=postgres.database,
            schema=postgres.schema,
        ) as child_lease:
            await _verify_and_complete_child(postgres, claim, child_lease, proof)
            await _assert_completed_shard(postgres, lease, claim)
            await _expire_recipe_after_shard_commit(postgres, lease)

        lifecycle_state = await _cleanup_lifecycle_state(postgres, lease, claim)
        assert tuple(lifecycle_state) == (
            "released",
            True,
            "complete",
            "building",
            True,
        )
