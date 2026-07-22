# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL proofs for projection child retained-read leases."""

from __future__ import annotations

import asyncio

import asyncpg
import pytest
from sqlalchemy.exc import DBAPIError

from process.provider_directory_physical_projection import (
    claim_projection_shard,
    projection_admission_consumer_id,
)
from process.provider_directory_projection_child_read import (
    assert_verified_projection_child_read_lease,
    claim_projection_child_read_lease,
    heartbeat_projection_child_read_lease,
    release_projection_child_read_lease,
    verify_projection_child_read_lease,
)
from process.provider_directory_projection_db import set_local_projection_action
from process.provider_directory_projection_types import (
    ProviderDirectoryProjectionBusy,
    ProviderDirectoryProjectionError,
    ProviderDirectoryProjectionLeaseLost,
)
from process.provider_directory_retained_consumer_claim_store import (
    release_retained_campaign_consumer,
)
from process.provider_directory_retained_artifact_contract import (
    RetainedArtifactError,
)
from tests.provider_directory_projection_foundation_postgres_support import (
    projection_foundation_postgres,
)
from tests.test_provider_directory_projection_foundation_postgres import (
    _digest,
    _fixture_projection_recipe,
    _range_subset_fixture,
    _sealed_retained_binding,
    _sealed_shared_artifact_bindings,
    _two_range_artifact,
)
from tests.provider_directory_retained_core_postgres_support import campaign_item
from tests.test_provider_directory_projection_child_read import (
    _claim_projection_children,
    _claimed_child_context,
    _complete_claimed_shard,
    _full_artifact_fixture,
)


def _database_arguments(postgres):
    """Return the explicit disposable projection database coordinates."""

    return {"database": postgres.database, "schema": postgres.schema}


@pytest.mark.asyncio
async def test_child_read_migration_upgrades_and_downgrades_empty(monkeypatch) -> None:
    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()
        await postgres.upgrade_child_read()
        child_oid = await postgres.database.scalar(
            "SELECT to_regclass(:relation)::oid::bigint;",
            relation=(
                f"{postgres.schema}."
                "provider_directory_projection_child_read_lease"
            ),
        )
        assert int(child_oid or 0) > 0
        assert await postgres.database.scalar(
            "SELECT to_regclass(:relation)::oid::bigint;",
            relation=(
                f"{postgres.schema}."
                "pd_projection_admission_parent_release_idx"
            ),
        ) is not None
        await postgres.downgrade_child_read()
        assert await postgres.database.scalar(
            "SELECT to_regclass(:relation);",
            relation=(
                f"{postgres.schema}."
                "provider_directory_projection_child_read_lease"
            ),
        ) is None


@pytest.mark.asyncio
async def test_child_read_claims_exact_full_artifact(monkeypatch) -> None:
    """Bind a child to full artifact coordinates without a range ordinal."""

    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()
        await postgres.upgrade_child_read()
        artifact_label = "child-full-artifact"

        def consumer_id(retained_binding):
            _artifact, recipe, admission_block, _shard = _full_artifact_fixture(
                retained_binding,
                artifact_label,
            )
            return projection_admission_consumer_id(recipe, (admission_block,))

        retained_binding = await _sealed_retained_binding(
            postgres,
            campaign_label="child-full-artifact",
            artifact_label=artifact_label,
            consumer_recipe_id=consumer_id,
        )
        produced_artifact, recipe, admission_block, shard = _full_artifact_fixture(
            retained_binding,
            artifact_label,
        )
        _shard_claims, child_leases = await _claim_projection_children(
            postgres,
            recipe,
            (admission_block,),
            (shard,),
            retained_binding.consumer_claim_generation,
        )
        (child_lease,) = child_leases
        assert child_lease.retained_range_ordinal is None
        assert child_lease.raw_byte_start == 0
        assert child_lease.expected_byte_count == (
            produced_artifact.artifact_byte_count
        )
        assert child_lease.expected_record_count == (
            produced_artifact.artifact_record_count
        )
        assert child_lease.input_sha256 == produced_artifact.manifest_sha256
        assert child_lease.expected_payload_sha256 == (
            produced_artifact.artifact_sha256
        )
        await release_projection_child_read_lease(
            child_lease,
            **_database_arguments(postgres),
        )


async def _assert_busy_and_unfenced_mutation(postgres, shard_claim, child) -> None:
    with pytest.raises(
        ProviderDirectoryProjectionBusy,
        match="provider_directory_projection_child_read_busy",
    ):
        await claim_projection_child_read_lease(
            shard_claim,
            **_database_arguments(postgres),
        )
    with pytest.raises(
        DBAPIError,
        match="provider_directory_projection_child_read_unfenced",
    ):
        async with postgres.database.transaction():
            await postgres.database.scalar(
                "SELECT set_config("
                "'healthporta.provider_directory_projection_child_action', "
                "'heartbeat', true);"
            )
            await postgres.database.status(
                f"""
                UPDATE "{postgres.schema}".
                       provider_directory_projection_child_read_lease
                   SET lease_expires_at = lease_expires_at + interval '1 second',
                       lease_heartbeat_at = now(), updated_at = now()
                 WHERE child_lease_token = :child_lease_token;
                """,
                child_lease_token=child.child_lease_token,
            )


async def _assert_parent_release_fences(postgres, retained, child) -> None:
    with pytest.raises(
        asyncpg.PostgresError,
        match="provider_directory_projection_retained_parent_live",
    ):
        await postgres.retained_connection.execute(
            f"""
            UPDATE "{postgres.schema}".
                   provider_directory_retained_artifact_consumer_reference
               SET released_at = now()
             WHERE campaign_id = $1 AND consumer_recipe_id = $2;
            """,
            retained.campaign_id,
            child.retained_consumer_recipe_id,
        )
    parent_release = release_retained_campaign_consumer(
        postgres.retained_connection,
        campaign_id=retained.campaign_id,
        consumer_recipe_id=child.retained_consumer_recipe_id,
        claimed_campaign_sha256=retained.campaign_sha256,
        consumer_claim_generation=retained.consumer_claim_generation,
    )
    heartbeat = heartbeat_projection_child_read_lease(
        child,
        **_database_arguments(postgres),
    )
    heartbeat_result, parent_result = await asyncio.wait_for(
        asyncio.gather(heartbeat, parent_release, return_exceptions=True),
        timeout=5,
    )
    assert heartbeat_result is None
    assert isinstance(parent_result, RetainedArtifactError)
    assert str(parent_result) == "provider_directory_projection_retained_parent_live"


async def _assert_verification_mismatches(postgres, child) -> None:
    verification_arguments_by_name = {
        "byte_count": child.expected_byte_count,
        "record_count": child.expected_record_count,
        "input_sha256": child.input_sha256,
        "payload_sha256": child.expected_payload_sha256,
    }
    mismatches = (
        {
            **verification_arguments_by_name,
            "byte_count": child.expected_byte_count + 1,
        },
        {
            **verification_arguments_by_name,
            "record_count": child.expected_record_count + 1,
        },
        {
            **verification_arguments_by_name,
            "input_sha256": _digest("wrong-canonical-input"),
        },
        {
            **verification_arguments_by_name,
            "payload_sha256": _digest("wrong-raw-input"),
        },
    )
    for mismatch in mismatches:
        with pytest.raises(
            ProviderDirectoryProjectionError,
            match="provider_directory_projection_child_read_verification_mismatch",
        ):
            await verify_projection_child_read_lease(
                child,
                **_database_arguments(postgres),
                **mismatch,
            )


async def _reclaim_and_reject_stale_child(postgres, shard_claim, child):
    await release_projection_child_read_lease(child, **_database_arguments(postgres))
    reclaimed = await claim_projection_child_read_lease(
        shard_claim,
        **_database_arguments(postgres),
    )
    assert reclaimed.child_generation == child.child_generation + 1
    assert reclaimed.child_lease_token != child.child_lease_token
    stale_operations = (
        heartbeat_projection_child_read_lease(child, **_database_arguments(postgres)),
        verify_projection_child_read_lease(
            child,
            byte_count=child.expected_byte_count,
            record_count=child.expected_record_count,
            input_sha256=child.input_sha256,
            payload_sha256=child.expected_payload_sha256,
            **_database_arguments(postgres),
        ),
        release_projection_child_read_lease(child, **_database_arguments(postgres)),
    )
    for stale_operation in stale_operations:
        with pytest.raises(
            ProviderDirectoryProjectionLeaseLost,
            match="provider_directory_projection_child_read_lease_lost",
        ):
            await stale_operation
    return reclaimed


async def _verify_child_and_fence_owner(postgres, shard_claim, child) -> None:
    with pytest.raises(
        DBAPIError,
        match="provider_directory_projection_child_read_verification_required",
    ):
        await _complete_claimed_shard(postgres, shard_claim)
    await verify_projection_child_read_lease(
        child,
        byte_count=child.expected_byte_count,
        record_count=child.expected_record_count,
        input_sha256=f" {child.input_sha256.upper()} ",
        payload_sha256=f" {child.expected_payload_sha256.upper()} ",
        **_database_arguments(postgres),
    )
    await assert_verified_projection_child_read_lease(
        child,
        **_database_arguments(postgres),
    )
    await _complete_claimed_shard(postgres, shard_claim)
    with pytest.raises(
        DBAPIError,
        match="provider_directory_projection_child_owner_live",
    ):
        async with postgres.database.transaction():
            await set_local_projection_action(
                postgres.database,
                "fail",
                recipe_id=shard_claim.recipe_lease.recipe.recipe_id,
                recipe_attempt=shard_claim.recipe_lease.attempt,
                recipe_lease_token=shard_claim.recipe_lease.lease_token,
            )
            await postgres.database.status(
                f"""
                UPDATE "{postgres.schema}".provider_directory_projection_recipe
                   SET status = 'failed', lease_token = NULL,
                       lease_expires_at = NULL, lease_heartbeat_at = NULL,
                       updated_at = now()
                 WHERE recipe_id = :recipe_id;
                """,
                recipe_id=shard_claim.recipe_lease.recipe.recipe_id,
            )


async def _release_and_assert_child_state(
    postgres,
    retained,
    shard_claim,
    child,
) -> None:
    await release_projection_child_read_lease(child, **_database_arguments(postgres))
    stored_child = await postgres.database.first(
        f"""
        SELECT status, verified_input_sha256, verified_payload_sha256
          FROM "{postgres.schema}".provider_directory_projection_child_read_lease
         WHERE recipe_id = :recipe_id AND partition_id = :partition_id;
        """,
        recipe_id=shard_claim.recipe_lease.recipe.recipe_id,
        partition_id=shard_claim.shard.partition_id,
    )
    assert tuple(stored_child) == (
        "released",
        child.input_sha256,
        child.expected_payload_sha256,
    )
    assert await postgres.database.scalar(
        f"""
        SELECT count(*)
          FROM "{postgres.schema}".
               provider_directory_retained_artifact_consumer_reference
         WHERE campaign_id = :campaign_id AND released_at IS NOT NULL;
        """,
        campaign_id=retained.campaign_id,
    ) == 0


@pytest.mark.asyncio
async def test_child_read_lifecycle_fences_stale_and_parent_operations(
    monkeypatch,
) -> None:
    """Fence stale tokens, incomplete proof, owner failure, and parent release."""

    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()
        await postgres.upgrade_child_read()
        retained, admission_block, shard_claim, first_child = (
            await _claimed_child_context(postgres, "child-lifecycle")
        )
        assert first_child.retained_range_ordinal == 0
        assert first_child.block_id == admission_block.block.block_id
        assert first_child.expected_byte_count == admission_block.block.payload_bytes
        assert first_child.expected_record_count == admission_block.block.record_count
        await _assert_busy_and_unfenced_mutation(postgres, shard_claim, first_child)
        await _assert_parent_release_fences(postgres, retained, first_child)
        await _assert_verification_mismatches(postgres, first_child)
        second_child = await _reclaim_and_reject_stale_child(
            postgres,
            shard_claim,
            first_child,
        )
        await _verify_child_and_fence_owner(postgres, shard_claim, second_child)
        await _release_and_assert_child_state(
            postgres,
            retained,
            shard_claim,
            second_child,
        )


async def _claimed_range_sibling_children(postgres):
    produced_artifact = _two_range_artifact(
        "child-sibling-ranges",
        campaign_item("child-sibling-ranges").artifact_kind,
    )

    def consumer_recipe_id(bindings, shared_artifact):
        recipe, admission_blocks, _shards = _range_subset_fixture(
            bindings[0], shared_artifact, (0, 1)
        )
        return projection_admission_consumer_id(recipe, admission_blocks)

    bindings, produced_artifact = await _sealed_shared_artifact_bindings(
        postgres,
        campaign_label="child-sibling-ranges",
        item_count=1,
        consumer_recipe_id=consumer_recipe_id,
        produced_artifact=produced_artifact,
    )
    recipe, admission_blocks, shards = _range_subset_fixture(
        bindings[0], produced_artifact, (0, 1)
    )
    return await _claim_projection_children(
        postgres,
        recipe,
        admission_blocks,
        shards,
        bindings[0].consumer_claim_generation,
    )


def _assert_range_sibling_identity(first_child, second_child) -> None:
    assert first_child.retained_artifact_sha256 == (
        second_child.retained_artifact_sha256
    )
    assert first_child.retained_layout_sha256 == second_child.retained_layout_sha256
    assert {
        first_child.retained_range_ordinal,
        second_child.retained_range_ordinal,
    } == {0, 1}
    assert first_child.raw_byte_start != second_child.raw_byte_start
    assert first_child.expected_payload_sha256 != second_child.expected_payload_sha256


async def _retry_first_sibling(postgres, shard_claim, first_child, second_child):
    await release_projection_child_read_lease(
        first_child,
        **_database_arguments(postgres),
    )
    retried_first = await claim_projection_child_read_lease(
        shard_claim,
        **_database_arguments(postgres),
    )
    assert retried_first.child_generation == 2
    sibling_row = await postgres.database.first(
        f"""
        SELECT status, child_generation, child_lease_token
          FROM "{postgres.schema}".provider_directory_projection_child_read_lease
         WHERE recipe_id = :recipe_id AND partition_id = :partition_id;
        """,
        recipe_id=second_child.shard_claim.recipe_lease.recipe.recipe_id,
        partition_id=second_child.shard_claim.shard.partition_id,
    )
    assert tuple(sibling_row) == (
        "active",
        second_child.child_generation,
        second_child.child_lease_token,
    )
    return retried_first


async def _assert_separate_artifact_isolation(postgres, retried_first, sibling) -> None:
    separate_context = await _claimed_child_context(
        postgres,
        "child-separate-artifact",
    )
    separate_child = separate_context[-1]
    assert separate_child.retained_artifact_sha256 not in {
        retried_first.retained_artifact_sha256,
        sibling.retained_artifact_sha256,
    }
    for child_lease in (retried_first, sibling, separate_child):
        await release_projection_child_read_lease(
            child_lease,
            **_database_arguments(postgres),
        )


@pytest.mark.asyncio
async def test_child_read_siblings_isolate_ranges_and_artifacts(monkeypatch) -> None:
    """Keep range siblings and unrelated artifacts independently retryable."""

    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()
        await postgres.upgrade_child_read()
        shard_claims, child_leases = await _claimed_range_sibling_children(postgres)
        first_child, second_child = child_leases
        _assert_range_sibling_identity(first_child, second_child)
        retried_first = await _retry_first_sibling(
            postgres,
            shard_claims[0],
            first_child,
            second_child,
        )
        await _assert_separate_artifact_isolation(
            postgres,
            retried_first,
            second_child,
        )
