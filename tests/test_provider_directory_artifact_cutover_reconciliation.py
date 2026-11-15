# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import contextlib

import pytest

from tests.test_provider_directory_artifact_cutover import (
    _assert_artifact_relation_values,
    _create_artifact_bundle_relations,
    _keep_stage_indexes,
    _prepared_bundle_stage,
    importer,
)
from tests.test_provider_directory_dataset_artifact_db import (
    _dataset_database,
    _insert_validated_shared_dataset,
)


async def _reconciliation_bundle(database, schema):
    await _insert_validated_shared_dataset(database, schema)
    dataset_fence = await importer._resolve_provider_directory_artifact_datasets(
        ["source_primary"],
        should_select_validated_candidates=True,
    )
    await _create_artifact_bundle_relations(database, schema)
    await database.status(
        f"CREATE TABLE {schema}.address_alias_state_v1 "
        "(singleton boolean PRIMARY KEY, schema_version smallint, active_ruleset_version smallint, generation bigint)"
    )
    await database.status(
        f"INSERT INTO {schema}.address_alias_state_v1 VALUES (true,:schema_version,:ruleset,0)",
        schema_version=importer.address_alias_sql.ADDRESS_ALIAS_SCHEMA_VERSION,
        ruleset=importer.address_alias_sql.ADDRESS_ALIAS_RULESET_VERSION,
    )
    artifact_stages = (
        await _prepared_bundle_stage(
            schema,
            "artifact_stage_a",
            "artifact_target_a",
            _keep_stage_indexes,
        ),
        await _prepared_bundle_stage(
            schema,
            "artifact_stage_b",
            "artifact_target_b",
            _keep_stage_indexes,
        ),
    )
    return dataset_fence, artifact_stages


@contextlib.contextmanager
def _active_dataset_fence(dataset_fence):
    fence_token = importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.set(dataset_fence)
    try:
        yield
    finally:
        importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.reset(fence_token)


async def _assert_promoted_dataset_pointers(database, schema):
    pointer_rows = await database.all(
        f"SELECT dataset_id, status, is_current "
        f"FROM {schema}.provider_directory_endpoint_dataset "
        "WHERE dataset_id IN ('dataset_shared', 'dataset_candidate') "
        "ORDER BY dataset_id;"
    )
    assert [tuple(pointer_row) for pointer_row in pointer_rows] == [
        ("dataset_candidate", importer.ENDPOINT_DATASET_PUBLISHED, True),
        ("dataset_shared", importer.ENDPOINT_DATASET_SUPERSEDED, False),
    ]


@pytest.mark.asyncio
async def test_real_postgres_artifact_bundle_accepts_timeout_after_commit(monkeypatch):
    """A lost commit acknowledgement verifies exact logical revisions and dataset pointers."""

    async with _dataset_database(monkeypatch) as (database, schema):
        dataset_fence, artifact_stages = await _reconciliation_bundle(
            database,
            schema,
        )
        promote_transaction = importer._promote_provider_directory_artifact_bundle_transaction

        async def commit_then_lose_acknowledgement(
            prepared_artifact_stages,
            **transaction_options,
        ):
            await promote_transaction(
                prepared_artifact_stages,
                **transaction_options,
            )
            raise TimeoutError

        monkeypatch.setattr(
            importer,
            "_promote_provider_directory_artifact_bundle_transaction",
            commit_then_lose_acknowledgement,
        )

        with _active_dataset_fence(dataset_fence):
            await importer._promote_provider_directory_artifact_bundle(artifact_stages)

        await _assert_artifact_relation_values(
            database,
            schema,
            {
                "artifact_target_a": "new-a",
                "artifact_target_b": "new-b",
            },
        )
        for stage_table in ("artifact_stage_a", "artifact_stage_b"):
            assert (
                await database.scalar(
                    "SELECT to_regclass(:relation_name);",
                    relation_name=f"{schema}.{stage_table}",
                )
                is None
            )
        await _assert_promoted_dataset_pointers(database, schema)


@pytest.mark.asyncio
async def test_real_postgres_timeout_reconciliation_rejects_relations_only_commit(
    monkeypatch,
):
    """Matching logical relation revisions are insufficient when the pointer stayed old."""
    async with _dataset_database(monkeypatch) as (database, schema):
        dataset_fence, artifact_stages = await _reconciliation_bundle(
            database,
            schema,
        )
        promote_transaction = importer._promote_provider_directory_artifact_bundle_transaction

        async def commit_relations_only_then_timeout(
            prepared_artifact_stages,
            **transaction_options,
        ):
            transaction_fence_token = importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.set(None)
            try:
                await promote_transaction(
                    prepared_artifact_stages,
                    **transaction_options,
                )
            finally:
                importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.reset(transaction_fence_token)
            raise TimeoutError

        monkeypatch.setattr(
            importer,
            "_promote_provider_directory_artifact_bundle_transaction",
            commit_relations_only_then_timeout,
        )
        with _active_dataset_fence(dataset_fence):
            with pytest.raises(TimeoutError):
                await importer._promote_provider_directory_artifact_bundle(artifact_stages)

        await _assert_artifact_relation_values(
            database,
            schema,
            {
                "artifact_target_a": "new-a",
                "artifact_target_b": "new-b",
            },
        )
        current_dataset_id = await database.scalar(
            f"SELECT dataset_id "
            f"FROM {schema}.provider_directory_endpoint_dataset "
            "WHERE endpoint_id = 'endpoint_shared' "
            "AND status = :published_status AND is_current = true;",
            published_status=importer.ENDPOINT_DATASET_PUBLISHED,
        )
        assert current_dataset_id == "dataset_shared"


@pytest.mark.asyncio
async def test_real_postgres_lost_ack_revision_is_portable_and_later_revision_refuses(monkeypatch):
    """A complete copied target keeps its revision; a different publication does not satisfy it."""
    async with _dataset_database(monkeypatch) as (database, schema):
        fence, stages = await _reconciliation_bundle(database, schema)
        identities = await importer._capture_provider_directory_artifact_promotion_identities(stages)
        with _active_dataset_fence(fence):
            await importer._promote_provider_directory_artifact_bundle_transaction(
                stages, promotion_identities=identities
            )
        for stage in stages:
            old_oid = await database.scalar(
                "SELECT to_regclass(:target)::oid", target=f"{schema}.{stage.target_relation}"
            )
            await database.status(f"CREATE TABLE {schema}.copied_target AS TABLE {schema}.{stage.target_relation}")
            await database.status(f"DROP TABLE {schema}.{stage.target_relation} RESTRICT")
            await database.status(f"ALTER TABLE {schema}.copied_target RENAME TO {stage.target_relation}")
            assert (
                await database.scalar("SELECT to_regclass(:target)::oid", target=f"{schema}.{stage.target_relation}")
                != old_oid
            )
        assert await importer._is_provider_directory_artifact_promotion_committed(identities, fence)
        await _assert_artifact_relation_values(
            database, schema, {"artifact_target_a": "new-a", "artifact_target_b": "new-b"}
        )
        await database.status(
            f"UPDATE {schema}.address_alias_artifact_state_v1 SET updated_at=updated_at+interval '1 microsecond'"
        )
        assert not await importer._is_provider_directory_artifact_promotion_committed(identities, fence)
        await _assert_promoted_dataset_pointers(database, schema)


@pytest.mark.asyncio
async def test_real_postgres_rolled_back_post_stamp_cannot_resolve_commit(monkeypatch):
    """A stamp observed before COMMIT is rejected when that transaction actually rolls back."""
    async with _dataset_database(monkeypatch) as (database, schema):
        fence, stages = await _reconciliation_bundle(database, schema)
        identities = await importer._capture_provider_directory_artifact_promotion_identities(stages)
        with _active_dataset_fence(fence):
            with pytest.raises(RuntimeError, match="late rollback"):
                async with database.transaction():
                    await importer._promote_provider_directory_artifact_bundle_transaction(
                        stages, promotion_identities=identities
                    )
                    assert all(identity.publication_revision is not None for identity in identities)
                    raise RuntimeError("late rollback")
        assert not await importer._is_provider_directory_artifact_promotion_committed(identities, fence)
        await _assert_artifact_relation_values(
            database,
            schema,
            {
                "artifact_target_a": "old-a",
                "artifact_target_b": "old-b",
                "artifact_stage_a": "new-a",
                "artifact_stage_b": "new-b",
            },
        )
        assert await database.scalar(f"SELECT count(*) FROM {schema}.address_alias_artifact_state_v1") == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("alias_generation", [None, 0])
async def test_real_postgres_single_stage_lost_ack_uses_written_future_revision(monkeypatch, alias_generation):
    """Alias-free cutovers advance even a future timestamp and recover only their written stamp."""
    async with _dataset_database(monkeypatch) as (database, schema):
        _fence, stages = await _reconciliation_bundle(database, schema)
        stage = stages[0]
        await database.status(
            f"INSERT INTO {schema}.address_alias_artifact_state_v1 VALUES (:target,0,'2100-01-01'::timestamptz)",
            target=stage.target_relation,
        )
        stage = await _prepared_bundle_stage(
            schema, stage.stage_table, stage.target_relation, stage.rename_stage_indexes
        )
        stage = importer.replace(
            stage, build_fence=importer.replace(stage.build_fence, alias_generation=alias_generation)
        )
        transaction = importer._promote_provider_directory_artifact_stage_transaction

        async def commit_then_timeout(*args, **kwargs):
            await transaction(*args, **kwargs)
            raise TimeoutError

        monkeypatch.setattr(importer, "_promote_provider_directory_artifact_stage_transaction", commit_then_timeout)
        await importer._promote_provider_directory_artifact_stage(
            schema,
            stage.stage_table,
            stage.target_relation,
            stage.rename_stage_indexes,
            stage.build_fence,
        )
        assert await database.scalar(
            f"SELECT generation=0 AND updated_at>'2100-01-01'::timestamptz FROM {schema}.address_alias_artifact_state_v1 WHERE artifact_name=:target",
            target=stage.target_relation,
        )
        await _assert_artifact_relation_values(database, schema, {stage.target_relation: "new-a"})


@pytest.mark.asyncio
async def test_uncommitted_writer_readback_refuses(monkeypatch):
    """A writer's own uncommitted stamp is not committed-publication evidence."""
    async with _dataset_database(monkeypatch) as (database, schema):
        fence, stages = await _reconciliation_bundle(database, schema)
        identities = await importer._capture_provider_directory_artifact_promotion_identities(stages)
        readback_by_component = {}
        with _active_dataset_fence(fence):
            with pytest.raises(RuntimeError, match="outer rollback"):
                async with database.transaction():
                    await importer._promote_provider_directory_artifact_bundle_transaction(
                        stages, promotion_identities=identities
                    )
                    assert all(identity.publication_revision is not None for identity in identities)
                    await database.status("SET TRANSACTION READ ONLY")
                    assert await database.scalar("SHOW transaction_read_only") == "on"
                    readback_by_component[
                        "relations"
                    ] = await importer._is_provider_directory_artifact_promotion_committed(identities, fence)
                    readback_by_component["bundle"] = await importer._is_artifact_bundle_promotion_committed(
                        stages, identities, fence, None
                    )
                    readback_by_component["initial"] = await importer.profile_initial.resolve_completion(
                        importer, stages, fence, identities
                    )
                    raise RuntimeError("outer rollback")
        await _assert_artifact_relation_values(
            database,
            schema,
            {
                "artifact_target_a": "old-a",
                "artifact_target_b": "old-b",
                "artifact_stage_a": "new-a",
                "artifact_stage_b": "new-b",
            },
        )
        assert await database.scalar(f"SELECT count(*) FROM {schema}.address_alias_artifact_state_v1") == 0
        assert readback_by_component == {"relations": False, "bundle": False, "initial": False}
