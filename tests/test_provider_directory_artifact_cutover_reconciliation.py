# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import pytest

from tests.test_provider_directory_artifact_cutover import (
    _assert_artifact_relation_values,
    _create_artifact_bundle_relations,
    _dataset_database,
    _keep_stage_indexes,
    _prepared_bundle_stage,
    importer,
)


@pytest.mark.asyncio
async def test_real_postgres_artifact_bundle_accepts_timeout_after_commit(monkeypatch):
    """A lost commit acknowledgement is reconciled from exact relation OIDs."""

    async with _dataset_database(monkeypatch) as (database, schema):
        await _create_artifact_bundle_relations(database, schema)
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
        promote_transaction = (
            importer._promote_provider_directory_artifact_bundle_transaction
        )

        async def commit_then_lose_acknowledgement(prepared_artifact_stages):
            await promote_transaction(prepared_artifact_stages)
            raise TimeoutError

        monkeypatch.setattr(
            importer,
            "_promote_provider_directory_artifact_bundle_transaction",
            commit_then_lose_acknowledgement,
        )

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
            assert await database.scalar(
                "SELECT to_regclass(:relation_name);",
                relation_name=f"{schema}.{stage_table}",
            ) is None
