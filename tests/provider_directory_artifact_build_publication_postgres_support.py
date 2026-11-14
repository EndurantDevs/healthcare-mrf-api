# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ordinary overlay builds use logical publication versions on native PostgreSQL."""

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from tests.provider_directory_serving_recovery_postgres_support import ordinary_serving_case


async def _indexes(_schema, _stage):
    """Probe tables do not have secondary indexes to rename."""
    return None


async def _stage(case):
    directory = case.directory
    async with directory._provider_directory_artifact_build_guard(
        case.schema, directory.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE
    ) as fence:
        assert fence.target_oid is None
    await case.database.status(f"CREATE TABLE {case.schema}.overlay_next(marker text PRIMARY KEY)")
    await case.database.status(f"INSERT INTO {case.schema}.overlay_next VALUES ('new')")
    return directory.ProviderDirectoryPreparedArtifactStage(
        case.schema, "overlay_next", directory.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE, _indexes, fence
    )


async def _cut(case, stage):
    await case.directory._promote_provider_directory_artifact_stage_transaction(
        stage.schema, stage.stage_table, stage.target_relation, stage.rename_stage_indexes, stage.build_fence
    )


@pytest.mark.parametrize("changed", ["generation", "timestamp", "missing"])
async def test_native_overlay_cut_refuses_changed_publication(monkeypatch, changed):
    async with ordinary_serving_case(monkeypatch) as case:
        stage = await _stage(case)
        updates_by_change = {
            "generation": "UPDATE {schema}.address_alias_artifact_state_v1 SET generation=generation+1",
            "timestamp": "UPDATE {schema}.address_alias_artifact_state_v1 SET updated_at=updated_at+interval '1 second'",
            "missing": "DELETE FROM {schema}.address_alias_artifact_state_v1",
        }
        await case.database.status(
            updates_by_change[changed].format(schema=case.schema)
            + " WHERE artifact_name='provider_directory_address_overlay'"
        )
        with pytest.raises(case.directory.ProviderDirectoryArtifactBuildStale):
            await _cut(case, stage)
        assert await case.database.scalar(f"SELECT marker FROM {case.schema}.{stage.target_relation}") == "old"
        assert await case.database.scalar(f"SELECT marker FROM {case.schema}.overlay_next") == "new"


async def test_native_copied_overlay_keeps_same_logical_publication(monkeypatch):
    async with ordinary_serving_case(monkeypatch) as case:
        stage = await _stage(case)
        oid_lookup = AsyncMock(side_effect=AssertionError("Canonical overlay fencing must not read OIDs"))
        monkeypatch.setattr(case.directory, "_provider_directory_relation_oid", oid_lookup)
        target = f"{case.schema}.{stage.target_relation}"
        async with case.database.transaction():
            await case.database.status(f"CREATE TABLE {case.schema}.overlay_copy (LIKE {target} INCLUDING ALL)")
            await case.database.status(f"INSERT INTO {case.schema}.overlay_copy SELECT * FROM {target}")
            await case.database.status(f"DROP TABLE {target}")
            await case.database.status(f"ALTER TABLE {case.schema}.overlay_copy RENAME TO {stage.target_relation}")
        async with case.database.transaction():
            await case.directory._assert_provider_directory_artifact_build_fence(stage)
        oid_lookup.assert_not_called()
        assert await case.database.scalar(f"SELECT marker FROM {target}") == "old"


async def test_native_empty_overlay_bootstrap_uses_logical_absence(monkeypatch):
    async with ordinary_serving_case(monkeypatch) as case:
        target = case.directory.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE
        await case.database.status(f"DROP TABLE {case.schema}.{target}")
        await case.database.status(
            f"DELETE FROM {case.schema}.address_alias_artifact_state_v1 WHERE artifact_name=:artifact", artifact=target
        )
        stage = await _stage(case)
        assert stage.build_fence.target_publication is None
        stage = replace(stage, build_fence=replace(stage.build_fence, alias_generation=0))
        await _cut(case, stage)
        assert await case.database.scalar(f"SELECT marker FROM {case.schema}.{target}") == "new"
        assert (await case.directory._address_overlay_publication(case.schema))[0] == 0
