# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL truth tests for artifact layout and two-worker DML."""

from __future__ import annotations

import importlib
import json
import logging
from unittest.mock import AsyncMock

import pytest

from tests.provider_directory_profile_artifact_pg_support import (
    _admit_artifact_geometry,
    _artifact_fixture,
    _assert_artifact_projection_is_nontrivial,
    _capacity_geometry,
    _materialize_two_worker_artifact_scope,
    _project_scope_budget,
    _scope_row_count,
    _scope_toast_bytes,
)


importer = importlib.import_module("process.provider_directory_fhir")

_TARGET_ACTUAL_BY_FIELD = {
    **dict.fromkeys(importer._PROFILE_CAPACITY_TARGET_ACTUAL_FIELDS, 4_096),
    "cutover_actual_hash": "a" * 64,
    "target_wal_start_lsn": "0/20", "wal_observed_lsn": "0/1020",
}


def _artifact_wave_plan(fixture):
    models = (importer.ProviderDirectoryPractitionerRole, importer.ProviderDirectoryOrganizationAffiliation)
    return importer._ArtifactScopeMaterializationPlan(
        source_table=fixture.relation_by_table[
            importer.ProviderDirectorySource.__tablename__
        ],
        created_tables=[],
        relation_by_table=fixture.relation_by_table,
        resource_scope_jobs=tuple((model, fixture.relation_by_table[model.__tablename__]) for model in models),
        model_by_table_name={model.__tablename__: model for model in models},
    )


@pytest.mark.asyncio
async def test_pg18_artifact_preflights_layout_before_two_worker_dml(
    monkeypatch,
):
    """Preflight the complete empty layout before two-worker payload DML."""
    async with _artifact_fixture(monkeypatch) as fixture:
        _assert_artifact_projection_is_nontrivial(fixture)
        assert await _scope_row_count(fixture) == 0
        toast_bytes_before = await _scope_toast_bytes(fixture)
        base_bytes, growth_bytes, wal_bytes, _ = (
            await _project_scope_budget(fixture)
        )
        geometry = _capacity_geometry(
            fixture,
            artifact_scratch_cap=base_bytes + growth_bytes,
            artifact_wal_cap=wal_bytes + (8 * 1024 * 1024),
        )
        admission_token = await _admit_artifact_geometry(fixture, geometry)
        try:
            await importer._preflight_provider_directory_artifact_scope_capacity(
                fixture.schema,
                fixture.relation_by_table,
                fixture.projection,
            )
            await _materialize_two_worker_artifact_scope(fixture)
            await importer._assert_provider_directory_artifact_scope_observed_capacity(
                fixture.schema,
                fixture.relation_by_table,
                fixture.projection,
            )
        finally:
            importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(
                admission_token
            )

        assert await _scope_row_count(fixture) == (
            fixture.projection.projected_rows
        )
        assert await _scope_toast_bytes(fixture) > toast_bytes_before


@pytest.mark.asyncio
async def test_pg18_missing_bucket_index_has_no_populated_repair_path(
    monkeypatch,
):
    async with _artifact_fixture(monkeypatch) as fixture:
        relation_name = (
            importer.ProviderDirectoryPractitionerRole.__tablename__
        )
        scope_table = fixture.relation_by_table[relation_name]
        index_name, _index_sql = (
            importer._provider_directory_profile_bucket_index_sql(
                fixture.schema,
                scope_table,
            )
        )
        relation_token = (
            importer._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.set(
                fixture.relation_by_table
            )
        )
        try:
            metrics = (
                await importer
                ._assert_provider_directory_profile_bucket_index(
                    fixture.schema,
                    relation_name,
                    lock_relation=False,
                )
            )
            assert metrics is not None
            assert metrics["index_name"] == index_name
            await fixture.database.status(
                f"DROP INDEX "
                f"{importer._unscoped_qt(fixture.schema, index_name)};"
            )
            with pytest.raises(
                importer.ProviderDirectoryArtifactBuildStale,
                match="bucket_index_missing",
            ):
                await importer._assert_provider_directory_profile_bucket_index(
                    fixture.schema,
                    relation_name,
                    lock_relation=False,
                )
            assert (
                await fixture.database.scalar(
                    "SELECT to_regclass(:index_ref);",
                    index_ref=importer._unscoped_qt(
                        fixture.schema,
                        index_name,
                    ),
                )
                is None
            )
        finally:
            importer._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.reset(
                relation_token
            )


@pytest.mark.asyncio
async def test_pg18_stage_storage_fingerprint_detects_dropped_index(
    monkeypatch,
):
    async with _artifact_fixture(monkeypatch) as fixture:
        stage_table = "provider_directory_profile_evidence_stage_probe"
        await fixture.database.status(
            importer.profile_artifact.profile_evidence_table_sql(
                fixture.schema,
                stage_table,
                logged=True,
            )
        )
        for statement in (
            importer.profile_artifact.profile_index_statements(
                fixture.schema,
                stage_table,
                evidence=True,
            )
        ):
            await fixture.database.status(statement)
        stage_oid = int(
            await fixture.database.scalar(
                "SELECT to_regclass(:relation_ref)::oid::bigint;",
                relation_ref=importer._unscoped_qt(
                    fixture.schema,
                    stage_table,
                ),
            )
            or 0
        )
        before = (
            await importer
            ._provider_directory_profile_stage_storage_fingerprint(
                fixture.schema,
                stage_table,
                expected_oid=stage_oid,
                lock_relation=False,
            )
        )
        dropped_index = importer.profile_artifact.profile_index_name(
            stage_table,
            "npi_fact_idx",
        )
        await fixture.database.status(
            f"DROP INDEX "
            f"{importer._unscoped_qt(fixture.schema, dropped_index)};"
        )
        after = (
            await importer
            ._provider_directory_profile_stage_storage_fingerprint(
                fixture.schema,
                stage_table,
                expected_oid=stage_oid,
                lock_relation=False,
            )
        )

        assert before != after
        assert len(before) == len(after) == 64


@pytest.mark.asyncio
async def test_pg18_clone_capacity_wave_and_target_actual_reuse(
    monkeypatch,
    caplog,
):
    async with _artifact_fixture(monkeypatch) as fixture:
        monkeypatch.setenv(
            importer.PROVIDER_DIRECTORY_PROFILE_CLONE_CAPACITY_OBSERVATION_ENV,
            "1",
        )
        caplog.set_level(logging.INFO, logger=importer.__name__)
        await importer._materialize_artifact_resource_waves(
            fixture.schema,
            _artifact_wave_plan(fixture),
            fixture.fence,
            frozenset({"PractitionerRole", "OrganizationAffiliation"}),
            None,
            {},
            1,
            2,
        )
        target_sampler = AsyncMock(side_effect=AssertionError("unexpected target sample"))
        monkeypatch.setattr(importer, "_profile_capacity_observation_sample", target_sampler)
        async with importer._observe_profile_capacity_wave(
            "target",
            {},
            coordinate_by_field={"wave": 1},
            target_actuals_by_field=_TARGET_ACTUAL_BY_FIELD,
        ):
            await fixture.database.scalar("SELECT 1;")
        target_sampler.assert_not_awaited()

        observations = [
            json.loads(log_record.message)
            for log_record in caplog.records
            if "profile-clone-capacity-observation.v1" in log_record.message
        ]
        artifact, target_observation_by_field = observations
        assert artifact["wal_observation"]["bytes"] > 0
        assert all(
            artifact["relation_observation"]["after"][name]
            >= artifact["relation_observation"]["before"][name]
            for name in artifact["relation_observation"]["after"]
        )
        assert artifact["temp_observation"]["status"] == (
            "delayed_database_aggregate"
        )
        assert target_observation_by_field["cutover_actual"] == (
            _TARGET_ACTUAL_BY_FIELD
        )
        assert target_observation_by_field["temp_observation"]["before"] is None
        assert target_observation_by_field["temp_observation"]["status"] == (
            "unavailable_transaction_local_stats"
        )
