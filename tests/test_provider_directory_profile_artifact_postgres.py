# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL truth tests for artifact layout and two-worker DML."""

from __future__ import annotations

import importlib

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
