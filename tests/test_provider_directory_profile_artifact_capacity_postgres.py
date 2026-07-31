# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL truth tests for aggregate artifact capacity refusal."""

from __future__ import annotations

import importlib

import pytest

from process import provider_directory_profile_capacity as capacity
from tests.provider_directory_profile_artifact_pg_support import (
    _admission,
    _admit_artifact_geometry,
    _artifact_fixture,
    _capacity_geometry,
    _project_scope_budget,
    _scope_row_count,
)


importer = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.asyncio
async def test_pg18_artifact_refuses_below_aggregate_caps(
    monkeypatch,
):
    async with _artifact_fixture(monkeypatch) as fixture:
        base_bytes, growth_bytes, wal_bytes, _ = (
            await _project_scope_budget(fixture)
        )
        assert base_bytes > 0
        assert growth_bytes > 1
        assert wal_bytes > 1

        refusal_cases = (
            (
                base_bytes + growth_bytes - 1,
                wal_bytes + (8 * 1024 * 1024),
                "provider_directory_profile_capacity_artifact_growth_projected",
            ),
            (
                base_bytes + growth_bytes,
                wal_bytes - 1,
                "provider_directory_profile_capacity_artifact_wal_projected",
            ),
        )
        for scratch_cap, wal_cap, error_match in refusal_cases:
            geometry = _capacity_geometry(
                fixture,
                artifact_scratch_cap=scratch_cap,
                artifact_wal_cap=wal_cap,
            )
            initial_wal_lsn = str(
                await fixture.database.scalar(
                    "SELECT pg_current_wal_insert_lsn()::text;"
                )
            )
            admission_token = (
                importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(
                    _admission(geometry, initial_wal_lsn)
                )
            )
            try:
                with pytest.raises(RuntimeError, match=error_match):
                    await importer._preflight_provider_directory_artifact_scope_capacity(
                        fixture.schema,
                        fixture.relation_by_table,
                        fixture.projection,
                    )
            finally:
                importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(
                    admission_token
                )
            assert await _scope_row_count(fixture) == 0


def _two_worker_growth_bounds(
    base_bytes: int,
    projections_by_table: dict[
        str,
        capacity.ProviderDirectoryProfileScratchProjection,
    ],
) -> tuple[int, int]:
    """Return single-worker and concurrent-wave artifact growth bounds."""
    source_growth = projections_by_table[
        importer.ProviderDirectorySource.__tablename__
    ].growth_bytes
    role_growth = projections_by_table[
        importer.ProviderDirectoryPractitionerRole.__tablename__
    ].growth_bytes
    affiliation_growth = projections_by_table[
        importer.ProviderDirectoryOrganizationAffiliation.__tablename__
    ].growth_bytes
    assert role_growth > 0
    assert affiliation_growth > 0
    return (
        base_bytes + source_growth + max(role_growth, affiliation_growth),
        base_bytes + source_growth + role_growth + affiliation_growth,
    )


@pytest.mark.asyncio
async def test_pg18_two_worker_artifact_wave_refuses_aggregate_overflow_before_dml(
    monkeypatch,
):
    """Refuse a two-worker wave whose aggregate growth exceeds its cap."""
    async with _artifact_fixture(monkeypatch) as fixture:
        base_bytes, _growth_bytes, wal_bytes, projections_by_table = (
            await _project_scope_budget(fixture)
        )
        single_worker_cap, concurrent_wave_required = (
            _two_worker_growth_bounds(base_bytes, projections_by_table)
        )
        assert single_worker_cap < concurrent_wave_required
        geometry = _capacity_geometry(
            fixture,
            artifact_scratch_cap=single_worker_cap,
            artifact_wal_cap=wal_bytes + (8 * 1024 * 1024),
        )
        assert geometry.artifact_scope_worker_count == 2
        admission_token = await _admit_artifact_geometry(fixture, geometry)
        try:
            with pytest.raises(
                RuntimeError,
                match=(
                    "provider_directory_profile_capacity_"
                    "artifact_growth_projected"
                ),
            ):
                await importer._preflight_provider_directory_artifact_scope_capacity(
                    fixture.schema,
                    fixture.relation_by_table,
                    fixture.projection,
                )
        finally:
            importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(
                admission_token
            )
        assert await _scope_row_count(fixture) == 0
