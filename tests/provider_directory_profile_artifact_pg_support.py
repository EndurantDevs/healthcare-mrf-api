# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Capacity and materialization helpers for artifact PostgreSQL tests."""

from __future__ import annotations

import asyncio
import datetime
import importlib
import os
import uuid
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest

from db.connection import Database
from process import provider_directory_profile_capacity as capacity
from tests.provider_directory_profile_artifact_pg_fixtures import (
    _ArtifactFixture,
    _POSTGRES_DSN_ENV,
    _SELECTED_RESOURCE_TYPES,
    _configure_database,
    _create_complete_empty_scope_layout,
    _create_source_and_resource_fixture,
    _require_postgresql_18,
)
from tests.test_provider_directory_profile_capacity import _geometry_payload
from tests.test_provider_directory_profile_control_capacity import (
    _control_wal_plan_input,
)


importer = importlib.import_module("process.provider_directory_fhir")
_RUN_ID = "run_" + "a" * 32

def _artifact_relation_caps(
    artifact_scratch_cap: int,
    artifact_wal_cap: int,
) -> list[dict[str, object]]:
    """Return generous relation caps with exact artifact overrides."""
    relation_caps = _geometry_payload()["relation_byte_caps"]
    for relation_cap in relation_caps:
        relation_cap["max_temp_bytes"] = 1024 * 1024
        relation_cap["max_wal_bytes"] = 64 * 1024 * 1024
        if relation_cap["relation_name"].endswith("_target"):
            relation_cap["max_target_growth_bytes"] = 64 * 1024 * 1024
            relation_cap["max_deleted_logical_bytes"] = 64 * 1024 * 1024
        else:
            relation_cap["max_scratch_bytes"] = 64 * 1024 * 1024
        if relation_cap["relation_name"] == "artifact_scope":
            relation_cap["max_scratch_bytes"] = artifact_scratch_cap
            relation_cap["max_wal_bytes"] = artifact_wal_cap
    return relation_caps


def _capacity_geometry(
    fixture: _ArtifactFixture,
    *,
    artifact_scratch_cap: int,
    artifact_wal_cap: int,
) -> capacity.ProviderDirectoryProfileCapacityGeometry:
    """Bind artifact projections and caps into an exact test geometry."""
    control_plan_input = _control_wal_plan_input(
        artifact_batch_counts=(100,) * 9,
        evidence_batch_count=100,
        compact_batch_count=100,
        affected_source_count=100,
    )
    seed_geometry = capacity.validated_capacity_geometry(
        _geometry_payload(
            database_pool_size=8,
            pool_reserve_connections=4,
            artifact_scope_wave_count=5,
            artifact_scope_worker_count=2,
            artifact_scope_batch_size=1,
            artifact_scope_projection_hash=fixture.projection.projection_hash,
            artifact_scope_projected_logical_bytes=(
                fixture.projection.projected_logical_bytes
            ),
            max_artifact_scope_rows=fixture.projection.projected_rows,
            tablespace_oid=fixture.tablespace_oid,
            temp_file_limit_bytes=1024 * 1024,
            minimum_remaining_bytes=1,
            relation_byte_caps=_artifact_relation_caps(
                artifact_scratch_cap,
                artifact_wal_cap,
            ),
            control_wal_plan_input_hash=(
                capacity.profile_control_wal_plan_input_hash(
                    control_plan_input
                )
            ),
            control_wal_upper_bound_bytes=1,
            control_metadata_data_upper_bound_bytes=1,
        )
    )
    seed_projection = capacity.project_profile_control_wal_capacity(
        seed_geometry,
        control_plan_input,
    )
    return capacity.validated_capacity_geometry(
        capacity.capacity_geometry_payload(
            replace(
                seed_geometry,
                control_wal_upper_bound_bytes=(
                    seed_projection.total_control_wal_bytes
                ),
                control_metadata_data_upper_bound_bytes=(
                    seed_projection.total_control_metadata_data_bytes
                ),
            )
        )
    )


def _admission(
    geometry: capacity.ProviderDirectoryProfileCapacityGeometry,
    initial_wal_lsn: str,
) -> importer._ProviderDirectoryProfileCapacityAdmission:
    control_plan_input = _control_wal_plan_input(
        artifact_batch_counts=(100,) * 9,
        evidence_batch_count=100,
        compact_batch_count=100,
        affected_source_count=100,
    )
    control_projection = (
        capacity.revalidate_profile_control_wal_projection(
            geometry,
            capacity.project_profile_control_wal_capacity(
                geometry,
                control_plan_input,
            ),
        )
    )
    return importer._ProviderDirectoryProfileCapacityAdmission(
        geometry=geometry,
        control_wal_projection=control_projection,
        lease=SimpleNamespace(
            attestation_id="1" * 64,
            lease_digest="2" * 64,
            max_build_deadline=(
                datetime.datetime.now(datetime.UTC)
                + datetime.timedelta(minutes=10)
            )
        ),
        database_identity=SimpleNamespace(),
        build_id="pdpb_" + "b" * 32,
        run_id=_RUN_ID,
        initial_wal_lsn=initial_wal_lsn,
        wal_tracker=importer._ProviderDirectoryProfileWalTracker(
            accounted_control_operation_counts={
                "capacity_consumption_insert": 1,
            }
        ),
    )


async def _project_scope_relation(
    fixture: _ArtifactFixture,
    probe_geometry: capacity.ProviderDirectoryProfileCapacityGeometry,
    base_table_name: str,
    scope_table_name: str,
) -> tuple[
    str,
    capacity.ProviderDirectoryProfileScratchProjection,
]:
    """Project one empty scope relation using its exact physical layout."""
    relation_ref = importer._unscoped_qt(fixture.schema, scope_table_name)
    relation_oid = int(
        await fixture.database.scalar(
            "SELECT to_regclass(:relation_ref)::oid::bigint;",
            relation_ref=relation_ref,
        )
        or 0
    )
    layout = (
        await importer._provider_directory_profile_relation_storage_fingerprint(
            relation_oid,
            expected_persistence="u",
        )
    )
    projected_table_by_name = {
        table.table_name: table for table in fixture.projection.tables
    }
    projected_table = projected_table_by_name[base_table_name]
    projection = capacity.project_profile_scratch_capacity(
        probe_geometry,
        capacity.ProviderDirectoryProfileScratchInput(
            relation_name="artifact_scope",
            inserted_rows=projected_table.projected_rows,
            inserted_logical_bytes=projected_table.projected_logical_bytes,
            toastable_column_count=len(layout.toastable_columns),
            main_index_pages=layout.main_index_pages,
            toast_index_pages=layout.toast_index_pages,
        ),
    )
    return relation_ref, projection


async def _project_scope_budget(
    fixture: _ArtifactFixture,
) -> tuple[
    int,
    int,
    int,
    dict[str, capacity.ProviderDirectoryProfileScratchProjection],
]:
    """Project aggregate base, growth, and WAL bytes for all scope tables."""
    probe_geometry = _capacity_geometry(
        fixture,
        artifact_scratch_cap=1024 * 1024 * 1024,
        artifact_wal_cap=1024 * 1024 * 1024,
    )
    projections_by_table: dict[
        str,
        capacity.ProviderDirectoryProfileScratchProjection,
    ] = {}
    relation_refs: list[str] = []
    for base_table_name, scope_table_name in sorted(
        fixture.relation_by_table.items()
    ):
        relation_ref, scope_projection = await _project_scope_relation(
            fixture,
            probe_geometry,
            base_table_name,
            scope_table_name,
        )
        projections_by_table[base_table_name] = scope_projection
        relation_refs.append(relation_ref)
    base_bytes = (
        await importer._provider_directory_profile_capacity_relation_bytes(
            relation_refs
        )
    )
    growth_bytes = sum(
        projection.growth_bytes
        for projection in projections_by_table.values()
    )
    wal_bytes = sum(
        projection.wal_bytes
        for projection in projections_by_table.values()
    )
    return base_bytes, growth_bytes, wal_bytes, projections_by_table


async def _scope_row_count(fixture: _ArtifactFixture) -> int:
    total_rows = 0
    for table_name in fixture.relation_by_table.values():
        total_rows += int(
            await fixture.database.scalar(
                "SELECT count(*) FROM "
                + importer._unscoped_qt(fixture.schema, table_name)
                + ";"
            )
            or 0
        )
    return total_rows


async def _scope_toast_bytes(fixture: _ArtifactFixture) -> int:
    return int(
        await fixture.database.scalar(
            """
            SELECT COALESCE(
                       SUM(pg_total_relation_size(relation.reltoastrelid)),
                       0
                   )::bigint
              FROM pg_class AS relation
              JOIN pg_namespace AS namespace
                ON namespace.oid = relation.relnamespace
             WHERE namespace.nspname = :schema
               AND relation.relname = ANY(CAST(:relation_names AS text[]))
               AND relation.reltoastrelid <> 0;
            """,
            schema=fixture.schema,
            relation_names=sorted(fixture.relation_by_table.values()),
        )
        or 0
    )


@asynccontextmanager
async def _artifact_fixture(monkeypatch: pytest.MonkeyPatch):
    database_dsn = os.getenv(_POSTGRES_DSN_ENV)
    if not database_dsn:
        pytest.skip(f"set {_POSTGRES_DSN_ENV} for artifact capacity tests")
    _configure_database(monkeypatch, database_dsn)
    schema = f"provider_directory_artifact_{uuid.uuid4().hex[:12]}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    database = Database()
    is_database_created = False
    original_db = importer.db
    try:
        await database.connect()
        await _require_postgresql_18(database)
        await database.status(
            f"CREATE SCHEMA {importer._q(schema)};"
        )
        is_database_created = True
        monkeypatch.setattr(importer, "db", database)
        fence = await _create_source_and_resource_fixture(database, schema)
        projection = (
            await importer._provider_directory_artifact_scope_exact_projection(
                schema,
                fence,
                _SELECTED_RESOURCE_TYPES,
                batch_size=1,
            )
        )
        relation_by_table = await _create_complete_empty_scope_layout(
            database,
            schema,
        )
        tablespace_oid = int(
            await database.scalar(
                """
                SELECT database_row.dattablespace::bigint
                  FROM pg_database AS database_row
                 WHERE database_row.datname = current_database();
                """
            )
            or 0
        )
        yield _ArtifactFixture(
            database=database,
            schema=schema,
            fence=fence,
            projection=projection,
            relation_by_table=relation_by_table,
            tablespace_oid=tablespace_oid,
        )
    finally:
        importer.db = original_db
        if is_database_created:
            await database.status(
                f"DROP SCHEMA IF EXISTS {importer._q(schema)} CASCADE;"
            )
        await database.disconnect()


def _assert_artifact_projection_is_nontrivial(
    fixture: _ArtifactFixture,
) -> None:
    """Require high-entropy source, role, and affiliation projections."""
    projection_by_type = {
        table.resource_type: table for table in fixture.projection.tables
    }
    assert fixture.projection.tables[0].projected_logical_bytes > 16_000
    assert projection_by_type[
        "PractitionerRole"
    ].projected_logical_bytes > 32_000
    assert projection_by_type[
        "OrganizationAffiliation"
    ].projected_logical_bytes > 32_000


async def _admit_artifact_geometry(
    fixture: _ArtifactFixture,
    geometry: capacity.ProviderDirectoryProfileCapacityGeometry,
):
    """Install one exact capacity admission and return its context token."""
    initial_wal_lsn = str(
        await fixture.database.scalar(
            "SELECT pg_current_wal_insert_lsn()::text;"
        )
    )
    return importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(
        _admission(geometry, initial_wal_lsn)
    )


async def _materialize_two_worker_artifact_scope(
    fixture: _ArtifactFixture,
) -> None:
    """Materialize the source then the two selected resource tables."""
    projection_by_table = {
        table.table_name: table for table in fixture.projection.tables
    }
    source_table = importer.ProviderDirectorySource.__tablename__
    await importer._materialize_provider_directory_artifact_source_scope(
        fixture.schema,
        fixture.relation_by_table[source_table],
        fixture.fence.source_ids,
        projection=projection_by_table[source_table],
    )
    worker_jobs = [
        importer._materialize_provider_directory_artifact_resource_scope(
            fixture.schema,
            fixture.relation_by_table[model.__tablename__],
            model,
            fixture.fence,
            _SELECTED_RESOURCE_TYPES,
            batch_size=1,
            projection=projection_by_table[model.__tablename__],
        )
        for model in (
            importer.ProviderDirectoryPractitionerRole,
            importer.ProviderDirectoryOrganizationAffiliation,
        )
    ]
    assert await asyncio.gather(*worker_jobs) == [2, 2]
