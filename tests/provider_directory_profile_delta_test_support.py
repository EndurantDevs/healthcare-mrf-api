# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Core fixtures for Provider Directory profile-delta PostgreSQL tests."""

from __future__ import annotations

import datetime
import importlib
import json
import os
import uuid
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import OperationalError

from db.connection import Database
from process import provider_directory_profile as profile
from process import provider_directory_profile_capacity as capacity
from tests.test_provider_directory_profile_capacity import _geometry_payload
from tests.test_provider_directory_profile_control_capacity import (
    _control_wal_plan_input,
)


importer = importlib.import_module("process.provider_directory_fhir")


def _bound_control_geometry(
    geometry_payload: dict[str, object],
) -> tuple[
    capacity.ProviderDirectoryProfileCapacityGeometry,
    capacity.ProviderDirectoryProfileControlWalProjection,
]:
    """Bind a generous but internally exact control ledger for PG truth tests."""

    plan_input = _control_wal_plan_input(
        artifact_batch_counts=(100,) * 9,
        evidence_batch_count=100,
        compact_batch_count=100,
        affected_source_count=100,
        admission_row_lock_count=2,
        cutover_row_lock_count=(
            importer.PROVIDER_DIRECTORY_PROFILE_CUTOVER_FIXED_ROW_LOCK_COUNT
            * importer.PROVIDER_DIRECTORY_ARTIFACT_CUTOVER_ATTEMPTS
        ),
    )
    geometry_payload.update(
        {
            "control_wal_plan_input_hash": (
                capacity.profile_control_wal_plan_input_hash(plan_input)
            ),
            "control_wal_upper_bound_bytes": 1,
            "control_metadata_data_upper_bound_bytes": 1,
        }
    )
    seed_geometry = capacity.validated_capacity_geometry(geometry_payload)
    seed_projection = capacity.project_profile_control_wal_capacity(
        seed_geometry,
        plan_input,
    )
    geometry = capacity.revalidate_capacity_geometry(
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
    projection = capacity.revalidate_profile_control_wal_projection(
        geometry,
        capacity.project_profile_control_wal_capacity(
            geometry,
            plan_input,
        ),
    )
    return geometry, projection


def _prepared_delta_lineage_by_field(
    source_vector,
    context_vector,
    geometry,
) -> dict[str, object]:
    """Return source, context, and geometry identity defaults."""
    return {
        "from_source_vector_hash": (
            importer._provider_directory_profile_source_vector_hash(
                source_vector
            )
        ),
        "to_source_vector_hash": (
            importer._provider_directory_profile_source_vector_hash(
                source_vector
            )
        ),
        "to_source_vector": source_vector,
        "from_source_context_vector_hash": (
            importer._provider_directory_profile_source_context_vector_hash(
                context_vector
            )
        ),
        "to_source_context_vector_hash": (
            importer._provider_directory_profile_source_context_vector_hash(
                context_vector
            )
        ),
        "to_source_context_vector": context_vector,
        "capacity_geometry_hash": capacity.capacity_geometry_hash(geometry),
        "capacity_geometry_json": (
            capacity.canonical_capacity_geometry_json(geometry)
        ),
    }


def _prepared_delta(
    **overrides: object,
) -> importer.ProviderDirectoryPreparedProfileDelta:
    """Return a complete prepared-delta fixture with exact lineage."""
    source_vector = (("source-a", "dataset-a"),)
    context_vector = (("source-a", "a" * 64),)
    geometry = capacity.validated_capacity_geometry(_geometry_payload())
    delta_by_field: dict[str, object] = {
        "schema": "profile_delta_test",
        "build_id": "pdpb_" + "1" * 32,
        "resume_lineage_hash": "2" * 64,
        "owner_run_id": "run-delta",
        "evidence_stage": "evidence_stage",
        "evidence_stage_oid": 11,
        "profile_stage": "profile_stage",
        "profile_stage_oid": 12,
        "affected_npi_stage": "affected_stage",
        "affected_npi_stage_oid": 13,
        "evidence_target_oid": 21,
        "profile_target_oid": 22,
        "from_generation_id": "pdprofile_" + "3" * 32,
        "generation_id": "pdprofile_" + "4" * 32,
        "operation": "publish",
        "selection_proof_id": "5" * 64,
        "control_generation": 7,
        "authority_revision": 7,
        "profile_schema_version": 1,
        "profile_strategy_version": (
            profile.PROFILE_BUILD_STRATEGY_VERSION
        ),
        "executable_plan_hash": "6" * 64,
        **_prepared_delta_lineage_by_field(
            source_vector,
            context_vector,
            geometry,
        ),
        "refresh_source_ids": (),
        "removed_source_ids": (),
        "expected_evidence_rows": 0,
        "expected_profile_rows": 0,
        "profile_as_of": "2026-07-30",
        "from_capacity_geometry_status": "legacy_unavailable",
        "from_capacity_geometry_hash": None,
        "from_capacity_geometry_json": None,
        "capacity_geometry_status": "verified",
    }
    delta_by_field.update(overrides)
    return importer.ProviderDirectoryPreparedProfileDelta(**delta_by_field)


async def _require_disposable_database(database: Database) -> None:
    try:
        database_name = str(
            await database.scalar("SELECT current_database();") or ""
        )
    except (OSError, OperationalError):
        pytest.skip("Profile delta tests need disposable PostgreSQL")
    is_disposable_database_opted_in = os.getenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROFILE_ALLOW_SCHEMA_TESTS",
        "",
    ).strip().lower() in {"1", "true", "yes", "on"}
    if (
        "test" not in database_name.lower()
        and not is_disposable_database_opted_in
    ):
        pytest.skip("Profile delta tests need a test database")


@asynccontextmanager
async def _delta_database(monkeypatch):
    schema = f"provider_directory_delta_{uuid.uuid4().hex[:12]}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    database = Database()
    is_database_created = False
    try:
        await database.connect()
        await _require_disposable_database(database)
        await database.status(
            f"CREATE SCHEMA {profile.quote_identifier(schema)};"
        )
        is_database_created = True
        yield database, schema
    finally:
        if is_database_created:
            await database.status(
                f"DROP SCHEMA IF EXISTS "
                f"{profile.quote_identifier(schema)} CASCADE;"
            )
        await database.disconnect()


async def _insert_evidence(
    database: Database,
    table_ref: str,
    *,
    evidence_key: str,
    fact_type: str,
    source_id: str,
    dataset_id: str,
    value_json: dict[str, object],
) -> None:
    await database.status(
        f"""
        INSERT INTO {table_ref} (
            evidence_key, npi, fact_type, fact_key, value_json,
            source_id, endpoint_id, dataset_id, resource_type,
            resource_id, active, observed_at
        ) VALUES (
            :evidence_key, 1000000004, :fact_type, :fact_key,
            CAST(:value_json AS jsonb), :source_id,
            :endpoint_id, :dataset_id, 'PractitionerRole',
            :resource_id, true, now()
        );
        """,
        evidence_key=evidence_key,
        fact_type=fact_type,
        fact_key=evidence_key,
        value_json=json.dumps(value_json),
        source_id=source_id,
        endpoint_id="endpoint-" + source_id,
        dataset_id=dataset_id,
        resource_id="resource-" + evidence_key,
    )
