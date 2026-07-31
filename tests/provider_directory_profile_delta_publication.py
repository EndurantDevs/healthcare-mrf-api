# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Publication and replay assertions for profile-delta tests."""

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
from tests.provider_directory_profile_delta_test_support import _prepared_delta


def _prepared_scenario_delta(
    scenario,
    lineage,
    oid_by_name: dict[str, int],
    geometry,
) -> importer.ProviderDirectoryPreparedProfileDelta:
    """Return the fully bound prepared delta for the truth-table scenario."""
    return _prepared_delta(
        schema=scenario.schema,
        build_id=lineage.build_id,
        resume_lineage_hash=lineage.resume_hash,
        owner_run_id="run-delta",
        evidence_stage=scenario.evidence_stage,
        evidence_stage_oid=oid_by_name["evidence_stage"],
        profile_stage=scenario.profile_stage,
        profile_stage_oid=oid_by_name["profile_stage"],
        affected_npi_stage=scenario.affected_stage,
        affected_npi_stage_oid=oid_by_name["affected_stage"],
        evidence_target_oid=oid_by_name["evidence_target"],
        profile_target_oid=oid_by_name["profile_target"],
        from_generation_id=scenario.old_generation,
        generation_id=scenario.new_generation,
        operation="publish",
        selection_proof_id=lineage.proof_id,
        control_generation=7,
        authority_revision=7,
        profile_schema_version=1,
        profile_strategy_version=profile.PROFILE_BUILD_STRATEGY_VERSION,
        executable_plan_hash=lineage.plan_hash,
        from_source_vector_hash=lineage.from_vector_hash,
        to_source_vector_hash=lineage.to_vector_hash,
        to_source_vector=lineage.to_vector,
        from_source_context_vector_hash=lineage.from_context_vector_hash,
        to_source_context_vector_hash=lineage.to_context_vector_hash,
        to_source_context_vector=lineage.to_context_vector,
        refresh_source_ids=("source-a",),
        removed_source_ids=(),
        expected_evidence_rows=2,
        expected_profile_rows=1,
        profile_as_of="2026-07-30",
        from_capacity_geometry_status="legacy_unavailable",
        from_capacity_geometry_hash=None,
        from_capacity_geometry_json=None,
        capacity_geometry_status="verified",
        capacity_geometry_hash=capacity.capacity_geometry_hash(geometry),
        capacity_geometry_json=(
            capacity.canonical_capacity_geometry_json(geometry)
        ),
        resume_checkpoint=(scenario.schema, lineage.build_id),
    )


async def _delta_capacity_admission(
    database: Database,
    lineage,
    capacity_context,
):
    """Return an admitted one-hour capacity context for the delta run."""
    initial_wal_lsn = str(
        await database.scalar(
            "SELECT pg_current_wal_insert_lsn()::text;"
        )
    )
    return importer._ProviderDirectoryProfileCapacityAdmission(
        geometry=capacity_context.geometry,
        control_wal_projection=capacity_context.control_projection,
        lease=SimpleNamespace(
            max_build_deadline=(
                datetime.datetime.now(datetime.UTC)
                + datetime.timedelta(hours=1)
            )
        ),
        database_identity=capacity_context.database_identity,
        build_id=lineage.build_id,
        run_id="run-delta",
        initial_wal_lsn=initial_wal_lsn,
        wal_tracker=importer._ProviderDirectoryProfileWalTracker(
            accounted_control_operation_counts={
                "admission_row_lock": 2,
                "capacity_consumption_insert": 1,
            }
        ),
    )


async def _publish_prepared_delta(profile_delta, admission) -> None:
    """Publish under exact capacity and artifact-fence context identities."""
    capacity_token = (
        importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(admission)
    )
    fence_token = importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.set(
        importer.ProviderDirectoryArtifactDatasetFence(())
    )
    try:
        await importer._promote_provider_directory_artifact_bundle_transaction(
            (),
            profile_delta=profile_delta,
        )
    finally:
        importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.reset(fence_token)
        importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(
            capacity_token
        )


async def _assert_delta_publication(
    database: Database,
    scenario,
    lineage,
    profile_delta,
) -> None:
    """Assert retained facts, changed facts, dropped scratch, and checkpoint."""
    evidence_rows = await database.all(
        f"SELECT source_id, dataset_id, fact_type "
        f"FROM {scenario.evidence_target_ref} ORDER BY source_id;"
    )
    assert [tuple(evidence_row) for evidence_row in evidence_rows] == [
        ("source-a", "dataset-a-new", "contact"),
        ("source-b", "dataset-b", "specialty"),
    ]
    profile_json = await database.scalar(
        f"SELECT profile_json FROM {scenario.profile_target_ref} "
        "WHERE npi = 1000000004;"
    )
    decoded_profile = (
        json.loads(profile_json)
        if isinstance(profile_json, str)
        else profile_json
    )
    assert set(decoded_profile["facts"]) == {"contact", "specialty"}
    assert await importer._is_provider_directory_profile_delta_committed(
        profile_delta
    )
    stage_names = (
        scenario.evidence_stage,
        scenario.profile_stage,
        scenario.affected_stage,
    )
    stage_absence_rows = []
    for stage_name in stage_names:
        stage_absence_rows.append(
            bool(
                await database.scalar(
                    "SELECT to_regclass(:relation_ref) IS NULL;",
                    relation_ref=profile.qualified_table(
                        scenario.schema, stage_name
                    ),
                )
            )
        )
    assert all(stage_absence_rows)
    assert int(
        await database.scalar(
            f"SELECT count(*) FROM {scenario.checkpoint_ref} "
            "WHERE build_id = :build_id;",
            build_id=lineage.build_id,
        )
        or 0
    ) == 0


async def _assert_delta_post_commit_scenario(
    database: Database,
    scenario,
    profile_delta,
    geometry,
    post_commit_scenario: str,
) -> None:
    """Replay the receipt or prove an identity conflict fails closed."""
    if post_commit_scenario == "replay_without_scratch":
        await importer._promote_provider_directory_artifact_bundle_transaction(
            (),
            profile_delta=profile_delta,
        )
        assert int(
            await database.scalar(
                f"SELECT count(*) FROM {scenario.evidence_target_ref};"
            )
            or 0
        ) == 2
        return
    if post_commit_scenario == "conflicting_receipt":
        conflicting_delta = replace(
            profile_delta,
            selection_proof_id="9" * 64,
        )
    else:
        changed_geometry_by_field = capacity.capacity_geometry_payload(geometry)
        changed_geometry_by_field["max_profile_rows"] += 1
        changed_geometry = capacity.validated_capacity_geometry(
            changed_geometry_by_field
        )
        conflicting_delta = replace(
            profile_delta,
            capacity_geometry_hash=capacity.capacity_geometry_hash(
                changed_geometry
            ),
            capacity_geometry_json=(
                capacity.canonical_capacity_geometry_json(changed_geometry)
            ),
        )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="provider_directory_profile_delta_receipt_conflict",
    ):
        await importer._promote_provider_directory_artifact_bundle_transaction(
            (),
            profile_delta=conflicting_delta,
        )
