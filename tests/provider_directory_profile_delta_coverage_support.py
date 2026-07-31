# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared fixtures for narrow Provider Directory Profile delta edge proofs."""

from __future__ import annotations

import contextlib
import dataclasses
import datetime
import importlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from tests.provider_directory_profile_delta_test_support import _prepared_delta
from tests.provider_directory_profile_execution_test_support import (
    _wal_tracker_admission,
)
from tests.test_provider_directory_profile_selection_attestation import (
    _execution,
)


importer = importlib.import_module("process.provider_directory_fhir")
profile = importlib.import_module("process.provider_directory_profile")


def _ready_delta_checkpoint(
    delta: importer.ProviderDirectoryPreparedProfileDelta,
) -> dict[str, object]:
    return {
        "build_id": delta.build_id,
        "owner_run_id": delta.owner_run_id,
        "state": "ready",
        "resume_lineage_hash": delta.resume_lineage_hash,
        "executable_plan_hash": delta.executable_plan_hash,
        "materialization_mode": "source_delta",
        "evidence_stage": delta.evidence_stage,
        "profile_stage": delta.profile_stage,
        "affected_npi_stage": delta.affected_npi_stage,
        "current_source_vector_hash": delta.from_source_vector_hash,
        "desired_source_vector_hash": delta.to_source_vector_hash,
        "current_source_context_vector_hash": (
            delta.from_source_context_vector_hash
        ),
        "desired_source_context_vector_hash": (
            delta.to_source_context_vector_hash
        ),
        "profile_as_of": delta.profile_as_of,
        "capacity_geometry_status": delta.capacity_geometry_status,
        "capacity_geometry_hash": delta.capacity_geometry_hash,
        "capacity_geometry_json": delta.capacity_geometry_json,
        "evidence_stage_oid": delta.evidence_stage_oid,
        "profile_stage_oid": delta.profile_stage_oid,
        "affected_npi_stage_oid": delta.affected_npi_stage_oid,
        "evidence_target_oid": delta.evidence_target_oid,
        "profile_target_oid": delta.profile_target_oid,
        "refresh_source_ids": list(delta.refresh_source_ids),
        "removed_source_ids": list(delta.removed_source_ids),
        "evidence_next_batch": 2,
        "evidence_total_batches": 2,
        "profile_next_batch": 3,
        "profile_total_batches": 3,
    }

def _matching_delta_receipt(
    delta: importer.ProviderDirectoryPreparedProfileDelta,
) -> dict[str, object]:
    return {
        **importer._provider_directory_profile_delta_receipt_static_values(
            delta
        ),
        "from_capacity_geometry_json": delta.from_capacity_geometry_json,
        "capacity_geometry_json": delta.capacity_geometry_json,
        "evidence_rows": delta.expected_evidence_rows,
        "profile_rows": delta.expected_profile_rows,
        "evidence_inserted": 0,
        "evidence_deleted": 0,
        "profile_inserted": 0,
        "profile_deleted": 0,
    }

def _matching_delta_serving_state(
    delta: importer.ProviderDirectoryPreparedProfileDelta,
) -> importer._ProviderDirectoryProfileServingState:
    return importer._ProviderDirectoryProfileServingState(
        status="published",
        operation="publish",
        control_generation=max(delta.control_generation - 1, 1),
        generation_id=delta.from_generation_id,
        selection_proof_id="a" * 64,
        authority_revision=max(delta.authority_revision - 1, 1),
        profile_schema_version=delta.profile_schema_version,
        profile_strategy_version=delta.profile_strategy_version,
        source_vector=(("source-a", "dataset-a"),),
        source_vector_hash=delta.from_source_vector_hash,
        source_context_vector=(("source-a", "a" * 64),),
        source_context_vector_hash=delta.from_source_context_vector_hash,
        executable_plan_hash="b" * 64,
        evidence_target_oid=delta.evidence_target_oid,
        profile_target_oid=delta.profile_target_oid,
        evidence_rows=delta.expected_evidence_rows,
        profile_rows=delta.expected_profile_rows,
        profile_as_of=delta.profile_as_of,
        published_at="2026-07-30T00:00:00+00:00",
        capacity_geometry_status=delta.from_capacity_geometry_status,
        capacity_geometry_hash=delta.from_capacity_geometry_hash,
        capacity_geometry_json=delta.from_capacity_geometry_json,
    )

def _valid_serving_row() -> dict[str, object]:
    source_vector = (("source-a", "dataset-a"),)
    context_vector = (("source-a", "a" * 64),)
    return {
        "status": "published",
        "operation": "publish",
        "control_generation": 7,
        "generation_id": "pdprofile_" + "1" * 32,
        "selection_proof_id": "2" * 64,
        "authority_revision": 7,
        "profile_schema_version": 1,
        "profile_strategy_version": profile.PROFILE_BUILD_STRATEGY_VERSION,
        "source_vector_json": (
            importer._provider_directory_profile_source_vector_json(
                source_vector
            )
        ),
        "source_vector_hash": (
            importer._provider_directory_profile_source_vector_hash(
                source_vector
            )
        ),
        "source_context_vector_json": (
            importer._provider_directory_profile_source_context_vector_json(
                context_vector
            )
        ),
        "source_context_vector_hash": (
            importer._provider_directory_profile_source_context_vector_hash(
                context_vector
            )
        ),
        "executable_plan_hash": "3" * 64,
        "capacity_geometry_status": "legacy_unavailable",
        "capacity_geometry_hash": None,
        "capacity_geometry_json": None,
        "evidence_target_oid": 11,
        "profile_target_oid": 12,
        "evidence_rows": 3,
        "profile_rows": 2,
        "profile_as_of": "2026-07-30",
        "published_at": datetime.datetime(
            2026,
            7,
            30,
            tzinfo=datetime.timezone.utc,
        ),
        "cutover_forecast_hash": None,
    }
