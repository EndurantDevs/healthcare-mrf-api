# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared fixtures for Provider Directory execution-boundary tests."""

from __future__ import annotations

import datetime
import importlib
from types import SimpleNamespace

from .test_provider_directory_profile_capacity import _geometry_payload
from .test_provider_directory_profile_control_capacity import (
    _bound_control_wal_projection,
)

importer = importlib.import_module("process.provider_directory_fhir")
capacity = importlib.import_module("process.provider_directory_profile_capacity")

def _published_dataset_state() -> dict[str, object]:
    return {
        "dataset_id": "dataset-1",
        "acquisition_root_run_id": "run-root-1",
        "dataset_hash": "b" * 64,
        "status": importer.ENDPOINT_DATASET_PUBLISHED,
        "is_current": True,
        "superseded_at": None,
        "publication_metadata_json": {"source_ids": ["pdfhir_payer"]},
    }


def _capacity_geometry_identity():
    geometry = capacity.validated_capacity_geometry(_geometry_payload())
    return (
        geometry,
        capacity.capacity_geometry_hash(geometry),
        capacity.canonical_capacity_geometry_json(geometry),
    )


def _profile_serving_state(
    source_vector,
    source_context_vector,
):
    return importer._ProviderDirectoryProfileServingState(
        status="published",
        operation="publish",
        control_generation=6,
        generation_id="pdprofile_" + "1" * 32,
        selection_proof_id="2" * 64,
        authority_revision=6,
        profile_schema_version=1,
        profile_strategy_version=(
            importer.profile_artifact.PROFILE_BUILD_STRATEGY_VERSION
        ),
        source_vector=source_vector,
        source_vector_hash=(
            importer._provider_directory_profile_source_vector_hash(
                source_vector
            )
        ),
        source_context_vector=source_context_vector,
        source_context_vector_hash=(
            importer._provider_directory_profile_source_context_vector_hash(
                source_context_vector
            )
        ),
        executable_plan_hash="3" * 64,
        evidence_target_oid=101,
        profile_target_oid=102,
        evidence_rows=5,
        profile_rows=2,
        profile_as_of="2026-07-30",
        published_at="2026-07-30T00:00:00Z",
    )


def _wal_tracker_admission():
    geometry, control_projection = _bound_control_wal_projection()
    return importer._ProviderDirectoryProfileCapacityAdmission(
        geometry=geometry,
        control_wal_projection=control_projection,
        lease=SimpleNamespace(
            max_build_deadline=(
                datetime.datetime.now(datetime.UTC)
                + datetime.timedelta(minutes=10)
            )
        ),
        database_identity=SimpleNamespace(),
        build_id="pdpb_" + "a" * 32,
        run_id="run_" + "b" * 32,
        initial_wal_lsn="0/1",
        wal_tracker=importer._ProviderDirectoryProfileWalTracker(
            accounted_control_operation_counts={
                "admission_row_lock": 2,
                "capacity_consumption_insert": 3,
            }
        ),
    )


def _capacity_consumption_row(admission):
    return {
        "attestation_id": admission.lease.attestation_id,
        "lease_digest": admission.lease.lease_digest,
        "capacity_geometry_hash": capacity.capacity_geometry_hash(
            admission.geometry
        ),
        "executable_plan_hash": admission.geometry.executable_plan_hash,
        "selection_proof_id": admission.geometry.selection_proof_id,
        "source_vector_hash": (
            admission.geometry.desired_source_vector_hash
        ),
        "source_context_vector_hash": (
            admission.geometry.desired_context_vector_hash
        ),
        "run_id": admission.run_id,
        "build_id": admission.build_id,
        "profile_as_of": admission.geometry.profile_as_of,
    }
