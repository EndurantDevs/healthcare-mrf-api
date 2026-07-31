# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Boundary coverage for capacity admission and serving bootstrap."""

from __future__ import annotations

import asyncio
import dataclasses
import datetime
import importlib
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, Mock

import pytest

from .test_provider_directory_profile_selection_attestation import _execution
from .test_provider_directory_profile_capacity import _geometry_payload
from .test_provider_directory_profile_control_capacity import (
    _bound_control_wal_projection,
)
from .provider_directory_profile_execution_test_support import (
    _capacity_consumption_row,
    _capacity_geometry_identity,
    _profile_serving_state,
    _published_dataset_state,
    _wal_tracker_admission,
)

importer = importlib.import_module("process.provider_directory_fhir")
capacity = importlib.import_module("process.provider_directory_profile_capacity")


def test_verified_admission_uses_execution_capacity_attestation(monkeypatch):
    geometry = capacity.validated_capacity_geometry(_geometry_payload())
    verified_lease = object()
    execution = dataclasses.replace(
        _execution(),
        capacity_attestation={"contract_id": "capacity-test"},
    )
    database_identity = SimpleNamespace(
        database_system_identifier="system-1",
        database_oid=42,
        database_name="healthporta",
    )
    verify_lease = Mock(return_value=verified_lease)
    monkeypatch.setattr(
        importer.profile_capacity,
        "capacity_geometry_hash",
        Mock(return_value="1" * 64),
    )
    monkeypatch.setattr(
        importer.profile_capacity_runtime,
        "configured_capacity_lease_trust",
        Mock(return_value=object()),
    )
    monkeypatch.setattr(
        importer,
        "verify_database_capacity_lease",
        verify_lease,
    )
    monkeypatch.setattr(
        importer,
        "_assert_provider_directory_profile_capacity_tablespaces",
        Mock(),
    )
    monkeypatch.setattr(
        importer,
        "assert_database_capacity_lease_reservation",
        Mock(),
    )

    assert importer._verified_admission_lease(
        execution,
        database_identity,
        geometry,
    ) is verified_lease
    verify_lease.assert_called_once_with(
        execution.capacity_attestation,
        trust=ANY,
        now=ANY,
        expected_capacity_geometry_hash="1" * 64,
        expected_database_system_identifier="system-1",
        expected_database_oid=42,
        expected_database_name="healthporta",
    )


def test_capacity_geometry_persistence_recomputes_hash_and_rejects_drift():
    geometry, geometry_hash, geometry_json = (
        _capacity_geometry_identity()
    )
    assert importer._provider_directory_profile_capacity_geometry_identity(
        status="verified",
        geometry_hash=geometry_hash,
        geometry_json=geometry_json,
    ) == ("verified", geometry_hash, geometry_json)

    changed_payload = capacity.capacity_geometry_payload(geometry)
    changed_payload["max_profile_rows"] += 1
    with pytest.raises(RuntimeError, match="geometry_hash_invalid"):
        importer._provider_directory_profile_capacity_geometry_identity(
            status="verified",
            geometry_hash=geometry_hash,
            geometry_json=changed_payload,
        )
    with pytest.raises(RuntimeError, match="legacy_geometry_invalid"):
        importer._provider_directory_profile_capacity_geometry_identity(
            status="legacy_unavailable",
            geometry_hash=None,
            geometry_json={},
        )


def test_checkpoint_refuses_same_plan_with_different_capacity_geometry():
    geometry, geometry_hash, geometry_json = (
        _capacity_geometry_identity()
    )
    plan = importer._ProviderDirectoryProfileBatchPlan(
        has_existing_artifacts=True,
        include_copy_batch=False,
        evidence_batches=(),
        compact_batches=(),
        fingerprint=geometry.executable_plan_hash,
        materialization_mode="source_delta",
    )
    build = importer._ProviderDirectoryProfileBuild(
        schema="mrf",
        generation_id="pdprofile_" + "1" * 32,
        source_ids=(),
        retained_source_ids=("source-a",),
        dataset_ids=(),
        profile_as_of=geometry.profile_as_of,
        evidence_stage="evidence-stage",
        profile_stage="profile-stage",
        batch_plan=plan,
        materialization_mode="source_delta",
        capacity_geometry_status="verified",
        capacity_geometry_hash=geometry_hash,
        capacity_geometry_json=geometry_json,
    )
    checkpoint_by_field = {
        "executable_plan_hash": plan.fingerprint,
        "materialization_mode": "source_delta",
        "evidence_total_batches": 0,
        "profile_total_batches": 0,
        "capacity_geometry_status": "verified",
        "capacity_geometry_hash": geometry_hash,
        "capacity_geometry_json": geometry_json,
    }
    assert importer._is_profile_checkpoint_geometry_matching(
        checkpoint_by_field,
        build=build,
    )

    changed_payload = capacity.capacity_geometry_payload(geometry)
    changed_payload["max_profile_rows"] += 1
    changed_geometry = capacity.validated_capacity_geometry(
        changed_payload
    )
    checkpoint_by_field["capacity_geometry_hash"] = (
        capacity.capacity_geometry_hash(changed_geometry)
    )
    checkpoint_by_field["capacity_geometry_json"] = (
        capacity.canonical_capacity_geometry_json(changed_geometry)
    )
    assert not importer._is_profile_checkpoint_geometry_matching(
        checkpoint_by_field,
        build=build,
    )


@pytest.mark.asyncio
async def test_admitted_serving_state_never_adopts_missing_singleton(
    monkeypatch,
):
    adoption = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_serving_state",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        importer,
        "_adopt_provider_directory_profile_serving_generation",
        adoption,
    )

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="serving_generation_missing",
    ):
        await importer._provider_directory_profile_delta_serving_state(
            "mrf",
            allow_adoption=False,
        )

    adoption.assert_not_awaited()


@pytest.mark.asyncio
async def test_serving_bootstrap_reads_existing_singleton_without_adoption(
    monkeypatch,
):
    serving_state = object()
    serving_lookup = AsyncMock(return_value=serving_state)
    adoption = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_serving_state",
        serving_lookup,
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_delta_serving_state",
        adoption,
    )

    observed = (
        await importer._bootstrap_provider_directory_profile_serving_generation(
            "mrf"
        )
    )

    assert observed is serving_state
    serving_lookup.assert_awaited_once_with("mrf", for_update=False)
    adoption.assert_not_awaited()


@pytest.mark.asyncio
async def test_serving_bootstrap_adopts_only_missing_singleton(
    monkeypatch,
):
    serving_state = object()
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_serving_state",
        AsyncMock(return_value=None),
    )
    adoption = AsyncMock(return_value=serving_state)
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_delta_serving_state",
        adoption,
    )

    observed = (
        await importer._bootstrap_provider_directory_profile_serving_generation(
            "mrf"
        )
    )

    assert observed is serving_state
    adoption.assert_awaited_once_with("mrf", allow_adoption=True)


@pytest.mark.asyncio
async def test_capacity_admission_requires_authoritative_control_run_id():
    with pytest.raises(RuntimeError, match="capacity_run_id_invalid"):
        await importer._admit_provider_directory_profile_capacity(
            run_id="run_" + "1" * 32,
            control_run_id="run_" + "2" * 32,
            execution=None,
            fence=None,
            resource_fence=None,
            artifact_resource_types=frozenset(),
        )


@pytest.mark.asyncio
async def test_capacity_admission_rejects_same_run_replay_before_planning(
    monkeypatch,
):
    """A consumed run needs terminal disposition and a fresh child lease."""
    run_id = "run_" + "1" * 32
    identity_lookup = AsyncMock()
    monkeypatch.setattr(importer.db, "scalar", AsyncMock(return_value=True))
    monkeypatch.setattr(
        importer,
        "_profile_admission_identity",
        identity_lookup,
    )

    with pytest.raises(
        RuntimeError,
        match="same_run_capacity_replay_unsupported",
    ):
        await importer._admit_provider_directory_profile_capacity(
            run_id=run_id,
            control_run_id=run_id,
            execution=_execution(),
            fence=None,
            resource_fence=None,
            artifact_resource_types=frozenset(),
        )

    identity_lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_capacity_control_run_lock_requires_active_provider_directory(
    monkeypatch,
):
    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(
            return_value=SimpleNamespace(
                _mapping={
                    "run_id": "run_" + "1" * 32,
                    "importer": "provider-directory-fhir",
                    "status": "running",
                }
            )
        ),
    )
    await importer._lock_provider_directory_profile_capacity_control_run(
        schema="mrf",
        run_id="run_" + "1" * 32,
    )

    importer.db.first.return_value = SimpleNamespace(
        _mapping={
            "run_id": "run_" + "1" * 32,
            "importer": "provider-directory-fhir",
            "status": "canceling",
        }
    )
    with pytest.raises(RuntimeError, match="control_run_invalid"):
        await importer._lock_provider_directory_profile_capacity_control_run(
            schema="mrf",
            run_id="run_" + "1" * 32,
        )


@pytest.mark.asyncio
async def test_capacity_acceptance_uses_post_lock_database_clock(
    monkeypatch,
):
    accepted_at = datetime.datetime(
        2026,
        7,
        30,
        12,
        tzinfo=datetime.timezone.utc,
    )
    lease = SimpleNamespace(
        expires_at=accepted_at + datetime.timedelta(hours=1),
        max_build_deadline=accepted_at + datetime.timedelta(minutes=30),
    )
    first = AsyncMock(
        return_value=SimpleNamespace(
            _mapping={
                "accepted_at": accepted_at,
                "deadline_open": True,
            }
        )
    )
    monkeypatch.setattr(importer.db, "first", first)
    assert (
        await importer._provider_directory_profile_capacity_acceptance_time(
            lease
        )
        == accepted_at
    )
    assert "clock_timestamp()" in first.await_args.args[0]

    first.return_value = SimpleNamespace(
        _mapping={
            "accepted_at": accepted_at,
            "deadline_open": False,
        }
    )
    with pytest.raises(
        importer.ProviderDirectoryCapacityLeaseError,
        match="deadline_reached",
    ):
        await importer._provider_directory_profile_capacity_acceptance_time(
            lease
        )


@pytest.mark.asyncio
async def test_current_profile_dataset_map_covers_absent_and_present_rows(
    monkeypatch,
):
    first = AsyncMock(side_effect=[None, _published_dataset_state()])
    monkeypatch.setattr(importer.db, "first", first)

    assert await importer._current_profile_dataset_map("pdfhir_payer") == {}
    assert await importer._current_profile_dataset_map("pdfhir_payer") == (
        _published_dataset_state()
    )


@pytest.mark.asyncio
async def test_source_local_profile_followup_requires_exact_current_dataset(
    monkeypatch,
):
    monkeypatch.setattr(
        importer.profile_artifact,
        "configured_profile_source_ids",
        lambda: ("pdfhir_payer",),
    )
    current_dataset = AsyncMock(return_value={})
    monkeypatch.setattr(importer, "_current_profile_dataset_map", current_dataset)
    assert await importer._source_local_profile_followup_if_current(
        source_ids=[],
        expected_acquisition_root_run_id="run-root-1",
    ) is None
    assert await importer._source_local_profile_followup_if_current(
        source_ids=["pdfhir_payer"],
        expected_acquisition_root_run_id=None,
    ) is None
    assert await importer._source_local_profile_followup_if_current(
        source_ids=["not-configured"],
        expected_acquisition_root_run_id="run-root-1",
    ) is None
    assert await importer._source_local_profile_followup_if_current(
        source_ids=["pdfhir_payer"],
        expected_acquisition_root_run_id="run-root-1",
    ) is None

    current_dataset.return_value = _published_dataset_state()
    followup = await importer._source_local_profile_followup_if_current(
        source_ids=["pdfhir_payer"],
        expected_acquisition_root_run_id="run-root-1",
    )
    assert followup["dataset_id"] == "dataset-1"
    assert followup["parent_run_id"] == "run-root-1"


@pytest.mark.asyncio
async def test_attested_profile_fence_covers_purge_and_publish(monkeypatch):
    purge_execution = SimpleNamespace(
        attestation=SimpleNamespace(operation="purge")
    )
    purge_fence = await importer._attested_profile_publication_fence(
        run_id="run-purge",
        metrics={},
        execution=purge_execution,
        source_ids=[],
    )
    assert purge_fence.datasets == ()

    expected_fence = importer.ProviderDirectoryArtifactDatasetFence(())
    prepare = AsyncMock(return_value=expected_fence)
    monkeypatch.setattr(
        importer,
        "_prepare_artifact_publication_fence",
        prepare,
    )
    observed_fence = await importer._attested_profile_publication_fence(
        run_id="run-publish",
        metrics={},
        execution=_execution(),
        source_ids=["pdfhir_payer"],
    )
    assert observed_fence is expected_fence
    prepare.assert_awaited_once()
