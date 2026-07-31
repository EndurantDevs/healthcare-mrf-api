# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Read-only capacity-preflight admission boundary tests."""

from __future__ import annotations

import hashlib
import importlib
import json
import types
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import BadRequest, SanicException

from api import provider_directory_profile_capacity_preflight as preflight_api
from process import provider_directory_profile as profile_artifact
from process import provider_directory_profile_capacity as capacity
from process import provider_directory_profile_runtime_observation as runtime
from tests.test_provider_directory_profile_capacity import _geometry_payload
from tests.test_provider_directory_profile_selection_attestation import (
    _execution,
)

importer = importlib.import_module("process.provider_directory_fhir")


def _geometry():
    payload = _geometry_payload()
    payload["profile_schema_version"] = (
        profile_artifact.PROFILE_SCHEMA_VERSION
    )
    return capacity.validated_capacity_geometry(payload)


def _workload():
    return types.SimpleNamespace(
        artifact_projection=types.SimpleNamespace(
            projected_rows=45,
            projected_logical_bytes=67_890,
            projection_hash="e" * 64,
        ),
    )


def _patch_fence_dependencies(monkeypatch, fence) -> None:
    monkeypatch.setattr(
        importer,
        "_resolve_provider_directory_artifact_datasets",
        AsyncMock(return_value=fence),
    )
    monkeypatch.setattr(
        importer,
        "_assert_profile_selection_matches_artifact_fence",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        importer,
        "_assert_no_provider_directory_resource_id_npi_backfill_candidates",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_artifact_resource_types",
        lambda *_args, **_kwargs: frozenset({"Organization"}),
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_resource_scope_fence",
        AsyncMock(return_value=fence),
    )


def _patch_geometry_dependencies(
    monkeypatch,
    workload,
    geometry_state,
) -> AsyncMock:
    monkeypatch.setattr(
        importer,
        "_profile_admission_identity",
        AsyncMock(return_value=types.SimpleNamespace()),
    )
    workload_builder = AsyncMock(return_value=workload)
    monkeypatch.setattr(
        importer,
        "_profile_admission_workload",
        workload_builder,
    )
    monkeypatch.setattr(
        importer,
        "_profile_admission_inputs",
        lambda *_args: types.SimpleNamespace(),
    )
    monkeypatch.setattr(
        importer,
        "_profile_admission_geometry",
        lambda *_args: geometry_state,
    )
    monkeypatch.setattr(
        importer,
        "_assert_admission_lock_projection",
        lambda *_args: None,
    )
    return workload_builder


def _assert_runtime_receipt(receipt, geometry) -> None:
    expected_observation_by_field = {
        "contract_id": runtime.PROFILE_RUNTIME_OBSERVATION_CONTRACT_ID,
        "healthcare_source_commit": "a" * 40,
        "profile_migration_revision": (
            "20260730110000_provider_directory_profile_delta"
        ),
        "profile_schema_version": profile_artifact.PROFILE_SCHEMA_VERSION,
        "profile_strategy_version": (
            profile_artifact.PROFILE_BUILD_STRATEGY_VERSION
        ),
        "postgres_server_version_num": geometry.postgres_server_version_num,
    }
    assert receipt["contract_id"] == (
        "healthporta.provider-directory-profile-capacity-preflight.v2"
    )
    assert receipt["runtime_observation"] == expected_observation_by_field
    receipt_by_field_without_hash = dict(receipt)
    receipt_hash = receipt_by_field_without_hash.pop("receipt_sha256")
    expected_hash = hashlib.sha256(
        b"healthporta.provider-directory-profile-capacity-preflight.v2\0"
        + json.dumps(
            receipt_by_field_without_hash,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    ).hexdigest()
    assert receipt_hash == expected_hash


@asynccontextmanager
async def _read_only_transaction():
    yield


def _patch_runtime_snapshot(monkeypatch, geometry):
    status = AsyncMock()
    runtime_rows = AsyncMock(
        return_value=[
            {
                "profile_migration_revision": (
                    "20260730110000_provider_directory_profile_delta"
                ),
                "postgres_server_version_num": (
                    geometry.postgres_server_version_num
                ),
            }
        ]
    )
    monkeypatch.setattr(
        importer.db,
        "transaction",
        _read_only_transaction,
    )
    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(
        importer,
        "assert_profile_selection_current_in_transaction",
        AsyncMock(),
    )
    monkeypatch.setattr(
        runtime,
        "build_baked_healthcare_source_commit",
        lambda: "a" * 40,
    )
    monkeypatch.setattr(runtime.db, "all", runtime_rows)
    return status, runtime_rows


@pytest.mark.asyncio
async def test_preflight_computes_geometry_inside_read_only_snapshot(
    monkeypatch,
):
    """Preflight must bind exact geometry inside one read-only snapshot."""

    execution = _execution()
    fence = importer.ProviderDirectoryArtifactDatasetFence(())
    workload = _workload()
    geometry_state = importer._ProfileAdmissionGeometry(
        geometry=_geometry(),
        control_wal_projection=types.SimpleNamespace(),
    )
    status, runtime_rows = _patch_runtime_snapshot(
        monkeypatch,
        geometry_state.geometry,
    )
    _patch_fence_dependencies(monkeypatch, fence)
    workload_builder = _patch_geometry_dependencies(
        monkeypatch,
        workload,
        geometry_state,
    )

    receipt = await importer.provider_directory_profile_capacity_preflight(
        execution
    )

    status.assert_awaited_once_with(
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;"
    )
    assert len(workload_builder.await_args.args) == 4
    assert receipt["capacity_geometry_hash"] == (
        capacity.capacity_geometry_hash(geometry_state.geometry)
    )
    assert receipt["artifact_scope_projection"]["projected_rows"] == 45
    _assert_runtime_receipt(receipt, geometry_state.geometry)
    runtime_rows.assert_awaited_once_with(
        runtime.profile_runtime_observation_sql()
    )
    assert (
        importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.get()
        is None
    )


@pytest.mark.asyncio
async def test_preflight_rejects_invalid_execution():
    with pytest.raises(
        importer.ProviderDirectoryProfileSelectionError,
        match="Profile capacity preflight execution is invalid",
    ):
        await importer.provider_directory_profile_capacity_preflight(object())


@pytest.mark.asyncio
async def test_control_preflight_requires_empty_lease_and_returns_receipt(
    monkeypatch,
):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    execution = _execution()
    request = types.SimpleNamespace(
        headers={"Authorization": "Bearer secret"},
        json={
            "provider_directory_profile_capacity_attestation": {},
        },
    )
    monkeypatch.setattr(
        preflight_api,
        "validated_profile_execution",
        lambda _payload: execution,
    )
    runner = AsyncMock(return_value={"receipt_sha256": "f" * 64})
    monkeypatch.setattr(
        preflight_api,
        "provider_directory_profile_capacity_preflight",
        runner,
    )

    response = await (
        preflight_api.control_provider_directory_profile_capacity_preflight(
            request
        )
    )

    assert json.loads(response.body) == {"receipt_sha256": "f" * 64}
    runner.assert_awaited_once_with(execution)
    request.json["provider_directory_profile_capacity_attestation"] = {
        "lease": {}
    }
    with pytest.raises(BadRequest, match="must be empty"):
        await (
            preflight_api.control_provider_directory_profile_capacity_preflight(
                request
            )
        )


@pytest.mark.asyncio
async def test_control_preflight_maps_selection_drift_to_conflict(monkeypatch):
    """A stale registered proof must return a deterministic conflict."""

    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    request = types.SimpleNamespace(
        headers={"Authorization": "Bearer secret"},
        json={
            "provider_directory_profile_capacity_attestation": {},
        },
    )
    monkeypatch.setattr(
        preflight_api,
        "validated_profile_execution",
        lambda _payload: _execution(),
    )
    monkeypatch.setattr(
        preflight_api,
        "provider_directory_profile_capacity_preflight",
        AsyncMock(
            side_effect=(
                preflight_api.ProviderDirectoryProfileSelectionStale(
                    "provider_directory_profile_selection_stale"
                )
            )
        ),
    )

    with pytest.raises(SanicException) as conflict:
        await (
            preflight_api.control_provider_directory_profile_capacity_preflight(
                request
            )
        )
    assert conflict.value.status_code == 409


@pytest.mark.asyncio
async def test_control_preflight_maps_runtime_observation_failure_to_conflict(
    monkeypatch,
):
    """Missing build provenance must fail closed with a stable conflict."""

    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    request = types.SimpleNamespace(
        headers={"Authorization": "Bearer secret"},
        json={
            "provider_directory_profile_capacity_attestation": {},
        },
    )
    monkeypatch.setattr(
        preflight_api,
        "validated_profile_execution",
        lambda _payload: _execution(),
    )
    monkeypatch.setattr(
        preflight_api,
        "provider_directory_profile_capacity_preflight",
        AsyncMock(
            side_effect=(
                runtime.ProviderDirectoryProfileRuntimeObservationError(
                    "provider_directory_profile_runtime_observation_"
                    "healthcare_source_commit_invalid"
                )
            )
        ),
    )

    with pytest.raises(SanicException) as conflict:
        await (
            preflight_api.control_provider_directory_profile_capacity_preflight(
                request
            )
        )
    assert conflict.value.status_code == 409
    assert str(conflict.value) == (
        "provider_directory_profile_runtime_observation_"
        "healthcare_source_commit_invalid"
    )
