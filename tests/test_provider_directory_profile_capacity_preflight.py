# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Read-only capacity-preflight admission boundary tests."""

from __future__ import annotations

import json
import importlib
import types
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import BadRequest, SanicException

from api import provider_directory_profile_capacity_preflight as preflight_api
from process import provider_directory_profile_capacity as capacity
from tests.test_provider_directory_profile_capacity import _geometry_payload
from tests.test_provider_directory_profile_selection_attestation import (
    _execution,
)

importer = importlib.import_module("process.provider_directory_fhir")


def _geometry():
    return capacity.validated_capacity_geometry(_geometry_payload())


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

    @asynccontextmanager
    async def transaction():
        yield

    monkeypatch.setattr(importer.db, "transaction", transaction)
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(
        importer,
        "assert_profile_selection_current_in_transaction",
        AsyncMock(),
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
    assert len(receipt["receipt_sha256"]) == 64
    assert (
        importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.get()
        is None
    )


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
