# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure branch coverage for UHC/Profile importer boundary helpers."""

from __future__ import annotations

import datetime
import json
import types
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_profile_capacity_preflight_contract as contract
from tests.provider_directory_profile_capacity_signing_guard_test_support import (
    synthetic_profile_execution,
)
from tests.test_provider_directory_profile_capacity_preflight import (
    EXPIRES_AT,
    ISSUED_AT,
    _geometry,
    _quiescence,
    _receipt,
    _receipt_storage,
    _request,
    _runtime_observation,
    _serving_state,
    _transaction,
    importer,
)


@pytest.fixture(autouse=True)
def _import_node(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")


def _workload():
    request = _request()
    return types.SimpleNamespace(
        limits_payload=request.limits_payload,
        limits_sha256=request.limits_sha256,
        artifact_projection=types.SimpleNamespace(
            projected_rows=45,
            projected_logical_bytes=67_890,
            projection_hash="8" * 64,
        ),
    )


def _lease(receipt):
    return types.SimpleNamespace(
        nonce=receipt["receipt_sha256"],
        expires_at=EXPIRES_AT,
        observed_at=ISSUED_AT,
        issued_at=ISSUED_AT,
        attestation_id="attestation-1",
        signing_preflight_guard={
            "healthcare_receipt": dict(receipt),
            "healthcare_receipt_sha256": receipt["receipt_sha256"],
            "healthcare_request_sha256": receipt["request_sha256"],
            "control_plane_receipt_sha256": (receipt["control_plane_receipt_sha256"]),
            "capacity_limits_sha256": receipt["capacity_limits_sha256"],
        },
    )


def _receipt_row(receipt):
    values = importer._profile_capacity_preflight_receipt_values(
        _request(),
        receipt,
        issued_at=ISSUED_AT,
    )
    return {
        **values,
        "receipt_json": receipt,
        "consumed_at": None,
    }


@pytest.mark.asyncio
async def test_preflight_fences_cover_publish_and_nonpublish(monkeypatch):
    fence = object()
    resource_fence = object()
    resource_types = frozenset({"Practitioner"})
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
    backfill = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_assert_no_provider_directory_resource_id_npi_backfill_candidates",
        backfill,
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_artifact_resource_types",
        lambda *_args, **_kwargs: resource_types,
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_resource_scope_fence",
        AsyncMock(return_value=resource_fence),
    )

    for operation in ("publish", "rebuild"):
        execution = types.SimpleNamespace(
            attestation=types.SimpleNamespace(operation=operation)
        )
        assert await importer._profile_capacity_preflight_fences(
            execution, ["source-a"]
        ) == (fence, resource_fence, resource_types)
    backfill.assert_awaited_once_with(importer._schema(), ["source-a"])


@pytest.mark.asyncio
async def test_preflight_geometry_success_and_limits_drift(monkeypatch):
    fence = object()
    resource_fence = object()
    resource_types = frozenset({"Practitioner"})
    identity = object()
    workload = types.SimpleNamespace(limits_sha256="a" * 64)
    geometry_state = types.SimpleNamespace(control_wal_projection=object())
    monkeypatch.setattr(
        importer,
        "_profile_capacity_preflight_fences",
        AsyncMock(return_value=(fence, resource_fence, resource_types)),
    )
    monkeypatch.setattr(
        importer,
        "_profile_admission_identity",
        AsyncMock(return_value=identity),
    )
    monkeypatch.setattr(
        importer,
        "_profile_admission_workload",
        AsyncMock(return_value=workload),
    )
    monkeypatch.setattr(
        importer,
        "_profile_admission_inputs",
        lambda *_args: object(),
    )
    monkeypatch.setattr(
        importer,
        "_profile_admission_geometry",
        lambda *_args: geometry_state,
    )
    lock_projection = Mock()
    monkeypatch.setattr(
        importer,
        "_assert_admission_lock_projection",
        lock_projection,
    )
    request = types.SimpleNamespace(
        execution=object(),
        limits=object(),
        limits_sha256=workload.limits_sha256,
    )
    assert await importer._profile_capacity_preflight_geometry(
        request, ["source-a"], types.SimpleNamespace(state=object())
    ) == (workload, geometry_state)

    request.limits_sha256 = "b" * 64
    with pytest.raises(RuntimeError, match="preflight_limits_changed"):
        await importer._profile_capacity_preflight_geometry(
            request, ["source-a"], types.SimpleNamespace(state=object())
        )


@pytest.mark.asyncio
async def test_receipt_context_orchestration(monkeypatch):
    """Capture one runtime, clock, receipt layout, and quiescence context."""
    geometry = _geometry()
    runtime_observation = _runtime_observation(geometry)
    quiescence, quiescence_sha256 = _quiescence()
    workload = _workload()
    geometry_state = types.SimpleNamespace(geometry=geometry)
    serving = types.SimpleNamespace()
    request = types.SimpleNamespace(
        request_sha256="a" * 64,
        expires_at=EXPIRES_AT,
    )
    monkeypatch.setattr(
        importer,
        "observe_profile_runtime",
        AsyncMock(return_value=runtime_observation),
    )
    monkeypatch.setattr(
        importer,
        "assert_runtime_observation_matches_geometry",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        importer,
        "_profile_capacity_preflight_clock",
        AsyncMock(return_value=ISSUED_AT),
    )
    monkeypatch.setattr(
        importer,
        "_profile_capacity_preflight_receipt_layout",
        AsyncMock(return_value=_receipt_storage()),
    )
    monkeypatch.setattr(
        importer,
        "_profile_capacity_quiescence",
        AsyncMock(return_value=(quiescence, quiescence_sha256)),
    )
    monkeypatch.setattr(importer, "assert_preflight_expiry", lambda *_a, **_k: None)
    context = await importer._profile_capacity_receipt_context(
        "mrf",
        request,
        None,
        workload,
        geometry_state,
        serving,
    )
    assert context.runtime_observation == runtime_observation
    assert context.issued_at == ISSUED_AT


@pytest.mark.asyncio
async def test_receipt_snapshot_orchestration(monkeypatch):
    """Persist one exact preflight snapshot after all bounded inputs agree."""
    existing_row_by_field = {"existing": True}
    snapshot_request = types.SimpleNamespace(request_sha256="a" * 64)
    snapshot_workload = object()
    snapshot_geometry = object()
    snapshot_serving = object()
    snapshot_context = types.SimpleNamespace(
        issued_at=ISSUED_AT,
        observed_at=ISSUED_AT,
    )
    receipt_by_field = {"receipt_sha256": "b" * 64}
    monkeypatch.setattr(
        importer,
        "_profile_capacity_preflight_existing_receipt",
        AsyncMock(return_value=existing_row_by_field),
    )
    monkeypatch.setattr(
        importer,
        "_profile_capacity_preflight_clock",
        AsyncMock(return_value=ISSUED_AT),
    )
    monkeypatch.setattr(
        importer,
        "_profile_capacity_quiescence",
        AsyncMock(return_value=({}, "c" * 64)),
    )
    monkeypatch.setattr(
        importer,
        "_profile_capacity_preflight_serving",
        AsyncMock(return_value=snapshot_serving),
    )
    monkeypatch.setattr(
        importer,
        "_profile_capacity_preflight_geometry",
        AsyncMock(return_value=(snapshot_workload, snapshot_geometry)),
    )
    monkeypatch.setattr(
        importer,
        "_profile_capacity_receipt_context",
        AsyncMock(return_value=snapshot_context),
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_preflight_receipt",
        lambda *_args: receipt_by_field,
    )
    monkeypatch.setattr(
        importer,
        "_checked_serialized_metadata_payload_bytes",
        lambda *_args, **_kwargs: None,
    )
    persisted = AsyncMock(return_value=receipt_by_field)
    monkeypatch.setattr(importer, "_persist_or_replay_capacity_receipt", persisted)
    assert (
        await importer._profile_capacity_preflight_snapshot(
            snapshot_request, ["source-a"]
        )
        == receipt_by_field
    )


@pytest.mark.asyncio
async def test_legacy_adoption_candidate_orchestration(monkeypatch):
    result_by_field = {"result": True}
    selection_result_by_field = {"profile_generation_id": "generation"}
    row_counts = (10, 20)
    adoption_targets = object()
    source_vector = (("source-a", "dataset-a"),)
    source_context_vector = (("source-a", "a" * 64),)
    candidate = object()
    monkeypatch.setattr(
        importer,
        "_profile_adoption_result_row",
        AsyncMock(return_value=(result_by_field, selection_result_by_field)),
    )
    monkeypatch.setattr(
        importer,
        "_profile_adoption_attested_row_counts",
        lambda _result: row_counts,
    )
    monkeypatch.setattr(
        importer,
        "_profile_adoption_targets",
        AsyncMock(return_value=adoption_targets),
    )
    monkeypatch.setattr(
        importer,
        "_profile_adoption_vectors",
        AsyncMock(return_value=(source_vector, source_context_vector)),
    )
    monkeypatch.setattr(
        importer,
        "_profile_adoption_as_of",
        lambda _result: "2026-08-10",
    )
    monkeypatch.setattr(
        importer,
        "_profile_adoption_candidate",
        lambda *_args: candidate,
    )
    assert await importer._profile_adoption_candidate_from_legacy("mrf") is candidate


@pytest.mark.asyncio
async def test_delta_materialization_loads_missing_serving_override(monkeypatch):
    serving_state = _serving_state()
    desired = importer._ProviderDirectoryProfileDesiredIdentity(
        source_ids=["source-a"],
        dataset_ids=["dataset-a"],
        source_vector=(("source-a", "dataset-a"),),
        source_vector_hash="a" * 64,
        source_context_vector=(("source-a", "b" * 64),),
        source_context_vector_hash="c" * 64,
    )
    load_serving = AsyncMock(return_value=serving_state)
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_delta_serving_state",
        load_serving,
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_delta_sources",
        lambda *_args: (("source-a",), ()),
    )
    execution_token = importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.set(
        object()
    )
    try:
        identity = await importer._profile_materialization_identity(
            "mrf",
            desired,
            has_existing_artifacts=True,
            allow_serving_generation_adoption=True,
        )
    finally:
        importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.reset(execution_token)
    assert identity.materialization_mode == "source_delta"
    assert identity.serving_state == serving_state
    load_serving.assert_awaited_once_with("mrf", allow_adoption=True)

    execution_token = importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.set(
        object()
    )
    try:
        overridden = await importer._profile_materialization_identity(
            "mrf",
            desired,
            has_existing_artifacts=True,
            allow_serving_generation_adoption=False,
            serving_state_override=serving_state,
        )
    finally:
        importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.reset(execution_token)
    assert overridden.serving_state == serving_state
