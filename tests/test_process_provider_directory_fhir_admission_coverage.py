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
async def test_receipt_consumption_orchestration(monkeypatch):
    """Consume one stored receipt after every closed guard succeeds."""
    receipt = _receipt()
    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value={}))
    monkeypatch.setattr(
        importer,
        "_profile_capacity_preflight_stored_receipt",
        lambda *_args: receipt,
    )
    monkeypatch.setattr(
        importer,
        "_profile_capacity_preflight_clock",
        AsyncMock(return_value=ISSUED_AT),
    )
    monkeypatch.setattr(
        importer,
        "_assert_profile_capacity_receipt_open",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        importer,
        "_assert_profile_capacity_preflight_static_binding",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        importer,
        "_assert_profile_capacity_receipt_storage",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_assert_profile_capacity_receipt_serving",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        importer,
        "_assert_profile_capacity_receipt_state",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_mark_profile_capacity_receipt_consumed",
        AsyncMock(),
    )
    await importer._consume_profile_capacity_preflight_receipt(
        schema="mrf",
        run_id="run-1",
        execution=synthetic_profile_execution(),
        identity=types.SimpleNamespace(),
        workload=types.SimpleNamespace(),
        geometry=_geometry(),
        lease=_lease(receipt),
        runtime_observation={},
    )


@pytest.mark.asyncio
async def test_admission_runtime_state_orchestration(monkeypatch):
    """Return the exact database identity and runtime observation."""
    receipt = _receipt()
    database_identity = object()
    runtime_observation_by_field = {"contract_id": "runtime"}
    monkeypatch.setattr(importer.db, "status", AsyncMock())
    monkeypatch.setattr(
        importer,
        "_admission_database_guard",
        AsyncMock(return_value=database_identity),
    )
    monkeypatch.setattr(
        importer,
        "observe_profile_runtime",
        AsyncMock(return_value=runtime_observation_by_field),
    )
    monkeypatch.setattr(
        importer,
        "assert_runtime_observation_matches_geometry",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        importer,
        "assert_capacity_lease_matches_runtime_observation",
        lambda *_args: None,
    )
    observed_identity, observed_runtime = (
        await importer._profile_admission_runtime_state(
            types.SimpleNamespace(),
            types.SimpleNamespace(),
            _geometry(),
            _lease(receipt),
        )
    )
    assert observed_identity is database_identity
    assert observed_runtime == runtime_observation_by_field


@pytest.mark.asyncio
async def test_admission_transaction_orchestration(monkeypatch):
    """Serialize one receipt and lease consumption transaction."""
    observed_identity = types.SimpleNamespace(wal_lsn="0/100")
    serving_state = object()
    monkeypatch.setattr(importer.db, "transaction", _transaction)
    monkeypatch.setattr(importer.db, "status", AsyncMock())
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_selection_catalog",
        Mock(return_value=object()),
    )
    for name in (
        "_lock_profile_capacity_preflight_state",
        "_lock_provider_directory_profile_capacity_control_run",
        "assert_profile_selection_current_in_transaction",
        "_consume_profile_capacity_preflight_receipt",
        "_assert_admission_run_toast",
        "_consume_admission_values",
    ):
        monkeypatch.setattr(importer, name, AsyncMock())
    monkeypatch.setattr(
        importer,
        "_locked_profile_admission_serving_state",
        AsyncMock(return_value=serving_state),
    )
    monkeypatch.setattr(
        importer,
        "_assert_provider_directory_profile_capacity_serving_state",
        Mock(),
    )
    monkeypatch.setattr(
        importer,
        "_profile_admission_runtime_state",
        AsyncMock(return_value=(observed_identity, {"runtime": True})),
    )
    identity = types.SimpleNamespace(serving_state=serving_state)
    workload = types.SimpleNamespace(
        database_identity=object(),
        control_wal_plan_input=object(),
    )
    assert (
        await importer._consume_admission_transaction(
            "run-1",
            synthetic_profile_execution(),
            identity,
            object(),
            object(),
            workload,
            object(),
        )
        is observed_identity
    )


def _admission_entrypoint_context():
    serving_state = object()
    return types.SimpleNamespace(
        observed_identity=types.SimpleNamespace(wal_lsn="0/100"),
        serving_state=serving_state,
        admission_identity=types.SimpleNamespace(serving_state=serving_state),
        geometry_state=types.SimpleNamespace(
            geometry=object(),
            control_wal_projection=object(),
        ),
        admission_result=object(),
    )


def _patch_admission_entrypoint_dependencies(monkeypatch, admission_context):
    dependency_by_name = {
        "_validated_admission_run_id": Mock(return_value="run-1"),
        "_assert_profile_capacity_run_unconsumed": AsyncMock(),
        "_profile_capacity_preflight_serving": AsyncMock(
            return_value=types.SimpleNamespace(state=admission_context.serving_state)
        ),
        "_profile_admission_identity": AsyncMock(
            return_value=admission_context.admission_identity
        ),
        "_profile_admission_workload": AsyncMock(
            return_value=types.SimpleNamespace(
                database_identity=admission_context.observed_identity
            )
        ),
        "_profile_admission_inputs": Mock(return_value=object()),
        "_profile_admission_geometry": Mock(
            return_value=admission_context.geometry_state
        ),
        "_verified_admission_lease": Mock(return_value=object()),
        "_assert_admission_lock_projection": Mock(),
        "_profile_admission_binding": Mock(return_value=("build-1", object())),
        "_consume_admission_transaction": AsyncMock(
            return_value=admission_context.observed_identity
        ),
        "_profile_admission_result": Mock(
            return_value=admission_context.admission_result
        ),
    }
    for dependency_name, dependency in dependency_by_name.items():
        monkeypatch.setattr(importer, dependency_name, dependency)


@pytest.mark.asyncio
async def test_admission_entrypoint_orchestration(monkeypatch):
    """Bind one admission request through the public capacity entrypoint."""
    admission_context = _admission_entrypoint_context()
    _patch_admission_entrypoint_dependencies(monkeypatch, admission_context)
    assert (
        await importer._admit_provider_directory_profile_capacity(
            run_id="run-1",
            control_run_id="run-1",
            execution=synthetic_profile_execution(),
            fence=object(),
            resource_fence=object(),
            artifact_resource_types=frozenset({"Practitioner"}),
        )
        is admission_context.admission_result
    )
