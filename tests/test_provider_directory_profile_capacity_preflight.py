# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Authenticated, replay-fenced Profile capacity preflight tests."""

from __future__ import annotations

import datetime
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
from process import provider_directory_profile_capacity_preflight_contract as contract
from process import provider_directory_profile_capacity_runtime as capacity_runtime
from process import provider_directory_profile_runtime_observation as runtime
from process import provider_directory_profile_selection as selection
from tests.test_provider_directory_profile_capacity import _geometry_payload
from tests.test_provider_directory_profile_capacity_runtime import _limits_payload
from tests.provider_directory_profile_capacity_signing_guard_test_support import (
    SYNTHETIC_PROFILE_SOURCE_ID,
    synthetic_profile_execution,
)
from tests.provider_directory_profile_capacity_runtime_test_support import (
    PROFILE_RUNTIME_WITNESS_MIGRATION_REVISION,
)


importer = importlib.import_module("process.provider_directory_fhir")
UTC = datetime.timezone.utc
ISSUED_AT = datetime.datetime(2026, 8, 10, 9, 0, tzinfo=UTC)
EXPIRES_AT = ISSUED_AT + datetime.timedelta(hours=1)


def _execution_payload() -> dict[str, object]:
    execution = synthetic_profile_execution()
    return {
        **selection._GLOBAL_PROFILE_PARAMS,
        "provider_directory_profile_generation": execution.generation,
        "provider_directory_profile_selection_attestation": (
            execution.attestation.payload
        ),
        "provider_directory_profile_capacity_attestation": {},
    }


def _request_payload(**guard_overrides: object) -> dict[str, object]:
    guard_by_field = {
        "contract_id": contract.CAPACITY_SIGNING_GUARD_REQUEST_CONTRACT_ID,
        "request_nonce": "1" * 64,
        "control_plane_receipt_sha256": "2" * 64,
        "expires_at": contract.utc_second_text(EXPIRES_AT),
        **guard_overrides,
    }
    return {
        "contract_id": contract.CAPACITY_PREFLIGHT_REQUEST_CONTRACT_ID,
        "profile_execution": _execution_payload(),
        "provider_directory_profile_capacity_limits": _limits_payload(),
        "signing_guard": guard_by_field,
    }


def _request(**guard_overrides: object):
    return contract.validated_capacity_preflight_request(
        _request_payload(**guard_overrides)
    )


def _geometry():
    payload = _geometry_payload()
    payload["profile_schema_version"] = profile_artifact.PROFILE_SCHEMA_VERSION
    return capacity.validated_capacity_geometry(payload)


def _runtime_observation(geometry) -> dict[str, object]:
    return {
        "contract_id": runtime.PROFILE_RUNTIME_OBSERVATION_CONTRACT_ID,
        "healthcare_source_commit": "a" * 40,
        "profile_migration_revision": PROFILE_RUNTIME_WITNESS_MIGRATION_REVISION,
        "profile_schema_version": profile_artifact.PROFILE_SCHEMA_VERSION,
        "profile_strategy_version": (profile_artifact.PROFILE_BUILD_STRATEGY_VERSION),
        "postgres_server_version_num": geometry.postgres_server_version_num,
    }


def _serving_state(**overrides: object):
    execution = synthetic_profile_execution()
    source_vector = ((SYNTHETIC_PROFILE_SOURCE_ID, "synthetic-dataset-1"),)
    context_vector = ((SYNTHETIC_PROFILE_SOURCE_ID, "3" * 64),)
    state_by_field = {
        "status": "published",
        "operation": "publish",
        "control_generation": 10,
        "generation_id": "pdprofile_" + "4" * 32,
        "selection_proof_id": execution.attestation.proof_id,
        "authority_revision": execution.attestation.authority_revision,
        "profile_schema_version": profile_artifact.PROFILE_SCHEMA_VERSION,
        "profile_strategy_version": (profile_artifact.PROFILE_BUILD_STRATEGY_VERSION),
        "source_vector": source_vector,
        "source_vector_hash": (
            importer._provider_directory_profile_source_vector_hash(source_vector)
        ),
        "source_context_vector": context_vector,
        "source_context_vector_hash": (
            importer._provider_directory_profile_source_context_vector_hash(
                context_vector
            )
        ),
        "executable_plan_hash": "5" * 64,
        "evidence_target_oid": 20_001,
        "profile_target_oid": 20_002,
        "evidence_rows": 67,
        "profile_rows": 45,
        "profile_as_of": "2026-08-09",
        "published_at": "2026-08-09T08:00:00+00:00",
    }
    return importer._ProviderDirectoryProfileServingState(
        **{**state_by_field, **overrides}
    )


def _serving(resolution: str = "existing"):
    state = _serving_state()
    payload = importer._profile_capacity_preflight_serving_payload(
        state,
        resolution=resolution,
    )
    return importer._ProfileCapacityPreflightServing(
        state=state,
        payload=payload,
        payload_sha256=contract.preflight_domain_sha256(
            contract.CAPACITY_SERVING_PREFLIGHT_DIGEST_DOMAIN,
            payload,
        ),
    )


def _quiescence() -> tuple[dict[str, object], str]:
    payload = {
        "contract_id": contract.CAPACITY_QUIESCENCE_CONTRACT_ID,
        "active_profile_run_count": 0,
        "claimed_profile_checkpoint_count": 0,
        "unexpired_capacity_consumption_count": 0,
        "outstanding_preflight_receipt_count": 0,
        "active_profile_run_statuses": list(importer._PROFILE_ACTIVE_RUN_STATUSES),
        "claimed_checkpoint_states": [
            "building_evidence",
            "evidence_complete",
            "building_profile",
            "ready",
        ],
        "capacity_consumption_boundary": "unexpired",
        "preflight_receipt_boundary": "unconsumed_and_unexpired",
    }
    return payload, contract.preflight_domain_sha256(
        contract.CAPACITY_QUIESCENCE_DIGEST_DOMAIN,
        payload,
    )


def _receipt_storage(**overrides: object) -> dict[str, object]:
    return {
        "contract_id": (
            "healthporta.provider-directory-profile-capacity-" "preflight-storage.v1"
        ),
        "relation_oid": 20_008,
        "exact_fingerprint": "6" * 64,
        "structural_fingerprint": "7" * 64,
        "main_index_pages": [1, 1, 1, 1],
        "toast_index_pages": [1],
        "toastable_column_count": 1,
        "effective_tablespace_oids": [1663],
        **overrides,
    }


def _receipt():
    request = _request()
    geometry = _geometry()
    quiescence, quiescence_sha256 = _quiescence()
    workload = types.SimpleNamespace(
        limits_payload=request.limits_payload,
        limits_sha256=request.limits_sha256,
        artifact_projection=types.SimpleNamespace(
            projected_rows=45,
            projected_logical_bytes=67_890,
            projection_hash="8" * 64,
        ),
    )
    context = importer._ProfileCapacityReceiptContext(
        workload=workload,
        geometry_state=importer._ProfileAdmissionGeometry(
            geometry=geometry,
            control_wal_projection=types.SimpleNamespace(),
        ),
        runtime_observation=_runtime_observation(geometry),
        serving=_serving(),
        quiescence=quiescence,
        quiescence_sha256=quiescence_sha256,
        receipt_storage=_receipt_storage(),
        issued_at=ISSUED_AT,
        observed_at=ISSUED_AT,
    )
    return importer._provider_directory_profile_capacity_preflight_receipt(
        request, context
    )


def test_request_binds_closed_limits_and_exact_v6_source_delta_identity(
    monkeypatch,
):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    request = _request()

    assert request.execution.attestation.profile_strategy_version == (
        profile_artifact.PROFILE_BUILD_STRATEGY_VERSION
    )
    assert (
        contract.profile_execution_identity_payload(request)["materialization_mode"]
        == "source_delta"
    )
    assert request.limits_payload == _limits_payload()
    assert request.limits_sha256 == contract.capacity_limits_sha256(_limits_payload())
    assert request.request_sha256 == contract.preflight_domain_sha256(
        contract.CAPACITY_PREFLIGHT_REQUEST_CONTRACT_ID,
        request.request_payload,
    )
    contract.assert_preflight_expiry(request, issued_at=ISSUED_AT)

    changed_payload = _request_payload()
    changed_payload["provider_directory_profile_capacity_limits"][
        "minimum_remaining_bytes"
    ] += 1
    changed = contract.validated_capacity_preflight_request(changed_payload)
    assert changed.limits_sha256 != request.limits_sha256
    assert changed.request_sha256 != request.request_sha256


@pytest.mark.parametrize("mutation", ("extra", "lease", "expiry"))
def test_request_rejects_open_or_noncanonical_input(monkeypatch, mutation):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    payload = _request_payload()
    if mutation == "extra":
        payload["unexpected"] = True
    elif mutation == "lease":
        payload["profile_execution"][
            "provider_directory_profile_capacity_attestation"
        ] = {"lease": {}}
    else:
        payload["signing_guard"]["expires_at"] = "2026-08-10T09:00:00+00:00"

    with pytest.raises(
        (
            contract.ProviderDirectoryProfileCapacityPreflightError,
            selection.ProviderDirectoryProfileSelectionError,
        )
    ):
        contract.validated_capacity_preflight_request(payload)


def test_receipt_hash_binds_limits_quiescence_and_storage(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    receipt = _receipt()
    supplied_sha256 = receipt.pop("receipt_sha256")

    assert set(receipt) | {"receipt_sha256"} == (
        importer._PROFILE_CAPACITY_PREFLIGHT_RECEIPT_FIELDS
    )
    assert supplied_sha256 == contract.preflight_domain_sha256(
        contract.CAPACITY_PREFLIGHT_CONTRACT_ID,
        receipt,
    )
    assert receipt["capacity_limits_sha256"] == (
        contract.capacity_limits_sha256(receipt["capacity_limits"])
    )
    assert (
        receipt["profile_execution_identity"]["profile_strategy_version"]
        == profile_artifact.PROFILE_BUILD_STRATEGY_VERSION
    )
    assert (
        receipt["profile_execution_identity"]["materialization_mode"] == "source_delta"
    )
    assert receipt["preflight_receipt_storage"]["relation_oid"] == 20_008


@pytest.mark.asyncio
async def test_missing_serving_state_projects_exact_legacy_adoption(
    monkeypatch,
):
    state = _serving_state()
    candidate = importer._ProfileAdoptionCandidate(
        generation_id=state.generation_id,
        selection_result={
            "generation": state.control_generation,
            "proof_id": state.selection_proof_id,
            "authority_revision": state.authority_revision,
            "profile_schema_version": state.profile_schema_version,
            "profile_strategy_version": state.profile_strategy_version,
        },
        source_vector=state.source_vector,
        source_context_vector=state.source_context_vector,
        profile_as_of=state.profile_as_of,
        profile_target_oid=state.profile_target_oid,
        evidence_target_oid=state.evidence_target_oid,
        profile_rows=state.profile_rows,
        evidence_rows=state.evidence_rows,
        published_at=state.published_at,
        executable_plan_hash=state.executable_plan_hash,
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_serving_state",
        AsyncMock(return_value=None),
    )
    candidate_loader = AsyncMock(return_value=candidate)
    monkeypatch.setattr(
        importer,
        "_profile_adoption_candidate_from_legacy",
        candidate_loader,
    )
    insert = AsyncMock()
    monkeypatch.setattr(importer, "_insert_profile_adoption", insert)

    serving = await importer._profile_capacity_preflight_serving("mrf")

    assert serving.state == state
    assert serving.payload["resolution"] == "legacy_adoption"
    assert serving.payload["profile_target_oid"] == state.profile_target_oid
    assert serving.payload_sha256 == contract.preflight_domain_sha256(
        contract.CAPACITY_SERVING_PREFLIGHT_DIGEST_DOMAIN,
        serving.payload,
    )
    candidate_loader.assert_awaited_once_with("mrf")
    insert.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_serving_state_without_legacy_success_fails_closed(
    monkeypatch,
):
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_serving_state",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        importer,
        "_profile_adoption_candidate_from_legacy",
        AsyncMock(
            side_effect=RuntimeError(
                "provider_directory_profile_serving_adoption_missing"
            )
        ),
    )

    with pytest.raises(RuntimeError, match="serving_adoption_missing"):
        await importer._profile_capacity_preflight_serving("mrf")


@asynccontextmanager
async def _transaction():
    yield


@pytest.mark.asyncio
async def test_preflight_serializes_request_before_issuing_receipt(
    monkeypatch,
):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    request = _request()
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "transaction", _transaction)
    monkeypatch.setattr(importer.db, "status", status)
    lock = AsyncMock()
    current = AsyncMock()
    snapshot = AsyncMock(return_value={"receipt_sha256": "9" * 64})
    monkeypatch.setattr(
        importer,
        "_lock_profile_capacity_preflight_state",
        lock,
    )
    monkeypatch.setattr(
        importer,
        "assert_profile_selection_current_in_transaction",
        current,
    )
    monkeypatch.setattr(
        importer,
        "_profile_capacity_preflight_snapshot",
        snapshot,
    )

    receipt = await importer.provider_directory_profile_capacity_preflight(request)

    assert receipt == {"receipt_sha256": "9" * 64}
    status.assert_awaited_once_with("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
    lock.assert_awaited_once_with("mrf")
    current.assert_awaited_once()
    snapshot.assert_awaited_once_with(request, [SYNTHETIC_PROFILE_SOURCE_ID])
    assert importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.get() is None


@pytest.mark.asyncio
async def test_exact_open_receipt_replays_and_nonce_reuse_fails(
    monkeypatch,
):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    request = _request()
    receipt = _receipt()
    receipt_values_by_field = importer._profile_capacity_preflight_receipt_values(
        request,
        receipt,
        issued_at=ISSUED_AT,
    )
    existing_by_field = {
        **receipt_values_by_field,
        "receipt_json": receipt,
        "consumed_at": None,
    }

    replayed = await importer._persist_or_replay_capacity_receipt(
        "mrf",
        request,
        receipt,
        existing_by_field,
        issued_at=ISSUED_AT,
        observed_at=ISSUED_AT + datetime.timedelta(seconds=1),
    )
    assert replayed == receipt

    changed_request = _request(control_plane_receipt_sha256="3" * 64)
    monkeypatch.setattr(
        importer.db,
        "all",
        AsyncMock(return_value=[existing_by_field]),
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="nonce_reused",
    ):
        await importer._profile_capacity_preflight_existing_receipt(
            "mrf",
            changed_request,
        )
