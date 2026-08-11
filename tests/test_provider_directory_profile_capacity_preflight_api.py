# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Authenticated API and atomic receipt-consumption preflight tests."""

from __future__ import annotations

import datetime
import json
import types
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import BadRequest, SanicException

from api import provider_directory_profile_capacity_preflight as preflight_api
from process import provider_directory_profile_capacity as capacity
from process import provider_directory_profile_capacity_preflight_contract as contract
from process import provider_directory_profile_capacity_runtime as capacity_runtime
from process import provider_directory_profile_runtime_observation as runtime
from tests.provider_directory_profile_capacity_signing_guard_test_support import (
    synthetic_profile_execution,
)
from tests.test_provider_directory_profile_capacity_preflight import (
    EXPIRES_AT,
    ISSUED_AT,
    _geometry,
    _receipt,
    _receipt_storage,
    _request,
    _request_payload,
    _transaction,
    importer,
)


@pytest.mark.asyncio
async def test_receipt_storage_oid_drift_blocks_atomic_consume(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    receipt_by_field = _receipt()
    receipt_row_by_field = {
        "receipt_sha256": receipt_by_field["receipt_sha256"],
        "receipt_json": receipt_by_field,
        "issued_at": ISSUED_AT,
        "expires_at": EXPIRES_AT,
        "consumed_at": None,
    }
    lease = types.SimpleNamespace(
        nonce=receipt_by_field["receipt_sha256"],
        expires_at=EXPIRES_AT,
        observed_at=ISSUED_AT,
        issued_at=ISSUED_AT,
    )
    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value=receipt_row_by_field))
    monkeypatch.setattr(
        importer,
        "_profile_capacity_preflight_clock",
        AsyncMock(return_value=ISSUED_AT + datetime.timedelta(seconds=1)),
    )
    monkeypatch.setattr(
        importer, "_assert_profile_capacity_preflight_static_binding", lambda *_args: None
    )
    monkeypatch.setattr(
        importer,
        "_profile_capacity_preflight_receipt_layout",
        AsyncMock(return_value=_receipt_storage(relation_oid=20_009)),
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="preflight_storage_changed",
    ):
        await importer._consume_profile_capacity_preflight_receipt(
            schema="mrf",
            run_id="run_" + "a" * 32,
            execution=synthetic_profile_execution(),
            identity=types.SimpleNamespace(),
            workload=types.SimpleNamespace(),
            geometry=_geometry(),
            lease=lease,
            runtime_observation={},
        )


@pytest.mark.asyncio
async def test_control_api_validates_closed_request_and_returns_receipt(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    raw_request = _request_payload()
    http_request = types.SimpleNamespace(
        headers={"Authorization": "Bearer secret"}, json=raw_request
    )
    runner = AsyncMock(return_value={"receipt_sha256": "f" * 64})
    monkeypatch.setattr(
        preflight_api, "provider_directory_profile_capacity_preflight", runner
    )
    response = await preflight_api.control_provider_directory_profile_capacity_preflight(
        http_request
    )
    assert json.loads(response.body) == {"receipt_sha256": "f" * 64}
    typed_request = runner.await_args.args[0]
    assert isinstance(
        typed_request, contract.ProviderDirectoryProfileCapacityPreflightRequest
    )
    assert typed_request.request_payload == raw_request
    raw_request["profile_execution"][
        "provider_directory_profile_capacity_attestation"
    ] = {"lease": {}}
    with pytest.raises(BadRequest, match="attestation_not_empty"):
        await preflight_api.control_provider_directory_profile_capacity_preflight(
            http_request
        )


def _install_receipt_only_route_stubs(monkeypatch, writes):
    async def status(statement: str, **insert_values: object):
        if " ".join(statement.split()).upper().startswith("SET TRANSACTION"):
            return None
        writes.append((statement, insert_values))
        return "INSERT 0 1"

    async def snapshot(request, _source_ids):
        return await importer._persist_or_replay_capacity_receipt(
            "mrf",
            request,
            _receipt(),
            None,
            issued_at=ISSUED_AT,
            observed_at=ISSUED_AT,
        )

    monkeypatch.setattr(importer.db, "transaction", _transaction)
    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(importer, "_lock_profile_capacity_preflight_state", AsyncMock())
    monkeypatch.setattr(
        importer, "assert_profile_selection_current_in_transaction", AsyncMock()
    )
    monkeypatch.setattr(importer, "_profile_capacity_preflight_snapshot", snapshot)
    monkeypatch.setattr(
        preflight_api,
        "provider_directory_profile_capacity_preflight",
        importer.provider_directory_profile_capacity_preflight,
    )


@pytest.mark.asyncio
async def test_control_api_only_writes_the_exact_preflight_receipt(monkeypatch):
    """The control route may persist a receipt, but no acquisition/publication."""

    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    http_request = types.SimpleNamespace(
        headers={"Authorization": "Bearer secret"}, json=_request_payload()
    )
    writes: list[tuple[str, dict[str, object]]] = []
    _install_receipt_only_route_stubs(monkeypatch, writes)
    response = await preflight_api.control_provider_directory_profile_capacity_preflight(
        http_request
    )
    assert json.loads(response.body) == _receipt()
    assert len(writes) == 1
    statement, insert_values = writes[0]
    assert (
        "INSERT INTO " + importer._profile_capacity_preflight_receipt_ref("mrf")
    ) in statement
    expected_values = importer._profile_capacity_preflight_receipt_values(
        _request(), _receipt(), issued_at=ISSUED_AT
    )
    assert insert_values == expected_values


@pytest.mark.asyncio
async def test_control_api_maps_selection_and_runtime_drift_to_conflict(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    http_request = types.SimpleNamespace(
        headers={"Authorization": "Bearer secret"}, json=_request_payload()
    )
    failures = (
        preflight_api.ProviderDirectoryProfileSelectionStale(
            "provider_directory_profile_selection_stale"
        ),
        runtime.ProviderDirectoryProfileRuntimeObservationError(
            "provider_directory_profile_runtime_observation_healthcare_source_commit_invalid"
        ),
    )
    for failure in failures:
        monkeypatch.setattr(
            preflight_api,
            "provider_directory_profile_capacity_preflight",
            AsyncMock(side_effect=failure),
        )
        with pytest.raises(SanicException) as conflict:
            await preflight_api.control_provider_directory_profile_capacity_preflight(
                http_request
            )
        assert conflict.value.status_code == 409


def test_configured_limits_remain_the_admission_boundary(monkeypatch):
    """The signed document must still equal the deployed admission limits."""

    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    request = _request()
    configured = capacity_runtime.validated_capacity_limits(request.limits_payload)
    assert contract.canonical_capacity_limits_payload(configured) == request.limits_payload
