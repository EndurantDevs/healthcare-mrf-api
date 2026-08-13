# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Read-only Provider Directory Profile capacity authority projection tests."""

from __future__ import annotations

import importlib
import json
from pathlib import Path
import types
from unittest.mock import AsyncMock, Mock

import pytest
from sanic.exceptions import BadRequest, Forbidden, SanicException
from sanic.models.server_types import ConnInfo

from api import provider_directory_profile_capacity_preflight as preflight_api
from process import provider_directory_profile_capacity as capacity
from process import provider_directory_profile_capacity_preflight_contract as contract
from process import provider_directory_profile_runtime_observation as runtime
from tests.provider_directory_profile_capacity_signing_guard_test_support import (
    SYNTHETIC_PROFILE_SOURCE_ID,
)
from tests.test_provider_directory_profile_capacity_preflight import (
    _execution_payload,
    _geometry,
    _runtime_observation,
    _serving,
    _transaction,
)
from tests.test_provider_directory_profile_capacity_runtime import _limits_payload


importer = importlib.import_module("process.provider_directory_fhir")
GOLDEN_PATH = (
    Path(__file__).parent
    / "fixtures/provider_directory_profile_capacity_authority_projection_v1.json"
)


def _conn_info(peername: tuple) -> ConnInfo:
    transport = Mock()
    transport.get_extra_info.side_effect = lambda name: {
        "sockname": ("127.0.0.1", 8080),
        "peername": peername,
    }.get(name)
    return ConnInfo(transport)


def _projection_payload() -> dict[str, object]:
    return {
        "contract_id": contract.CAPACITY_AUTHORITY_PROJECTION_REQUEST_CONTRACT_ID,
        "profile_execution": _execution_payload(),
        "provider_directory_profile_capacity_limits": _limits_payload(),
    }


def _projection_request():
    return contract.validated_capacity_authority_projection_request(
        _projection_payload()
    )


def _projection_geometry(request):
    """Match the production geometry's execution and reviewed-limit bindings."""

    geometry_by_field = capacity.capacity_geometry_payload(_geometry())
    execution_identity = contract.profile_execution_identity_payload(request)
    geometry_by_field.update(
        {
            field_name: request.limits_payload[field_name]
            for field_name in (
                "artifact_scope_batch_size",
                "pool_reserve_connections",
                "work_mem_bytes",
                "maintenance_work_mem_bytes",
                "temp_file_limit_bytes",
                "max_build_seconds",
                "statement_timeout_ms",
                "lock_timeout_ms",
                "minimum_remaining_bytes",
                "max_evidence_rows",
                "max_affected_npis",
                "max_profile_rows",
                "relation_byte_caps",
            )
        }
    )
    geometry_by_field.update(
        selection_proof_id=execution_identity["selection_proof_id"],
        profile_input_digest=execution_identity["profile_input_digest"],
        max_artifact_scope_rows=45,
    )
    return capacity.validated_capacity_geometry(geometry_by_field)


def test_authority_projection_request_is_closed_and_digest_bound(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    request = _projection_request()

    assert request.request_payload == _projection_payload()
    assert request.request_sha256 == contract.preflight_domain_sha256(
        contract.CAPACITY_AUTHORITY_PROJECTION_REQUEST_CONTRACT_ID,
        request.request_payload,
    )
    assert request.limits_payload == _limits_payload()

    invalid = _projection_payload()
    invalid["signing_guard"] = {}
    with pytest.raises(
        contract.ProviderDirectoryProfileCapacityPreflightError,
        match="authority_projection_request_fields_invalid",
    ):
        contract.validated_capacity_authority_projection_request(invalid)


def test_authority_projection_route_is_registered_as_post():
    blueprint = types.SimpleNamespace(add_route=Mock())

    preflight_api.register_profile_capacity_preflight_route(blueprint)

    blueprint.add_route.assert_any_call(
        preflight_api.control_profile_capacity_authority_projection,
        "/provider-directory/profile-capacity-authority-projection",
        methods=("POST",),
    )


@pytest.mark.parametrize(
    "peername",
    [
        ("127.0.0.1", 41234),
        ("::1", 41234, 0, 0),
        ("::ffff:127.0.0.1", 41234, 0, 0),
    ],
)
def test_socket_loopback_accepts_real_conn_info_peername(peername):
    conn_info = _conn_info(peername)
    assert isinstance(conn_info.client, str)
    assert conn_info.peername == peername

    preflight_api._require_socket_loopback(
        types.SimpleNamespace(conn_info=conn_info)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "peername",
    [
        ("10.0.0.8", 41234),
        ("not-an-ip", 41234),
    ],
)
async def test_authority_projection_rejects_forwarded_loopback_for_remote_peer(
    monkeypatch,
    peername,
):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    runner = AsyncMock(return_value={"authority_projection_sha256": "f" * 64})
    monkeypatch.setattr(
        preflight_api,
        "provider_directory_profile_capacity_authority_projection",
        runner,
    )
    remote_request = types.SimpleNamespace(
        headers={
            "Authorization": "Bearer secret",
            "X-Forwarded-For": "127.0.0.1",
        },
        json=_projection_payload(),
        conn_info=_conn_info(peername),
    )
    with pytest.raises(Forbidden, match="loopback"):
        await preflight_api.control_profile_capacity_authority_projection(
            remote_request
        )
    runner.assert_not_awaited()


@pytest.mark.asyncio
async def test_authority_projection_maps_invalid_request_to_bad_request(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    invalid_request = types.SimpleNamespace(
        headers={"Authorization": "Bearer secret"},
        json={},
        conn_info=_conn_info(("127.0.0.1", 41234)),
    )

    with pytest.raises(BadRequest, match="authority_projection_request_fields_invalid"):
        await preflight_api.control_profile_capacity_authority_projection(
            invalid_request
        )


@pytest.mark.asyncio
async def test_authority_projection_maps_stale_state_to_conflict(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    runner = AsyncMock(
        side_effect=preflight_api.ProviderDirectoryArtifactBuildStale("stale")
    )
    monkeypatch.setattr(
        preflight_api,
        "provider_directory_profile_capacity_authority_projection",
        runner,
    )
    stale_request = types.SimpleNamespace(
        headers={"Authorization": "Bearer secret"},
        json=_projection_payload(),
        conn_info=_conn_info(("127.0.0.1", 41234)),
    )

    with pytest.raises(SanicException) as raised:
        await preflight_api.control_profile_capacity_authority_projection(
            stale_request
        )
    assert raised.value.status_code == 409
    runner.assert_awaited_once()


@pytest.mark.asyncio
async def test_authority_projection_accepts_ipv6_loopback_peer(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    runner = AsyncMock(return_value={"authority_projection_sha256": "f" * 64})
    monkeypatch.setattr(
        preflight_api,
        "provider_directory_profile_capacity_authority_projection",
        runner,
    )
    loopback_request = types.SimpleNamespace(
        headers={"Authorization": "Bearer secret"},
        json=_projection_payload(),
        conn_info=_conn_info(("::1", 41234, 0, 0)),
    )
    projection_response = (
        await preflight_api.control_profile_capacity_authority_projection(
            loopback_request
        )
    )
    assert json.loads(projection_response.body) == {
        "authority_projection_sha256": "f" * 64
    }
    runner.assert_awaited_once()


def _projection_stubs():
    """Build observable projection dependencies without database access."""

    request = _projection_request()
    geometry = _projection_geometry(request)
    serving = _serving()
    workload = types.SimpleNamespace(
        limits_payload=request.limits_payload,
        limits_sha256=request.limits_sha256,
        artifact_projection=types.SimpleNamespace(
            projected_rows=45,
            projected_logical_bytes=67_890,
            projection_hash="8" * 64,
        ),
    )
    geometry_state = importer._ProfileAdmissionGeometry(
        geometry=geometry,
        control_wal_projection=types.SimpleNamespace(),
    )
    runtime_observation = _runtime_observation(geometry)
    return types.SimpleNamespace(
        request=request,
        geometry=geometry,
        serving=serving,
        runtime_observation=runtime_observation,
        status=AsyncMock(),
        current=AsyncMock(),
        geometry_loader=AsyncMock(return_value=(workload, geometry_state)),
        runtime_loader=AsyncMock(return_value=runtime_observation),
        runtime_match=Mock(),
        persist=AsyncMock(side_effect=AssertionError("receipt persisted")),
        consume=AsyncMock(side_effect=AssertionError("receipt consumed")),
    )


async def _project_with_stubs(monkeypatch):
    """Run one projection with every external dependency made observable."""

    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    observed = _projection_stubs()
    monkeypatch.setattr(importer.db, "transaction", _transaction)
    monkeypatch.setattr(importer.db, "status", observed.status)
    monkeypatch.setattr(
        importer,
        "assert_profile_selection_current_in_transaction",
        observed.current,
    )
    monkeypatch.setattr(
        importer,
        "_profile_capacity_preflight_serving",
        AsyncMock(return_value=observed.serving),
    )
    monkeypatch.setattr(
        importer,
        "_profile_capacity_preflight_geometry",
        observed.geometry_loader,
    )
    monkeypatch.setattr(importer, "observe_profile_runtime", observed.runtime_loader)
    monkeypatch.setattr(
        importer,
        "assert_runtime_observation_matches_geometry",
        observed.runtime_match,
    )
    monkeypatch.setattr(
        importer,
        "_persist_or_replay_capacity_receipt",
        observed.persist,
    )
    monkeypatch.setattr(
        importer,
        "_consume_profile_capacity_preflight_receipt",
        observed.consume,
    )
    observed.projection = (
        await importer.provider_directory_profile_capacity_authority_projection(
            observed.request
        )
    )
    return observed


@pytest.mark.asyncio
async def test_projection_transaction_is_read_only_and_receipt_free(monkeypatch):
    observed = await _project_with_stubs(monkeypatch)

    observed.status.assert_awaited_once_with(
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY;"
    )
    observed.current.assert_awaited_once()
    observed.geometry_loader.assert_awaited_once_with(
        observed.request,
        [SYNTHETIC_PROFILE_SOURCE_ID],
        observed.serving,
    )
    observed.runtime_loader.assert_awaited_once_with()
    observed.runtime_match.assert_called_once_with(
        observed.runtime_observation,
        observed.geometry,
    )
    observed.persist.assert_not_awaited()
    observed.consume.assert_not_awaited()
    assert importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.get() is None


@pytest.mark.asyncio
async def test_projection_response_is_closed_and_self_hashed(monkeypatch):
    observed = await _project_with_stubs(monkeypatch)
    projection = observed.projection

    assert set(projection) == {
        "contract_id",
        "request_contract_id",
        "request_sha256",
        "profile_execution_identity",
        "capacity_limits",
        "capacity_limits_sha256",
        "capacity_geometry_hash",
        "capacity_geometry",
        "required_reservation_bytes_by_storage_class",
        "artifact_scope_projection",
        "runtime_observation",
        "serving_generation_preflight",
        "serving_generation_preflight_sha256",
        "authority_projection_sha256",
    }
    supplied_projection_sha256 = projection.pop("authority_projection_sha256")
    assert supplied_projection_sha256 == contract.preflight_domain_sha256(
        contract.CAPACITY_AUTHORITY_PROJECTION_CONTRACT_ID,
        projection,
    )
    assert projection["capacity_geometry_hash"] == capacity.capacity_geometry_hash(
        observed.geometry
    )


@pytest.mark.asyncio
async def test_projection_matches_cross_repository_golden(monkeypatch):
    observed = await _project_with_stubs(monkeypatch)
    expected_bytes = (
        contract.canonical_preflight_json(
            {
                "request": _projection_payload(),
                "projection": observed.projection,
            }
        ).encode("ascii")
        + b"\n"
    )

    assert GOLDEN_PATH.read_bytes() == expected_bytes
