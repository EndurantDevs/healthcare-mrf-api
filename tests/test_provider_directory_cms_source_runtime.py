# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Specific source capability transport over actual signed CMS request fixtures."""

import asyncio
from copy import deepcopy
from dataclasses import fields, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from process import provider_directory_profile_capacity_preflight_contract as preflight
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_registry_cms_prepared_pair import RegistryCMSRetentionRequest
from process.network_registry_cms_source_attempt import RegistryCMSSourceAttempt
from process.provider_directory_cms_source_runtime import (
    BoundCMSRegistrySourceJob,
    CMSRegistrySourceInvocation,
    CMSRegistrySourceRuntime,
)
from tests.provider_directory_cms_capacity_test_support import cms_execution, cms_guard, rehash_guard, sign_guard
from tests.test_provider_directory_cms_preflight import _retention_policy
from tests.test_provider_directory_profile_capacity_attestation import _verify


@pytest.fixture
def source_case(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    execution = cms_execution()
    policy = _retention_policy(execution)
    guard = cms_guard(execution=execution)
    for name in ("control_plane_request", "healthcare_request"):
        guard[name]["cms_nonprofile_admission"]["registry_source_retention"] = deepcopy(policy)
    rehash_guard(guard)
    lease = _verify(
        sign_guard(guard, cms=True),
        expected_capacity_geometry_hash=guard["healthcare_receipt"]["capacity_geometry_hash"],
    )
    request = preflight.validated_capacity_preflight_request(guard["healthcare_request"])
    retention = RegistryCMSRetentionRequest(
        PinnedFHIRMembershipSource(**policy["source_pin"]),
        RegistryNetworkSourceCoordinates(**policy["binding_coordinates"]),
        policy["selection_proof_id"],
        policy["expected_admission_sha256"],
        policy["expected_metadata_sha256"],
        policy["extra_data_upper_bound_bytes"],
        policy["extra_wal_upper_bound_bytes"],
    )
    bound = BoundCMSRegistrySourceJob(
        retention,
        UUID(policy["capture_id"]),
        policy["owner_role"],
        tuple(policy["runtime_roles"]),
        Mock(name="source_session_factory"),
        AsyncMock(name="notify_receipt"),
        RegistryCMSSourceAttempt("run", "run:" + "a" * 32, "2026-10-08T10:00:00+00:00"),
    )
    return SimpleNamespace(execution=execution, request=request, lease=lease, bound=bound)


def _invocation(callback, task=None):
    context_by_field = {"cms_registry_source_runtime": CMSRegistrySourceRuntime(callback), "attempt": object()}
    return CMSRegistrySourceInvocation.from_context(context_by_field, task or {"operation": "refresh"})


async def _bind(invocation, case):
    return await invocation.bind_verified_job(case.execution, case.request, case.lease)


@pytest.mark.asyncio
async def test_bind_transports_original_payload_and_actual_attempt_handles(source_case):
    task_by_field = {"refresh_preset": "desired", "parameters": {"selected": ["original"]}}
    original = deepcopy(task_by_field)
    callback = AsyncMock(return_value=source_case.bound)
    invocation = _invocation(callback, task_by_field)
    task_by_field["parameters"]["selected"].append("expanded")
    task_by_field["refresh_preset"] = "expanded"
    result = await _bind(invocation, source_case)
    arguments = callback.await_args.kwargs
    assert result is source_case.bound
    assert arguments["context"] is invocation.context
    assert arguments["task"] == original
    assert arguments["task"] is not task_by_field and arguments["task"] is not invocation.task
    assert arguments["execution"] is source_case.execution
    assert arguments["request"] is source_case.request
    assert arguments["lease"] is source_case.lease
    source_case.bound.source_session_factory.assert_not_called()
    source_case.bound.notify_receipt.assert_not_awaited()


@pytest.mark.asyncio
async def test_callback_cannot_change_later_original_payload_snapshot(source_case):
    transported_tasks = []

    async def authenticate(**arguments):
        transported_tasks.append(deepcopy(arguments["task"]))
        arguments["task"]["parameters"]["selected"].append("callback")
        return source_case.bound

    invocation = _invocation(authenticate, {"parameters": {"selected": ["original"]}})
    await _bind(invocation, source_case)
    await _bind(invocation, source_case)
    assert transported_tasks == [{"parameters": {"selected": ["original"]}}] * 2


@pytest.mark.parametrize("wire_value", [None, {}, "module:function", CMSRegistrySourceRuntime(AsyncMock())])
def test_wire_task_field_does_not_create_capability(wire_value):
    assert CMSRegistrySourceInvocation.from_context({}, {"cms_registry_source_runtime": wire_value}) is None


@pytest.mark.parametrize("internal_value", [None, {}, "module:function", SimpleNamespace(authenticate_job=AsyncMock())])
def test_present_invalid_internal_runtime_fails_closed(internal_value):
    with pytest.raises(ValueError, match="invocation_invalid"):
        CMSRegistrySourceInvocation.from_context({"cms_registry_source_runtime": internal_value}, {})


def test_runtime_subclass_and_non_dict_original_task_fail_closed():
    class DerivedRuntime(CMSRegistrySourceRuntime):
        pass

    for runtime, task in ((DerivedRuntime(AsyncMock()), {}), (CMSRegistrySourceRuntime(AsyncMock()), [])):
        with pytest.raises(ValueError, match="invocation_invalid"):
            CMSRegistrySourceInvocation.from_context({"cms_registry_source_runtime": runtime}, task)
    with pytest.raises(ValueError, match="runtime_invalid"):
        CMSRegistrySourceRuntime(None)


@pytest.mark.parametrize("admission", [None, {}, {"registry_source_retention": None}])
@pytest.mark.asyncio
async def test_missing_or_invalid_policy_rejects_before_authentication(source_case, admission):
    callback = AsyncMock(return_value=source_case.bound)
    source_case.request = replace(source_case.request, cms_nonprofile_admission=admission)
    with pytest.raises(ValueError, match="policy_required|retention_policy_invalid"):
        await _bind(_invocation(callback), source_case)
    callback.assert_not_awaited()


@pytest.mark.parametrize("field", ["source_session_factory", "notify_receipt"])
def test_bound_requires_callable_handles(source_case, field):
    with pytest.raises(ValueError, match="handles_required"):
        replace(source_case.bound, **{field: None})


@pytest.mark.parametrize(
    "change",
    [
        {"capture_id": UUID(int=0)},
        {"capture_id": "00000000-0000-0000-0000-000000000001"},
        {"runtime_roles": ("writer", "reader")},
        {"runtime_roles": ("reader", "reader")},
        {"runtime_roles": ("retained_owner",)},
        {"retention_request": SimpleNamespace()},
    ],
)
def test_bound_reuses_exact_retention_input_validation(source_case, change):
    with pytest.raises(ValueError):
        replace(source_case.bound, **change)


@pytest.mark.asyncio
async def test_forged_callback_result_type_is_rejected(source_case):
    class DerivedBound(BoundCMSRegistrySourceJob):
        pass

    subclass = DerivedBound(**{item.name: getattr(source_case.bound, item.name) for item in fields(source_case.bound)})
    for result in (SimpleNamespace(**vars(source_case.bound)), subclass, None):
        with pytest.raises(ValueError, match="job_invalid"):
            await _bind(_invocation(AsyncMock(return_value=result)), source_case)


@pytest.mark.asyncio
async def test_unattributed_callback_result_is_rejected(source_case):
    bound = replace(source_case.bound, source_attempt=None)
    with pytest.raises(ValueError, match="source_attempt_required"):
        await _bind(_invocation(AsyncMock(return_value=bound)), source_case)
    source_case.bound.source_session_factory.assert_not_called()


@pytest.mark.parametrize("field", ["capture_id", "owner_role", "runtime_roles", "retention_request"])
@pytest.mark.asyncio
async def test_returned_job_policy_must_equal_verified_request(source_case, field):
    changes_by_field = {
        "capture_id": UUID(int=2),
        "owner_role": "other_owner",
        "runtime_roles": ("other_reader", "writer"),
        "retention_request": replace(source_case.bound.retention_request, extra_data_upper_bound_bytes=8192),
    }
    bound = replace(source_case.bound, **{field: changes_by_field[field]})
    with pytest.raises(ValueError, match="policy_changed"):
        await _bind(_invocation(AsyncMock(return_value=bound)), source_case)


def _changed_execution(execution, field):
    if field == "generation":
        return replace(execution, generation=execution.generation + 1)
    attestation = execution.attestation
    if field == "proof_id":
        return replace(execution, attestation=replace(attestation, proof_id="d" * 64))
    payload = deepcopy(attestation.payload)
    if field == "as_of":
        payload["desired_profile_as_of"] = "2026-07-29"
    else:
        payload["desired_cms_dataset"][field] = "changed"
    return replace(execution, attestation=replace(attestation, payload=payload))


@pytest.mark.parametrize(
    "field", ["generation", "proof_id", "as_of", "source_id", "endpoint_id", "dataset_id", "dataset_hash"]
)
@pytest.mark.parametrize("location", ["execution", "request"])
@pytest.mark.asyncio
async def test_captured_and_live_selection_must_agree(source_case, field, location):
    callback = AsyncMock(return_value=source_case.bound)
    if location == "execution":
        source_case.execution = _changed_execution(source_case.execution, field)
    else:
        source_case.request = replace(
            source_case.request, execution=_changed_execution(source_case.request.execution, field)
        )
    with pytest.raises(ValueError, match="selection_changed"):
        await _bind(_invocation(callback), source_case)
    callback.assert_not_awaited()


@pytest.mark.parametrize("mutation", ["policy", "selection", "handle"])
@pytest.mark.asyncio
async def test_callback_drift_is_checked_again_before_return(source_case, mutation):
    async def authenticate(**arguments):
        if mutation == "policy":
            arguments["request"].cms_nonprofile_admission["registry_source_retention"]["capture_id"] = str(UUID(int=2))
        elif mutation == "selection":
            arguments["execution"].attestation.desired_cms_dataset["dataset_hash"] = "d" * 64
        else:
            object.__setattr__(source_case.bound, "notify_receipt", None)
        return source_case.bound

    with pytest.raises(ValueError, match="policy_changed|selection_changed|handles_required"):
        await _bind(_invocation(authenticate), source_case)


@pytest.mark.parametrize("error", [RuntimeError("authentication unavailable"), asyncio.CancelledError()])
@pytest.mark.asyncio
async def test_authentication_errors_and_cancellation_propagate_without_publication(source_case, error):
    callback = AsyncMock(side_effect=error)
    with pytest.raises(type(error)) as caught:
        await _bind(_invocation(callback), source_case)
    assert caught.value is error
    source_case.bound.source_session_factory.assert_not_called()
    source_case.bound.notify_receipt.assert_not_awaited()


@pytest.mark.asyncio
async def test_synchronous_authenticator_cannot_supply_unawaited_bound_job(source_case):
    with pytest.raises(TypeError):
        await _bind(_invocation(Mock(return_value=source_case.bound)), source_case)


@pytest.mark.parametrize("field", ["execution", "request", "lease"])
@pytest.mark.asyncio
async def test_unverified_input_types_fail_before_callback(source_case, field):
    callback = AsyncMock(return_value=source_case.bound)
    setattr(source_case, field, SimpleNamespace(**vars(getattr(source_case, field))))
    with pytest.raises(ValueError, match="verified_inputs_required"):
        await _bind(_invocation(callback), source_case)
    callback.assert_not_awaited()


@pytest.mark.asyncio
async def test_specific_handles_remain_hidden_and_preserve_awaited_notification(source_case):
    callback = AsyncMock(return_value=source_case.bound)
    invocation = _invocation(callback, {"private_attempt_value": object()})
    bound = await _bind(invocation, source_case)
    assert repr(invocation) == "CMSRegistrySourceInvocation()"
    assert repr(invocation.runtime) == "CMSRegistrySourceRuntime()"
    assert "source_session_factory" not in repr(bound) and "notify_receipt" not in repr(bound)
    assert replace(bound, source_session_factory=Mock(), notify_receipt=AsyncMock()) == bound
    session, pair = object(), object()
    await bound.notify_receipt(session, receipt_id=7, pair=pair)
    bound.notify_receipt.assert_awaited_once_with(session, receipt_id=7, pair=pair)
