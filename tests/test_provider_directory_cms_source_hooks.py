# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Owner capability placement and awaited receipt hooks; component proof only."""

import asyncio
from contextlib import asynccontextmanager, contextmanager
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from process import network_registry_cms_prepared_pair as retained
from process import provider_directory_cms_preparation as preparation
from process import provider_directory_cms_publication as publication
from process import provider_directory_cms_serving as serving
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_registry_cms_source_attempt import RegistryCMSSourceAttempt
from process.provider_directory_cms_source_runtime import (
    BoundCMSRegistrySourceJob,
    CMSRegistrySourceInvocation,
    CMSRegistrySourceRuntime,
    bind_claimed_cms_registry_source_runtime,
)
from tests.provider_directory_cms_capacity_test_support import cms_execution
from tests.test_provider_directory_cms_preflight import _retention_policy
from tests.test_provider_directory_cms_source_runtime import source_case


def _bound_job():
    policy = _retention_policy(cms_execution())
    request = retained.RegistryCMSRetentionRequest(
        PinnedFHIRMembershipSource(**policy["source_pin"]),
        RegistryNetworkSourceCoordinates(**policy["binding_coordinates"]),
        policy["selection_proof_id"],
        policy["expected_admission_sha256"],
        policy["expected_metadata_sha256"],
        policy["extra_data_upper_bound_bytes"],
        policy["extra_wal_upper_bound_bytes"],
    )
    return BoundCMSRegistrySourceJob(
        request,
        UUID(policy["capture_id"]),
        policy["owner_role"],
        tuple(policy["runtime_roles"]),
        Mock(),
        AsyncMock(),
        RegistryCMSSourceAttempt("run", "run:" + "a" * 32, "2026-10-08T10:00:00+00:00"),
        read_receipt=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_historical_work_bypasses_capability_but_retention_requires_it():
    historical = SimpleNamespace(cms_nonprofile_admission={})
    assert await serving._authenticated_source_job(None, object(), historical, object()) is None
    enrolled = SimpleNamespace(cms_nonprofile_admission={"registry_source_retention": {}})
    with pytest.raises(RuntimeError, match="cms_registry_source_runtime_required"):
        await serving._authenticated_source_job(None, object(), enrolled, object())
    with pytest.raises(RuntimeError, match="cms_registry_source_runtime_invalid"):
        await serving._authenticated_source_job({}, object(), enrolled, object())


def _retention_scope_case(monkeypatch, capture_failure):
    """Install ownership assertions at the exact address and Profile boundaries."""
    events = []
    job = _bound_job()
    source_pair, address = object(), object()
    prepared = SimpleNamespace(
        fhir=object(),
        execution=object(),
        fence=object(),
        relation_overrides={},
        overlay_identity=object(),
        nonprofile_admission=SimpleNamespace(registry_source_job=job),
        archive_delta=None,
        metrics={},
        assert_ready=AsyncMock(),
        registry_source_pair=None,
        source_session_factory=None,
    )

    @asynccontextmanager
    async def address_factory(*args, **kwargs):
        events.append("address-ready")
        try:
            yield address
        finally:
            events.append("address-closed")

    async def capture(actual_prepared, actual_factory, request, **options):
        assert actual_prepared is prepared and prepared.address is address
        assert actual_factory is address_factory and request is job.retention_request
        assert options == dict(
            capture_id=job.capture_id,
            owner_role=job.owner_role,
            runtime_roles=job.runtime_roles,
            source_session_factory=job.source_session_factory,
            source_attempt=job.source_attempt,
        )
        events.append("source-retained")
        if capture_failure:
            raise RuntimeError("synthetic retention failure")
        return source_pair

    @asynccontextmanager
    async def profile(*args):
        assert prepared.registry_source_pair is source_pair
        assert prepared.source_session_factory is job.source_session_factory
        events.append("profile-resumed")
        yield object(), {"profile": {"prepared": True}}

    monkeypatch.setattr(retained, "prepare_registry_cms_source_pair", capture)
    monkeypatch.setattr(preparation, "_profile_scope", profile)
    monkeypatch.setattr(preparation, "_emit_prepared_manifest", AsyncMock())
    return prepared, events, address_factory


@pytest.mark.asyncio
@pytest.mark.parametrize("capture_failure", [False, True])
async def test_source_retention_finishes_before_profile_scope(monkeypatch, capture_failure):
    prepared, events, address_factory = _retention_scope_case(monkeypatch, capture_failure)
    operation = preparation._prepare_address_and_profile(
        prepared, run_id="run", control_run_id="run", metrics={}, address_preparation=address_factory
    )
    if capture_failure:
        with pytest.raises(RuntimeError, match="synthetic retention failure"):
            async with operation:
                pytest.fail("failed retention exposed preparation")
        assert events == ["address-ready", "source-retained", "address-closed"]
    else:
        async with operation:
            assert events == ["address-ready", "source-retained", "profile-resumed"]
        assert events[-1] == "address-closed"


def _receipt_binding_case():
    from dataclasses import replace

    from tests.test_cms_registry_source_pair_slot import _prepared

    job, session, retained_pair, expected_payload = _bound_job(), object(), object(), {}
    source_pair = replace(_prepared(), source_attempt=job.source_attempt)
    job = replace(
        job,
        retention_request=source_pair.request,
        capture_id=source_pair.recipe.source_pin.retained_epoch.epoch_id,
        owner_role=source_pair.owner_role,
        runtime_roles=source_pair.runtime_roles,
        source_attempt=source_pair.source_attempt,
    )
    prepared = SimpleNamespace(
        nonprofile_admission=SimpleNamespace(
            registry_source_job=job,
            plan=SimpleNamespace(
                capacity_geometry_hash=source_pair.capacity_geometry_hash,
                native_address_input_hash=source_pair.native_address_input_hash,
            ),
            lease=SimpleNamespace(lease_digest=source_pair.lease_digest),
        ),
        registry_source_pair=source_pair,
    )
    return job, session, retained_pair, expected_payload, prepared


@pytest.mark.asyncio
@pytest.mark.parametrize("notify_failure", [False, True])
async def test_receipt_binding_and_notification_share_the_passed_session(monkeypatch, notify_failure):
    job, session, retained_pair, expected_payload, prepared = _receipt_binding_case()
    events = []

    @asynccontextmanager
    async def scope(actual_session, actual_prepared, receipt_id, receipt_payload):
        assert actual_session is session and actual_prepared is prepared
        events.append("authenticated")
        yield retained_pair

    @asynccontextmanager
    async def publisher(actual_session):
        assert actual_session is session
        events.append("publisher")
        try:
            yield
        finally:
            events.append("owner-restored")

    async def bind(actual_session, pair, *, receipt_id, receipt_payload):
        assert actual_session is session and pair is prepared.registry_source_pair
        assert receipt_id == "receipt" and receipt_payload is expected_payload
        events.append("bound")
        return retained_pair

    async def notify(actual_session, *, receipt_id, pair):
        assert actual_session is session and pair is retained_pair and receipt_id == "receipt"
        events.append("notified")
        if notify_failure:
            raise RuntimeError("synthetic notification failure")

    job.notify_receipt.side_effect = notify
    from process import provider_directory_cms_source_runtime as source_runtime
    from process import provider_directory_generic_source_policy as source_policy

    monkeypatch.setattr(source_runtime, "registry_source_publication", scope)
    monkeypatch.setattr(source_policy, "source_publisher_body", publisher)
    monkeypatch.setattr(retained, "bind_prepared_registry_cms_source_pair", bind)
    operation = publication._bind_registry_source_receipt(session, prepared, "receipt", expected_payload)
    if notify_failure:
        with pytest.raises(RuntimeError, match="synthetic notification failure"):
            await operation
    else:
        await operation
    assert events == ["authenticated", "bound", "publisher", "notified", "owner-restored"]
    job.notify_receipt.assert_awaited_once()


@pytest.mark.asyncio
async def test_retention_hook_rejects_a_missing_prepared_pair():
    prepared = SimpleNamespace(
        nonprofile_admission=SimpleNamespace(registry_source_job=_bound_job()), registry_source_pair=None
    )
    with pytest.raises(RuntimeError, match="cms_registry_source_runtime_invalid"):
        await publication._bind_registry_source_receipt(object(), prepared, "receipt", {})


@contextmanager
def _claimed_source_context(monkeypatch, case):
    """Supply component claim evidence; native ownership is tested by the bootstrap."""
    from process.live_progress import (
        reset_live_progress_context,
        set_live_progress_context,
    )

    attempt = case.bound.source_attempt
    target_job = "provider_directory_start_" + attempt.run_id
    monkeypatch.setenv("HLTHPRT_WORKER_ONCE_TARGET_JOB_ID", target_job)
    authenticator = AsyncMock(return_value=case.bound)
    binder = AsyncMock(return_value=authenticator)
    runtime = CMSRegistrySourceRuntime(AsyncMock(side_effect=ValueError("unbound")), binder)
    context_by_field = {
        "control_run_id": attempt.run_id,
        "job_id": target_job,
        "cms_registry_source_runtime": runtime,
        "context": {
            "control_run_id": attempt.run_id,
            "_control_attempt_id": attempt.attempt_id,
            "_control_attempt_started_at": attempt.attempt_started_at,
        },
    }
    task_by_field = {"run_id": attempt.run_id, "parameters": {"selected": ["original"]}}
    control_by_field = {
        "run_id": attempt.run_id,
        "importer": "provider-directory-fhir",
        "target_module": "process.provider_directory_fhir",
        "target_function": "process_data",
        "run_shutdown": True,
        "task": task_by_field,
    }
    token = set_live_progress_context(importer="provider-directory-fhir", **attempt.as_dict())
    try:
        yield context_by_field, control_by_field, task_by_field, binder, authenticator
    finally:
        reset_live_progress_context(token)


async def _authenticate_claim(context, task, case):
    invocation = CMSRegistrySourceInvocation.from_context(context, task)
    return await invocation.bind_verified_job(case.execution, case.request, case.lease)


@pytest.mark.asyncio
async def test_claim_binds_once_and_preserves_original_attempt_and_notification(monkeypatch, source_case):
    with _claimed_source_context(monkeypatch, source_case) as (context, control, task, binder, authenticate):
        await bind_claimed_cms_registry_source_runtime(context, control, task)
        assert context["cms_registry_source_runtime"].bind_claimed_attempt is None
        binder.assert_awaited_once()
        assert binder.await_args.kwargs["context"] is context
        assert binder.await_args.kwargs["task"] == task and binder.await_args.kwargs["task"] is not task
        assert binder.await_args.kwargs["source_attempt"] == source_case.bound.source_attempt
        bound = await _authenticate_claim(context, task, source_case)
        assert bound is source_case.bound
        assert bound.notify_receipt is source_case.bound.notify_receipt
        authenticate.assert_awaited_once()
        with pytest.raises(ValueError, match="claim_invalid"):
            await bind_claimed_cms_registry_source_runtime(context, control, task)
        binder.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "context_copy",
        "nested_copy",
        "attempt",
        "started_at",
        "run_id",
        "task_copy",
        "task_original",
        "job_id",
        "environment",
        "live",
        "control",
        "runtime",
    ],
)
async def test_claim_rejects_original_invocation_substitution(monkeypatch, source_case, mutation):
    from process.live_progress import (
        reset_live_progress_context,
        set_live_progress_context,
    )

    with _claimed_source_context(monkeypatch, source_case) as (context, control, task, _binder, authenticate):
        await bind_claimed_cms_registry_source_runtime(context, control, task)
        actual_context_by_field, actual_task, live_token = context, deepcopy(task), None
        original_invocation = CMSRegistrySourceInvocation.from_context(context, actual_task)
        match mutation:
            case "context_copy":
                actual_context_by_field = dict(context)
            case "nested_copy":
                context["context"] = dict(context["context"])
            case "attempt":
                context["context"]["_control_attempt_id"] = "run:" + "b" * 32
            case "started_at":
                context["context"]["_control_attempt_started_at"] = "2026-10-08T11:00:00+00:00"
            case "run_id":
                context["control_run_id"] = "other"
            case "task_copy":
                actual_task["parameters"]["selected"].append("forged")
            case "task_original":
                task["parameters"]["selected"].append("changed")
            case "job_id":
                context["job_id"] = "other"
            case "environment":
                monkeypatch.setenv("HLTHPRT_WORKER_ONCE_TARGET_JOB_ID", "other")
            case "live":
                live_token = set_live_progress_context(attempt_id="run:" + "b" * 32)
            case "control":
                control["target_function"] = "other"
            case _:
                context["cms_registry_source_runtime"] = CMSRegistrySourceRuntime(authenticate)
        try:
            with pytest.raises(ValueError, match="claim_changed"):
                if mutation == "runtime":
                    await original_invocation.bind_verified_job(
                        source_case.execution, source_case.request, source_case.lease
                    )
                else:
                    await _authenticate_claim(actual_context_by_field, actual_task, source_case)
        finally:
            if live_token is not None:
                reset_live_progress_context(live_token)
        authenticate.assert_not_awaited()


@pytest.mark.asyncio
async def test_claim_rejects_inherited_child_task_before_authentication(monkeypatch, source_case):
    with _claimed_source_context(monkeypatch, source_case) as (context, control, task, _binder, authenticate):
        await bind_claimed_cms_registry_source_runtime(context, control, task)
        with pytest.raises(ValueError, match="claim_changed"):
            await asyncio.create_task(_authenticate_claim(context, task, source_case))
        authenticate.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["importer", "target_module", "target_function", "call_style", "run_shutdown"])
async def test_claim_requires_fixed_cms_adapter_before_bootstrap(monkeypatch, source_case, field):
    with _claimed_source_context(monkeypatch, source_case) as (context, control, task, binder, authenticate):
        control[field] = False if field == "run_shutdown" else "other"
        with pytest.raises(ValueError, match="claim_invalid"):
            await bind_claimed_cms_registry_source_runtime(context, control, task)
        binder.assert_not_awaited()
        authenticate.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["bootstrap", "authenticate", "returned_attempt"])
async def test_claim_rechecks_after_awaited_bootstrap_and_authentication(monkeypatch, source_case, mutation):
    with _claimed_source_context(monkeypatch, source_case) as (context, control, task, binder, authenticate):

        async def drift(**_arguments):
            context["context"]["_control_attempt_id"] = "run:" + "b" * 32
            return authenticate if mutation == "bootstrap" else source_case.bound

        if mutation == "bootstrap":
            binder.side_effect = drift
        elif mutation == "authenticate":
            authenticate.side_effect = drift
        else:
            authenticate.return_value = replace(
                source_case.bound,
                source_attempt=RegistryCMSSourceAttempt(
                    "run", "run:" + "b" * 32, source_case.bound.source_attempt.attempt_started_at
                ),
            )
        with pytest.raises(ValueError, match="claim_changed"):
            await bind_claimed_cms_registry_source_runtime(context, control, task)
            await _authenticate_claim(context, task, source_case)


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", [None, 3, CMSRegistrySourceRuntime(AsyncMock())])
async def test_claim_requires_explicit_bootstrap_and_callable_authenticator(monkeypatch, source_case, invalid):
    with _claimed_source_context(monkeypatch, source_case) as (context, control, task, binder, authenticate):
        if isinstance(invalid, CMSRegistrySourceRuntime):
            context["cms_registry_source_runtime"] = invalid
        else:
            binder.return_value = invalid
        with pytest.raises(ValueError, match="claim_invalid"):
            await bind_claimed_cms_registry_source_runtime(context, control, task)
        authenticate.assert_not_awaited()
