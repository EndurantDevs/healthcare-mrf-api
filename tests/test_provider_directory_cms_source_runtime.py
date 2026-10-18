# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Specific source capability transport over actual signed CMS request fixtures."""

import asyncio
from copy import deepcopy
from dataclasses import fields, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from process import provider_directory_profile_capacity_attestation as capacity_lease
from process import provider_directory_profile_capacity_preflight_contract as preflight
from process import provider_directory_profile_capacity_signing_guard as guard_contract
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
def source_case(monkeypatch, request):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    execution = cms_execution()
    policy = _retention_policy(execution)
    if getattr(request, "param", None) is not None:
        policy["source_pin"]["as_of"] = request.param
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
        read_receipt=AsyncMock(name="read_receipt"),
    )
    return SimpleNamespace(execution=execution, request=request, lease=lease, bound=bound)


def _invocation(callback, task=None):
    context_by_field = {"cms_registry_source_runtime": CMSRegistrySourceRuntime(callback), "attempt": object()}
    return CMSRegistrySourceInvocation.from_context(context_by_field, task or {"operation": "refresh"})


async def _bind(invocation, case):
    return await invocation.bind_verified_job(case.execution, case.request, case.lease)


@pytest.mark.parametrize("handle", [None, 3, "not-a-factory"])
def test_retention_handle_rejects_noncallable_or_source_pool(source_case, handle):
    if handle is None:
        handle = source_case.bound.source_session_factory
    with pytest.raises(ValueError, match="retention_handles_required"):
        replace(source_case.bound, retention_session_factory=handle)


@pytest.mark.asyncio
async def test_source_preparation_requires_separate_retention_handle(monkeypatch, source_case):
    from process import network_registry_cms_prepared_pair as retention
    from process import provider_directory_cms_preparation as preparation
    from process import provider_directory_generic_source_policy as policy

    prepare = AsyncMock()
    monkeypatch.setattr(retention, "prepare_registry_cms_source_pair", prepare)
    prepared = SimpleNamespace(nonprofile_admission=SimpleNamespace(registry_source_job=source_case.bound))
    async with policy.bind_generic_source_transaction_factory(SimpleNamespace(transaction=lambda: None)):
        with pytest.raises(RuntimeError, match="retention_handles_required"):
            await preparation._prepare_registry_source_pair(prepared, object())
    prepare.assert_not_awaited()
    assert not hasattr(prepared, "source_session_factory")


@pytest.mark.asyncio
async def test_preparation_uses_retention_pool_and_preserves_final_source_pool(monkeypatch, source_case):
    from process import network_registry_cms_prepared_pair as retention
    from process import provider_directory_cms_preparation as preparation

    retained_sessions = Mock()
    job = replace(source_case.bound, retention_session_factory=retained_sessions)
    prepare = AsyncMock(return_value=object())
    monkeypatch.setattr(retention, "prepare_registry_cms_source_pair", prepare)
    prepared = SimpleNamespace(nonprofile_admission=SimpleNamespace(registry_source_job=job))
    await preparation._prepare_registry_source_pair(prepared, object())
    assert prepared.source_session_factory is job.source_session_factory
    assert prepare.await_args.kwargs["source_session_factory"] is retained_sessions
    assert prepared.registry_source_pair is prepare.return_value


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


@pytest.mark.asyncio
@pytest.mark.parametrize("source_case", ["2026-09-24"], indirect=True)
async def test_source_epoch_can_differ_from_frozen_profile_date(source_case):
    """Source time is authenticated by admission, separately from Profile time."""
    source_as_of = "2026-09-24"
    assert (
        source_case.request.cms_nonprofile_admission["registry_source_retention"]["source_pin"]["as_of"] == source_as_of
    )
    assert source_case.bound.retention_request.source_pin.as_of == source_as_of
    callback = AsyncMock(return_value=source_case.bound)
    assert source_case.execution.attestation.desired_profile_as_of != source_as_of
    assert await _bind(_invocation(callback), source_case) is source_case.bound
    callback.assert_awaited_once()


@pytest.mark.asyncio
async def test_callback_cannot_move_both_profile_dates_together(source_case):
    """A callback cannot change the frozen Profile epoch through shared payloads."""

    async def authenticate(**arguments):
        for execution in (arguments["execution"], arguments["request"].execution):
            execution.attestation.payload["desired_profile_as_of"] = "2026-07-29"
        return source_case.bound

    with pytest.raises(ValueError, match="selection_changed"):
        await _bind(_invocation(authenticate), source_case)
    source_case.bound.notify_receipt.assert_not_awaited()


@pytest.mark.parametrize("source_case", ["2026-09-24"], indirect=True)
@pytest.mark.parametrize("changed_source_date", ["2026-09-25", "2026-07-29"])
def test_changed_source_date_requires_a_new_signature(source_case, changed_source_date):
    """Fresh hash links cannot authorize a change to an already signed source epoch."""
    envelope = sign_guard(source_case.lease.signing_preflight_guard, cms=True)
    body = envelope["lease"]
    guard = body["signing_preflight_guard"]
    for name in ("control_plane_request", "healthcare_request"):
        guard[name]["cms_nonprofile_admission"]["registry_source_retention"]["source_pin"]["as_of"] = (
            changed_source_date
        )
    rehash_guard(guard)
    body["nonce"] = guard["healthcare_receipt"]["receipt_sha256"]
    body["signing_preflight_guard_sha256"] = preflight.preflight_domain_sha256(
        guard_contract.CAPACITY_SIGNING_PREFLIGHT_GUARD_DIGEST_DOMAIN, guard
    )
    body.pop("attestation_id")
    body["attestation_id"] = capacity_lease.capacity_attestation_id(body)
    with pytest.raises(capacity_lease.ProviderDirectoryCapacityLeaseError, match="invalid_signature"):
        _verify(envelope, expected_capacity_geometry_hash=source_case.lease.capacity_geometry_hash)


@pytest.mark.asyncio
@pytest.mark.parametrize("source_case", ["2026-09-24"], indirect=True)
async def test_bound_source_date_must_equal_the_verified_policy(source_case):
    changed = replace(
        source_case.bound,
        retention_request=replace(
            source_case.bound.retention_request,
            source_pin=replace(source_case.bound.retention_request.source_pin, as_of="2026-09-25"),
        ),
    )
    callback = AsyncMock(return_value=changed)
    with pytest.raises(ValueError, match="policy_changed"):
        await _bind(_invocation(callback), source_case)
    callback.assert_awaited_once()
    source_case.bound.notify_receipt.assert_not_awaited()


def _registry_values():
    from tests.test_cms_registry_source_pair_slot import _prepared, _row
    from tests.test_provider_directory_cms_source_hooks import _bound_job

    job = _bound_job()
    pair = replace(_prepared(), source_attempt=job.source_attempt)
    job = replace(job, retention_request=pair.request, capture_id=pair.recipe.source_pin.retained_epoch.epoch_id)
    admission = SimpleNamespace(
        registry_source_job=job,
        plan=SimpleNamespace(
            capacity_geometry_hash=pair.capacity_geometry_hash,
            native_address_input_hash=pair.native_address_input_hash,
        ),
        lease=SimpleNamespace(lease_digest=pair.lease_digest),
    )
    row = _row(pair)
    return job, pair, admission, row


def _registry_native(monkeypatch, receipt_row):
    import json

    from process import provider_directory_cms_source_runtime as runtime
    from process import provider_directory_generic_source_policy as policy
    from tests.test_provider_directory_generic_source_policy import _connection

    driver, _ = _connection()
    state = SimpleNamespace(pid=1234, xid="42", actor="source_publisher", locked=True, active=True)

    async def fetchrow(query, *_arguments):
        if query == runtime._PUBLICATION_IDENTITY_SQL:
            return dict(
                pid=state.pid,
                xid=state.xid,
                session="source_publisher",
                current=state.actor,
                isolation="read committed",
                readonly="off",
            )
        if "registry_source_pair_json::text" in query:
            return {**receipt_row, "payload": json.dumps(receipt_row["payload"])}
        return dict(session="source_publisher", current=state.actor)

    async def fetchval(query, *_arguments):
        if query == runtime._CAPTURE_LOCK_SQL:
            return state.locked
        return "read committed" if query == "SHOW transaction_isolation" else state.xid

    async def execute(query):
        state.actor = "source_owner" if query.startswith("SET LOCAL ROLE") else "source_publisher"

    driver.fetchrow.side_effect, driver.fetchval.side_effect, driver.execute.side_effect = fetchrow, fetchval, execute
    driver.get_server_pid = lambda: state.pid
    driver.is_in_transaction = lambda: state.active
    monkeypatch.setattr(policy, "storage_roles", AsyncMock())
    monkeypatch.setattr(policy, "_copy_driver", AsyncMock(return_value=driver))
    return driver, state


@pytest.fixture
def registry_publication_case(monkeypatch):
    from contextlib import asynccontextmanager
    from contextvars import ContextVar

    from process import provider_directory_cms_publication_custody as custody
    from process import provider_directory_generic_source_policy as policy
    from process import provider_directory_owned_wal_transaction as owned
    from process import provider_directory_profile_control_custody as control

    job, pair, admission, receipt_row = _registry_values()
    driver, native = _registry_native(monkeypatch, receipt_row)
    connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver)))
    session = SimpleNamespace(connection=AsyncMock(return_value=connection), in_transaction=lambda: True)
    session.in_nested_transaction = lambda: False
    database = SimpleNamespace(
        _transaction_binding=lambda: SimpleNamespace(session=session), has_reader_session=lambda: False
    )
    fence, window = object(), object()
    fhir = SimpleNamespace(
        db=database,
        profile_control_custody=control,
        profile_owned_wal=owned,
        _provider_directory_profile_capacity_admission=lambda: admission,
        _PROFILE_CAPACITY_MUTATION_WINDOW=ContextVar("synthetic_window", default=window),
        _PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE=ContextVar("synthetic_fence", default=fence),
    )
    admission.wal_tracker = SimpleNamespace(owned_control_transaction_groups=[])
    admission.geometry = SimpleNamespace(bounded_admission=True)
    prepared = SimpleNamespace(fhir=fhir, fence=fence, nonprofile_admission=admission, registry_source_pair=pair)
    outcome = owned.OwnedWalTransaction(session, connection=connection, retained_driver=driver, retained_pid=1234)
    source_policy = policy.GenericSourcePolicy(
        pair.request.source_pin.schema_name, "source_owner", "source_publisher", ("source_reader",)
    )

    @asynccontextmanager
    async def bind():
        token = owned._ACTIVE_OWNED_TRANSACTION.set((database, asyncio.current_task(), outcome))
        try:
            async with policy.bind_generic_source_publisher(
                driver, source_policy, verify_role_profile=AsyncMock(), transaction_isolation="read committed"
            ):
                async with policy.source_owner_transaction(database), policy.source_publication_body(database):
                    async with custody.source_body(fhir, job.source_session_factory, job.capture_id, session) as group:
                        yield SimpleNamespace(
                            prepared=prepared,
                            session=session,
                            driver=driver,
                            native=native,
                            row=receipt_row,
                            job=job,
                            pair=pair,
                            group=group,
                            outcome=outcome,
                        )
        finally:
            owned._ACTIVE_OWNED_TRANSACTION.reset(token)

    return bind


@pytest.mark.asyncio
async def test_registry_scope_authenticates_existing_owner_group_and_temporary_publisher(registry_publication_case):
    from process import provider_directory_cms_source_runtime as runtime
    from process import provider_directory_generic_source_policy as policy
    from process.network_custom_address_source import NetworkCustomAddressSourceError, _require_transaction

    async with registry_publication_case() as case:
        async with runtime.registry_source_publication(
            case.session, case.prepared, case.row["receipt_id"], case.row["payload"]
        ) as pair:
            await runtime.require_registry_epoch_transaction(
                case.driver, case.pair.request.source_pin, case.pair.recipe.source_pin.retained_epoch
            )
            async with policy.source_publisher_body(case.session):
                await runtime.require_registry_notification_publication(case.session, case.job.source_attempt, pair)
            assert case.native.actor == "source_owner"
            with pytest.raises(NetworkCustomAddressSourceError, match="repeatable-read"):
                await _require_transaction(case.driver)
        assert runtime._REGISTRY_PUBLICATION.get() is None
        assert case.group["consumed"] is False and case.outcome.commit_state == "not_attempted"


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["task", "session", "attempt", "pair", "lock", "xid", "group", "completed"])
async def test_registry_scope_refuses_changed_publication_authority(registry_publication_case, changed):
    from process import provider_directory_cms_source_runtime as runtime

    async with registry_publication_case() as case:
        async with runtime.registry_source_publication(
            case.session, case.prepared, case.row["receipt_id"], case.row["payload"]
        ) as pair:
            session, attempt, supplied = case.session, case.job.source_attempt, pair
            if changed == "session":
                session = SimpleNamespace(connection=case.session.connection, in_transaction=lambda: True)
            if changed == "attempt":
                attempt = replace(attempt, attempt_id="run:" + "b" * 32)
            if changed == "pair":
                supplied = replace(pair, cms_receipt_payload=pair.cms_receipt_payload + " ")
            if changed == "lock":
                case.native.locked = False
            if changed == "xid":
                case.native.xid = "43"
            if changed == "group":
                case.group["identity"] = tuple(list(case.group["identity"]))
            if changed == "completed":
                case.outcome.cleanup_complete = True
            try:
                operation = runtime.require_registry_notification_publication(session, attempt, supplied)
                if changed == "task":
                    operation = asyncio.create_task(operation)
                with pytest.raises((ValueError, RuntimeError)):
                    await operation
            finally:
                case.native.locked, case.native.xid = True, "42"
                case.group["identity"] = case.group["original_identity"]
                case.outcome.cleanup_complete = False


@pytest.mark.asyncio
async def test_registry_scope_refuses_foreign_epoch_and_never_leaks_after_failure(registry_publication_case):
    from process import provider_directory_cms_source_runtime as runtime

    async with registry_publication_case() as case:
        with pytest.raises(RuntimeError, match="synthetic callback"):
            async with runtime.registry_source_publication(
                case.session, case.prepared, case.row["receipt_id"], case.row["payload"]
            ):
                epoch = case.pair.recipe.source_pin.retained_epoch
                with pytest.raises(ValueError, match="publication_invalid"):
                    await runtime.require_registry_epoch_transaction(
                        case.driver, case.pair.request.source_pin, replace(epoch, schema_oid=epoch.schema_oid + 1)
                    )
                raise RuntimeError("synthetic callback")
        assert runtime._REGISTRY_PUBLICATION.get() is None
        with pytest.raises(ValueError, match="publication_invalid"):
            await runtime.require_registry_notification_publication(case.session, case.job.source_attempt, object())


@pytest.mark.asyncio
async def test_wrong_first_rc_notification_attempt_refuses_before_any_insert(registry_publication_case, monkeypatch):
    from process import provider_directory_cms_source_runtime as runtime
    from process import provider_directory_generic_source_policy as policy
    from process import registry_cms_source_notifications as notifications

    async with registry_publication_case() as case:
        store = notifications.RegistryCMSSourceNotificationStore(
            "notification_schema",
            "notification_owner",
            "source_publisher",
            replace(case.job.source_attempt, attempt_id="run:" + "b" * 32),
        )
        monkeypatch.setattr(
            notifications.RegistryCMSSourceNotificationStore, "_connection", AsyncMock(return_value=case.driver)
        )
        current = AsyncMock()
        monkeypatch.setattr(notifications, "_require_current_publication", current)
        async with runtime.registry_source_publication(
            case.session, case.prepared, case.row["receipt_id"], case.row["payload"]
        ) as pair:
            async with policy.source_publisher_body(case.session):
                before = len(case.driver.execute.await_args_list)
                with pytest.raises(ValueError, match="publication_invalid"):
                    await store.notify_receipt(case.session, receipt_id=case.row["receipt_id"], pair=pair)
                assert len(case.driver.execute.await_args_list) == before
        current.assert_not_awaited()


@pytest.mark.asyncio
async def test_bound_job_requires_explicit_read_handle_before_preparation(source_case):
    job = replace(source_case.bound, read_receipt=None)
    with pytest.raises(ValueError, match="handles_required"):
        await _bind(_invocation(AsyncMock(return_value=job)), source_case)


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", [None, {}, True, object()])
async def test_prepared_slot_has_no_ambient_scope_authority(scope):
    from process import provider_directory_cms_source_runtime as runtime

    session = SimpleNamespace(connection=AsyncMock())
    token = runtime._REGISTRY_PUBLICATION.set(scope)
    try:
        if scope is None:
            assert await runtime._current_registry_prepared_pair(session, object(), object(), "stamp") is None
        else:
            with pytest.raises(ValueError, match="^cms_registry_source_publication_invalid$"):
                await runtime._current_registry_prepared_pair(session, object(), object(), "stamp")
        session.connection.assert_not_awaited()
    finally:
        runtime._REGISTRY_PUBLICATION.reset(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("is_recovered", [False, True])
async def test_verified_notice_metadata_follows_successful_read_exit(monkeypatch, is_recovered):
    from contextlib import asynccontextmanager

    from process import provider_directory_cms_publication as publication
    from process.registry_cms_source_notifications import RegistryCMSSourceNotification
    from tests.test_cms_registry_source_pair_slot import _registry_recovery_case

    case = _registry_recovery_case(monkeypatch)
    original_factory, original_begin = case.job.source_session_factory, case.session.begin

    @asynccontextmanager
    async def transaction():
        async with original_begin():
            yield
            assert "registry_source_notification" not in case.prepared.metrics

    @asynccontextmanager
    async def factory():
        async with original_factory() as session:
            yield session
            assert "registry_source_notification" not in case.prepared.metrics

    case.session.begin = transaction
    case.prepared.nonprofile_admission.registry_source_job = replace(case.job, source_session_factory=factory)
    consume = AsyncMock()
    monkeypatch.setattr(publication, "_consume_preparations", consume)
    assert (
        await publication._finalize_committed_publication(
            case.directory, case.prepared, None, None, case.row["receipt_id"], case.row["payload"], is_recovered
        )
        == case.row["receipt_id"]
    )
    notice = RegistryCMSSourceNotification(case.job.source_attempt, case.bound)
    assert case.prepared.metrics.get("registry_source_notification") == {
        "source_schema": case.pair.request.source_pin.schema_name,
        "receipt_id": case.row["receipt_id"],
        "notification_sha256": notice.sha256,
    }
    assert case.prepared.metrics["cms_serving"]["recovered_commit"] is is_recovered
    consume.assert_awaited_once()
    case.job.notify_receipt.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["transaction", "session"])
async def test_failed_read_exit_publishes_no_notice_metadata(monkeypatch, failure):
    from contextlib import asynccontextmanager

    from process import provider_directory_cms_publication as publication
    from tests.test_cms_registry_source_pair_slot import _registry_recovery_case

    case = _registry_recovery_case(monkeypatch)
    original_factory, original_begin = case.job.source_session_factory, case.session.begin

    @asynccontextmanager
    async def transaction():
        async with original_begin():
            yield
            if failure == "transaction":
                raise RuntimeError("synthetic read commit acknowledgement")

    @asynccontextmanager
    async def factory():
        async with original_factory() as session:
            yield session
            if failure == "session":
                raise RuntimeError("synthetic read session exit")

    case.session.begin = transaction
    case.prepared.nonprofile_admission.registry_source_job = replace(case.job, source_session_factory=factory)
    consume = AsyncMock()
    monkeypatch.setattr(publication, "_consume_preparations", consume)
    with pytest.raises(RuntimeError, match="synthetic read"):
        await publication._finalize_committed_publication(
            case.directory, case.prepared, None, None, case.row["receipt_id"], case.row["payload"], True
        )
    assert case.prepared.metrics == {}
    consume.assert_not_awaited()
    case.job.notify_receipt.assert_not_awaited()
