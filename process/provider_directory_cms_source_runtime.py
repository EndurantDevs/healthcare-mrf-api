# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Specific in-process source capabilities bound to an already verified policy."""

from __future__ import annotations

import asyncio
import contextvars
import os
from collections.abc import Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from process.network_registry_cms_prepared_pair import (
    RegistryCMSRetentionRequest,
    _validate_preparation_request,
)
from process.network_registry_cms_source_attempt import RegistryCMSSourceAttempt
from process.provider_directory_cms_capacity_contract import (
    validated_registry_source_retention_policy,
)
from process.provider_directory_profile_capacity_attestation import (
    VerifiedDatabaseCapacityLease,
)
from process.provider_directory_profile_capacity_preflight_contract import (
    ProviderDirectoryProfileCapacityPreflightRequest,
)
from process.provider_directory_profile_selection_contract import (
    ProviderDirectoryProfileExecution,
)


@dataclass(frozen=True)
class BoundCMSRegistrySourceJob:
    """Exact source retention inputs and trusted handles for one owned publication."""

    retention_request: RegistryCMSRetentionRequest
    capture_id: UUID
    owner_role: str
    runtime_roles: tuple[str, ...]
    source_session_factory: Callable[[], Any] = field(repr=False, compare=False)
    notify_receipt: Callable[..., Awaitable[None]] = field(repr=False, compare=False)
    source_attempt: RegistryCMSSourceAttempt | None = None
    retention_session_factory: Callable[[], Any] | None = field(default=None, repr=False, compare=False)
    read_receipt: Callable[..., Awaitable[Any]] | None = field(default=None, repr=False, compare=False)

    def __post_init__(self):
        _validate_preparation_request(self.retention_request, self.capture_id, self.owner_role, self.runtime_roles)
        validated_registry_source_retention_policy(
            self.retention_request.policy(self.capture_id, self.owner_role, self.runtime_roles)
        )
        if not callable(self.source_session_factory) or not callable(self.notify_receipt):
            raise ValueError("cms_registry_source_handles_required")
        if self.read_receipt is not None and not callable(self.read_receipt):
            raise ValueError("cms_registry_source_handles_required")
        if self.source_attempt is not None and type(self.source_attempt) is not RegistryCMSSourceAttempt:
            raise ValueError("cms_registry_source_attempt_required")
        if self.retention_session_factory is not None and (
            not callable(self.retention_session_factory)
            or self.retention_session_factory is self.source_session_factory
        ):
            raise ValueError("cms_registry_source_retention_handles_required")


@dataclass(frozen=True)
class CMSRegistrySourceRuntime:
    """One specific authenticator supplied by the trusted process bootstrap."""

    authenticate_job: Callable[..., Awaitable[BoundCMSRegistrySourceJob]] = field(repr=False, compare=False)
    bind_claimed_attempt: Callable[..., Awaitable[Callable]] | None = field(default=None, repr=False, compare=False)

    def __post_init__(self):
        if not callable(self.authenticate_job) or (
            self.bind_claimed_attempt is not None and not callable(self.bind_claimed_attempt)
        ):
            raise ValueError("cms_registry_source_runtime_invalid")


def _cms_claim_boundary(context, control_task, target_task):
    """Require the fixed adapter and its actual bootstrap runtime."""
    if type(context) is not dict:
        raise ValueError("cms_registry_source_claim_invalid")
    runtime = context.get("cms_registry_source_runtime")
    current_task = asyncio.current_task()
    job_context = context.get("context")
    if (
        type(runtime) is not CMSRegistrySourceRuntime
        or runtime.bind_claimed_attempt is None
        or current_task is None
        or type(job_context) is not dict
        or type(control_task) is not dict
        or type(target_task) is not dict
        or control_task.get("importer") != "provider-directory-fhir"
        or control_task.get("target_module") != "process.provider_directory_fhir"
        or control_task.get("target_function") != "process_data"
        or control_task.get("call_style", "ctx_task") != "ctx_task"
        or control_task.get("run_shutdown") is not True
    ):
        raise ValueError("cms_registry_source_claim_invalid")
    return runtime, current_task, job_context


async def bind_claimed_cms_registry_source_runtime(context, control_task, target_task):
    """Bind the successful wrapper's actual invocation before bootstrap authentication."""
    runtime, current_task, job_context = _cms_claim_boundary(context, control_task, target_task)
    source_attempt = RegistryCMSSourceAttempt(
        context.get("control_run_id"),
        job_context.get("_control_attempt_id"),
        job_context.get("_control_attempt_started_at"),
    )
    target_job_id = "provider_directory_start_" + source_attempt.run_id
    original_task = deepcopy(target_task)
    original_control_task = deepcopy(control_task)

    def require_original_invocation(expected_runtime, actual_context, actual_task):
        """Keep the original claim task, context, target and native attempt together."""
        from process.live_progress import current_live_progress_context

        live = current_live_progress_context()
        if (
            asyncio.current_task() is not current_task
            or actual_context is not context
            or type(actual_task) is not dict
            or context.get("context") is not job_context
            or context.get("cms_registry_source_runtime") is not expected_runtime
            or context.get("control_run_id") != source_attempt.run_id
            or job_context.get("control_run_id") != source_attempt.run_id
            or job_context.get("_control_attempt_id") != source_attempt.attempt_id
            or job_context.get("_control_attempt_started_at") != source_attempt.attempt_started_at
            or control_task != original_control_task
            or control_task.get("run_id") != source_attempt.run_id
            or target_task != original_task
            or actual_task != original_task
            or original_task.get("run_id") != source_attempt.run_id
            or context.get("job_id") != target_job_id
            or os.getenv("HLTHPRT_WORKER_ONCE_TARGET_JOB_ID") != target_job_id
            or live.get("run_id") != source_attempt.run_id
            or live.get("importer") != "provider-directory-fhir"
            or live.get("attempt_id") != source_attempt.attempt_id
            or live.get("attempt_started_at") != source_attempt.attempt_started_at
        ):
            raise ValueError("cms_registry_source_claim_changed")

    require_original_invocation(runtime, context, original_task)
    authenticate_job = await runtime.bind_claimed_attempt(
        context=context, task=deepcopy(original_task), source_attempt=source_attempt
    )
    require_original_invocation(runtime, context, original_task)
    if not callable(authenticate_job):
        raise ValueError("cms_registry_source_claim_invalid")

    async def authenticate_claimed_job(**arguments):
        """Authenticate initial invocation without constraining later publication tasks."""
        require_original_invocation(bound_runtime, arguments.get("context"), arguments.get("task"))
        bound_job = await authenticate_job(**arguments)
        require_original_invocation(bound_runtime, arguments.get("context"), arguments.get("task"))
        if type(bound_job) is not BoundCMSRegistrySourceJob or bound_job.source_attempt != source_attempt:
            raise ValueError("cms_registry_source_claim_changed")
        return bound_job

    bound_runtime = CMSRegistrySourceRuntime(authenticate_claimed_job)
    context["cms_registry_source_runtime"] = bound_runtime


@dataclass(frozen=True)
class CMSRegistrySourceInvocation:
    """Snapshot the original task while retaining the actual attempt context."""

    runtime: CMSRegistrySourceRuntime = field(repr=False, compare=False)
    context: dict[str, Any] = field(repr=False, compare=False)
    task: dict[str, Any] = field(repr=False, compare=False)

    def __post_init__(self):
        if type(self.runtime) is not CMSRegistrySourceRuntime or type(self.task) is not dict:
            raise ValueError("cms_registry_source_invocation_invalid")
        object.__setattr__(self, "task", deepcopy(self.task))

    @classmethod
    def from_context(cls, context, task):
        """Read only the fixed internal context capability before preset expansion."""
        if "cms_registry_source_runtime" not in context:
            return None
        return cls(context["cms_registry_source_runtime"], context, task)

    async def bind_verified_job(self, execution, request, lease):
        """Authenticate one original attempt and require its exact signed retention policy."""
        if (
            type(execution) is not ProviderDirectoryProfileExecution
            or type(request) is not ProviderDirectoryProfileCapacityPreflightRequest
            or type(lease) is not VerifiedDatabaseCapacityLease
        ):
            raise ValueError("cms_registry_source_verified_inputs_required")
        policy = _request_policy(request)
        profile_as_of = execution.attestation.desired_profile_as_of
        _assert_selected_source(execution, request, policy, profile_as_of)
        bound = await self.runtime.authenticate_job(
            context=self.context,
            task=deepcopy(self.task),
            execution=execution,
            request=request,
            lease=lease,
        )
        if type(bound) is not BoundCMSRegistrySourceJob:
            raise ValueError("cms_registry_source_job_invalid")
        if not all(
            callable(handle)
            for handle in (
                bound.source_session_factory,
                bound.notify_receipt,
                bound.read_receipt,
            )
        ):
            raise ValueError("cms_registry_source_handles_required")
        if type(bound.source_attempt) is not RegistryCMSSourceAttempt:
            raise ValueError("cms_registry_source_attempt_required")
        if (
            _request_policy(request) != policy
            or bound.retention_request.policy(bound.capture_id, bound.owner_role, bound.runtime_roles) != policy
        ):
            raise ValueError("cms_registry_source_policy_changed")
        _assert_selected_source(execution, request, policy, profile_as_of)
        return bound


def _request_policy(request):
    """Reuse the existing closed policy decoder without accepting historical absence."""
    admission = request.cms_nonprofile_admission
    if not isinstance(admission, Mapping) or "registry_source_retention" not in admission:
        raise ValueError("cms_registry_source_policy_required")
    return validated_registry_source_retention_policy(admission["registry_source_retention"])


def _assert_selected_source(execution, request, policy, expected_profile_as_of):
    """Keep the authenticated policy on both captured executions' exact CMS selection."""
    selected = request.execution
    if type(selected) is not ProviderDirectoryProfileExecution or selected.generation != execution.generation:
        raise ValueError("cms_registry_source_selection_changed")
    pin = policy["source_pin"]
    for actual in (execution, selected):
        attestation = actual.attestation
        cms = attestation.desired_cms_dataset
        if (
            attestation.proof_id != policy["selection_proof_id"]
            or attestation.desired_profile_as_of != expected_profile_as_of
            or not isinstance(cms, Mapping)
            or any(cms.get(name) != pin[name] for name in ("source_id", "endpoint_id", "dataset_id"))
            or cms.get("dataset_hash") != pin["dataset_sha256"]
        ):
            raise ValueError("cms_registry_source_selection_changed")


_PUBLICATION_BRAND = object()
_REGISTRY_PUBLICATION = contextvars.ContextVar("cms_registry_source_publication", default=None)
_CAPTURE_LOCK_SQL = """SELECT EXISTS(SELECT FROM pg_locks WHERE locktype='advisory'
 AND pid=pg_backend_pid() AND mode='ExclusiveLock' AND granted AND objsubid=1
 AND classid=$1::oid AND objid=$2::oid)"""
_PUBLICATION_IDENTITY_SQL = """SELECT pg_backend_pid() AS pid,pg_current_xact_id()::text AS xid,
 session_user::text AS session,current_user::text AS current,
 current_setting('transaction_isolation') AS isolation,
 current_setting('transaction_read_only') AS readonly"""


@dataclass(frozen=True)
class _RegistryPublication:
    task: object
    prepared: object
    job: BoundCMSRegistrySourceJob
    prepared_pair: object
    publisher: object
    outcome: object
    group: object
    session: object
    driver: object
    origin: tuple
    receipt_id: str
    receipt_payload: object
    expected_pair: object
    brand: object = field(repr=False, compare=False)


def _publication_invalid():
    raise ValueError("cms_registry_source_publication_invalid")


def _publication_group(prepared, session, job):
    from process.provider_directory_owned_wal_transaction import (
        current_owned_wal_transaction,
    )

    fhir = prepared.fhir
    admission = fhir._provider_directory_profile_capacity_admission()
    outcome = current_owned_wal_transaction(fhir.db)
    if (
        outcome is None
        or admission is None
        or outcome.session is not session
        or outcome.commit_state != "not_attempted"
        or outcome.cleanup_complete is not False
    ):
        _publication_invalid()
    groups = [
        group
        for group in admission.wal_tracker.owned_control_transaction_groups
        if type(group.get("identity")) is tuple
        and len(group["identity"]) == 5
        and group["identity"][0] == "cms_publication_source"
        and group["original_outcome"] is outcome
    ]
    if len(groups) != 1:
        _publication_invalid()
    group = groups[0]
    identity = group["identity"]
    if not (
        group["task"] is asyncio.current_task()
        and group["admission"] is admission
        and group["window"] is fhir._PROFILE_CAPACITY_MUTATION_WINDOW.get()
        and identity is group["original_identity"]
        and identity[1] is job.source_session_factory
        and identity[2] == job.capture_id
        and identity[3] is session
        and identity[4] is prepared.fence is fhir._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.get()
        and group["outcome"] is outcome
        and group["session"] is session
        and group["connection"] is outcome.connection
        and group["driver"] is outcome.retained_driver
        and group["pid"] == outcome.retained_pid
        and type(group["pid"]) is int
        and group["body_complete"] is False
        and group["consumed"] is False
        and group["failure"] is None
        and group["status"] == "incomplete"
        and group["accounting_authority"] is False
        and group["reservation_refund"] is False
        and group.get("statement_custody") is group.get("original_statement_custody") is None
    ):
        _publication_invalid()
    return outcome, group


async def _own_registry_pair(session, prepared_pair, receipt_id, receipt_payload):
    import json

    from db.plan_release_storage import storage_relation
    from process.network_cms_registry_address_capture import native_driver
    from process.network_registry_cms_prepared_pair import _pair_from_prepared_receipt

    connection = await native_driver(session)
    table = storage_relation(
        prepared_pair.request.source_pin.schema_name,
        "provider_directory_cms_serving_receipt",
    )
    row = await connection.fetchrow(
        f"SELECT receipt_id,payload::text,publication_xid::text,profile_generation_id,registry_source_pair_json::text "
        f"FROM {table} WHERE receipt_id=$1 AND publication_xid=pg_current_xact_id()",
        receipt_id,
    )
    if row is None or json.loads(row["payload"]) != receipt_payload:
        _publication_invalid()
    return _pair_from_prepared_receipt(prepared_pair, row)


async def _require_registry_publication(scope, connection):
    from process import provider_directory_generic_source_policy as policy

    if (
        type(scope) is not _RegistryPublication
        or scope.brand is not _PUBLICATION_BRAND
        or scope.task is not asyncio.current_task()
        or connection is not scope.driver
        or not connection.is_in_transaction()
        or scope.session.in_nested_transaction()
        or policy.bound_publisher() is not scope.publisher
        or scope.publisher.connection is not connection
    ):
        _publication_invalid()
    outcome, group = _publication_group(scope.prepared, scope.session, scope.job)
    if outcome is not scope.outcome or group is not scope.group:
        _publication_invalid()
    if (await scope.session.connection()) is not outcome.connection:
        _publication_invalid()
    identity = await connection.fetchrow(_PUBLICATION_IDENTITY_SQL)
    roles = scope.publisher.policy
    if (
        (identity["pid"], identity["xid"]) != scope.origin
        or identity["pid"] != outcome.retained_pid
        or outcome.retained_driver is not connection
        or identity["session"] != roles.publisher_role
        or identity["current"] not in (roles.owner_role, roles.publisher_role)
        or identity["isolation"] != "read committed"
        or identity["readonly"] != "off"
    ):
        _publication_invalid()
    if identity["current"] == roles.owner_role:
        if await policy.require_source_publication_transaction(scope.prepared.fhir.db) is not scope.publisher:
            _publication_invalid()
    elif policy._OWNER.get() is not None:
        _publication_invalid()
    key = int.from_bytes(scope.job.capture_id.bytes[:8], "big")
    if await connection.fetchval(_CAPTURE_LOCK_SQL, key >> 32, key & (2**32 - 1)) is not True:
        _publication_invalid()
    if (
        await _own_registry_pair(scope.session, scope.prepared_pair, scope.receipt_id, scope.receipt_payload)
        != scope.expected_pair
    ):
        _publication_invalid()


@asynccontextmanager
async def registry_source_publication(session, prepared, receipt_id, receipt_payload):
    """Bind retained verification to this genuine original SOURCE publication only."""
    from process import provider_directory_generic_source_policy as policy
    from process.network_cms_registry_address_capture import native_driver
    from process.provider_directory_cms_publication import _registry_preparation

    if _REGISTRY_PUBLICATION.get() is not None:
        _publication_invalid()
    prepared_pair = _registry_preparation(prepared)
    job = prepared.nonprofile_admission.registry_source_job
    if (
        prepared_pair is None
        or type(job.source_attempt) is not RegistryCMSSourceAttempt
        or not callable(job.read_receipt)
    ):
        _publication_invalid()
    publisher = await policy.require_source_publication_transaction(prepared.fhir.db)
    if publisher.policy.schema != prepared_pair.request.source_pin.schema_name:
        _publication_invalid()
    connection = await native_driver(session)
    outcome, group = _publication_group(prepared, session, job)
    identity = await connection.fetchrow(_PUBLICATION_IDENTITY_SQL)
    expected_pair = await _own_registry_pair(session, prepared_pair, receipt_id, receipt_payload)
    scope = _RegistryPublication(
        asyncio.current_task(),
        prepared,
        job,
        prepared_pair,
        publisher,
        outcome,
        group,
        session,
        connection,
        (identity["pid"], identity["xid"]),
        receipt_id,
        deepcopy(receipt_payload),
        expected_pair,
        _PUBLICATION_BRAND,
    )
    await _require_registry_publication(scope, connection)
    token = _REGISTRY_PUBLICATION.set(scope)
    try:
        yield expected_pair
        if await policy.require_source_publication_transaction(prepared.fhir.db) is not publisher:
            _publication_invalid()
        await _require_registry_publication(scope, connection)
    finally:
        _REGISTRY_PUBLICATION.reset(token)


async def require_registry_epoch_transaction(connection, source_pin, epoch):
    """Preserve ordinary stable reads; authenticate only this publication's retained epoch."""
    from process.network_custom_address_source import _require_transaction

    scope = _REGISTRY_PUBLICATION.get()
    if scope is None:
        await _require_transaction(connection)
        return
    if type(scope) is not _RegistryPublication:
        _publication_invalid()
    if (
        source_pin != scope.prepared_pair.request.source_pin
        or epoch != scope.prepared_pair.recipe.source_pin.retained_epoch
    ):
        _publication_invalid()
    await _require_registry_publication(scope, connection)


async def require_registry_notification_publication(session, source_attempt, pair):
    """Require the original attempt and exact own-transaction pair for an RC notification."""
    from process.network_cms_registry_address_capture import native_driver

    scope = _REGISTRY_PUBLICATION.get()
    if (
        type(scope) is not _RegistryPublication
        or source_attempt != scope.job.source_attempt
        or pair != scope.expected_pair
    ):
        _publication_invalid()
    await _require_registry_publication(scope, await native_driver(session))
    if session is not scope.session:
        _publication_invalid()


async def _current_registry_prepared_pair(session, prepared_pair, pair, comment):
    """Use only this original publication's authenticated own receipt; never a reader waiver."""
    from process.network_cms_registry_address_capture import native_driver
    from process.network_registry_cms_prepared_pair import _json, _prepared_comment_prefix

    scope = _REGISTRY_PUBLICATION.get()
    if scope is None:
        return None
    if (
        type(scope) is not _RegistryPublication
        or session is not scope.session
        or prepared_pair != scope.prepared_pair
        or pair != scope.expected_pair
        or comment != _prepared_comment_prefix(scope.prepared_pair) + _json(scope.prepared_pair.as_dict())
    ):
        _publication_invalid()
    await _require_registry_publication(scope, await native_driver(session))
    return scope.expected_pair
