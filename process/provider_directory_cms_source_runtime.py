# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Specific in-process source capabilities bound to an already verified policy."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from process.network_registry_cms_prepared_pair import (
    RegistryCMSRetentionRequest,
    _validate_preparation_request,
)
from process.network_registry_cms_source_attempt import RegistryCMSSourceAttempt
from process.provider_directory_cms_capacity_contract import validated_registry_source_retention_policy
from process.provider_directory_profile_capacity_attestation import VerifiedDatabaseCapacityLease
from process.provider_directory_profile_capacity_preflight_contract import (
    ProviderDirectoryProfileCapacityPreflightRequest,
)
from process.provider_directory_profile_selection_contract import ProviderDirectoryProfileExecution


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

    def __post_init__(self):
        _validate_preparation_request(self.retention_request, self.capture_id, self.owner_role, self.runtime_roles)
        validated_registry_source_retention_policy(
            self.retention_request.policy(self.capture_id, self.owner_role, self.runtime_roles)
        )
        if not callable(self.source_session_factory) or not callable(self.notify_receipt):
            raise ValueError("cms_registry_source_handles_required")
        if self.source_attempt is not None and type(self.source_attempt) is not RegistryCMSSourceAttempt:
            raise ValueError("cms_registry_source_attempt_required")


@dataclass(frozen=True)
class CMSRegistrySourceRuntime:
    """One specific authenticator supplied by the trusted process bootstrap."""

    authenticate_job: Callable[..., Awaitable[BoundCMSRegistrySourceJob]] = field(repr=False, compare=False)

    def __post_init__(self):
        if not callable(self.authenticate_job):
            raise ValueError("cms_registry_source_runtime_invalid")


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
        _assert_selected_source(execution, request, policy)
        bound = await self.runtime.authenticate_job(
            context=self.context, task=deepcopy(self.task), execution=execution, request=request, lease=lease
        )
        if type(bound) is not BoundCMSRegistrySourceJob:
            raise ValueError("cms_registry_source_job_invalid")
        if not callable(bound.source_session_factory) or not callable(bound.notify_receipt):
            raise ValueError("cms_registry_source_handles_required")
        if type(bound.source_attempt) is not RegistryCMSSourceAttempt:
            raise ValueError("cms_registry_source_attempt_required")
        if (
            _request_policy(request) != policy
            or bound.retention_request.policy(bound.capture_id, bound.owner_role, bound.runtime_roles) != policy
        ):
            raise ValueError("cms_registry_source_policy_changed")
        _assert_selected_source(execution, request, policy)
        return bound


def _request_policy(request):
    """Reuse the existing closed policy decoder without accepting historical absence."""
    admission = request.cms_nonprofile_admission
    if not isinstance(admission, Mapping) or "registry_source_retention" not in admission:
        raise ValueError("cms_registry_source_policy_required")
    return validated_registry_source_retention_policy(admission["registry_source_retention"])


def _assert_selected_source(execution, request, policy):
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
            or attestation.desired_profile_as_of != pin["as_of"]
            or not isinstance(cms, Mapping)
            or any(cms.get(name) != pin[name] for name in ("source_id", "endpoint_id", "dataset_id"))
            or cms.get("dataset_hash") != pin["dataset_sha256"]
        ):
            raise ValueError("cms_registry_source_selection_changed")
