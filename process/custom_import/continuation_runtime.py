# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Authenticate processing epochs without changing retained producer provenance."""

from __future__ import annotations

import hmac
from dataclasses import dataclass, field

from sqlalchemy import func, select

from db.models.custom_import_continuation import CustomImportBuildContinuation
from process.custom_import.execution import lease_token_sha256
from process.custom_import.runner_types import LeaseAuthorityLost


@dataclass(frozen=True)
class BuildContinuationContext:
    continuation_id: int
    build_id: int
    producer_token_sha256: bytes = field(repr=False)

    def __post_init__(self):
        if any(type(value) is not int or not 0 < value < 2**63 for value in (self.continuation_id, self.build_id)):
            raise ValueError("continuation and build identities must be positive integers")
        if type(self.producer_token_sha256) is not bytes or len(self.producer_token_sha256) != 32:
            raise ValueError("retained producer digest must contain 32 immutable bytes")


def producer_token_sha256(request):
    """Use retained provenance for continued data and its finality receipts."""

    context = getattr(request, "continuation", None)
    return context.producer_token_sha256 if context is not None else lease_token_sha256(request.lease_token)


def processing_deadline(request):
    """Remove only an explicitly continued request's historical processing cap."""

    return None if getattr(request, "continuation", None) is not None else request.build_deadline_at


def execution_continuation_matches(authorization, execution, lease, continuation_id, nonce_sha256, now):
    """Compare the immutable epoch with the locked execution and mutable lease."""

    return (
        authorization is not None
        and authorization.continuation_id == continuation_id == getattr(lease, "continuation_id", None)
        and getattr(lease, "continuation_started_at", None) is not None
        and authorization.contract == "custom-import/build-continuation/v1"
        and authorization.processing_deadline_removed is True
        and authorization.execution_id == execution.execution_id == lease.execution_id
        and authorization.dataset_id == execution.dataset_id
        and authorization.definition_revision_id == execution.definition_revision_id
        and authorization.schema_revision_id == execution.schema_revision_id
        and authorization.capture_bundle_id == execution.capture_bundle_id
        and authorization.request_identity_sha256 == execution.request_identity_sha256
        and authorization.producer_fence == lease.fence
        and hmac.compare_digest(bytes(authorization.nonce_sha256), nonce_sha256)
        and lease.token_sha256 is not None
        and hmac.compare_digest(bytes(lease.token_sha256), nonce_sha256)
        and lease.expires_at is not None
        and lease.expires_at > now
    )


def verify_continuation_scope(request, execution, lease, authorization, now, *, build=None):
    """Require the exact current epoch and unchanged retained build pins."""

    context = getattr(request, "continuation", None)
    if context is None:
        if getattr(lease, "continuation_id", None) is not None:
            raise LeaseAuthorityLost("processing continuation requires its current epoch")
        return
    if (
        not execution_continuation_matches(
            authorization, execution, lease, context.continuation_id, lease_token_sha256(request.lease_token), now
        )
        or authorization.build_id != context.build_id
        or any(
            getattr(authorization, name) != getattr(request, name)
            for name in ("dataset_id", "definition_revision_id", "schema_revision_id", "execution_id")
        )
        or authorization.producer_fence != request.fence
        or authorization.producer_token_sha256 != context.producer_token_sha256
        or authorization.original_deadline_at != request.build_deadline_at
        or authorization.base_generation_id != request.expected_base_generation_id
        or authorization.base_pointer_version != request.expected_pointer_version
        or (build is not None and build.build_id != context.build_id)
    ):
        raise LeaseAuthorityLost("processing continuation authority is no longer current")


async def verify_and_bind_continuation(session, request, execution, lease, now, *, build=None):
    """Verify immutable authorization before exposing authority to native guards."""

    context = getattr(request, "continuation", None)
    authorization = None
    if context is not None:
        authorization = (
            await session.execute(
                select(CustomImportBuildContinuation).where(
                    CustomImportBuildContinuation.continuation_id == context.continuation_id
                )
            )
        ).scalar_one_or_none()
    verify_continuation_scope(request, execution, lease, authorization, now, build=build)
    if context is not None:
        await session.execute(continuation_binding_statement(request))
    return authorization


def continuation_binding_statement(request):
    """Bind native guards only after the caller verifies the exact epoch."""

    return continuation_epoch_binding_statement(
        request.continuation.continuation_id, lease_token_sha256(request.lease_token)
    )


def continuation_epoch_binding_statement(continuation_id, nonce_sha256):
    """Build transaction-local authority settings after authenticated verification."""

    return select(
        func.set_config("custom_import.continuation_id", str(continuation_id), True),
        func.set_config("custom_import.continuation_nonce_sha256", nonce_sha256.hex(), True),
    )
