# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Purpose-scoped raw registration using the existing lifecycle and caller pool."""

from __future__ import annotations

import hmac
from contextlib import asynccontextmanager
from datetime import timedelta
from types import SimpleNamespace

from db.models.custom_import import CustomImportDefinitionRevision
from process.custom_import import execution, runner_registry
from process.custom_import.capture_authority import require_raw_capture_authority
from process.custom_import.capture_wire import manifested_content_sha256
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.raw_capture_store import load_replayable_raw_bundle, register_raw_capture_bundle
from process.custom_import.runner_types import LeaseAuthorityLost


class CaptureHandoffError(ValueError):
    """No capture outcome can safely be reported to this caller."""


def _unavailable():
    return CaptureHandoffError("custom_import_capture_unavailable")


async def _locked_authority(session, verified, *, acquisition):
    """Use dataset → execution → lease order and the original first-start budget."""

    permit = verified.permit
    locked = await execution._lock_current_capture_binding(
        session,
        execution_id=verified.execution_id,
        dataset_id=permit.dataset_id,
        definition_revision_id=permit.definition_revision_id,
        schema_revision_id=permit.schema_revision_id,
        fence=verified.fence,
        token_sha256=execution.lease_token_sha256(verified.token.value),
    )
    if locked is None:
        raise LeaseAuthorityLost("custom_import_capture_authority_lost")
    retained, _state, now = locked
    identity = retained.request_identity_sha256
    if (
        retained.idempotency_key != permit.idempotency_key
        or not isinstance(identity, bytes)
        or not hmac.compare_digest(identity, bytes.fromhex(permit.request_identity_sha256))
        or not permit.issued_at <= now < permit.expires_at
        or retained.started_at is None
        or retained.started_at > now
    ):
        raise _unavailable()
    deadline = min(permit.expires_at, retained.started_at + timedelta(seconds=permit.timeout_seconds))
    if acquisition and (verified.fence != 1 or retained.capture_bundle_id is not None or now >= deadline):
        raise LeaseAuthorityLost("custom_import_capture_acquisition_closed")
    model = await session.get(CustomImportDefinitionRevision, permit.definition_revision_id)
    if model is None:
        raise _unavailable()
    definition = CustomImportDefinition.from_json(model.canonical_definition)
    if not hmac.compare_digest(definition.digest, permit.definition_sha256):
        raise _unavailable()
    await runner_registry.validate_revision_identity(
        session,
        SimpleNamespace(
            dataset_id=permit.dataset_id,
            definition_revision_id=permit.definition_revision_id,
            schema_revision_id=permit.schema_revision_id,
            definition=definition,
        ),
    )
    return retained, definition, deadline


@asynccontextmanager
async def _authorized_session(session_factory, verified, *, acquisition):
    """Do not keep a transaction or authority locks across network transfer."""

    async with session_factory() as session, session.begin():
        retained, definition, deadline = await _locked_authority(session, verified, acquisition=acquisition)
        yield session, retained, definition, deadline


async def preflight_capture(session_factory, verified):
    """Prove first-acquisition authority and registrar capability before source access."""

    async with _authorized_session(session_factory, verified, acquisition=True) as (
        session,
        _retained,
        _definition,
        deadline,
    ):
        await require_raw_capture_authority(session)
        return deadline


def _check_content(verified, definition, captures):
    manifests = tuple(capture.manifest for capture in captures)
    actual = manifested_content_sha256(definition, manifests)
    if not hmac.compare_digest(actual, verified.permit.expected_content_sha256):
        raise _unavailable()


async def register_capture(session_factory, verified, captures):
    """Register exact originals and bind atomically; uncertain COMMIT is not failure proof."""

    permit = verified.permit
    async with _authorized_session(session_factory, verified, acquisition=True) as (
        session,
        _retained,
        definition,
        _deadline,
    ):
        _check_content(verified, definition, captures)
        registered = await register_raw_capture_bundle(
            session,
            dataset_id=permit.dataset_id,
            definition_revision_id=permit.definition_revision_id,
            schema_revision_id=permit.schema_revision_id,
            captures=captures,
            maximum_dataset_retained_bytes=permit.maximum_dataset_retained_bytes,
            limits=permit.limits,
        )
        # Decoding/COPY may consume the remaining budget; never bind on an old check.
        await _locked_authority(session, verified, acquisition=True)
        bound = await execution.bind_execution_capture_bundle(
            session,
            execution_id=verified.execution_id,
            dataset_id=permit.dataset_id,
            definition_revision_id=permit.definition_revision_id,
            schema_revision_id=permit.schema_revision_id,
            capture_bundle_id=registered.capture_bundle_id,
            fence=verified.fence,
            token=verified.token.value,
        )
        if bound is None:
            raise LeaseAuthorityLost("custom_import_capture_authority_lost")
        now = await execution._database_now(session)
        if now >= _deadline or now >= permit.expires_at:
            raise LeaseAuthorityLost("custom_import_capture_acquisition_closed")
        bundle_id = bound.capture_bundle_id
    return bundle_id


async def retained_capture(session_factory, verified):
    """Observe only an exact committed binding; acquisition expiry never permits refetch."""

    permit = verified.permit
    async with _authorized_session(session_factory, verified, acquisition=False) as (
        session,
        retained,
        definition,
        _deadline,
    ):
        if retained.capture_bundle_id is None:
            raise _unavailable()
        captures = await load_replayable_raw_bundle(
            session,
            capture_bundle_id=retained.capture_bundle_id,
            dataset_id=permit.dataset_id,
            definition_revision_id=permit.definition_revision_id,
            schema_revision_id=permit.schema_revision_id,
            limits=permit.limits,
        )
        _check_content(verified, definition, captures)
        await _locked_authority(session, verified, acquisition=False)
        return retained.capture_bundle_id, captures
