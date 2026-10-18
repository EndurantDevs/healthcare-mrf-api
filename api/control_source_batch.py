# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""One fixed SOURCE operation through the existing trusted application pool.

The service, not request data, loads capture bytes and configuration and checks
the same original permit/lease under page locks through COMMIT. No new pool,
credential, user-selected query or uploaded row is accepted by this handler.
"""

from __future__ import annotations

import asyncio
from dataclasses import asdict
from datetime import datetime, timezone
from inspect import getattr_static

from sanic import Blueprint
from sanic.exceptions import BadRequest, PayloadTooLarge
from sqlalchemy.exc import InterfaceError, OperationalError

from api import control_admission_batch as admission
from api.control_batch_diagnostics import BatchFailureStage, log_batch_failure
from db.connection import db
from process.custom_import import source_authorization as authorization
from process.custom_import.admission_authorization import AdmissionAuthorizationError
from process.custom_import.runner_types import CancellationRequested, LeaseAuthorityLost
from process.custom_import.source_worker import validate_receipt

blueprint = Blueprint("custom_import_source", url_prefix="/control/v1")


class SourceProgressConflict(RuntimeError):
    """The original cursor is no longer current; no retry is authorized."""


def _can_retry_source_operation(error):
    """Require a known invalidated connection and completed, uncanceled cleanup."""

    try:
        task = asyncio.current_task()
        return (
            getattr_static(error, "connection_invalidated", False) is True
            and getattr_static(error, "_custom_import_retry_blocked", False) is False
            and (task is None or not task.cancelling())
        )
    except Exception:
        return False


async def _source_operation(session_factory, verified):
    # Delay the engine import so authorization has no capture-store import cycle.
    from process.custom_import.source_batch import SourceCursor, SourceCursorConflict, serve_source_batch

    pins = verified.pins
    operation_dict = dict(
        execution_id=pins.execution_id,
        build_id=pins.build_id,
        fence=pins.fence,
        stream_slot=pins.stream_slot,
        expected_cursor=SourceCursor(**asdict(pins.expected_cursor)),
        lease_token=verified.token.value,
        source_permit=verified.permit,
    )
    for attempt in range(2):
        try:
            return await serve_source_batch(session_factory, **operation_dict)
        except SourceCursorConflict:
            raise SourceProgressConflict("custom_import_source_progress_conflict") from None
        except (InterfaceError, OperationalError) as error:
            if attempt or not _can_retry_source_operation(error):
                raise
            log_batch_failure(BatchFailureStage.SOURCE_EXECUTE, error, is_retry=True)


def _receipt(verified, result):
    """Only the returned committed receipt, with no later mutable cursor read."""

    try:
        document = asdict(result)
        # Capture and EOF provenance come from the locked service. The worker
        # additionally binds this receipt to its retained pre-POST observation.
        validate_receipt(
            document,
            verified.pins,
            capture_bundle_id=document["capture_bundle_id"],
            was_complete=False,
        )
        return document
    except ValueError, TypeError, KeyError, RuntimeError:
        raise admission._unavailable() from None


async def serve_source_batch(request, session_factory, *, keyring, expected_origin, trusted_now):
    """Authenticate once and report only the fixed service's committed outcome."""

    stage = BatchFailureStage.SOURCE_AUTHORIZE
    try:
        verified = authorization.verify_request(
            headers=admission._contract_headers(request.headers, source=True),
            body=request.body,
            method=request.method,
            path=request.path,
            query_string=request.query_string,
            trusted_now=trusted_now,
            expected_origin=expected_origin,
            keyring=keyring,
        )
        admission.extend_batch_response_deadline(
            request, expires_at=verified.permit.expires_at, trusted_now=trusted_now
        )
        stage = BatchFailureStage.SOURCE_EXECUTE
        committed = await _source_operation(session_factory, verified)
        stage = BatchFailureStage.SOURCE_RECEIPT
        return admission._reply(_receipt(verified, committed))
    except AdmissionAuthorizationError:
        return admission._reply({"error": "source_forbidden"}, 403)
    except LeaseAuthorityLost, CancellationRequested:
        return admission._reply({"error": "source_authority_lost"}, 409)
    except SourceProgressConflict:
        return admission._reply({"error": "source_progress_conflict"}, 409)
    except Exception as error:
        # Includes uncertain COMMIT: never claim rollback or synthesize a cursor.
        log_batch_failure(stage, error)
        return admission._reply({"error": "source_unavailable"}, 503)


@blueprint.post("/custom-import/source-batch", stream=True, strict_slashes=True)
async def source_batch(request):
    """Bound the body before using the startup-pinned authority and existing pool."""

    authority = getattr(request.app.ctx, "custom_import_admission_authority", None)
    if authority is None:
        return admission._reply({"error": "source_unavailable"}, 503)
    try:
        admission._contract_headers(request.headers, source=True)
        await admission._receive_admission_body(request)
    except PayloadTooLarge:
        return admission._reply({"error": "source_body_too_large"}, 413)
    except BadRequest, AdmissionAuthorizationError:
        return admission._reply({"error": "source_forbidden"}, 403)
    except Exception as error:
        log_batch_failure(BatchFailureStage.SOURCE_RECEIVE, error)
        return admission._reply({"error": "source_unavailable"}, 503)
    keyring, origin = authority
    return await serve_source_batch(
        request,
        db.session_factory,
        keyring=keyring,
        expected_origin=origin,
        trusted_now=datetime.now(timezone.utc),
    )
