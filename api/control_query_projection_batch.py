# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Authenticate one fixed projection step through the existing application pool."""

from __future__ import annotations

import asyncio
from dataclasses import asdict
from datetime import datetime, timezone

from sanic import Blueprint
from sanic.exceptions import BadRequest, PayloadTooLarge

from api import control_admission_batch as admission
from db.connection import db
from process.custom_import import query_projection_authorization as authorization
from process.custom_import.admission_authorization import AdmissionAuthorizationError
from process.custom_import.build_source import batch_operation_deadline
from process.custom_import.query_projection_worker import validate_receipt
from process.custom_import.runner_types import CancellationRequested, LeaseAuthorityLost

blueprint = Blueprint("custom_import_query_projection", url_prefix="/control/v1")


class ProjectionProgressConflict(RuntimeError):
    """The original retained cursor has changed."""


async def _projection_operation(session_factory, verified):
    from process.custom_import.query_projection_build import ProjectionCursorConflict, serve_projection_batch

    try:
        return await serve_projection_batch(session_factory, verified)
    except ProjectionCursorConflict:
        raise ProjectionProgressConflict("custom_import_projection_progress_conflict") from None


def _receipt(verified, result):
    try:
        document = asdict(result)
        validate_receipt(
            document,
            verified.pins,
            generation_id=document["generation_id"],
            capture_bundle_id=document["capture_bundle_id"],
        )
        return document
    except ValueError, TypeError, KeyError, RuntimeError:
        raise admission._unavailable() from None


async def serve_projection_batch(request, session_factory, *, keyring, expected_origin, trusted_now):
    """Return only a committed receipt from the fixed, independently authorized step."""

    try:
        verified = authorization.verify_request(
            headers=admission._contract_headers(request.headers, projection=True),
            body=request.body,
            method=request.method,
            path=request.path,
            query_string=request.query_string,
            trusted_now=trusted_now,
            expected_origin=expected_origin,
            keyring=keyring,
        )
        operation_deadline = admission.extend_batch_response_deadline(
            request, expires_at=verified.permit.expires_at, trusted_now=trusted_now
        )
        with batch_operation_deadline(operation_deadline):
            async with asyncio.timeout_at(operation_deadline):
                committed = await _projection_operation(session_factory, verified)
        return admission._reply(_receipt(verified, committed))
    except AdmissionAuthorizationError:
        return admission._reply({"error": "projection_forbidden"}, 403)
    except LeaseAuthorityLost, CancellationRequested:
        return admission._reply({"error": "projection_authority_lost"}, 409)
    except ProjectionProgressConflict:
        return admission._reply({"error": "projection_progress_conflict"}, 409)
    except Exception:
        return admission._reply({"error": "projection_unavailable"}, 503)


@blueprint.post("/custom-import/query-projection-batch", stream=True, strict_slashes=True)
async def query_projection_batch(request):
    """Bound request bytes and use only startup-pinned authority and the existing pool."""

    authority = getattr(request.app.ctx, "custom_import_admission_authority", None)
    if authority is None:
        return admission._reply({"error": "projection_unavailable"}, 503)
    try:
        admission._contract_headers(request.headers, projection=True)
        await admission._receive_admission_body(request)
    except PayloadTooLarge:
        return admission._reply({"error": "projection_body_too_large"}, 413)
    except BadRequest, AdmissionAuthorizationError:
        return admission._reply({"error": "projection_forbidden"}, 403)
    except Exception:
        return admission._reply({"error": "projection_unavailable"}, 503)
    keyring, origin = authority
    return await serve_projection_batch(
        request, db.session_factory, keyring=keyring, expected_origin=origin, trusted_now=datetime.now(timezone.utc)
    )
