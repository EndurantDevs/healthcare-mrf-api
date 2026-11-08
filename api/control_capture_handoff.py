# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Default-off, bounded capture handoff through the existing trusted pool."""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone

from sanic import Blueprint, response
from sanic.exceptions import PayloadTooLarge

from api import control_admission_batch as admission
from db.connection import db
from process.custom_import import capture_authorization as authorization
from process.custom_import import capture_handoff as service
from process.custom_import import capture_wire
from process.custom_import.runner_types import LeaseAuthorityLost

ENABLED_ENV = "HLTHPRT_CUSTOM_IMPORT_CAPTURE_HANDOFF_ENABLED"
blueprint = Blueprint("custom_import_capture", url_prefix="/control/v1")
_HEADERS = (
    "Content-Type",
    "Authorization",
    "X-Custom-Import-Capture-Context",
    "X-Custom-Import-Capture-Key-Id",
    "X-Custom-Import-Capture-Signature",
    "X-Custom-Import-Capture-Execution-Id",
    "X-Custom-Import-Capture-Fence",
)
_HEADER_NAMES = frozenset(name.lower() for name in _HEADERS)
_LOGGER = logging.getLogger(__name__)


@blueprint.listener("before_server_start")
async def initialize_capture_authority(app, _loop):
    """Enabling the separate capability requires already pinned control authority."""

    app.ctx.custom_import_capture_authority = None
    enabled = os.environ.get(ENABLED_ENV)
    if enabled in (None, "0"):
        return
    authority = getattr(app.ctx, "custom_import_admission_authority", None)
    if enabled != "1" or authority is None:
        raise service.CaptureHandoffError("custom_import_capture_configuration_invalid")
    app.ctx.custom_import_capture_authority = authority


def _contract_headers(headers):
    """Preserve duplicates and reject another operation's control headers."""

    if not callable(getattr(headers, "getall", None)):
        raise authorization.CaptureAuthorizationError("custom_import_capture_forbidden")
    for name, _value in headers.items():
        if (
            not isinstance(name, str)
            or not name.isascii()
            or (name.lower().startswith("x-custom-import-") and name.lower() not in _HEADER_NAMES)
        ):
            raise authorization.CaptureAuthorizationError("custom_import_capture_forbidden")
    encoding = headers.getall("Content-Encoding", [])
    if encoding not in ([], ["identity"]):
        raise authorization.CaptureAuthorizationError("custom_import_capture_forbidden")
    pairs = []
    for name in _HEADERS:
        values = headers.getall(name, [])
        if len(values) != 1:
            raise authorization.CaptureAuthorizationError("custom_import_capture_forbidden")
        pairs.append((name, values[0]))
    return pairs


async def _empty_body(request):
    request.stream.request_max_size = 1
    async for chunk in request.stream:
        if chunk:
            raise capture_wire.CaptureWireError("custom_import_capture_body_invalid")


def _binary_reply(bundle_id, captures, limits, deadline):
    async def write_body(reply):
        """Apply the fixed ceiling when Sanic actually starts the deferred writes."""

        async with asyncio.timeout_at(deadline):
            async for chunk in capture_wire.iter_capture_body(captures, limits=limits):
                if asyncio.get_running_loop().time() >= deadline:
                    raise TimeoutError
                await reply.write(chunk)

    return response.ResponseStream(
        write_body,
        content_type="application/octet-stream",
        headers={
            "Cache-Control": "no-store",
            "Pragma": "no-cache",
            "X-Custom-Import-Capture-Bundle-Id": str(bundle_id),
        },
    )


async def serve_capture_handoff(request, session_factory, *, keyring, expected_origin, trusted_now):
    """Authenticate before body receipt; recheck locked authority after transfer."""

    try:
        verified = authorization.verify_request(
            headers=_contract_headers(request.headers),
            method=request.method,
            path=request.path,
            query_string=request.query_string,
            trusted_now=trusted_now,
            expected_origin=expected_origin,
            keyring=keyring,
        )
        deadline = admission.extend_batch_response_deadline(
            request, expires_at=verified.permit.expires_at, trusted_now=trusted_now
        )
        permit_deadline = (
            asyncio.get_running_loop().time()
            + (verified.permit.expires_at - datetime.now(timezone.utc)).total_seconds()
        )
        deadline = min(deadline, permit_deadline) if deadline is not None else permit_deadline
        async with asyncio.timeout_at(deadline):
            if request.path.endswith("-retained"):
                await _empty_body(request)
                bundle_id, captures = await service.retained_capture(session_factory, verified)
                return _binary_reply(bundle_id, captures, verified.permit.limits, deadline)
            acquisition_deadline = await service.preflight_capture(session_factory, verified)
            remaining = (acquisition_deadline - datetime.now(timezone.utc)).total_seconds()
            async with asyncio.timeout(max(0, remaining)):
                if request.path.endswith("-preflight"):
                    await _empty_body(request)
                    return admission._reply(
                        {"acquisition_deadline_at": acquisition_deadline.isoformat().replace("+00:00", "Z")}
                    )
                request.stream.request_max_size = (
                    verified.permit.limits.maximum_compressed_bytes + capture_wire.HEADER_MAX + 12
                )
                captures = await capture_wire.read_capture_body(request.stream, limits=verified.permit.limits)
                bundle_id = await service.register_capture(session_factory, verified, captures)
                return admission._reply({"capture_bundle_id": bundle_id})
    except authorization.CaptureAuthorizationError:
        return admission._reply({"error": "capture_forbidden"}, 403)
    except PayloadTooLarge:
        return admission._reply({"error": "capture_body_too_large"}, 413)
    except capture_wire.CaptureWireError:
        return admission._reply({"error": "capture_body_invalid"}, 400)
    except LeaseAuthorityLost:
        return admission._reply({"error": "capture_authority_lost"}, 409)
    except Exception as error:
        # An unavailable result includes uncertain COMMIT; never log permit or payload.
        _LOGGER.warning("custom_import_capture_unavailable (%s)", type(error).__name__)
        return admission._reply({"error": "capture_unavailable"}, 503)


async def _capture_route(request):
    authority = getattr(request.app.ctx, "custom_import_capture_authority", None)
    if authority is None:
        return admission._reply({"error": "capture_unavailable"}, 503)
    keyring, origin = authority
    return await serve_capture_handoff(
        request,
        db.session_factory,
        keyring=keyring,
        expected_origin=origin,
        trusted_now=datetime.now(timezone.utc),
    )


blueprint.add_route(
    _capture_route,
    "/custom-import/capture-preflight",
    methods=["POST"],
    stream=True,
    strict_slashes=True,
    name="capture_preflight",
)
blueprint.add_route(
    _capture_route,
    "/custom-import/capture-bundle",
    methods=["POST"],
    stream=True,
    strict_slashes=True,
    name="capture_bundle",
)
blueprint.add_route(
    _capture_route,
    "/custom-import/capture-retained",
    methods=["POST"],
    stream=True,
    strict_slashes=True,
    name="capture_retained",
)
