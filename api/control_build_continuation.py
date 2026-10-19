# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Separate trusted control for explicitly authorized retained-build processing."""

from __future__ import annotations

import asyncio
import hmac
import json
import os

from sanic import Blueprint, response
from sqlalchemy import text

from api.control_execution_evidence import _IDEMPOTENCY_KEY, _object, _positive_id
from api.control_execution_stop import _retained_identity
from db.connection import db
from process.custom_import.build_continuation import (
    authorize_build_continuation,
    inspect_build_continuation,
    parse_build_continuation_authorization,
)
from process.custom_import.execution import IdempotencyConflict

blueprint = Blueprint("control_build_continuation", url_prefix="/control/v1")
_TOKEN_ENV = "HLTHPRT_CUSTOM_IMPORT_CONTINUATION_TOKEN"
_PINS = frozenset({"dataset_id", "definition_revision_id", "source_binding_revision_id", "idempotency_key"})


def _authorized(headers):
    expected = (os.getenv(_TOKEN_ENV) or "").strip()
    other_tokens = {
        (os.getenv(name) or "").strip()
        for name in (
            "HLTHPRT_CONTROL_API_TOKEN",
            "HLTHPRT_CUSTOM_IMPORT_EXECUTION_STOP_TOKEN",
            "HLTHPRT_CUSTOM_IMPORT_EVIDENCE_READ_TOKEN",
        )
    }
    supplied = str((headers or {}).get("Authorization", ""))
    return bool(
        expected
        and expected not in other_tokens
        and supplied.startswith("Bearer ")
        and hmac.compare_digest(
            supplied[7:].strip().encode("utf-8", "surrogatepass"), expected.encode("utf-8", "surrogatepass")
        )
    )


def _document(request, *, authorize):
    body = getattr(request, "body", None)
    if type(body) is not bytes or not 1 <= len(body) <= 16384 or getattr(request, "query_string", ""):
        raise ValueError("invalid continuation body")
    document = json.loads(body, object_pairs_hook=_object)
    extra = "authorization" if authorize else "build_id"
    if type(document) is not dict or document.keys() != _PINS | {extra}:
        raise ValueError("invalid continuation fields")
    if type(document["idempotency_key"]) is not str or _IDEMPOTENCY_KEY.fullmatch(document["idempotency_key"]) is None:
        raise ValueError("invalid continuation key")
    pins_by_field = {key: document[key] if key == "idempotency_key" else _positive_id(document[key]) for key in _PINS}
    command_payload = (
        parse_build_continuation_authorization(document[extra]) if authorize else _positive_id(document[extra])
    )
    return pins_by_field, command_payload


def _reply(command_payload, status=200):
    return response.json(command_payload, status=status, headers={"Cache-Control": "no-store"})


async def serve_build_continuation(request, session, *, authorize):
    """Authorize once or inspect bounded status without acquiring source data."""

    if not _authorized(getattr(request, "headers", None)):
        return _reply({"error": "forbidden"}, 403)
    try:
        pins_by_field, command_payload = _document(request, authorize=authorize)
    except TypeError, ValueError, UnicodeError:
        return _reply({"error": "invalid_request"}, 400)
    if session is None or session.in_transaction():
        return _reply({"error": "continuation_unavailable"}, 503)
    try:
        async with asyncio.timeout(8), session.begin():
            if not authorize:
                await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            await session.execute(text("SET LOCAL statement_timeout = '2000ms'"))
            await session.execute(text("SET LOCAL lock_timeout = '1000ms'"))
            schema, digest = await _retained_identity(
                session,
                pins_by_field["dataset_id"],
                pins_by_field["definition_revision_id"],
                pins_by_field["source_binding_revision_id"],
            )
            pins_by_field.update(schema_revision_id=schema, request_identity_sha256=digest)
            operation_receipt = (
                await authorize_build_continuation(session, command_payload, **pins_by_field)
                if authorize
                else await inspect_build_continuation(session, build_id=command_payload, **pins_by_field)
            )
    except IdempotencyConflict:
        return _reply({"error": "identity_conflict"}, 409)
    except Exception:
        return _reply({"error": "continuation_unavailable"}, 503)
    return _reply(operation_receipt)


@blueprint.post("/custom-import/build-continuation-authorize")
async def build_continuation_authorize(request):
    """Record an exact immutable processing transfer before launching its worker."""

    return await serve_build_continuation(request, getattr(request.ctx, "sa_session", None), authorize=True)


@blueprint.post("/custom-import/build-continuation-status")
async def build_continuation_status(request):
    """Read one exact retained build without returning current bearer material."""

    return await serve_build_continuation(request, getattr(request.ctx, "sa_session", None), authorize=False)


@blueprint.post("/custom-import/build-continuation-projection", stream=True, strict_slashes=True)
async def build_continuation_projection(request):
    """Perform one nonce-scoped owner projection step for the retained build."""

    from process.custom_import.continuation_projection import serve_continuation_projection

    return await serve_continuation_projection(request, db.session_factory)
