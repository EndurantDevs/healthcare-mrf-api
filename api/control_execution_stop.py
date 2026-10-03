# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fence a retained source request before and after its worker is stopped."""

from __future__ import annotations

import asyncio
import hmac
import json
import os
from types import SimpleNamespace

from sanic import Blueprint, response

from api.control_execution_evidence import _IDEMPOTENCY_KEY, _MAX_BODY_BYTES, _object, _positive_id
from process.custom_import.execution import (
    IdempotencyConflict,
    finalize_stopped_execution_request,
    request_bound_execution_cancellation,
)
from process.custom_import.snowflake_bundle import SnowflakeBundleAcquisitionConnector
from process.custom_import.snowflake_source_binding import load_snowflake_source_binding

blueprint = Blueprint("control_execution_stop", url_prefix="/control/v1")
_FIELDS = frozenset({"dataset_id", "definition_revision_id", "source_binding_revision_id", "idempotency_key"})
_TOKEN_ENV = "HLTHPRT_CUSTOM_IMPORT_EXECUTION_STOP_TOKEN"


def _error(code, status):
    return response.json({"error": code}, status=status, headers={"Cache-Control": "no-store"})


def _authorized(headers):
    expected = (os.getenv(_TOKEN_ENV) or "").strip()
    other_tokens = {
        (os.getenv("HLTHPRT_CONTROL_API_TOKEN") or "").strip(),
        (os.getenv("HLTHPRT_CUSTOM_IMPORT_EVIDENCE_READ_TOKEN") or "").strip(),
    }
    if not expected or expected in other_tokens:
        return False
    authorization = str((headers or {}).get("Authorization", ""))
    if not authorization.startswith("Bearer "):
        return False
    supplied = authorization.removeprefix("Bearer ").strip()
    return hmac.compare_digest(supplied.encode("utf-8", "surrogatepass"), expected.encode("utf-8", "surrogatepass"))


def _pins(request):
    body = getattr(request, "body", None)
    if type(body) is not bytes or not 1 <= len(body) <= _MAX_BODY_BYTES or getattr(request, "query_string", ""):
        raise ValueError("invalid body")
    document = json.loads(body, object_pairs_hook=_object)
    if type(document) is not dict or document.keys() != _FIELDS:
        raise ValueError("invalid fields")
    key = document["idempotency_key"]
    if type(key) is not str or _IDEMPOTENCY_KEY.fullmatch(key) is None:
        raise ValueError("invalid key")
    return (
        _positive_id(document["dataset_id"]),
        _positive_id(document["definition_revision_id"]),
        _positive_id(document["source_binding_revision_id"]),
        key,
    )


def _no_source_access(*_args, **_kwargs):
    raise RuntimeError("source access is unavailable to execution control")


async def _retained_identity(session, dataset_id, definition_revision_id, source_binding_revision_id):
    from process.custom_import.snowflake_segmented_runner import configured_request_identity

    loaded = await load_snowflake_source_binding(
        session,
        definition_revision_id=definition_revision_id,
        source_binding_revision_id=source_binding_revision_id,
    )
    if loaded.dataset_id != dataset_id:
        raise ValueError("binding dataset differs")
    no_source = SimpleNamespace(load_key_pair=_no_source_access, fetch_bundle=_no_source_access)
    connector = SnowflakeBundleAcquisitionConnector(
        approved_relations=loaded.approved_relations,
        credential_provider=no_source,
        adapter=no_source,
    )
    bundle_request = connector.prepare_request(
        loaded.definition,
        bindings=loaded.bundle_bindings,
        processing_policy=getattr(loaded.binding, "processing_policy", None),
        snapshot_token_mode=getattr(loaded.binding, "snapshot_token_mode", None),
        decimal_conversions=getattr(loaded.binding, "decimal_conversions", None),
    )
    statement = connector.build_statement(bundle_request)
    digest = configured_request_identity(
        bundle_request,
        statement,
        source_binding_sha256=loaded.source_binding_sha256,
        processing_policy=getattr(loaded.binding, "processing_policy", None),
    )
    return loaded.schema_revision_id, digest


async def serve_execution_stop(request, session, *, finalize):
    """The private caller requests cancellation, then proves worker stop to finalize."""

    if not _authorized(getattr(request, "headers", None)):
        return _error("forbidden", 403)
    try:
        dataset_id, definition_revision_id, source_binding_revision_id, key = _pins(request)
    except TypeError, ValueError, UnicodeError:
        return _error("invalid_request", 400)
    if session is None or session.in_transaction():
        return _error("stop_unavailable", 503)
    try:
        async with asyncio.timeout(8), session.begin():
            schema_revision_id, digest = await _retained_identity(
                session, dataset_id, definition_revision_id, source_binding_revision_id
            )
            transition = await (
                finalize_stopped_execution_request if finalize else request_bound_execution_cancellation
            )(
                session,
                dataset_id=dataset_id,
                definition_revision_id=definition_revision_id,
                schema_revision_id=schema_revision_id,
                source_binding_revision_id=source_binding_revision_id,
                idempotency_key=key,
                request_identity_sha256=digest,
            )
    except IdempotencyConflict:
        return _error("identity_conflict", 409)
    except Exception:
        return _error("stop_unavailable", 503)
    return response.json(
        {"execution_id": transition.execution_id, "state": transition.state},
        headers={"Cache-Control": "no-store"},
    )


@blueprint.post("/custom-import/execution-stop-request")
async def execution_stop_request(request):
    """Commit an exact cancellation fence before external worker stop."""

    return await serve_execution_stop(request, getattr(request.ctx, "sa_session", None), finalize=False)


@blueprint.post("/custom-import/execution-stop-finalize")
async def execution_stop_finalize(request):
    """Terminalize the fenced request after its worker is absent."""

    return await serve_execution_stop(request, getattr(request.ctx, "sa_session", None), finalize=True)
