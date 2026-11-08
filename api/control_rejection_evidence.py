# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""A dedicated read-only audience for bounded retained rejection pages."""

from __future__ import annotations

import hmac
import json
import os
import re

from sanic import Blueprint, response

from api.control_execution_evidence import _IDEMPOTENCY_KEY, _InvalidRequest, _object, _positive_id
from process.custom_import import rejection_evidence as evidence

blueprint = Blueprint("control_rejection_evidence", url_prefix="/control/v1")

_TOKEN_ENV = "HLTHPRT_CUSTOM_IMPORT_REJECTION_READ_TOKEN"
_OTHER_TOKEN_ENVS = (
    "HLTHPRT_CONTROL_API_TOKEN",
    "HLTHPRT_REGISTRY_CONTROL_API_TOKEN",
    "HLTHPRT_CUSTOM_IMPORT_EVIDENCE_READ_TOKEN",
    "HLTHPRT_CUSTOM_IMPORT_EXECUTION_STOP_TOKEN",
    "HLTHPRT_CUSTOM_IMPORT_CONTINUATION_TOKEN",
)
_REQUEST_FIELDS = frozenset(
    {"dataset_id", "definition_revision_id", "idempotency_key", "build_id", "after_ordinal", "scope_sha256", "limit"}
)
_SHA256 = re.compile(r"[0-9a-f]{64}", re.ASCII)
_MAX_BODY_BYTES = 4096
_MAX_PAGE_BYTES = evidence.MAX_PAGE_BYTES - 4096
_NO_STORE = {"Cache-Control": "no-store"}
_PAGE_PATH = "/control/v1/custom-import/rejection-page"


def register_rejection_private_responses(app):
    """Keep route-boundary errors private even when no handler is selected."""

    @app.middleware("response")
    async def private_rejection_response(request, reply):
        """Prevent caching retained-page and framework-boundary responses."""
        if request.path in {_PAGE_PATH, _PAGE_PATH + "/"}:
            reply.headers["Cache-Control"] = "no-store"


def _single_header(headers, name):
    headers = headers or {}
    values = (
        headers.getall(name, [])
        if hasattr(headers, "getall")
        else [value for key, value in headers.items() if key.casefold() == name.casefold()]
    )
    return values[0] if len(values) == 1 and type(values[0]) is str else None


def _is_authorized(headers):
    expected = (os.getenv(_TOKEN_ENV) or "").strip()
    if not expected or any(character.isspace() for character in expected):
        return False
    expected_bytes = expected.encode("utf-8", "surrogatepass")
    if any(
        hmac.compare_digest(expected_bytes, (os.getenv(name) or "").strip().encode("utf-8", "surrogatepass"))
        for name in _OTHER_TOKEN_ENVS
    ):
        return False
    supplied = _single_header(headers, "Authorization")
    return bool(
        supplied is not None
        and supplied.startswith("Bearer ")
        and hmac.compare_digest(supplied[7:].encode("utf-8", "surrogatepass"), expected_bytes)
    )


def _request_pins(request):
    body = getattr(request, "body", None)
    content_type = _single_header(getattr(request, "headers", None), "Content-Type")
    if (
        type(body) is not bytes
        or not 1 <= len(body) <= _MAX_BODY_BYTES
        or getattr(request, "query_string", "")
        or content_type is None
        or [part.strip().lower() for part in content_type.split(";")]
        not in (["application/json"], ["application/json", "charset=utf-8"])
    ):
        raise _InvalidRequest("invalid body")
    document = json.loads(body.decode("utf-8"), object_pairs_hook=_object)
    if type(document) is not dict or document.keys() != _REQUEST_FIELDS:
        raise _InvalidRequest("invalid fields")
    for name in ("dataset_id", "definition_revision_id"):
        _positive_id(document[name])
    key = document["idempotency_key"]
    if type(key) is not str or _IDEMPOTENCY_KEY.fullmatch(key) is None:
        raise _InvalidRequest("invalid key")
    build_id = document["build_id"]
    after = document["after_ordinal"]
    scope = document["scope_sha256"]
    if build_id is not None:
        _positive_id(build_id)
    if (
        type(document["limit"]) is not int
        or not 1 <= document["limit"] <= evidence.MAX_EVENTS
        or after is not None
        and (type(after) is not int or not 0 <= after < 2**63 or build_id is None or scope is None)
        or scope is not None
        and (type(scope) is not str or _SHA256.fullmatch(scope) is None or build_id is None)
    ):
        raise _InvalidRequest("invalid page selector")
    return document


def _reply(payload, status=200):
    return response.raw(evidence._encoded(payload), status=status, content_type="application/json", headers=_NO_STORE)


async def serve_rejection_evidence(request, session):
    """Resolve trusted pins and read one page before emitting any retained values."""

    if not _is_authorized(getattr(request, "headers", None)):
        return _reply({"error": "forbidden"}, 403)
    try:
        pins = _request_pins(request)
    except ValueError, TypeError, UnicodeError, RecursionError:
        return _reply({"error": "invalid_request"}, 400)
    try:
        async with evidence.rejection_page_transaction(session):
            execution_id, build_id = await evidence.retained_request_build(
                session,
                **{
                    name: pins[name] for name in ("dataset_id", "definition_revision_id", "idempotency_key", "build_id")
                },
            )
            page = await evidence.rejection_page_in_transaction(
                session,
                dataset_id=pins["dataset_id"],
                execution_id=execution_id,
                build_id=build_id,
                after_ordinal=pins["after_ordinal"],
                scope_sha256=pins["scope_sha256"],
                limit=pins["limit"],
                page_bytes=_MAX_PAGE_BYTES,
            )
        return _reply(page)
    except evidence.RejectionEvidenceNotFound:
        return _reply({"error": "not_found"}, 404)
    except evidence.RejectionEvidenceNotFinal:
        return _reply({"error": "evidence_not_final"}, 409)
    except Exception:
        return _reply({"error": "evidence_unavailable"}, 503)


@blueprint.post("/custom-import/rejection-page", strict_slashes=True)
async def rejection_page(request):
    """Expose only the separately configured retained-row read capability."""

    return await serve_rejection_evidence(request, getattr(request.ctx, "sa_session", None))
