# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Control-authenticated source review on an explicitly installed server service."""

from __future__ import annotations

import json

from sanic import Blueprint, response
from sanic.exceptions import Forbidden
from sqlalchemy.exc import SQLAlchemyError

from api.control_auth import require_control_auth
from api.endpoint.registry_management import _unique_object
from process.registry_ptg_scope_engine import (
    SCOPE_DEADLINE_HEADER,
    RegistryPTGScopeDeadlineExpired,
    RegistryPTGScopeEngineService,
    registry_ptg_scope_deadline,
    validated_registry_ptg_choices_envelope,
    validated_registry_ptg_scope_envelope,
)

blueprint = Blueprint("registry_ptg_scope_control", url_prefix="/registry/ptg-source-scopes")
_HEADERS = {"Cache-Control": "private, no-store"}


def install_registry_ptg_scope_engine(app, service):
    """Install once at server startup; neither requests nor ambient DBs supply it."""
    if (
        type(service) is not RegistryPTGScopeEngineService
        or getattr(app.ctx, "registry_ptg_scope_engine", None) is not None
    ):
        raise ValueError("registry_ptg_scope_service_unconfigured")
    app.ctx.registry_ptg_scope_engine = service


def _body(request, operation):
    if getattr(request, "query_string", "") or request.args or not request.body or len(request.body) > 131072:
        raise ValueError("registry_ptg_scope_request_invalid")
    document = json.loads(request.body, object_pairs_hook=_unique_object)
    if operation == "approve_offices":
        from process.registry_ptg_office_approval import validated_office_envelope

        return validated_office_envelope(document)
    return (
        validated_registry_ptg_choices_envelope(document)
        if operation == "choices"
        else validated_registry_ptg_scope_envelope(document, operation)
    )


def _reply(payload, status=200):
    return response.json(
        payload,
        status=status,
        headers=_HEADERS,
        dumps=json.dumps,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )


def _request_deadline(request):
    headers = request.headers
    if hasattr(headers, "getall") and len(headers.getall(SCOPE_DEADLINE_HEADER, [])) > 1:
        raise ValueError("registry_ptg_scope_request_invalid")
    return registry_ptg_scope_deadline(headers.get(SCOPE_DEADLINE_HEADER))


async def _execute_scope(request, operation):
    try:
        require_control_auth(request)
    except Forbidden:
        return _reply({"error": {"code": "registry_service_auth_required"}}, 401)
    try:
        deadline = _request_deadline(request)
        envelope = _body(request, operation)
        service = getattr(request.app.ctx, "registry_ptg_scope_engine", None)
        if type(service) is not RegistryPTGScopeEngineService:
            return _reply({"error": {"code": "registry_unavailable"}}, 503)
        reply_by_field = await getattr(service, operation)(envelope, deadline=deadline)
        return _reply(reply_by_field)
    except RegistryPTGScopeDeadlineExpired as error:
        return _reply(
            {
                "error": {
                    "code": "registry_source_scope_outcome_unknown"
                    if error.outcome_unknown
                    else "registry_source_scope_deadline_expired"
                }
            },
            504,
        )
    except PermissionError:
        return _reply({"error": {"code": "registry_access_denied"}}, 403)
    except (ValueError, TypeError, UnicodeError, RecursionError) as error:
        is_conflict = str(error) in {"registry_ptg_scope_source_changed", "registry_ptg_scope_idempotency_conflict"}
        return _reply(
            {"error": {"code": "registry_source_scope_conflict" if is_conflict else "registry_request_invalid"}},
            409 if is_conflict else 400,
        )
    except RuntimeError, OSError, SQLAlchemyError:
        return _reply({"error": {"code": "registry_unavailable"}}, 503)


@blueprint.post("/preview")
async def preview_registry_ptg_scope(request):
    """Resolve a bounded reviewed intent without enabling cohort admission."""
    return await _execute_scope(request, "preview")


@blueprint.post("/approve")
async def approve_registry_ptg_scope(request):
    """Append only through the installed protected service and fresh authority."""
    return await _execute_scope(request, "approve")


@blueprint.post("/choices")
async def read_registry_ptg_scope_choices(request):
    """Read exact retained producer choices under existing Control authentication."""
    return await _execute_scope(request, "choices")


@blueprint.post("/approve-offices")
async def approve_registry_ptg_offices(request):
    """Freshly authorize a complete office review; no membership is admitted."""
    return await _execute_scope(request, "approve_offices")
