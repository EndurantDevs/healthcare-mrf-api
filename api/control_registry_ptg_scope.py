# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Control-authenticated source review on an explicitly installed server service."""

from __future__ import annotations

import json

from asyncpg import InterfaceError, PostgresError
from sanic import Blueprint, response
from sanic.exceptions import Forbidden
from sqlalchemy.exc import SQLAlchemyError

from api.control_auth import require_registry_auth
from api.endpoint.registry_management import _unique_object
from process.registry_ptg_scope_engine import (
    SCOPE_DEADLINE_HEADER,
    RegistryPTGScopeDeadlineExpired,
    RegistryPTGScopeEngineService,
    registry_ptg_scope_deadline,
    validated_registry_ptg_choices_envelope,
    validated_registry_ptg_scope_envelope,
    validated_registry_ptg_source_reviews_envelope,
)

blueprint = Blueprint("registry_ptg_scope_control", url_prefix="/registry/ptg-source-scopes")
_HEADERS = {"Cache-Control": "private, no-store"}


def install_registry_ptg_scope_engine(app, service, *, office_preparation=None, office_coordinator=None):
    """Install once at server startup; neither requests nor ambient DBs supply it."""
    from process.registry_ptg_office_coordinator import RegistryPTGOfficeIntentCoordinator
    from process.registry_ptg_office_preparation import RegistryPTGOfficePreparationService

    if (
        type(service) is not RegistryPTGScopeEngineService
        or getattr(app.ctx, "registry_ptg_scope_engine", None) is not None
        or getattr(app.ctx, "registry_ptg_office_preparation", None) is not None
        or getattr(app.ctx, "registry_ptg_office_coordinator", None) is not None
        or office_preparation is not None
        and (
            type(office_preparation) is not RegistryPTGOfficePreparationService
            or office_preparation.scope_service is not service
        )
        or office_coordinator is not None
        and (
            type(office_coordinator) is not RegistryPTGOfficeIntentCoordinator
            or office_coordinator.preparation is not office_preparation
        )
    ):
        raise ValueError("registry_ptg_scope_service_unconfigured")
    app.ctx.registry_ptg_scope_engine = service
    app.ctx.registry_ptg_office_preparation = office_preparation
    app.ctx.registry_ptg_office_coordinator = office_coordinator


def _body(request, operation):
    if getattr(request, "query_string", "") or request.args or not request.body or len(request.body) > 131072:
        raise ValueError("registry_ptg_scope_request_invalid")
    document = json.loads(request.body, object_pairs_hook=_unique_object)
    if operation == "source_reviews":
        return validated_registry_ptg_source_reviews_envelope(document)
    if operation == "office_choices":
        from process.registry_ptg_office_preparation_contract import validated_office_choices_envelope

        return validated_office_choices_envelope(document)
    if operation == "office_intents":
        from process.registry_ptg_office_intent_view import validated_office_intent_list_envelope

        return validated_office_intent_list_envelope(document)
    if operation == "prepare_offices":
        from process.registry_ptg_office_coordinator import _envelope

        return _envelope(document)[0]
    if operation == "read_offices":
        from process.registry_ptg_office_preparation_contract import validated_preparation_envelope

        return validated_preparation_envelope(document, read=operation == "read_offices")
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


def _operation_service(app, operation):
    service = getattr(app.ctx, "registry_ptg_scope_engine", None)
    if type(service) is not RegistryPTGScopeEngineService:
        return None, None
    if operation in {"prepare_offices", "office_intents"}:
        from process.registry_ptg_office_coordinator import RegistryPTGOfficeIntentCoordinator

        coordinator = getattr(app.ctx, "registry_ptg_office_coordinator", None)
        preparation = getattr(app.ctx, "registry_ptg_office_preparation", None)
        if (
            type(coordinator) is not RegistryPTGOfficeIntentCoordinator
            or coordinator.preparation is not preparation
            or preparation is None
            or preparation.scope_service is not service
        ):
            return None, None
        return coordinator, "prepare" if operation == "prepare_offices" else "list"
    if operation in {"read_offices", "office_choices"}:
        from process.registry_ptg_office_preparation import RegistryPTGOfficePreparationService

        preparation = getattr(app.ctx, "registry_ptg_office_preparation", None)
        if type(preparation) is not RegistryPTGOfficePreparationService or preparation.scope_service is not service:
            return None, None
        return (
            preparation,
            "office_choices" if operation == "office_choices" else "read",
        )
    return service, operation


def _prepared_reply(result, envelope):
    # Returning from prepare means commit may already have happened. Any malformed
    # result or encoding failure must preserve the caller's original intent.
    try:
        from process.registry_ptg_office_coordinator import _envelope
        from process.registry_ptg_office_intent import _digest
        from process.registry_ptg_office_intent_view import project_registry_ptg_office_prepare_result

        _, actor, semantic = _envelope(envelope)
        if result["intent"]["semantic_sha256"] != _digest(semantic):
            raise ValueError("registry_ptg_office_intent_result_changed")
        return _reply(
            project_registry_ptg_office_prepare_result(
                result,
                actor,
                envelope["source_command"],
                scope_approval_sha256=envelope["selector"]["scope_approval_sha256"],
            )
        )
    except Exception:
        raise RegistryPTGScopeDeadlineExpired(outcome_unknown=True) from None


def _validation_reply(operation, error):
    """Keep actor, unavailable-profile and conflict errors closed and sanitized."""
    if operation in {"prepare_offices", "office_intents"} and str(error) in {
        "registry_reactivation_actor_invalid",
        "registry_actor_invalid",
    }:
        return _reply({"error": {"code": "registry_access_denied"}}, 403)
    if operation in {"prepare_offices", "read_offices", "office_choices", "office_intents"} and str(error) in {
        "registry_ptg_office_service_unconfigured",
        "registry_ptg_office_profile_unavailable",
        "registry_ptg_scope_store_unprotected",
        "registry_ptg_scope_company_fence_unavailable",
        "registry_ptg_office_queue_permissions_unavailable",
        "registry_ptg_office_writer_unavailable",
        "registry_ptg_office_coordinator_unconfigured",
    }:
        return _reply({"error": {"code": "registry_unavailable"}}, 503)
    is_conflict = str(error) in {
        "registry_ptg_scope_source_changed",
        "registry_ptg_scope_idempotency_conflict",
    } or (
        operation in {"prepare_offices", "read_offices", "office_choices", "office_intents"}
        and str(error)
        in {
            "registry_ptg_office_preparation_busy",
            "registry_ptg_office_capacity_exhausted",
            "registry_ptg_office_source_changed",
            "registry_ptg_office_selection_changed",
            "registry_ptg_office_selection_unresolved",
        }
    )
    return _reply(
        {"error": {"code": "registry_source_scope_conflict" if is_conflict else "registry_request_invalid"}},
        409 if is_conflict else 400,
    )


async def _execute_scope(request, operation):
    try:
        require_registry_auth(request)
    except Forbidden:
        return _reply({"error": {"code": "registry_service_auth_required"}}, 401)
    try:
        deadline = _request_deadline(request)
        envelope = _body(request, operation)
        service, method = _operation_service(request.app, operation)
        if service is None:
            return _reply({"error": {"code": "registry_unavailable"}}, 503)
        reply_by_field = await getattr(service, method)(envelope, deadline=deadline)
        if operation == "prepare_offices":
            return _prepared_reply(reply_by_field, envelope)
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
        return _validation_reply(operation, error)
    except RuntimeError, OSError, SQLAlchemyError, PostgresError, InterfaceError:
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


@blueprint.post("/source-reviews")
async def read_registry_ptg_source_reviews(request):
    """Read original approved source reviews under current batch authorization."""
    return await _execute_scope(request, "source_reviews")


@blueprint.post("/prepare-offices")
async def prepare_registry_ptg_offices(request):
    """Reserve durable reviewed intent before exact physical office preparation."""
    return await _execute_scope(request, "prepare_offices")


@blueprint.post("/read-offices")
async def read_registry_ptg_offices(request):
    """Recover the exact closed office command through the installed Reader."""
    return await _execute_scope(request, "read_offices")


@blueprint.post("/office-choices")
async def read_registry_ptg_office_choices(request):
    """Read bounded exact approved site choices under current source authority."""
    return await _execute_scope(request, "office_choices")


@blueprint.post("/office-intents")
async def read_registry_ptg_office_intents(request):
    """Discover original source-bound office intents under fresh authority."""
    return await _execute_scope(request, "office_intents")
