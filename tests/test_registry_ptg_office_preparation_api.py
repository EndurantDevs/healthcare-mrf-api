# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual Control transport/dispatch; native and live-authority seams stay separate."""

import copy
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sanic import Sanic
from sqlalchemy.exc import SQLAlchemyError

from api import control_registry_ptg_scope as routes
from process.registry_ptg_office_coordinator import RegistryPTGOfficeIntentCoordinator
from process.registry_ptg_office_preparation import RegistryPTGOfficePreparationService
from process.registry_ptg_scope_engine import RegistryPTGScopeDeadlineExpired
from tests.test_registry_ptg_office_intent import selector as semantic_selector
from tests.test_registry_ptg_office_intent_view import envelope as list_envelope
from tests.test_registry_ptg_office_intent_view import internal_result
from tests.test_registry_ptg_office_preparation import envelope
from tests.test_registry_ptg_scope_engine import _service


def _services():
    scope, _ = _service()
    preparation = object.__new__(RegistryPTGOfficePreparationService)
    object.__setattr__(preparation, "scope_service", scope)
    object.__setattr__(preparation, "publisher_sessions", object())
    app = SimpleNamespace(ctx=SimpleNamespace())
    coordinator = object.__new__(RegistryPTGOfficeIntentCoordinator)
    object.__setattr__(coordinator, "preparation", preparation)
    routes.install_registry_ptg_scope_engine(app, scope, office_preparation=preparation, office_coordinator=coordinator)
    return app, preparation


def _document(operation):
    if operation == "prepare":
        document = list_envelope()
        document["selector"] = semantic_selector()
        document["selector"]["reason"] = "Reviewed original office intent 0"
        document["read_limits"] = envelope()["read_limits"]
        return document
    document = envelope()
    if operation == "read":
        handles = {"capture_id", "scope_id", "client_id", "scope_approval_sha256", "idempotency_key"}
        document["selector"] = {name: document["selector"][name] for name in handles}
    return document


def _request(app, operation, *, document=None):
    return SimpleNamespace(
        app=app,
        body=json.dumps(_document(operation) if document is None else document).encode(),
        headers={"Authorization": "Bearer synthetic-control"},
        args={},
        query_string="",
    )


def _handler(operation):
    return routes.prepare_registry_ptg_offices if operation == "prepare" else routes.read_registry_ptg_offices


def _method(monkeypatch, operation, *, error=None):
    calls = []

    async def method(service, document, *, deadline):
        calls.append((service, document, deadline))
        if error:
            raise error
        if operation == "prepare":
            return await internal_result(completed=True)
        return {"capture_id": document["selector"]["capture_id"], "state": "prepared"}

    owner = RegistryPTGOfficeIntentCoordinator if operation == "prepare" else RegistryPTGOfficePreparationService
    monkeypatch.setattr(owner, operation, method)
    return calls


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["prepare", "read"])
async def test_control_auth_precedes_office_body_and_context(monkeypatch, operation):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control")
    monkeypatch.setattr(routes, "_body", lambda *args: pytest.fail("unauthenticated body parse"))
    result = await _handler(operation)(SimpleNamespace(headers={}))
    assert result.status == 401 and result.headers["Cache-Control"] == "private, no-store"


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["prepare", "read"])
async def test_closed_request_reaches_only_linked_installed_service(monkeypatch, operation):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control")
    app, preparation = _services()
    calls = _method(monkeypatch, operation)
    result = await _handler(operation)(_request(app, operation))
    assert result.status == 200 and result.headers["Cache-Control"] == "private, no-store"
    expected = app.ctx.registry_ptg_office_coordinator if operation == "prepare" else preparation
    assert len(calls) == 1 and calls[0][0] is expected
    assert calls[0][1] == _document(operation) and type(calls[0][2]) is float
    payload = json.loads(result.body)
    if operation == "prepare":
        assert payload["intent"]["selector"]["capture_id"] == payload["preparation"]["capture_id"]
        assert payload["intent"]["selector"]["reason"] == _document(operation)["selector"]["reason"]
    else:
        assert payload["capture_id"] == envelope()["selector"]["capture_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement", [None, object(), "different-source"])
async def test_missing_or_unlinked_office_service_is_unavailable(monkeypatch, replacement):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control")
    app, preparation = _services()
    if replacement == "different-source":
        other_scope, _ = _service()
        object.__setattr__(preparation, "scope_service", other_scope)
    else:
        app.ctx.registry_ptg_office_preparation = replacement
    calls = _method(monkeypatch, "prepare")
    result = await routes.prepare_registry_ptg_offices(_request(app, "prepare"))
    assert result.status == 503 and not calls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        lambda document: document.update(command={}),
        lambda document: document.update(recipes=[]),
        lambda document: document["selector"].update(owner_role="caller_owner"),
        lambda document: document["actor"].update(roles=["admin"]),
        lambda document: document["read_limits"].update(maximum_bytes=True),
        lambda document: document["selector"].update(client_id="system"),
    ],
)
async def test_browser_authority_and_extra_fields_refuse(monkeypatch, mutation):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control")
    app, _ = _services()
    calls = _method(monkeypatch, "prepare")
    document = _document("prepare")
    mutation(document)
    result = await routes.prepare_registry_ptg_offices(_request(app, "prepare", document=document))
    assert result.status == 400 and not calls


@pytest.mark.asyncio
async def test_original_admin_session_shape_is_required(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control")
    app, _ = _services()
    calls = _method(monkeypatch, "prepare")
    document = _document("prepare")
    document["actor"]["client_id"] = "synthetic_client"
    result = await routes.prepare_registry_ptg_offices(_request(app, "prepare", document=document))
    assert result.status == 403 and not calls


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["oversize", "duplicate", "query", "deadline", "read-extra"])
async def test_bounds_and_recovery_handles_precede_service(monkeypatch, failure):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control")
    app, _ = _services()
    calls = _method(monkeypatch, "read")
    request = _request(app, "read")
    if failure == "oversize":
        request.body = b" " * 131073
    elif failure == "duplicate":
        request.body = request.body[:-1] + b',"actor":' + json.dumps(envelope()["actor"]).encode() + b"}"
    elif failure == "query":
        request.query_string = "authority=caller"
    elif failure == "deadline":
        request.headers[routes.SCOPE_DEADLINE_HEADER] = "invalid"
    else:
        request.body = json.dumps(envelope()).encode()
    result = await routes.read_registry_ptg_offices(request)
    assert result.status == 400 and not calls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error,status,code",
    [
        (PermissionError("synthetic private detail"), 403, "registry_access_denied"),
        (RegistryPTGScopeDeadlineExpired(outcome_unknown=False), 504, "registry_source_scope_deadline_expired"),
        (RegistryPTGScopeDeadlineExpired(outcome_unknown=True), 504, "registry_source_scope_outcome_unknown"),
        (ValueError("registry_ptg_office_preparation_busy"), 409, "registry_source_scope_conflict"),
        (ValueError("registry_ptg_office_capacity_exhausted"), 409, "registry_source_scope_conflict"),
        (ValueError("registry_ptg_office_selection_changed"), 409, "registry_source_scope_conflict"),
        (ValueError("registry_ptg_office_profile_unavailable"), 503, "registry_unavailable"),
        (ValueError("registry_ptg_scope_store_unprotected"), 503, "registry_unavailable"),
        (RuntimeError("synthetic private detail"), 503, "registry_unavailable"),
        (SQLAlchemyError("synthetic private detail"), 503, "registry_unavailable"),
        (ValueError("synthetic private detail"), 400, "registry_request_invalid"),
    ],
)
async def test_office_failures_are_bounded_and_sanitized(monkeypatch, error, status, code):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control")
    app, _ = _services()
    calls = _method(monkeypatch, "prepare", error=error)
    document = copy.deepcopy(_document("prepare"))
    result = await routes.prepare_registry_ptg_offices(_request(app, "prepare", document=document))
    assert result.status == status and json.loads(result.body) == {"error": {"code": code}}
    assert len(calls) == 1 and calls[0][1]["selector"] == document["selector"]
    assert "capture_id" not in calls[0][1]["selector"]


def test_installation_refuses_foreign_link_and_existing_owner():
    app, preparation = _services()
    other_scope, _ = _service()
    empty = SimpleNamespace(ctx=SimpleNamespace())
    with pytest.raises(ValueError, match="service_unconfigured"):
        routes.install_registry_ptg_scope_engine(empty, other_scope, office_preparation=preparation)
    assert vars(empty.ctx) == {}
    with pytest.raises(ValueError, match="service_unconfigured"):
        routes.install_registry_ptg_scope_engine(app, other_scope)
    assert app.ctx.registry_ptg_office_preparation is preparation


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["prepare", "read"])
async def test_actual_post_routes_preserve_auth_and_methods(monkeypatch, operation):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control")
    installed, preparation = _services()
    calls = _method(monkeypatch, operation)
    app = Sanic("synthetic_office_routes_" + uuid4().hex)
    prefix = "/control/v1/registry/ptg-source-scopes"
    app.blueprint(routes.blueprint, url_prefix=prefix)
    routes.install_registry_ptg_scope_engine(
        app,
        installed.ctx.registry_ptg_scope_engine,
        office_preparation=preparation,
        office_coordinator=installed.ctx.registry_ptg_office_coordinator,
    )
    try:
        _, denied = await app.asgi_client.post(prefix + "/" + operation + "-offices", json=_document(operation))
        assert denied.status_code == 401 and not calls
        _, allowed = await app.asgi_client.post(
            prefix + "/" + operation + "-offices",
            json=_document(operation),
            headers={"Authorization": "Bearer synthetic-control"},
        )
        assert allowed.status_code == 200 and len(calls) == 1
        _, wrong_method = await app.asgi_client.get(prefix + "/" + operation + "-offices")
        assert wrong_method.status_code == 405 and len(calls) == 1
    finally:
        Sanic.unregister_app(app)


def _capture_boundaries(monkeypatch, descriptor, calls):
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock

    from process import registry_ptg_office_preparation as producer_service

    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(scalar_one=lambda: "original")))

    @asynccontextmanager
    async def transaction(*args, **kwargs):
        try:
            yield session
        except BaseException:
            calls.append("rollback")
            raise
        else:
            calls.append("commit")

    async def capture(*args):
        calls.append("capture")
        return descriptor, "e" * 64, 1

    monkeypatch.setattr(producer_service, "registry_company_approval_transaction", transaction)
    monkeypatch.setattr(producer_service, "_prepare_capture", capture)
    monkeypatch.setattr(producer_service, "_reserve_capacity", AsyncMock())


@pytest.mark.asyncio
@pytest.mark.parametrize("denied", [None, "source", "office"])
async def test_internal_physical_service_reauthorizes_source_and_generated_office(monkeypatch, denied):
    from unittest.mock import AsyncMock

    from process import registry_ptg_office_capture as capture
    from process import registry_ptg_office_preparation as producer_service
    from process.registry_ptg_scope_engine import RegistryPTGScopeAuthorityClient
    from tests.test_registry_ptg_office_approval import _command
    from tests.test_registry_ptg_published_office_capture import _fixture

    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control")
    app, preparation = _services()
    calls = []
    command = _command()
    _, _, source_document = _fixture()
    source_command = source_document["command"]
    manifest_by_field = {"command": command, "command_sha256": capture._digest(command)}
    descriptor = SimpleNamespace(
        manifest_json=capture._canonical(manifest_by_field), manifest_sha256=capture._digest(manifest_by_field)
    )
    _capture_boundaries(monkeypatch, descriptor, calls)
    monkeypatch.setattr(producer_service, "_source_context", AsyncMock(return_value=(object(), source_command)))

    def authorize(authority, document, **kwargs):
        phase = "source" if document["command"] == source_command else "office"
        assert document["command"] == (source_command if phase == "source" else command)
        assert document["actor"] == envelope()["actor"]
        assert document["session_token_sha256"] == envelope()["session_token_sha256"]
        calls.append("authorize-" + phase)
        return {
            "authorized": phase != denied,
            "actor": document["actor"],
            "command_sha256": capture._digest(document["command"]),
            "policy_revision": 1,
        }

    monkeypatch.setattr(RegistryPTGScopeAuthorityClient, "_request", authorize)
    if denied:
        with pytest.raises(PermissionError):
            await preparation.prepare(envelope())
        expected = (
            ["authorize-source", "rollback"]
            if denied == "source"
            else ["authorize-source", "capture", "authorize-office", "rollback"]
        )
        assert calls == expected
    else:
        prepared = await preparation.prepare(envelope())
        assert calls == ["authorize-source", "capture", "authorize-office", "commit"]
        assert prepared["command"] == command
