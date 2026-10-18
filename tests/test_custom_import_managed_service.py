# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Service policy binds the signed operation and its actual request Reader."""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sanic import Blueprint, Sanic, response
from sanic.exceptions import InvalidUsage, SanicException

from api import custom_import_plan_sql as plan_sql
from api import custom_import_provider_service_http as service
from api import custom_import_provider_service_sql as service_sql
from api import custom_import_read_http as transport
from api import provider_collection_network_read as collection
from api import provider_detail_network_read as detail
from api.endpoint import extension_reads, npi
from api.network_address_scope import current_network_address_scope, network_address_read_scope
from api.provider_collection_network_contract import parse_managed_collection_query
from tests import test_custom_import_provider_service_http as legacy
from tests import test_custom_import_read_http as permits
from tests.test_custom_import_provider_collection import import_boundary as import_boundary
from tests.test_network_catalog_routes import v2_envelope
from tests.test_provider_collection_network_read import _service_refusal
from tests.test_provider_collection_network_read import boundary as boundary


def _signed_request(*, release=False, include_filter=True, limit="2", large=False):
    document = json.loads(legacy._body(include_filter=include_filter))
    document["native_query"].update(limit=limit, offset="1")
    if release:
        document["native_query"]["plan_release_id"] = "hprelease_01ARZ3NDEKTSV4RRFFQ69G5FAV"
    document["managed_collection"] = v2_envelope(
        {"kind": "by-service", "native_query": document["native_query"], "network_generation": "9"},
        **({"excluded_network_ids": list(range(1, 5001))} if large else {}),
    )
    body = transport._canonical_json_bytes(document)
    return SimpleNamespace(
        body=body,
        headers=permits._headers(
            body=body,
            path=service.CUSTOM_IMPORT_PROVIDER_SERVICE_PATH,
            contract=transport.CUSTOM_IMPORT_PROVIDER_TRANSPORT_CONTRACT,
            body_sha256=transport.managed_provider_body_sha256,
            signature_message=transport.custom_import_provider_signature_message,
        ),
        method="POST",
        path=service.CUSTOM_IMPORT_PROVIDER_SERVICE_PATH,
        query_string="",
        ctx=SimpleNamespace(),
    )


def _temporary_reader(monkeypatch, boundary):
    calls = []
    prepare = AsyncMock()
    monkeypatch.setattr(plan_sql, "prepare_plan_query_tables", prepare)

    @asynccontextmanager
    async def reader_session(*, temporary_setup):
        assert not boundary.state.active and temporary_setup is prepare
        calls.append("prepare")
        await temporary_setup(boundary.session)
        try:
            yield boundary.session
        finally:
            calls.append("released")

    monkeypatch.setattr(npi.db, "reader_session", reader_session)
    return calls, prepare


@pytest.mark.asyncio
@pytest.mark.parametrize("release", [False, True])
@pytest.mark.parametrize("include_filter", [False, True])
@pytest.mark.parametrize("has_prior", [False, True])
async def test_signed_service_uses_one_policy_pin_and_restores_actual_reader(
    import_boundary, monkeypatch, release, include_filter, has_prior
):
    permits._install_keyring(monkeypatch)
    request = _signed_request(release=release, include_filter=include_filter, large=True)
    assert transport._MAX_BODY_BYTES < len(request.body)
    prior = object()
    if has_prior:
        request.ctx.sa_session = prior
    calls, prepare = _temporary_reader(monkeypatch, import_boundary)

    async def payload(same_request, reader, parsed, verified):
        assert same_request is request and reader is import_boundary.session
        assert request.ctx.sa_session is reader and import_boundary.state.active
        scope = current_network_address_scope()
        assert scope.manifest is import_boundary.manifest and scope.network_ids is None
        assert scope.excluded_network_ids == tuple(range(1, 5001))
        assert scope.organization_policy is parsed.managed_collection.organization_policy
        assert parsed.include_filter is include_filter and parsed.native_args.get("offset") == "1"
        assert verified.credential
        return {"items": [], "pagination": {"total": 0, "limit": 2, "offset": 1, "page": 1}, "query": {}}

    monkeypatch.setattr(service, "_read_service_payload", payload)
    reply = await service.serve_custom_import_provider_service(request, None)
    assert reply.status == 200, reply.body
    document = json.loads(reply.body)
    assert document["managed_collection"]["generation"] == "9"
    assert document["managed_collection"]["approved_custom_revision"] == "4"
    assert reply.headers["Cache-Control"] == "private, no-store"
    assert reply.headers["X-Network-Generation"] == "9"
    assert reply.headers["X-Network-Access-Scope"] == current_scope_digest(request)
    assert calls == (["prepare", "released"] if release else [])
    assert prepare.await_count == int(release)
    detail._pin.assert_awaited_once()
    assert len(import_boundary.state.snapshots) == 1 and not import_boundary.state.active
    assert getattr(request.ctx, "sa_session", None) is (prior if has_prior else None)
    assert hasattr(request.ctx, "sa_session") is has_prior
    assert current_network_address_scope() is None


def current_scope_digest(request):
    return json.loads(request.body)["managed_collection"]["access_scope_sha256"]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["signature", "path", "limit", "shadow", "kind"])
async def test_signed_service_refuses_before_reader(import_boundary, monkeypatch, failure):
    permits._install_keyring(monkeypatch)
    request = _signed_request(limit="51" if failure == "limit" else "2")
    if failure == "signature":
        document = json.loads(request.body)
        document["include_filter"] = False
        request.body = transport._canonical_json_bytes(document)
    elif failure == "path":
        request.path = "/api/v1/extensions/custom-import/providers"
    elif failure in {"shadow", "kind"}:
        document = json.loads(request.body)
        query = document["managed_collection"]["query"]
        if failure == "shadow":
            query["native_query"]["code"] = "99214"
        else:
            query["kind"] = "list"
        request.body = transport._canonical_json_bytes(document)
    reply = await service.serve_custom_import_provider_service(request, None)
    assert reply.status in {400, 404}
    assert not import_boundary.state.snapshots
    detail._pin.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [400, 403, 404, 409, 503])
async def test_signed_service_sanitizes_errors_and_releases_scope(import_boundary, monkeypatch, status):
    permits._install_keyring(monkeypatch)
    request = _signed_request()
    monkeypatch.setattr(
        service, "_read_service_payload", AsyncMock(side_effect=SanicException("private value", status))
    )
    reply = await service.serve_custom_import_provider_service(request, None)
    assert reply.status == status and b"private value" not in reply.body
    assert set(json.loads(reply.body)) == {"error"}
    assert not import_boundary.state.active and current_network_address_scope() is None
    assert not hasattr(request.ctx, "sa_session")


@pytest.mark.asyncio
@pytest.mark.parametrize("document", [{"items": [{"npi": 1000000001}]}, {"rates": [{"rate": 10}]}])
async def test_native_service_preserves_original_native_shapes(boundary, monkeypatch, document):
    boundary.state.document = document
    monkeypatch.setattr(service, "_service_page", npi.list_providers)
    query = parse_managed_collection_query({"kind": "by-service", "native_query": {"code": "99213", "offset": "1"}})
    result = await collection.read_managed_provider_collection(
        boundary.request,
        query,
        excluded_network_ids=(7,),
        access_scope_sha256="a" * 64,
    )
    assert result["data"] == document
    assert boundary.state.seen[0][1].get("offset") == "1"
    assert not boundary.state.active and current_network_address_scope() is None


def test_explicit_native_service_arguments_require_trusted_managed_scope(boundary):
    params_by_name = {"code": "99213"}
    with pytest.raises(InvalidUsage):
        service_sql.provider_service_native_args({}, params_by_name, None)
    with network_address_read_scope(boundary.manifest, (84,), access_scope_sha256="a" * 64):
        assert service_sql.provider_service_native_args({}, params_by_name, None) is params_by_name
    with network_address_read_scope(boundary.manifest, (84,)):
        with pytest.raises(InvalidUsage):
            service_sql.provider_service_native_args({}, params_by_name, None)
    with network_address_read_scope(
        boundary.manifest,
        None,
        excluded_network_ids=(),
        access_scope_sha256="a" * 64,
    ):
        assert service_sql.provider_service_native_args({}, params_by_name, None) is params_by_name
    with pytest.raises(ValueError, match="network_address_scope_invalid"):
        with network_address_read_scope(boundary.manifest, (84,), organization_policy=object()):
            pytest.fail("invalid visibility must not install a scope")


async def _signed_service_post(request, *, prior=None):
    app = Sanic("signed_service_refusal_" + uuid4().hex)
    app.blueprint(Blueprint.group([extension_reads.blueprint], version_prefix="/api/v"))
    observed_requests = []

    @app.middleware("request")
    async def bind(same_request):
        observed_requests.append(same_request)
        if prior is not None:
            same_request.ctx.sa_session = prior

    _, reply = await app.asgi_client.post(request.path, data=request.body, headers=request.headers)
    same_request = observed_requests[0]
    assert getattr(same_request.ctx, "sa_session", None) is prior
    assert hasattr(same_request.ctx, "sa_session") is (prior is not None)
    assert reply.headers["Cache-Control"] == "private, no-store"
    assert current_network_address_scope() is None
    return reply


def _install_managed_native_refusal(monkeypatch, held, encoded, *, status=422):
    legacy._install(monkeypatch, held.session)
    original_response = transport._response

    async def refused(request, *, native_args, import_context):
        assert request.ctx.sa_session is held.session and held.state.active and held.state.catalog_active
        assert current_network_address_scope().manifest is held.manifest
        assert native_args.get("code") == "99213" and import_context.require_match
        assert held.session.events == ["resolve", "prepare"]
        held.session.events.append("native_refusal")
        return response.raw(encoded, status=status)

    def after_cleanup(body, reply_status):
        assert not held.state.active and not held.state.catalog_active
        assert current_network_address_scope() is None
        return original_response(body, reply_status)

    monkeypatch.setattr(service, "_service_page", refused)
    monkeypatch.setattr(transport, "_response", after_cleanup)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "code",
    [
        "ptg2_provider_scope_refused",
        "ptg2_online_work_budget_exceeded",
        "ptg2_provider_filter_scope_required",
        "ptg2_location_scope_too_broad",
        "ptg2_provider_filter_unsupported",
    ],
)
@pytest.mark.parametrize("include_filter", [False, True])
@pytest.mark.parametrize("has_prior", [False, True])
async def test_signed_service_http_preserves_only_sanitized_refusal(
    import_boundary, monkeypatch, code, include_filter, has_prior
):
    refusal_by_field = _service_refusal(code)
    refusal_by_field.update(detail="discarded detail", diagnostic="discarded context")
    refusal_by_field["message"] = "é" * 301
    refusal_by_field["fix_it"].update(retry_options=[{"code": "discarded guidance"}], diagnostic="discarded")
    _install_managed_native_refusal(monkeypatch, import_boundary, json.dumps(refusal_by_field).encode())
    request = _signed_request(include_filter=include_filter)
    reply = await _signed_service_post(request, prior=object() if has_prior else None)
    expected_by_field = _service_refusal(code) | {"message": "é" * 300}
    assert reply.status == 422 and reply.json == expected_by_field
    assert reply.headers["X-Network-Access-Scope"] == current_scope_digest(request)
    assert "X-Network-Generation" not in reply.headers and "managed_collection" not in reply.json
    assert "discarded" not in reply.text and "items" not in reply.json
    detail._pin.assert_awaited_once()
    assert len(import_boundary.state.snapshots) == 1
    assert import_boundary.session.events == ["resolve", "prepare", "native_refusal"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "encoded",
    [
        b"\xff",
        b"{",
        b"[]",
        b'{"status":422,"status":422}',
        b'{"status":NaN}',
        b"[" * 2000,
        b" " * (1024 * 1024 + 1),
        json.dumps(_service_refusal() | {"code": "unknown"}).encode(),
        json.dumps(_service_refusal() | {"status": "422"}).encode(),
        json.dumps(_service_refusal() | {"fix_it": {"reason": "x" * 8192, "retry_options": []}}).encode(),
    ],
)
async def test_signed_service_http_malformed_refusal_is_terminal(import_boundary, monkeypatch, encoded):
    _install_managed_native_refusal(monkeypatch, import_boundary, encoded)
    reply = await _signed_service_post(_signed_request(), prior=object())
    assert reply.status == 503 and set(reply.json) == {"error"}
    assert "X-Network-Generation" not in reply.headers and "unknown" not in reply.text
    assert import_boundary.session.events == ["resolve", "prepare", "native_refusal"]
    detail._pin.assert_awaited_once()
    assert len(import_boundary.state.snapshots) == 1


@pytest.mark.asyncio
async def test_signed_service_http_accepts_exact_refusal_bounds(import_boundary, monkeypatch):
    refusal_by_field = _service_refusal()
    overhead = len(json.dumps({"reason": "", "retry_options": []}, separators=(",", ":")).encode())
    refusal_by_field["fix_it"]["reason"] = "x" * (8192 - overhead)
    encoded = json.dumps(refusal_by_field).encode()
    encoded += b" " * (1024 * 1024 - len(encoded))
    _install_managed_native_refusal(monkeypatch, import_boundary, encoded)
    reply = await _signed_service_post(_signed_request())
    assert reply.status == 422 and reply.json == refusal_by_field
    assert "X-Network-Generation" not in reply.headers
    assert import_boundary.session.events == ["resolve", "prepare", "native_refusal"]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [400, 403, 404, 409, 503])
async def test_signed_service_http_other_native_errors_keep_safe_status(import_boundary, monkeypatch, status):
    _install_managed_native_refusal(monkeypatch, import_boundary, b"discarded diagnostic", status=status)
    reply = await _signed_service_post(_signed_request())
    assert reply.status == status and set(reply.json) == {"error"}
    assert "X-Network-Generation" not in reply.headers and "discarded" not in reply.text
    assert import_boundary.session.events == ["resolve", "prepare", "native_refusal"]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["signature", "client", "target", "policy", "path"])
async def test_signed_service_http_authority_refuses_before_reader(import_boundary, monkeypatch, damage):
    _install_managed_native_refusal(monkeypatch, import_boundary, json.dumps(_service_refusal()).encode())
    request = _signed_request()
    if damage == "signature":
        request.headers[transport.CUSTOM_IMPORT_READ_SIGNATURE_HEADER] = "invalid"
    elif damage == "path":
        request.path = service.CUSTOM_IMPORT_PROVIDER_SERVICE_PATH + "?ignored=1"
    else:
        document_by_field = json.loads(request.body)
        if damage == "target":
            document_by_field["target"]["generation_id"] = "other-generation"
        elif damage == "client":
            document_by_field["managed_collection"] = v2_envelope(
                document_by_field["managed_collection"]["query"], client_id="other-client"
            )
        else:
            document_by_field["managed_collection"]["access_scope_sha256"] = "0" * 64
        request.body = transport._canonical_json_bytes(document_by_field)
    reply = await _signed_service_post(request, prior=object())
    assert reply.status in {400, 404} and set(reply.json) == {"error"}
    assert "X-Network-Generation" not in reply.headers
    assert not import_boundary.state.snapshots and import_boundary.session.events == []
    detail._pin.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("has_location_error", [False, True])
async def test_signed_service_http_preserves_actual_native_scope_refusal(
    import_boundary, monkeypatch, has_location_error
):
    from api.endpoint import pricing

    native_page = service._service_page
    legacy._install(monkeypatch, import_boundary.session)
    monkeypatch.setattr(service, "_service_page", native_page)
    calls, prepare = _temporary_reader(monkeypatch, import_boundary)
    monkeypatch.setattr(pricing, "resolve_plan_release_guard_selection", AsyncMock(return_value=None))
    monkeypatch.setattr(pricing, "resolve_plan_release_serving", AsyncMock(return_value=None))
    error_type = pricing.PTG2LocationScopeError if has_location_error else pricing.PTG2ProviderFilterScopeError
    search = AsyncMock(side_effect=error_type("discarded engine diagnostic"))
    monkeypatch.setattr(pricing, "search_current_ptg2_index", search)
    request = _signed_request(release=True)
    reply = await _signed_service_post(request, prior=object())
    expected_code = "ptg2_location_scope_too_broad" if has_location_error else "ptg2_provider_filter_scope_required"
    assert reply.status == 422 and reply.json["code"] == expected_code
    assert set(reply.json) == {"status", "code", "message", "fix_it"}
    assert reply.json["fix_it"]["retry_options"] == [] and "discarded" not in reply.text
    assert reply.headers["X-Network-Access-Scope"] == current_scope_digest(request)
    assert "X-Network-Generation" not in reply.headers
    search.assert_awaited_once()
    assert search.await_args.args[0] is import_boundary.session
    assert isinstance(search.await_args.kwargs["import_context"], service_sql.ProviderServiceImportQuery)
    assert "hydrate" not in import_boundary.session.events and "finality" not in import_boundary.session.events
    assert prepare.await_count == 1 and calls == ["prepare", "released"]
    detail._pin.assert_awaited_once()
    assert len(import_boundary.state.snapshots) == 1 and not import_boundary.state.active
