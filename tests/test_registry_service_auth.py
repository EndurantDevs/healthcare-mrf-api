# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Dedicated Registry credentials stay outside generic operator actions."""

import json
from types import SimpleNamespace

import pytest
from sanic.exceptions import Forbidden

from api import control, control_auth, control_registry_ptg_scope
from api.endpoint import network_catalog, network_providers, network_serving, registry_management, registry_publication

_OPERATOR = "synthetic-operator-token"
_REGISTRY = "synthetic-registry-token"
_HEADERS = ("Authorization", "X-HealthPorta-Control-Token")
_ROUTES = (
    (registry_management.list_records, ("group",), 403),
    (registry_publication.publication_state, (), 403),
    (network_serving.serving_manifest, (), 403),
    (network_catalog.network_catalog_page, (), 403),
    (network_providers.network_provider_page, (), 403),
    (control_registry_ptg_scope.read_registry_ptg_scope_choices, (), 401),
)


def _request(token, header):
    value = "Bearer " + token if header == "Authorization" else token
    # The invalid body must be rejected after auth, without touching a DB context.
    return SimpleNamespace(headers={header: value}, body=b"\xff", args={}, query_string="")


@pytest.fixture(autouse=True)
def credentials(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", _OPERATOR)
    monkeypatch.setenv("HLTHPRT_REGISTRY_CONTROL_API_TOKEN", _REGISTRY)


@pytest.mark.parametrize("header", _HEADERS)
@pytest.mark.parametrize("token", [_OPERATOR, _REGISTRY])
def test_registry_accepts_separate_or_existing_operator_credentials(header, token):
    control_auth.require_registry_auth(_request(token, header))


@pytest.mark.parametrize("header", _HEADERS)
@pytest.mark.parametrize("operator_configured", [False, True])
def test_registry_only_token_is_rejected_by_generic_control(monkeypatch, header, operator_configured):
    if not operator_configured:
        monkeypatch.delenv("HLTHPRT_CONTROL_API_TOKEN")
    with pytest.raises(Forbidden):
        control_auth.require_control_auth(_request(_REGISTRY, header))


@pytest.mark.parametrize("header", _HEADERS)
def test_registry_credential_works_without_configuring_generic_control(monkeypatch, header):
    monkeypatch.delenv("HLTHPRT_CONTROL_API_TOKEN")
    control_auth.require_registry_auth(_request(_REGISTRY, header))


@pytest.mark.parametrize("header", _HEADERS)
def test_operator_credential_works_without_configuring_registry(monkeypatch, header):
    monkeypatch.delenv("HLTHPRT_REGISTRY_CONTROL_API_TOKEN")
    request = _request(_OPERATOR, header)
    control_auth.require_control_auth(request)
    control_auth.require_registry_auth(request)


@pytest.mark.parametrize("registry_value", ["", " ", "invalid-\u00e9"])
def test_invalid_registry_configuration_preserves_operator_access(monkeypatch, registry_value):
    monkeypatch.setenv("HLTHPRT_REGISTRY_CONTROL_API_TOKEN", registry_value)
    request = _request(_OPERATOR, "Authorization")
    control_auth.require_registry_auth(request)
    monkeypatch.delenv("HLTHPRT_CONTROL_API_TOKEN")
    with pytest.raises(Forbidden):
        control_auth.require_registry_auth(request)


@pytest.mark.parametrize("token", ["", "wrong", _OPERATOR, _REGISTRY])
def test_missing_configuration_never_accepts_a_supplied_token(monkeypatch, token):
    monkeypatch.delenv("HLTHPRT_CONTROL_API_TOKEN")
    monkeypatch.delenv("HLTHPRT_REGISTRY_CONTROL_API_TOKEN")
    with pytest.raises(Forbidden):
        control_auth.require_registry_auth(_request(token, "Authorization"))


@pytest.mark.parametrize("header", _HEADERS)
def test_configured_credentials_reject_wrong_or_missing_tokens(header):
    for request in (_request("wrong", header), SimpleNamespace(headers={}), SimpleNamespace()):
        with pytest.raises(Forbidden):
            control_auth.require_registry_auth(request)


@pytest.mark.parametrize("header", _HEADERS)
@pytest.mark.parametrize("token", [_OPERATOR, _REGISTRY])
def test_existing_header_and_configuration_whitespace_semantics(monkeypatch, header, token):
    name = "HLTHPRT_CONTROL_API_TOKEN" if token == _OPERATOR else "HLTHPRT_REGISTRY_CONTROL_API_TOKEN"
    monkeypatch.setenv(name, " " + token + " ")
    request = _request(token + " ", header)
    control_auth.require_registry_auth(request)
    if token == _OPERATOR:
        control_auth.require_control_auth(request)


@pytest.mark.parametrize("header", _HEADERS)
@pytest.mark.parametrize("registry_configured", [False, True])
def test_malformed_headers_fail_closed_for_both_verifiers(monkeypatch, header, registry_configured):
    if not registry_configured:
        monkeypatch.delenv("HLTHPRT_REGISTRY_CONTROL_API_TOKEN")
    request = _request("invalid-\u00e9", header)
    with pytest.raises(Forbidden, match="token is invalid"):
        control_auth.require_registry_auth(request)
    with pytest.raises(Forbidden, match="token is invalid"):
        control_auth.require_control_auth(request)


@pytest.mark.parametrize("header", _HEADERS)
@pytest.mark.parametrize("name", ["HLTHPRT_CONTROL_API_TOKEN", "HLTHPRT_REGISTRY_CONTROL_API_TOKEN"])
def test_matching_non_ascii_configuration_and_header_are_denied(monkeypatch, header, name):
    monkeypatch.setenv(name, "invalid-\u00e9")
    request = _request("invalid-\u00e9", header)
    with pytest.raises(Forbidden):
        control_auth.require_registry_auth(request)
    with pytest.raises(Forbidden):
        control_auth.require_control_auth(request)


@pytest.mark.asyncio
@pytest.mark.parametrize("handler", [control.control_ensure_worker, control.control_node_health])
@pytest.mark.parametrize("header", _HEADERS)
@pytest.mark.parametrize("token", [_REGISTRY, "invalid-\u00e9"])
async def test_invalid_operator_credentials_cannot_enter_actual_generic_actions(monkeypatch, handler, header, token):
    async def forbidden_work(*args, **kwargs):
        pytest.fail("Registry credential reached generic control work")

    monkeypatch.setattr(control, "guarded_ensure_worker", forbidden_work)
    monkeypatch.setattr(control, "node_health", forbidden_work)
    with pytest.raises(Forbidden):
        await handler(_request(token, header))


@pytest.mark.asyncio
@pytest.mark.parametrize("handler,arguments,denied_status", _ROUTES)
@pytest.mark.parametrize("header", _HEADERS)
@pytest.mark.parametrize("token", ["wrong", "invalid-\u00e9"])
async def test_actual_registry_boundaries_deny_before_body_or_database(
    handler, arguments, denied_status, header, token
):
    request = _request(token, header)
    if handler in (network_catalog.network_catalog_page, control_registry_ptg_scope.read_registry_ptg_scope_choices):
        result = await handler(request, *arguments)
        assert result.status == denied_status
        assert json.loads(result.body)["error"]["code"] in {
            "network_catalog_forbidden",
            "registry_service_auth_required",
        }
    else:
        with pytest.raises(Forbidden):
            await handler(request, *arguments)
    assert not hasattr(request, "ctx")


@pytest.mark.asyncio
@pytest.mark.parametrize("handler,arguments,denied_status", _ROUTES)
@pytest.mark.parametrize("header", _HEADERS)
@pytest.mark.parametrize("token", [_OPERATOR, _REGISTRY])
async def test_actual_registry_boundaries_admit_both_credentials_without_database(
    handler, arguments, denied_status, header, token
):
    request = _request(token, header)
    result = await handler(request, *arguments)
    assert result.status == 400 and result.status != denied_status
    assert not hasattr(request, "ctx")
