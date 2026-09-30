# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Scoped registration routes commit before exposing retained evidence."""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
import os
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from api import control_registration_authority as routes
from process.custom_import.registration_authority import (
    RegistrationAuthorityConflict,
    RegistrationAuthorityDenied,
    RegistrationAuthorityError,
    RegistrationAuthorityState,
)

_AUTHORITY_ID = "a" * 64
_NOW = dt.datetime(2026, 9, 30, 12, tzinfo=dt.UTC)
_RECEIPT = {
    "dataset_id": 1,
    "definition_revision_id": 2,
    "schema_revision_id": 3,
    "source_binding_revision_id": 4,
    "source_binding_revision": 1,
    "definition_sha256": "1" * 64,
    "schema_sha256": "2" * 64,
    "source_binding_sha256": "3" * 64,
    "status": "registered",
}
_REGISTRATION = {"dataset_key": "synthetic", "definition": {}, "source_binding": {}}


class _Session:
    new = ()
    dirty = ()
    deleted = ()

    def __init__(self):
        self.active = False
        self.commits = 0
        self.fail_commit = False

    def in_transaction(self):
        return self.active

    @asynccontextmanager
    async def begin(self):
        assert not self.active
        self.active = True
        try:
            yield self
            if self.fail_commit:
                raise RuntimeError("ambiguous commit")
            self.commits += 1
        finally:
            self.active = False


def _request(body=b"", *, bearer=None, session=None, query="", control_header=None):
    headers_by_name = {}
    if bearer is not None:
        headers_by_name["Authorization"] = "Bearer " + bearer
    if control_header is not None:
        headers_by_name["X-HealthPorta-Control-Token"] = control_header
    return SimpleNamespace(
        body=body,
        headers=headers_by_name,
        query_string=query,
        ctx=SimpleNamespace(sa_session=session or _Session()),
    )


def _state(*, result=None, token_hash=None, revoked=False):
    return RegistrationAuthorityState(
        authority_id=_AUTHORITY_ID,
        input_sha256=b"i" * 32,
        token_sha256=token_hash or b"t" * 32,
        expires_at=_NOW,
        created_at=_NOW,
        revoked_at=_NOW if revoked else None,
        result_receipt=None if result is None else json.dumps(result, separators=(",", ":"), sort_keys=True),
    )


def _payload(reply):
    assert reply.headers["Cache-Control"] == "no-store"
    return json.loads(reply.body)


@pytest.fixture
def control_token(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control-token")
    return "synthetic-control-token"


def test_only_the_four_fixed_routes_are_declared():
    declared_routes = {(route.uri, tuple(sorted(route.methods))) for route in routes.blueprint._future_routes}
    path = "/custom-import/registration-authorities/<authority_id>"
    assert declared_routes == {
        (path, ("PUT",)),
        (path, ("GET",)),
        (path + "/revoke", ("POST",)),
        (path + "/register", ("POST",)),
    }
    assert all(not route.ignore_body for route in routes.blueprint._future_routes)


@pytest.mark.asyncio
async def test_broad_auth_precedes_body_and_capability_never_falls_back(control_token):
    class _Unreadable:
        headers = {"Authorization": "Bearer synthetic-control-token"}

        @property
        def body(self):
            raise AssertionError("scoped route must not inspect body")

    assert (await routes.serve_register(_Unreadable(), _AUTHORITY_ID)).status == 403

    class _UnreadableBroad:
        headers = {}

        @property
        def body(self):
            raise AssertionError("broad route must not inspect body")

    assert (await routes.serve_mint(_UnreadableBroad(), _AUTHORITY_ID)).status == 403
    token = base64.urlsafe_b64encode(os.urandom(32)).decode("ascii").rstrip("=")
    request = _request(b"{}", bearer=token, control_header=control_token)
    assert (await routes.serve_register(request, _AUTHORITY_ID)).status == 403


@pytest.mark.asyncio
@pytest.mark.parametrize("serve", [routes.serve_mint, routes.serve_get, routes.serve_revoke])
@pytest.mark.parametrize("header", ["Authorization", "X-HealthPorta-Control-Token"])
async def test_non_ascii_broad_header_fails_closed_before_body(control_token, serve, header):
    class _Unreadable:
        headers = {header: "Bearer synthétic" if header == "Authorization" else "synthétic"}

        @property
        def body(self):
            raise AssertionError("unauthorized body must not be read")

    reply = await serve(_Unreadable(), _AUTHORITY_ID)
    assert reply.status == 403 and _payload(reply) == {"error": "forbidden"}


@pytest.mark.asyncio
async def test_non_ascii_control_configuration_denies_scoped_route_before_body(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthétic-control-token")
    bearer = base64.urlsafe_b64encode(b"t" * 32).decode("ascii").rstrip("=")

    class _Unreadable:
        headers = {"Authorization": "Bearer " + bearer}

        @property
        def body(self):
            raise AssertionError("denied request must not inspect body")

    reply = await routes.serve_register(_Unreadable(), _AUTHORITY_ID)
    assert reply.status == 403 and _payload(reply) == {"error": "forbidden"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        b"{}",
        b'{"registration":{},"registration":{}}',
        b"[1]",
        b"{" + b" " * (routes._MAX_BODY_BYTES + 1) + b"}",
    ],
)
async def test_mint_rejects_open_duplicate_and_oversize_bodies(control_token, body):
    session = _Session()
    reply = await routes.serve_mint(_request(body, bearer=control_token, session=session), _AUTHORITY_ID)
    assert reply.status == 400 and _payload(reply) == {"error": "invalid_request"}
    assert session.commits == 0


@pytest.mark.asyncio
async def test_mint_commits_then_reads_back_own_pins(control_token, monkeypatch):
    session = _Session()
    state = _state()
    events = []

    async def mint(current, **pins):
        assert current is session and current.active
        assert pins == {
            "authority_id": _AUTHORITY_ID,
            "registration": _REGISTRATION,
            "expires_at": _NOW,
            "token_sha256": b"t" * 32,
        }
        events.append("mint")
        return state

    async def read(current, authority_id):
        assert current is session and current.active and current.commits == 1
        assert authority_id == _AUTHORITY_ID
        events.append("read")
        return state

    monkeypatch.setattr(routes, "mint_registration_authority", mint)
    monkeypatch.setattr(routes, "get_registration_authority", read)
    mint_document_by_name = {
        "registration": _REGISTRATION,
        "expires_at": "2026-09-30T12:00:00Z",
        "token_sha256": "74" * 32,
    }
    reply = await routes.serve_mint(
        _request(json.dumps(mint_document_by_name).encode(), bearer=control_token, session=session), _AUTHORITY_ID
    )
    assert reply.status == 200 and _payload(reply)["input_sha256"] == (b"i" * 32).hex()
    assert events == ["mint", "read"] and session.commits == 2


@pytest.mark.asyncio
async def test_revoke_tombstone_and_get_history_require_broad_auth(control_token, monkeypatch):
    session = _Session()
    tombstone = RegistrationAuthorityState(_AUTHORITY_ID, None, None, None, _NOW, _NOW, None)

    async def revoke(current, authority_id):
        assert current.active and authority_id == _AUTHORITY_ID
        return tombstone

    async def read(current, authority_id):
        return tombstone

    monkeypatch.setattr(routes, "revoke_registration_authority", revoke)
    monkeypatch.setattr(routes, "get_registration_authority", read)
    reply = await routes.serve_revoke(_request(b"{}", bearer=control_token, session=session), _AUTHORITY_ID)
    assert reply.status == 200 and _payload(reply)["revoked_at"] == "2026-09-30T12:00:00Z"
    assert session.commits == 2
    reply = await routes.serve_get(_request(bearer=control_token, session=session), _AUTHORITY_ID)
    assert reply.status == 200 and _payload(reply)["result"] is None
    assert (await routes.serve_get(_request(bearer="other"), _AUTHORITY_ID)).status == 403


@pytest.mark.asyncio
async def test_register_uses_exact_capability_and_committed_replay(control_token, monkeypatch):
    token = os.urandom(32)
    bearer = base64.urlsafe_b64encode(token).decode("ascii").rstrip("=")
    session = _Session()
    state = _state(result=_RECEIPT, token_hash=hashlib.sha256(token).digest())
    calls = []

    async def register(current, **input_by_name):
        calls.append("register")
        assert current.active and input_by_name == {
            "authority_id": _AUTHORITY_ID,
            "registration": _REGISTRATION,
            "token": token,
        }
        return state

    async def read(current, authority_id):
        assert current.active and current.commits % 2 == 1 and authority_id == _AUTHORITY_ID
        return state

    monkeypatch.setattr(routes, "register_with_authority", register)
    monkeypatch.setattr(routes, "get_registration_authority", read)
    body = json.dumps(_REGISTRATION).encode()
    for _ in range(2):
        reply = await routes.serve_register(_request(body, bearer=bearer, session=session), _AUTHORITY_ID)
        assert reply.status == 200 and _payload(reply) == _RECEIPT
    assert len(calls) == 2 and session.commits == 4
    assert (await routes.serve_register(_request(body, bearer=control_token), _AUTHORITY_ID)).status == 403


@pytest.mark.asyncio
async def test_failure_and_unknown_commit_never_acknowledge_or_retry_graph(monkeypatch):
    token = os.urandom(32)
    bearer = base64.urlsafe_b64encode(token).decode("ascii").rstrip("=")
    body = json.dumps(_REGISTRATION).encode()
    session = _Session()
    session.fail_commit = True
    calls = []

    async def register(_session, **_pins):
        calls.append("register")
        return _state(result=_RECEIPT)

    monkeypatch.setattr(routes, "register_with_authority", register)
    reply = await routes.serve_register(_request(body, bearer=bearer, session=session), _AUTHORITY_ID)
    assert reply.status == 503 and _payload(reply) == {"error": "registration_unavailable"}
    assert len(calls) == 1 and session.commits == 0

    async def denied(_session, **_pins):
        raise RegistrationAuthorityDenied("redacted")

    monkeypatch.setattr(routes, "register_with_authority", denied)
    assert (await routes.serve_register(_request(body, bearer=bearer), _AUTHORITY_ID)).status == 403

    async def conflict(_session, **_pins):
        raise RegistrationAuthorityConflict("redacted")

    monkeypatch.setattr(routes, "register_with_authority", conflict)
    assert (await routes.serve_register(_request(body, bearer=bearer), _AUTHORITY_ID)).status == 409

    async def invalid(_session, **_pins):
        raise RegistrationAuthorityError("redacted")

    monkeypatch.setattr(routes, "register_with_authority", invalid)
    reply = await routes.serve_register(_request(body, bearer=bearer), _AUTHORITY_ID)
    assert reply.status == 400 and _payload(reply) == {"error": "invalid_request"}


@pytest.mark.asyncio
async def test_committed_result_requires_matching_readback(monkeypatch):
    token = os.urandom(32)
    bearer = base64.urlsafe_b64encode(token).decode("ascii").rstrip("=")
    session = _Session()
    calls = []

    async def register(_session, **_pins):
        calls.append("register")
        return _state(result=_RECEIPT)

    async def missing(_session, _authority_id):
        return None

    monkeypatch.setattr(routes, "register_with_authority", register)
    monkeypatch.setattr(routes, "get_registration_authority", missing)
    reply = await routes.serve_register(
        _request(json.dumps(_REGISTRATION).encode(), bearer=bearer, session=session), _AUTHORITY_ID
    )
    assert reply.status == 503 and _payload(reply) == {"error": "registration_unavailable"}
    assert len(calls) == 1 and session.commits == 2


@pytest.mark.asyncio
async def test_query_and_noncanonical_capability_fail_before_store(control_token, monkeypatch):
    async def unexpected(*_args, **_kwargs):
        raise AssertionError("store must not be reached")

    monkeypatch.setattr(routes, "register_with_authority", unexpected)
    monkeypatch.setattr(routes, "get_registration_authority", unexpected)
    assert (await routes.serve_get(_request(bearer=control_token, query="x=1"), _AUTHORITY_ID)).status == 400
    assert (await routes.serve_get(_request(b"{}", bearer=control_token), _AUTHORITY_ID)).status == 400
    bearer = base64.urlsafe_b64encode(os.urandom(32)).decode("ascii").rstrip("=")
    assert (await routes.serve_register(_request(b"{}", bearer=bearer + "="), _AUTHORITY_ID)).status == 403
    assert (await routes.serve_register(_request(b"{}", bearer=bearer, query="x=1"), _AUTHORITY_ID)).status == 400
