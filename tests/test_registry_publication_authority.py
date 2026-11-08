# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual execution validators consume a fresh, bounded authority exchange."""

import asyncio
import hashlib
import json
import traceback

import httpx
import pytest

from process import registry_publication_authority as authority
from process import registry_publication_execution as execution
from process.registry_publication_queue import _request_metadata

pytestmark = pytest.mark.asyncio
_ORIGIN = "https://authority.example.invalid"
_TOKEN = "synthetic-service-token"
_ACTOR = {"kind": "platform_admin", "user_id": "11111111-1111-4111-8111-111111111111", "client_id": "system"}
_SELECTION = [{"record_kind": "membership", "record_id": 17, "revision": 3}]
_SESSION = "a" * 64


def _canonical(document):
    return json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _command(operation):
    if operation == "publish_initial_inventory":
        return {
            "operation": operation,
            "bootstrap_id": "22222222-2222-4222-8222-222222222222",
            "bootstrap_generation_sha256": "b" * 64,
            "expected_serving_generation": 0,
            "expected_approved_revision": 0,
            "reason": "Keep original reviewed reason λ",
            "idempotency_key": "synthetic-original-key",
        }
    command_by_name = {
        "operation": operation,
        "target_generation": 8,
        "expected_serving_generation": 9,
        "expected_approved_revision": 3,
        "reason": "Keep original reviewed reason λ",
        "idempotency_key": "synthetic-original-key",
    }
    if operation == "publish_source_rollback":
        command_by_name.pop("target_generation")
        command_by_name.update(preview_request_id="22222222-2222-4222-8222-222222222222", candidate_sha256="b" * 64)
    return command_by_name


def _record(operation=None):
    command = (
        _command(operation)
        if operation
        else {
            "expected_draft_revision": 4,
            "expected_approved_revision": 3,
            "expected_serving_generation": 9,
            "selection": _SELECTION,
            "reason": "synthetic reviewed change",
            "idempotency_key": "synthetic-original-key",
        }
    )
    key = command.pop("idempotency_key")
    return {
        **_request_metadata(command, {**_ACTOR, "impersonator_id": None}, _SESSION, key),
        "session_token_sha256": _SESSION,
        "idempotency_key": key,
    }


def _receipt(document, **changes):
    field = "selection" if "selection" in document else "command"
    return {
        "authorized": True,
        "actor": document["actor"],
        field + "_sha256": hashlib.sha256(_canonical(document[field])).hexdigest(),
        "policy_revision": 5,
    } | changes


class _Stream(httpx.AsyncByteStream):
    def __init__(self, chunks):
        self.chunks = chunks
        self.closed = False
        self.yielded = 0

    async def __aiter__(self):
        for chunk in self.chunks:
            self.yielded += 1
            if isinstance(chunk, Exception):
                raise chunk
            yield chunk

    async def aclose(self):
        self.closed = True


def _reply(document=None, *, raw=None, status=200, headers=(), stream=None):
    defaults = [("Content-Type", "application/json"), ("Cache-Control", "private, no-store")]
    overridden_names = {name.lower() for name, _value in headers}
    defaults = [(name, value) for name, value in defaults if name.lower() not in overridden_names]
    body = _canonical(document) if raw is None else raw
    return httpx.Response(status, headers=defaults + list(headers), stream=stream or _Stream([body]))


def _client(handler, **changes):
    return authority.RegistryPublicationAuthorityClient(
        changes.pop("base_url", _ORIGIN),
        changes.pop("service_token", _TOKEN),
        transport=httpx.MockTransport(handler),
        **changes,
    )


async def _validate(client, operation=None):
    if operation:
        return await execution._fresh_retained_operation_authority(client, _record(operation), operation)
    return await execution._fresh_authority(client, _record())


@pytest.mark.parametrize(
    "operation,path",
    [
        (None, "publication"),
        ("reactivate_generation", "reactivation"),
        ("prepare_source_rollback", "source-rollback"),
        ("publish_source_rollback", "source-rollback"),
        ("publish_initial_inventory", "initial-inventory"),
    ],
)
async def test_exact_wire_callback_preserves_original_command_and_pool_lifecycle(operation, path):
    calls, streams = [], []

    async def handler(request):
        calls.append(request)
        document = json.loads(request.content)
        assert request.method == "POST"
        assert str(request.url) == _ORIGIN + f"/internal/v1/registry/{path}/authorize"
        assert request.headers["authorization"] == "Bearer " + _TOKEN
        assert request.headers["accept-encoding"] == "identity"
        assert request.headers["cache-control"] == "no-store"
        assert "cookie" not in request.headers
        assert document["session_token_sha256"] == _SESSION and document["actor"] == _ACTOR
        assert document["command" if operation else "selection"] == (_command(operation) if operation else _SELECTION)
        assert request.content == _canonical(document)
        streams.append(_Stream([_canonical(_receipt(document))]))
        return _reply(stream=streams[-1])

    async with _client(handler) as client:
        pool = client._client
        await _validate(client, operation)
        await _validate(client, operation)
        assert client._client is pool and not pool.is_closed
    assert pool.is_closed and len(calls) == 2 and all(stream.closed for stream in streams)
    await client.aclose()


async def test_every_validator_read_observes_fresh_policy_and_later_revocation():
    calls = []

    async def handler(request):
        calls.append(request)
        if len(calls) == 3:
            return _reply(raw=b'{"error":{"code":"registry_access_denied"}}', status=403)
        return _reply(_receipt(json.loads(request.content), policy_revision=4 + len(calls)))

    async with _client(handler) as client:
        assert await _validate(client) == 5
        assert await _validate(client) == 6
        with pytest.raises(PermissionError, match="authority_denied"):
            await _validate(client)
    assert len(calls) == 3


async def test_initial_inventory_full_command_has_stable_cross_transport_digest():
    command = _command("publish_initial_inventory")
    assert (
        hashlib.sha256(_canonical(command)).hexdigest()
        == "74eba5de8dee5da70ed8edefbd413f3737df6b10626baf76ce31c6b39965f302"
    )
    path, digest_field, body = authority._body(_SESSION, _ACTOR, None, command)
    assert path == "/internal/v1/registry/initial-inventory/authorize" and digest_field == "command_sha256"
    assert json.loads(body)["command"] == command


@pytest.mark.parametrize(
    "operation",
    [None, "reactivate_generation", "prepare_source_rollback", "publish_source_rollback", "publish_initial_inventory"],
)
@pytest.mark.parametrize("change", ["actor", "digest", "policy", "denied"])
async def test_real_execution_validator_refuses_changed_evidence(operation, change):
    async def handler(request):
        document = json.loads(request.content)
        receipt = _receipt(document)
        if change == "actor":
            receipt["actor"] = {**_ACTOR, "user_id": "33333333-3333-4333-8333-333333333333"}
        elif change == "digest":
            receipt[("command" if operation else "selection") + "_sha256"] = "f" * 64
        elif change == "policy":
            receipt["policy_revision"] = True
        else:
            receipt["authorized"] = False
        return _reply(receipt)

    async with _client(handler) as client:
        with pytest.raises(ValueError, match="authority_invalid"):
            await _validate(client, operation)


@pytest.mark.parametrize("status", [301, 302, 307, 308, 400, 401, 403, 404, 408, 429, 500, 503])
async def test_status_refusal_never_retries_or_follows_redirect(status):
    calls = []

    async def handler(request):
        calls.append(request)
        return _reply(
            raw=b"private refusal body", status=status, headers=[("Location", "https://other.example.invalid")]
        )

    async with _client(handler) as client:
        expected = (
            PermissionError
            if status in {401, 403}
            else RuntimeError
            if status in {408, 429} or status >= 500
            else ValueError
        )
        with pytest.raises(expected, match="authority_(invalid|unavailable|denied)"):
            await _validate(client)
    assert len(calls) == 1


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"{",
        b"[]",
        b"null",
        b"\xff",
        b'{"authorized":true,"authorized":true}',
        b'{"authorized":true,"actor":{"kind":"a","kind":"b"},"selection_sha256":"'
        + b"a" * 64
        + b'","policy_revision":1}',
        b'{"authorized":NaN}',
        b"[" * 2000 + b"]" * 2000,
    ],
)
async def test_malformed_and_duplicate_json_never_establish_authority(raw):
    async with _client(lambda request: _reply(raw=raw)) as client:
        with pytest.raises(ValueError, match="authority_invalid"):
            await _validate(client)


@pytest.mark.parametrize("decoder_recursion", [False, True], ids=["bounded-json", "decoder-recursion"])
async def test_deeply_nested_json_is_invalid_without_transient_retry(monkeypatch, decoder_recursion):
    receipt_body = b"[" * 32000 + b"]" * 32000
    assert len(receipt_body) <= authority._MAX_BYTES
    if decoder_recursion:

        def decode_receipt(*arguments, **keywords):
            """Model the JSON decoder's platform-dependent recursion guard."""
            raise RecursionError

        monkeypatch.setattr(authority.json, "loads", decode_receipt)
    async with _client(lambda request: _reply(raw=receipt_body)) as client:
        with pytest.raises(ValueError, match="authority_invalid"):
            await client(session_token_sha256=_SESSION, actor=_ACTOR, selection=_SELECTION)


@pytest.mark.parametrize(
    "change",
    [
        {"extra": 1},
        {"authorized": 1},
        {"actor": []},
        {"selection_sha256": "a" * 63},
        {"selection_sha256": "A" * 64},
        {"policy_revision": -1},
        {"policy_revision": 2**63},
        {"policy_revision": 1.0},
        {"policy_revision": None},
    ],
)
async def test_closed_response_types_and_fields(change):
    async with _client(lambda request: _reply(_receipt(json.loads(request.content), **change))) as client:
        with pytest.raises(ValueError, match="authority_invalid"):
            await _validate(client)


@pytest.mark.parametrize(
    "headers",
    [
        [("Content-Type", "text/html")],
        [("Content-Type", "application/json"), ("Content-Type", "application/json")],
        [("Cache-Control", "public")],
        [("Content-Encoding", "gzip")],
        [("Set-Cookie", "session=private")],
        [("Content-Length", "65537")],
        [("Content-Length", "-1")],
        [("Content-Length", "0")],
        [("Content-Length", "500")],
        [("Content-Length", "1"), ("Content-Length", "1")],
        [("Transfer-Encoding", "gzip")],
        [("Transfer-Encoding", "chunked"), ("Content-Length", "1")],
    ],
)
async def test_invalid_headers_and_truncated_body_fail_closed(headers):
    stream = _Stream([b"{}"])
    async with _client(lambda request: _reply(stream=stream, headers=headers)) as client:
        with pytest.raises(ValueError, match="authority_invalid"):
            await _validate(client)
    assert stream.closed


async def test_exact_response_bound_and_bounded_chunked_overflow():
    document_by_name = {"session_token_sha256": _SESSION, "actor": _ACTOR, "selection": _SELECTION}
    body = _canonical(_receipt(document_by_name))
    exact = body + b" " * (65_536 - len(body))
    async with _client(lambda request: _reply(raw=exact, headers=[("Content-Length", "65536")])) as client:
        assert await _validate(client) == 5
    stream = _Stream([b" " * 32_768, b" " * 32_768, b"x", b"must not consume"])
    async with _client(lambda request: _reply(stream=stream, headers=[("Transfer-Encoding", "chunked")])) as client:
        with pytest.raises(ValueError, match="authority_invalid"):
            await _validate(client)
    assert stream.closed and stream.yielded == 3


@pytest.mark.parametrize(
    "error",
    [
        httpx.ReadTimeout("private-token-body-url"),
        httpx.ReadError("private-token-body-url"),
        httpx.ConnectError("private-token-body-url"),
        OSError("private-token-body-url"),
    ],
)
async def test_transport_errors_are_safe_and_single_attempt(error):
    calls = []

    async def handler(request):
        calls.append(request)
        raise error

    async with _client(handler) as client:
        with pytest.raises(RuntimeError) as caught:
            await _validate(client)
    assert len(calls) == 1
    assert str(caught.value) == "registry_publication_authority_unavailable"
    assert caught.value.__context__ is None
    assert "private-token-body-url" not in "".join(traceback.format_exception(caught.value))
    assert _TOKEN not in repr(client)


async def test_total_deadline_closes_response_and_cancellation_is_not_authority(monkeypatch):
    class Waiting(_Stream):
        async def __aiter__(self):
            await asyncio.Event().wait()
            yield b"unreachable"

    stream = Waiting([])
    async with _client(lambda request: _reply(stream=stream)) as client:
        monkeypatch.setattr(authority, "_TIMEOUT_SECONDS", 0.01)
        with pytest.raises(RuntimeError, match="authority_unavailable"):
            await _validate(client)
    assert stream.closed


@pytest.mark.parametrize(
    "origin",
    [
        "",
        "https://user:private@example.invalid",
        "https://example.invalid/path",
        "https://example.invalid?query",
        "https://example.invalid#fragment",
        "file:///private",
        "https://example.invalid:99999",
        "https://example.invalid\n",
        "https://example.invalid\\private",
    ],
)
async def test_invalid_operator_origin_is_safely_refused(origin):
    with pytest.raises(ValueError) as caught:
        _client(lambda request: None, base_url=origin)
    assert str(caught.value) == "registry_publication_authority_invalid"
    assert caught.value.__context__ is None


async def test_trusted_http_origin_supported_and_no_cookie_forwarded():
    calls = []

    async def handler(request):
        calls.append(request)
        assert str(request.url).startswith("http://authority.example.invalid/internal/v1/")
        assert "cookie" not in request.headers
        return _reply(_receipt(json.loads(request.content)))

    async with _client(handler, base_url="http://authority.example.invalid/") as client:
        client._client.cookies.set("private", "cookie")
        assert await _validate(client) == 5
        assert not client._client.cookies
        assert await _validate(client) == 5
    assert len(calls) == 2


@pytest.mark.parametrize(
    "changes",
    [
        {"session_token_sha256": "bad"},
        {"actor": {**_ACTOR, "extra": "private"}},
        {"selection": []},
        {"selection": _SELECTION * 101},
        {"selection": [{**_SELECTION[0], "record_id": "x" * 129}]},
        {"selection": [{**_SELECTION[0], "revision": True}]},
        {"selection": None},
        {"command": _command("reactivate_generation")},
        {"selection": None, "command": {"operation": "unknown"}},
    ],
)
async def test_invalid_request_never_posts(changes):
    calls = []
    async with _client(lambda request: calls.append(request)) as client:
        with pytest.raises(ValueError, match="authority_invalid"):
            await client(**({"session_token_sha256": _SESSION, "actor": _ACTOR, "selection": _SELECTION} | changes))
    assert calls == []


async def test_oversized_encoded_request_is_rejected_before_post():
    calls = []
    selection = [{"record_kind": "\0" * 32, "record_id": "\0" * 128, "revision": 3}] * 100
    async with _client(lambda request: calls.append(request)) as client:
        with pytest.raises(ValueError, match="authority_invalid"):
            await client(session_token_sha256=_SESSION, actor=_ACTOR, selection=selection)
    assert calls == []


async def test_external_cancellation_closes_response_without_returning_authority():
    entered = asyncio.Event()

    class Waiting(_Stream):
        async def __aiter__(self):
            entered.set()
            await asyncio.Event().wait()
            yield b"unreachable"

    stream = Waiting([])
    async with _client(lambda request: _reply(stream=stream)) as client:
        task = asyncio.create_task(_validate(client))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert stream.closed


@pytest.mark.parametrize("token", ["", "private token", "private\r\ntoken", "λ", "x" * 4097, None])
async def test_invalid_service_bearer_never_constructs_a_pool(token):
    with pytest.raises(ValueError) as caught:
        _client(lambda request: None, service_token=token)
    assert str(caught.value) == "registry_publication_authority_invalid"
    assert caught.value.__context__ is None


async def test_pool_uses_verified_tls_no_environment_proxy_and_no_redirects(monkeypatch):
    constructed_clients = []
    original = httpx.AsyncClient

    def create(**options):
        constructed_clients.append(options)
        return original(**options)

    monkeypatch.setattr(httpx, "AsyncClient", create)
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.example.invalid:1")
    async with _client(lambda request: _reply(_receipt(json.loads(request.content)))) as client:
        assert await _validate(client) == 5
        assert await _validate(client) == 5
    assert len(constructed_clients) == 1
    assert constructed_clients[0]["trust_env"] is False
    assert constructed_clients[0]["verify"] is True
    assert constructed_clients[0]["follow_redirects"] is False
    assert constructed_clients[0]["timeout"].connect == 5
    assert constructed_clients[0]["timeout"].read == 15


async def test_selection_order_and_typed_identity_are_not_rewritten(monkeypatch):
    selected_records = [
        {"record_kind": "provider", "record_id": "44444444-4444-4444-8444-444444444444", "revision": 4},
        {"record_kind": "membership", "record_id": 17, "revision": 3},
    ]
    monkeypatch.setattr(__import__(__name__, fromlist=["_SELECTION"]), "_SELECTION", selected_records)

    async def handler(request):
        document = json.loads(request.content)
        assert document["selection"] == selected_records
        return _reply(_receipt(document))

    async with _client(handler) as client:
        assert await _validate(client) == 5
