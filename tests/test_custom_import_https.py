# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic HTTPS acquisition checks; no DNS lookup or socket is permitted."""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import socket
import traceback
from dataclasses import replace
from tempfile import SpooledTemporaryFile
from types import SimpleNamespace

import aiohttp
import pytest
from multidict import CIMultiDict

from process.custom_import import https
from process.custom_import.capture import iter_records, verify_capture
from process.custom_import.capture_limits import CaptureLimits
from process.custom_import.definition import SourceStream

_PAYLOAD = b"id,name\n1,example\n"
_URL = "https://downloads.example/records.csv?sample=redaction-marker"
_STREAM = SourceStream("root", "root", None, "csv", "none", None, "declared-snapshot")
_LIMITS = CaptureLimits(maximum_compressed_bytes=1024, maximum_decoded_bytes=4096, maximum_record_bytes=512)
_NATIVE_REQUEST = aiohttp.ClientSession._request


class _Response:
    """Small native request-context response, with controllable body failures."""

    def __init__(self, chunks=(_PAYLOAD,), *, status=200, headers=(), pause=False):
        self.status = status
        self.headers = CIMultiDict(headers)
        self.chunks = chunks
        self.should_pause = pause
        self.started = asyncio.Event()
        self.closed = False
        self.content = self
        self.reads = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        self.closed = True

    async def iter_chunked(self, chunk_bytes):
        assert chunk_bytes == _LIMITS.read_chunk_bytes
        self.started.set()
        if self.should_pause:
            await asyncio.Event().wait()
        for chunk in self.chunks:
            self.reads += 1
            if isinstance(chunk, BaseException):
                raise chunk
            yield chunk


class _Resolver:
    """Fake only the DNS backend, leaving strict and native connector checks real."""

    def __init__(self, transport):
        self.transport = transport
        self.closed = False

    async def resolve(self, host, port=0, family=0):
        self.transport.dns_calls.append((host, port, family))
        addresses = self.transport.dns_answers[0]
        if len(self.transport.dns_answers) > 1:
            self.transport.dns_answers.pop(0)
        if isinstance(addresses, BaseException):
            raise addresses
        return [
            {
                "hostname": host,
                "host": address,
                "port": port,
                "family": socket.AF_INET6 if ":" in address else socket.AF_INET,
                "proto": 0,
                "flags": socket.AI_NUMERICHOST,
            }
            for address in addresses
        ]

    async def close(self):
        self.closed = True


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    """A swallowed guard exception must still fail the synthetic test."""
    attempts = []

    def unexpected_network(*_args, **_kwargs):
        attempts.append(True)
        raise AssertionError("real network access is forbidden")

    monkeypatch.setattr(socket, "getaddrinfo", unexpected_network)
    monkeypatch.setattr(aiohttp.connector.aiohappyeyeballs, "start_connection", unexpected_network)
    yield
    assert not attempts


@pytest.fixture
def transport(monkeypatch):
    """Keep native session/connector lifetimes, replacing only request I/O and DNS."""
    state = SimpleNamespace(
        responses=[],
        dns_answers=[["8.8.8.8"]],
        dns_calls=[],
        requests=[],
        resolvers=[],
        sessions=[],
        connectors=[],
        spools=[],
    )
    session_type = aiohttp.ClientSession
    connector_type = aiohttp.TCPConnector

    def make_session(**kwargs):
        session = session_type(**kwargs)
        state.sessions.append(session)
        return session

    def make_connector(**kwargs):
        connector = connector_type(**kwargs)
        state.connectors.append(connector)
        return connector

    def make_resolver():
        resolver = _Resolver(state)
        state.resolvers.append(resolver)
        return resolver

    def make_spool(**kwargs):
        assert kwargs == {"max_size": _LIMITS.maximum_compressed_bytes, "mode": "w+b"}
        spool = SpooledTemporaryFile(max_size=1, mode="w+b")
        state.spools.append(spool)
        return spool

    async def request(session, method, url, **kwargs):
        state.requests.append((method, url, kwargs))
        await session.connector._resolve_host(url.raw_host, url.port)
        response = state.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response

    monkeypatch.setattr(session_type, "_request", request)
    monkeypatch.setattr(aiohttp, "ClientSession", make_session)
    monkeypatch.setattr(aiohttp, "TCPConnector", make_connector)
    monkeypatch.setattr(aiohttp, "DefaultResolver", make_resolver)
    monkeypatch.setattr(https, "SpooledTemporaryFile", make_spool)
    return state


async def _acquire(**overrides):
    argument_by_name = {
        "url": _URL,
        "source_stream": _STREAM,
        "source_snapshot_token": "snapshot-one",
        "expected_sha256": hashlib.sha256(_PAYLOAD).hexdigest(),
        "expected_bytes": len(_PAYLOAD),
        "limits": _LIMITS,
        "timeout_seconds": 5,
    }
    argument_by_name.update(overrides)
    return await https.acquire_https(**argument_by_name)


def _assert_closed(transport, responses=()):
    assert all(session.closed for session in transport.sessions)
    assert all(connector.closed for connector in transport.connectors)
    assert all(resolver.closed for resolver in transport.resolvers)
    assert all(spool.closed for spool in transport.spools)
    assert all(response.closed for response in responses)


def _assert_redacted(error):
    rendered = "".join(traceback.format_exception(error))
    assert "redaction-marker" not in str(error)
    assert "driver-details" not in rendered
    assert "https://downloads.example/records.csv?sample=redaction-marker" not in rendered
    assert error.__cause__ is None


@pytest.mark.parametrize("length_header", [(), (("Content-Length", str(len(_PAYLOAD))),)])
async def test_exact_bytes_reuse_capture(transport, length_header):
    response = _Response((_PAYLOAD[:4], _PAYLOAD[4:]), headers=length_header)
    transport.responses = [response]

    captured = await _acquire()

    assert captured.payload == _PAYLOAD
    assert captured.manifest.source_snapshot_token == "snapshot-one"
    assert captured.manifest.compressed_sha256 == hashlib.sha256(_PAYLOAD).hexdigest()
    verify_capture(captured, _STREAM, limits=_LIMITS)
    assert [record.values for record in iter_records(captured, _STREAM, limits=_LIMITS)] == [
        {"id": "1", "name": "example"}
    ]
    assert transport.spools[0]._rolled
    _assert_closed(transport, [response])


async def test_raw_gzip_uses_existing_decoder(transport):
    compressed = gzip.compress(_PAYLOAD, mtime=0)
    stream = replace(_STREAM, compression="gzip")
    response = _Response((compressed,), headers=(("Content-Encoding", "identity"),))
    transport.responses = [response]

    captured = await _acquire(
        source_stream=stream, expected_bytes=len(compressed), expected_sha256=hashlib.sha256(compressed).hexdigest()
    )

    assert captured.payload == compressed
    assert captured.manifest.decoded_sha256 == hashlib.sha256(_PAYLOAD).hexdigest()
    assert len(list(iter_records(captured, stream, limits=_LIMITS))) == 1
    _assert_closed(transport, [response])


async def test_session_has_no_ambient_authority(transport, monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.example:8080")
    monkeypatch.setenv("NETRC", "/not-read/example-netrc")
    monkeypatch.setenv("HLTHPRT_FETCH_ALLOW_LOCAL", "true")
    transport.responses = [_Response()]

    await _acquire()

    session = transport.sessions[0]
    connector = transport.connectors[0]
    assert session.trust_env is False and session.auth is None
    assert session.auto_decompress is False and session._retry_connection is False
    assert session.headers == {"Accept-Encoding": "identity"}
    assert isinstance(session.cookie_jar, aiohttp.DummyCookieJar)
    assert connector._ssl is True and connector.force_close and not connector.use_dns_cache
    assert transport.requests[0][0] == "GET"
    assert transport.requests[0][2] == {"allow_redirects": False, "proxy": None, "ssl": True}
    _assert_closed(transport)


async def test_native_client_does_not_retry(transport, monkeypatch):
    """Exercise aiohttp's actual retry/auth path with only its connection faked."""
    attempts = []

    async def disconnect(connector, request, **_kwargs):
        attempts.append(request)
        await connector._resolve_host(request.url.raw_host, request.url.port)
        raise aiohttp.ServerDisconnectedError(f"driver-details {_URL}")

    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.example:8080")
    monkeypatch.setenv("NETRC", "/not-read/example-netrc")
    monkeypatch.setattr(aiohttp.client.ClientSession, "_request", _NATIVE_REQUEST)
    monkeypatch.setattr(aiohttp.connector.TCPConnector, "connect", disconnect)

    with pytest.raises(https.HttpsAcquisitionError, match="https_acquisition_failed") as caught:
        await _acquire()

    assert len(attempts) == 1
    assert attempts[0].proxy is None and attempts[0].ssl is True
    assert "Authorization" not in attempts[0].headers and "Cookie" not in attempts[0].headers
    assert attempts[0].headers["Accept-Encoding"] == "identity"
    _assert_redacted(caught.value)
    _assert_closed(transport)


@pytest.mark.parametrize(
    "url",
    [
        None,
        "",
        "http://downloads.example/object",
        "file:///example.csv",
        "/relative.csv",
        "https:///missing",
        "https://user:password@downloads.example/object",
        "https://@downloads.example/object",
        "https://downloads.example/object#fragment",
        "https://downloads.example/object#",
        "https://downloads.example:0/object",
        "https://downloads.example:65536/object",
        "https://downloads.example/line\nfeed",
        " https://downloads.example/object",
        "https://127.0.0.1/object",
        "https://10.0.0.1/object",
        "https://169.254.169.254/object",
        "https://100.64.0.1/object",
        "https://198.18.0.1/object",
        "https://[::1]/object",
        "https://[fc00::1]/object",
        "https://[fe80::1%25lo0]/object",
        "https://[ff02::1]/object",
        "https://127.1/object",
        "https://2130706433/object",
        "https://0177.0.0.1/object",
    ],
)
async def test_invalid_urls_never_dispatch(transport, monkeypatch, url):
    monkeypatch.setenv("HLTHPRT_FETCH_ALLOW_LOCAL", "yes")
    with pytest.raises(https.HttpsAcquisitionError) as failure:
        await _acquire(url=url)

    assert not transport.requests and not transport.resolvers and not transport.spools
    _assert_redacted(failure.value)


@pytest.mark.parametrize(
    "arguments",
    [
        {"expected_sha256": "wrong"},
        {"expected_sha256": "A" * 64},
        {"expected_sha256": None},
        {"expected_bytes": -1},
        {"expected_bytes": True},
        {"expected_bytes": 1.5},
        {"expected_bytes": 1025},
        {"limits": None},
        {"timeout_seconds": 0},
        {"timeout_seconds": -1},
        {"timeout_seconds": True},
        {"timeout_seconds": float("nan")},
        {"timeout_seconds": float("inf")},
        {"timeout_seconds": "5"},
        {"source_snapshot_token": ""},
        {"source_snapshot_token": "not\nopaque"},
        {"source_stream": None},
        {"source_stream": replace(_STREAM, format="zip")},
        {"source_stream": replace(_STREAM, compression="zip")},
    ],
)
async def test_invalid_inputs_never_dispatch(transport, arguments):
    with pytest.raises(https.HttpsAcquisitionError):
        await _acquire(**arguments)

    assert not transport.requests and not transport.resolvers and not transport.spools


@pytest.mark.parametrize("address", ["8.8.8.8", "[2606:4700:4700::1111]"])
async def test_global_literals_need_no_dns(transport, address):
    transport.responses = [_Response()]

    assert (await _acquire(url=f"https://{address}/object")).payload == _PAYLOAD
    assert not transport.dns_calls
    _assert_closed(transport)


@pytest.mark.parametrize("answers", [[], ["10.0.0.1"], ["8.8.8.8", "10.0.0.1"], ["::1"], ["100.64.1.1"]])
async def test_connection_dns_is_strict(transport, monkeypatch, answers):
    monkeypatch.setenv("HLTHPRT_FETCH_ALLOW_LOCAL", "yes")
    transport.dns_answers = [answers]
    response = _Response()
    transport.responses = [response]

    with pytest.raises(https.HttpsAcquisitionError) as failure:
        await _acquire()

    assert response.reads == 0
    assert len(transport.dns_calls) == 1
    _assert_redacted(failure.value)
    _assert_closed(transport)


async def test_rebinding_on_redirect_is_denied(transport):
    redirect = _Response(status=302, headers=(("Location", "/second"),))
    transport.responses = [redirect, _Response()]
    transport.dns_answers = [["8.8.8.8"], ["10.0.0.1"]]

    with pytest.raises(https.HttpsAcquisitionError):
        await _acquire()

    assert len(transport.requests) == 2 and len(transport.dns_calls) == 2
    _assert_closed(transport, [redirect])


@pytest.mark.parametrize(
    "url,location",
    [
        ("https://downloads.example/object", "/next.csv"),
        ("https://downloads.example/object", "https://downloads.example:443/next.csv"),
        ("https://downloads.example:443/object", "https://downloads.example/next.csv"),
        ("https://downloads.example:8443/object", "https://downloads.example:8443/next.csv"),
    ],
)
async def test_same_origin_redirect_succeeds(transport, url, location):
    redirect = _Response(status=302, headers=(("Location", location), ("Set-Cookie", "sample=discard")))
    terminal = _Response()
    transport.responses = [redirect, terminal]

    assert (await _acquire(url=url)).payload == _PAYLOAD
    assert len(transport.requests) == 2
    assert not transport.sessions[0].cookie_jar
    _assert_closed(transport, [redirect, terminal])


@pytest.mark.parametrize(
    "location",
    [
        "http://downloads.example/object",
        "https://other.example/object",
        "//other.example/object",
        "https://downloads.example:444/object",
        "https://127.0.0.1/object",
        "https://user@downloads.example/object",
        "https://@downloads.example/object",
        "//@downloads.example/object",
        "#fragment",
        "/next#",
        "/next\npart",
        "",
    ],
)
async def test_redirect_denied_before_next_request(transport, location):
    redirect = _Response(status=302, headers=(("Location", location),))
    transport.responses = [redirect]

    with pytest.raises(https.HttpsAcquisitionError):
        await _acquire()

    assert len(transport.requests) == 1
    _assert_closed(transport, [redirect])


@pytest.mark.parametrize("headers", [(), (("Location", "/one"), ("Location", "/two"))])
async def test_redirect_location_is_unambiguous(transport, headers):
    redirect = _Response(status=307, headers=headers)
    transport.responses = [redirect]

    with pytest.raises(https.HttpsAcquisitionError, match="https_redirect_denied"):
        await _acquire()

    _assert_closed(transport, [redirect])


async def test_redirect_hop_limit_is_finite(transport):
    redirects = [_Response(status=308, headers=(("Location", "/again"),)) for _ in range(4)]
    transport.responses = list(redirects)

    with pytest.raises(https.HttpsAcquisitionError, match="https_redirect_denied"):
        await _acquire()

    assert len(transport.requests) == 4
    _assert_closed(transport, redirects)


@pytest.mark.parametrize(
    "status,headers",
    [
        (206, ()),
        (204, ()),
        (404, ()),
        (200, (("Content-Range", "bytes 0-1/2"),)),
        (200, (("Content-Encoding", "gzip"),)),
        (200, (("Content-Encoding", "identity, gzip"),)),
        (200, (("Content-Encoding", "identity"), ("Content-Encoding", "gzip"))),
        (200, (("Transfer-Encoding", "gzip"),)),
        (200, (("Content-Length", "invalid"),)),
        (200, (("Content-Length", "1"), ("Content-Length", "2"))),
        (200, (("Content-Length", "1"),)),
        (200, (("Content-Length", "999"),)),
        (200, (("Content-Length", str(len(_PAYLOAD))), ("Transfer-Encoding", "chunked"))),
    ],
)
async def test_noncomplete_responses_are_rejected(transport, status, headers):
    response = _Response(status=status, headers=headers)
    transport.responses = [response]

    with pytest.raises(https.HttpsAcquisitionError):
        await _acquire()

    assert response.reads == 0
    _assert_closed(transport, [response])


@pytest.mark.parametrize("chunks", [(_PAYLOAD[:-1],), (_PAYLOAD, b"extra"), (_PAYLOAD + b"x" * 2048,)])
async def test_actual_length_is_authoritative(transport, chunks):
    response = _Response(chunks, headers=(("Content-Length", str(len(_PAYLOAD))),))
    transport.responses = [response]

    with pytest.raises(https.HttpsAcquisitionError, match="https_length_mismatch"):
        await _acquire()

    _assert_closed(transport, [response])


async def test_digest_mismatch_has_no_capture(transport):
    response = _Response()
    transport.responses = [response]

    with pytest.raises(https.HttpsAcquisitionError, match="https_digest_mismatch"):
        await _acquire(expected_sha256="0" * 64)

    _assert_closed(transport, [response])


async def test_decoded_budget_still_applies(transport):
    compressed = gzip.compress(b"a" * 4097, mtime=0)
    response = _Response((compressed,))
    transport.responses = [response]

    with pytest.raises(https.HttpsAcquisitionError, match="https_acquisition_failed"):
        await _acquire(
            source_stream=replace(_STREAM, compression="gzip"),
            expected_bytes=len(compressed),
            expected_sha256=hashlib.sha256(compressed).hexdigest(),
        )

    _assert_closed(transport, [response])


@pytest.mark.parametrize("phase", ["dns", "request", "body"])
async def test_driver_failures_are_redacted(transport, phase):
    failure = aiohttp.ServerDisconnectedError(f"driver-details {_URL}")
    response = _Response((_PAYLOAD[:2], failure))
    if phase == "dns":
        transport.dns_answers = [failure]
    transport.responses = [failure if phase == "request" else response]

    with pytest.raises(https.HttpsAcquisitionError, match="https_acquisition_failed") as caught:
        await _acquire()

    assert len(transport.requests) == 1
    _assert_redacted(caught.value)
    _assert_closed(transport, [response] if phase == "body" else [])


async def test_timeout_closes_owned_resources(transport):
    response = _Response(pause=True)
    transport.responses = [response]

    with pytest.raises(https.HttpsAcquisitionError, match="https_acquisition_timeout"):
        await _acquire(timeout_seconds=0.01)

    _assert_closed(transport, [response])


async def test_cancellation_closes_owned_resources(transport):
    response = _Response(pause=True)
    transport.responses = [response]
    acquisition = asyncio.create_task(_acquire())
    await asyncio.wait_for(response.started.wait(), timeout=1)
    acquisition.cancel(f"driver-details {_URL}")

    with pytest.raises(asyncio.CancelledError) as caught:
        await acquisition

    assert acquisition.cancelled()
    _assert_redacted(caught.value)
    _assert_closed(transport, [response])


async def test_expired_seal_cannot_return(transport, monkeypatch):
    transport.responses = [_Response()]
    original_capture = https.capture_stream
    loop = asyncio.get_running_loop()
    original_time = loop.time

    def expire_after_sealing(*args, **kwargs):
        captured = original_capture(*args, **kwargs)
        monkeypatch.setattr(loop, "time", lambda: original_time() + 10)
        return captured

    monkeypatch.setattr(https, "capture_stream", expire_after_sealing)
    with pytest.raises(https.HttpsAcquisitionError, match="https_acquisition_timeout"):
        await _acquire()

    _assert_closed(transport)


async def test_seal_cancellation_is_observed(transport, monkeypatch):
    transport.responses = [_Response()]
    original_capture = https.capture_stream
    loop = asyncio.get_running_loop()

    def cancel_after_sealing(*args, **kwargs):
        captured = original_capture(*args, **kwargs)
        loop.call_soon(asyncio.current_task().cancel, f"driver-details {_URL}")
        return captured

    monkeypatch.setattr(https, "capture_stream", cancel_after_sealing)
    acquisition = asyncio.create_task(_acquire())
    with pytest.raises(asyncio.CancelledError) as caught:
        await acquisition

    assert acquisition.cancelled()
    _assert_redacted(caught.value)
    _assert_closed(transport)
