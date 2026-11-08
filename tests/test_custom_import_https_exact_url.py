# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact signed-object URL behavior with the ordinary HTTPS security boundary."""

import aiohttp
import pytest
from yarl import URL

from process.custom_import import https
from tests.test_custom_import_https import (
    _NATIVE_REQUEST,
    _PAYLOAD,
    _acquire,
    _assert_closed,
    _Response,
)
from tests.test_custom_import_https import no_network as no_network
from tests.test_custom_import_https import transport as transport

_EXACT_URL = "https://downloads.example/bucket/a/../caf%C3%A9%2B%25?versionId=a%2Fb%2Bc%20d&token=x%2Fy"


async def test_exact_mode_preserves_path_and_query_octets(transport):
    transport.responses = [response := _Response()]
    assert (await _acquire(url=_EXACT_URL, exact_url=True)).payload == _PAYLOAD
    sent_url = transport.requests[0][1]
    assert sent_url.raw_path == "/bucket/a/../caf%C3%A9%2B%25"
    assert sent_url.raw_query_string == "versionId=a%2Fb%2Bc%20d&token=x%2Fy"
    assert sent_url != URL(_EXACT_URL)
    _assert_closed(transport, [response])


async def test_native_client_keeps_exact_octets_and_no_ambient_authority(transport, monkeypatch):
    attempts = []

    async def disconnect(connector, request, **_options):
        attempts.append(request)
        await connector._resolve_host(request.url.raw_host, request.url.port)
        raise aiohttp.ServerDisconnectedError("synthetic-disconnect")

    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.example:8080")
    monkeypatch.setattr(aiohttp.client.ClientSession, "_request", _NATIVE_REQUEST)
    monkeypatch.setattr(aiohttp.connector.TCPConnector, "connect", disconnect)
    with pytest.raises(https.HttpsAcquisitionError):
        await _acquire(url=_EXACT_URL, exact_url=True)
    assert len(attempts) == 1
    request = attempts[0]
    assert request.url.raw_path == "/bucket/a/../caf%C3%A9%2B%25"
    assert request.url.raw_query_string == "versionId=a%2Fb%2Bc%20d&token=x%2Fy"
    assert request.proxy is None and request.ssl is True
    assert "Authorization" not in request.headers and "Cookie" not in request.headers
    _assert_closed(transport)


@pytest.mark.parametrize("status", (301, 302, 303, 307, 308))
async def test_exact_mode_rejects_even_same_origin_redirects(transport, status):
    transport.responses = [response := _Response(status=status, headers=(("Location", "/next"),))]
    with pytest.raises(https.HttpsAcquisitionError, match="https_redirect_denied"):
        await _acquire(exact_url=True)
    assert len(transport.requests) == 1
    _assert_closed(transport, [response])


@pytest.mark.parametrize("value", (None, 0, 1, "true"))
async def test_exact_mode_is_an_explicit_boolean_before_io(transport, value):
    with pytest.raises(https.HttpsAcquisitionError, match="https_request_invalid"):
        await _acquire(exact_url=value)
    assert not transport.requests and not transport.sessions


@pytest.mark.parametrize(
    "url",
    (
        "http://downloads.example/object",
        "https://127.0.0.1/object",
        "https://169.254.169.254/object",
        "https://user@downloads.example/object",
        "https://downloads.example/object#fragment",
    ),
)
async def test_exact_mode_keeps_raw_authority_and_scheme_guards(transport, url):
    with pytest.raises(https.HttpsAcquisitionError):
        await _acquire(url=url, exact_url=True)
    assert not transport.requests and not transport.sessions


async def test_exact_mode_keeps_actual_resolver_destination_checks(transport):
    transport.responses = [_Response()]
    transport.dns_answers = [["8.8.8.8", "127.0.0.1"]]
    with pytest.raises(https.HttpsAcquisitionError):
        await _acquire(exact_url=True)
    assert transport.dns_calls
    _assert_closed(transport)
