# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compressed reviewed-directory responses keep the existing transport bounds."""

import asyncio
import gzip
import json

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from tests.test_provider_directory_credentialed_http import _reviewed_source, directory

PAYLOAD = {
    "resourceType": "Bundle",
    "type": "searchset",
    "entry": [{"resource": {"resourceType": "Location", "id": "office", "name": "R\u016bta"}}],
}
BODY = json.dumps(PAYLOAD, ensure_ascii=False).encode()


async def _fetch_response(monkeypatch, body, encoding, *, persistent, status=200, extra_headers=None):
    observed_headers = []

    async def page(request):
        observed_headers.append(dict(request.headers))
        return web.Response(body=body, status=status, headers={"Content-Encoding": encoding, **(extra_headers or {})})

    http_app = web.Application()
    http_app.router.add_get("/fhir/Location", page)
    async with TestServer(http_app) as server:
        base = str(server.make_url("/fhir"))
        source_by_field, credential_spec_by_field = _reviewed_source(monkeypatch, base, str(server.make_url("/token")))
        credential_spec_by_field.clear()
        credential_spec_by_field["bearer_token"] = "synthetic-token"
        monkeypatch.delenv(directory.SOURCE_HTTP_KEEPALIVE_ENV, raising=False)
        async with directory._source_http_session_scope(force=persistent):
            fetch_result = await directory._fetch_source_json_once(source_by_field, base + "/Location", timeout=2)
    assert observed_headers[0]["Accept-Encoding"] == "gzip"
    assert observed_headers[0]["Authorization"] == "Bearer synthetic-token"
    return fetch_result


@pytest.mark.parametrize("persistent", [False, True])
@pytest.mark.parametrize("encoding", ["identity", "gzip", "members"])
async def test_reviewed_gzip_and_identity_decode_identically(monkeypatch, persistent, encoding):
    body = BODY
    if encoding == "gzip":
        body = gzip.compress(body)
    elif encoding == "members":
        midpoint = len(body) // 2
        body = gzip.compress(body[:midpoint]) + gzip.compress(body[midpoint:])
    fetch_result = await _fetch_response(
        monkeypatch, body, "gzip" if encoding == "members" else encoding, persistent=persistent
    )
    assert fetch_result[:3] == (200, PAYLOAD, None)


def _invalid_gzip(kind):
    body = gzip.compress(BODY)
    if kind == "magic":
        return b"invalid gzip"
    if kind == "truncated":
        return body[:-1]
    if kind == "crc":
        return body[:-8] + bytes([body[-8] ^ 1]) + body[-7:]
    return body + gzip.compress(b" ")[:-1]


@pytest.mark.parametrize("persistent", [False, True])
@pytest.mark.parametrize("kind", ["magic", "truncated", "crc", "later_member"])
async def test_invalid_gzip_cannot_become_successful_empty_data(monkeypatch, persistent, kind):
    fetch_result = await _fetch_response(monkeypatch, _invalid_gzip(kind), "gzip", persistent=persistent)
    assert fetch_result[0] is None and fetch_result[1] is None
    assert "provider_directory_response_gzip_invalid" in fetch_result[2]


@pytest.mark.parametrize("persistent", [False, True])
@pytest.mark.parametrize("encoding", ["br", "gzip, identity"])
async def test_unsupported_content_encoding_is_refused(monkeypatch, persistent, encoding):
    fetch_result = await _fetch_response(monkeypatch, BODY, encoding, persistent=persistent)
    assert fetch_result[0] is None and fetch_result[1] is None
    assert "provider_directory_response_encoding_unsupported" in fetch_result[2]


@pytest.mark.parametrize("persistent", [False, True])
@pytest.mark.parametrize("bound", ["wire", "decoded", "exact"])
async def test_gzip_wire_and_decoded_bodies_are_independently_bounded(monkeypatch, persistent, bound):
    limit = 1024
    monkeypatch.setattr(directory, "MAX_FHIR_JSON_BODY_BYTES", limit)
    if bound == "wire":
        compressed = gzip.compress(BODY)
        body = compressed + b"\x00" * (limit + 1 - len(compressed))
    else:
        raw = BODY + b" " * (limit - len(BODY) + (1 if bound == "decoded" else 0))
        body = gzip.compress(raw)
        assert len(body) < limit
    fetch_result = await _fetch_response(monkeypatch, body, "gzip", persistent=persistent)
    if bound == "exact":
        assert fetch_result[:3] == (200, PAYLOAD, None)
    else:
        assert fetch_result[0] is None and fetch_result[1] is None
        assert f"provider_directory_response_{bound}_limit_exceeded" in fetch_result[2]


@pytest.mark.parametrize("persistent", [False, True])
@pytest.mark.parametrize("valid_body", [False, True])
async def test_compressed_error_bodies_keep_status_and_retry_after(monkeypatch, persistent, valid_body):
    body = gzip.compress(json.dumps({"resourceType": "OperationOutcome"}).encode()) if valid_body else b"invalid"
    fetch_result = await _fetch_response(
        monkeypatch, body, "gzip", persistent=persistent, status=429, extra_headers={"Retry-After": "1200"}
    )
    assert fetch_result[0] == 429 and fetch_result[2] is None
    assert directory._source_retry_after_seconds(fetch_result[1], max_delay_seconds=None) == 1200


@pytest.mark.parametrize("persistent", [False, True])
@pytest.mark.parametrize("bound", ["wire", "decoded"])
async def test_compressed_errors_keep_the_stricter_error_body_cap(monkeypatch, persistent, bound):
    limit = 1024 * 1024
    if bound == "wire":
        compressed = gzip.compress(BODY)
        body = compressed + b"\x00" * (limit + 1 - len(compressed))
    else:
        body = gzip.compress(BODY + b" " * (limit + 1 - len(BODY)))
    fetch_result = await _fetch_response(
        monkeypatch, body, "gzip", persistent=persistent, status=429, extra_headers={"Retry-After": "1200"}
    )
    assert fetch_result[:3] == (429, {directory.SOURCE_RETRY_AFTER_FIELD: "1200"}, None)


async def test_gzip_stream_keeps_the_existing_read_timeout(monkeypatch):
    release_event = asyncio.Event()

    async def page(request):
        response = web.StreamResponse(headers={"Content-Encoding": "gzip"})
        await response.prepare(request)
        await response.write(gzip.compress(BODY)[:8])
        await release_event.wait()
        return response

    http_app = web.Application()
    http_app.router.add_get("/fhir/Location", page)
    async with TestServer(http_app) as server:
        base = str(server.make_url("/fhir"))
        source_by_field, credential_spec_by_field = _reviewed_source(monkeypatch, base, str(server.make_url("/token")))
        credential_spec_by_field.clear()
        credential_spec_by_field["bearer_token"] = "synthetic-token"
        try:
            async with directory._source_http_session_scope(force=True):
                fetch_result = await directory._fetch_source_json_once(
                    source_by_field, base + "/Location", timeout=0.02
                )
        finally:
            release_event.set()
    assert fetch_result[0] is None and "timeout" in fetch_result[2].lower()
