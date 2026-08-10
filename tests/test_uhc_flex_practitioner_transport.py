# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused security and failure tests for exact Flex Practitioner HTTP."""

from __future__ import annotations

import asyncio
import datetime as dt
import email.utils
import json

import aiohttp
import pytest

from process.uhc_flex_official_cohort_contract import (
    UHC_FLEX_OFFICIAL_NPI_SYSTEM,
)
from process.uhc_flex_practitioner_query import (
    uhc_flex_practitioner_query_url,
)
from process import uhc_flex_practitioner_transport as transport


REQUESTED_NPI = 1234567893
OTHER_NPI = 1588616783


class _Content:
    def __init__(self, chunks):
        self._chunks = tuple(chunks)

    async def iter_chunked(self, chunk_size):
        assert chunk_size == transport.UHC_FLEX_PRACTITIONER_RESPONSE_CHUNK_BYTES
        for chunk in self._chunks:
            if isinstance(chunk, BaseException):
                raise chunk
            yield chunk


class _Response:
    def __init__(
        self,
        chunks=(),
        *,
        status=200,
        headers=None,
        url=None,
    ):
        self.content = _Content(chunks)
        self.status = status
        self.headers = dict(headers or {})
        self.url = url or uhc_flex_practitioner_query_url(REQUESTED_NPI)
        self.entered = False
        self.exited = False

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, *_args):
        self.exited = True
        return False


class _Session:
    def __init__(self, response=None, *, request_error=None):
        self.response = response
        self.request_error = request_error
        self.requests = []

    def get(self, url, **request_options):
        self.requests.append((url, request_options))
        if self.request_error is not None:
            raise self.request_error
        return self.response


def _bundle(resources=()):
    return {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": len(resources),
        "entry": [{"resource": resource} for resource in resources],
    }


def _practitioner(*, npi=REQUESTED_NPI):
    return {
        "resourceType": "Practitioner",
        "id": "practitioner-a",
        "identifier": [
            {
                "system": UHC_FLEX_OFFICIAL_NPI_SYSTEM,
                "value": str(npi),
            }
        ],
    }


def _response_from_body(
    body,
    *,
    chunks=None,
    status=200,
    headers=None,
    url=None,
):
    if isinstance(body, dict):
        body = json.dumps(body, separators=(",", ":")).encode()
    response_headers = {
        "Content-Type": "application/fhir+json; charset=utf-8",
        "Content-Encoding": "identity",
        "Content-Length": str(len(body)),
        **(headers or {}),
    }
    return _Response(
        chunks if chunks is not None else (body,),
        status=status,
        headers=response_headers,
        url=url,
    )


async def _transport_error(session, **request_options):
    with pytest.raises(transport.UHCFlexPractitionerTransportError) as error_info:
        await transport.fetch_uhc_flex_practitioner(
            session,
            REQUESTED_NPI,
            **request_options,
        )
    return error_info.value


@pytest.mark.asyncio
async def test_exact_get_uses_fhir_identity_headers_and_bounded_timeout():
    body = json.dumps(_bundle([_practitioner()]), separators=(",", ":")).encode()
    response = _response_from_body(body, chunks=(body[:7], body[7:]))
    session = _Session(response)
    progress_events = []
    cancel_events = []

    async def cancel_check():
        cancel_events.append("checked")

    async def progress_callback(phase, byte_count):
        progress_events.append((phase, byte_count))

    result = await transport.fetch_uhc_flex_practitioner(
        session,
        REQUESTED_NPI,
        cancel_check=cancel_check,
        progress_callback=progress_callback,
    )

    assert result.resource_ids == ("practitioner-a",)
    assert result.requested_npi == REQUESTED_NPI
    assert len(cancel_events) == 6
    assert progress_events == [
        ("request_started", 0),
        ("response_bytes", 7),
        ("response_bytes", len(body)),
        ("response_validated", len(body)),
    ]
    assert len(session.requests) == 1
    request_url, request_options = session.requests[0]
    assert request_url == uhc_flex_practitioner_query_url(REQUESTED_NPI)
    assert request_options["headers"] == {
        "Accept": "application/fhir+json",
        "Accept-Encoding": "identity",
    }
    assert request_options["allow_redirects"] is False
    timeout = request_options["timeout"]
    assert type(timeout) is aiohttp.ClientTimeout
    assert timeout.total == transport.UHC_FLEX_PRACTITIONER_TIMEOUT_SECONDS
    assert timeout.connect == 10.0
    assert timeout.sock_connect == 10.0
    assert timeout.sock_read == 20.0
    assert response.entered is True
    assert response.exited is True


@pytest.mark.parametrize("status", [408, 423, 425, 429, 500, 503, 599])
@pytest.mark.asyncio
async def test_transient_statuses_are_retryable_with_bounded_retry_after(status):
    response = _Response(
        status=status,
        headers={"Retry-After": "900"},
    )

    error = await _transport_error(_Session(response))

    assert error.code == "http_transient"
    assert error.is_retryable is True
    assert error.retry_after_seconds == 60.0
    assert str(REQUESTED_NPI) not in str(error)


@pytest.mark.parametrize("status", [201, 400, 401, 404, 409, 422])
@pytest.mark.asyncio
async def test_other_non_success_statuses_are_terminal(status):
    error = await _transport_error(_Session(_Response(status=status)))

    assert error.code == "http_terminal"
    assert error.is_retryable is False
    assert error.retry_after_seconds == 0.0


@pytest.mark.parametrize("status", [300, 301, 302, 307, 308, 399])
@pytest.mark.asyncio
async def test_redirects_are_terminal_and_never_followed(status):
    response = _Response(
        status=status,
        headers={"Location": "https://example.test/secret"},
    )
    session = _Session(response)

    error = await _transport_error(session)

    assert error.code == "redirect_forbidden"
    assert error.is_retryable is False
    assert session.requests[0][1]["allow_redirects"] is False
    assert "example.test" not in str(error)


@pytest.mark.asyncio
async def test_response_url_must_remain_the_exact_trusted_query():
    response = _response_from_body(
        _bundle(),
        url="https://example.test/fhirpublic/R4/Practitioner",
    )

    error = await _transport_error(_Session(response))

    assert error.code == "response_url_invalid"
    assert error.is_retryable is False
    assert "example.test" not in str(error)


@pytest.mark.parametrize(
    "content_type",
    [
        "application/json",
        "text/html",
        "application/fhir+json; charset=latin-1",
        "application/fhir+json; profile=https://example.test",
        "",
    ],
)
@pytest.mark.asyncio
async def test_response_requires_exact_fhir_utf8_media_type(content_type):
    response = _response_from_body(
        _bundle(),
        headers={"Content-Type": content_type},
    )

    error = await _transport_error(_Session(response))

    assert error.code == "content_type_invalid"
    assert error.is_retryable is False


@pytest.mark.parametrize("encoding", ["gzip", "br", "deflate", "identity, gzip"])
@pytest.mark.asyncio
async def test_response_rejects_encoded_content(encoding):
    response = _response_from_body(
        _bundle(),
        headers={"Content-Encoding": encoding},
    )

    error = await _transport_error(_Session(response))

    assert error.code == "content_encoding_invalid"
    assert error.is_retryable is False


@pytest.mark.asyncio
async def test_streaming_and_declared_body_caps_fail_before_unbounded_read():
    declared_response = _response_from_body(
        _bundle(),
        headers={"Content-Length": "11"},
    )
    declared_error = await _transport_error(
        _Session(declared_response),
        max_response_bytes=10,
    )

    streamed_response = _response_from_body(
        b"12345678901",
        chunks=(b"123456", b"78901"),
        headers={"Content-Length": "10"},
    )
    streamed_error = await _transport_error(
        _Session(streamed_response),
        max_response_bytes=10,
    )

    assert declared_error.code == "body_too_large"
    assert streamed_error.code == "body_too_large"
    assert declared_error.is_retryable is False
    assert streamed_error.is_retryable is False


@pytest.mark.asyncio
async def test_short_or_payload_error_response_is_retryable_truncation():
    short_response = _response_from_body(
        b"{}",
        headers={"Content-Length": "3"},
    )
    short_error = await _transport_error(_Session(short_response))

    payload_error_response = _response_from_body(
        b"{}",
        chunks=(b"{", aiohttp.ClientPayloadError("contains sensitive data")),
    )
    payload_error = await _transport_error(_Session(payload_error_response))

    assert short_error.code == "payload_truncated"
    assert short_error.is_retryable is True
    assert payload_error.code == "payload_truncated"
    assert payload_error.is_retryable is True
    assert "sensitive" not in str(payload_error)


@pytest.mark.parametrize(
    ("request_error", "expected_code"),
    [
        (asyncio.TimeoutError("contains sensitive data"), "transport_timeout"),
        (ConnectionResetError("contains sensitive data"), "transport_connection"),
        (
            aiohttp.ClientConnectionError("contains sensitive data"),
            "transport_connection",
        ),
    ],
)
@pytest.mark.asyncio
async def test_timeout_and_connection_errors_are_retryable_and_sanitized(
    request_error,
    expected_code,
):
    error = await _transport_error(
        _Session(request_error=request_error)
    )

    assert error.code == expected_code
    assert error.is_retryable is True
    assert "sensitive" not in str(error)
    assert str(REQUESTED_NPI) not in str(error)


@pytest.mark.parametrize(
    "raw_body",
    [
        b'{"resourceType":"Bundle","resourceType":"Bundle","type":"searchset"}',
        b'{"resourceType":"Bundle","type":"searchset","total":NaN}',
        b'{"resourceType":"Bundle","type":"searchset","total":1e999}',
        b'{"resourceType":"Bundle","type":"searchset"} trailing',
        b"\xff",
        b"[]",
    ],
)
@pytest.mark.asyncio
async def test_json_parser_rejects_duplicates_nonfinite_trailing_and_non_object(
    raw_body,
):
    error = await _transport_error(
        _Session(_response_from_body(raw_body))
    )

    assert error.code == "json_invalid"
    assert error.is_retryable is False


@pytest.mark.asyncio
async def test_bundle_validation_failure_is_terminal_and_retains_no_npi():
    response = _response_from_body(_bundle([_practitioner(npi=OTHER_NPI)]))

    error = await _transport_error(_Session(response))

    assert error.code == "response_validation"
    assert error.validation_code == "cross_npi"
    assert error.is_retryable is False
    assert str(REQUESTED_NPI) not in str(error)
    assert str(OTHER_NPI) not in str(error)


@pytest.mark.asyncio
async def test_cancel_before_request_and_during_stream_is_cooperative():
    before_session = _Session(_response_from_body(_bundle()))

    with pytest.raises(asyncio.CancelledError, match="cancellation requested"):
        await transport.fetch_uhc_flex_practitioner(
            before_session,
            REQUESTED_NPI,
            cancel_check=lambda: True,
        )
    assert before_session.requests == []

    body = json.dumps(_bundle(), separators=(",", ":")).encode()
    during_session = _Session(
        _response_from_body(body, chunks=(body[:1], body[1:]))
    )
    cancel_events = []
    progress_events = []

    def cancel_during_stream():
        cancel_events.append("checked")
        return len(cancel_events) == 4

    with pytest.raises(asyncio.CancelledError, match="cancellation requested"):
        await transport.fetch_uhc_flex_practitioner(
            during_session,
            REQUESTED_NPI,
            cancel_check=cancel_during_stream,
            progress_callback=lambda phase, count: progress_events.append(
                (phase, count)
            ),
        )

    assert progress_events == [
        ("request_started", 0),
        ("response_bytes", 1),
    ]
    assert during_session.response.exited is True


@pytest.mark.asyncio
async def test_callback_errors_are_terminal_and_sanitized():
    def broken_progress(_phase, _byte_count):
        raise RuntimeError(f"callback leaked {REQUESTED_NPI}")

    error = await _transport_error(
        _Session(_response_from_body(_bundle())),
        progress_callback=broken_progress,
    )

    assert error.code == "callback_failed"
    assert error.is_retryable is False
    assert str(REQUESTED_NPI) not in str(error)


def test_retry_after_accepts_delta_or_date_but_never_exceeds_bound():
    future = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=15)

    assert transport.uhc_flex_practitioner_retry_after_seconds("2.5") == 2.5
    assert transport.uhc_flex_practitioner_retry_after_seconds("9999") == 60.0
    assert transport.uhc_flex_practitioner_retry_after_seconds("-3") == 0.0
    assert transport.uhc_flex_practitioner_retry_after_seconds("invalid") == 0.0
    date_delay = transport.uhc_flex_practitioner_retry_after_seconds(
        email.utils.format_datetime(future, usegmt=True)
    )
    assert 13.0 <= date_delay <= 15.0


@pytest.mark.parametrize(
    ("npi", "max_response_bytes", "timeout_seconds"),
    [
        (str(REQUESTED_NPI), 10, 1),
        (REQUESTED_NPI, 0, 1),
        (REQUESTED_NPI, 10, 0),
        (REQUESTED_NPI, 10, 301),
    ],
)
@pytest.mark.asyncio
async def test_invalid_request_bounds_fail_before_network(
    npi,
    max_response_bytes,
    timeout_seconds,
):
    session = _Session(_response_from_body(_bundle()))

    with pytest.raises(transport.UHCFlexPractitionerTransportError) as error_info:
        await transport.fetch_uhc_flex_practitioner(
            session,
            npi,
            max_response_bytes=max_response_bytes,
            timeout_seconds=timeout_seconds,
        )

    assert error_info.value.code == "request_invalid"
    assert session.requests == []
