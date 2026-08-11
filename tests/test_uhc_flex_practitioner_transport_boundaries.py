# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Boundary coverage for the bounded Flex Practitioner HTTP transport."""

import asyncio
import datetime as dt

import aiohttp
import pytest

from process import uhc_flex_practitioner_transport as transport
from process.uhc_flex_practitioner_query import uhc_flex_practitioner_query_url


REQUESTED_NPI = 1234567893
OTHER_NPI = 1588616783


class _Content:
    def __init__(self, chunks):
        self.chunks = chunks

    async def iter_chunked(self, _chunk_size):
        for chunk in self.chunks:
            yield chunk


class _Response:
    def __init__(self, chunks=(), *, headers=None, url=None):
        self.content = _Content(chunks)
        self.headers = headers or {}
        self.url = url or uhc_flex_practitioner_query_url(REQUESTED_NPI)


class _FailingSession:
    def __init__(self, error):
        self.error = error

    def get(self, *_args, **_kwargs):
        raise self.error


@pytest.mark.parametrize("value", [True, "1", None, float("nan"), float("inf")])
def test_retry_after_bound_rejects_nonfinite_or_nonnumeric_values(value):
    assert transport._bounded_retry_after_value(value) == 0.0


def test_transport_error_sanitizes_unknown_code_and_retry_metadata():
    error = transport.UHCFlexPractitionerTransportError(
        "provider-secret",
        retryable=True,
        retry_after_seconds=float("nan"),
        validation_code="secret",
    )

    assert error.code == "transport_failure"
    assert error.retry_after_seconds == 0.0
    assert error.validation_code is None
    assert "provider-secret" not in str(error)

    validation_error = transport.UHCFlexPractitionerTransportError(
        "response_validation",
        validation_code="secret",
    )
    assert validation_error.validation_code is None


def test_retry_after_accepts_naive_http_date(monkeypatch):
    retry_at = dt.datetime.now(dt.UTC).replace(tzinfo=None) + dt.timedelta(seconds=2)
    monkeypatch.setattr(
        transport.email.utils,
        "parsedate_to_datetime",
        lambda _header: retry_at,
    )

    assert 0.0 < transport.uhc_flex_practitioner_retry_after_seconds("date") <= 2.0


def test_header_lookup_handles_missing_mapping_and_folded_name():
    assert transport._header_value(None, "Content-Type") is None
    assert transport._header_value({"content-type": "value"}, "Content-Type") == "value"


@pytest.mark.parametrize("url", [None, "x" * 513])
def test_exact_url_rejects_wrong_type_or_oversize(url):
    with pytest.raises(transport.UHCFlexPractitionerTransportError):
        transport._exact_url_parts(url)


def test_exact_url_rejects_parse_and_identifier_failures():
    with pytest.raises(transport.UHCFlexPractitionerTransportError):
        transport._exact_url_parts(
            "https://flex.optum.com:bad/fhirpublic/R4/Practitioner?identifier=x&_count=16"
        )
    with pytest.raises(transport.UHCFlexPractitionerTransportError):
        transport._exact_url_parts(
            "https://flex.optum.com/fhirpublic/R4/Practitioner?identifier=bad&_count=16"
        )


def test_validated_request_rechecks_expected_prefix(monkeypatch):
    monkeypatch.setattr(
        transport,
        "uhc_flex_practitioner_query_url",
        lambda _npi: "https://example.test/not-flex",
    )
    monkeypatch.setattr(transport, "_exact_url_parts", lambda _url: ())

    with pytest.raises(transport.UHCFlexPractitionerTransportError) as error_info:
        transport._validated_request_url(REQUESTED_NPI)
    assert error_info.value.code == "request_invalid"


def test_response_url_rejects_another_valid_exact_query():
    response = _Response(url=uhc_flex_practitioner_query_url(OTHER_NPI))
    with pytest.raises(transport.UHCFlexPractitionerTransportError) as error_info:
        transport._require_exact_response_url(
            response,
            uhc_flex_practitioner_query_url(REQUESTED_NPI),
        )
    assert error_info.value.code == "response_url_invalid"


@pytest.mark.asyncio
async def test_cancel_callback_translates_failure_but_preserves_cancellation():
    async def cancelled():
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await transport._invoke_cancel(cancelled)
    with pytest.raises(transport._TransportCallbackError):
        await transport._invoke_cancel(lambda: (_ for _ in ()).throw(RuntimeError("secret")))


@pytest.mark.asyncio
async def test_progress_callback_preserves_cancellation():
    async def cancelled(_phase, _count):
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await transport._invoke_progress(cancelled, "phase", 0)


def test_declared_length_handles_missing_invalid_and_empty_values():
    assert transport._declared_content_length(_Response(), 100) is None
    with pytest.raises(transport.UHCFlexPractitionerTransportError) as invalid_error:
        transport._declared_content_length(
            _Response(headers={"Content-Length": "1.5"}),
            100,
        )
    assert invalid_error.value.code == "content_length_invalid"

    with pytest.raises(transport.UHCFlexPractitionerTransportError) as empty_error:
        transport._declared_content_length(
            _Response(headers={"Content-Length": "0"}),
            100,
        )
    assert empty_error.value.code == "payload_truncated"


def test_response_headers_allow_empty_parameter_between_valid_values():
    response = _Response(
        headers={"Content-Type": "application/fhir+json;;charset=utf-8"}
    )
    assert transport._validate_response_headers(response, 100) is None


@pytest.mark.asyncio
async def test_body_reader_rejects_nonbytes_and_skips_empty_chunks():
    with pytest.raises(transport.UHCFlexPractitionerTransportError) as error_info:
        await transport._read_response_body(
            _Response(chunks=("text",)),
            declared_length=None,
            max_response_bytes=100,
            cancel_check=None,
            progress_callback=None,
        )
    assert error_info.value.code == "body_invalid"

    body = await transport._read_response_body(
        _Response(chunks=(b"", b"{}")),
        declared_length=2,
        max_response_bytes=100,
        cancel_check=None,
        progress_callback=None,
    )
    assert body == b"{}"


@pytest.mark.asyncio
async def test_body_reader_rejects_download_larger_than_declared_length():
    with pytest.raises(transport.UHCFlexPractitionerTransportError) as error_info:
        await transport._read_response_body(
            _Response(chunks=(b"{}",)),
            declared_length=1,
            max_response_bytes=100,
            cancel_check=None,
            progress_callback=None,
        )
    assert error_info.value.code == "content_length_invalid"


def test_strict_json_float_allows_finite_values():
    assert transport._strict_json_float("1.25") == 1.25


@pytest.mark.asyncio
async def test_default_session_is_anonymous_and_closable():
    session = transport.default_uhc_flex_practitioner_session()
    try:
        assert session.auto_decompress is False
        assert session.trust_env is False
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_unclassified_aiohttp_client_error_is_terminal_transport_failure():
    with pytest.raises(transport.UHCFlexPractitionerTransportError) as error_info:
        await transport.fetch_uhc_flex_practitioner(
            _FailingSession(aiohttp.ClientError("secret")),
            REQUESTED_NPI,
        )
    assert error_info.value.code == "transport_failure"
    assert error_info.value.is_retryable is False
    assert "secret" not in str(error_info.value)
