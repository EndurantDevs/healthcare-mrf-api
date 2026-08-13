# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded anonymous HTTP transport for exact Flex Practitioner queries."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
import datetime as dt
import email.utils
import inspect
import json
import math
from typing import Any
import urllib.parse

import aiohttp

from process.uhc_flex_official_cohort_contract import (
    UHC_FLEX_OFFICIAL_NPI_SYSTEM,
    UHC_FLEX_OFFICIAL_RESOURCE_TYPE,
)
from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_API_BASE,
    UHC_FLEX_PRACTITIONER_QUERY_COUNT,
    UHC_FLEX_PRACTITIONER_SEARCH_PARAMETER,
)
from process.uhc_flex_practitioner_query import (
    UHCFlexPractitionerQueryError,
    UHCFlexPractitionerQueryResult,
    classify_uhc_flex_practitioner_exception,
    classify_uhc_flex_practitioner_http_status,
    uhc_flex_practitioner_query_url,
    validate_uhc_flex_practitioner_search_bundle,
)


UHC_FLEX_PRACTITIONER_MAX_RESPONSE_BYTES = 20 * 1024 * 1024
UHC_FLEX_PRACTITIONER_RESPONSE_CHUNK_BYTES = 64 * 1024
UHC_FLEX_PRACTITIONER_TIMEOUT_SECONDS = 30.0
UHC_FLEX_PRACTITIONER_MAX_TIMEOUT_SECONDS = 300.0
UHC_FLEX_PRACTITIONER_MAX_RETRY_AFTER_SECONDS = 60.0
UHC_FLEX_PRACTITIONER_ACCEPT = "application/fhir+json"

CancelCheck = Callable[[], Awaitable[bool | None] | bool | None]
ProgressCallback = Callable[[str, int], Awaitable[None] | None]


_ERROR_MESSAGES = {
    "body_invalid": "Flex Practitioner response body is invalid",
    "body_too_large": "Flex Practitioner response exceeds its byte bound",
    "callback_failed": "Flex Practitioner transport callback failed",
    "content_encoding_invalid": "Flex Practitioner content encoding is invalid",
    "content_length_invalid": "Flex Practitioner content length is invalid",
    "content_type_invalid": "Flex Practitioner response media type is invalid",
    "http_terminal": "Flex Practitioner endpoint returned a terminal status",
    "http_transient": "Flex Practitioner endpoint returned a transient status",
    "json_invalid": "Flex Practitioner response is not strict JSON",
    "payload_truncated": "Flex Practitioner response payload was truncated",
    "redirect_forbidden": "Flex Practitioner redirects are forbidden",
    "request_invalid": "Flex Practitioner exact request is invalid",
    "response_url_invalid": "Flex Practitioner response URL is invalid",
    "response_validation": "Flex Practitioner response validation failed",
    "transport_connection": "Flex Practitioner connection failed",
    "transport_failure": "Flex Practitioner transport failed",
    "transport_timeout": "Flex Practitioner transport timed out",
}


class UHCFlexPractitionerTransportError(RuntimeError):
    """Expose bounded retry metadata without retaining an NPI or body."""

    def __init__(
        self,
        code: str,
        *,
        retryable: bool = False,
        retry_after_seconds: float = 0.0,
        validation_code: str | None = None,
    ) -> None:
        safe_code = code if code in _ERROR_MESSAGES else "transport_failure"
        safe_retry_after = _bounded_retry_after_value(retry_after_seconds)
        self.code = safe_code
        self.reason_code = safe_code
        self.retryable = retryable is True
        self.is_retryable = self.retryable
        self.retry_after_seconds = safe_retry_after if self.retryable else 0.0
        normalized_validation_code = (
            UHCFlexPractitionerQueryError(validation_code).code
            if type(validation_code) is str
            else None
        )
        self.validation_code = (
            validation_code
            if safe_code == "response_validation"
            and normalized_validation_code == validation_code
            else None
        )
        super().__init__(_ERROR_MESSAGES[safe_code])


class _TransportCallbackError(Exception):
    """Keep callback details outside public transport failures."""


def _bounded_retry_after_value(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0.0
    seconds = float(value)
    if not math.isfinite(seconds):
        return 0.0
    return max(0.0, min(seconds, UHC_FLEX_PRACTITIONER_MAX_RETRY_AFTER_SECONDS))


def uhc_flex_practitioner_retry_after_seconds(raw_header: object) -> float:
    """Parse a delta or HTTP date without admitting an unbounded delay."""

    if type(raw_header) is not str or not raw_header.strip():
        return 0.0
    header_text = raw_header.strip()
    try:
        return _bounded_retry_after_value(float(header_text))
    except ValueError:
        try:
            retry_at = email.utils.parsedate_to_datetime(header_text)
        except (TypeError, ValueError):
            return 0.0
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=dt.UTC)
        return _bounded_retry_after_value(
            (retry_at - dt.datetime.now(dt.UTC)).total_seconds()
        )


def _header_value(headers: object, header_name: str) -> str | None:
    if not isinstance(headers, Mapping):
        return None
    direct_value = headers.get(header_name)
    if direct_value is not None:
        return str(direct_value)
    folded_name = header_name.casefold()
    for raw_name, raw_value in headers.items():
        if str(raw_name).casefold() == folded_name:
            return str(raw_value)
    return None


def _exact_url_parts(url: object) -> tuple[str, str, str, tuple[tuple[str, str], ...]]:
    if type(url) is not str or len(url) > 512:
        raise UHCFlexPractitionerTransportError("request_invalid")
    try:
        parsed_url = urllib.parse.urlsplit(url)
        query_pairs = tuple(
            urllib.parse.parse_qsl(
                parsed_url.query,
                keep_blank_values=True,
                strict_parsing=True,
            )
        )
        parsed_port = parsed_url.port
    except (UnicodeError, ValueError):
        raise UHCFlexPractitionerTransportError("request_invalid") from None
    if (
        parsed_url.scheme != "https"
        or parsed_url.netloc != "flex.optum.com"
        or parsed_url.hostname != "flex.optum.com"
        or parsed_port is not None
        or parsed_url.username is not None
        or parsed_url.password is not None
        or parsed_url.fragment
        or parsed_url.path != "/fhirpublic/R4/Practitioner"
        or len(query_pairs) != 2
        or query_pairs[0][0] != UHC_FLEX_PRACTITIONER_SEARCH_PARAMETER
        or query_pairs[1] != (
            "_count",
            str(UHC_FLEX_PRACTITIONER_QUERY_COUNT),
        )
    ):
        raise UHCFlexPractitionerTransportError("request_invalid")
    identifier_value = query_pairs[0][1]
    identifier_prefix = f"{UHC_FLEX_OFFICIAL_NPI_SYSTEM}|"
    raw_npi = identifier_value.removeprefix(identifier_prefix)
    if (
        not identifier_value.startswith(identifier_prefix)
        or len(raw_npi) != 10
        or not raw_npi.isascii()
        or not raw_npi.isdigit()
    ):
        raise UHCFlexPractitionerTransportError("request_invalid")
    return parsed_url.scheme, parsed_url.netloc, parsed_url.path, query_pairs


def _validated_request_url(requested_npi: object) -> str:
    try:
        request_url = uhc_flex_practitioner_query_url(requested_npi)
    except UHCFlexPractitionerQueryError:
        raise UHCFlexPractitionerTransportError("request_invalid") from None
    _exact_url_parts(request_url)
    expected_prefix = (
        f"{UHC_FLEX_PRACTITIONER_API_BASE}/"
        f"{UHC_FLEX_OFFICIAL_RESOURCE_TYPE}?"
    )
    if not request_url.startswith(expected_prefix):
        raise UHCFlexPractitionerTransportError("request_invalid")
    return request_url


def _require_exact_response_url(response: Any, request_url: str) -> None:
    raw_response_url = getattr(response, "url", None)
    try:
        response_url_parts = _exact_url_parts(str(raw_response_url))
        request_url_parts = _exact_url_parts(request_url)
    except UHCFlexPractitionerTransportError:
        raise UHCFlexPractitionerTransportError("response_url_invalid") from None
    if response_url_parts != request_url_parts:
        raise UHCFlexPractitionerTransportError("response_url_invalid")


def _validated_response_byte_bound(max_response_bytes: object) -> int:
    if (
        type(max_response_bytes) is not int
        or not 0 < max_response_bytes <= UHC_FLEX_PRACTITIONER_MAX_RESPONSE_BYTES
    ):
        raise UHCFlexPractitionerTransportError("request_invalid")
    return max_response_bytes


def _request_timeout(timeout_seconds: object) -> aiohttp.ClientTimeout:
    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, (int, float))
        or not math.isfinite(float(timeout_seconds))
        or not 0 < float(timeout_seconds) <= UHC_FLEX_PRACTITIONER_MAX_TIMEOUT_SECONDS
    ):
        raise UHCFlexPractitionerTransportError("request_invalid")
    total_seconds = float(timeout_seconds)
    connect_seconds = min(total_seconds, 10.0)
    read_seconds = min(total_seconds, 20.0)
    return aiohttp.ClientTimeout(
        total=total_seconds,
        connect=connect_seconds,
        sock_connect=connect_seconds,
        sock_read=read_seconds,
    )


async def _invoke_cancel(cancel_check: CancelCheck | None) -> None:
    if cancel_check is None:
        return
    try:
        result = cancel_check()
        if inspect.isawaitable(result):
            result = await result
    except asyncio.CancelledError:
        raise
    except Exception:
        raise _TransportCallbackError from None
    if result is True:
        raise asyncio.CancelledError("Flex Practitioner cancellation requested")


async def _invoke_progress(
    progress_callback: ProgressCallback | None,
    phase: str,
    byte_count: int,
) -> None:
    if progress_callback is None:
        return
    try:
        result = progress_callback(phase, byte_count)
        if inspect.isawaitable(result):
            await result
    except asyncio.CancelledError:
        raise
    except Exception:
        raise _TransportCallbackError from None


def _declared_content_length(response: Any, max_response_bytes: int) -> int | None:
    raw_length = _header_value(getattr(response, "headers", None), "Content-Length")
    if raw_length is None:
        return None
    if not raw_length.isascii() or not raw_length.isdigit():
        raise UHCFlexPractitionerTransportError("content_length_invalid")
    declared_length = int(raw_length)
    if declared_length <= 0:
        raise UHCFlexPractitionerTransportError("payload_truncated", retryable=True)
    if declared_length > max_response_bytes:
        raise UHCFlexPractitionerTransportError("body_too_large")
    return declared_length


def _validate_response_headers(response: Any, max_response_bytes: int) -> int | None:
    content_encoding = (
        _header_value(getattr(response, "headers", None), "Content-Encoding")
        or "identity"
    ).strip().lower()
    if content_encoding != "identity":
        raise UHCFlexPractitionerTransportError("content_encoding_invalid")
    raw_content_type = (
        _header_value(getattr(response, "headers", None), "Content-Type") or ""
    )
    content_type_parts = [part.strip() for part in raw_content_type.split(";")]
    if content_type_parts[0].lower() != UHC_FLEX_PRACTITIONER_ACCEPT:
        raise UHCFlexPractitionerTransportError("content_type_invalid")
    for parameter in content_type_parts[1:]:
        if not parameter:
            continue
        parameter_name, separator, parameter_value = parameter.partition("=")
        if (
            not separator
            or parameter_name.strip().lower() != "charset"
            or parameter_value.strip().strip('"').lower() not in {"utf-8", "utf8"}
        ):
            raise UHCFlexPractitionerTransportError("content_type_invalid")
    return _declared_content_length(response, max_response_bytes)


async def _read_response_body(
    response: Any,
    *,
    declared_length: int | None,
    max_response_bytes: int,
    cancel_check: CancelCheck | None,
    progress_callback: ProgressCallback | None,
) -> bytes:
    response_body = bytearray()
    async for response_chunk in response.content.iter_chunked(
        UHC_FLEX_PRACTITIONER_RESPONSE_CHUNK_BYTES
    ):
        await _invoke_cancel(cancel_check)
        if type(response_chunk) is not bytes:
            raise UHCFlexPractitionerTransportError("body_invalid")
        if not response_chunk:
            continue
        if len(response_chunk) > max_response_bytes - len(response_body):
            raise UHCFlexPractitionerTransportError("body_too_large")
        response_body.extend(response_chunk)
        await _invoke_progress(progress_callback, "response_bytes", len(response_body))
    downloaded_length = len(response_body)
    if downloaded_length == 0 or (
        declared_length is not None and downloaded_length < declared_length
    ):
        raise UHCFlexPractitionerTransportError("payload_truncated", retryable=True)
    if declared_length is not None and downloaded_length != declared_length:
        raise UHCFlexPractitionerTransportError("content_length_invalid")
    return bytes(response_body)


def _strict_json_float(raw_value: str) -> float:
    parsed_value = float(raw_value)
    if not math.isfinite(parsed_value):
        raise ValueError
    return parsed_value


def _reject_json_constant(_raw_value: str) -> None:
    raise ValueError


def _strict_json_object(
    object_pairs: list[tuple[str, Any]],
) -> dict[str, Any]:
    object_by_field: dict[str, Any] = {}
    for field_name, field_value in object_pairs:
        if field_name in object_by_field:
            raise ValueError
        object_by_field[field_name] = field_value
    return object_by_field


def _strict_json_payload(response_body: bytes) -> dict[str, Any]:
    try:
        response_text = response_body.decode("utf-8")
        response_payload = json.loads(
            response_text,
            object_pairs_hook=_strict_json_object,
            parse_constant=_reject_json_constant,
            parse_float=_strict_json_float,
        )
    except (
        MemoryError,
        OverflowError,
        RecursionError,
        UnicodeError,
        ValueError,
    ):
        raise UHCFlexPractitionerTransportError("json_invalid") from None
    if type(response_payload) is not dict:
        raise UHCFlexPractitionerTransportError("json_invalid")
    return response_payload


def default_uhc_flex_practitioner_session() -> aiohttp.ClientSession:
    """Create an anonymous verified-TLS session without decompression or cookies."""

    return aiohttp.ClientSession(
        auto_decompress=False,
        cookie_jar=aiohttp.DummyCookieJar(),
        skip_auto_headers={"Accept-Encoding"},
        trust_env=False,
    )


def _require_success_status(response: Any) -> None:
    response_status = getattr(response, "status", None)
    status_decision = classify_uhc_flex_practitioner_http_status(response_status)
    if status_decision.category == "success":
        return
    if type(response_status) is int and 300 <= response_status <= 399:
        raise UHCFlexPractitionerTransportError("redirect_forbidden")
    is_retryable = status_decision.is_retryable
    raise UHCFlexPractitionerTransportError(
        "http_transient" if is_retryable else "http_terminal",
        retryable=is_retryable,
        retry_after_seconds=uhc_flex_practitioner_retry_after_seconds(
            _header_value(getattr(response, "headers", None), "Retry-After")
        ),
    )


async def _read_exact_response(
    session: Any, requested_npi: object, request_url: str,
    response_byte_bound: int,
    timeout: aiohttp.ClientTimeout,
    cancel_check: CancelCheck | None,
    progress_callback: ProgressCallback | None,
) -> UHCFlexPractitionerQueryResult:
    await _invoke_cancel(cancel_check)
    await _invoke_progress(progress_callback, "request_started", 0)
    async with session.get(
        request_url,
        headers={
            "Accept": UHC_FLEX_PRACTITIONER_ACCEPT,
            "Accept-Encoding": "identity",
        },
        timeout=timeout,
        allow_redirects=False,
    ) as response:
        await _invoke_cancel(cancel_check)
        _require_exact_response_url(response, request_url)
        _require_success_status(response)
        declared_length = _validate_response_headers(response, response_byte_bound)
        response_body = await _read_response_body(
            response,
            declared_length=declared_length,
            max_response_bytes=response_byte_bound,
            cancel_check=cancel_check,
            progress_callback=progress_callback,
        )
    await _invoke_cancel(cancel_check)
    response_payload = _strict_json_payload(response_body)
    query_result = validate_uhc_flex_practitioner_search_bundle(
        requested_npi, response_payload)
    await _invoke_cancel(cancel_check)
    await _invoke_progress(
        progress_callback, "response_validated", len(response_body))
    return query_result


async def fetch_uhc_flex_practitioner(
    session: Any,
    requested_npi: object,
    *,
    max_response_bytes: int = UHC_FLEX_PRACTITIONER_MAX_RESPONSE_BYTES,
    timeout_seconds: float = UHC_FLEX_PRACTITIONER_TIMEOUT_SECONDS,
    cancel_check: CancelCheck | None = None,
    progress_callback: ProgressCallback | None = None,
) -> UHCFlexPractitionerQueryResult:
    """Fetch and validate one exact NPI query without retries or concurrency."""

    request_url = _validated_request_url(requested_npi)
    response_byte_bound = _validated_response_byte_bound(max_response_bytes)
    timeout = _request_timeout(timeout_seconds)
    try:
        return await _read_exact_response(
            session, requested_npi, request_url, response_byte_bound,
            timeout, cancel_check, progress_callback)
    except asyncio.CancelledError:
        raise
    except UHCFlexPractitionerTransportError:
        raise
    except UHCFlexPractitionerQueryError as error:
        raise UHCFlexPractitionerTransportError(
            "response_validation",
            retryable=classify_uhc_flex_practitioner_exception(error).is_retryable,
            validation_code=error.code,
        ) from None
    except _TransportCallbackError:
        raise UHCFlexPractitionerTransportError("callback_failed") from None
    except (aiohttp.ClientPayloadError, aiohttp.ServerDisconnectedError, EOFError):
        raise UHCFlexPractitionerTransportError("payload_truncated", retryable=True) from None
    except (asyncio.TimeoutError, TimeoutError):
        raise UHCFlexPractitionerTransportError(
            "transport_timeout", retryable=True) from None
    except (aiohttp.ClientConnectionError, ConnectionError, OSError):
        raise UHCFlexPractitionerTransportError(
            "transport_connection", retryable=True) from None
    except aiohttp.ClientError:
        raise UHCFlexPractitionerTransportError("transport_failure") from None
