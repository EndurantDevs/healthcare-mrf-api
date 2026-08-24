# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded response and continuation validation for rooted-graph HTTP."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import datetime as dt
import email.utils
import json
import math
import re
from typing import Any
import urllib.parse

import aiohttp

from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_MISSING_RESPONSE_BYTES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAGE_BYTES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAGES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_QUERY_BYTES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_URL_BYTES,
)
from process.provider_directory_rooted_graph_query import (
    ROOTED_GRAPH_QUERY_DIRECT_READ,
)
from process.provider_directory_rooted_graph_store_contract import (
    ProviderDirectoryRootedGraphWorkClaim,
)
from process.provider_directory_rooted_graph_terminal import (
    validate_rooted_graph_missing_response,
)


PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_MISSING_BYTES = (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_MISSING_RESPONSE_BYTES
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_TIMEOUT_SECONDS = 30.0
PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RETRY_AFTER_SECONDS = 60.0
PROVIDER_DIRECTORY_ROOTED_GRAPH_RESPONSE_CHUNK_BYTES = 64 * 1024
PROVIDER_DIRECTORY_ROOTED_GRAPH_ACCEPT = "application/fhir+json"

_TRANSIENT_HTTP_STATUSES = frozenset({408, 423, 425, 429})
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_ERROR_MESSAGES = {
    "body_invalid": "rooted graph response body is invalid",
    "claim_rebound_invalid": "rooted graph claimed query cannot be rebound",
    "content_encoding_invalid": "rooted graph content encoding is invalid",
    "content_length_invalid": "rooted graph content length is invalid",
    "content_type_invalid": "rooted graph media type is invalid",
    "http_terminal": "rooted graph endpoint returned a terminal status",
    "http_transient": "rooted graph endpoint returned a transient status",
    "json_invalid": "rooted graph response is not strict JSON",
    "page_limit": "rooted graph page bound was exceeded",
    "pagination_invalid": "rooted graph continuation is invalid",
    "payload_truncated": "rooted graph response payload was truncated",
    "query_limit": "rooted graph query bound was exceeded",
    "redirect_forbidden": "rooted graph redirects are forbidden",
    "request_invalid": "rooted graph HTTP request is invalid",
    "resource_limit": "rooted graph resource bound was exceeded",
    "response_invalid": "rooted graph FHIR response is invalid",
    "response_url_invalid": "rooted graph response URL is invalid",
    "transport_connection": "rooted graph connection failed",
    "transport_failure": "rooted graph transport failed",
    "transport_timeout": "rooted graph transport timed out",
}


class ProviderDirectoryRootedGraphHTTPError(RuntimeError):
    """Expose a bounded transport code without retaining URLs or payloads."""

    def __init__(
        self,
        code: str,
        *,
        retryable: bool = False,
        retry_after_seconds: float = 0.0,
    ) -> None:
        safe_code = code if code in _ERROR_MESSAGES else "transport_failure"
        self.code = safe_code
        self.retryable = retryable is True
        self.retry_after_seconds = (
            _bounded_retry_after(retry_after_seconds) if self.retryable else 0.0
        )
        super().__init__(_ERROR_MESSAGES[safe_code])


@dataclass(frozen=True, slots=True)
class ProviderDirectoryRootedGraphHTTPBounds:
    """Lower-configurable hard caps for one complete claimed query."""

    max_page_bytes: int = PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAGE_BYTES
    max_query_bytes: int = PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_QUERY_BYTES
    max_missing_response_bytes: int = PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_MISSING_BYTES
    max_pages: int = PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAGES
    max_resources: int = PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCES
    max_url_bytes: int = PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_URL_BYTES
    timeout_seconds: float = PROVIDER_DIRECTORY_ROOTED_GRAPH_TIMEOUT_SECONDS

    def __post_init__(self) -> None:
        integer_bounds = (
            (self.max_page_bytes, PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAGE_BYTES),
            (self.max_query_bytes, PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_QUERY_BYTES),
            (
                self.max_missing_response_bytes,
                PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_MISSING_BYTES,
            ),
            (self.max_pages, PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAGES),
            (self.max_resources, PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCES),
            (self.max_url_bytes, PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_URL_BYTES),
        )
        if any(
            type(value) is not int or not 0 < value <= hard_maximum
            for value, hard_maximum in integer_bounds
        ):
            raise ValueError("provider_directory_rooted_graph_http_bounds_invalid")
        if (
            isinstance(self.timeout_seconds, bool)
            or not isinstance(self.timeout_seconds, (int, float))
            or not math.isfinite(float(self.timeout_seconds))
            or not 0
            < float(self.timeout_seconds)
            <= PROVIDER_DIRECTORY_ROOTED_GRAPH_TIMEOUT_SECONDS
        ):
            raise ValueError("provider_directory_rooted_graph_http_bounds_invalid")


@dataclass(frozen=True, slots=True, repr=False)
class ProviderDirectoryRootedGraphHTTPResult:
    """One finite transport result ready for canonical witness reduction."""

    query_id: str
    resources: tuple[dict[str, Any], ...]
    advertised_total: int | None
    terminal_page_count: int
    total_bytes: int
    missing_http_status: int | None = None
    missing_response_sha256: str | None = None
    missing_response_json_text: str | None = None

    def __post_init__(self) -> None:
        is_missing = self.missing_http_status in {404, 410}
        try:
            if is_missing:
                validate_rooted_graph_missing_response(
                    self.missing_http_status,
                    self.missing_response_sha256,
                    self.total_bytes,
                    self.missing_response_json_text,
                )
        except ValueError:
            raise ValueError(
                "provider_directory_rooted_graph_http_result_invalid"
            ) from None
        if (
            type(self.query_id) is not str
            or type(self.resources) is not tuple
            or type(self.terminal_page_count) is not int
            or self.terminal_page_count < 1
            or type(self.total_bytes) is not int
            or self.total_bytes < 0
            or (
                self.advertised_total is not None
                and (
                    type(self.advertised_total) is not int or self.advertised_total < 0
                )
            )
            or (
                self.missing_http_status is not None
                and (type(self.missing_http_status) is not int or not is_missing)
            )
            or (
                is_missing
                and (
                    self.resources
                    or self.advertised_total is not None
                    or self.terminal_page_count != 1
                    or self.total_bytes <= 0
                    or self.total_bytes
                    > PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_MISSING_BYTES
                    or type(self.missing_response_sha256) is not str
                    or _SHA256_PATTERN.fullmatch(self.missing_response_sha256) is None
                    or type(self.missing_response_json_text) is not str
                )
            )
            or (not is_missing and self.missing_response_sha256 is not None)
            or (not is_missing and self.missing_response_json_text is not None)
        ):
            raise ValueError("provider_directory_rooted_graph_http_result_invalid")


def _bounded_retry_after(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0.0
    seconds = float(value)
    if not math.isfinite(seconds):
        return 0.0
    return max(
        0.0,
        min(seconds, PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RETRY_AFTER_SECONDS),
    )


def rooted_graph_retry_after_seconds(raw_header: object) -> float:
    """Parse one delta/date Retry-After without admitting an unbounded delay."""

    if type(raw_header) is not str or not raw_header.strip():
        return 0.0
    header_text = raw_header.strip()
    try:
        return _bounded_retry_after(float(header_text))
    except ValueError:
        try:
            retry_at = email.utils.parsedate_to_datetime(header_text)
        except (TypeError, ValueError):
            return 0.0
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=dt.UTC)
        return _bounded_retry_after(
            (retry_at - dt.datetime.now(dt.UTC)).total_seconds()
        )


def _header_value(headers: object, name: str) -> str | None:
    if not isinstance(headers, Mapping):
        return None
    value = headers.get(name)
    if value is not None:
        return str(value)
    folded_name = name.casefold()
    for raw_name, raw_value in headers.items():
        if str(raw_name).casefold() == folded_name:
            return str(raw_value)
    return None


def _timeout(bounds: ProviderDirectoryRootedGraphHTTPBounds) -> aiohttp.ClientTimeout:
    total = float(bounds.timeout_seconds)
    connect = min(total, 10.0)
    read = min(total, 20.0)
    return aiohttp.ClientTimeout(
        total=total,
        connect=connect,
        sock_connect=connect,
        sock_read=read,
    )


def _url_byte_length(url: str) -> int:
    try:
        return len(url.encode("utf-8"))
    except UnicodeError:
        raise ProviderDirectoryRootedGraphHTTPError("pagination_invalid") from None


def _validated_next_url(
    *,
    api_base: str,
    collection_url: str,
    current_url: str,
    next_link: object,
    max_url_bytes: int,
) -> str:
    if (
        type(next_link) is not str
        or not next_link
        or next_link != next_link.strip()
        or next_link.startswith("//")
        or "\\" in next_link
        or _url_byte_length(next_link) > max_url_bytes
    ):
        raise ProviderDirectoryRootedGraphHTTPError("pagination_invalid")
    try:
        resolved_url = urllib.parse.urljoin(current_url, next_link)
        parsed_base = urllib.parse.urlsplit(api_base)
        parsed_collection = urllib.parse.urlsplit(collection_url)
        parsed_next = urllib.parse.urlsplit(resolved_url)
        base_origin = (
            parsed_base.hostname.casefold() if parsed_base.hostname else None,
            parsed_base.port or 443,
        )
        next_origin = (
            parsed_next.hostname.casefold() if parsed_next.hostname else None,
            parsed_next.port or 443,
        )
        decoded_path = urllib.parse.unquote(parsed_next.path, errors="strict")
        collection_path = urllib.parse.unquote(
            parsed_collection.path,
            errors="strict",
        )
    except (UnicodeError, ValueError):
        raise ProviderDirectoryRootedGraphHTTPError("pagination_invalid") from None
    path_segments = decoded_path.split("/")
    encoded_path = parsed_next.path.casefold()
    if (
        _url_byte_length(resolved_url) > max_url_bytes
        or parsed_next.scheme != "https"
        or next_origin != base_origin
        or parsed_next.username is not None
        or parsed_next.password is not None
        or parsed_next.fragment
        or decoded_path != collection_path
        or any(marker in encoded_path for marker in ("%2f", "%5c", "%2e"))
        or any(segment in {".", ".."} for segment in path_segments)
        or "" in path_segments[1:]
        or "\\" in decoded_path
        or any(ord(character) < 32 for character in resolved_url)
    ):
        raise ProviderDirectoryRootedGraphHTTPError("pagination_invalid")
    return resolved_url


def _strict_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    object_by_field: dict[str, Any] = {}
    for field_name, field_value in pairs:
        if field_name in object_by_field:
            raise ValueError
        object_by_field[field_name] = field_value
    return object_by_field


def _reject_json_constant(_raw_value: str) -> None:
    raise ValueError


def _strict_json_float(raw_value: str) -> float:
    try:
        parsed_value = float(raw_value)
        roundtrip_token = json.dumps(
            parsed_value,
            allow_nan=False,
            separators=(",", ":"),
        )
    except (OverflowError, ValueError):
        raise ValueError
    if not math.isfinite(parsed_value) or roundtrip_token != raw_value:
        raise ValueError
    return parsed_value


def _strict_json_payload(body: bytes) -> dict[str, Any]:
    try:
        payload = json.loads(
            body.decode("utf-8"),
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
        raise ProviderDirectoryRootedGraphHTTPError("json_invalid") from None
    if type(payload) is not dict:
        raise ProviderDirectoryRootedGraphHTTPError("json_invalid")
    return payload


def _declared_length(response: Any) -> int | None:
    raw_length = _header_value(getattr(response, "headers", None), "Content-Length")
    if raw_length is None:
        return None
    if not raw_length.isascii() or not raw_length.isdigit():
        raise ProviderDirectoryRootedGraphHTTPError("content_length_invalid")
    declared = int(raw_length)
    if declared <= 0:
        raise ProviderDirectoryRootedGraphHTTPError(
            "payload_truncated",
            retryable=True,
        )
    return declared


def _validate_headers(response: Any) -> int | None:
    encoding = (
        (
            _header_value(getattr(response, "headers", None), "Content-Encoding")
            or "identity"
        )
        .strip()
        .casefold()
    )
    if encoding != "identity":
        raise ProviderDirectoryRootedGraphHTTPError("content_encoding_invalid")
    content_type = (
        _header_value(getattr(response, "headers", None), "Content-Type") or ""
    )
    parts = [part.strip() for part in content_type.split(";")]
    if not parts or parts[0].casefold() != PROVIDER_DIRECTORY_ROOTED_GRAPH_ACCEPT:
        raise ProviderDirectoryRootedGraphHTTPError("content_type_invalid", retryable=True)
    for parameter in parts[1:]:
        name, separator, charset = parameter.partition("=")
        if (
            not separator
            or name.strip().casefold() != "charset"
            or charset.strip().strip('"').casefold() not in {"utf-8", "utf8"}
        ):
            raise ProviderDirectoryRootedGraphHTTPError("content_type_invalid", retryable=True)
    return _declared_length(response)


async def _read_body(
    response: Any,
    *,
    declared_length: int | None,
    page_limit: int,
    query_remaining: int,
) -> bytes:
    if declared_length is not None:
        if declared_length > page_limit:
            raise ProviderDirectoryRootedGraphHTTPError("page_limit")
        if declared_length > query_remaining:
            raise ProviderDirectoryRootedGraphHTTPError("query_limit")
    body = bytearray()
    async for chunk in response.content.iter_chunked(
        PROVIDER_DIRECTORY_ROOTED_GRAPH_RESPONSE_CHUNK_BYTES
    ):
        if type(chunk) is not bytes:
            raise ProviderDirectoryRootedGraphHTTPError("body_invalid")
        if not chunk:
            continue
        if len(chunk) > page_limit - len(body):
            raise ProviderDirectoryRootedGraphHTTPError("page_limit")
        if len(chunk) > query_remaining - len(body):
            raise ProviderDirectoryRootedGraphHTTPError("query_limit")
        body.extend(chunk)
    if not body or (declared_length is not None and len(body) < declared_length):
        raise ProviderDirectoryRootedGraphHTTPError(
            "payload_truncated",
            retryable=True,
        )
    if declared_length is not None and len(body) != declared_length:
        raise ProviderDirectoryRootedGraphHTTPError("content_length_invalid")
    return bytes(body)


def _request_url_identity(candidate: object) -> tuple[object, ...] | None:
    if type(candidate) is not str:
        return None
    try:
        parsed = urllib.parse.urlsplit(candidate)
        origin = (
            parsed.hostname.casefold() if parsed.hostname else None,
            parsed.port or 443,
        )
    except ValueError:
        return None
    if (
        parsed.scheme != "https"
        or origin[0] is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        return None
    return origin, parsed.path, parsed.query


def _require_response_url(response: Any, request_url: str) -> None:
    response_url = str(getattr(response, "url", ""))
    if _request_url_identity(response_url) != _request_url_identity(request_url):
        raise ProviderDirectoryRootedGraphHTTPError("response_url_invalid")


def _require_success_status(
    response: Any,
    claim: ProviderDirectoryRootedGraphWorkClaim,
) -> int | None:
    status = getattr(response, "status", None)
    if status == 200:
        return None
    if (
        claim.kind == ROOTED_GRAPH_QUERY_DIRECT_READ
        and type(status) is int
        and status in {404, 410}
    ):
        return status
    if type(status) is int and 300 <= status <= 399:
        raise ProviderDirectoryRootedGraphHTTPError("redirect_forbidden")
    is_retryable = type(status) is int and (
        status in _TRANSIENT_HTTP_STATUSES or 500 <= status <= 599
    )
    raise ProviderDirectoryRootedGraphHTTPError(
        "http_transient" if is_retryable else "http_terminal",
        retryable=is_retryable,
        retry_after_seconds=rooted_graph_retry_after_seconds(
            _header_value(getattr(response, "headers", None), "Retry-After")
        ),
    )


provider_directory_rooted_graph_retry_after_seconds = rooted_graph_retry_after_seconds
