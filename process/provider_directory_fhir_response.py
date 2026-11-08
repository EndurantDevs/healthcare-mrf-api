# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""FHIR response checks shared by collection imports and access probes."""

import asyncio
import contextvars
import datetime
import gzip
import io
import logging
import urllib.parse
import zlib
from contextlib import asynccontextmanager, nullcontext
from typing import Any, Awaitable, Callable

SOURCE_HTTP_REQUEST_SLOTS: contextvars.ContextVar[dict | None] = contextvars.ContextVar(
    "provider_directory_http_request_slots", default=None
)


def decode_bounded_response_body(body, encoding, max_bytes):
    """Bound both wire bytes and inflation while verifying every gzip member."""
    if len(body) > max_bytes:
        raise ValueError("provider_directory_response_wire_limit_exceeded")
    encoding = (encoding or "").strip().lower()
    if encoding not in {"", "identity", "gzip"}:
        raise ValueError("provider_directory_response_encoding_unsupported")
    if encoding == "gzip":
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(body)) as stream:
                body = stream.read(max_bytes + 1)
        except (OSError, EOFError, zlib.error) as exc:
            raise ValueError("provider_directory_response_gzip_invalid") from exc
        if len(body) > max_bytes:
            raise ValueError("provider_directory_response_decoded_limit_exceeded")
    return body


class AdaptiveRequestSlots:
    """Drain active requests before admitting work at a reduced endpoint limit."""

    def __init__(self, capacity):
        self.limit = capacity
        self.active = 0
        self.condition = asyncio.Condition()

    async def __aenter__(self):
        async with self.condition:
            await self.condition.wait_for(lambda: self.active < self.limit)
            self.active += 1
        return self

    async def __aexit__(self, *_exc):
        async with self.condition:
            self.active -= 1
            self.condition.notify_all()

    def reduce(self):
        """Retain a lower allowance until this import's transport closes."""
        self.limit = max(1, self.limit // 2)


def _response_limits_concurrency(response):
    """Reduce load for throttling, unavailable upstreams and transport timeouts."""
    status, _payload, error, _elapsed = response
    return status in {429, 503, 504} or bool(error and any(word in error.lower() for word in ("timeout", "timed out")))


def is_empty_search_set(
    status_code: int | None,
    fhir_payload: dict[str, Any] | None,
    fetch_error: str | None,
) -> bool:
    """Recognize a successful empty search without discarding transport errors."""
    return bool(
        status_code == 200
        and fetch_error is None
        and isinstance(fhir_payload, dict)
        and fhir_payload.get("resourceType") == "Bundle"
        and fhir_payload.get("type") == "searchset"
        and not (fhir_payload.get("entry") or [])
    )


def has_operation_outcome_error(outcome: dict[str, Any]) -> bool:
    """Treat missing, fatal, error, and unknown issue severities as failures."""
    issues = [issue for issue in outcome.get("issue") or [] if isinstance(issue, dict)]
    if not issues:
        return True
    return any(str(issue.get("severity") or "").strip().lower() not in {"warning", "information"} for issue in issues)


def is_access_denied_outcome(resource_payload: dict[str, Any] | None) -> bool:
    """Identify permission failures encoded in an OperationOutcome."""
    if not resource_payload or resource_payload.get("resourceType") != "OperationOutcome":
        return False
    issue_text = " ".join(
        str(issue.get("diagnostics") or issue.get("details", {}).get("text") or "")
        for issue in resource_payload.get("issue") or []
        if isinstance(issue, dict)
    ).lower()
    return any(marker in issue_text for marker in ("access denied", "not authorized", "not authorised", "forbidden"))


async def fetch_paced_source_response(directory, source_record, url, *, timeout):
    """Gate queued sends before pacing and publish cooldown before releasing a slot."""
    if directory.current_version_census_contract(source_record) is not None:
        return await directory._fetch_source_json_unpaced(source_record, url, timeout=timeout)
    pacing_state = source_record.get("_provider_directory_request_pacing_state")
    request_slots = total_slots = nullcontext()
    if pacing_state is not None:
        slot_map = SOURCE_HTTP_REQUEST_SLOTS.get()
        session = directory._SOURCE_HTTP_SESSION.get()
        if slot_map is not None and session is not None and session.connector is not None:
            if "total" not in slot_map:
                slot_map["total"] = asyncio.Semaphore(max(1, session.connector.limit))
            total_slots = slot_map["total"]
        capacity = session.connector.limit_per_host if session is not None and session.connector is not None else 1
        parsed_url = urllib.parse.urlsplit(url)
        slot_key = (
            parsed_url.scheme,
            parsed_url.hostname,
            parsed_url.port or (443 if parsed_url.scheme == "https" else 80),
        )
        if slot_map is None:
            slot_map, slot_key = pacing_state, "request_slots"
        if slot_key not in slot_map:
            slot_map[slot_key] = AdaptiveRequestSlots(max(1, capacity))
        request_slots = slot_map[slot_key]
    while True:
        async with request_slots, total_slots:
            await directory._pace_source_request(source_record)
            if isinstance(request_slots, AdaptiveRequestSlots) and request_slots.active > request_slots.limit:
                continue
            response = await directory._fetch_source_json_unpaced(source_record, url, timeout=timeout)
            if isinstance(request_slots, AdaptiveRequestSlots) and _response_limits_concurrency(response):
                previous_limit = request_slots.limit
                request_slots.reduce()
                if request_slots.limit != previous_limit:
                    directory.LOGGER.info(
                        "Provider Directory request concurrency reduced: previous=%s effective=%s",
                        previous_limit,
                        request_slots.limit,
                    )
            if response[0] in {429, 503, 504} and pacing_state is not None:
                delay = directory._source_retry_after_seconds(
                    response[1],
                    max_delay_seconds=directory.source_selection.retry_after_limit(directory, source_record),
                )
                if delay is not None:
                    pacing_state["retry_after_monotonic"] = max(
                        pacing_state.get("retry_after_monotonic", 0.0), directory.time.monotonic() + delay
                    )
            return response


BULK_EXPORT_WAIT_CALLBACK: contextvars.ContextVar[Callable[[dict[str, Any] | None], Awaitable[None]] | None] = (
    contextvars.ContextVar("provider_directory_bulk_export_wait_callback", default=None)
)


@asynccontextmanager
async def bulk_export_wait_scope(callback):
    """Keep each cursor observation local and preserve its primary failure."""
    token = BULK_EXPORT_WAIT_CALLBACK.set(callback)
    has_primary_error = False
    try:
        yield
    except BaseException:
        has_primary_error = True
        raise
    finally:
        try:
            await clear_bulk_export_wait(has_primary_error=has_primary_error)
        finally:
            BULK_EXPORT_WAIT_CALLBACK.reset(token)


async def clear_bulk_export_wait(*, has_primary_error=False):
    """Clear visibility without replacing a primary transport or cursor error."""
    callback = BULK_EXPORT_WAIT_CALLBACK.get()
    if callback is None:
        return
    try:
        await callback(None)
    except (Exception, asyncio.CancelledError) as cleanup_error:
        if not has_primary_error:
            raise
        logging.getLogger(__name__).warning("Export wait cleanup failed: %s", type(cleanup_error).__name__)


def bulk_export_wait_observation(
    resource_type: str,
    observed_at: datetime.datetime,
    next_poll_at: datetime.datetime,
) -> dict[str, Any]:
    """Describe an actual pending response without retaining its capability."""
    return {
        "resource": resource_type,
        "http_status": 202,
        "observed_at": observed_at.isoformat(),
        "next_poll_at": next_poll_at.isoformat(),
    }


async def handle_bulk_export_pending_poll(
    directory,
    context,
    headers: dict[str, str],
    poll: int,
) -> tuple[datetime.datetime | None, str | None]:
    """Publish an actual pending response before its existing poll boundary."""
    current_time = directory._bulk_export_now_utc()
    if context.pending_deadline is not None and current_time >= context.pending_deadline:
        return None, directory.BULK_EXPORT_STATUS_DEADLINE_EXCEEDED
    next_poll_at = directory._next_bulk_export_poll_at(
        headers.get("retry-after"),
        context.poll_seconds,
        current_time,
        context.pending_deadline,
    )
    callback = BULK_EXPORT_WAIT_CALLBACK.get()
    if callback is not None:
        await callback(bulk_export_wait_observation(context.resource_type, current_time, next_poll_at))
    if context.options.next_poll_handler is not None:
        await context.options.next_poll_handler(next_poll_at)
    if poll == 1 or poll % 10 == 0:
        directory._bulk_export_log(
            "poll_wait",
            source_id=context.source_record.get("source_id"),
            resource=context.resource_type,
            poll=poll,
            max_polls=context.poll_limit,
            status=202,
            sleep_seconds=max(0.0, (next_poll_at - current_time).total_seconds()),
            status_url=directory._bulk_capability_log_identity(context.status_url),
        )
    if poll >= context.poll_limit:
        return next_poll_at, "bulk_export_timeout"
    wait_error = await directory._wait_for_bulk_export_poll_boundary(
        next_poll_at,
        context.pending_deadline,
    )
    return next_poll_at, wait_error


async def read_source_http_payload(directory, response, *, timeout, allow_gzip=False):
    """Read JSON while retaining status metadata when an error body stalls."""
    is_error_response = not 200 <= response.status < 300
    max_bytes = 1024 * 1024 if is_error_response else directory.MAX_FHIR_JSON_BODY_BYTES
    try:
        async with directory.asyncio.timeout(max(1, timeout)) if allow_gzip else directory.contextlib.nullcontext():
            body = await directory._read_source_http_response_body(
                response,
                timeout=timeout,
                max_bytes=max_bytes + 1 if allow_gzip else max_bytes,
            )
            if allow_gzip:
                body = await directory.asyncio.to_thread(
                    directory.fhir_response.decode_bounded_response_body,
                    body,
                    response.headers.get("Content-Encoding"),
                    max_bytes,
                )
    except Exception:
        if not is_error_response:
            raise
        return directory._source_http_payload_with_retry_after(None, response.headers)
    payload_by_field = directory._decode_json_body(body)
    if not is_error_response:
        return payload_by_field
    return directory._source_http_payload_with_retry_after(payload_by_field, response.headers)


def session_request_options(directory, url, *, request_timeout, options_by_name):
    """Retain exact URL bytes and redirect policy for each persistent GET."""
    headers_by_name = {
        "Accept": "application/fhir+json, application/json;q=0.9, */*;q=0.1",
        "Accept-Encoding": "identity",
        "User-Agent": directory.USER_AGENT,
    }
    request_options = options_by_name["proxy_options"] or {}
    if options_by_name["extra_headers"]:
        if any(key.lower() == "proxy-authorization" for key in options_by_name["extra_headers"]):
            raise ValueError("source_proxy_origin_header_forbidden")
        headers_by_name.update(options_by_name["extra_headers"])
    if options_by_name["allow_gzip"]:
        headers_by_name = {
            key: header_value for key, header_value in headers_by_name.items() if key.lower() != "accept-encoding"
        }
        headers_by_name["Accept-Encoding"] = "gzip"
    is_continuation = directory._has_fhir_continuation_query(url)
    is_encoded = (
        options_by_name["preserve_url_bytes"]
        or request_options
        or directory.christus.is_request_url(url)
        or is_continuation
    )
    request_url = directory.URL(url, encoded=True) if is_encoded else url
    should_allow_redirects = (
        options_by_name["allow_redirects"]
        and not directory.christus.is_request_url(url)
        and not is_continuation
        and directory.source_selection.QUALIFICATION_API_BASE.get() is None
        and not request_options
    )
    return request_url, headers_by_name, request_timeout, should_allow_redirects, request_options


async def fetch_session_json(directory, session, url, *, timeout, options_by_name):
    """Fetch bounded JSON through the selected session and retain safe failures."""
    request_timeout = directory.aiohttp.ClientTimeout(
        total=None, connect=timeout, sock_connect=timeout, sock_read=timeout
    )
    started = directory.time.monotonic()
    try:
        request_url, headers_by_name, request_timeout, should_allow_redirects, request_options = (
            session_request_options(directory, url, request_timeout=request_timeout, options_by_name=options_by_name)
        )
        async with session.get(
            request_url,
            headers=headers_by_name,
            timeout=request_timeout,
            allow_redirects=should_allow_redirects,
            **request_options,
        ) as response:
            payload_by_field = await directory._read_source_http_payload(
                response, timeout=timeout, allow_gzip=options_by_name["allow_gzip"]
            )
            return response.status, payload_by_field, None, int((directory.time.monotonic() - started) * 1000)
    except directory.aiohttp.TooManyRedirects as exc:
        return directory._source_http_redirect_result(exc, started=started)
    except Exception as exc:
        error = (
            str(exc)
            if isinstance(exc, ValueError) and str(exc).startswith("provider_directory_response_")
            else type(exc).__name__
        )
        return None, None, error, int((directory.time.monotonic() - started) * 1000)


def fetch_json_sync(directory, url, *, timeout, extra_headers=None, query_params_by_name=None, allow_gzip=False):
    """Fetch through the admitted source transport without anonymous downgrade."""
    headers_by_name = {
        "Accept": "application/fhir+json, application/json;q=0.9, */*;q=0.1",
        "User-Agent": directory.USER_AGENT,
    }
    if extra_headers:
        headers_by_name.update(extra_headers)
    if allow_gzip:
        headers_by_name = {
            key: header_value for key, header_value in headers_by_name.items() if key.lower() != "accept-encoding"
        }
        headers_by_name["Accept-Encoding"] = "gzip"
    fetch_url = directory._url_with_query_params(url, query_params_by_name)
    started = directory.time.monotonic()
    request = directory.urllib.request.Request(fetch_url, headers=headers_by_name)
    try:
        with directory.source_selection.open_source_request(directory, request, timeout=timeout) as response:
            if allow_gzip:
                body = directory._read_response_body_with_deadline(
                    response, timeout=timeout, max_bytes=directory.MAX_FHIR_JSON_BODY_BYTES + 1
                )
                body = directory.fhir_response.decode_bounded_response_body(
                    body, response.headers.get("Content-Encoding"), directory.MAX_FHIR_JSON_BODY_BYTES
                )
            else:
                body = directory._read_response_body_with_deadline(response, timeout=timeout)
            return (
                response.status,
                directory._decode_json_body(body),
                None,
                int((directory.time.monotonic() - started) * 1000),
            )
    except directory.urllib.error.HTTPError as exc:
        body = read_sync_error_body(directory, exc, timeout=timeout, allow_gzip=allow_gzip)
        error_payload_dict = directory._source_http_payload_with_retry_after(
            directory._decode_json_body(body), exc.headers
        )
        return exc.code, error_payload_dict, None, int((directory.time.monotonic() - started) * 1000)
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        if allow_gzip:
            error = (
                str(exc)
                if isinstance(exc, ValueError) and str(exc).startswith("provider_directory_response_")
                else type(exc).__name__
            )
        return None, None, error, int((directory.time.monotonic() - started) * 1000)


def read_sync_error_body(directory, response, *, timeout, allow_gzip):
    """Keep the stricter error cap and retain status when its body cannot be read."""
    try:
        if not allow_gzip:
            return response.read(1024 * 1024)
        body = directory._read_response_body_with_deadline(response, timeout=timeout, max_bytes=1024 * 1024 + 1)
        return directory.fhir_response.decode_bounded_response_body(
            body, response.headers.get("Content-Encoding"), 1024 * 1024
        )
    except Exception:
        return b""
