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
from collections import deque
from contextlib import asynccontextmanager, nullcontext
from typing import Any, Awaitable, Callable, Mapping

from process.provider_directory_fetch_types import ResourceFetchResult
from process.provider_directory_optum_outage import HTTP_OUTAGE_STATUSES, PARTITION_REQUEST, OptumOutage

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

    def _log_limit_change(self, directory, previous):
        if previous != self.limit:
            directory.LOGGER.info(
                "Provider Directory request concurrency %s: previous=%s effective=%s",
                "reduced" if self.limit < previous else "recovered",
                previous,
                self.limit,
            )

    async def send(self, directory, send, _pacing_state):
        """Apply the endpoint's existing reduction after an admitted send."""
        response = await send()
        if _response_limits_concurrency(response):
            previous = self.limit
            self.reduce()
            self._log_limit_change(directory, previous)
        return response


class _OptumRequestSlots(AdaptiveRequestSlots):
    """Back off for rate limits or clustered transport failures."""

    RESPONSE_WINDOW_SIZE = 32
    MIN_TRANSIENT_SAMPLE = 12
    TRANSIENT_FAILURE_PERCENT = 25
    CONSECUTIVE_TRANSIENT_LIMIT = 4
    MAX_TRANSIENT_FLOOR = 8
    MAX_SATURATION_GAP_SECONDS = 1.0
    MIN_SATURATED_SECONDS = 30.0
    MIN_HEALTHY_RESPONSES = 8

    def __init__(self, capacity):
        super().__init__(capacity)
        self.capacity = capacity
        self.waiters = self.started = self.completed = 0
        self.request_seconds = 0.0
        self.reductions = self.recoveries = 0
        self.throttled_responses = self.transient_failures = 0
        self.transient_by_kind: dict[str, int] = {}
        self.outage = OptumOutage()
        self.recent_outcomes = deque(maxlen=self.RESPONSE_WINDOW_SIZE)
        self._reset_health()

    def _reset_health(self):
        self.saturated_seconds = 0.0
        self.saturated_since = None
        self.last_saturated_at = None

    async def __aenter__(self):
        self.waiters += 1
        try:
            return await super().__aenter__()
        finally:
            self.waiters -= 1

    async def send(self, directory, send, pacing_state):
        """Record actual sends after pacing, token checks and both slot budgets."""
        if self.started == self.completed and not self.waiters:
            self._reset_health()
        epoch = self.reductions + self.recoveries
        outage_epoch = self.outage.epoch
        send_limit = self.limit
        started_at = directory.time.monotonic()
        self.started += 1
        if self.started - self.completed == self.limit:
            if (
                self.last_saturated_at is not None
                and started_at - self.last_saturated_at > self.MAX_SATURATION_GAP_SECONDS
            ):
                self._reset_health()
            self.saturated_since = started_at
        response = None
        try:
            response = await send()
            return response
        finally:
            now = directory.time.monotonic()
            self.completed += 1
            self.request_seconds += now - started_at
            if self.saturated_since is not None:
                self.saturated_seconds += now - self.saturated_since
                self.saturated_since = None
                self.last_saturated_at = now
            previous = self.limit
            self._adjust(epoch, send_limit, response, now, pacing_state)
            if response is not None:
                self.outage.observe(response[0], now, outage_epoch)
            self._log_limit_change(directory, previous)

    def _adjust(self, epoch, send_limit, response, now, pacing_state):
        if response is None:
            self._reset_health()
            return
        is_current_epoch = epoch == self.reductions + self.recoveries
        if response[0] == 429:
            self.throttled_responses += 1
            # A late throttle still applies if recovery has restored its original allowance.
            if is_current_epoch or send_limit <= self.limit:
                self._back_off()
            return
        outcome = _optum_request_outcome(response)
        if outcome.startswith("transient_"):
            self.transient_failures += 1
            self.transient_by_kind[outcome] = self.transient_by_kind.get(outcome, 0) + 1
            outcome = "transient"
        if not is_current_epoch:
            return
        self.recent_outcomes.append(outcome)
        if outcome == "transient" and self._has_excess_transient_failures():
            self._back_off(min(self.MAX_TRANSIENT_FLOOR, max(1, self.capacity // 4)))
        elif now < pacing_state.get("retry_after_monotonic", 0.0):
            self._reset_health()
        elif outcome == "healthy":
            self._try_recover(now)

    def _has_excess_transient_failures(self):
        window = self.recent_outcomes
        failures = window.count("transient")
        has_sustained_failures = (
            len(window) >= self.MIN_TRANSIENT_SAMPLE and failures * 100 >= len(window) * self.TRANSIENT_FAILURE_PERCENT
        )
        tail_length = self.CONSECUTIVE_TRANSIENT_LIMIT
        return has_sustained_failures or list(window)[-tail_length:] == ["transient"] * tail_length

    def _try_recover(self, now):
        successes = self.recent_outcomes.count("healthy")
        if (
            self.limit < self.capacity
            and self.waiters
            and self.last_saturated_at == now
            and successes >= self.MIN_HEALTHY_RESPONSES
            and successes * 5 >= len(self.recent_outcomes) * 4
            and self.saturated_seconds >= self.MIN_SATURATED_SECONDS
        ):
            self.limit = min(self.capacity, self.limit + max(2, self.limit // 5))
            self.recoveries += 1
            self.recent_outcomes.clear()
            self._reset_health()

    def _back_off(self, floor=1):
        self.recent_outcomes.clear()
        self._reset_health()
        if self.limit > floor:
            self.limit = max(floor, self.limit // 2)
            self.reductions += 1


def request_concurrency_snapshot(source_record: Mapping[str, Any]) -> dict[str, int] | None:
    """Return numeric Optum transport totals shared by resource views and progress callbacks."""
    pacing_state = source_record.get("_provider_directory_request_pacing_state") or {}
    slots = pacing_state.get("optum_request_slots")
    if slots is None or not slots.started:
        return None
    outage = slots.outage
    return {
        "capacity": slots.capacity,
        "limit": slots.limit,
        "active": slots.started - slots.completed,
        "waiters": slots.waiters,
        "started": slots.started,
        "completed": slots.completed,
        "request_time_ms": round(slots.request_seconds * 1000),
        "reductions": slots.reductions,
        "recoveries": slots.recoveries,
        "throttled_responses": slots.throttled_responses,
        "transient_failures": slots.transient_failures,
        **slots.transient_by_kind,
        **({"http_500_responses": outage.http_500_responses} if outage.http_500_responses else {}),
        **(
            {
                "outage_pauses": outage.epoch,
                "outage_active": int(outage.probe_after is not None),
                "outage_probes": outage.probes,
            }
            if outage.epoch
            else {}
        ),
    }


def _response_limits_concurrency(response):
    """Reduce load for throttling, unavailable upstreams and transport timeouts."""
    status, _payload, error, _elapsed = response
    return status in {429, 503, 504} or _request_timed_out(error)


def _request_timed_out(error):
    return bool(error and any(word in error.lower() for word in ("timeout", "timed out")))


def _optum_request_outcome(response):
    status, payload, error, _elapsed = response
    if status in {502, 503, 504}:
        return f"transient_http_{status}"
    if _request_timed_out(error):
        return "transient_timeout"
    if error in {"ClientConnectionResetError", "ClientOSError", "ConnectionResetError", "ServerDisconnectedError"}:
        return "transient_disconnect"
    resource_type = payload.get("resourceType") if isinstance(payload, dict) else None
    if (
        status == 200
        and error is None
        and isinstance(resource_type, str)
        and resource_type not in {"", "OperationOutcome"}
    ):
        return "healthy"
    return "other"


def _is_retryable_optum_continuation(response):
    return response[0] == 429 or _optum_request_outcome(response).startswith("transient_")


async def fetch_partition_response(directory, request):
    """Carry the partition identity and deadline through the shared request admission."""
    identity = directory._uhc_partition_identity(
        request.source_record, request.resource_type, request.partition_start_url
    )
    token = PARTITION_REQUEST.set((request, identity))
    try:
        return await _fetch_partition_with_cooldown(directory, request, identity)
    finally:
        PARTITION_REQUEST.reset(token)


async def _fetch_partition_with_cooldown(directory, request, identity):
    """Retry a mandatory Optum continuation on its original cursor before deferring the leaf."""
    response = await directory._fetch_source_json(
        request.source_record, request.current_url, timeout=request.fetch_options.timeout
    )
    if (
        request.is_residual_only
        or request.current_url == request.partition_start_url
        or not _is_retryable_optum_continuation(response)
        or not directory._has_fhir_continuation_query(request.current_url)
        or not identity
    ):
        return response
    cooldown = await directory._retry_rest_pagination_after_cooldown(
        request.source_record,
        request.current_url,
        response,
        timeout=request.fetch_options.timeout,
        deadline_at=request.fetch_options.deadline_at,
        is_retryable=_is_retryable_optum_continuation,
    )
    return cooldown.fetch_result


def strict_search_next_link(
    payload: dict[str, Any],
) -> str | None:
    """Return the exact next link or reject ambiguous paging metadata."""
    raw_links = payload.get("link", [])
    if not isinstance(raw_links, list) or any(not isinstance(link, dict) for link in raw_links):
        raise ValueError("provider_directory_current_version_census_next_link_invalid")
    next_urls: list[str] = []
    for link_by_field in raw_links:
        if link_by_field.get("relation") != "next":
            continue
        raw_url = link_by_field.get("url")
        if type(raw_url) is not str or not raw_url or raw_url.strip() != raw_url:
            raise ValueError("provider_directory_current_version_census_next_link_invalid")
        next_urls.append(raw_url)
    if len(next_urls) > 1:
        raise ValueError("provider_directory_current_version_census_next_link_invalid")
    return next_urls[0] if next_urls else None


def strict_search_bundle_error(
    payload: dict[str, Any],
    resource_type: str,
) -> str | None:
    """Reject malformed search sets and rows outside the requested collection."""
    if payload.get("resourceType") != "Bundle" or payload.get("type") != "searchset":
        return "provider_directory_current_version_census_searchset_required"
    raw_entries = payload.get("entry", [])
    if not isinstance(raw_entries, list):
        return "provider_directory_current_version_census_entries_invalid"
    for entry_by_field in raw_entries:
        resource_by_field = entry_by_field.get("resource") if isinstance(entry_by_field, dict) else None
        if not isinstance(resource_by_field, dict):
            return "provider_directory_current_version_census_entries_invalid"
        if resource_by_field.get("resourceType") != resource_type:
            return "provider_directory_current_version_census_resource_type_mismatch"
    return None


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


def resource_search_payload_error(
    directory,
    source_record: dict[str, Any],
    request_url: str,
    status_code: int | None,
    payload_by_field: dict[str, Any] | None,
) -> str | None:
    """Reject errored or ambiguous successful collection responses."""
    aetna_error = directory._aetna_resource_search_payload_error(
        source_record,
        request_url,
        status_code,
        payload_by_field,
    )
    if aetna_error:
        return aetna_error
    resource_type = directory._fhir_collection_search_resource_type(
        source_record,
        request_url,
    )
    api_base = directory._canonical_base(source_record.get("canonical_api_base") or source_record.get("api_base"))
    is_root_continuation = bool(
        api_base
        and directory._canonical_base(request_url) == api_base
        and directory._has_fhir_continuation_query(request_url)
    )
    if status_code != 200 or not (resource_type or is_root_continuation) or not isinstance(payload_by_field, dict):
        return None
    if payload_by_field.get("resourceType") == "OperationOutcome":
        return directory.RESOURCE_SEARCH_OPERATION_OUTCOME_ERROR
    if not directory._is_bundle_payload(payload_by_field):
        return None
    if (
        resource_type
        and api_base == directory.CIGNA_PROVIDER_DIRECTORY_BASE
        and directory._has_fhir_continuation_query(request_url)
        and directory._is_empty_fhir_search_set(status_code, payload_by_field, None)
        and type(payload_by_field.get("total")) is int
        and payload_by_field["total"] == 0
    ):
        return directory.CIGNA_AMBIGUOUS_EMPTY_CONTINUATION_ERROR
    entry_resources = [entry["resource"] for entry in directory._bundle_entries(payload_by_field)]
    outcome_resources = [resource for resource in entry_resources if resource.get("resourceType") == "OperationOutcome"]
    resource_types = (resource_type,) if resource_type else directory.DEFAULT_RESOURCES
    if outcome_resources and (
        not any(resource.get("resourceType") in resource_types for resource in entry_resources)
        or any(directory._has_operation_outcome_error(outcome) for outcome in outcome_resources)
    ):
        return directory.RESOURCE_SEARCH_OPERATION_OUTCOME_ERROR
    return None


async def fetch_paced_source_response(directory, source_record, url, *, timeout):
    """Re-admit every authenticated send after any shared token renewal."""
    if directory.source_selection.uses_credentialed_rest_transport(directory, source_record):

        async def send_authenticated(options):
            """Acquire ordinary request capacity for one authenticated GET."""
            return await _fetch_admitted_source_response(
                directory,
                source_record,
                url,
                send=lambda: directory.source_selection.send_authenticated_page(
                    directory, source_record, url, timeout=timeout, headers_by_name=options["headers"]
                ),
                ready=lambda: directory.source_selection.has_current_credentialed_rest_options(directory, options),
            )

        return await directory.source_selection.fetch_credentialed_rest_json(
            directory, source_record, url, timeout=timeout, send=send_authenticated
        )
    return await _fetch_admitted_source_response(
        directory,
        source_record,
        url,
        send=lambda: directory._fetch_source_json_unpaced(source_record, url, timeout=timeout),
        probe_timeout=min(timeout, 10),
    )


def _source_response_slots(directory, source_record, url):
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
            slots_type = AdaptiveRequestSlots
            if directory.uhc_scan.is_source(directory, source_record) and directory._is_url_within_api_base(
                url, directory.UHC_PROVIDER_DIRECTORY_BASE
            ):
                slots_type = _OptumRequestSlots
            slot_map[slot_key] = slots_type(max(1, capacity))
        request_slots = slot_map[slot_key]
        if isinstance(request_slots, _OptumRequestSlots):
            pacing_state["optum_request_slots"] = request_slots
    return pacing_state, request_slots, total_slots


async def _probe_optum_backend(directory, source_record, timeout):
    """Send a bounded search through the same source admission and a separate probe cursor."""
    url = f"{directory.UHC_PROVIDER_DIRECTORY_BASE}/InsurancePlan?_count=1"
    partition = PARTITION_REQUEST.set(None)
    proxy_cursor = directory.source_proxy.PARTITION_CURSOR_KEY.set("optum-outage-probe")
    try:
        async with asyncio.timeout(timeout):
            return await _fetch_admitted_source_response(
                directory,
                source_record,
                url,
                send=lambda: directory._fetch_source_json_unpaced(source_record, url, timeout=timeout),
            )
    except TimeoutError:
        return None, None, "TimeoutError", timeout * 1000
    finally:
        directory.source_proxy.PARTITION_CURSOR_KEY.reset(proxy_cursor)
        PARTITION_REQUEST.reset(partition)


async def _fetch_admitted_source_response(directory, source_record, url, *, send, ready=None, probe_timeout=None):
    """Admit paced sends and hold Optum scans outside request slots during a backend outage."""
    if directory.current_version_census_contract(source_record) is not None:
        return await send()
    pacing_state, request_slots, total_slots = _source_response_slots(directory, source_record, url)
    pacing_options = {} if ready is None else {"ready": ready}
    outage = (
        request_slots.outage if probe_timeout is not None and isinstance(request_slots, _OptumRequestSlots) else None
    )
    can_retry_interrupted = True
    while True:
        if outage is not None and not await outage.has_recovered_within_deadline(
            directory, lambda: _probe_optum_backend(directory, source_record, probe_timeout)
        ):
            return None, None, "deadline_reached", 0
        async with request_slots, total_slots:
            if outage is not None and outage.probe_after is not None:
                continue
            try:
                await directory._pace_source_request(source_record, allow_census_defer=True, **pacing_options)
            except directory.source_selection.CensusRequestDeferred:
                return None, None, directory.source_selection.CENSUS_REQUEST_DEFERRED_ERROR, 0
            if outage is not None and outage.probe_after is not None:
                continue
            if isinstance(request_slots, AdaptiveRequestSlots) and request_slots.active > request_slots.limit:
                continue
            if ready is not None and not ready():
                return None
            outage_epoch = outage.epoch if outage is not None else None
            response = await _send_in_source_slot(directory, source_record, request_slots, pacing_state, send)
        if (
            can_retry_interrupted
            and outage is not None
            and outage_epoch != outage.epoch
            and response[0] in HTTP_OUTAGE_STATUSES
        ):
            can_retry_interrupted = False
            continue
        return response


async def _send_in_source_slot(directory, source_record, request_slots, pacing_state, send):
    response = (
        await request_slots.send(directory, send, pacing_state)
        if isinstance(request_slots, AdaptiveRequestSlots)
        else await send()
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


async def fetch_last_updated_partition_count(directory, source_record, request_url, *, timeout, deadline_at=None):
    """Fetch one exact, non-paginated partition count observation."""
    retry_delays = directory.LAST_UPDATED_PARTITION_COUNT_ENVELOPE_RETRY_DELAYS_SECONDS
    attempt_index = 0
    while True:
        fetch_result, is_deadline_blocked = await directory._fetch_cooldown_checkpoint_page(
            source_record,
            request_url,
            (None, None, "TimeoutError: census deadline reached", 0),
            timeout=timeout,
            deadline_at=deadline_at,
        )
        status_code, response_payload, error, _elapsed = fetch_result
        split_observation = None
        if not is_deadline_blocked:
            split_observation = directory.capped_partition.split_observation(
                directory, source_record, request_url, status_code, response_payload, error
            )
        if split_observation is not None:
            return directory.LastUpdatedCountFetch(split_observation, pages_fetched=attempt_index + 1)
        pages_fetched = attempt_index + int(error != directory.source_selection.CENSUS_REQUEST_DEFERRED_ERROR)
        if status_code != 200 or error:
            failure_error = error or f"http_{status_code}"
            is_transient = directory._is_transient_source_fetch_failure(status_code, error)
            return directory.LastUpdatedCountFetch(
                observation=(None if is_transient else directory.CountObservation.error(failure_error)),
                pages_fetched=pages_fetched,
                error=failure_error,
                transient=is_transient,
                retry_not_before=(
                    directory._last_updated_partition_retry_not_before(
                        source_record,
                        request_url,
                        status_code,
                        response_payload,
                        error,
                        max_delay_seconds=directory.source_selection.retry_after_limit(directory, source_record),
                    )
                    if is_transient
                    else None
                ),
            )
        count_fetch = directory._validate_last_updated_partition_count_payload(response_payload)
        if count_fetch.error != "non_searchset_count_bundle":
            return directory.replace(count_fetch, pages_fetched=pages_fetched)
        if attempt_index == len(retry_delays):
            return directory.replace(
                count_fetch,
                pages_fetched=pages_fetched,
                error="non_searchset_count_bundle_retry_exhausted",
            )
        delay = retry_delays[attempt_index]
        if deadline_at is not None:
            delay = min(delay, max(0, deadline_at - directory.time.monotonic()))
        await directory.asyncio.sleep(delay)
        attempt_index += 1


def opaque_terminal_proof(directory, proof_by_field, request_url, pages_processed, rows_processed):
    """Bind an observed terminal page to its exact issued request and committed counters."""
    return {
        **proof_by_field,
        "terminal_page": {
            "request_sha256": directory.hashlib.sha256(request_url.encode("utf-8")).hexdigest(),
            "pages_processed": pages_processed,
            "rows_processed": rows_processed,
            "has_next": False,
        },
    }


async def opaque_terminal_resume_error(directory, context, resource_type, resume_state):
    """Permit count-only recovery only for an intact terminal receipt and candidate."""
    if "terminal_page" not in resume_state.completeness or resume_state.complete:
        return None
    receipt_by_field = resume_state.completeness["terminal_page"]
    if (
        not isinstance(receipt_by_field, dict)
        or set(receipt_by_field) != {"request_sha256", "pages_processed", "rows_processed", "has_next"}
        or receipt_by_field["has_next"] is not False
        or type(receipt_by_field["pages_processed"]) is not int
        or receipt_by_field["pages_processed"] < 1
        or type(receipt_by_field["rows_processed"]) is not int
        or receipt_by_field["rows_processed"] < 0
        or receipt_by_field["pages_processed"] != resume_state.pages_processed
        or receipt_by_field["rows_processed"] != resume_state.rows_processed
        or not resume_state.next_url
        or receipt_by_field["request_sha256"]
        != directory.hashlib.sha256(resume_state.next_url.encode("utf-8")).hexdigest()
    ):
        return "terminal_receipt_invalid"
    if await directory._caresource_unique_candidate_count(context, resource_type) != resume_state.rows_processed:
        return "terminal_candidate_count_mismatch"
    return None


def retained_census_retry_boundary(directory, source_record, retry_at):
    """Keep an observed shared cooldown when a queued census exhausts its deadline."""
    if (
        retry_at is None
        or not directory.source_selection.uses_census_retry_policy(directory, source_record)
        or directory.current_version_census_contract(source_record) is not None
    ):
        return retry_at
    pacing_by_field = source_record.get("_provider_directory_request_pacing_state", source_record)
    remaining = pacing_by_field.get("retry_after_monotonic", 0) - directory.time.monotonic()
    if remaining <= 0:
        return retry_at
    return max(
        retry_at,
        directory._retry_not_before_timestamp(directory.datetime.datetime.now(directory.datetime.UTC), remaining),
    )


def resource_fetch_timing_diagnostic(directory, fetch_result: ResourceFetchResult) -> dict[str, Any]:
    """Return transport, persistence, and prefetch timings and counts."""
    return {
        "source_fetch_elapsed_ms": fetch_result.source_fetch_elapsed_ms,
        "stream_write_elapsed_seconds": fetch_result.stream_write_elapsed_seconds,
        "checkpoint_persist_elapsed_seconds": fetch_result.checkpoint_persist_elapsed_seconds,
        "page_prefetch_eligible": fetch_result.page_prefetch_eligible,
        "page_prefetch_started": fetch_result.page_prefetch_started,
        "page_prefetch_consumed": fetch_result.page_prefetch_consumed,
        "page_prefetch_discarded": fetch_result.page_prefetch_discarded,
        "page_prefetch_wait_seconds": fetch_result.page_prefetch_wait_seconds,
    }


def resource_fetch_completeness_diagnostics(directory, fetch_result: ResourceFetchResult) -> dict[str, Any]:
    """Return mode-specific completeness proof fields."""
    subset_proof = (
        directory._sanitized_server_issued_subset_execution_proof(fetch_result.fetch_diagnostic)
        if fetch_result.fetch_mode == directory.SERVER_ISSUED_SUBSET_FETCH_MODE
        else None
    )
    replay_evidence = (
        directory._server_issued_subset_internal_replay_evidence(fetch_result.fetch_diagnostic)
        if fetch_result.fetch_mode == directory.SERVER_ISSUED_SUBSET_FETCH_MODE
        else None
    )
    return {
        **(
            {
                directory.timestamp_recovery.KEY: directory.timestamp_recovery.completion_receipt(
                    fetch_result.fetch_diagnostic
                )
            }
            if directory.timestamp_recovery.marked(fetch_result.fetch_diagnostic)
            else {}
        ),
        "last_updated_completeness": fetch_result.fetch_diagnostic
        if fetch_result.fetch_mode == directory.LAST_UPDATED_PARTITION_FETCH_MODE
        else None,
        "caresource_opaque_cursor_completeness": fetch_result.fetch_diagnostic
        if fetch_result.fetch_mode == directory.CARESOURCE_OPAQUE_CURSOR_FETCH_MODE
        else None,
        "current_version_census_completeness": fetch_result.fetch_diagnostic
        if fetch_result.fetch_mode == directory.CURRENT_VERSION_CENSUS_FETCH_MODE
        else None,
        "server_issued_subset_completeness": subset_proof,
        "server_issued_subset_coverage": directory._server_issued_subset_coverage(fetch_result.fetch_diagnostic)
        if fetch_result.fetch_mode == directory.SERVER_ISSUED_SUBSET_FETCH_MODE
        else None,
        **(
            {directory._SERVER_ISSUED_SUBSET_INTERNAL_REPLAY_KEY: replay_evidence}
            if replay_evidence is not None
            else {}
        ),
    }


def sanitized_server_issued_subset_execution_proof(directory, proof_by_field: Any) -> dict[str, Any] | None:
    """Exclude root-specific opaque-hop hashes from status diagnostics."""
    if not isinstance(proof_by_field, Mapping):
        return None
    return {
        field_name: field_value
        for field_name, field_value in proof_by_field.items()
        if field_name != "continuation_hop_sha256"
    }


def server_issued_subset_internal_replay_evidence(directory, proof_by_field: Any) -> dict[str, Any] | None:
    """Retain token-inclusive replay hashes only for sealed dataset evidence."""
    if not isinstance(proof_by_field, Mapping):
        return None
    continuation_hop_sha256 = proof_by_field.get("continuation_hop_sha256")
    if type(continuation_hop_sha256) is not list:
        return None
    return {"continuation_hop_sha256": list(continuation_hop_sha256)}


def server_issued_subset_coverage(directory, proof_by_field: Any) -> dict[str, Any] | None:
    """Return safe resource-level coverage without implying absent targets."""
    if not isinstance(proof_by_field, Mapping):
        return None
    geometry = proof_by_field.get("terminal_page_geometry")
    continuation_shape_sha256 = proof_by_field.get("continuation_shape_sha256")
    return {
        "cutoff": proof_by_field.get("cutoff"),
        "scope": "server_issued_traversal_subset",
        "advertised_pre": proof_by_field.get("advertised_pre"),
        "advertised_post": proof_by_field.get("advertised_post"),
        "returned_unique": proof_by_field.get("returned_unique"),
        "deficit": proof_by_field.get("deficit"),
        "geometry": {
            "pages": geometry.get("pages_processed"),
            "logical_terminal_offset": geometry.get("terminal_page_start_offset"),
            "sparse_pages": geometry.get("sparse_pages"),
            "empty_pages": geometry.get("empty_pages"),
            "page_entry_counts_sha256": directory.subset_canonical_sha256(proof_by_field.get("page_entry_counts")),
            "geometry_sha256": directory.subset_canonical_sha256(
                {**dict(geometry), "page_entry_counts": proof_by_field.get("page_entry_counts")}
            ),
        }
        if isinstance(geometry, Mapping)
        else None,
        "continuation": {
            "validated_hops": len(continuation_shape_sha256),
            "chain_sha256": directory.subset_canonical_sha256(continuation_shape_sha256),
        }
        if type(continuation_shape_sha256) is list
        else None,
        "twin_state": "pending_matching_reviewed_root",
        "proof_state": "resource_terminal_verified" if proof_by_field.get("verified") is True else "not_verified",
        "unresolved_reference_count": None,
        "absence_semantics": "unknown_under_subset",
    }


def resource_fetch_diagnostic(directory, fetch_result: ResourceFetchResult, *, rows_written: int) -> dict[str, Any]:
    """Return one resource fetch's nonsecret operational diagnostics."""
    is_server_issued_subset = fetch_result.fetch_mode == directory.SERVER_ISSUED_SUBSET_FETCH_MODE
    is_timestamp_traversal = fetch_result.fetch_mode == directory.timestamp_recovery.FETCH_MODE
    return {
        "complete": fetch_result.complete,
        "collection_complete": False if is_server_issued_subset or is_timestamp_traversal else fetch_result.complete,
        **(
            {
                "traversal_complete": fetch_result.complete,
                "absence_semantics": "unknown_outside_observed_timestamp_traversal",
            }
            if is_timestamp_traversal
            else {}
        ),
        **(
            {
                "traversal_complete": fetch_result.complete,
                "source_continuation_exhausted": bool(fetch_result.complete and (not fetch_result.next_url_remaining)),
                "absence_semantics": "unknown_under_subset",
            }
            if is_server_issued_subset
            else {}
        ),
        "plan_graph_complete": False,
        "bounded": fetch_result.is_bounded,
        "error": fetch_result.error,
        "fetch_mode": fetch_result.fetch_mode,
        "pages_fetched": fetch_result.pages_fetched,
        "rows_fetched": fetch_result.rows_fetched,
        "rows_written": rows_written,
        **directory._resource_fetch_timing_diagnostic(fetch_result),
        "row_limit_reached": fetch_result.row_limit_reached,
        "page_limit_reached": fetch_result.page_limit_reached,
        "hard_page_limit_reached": fetch_result.hard_page_limit_reached,
        "deadline_reached": fetch_result.deadline_reached,
        "next_url_remaining": fetch_result.next_url_remaining,
        "retry_not_before": fetch_result.retry_not_before,
        "source_fetch": directory._safe_source_fetch_diagnostic(fetch_result),
        **directory._resource_fetch_completeness_diagnostics(fetch_result),
        "pagination_cooldown_retries": fetch_result.pagination_cooldown_retries,
        "pagination_cooldown_wait_seconds": fetch_result.pagination_cooldown_wait_seconds,
        "pagination_cooldown_recovered": fetch_result.is_pagination_cooldown_recovered,
        "pagination_cooldown_exhausted": fetch_result.is_pagination_cooldown_exhausted,
        "pagination_cooldown_deadline_blocked": fetch_result.is_pagination_cooldown_deadline_blocked,
    }


def safe_source_fetch_diagnostic(directory, fetch_result: ResourceFetchResult) -> dict[str, Any] | None:
    """Hide source transport detail for reviewed cursor proof modes."""
    hidden_fetch_modes = {
        directory.timestamp_recovery.FETCH_MODE,
        directory.LAST_UPDATED_PARTITION_FETCH_MODE,
        directory.CARESOURCE_OPAQUE_CURSOR_FETCH_MODE,
        directory.CURRENT_VERSION_CENSUS_FETCH_MODE,
        directory.SERVER_ISSUED_SUBSET_FETCH_MODE,
    }
    return None if fetch_result.fetch_mode in hidden_fetch_modes else fetch_result.fetch_diagnostic


def pagination_terminal_failure_details(
    directory, diagnostics_by_resource: dict[str, dict[str, Any]], incomplete_resource_types: list[str]
) -> str | None:
    """Return the first terminal completeness failure across census modes."""
    timestamp_failures = [
        resource
        for resource in incomplete_resource_types
        if str(diagnostics_by_resource[resource].get("error") or "").startswith(
            "provider_directory_timestamp_recovery_blocked:"
        )
    ]
    if timestamp_failures:
        return ",".join((f"{resource}={diagnostics_by_resource[resource]['error']}" for resource in timestamp_failures))
    failure_helpers = (
        directory._last_updated_partition_terminal_failure_details,
        directory._caresource_terminal_failure_details,
        directory._current_version_census_terminal_failure_details,
    )
    for failure_helper in failure_helpers:
        failure_details = failure_helper(diagnostics_by_resource, incomplete_resource_types)
        if failure_details:
            return failure_details
    return None


def is_resource_diagnostic_acquisition_complete(directory, diagnostic: dict[str, Any]) -> bool:
    """Validate recovery evidence before the existing ordinary and graph completion rules."""
    if not directory.timestamp_recovery.diagnostic_is_complete(diagnostic):
        return False
    if diagnostic.get("complete") is True:
        return True
    return bool(
        diagnostic.get("fetch_mode") == directory.UHC_PLAN_GRAPH_FETCH_MODE
        and diagnostic.get("plan_graph_complete") is True
        and (diagnostic.get("collection_complete") is False)
        and (diagnostic.get("stability_verified") is True)
        and (not diagnostic.get("error"))
        and (not diagnostic.get("bounded"))
        and (not diagnostic.get("next_url_remaining"))
    )


def is_resource_fetch_complete_for_publish(directory, result: ResourceFetchResult) -> bool:
    """Apply the existing completion accounting with recovery receipt validation."""
    return (
        result.complete
        and (not result.error)
        and (not result.is_bounded)
        and (not result.next_url_remaining)
        and directory.timestamp_recovery.diagnostic_is_complete(
            directory._resource_fetch_diagnostic(result, rows_written=result.rows_written)
        )
    )


def record_resource_completion(
    directory,
    completion: dict[str, set[str]] | None,
    resource_type: str,
    source_ids: list[str],
    result: ResourceFetchResult,
) -> None:
    """Apply the existing completion accounting with recovery receipt validation."""
    if completion is None or not directory._is_resource_fetch_complete_for_publish(result):
        return
    completion.setdefault(resource_type, set()).update(source_ids)


def source_fetch_candidate_urls(directory, url: str) -> list[str]:
    """Preserve exact recovery requests and ordinary page-size fallbacks."""
    parsed = urllib.parse.urlsplit(url)
    query_items = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    query_by_name = dict(query_items)
    is_timestamp_window = (
        len(query_items) == 2
        and query_by_name.keys() == {"_count", "_lastUpdated"}
        and query_by_name["_count"] == str(directory.timestamp_recovery.PAGE_SIZE)
        and query_by_name["_lastUpdated"].startswith("lt")
        and url.split("?", 1)[0]
        in {
            f"{directory.INDEPENDENCE_PROVIDER_DIRECTORY_BASE}/{resource}"
            for resource in directory.INDEPENDENCE_TIMESTAMP_RECOVERY_RESOURCES
        }
    )
    if directory.christus.is_request_url(url) or directory._has_fhir_continuation_query(url) or is_timestamp_window:
        return [url]
    try:
        requested_count = int(query_by_name.get("_count") or 0)
    except TypeError, ValueError:
        return [url]
    if requested_count <= 1:
        return [url]
    candidate_urls = [url]
    for fallback_count in (100, 25, 10, 1):
        if fallback_count >= requested_count:
            continue
        candidate_urls.append(directory._url_with_replaced_query_item(url, "_count", str(fallback_count)))
    return candidate_urls


def source_fetch_response_class(
    status_code: int | None,
    fetch_error: str | None,
) -> str | None:
    """Classify source transport failures for the existing bounded retry policy."""
    if fetch_error:
        return "transient_transport_error"
    if status_code == 423:
        return "transient_locked"
    if status_code == 429:
        return "transient_rate_limited"
    if status_code is not None and 500 <= status_code < 600:
        return "transient_server_error"
    return None
