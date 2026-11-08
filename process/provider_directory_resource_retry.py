# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retry one retained REST cursor without waiting for its sibling scans."""

import asyncio
import datetime
import math
import re
from contextlib import nullcontext
from dataclasses import dataclass, field, replace

DEFAULT_RETRY_WAIT_LIMIT_SECONDS = 600.0
ATTEMPT_COUNTERS = (
    "rows_written",
    "source_fetch_elapsed_ms",
    "stream_write_elapsed_seconds",
    "checkpoint_persist_elapsed_seconds",
    "page_prefetch_started",
    "page_prefetch_consumed",
    "page_prefetch_discarded",
    "page_prefetch_wait_seconds",
    "pagination_cooldown_retries",
    "pagination_cooldown_wait_seconds",
)


@dataclass
class ResourceRetryState:
    """Keep attempt deltas separate from the cursor's cumulative position."""

    deadline_at: float | None
    attempt: int = 0
    no_progress: int = 0
    position: tuple[int, int] | None = None
    totals: dict = field(default_factory=lambda: dict.fromkeys(ATTEMPT_COUNTERS, 0))

    def absorb(self, result):
        """Sum writes and timings once while retaining the latest durable position."""
        position = (result.pages_fetched, result.rows_fetched)
        advanced = (
            self.position is not None
            and all(a >= b for a, b in zip(position, self.position))
            and position != self.position
        )
        self.no_progress = 0 if advanced else self.no_progress + 1
        self.position = position
        for name in ATTEMPT_COUNTERS:
            self.totals[name] += getattr(result, name)
        return replace(result, **self.totals)

    def is_exhausted(self, directory, delay):
        """Yield long default waits or stop at the deadline or failure budget."""
        budget = directory._source_fetch_retry_attempts()
        return (
            self.no_progress >= budget
            or (self.deadline_at is None and self.attempt >= budget)
            or (self.deadline_at is None and delay > DEFAULT_RETRY_WAIT_LIMIT_SECONDS)
            or (self.deadline_at is not None and directory.time.monotonic() + delay >= self.deadline_at)
        )


def retryable(directory, runner, source_record_by_field, fetch_result):
    """Only resume an unbounded native REST cursor with transient-fetch evidence."""
    options = runner.options
    checkpoint_context = source_record_by_field.get("_pagination_checkpoint_context")
    diagnostic = fetch_result.fetch_diagnostic or {}
    status = diagnostic.get("status")
    http_error = re.fullmatch(r"http_([0-9]{3})", fetch_result.error or "")
    if http_error and status != int(http_error[1]):
        return False
    error = None if http_error else fetch_result.error
    return bool(
        options.checkpointing_enabled
        and options.deferred_materialization
        and runner.is_streaming
        and options.per_resource_limit <= 0
        and options.page_limit <= 0
        and options.linked_resource_limit <= 0
        and not options.stale_cleanup
        and directory._has_bound_resource_scan_checkpoint(source_record_by_field, checkpoint_context)
        and checkpoint_context.allow_retained_pagination_reset is False
        and fetch_result.fetch_mode == "checkpointed_paged"
        and not fetch_result.complete
        and fetch_result.error
        and fetch_result.next_url_remaining
        and not fetch_result.is_bounded
        and diagnostic.get("response_class")
        in {
            "transient_transport_error",
            "transient_locked",
            "transient_rate_limited",
            "transient_server_error",
        }
        and directory._is_transient_source_fetch_failure(status, error)
    )


def parsed_deadline(value):
    """Accept only an aware timestamp and normalize its comparison timezone."""
    if not isinstance(value, str):
        return None
    try:
        boundary = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        return boundary.astimezone(datetime.UTC) if boundary.tzinfo is not None else None
    except ValueError, TypeError, OverflowError:
        return None


def retry_boundary(directory, source, result):
    """Honor both the resource deadline and the shared server pacing deadline."""
    now = datetime.datetime.now(datetime.UTC)
    delay = float(directory.SOURCE_TRANSIENT_RETRY_FALLBACK_SECONDS)
    if result.retry_not_before is not None:
        boundary = parsed_deadline(result.retry_not_before)
        if boundary is None:
            return None
        delay = max(0.0, (boundary - now).total_seconds())
    pacing = source.get("_provider_directory_request_pacing_state", source)
    delay = max(delay, pacing.get("retry_after_monotonic", 0) - directory.time.monotonic())
    if not math.isfinite(delay):
        return None
    try:
        return delay, (now + datetime.timedelta(seconds=delay)).isoformat()
    except OverflowError:
        return None


def update_metrics(directory, metrics_by_field):
    """Persist live retry fences in the existing ordinary continuation fields."""
    if not isinstance(metrics_by_field, dict) or not isinstance(metrics_by_field.get("active_source_groups"), list):
        return
    now = datetime.datetime.now(datetime.UTC)
    key = "provider_directory_retry_not_before_by_resource"
    scalar_key = directory.SOURCE_RETRY_NOT_BEFORE_METRIC
    previous_scalar = parsed_deadline(metrics_by_field.get(scalar_key))
    deadlines_by_resource = {
        resource: boundary
        for resource, deadline_value in (metrics_by_field.get(key) or {}).items()
        if (boundary := parsed_deadline(deadline_value)) is not None
    }
    for group_by_field in metrics_by_field["active_source_groups"]:
        if not isinstance(group_by_field, dict) or not isinstance(group_by_field.get("resource_scans"), dict):
            continue
        for resource, scan_by_field in group_by_field["resource_scans"].items():
            if not isinstance(scan_by_field, dict):
                continue
            previous = deadlines_by_resource.get(resource)
            current = parsed_deadline(scan_by_field.get("retry_not_before"))
            candidates = [
                deadline_value
                for deadline_value in (previous, current)
                if deadline_value is not None and deadline_value > now
            ]
            if candidates:
                deadlines_by_resource[resource] = max(candidates)
            else:
                deadlines_by_resource.pop(resource, None)
    if deadlines_by_resource or key in metrics_by_field:
        metrics_by_field[key] = {
            resource: deadline_value.isoformat() for resource, deadline_value in deadlines_by_resource.items()
        }
    future_boundaries = [
        deadline_value
        for deadline_value in (*deadlines_by_resource.values(), previous_scalar)
        if deadline_value is not None and deadline_value > now
    ]
    if future_boundaries or scalar_key in metrics_by_field or key in metrics_by_field:
        metrics_by_field[scalar_key] = max(future_boundaries).isoformat() if future_boundaries else None


def safe_error(result):
    """Publish fixed classes, never provider bodies, credentials or cursor text."""
    if result is None or not result.error:
        return None
    if re.fullmatch(r"http_[0-9]{3}", result.error):
        return result.error
    if result.error in {
        "TimeoutError",
        "ServerTimeoutError",
        "ClientPayloadError",
        "ClientConnectorError",
        "ConnectionResetError",
        "non_bundle_payload",
        "deadline_reached",
    }:
        return result.error
    return "source_fetch_error"


async def report(directory, runner, resource, state, retry, result=None, retry_at=None):
    """Replace one resource's safe progress snapshot without changing row counters."""
    if retry_at is None and result is not None:
        boundary = parsed_deadline(result.retry_not_before)
        retry_at = boundary.isoformat() if boundary is not None else None
    snapshot_by_field = {
        "state": state,
        "attempt": retry.attempt,
        "consecutive_no_progress": retry.no_progress,
        "error": safe_error(result),
        "retry_not_before": retry_at,
        "complete": bool(result is not None and directory._is_resource_fetch_complete_for_publish(result)),
        "pages_processed": result.pages_fetched if result is not None else None,
        "rows_processed": result.rows_fetched if result is not None else None,
    }
    async with runner.progress_lock:
        group_by_field = runner.active_group_by_key[runner.group_key]
        group_by_field["resource_scans"] = {**group_by_field.get("resource_scans", {}), resource: snapshot_by_field}
    await runner.report_progress(force=True)
    return snapshot_by_field


async def settled(directory, runner, resource, result):
    """Expose the final guarded result, including rejected empty collections."""
    if runner.group_key not in runner.active_group_by_key:
        return None
    previous = runner.active_group_by_key[runner.group_key].get("resource_scans", {}).get(resource, {})
    retry = ResourceRetryState(
        None, attempt=previous.get("attempt", 1), no_progress=previous.get("consecutive_no_progress", 0)
    )
    state = (
        "complete"
        if directory._is_resource_fetch_complete_for_publish(result)
        else ("exhausted" if previous.get("state") == "exhausted" else "incomplete")
    )
    return await report(directory, runner, resource, state, retry, result, previous.get("retry_not_before"))


async def wait(directory, runner, delay):
    """Keep cooperative cancellation responsive without owning a scan slot."""
    deadline = directory.time.monotonic() + delay
    while True:
        await directory._raise_if_resource_import_cancelled(runner.options.cancel_ctx, runner.options.cancel_task)
        remaining = deadline - directory.time.monotonic()
        if remaining <= 0:
            return
        await asyncio.sleep(min(remaining, 5.0))


async def fetch(directory, runner, resource, source_record_by_field, *, report_start, resource_semaphore=None):
    """Retry this cursor within existing budgets, joining ordinary fatal failures."""
    seconds = runner.options.resource_deadline_seconds
    retry = ResourceRetryState(None)
    fetch_result = None
    while True:
        retry.attempt += 1
        async with resource_semaphore if resource_semaphore is not None else nullcontext():
            await directory._raise_if_resource_import_cancelled(runner.options.cancel_ctx, runner.options.cancel_task)
            if retry.attempt == 1 and seconds > 0:
                retry.deadline_at = directory.time.monotonic() + seconds
            remaining = retry.deadline_at - directory.time.monotonic() if retry.deadline_at is not None else None
            if remaining is not None and remaining <= 0:
                await report(directory, runner, resource, "exhausted", retry, fetch_result)
                return fetch_result
            await report(directory, runner, resource, "fetching", retry, fetch_result)
            fetched = await runner.fetch(
                resource,
                source_record_by_field,
                report_start=report_start and retry.attempt == 1,
                deadline_seconds=remaining,
            )
        if fetched is None:
            await report(directory, runner, resource, "unavailable", retry)
            return None
        fetch_result = retry.absorb(fetched)
        if not retryable(directory, runner, source_record_by_field, fetched):
            return fetch_result
        boundary = retry_boundary(directory, source_record_by_field, fetched)
        if boundary is None or retry.is_exhausted(directory, boundary[0]):
            if boundary is not None:
                fetch_result = replace(fetch_result, retry_not_before=boundary[1])
            await report(
                directory, runner, resource, "exhausted", retry, fetch_result, boundary[1] if boundary else None
            )
            return fetch_result
        await report(directory, runner, resource, "retry_wait", retry, fetch_result, boundary[1])
        await wait(directory, runner, boundary[0])


async def import_one(directory, runner, resource, report_start, resource_semaphore):
    """Fetch one task-local source view and merge its guarded outcome once."""
    source_by_field = dict(runner.source_record)
    result = await fetch(
        directory, runner, resource, source_by_field, report_start=report_start, resource_semaphore=resource_semaphore
    )
    return await runner.record(resource, source_by_field, result)
