# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pause a shared Optum scan wave during a sustained resource backend outage."""

import asyncio
import contextvars
from collections import deque

PARTITION_REQUEST = contextvars.ContextVar("provider_directory_optum_partition_request", default=None)
HTTP_OUTAGE_STATUSES = frozenset({500, 502, 503, 504})
OUTAGE_SAMPLE_SIZE = 20
OUTAGE_FAILURE_COUNT = 18
OUTAGE_DISTINCT_PARTITIONS = 4
PROBE_DELAYS = (60, 300, 900)
CANCELLATION_POLL_SECONDS = 30


class OptumOutage:
    """Share one bounded observation window and recovery probe across resource families."""

    def __init__(self):
        self.samples = deque(maxlen=OUTAGE_SAMPLE_SIZE)
        self.probe_lock = asyncio.Lock()
        self.epoch = 0
        self.probe_after = None
        self.probe_attempt = 0
        self.http_500_responses = 0
        self.probes = 0

    def observe(self, status, now, epoch):
        """Count backend responses and pause after a sustained cross-partition outage."""
        if status == 500:
            self.http_500_responses += 1
        context = PARTITION_REQUEST.get()
        if context is None or self.probe_after is not None or epoch != self.epoch:
            return
        request, identity = context
        if not identity or "'" in identity or "%27" in identity:
            return
        failure = (request.resource_type, identity) if status in HTTP_OUTAGE_STATUSES else None
        self.samples.append(failure)
        if (
            len(self.samples) < OUTAGE_SAMPLE_SIZE
            or self.samples.count(None) > OUTAGE_SAMPLE_SIZE - OUTAGE_FAILURE_COUNT
        ):
            return
        if len(set(self.samples) - {None}) >= OUTAGE_DISTINCT_PARTITIONS:
            self.epoch += 1
            self.probe_after = now + PROBE_DELAYS[0]

    async def has_recovered_within_deadline(self, directory, probe):
        """Release queued scans only after a fresh valid InsurancePlan search."""
        if self.probe_after is None:
            return True
        context = PARTITION_REQUEST.get()
        options = context[0].fetch_options if context is not None else None
        deadline = options.deadline_at if options is not None else None
        remaining = None if deadline is None else max(0, deadline - directory.time.monotonic())
        if remaining == 0:
            return False
        try:
            async with asyncio.timeout(remaining), self.probe_lock:
                await self._probe_until_ready(directory, probe, options)
        except TimeoutError:
            return False
        return True

    async def _probe_until_ready(self, directory, probe, options):
        while self.probe_after is not None:
            if options is not None:
                await directory._raise_if_resource_import_cancelled(options.cancel_ctx, options.cancel_task)
            delay = self.probe_after - directory.time.monotonic()
            if delay > 0:
                should_poll_cancellation = options is not None and options.cancel_ctx is not None
                await asyncio.sleep(min(delay, CANCELLATION_POLL_SECONDS) if should_poll_cancellation else delay)
                continue
            self.probes += 1
            # Reserve the next boundary before sending so a canceled probe cannot trigger a burst.
            self.probe_attempt = min(self.probe_attempt + 1, len(PROBE_DELAYS) - 1)
            self.probe_after = directory.time.monotonic() + PROBE_DELAYS[self.probe_attempt]
            if _is_healthy_search(await probe()):
                self.probe_after = None
                self.probe_attempt = 0
                self.samples.clear()


def _is_healthy_search(response):
    status, payload, error, _elapsed = response
    if (
        status != 200
        or error
        or not isinstance(payload, dict)
        or payload.get("resourceType") != "Bundle"
        or payload.get("type") != "searchset"
    ):
        return False
    entries = payload.get("entry", [])
    if not isinstance(entries, list):
        return False
    if not entries:
        total = payload.get("total", 0)
        return type(total) is int and total == 0
    if len(entries) != 1 or not isinstance(entries[0], dict):
        return False
    resource = entries[0].get("resource")
    return (
        isinstance(resource, dict)
        and resource.get("resourceType") == "InsurancePlan"
        and isinstance(resource.get("id"), str)
        and bool(resource["id"])
    )
