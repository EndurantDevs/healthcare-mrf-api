# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""FHIR response checks shared by collection imports and access probes."""

import asyncio
import contextvars
import urllib.parse
from contextlib import nullcontext
from typing import Any

SOURCE_HTTP_REQUEST_SLOTS: contextvars.ContextVar[dict | None] = contextvars.ContextVar(
    "provider_directory_http_request_slots", default=None
)


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
                delay = directory._source_retry_after_seconds(response[1])
                if delay is not None:
                    pacing_state["retry_after_monotonic"] = max(
                        pacing_state.get("retry_after_monotonic", 0.0), directory.time.monotonic() + delay
                    )
            return response
