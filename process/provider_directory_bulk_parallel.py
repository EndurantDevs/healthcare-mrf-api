# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bound independent bulk transfers without sharing physical Source sessions."""

import asyncio
from contextlib import asynccontextmanager, nullcontext

from process.provider_directory_fhir_response import AdaptiveRequestSlots


def output_concurrency(directory, source_record):
    """Return only the existing validator-bound output concurrency setting."""
    if not directory._is_bulk_range_resume_enabled(source_record):
        return directory.BULK_EXPORT_DEFAULT_OUTPUT_CONCURRENCY
    configured = directory._source_metadata(source_record).get(
        "provider_directory_bulk_export_output_concurrency", directory.BULK_EXPORT_DEFAULT_OUTPUT_CONCURRENCY
    )
    if isinstance(configured, bool):
        raise ValueError("bulk_export_output_concurrency_invalid")
    try:
        concurrency = int(configured)
    except (TypeError, ValueError) as exc:
        raise ValueError("bulk_export_output_concurrency_invalid") from exc
    if not 1 <= concurrency <= directory.BULK_EXPORT_MAX_OUTPUT_CONCURRENCY:
        raise ValueError("bulk_export_output_concurrency_invalid")
    return concurrency


def state(directory, source_record):
    """Retain run-local transfer and writer limits across shallow resource copies."""
    if "_provider_directory_bulk_parallel" not in source_record:
        source_record["_provider_directory_bulk_parallel"] = {
            "requests": AdaptiveRequestSlots(output_concurrency(directory, source_record)),
            "writer": asyncio.Lock(),
        }
    return source_record["_provider_directory_bulk_parallel"]


def prepare_scan(directory, source_record, concurrency):
    """Create run-local limits before the resource scheduler copies its input."""
    shared = state(directory, source_record)
    if "resources" not in shared:
        shared["requests"].limit = max(concurrency, shared["requests"].limit)
        shared["resources"] = AdaptiveRequestSlots(concurrency)


def resource_slots(source_record, concurrency):
    """Reuse the adaptive export allowance for a bulk resource wave."""
    shared = source_record.get("_provider_directory_bulk_parallel", {})
    return shared.get("resources") or asyncio.Semaphore(concurrency)


def write_slot(directory, source_record):
    """Reserve Source capacity before opening an atomic output batch."""
    # One atomic payload/checkpoint batch leaves the second Source session
    # available for ownership observations. Never retain a session over HTTP.
    return state(directory, source_record)["writer"] if directory.is_source_storage_required() else nullcontext()


def _throttle(directory, source_record, shared, response):
    headers_by_name = {str(key).lower(): str(value) for key, value in response.headers.items()}
    delay = directory._source_retry_after_seconds(
        {directory.SOURCE_RETRY_AFTER_FIELD: headers_by_name.get("retry-after")}
    )
    pacing = source_record.setdefault("_provider_directory_request_pacing_state", {})
    now = directory.time.monotonic()
    deadline = pacing.get("retry_after_monotonic", 0)
    if deadline <= now:
        shared["requests"].reduce()
        if "resources" in shared:
            shared["resources"].reduce()
    pacing["retry_after_monotonic"] = max(deadline, now + (delay if delay is not None else 5))


@asynccontextmanager
async def _paced_response(directory, source_record, session, url, shared, options):
    """Hold the transfer slot through response consumption and cancellation."""
    while True:
        async with shared["requests"] as slots:
            await directory._pace_source_request(source_record)
            if slots.active > slots.limit:
                continue
            try:
                async with session.get(url, **options) as response:
                    yield response
                    return
            except TimeoutError:
                slots.reduce()
                raise


@asynccontextmanager
async def request(directory, source_record, session, url, *, export_start=False, **options):
    """Share one adaptive transfer budget; retry only rejected, unread responses."""
    shared = state(directory, source_record)
    attempts = 0
    while True:
        async with _paced_response(directory, source_record, session, url, shared, options) as response:
            is_limited = response.status in {429, 503, 504}
            if is_limited:
                _throttle(directory, source_record, shared, response)
            # An export start can have an unknown acceptance outcome.
            # Retry only an explicit rate-limit rejection for that GET.
            is_retryable = is_limited and (not export_start or response.status == 429)
            if is_retryable and attempts < 2:
                attempts += 1
                continue
            yield response
            return


def is_bulk_scan(directory, source_record, options):
    """Apply the existing effective bulk selection to this resource scan."""
    return directory._is_source_bulk_export_effective(
        source_record,
        options.bulk_export,
        per_resource_limit=options.per_resource_limit,
        checkpoint_context=source_record.get("_pagination_checkpoint_context"),
    )


def has_parallel_transport(directory, source_record, resource_types):
    """Require independently addressed exports with immutable output resume."""
    base = directory._canonical_base(source_record.get("canonical_api_base") or source_record.get("api_base"))
    return bool(
        base
        and directory._is_bulk_range_resume_enabled(source_record)
        and resource_types
        and all(
            (url := directory._bulk_export_start_url(source_record, resource))
            and directory._is_safe_bulk_https_url(url)
            and directory._is_same_bulk_url_origin(url, base)
            for resource in resource_types
        )
    )


def effective_resource_concurrency(directory, source_record, options):
    """Keep the existing durable-scan contract and reserve ordinary guard capacity."""
    requested = min(
        directory._validated_resource_scan_concurrency(options.requested_concurrency),
        directory._source_resource_scan_concurrency(source_record),
    )
    is_bulk = is_bulk_scan(directory, source_record, options)
    if is_bulk:
        # Each export holds one ordinary advisory-lock backend until it drains.
        requested = min(requested, max(1, directory._provider_directory_database_pool_capacity() - 1))
    base = directory._canonical_base(source_record.get("canonical_api_base") or source_record.get("api_base"))
    checkpoint = source_record.get("_pagination_checkpoint_context")
    resources = directory.resource_scan_types(directory, source_record, options.resource_types, options)
    is_safe = bool(
        requested > 1
        and directory.current_version_census_contract(source_record) is None
        and directory._has_bound_resource_scan_checkpoint(source_record, checkpoint)
        and checkpoint.canonical_api_base == base
        and len(options.resource_types) == len(set(options.resource_types))
        and options.per_resource_limit <= 0
        and options.page_limit <= 0
        and options.page_count > 0
        and options.stream_batch_size > 0
        and options.source_concurrency == 1
        and options.linked_resource_limit <= 0
        and not options.stale_cleanup
        and options.checkpointing_enabled
        and options.deferred_materialization
        and not directory._is_practitioner_role_reverse_lookup_planned(source_record, list(options.resource_types))
        and (
            directory.is_source_storage_required()
            or directory._provider_directory_database_pool_capacity() >= requested + 1
        )
        and (
            has_parallel_transport(directory, source_record, resources)
            if is_bulk
            else directory._has_parallel_resource_transport(source_record, resources, options.page_count)
        )
    )
    if not is_safe:
        return 1
    try:
        if directory._bound_upsert_session() is not None:
            return 1
    except RuntimeError:
        return 1
    return min(requested, len(resources))
