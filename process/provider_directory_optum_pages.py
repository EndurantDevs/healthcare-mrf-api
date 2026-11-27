# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Recover readable slices on the same Optum search handle."""

from dataclasses import dataclass, replace
from urllib.parse import parse_qsl, urlencode, urlsplit

from process.provider_directory_fetch_types import PartitionPageFetchResult
from process.provider_directory_optum_outage import is_page_error

MAX_RECOVERY_REQUESTS = 96
SPLIT_FIRST_PAGE = object()
RECOVERABLE_RESOURCE_TYPES = frozenset(
    {"Location", "Organization", "Practitioner", "OrganizationAffiliation", "PractitionerRole"}
)


@dataclass(frozen=True)
class RecoveredPage:
    """Verified entries and any unreadable offsets in one original page window."""

    batches: list[tuple[str, list[dict]]]
    next_url: str | None
    unreadable_offsets: tuple[int, ...]


@dataclass(frozen=True)
class PageWindow:
    """A bounded window on a previously resolved server-issued continuation."""

    url: str
    start: int
    count: int
    total: int

    def slice_url(self, start, count):
        """Change only the slice coordinates on the original handle."""
        parsed = urlsplit(self.url)
        coordinate_by_name = {"_getpagesoffset": str(start), "_count": str(count)}
        query_items = [
            (name, coordinate_by_name.get(name, value))
            for name, value in parse_qsl(parsed.query, keep_blank_values=True)
        ]
        return parsed._replace(query=urlencode(query_items)).geturl()


def _page_window(url, total):
    if type(total) is not int or not 0 < total < 10_000:
        return None
    query_items = parse_qsl(urlsplit(url).query, keep_blank_values=True)
    number_by_name = {}
    for name in ("_getpages", "_getpagesoffset", "_count"):
        values = [value for key, value in query_items if key == name]
        if len(values) != 1 or not values[0]:
            return None
        if name != "_getpages":
            coordinate = values[0]
            if not coordinate.isascii() or not coordinate.isdecimal() or len(coordinate) > 5:
                return None
            number_by_name[name] = int(coordinate)
    start, count = number_by_name["_getpagesoffset"], number_by_name["_count"]
    return PageWindow(url, start, count, total) if 0 <= start < total and 1 <= count <= 100 else None


def _verified_entries(response, resource_type, count, total, seen_ids):
    status, bundle, error, _elapsed = response
    if status != 200 or error or not isinstance(bundle, dict):
        return None
    if bundle.get("resourceType") != "Bundle" or bundle.get("type") != "searchset":
        return None
    if "total" in bundle and (type(bundle["total"]) is not int or bundle["total"] != total):
        return None
    entries = bundle.get("entry")
    if not isinstance(entries, list) or len(entries) != count:
        return None
    for entry in entries:
        resource = entry.get("resource") if isinstance(entry, dict) else None
        if not isinstance(resource, dict) or resource.get("resourceType") != resource_type:
            return None
        identifier = resource.get("id")
        if not isinstance(identifier, str) or not identifier or identifier in seen_ids:
            return None
        seen_ids.add(identifier)
    return entries


def _halves(start, stop):
    midpoint = start + (stop - start) // 2
    return [(midpoint, stop), (start, midpoint)]


async def recover_page(url, total, resource_type, send):
    """Bisect one failed window without trusting a new search or widening the page."""
    window = _page_window(url, total)
    if window is None:
        return None
    end = min(window.start + window.count, window.total)
    is_singleton = end - window.start == 1
    pending_slices = [(window.start, end)] if is_singleton else _halves(window.start, end)
    recovered_batches = []
    unreadable_offsets = []
    seen_ids = set()
    requests_left = MAX_RECOVERY_REQUESTS
    while pending_slices and requests_left:
        start, stop = pending_slices.pop()
        count = stop - start
        slice_url = window.slice_url(start, count)
        response = await send(slice_url)
        requests_left -= 1
        entries = _verified_entries(response, resource_type, count, window.total, seen_ids)
        if entries is not None:
            recovered_batches.append((slice_url, entries))
            continue
        if not is_page_error(response):
            return None
        if count > 1:
            pending_slices.extend(_halves(start, stop))
            continue
        if not is_singleton:
            if not requests_left or not is_page_error(await send(slice_url)):
                return None
            requests_left -= 1
        unreadable_offsets.append(start)
    if pending_slices or (not recovered_batches and not is_singleton):
        return None
    next_url = window.slice_url(end, window.count) if end < window.total else None
    return RecoveredPage(recovered_batches, next_url, tuple(unreadable_offsets))


def _summary_count_url(url):
    parsed = urlsplit(url)
    query_items = [
        (name, value)
        for name, value in parse_qsl(parsed.query, keep_blank_values=True)
        if name not in {"_count", "_summary"}
    ]
    query_items.append(("_summary", "count"))
    return parsed._replace(query=urlencode(query_items)).geturl()


async def _recover_first_page(directory, request, send):
    """Verify a single-row partition whose only record fails page-1 serialization."""
    if request.current_url != request.partition_start_url or directory._has_fhir_continuation_query(
        request.current_url
    ):
        return None
    summary_status, summary_bundle, summary_error, _elapsed = await send(_summary_count_url(request.current_url))
    if (
        summary_status != 200
        or summary_error
        or not isinstance(summary_bundle, dict)
        or summary_bundle.get("resourceType") != "Bundle"
        or summary_bundle.get("type") != "searchset"
    ):
        return None
    if directory._bundle_total(summary_bundle) != 1:
        return None
    single_url = directory._url_with_replaced_query_item(request.current_url, "_count", "1")
    if not is_page_error(await send(single_url)):
        return None
    return RecoveredPage([], None, (0,))


def split_unreadable_first_page(directory, request, fetch_result):
    """Split a page-1 serialization failure into child prefixes to recover sibling records."""
    if (
        request.resource_type not in RECOVERABLE_RESOURCE_TYPES
        or request.resource_type == "Location"
        or request.is_residual_only
        or request.current_url != request.partition_start_url
        or directory._has_fhir_continuation_query(request.current_url)
        or not directory.uhc_scan.is_source(directory, request.source_record)
        or not is_page_error(fetch_result)
    ):
        return False
    query_by_name = dict(parse_qsl(urlsplit(request.current_url).query, keep_blank_values=True))
    search_param = {
        "Practitioner": "family",
        "Organization": "name",
        **directory.uhc_scan.POSTAL_SEARCH_PARAMS,
    }.get(request.resource_type)
    prefix = directory._uhc_query_prefix(query_by_name, search_param or "")
    if not search_param or not prefix:
        return False
    if request.resource_type in directory.uhc_scan.POSTAL_SEARCH_PARAMS:
        if len(prefix) >= 5 or not prefix.isascii() or not prefix.isdigit():
            return False
        child_prefixes = directory.uhc_scan.postal_child_prefixes(prefix)
    else:
        max_prefix_length = directory._env_int(
            "HLTHPRT_PROVIDER_DIRECTORY_UHC_MAX_PREFIX_LENGTH",
            directory.UHC_ADAPTIVE_PARTITION_DEFAULT_MAX_PREFIX_LENGTH,
        )
        if len(prefix) >= max_prefix_length:
            return False
        child_prefixes = [prefix + character for character in directory.UHC_TEXT_SEARCH_ALPHABET]
    child_urls = [
        directory._url_with_replaced_query_item(request.current_url, search_param, child_prefix)
        for child_prefix in child_prefixes
    ]
    queue = request.start_url_queue
    ordered_urls = reversed(child_urls) if isinstance(queue, directory.asyncio.LifoQueue) else child_urls
    for child_url in ordered_urls:
        child_identity = directory._uhc_partition_identity(request.source_record, request.resource_type, child_url)
        if child_identity not in request.state.completed_partition_identities:
            queue.put_nowait(child_url)
    return bool(child_urls)


async def recover_partition_page(directory, request, fetch_result):
    """Read bounded subpages through the original source request admission."""
    if (
        request.resource_type not in RECOVERABLE_RESOURCE_TYPES
        or request.is_residual_only
        or not directory.uhc_scan.is_source(directory, request.source_record)
        or not is_page_error(fetch_result)
    ):
        return None

    async def send(url):
        """Observe the chain deadline and cancellation before an admitted read."""
        options = request.fetch_options
        if options.cancel_ctx is not None:
            await directory._raise_if_resource_import_cancelled(options.cancel_ctx, options.cancel_task)
        if options.deadline_at is not None and directory.time.monotonic() >= options.deadline_at:
            return None, None, "deadline_reached", 0
        return await directory.fhir_response.fetch_partition_response(directory, replace(request, current_url=url))

    if request.current_url == request.partition_start_url:
        recovered = await _recover_first_page(directory, request, send)
        if recovered is None and split_unreadable_first_page(directory, request, fetch_result):
            return SPLIT_FIRST_PAGE
    else:
        recovered = await recover_page(request.current_url, request.expected_total, request.resource_type, send)
    if recovered is not None:
        await directory._mark_partition_page_fetched(request.state)
    return recovered


async def consume_recovered_page(directory, request, recovered):
    """Keep readable rows and visit later pages without completing a leaf with gaps."""
    predicate_error = directory._uhc_location_partition_predicate_error(
        request.source_record,
        request.resource_type,
        request.partition_start_url,
        {"entry": [entry for _, entries in recovered.batches for entry in entries]},
    )
    if predicate_error:
        await directory._mark_partition_fetch_error(request.state, predicate_error)
        return PartitionPageFetchResult(next_url=None, outcome="failed")
    state = request.state
    unreadable_slot_count = len(recovered.unreadable_offsets)
    if unreadable_slot_count:
        request.unreadable_page_counts.append(unreadable_slot_count)
        async with state.state_lock:
            state.unreadable_optum_slot_count += unreadable_slot_count
            state.unreadable_optum_page_count += 1
            if len(state.unreadable_optum_pages) < 25:
                state.unreadable_optum_pages.append(
                    {
                        "partition_url_hash": directory._pagination_url_hash(request.partition_start_url),
                        "offsets": recovered.unreadable_offsets,
                    }
                )
        await directory._mark_partition_fetch_error(state, "optum_unreadable_search_slots")
    fetched_rows = 0
    for slice_url, entries in recovered.batches:
        fetched_rows += await directory._consume_partition_entries(
            state,
            request.source_record,
            request.resource_type,
            entries,
            slice_url,
            request.pending_resource_rows,
            request.fetch_options,
        )
    page_result = await directory._partition_page_next_result(
        request.source_record, request.current_url, recovered.next_url, state, bool(unreadable_slot_count)
    )
    return replace(page_result, rows_fetched=fetched_rows, unreadable_slot_count=unreadable_slot_count)


def unreadable_diagnostic(state):
    """Expose bounded unreadable positions without recording the server's search handle."""
    if not state.unreadable_optum_slot_count:
        return None
    return {
        "unreadable_optum_slot_count": state.unreadable_optum_slot_count,
        "unreadable_optum_page_count": state.unreadable_optum_page_count,
        "unreadable_optum_pages": state.unreadable_optum_pages,
    }


def has_only_unreadable_slots(diagnostic):
    """Allow checking known references while leaving a fully visited gapped scan incomplete."""
    evidence = diagnostic.get("source_fetch")
    if not isinstance(evidence, dict) or diagnostic.get("next_url_remaining") is not False:
        return False
    page_count = evidence.get("unreadable_optum_page_count")
    slot_count = evidence.get("unreadable_optum_slot_count")
    return (
        type(page_count) is int
        and type(slot_count) is int
        and slot_count >= page_count > 0
        and diagnostic.get("error") == f"partition_errors_{page_count}_last_optum_unreadable_search_slots"
    )
