# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Traverse issued search caches without trusting the gateway's page totals."""

import asyncio
import re
from dataclasses import dataclass, field, replace
from types import SimpleNamespace
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit

API_BASE = "https://api.bluecrossmn.com/fhir/provider"
CACHED_RESOURCES = frozenset({"InsurancePlan", "Location", "HealthcareService"})
STRATEGY_VERSION = "provider-directory-fhir-minnesota-time-cache-v4"
CACHE_TOTAL_KEY = "minnesota_cache_expected_total"
PARTITION_HINTS_PARAM = "provider_directory_partition_hints"
INITIAL_PAGE_COUNT = 50
PAGE_COUNT_CAP = 500
DIRECT_VERIFICATION_LIMIT = 16
ORGANIZATION_REFERENCE_LIMIT = 256
ORGANIZATION_RECOVERY_LIMIT = 128
_QUERY_NAMES = frozenset(
    {"_count", "_getpages", "_getpagesoffset", "_pretty", "_bundletype", "_summary", "_total", "_format"}
)


def require_partition_hint_task(task, *, source_storage):
    """Accept geometry only for a scoped complete acquisition."""
    if PARTITION_HINTS_PARAM not in task:
        return
    if not (
        source_storage
        and task.get("provider_directory_endpoint_scope") == API_BASE
        and task.get("full_refresh") is True
        and task.get("import_resources") is True
        and task.get("allow_retained_pagination_reset") is False
        and not any(task.get(field) for field in ("seed_only", "bulk_export", "resource_limit", "page_limit"))
    ):
        raise ValueError("provider_directory_partition_hints_scope_invalid")


def with_partition_hints(directory, task, source_rows, resources):
    """Apply per-run split geometry after catalog persistence and endpoint selection."""
    if PARTITION_HINTS_PARAM not in task:
        return source_rows
    if (
        len(source_rows) != 1
        or task.get("source_ids") != [source_rows[0].get("source_id")]
        or directory._canonical_base(source_rows[0].get("canonical_api_base") or source_rows[0].get("api_base"))
        != API_BASE
        or set(resources) != set(directory.PUBLIC_DIRECTORY_SEVEN_RESOURCES)
    ):
        raise ValueError("provider_directory_partition_hints_scope_invalid")
    source = source_rows[0]
    hints = task[PARTITION_HINTS_PARAM]
    configured = directory._last_updated_partition_resource_configs(source)
    if not isinstance(hints, dict) or not hints or not hints.keys() <= configured.keys():
        raise ValueError("provider_directory_partition_hints_invalid")
    for resource_type, geometry in hints.items():
        if not isinstance(geometry, dict) or geometry.keys() != {"end", "planned_splits"}:
            raise ValueError("provider_directory_partition_hints_invalid")
        settings_by_name = {**configured[resource_type], **geometry}
        if not directory._build_partition_config(settings_by_name).planned_splits:
            raise ValueError("provider_directory_partition_hints_invalid")
        configured[resource_type] = settings_by_name
    metadata = directory._source_metadata(source)
    config_key = directory.LAST_UPDATED_PARTITION_METADATA_KEY
    partition_by_field = {**metadata[config_key], "resources": configured}
    return [{**source, "metadata_json": {**metadata, config_key: partition_by_field}}]


def _organization_proof_ids(directory, window_id, references, fingerprints):
    """Resolve distinct source IDs whose retained observation and raw hash both match."""
    fingerprint_by_id = {}
    for occurrence, fingerprint in fingerprints.items():
        matches = [
            reference
            for reference in references
            if directory._partition_occurrence_id(window_id, reference, fingerprint, 0) == occurrence
        ]
        if len(matches) != 1 or matches[0] in fingerprint_by_id:
            return {}
        fingerprint_by_id[matches[0]] = fingerprint
    return fingerprint_by_id


@dataclass
class OrganizationReferenceRecovery:
    references: tuple[str, ...] | None = None
    new_window_ids: dict[str, tuple[str, ...]] | None = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


def _empty_gateway_timeout(fetched):
    return (
        fetched.transient
        and fetched.error == "http_504"
        and (fetched.pages_fetched == 1 or fetched.first_page_gateway_timeout)
        and not fetched.resources
    )


async def split_timed_out_window(directory, source_record, resource_type, config, state, window, fetched):
    """Keep exact counts while reducing a gateway-limited data request."""
    if not (
        directory._canonical_base(source_record.get("canonical_api_base") or source_record.get("api_base")) == API_BASE
        and _empty_gateway_timeout(fetched)
        and state.plan.split_acquisition_window(window.window_id)
    ):
        return None
    state.pages_fetched += fetched.pages_fetched
    await directory._save_last_updated_partition_plan(
        state.context,
        resource_type,
        config,
        state.plan,
        census=state.census,
        pages_processed=state.pages_fetched,
        rows_processed=state.rows_fetched,
    )
    return window


async def verify_timed_out_practitioner_window(
    directory, source_record, resource_type, config, state, options, window, fetched
):
    """Verify a small retained second pass when the range search times out."""
    if not (
        resource_type == "Practitioner"
        and directory._canonical_base(source_record.get("canonical_api_base") or source_record.get("api_base"))
        == API_BASE
        and _empty_gateway_timeout(fetched)
        and set(window.passes) == {1}
        and window.count is not None
        and 0 < window.count <= DIRECT_VERIFICATION_LIMIT
    ):
        return fetched
    # The first pass binds the identities. An independent count proves that
    # those identities can still account for the entire window.
    proof = await directory._load_last_updated_partition_window_fingerprints(
        state.context, resource_type, window.window_id, 1
    )
    if len(proof) != window.count or not all(
        key not in {".", ".."} and re.fullmatch(r"[A-Za-z0-9.-]{1,64}", key) for key in proof
    ):
        return replace(fetched, error="window_direct_proof_invalid", transient=False, retry_not_before=None)
    pages, failure = await _recount_direct_window(directory, source_record, config, state, options, window, fetched)
    if failure is not None:
        return failure
    return await _read_practitioner_window(directory, source_record, state, options, window, sorted(proof), pages)


async def _recount_direct_window(directory, source_record, config, state, options, window, fetched):
    count_url = directory._last_updated_partition_url(
        state.start_url, window, page_count=config.page_count, count_only=True
    )
    count = await directory._fetch_last_updated_partition_count(
        source_record, count_url, timeout=options.timeout, deadline_at=state.deadline_at
    )
    pages = fetched.pages_fetched + count.pages_fetched
    if count.transient:
        return pages, replace(
            fetched,
            pages_fetched=pages,
            first_page_gateway_timeout=True,
            retry_not_before=count.retry_not_before or fetched.retry_not_before,
        )
    if count.observation != directory.CountObservation.exact(window.count):
        return pages, directory.LastUpdatedWindowFetch((), pages, False, error="window_direct_count_mismatch")
    return pages, None


async def _direct_request(directory, source_record, state, options, url, pages, *, optional=False):
    if options.cancel_ctx is not None:
        await directory._raise_if_resource_import_cancelled(options.cancel_ctx, options.cancel_task)
    response, deadline_blocked = await directory._fetch_cooldown_checkpoint_page(
        source_record, url, (None, None, "TimeoutError", 0), timeout=options.timeout, deadline_at=state.deadline_at
    )
    status, payload, error, _elapsed = response
    pages += int(not deadline_blocked)
    if status == 404 and error is None and optional:
        return None, pages, None
    if status != 200 or error:
        failure = directory._last_updated_window_page_failure(source_record, url, status, payload, error)
        return (
            None,
            pages,
            directory.LastUpdatedWindowFetch(
                (),
                pages,
                False,
                error=failure.error,
                transient=failure.transient,
                deadline_reached=deadline_blocked,
                retry_not_before=failure.retry_not_before,
            ),
        )
    if not isinstance(payload, dict):
        return None, pages, directory.LastUpdatedWindowFetch((), pages, False, error="window_direct_resource_mismatch")
    return payload, pages, None


async def _read_practitioner_window(directory, source_record, state, options, window, resource_ids, pages):
    """Read exact IDs through the shared pacing and deadline checks."""
    resources = []
    for resource_id in resource_ids:
        url = f"{API_BASE}/Practitioner/{resource_id}"
        resource_by_field, pages, failure = await _direct_request(directory, source_record, state, options, url, pages)
        if failure is not None:
            return failure
        if (
            not isinstance(resource_by_field, dict)
            or resource_by_field.get("resourceType") != "Practitioner"
            or resource_by_field.get("id") != resource_id
            or directory._last_updated_partition_resource_window_error(resource_by_field, window)
        ):
            return directory.LastUpdatedWindowFetch((), pages, False, error="window_direct_resource_mismatch")
        resources.append(resource_by_field)
    return directory.LastUpdatedWindowFetch(tuple(resources), pages, True)


async def verify_timed_out_organization_window(
    directory, source_record, resource_type, config, state, options, window, fetched
):
    """Recover a bounded tail using scoped references and the original partition proof."""
    if not (
        directory._uses_v4_organization_partition(source_record, resource_type)
        and directory._canonical_base(source_record.get("canonical_api_base") or source_record.get("api_base"))
        == API_BASE
        and _empty_gateway_timeout(fetched)
        and (window.count or 0) > 0
    ):
        return fetched
    from process.provider_directory_generic_source_adapters import source_organization_reference_ids
    from process.provider_directory_generic_source_policy import is_source_storage_required

    pending_windows = [leaf for leaf in state.plan.leaf_windows() if len(leaf.passes) < 2]
    if (
        not pending_windows
        or any(leaf.count is None for leaf in pending_windows)
        or sum(leaf.count for leaf in pending_windows) > ORGANIZATION_RECOVERY_LIMIT
        or not is_source_storage_required()
    ):
        return fetched
    if state.organization_recovery is None:
        state.organization_recovery = OrganizationReferenceRecovery()
    recovery = state.organization_recovery
    async with recovery.lock:
        pages, failure = await _recount_direct_window(directory, source_record, config, state, options, window, fetched)
        if failure is not None:
            return failure
        if recovery.references is None:
            recovery.references = await source_organization_reference_ids(
                directory, state.context.dataset_id, API_BASE, ORGANIZATION_REFERENCE_LIMIT
            )
        if window.passes:
            proof = await directory._load_partition_pass_proof(state.context, resource_type, window.window_id, 1)
            ids = _organization_proof_ids(
                directory, window.window_id, recovery.references, proof.source_fingerprints_by_id
            )
        else:
            if recovery.new_window_ids is None:
                recovery.new_window_ids, pages, failure = await _discover_organization_window_ids(
                    directory, source_record, state, options, recovery.references, pending_windows, pages
                )
                if failure is not None:
                    return failure
            ids = dict.fromkeys(recovery.new_window_ids.get(window.window_id, ()))
        if len(ids) != window.count:
            return replace(fetched, pages_fetched=pages, first_page_gateway_timeout=True)
        return await _read_organization_window(directory, source_record, state, options, window, ids, pages)


async def _discover_organization_window_ids(
    directory, source_record, state, options, references, pending_windows, pages
):
    # Discovery reuses IDs across windows. Every pass still fetches its own payload.
    targets = [window for window in pending_windows if not window.passes and window.count]
    ids_by_window = {window.window_id: [] for window in targets}
    for resource_id in references:
        url = f"{API_BASE}/Organization/{resource_id}"
        resource, pages, failure = await _direct_request(
            directory, source_record, state, options, url, pages, optional=True
        )
        if failure is not None:
            return None, pages, failure
        if resource is None:
            continue
        if not _matches_organization_id(resource, resource_id):
            return (
                None,
                pages,
                directory.LastUpdatedWindowFetch((), pages, False, error="window_direct_resource_mismatch"),
            )
        for window in targets:
            error = directory._last_updated_partition_resource_window_error(resource, window)
            if error is None:
                ids_by_window[window.window_id].append(resource_id)
                break
            if error != "resource_last_updated_outside_window":
                return None, pages, directory.LastUpdatedWindowFetch((), pages, False, error=error)
    return {window_id: tuple(window_ids) for window_id, window_ids in ids_by_window.items()}, pages, None


def _matches_organization_id(resource, resource_id):
    return (
        isinstance(resource, dict)
        and resource.get("resourceType") == "Organization"
        and resource.get("id") == resource_id
    )


def _organization_matches_window(directory, resource, resource_id, window, fingerprint):
    return (
        _matches_organization_id(resource, resource_id)
        and directory._last_updated_partition_resource_window_error(resource, window) is None
        and (
            fingerprint is None
            or directory.fingerprint_resource(resource, directory.V4_ORGANIZATION_PARTITION_VOLATILE_PATHS)
            == fingerprint
        )
    )


def _single_search_resource(bundle):
    if not isinstance(bundle, dict) or bundle.get("resourceType") != "Bundle" or bundle.get("type") != "searchset":
        return None
    links = bundle.get("link", [])
    entries = bundle.get("entry")
    if (
        type(bundle.get("total", 1)) is not int
        or bundle.get("total", 1) != 1
        or not isinstance(links, list)
        or any(not isinstance(link, dict) or link.get("relation") == "next" for link in links)
        or not isinstance(entries, list)
        or len(entries) != 1
        or not isinstance(entries[0], dict)
    ):
        return None
    search_metadata = entries[0].get("search", {})
    if not isinstance(search_metadata, dict) or search_metadata.get("mode") not in (None, "match"):
        return None
    return entries[0].get("resource")


async def _read_organization_window(directory, source_record, state, options, window, ids, pages):
    resources = []
    for resource_id, fingerprint in sorted(ids.items()):
        resource = None
        if fingerprint is not None:
            url = f"{API_BASE}/Organization/{resource_id}"
            resource, pages, failure = await _direct_request(
                directory, source_record, state, options, url, pages, optional=True
            )
            if failure is not None:
                return failure
        if not _organization_matches_window(directory, resource, resource_id, window, fingerprint):
            # A direct representation can differ from the resource returned by search.
            query = urlencode({"_id": resource_id, "_count": 2})
            bundle, pages, failure = await _direct_request(
                directory, source_record, state, options, f"{API_BASE}/Organization?{query}", pages
            )
            if failure is not None:
                return failure
            resource = _single_search_resource(bundle)
            if not _organization_matches_window(directory, resource, resource_id, window, fingerprint):
                return directory.LastUpdatedWindowFetch((), pages, False, error="window_direct_resource_mismatch")
        resources.append(resource)
    return directory.LastUpdatedWindowFetch(tuple(resources), pages, True)


def _query(url, resource):
    parsed = urlsplit(url)
    pairs = parse_qsl(parsed.query, keep_blank_values=True)
    query_by_name = dict(pairs)
    if (
        parsed.scheme != "https"
        or parsed.netloc != urlsplit(API_BASE).netloc
        or parsed.path not in {urlsplit(API_BASE).path, urlsplit(API_BASE).path + "/" + resource}
        or parsed.fragment
        or len(pairs) != len(query_by_name)
        or set(query_by_name) - _QUERY_NAMES
    ):
        raise ValueError("minnesota_cache_url_invalid")
    return query_by_name


def start_url(directory, url):
    """Zero requests the native first page, including its search-cache handle."""
    if urlsplit(url).path.rsplit("/", 1)[-1] in CACHED_RESOURCES:
        return directory._url_with_replaced_query_item(url, "_count", "0")
    return url


def page_error(bundle_payload, resource, current_url, expected_total=None):
    """Refuse malformed pages and evidence contradicting cached completion."""
    if expected_total is not None and (type(expected_total) is not int or expected_total < 0):
        return "minnesota_cache_expected_total_invalid"
    total = bundle_payload.get("total")
    if total is not None and expected_total is not None and total != expected_total:
        return "minnesota_cache_total_changed"
    total = expected_total if total is None else total
    links = bundle_payload.get("link", [])
    if not isinstance(links, list) or any(not isinstance(link, dict) for link in links):
        return "minnesota_cache_links_invalid"
    next_links = [link.get("url") for link in links if link.get("relation") == "next"]
    if len(next_links) > 1 or any(not isinstance(url, str) or not url.strip() for url in next_links):
        return "minnesota_cache_links_invalid"
    entries = bundle_payload.get("entry", [])
    if not isinstance(entries, list) or any(
        not isinstance(entry, dict)
        or not isinstance(entry.get("resource"), dict)
        or entry["resource"].get("resourceType") != resource
        or not isinstance(entry["resource"].get("id"), str)
        or not entry["resource"]["id"]
        for entry in entries
    ):
        return "minnesota_cache_resource_invalid"
    try:
        query = _query(current_url, resource)
        is_initial = query.get("_count") == "0" and "_getpages" not in query
        if "_getpages" in query or is_initial:
            offset, count = (
                (0, INITIAL_PAGE_COUNT) if is_initial else (int(query["_getpagesoffset"]), int(query["_count"]))
            )
            if total is not None:
                if type(total) is not int or total < 0:
                    return "minnesota_cache_total_invalid"
                processed = offset + len(entries)
                if total < processed:
                    return "minnesota_cache_total_below_processed"
                if len(entries) < count and not next_links and total > processed:
                    return "minnesota_cache_short_page_before_total"
    except (KeyError, ValueError) as exc:
        return "minnesota_cache_page_invalid:" + str(exc)
    bundle_type = bundle_payload.get("type")
    if bundle_type != "searchset" and (is_initial or bundle_type is not None):
        return "minnesota_cache_non_searchset_bundle"
    # The gateway omits cached page types; empty pages still need terminal accounting.
    if not entries and bundle_type is None and total is None:
        return "minnesota_cache_ambiguous_empty_page"
    if is_initial and not entries and bundle_payload.get("total") != 0:
        return "minnesota_cache_initial_page_missing"
    return None


async def completion_error(
    directory, context, resource, expected_total, rows_processed, retained_rows=None, *, source_record=None, run_id=None
):
    """Reconcile fetched rows and any advertised total with distinct stored IDs."""
    if expected_total is not None and rows_processed != expected_total:
        return "minnesota_cache_unique_candidate_total_mismatch"
    if (
        context is None
        and retained_rows is None
        and source_record
        and source_record.get("_endpoint_dataset_id")
        and source_record.get("_endpoint_id")
    ):
        # Candidates without pagination use the current run, including retries.
        context = SimpleNamespace(
            dataset_id=source_record["_endpoint_dataset_id"],
            endpoint_id=source_record["_endpoint_id"],
            acquisition_root_run_id=run_id,
            source_ids=tuple(source_record.get("_partition_checkpoint_source_ids") or (source_record["source_id"],)),
        )
    if context is not None:
        unique_count = await directory._caresource_unique_candidate_count(context, resource)
    elif retained_rows is not None:
        unique_count = len({resource_row["resource_id"] for resource_row in retained_rows})
    else:
        return "minnesota_cache_candidate_context_missing"
    if unique_count != rows_processed:
        return "minnesota_cache_unique_candidate_total_mismatch"
    return None


def resume_error(resume_state):
    """Retain known population bounds through interruptions and completion reuse."""
    if resume_state is None:
        return None
    total = resume_state.completeness.get(CACHE_TOTAL_KEY)
    if total is None:
        return None
    if type(total) is not int or total < 0:
        return "minnesota_cache_expected_total_invalid"
    if resume_state.rows_processed > total or (resume_state.complete and resume_state.rows_processed != total):
        return "minnesota_cache_checkpoint_total_mismatch"
    return None


def next_url(current_url, next_link, resource, page_count, entry_count):
    """Advance only a server-issued cache, using the verified 500-row cap."""
    if (
        resource not in CACHED_RESOURCES
        or type(entry_count) is not int
        or type(page_count) is not int
        or not 0 < page_count <= PAGE_COUNT_CAP
    ):
        raise ValueError("minnesota_cache_page_invalid")
    current = _query(current_url, resource)
    if "_getpages" not in current:
        if current.get("_count") != "0" or "_getpagesoffset" in current or not 0 <= entry_count <= INITIAL_PAGE_COUNT:
            raise ValueError("minnesota_cache_initial_page_invalid")
        if not next_link:
            if entry_count == INITIAL_PAGE_COUNT:
                raise ValueError("minnesota_cache_handle_missing")
            return None
        issued = _query(urljoin(current_url, next_link), resource)
        cache = issued.get("_getpages")
        if issued.get("_getpagesoffset") != str(entry_count) or issued.get("_count") != str(INITIAL_PAGE_COUNT):
            raise ValueError("minnesota_cache_initial_cursor_invalid")
        offset = entry_count
    else:
        cache = current["_getpages"]
        count = current.get("_count", "")
        offset = current.get("_getpagesoffset", "")
        if not count.isascii() or not count.isdigit() or not 0 < int(count) <= PAGE_COUNT_CAP:
            raise ValueError("minnesota_cache_count_invalid")
        if not offset.isascii() or not offset.isdigit() or not 0 <= entry_count <= int(count):
            raise ValueError("minnesota_cache_offset_invalid")
        offset = int(offset) + entry_count
        if next_link:
            issued = _query(urljoin(current_url, next_link), resource)
            # The gateway can append an extra -H to each continuation value.
            if issued.get("_getpages") not in {cache, cache + "-H"} or issued.get("_getpagesoffset", "").removesuffix(
                "-H"
            ) != str(offset):
                raise ValueError("minnesota_cache_cursor_changed")
        elif entry_count < int(count):
            return None
    if (
        not cache
        or len(cache) > 1024
        or not all(character.isascii() and (character.isalnum() or character == "-") for character in cache)
    ):
        raise ValueError("minnesota_cache_handle_invalid")
    if not entry_count:
        raise ValueError("minnesota_cache_empty_continuation")
    return (
        API_BASE
        + "?"
        + urlencode({"_getpages": cache, "_getpagesoffset": offset, "_count": page_count, "_bundletype": "searchset"})
    )
