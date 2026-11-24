# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Postal ZIP+4 descent, transport shaping and reference closure for the capped UHC FHIR search."""

import asyncio
import re
import urllib.parse
from contextlib import aclosing
from functools import partial
from itertools import batched, islice

from db.plan_release_storage import storage_relation
from process import provider_directory_source_proxy as source_proxy

POSTAL_SEARCH_PARAMS = {
    "PractitionerRole": "location.address-postalcode",
    "OrganizationAffiliation": "location.address-postalcode",
    "Location": "address-postalcode",
}
BEST_EFFORT_TYPES = frozenset({"Practitioner", "Organization"})
BARE_ZIP_ERROR = "uhc_bare_zip_postal_codes_unverified"
REFERENCE_CLOSURE_ERROR = "uhc_reference_closure_incomplete"
REFERENCE_SOURCE_TYPES = ("InsurancePlan", "PractitionerRole", "OrganizationAffiliation")
REFERENCE_TARGET_TYPES = ("Practitioner", "Organization", "Location")
REFERENCE_BATCH_SIZE = {"Practitioner": 15, "Location": 10}
ROLE_LINK_FIELDS = {"Practitioner": "practitioner_ref", "Organization": "organization_ref", "Location": "location_refs"}
RETRY_ROUNDS = 2
RETRY_DELAY_SECONDS = 2.0
REFERENCE_FLUSH_ROWS = 500
_DIGITS = "0123456789"
_ZIP5 = re.compile(r"[0-9]{5}")
_PARTIAL_ZIP4 = re.compile(r"[0-9]{5}-[0-9]{1,3}")


def is_source(directory, source_record):
    """Match the public Optum Flex base, which caps every search at 10,000 rows."""
    base = source_record.get("canonical_api_base") or source_record.get("api_base")
    return directory._canonical_base(base) == directory.UHC_PROVIDER_DIRECTORY_BASE


def postal_child_prefixes(prefix):
    """Split a capped prefix: digits to five, then `<zip5>-<digit>`, then digits to the full ZIP+4."""
    if _ZIP5.fullmatch(prefix):
        return [f"{prefix}-{digit}" for digit in _DIGITS]
    if len(prefix) < 5 or _PARTIAL_ZIP4.fullmatch(prefix):
        return [prefix + digit for digit in _DIGITS]
    return []


async def location_start_urls(directory, source_record, page_count):
    """Search only five-digit ZIPs from the existing geographic reference inventory."""
    start_url = directory._resource_start_url(source_record, "Location", page_count=page_count)
    if not start_url:
        return []
    query_by_name = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(start_url).query, keep_blank_values=True))
    if "address-postalcode" in query_by_name:
        return [start_url]
    relation = storage_relation(directory._schema(), "geo_zip_lookup")
    zip_rows = await directory.db.all(
        f"SELECT DISTINCT zip_code FROM {relation} WHERE zip_code ~ '^[0-9]{{5}}$' ORDER BY zip_code"
    )
    if not zip_rows:
        raise RuntimeError("provider_directory_uhc_location_zip_inventory_empty")
    return [directory._url_with_query_item(start_url, "address-postalcode", zip_row[0]) for zip_row in zip_rows]


def location_partition_predicate_error(directory, source_record, resource_type, url, bundle):
    """Fail a Location partition if a returned address falls outside its search."""
    if resource_type != "Location" or not is_source(directory, source_record):
        return None
    query_by_name = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query, keep_blank_values=True))
    state = directory._clean_text(query_by_name.get("address-state"))
    city = directory._uhc_query_prefix(query_by_name, "address-city") if state else None
    postal = directory._uhc_query_prefix(query_by_name, "address-postalcode")
    if not state and not postal:
        return None
    for entry in directory._bundle_entries(bundle):
        resource = entry.get("resource")
        if not isinstance(resource, dict) or resource.get("resourceType") != "Location":
            continue
        address = resource.get("address")
        address = address if isinstance(address, dict) else {}
        if state and (directory._clean_text(address.get("state")) or "").casefold() != state.casefold():
            return "uhc_location_address_state_predicate_rejected"
        if city and not (directory._clean_text(address.get("city")) or "").casefold().startswith(city.casefold()):
            return "uhc_location_address_city_predicate_rejected"
        if postal and not (directory._clean_text(address.get("postalCode")) or "").startswith(postal):
            return "uhc_location_address_postal_predicate_rejected"
    return None


def unenumerable_residual(directory, source_record, resource_type, start_urls):
    """State what postal partitions cannot reach: rows whose location has no postal code to search by."""
    search_param = POSTAL_SEARCH_PARAMS.get(resource_type)
    if not search_param or not is_source(directory, source_record):
        return None
    if not any(search_param in urllib.parse.parse_qs(urllib.parse.urlsplit(url).query) for url in start_urls):
        return None
    if resource_type == "Location":
        return {"unenumerable_residual": "location_outside_known_zip_inventory"}
    # `:missing` is ignored and unfiltered totals clamp at the cap, so this residual cannot be counted or listed.
    return {"unenumerable_residual": "location_without_postal_code"}


def request_concurrency(directory, source_records):
    """Let the partition setting bound in-flight requests only when every source is this endpoint."""
    if source_records and all(is_source(directory, source_record) for source_record in source_records):
        return min(100, directory._partition_fetch_concurrency())
    return 1


def resource_batch_size(directory, runner, source_record):
    """Share the row-buffer budget across postal families in the wave."""
    size = runner.options.stream_batch_size
    if (
        runner.is_streaming
        and runner.effective_concurrency > 1
        and directory._is_uhc_role_postal_partition_enabled(source_record)
    ):
        return max(1, size // runner.effective_concurrency)
    return size


def enter_proxy_cursor(directory, source_record, resource_type, request_url):
    """Keep one partition on one proxy route while spreading partitions across the pool."""
    identity = directory._uhc_partition_identity(source_record, resource_type, request_url)
    return source_proxy.PARTITION_CURSOR_KEY.set(f"{resource_type}:{identity}" if identity else None)


async def _location_total(directory, source_record, postal_prefix, timeout):
    url = directory._url_with_query_params(
        f"{directory.UHC_PROVIDER_DIRECTORY_BASE}/Location",
        {"address-postalcode": postal_prefix, "_summary": "count"},
    )
    status_code, payload, fetch_error, _elapsed = await directory._fetch_source_json(
        source_record, url, timeout=timeout
    )
    if status_code != 200 or fetch_error or not isinstance(payload, dict):
        return None
    return directory._bundle_total(payload)


async def split_guard_error(directory, request):
    """Fail closed when a split ZIP holds Locations whose code does not start `<zip5>-`."""
    search_param = POSTAL_SEARCH_PARAMS.get(request.resource_type)
    if not search_param or not is_source(directory, request.source_record):
        return None
    query_by_name = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(request.current_url).query))
    prefix = query_by_name.get(search_param) or ""
    if not _ZIP5.fullmatch(prefix):
        return None
    timeout = request.fetch_options.timeout
    bare_total = await _location_total(directory, request.source_record, prefix, timeout)
    dashed_total = await _location_total(directory, request.source_record, f"{prefix}-", timeout)
    # Re-read the bare total so a change between the two reads cannot pass as equal.
    recheck_total = await _location_total(directory, request.source_record, prefix, timeout)
    if bare_total is None or not bare_total == dashed_total == recheck_total:
        return BARE_ZIP_ERROR
    if bare_total >= directory.UHC_ADAPTIVE_PARTITION_TOTAL_CAP:
        return BARE_ZIP_ERROR
    return None


def defer_failed_leaf(directory, request, fetch_error):
    """Queue the whole leaf for a later in-run retry and forget its page identities so the re-walk is not a loop."""
    if request.is_residual_only:
        # shortcut: a name parent's residual walk is a best-effort bulk pass; by-id closure reads referenced rows.
        return
    for page_identity in request.page_identities:
        request.state.seen_urls.discard(page_identity)
    request.state.retry_leaf_urls[request.partition_start_url] = fetch_error


async def retry_failed_leaves(directory, source_record, resource_type, start_url_queue, state, fetch_options):
    """Re-walk leaves whose pages still failed after transport retries, then record what remains."""
    for retry_round in range(RETRY_ROUNDS):
        if not state.retry_leaf_urls or state.stop_event.is_set():
            break
        await asyncio.sleep(RETRY_DELAY_SECONDS * (retry_round + 1))
        retry_urls = list(state.retry_leaf_urls)
        state.retry_leaf_urls.clear()
        if isinstance(start_url_queue, asyncio.LifoQueue):
            retry_urls.reverse()
        for request_url in retry_urls:
            start_url_queue.put_nowait(request_url)
        await directory._execute_partition_fetch_workers(
            source_record,
            resource_type,
            start_url_queue,
            state,
            fetch_options,
            directory._partition_fetch_concurrency(),
        )
    for fetch_error in state.retry_leaf_urls.values():
        await directory._mark_partition_fetch_error(state, fetch_error)
    state.retry_leaf_urls.clear()


async def _stored_rows(directory, dataset_id, resource_type):
    """Stream one acquired family from isolated candidate storage."""
    from process.provider_directory_generic_source_adapters import _source_family_rows

    async with aclosing(_source_family_rows(directory, dataset_id, (resource_type,))) as stored_rows:
        async for stored_row in stored_rows:
            yield stored_row["payload_json"]


def _missing_row_references(directory, resource_row, fields_by_target, acquired_ids_by_type):
    for target_type, reference, _field in directory._row_linked_reference_candidates(resource_row, fields_by_target):
        resource_key = directory._reference_resource_key(reference, target_type)
        if resource_key and resource_key[1] not in acquired_ids_by_type[target_type]:
            yield resource_key


async def _missing_references(directory, dataset_id, resource_types):
    """Hold only ids: acquired targets, then references from roles, affiliations and plans."""
    acquired_ids_by_type = {
        resource_type: set() for resource_type in REFERENCE_TARGET_TYPES if resource_type in resource_types
    }
    for target_type, acquired_ids in acquired_ids_by_type.items():
        async for resource_row in _stored_rows(directory, dataset_id, target_type):
            acquired_ids.add(resource_row.get("resource_id"))
    missing_keys = set()
    for source_type in (resource_type for resource_type in REFERENCE_SOURCE_TYPES if resource_type in resource_types):
        fields_by_target = {
            target_type: fields
            for target_type, fields in directory.LINKED_REFERENCE_FIELDS[source_type].items()
            if target_type in acquired_ids_by_type
        }
        async for resource_row in _stored_rows(directory, dataset_id, source_type):
            missing_keys.update(
                _missing_row_references(directory, resource_row, fields_by_target, acquired_ids_by_type)
            )
    return sorted(missing_keys)


async def _read_by_id(directory, source_record, resource_key, timeout, run_id):
    """Read one referenced resource; 404/410 means the reference is dangling, not a run failure."""
    resource_type, resource_id = resource_key
    url = f"{directory.UHC_PROVIDER_DIRECTORY_BASE}/{resource_type}/{urllib.parse.quote(resource_id, safe='')}"
    status_code, payload, fetch_error, _elapsed = await directory._fetch_source_json(
        source_record, url, timeout=directory._source_resource_timeout(source_record, resource_type, timeout)
    )
    if status_code in {404, 410}:
        return "missing", None
    if status_code != 200 or fetch_error or not isinstance(payload, dict):
        return "failed", None
    parsed = _parse_reference(directory, source_record, resource_key, payload, url, "rest_read", run_id)
    return ("fetched", parsed) if parsed is not None else ("failed", None)


def _parse_reference(directory, source_record, resource_key, payload, fetch_url, mode, run_id):
    resource_type, resource_id = resource_key
    url = f"{directory.UHC_PROVIDER_DIRECTORY_BASE}/{resource_type}/{urllib.parse.quote(resource_id, safe='')}"
    if payload.get("resourceType") != resource_type or payload.get("id") != resource_id:
        return None
    acquisition = directory.FHIRAcquisitionContext(
        self_url=url,
        fetch_url=fetch_url,
        fetch_mode=mode,
        semantic_projection_as_of=directory._source_semantic_projection_as_of(source_record),
    )
    parsed = directory.parse_fhir_resource(
        source_record["source_id"], payload, resource_url=url, acquisition=acquisition, run_id=run_id
    )
    if (
        not parsed
        or directory.RESOURCE_TYPES_BY_MODEL.get(parsed[0]) != resource_type
        or parsed[1].get("resource_id") != resource_id
    ):
        return None
    return parsed


async def _read_id_batch(directory, source_record, resource_keys, timeout, run_id):
    resource_type = resource_keys[0][0]
    expected_ids = {resource_id for _, resource_id in resource_keys}
    if not all(re.fullmatch(r"[A-Za-z0-9.-]{1,64}", resource_id) for resource_id in expected_ids):
        return {}
    url = directory._url_with_query_params(
        f"{directory.UHC_PROVIDER_DIRECTORY_BASE}/{resource_type}",
        {"_id": ",".join(resource_id for _, resource_id in resource_keys), "_count": len(resource_keys)},
    )
    status, bundle, error, _elapsed = await directory._fetch_source_json(
        source_record, url, timeout=directory._source_resource_timeout(source_record, resource_type, timeout)
    )
    if directory._is_transient_source_fetch_failure(status, error):
        return None
    if status != 200 or error or not directory._is_bundle_payload(bundle) or directory._next_link(bundle):
        return {}
    parsed_by_key = {}
    for entry in directory._bundle_entries(bundle):
        payload = entry.get("resource")
        resource_id = payload.get("id") if isinstance(payload, dict) else None
        key = (resource_type, resource_id)
        if not isinstance(resource_id, str) or resource_id not in expected_ids or key in parsed_by_key:
            return {}
        parsed = _parse_reference(directory, source_record, key, payload, url, "rest_bundle", run_id)
        if parsed is None:
            return {}
        parsed_by_key[key] = parsed
    return parsed_by_key


class _ReferenceReader:
    """Read missing references concurrently and persist them in bounded batches."""

    def __init__(self, directory, source_record, persist, *, timeout, run_id, use_batches=False, concurrency=None):
        self.directory, self.source_record, self.persist = directory, source_record, persist
        self.timeout, self.run_id = timeout, run_id
        self.use_batches = use_batches
        self.concurrency = concurrency or directory._partition_fetch_concurrency()
        self.counts_by_type = {
            resource_type: {"fetched": 0, "missing": 0, "failed": 0} for resource_type in REFERENCE_TARGET_TYPES
        }
        self.written_by_type = dict.fromkeys(REFERENCE_TARGET_TYPES, 0)
        self.rows_by_model = {}

    async def _flush(self, model, minimum_rows=1):
        if len(self.rows_by_model.get(model, ())) < minimum_rows:
            return
        resource_rows = self.rows_by_model.pop(model)
        written = await self.persist(model, resource_rows)
        resource_type = self.directory.RESOURCE_TYPES_BY_MODEL[model]
        self.written_by_type[resource_type] += written

    async def _worker(self, worker_index, key_queue, failed_keys, parsed_rows):
        """Fetch and parse only; writes stay in the task that owns the transaction binding."""
        while not key_queue.empty():
            resource_keys = key_queue.get_nowait()
            cursor_token = source_proxy.PARTITION_CURSOR_KEY.set(f"{resource_keys[0][0]}:read:{worker_index}")
            try:
                if len(resource_keys) > 1:
                    staged_by_key = await _read_id_batch(
                        self.directory, self.source_record, resource_keys, self.timeout, self.run_id
                    )
                else:
                    staged_by_key = {}
                if staged_by_key is None:
                    failed_keys.extend(resource_keys)
                    continue
                for resource_key in resource_keys:
                    if (parsed := staged_by_key.get(resource_key)) is None:
                        outcome, parsed = await _read_by_id(
                            self.directory, self.source_record, resource_key, self.timeout, self.run_id
                        )
                    else:
                        outcome = "fetched"
                    if outcome == "failed":
                        failed_keys.append(resource_key)
                        continue
                    self.counts_by_type[resource_key[0]][outcome] += 1
                    if parsed:
                        await parsed_rows.put(parsed)
            finally:
                source_proxy.PARTITION_CURSOR_KEY.reset(cursor_token)
        await parsed_rows.put(None)

    async def _persist_parsed(self, parsed_rows, worker_count):
        while worker_count:
            parsed = await parsed_rows.get()
            if parsed is None:
                worker_count -= 1
                continue
            self.rows_by_model.setdefault(parsed[0], []).append(parsed[1])
            await self._flush(parsed[0], REFERENCE_FLUSH_ROWS)

    async def read(self, resource_keys):
        """Return keys that still failed after the bounded in-run retry rounds."""
        for retry_round in range(RETRY_ROUNDS + 1):
            if retry_round:
                await asyncio.sleep(RETRY_DELAY_SECONDS * retry_round)
            key_queue, failed_keys = asyncio.Queue(), []
            keys_by_type = {}
            for key in resource_keys:
                keys_by_type.setdefault(key[0], []).append(key)
            for resource_type, keys in keys_by_type.items():
                batch_size = REFERENCE_BATCH_SIZE.get(resource_type, 1) if self.use_batches and not retry_round else 1
                for offset in range(0, len(keys), batch_size):
                    key_queue.put_nowait(keys[offset : offset + batch_size])
            worker_count = min(self.concurrency, max(1, key_queue.qsize()))
            parsed_rows = asyncio.Queue(maxsize=2 * worker_count)
            try:
                # A failing worker cancels this task's persist loop; a failing persist cancels the workers.
                async with asyncio.TaskGroup() as task_group:
                    for index in range(worker_count):
                        task_group.create_task(self._worker(index, key_queue, failed_keys, parsed_rows))
                    await self._persist_parsed(parsed_rows, worker_count)
            except BaseExceptionGroup as worker_errors:
                raise worker_errors.exceptions[0]  # keep the caller-facing exception type
            resource_keys = failed_keys
            if not resource_keys:
                break
        for model in list(self.rows_by_model):
            await self._flush(model)
        for resource_type, _resource_id in resource_keys:
            self.counts_by_type[resource_type]["failed"] += 1
        return resource_keys


def _scans_complete(directory, diagnostics_by_type):
    return all(
        directory._is_resource_diagnostic_acquisition_complete(diagnostic)
        and not diagnostic.get("error")
        and not diagnostic.get("bounded")
        for diagnostic in diagnostics_by_type.values()
    )


async def hydrate_saved_role_links(
    directory, source_record, resource_type, persist, role_done, *, timeout, run_id, max_batches=None
):
    """Fill missing targets from saved roles while the role scan still runs."""
    from process.provider_directory_generic_source_adapters import missing_saved_role_links

    concurrency = max(1, directory._partition_fetch_concurrency() // 8)
    keys = await missing_saved_role_links(
        directory, source_record["_endpoint_dataset_id"], resource_type, ROLE_LINK_FIELDS[resource_type]
    )
    reader = _ReferenceReader(
        directory, source_record, persist, timeout=timeout, run_id=run_id, use_batches=True, concurrency=concurrency
    )
    batches = batched(keys, 2 * concurrency * REFERENCE_BATCH_SIZE.get(resource_type, 1))
    for chunk in islice(batches, max_batches):
        if role_done.is_set():
            break
        await reader.read(chunk)
    return reader.written_by_type[resource_type]


async def hydrate_before_scan(directory, runner, resource_type, role_done):
    """Start Location intake with one batch of IDs already known from roles."""
    if resource_type != "Location" or role_done.is_set() or not directory.is_source_storage_required():
        return
    proxy_run = source_proxy.current_run()
    if not proxy_run or not proxy_run.enabled(runner.source_record):
        return
    written = await hydrate_saved_role_links(
        directory,
        runner.source_record,
        resource_type,
        partial(runner.persist_batch, resource_type),
        role_done,
        timeout=runner.options.timeout,
        run_id=runner.options.run_id,
        max_batches=1,
    )
    runner.count_by_resource[resource_type] += written


async def hydrate_after_scan(directory, runner, resource_type, role_done):
    """Keep a completed target's extra reads off the role scan's critical path."""
    if resource_type == "PractitionerRole":
        role_done.set()
        return
    if resource_type not in ROLE_LINK_FIELDS or role_done.is_set():
        return
    diagnostic = runner.diagnostic_by_resource.get(resource_type)
    if not diagnostic or not _scans_complete(directory, {resource_type: diagnostic}):
        return
    written = await hydrate_saved_role_links(
        directory,
        runner.source_record,
        resource_type,
        partial(runner.persist_batch, resource_type),
        role_done,
        timeout=runner.options.timeout,
        run_id=runner.options.run_id,
    )
    if written:
        runner.count_by_resource[resource_type] = runner.count_by_resource.get(resource_type, 0) + written
        diagnostic["prefetched_role_links"] = written


async def close_references(directory, source_record, persist, diagnostics_by_type, count_by_type, *, timeout, run_id):
    """Read by id every referenced Practitioner, Organization and Location the scans did not return."""
    if (
        not is_source(directory, source_record)
        or not directory._is_uhc_role_postal_partition_enabled(source_record)
        or not _scans_complete(directory, diagnostics_by_type)
    ):
        return
    target_types = [resource_type for resource_type in REFERENCE_TARGET_TYPES if resource_type in diagnostics_by_type]
    stored_dataset_id = directory._clean_text(source_record.get("_endpoint_dataset_id"))
    dataset_id = stored_dataset_id if directory.is_source_storage_required() else None
    proxy_run = source_proxy.current_run()
    reader = _ReferenceReader(
        directory,
        source_record,
        persist,
        timeout=timeout,
        run_id=run_id,
        use_batches=bool(dataset_id and proxy_run and proxy_run.enabled(source_record)),
    )
    if dataset_id:
        await reader.read(await _missing_references(directory, dataset_id, set(diagnostics_by_type)))
    for resource_type in target_types:
        diagnostic = diagnostics_by_type[resource_type]
        diagnostic["reference_closure"] = dict(reader.counts_by_type[resource_type])
        count_by_type[resource_type] = count_by_type.get(resource_type, 0) + reader.written_by_type[resource_type]
        if not dataset_id or reader.counts_by_type[resource_type]["failed"]:
            diagnostic["error"] = REFERENCE_CLOSURE_ERROR
