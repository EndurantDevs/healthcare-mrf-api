# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Postal ZIP+4 descent, transport shaping and reference closure for the capped UHC FHIR search."""

import asyncio
import re
import urllib.parse
from contextlib import aclosing

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


def unenumerable_residual(directory, source_record, resource_type, start_urls):
    """State what postal partitions cannot reach: rows whose location has no postal code to search by."""
    search_param = POSTAL_SEARCH_PARAMS.get(resource_type)
    if not search_param or not is_source(directory, source_record):
        return None
    if not any(search_param in urllib.parse.parse_qs(urllib.parse.urlsplit(url).query) for url in start_urls):
        return None
    # `:missing` is ignored and unfiltered totals clamp at the cap, so this residual cannot be counted or listed.
    return {"unenumerable_residual": "location_without_postal_code"}


def request_concurrency(directory, source_records):
    """Let the partition setting bound in-flight requests only when every source is this endpoint."""
    if source_records and all(is_source(directory, source_record) for source_record in source_records):
        return min(100, directory._partition_fetch_concurrency())
    return 1


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
    acquisition = directory.FHIRAcquisitionContext(
        self_url=url,
        fetch_url=url,
        fetch_mode="rest_read",
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
        return "failed", None
    return "fetched", parsed


class _ReferenceReader:
    """Read missing references concurrently and persist them in bounded batches."""

    def __init__(self, directory, source_record, persist, *, timeout, run_id):
        self.directory, self.source_record, self.persist = directory, source_record, persist
        self.timeout, self.run_id = timeout, run_id
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
            resource_key = key_queue.get_nowait()
            cursor_token = source_proxy.PARTITION_CURSOR_KEY.set(f"{resource_key[0]}:read:{worker_index}")
            try:
                outcome, parsed = await _read_by_id(
                    self.directory, self.source_record, resource_key, self.timeout, self.run_id
                )
            finally:
                source_proxy.PARTITION_CURSOR_KEY.reset(cursor_token)
            if outcome == "failed":
                failed_keys.append(resource_key)
                continue
            self.counts_by_type[resource_key[0]][outcome] += 1
            if parsed:
                await parsed_rows.put(parsed)
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
            for resource_key in resource_keys:
                key_queue.put_nowait(resource_key)
            worker_count = min(self.directory._partition_fetch_concurrency(), max(1, len(resource_keys)))
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
    reader = _ReferenceReader(directory, source_record, persist, timeout=timeout, run_id=run_id)
    if dataset_id:
        await reader.read(await _missing_references(directory, dataset_id, set(diagnostics_by_type)))
    for resource_type in target_types:
        diagnostic = diagnostics_by_type[resource_type]
        diagnostic["reference_closure"] = dict(reader.counts_by_type[resource_type])
        count_by_type[resource_type] = count_by_type.get(resource_type, 0) + reader.written_by_type[resource_type]
        if not dataset_id or reader.counts_by_type[resource_type]["failed"]:
            diagnostic["error"] = REFERENCE_CLOSURE_ERROR
