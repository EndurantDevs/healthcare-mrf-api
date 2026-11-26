# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Kaiser Permanente public Provider Directory: sparse offset windows, _include crawl and reference repair."""

from process import provider_directory_uhc_scan as uhc_scan

API_BASE = "https://kpx-service-bus.kp.org/service/hp/mhpo/healthplanproviderv1rc"
PAGE_COUNT = 250
CRAWLED_RESOURCES = ("PractitionerRole", "OrganizationAffiliation", "InsurancePlan")
NODE_RESOURCES = ("Practitioner", "Organization", "Location", "HealthcareService")
# Second hop: nodes that only included resources point to.
NODE_REFERENCE_FIELDS = {
    "HealthcareService": {"Organization": ("provided_by_ref",), "Location": ("location_refs",)},
    "Location": {"Organization": ("managing_organization_ref",)},
    "Organization": {"Organization": ("part_of_ref",)},
}
# 90 % of the rows the previous published edition returned for each crawled type.
MINIMUM_ROWS = {"PractitionerRole": 176_663, "OrganizationAffiliation": 374_753, "InsurancePlan": 401_707}
TOTAL_DRIFT_RATIO = 0.01
REPAIR_BATCH_SIZE = 50
REPAIR_CONCURRENCY = 8
REPAIR_ROUNDS = 4
TOTAL_DRIFT_ERROR = "kaiser_advertised_total_drift"
ROWS_BELOW_MINIMUM_ERROR = "kaiser_rows_below_previous_edition"
REFERENCE_REPAIR_ERROR = "kaiser_reference_repair_incomplete"
_TOTALS = {}
_STORED_INCLUDES = {}


def is_source(directory, source_record):
    """Match the one public Kaiser base."""
    return (
        directory._canonical_base(source_record.get("canonical_api_base") or source_record.get("api_base")) == API_BASE
    )


def seed_rows():
    """Keep the reviewed identity fields so the source id stays stable."""
    resources = list(CRAWLED_RESOURCES)
    return [
        {
            "id": "kaiser",
            "org_name": "Kaiser Permanente",
            "plan_name": "Kaiser Permanente Provider Directory",
            "api_base": API_BASE,
            "auth_type": "none",
            "requires_registration": False,
            "source": "healthporta-reviewed-provider-directory-candidate",
            "source_url": API_BASE,
            "fhir_version": "4.0.1",
            "note": "Three link collections crawled with _include=*; referenced resources are repaired by _id.",
            "metadata_json": {
                "provider_directory_confirmed_base": API_BASE,
                "provider_directory_confirmed_catalog_url": API_BASE,
                "provider_directory_confirmed_metadata_url": API_BASE + "/metadata",
                "provider_directory_supported_resources": resources,
                "provider_directory_fully_enumerable_resources": resources,
                "provider_directory_expected_nonempty_resources": resources,
                "provider_directory_resource_fetch_order": resources,
                "provider_directory_resource_page_count_caps": dict.fromkeys(resources, PAGE_COUNT),
                "provider_directory_coverage_mode": "full",
                "provider_directory_manual_only": False,
                "provider_directory_acquisition_enabled": True,
            },
        }
    ]


def start_url(directory, source_record, url):
    """Crawled link types bring their referenced nodes on the same page."""
    return directory._url_with_replaced_query_item(url, "_include", "*") if is_source(directory, source_record) else url


async def page_entries(directory, source_record, resource_type, response_payload, page_url, persist, run_id):
    """Store included resources under their own type once per run and return only the page's matches."""
    entries = directory._bundle_entries(response_payload)
    if not is_source(directory, source_record):
        return entries
    run_key = (source_record["source_id"], run_id)
    if isinstance(response_payload.get("total"), int):
        _TOTALS.setdefault((*run_key, resource_type), response_payload["total"])
    stored_keys = _STORED_INCLUDES.setdefault(run_key, set())
    matches, included_by_model = [], {}
    for entry in entries:
        resource = entry["resource"]
        if resource.get("resourceType") == resource_type:
            matches.append(entry)
            continue
        key = f"{resource.get('resourceType')}/{resource.get('id')}"
        if persist is None or key in stored_keys:
            continue
        parsed = directory.parse_fhir_resource(
            source_record["source_id"],
            resource,
            resource_url=directory._clean_text(entry.get("fullUrl")),
            acquisition=directory._rest_bundle_acquisition(entry, page_url, source_record),
            run_id=run_id,
            normalize_location_contacts=False,
        )
        if parsed:
            stored_keys.add(key)
            included_by_model.setdefault(parsed[0], []).append(parsed[1])
    for model, resource_rows in included_by_model.items():
        await persist(model, resource_rows)
    return matches


async def _advertised_total(directory, source_record, resource_type, timeout):
    url = f"{API_BASE}/{resource_type}?_summary=count"
    status, count_bundle, error, _elapsed = await directory._fetch_source_json(source_record, url, timeout=timeout)
    total = count_bundle.get("total") if status == 200 and not error and isinstance(count_bundle, dict) else None
    return total if isinstance(total, int) else None


async def _stored_count(directory, dataset_id, resource_type):
    stored_count = 0
    async for _stored_row in uhc_scan._stored_rows(directory, dataset_id, resource_type):
        stored_count += 1
    return stored_count


async def _repair_references(directory, source_record, persist, dataset_id, *, timeout, run_id):
    """Read by _id every node a stored resource points to until nothing new is missing; return unresolved keys."""
    reader = uhc_scan._ReferenceReader(
        directory,
        source_record,
        persist,
        timeout=timeout,
        run_id=run_id,
        use_batches=True,
        concurrency=REPAIR_CONCURRENCY,
    ).use_types(dict.fromkeys(NODE_RESOURCES, REPAIR_BATCH_SIZE))
    all_resources = {*CRAWLED_RESOURCES, *NODE_RESOURCES}
    fields_by_source = uhc_scan.reference_fields(directory, all_resources, CRAWLED_RESOURCES, NODE_RESOURCES)
    fields_by_source |= NODE_REFERENCE_FIELDS
    attempted_keys = set()
    for repair_round in range(REPAIR_ROUNDS + 1):
        stored_missing = await uhc_scan.missing_references(directory, dataset_id, fields_by_source)
        # A 404 stays missing in storage; it is counted once and never read again.
        missing_keys = [key for key in stored_missing if key not in attempted_keys]
        if not missing_keys or repair_round == REPAIR_ROUNDS:
            return reader, missing_keys
        attempted_keys.update(missing_keys)
        if failed_keys := await reader.read(missing_keys):
            return reader, failed_keys


async def close_references(directory, source_record, persist, diagnostics_by_type, count_by_type, *, timeout, run_id):
    """Repair references, then gate the crawl on advertised-total drift and the previous edition's row counts."""
    crawled_diagnostics = [diagnostics_by_type.get(resource_type) for resource_type in CRAWLED_RESOURCES]
    if not all(diagnostic and uhc_scan._is_scan_complete(directory, diagnostic) for diagnostic in crawled_diagnostics):
        return
    dataset_id = directory._clean_text(source_record.get("_endpoint_dataset_id"))
    if not dataset_id or not directory.is_source_storage_required():
        for diagnostic in crawled_diagnostics:
            diagnostic["error"] = REFERENCE_REPAIR_ERROR
        return
    reader, unresolved_keys = await _repair_references(
        directory, source_record, persist, dataset_id, timeout=timeout, run_id=run_id
    )
    run_key = (source_record["source_id"], run_id)
    _STORED_INCLUDES.pop(run_key, None)
    stored_by_type = {
        resource_type: await _stored_count(directory, dataset_id, resource_type)
        for resource_type in (*CRAWLED_RESOURCES, *NODE_RESOURCES)
    }
    count_by_type.update({resource_type: stored_by_type[resource_type] for resource_type in NODE_RESOURCES})
    for resource_type, diagnostic in zip(CRAWLED_RESOURCES, crawled_diagnostics):
        before = _TOTALS.pop((*run_key, resource_type), None)
        after = await _advertised_total(directory, source_record, resource_type, timeout)
        diagnostic["kaiser_acquisition"] = {
            "advertised_total_before": before,
            "advertised_total_after": after,
            "rows_stored": stored_by_type[resource_type],
            "node_rows_stored": {node_type: stored_by_type[node_type] for node_type in NODE_RESOURCES},
            "reference_repair": reader.counts_by_type,
            "unresolved_references": len(unresolved_keys),
        }
        if unresolved_keys:
            diagnostic["error"] = REFERENCE_REPAIR_ERROR
        elif before is not None and (after is None or abs(after - before) > before * TOTAL_DRIFT_RATIO):
            diagnostic["error"] = TOTAL_DRIFT_ERROR
        elif stored_by_type[resource_type] < MINIMUM_ROWS[resource_type]:
            diagnostic["error"] = ROWS_BELOW_MINIMUM_ERROR
