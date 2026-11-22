# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Highmark's partitioned Provider Directory REST contract."""

from dataclasses import replace

API_ROOT = "https://apicms.hmhs.com/fhir/v4/providers"
KEY_HEADER = "x-hmhs-keyId"
PAGE_SIZE = 500
RETIRED_CURSOR_KEY = "highmark_retired_companion_cursor_v1"
CHUNKED_PARTITIONS = frozenset({"HM", "HMDE", "HMNY", "HMWV"})
CHUNKED_API_BASES = frozenset(f"{API_ROOT}/{partition}" for partition in CHUNKED_PARTITIONS)
RETAINED_COMPANIONS = {
    f"{API_ROOT}/HM": frozenset({"InsurancePlan", "HealthcareService", "OrganizationAffiliation"}),
    f"{API_ROOT}/HMWV": frozenset({"Location", "HealthcareService"}),
}
ROLE_REQUEST_TIMEOUT_SECONDS = 60
SOURCE_URL = (
    "https://cmsapiportal.hmhs.com/api-details?Itemid=171"
    "&apiId=696bf543-f205-428f-8065-b3ca967935c7&catalogModuleId=119"
    "&managerId=1&swaggerVersion=2.0&type=rest&usage=api"
)
PARTITIONS = {
    "HM": "Highmark Pennsylvania Non-Medicaid",
    "HMPAMS": "Highmark Pennsylvania Medicaid",
    "HMWVMS": "Highmark West Virginia Medicaid",
    "HMDE": "Highmark Delaware",
    "HMWV": "Highmark West Virginia",
    "HMNY": "Highmark New York",
    "BCBSDE": "Blue Cross Blue Shield Delaware",
    "BCBSWY": "Blue Cross Blue Shield of Wyoming",
}
API_BASES = frozenset(f"{API_ROOT}/{partition}" for partition in PARTITIONS)


def has_exact_census_key(directory, source_record, resource, api_base):
    """Authorize the census only with a header key bound to this exact tenant."""
    if (
        not (
            (resource == "PractitionerRole" and api_base in CHUNKED_API_BASES)
            or resource in RETAINED_COMPANIONS.get(api_base, ())
        )
        or source_record.get("auth_type") != "api_key"
        or source_record.get("requires_api_key") is not True
        or directory.source_proxy.selected_run(directory, source_record) is not None
    ):
        return False
    credential_spec = directory._credential_spec_for_source(source_record)
    api_key = credential_spec.get("api_key")
    return (
        set(credential_spec) == {"api_key", "_matched_by"}
        and credential_spec["_matched_by"] == [f"api_bases:{api_base}"]
        and isinstance(api_key, dict)
        and set(api_key) == {"header", "value"}
        and isinstance(api_key["header"], str)
        and api_key["header"].lower() == KEY_HEADER.lower()
        and isinstance(api_key["value"], str)
        and directory._api_key_header(credential_spec) is not None
    )


def _date_partition_settings(*, ceiling, maximum_pages_per_window):
    return {
        "start": "1900-01-01T00:00:00Z",
        "end": "resolved_now",
        "ceiling": ceiling,
        "minimum_width_seconds": 0.001,
        "boundary_precision_seconds": 0.001,
        "page_count": PAGE_SIZE,
        "maximum_pages_per_window": maximum_pages_per_window,
        "census_mode": "unfiltered-leaf-sum-v1",
    }


def _role_partition_acquisition(partition):
    if partition not in CHUNKED_PARTITIONS:
        return {}
    return {
        "provider_directory_last_updated_partition_acquisition": {
            "enabled": True,
            "resources": {"PractitionerRole": _date_partition_settings(ceiling=25000, maximum_pages_per_window=64)},
        },
    }


def companion_source_id(directory, api_base):
    """Derive the exact enrolled source for a recoverable tenant."""
    if api_base not in RETAINED_COMPANIONS:
        return None
    return next(directory._stable_source_id(row) for row in seed_rows(()) if row["api_base"] == api_base)


def is_companion_source(directory, source_ids):
    """Identify a single source in the recovery allowlist."""
    return len(source_ids) == 1 and any(
        source_ids[0] == companion_source_id(directory, api_base) for api_base in RETAINED_COMPANIONS
    )


def companion_partition_settings(api_base, resource):
    """Use the counted acquisition for a separately admitted companion."""
    if resource not in RETAINED_COMPANIONS.get(api_base, ()):
        return None
    return _date_partition_settings(ceiling=4000, maximum_pages_per_window=12)


def retained_companion_cursor(directory, checkpoint):
    """Recognize only a retired, previously resumable ordinary checkpoint."""
    from process.provider_directory_generic_source_storage import _is_resumable_transition_checkpoint

    archive_by_field = dict(directory._json_object(checkpoint.get("completeness_json")))
    original_by_field = {
        **checkpoint,
        "state": "active",
        "next_url": archive_by_field.pop(RETIRED_CURSOR_KEY, None),
        "completeness_json": archive_by_field,
    }
    pages, rows = checkpoint["pages_processed"], checkpoint["rows_processed"]
    if (
        checkpoint.get("state") != "superseded"
        or checkpoint.get("next_url") is not None
        or not companion_partition_settings(checkpoint["canonical_api_base"], checkpoint["resource_type"])
        or type(pages) is not int
        or type(rows) is not int
        or min(pages, rows) <= 0
        or not _is_resumable_transition_checkpoint(original_by_field)
    ):
        raise RuntimeError("provider_directory_highmark_companion_predecessor_invalid")
    return pages, rows


async def admitted_companion_partition(directory, context, resource):
    """Load the conversion without changing the endpoint's seeded scope hash."""
    if (
        context is None
        or not context.dataset_id
        or (settings := companion_partition_settings(context.canonical_api_base, resource)) is None
    ):
        return None, (0, 0)
    predecessor_by_field = await directory._fetch_pagination_checkpoint(context, resource)
    if predecessor_by_field.get("state") != "superseded":
        return None, (0, 0)
    predecessor_by_field = {**predecessor_by_field, "canonical_api_base": context.canonical_api_base}
    progress = retained_companion_cursor(directory, predecessor_by_field)
    successor_by_field = await directory._fetch_pagination_checkpoint(
        context, directory._last_updated_partition_checkpoint_resource_type(resource)
    )
    if not (
        context.lineage_verified
        and directory._has_matching_pagination_checkpoint_identity(predecessor_by_field, context)
        and directory._has_matching_pagination_checkpoint_identity(successor_by_field, context)
    ):
        raise RuntimeError("provider_directory_highmark_companion_successor_missing")
    return directory._build_partition_config(settings), progress


def with_retained_companion_progress(result, progress):
    """Report the accumulated traversal without modifying the window proof."""
    pages, rows = progress
    if not rows:
        return result
    # A successful window result reports unique rows after two complete traversals.
    window_observations = result.rows_fetched * 2 if result.complete else result.rows_fetched
    return replace(
        result,
        pages_fetched=result.pages_fetched + pages,
        rows_fetched=window_observations + rows,
        fetch_diagnostic={**(result.fetch_diagnostic or {}), "retained_ordinary_rows": rows},
    )


def search_next_link(directory, source_record, payload, resource_type, request_url):
    """Validate the search response and resolve its continuation before staging."""
    api_base = directory._canonical_base(source_record.get("canonical_api_base") or source_record.get("api_base"))
    # Highmark omits the Bundle type on issued tenant-root continuation responses.
    if (
        payload.get("type") is None
        and directory._canonical_base(request_url) == api_base
        and directory._is_trusted_fhir_pagination_url(source_record, api_base, request_url)
    ):
        payload = {**payload, "type": "searchset"}
    if not directory.source_selection.is_search_bundle_valid(directory, payload, resource_type):
        raise ValueError("highmark_invalid_search_bundle")
    try:
        next_link = directory._current_version_census_next_link(payload)
    except ValueError:
        raise ValueError("highmark_invalid_next_link") from None
    return directory._resolved_fhir_next_url(source_record, request_url, next_link, resource_type=resource_type)


def seed_rows(resources):
    """Use one source and checkpoint scope per partition."""
    return [
        {
            "id": f"highmark-{partition.lower()}",
            "org_name": org_name,
            "plan_name": f"{org_name} Provider Directory",
            "api_base": f"{API_ROOT}/{partition}",
            "auth_type": "api_key",
            "requires_api_key": True,
            "requires_registration": True,
            "fhir_version": "4.0.1",
            "source": "highmark-provider-directory",
            "source_url": SOURCE_URL,
            "is_medicaid_mco": partition in {"HMPAMS", "HMWVMS"},
            "metadata_json": {
                **_role_partition_acquisition(partition),
                "provider_directory_supported_resources": list(resources),
                "provider_directory_fully_enumerable_resources": list(resources),
                "provider_directory_expected_nonempty_resources": [
                    "InsurancePlan",
                    "PractitionerRole",
                    "Practitioner",
                    "Organization",
                    "Location",
                ],
                "provider_directory_resource_page_count_caps": {resource: PAGE_SIZE for resource in resources},
                "provider_directory_coverage_mode": "full",
                "provider_directory_acquisition_enabled": True,
            },
        }
        for partition, org_name in PARTITIONS.items()
    ]
