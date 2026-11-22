# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Highmark's partitioned Provider Directory REST contract."""

API_ROOT = "https://apicms.hmhs.com/fhir/v4/providers"
KEY_HEADER = "x-hmhs-keyId"
PAGE_SIZE = 500
CHUNKED_PARTITIONS = frozenset({"HM", "HMDE", "HMNY", "HMWV"})
CHUNKED_API_BASES = frozenset(f"{API_ROOT}/{partition}" for partition in CHUNKED_PARTITIONS)
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


def has_exact_role_census_key(directory, source_record, resource, api_base):
    """Authorize the census only with a header key bound to this exact tenant."""
    if (
        resource != "PractitionerRole"
        or api_base not in CHUNKED_API_BASES
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


def _role_partition_acquisition(partition):
    if partition not in CHUNKED_PARTITIONS:
        return {}
    return {
        "provider_directory_last_updated_partition_acquisition": {
            "enabled": True,
            "resources": {
                "PractitionerRole": {
                    "start": "1900-01-01T00:00:00Z",
                    "end": "resolved_now",
                    "ceiling": 25000,
                    "minimum_width_seconds": 0.001,
                    "boundary_precision_seconds": 0.001,
                    "page_count": PAGE_SIZE,
                    "maximum_pages_per_window": 64,
                    "census_mode": "unfiltered-leaf-sum-v1",
                },
            },
        },
    }


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
