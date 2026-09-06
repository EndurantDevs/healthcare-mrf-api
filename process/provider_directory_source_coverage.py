# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Reviewed upstream coverage failures, independent of Profile selection."""

from typing import Any

MICHIGAN_SOURCE_ID = "pdfhir_75511676b61b2bddb6f94322"
INTEROPSTATION_MDHHS_PROVIDER_DIRECTORY_BASE = "https://api.interopstation.com/mdhhs/fhir"
MICHIGAN_PROVIDER_DIRECTORY_BASE = "https://mi.fhir.mhbapp.com/pd/api/v1"
MICHIGAN_SUPPORTED_RESOURCES = frozenset({
    "Location", "Organization", "OrganizationAffiliation", "Practitioner", "PractitionerRole",
})
MICHIGAN_COVERAGE_WARNING = (
    "Partial imported data: Michigan's upstream stops searches after ten pages "
    "while additional records remain accessible individually. The imported counts "
    "are not directory totals. Full acquisition and new publication are blocked "
    "until an exhaustive retrieval method is verified."
)


def acquisition_coverage_blocked_reason(
    source_id: str | None,
    api_base: str | None = None,
) -> str | None:
    """Do not trust stale full-coverage metadata for a reviewed truncated source."""
    if source_id == MICHIGAN_SOURCE_ID or api_base in {
        INTEROPSTATION_MDHHS_PROVIDER_DIRECTORY_BASE,
        MICHIGAN_PROVIDER_DIRECTORY_BASE,
    }:
        return "upstream_search_window_incomplete"
    return None


def michigan_provider_directory_metadata(previous_api_base: str | None) -> dict[str, Any]:
    """Keep probes and source identity without treating cursor exhaustion as coverage."""
    supported_resources = sorted(MICHIGAN_SUPPORTED_RESOURCES)
    return {
        "provider_directory_override": "michigan_mhbapp_public_provider_directory",
        "provider_directory_override_reason": MICHIGAN_COVERAGE_WARNING,
        "provider_directory_previous_api_base": previous_api_base,
        "provider_directory_confirmed_base": MICHIGAN_PROVIDER_DIRECTORY_BASE,
        "provider_directory_confirmed_metadata_url": f"{MICHIGAN_PROVIDER_DIRECTORY_BASE}/metadata",
        "provider_directory_supported_resources": supported_resources,
        "provider_directory_expected_nonempty_resources": supported_resources,
        "provider_directory_resource_page_count_caps": {
            "Location": 100,
            "Organization": 100,
            "OrganizationAffiliation": 100,
            "Practitioner": 10,
            "PractitionerRole": 25,
        },
        "provider_directory_fully_enumerable_resources": [],
        "provider_directory_coverage_mode": "probe_only",
        "provider_directory_acquisition_enabled": False,
        "provider_directory_acquisition_blocked_reason": MICHIGAN_COVERAGE_WARNING,
    }
