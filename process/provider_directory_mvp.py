# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source-scoped configuration for the public MVP Provider Directory."""

API_BASE = "https://api.mvphealthcare.com/provdirfhirapi"
DOC_URL = "https://www.mvphealthcare.com/developers/apis"
CHUNKED_RESOURCES = ("Organization", "Location", "PractitionerRole", "Practitioner")
RESOURCES = CHUNKED_RESOURCES + ("InsurancePlan", "HealthcareService", "OrganizationAffiliation", "Endpoint")


def seed_rows():
    """Keep the direct directory distinct from product-specific transport aliases."""
    return [
        {
            "id": "mvp-health-care-provider-directory",
            "org_name": "MVP Health Care",
            "plan_name": None,
            "api_base": API_BASE,
            "auth_type": "none",
            "requires_registration": False,
            "source": DOC_URL,
            "source_url": DOC_URL,
            "fhir_version": "4.0.1",
            "note": "Provider Directory only; product and network participation come from native resources.",
            "metadata_json": {
                "provider_directory_confirmed_base": API_BASE,
                "provider_directory_confirmed_catalog_url": DOC_URL,
                "provider_directory_confirmed_metadata_url": API_BASE + "/metadata",
                "provider_directory_supported_resources": list(RESOURCES),
                "provider_directory_fully_enumerable_resources": list(RESOURCES),
                "provider_directory_expected_nonempty_resources": list(CHUNKED_RESOURCES) + ["InsurancePlan"],
                "provider_directory_resource_fetch_order": list(RESOURCES),
                "provider_directory_resource_page_count_caps": dict.fromkeys(RESOURCES, 100),
                "provider_directory_coverage_mode": "full",
                "provider_directory_manual_only": False,
                "provider_directory_acquisition_enabled": True,
                "provider_directory_last_updated_partition_acquisition": {
                    "enabled": True,
                    "resources": {
                        resource: {
                            "start": "1900-01-01T00:00:00Z",
                            "end": "resolved_now",
                            "ceiling": 10000,
                            # Subsecond search ranges omit timestamped records on this endpoint.
                            "minimum_width_seconds": 1,
                            "boundary_precision_seconds": 1,
                            "page_count": 100,
                            "volatile_metadata_paths": [],
                            "versioned_search_limit": 50000,
                        }
                        for resource in CHUNKED_RESOURCES
                    },
                },
            },
        }
    ]
