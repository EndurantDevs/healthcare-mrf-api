# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Metadata for the carrier-native public directory; acquisition stays shared."""

API_BASE = "https://api-bcbsal-prd.safhir.io/v1/api/provider-directory"
DOC_URL = "https://www.bcbsal.org/web/documents/d/portal/fhir-documentation-1-"


def seed_rows(directory) -> list[dict]:
    """Configure the carrier directory independently of the Alabama HIE."""
    ordered_resources = [
        "PractitionerRole",
        "Location",
        "HealthcareService",
        "Practitioner",
        "OrganizationAffiliation",
        "Organization",
        "InsurancePlan",
        "Endpoint",
    ]
    return [
        {
            "id": "bcbs-alabama-public-provider-directory",
            "org_name": "Blue Cross and Blue Shield of Alabama",
            "plan_name": None,
            "api_base": API_BASE,
            "source": DOC_URL,
            "source_url": DOC_URL,
            "source_detail": "official public Provider Directory FHIR API",
            "auth_type": "none",
            "requires_registration": False,
            "last_validated_status": "valid",
            "note": "Carrier-native directory; native plan and membership evidence retain their product scope.",
            **{
                field: f"{url}?_total=accurate"
                for field, url in directory._source_override_endpoint_fields(API_BASE).items()
            },
            "metadata_json": {
                "provider_directory_confirmed_base": API_BASE,
                "provider_directory_confirmed_metadata_url": f"{API_BASE}/metadata",
                "provider_directory_confirmed_catalog_url": DOC_URL,
                "provider_directory_supported_resources": list(directory.DEFAULT_RESOURCES),
                "provider_directory_fully_enumerable_resources": list(directory.DEFAULT_RESOURCES),
                "provider_directory_expected_nonempty_resources": [
                    resource for resource in directory.DEFAULT_RESOURCES if resource != "Endpoint"
                ],
                "provider_directory_resource_page_count_caps": {
                    resource: 100 for resource in directory.DEFAULT_RESOURCES
                },
                "provider_directory_resource_fetch_order": ordered_resources,
                "provider_directory_request_interval_seconds": 1.1,
                "provider_directory_coverage_mode": "full",
                "provider_directory_acquisition_enabled": True,
            },
        }
    ]
