# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Public provider-directory seed metadata, without inferred product membership."""

API_BASE = "https://api.apim.pacificsource.com/fhir/provider/R4"
RESOURCES = (
    "InsurancePlan",
    "PractitionerRole",
    "Practitioner",
    "Organization",
    "Location",
    "HealthcareService",
    "OrganizationAffiliation",
    "Endpoint",
)


def _partition_acquisition() -> dict:
    """Keep cursor chains bounded under the existing exhaustive verifier."""
    return {
        "enabled": True,
        "resources": {
            resource: {
                "start": "1900-01-01T00:00:00Z",
                "end": "resolved_now",
                "ceiling": 25 if resource == "PractitionerRole" else 1000,
                "minimum_width_seconds": 1,
                "boundary_precision_seconds": 1,
                "page_count": 1 if resource == "PractitionerRole" else 200,
                "census_mode": "unfiltered-leaf-sum-v1",
                "volatile_metadata_paths": [],
            }
            for resource in (
                "Practitioner",
                "Location",
                "Organization",
                "HealthcareService",
                "PractitionerRole",
            )
        },
    }


def seed_rows() -> list[dict]:
    """Keep the public directory distinct from historical product aliases."""
    return [
        {
            "id": "pacificsource-provider-directory",
            "org_name": "PacificSource",
            "plan_name": None,
            "api_base": API_BASE,
            "endpoint_insurance_plan": f"{API_BASE}/InsurancePlan?_format=json",
            "endpoint_practitioner": f"{API_BASE}/Practitioner?_format=json",
            "endpoint_organization": f"{API_BASE}/Organization?_format=json",
            "endpoint_location": f"{API_BASE}/Location?_format=json",
            "endpoint_practitioner_role": f"{API_BASE}/PractitionerRole?_format=json",
            "endpoint_healthcare_service": f"{API_BASE}/HealthcareService?_format=json",
            "endpoint_organization_affiliation": f"{API_BASE}/OrganizationAffiliation?_format=json",
            "endpoint_endpoint": f"{API_BASE}/Endpoint?_format=json",
            "auth_type": "none",
            "last_validated_status": "valid",
            "requires_registration": False,
            "requires_api_key": False,
            "fhir_version": "4.0.1",
            "source": "pacificsource-provider-directory",
            "source_url": "https://pacificsource.com/resources/interoperability-and-transparency/developer-access-instructions",
            "metadata_json": {
                "provider_directory_supported_resources": list(RESOURCES),
                "provider_directory_fully_enumerable_resources": list(RESOURCES),
                "provider_directory_expected_nonempty_resources": [
                    resource for resource in RESOURCES if resource != "Endpoint"
                ],
                "provider_directory_resource_page_count_caps": {
                    **{resource: 100 for resource in RESOURCES},
                    **{resource: 200 for resource in ("Practitioner", "Location", "Organization", "HealthcareService")},
                    "PractitionerRole": 1,
                    "OrganizationAffiliation": 50,
                },
                "provider_directory_resource_scan_concurrency": 8,
                "provider_directory_opaque_cursor_census": True,
                "provider_directory_last_updated_partition_acquisition": _partition_acquisition(),
                "provider_directory_probe_resource": "Practitioner",
                "provider_directory_resource_count_estimates": {
                    "Practitioner": 3931561,
                    "Location": 1608471,
                    "HealthcareService": 99422,
                    "PractitionerRole": 96289,
                    "Organization": 78161,
                    "InsurancePlan": 7379,
                    "OrganizationAffiliation": 3134,
                    "Endpoint": 0,
                },
                "provider_directory_coverage_mode": "carrier_directory",
                "provider_directory_directory_scope": "carrier",
                "provider_directory_plan_provenance_neutralized": True,
                "provider_directory_acquisition_enabled": True,
            },
        }
    ]
