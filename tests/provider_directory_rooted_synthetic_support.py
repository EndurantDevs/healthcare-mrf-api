# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Eight-family synthetic graph with repeated evidence and an unasserted office."""

from copy import deepcopy

from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NET_NETWORK_EXTENSION_URLS,
)
from tests import cms_npd_admission_postgres_support as imported


def rooted_source_resources():
    from tests.fhir_catalog_sync_integration_support import source_resources

    resources = source_resources(imported._resource_rows, "synthetic", retain_old=True)
    resources["Organization"].append(
        {
            "id": "provider-one",
            "active": True,
            "identifier": [{"system": "http://hl7.org/fhir/sid/us-npi", "value": "1234567893"}],
        }
    )
    resources["Practitioner"].extend(
        [
            {"id": "duplicate-root", "identifier": deepcopy(resources["Practitioner"][0]["identifier"])},
            {
                "id": "outside-cohort",
                "identifier": [{"system": "http://hl7.org/fhir/sid/us-npi", "value": "1000000491"}],
            },
        ]
    )
    resources["OrganizationAffiliation"] = [
        {
            "id": f"affiliation-{index:04d}",
            "active": True,
            "organization": {"reference": "Organization/insurer-1"},
            "participatingOrganization": {"reference": "Organization/provider-one"},
            "network": [{"reference": "Organization/" + ("network-old" if index % 2 else "network-new")}],
            "location": [{"reference": "Location/site-2"}],
        }
        for index in range(795)
    ]
    resources["PractitionerRole"] = [
        {
            "id": f"role-{index:04d}",
            "active": True,
            "practitioner": {"reference": "Practitioner/1234567893"},
            "organization": {"reference": "Organization/provider-one"},
            "location": [{"reference": "Location/site-2"}],
            "extension": [
                {
                    "url": PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NET_NETWORK_EXTENSION_URLS[0],
                    "valueReference": {"reference": "Organization/" + ("network-old" if index % 2 else "network-new")},
                }
            ],
        }
        for index in range(23)
    ]
    return resources


def wide_network_membership_resources():
    networks = [{"reference": f"Organization/network-wide-{index}"} for index in range(795)]
    return {
        "OrganizationAffiliation": [
            {
                "id": "wide-affiliation",
                "active": True,
                "participatingOrganization": {"reference": "Organization/provider-one"},
                "network": networks,
                "location": [{"reference": "Location/office-one"}],
            }
        ],
        "PractitionerRole": [
            {
                "id": "wide-role",
                "active": True,
                "practitioner": {"reference": "Practitioner/practitioner-one"},
                "location": [{"reference": "Location/office-one"}],
                "extension": [
                    {
                        "url": PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NET_NETWORK_EXTENSION_URLS[0],
                        "valueReference": reference,
                    }
                    for reference in networks[:23]
                ],
            }
        ],
    }
