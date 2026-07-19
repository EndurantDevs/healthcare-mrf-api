# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib


importer = importlib.import_module("process.provider_directory_fhir")


def test_plan_net_new_patient_extension_ignores_malformed_members():
    """Keep malformed Plan-Net extension members out of the stored profile."""
    extension_url = next(iter(importer.PLAN_NET_NEW_PATIENTS_EXTENSION_URLS))
    extension_by_field = {
        "url": extension_url,
        "extension": [
            None,
            {
                "url": "acceptingPatients",
                "valueCodeableConcept": {"coding": [None, {"code": "newpt"}]},
            },
            {
                "url": "fromNetwork",
                "valueReference": {"reference": "Organization/network-a"},
            },
        ],
    }

    assert importer._plan_net_accepting_patient_entry(None) is None
    assert importer._plan_net_accepting_patient_entry(extension_by_field) == {
        "code": "newpt",
        "from_network_ref": "Organization/network-a",
    }


def test_reviewed_telehealth_keeps_only_supported_extension_shapes():
    """Normalize reviewed booleans and virtual delivery without arbitrary data."""
    reviewed_url = next(iter(importer.REVIEWED_TELEHEALTH_EXTENSION_URLS))
    delivery_url = next(iter(importer.PLAN_NET_DELIVERY_METHOD_EXTENSION_URLS))
    resource_by_field = {
        "extension": [
            None,
            {"url": reviewed_url, "valueBoolean": False},
            {"url": "https://example.test/unsupported", "valueBoolean": True},
            {
                "url": delivery_url,
                "extension": [
                    None,
                    {
                        "url": "type",
                        "valueCodeableConcept": {
                            "coding": [{"code": "physical"}]
                        },
                    },
                ],
            },
        ]
    }

    assert importer._reviewed_telehealth(resource_by_field) == [{"supported": False}]


def test_profile_collection_normalizers_ignore_empty_and_malformed_items():
    """Do not persist malformed profile collection members as empty records."""
    assert importer._normalize_identifier_record(None) is None
    assert importer._normalized_insurance_plan_backbones({"plan": [None]}) == []
    assert importer._normalized_quantity(None) is None
    assert importer._normalized_insurance_plan_benefits([None, {"limit": [None]}]) == [
        {"type_codes": [], "requirement": None, "limits": []}
    ]
    assert importer._normalized_insurance_plan_coverage({"coverage": [None]}) == []

    collection_cases = (
        (importer._normalized_human_names, [None, {}]),
        (importer._normalized_fhir_addresses, [None, {}]),
        (importer._normalized_qualifications, [None, {}]),
        (importer._normalized_communications, [None, {}]),
        (importer._normalized_photo_metadata, [None, {}]),
        (importer._normalized_service_eligibility, [None, {}]),
    )
    for normalize, malformed_items in collection_cases:
        assert normalize(malformed_items) == []

    assert importer._safe_profile_photo_url(None) is None
