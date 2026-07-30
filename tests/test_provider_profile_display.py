from __future__ import annotations

import pytest

from api.provider_profile import PROFILE_COMPOSER_VERSION, compose_provider_profile
from api.provider_profile_display import display_value


@pytest.mark.parametrize(
    ("fact_type", "value", "expected"),
    (
        (
            "credential",
            {
                "classification": "credential",
                "coding": {
                    "code": "MD",
                    "display": "Doctor of Medicine",
                    "system": "http://example.test/credential",
                },
            },
            "Doctor of Medicine (MD)",
        ),
        (
            "credential",
            {
                "classification": "credential",
                "coding": [
                    {"code": "MD", "display": "Doctor of Medicine"},
                    {"code": "DO", "display": "Doctor of Osteopathic Medicine"},
                ],
            },
            "Doctor of Medicine (MD); Doctor of Osteopathic Medicine (DO)",
        ),
        (
            "language",
            {
                "codes": [
                    {
                        "code": "fr",
                        "display": "French",
                        "system": "urn:ietf:bcp:47",
                    }
                ]
            },
            "French (fr)",
        ),
        (
            "role_context",
            {
                "organization_ref": "https://example.test/Organization/opaque",
                "specialty_codes": [
                    {
                        "code": "208600000X",
                        "display": "Surgery Physician",
                        "system": "http://nucc.org/provider-taxonomy",
                    }
                ],
                "accepting_patients": [{"code": "newpt"}],
                "available_time": [],
            },
            "Surgery Physician (208600000X) — Accepting new patients",
        ),
        (
            "contact",
            {"system": "phone", "value": "555-0100", "use": "work"},
            "Phone: 555-0100 (work)",
        ),
        (
            "age",
            {"years": 52, "as_of": "2026-07-28"},
            "Age: 52 years (as of 2026-07-28)",
        ),
        ("age", {}, "Age"),
        (
            "years_of_practice",
            {"years": 25, "estimated": True, "as_of": "2026-07-28"},
            "Estimated years in practice: 25 (as of 2026-07-28)",
        ),
        (
            "years_of_practice",
            {"years": 25, "estimated": False},
            "Years in practice: 25",
        ),
        ("years_of_practice", {}, "Years of practice"),
        ("contact", {"value": "person@example.test"}, "Contact: person@example.test"),
        ("contact", {}, "Contact"),
        (
            "qualification_detail",
            {"issuer_display": "Example Medical School"},
            "Qualification issued by Example Medical School",
        ),
        ("qualification_detail", {}, "Qualification detail"),
        (
            "role_context",
            {
                "role_codes": [{"code": "pcp", "display": "Primary care"}],
                "telehealth": [{"code": "virtual"}],
                "accepting_medicaid": True,
            },
            "Primary care (pcp) — Telehealth available — Accepting Medicaid",
        ),
        ("role_context", {}, "Role context"),
        ("accepting_medicaid", {"accepted": True}, "Accepting medicaid: Yes"),
        ("accepting_medicaid", {"accepted": False}, "Accepting medicaid: No"),
        ("accepting_medicaid", {}, "Accepting medicaid"),
        ("service", {"name": "Outpatient surgery"}, "Outpatient surgery"),
        (
            "service",
            {"specialty_codes": [{"code": "208600000X", "display": "Surgery"}]},
            "Surgery (208600000X)",
        ),
        (
            "service",
            {"specialty_codes": [], "type_codes": [{"code": "consult"}]},
            "consult",
        ),
        (
            "service",
            {
                "specialty_codes": [],
                "type_codes": [],
                "category_codes": [{"display": "Clinic"}],
            },
            "Clinic",
        ),
        ("service", {"extra_details": "Referral required"}, "Referral required"),
        ("service", {"comment": "Call ahead"}, "Call ahead"),
        ("service", {}, "Service"),
        (
            "endpoint",
            {
                "name": "FHIR endpoint",
                "connection_type_display": "REST",
                "address": "https://example.test/fhir",
            },
            "FHIR endpoint — REST — https://example.test/fhir",
        ),
        (
            "endpoint",
            {"connection_type_display": "REST"},
            "REST",
        ),
        ("specialty", {"display": "Family Medicine", "code": "207Q00000X"}, "Family Medicine (207Q00000X)"),
        ("role", {"code": "doctor"}, "doctor"),
        ("specialty", {}, "Specialty"),
        (
            "new_patient_acceptance",
            {"code": "existptonly"},
            "Existing patients only",
        ),
        (
            "new_patient_acceptance",
            {"code": "nopt"},
            "Not accepting new patients",
        ),
        (
            "new_patient_acceptance",
            {"code": "source-specific"},
            "source-specific",
        ),
        ("new_patient_acceptance", {}, "New patient acceptance"),
        (
            "organization",
            {"name": "Example Clinic", "code": "ORG"},
            "Example Clinic (ORG)",
        ),
        ("organization", {"name": "Example Clinic", "code": "Example Clinic"}, "Example Clinic"),
        ("organization", {}, "Organization"),
        (
            "organization",
            {
                "name": "Example UHC Facility",
                "address_status": "payer_directory_candidate",
                "tin_status": "unavailable_from_uhc_source",
            },
            (
                "Example UHC Facility — payer-directory candidate location; "
                "TIN unavailable from UHC source"
            ),
        ),
        (
            "plan_membership",
            {
                "participating_organization": {
                    "name": "Example UHC Facility"
                },
                "plan_scope": {
                    "plan_id": "12345IL0010001",
                    "plan_year": 2026,
                },
                "ownership_status": "not_asserted",
            },
            (
                "Payer-reported plan membership: Example UHC Facility for "
                "12345IL0010001 (2026); ownership not asserted"
            ),
        ),
        (
            "plan_membership",
            {"insurance_plan_refs": ["InsurancePlan/plan-key"]},
            "Payer-reported plan membership: InsurancePlan/plan-key",
        ),
        (
            "plan_membership",
            {},
            "Payer-reported plan membership",
        ),
        (
            "provider_detail",
            {
                "opaque_ref": "https://example.test/Resource/1",
                "codes": [{"code": "detail"}],
            },
            "detail",
        ),
        ("provider_detail", {"reported": True}, "Provider detail: Yes"),
        ("provider_detail", {"description": "{not-json"}, "Provider detail"),
        ("unknown_fact", {"other": 7}, "Other: 7"),
        ("unknown_fact", ["unstructured", "value"], "unstructured; value"),
        ("unknown_fact", [{"code": "A"}, {"code": "A"}], "A"),
        ("unknown_fact", '{"other": 7}', "Other: 7"),
        ("unknown_fact", "{not-json", "Unknown fact"),
        ("unknown_fact", "", "Unknown fact"),
        ("unknown_fact", 7, "7"),
        ("unknown_fact", None, "Unknown fact"),
    ),
)
def test_display_value_renders_structured_facts_for_people(
    fact_type,
    value,
    expected,
):
    assert display_value(fact_type, value) == expected


@pytest.mark.parametrize(
    "value",
    (
        {"opaque_ref": "https://example.test/Resource/1"},
        [{"nested": {"still": "structured"}}],
        {"empty": [], "also_empty": {}},
        {"coding": [{"code": "MD"}]},
    ),
)
def test_display_value_never_serializes_structured_data(value):
    display = display_value("provider_detail", value)

    assert display
    assert all(marker not in display for marker in ("{", "}", "[", "]"))


def _structured_fhir_profile_by_key():
    """Return representative nested Provider Directory facts."""
    credential_value_by_key = {
        "classification": "credential",
        "coding": {"code": "MD", "display": "Doctor of Medicine"},
    }
    language_value_by_key = {"codes": [{"code": "fr", "display": "French"}]}
    role_value_by_key = {
        "specialty_codes": [
            {"code": "208600000X", "display": "Surgery Physician"}
        ],
        "accepting_patients": [{"code": "newpt"}],
    }
    return {
        "generation_id": "fhir-generation",
        "facts": {
            "credential": {"items": [{"value": credential_value_by_key}]},
            "language": {"items": [{"value": language_value_by_key}]},
            "role_context": {"items": [{"value": role_value_by_key}]},
        },
    }


def test_composer_formats_fhir_facts_for_people():
    fhir_profile_by_key = _structured_fhir_profile_by_key()
    profile = compose_provider_profile(
        1234567893,
        state_projection=None,
        fhir_profile=fhir_profile_by_key,
    )

    assert profile is not None
    assert PROFILE_COMPOSER_VERSION == "provider-profile-composer/v4"
    assert profile["composer_version"] == PROFILE_COMPOSER_VERSION
    expected_displays_by_category = {
        "certifications": ["Doctor of Medicine (MD)"],
        "languages": ["French (fr)"],
        "services": [
            "Surgery Physician (208600000X) — Accepting new patients"
        ],
    }
    for category, displays in expected_displays_by_category.items():
        profile_items = profile["categories"][category]["items"]
        assert [profile_item["display"] for profile_item in profile_items] == displays
        assert all(
            not profile_item["display"].startswith(("{", "["))
            for profile_item in profile_items
        )


def test_composer_preserves_values_and_stable_item_ids():
    fhir_profile_by_key = _structured_fhir_profile_by_key()
    profile = compose_provider_profile(
        1234567893,
        state_projection=None,
        fhir_profile=fhir_profile_by_key,
    )
    repeated_profile = compose_provider_profile(
        1234567893,
        state_projection=None,
        fhir_profile=fhir_profile_by_key,
    )

    assert profile is not None
    assert repeated_profile is not None
    for category in ("certifications", "languages", "services"):
        profile_items = profile["categories"][category]["items"]
        repeated_profile_items = repeated_profile["categories"][category]["items"]
        assert [profile_item["item_id"] for profile_item in profile_items] == [
            profile_item["item_id"] for profile_item in repeated_profile_items
        ]
    fhir_facts_by_type = fhir_profile_by_key["facts"]
    assert profile["categories"]["certifications"]["items"][0]["value"] == (
        fhir_facts_by_type["credential"]["items"][0]["value"]
    )
    assert profile["categories"]["languages"]["items"][0]["value"] == {
        "codes": [
            {
                "system": "urn:ietf:bcp:47",
                "code": "fr",
                "display": "French",
            }
        ]
    }
    assert profile["categories"]["services"]["items"][0]["value"] == (
        fhir_facts_by_type["role_context"]["items"][0]["value"]
    )


def _uhc_organization_value_by_field():
    return {
        "resource_id": "uhc-facility",
        "npi": 1000000491,
        "name": "Example UHC Facility",
        "type_codes": ["Clinic"],
        "candidate_addresses": [
            {
                "line": ["1 Main St"],
                "city": "Chicago",
                "state": "IL",
                "postalCode": "60601",
            }
        ],
        "address_status": "payer_directory_candidate",
        "tax_id": None,
        "tin_status": "unavailable_from_uhc_source",
        "source_lineage": {
            "catalog_set_sha256": "c" * 64,
            "source_file_id": "f" * 64,
            "file_name": "JSON_Providers_ILIEX.json",
            "artifact_sha256": "a" * 64,
            "record_ordinal": 17,
            "logical_scope_id": "b" * 64,
        },
    }


def _uhc_membership_value_by_field(source_lineage_by_field):
    return {
        "participating_organization": {
            "resource_id": "uhc-facility",
            "npi": 1000000491,
            "name": "Example UHC Facility",
        },
        "insurance_plan_refs": ["InsurancePlan/uhc-plan"],
        "plan_scope": {
            "plan_id": "12345IL0010001",
            "plan_year": 2026,
            "plan_key_id": "d" * 64,
        },
        "relationship_type": "payer_reported_provider_plan_membership",
        "ownership_status": "not_asserted",
        "source_lineage": source_lineage_by_field,
    }


def _uhc_profile_by_field():
    organization_value_by_field = _uhc_organization_value_by_field()
    membership_value_by_field = _uhc_membership_value_by_field(
        organization_value_by_field["source_lineage"]
    )
    return {
        "generation_id": "fhir-generation",
        "sources": [
            {
                "source_id": "uhc-provider-files",
                "endpoint_id": "uhc-endpoint",
                "dataset_id": "uhc-dataset",
                "org_name": "UnitedHealthcare",
                "plan_name": "Official provider files",
                "api_base": "https://providermrf.uhc.com",
            }
        ],
        "facts": {
            "organization": {
                "items": [{"value": organization_value_by_field}],
                "total": 1,
            },
            "plan_membership": {
                "items": [{"value": membership_value_by_field}],
                "total": 1,
            },
        },
    }


def test_composer_serves_uhc_organization_membership_and_exact_lineage():
    """Composer preserves nullable TIN and exact Provider Directory lineage."""
    composed = compose_provider_profile(
        1000000491,
        state_projection=None,
        fhir_profile=_uhc_profile_by_field(),
    )

    assert composed is not None
    organization_item = composed["categories"]["organizations"]["items"][0]
    membership_item = composed["categories"]["network_participation"][
        "items"
    ][0]
    assert organization_item["value"]["tax_id"] is None
    assert (
        organization_item["value"]["tin_status"]
        == "unavailable_from_uhc_source"
    )
    assert membership_item["type"] == "plan_membership"
    assert membership_item["value"]["ownership_status"] == "not_asserted"
    assert composed["sources"] == [
        {
            "source_key": "uhc-provider-files",
            "source_kind": "provider_directory_fhir",
            "endpoint_id": "uhc-endpoint",
            "dataset_id": "uhc-dataset",
            "organization": "UnitedHealthcare",
            "plan_name": "Official provider files",
            "api_base": "https://providermrf.uhc.com",
        }
    ]
