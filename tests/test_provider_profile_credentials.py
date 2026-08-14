from __future__ import annotations

from api.provider_profile import (
    compose_provider_profile,
    compose_provider_profile_evidence,
)
from process.florida_mqa_profile import PROFILE_SCHEMA_VERSION, STANDARD_CATEGORIES

_TYPE_SYSTEM = "http://terminology.hl7.org/CodeSystem/v2-0203"
_NUCC_SYSTEM = "http://nucc.org/provider-taxonomy"


def _coded(classification: str, display: str, **coding: str) -> dict:
    return {
        "value": {
            "coding": {"display": display, **coding},
            "classification": classification,
        }
    }


def _identifier(code: str, value: str) -> dict:
    return {
        "value": {
            "identifiers": [
                {
                    "type_codes": [
                        {"system": _TYPE_SYSTEM, "code": code}
                    ],
                    "value": value,
                }
            ],
            "issuer_display": "Example institution",
        }
    }


_FACTS = {
    "qualification": {
        "items": [
            _coded("qualification", "Clinical specialty", text="Expertise"),
            _coded("qualification", "Other qualification"),
        ],
        "total": 3,
        "truncated": True,
    },
    "taxonomy_qualification": {
        "items": [
            _coded(
                "taxonomy_qualification",
                "Specialty board",
                system=_NUCC_SYSTEM,
                code="Certification",
            ),
            _coded(
                "taxonomy_qualification",
                "Family Medicine",
                system=_NUCC_SYSTEM,
                code="207Q00000X",
            ),
        ],
        "total": 2,
        "truncated": False,
    },
    "qualification_detail": {
        "items": [
            _identifier("LN", "SYNTHETIC-123"),
            _identifier("NPI", "1234567893"),
        ],
        "total": 2,
        "truncated": False,
    },
}


def _profile() -> dict:
    profile = compose_provider_profile(
        1234567893,
        state_projection=None,
        fhir_profile={"facts": _FACTS},
    )
    assert profile is not None
    return profile


def test_composer_places_exact_credential_facts_in_public_categories():
    profile = _profile()
    facts_by_category = {
        category: {item["type"]: item["display"] for item in group["items"]}
        for category, group in profile["categories"].items()
    }
    assert facts_by_category["licenses"] == {
        "license": "License number: SYNTHETIC-123"
    }
    assert facts_by_category["specialties"] == {
        "area_of_expertise": "Clinical specialty",
        "taxonomy_qualification": "Family Medicine (207Q00000X)",
    }
    assert facts_by_category["certifications"] == {
        "board_certification": "Specialty board (Certification)",
        "qualification": "Other qualification",
        "qualification_detail": "Qualification issued by Example institution",
    }


def test_reclassified_credentials_keep_complete_evidence_and_honest_totals():
    profile = _profile()
    for category in ("specialties", "certifications"):
        assert profile["categories"][category]["truncated"] is True
        assert "source_reported_total" not in profile["categories"][category]

    evidence = compose_provider_profile_evidence(
        state_projection=None,
        fhir_evidence={"facts": _FACTS},
        provider_profile=profile,
    )
    assert evidence is not None
    facts = evidence["sources"]["provider_directory_fhir"]["facts"]
    assert {name: len(group["items"]) for name, group in facts.items()} == {
        "qualification": 2,
        "taxonomy_qualification": 2,
        "qualification_detail": 2,
    }


def _summary_fact(
    fact_type: str,
    display: str,
    value_by_key: dict,
    *,
    sensitive: bool = False,
) -> dict:
    return {
        "type": fact_type,
        "display": display,
        "value": value_by_key,
        "sensitive": sensitive,
        "public_default": not sensitive,
    }


def _summary_group(*profile_items: dict) -> dict:
    return {"availability": "available", "items": list(profile_items)}


def _summary_state_projection() -> dict:
    categories_by_key = {
        category: {"availability": "unavailable", "items": []}
        for category in STANDARD_CATEGORIES
    }
    categories_by_key["identity"] = _summary_group(
        _summary_fact(
            "name", "Practitioner name: Alex Example", {"text": "Alex Example"}
        )
    )
    categories_by_key["education"] = _summary_group(
        _summary_fact(
            "education_history",
            "Education: Example Medical School (2001)",
            {"institution": "Example Medical School", "graduation_year": 2001},
        ),
        _summary_fact(
            "adverse_education_note",
            "Private education note",
            {"note": "not for public summary"},
            sensitive=True,
        ),
    )
    categories_by_key["licenses"] = _summary_group(
        _summary_fact(
            "state_license",
            "License number: SYNTHETIC-STATE-123",
            {"license_number": "SYNTHETIC-STATE-123"},
        )
    )
    categories_by_key["organizations"] = _summary_group(
        _summary_fact(
            "organization",
            "Example Organization",
            {"name": "Example Organization"},
        )
    )
    categories_by_key["professional_experience"] = _summary_group(
        _summary_fact(
            "county_reference",
            "County: Example County",
            {"county": "Example County"},
        )
    )
    return {
        "generation_id": "state-generation",
        "profile": {
            "schema_version": PROFILE_SCHEMA_VERSION,
            "npi": 1234567893,
            "categories": categories_by_key,
            "sources": [],
        },
    }


def _summary_fhir_profile() -> dict:
    return {
        "generation_id": "directory-generation",
        "facts": {
            "credential": {
                "items": [_coded("credential", "Doctor of Medicine", code="MD")]
            },
            "specialty": {
                "items": [
                    {
                        "value": {
                            "display": "Family Medicine",
                            "code": "207Q00000X",
                        }
                    }
                ]
            },
            "years_of_practice": {
                "items": [
                    {
                        "value": {
                            "years": 20,
                            "estimated": True,
                            "as_of": "2026-08-13",
                        }
                    }
                ]
            },
            "language": {
                "items": [
                    {
                        "value": {
                            "codes": [{"code": "es", "display": "Spanish"}]
                        }
                    }
                ]
            },
        },
    }


def test_complete_profile_gets_deterministic_public_professional_summary():
    profile = compose_provider_profile(
        1234567893,
        state_projection=_summary_state_projection(),
        fhir_profile=_summary_fhir_profile(),
        include_sensitive=True,
    )

    assert profile is not None
    summary = profile["professional_summary"]
    assert summary == {
        "label": "Generated professional summary",
        "text": (
            "Public source records list Alex Example and report these professional "
            "details: Doctor of Medicine (MD); Family Medicine (207Q00000X); "
            "Education: Example Medical School (2001); Estimated years in practice: "
            "20 (as of 2026-08-13); Spanish (es)."
        ),
        "authorship": "generated_from_structured_source_data",
        "basis": [
            {
                "category": category,
                "item_id": next(
                    profile_item["item_id"]
                    for profile_item in profile["categories"][category]["items"]
                    if profile_item["type"] == fact_type
                ),
            }
            for category, fact_type in (
                ("identity", "name"),
                ("certifications", "credential"),
                ("specialties", "specialty"),
                ("education", "education_history"),
                ("professional_experience", "years_of_practice"),
                ("languages", "language"),
            )
        ],
    }
    assert all(
        excluded not in summary["text"]
        for excluded in (
            "SYNTHETIC-STATE-123",
            "Private education note",
            "Example Organization",
            "Example County",
        )
    )


def test_professional_summary_is_omitted_for_identity_only_profile():
    profile = compose_provider_profile(
        1234567893,
        state_projection=None,
        fhir_profile={
            "facts": {
                "name": {
                    "items": [
                        {
                            "value": {"text": "Alex Example"},
                        }
                    ]
                }
            }
        },
    )

    assert profile is not None
    assert "professional_summary" not in profile


def test_professional_summary_never_repeats_license_marked_qualifications():
    fhir_profile_by_key = {
        "facts": {
            "qualification": {
                "items": [
                    {
                        "value": {
                            "coding": {
                                "display": "Medical License",
                                "code": "SYNTHETIC-123",
                            }
                        }
                    }
                ]
            }
        }
    }

    profile = compose_provider_profile(
        1234567893,
        state_projection=None,
        fhir_profile=fhir_profile_by_key,
    )

    assert profile is not None
    assert "professional_summary" not in profile


def test_professional_summary_is_omitted_for_filtered_or_paged_profile():
    filtered = compose_provider_profile(
        1234567893,
        state_projection=_summary_state_projection(),
        fhir_profile=_summary_fhir_profile(),
        requested_categories=["identity", "certifications"],
    )
    paged = compose_provider_profile(
        1234567893,
        state_projection=_summary_state_projection(),
        fhir_profile=_summary_fhir_profile(),
        page_category="certifications",
        page_limit=1,
    )

    assert filtered is not None
    assert paged is not None
    assert "professional_summary" not in filtered
    assert "professional_summary" not in paged
