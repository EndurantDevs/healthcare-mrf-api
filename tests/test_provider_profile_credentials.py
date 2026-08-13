from __future__ import annotations

from api.provider_profile import (
    compose_provider_profile,
    compose_provider_profile_evidence,
)

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
