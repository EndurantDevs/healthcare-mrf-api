# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Classify and summarize public Provider Profile facts."""

from __future__ import annotations

from typing import Any, Mapping

from api.provider_profile_display import license_number
from process.florida_mqa_profile import STANDARD_CATEGORIES

_FHIR_CATEGORY_BY_FACT = {
    "name": "identity",
    "administrative_gender": "demographics",
    "age": "demographics",
    "contact": "contact",
    "endpoint": "contact",
    "language": "languages",
    "years_of_practice": "professional_experience",
    "taxonomy_qualification": "specialties",
    "qualification": "certifications",
    "qualification_detail": "certifications",
    "credential": "certifications",
    "specialty": "specialties",
    "role": "services",
    "role_identifier": "services",
    "role_context": "services",
    "service": "services",
    "organization": "organizations",
    "affiliation": "affiliations",
    "plan_membership": "network_participation",
    "new_patient_acceptance": "accepting_patients",
    "telehealth": "telehealth",
    "accepting_medicaid": "network_participation",
}
_PROFESSIONAL_SUMMARY_FACT_TYPES = {
    "identity": frozenset({"name"}),
    "certifications": frozenset(
        {
            "board_certification",
            "credential",
            "specialty_certification",
        }
    ),
    "specialties": frozenset(
        {"area_of_expertise", "specialty", "taxonomy_qualification"}
    ),
    "education": frozenset({"education_history", "other_health_degree"}),
    "professional_experience": frozenset({"practice_start", "years_of_practice"}),
    "languages": frozenset({"language"}),
}


def _public_fhir_fact(fact_type: str, value: Any) -> tuple[str, str]:
    """Classify exact source-backed qualification meanings for public display."""
    category = _FHIR_CATEGORY_BY_FACT.get(fact_type, "services")
    if not isinstance(value, Mapping):
        return fact_type, category
    coding = value.get("coding")
    if (
        fact_type == "qualification"
        and isinstance(coding, Mapping)
        and coding.get("text") == "Expertise"
    ):
        return "area_of_expertise", "specialties"
    if (
        fact_type == "taxonomy_qualification"
        and isinstance(coding, Mapping)
        and coding.get("system") == "http://nucc.org/provider-taxonomy"
        and coding.get("code") == "Certification"
    ):
        return "board_certification", "certifications"
    if fact_type == "qualification_detail" and license_number(value) is not None:
        return "license", "licenses"
    return fact_type, category


def _professional_summary_item(
    category: str,
    categories: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    """Return the first safe public fact for one generated-summary category."""
    group = categories.get(category)
    if not isinstance(group, Mapping):
        return None
    for profile_item in group.get("items", []):
        if not isinstance(profile_item, Mapping):
            continue
        display = str(profile_item.get("display") or "").strip()
        fact_type = str(profile_item.get("type") or "")
        fact_value = profile_item.get("value")
        if (
            not display
            or not profile_item.get("item_id")
            or fact_type not in _PROFESSIONAL_SUMMARY_FACT_TYPES[category]
            or profile_item.get("sensitive") is True
            or profile_item.get("public_default") is False
            or any(
                spelling in f"{fact_type} {display}".casefold()
                for spelling in ("license", "licence")
            )
            or (
                isinstance(fact_value, Mapping)
                and license_number(fact_value) is not None
            )
        ):
            continue
        return profile_item
    return None


def _professional_summary_name(profile_item: Mapping[str, Any]) -> str:
    """Prefer the structured name while retaining legacy display compatibility."""
    value = profile_item.get("value")
    if isinstance(value, Mapping):
        name = str(value.get("text") or "").strip()
        if name:
            return name
    display = str(profile_item.get("display") or "").strip()
    for prefix in ("Practitioner name:", "Provider name:", "Name:"):
        if display.casefold().startswith(prefix.casefold()):
            return display[len(prefix) :].strip()
    return display


def _professional_summary(
    profile: Mapping[str, Any],
    page_category: str | None,
) -> dict[str, Any] | None:
    """Generate cautious prose only for a complete, unpaged public profile."""
    categories = profile.get("categories")
    if (
        page_category is not None
        or not isinstance(categories, Mapping)
        or set(categories) != set(STANDARD_CATEGORIES)
    ):
        return None
    selected_items = [
        (category, profile_item)
        for category in _PROFESSIONAL_SUMMARY_FACT_TYPES
        if (
            profile_item := _professional_summary_item(category, categories)
        )
        is not None
    ]
    details = [
        str(profile_item["display"]).strip()
        for category, profile_item in selected_items
        if category != "identity"
    ]
    if not details:
        return None
    identity_item = next(
        (
            profile_item
            for category, profile_item in selected_items
            if category == "identity"
        ),
        None,
    )
    subject = (
        f" list {_professional_summary_name(identity_item)} and"
        if identity_item is not None
        else ""
    )
    return {
        "label": "Generated professional summary",
        "text": (
            f"Public source records{subject} report these professional details: "
            f"{'; '.join(details)}."
        ),
        "authorship": "generated_from_structured_source_data",
        "basis": [
            {"category": category, "item_id": str(profile_item["item_id"])}
            for category, profile_item in selected_items
        ],
    }
