# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compose state-regulator and Provider Directory facts into one public profile."""

from __future__ import annotations

import copy
import hashlib
import json
import re
from typing import Any, Iterable, Mapping

from sqlalchemy import text

from api.provider_language import language_identity
from api.provider_language_merge import (
    canonicalize_language_category,
    evidence_value_key,
    fhir_support_count,
)
from api.provider_profile_display import display_value
from db.models import ProviderProfileProjection, db
from process.florida_mqa_profile import PROFILE_SCHEMA_VERSION, STANDARD_CATEGORIES
from process.provider_profile_reported_range import normalize_projected_state_facts

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
    "new_patient_acceptance": "accepting_patients",
    "telehealth": "telehealth",
    "accepting_medicaid": "network_participation",
}
PROFILE_COMPOSER_VERSION = "provider-profile-composer/v3"

def _empty_profile(npi: int) -> dict[str, Any]:
    return {
        "schema_version": PROFILE_SCHEMA_VERSION,
        "npi": npi,
        "generation_id": None,
        "categories": {
            category: {"availability": "unavailable", "items": []}
            for category in STANDARD_CATEGORIES
        },
        "sources": [],
        "important_context": [],
    }


def _canonical_item_key(item: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(item.get("type") or ""),
        str(item.get("display") or ""),
        json.dumps(item.get("value"), sort_keys=True, default=str, separators=(",", ":")),
        str(item.get("logical_fact_key") or item.get("source_ids") or ""),
    )


def _stable_item_id(npi: int, category: str, item: Mapping[str, Any]) -> str:
    stable_value: Any = item.get("value")
    if category == "languages" and str(item.get("type")) == "language":
        stable_value = language_identity(stable_value)
    payload = json.dumps(
        [
            npi,
            category,
            "canonical_fact",
            str(item.get("type") or ""),
            stable_value,
        ],
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


async def fetch_state_profile_projection(npi: int) -> dict[str, Any] | None:
    """Load the published state-profile projection for one NPI."""
    schema = ProviderProfileProjection.__table__.schema or "mrf"
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise RuntimeError("provider_profile_schema_invalid")
    table = f"{schema}.provider_profile_projection"
    if await db.scalar(text("SELECT to_regclass(:table)"), table=table) is None:
        return None
    row = await db.first(
        text(
            f"""
            SELECT profile_json, evidence_json, generation_id, published_at
              FROM {table}
             WHERE npi = :npi
            """
        ),
        npi=npi,
    )
    if row is None:
        return None
    mapping = row._mapping
    return {
        "profile": mapping["profile_json"],
        "evidence": mapping["evidence_json"],
        "generation_id": mapping["generation_id"],
        "published_at": mapping["published_at"],
    }


def compose_provider_profile(
    npi: int,
    *,
    state_projection: Mapping[str, Any] | None,
    fhir_profile: Mapping[str, Any] | None,
    requested_categories: Iterable[str] | None = None,
    include_sensitive: bool = False,
    page_category: str | None = None,
    page_limit: int = 25,
    page_offset: int = 0,
) -> dict[str, Any] | None:
    """Merge FHIR and state assertions into the canonical provider profile."""
    if state_projection is None and fhir_profile is None:
        return None
    state_profile = state_projection.get("profile") if state_projection else None
    profile = copy.deepcopy(state_profile) if isinstance(state_profile, Mapping) else _empty_profile(npi)
    normalize_projected_state_facts(profile)
    profile["schema_version"] = PROFILE_SCHEMA_VERSION
    profile["npi"] = npi
    source_generations_by_key = {
        "state_regulator": (
            state_projection.get("generation_id")
            if state_projection
            else None
        )
        or (
            state_profile.get("generation_id")
            if isinstance(state_profile, Mapping)
            else None
        ),
        "provider_directory_fhir": (
            fhir_profile.get("generation_id")
            if isinstance(fhir_profile, Mapping)
            else None
        )
        or (
            "content:"
            + hashlib.sha256(
                json.dumps(
                    fhir_profile,
                    sort_keys=True,
                    default=str,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
            if isinstance(fhir_profile, Mapping)
            else None
        ),
    }
    source_generations_by_key = {
        key: field_value for key, field_value in source_generations_by_key.items() if field_value
    }
    profile["source_generations"] = source_generations_by_key
    profile["composer_version"] = PROFILE_COMPOSER_VERSION
    profile["generation_id"] = hashlib.sha256(
        json.dumps(
            {
                "schema_version": PROFILE_SCHEMA_VERSION,
                "composer_version": PROFILE_COMPOSER_VERSION,
                "source_generations": source_generations_by_key,
            },
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    categories = profile.setdefault("categories", {})
    for category in STANDARD_CATEGORIES:
        categories.setdefault(category, {"availability": "unavailable", "items": []})
    language_fallback_availability = str(
        categories["languages"].get("availability") or "unavailable"
    )
    if (
        isinstance(fhir_profile, Mapping)
        and isinstance(fhir_profile.get("facts"), Mapping)
        and language_fallback_availability == "unavailable"
    ):
        language_fallback_availability = "not_reported"

    fhir_facts = fhir_profile.get("facts", {}) if isinstance(fhir_profile, Mapping) else {}
    if isinstance(fhir_facts, Mapping):
        for fact_type, fact_group in fhir_facts.items():
            category = _FHIR_CATEGORY_BY_FACT.get(str(fact_type), "services")
            group = fact_group if isinstance(fact_group, Mapping) else {}
            profile_items = group.get("items", []) if isinstance(group, Mapping) else []
            if not isinstance(profile_items, list):
                continue
            publication_target = categories[category]
            publication_target["_source_reported_total"] = int(
                publication_target.get("_source_reported_total", 0)
            ) + int(group.get("total") or len(profile_items))
            publication_target["_source_materialized_count"] = int(
                publication_target.get("_source_materialized_count", 0)
            ) + len(profile_items)
            publication_target["_source_truncated"] = bool(
                publication_target.get("_source_truncated")
                or group.get("truncated")
            )
            existing_by_key = {
                (
                    str(profile_item.get("type")),
                    json.dumps(profile_item.get("value"), sort_keys=True, default=str),
                ): profile_item
                for profile_item in publication_target.get("items", [])
                if isinstance(profile_item, Mapping)
            }
            for existing_item in existing_by_key.values():
                if existing_item.get("source_record_id"):
                    existing_item["source_kinds"] = sorted(
                        {
                            *existing_item.get("source_kinds", []),
                            "state_regulator",
                        }
                    )
            for profile_item in profile_items:
                if not isinstance(profile_item, Mapping):
                    continue
                field_value = profile_item.get("value")
                key = (str(fact_type), json.dumps(field_value, sort_keys=True, default=str))
                if key in existing_by_key:
                    existing_item = existing_by_key[key]
                    is_already_has_fhir = (
                        "provider_directory_fhir"
                        in existing_item.get("source_kinds", [])
                    )
                    existing_support_count = max(
                        int(existing_item.get("assertion_count") or 0),
                        len(existing_item.get("source_record_ids") or []),
                        1,
                    )
                    if not existing_item.get("assertions"):
                        existing_item["assertions"] = [
                            {
                                "source_kind": "state_regulator",
                                "assertion_type": existing_item.get("assertion_type"),
                                "verification_status": existing_item.get(
                                    "verification_status"
                                ),
                            }
                        ]
                    source_assertions = existing_item["assertions"]
                    fhir_assertion_by_key = {
                        "source_kind": "provider_directory_fhir",
                        "assertion_type": "provider_directory_reported",
                        "verification_status": "payer_directory_source",
                    }
                    if fhir_assertion_by_key not in source_assertions:
                        source_assertions.append(fhir_assertion_by_key)
                    fhir_support_total = fhir_support_count(profile_item)
                    existing_item["assertion_count"] = (
                        max(existing_support_count, fhir_support_total)
                        if is_already_has_fhir
                        else existing_support_count + fhir_support_total
                    )
                    existing_item["source_kinds"] = sorted(
                        {
                            *existing_item.get("source_kinds", []),
                            "provider_directory_fhir",
                        }
                    )
                    existing_item["source_ids"] = sorted(
                        {
                            *existing_item.get("source_ids", []),
                            *profile_item.get("source_ids", []),
                        }
                    )
                    for count_field in ("source_count", "independent_source_count"):
                        if profile_item.get(count_field) is not None:
                            existing_item[count_field] = int(profile_item[count_field]) + 1
                    continue
                normalized_item_by_key = {
                    "type": str(fact_type),
                    "display": display_value(str(fact_type), field_value),
                    "value": field_value,
                    "assertion_type": "provider_directory_reported",
                    "verification_status": "payer_directory_source",
                    "source_kinds": ["provider_directory_fhir"],
                    "source_ids": profile_item.get("source_ids", []),
                    "source_count": profile_item.get("source_count"),
                    "independent_source_count": profile_item.get("independent_source_count"),
                    "assertions": [
                        {
                            "source_kind": "provider_directory_fhir",
                            "assertion_type": "provider_directory_reported",
                            "verification_status": "payer_directory_source",
                        }
                    ],
                    "assertion_count": fhir_support_count(profile_item),
                    "sensitive": False,
                    "public_default": True,
                }
                publication_target.setdefault("items", []).append(normalized_item_by_key)
                existing_by_key[key] = normalized_item_by_key
            if publication_target.get("items"):
                publication_target["availability"] = "available"
    if isinstance(fhir_profile, Mapping):
        profile.setdefault("sources", []).extend(
            {
                "source_key": profile_source.get("source_id"),
                "source_kind": "provider_directory_fhir",
                "organization": profile_source.get("org_name"),
                "plan_name": profile_source.get("plan_name"),
                "api_base": profile_source.get("api_base"),
            }
            for profile_source in fhir_profile.get("sources", [])
            if isinstance(profile_source, Mapping)
        )
    canonicalize_language_category(
        categories["languages"],
        fhir_source_rows=(
            profile_source
            for profile_source in (
                fhir_profile.get("sources", [])
                if isinstance(fhir_profile, Mapping)
                else []
            )
            if isinstance(profile_source, Mapping)
        ),
        fallback_availability=language_fallback_availability,
    )

    requested_items = set(requested_categories or STANDARD_CATEGORIES)
    profile["categories"] = {
        category: group
        for category, group in categories.items()
        if category in requested_items
    }
    for category, group in profile["categories"].items():
        profile_items = group.get("items", [])
        if not include_sensitive:
            group["items"] = [
                profile_item
                for profile_item in profile_items
                if not profile_item.get("sensitive") or profile_item.get("public_default")
            ]
        if profile_items and not group["items"]:
            group["availability"] = "restricted"
        normalized_items = []
        for profile_item in group["items"]:
            normalized_item_by_key = dict(profile_item)
            normalized_item_by_key["item_id"] = _stable_item_id(
                npi,
                category,
                normalized_item_by_key,
            )
            normalized_items.append(normalized_item_by_key)
        group["items"] = sorted(normalized_items, key=_canonical_item_key)
        group["total"] = len(group["items"])
        group["returned"] = len(group["items"])
        group["truncated"] = bool(group.pop("_source_truncated", False))
        source_reported_total = group.pop("_source_reported_total", None)
        source_materialized_count = int(
            group.pop("_source_materialized_count", 0)
        )
        if (
            source_reported_total is not None
            and (
                int(source_reported_total) > source_materialized_count
                or group["truncated"]
            )
        ):
            group["source_reported_total"] = int(source_reported_total)
    if page_category is not None:
        group = profile["categories"][page_category]
        visible_items = group["items"]
        total = len(visible_items)
        returned_items = visible_items[page_offset : page_offset + page_limit]
        group["items"] = returned_items
        group["returned"] = len(returned_items)
        profile["category_pagination"] = {
            "category": page_category,
            "total": total,
            "returned": len(returned_items),
            "limit": page_limit,
            "offset": page_offset,
            "has_more": page_offset + len(returned_items) < total,
        }
    profile["sources"] = list(
        {
            json.dumps(profile_source, sort_keys=True, default=str): profile_source
            for profile_source in profile.get("sources", [])
            if isinstance(profile_source, Mapping)
        }.values()
    )
    return profile


def compose_provider_profile_evidence(
    *,
    state_projection: Mapping[str, Any] | None,
    fhir_evidence: Mapping[str, Any] | None,
    provider_profile: Mapping[str, Any] | None = None,
    page_category: str | None = None,
) -> dict[str, Any] | None:
    """Return provenance limited to assertions visible on the composed profile page."""
    evidence_by_key: dict[str, Any] = {"schema_version": PROFILE_SCHEMA_VERSION, "sources": {}}
    returned_items: list[Mapping[str, Any]] = []
    if provider_profile:
        categories = provider_profile.get("categories", {})
        if isinstance(categories, Mapping):
            returned_items = [
                profile_item
                for group in categories.values()
                if isinstance(group, Mapping)
                for profile_item in group.get("items", [])
                if isinstance(profile_item, Mapping)
            ]
    returned_record_ids = {
        str(profile_item.get("source_record_id"))
        for profile_item in returned_items
        if profile_item.get("source_record_id")
    }
    returned_record_ids.update(
        str(record_id)
        for profile_item in returned_items
        for record_id in profile_item.get("source_record_ids", [])
        if record_id
    )
    state_evidence = state_projection.get("evidence") if state_projection else None
    if isinstance(state_evidence, Mapping):
        state_payload = copy.deepcopy(state_evidence)
        if provider_profile:
            state_payload["records"] = [
                source_record
                for source_record in state_payload.get("records", [])
                if isinstance(source_record, Mapping)
                and str(source_record.get("source_record_id")) in returned_record_ids
            ]
        evidence_by_key["sources"]["state_regulator"] = state_payload
    if isinstance(fhir_evidence, Mapping):
        fhir_payload = copy.deepcopy(fhir_evidence)
        if provider_profile:
            returned_fhir_keys = {
                (
                    str(profile_item.get("type") or ""),
                    evidence_value_key(
                        str(profile_item.get("type") or ""),
                        profile_item.get("value"),
                    ),
                )
                for profile_item in returned_items
                if (
                    "provider_directory_fhir" in profile_item.get("source_kinds", [])
                    or not profile_item.get("source_record_id")
                )
            }
            facts = fhir_payload.get("facts", {})
            if isinstance(facts, Mapping):
                filtered_facts_by_key: dict[str, Any] = {}
                for fact_type, fact_group in facts.items():
                    group = fact_group if isinstance(fact_group, Mapping) else {}
                    profile_items = [
                        profile_item
                        for profile_item in group.get("items", [])
                        if isinstance(profile_item, Mapping)
                        and (
                            str(fact_type),
                            evidence_value_key(
                                str(fact_type),
                                profile_item.get("value"),
                            ),
                        )
                        in returned_fhir_keys
                    ]
                    if profile_items:
                        filtered_group_by_key = dict(group)
                        filtered_group_by_key["items"] = profile_items
                        filtered_group_by_key["total"] = len(profile_items)
                        filtered_group_by_key["truncated"] = False
                        filtered_facts_by_key[str(fact_type)] = filtered_group_by_key
                fhir_payload["facts"] = filtered_facts_by_key
        evidence_by_key["sources"]["provider_directory_fhir"] = fhir_payload
    return evidence_by_key if evidence_by_key["sources"] else None
