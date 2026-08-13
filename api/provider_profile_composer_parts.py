# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Private, pure helpers for composing public Provider Profile payloads."""

from __future__ import annotations

import copy
import hashlib
import json
from typing import Any, Iterable, Mapping

from api.provider_language import language_identity
from api.provider_language_merge import fhir_support_count
from api.provider_profile_display import display_value
from api.provider_profile_public_facts import (
    _FHIR_CATEGORY_BY_FACT,
    _public_fhir_fact,
)
from process.florida_mqa_profile import PROFILE_SCHEMA_VERSION, STANDARD_CATEGORIES
from process.provider_profile_reported_range import normalize_projected_state_facts

PROFILE_COMPOSER_VERSION = "provider-profile-composer/v5"


def _empty_profile(npi: int) -> dict[str, Any]:
    """Create the complete unavailable-category profile envelope."""
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
        json.dumps(
            item.get("value"), sort_keys=True, default=str, separators=(",", ":")
        ),
        str(item.get("logical_fact_key") or item.get("source_ids") or ""),
    )


def _stable_item_id(npi: int, category: str, item: Mapping[str, Any]) -> str:
    stable_value: Any = item.get("value")
    if category == "languages" and str(item.get("type")) == "language":
        stable_value = language_identity(stable_value)
    payload = json.dumps(
        [npi, category, "canonical_fact", str(item.get("type") or ""), stable_value],
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _source_generation_ids(
    state_projection: Mapping[str, Any] | None,
    state_profile: Mapping[str, Any] | None,
    fhir_profile: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Return stable generation identities for each composed source."""
    state_generation_id = (
        state_projection.get("generation_id") if state_projection else None
    ) or (state_profile.get("generation_id") if state_profile else None)
    fhir_generation_id = (
        fhir_profile.get("generation_id") if isinstance(fhir_profile, Mapping) else None
    )
    if not fhir_generation_id and isinstance(fhir_profile, Mapping):
        serialized_profile = json.dumps(
            fhir_profile,
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        )
        fhir_generation_id = (
            "content:" + hashlib.sha256(serialized_profile.encode()).hexdigest()
        )
    source_generation_ids_by_key = {
        "state_regulator": state_generation_id,
        "provider_directory_fhir": fhir_generation_id,
    }
    return {
        source_key: generation_id
        for source_key, generation_id in source_generation_ids_by_key.items()
        if generation_id
    }


def _composed_generation_id(source_generation_ids: Mapping[str, Any]) -> str:
    """Hash the exact versions that determine a composed profile."""
    generation_payload_by_field = {
        "schema_version": PROFILE_SCHEMA_VERSION,
        "composer_version": PROFILE_COMPOSER_VERSION,
        "source_generations": source_generation_ids,
    }
    serialized_payload = json.dumps(
        generation_payload_by_field,
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    )
    return hashlib.sha256(serialized_payload.encode()).hexdigest()


def _initialize_composed_profile(
    npi: int,
    state_projection: Mapping[str, Any] | None,
    fhir_profile: Mapping[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any], str]:
    """Create the mutable profile envelope and source generation fence."""
    state_profile = state_projection.get("profile") if state_projection else None
    profile = (
        copy.deepcopy(state_profile)
        if isinstance(state_profile, Mapping)
        else _empty_profile(npi)
    )
    normalize_projected_state_facts(profile)
    profile["schema_version"] = PROFILE_SCHEMA_VERSION
    profile["npi"] = npi
    source_generation_ids = _source_generation_ids(
        state_projection,
        state_profile if isinstance(state_profile, Mapping) else None,
        fhir_profile,
    )
    profile["source_generations"] = source_generation_ids
    profile["composer_version"] = PROFILE_COMPOSER_VERSION
    profile["generation_id"] = _composed_generation_id(source_generation_ids)
    categories = profile.setdefault("categories", {})
    for category in STANDARD_CATEGORIES:
        categories.setdefault(category, {"availability": "unavailable", "items": []})
    language_availability = str(
        categories["languages"].get("availability") or "unavailable"
    )
    if (
        isinstance(fhir_profile, Mapping)
        and isinstance(fhir_profile.get("facts"), Mapping)
        and language_availability == "unavailable"
    ):
        language_availability = "not_reported"
    return profile, categories, language_availability


def _fhir_fact_item_key(fact_type: object, field_value: Any) -> tuple[str, str]:
    """Build the equality key used for cross-source fact merging."""
    return str(fact_type), json.dumps(field_value, sort_keys=True, default=str)


def _existing_items_by_fhir_key(
    publication_target: Mapping[str, Any],
) -> dict[tuple[str, str], Any]:
    """Index existing state facts and mark their retained source kind."""
    existing_items_by_key = {
        _fhir_fact_item_key(
            profile_item.get("type"), profile_item.get("value")
        ): profile_item
        for profile_item in publication_target.get("items", [])
        if isinstance(profile_item, Mapping)
    }
    for existing_item in existing_items_by_key.values():
        if existing_item.get("source_record_id"):
            existing_item["source_kinds"] = sorted(
                {*existing_item.get("source_kinds", []), "state_regulator"}
            )
    return existing_items_by_key


def _merge_existing_fhir_item(
    existing_item: dict[str, Any],
    profile_item: Mapping[str, Any],
) -> None:
    """Attach one FHIR assertion to its equal existing profile fact."""
    has_existing_fhir = "provider_directory_fhir" in existing_item.get(
        "source_kinds", []
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
                "verification_status": existing_item.get("verification_status"),
            }
        ]
    fhir_assertion_by_field = {
        "source_kind": "provider_directory_fhir",
        "assertion_type": "provider_directory_reported",
        "verification_status": "payer_directory_source",
    }
    if fhir_assertion_by_field not in existing_item["assertions"]:
        existing_item["assertions"].append(fhir_assertion_by_field)
    fhir_support_total = fhir_support_count(profile_item)
    existing_item["assertion_count"] = (
        max(existing_support_count, fhir_support_total)
        if has_existing_fhir
        else existing_support_count + fhir_support_total
    )
    existing_item["source_kinds"] = sorted(
        {*existing_item.get("source_kinds", []), "provider_directory_fhir"}
    )
    existing_item["source_ids"] = sorted(
        {*existing_item.get("source_ids", []), *profile_item.get("source_ids", [])}
    )
    for count_field in ("source_count", "independent_source_count"):
        if profile_item.get(count_field) is not None:
            existing_item[count_field] = int(profile_item[count_field]) + 1


def _new_fhir_profile_item(
    fact_type: object,
    profile_item: Mapping[str, Any],
) -> dict[str, Any]:
    """Normalize one FHIR-only assertion into the public profile shape."""
    field_value = profile_item.get("value")
    return {
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


def _merge_fhir_fact_group(
    publication_target: dict[str, Any],
    fact_type: object,
    fact_group: Mapping[str, Any],
    profile_items: list[Any],
) -> None:
    """Merge one materialized FHIR fact group into a profile category."""
    if fact_group.get("_total_unknown"):
        publication_target["_source_total_unknown"] = True
    else:
        publication_target["_source_reported_total"] = int(
            publication_target.get("_source_reported_total", 0)
        ) + int(fact_group.get("total") or len(profile_items))
    publication_target["_source_materialized_count"] = int(
        publication_target.get("_source_materialized_count", 0)
    ) + len(profile_items)
    publication_target["_source_truncated"] = bool(
        publication_target.get("_source_truncated") or fact_group.get("truncated")
    )
    existing_items_by_key = _existing_items_by_fhir_key(publication_target)
    for profile_item in profile_items:
        if not isinstance(profile_item, Mapping):
            continue
        item_key = _fhir_fact_item_key(fact_type, profile_item.get("value"))
        existing_item = existing_items_by_key.get(item_key)
        if existing_item is not None:
            _merge_existing_fhir_item(existing_item, profile_item)
            continue
        normalized_item_by_field = _new_fhir_profile_item(fact_type, profile_item)
        publication_target.setdefault("items", []).append(normalized_item_by_field)
        existing_items_by_key[item_key] = normalized_item_by_field
    if publication_target.get("items"):
        publication_target["availability"] = "available"


def _merge_fhir_profile_facts(
    categories: dict[str, Any],
    fhir_profile: Mapping[str, Any] | None,
) -> None:
    """Merge every supported FHIR fact group into its public category."""
    fhir_facts = (
        fhir_profile.get("facts", {}) if isinstance(fhir_profile, Mapping) else {}
    )
    if not isinstance(fhir_facts, Mapping):
        return
    for fact_type, fact_group in fhir_facts.items():
        normalized_fact_type = str(fact_type)
        category = _FHIR_CATEGORY_BY_FACT.get(normalized_fact_type, "services")
        group = fact_group if isinstance(fact_group, Mapping) else {}
        profile_items = group.get("items", [])
        if not isinstance(profile_items, list):
            continue
        if normalized_fact_type not in {
            "qualification",
            "taxonomy_qualification",
            "qualification_detail",
        } or not profile_items:
            _merge_fhir_fact_group(categories[category], fact_type, group, profile_items)
            continue
        profile_items_by_public_fact: dict[tuple[str, str], list[Any]] = {}
        for profile_item in profile_items:
            fact_value = (
                profile_item.get("value")
                if isinstance(profile_item, Mapping)
                else None
            )
            public_fact = _public_fhir_fact(normalized_fact_type, fact_value)
            profile_items_by_public_fact.setdefault(public_fact, []).append(profile_item)
        source_total = int(group.get("total") or len(profile_items))
        is_partition_complete = (
            not group.get("truncated") and source_total == len(profile_items)
        )
        for (
            public_fact_type,
            public_category,
        ), partition_profile_items in profile_items_by_public_fact.items():
            partition_group = (
                {"total": len(partition_profile_items), "truncated": False}
                if is_partition_complete
                else {"truncated": True, "_total_unknown": True}
            )
            _merge_fhir_fact_group(
                categories[public_category],
                public_fact_type,
                partition_group,
                partition_profile_items,
            )


def _public_fhir_source(profile_source: Mapping[str, Any]) -> dict[str, Any]:
    """Project one FHIR source descriptor into its public source fields."""
    public_source_by_field = {
        "source_key": profile_source.get("source_id"),
        "source_kind": "provider_directory_fhir",
    }
    for source_field in ("endpoint_id", "dataset_id"):
        if profile_source.get(source_field):
            public_source_by_field[source_field] = profile_source.get(source_field)
    public_source_by_field.update(
        {
            "organization": profile_source.get("org_name"),
            "plan_name": profile_source.get("plan_name"),
            "api_base": profile_source.get("api_base"),
        }
    )
    return public_source_by_field


def _append_fhir_sources(
    profile: dict[str, Any],
    fhir_profile: Mapping[str, Any] | None,
) -> None:
    """Append public FHIR source descriptors before final de-duplication."""
    if not isinstance(fhir_profile, Mapping):
        return
    profile.setdefault("sources", []).extend(
        _public_fhir_source(profile_source)
        for profile_source in fhir_profile.get("sources", [])
        if isinstance(profile_source, Mapping)
    )


def _fhir_source_rows(
    fhir_profile: Mapping[str, Any] | None,
) -> Iterable[Mapping[str, Any]]:
    """Yield only mapping-shaped FHIR source descriptors."""
    if not isinstance(fhir_profile, Mapping):
        return ()
    return (
        profile_source
        for profile_source in fhir_profile.get("sources", [])
        if isinstance(profile_source, Mapping)
    )


def _finalize_category_group(
    npi: int,
    category: str,
    group: dict[str, Any],
    include_sensitive: bool,
) -> None:
    """Apply visibility, stable identity, ordering, and completeness metadata."""
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
        normalized_item_by_field = dict(profile_item)
        normalized_item_by_field["item_id"] = _stable_item_id(
            npi, category, normalized_item_by_field
        )
        normalized_items.append(normalized_item_by_field)
    group["items"] = sorted(normalized_items, key=_canonical_item_key)
    group["total"] = len(group["items"])
    group["returned"] = len(group["items"])
    group["truncated"] = bool(group.pop("_source_truncated", False))
    source_reported_total = group.pop("_source_reported_total", None)
    source_total_unknown = bool(group.pop("_source_total_unknown", False))
    source_materialized_count = int(group.pop("_source_materialized_count", 0))
    has_unmaterialized_items = (
        source_reported_total is not None
        and int(source_reported_total) > source_materialized_count
    )
    if not source_total_unknown and (
        has_unmaterialized_items
        or (source_reported_total is not None and group["truncated"])
    ):
        group["source_reported_total"] = int(source_reported_total)


def _finalize_profile_categories(
    profile: dict[str, Any],
    npi: int,
    categories: Mapping[str, Any],
    requested_categories: Iterable[str] | None,
    include_sensitive: bool,
) -> None:
    """Select requested categories and finalize every retained group."""
    requested_items = set(requested_categories or STANDARD_CATEGORIES)
    profile["categories"] = {
        category: group
        for category, group in categories.items()
        if category in requested_items
    }
    for category, group in profile["categories"].items():
        _finalize_category_group(npi, category, group, include_sensitive)


def _paginate_profile_category(
    profile: dict[str, Any],
    page_category: str | None,
    page_limit: int,
    page_offset: int,
) -> None:
    """Slice one requested category and publish deterministic page metadata."""
    if page_category is None:
        return
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


def _deduplicate_profile_sources(profile: dict[str, Any]) -> None:
    """Preserve first-seen source order while removing equal descriptors."""
    profile["sources"] = list(
        {
            json.dumps(profile_source, sort_keys=True, default=str): profile_source
            for profile_source in profile.get("sources", [])
            if isinstance(profile_source, Mapping)
        }.values()
    )
