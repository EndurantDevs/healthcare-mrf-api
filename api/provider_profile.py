# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compose state-regulator and Provider Directory facts into one public profile."""

from __future__ import annotations

import copy
import re
from typing import Any, Iterable, Mapping

from sqlalchemy import text

from api.provider_language_merge import (
    canonicalize_language_category,
    evidence_value_key,
)
from api.provider_profile_composer_parts import (
    PROFILE_COMPOSER_VERSION,
    _append_fhir_sources,
    _deduplicate_profile_sources,
    _fhir_source_rows,
    _finalize_profile_categories,
    _initialize_composed_profile,
    _merge_fhir_profile_facts,
    _paginate_profile_category,
    _public_fhir_fact,
)
from db.models import ProviderProfileProjection, db
from process.florida_mqa_profile import PROFILE_SCHEMA_VERSION


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
    profile, categories, language_availability = _initialize_composed_profile(
        npi,
        state_projection,
        fhir_profile,
    )
    _merge_fhir_profile_facts(categories, fhir_profile)
    _append_fhir_sources(profile, fhir_profile)
    canonicalize_language_category(
        categories["languages"],
        fhir_source_rows=_fhir_source_rows(fhir_profile),
        fallback_availability=language_availability,
    )
    _finalize_profile_categories(
        profile,
        npi,
        categories,
        requested_categories,
        include_sensitive,
    )
    _paginate_profile_category(profile, page_category, page_limit, page_offset)
    _deduplicate_profile_sources(profile)
    return profile


def _returned_profile_items(
    provider_profile: Mapping[str, Any] | None,
) -> list[Mapping[str, Any]]:
    """Flatten only mapping-shaped facts returned on the composed page."""
    if not provider_profile:
        return []
    categories = provider_profile.get("categories", {})
    if not isinstance(categories, Mapping):
        return []
    returned_items: list[Mapping[str, Any]] = []
    for group in categories.values():
        if not isinstance(group, Mapping):
            continue
        returned_items.extend(
            profile_item
            for profile_item in group.get("items", [])
            if isinstance(profile_item, Mapping)
        )
    return returned_items


def _returned_state_record_ids(
    returned_items: Iterable[Mapping[str, Any]],
) -> set[str]:
    """Collect direct and grouped state-record identities from visible facts."""
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
    return returned_record_ids


def _state_evidence_payload(
    state_projection: Mapping[str, Any] | None,
    provider_profile: Mapping[str, Any] | None,
    returned_record_ids: set[str],
) -> dict[str, Any] | None:
    """Filter state evidence to records visible in the composed profile."""
    state_evidence = state_projection.get("evidence") if state_projection else None
    if not isinstance(state_evidence, Mapping):
        return None
    state_payload = copy.deepcopy(state_evidence)
    if provider_profile:
        state_payload["records"] = [
            source_record
            for source_record in state_payload.get("records", [])
            if isinstance(source_record, Mapping)
            and str(source_record.get("source_record_id")) in returned_record_ids
        ]
    return state_payload


def _returned_fhir_fact_keys(
    returned_items: Iterable[Mapping[str, Any]],
) -> set[tuple[str, str]]:
    """Return canonical FHIR fact keys represented by visible profile items."""
    returned_fhir_keys = set()
    for profile_item in returned_items:
        has_fhir_source = "provider_directory_fhir" in profile_item.get(
            "source_kinds", []
        )
        if has_fhir_source or not profile_item.get("source_record_id"):
            fact_type = str(profile_item.get("type") or "")
            returned_fhir_keys.add(
                (fact_type, evidence_value_key(fact_type, profile_item.get("value")))
            )
    return returned_fhir_keys


def _filtered_fhir_fact_group(
    fact_type: object,
    fact_group: Any,
    returned_fhir_keys: set[tuple[str, str]],
) -> dict[str, Any] | None:
    """Filter one FHIR evidence group to facts present on the public page."""
    group = fact_group if isinstance(fact_group, Mapping) else {}
    profile_items = []
    for profile_item in group.get("items", []):
        if not isinstance(profile_item, Mapping):
            continue
        value = profile_item.get("value")
        public_fact_type, _category = _public_fhir_fact(str(fact_type), value)
        if (
            public_fact_type,
            evidence_value_key(public_fact_type, value),
        ) in returned_fhir_keys:
            profile_items.append(profile_item)
    if not profile_items:
        return None
    filtered_group_by_field = dict(group)
    filtered_group_by_field["items"] = profile_items
    filtered_group_by_field["total"] = len(profile_items)
    filtered_group_by_field["truncated"] = False
    return filtered_group_by_field


def _filter_fhir_facts(
    facts: Mapping[str, Any],
    returned_fhir_keys: set[tuple[str, str]],
) -> dict[str, Any]:
    """Filter every FHIR evidence group without changing source order."""
    filtered_facts_by_type: dict[str, Any] = {}
    for fact_type, fact_group in facts.items():
        filtered_group = _filtered_fhir_fact_group(
            fact_type,
            fact_group,
            returned_fhir_keys,
        )
        if filtered_group is not None:
            filtered_facts_by_type[str(fact_type)] = filtered_group
    return filtered_facts_by_type


def _fhir_evidence_payload(
    fhir_evidence: Mapping[str, Any] | None,
    provider_profile: Mapping[str, Any] | None,
    returned_items: Iterable[Mapping[str, Any]],
) -> dict[str, Any] | None:
    """Filter FHIR evidence to facts visible in the composed profile."""
    if not isinstance(fhir_evidence, Mapping):
        return None
    fhir_payload = copy.deepcopy(fhir_evidence)
    if not provider_profile:
        return fhir_payload
    returned_fhir_keys = _returned_fhir_fact_keys(returned_items)
    facts = fhir_payload.get("facts", {})
    if isinstance(facts, Mapping):
        fhir_payload["facts"] = _filter_fhir_facts(facts, returned_fhir_keys)
    return fhir_payload


def compose_provider_profile_evidence(
    *,
    state_projection: Mapping[str, Any] | None,
    fhir_evidence: Mapping[str, Any] | None,
    provider_profile: Mapping[str, Any] | None = None,
    page_category: str | None = None,
) -> dict[str, Any] | None:
    """Return provenance limited to assertions visible on the composed profile page."""
    evidence_by_key = {"schema_version": PROFILE_SCHEMA_VERSION, "sources": {}}
    returned_items = _returned_profile_items(provider_profile)
    state_payload = _state_evidence_payload(
        state_projection,
        provider_profile,
        _returned_state_record_ids(returned_items),
    )
    if state_payload is not None:
        evidence_by_key["sources"]["state_regulator"] = state_payload
    fhir_payload = _fhir_evidence_payload(
        fhir_evidence,
        provider_profile,
        returned_items,
    )
    if fhir_payload is not None:
        evidence_by_key["sources"]["provider_directory_fhir"] = fhir_payload
    return evidence_by_key if evidence_by_key["sources"] else None
