# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compose state, CMS and Provider Directory facts into one public profile."""

from __future__ import annotations

import copy
import hashlib
import re
from typing import Any, Iterable, Mapping

from sqlalchemy import text

from api.provider_education import canonicalize_education_category
from api.provider_language_merge import (
    canonicalize_language_category,
    evidence_value_key,
)
from api.provider_profile_cms import (
    fetch_cms_education_projection,
    merge_cms_education_projection,
)
from api.provider_profile_cms_npd import fetch_cms_npd_projection
from api.provider_profile_composer_parts import (
    PROFILE_COMPOSER_VERSION,
    _append_fhir_sources,
    _deduplicate_profile_sources,
    _fhir_source_rows,
    _finalize_profile_categories,
    _initialize_composed_profile,
    _merge_fhir_profile_facts,
    _paginate_profile_category,
)
from api.provider_profile_public_facts import (
    _professional_summary,
    _public_fhir_fact,
)
from api.provider_profile_states import (
    canonicalize_exact_category,
    fetch_additional_state_profile_projections,
    merge_state_profile_projection,
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


async def fetch_provider_profile_projection(npi: int, *, include_cms_directory: bool = True) -> dict[str, Any] | None:
    """Load independently published state and CMS facts without replacing either.

    Network-scoped callers pass ``include_cms_directory=False``: directory roles are not filtered by network.
    """
    state_projection = await fetch_state_profile_projection(npi)
    cms_projection = await fetch_cms_education_projection(npi)
    additional_state_projections = await fetch_additional_state_profile_projections(npi)
    projection = merge_cms_education_projection(npi, state_projection, cms_projection)
    for state_projection in additional_state_projections:
        projection = merge_state_profile_projection(npi, projection, state_projection)
    if include_cms_directory:
        projection = merge_state_profile_projection(npi, projection, await fetch_cms_npd_projection(npi))
    return projection


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
    """Merge FHIR and independently published assertions into one provider profile."""
    if state_projection is None and fhir_profile is None:
        return None
    profile, categories, language_availability = _initialize_composed_profile(
        npi,
        state_projection,
        fhir_profile,
    )
    _merge_fhir_profile_facts(categories, fhir_profile)
    _append_fhir_sources(profile, fhir_profile)
    canonicalize_education_category(categories["education"])
    for category in ("training", "specialties", "services", "certifications"):
        canonicalize_exact_category(categories[category])
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
    profile.pop("professional_summary", None)
    professional_summary = _professional_summary(profile, page_category)
    if professional_summary is not None:
        profile["professional_summary"] = professional_summary
    _paginate_profile_category(profile, page_category, page_limit, page_offset)
    _deduplicate_profile_sources(profile)
    return profile


def compose_source_filtered_profile(npi, min_address_sources, **profile_options_by_name):
    """Remove uncounted postal facts before the existing profile pagination."""
    state_projection = profile_options_by_name.get("state_projection")
    if state_projection is not None:
        state_projection = _without_uncounted_postal_addresses(state_projection)
        locations = (state_projection.get("profile") or {}).get("categories", {}).get("locations")
        if locations is not None:
            locations["items"] = []
            locations["availability"] = "unavailable"
        profile_options_by_name["state_projection"] = state_projection
    fhir_profile = profile_options_by_name.get("fhir_profile")
    if fhir_profile is not None:
        profile_options_by_name["fhir_profile"] = _without_uncounted_postal_addresses(fhir_profile)
    profile = compose_provider_profile(npi, **profile_options_by_name)
    if profile is not None:
        profile["generation_id"] = hashlib.sha256(
            f"{profile['generation_id']}:min_address_sources:{min_address_sources}".encode()
        ).hexdigest()
    return profile


def _without_uncounted_postal_addresses(profile_value, *, preserve_office_postal=False):
    """Keep unrelated profile facts while excluding unsupported postal contacts."""
    if isinstance(profile_value, list):
        return [
            _without_uncounted_postal_addresses(field_value, preserve_office_postal=preserve_office_postal)
            for field_value in profile_value
        ]
    if not isinstance(profile_value, Mapping):
        return profile_value
    street_fields = {
        "address_line_1",
        "address_line_2",
        "address_line_3",
        "first_line",
        "second_line",
    }
    has_unknown_postal = not preserve_office_postal and bool(street_fields.intersection(profile_value))
    postal_fields = street_fields | {
        "city",
        "city_name",
        "state",
        "state_name",
        "state_code",
        "zip_code",
        "zip5",
        "postal_code",
        "country",
        "country_code",
        "county",
        "formatted_address",
        "lat",
        "long",
    }
    return {
        key: _source_filtered_postal_field(key, field_value)
        for key, field_value in profile_value.items()
        if key not in {"practice_location", "practice_locations", "mailing_address"}
        and not (key == "address" and isinstance(field_value, (Mapping, list)))
        and not (has_unknown_postal and key in postal_fields)
    }


def _source_filtered_postal_field(key, value):
    """Preserve qualified office fields while filtering their nested metadata."""
    if key == "address_list":
        return _without_uncounted_postal_addresses(value, preserve_office_postal=True)
    if key == "address_groups" and isinstance(value, list):
        return [
            {
                field: _without_uncounted_postal_addresses(item, preserve_office_postal=field == "members")
                for field, item in group.items()
            }
            if isinstance(group, Mapping)
            else _without_uncounted_postal_addresses(group)
            for group in value
        ]
    if key in {"ffs_practice_cities", "ffs_practice_states", "ffs_practice_zip_codes"}:
        return []
    return _without_uncounted_postal_addresses(value)


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
            profile_item for profile_item in group.get("items", []) if isinstance(profile_item, Mapping)
        )
    return returned_items


def _returned_projection_record_ids(
    returned_items: Iterable[Mapping[str, Any]],
) -> set[str]:
    """Collect direct and grouped source-record identities from visible facts."""
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


def _projection_evidence_payload(
    source_evidence: Mapping[str, Any] | None,
    provider_profile: Mapping[str, Any] | None,
    returned_record_ids: set[str],
) -> dict[str, Any] | None:
    """Filter one source's evidence to records visible in the composed profile."""
    if not isinstance(source_evidence, Mapping):
        return None
    source_payload = copy.deepcopy(source_evidence)
    if provider_profile:
        source_payload["records"] = [
            source_record
            for source_record in source_payload.get("records", [])
            if isinstance(source_record, Mapping) and str(source_record.get("source_record_id")) in returned_record_ids
        ]
    return source_payload


def _returned_fhir_fact_keys(
    returned_items: Iterable[Mapping[str, Any]],
) -> set[tuple[str, str]]:
    """Return canonical FHIR fact keys represented by visible profile items."""
    returned_fhir_keys = set()
    for profile_item in returned_items:
        has_fhir_source = "provider_directory_fhir" in profile_item.get("source_kinds", [])
        if has_fhir_source or not profile_item.get("source_record_id"):
            fact_type = str(profile_item.get("type") or "")
            returned_fhir_keys.add((fact_type, evidence_value_key(fact_type, profile_item.get("value"))))
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
    returned_record_ids = _returned_projection_record_ids(returned_items)
    for source_kind, evidence_field in (("state_regulator", "evidence"), ("cms_doctors", "cms_evidence")):
        source_payload = _projection_evidence_payload(
            state_projection.get(evidence_field) if state_projection else None,
            provider_profile,
            returned_record_ids,
        )
        if source_payload is not None:
            evidence_by_key["sources"][source_kind] = source_payload
    for source_key, source_evidence in (state_projection or {}).get("additional_state_evidence", {}).items():
        source_payload = _projection_evidence_payload(source_evidence, provider_profile, returned_record_ids)
        if source_payload is not None:
            evidence_by_key["sources"][source_key] = source_payload
    fhir_payload = _fhir_evidence_payload(
        fhir_evidence,
        provider_profile,
        returned_items,
    )
    if fhir_payload is not None:
        evidence_by_key["sources"]["provider_directory_fhir"] = fhir_payload
    return evidence_by_key if evidence_by_key["sources"] else None
