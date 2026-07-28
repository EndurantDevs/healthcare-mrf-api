# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Merge normalized provider-language facts without losing source provenance."""

from __future__ import annotations

import copy
import json
from collections.abc import Iterable, Mapping, MutableMapping
from typing import Any

from api.provider_language import language_identity, normalize_language_value
from api.provider_profile_display import display_value

LanguageEntry = tuple[Mapping[str, Any], dict[str, Any]]


def fhir_support_count(fhir_fact: Mapping[str, Any]) -> int:
    """Return the strongest compact count of supporting FHIR resources."""
    source_ids = fhir_fact.get("source_ids")
    source_id_count = len(source_ids) if isinstance(source_ids, list) else 0
    return max(
        int(fhir_fact.get("evidence_count") or 0),
        int(fhir_fact.get("source_count") or 0),
        source_id_count,
        1,
    )


def evidence_value_key(fact_type: str, fact_value: Any) -> str:
    """Return an evidence filter key aligned with public language identity."""
    comparable_value = (
        language_identity(fact_value)
        if fact_type == "language"
        else fact_value
    )
    return json.dumps(
        comparable_value,
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    )


def _string_values(raw_collection: Any) -> set[str]:
    if not isinstance(raw_collection, list):
        return set()
    return {str(member) for member in raw_collection if member}


def _entries_by_identity(
    language_facts: Iterable[Mapping[str, Any]],
) -> dict[tuple[str, str], list[LanguageEntry]]:
    entries_by_identity: dict[tuple[str, str], list[LanguageEntry]] = {}
    for language_fact in language_facts:
        normalized_language = normalize_language_value(language_fact.get("value"))
        if normalized_language is None:
            continue
        identity, canonical_language_by_field = normalized_language
        entries_by_identity.setdefault(identity, []).append(
            (language_fact, canonical_language_by_field)
        )
    return entries_by_identity


def _endpoint_by_source_id(
    fhir_source_rows: Iterable[Mapping[str, Any]],
) -> dict[str, str]:
    return {
        str(fhir_source.get("source_id")): str(fhir_source.get("endpoint_id"))
        for fhir_source in fhir_source_rows
        if fhir_source.get("source_id") and fhir_source.get("endpoint_id")
    }


def _canonical_language_by_field(entries: list[LanguageEntry]) -> dict[str, Any]:
    canonical_language_by_field = dict(entries[0][1])
    if any(
        language_by_field.get("preferred") is True
        for _fact, language_by_field in entries
    ):
        canonical_language_by_field["preferred"] = True
    warnings = {
        str(language_by_field.get("normalization_warning"))
        for _fact, language_by_field in entries
        if language_by_field.get("normalization_warning")
    }
    if "multiple_source_language_codes" in warnings:
        canonical_language_by_field["normalization_warning"] = (
            "multiple_source_language_codes"
        )
    elif warnings:
        canonical_language_by_field["normalization_warning"] = (
            "source_code_display_mismatch"
        )
    return canonical_language_by_field


def _base_language_fact(entries: list[LanguageEntry]) -> Mapping[str, Any]:
    language_facts = [language_fact for language_fact, _language in entries]
    return min(
        language_facts,
        key=lambda language_fact: (
            "state_regulator"
            not in _string_values(language_fact.get("source_kinds")),
            json.dumps(
                language_fact,
                sort_keys=True,
                default=str,
                separators=(",", ":"),
            ),
        ),
    )


def _unique_assertions(
    language_facts: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    assertions_by_key: dict[str, dict[str, Any]] = {}
    for language_fact in language_facts:
        raw_assertions = language_fact.get("assertions")
        if not isinstance(raw_assertions, list):
            continue
        for assertion_by_field in raw_assertions:
            if not isinstance(assertion_by_field, Mapping):
                continue
            normalized_assertion_by_field = dict(assertion_by_field)
            assertion_key = json.dumps(
                normalized_assertion_by_field,
                sort_keys=True,
                default=str,
                separators=(",", ":"),
            )
            assertions_by_key.setdefault(
                assertion_key,
                normalized_assertion_by_field,
            )
    return list(assertions_by_key.values())


def _provenance_sets(
    language_facts: Iterable[Mapping[str, Any]],
) -> tuple[set[str], set[str], set[str]]:
    facts = list(language_facts)
    source_kinds = set().union(
        *(_string_values(fact.get("source_kinds")) for fact in facts)
    )
    source_ids = set().union(
        *(_string_values(fact.get("source_ids")) for fact in facts)
    )
    source_record_ids = set().union(
        *(_string_values(fact.get("source_record_ids")) for fact in facts)
    )
    source_record_ids.update(
        str(fact.get("source_record_id"))
        for fact in facts
        if fact.get("source_record_id")
    )
    return source_kinds, source_ids, source_record_ids


def _apply_provenance(
    merged_fact_by_field: MutableMapping[str, Any],
    language_facts: list[Mapping[str, Any]],
    source_kinds: set[str],
    source_ids: set[str],
    source_record_ids: set[str],
) -> None:
    merged_fact_by_field["source_kinds"] = sorted(source_kinds)
    if source_ids:
        merged_fact_by_field["source_ids"] = sorted(source_ids)
    else:
        merged_fact_by_field.pop("source_ids", None)
    if source_record_ids:
        merged_fact_by_field["source_record_id"] = min(source_record_ids)
        merged_fact_by_field["source_record_ids"] = sorted(source_record_ids)
    else:
        merged_fact_by_field.pop("source_record_id", None)
        merged_fact_by_field.pop("source_record_ids", None)
    assertions = _unique_assertions(language_facts)
    if assertions:
        merged_fact_by_field["assertions"] = assertions
    else:
        merged_fact_by_field.pop("assertions", None)


def _independent_source_count(
    fhir_facts: list[Mapping[str, Any]],
    source_ids: set[str],
    endpoint_by_source_id: Mapping[str, str],
    state_source_count: int,
) -> int:
    endpoint_ids = {
        endpoint_by_source_id[source_id]
        for source_id in source_ids
        if source_id in endpoint_by_source_id
    }
    fhir_independent_floor = max(
        (
            int(fact.get("independent_source_count") or 0)
            for fact in fhir_facts
        ),
        default=0,
    )
    return state_source_count + (
        max(len(endpoint_ids), fhir_independent_floor, 1) if fhir_facts else 0
    )


def _support_counts(
    language_facts: list[Mapping[str, Any]],
    source_ids: set[str],
    source_record_ids: set[str],
    endpoint_by_source_id: Mapping[str, str],
) -> tuple[int, int, int]:
    """Count record support, logical feeds, and independent source systems."""
    state_facts = [
        fact
        for fact in language_facts
        if "state_regulator" in _string_values(fact.get("source_kinds"))
    ]
    fhir_facts = [
        fact
        for fact in language_facts
        if "provider_directory_fhir"
        in _string_values(fact.get("source_kinds"))
    ]
    state_source_count = 1 if state_facts else 0
    state_support_floor = max(
        (int(fact.get("assertion_count") or 1) for fact in state_facts),
        default=0,
    )
    state_support = (
        max(len(source_record_ids), state_support_floor) if state_facts else 0
    )
    fhir_support_floor = max(
        (
            max(int(fact.get("assertion_count") or 0), fhir_support_count(fact))
            for fact in fhir_facts
        ),
        default=0,
    )
    assertion_count = max(
        state_support
        + (max(len(source_ids), fhir_support_floor) if fhir_facts else 0),
        1,
    )
    fhir_source_floor = max(
        (int(fact.get("source_count") or 0) for fact in fhir_facts),
        default=0,
    )
    source_count = state_source_count + (
        max(len(source_ids), fhir_source_floor, 1) if fhir_facts else 0
    )
    independent_source_count = _independent_source_count(
        fhir_facts,
        source_ids,
        endpoint_by_source_id,
        state_source_count,
    )
    return assertion_count, source_count, independent_source_count


def _merged_language_fact(
    entries: list[LanguageEntry],
    endpoint_by_source_id: Mapping[str, str],
) -> dict[str, Any]:
    language_facts = [language_fact for language_fact, _language in entries]
    canonical_language_by_field = _canonical_language_by_field(entries)
    merged_fact_by_field = copy.deepcopy(dict(_base_language_fact(entries)))
    merged_fact_by_field.update(
        {
            "type": "language",
            "value": canonical_language_by_field,
            "display": display_value("language", canonical_language_by_field),
        }
    )
    source_kinds, source_ids, source_record_ids = _provenance_sets(language_facts)
    _apply_provenance(
        merged_fact_by_field,
        language_facts,
        source_kinds,
        source_ids,
        source_record_ids,
    )
    assertion_count, source_count, independent_source_count = _support_counts(
        language_facts,
        source_ids,
        source_record_ids,
        endpoint_by_source_id,
    )
    merged_fact_by_field.update(
        {
            "assertion_count": assertion_count,
            "source_count": source_count,
            "independent_source_count": independent_source_count,
        }
    )
    return merged_fact_by_field


def canonical_language_items(
    language_facts: Iterable[Mapping[str, Any]],
    *,
    fhir_source_rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Semantically merge state and FHIR representations of each language."""
    endpoint_index = _endpoint_by_source_id(fhir_source_rows)
    return [
        _merged_language_fact(entries, endpoint_index)
        for entries in _entries_by_identity(language_facts).values()
    ]


def canonicalize_language_category(
    language_group: MutableMapping[str, Any],
    *,
    fhir_source_rows: Iterable[Mapping[str, Any]],
    fallback_availability: str,
) -> None:
    """Replace raw language facts with canonical items and preserve null state."""
    language_facts = [
        language_fact
        for language_fact in language_group.get("items", [])
        if isinstance(language_fact, Mapping)
    ]
    language_group["items"] = canonical_language_items(
        language_facts,
        fhir_source_rows=fhir_source_rows,
    )
    empty_availability = (
        "not_reported"
        if fallback_availability in {"available", "not_reported"}
        else fallback_availability
    )
    language_group["availability"] = (
        "available" if language_group["items"] else empty_availability
    )
