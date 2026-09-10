# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Conservatively corroborate education assertions without changing source facts."""

from __future__ import annotations

import copy
import hashlib
import json
import unicodedata
from collections import Counter
from collections.abc import Mapping, MutableMapping
from datetime import date

from api.provider_language_merge import _apply_provenance, _provenance_sets


def _text_key(value: object) -> str:
    return " ".join(unicodedata.normalize("NFKC", str(value)).casefold().split())


def _institution_key(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = _text_key(value)
    return None if normalized in {"", "other", "unknown", "n/a", "not reported"} else normalized


def _date_year(value: Mapping) -> int | None:
    reported_date = value["graduation_date"]
    precision = value.get("graduation_date_precision")
    try:
        if precision == "year" and len(str(reported_date)) == 4:
            return date.fromisoformat(f"{reported_date}-01-01").year
        if precision not in (None, "day"):
            return None
        parsed_date = date.fromisoformat(str(reported_date))
        return parsed_date.year if parsed_date.isoformat() == reported_date else None
    except ValueError:
        return None


def _graduation_year(value: Mapping) -> int | None:
    """Accept normalized source dates and years, never guessed date prefixes."""
    reported_year = value.get("graduation_year")
    year = None
    if reported_year not in (None, ""):
        year_text = str(reported_year)
        if len(year_text) != 4 or not year_text.isascii() or not year_text.isdigit():
            return None
        year = int(year_text)
    reported_date = value.get("graduation_date")
    if reported_date not in (None, ""):
        date_year = _date_year(value)
        if date_year is None or (year is not None and year != date_year):
            return None
        year = date_year
    return year if year is not None and 1800 <= year <= 9999 else None


def _education_core(value: Mapping) -> tuple[str, int] | None:
    institution = _institution_key(value.get("institution"))
    year = _graduation_year(value)
    return (institution, year) if institution and year is not None else None


def _visibility(fact: Mapping) -> tuple[bool, bool]:
    return bool(fact.get("sensitive")), bool(fact.get("public_default"))


def _value_key(value: Mapping) -> str:
    comparable_value_by_field = dict(value)
    institution = _institution_key(value.get("institution"))
    if institution:
        comparable_value_by_field["institution"] = institution
    return json.dumps(comparable_value_by_field, sort_keys=True, default=str, separators=(",", ":"))


def _is_compatible(left: Mapping, right: Mapping) -> bool:
    """Unknown details do not override conflicting known dates or programs."""
    if _visibility(left) != _visibility(right):
        return False
    left_value, right_value = left["value"], right["value"]
    core = _education_core(left_value)
    if core is None or core != _education_core(right_value):
        return False
    left_date, right_date = left_value.get("graduation_date"), right_value.get("graduation_date")
    if left_date and right_date and "year" not in (
        left_value.get("graduation_date_precision"), right_value.get("graduation_date_precision"),
    ) and left_date != right_date:
        return False
    for field in left_value.keys() & right_value.keys():
        if field in {"institution", "graduation_year", "graduation_date", "graduation_date_precision"}:
            continue
        left_detail, right_detail = left_value[field], right_value[field]
        if left_detail in (None, "") or right_detail in (None, ""):
            continue
        if _text_key(left_detail) != _text_key(right_detail):
            return False
    return True


def _source_assertions(fact: Mapping) -> dict:
    """Keep original values, displays and supporting identities on each assertion."""
    enriched_fact = copy.deepcopy(dict(fact))
    _kinds, source_ids, record_ids = _provenance_sets([fact])
    if record_ids and not _kinds:
        enriched_fact["source_kinds"] = ["state_regulator"]
    assertions = enriched_fact.get("assertions") or [{
        "source_kind": source_kind,
        "assertion_type": fact.get("assertion_type"),
        "verification_status": fact.get("verification_status"),
    } for source_kind in enriched_fact.get("source_kinds", [])]
    for assertion in assertions:
        if "value" in assertion:
            continue
        if "quality_flags" in fact:
            assertion.setdefault("quality_flags", copy.deepcopy(fact["quality_flags"]))
        assertion.setdefault("value", copy.deepcopy(fact["value"]))
        if fact.get("display") is not None:
            assertion.setdefault("display", fact["display"])
        assertion.setdefault("source_record_ids", sorted(record_ids))
        if source_ids:
            assertion.setdefault("source_ids", sorted(source_ids))
    enriched_fact["assertions"] = assertions
    return enriched_fact


def _matching_groups(facts: list[dict]) -> list[list[dict]]:
    """Merge exact events and closed groups of mutually compatible candidates."""
    exact_groups_by_key: dict[tuple, list[dict]] = {}
    for fact in facts:
        exact_key = (_visibility(fact), _value_key(fact["value"]))
        exact_groups_by_key.setdefault(exact_key, []).append(fact)
    groups = list(exact_groups_by_key.values())
    # ponytail: Pairwise matching is per provider; index school/year if large histories emerge.
    candidates_by_index = {
        index: {
            other_index for other_index, other in enumerate(groups)
            if other_index != index and _is_compatible(group[0], other[0])
        }
        for index, group in enumerate(groups)
    }
    consumed_indices: set[int] = set()
    matched_groups = []
    for index, group in enumerate(groups):
        if index in consumed_indices:
            continue
        member_indices = {index, *candidates_by_index[index]}
        if all(
            candidates_by_index[member] == member_indices - {member}
            for member in member_indices
        ):
            consumed_indices.update(member_indices)
            matched_groups.append([fact for member in sorted(member_indices) for fact in groups[member]])
        else:
            matched_groups.append(group)
        consumed_indices.add(index)
    return matched_groups


def _preferred_fact(facts: list[dict]) -> dict:
    return min(facts, key=lambda fact: (
        -sum(value not in (None, "") for value in fact["value"].values()),
        "state_regulator" not in fact.get("source_kinds", []),
        json.dumps(fact, sort_keys=True, default=str),
    ))


def _corroborated_fields(facts: list[dict]) -> list[str]:
    """Identify shared claims, without promoting source agreement to verification."""
    institutions = {_institution_key(fact["value"].get("institution")) for fact in facts}
    years = {_graduation_year(fact["value"]) for fact in facts}
    fields = []
    if None not in institutions and len(institutions) == 1:
        fields.append("institution")
    if None not in years and len(years) == 1:
        fields.append("graduation_year")
    return fields


def _merged_fact(facts: list[dict], core_count: int) -> dict:
    preferred = _preferred_fact(facts)
    merged = copy.deepcopy(preferred)
    source_kinds, source_ids, record_ids = _provenance_sets(facts)
    _apply_provenance(merged, facts, source_kinds, source_ids, record_ids)
    if merged.get("assertions"):
        merged["assertions"].sort(key=lambda assertion: json.dumps(assertion, sort_keys=True))
    merged["assertion_count"] = max(
        len(record_ids), *(int(fact.get("assertion_count") or 1) for fact in facts),
    )
    merged["quality_flags"] = sorted({flag for fact in facts for flag in fact.get("quality_flags", [])})
    if len(facts) > 1 and len(merged.get("assertions", [])) > 1:
        merged["corroborated_fields"] = _corroborated_fields(facts)
    core = _education_core(preferred["value"])
    identity = core if core is not None and core_count == 1 else _value_key(preferred["value"])
    merged["logical_fact_key"] = hashlib.sha256(
        json.dumps(["education/v1", _visibility(preferred), identity], sort_keys=True).encode()
    ).hexdigest()
    return merged


def canonicalize_education_category(group: MutableMapping) -> None:
    """Merge compatible school/year facts only in the composed public profile."""
    education_facts = []
    other_facts = []
    for fact in group.get("items", []):
        if fact.get("type") == "education_history" and isinstance(fact.get("value"), Mapping):
            education_facts.append(_source_assertions(fact))
        else:
            other_facts.append(fact)
    groups = _matching_groups(education_facts)
    core_counts = Counter(
        (_visibility(group_facts[0]), _education_core(group_facts[0]["value"]))
        for group_facts in groups
    )
    group["items"] = [*other_facts, *(
        _merged_fact(group_facts, core_counts[
            (_visibility(group_facts[0]), _education_core(group_facts[0]["value"]))
        ])
        for group_facts in groups
    )]
