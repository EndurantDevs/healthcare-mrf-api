# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Normalize source-reported provider age and birth-year ranges."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from typing import Any

_RANGE_PATTERN = re.compile(
    r"^\s*(?P<start>\d{1,4})\s*[-–—]\s*(?P<end>\d{1,4})\s*$"
)
_NOT_REPORTED_VALUES = {"", "n/a", "na", "not applicable", "unknown"}
_NON_TEMPORAL_PROFILE_MASTER_FACTS = frozenset(
    {
        "age_range",
        "birth_year_range",
        "name",
        "nica_assessment_status",
        "other_state_license_indicator",
        "provider_address",
    }
)


def _range_bounds(source_text: str) -> tuple[int, int] | None:
    match = _RANGE_PATTERN.fullmatch(source_text)
    if match is None:
        return None
    start = int(match.group("start"))
    end = int(match.group("end"))
    return (start, end) if start <= end else None


def normalize_reported_range(source_value: Any) -> dict[str, Any] | None:
    """Classify the Florida source field by its actual numeric semantics."""
    source_text = str(source_value or "").strip()
    if source_text.casefold() in _NOT_REPORTED_VALUES:
        return None
    bounds = _range_bounds(source_text)
    if bounds is None:
        return None
    start, end = bounds
    if len(str(start)) == 4 and len(str(end)) == 4:
        if not 1800 <= start <= end <= 2200:
            return None
        return {
            "fact_type": "birth_year_range",
            "display": f"Reported birth year range: {start}–{end}",
            "value": {
                "start_year": start,
                "end_year": end,
                "precision": "range",
                "source_text": source_text,
            },
        }
    if not 0 <= start <= end <= 130:
        return None
    return {
        "fact_type": "age_range",
        "display": f"Reported age range: {start}–{end} years",
        "value": {
            "minimum_years": start,
            "maximum_years": end,
            "precision": "range",
            "source_text": source_text,
        },
    }


def normalize_projected_reported_range(
    profile_item_by_key: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Upgrade one legacy projected range without changing source provenance."""
    fact_type = str(profile_item_by_key.get("type") or "")
    if fact_type not in {"age_range", "birth_year_range"}:
        return dict(profile_item_by_key)
    reported_range_value = profile_item_by_key.get("value")
    source_text = (
        reported_range_value.get("source_text")
        if isinstance(reported_range_value, Mapping)
        else reported_range_value
    )
    normalized = normalize_reported_range(source_text)
    if normalized is None:
        return None
    normalized_item_by_key = dict(profile_item_by_key)
    normalized_item_by_key["type"] = normalized["fact_type"]
    normalized_item_by_key["display"] = normalized["display"]
    normalized_item_by_key["value"] = normalized["value"]
    normalized_item_by_key.pop("effective_period", None)
    normalized_item_by_key["logical_fact_key"] = hashlib.sha256(
        json.dumps(
            [
                "demographics",
                normalized["fact_type"],
                {normalized["fact_type"]: normalized["value"]},
            ],
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return normalized_item_by_key


def _normalize_legacy_effective_period(
    profile_item_by_key: Mapping[str, Any],
) -> dict[str, Any]:
    normalized_item_by_key = dict(profile_item_by_key)
    fact_type = str(profile_item_by_key.get("type") or "")
    if fact_type in _NON_TEMPORAL_PROFILE_MASTER_FACTS:
        normalized_item_by_key.pop("effective_period", None)
    elif fact_type == "practice_start":
        effective_period_by_key = profile_item_by_key.get("effective_period")
        start = (
            effective_period_by_key.get("start")
            if isinstance(effective_period_by_key, Mapping)
            else None
        )
        if start:
            normalized_item_by_key["effective_period"] = {"start": start}
        else:
            normalized_item_by_key.pop("effective_period", None)
    return normalized_item_by_key


def normalize_projected_state_facts(profile: dict[str, Any]) -> None:
    """Upgrade legacy state facts while a corrected import is being published."""
    profile_sources = profile.get("sources")
    if not isinstance(profile_sources, list) or not any(
        isinstance(profile_source, Mapping)
        and profile_source.get("source_key") == "florida-mqa"
        for profile_source in profile_sources
    ):
        return
    categories = profile.get("categories")
    if not isinstance(categories, Mapping):
        return
    for category_group_by_key in categories.values():
        if not isinstance(category_group_by_key, dict):
            continue
        profile_items = category_group_by_key.get("items")
        if not isinstance(profile_items, list):
            continue
        normalized_items: list[dict[str, Any]] = []
        for profile_item in profile_items:
            if not isinstance(profile_item, Mapping):
                continue
            normalized_range_by_key = normalize_projected_reported_range(profile_item)
            if normalized_range_by_key is not None:
                normalized_items.append(
                    _normalize_legacy_effective_period(normalized_range_by_key)
                )
        category_group_by_key["items"] = normalized_items
        if (
            not normalized_items
            and category_group_by_key.get("availability") == "available"
        ):
            category_group_by_key["availability"] = "not_reported"
