# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import re
from typing import Optional

_TIER_PATTERN = re.compile(r"[^a-z0-9\s]")

TIER_SLUGS = (
    "tier_1",
    "tier_2",
    "tier_3",
    "tier_4",
    "tier_5",
    "tier_6",
    "generic",
    "preferred_generic",
    "non_preferred_generic",
    "brand",
    "preferred_brand",
    "non_preferred_brand",
    "specialty",
    "preferred_specialty",
    "non_preferred_specialty",
    "other",
    "unknown",
)


def normalize_drug_tier_slug(tier_label: Optional[str]) -> str:
    """
    Collapse free-form drug tier labels into a predictable slug.
    Unknown or unclassified tiers fall back to 'other'/'unknown'.
    """
    if not tier_label:
        return "unknown"
    lowered = tier_label.strip().lower()
    lowered = _TIER_PATTERN.sub(" ", lowered)
    if not lowered:
        return "unknown"
    tokens = [token for token in lowered.split() if token]
    if not tokens:
        return "unknown"

    def _has_tier_word(word: str) -> bool:
        return word in tokens

    # Tier 1-6 shortcuts (common in CMS data)
    if tokens[0] == "tier" and len(tokens) > 1 and tokens[1].isdigit():
        number = tokens[1]
        if number in {"1", "2", "3", "4", "5", "6"}:
            return f"tier_{number}"

    # Specialty tiers
    if _has_tier_word("specialty"):
        if _has_tier_word("preferred"):
            return "preferred_specialty"
        if (
            _has_tier_word("non")
            or _has_tier_word("nonpreferred")
            or "nonpreferred" in lowered
        ):
            return "non_preferred_specialty"
        return "specialty"

    # Generic tiers
    if _has_tier_word("generic"):
        if _has_tier_word("preferred"):
            return "preferred_generic"
        if _has_tier_word("non") or "nonpreferred" in lowered:
            return "non_preferred_generic"
        return "generic"

    # Brand tiers
    if _has_tier_word("brand"):
        if _has_tier_word("preferred"):
            return "preferred_brand"
        if _has_tier_word("non") or "nonpreferred" in lowered:
            return "non_preferred_brand"
        return "brand"

    # Catch-all: normalized slug of the first two tokens
    guessed = "_".join(tokens[:2])
    if guessed in TIER_SLUGS:
        return guessed

    return "other"
