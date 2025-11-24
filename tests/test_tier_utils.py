# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import pytest

from api.tier_utils import normalize_drug_tier_slug


def test_normalize_tier_unknown_inputs():
    assert normalize_drug_tier_slug(None) == "unknown"
    assert normalize_drug_tier_slug("") == "unknown"
    assert normalize_drug_tier_slug("   ") == "unknown"
    assert normalize_drug_tier_slug("!!!") == "unknown"


def test_normalize_numeric_tiers():
    assert normalize_drug_tier_slug("Tier 1") == "tier_1"
    assert normalize_drug_tier_slug("tier 3 something") == "tier_3"


def test_normalize_specialty_tiers():
    assert normalize_drug_tier_slug("Preferred Specialty") == "preferred_specialty"
    assert normalize_drug_tier_slug("Non Specialty") == "non_preferred_specialty"
    assert normalize_drug_tier_slug("Specialty") == "specialty"


def test_normalize_generic_tiers():
    assert normalize_drug_tier_slug("Generic") == "generic"
    assert normalize_drug_tier_slug("Preferred Generic") == "preferred_generic"
    assert normalize_drug_tier_slug("Non Generic") == "non_preferred_generic"


def test_normalize_brand_tiers():
    assert normalize_drug_tier_slug("Brand") == "brand"
    assert normalize_drug_tier_slug("Preferred Brand") == "preferred_brand"
    assert normalize_drug_tier_slug("nonpreferred brand") == "non_preferred_brand"


def test_normalize_fallback_slug():
    assert normalize_drug_tier_slug("Unknown") == "unknown"
    assert normalize_drug_tier_slug("other tier") == "other"
