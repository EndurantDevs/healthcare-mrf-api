# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicitly account for provider-only inputs without asserting an office match."""

import hashlib
import json
import re

COMPONENT = "registry_native_provider_no_offices"
POLICY = "native_provider_only_no_offices_v1"
_HASHES = ("recipe_sha256", "capture_sha256", "catalog_generation")
_ZERO_COUNTS = ("office_rows", "page_count", "matched_offices", "missing_offices")


def validate_native_provider_office_derivation(document):
    """Reject every address field and every nonzero office claim."""
    fields = {"component", "derivation_version", "office_policy", "source_rows", *_HASHES, *_ZERO_COUNTS}
    if (
        type(document) is not dict
        or set(document) != fields
        or document["component"] != COMPONENT
        or type(document["derivation_version"]) is not int
        or document["derivation_version"] != 4
        or document["office_policy"] != POLICY
        or type(document["source_rows"]) is not int
        or not 0 <= document["source_rows"] < 2**63
        or any(type(document[field]) is not int or document[field] != 0 for field in _ZERO_COUNTS)
        or any(
            type(document[field]) is not str or re.fullmatch(r"[0-9a-f]{64}", document[field]) is None
            for field in _HASHES
        )
    ):
        raise ValueError("registry_native_provider_offices_invalid")
    return json.loads(json.dumps(document, sort_keys=True))


def native_provider_office_derivation(recipe):
    """Provider facts and their mapping are verified separately from this zero-office contract."""
    from process.registry_native_provider_source import PinnedNativeProviderMembershipSource
    from process.registry_source_recipe_store import canonical_registry_source_recipes

    if type(recipe.source_pin) is not PinnedNativeProviderMembershipSource:
        raise ValueError("registry_native_provider_offices_invalid")
    pin = recipe.source_pin
    pin.__post_init__()
    return validate_native_provider_office_derivation(
        {
            "component": COMPONENT,
            "derivation_version": 4,
            "office_policy": POLICY,
            "recipe_sha256": hashlib.sha256(canonical_registry_source_recipes((recipe,)).encode()).hexdigest(),
            "capture_sha256": pin.generation_id,
            "catalog_generation": pin.catalog_generation,
            "source_rows": pin.capture["rows"],
            **dict.fromkeys(_ZERO_COUNTS, 0),
        }
    )


def validate_raw_or_native_office_derivation(document):
    """Keep the original raw contract distinct from a provider-only zero-office declaration."""
    if type(document) is dict and document.get("derivation_version") == 4:
        return validate_native_provider_office_derivation(document)
    from process.registry_source_office_summary_v3 import validate_derivation

    return validate_derivation(document)
