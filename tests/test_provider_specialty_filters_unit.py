# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from api.provider_specialty_filters import (
    _normalize_specialty_key,
    resolve_provider_specialty_filter,
)


def test_normalize_specialty_key_collapses_separators():
    assert _normalize_specialty_key("Obstetrics & Gynecology") == "obstetrics and gynecology"
    assert _normalize_specialty_key("obstetrics/gynecology") == "obstetrics gynecology"
    assert _normalize_specialty_key("OB-GYN") == "ob gyn"
    assert _normalize_specialty_key("OB/GYN") == "ob gyn"
    assert _normalize_specialty_key("  Family   Medicine ") == "family medicine"


def test_obgyn_variants_resolve_to_taxonomy_codes():
    for spelling in (
        "obgyn",
        "ob-gyn",
        "OB/GYN",
        "obstetrics & gynecology",
        "Obstetrics & Gynecology",
        "obstetrics and gynecology",
        "gynecology",
        "gynecologist",
        "obstetrics",
    ):
        resolved = resolve_provider_specialty_filter({"specialty": spelling})
        assert resolved.active, spelling
        assert "207V00000X" in resolved.taxonomy_codes, spelling


def test_known_aliases_still_resolve_after_normalization():
    resolved = resolve_provider_specialty_filter({"specialty": "Primary Care"})
    assert "207Q00000X" in resolved.taxonomy_codes
    resolved = resolve_provider_specialty_filter({"specialty": "internal medicine"})
    assert resolved.active


def test_unknown_specialty_falls_back_to_classification_predicate():
    resolved = resolve_provider_specialty_filter({"specialty": "Urology"})
    assert resolved.active
    assert resolved.classification == "Urology"
    assert resolved.taxonomy_codes == ()
    assert resolved.use_classification_predicate is True


def test_empty_specialty_stays_inactive():
    resolved = resolve_provider_specialty_filter({"specialty": ""})
    assert not resolved.active
    resolved = resolve_provider_specialty_filter({})
    assert not resolved.active


def test_explicit_taxonomy_codes_win_over_fallback():
    resolved = resolve_provider_specialty_filter(
        {"specialty": "something unknown", "taxonomy_codes": "207V00000X"}
    )
    assert resolved.taxonomy_codes == ("207V00000X",)
    assert resolved.classification is None
