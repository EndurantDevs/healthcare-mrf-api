# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import pytest

from api import provider_specialty_filters as psf
from api.provider_specialty_filters import (
    SpecialtyResolutionCache,
    _normalize_specialty_key,
    _specialty_key_variants,
    resolve_provider_specialty_filter,
)


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return list(self._rows)


class _FakeSession:
    def __init__(self, nucc_rows=None, synonym_rows=None):
        self.nucc_rows = nucc_rows or []
        self.synonym_rows = synonym_rows or []
        self.execute_calls = 0

    async def execute(self, statement):
        self.execute_calls += 1
        if "terminology_synonym" in str(statement):
            return _FakeResult(self.synonym_rows)
        return _FakeResult(self.nucc_rows)


NUCC_ROWS = [
    {"code": "2085R0202X", "classification": "Radiology", "specialization": "Diagnostic Radiology", "display_name": "Diagnostic Radiology Physician"},
    {"code": "2085N0700X", "classification": "Radiology", "specialization": "Neuroradiology", "display_name": "Neuroradiology Physician"},
    {"code": "2084P0800X", "classification": "Psychiatry & Neurology", "specialization": "Psychiatry", "display_name": "Psychiatrist"},
    {"code": "2084N0400X", "classification": "Psychiatry & Neurology", "specialization": "Neurology", "display_name": "Neurologist"},
    {"code": "207K00000X", "classification": "Allergy & Immunology", "specialization": "", "display_name": "Allergy & Immunology Physician"},
    {"code": "208800000X", "classification": "Urology", "specialization": "", "display_name": "Urologist"},
    {"code": "2088P0231X", "classification": "Urology", "specialization": "Pediatric Urology", "display_name": "Pediatric Urology Physician"},
    {"code": "207N00000X", "classification": "Dermatology", "specialization": "", "display_name": "Dermatologist"},
    {"code": "207NS0135X", "classification": "Dermatology", "specialization": "MOHS-Micrographic Surgery", "display_name": "MOHS-Micrographic Surgery Physician"},
]

SYNONYM_ROWS = [
    {"synonym": "heart doctor", "target_system": "NUCC", "target_code": "207RC0000X", "metadata_json": None},
    {"synonym": "Cardiology", "target_system": "PROVIDER_TYPE", "target_code": "Cardiology", "metadata_json": '{"nucc_code": "207RC0000X"}'},
]


async def _loaded_cache():
    cache = SpecialtyResolutionCache()
    await cache.ensure(_FakeSession(NUCC_ROWS, SYNONYM_ROWS))
    return cache


def test_normalize_specialty_key_matches_terminology_term_key_semantics():
    assert _normalize_specialty_key("Obstetrics & Gynecology") == "obstetrics gynecology"
    assert _normalize_specialty_key("OB/GYN") == "ob gyn"
    assert _normalize_specialty_key("  Family   Medicine ") == "family medicine"


def test_specialty_key_variants_bridge_and_spellings():
    assert _specialty_key_variants("obstetrics and gynecology") == (
        "obstetrics and gynecology",
        "obstetrics gynecology",
    )
    assert _specialty_key_variants("Obstetrics & Gynecology") == ("obstetrics gynecology",)


def test_obgyn_variants_resolve_from_static_tier():
    for spelling in (
        "obgyn",
        "ob-gyn",
        "OB/GYN",
        "obstetrics & gynecology",
        "Obstetrics & Gynecology",
        "obstetrics and gynecology",
        "gynecology",
        "obstetrics",
    ):
        resolved = resolve_provider_specialty_filter({"specialty": spelling})
        assert resolved.active, spelling
        assert "207V00000X" in resolved.taxonomy_codes, spelling


def test_unknown_specialty_is_inactive_with_unresolved_marker():
    resolved = resolve_provider_specialty_filter({"specialty": "definitely not a specialty"})
    assert not resolved.active
    assert resolved.unresolved_specialty == "definitely not a specialty"


def test_typo_gets_suggestions():
    resolved = resolve_provider_specialty_filter({"specialty": "dermatolgy"})
    assert not resolved.active
    assert any("dermatology" in suggestion for suggestion in resolved.suggested_specialties)


def test_explicit_taxonomy_codes_win():
    resolved = resolve_provider_specialty_filter(
        {"specialty": "something unknown", "taxonomy_codes": "207V00000X"}
    )
    assert resolved.taxonomy_codes == ("207V00000X",)
    assert resolved.unresolved_specialty is None


@pytest.mark.asyncio
async def test_nucc_tier_resolves_specializations_and_families():
    cache = await _loaded_cache()
    # Specialization-level term (a8fd216 review finding: used to silently empty)
    assert cache.lookup("Psychiatry") == ("2084P0800X",)
    # Classification family with no base row (Radiology) resolves to all family codes
    assert set(cache.lookup("Radiology")) == {"2085R0202X", "2085N0700X"}
    # "&" classification reachable via "and" spelling and vice versa
    assert cache.lookup("allergy and immunology") == ("207K00000X",)
    assert cache.lookup("Allergy & Immunology") == ("207K00000X",)
    assert set(cache.lookup("Urology")) == {"208800000X", "2088P0231X"}


@pytest.mark.asyncio
async def test_include_subspecialties_false_uses_family_base_codes():
    cache = await _loaded_cache()
    assert cache.lookup("Urology", include_subspecialties=False) == ("208800000X",)
    # Families with no base (specialization-empty) row keep the whole family
    # rather than going empty.
    assert set(cache.lookup("Radiology", include_subspecialties=False)) == {"2085R0202X", "2085N0700X"}
    # A directly-named specialization is the request itself.
    assert cache.lookup("Psychiatry", include_subspecialties=False) == ("2084P0800X",)


@pytest.mark.asyncio
async def test_colliding_synonym_rows_union_deterministically():
    colliding_rows = [
        {"synonym": "Radiology", "target_system": "NUCC", "target_code": "2085N0700X", "metadata_json": None},
        {"synonym": "radiology", "target_system": "PROVIDER_TYPE", "target_code": "Diagnostic Radiology", "metadata_json": '{"nucc_code": "2085R0202X"}'},
    ]
    for ordering in (colliding_rows, list(reversed(colliding_rows))):
        cache = SpecialtyResolutionCache()
        await cache.ensure(_FakeSession(NUCC_ROWS, ordering))
        assert cache.lookup("radiology") == ("2085N0700X", "2085R0202X"), ordering


@pytest.mark.asyncio
async def test_terminology_synonym_tier_resolves_colloquial_aliases():
    cache = await _loaded_cache()
    assert cache.lookup("heart doctor") == ("207RC0000X",)


@pytest.mark.asyncio
async def test_static_curated_tier_wins_over_nucc_family():
    cache = await _loaded_cache()
    # Static dict pins dermatology to the base code; NUCC family would add MOHS.
    assert cache.lookup("dermatology") == ("207N00000X",)


@pytest.mark.asyncio
async def test_cache_ttl_prevents_reload_within_window():
    cache = SpecialtyResolutionCache()
    session = _FakeSession(NUCC_ROWS, SYNONYM_ROWS)
    await cache.ensure(session)
    first_calls = session.execute_calls
    await cache.ensure(session)
    assert session.execute_calls == first_calls


@pytest.mark.asyncio
async def test_cache_failure_keeps_static_tier():
    class _BrokenSession:
        async def execute(self, statement):
            raise RuntimeError("db down")

    cache = SpecialtyResolutionCache()
    await cache.ensure(_BrokenSession())
    assert cache.lookup("dermatology") == ("207N00000X",)
    assert cache.lookup("Urology") == ()


@pytest.mark.asyncio
async def test_resolver_uses_loaded_shared_cache(monkeypatch):
    cache = await _loaded_cache()
    monkeypatch.setattr(psf, "_SPECIALTY_RESOLUTION_CACHE", cache)
    resolved = resolve_provider_specialty_filter({"specialty": "Psychiatry"})
    assert resolved.active
    assert resolved.taxonomy_codes == ("2084P0800X",)
    assert resolved.unresolved_specialty is None


def test_seed_rows_mirror_static_alias_dict():
    from process.terminology_synonyms import _specialty_alias_rows

    specialty_rows = _specialty_alias_rows()
    seeded_alias_pairs = {(specialty_row["synonym"], specialty_row["target_code"]) for specialty_row in specialty_rows}
    for alias, codes in psf._SPECIALTY_TAXONOMY_CODE_ALIASES.items():
        for code in codes:
            assert (alias, code) in seeded_alias_pairs
    assert all(specialty_row["target_system"] == "NUCC" for specialty_row in specialty_rows)
    assert all(specialty_row["domain"] == "provider_type" for specialty_row in specialty_rows)
