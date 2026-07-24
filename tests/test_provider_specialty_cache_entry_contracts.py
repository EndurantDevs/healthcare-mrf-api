# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import pytest

from api import provider_specialty_cache_entries as cache_entries
from api.provider_specialty_filters import _specialty_key_variants


class _MappingResult:
    def __init__(self, mappings):
        self._mappings = mappings

    def mappings(self):
        return list(self._mappings)


class _SpecialtySession:
    def __init__(self, *, synonym_mappings=(), nucc_mappings=()):
        self._synonym_mappings = synonym_mappings
        self._nucc_mappings = nucc_mappings
        self.statements = []

    async def execute(self, statement):
        self.statements.append(statement)
        if "terminology_synonym" in str(statement):
            return _MappingResult(self._synonym_mappings)
        return _MappingResult(self._nucc_mappings)


def _nucc_mapping(
    code,
    *,
    classification="",
    specialization="",
    display_name="",
):
    return {
        "code": code,
        "classification": classification,
        "specialization": specialization,
        "display_name": display_name,
    }


_BULK_NUCC_MAPPINGS = [
    _nucc_mapping(
        " 100000000x ",
        classification="Family",
        display_name="Shared",
    ),
    _nucc_mapping(
        "200000000X",
        classification="Family",
        specialization="Subspecialty",
        display_name="Shared",
    ),
    _nucc_mapping(
        "300000000X",
        specialization="Subspecialty",
    ),
    _nucc_mapping(
        "400000000X",
        classification="Other",
    ),
    _nucc_mapping(
        "",
        classification="Ignored",
        specialization="Ignored",
        display_name="Ignored",
    ),
]

_BULK_SYNONYM_MAPPINGS = [
    {
        "synonym": "Shared",
        "target_system": "NUCC",
        "target_code": "900000000x",
        "metadata_json": None,
    },
    {
        "synonym": "shared",
        "target_system": "PROVIDER_TYPE",
        "target_code": "",
        "metadata_json": '{"nucc_code": "800000000x"}',
    },
    {
        "synonym": " ",
        "target_system": "NUCC",
        "target_code": "700000000X",
        "metadata_json": None,
    },
    {
        "synonym": "invalid-json",
        "target_system": "PROVIDER_TYPE",
        "target_code": "",
        "metadata_json": "{",
    },
    {
        "synonym": "non-object-json",
        "target_system": "PROVIDER_TYPE",
        "target_code": "",
        "metadata_json": "[]",
    },
    {
        "synonym": "missing-metadata",
        "target_system": "PROVIDER_TYPE",
        "target_code": "",
        "metadata_json": None,
    },
    {
        "synonym": "invalid-metadata-type",
        "target_system": "PROVIDER_TYPE",
        "target_code": "",
        "metadata_json": object(),
    },
    {
        "synonym": "missing-code",
        "target_system": "NUCC",
        "target_code": "",
        "metadata_json": None,
    },
]


def test_index_entry_skips_empty_code_sets_and_preserves_first_code_order():
    entries_by_variant = {}

    cache_entries._index_entry(
        entries_by_variant,
        lambda name: (name.lower(),),
        "Empty",
        [],
    )
    cache_entries._index_entry(
        entries_by_variant,
        lambda name: (name.lower(), f"{name.lower()}-variant"),
        "Family",
        ["200000000X", "100000000X", "200000000X"],
        ["100000000X", "100000000X"],
    )

    assert entries_by_variant == {
        "family": (
            ("200000000X", "100000000X"),
            ("100000000X",),
        ),
        "family-variant": (
            ("200000000X", "100000000X"),
            ("100000000X",),
        ),
    }


@pytest.mark.asyncio
async def test_bulk_loader_makes_precedence_and_invalid_row_handling_explicit():
    session = _SpecialtySession(
        nucc_mappings=_BULK_NUCC_MAPPINGS,
        synonym_mappings=_BULK_SYNONYM_MAPPINGS,
    )

    entries_by_variant = await cache_entries.load_dynamic_specialty_entries_by_variant(
        session,
        lambda value: (value.lower(),),
    )

    assert entries_by_variant["family"] == (
        ("100000000X", "200000000X"),
        ("100000000X",),
    )
    assert entries_by_variant["subspecialty"] == (
        ("200000000X", "300000000X"),
        ("200000000X", "300000000X"),
    )
    assert entries_by_variant["other"] == (
        ("400000000X",),
        ("400000000X",),
    )
    assert entries_by_variant["shared"] == (
        ("800000000X", "900000000X"),
        ("800000000X", "900000000X"),
    )
    assert "ignored" not in entries_by_variant
    assert "invalid-json" not in entries_by_variant
    assert "non-object-json" not in entries_by_variant
    assert "missing-metadata" not in entries_by_variant
    assert "invalid-metadata-type" not in entries_by_variant
    assert "missing-code" not in entries_by_variant


@pytest.mark.asyncio
async def test_single_entry_skips_database_when_the_term_has_no_key_variants():
    session = _SpecialtySession()

    resolved_entry = await cache_entries.load_dynamic_specialty_entry(
        session,
        lambda term: (),
        " ",
    )

    assert resolved_entry is None
    assert session.statements == []


@pytest.mark.asyncio
async def test_single_entry_uses_sorted_synonym_union_before_nucc_fallback():
    session = _SpecialtySession(
        synonym_mappings=[
            {
                "target_system": "NUCC",
                "target_code": "200000000x",
                "metadata_json": None,
            },
            {
                "target_system": "PROVIDER_TYPE",
                "target_code": "",
                "metadata_json": '{"nucc_code": "100000000x"}',
            },
            {
                "target_system": "PROVIDER_TYPE",
                "target_code": "",
                "metadata_json": None,
            },
        ],
        nucc_mappings=[
            _nucc_mapping(
                "900000000X",
                classification="Imaging",
            )
        ],
    )

    resolved_entry = await cache_entries.load_dynamic_specialty_entry(
        session,
        _specialty_key_variants,
        "Imaging",
    )

    assert resolved_entry == (
        ("100000000X", "200000000X"),
        ("100000000X", "200000000X"),
    )
    assert len(session.statements) == 1
    assert "terminology_synonym.term_key IN" in str(session.statements[0])


@pytest.mark.asyncio
async def test_single_entry_returns_none_when_both_database_tiers_miss():
    session = _SpecialtySession()

    resolved_entry = await cache_entries.load_dynamic_specialty_entry(
        session,
        _specialty_key_variants,
        "Unlisted Specialty",
    )

    assert resolved_entry is None
    assert len(session.statements) == 2
    nucc_statement = str(session.statements[1])
    assert "FROM mrf.nucc_taxonomy" in nucc_statement
    assert "regexp_replace" in nucc_statement


@pytest.mark.asyncio
async def test_classification_match_returns_all_codes_and_only_base_codes():
    session = _SpecialtySession(
        nucc_mappings=[
            _nucc_mapping(
                "200000000x",
                classification="Imaging",
                specialization="Focused Imaging",
                display_name="Imaging",
            ),
            _nucc_mapping(
                "100000000x",
                classification="Imaging",
                display_name="Imaging",
            ),
            _nucc_mapping(
                "900000000X",
                specialization="Imaging",
                display_name="Imaging",
            ),
        ]
    )

    resolved_entry = await cache_entries.load_dynamic_specialty_entry(
        session,
        _specialty_key_variants,
        "Imaging",
    )

    assert resolved_entry == (
        ("100000000X", "200000000X"),
        ("100000000X",),
    )


@pytest.mark.asyncio
async def test_classification_without_a_base_row_keeps_the_whole_family():
    session = _SpecialtySession(
        nucc_mappings=[
            _nucc_mapping(
                "200000000X",
                classification="Imaging",
                specialization="Focused Imaging",
            ),
            _nucc_mapping(
                "100000000X",
                classification="Imaging",
                specialization="General Imaging",
            ),
        ]
    )

    resolved_entry = await cache_entries.load_dynamic_specialty_entry(
        session,
        _specialty_key_variants,
        "Imaging",
    )

    assert resolved_entry == (
        ("100000000X", "200000000X"),
        ("100000000X", "200000000X"),
    )


@pytest.mark.asyncio
async def test_specialization_match_has_priority_over_display_name_match():
    session = _SpecialtySession(
        nucc_mappings=[
            _nucc_mapping(
                "100000000X",
                classification="Other",
                specialization="Imaging",
            ),
            _nucc_mapping(
                "900000000X",
                classification="Other",
                display_name="Imaging",
            ),
        ]
    )

    resolved_entry = await cache_entries.load_dynamic_specialty_entry(
        session,
        _specialty_key_variants,
        "Imaging",
    )

    assert resolved_entry == (("100000000X",), ("100000000X",))


@pytest.mark.asyncio
async def test_display_name_is_the_last_nucc_match_tier():
    session = _SpecialtySession(
        nucc_mappings=[
            _nucc_mapping(
                "100000000X",
                classification="Other",
                specialization="Different",
                display_name="Imaging",
            )
        ]
    )

    resolved_entry = await cache_entries.load_dynamic_specialty_entry(
        session,
        _specialty_key_variants,
        "Imaging",
    )

    assert resolved_entry == (("100000000X",), ("100000000X",))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "nucc_mappings",
    [
        [
            _nucc_mapping(
                "100000000X",
                classification="Other",
                specialization="Different",
                display_name="Unrelated",
            )
        ],
        [
            _nucc_mapping(
                "",
                display_name="Imaging",
            )
        ],
    ],
)
async def test_nucc_rows_without_a_usable_matching_code_resolve_to_none(
    nucc_mappings,
):
    session = _SpecialtySession(nucc_mappings=nucc_mappings)

    resolved_entry = await cache_entries.load_dynamic_specialty_entry(
        session,
        _specialty_key_variants,
        "Imaging",
    )

    assert resolved_entry is None


def test_python_term_normalization_matches_the_sql_ascii_contract():
    assert cache_entries._normalized_term_value("  Café / Imaging 42 ") == (
        "caf imaging 42"
    )
