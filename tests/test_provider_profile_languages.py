# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Provider-level language normalization and provenance contracts."""

from pathlib import Path

from api.provider_language import normalize_language_value
from api.provider_profile import (
    compose_provider_profile,
    compose_provider_profile_evidence,
)
from process.florida_mqa_profile import PROFILE_SCHEMA_VERSION, STANDARD_CATEGORIES


def _state_profile(language: str | None) -> dict:
    categories_by_name = {
        category: {"availability": "unavailable", "items": []}
        for category in STANDARD_CATEGORIES
    }
    categories_by_name["languages"] = {
        "availability": "available" if language else "not_reported",
        "items": (
            [
                {
                    "type": "spoken_language",
                    "display": f"Language: {language}",
                    "value": {"language": language},
                    "assertion_type": "self_reported",
                    "verification_status": "not_independently_verified",
                    "source_record_id": "state-language-record",
                    "source_record_ids": ["state-language-record"],
                    "source_kinds": ["state_regulator"],
                    "assertions": [
                        {
                            "source_kind": "state_regulator",
                            "assertion_type": "self_reported",
                            "verification_status": "not_independently_verified",
                        }
                    ],
                    "assertion_count": 1,
                    "sensitive": False,
                    "public_default": True,
                }
            ]
            if language
            else []
        ),
    }
    return {
        "schema_version": PROFILE_SCHEMA_VERSION,
        "npi": 1000000004,
        "categories": categories_by_name,
        "sources": [],
    }


def _fhir_language_fact(
    language_value_by_field: dict,
    source_id: str = "directory-one",
) -> dict:
    return {
        "value": language_value_by_field,
        "source_ids": [source_id],
        "source_count": 1,
        "independent_source_count": 1,
    }


def _fhir_profile_with_duplicate_french() -> dict:
    return {
        "generation_id": "fhir-generation",
        "facts": {
            "language": {
                "items": [
                    _fhir_language_fact({"text": "French"}),
                    _fhir_language_fact(
                        {
                            "codes": [
                                {
                                    "system": "urn:ietf:bcp:47",
                                    "code": "fr",
                                    "display": "French",
                                }
                            ]
                        }
                    ),
                    _fhir_language_fact(
                        {
                            "codes": [
                                {
                                    "system": "urn:ietf:bcp:47",
                                    "code": "en",
                                    "display": "English",
                                }
                            ]
                        }
                    ),
                ],
                "total": 3,
                "truncated": False,
            }
        },
        "sources": [
            {
                "source_id": "directory-one",
                "endpoint_id": "endpoint-one",
            }
        ],
    }


def _assert_canonical_french(profile_by_field: dict) -> dict:
    language_group_by_field = profile_by_field["categories"]["languages"]
    assert language_group_by_field["total"] == 2
    french_fact_by_field = next(
        language_fact
        for language_fact in language_group_by_field["items"]
        if language_fact["display"] == "French (fr)"
    )
    assert french_fact_by_field["type"] == "language"
    assert french_fact_by_field["value"] == {
        "codes": [
            {
                "system": "urn:ietf:bcp:47",
                "code": "fr",
                "display": "French",
            }
        ]
    }
    assert french_fact_by_field["source_kinds"] == [
        "provider_directory_fhir",
        "state_regulator",
    ]
    assert french_fact_by_field["source_ids"] == ["directory-one"]
    assert french_fact_by_field["source_count"] == 2
    assert french_fact_by_field["independent_source_count"] == 2
    assert french_fact_by_field["assertion_count"] == 2
    assert len(french_fact_by_field["assertions"]) == 2
    state_assertion_by_field = next(
        assertion_by_field
        for assertion_by_field in french_fact_by_field["assertions"]
        if assertion_by_field["source_kind"] == "state_regulator"
    )
    assert french_fact_by_field["assertion_type"] == "self_reported"
    assert french_fact_by_field["verification_status"] == (
        "not_independently_verified"
    )
    assert state_assertion_by_field["assertion_type"] == "self_reported"
    assert state_assertion_by_field["verification_status"] == (
        "not_independently_verified"
    )
    return french_fact_by_field


def test_language_composer_semantically_deduplicates_source_shapes():
    """Merge equivalent source shapes without inflating support counts."""
    state_profile_by_field = _state_profile("FRENCH")
    profile_by_field = compose_provider_profile(
        1000000004,
        state_projection={"profile": state_profile_by_field},
        fhir_profile=_fhir_profile_with_duplicate_french(),
        requested_categories=["languages"],
    )
    french_fact_by_field = _assert_canonical_french(profile_by_field)
    state_only_profile_by_field = compose_provider_profile(
        1000000004,
        state_projection={"profile": state_profile_by_field},
        fhir_profile=None,
        requested_categories=["languages"],
    )
    assert (
        french_fact_by_field["item_id"]
        == state_only_profile_by_field["categories"]["languages"]["items"][0][
            "item_id"
        ]
    )


def test_language_normalizer_preserves_valid_script_and_region_subtags():
    identity, value = normalize_language_value(
        {
            "codes": [
                {
                    "system": "urn:ietf:bcp:47",
                    "code": "zh_hant_tw",
                    "display": "Traditional Chinese",
                }
            ]
        }
    )

    assert identity == ("code", "zh-Hant-TW")
    assert value["codes"][0] == {
        "system": "urn:ietf:bcp:47",
        "code": "zh-Hant-TW",
        "display": "Chinese",
    }


def test_language_normalizer_resolves_reviewed_legacy_codes_and_drops_unknown():
    identity, value = normalize_language_value(
        {
            "codes": [
                {
                    "system": "http://hl7.org/fhir/ValueSet/languages",
                    "code": "SPAN",
                }
            ]
        }
    )

    assert identity == ("code", "es")
    assert value["codes"][0]["display"] == "Spanish"
    assert normalize_language_value({"codes": [{"code": "UNK"}]}) is None
    assert normalize_language_value({"text": "unknown"}) is None
    assert normalize_language_value({"text": "140"}) is None
    assert normalize_language_value({"text": "140.0"}) is None
    assert normalize_language_value({"text": "140 - 141"}) is None


def test_language_normalizer_rejects_malformed_or_untrusted_codes():
    rejected_codes = (
        ("urn:ietf:bcp:47", "en-u"),
        ("urn:ietf:bcp:47", "en-US-Latn"),
        ("urn:ietf:bcp:47", "english-abc"),
        ("urn:ietf:bcp:47", "en-a-foo-a-bar"),
        ("https://codes.example.invalid/language", "qz"),
    )
    for code_system, language_code in rejected_codes:
        assert normalize_language_value(
            {
                "codes": [
                    {
                        "system": code_system,
                        "code": language_code,
                    }
                ]
            }
        ) is None


def test_language_normalizer_marks_incompatible_source_representations():
    _identity, conflicting_codes_by_field = normalize_language_value(
        {
            "codes": [
                {"code": "fr", "display": "French"},
                {"code": "es", "display": "Spanish"},
            ]
        }
    )
    _identity, conflicting_labels_by_field = normalize_language_value(
        {
            "text": "English",
            "display": "Spanish",
        }
    )
    _identity, mixed_conflict_by_field = normalize_language_value(
        {
            "text": "French",
            "display": "Spanish",
            "codes": [{"system": "urn:ietf:bcp:47", "code": "fr"}],
        }
    )

    assert conflicting_codes_by_field["normalization_warning"] == (
        "multiple_source_language_codes"
    )
    assert conflicting_labels_by_field["normalization_warning"] == (
        "multiple_source_language_codes"
    )
    assert mixed_conflict_by_field["normalization_warning"] == (
        "multiple_source_language_codes"
    )


def test_language_composer_keeps_ambiguous_labels_as_readable_text():
    profile_by_field = compose_provider_profile(
        1000000004,
        state_projection={"profile": _state_profile("CAPE VERDEAN CREOLE")},
        fhir_profile={
            "facts": {
                "language": {
                    "items": [
                        _fhir_language_fact(
                            {"text": "  cape   verdean creole  "}
                        ),
                    ]
                }
            },
            "sources": [],
        },
        requested_categories=["languages"],
    )

    language_fact_by_field = profile_by_field["categories"]["languages"]["items"][0]
    assert language_fact_by_field["display"] == "Cape Verdean Creole"
    assert language_fact_by_field["value"] == {"text": "Cape Verdean Creole"}
    assert "codes" not in language_fact_by_field["value"]


def test_language_code_wins_a_source_code_display_conflict_without_hiding_it():
    profile_by_field = compose_provider_profile(
        1000000004,
        state_projection=None,
        fhir_profile={
            "facts": {
                "language": {
                    "items": [
                        _fhir_language_fact(
                            {
                                "codes": [
                                    {
                                        "system": "urn:ietf:bcp:47",
                                        "code": "fr",
                                        "display": "Spanish",
                                    }
                                ]
                            }
                        )
                    ]
                }
            },
            "sources": [],
        },
        requested_categories=["languages"],
    )

    language_fact_by_field = profile_by_field["categories"]["languages"]["items"][0]
    assert language_fact_by_field["display"] == "French (fr)"
    assert language_fact_by_field["value"]["normalization_warning"] == (
        "source_code_display_mismatch"
    )


def test_language_evidence_retains_each_raw_semantic_variant():
    fhir_profile_by_field = {
        "facts": {
            "language": {
                "items": [
                    _fhir_language_fact({"text": "French"}),
                    _fhir_language_fact(
                        {
                            "codes": [
                                {
                                    "system": "urn:ietf:bcp:47",
                                    "code": "fr",
                                    "display": "French",
                                }
                            ]
                        }
                    ),
                ],
                "total": 2,
            }
        },
        "sources": [],
    }
    profile_by_field = compose_provider_profile(
        1000000004,
        state_projection=None,
        fhir_profile=fhir_profile_by_field,
        requested_categories=["languages"],
    )
    evidence_by_field = compose_provider_profile_evidence(
        state_projection=None,
        fhir_evidence=fhir_profile_by_field,
        provider_profile=profile_by_field,
        page_category="languages",
    )

    raw_language_facts = evidence_by_field["sources"]["provider_directory_fhir"]["facts"][
        "language"
    ]["items"]
    assert [language_fact["value"] for language_fact in raw_language_facts] == [
        {"text": "French"},
        {
            "codes": [
                {
                    "system": "urn:ietf:bcp:47",
                    "code": "fr",
                    "display": "French",
                }
            ]
        },
    ]


def test_empty_language_assertion_does_not_turn_unknown_into_available():
    profile_by_field = compose_provider_profile(
        1000000004,
        state_projection={"profile": _state_profile(None)},
        fhir_profile={
            "facts": {
                "language": {
                    "items": [{"value": {}, "source_ids": ["directory-one"]}]
                }
            },
            "sources": [],
        },
        requested_categories=["languages"],
    )

    assert profile_by_field["categories"]["languages"]["availability"] == (
        "not_reported"
    )
    assert profile_by_field["categories"]["languages"]["items"] == []


def test_nonpositive_state_language_becomes_not_reported():
    for state_label in ("UNKNOWN", "140"):
        profile_by_field = compose_provider_profile(
            1000000004,
            state_projection={"profile": _state_profile(state_label)},
            fhir_profile=None,
            requested_categories=["languages"],
        )

        assert profile_by_field["categories"]["languages"]["availability"] == (
            "not_reported"
        )
        assert profile_by_field["categories"]["languages"]["items"] == []


def test_fhir_coverage_without_language_is_not_reported():
    profile_by_field = compose_provider_profile(
        1000000004,
        state_projection=None,
        fhir_profile={
            "facts": {},
            "sources": [
                {
                    "source_id": "directory-one",
                    "endpoint_id": "endpoint-one",
                }
            ],
        },
        requested_categories=["languages"],
    )

    assert profile_by_field["categories"]["languages"]["availability"] == (
        "not_reported"
    )
    assert profile_by_field["categories"]["languages"]["items"] == []


def test_language_assertion_count_uses_distinct_fhir_resource_evidence():
    fhir_fact_by_field = _fhir_language_fact({"text": "French"})
    fhir_fact_by_field["evidence_count"] = 3
    profile_by_field = compose_provider_profile(
        1000000004,
        state_projection=None,
        fhir_profile={
            "facts": {"language": {"items": [fhir_fact_by_field]}},
            "sources": [],
        },
        requested_categories=["languages"],
    )

    assert (
        profile_by_field["categories"]["languages"]["items"][0][
            "assertion_count"
        ]
        == 3
    )


def test_compact_fhir_projection_carries_resource_evidence_count():
    aggregate_sql = (
        Path(__file__).parents[1]
        / "process"
        / "sql"
        / "provider_directory_profile_aggregate.sql"
    ).read_text()
    compact_projection = aggregate_sql.split("AS compact_items", 1)[0]

    assert "'evidence_count', evidence_count" in compact_projection
