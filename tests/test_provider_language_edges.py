# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Edge contracts for canonical provider-language normalization."""

from api.provider_language import normalize_language_value
from api.provider_language_merge import canonical_language_items


def test_language_normalizer_supports_trusted_edge_shapes():
    assert normalize_language_value(
        {"codes": [{"system": "urn:ietf:bcp:47", "code": "sl-rozaj-rozaj"}]}
    ) is None
    assert normalize_language_value({"coding": {"code": "French"}})[0] == (
        "code",
        "fr",
    )
    assert normalize_language_value({"code": "fr"})[0] == ("code", "fr")
    assert normalize_language_value("French")[0] == ("code", "fr")
    assert normalize_language_value(42) is None
    assert normalize_language_value(
        {"codes": [{"system": "urn:ietf:bcp:47", "code": "qaa"}]}
    )[1]["codes"][0]["display"] == "qaa"
    assert normalize_language_value(
        {
            "codes": [
                {
                    "system": "urn:ietf:bcp:47",
                    "code": "qaa",
                    "display": "SOME LANGUAGE",
                }
            ]
        }
    )[1]["codes"][0]["display"] == "Some Language"
    assert normalize_language_value({"text": "Creole", "preferred": True})[1][
        "preferred"
    ] is True
    assert normalize_language_value({"code": "fr", "preferred": True})[1][
        "preferred"
    ] is True


def test_language_merge_sanitizes_provenance_and_preserves_preferences():
    merged_language_items = canonical_language_items(
        [
            {
                "value": {
                    "codes": [
                        {"code": "fr", "display": "French"},
                        {"code": "es", "display": "Spanish"},
                    ]
                },
            },
            {
                "value": {"text": "French", "preferred": True},
                "assertions": [
                    42,
                    {
                        "source_kind": "state_regulator",
                        "assertion_type": "self_reported",
                    },
                ],
            },
            {
                "value": {"text": "German"},
                "assertions": "not-a-list",
            },
        ],
        fhir_source_rows=[],
    )
    french_language_item = next(
        language_item
        for language_item in merged_language_items
        if language_item["display"] == "French (fr)"
    )
    assert french_language_item["value"]["preferred"] is True
    assert french_language_item["value"]["normalization_warning"] == (
        "multiple_source_language_codes"
    )
    assert french_language_item["assertions"] == [
        {
            "source_kind": "state_regulator",
            "assertion_type": "self_reported",
        }
    ]
    german_language_item = next(
        language_item
        for language_item in merged_language_items
        if language_item["display"] == "German (de)"
    )
    assert "assertions" not in german_language_item
