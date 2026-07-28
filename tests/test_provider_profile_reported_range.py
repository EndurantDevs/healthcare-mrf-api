from __future__ import annotations

from api.provider_profile import compose_provider_profile
from process.provider_profile_reported_range import (
    normalize_projected_reported_range,
    normalize_projected_state_facts,
    normalize_reported_range,
)


def test_reported_age_range_uses_age_semantics():
    assert normalize_reported_range("80 - 90") == {
        "fact_type": "age_range",
        "display": "Reported age range: 80–90 years",
        "value": {
            "minimum_years": 80,
            "maximum_years": 90,
            "precision": "range",
            "source_text": "80 - 90",
        },
    }


def test_reported_birth_year_range_retains_genuine_year_semantics():
    assert normalize_reported_range("1970-1975") == {
        "fact_type": "birth_year_range",
        "display": "Reported birth year range: 1970–1975",
        "value": {
            "start_year": 1970,
            "end_year": 1975,
            "precision": "range",
            "source_text": "1970-1975",
        },
    }


def test_unreported_and_invalid_ranges_are_not_published():
    assert normalize_reported_range("N/A") is None
    assert normalize_reported_range("") is None
    assert normalize_reported_range("90 - 80") is None
    assert normalize_reported_range("80 - 140") is None
    assert normalize_reported_range("1700 - 1750") is None
    assert normalize_reported_range("1970 - 2300") is None
    assert normalize_reported_range("not a range") is None


def test_legacy_projection_upgrade_removes_period_and_rekeys_fact():
    legacy_item_by_key = {
        "type": "birth_year_range",
        "logical_fact_key": "legacy-key",
        "display": "Birth year range: 80 - 90",
        "value": {"precision": "range", "source_text": "80 - 90"},
        "effective_period": {"start": "12/31/1973", "end": "01/31/2027"},
        "source_record_ids": ["record-a", "record-b"],
        "assertion_count": 2,
    }

    normalized = normalize_projected_reported_range(legacy_item_by_key)

    assert normalized is not None
    assert normalized["type"] == "age_range"
    assert normalized["display"] == "Reported age range: 80–90 years"
    assert normalized["value"]["minimum_years"] == 80
    assert "effective_period" not in normalized
    assert normalized["logical_fact_key"] != "legacy-key"
    assert len(normalized["logical_fact_key"]) == 64
    assert normalized["source_record_ids"] == ["record-a", "record-b"]


def test_projection_range_upgrade_preserves_unrelated_and_rejects_invalid():
    unrelated_item_by_key = {"type": "name", "value": {"text": "Alex Example"}}

    assert normalize_projected_reported_range(unrelated_item_by_key) == (
        unrelated_item_by_key
    )
    assert normalize_projected_reported_range(
        {"type": "birth_year_range", "value": "N/A"}
    ) is None


def test_composer_upgrades_existing_state_projection_without_reimport():
    state_profile_by_key = {
        "generation_id": "state-generation",
        "sources": [{"source_key": "florida-mqa"}],
        "categories": {
            "demographics": {
                "availability": "available",
                "items": [
                    {
                        "type": "birth_year_range",
                        "display": "Birth year range: 80 - 90",
                        "value": {
                            "precision": "range",
                            "source_text": "80 - 90",
                        },
                        "effective_period": {
                            "start": "12/31/1973",
                            "end": "01/31/2027",
                        },
                        "source_record_id": "record-a",
                        "source_record_ids": ["record-a"],
                        "source_kinds": ["state_regulator"],
                        "assertion_type": "state_reported",
                        "verification_status": "government_source",
                        "assertion_count": 1,
                        "sensitive": False,
                        "public_default": True,
                    }
                ],
            }
        },
    }

    profile = compose_provider_profile(
        1295763977,
        state_projection={
            "generation_id": "state-generation",
            "profile": state_profile_by_key,
        },
        fhir_profile=None,
    )

    assert profile is not None
    profile_item = profile["categories"]["demographics"]["items"][0]
    assert profile_item["type"] == "age_range"
    assert profile_item["display"] == "Reported age range: 80–90 years"
    assert "effective_period" not in profile_item


def test_projection_upgrade_omits_not_applicable_range():
    profile_by_key = {
        "sources": [{"source_key": "florida-mqa"}],
        "categories": {
            "demographics": {
                "availability": "available",
                "items": [
                    {
                        "type": "birth_year_range",
                        "value": {"source_text": "N/A"},
                    }
                ],
            }
        }
    }

    normalize_projected_state_facts(profile_by_key)

    assert profile_by_key["categories"]["demographics"] == {
        "availability": "not_reported",
        "items": [],
    }


def test_state_compatibility_removes_profile_master_period_leaks():
    leaked_period_by_key = {"start": "1973-12-31", "end": "2027-01-31"}
    fact_types = (
        "name",
        "provider_address",
        "other_state_license_indicator",
        "nica_assessment_status",
        "practice_start",
        "state_license",
    )
    profile_by_key = {
        "sources": [{"source_key": "florida-mqa"}],
        "categories": {
            fact_type: {
                "availability": "available",
                "items": [
                    {
                        "type": fact_type,
                        "value": {"source": "legacy"},
                        "effective_period": dict(leaked_period_by_key),
                    }
                ],
            }
            for fact_type in fact_types
        }
    }

    normalize_projected_state_facts(profile_by_key)

    item_by_type = {
        fact_type: profile_by_key["categories"][fact_type]["items"][0]
        for fact_type in fact_types
    }
    for fact_type in fact_types[:4]:
        assert "effective_period" not in item_by_type[fact_type]
    assert item_by_type["practice_start"]["effective_period"] == {
        "start": "1973-12-31"
    }
    assert item_by_type["state_license"]["effective_period"] == leaked_period_by_key


def test_state_compatibility_does_not_change_other_state_periods():
    profile_by_key = {
        "sources": [{"source_key": "another-state-regulator"}],
        "categories": {
            "identity": {
                "availability": "available",
                "items": [
                    {
                        "type": "name",
                        "effective_period": {
                            "start": "2020-01-01",
                            "end": "2024-12-31",
                        },
                    }
                ],
            }
        },
    }

    normalize_projected_state_facts(profile_by_key)

    assert profile_by_key["categories"]["identity"]["items"][0][
        "effective_period"
    ] == {"start": "2020-01-01", "end": "2024-12-31"}


def test_state_compatibility_tolerates_incomplete_legacy_documents():
    normalize_projected_state_facts({})
    normalize_projected_state_facts({"sources": "not-a-list"})
    normalize_projected_state_facts(
        {"sources": [{"source_key": "florida-mqa"}], "categories": []}
    )
    profile_by_key = {
        "sources": [{"source_key": "florida-mqa"}],
        "categories": {
            "not_a_group": [],
            "not_items": {"availability": "unavailable", "items": {}},
            "mixed": {
                "availability": "available",
                "items": [
                    None,
                    {
                        "type": "practice_start",
                        "effective_period": {"end": "2027-01-31"},
                    },
                ],
            },
        },
    }

    normalize_projected_state_facts(profile_by_key)

    assert profile_by_key["categories"]["not_items"]["items"] == {}
    assert profile_by_key["categories"]["mixed"] == {
        "availability": "available",
        "items": [{"type": "practice_start"}],
    }
