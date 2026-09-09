# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Keep reported portfolio assertions separate from unchanged education facts."""

import copy
import hashlib
import json

import pytest

from process import massachusetts_profile_rows as rows
from api import provider_profile as profile_api
from api import provider_profile_states as state_api
from tests.test_massachusetts_profile_rows import EVIDENCE, PROFILE, parse
from tests.test_provider_profile_massachusetts import NPI, _row, _compose, _envelope, _legacy_envelope


BOARD = {
    "boardName": " Example Board ",
    "specialties": ["Internal Medicine", "Internal Medicine"],
    "subspecialties": ["Pulmonary Disease, Critical Care Medicine"],
}
PORTFOLIO = {
    **PROFILE,
    "boardCertifications": {"abms": [BOARD], "aoa": [BOARD]},
    "specialties": [" Internal Medicine, Pulmonary Disease ", "Internal Medicine", "Internal Medicine"],
    "acceptingNewPatient": "Yes", "acceptingMedicaid": "Yes",
}


def _parse(profile=PORTFOLIO, **options):
    return parse(copy.deepcopy(profile), categories=rows.PROFILE_CATEGORIES, **options)


def test_legacy_default_output_is_byte_stable_and_skips_unselected_sections():
    result = parse()
    assert hashlib.sha256(json.dumps(result, sort_keys=True, ensure_ascii=False).encode()).hexdigest() == (
        "a6b1c2748e70c0fa42ac6e322b0219a12ac4b842315753b4563ac1565e01609b"
    )
    profile_by_field = {**PROFILE, "boardCertifications": "malformed", "specialties": 42}
    legacy_record, legacy_facts = parse(profile_by_field)
    assert legacy_facts == result[1]
    assert legacy_record["raw_payload"] == profile_by_field
    assert legacy_record["normalized_payload"] == result[0]["normalized_payload"]


def test_board_objects_and_specialty_rows_preserve_original_support_without_inference():
    original = copy.deepcopy(PORTFOLIO)
    record, facts = _parse()
    assert PORTFOLIO == original and record["raw_payload"] == original
    assert facts[:2] == parse(PORTFOLIO)[1]
    certifications = facts[2:4]
    assert [fact["value_json"]["board_family"] for fact in certifications] == ["abms", "aoa"]
    assert len({fact["fact_id"] for fact in facts}) == 7
    for index, fact in enumerate(certifications):
        assert fact["fact_type"] == "board_certification"
        assert fact["value_json"] == {"certifying_board": "Example Board", "board_family": ("abms", "aoa")[index],
                                      "specialties": BOARD["specialties"], "subspecialties": BOARD["subspecialties"]}
        assert fact["source_json"]["source_path"] == f"boardCertifications.{('abms', 'aoa')[index]}[0]"
        assert fact["source_json"]["raw_fields"] == BOARD
        assert fact["display"] == "Example Board"
    assert [fact["value_json"] for fact in facts[4:]] == [
        {"text": "Internal Medicine, Pulmonary Disease"}, {"text": "Internal Medicine"}, {"text": "Internal Medicine"},
    ]
    for index, fact in enumerate(facts[4:]):
        assert fact["source_json"]["source_path"] == f"specialties[{index}]"
        assert fact["source_json"]["raw_fields"] == PORTFOLIO["specialties"][index]
    for fact in facts[2:]:
        assert fact["assertion_type"] == "self_reported" and fact["verification_status"] == "not_independently_verified"
        assert fact["source_json"]["content_sha256"] == EVIDENCE["content_sha256"]
        assert fact["effective_start"] is fact["effective_end"] is None
        assert fact["sensitive"] is False and fact["public_default"] is True
        assert not {"taxonomy", "certification_date", "expiration_date", "accepted"} & fact["value_json"].keys()


def test_repeated_boards_keep_distinct_paths_and_ids_across_runs():
    profile_by_field = {**PORTFOLIO, "boardCertifications": {"abms": [BOARD, BOARD]}}
    _, before = _parse(profile_by_field)
    _, after = _parse(profile_by_field, evidence={**EVIDENCE, "run_id": "next-synthetic-run"})
    assert before[2]["source_json"]["source_path"] == "boardCertifications.abms[0]"
    assert before[3]["source_json"]["source_path"] == "boardCertifications.abms[1]"
    assert len({fact["fact_id"] for fact in before}) == len(before)
    assert [fact["logical_fact_key"] for fact in before] == [fact["logical_fact_key"] for fact in after]
    assert not {fact["fact_id"] for fact in before} & {fact["fact_id"] for fact in after}


@pytest.mark.parametrize("changes", [{"isPendingReview": True}, {"licenseMetaId": 2}, {"licenseNumber": "999999"}])
def test_held_identity_or_pending_review_never_parses_portfolio(changes):
    record, facts = _parse({**PORTFOLIO, "boardCertifications": "malformed", "specialties": 42, **changes})
    assert facts == [] and record["normalized_payload"]["visibility"].startswith("held_")


@pytest.mark.parametrize("with_portfolio", [False, True])
def test_school_hidden_gate_applies_only_to_education_and_training(with_portfolio):
    profile_by_field = {**(PORTFOLIO if with_portfolio else PROFILE), "educationAndTrainings": {
        "education": {"name": ""}, "trainings": "hidden malformed section",
    }}
    record, facts = _parse(profile_by_field)
    assert {fact["category"] for fact in facts} == ({"certifications", "specialties"} if with_portfolio else set())
    assert record["normalized_payload"]["visibility"] == ("public" if with_portfolio else "education_section_hidden")
    if with_portfolio:
        assert record["normalized_payload"]["education_visibility"] == "education_section_hidden"
    assert record["raw_payload"] == profile_by_field


@pytest.mark.parametrize("section", [None, {}, {"abms": None, "aoa": None}, {"abms": [], "aoa": []}])
def test_absent_boards_and_specialties_do_not_invent_negative_facts(section):
    record, facts = _parse({**PROFILE, "boardCertifications": section, "specialties": None})
    assert facts == parse(PROFILE)[1] and record["normalized_payload"]["visibility"] == "public"


def test_absent_education_does_not_hide_public_portfolio():
    record, facts = _parse({**PORTFOLIO, "educationAndTrainings": None})
    assert len(facts) == 5 and record["normalized_payload"]["visibility"] == "public"


@pytest.mark.parametrize("changes", [
    {"boardCertifications": []}, {"boardCertifications": {"abms": {}}},
    {"boardCertifications": {"aoa": [None]}}, {"boardCertifications": {"abms": [{}]}},
    {"boardCertifications": {"abms": [{"boardName": 42}]}},
    {"boardCertifications": {"abms": [{**BOARD, "specialties": "Medicine"}]}},
    {"boardCertifications": {"abms": [{**BOARD, "subspecialties": [None]}]}},
    {"specialties": "Medicine"}, {"specialties": {}}, {"specialties": [42]}, {"specialties": [""]},
])
def test_malformed_selected_portfolio_fails_closed(changes):
    with pytest.raises(ValueError, match="massachusetts_profile_"):
        _parse({**PORTFOLIO, **changes})


@pytest.mark.parametrize("scope", [None, [], ["education"], ["certifications"], list(reversed(rows.PROFILE_CATEGORIES))])
def test_only_supported_frozen_scopes_are_accepted(scope):
    with pytest.raises(ValueError, match="categories_invalid"):
        parse(PORTFOLIO, categories=scope)


def _projection(profile=PORTFOLIO):
    _, facts = _parse(profile)
    template = _row()
    template["source_manifest"]["categories"] = list(rows.PROFILE_CATEGORIES)
    return state_api._state_projection(NPI, [{**template, **fact} for fact in facts])


def test_api_preserves_education_and_original_board_support_in_each_page():
    projection = _projection()
    envelope = state_api.merge_state_profile_projection(NPI, _legacy_envelope(), projection)
    before = _compose(_envelope(incumbent=_legacy_envelope()))
    after = _compose(envelope)
    assert after["schema_version"] == before["schema_version"]
    assert after["source_generations"] == before["source_generations"]
    assert after["important_context"][:len(before["important_context"])] == before["important_context"]
    assert "Certification validity" in " ".join(after["important_context"])
    for category in ("certifications", "specialties"):
        count = len(after["categories"][category]["items"])
        assert count > 0
        seen_fact_ids = set()
        for offset in range(count):
            page = _compose(envelope, requested_categories=[category], page_category=category, page_limit=1, page_offset=offset)
            item, = page["categories"][category]["items"]
            evidence = profile_api.compose_provider_profile_evidence(
                state_projection=envelope, fhir_evidence=None, provider_profile=page, page_category=category,
            )
            record, = evidence["sources"][rows.SOURCE_KEY]["records"]
            assert record["source_record_id"] in item["source_record_ids"]
            assert record["source_path"].startswith("boardCertifications." if category == "certifications" else "specialties[")
            assert record["fact_id"] not in seen_fact_ids
            seen_fact_ids.add(record["fact_id"])
            assert item["assertion_type"] == "self_reported" and item["verification_status"] == "not_independently_verified"
            assert evidence["sources"]["state_regulator"]["records"] == []


def test_api_uses_manifest_to_distinguish_unavailable_from_not_reported():
    legacy = _compose(_envelope())
    richer = _compose(state_api.merge_state_profile_projection(NPI, None, _projection(PROFILE)))
    for category in ("certifications", "specialties"):
        assert legacy["categories"][category] == {"availability": "unavailable", "items": [], "total": 0, "returned": 0, "truncated": False}
        assert richer["categories"][category] == {"availability": "not_reported", "items": [], "total": 0, "returned": 0, "truncated": False}
    assert richer["important_context"] == legacy["important_context"]


def test_legacy_api_manifest_rejects_new_fact_category():
    fact = _row("certification", category="certifications", fact_type="board_certification")
    with pytest.raises(RuntimeError, match="categories_invalid"):
        state_api._state_projection(NPI, [fact])
