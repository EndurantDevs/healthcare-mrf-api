from __future__ import annotations

import copy
from itertools import permutations

import pytest

from api.provider_education import canonicalize_education_category
from api.provider_profile import compose_provider_profile, compose_provider_profile_evidence


SCHOOL = "Example Medical School"
YEAR_VALUE = {"institution": SCHOOL, "graduation_year": 2001}
DATE_VALUE = {"institution": SCHOOL.upper(), "graduation_date": "2001-06-01", "graduation_date_precision": "day", "program": "Medicine", "degree_or_certificate_code": "MD"}


def _fact(record_id, value, *, source_kind="state_regulator", **overrides):
    return {
        "type": "education_history", "value": copy.deepcopy(value),
        "display": f"Source education: {record_id}",
        "source_kinds": [source_kind], "source_record_ids": [record_id],
        "assertion_type": "reported", "verification_status": "not_independently_verified",
        "assertion_count": 1, "public_default": True, "sensitive": False,
        **overrides,
    }


def _canonical(*facts):
    category_by_field = {"items": list(facts)}
    canonicalize_education_category(category_by_field)
    return sorted(category_by_field["items"], key=lambda fact: fact.get("logical_fact_key", ""))


def _assertion(fact, source_kind):
    assertions = [item for item in fact["assertions"] if item["source_kind"] == source_kind]
    assert len(assertions) == 1
    return assertions[0]


def _projection(*facts):
    return {
        "generation_id": "synthetic-state-generation",
        "profile": {"categories": {"education": {"items": list(facts), "availability": "available"}}},
        "evidence": {"records": [{"source_record_id": record_id} for fact in facts if "state_regulator" in fact["source_kinds"] for record_id in fact["source_record_ids"]]},
        "cms_evidence": {"records": [{"source_record_id": record_id} for fact in facts if "cms_doctors" in fact["source_kinds"] for record_id in fact["source_record_ids"]]},
    }


def _compose(projection, **options):
    return compose_provider_profile(1000000004, state_projection=projection, fhir_profile=None, **options)


def test_correlated_school_and_year_keep_original_richer_value_and_assertions():
    rich_value_by_field = {**DATE_VALUE, "major": "Medicine", "attendance_start": "1997-09-01", "institution_identifier": "state-school-1"}
    state_fact = _fact("state-1", rich_value_by_field)
    cms_fact = _fact("cms-1", {**YEAR_VALUE, "institution": "  Example\u00a0MEDICAL   School "}, source_kind="cms_doctors")
    originals = copy.deepcopy([state_fact, cms_fact])
    merged, = _canonical(state_fact, cms_fact)

    assert merged["value"] == rich_value_by_field
    assert merged["display"] == state_fact["display"]
    assert merged["corroborated_fields"] == ["institution", "graduation_year"]
    assert merged["source_kinds"] == ["cms_doctors", "state_regulator"]
    assert merged["source_record_ids"] == ["cms-1", "state-1"]
    assert merged["assertion_count"] == 2
    assert "source_count" not in merged and "independent_source_count" not in merged
    for original in originals:
        assertion_by_field = _assertion(merged, original["source_kinds"][0])
        assert assertion_by_field["value"] == original["value"]
        assert assertion_by_field["display"] == original["display"]
        assert assertion_by_field["source_record_ids"] == original["source_record_ids"]
    assert [state_fact, cms_fact] == originals


@pytest.mark.parametrize("duplicate_value", [YEAR_VALUE, DATE_VALUE])
def test_exact_groups_are_one_event_candidate_in_any_order(duplicate_value):
    other_value = DATE_VALUE if duplicate_value == YEAR_VALUE else YEAR_VALUE
    facts = [_fact("state-1", duplicate_value), _fact("state-2", duplicate_value), _fact("cms-1", other_value, source_kind="cms_doctors")]
    expected = _canonical(*facts)
    assert len(expected) == 1
    assert expected[0]["assertion_count"] == 3
    assert len(expected[0]["assertions"]) == 3
    for shuffled in permutations(facts):
        assert _canonical(*shuffled) == expected


@pytest.mark.parametrize("second_details", [{"degree_or_certificate_code": "DO"}, {"graduation_date": "2001-07-01"}, {"institution_identifier": "second-school"}])
def test_weak_year_does_not_choose_between_two_known_events(second_details):
    first_value_by_field = {**DATE_VALUE, "institution_identifier": "first-school"}
    facts = [_fact("state-1", first_value_by_field), _fact("state-2", {**first_value_by_field, **second_details}), _fact("cms-1", YEAR_VALUE, source_kind="cms_doctors")]
    for shuffled in permutations(facts):
        result = _canonical(*shuffled)
        assert len(result) == 3
        assert len({fact["logical_fact_key"] for fact in result}) == 3
        assert all("corroborated_fields" not in fact for fact in result)


def test_three_partial_candidates_remain_ambiguous():
    facts = [_fact("state-1", DATE_VALUE), _fact("state-2", {**YEAR_VALUE, "major": "Medicine"}), _fact("cms-1", YEAR_VALUE, source_kind="cms_doctors")]
    assert len(_canonical(*facts)) == 3


@pytest.mark.parametrize("other_value", [
    {**YEAR_VALUE, "graduation_year": 2002},
    {**DATE_VALUE, "graduation_year": 2002},
    {**DATE_VALUE, "graduation_date_precision": "source"},
    {**DATE_VALUE, "graduation_date": "2001-02-30"},
    {**DATE_VALUE, "graduation_date": "20010601"},
    {**DATE_VALUE, "graduation_date": "2001-W22-5"},
    {"institution": SCHOOL},
    {**YEAR_VALUE, "graduation_year": "unknown"},
    {**YEAR_VALUE, "graduation_year": True},
    {**YEAR_VALUE, "graduation_year": 1799},
    {**YEAR_VALUE, "institution": "OTHER"},
    {**YEAR_VALUE, "institution": "unknown"},
    {**YEAR_VALUE, "institution": None},
    {**YEAR_VALUE, "institution": "Example Medical School."},
    {**YEAR_VALUE, "institution": "Example Medical School College of Medicine"},
])
def test_no_partial_merge_for_conflicting_unknown_or_unparsed_core(other_value):
    assert len(_canonical(_fact("first", YEAR_VALUE), _fact("second", other_value))) == 2


@pytest.mark.parametrize("details", [
    {"graduation_date": "2001-07-01"},
    {"degree_or_certificate_code": "DO"},
    {"program": "Dentistry"},
    {"major": "Dentistry"},
    {"attendance_start": "1998-09-01"},
    {"institution_identifier": "school-2"},
])
def test_known_details_cannot_be_overwritten(details):
    rich_value_by_field = {**DATE_VALUE, "major": "Medicine", "attendance_start": "1997-09-01", "institution_identifier": "school-1"}
    assert len(_canonical(_fact("first", rich_value_by_field), _fact("second", {**rich_value_by_field, **details}))) == 2


def test_blank_detail_can_share_a_known_school_and_year():
    merged, = _canonical(_fact("first", {**YEAR_VALUE, "program": ""}), _fact("second", {**YEAR_VALUE, "program": "Medicine"}))
    assert merged["value"]["program"] == "Medicine"
    assert merged["corroborated_fields"] == ["institution", "graduation_year"]


@pytest.mark.parametrize(("value", "fields"), [
    ({"institution": SCHOOL}, ["institution"]),
    ({"institution": "OTHER", "graduation_year": 2001}, ["graduation_year"]),
])
def test_exact_partial_duplicates_only_corroborate_known_fields(value, fields):
    merged, = _canonical(_fact("first", value), _fact("second", value))
    assert merged["corroborated_fields"] == fields


@pytest.mark.parametrize("other_value", [YEAR_VALUE, DATE_VALUE])
def test_year_precision_can_corroborate_explicit_year_or_date(other_value):
    state_fact = _fact("state-1", {"institution": SCHOOL, "graduation_date": "2001", "graduation_date_precision": "year"})
    merged, = _canonical(state_fact, _fact("other-1", other_value))
    assert merged["value"] in (state_fact["value"], other_value)
    assert merged["corroborated_fields"] == ["institution", "graduation_year"]


@pytest.mark.parametrize("visibility", [{"sensitive": True, "public_default": False}, {"sensitive": True}, {"public_default": False}])
def test_visibility_boundaries_stay_separate(visibility):
    assert len(_canonical(_fact("state-1", DATE_VALUE, **visibility), _fact("cms-1", YEAR_VALUE, source_kind="cms_doctors"))) == 2


def test_restricted_state_fact_cannot_absorb_public_cms_or_expose_evidence():
    projection = _projection(_fact("state-private", DATE_VALUE, sensitive=True, public_default=False), _fact("cms-public", YEAR_VALUE, source_kind="cms_doctors"))
    profile = _compose(projection)
    visible_fact, = profile["categories"]["education"]["items"]
    evidence = compose_provider_profile_evidence(state_projection=projection, fhir_evidence=None, provider_profile=profile)
    assert visible_fact["value"] == YEAR_VALUE
    assert visible_fact["source_record_ids"] == ["cms-public"]
    cms_only = _compose(_projection(_fact("cms-public", YEAR_VALUE, source_kind="cms_doctors")))
    assert visible_fact["item_id"] == cms_only["categories"]["education"]["items"][0]["item_id"]
    assert evidence["sources"]["state_regulator"]["records"] == []
    assert evidence["sources"]["cms_doctors"]["records"] == [{"source_record_id": "cms-public"}]
    assert len(_compose(projection, include_sensitive=True)["categories"]["education"]["items"]) == 2


def test_future_flag_and_display_remain_on_original_assertion():
    state_fact = _fact("state-1", {**DATE_VALUE, "graduation_date": "2028-06-01"})
    cms_fact = _fact("cms-1", {**YEAR_VALUE, "graduation_year": 2028}, source_kind="cms_doctors", display="Reported future graduation year: 2028", quality_flags=["graduation_year_in_future"])
    merged, = _canonical(state_fact, cms_fact)
    state_assertion_by_field = _assertion(merged, "state_regulator")
    cms_assertion_by_field = _assertion(merged, "cms_doctors")
    assert merged["value"] == state_fact["value"]
    assert merged["quality_flags"] == ["graduation_year_in_future"]
    assert "quality_flags" not in state_assertion_by_field
    assert cms_assertion_by_field["quality_flags"] == ["graduation_year_in_future"]
    assert cms_assertion_by_field["display"] == cms_fact["display"]
    assert _canonical(merged) == [merged]


def test_legacy_state_support_gets_assertions_and_missing_display_is_omitted():
    legacy = _fact("legacy-state", YEAR_VALUE)
    legacy.pop("source_kinds")
    legacy.pop("display")
    merged, = _canonical(legacy, _fact("cms-1", YEAR_VALUE, source_kind="cms_doctors"))
    assert merged["source_kinds"] == ["cms_doctors", "state_regulator"]
    assertion_by_field = _assertion(merged, "state_regulator")
    assert "display" not in assertion_by_field
    assert assertion_by_field["source_record_ids"] == ["legacy-state"]


@pytest.mark.parametrize("source_kind", ["provider_directory_fhir", "academic_registry"])
def test_explicit_source_with_record_id_keeps_its_own_attribution(source_kind):
    fact = _fact("source-record", YEAR_VALUE, source_kind=source_kind)
    merged, = _canonical(fact)
    assert merged["source_kinds"] == [source_kind]
    assertion_by_field = _assertion(merged, source_kind)
    assert assertion_by_field["source_record_ids"] == ["source-record"]
    assert len(merged["assertions"]) == 1


def test_repeated_same_assertion_does_not_inflate_support_or_corroboration():
    fact = _fact("state-1", YEAR_VALUE)
    merged, = _canonical(fact, copy.deepcopy(fact))
    assert merged["assertion_count"] == 1
    assert len(merged["assertions"]) == 1
    assert "corroborated_fields" not in merged


def test_normalization_works_for_explicit_non_cms_source_and_preserves_source_ids():
    directory_fact = _fact("directory-record", YEAR_VALUE, source_kind="provider_directory_fhir", source_ids=["directory-feed"])
    directory_fact["source_record_ids"] = []
    merged, = _canonical(directory_fact, _fact("state-record", DATE_VALUE))
    assert merged["source_kinds"] == ["provider_directory_fhir", "state_regulator"]
    assertion_by_field = _assertion(merged, "provider_directory_fhir")
    assert assertion_by_field["source_ids"] == ["directory-feed"]


def test_composed_identity_survives_unique_richer_support_and_evidence_is_retained():
    cms_fact = _fact("cms-1", YEAR_VALUE, source_kind="cms_doctors")
    first_profile = _compose(_projection(cms_fact))
    projection = _projection(cms_fact, _fact("state-1", DATE_VALUE))
    next_profile = _compose(projection)
    first_item, = first_profile["categories"]["education"]["items"]
    next_item, = next_profile["categories"]["education"]["items"]
    assert first_item["item_id"] == next_item["item_id"]
    evidence = compose_provider_profile_evidence(state_projection=projection, fhir_evidence=None, provider_profile=next_profile)
    assert evidence["sources"]["state_regulator"]["records"] == [{"source_record_id": "state-1"}]
    assert evidence["sources"]["cms_doctors"]["records"] == [{"source_record_id": "cms-1"}]


def test_other_education_types_remain_unchanged():
    other_fact_by_field = {"type": "training_history", "value": "Reported training", "display": "Training"}
    assert _canonical(other_fact_by_field) == [other_fact_by_field]
