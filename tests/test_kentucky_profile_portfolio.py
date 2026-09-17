# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retain literal practice information without changing legacy education evidence."""

import copy
from unittest.mock import AsyncMock

import pytest

from api import provider_profile as profile_api
from api import provider_profile_composer_parts as composer
from api import provider_profile_states as state_api
from api.endpoint import npi as npi_api
from process import kentucky_profile_rows as rows
from tests.test_kentucky_profile import ImportHarness, _response, _retained_run, worker
from tests.test_kentucky_profile_rows import CANDIDATE, EVIDENCE, FIELDS, LICENSE, response_html
from tests.test_massachusetts_profile_portfolio import _projection as _massachusetts_projection
from tests.test_provider_profile_kentucky import NPI, _combined_projection, _compose, _row

PORTFOLIO_FIELDS = [*FIELDS, ("*Area of Practice", " Internal Medicine, Pediatrics "),
                    ("Type of Practice", " Resident/Fellow ")]
EMPTY_PORTFOLIO = [*FIELDS, ("*Area of Practice", ""), ("Type of Practice", "")]


def _parse(fields=PORTFOLIO_FIELDS, *, categories=rows.PROFILE_CATEGORIES, candidates=None, html=None):
    return rows.parse_profile(response_html(fields) if html is None else html, license_number=LICENSE,
                              candidates=[CANDIDATE] if candidates is None else candidates,
                              evidence=EVIDENCE, categories=categories)


def test_default_scope_preserves_education_exactly():
    legacy = rows.parse_profile(response_html(PORTFOLIO_FIELDS), license_number=LICENSE,
                                candidates=[CANDIDATE], evidence=EVIDENCE)
    assert legacy == _parse(categories=rows.LEGACY_CATEGORIES)
    source_record, facts = _parse()
    assert source_record == legacy[0] and facts[:1] == legacy[1]
    assert len(facts) == 3


def test_literal_practice_facts_keep_original_evidence():
    source_record, facts = _parse()
    specialty, practice = facts[1:]
    assert specialty["value_json"] == {"text": "Internal Medicine, Pediatrics"}
    assert practice["value_json"] == {"practice_type": "Resident/Fellow"}
    assert (specialty["category"], specialty["fact_type"]) == ("specialties", "specialty")
    assert (practice["category"], practice["fact_type"]) == ("services", "practice_type")
    for fact, label in ((specialty, "*Area of Practice"), (practice, "Type of Practice")):
        assert fact["source_json"]["raw_fields"] == {label: dict(PORTFOLIO_FIELDS)[label]}
        assert fact["source_json"]["source_path"] == f"detail.{label}"
        assert fact["source_json"]["content_sha256"] == EVIDENCE["content_sha256"]
        assert fact["source_json"]["source_record_id"] == source_record["record_id"]
        assert fact["assertion_type"] == "source_reported"
        assert fact["verification_status"] == "not_independently_verified"
        assert fact["effective_start"] is fact["effective_end"] is None
    assert len({fact["fact_id"] for fact in facts}) == 3
    assert {fact["category"] for fact in facts}.isdisjoint({"certifications", "training", "professional_experience", "accepting_patients"})


@pytest.mark.parametrize("reported", ["", " \t ", "None on File", "NONE ON FILE"])
def test_empty_or_placeholder_practice_is_not_positive(reported):
    fields = [*FIELDS, ("*Area of Practice", reported), ("Type of Practice", reported)]
    assert _parse(fields)[1] == _parse(EMPTY_PORTFOLIO)[1]


@pytest.mark.parametrize("year,visibility", [("", "education_not_reported"), ("invalid", "education_unusable")])
def test_missing_school_keeps_usable_practice_visible(year, visibility):
    fields = [(label, {"Medical School": "", "Year Graduated": year}.get(label, value)) for label, value in PORTFOLIO_FIELDS]
    source_record, facts = _parse(fields)
    assert source_record["normalized_payload"]["visibility"] == "public"
    assert source_record["normalized_payload"]["education_visibility"] == visibility
    assert {fact["category"] for fact in facts} == {"specialties", "services"}


@pytest.mark.parametrize("scope", [None, [], ["education", "specialties"], ["specialties", "services"],
                                  ["education", "services", "specialties"], ["education", "education"]])
def test_partial_or_reordered_scope_is_rejected(scope):
    with pytest.raises(ValueError, match="categories_invalid"):
        _parse(categories=scope)


@pytest.mark.parametrize("attribute", ['hidden', 'aria-hidden="true"', 'style="display: none"', 'style="visibility: hidden"'])
@pytest.mark.parametrize("container", ["body", "section"])
def test_explicitly_hidden_markup_fails_closed(attribute, container):
    html = response_html(PORTFOLIO_FIELDS)
    if container == "body":
        html = html.replace("<body>", f"<body {attribute}>")
    else:
        html = html.replace("<body>", f"<body><section {attribute}>").replace("</body>", "</section></body>")
    with pytest.raises(ValueError, match="hidden_profile_markup"):
        _parse(html=html)


def test_hidden_form_state_does_not_hide_profile():
    html = response_html(PORTFOLIO_FIELDS).replace('<div class="ky-cm-content">',
        '<input type="hidden" name="__VIEWSTATE" value="synthetic-state" hidden><div class="ky-cm-content">')
    assert _parse(html=html)[1] == _parse()[1]


def test_nullable_attributes_do_not_raise_attribute_error():
    html = response_html(PORTFOLIO_FIELDS).replace("<body>", '<body aria-hidden style><input type>')
    assert _parse(html=html)[1] == _parse()[1]


@pytest.mark.parametrize("label", ["*Area of Practice", "Type of Practice"])
def test_missing_expanded_label_fails_closed(label):
    fields = [(key, value) for key, value in PORTFOLIO_FIELDS if key != label]
    with pytest.raises(ValueError, match="portfolio_labels_missing"):
        _parse(fields)
    assert len(_parse(fields, categories=rows.LEGACY_CATEGORIES)[1]) == 1


def test_ambiguous_and_unmatched_facts_remain_unattached():
    for candidates, status in (([], "unmatched"), ([CANDIDATE, {**CANDIDATE, "npi": "1000000012"}], "ambiguous")):
        source_record, facts = _parse(candidates=candidates)
        assert source_record["match_status"] == status
        assert len(facts) == 3 and all(fact["npi"] is None for fact in facts)
    assert _parse(PORTFOLIO_FIELDS * 2)[1] == []
    fields = [(label, "C0088" if label == "License" else value) for label, value in PORTFOLIO_FIELDS]
    assert _parse(fields)[0]["normalized_payload"]["visibility"] == "held_identity"
    assert _parse([])[1] == []


def _published_rows(fields=PORTFOLIO_FIELDS):
    template = _row(generation=EVIDENCE["run_id"])
    template["source_manifest"]["categories"] = list(rows.PROFILE_CATEGORIES)
    return [{**copy.deepcopy(template), **fact} for fact in _parse(fields)[1]]


def test_full_and_paged_api_preserve_actual_assertions():
    fact_rows = _published_rows()
    projection = _combined_projection(*fact_rows)
    full = _compose(projection)
    for category in ("specialties", "services"):
        item, = full["categories"][category]["items"]
        fact = next(fact for fact in fact_rows if fact["category"] == category)
        assert item["value"] == fact["value_json"] and item["source_ids"] == [rows.SOURCE_KEY]
        assert item["assertions"][0]["value"] == fact["value_json"]
        page = _compose(projection, page_category=category, page_limit=1)
        assert page["categories"][category]["items"] == [item]
        assert page["categories"][category]["total"] == 1
    assert any("does not verify current specialties" in context for context in full["important_context"])
    assert _compose(_combined_projection())["categories"]["services"]["availability"] == "unavailable"


@pytest.mark.parametrize("include_kentucky", [False, True])
def test_equal_specialties_share_one_item_with_all_page_evidence(include_kentucky):
    projection = state_api.merge_state_profile_projection(NPI, None, _massachusetts_projection())
    if include_kentucky:
        fields = [(label, "Internal Medicine" if label == "*Area of Practice" else reported_value)
                  for label, reported_value in PORTFOLIO_FIELDS]
        kentucky = state_api._state_projection(NPI, _published_rows(fields), source_key=rows.SOURCE_KEY)
        projection = state_api.merge_state_profile_projection(NPI, projection, kentucky)
    original = copy.deepcopy(projection)
    full = _compose(projection)
    specialty_facts = full["categories"]["specialties"]["items"]
    assert len(specialty_facts) == len({specialty_fact["item_id"] for specialty_fact in specialty_facts}) == 2
    assert {specialty_fact["value"]["text"] for specialty_fact in specialty_facts} == {"Internal Medicine", "Internal Medicine, Pulmonary Disease"}
    shared = next(specialty_fact for specialty_fact in specialty_facts if specialty_fact["value"] == {"text": "Internal Medicine"})
    assert shared["assertion_count"] == len(shared["assertions"]) == len(shared["source_record_ids"]) == 2 + include_kentucky
    assert set(shared["source_ids"]) == {state_api.MASSACHUSETTS_SOURCE_KEY} | ({rows.SOURCE_KEY} if include_kentucky else set())
    assert all(assertion["value"] == shared["value"] for assertion in shared["assertions"])
    assert len(full["categories"]["certifications"]["items"]) == 2
    seen_ids = set()
    for offset, expected in enumerate(specialty_facts):
        page = _compose(projection, requested_categories=["specialties"], page_category="specialties", page_limit=1, page_offset=offset)
        specialty_fact, = page["categories"]["specialties"]["items"]
        assert specialty_fact == expected and specialty_fact["item_id"] not in seen_ids
        seen_ids.add(specialty_fact["item_id"])
        assert page["categories"]["specialties"]["total"] == 2
        evidence = profile_api.compose_provider_profile_evidence(
            state_projection=projection, fhir_evidence=None, provider_profile=page, page_category="specialties",
        )
        evidence_records = [evidence_record for source_evidence in evidence["sources"].values() for evidence_record in source_evidence["records"]]
        assert {evidence_record["source_record_id"] for evidence_record in evidence_records} == set(specialty_fact["source_record_ids"])
        originals = [evidence_record for source_evidence in original["additional_state_evidence"].values()
                     for evidence_record in source_evidence["records"] if evidence_record["source_record_id"] in specialty_fact["source_record_ids"]]
        assert evidence_records == originals
    assert _compose(projection, page_category="specialties", page_limit=1, page_offset=2)["categories"]["specialties"]["items"] == []
    assert projection == original


def test_specialty_composer_change_invalidates_previous_page_generation(monkeypatch):
    projection = state_api.merge_state_profile_projection(NPI, None, _massachusetts_projection())
    current = _compose(projection)
    with monkeypatch.context() as previous:
        previous.setattr(composer, "PROFILE_COMPOSER_VERSION", "provider-profile-composer/v8")
        old = _compose(projection)
    assert current["composer_version"] == "provider-profile-composer/v11"
    assert current["source_generations"] == old["source_generations"]
    assert current["generation_id"] != old["generation_id"]
    assert npi_api._provider_profile_generation_error(current, old["generation_id"]).status == 409


def test_equal_practice_keeps_both_licenses():
    published_facts = []
    for license_number in (LICENSE, "C0008"):
        reported_fields = [(label, {"License": license_number, "Type of Practice": " Group Practice "}.get(label, reported_value))
                           for label, reported_value in PORTFOLIO_FIELDS]
        _, parsed_facts = rows.parse_profile(
            response_html(reported_fields, license_number=license_number), license_number=license_number,
            candidates=[{**CANDIDATE, "license_number": license_number}], evidence=EVIDENCE, categories=rows.PROFILE_CATEGORIES,
        )
        publication = _row(license_number=license_number, generation=EVIDENCE["run_id"])
        publication["source_manifest"]["categories"] = list(rows.PROFILE_CATEGORIES)
        published_facts.extend({**publication, **fact} for fact in parsed_facts)
    projection = state_api.merge_state_profile_projection(
        NPI, None, state_api._state_projection(NPI, published_facts, source_key=rows.SOURCE_KEY),
    )
    original = copy.deepcopy(projection)
    original_practice = original["profile"]["categories"]["services"]["items"]
    assert len(original_practice) == 2
    full = _compose(projection, requested_categories=["services"])
    practice_fact, = full["categories"]["services"]["items"]
    assert practice_fact["value"] == {"practice_type": "Group Practice"}
    assert practice_fact["assertion_count"] == len(practice_fact["assertions"]) == len(practice_fact["source_record_ids"]) == 2
    assert practice_fact["assertions"] == [assertion for fact in original_practice for assertion in fact["assertions"]]
    page = _compose(projection, requested_categories=["services"], page_category="services", page_limit=1)
    for response in (full, page):
        assert response["categories"]["services"]["items"] == [practice_fact]
        assert response["categories"]["services"]["total"] == 1
        evidence = profile_api.compose_provider_profile_evidence(
            state_projection=projection, fhir_evidence=None, provider_profile=response, page_category="services",
        )
        evidence_records = evidence["sources"][rows.SOURCE_KEY]["records"]
        assert evidence_records == [evidence_record for evidence_record in original["additional_state_evidence"][rows.SOURCE_KEY]["records"]
                                    if evidence_record["source_path"] == "detail.Type of Practice"]
        assert {evidence_record["source_record_id"] for evidence_record in evidence_records} == set(practice_fact["source_record_ids"])
        assert len({evidence_record["profile_source_record_id"] for evidence_record in evidence_records}) == 2
        assert all(evidence_record["raw_fields"] == {"Type of Practice": " Group Practice "} for evidence_record in evidence_records)
    assert _compose(projection, page_category="services", page_limit=1, page_offset=1)["categories"]["services"]["items"] == []
    assert projection == original


def test_new_scope_without_practice_reports_missing():
    projection = state_api._state_projection(NPI, _published_rows(EMPTY_PORTFOLIO), source_key=rows.SOURCE_KEY)
    assert projection["categories"]["specialties"]["availability"] == "not_reported"
    assert projection["categories"]["services"]["availability"] == "not_reported"


def test_api_does_not_widen_legacy_or_other_sources():
    facts = _published_rows()
    facts[0]["source_manifest"]["categories"] = list(rows.LEGACY_CATEGORIES)
    with pytest.raises(RuntimeError, match="categories_invalid"):
        state_api._state_projection(NPI, facts, source_key=rows.SOURCE_KEY)
    for fact in _published_rows()[1:]:
        fact["fact_type"] = "board_certification"
        with pytest.raises(RuntimeError, match="categories_invalid"):
            state_api._state_projection(NPI, [fact], source_key=rows.SOURCE_KEY)


@pytest.mark.parametrize("resume_scope", [None, rows.LEGACY_CATEGORIES, rows.PROFILE_CATEGORIES])
async def test_driver_preserves_frozen_scope(monkeypatch, tmp_path, resume_scope):
    harness = ImportHarness(monkeypatch, tmp_path)
    for license_number in harness.responses_by_license:
        fields = [(label, license_number if label == "License" else value) for label, value in PORTFOLIO_FIELDS]
        harness.responses_by_license[license_number] = _response(license_number, html=response_html(fields, license_number=license_number))
    task_by_field = {**harness.task, "max_providers": 2}
    if resume_scope is not None:
        previous, _directory = _retained_run(harness)
        previous["source_manifest"]["categories"] = list(resume_scope)
        monkeypatch.setattr(worker.store, "read_resume_run", AsyncMock(return_value=previous))
        task_by_field["resume_from"] = previous["run_id"]
    await worker.import_profiles(harness.ctx, task_by_field)
    expected_categories = list(rows.PROFILE_CATEGORIES if resume_scope is None else resume_scope)
    claimed = harness.store_by_name["claim_run"].call_args.args[0]
    assert claimed["source_manifest"]["categories"] == expected_categories
    assert {fact["category"] for fact in harness.rows_for(worker.ProviderProfileFact)} == set(expected_categories)
    if resume_scope is not None:
        assert harness.requests == []
