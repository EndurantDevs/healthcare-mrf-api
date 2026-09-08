# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import copy
from datetime import datetime
from html import escape

import pytest

from process import kentucky_profile_rows as rows

NPI = "1000000004"
OTHER_NPI = "1000000012"
LICENSE = "C0007"
EVIDENCE = {
    "run_id": "synthetic-run", "artifact_id": "synthetic-artifact",
    "source_url": "https://example.test/LicenseList.aspx?FLD2=C0007",
    "downloaded_at": "2026-09-08T00:00:00Z", "content_sha256": "a" * 64, "row_number": 3,
}
CANDIDATE = {"npi": NPI, "license_number": LICENSE, "license_state": "KY",
             "first_name": "Alex", "middle_name": "Morgan", "last_name": "Example", "suffix": "Jr"}
FIELDS = [("Name", " Alex  Morgan Example Jr. M.D. "), ("License", LICENSE),
          ("Status", "Resident/Fellow"), ("Medical School", " Synthetic & Medical School "), ("Year Graduated", "2001")]


def field_markup(label, text):
    return f'<div class="row"><div class="cols2-col1"><b>{escape(label)}:&nbsp;</b></div><div class="cols2-col2">{escape(text)}</div></div>'


def response_html(fields=FIELDS, *, license_number=LICENSE):
    markup = "".join(field_markup(label, text) for label, text in fields)
    return f'''<html><body><form id="Form1" action="LicenseList.aspx?AGY=5&amp;FLD1=&amp;FLD2={license_number}&amp;FLD3=0&amp;FLD4=0&amp;TYPE=">
      <div class="ky-cm-content">
      <input id="usLicenseList_rdbtDetail" value="rdbtDetail" checked="checked" />
      <span id="usLicenseList_lblSearchCriterion"><b>Search Criterion: KY License Number = {license_number}; Practice County = 0; Specialty = 0;</b></span>
      {markup}</div></form></body></html>'''


def parsed_profile(fields=FIELDS, *, candidates=None, license_number=LICENSE, evidence=None):
    return rows.parse_profile(response_html(fields, license_number=license_number), license_number=license_number,
        candidates=copy.deepcopy([CANDIDATE] if candidates is None else candidates), evidence=EVIDENCE if evidence is None else evidence)


def test_retention_keeps_original_education_without_training_inference():
    source_record, facts = parsed_profile()
    assert source_record["matched_npi"] == int(NPI)
    assert source_record["match_status"] == "deterministic"
    assert source_record["license_number"] == LICENSE
    assert source_record["raw_payload"]["html"] == response_html()
    assert source_record["raw_payload"]["profiles"][0]["Name"] == FIELDS[0][1]
    assert source_record["source_key"] == "kentucky-kbml"
    assert len(facts) == 1
    fact = facts[0]
    assert fact["value_json"] == {"institution": "Synthetic & Medical School", "graduation_year": 2001}
    assert (fact["category"], fact["fact_type"]) == ("education", "education_history")
    assert fact["npi"] == int(NPI) and fact["published_at"] is None
    assert fact["effective_start"] is fact["effective_end"] is None
    assert fact["assertion_type"] == "source_reported"
    assert fact["verification_status"] == "not_independently_verified"
    assert fact["source_json"]["source_record_id"] == source_record["record_id"]
    assert fact["source_json"]["schema_version"] == "ky-kbml-profile/v1"
    assert fact["source_json"]["raw_fields"]["Medical School"] == FIELDS[3][1]
    assert "Resident" not in str(fact) and "experience" not in str(fact)


def test_labels_allow_reordering_nested_text_and_optional_field_absence():
    fields = [FIELDS[0], FIELDS[4], FIELDS[1], FIELDS[3]]
    html = response_html(fields).replace("Synthetic &amp; Medical School", "Synthetic <span>&amp;</span> Medical School")
    source_record, facts = rows.parse_profile(html, license_number=LICENSE, candidates=[CANDIDATE], evidence=EVIDENCE)
    assert source_record["matched_npi"] == int(NPI)
    assert facts[0]["value_json"]["graduation_year"] == 2001
    assert facts[0]["value_json"]["institution"] == "Synthetic & Medical School"


def test_legacy_spacing_and_blank_layout_rows_preserve_education():
    html = response_html([*FIELDS, ("", " ")]).replace(
        "Synthetic &amp; Medical School", "Synthetic<br>&amp; Medical School"
    ).replace("</form>", '<div class="ky-cm-post">Published: 09/28/2004 [wvd]</div></form>')
    evidence_by_field = {**EVIDENCE, "downloaded_at": datetime.fromisoformat(EVIDENCE["downloaded_at"])}
    record, facts = rows.parse_profile(html, license_number=LICENSE, candidates=[CANDIDATE], evidence=evidence_by_field)
    assert record["match_status"] == "deterministic"
    assert facts[0]["value_json"] == {"institution": "Synthetic & Medical School", "graduation_year": 2001}
    assert facts[0]["source_json"]["downloaded_at"] == "2026-09-08T00:00:00+00:00"


@pytest.mark.parametrize("candidate_changes", [
    {"first_name": None}, {"first_name": " "}, {"last_name": None}, {"last_name": " "},
    {"middle_name": 123}, {"suffix": 123},
])
def test_incomplete_or_malformed_registry_names_never_attach_facts(candidate_changes):
    record, facts = parsed_profile(candidates=[{**CANDIDATE, **candidate_changes}])
    assert record["match_status"] == "identity_conflict" and record["matched_npi"] is None
    assert facts[0]["npi"] is None
    assert facts[0]["value_json"]["institution"] == "Synthetic & Medical School"


def test_matching_names_without_middle_name_or_suffix_need_no_inference():
    fields = [("Name", "Alex Example M.D."), *FIELDS[1:]]
    candidate_by_field = {**CANDIDATE, "middle_name": None, "suffix": None}
    record, facts = parsed_profile(fields, candidates=[candidate_by_field])
    assert record["match_status"] == "deterministic" and facts[0]["npi"] == int(NPI)


@pytest.mark.parametrize(("display_name", "candidate_changes", "expected"), [
    ("Alex M. Example Jr. D.O.", {}, "deterministic"),
    ("Ａｌｅｘ Morgan EXAMPLE Jr M.D.", {}, "deterministic"),
    ("Alex Morgan Example Jr M.D.", {"license_number": " C0007 "}, "deterministic"),
    ("A Morgan Example Jr M.D.", {}, "identity_conflict"),
    ("Alex Example Jr M.D.", {}, "identity_conflict"),
    ("Alex Morgan Example M.D.", {}, "identity_conflict"),
    ("Alex Taylor Example Jr M.D.", {}, "identity_conflict"),
    ("Alex Morgan Different Jr M.D.", {}, "identity_conflict"),
    ("Alex Morgan Example Jr M.D.", {"suffix": "Sr"}, "identity_conflict"),
    ("Alex Morgan Example Jr M.D.", {"license_state": "FL"}, "unmatched"),
    ("Alex Morgan Example Jr M.D.", {"license_number": "C007"}, "unmatched"),
    ("Alex Morgan Example Jr M.D.", {"npi": "1000000005"}, "unmatched"),
    ("Alex Morgan Van Example Jr M.D.", {"last_name": "Van Example"}, "deterministic"),
    ("Mary Alex Morgan Example Jr M.D.", {"first_name": "Mary Alex"}, "deterministic"),
])
def test_full_name_comparison_is_conservative(display_name, candidate_changes, expected):
    fields = [("Name", display_name), *FIELDS[1:]]
    source_record, facts = parsed_profile(fields, candidates=[{**CANDIDATE, **candidate_changes}])
    assert source_record["match_status"] == expected
    assert source_record["matched_npi"] == (int(NPI) if expected == "deterministic" else None)
    assert facts[0]["npi"] == source_record["matched_npi"]


def test_ambiguous_registry_matches_retain_unattached_assertions():
    source_record, facts = parsed_profile(candidates=[CANDIDATE, {**CANDIDATE, "npi": OTHER_NPI}])
    assert source_record["match_status"] == "ambiguous" and facts[0]["npi"] is None
    assert parsed_profile(candidates=[CANDIDATE, CANDIDATE])[0]["matched_npi"] == int(NPI)
    assert parsed_profile(candidates=[])[0]["match_status"] == "unmatched"
    inconsistent_candidates = [CANDIDATE, {**CANDIDATE, "first_name": "Different"}]
    assert parsed_profile(candidates=inconsistent_candidates)[0]["match_status"] == "identity_conflict"


@pytest.mark.parametrize(("display_name", "expected"), [
    ("Alex Morgan Do", "deterministic"),
    ("Alex Morgan Do M.D.", "deterministic"),
    ("Alex Morgan Do D.O.", "deterministic"),
    ("Alex Morgan D.O.", "identity_conflict"),
    ("Alex Morgan DO", "identity_conflict"),
    ("Alex Morgan MD", "identity_conflict"),
])
def test_surname_do_is_distinct_from_a_credential(display_name, expected):
    fields = [("Name", display_name), *FIELDS[1:]]
    candidate_by_field = {**CANDIDATE, "last_name": "Do", "suffix": None}
    source_record, facts = parsed_profile(fields, candidates=[candidate_by_field])
    assert source_record["match_status"] == expected
    assert facts[0]["npi"] == (int(NPI) if expected == "deterministic" else None)


def test_explicit_credential_cannot_match_a_same_license_surname():
    fields = [("Name", "Alex Morgan D.O."), *FIELDS[1:]]
    candidates = [{**CANDIDATE, "middle_name": None, "last_name": "Morgan", "suffix": None},
                  {**CANDIDATE, "npi": OTHER_NPI, "last_name": "Do", "suffix": None}]
    assert parsed_profile(fields, candidates=candidates)[0]["matched_npi"] == int(NPI)
    fields = [("Name", "Alex Morgan DO"), *FIELDS[1:]]
    source_record, facts = parsed_profile(fields, candidates=[])
    assert source_record["match_status"] == "unmatched" and facts[0]["npi"] is None
    assert source_record["match_evidence"]["reason"] == "ambiguous_undotted_credential"


@pytest.mark.parametrize("license_number", ["00079", "C0007"])
def test_original_license_text_is_not_coerced(license_number):
    fields = [(label, license_number if label == "License" else text) for label, text in FIELDS]
    candidate_by_field = {**CANDIDATE, "license_number": license_number}
    source_record, facts = parsed_profile(fields, candidates=[candidate_by_field], license_number=license_number)
    assert source_record["license_number"] == license_number and facts[0]["npi"] == int(NPI)


@pytest.mark.parametrize("fields", [FIELDS + FIELDS, [(label, "0791" if label == "License" else text) for label, text in FIELDS]])
def test_multiple_or_mismatched_results_are_held(fields):
    source_record, facts = parsed_profile(fields)
    assert source_record["match_status"] == "identity_conflict"
    assert source_record["normalized_payload"]["visibility"] == "held_identity"
    assert source_record["matched_npi"] is None and facts == []
    assert len(source_record["raw_payload"]["profiles"]) == (2 if len(fields) > len(FIELDS) else 1)


@pytest.mark.parametrize(("year", "normalized", "flag"), [
    (" 2030 ", 2030, "graduation_year_in_future"),
    ("1700", 1700, "graduation_year_before_1800"),
    ("20O1", None, "graduation_year_invalid"),
    ("0000", None, "graduation_year_invalid"),
    ("2001-06-30", None, "graduation_year_invalid"),
])
def test_year_quality_keeps_raw_source(year, normalized, flag):
    fields = [(label, year if label == "Year Graduated" else text) for label, text in FIELDS]
    source_record, facts = parsed_profile(fields)
    assert facts[0]["value_json"].get("graduation_year") == normalized
    assert facts[0]["source_json"]["raw_fields"]["Year Graduated"] == year
    assert facts[0]["source_json"]["quality_flags"] == [flag]
    assert source_record["normalized_payload"]["quality_flags"] == [flag]
    facts[0]["source_json"]["quality_flags"].append("additional_fact_flag")
    assert source_record["normalized_payload"]["quality_flags"] == [flag]


def test_blank_education_and_year_only_are_distinct():
    fields = [(label, "" if label in {"Medical School", "Year Graduated"} else text) for label, text in FIELDS]
    source_record, facts = parsed_profile(fields)
    assert source_record["normalized_payload"]["visibility"] == "education_not_reported" and facts == []
    fields = [(label, "" if label == "Medical School" else text) for label, text in FIELDS]
    assert parsed_profile(fields)[1][0]["value_json"] == {"graduation_year": 2001}
    fields = [(label, "invalid" if label == "Year Graduated" else text) for label, text in fields]
    source_record, facts = parsed_profile(fields)
    assert source_record["normalized_payload"] == {
        "schema_version": rows.SCHEMA_VERSION, "visibility": "education_unusable", "quality_flags": ["graduation_year_invalid"],
    }
    assert facts == [] and source_record["raw_payload"]["profiles"][0]["Year Graduated"] == "invalid"


def test_only_validated_empty_result_is_not_found():
    source_record, facts = parsed_profile([])
    assert source_record["match_status"] == "not_found"
    assert source_record["normalized_payload"]["visibility"] == "not_found" and facts == []
    with pytest.raises(ValueError, match="result_envelope_invalid"):
        rows.parse_profile("<html><body>Temporarily unavailable</body></html>", license_number=LICENSE, candidates=[], evidence=EVIDENCE)


def test_valueless_class_is_inert_outside_results_and_rejected_inside():
    html = response_html([]).replace("<body>", "<body><div class></div>")
    assert rows.parse_profile(html, license_number=LICENSE, candidates=[], evidence=EVIDENCE)[0]["match_status"] == "not_found"
    html = response_html().replace('class="row"', "class", 1)
    with pytest.raises(ValueError, match="kentucky_profile_unexpected_result_markup"):
        rows.parse_profile(html, license_number=LICENSE, candidates=[CANDIDATE], evidence=EVIDENCE)


@pytest.mark.parametrize("evidence_by_field", [
    {key: value for key, value in EVIDENCE.items() if key != "content_sha256"},
    {**EVIDENCE, "downloaded_at": "not-a-timestamp"},
    {**EVIDENCE, "downloaded_at": 20260908},
    {**EVIDENCE, "downloaded_at": None},
])
def test_malformed_evidence_is_rejected_with_source_error(evidence_by_field):
    with pytest.raises(ValueError, match="kentucky_profile_evidence_"):
        parsed_profile(evidence=evidence_by_field)


@pytest.mark.parametrize("unexpected", ["Service temporarily unavailable", "<p>Service temporarily unavailable</p>", "<table></table>"])
@pytest.mark.parametrize("boundary", ["</div></form>", "</form>"])
def test_intact_empty_envelope_rejects_unknown_content(unexpected, boundary):
    html = response_html([]).replace(boundary, unexpected + boundary)
    with pytest.raises(ValueError, match="unexpected_result_"):
        rows.parse_profile(html, license_number=LICENSE, candidates=[], evidence=EVIDENCE)


@pytest.mark.parametrize("change", [
    lambda html: html.replace("checked=\"checked\"", ""),
    lambda html: html.replace("KY License Number = C0007", "KY License Number = C007"),
    lambda html: html.replace("FLD2=C0007", "FLD2=C007"),
    lambda html: html.replace("LicenseList.aspx?", "LicenseSearch.aspx?"),
    lambda html: html.replace("LicenseList.aspx?", "javascript:LicenseList.aspx?"),
    lambda html: html.replace("</html>", ""),
    lambda html: html.replace("</form>", ""),
    lambda html: html.replace("<b>License:", "<b>Other:"),
    lambda html: html.replace("<b>Name:", "<b>Other:"),
    lambda html: html.replace("<b>Year Graduated:", "<b>Medical School:"),
    lambda html: html.replace("</div></div>", "</div>", 1),
    lambda html: html + '<div class="row"></div>',
    lambda html: html.replace('id="Form1"', 'id="Form1" id="other"'),
])
def test_schema_and_identity_failures_are_rejected(change):
    with pytest.raises(ValueError, match="kentucky_profile_"):
        rows.parse_profile(change(response_html()), license_number=LICENSE, candidates=[CANDIDATE], evidence=EVIDENCE)


def test_limits_duplicate_identity_and_evidence_scope():
    with pytest.raises(ValueError, match="html_input_limit"):
        rows.extract_profiles("x" * (rows.MAX_HTML_BYTES + 1), license_number=LICENSE)
    duplicate_license = FIELDS + [("License", LICENSE)]
    with pytest.raises(ValueError, match="missing_or_duplicate_identity_label"):
        parsed_profile(duplicate_license)
    evidence_by_field = {**EVIDENCE, "unrelated": {"private": "excluded"}}
    source_record, facts = parsed_profile(evidence=evidence_by_field)
    other_record, other_facts = parsed_profile(evidence={**evidence_by_field, "run_id": "next"})
    assert "unrelated" not in facts[0]["source_json"]
    assert source_record["record_id"] != other_record["record_id"]
    assert facts[0]["logical_fact_key"] == other_facts[0]["logical_fact_key"]
    facts[0]["source_json"]["raw_fields"]["Medical School"] = "changed"
    assert source_record["raw_payload"]["profiles"][0]["Medical School"] == FIELDS[3][1]


@pytest.mark.parametrize("license_number", ["TP001", "R001", "FT001", "IP001"])
def test_alphanumeric_licenses_remain_literal(license_number):
    fields = [(label, license_number if label == "License" else text) for label, text in FIELDS]
    candidate_by_field = {**CANDIDATE, "license_number": license_number}
    source_record, facts = parsed_profile(fields, candidates=[candidate_by_field], license_number=license_number)
    assert source_record["matched_npi"] == int(NPI) and source_record["license_number"] == license_number
    assert facts[0]["value_json"] == {"institution": "Synthetic & Medical School", "graduation_year": 2001}
