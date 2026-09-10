# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import copy
import csv
import hashlib
import io
from itertools import product

import pytest

from process import tennessee_profile_rows as rows

BASE_COLUMNS = (
    "Board", "Profession", "Rank", "LastName", "FirstName", "MiddleName", "Title",
    "LicenseNumber", "ExpirationDate", "OriginalDate", "Status", "LicenseActivity",
    "StatusEffectiveDate", "Disciplined",
)
FACT_COLUMNS = (
    "ModifierDescription", "ModifierType", "DegreeEarned", "EducationProvider",
    "EducationProviderLocation", "GraduationDate", "OtherTrainingProvider",
    "OtherTrainingLocation", "OtherTrainingFromDate", "OtherTrainingEndDate",
)
PRACTICE_COLUMNS = (
    "PracticeName", "PracticeAddress", "PracticeAddress2", "PracticeCity", "PracticeState",
    "PracticeZIP", "PracticeAreaCode", "PracticePhoneNumber", "PracticeExtension", "PracticeCounty",
)
HEADERS = BASE_COLUMNS + FACT_COLUMNS
PRACTICE_HEADERS = BASE_COLUMNS + FACT_COLUMNS[:2] + PRACTICE_COLUMNS + FACT_COLUMNS[2:]
FULL_HEADERS = BASE_COLUMNS + ("Gender", "Race") + FACT_COLUMNS[:2] + PRACTICE_COLUMNS + FACT_COLUMNS[2:]
EVIDENCE = {
    "run_id": "synthetic-run", "artifact_id": "synthetic-artifact",
    "source_url": "https://example.test/LicensureReports/Home/CreateFile",
    "downloaded_at": "2026-09-09T00:00:00Z",
}


def source_row(**changes):
    return {
        **dict.fromkeys(FULL_HEADERS, ""), "Board": "Medical Examiners", "Profession": "Medical Doctor",
        "Rank": "Medical Doctor", "LastName": "Example", "FirstName": "Alex", "MiddleName": "Morgan",
        "LicenseNumber": "000123", "Status": "Licensed", "LicenseActivity": "Full Time",
        "OriginalDate": "01/01/1999", "ExpirationDate": "01/01/2027", "StatusEffectiveDate": "01/01/2025",
        "Disciplined": "N", "DegreeEarned": "MD", "EducationProvider": "Synthetic Medical School",
        "EducationProviderLocation": "Example City", "GraduationDate": "06/15/1998",
        "OtherTrainingProvider": "Synthetic Hospital", "OtherTrainingLocation": "Example City",
        "OtherTrainingFromDate": "07/01/1998", "OtherTrainingEndDate": "06/30/2001",
        "ModifierDescription": "Internal Medicine", "ModifierType": "Specialty", **changes,
    }


def report_bytes(source_rows, headers=HEADERS):
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=headers, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(source_rows)
    return buffer.getvalue().encode("utf-8")


def parse_sample(source_rows, headers=HEADERS):
    content = report_bytes(source_rows, headers)
    return rows.parse_report(content, evidence={**EVIDENCE, "content_sha256": hashlib.sha256(content).hexdigest()})


def quoted_row(source_record, headers=PRACTICE_HEADERS):
    buffer = io.StringIO(newline="")
    csv.DictWriter(buffer, fieldnames=headers, extrasaction="ignore", quoting=csv.QUOTE_ALL).writerow(source_record)
    return buffer.getvalue().encode("utf-8")


def malformed_row(*, field="EducationProvider", headers=PRACTICE_HEADERS, **changes):
    source_record = source_row(**{field: 'Synthetic "Quoted" Value', **changes})
    return quoted_row(source_record, headers).replace(b'""Quoted""', b'"Quoted"', 1)


def parse_content(content):
    return rows.parse_report(content, evidence={**EVIDENCE, "content_sha256": hashlib.sha256(content).hexdigest()})


def test_cartesian_rows_preserve_independent_histories():
    source_rows = [source_row(EducationProvider=school, GraduationDate=graduated,
        OtherTrainingProvider=training, ModifierDescription=specialty)
        for (school, graduated), training, specialty in product(
            [("Synthetic School A", "06/15/1998"), ("Synthetic School B", "06/16/2005")],
            ["Synthetic Hospital A", "Synthetic Hospital B"], ["Internal Medicine", "Cardiology"])]
    source_records, facts = parse_sample(source_rows + source_rows[:1])
    assert len(source_records) == 1 and len(facts) == 6
    source_record = source_records[0]
    assert source_record["license_number"] == "000123" and source_record["matched_npi"] is None
    assert source_record["match_status"] == "unmatched" and len(source_record["raw_payload"]["rows"]) == 9
    assert source_record["raw_payload"]["rows"][0]["fields"]["OriginalDate"] == "01/01/1999"
    education_facts = [fact for fact in facts if fact["category"] == "education"]
    assert {(fact["value_json"]["institution"], fact["value_json"]["graduation_date"]) for fact in education_facts} == {
        ("Synthetic School A", "1998-06-15"), ("Synthetic School B", "2005-06-16"),
    }
    assert all(fact["value_json"]["degree"] == "MD" for fact in education_facts)
    assert sorted(len(fact["source_json"]["row_numbers"]) for fact in education_facts) == [4, 5]
    assert {fact["fact_type"] for fact in facts} == {"education_history", "other_training", "specialty"}
    assert all(fact["npi"] is None and fact["published_at"] is None for fact in facts)
    assert all(fact["assertion_type"] == "source_reported" for fact in facts)
    assert all(fact["verification_status"] == "not_independently_verified" for fact in facts)
    assert "experience" not in str(facts) and "completed" not in str(facts)


def test_full_header_retains_unprojected_fields():
    source_record = source_row(Gender="Not reported", Race="Not reported", PracticeName="Synthetic Practice")
    retained, facts = parse_sample([source_record], FULL_HEADERS)
    assert retained[0]["raw_payload"]["rows"][0]["fields"] == source_record
    assert {fact["category"] for fact in facts} == {"education", "training", "specialties"}
    assert "Race" not in str(facts) and "PracticeName" not in str(facts)


@pytest.mark.parametrize("headers", [PRACTICE_HEADERS, tuple(reversed(PRACTICE_HEADERS))])
@pytest.mark.parametrize("profession", [{}, {"Board": "Osteopathy", "Profession": "Osteopathic Physician", "DegreeEarned": "DO"}])
def test_practice_header_retains_fields_without_demographics(headers, profession):
    source_record = source_row(**profession, PracticeName="Synthetic Practice", PracticeState="FL")
    retained, facts = parse_sample([source_record], headers)
    assert retained[0]["raw_payload"]["rows"][0]["fields"] == {field: source_record[field] for field in headers}
    assert "Gender" not in retained[0]["raw_payload"]["rows"][0]["fields"]
    assert "Race" not in retained[0]["raw_payload"]["rows"][0]["fields"]
    _, baseline_facts = parse_sample([source_record])
    assert {fact["logical_fact_key"]: fact["value_json"] for fact in facts} == {
        fact["logical_fact_key"]: fact["value_json"] for fact in baseline_facts}
    assert all(fact["npi"] is None and fact["published_at"] is None for fact in facts)
    assert "PracticeName" not in str(facts) and "PracticeState" not in str(facts)


def test_blank_licenses_are_separate_held_applications():
    source_records, facts = parse_sample([
        source_row(LicenseNumber="", Status="Denied Application"),
        source_row(LicenseNumber="", FirstName="Casey", Status="Closed Application"),
        source_row(), source_row(Board="Osteopathy", Profession="Osteopathic Physician", DegreeEarned="DO"),
    ])
    assert len(source_records) == 4 and len({record["record_id"] for record in source_records}) == 4
    assert len(facts) == 6
    assert all(record["normalized_payload"]["visibility"] == "held_identity" for record in source_records[:2])
    assert all(record["match_evidence"]["reason"] == "license_number_missing" for record in source_records[:2])
    assert all(fact["source_record_id"] not in {record["record_id"] for record in source_records[:2]} for fact in facts)
    assert {record["profession_code"] for record in source_records[2:]} == {"1606", "1907"}


@pytest.mark.parametrize("change", [{"FirstName": "Different"}, {"MiddleName": "Different"},
    {"Title": "Jr"}, {"Status": "Expired"}, {"LastName": ""}])
def test_conflicting_identity_holds_entire_license(change):
    source_records, facts = parse_sample([source_row(), source_row(**change)])
    assert len(source_records) == 1 and facts == []
    assert source_records[0]["match_status"] == "identity_conflict"
    assert source_records[0]["normalized_payload"]["visibility"] == "held_identity"
    assert len(source_records[0]["raw_payload"]["rows"]) == 2


def test_same_school_with_distinct_degrees_and_dates():
    _, facts = parse_sample([source_row(), source_row(DegreeEarned="PHD"), source_row(GraduationDate="06/16/1998")])
    education_facts = [fact for fact in facts if fact["category"] == "education"]
    assert len(education_facts) == 3 and len({fact["logical_fact_key"] for fact in education_facts}) == 3
    assert len({fact["value_json"]["graduation_year"] for fact in education_facts}) == 1


@pytest.mark.parametrize("modifier_type", ["Board Certification", "Unknown", ""])
def test_unrecognized_modifiers_are_retained_only(modifier_type):
    source_records, facts = parse_sample([source_row(ModifierType=modifier_type)])
    assert len(facts) == 2
    assert source_records[0]["raw_payload"]["rows"][0]["fields"]["ModifierType"] == modifier_type
    assert "modifier_unmapped" in source_records[0]["normalized_payload"]["quality_flags"]
    assert not any(fact["category"] == "certifications" for fact in facts)


def test_blank_facts_do_not_infer_education():
    empty = source_row(**dict.fromkeys(FACT_COLUMNS, ""))
    source_records, facts = parse_sample([empty])
    assert facts == [] and source_records[0]["normalized_payload"]["visibility"] == "public"
    empty["DegreeEarned"] = "PREMED"
    assert parse_sample([empty])[1][0]["value_json"] == {"degree": "PREMED"}


@pytest.mark.parametrize(("date_text", "expected", "flag"), [
    ("02/29/2000", "2000-02-29", None), ("02/29/2001", "02/29/2001", "graduation_date_invalid"),
    ("2001", "2001", "graduation_date_invalid"), ("13/01/2001", "13/01/2001", "graduation_date_invalid"),
    ("01/01/1700", "1700-01-01", "graduation_date_before_1800"),
    ("01/01/2030", "2030-01-01", "graduation_date_in_future"),
])
def test_reported_dates_keep_precision_and_quality(date_text, expected, flag):
    _, facts = parse_sample([source_row(GraduationDate=date_text)])
    education = next(fact for fact in facts if fact["category"] == "education")
    assert education["value_json"]["graduation_date"] == expected
    assert education["source_json"]["raw_fields"]["GraduationDate"] == date_text
    assert (flag in education["source_json"]["quality_flags"]) if flag else not education["source_json"]["quality_flags"]
    assert education["effective_start"] is education["effective_end"] is None


def test_training_dates_never_supply_program_type():
    _, facts = parse_sample([source_row(OtherTrainingEndDate="01/01/1990")])
    training = next(fact for fact in facts if fact["category"] == "training")
    assert training["fact_type"] == "other_training"
    assert training["effective_start"] == "1998-07-01" and training["effective_end"] == "1990-01-01"
    assert training["source_json"]["quality_flags"] == ["training_period_reversed"]
    assert "program_type" not in training["value_json"] and "specialty" not in training["value_json"]


def test_ids_are_stable_across_row_order_and_runs():
    source_rows = [source_row(), source_row(EducationProvider="Another Synthetic School")]
    first_records, first_facts = parse_sample(source_rows)
    second_records, second_facts = parse_sample(list(reversed(source_rows)))
    assert first_records[0]["source_record_key"] == second_records[0]["source_record_key"]
    assert {fact["logical_fact_key"] for fact in first_facts} == {fact["logical_fact_key"] for fact in second_facts}
    content = report_bytes(source_rows)
    third_records, third_facts = rows.parse_report(content, evidence={**EVIDENCE, "run_id": "another-run",
        "content_sha256": hashlib.sha256(content).hexdigest()})
    assert third_records[0]["record_id"] != first_records[0]["record_id"]
    assert {fact["logical_fact_key"] for fact in third_facts} == {fact["logical_fact_key"] for fact in first_facts}


@pytest.mark.parametrize(("category", "field"), [
    ("education", "EducationProvider"), ("education", "DegreeEarned"),
    ("education", "GraduationDate"), ("training", "OtherTrainingProvider"),
    ("training", "OtherTrainingEndDate"), ("specialties", "ModifierDescription"),
])
def test_normalized_facts_merge_whitespace_and_keep_capture_identity(category, field):
    original = source_row(GraduationDate="01/01/2030", OtherTrainingEndDate="01/01/1990")
    value = original[field]
    variants = [value, value + " ", " " + value, value.replace(" ", "  "),
                value.replace(" ", "\n"), "\xa0" + value.replace(" ", "\xa0")]
    source_records, facts = parse_sample([{**original, field: variant} for variant in variants])
    assert len(facts) == 3 and len({fact["fact_id"] for fact in facts}) == 3
    fact = next(fact for fact in facts if fact["category"] == category)
    assert fact["source_json"]["row_numbers"] == list(range(1, len(variants) + 1))
    assert fact["source_json"]["raw_fields"][field] == value
    assert [row["fields"][field] for row in source_records[0]["raw_payload"]["rows"]] == variants
    assert source_records[0]["normalized_payload"]["quality_flags"] == [
        "graduation_date_in_future", "graduation_year_in_future", "training_period_reversed"]
    for variant in variants:
        content = report_bytes([{**original, field: variant}])
        other_records, other_facts = rows.parse_report(content, evidence={**EVIDENCE, "run_id": "next-capture",
            "content_sha256": hashlib.sha256(content).hexdigest()})
        other = next(fact for fact in other_facts if fact["category"] == category)
        assert other_records[0]["record_id"] != source_records[0]["record_id"]
        assert other["fact_id"] != fact["fact_id"]
        assert other["logical_fact_key"] == fact["logical_fact_key"]
        assert other["value_json"] == fact["value_json"] and other["display"] == fact["display"]
        assert other["source_json"]["quality_flags"] == fact["source_json"]["quality_flags"]


def test_equal_display_dates_with_different_precision_stay_distinct():
    _, facts = parse_sample([source_row(GraduationDate=value) for value in ["06/15/1998", "1998-06-15"]])
    education_facts = [fact for fact in facts if fact["category"] == "education"]
    assert len(education_facts) == 2 and len({fact["logical_fact_key"] for fact in education_facts}) == 2
    assert education_facts[0]["display"] == education_facts[1]["display"]
    assert {fact["value_json"]["graduation_date_precision"] for fact in education_facts} == {"day", "source"}
    assert education_facts[1]["source_json"]["quality_flags"] == ["graduation_date_invalid"]


@pytest.mark.parametrize("headers", [HEADERS[:-1], HEADERS + ("Unexpected",), HEADERS[:-1] + (HEADERS[0],),
                                   PRACTICE_HEADERS[:-1], PRACTICE_HEADERS + ("Gender",)])
def test_changed_header_is_rejected(headers):
    with pytest.raises(ValueError, match="^tennessee_profile_headers_invalid$"):
        parse_sample([source_row()], headers)


@pytest.mark.parametrize("content", [b"", b"<html>Service unavailable</html>", b"\xff", b"\x00"])
def test_nonreport_input_is_rejected(content):
    with pytest.raises(ValueError, match="^tennessee_profile_(headers_invalid|encoding_invalid|input_invalid)$"):
        rows.parse_report(content, evidence={**EVIDENCE, "content_sha256": hashlib.sha256(content).hexdigest()})


@pytest.mark.parametrize("tail", [b"\n", b"a,b\r\n", b'"unterminated'])
def test_truncated_or_extra_rows_are_rejected(tail):
    content = report_bytes([source_row()]) + tail
    with pytest.raises(ValueError, match="^tennessee_profile_(row_width_invalid|csv_invalid)$"):
        rows.parse_report(content, evidence={**EVIDENCE, "content_sha256": hashlib.sha256(content).hexdigest()})


@pytest.mark.parametrize("changes", [{"Board": "Other board"}, {"Profession": "Medical Doctor (Special Training)"},
    {"LicenseNumber": "12A"}, {"LicenseNumber": "１２３"}])
def test_unverified_source_identity_is_rejected(changes):
    with pytest.raises(ValueError, match="^tennessee_profile_(profession_invalid|license_invalid)$"):
        parse_sample([source_row(**changes)])


@pytest.mark.parametrize("changes", [{"run_id": ""}, {"artifact_id": 1}, {"downloaded_at": "not-a-date"},
    {"downloaded_at": "2026-09-09"}, {"source_url": ""}, {"content_sha256": "a" * 64}])
def test_invalid_artifact_evidence_is_rejected(changes):
    content = report_bytes([source_row()])
    evidence_by_field = {**EVIDENCE, "content_sha256": hashlib.sha256(content).hexdigest(), **changes}
    with pytest.raises(ValueError, match="^tennessee_profile_evidence_"):
        rows.parse_report(content, evidence=evidence_by_field)


def test_bom_multiline_and_reordered_headers():
    original = source_row(EducationProvider='Synthetic, "Medical"\nSchool')
    content = b"\xef\xbb\xbf" + report_bytes([original], tuple(reversed(HEADERS)))
    evidence_by_field = {**EVIDENCE, "content_sha256": hashlib.sha256(content).hexdigest()}
    before = copy.deepcopy(evidence_by_field)
    source_records, facts = rows.parse_report(content, evidence=evidence_by_field)
    assert evidence_by_field == before
    assert source_records[0]["raw_payload"]["rows"][0]["fields"]["EducationProvider"] == original["EducationProvider"]
    assert facts[0]["value_json"]["institution"] == 'Synthetic, "Medical" School'
    assert parse_sample([]) == ([], [])


def test_report_and_field_limits(monkeypatch):
    monkeypatch.setattr(rows, "MAX_REPORT_BYTES", 10)
    with pytest.raises(ValueError, match="^tennessee_profile_input_invalid$"):
        parse_sample([source_row()])
    monkeypatch.setattr(rows, "MAX_REPORT_BYTES", 1_000_000)
    monkeypatch.setattr(rows, "MAX_ROWS", 1)
    with pytest.raises(ValueError, match="^tennessee_profile_row_limit$"):
        parse_sample([source_row(), source_row()])
    monkeypatch.setattr(rows, "MAX_FIELD_CHARS", 3)
    with pytest.raises(ValueError, match="^tennessee_profile_field_limit$"):
        parse_sample([source_row()])


def test_late_malformed_row_holds_valid_rows_and_preserves_source_coordinates():
    valid = quoted_row(source_row(EducationProvider='Synthetic, "Medical"\nSchool'))
    malformed = malformed_row()
    content = report_bytes([], PRACTICE_HEADERS) + valid + malformed + quoted_row(source_row(LicenseNumber="000999"))
    records, facts = parse_content(content)
    held, unaffected = records
    assert held["license_number"] == "000123" and held["matched_npi"] is None
    assert held["normalized_payload"]["visibility"] == "held_identity"
    assert held["match_evidence"]["reason"] == "malformed_source_row"
    assert len(held["raw_payload"]["rows"]) == 1
    assert held["raw_payload"]["rows"][0]["fields"]["EducationProvider"] == 'Synthetic, "Medical"\nSchool'
    error, = held["raw_payload"]["malformed_rows"]
    assert error["row_number"] == 2 and error["physical_line_start"] == error["physical_line_end"] == 4
    assert error["raw_text"].encode("utf-8") == malformed
    assert error["content_sha256"] == hashlib.sha256(malformed).hexdigest()
    assert set(error["fields"]) == set(BASE_COLUMNS) and error["reason"] == "csv_quote_invalid"
    assert len(facts) == 3 and {fact["source_record_id"] for fact in facts} == {unaffected["record_id"]}
    assert unaffected["raw_payload"]["rows"][0]["row_number"] == 3


@pytest.mark.parametrize("field", ["EducationProvider", "OtherTrainingProvider", "PracticeName", "PracticeAddress",
                                 "PracticeCity", "PracticeAddress2", "PracticeZIP"])
def test_malformed_only_license_retains_exact_line_without_fact_interpretation(field):
    malformed = malformed_row(field=field)
    records, facts = parse_content(report_bytes([], PRACTICE_HEADERS) + malformed)
    assert len(records) == 1 and facts == []
    assert records[0]["raw_payload"]["rows"] == []
    assert records[0]["raw_payload"]["malformed_rows"][0]["raw_text"].encode() == malformed
    assert records[0]["normalized_payload"]["quality_flags"] == ["malformed_source_row"]
    assert records[0]["matched_npi"] is None and records[0]["match_status"] == "unmatched"


def test_malformed_row_before_valid_row_holds_whole_license():
    content = report_bytes([], PRACTICE_HEADERS) + malformed_row() + quoted_row(source_row()) + malformed_row()
    records, facts = parse_content(content)
    assert facts == [] and len(records) == 1
    assert len(records[0]["raw_payload"]["rows"]) == 1
    assert [entry["row_number"] for entry in records[0]["raw_payload"]["malformed_rows"]] == [1, 3]


@pytest.mark.parametrize("headers", [HEADERS, FULL_HEADERS, tuple(reversed(PRACTICE_HEADERS))])
def test_unobserved_malformed_layouts_still_reject_report(headers):
    with pytest.raises(ValueError, match="^tennessee_profile_csv_invalid$"):
        parse_content(report_bytes([], headers) + malformed_row(headers=headers))


@pytest.mark.parametrize("changes", [{"LicenseNumber": ""}, {"LicenseNumber": "12A"}, {"FirstName": ""},
                                    {"LastName": ""}, {"Board": "Unknown"}, {"Profession": "Medical Doctor (Special Training)"}])
def test_unidentifiable_malformed_record_rejects_entire_report(changes):
    content = report_bytes([], PRACTICE_HEADERS) + quoted_row(source_row()) + malformed_row(**changes)
    with pytest.raises(ValueError, match="^tennessee_profile_csv_invalid$"):
        parse_content(content)


@pytest.mark.parametrize("corruption", ["identity", "multiline", "lf_only", "unterminated", "extra_delimiter"])
def test_unsupported_corruption_is_never_recovered(corruption):
    malformed = malformed_row(field="FirstName") if corruption == "identity" else malformed_row()
    if corruption == "multiline":
        malformed = malformed.replace(b'Synthetic "Quoted"', b'Synthetic\n"Quoted"')
    if corruption == "lf_only":
        malformed = malformed.replace(b"\r\n", b"\n")
    if corruption == "unterminated":
        malformed = malformed[:-3]
    if corruption == "extra_delimiter":
        malformed = malformed.replace(b'"Quoted"', b'"Quoted","Extra"')
    content = report_bytes([], PRACTICE_HEADERS) + malformed + quoted_row(source_row())
    with pytest.raises(ValueError, match="^tennessee_profile_csv_invalid$"):
        parse_content(content)


def test_quarantined_rows_obey_unchanged_row_and_field_caps(monkeypatch):
    malformed = malformed_row()
    monkeypatch.setattr(rows, "MAX_ROWS", 1)
    with pytest.raises(ValueError, match="^tennessee_profile_row_limit$"):
        parse_content(report_bytes([], PRACTICE_HEADERS) + malformed * 2)
    monkeypatch.setattr(rows, "MAX_FIELD_CHARS", len(malformed.decode()) - 1)
    with pytest.raises(ValueError, match="^tennessee_profile_field_limit$"):
        parse_content(report_bytes([], PRACTICE_HEADERS) + malformed)
