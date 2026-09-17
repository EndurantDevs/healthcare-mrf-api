# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import copy
from datetime import datetime

import pytest

from process import massachusetts_profile_rows as rows

NPI = "1000000004"
OTHER_NPI = "1000000012"
LICENSE = "123456"
EVIDENCE = {
    "run_id": "synthetic-run", "artifact_id": "synthetic-artifact",
    "source_url": "https://example.test/public/profile/123456",
    "downloaded_at": "2026-09-08T00:00:00Z", "content_sha256": "a" * 64,
    "row_number": 7,
}
CANDIDATE = {
    "npi": NPI, "license_number": LICENSE, "first_name": "Alex",
    "last_name": "Example", "taxonomy": "208D00000X",
}
PROFILE = {
    "id": 123, "licenseNumber": LICENSE, "licenseMetaId": 1,
    "firstName": "Alex", "lastName": "Example", "npiNumber": NPI,
    "degree": "M.D. or M.D. Equivalent", "status": "Active",
    "educationAndTrainings": {
        "education": {"name": " Example Medical School ", "graduationDate": "2001-06-01T00:00:00"},
        "trainings": [{
            "programType": "Resident", "specialty": "Example Specialty",
            "location": "Example Hospital", "startDate": "2001-07-01",
            "endDate": "2004-06-30", "isAccredited": True,
        }],
    },
}


def parse(profile=None, candidates=None, **kwargs):
    return rows.parse_profile(
        copy.deepcopy(PROFILE if profile is None else profile),
        license_number=kwargs.pop("license_number", LICENSE),
        candidates=copy.deepcopy([CANDIDATE] if candidates is None else candidates),
        evidence=kwargs.pop("evidence", EVIDENCE), **kwargs,
    )


def test_matched_fields_preserve_raw_provenance_without_claiming_degree_or_experience():
    original = copy.deepcopy(PROFILE)
    source_record, facts = parse()
    assert source_record["matched_npi"] == int(NPI)
    assert source_record["match_status"] == "deterministic"
    assert source_record["raw_payload"] == original
    assert source_record["source_key"] == rows.SOURCE_KEY == "massachusetts-borim"
    assert source_record["source_record_key"] == f"{rows.SOURCE_KEY}:{LICENSE}"
    assert [(fact["category"], fact["fact_type"]) for fact in facts] == [
        ("education", "education_history"), ("training", "postgraduate_training"),
    ]
    assert facts[0]["value_json"] == {
        "institution": "Example Medical School", "graduation_date": "2001-06-01",
        "graduation_date_precision": "day", "graduation_year": 2001,
    }
    assert facts[1]["effective_start"] == "2001-07-01"
    assert facts[1]["effective_end"] == "2004-06-30"
    for fact in facts:
        assert fact["npi"] == int(NPI)
        assert fact["published_at"] is None
        assert fact["source_record_id"] == source_record["record_id"]
        assert fact["assertion_type"] == "self_reported"
        assert fact["verification_status"] == "not_independently_verified"
        assert fact["source_json"]["source_url"] == EVIDENCE["source_url"]
        assert "isAccredited" not in str(fact)
        assert "degree" not in fact["value_json"]
        assert "experience" not in str(fact)
    assert facts[0]["source_json"]["raw_fields"]["name"] == " Example Medical School "
    facts[0]["source_json"]["raw_fields"]["name"] = "changed"
    assert PROFILE == original == source_record["raw_payload"]


def test_identity_is_stable_across_runs_and_retains_duplicate_training_assertions():
    profile = copy.deepcopy(PROFILE)
    profile["educationAndTrainings"]["trainings"] *= 2
    source_record, facts = parse(profile)
    again_record, again = parse(profile, evidence={**EVIDENCE, "run_id": "next", "artifact_id": "next"})
    assert source_record["record_id"] != again_record["record_id"]
    assert [fact["logical_fact_key"] for fact in facts] == [fact["logical_fact_key"] for fact in again]
    assert len({fact["fact_id"] for fact in facts}) == 3
    assert len({fact["logical_fact_key"] for fact in facts}) == 3
    assert [fact["source_json"]["source_path"] for fact in facts[1:]] == [
        "educationAndTrainings.trainings[0]", "educationAndTrainings.trainings[1]",
    ]


def test_public_evidence_does_not_spread_unrelated_caller_fields():
    _, facts = parse(evidence={**EVIDENCE, "raw_profile": {"hidden_note": "private"}})
    assert all("raw_profile" not in fact["source_json"] for fact in facts)


@pytest.mark.parametrize("family", ["abms", "aoa"])
def test_board_evidence_keeps_public_fields(family):
    board_by_field = {"boardName": " Example Board ", "specialties": [" Internal Medicine "],
                      "subspecialties": [" Example Subspecialty "], "unlisted": {"note": "synthetic withheld detail"}}
    profile_by_field = {**PROFILE, "boardCertifications": {family: [board_by_field]}}
    source_record, facts = parse(profile_by_field, categories=rows.PROFILE_CATEGORIES)
    certification = next(fact for fact in facts if fact["category"] == "certifications")
    assert certification["source_json"]["raw_fields"] == {
        key: board_by_field[key] for key in ("boardName", "specialties", "subspecialties")
    }
    assert "unlisted" not in str(facts) and "synthetic withheld detail" not in str(facts)
    assert source_record["raw_payload"] == profile_by_field
    certification["source_json"]["raw_fields"]["specialties"].append("changed")
    assert source_record["raw_payload"] == profile_by_field


def test_captured_cohort_is_trusted_without_taxonomy_prefix_guesses():
    candidate_by_field = {**CANDIDATE, "taxonomy": None, "license_number": " 123456 \t"}
    original = copy.deepcopy(candidate_by_field)
    source_record, facts = rows.parse_profile(
        PROFILE, license_number=LICENSE, candidates=[candidate_by_field], evidence=EVIDENCE,
    )
    assert source_record["matched_npi"] == int(NPI)
    assert facts
    assert candidate_by_field == original
    assert source_record["raw_payload"]["licenseNumber"] == LICENSE


@pytest.mark.parametrize(("profile_changes", "candidate_changes", "expected"), [
    ({}, {}, "deterministic"),
    ({"firstName": "Ａｌｅｘ", "lastName": " EXAMPLE "}, {}, "deterministic"),
    ({"firstName": "A"}, {}, "deterministic"),
    ({}, {"first_name": "A"}, "deterministic"),
    ({"firstName": "Andrew"}, {}, "identity_conflict"),
    ({"firstName": ""}, {}, "identity_conflict"),
    ({"lastName": "Different"}, {}, "identity_conflict"),
    ({"npiNumber": "1000000005"}, {}, "identity_conflict"),
    ({"npiNumber": OTHER_NPI}, {}, "identity_conflict"),
    ({}, {"license_number": "0123456"}, "identity_conflict"),
    ({}, {"license_number": " 123456 \t"}, "deterministic"),
    ({}, {"npi": "1000000005"}, "identity_conflict"),
    ({"npiNumber": None}, {}, "deterministic"),
    ({"npiNumber": ""}, {}, "deterministic"),
    ({"npiNumber": None, "firstName": "A"}, {}, "identity_conflict"),
])
def test_conservative_identity_matching(profile_changes, candidate_changes, expected):
    source_record, facts = parse({**PROFILE, **profile_changes}, [{**CANDIDATE, **candidate_changes}])
    assert source_record["match_status"] == expected
    assert source_record["matched_npi"] == (int(NPI) if expected == "deterministic" else None)
    assert all(fact["npi"] == source_record["matched_npi"] for fact in facts)
    assert source_record["raw_payload"]["npiNumber"] == profile_changes.get("npiNumber", NPI)


def test_missing_npi_requires_unique_person_and_deduplicates_same_npi_taxonomy_rows():
    profile_by_field = {**PROFILE, "npiNumber": None}
    source_record, facts = parse(profile_by_field, [CANDIDATE, {**CANDIDATE, "npi": OTHER_NPI}])
    assert source_record["match_status"] == "ambiguous"
    assert source_record["matched_npi"] is None
    assert facts and all(fact["npi"] is None for fact in facts)
    assert parse(profile_by_field, [CANDIDATE, CANDIDATE])[0]["matched_npi"] == int(NPI)
    assert parse(profile_by_field, [])[0]["match_status"] == "unmatched"
    assert parse(PROFILE, [CANDIDATE, {**CANDIDATE, "npi": OTHER_NPI}])[0]["matched_npi"] == int(NPI)
    assert parse(PROFILE, [CANDIDATE, {**CANDIDATE, "first_name": "Different"}])[0]["matched_npi"] is None


@pytest.mark.parametrize("changes", [
    {"licenseMetaId": 2}, {"licenseMetaId": 7}, {"licenseNumber": "999999"},
    {"isPendingReview": True}, {"isPendingReview": "unexpected truthy flag"},
])
def test_wrong_identity_or_pending_review_holds_raw_record_without_hidden_facts(changes):
    profile_by_field = {**PROFILE, **changes}
    profile_by_field["educationAndTrainings"] = {"hidden": "must not even parse this schema"}
    source_record, facts = parse(profile_by_field)
    assert source_record["matched_npi"] is None
    assert facts == []
    assert source_record["raw_payload"] == profile_by_field
    assert source_record["normalized_payload"]["visibility"].startswith("held_")


@pytest.mark.parametrize("profile", [
    {"error": "temporary upstream error"}, {"licenseNumber": LICENSE}, {"licenseMetaId": 1},
    {**PROFILE, "licenseMetaId": None}, {**PROFILE, "licenseMetaId": True},
    {**PROFILE, "licenseMetaId": "1"}, {**PROFILE, "licenseNumber": None},
    {**PROFILE, "licenseNumber": 123456}, {**PROFILE, "licenseNumber": []},
    {**PROFILE, "licenseNumber": ""}, {**PROFILE, "licenseNumber": "  "},
])
def test_error_objects_and_malformed_core_identity_fail_schema_validation(profile):
    with pytest.raises(ValueError, match="massachusetts_profile_identity_schema_invalid"):
        parse(profile)


def test_minimal_pending_profile_does_not_require_hidden_details():
    source_record, facts = parse({"licenseNumber": LICENSE, "licenseMetaId": 1, "isPendingReview": True})
    assert source_record["normalized_payload"]["visibility"] == "held_pending_review"
    assert facts == []


@pytest.mark.parametrize("education", [None, {}, {"name": ""}, {"name": "  ", "graduationDate": "2000"}])
def test_missing_school_name_hides_entire_section_like_public_ui(education):
    profile = copy.deepcopy(PROFILE)
    profile["educationAndTrainings"]["education"] = education
    source_record, facts = parse(profile)
    assert source_record["matched_npi"] == int(NPI)
    assert source_record["normalized_payload"]["visibility"] == "education_section_hidden"
    assert facts == []
    assert source_record["raw_payload"] == profile


@pytest.mark.parametrize(("raw", "normalized", "precision", "year", "flag"), [
    ("2001", "2001", "year", 2001, None),
    ("2001-06", "2001-06", "month", 2001, None),
    ("2001-06-01", "2001-06-01", "day", 2001, None),
    ("2001-06-01T00:00:00+14:00", "2001-06-01", "day", 2001, None),
    ("2001-06-01T00:00:00.000Z", "2001-06-01", "day", 2001, None),
    ("2030-01-01", "2030-01-01", "day", 2030, "graduation_year_in_future"),
    ("2026-12-01", "2026-12-01", "day", 2026, "graduation_date_in_future"),
    ("1799", "1799", "year", 1799, "graduation_date_before_1800"),
    ("2001-02-29", "2001-02-29", "source", None, "graduation_date_invalid"),
    ("2001-13", "2001-13", "source", None, "graduation_date_invalid"),
    ("0000", "0000", "source", None, "graduation_date_invalid"),
    ("2001-06-01 junk", "2001-06-01 junk", "source", None, "graduation_date_invalid"),
    ("２００１", "２００１", "source", None, "graduation_date_invalid"),
])
def test_source_date_precision_and_quality_flags(raw, normalized, precision, year, flag):
    profile = copy.deepcopy(PROFILE)
    profile["educationAndTrainings"]["education"]["graduationDate"] = raw
    _, facts = parse(profile)
    value = facts[0]["value_json"]
    assert value["graduation_date"] == normalized
    assert value["graduation_date_precision"] == precision
    assert value.get("graduation_year") == year
    assert flag in facts[0]["source_json"]["quality_flags"] if flag else not facts[0]["source_json"]["quality_flags"]
    assert facts[0]["source_json"]["raw_fields"]["graduationDate"] == raw


def test_partial_and_legacy_training_dates_retain_uncertainty_and_raw_values():
    profile = copy.deepcopy(PROFILE)
    profile["educationAndTrainings"]["education"]["graduationDate"] = None
    profile["educationAndTrainings"]["trainings"] = [
        {"location": "Example Institute", "programType": "Fellow", "startDate": "2030-07-01", "endDate": None},
        {"programType": None, "specialty": "Resident:Example Specialty", "startDate": "malformed", "endDate": "2004"},
        {"startDate": "2005-01-01", "endDate": "2004-01-01"},
        {"location": "Undated Example Hospital"}, {},
    ]
    source_record, facts = parse(profile, evidence={**EVIDENCE, "downloaded_at": datetime(2026, 9, 8)})
    assert len(facts) == 5
    assert facts[0]["value_json"] == {"institution": "Example Medical School"}
    assert facts[1]["effective_end"] is None
    assert facts[1]["source_json"]["quality_flags"] == ["attendance_start_in_future"]
    assert facts[2]["value_json"]["specialty"] == "Resident:Example Specialty"
    assert "program_type" not in facts[2]["value_json"]
    assert facts[2]["effective_start"] is None
    assert facts[2]["effective_end"] == "2004"
    assert facts[2]["source_json"]["quality_flags"] == ["attendance_start_invalid"]
    assert facts[3]["source_json"]["quality_flags"] == ["training_period_reversed"]
    assert facts[4]["effective_start"] is None
    assert "completed" not in str(facts) and "student" not in str(facts)
    assert source_record["raw_payload"]["educationAndTrainings"]["trainings"] == profile["educationAndTrainings"]["trainings"]


@pytest.mark.parametrize("section", [None, {"education": {"name": "School"}}, {"education": {"name": "School"}, "trainings": None}])
def test_absent_optional_section_or_trainings(section):
    _, facts = parse({**PROFILE, "educationAndTrainings": section})
    assert len(facts) == (0 if section is None else 1)


@pytest.mark.parametrize("section", [
    [], {"education": []}, {"education": {"name": {}}},
    {"education": {"name": "School"}, "trainings": {}},
    {"education": {"name": "School"}, "trainings": ["wrong"]},
    {"education": {"name": "School", "graduationDate": []}},
])
def test_malformed_section_schema_fails_closed(section):
    with pytest.raises(ValueError, match="massachusetts_profile_"):
        parse({**PROFILE, "educationAndTrainings": section})


@pytest.mark.parametrize("kwargs", [
    {"profile": {}}, {"profile": []}, {"license_number": "１２３４５６"},
    {"license_number": "MD123456"}, {"license_number": " 123456 "},
    {"candidates": {}}, {"candidates": [None]}, {"evidence": {}},
])
def test_invalid_parser_contract_fails_closed(kwargs):
    with pytest.raises(ValueError, match="massachusetts_profile_"):
        parse(**kwargs)
