# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import copy
import hashlib
import json
from datetime import datetime, timezone

import pytest

from process import rhode_island_profile_rows as rows


def occurrence(license_number="MD00001", **changes):
    profile = dict.fromkeys(rows.PROFILE_FIELDS, "")
    profile.update(
        {
            "License_No": license_number,
            "Profession_Name": "Physician",
            "Primary_License_Type_Name": rows.LICENSE_TYPES[license_number[:2]],
            "First_Name": "Alex",
            "Last_Name": "Example",
            "Sort_Name": "EXAMPLE ALEX",
            "License_ID": "synthetic-id",
            "Profession_ID": "synthetic-profession",
            "Secondary_License_Status_Name": "Active",
            "School_Name": " Example Medical School ",
            "School_City": "Example City",
            "School_State": "RI",
            "School_Nation": "Example Country",
            "School_Grad_Year": "2001",
            "Specialty_Name": "Example Specialty",
            "Qualifying_Exam": "Example examination",
            "License_Issue_Date": "01/01/2002",
            **dict.fromkeys(rows.HOSPITALS, "N"),
            "Mirian_Hospital": "Y",
        }
    )
    profile.update(changes)
    return [profile[field] for field in rows.PROFILE_FIELDS]


def evidence(payload, license_number="MD00001"):
    return {
        "run_id": "synthetic-run",
        "artifact_id": "synthetic-artifact",
        "source_url": f"https://datahealth.ri.gov/find/providers/loadRecord.php?id={license_number}",
        "downloaded_at": "2026-09-09T00:00:00Z",
        "row_number": 1,
        "content_sha256": hashlib.sha256(payload).hexdigest(),
    }


def parse(values, license_number="MD00001"):
    payload = json.dumps(values).encode()
    return rows.parse_profile(payload, license_number=license_number, evidence=evidence(payload, license_number))


def test_datetime_evidence_and_blank_specialty_preserve_other_facts():
    values = occurrence(Specialty_Name=" \t ")
    payload = json.dumps(values).encode()
    observed_at = datetime(2026, 9, 9, tzinfo=timezone.utc)
    metadata = {**evidence(payload), "downloaded_at": observed_at}
    record, facts = rows.parse_profile(payload, license_number="MD00001", evidence=metadata)
    assert record["raw_payload"]["values"] == values
    assert {fact["category"] for fact in facts} == {"education", "privileges"}
    assert all(fact["source_json"]["downloaded_at"] == observed_at.isoformat() for fact in facts)
    assert metadata["downloaded_at"] is observed_at


@pytest.mark.parametrize("license_number", ["MD00001", "DO00001"])
def test_source_occurrences_and_fact_provenance(license_number):
    first = occurrence(license_number)
    raw_values = first + occurrence(license_number, Specialty_Name="Second Specialty")
    source_record, facts = parse(raw_values, license_number)
    education_facts = [fact for fact in facts if fact["category"] == "education"]
    specialties = [fact for fact in facts if fact["category"] == "specialties"]
    privileges = [fact for fact in facts if fact["category"] == "privileges"]
    assert source_record["raw_payload"] == {"fields": list(rows.PROFILE_FIELDS), "values": raw_values}
    assert source_record["normalized_payload"]["occurrence_count"] == 2
    assert source_record["matched_npi"] is None and source_record["match_status"] == "unmatched"
    assert len(education_facts) == 1 and education_facts[0]["value_json"] == {
        "institution": "Example Medical School",
        "city": "Example City",
        "state": "RI",
        "country": "Example Country",
        "graduation_date": "2001",
        "graduation_date_precision": "year",
        "graduation_year": 2001,
    }
    assert education_facts[0]["source_json"]["occurrence_indexes"] == [0, 1]
    assert education_facts[0]["source_json"]["source_paths"] == ["values[0:43]", "values[43:86]"]
    assert education_facts[0]["source_json"]["raw_fields"]["School_Name"] == " Example Medical School "
    assert [fact["value_json"]["text"] for fact in specialties] == ["Example Specialty", "Second Specialty"]
    assert len(privileges) == 1 and privileges[0]["fact_type"] == "staff_privilege"
    assert privileges[0]["source_json"]["occurrence_indexes"] == [0, 1]
    assert all(fact["value_json"] == {"institution": "Miriam Hospital"} for fact in privileges)
    assert len({fact["fact_id"] for fact in facts}) == len(facts)
    for fact in facts:
        assert fact["npi"] is None and fact["published_at"] is None
        assert fact["source_record_id"] == source_record["record_id"]
        assert fact["assertion_type"] == "source_reported"
        assert fact["verification_status"] == "not_independently_verified"
        assert fact["effective_start"] is None and fact["effective_end"] is None
        assert not {"degree", "certification", "experience", "qualifying_exam"} & fact["value_json"].keys()
    education_facts[0]["source_json"]["raw_fields"]["School_Name"] = "changed"
    assert source_record["raw_payload"]["values"] == raw_values


def test_conflicting_schools_years_and_locations_survive():
    changes = [
        {},
        {"Specialty_Name": "Second Specialty"},
        {"School_Grad_Year": "2002"},
        {"School_Name": "Other Example School"},
        {"School_City": "Another City"},
    ]
    values = sum((occurrence(**change) for change in changes), [])
    record, facts = parse(values)
    education_facts = [fact for fact in facts if fact["category"] == "education"]
    assert len(education_facts) == 4 and len({fact["logical_fact_key"] for fact in education_facts}) == 4
    assert [fact["source_json"]["occurrence_indexes"] for fact in education_facts] == [[0, 1], [2], [3], [4]]
    assert record["raw_payload"]["values"] == values
    assert sum(fact["category"] == "specialties" for fact in facts) == 2


@pytest.mark.parametrize("school", ["", " \t ", "Other", "UNKNOWN", "N/A", "Ｎ／Ａ", "not reported"])
def test_school_sentinels_do_not_create_education(school):
    record, facts = parse(occurrence(School_Name=school, Mirian_Hospital="N"))
    assert [fact["category"] for fact in facts] == ["specialties"]
    assert record["raw_payload"]["values"][rows.PROFILE_FIELDS.index("School_Name")] == school


@pytest.mark.parametrize(
    "year, flag",
    [
        ("not reported", "graduation_year_invalid"),
        ("0000", "graduation_year_invalid"),
        ("2001-02-03", "graduation_year_invalid"),
        ("２００１", "graduation_year_invalid"),
        ("3000", "graduation_year_in_future"),
        ("", None),
    ],
)
def test_year_quality_is_preserved_without_experience(year, flag):
    record, facts = parse(occurrence(School_Grad_Year=year))
    education = next(fact for fact in facts if fact["category"] == "education")
    assert education["source_json"]["raw_fields"]["School_Grad_Year"] == year
    assert record["normalized_payload"]["quality_flags"] == ([flag] if flag else [])
    if flag == "graduation_year_invalid":
        assert "graduation_year" not in education["value_json"]
        assert education["value_json"]["graduation_date_precision"] == "source"


@pytest.mark.parametrize(
    "change",
    [
        {"License_No": "MD00002"},
        {"Profession_Name": "Other"},
        {"Primary_License_Type_Name": "Osteopathic Physician (DO)"},
        {"First_Name": "Different"},
        {"License_ID": "different"},
        {"Mirian_Hospital": "unknown"},
    ],
)
def test_later_block_failure_prevents_all_facts(change, monkeypatch):
    def unexpected_facts(*args):
        pytest.fail("Facts were constructed before every block was validated")

    monkeypatch.setattr(rows, "_profile_facts", unexpected_facts)
    with pytest.raises(ValueError, match="rhode_island_profile_"):
        parse(occurrence() + occurrence(**change))


@pytest.mark.parametrize(
    "values", [[], {}, None, occurrence() + ["tail"], [occurrence()], [None] + occurrence()[1:], [1] + occurrence()[1:]]
)
def test_positional_shape_rejects_unknown_tail(values):
    with pytest.raises(ValueError, match="rhode_island_profile_"):
        parse(values)


@pytest.mark.parametrize("payload", [b"[", b"\xff", b"", b" " * (rows.MAX_JSON_BYTES + 1)])
def test_json_bytes_are_bounded(payload):
    with pytest.raises(ValueError, match="rhode_island_profile_"):
        rows.parse_profile(payload, license_number="MD00001", evidence=evidence(payload))


@pytest.mark.parametrize(
    "field, value",
    [
        ("source_url", "https://example.test/unbound"),
        ("content_sha256", "a" * 64),
        ("row_number", True),
        ("downloaded_at", "invalid"),
        ("artifact_id", ""),
    ],
)
def test_evidence_must_bind_exact_retained_bytes(field, value):
    payload = json.dumps(occurrence()).encode()
    metadata = evidence(payload)
    metadata[field] = value
    with pytest.raises(ValueError, match="rhode_island_profile_"):
        rows.parse_profile(payload, license_number="MD00001", evidence=metadata)


@pytest.mark.parametrize(
    "change",
    [
        {"artifact_file": "../page.json"},
        {"source_url": "https://datahealth.ri.gov/find/providers/results.php?license=MD00002"},
        {"source_url": "https://example.test/page"},
        {"content_sha256": ""},
        {"content_sha256": "g" * 64},
        {"content_sha256": None},
        {"downloaded_at": "not a timestamp"},
        {"downloaded_at": "2026-09-09T00:00:00"},
        {"downloaded_at": "2026-09-09"},
        {"downloaded_at": "0" * 65},
        {"downloaded_at": None},
        {"unrecognized": "field"},
        None,
        {},
    ],
)
def test_malformed_optional_schema_page_is_rejected(change):
    payload = json.dumps(occurrence()).encode()
    schema_page_by_field = {
        "artifact_file": "page.json",
        "source_url": "https://datahealth.ri.gov/find/providers/results.php?license=MD00001",
        "downloaded_at": "2026-09-09T00:00:00Z",
        "content_sha256": "a" * 64,
    }
    metadata = {**evidence(payload), "schema_page": {**schema_page_by_field, **change} if change else change}
    with pytest.raises(ValueError, match="rhode_island_profile_schema_page_"):
        rows.parse_profile(payload, license_number="MD00001", evidence=metadata)


@pytest.mark.parametrize("license_number", ["MD1", "md00001", " MD00001", "MD00001 ", "DO００００１", None])
def test_requested_license_is_literal(license_number):
    payload = json.dumps(occurrence()).encode()
    with pytest.raises(ValueError, match="rhode_island_profile_invalid_requested_license"):
        rows.parse_profile(payload, license_number=license_number, evidence=evidence(payload))


def test_repeated_raw_values_and_evidence_are_isolated():
    values = occurrence(School_Grad_Year="") * 2
    payload = json.dumps(values).encode()
    metadata = {**evidence(payload), "unrelated_private_note": "excluded"}
    original = copy.deepcopy(metadata)
    record, facts = rows.parse_profile(payload, license_number="MD00001", evidence=metadata)
    assert record["raw_payload"]["values"] == values and metadata == original
    assert all("unrelated_private_note" not in fact["source_json"] for fact in facts)
    later_record, later_facts = rows.parse_profile(
        payload,
        license_number="MD00001",
        evidence={**metadata, "run_id": "next"},
    )
    assert later_record["record_id"] != record["record_id"]
    assert [fact["logical_fact_key"] for fact in later_facts] == [fact["logical_fact_key"] for fact in facts]


def test_full_normalized_values_deduplicate_without_losing_raw_occurrences():
    first = occurrence(School_Grad_Year=" 3000 ")
    second = occurrence(
        School_Name="Example\tMedical  School",
        School_City=" Example   City ",
        School_Grad_Year="3000",
        Specialty_Name=" Example  Specialty ",
    )
    record, facts = parse(first + second)
    assert len(facts) == 3
    assert record["raw_payload"]["values"] == first + second
    for fact in facts:
        provenance = fact["source_json"]
        assert provenance["occurrence_indexes"] == [0, 1]
        assert provenance["source_paths"] == ["values[0:43]", "values[43:86]"]
        assert [raw["occurrence_index"] for raw in provenance["raw_occurrences"]] == [0, 1]
        assert provenance["raw_fields"] == provenance["raw_occurrences"][0]["raw_fields"]
    school = next(fact for fact in facts if fact["category"] == "education")
    assert school["source_json"]["quality_flags"] == ["graduation_year_in_future"]
    assert [entry["raw_fields"]["School_Grad_Year"] for entry in school["source_json"]["raw_occurrences"]] == [
        " 3000 ",
        "3000",
    ]
    assert all(
        entry["quality_flags"] == ["graduation_year_in_future"] for entry in school["source_json"]["raw_occurrences"]
    )


def test_reordering_occurrences_does_not_change_fact_identity():
    first, second = occurrence(), occurrence(School_Grad_Year="2002", Specialty_Name="Second Specialty")
    _, left = parse(first + second)
    _, right = parse(second + first)
    assert {fact["fact_id"] for fact in left} == {fact["fact_id"] for fact in right}
    assert {fact["logical_fact_key"] for fact in left} == {fact["logical_fact_key"] for fact in right}


def test_full_value_conflicts_and_precision_remain_distinct():
    variants = [
        {},
        {"School_State": "MA"},
        {"School_Nation": "Other Country"},
        {"School_Grad_Year": "2001-01-01"},
        {"School_Grad_Year": "２００１"},
    ]
    _, facts = parse(sum((occurrence(**change) for change in variants), []))
    schools = [fact for fact in facts if fact["category"] == "education"]
    assert len(schools) == len(variants)
    assert len({fact["fact_id"] for fact in schools}) == len(variants)
    assert schools[-1]["value_json"]["graduation_date_precision"] == "source"
    assert schools[-1]["source_json"]["quality_flags"] == ["graduation_year_invalid"]
