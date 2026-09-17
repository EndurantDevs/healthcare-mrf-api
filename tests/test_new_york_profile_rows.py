# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import copy
import hashlib
import json

import pytest

from api.provider_education import canonicalize_education_category
from process import new_york_profile_rows as rows
from process.florida_mqa_profile import _profile_item
from process.provider_directory_projection_types import ProviderDirectoryProjectionError

LICENSE = "654321"
PHYSICIAN_ID = "91001"
RESPONSE = {
    "status": 200, "error": None, "errorMessage": None, "backend": "synthetic",
    "data": {
        "physicianId": PHYSICIAN_ID,
        "phyInfo": {
            "physicianID": int(PHYSICIAN_ID), "firstName": "Alex", "middleName": "", "lastName": "Example",
            "suffix": "", "licenseNumber": LICENSE, "licenseDate": "07-01-2002",
            "nationalProviderId": "1000000004", "lastUpdated": "08-01-2026",
        },
        "medSchools": [{"schoolName": " Example Medical School ", "gradDate": "2001"}],
        "gmeSchools": [{
            "amahospital": "Example Hospital", "gmeStartDate": "07-01-2001", "gmedate": "06-30-2004",
            "completionIndc": " ", "primarySpecialtyIndicator": "", "specialty": "Example Specialty",
            "informationDate": "",
        }],
        "boardCertification": [{
            "boardName": "Example Board", "specialty": "Example Specialty", "certificationDate": "11-15-2004",
            "expirationDate": "", "informationDate": "08-01-2026", "statusCode": "",
        }],
        "practiceaddress": None, "boardSubcertifications": [], "profMember": [],
    },
}
EVIDENCE = {
    "run_id": "synthetic-ny-run", "artifact_id": "synthetic-ny-artifact", "row_number": 1,
    "source_url": f"https://www.nydoctorprofile.com/api/v1/physician/profile/{PHYSICIAN_ID}?sections=EDUCATIONALL&physicianId={PHYSICIAN_ID}",
    "downloaded_at": "2026-09-09T00:00:00Z",
}


def parsed_education(response=None, *, evidence=None, body=None, **kwargs):
    encoded = json.dumps(RESPONSE if response is None else response).encode() if body is None else body
    evidence_by_field = {**EVIDENCE, "content_sha256": hashlib.sha256(encoded).hexdigest(), **(evidence or {})}
    return rows.parse_education(encoded, license_number=kwargs.pop("license_number", LICENSE),
                                physician_id=kwargs.pop("physician_id", PHYSICIAN_ID), evidence=evidence_by_field, **kwargs)


def test_education_keeps_literal_training_completion_and_unbound_source_identity():
    source_record, facts = parsed_education()
    assert source_record["raw_payload"] == RESPONSE
    assert source_record["matched_npi"] is None
    assert source_record["match_evidence"]["reason"] == "registry_binding_not_performed"
    assert [fact["fact_type"] for fact in facts] == ["education_history", "postgraduate_training", "board_certification"]
    assert facts[0]["value_json"] == {"institution": "Example Medical School", "graduation_date": "2001",
                                       "graduation_date_precision": "year", "graduation_year": 2001}
    assert facts[1]["value_json"] == {
        "institution": "Example Hospital", "specialty": "Example Specialty", "attendance_start": "07-01-2001",
        "attendance_start_precision": "source", "completion_date": "06-30-2004", "completion_date_precision": "source",
    }
    assert "completion_unreported" in facts[1]["source_json"]["quality_flags"]
    assert facts[2]["value_json"]["certification_date"] == "11-15-2004"
    assert facts[2]["source_json"]["information_date"] == "08-01-2026"
    for fact in facts:
        assert fact["npi"] is None and fact["published_at"] is None
        assert fact["source_json"]["jurisdiction"] == "NY"
        assert fact["source_record_id"] == source_record["record_id"]
        assert fact["verification_status"] == "not_independently_verified"
        assert fact["effective_start"] is None and fact["effective_end"] is None
        assert not {"degree", "completed", "years_of_experience"} & fact["value_json"].keys()


def test_distinct_schools_conflicting_years_and_duplicate_rows_keep_separate_assertions():
    response = copy.deepcopy(RESPONSE)
    response["data"]["medSchools"] += [
        {"schoolName": "Other Medical School", "gradDate": "2003"},
        {"schoolName": "Example Medical School", "gradDate": "2004"},
        copy.deepcopy(response["data"]["medSchools"][0]),
    ]
    source_record, facts = parsed_education(response)
    again_record, again = parsed_education(response, evidence={"run_id": "next-run", "artifact_id": "next-artifact"})
    assert len(facts) == 6 and len({fact["fact_id"] for fact in facts}) == 6
    assert source_record["record_id"] != again_record["record_id"]
    assert [fact["logical_fact_key"] for fact in facts] == [fact["logical_fact_key"] for fact in again]
    assert [fact["source_json"]["source_path"] for fact in facts[:4]] == [f"data.medSchools[{index}]" for index in range(4)]
    assert response == source_record["raw_payload"]


def test_shared_school_canonicalizer_corroborates_cms_without_collapsing_distinct_education():
    response = copy.deepcopy(RESPONSE)
    response["data"]["medSchools"] += [
        {"schoolName": "Other Medical School", "gradDate": "2003"},
        {"schoolName": "Example Medical School", "gradDate": "2004"},
    ]
    source_record, facts = parsed_education(response)
    cms_fact_by_field = {
        "type": "education_history", "display": "Example Medical School — 2001",
        "value": {"institution": "  EXAMPLE\u00a0Medical School ", "graduation_year": 2001},
        "source_kinds": ["cms_doctors"], "source_record_ids": ["synthetic-cms-record"],
        "assertion_type": "reported", "verification_status": "not_independently_verified",
        "assertion_count": 1, "public_default": True, "sensitive": False,
    }
    category_by_field = {"items": [_profile_item(fact) for fact in facts if fact["category"] == "education"] + [cms_fact_by_field]}
    canonicalize_education_category(category_by_field)

    assert len(category_by_field["items"]) == 3
    corroborated_items = [fact for fact in category_by_field["items"] if fact["assertion_count"] == 2]
    assert len(corroborated_items) == 1
    corroborated_fact = corroborated_items[0]
    assert corroborated_fact["corroborated_fields"] == ["institution", "graduation_year"]
    assert corroborated_fact["source_kinds"] == ["cms_doctors", "state_regulator"]
    assert set(corroborated_fact["source_record_ids"]) == {source_record["record_id"], "synthetic-cms-record"}
    assert {assertion["source_kind"]: assertion["value"] for assertion in corroborated_fact["assertions"]} == {
        "cms_doctors": cms_fact_by_field["value"], "state_regulator": facts[0]["value_json"],
    }
    assert {fact["value"]["graduation_year"] for fact in category_by_field["items"]} == {2001, 2003, 2004}
    assert {fact["value"]["institution"] for fact in category_by_field["items"]} == {"Example Medical School", "Other Medical School"}
    assert all(fact["npi"] is None and fact["published_at"] is None for fact in facts)


@pytest.mark.parametrize("indicator", ["Y", "N", "UNKNOWN"])
def test_completion_indicators_remain_source_codes_without_status_inference(indicator):
    response = copy.deepcopy(RESPONSE)
    response["data"]["gmeSchools"][0]["completionIndc"] = indicator
    _, facts = parsed_education(response)
    assert facts[1]["value_json"]["completion_indicator"] == indicator
    assert "completed" not in facts[1]["value_json"]
    assert "completion_unreported" not in facts[1]["source_json"]["quality_flags"]


def test_future_and_invalid_years_keep_shared_quality_flags():
    response = copy.deepcopy(RESPONSE)
    response["data"]["medSchools"] = [{"schoolName": "", "gradDate": "2099"},
                                       {"schoolName": "Example Medical School", "gradDate": "0000"}]
    _, facts = parsed_education(response)
    assert facts[0]["value_json"]["graduation_year"] == 2099
    assert "graduation_year_in_future" in facts[0]["source_json"]["quality_flags"]
    assert "graduation_year" not in facts[1]["value_json"]
    assert "graduation_date_invalid" in facts[1]["source_json"]["quality_flags"]


@pytest.mark.parametrize("date_text", ["03/04/2001", "20010304", "03-04-2001"])
def test_unverified_date_formats_remain_literal_without_inferred_year(date_text):
    response = copy.deepcopy(RESPONSE)
    response["data"]["medSchools"][0]["gradDate"] = date_text
    _, facts = parsed_education(response)
    assert facts[0]["value_json"] == {"institution": "Example Medical School", "graduation_date": date_text,
                                       "graduation_date_precision": "source"}
    assert facts[0]["source_json"]["quality_flags"] == ["graduation_date_format_unverified"]


def test_iso_calendar_date_uses_shared_date_precision():
    response = copy.deepcopy(RESPONSE)
    response["data"]["medSchools"][0]["gradDate"] = "2001-03-04"
    _, facts = parsed_education(response)
    assert facts[0]["value_json"] == {"institution": "Example Medical School", "graduation_date": "2001-03-04",
                                       "graduation_date_precision": "day", "graduation_year": 2001}
    assert facts[0]["source_json"]["quality_flags"] == []


def test_empty_sections_and_blank_template_rows_are_retained_without_invented_facts():
    response = copy.deepcopy(RESPONSE)
    response["data"]["medSchools"] = [{"schoolName": "", "gradDate": ""}]
    response["data"]["gmeSchools"] = [{field: "" for field in response["data"]["gmeSchools"][0]}]
    response["data"]["boardCertification"] = []
    source_record, facts = parsed_education(response)
    assert source_record["raw_payload"] == response and facts == []


def test_provenance_is_detached_and_does_not_publish_unrelated_caller_fields():
    response = copy.deepcopy(RESPONSE)
    source_record, facts = parsed_education(response, evidence={"internal_note": "not-for-publication"})
    assert all("internal_note" not in fact["source_json"] for fact in facts)
    facts[0]["source_json"]["raw_fields"]["schoolName"] = "changed"
    assert response == source_record["raw_payload"] == RESPONSE


@pytest.mark.parametrize("changes,reason", [
    ({"status": 400, "error": ["No search criteria provided"]}, "response_unsuccessful"),
    ({"status": True}, "response_unsuccessful"),
    ({"errorMessage": "temporary source failure"}, "response_unsuccessful"),
    ({"data": None}, "profile_id_mismatch"),
])
def test_unsuccessful_json_is_not_an_empty_publication(changes, reason):
    with pytest.raises(ValueError, match=f"new_york_profile_{reason}"):
        parsed_education({**RESPONSE, **changes})


@pytest.mark.parametrize("section", list(rows.SECTIONS))
@pytest.mark.parametrize("replacement", [None, {}, "", [{"unexpected": "field"}]])
def test_missing_or_malformed_sections_cannot_erase_previous_source_data(section, replacement):
    response = copy.deepcopy(RESPONSE)
    response["data"][section] = replacement
    with pytest.raises(ValueError, match="new_york_profile_(section_incomplete|row_schema_invalid)"):
        parsed_education(response)


@pytest.mark.parametrize("field,replacement", [("physicianID", True), ("physicianID", 91002),
                                                ("licenseNumber", "0654321"), ("firstName", ""),
                                                ("middleName", None), ("nationalProviderId", 1000000004)])
def test_profile_identity_cannot_drift_or_coerce_types(field, replacement):
    response = copy.deepcopy(RESPONSE)
    response["data"]["phyInfo"][field] = replacement
    with pytest.raises(ValueError, match="new_york_profile_identity_(mismatch|schema_invalid)"):
        parsed_education(response)


@pytest.mark.parametrize("evidence,reason", [
    ({"content_sha256": "0" * 64}, "body_hash_mismatch"),
    ({"row_number": True}, "evidence_invalid"),
    ({"run_id": ""}, "evidence_invalid"),
    ({"downloaded_at": "not-a-date"}, "observation_timestamp_invalid"),
    ({"downloaded_at": "2026-09-09"}, "observation_timezone_missing"),
    ({"source_url": EVIDENCE["source_url"].replace("EDUCATIONALL", "PRACTICEINFOALL")}, "source_url_invalid"),
    ({"source_url": EVIDENCE["source_url"] + "&sections=EDUCATIONALL"}, "source_url_invalid"),
    ({"source_url": EVIDENCE["source_url"].replace("www.nydoctorprofile.com", "example.test")}, "source_url_invalid"),
])
def test_source_identity_and_observation_are_bound_to_exact_body(evidence, reason):
    with pytest.raises(ValueError, match=f"new_york_profile_{reason}"):
        parsed_education(evidence=evidence)


@pytest.mark.parametrize("body", [b'{"status":200,"status":400}', b'{"value":NaN}', b'[]', b'\xff'])
def test_shared_json_decoder_rejects_ambiguous_or_invalid_json(body):
    with pytest.raises(ProviderDirectoryProjectionError, match="provider_directory_projection_native_spool_invalid"):
        parsed_education(body=body)


def test_parser_bounds_body_and_row_count(monkeypatch):
    with pytest.raises(ValueError, match="new_york_profile_body_invalid"):
        parsed_education(body=b"x" * (rows.MAX_PROFILE_BYTES + 1))
    monkeypatch.setattr(rows, "MAX_SECTION_ROWS", 0)
    with pytest.raises(ValueError, match="new_york_profile_section_incomplete"):
        parsed_education()
