# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retain public NY education assertions before registry identity binding."""

from __future__ import annotations

import copy
import hashlib
import re
from datetime import datetime
from urllib.parse import parse_qsl, urlsplit

from process.florida_mqa_profile import _logical_fact_key
from process.massachusetts_profile_rows import _hash, _source_date
from process.provider_directory_projection_json import decoded_json_object

SOURCE_KEY = "new-york-nypp"
SCHEMA_VERSION = "ny-nypp-education/v1"
MAX_PROFILE_BYTES = 2_000_000
MAX_SECTION_ROWS = 2048
SECTIONS = {
    "medSchools": ("education", "education_history"),
    "gmeSchools": ("training", "postgraduate_training"),
    "boardCertification": ("certifications", "board_certification"),
}
TEXT_FIELDS = {
    "medSchools": (("schoolName", "institution"),),
    "gmeSchools": (("amahospital", "institution"), ("specialty", "specialty"),
                   ("completionIndc", "completion_indicator"),
                   ("primarySpecialtyIndicator", "primary_specialty_indicator")),
    "boardCertification": (("boardName", "certifying_board"), ("specialty", "specialty"),
                           ("statusCode", "status_code")),
}
DATE_FIELDS = {
    "medSchools": (("gradDate", "graduation_date"),),
    "gmeSchools": (("gmeStartDate", "attendance_start"), ("gmedate", "completion_date")),
    "boardCertification": (("certificationDate", "certification_date"), ("expirationDate", "expiration_date")),
}


def _require(condition, reason):
    if not condition:
        raise ValueError("new_york_profile_" + reason)


def _validated_evidence(body, license_number, physician_id, evidence):
    _require(isinstance(body, bytes) and 0 < len(body) <= MAX_PROFILE_BYTES, "body_invalid")
    _require(isinstance(license_number, str) and re.fullmatch(r"[0-9]{6}", license_number), "license_invalid")
    _require(isinstance(physician_id, str) and re.fullmatch(r"[1-9][0-9]{0,11}", physician_id), "physician_id_invalid")
    fields = ("run_id", "artifact_id", "source_url", "downloaded_at", "content_sha256", "row_number")
    _require(isinstance(evidence, dict) and set(fields) <= evidence.keys(), "evidence_missing")
    _require(all(isinstance(evidence[field], str) and evidence[field].strip() for field in fields[:-1])
             and type(evidence["row_number"]) is int and evidence["row_number"] > 0, "evidence_invalid")
    _require(evidence["content_sha256"] == hashlib.sha256(body).hexdigest(), "body_hash_mismatch")
    parsed_url = urlsplit(evidence["source_url"])
    query_pairs = parse_qsl(parsed_url.query, keep_blank_values=True)
    _require(parsed_url.scheme == "https" and parsed_url.netloc == "www.nydoctorprofile.com"
             and parsed_url.path == f"/api/v1/physician/profile/{physician_id}" and not parsed_url.fragment
             and len(query_pairs) == 2
             and dict(query_pairs) == {"sections": "EDUCATIONALL", "physicianId": physician_id}, "source_url_invalid")
    try:
        observed_at = datetime.fromisoformat(evidence["downloaded_at"])
    except ValueError as exc:
        raise ValueError("new_york_profile_observation_timestamp_invalid") from exc
    _require(observed_at.utcoffset() is not None, "observation_timezone_missing")
    return {field: evidence[field] for field in fields}, observed_at.date()


def _validated_profile(body, license_number, physician_id):
    response_by_field = decoded_json_object(body)
    _require({"status", "error", "errorMessage", "data"} <= response_by_field.keys()
             and type(response_by_field["status"]) is int and response_by_field["status"] == 200
             and response_by_field["error"] is None and response_by_field["errorMessage"] is None, "response_unsuccessful")
    profile_by_field = response_by_field["data"]
    _require(isinstance(profile_by_field, dict) and profile_by_field.get("physicianId") == physician_id, "profile_id_mismatch")
    identity_by_field = profile_by_field.get("phyInfo")
    text_fields = ("firstName", "middleName", "lastName", "suffix", "licenseNumber", "licenseDate", "nationalProviderId", "lastUpdated")
    _require(isinstance(identity_by_field, dict)
             and all(isinstance(identity_by_field.get(field), str) for field in text_fields), "identity_schema_invalid")
    _require(identity_by_field["firstName"].strip() and identity_by_field["lastName"].strip()
             and type(identity_by_field.get("physicianID")) is int
             and str(identity_by_field["physicianID"]) == physician_id
             and identity_by_field["licenseNumber"] == license_number, "identity_mismatch")
    for section in SECTIONS:
        section_rows = profile_by_field.get(section)
        required_fields = {field for field, _ in (*TEXT_FIELDS[section], *DATE_FIELDS[section])}
        _require(isinstance(section_rows, list) and len(section_rows) <= MAX_SECTION_ROWS, "section_incomplete")
        _require(all(isinstance(source_row, dict) and required_fields <= source_row.keys()
                     and all(isinstance(field_value, str) for field_value in source_row.values())
                     for source_row in section_rows), "row_schema_invalid")
    return response_by_field


def _retained_record(response_by_field, license_number, physician_id, evidence):
    source_record_key = f"{SOURCE_KEY}:{license_number}"
    return {
        "record_id": _hash([evidence["run_id"], source_record_key]),
        "run_id": evidence["run_id"], "artifact_id": evidence["artifact_id"],
        "source_key": SOURCE_KEY, "source_record_key": source_record_key,
        "profession_code": None, "license_id": physician_id, "license_number": license_number,
        "raw_payload": copy.deepcopy(response_by_field),
        "normalized_payload": {"schema_version": SCHEMA_VERSION, "visibility": "public", "quality_flags": []},
        "matched_npi": None, "match_status": "unmatched",
        "match_evidence": {"jurisdiction": "NY", "requested_license_number": license_number,
                           "source_profile_id": physician_id, "reason": "registry_binding_not_performed"},
        "row_number": evidence["row_number"],
    }


def _date_fields(source_text, field, observed_on):
    date_text = source_text.strip()
    if not date_text:
        return {}, []
    if re.fullmatch(r"[0-9]{4}(?:-[0-9]{2}-[0-9]{2})?", date_text):
        return _source_date(date_text, field, observed_on)
    return {field: date_text, f"{field}_precision": "source"}, [f"{field}_format_unverified"]


def _section_value(section, source_row, observed_on):
    value_by_field = {field: " ".join(source_row[source_field].split())
                      for source_field, field in TEXT_FIELDS[section] if source_row[source_field].strip()}
    flags = []
    for source_field, field in DATE_FIELDS[section]:
        date_fields_by_name, date_flags = _date_fields(source_row[source_field], field, observed_on)
        value_by_field.update(date_fields_by_name)
        flags.extend(date_flags)
    if section == "gmeSchools" and not source_row["completionIndc"].strip():
        flags.append("completion_unreported")
    return value_by_field, flags


def _source_fact(source_record, evidence, section, source_row, index, observed_on):
    value_by_field, flags = _section_value(section, source_row, observed_on)
    if not set(value_by_field) - {"completion_indicator", "primary_specialty_indicator", "status_code"}:
        return None
    category, fact_type = SECTIONS[section]
    source_path = f"data.{section}[{index}]"
    fact_key = f"{SOURCE_KEY}:{source_record['license_number']}:{source_path}:{_hash(value_by_field)}"
    logical_key = _logical_fact_key(category, fact_type, fact_key, value_by_field)
    display_fields = ("institution", "certifying_board", "specialty", "graduation_date", "attendance_start",
                      "completion_date", "certification_date", "expiration_date")
    return {
        "fact_id": _hash([source_record["record_id"], logical_key]),
        "run_id": source_record["run_id"], "npi": None, "source_record_id": source_record["record_id"],
        "logical_fact_key": logical_key, "category": category, "fact_type": fact_type,
        "display": " — ".join(str(value_by_field[field]) for field in display_fields if value_by_field.get(field)),
        "value_json": value_by_field, "availability": "available", "assertion_type": "source_reported",
        "verification_status": "not_independently_verified", "effective_start": None, "effective_end": None,
        "source_json": {**evidence, "source_key": SOURCE_KEY, "schema_version": SCHEMA_VERSION,
                        "agency": "New York State Department of Health", "jurisdiction": "NY",
                        "source_record_id": source_record["record_id"], "source_path": source_path,
                        "raw_fields": copy.deepcopy(source_row), "quality_flags": flags,
                        "information_date": source_row.get("informationDate"),
                        "profile_updated_at": source_record["raw_payload"]["data"]["phyInfo"]["lastUpdated"]},
        "sensitive": False, "public_default": True, "published_at": None,
    }


def parse_education(body, *, license_number, physician_id, evidence):
    """Keep distinct source rows; a later cohort binder must assign any public NPI."""
    provenance, observed_on = _validated_evidence(body, license_number, physician_id, evidence)
    response_by_field = _validated_profile(body, license_number, physician_id)
    source_record = _retained_record(response_by_field, license_number, physician_id, provenance)
    facts = []
    for section in SECTIONS:
        for index, source_row in enumerate(response_by_field["data"][section]):
            source_fact = _source_fact(source_record, provenance, section, source_row, index, observed_on)
            if source_fact is not None:
                facts.append(source_fact)
    return source_record, facts
