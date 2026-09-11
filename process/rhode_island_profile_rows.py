# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure parsing of retained Rhode Island physician profile JSON occurrences."""

from __future__ import annotations

import copy
import hashlib
import json
import re
from datetime import datetime

from api.provider_education import _graduation_year, _institution_key

SOURCE_KEY = "rhode-island-doh"
SCHEMA_VERSION = "ri-doh-profile/v1"
MAX_JSON_BYTES = 1_048_576
PROFILE_FIELDS = (
    "Bradley_Hospital",
    "Butler_Hospital",
    "First_Name",
    "Kent_County_Hospital",
    "Landmark_Hospital",
    "Last_Name",
    "License_Address_Line_1",
    "License_Address_Line_2",
    "License_Address_Line_3",
    "License_City",
    "License_Expiration_Date",
    "License_ID",
    "License_Issue_Date",
    "License_No",
    "License_Phone",
    "License_State",
    "License_ZIP",
    "Memorial_Hospital",
    "Middle_Name",
    "Mirian_Hospital",
    "Newport_Hospital",
    "Person_Gender",
    "Primary_License_Type_Code",
    "Primary_License_Type_Name",
    "Profession_ID",
    "Profession_Name",
    "Qualifying_Exam",
    "Rhode_Island_Hospital",
    "Roger_Williams_Hospital",
    "Saint_Joseph_Hospital",
    "School_City",
    "School_Grad_Year",
    "School_Name",
    "School_Nation",
    "School_State",
    "Secondary_License_Status_Name",
    "Slater_Hospital",
    "Sort_Name",
    "South_County_Hospital",
    "Specialty_Name",
    "VA_Hospital",
    "Westerly_Hospital",
    "Women_Infants_Hospital",
)
LICENSE_TYPES = {"MD": "Allopathic Physician (MD)", "DO": "Osteopathic Physician (DO)"}
SCHOOL_FIELDS = ("School_Name", "School_City", "School_State", "School_Nation", "School_Grad_Year")
HOSPITALS = {
    "Bradley_Hospital": "Bradley Hospital",
    "Butler_Hospital": "Butler Hospital",
    "Kent_County_Hospital": "Kent County Hospital",
    "Landmark_Hospital": "Landmark Hospital",
    "Memorial_Hospital": "Memorial Hospital",
    "Mirian_Hospital": "Miriam Hospital",
    "Newport_Hospital": "Newport Hospital",
    "Rhode_Island_Hospital": "Rhode Island Hospital",
    "Roger_Williams_Hospital": "Roger Williams Hospital",
    "Saint_Joseph_Hospital": "Saint Joseph Hospital",
    "Slater_Hospital": "Slater Hospital",
    "South_County_Hospital": "South County Hospital",
    "VA_Hospital": "VA Hospital",
    "Westerly_Hospital": "Westerly Hospital",
    "Women_Infants_Hospital": "Women and Infants Hospital",
}
_IDENTITY_FIELDS = (
    "License_ID",
    "Profession_ID",
    "Primary_License_Type_Code",
    "Sort_Name",
    "First_Name",
    "Middle_Name",
    "Last_Name",
)


def _require(condition, reason):
    if not condition:
        raise ValueError("rhode_island_profile_" + reason)


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def _validated_schema_page(metadata, license_number):
    """Retain page provenance without treating metadata as acquisition proof."""
    _require(
        isinstance(metadata, dict)
        and set(metadata) == {"artifact_file", "source_url", "downloaded_at", "content_sha256"}
        and metadata["artifact_file"] == "page.json"
        and metadata["source_url"] == f"https://datahealth.ri.gov/find/providers/results.php?license={license_number}"
        and isinstance(metadata["content_sha256"], str)
        and re.fullmatch(r"[a-f0-9]{64}", metadata["content_sha256"])
        and isinstance(metadata["downloaded_at"], str)
        and len(metadata["downloaded_at"]) <= 64,
        "schema_page_invalid",
    )
    try:
        observed_at = datetime.fromisoformat(metadata["downloaded_at"])
    except ValueError:
        raise ValueError("rhode_island_profile_schema_page_timestamp_invalid") from None
    _require(observed_at.utcoffset() is not None, "schema_page_timestamp_invalid")
    return copy.deepcopy(metadata)


def _validated_evidence(payload, license_number, evidence):
    _require(isinstance(payload, bytes) and 0 < len(payload) <= MAX_JSON_BYTES, "invalid_json_bytes")
    _require(
        isinstance(license_number, str) and re.fullmatch(r"(?:MD|DO)[0-9]{5}", license_number),
        "invalid_requested_license",
    )
    fields = {"run_id", "artifact_id", "source_url", "downloaded_at", "content_sha256", "row_number"}
    _require(isinstance(evidence, dict) and fields <= evidence.keys(), "evidence_missing")
    retained_by_field = {key: copy.deepcopy(evidence[key]) for key in fields}
    _require(
        all(
            isinstance(retained_by_field[key], str) and retained_by_field[key].strip()
            for key in fields - {"downloaded_at", "row_number"}
        ),
        "invalid_evidence_text",
    )
    _require(type(retained_by_field["row_number"]) is int and retained_by_field["row_number"] > 0, "invalid_row_number")
    expected_url = f"https://datahealth.ri.gov/find/providers/loadRecord.php?id={license_number}"
    _require(retained_by_field["source_url"] == expected_url, "evidence_url_mismatch")
    _require(retained_by_field["content_sha256"] == hashlib.sha256(payload).hexdigest(), "evidence_hash_mismatch")
    if isinstance(retained_by_field["downloaded_at"], datetime):
        retained_by_field["downloaded_at"] = retained_by_field["downloaded_at"].isoformat()
    try:
        datetime.fromisoformat(retained_by_field["downloaded_at"])
    except TypeError, ValueError:
        raise ValueError("rhode_island_profile_invalid_timestamp") from None
    if "schema_page" in evidence:
        retained_by_field["schema_page"] = _validated_schema_page(evidence["schema_page"], license_number)
    return retained_by_field


def _profile_occurrences(payload, license_number):
    try:
        values = json.loads(payload.decode("utf-8"))
    except UnicodeDecodeError, ValueError, RecursionError:
        raise ValueError("rhode_island_profile_invalid_json") from None
    _require(isinstance(values, list) and values and len(values) % len(PROFILE_FIELDS) == 0, "invalid_positional_shape")
    _require(all(isinstance(value, str) for value in values), "invalid_field_type")
    occurrences = [
        dict(zip(PROFILE_FIELDS, values[offset : offset + len(PROFILE_FIELDS)]))
        for offset in range(0, len(values), len(PROFILE_FIELDS))
    ]
    for occurrence in occurrences:
        _require(
            occurrence["License_No"] == license_number
            and occurrence["Profession_Name"] == "Physician"
            and occurrence["Primary_License_Type_Name"] == LICENSE_TYPES[license_number[:2]],
            "license_profession_type_mismatch",
        )
        _require(all(occurrence[field] == occurrences[0][field] for field in _IDENTITY_FIELDS), "conflicting_identity")
        _require(all(occurrence[field] in ("", "Y", "N") for field in HOSPITALS), "invalid_hospital_flag")
    return values, occurrences


def _retained_record(values, occurrences, license_number, evidence):
    record_key = f"{SOURCE_KEY}:{license_number}"
    return {
        "record_id": _hash([evidence["run_id"], record_key]),
        "run_id": evidence["run_id"],
        "artifact_id": evidence["artifact_id"],
        "source_key": SOURCE_KEY,
        "source_record_key": record_key,
        "profession_code": occurrences[0]["Profession_ID"],
        "license_id": occurrences[0]["License_ID"],
        "license_number": license_number,
        "raw_payload": {"fields": list(PROFILE_FIELDS), "values": values},
        "normalized_payload": {
            "schema_version": SCHEMA_VERSION,
            "visibility": "public",
            "occurrence_count": len(occurrences),
            "quality_flags": [],
        },
        "matched_npi": None,
        "match_status": "unmatched",
        "row_number": evidence["row_number"],
        "match_evidence": {
            "jurisdiction": "RI",
            "requested_license_number": license_number,
            "method": "exact_source_license_profession_type",
            "npi_binding": "not_attempted",
        },
    }


def _school_value(occurrence, observed_year):
    if _institution_key(occurrence["School_Name"]) is None:
        return {}, []
    value_by_field = {
        field: " ".join(occurrence[source].split())
        for field, source in (
            ("institution", "School_Name"),
            ("city", "School_City"),
            ("state", "School_State"),
            ("country", "School_Nation"),
        )
        if occurrence[source].strip()
    }
    year_text = occurrence["School_Grad_Year"].strip()
    flags = []
    if year_text:
        year = _graduation_year({"graduation_year": year_text})
        value_by_field.update(graduation_date=year_text, graduation_date_precision="year" if year else "source")
        if year is None:
            flags.append("graduation_year_invalid")
        else:
            value_by_field["graduation_year"] = year
            if year > observed_year:
                flags.append("graduation_year_in_future")
    return value_by_field, flags


def _fact(source_record, evidence, kind, fact_by_field, raw_fields, indexes, flags=()):
    category, fact_type = kind
    logical_key = _hash([SOURCE_KEY, source_record["license_number"], kind, fact_by_field])
    return {
        "fact_id": _hash([source_record["record_id"], logical_key]),
        "run_id": source_record["run_id"],
        "npi": None,
        "source_record_id": source_record["record_id"],
        "logical_fact_key": logical_key,
        "category": category,
        "fact_type": fact_type,
        "display": " — ".join(
            str(fact_by_field[field])
            for field in (
                "institution",
                "text",
                "city",
                "state",
                "country",
                "graduation_date",
            )
            if fact_by_field.get(field)
        ),
        "value_json": fact_by_field,
        "availability": "available",
        "assertion_type": "source_reported",
        "verification_status": "not_independently_verified",
        "effective_start": None,
        "effective_end": None,
        "sensitive": False,
        "public_default": True,
        "published_at": None,
        "source_json": {
            **copy.deepcopy(evidence),
            "source_key": SOURCE_KEY,
            "schema_version": SCHEMA_VERSION,
            "agency": "Rhode Island Department of Health",
            "jurisdiction": "RI",
            "source_record_id": source_record["record_id"],
            "occurrence_indexes": list(indexes),
            "source_paths": [
                f"values[{index * len(PROFILE_FIELDS)}:{(index + 1) * len(PROFILE_FIELDS)}]" for index in indexes
            ],
            "raw_fields": copy.deepcopy(raw_fields),
            "quality_flags": list(flags),
            "raw_occurrences": [
                {"occurrence_index": index, "raw_fields": copy.deepcopy(raw_fields), "quality_flags": list(flags)}
                for index in indexes
            ],
        },
    }


def _occurrence_facts(occurrence, index, record, evidence):
    facts = []
    specialty = " ".join(occurrence["Specialty_Name"].split())
    if specialty:
        facts.append(
            _fact(
                record,
                evidence,
                ("specialties", "specialty"),
                {"text": specialty},
                {"Specialty_Name": occurrence["Specialty_Name"]},
                [index],
            )
        )
    for field, institution in HOSPITALS.items():
        if occurrence[field] == "Y":
            facts.append(
                _fact(
                    record,
                    evidence,
                    ("privileges", "staff_privilege"),
                    {"institution": institution},
                    {field: occurrence[field]},
                    [index],
                )
            )
    return facts


def _profile_facts(occurrences, source_record, evidence):
    facts_by_key = {}
    observed_year = datetime.fromisoformat(evidence["downloaded_at"]).year
    for index, occurrence in enumerate(occurrences):
        occurrence_facts = []
        school_by_field, flags = _school_value(occurrence, observed_year)
        if school_by_field:
            occurrence_facts.append(
                _fact(
                    source_record,
                    evidence,
                    ("education", "education_history"),
                    school_by_field,
                    {field: occurrence[field] for field in SCHOOL_FIELDS},
                    [index],
                    flags,
                )
            )
        occurrence_facts.extend(_occurrence_facts(occurrence, index, source_record, evidence))
        for fact in occurrence_facts:
            key = fact["logical_fact_key"]
            if key not in facts_by_key:
                facts_by_key[key] = fact
                continue
            retained = facts_by_key[key]["source_json"]
            for field in ("occurrence_indexes", "source_paths", "raw_occurrences"):
                retained[field].extend(fact["source_json"][field])
            retained["quality_flags"] = sorted(
                set(retained["quality_flags"]) | set(fact["source_json"]["quality_flags"])
            )
    return list(facts_by_key.values())


def parse_profile(payload, *, license_number, evidence):
    """Return one retained source record and facts, or raise without partial success.

    The caller retains the bounded original bytes. This parser handles the observed
    flat 43N representation, validates every occurrence, and never binds an NPI.
    Identical full normalized values collapse within each fact type, preserving
    every raw occurrence, source path and quality flag. Conflicting values survive.
    Acquisition, schema-page verification and publication remain caller-owned.
    """
    retained_evidence = _validated_evidence(payload, license_number, evidence)
    values, occurrences = _profile_occurrences(payload, license_number)
    record = _retained_record(values, occurrences, license_number, retained_evidence)
    facts = _profile_facts(occurrences, record, retained_evidence)
    record["normalized_payload"]["quality_flags"] = sorted(
        {flag for fact in facts for flag in fact["source_json"]["quality_flags"]}
    )
    return record, facts
