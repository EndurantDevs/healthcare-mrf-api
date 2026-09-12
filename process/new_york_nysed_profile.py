# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Normalize and replay exact NYSED physician evidence without assigning NPIs."""

from __future__ import annotations

import base64
import binascii
import copy
import hashlib
import re
from datetime import date, datetime, timedelta
from pathlib import Path

from process.kentucky_profile_acquisition import _read_artifact
from process.massachusetts_profile_acquisition import encoded_json
from process.massachusetts_profile_rows import _hash, _source_date
from process.new_york_profile_acquisition import _validated_headers
from process.provider_directory_projection_json import decoded_json_object

SOURCE_KEY = "new-york-nysed"
SCHEMA_VERSION = "ny-nysed-physician/v1"
PROFESSION_CODE = "060"
BASE_URL = "https://api.nysed.gov/rosa/V2/byProfessionAndLicenseNumber"
MAX_RESPONSE_BYTES = 2_000_000
MAX_METADATA_BYTES = 64 * 1024
TEXT_LABELS = {
    "profession": "Profession",
    "name": "Name",
    "address": "Address",
    "status": "Status",
    "dateOfLicensure": "Date of Licensure",
    "additionalQualifications": "Additional Qualifications",
    "registeredThroughDate": "Registered through Date",
    "schoolName": "Medical School",
    "schoolDegreeDate": "Degree Date",
    "licenseNumber": "License Number",
}
UNREPORTED = {"", "none", "n/a", "unknown", "not reported", "not available", "not applicable"}
MONTHS = (
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)


def _require(condition, reason):
    if not condition:
        raise ValueError("new_york_nysed_" + reason)


def _timestamp(timestamp):
    _require(isinstance(timestamp, str), "timestamp_invalid")
    try:
        parsed = datetime.fromisoformat(timestamp)
    except ValueError as error:
        raise ValueError("new_york_nysed_timestamp_invalid") from error
    _require(parsed.utcoffset() == timedelta(0), "timestamp_invalid")
    return parsed


def request_descriptor(license_number):
    """Describe the reproducible public request without retaining its header key."""
    _require(isinstance(license_number, str) and re.fullmatch(r"[0-9]{6}", license_number), "license_invalid")
    return {
        "method": "GET",
        "source_url": f"{BASE_URL}?licenseNumber={license_number}&professionCode={PROFESSION_CODE}",
        "headers": {"Accept": "application/json", "Accept-Encoding": "identity"},
        "additional_header_names": ["x-oapi-key"],
        "allow_redirects": False,
        "tls_verification": True,
        "retry": False,
    }


def _wrapped_field(profile_by_field, field, label, value_type):
    wrapped = profile_by_field.get(field)
    _require(
        isinstance(wrapped, dict) and wrapped.get("label") == label and isinstance(wrapped.get("value"), value_type),
        "field_schema_invalid",
    )
    return wrapped["value"]


def _validated_profile(body, license_number):
    _require(isinstance(body, bytes) and 0 < len(body) <= MAX_RESPONSE_BYTES, "body_invalid")
    request_descriptor(license_number)
    profile_by_field = decoded_json_object(body)
    text_by_field = {field: _wrapped_field(profile_by_field, field, label, str) for field, label in TEXT_LABELS.items()}
    _require(
        profile_by_field.get("professionCode") == PROFESSION_CODE
        and text_by_field["profession"] == "Medicine (060)"
        and text_by_field["licenseNumber"] == license_number,
        "identity_mismatch",
    )
    _require(text_by_field["name"].strip().casefold() not in UNREPORTED, "name_missing")
    _wrapped_field(profile_by_field, "additionalLicenses", "Additional Licenses", list)
    privileges = _wrapped_field(profile_by_field, "privileges", "Additional Qualifications", list)
    _require(all(isinstance(privilege, str) for privilege in privileges), "privileges_schema_invalid")
    _require(
        all(
            isinstance(profile_by_field.get(field), list)
            for field in ("enforcementActions", "certificateOfAuthorizations")
        )
        and isinstance(profile_by_field.get("noEnforcementActionsFoundMessage"), str)
        and type(profile_by_field.get("index")) is int,
        "profile_schema_invalid",
    )
    return profile_by_field, text_by_field


def _reported_text(source_text):
    normalized = " ".join(source_text.split())
    return "" if normalized.casefold() in UNREPORTED else normalized


def _date_value(source_text, field, observed_on):
    normalized = _reported_text(source_text)
    if not normalized:
        return {}, []
    english_date = re.fullmatch(r"([A-Za-z]+) ([0-9]{1,2}), ([0-9]{4})", normalized)
    if english_date and english_date[1] in MONTHS:
        try:
            normalized = date(int(english_date[3]), MONTHS.index(english_date[1]) + 1, int(english_date[2])).isoformat()
        except ValueError:
            normalized = _reported_text(source_text)
    date_by_field, flags = _source_date(normalized, field, observed_on)
    if field == "registered_through_date":
        flags = [flag for flag in flags if flag != "registered_through_date_in_future"]
    return date_by_field, flags


def _source_record(profile_by_field, license_number, evidence):
    source_record_key = f"{SOURCE_KEY}:{PROFESSION_CODE}:{license_number}"
    return {
        "record_id": _hash([evidence["run_id"], source_record_key]),
        "run_id": evidence["run_id"],
        "artifact_id": evidence["artifact_id"],
        "source_key": SOURCE_KEY,
        "source_record_key": source_record_key,
        "profession_code": PROFESSION_CODE,
        "license_id": None,
        "license_number": license_number,
        "raw_payload": copy.deepcopy(profile_by_field),
        "normalized_payload": {"schema_version": SCHEMA_VERSION, "visibility": "public", "quality_flags": []},
        "matched_npi": None,
        "match_status": "unmatched",
        "row_number": 1,
        "match_evidence": {
            "jurisdiction": "NY",
            "profession_code": PROFESSION_CODE,
            "requested_license_number": license_number,
            "reason": "registry_binding_not_performed",
        },
    }


def _source_fact(source_record, evidence, category, fact_type, fact_value, raw_fields, flags):
    logical_key = _hash([source_record["source_record_key"], category, fact_type, fact_value])
    display = (
        " — ".join(str(fact_value[field]) for field in ("institution", "graduation_date") if fact_value.get(field))
        if category == "education"
        else f"New York Medicine license {source_record['license_number']}"
    )
    return {
        "fact_id": _hash([source_record["record_id"], logical_key]),
        "run_id": source_record["run_id"],
        "npi": None,
        "source_record_id": source_record["record_id"],
        "logical_fact_key": logical_key,
        "category": category,
        "fact_type": fact_type,
        "display": display,
        "value_json": fact_value,
        "availability": "available",
        "assertion_type": "source_reported",
        "verification_status": "not_independently_verified",
        "effective_start": None,
        "effective_end": None,
        "source_json": {
            **evidence,
            "source_key": SOURCE_KEY,
            "schema_version": SCHEMA_VERSION,
            "agency": "New York State Education Department",
            "jurisdiction": "NY",
            "source_record_id": source_record["record_id"],
            "source_path": ",".join(raw_fields),
            "raw_fields": copy.deepcopy(raw_fields),
            "quality_flags": flags,
        },
        "sensitive": False,
        "public_default": True,
        "published_at": None,
    }


def _profile_facts(source_record, text_by_field, evidence, observed_on):
    profile_by_field = source_record["raw_payload"]
    institution = _reported_text(text_by_field["schoolName"])
    education_value, education_flags = _date_value(text_by_field["schoolDegreeDate"], "graduation_date", observed_on)
    if institution:
        education_value["institution"] = institution
    facts = []
    if education_value:
        facts.append(
            _source_fact(
                source_record,
                evidence,
                "education",
                "education_history",
                education_value,
                {field: profile_by_field[field] for field in ("schoolName", "schoolDegreeDate")},
                education_flags,
            )
        )
    license_by_field = {
        "jurisdiction": "NY",
        "profession_code": PROFESSION_CODE,
        "profession": text_by_field["profession"],
        "license_number": source_record["license_number"],
    }
    status = _reported_text(text_by_field["status"])
    if status:
        license_by_field["license_status"] = status
    license_flags = []
    for source_field, field in (
        ("dateOfLicensure", "date_of_licensure"),
        ("registeredThroughDate", "registered_through_date"),
    ):
        date_by_field, flags = _date_value(text_by_field[source_field], field, observed_on)
        license_by_field.update(date_by_field)
        license_flags.extend(flags)
    facts.append(
        _source_fact(
            source_record,
            evidence,
            "licenses",
            "state_licensure_record",
            license_by_field,
            {
                field: profile_by_field[field]
                for field in ("profession", "licenseNumber", "status", "dateOfLicensure", "registeredThroughDate")
            },
            license_flags,
        )
    )
    return facts


def parse_profile(body, *, license_number, evidence):
    """Retain source education and registration meanings without inferring practice."""
    _require(isinstance(body, bytes) and 0 < len(body) <= MAX_RESPONSE_BYTES, "body_invalid")
    descriptor = request_descriptor(license_number)
    required_fields = {"run_id", "artifact_id", "source_url", "downloaded_at", "content_sha256"}
    _require(
        isinstance(evidence, dict)
        and required_fields <= evidence.keys()
        and all(isinstance(evidence[field], str) and evidence[field].strip() for field in required_fields),
        "evidence_invalid",
    )
    _require(
        evidence["source_url"] == descriptor["source_url"]
        and evidence["content_sha256"] == hashlib.sha256(body).hexdigest(),
        "evidence_mismatch",
    )
    observed_on = _timestamp(evidence["downloaded_at"]).date()
    profile_by_field, text_by_field = _validated_profile(body, license_number)
    provenance_by_field = {field: evidence[field] for field in required_fields}
    source_record = _source_record(profile_by_field, license_number, provenance_by_field)
    return source_record, _profile_facts(source_record, text_by_field, provenance_by_field, observed_on)


def _response_headers(headers, *, no_content):
    _require(
        isinstance(headers, list)
        and all(
            isinstance(pair, list) and len(pair) == 2 and all(isinstance(part, str) for part in pair)
            for pair in headers
        ),
        "headers_invalid",
    )
    if not no_content:
        _validated_headers(headers)
        return
    content_types = [text for name, text in headers if name.lower() == "content-type"]
    encodings = [text for name, text in headers if name.lower() == "content-encoding"]
    _require(len(content_types) <= 1 and len(encodings) <= 1, "headers_ambiguous")
    _require(
        all(text.split(";", 1)[0].strip().lower() == "application/json" for text in content_types)
        and all(text.strip().lower() == "identity" for text in encodings),
        "headers_invalid",
    )


def _validated_response(response_by_field, descriptor, *, expected_status=200):
    _require(
        response_by_field.get("schema_version") == SCHEMA_VERSION
        and response_by_field.get("source_key") == SOURCE_KEY
        and response_by_field.get("source_url") == descriptor["source_url"]
        and response_by_field.get("request_sha256") == _hash(descriptor),
        "response_identity_invalid",
    )
    _require(
        response_by_field.get("complete") is True
        and "error_type" not in response_by_field
        and not response_by_field.get("redacted_key_echo")
        and type(response_by_field.get("status")) is int
        and response_by_field["status"] == expected_status,
        "response_incomplete",
    )
    _response_headers(response_by_field.get("headers"), no_content=expected_status == 204)
    try:
        body = base64.b64decode(response_by_field["body_base64"], validate=True)
    except (KeyError, TypeError, ValueError, binascii.Error) as error:
        raise ValueError("new_york_nysed_body_invalid") from error
    _require(
        (0 <= len(body) <= MAX_RESPONSE_BYTES if expected_status == 204 else 0 < len(body) <= MAX_RESPONSE_BYTES)
        and type(response_by_field.get("received_bytes")) is int
        and response_by_field["received_bytes"] == len(body)
        and response_by_field.get("content_sha256") == hashlib.sha256(body).hexdigest(),
        "body_changed",
    )
    _require(expected_status != 204 or body == b"", "no_content_body_not_empty")
    return body


def acquisition_result(manifest_by_field, response_by_field):
    """Rebuild unbound assertions from a complete response and its source scope."""
    descriptor = request_descriptor(manifest_by_field["license_number"])
    body = _validated_response(response_by_field, descriptor)
    evidence_by_field = {
        "run_id": manifest_by_field["run_id"],
        "artifact_id": _hash(response_by_field),
        **{field: response_by_field[field] for field in ("source_url", "downloaded_at", "content_sha256")},
    }
    source_record, facts = parse_profile(
        body, license_number=manifest_by_field["license_number"], evidence=evidence_by_field
    )
    return {"outcome": "acquired", "source_record": source_record, "facts": facts}


def held_acquisition_result(manifest_by_field, response_by_field):
    """Retain an empty 204 as no profile returned, without a licensing conclusion."""
    descriptor = request_descriptor(manifest_by_field["license_number"])
    _validated_response(response_by_field, descriptor, expected_status=204)
    return {"outcome": "held", "reason": "no_profile_returned", "source_record": None, "facts": []}


def _retained_acquisition(destination, receipt_sha256, expected_outcome):
    _require(isinstance(receipt_sha256, str) and re.fullmatch(r"[a-f0-9]{64}", receipt_sha256), "receipt_pin_invalid")
    receipt = _read_artifact(destination / "result.json", MAX_METADATA_BYTES)
    _require(
        _hash(receipt) == receipt_sha256
        and receipt.get("outcome") == expected_outcome
        and receipt.get("schema_version") == SCHEMA_VERSION,
        "receipt_changed_or_failed",
    )
    manifest_by_field = _read_artifact(destination / "manifest.json", MAX_METADATA_BYTES)
    _require(
        _hash(manifest_by_field) == receipt.get("manifest_sha256")
        and set(manifest_by_field)
        == {"schema_version", "source_key", "profession_code", "license_number", "run_id", "started_at"}
        and manifest_by_field["schema_version"] == SCHEMA_VERSION
        and manifest_by_field["source_key"] == SOURCE_KEY
        and manifest_by_field["profession_code"] == PROFESSION_CODE
        and isinstance(manifest_by_field["run_id"], str)
        and manifest_by_field["run_id"].strip(),
        "manifest_invalid",
    )
    descriptor = request_descriptor(manifest_by_field["license_number"])
    retained_request = _read_artifact(destination / "request.json", MAX_METADATA_BYTES)
    _require(
        encoded_json(retained_request) == encoded_json(descriptor)
        and receipt.get("request_sha256") == _hash(descriptor),
        "request_changed",
    )
    response_by_field = _read_artifact(destination / "response.json", MAX_RESPONSE_BYTES * 2)
    _require(receipt.get("response_sha256") == _hash(response_by_field), "response_changed")
    _require(
        _timestamp(manifest_by_field["started_at"])
        <= _timestamp(response_by_field.get("downloaded_at"))
        <= _timestamp(receipt.get("completed_at")),
        "chronology_invalid",
    )
    return manifest_by_field, response_by_field, receipt


def read_acquisition(destination: Path, *, receipt_sha256: str):
    """Replay acquired bytes pinned by an independently retained receipt."""
    manifest_by_field, response_by_field, receipt = _retained_acquisition(destination, receipt_sha256, "acquired")
    acquired = acquisition_result(manifest_by_field, response_by_field)
    _require(
        type(receipt.get("fact_count")) is int and receipt["fact_count"] == len(acquired["facts"]), "fact_count_changed"
    )
    return {**acquired, "receipt_sha256": receipt_sha256}


def read_held_acquisition(destination: Path, *, receipt_sha256: str):
    """Replay only a pinned complete empty 204; it does not prove lack of licensure."""
    manifest_by_field, response_by_field, receipt = _retained_acquisition(destination, receipt_sha256, "held")
    held = held_acquisition_result(manifest_by_field, response_by_field)
    _require(
        type(receipt.get("fact_count")) is int
        and receipt["fact_count"] == 0
        and receipt.get("reason") == held["reason"],
        "held_result_changed",
    )
    return {**held, "receipt_sha256": receipt_sha256}
