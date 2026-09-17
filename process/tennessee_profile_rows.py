# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retain Tennessee physician CSV assertions without acquisition or NPI matching."""

from __future__ import annotations

import copy
import csv
import hashlib
import io
import re
from datetime import date, datetime
from itertools import count

from process.massachusetts_profile_rows import _source_date
from process.provider_directory_projection_types import stable_hash

SOURCE_KEY = "tennessee-tdh"
SCHEMA_VERSION = "tn-tdh-profile/v1"
MAX_REPORT_BYTES = 256 * 1024 * 1024
MAX_ROWS = 500_000
MAX_FIELD_CHARS = 16_384
BASE_FIELDS = (
    "Board", "Profession", "Rank", "LastName", "FirstName", "MiddleName", "Title",
    "LicenseNumber", "ExpirationDate", "OriginalDate", "Status", "LicenseActivity",
    "StatusEffectiveDate", "Disciplined",
)
FACT_FIELDS_BY_CATEGORY = {
    "education": ("DegreeEarned", "EducationProvider", "EducationProviderLocation", "GraduationDate"),
    "training": ("OtherTrainingProvider", "OtherTrainingLocation", "OtherTrainingFromDate", "OtherTrainingEndDate"),
    "specialties": ("ModifierDescription", "ModifierType"),
}
PRACTICE_FIELDS = (
    "PracticeName", "PracticeAddress", "PracticeAddress2", "PracticeCity", "PracticeState",
    "PracticeZIP", "PracticeAreaCode", "PracticePhoneNumber", "PracticeExtension", "PracticeCounty",
)
EXTRA_FIELDS = ("Gender", "Race") + PRACTICE_FIELDS
ORDERED_PRACTICE_HEADER = (BASE_FIELDS + FACT_FIELDS_BY_CATEGORY["specialties"] + PRACTICE_FIELDS
                           + FACT_FIELDS_BY_CATEGORY["education"] + FACT_FIELDS_BY_CATEGORY["training"])
IDENTITY_PREFIX = re.compile(r'(?:"(?:[^"]|"")*",){14}')
REQUIRED_FIELDS = frozenset(BASE_FIELDS + sum(FACT_FIELDS_BY_CATEGORY.values(), ()))
PROFESSION_CODES = {("Medical Examiners", "Medical Doctor"): "1606", ("Osteopathy", "Osteopathic Physician"): "1907"}
FACT_TYPES = {"education": "education_history", "training": "other_training", "specialties": "specialty"}


def _require(condition, reason):
    if not condition:
        raise ValueError("tennessee_profile_" + reason)


def _text(source_text):
    return " ".join(source_text.split())


def _validated_evidence(content, evidence):
    required_fields = {"run_id", "artifact_id", "source_url", "downloaded_at", "content_sha256"}
    _require(isinstance(evidence, dict) and required_fields <= evidence.keys(), "evidence_missing")
    evidence_by_field = {field: copy.deepcopy(evidence[field]) for field in required_fields}
    for field in ("run_id", "artifact_id", "source_url"):
        _require(isinstance(evidence_by_field[field], str) and bool(evidence_by_field[field].strip()), "evidence_text_invalid")
    timestamp = evidence_by_field["downloaded_at"]
    if isinstance(timestamp, datetime):
        timestamp = timestamp.isoformat()
    try:
        observed_at = datetime.fromisoformat(timestamp)
    except (TypeError, ValueError):
        raise ValueError("tennessee_profile_evidence_timestamp_invalid") from None
    _require(observed_at.tzinfo is not None and observed_at.utcoffset() is not None, "evidence_timestamp_invalid")
    _require(evidence_by_field["content_sha256"] == hashlib.sha256(content).hexdigest(), "evidence_digest_mismatch")
    evidence_by_field["downloaded_at"] = timestamp
    return evidence_by_field


def _captured_lines(source_text, captured_lines):
    for line in io.StringIO(source_text, newline=""):
        captured_lines.append(line)
        yield line


def _csv_reader(content):
    try:
        source_text = content.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise ValueError("tennessee_profile_encoding_invalid") from None
    _require("\x00" not in source_text, "input_invalid")
    captured_lines = []
    reader = csv.reader(_captured_lines(source_text, captured_lines), strict=True)
    header = next(reader, [])
    _require(len(header) == len(set(header)) and set(header) in (
        REQUIRED_FIELDS, REQUIRED_FIELDS | set(PRACTICE_FIELDS), REQUIRED_FIELDS | set(EXTRA_FIELDS),
    ), "headers_invalid")
    return header, reader, captured_lines


def _quarantined_row(header, captured_lines, error, physical_line):
    """Read only a strict identity prefix from the observed single-line quote error."""
    _require(tuple(header) == ORDERED_PRACTICE_HEADER and len(captured_lines) == 1
             and str(error) == "',' expected after '\"'", "csv_invalid")
    raw_text = captured_lines[0]
    # Bound the entire uninterpreted line so no possible field exceeds its cap.
    _require(len(raw_text) <= MAX_FIELD_CHARS, "field_limit")
    _require(raw_text.startswith('"') and raw_text.endswith('"\r\n')
             and raw_text.count('\",\"') == 33, "csv_invalid")
    prefix = IDENTITY_PREFIX.match(raw_text)
    _require(prefix is not None, "csv_invalid")
    cells = next(csv.reader([prefix.group(0)[:-1]], strict=True))
    _require(len(cells) == len(BASE_FIELDS) and (cells[0], cells[1]) in PROFESSION_CODES
             and cells[3].strip() and cells[4].strip() and re.fullmatch(r"[0-9]{1,32}", cells[7].strip()), "csv_invalid")
    return dict(zip(BASE_FIELDS, cells)), {
        "reason": "csv_quote_invalid", "raw_text": raw_text,
        "content_sha256": hashlib.sha256(raw_text.encode("utf-8")).hexdigest(),
        "physical_line_start": physical_line, "physical_line_end": physical_line,
    }


def _source_rows(header, reader, captured_lines):
    for row_number in count(1):
        captured_lines.clear()
        physical_line = reader.line_num + 1
        try:
            cells = next(reader)
        except StopIteration:
            return
        except csv.Error as error:
            source_by_field, malformed = _quarantined_row(header, captured_lines, error, physical_line)
        else:
            _require(len(cells) == len(header), "row_width_invalid")
            _require(all(len(cell) <= MAX_FIELD_CHARS for cell in cells), "field_limit")
            source_by_field, malformed = dict(zip(header, cells)), None
        _require(row_number <= MAX_ROWS, "row_limit")
        yield row_number, source_by_field, malformed


def _retained_record(source_row, row_number, evidence):
    profession = PROFESSION_CODES.get((source_row["Board"], source_row["Profession"]))
    _require(profession is not None, "profession_invalid")
    license_number = source_row["LicenseNumber"].strip()
    _require(not license_number or re.fullmatch(r"[0-9]{1,32}", license_number), "license_invalid")
    identity = license_number or f"unlicensed:{evidence['content_sha256']}:{row_number}"
    record_key = f"{SOURCE_KEY}:{profession}:{identity}"
    return {
        "record_id": stable_hash([evidence["run_id"], record_key], domain=SCHEMA_VERSION),
        "run_id": evidence["run_id"], "artifact_id": evidence["artifact_id"], "source_key": SOURCE_KEY,
        "source_record_key": record_key, "profession_code": profession, "license_id": None,
        "license_number": license_number or None, "row_number": row_number,
        "raw_payload": {"rows": []},
        "normalized_payload": {"schema_version": SCHEMA_VERSION, "visibility": "public", "quality_flags": []},
        "matched_npi": None, "match_status": "unmatched",
        "match_evidence": {"jurisdiction": "TN", "method": "not_matched", "license_number": license_number or None},
    }


def _group_source_rows(header, reader, captured_lines, evidence):
    records_by_key = {}
    for row_number, source_by_field, malformed in _source_rows(header, reader, captured_lines):
        source_record = _retained_record(source_by_field, row_number, evidence)
        source_record = records_by_key.setdefault(source_record["source_record_key"], source_record)
        retained_by_field = {"row_number": row_number, "fields": source_by_field}
        if malformed is None:
            source_record["raw_payload"]["rows"].append(retained_by_field)
        else:
            source_record["raw_payload"].setdefault("malformed_rows", []).append({**retained_by_field, **malformed})
    return list(records_by_key.values())


def _is_held_identity(source_record):
    source_rows = source_record["raw_payload"]["rows"]
    reason = None
    if source_record["raw_payload"].get("malformed_rows"):
        reason = "malformed_source_row"
    elif source_record["license_number"] is None:
        reason = "license_number_missing"
    elif any(not _text(entry["fields"][field]) for entry in source_rows for field in ("FirstName", "LastName")):
        reason = "source_identity_conflict"
    elif len({tuple(entry["fields"][field] for field in BASE_FIELDS) for entry in source_rows}) != 1:
        reason = "source_identity_conflict"
    if reason is None:
        return False
    source_record["normalized_payload"].update(visibility="held_identity", quality_flags=[reason])
    source_record["match_evidence"]["reason"] = reason
    if reason == "source_identity_conflict":
        source_record["match_status"] = "identity_conflict"
    return True


def _reported_date(source_text, field, observed_on):
    date_text = _text(source_text)
    if not date_text:
        return {}, []
    matched = re.fullmatch(r"([0-9]{2})/([0-9]{2})/([0-9]{4})", date_text)
    if matched is not None:
        month, day, year = map(int, matched.groups())
        try:
            normalized_date = date(year, month, day).isoformat()
        except ValueError:
            normalized_date = None
        if normalized_date is not None:
            return _source_date(normalized_date, field, observed_on)
    return {field: date_text, f"{field}_precision": "source"}, [f"{field}_invalid"]


def _fact_value(category, raw_fields, observed_on):
    if category == "specialties":
        if raw_fields["ModifierType"] == "Specialty" and _text(raw_fields["ModifierDescription"]):
            return {"text": _text(raw_fields["ModifierDescription"])}, []
        return {}, ["modifier_unmapped"]
    source_fields = FACT_FIELDS_BY_CATEGORY[category]
    normalized_fields = ("degree", "institution", "institution_location") if category == "education" else ("institution", "institution_location")
    value_by_field = {field: _text(raw_fields[source_field]) for field, source_field in zip(normalized_fields, source_fields)
                      if _text(raw_fields[source_field])}
    date_fields = (("graduation_date", "GraduationDate"),) if category == "education" else (
        ("attendance_start", "OtherTrainingFromDate"), ("attendance_end", "OtherTrainingEndDate"),
    )
    flags = []
    for field, source_field in date_fields:
        normalized, date_flags = _reported_date(raw_fields[source_field], field, observed_on)
        value_by_field.update(normalized)
        flags.extend(date_flags)
    if category == "training" and value_by_field.get("attendance_start_precision") == value_by_field.get("attendance_end_precision") == "day":
        if value_by_field["attendance_start"] > value_by_field["attendance_end"]:
            flags.append("training_period_reversed")
    return value_by_field, flags


def _retained_fact(source_record, evidence, category, raw_fields, value_by_field, flags, row_number):
    fact_type = FACT_TYPES[category]
    logical_key = stable_hash([source_record["source_record_key"], category, fact_type, value_by_field], domain=SCHEMA_VERSION)
    display_fields = ("degree", "institution", "institution_location", "graduation_date", "attendance_start", "attendance_end", "text")
    return {
        "fact_id": stable_hash([source_record["record_id"], logical_key], domain=SCHEMA_VERSION),
        "run_id": source_record["run_id"], "npi": None, "source_record_id": source_record["record_id"],
        "logical_fact_key": logical_key, "category": category, "fact_type": fact_type,
        "display": " — ".join(str(value_by_field[field]) for field in display_fields if value_by_field.get(field)),
        "value_json": value_by_field, "availability": "available", "assertion_type": "source_reported",
        "verification_status": "not_independently_verified",
        "effective_start": value_by_field.get("attendance_start") if value_by_field.get("attendance_start_precision") == "day" else None,
        "effective_end": value_by_field.get("attendance_end") if value_by_field.get("attendance_end_precision") == "day" else None,
        "sensitive": False, "public_default": True, "published_at": None,
        "source_json": {**evidence, "source_key": SOURCE_KEY, "schema_version": SCHEMA_VERSION,
            "agency": "Tennessee Department of Health", "jurisdiction": "TN", "source_record_id": source_record["record_id"],
            "source_path": "csv." + "+".join(FACT_FIELDS_BY_CATEGORY[category]), "raw_fields": raw_fields,
            "row_number": row_number, "row_numbers": [row_number], "quality_flags": flags},
    }


def _profile_facts(source_record, evidence):
    if _is_held_identity(source_record):
        return []
    facts_by_tuple = {}
    quality_flags = set()
    observed_on = datetime.fromisoformat(evidence["downloaded_at"]).date()
    for entry in source_record["raw_payload"]["rows"]:
        for category, fields in FACT_FIELDS_BY_CATEGORY.items():
            raw_by_field = {field: entry["fields"][field] for field in fields}
            if not any(_text(source_text) for source_text in raw_by_field.values()):
                continue
            value_by_field, flags = _fact_value(category, raw_by_field, observed_on)
            quality_flags.update(flags)
            if not value_by_field:
                continue
            tuple_key = (category, *sorted(value_by_field.items()))
            if tuple_key in facts_by_tuple:
                facts_by_tuple[tuple_key]["source_json"]["row_numbers"].append(entry["row_number"])
                continue
            facts_by_tuple[tuple_key] = _retained_fact(
                source_record, evidence, category, raw_by_field, value_by_field, flags, entry["row_number"],
            )
    source_record["normalized_payload"]["quality_flags"] = sorted(quality_flags)
    return list(facts_by_tuple.values())


def parse_report(content, *, evidence):
    """Parse one captured 24/34/36-column report into unbound source records/facts.

    Only observed MD/DO profession schemas are supported. All ranks and statuses
    remain literal. Blank licenses and conflicting identities yield held records.
    Education, other training and recognized specialties deduplicate separately;
    optional practice/demographic fields and unknown modifiers remain raw only.
    Observed one-line quote failures retain only their identity prefix and hold
    the entire license group. Unsupported corruption still rejects the report.
    No source completeness, NPI identity, publication or degree completion is inferred.
    """
    _require(isinstance(content, bytes) and len(content) <= MAX_REPORT_BYTES, "input_invalid")
    evidence_by_field = _validated_evidence(content, evidence)
    # ponytail: retain up to 256 MiB/500k rows; stream staging if measured worker capacity requires it.
    try:
        header, reader, captured_lines = _csv_reader(content)
        source_records = _group_source_rows(header, reader, captured_lines, evidence_by_field)
    except csv.Error:
        raise ValueError("tennessee_profile_csv_invalid") from None
    facts = [fact for source_record in source_records for fact in _profile_facts(source_record, evidence_by_field)]
    return source_records, facts
