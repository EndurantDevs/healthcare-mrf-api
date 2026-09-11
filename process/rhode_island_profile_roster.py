# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure retention of the observed Rhode Island MD/DO license-roster CSV."""

from __future__ import annotations

import csv
import hashlib
import io
import re
from datetime import datetime

from process.rhode_island_profile_rows import LICENSE_TYPES, SOURCE_KEY

SCHEMA_VERSION = "ri-doh-roster/v1"
SOURCE_URL = "https://datahealth.ri.gov/lists/licensees/download.php"
# Observed largest file: 2.1 MB / 7,507 rows. These are bounds, not coverage claims.
MAX_ROSTER_BYTES = 16 * 1024 * 1024
MAX_ROSTER_ROWS = 100_000
MAX_FIELD_CHARS = 16_384
ROSTER_COLUMNS = (
    "Name",
    "First",
    "Middle",
    "Last",
    "Owner Manager Name",
    "License No",
    "Profession",
    "License Type",
    "Status",
    "Issue Date",
    "Specialty",
    "Expiration Date",
    "License Address Line 1",
    "License Address Line 2",
    "License Address Line 3",
    "City",
    "State",
    "Zip",
    "Phone",
    "Fax",
    "Total Capacity Beds",
    "Alzheimer/Special Care Unit Beds",
    "Skilled Nursing Facility Beds",
    "Long-Term Care Beds",
    "Skilled/Long-Term Care Beds",
    "Private Pay Beds",
)


def _require(condition, reason):
    if not condition:
        raise ValueError("rhode_island_roster_" + reason)


def _validated_evidence(payload, license_type, expected_rows, evidence):
    _require(isinstance(payload, bytes) and 0 < len(payload) <= MAX_ROSTER_BYTES, "invalid_csv_bytes")
    _require(license_type in LICENSE_TYPES.values(), "unsupported_license_type")
    _require(type(expected_rows) is int and 0 < expected_rows <= MAX_ROSTER_ROWS, "invalid_expected_rows")
    fields = ("run_id", "artifact_id", "source_url", "downloaded_at", "content_sha256")
    _require(
        isinstance(evidence, dict)
        and all(isinstance(evidence.get(field), str) and evidence[field].strip() for field in fields),
        "evidence_missing",
    )
    provenance_by_field = {field: evidence[field] for field in fields}
    _require(provenance_by_field["source_url"] == SOURCE_URL, "evidence_url_mismatch")
    _require(provenance_by_field["content_sha256"] == hashlib.sha256(payload).hexdigest(), "evidence_hash_mismatch")
    try:
        observed_at = datetime.fromisoformat(provenance_by_field["downloaded_at"])
    except ValueError:
        raise ValueError("rhode_island_roster_invalid_timestamp") from None
    _require(observed_at.utcoffset() is not None, "observation_timezone_missing")
    return provenance_by_field


def _csv_rows(payload):
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        raise ValueError("rhode_island_roster_invalid_utf8") from None
    _require("\x00" not in text, "nul_in_csv")
    reader = csv.reader(io.StringIO(text, newline=""), strict=True)
    try:
        _require(next(reader, None) == list(ROSTER_COLUMNS), "column_schema_mismatch")
        for row_number, values in enumerate(reader, 1):
            _require(row_number <= MAX_ROSTER_ROWS, "row_limit_exceeded")
            _require(len(values) == len(ROSTER_COLUMNS), "row_width_mismatch")
            _require(all(len(value) <= MAX_FIELD_CHARS for value in values), "field_limit_exceeded")
            yield row_number, reader.line_num, dict(zip(ROSTER_COLUMNS, values))
    except csv.Error:
        raise ValueError("rhode_island_roster_invalid_csv") from None


def _retain_occurrence(roots_by_license, raw_fields, row_number, line_number, license_type):
    license_number = raw_fields["License No"]
    prefix = next(prefix for prefix, name in LICENSE_TYPES.items() if name == license_type)
    _require(
        re.fullmatch(prefix + r"[0-9]{5}", license_number)
        and raw_fields["Profession"] == "Physician"
        and raw_fields["License Type"] == license_type,
        "cohort_identity_mismatch",
    )
    if license_number not in roots_by_license:
        roots_by_license[license_number] = {"license_number": license_number, "specialties": {}, "originals": []}
    root = roots_by_license[license_number]
    if root["originals"]:
        first_fields = root["originals"][0]["raw_payload"]
        _require(
            all(raw_fields[field] == first_fields[field] for field in ROSTER_COLUMNS if field != "Specialty"),
            "conflicting_license_rows",
        )
    specialty = raw_fields["Specialty"]
    if specialty:
        root["specialties"].setdefault(specialty, None)
    root["originals"].append({"row_number": row_number, "line_number": line_number, "raw_payload": raw_fields})


def parse_roster(payload, *, license_type, expected_rows, evidence):
    """Retain every literal CSV occurrence grouped by exact license.

    Repeated rows must agree outside Specialty. Preserve statuses literally;
    bounds exceed observed MD/DO files without guaranteeing broader coverage.
    No process-global CSV limit changes. expected_rows is a separate source
    count, not a shared snapshot or completeness proof: the caller retains the
    original bytes and binds that count to acquisition evidence. Errors raise
    before returning a result; no row is silently skipped or truncated.
    """
    provenance = _validated_evidence(payload, license_type, expected_rows, evidence)
    roots_by_license = {}
    input_row_count = 0
    for row_number, line_number, raw_fields in _csv_rows(payload):
        _retain_occurrence(roots_by_license, raw_fields, row_number, line_number, license_type)
        input_row_count = row_number
    _require(input_row_count == expected_rows, "source_row_count_mismatch")
    return {
        "source_key": SOURCE_KEY,
        "schema_version": SCHEMA_VERSION,
        "evidence": provenance,
        "license_type": license_type,
        "input_row_count": input_row_count,
        "expected_row_count": expected_rows,
        "unique_license_count": len(roots_by_license),
        "roots": [{**root, "specialties": list(root["specialties"])} for _, root in sorted(roots_by_license.items())],
    }
