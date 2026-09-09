# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retain Illinois licensing rows and bridge visible profile identity to a license."""

from __future__ import annotations

import copy
import hashlib
import json
import re
from datetime import datetime
from urllib.parse import urlsplit

SOURCE_KEY = "illinois-idfpr"
SCHEMA_VERSION = "il-idfpr-roster/v1"
PHYSICIAN_DESCRIPTION = "LICENSED PHYSICIAN AND SURGEON"
ROSTER_COLUMNS = frozenset((
    "license_type", "description", "license_number", "license_status", "business", "title",
    "first_name", "middle", "last_name", "prefix", "suffix", "business_name", "businessdba",
    "original_issue_date", "effective_date", "expiration_date", "city", "state", "zip", "county",
    "specialty_qualifier", "controlled_substance_schedule", "delegated_controlled_substance",
    "ever_disciplined", "lastmodifieddate", "case_number", "action", "discipline_start_date",
    "discipline_end_date", "discipline_reason",
))
NAME_FIELDS = ("first_name", "middle", "last_name", "title", "prefix", "suffix")


def _text(value):
    return " ".join(value.split()) if isinstance(value, str) else ""


def _issue_date(value):
    """Accept the observed calendar-date spelling without inventing time precision."""
    value = _text(value)
    if not re.fullmatch(r"[0-9]{2}/[0-9]{2}/[0-9]{4}", value):
        return None
    try:
        return datetime.strptime(value, "%m/%d/%Y").date().isoformat()
    except ValueError:
        return None


def _evidence(evidence):
    fields = ("run_id", "artifact_id", "source_url", "downloaded_at", "content_sha256")
    if not isinstance(evidence, dict) or any(not isinstance(evidence.get(k), str) or not evidence[k].strip() for k in fields):
        raise ValueError("illinois_roster_evidence_missing")
    evidence_by_field = {k: evidence[k] for k in fields}
    url = urlsplit(evidence_by_field["source_url"])
    if (url.scheme != "https" or url.netloc != "data.illinois.gov" or url.fragment
            or url.path not in {"/resource/pzzh-kp68.json", "/resource/pzzh-kp68.csv"}):
        raise ValueError("illinois_roster_source_url_invalid")
    if not re.fullmatch(r"[0-9a-f]{64}", evidence_by_field["content_sha256"]):
        raise ValueError("illinois_roster_content_hash_invalid")
    try:
        observed_at = datetime.fromisoformat(evidence_by_field["downloaded_at"])
    except ValueError as exc:
        raise ValueError("illinois_roster_observation_timestamp_invalid") from exc
    if observed_at.utcoffset() is None:
        raise ValueError("illinois_roster_observation_timezone_missing")
    return evidence_by_field


def _row_identity(raw_row):
    """Hold unsupported identity formats while retaining the complete original row."""
    if not isinstance(raw_row, dict):
        return "invalid", "row_object_required", None
    if set(raw_row) - ROSTER_COLUMNS or any(column_value is not None and not isinstance(column_value, str) for column_value in raw_row.values()):
        return "invalid", "column_schema_invalid", None
    if not _text(raw_row.get("license_type")) or not _text(raw_row.get("description")):
        return "invalid", "license_category_missing", None
    if raw_row["license_type"] != "MEDICAL BOARD" or raw_row["description"] != PHYSICIAN_DESCRIPTION:
        return "excluded", "not_physician_and_surgeon", None
    required = (*NAME_FIELDS, "license_number", "original_issue_date", "business")
    if any(not isinstance(raw_row.get(field), str) for field in required):
        return "invalid", "identity_component_missing", None
    if raw_row["business"] != "N":
        return "invalid", "individual_physician_required", None
    if not _text(raw_row["first_name"]) or not _text(raw_row["last_name"]):
        return "invalid", "legal_name_missing", None
    # Observed MD/DO suffixes follow the last name with empty title and prefix.
    if (_text(raw_row["prefix"]) or _text(raw_row["suffix"]) not in {"", "MD", "DO"}
            or (_text(raw_row["suffix"]) and _text(raw_row["title"]))):
        return "invalid", "name_affix_order_unverified", None
    if not re.fullmatch(r"036[0-9]{6}", raw_row["license_number"]):
        return "invalid", "physician_license_format_unverified", None
    issued_on = _issue_date(raw_row["original_issue_date"])
    if issued_on is None:
        return "invalid", "original_issue_date_invalid", None
    identity_by_field = {field: _text(raw_row[field]) for field in NAME_FIELDS}
    identity_by_field["display_name"] = " ".join(identity_by_field[field] for field in (*NAME_FIELDS[:4], "suffix")
                                                if identity_by_field[field])
    identity_by_field["original_issue_date"] = issued_on
    identity_by_field["license_number"] = raw_row["license_number"]
    return "eligible", "complete_physician_identity", identity_by_field


def parse_roster(rows, *, evidence):
    """Group identical JSON row values; retain every occurrence and its provenance."""
    provenance = _evidence(evidence)
    if not isinstance(rows, (list, tuple)):
        raise ValueError("illinois_roster_row_sequence_required")
    records_by_digest = {}
    for row_number, row in enumerate(rows, 1):
        try:
            encoded = json.dumps(row, sort_keys=True, ensure_ascii=False, allow_nan=False).encode()
        except (TypeError, ValueError) as exc:
            raise ValueError("illinois_roster_json_row_required") from exc
        digest = hashlib.sha256(encoded).hexdigest()
        if digest not in records_by_digest:
            status, reason, identity = _row_identity(row)
            records_by_digest[digest] = {"row_sha256": digest, "status": status, "reason": reason,
                                         "identity": identity, "originals": []}
        records_by_digest[digest]["originals"].append({"row_number": row_number, "raw_payload": copy.deepcopy(row)})
    return {"source_key": SOURCE_KEY, "schema_version": SCHEMA_VERSION, "evidence": provenance,
            "input_row_count": len(rows), "records": list(records_by_digest.values())}


def _profile_identity(identity):
    fields = ("display_name", "original_issue_date", "license_status", "expiration_date")
    if not isinstance(identity, dict) or any(not isinstance(identity.get(field), str) for field in fields):
        return None
    name, issued_on = _text(identity["display_name"]), _issue_date(identity["original_issue_date"])
    return (name.casefold(), issued_on) if name and issued_on else None


def _relevant_invalid(record, named_records, issued_on):
    """A malformed row cannot be silently removed from a potentially matching name."""
    raw = record["originals"][0]["raw_payload"]
    if isinstance(raw, dict):
        if any(raw.get("license_number") == row["identity"]["license_number"] for row in named_records):
            return True
        raw_date = _issue_date(raw.get("original_issue_date"))
        if raw_date is not None and raw_date != issued_on:
            return False
    if not isinstance(raw, dict) or any(not isinstance(raw.get(field), str) for field in NAME_FIELDS):
        return True
    first, last = _text(raw.get("first_name")).casefold(), _text(raw.get("last_name")).casefold()
    if not first or not last:
        return True
    name = " ".join(_text(raw[field]) for field in NAME_FIELDS[:4] if _text(raw[field])).casefold()
    names = {name}
    if _text(raw["suffix"]) in {"MD", "DO"} and not _text(raw["title"]) and not _text(raw["prefix"]):
        names.add(name + " " + _text(raw["suffix"]).casefold())
    return any(row["identity"]["display_name"].casefold() in names
               or (first == row["identity"]["first_name"].casefold()
                   and last == row["identity"]["last_name"].casefold()) for row in named_records)


def _candidate_result(profile_key, records):
    named_records = [row for row in records if row["status"] == "eligible"
                     and row["identity"]["display_name"].casefold() == profile_key[0]]
    malformed_records = [row for row in records if row["status"] == "invalid" and _relevant_invalid(row, named_records, profile_key[1])]
    if malformed_records:
        return "identity_conflict", "relevant_roster_identity_invalid", None, named_records + malformed_records
    if not named_records:
        return "unmatched", "no_exact_legal_name", None, []
    dated_records = [row for row in named_records if row["identity"]["original_issue_date"] == profile_key[1]]
    if not dated_records:
        return "identity_conflict", "original_issue_date_conflict", None, named_records
    if len(dated_records) != 1:
        return "ambiguous", "multiple_matching_source_rows", None, named_records
    license_number = dated_records[0]["identity"]["license_number"]
    related_records = [row for row in records if isinstance(row["originals"][0]["raw_payload"], dict)
                       and row["originals"][0]["raw_payload"].get("license_number") == license_number]
    if len(related_records) != 1:
        return "identity_conflict", "conflicting_license_rows", None, related_records
    return "matched", "unique_exact_legal_name_issue_date_description", license_number, named_records


def match_profile(identity, roster):
    """Return a source license bridge, never an NPI or a profile freshness assertion."""
    # ponytail: scan the supplied candidate rows; index a frozen roster before statewide matching.
    if not isinstance(roster, dict) or roster.get("schema_version") != SCHEMA_VERSION or roster.get("source_key") != SOURCE_KEY:
        raise ValueError("illinois_roster_normalized_input_required")
    profile_key = _profile_identity(identity)
    if profile_key is None:
        status, reason, license_number, candidates = "identity_conflict", "profile_identity_invalid", None, []
    else:
        status, reason, license_number, candidates = _candidate_result(profile_key, roster["records"])
    return {"license_number": license_number, "status": status, "reason": reason,
            "profile_identity": copy.deepcopy(identity), "candidate_records": copy.deepcopy(candidates),
            "evidence": {"method": "exact_il_roster_legal_name_issue_date_description", "jurisdiction": "IL",
                         "roster": copy.deepcopy(roster["evidence"]), "input_row_count": roster["input_row_count"],
                         "candidate_row_sha256s": [row["row_sha256"] for row in candidates]}}
