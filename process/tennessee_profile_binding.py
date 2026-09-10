# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Offline TN identity decisions; supplied pins do not authorize capture or publication."""

from __future__ import annotations

import copy
import hashlib
import re
from collections import defaultdict
from pathlib import Path

from process.kentucky_profile_acquisition import _read_artifact
from process.massachusetts_profile_acquisition import encoded_json
from process.provider_directory_profile import is_valid_npi
from process.tennessee_profile_rows import MAX_REPORT_BYTES, parse_report

SCHEMA_VERSION = "tn-profile-binding/v1"
SNAPSHOT_SCHEMA = "tn-nppes-retained-snapshot/v1"
COVERAGE_SCOPE = "all_current_literal_tn_license_occurrences"
MAX_SNAPSHOT_BYTES = 64 * 1024 * 1024
PROFESSIONS = frozenset({"1606", "1907"})
REGISTRY_COLUMNS = [
    "npi", "taxonomy_occurrence_checksum", "license_number", "license_state", "taxonomy",
    "primary_taxonomy_switch", "joined_npi", "entity_type_code", "first_name", "middle_name",
    "last_name", "suffix", "joined_taxonomy_code", "taxonomy_grouping",
]
# Capture this exact SELECT in one READ ONLY, REPEATABLE READ transaction. Retain
# its complete cursor, row count, transaction metadata and relation OIDs while
# the SELECT's relation locks remain held. Do not add identity/physician filters.
CAPTURE_QUERY = """SELECT t.npi, t.checksum AS taxonomy_occurrence_checksum,
       t.provider_license_number AS license_number,
       t.provider_license_number_state_code AS license_state,
       t.healthcare_provider_taxonomy_code AS taxonomy,
       t.healthcare_provider_primary_taxonomy_switch AS primary_taxonomy_switch,
       n.npi AS joined_npi, n.entity_type_code,
       n.provider_first_name AS first_name, n.provider_middle_name AS middle_name,
       n.provider_last_name AS last_name, n.provider_name_suffix_text AS suffix,
       u.code AS joined_taxonomy_code, u.grouping AS taxonomy_grouping
  FROM mrf.npi_taxonomy t
  LEFT JOIN mrf.npi n ON n.npi = t.npi
  LEFT JOIN mrf.nucc_taxonomy u ON u.code = t.healthcare_provider_taxonomy_code
 WHERE t.provider_license_number_state_code = 'TN'
 ORDER BY t.npi, t.checksum"""
QUERY_SHA256 = hashlib.sha256(CAPTURE_QUERY.encode("utf-8")).hexdigest()
NAME_FIELDS = (("FirstName", "first_name"), ("MiddleName", "middle_name"), ("LastName", "last_name"))
# Title is not a credential field. Recognize only ordinary generational suffix
# literals; no punctuation equivalence, initials, credential stripping or guessing.
SUFFIX_LITERALS = frozenset({"", "jr", "jr.", "sr", "sr.", "ii", "iii", "iv", "v", "vi"})


def _require(condition, reason):
    if not condition:
        raise ValueError("tennessee_binding_" + reason)


def _check_pin(pin, name):
    _require(isinstance(pin, str) and re.fullmatch(r"[0-9a-f]{64}", pin), name + "_pin_invalid")


def _report_inputs(reports_by_profession):
    _require(isinstance(reports_by_profession, dict) and set(reports_by_profession) == PROFESSIONS,
             "report_professions_invalid")
    report_by_profession = {}
    for profession, report in reports_by_profession.items():
        _require(isinstance(report, dict) and set(report) == {"content", "evidence"}
                 and isinstance(report["content"], bytes) and 0 < len(report["content"]) <= MAX_REPORT_BYTES
                 and isinstance(report["evidence"], dict), "report_invalid")
        # Bytes are immutable; copy only the small metadata, never parsed batches.
        report_by_profession[profession] = {"content": report["content"], "evidence": copy.deepcopy(report["evidence"])}
    return report_by_profession


def reports_content_sha256(reports_by_profession: dict) -> str:
    """Digest both byte streams and JSON evidence; independently pin at capture.

    Each profession entry has exactly ``content`` (CSV bytes) and ``evidence``
    (the parser's JSON evidence). Recomputing this digest is not capture authority.
    """
    pin_by_profession = {profession: {"content_sha256": hashlib.sha256(report["content"]).hexdigest(),
                         "evidence": report["evidence"]}
            for profession, report in _report_inputs(reports_by_profession).items()}
    return hashlib.sha256(encoded_json(pin_by_profession)).hexdigest()


def _validate_snapshot_metadata(snapshot):
    _require(snapshot.get("schema_version") == SNAPSHOT_SCHEMA
             and snapshot.get("coverage_scope") == COVERAGE_SCOPE
             and snapshot.get("source_schema") == "mrf" and snapshot.get("source_state") == "TN"
             and snapshot.get("query_sha256") == QUERY_SHA256
             and snapshot.get("columns") == REGISTRY_COLUMNS, "snapshot_scope_invalid")
    _require(snapshot.get("status") == "passed" and snapshot.get("all_rows_received") is True
             and snapshot.get("connection_closed") is True, "snapshot_incomplete")
    transaction = snapshot.get("snapshot")
    _require(isinstance(transaction, dict) and transaction.get("read_only") == "on"
             and transaction.get("isolation") == "repeatable read"
             and isinstance(transaction.get("snapshot_id"), str)
             and re.fullmatch(r"[0-9]+:[0-9]+:(?:[0-9]+(?:,[0-9]+)*)?", transaction["snapshot_id"])
             and all(isinstance(transaction.get(field), str) and transaction[field].strip()
                     for field in ("snapshot_started_at", "server_version", "database_name"))
             and type(transaction.get("backend_pid")) is int and transaction["backend_pid"] > 0,
             "snapshot_transaction_invalid")
    relations = snapshot.get("registry_relations")
    _require(isinstance(relations, dict) and set(relations) == {"mrf.npi", "mrf.npi_taxonomy", "mrf.nucc_taxonomy"}
             and all(type(oid) is int and oid > 0 for oid in relations.values()), "snapshot_relations_invalid")


def read_registry_snapshot(path: Path, *, snapshot_sha256: str) -> dict:
    """Validate the supplied canonical envelope, not its truth or freshness.

    Capture authority and completeness require independent evidence. This bound
    does not establish that a full TN snapshot fits or that a worker can import it.
    Missing joins or identity fields remain conflicts for the literal license;
    a missing license key or nontext value rejects the snapshot. Literal NULL
    licenses are retained without assigning them to a numbered source license.
    """
    _check_pin(snapshot_sha256, "snapshot")
    snapshot = _read_artifact(path, MAX_SNAPSHOT_BYTES)
    _require(hashlib.sha256(encoded_json(snapshot)).hexdigest() == snapshot_sha256, "snapshot_changed")
    _validate_snapshot_metadata(snapshot)
    registry_rows = snapshot.get("registry_rows")
    _require(isinstance(registry_rows, list)
             and all(isinstance(candidate, dict) and "license_number" in candidate
                     and (candidate["license_number"] is None or isinstance(candidate["license_number"], str))
                     and set(candidate) <= set(REGISTRY_COLUMNS) for candidate in registry_rows), "snapshot_rows_invalid")
    _require(type(snapshot.get("row_count")) is int and type(snapshot.get("expected_source_row_count")) is int
             and snapshot["expected_source_row_count"] == snapshot["row_count"] == len(registry_rows),
             "snapshot_count_changed")
    registry_bytes = encoded_json(registry_rows)
    _require(type(snapshot.get("registry_rows_bytes")) is int and snapshot["registry_rows_bytes"] == len(registry_bytes)
             and snapshot.get("registry_rows_sha256") == hashlib.sha256(registry_bytes).hexdigest(), "snapshot_rows_changed")
    previous = None
    for candidate in registry_rows:
        # An invalid occurrence identity stays in its license's candidate list.
        if type(candidate.get("npi")) is not int or type(candidate.get("taxonomy_occurrence_checksum")) is not int:
            continue
        occurrence = candidate["npi"], candidate["taxonomy_occurrence_checksum"]
        _require(previous is None or previous <= occurrence, "snapshot_order_changed")
        previous = occurrence
    return snapshot


def _name(value):
    return " ".join(value.split()).casefold()


def _is_compatible_candidate(identity, candidate):
    if (set(candidate) != set(REGISTRY_COLUMNS) or type(candidate.get("npi")) is not int
            or not is_valid_npi(candidate["npi"]) or type(candidate.get("joined_npi")) is not int
            or candidate["joined_npi"] != candidate["npi"]
            or type(candidate.get("taxonomy_occurrence_checksum")) is not int
            or candidate.get("license_state") != "TN" or type(candidate.get("entity_type_code")) is not int
            or candidate["entity_type_code"] != 1 or not isinstance(candidate.get("taxonomy"), str)
            or not candidate["taxonomy"].strip() or candidate.get("joined_taxonomy_code") != candidate["taxonomy"]
            or candidate.get("taxonomy_grouping") != "Allopathic & Osteopathic Physicians"):
        return False
    for source_field, registry_field in (*NAME_FIELDS, ("Title", "suffix")):
        value = candidate[registry_field]
        if not (isinstance(value, str) or registry_field in {"middle_name", "suffix"} and value is None):
            return False
        if _name(identity[source_field]) != _name(value or ""):
            return False
    return True


def _decision(source_record, candidates, source_records):
    decision_by_field = {"method": "exact_tn_license_name_components", "status": "identity_conflict", "npi": None,
                "candidate_rows": candidates,
                "source_groups": [{"source_record_key": peer_record["source_record_key"],
                                   "profession_code": peer_record["profession_code"]} for peer_record in source_records],
                "registry_license_profession": "not_supplied_by_nppes"}
    if source_record["normalized_payload"]["visibility"] == "held_identity":
        return {**decision_by_field, "status": source_record["match_status"], "reason": source_record["match_evidence"]["reason"]}
    identity = source_record["raw_payload"]["rows"][0]["fields"]
    if _name(identity["Title"]) not in SUFFIX_LITERALS:
        return {**decision_by_field, "reason": "source_title_ambiguous"}
    identity_by_source = {}
    for peer_record in source_records:
        if peer_record["normalized_payload"]["visibility"] == "held_identity":
            return {**decision_by_field, "reason": "cross_board_identity_unresolved"}
        peer_identity = peer_record["raw_payload"]["rows"][0]["fields"]
        if _name(peer_identity["Title"]) not in SUFFIX_LITERALS:
            return {**decision_by_field, "reason": "cross_board_identity_unresolved"}
        identity_by_source[peer_record["source_record_key"]] = peer_identity
    name_keys = {tuple(_name(identity[field]) for field in ("FirstName", "MiddleName", "LastName", "Title"))
                 for identity in identity_by_source.values()}
    if len(name_keys) != len(identity_by_source):
        return {**decision_by_field, "reason": "cross_board_identity_unresolved"}
    if not candidates:
        return {**decision_by_field, "status": "unmatched", "reason": "no_exact_license_candidates"}
    npis_by_source = {key: set() for key in identity_by_source}
    candidate_source_keys = []
    # Explain every occurrence using exactly one source identity. A different
    # name is not discarded: only a fully validated peer can account for it.
    for candidate in candidates:
        matches = [key for key, identity in identity_by_source.items() if _is_compatible_candidate(identity, candidate)]
        if len(matches) != 1:
            return {**decision_by_field, "reason": "registry_identity_conflict"}
        npis_by_source[matches[0]].add(candidate["npi"])
        candidate_source_keys.append(matches[0])
    decision_by_field["candidate_source_record_keys"] = candidate_source_keys
    if any(len(npis) > 1 for npis in npis_by_source.values()):
        return {**decision_by_field, "status": "ambiguous", "reason": "multiple_matching_npis"}
    if any(not npis for npis in npis_by_source.values()):
        return {**decision_by_field, "reason": "cross_board_identity_unresolved"}
    npi_by_source = {key: next(iter(npis)) for key, npis in npis_by_source.items()}
    if len(set(npi_by_source.values())) != len(npi_by_source):
        return {**decision_by_field, "reason": "cross_board_npi_conflict"}
    return {**decision_by_field, "status": "deterministic", "npi": npi_by_source[source_record["source_record_key"]],
            "reason": "unique_exact_license_name"}


def bind_reports(reports_by_profession: dict, *, reports_sha256: str,
                 snapshot_path: Path, snapshot_sha256: str) -> dict:
    """Reparse both pinned MD/DO reports and return unmodified assertions plus NPI decisions.

    Report entries follow ``reports_content_sha256``. Both nonempty profession
    files are required to detect cross-board license collisions. Capture pins
    must come from separately authenticated acquisition, not this function.
    Parse results are owned here and updated in place, avoiding a second copy of
    the full reports. Only binding fields and fact.npi change; publication stays
    unset. This provides no capture, worker, import or publication authority.
    """
    _check_pin(reports_sha256, "reports")
    reports_by_profession = _report_inputs(reports_by_profession)
    _require(reports_content_sha256(reports_by_profession) == reports_sha256, "reports_changed")
    _require(reports_by_profession["1606"]["evidence"].get("run_id")
             == reports_by_profession["1907"]["evidence"].get("run_id"), "report_run_id_mismatch")
    snapshot = read_registry_snapshot(snapshot_path, snapshot_sha256=snapshot_sha256)
    candidates_by_license = defaultdict(list)
    for candidate in snapshot["registry_rows"]:
        candidates_by_license[candidate["license_number"]].append(candidate)
    retained_source_records, facts = [], []
    for profession in sorted(PROFESSIONS):
        report = reports_by_profession[profession]
        source_records, source_facts = parse_report(report["content"], evidence=report["evidence"])
        _require(source_records and all(source_record["profession_code"] == profession for source_record in source_records),
                 "report_profession_mismatch")
        retained_source_records.extend(source_records)
        facts.extend(source_facts)
    groups_by_license = defaultdict(list)
    for source_record in retained_source_records:
        if source_record["license_number"] is not None:
            groups_by_license[source_record["license_number"]].append(source_record)
    npi_by_record = {}
    for source_record in retained_source_records:
        decision_by_field = _decision(source_record, candidates_by_license.get(source_record["license_number"], []),
                             groups_by_license.get(source_record["license_number"], []))
        decision_by_field.update(reports_sha256=reports_sha256, snapshot_sha256=snapshot_sha256, coverage_scope=COVERAGE_SCOPE)
        source_record["match_evidence"]["registry_binding"] = decision_by_field
        source_record.update(matched_npi=decision_by_field["npi"], match_status=decision_by_field["status"])
        npi_by_record[source_record["record_id"]] = decision_by_field["npi"]
    for fact in facts:
        fact["npi"] = npi_by_record[fact["source_record_id"]]
    return {"schema_version": SCHEMA_VERSION, "reports_sha256": reports_sha256, "snapshot_sha256": snapshot_sha256,
            "coverage_scope": COVERAGE_SCOPE, "source_records": retained_source_records, "facts": facts}
