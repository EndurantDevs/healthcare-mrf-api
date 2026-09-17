# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure RI license identity decisions over supplied registry occurrences."""

from __future__ import annotations

import copy
import re

from process.provider_directory_profile import is_valid_npi
from process.rhode_island_profile_rows import PROFILE_FIELDS, parse_profile

REGISTRY_COLUMNS = frozenset(
    {
        "npi",
        "taxonomy_occurrence_checksum",
        "license_number",
        "license_state",
        "taxonomy",
        "primary_taxonomy_switch",
        "joined_npi",
        "entity_type_code",
        "first_name",
        "middle_name",
        "last_name",
        "suffix",
        "joined_taxonomy_code",
        "taxonomy_grouping",
    }
)
NAME_FIELDS = (("First_Name", "first_name"), ("Middle_Name", "middle_name"), ("Last_Name", "last_name"))


def _name(value):
    return " ".join(value.split()).casefold()


def _is_matching_candidate(identity_by_field, candidate):
    if (
        set(candidate) != REGISTRY_COLUMNS
        or type(candidate.get("npi")) is not int
        or not is_valid_npi(candidate["npi"])
        or type(candidate.get("joined_npi")) is not int
        or candidate["joined_npi"] != candidate["npi"]
        or type(candidate.get("taxonomy_occurrence_checksum")) is not int
        or type(candidate.get("entity_type_code")) is not int
        or candidate["entity_type_code"] != 1
        or not isinstance(candidate.get("taxonomy"), str)
        or not candidate["taxonomy"].strip()
        or candidate.get("joined_taxonomy_code") != candidate["taxonomy"]
        or candidate.get("taxonomy_grouping") != "Allopathic & Osteopathic Physicians"
        or candidate.get("suffix") not in (None, "")
    ):
        return False
    for source_field, registry_field in NAME_FIELDS:
        value = candidate.get(registry_field)
        if value is None and registry_field == "middle_name":
            value = ""
        if not isinstance(value, str) or _name(identity_by_field[source_field]) != _name(value):
            return False
    return True


def _decision(identity_by_field, license_number, candidates):
    indexes = [
        index
        for index, candidate_by_field in enumerate(candidates)
        if candidate_by_field.get("license_state") == "RI"
        and candidate_by_field.get("license_number") == license_number
    ]
    exact_candidates = [candidates[index] for index in indexes]
    decision_by_field = {
        "method": "exact_ri_prefixed_license_name_components",
        "status": "unmatched",
        "npi": None,
        "candidate_rows": copy.deepcopy(candidates),
        "exact_candidate_indexes": indexes,
        "coverage_scope": "supplied_registry_occurrences_only",
        "registry_completeness_verified": False,
        "reason": "no_exact_prefixed_license_candidates",
    }
    if not exact_candidates:
        return decision_by_field
    # The source has no separate suffix field. Do not guess suffix placement or
    # let initial-only first/last names establish an exact person identity.
    if any(
        not any(len(part) > 1 for part in re.findall(r"[^\W\d_]+", identity_by_field[field]))
        for field in ("First_Name", "Last_Name")
    ):
        return {**decision_by_field, "status": "identity_conflict", "reason": "source_name_incomplete"}
    if not all(
        _is_matching_candidate(identity_by_field, candidate_by_field) for candidate_by_field in exact_candidates
    ):
        return {**decision_by_field, "status": "identity_conflict", "reason": "registry_identity_conflict"}
    npis = {candidate_by_field["npi"] for candidate_by_field in exact_candidates}
    if len(npis) != 1:
        return {**decision_by_field, "status": "ambiguous", "reason": "multiple_matching_npis"}
    return {
        **decision_by_field,
        "status": "deterministic",
        "npi": next(iter(npis)),
        "reason": "unique_exact_license_name",
    }


def bind_profile(payload, *, license_number, evidence, candidates):
    """Reparse retained bytes and attach a conditional, offline NPI decision.

    Supply every registry occurrence, including invalid joins, for the exact RI
    license. Licenses are compared literally: no trimming, prefix inference,
    zero padding or board substitution. All supplied rows survive in evidence.
    Complete registry capture, source acquisition/schema-page validation and
    publication remain caller-owned; this function proves none of those gates.
    """
    if not isinstance(candidates, (list, tuple)) or any(
        not isinstance(row, dict)
        or "license_number" not in row
        or "license_state" not in row
        or row["license_number"] is not None
        and not isinstance(row["license_number"], str)
        or not isinstance(row["license_state"], str)
        for row in candidates
    ):
        raise ValueError("rhode_island_binding_invalid_candidates")
    record, facts = parse_profile(payload, license_number=license_number, evidence=evidence)
    identity_by_field = dict(zip(PROFILE_FIELDS, record["raw_payload"]["values"][: len(PROFILE_FIELDS)], strict=True))
    decision = _decision(identity_by_field, license_number, candidates)
    record["match_evidence"]["registry_binding"] = decision
    record["match_evidence"]["npi_binding"] = "supplied_registry_occurrences"
    record.update(matched_npi=decision["npi"], match_status=decision["status"])
    for fact in facts:
        fact["npi"] = decision["npi"]
    return record, facts
