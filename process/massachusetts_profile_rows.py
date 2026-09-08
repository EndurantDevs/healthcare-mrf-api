# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure normalization of public BORIM profiles rooted in registry licenses."""

from __future__ import annotations

import copy
import hashlib
import json
import re
import unicodedata
from datetime import date, datetime

from process.provider_directory_profile import is_valid_npi

SOURCE_KEY = "massachusetts-borim"
SCHEMA_VERSION = "ma-borim-profile/v1"


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def _text(value):
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError("massachusetts_profile_invalid_text")
    return " ".join(value.split())


def _name(value):
    return unicodedata.normalize("NFKC", _text(value)).casefold()


def _names_match(profile, candidate, *, initials):
    first, last = _name(profile.get("firstName")), _name(profile.get("lastName"))
    other_first, other_last = _name(candidate.get("first_name")), _name(candidate.get("last_name"))
    if not first or not last or last != other_last:
        return False
    return first == other_first or (
        initials and bool(other_first) and min(len(first), len(other_first)) == 1
        and first[0] == other_first[0]
    )


def _match(profile, license_number, candidates):
    eligible_candidates = [candidate for candidate in candidates if (
        str(candidate.get("license_number") or "").strip() == license_number
        and is_valid_npi(candidate.get("npi"))
    )]
    source_npi = profile.get("npiNumber")
    evidence_by_field = {
        "method": "exact_ma_full_license_npi_name",
        "jurisdiction": "MA",
        "requested_license_number": license_number,
        "candidate_npis": sorted({int(candidate["npi"]) for candidate in eligible_candidates}),
        "source_npi": source_npi,
    }
    if source_npi not in (None, ""):
        if not is_valid_npi(source_npi):
            return None, "identity_conflict", {**evidence_by_field, "reason": "invalid_source_npi"}
        matching_candidates = [candidate for candidate in eligible_candidates if int(candidate["npi"]) == int(source_npi)]
        if not matching_candidates or not all(_names_match(profile, candidate, initials=True) for candidate in matching_candidates):
            return None, "identity_conflict", {**evidence_by_field, "reason": "source_npi_identity_conflict"}
        return int(source_npi), "deterministic", evidence_by_field
    evidence_by_field["method"] = "exact_ma_full_license_name"
    matching_npis = {int(candidate["npi"]) for candidate in eligible_candidates
                     if _names_match(profile, candidate, initials=False)}
    if len(matching_npis) == 1:
        return matching_npis.pop(), "deterministic", evidence_by_field
    status = "ambiguous" if matching_npis else "identity_conflict" if eligible_candidates else "unmatched"
    return None, status, {**evidence_by_field, "reason": "no_unique_exact_license_name"}


def _source_date(raw, field, observed_on):
    """Keep calendar precision; never reinterpret a date using a timezone."""
    date_text = _text(raw)
    if not date_text:
        return {}, []
    precision = "source"
    normalized = date_text
    parsed = None
    try:
        if re.fullmatch(r"[0-9]{4}", date_text):
            parsed, precision = date(int(date_text), 1, 1), "year"
        elif re.fullmatch(r"[0-9]{4}-[0-9]{2}", date_text):
            parsed, precision = date.fromisoformat(date_text + "-01"), "month"
        elif re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}(?:[T ][0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?(?:Z|[+-][0-9]{2}:[0-9]{2})?)?", date_text):
            parsed = datetime.fromisoformat(date_text).date()
            precision, normalized = "day", parsed.isoformat()
    except ValueError:
        parsed = None
    flags = []
    if parsed is None:
        precision = "source"
        flags.append(f"{field}_invalid")
    elif parsed > observed_on:
        flags.append(f"{field}_in_future")
    if parsed is not None and parsed.year < 1800:
        flags.append(f"{field}_before_1800")
    date_fields_by_name = {field: normalized, f"{field}_precision": precision}
    if field == "graduation_date" and parsed is not None:
        date_fields_by_name["graduation_year"] = parsed.year
        if parsed.year > observed_on.year:
            flags.append("graduation_year_in_future")
    return date_fields_by_name, flags


def _fact(source_record, evidence, category, value_by_field, raw_fields, index, flags):
    fact_type = "education_history" if category == "education" else "postgraduate_training"
    logical_key = _hash([SOURCE_KEY, source_record["license_number"], category, fact_type, index, value_by_field])
    display_fields = ("institution", "program_type", "specialty", "graduation_date", "attendance_start", "attendance_end")
    display = " — ".join(str(value_by_field[field]) for field in display_fields if value_by_field.get(field))
    return {
        "fact_id": _hash([source_record["record_id"], logical_key]),
        "run_id": source_record["run_id"],
        "npi": source_record["matched_npi"],
        "source_record_id": source_record["record_id"],
        "logical_fact_key": logical_key,
        "category": category,
        "fact_type": fact_type,
        "display": display,
        "value_json": value_by_field,
        "availability": "available",
        "assertion_type": "self_reported",
        "verification_status": "not_independently_verified",
        "effective_start": value_by_field.get("attendance_start") if value_by_field.get("attendance_start_precision") != "source" else None,
        "effective_end": value_by_field.get("attendance_end") if value_by_field.get("attendance_end_precision") != "source" else None,
        "source_json": {
            **evidence,
            "source_key": SOURCE_KEY,
            "schema_version": SCHEMA_VERSION,
            "agency": "Massachusetts Board of Registration in Medicine",
            "jurisdiction": "MA",
            "source_record_id": source_record["record_id"],
            "source_path": "educationAndTrainings.education" if category == "education" else f"educationAndTrainings.trainings[{index}]",
            "raw_fields": copy.deepcopy(raw_fields),
            "quality_flags": flags,
        },
        "sensitive": False,
        "public_default": True,
        "published_at": None,
    }


def _parser_evidence(profile, license_number, candidates, evidence):
    if not isinstance(profile, dict) or not profile:
        raise ValueError("massachusetts_profile_object_required")
    if (not isinstance(profile.get("licenseNumber"), str) or not profile["licenseNumber"].strip()
            or type(profile.get("licenseMetaId")) is not int):
        raise ValueError("massachusetts_profile_identity_schema_invalid")
    if not isinstance(license_number, str) or not re.fullmatch(r"[0-9]+", license_number):
        raise ValueError("massachusetts_profile_numeric_license_required")
    if not isinstance(candidates, (list, tuple)) or any(not isinstance(row, dict) for row in candidates):
        raise ValueError("massachusetts_profile_candidates_invalid")
    required_fields = {"run_id", "artifact_id", "source_url", "downloaded_at", "content_sha256", "row_number"}
    if not isinstance(evidence, dict) or not required_fields <= evidence.keys():
        raise ValueError("massachusetts_profile_evidence_missing")
    evidence_by_field = {key: copy.deepcopy(evidence[key]) for key in required_fields}
    if isinstance(evidence_by_field["downloaded_at"], datetime):
        evidence_by_field["downloaded_at"] = evidence_by_field["downloaded_at"].isoformat()
    datetime.fromisoformat(evidence_by_field["downloaded_at"])
    return evidence_by_field


def _retained_record(profile, license_number, evidence):
    record_key = f"{SOURCE_KEY}:{license_number}"
    return {
        "record_id": _hash([evidence["run_id"], record_key]),
        "run_id": evidence["run_id"],
        "artifact_id": evidence["artifact_id"],
        "source_key": SOURCE_KEY,
        "source_record_key": record_key,
        "profession_code": str(profile.get("licenseMetaId")) if profile.get("licenseMetaId") is not None else None,
        "license_id": str(profile.get("id")) if profile.get("id") is not None else None,
        "license_number": license_number,
        "raw_payload": copy.deepcopy(profile),
        "normalized_payload": {"schema_version": SCHEMA_VERSION, "visibility": "public", "quality_flags": []},
        "matched_npi": None,
        "match_status": "unmatched",
        "match_evidence": {"jurisdiction": "MA", "requested_license_number": license_number},
        "row_number": evidence["row_number"],
    }


def _is_held_profile(profile, license_number, source_record):
    if profile["licenseNumber"] != license_number or profile["licenseMetaId"] != 1:
        source_record["match_status"] = "identity_conflict"
        source_record["match_evidence"]["reason"] = "license_number_or_type_mismatch"
        source_record["normalized_payload"]["visibility"] = "held_identity"
        return True
    if profile.get("isPendingReview"):
        source_record["normalized_payload"]["visibility"] = "held_pending_review"
        source_record["match_evidence"]["reason"] = "profile_pending_review"
        return True
    return False


def _training_facts(trainings, source_record, evidence, observed_on):
    if trainings is None:
        return []
    if not isinstance(trainings, list) or any(not isinstance(training, dict) for training in trainings):
        raise ValueError("massachusetts_profile_training_schema_invalid")
    facts = []
    for index, training in enumerate(trainings):
        value_by_field, flags = _training_value(training, observed_on)
        if value_by_field:
            facts.append(_fact(
                source_record, evidence, "training", value_by_field,
                {key: training.get(key) for key in ("location", "programType", "specialty", "startDate", "endDate")},
                index, flags,
            ))
    return facts


def _training_value(training, observed_on):
    value_by_field = {field: _text(training.get(source_field)) for field, source_field in (
        ("institution", "location"), ("program_type", "programType"), ("specialty", "specialty"),
    ) if _text(training.get(source_field))}
    flags = []
    for field, source_field in (("attendance_start", "startDate"), ("attendance_end", "endDate")):
        date_fields, date_flags = _source_date(training.get(source_field), field, observed_on)
        value_by_field.update(date_fields)
        flags.extend(date_flags)
    if (value_by_field.get("attendance_start_precision") == value_by_field.get("attendance_end_precision") == "day"
            and value_by_field["attendance_start"] > value_by_field["attendance_end"]):
        flags.append("training_period_reversed")
    return value_by_field, flags


def _visible_facts(profile, source_record, evidence):
    section = profile.get("educationAndTrainings")
    if section is None:
        return []
    if not isinstance(section, dict) or (section.get("education") is not None and not isinstance(section["education"], dict)):
        raise ValueError("massachusetts_profile_education_schema_invalid")
    education = section.get("education") or {}
    institution = _text(education.get("name"))
    # The public UI hides this entire section unless the school name is present.
    if not institution:
        source_record["normalized_payload"]["visibility"] = "education_section_hidden"
        return []
    observed_on = datetime.fromisoformat(evidence["downloaded_at"]).date()
    value_by_field, flags = _source_date(education.get("graduationDate"), "graduation_date", observed_on)
    value_by_field["institution"] = institution
    facts = [_fact(source_record, evidence, "education", value_by_field,
                   {key: education.get(key) for key in ("name", "graduationDate")}, 0, flags)]
    facts.extend(_training_facts(section.get("trainings"), source_record, evidence, observed_on))
    source_record["normalized_payload"]["quality_flags"] = sorted({
        flag for fact in facts for flag in fact["source_json"]["quality_flags"]
    })
    return facts


def parse_profile(profile, *, license_number, candidates, evidence):
    """Normalize one public response against a trusted captured physician cohort.

    Candidates must already be filtered by the exact NUCC physician grouping,
    individual entity type and MA license state in capture_registry_cohort.
    Only surrounding candidate-license whitespace is ignored; source licenses
    remain exact. No prefix, leading-zero or taxonomy-prefix guesses occur here.

    Invalid schemas raise ValueError. Conflicts retain npi=None; hidden sections
    yield no facts. The caller owns acquisition, persistence and publication.
    """
    evidence_by_field = _parser_evidence(profile, license_number, candidates, evidence)
    source_record = _retained_record(profile, license_number, evidence_by_field)
    if _is_held_profile(profile, license_number, source_record):
        return source_record, []
    npi, status, match_evidence = _match(profile, license_number, candidates)
    source_record.update(matched_npi=npi, match_status=status, match_evidence=match_evidence)
    return source_record, _visible_facts(profile, source_record, evidence_by_field)
