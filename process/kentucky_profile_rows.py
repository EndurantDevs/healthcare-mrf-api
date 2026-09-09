# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure retention of exact-license Kentucky public physician profiles."""

from __future__ import annotations

import copy
import hashlib
import json
import re
import unicodedata
from datetime import datetime
from html.parser import HTMLParser
from urllib.parse import parse_qs, urlsplit

from process.provider_directory_profile import is_valid_npi

SOURCE_KEY = "kentucky-kbml"
SCHEMA_VERSION = "ky-kbml-profile/v1"
LEGACY_CATEGORIES = ("education",)
PROFILE_CATEGORIES = (*LEGACY_CATEGORIES, "specialties", "services")
MAX_HTML_BYTES = 1_000_000
MAX_FIELD_ROWS = 2048


def _require(condition, reason):
    if not condition:
        raise ValueError("kentucky_profile_" + reason)


def _text(value):
    _require(isinstance(value, str), "invalid_text")
    return " ".join(value.split())


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


class _DetailParser(HTMLParser):
    """Read complete label/value div rows; surrounding legacy markup is inert."""

    def __init__(self, *, reject_hidden=False):
        super().__init__(convert_charrefs=True)
        self.reject_hidden = reject_hidden
        self.div_depth = 0
        self.row_depth = None
        self.cell_depth = None
        self.cell_kind = None
        self.cell_parts = []
        self.row_cells = []
        self.field_rows = []
        self.form_actions = []
        self.detail_choices = []
        self.criterion_parts = []
        self.criterion_count = 0
        self.in_criterion = False
        self.in_result_form = False
        self.content_count = 0
        self.content_depth = None
        self.results_started = False
        self.result_tail_parts = []
        self.closed_html = False

    def handle_starttag(self, tag, attrs):
        """Capture detail controls and enter bounded label/value cells."""
        _require(not self.closed_html, "markup_after_document")
        attrs_by_name = dict(attrs)
        _require(len(attrs_by_name) == len(attrs), "duplicate_html_attribute")
        if self.reject_hidden and not (tag == "input" and (attrs_by_name.get("type") or "").lower() == "hidden"):
            _require("hidden" not in attrs_by_name and (attrs_by_name.get("aria-hidden") or "").lower() != "true"
                     and not re.search(r"display\s*:\s*none|visibility\s*:\s*(?:hidden|collapse)",
                                       attrs_by_name.get("style") or "", re.IGNORECASE), "hidden_profile_markup")
        if tag == "form" and attrs_by_name.get("id") == "Form1":
            self.form_actions.append(attrs_by_name.get("action", ""))
            self.in_result_form = True
        if tag == "input" and attrs_by_name.get("id") == "usLicenseList_rdbtDetail":
            self.detail_choices.append(attrs_by_name.get("value") == "rdbtDetail" and "checked" in attrs_by_name)
        if tag == "span" and attrs_by_name.get("id") == "usLicenseList_lblSearchCriterion":
            _require(self.content_depth is not None, "criterion_outside_result_content")
            self.criterion_count += 1
            self.in_criterion = True
        classes = (attrs_by_name.get("class") or "").split()
        if self.results_started and self.in_result_form and self.cell_depth is None:
            allowed_classes = {"row", "cols2-col1", "cols2-col2"} if self.content_depth is not None else {"ky-cm-post"}
            _require(tag == "div" and bool(set(classes) & allowed_classes), "unexpected_result_markup")
        if tag == "div":
            self._start_div(classes)
        elif tag == "br" and self.cell_depth is not None:
            self.cell_parts.append(" ")

    def _start_div(self, classes):
        self.div_depth += 1
        _require(self.div_depth <= 64, "html_nesting_limit")
        if "ky-cm-content" in classes:
            _require(self.in_result_form and self.content_depth is None, "result_content_invalid")
            self.content_count += 1
            self.content_depth = self.div_depth
        if "row" in classes:
            _require(self.row_depth is None and self.content_depth is not None and self.results_started, "nested_or_misplaced_field_row")
            self.row_depth = self.div_depth
            self.row_cells = []
        kinds = set(classes) & {"cols2-col1", "cols2-col2"}
        if kinds:
            _require(len(kinds) == 1 and self.row_depth is not None
                     and self.cell_depth is None and self.div_depth == self.row_depth + 1, "invalid_field_cell")
            self.cell_kind = kinds.pop()
            self.cell_depth = self.div_depth
            self.cell_parts = []

    def handle_data(self, text):
        """Preserve decoded source text inside the selected field and criterion."""
        if self.cell_depth is not None:
            self.cell_parts.append(text)
        if self.in_criterion:
            self.criterion_parts.append(text)
        elif self.results_started and self.content_depth is not None and self.cell_depth is None:
            _require(not _text(text), "unexpected_result_text")
        elif self.results_started and self.in_result_form and self.cell_depth is None:
            self.result_tail_parts.append(text)

    def handle_endtag(self, tag):
        """Finish complete cells and reject inconsistent div boundaries."""
        if tag == "span" and self.in_criterion:
            self.in_criterion = False
            self.results_started = True
        if tag == "form":
            self.in_result_form = False
        if tag == "html":
            self.closed_html = True
        if tag != "div":
            return
        _require(self.div_depth > 0, "unbalanced_div")
        if self.cell_depth == self.div_depth:
            self.row_cells.append((self.cell_kind, "".join(self.cell_parts)))
            self.cell_depth = None
        if self.row_depth == self.div_depth:
            _require([kind for kind, _ in self.row_cells] == ["cols2-col1", "cols2-col2"], "incomplete_field_row")
            self.field_rows.append(tuple(text for _, text in self.row_cells))
            _require(len(self.field_rows) <= MAX_FIELD_ROWS, "field_row_limit")
            self.row_depth = None
        if self.content_depth == self.div_depth:
            self.content_depth = None
        self.div_depth -= 1


def _validate_envelope(parser, license_number):
    _require(parser.closed_html and parser.div_depth == 0 and not parser.in_criterion
             and not parser.in_result_form, "incomplete_html")
    _require(len(parser.form_actions) == 1 and parser.detail_choices == [True]
             and parser.criterion_count == 1 and parser.content_count == 1, "result_envelope_invalid")
    action = urlsplit(parser.form_actions[0])
    expected_query_by_field = {"AGY": ["5"], "FLD1": [""], "FLD2": [license_number], "FLD3": ["0"], "FLD4": ["0"], "TYPE": [""]}
    _require(action.path == "LicenseList.aspx" and not action.scheme and not action.netloc and not action.fragment
             and parse_qs(action.query, keep_blank_values=True) == expected_query_by_field, "result_query_mismatch")
    expected = f"Search Criterion: KY License Number = {license_number}; Practice County = 0; Specialty = 0;"
    _require(_text("".join(parser.criterion_parts)) == expected, "result_criterion_mismatch")
    tail = _text("".join(parser.result_tail_parts))
    _require(not tail or re.fullmatch(r"Published: [0-9]{2}/[0-9]{2}/[0-9]{4} \[wvd\]", tail), "unexpected_result_text")


def extract_profiles(html, *, license_number, reject_hidden=False):
    """Extract all profiles from the validated exact-license detail envelope."""
    _require(isinstance(license_number, str) and re.fullmatch(r"[A-Za-z0-9]{1,32}", license_number), "invalid_license")
    _require(isinstance(html, str) and len(html.encode()) <= MAX_HTML_BYTES, "html_input_limit")
    parser = _DetailParser(reject_hidden=reject_hidden)
    parser.feed(html)
    parser.close()
    _validate_envelope(parser, license_number)
    profiles = []
    for raw_label, raw_value in parser.field_rows:
        label = _text(raw_label).rstrip(":").strip()
        if not label:
            _require(not _text(raw_value), "unlabelled_field")
            continue
        if label == "Name":
            profiles.append({})
        _require(bool(profiles) and label not in profiles[-1], "missing_or_duplicate_identity_label")
        profiles[-1][label] = raw_value
    for profile in profiles:
        _require({"Name", "License", "Medical School", "Year Graduated"} <= profile.keys(), "profile_labels_missing")
        _require(_text(profile["Name"]) and _text(profile["License"]), "empty_identity")
    return profiles


def _normalized_name(name):
    return _text(unicodedata.normalize("NFKC", name).casefold().replace(".", "").replace(",", " "))


def _candidate_names(candidate):
    first, last = candidate.get("first_name"), candidate.get("last_name")
    if not isinstance(first, str) or not isinstance(last, str) or not _text(first) or not _text(last):
        return set()
    middle, suffix = candidate.get("middle_name") or "", candidate.get("suffix") or ""
    if not isinstance(middle, str) or not isinstance(suffix, str):
        return set()
    middle_names = {_text(middle)}
    if _text(middle):
        middle_names.add(" ".join(part[0] for part in _text(middle).split()))
    return {_normalized_name(" ".join((first, middle_name, last, suffix))) for middle_name in middle_names}


def _match_profile(profile, license_number, candidates):
    eligible_candidates = [candidate for candidate in candidates if candidate.get("license_state", "KY") == "KY"
                and str(candidate.get("license_number") or "").strip() == license_number
                and is_valid_npi(candidate.get("npi"))]
    display_name = _text(unicodedata.normalize("NFKC", profile["Name"]))
    source_name = _normalized_name(re.sub(r" (?i:M\.D\.?|D\.O\.?)$", "", display_name))
    evidence_by_field = {"method": "exact_ky_full_license_full_name", "jurisdiction": "KY",
                         "requested_license_number": license_number, "source_name": profile["Name"],
                         "candidate_npis": sorted({int(candidate["npi"]) for candidate in eligible_candidates})}
    # Undotted uppercase MD/DO may be a credential or a surname; preserve it without guessing.
    if re.search(r" (?:MD|DO)$", display_name):
        return None, "identity_conflict" if eligible_candidates else "unmatched", {
            **evidence_by_field, "reason": "ambiguous_undotted_credential",
        }
    compatible_npis = {int(candidate["npi"]) for candidate in eligible_candidates if source_name in _candidate_names(candidate)}
    if len(compatible_npis) == 1:
        matched_npi = next(iter(compatible_npis))
        if all(source_name in _candidate_names(candidate) for candidate in eligible_candidates if int(candidate["npi"]) == matched_npi):
            return matched_npi, "deterministic", evidence_by_field
    status = "ambiguous" if len(compatible_npis) > 1 else "identity_conflict" if eligible_candidates else "unmatched"
    return None, status, {**evidence_by_field, "reason": "no_unique_exact_license_name"}


def _validated_evidence(evidence):
    fields = {"run_id", "artifact_id", "source_url", "downloaded_at", "content_sha256", "row_number"}
    _require(isinstance(evidence, dict) and fields <= evidence.keys(), "evidence_missing")
    evidence_by_field = {key: copy.deepcopy(evidence[key]) for key in fields}
    if isinstance(evidence_by_field["downloaded_at"], datetime):
        evidence_by_field["downloaded_at"] = evidence_by_field["downloaded_at"].isoformat()
    try:
        datetime.fromisoformat(evidence_by_field["downloaded_at"])
    except (TypeError, ValueError):
        raise ValueError("kentucky_profile_evidence_timestamp_invalid") from None
    return evidence_by_field


def _retained_record(html, profiles, license_number, evidence):
    record_key = f"{SOURCE_KEY}:{license_number}"
    return {"record_id": _hash([evidence["run_id"], record_key]), "run_id": evidence["run_id"],
            "artifact_id": evidence["artifact_id"], "source_key": SOURCE_KEY, "source_record_key": record_key,
            "profession_code": None, "license_id": None, "license_number": license_number,
            "raw_payload": {"html": html, "profiles": copy.deepcopy(profiles)},
            "normalized_payload": {"schema_version": SCHEMA_VERSION, "visibility": "public", "quality_flags": []},
            "matched_npi": None, "match_status": "unmatched", "row_number": evidence["row_number"],
            "match_evidence": {"jurisdiction": "KY", "requested_license_number": license_number}}


def _education_value(profile, observed_year):
    institution, year_text = _text(profile["Medical School"]), _text(profile["Year Graduated"])
    value_by_field = {"institution": institution} if institution else {}
    flags = []
    if year_text:
        if not re.fullmatch(r"[0-9]{4}", year_text) or int(year_text) == 0:
            flags.append("graduation_year_invalid")
        else:
            value_by_field["graduation_year"] = int(year_text)
            if int(year_text) > observed_year:
                flags.append("graduation_year_in_future")
            if int(year_text) < 1800:
                flags.append("graduation_year_before_1800")
    return value_by_field, flags


def _education_fact(profile, source_record, evidence):
    value_by_field, flags = _education_value(profile, datetime.fromisoformat(evidence["downloaded_at"]).year)
    source_record["normalized_payload"]["quality_flags"] = list(flags)
    if not value_by_field:
        source_record["normalized_payload"]["visibility"] = "education_unusable" if flags else "education_not_reported"
        return []
    return [_profile_fact(source_record, evidence, "education", "education_history", value_by_field,
                          {label: profile[label] for label in ("Medical School", "Year Graduated")},
                          " — ".join(str(value) for value in value_by_field.values()), flags)]


def _profile_fact(source_record, evidence, category, fact_type, value_by_field, raw_fields, display, flags):
    logical_key = _hash([SOURCE_KEY, source_record["license_number"], fact_type, value_by_field])
    return {"fact_id": _hash([source_record["record_id"], logical_key]), "run_id": source_record["run_id"],
             "npi": source_record["matched_npi"], "source_record_id": source_record["record_id"],
             "logical_fact_key": logical_key, "category": category, "fact_type": fact_type,
             "display": display, "value_json": value_by_field,
             "availability": "available", "assertion_type": "source_reported", "verification_status": "not_independently_verified",
             "effective_start": None, "effective_end": None, "sensitive": False, "public_default": True, "published_at": None,
             "source_json": {**evidence, "source_key": SOURCE_KEY, "schema_version": SCHEMA_VERSION,
                 "agency": "Kentucky Board of Medical Licensure", "jurisdiction": "KY", "source_record_id": source_record["record_id"],
                 "source_path": "detail." + "+".join(raw_fields), "quality_flags": flags,
                 "raw_fields": raw_fields}}


def _portfolio_facts(profile, source_record, evidence):
    _require({"*Area of Practice", "Type of Practice"} <= profile.keys(), "portfolio_labels_missing")
    facts = []
    for label, category, fact_type, value_field, display_label in (
        ("*Area of Practice", "specialties", "specialty", "text", "Reported area of practice"),
        ("Type of Practice", "services", "practice_type", "practice_type", "Reported type of practice"),
    ):
        raw_value = profile[label]
        reported = _text(raw_value)
        if reported and reported.casefold() != "none on file":
            facts.append(_profile_fact(source_record, evidence, category, fact_type, {value_field: reported},
                                       {label: raw_value}, f"{display_label}: {reported}", []))
    if facts and source_record["normalized_payload"]["visibility"] != "public":
        normalized = source_record["normalized_payload"]
        normalized["education_visibility"] = normalized["visibility"]
        normalized["visibility"] = "public"
    return facts


def parse_profile(html, *, license_number, candidates, evidence, categories=LEGACY_CATEGORIES):
    """Retain one exact-license result, without acquisition or publication.

    Candidates are trusted registry rows already restricted to individual
    physicians and Kentucky licenses; an explicit non-KY license_state is rejected.
    Names are compared against assembled registry components, never split from
    the source. Missing middle names or suffixes are not guessed. Ambiguous or
    unmatched source facts retain npi=None. Multiple source results are held.
    """
    _require(isinstance(candidates, (list, tuple)) and all(isinstance(candidate, dict) for candidate in candidates), "invalid_candidates")
    _require(categories in (LEGACY_CATEGORIES, PROFILE_CATEGORIES, list(LEGACY_CATEGORIES), list(PROFILE_CATEGORIES)),
             "categories_invalid")
    evidence_by_field = _validated_evidence(evidence)
    profiles = extract_profiles(html, license_number=license_number, reject_hidden="specialties" in categories)
    source_record = _retained_record(html, profiles, license_number, evidence_by_field)
    if not profiles:
        source_record.update(match_status="not_found")
        source_record["normalized_payload"]["visibility"] = "not_found"
        return source_record, []
    if len(profiles) != 1 or _text(profiles[0]["License"]) != license_number:
        source_record.update(match_status="identity_conflict")
        source_record["normalized_payload"]["visibility"] = "held_identity"
        source_record["match_evidence"]["reason"] = "multiple_or_mismatched_result_identities"
        return source_record, []
    matched_npi, status, match_evidence = _match_profile(profiles[0], license_number, candidates)
    source_record.update(matched_npi=matched_npi, match_status=status, match_evidence=match_evidence)
    facts = _education_fact(profiles[0], source_record, evidence_by_field)
    if "specialties" in categories:
        facts.extend(_portfolio_facts(profiles[0], source_record, evidence_by_field))
    return source_record, facts
