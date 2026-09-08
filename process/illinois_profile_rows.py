# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retain Illinois public Education pages without inventing license identities."""

from __future__ import annotations

import copy
import hashlib
import re
from datetime import datetime
from html.parser import HTMLParser
from urllib.parse import urlsplit

from process.massachusetts_profile_rows import _hash

SOURCE_KEY = "illinois-idfpr"
SCHEMA_VERSION = "il-idfpr-profile/v1"
MAX_HTML_BYTES = 1_000_000
MAX_FIELDS = 2048
PREFIX = "ctl00_ctl00_MainContent_MainContentContainer_"
IDENTITY_FIELDS = {
    "FormattedContactName": "display_name", "FirstEffectiveDate": "original_issue_date",
    "LicenseStatus": "license_status", "ExpirationDate": "expiration_date",
}
HEADER_FIELDS = set(IDENTITY_FIELDS) | {
    "LocationCityState", "lblLicenseStatus", "lblOrigIssueDate", "lblExpirationDate",
}
SECTION_FIELDS = {"SectionTitle", "HeadingNote", "Required", "LastUpdatedText", "SectionPublicDisclaimer"}
FIELD_PATTERN = re.compile(PREFIX + r"(repProfileHeader|repSection[123](?:Header)?)_(ctl[0-9]+_ctl[0-9]+)_([A-Za-z]+)")


def _require(condition, reason):
    if not condition:
        raise ValueError("illinois_profile_" + reason)


def _text(raw_text):
    return " ".join(raw_text.split())


def _is_hidden(attrs):
    style = re.sub(r"\s+", "", attrs.get("style") or "").lower()
    return "hidden" in attrs or (attrs.get("aria-hidden") or "").lower() == "true" or bool(
        re.search(r"(?:^|;)(?:display:none|visibility:hidden)(?:!important)?(?:;|$)", style)
    )


class _EducationParser(HTMLParser):
    """Read observed repeater spans and their section/table ownership."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.divs = []
        self.tables = []
        self.table_count = 0
        self.span_depth = 0
        self.capture = None
        self.fields = []
        self.field_ids = set()
        self.sections = []
        self.header_count = 0
        self.form_count = 0
        self.in_form = False
        self.form_closed = False
        self.html_closed = False
        self.body_hidden = False
        self.body_count = 0
        self.html_count = 0
        self.education_links = []
        self.active_link = None

    def handle_starttag(self, tag, attrs):
        """Track visible profile containers before accepting their source fields."""
        _require(not self.html_closed, "markup_after_document")
        attrs_by_name = dict(attrs)
        _require(len(attrs_by_name) == len(attrs), "duplicate_attribute")
        is_hidden = _is_hidden(attrs_by_name)
        if self.capture is not None:
            _require(tag in {"span", "b", "strong", "i", "em", "br"}, "unexpected_field_markup")
        if tag == "html":
            self.html_count += 1
            _require(self.html_count == 1, "duplicate_document")
        if tag == "body":
            self.body_count += 1
            _require(self.body_count == 1, "duplicate_body")
            self.body_hidden = is_hidden
        if tag == "form":
            self._start_form(attrs_by_name, is_hidden)
        if tag == "div":
            self._start_div(attrs_by_name, is_hidden)
        if self._section() is not None or self.capture is not None or any(
                identifier.endswith("_divProfileHeader") for identifier, _ in self.divs):
            _require(not is_hidden and tag not in {"script", "iframe", "object"}, "hidden_or_active_section")
        if tag == "table":
            self.table_count += 1
            self.tables.append(self.table_count)
            _require(len(self.tables) <= 64, "table_nesting_limit")
        if tag == "span":
            self.span_depth += 1
            self._start_field(attrs_by_name, is_hidden)
        if tag == "a":
            self.active_link = {"text": [], "attrs": attrs_by_name, "visible": self.in_form and not is_hidden
                                and not self.body_hidden and not any(hidden for _, hidden in self.divs)}
        if tag == "br" and self.capture is not None:
            self.capture["parts"].append("\n")
        _require(not is_hidden or tag == "input", "hidden_container")

    def _start_form(self, attrs, hidden):
        action = urlsplit(attrs.get("action") or "")
        _require(not self.in_form and self.form_count == 0 and attrs.get("id") == "aspnetForm"
                 and attrs.get("method", "").lower() == "post" and not hidden
                 and not action.scheme and not action.netloc and not action.fragment
                 and action.path.lower() == "profiledetails.aspx", "invalid_profile_form")
        self.form_count += 1
        self.in_form = True

    def _start_div(self, attrs, hidden):
        identifier = attrs.get("id") or ""
        parent_hidden = self.divs[-1][1] if self.divs else self.body_hidden
        self.divs.append((identifier, hidden or parent_hidden))
        _require(len(self.divs) <= 64, "div_nesting_limit")
        if re.fullmatch(PREFIX + r"repProfileHeader_ctl[0-9]+_ctl[0-9]+_divProfileHeader", identifier):
            _require(self.in_form and not self.divs[-1][1], "hidden_or_misplaced_header")
            self.header_count += 1
        if identifier.startswith(PREFIX + "divSection"):
            section = identifier.removeprefix(PREFIX + "divSection")
            _require(self.in_form and section in {"1", "2", "3"} and not self.divs[-1][1], "unrecognized_section")
            self.sections.append(section)

    def _section(self):
        return next((identifier.removeprefix(PREFIX + "divSection") for identifier, _ in reversed(self.divs)
                     if identifier.startswith(PREFIX + "divSection")), None)

    def _start_field(self, attrs, hidden):
        identifier = attrs.get("id") or ""
        matched = FIELD_PATTERN.fullmatch(identifier)
        if matched is None:
            _require(not identifier.startswith((PREFIX + "repSection", PREFIX + "repProfileHeader")), "unrecognized_repeater_field")
            return
        _require(self.capture is None and identifier not in self.field_ids, "duplicate_or_nested_field")
        _require(self.in_form and not hidden and not self.body_hidden and self.tables
                 and not any(is_hidden for _, is_hidden in self.divs), "hidden_or_misplaced_field")
        group, index, field = matched.groups()
        section = self._section()
        if group == "repProfileHeader":
            header_id = PREFIX + group + "_" + index + "_divProfileHeader"
            _require(field in HEADER_FIELDS and any(identifier == header_id
                     for identifier, _ in self.divs), "unrecognized_header_field")
        else:
            _require(section == group[10:11], "field_section_mismatch")
            allowed = SECTION_FIELDS if group.endswith("Header") else {
                "1": {"YearsInPracticeLabel", "YearsInPractice"}, "2": {"SchoolLocation"},
                "3": {"ProgramType", "Specialty", "SchoolLocation"},
            }[section]
            _require(field in allowed, "unrecognized_repeater_field")
        self.field_ids.add(identifier)
        _require(len(self.field_ids) <= MAX_FIELDS, "field_count_limit")
        self.capture = {"id": identifier, "group": group, "index": index, "field": field,
                        "table": self.tables[-1], "depth": self.span_depth, "parts": []}

    def handle_endtag(self, tag):
        """Close only fully collected fields and balanced profile containers."""
        if tag == "span":
            _require(self.span_depth > 0, "unbalanced_span")
            if self.capture is not None and self.capture["depth"] == self.span_depth:
                self.fields.append({**self.capture, "raw": "".join(self.capture["parts"])})
                self.capture = None
            self.span_depth -= 1
        if tag == "div":
            _require(bool(self.divs), "unbalanced_div")
            self.divs.pop()
        if tag == "table":
            _require(bool(self.tables) and self.capture is None, "unbalanced_table")
            self.tables.pop()
        if tag == "a" and self.active_link is not None:
            if _text("".join(self.active_link["text"])) == "Education":
                _require(self.active_link["visible"], "hidden_or_misplaced_education_tab")
                self.education_links.append(self.active_link["attrs"])
            self.active_link = None
        if tag == "form":
            _require(self.in_form and self.capture is None and not self.divs and not self.tables, "unbalanced_form")
            self.in_form = False
            self.form_closed = True
        if tag == "html":
            _require(self.form_closed, "incomplete_document")
            self.html_closed = True

    def handle_data(self, text):
        """Preserve field text and reject unrecognized visible education content."""
        if self.capture is not None:
            self.capture["parts"].append(text)
        elif self._section() in {"2", "3"} and _text(text):
            _require(self.active_link is not None and _text(text) == "Top of Profile", "unexpected_section_text")
        elif self.html_closed:
            _require(not _text(text), "text_after_document")
        if self.active_link is not None:
            self.active_link["text"].append(text)


def _one_field(fields, group, field):
    selected_fields = [entry for entry in fields if entry["group"] == group and entry["field"] == field]
    _require(len(selected_fields) == 1, "missing_or_duplicate_field")
    return selected_fields[0]["raw"]


def _section_entries(fields, number):
    group = "repSection" + number
    entries_by_index = {}
    for entry in fields:
        if entry["group"] != group:
            continue
        retained = entries_by_index.setdefault(entry["index"], {"source_path": group + "_" + entry["index"],
                            "table": entry["table"], "fields": {}, "field_ids": {}})
        _require(retained["table"] == entry["table"], "split_repeater_row")
        retained["fields"][entry["field"]] = entry["raw"]
        retained["field_ids"][entry["field"]] = entry["id"]
    expected = {"SchoolLocation"} if number == "2" else {"ProgramType", "Specialty", "SchoolLocation"}
    _require(entries_by_index, "unrecognized_empty_section")
    _require(len({entry["table"] for entry in entries_by_index.values()}) == len(entries_by_index), "mixed_repeater_rows")
    for entry in entries_by_index.values():
        _require(set(entry["fields"]) == expected, "incomplete_repeater_row")
        _require(any(_text(raw) for raw in entry["fields"].values()), "empty_repeater_row")
        entry.pop("table")
    return list(entries_by_index.values())


def extract_profile(html):
    """Return visible source identity and ordered repeaters; no license/NPI join."""
    _require(isinstance(html, str) and len(html.encode()) <= MAX_HTML_BYTES, "html_input_limit")
    parser = _EducationParser()
    parser.feed(html)
    parser.close()
    _require(parser.html_closed and parser.form_closed and parser.capture is None and parser.span_depth == 0
             and not parser.divs and not parser.tables and parser.header_count == 1
             and parser.body_count == parser.html_count == 1, "incomplete_profile")
    _require(sorted(parser.sections) == ["1", "2", "3"], "section_set_invalid")
    _require(len(parser.education_links) == 1 and "profile_tabs_selected" in
             (parser.education_links[0].get("class") or "").split()
             and parser.education_links[0].get("href") == "javascript:__doPostBack('ctl00$ctl00$MainContent$MainContentContainer$ProfileMenu','3')",
             "education_tab_not_selected")
    identity_by_field = {name: _one_field(parser.fields, "repProfileHeader", field) for field, name in IDENTITY_FIELDS.items()}
    _require(all(_text(raw) for raw in identity_by_field.values()), "missing_identity")
    metadata_tables = {entry["table"] for entry in parser.fields if entry["group"] == "repProfileHeader"
                       and entry["field"] in {"FirstEffectiveDate", "LicenseStatus", "ExpirationDate"}}
    _require(len(metadata_tables) == 1, "mixed_header_tables")
    section_metadata_by_number = {}
    for number, title in (("2", "Medical School"), ("3", "Post Graduate Education")):
        group = "repSection" + number + "Header"
        _require(_text(_one_field(parser.fields, group, "SectionTitle")) == title, "section_title_mismatch")
        section_metadata_by_number[number] = {field: _one_field(parser.fields, group, field) for field in SECTION_FIELDS}
    return {"identity": identity_by_field, "education": _section_entries(parser.fields, "2"),
            "training": _section_entries(parser.fields, "3"), "section_metadata": section_metadata_by_number}


def _validated_evidence(evidence, html):
    fields = {"run_id", "artifact_id", "source_url", "downloaded_at", "content_sha256", "row_number"}
    _require(isinstance(evidence, dict) and fields <= evidence.keys(), "evidence_missing")
    evidence_by_field = {field: copy.deepcopy(evidence[field]) for field in fields}
    _require(all(isinstance(evidence_by_field[field], str) and evidence_by_field[field].strip()
                 for field in ("run_id", "artifact_id", "source_url")), "evidence_identity_invalid")
    source_url = urlsplit(evidence_by_field["source_url"])
    _require(source_url.scheme == "https" and source_url.hostname == "idfprapps.illinois.gov"
             and source_url.username is None and source_url.password is None and not source_url.fragment
             and source_url.path.lower() == "/applications/professionprofile/profiledetails.aspx", "evidence_url_invalid")
    _require(type(evidence_by_field["row_number"]) is int and evidence_by_field["row_number"] > 0, "evidence_row_invalid")
    _require(evidence_by_field["content_sha256"] == hashlib.sha256(html.encode()).hexdigest(), "evidence_content_mismatch")
    if isinstance(evidence_by_field["downloaded_at"], datetime):
        evidence_by_field["downloaded_at"] = evidence_by_field["downloaded_at"].isoformat()
    try:
        observed = datetime.fromisoformat(evidence_by_field["downloaded_at"])
        _require(observed.utcoffset() is not None, "evidence_timestamp_invalid")
    except (TypeError, ValueError):
        raise ValueError("illinois_profile_evidence_timestamp_invalid") from None
    return evidence_by_field


def _retained_record(html, profile, evidence):
    identity = profile["identity"]
    record_key = SOURCE_KEY + ":profile:" + _hash([identity["display_name"], identity["original_issue_date"]])
    return {"record_id": _hash([evidence["run_id"], record_key]), "run_id": evidence["run_id"],
            "artifact_id": evidence["artifact_id"], "source_key": SOURCE_KEY, "source_record_key": record_key,
            "profession_code": None, "license_id": None, "license_number": None,
            "raw_payload": {"html": html, "profile": copy.deepcopy(profile)},
            "normalized_payload": {"schema_version": SCHEMA_VERSION, "visibility": "public",
                                   "profile_identity": copy.deepcopy(identity), "quality_flags": []},
            "matched_npi": None, "match_status": "unmatched", "row_number": evidence["row_number"],
            "match_evidence": {"jurisdiction": "IL", "reason": "license_bridge_required",
                               "profile_identity": copy.deepcopy(identity)}}


def _reported_year(composite, category, observed_year):
    field = "graduation_year" if category == "education" else "completion_year"
    matched = re.search(r",\s*([0-9]{4})\s*$", composite)
    if matched is None:
        return {}, [field + "_unresolved"]
    year = int(matched[1])
    if year < 1800:
        return {}, [field + "_before_1800"]
    return {field: year}, [field + "_in_future"] if year > observed_year else []


def _education_fact(entry, category, source_record, evidence):
    raw_fields = entry["fields"]
    composite = _text(raw_fields["SchoolLocation"])
    value_by_field, flags = _reported_year(composite, category, datetime.fromisoformat(evidence["downloaded_at"]).year)
    flags.append("institution_boundary_unresolved")
    if composite:
        value_by_field["reported_school_location"] = composite
    if category == "training":
        value_by_field.update({field: _text(raw_fields[raw_name]) for field, raw_name in
                               (("program_type", "ProgramType"), ("program", "Specialty")) if _text(raw_fields[raw_name])})
    fact_type = "education_history" if category == "education" else "postgraduate_training"
    logical_key = _hash([source_record["source_record_key"], category, entry["source_path"], value_by_field])
    return {"fact_id": _hash([source_record["record_id"], logical_key]), "run_id": source_record["run_id"],
            "npi": None, "source_record_id": source_record["record_id"], "logical_fact_key": logical_key,
            "category": category, "fact_type": fact_type,
            "display": " — ".join(_text(raw_fields[field]) for field in raw_fields if _text(raw_fields[field])),
            "value_json": value_by_field, "availability": "available",
            "assertion_type": "self_reported", "verification_status": "not_independently_verified",
            "effective_start": None, "effective_end": None, "sensitive": False, "public_default": True, "published_at": None,
            "source_json": {**copy.deepcopy(evidence), "source_key": SOURCE_KEY, "schema_version": SCHEMA_VERSION,
                "agency": "Illinois Department of Financial and Professional Regulation", "jurisdiction": "IL",
                "source_record_id": source_record["record_id"], "source_path": entry["source_path"],
                "raw_fields": copy.deepcopy(raw_fields), "field_ids": copy.deepcopy(entry["field_ids"]),
                "quality_flags": flags}}


def parse_profile(html, *, evidence):
    """Retain original visible assertions; external roster matching owns identity.

    The generic profile URL is session-selected and supplies neither license nor
    NPI. SchoolLocation is a single source composite, not a canonical institution.
    A final comma and four-digit year retain the guide's reported year semantics;
    no degree completion, student status, attendance dates or experience is inferred.
    """
    profile = extract_profile(html)
    evidence_by_field = _validated_evidence(evidence, html)
    source_record = _retained_record(html, profile, evidence_by_field)
    notices = [section[field] for section in profile["section_metadata"].values()
               for field in ("HeadingNote", "SectionPublicDisclaimer") if _text(section[field])]
    if notices:
        source_record["normalized_payload"].update(visibility="held_source_notice", source_notices=notices)
        return source_record, []
    facts = [_education_fact(entry, category, source_record, evidence_by_field)
             for category in ("education", "training") for entry in profile[category]]
    source_record["normalized_payload"]["quality_flags"] = sorted({flag for fact in facts
                                                for flag in fact["source_json"]["quality_flags"]})
    return source_record, facts
