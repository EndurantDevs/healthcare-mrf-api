# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Acquire both regular Tennessee physician reports through ordinary public forms."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path

import aiohttp

from process.kentucky_profile_acquisition import _reject_symlinks
from process.massachusetts_profile_acquisition import encoded_json

BASE_URL = "https://internet.health.tn.gov/LicensureReports"
PROFESSIONS_URL = BASE_URL + "/Home/FetchProfessionsByBoard"
REPORT_URL = BASE_URL + "/Home/CreateFile"
ACQUISITION_SCHEMA = "tn-regular-physician-report-acquisition/v1"
MAX_REPORT_BYTES = 256 * 1024 * 1024
MAX_FORM_BYTES = 2 * 1024 * 1024
RESPONSE_SECONDS = 180
RUN_SECONDS = 480
REPORTS = (("md", "16", "1606", "Medical Doctor"), ("do", "19", "1907", "Osteopathic Physician"))
OPTIONAL_FIELDS = ("PersonFlag", "EduFlag", "PracticeFlag", "SAQFlag")
SELECT_FIELDS = {"Board.BoardCode", "Profession.ProfessionCode", "Rank.RankName", "State.StateCode",
                 "County.CountyCode", "Status.Status"}


def _require(condition, reason):
    if not condition:
        raise ValueError("tennessee_acquisition_" + reason)


class ReportForms(HTMLParser):
    """Read only form controls; neither scripts nor linked resources are executed."""

    def __init__(self, body):
        super().__init__()
        self.forms, self.current, self.select, self.option = [], None, None, None
        self.feed(body.decode("utf-8-sig"))
        self.close()
        _require(self.current is None and self.select is None and self.option is None, "form_incomplete")

    def handle_starttag(self, tag, attrs):
        """Retain controls with their containing form and reject ambiguous fields."""
        attributes_by_name = dict(attrs)
        _require(len(attributes_by_name) == len(attrs), "form_attributes_ambiguous")
        if tag == "form":
            _require(self.current is None, "form_nested")
            self.current = {"action": attributes_by_name.get("action"), "method": attributes_by_name.get("method"),
                            "hidden": [], "checkboxes": {}, "options": {}, "buttons": [], "disabled_selects": []}
            self.forms.append(self.current)
        elif self.current is not None:
            self._control(tag, attributes_by_name)

    def _control(self, tag, fields):
        name = fields.get("name")
        if tag == "select":
            _require(self.select is None and name not in self.current["options"] and name in SELECT_FIELDS,
                     "form_select_changed")
            self.select = name
            self.current["options"][name] = []
            if "disabled" in fields:
                self.current["disabled_selects"].append(name)
            return
        if tag == "option" and self.select is not None:
            _require(self.option is None, "form_option_changed")
            self.option = {"value": fields.get("value"), "selected": "selected" in fields, "text": ""}
            return
        if tag == "input":
            _require("disabled" not in fields, "form_input_disabled")
            if fields.get("type") == "hidden":
                self.current["hidden"].append((name, fields.get("value", "")))
            elif fields.get("type") == "checkbox":
                _require(name not in self.current["checkboxes"], "form_checkbox_duplicated")
                self.current["checkboxes"][name] = (fields.get("value"), "checked" in fields)
            else:
                _require(False, "form_input_changed")
        if tag == "button":
            self.current["buttons"].append(fields)

    def handle_data(self, text):
        """Preserve the displayed option label for contract validation."""
        if self.option is not None:
            self.option["text"] += text

    def handle_endtag(self, tag):
        """Close form controls without accepting overlapping selects or forms."""
        if tag == "option" and self.option is not None:
            self.option["text"] = " ".join(self.option["text"].split())
            self.current["options"][self.select].append(self.option)
            self.option = None
        elif tag == "select":
            _require(self.option is None, "form_option_incomplete")
            self.select = None
        elif tag == "form":
            _require(self.select is None, "form_select_incomplete")
            self.current = None


def _validated_form(body, board, *, submitted=False):
    forms = ReportForms(body).forms
    _require([(form["action"], form["method"]) for form in forms] == [
        ("/LicensureReports", "post"), ("/LicensureReports/Home/CreateFile", "get")], "form_action_changed")
    form, download = forms
    _require(set(form["disabled_selects"]) == {"Profession.ProfessionCode", "Rank.RankName", "County.CountyCode"},
             "form_disabled_filters_changed")
    _require(not download["options"] and not download["checkboxes"], "download_filters_changed")
    buttons = form["buttons"]
    _require(len(buttons) == 1 and buttons[0].get("id") == "submit-button"
             and buttons[0].get("type") == "submit"
             and not {"form", "formaction", "formmethod", "disabled", "name"}.intersection(buttons[0]), "submit_button_changed")
    hidden = form["hidden"]
    _require(len(hidden) == 5 and dict(hidden).keys() == {"__RequestVerificationToken", *OPTIONAL_FIELDS}
             and bool(dict(hidden)["__RequestVerificationToken"])
             and all(dict(hidden)[name] == "false" for name in OPTIONAL_FIELDS), "form_hidden_changed")
    _require(form["checkboxes"] == {name: ("true", submitted and name != "PersonFlag") for name in OPTIONAL_FIELDS},
             "form_checkbox_changed")
    options = form["options"]
    _require(options.keys() == SELECT_FIELDS, "form_filters_changed")
    locations = [option for option in options["State.StateCode"] if option["value"] == "100"]
    _require(len(locations) == 1 and locations[0]["text"] == "All Locations", "all_locations_changed")
    _require(sum(option["value"] == board for option in options["Board.BoardCode"]) == 1, "board_changed")
    _require(options["Status.Status"] == [] and options["Rank.RankName"] == [
        {"value": "", "selected": True, "text": "Default to all Ranks or select..."}], "unrestricted_filters_changed")
    for name, label in (("Profession.ProfessionCode", "Professions"), ("County.CountyCode", "Counties")):
        _require(options[name] == [{"value": None, "selected": True, "text": f"Default to all {label} or select..."}],
                 "default_filter_changed")
    if submitted:
        for name, selected_value in (("Board.BoardCode", board), ("State.StateCode", "100")):
            _require([option["value"] for option in options[name] if option["selected"] and option["value"]] == [selected_value],
                     "submitted_filter_changed")
        buttons = download["buttons"]
        _require(download["hidden"] == [("hasFile", "True")] and len(buttons) == 1
                 and buttons[0].get("id") == "hidden-button" and buttons[0].get("type") == "submit"
                 and "hidden" in buttons[0]
                 and not {"form", "formaction", "formmethod", "disabled", "name"}.intersection(buttons[0]), "download_trigger_changed")
    else:
        _require(not download["hidden"] and not download["buttons"], "unexpected_download_trigger")
    return hidden


def _unique_object(pairs):
    _require(len(dict(pairs)) == len(pairs), "professions_ambiguous")
    return dict(pairs)


def _validated_professions(body, profession, label):
    professions = json.loads(body, object_pairs_hook=_unique_object)
    _require(isinstance(professions, list) and all(
        isinstance(entry, dict) and entry.keys() == {"professionCode", "professionName"} and type(entry.get("professionCode")) is int
        and isinstance(entry.get("professionName"), str) for entry in professions), "professions_invalid")
    codes = [entry["professionCode"] for entry in professions]
    _require(len(set(codes)) == len(codes) and [entry["professionName"] for entry in professions
             if str(entry["professionCode"]) == profession] == [label], "profession_changed")


def _new_file(path):
    _reject_symlinks(path)
    return os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600), "wb")


def _response_headers(response, content_type, limit):
    headers = response.headers
    for name in ("Content-Type", "Content-Length", "Content-Encoding", "Transfer-Encoding"):
        _require(len(headers.getall(name, [])) <= 1, "headers_ambiguous")
    _require(headers.get("Content-Type", "").split(";", 1)[0].strip().lower() == content_type, "content_type_invalid")
    _require(headers.get("Content-Encoding", "identity").strip().lower() == "identity", "content_encoding_invalid")
    length = headers.get("Content-Length")
    transfer = headers.get("Transfer-Encoding")
    _require(transfer is None or (transfer.lower() == "chunked" and length is None), "transfer_encoding_invalid")
    if length is not None:
        _require(re.fullmatch(r"[0-9]{1,20}", length) is not None, "content_length_invalid")
        _require(int(length) <= limit, "response_too_large")
    return int(length) if length is not None else None


async def _stream_response(response, output, receipt_by_field, progress, completed, limit, expected_length):
    digest = hashlib.sha256()
    try:
        while True:
            await progress(completed, 8)
            chunk = await response.content.read(64 * 1024)
            if not chunk:
                _require(response.content.at_eof(), "partial_eof")
                break
            retained = chunk[:limit - receipt_by_field["content_bytes"]]
            output.write(retained)
            digest.update(retained)
            receipt_by_field["content_bytes"] += len(retained)
            _require(len(retained) == len(chunk), "response_too_large")
        _require(receipt_by_field["content_bytes"] > 0, "response_empty")
        _require(expected_length is None or receipt_by_field["content_bytes"] == expected_length, "content_length_mismatch")
        await progress(completed, 8)
        receipt_by_field.update(eof=True, content_length_verified=expected_length is not None, complete=True)
    finally:
        receipt_by_field["content_sha256"] = digest.hexdigest()


async def _fetch_response(session, directory, stage, route, progress, completed, fields=None):
    method, source_url, content_type = route
    _require((method, source_url) in {("GET", BASE_URL), ("POST", BASE_URL), ("POST", PROFESSIONS_URL),
                                    ("GET", REPORT_URL)}, "route_invalid")
    limit = MAX_REPORT_BYTES if content_type == "text/csv" else MAX_FORM_BYTES
    suffix = {"text/html": "html", "application/json": "json", "text/csv": "csv"}[content_type]
    path = directory / f"{stage}.{suffix}"
    receipt_by_field = {"stage": stage, "method": method, "source_url": source_url, "filepath": str(path),
               "downloaded_at": datetime.now(timezone.utc).isoformat(), "status": None, "headers": {},
               "content_bytes": 0, "content_sha256": hashlib.sha256(b"").hexdigest(),
               "eof": False, "content_length_verified": False, "complete": False}
    await progress(completed, 8)
    async with asyncio.timeout(RESPONSE_SECONDS):
        with _new_file(path) as output, _new_file(directory / f"{stage}.receipt.json") as retained_receipt:
            try:
                async with session.request(method, source_url, data=fields, allow_redirects=False) as response:
                    receipt_by_field.update(status=response.status, headers={name: response.headers.getall(name, []) for name in (
                        "Content-Type", "Content-Length", "Content-Encoding", "Transfer-Encoding")})
                    _require(str(response.url) == source_url, "response_url_changed")
                    _require(response.status == 200, f"http_failure:{response.status}")
                    expected_length = _response_headers(response, content_type, limit)
                    await _stream_response(response, output, receipt_by_field, progress, completed, limit, expected_length)
            except BaseException as exc:
                receipt_by_field.update(complete=False, error_type=type(exc).__name__)
                raise
            finally:
                output.flush()
                os.fsync(output.fileno())
                retained_receipt.write(encoded_json(receipt_by_field))
                retained_receipt.flush()
                os.fsync(retained_receipt.fileno())
    return receipt_by_field


async def _acquire_board(directory, progress, specification, responses):
    label, board, profession, profession_label = specification
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=RESPONSE_SECONDS), cookie_jar=aiohttp.CookieJar(),
                                     trust_env=False, auto_decompress=False,
                                     headers={"User-Agent": "Mozilla/5.0", "Referer": BASE_URL,
                                              "Accept-Encoding": "identity"}) as session:
        # aiohttp otherwise repeats idempotent requests after connection failures.
        _require(type(getattr(session, "_retry_connection", None)) is bool, "retry_control_unavailable")
        session._retry_connection = False
        form = await _fetch_response(session, directory, label + "-form", ("GET", BASE_URL, "text/html"),
                                     progress, len(responses))
        responses.append(form)
        hidden = _validated_form(Path(form["filepath"]).read_bytes(), board)
        professions = await _fetch_response(session, directory, label + "-professions",
                                            ("POST", PROFESSIONS_URL, "application/json"), progress, len(responses),
                                            [("id", board)])
        responses.append(professions)
        _validated_professions(Path(professions["filepath"]).read_bytes(), profession, profession_label)
        filters = [("Board.BoardCode", board), ("Profession.ProfessionCode", profession), ("Rank.RankName", ""),
                   ("State.StateCode", "100"), ("EduFlag", "true"), ("SAQFlag", "true"), ("PracticeFlag", "true")]
        submitted = await _fetch_response(session, directory, label + "-submitted", ("POST", BASE_URL, "text/html"),
                                          progress, len(responses), filters + hidden)
        responses.append(submitted)
        _validated_form(Path(submitted["filepath"]).read_bytes(), board, submitted=True)
        report = await _fetch_response(session, directory, label + "-report", ("GET", REPORT_URL, "text/csv"),
                                       progress, len(responses))
        responses.append(report)
    return {**report, "profession_code": profession, "profession_label": profession_label,
            "filters": [[name, value] for name, value in filters + hidden if name != "__RequestVerificationToken"]}


async def acquire_reports(directory: Path, progress) -> dict:
    """Return both report paths and eight HTTP receipts only after complete acquisition.

    The caller exclusively owns the existing directory. ``progress(completed, 8)``
    runs before requests and around body reads and may raise to cancel. Report
    entries are keyed by profession code and include exact byte count, SHA-256,
    UTC download time, source URL and token-free posted filters. Raw form files
    contain session tokens and must remain private. Failed files are retained;
    only ``acquisition.json`` records completion of the two-report unit.
    """
    directory = directory.absolute()
    _reject_symlinks(directory)
    _require(directory.is_dir(), "directory_invalid")
    for name in ["acquisition.json", *(f"{label}-{stage}.{suffix}" for label, *_ in REPORTS
                 for stage, extension in (("form", "html"), ("professions", "json"), ("submitted", "html"), ("report", "csv"))
                 for suffix in (extension, "receipt.json"))]:
        path = directory / name
        _reject_symlinks(path)
        if path.exists():
            raise FileExistsError(path)
    responses, reports = [], {}
    async with asyncio.timeout(RUN_SECONDS):
        for specification in REPORTS:
            reports[specification[2]] = await _acquire_board(directory, progress, specification, responses)
        await progress(8, 8)
        manifest_by_field = {"schema_version": ACQUISITION_SCHEMA, "complete": True, "responses": responses, "reports": reports}
        with _new_file(directory / "acquisition.json") as output:
            output.write(encoded_json(manifest_by_field))
            output.flush()
            os.fsync(output.fileno())
    return manifest_by_field
