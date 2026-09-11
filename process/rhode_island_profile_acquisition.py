# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retain and validate one public RI license lookup; no cohort or publication work."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import re
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path

import aiohttp

from process.massachusetts_profile_acquisition import write_new_json
from process.rhode_island_profile_rows import MAX_JSON_BYTES, PROFILE_FIELDS, SOURCE_KEY, parse_profile

PAGE_URL = "https://datahealth.ri.gov/find/providers/results.php"
RECORD_URL = "https://datahealth.ri.gov/find/providers/loadRecord.php"
MAX_RESPONSE_BYTES = MAX_JSON_BYTES
REQUEST_TIMEOUT_SECONDS = 30
RESPONSE_SCHEMA = "ri-doh-public-response/v1"
# Both retained MD/DO pages carry this identical, non-personal inline schema script.
SCHEMA_SCRIPT_SHA256 = "31a33ca658078f931eec02440df15a5f05474de5d7e097418697b0e36432ef8e"
_INERT_TAGS = {
    "template",
    "noscript",
    "textarea",
    "title",
    "style",
    "xmp",
    "iframe",
    "noembed",
    "noframes",
    "plaintext",
}


def _require(condition, reason):
    if not condition:
        raise ValueError("rhode_island_profile_" + reason)


class _InlineScripts(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=False)
        self.is_script = False
        self.scripts = []
        self.fragments = []
        self.attributes = ()
        self.inert_tags = []
        self.has_invalid_context = False

    def handle_starttag(self, tag, attrs):
        """Keep script attributes so inactive and external scripts cannot be authority."""
        if tag in _INERT_TAGS:
            self.inert_tags.append(tag)
        if tag == "script":
            self.is_script = True
            self.attributes = tuple(attrs)
            if self.inert_tags:
                self.attributes += (("inactive", None),)
            self.fragments = []

    def handle_endtag(self, tag):
        """Preserve each script boundary for duplicate and exact-body checks."""
        if tag in _INERT_TAGS:
            if not self.inert_tags or self.inert_tags[-1] != tag or tag == "plaintext":
                self.has_invalid_context = True
            else:
                self.inert_tags.pop()
        if tag == "script" and self.is_script:
            self.scripts.append((self.attributes, "".join(self.fragments)))
            self.is_script = False

    def handle_startendtag(self, tag, attrs):
        """Reject self-closing script and inert tags, which HTML does not actually close."""
        if tag in _INERT_TAGS or tag == "script":
            self.has_invalid_context = True
        else:
            super().handle_startendtag(tag, attrs)

    def handle_data(self, text):
        """Collect source text without evaluating it."""
        if self.is_script:
            self.fragments.append(text)


def verify_page(body, license_number):
    """Recognize the observed inline mapping and literal request, without running JS.

    The entire reviewed schema script must match, including its request function;
    inserted transformations, comments and unsupported formatting fail closed.
    The separate literal call must also be an active script. This is not a JS interpreter.
    """
    parser = _InlineScripts()
    parser.feed(body.decode("utf-8"))
    _require(not parser.has_invalid_context and not parser.inert_tags, "page_context_invalid")
    relevant_scripts = [
        (attrs, script)
        for attrs, script in parser.scripts
        if re.search(r"\b(?:Record|loaddata|thisRecord|xmlhttp)\b", script)
    ]
    _require(len(relevant_scripts) == 2, "page_scripts_ambiguous")
    schema_scripts = [
        script
        for attrs, script in relevant_scripts
        if attrs == (("type", "text/javascript"),)
        and hashlib.sha256(script.encode("utf-8")).hexdigest() == SCHEMA_SCRIPT_SHA256
    ]
    calls = [
        script for attrs, script in relevant_scripts if not attrs and script.strip() == f"loaddata('{license_number}');"
    ]
    _require(len(schema_scripts) == 1 and len(calls) == 1, "page_script_shape_changed")
    scripts = schema_scripts[0]
    constructors = re.findall(r"\bfunction\s+Record\s*\(([^()]*)\)\s*\{([^{}]*)\}", scripts)
    _require(
        len(constructors) == 1 and len(re.findall(r"\bfunction\s+Record\b", scripts)) == 1, "page_constructor_ambiguous"
    )
    fields, assignments = constructors[0]
    _require(tuple(field.strip() for field in fields.split(",")) == PROFILE_FIELDS, "page_fields_changed")
    expected_assignments = "".join(f"this.{field}={field};" for field in PROFILE_FIELDS)
    _require(re.sub(r"\s+", "", assignments) == expected_assignments, "page_assignments_changed")
    calls = re.findall(r"\bnew\s+Record\s*\(([^()]*)\)", scripts)
    _require(len(calls) == 1 and len(re.findall(r"\bnew\s+Record\b", scripts)) == 1, "page_record_call_ambiguous")
    expected_arguments = ",".join(f"thisRecord[{index}]" for index in range(len(PROFILE_FIELDS)))
    _require(re.sub(r"\s+", "", calls[0]) == expected_arguments, "page_indices_changed")
    compact = re.sub(r"\s+", "", scripts)
    _require(len(re.findall(r"\bRecord\s*=", scripts)) == 0, "page_constructor_reassigned")
    _require(
        len(re.findall(r"\bloaddata\s*\(", scripts)) == 1
        and len(re.findall(r"\bfunction\s+loaddata\s*\(str\)", scripts)) == 1
        and not re.search(r"\bloaddata\s*=", scripts),
        "page_license_call_ambiguous",
    )
    _require(
        compact.count("varres=xmlhttp.responseText;varthisRecord=JSON.parse(res);") == 1
        and len(re.findall(r"\bthisRecord\s*=", scripts)) == 1,
        "page_json_binding_changed",
    )
    _require(
        len(re.findall(r"\bxmlhttp\s*\.\s*open\s*\(", scripts)) == 1
        and compact.count('xmlhttp.open("GET","loadRecord.php?id="+str,true);') == 1,
        "page_record_request_changed",
    )
    return RECORD_URL + "?id=" + license_number


def _error_code(exception):
    if isinstance(exception, ValueError) and str(exception).startswith("rhode_island_profile_"):
        return str(exception)
    return type(exception).__name__


async def _capture_response(session, url, path):
    body = bytearray()
    receipt_by_field = {
        "schema_version": RESPONSE_SCHEMA,
        "source_key": SOURCE_KEY,
        "source_url": url,
        "method": "GET",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "status": None,
        "headers": {},
        "eof": False,
        "received_bytes": 0,
        "error": None,
    }
    try:
        async with session.get(url, allow_redirects=False) as response:
            receipt_by_field.update(
                status=response.status,
                response_url=str(response.url),
                headers={
                    name: list(response.headers.getall(name, []))
                    for name in (
                        "Content-Type",
                        "Content-Encoding",
                        "Content-Length",
                        "Content-Disposition",
                        "Location",
                    )
                },
            )
            async for chunk in response.content.iter_chunked(64 * 1024):
                receipt_by_field["received_bytes"] += len(chunk)
                body.extend(chunk[: MAX_RESPONSE_BYTES - len(body)])
                _require(receipt_by_field["received_bytes"] <= MAX_RESPONSE_BYTES, "response_too_large")
            receipt_by_field["eof"] = True
    except (Exception, asyncio.CancelledError) as exception:
        receipt_by_field["error"] = _error_code(exception)
        raise
    finally:
        receipt_by_field.update(
            downloaded_at=datetime.now(timezone.utc).isoformat(),
            retained_bytes=len(body),
            content_sha256=hashlib.sha256(body).hexdigest(),
            body_base64=base64.b64encode(body).decode("ascii"),
            truncated=receipt_by_field["received_bytes"] > len(body),
        )
        write_new_json(path, receipt_by_field)
    return bytes(body), receipt_by_field


def _validate_response(body, receipt):
    _require(receipt["status"] == 200, "http_status_invalid")
    _require(receipt["response_url"] == receipt["source_url"], "response_url_changed")
    headers = receipt["headers"]
    content_types = headers["Content-Type"]
    _require(
        len(content_types) == 1 and re.fullmatch(r"text/html\s*;\s*charset=UTF-8", content_types[0], re.I),
        "content_type_invalid",
    )
    _require(headers["Content-Encoding"] in ([], ["identity"]), "content_encoding_invalid")
    lengths = headers["Content-Length"]
    _require(
        not lengths
        or len(lengths) == 1
        and lengths[0].isascii()
        and lengths[0].isdigit()
        and int(lengths[0]) == len(body),
        "content_length_invalid",
    )
    _require(bool(body), "empty_response")
    try:
        body.decode("utf-8")
    except UnicodeDecodeError:
        raise ValueError("rhode_island_profile_invalid_utf8") from None


async def _acquire_pair(session, license_number, destination, run_id):
    page_body, page_receipt = await _capture_response(
        session,
        PAGE_URL + "?license=" + license_number,
        destination / "page.json",
    )
    _validate_response(page_body, page_receipt)
    record_url = verify_page(page_body, license_number)
    record_body, record_receipt = await _capture_response(session, record_url, destination / "record.json")
    _validate_response(record_body, record_receipt)
    page_evidence_by_field = {
        "artifact_file": "page.json",
        **{field: page_receipt[field] for field in ("source_url", "downloaded_at", "content_sha256")},
    }
    evidence_by_field = {
        "run_id": run_id,
        "artifact_id": run_id + ":record",
        "row_number": 1,
        "schema_page": page_evidence_by_field,
        **{field: record_receipt[field] for field in ("source_url", "downloaded_at", "content_sha256")},
    }
    source_record, facts = parse_profile(record_body, license_number=license_number, evidence=evidence_by_field)
    return {
        "status": "success",
        "run_id": run_id,
        "source_key": SOURCE_KEY,
        "license_number": license_number,
        "schema_page": page_evidence_by_field,
        "record_artifact_file": "record.json",
        "source_record": source_record,
        "facts": facts,
    }


async def acquire_profile(license_number, destination: Path, *, run_id):
    """Retain at most two GETs, each capped at 1 MiB and 30 seconds, without retries.

    The caller supplies an unused local directory. Every attempted response is
    retained losslessly in base64 (a bounded prefix on overflow), including errors.
    Failed attempts have no parsed success. This does not bind an NPI or publish.
    """
    _require(
        isinstance(license_number, str) and re.fullmatch(r"(?:MD|DO)[0-9]{5}", license_number),
        "invalid_requested_license",
    )
    _require(isinstance(run_id, str) and bool(run_id.strip()), "invalid_run_id")
    destination = Path(destination)
    _require(not any(path.is_symlink() for path in (destination, *destination.parents)), "artifact_symlink")
    destination.mkdir(exist_ok=False)
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS, ceil_threshold=31),
            trust_env=False,
            cookie_jar=aiohttp.DummyCookieJar(),
            auto_decompress=False,
            headers={"Accept-Encoding": "identity"},
        ) as session:
            # aiohttp otherwise repeats idempotent requests after connection failures.
            session._retry_connection = False
            acquired = await _acquire_pair(session, license_number, destination, run_id)
    except (Exception, asyncio.CancelledError) as exception:
        write_new_json(
            destination / "result.json",
            {
                "status": "failed",
                "run_id": run_id,
                "source_key": SOURCE_KEY,
                "license_number": license_number,
                "error": _error_code(exception),
            },
        )
        raise
    write_new_json(destination / "result.json", acquired)
    return acquired
