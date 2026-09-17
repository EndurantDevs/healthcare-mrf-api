# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Capture both published active physician rosters, retaining their preview counts."""

from __future__ import annotations

import asyncio
import base64
import csv
import hashlib
import io
import re
from datetime import datetime, timezone

import aiohttp

from process.massachusetts_profile_acquisition import write_new_json
from process.rhode_island_profile_acquisition import _InlineScripts
from process.rhode_island_profile_roster import (
    MAX_ROSTER_BYTES,
    MAX_ROSTER_ROWS,
    ROSTER_COLUMNS,
    SOURCE_URL,
    parse_roster,
)
from process.rhode_island_profile_rows import LICENSE_TYPES

PREVIEW_URL = "https://datahealth.ri.gov/lists/licensees/getList.php"
COVERAGE_SCOPE = "active_md_do_excluding_limited_volunteer"
MAX_PREVIEW_BYTES = 8 * 1024 * 1024
REQUEST_SECONDS = 30


def _require(condition, reason):
    if not condition:
        raise ValueError("rhode_island_cohort_" + reason)


def parse_preview(body, license_type):
    """Return the unchanged form field and independent source-reported preview_row count."""
    _require(0 < len(body) <= MAX_PREVIEW_BYTES, "preview_size_invalid")
    parser = _InlineScripts()
    parser.feed(body.decode("utf-8"))
    parser.close()
    _require(
        not parser.has_invalid_context and not parser.inert_tags and not parser.is_script,
        "preview_script_context_invalid",
    )
    scripts = parser.scripts
    _require(len(scripts) == 1 and not scripts[0][0], "preview_scripts_changed")
    match = re.fullmatch(
        r"\s*var outputQry = `([^`]+)`;\s*var numbRows = '([0-9,]+)';\s*"
        r"var info = '<p>Excerpt from the database\. Click on the Download button to access the full report\.</p>';\s*",
        scripts[0][1],
    )
    _require(match is not None, "preview_contract_changed")
    literal, count_text = match.groups()
    _require("${" not in literal and "\\" not in literal and "\x00" not in literal, "preview_expression_forbidden")
    expected_rows = int(count_text.replace(",", ""))
    _require(format(expected_rows, ",") == count_text and 0 < expected_rows <= MAX_ROSTER_ROWS, "preview_count_invalid")
    preview_rows = list(csv.reader(io.StringIO(literal, newline=""), strict=True))
    preview_columns = [column.replace("License Address", "Address") for column in ROSTER_COLUMNS]
    _require(
        preview_rows and preview_rows[0] == preview_columns and 0 < len(preview_rows) - 1 <= 101,
        "preview_schema_changed",
    )
    _require(len(preview_rows) - 1 == min(expected_rows, 101), "preview_count_mismatch")
    _require(
        all(
            len(preview_row) == len(ROSTER_COLUMNS) and preview_row[6] == "Physician" and preview_row[7] == license_type
            for preview_row in preview_rows[1:]
        ),
        "preview_scope_changed",
    )
    return literal, expected_rows, preview_rows[1:]


async def _post(session, url, fields, path, limit, progress):
    """Retain every bounded attempt, including incomplete response bytes on failure."""
    body = bytearray()
    receipt_by_field = {
        "source_url": url,
        "method": "POST",
        "request_fields": fields,
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
        "complete": False,
    }
    try:
        await progress()
        async with session.post(url, data=fields, allow_redirects=False) as response:
            receipt_by_field.update(
                status=response.status,
                response_url=str(response.url),
                headers={
                    key: response.headers.getall(key, [])
                    for key in ("Content-Type", "Content-Encoding", "Content-Length", "Content-Disposition")
                },
            )
            async for chunk in response.content.iter_chunked(64 * 1024):
                body.extend(chunk[: max(0, limit + 1 - len(body))])
                _require(len(body) <= limit, "response_too_large")
                await progress()
            receipt_by_field["complete"] = True
        _require(
            receipt_by_field["status"] == 200 and receipt_by_field["response_url"] == url, "response_status_invalid"
        )
        headers = receipt_by_field["headers"]
        _require(headers["Content-Encoding"] in ([], ["identity"]), "content_encoding_invalid")
        content_type = headers["Content-Type"]
        expected_type = "text/html" if url == PREVIEW_URL else "application/octet-stream"
        _require(
            len(content_type) == 1 and content_type[0].split(";", 1)[0].strip().lower() == expected_type,
            "content_type_invalid",
        )
        lengths = headers["Content-Length"]
        _require(
            not lengths or len(lengths) == 1 and lengths[0].isdigit() and int(lengths[0]) == len(body),
            "content_length_invalid",
        )
        _require(bool(body), "empty_response")
        return bytes(body), receipt_by_field
    except (Exception, asyncio.CancelledError) as error:
        receipt_by_field["error"] = type(error).__name__
        raise
    finally:
        receipt_by_field.update(
            content_sha256=hashlib.sha256(body).hexdigest(),
            content_bytes=len(body),
            body_base64=base64.b64encode(body).decode("ascii"),
        )
        write_new_json(path, receipt_by_field)


async def acquire_rosters(directory, *, run_id, artifact_id, progress):
    """Acquire the exact full MD/DO pair; a preview is never accepted as the cohort."""
    roots, descriptors = [], {}
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=REQUEST_SECONDS, ceil_threshold=31),
        trust_env=False,
        cookie_jar=aiohttp.DummyCookieJar(),
        auto_decompress=False,
        headers={"Accept-Encoding": "identity"},
    ) as session:
        session._retry_connection = False
        for prefix, license_type in LICENSE_TYPES.items():
            preview_body, preview = await _post(
                session,
                PREVIEW_URL,
                {"jxProfession": "Physician", "jxLicense": license_type},
                directory / f"{prefix}-preview.json",
                MAX_PREVIEW_BYTES,
                progress,
            )
            literal, expected_rows, preview_rows = parse_preview(preview_body, license_type)
            content, download = await _post(
                session,
                SOURCE_URL,
                {"output": literal, "prof": "Physician", "licType": license_type},
                directory / f"{prefix}-download.json",
                MAX_ROSTER_BYTES,
                progress,
            )
            evidence_by_field = {
                "run_id": run_id,
                "artifact_id": artifact_id,
                **{key: download[key] for key in ("source_url", "downloaded_at", "content_sha256")},
            }
            roster = parse_roster(
                content, license_type=license_type, expected_rows=expected_rows, evidence=evidence_by_field
            )
            acquired_rows = list(csv.reader(io.StringIO(content.decode("utf-8"), newline=""), strict=True))[1:]
            _require(acquired_rows[: len(preview_rows)] == preview_rows, "preview_download_changed")
            _require(
                all(
                    original["raw_payload"]["Status"] in ("Active", "Active Restricted", "Active Probation")
                    for root in roster["roots"]
                    for original in root["originals"]
                ),
                "active_status_scope_changed",
            )
            roots.extend(roster["roots"])
            descriptors[prefix] = _roster_descriptor(license_type, expected_rows, roster, preview, download)
    _require(len({root["license_number"] for root in roots}) == len(roots), "cohort_license_duplicate")
    return {"coverage_scope": COVERAGE_SCOPE, "rosters": descriptors, "roots": roots}


def _roster_descriptor(license_type, expected_rows, roster, preview, download):
    return {
        "license_type": license_type,
        "expected_rows": expected_rows,
        "input_rows": roster["input_row_count"],
        "unique_licenses": roster["unique_license_count"],
        "preview": {key: preview[key] for key in ("source_url", "downloaded_at", "content_sha256", "content_bytes")},
        "download": {key: download[key] for key in ("source_url", "downloaded_at", "content_sha256", "content_bytes")},
    }
