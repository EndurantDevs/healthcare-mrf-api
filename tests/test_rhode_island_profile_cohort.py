# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import base64
import csv
import io
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from process import rhode_island_profile_cohort as cohort
from process.rhode_island_profile_roster import ROSTER_COLUMNS
from process.rhode_island_profile_rows import LICENSE_TYPES
from tests.test_rhode_island_profile_acquisition import ProfileResponse


def roster_csv(prefix="MD", *, preview=False, specialty="Family Medicine"):
    fields = dict.fromkeys(ROSTER_COLUMNS, "")
    fields.update(
        {
            "Name": "EXAMPLE ALEX",
            "First": "Alex",
            "Last": "Example",
            "License No": prefix + "00001",
            "Profession": "Physician",
            "License Type": LICENSE_TYPES[prefix],
            "Status": "Active",
            "Specialty": specialty,
        }
    )
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow([column.replace("License Address", "Address") if preview else column for column in ROSTER_COLUMNS])
    writer.writerow(fields.values())
    return buffer.getvalue()


def preview_body(prefix="MD", *, count="1", literal=None):
    literal = roster_csv(prefix, preview=True) if literal is None else literal
    return (
        "<table></table><script>\nvar outputQry = `" + literal + "`;\nvar numbRows = '" + count + "';\n"
        "var info = '<p>Excerpt from the database. Click on the Download button to access the full report.</p>';\n</script>"
    ).encode()


@pytest.mark.parametrize(
    "mutation",
    [
        lambda body: body.replace(b"var numbRows", b"numbRows"),
        lambda body: body.replace(b"Alex", b"${call()}"),
        lambda body: body.replace(b"Alex", b"A\\lex"),
        lambda body: body.replace(b"'1'", b"'01'"),
        lambda body: body.replace(b"'1'", b"'2'"),
        lambda body: body + b"<script>var numbRows = '1';</script>",
        lambda body: body.replace(b"<script>", b"<script src='other'>"),
        lambda body: body.replace(b"Physician,", b"Other,"),
        lambda body: body.replace(b"Name,First", b"Other,First"),
    ],
)
def test_preview_drift_never_becomes_download_scope(mutation):
    with pytest.raises(ValueError):
        cohort.parse_preview(mutation(preview_body()), LICENSE_TYPES["MD"])


def test_preview_keeps_literal_source_form_field():
    literal, count, rows = cohort.parse_preview(preview_body(), LICENSE_TYPES["MD"])
    assert literal == roster_csv(preview=True) and count == 1 and rows[0][5] == "MD00001"


class Session:
    def __init__(self, responses):
        self.responses, self.requests, self.closed = list(responses), [], False

    def post(self, url, *, data, allow_redirects):
        assert allow_redirects is False
        self.requests.append((url, data))
        response = self.responses.pop(0)
        response.url = url
        return response

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.closed = True


def responses():
    return [
        response
        for prefix in LICENSE_TYPES
        for response in (
            ProfileResponse(preview_body(prefix)),
            ProfileResponse(roster_csv(prefix).encode(), headers={"Content-Type": "application/octet-stream"}),
        )
    ]


async def test_complete_pair_uses_exact_preview_literal_and_preserves_each_response(tmp_path, monkeypatch):
    session = Session(responses())
    monkeypatch.setattr(cohort.aiohttp, "ClientSession", lambda **kwargs: session)
    result = await cohort.acquire_rosters(tmp_path, run_id="r", artifact_id="a", progress=AsyncMock())
    assert [root["license_number"] for root in result["roots"]] == ["MD00001", "DO00001"]
    assert result["roots"][0]["originals"][0]["raw_payload"]["Status"] == "Active"
    assert session.closed and session._retry_connection is False and len(session.requests) == 4
    assert session.requests[1][1] == {
        "output": roster_csv(preview=True),
        "prof": "Physician",
        "licType": LICENSE_TYPES["MD"],
    }
    assert len(list(tmp_path.iterdir())) == 4


async def test_download_count_or_prefix_disagreement_is_not_complete(tmp_path, monkeypatch):
    values = responses()
    values[1] = ProfileResponse(
        roster_csv(specialty="Changed").encode(), headers={"Content-Type": "application/octet-stream"}
    )
    session = Session(values)
    monkeypatch.setattr(cohort.aiohttp, "ClientSession", lambda **kwargs: session)
    with pytest.raises(ValueError, match="preview_download_changed"):
        await cohort.acquire_rosters(tmp_path, run_id="r", artifact_id="a", progress=AsyncMock())
    assert session.closed and len(session.requests) == 2


@pytest.mark.parametrize("failure", [asyncio.CancelledError(), TimeoutError(), ValueError("bad")])
async def test_interrupted_response_retains_original_prefix_and_closes(tmp_path, failure):
    import json

    response = ProfileResponse(b"", chunks=[b"retained", failure])
    path = tmp_path / "attempt.json"
    with pytest.raises(type(failure)):
        await cohort._post(Session([response]), cohort.PREVIEW_URL, {}, path, 20, AsyncMock())
    receipt = json.loads(path.read_bytes())
    assert base64.b64decode(receipt["body_base64"]) == b"retained"
    assert receipt["complete"] is False and response.closed


async def test_overflow_is_bounded_and_never_complete(tmp_path):
    import json

    path = tmp_path / "attempt.json"
    with pytest.raises(ValueError, match="response_too_large"):
        await cohort._post(Session([ProfileResponse(b"too much")]), cohort.PREVIEW_URL, {}, path, 3, AsyncMock())
    receipt = json.loads(path.read_bytes())
    assert receipt["content_bytes"] == 4 and not receipt["complete"]


@pytest.mark.parametrize(
    "tag", ["template", "noscript", "textarea", "title", "style", "xmp", "iframe", "noembed", "noframes", "plaintext"]
)
def test_inactive_preview_script_cannot_supply_source_count(tag):
    body = b"<" + tag.encode() + b">" + preview_body() + b"</" + tag.encode() + b">"
    with pytest.raises(ValueError):
        cohort.parse_preview(body, LICENSE_TYPES["MD"])


@pytest.mark.parametrize("prefix", [b"<script/>", b"<template/>", b"<textarea/>"])
def test_self_closing_context_is_not_source_authority(prefix):
    with pytest.raises(ValueError):
        cohort.parse_preview(prefix + preview_body(), LICENSE_TYPES["MD"])
