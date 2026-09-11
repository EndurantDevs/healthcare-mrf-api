# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic transport coverage for the one-license RI acquisition boundary."""

import asyncio
import base64
import copy
import hashlib
import json
from types import SimpleNamespace

import aiohttp
import pytest
from multidict import CIMultiDict

from process import rhode_island_profile_acquisition as acquisition
from process.rhode_island_profile_binding import bind_profile
from process.rhode_island_profile_registry import bind_snapshot_profiles
from process.rhode_island_profile_rows import LICENSE_TYPES, PROFILE_FIELDS
from tests.test_rhode_island_profile_binding import candidate
from tests.test_rhode_island_profile_registry import retained_snapshot, snapshot


def _page(license_number="MD00001"):
    assignments = "".join(f"this.{field} = {field};" for field in PROFILE_FIELDS)
    arguments = ", ".join(f"thisRecord[{index}]" for index in range(43))
    return (
        f'<script type="text/javascript">function Record({", ".join(PROFILE_FIELDS)}) {{{assignments}}}'
        "function loaddata(str) {var res = xmlhttp.responseText; var thisRecord = JSON.parse(res);"
        f"myRecord = new Record({arguments});"
        'xmlhttp.open("GET","loadRecord.php?id="+str,true); xmlhttp.send();}'
        f"</script><script>loaddata('{license_number}');</script>"
    ).encode()


@pytest.fixture(autouse=True)
def synthetic_schema_fingerprint(monkeypatch):
    """Use synthetic script bytes in tests; real retained-page replay checks the pinned source hash."""
    assert acquisition.SCHEMA_SCRIPT_SHA256 == "31a33ca658078f931eec02440df15a5f05474de5d7e097418697b0e36432ef8e"
    schema_script = _page().decode().split(">", 1)[1].split("</script>", 1)[0]
    monkeypatch.setattr(acquisition, "SCHEMA_SCRIPT_SHA256", hashlib.sha256(schema_script.encode()).hexdigest())


def test_schema_allows_unrelated_self_closing_tags():
    page = b'<br/><img alt="Example"/>' + _page() + b"<hr/>"
    assert acquisition.verify_page(page, "MD00001") == acquisition.RECORD_URL + "?id=MD00001"


def _profile(license_number="MD00001"):
    profile_by_field = dict.fromkeys(PROFILE_FIELDS, "")
    profile_by_field.update(
        License_No=license_number,
        Profession_Name="Physician",
        License_ID="synthetic",
        First_Name="Synthetic",
        Last_Name="Example",
        School_Name="Example Medical School",
        Primary_License_Type_Name=LICENSE_TYPES[license_number[:2]],
        School_Grad_Year="2001",
    )
    occurrences = []
    for specialty in ("Family Medicine", "Internal Medicine"):
        profile_by_field["Specialty_Name"] = specialty
        occurrences.extend(profile_by_field[field] for field in PROFILE_FIELDS)
    return json.dumps(occurrences).encode()


class ProfileResponse:
    def __init__(self, body, *, status=200, headers=None, chunks=None, url=None):
        self.status = status
        self.headers = CIMultiDict(headers if headers is not None else {"Content-Type": "text/html; charset=UTF-8"})
        self.chunks = chunks if chunks is not None else [body]
        self.content = SimpleNamespace(iter_chunked=self.iter_body)
        self.url = url
        self.closed = False

    async def iter_body(self, chunk_size):
        assert chunk_size == 64 * 1024
        for chunk in self.chunks:
            if isinstance(chunk, BaseException):
                raise chunk
            yield chunk

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exception):
        self.closed = True
        return False


class ProfileSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []
        self.options = {}
        self.closed = False

    def get(self, url, **options):
        self.requests.append((url, options))
        assert self.responses, "Unexpected additional request"
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        if response.url is None:
            response.url = url
        return response

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exception):
        self.closed = True
        return False


@pytest.fixture
def install_session(monkeypatch):
    def install(*responses):
        session = ProfileSession(responses)

        def factory(**options):
            session.options = options
            return session

        monkeypatch.setattr(acquisition.aiohttp, "ClientSession", factory)
        return session

    return install


def _receipt(destination, filename):
    return json.loads((destination / filename).read_bytes())


@pytest.mark.parametrize("license_number", ["MD00001", "DO00002"])
async def test_retains_source_occurrences(tmp_path, install_session, license_number):
    page, profile = _page(license_number), _profile(license_number)
    session = install_session(ProfileResponse(page), ProfileResponse(profile))
    destination = tmp_path / "new"
    acquired = await acquisition.acquire_profile(license_number, destination, run_id="synthetic-run")
    assert session.requests == [
        (acquisition.PAGE_URL + "?license=" + license_number, {"allow_redirects": False}),
        (acquisition.RECORD_URL + "?id=" + license_number, {"allow_redirects": False}),
    ]
    assert session.closed and session._retry_connection is False
    assert session.options["timeout"].total == 30 and session.options["timeout"].ceil_threshold > 30
    assert session.options["trust_env"] is False and session.options["auto_decompress"] is False
    assert isinstance(session.options["cookie_jar"], aiohttp.DummyCookieJar)
    assert session.options["headers"] == {"Accept-Encoding": "identity"}
    assert acquired == _receipt(destination, "result.json")
    assert acquired["source_record"]["normalized_payload"]["occurrence_count"] == 2
    assert acquired["source_record"]["raw_payload"]["values"] == json.loads(profile)
    assert acquired["source_record"]["matched_npi"] is None
    assert len(acquired["facts"]) == 3
    for fact in acquired["facts"]:
        assert fact["npi"] is None and fact["published_at"] is None
        assert fact["source_json"]["schema_page"]["content_sha256"] == hashlib.sha256(page).hexdigest()
    for filename, body in (("page.json", page), ("record.json", profile)):
        receipt = _receipt(destination, filename)
        assert base64.b64decode(receipt["body_base64"], validate=True) == body
        assert receipt["content_sha256"] == hashlib.sha256(body).hexdigest()
        assert receipt["eof"] and not receipt["truncated"] and receipt["error"] is None


@pytest.mark.parametrize("use_snapshot", [False, True])
async def test_schema_page_survives_binding(tmp_path, install_session, use_snapshot):
    license_number = "MD00001"
    profile = _profile(license_number)
    session = install_session(ProfileResponse(_page()), ProfileResponse(profile))
    acquired = await acquisition.acquire_profile(license_number, tmp_path / "new", run_id="synthetic-run")
    metadata = acquired["facts"][0]["source_json"]
    original_metadata = copy.deepcopy(metadata)
    candidates = [candidate(first_name="Synthetic")]
    if use_snapshot:
        retained = retained_snapshot(tmp_path, snapshot(candidates))
        record, facts = next(bind_snapshot_profiles([(license_number, profile, metadata)], **retained))
    else:
        record, facts = bind_profile(profile, license_number=license_number, evidence=metadata, candidates=candidates)
    assert metadata == original_metadata
    assert record["matched_npi"] == 1003000126
    assert record["match_evidence"]["registry_binding"]["registry_completeness_verified"] is False
    assert len(session.requests) == 2 and session.closed
    for fact, original_fact in zip(facts, acquired["facts"], strict=True):
        assert fact == {**original_fact, "npi": 1003000126}
        assert fact["source_json"]["schema_page"] == acquired["schema_page"]
        assert fact["published_at"] is None
    facts[0]["source_json"]["schema_page"]["content_sha256"] = "0" * 64
    assert facts[1]["source_json"]["schema_page"] == acquired["schema_page"]
    assert metadata == original_metadata


@pytest.mark.parametrize(
    ("old", "new"),
    [
        (b"Bradley_Hospital, Butler_Hospital", b"Butler_Hospital, Bradley_Hospital"),
        (b"this.Bradley_Hospital = Bradley_Hospital;", b"this.Bradley_Hospital = Butler_Hospital;"),
        (b"thisRecord[42]", b"thisRecord[43]"),
        (b"loaddata('MD00001');", b"loaddata('MD00002');"),
        (b"loadRecord.php?id=", b"other.php?id="),
        (b'"GET"', b'"POST"'),
        (b"JSON.parse(res)", b"JSON.parse(other)"),
        (b"function Record(", b"function Other("),
        (b"myRecord = new Record", b"thisRecord.reverse(); myRecord = new Record"),
        (b"xmlhttp.open", b"str = 'MD99999'; xmlhttp.open"),
        (b'type="text/javascript"', b'type="application/json"'),
        (b"function Record(", b"/* function Record("),
        (b"xmlhttp.send();}", b"xmlhttp.send();} */"),
        (b"function Record(", b"// function Record("),
    ],
)
async def test_changed_page_stops_lookup(tmp_path, install_session, old, new):
    page = _page().replace(old, new)
    session = install_session(ProfileResponse(page))
    destination = tmp_path / "new"
    with pytest.raises(ValueError, match="rhode_island_profile_page_"):
        await acquisition.acquire_profile("MD00001", destination, run_id="test")
    assert len(session.requests) == 1 and session.closed
    assert base64.b64decode(_receipt(destination, "page.json")["body_base64"]) == page
    assert not (destination / "record.json").exists()
    assert "source_record" not in _receipt(destination, "result.json")


@pytest.mark.parametrize(
    "extra",
    [
        b"function Record() {}",
        b"new Record();",
        b"loaddata('MD00001');",
        b"function loaddata(str) {}",
        b'xmlhttp.open("GET","loadRecord.php?id="+str,true);',
        b"Record = function() {};",
        b"thisRecord = [];",
    ],
)
async def test_ambiguous_page_rejected(tmp_path, install_session, extra):
    page = _page() + b"<script>" + extra + b"</script>"
    session = install_session(ProfileResponse(page))
    with pytest.raises(ValueError, match="rhode_island_profile_page_"):
        await acquisition.acquire_profile("MD00001", tmp_path / "new", run_id="test")
    assert len(session.requests) == 1


@pytest.mark.parametrize("wrapper", ["template", "noscript", "textarea", "iframe", "title"])
async def test_inactive_context_stops_lookup(tmp_path, install_session, wrapper):
    page = f"<{wrapper}>".encode() + _page() + f"</{wrapper}>".encode()
    session = install_session(ProfileResponse(page))
    with pytest.raises(ValueError, match="rhode_island_profile_page_"):
        await acquisition.acquire_profile("MD00001", tmp_path / "new", run_id="test")
    assert len(session.requests) == 1


async def test_commented_schema_stops_lookup(tmp_path, install_session):
    page = _page().replace(b"function Record(", b"/* function Record(")
    page = page.replace(b"xmlhttp.send();}", b"xmlhttp.send();} */")
    session = install_session(ProfileResponse(page))
    with pytest.raises(ValueError, match="page_script_shape_changed"):
        await acquisition.acquire_profile("MD00001", tmp_path / "new", run_id="test")
    assert len(session.requests) == 1


@pytest.mark.parametrize(
    "prefix", [b"<template></textarea>", b"<plaintext></plaintext>", b"</template>", b"<template/>", b"<script/>"]
)
async def test_invalid_context_stops_lookup(tmp_path, install_session, prefix):
    session = install_session(ProfileResponse(prefix + _page()))
    with pytest.raises(ValueError, match="page_context_invalid"):
        await acquisition.acquire_profile("MD00001", tmp_path / "new", run_id="test")
    assert len(session.requests) == 1


@pytest.mark.parametrize("stage", ["page", "record"])
@pytest.mark.parametrize(
    "options",
    [
        {"status": 302},
        {"status": 500},
        {"headers": {}},
        {"headers": {"Content-Type": "application/json; charset=UTF-8"}},
        {"headers": {"Content-Type": "text/html; charset=latin-1"}},
        {"headers": [("Content-Type", "text/html; charset=UTF-8")] * 2},
        {"headers": {"Content-Type": "text/html; charset=UTF-8", "Content-Encoding": "gzip"}},
        {"headers": {"Content-Type": "text/html; charset=UTF-8", "Content-Length": "1"}},
        {"url": "https://example.invalid/redirect"},
    ],
)
async def test_invalid_response_is_retained(tmp_path, install_session, stage, options):
    body = _page() if stage == "page" else _profile()
    responses = [ProfileResponse(_page())] if stage == "record" else []
    session = install_session(*responses, ProfileResponse(body, **options))
    destination = tmp_path / "new"
    with pytest.raises(ValueError, match="rhode_island_profile_"):
        await acquisition.acquire_profile("MD00001", destination, run_id="test")
    assert len(session.requests) == (2 if stage == "record" else 1)
    assert base64.b64decode(_receipt(destination, stage + ".json")["body_base64"]) == body
    assert _receipt(destination, "result.json")["status"] == "failed"


@pytest.mark.parametrize("body", [b"", b"\xff", b"[]", b"[", _profile("MD00002")])
async def test_bad_record_has_no_facts(tmp_path, install_session, body):
    install_session(ProfileResponse(_page()), ProfileResponse(body))
    destination = tmp_path / "new"
    with pytest.raises(ValueError, match="rhode_island_profile_"):
        await acquisition.acquire_profile("MD00001", destination, run_id="test")
    assert base64.b64decode(_receipt(destination, "record.json")["body_base64"]) == body
    assert "facts" not in _receipt(destination, "result.json")


@pytest.mark.parametrize("exception", [TimeoutError(), aiohttp.ClientPayloadError("partial"), asyncio.CancelledError()])
async def test_partial_body_survives_error(tmp_path, install_session, exception):
    session = install_session(ProfileResponse(_page()), ProfileResponse(b"", chunks=[b"partial", exception]))
    destination = tmp_path / "new"
    with pytest.raises(type(exception)):
        await acquisition.acquire_profile("MD00001", destination, run_id="test")
    receipt = _receipt(destination, "record.json")
    assert base64.b64decode(receipt["body_base64"]) == b"partial"
    assert not receipt["eof"] and receipt["error"] == type(exception).__name__
    assert len(session.requests) == 2 and session.closed
    assert _receipt(destination, "result.json")["error"] == type(exception).__name__


async def test_transport_failure_has_no_retry(tmp_path, install_session):
    session = install_session(aiohttp.ClientConnectionError("synthetic disconnect"))
    destination = tmp_path / "new"
    with pytest.raises(aiohttp.ClientConnectionError):
        await acquisition.acquire_profile("MD00001", destination, run_id="test")
    receipt = _receipt(destination, "page.json")
    assert receipt["status"] is None and receipt["retained_bytes"] == 0 and not receipt["eof"]
    assert len(session.requests) == 1 and session._retry_connection is False


async def test_overflow_retains_bounded_prefix(tmp_path, install_session):
    limit = acquisition.MAX_RESPONSE_BYTES
    session = install_session(ProfileResponse(_page()), ProfileResponse(b"", chunks=[b"x" * limit, b"tail"]))
    destination = tmp_path / "new"
    with pytest.raises(ValueError, match="response_too_large"):
        await acquisition.acquire_profile("MD00001", destination, run_id="test")
    receipt = _receipt(destination, "record.json")
    assert base64.b64decode(receipt["body_base64"]) == b"x" * limit
    assert receipt["truncated"] and not receipt["eof"] and receipt["received_bytes"] == limit + 4
    assert session.closed


@pytest.mark.parametrize("license_number", [None, "MD1", "md00001", "MD00001 ", "DO１２３４５", "MD00001&id=2"])
async def test_invalid_license_precedes_transport(tmp_path, install_session, license_number):
    session = install_session()
    destination = tmp_path / "new"
    with pytest.raises(ValueError, match="invalid_requested_license"):
        await acquisition.acquire_profile(license_number, destination, run_id="test")
    assert not destination.exists() and not session.requests and not session.options


async def test_existing_destination_is_preserved(tmp_path, install_session):
    session = install_session()
    destination = tmp_path / "existing"
    destination.mkdir()
    marker = destination / "result.json"
    marker.write_bytes(b"incumbent")
    with pytest.raises(FileExistsError):
        await acquisition.acquire_profile("MD00001", destination, run_id="test")
    assert marker.read_bytes() == b"incumbent" and not session.options


async def test_symlink_parent_precedes_transport(tmp_path, install_session):
    session = install_session()
    linked = tmp_path / "linked"
    linked.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(ValueError, match="artifact_symlink"):
        await acquisition.acquire_profile("MD00001", linked / "new", run_id="test")
    assert not (tmp_path / "new").exists() and not session.options
