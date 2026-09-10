# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exercise exact Kentucky acquisition with synthetic HTTP and disposable registry data."""

import asyncio
import copy
import hashlib
import json
import os
import uuid
from contextlib import asynccontextmanager
from html import escape
from types import SimpleNamespace
from unittest.mock import AsyncMock
from urllib.parse import quote

import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from process import kentucky_profile_acquisition as acquisition


def _candidate(license_number="C0007", **overrides):
    return {"license_number": license_number, "license_state": "KY", "npi": 1000000004,
            "first_name": "Alex", "middle_name": "Morgan", "last_name": "Example", "suffix": "Jr",
            "taxonomy": "207Q00000X", **overrides}


def _html(license_number="C0007", *, found=True):
    fields = [("Name", "Alex Morgan Example Jr M.D."), ("License", license_number),
              ("Medical School", "Synthetic Médical School"), ("Year Graduated", "2001")] if found else []
    field_rows = "".join(
        f'<div class="row"><div class="cols2-col1"><b>{escape(label)}:</b></div>'
        f'<div class="cols2-col2">{escape(value)}</div></div>' for label, value in fields
    )
    return f'''<html><body><form id="Form1" action="LicenseList.aspx?AGY=5&amp;FLD1=&amp;FLD2={license_number}&amp;FLD3=0&amp;FLD4=0&amp;TYPE=">
    <div class="ky-cm-content"><input id="usLicenseList_rdbtDetail" value="rdbtDetail" checked="checked" />
    <span id="usLicenseList_lblSearchCriterion"><b>Search Criterion: KY License Number = {license_number}; Practice County = 0; Specialty = 0;</b></span>
    {field_rows}</div></form></body></html>'''.encode("utf-8")


def _envelope(license_number="C0007", *, body=None, **overrides):
    body = _html(license_number) if body is None else body
    return {"schema_version": acquisition.RESPONSE_SCHEMA, "source_key": acquisition.SOURCE_KEY,
            "license_number": license_number, "source_url": acquisition.source_url(license_number),
            "downloaded_at": "2026-09-08T12:00:00+00:00", "status": 200,
            "content_type": "text/html; charset=utf-8", "content_sha256": hashlib.sha256(body).hexdigest(),
            "body_text": body.decode("utf-8"), **overrides}


class ProfileResponse:
    def __init__(self, body=None, *, status=200, content_type="text/html; charset=utf-8", chunks=None):
        self.status = status
        self.headers = {"Content-Type": content_type} if content_type is not None else {}
        self.body_chunks = chunks if chunks is not None else [_html() if body is None else body]
        self.content = SimpleNamespace(iter_chunked=self.iter_body)
        self.chunks_read = 0

    async def iter_body(self, _chunk_size):
        for chunk in self.body_chunks:
            self.chunks_read += 1
            yield chunk

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exception):
        return False


class ProfileSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []
        self.closed = False
        self._retry_connection = True

    def get(self, url, **options):
        self.requests.append((url, options))
        if not self.responses:
            raise AssertionError("Unexpected HTTP request")
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
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
        monkeypatch.setattr(acquisition.aiohttp, "ClientSession", lambda **_options: session)
        monkeypatch.setattr(acquisition, "REQUEST_INTERVAL_SECONDS", 0)
        return session
    return install


def test_cohort_preserves_raw_names_exclusions_conflicts_and_literal_licenses():
    original = _candidate(" C0007 ")
    conflict = _candidate(npi=1000000012, middle_name="Taylor", suffix=None)
    excluded_rows = [_candidate(value) for value in (None, "", "NONE", "N/A", "PENDING", "١٢٣", "C-007", "1.0", 123, "A" * 32 + "1")]
    source_rows = [original, original, conflict, _candidate("00042"), _candidate("c0007"), *excluded_rows]
    cohort = acquisition.build_cohort(source_rows, {"mrf.npi": 12})
    assert [root["license_number"] for root in cohort["roots"]] == ["00042", "C0007", "c0007"]
    assert cohort["roots"][1]["candidates"] == sorted([original, conflict], key=acquisition.encoded_json)
    assert cohort["excluded_rows"] == sorted(excluded_rows, key=acquisition.encoded_json)
    assert cohort["source_rows"] == len(source_rows) and original["license_number"] == " C0007 "
    assert cohort["source_key"] == "kentucky-kbml"
    assert cohort["coverage_scope"] == "nppes_ky_alphanumeric_physician_license_cohort"


def test_cohort_digest_binds_source_rows_names_exclusions_and_relation_oids():
    original_rows = [_candidate(), _candidate("NONE")]
    cohort = acquisition.build_cohort(original_rows, {"mrf.npi": 12})
    assert acquisition.build_cohort(list(reversed(original_rows)), {"mrf.npi": 12}) == cohort
    changes = [(original_rows, {"mrf.npi": 13}), (original_rows + [original_rows[0]], {"mrf.npi": 12})]
    changes.extend(([_candidate(**{field: "Changed"}), original_rows[1]], {"mrf.npi": 12})
                   for field in ("first_name", "middle_name", "last_name", "suffix", "license_state", "taxonomy"))
    changes.append(([_candidate(), _candidate("PENDING")], {"mrf.npi": 12}))
    for source_rows, relations in changes:
        assert acquisition.build_cohort(source_rows, relations)["registry_generation"] != cohort["registry_generation"]


def _write_cohort(path, cohort):
    cohort.pop("registry_generation", None)
    cohort["registry_generation"] = hashlib.sha256(acquisition.encoded_json(cohort)).hexdigest()
    path.write_bytes(acquisition.encoded_json(cohort))


def test_cohort_round_trip_and_changed_digest(tmp_path):
    path = tmp_path / "cohort.json"
    cohort = acquisition.build_cohort([_candidate()], {"mrf.npi": 12})
    acquisition.write_new_json(path, cohort)
    assert acquisition.read_cohort(path) == cohort
    cohort["roots"][0]["candidates"][0]["middle_name"] = "Changed"
    path.write_text(json.dumps(cohort))
    with pytest.raises(ValueError, match="cohort_changed"):
        acquisition.read_cohort(path)


@pytest.mark.parametrize(("field", "value"), [("source_key", "other"), ("schema_version", "other"), ("coverage_scope", "statewide")])
def test_rehashed_cohort_rejects_different_source_or_scope(tmp_path, field, value):
    cohort = acquisition.build_cohort([_candidate()], {})
    cohort[field] = value
    path = tmp_path / "cohort.json"
    _write_cohort(path, cohort)
    with pytest.raises(ValueError, match="cohort_changed"):
        acquisition.read_cohort(path)


@pytest.mark.parametrize("roots", [[{"license_number": "../1"}], [{"license_number": []}], [{}], [None], None,
                                  [{"license_number": "C0007"}, {"license_number": "C0007"}]])
async def test_invalid_roots_fail_before_paths_session_or_retained_access(tmp_path, monkeypatch, roots):
    session = AsyncMock(side_effect=AssertionError("Unexpected transport"))
    monkeypatch.setattr(acquisition.aiohttp, "ClientSession", session)
    destination = tmp_path / "new"
    with pytest.raises(ValueError, match="cohort_licenses_invalid"):
        await acquisition.acquire_profiles(roots, destination, AsyncMock(), retained=tmp_path / "missing")
    assert not destination.exists()
    session.assert_not_called()
    cohort = acquisition.build_cohort([], {})
    cohort["roots"] = roots
    path = tmp_path / "cohort.json"
    _write_cohort(path, cohort)
    with pytest.raises(ValueError, match="cohort_licenses_invalid"):
        acquisition.read_cohort(path)


@pytest.mark.parametrize("schema", ["mrf;DROP SCHEMA mrf", "mrf.public", "", "space schema", None])
async def test_unsafe_schema_fails_before_database(monkeypatch, schema):
    transaction = AsyncMock(side_effect=AssertionError("Unexpected database"))
    monkeypatch.setattr(acquisition.db, "transaction", transaction)
    with pytest.raises(ValueError, match="schema_invalid"):
        await acquisition.capture_registry_cohort(schema)
    transaction.assert_not_called()


@pytest.mark.parametrize("license_number", ["0", "C0007", "00042", "c0007", "A" * 31 + "1"])
async def test_public_get_preserves_literal_query_and_raw_utf8(license_number):
    body = _html(license_number)
    session = ProfileSession([ProfileResponse(body)])
    envelope = await acquisition.fetch_profile(session, license_number)
    expected_url = f"https://web1.ky.gov/GenSearch/LicenseList.aspx?AGY=5&FLD1=&FLD2={license_number}&FLD3=0&FLD4=0&TYPE="
    assert session.requests == [(expected_url, {"allow_redirects": False})]
    assert envelope["body_text"].encode("utf-8") == body
    assert envelope["content_sha256"] == hashlib.sha256(body).hexdigest()
    assert envelope["source_key"] == "kentucky-kbml" and envelope["schema_version"] == acquisition.RESPONSE_SCHEMA
    assert acquisition.decoded_profile(envelope)[0]["License"] == license_number


@pytest.mark.parametrize("license_number", [None, 123, "", "NONE", "C-007", " C0007 ", "../7", "１", "A" * 32 + "1"])
async def test_bad_license_never_reaches_transport(license_number):
    session = ProfileSession([])
    with pytest.raises(ValueError, match="license_invalid"):
        await acquisition.fetch_profile(session, license_number)
    assert session.requests == []


@pytest.mark.parametrize("status", [302, 403, 404, 429, 500])
async def test_http_failure_is_not_retried_or_read(status):
    response = ProfileResponse(status=status)
    session = ProfileSession([response])
    with pytest.raises(ValueError, match=f"http_failure:{status}"):
        await acquisition.fetch_profile(session, "C0007")
    assert len(session.requests) == 1 and response.chunks_read == 0


@pytest.mark.parametrize("content_type", [None, "application/json", "text/plain", "text/htmlp"])
async def test_non_html_media_type_is_rejected(content_type):
    session = ProfileSession([ProfileResponse(content_type=content_type)])
    with pytest.raises(ValueError, match="content_type_invalid"):
        await acquisition.fetch_profile(session, "C0007")


@pytest.mark.parametrize("body", [b"", b"\xff", b"{}", b"<html><body>Service unavailable</body></html>",
                                 _html()[:-7], _html("C007"),
                                 _html(found=False).replace(b"</div></form>", b"<p>Error</p></div></form>")])
async def test_empty_invalid_truncated_or_wrong_query_pages_are_not_checkpointed(tmp_path, install_session, body):
    session = install_session(ProfileResponse(body))
    destination = tmp_path / "new"
    with pytest.raises(ValueError):
        await acquisition.acquire_profiles([{"license_number": "C0007"}], destination, AsyncMock())
    assert list(destination.iterdir()) == [] and len(session.requests) == 1 and session.closed


async def test_only_complete_empty_lookup_is_not_found():
    envelope = await acquisition.fetch_profile(ProfileSession([ProfileResponse(_html(found=False))]), "C0007")
    assert acquisition.decoded_profile(envelope) == []
    assert envelope["body_text"]


async def test_stream_bound_stops_at_first_overflow_chunk(monkeypatch):
    monkeypatch.setattr(acquisition, "MAX_PROFILE_BYTES", 5)
    response = ProfileResponse(chunks=[b"123", b"456", b"never read"])
    with pytest.raises(ValueError, match="response_too_large"):
        await acquisition.fetch_profile(ProfileSession([response]), "C0007")
    assert response.chunks_read == 2


async def test_profile_cap_counts_utf8_bytes_at_the_boundary(monkeypatch):
    body = _html()
    assert len(body) > len(body.decode("utf-8"))
    monkeypatch.setattr(acquisition, "MAX_PROFILE_BYTES", len(body))
    assert (await acquisition.fetch_profile(ProfileSession([ProfileResponse(body)]), "C0007"))["status"] == 200
    monkeypatch.setattr(acquisition, "MAX_PROFILE_BYTES", len(body) - 1)
    with pytest.raises(ValueError, match="response_too_large"):
        await acquisition.fetch_profile(ProfileSession([ProfileResponse(body)]), "C0007")
    with pytest.raises(ValueError, match="response_changed"):
        acquisition.decoded_profile(_envelope(body=body))


@pytest.mark.parametrize(("field", "value", "reason"), [
    ("source_key", "other", "identity_invalid"), ("schema_version", "other", "identity_invalid"),
    ("license_number", "C007", "identity_invalid"), ("source_url", "https://example.test", "identity_invalid"),
    ("downloaded_at", "not-a-time", "download_time_invalid"), ("downloaded_at", "2026-09-08T12:00:00", "download_time_invalid"),
    ("downloaded_at", "2026-09-08T12:00:00+01:00", "download_time_invalid"),
    ("content_sha256", "0" * 64, "response_changed"), ("body_text", None, "response_changed"),
    ("status", 302, "http_failure"),
])
def test_replay_rejects_changed_envelope(tmp_path, field, value, reason):
    path = tmp_path / "C0007.json"
    acquisition.write_new_json(path, _envelope(**{field: value}))
    with pytest.raises(ValueError, match=reason):
        acquisition.read_response(path, "C0007")


@pytest.mark.parametrize("artifact", ["cohort", "response"])
def test_artifact_rejects_symlink_parent_and_oversize(tmp_path, artifact):
    source = tmp_path / "source"
    source.mkdir()
    path = source / "artifact.json"
    value = acquisition.build_cohort([_candidate()], {}) if artifact == "cohort" else _envelope()
    reader = acquisition.read_cohort if artifact == "cohort" else lambda path: acquisition.read_response(path, "C0007")
    acquisition.write_new_json(path, value)
    linked = tmp_path / "linked"
    linked.symlink_to(source, target_is_directory=True)
    with pytest.raises(ValueError, match="artifact_symlink"):
        reader(linked / path.name)
    with path.open("r+b") as output:
        output.truncate(64 * 1024 * 1024 + 1)
    with pytest.raises(ValueError, match="artifact_file_invalid"):
        reader(path)


async def test_replay_preserves_exact_envelopes_and_hashes_every_response_field(tmp_path, install_session):
    retained = tmp_path / "retained"
    retained.mkdir()
    first = _envelope()
    acquisition.write_new_json(retained / "C0007.json", first)
    original = (retained / "C0007.json").read_bytes()
    session = install_session(ProfileResponse(_html("00042", found=False)))
    roots = [{"license_number": number} for number in ("C0007", "00042")]
    destination = tmp_path / "new"
    metrics = await acquisition.acquire_profiles(roots, destination, AsyncMock(), retained=retained)
    second = acquisition.read_response(destination / "00042.json", "00042")
    expected_hash = hashlib.sha256(acquisition.encoded_json(first) + acquisition.encoded_json(second)).hexdigest()
    assert metrics == {"responses": 2, "response_bytes": len(_html()) + len(_html("00042", found=False)),
                       "reused_responses": 1, "responses_sha256": expected_hash}
    assert (retained / "C0007.json").read_bytes() == (destination / "C0007.json").read_bytes() == original
    assert session.requests == [(acquisition.source_url("00042"), {"allow_redirects": False})]
    session = install_session()
    replay = await acquisition.acquire_profiles(roots, tmp_path / "replay", AsyncMock(), retained=destination)
    assert replay == {**metrics, "reused_responses": 2} and session.requests == [] and session.closed
    first["content_type"] = "text/html; charset=UTF-8"
    (retained / "C0007.json").write_bytes(acquisition.encoded_json(first))
    changed = await acquisition.acquire_profiles([roots[0]], tmp_path / "changed", AsyncMock(), retained=retained)
    assert changed["responses_sha256"] == hashlib.sha256(acquisition.encoded_json(first)).hexdigest()


@pytest.mark.parametrize("failure", ["changed", "dangling_symlink", "directory", "wrong_query", "truncated", "error"])
async def test_invalid_retained_response_never_falls_back_to_http(tmp_path, install_session, failure):
    retained = tmp_path / "retained"
    retained.mkdir()
    path = retained / "C0007.json"
    if failure == "changed":
        acquisition.write_new_json(path, _envelope(content_sha256="0" * 64))
    elif failure in {"wrong_query", "truncated", "error"}:
        body_by_failure = {"wrong_query": _html("C007"), "truncated": _html()[:-7], "error": b"Service unavailable"}
        acquisition.write_new_json(path, _envelope(body=body_by_failure[failure]))
    elif failure == "dangling_symlink":
        path.symlink_to(tmp_path / "missing")
    else:
        path.mkdir()
    session = install_session()
    with pytest.raises(ValueError):
        await acquisition.acquire_profiles([{"license_number": "C0007"}], tmp_path / "new", AsyncMock(), retained=retained)
    assert session.requests == [] and path.is_symlink() == (failure == "dangling_symlink")


@pytest.mark.parametrize("kind", ["destination", "destination_parent", "retained"])
async def test_symlink_directories_fail_before_transport(tmp_path, install_session, kind):
    source = tmp_path / "source"
    source.mkdir()
    linked = tmp_path / "linked"
    linked.symlink_to(source, target_is_directory=True)
    destination = linked if kind == "destination" else linked / "new" if kind == "destination_parent" else tmp_path / "new"
    session = install_session()
    with pytest.raises(ValueError, match="artifact_symlink"):
        await acquisition.acquire_profiles([{"license_number": "C0007"}], destination, AsyncMock(),
                                           retained=linked if kind == "retained" else None)
    assert session.requests == [] and list(source.iterdir()) == []


async def test_existing_destination_is_not_replaced(tmp_path, install_session):
    destination = tmp_path / "incumbent"
    destination.mkdir()
    marker = destination / "keep"
    marker.write_text("incumbent")
    session = install_session()
    with pytest.raises(FileExistsError):
        await acquisition.acquire_profiles([{"license_number": "C0007"}], destination, AsyncMock())
    assert marker.read_text() == "incumbent" and session.requests == []


async def test_checkpoint_collision_preserves_incumbent_and_stops(tmp_path, install_session):
    destination = tmp_path / "new"
    marker = destination / "C0007.json"
    session = install_session(ProfileResponse(), ProfileResponse(_html("00042")))

    async def create_collision(_completed, _total):
        marker.write_text("incumbent")

    with pytest.raises(FileExistsError):
        await acquisition.acquire_profiles([{"license_number": "C0007"}, {"license_number": "00042"}],
                                           destination, create_collision)
    assert marker.read_text() == "incumbent" and set(destination.iterdir()) == {marker}
    assert len(session.requests) == 1 and session.closed


@pytest.mark.parametrize("completed_before_cancel", [0, 1])
async def test_cancellation_stops_before_next_request_and_keeps_checkpoint(tmp_path, install_session, completed_before_cancel):
    session = install_session(ProfileResponse(), ProfileResponse(_html("00042")))
    destination = tmp_path / "new"

    async def cancel_progress(completed, _total):
        if completed == completed_before_cancel:
            raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await acquisition.acquire_profiles([{"license_number": "C0007"}, {"license_number": "00042"}], destination, cancel_progress)
    assert len(session.requests) == completed_before_cancel and session.closed
    assert len(list(destination.iterdir())) == completed_before_cancel
    if completed_before_cancel:
        assert acquisition.read_response(destination / "C0007.json", "C0007")["status"] == 200


async def test_transport_failure_has_no_retry_and_preserves_prior_response(tmp_path, install_session):
    session = install_session(ProfileResponse(), asyncio.TimeoutError())
    destination = tmp_path / "new"
    with pytest.raises(asyncio.TimeoutError):
        await acquisition.acquire_profiles([{"license_number": "C0007"}, {"license_number": "00042"}], destination, AsyncMock())
    assert len(session.requests) == 2 and session.closed
    assert [path.name for path in destination.iterdir()] == ["C0007.json"]


@pytest.mark.parametrize("control", ["absent", None, 0, "false"])
async def test_retry_control_unavailable_fails_before_requests(tmp_path, install_session, control):
    session = install_session(ProfileResponse())
    if control == "absent":
        del session._retry_connection
    else:
        session._retry_connection = control
    destination = tmp_path / "new"
    with pytest.raises(ValueError, match="kentucky_profile_retry_control_unavailable"):
        await acquisition.acquire_profiles([{"license_number": "C0007"}], destination, AsyncMock())
    assert session.requests == [] and session.closed and list(destination.iterdir()) == []


@pytest.mark.parametrize("control", [True, False])
async def test_existing_boolean_retry_control_is_disabled(tmp_path, install_session, control):
    session = install_session(ProfileResponse())
    session._retry_connection = control
    await acquisition.acquire_profiles([{"license_number": "C0007"}], tmp_path / "new", AsyncMock())
    assert session._retry_connection is False and len(session.requests) == 1 and session.closed


@pytest.mark.parametrize("failure_type", [acquisition.aiohttp.ServerDisconnectedError, acquisition.aiohttp.ClientOSError])
async def test_real_client_connection_failure_is_not_retried(tmp_path, monkeypatch, failure_type):
    """Exercise aiohttp's retry loop with middleware that fails before any transport."""
    client_session = acquisition.aiohttp.ClientSession
    attempts = []
    sessions = []
    blocked_transport = AsyncMock(side_effect=AssertionError("Unexpected network connection"))
    monkeypatch.setattr(acquisition.aiohttp.TCPConnector, "connect", blocked_transport)

    async def fail_before_transport(request, _handler):
        attempts.append(str(request.url))
        raise failure_type("synthetic connection failure")

    def create_session(**options):
        session = client_session(**options, middlewares=(fail_before_transport,))
        sessions.append(session)
        return session

    monkeypatch.setattr(acquisition.aiohttp, "ClientSession", create_session)
    destination = tmp_path / "new"
    with pytest.raises(failure_type, match="synthetic connection failure"):
        await acquisition.acquire_profiles([{"license_number": "C0007"}], destination, AsyncMock())
    assert attempts == [acquisition.source_url("C0007")]
    assert list(destination.iterdir()) == [] and all(session.closed for session in sessions)
    blocked_transport.assert_not_called()


async def test_total_cap_counts_replay_and_stops_before_overflow(tmp_path, install_session, monkeypatch):
    retained = tmp_path / "retained"
    retained.mkdir()
    acquisition.write_new_json(retained / "C0007.json", _envelope())
    monkeypatch.setattr(acquisition, "MAX_ACQUISITION_BYTES", len(_html()) + len(_html("00042")) - 1)
    session = install_session(ProfileResponse(_html("00042")))
    destination = tmp_path / "new"
    with pytest.raises(ValueError, match="acquisition_too_large"):
        await acquisition.acquire_profiles([{"license_number": "C0007"}, {"license_number": "00042"}],
                                           destination, AsyncMock(), retained=retained)
    assert [path.name for path in destination.iterdir()] == ["C0007.json"]
    assert len(session.requests) == 1


async def test_request_starts_are_sequential_and_two_seconds_apart(tmp_path, install_session, monkeypatch):
    licenses = ["C0007", "00042", "00043"]
    session = install_session(*(ProfileResponse(_html(number)) for number in licenses))
    clock = SimpleNamespace(now=100.0)
    request_starts = []
    original_get = session.get

    def timed_get(url, **options):
        request_starts.append(clock.now)
        return original_get(url, **options)

    async def advance_clock(delay):
        clock.now += delay

    monkeypatch.setattr(session, "get", timed_get)
    monkeypatch.setattr(acquisition, "REQUEST_INTERVAL_SECONDS", 2.0)
    monkeypatch.setattr(acquisition, "asyncio", SimpleNamespace(
        sleep=advance_clock, get_running_loop=lambda: SimpleNamespace(time=lambda: clock.now),
    ))
    await acquisition.acquire_profiles([{"license_number": number} for number in licenses], tmp_path / "new", AsyncMock())
    assert request_starts == [100.0, 102.0, 104.0]


def _narrowed_html(surname, *, returned_license="0", found=True, year="2001"):
    body = _html("0", found=found).replace(b"FLD1=&amp;", f"FLD1={quote(surname)}&amp;".encode())
    body = body.replace(b"Search Criterion: KY", f"Search Criterion: Last Name = {escape(surname)}; KY".encode())
    body = body.replace(b">0</div>", f">{returned_license}</div>".encode())
    return body.replace(b">2001</div>", f">{year}</div>".encode())


def _narrowing_root():
    return {"license_number": "0", "candidates": [
        _candidate("0", last_name=surname, npi=npi)
        for surname, npi in zip(("Example", "Other", "Sample", "Synthetic"),
                                (1000000004, 1000000012, 1000000020, 1000000038), strict=True)]}


def _narrowed_envelope(root, bodies):
    attempt_by_field = {"truncated": True, "complete": False, "outcome": "response_too_large",
               "observed_bytes": acquisition.MAX_PROFILE_BYTES + 1, "read_prefix_sha256": "a" * 64,
               "license_number": "0", "source_url": acquisition.source_url("0"),
               "downloaded_at": "2026-09-08T11:00:00+00:00", "status": 200, "content_type": "text/html"}
    scope = acquisition._candidate_query_scope(root)
    return {"schema_version": acquisition.NARROWED_SCHEMA, "source_key": acquisition.SOURCE_KEY,
            "license_number": "0", "query_scope": scope, "oversized_attempt": attempt_by_field,
            "responses": [_envelope("0", body=body, source_url=acquisition.source_url("0", last_name=query["last_name"]))
                          for query, body in zip(scope["queries"], bodies, strict=True)]}


async def test_oversized_root_retains_all_four_queries_and_replays_without_http(tmp_path, install_session, monkeypatch):
    monkeypatch.setattr(acquisition, "MAX_PROFILE_BYTES", 2000)
    root = _narrowing_root()
    root["candidates"].append(copy.deepcopy(root["candidates"][0]))
    bodies = [_narrowed_html(candidate["last_name"], returned_license="01234", found=index == 0)
              for index, candidate in enumerate(root["candidates"][:4])]
    prefix = b"x" * 2017
    session = install_session(ProfileResponse(chunks=[prefix[:1000], prefix[1000:]]), *(ProfileResponse(body) for body in bodies))
    destination = tmp_path / "new"
    metrics = await acquisition.acquire_profiles([root], destination, AsyncMock())
    response = acquisition.read_response(destination / "0.json", "0", candidates=root["candidates"])
    assert [url for url, _options in session.requests] == [acquisition.source_url("0"), *[
        acquisition.source_url("0", last_name=candidate["last_name"]) for candidate in root["candidates"][:4]]]
    assert response["query_scope"]["queries"][0]["candidate_indexes"] == [0, 4]
    assert response["oversized_attempt"]["read_prefix_sha256"] == hashlib.sha256(prefix).hexdigest()
    assert response["oversized_attempt"]["observed_bytes"] == len(prefix)
    assert response["oversized_attempt"]["truncated"] is True and response["oversized_attempt"]["complete"] is False
    assert "body_text" not in response["oversized_attempt"] and "content_sha256" not in response["oversized_attempt"]
    assert [receipt["body_text"].encode() for receipt in response["responses"]] == bodies
    assert metrics["responses"] == 1 and metrics["narrowed_queries"] == 4 and metrics["retained_http_responses"] == 5
    assert metrics["response_bytes"] == len(prefix) + sum(map(len, bodies))
    from process.kentucky_profile import _source_rows
    record, facts = _source_rows(root, response, {"run_id": "a" * 64, "artifact_id": "b" * 64}, 1)
    assert record["match_status"] == "identity_conflict" and record["matched_npi"] is None and facts == []
    assert record["raw_payload"]["acquisition"] == response
    session = install_session()
    replay = await acquisition.acquire_profiles([root], tmp_path / "replay", AsyncMock(), retained=destination)
    assert replay == {**metrics, "reused_responses": 1} and session.requests == []


@pytest.mark.parametrize("surname", [None, "", " ", "Unknown%", "[Unknown]", "x" * 51])
async def test_unsupported_candidate_never_queries_a_subset(tmp_path, install_session, monkeypatch, surname):
    monkeypatch.setattr(acquisition, "MAX_PROFILE_BYTES", 2000)
    root = _narrowing_root(); root["candidates"][-1]["last_name"] = surname
    session = install_session(ProfileResponse(chunks=[b"x" * 2000, b"x"]))
    destination = tmp_path / "new"
    with pytest.raises(ValueError, match="narrowing_surname_unsupported"):
        await acquisition.acquire_profiles([root], destination, AsyncMock())
    assert len(session.requests) == 1 and not (destination / "0.json").exists()
    assert json.loads((destination / "0.narrowed/oversized.json").read_text())["truncated"] is True


async def test_interrupted_narrowing_keeps_checkpoint_and_refuses_resume(tmp_path, install_session, monkeypatch):
    monkeypatch.setattr(acquisition, "MAX_PROFILE_BYTES", 2000)
    root = _narrowing_root()
    session = install_session(ProfileResponse(chunks=[b"x" * 2000, b"x"]),
                              ProfileResponse(_narrowed_html("Example")), asyncio.TimeoutError())
    destination = tmp_path / "new"
    with pytest.raises(asyncio.TimeoutError):
        await acquisition.acquire_profiles([root], destination, AsyncMock())
    assert len(session.requests) == 3 and (destination / "0.narrowed/1.json").is_file()
    assert (destination / "0.narrowed/2.failed.json").is_file() and not (destination / "0.json").exists()
    session = install_session()
    with pytest.raises(ValueError, match="narrowed_checkpoint_incomplete"):
        await acquisition.acquire_profiles([root], tmp_path / "replay", AsyncMock(), retained=destination)
    assert session.requests == [] and not (tmp_path / "replay").exists()


async def test_same_text_without_typed_overflow_does_not_narrow(tmp_path, install_session):
    session = install_session(ValueError("kentucky_profile_response_too_large"))
    with pytest.raises(ValueError, match="response_too_large"):
        await acquisition.acquire_profiles([_narrowing_root()], tmp_path / "new", AsyncMock())
    assert len(session.requests) == 1 and list((tmp_path / "new").iterdir()) == []


@pytest.mark.parametrize("change", ["candidate", "queries", "url", "body_hash", "truncation", "byte_count"])
async def test_narrowed_replay_tampering_fails_before_http(tmp_path, install_session, change):
    root = _narrowing_root()
    envelope = _narrowed_envelope(root, [_narrowed_html(candidate["last_name"]) for candidate in root["candidates"]])
    if change == "candidate":
        root["candidates"][0]["middle_name"] = "Changed"
    if change == "queries":
        envelope["responses"].pop()
    if change == "url":
        envelope["responses"][0]["source_url"] = acquisition.source_url("0")
    if change == "body_hash":
        envelope["responses"][0]["content_sha256"] = "b" * 64
    if change == "truncation":
        envelope["oversized_attempt"]["complete"] = True
    if change == "byte_count":
        envelope["oversized_attempt"]["observed_bytes"] = 1
    retained = tmp_path / "retained"; retained.mkdir()
    acquisition.write_new_json(retained / "0.json", envelope)
    session = install_session()
    with pytest.raises(ValueError):
        await acquisition.acquire_profiles([root], tmp_path / "new", AsyncMock(), retained=retained)
    assert session.requests == [] and not (tmp_path / "new").exists()


async def test_overflow_consumed_prefix_counts_towards_unchanged_global_cap(tmp_path, install_session, monkeypatch):
    monkeypatch.setattr(acquisition, "MAX_PROFILE_BYTES", 2000)
    body = _narrowed_html("Example")
    monkeypatch.setattr(acquisition, "MAX_ACQUISITION_BYTES", 2017 + len(body) - 1)
    session = install_session(ProfileResponse(chunks=[b"x" * 2000, b"x" * 17]), ProfileResponse(body))
    with pytest.raises(ValueError, match="acquisition_too_large"):
        await acquisition.acquire_profiles([_narrowing_root()], tmp_path / "new", AsyncMock())
    assert len(session.requests) == 2 and (tmp_path / "new/0.narrowed/1.json").is_file()
    assert not (tmp_path / "new/0.json").exists()


@pytest.mark.parametrize("outcome", ["empty", "identical", "conflict"])
def test_narrowed_union_preserves_unknowns_and_conflicting_full_payloads(outcome):
    from process.kentucky_profile import _source_rows
    root = _narrowing_root()
    bodies = [_narrowed_html(candidate["last_name"], found=outcome != "empty",
                             year="2002" if outcome == "conflict" and index == 1 else "2001")
              for index, candidate in enumerate(root["candidates"])]
    envelope = _narrowed_envelope(root, bodies)
    record, facts = _source_rows(root, envelope, {"run_id": "a" * 64, "artifact_id": "b" * 64}, 1)
    assert len(record["raw_payload"]["acquisition"]["responses"]) == 4
    if outcome == "identical":
        assert len(record["raw_payload"]["profiles"]) == 1 and record["match_status"] == "deterministic"
        assert len(facts) == 1 and facts[0]["npi"] == 1000000004
        assert facts[0]["source_json"]["source_url"] == envelope["responses"][0]["source_url"]
        assert facts[0]["source_json"]["content_sha256"] == envelope["responses"][0]["content_sha256"]
    else:
        assert record["matched_npi"] is None and facts == []
        assert record["match_status"] == ("unmatched" if outcome == "empty" else "identity_conflict")
        assert record["normalized_payload"]["visibility"] == "held_identity"
        if outcome == "empty":
            assert record["match_evidence"]["reason"] == "no_profile_within_candidate_name_queries"


async def _create_registry_tables(database, schema):
    statements = (
        f"CREATE TABLE {schema}.npi (npi bigint PRIMARY KEY, entity_type_code integer, provider_first_name text, "
        "provider_middle_name text, provider_last_name text, provider_name_suffix_text text)",
        f"CREATE TABLE {schema}.npi_taxonomy (npi bigint, provider_license_number text, "
        "healthcare_provider_taxonomy_code text, provider_license_number_state_code text)",
        f"CREATE TABLE {schema}.nucc_taxonomy (code text PRIMARY KEY, grouping text)",
        f"INSERT INTO {schema}.npi VALUES "
        "(1000000004,1,'Alex','Morgan','Example','Jr'), (1000000012,1,'Second',NULL,'Example',NULL), "
        "(1000000020,2,'Example',NULL,'Organization',NULL), (1000000038,1,'Example',NULL,'Dentist',NULL)",
        f"INSERT INTO {schema}.nucc_taxonomy VALUES "
        "('207Q00000X','Allopathic & Osteopathic Physicians'), ('122300000X','Dental Providers')",
        f"INSERT INTO {schema}.npi_taxonomy VALUES "
        "(1000000004,'C0007','207Q00000X','KY'), (1000000004,' 00042 ','207Q00000X','KY'), "
        "(1000000004,'NONE','207Q00000X','KY'), (1000000012,'C0007','207Q00000X','KY'), "
        "(1000000004,'999','207Q00000X','NY'), (1000000020,'789','207Q00000X','KY'), "
        "(1000000038,'111','122300000X','KY')",
    )
    for statement in statements:
        await database.status(statement)


@asynccontextmanager
async def _registry_database(monkeypatch):
    """Use only a UUID-owned schema and verify its removal before disconnecting."""
    database_dsn = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_PROFILE_POSTGRES_DSN")
    if not database_dsn:
        pytest.skip("set the profile PostgreSQL DSN to check cohort snapshots")
    schema = f"ky_cohort_{uuid.uuid4().hex}"
    engine = create_async_engine(make_url(database_dsn).set(drivername="postgresql+asyncpg"))
    database = Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False))
    is_schema_created = False
    try:
        await database.status(f"CREATE SCHEMA {schema}")
        is_schema_created = True
        await _create_registry_tables(database, schema)
        monkeypatch.setattr(acquisition, "db", database)
        yield database, schema
    finally:
        try:
            if is_schema_created:
                await database.status(f"DROP SCHEMA {schema} CASCADE")
                assert await database.scalar(f"SELECT to_regnamespace('{schema}')") is None
        finally:
            await database.disconnect()


async def test_cohort_filters_names_conflicts_and_repeatable_snapshot_in_postgresql(monkeypatch):
    """Freeze all matching names and licenses while a concurrent registry write commits."""
    async with _registry_database(monkeypatch) as (database, schema):
        original_all = database.all
        observed_reads = []

        async def read_then_insert(statement, **parameters):
            query_rows = await original_all(statement, **parameters)
            observed_reads.append(str(statement))
            if len(observed_reads) == 1:
                assert await database.scalar("SHOW transaction_isolation") == "repeatable read"
                assert await database.scalar("SHOW transaction_read_only") == "on"
                async with database.engine.begin() as writer:
                    await writer.exec_driver_sql(
                        f"INSERT INTO {schema}.npi_taxonomy VALUES (1000000012,'456','207Q00000X','KY')"
                    )
            return query_rows

        monkeypatch.setattr(database, "all", read_then_insert)
        cohort = await acquisition.capture_registry_cohort(schema)
        assert [root["license_number"] for root in cohort["roots"]] == ["00042", "C0007"]
        assert cohort["roots"][0]["candidates"] == [_candidate(" 00042 ")]
        assert {candidate["npi"] for candidate in cohort["roots"][1]["candidates"]} == {1000000004, 1000000012}
        assert cohort["excluded_rows"] == [_candidate("NONE")] and cohort["source_rows"] == 4
        assert len(cohort["relations"]) == 3
        assert all(isinstance(oid, int) and oid > 0 for oid in cohort["relations"].values())
        assert await database.scalar(f"SELECT count(*) FROM {schema}.npi_taxonomy") == 8


@pytest.mark.parametrize("table_name", ["npi", "npi_taxonomy", "nucc_taxonomy"])
async def test_relation_names_stay_locked_until_cohort_identity_is_read(monkeypatch, table_name):
    """A registry name swap must wait until the row and OID reads commit together."""
    async with _registry_database(monkeypatch) as (database, schema):
        original_all = database.all
        original_oid = await database.scalar(f"SELECT to_regclass('{schema}.{table_name}')::oid::bigint")
        observed_reads = []

        async def read_then_attempt_rename(statement, **parameters):
            query_rows = await original_all(statement, **parameters)
            observed_reads.append(str(statement))
            if len(observed_reads) == 1:
                with pytest.raises(DBAPIError) as locked:
                    async with database.engine.begin() as writer:
                        await writer.exec_driver_sql("SET LOCAL lock_timeout = '50ms'")
                        await writer.exec_driver_sql(f"ALTER TABLE {schema}.{table_name} RENAME TO replaced_table")
                assert locked.value.orig.sqlstate == "55P03"
            return query_rows

        monkeypatch.setattr(database, "all", read_then_attempt_rename)
        cohort = await acquisition.capture_registry_cohort(schema)
        assert cohort["relations"][f"{schema}.{table_name}"] == original_oid
        async with database.engine.begin() as writer:
            await writer.exec_driver_sql(f"ALTER TABLE {schema}.{table_name} RENAME TO replaced_table")
        assert await database.scalar(f"SELECT to_regclass('{schema}.replaced_table')::oid::bigint") == original_oid
