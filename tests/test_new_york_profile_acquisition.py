# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import base64
import hashlib
import json
from types import SimpleNamespace

import pytest
from multidict import CIMultiDict

from process import new_york_profile_acquisition as acquisition
from process import new_york_profile_retained as retained


def _search(total=1, **overrides):
    return {"status": 200, "backend": "synthetic", "error": None, "errorMessage": None,
            "data": {"pageNumber": 1, "numberOfResults": total, "physicians": [
                {"physicianID": "91001", "physicianFirstName": "Alex", "physicianLastName": "Example",
                 "statusCode": "A", "terminationCode": "", "terminationText": "", "dataMessage": "",
                 "medicalPractice": [], "practiceLocations": [], **overrides} for _ in range(min(10, total))]}}


def _education(license_number="654321", **identity):
    return {"status": 200, "backend": "synthetic", "error": None, "errorMessage": None, "data": {
        "physicianId": "91001", "phyInfo": {
            "physicianID": 91001, "firstName": "Alex", "middleName": "", "lastName": "Example", "suffix": "",
            "licenseNumber": license_number, "licenseDate": "", "nationalProviderId": "1000000004",
            "lastUpdated": "", **identity,
        },
        "medSchools": [{"schoolName": "Example Medical School", "gradDate": "2001"}],
        "gmeSchools": [], "boardCertification": [],
    }}


class SourceResponse:
    def __init__(self, value=None, *, body=None, status=200, url=None, content_type="application/json", encoding=None, chunks=None):
        self.status, self.url = status, url
        self.headers = {"Content-Type": content_type} if content_type is not None else {}
        if encoding is not None:
            self.headers["Content-Encoding"] = encoding
        self.body = acquisition.encoded_json(value) if body is None else body
        self.body_chunks = chunks if chunks is not None else [self.body]
        self.content = SimpleNamespace(iter_chunked=self.iter_body)
        self.chunks_read = 0

    async def iter_body(self, _chunk_size):
        for chunk in self.body_chunks:
            self.chunks_read += 1
            if isinstance(chunk, BaseException):
                raise chunk
            yield chunk

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exception):
        return False


class SourceSession:
    def __init__(self, responses, destination):
        self.responses, self.destination = list(responses), destination
        self.requests, self.options = [], {}
        self.closed, self._retry_connection = False, True

    def request(self, method, url, **options):
        assert self._retry_connection is False
        stage = "search" if not self.requests else "education"
        request = json.loads((self.destination / f"{stage}.request.json").read_bytes())
        assert request["method"] == method and request["source_url"] == url
        assert options["data"] == (request["body_text"].encode() if request["body_text"] is not None else None)
        self.requests.append((method, url, options))
        if not self.responses:
            raise AssertionError("Unexpected source request")
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        response.url = url if response.url is None else response.url
        return response

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exception):
        self.closed = True
        return False


@pytest.fixture
def source_session(monkeypatch, tmp_path):
    def install(*responses):
        session = SourceSession(responses, tmp_path / "acquisition")

        def create(**options):
            session.options = options
            return session

        monkeypatch.setattr(acquisition.aiohttp, "ClientSession", create)
        monkeypatch.setattr(acquisition, "REQUEST_INTERVAL_SECONDS", 0)
        return session
    return install


def _artifact(session, name):
    return json.loads((session.destination / name).read_bytes())


async def _acquire(session, license_number="654321", **options):
    return await acquisition.acquire_license(license_number, session.destination, run_id=options.pop("run_id", "synthetic-run"), **options)


@pytest.mark.parametrize("license_number", ["654321", "000123"])
async def test_exact_license_acquisition_retains_requests_bytes_and_unbound_facts(source_session, license_number):
    search, education = SourceResponse(_search()), SourceResponse(_education(license_number))
    session = source_session(search, education)
    result = await _acquire(session, license_number)
    assert result["outcome"] == "acquired" and result["identity_review_required"] is False
    assert len(result["facts"]) == 1 and result["source_record"]["matched_npi"] is None
    assert result["facts"][0]["npi"] is None and result["facts"][0]["published_at"] is None
    assert [(method, url) for method, url, _ in session.requests] == [
        ("POST", acquisition.SEARCH_URL), ("GET", acquisition.PROFILE_URL + "91001?sections=EDUCATIONALL&physicianId=91001")]
    assert json.loads(session.requests[0][2]["data"]) == acquisition._search_fields(license_number)
    assert all(options["allow_redirects"] is False and options["headers"]["Accept-Encoding"] == "identity"
               for _, _, options in session.requests)
    assert session.options["trust_env"] is False and session.options["auto_decompress"] is False
    assert session.options["timeout"].total == 60 and isinstance(session.options["cookie_jar"], acquisition.aiohttp.DummyCookieJar)
    assert session.closed and _artifact(session, "result.json")["fact_count"] == 1
    for stage, response in (("search", search), ("education", education)):
        evidence = _artifact(session, f"{stage}.response.json")
        request = _artifact(session, f"{stage}.request.json")
        assert base64.b64decode(evidence["body_base64"]) == response.body
        assert evidence["content_sha256"] == hashlib.sha256(response.body).hexdigest()
        assert evidence["request_sha256"] == hashlib.sha256(acquisition.encoded_json(request)).hexdigest()
        assert evidence["complete"] is True and evidence["received_bytes"] == len(response.body)
    assert result["source_record"]["match_evidence"]["license_search"]["content_sha256"] == hashlib.sha256(search.body).hexdigest()


@pytest.mark.parametrize("total", [0, 2, 11])
async def test_non_singleton_search_is_held_without_fetch_or_empty_publication(source_session, total):
    session = source_session(SourceResponse(_search(total)))
    result = await _acquire(session)
    assert result == {"outcome": "held", "reason": "search_not_singleton", "reported_total": total,
                      "source_record": None, "facts": []}
    assert len(session.requests) == 1 and _artifact(session, "result.json")["outcome"] == "held"
    assert not (session.destination / "education.request.json").exists()


async def test_disagreeing_search_header_names_remain_unbound_and_explicit(source_session):
    session = source_session(SourceResponse(_search(physicianFirstName="Example", physicianLastName="Alex")), SourceResponse(_education()))
    result = await _acquire(session)
    record = result["source_record"]
    assert result["outcome"] == "acquired" and result["identity_review_required"] is True
    assert record["matched_npi"] is None and record["match_evidence"]["reason"] == "registry_binding_not_performed"
    assert record["raw_payload"]["data"]["phyInfo"]["firstName"] == "Alex"
    assert record["match_evidence"]["license_search"]["raw_identity"]["physicianFirstName"] == "Example"
    assert record["match_evidence"]["license_search"]["header_names_agree"] is False
    assert record["normalized_payload"]["quality_flags"] == ["search_header_name_disagreement"]
    assert result["facts"][0]["source_json"]["quality_flags"] == ["search_header_name_disagreement"]


@pytest.mark.parametrize("license_number", [None, 654321, "12345", "1234567", " 654321 ", "../123", "１２３４５６"])
async def test_invalid_license_stops_before_directory_or_transport(source_session, license_number):
    session = source_session()
    with pytest.raises(ValueError, match="license_invalid"):
        await _acquire(session, license_number)
    assert session.requests == [] and not session.destination.exists()


async def test_invalid_run_stops_before_directory_or_transport(source_session):
    session = source_session()
    with pytest.raises(ValueError, match="run_id_invalid"):
        await _acquire(session, run_id=" ")
    assert session.requests == [] and not session.destination.exists()


@pytest.mark.parametrize("body", [b"", b"\xff", b"{}", b'{"status":200,"status":400}',
                                 b'{"status":200,"data":NaN}', b'<html>Access denied</html>'])
async def test_invalid_json_retains_exact_body_and_fails_without_retry(source_session, body):
    session = source_session(SourceResponse(body=body))
    with pytest.raises((ValueError, RuntimeError)):
        await _acquire(session)
    assert len(session.requests) == 1 and _artifact(session, "result.json")["outcome"] == "failed"
    assert base64.b64decode(_artifact(session, "search.response.json")["body_base64"]) == body


@pytest.mark.parametrize("changes", [{"status": 400}, {"status": True}, {"error": ["No search criteria provided"]},
                                   {"errorMessage": "Source failure"}, {"data": None}])
async def test_json_failure_never_becomes_completed_empty_search(source_session, changes):
    session = source_session(SourceResponse({**_search(), **changes}))
    with pytest.raises(ValueError, match="search_(unsuccessful|incomplete)"):
        await _acquire(session)
    assert len(session.requests) == 1 and _artifact(session, "result.json")["outcome"] == "failed"


@pytest.mark.parametrize("changes", [{"pageNumber": True}, {"pageNumber": 2}, {"numberOfResults": True},
                                   {"numberOfResults": -1}, {"numberOfResults": 2}, {"physicians": []}, {"physicians": None}])
async def test_inconsistent_counts_and_pages_are_failures(source_session, changes):
    response = _search()
    response["data"].update(changes)
    session = source_session(SourceResponse(response))
    with pytest.raises(ValueError, match="search_incomplete"):
        await _acquire(session)
    assert len(session.requests) == 1


@pytest.mark.parametrize("identity", [{"physicianID": 91001}, {"physicianID": "../1"}, {"physicianID": "0"},
                                    {"physicianFirstName": ""}, {"statusCode": None}, {"practiceLocations": None}])
async def test_invalid_search_identity_cannot_form_profile_url(source_session, identity):
    session = source_session(SourceResponse(_search(**identity)))
    with pytest.raises(ValueError, match="search_identity_invalid"):
        await _acquire(session)
    assert len(session.requests) == 1


@pytest.mark.parametrize("identity", [{"licenseNumber": "654322"}, {"physicianID": 91002}])
async def test_education_identity_drift_is_retained_and_failed(source_session, identity):
    response = SourceResponse(_education(**identity))
    session = source_session(SourceResponse(_search()), response)
    with pytest.raises(ValueError, match="new_york_profile_identity_mismatch"):
        await _acquire(session)
    assert len(session.requests) == 2 and _artifact(session, "result.json")["outcome"] == "failed"
    assert base64.b64decode(_artifact(session, "education.response.json")["body_base64"]) == response.body


@pytest.mark.parametrize("options,reason", [({"status": 302}, "http_failure"), ({"status": 403}, "http_failure"),
    ({"status": 429}, "http_failure"), ({"status": 500}, "http_failure"), ({"content_type": None}, "headers_ambiguous"),
    ({"content_type": "text/html"}, "content_type_invalid"), ({"encoding": "gzip"}, "content_encoding_invalid"),
    ({"url": "https://example.test/other"}, "response_url_changed")])
async def test_transport_envelope_failure_retains_body_without_redirect_or_retry(source_session, options, reason):
    session = source_session(SourceResponse(_search(), **options))
    with pytest.raises(ValueError, match=reason):
        await _acquire(session)
    assert len(session.requests) == 1 and _artifact(session, "search.response.json")["complete"] is True


@pytest.mark.parametrize("stage", ["search", "education"])
@pytest.mark.parametrize("duplicate", [
    ("content-type", "text/plain"), ("Content-Type", "application/json"),
    ("content-encoding", "gzip"), ("Content-Encoding", "identity"),
])
async def test_duplicate_headers_fail_with_replayable_failure_evidence(source_session, stage, duplicate):
    responses = [SourceResponse(_search()), SourceResponse(_education())]
    response = responses[0 if stage == "search" else 1]
    response.headers = CIMultiDict([("Content-Type", "application/json"), ("Content-Encoding", "identity"), duplicate])
    session = source_session(*responses)
    with pytest.raises(ValueError, match="^new_york_acquisition_headers_ambiguous$"):
        await _acquire(session)
    captured = _artifact(session, f"{stage}.response.json")
    assert captured["headers"] == [list(pair) for pair in response.headers.items()]
    assert captured["complete"] is True and base64.b64decode(captured["body_base64"]) == response.body
    assert _artifact(session, "result.json")["outcome"] == "failed"
    assert len(session.requests) == (1 if stage == "search" else 2) and session.closed
    with pytest.raises(ValueError, match="^new_york_acquisition_headers_ambiguous$"):
        retained._response_headers(captured)


async def test_missing_retry_control_fails_before_any_request(source_session):
    session = source_session(SourceResponse(_search()), SourceResponse(_education()))
    del session._retry_connection
    with pytest.raises(ValueError, match="^new_york_acquisition_retry_control_unavailable$"):
        await _acquire(session)
    assert session.requests == [] and session.closed
    assert _artifact(session, "result.json")["reason"] == "new_york_acquisition_retry_control_unavailable"
    assert not (session.destination / "search.request.json").exists()


@pytest.mark.parametrize("error", [TimeoutError("uncertain"), asyncio.CancelledError(), ConnectionError("uncertain")])
async def test_uncertain_request_remains_failed_and_cannot_reuse_destination(source_session, error):
    session = source_session(error)
    with pytest.raises(type(error)):
        await _acquire(session)
    assert _artifact(session, "search.response.json")["complete"] is False
    assert _artifact(session, "result.json")["outcome"] == "failed"
    with pytest.raises(FileExistsError):
        await _acquire(session)
    assert len(session.requests) == 1 and session.closed


async def test_partial_body_is_retained_on_interrupted_stream(source_session):
    session = source_session(SourceResponse(chunks=[b'{"status":', TimeoutError("interrupted")]))
    with pytest.raises(TimeoutError):
        await _acquire(session)
    response = _artifact(session, "search.response.json")
    assert response["complete"] is False and base64.b64decode(response["body_base64"]) == b'{"status":'
    assert response["received_bytes"] == 10 and len(session.requests) == 1


async def test_overflow_retains_only_bounded_prefix_without_reading_more(source_session, monkeypatch):
    response = SourceResponse(chunks=[b"123", b"456", b"never read"])
    session = source_session(response)
    monkeypatch.setattr(acquisition, "MAX_PROFILE_BYTES", 5)
    with pytest.raises(ValueError, match="response_too_large"):
        await _acquire(session)
    retained = _artifact(session, "search.response.json")
    assert retained["complete"] is False and base64.b64decode(retained["body_base64"]) == b"12345"
    assert retained["received_bytes"] == 6 and response.chunks_read == 2


async def test_preexisting_destination_is_never_modified(source_session):
    session = source_session()
    session.destination.mkdir()
    sentinel = session.destination / "manifest.json"
    sentinel.write_bytes(b"owned by another attempt")
    with pytest.raises(FileExistsError):
        await _acquire(session)
    assert sentinel.read_bytes() == b"owned by another attempt" and session.requests == []


async def test_symlink_destination_is_rejected_before_transport(source_session, tmp_path):
    session = source_session()
    target = tmp_path / "other"
    target.mkdir()
    session.destination.symlink_to(target, target_is_directory=True)
    with pytest.raises(ValueError, match="artifact_symlink"):
        await _acquire(session)
    assert list(target.iterdir()) == [] and session.requests == []


async def test_exclusive_request_collision_stops_before_dispatch(source_session, monkeypatch):
    session = source_session(SourceResponse(_search()))
    original_writer = acquisition.write_new_json

    def collide(path, value):
        if path.name == "search.request.json":
            path.write_bytes(b"collision")
        original_writer(path, value)

    monkeypatch.setattr(acquisition, "write_new_json", collide)
    with pytest.raises(FileExistsError):
        await _acquire(session)
    assert session.requests == [] and (session.destination / "search.request.json").read_bytes() == b"collision"
