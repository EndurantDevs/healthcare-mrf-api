# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import base64
import hashlib
import json
import socket
from types import SimpleNamespace

import pytest
from multidict import CIMultiDict

from api.provider_education import canonicalize_education_category
from process import new_york_nysed_profile as profile
from process import new_york_nysed_profile_acquisition as acquisition

PUBLIC_HEADER = "synthetic-public-application-header"


def _profile_body(license_number="654321", **changes):
    text_by_field = {
        "profession": "Medicine (060)",
        "name": "EXAMPLE ALEX MORGAN",
        "address": "Example City, NY",
        "status": "Registered",
        "dateOfLicensure": "June 15, 2003",
        "additionalQualifications": "None",
        "registeredThroughDate": "December 31, 2030",
        "schoolName": "Example Medical School",
        "schoolDegreeDate": "May 20, 2001",
        "licenseNumber": license_number,
    }
    text_by_field.update(changes)
    return {
        **{field: {"label": label, "value": text_by_field[field]} for field, label in profile.TEXT_LABELS.items()},
        "professionCode": "060",
        "enforcementActions": [],
        "index": 0,
        "noEnforcementActionsFoundMessage": "Physician discipline is reported by a separate agency.",
        "additionalLicenses": {"label": "Additional Licenses", "value": []},
        "privileges": {"label": "Additional Qualifications", "value": ["None"]},
        "certificateOfAuthorizations": [],
    }


def _parse(profile_by_field, license_number="654321", *, evidence_changes=None):
    body = profile.encoded_json(profile_by_field)
    evidence_by_field = {
        "run_id": "synthetic-run",
        "artifact_id": "synthetic-artifact",
        "source_url": profile.request_descriptor(license_number)["source_url"],
        "downloaded_at": "2026-09-11T00:00:00+00:00",
        "content_sha256": hashlib.sha256(body).hexdigest(),
    }
    evidence_by_field.update(evidence_changes or {})
    return profile.parse_profile(body, license_number=license_number, evidence=evidence_by_field)


class SourceResponse:
    def __init__(self, profile_by_field=None, *, raw=None, status=200, headers=None, url=None, chunks=None):
        self.body = profile.encoded_json(profile_by_field or _profile_body()) if raw is None else raw
        self.status, self.url = status, url
        self.headers = CIMultiDict([("Content-Type", "application/json")] if headers is None else headers)
        self.body_chunks = [self.body] if chunks is None else chunks
        self.content = SimpleNamespace(iter_chunked=self.iter_chunks)

    async def iter_chunks(self, _size):
        for chunk in self.body_chunks:
            if isinstance(chunk, BaseException):
                raise chunk
            yield chunk

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exception):
        return False


class SourceSession:
    def __init__(self, response):
        self.response, self.requests, self.options = response, [], {}
        self._retry_connection, self.closed = True, False

    def get(self, url, **options):
        assert self._retry_connection is False
        assert options["ssl"] is True and options["allow_redirects"] is False
        assert options["headers"]["x-oapi-key"] == PUBLIC_HEADER
        self.requests.append((url, options))
        if isinstance(self.response, BaseException):
            raise self.response
        self.response.url = url if self.response.url is None else self.response.url
        return self.response

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exception):
        self.closed = True
        return False


@pytest.fixture(autouse=True)
def deny_network(monkeypatch):
    def reject(*_args, **_kwargs):
        raise AssertionError("Synthetic checks forbid network access")

    monkeypatch.setattr(socket.socket, "connect", reject)
    monkeypatch.setattr(socket.socket, "connect_ex", reject)
    monkeypatch.setattr(socket, "create_connection", reject)


@pytest.fixture
def source_session(monkeypatch, tmp_path):
    def install(response):
        session = SourceSession(response)
        session.destination = tmp_path.resolve() / "acquisition"

        def create(**options):
            session.options = options
            return session

        monkeypatch.setattr(acquisition.aiohttp, "ClientSession", create)
        return session

    return install


async def _acquire(session, license_number="654321"):
    return await acquisition.acquire_license(
        license_number, session.destination, run_id="synthetic-run", api_key=PUBLIC_HEADER
    )


def _artifact(session, name):
    return json.loads((session.destination / name).read_bytes())


def test_source_facts_preserve_meanings():
    source_record, facts = _parse(_profile_body())
    education, license_fact = facts
    assert source_record["matched_npi"] is None and source_record["match_status"] == "unmatched"
    assert source_record["profession_code"] == "060"
    assert source_record["raw_payload"]["name"]["value"] == "EXAMPLE ALEX MORGAN"
    assert education["value_json"] == {
        "institution": "Example Medical School",
        "graduation_date": "2001-05-20",
        "graduation_date_precision": "day",
        "graduation_year": 2001,
    }
    assert license_fact["value_json"]["license_status"] == "Registered"
    assert license_fact["value_json"]["registered_through_date"] == "2030-12-31"
    assert license_fact["value_json"]["date_of_licensure"] == "2003-06-15"
    assert not license_fact["source_json"]["quality_flags"]
    assert {(fact["category"], fact["fact_type"]) for fact in facts} == {
        ("education", "education_history"),
        ("licenses", "state_licensure_record"),
    }
    assert all(
        fact["npi"] is None
        and fact["published_at"] is None
        and fact["effective_start"] is None
        and fact["effective_end"] is None
        for fact in facts
    )
    assert all(
        fact["assertion_type"] == "source_reported" and fact["verification_status"] == "not_independently_verified"
        for fact in facts
    )
    assert not any("address" in fact["value_json"] or "expiration_date" in fact["value_json"] for fact in facts)


@pytest.mark.parametrize("body", [None, [], "{}", b""])
def test_invalid_body_type_is_rejected(body):
    with pytest.raises(ValueError, match="body_invalid"):
        profile.parse_profile(body, license_number="654321", evidence={})


@pytest.mark.parametrize(
    "changes,error",
    [
        ({"run_id": ""}, "evidence_invalid"),
        ({"content_sha256": "0" * 64}, "evidence_mismatch"),
        ({"source_url": profile.request_descriptor("123456")["source_url"]}, "evidence_mismatch"),
        ({"downloaded_at": "not-a-date"}, "timestamp_invalid"),
        ({"downloaded_at": "2026-09-11T00:00:00+01:00"}, "timestamp_invalid"),
    ],
)
def test_valid_body_requires_matching_evidence(changes, error):
    with pytest.raises(ValueError, match=f"^new_york_nysed_{error}$"):
        _parse(_profile_body(), evidence_changes=changes)


@pytest.mark.asyncio
async def test_installed_aiohttp_disables_implicit_retry(monkeypatch, tmp_path):
    async def stop_before_request(session, *_args):
        assert isinstance(session, acquisition.aiohttp.ClientSession)
        assert session._retry_connection is False
        raise RuntimeError("synthetic stop before request")

    monkeypatch.setattr(acquisition, "_fetch_profile", stop_before_request)
    with pytest.raises(RuntimeError, match="^synthetic stop before request$"):
        await acquisition._acquire_profile(tmp_path, {}, profile.request_descriptor("654321"), PUBLIC_HEADER)


@pytest.mark.parametrize("month", ["March", "May", "June", "September", "December"])
def test_english_month_dates(month):
    _, facts = _parse(_profile_body(schoolDegreeDate=f"{month} 15, 2001"))
    assert facts[0]["value_json"]["graduation_date"] == f"2001-{profile.MONTHS.index(month) + 1:02d}-15"


@pytest.mark.parametrize("reported", ["", "None", " N/A ", "not reported", "Unknown"])
def test_education_sentinels_are_not_facts(reported):
    _, facts = _parse(_profile_body(schoolName=reported, schoolDegreeDate=reported))
    assert [fact["category"] for fact in facts] == ["licenses"]


@pytest.mark.parametrize("missing_school", [False, True])
@pytest.mark.parametrize("missing_date", [False, True])
def test_unreported_education_preserves_profile_without_education_fact(missing_school, missing_date):
    source = _profile_body(schoolName=None, schoolDegreeDate=None)
    for field, missing in (("schoolName", missing_school), ("schoolDegreeDate", missing_date)):
        if missing:
            del source[field]
    record, facts = _parse(source)
    assert record["raw_payload"] == source
    assert record["matched_npi"] is None and record["match_status"] == "unmatched"
    assert [fact["category"] for fact in facts] == ["licenses"]
    assert facts[0]["value_json"]["license_status"] == "Registered"


@pytest.mark.parametrize("field", profile.EDUCATION_FIELDS)
@pytest.mark.parametrize("missing", [False, True])
def test_partial_education_retains_only_reported_fields(field, missing):
    source = _profile_body(**{field: None})
    if missing:
        del source[field]
    record, facts = _parse(source)
    education = facts[0]
    assert record["raw_payload"] == source and education["category"] == "education"
    assert ("institution" in education["value_json"]) == (field != "schoolName")
    assert ("graduation_year" in education["value_json"]) == (field != "schoolDegreeDate")
    assert education["source_json"]["raw_fields"] == {
        name: source[name] for name in profile.EDUCATION_FIELDS if name in source
    }


@pytest.mark.parametrize(
    "field,normalized_field",
    [("dateOfLicensure", "date_of_licensure"), ("registeredThroughDate", "registered_through_date")],
)
@pytest.mark.parametrize("missing", [False, True])
def test_unreported_license_dates_preserve_reported_facts(field, normalized_field, missing):
    source = _profile_body(**{field: None})
    if missing:
        del source[field]
    record, facts = _parse(source)
    assert record["raw_payload"] == source
    assert facts[0]["value_json"]["institution"] == "Example Medical School"
    license_fact = facts[1]
    assert normalized_field not in license_fact["value_json"]
    assert not license_fact["source_json"]["quality_flags"]
    assert (field in license_fact["source_json"]["raw_fields"]) is not missing
    if not missing:
        assert license_fact["source_json"]["raw_fields"][field] == source[field]


@pytest.mark.parametrize("reported", ["February 30, 2001", "Spring 2001", "05/06/2001"])
def test_unrecognized_dates_remain_source_text(reported):
    _, facts = _parse(_profile_body(schoolDegreeDate=reported))
    assert facts[0]["value_json"]["graduation_date"] == reported
    assert facts[0]["value_json"]["graduation_date_precision"] == "source"
    assert "graduation_year" not in facts[0]["value_json"]
    assert "graduation_date_invalid" in facts[0]["source_json"]["quality_flags"]


def test_future_degree_is_not_completed_training():
    _, facts = _parse(_profile_body(schoolDegreeDate="May 20, 2030"))
    assert "graduation_year_in_future" in facts[0]["source_json"]["quality_flags"]
    assert "completed" not in facts[0]["value_json"] and "student" not in facts[0]["value_json"]


@pytest.mark.parametrize("field", list(profile.TEXT_LABELS))
def test_wrong_field_label_or_type_is_rejected(field):
    mutations = [
        {"label": "Unexpected", "value": "text"},
        {"label": profile.TEXT_LABELS[field], "value": 123},
        {"label": profile.TEXT_LABELS[field]},
        None,
    ]
    if field not in profile.OPTIONAL_TEXT_FIELDS:
        mutations.append({"label": profile.TEXT_LABELS[field], "value": None})
    for mutation in mutations:
        source = _profile_body()
        source[field] = mutation
        with pytest.raises(ValueError, match="schema_invalid"):
            _parse(source)


@pytest.mark.parametrize(
    "field,replacement",
    [
        ("professionCode", "040"),
        ("professionCode", 60),
        ("profession", {"label": "Profession", "value": "Pharmacy (040)"}),
        ("licenseNumber", {"label": "License Number", "value": "123456"}),
        ("name", {"label": "Name", "value": "None"}),
        ("index", True),
        ("privileges", {"label": "Additional Qualifications", "value": [None]}),
        ("enforcementActions", {}),
        ("certificateOfAuthorizations", None),
    ],
)
def test_wrong_identity_or_container_is_rejected(field, replacement):
    source = _profile_body()
    source[field] = replacement
    with pytest.raises(ValueError):
        _parse(source)


def test_separate_agency_discipline_and_qualifications_stay_raw():
    source = _profile_body(additionalQualifications="Example qualification")
    source["enforcementActions"] = [{"text": "Example external action"}]
    source["privileges"]["value"] = ["Example qualification"]
    source_record, facts = _parse(source)
    assert source_record["raw_payload"] == source
    assert [fact["category"] for fact in facts] == ["education", "licenses"]


def test_education_uses_existing_corroboration_contract():
    _, facts = _parse(_profile_body())
    state_by_field = {
        "type": "education_history",
        "value": facts[0]["value_json"],
        "source_kinds": ["state_regulator"],
        "source_record_id": "synthetic-nysed-assertion",
        "sensitive": False,
        "public_default": True,
    }
    cms_by_field = {
        "type": "education_history",
        "value": {"institution": "EXAMPLE MEDICAL SCHOOL", "graduation_year": 2001},
        "source_kinds": ["cms_doctors"],
        "source_record_id": "synthetic-cms-assertion",
        "sensitive": False,
        "public_default": True,
    }
    group_by_field = {"items": [state_by_field, cms_by_field]}
    canonicalize_education_category(group_by_field)
    assert len(group_by_field["items"]) == 1
    assert group_by_field["items"][0]["corroborated_fields"] == ["institution", "graduation_year"]
    assert len(group_by_field["items"][0]["assertions"]) == 2


@pytest.mark.parametrize("license_number", ["654321", "000123"])
async def test_acquisition_and_pinned_replay(source_session, license_number):
    session = source_session(SourceResponse(_profile_body(license_number)))
    acquired = await _acquire(session, license_number)
    assert len(session.requests) == 1 and session.closed
    assert session.options["trust_env"] is False and session.options["auto_decompress"] is False
    assert session.options["timeout"].total == 60
    assert isinstance(session.options["cookie_jar"], acquisition.aiohttp.DummyCookieJar)
    assert profile.read_acquisition(session.destination, receipt_sha256=acquired["receipt_sha256"]) == acquired
    for artifact in session.destination.iterdir():
        assert PUBLIC_HEADER.encode() not in artifact.read_bytes()
        assert artifact.stat().st_mode & 0o777 == 0o600
    assert _artifact(session, "request.json")["additional_header_names"] == ["x-oapi-key"]
    assert set(_artifact(session, "request.json")["headers"]) == {"Accept", "Accept-Encoding"}
    assert base64.b64decode(_artifact(session, "response.json")["body_base64"]) == session.response.body


@pytest.mark.parametrize("missing_license_dates", [False, True])
async def test_unreported_education_acquisition_and_replay(source_session, missing_license_dates):
    source = _profile_body(schoolName=None)
    del source["schoolDegreeDate"]
    if missing_license_dates:
        source["dateOfLicensure"]["value"] = None
        del source["registeredThroughDate"]
    session = source_session(SourceResponse(source))
    acquired = await _acquire(session)
    assert len(session.requests) == 1 and session.closed
    assert acquired["outcome"] == "acquired"
    assert acquired["source_record"]["raw_payload"] == source
    assert [fact["category"] for fact in acquired["facts"]] == ["licenses"]
    assert _artifact(session, "result.json")["fact_count"] == 1
    assert profile.read_acquisition(session.destination, receipt_sha256=acquired["receipt_sha256"]) == acquired
    assert base64.b64decode(_artifact(session, "response.json")["body_base64"]) == session.response.body


@pytest.mark.parametrize("field", ["address", "additionalQualifications"])
@pytest.mark.parametrize("missing", [False, True])
async def test_unreported_descriptive_metadata_preserves_facts_and_replay(source_session, field, missing):
    source = _profile_body(**{field: None})
    if missing:
        del source[field]
    session = source_session(SourceResponse(source))
    acquired = await _acquire(session)
    _, expected_facts = _parse(_profile_body())
    assert len(session.requests) == 1 and session.closed
    assert acquired["outcome"] == "acquired"
    assert acquired["source_record"]["raw_payload"] == source
    assert [fact["value_json"] for fact in acquired["facts"]] == [fact["value_json"] for fact in expected_facts]
    assert [fact["category"] for fact in acquired["facts"]] == ["education", "licenses"]
    assert profile.read_acquisition(session.destination, receipt_sha256=acquired["receipt_sha256"]) == acquired
    assert base64.b64decode(_artifact(session, "response.json")["body_base64"]) == session.response.body


@pytest.mark.parametrize(
    "response",
    [
        SourceResponse(status=403),
        SourceResponse(status=302),
        SourceResponse(url="https://example.org/changed"),
        SourceResponse(raw=b"<html>challenge</html>"),
        SourceResponse(raw=b'{"name": 1, "name": 2}'),
        SourceResponse(raw=b'{"index": NaN}'),
        SourceResponse(headers=[]),
        SourceResponse(headers=[("Content-Type", "text/html")]),
        SourceResponse(headers=[("Content-Type", "application/json"), ("content-type", "application/json")]),
        SourceResponse(headers=[("Content-Type", "application/json"), ("Content-Encoding", "gzip")]),
        SourceResponse(
            headers=[
                ("Content-Type", "application/json"),
                ("Content-Encoding", "identity"),
                ("content-encoding", "identity"),
            ]
        ),
    ],
)
async def test_unsafe_responses_fail_once(source_session, response):
    session = source_session(response)
    with pytest.raises(ValueError, match="new_york_nysed_acquisition_failed"):
        await _acquire(session)
    assert session.closed and len(session.requests) == 1
    assert _artifact(session, "result.json")["outcome"] == "failed"


@pytest.mark.parametrize(
    "error", [TimeoutError(PUBLIC_HEADER), RuntimeError(PUBLIC_HEADER), asyncio.CancelledError(PUBLIC_HEADER)]
)
async def test_partial_failures_preserve_evidence_without_key(source_session, error):
    session = source_session(SourceResponse(chunks=[b'{"partial":', error]))
    with pytest.raises((ValueError, asyncio.CancelledError)) as caught:
        await _acquire(session)
    assert PUBLIC_HEADER not in str(caught.value)
    assert session.closed and len(session.requests) == 1
    response = _artifact(session, "response.json")
    assert response["complete"] is False and base64.b64decode(response["body_base64"]) == b'{"partial":'
    assert all(PUBLIC_HEADER.encode() not in path.read_bytes() for path in session.destination.iterdir())


@pytest.mark.parametrize("error", [KeyboardInterrupt(PUBLIC_HEADER), SystemExit(PUBLIC_HEADER), SystemExit(7)])
async def test_control_flow_is_sanitized_and_preserved(source_session, error):
    session = source_session(SourceResponse(chunks=[b'{"partial":', error]))
    with pytest.raises(type(error)) as caught:
        await _acquire(session)
    assert PUBLIC_HEADER not in str(caught.value)
    if isinstance(error, SystemExit):
        assert caught.value.code == (error.code if isinstance(error.code, int) else 1)
    assert session.closed and _artifact(session, "result.json")["outcome"] == "failed"
    assert all(PUBLIC_HEADER.encode() not in path.read_bytes() for path in session.destination.iterdir())


async def test_reflected_header_value_is_discarded(source_session):
    session = source_session(
        SourceResponse(
            raw=PUBLIC_HEADER.encode(), headers=[("Content-Type", "application/json"), ("x-oapi-key", PUBLIC_HEADER)]
        )
    )
    with pytest.raises(ValueError):
        await _acquire(session)
    response = _artifact(session, "response.json")
    assert response["redacted_key_echo"] is True and response["body_base64"] == "" and response["complete"] is False
    assert all(PUBLIC_HEADER.encode() not in path.read_bytes() for path in session.destination.iterdir())


async def test_oversize_response_retains_only_bound(source_session, monkeypatch):
    monkeypatch.setattr(acquisition, "MAX_RESPONSE_BYTES", 16)
    session = source_session(SourceResponse(chunks=[b"a" * 8, b"b" * 16]))
    with pytest.raises(ValueError):
        await _acquire(session)
    response = _artifact(session, "response.json")
    assert response["complete"] is False and response["received_bytes"] == 24
    assert len(base64.b64decode(response["body_base64"])) == 16


@pytest.mark.parametrize("artifact", ["manifest.json", "request.json", "response.json", "result.json"])
async def test_changed_artifact_rejects_original_pin(source_session, artifact):
    session = source_session(SourceResponse())
    acquired = await _acquire(session)
    changed = _artifact(session, artifact)
    changed["unexpected"] = True
    (session.destination / artifact).write_bytes(profile.encoded_json(changed))
    with pytest.raises(ValueError):
        profile.read_acquisition(session.destination, receipt_sha256=acquired["receipt_sha256"])


async def test_response_repin_cannot_hide_incomplete_body(source_session):
    session = source_session(SourceResponse())
    await _acquire(session)
    response = _artifact(session, "response.json")
    response["complete"] = False
    (session.destination / "response.json").write_bytes(profile.encoded_json(response))
    receipt = _artifact(session, "result.json")
    receipt["response_sha256"] = profile._hash(response)
    (session.destination / "result.json").write_bytes(profile.encoded_json(receipt))
    with pytest.raises(ValueError, match="response_incomplete"):
        profile.read_acquisition(session.destination, receipt_sha256=profile._hash(receipt))


@pytest.mark.parametrize(
    "field,replacement",
    [
        ("status", True),
        ("received_bytes", True),
        ("body_base64", "!"),
        ("content_sha256", "0" * 64),
        ("headers", {}),
        ("source_key", "unexpected"),
        ("downloaded_at", "invalid"),
        ("downloaded_at", "2000-01-01T00:00:00+00:00"),
    ],
)
async def test_invalid_replayed_response_is_rejected(source_session, field, replacement):
    session = source_session(SourceResponse())
    await _acquire(session)
    response = _artifact(session, "response.json")
    response[field] = replacement
    (session.destination / "response.json").write_bytes(profile.encoded_json(response))
    receipt = _artifact(session, "result.json")
    receipt["response_sha256"] = profile._hash(response)
    (session.destination / "result.json").write_bytes(profile.encoded_json(receipt))
    with pytest.raises(ValueError):
        profile.read_acquisition(session.destination, receipt_sha256=profile._hash(receipt))


@pytest.mark.parametrize("license_number", ["12345", " 654321", "060654321", "１２３４５６", None, 654321])
async def test_bad_license_creates_no_attempt(source_session, license_number):
    session = source_session(SourceResponse())
    with pytest.raises(ValueError, match="license_invalid"):
        await _acquire(session, license_number)
    assert not session.destination.exists() and not session.requests


@pytest.mark.parametrize("api_key", ["", "key\nvalue", "key\rvalue", None, "é", "x" * 513])
async def test_invalid_key_creates_no_attempt(source_session, api_key):
    session = source_session(SourceResponse())
    with pytest.raises(ValueError, match="api_key_invalid"):
        await acquisition.acquire_license("654321", session.destination, run_id="synthetic-run", api_key=api_key)
    assert not session.destination.exists() and not session.requests


async def test_existing_attempt_is_preserved(source_session):
    session = source_session(SourceResponse())
    session.destination.mkdir()
    sentinel = session.destination / "sentinel"
    sentinel.write_text("keep")
    with pytest.raises(FileExistsError):
        await _acquire(session)
    assert sentinel.read_text() == "keep" and not session.requests


async def test_symlink_attempt_is_rejected(source_session, tmp_path):
    session = source_session(SourceResponse())
    real_directory = tmp_path / "real"
    real_directory.mkdir()
    session.destination.symlink_to(real_directory, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        await _acquire(session)
    assert not list(real_directory.iterdir()) and not session.requests


@pytest.mark.parametrize(
    "headers",
    [
        [],
        [("Content-Type", "application/json")],
        [("Content-Type", "application/json; charset=utf-8"), ("Content-Encoding", "identity")],
    ],
)
async def test_empty_204_retains_explicit_held_outcome(source_session, headers):
    session = source_session(SourceResponse(raw=b"", status=204, headers=headers))
    acquired = await _acquire(session, "000000")
    assert acquired == {
        "outcome": "held",
        "reason": "no_profile_returned",
        "source_record": None,
        "facts": [],
        "receipt_sha256": acquired["receipt_sha256"],
    }
    assert profile.read_held_acquisition(session.destination, receipt_sha256=acquired["receipt_sha256"]) == acquired
    receipt = _artifact(session, "result.json")
    assert receipt["outcome"] == "held" and receipt["reason"] == "no_profile_returned" and receipt["fact_count"] == 0
    response = _artifact(session, "response.json")
    assert response["status"] == 204 and response["complete"] is True and response["received_bytes"] == 0
    assert response["body_base64"] == "" and response["content_sha256"] == hashlib.sha256(b"").hexdigest()
    assert session.closed and len(session.requests) == 1
    assert all(PUBLIC_HEADER.encode() not in path.read_bytes() for path in session.destination.iterdir())
    with pytest.raises(ValueError, match="receipt_changed_or_failed"):
        profile.read_acquisition(session.destination, receipt_sha256=acquired["receipt_sha256"])


async def test_acquired_200_is_not_held(source_session):
    session = source_session(SourceResponse())
    acquired = await _acquire(session)
    with pytest.raises(ValueError, match="receipt_changed_or_failed"):
        profile.read_held_acquisition(session.destination, receipt_sha256=acquired["receipt_sha256"])
    assert profile.read_acquisition(session.destination, receipt_sha256=acquired["receipt_sha256"]) == acquired


@pytest.mark.parametrize(
    "response",
    [SourceResponse(raw=b"", status=status, headers=[]) for status in (200, 403, 404, 500, 503)]
    + [
        SourceResponse(raw=b"unexpected", status=204, headers=[]),
        SourceResponse(raw=b"", status=204, headers=[("Content-Type", "text/html")]),
        SourceResponse(raw=b"", status=204, headers=[("Content-Encoding", "gzip")]),
        SourceResponse(
            raw=b"", status=204, headers=[("Content-Type", "application/json"), ("content-type", "application/json")]
        ),
        SourceResponse(
            raw=b"", status=204, headers=[("Content-Encoding", "identity"), ("content-encoding", "identity")]
        ),
        SourceResponse(status=204, headers=[], chunks=[TimeoutError("synthetic failure")]),
    ],
)
async def test_unproven_negative_responses_remain_failures(source_session, response):
    session = source_session(response)
    with pytest.raises(ValueError, match="acquisition_failed"):
        await _acquire(session)
    assert _artifact(session, "result.json")["outcome"] == "failed"
    assert session.closed and len(session.requests) == 1


@pytest.fixture
async def held_case(source_session):
    session = source_session(SourceResponse(raw=b"", status=204, headers=[]))
    return session, await _acquire(session)


@pytest.mark.parametrize("artifact", ["manifest.json", "request.json", "response.json", "result.json"])
async def test_held_replay_requires_original_artifact_pins(held_case, artifact):
    session, acquired = held_case
    changed_by_field = _artifact(session, artifact)
    changed_by_field["changed"] = True
    (session.destination / artifact).write_bytes(profile.encoded_json(changed_by_field))
    with pytest.raises(ValueError):
        profile.read_held_acquisition(session.destination, receipt_sha256=acquired["receipt_sha256"])


@pytest.mark.parametrize(
    "field,replacement",
    [
        ("complete", False),
        ("error_type", "TimeoutError"),
        ("redacted_key_echo", True),
        ("status", 200),
        ("status", 404),
        ("status", 403),
        ("status", 500),
        ("status", "204"),
        ("received_bytes", 1),
        ("received_bytes", False),
        ("body_base64", "!"),
        ("content_sha256", "0" * 64),
        ("source_key", "unexpected"),
        ("schema_version", "unexpected"),
        ("request_sha256", "0" * 64),
        ("source_url", "https://example.org/changed"),
        ("downloaded_at", "2000-01-01T00:00:00+00:00"),
        ("downloaded_at", "2030-01-01T00:00:00+00:00"),
        ("downloaded_at", "2026-01-01T00:00:00"),
        ("headers", None),
    ],
)
async def test_held_replay_validates_transport_semantics(held_case, field, replacement):
    session, _ = held_case
    response_by_field = _artifact(session, "response.json")
    response_by_field[field] = replacement
    (session.destination / "response.json").write_bytes(profile.encoded_json(response_by_field))
    receipt_by_field = _artifact(session, "result.json")
    receipt_by_field["response_sha256"] = profile._hash(response_by_field)
    (session.destination / "result.json").write_bytes(profile.encoded_json(receipt_by_field))
    with pytest.raises(ValueError):
        profile.read_held_acquisition(session.destination, receipt_sha256=profile._hash(receipt_by_field))


async def test_held_replay_rejects_nonempty_body(held_case):
    session, _ = held_case
    response_by_field = _artifact(session, "response.json")
    response_by_field.update(
        body_base64=base64.b64encode(b"{}").decode(), received_bytes=2, content_sha256=hashlib.sha256(b"{}").hexdigest()
    )
    (session.destination / "response.json").write_bytes(profile.encoded_json(response_by_field))
    receipt_by_field = _artifact(session, "result.json")
    receipt_by_field["response_sha256"] = profile._hash(response_by_field)
    (session.destination / "result.json").write_bytes(profile.encoded_json(receipt_by_field))
    with pytest.raises(ValueError, match="no_content_body_not_empty"):
        profile.read_held_acquisition(session.destination, receipt_sha256=profile._hash(receipt_by_field))


@pytest.mark.parametrize(
    "field,replacement",
    [
        ("reason", "unlicensed"),
        ("fact_count", 1),
        ("fact_count", False),
        ("outcome", "acquired"),
        ("completed_at", "2000-01-01T00:00:00+00:00"),
        ("request_sha256", "0" * 64),
    ],
)
async def test_held_replay_rejects_changed_receipt(held_case, field, replacement):
    session, _ = held_case
    receipt_by_field = _artifact(session, "result.json")
    receipt_by_field[field] = replacement
    (session.destination / "result.json").write_bytes(profile.encoded_json(receipt_by_field))
    with pytest.raises(ValueError):
        profile.read_held_acquisition(session.destination, receipt_sha256=profile._hash(receipt_by_field))


@pytest.mark.parametrize(
    "field,replacement",
    [
        ("license_number", "123456"),
        ("profession_code", "040"),
        ("source_key", "unexpected"),
        ("started_at", "2030-01-01T00:00:00+00:00"),
    ],
)
async def test_held_replay_checks_manifest_identity(held_case, field, replacement):
    session, _ = held_case
    manifest_by_field = _artifact(session, "manifest.json")
    manifest_by_field[field] = replacement
    (session.destination / "manifest.json").write_bytes(profile.encoded_json(manifest_by_field))
    receipt_by_field = _artifact(session, "result.json")
    receipt_by_field["manifest_sha256"] = profile._hash(manifest_by_field)
    (session.destination / "result.json").write_bytes(profile.encoded_json(receipt_by_field))
    with pytest.raises(ValueError):
        profile.read_held_acquisition(session.destination, receipt_sha256=profile._hash(receipt_by_field))
