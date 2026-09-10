# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import base64
import hashlib
import json

import pytest

from process import new_york_profile_retained as retained
from process.new_york_profile_acquisition import encoded_json
from tests.test_new_york_profile_acquisition import SourceResponse, _acquire, _education, _search, source_session


def _change(destination, name, **changes):
    path = destination / name
    value = json.loads(path.read_bytes())
    value.update(changes)
    path.write_bytes(encoded_json(value))


def _body(destination, stage, payload):
    body = encoded_json(payload)
    _change(destination, f"{stage}.response.json", body_base64=base64.b64encode(body).decode("ascii"),
            content_sha256=hashlib.sha256(body).hexdigest(), received_bytes=len(body))


@pytest.fixture
async def acquisition_path(source_session):
    session = source_session(SourceResponse(_search()), SourceResponse(_education()))
    await _acquire(session)
    return session.destination


@pytest.fixture
def manifest_sha256(acquisition_path):
    return hashlib.sha256((acquisition_path / "manifest.json").read_bytes()).hexdigest()


@pytest.mark.parametrize("disagree", [False, True])
async def test_retained_result_matches_live_and_keeps_source_names_unbound(source_session, disagree):
    search = _search(physicianFirstName="Example", physicianLastName="Alex") if disagree else _search()
    session = source_session(SourceResponse(search), SourceResponse(_education()))
    live = await _acquire(session)
    manifest_sha256 = hashlib.sha256((session.destination / "manifest.json").read_bytes()).hexdigest()
    replayed = retained.read_acquisition(session.destination, manifest_sha256=manifest_sha256)
    assert replayed == live
    assert replayed["identity_review_required"] is disagree
    assert replayed["source_record"]["matched_npi"] is None
    assert all(fact["npi"] is None and fact["published_at"] is None for fact in replayed["facts"])
    assert len(session.requests) == 2


async def test_complete_profile_without_education_is_reparsed(source_session):
    education = _education()
    education["data"]["medSchools"] = []
    session = source_session(SourceResponse(_search()), SourceResponse(education))
    live = await _acquire(session)
    manifest_sha256 = hashlib.sha256((session.destination / "manifest.json").read_bytes()).hexdigest()
    assert retained.read_acquisition(session.destination, manifest_sha256=manifest_sha256) == live
    assert live["facts"] == [] and live["source_record"] is not None


@pytest.mark.parametrize("changes", [
    {"schema_version": "other"}, {"source_key": "other"}, {"license_number": "12345"},
    {"license_number": "123456"}, {"run_id": ""}, {"run_id": "different-valid-run"},
    {"session_id": "x"}, {"session_id": "0" * 32},
    {"started_at": "invalid"}, {"started_at": "2020-01-01T00:00:00"},
    {"started_at": "2000-01-01T00:00:00+00:00"},
    {"started_at": "2999-01-01T00:00:00+00:00"}, {"unexpected": True},
])
async def test_changed_manifest_cannot_rebind_retained_evidence(acquisition_path, manifest_sha256, changes):
    _change(acquisition_path, "manifest.json", **changes)
    with pytest.raises(ValueError):
        retained.read_acquisition(acquisition_path, manifest_sha256=manifest_sha256)


@pytest.mark.parametrize("digest", [None, 1, "", "a" * 63, "A" * 64, "g" * 64, "0" * 64])
async def test_missing_invalid_or_wrong_manifest_pin_is_rejected(acquisition_path, digest):
    with pytest.raises(ValueError, match="manifest_(hash_invalid|changed)"):
        retained.read_acquisition(acquisition_path, manifest_sha256=digest)


@pytest.mark.parametrize("stage", ["search", "education"])
@pytest.mark.parametrize("changes", [
    {"method": "PUT"}, {"source_url": "https://example.test/"}, {"body_text": "{}"},
    {"allow_redirects": True}, {"allow_redirects": 0}, {"headers": {}}, {"unexpected": True},
])
async def test_changed_request_is_rejected_even_with_recomputed_hash(acquisition_path, manifest_sha256, stage, changes):
    _change(acquisition_path, f"{stage}.request.json", **changes)
    request = json.loads((acquisition_path / f"{stage}.request.json").read_bytes())
    _change(acquisition_path, f"{stage}.response.json", request_sha256=hashlib.sha256(encoded_json(request)).hexdigest())
    with pytest.raises(ValueError, match="request_changed"):
        retained.read_acquisition(acquisition_path, manifest_sha256=manifest_sha256)


@pytest.mark.parametrize("stage", ["search", "education"])
@pytest.mark.parametrize("changes", [
    {"schema_version": "other"}, {"source_key": "other"}, {"source_url": "https://example.test/"},
    {"request_sha256": "0" * 64}, {"complete": False}, {"complete": 1}, {"error_type": "TimeoutError"},
    {"status": 500}, {"status": True}, {"downloaded_at": None}, {"downloaded_at": "2020-01-01"},
    {"received_bytes": 1}, {"received_bytes": True}, {"content_sha256": "0" * 64},
    {"body_base64": "!"}, {"body_base64": "é"}, {"body_base64": ""}, {"body_base64": None},
    {"headers": None}, {"headers": [["Content-Type"]]}, {"headers": [["Content-Type", 1]]},
    {"headers": []}, {"headers": [["Content-Type", "text/html"]]},
    {"headers": [["Content-Type", "application/json"], ["Content-Encoding", "gzip"]]},
    {"headers": [["Content-Type", "application/json"], ["content-type", "text/html"]]},
    {"headers": [["Content-Type", "application/json"], ["Content-Encoding", "identity"], ["content-encoding", "gzip"]]},
])
async def test_response_tampering_and_partial_evidence_are_rejected(acquisition_path, manifest_sha256, stage, changes):
    _change(acquisition_path, f"{stage}.response.json", **changes)
    with pytest.raises(ValueError):
        retained.read_acquisition(acquisition_path, manifest_sha256=manifest_sha256)


@pytest.mark.parametrize("stage", ["search", "education"])
async def test_chronology_is_checked_between_stages(acquisition_path, manifest_sha256, stage):
    _change(acquisition_path, f"{stage}.response.json", downloaded_at="2000-01-01T00:00:00+00:00")
    with pytest.raises(ValueError, match="chronology_invalid"):
        retained.read_acquisition(acquisition_path, manifest_sha256=manifest_sha256)


@pytest.mark.parametrize("changes", [
    {"outcome": "held"}, {"outcome": "failed"}, {"identity_review_required": True},
    {"identity_review_required": 0}, {"physician_id": "91002"}, {"fact_count": 0},
    {"fact_count": True}, {"npi": 1000000004},
])
async def test_terminal_summary_is_not_authority_for_records_or_flags(acquisition_path, manifest_sha256, changes):
    _change(acquisition_path, "result.json", **changes)
    with pytest.raises(ValueError):
        retained.read_acquisition(acquisition_path, manifest_sha256=manifest_sha256)


async def test_raw_name_disagreement_is_recomputed(acquisition_path, manifest_sha256):
    _body(acquisition_path, "search", _search(physicianFirstName="Example", physicianLastName="Alex"))
    with pytest.raises(ValueError, match="result_changed"):
        retained.read_acquisition(acquisition_path, manifest_sha256=manifest_sha256)
    _change(acquisition_path, "result.json", identity_review_required=True)
    result = retained.read_acquisition(acquisition_path, manifest_sha256=manifest_sha256)
    assert result["source_record"]["match_evidence"]["license_search"]["header_names_agree"] is False
    assert result["source_record"]["normalized_payload"]["quality_flags"] == ["search_header_name_disagreement"]
    assert result["facts"][0]["source_json"]["quality_flags"] == ["search_header_name_disagreement"]


@pytest.mark.parametrize("total", [0, 2, 11])
async def test_forged_acquired_summary_cannot_promote_nonsingleton_search(acquisition_path, manifest_sha256, total):
    _body(acquisition_path, "search", _search(total))
    with pytest.raises(ValueError, match="search_not_singleton"):
        retained.read_acquisition(acquisition_path, manifest_sha256=manifest_sha256)


@pytest.mark.parametrize("stage", ["search", "education"])
async def test_json_error_envelope_cannot_become_an_empty_success(acquisition_path, manifest_sha256, stage):
    _body(acquisition_path, stage, {"status": 400, "error": ["synthetic failure"], "errorMessage": None, "data": None})
    with pytest.raises(ValueError, match="unsuccessful"):
        retained.read_acquisition(acquisition_path, manifest_sha256=manifest_sha256)


@pytest.mark.parametrize("stage", ["search", "education"])
@pytest.mark.parametrize("body", [b'{"status":200,"status":400}', b'{"status":NaN}', b"\xff"])
async def test_strict_source_json_is_checked_after_byte_hashes(acquisition_path, manifest_sha256, stage, body):
    _change(acquisition_path, f"{stage}.response.json", body_base64=base64.b64encode(body).decode("ascii"),
            content_sha256=hashlib.sha256(body).hexdigest(), received_bytes=len(body))
    with pytest.raises((ValueError, RuntimeError)):
        retained.read_acquisition(acquisition_path, manifest_sha256=manifest_sha256)


@pytest.mark.parametrize("identity", [{"licenseNumber": "123456"}, {"physicianID": 91002}])
async def test_reparse_rejects_different_header_identity(acquisition_path, manifest_sha256, identity):
    _body(acquisition_path, "education", _education(**identity))
    with pytest.raises(ValueError, match="identity_mismatch"):
        retained.read_acquisition(acquisition_path, manifest_sha256=manifest_sha256)


@pytest.mark.parametrize("name", ["manifest.json", "result.json", "search.request.json", "search.response.json",
                                 "education.request.json", "education.response.json"])
async def test_missing_or_symlinked_artifacts_are_not_read(acquisition_path, manifest_sha256, name):
    path = acquisition_path / name
    original = path.read_bytes()
    path.unlink()
    with pytest.raises(ValueError, match="artifact_file_invalid"):
        retained.read_acquisition(acquisition_path, manifest_sha256=manifest_sha256)
    target = acquisition_path / "synthetic-target.json"
    target.write_bytes(original)
    path.symlink_to(target)
    with pytest.raises(ValueError, match="symlink"):
        retained.read_acquisition(acquisition_path, manifest_sha256=manifest_sha256)


async def test_symlinked_parent_is_rejected(acquisition_path, manifest_sha256):
    alias = acquisition_path.parent / "alias"
    alias.symlink_to(acquisition_path, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        retained.read_acquisition(alias, manifest_sha256=manifest_sha256)


@pytest.mark.parametrize("name,limit", [("manifest.json", "MAX_METADATA_BYTES"),
                                       ("education.response.json", "MAX_RESPONSE_ENVELOPE_BYTES")])
async def test_envelope_is_bounded_before_decoding(acquisition_path, manifest_sha256, monkeypatch, name, limit):
    read_artifact = retained._read_artifact
    capped_paths = []

    def read_with_target_cap(path, max_bytes):
        if path.name == name:
            assert max_bytes == getattr(retained, limit)
            capped_paths.append(path.name)
            max_bytes = 1
        return read_artifact(path, max_bytes)

    monkeypatch.setattr(retained, "_read_artifact", read_with_target_cap)
    with pytest.raises(ValueError, match="artifact_file_invalid"):
        retained.read_acquisition(acquisition_path, manifest_sha256=manifest_sha256)
    assert capped_paths == [name]


async def test_decoded_body_limit_is_independent(acquisition_path, manifest_sha256, monkeypatch):
    monkeypatch.setattr(retained, "MAX_PROFILE_BYTES", 1)
    with pytest.raises(ValueError, match="body_changed"):
        retained.read_acquisition(acquisition_path, manifest_sha256=manifest_sha256)
