# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import base64
import copy
import hashlib
import socket

import pytest

from process import new_york_nysed_profile as profile
from process import new_york_nysed_profile_retries as retries

DESCRIPTOR = profile.request_descriptor("654321")
MANIFEST = {"license_number": "654321", "started_at": "2026-09-11T00:00:00+00:00"}


@pytest.fixture(autouse=True)
def deny_network(monkeypatch):
    def reject(*_args, **_kwargs):
        raise AssertionError("Synthetic checks forbid network access")

    monkeypatch.setattr(socket.socket, "connect", reject)
    monkeypatch.setattr(socket.socket, "connect_ex", reject)
    monkeypatch.setattr(socket, "create_connection", reject)


def _response(second=1, body=b"Synthetic request exceeded its time limit; retry later.", **changes):
    response_by_field = {
        "schema_version": profile.SCHEMA_VERSION,
        "source_key": profile.SOURCE_KEY,
        "source_url": DESCRIPTOR["source_url"],
        "request_sha256": profile._hash(DESCRIPTOR),
        "downloaded_at": f"2026-09-11T00:00:{second:02d}+00:00",
        "status": 408,
        "headers": [["Content-Type", "application/json"]],
        "received_bytes": len(body),
        "complete": True,
        "content_sha256": hashlib.sha256(body).hexdigest(),
        "body_base64": base64.b64encode(body).decode("ascii"),
    }
    response_by_field.update(changes)
    return response_by_field


def _receipt(count=2):
    return {
        "completed_at": "2026-09-11T00:00:25+00:00",
        "timeout_retries": retries.timeout_history([_response(second) for second in (1, 6)[:count]]),
    }


def _write_responses(destination, receipt):
    for filename in ("manifest.json", "request.json", "response.json", "result.json"):
        (destination / filename).write_bytes(b"{}")
    for second, entry in zip((1, 6), receipt["timeout_retries"]["responses"], strict=True):
        (destination / entry["filename"]).write_bytes(profile.encoded_json(_response(second)))


@pytest.mark.parametrize("number", [None, True, False, 0, 3, 1.0, "1", -1])
def test_timeout_slots_reject_invalid_ordinals(number):
    with pytest.raises(ValueError, match="timeout_ordinal_invalid"):
        retries.timeout_filename(number)


def test_timeout_history_retains_original_identity():
    responses = [_response(1), _response(6)]
    original = copy.deepcopy(responses)
    history = retries.timeout_history(responses)
    assert history == {
        "schema_version": "ny-nysed-timeout-retries/v1",
        "max_attempts": 3,
        "delays_seconds": [5, 15],
        "responses": [
            {
                "filename": f"timeout-{number}.response.json",
                "response_sha256": profile._hash(response),
                "downloaded_at": response["downloaded_at"],
            }
            for number, response in enumerate(responses, 1)
        ],
    }
    assert responses == original and DESCRIPTOR["retry"] is False


@pytest.mark.parametrize("responses", [None, {}, (), [_response()] * 3])
def test_history_rejects_invalid_or_exhausted_input(responses):
    with pytest.raises(ValueError, match="timeout_history_invalid"):
        retries.timeout_history(responses)


@pytest.mark.parametrize(
    "response", [None, {}, _response(source_url=None), _response(source_url="https://example.invalid/")]
)
def test_history_requires_exact_request_identity(response):
    with pytest.raises(ValueError, match="timeout_identity_invalid"):
        retries.timeout_history([response])


def test_history_rejects_mixed_license_responses():
    other_descriptor = profile.request_descriptor("123456")
    different = _response(6, source_url=other_descriptor["source_url"], request_sha256=profile._hash(other_descriptor))
    with pytest.raises(ValueError, match="response_identity_invalid"):
        retries.timeout_history([_response(), different])


@pytest.mark.parametrize(
    "changes",
    [
        {"status": 200},
        {"status": 204},
        {"status": 429},
        {"status": 500},
        {"status": 408.0},
        {"complete": False},
        {"complete": 1},
        {"error_type": "TimeoutError"},
        {"redacted_key_echo": False},
        {"timeout_retries": {}},
        {"schema_version": "invalid"},
        {"source_key": "invalid"},
        {"source_url": DESCRIPTOR["source_url"] + "&extra=1"},
        {"request_sha256": "0" * 64},
        {"body_base64": "invalid"},
        {"content_sha256": "0" * 64},
        {"received_bytes": True},
        {"headers": [["Content-Type", "text/plain"]]},
        {"headers": [["Content-Type", "application/json"], ["Content-Type", "application/json"]]},
        {"headers": [["Content-Type", "application/json"], ["Content-Encoding", "gzip"]]},
        {"headers": [["Content-Type", "application/json"], ["Unexpected", "metadata"]]},
        {"downloaded_at": None},
        {"downloaded_at": "invalid"},
        {"downloaded_at": "2026-09-11T00:00:01"},
        {"downloaded_at": "2026-09-11T00:00:01+01:00"},
    ],
)
def test_only_complete_canonical_timeouts_are_eligible(changes):
    with pytest.raises(ValueError):
        retries.validate_timeout_response(_response(**changes), DESCRIPTOR)


def test_timeout_response_size_boundaries():
    assert retries.validate_timeout_response(_response(body=b"x" * 65536), DESCRIPTOR) is None
    with pytest.raises(ValueError, match="timeout_envelope_too_large_or_changed"):
        retries.validate_timeout_response(_response(body=b"x" * 65537), DESCRIPTOR)
    with pytest.raises(ValueError, match="timeout_envelope_too_large_or_changed"):
        retries.validate_timeout_response(
            _response(headers=[["Content-Type", "application/json; charset=" + "x" * 131072]]), DESCRIPTOR
        )


def test_timeout_requires_canonical_base64():
    with pytest.raises(ValueError, match="timeout_envelope_too_large_or_changed"):
        retries.validate_timeout_response(_response(body=b"x", body_base64="eB=="), DESCRIPTOR)


def test_legacy_receipts_need_no_retry_files(tmp_path):
    assert retries.timeout_history([]) is None
    assert retries.timeout_files({}, {}) == {}
    for filename in ("manifest.json", "request.json", "response.json", "result.json"):
        (tmp_path / filename).write_bytes(b"{}")
    assert retries.read_timeout_responses(tmp_path, {}, {}, {}) is None


@pytest.mark.parametrize(
    "changes",
    [
        {"schema_version": "invalid"},
        {"max_attempts": 4},
        {"max_attempts": 3.0},
        {"delays_seconds": [5.0, 15]},
        {"delays_seconds": [5, 10]},
        {"delays_seconds": None},
        {"responses": []},
        {"responses": {}},
        {"extra": "metadata"},
    ],
)
def test_retry_policy_is_fixed(changes):
    receipt = _receipt()
    receipt["timeout_retries"].update(changes)
    with pytest.raises(ValueError, match="timeout_history_invalid"):
        retries.timeout_files(receipt, MANIFEST)


@pytest.mark.parametrize("history", [None, [], {}])
def test_explicit_empty_history_is_invalid(history):
    with pytest.raises(ValueError, match="timeout_history_invalid"):
        retries.timeout_files({"timeout_retries": history}, MANIFEST)


@pytest.mark.parametrize(
    "changes",
    [
        {"filename": "../timeout-1.response.json"},
        {"filename": "timeout-2.response.json"},
        {"response_sha256": "A" * 64},
        {"response_sha256": None},
        {"response_sha256": "a" * 63},
        {"nested": {}},
    ],
)
def test_retry_index_rejects_mutation(changes):
    receipt = _receipt()
    receipt["timeout_retries"]["responses"][0].update(changes)
    with pytest.raises(ValueError, match="timeout_index_invalid"):
        retries.timeout_files(receipt, MANIFEST)


@pytest.mark.parametrize("replacement", [None, [], {}, 1])
def test_retry_index_requires_objects(replacement):
    receipt = _receipt()
    receipt["timeout_retries"]["responses"][0] = replacement
    with pytest.raises(ValueError, match="timeout_index_invalid"):
        retries.timeout_files(receipt, MANIFEST)


def test_retry_chronology_enforces_both_delays():
    receipt = _receipt()
    assert retries.timeout_files(receipt, MANIFEST, final_response=_response(21)) == {
        "timeout-1.response.json": 131072,
        "timeout-2.response.json": 131072,
    }
    for second in (5, 0):
        changed = copy.deepcopy(receipt)
        changed["timeout_retries"]["responses"][1]["downloaded_at"] = _response(second)["downloaded_at"]
        with pytest.raises(ValueError, match="timeout_chronology_invalid"):
            retries.timeout_files(changed, MANIFEST)
    for second in (20, 26):
        with pytest.raises(ValueError, match="timeout_chronology_invalid"):
            retries.timeout_files(receipt, MANIFEST, final_response=_response(second))


def test_retry_window_includes_start_and_completion():
    receipt = _receipt(count=1)
    assert retries.timeout_files(receipt, MANIFEST, final_response=_response(6))
    with pytest.raises(ValueError, match="timeout_chronology_invalid"):
        retries.timeout_files(receipt, {**MANIFEST, "started_at": _response(2)["downloaded_at"]})
    receipt["completed_at"] = _response(5)["downloaded_at"]
    with pytest.raises(ValueError, match="timeout_chronology_invalid"):
        retries.timeout_files(receipt, MANIFEST)


@pytest.mark.parametrize("invalid", [None, [], "invalid"])
def test_retry_context_requires_objects(invalid):
    with pytest.raises(ValueError, match="timeout_receipt_invalid"):
        retries.timeout_files(invalid, MANIFEST)
    with pytest.raises(ValueError, match="timeout_manifest_invalid"):
        retries.timeout_files(_receipt(), invalid)
    if invalid is not None:
        with pytest.raises(ValueError, match="timeout_final_response_invalid"):
            retries.timeout_files(_receipt(), MANIFEST, final_response=invalid)


def test_retry_inventory_requires_matching_hashes():
    receipt = _receipt()
    sha256_by_file = {entry["filename"]: entry["response_sha256"] for entry in receipt["timeout_retries"]["responses"]}
    assert set(retries.timeout_files(receipt, MANIFEST, file_sha256=sha256_by_file)) == set(sha256_by_file)
    for changed in ({}, [], {**sha256_by_file, "timeout-1.response.json": "0" * 64}):
        with pytest.raises(ValueError, match="timeout_file_changed"):
            retries.timeout_files(receipt, MANIFEST, file_sha256=changed)


def test_retained_history_replays_without_requests(tmp_path):
    receipt = _receipt()
    _write_responses(tmp_path, receipt)
    assert retries.read_timeout_responses(tmp_path, MANIFEST, _response(21), receipt) is None


@pytest.mark.parametrize("mutation", ["body", "timestamp", "status", "license"])
def test_retained_history_rejects_changed_evidence(tmp_path, mutation):
    receipt = _receipt()
    _write_responses(tmp_path, receipt)
    changed = _response()
    if mutation == "body":
        changed = _response(body=b"Changed retained response")
    elif mutation == "timestamp":
        changed["downloaded_at"] = _response(2)["downloaded_at"]
        receipt["timeout_retries"]["responses"][0]["response_sha256"] = profile._hash(changed)
    elif mutation == "status":
        changed["status"] = 200
        receipt["timeout_retries"]["responses"][0]["response_sha256"] = profile._hash(changed)
    else:
        different = profile.request_descriptor("123456")
        changed.update(source_url=different["source_url"], request_sha256=profile._hash(different))
        receipt["timeout_retries"]["responses"][0]["response_sha256"] = profile._hash(changed)
    (tmp_path / "timeout-1.response.json").write_bytes(profile.encoded_json(changed))
    with pytest.raises(ValueError):
        retries.read_timeout_responses(tmp_path, MANIFEST, _response(21), receipt)


@pytest.mark.parametrize("mutation", ["missing", "oversize", "symlink"])
def test_retained_history_requires_bounded_regular_files(tmp_path, mutation):
    receipt = _receipt()
    _write_responses(tmp_path, receipt)
    timeout_path = tmp_path / "timeout-1.response.json"
    if mutation == "missing":
        timeout_path.unlink()
    elif mutation == "oversize":
        timeout_path.write_bytes(b" " * (retries.MAX_TIMEOUT_ENVELOPE_BYTES + 1))
    else:
        timeout_path.rename(tmp_path / "original.json")
        timeout_path.symlink_to(tmp_path / "original.json")
    with pytest.raises(ValueError, match="kentucky_profile_artifact_|timeout_inventory_invalid"):
        retries.read_timeout_responses(tmp_path, MANIFEST, _response(21), receipt)


@pytest.mark.parametrize("mutation", ["extra", "removed_history", "truncated_history", "directory", "absent"])
def test_replay_rejects_unaccounted_capture_files(tmp_path, mutation):
    receipt = _receipt()
    _write_responses(tmp_path, receipt)
    destination = tmp_path
    if mutation == "extra":
        (tmp_path / "timeout-3.response.json").write_bytes(b"{}")
    elif mutation == "removed_history":
        del receipt["timeout_retries"]
    elif mutation == "truncated_history":
        receipt["timeout_retries"]["responses"].pop()
    elif mutation == "directory":
        (tmp_path / "request.json").unlink()
        (tmp_path / "request.json").mkdir()
    else:
        destination = tmp_path / "absent"
    with pytest.raises(ValueError, match="timeout_inventory_invalid"):
        retries.read_timeout_responses(destination, MANIFEST, _response(21), receipt)
