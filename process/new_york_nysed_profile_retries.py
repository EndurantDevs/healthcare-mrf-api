# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Authenticate bounded, explicit recovery of complete NYSED HTTP 408 responses."""

from __future__ import annotations

import base64
import re
from datetime import timedelta

from process.kentucky_profile_acquisition import _read_artifact, _reject_symlinks
from process.massachusetts_profile_acquisition import encoded_json
from process.massachusetts_profile_rows import _hash

TIMEOUT_DELAYS = (5, 15)
MAX_TIMEOUT_BODY_BYTES = 65536
MAX_TIMEOUT_ENVELOPE_BYTES = 131072
TIMEOUT_SCHEMA_VERSION = "ny-nysed-timeout-retries/v1"
RESPONSE_FIELDS = {
    "schema_version",
    "source_key",
    "source_url",
    "request_sha256",
    "downloaded_at",
    "status",
    "headers",
    "received_bytes",
    "complete",
    "content_sha256",
    "body_base64",
}


def timeout_filename(number):
    """Limit retained paths to the two explicit recovery slots."""
    from process.new_york_nysed_profile import _require

    _require(type(number) is int and 1 <= number <= len(TIMEOUT_DELAYS), "timeout_ordinal_invalid")
    return f"timeout-{number}.response.json"


def validate_timeout_response(response, descriptor):
    """Accept only a complete canonical 408 envelope, never profile absence."""
    from process import new_york_nysed_profile as profile

    profile._require(isinstance(response, dict) and set(response) == RESPONSE_FIELDS, "timeout_envelope_invalid")
    body = profile._validated_response(response, descriptor, expected_status=408)
    profile._require(
        len(body) <= MAX_TIMEOUT_BODY_BYTES
        and response["body_base64"] == base64.b64encode(body).decode("ascii")
        and len(encoded_json(response)) <= MAX_TIMEOUT_ENVELOPE_BYTES,
        "timeout_envelope_too_large_or_changed",
    )
    profile._require(
        all(name.lower() in {"content-type", "content-encoding"} for name, _ in response["headers"]),
        "timeout_headers_invalid",
    )
    profile._timestamp(response["downloaded_at"])


def _timeout_descriptor(response):
    from process import new_york_nysed_profile as profile

    profile._require(
        isinstance(response, dict) and isinstance(response.get("source_url"), str), "timeout_identity_invalid"
    )
    matched = re.fullmatch(
        re.escape(profile.BASE_URL) + r"\?licenseNumber=([0-9]{6})&professionCode=060", response["source_url"]
    )
    profile._require(matched is not None, "timeout_identity_invalid")
    return profile.request_descriptor(matched[1])


def timeout_history(responses):
    """Index original timeout envelopes without changing final response identity."""
    from process import new_york_nysed_profile as profile

    profile._require(isinstance(responses, list) and len(responses) <= len(TIMEOUT_DELAYS), "timeout_history_invalid")
    if not responses:
        return None
    descriptor = _timeout_descriptor(responses[0])
    entries = []
    for number, response in enumerate(responses, 1):
        validate_timeout_response(response, descriptor)
        entries.append(
            {
                "filename": timeout_filename(number),
                "response_sha256": _hash(response),
                "downloaded_at": response["downloaded_at"],
            }
        )
    return {
        "schema_version": TIMEOUT_SCHEMA_VERSION,
        "max_attempts": len(TIMEOUT_DELAYS) + 1,
        "delays_seconds": list(TIMEOUT_DELAYS),
        "responses": entries,
    }


def _timeout_entries(history):
    from process.new_york_nysed_profile import _require

    _require(
        isinstance(history, dict)
        and set(history) == {"schema_version", "max_attempts", "delays_seconds", "responses"}
        and history["schema_version"] == TIMEOUT_SCHEMA_VERSION
        and type(history["max_attempts"]) is int
        and history["max_attempts"] == len(TIMEOUT_DELAYS) + 1
        and history["delays_seconds"] == list(TIMEOUT_DELAYS)
        and all(type(delay) is int for delay in history["delays_seconds"])
        and isinstance(history["responses"], list)
        and 1 <= len(history["responses"]) <= len(TIMEOUT_DELAYS),
        "timeout_history_invalid",
    )
    for number, entry in enumerate(history["responses"], 1):
        _require(
            isinstance(entry, dict)
            and set(entry) == {"filename", "response_sha256", "downloaded_at"}
            and entry["filename"] == timeout_filename(number)
            and isinstance(entry["response_sha256"], str)
            and re.fullmatch(r"[a-f0-9]{64}", entry["response_sha256"]),
            "timeout_index_invalid",
        )
    return history["responses"]


def timeout_files(receipt, manifest, *, final_response=None, file_sha256=None):
    """Validate the receipt-pinned inventory and minimum recovery chronology."""
    from process import new_york_nysed_profile as profile

    profile._require(isinstance(receipt, dict), "timeout_receipt_invalid")
    if "timeout_retries" not in receipt:
        return {}
    entries = _timeout_entries(receipt["timeout_retries"])
    profile._require(isinstance(manifest, dict), "timeout_manifest_invalid")
    earliest_next = profile._timestamp(manifest.get("started_at"))
    extra_limits_by_file = {}
    for number, entry in enumerate(entries, 1):
        downloaded_at = profile._timestamp(entry["downloaded_at"])
        profile._require(downloaded_at >= earliest_next, "timeout_chronology_invalid")
        earliest_next = downloaded_at + timedelta(seconds=TIMEOUT_DELAYS[number - 1])
        extra_limits_by_file[entry["filename"]] = MAX_TIMEOUT_ENVELOPE_BYTES
        if file_sha256 is not None:
            profile._require(
                isinstance(file_sha256, dict) and file_sha256.get(entry["filename"]) == entry["response_sha256"],
                "timeout_file_changed",
            )
    completed_at = profile._timestamp(receipt.get("completed_at"))
    profile._require(completed_at >= earliest_next, "timeout_chronology_invalid")
    if final_response is not None:
        profile._require(isinstance(final_response, dict), "timeout_final_response_invalid")
        profile._require(
            earliest_next <= profile._timestamp(final_response.get("downloaded_at")) <= completed_at,
            "timeout_chronology_invalid",
        )
    return extra_limits_by_file


def read_timeout_responses(destination, manifest, final_response, receipt):
    """Replay every indexed original response using the same request identity."""
    from process import new_york_nysed_profile as profile

    extra_limits_by_file = timeout_files(receipt, manifest, final_response=final_response)
    _reject_symlinks(destination)
    profile._require(destination.is_dir(), "timeout_inventory_invalid")
    retained_paths = list(destination.iterdir())
    profile._require(
        {path.name for path in retained_paths}
        == {"manifest.json", "request.json", "response.json", "result.json"} | set(extra_limits_by_file),
        "timeout_inventory_invalid",
    )
    for path in retained_paths:
        _reject_symlinks(path)
        profile._require(path.is_file(), "timeout_inventory_invalid")
    if not extra_limits_by_file:
        return
    descriptor = profile.request_descriptor(manifest["license_number"])
    for entry in receipt["timeout_retries"]["responses"]:
        response = _read_artifact(destination / entry["filename"], extra_limits_by_file[entry["filename"]])
        profile._require(
            _hash(response) == entry["response_sha256"] and response.get("downloaded_at") == entry["downloaded_at"],
            "timeout_response_changed",
        )
        validate_timeout_response(response, descriptor)
