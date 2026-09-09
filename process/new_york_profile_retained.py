# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Revalidate retained NY acquisition evidence without network access or NPI binding."""

from __future__ import annotations

import base64
import binascii
import hashlib
import re
from datetime import datetime, timedelta
from pathlib import Path

from process.kentucky_profile_acquisition import _read_artifact
from process.new_york_profile_acquisition import (
    ACQUISITION_SCHEMA, MAX_PROFILE_BYTES, PROFILE_URL, SEARCH_URL, SOURCE_KEY,
    _education_result, _request, _require, _search_fields, _search_result, _validated_headers, encoded_json,
)

MAX_METADATA_BYTES = 64 * 1024
MAX_RESPONSE_ENVELOPE_BYTES = MAX_PROFILE_BYTES * 2


def _utc_timestamp(value):
    try:
        timestamp = datetime.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("new_york_acquisition_timestamp_invalid") from exc
    _require(timestamp.utcoffset() == timedelta(0), "timestamp_invalid")
    return timestamp


def _manifest(destination, manifest_sha256):
    _require(isinstance(manifest_sha256, str) and re.fullmatch(r"[0-9a-f]{64}", manifest_sha256), "manifest_hash_invalid")
    manifest_by_field = _read_artifact(destination / "manifest.json", MAX_METADATA_BYTES)
    _require(hashlib.sha256(encoded_json(manifest_by_field)).hexdigest() == manifest_sha256, "manifest_changed")
    _require(set(manifest_by_field) == {"schema_version", "source_key", "license_number", "run_id", "session_id", "started_at"}
             and manifest_by_field["schema_version"] == ACQUISITION_SCHEMA
             and manifest_by_field["source_key"] == SOURCE_KEY, "manifest_invalid")
    _require(isinstance(manifest_by_field["license_number"], str)
             and re.fullmatch(r"[0-9]{6}", manifest_by_field["license_number"]), "license_invalid")
    _require(isinstance(manifest_by_field["run_id"], str) and manifest_by_field["run_id"].strip(), "run_id_invalid")
    _require(isinstance(manifest_by_field["session_id"], str)
             and re.fullmatch(r"[0-9a-f]{32}", manifest_by_field["session_id"]), "session_invalid")
    _utc_timestamp(manifest_by_field["started_at"])
    return manifest_by_field


def _response_body(response_by_field):
    _require(isinstance(response_by_field.get("body_base64"), str), "body_invalid")
    try:
        body = base64.b64decode(response_by_field["body_base64"], validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ValueError("new_york_acquisition_body_invalid") from exc
    _require(0 < len(body) <= MAX_PROFILE_BYTES
             and type(response_by_field.get("received_bytes")) is int
             and response_by_field["received_bytes"] == len(body)
             and response_by_field.get("content_sha256") == hashlib.sha256(body).hexdigest(), "body_changed")
    return body


def _response_headers(response_by_field):
    header_pairs = response_by_field.get("headers")
    _require(isinstance(header_pairs, list)
             and all(isinstance(pair, list) and len(pair) == 2
                     and all(isinstance(value, str) for value in pair) for pair in header_pairs), "headers_invalid")
    _validated_headers(header_pairs)


def _response(destination, stage, expected_request):
    request_by_field = _read_artifact(destination / f"{stage}.request.json", MAX_METADATA_BYTES)
    _require(encoded_json(request_by_field) == encoded_json(expected_request), "request_changed")
    response_by_field = _read_artifact(destination / f"{stage}.response.json", MAX_RESPONSE_ENVELOPE_BYTES)
    _require(response_by_field.get("schema_version") == ACQUISITION_SCHEMA
             and response_by_field.get("source_key") == SOURCE_KEY
             and response_by_field.get("source_url") == expected_request["source_url"]
             and response_by_field.get("request_sha256") == hashlib.sha256(encoded_json(request_by_field)).hexdigest(),
             "response_identity_invalid")
    _require(response_by_field.get("complete") is True and "error_type" not in response_by_field, "response_incomplete")
    _require(type(response_by_field.get("status")) is int and response_by_field["status"] == 200, "http_failure")
    _utc_timestamp(response_by_field.get("downloaded_at"))
    _response_headers(response_by_field)
    return _response_body(response_by_field), response_by_field


def read_acquisition(destination: Path, *, manifest_sha256: str) -> dict:
    """Rebuild an acquired attempt using a manifest digest pinned in its original receipt.

    Internal hashes prove consistency, not authenticity against replacement of all evidence.
    """
    manifest_by_field = _manifest(destination, manifest_sha256)
    summary_by_field = _read_artifact(destination / "result.json", MAX_METADATA_BYTES)
    _require(summary_by_field.get("outcome") == "acquired", "not_acquired")
    session_id = manifest_by_field["session_id"]
    search_request = _request("POST", SEARCH_URL, session_id, _search_fields(manifest_by_field["license_number"]))
    body, search_response = _response(destination, "search", search_request)
    physician_by_field, _ = _search_result(body)
    _require(physician_by_field is not None, "search_not_singleton")
    physician_id = physician_by_field["physicianID"]
    source_url = PROFILE_URL + physician_id + f"?sections=EDUCATIONALL&physicianId={physician_id}"
    body, profile_response = _response(destination, "education", _request("GET", source_url, session_id))
    _require(_utc_timestamp(manifest_by_field["started_at"]) <= _utc_timestamp(search_response["downloaded_at"])
             <= _utc_timestamp(profile_response["downloaded_at"]), "chronology_invalid")
    result = _education_result(manifest_by_field, physician_by_field, search_response, body, profile_response)
    rebuilt_summary_by_field = {key: value for key, value in result.items() if key not in {"source_record", "facts"}}
    rebuilt_summary_by_field["fact_count"] = len(result["facts"])
    _require(encoded_json(summary_by_field) == encoded_json(rebuilt_summary_by_field), "result_changed")
    return result
