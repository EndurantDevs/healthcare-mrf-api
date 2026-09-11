# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Revalidate retained NY acquisition evidence without network access or NPI binding."""

from __future__ import annotations

import base64
import binascii
import hashlib
import re
from datetime import datetime, timedelta
from pathlib import Path

from process.kentucky_profile_acquisition import _read_artifact, _reject_symlinks
from process.new_york_profile_acquisition import (
    ACQUISITION_SCHEMA,
    MAX_PROFILE_BYTES,
    PROFILE_URL,
    SEARCH_URL,
    SOURCE_KEY,
    _education_result,
    _request,
    _require,
    _search_fields,
    _search_result,
    _validated_headers,
    encoded_json,
)
from process.provider_directory_projection_json import decoded_json_object

MAX_METADATA_BYTES = 64 * 1024
MAX_RESPONSE_ENVELOPE_BYTES = MAX_PROFILE_BYTES * 2
HELD_FILES = {
    "manifest.json": MAX_METADATA_BYTES,
    "result.json": MAX_METADATA_BYTES,
    "search.request.json": MAX_METADATA_BYTES,
    "search.response.json": MAX_RESPONSE_ENVELOPE_BYTES,
}


def _utc_timestamp(value):
    try:
        timestamp = datetime.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("new_york_acquisition_timestamp_invalid") from exc
    _require(timestamp.utcoffset() == timedelta(0), "timestamp_invalid")
    return timestamp


def _manifest(destination, manifest_sha256):
    return _validated_manifest(_read_artifact(destination / "manifest.json", MAX_METADATA_BYTES), manifest_sha256)


def _validated_manifest(manifest_by_field, manifest_sha256):
    _require(
        isinstance(manifest_sha256, str) and re.fullmatch(r"[0-9a-f]{64}", manifest_sha256), "manifest_hash_invalid"
    )
    _require(hashlib.sha256(encoded_json(manifest_by_field)).hexdigest() == manifest_sha256, "manifest_changed")
    _require(
        set(manifest_by_field)
        == {"schema_version", "source_key", "license_number", "run_id", "session_id", "started_at"}
        and manifest_by_field["schema_version"] == ACQUISITION_SCHEMA
        and manifest_by_field["source_key"] == SOURCE_KEY,
        "manifest_invalid",
    )
    _require(
        isinstance(manifest_by_field["license_number"], str)
        and re.fullmatch(r"[0-9]{6}", manifest_by_field["license_number"]),
        "license_invalid",
    )
    _require(isinstance(manifest_by_field["run_id"], str) and manifest_by_field["run_id"].strip(), "run_id_invalid")
    _require(
        isinstance(manifest_by_field["session_id"], str)
        and re.fullmatch(r"[0-9a-f]{32}", manifest_by_field["session_id"]),
        "session_invalid",
    )
    _utc_timestamp(manifest_by_field["started_at"])
    return manifest_by_field


def _response_body(response_by_field):
    _require(isinstance(response_by_field.get("body_base64"), str), "body_invalid")
    try:
        body = base64.b64decode(response_by_field["body_base64"], validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ValueError("new_york_acquisition_body_invalid") from exc
    _require(
        0 < len(body) <= MAX_PROFILE_BYTES
        and type(response_by_field.get("received_bytes")) is int
        and response_by_field["received_bytes"] == len(body)
        and response_by_field.get("content_sha256") == hashlib.sha256(body).hexdigest(),
        "body_changed",
    )
    return body


def _response_headers(response_by_field):
    header_pairs = response_by_field.get("headers")
    _require(
        isinstance(header_pairs, list)
        and all(
            isinstance(pair, list) and len(pair) == 2 and all(isinstance(value, str) for value in pair)
            for pair in header_pairs
        ),
        "headers_invalid",
    )
    _validated_headers(header_pairs)


def _response(destination, stage, expected_request):
    request_by_field = _read_artifact(destination / f"{stage}.request.json", MAX_METADATA_BYTES)
    response_by_field = _read_artifact(destination / f"{stage}.response.json", MAX_RESPONSE_ENVELOPE_BYTES)
    return _validated_response(request_by_field, response_by_field, expected_request)


def _validated_response(request_by_field, response_by_field, expected_request):
    _require(encoded_json(request_by_field) == encoded_json(expected_request), "request_changed")
    _require(
        response_by_field.get("schema_version") == ACQUISITION_SCHEMA
        and response_by_field.get("source_key") == SOURCE_KEY
        and response_by_field.get("source_url") == expected_request["source_url"]
        and response_by_field.get("request_sha256") == hashlib.sha256(encoded_json(request_by_field)).hexdigest(),
        "response_identity_invalid",
    )
    _require(response_by_field.get("complete") is True and "error_type" not in response_by_field, "response_incomplete")
    _require(type(response_by_field.get("status")) is int and response_by_field["status"] == 200, "http_failure")
    _utc_timestamp(response_by_field.get("downloaded_at"))
    _response_headers(response_by_field)
    return _response_body(response_by_field), response_by_field


def _held_files(destination):
    _reject_symlinks(destination)
    _require(
        destination.is_dir() and {path.name for path in destination.iterdir()} == set(HELD_FILES),
        "held_inventory_invalid",
    )
    contents_by_name = {}
    for name, byte_limit in HELD_FILES.items():
        path = destination / name
        _reject_symlinks(path)
        _require(path.is_file() and path.stat().st_size <= byte_limit, "held_artifact_invalid")
        with path.open("rb") as stream:
            content = stream.read(byte_limit + 1)
        _require(0 < len(content) <= byte_limit, "held_artifact_invalid")
        contents_by_name[name] = content
    digest = hashlib.sha256(
        encoded_json({name: hashlib.sha256(content).hexdigest() for name, content in contents_by_name.items()})
    ).hexdigest()
    return contents_by_name, digest


def held_acquisition_content_sha256(destination: Path) -> str:
    """Pin the four actual files independently at trusted capture time."""
    return _held_files(destination)[1]


def read_held_acquisition(destination: Path, *, manifest_sha256: str, acquisition_sha256: str) -> dict:
    """Replay a pinned no-match or ambiguous search, never a complete physician inventory."""
    _require(
        isinstance(acquisition_sha256, str) and re.fullmatch(r"[0-9a-f]{64}", acquisition_sha256),
        "acquisition_pin_invalid",
    )
    contents_by_name, digest = _held_files(destination)
    _require(digest == acquisition_sha256, "acquisition_changed")
    artifacts_by_name = {name: decoded_json_object(content) for name, content in contents_by_name.items()}
    manifest_by_field = _validated_manifest(artifacts_by_name["manifest.json"], manifest_sha256)
    request = _request(
        "POST", SEARCH_URL, manifest_by_field["session_id"], _search_fields(manifest_by_field["license_number"])
    )
    body, response = _validated_response(
        artifacts_by_name["search.request.json"], artifacts_by_name["search.response.json"], request
    )
    physician, total = _search_result(body)
    _require(physician is None, "held_search_singleton")
    _require(
        _utc_timestamp(manifest_by_field["started_at"]) <= _utc_timestamp(response["downloaded_at"]),
        "chronology_invalid",
    )
    summary_by_field = {"outcome": "held", "reason": "search_not_singleton", "reported_total": total, "fact_count": 0}
    _require(encoded_json(artifacts_by_name["result.json"]) == encoded_json(summary_by_field), "result_changed")
    _require(held_acquisition_content_sha256(destination) == acquisition_sha256, "acquisition_changed")
    return {key: value for key, value in summary_by_field.items() if key != "fact_count"} | {
        "source_record": None,
        "facts": [],
    }


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
    _require(
        _utc_timestamp(manifest_by_field["started_at"])
        <= _utc_timestamp(search_response["downloaded_at"])
        <= _utc_timestamp(profile_response["downloaded_at"]),
        "chronology_invalid",
    )
    result = _education_result(manifest_by_field, physician_by_field, search_response, body, profile_response)
    rebuilt_summary_by_field = {key: value for key, value in result.items() if key not in {"source_record", "facts"}}
    rebuilt_summary_by_field["fact_count"] = len(result["facts"])
    _require(encoded_json(summary_by_field) == encoded_json(rebuilt_summary_by_field), "result_changed")
    return result
