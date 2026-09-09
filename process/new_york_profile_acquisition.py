# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retain one exact NY license lookup before any registry binding or publication."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
from multidict import CIMultiDict

from process.kentucky_profile_acquisition import _reject_symlinks
from process.massachusetts_profile_acquisition import encoded_json, write_new_json
from process.new_york_profile_rows import MAX_PROFILE_BYTES, SOURCE_KEY, parse_education
from process.provider_directory_projection_json import decoded_json_object

SEARCH_URL = "https://www.nydoctorprofile.com/api/v1/physicians"
PROFILE_URL = "https://www.nydoctorprofile.com/api/v1/physician/profile/"
ACQUISITION_SCHEMA = "ny-nypp-license-acquisition/v1"
REQUEST_INTERVAL_SECONDS = 0.5


def _require(condition, reason):
    if not condition:
        raise ValueError("new_york_acquisition_" + reason)


def _search_fields(license_number):
    return {
        "pageNumber": 1, "numberOfResults": 10, "advancedSearch": "Y", "specialty": "", "noDataMsg": "",
        "specialtyGroup": None, "specialtySubGroup": None, "hospitalCode": "", "countyFipsCode": None,
        "city": None, "print": "N", "specialityGroupSearch": "", "specialitySubGroupSearch": "",
        "medicadIndicator": None, "medicareIndicator": None, "hivIndicator": None, "hospitalName": None,
        "licenseNumber": license_number,
    }


def _request(method, source_url, session_id, fields=None):
    return {
        "method": method, "source_url": source_url,
        "headers": {"Accept": "application/json", "Content-Type": "application/json",
                    "Accept-Encoding": "identity", "sessionId": session_id},
        "body_text": encoded_json(fields).decode("utf-8") if fields is not None else None,
        "allow_redirects": False,
    }


def _retained_response(destination, stage, request, response_by_field, chunks, complete):
    body = b"".join(chunks)
    response_by_field.update({
        "schema_version": ACQUISITION_SCHEMA, "source_key": SOURCE_KEY,
        "request_sha256": hashlib.sha256(encoded_json(request)).hexdigest(),
        "downloaded_at": datetime.now(timezone.utc).isoformat(), "complete": complete,
        "content_sha256": hashlib.sha256(body).hexdigest(), "body_base64": base64.b64encode(body).decode("ascii"),
    })
    write_new_json(destination / f"{stage}.response.json", response_by_field)
    return body


def _validated_headers(header_pairs):
    headers = CIMultiDict(header_pairs)
    _require(len(headers.getall("Content-Type", [])) == 1
             and len(headers.getall("Content-Encoding", [])) <= 1, "headers_ambiguous")
    _require(headers.get("Content-Type", "").split(";", 1)[0].strip().lower() == "application/json", "content_type_invalid")
    _require(headers.get("Content-Encoding", "identity").strip().lower() == "identity", "content_encoding_invalid")


async def _fetch_response(session, destination, stage, request):
    write_new_json(destination / f"{stage}.request.json", request)
    chunks = []
    response_by_field = {"status": None, "source_url": None, "headers": [], "received_bytes": 0}
    try:
        async with session.request(request["method"], request["source_url"], headers=request["headers"],
                                   data=request["body_text"].encode("utf-8") if request["body_text"] is not None else None,
                                   allow_redirects=False) as response:
            response_by_field.update(status=response.status, source_url=str(response.url), headers=list(response.headers.items()))
            async for chunk in response.content.iter_chunked(64 * 1024):
                remaining = MAX_PROFILE_BYTES - response_by_field["received_bytes"]
                chunks.append(chunk[:remaining])
                response_by_field["received_bytes"] += len(chunk)
                _require(response_by_field["received_bytes"] <= MAX_PROFILE_BYTES, "response_too_large")
    except BaseException as exc:
        response_by_field["error_type"] = type(exc).__name__
        _retained_response(destination, stage, request, response_by_field, chunks, False)
        raise
    body = _retained_response(destination, stage, request, response_by_field, chunks, True)
    _require(response_by_field["source_url"] == request["source_url"], "response_url_changed")
    _require(response_by_field["status"] == 200, "http_failure")
    _validated_headers(response_by_field["headers"])
    return body, response_by_field


def _search_result(body):
    response_by_field = decoded_json_object(body)
    _require({"status", "error", "errorMessage", "data"} <= response_by_field.keys()
             and type(response_by_field["status"]) is int and response_by_field["status"] == 200
             and response_by_field["error"] is None and response_by_field["errorMessage"] is None, "search_unsuccessful")
    result_by_field = response_by_field["data"]
    _require(isinstance(result_by_field, dict) and type(result_by_field.get("pageNumber")) is int
             and result_by_field["pageNumber"] == 1 and type(result_by_field.get("numberOfResults")) is int
             and result_by_field["numberOfResults"] >= 0 and isinstance(result_by_field.get("physicians"), list)
             and len(result_by_field["physicians"]) == min(10, result_by_field["numberOfResults"]), "search_incomplete")
    if result_by_field["numberOfResults"] != 1:
        return None, result_by_field["numberOfResults"]
    physician_by_field = result_by_field["physicians"][0]
    text_fields = ("dataMessage", "physicianFirstName", "physicianID", "physicianLastName", "statusCode",
                   "terminationCode", "terminationText")
    _require(isinstance(physician_by_field, dict)
             and all(isinstance(physician_by_field.get(field), str) for field in text_fields)
             and all(isinstance(physician_by_field.get(field), list) for field in ("medicalPractice", "practiceLocations")), "search_identity_invalid")
    _require(re.fullmatch(r"[1-9][0-9]{0,11}", physician_by_field["physicianID"])
             and physician_by_field["physicianFirstName"].strip() and physician_by_field["physicianLastName"].strip(), "search_identity_invalid")
    return physician_by_field, 1


async def _acquire_education(session, destination, manifest):
    license_number, session_id = manifest["license_number"], manifest["session_id"]
    search_request = _request("POST", SEARCH_URL, session_id, _search_fields(license_number))
    body, search_response = await _fetch_response(session, destination, "search", search_request)
    physician_by_field, total = _search_result(body)
    if physician_by_field is None:
        return {"outcome": "held", "reason": "search_not_singleton", "reported_total": total,
                "source_record": None, "facts": []}
    physician_id = physician_by_field["physicianID"]
    await asyncio.sleep(REQUEST_INTERVAL_SECONDS)
    source_url = PROFILE_URL + physician_id + f"?sections=EDUCATIONALL&physicianId={physician_id}"
    body, profile_response = await _fetch_response(session, destination, "education", _request("GET", source_url, session_id))
    return _education_result(manifest, physician_by_field, search_response, body, profile_response)


def _education_result(manifest, physician_by_field, search_response, body, profile_response):
    physician_id = physician_by_field["physicianID"]
    evidence_by_field = {
        "run_id": manifest["run_id"], "artifact_id": hashlib.sha256(encoded_json(profile_response)).hexdigest(),
        "source_url": profile_response["source_url"], "downloaded_at": profile_response["downloaded_at"],
        "content_sha256": profile_response["content_sha256"], "row_number": 1,
    }
    source_record, facts = parse_education(body, license_number=manifest["license_number"], physician_id=physician_id,
                                         evidence=evidence_by_field)
    identity_by_field = source_record["raw_payload"]["data"]["phyInfo"]
    has_matching_names = (identity_by_field["firstName"] == physician_by_field["physicianFirstName"]
                          and identity_by_field["lastName"] == physician_by_field["physicianLastName"])
    source_record["match_evidence"]["license_search"] = {
        "source_url": SEARCH_URL, "request_sha256": search_response["request_sha256"],
        "content_sha256": search_response["content_sha256"], "downloaded_at": search_response["downloaded_at"],
        "raw_identity": {field: physician_by_field[field] for field in ("physicianID", "physicianFirstName", "physicianLastName")},
        "header_names_agree": has_matching_names,
    }
    if not has_matching_names:
        source_record["normalized_payload"]["quality_flags"].append("search_header_name_disagreement")
        for fact in facts:
            fact["source_json"]["quality_flags"].append("search_header_name_disagreement")
    return {"outcome": "acquired", "physician_id": physician_id, "identity_review_required": not has_matching_names,
            "source_record": source_record, "facts": facts}


async def acquire_license(license_number: str, destination: Path, *, run_id: str) -> dict:
    """Use a fresh evidence directory; uncertain or held attempts are never retried."""
    _require(isinstance(license_number, str) and re.fullmatch(r"[0-9]{6}", license_number), "license_invalid")
    _require(isinstance(run_id, str) and run_id.strip(), "run_id_invalid")
    _reject_symlinks(destination)
    destination.mkdir(mode=0o700, exist_ok=False)
    manifest_by_field = {"schema_version": ACQUISITION_SCHEMA, "source_key": SOURCE_KEY,
                         "license_number": license_number, "run_id": run_id, "session_id": uuid.uuid4().hex,
                         "started_at": datetime.now(timezone.utc).isoformat()}
    write_new_json(destination / "manifest.json", manifest_by_field)
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60), cookie_jar=aiohttp.DummyCookieJar(),
                                         trust_env=False, auto_decompress=False) as session:
            # aiohttp otherwise repeats idempotent requests after connection failures.
            _require(type(getattr(session, "_retry_connection", None)) is bool, "retry_control_unavailable")
            session._retry_connection = False
            result = await _acquire_education(session, destination, manifest_by_field)
    except BaseException as exc:
        write_new_json(destination / "result.json", {"outcome": "failed", "error_type": type(exc).__name__,
                        "reason": str(exc) if str(exc).startswith("new_york_") else type(exc).__name__})
        raise
    summary_by_field = {key: value for key, value in result.items() if key not in {"source_record", "facts"}}
    summary_by_field["fact_count"] = len(result["facts"])
    write_new_json(destination / "result.json", summary_by_field)
    return result
