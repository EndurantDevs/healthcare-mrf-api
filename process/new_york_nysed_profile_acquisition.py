# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Acquire one exact NYSED Medicine license with bounded, replayable evidence."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

from process.kentucky_profile_acquisition import _reject_symlinks
from process.massachusetts_profile_acquisition import encoded_json, write_new_json
from process.massachusetts_profile_rows import _hash
from process.new_york_nysed_profile import (
    MAX_RESPONSE_BYTES,
    PROFESSION_CODE,
    SCHEMA_VERSION,
    SOURCE_KEY,
    _require,
    acquisition_result,
    held_acquisition_result,
    request_descriptor,
)


def _now():
    return datetime.now(timezone.utc).isoformat()


def _response_headers(headers, api_key):
    """Retain validation headers only; a reflected request key is never saved."""
    return [
        [name, value.replace(api_key, "[omitted]")]
        for name, value in headers.items()
        if name.lower() in {"content-type", "content-encoding"}
    ]


def _retain_response(destination, descriptor, response_by_field, chunks, complete, api_key):
    body = b"".join(chunks)
    if api_key.encode("ascii") in body:
        body, complete = b"", False
        response_by_field["redacted_key_echo"] = True
    response_by_field.update(
        {
            "schema_version": SCHEMA_VERSION,
            "source_key": SOURCE_KEY,
            "request_sha256": _hash(descriptor),
            "downloaded_at": _now(),
            "complete": complete,
            "content_sha256": hashlib.sha256(body).hexdigest(),
            "body_base64": base64.b64encode(body).decode("ascii"),
        }
    )
    write_new_json(destination / "response.json", response_by_field)
    return response_by_field


async def _fetch_profile(session, destination, descriptor, api_key):
    response_by_field = {"status": None, "source_url": None, "headers": [], "received_bytes": 0}
    chunks = []
    try:
        async with session.get(
            descriptor["source_url"],
            headers={**descriptor["headers"], "x-oapi-key": api_key},
            allow_redirects=False,
            ssl=True,
        ) as response:
            response_by_field["status"] = response.status
            _require(str(response.url) == descriptor["source_url"], "response_url_changed")
            response_by_field["source_url"] = descriptor["source_url"]
            response_by_field["headers"] = _response_headers(response.headers, api_key)
            async for chunk in response.content.iter_chunked(64 * 1024):
                remaining = MAX_RESPONSE_BYTES - response_by_field["received_bytes"]
                chunks.append(chunk[:remaining])
                response_by_field["received_bytes"] += len(chunk)
                _require(response_by_field["received_bytes"] <= MAX_RESPONSE_BYTES, "response_too_large")
    except BaseException as error:
        response_by_field["error_type"] = type(error).__name__
        _retain_response(destination, descriptor, response_by_field, chunks, False, api_key)
        raise
    return _retain_response(destination, descriptor, response_by_field, chunks, True, api_key)


async def _acquire_profile(destination, manifest_by_field, descriptor, api_key):
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=60),
        cookie_jar=aiohttp.DummyCookieJar(),
        trust_env=False,
        auto_decompress=False,
    ) as session:
        # aiohttp otherwise repeats idempotent requests after connection failures.
        _require(type(getattr(session, "_retry_connection", None)) is bool, "retry_control_unavailable")
        session._retry_connection = False
        response_by_field = await _fetch_profile(session, destination, descriptor, api_key)
    acquired = (
        held_acquisition_result(manifest_by_field, response_by_field)
        if response_by_field["status"] == 204
        else acquisition_result(manifest_by_field, response_by_field)
    )
    receipt_by_field = {
        "schema_version": SCHEMA_VERSION,
        "outcome": acquired["outcome"],
        "completed_at": _now(),
        "manifest_sha256": _hash(manifest_by_field),
        "request_sha256": _hash(descriptor),
        "response_sha256": _hash(response_by_field),
        "fact_count": len(acquired["facts"]),
    }
    if acquired["outcome"] == "held":
        receipt_by_field["reason"] = acquired["reason"]
    write_new_json(destination / "result.json", receipt_by_field)
    return {**acquired, "receipt_sha256": _hash(receipt_by_field)}


async def acquire_license(license_number: str, destination: Path, *, run_id: str, api_key: str) -> dict:
    """Use a fresh directory and an explicit public application header value."""
    descriptor = request_descriptor(license_number)
    _require(isinstance(api_key, str) and re.fullmatch(r"[!-~]{1,512}", api_key), "api_key_invalid")
    _require(isinstance(run_id, str) and run_id.strip() and api_key not in run_id, "run_id_invalid")
    manifest_by_field = {
        "schema_version": SCHEMA_VERSION,
        "source_key": SOURCE_KEY,
        "profession_code": PROFESSION_CODE,
        "license_number": license_number,
        "run_id": run_id,
        "started_at": _now(),
    }
    _require(
        api_key.encode("ascii") not in encoded_json(manifest_by_field)
        and api_key.encode("ascii") not in encoded_json(descriptor),
        "api_key_overlaps_metadata",
    )
    _reject_symlinks(destination)
    destination.mkdir(mode=0o700, exist_ok=False)
    write_new_json(destination / "manifest.json", manifest_by_field)
    write_new_json(destination / "request.json", descriptor)
    try:
        return await _acquire_profile(destination, manifest_by_field, descriptor, api_key)
    except BaseException as error:
        error_type = type(error).__name__
        write_new_json(
            destination / "result.json",
            {"schema_version": SCHEMA_VERSION, "outcome": "failed", "error_type": error_type},
        )
        if isinstance(error, asyncio.CancelledError):
            raise asyncio.CancelledError() from None
        if isinstance(error, KeyboardInterrupt):
            raise KeyboardInterrupt() from None
        if isinstance(error, SystemExit):
            exit_code = error.code if error.code is None or isinstance(error.code, int) else 1
            raise SystemExit(exit_code) from None
        raise ValueError("new_york_nysed_acquisition_failed:" + error_type) from None
