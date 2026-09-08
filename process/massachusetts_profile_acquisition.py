# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Acquire public BORIM profiles against an explicitly frozen registry cohort."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import tempfile
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp
from sqlalchemy import text

from db.models import db
from process.provider_directory_projection_json import decoded_json_object

API_BASE = "https://api.medboard.mass.gov/api-public/search/physician-profiles/"
COHORT_SCHEMA = "ma-nppes-license-cohort/v1"
RESPONSE_SCHEMA = "ma-borim-public-response/v1"
MAX_PROFILE_BYTES = 1024 * 1024
MAX_ACQUISITION_BYTES = 1024 * 1024 * 1024
REQUEST_INTERVAL_SECONDS = 0.5
LICENSE_PATTERN = re.compile(r"[0-9]{1,10}", flags=re.ASCII)


def encoded_json(value: object) -> bytes:
    """Make retained manifests and replay identities deterministic."""
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False).encode("utf-8")


def write_new_json(path: Path, value: object) -> None:
    """Atomically create one owned artifact without overwriting an existing path."""
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as temporary:
            temporary_path = Path(temporary.name)
            temporary.write(encoded_json(value))
            temporary.flush()
            os.fsync(temporary.fileno())
        os.link(temporary_path, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def build_cohort(source_rows: list[dict], relations: dict) -> dict:
    """Retain excluded formats as well as every eligible license and candidate."""
    candidates_by_license = defaultdict(dict)
    excluded_rows = []
    for source_row in source_rows:
        candidate_by_field = dict(source_row)
        license_number = str(candidate_by_field.get("license_number") or "").strip()
        if not LICENSE_PATTERN.fullmatch(license_number):
            excluded_rows.append(candidate_by_field)
            continue
        identity = encoded_json(candidate_by_field)
        candidates_by_license[license_number][identity] = candidate_by_field
    roots = [
        {"license_number": license_number, "candidates": [candidates[key] for key in sorted(candidates)]}
        for license_number, candidates in sorted(candidates_by_license.items())
    ]
    cohort_by_field = {
        "schema_version": COHORT_SCHEMA,
        "coverage_scope": "nppes_ma_numeric_physician_license_cohort",
        "relations": relations,
        "source_rows": len(source_rows),
        "roots": roots,
        "excluded_rows": sorted(excluded_rows, key=encoded_json),
    }
    cohort_by_field["registry_generation"] = hashlib.sha256(encoded_json(cohort_by_field)).hexdigest()
    return cohort_by_field


async def capture_registry_cohort(schema: str) -> dict:
    """Read license roots and relation identities in one consistent snapshot."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise ValueError("massachusetts_profile_schema_invalid")
    async with db.transaction():
        await db.status("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        relations = await db.all(text(
            "SELECT name, to_regclass(name)::oid::bigint AS oid FROM unnest(CAST(:names AS text[])) name"
        ), names=[f"{schema}.{name}" for name in ("npi", "npi_taxonomy", "nucc_taxonomy")])
        source_rows = await db.all(text(f"""
            SELECT t.npi, t.provider_license_number AS license_number,
                   t.healthcare_provider_taxonomy_code AS taxonomy,
                   n.provider_first_name AS first_name, n.provider_last_name AS last_name
              FROM {schema}.npi_taxonomy t
              JOIN {schema}.npi n ON n.npi = t.npi
              JOIN {schema}.nucc_taxonomy u ON u.code = t.healthcare_provider_taxonomy_code
             WHERE t.provider_license_number_state_code = 'MA'
               AND n.entity_type_code = 1
               AND u.grouping = 'Allopathic & Osteopathic Physicians'
        """))
    return build_cohort(
        [dict(row._mapping) for row in source_rows],
        {row._mapping["name"]: row._mapping["oid"] for row in relations},
    )


def read_cohort(path: Path) -> dict:
    """Verify a retained cohort before using its exact acquisition scope."""
    if path.is_symlink() or path.stat().st_size > 64 * 1024 * 1024:
        raise ValueError("massachusetts_profile_cohort_file_invalid")
    cohort = decoded_json_object(path.read_bytes())
    generation = cohort.pop("registry_generation", None)
    if cohort.get("schema_version") != COHORT_SCHEMA or generation != hashlib.sha256(encoded_json(cohort)).hexdigest():
        raise ValueError("massachusetts_profile_cohort_changed")
    cohort["registry_generation"] = generation
    licenses = [root["license_number"] for root in cohort["roots"]]
    if len(set(licenses)) != len(licenses) or any(not LICENSE_PATTERN.fullmatch(value) for value in licenses):
        raise ValueError("massachusetts_profile_cohort_licenses_invalid")
    return cohort


def decoded_profile(response: dict) -> dict | None:
    """An observed HTTP 200 empty body means no resolved public full profile."""
    body = response["body_text"].encode("utf-8")
    if len(body) > MAX_PROFILE_BYTES or hashlib.sha256(body).hexdigest() != response["content_sha256"]:
        raise ValueError("massachusetts_profile_response_changed")
    if response.get("status") != 200:
        raise ValueError("massachusetts_profile_http_failure")
    if not body:
        return None
    if str(response.get("content_type") or "").split(";", 1)[0].strip().lower() != "application/json":
        raise ValueError("massachusetts_profile_content_type_invalid")
    return decoded_json_object(body)


def read_response(path: Path, license_number: str) -> dict:
    """Read exactly one previously acquired response, including its empty result."""
    if path.is_symlink() or path.stat().st_size > MAX_PROFILE_BYTES * 8:
        raise ValueError("massachusetts_profile_response_file_invalid")
    response = decoded_json_object(path.read_bytes())
    if (
        response.get("schema_version") != RESPONSE_SCHEMA
        or response.get("license_number") != license_number
        or response.get("source_url") != API_BASE + license_number
    ):
        raise ValueError("massachusetts_profile_response_identity_invalid")
    try:
        downloaded_at = datetime.fromisoformat(response.get("downloaded_at"))
    except (TypeError, ValueError) as exc:
        raise ValueError("massachusetts_profile_download_time_invalid") from exc
    if downloaded_at.utcoffset() != timedelta(0):
        raise ValueError("massachusetts_profile_download_time_invalid")
    decoded_profile(response)
    return response


async def _read_profile_body(response) -> bytes:
    chunks = []
    size = 0
    async for chunk in response.content.iter_chunked(64 * 1024):
        size += len(chunk)
        if size > MAX_PROFILE_BYTES:
            raise ValueError("massachusetts_profile_response_too_large")
        chunks.append(chunk)
    return b"".join(chunks)


async def fetch_profile(session, license_number: str) -> dict:
    """Fetch only the observed public route; never follow alternate destinations."""
    if not LICENSE_PATTERN.fullmatch(license_number):
        raise ValueError("massachusetts_profile_license_invalid")
    async with session.get(API_BASE + license_number, allow_redirects=False) as response:
        if response.status != 200:
            raise ValueError(f"massachusetts_profile_http_failure:{response.status}")
        body = await _read_profile_body(response)
        response_by_field = {
            "schema_version": RESPONSE_SCHEMA,
            "license_number": license_number,
            "source_url": API_BASE + license_number,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
            "status": response.status,
            "content_type": response.headers.get("Content-Type"),
            "content_sha256": hashlib.sha256(body).hexdigest(),
            "body_text": body.decode("utf-8"),
        }
    decoded_profile(response_by_field)
    return response_by_field


async def acquire_profiles(roots: list[dict], destination: Path, progress, *, retained: Path | None = None) -> dict:
    """Persist each response atomically; exact retained bytes make failed runs resumable."""
    destination.mkdir(exist_ok=False)
    total_bytes = 0
    reused_count = 0
    last_request_started = 0.0
    response_hash = hashlib.sha256()
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
        for index, root in enumerate(roots, 1):
            await progress(index - 1, len(roots))
            license_number = root["license_number"]
            old_path = retained / f"{license_number}.json" if retained else None
            if old_path is not None and old_path.is_symlink():
                raise ValueError("massachusetts_profile_response_file_invalid")
            if old_path is not None and old_path.exists():
                response = read_response(old_path, license_number)
                reused_count += 1
            else:
                await asyncio.sleep(max(0.0, REQUEST_INTERVAL_SECONDS - (asyncio.get_running_loop().time() - last_request_started)))
                last_request_started = asyncio.get_running_loop().time()
                response = await fetch_profile(session, license_number)
            total_bytes += len(response["body_text"].encode("utf-8"))
            if total_bytes > MAX_ACQUISITION_BYTES:
                raise ValueError("massachusetts_profile_acquisition_too_large")
            write_new_json(destination / f"{license_number}.json", response)
            response_hash.update(encoded_json([license_number, response["content_sha256"], response["downloaded_at"]]))
            await progress(index, len(roots))
    return {"responses": len(roots), "response_bytes": total_bytes, "reused_responses": reused_count, "responses_sha256": response_hash.hexdigest()}
