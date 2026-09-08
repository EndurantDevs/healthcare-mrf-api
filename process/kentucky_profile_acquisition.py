# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retain exact Kentucky lookups against a frozen physician registry cohort."""

from __future__ import annotations

import asyncio
import hashlib
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode

import aiohttp
from sqlalchemy import text

from db.models import db
from process.kentucky_profile_rows import MAX_HTML_BYTES, SOURCE_KEY, extract_profiles
from process.massachusetts_profile_acquisition import encoded_json, write_new_json
from process.provider_directory_projection_json import decoded_json_object

LOOKUP_URL = "https://web1.ky.gov/GenSearch/LicenseList.aspx"
COHORT_SCHEMA = "ky-nppes-license-cohort/v1"
RESPONSE_SCHEMA = "ky-kbml-public-response/v1"
COVERAGE_SCOPE = "nppes_ky_alphanumeric_physician_license_cohort"
MAX_PROFILE_BYTES = MAX_HTML_BYTES
MAX_ACQUISITION_BYTES = 1_000_000_000
REQUEST_INTERVAL_SECONDS = 2.0
LICENSE_PATTERN = re.compile(r"(?=[A-Za-z0-9]*[0-9])[A-Za-z0-9]{1,32}", flags=re.ASCII)


def _is_valid_license(license_number: object) -> bool:
    return isinstance(license_number, str) and LICENSE_PATTERN.fullmatch(license_number) is not None


def source_url(license_number: str) -> str:
    """Keep literal numeric and prefixed licenses on the observed exact-search route."""
    if not _is_valid_license(license_number):
        raise ValueError("kentucky_profile_license_invalid")
    return LOOKUP_URL + "?" + urlencode({
        "AGY": "5", "FLD1": "", "FLD2": license_number, "FLD3": "0", "FLD4": "0", "TYPE": "",
    })


def _license_numbers(roots: list[dict]) -> list[str]:
    """Validate the entire scope before deriving any response paths."""
    if not isinstance(roots, list) or any(not isinstance(root, dict) for root in roots):
        raise ValueError("kentucky_profile_cohort_licenses_invalid")
    licenses = [root.get("license_number") for root in roots]
    if any(not _is_valid_license(value) for value in licenses) or len(set(licenses)) != len(licenses):
        raise ValueError("kentucky_profile_cohort_licenses_invalid")
    return licenses


def build_cohort(source_rows: list[dict], relations: dict) -> dict:
    """Preserve raw names, excluded license formats, and conflicting candidates."""
    candidates_by_license = defaultdict(dict)
    excluded_rows = []
    for source_row in source_rows:
        candidate_by_field = dict(source_row)
        raw_license = candidate_by_field.get("license_number")
        license_number = raw_license.strip() if isinstance(raw_license, str) else raw_license
        if not _is_valid_license(license_number):
            excluded_rows.append(candidate_by_field)
            continue
        candidates_by_license[license_number][encoded_json(candidate_by_field)] = candidate_by_field
    roots = [
        {"license_number": license_number, "candidates": [candidates[key] for key in sorted(candidates)]}
        for license_number, candidates in sorted(candidates_by_license.items())
    ]
    cohort_by_field = {
        "schema_version": COHORT_SCHEMA, "source_key": SOURCE_KEY, "coverage_scope": COVERAGE_SCOPE,
        "relations": relations, "source_rows": len(source_rows), "roots": roots,
        "excluded_rows": sorted(excluded_rows, key=encoded_json),
    }
    cohort_by_field["registry_generation"] = hashlib.sha256(encoded_json(cohort_by_field)).hexdigest()
    return cohort_by_field


async def capture_registry_cohort(schema: str) -> dict:
    """Freeze source relation identities and individual physician licenses together."""
    if not isinstance(schema, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise ValueError("kentucky_profile_schema_invalid")
    async with db.transaction():
        await db.status("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        source_rows = await db.all(text(f"""
            SELECT t.npi, t.provider_license_number AS license_number,
                   t.provider_license_number_state_code AS license_state,
                   t.healthcare_provider_taxonomy_code AS taxonomy,
                   n.provider_first_name AS first_name, n.provider_middle_name AS middle_name,
                   n.provider_last_name AS last_name, n.provider_name_suffix_text AS suffix
              FROM {schema}.npi_taxonomy t
              JOIN {schema}.npi n ON n.npi = t.npi
              JOIN {schema}.nucc_taxonomy u ON u.code = t.healthcare_provider_taxonomy_code
             WHERE t.provider_license_number_state_code = 'KY'
               AND n.entity_type_code = 1
               AND u.grouping = 'Allopathic & Osteopathic Physicians'
        """))
        # The SELECT keeps relation locks until commit, preventing name swaps before the OID read.
        relations = await db.all(text(
            "SELECT name, to_regclass(name)::oid::bigint AS oid FROM unnest(CAST(:names AS text[])) name"
        ), names=[f"{schema}.{name}" for name in ("npi", "npi_taxonomy", "nucc_taxonomy")])
    return build_cohort(
        [dict(row._mapping) for row in source_rows],
        {row._mapping["name"]: row._mapping["oid"] for row in relations},
    )


def _reject_symlinks(path: Path) -> None:
    if any(component.is_symlink() for component in (path, *path.parents)):
        raise ValueError("kentucky_profile_artifact_symlink")


def _read_artifact(path: Path, max_bytes: int) -> dict:
    _reject_symlinks(path)
    if not path.is_file() or path.stat().st_size > max_bytes:
        raise ValueError("kentucky_profile_artifact_file_invalid")
    return decoded_json_object(path.read_bytes())


def read_cohort(path: Path) -> dict:
    """Reject changed source manifests before using their retained scope."""
    cohort = _read_artifact(path, 64 * 1024 * 1024)
    generation = cohort.pop("registry_generation", None)
    if (
        cohort.get("schema_version") != COHORT_SCHEMA or cohort.get("source_key") != SOURCE_KEY
        or cohort.get("coverage_scope") != COVERAGE_SCOPE
        or generation != hashlib.sha256(encoded_json(cohort)).hexdigest()
    ):
        raise ValueError("kentucky_profile_cohort_changed")
    _license_numbers(cohort.get("roots"))
    cohort["registry_generation"] = generation
    return cohort


def decoded_profile(response: dict) -> list[dict]:
    """Only a validated empty result envelope represents a lookup without profiles."""
    body_text = response.get("body_text")
    if not isinstance(body_text, str):
        raise ValueError("kentucky_profile_response_changed")
    body = body_text.encode("utf-8")
    if not body or len(body) > MAX_PROFILE_BYTES or hashlib.sha256(body).hexdigest() != response.get("content_sha256"):
        raise ValueError("kentucky_profile_response_changed")
    if response.get("status") != 200:
        raise ValueError("kentucky_profile_http_failure")
    if str(response.get("content_type") or "").split(";", 1)[0].strip().lower() != "text/html":
        raise ValueError("kentucky_profile_content_type_invalid")
    return extract_profiles(body_text, license_number=response.get("license_number"))


def read_response(path: Path, license_number: str) -> dict:
    """Verify the exact source, query, timestamp, and raw HTML before replay."""
    expected_url = source_url(license_number)
    response = _read_artifact(path, MAX_PROFILE_BYTES * 8)
    if (
        response.get("schema_version") != RESPONSE_SCHEMA or response.get("source_key") != SOURCE_KEY
        or response.get("license_number") != license_number or response.get("source_url") != expected_url
    ):
        raise ValueError("kentucky_profile_response_identity_invalid")
    try:
        downloaded_at = datetime.fromisoformat(response.get("downloaded_at"))
    except (TypeError, ValueError) as exc:
        raise ValueError("kentucky_profile_download_time_invalid") from exc
    if downloaded_at.utcoffset() != timedelta(0):
        raise ValueError("kentucky_profile_download_time_invalid")
    decoded_profile(response)
    return response


async def _read_profile_body(response) -> bytes:
    chunks = []
    size = 0
    async for chunk in response.content.iter_chunked(64 * 1024):
        size += len(chunk)
        if size > MAX_PROFILE_BYTES:
            raise ValueError("kentucky_profile_response_too_large")
        chunks.append(chunk)
    return b"".join(chunks)


async def fetch_profile(session, license_number: str) -> dict:
    """Validate and retain one ordinary public GET without following redirects."""
    url = source_url(license_number)
    async with session.get(url, allow_redirects=False) as response:
        if response.status != 200:
            raise ValueError(f"kentucky_profile_http_failure:{response.status}")
        body = await _read_profile_body(response)
        response_by_field = {
            "schema_version": RESPONSE_SCHEMA, "source_key": SOURCE_KEY,
            "license_number": license_number, "source_url": url,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
            "status": response.status, "content_type": response.headers.get("Content-Type"),
            "content_sha256": hashlib.sha256(body).hexdigest(), "body_text": body.decode("utf-8"),
        }
    decoded_profile(response_by_field)
    return response_by_field


async def acquire_profiles(roots: list[dict], destination: Path, progress, *, retained: Path | None = None) -> dict:
    """Checkpoint validated responses without replacing any retained artifacts."""
    licenses = _license_numbers(roots)
    _reject_symlinks(destination)
    if retained is not None:
        _reject_symlinks(retained)
        if not retained.is_dir():
            raise ValueError("kentucky_profile_retained_directory_invalid")
    destination.mkdir(exist_ok=False)
    total_bytes = reused_count = 0
    last_request_started = 0.0
    response_hash = hashlib.sha256()
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
        # aiohttp otherwise repeats idempotent requests after connection failures.
        session._retry_connection = False
        for index, license_number in enumerate(licenses, 1):
            await progress(index - 1, len(licenses))
            old_path = retained / f"{license_number}.json" if retained is not None else None
            if old_path is not None and (old_path.exists() or old_path.is_symlink()):
                response = read_response(old_path, license_number)
                reused_count += 1
            else:
                elapsed = asyncio.get_running_loop().time() - last_request_started
                await asyncio.sleep(max(0.0, REQUEST_INTERVAL_SECONDS - elapsed))
                last_request_started = asyncio.get_running_loop().time()
                response = await fetch_profile(session, license_number)
            total_bytes += len(response["body_text"].encode("utf-8"))
            if total_bytes > MAX_ACQUISITION_BYTES:
                raise ValueError("kentucky_profile_acquisition_too_large")
            write_new_json(destination / f"{license_number}.json", response)
            response_hash.update(encoded_json(response))
            await progress(index, len(licenses))
    return {"responses": len(licenses), "response_bytes": total_bytes, "reused_responses": reused_count,
            "responses_sha256": response_hash.hexdigest()}
