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
NARROWED_SCHEMA = "ky-kbml-candidate-name-responses/v2"
NARROWED_SCOPE = "frozen_nppes_candidate_surnames_and_literal_license"
COVERAGE_SCOPE = "nppes_ky_alphanumeric_physician_license_cohort"
MAX_PROFILE_BYTES = MAX_HTML_BYTES
MAX_ACQUISITION_BYTES = 1_000_000_000
REQUEST_INTERVAL_SECONDS = 2.0
MAX_NARROWED_QUERIES = 32
LICENSE_PATTERN = re.compile(r"(?=[A-Za-z0-9]*[0-9])[A-Za-z0-9]{1,32}", flags=re.ASCII)


def _is_valid_license(license_number: object) -> bool:
    return isinstance(license_number, str) and LICENSE_PATTERN.fullmatch(license_number) is not None


def source_url(license_number: str, *, last_name: str = "") -> str:
    """Use the literal-license lookup route with an optional documented last-name field."""
    if not _is_valid_license(license_number):
        raise ValueError("kentucky_profile_license_invalid")
    return LOOKUP_URL + "?" + urlencode({
        "AGY": "5", "FLD1": last_name, "FLD2": license_number, "FLD3": "0", "FLD4": "0", "TYPE": "",
    })


class ProfileResponseTooLarge(ValueError):
    """An incomplete response with an exact count/hash of the consumed prefix."""

    def __init__(self, observed_bytes, read_prefix_sha256):
        super().__init__("kentucky_profile_response_too_large")
        self.receipt = {"truncated": True, "complete": False, "outcome": "response_too_large",
                        "observed_bytes": observed_bytes, "read_prefix_sha256": read_prefix_sha256}


def _candidate_query_scope(root):
    candidates = root.get("candidates")
    if not isinstance(candidates, list) or not candidates or any(not isinstance(candidate, dict) for candidate in candidates):
        raise ValueError("kentucky_profile_narrowing_candidates_missing")
    indexes_by_surname = defaultdict(list)
    for index, candidate in enumerate(candidates):
        surname = candidate.get("last_name")
        if (not isinstance(surname, str) or not surname.strip() or surname != surname.strip()
                or len(surname) > 50 or not surname.isprintable() or any(char in surname for char in "%_[]")):
            raise ValueError("kentucky_profile_narrowing_surname_unsupported")
        indexes_by_surname[surname].append(index)
    if len(indexes_by_surname) > MAX_NARROWED_QUERIES:
        raise ValueError("kentucky_profile_narrowing_query_limit_exceeded")
    return {"coverage_scope": NARROWED_SCOPE,
            "candidates_sha256": hashlib.sha256(encoded_json(candidates)).hexdigest(),
            "queries": [{"last_name": surname, "candidate_indexes": indexes_by_surname[surname]}
                        for surname in sorted(indexes_by_surname)]}


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


def decoded_profile(response: dict, *, last_name: str = "") -> list[dict]:
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
    return extract_profiles(body_text, license_number=response.get("license_number"), last_name=last_name)


def read_response(path: Path, license_number: str, *, candidates: list | None = None) -> dict:
    """Verify the exact source, query, timestamp, and raw HTML before replay."""
    expected_url = source_url(license_number)
    response = _read_artifact(path, MAX_PROFILE_BYTES * 8)
    if response.get("schema_version") == NARROWED_SCHEMA:
        _validate_narrowed(response, {"license_number": license_number, "candidates": candidates})
        return response
    _validate_response(response, license_number, expected_url)
    return response


def _validate_response(response, license_number, expected_url, *, last_name=""):
    if (
        not isinstance(response, dict) or response.get("schema_version") != RESPONSE_SCHEMA or response.get("source_key") != SOURCE_KEY
        or response.get("license_number") != license_number or response.get("source_url") != expected_url
    ):
        raise ValueError("kentucky_profile_response_identity_invalid")
    try:
        downloaded_at = datetime.fromisoformat(response.get("downloaded_at"))
    except (TypeError, ValueError) as exc:
        raise ValueError("kentucky_profile_download_time_invalid") from exc
    if downloaded_at.utcoffset() != timedelta(0):
        raise ValueError("kentucky_profile_download_time_invalid")
    decoded_profile(response, last_name=last_name)


async def _read_profile_body(response) -> bytes:
    chunks = []
    size = 0
    prefix_hash = hashlib.sha256()
    async for chunk in response.content.iter_chunked(64 * 1024):
        size += len(chunk)
        prefix_hash.update(chunk)
        if size > MAX_PROFILE_BYTES:
            raise ProfileResponseTooLarge(size, prefix_hash.hexdigest())
        chunks.append(chunk)
    return b"".join(chunks)


async def fetch_profile(session, license_number: str, *, last_name: str = "") -> dict:
    """Validate and retain one ordinary public GET without following redirects."""
    url = source_url(license_number, last_name=last_name)
    async with session.get(url, allow_redirects=False) as response:
        if response.status != 200:
            raise ValueError(f"kentucky_profile_http_failure:{response.status}")
        content_type = response.headers.get("Content-Type")
        if str(content_type or "").split(";", 1)[0].strip().lower() != "text/html":
            raise ValueError("kentucky_profile_content_type_invalid")
        try:
            body = await _read_profile_body(response)
        except ProfileResponseTooLarge as error:
            error.receipt.update(license_number=license_number, source_url=url,
                                 downloaded_at=datetime.now(timezone.utc).isoformat(),
                                 status=response.status, content_type=content_type)
            raise
        response_by_field = {
            "schema_version": RESPONSE_SCHEMA, "source_key": SOURCE_KEY,
            "license_number": license_number, "source_url": url,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
            "status": response.status, "content_type": response.headers.get("Content-Type"),
            "content_sha256": hashlib.sha256(body).hexdigest(), "body_text": body.decode("utf-8"),
        }
    decoded_profile(response_by_field, last_name=last_name)
    return response_by_field


def response_bytes(response):
    """Count complete bodies and every byte consumed before an oversized stop."""
    if response.get("schema_version") == NARROWED_SCHEMA:
        return response["oversized_attempt"]["observed_bytes"] + sum(
            len(query["body_text"].encode("utf-8")) for query in response["responses"])
    return len(response["body_text"].encode("utf-8"))


def _validate_oversized_attempt(attempt, license_number):
    fields = {"truncated", "complete", "outcome", "observed_bytes", "read_prefix_sha256",
              "license_number", "source_url", "downloaded_at", "status", "content_type"}
    if (not isinstance(attempt, dict) or set(attempt) != fields
            or attempt["truncated"] is not True or attempt["complete"] is not False
            or attempt["outcome"] != "response_too_large" or attempt["status"] != 200
            or attempt["license_number"] != license_number or attempt["source_url"] != source_url(license_number)
            or type(attempt["observed_bytes"]) is not int or not MAX_PROFILE_BYTES < attempt["observed_bytes"] <= MAX_PROFILE_BYTES + 64 * 1024
            or not re.fullmatch(r"[a-f0-9]{64}", str(attempt["read_prefix_sha256"]))
            or str(attempt["content_type"]).split(";", 1)[0].strip().lower() != "text/html"):
        raise ValueError("kentucky_profile_truncated_attempt_invalid")
    try:
        downloaded_at = datetime.fromisoformat(attempt["downloaded_at"])
    except (TypeError, ValueError) as error:
        raise ValueError("kentucky_profile_download_time_invalid") from error
    if downloaded_at.utcoffset() != timedelta(0):
        raise ValueError("kentucky_profile_download_time_invalid")


def _validate_narrowed(response, root):
    scope = _candidate_query_scope(root)
    if (set(response) != {"schema_version", "source_key", "license_number", "query_scope", "oversized_attempt", "responses"}
            or response["schema_version"] != NARROWED_SCHEMA or response["source_key"] != SOURCE_KEY
            or response["license_number"] != root["license_number"] or response["query_scope"] != scope
            or not isinstance(response["responses"], list) or len(response["responses"]) != len(scope["queries"])
            or len(encoded_json(response)) > MAX_PROFILE_BYTES * 8):
        raise ValueError("kentucky_profile_narrowed_response_changed")
    _validate_oversized_attempt(response["oversized_attempt"], root["license_number"])
    previous_time = response["oversized_attempt"]["downloaded_at"]
    for query, receipt in zip(scope["queries"], response["responses"], strict=True):
        _validate_response(receipt, root["license_number"], source_url(root["license_number"], last_name=query["last_name"]),
                           last_name=query["last_name"])
        if datetime.fromisoformat(receipt["downloaded_at"]) < datetime.fromisoformat(previous_time):
            raise ValueError("kentucky_profile_narrowed_response_order")
        previous_time = receipt["downloaded_at"]


async def _validate_retained_roots(roots, retained, progress):
    """Validate all retained roots before transport, yielding between bounded files."""
    if retained is None:
        return
    for index, root in enumerate(roots):
        await asyncio.sleep(0)
        await progress(index, len(roots))
        path = retained / f"{root['license_number']}.json"
        partial = retained / f"{root['license_number']}.narrowed"
        if path.exists() or path.is_symlink():
            read_response(path, root["license_number"], candidates=root.get("candidates"))
        elif partial.exists() or partial.is_symlink():
            _reject_symlinks(partial)
            raise ValueError("kentucky_profile_narrowed_checkpoint_incomplete")


async def _checkpoint_query(root, query, directory, fetch, account):
    index = query["index"]
    url = source_url(root["license_number"], last_name=query["last_name"])
    write_new_json(directory / f"{index}.started.json", {"source_url": url, "last_name": query["last_name"]})
    try:
        response = await fetch(root["license_number"], query["last_name"])
    except BaseException as error:
        failure = error.receipt if isinstance(error, ProfileResponseTooLarge) else {"source_url": url, "error_type": type(error).__name__}
        write_new_json(directory / f"{index}.failed.json", failure)
        if isinstance(error, ProfileResponseTooLarge):
            account(error.receipt["observed_bytes"])
        raise
    write_new_json(directory / f"{index}.json", response)
    account(response_bytes(response))
    return response


async def _acquire_narrowed(root, destination, fetch, account, attempt):
    directory = destination / f"{root['license_number']}.narrowed"
    _reject_symlinks(directory)
    directory.mkdir(exist_ok=False)
    write_new_json(directory / "oversized.json", attempt)
    account(attempt["observed_bytes"])
    scope = _candidate_query_scope(root)
    write_new_json(directory / "scope.json", scope)
    response_by_field = {"schema_version": NARROWED_SCHEMA, "source_key": SOURCE_KEY,
                "license_number": root["license_number"], "query_scope": scope,
                "oversized_attempt": attempt, "responses": []}
    for index, query in enumerate(scope["queries"], 1):
        response_by_field["responses"].append(await _checkpoint_query(root, {**query, "index": index}, directory, fetch, account))
        if len(encoded_json(response_by_field)) > MAX_PROFILE_BYTES * 8:
            raise ValueError("kentucky_profile_narrowed_bundle_too_large")
    _validate_narrowed(response_by_field, root)
    return response_by_field


async def _acquire_response(root, destination, retained, fetch, account):
    path = retained / f"{root['license_number']}.json" if retained is not None else None
    if path is not None and (path.exists() or path.is_symlink()):
        response = read_response(path, root["license_number"], candidates=root.get("candidates"))
        account(response_bytes(response))
        return response, True
    try:
        response = await fetch(root["license_number"])
    except ProfileResponseTooLarge as error:
        return await _acquire_narrowed(root, destination, fetch, account, error.receipt), False
    account(response_bytes(response))
    return response, False


async def acquire_profiles(roots: list[dict], destination: Path, progress, *, retained: Path | None = None) -> dict:
    """Checkpoint validated responses without replacing any retained artifacts."""
    licenses = _license_numbers(roots)
    _reject_symlinks(destination)
    if retained is not None:
        _reject_symlinks(retained)
        if not retained.is_dir():
            raise ValueError("kentucky_profile_retained_directory_invalid")
    await _validate_retained_roots(roots, retained, progress)
    destination.mkdir(exist_ok=False)
    reused_count = 0
    narrowed_roots = narrowed_queries = oversized_bytes = 0
    state_by_field = {"total_bytes": 0, "last_request_started": 0.0}
    response_hash = hashlib.sha256()

    def account(count):
        """Account for every consumed source byte, including truncated attempts."""
        state_by_field["total_bytes"] += count
        if state_by_field["total_bytes"] > MAX_ACQUISITION_BYTES:
            raise ValueError("kentucky_profile_acquisition_too_large")

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
        # aiohttp otherwise repeats idempotent requests after connection failures.
        if type(getattr(session, "_retry_connection", None)) is not bool:
            raise ValueError("kentucky_profile_retry_control_unavailable")
        session._retry_connection = False
        async def fetch(license_number, last_name=""):
            """Honor cancellation and spacing before each distinct source query."""
            await progress(index - 1, len(licenses))
            elapsed = asyncio.get_running_loop().time() - state_by_field["last_request_started"]
            await asyncio.sleep(max(0.0, REQUEST_INTERVAL_SECONDS - elapsed))
            state_by_field["last_request_started"] = asyncio.get_running_loop().time()
            return await fetch_profile(session, license_number, **({"last_name": last_name} if last_name else {}))

        for index, root in enumerate(roots, 1):
            await progress(index - 1, len(licenses))
            response, reused = await _acquire_response(root, destination, retained, fetch, account)
            reused_count += reused
            if response["schema_version"] == NARROWED_SCHEMA:
                narrowed_roots += 1
                narrowed_queries += len(response["responses"])
                oversized_bytes += response["oversized_attempt"]["observed_bytes"]
            license_number = root["license_number"]
            write_new_json(destination / f"{license_number}.json", response)
            response_hash.update(encoded_json(response))
            await progress(index, len(licenses))
    metrics_by_field = {"responses": len(licenses), "response_bytes": state_by_field["total_bytes"], "reused_responses": reused_count,
               "responses_sha256": response_hash.hexdigest()}
    if narrowed_roots:
        metrics_by_field.update(narrowed_licenses=narrowed_roots, narrowed_queries=narrowed_queries,
                       oversized_response_bytes=oversized_bytes, complete_response_bytes=state_by_field["total_bytes"] - oversized_bytes,
                       retained_http_responses=len(licenses) + narrowed_queries)
    return metrics_by_field
