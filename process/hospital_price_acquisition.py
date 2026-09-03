# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Hospital registry, locator, source-download, and native-parser operations."""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from db.models import HospitalPriceVersion, db
from process.control_cancel import ImportCancelledError
from process.formulary_fhir.async_safety import drain_operation
from process.hospital_hpt_locator import (
    MAX_HOSPITAL_HPT_LOCATOR_BYTES,
    HospitalHptLocatorRecord,
    match_hospital_hpt_locator,
    parse_hospital_hpt_locator,
)
from process.hospital_price_native import (
    HospitalParserReceipt,
    validate_hospital_parser_summary,
)
from process.ptg_parts.artifacts import PTG2ArtifactStore
from process.ptg_parts.canonical import canonicalize_url
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.rust_scanner import (
    _ptg2_rust_scanner_binary,
    _ptg2_scanner_binary_profile,
    _subprocess_session_options,
    _terminate_asyncio_subprocess_group,
)
from process.ptg_parts.source_download import (
    PTG2_DEFAULT_MAX_BYTES,
    download_raw_artifact,
)


REGISTRY_VERSION = 1
_HOSPITAL_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)


@dataclass(frozen=True)
class LocatorResult:
    url: str
    locator_id: str
    observation_id: str
    hospitals: tuple[dict[str, str], ...]
    records: tuple[HospitalHptLocatorRecord, ...] | None
    error_code: str | None = None
    error_detail: str | None = None
    fetch_failed: bool = False


@dataclass(frozen=True)
class Candidate:
    hospital_id: str
    hospital_name: str
    locator_id: str
    observation_id: str
    source_url: str
    locator_name: str | None = None
    locator_url: str | None = None
    initial_error_code: str | None = None
    initial_error_detail: str | None = None


@dataclass
class Attempt:
    attempt_id: str
    hospital_id: str
    hospital_name: str
    source_url: str
    expected_generation: int
    locator_name: str | None = None
    locator_url: str | None = None
    final_source_url: str | None = None
    source_http_status: int | None = None


@dataclass(frozen=True)
class DownloadedSource:
    url: str
    raw: Any | None
    attempts: tuple[Attempt, ...]
    error_code: str | None = None
    error_detail: str | None = None
    auth_refresh_required: bool = False


def schema_name() -> str:
    """Return the installed hospital-price schema name."""

    return str(HospitalPriceVersion.__table__.schema or "mrf")


def positive_env(name: str, default: int) -> int:
    """Read a positive integer environment setting or use its default."""

    try:
        return max(int(os.getenv(name, "").strip() or default), 1)
    except ValueError:
        return default


def error_details(exc: BaseException) -> tuple[str, str]:
    """Reduce an exception to bounded attempt error fields."""

    code = exc.__class__.__name__.replace("Error", "").lower()[:64] or "error"
    return code, (" ".join(str(exc).split())[:2000] or code)


def locator_id(url: str) -> str:
    """Build the stable database identifier for a locator URL."""

    return hashlib.sha256(f"hospital-price-locator\0{url}".encode()).hexdigest()


def _registry_records(
    hospitals: Sequence[dict[str, str]],
) -> list[tuple[Any, ...]]:
    return [
        (
            hospital["hospital_id"],
            None,
            locator_id(hospital["cms_hpt_url"]),
            hospital["name"],
            REGISTRY_VERSION,
        )
        for hospital in hospitals
    ]


async def _copy_registry_stages(
    connection: Any,
    hospitals: Sequence[dict[str, str]],
    locator_stage_name: str,
    hospital_stage_name: str,
) -> None:
    driver = getattr(
        connection.raw_connection, "driver_connection", connection.raw_connection
    )
    await driver.copy_records_to_table(
        locator_stage_name,
        columns=["locator_id", "cms_hpt_url"],
        records=tuple(dict.fromkeys(
            (locator_id(row["cms_hpt_url"]), row["cms_hpt_url"])
            for row in hospitals
        )),
    )
    await driver.copy_records_to_table(
        hospital_stage_name,
        columns=["hospital_id", "facility_anchor_id", "locator_id", "name",
                 "registry_version"],
        records=_registry_records(hospitals),
    )


async def sync_registry(hospitals: Sequence[dict[str, str]]) -> None:
    """Upsert selected registry hospitals and their immutable locator rows."""

    schema = _quote_ident(schema_name())
    token = uuid.uuid4().hex[:12]
    locator_stage_name = f"hospital_locator_registry_{token}"
    hospital_stage_name = f"hospital_registry_{token}"
    locator_stage, hospital_stage = map(
        _quote_ident, (locator_stage_name, hospital_stage_name)
    )
    async with db.acquire() as connection:
        if not await connection.scalar(
            "SELECT to_regclass(:hospital) IS NOT NULL "
            "AND to_regclass(:packed_root) IS NOT NULL "
            "AND to_regclass(:data_block) IS NOT NULL",
            hospital=f"{schema_name()}.hospital_price_hospital",
            packed_root=f"{schema_name()}.hospital_price_packed_root",
            data_block=f"{schema_name()}.hospital_price_data_block",
        ):
            raise RuntimeError("hospital price storage migration is not installed")
        await connection.status(
            f"CREATE TEMP TABLE {locator_stage} "
            "(locator_id varchar(64), cms_hpt_url text) ON COMMIT DROP"
        )
        await connection.status(
            f"CREATE TEMP TABLE {hospital_stage} ("
            "hospital_id varchar(64), facility_anchor_id varchar(128), "
            "locator_id varchar(64), name varchar(256), registry_version integer"
            ") ON COMMIT DROP"
        )
        await _copy_registry_stages(
            connection, hospitals, locator_stage_name, hospital_stage_name,
        )
        await connection.status(
            f"INSERT INTO {schema}.hospital_price_locator(locator_id, cms_hpt_url) "
            f"SELECT locator_id, cms_hpt_url FROM {locator_stage} "
            "ON CONFLICT (locator_id) DO UPDATE SET cms_hpt_url=EXCLUDED.cms_hpt_url"
        )
        await connection.status(
            f"INSERT INTO {schema}.hospital_price_hospital("
            "hospital_id, facility_anchor_id, locator_id, name, registry_version) "
            f"SELECT hospital_id, facility_anchor_id, locator_id, name, registry_version "
            f"FROM {hospital_stage} ON CONFLICT (hospital_id) DO UPDATE SET "
            "locator_id=EXCLUDED.locator_id, name=EXCLUDED.name, "
            "registry_version=EXCLUDED.registry_version, "
            "updated_at=transaction_timestamp()"
        )
        await connection.status(
            f"INSERT INTO {schema}.hospital_price_current(hospital_id) "
            f"SELECT hospital_id FROM {hospital_stage} ON CONFLICT DO NOTHING"
        )


async def _record_locator_observation(
    url: str,
    locator: str,
    observation: str,
    status: str,
    raw: Any | None = None,
    error_code: str | None = None,
    error_detail: str | None = None,
) -> None:
    schema = _quote_ident(schema_name())
    head = raw.head if raw is not None else None
    await db.status(
        f"INSERT INTO {schema}.hospital_price_locator_observation("
        "observation_id, locator_id, registry_version, requested_url, final_url, "
        "result_status, http_status, response_sha256, response_byte_count, checked_at, "
        "error_code, error_detail) VALUES ("
        ":observation, :locator, :version, :url, :final_url, :status, :http_status, "
        ":sha256, :byte_count, :checked_at, :error_code, :error_detail)",
        observation=observation, locator=locator, version=REGISTRY_VERSION, url=url,
        final_url=(str(head.url) if head else None),
        status=status, http_status=(int(head.status) if head and head.status else None),
        sha256=(raw.raw_sha256 if raw else None),
        byte_count=(raw.byte_count if raw else None), checked_at=dt.datetime.now(dt.UTC),
        error_code=error_code, error_detail=error_detail,
    )


async def fetch_locator(
    locator_group: tuple[str, tuple[dict[str, str], ...]], store: PTG2ArtifactStore
) -> LocatorResult:
    """Fetch, parse, and persist one deduplicated locator observation."""

    url, hospitals = locator_group
    locator, observation, raw = locator_id(url), uuid.uuid4().hex, None
    try:
        raw = await download_raw_artifact(
            url, store=store, reuse_raw_artifacts=False,
            max_bytes=MAX_HOSPITAL_HPT_LOCATOR_BYTES,
            keep_partial_artifacts=False, exact_get_evidence=True,
            user_agent=_HOSPITAL_USER_AGENT,
        )
        locator_payload = await asyncio.to_thread(Path(raw.raw_path).read_bytes)
        locator_records = parse_hospital_hpt_locator(locator_payload)
        status = "redirected_verified" if raw.head and raw.head.url != url else "verified"
        await _record_locator_observation(url, locator, observation, status, raw)
        return LocatorResult(url, locator, observation, hospitals, locator_records)
    except (ImportCancelledError, asyncio.CancelledError):
        raise
    except Exception as exc:
        code, detail = error_details(exc)
        is_fetch_failure = raw is None and (
            getattr(exc, "_ptg2_response_body_started", None) is False
        )
        await _record_locator_observation(
            url, locator, observation,
            "fetch_failed" if is_fetch_failure else "invalid",
            raw, code, detail,
        )
        return LocatorResult(
            url,
            locator,
            observation,
            hospitals,
            None,
            code,
            detail,
            fetch_failed=is_fetch_failure,
        )


def _locator_error_candidates(locator_result: LocatorResult) -> tuple[Candidate, ...]:
    candidates: list[Candidate] = []
    for hospital in locator_result.hospitals:
        fallback_url = (
            hospital.get("fallback_mrf_url")
            if locator_result.fetch_failed
            else None
        )
        candidates.append(
            Candidate(
                hospital_id=hospital["hospital_id"],
                hospital_name=hospital["name"],
                locator_id=locator_result.locator_id,
                observation_id=locator_result.observation_id,
                source_url=fallback_url or locator_result.url,
                locator_name=hospital.get("locator_name") or hospital["name"],
                locator_url=locator_result.url,
                initial_error_code=(
                    None
                    if fallback_url
                    else locator_result.error_code or "locator_invalid"
                ),
                initial_error_detail=(
                    None if fallback_url else locator_result.error_detail
                ),
            )
        )
    return tuple(candidates)


def candidates_from_locators(
    locator_results: Sequence[LocatorResult],
) -> tuple[Candidate, ...]:
    """Resolve exact locator bindings into per-hospital MRF candidates."""

    candidates: list[Candidate] = []
    for locator_result in locator_results:
        if locator_result.records is None:
            candidates.extend(_locator_error_candidates(locator_result))
            continue
        match = match_hospital_hpt_locator(
            locator_result.hospitals, locator_result.url, locator_result.records
        )
        hospital_by_id = {
            hospital["hospital_id"]: hospital
            for hospital in locator_result.hospitals
        }
        candidates.extend(
            Candidate(
                hospital_id=binding.hospital_id,
                hospital_name=hospital_by_id[binding.hospital_id]["name"],
                locator_id=locator_result.locator_id,
                observation_id=locator_result.observation_id,
                source_url=hospital_by_id[binding.hospital_id].get("fallback_mrf_url")
                or binding.mrf_url,
                locator_name=(
                    locator_result.records[binding.record_index].location_name
                    if binding.record_index is not None
                    else None
                ),
                locator_url=locator_result.url,
            )
            for binding in match.bindings
        )
        for ids, code, detail in (
            (match.ambiguous_hospital_ids, "locator_ambiguous", "locator name is not one-to-one"),
            (match.unmatched_hospital_ids, "locator_unmatched", "locator has no exact name match"),
        ):
            candidates.extend(
                Candidate(
                    hospital_id=hospital_id,
                    hospital_name=hospital_by_id[hospital_id]["name"],
                    locator_id=locator_result.locator_id,
                    observation_id=locator_result.observation_id,
                    source_url=locator_result.url,
                    locator_name=(
                        hospital_by_id[hospital_id].get("locator_name")
                        or hospital_by_id[hospital_id]["name"]
                    ),
                    locator_url=locator_result.url,
                    initial_error_code=code,
                    initial_error_detail=detail,
                )
                for hospital_id in ids
            )
    return tuple(candidates)


async def download_source(
    source_job: tuple[str, tuple[Attempt, ...]], store: PTG2ArtifactStore,
    max_bytes: int,
    *,
    exact_url_only: bool = False,
) -> DownloadedSource:
    """Download one canonical MRF URL for all associated attempts."""

    url, attempts = source_job
    last_error: tuple[str, str] | None = None
    for attempt in attempts:
        attempt.final_source_url = None
        attempt.source_http_status = None
    request_urls = (
        (url,)
        if exact_url_only
        else tuple(dict.fromkeys((url, *(attempt.source_url for attempt in attempts))))
    )
    for request_url in request_urls:
        raw = None
        download_error: Exception | None = None
        for user_agent in (_HOSPITAL_USER_AGENT, None):
            try:
                raw = await download_raw_artifact(
                    request_url, store=store, reuse_raw_artifacts=False,
                    max_bytes=max_bytes, keep_partial_artifacts=False,
                    **({"user_agent": user_agent} if user_agent else {}),
                )
                break
            except (ImportCancelledError, asyncio.CancelledError):
                raise
            except Exception as exc:
                if download_error is None:
                    download_error = exc
                if not (
                    user_agent is not None
                    and getattr(exc, "status", None) == 403
                    and getattr(exc, "_ptg2_response_body_started", None) is False
                ):
                    break
        if raw is None:
            assert download_error is not None
            last_error = error_details(download_error)
            status = getattr(download_error, "status", None)
            if type(status) is int and 100 <= status <= 599:
                affected_attempts = (
                    attempts
                    if exact_url_only
                    else tuple(
                        attempt for attempt in attempts
                        if attempt.source_url == request_url
                    )
                )
                for attempt in affected_attempts:
                    attempt.final_source_url = request_url
                    attempt.source_http_status = status
            continue
        for attempt in attempts:
            if raw.head:
                attempt.final_source_url = str(raw.head.url)
                attempt.source_http_status = int(raw.head.status) if raw.head.status else None
        return DownloadedSource(request_url, raw, attempts)
    assert last_error is not None
    return DownloadedSource(
        url, None, attempts, *last_error,
        auth_refresh_required=any(
            attempt.source_http_status in {401, 403} for attempt in attempts
        ),
    )


def _release_parser_binary() -> Path:
    binary = _ptg2_rust_scanner_binary()
    if binary is None or _ptg2_scanner_binary_profile(binary) == "debug":
        raise RuntimeError("hospital MRF imports require a release Rust parser")
    return binary


async def run_native_parser(
    input_path: Path,
    output_directory: Path,
    version_id: str,
    source_format: str,
    input_bytes: int,
    max_decompressed_bytes: int,
    max_output_bytes: int,
) -> HospitalParserReceipt:
    """Run the release native parser and validate its complete receipt."""

    binary = _release_parser_binary()
    process = None
    spawn = asyncio.create_task(
        asyncio.create_subprocess_exec(
            str(binary), "--hospital-mrf-copy", source_format, version_id,
            str(input_path), str(output_directory),
            str(max_decompressed_bytes), str(max_output_bytes), "packed",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            **_subprocess_session_options(asyncio.create_subprocess_exec),
        )
    )
    try:
        process = await asyncio.shield(spawn)
        stdout, stderr = await process.communicate()
        if process.returncode:
            detail = stderr.decode("utf-8", errors="replace")[-2000:]
            raise RuntimeError(f"hospital MRF parser exited {process.returncode}: {detail}")
        return await drain_operation(
            asyncio.to_thread(
                validate_hospital_parser_summary,
                stdout,
                version_id=version_id,
                source_format=source_format,
                input_bytes=input_bytes,
                output_directory=output_directory,
                max_decompressed_bytes=max_decompressed_bytes,
                max_output_bytes=max_output_bytes,
            ),
            preserve_cancellation=True,
        )
    except BaseException:
        if process is None:
            async def _wait_for_spawn() -> Any:
                return await spawn

            try:
                process = await drain_operation(
                    _wait_for_spawn(), preserve_cancellation=False
                )
            except BaseException:
                process = None
        if process is not None and process.returncode is None:
            await drain_operation(
                _terminate_asyncio_subprocess_group(process),
                preserve_cancellation=False,
            )
        raise
