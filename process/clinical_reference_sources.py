# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Download, cancellation, and MED-RT source contracts for clinical references."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

import asyncpg
import redis

from db.models import db
from process.clinical_reference_rows import (
    NLM_ATTRIBUTION,
    _concept_row,
    _relationship_row,
    _synonym_row,
)
from process.control_cancel import ImportCancelledError
from process.redis_config import build_redis_settings

UMLS_RELEASES_URL = "https://uts-ws.nlm.nih.gov/releases"
UMLS_DOWNLOAD_URL = "https://uts-ws.nlm.nih.gov/download"
RXCLASS_BY_RXCUI_URL = "https://rxnav.nlm.nih.gov/REST/rxclass/class/byRxcui.json"

DEFAULT_CLINICAL_REFERENCE_SOURCES = "icd10cm,mesh,rxnorm,medrt"
RESTRICTED_SOURCE_ALIASES = {"snomed"}


def _now_isoformat() -> str:
    import datetime

    return datetime.datetime.utcnow().isoformat() + "Z"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source_stream:
        for artifact_chunk in iter(lambda: source_stream.read(1024 * 1024), b""):
            digest.update(artifact_chunk)
    return digest.hexdigest()


def _is_cancel_requested(run_id: str | None) -> bool:
    if not run_id:
        return False
    try:
        redis_settings = build_redis_settings()
        redis_dsn = os.getenv("HLTHPRT_REDIS_ADDRESS")
        if redis_dsn:
            cancel_client = redis.Redis.from_url(
                redis_dsn,
                socket_connect_timeout=2,
                socket_timeout=2,
            )
        else:
            cancel_client = redis.Redis(
                host=redis_settings.host,
                port=redis_settings.port,
                password=redis_settings.password,
                db=redis_settings.database,
                socket_connect_timeout=2,
                socket_timeout=2,
            )
        cancel_value = cancel_client.get(f"cancel:{run_id}")
        return cancel_value in {b"1", "1", 1, True}
    except Exception:
        return False


def _raise_if_cancelled(run_id: str | None) -> None:
    if _is_cancel_requested(run_id):
        raise ImportCancelledError(f"import run {run_id} was cancelled")


def _umls_download_url(url: str, api_key: str) -> str:
    return (
        f"{UMLS_DOWNLOAD_URL}?url={urllib.parse.quote(url, safe='')}"
        f"&apiKey={urllib.parse.quote(api_key)}"
    )


def _redact_sensitive_url(value: str) -> str:
    return re.sub(r"(?i)(apiKey=)[^\s&]+", r"\1<redacted>", str(value))


def _discard_partial_download(temporary_path: Path) -> None:
    try:
        temporary_path.unlink(missing_ok=True)
    except OSError:
        return


def _download_url(
    url: str,
    path: Path,
    *,
    api_key: str | None = None,
    force: bool = False,
    run_id: str | None = None,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size > 0 and not force:
        return path
    _raise_if_cancelled(run_id)
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    request_url = _umls_download_url(url, api_key) if api_key else url
    request = urllib.request.Request(
        request_url,
        headers={"User-Agent": "HealthPorta terminology importer"},
    )
    try:
        with urllib.request.urlopen(request, timeout=3600) as response, temporary_path.open(
            "wb"
        ) as output_stream:
            while True:
                _raise_if_cancelled(run_id)
                artifact_chunk = response.read(1024 * 1024)
                if not artifact_chunk:
                    break
                output_stream.write(artifact_chunk)
        _raise_if_cancelled(run_id)
        temporary_path.replace(path)
    except ImportCancelledError:
        _discard_partial_download(temporary_path)
        raise
    except Exception as exc:
        _discard_partial_download(temporary_path)
        redacted_url = _redact_sensitive_url(request_url)
        redacted_error = _redact_sensitive_url(str(exc))
        raise RuntimeError(f"download failed for {redacted_url}: {redacted_error}") from exc
    manifest_map = {
        "source_url": url,
        "downloaded_at": _now_isoformat(),
        "byte_count": path.stat().st_size,
        "sha256": _sha256_file(path),
    }
    path.with_suffix(path.suffix + ".manifest.json").write_text(
        json.dumps(manifest_map, indent=2),
        encoding="utf-8",
    )
    return path


def _release_current(release_type: str) -> dict[str, Any]:
    releases_url = f"{UMLS_RELEASES_URL}?releaseType={release_type}"
    with urllib.request.urlopen(releases_url, timeout=60) as response:
        release_maps = json.loads(response.read().decode("utf-8"))
    for release_map in release_maps:
        if release_map.get("current"):
            return release_map
    if release_maps:
        return release_maps[0]
    raise RuntimeError(f"No UMLS release found for {release_type}")


async def _load_product_rxcuis(test_limit: int | None = None) -> list[str]:
    rx_schema = (
        os.getenv("HLTHPRT_RX_DB_SCHEMA")
        or os.getenv("HLTHPRT_DRUG_DB_SCHEMA")
        or "rx_data"
    )
    rx_database = (
        os.getenv("HLTHPRT_RX_DB_DATABASE")
        or os.getenv("HLTHPRT_DRUG_DB_DATABASE")
        or os.getenv("HLTHPRT_DB_DATABASE")
        or "postgres"
    )
    connection = None
    try:
        connection = await asyncpg.connect(
            host=os.getenv("HLTHPRT_RX_DB_HOST")
            or os.getenv("HLTHPRT_DRUG_DB_HOST")
            or os.getenv("HLTHPRT_DB_HOST")
            or "127.0.0.1",
            port=int(
                os.getenv("HLTHPRT_RX_DB_PORT")
                or os.getenv("HLTHPRT_DRUG_DB_PORT")
                or os.getenv("HLTHPRT_DB_PORT")
                or "5432"
            ),
            user=os.getenv("HLTHPRT_RX_DB_USER")
            or os.getenv("HLTHPRT_DRUG_DB_USER")
            or os.getenv("HLTHPRT_DB_USER")
            or "postgres",
            password=os.getenv("HLTHPRT_RX_DB_PASSWORD")
            or os.getenv("HLTHPRT_DRUG_DB_PASSWORD")
            or os.getenv("HLTHPRT_DB_PASSWORD")
            or "",
            database=rx_database,
        )
        rxcui_rows = await connection.fetch(
            f"""
            SELECT DISTINCT unnest(rxnorm_ids) AS rxcui
              FROM {rx_schema}.product
             WHERE rxnorm_ids IS NOT NULL
            """
        )
        rxcui_values = [
            str(rxcui_map["rxcui"])
            for rxcui_map in rxcui_rows
            if rxcui_map["rxcui"]
        ]
        return rxcui_values[:test_limit] if test_limit else rxcui_values
    except Exception as exc:
        print(
            f"MED-RT source skipped: {rx_schema}.product unavailable "
            f"in {rx_database} ({exc})"
        )
        return []
    finally:
        if connection is not None:
            await connection.close()


def _rxclass_for_rxcui(rxcui: str) -> list[dict[str, Any]]:
    query_string = urllib.parse.urlencode(
        {"rxcui": rxcui, "relaSource": "MEDRT", "relas": "may_treat"}
    )
    last_error: Exception | None = None
    for attempt_number in range(1, 4):
        try:
            with urllib.request.urlopen(
                f"{RXCLASS_BY_RXCUI_URL}?{query_string}",
                timeout=30,
            ) as response:
                response_map = json.loads(response.read().decode("utf-8"))
            return (
                response_map.get("rxclassDrugInfoList", {}).get("rxclassDrugInfo", [])
                or []
            )
        except Exception as exc:
            last_error = exc
            if attempt_number < 3:
                time.sleep(0.5 * attempt_number)
    if last_error:
        raise last_error
    return []


async def _lookup_rxclass(
    rxcui: str,
    semaphore: asyncio.Semaphore,
) -> tuple[str, list[dict[str, Any]]]:
    async with semaphore:
        try:
            return rxcui, await asyncio.to_thread(_rxclass_for_rxcui, rxcui)
        except Exception as exc:
            print(f"RxClass MEDRT lookup failed for RXCUI {rxcui}: {exc}")
            return rxcui, []


def _add_medrt_class_info(
    rxcui: str,
    class_info: dict[str, Any],
    concepts_by_identity: dict[tuple[str, str], dict[str, Any]],
    synonyms_by_identity: dict[tuple[str, str, str], dict[str, Any]],
    relationship_rows: list[dict[str, Any]],
) -> None:
    class_concept_map = class_info.get("rxclassMinConceptItem") or {}
    class_id = class_concept_map.get("classId")
    class_name = class_concept_map.get("className")
    class_type = class_concept_map.get("classType") or "CLASS"
    if not class_id or not class_name:
        return
    code_system = "MESH" if class_id.startswith("D") else "MEDRT"
    code_type = "condition" if class_type == "DISEASE" else "concept"
    concepts_by_identity[(code_system, class_id)] = _concept_row(
        code_system,
        class_id,
        code_type,
        class_name,
        "rxclass_medrt",
        None,
        attribution=NLM_ATTRIBUTION,
    )
    synonyms_by_identity[(code_system, class_id, class_name)] = _synonym_row(
        code_system,
        class_id,
        class_name,
        "preferred",
        "rxclass_medrt",
        NLM_ATTRIBUTION,
    )
    relationship_rows.append(
        _relationship_row(
            "RXNORM",
            rxcui,
            class_info.get("rela") or "may_treat",
            code_system,
            class_id,
            "rxclass_medrt",
        )
    )


async def _load_medrt_from_rxclass(
    test_mode: bool,
) -> tuple[list[dict], list[dict], list[dict]]:
    default_limit = "40" if test_mode else "0"
    rxcui_limit = int(
        os.getenv("HLTHPRT_MEDRT_RXCUI_LIMIT")
        or os.getenv("HLTHPRT_MEDRT_TEST_RXCUI_LIMIT")
        or default_limit
    )
    concurrency = max(1, int(os.getenv("HLTHPRT_MEDRT_RXCLASS_CONCURRENCY", "24")))
    rxcuis = await _load_product_rxcuis(rxcui_limit if rxcui_limit > 0 else None)
    concepts_by_identity: dict[tuple[str, str], dict[str, Any]] = {}
    synonyms_by_identity: dict[tuple[str, str, str], dict[str, Any]] = {}
    relationship_rows: list[dict[str, Any]] = []
    semaphore = asyncio.Semaphore(concurrency)
    print(f"RxClass MEDRT lookup start: rxcuis={len(rxcuis)} concurrency={concurrency}")
    lookup_tasks = [
        asyncio.create_task(_lookup_rxclass(rxcui, semaphore)) for rxcui in rxcuis
    ]
    completed_count = 0
    for lookup_task in asyncio.as_completed(lookup_tasks):
        rxcui, class_infos = await lookup_task
        completed_count += 1
        for class_info in class_infos:
            _add_medrt_class_info(
                rxcui,
                class_info,
                concepts_by_identity,
                synonyms_by_identity,
                relationship_rows,
            )
        if completed_count % 250 == 0 or completed_count == len(rxcuis):
            print(f"RxClass MEDRT progress: {completed_count}/{len(rxcuis)} RXCUIs")
    return (
        list(concepts_by_identity.values()),
        list(synonyms_by_identity.values()),
        relationship_rows,
    )


def _is_restricted_terminology_enabled() -> bool:
    configured_value = str(
        os.getenv("HLTHPRT_ENABLE_RESTRICTED_TERMINOLOGIES") or ""
    )
    return configured_value.strip().lower() in {"1", "true", "yes"}


def _selected_sources(raw: str | None) -> set[str]:
    explicit_sources = (
        raw if raw is not None else os.getenv("HLTHPRT_CLINICAL_REFERENCE_SOURCES")
    )
    configured_sources = explicit_sources or DEFAULT_CLINICAL_REFERENCE_SOURCES
    selected_source_names = {
        source_name.strip().lower()
        for source_name in configured_sources.split(",")
        if source_name.strip()
    }
    restricted_source_names = selected_source_names & RESTRICTED_SOURCE_ALIASES
    if restricted_source_names and not _is_restricted_terminology_enabled():
        joined_source_names = ", ".join(sorted(restricted_source_names))
        raise RuntimeError(
            "restricted terminology source(s) require "
            "HLTHPRT_ENABLE_RESTRICTED_TERMINOLOGIES=1: "
            f"{joined_source_names}"
        )
    return selected_source_names
