# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable, catalog-only visibility for UHC's official file listings."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import os
import re
from collections.abc import Mapping
from typing import Any

import aiohttp

from db.models import db
from process.uhc_provider_file_catalog_artifacts import (
    fetch_catalog_snapshot,
    retain_catalog_snapshot,
    validate_retained_catalog_proof,
)
from process.uhc_provider_file_catalog_store import (
    CATALOG_FILE_TABLE,
    CATALOG_SCHEMA_VERSION,
    CATALOG_SET_TABLE,
    RAW_OBSERVATION_TABLE,
    _catalog_counts,
    _assert_persisted_files,
    _file_parameters,
    _source_collection_summary,
    _table,
    record_catalog_observation,
)
from process.uhc_provider_file_catalog_types import (
    CATALOG_CONTRACT,
    CATALOG_CONTRACT_LIMITS,
    CATALOG_URLS,
    UHCFileCatalogError,
    UHCFileCatalogItem,
    UHCFileCatalogNotFound,
    UHCObservedFileCatalog,
    observed_catalog_from_payloads,
    validate_observed_catalog,
)
from process.uhc_provider_file_source_identity import (
    UHC_PROVIDER_FILE_ADAPTER_ID,
    UHC_PROVIDER_FILE_CATALOG_SOURCE_ID,
    UHC_PROVIDER_FILE_DISPLAY_NAME,
    UHC_PROVIDER_FILE_ENTRY_ID,
    UHC_PROVIDER_FILE_OWNER_ID,
    UHC_PROVIDER_FILE_PORTAL_BASE,
)


DEFAULT_STALE_SECONDS = 24 * 60 * 60
UHC_ENTRY_ID = "uhc"
UHC_SOURCE_ENTRY_ID = UHC_PROVIDER_FILE_ENTRY_ID
UHC_CATALOG_SOURCE_ID = UHC_PROVIDER_FILE_CATALOG_SOURCE_ID
UHC_PROFILE_SOURCE_IDS: tuple[str, ...] = ()
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def _row_fields(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    mapping = row._mapping if hasattr(row, "_mapping") else row
    return dict(mapping)


def _json_value(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (ValueError, RecursionError) as error:
            raise UHCFileCatalogError("UHC persisted catalog JSON is invalid") from error
    return value


def _collection_support(summary: Mapping[str, Any]) -> str:
    file_count = summary.get("file_count")
    if isinstance(file_count, bool) or not isinstance(file_count, int) or file_count < 0:
        raise UHCFileCatalogError("UHC persisted collection summary is invalid")
    return "cataloged" if file_count else "not_applicable"


def _full_collection_summary(value: Any) -> tuple[dict[str, Any], ...]:
    summaries = _json_value(value)
    if not isinstance(summaries, list):
        raise UHCFileCatalogError("UHC persisted collection summary is invalid")
    if any(not isinstance(summary, Mapping) for summary in summaries):
        raise UHCFileCatalogError("UHC persisted collection summary is invalid")
    return tuple(
        {
            **dict(summary),
            "catalog_support": _collection_support(summary),
        }
        for summary in summaries
    )


def _validate_selector(value: str | None, selector_name: str) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not _SHA256_PATTERN.fullmatch(normalized):
        raise ValueError(f"{selector_name} must be a lowercase SHA-256 hash")
    return normalized


async def _selected_observation(
    catalog_set_sha256: str | None,
    raw_set_sha256: str | None,
) -> dict[str, Any]:
    catalog_hash = _validate_selector(catalog_set_sha256, "catalog_set_sha256")
    raw_hash = _validate_selector(raw_set_sha256, "raw_set_sha256")
    if catalog_hash and raw_hash:
        raise ValueError("catalog_set_sha256 and raw_set_sha256 are mutually exclusive")
    where_clause = ""
    selection_parameters_by_name: dict[str, Any] = {}
    if catalog_hash:
        where_clause = "WHERE semantic.catalog_set_sha256=:catalog_set_sha256"
        selection_parameters_by_name["catalog_set_sha256"] = catalog_hash
    elif raw_hash:
        where_clause = "WHERE raw.raw_set_sha256=:raw_set_sha256"
        selection_parameters_by_name["raw_set_sha256"] = raw_hash
    observation_fields = _row_fields(
        await db.first(
            f"""SELECT semantic.catalog_set_sha256, semantic.schema_version,
                       semantic.families_json, semantic.collection_summary_json,
                       semantic.file_count, semantic.provider_file_count,
                       semantic.plan_reference_file_count,
                       raw.raw_set_sha256, raw.raw_documents_json,
                       raw.first_observed_at AS observation_first_observed_at,
                       raw.last_observed_at AS observation_last_observed_at
                  FROM {_table(RAW_OBSERVATION_TABLE)} raw
                  JOIN {_table(CATALOG_SET_TABLE)} semantic
                    ON semantic.catalog_set_sha256=raw.catalog_set_sha256
                  {where_clause}
                 ORDER BY raw.last_observed_at DESC, raw.raw_set_sha256 DESC
                 LIMIT 1;""",
            **selection_parameters_by_name,
        )
    )
    if not observation_fields and (catalog_hash or raw_hash):
        raise UHCFileCatalogNotFound(catalog_hash or raw_hash or "")
    return observation_fields


def _iso_utc(value: Any) -> str | None:
    if not isinstance(value, dt.datetime):
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")


def _stale_seconds() -> int:
    try:
        seconds = int(
            os.getenv("HLTHPRT_UHC_CATALOG_STALE_SECONDS", str(DEFAULT_STALE_SECONDS))
        )
    except ValueError as error:
        raise UHCFileCatalogError("UHC catalog staleness threshold is invalid") from error
    if seconds <= 0:
        raise UHCFileCatalogError("UHC catalog staleness threshold is invalid")
    return seconds


def _is_stale(observed_at: Any, stale_seconds: int) -> bool:
    if not isinstance(observed_at, dt.datetime):
        return True
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=dt.UTC)
    return dt.datetime.now(dt.UTC) - observed_at.astimezone(dt.UTC) > dt.timedelta(
        seconds=stale_seconds
    )


def _empty_catalog_payload() -> dict[str, Any]:
    return {
        "schema_version": CATALOG_SCHEMA_VERSION,
        "catalog_contract": CATALOG_CONTRACT,
        "catalog_limits": dict(CATALOG_CONTRACT_LIMITS),
        "entry_id": UHC_ENTRY_ID,
        "source_entry_id": UHC_SOURCE_ENTRY_ID,
        "display_name": UHC_PROVIDER_FILE_DISPLAY_NAME,
        "owner_id": UHC_PROVIDER_FILE_OWNER_ID,
        "catalog_source_id": UHC_CATALOG_SOURCE_ID,
        "portal_base": UHC_PROVIDER_FILE_PORTAL_BASE,
        "adapter_id": UHC_PROVIDER_FILE_ADAPTER_ID,
        "registered_source_id": None,
        "source_ids": list(UHC_PROFILE_SOURCE_IDS),
        "catalog_mode": "catalog_only",
        "catalog_observed": False,
        "acquisition_runnable": False,
        "profile_eligible": False,
        "provider_directory_current": False,
        "fhir_publication_ready": False,
        "reference_aware_gc_ready": False,
        "catalog_set_sha256": None,
        "raw_set_sha256": None,
        "catalog_file_count": 0,
        "provider_membership_file_count": 0,
        "plan_reference_file_count": 0,
        "collections": [],
        "raw_observation": None,
        "items": [],
        "latest_file_outcomes_available": False,
        "latest_file_outcomes_reason": "catalog_only_not_imported",
    }


def _catalog_file_from_record(file_record: Mapping[str, Any]) -> UHCFileCatalogItem:
    return UHCFileCatalogItem(
        family=str(file_record.get("family") or ""),
        collection_kind=str(file_record.get("collection_kind") or ""),
        file_id=str(file_record.get("file_id") or ""),
        file_name=str(file_record.get("file_name") or ""),
        source_url=str(file_record.get("source_url") or ""),
        catalog_modified_at=str(file_record.get("catalog_modified_at") or ""),
        catalog_entry_sha256=str(file_record.get("catalog_entry_sha256") or ""),
        size_bytes=file_record.get("size_bytes"),
    )


def _public_raw_observation(
    proof: dict[str, Any],
    first_observed_at: Any,
    last_observed_at: Any,
) -> dict[str, Any]:
    return {
        "raw_set_sha256": proof["raw_set_sha256"],
        "first_observed_at": _iso_utc(first_observed_at),
        "last_observed_at": _iso_utc(last_observed_at),
        "documents": [
            {
                "family": document["family"],
                "url": document["url"],
                "response_url": document["response_url"],
                "raw_sha256": document["raw_sha256"],
                "byte_count": document["byte_count"],
                "retained": True,
            }
            for document in proof["documents"]
        ],
    }


async def _catalog_file_records(
    selected_catalog_hash: str,
) -> list[dict[str, Any]]:
    database_records = await db.all(
        f"""SELECT catalog_set_sha256, file_id, family, collection_kind,
                   file_name, source_url, catalog_modified_at,
                   catalog_entry_sha256, size_bytes, availability,
                   catalog_support
              FROM {_table(CATALOG_FILE_TABLE)}
             WHERE catalog_set_sha256=:catalog_set_sha256
             ORDER BY family, collection_kind, file_name, file_id;""",
        catalog_set_sha256=selected_catalog_hash,
    )
    return [_row_fields(database_record) for database_record in database_records]


def _validated_persisted_catalog(
    observation_fields: Mapping[str, Any],
    file_records: list[dict[str, Any]],
) -> tuple[UHCObservedFileCatalog, dict[str, int]]:
    if observation_fields.get("schema_version") != CATALOG_SCHEMA_VERSION:
        raise UHCFileCatalogError("UHC persisted catalog schema version is inconsistent")
    persisted_families = _json_value(observation_fields.get("families_json"))
    if persisted_families != sorted(CATALOG_URLS):
        raise UHCFileCatalogError("UHC persisted catalog families are inconsistent")
    catalog_files = tuple(
        _catalog_file_from_record(file_record) for file_record in file_records
    )
    collection_summaries = _full_collection_summary(
        observation_fields["collection_summary_json"]
    )
    persisted_catalog = UHCObservedFileCatalog(
        files=catalog_files,
        catalog_set_sha256=str(observation_fields["catalog_set_sha256"]),
        collection_summary=collection_summaries,
    )
    validate_observed_catalog(persisted_catalog)
    _assert_persisted_files(file_records, _file_parameters(persisted_catalog))
    catalog_counts_by_name = _catalog_counts(persisted_catalog)
    if any(
        observation_fields.get(count_name) != count
        for count_name, count in catalog_counts_by_name.items()
    ):
        raise UHCFileCatalogError("UHC persisted catalog counts are inconsistent")
    return persisted_catalog, catalog_counts_by_name


async def _validated_persisted_raw_proof(
    observation_fields: Mapping[str, Any],
) -> tuple[dict[str, Any], UHCObservedFileCatalog]:
    proof_by_field = {
        "raw_set_sha256": str(observation_fields["raw_set_sha256"]),
        "documents": _json_value(observation_fields["raw_documents_json"]),
    }
    return await asyncio.to_thread(validate_retained_catalog_proof, proof_by_field)


def _public_catalog_files(
    file_records: list[dict[str, Any]],
    catalog_files: tuple[UHCFileCatalogItem, ...],
) -> list[dict[str, Any]]:
    public_files: list[dict[str, Any]] = []
    for file_record, catalog_file in zip(file_records, catalog_files, strict=True):
        public_files.append(
            {
                **catalog_file.identity_payload(),
                "availability": file_record["availability"],
                "catalog_support": file_record["catalog_support"],
                "imported": False,
                "data_file_raw_sha256": None,
                "latest_file_outcome": None,
                "latest_file_outcome_reason": "catalog_only_not_imported",
            }
        )
    return public_files


def _observed_catalog_document(
    observation_fields: Mapping[str, Any],
    persisted_catalog: UHCObservedFileCatalog,
    catalog_counts_by_name: dict[str, int],
    raw_proof: dict[str, Any],
    public_files: list[dict[str, Any]],
) -> dict[str, Any]:
    observed_at = observation_fields.get("observation_last_observed_at")
    stale_seconds = _stale_seconds()
    return {
        "schema_version": CATALOG_SCHEMA_VERSION,
        "catalog_contract": CATALOG_CONTRACT,
        "catalog_limits": dict(CATALOG_CONTRACT_LIMITS),
        "entry_id": UHC_ENTRY_ID,
        "source_entry_id": UHC_SOURCE_ENTRY_ID,
        "display_name": UHC_PROVIDER_FILE_DISPLAY_NAME,
        "owner_id": UHC_PROVIDER_FILE_OWNER_ID,
        "catalog_source_id": UHC_CATALOG_SOURCE_ID,
        "portal_base": UHC_PROVIDER_FILE_PORTAL_BASE,
        "adapter_id": UHC_PROVIDER_FILE_ADAPTER_ID,
        "registered_source_id": None,
        "source_ids": list(UHC_PROFILE_SOURCE_IDS),
        "catalog_mode": "catalog_only",
        "catalog_observed": True,
        "acquisition_runnable": False,
        "profile_eligible": False,
        "provider_directory_current": False,
        "fhir_publication_ready": False,
        "reference_aware_gc_ready": False,
        "catalog_set_sha256": persisted_catalog.catalog_set_sha256,
        "raw_set_sha256": raw_proof["raw_set_sha256"],
        "catalog_file_count": catalog_counts_by_name["file_count"],
        "provider_membership_file_count": catalog_counts_by_name[
            "provider_file_count"
        ],
        "plan_reference_file_count": catalog_counts_by_name[
            "plan_reference_file_count"
        ],
        "collections": list(persisted_catalog.collection_summary),
        "catalog_observed_at": _iso_utc(observed_at),
        "catalog_stale": _is_stale(observed_at, stale_seconds),
        "catalog_stale_after_seconds": stale_seconds,
        "raw_observation": _public_raw_observation(
            raw_proof,
            observation_fields.get("observation_first_observed_at"),
            observed_at,
        ),
        "items": public_files,
        "latest_file_outcomes_available": False,
        "latest_file_outcomes_reason": "catalog_only_not_imported",
    }


async def uhc_provider_file_catalog(
    *,
    catalog_set_sha256: str | None = None,
    raw_set_sha256: str | None = None,
) -> dict[str, Any]:
    """Return one authenticated catalog view without acquisition state."""

    observation = await _selected_observation(catalog_set_sha256, raw_set_sha256)
    if not observation:
        return _empty_catalog_payload()
    file_records = await _catalog_file_records(str(observation["catalog_set_sha256"]))
    persisted_catalog, catalog_counts_by_name = _validated_persisted_catalog(
        observation,
        file_records,
    )
    raw_proof, retained_catalog = await _validated_persisted_raw_proof(observation)
    if retained_catalog != persisted_catalog:
        raise UHCFileCatalogError(
            "UHC retained raw catalog does not match persisted semantics"
        )
    public_files = _public_catalog_files(file_records, persisted_catalog.files)
    return _observed_catalog_document(
        observation,
        persisted_catalog,
        catalog_counts_by_name,
        raw_proof,
        public_files,
    )


async def refresh_uhc_provider_file_catalog() -> dict[str, Any]:
    """Refresh only UHC's two small listing documents and retain exact proof."""

    timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=20)
    try:
        async with aiohttp.ClientSession(
            timeout=timeout,
            auto_decompress=False,
            headers={"User-Agent": "HealthPorta-UHC-File-Catalog/1.0"},
        ) as session:
            snapshot = await fetch_catalog_snapshot(session)
    except asyncio.TimeoutError as error:
        raise UHCFileCatalogError("UHC catalog request timed out") from error
    except aiohttp.ClientError as error:
        raise UHCFileCatalogError("UHC catalog transport is unavailable") from error
    raw_proof = await asyncio.to_thread(retain_catalog_snapshot, snapshot)
    catalog = observed_catalog_from_payloads(snapshot.payloads_by_family)
    await record_catalog_observation(catalog, raw_proof)
    return await uhc_provider_file_catalog(raw_set_sha256=snapshot.raw_set_sha256)
