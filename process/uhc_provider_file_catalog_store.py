# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Transactional storage boundary for immutable UHC catalog observations."""

from __future__ import annotations

import asyncio
import json
import os
import re
from collections.abc import Mapping
from typing import Any

from sqlalchemy import text as sa_text

from db.models import db
from process.uhc_provider_file_catalog_artifacts import validate_retained_catalog_proof
from process.uhc_provider_file_catalog_types import (
    CATALOG_URLS,
    PLAN_REFERENCE,
    PROVIDER_MEMBERSHIP,
    UHCFileCatalogError,
    UHCObservedFileCatalog,
    canonical_json,
    validate_observed_catalog,
)


CATALOG_SET_TABLE = "provider_directory_uhc_catalog_set"
CATALOG_FILE_TABLE = "provider_directory_uhc_catalog_file"
RAW_OBSERVATION_TABLE = "provider_directory_uhc_catalog_raw_observation"
CATALOG_SCHEMA_VERSION = 1


def _schema() -> str:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema_name):
        raise UHCFileCatalogError("UHC catalog database schema is invalid")
    return schema_name


def _table(table_name: str) -> str:
    return f'"{_schema()}"."{table_name}"'


def _record_fields(database_record: Any) -> dict[str, Any]:
    if database_record is None:
        return {}
    record_mapping = (
        database_record._mapping
        if hasattr(database_record, "_mapping")
        else database_record
    )
    return dict(record_mapping)


def _first_query_fields(query_result: Any) -> dict[str, Any]:
    result_mappings = getattr(query_result, "mappings", None)
    if callable(result_mappings):
        return _record_fields(result_mappings().first())
    first_record = getattr(query_result, "first", None)
    return _record_fields(first_record() if callable(first_record) else None)


def _all_query_fields(query_result: Any) -> list[dict[str, Any]]:
    result_mappings = getattr(query_result, "mappings", None)
    if callable(result_mappings):
        return [_record_fields(record) for record in result_mappings().all()]
    all_records = getattr(query_result, "all", None)
    database_records = all_records() if callable(all_records) else []
    return [_record_fields(record) for record in database_records]


def _json_document(json_document: Any) -> Any:
    if isinstance(json_document, str):
        try:
            return json.loads(json_document)
        except ValueError as error:
            raise UHCFileCatalogError("UHC persisted catalog JSON is invalid") from error
    return json_document


def _source_collection_summary(
    catalog: UHCObservedFileCatalog,
) -> list[dict[str, Any]]:
    return [
        {
            field_name: field_value
            for field_name, field_value in collection.items()
            if field_name != "catalog_support"
        }
        for collection in catalog.collection_summary
    ]


def _catalog_counts(catalog: UHCObservedFileCatalog) -> dict[str, int]:
    provider_file_count = sum(
        catalog_file.collection_kind == PROVIDER_MEMBERSHIP
        for catalog_file in catalog.files
    )
    plan_reference_file_count = sum(
        catalog_file.collection_kind == PLAN_REFERENCE
        for catalog_file in catalog.files
    )
    return {
        "file_count": len(catalog.files),
        "provider_file_count": provider_file_count,
        "plan_reference_file_count": plan_reference_file_count,
    }


def _file_parameters(catalog: UHCObservedFileCatalog) -> list[dict[str, Any]]:
    summary_by_collection = {
        (collection["family"], collection["collection_kind"]): collection
        for collection in catalog.collection_summary
    }
    return [
        {
            "catalog_set_sha256": catalog.catalog_set_sha256,
            **catalog_file.identity_payload(),
            "availability": summary_by_collection[
                (catalog_file.family, catalog_file.collection_kind)
            ]["availability"],
            "catalog_support": summary_by_collection[
                (catalog_file.family, catalog_file.collection_kind)
            ]["catalog_support"],
        }
        for catalog_file in catalog.files
    ]


def _assert_equal_json(actual_document: Any, expected_document: Any, message: str) -> None:
    if canonical_json(_json_document(actual_document)) != canonical_json(
        expected_document
    ):
        raise UHCFileCatalogError(message)


def _assert_persisted_set(
    set_fields: dict[str, Any],
    catalog: UHCObservedFileCatalog,
    catalog_counts_by_name: dict[str, int],
) -> None:
    expected_scalar_by_name = {
        "catalog_set_sha256": catalog.catalog_set_sha256,
        "schema_version": CATALOG_SCHEMA_VERSION,
        **catalog_counts_by_name,
    }
    if any(
        set_fields.get(field_name) != expected_value
        for field_name, expected_value in expected_scalar_by_name.items()
    ):
        raise UHCFileCatalogError("UHC persisted semantic catalog is inconsistent")
    _assert_equal_json(
        set_fields.get("families_json"),
        sorted(CATALOG_URLS),
        "UHC persisted catalog families are inconsistent",
    )
    _assert_equal_json(
        set_fields.get("collection_summary_json"),
        _source_collection_summary(catalog),
        "UHC persisted collection summary is inconsistent",
    )


def _immutable_file_fields(file_fields: Mapping[str, Any]) -> dict[str, Any]:
    return {
        field_name: file_fields.get(field_name)
        for field_name in (
            "catalog_set_sha256",
            "file_id",
            "family",
            "collection_kind",
            "file_name",
            "source_url",
            "catalog_modified_at",
            "catalog_entry_sha256",
            "size_bytes",
            "availability",
            "catalog_support",
        )
    }


def _file_sort_key(file_fields: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        file_fields["family"],
        file_fields["collection_kind"],
        file_fields["file_name"],
    )


def _assert_persisted_files(
    file_records: list[dict[str, Any]],
    expected_file_parameters: list[dict[str, Any]],
) -> None:
    actual_files = sorted(
        (_immutable_file_fields(file_record) for file_record in file_records),
        key=_file_sort_key,
    )
    expected_files = sorted(
        (
            _immutable_file_fields(file_parameters)
            for file_parameters in expected_file_parameters
        ),
        key=_file_sort_key,
    )
    if actual_files != expected_files:
        raise UHCFileCatalogError("UHC persisted catalog file set is inconsistent")


def _semantic_set_insert_statement():
    return sa_text(
        f"""INSERT INTO {_table(CATALOG_SET_TABLE)} (
            catalog_set_sha256, schema_version, families_json,
            collection_summary_json, file_count, provider_file_count,
            plan_reference_file_count, first_observed_at, last_observed_at
        ) VALUES (
            :catalog_set_sha256, :schema_version, CAST(:families_json AS jsonb),
            CAST(:collection_summary_json AS jsonb), :file_count,
            :provider_file_count, :plan_reference_file_count, now(), now()
        ) ON CONFLICT (catalog_set_sha256) DO UPDATE SET last_observed_at=now();"""
    )


def _semantic_set_select_statement():
    return sa_text(
        f"""SELECT catalog_set_sha256, schema_version, families_json,
                   collection_summary_json, file_count,
                   provider_file_count, plan_reference_file_count
              FROM {_table(CATALOG_SET_TABLE)}
             WHERE catalog_set_sha256=:catalog_set_sha256;"""
    )


def _catalog_file_insert_statement():
    return sa_text(
        f"""INSERT INTO {_table(CATALOG_FILE_TABLE)} (
            catalog_set_sha256, file_id, family, collection_kind,
            file_name, source_url, catalog_modified_at, catalog_entry_sha256,
            size_bytes, availability, catalog_support, created_at
        ) VALUES (
            :catalog_set_sha256, :file_id, :family, :collection_kind,
            :file_name, :source_url, :catalog_modified_at,
            :catalog_entry_sha256, :size_bytes, :availability,
            :catalog_support, now()
        ) ON CONFLICT (catalog_set_sha256, file_id) DO NOTHING;"""
    )


def _catalog_file_select_statement():
    return sa_text(
        f"""SELECT catalog_set_sha256, file_id, family, collection_kind,
                   file_name, source_url, catalog_modified_at,
                   catalog_entry_sha256, size_bytes, availability,
                   catalog_support
              FROM {_table(CATALOG_FILE_TABLE)}
             WHERE catalog_set_sha256=:catalog_set_sha256
             ORDER BY family, collection_kind, file_name, file_id;"""
    )


def _raw_observation_insert_statement():
    return sa_text(
        f"""INSERT INTO {_table(RAW_OBSERVATION_TABLE)} (
            raw_set_sha256, catalog_set_sha256, raw_documents_json,
            first_observed_at, last_observed_at
        ) VALUES (
            :raw_set_sha256, :catalog_set_sha256,
            CAST(:raw_documents_json AS jsonb), now(), now()
        ) ON CONFLICT (raw_set_sha256) DO UPDATE SET last_observed_at=now();"""
    )


def _raw_observation_select_statement():
    return sa_text(
        f"""SELECT raw_set_sha256, catalog_set_sha256, raw_documents_json
              FROM {_table(RAW_OBSERVATION_TABLE)}
             WHERE raw_set_sha256=:raw_set_sha256;"""
    )


async def _persist_semantic_set(
    database_session: Any,
    catalog: UHCObservedFileCatalog,
    catalog_counts_by_name: dict[str, int],
) -> None:
    set_parameters_by_field = {
        "catalog_set_sha256": catalog.catalog_set_sha256,
        "schema_version": CATALOG_SCHEMA_VERSION,
        "families_json": json.dumps(sorted(CATALOG_URLS)),
        "collection_summary_json": json.dumps(_source_collection_summary(catalog)),
        **catalog_counts_by_name,
    }
    await database_session.execute(
        _semantic_set_insert_statement(),
        set_parameters_by_field,
    )
    set_query_result = await database_session.execute(
        _semantic_set_select_statement(),
        {"catalog_set_sha256": catalog.catalog_set_sha256},
    )
    _assert_persisted_set(
        _first_query_fields(set_query_result),
        catalog,
        catalog_counts_by_name,
    )


async def _persist_catalog_files(
    database_session: Any,
    catalog: UHCObservedFileCatalog,
    expected_file_parameters: list[dict[str, Any]],
) -> None:
    await database_session.execute(
        _catalog_file_insert_statement(),
        expected_file_parameters,
    )
    file_query_result = await database_session.execute(
        _catalog_file_select_statement(),
        {"catalog_set_sha256": catalog.catalog_set_sha256},
    )
    _assert_persisted_files(
        _all_query_fields(file_query_result),
        expected_file_parameters,
    )


async def _persist_raw_observation(
    database_session: Any,
    catalog: UHCObservedFileCatalog,
    normalized_proof: dict[str, Any],
) -> None:
    raw_parameters_by_field = {
        "raw_set_sha256": normalized_proof["raw_set_sha256"],
        "catalog_set_sha256": catalog.catalog_set_sha256,
        "raw_documents_json": json.dumps(normalized_proof["documents"]),
    }
    await database_session.execute(
        _raw_observation_insert_statement(),
        raw_parameters_by_field,
    )
    raw_query_result = await database_session.execute(
        _raw_observation_select_statement(),
        {"raw_set_sha256": normalized_proof["raw_set_sha256"]},
    )
    persisted_raw_fields = _first_query_fields(raw_query_result)
    if (
        persisted_raw_fields.get("raw_set_sha256")
        != normalized_proof["raw_set_sha256"]
        or persisted_raw_fields.get("catalog_set_sha256")
        != catalog.catalog_set_sha256
    ):
        raise UHCFileCatalogError("UHC persisted raw catalog identity is inconsistent")
    _assert_equal_json(
        persisted_raw_fields.get("raw_documents_json"),
        normalized_proof["documents"],
        "UHC persisted raw catalog proof is inconsistent",
    )


async def record_catalog_observation(
    catalog: UHCObservedFileCatalog,
    raw_proof: dict[str, Any],
) -> None:
    """Persist fully recomputed semantic and exact raw catalog identities."""

    validate_observed_catalog(catalog)
    normalized_proof, retained_catalog = await asyncio.to_thread(
        validate_retained_catalog_proof,
        raw_proof,
    )
    if retained_catalog != catalog:
        raise UHCFileCatalogError(
            "UHC retained raw catalog does not match its semantic catalog"
        )
    catalog_counts_by_name = _catalog_counts(catalog)
    expected_file_parameters = _file_parameters(catalog)
    async with db.transaction() as database_session:
        await _persist_semantic_set(
            database_session,
            catalog,
            catalog_counts_by_name,
        )
        await _persist_catalog_files(
            database_session,
            catalog,
            expected_file_parameters,
        )
        await _persist_raw_observation(database_session, catalog, normalized_proof)
