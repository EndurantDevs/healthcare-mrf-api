# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Transactional storage for the official UHC Practitioner NPI cohort."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
import re
from typing import Any, Mapping

from db.models import db
from process.provider_directory_profile import valid_npi_sql
from process.uhc_canonical_proof import (
    UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY,
    UhcCanonicalProofError,
    validate_uhc_canonical_content_proof,
)
from process.uhc_flex_official_cohort_contract import (
    UHCFlexOfficialCohortError,
    UHCFlexOfficialNPICohort,
    UHC_FLEX_OFFICIAL_NPI_SYSTEM,
    UHC_FLEX_OFFICIAL_RESOURCE_TYPE,
    build_uhc_flex_official_cohort,
)
from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID


_COHORT_TABLE = "provider_directory_uhc_flex_npi_cohort"
_MEMBER_TABLE = "provider_directory_uhc_flex_npi_member"
_DATASET_TABLE = "provider_directory_endpoint_dataset"
_RESOURCE_TABLE = "provider_directory_dataset_resource"
_SOURCE_TABLE = "provider_directory_source"
_SCHEMA_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_NPI_TEXT_SQL = "resource_record.payload_json::jsonb ->> 'npi'"
_NPI_NUMBER_SQL = f"CAST(({_NPI_TEXT_SQL}) AS bigint)"
_NPI_SHAPE_SQL = (
    "pg_catalog.jsonb_typeof("
    "resource_record.payload_json::jsonb -> 'npi') = 'number' "
    f"AND ({_NPI_TEXT_SQL}) ~ '^[0-9]{{10}}$' "
    "AND pg_catalog.jsonb_typeof("
    "resource_record.payload_json::jsonb -> 'identifiers') = 'array' "
    "AND 1 = ("
    "SELECT count(*) FROM pg_catalog.jsonb_array_elements("
    "CASE WHEN pg_catalog.jsonb_typeof("
    "resource_record.payload_json::jsonb -> 'identifiers') = 'array' "
    "THEN resource_record.payload_json::jsonb -> 'identifiers' "
    "ELSE '[]'::jsonb END) AS identifier "
    f"WHERE identifier ->> 'system' = '{UHC_FLEX_OFFICIAL_NPI_SYSTEM}') "
    "AND 1 = ("
    "SELECT count(*) FROM pg_catalog.jsonb_array_elements("
    "CASE WHEN pg_catalog.jsonb_typeof("
    "resource_record.payload_json::jsonb -> 'identifiers') = 'array' "
    "THEN resource_record.payload_json::jsonb -> 'identifiers' "
    "ELSE '[]'::jsonb END) AS identifier "
    f"WHERE identifier ->> 'system' = '{UHC_FLEX_OFFICIAL_NPI_SYSTEM}' "
    "AND pg_catalog.jsonb_typeof(identifier -> 'value') = 'string' "
    f"AND identifier ->> 'value' = {_NPI_TEXT_SQL})"
)


@dataclass(frozen=True, slots=True, repr=False)
class UHCFlexOfficialCohortSyncResult:
    """Return the exact sealed cohort and whether this call created it."""

    cohort: UHCFlexOfficialNPICohort = field(repr=False)
    created: bool

    def __post_init__(self) -> None:
        if type(self.cohort) is not UHCFlexOfficialNPICohort:
            raise ValueError("UHC Flex official cohort sync result is invalid")
        if type(self.created) is not bool:
            raise ValueError("UHC Flex official cohort sync result is invalid")


@dataclass(frozen=True, slots=True, repr=False)
class _OfficialPractitionerSnapshot:
    endpoint_id: str
    dataset_id: str
    acquisition_root_run_id: str
    dataset_hash: str
    content_proof_sha256: str
    practitioner_resource_count: int


def _schema_name() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise UHCFlexOfficialCohortError("state")
    schema_name = runtime_schema or legacy_schema or "mrf"
    if _SCHEMA_PATTERN.fullmatch(schema_name) is None:
        raise UHCFlexOfficialCohortError("state")
    return schema_name


def _table(table_name: str) -> str:
    return f'"{_schema_name()}"."{table_name}"'


def _row_fields(database_row: Any) -> dict[str, Any]:
    if database_row is None:
        return {}
    row_mapping = (
        database_row._mapping
        if hasattr(database_row, "_mapping")
        else database_row
    )
    return dict(row_mapping)


def _json_object(raw_document: object) -> dict[str, Any]:
    if type(raw_document) is str:
        try:
            raw_document = json.loads(raw_document)
        except ValueError as error:
            raise UHCFlexOfficialCohortError("evidence") from error
    if not isinstance(raw_document, Mapping):
        raise UHCFlexOfficialCohortError("evidence")
    return dict(raw_document)


def _strict_text(value: object, maximum_length: int) -> str:
    if (
        type(value) is not str
        or not value
        or len(value) > maximum_length
        or value != value.strip()
        or any(not character.isprintable() for character in value)
    ):
        raise UHCFlexOfficialCohortError("evidence")
    return value


def _positive_count(value: object) -> int:
    if type(value) is not int or value < 1 or value > (1 << 63) - 1:
        raise UHCFlexOfficialCohortError("evidence")
    return value


def _validated_snapshot(source_row: Any) -> _OfficialPractitionerSnapshot:
    source_by_field = _row_fields(source_row)
    endpoint_id = _strict_text(source_by_field.get("endpoint_id"), 64)
    dataset_id = _strict_text(source_by_field.get("dataset_id"), 96)
    root_run_id = _strict_text(
        source_by_field.get("acquisition_root_run_id"),
        64,
    )
    dataset_hash = _strict_text(source_by_field.get("dataset_hash"), 64)
    if (
        source_by_field.get("source_id") != UHC_PROVIDER_FILE_SOURCE_ID
        or source_by_field.get("dataset_endpoint_id") != endpoint_id
        or source_by_field.get("status") != "published"
        or source_by_field.get("is_current") is not True
        or re.fullmatch(r"[0-9a-f]{64}", dataset_hash) is None
    ):
        raise UHCFlexOfficialCohortError("evidence")
    metadata_by_field = _json_object(
        source_by_field.get("publication_metadata_json")
    )
    try:
        proof_by_field = validate_uhc_canonical_content_proof(
            metadata_by_field.get(UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY),
            dataset_id=dataset_id,
            endpoint_id=endpoint_id,
            acquisition_root_run_id=root_run_id,
        )
    except (TypeError, ValueError, UhcCanonicalProofError) as error:
        raise UHCFlexOfficialCohortError("evidence") from error
    resource_counts = proof_by_field.get("resource_counts")
    practitioner_count = (
        resource_counts.get(UHC_FLEX_OFFICIAL_RESOURCE_TYPE)
        if isinstance(resource_counts, Mapping)
        else None
    )
    if (
        proof_by_field.get("source_id") != UHC_PROVIDER_FILE_SOURCE_ID
        or proof_by_field.get("dataset_hash") != dataset_hash
        or proof_by_field.get("resource_count")
        != source_by_field.get("resource_count")
    ):
        raise UHCFlexOfficialCohortError("evidence")
    return _OfficialPractitionerSnapshot(
        endpoint_id=endpoint_id,
        dataset_id=dataset_id,
        acquisition_root_run_id=root_run_id,
        dataset_hash=dataset_hash,
        content_proof_sha256=_strict_text(
            proof_by_field.get("proof_sha256"),
            64,
        ),
        practitioner_resource_count=_positive_count(practitioner_count),
    )


async def _current_official_snapshot(
    database: Any,
) -> _OfficialPractitionerSnapshot:
    source_rows = await database.all(
        f"""
        SELECT source_record.source_id,
               source_record.endpoint_id,
               dataset_record.endpoint_id AS dataset_endpoint_id,
               dataset_record.dataset_id,
               dataset_record.acquisition_root_run_id,
               dataset_record.dataset_hash,
               dataset_record.status,
               dataset_record.is_current,
               dataset_record.resource_count,
               dataset_record.publication_metadata_json
          FROM {_table(_SOURCE_TABLE)} AS source_record
          JOIN {_table(_DATASET_TABLE)} AS dataset_record
            ON dataset_record.endpoint_id = source_record.endpoint_id
         WHERE source_record.source_id = :source_id
           AND dataset_record.status = 'published'
           AND dataset_record.is_current = true
         ORDER BY dataset_record.dataset_id
         LIMIT 2
         FOR SHARE OF source_record, dataset_record;
        """,
        source_id=UHC_PROVIDER_FILE_SOURCE_ID,
    )
    if not source_rows:
        raise UHCFlexOfficialCohortError("missing")
    if len(source_rows) != 1:
        raise UHCFlexOfficialCohortError("state")
    return _validated_snapshot(source_rows[0])


async def _official_practitioner_npi_count(
    database: Any,
    snapshot: _OfficialPractitionerSnapshot,
) -> int:
    count_row = await database.first(
        f"""
        WITH practitioner_npi AS (
            SELECT CASE WHEN {_NPI_SHAPE_SQL}
                        THEN {_NPI_NUMBER_SQL}
                        ELSE NULL END AS npi
              FROM {_table(_RESOURCE_TABLE)} AS resource_record
             WHERE resource_record.dataset_id = :dataset_id
               AND resource_record.resource_type = :resource_type
        )
        SELECT count(*)::bigint AS practitioner_resource_count,
               count(*) FILTER (
                   WHERE npi IS NULL OR NOT {valid_npi_sql("npi")}
               )::bigint AS invalid_npi_count,
               count(DISTINCT npi) FILTER (
                   WHERE npi IS NOT NULL AND {valid_npi_sql("npi")}
               )::bigint AS npi_count
          FROM practitioner_npi;
        """,
        dataset_id=snapshot.dataset_id,
        resource_type=UHC_FLEX_OFFICIAL_RESOURCE_TYPE,
    )
    count_by_field = _row_fields(count_row)
    if (
        count_by_field.get("practitioner_resource_count")
        != snapshot.practitioner_resource_count
        or count_by_field.get("invalid_npi_count") != 0
    ):
        raise UHCFlexOfficialCohortError("evidence")
    return _positive_count(count_by_field.get("npi_count"))


def _cohort_from_row(cohort_row: Any) -> UHCFlexOfficialNPICohort:
    cohort_by_field = _row_fields(cohort_row)
    try:
        return UHCFlexOfficialNPICohort(
            cohort_id=cohort_by_field.get("cohort_id"),
            official_endpoint_id=cohort_by_field.get("official_endpoint_id"),
            official_dataset_id=cohort_by_field.get("official_dataset_id"),
            official_acquisition_root_run_id=cohort_by_field.get(
                "official_acquisition_root_run_id"
            ),
            official_dataset_hash=cohort_by_field.get("official_dataset_hash"),
            official_content_proof_sha256=cohort_by_field.get(
                "official_content_proof_sha256"
            ),
            practitioner_resource_count=cohort_by_field.get(
                "practitioner_resource_count"
            ),
            npi_count=cohort_by_field.get("npi_count"),
            contract_id=cohort_by_field.get("contract_id"),
            authority_id=cohort_by_field.get("authority_id"),
            official_source_id=cohort_by_field.get("official_source_id"),
            resource_type=cohort_by_field.get("resource_type"),
            cohort_complete=cohort_by_field.get("cohort_complete"),
            endpoint_collection_complete=cohort_by_field.get(
                "endpoint_collection_complete"
            ),
            endpoint_complete=cohort_by_field.get("endpoint_complete"),
        )
    except (TypeError, ValueError) as error:
        raise UHCFlexOfficialCohortError("state") from error


async def _stored_cohort_for_dataset(
    database: Any,
    dataset_id: str,
) -> UHCFlexOfficialNPICohort | None:
    cohort_rows = await database.all(
        f"""
        SELECT *
          FROM {_table(_COHORT_TABLE)}
         WHERE official_dataset_id = :dataset_id
         ORDER BY cohort_id
         LIMIT 2;
        """,
        dataset_id=dataset_id,
    )
    if not cohort_rows:
        return None
    if len(cohort_rows) != 1:
        raise UHCFlexOfficialCohortError("state")
    return _cohort_from_row(cohort_rows[0])


def _is_cohort_snapshot_match(
    cohort: UHCFlexOfficialNPICohort,
    snapshot: _OfficialPractitionerSnapshot,
) -> bool:
    return (
        cohort.official_endpoint_id == snapshot.endpoint_id
        and cohort.official_dataset_id == snapshot.dataset_id
        and cohort.official_acquisition_root_run_id
        == snapshot.acquisition_root_run_id
        and cohort.official_dataset_hash == snapshot.dataset_hash
        and cohort.official_content_proof_sha256
        == snapshot.content_proof_sha256
        and cohort.practitioner_resource_count
        == snapshot.practitioner_resource_count
    )


async def _insert_members(
    database: Any,
    cohort: UHCFlexOfficialNPICohort,
) -> None:
    inserted_count = await database.status(
        f"""
        INSERT INTO {_table(_MEMBER_TABLE)} (cohort_id, npi)
        SELECT :cohort_id, candidate_npi.npi
          FROM (
                SELECT DISTINCT {_NPI_NUMBER_SQL} AS npi
                  FROM {_table(_RESOURCE_TABLE)} AS resource_record
                 WHERE resource_record.dataset_id = :dataset_id
                   AND resource_record.resource_type = :resource_type
                   AND {_NPI_SHAPE_SQL}
               ) AS candidate_npi
         WHERE {valid_npi_sql("candidate_npi.npi")}
         ORDER BY candidate_npi.npi
        ON CONFLICT DO NOTHING;
        """,
        cohort_id=cohort.cohort_id,
        dataset_id=cohort.official_dataset_id,
        resource_type=UHC_FLEX_OFFICIAL_RESOURCE_TYPE,
    )
    if inserted_count != cohort.npi_count:
        raise UHCFlexOfficialCohortError("state")


async def _insert_header(
    database: Any,
    cohort: UHCFlexOfficialNPICohort,
) -> None:
    inserted_count = await database.status(
        f"""
        INSERT INTO {_table(_COHORT_TABLE)} (
            cohort_id, contract_id, authority_id, official_source_id,
            official_endpoint_id, official_dataset_id,
            official_acquisition_root_run_id, official_dataset_hash,
            official_content_proof_sha256, resource_type,
            practitioner_resource_count, npi_count,
            cohort_complete, endpoint_collection_complete, endpoint_complete
        ) VALUES (
            :cohort_id, :contract_id, :authority_id, :official_source_id,
            :official_endpoint_id, :official_dataset_id,
            :official_acquisition_root_run_id, :official_dataset_hash,
            :official_content_proof_sha256, :resource_type,
            :practitioner_resource_count, :npi_count,
            :cohort_complete, :endpoint_collection_complete, :endpoint_complete
        );
        """,
        **{
            field_name: getattr(cohort, field_name)
            for field_name in (
                "cohort_id",
                "contract_id",
                "authority_id",
                "official_source_id",
                "official_endpoint_id",
                "official_dataset_id",
                "official_acquisition_root_run_id",
                "official_dataset_hash",
                "official_content_proof_sha256",
                "resource_type",
                "practitioner_resource_count",
                "npi_count",
                "cohort_complete",
                "endpoint_collection_complete",
                "endpoint_complete",
            )
        },
    )
    if inserted_count != 1:
        raise UHCFlexOfficialCohortError("state")


async def sync_uhc_flex_official_cohort(
    *,
    database: Any = db,
) -> UHCFlexOfficialCohortSyncResult:
    """Seal or exactly replay the current official UHC Practitioner cohort."""

    async with database.transaction():
        is_locked = await database.scalar(
            """
            SELECT pg_catalog.pg_try_advisory_xact_lock(
                       pg_catalog.hashtextextended(:lock_identity, 0)
                   );
            """,
            lock_identity=(
                "provider-directory-uhc-flex-official-cohort:"
                f"{UHC_PROVIDER_FILE_SOURCE_ID}"
            ),
        )
        if is_locked is not True:
            raise UHCFlexOfficialCohortError("busy")
        snapshot = await _current_official_snapshot(database)
        existing_cohort = await _stored_cohort_for_dataset(
            database,
            snapshot.dataset_id,
        )
        if existing_cohort is not None:
            if not _is_cohort_snapshot_match(existing_cohort, snapshot):
                raise UHCFlexOfficialCohortError("state")
            return UHCFlexOfficialCohortSyncResult(existing_cohort, False)
        npi_count = await _official_practitioner_npi_count(database, snapshot)
        try:
            cohort = build_uhc_flex_official_cohort(
                official_endpoint_id=snapshot.endpoint_id,
                official_dataset_id=snapshot.dataset_id,
                official_acquisition_root_run_id=snapshot.acquisition_root_run_id,
                official_dataset_hash=snapshot.dataset_hash,
                official_content_proof_sha256=snapshot.content_proof_sha256,
                practitioner_resource_count=snapshot.practitioner_resource_count,
                npi_count=npi_count,
            )
        except ValueError as error:
            raise UHCFlexOfficialCohortError("evidence") from error
        await _insert_members(database, cohort)
        await _insert_header(database, cohort)
        stored_cohort = await _stored_cohort_for_dataset(database, snapshot.dataset_id)
        if stored_cohort != cohort:
            raise UHCFlexOfficialCohortError("state")
        return UHCFlexOfficialCohortSyncResult(cohort, True)


__all__ = (
    "sync_uhc_flex_official_cohort",
    "UHCFlexOfficialCohortSyncResult",
)
