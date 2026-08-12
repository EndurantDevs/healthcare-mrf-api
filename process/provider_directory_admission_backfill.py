# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Transactional backfill for Provider Directory admission receipts."""

from __future__ import annotations

from collections.abc import Mapping
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any

from process.provider_directory_admission_seal import (
    ADMISSION_RAW_METADATA_MAX_BYTES,
    AdmissionSealError,
    ProviderDirectoryAdmissionSeal,
    validate_generic_admission_copy,
)


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_FINALIZED_STATUSES = frozenset(
    {
        "validated",
        "published",
        "superseded",
        "verification_baseline",
        "verification_mismatch",
    }
)
_SEAL_FIELDS = (
    "publication_metadata_summary_json",
    "publication_metadata_sha256",
    "content_proof_admission_version",
    "content_proof_admission_kind",
    "content_proof_admission_sha256",
    "content_proof_resource_types",
)


def _schema_name() -> str:
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or os.getenv("DB_SCHEMA") or "mrf"
    if _IDENTIFIER_RE.fullmatch(schema) is None:
        raise AdmissionSealError(
            "provider_directory_admission_schema_invalid"
        )
    return schema


def _qualified_dataset_table() -> str:
    return f'"{_schema_name()}"."provider_directory_endpoint_dataset"'


async def _fetch_dataset_row(
    connection: Any,
    dataset_ref: str,
    dataset_id: str,
) -> Any:
    return await connection.fetchrow(
        f"""
        SELECT dataset_id, endpoint_id,
               COALESCE(acquisition_root_run_id, import_run_id)
                   AS evidence_run_id,
               dataset_hash, resource_count, status,
               completion_proof_required_version,
               CASE
                   WHEN completion_proof_required_version = 3
                   THEN completion_proof_json
                       -> 'dataset' -> 'resource_hashes'
               END AS completion_resource_hashes,
               CASE
                   WHEN completion_proof_required_version = 3
                   THEN completion_proof_json
                       -> 'dataset' -> 'resource_counts'
               END AS completion_resource_counts,
               octet_length(publication_metadata_json::text)
                   AS raw_metadata_bytes,
               publication_metadata_summary_json,
               publication_metadata_sha256,
               content_proof_admission_version,
               content_proof_admission_kind,
               content_proof_admission_sha256,
               content_proof_resource_types,
               ctid::text AS row_ctid, xmin::text AS row_xmin
          FROM {dataset_ref}
         WHERE dataset_id = $1
         FOR UPDATE
        """,
        dataset_id,
    )


def _existing_seal_result(
    dataset_row: Any,
    dataset_id: str,
) -> dict[str, Any] | None:
    seal_values = tuple(dataset_row[field_name] for field_name in _SEAL_FIELDS)
    if all(seal_value is not None for seal_value in seal_values):
        return {
            "dataset_id": dataset_id,
            "status": "already_sealed",
            "admission_kind": dataset_row["content_proof_admission_kind"],
        }
    if any(seal_value is not None for seal_value in seal_values):
        raise AdmissionSealError("provider_directory_admission_partial_seal")
    return None


def _validated_row_inputs(dataset_row: Any) -> tuple[int | None, int]:
    if dataset_row["status"] not in _FINALIZED_STATUSES:
        raise AdmissionSealError("provider_directory_admission_status_invalid")
    completion_version = dataset_row["completion_proof_required_version"]
    if completion_version not in {None, 3} or (
        completion_version == 3
        and (
            not isinstance(dataset_row["completion_resource_hashes"], Mapping)
            or not isinstance(
                dataset_row["completion_resource_counts"], Mapping
            )
        )
    ):
        raise AdmissionSealError(
            "provider_directory_admission_completion_summary_invalid"
        )
    raw_metadata_bytes = dataset_row["raw_metadata_bytes"]
    if (
        type(raw_metadata_bytes) is not int
        or raw_metadata_bytes <= 0
        or raw_metadata_bytes > ADMISSION_RAW_METADATA_MAX_BYTES
    ):
        raise AdmissionSealError(
            "provider_directory_admission_metadata_size_invalid"
        )
    if (
        not dataset_row["evidence_run_id"]
        or not dataset_row["dataset_hash"]
        or isinstance(dataset_row["resource_count"], bool)
        or not isinstance(dataset_row["resource_count"], int)
    ):
        raise AdmissionSealError(
            "provider_directory_admission_parent_identity_invalid"
        )
    return completion_version, raw_metadata_bytes


async def _copy_locked_metadata(
    connection: Any,
    dataset_ref: str,
    dataset_row: Any,
    copy_file: Any,
) -> None:
    async def _spool_copy(copy_chunk: bytes) -> None:
        if (
            copy_file.tell() + len(copy_chunk)
            > ADMISSION_RAW_METADATA_MAX_BYTES + 128
        ):
            raise AdmissionSealError(
                "provider_directory_admission_copy_size_invalid"
            )
        copy_file.write(copy_chunk)

    try:
        copy_status = await connection.copy_from_query(
            f"""
            SELECT publication_metadata_json::text
              FROM {dataset_ref}
             WHERE dataset_id = $1
               AND ctid::text = $2
               AND xmin::text = $3
            """,
            dataset_row["dataset_id"],
            dataset_row["row_ctid"],
            dataset_row["row_xmin"],
            output=_spool_copy,
            format="binary",
        )
        copy_file.flush()
        os.fsync(copy_file.fileno())
    finally:
        copy_file.close()
    if copy_status != "COPY 1":
        raise AdmissionSealError("provider_directory_admission_copy_lost")


async def _validated_dataset_seal(
    connection: Any,
    dataset_ref: str,
    dataset_row: Any,
    completion_version: int | None,
) -> ProviderDirectoryAdmissionSeal:
    with tempfile.TemporaryDirectory(
        prefix="provider-directory-admission-"
    ) as temporary_directory:
        temporary_path = Path(temporary_directory)
        copy_file = tempfile.NamedTemporaryFile(
            prefix="metadata-",
            suffix=".copy",
            dir=temporary_path,
            delete=False,
        )
        copy_path = Path(copy_file.name)
        os.chmod(copy_path, 0o600)
        await _copy_locked_metadata(
            connection,
            dataset_ref,
            dataset_row,
            copy_file,
        )
        return validate_generic_admission_copy(
            copy_path,
            dataset_id=dataset_row["dataset_id"],
            endpoint_id=dataset_row["endpoint_id"],
            evidence_run_id=dataset_row["evidence_run_id"],
            dataset_hash=dataset_row["dataset_hash"],
            resource_count=dataset_row["resource_count"],
            scratch_directory=temporary_path,
            expected_resource_hashes=(
                dataset_row["completion_resource_hashes"]
                if completion_version == 3
                else None
            ),
            expected_resource_counts=(
                dataset_row["completion_resource_counts"]
                if completion_version == 3
                else None
            ),
        )


async def _store_seal(
    connection: Any,
    dataset_ref: str,
    dataset_id: str,
    dataset_row: Any,
    seal: ProviderDirectoryAdmissionSeal,
) -> None:
    update_status = await connection.execute(
        f"""
        UPDATE {dataset_ref}
           SET publication_metadata_summary_json = $1::jsonb,
               publication_metadata_sha256 = $2,
               content_proof_admission_version = $3,
               content_proof_admission_kind = $4,
               content_proof_admission_sha256 = $5,
               content_proof_resource_types = $6::varchar[]
         WHERE dataset_id = $7
           AND ctid::text = $8
           AND xmin::text = $9
           AND publication_metadata_summary_json IS NULL
           AND publication_metadata_sha256 IS NULL
           AND content_proof_admission_version IS NULL
           AND content_proof_admission_kind IS NULL
           AND content_proof_admission_sha256 IS NULL
           AND content_proof_resource_types IS NULL
        """,
        json.dumps(seal.metadata_summary, ensure_ascii=False),
        seal.metadata_sha256,
        seal.admission_version,
        seal.admission_kind,
        seal.proof_sha256,
        list(seal.resource_types),
        dataset_id,
        dataset_row["row_ctid"],
        dataset_row["row_xmin"],
    )
    if update_status != "UPDATE 1":
        raise AdmissionSealError(
            "provider_directory_admission_backfill_lost"
        )


async def _backfill_locked_dataset(
    connection: Any,
    dataset_ref: str,
    dataset_id: str,
) -> dict[str, Any]:
    dataset_row = await _fetch_dataset_row(connection, dataset_ref, dataset_id)
    if dataset_row is None:
        raise AdmissionSealError("provider_directory_admission_dataset_missing")
    existing_result = _existing_seal_result(dataset_row, dataset_id)
    if existing_result is not None:
        return existing_result
    completion_version, raw_metadata_bytes = _validated_row_inputs(dataset_row)
    seal = await _validated_dataset_seal(
        connection,
        dataset_ref,
        dataset_row,
        completion_version,
    )
    await _store_seal(connection, dataset_ref, dataset_id, dataset_row, seal)
    return {
        "dataset_id": dataset_id,
        "status": "sealed",
        "admission_kind": seal.admission_kind,
        "metadata_sha256": seal.metadata_sha256,
        "proof_sha256": seal.proof_sha256,
        "resource_types": list(seal.resource_types),
        "raw_metadata_bytes": raw_metadata_bytes,
    }


async def _backfill_provider_directory_admission_seal(
    dataset_id: str,
    *,
    database: Any | None = None,
) -> dict[str, Any]:
    if not dataset_id or dataset_id != dataset_id.strip():
        raise AdmissionSealError(
            "provider_directory_admission_dataset_id_invalid"
        )
    if database is None:
        from db.models import db as database

    dataset_ref = _qualified_dataset_table()
    async with database.acquire_driver() as connection:
        async with connection.transaction(isolation="repeatable_read"):
            return await _backfill_locked_dataset(
                connection,
                dataset_ref,
                dataset_id,
            )
