# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Guarded compact-receipt repair for one legacy Provider Directory row."""

from __future__ import annotations

from collections.abc import Mapping
import json
import os
from typing import Any

from process.provider_directory_admission_seal import _IDENTIFIER_RE
from process.provider_directory_fhir import (
    PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY,
    SOURCE_SUMMARY_METADATA_KEY,
    _artifact_bounded_publication_metadata_sql,
    _artifact_normalized_receipt_resources,
    _artifact_selection_receipt,
)


_SEAL_FIELDS = (
    "publication_metadata_summary_json",
    "publication_metadata_sha256",
    "content_proof_admission_version",
    "content_proof_admission_kind",
    "content_proof_admission_sha256",
    "content_proof_resource_types",
)
_SELECTED_DATASET_COLUMNS = f"""
    raw_dataset.dataset_id,
    raw_dataset.endpoint_id,
    COALESCE(
        raw_dataset.acquisition_root_run_id,
        raw_dataset.import_run_id
    ) AS evidence_run_id,
    raw_dataset.previous_dataset_id,
    raw_dataset.dataset_hash,
    raw_dataset.resource_count,
    raw_dataset.status,
    raw_dataset.is_current,
    raw_dataset.superseded_at,
    raw_dataset.artifact_selection_receipt_json,
    raw_dataset.publication_metadata_summary_json,
    raw_dataset.publication_metadata_sha256,
    raw_dataset.content_proof_admission_version,
    raw_dataset.content_proof_admission_kind,
    raw_dataset.content_proof_admission_sha256,
    raw_dataset.content_proof_resource_types,
    raw_dataset.publication_metadata_json::jsonb
        - '{PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY}'
        AS proofless_publication_metadata_json,
    proof.complete AS proof_complete,
    proof.contract_id AS proof_contract_id,
    proof.proof_sha256,
    proof.dataset_hash AS proof_dataset_hash,
    proof.resource_count AS proof_resource_count,
    proof.resource_hashes AS proof_resource_hashes,
    proof.resource_counts AS proof_resource_counts,
    raw_dataset.ctid::text AS row_ctid,
    raw_dataset.xmin::text AS row_xmin
"""
_PROOF_RECORD_SQL = f"""
    CROSS JOIN LATERAL jsonb_to_record(
        COALESCE(
            raw_dataset.publication_metadata_json::jsonb
                -> '{PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY}',
            '{{}}'::jsonb
        )
    ) AS proof(
        complete jsonb,
        contract_id jsonb,
        proof_sha256 jsonb,
        dataset_hash jsonb,
        resource_count jsonb,
        resource_hashes jsonb,
        resource_counts jsonb
    )
"""


class ProviderDirectorySelectionReceiptBackfillError(RuntimeError):
    """Reject an unsafe or stale compact-receipt repair."""


def _schema() -> str:
    runtime = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy = os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise ProviderDirectorySelectionReceiptBackfillError(
            "provider_directory_selection_receipt_schema_invalid"
        )
    schema = runtime or legacy or "mrf"
    if _IDENTIFIER_RE.fullmatch(schema) is None:
        raise ProviderDirectorySelectionReceiptBackfillError(
            "provider_directory_selection_receipt_schema_invalid"
        )
    return schema


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _bounded_metadata_sql() -> str:
    metadata = "dataset.proofless_publication_metadata_json"
    bounded = _artifact_bounded_publication_metadata_sql(metadata)
    return f"""
        ({bounded}) || jsonb_build_object(
            '{PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY}',
            jsonb_build_object(
                'complete', dataset.proof_complete,
                'contract_id', dataset.proof_contract_id,
                'proof_sha256', dataset.proof_sha256
            )
        )
    """


def _proof_summary_sql() -> str:
    return f"""
        jsonb_build_object(
            'dataset_hash', dataset.proof_dataset_hash,
            'resource_count', dataset.proof_resource_count,
            'resource_hashes', dataset.proof_resource_hashes,
            'resource_counts', dataset.proof_resource_counts
        )
    """


def _dataset_row_sql(dataset_ref: str, *, lock: bool) -> str:
    """Project one bounded dataset row while detoasting raw proof twice."""

    lock_sql = "FOR UPDATE OF raw_dataset" if lock else ""
    return f"""
        WITH selected_dataset AS MATERIALIZED (
            SELECT {_SELECTED_DATASET_COLUMNS}
              FROM {dataset_ref} AS raw_dataset
              {_PROOF_RECORD_SQL}
             WHERE raw_dataset.dataset_id = $1
             {lock_sql}
        )
        SELECT dataset.dataset_id,
               dataset.endpoint_id,
               dataset.evidence_run_id,
               dataset.previous_dataset_id,
               dataset.dataset_hash,
               dataset.resource_count,
               dataset.status,
               dataset.is_current,
               dataset.superseded_at,
               dataset.artifact_selection_receipt_json,
               dataset.publication_metadata_summary_json,
               dataset.publication_metadata_sha256,
               dataset.content_proof_admission_version,
               dataset.content_proof_admission_kind,
               dataset.content_proof_admission_sha256,
               dataset.content_proof_resource_types,
               {_bounded_metadata_sql()} AS bounded_publication_metadata_json,
               {_proof_summary_sql()} AS content_proof_summary_json,
               dataset.row_ctid,
               dataset.row_xmin
          FROM selected_dataset AS dataset
    """


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as error:
            raise ProviderDirectorySelectionReceiptBackfillError(
                "provider_directory_selection_receipt_metadata_invalid"
            ) from error
    if not isinstance(value, Mapping):
        raise ProviderDirectorySelectionReceiptBackfillError(
            "provider_directory_selection_receipt_metadata_invalid"
        )
    return dict(value)


def _validated_current_row(row: Mapping[str, Any]) -> None:
    if (
        row.get("status") != "published"
        or row.get("is_current") is not True
        or row.get("superseded_at") is not None
        or not row.get("endpoint_id")
        or not row.get("evidence_run_id")
        or not row.get("dataset_hash")
        or isinstance(row.get("resource_count"), bool)
        or not isinstance(row.get("resource_count"), int)
        or row["resource_count"] < 0
    ):
        raise ProviderDirectorySelectionReceiptBackfillError(
            "provider_directory_selection_receipt_dataset_state_invalid"
        )


def _validated_receipt(
    dataset_by_field: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    metadata = _json_object(dataset_by_field.get("bounded_publication_metadata_json"))
    source_ids = metadata.get("source_ids")
    selected_resources = metadata.get("selected_resources")
    proof_header = metadata.get(PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY)
    if (
        not isinstance(source_ids, list)
        or not source_ids
        or not isinstance(selected_resources, list)
        or not selected_resources
        or not isinstance(proof_header, Mapping)
    ):
        raise ProviderDirectorySelectionReceiptBackfillError(
            "provider_directory_selection_receipt_metadata_invalid"
        )
    verification_by_field = dict(dataset_by_field)
    verification_by_field["content_proof_contract_id"] = proof_header.get("contract_id")
    verification_by_field["content_proof_sha256"] = proof_header.get("proof_sha256")
    try:
        _artifact_normalized_receipt_resources(
            verification_by_field,
            source_id=source_ids[0],
            endpoint_id=dataset_by_field["endpoint_id"],
            dataset_id=dataset_by_field["dataset_id"],
            evidence_run_id=dataset_by_field["evidence_run_id"],
            selected_resources=tuple(selected_resources),
            publication_metadata=metadata,
        )
        receipt = _artifact_selection_receipt(metadata)
    except (RuntimeError, TypeError, ValueError) as error:
        raise ProviderDirectorySelectionReceiptBackfillError(
            "provider_directory_selection_receipt_metadata_invalid"
        ) from error
    if receipt is None:
        raise ProviderDirectorySelectionReceiptBackfillError(
            "provider_directory_selection_receipt_metadata_invalid"
        )
    summary = _json_object(metadata.get(SOURCE_SUMMARY_METADATA_KEY))
    proof_summary = _json_object(dataset_by_field.get("content_proof_summary_json"))
    expected_proof_by_field = {
        "dataset_hash": dataset_by_field["dataset_hash"],
        "resource_count": dataset_by_field["resource_count"],
        "resource_hashes": summary.get("resource_hashes"),
        "resource_counts": summary.get("resource_counts"),
    }
    if proof_summary != expected_proof_by_field:
        raise ProviderDirectorySelectionReceiptBackfillError(
            "provider_directory_selection_receipt_proof_summary_invalid"
        )
    return receipt, summary


async def _retained_resource_counts(
    connection: Any,
    resource_ref: str,
    dataset_id: str,
    resource_types: list[str],
) -> dict[str, int]:
    rows = await connection.fetch(
        f"""
        SELECT resource_type, count(*)::bigint AS resource_count
         FROM {resource_ref}
         WHERE dataset_id = $1
           AND resource_type NOT LIKE 'LU:%:pass:%'
         GROUP BY resource_type
         ORDER BY resource_type
        """,
        dataset_id,
    )
    count_by_resource_type = {
        str(resource["resource_type"]): int(resource["resource_count"])
        for resource in rows
    }
    for resource_type in resource_types:
        count_by_resource_type.setdefault(resource_type, 0)
    return count_by_resource_type


def _receipt_result(
    dataset_by_field: Mapping[str, Any],
    receipt: Mapping[str, Any],
    summary: Mapping[str, Any],
    status: str,
) -> dict[str, Any]:
    source_ids = receipt.get("source_ids")
    resource_counts = summary.get("resource_counts")
    return {
        "dataset_id": dataset_by_field["dataset_id"],
        "status": status,
        "receipt_bytes": len(
            json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
        ),
        "source_count": len(source_ids) if isinstance(source_ids, list) else 0,
        "resource_types": (
            sorted(resource_counts) if isinstance(resource_counts, dict) else []
        ),
        "resource_count": dataset_by_field["resource_count"],
    }


async def _store_receipt(
    connection: Any,
    dataset_ref: str,
    dataset_by_field: Mapping[str, Any],
    receipt: Mapping[str, Any],
) -> None:
    update_status = await connection.execute(
        f"""
        UPDATE {dataset_ref}
           SET artifact_selection_receipt_json = $1::jsonb
         WHERE dataset_id = $2
           AND ctid::text = $3
           AND xmin::text = $4
           AND endpoint_id = $5
           AND COALESCE(acquisition_root_run_id, import_run_id)
               IS NOT DISTINCT FROM $6
           AND previous_dataset_id IS NOT DISTINCT FROM $7
           AND dataset_hash IS NOT DISTINCT FROM $8
           AND resource_count = $9
           AND status = $10
           AND is_current IS TRUE
           AND superseded_at IS NULL
           AND artifact_selection_receipt_json IS NULL
           AND publication_metadata_summary_json IS NULL
           AND publication_metadata_sha256 IS NULL
           AND content_proof_admission_version IS NULL
           AND content_proof_admission_kind IS NULL
           AND content_proof_admission_sha256 IS NULL
           AND content_proof_resource_types IS NULL
        """,
        json.dumps(receipt, sort_keys=True, separators=(",", ":")),
        dataset_by_field["dataset_id"],
        dataset_by_field["row_ctid"],
        dataset_by_field["row_xmin"],
        dataset_by_field["endpoint_id"],
        dataset_by_field["evidence_run_id"],
        dataset_by_field["previous_dataset_id"],
        dataset_by_field["dataset_hash"],
        dataset_by_field["resource_count"],
        dataset_by_field["status"],
    )
    if update_status != "UPDATE 1":
        raise ProviderDirectorySelectionReceiptBackfillError(
            "provider_directory_selection_receipt_backfill_lost"
        )


async def _load_dataset_by_field(
    connection: Any,
    dataset_ref: str,
    dataset_id: str,
    *,
    lock: bool,
) -> dict[str, Any]:
    raw_dataset = await connection.fetchrow(
        _dataset_row_sql(dataset_ref, lock=lock),
        dataset_id,
    )
    if raw_dataset is None:
        raise ProviderDirectorySelectionReceiptBackfillError(
            "provider_directory_selection_receipt_dataset_missing"
        )
    dataset_by_field = dict(raw_dataset)
    _validated_current_row(dataset_by_field)
    return dataset_by_field


async def _require_retained_resource_counts(
    connection: Any,
    resource_ref: str,
    dataset_id: str,
    summary: Mapping[str, Any],
) -> None:
    resource_counts = summary.get("resource_counts")
    if not isinstance(resource_counts, dict):
        raise ProviderDirectorySelectionReceiptBackfillError(
            "provider_directory_selection_receipt_metadata_invalid"
        )
    expected_count_by_resource_type = {
        str(resource_type): int(resource_count)
        for resource_type, resource_count in resource_counts.items()
    }
    retained_count_by_resource_type = await _retained_resource_counts(
        connection,
        resource_ref,
        dataset_id,
        sorted(expected_count_by_resource_type),
    )
    if retained_count_by_resource_type != expected_count_by_resource_type:
        raise ProviderDirectorySelectionReceiptBackfillError(
            "provider_directory_selection_receipt_retained_counts_changed"
        )


async def _finish_receipt_backfill(
    connection: Any,
    dataset_ref: str,
    dataset_by_field: Mapping[str, Any],
    receipt: Mapping[str, Any],
    summary: Mapping[str, Any],
    *,
    apply: bool,
) -> dict[str, Any]:
    stored_receipt = dataset_by_field.get("artifact_selection_receipt_json")
    if any(dataset_by_field.get(field_name) is not None for field_name in _SEAL_FIELDS):
        raise ProviderDirectorySelectionReceiptBackfillError(
            "provider_directory_selection_receipt_admission_state_invalid"
        )
    if stored_receipt is not None:
        if _json_object(stored_receipt) != receipt:
            raise ProviderDirectorySelectionReceiptBackfillError(
                "provider_directory_selection_receipt_stored_receipt_invalid"
            )
        status = "already_stored"
    elif not apply:
        status = "validated"
    else:
        await _store_receipt(
            connection,
            dataset_ref,
            dataset_by_field,
            receipt,
        )
        status = "stored"
    return _receipt_result(dataset_by_field, receipt, summary, status)


async def backfill_provider_directory_selection_receipt(
    dataset_id: str,
    *,
    apply: bool = False,
    database: Any | None = None,
) -> dict[str, Any]:
    """Validate, then optionally store, one compact current-row receipt."""

    if not dataset_id or dataset_id != dataset_id.strip():
        raise ProviderDirectorySelectionReceiptBackfillError(
            "provider_directory_selection_receipt_dataset_id_invalid"
        )
    if database is None:
        from db.models import db as database

    schema = _schema()
    dataset_ref = _qt(schema, "provider_directory_endpoint_dataset")
    resource_ref = _qt(schema, "provider_directory_dataset_resource")
    async with database.acquire_driver() as connection:
        async with connection.transaction(
            isolation="repeatable_read",
            readonly=not apply,
        ):
            await connection.execute("SET LOCAL statement_timeout = '30s';")
            if apply:
                await connection.execute("SET LOCAL lock_timeout = '5s';")
            dataset_by_field = await _load_dataset_by_field(
                connection,
                dataset_ref,
                dataset_id,
                lock=apply,
            )
            receipt, summary = _validated_receipt(dataset_by_field)
            await _require_retained_resource_counts(
                connection,
                resource_ref,
                dataset_id,
                summary,
            )
            return await _finish_receipt_backfill(
                connection,
                dataset_ref,
                dataset_by_field,
                receipt,
                summary,
                apply=apply,
            )


__all__ = [
    "ProviderDirectorySelectionReceiptBackfillError",
    "backfill_provider_directory_selection_receipt",
]
