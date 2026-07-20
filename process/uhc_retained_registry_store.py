# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Set-based PostgreSQL persistence for native-retained UHC proofs."""

from __future__ import annotations

import json
import os
import re
import stat
from pathlib import Path
from typing import Any, Mapping, Sequence

import asyncpg

from process.uhc_retained_range_manifest import (
    RANGE_CANONICALIZATION_ID,
    RANGE_CONTRACT_ID,
)
from process.uhc_retained_registry_contract import (
    SourceBinding,
    UHCSourceBindingMismatch,
    expected_catalog_file_hash_pair,
    require_digest,
)
from process.uhc_retained_types import (
    RawRangeProof,
    RetainedRawArtifactProof,
    UHCRetainedAdmissionError,
)


_IDENTIFIER_RE = re.compile(r"[a-z_][a-z0-9_]{0,62}")


def schema_name() -> str:
    """Return the configured, safely quoted UHC registry schema name."""

    configured_schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if not _IDENTIFIER_RE.fullmatch(configured_schema):
        raise UHCRetainedAdmissionError("invalid UHC registry schema")
    return configured_schema


def table_name(table: str) -> str:
    """Return one safely schema-qualified UHC registry table."""

    if not _IDENTIFIER_RE.fullmatch(table):
        raise UHCRetainedAdmissionError("invalid UHC registry table")
    return f'"{schema_name()}"."{table}"'


def file_uri(path: str | Path) -> str:
    """Return a file URI only for a verified immutable regular file."""

    absolute = Path(os.path.abspath(path))
    try:
        path_stat = os.stat(absolute, follow_symlinks=False)
    except OSError as error:
        raise UHCRetainedAdmissionError(
            f"retained artifact is missing: {absolute}"
        ) from error
    if (
        not stat.S_ISREG(path_stat.st_mode)
        or path_stat.st_nlink != 1
        or path_stat.st_mode & 0o022
    ):
        raise UHCRetainedAdmissionError(
            f"retained artifact permissions or link count are unsafe: {absolute}"
        )
    return absolute.as_uri()


def _assert_row_matches(
    database_record: Mapping[str, Any] | None,
    expected_fields: Mapping[str, Any],
    *,
    label: str,
) -> None:
    if database_record is None:
        raise UHCRetainedAdmissionError(f"{label} was not persisted")
    mismatch_by_field = {
        key: (database_record[key], expected_value)
        for key, expected_value in expected_fields.items()
        if database_record[key] != expected_value
    }
    if mismatch_by_field:
        raise UHCSourceBindingMismatch(
            f"immutable {label} mismatch: {mismatch_by_field}"
        )


def _advisory_lock_key(artifact_sha256: str) -> int:
    unsigned_key = int(require_digest(artifact_sha256, "artifact_sha256")[:16], 16)
    return unsigned_key if unsigned_key < 2**63 else unsigned_key - 2**64


def _assert_exact_catalog_record(
    catalog_record: Mapping[str, Any] | None,
    binding: SourceBinding,
    raw_artifact: RetainedRawArtifactProof,
) -> None:
    """Recompute and compare the complete immutable catalog identity."""

    _assert_row_matches(
        catalog_record,
        {
            "family": binding.family,
            "collection_kind": binding.collection_kind,
            "file_name": binding.file_name,
            "source_url": binding.source_url,
            "catalog_modified_at": binding.catalog_modified_at,
            "size_bytes": binding.size_bytes,
            "catalog_entry_sha256": binding.catalog_entry_sha256,
        },
        label="catalog file identity",
    )
    if catalog_record is not None:
        expected_entry_sha256, expected_file_id = expected_catalog_file_hash_pair(
            family=str(catalog_record["family"]),
            collection_kind=str(catalog_record["collection_kind"]),
            file_name=str(catalog_record["file_name"]),
            source_url=str(catalog_record["source_url"]),
            catalog_modified_at=str(catalog_record["catalog_modified_at"]),
            size_bytes=catalog_record["size_bytes"],
        )
        if (
            binding.catalog_entry_sha256 != expected_entry_sha256
            or binding.source_file_id != expected_file_id
        ):
            raise UHCSourceBindingMismatch(
                "catalog file identity does not match all current catalog fields"
            )
    if (
        catalog_record is not None
        and catalog_record["size_bytes"] is not None
        and catalog_record["size_bytes"] != raw_artifact.byte_count
    ):
        raise UHCSourceBindingMismatch(
            "catalog file byte count and raw artifact differ"
        )
    _assert_row_matches(
        catalog_record,
        {"availability": "published", "catalog_support": "cataloged"},
        label="catalog file availability",
    )


def _range_rows(ranges: Sequence[RawRangeProof]) -> list[dict[str, object]]:
    return [
        {
            "range_ordinal": raw_range.range_ordinal,
            "raw_byte_start": raw_range.raw_byte_start,
            "raw_byte_end": raw_range.raw_byte_end,
            "raw_byte_count": raw_range.raw_byte_count,
            "raw_sha256": raw_range.raw_sha256,
            "record_start": raw_range.record_start,
            "record_end": raw_range.record_end,
            "record_count": raw_range.record_count,
            "canonical_sha256": raw_range.canonical_sha256,
            "canonical_byte_count": raw_range.canonical_byte_count,
        }
        for raw_range in ranges
    ]


def _reference_rows(
    raw_artifact: RetainedRawArtifactProof,
    raw_uri: str,
    manifest_uri: str,
) -> list[dict[str, object]]:
    return [
        {
            "content_sha256": raw_artifact.sha256,
            "artifact_kind": "raw",
            "layout_artifact_sha256": None,
            "contract_version": 0,
            "range_count": 0,
            "storage_uri": raw_uri,
        },
        {
            "content_sha256": raw_artifact.manifest_sha256,
            "artifact_kind": "manifest",
            "layout_artifact_sha256": raw_artifact.sha256,
            "contract_version": raw_artifact.contract_version,
            "range_count": raw_artifact.range_count,
            "storage_uri": manifest_uri,
        },
    ]


def _decode_proof_rows(value: object) -> dict[str, object]:
    if not isinstance(value, str):
        raise UHCRetainedAdmissionError("retained UHC proof batch is invalid")
    try:
        decoded = json.loads(value)
    except ValueError as error:
        raise UHCRetainedAdmissionError(
            "retained UHC proof batch is invalid"
        ) from error
    if not isinstance(decoded, dict):
        raise UHCRetainedAdmissionError("retained UHC proof batch is invalid")
    return decoded


async def persist_source_proofs(
    connection: asyncpg.Connection,
    *,
    binding: SourceBinding,
    raw_artifact: RetainedRawArtifactProof,
    ranges: Sequence[RawRangeProof],
) -> None:
    """Persist and return-verify every immutable row in one set-based query."""

    raw_uri = file_uri(raw_artifact.path)
    manifest_uri = file_uri(raw_artifact.manifest_path)
    range_rows = _range_rows(ranges)
    reference_rows = _reference_rows(raw_artifact, raw_uri, manifest_uri)
    database_record = await connection.fetchrow(
        f"""/* uhc_retained_proof_batch_v2 */
        WITH lock_gate AS MATERIALIZED (
            SELECT pg_advisory_xact_lock($26::bigint) AS acquired
        ), catalog_gate AS MATERIALIZED (
            SELECT catalog.family, catalog.collection_kind,
                   catalog.file_name, catalog.catalog_entry_sha256,
                   catalog.source_url, catalog.catalog_modified_at,
                   catalog.size_bytes, catalog.availability,
                   catalog.catalog_support
              FROM {table_name('provider_directory_uhc_catalog_file')} AS catalog
              CROSS JOIN lock_gate
             WHERE catalog.catalog_set_sha256=$15
               AND catalog.file_id=$16
               FOR SHARE OF catalog
        ), raw_upsert AS (
            INSERT INTO {table_name('provider_directory_uhc_raw_artifact')} (
                artifact_sha256, byte_count, storage_uri, status,
                verified_at, created_at
            )
            SELECT $1, $2, $3, 'verified', now(), now()
              FROM catalog_gate
            ON CONFLICT (artifact_sha256) DO UPDATE
                SET artifact_sha256=EXCLUDED.artifact_sha256
            RETURNING artifact_sha256, byte_count, storage_uri, status
        ), layout_upsert AS (
            INSERT INTO {table_name('provider_directory_uhc_raw_layout')} (
                artifact_sha256, contract_version, range_count, record_count,
                contract_id, canonicalization_id, producer_build_id,
                range_set_sha256, canonical_byte_count,
                manifest_sha256, manifest_byte_count, manifest_storage_uri,
                status, verified_at, created_at
            )
            SELECT $1, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                   $14, 'verified', now(), now()
              FROM raw_upsert
            ON CONFLICT (artifact_sha256, contract_version, range_count)
            DO UPDATE SET artifact_sha256=EXCLUDED.artifact_sha256
            RETURNING artifact_sha256, contract_version, range_count,
                      record_count, contract_id, canonicalization_id,
                      producer_build_id, range_set_sha256,
                      canonical_byte_count, manifest_sha256,
                      manifest_byte_count, manifest_storage_uri, status
        ), binding_upsert AS (
            INSERT INTO {table_name('provider_directory_uhc_source_binding')} (
                catalog_set_sha256, source_file_id, family, collection_kind,
                file_name, source_url, catalog_modified_at, size_bytes,
                catalog_entry_sha256, artifact_sha256,
                bound_at, released_at
            )
            SELECT $15, $16, $17, $18, $19, $20, $21, $22, $23, $1,
                   now(), NULL
              FROM raw_upsert
            ON CONFLICT (catalog_set_sha256, source_file_id)
            DO UPDATE SET catalog_set_sha256=EXCLUDED.catalog_set_sha256
            RETURNING catalog_set_sha256, source_file_id, family,
                      collection_kind, file_name, source_url,
                      catalog_modified_at, size_bytes, catalog_entry_sha256,
                      artifact_sha256, released_at
        ), range_input AS (
            SELECT *
              FROM jsonb_to_recordset($24::jsonb) AS input (
                range_ordinal integer,
                raw_byte_start bigint,
                raw_byte_end bigint,
                raw_byte_count bigint,
                raw_sha256 text,
                record_start bigint,
                record_end bigint,
                record_count bigint,
                canonical_sha256 text,
                canonical_byte_count bigint
              )
        ), range_upsert AS (
            INSERT INTO {table_name('provider_directory_uhc_raw_range')} (
                artifact_sha256, contract_version, range_count, range_ordinal,
                raw_byte_start, raw_byte_end, raw_byte_count, raw_sha256,
                record_start, record_end, record_count,
                canonical_sha256, canonical_byte_count,
                status, verified_at
            )
            SELECT $1, $4, $5, input.range_ordinal,
                   input.raw_byte_start, input.raw_byte_end,
                   input.raw_byte_count, input.raw_sha256,
                   input.record_start, input.record_end, input.record_count,
                   input.canonical_sha256, input.canonical_byte_count,
                   'verified', now()
              FROM range_input AS input
              CROSS JOIN layout_upsert
            ON CONFLICT (
                artifact_sha256, contract_version, range_count, range_ordinal
            ) DO UPDATE SET artifact_sha256=EXCLUDED.artifact_sha256
            RETURNING artifact_sha256, contract_version, range_count,
                      range_ordinal, raw_byte_start, raw_byte_end,
                      raw_byte_count, raw_sha256,
                      record_start, record_end, record_count,
                      canonical_sha256, canonical_byte_count, status
        ), reference_input AS (
            SELECT *
              FROM jsonb_to_recordset($25::jsonb) AS input (
                content_sha256 text,
                artifact_kind text,
                layout_artifact_sha256 text,
                contract_version integer,
                range_count integer,
                storage_uri text
              )
        ), reference_upsert AS (
            INSERT INTO {table_name('provider_directory_uhc_artifact_reference')} (
                content_sha256, artifact_kind, layout_artifact_sha256,
                contract_version, range_count,
                catalog_set_sha256, source_file_id, storage_uri,
                created_at, retain_until, released_at
            )
            SELECT input.content_sha256, input.artifact_kind,
                   input.layout_artifact_sha256, input.contract_version,
                   input.range_count, $15, $16, input.storage_uri,
                   now(), NULL, NULL
              FROM reference_input AS input
              CROSS JOIN binding_upsert
              CROSS JOIN layout_upsert
            ON CONFLICT (
                catalog_set_sha256, source_file_id, artifact_kind,
                contract_version, range_count
            ) DO UPDATE SET catalog_set_sha256=EXCLUDED.catalog_set_sha256
            RETURNING content_sha256, artifact_kind,
                      layout_artifact_sha256, contract_version, range_count,
                      storage_uri, retain_until, released_at
        )
        SELECT jsonb_build_object(
            'catalog', (
                SELECT to_jsonb(row_value) FROM catalog_gate AS row_value
            ),
            'raw', (SELECT to_jsonb(row_value) FROM raw_upsert AS row_value),
            'layout', (
                SELECT to_jsonb(row_value) FROM layout_upsert AS row_value
            ),
            'binding', (
                SELECT to_jsonb(row_value) FROM binding_upsert AS row_value
            ),
            'ranges', COALESCE((
                SELECT jsonb_agg(to_jsonb(row_value)
                                 ORDER BY row_value.range_ordinal)
                  FROM range_upsert AS row_value
            ), '[]'::jsonb),
            'references', COALESCE((
                SELECT jsonb_agg(to_jsonb(row_value)
                                 ORDER BY row_value.artifact_kind)
                  FROM reference_upsert AS row_value
            ), '[]'::jsonb)
        )::text AS proof_rows""",
        raw_artifact.sha256,
        raw_artifact.byte_count,
        raw_uri,
        raw_artifact.contract_version,
        raw_artifact.range_count,
        raw_artifact.record_count,
        RANGE_CONTRACT_ID,
        RANGE_CANONICALIZATION_ID,
        raw_artifact.producer_build_id,
        raw_artifact.range_set_sha256,
        raw_artifact.canonical_byte_count,
        raw_artifact.manifest_sha256,
        raw_artifact.manifest_byte_count,
        manifest_uri,
        binding.catalog_set_sha256,
        binding.source_file_id,
        binding.family,
        binding.collection_kind,
        binding.file_name,
        binding.source_url,
        binding.catalog_modified_at,
        binding.size_bytes,
        binding.catalog_entry_sha256,
        json.dumps(range_rows, separators=(",", ":")),
        json.dumps(reference_rows, separators=(",", ":")),
        _advisory_lock_key(raw_artifact.sha256),
    )
    if database_record is None:
        raise UHCRetainedAdmissionError("retained UHC proof batch was not persisted")
    proof_rows = _decode_proof_rows(database_record["proof_rows"])
    _assert_exact_catalog_record(
        proof_rows.get("catalog"),
        binding,
        raw_artifact,
    )
    expected_raw = {
        "artifact_sha256": raw_artifact.sha256,
        "byte_count": raw_artifact.byte_count,
        "storage_uri": raw_uri,
        "status": "verified",
    }
    expected_layout = {
        "artifact_sha256": raw_artifact.sha256,
        "contract_version": raw_artifact.contract_version,
        "range_count": raw_artifact.range_count,
        "record_count": raw_artifact.record_count,
        "contract_id": RANGE_CONTRACT_ID,
        "canonicalization_id": RANGE_CANONICALIZATION_ID,
        "producer_build_id": raw_artifact.producer_build_id,
        "range_set_sha256": raw_artifact.range_set_sha256,
        "canonical_byte_count": raw_artifact.canonical_byte_count,
        "manifest_sha256": raw_artifact.manifest_sha256,
        "manifest_byte_count": raw_artifact.manifest_byte_count,
        "manifest_storage_uri": manifest_uri,
        "status": "verified",
    }
    expected_binding = {
        "catalog_set_sha256": binding.catalog_set_sha256,
        "source_file_id": binding.source_file_id,
        "family": binding.family,
        "collection_kind": binding.collection_kind,
        "file_name": binding.file_name,
        "source_url": binding.source_url,
        "catalog_modified_at": binding.catalog_modified_at,
        "size_bytes": binding.size_bytes,
        "catalog_entry_sha256": binding.catalog_entry_sha256,
        "artifact_sha256": binding.artifact_sha256,
        "released_at": None,
    }
    _assert_row_matches(proof_rows.get("raw"), expected_raw, label="raw artifact")
    _assert_row_matches(
        proof_rows.get("layout"),
        expected_layout,
        label="raw layout",
    )
    _assert_row_matches(
        proof_rows.get("binding"),
        expected_binding,
        label="source binding",
    )
    observed_ranges = proof_rows.get("ranges")
    expected_ranges = [
        {
            "artifact_sha256": raw_artifact.sha256,
            "contract_version": raw_artifact.contract_version,
            "range_count": raw_artifact.range_count,
            **range_row,
            "status": "verified",
        }
        for range_row in range_rows
    ]
    if observed_ranges != expected_ranges:
        raise UHCSourceBindingMismatch("immutable raw range batch mismatch")
    observed_references = proof_rows.get("references")
    expected_references = sorted(
        (
            {
                **reference_row,
                "retain_until": None,
                "released_at": None,
            }
            for reference_row in reference_rows
        ),
        key=lambda reference_row: str(reference_row["artifact_kind"]),
    )
    if observed_references != expected_references:
        raise UHCSourceBindingMismatch("immutable artifact reference batch mismatch")
