# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retained-evidence validation for reviewed subset abandonment."""

from __future__ import annotations

import json
from typing import Any, Mapping, Sequence

from process.provider_directory_fhir_subset_abandonment_contract import (
    ReviewedSubsetAbandonmentError,
    _json_object,
    _json_text_tuple,
    _quoted_relation,
    _row_mapping,
    _text,
)


def _checkpoint_identity_context(
    checkpoint_rows: Sequence[Mapping[str, Any]],
    candidate_by_field: Mapping[str, Any],
    source_by_field: Mapping[str, Any],
    resource_types: tuple[str, ...],
) -> tuple[str, tuple[str, ...], str, str, str]:
    candidate_metadata = _json_object(
        candidate_by_field.get("publication_metadata_json")
    )
    source_scope_sha256 = _text(
        candidate_metadata.get("verification_source_scope_hash")
    )
    expected_source_ids = _json_text_tuple(candidate_metadata.get("source_ids"))
    expected_owner = _text(candidate_by_field.get("import_run_id"))
    expected_root = _text(candidate_by_field.get("acquisition_root_run_id"))
    canonical_api_base = _text(source_by_field.get("canonical_api_base"))
    checkpoint_resource_types = tuple(
        checkpoint_by_field.get("resource_type")
        for checkpoint_by_field in checkpoint_rows
    )
    if (
        source_scope_sha256 is None
        or len(source_scope_sha256) != 64
        or expected_owner is None
        or expected_root is None
        or canonical_api_base is None
        or checkpoint_resource_types != resource_types
    ):
        raise ReviewedSubsetAbandonmentError("evidence")
    return (
        source_scope_sha256,
        expected_source_ids,
        expected_owner,
        expected_root,
        canonical_api_base,
    )


def validated_checkpoint_summary(
    checkpoint_rows: Sequence[Mapping[str, Any]],
    candidate_by_field: Mapping[str, Any],
    source_by_field: Mapping[str, Any],
    resource_types: tuple[str, ...],
) -> tuple[str, int, int]:
    """Validate checkpoint lineage and return its scope and count totals."""

    (
        source_scope_sha256,
        expected_source_ids,
        expected_owner,
        expected_root,
        canonical_api_base,
    ) = _checkpoint_identity_context(
        checkpoint_rows,
        candidate_by_field,
        source_by_field,
        resource_types,
    )
    pages_processed = 0
    rows_processed = 0
    for checkpoint_by_field in checkpoint_rows:
        if (
            _text(checkpoint_by_field.get("canonical_api_base")) != canonical_api_base
            or _text(checkpoint_by_field.get("source_scope_hash"))
            != source_scope_sha256
            or _json_text_tuple(checkpoint_by_field.get("source_ids"))
            != expected_source_ids
            or _text(checkpoint_by_field.get("dataset_id"))
            != _text(candidate_by_field.get("dataset_id"))
            or _text(checkpoint_by_field.get("acquisition_root_run_id"))
            != expected_root
            or _text(checkpoint_by_field.get("owner_run_id")) != expected_owner
            or _text(checkpoint_by_field.get("state")) not in {"active", "complete"}
            or type(checkpoint_by_field.get("pages_processed")) is not int
            or checkpoint_by_field["pages_processed"] < 0
            or type(checkpoint_by_field.get("rows_processed")) is not int
            or checkpoint_by_field["rows_processed"] < 0
        ):
            raise ReviewedSubsetAbandonmentError("evidence")
        pages_processed += checkpoint_by_field["pages_processed"]
        rows_processed += checkpoint_by_field["rows_processed"]
    return source_scope_sha256, pages_processed, rows_processed


async def _resource_counts_by_type(
    database: Any,
    dataset_id: str,
    checkpoint_counts_by_type: Mapping[str, int],
) -> dict[str, int]:
    resource_rows = await database.all(
        f"""
        SELECT resource_type, count(*) AS resource_count
          FROM {_quoted_relation('provider_directory_dataset_resource')}
         WHERE dataset_id = :dataset_id
           AND resource_type NOT LIKE 'LU:%:pass:%'
         GROUP BY resource_type
         ORDER BY resource_type;
        """,
        dataset_id=dataset_id,
    )
    resource_counts_by_type = dict.fromkeys(checkpoint_counts_by_type, 0)
    resource_counts_by_type.update(
        {
            str(_row_mapping(resource_by_field)["resource_type"]): int(
                _row_mapping(resource_by_field)["resource_count"]
            )
            for resource_by_field in resource_rows
        }
    )
    if resource_counts_by_type != checkpoint_counts_by_type:
        raise ReviewedSubsetAbandonmentError("evidence")
    return resource_counts_by_type


async def _proof_totals(
    database: Any,
    dataset_id: str,
    root_run_id: str,
    source_ids: tuple[str, ...],
) -> tuple[int, int]:
    proof_rows = await database.all(
        f"""
        SELECT count(*) AS shard_count,
               COALESCE(sum(shard.resource_count), 0) AS proof_row_count,
               count(*) FILTER (
                   WHERE shard.acquisition_root_run_id IS DISTINCT FROM
                             :root_run_id
                      OR shard.source_ids_json IS DISTINCT FROM
                             CAST(:source_ids AS jsonb)
               ) AS invalid_lineage_count
          FROM {_quoted_relation('provider_directory_dataset_proof_shard')}
               AS shard
         WHERE shard.dataset_id = :dataset_id;
        """,
        dataset_id=dataset_id,
        root_run_id=root_run_id,
        source_ids=json.dumps(source_ids),
    )
    if len(proof_rows) != 1:
        raise ReviewedSubsetAbandonmentError("evidence")
    proof_by_field = _row_mapping(proof_rows[0])
    if int(proof_by_field.get("invalid_lineage_count") or 0) != 0:
        raise ReviewedSubsetAbandonmentError("evidence")
    return (
        int(proof_by_field.get("shard_count") or 0),
        int(proof_by_field.get("proof_row_count") or 0),
    )


async def _proof_counts_by_type(
    database: Any,
    dataset_id: str,
    checkpoint_counts_by_type: Mapping[str, int],
) -> dict[str, int]:
    proof_resource_rows = await database.all(
        f"""
        SELECT raw_count.resource_type,
               sum(CAST(raw_count.resource_count_text AS bigint))
                   AS resource_count
          FROM {_quoted_relation('provider_directory_dataset_proof_shard')}
               AS shard
          CROSS JOIN LATERAL pg_catalog.jsonb_each_text(
               shard.resource_counts_json
          ) AS raw_count(resource_type, resource_count_text)
         WHERE shard.dataset_id = :dataset_id
         GROUP BY raw_count.resource_type
         ORDER BY raw_count.resource_type;
        """,
        dataset_id=dataset_id,
    )
    proof_counts_by_type = dict.fromkeys(checkpoint_counts_by_type, 0)
    proof_counts_by_type.update(
        {
            str(_row_mapping(proof_resource_by_field)["resource_type"]): int(
                _row_mapping(proof_resource_by_field)["resource_count"]
            )
            for proof_resource_by_field in proof_resource_rows
        }
    )
    return proof_counts_by_type


async def retained_evidence_counts(
    database: Any,
    candidate_by_field: Mapping[str, Any],
    checkpoint_rows: Sequence[Mapping[str, Any]],
) -> tuple[int, int, int]:
    """Return exact retained resource and proof counts after parity checks."""

    dataset_id = str(candidate_by_field["dataset_id"])
    root_run_id = str(candidate_by_field["acquisition_root_run_id"])
    source_ids = _json_text_tuple(
        _json_object(candidate_by_field["publication_metadata_json"])["source_ids"]
    )
    checkpoint_counts_by_type = {
        str(checkpoint_by_field["resource_type"]): int(
            checkpoint_by_field["rows_processed"]
        )
        for checkpoint_by_field in checkpoint_rows
    }
    resource_counts_by_type = await _resource_counts_by_type(
        database,
        dataset_id,
        checkpoint_counts_by_type,
    )
    proof_shard_count, proof_row_count = await _proof_totals(
        database,
        dataset_id,
        root_run_id,
        source_ids,
    )
    proof_counts_by_type = await _proof_counts_by_type(
        database,
        dataset_id,
        checkpoint_counts_by_type,
    )
    resource_count = sum(resource_counts_by_type.values())
    if (
        resource_count != proof_row_count
        or proof_counts_by_type != checkpoint_counts_by_type
    ):
        raise ReviewedSubsetAbandonmentError("evidence")
    return resource_count, proof_shard_count, proof_row_count
