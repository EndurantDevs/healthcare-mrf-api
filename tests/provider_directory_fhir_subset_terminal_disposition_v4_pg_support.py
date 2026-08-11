# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL seed support for the neutral direct-v4 terminal profile."""

from __future__ import annotations

import json

from tests.provider_directory_fhir_subset_terminal_disposition_v4_support import (
    direct_v4_inputs,
)
from tests.provider_directory_subset_completion_pg_setup import (
    insert_subset_candidate,
)


async def _insert_source_and_candidate(
    scenario,
    source_by_field,
    candidate_by_field,
) -> None:
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_api_endpoint (
            endpoint_id
        ) VALUES ('endpoint-serving'), ('endpoint-a')
        """
    )
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_source (
            source_id, endpoint_id, canonical_api_base,
            requires_registration, requires_api_key, auth_type,
            metadata_json, updated_at
        ) VALUES ($1, $2, $3, false, false, 'none', $4::jsonb,
                  pg_catalog.transaction_timestamp())
        """,
        source_by_field["source_id"],
        source_by_field["endpoint_id"],
        source_by_field["canonical_api_base"],
        json.dumps(source_by_field["metadata_json"]),
    )
    await insert_subset_candidate(
        scenario,
        dataset_id=candidate_by_field["dataset_id"],
        root_run_id=candidate_by_field["acquisition_root_run_id"],
        resource_count=candidate_by_field["resource_count"],
    )
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET import_run_id = $1,
               status = 'failed',
               publication_metadata_json = $2::jsonb
         WHERE dataset_id = $3
        """,
        candidate_by_field["import_run_id"],
        json.dumps(candidate_by_field["publication_metadata_json"]),
        candidate_by_field["dataset_id"],
    )


async def _insert_resources(
    scenario,
    candidate_by_field,
    checkpoint_rows,
) -> None:
    resource_rows = []
    for ordinal, checkpoint_by_field in enumerate(checkpoint_rows, start=1):
        resource_type = checkpoint_by_field["resource_type"]
        for row_ordinal in range(checkpoint_by_field["rows_processed"]):
            resource_rows.append(
                (
                    candidate_by_field["dataset_id"],
                    resource_type,
                    f"resource-{ordinal}-{row_ordinal}",
                    f"{ordinal:064x}",
                    json.dumps({"resourceType": resource_type}),
                    f"{ordinal + 20:064x}",
                )
            )
    await scenario.connection.executemany(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_dataset_resource (
            dataset_id, resource_type, resource_id, payload_hash,
            payload_json, acquired_resource_sha256
        ) VALUES ($1, $2, $3, $4, $5::jsonb, $6)
        """,
        resource_rows,
    )


async def _insert_proof_shards(
    scenario,
    source_by_field,
    candidate_by_field,
    checkpoint_rows,
) -> None:
    proof_rows = []
    for ordinal, checkpoint_by_field in enumerate(checkpoint_rows, start=1):
        resource_type = checkpoint_by_field["resource_type"]
        resource_count = checkpoint_by_field["rows_processed"]
        proof_rows.append(
            (
                candidate_by_field["dataset_id"],
                f"{ordinal + 30:064x}",
                candidate_by_field["endpoint_id"],
                candidate_by_field["acquisition_root_run_id"],
                json.dumps([source_by_field["source_id"]]),
                resource_count,
                json.dumps({resource_type: resource_count}),
                json.dumps([resource_type, "first", f"{ordinal + 40:064x}"]),
                json.dumps([resource_type, "last", f"{ordinal + 50:064x}"]),
                f"{ordinal + 60:064x}",
                f"{ordinal + 70:064x}",
                b"proof",
            )
        )
    await scenario.connection.executemany(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_dataset_proof_shard (
            dataset_id, shard_id, endpoint_id, acquisition_root_run_id,
            source_ids_json, resource_count, resource_counts_json,
            first_identity_json, last_identity_json, input_sha256,
            artifact_sha256, artifact_byte_count, payload_bytes
        ) VALUES (
            $1, $2, $3, $4, $5::jsonb, $6, $7::jsonb,
            $8::jsonb, $9::jsonb, $10, $11, 5, $12
        )
        """,
        proof_rows,
    )


async def _insert_checkpoints(scenario, checkpoint_rows) -> None:
    await scenario.connection.executemany(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_pagination_checkpoint (
            canonical_api_base, resource_type, source_scope_hash, dataset_id,
            source_ids, acquisition_root_run_id, owner_run_id, retry_of_run_id,
            start_url_hash, next_url, state, pages_processed, rows_processed,
            recent_cursor_hashes, completeness_json, updated_at, completed_at
        ) VALUES (
            $1, $2, $3, $4, $5::jsonb, $6, $7, $8,
            $9, $10, $11::varchar, $12, $13, $14::jsonb, $15::jsonb,
            $16::timestamp,
            CASE WHEN $11::varchar = 'complete'
                 THEN $16::timestamp ELSE NULL END
        )
        """,
        [
            (
                checkpoint_by_field["canonical_api_base"],
                checkpoint_by_field["resource_type"],
                checkpoint_by_field["source_scope_hash"],
                checkpoint_by_field["dataset_id"],
                json.dumps(checkpoint_by_field["source_ids"]),
                checkpoint_by_field["acquisition_root_run_id"],
                checkpoint_by_field["owner_run_id"],
                checkpoint_by_field["retry_of_run_id"],
                checkpoint_by_field["start_url_hash"],
                checkpoint_by_field["next_url"],
                checkpoint_by_field["state"],
                checkpoint_by_field["pages_processed"],
                checkpoint_by_field["rows_processed"],
                json.dumps(checkpoint_by_field["recent_cursor_hashes"]),
                json.dumps(checkpoint_by_field["completeness_json"]),
                checkpoint_by_field["updated_at"],
            )
            for checkpoint_by_field in checkpoint_rows
        ],
    )


async def seed_direct_v4_terminal_root(scenario) -> None:
    """Seed one failed direct-v4 root without enacting its disposition."""

    source_by_field, candidate_by_field, checkpoint_rows = direct_v4_inputs()
    await _insert_source_and_candidate(
        scenario,
        source_by_field,
        candidate_by_field,
    )
    await _insert_resources(scenario, candidate_by_field, checkpoint_rows)
    await _insert_proof_shards(
        scenario,
        source_by_field,
        candidate_by_field,
        checkpoint_rows,
    )
    await _insert_checkpoints(scenario, checkpoint_rows)
    await scenario.connection.execute(
        "SET CONSTRAINTS ALL IMMEDIATE; SET CONSTRAINTS ALL DEFERRED;"
    )


__all__ = ("seed_direct_v4_terminal_root",)
