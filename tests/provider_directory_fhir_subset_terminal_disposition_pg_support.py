# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL seed support for one neutral mixed-terminal retained root."""

from __future__ import annotations

from copy import deepcopy
import json

from process.provider_directory_fhir_root_policy import (
    POLICY_PENDING_STATUS,
    REVIEWED_ROOT_POLICY_METADATA_KEY,
)
from tests.provider_directory_fhir_subset_terminal_disposition_support import (
    POLICY,
    terminal_disposition_inputs,
)
from tests.provider_directory_subset_completion_pg_setup import (
    insert_subset_candidate,
)
from tests.provider_directory_subset_completion_pg_support import (
    valid_source_metadata,
)


def _terminal_seed_records():
    """Return mutable records for one neutral retained mixed-terminal root."""
    source_record, candidate_record, checkpoint_records = (
        terminal_disposition_inputs()
    )
    source_metadata = valid_source_metadata(POLICY_PENDING_STATUS)
    source_metadata.update(deepcopy(source_record["metadata_json"]))
    source_metadata[REVIEWED_ROOT_POLICY_METADATA_KEY] = deepcopy(POLICY)
    source_metadata["provider_directory_verification_campaign_id"] = (
        candidate_record["publication_metadata_json"]["verification_campaign_id"]
    )
    source_record["metadata_json"] = source_metadata
    return source_record, candidate_record, checkpoint_records


async def _insert_terminal_source_and_candidate(
    scenario,
    source_record,
    candidate_record,
) -> None:
    """Insert the source and failed candidate without terminalizing either."""
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
        ) VALUES (
            $1, $2, $3, false, false, 'none', $4::jsonb,
            pg_catalog.transaction_timestamp()
        )
        """,
        source_record["source_id"],
        source_record["endpoint_id"],
        source_record["canonical_api_base"],
        json.dumps(source_record["metadata_json"]),
    )
    await insert_subset_candidate(
        scenario,
        dataset_id=candidate_record["dataset_id"],
        root_run_id=candidate_record["acquisition_root_run_id"],
        resource_count=candidate_record["resource_count"],
    )
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET import_run_id = $1,
               status = 'failed',
               publication_metadata_json = $2::jsonb
        WHERE dataset_id = $3
        """,
        candidate_record["import_run_id"],
        json.dumps(candidate_record["publication_metadata_json"]),
        candidate_record["dataset_id"],
    )


async def _insert_terminal_resources(
    scenario,
    candidate_record,
    checkpoint_records,
) -> dict[str, int]:
    """Insert retained resources and return their exact per-type counts."""
    resource_rows = []
    resource_count_by_type = {}
    for ordinal, checkpoint in enumerate(checkpoint_records, start=1):
        resource_type = checkpoint["resource_type"]
        resource_count_by_type[resource_type] = checkpoint["rows_processed"]
        for row_ordinal in range(checkpoint["rows_processed"]):
            resource_rows.append(
                (
                    candidate_record["dataset_id"],
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
    return resource_count_by_type


async def _insert_terminal_proof_shard(
    scenario,
    source_record,
    candidate_record,
    checkpoint_records,
    resource_count_by_type,
) -> None:
    """Insert one proof shard that exactly covers every retained resource."""
    await scenario.connection.execute(
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
        candidate_record["dataset_id"],
        "a" * 64,
        candidate_record["endpoint_id"],
        candidate_record["acquisition_root_run_id"],
        json.dumps([source_record["source_id"]]),
        candidate_record["resource_count"],
        json.dumps(resource_count_by_type),
        json.dumps(
            [checkpoint_records[0]["resource_type"], "resource-first", "b" * 64]
        ),
        json.dumps(
            [checkpoint_records[-1]["resource_type"], "resource-last", "c" * 64]
        ),
        "d" * 64,
        "e" * 64,
        b"proof",
    )


async def _insert_terminal_checkpoints(scenario, checkpoint_records) -> None:
    """Insert the seven active/complete checkpoints with their exact proofs."""
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
                checkpoint["canonical_api_base"],
                checkpoint["resource_type"],
                checkpoint["source_scope_hash"],
                checkpoint["dataset_id"],
                json.dumps(checkpoint["source_ids"]),
                checkpoint["acquisition_root_run_id"],
                checkpoint["owner_run_id"],
                checkpoint["retry_of_run_id"],
                checkpoint["start_url_hash"],
                checkpoint["next_url"],
                checkpoint["state"],
                checkpoint["pages_processed"],
                checkpoint["rows_processed"],
                json.dumps(checkpoint["recent_cursor_hashes"]),
                json.dumps(checkpoint["completeness_json"]),
                checkpoint["updated_at"],
            )
            for checkpoint in checkpoint_records
        ],
    )


async def seed_mixed_terminal_root(scenario) -> None:
    """Seed one failed retained root without pre-enacting its disposition."""
    source_record, candidate_record, checkpoint_records = _terminal_seed_records()
    await _insert_terminal_source_and_candidate(
        scenario,
        source_record,
        candidate_record,
    )
    resource_count_by_type = await _insert_terminal_resources(
        scenario,
        candidate_record,
        checkpoint_records,
    )
    await _insert_terminal_proof_shard(
        scenario,
        source_record,
        candidate_record,
        checkpoint_records,
        resource_count_by_type,
    )
    await _insert_terminal_checkpoints(scenario, checkpoint_records)
    await scenario.connection.execute(
        "SET CONSTRAINTS ALL IMMEDIATE; SET CONSTRAINTS ALL DEFERRED;"
    )


__all__ = ("seed_mixed_terminal_root",)
