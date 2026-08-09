# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Single-root artifact eligibility database tests."""

from __future__ import annotations

import copy
import importlib
import json

import pytest

from db.connection import Database
from tests import test_provider_directory_artifact_eligibility_db as shared
from tests.provider_directory_fhir_subset_activation_support import (
    single_root_activation_inputs,
)


importer = importlib.import_module("process.provider_directory_fhir")


async def _compact_eligible_ids(
    database: Database,
    schema: str,
    endpoint_id: str,
) -> list[str]:
    dataset_ref = f"{schema}.provider_directory_endpoint_dataset"
    source_ref = f"{schema}.provider_directory_source"
    gate = importer._artifact_reviewed_candidate_eligibility_sql(
        dataset_ref,
        source_ref,
        metadata="dataset.eligibility_metadata_jsonb",
    )
    rows = await database.all(
        f"""
        WITH {importer._artifact_candidate_eligibility_ctes(dataset_ref)}
        SELECT dataset.dataset_id
          FROM artifact_candidate_metadata AS dataset
         WHERE {gate}
         ORDER BY dataset.dataset_id;
        """,
        endpoint_ids=[endpoint_id],
        validated_status=importer.ENDPOINT_DATASET_VALIDATED,
    )
    return [str(row._mapping["dataset_id"]) for row in rows]


async def _insert_single_root_candidate(
    database: Database,
    schema: str,
) -> dict[str, object]:
    source_record, dataset_rows, _evidence = single_root_activation_inputs()
    source_metadata = copy.deepcopy(source_record["metadata_json"])
    source_metadata["provider_directory_candidate_status"] = (
        importer.PROVIDER_DIRECTORY_ROOT_POLICY_VERIFIED
    )
    dataset_row = dataset_rows[0]
    await database.status(
        f"""
        INSERT INTO {schema}.provider_directory_source (
            source_id, endpoint_id, metadata_json
        ) VALUES (:source_id, :endpoint_id, CAST(:metadata AS jsonb));
        """,
        source_id=source_record["source_id"],
        endpoint_id=source_record["endpoint_id"],
        metadata=json.dumps(source_metadata),
    )
    await database.status(
        f"""
        INSERT INTO {schema}.provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, acquisition_root_run_id, dataset_hash,
            status, is_current, resource_count, publication_metadata_json,
            completion_proof_required_version, completion_proof_json,
            completion_proof_sha256
        ) VALUES (
            :dataset_id, :endpoint_id, :root_run_id, :dataset_hash,
            :status, false, :resource_count, CAST(:metadata AS jsonb),
            :required_version, CAST(:completion_proof AS jsonb),
            :completion_sha256
        );
        """,
        dataset_id=dataset_row["dataset_id"],
        endpoint_id=dataset_row["endpoint_id"],
        root_run_id=dataset_row["acquisition_root_run_id"],
        dataset_hash=dataset_row["dataset_hash"],
        status=dataset_row["status"],
        resource_count=dataset_row["resource_count"],
        metadata=json.dumps(dataset_row["publication_metadata_json"]),
        required_version=dataset_row["completion_proof_required_version"],
        completion_proof=json.dumps(dataset_row["completion_proof_json"]),
        completion_sha256=dataset_row["completion_proof_sha256"],
    )
    return dataset_row


@pytest.mark.asyncio
async def test_single_root_policy_candidate_requires_exact_stored_proof(
    monkeypatch,
):
    async with shared._candidate_database(monkeypatch) as (database, schema):
        dataset_row = await _insert_single_root_candidate(database, schema)
        assert shared._option_ids(
            await shared._artifact_options(database, schema, "endpoint-a")
        ) == [dataset_row["dataset_id"]]
        assert await _compact_eligible_ids(
            database, schema, "endpoint-a"
        ) == [dataset_row["dataset_id"]]

        await database.status(
            f"""
            UPDATE {schema}.provider_directory_endpoint_dataset
               SET publication_metadata_json = publication_metadata_json
                   || '{{"verification_role": null}}'::jsonb
             WHERE dataset_id = :dataset_id;
            """,
            dataset_id=dataset_row["dataset_id"],
        )
        assert await _compact_eligible_ids(database, schema, "endpoint-a") == []
        await database.status(
            f"""
            UPDATE {schema}.provider_directory_endpoint_dataset
               SET publication_metadata_json = publication_metadata_json
                   - 'verification_role'
             WHERE dataset_id = :dataset_id;
            """,
            dataset_id=dataset_row["dataset_id"],
        )
        await database.status(
            f"""
            UPDATE {schema}.provider_directory_endpoint_dataset
               SET publication_metadata_json = jsonb_set(
                   publication_metadata_json,
                   '{{{importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY},dataset_hash}}',
                   '"tampered"'::jsonb
               )
             WHERE dataset_id = :dataset_id;
            """,
            dataset_id=dataset_row["dataset_id"],
        )
        assert await shared._artifact_options(database, schema, "endpoint-a") == []
