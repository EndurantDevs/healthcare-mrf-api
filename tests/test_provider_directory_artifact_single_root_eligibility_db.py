# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Single-root artifact eligibility database tests."""

from __future__ import annotations

import copy
from datetime import datetime
import importlib
import json

import pytest

from db.connection import Database
from tests import test_provider_directory_artifact_eligibility_db as shared
from tests.provider_directory_fhir_subset_activation_support import (
    activation_inputs,
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
    rows = await database.all(
        f"""
        WITH {importer._artifact_explicit_candidate_ids_ctes(
            dataset_ref, source_ref
        )}
        SELECT dataset_id
          FROM eligible_candidate_ids
         ORDER BY dataset_id;
        """,
        source_ids=["synthetic-source"],
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


async def _insert_twin_candidates(database: Database, schema: str) -> None:
    source_record, dataset_rows, _evidence = activation_inputs()
    source_metadata = copy.deepcopy(source_record["metadata_json"])
    source_metadata["provider_directory_candidate_status"] = (
        importer.PROVIDER_DIRECTORY_SUBSET_TWIN_ROOT_VERIFIED
    )
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
    for dataset_row in dataset_rows:
        await database.status(
            f"""
            INSERT INTO {schema}.provider_directory_endpoint_dataset (
                dataset_id, endpoint_id, acquisition_root_run_id,
                dataset_hash, status, is_current, resource_count,
                publication_metadata_json,
                completion_proof_required_version, completion_proof_json,
                completion_proof_sha256
            ) VALUES (
                :dataset_id, :endpoint_id, :root_run_id,
                :dataset_hash, :status, false, :resource_count,
                CAST(:metadata AS jsonb), :required_version,
                CAST(:completion_proof AS jsonb), :completion_sha256
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


@pytest.mark.asyncio
async def test_v3_twin_candidate_keeps_unsealed_baseline_compatibility(
    monkeypatch,
):
    async with shared._candidate_database(monkeypatch) as (database, schema):
        await _insert_twin_candidates(database, schema)
        assert shared._option_ids(
            await shared._artifact_options(database, schema, "endpoint-a")
        ) == ["dataset-candidate"]
        assert await _compact_eligible_ids(database, schema, "endpoint-a") == [
            "dataset-candidate"
        ]

        await database.status(
            f"""
            UPDATE {schema}.provider_directory_endpoint_dataset
               SET publication_metadata_summary_json =
                       publication_metadata_json,
                   publication_metadata_sha256 = repeat('0', 64),
                   content_proof_admission_version =
                       {importer.ADMISSION_SEAL_VERSION},
                   content_proof_admission_kind =
                       '{importer.ADMISSION_KIND_GENERIC}',
                   content_proof_admission_sha256 = repeat('a', 64),
                   content_proof_resource_types =
                       ARRAY['Organization']::varchar[]
             WHERE dataset_id = 'dataset-candidate';
            """
        )
        assert shared._option_ids(
            await shared._artifact_options(database, schema, "endpoint-a")
        ) == ["dataset-candidate"]
        assert await _compact_eligible_ids(database, schema, "endpoint-a") == [
            "dataset-candidate"
        ]

        await database.status(
            f"""
            UPDATE {schema}.provider_directory_endpoint_dataset
               SET publication_metadata_json = jsonb_set(
                       publication_metadata_json,
                       '{{{importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY},shards}}',
                       (
                           SELECT jsonb_agg(sequence_id)
                             FROM generate_series(1, 1025) AS sequence_id
                       )
                   )
             WHERE dataset_id = 'dataset-baseline';
            """
        )
        assert await shared._artifact_options(
            database, schema, "endpoint-a"
        ) == []
        assert await _compact_eligible_ids(database, schema, "endpoint-a") == []
async def _insert_policy_two_dataset(
    database: Database,
    schema: str,
    dataset_row: dict[str, object],
    policy_document: dict[str, object],
) -> dict[str, object]:
    explicit_policy_row = copy.deepcopy(dataset_row)
    explicit_policy_row["publication_metadata_json"][
        importer.REVIEWED_ROOT_POLICY_METADATA_KEY
    ] = policy_document
    await database.status(
        f"""
        INSERT INTO {schema}.provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, import_run_id, acquisition_root_run_id,
            previous_dataset_id, dataset_hash, status, is_current,
            resource_count, validated_at, publication_metadata_json,
            completion_proof_required_version, completion_proof_json,
            completion_proof_sha256
        ) VALUES (
            :dataset_id, :endpoint_id, :root_run_id, :root_run_id, NULL,
            :dataset_hash, :status, false, :resource_count, :validated_at,
            CAST(:metadata AS jsonb), :required_version,
            CAST(:completion_proof AS jsonb), :completion_sha256
        );
        """,
        dataset_id=explicit_policy_row["dataset_id"],
        endpoint_id=explicit_policy_row["endpoint_id"],
        root_run_id=explicit_policy_row["acquisition_root_run_id"],
        dataset_hash=explicit_policy_row["dataset_hash"],
        status=explicit_policy_row["status"],
        resource_count=explicit_policy_row["resource_count"],
        validated_at=(
            datetime.fromisoformat(
                explicit_policy_row["validated_at"]
            ).replace(tzinfo=None)
            if explicit_policy_row["validated_at"]
            else None
        ),
        metadata=json.dumps(explicit_policy_row["publication_metadata_json"]),
        required_version=explicit_policy_row["completion_proof_required_version"],
        completion_proof=json.dumps(explicit_policy_row["completion_proof_json"]),
        completion_sha256=explicit_policy_row["completion_proof_sha256"],
    )
    return explicit_policy_row


async def _insert_policy_two_subset_pair(
    candidate_store: Database,
    schema: str,
) -> tuple[dict[str, object], dict[str, object]]:
    await candidate_store.status(
        f"ALTER TABLE {schema}.provider_directory_endpoint_dataset "
        "ADD COLUMN import_run_id varchar(64), ADD COLUMN previous_dataset_id varchar(96), "
        "ADD COLUMN validated_at timestamp, "
        "ADD COLUMN published_at timestamp;"
    )
    source_record, dataset_rows, _evidence = activation_inputs()
    policy_document = importer.ReviewedRootPolicy(2).document()
    source_metadata = copy.deepcopy(source_record["metadata_json"])
    source_metadata["provider_directory_candidate_status"] = importer.PROVIDER_DIRECTORY_ROOT_POLICY_VERIFIED
    source_metadata[importer.REVIEWED_ROOT_POLICY_METADATA_KEY] = policy_document
    await candidate_store.status(
        f"""
        INSERT INTO {schema}.provider_directory_source (
            source_id, endpoint_id, metadata_json
        ) VALUES (:source_id, :endpoint_id, CAST(:metadata AS jsonb));
        """,
        source_id=source_record["source_id"],
        endpoint_id=source_record["endpoint_id"],
        metadata=json.dumps(source_metadata),
    )
    for dataset_row in dataset_rows:
        candidate = await _insert_policy_two_dataset(candidate_store, schema, dataset_row, policy_document)
    return source_record, candidate


@pytest.mark.asyncio
async def test_explicit_policy_two_candidate_remains_eligible_and_publishable(
    monkeypatch,
):
    async with shared._candidate_database(monkeypatch) as (database, schema):
        source_record, candidate = await _insert_policy_two_subset_pair(database, schema)
        endpoint_id = candidate["endpoint_id"]
        candidate_id = candidate["dataset_id"]
        assert shared._option_ids(await shared._artifact_options(database, schema, endpoint_id)) == [
            candidate_id
        ]
        assert await shared._compact_candidate_ids(
            database, endpoint_id, source_record["source_id"]
        ) == [candidate_id]

        monkeypatch.setattr(importer, "db", database)
        await importer._publish_validated_artifact_dataset(
            importer.ProviderDirectoryArtifactDataset(
                source_id=source_record["source_id"],
                endpoint_id=endpoint_id,
                dataset_id=candidate_id,
                evidence_run_id=candidate["acquisition_root_run_id"],
                status=importer.ENDPOINT_DATASET_VALIDATED,
                is_current=False,
                dataset_hash=candidate["dataset_hash"],
                resource_count=candidate["resource_count"],
                reviewed_root_policy=importer.ReviewedRootPolicy(2),
                completion_proof_required_version=3,
            )
        )
        published = await database.first(
            f"""SELECT status, is_current, published_at
                  FROM {schema}.provider_directory_endpoint_dataset
                 WHERE dataset_id = :dataset_id""",
            dataset_id=candidate_id,
        )
        assert published is not None
        assert published._mapping["status"] == importer.ENDPOINT_DATASET_PUBLISHED
        assert published._mapping["is_current"] is True
        assert published._mapping["published_at"] is not None


@pytest.mark.asyncio
async def test_profile_absent_candidate_rejects_oversized_legacy_proof(
    monkeypatch,
):
    async with shared._candidate_database(monkeypatch) as (database, schema):
        await database.status(
            f"""
            INSERT INTO {schema}.provider_directory_source (
                source_id, endpoint_id, metadata_json
            ) VALUES ('oversized_source', 'oversized_endpoint', '{{}}'::jsonb);
            """
        )
        await database.status(
            f"""
            INSERT INTO {schema}.provider_directory_endpoint_dataset (
                dataset_id, endpoint_id, acquisition_root_run_id,
                dataset_hash, status, is_current, resource_count,
                publication_metadata_json
            ) VALUES (
                'oversized_candidate', 'oversized_endpoint', 'oversized_root',
                :dataset_hash, :validated, false, 1,
                jsonb_build_object(
                    'requires_twin_root_verification', false,
                    'source_ids', jsonb_build_array('oversized_source'),
                    '{importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY}',
                    jsonb_build_object(
                        'shards', (
                            SELECT jsonb_agg(sequence_id)
                              FROM generate_series(1, 1025) AS sequence_id
                        )
                    )
                )
            );
            """,
            dataset_hash=shared.DATASET_HASH,
            validated=importer.ENDPOINT_DATASET_VALIDATED,
        )

        assert await shared._artifact_options(
            database, schema, "oversized_endpoint"
        ) == []
        dataset_ref = f"{schema}.provider_directory_endpoint_dataset"
        source_ref = f"{schema}.provider_directory_source"
        candidate_rows = await database.all(
            f"""
            WITH {importer._artifact_explicit_candidate_ids_ctes(
                dataset_ref, source_ref
            )}
            SELECT dataset_id
              FROM eligible_candidate_ids;
            """,
            source_ids=["oversized_source"],
            validated_status=importer.ENDPOINT_DATASET_VALIDATED,
        )
        assert candidate_rows == []
