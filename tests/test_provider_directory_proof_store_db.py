# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from contextlib import asynccontextmanager
import hashlib
import importlib
import json
import uuid
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import OperationalError

from db.connection import Database
from db.models import (
    ProviderDirectoryCanonicalResource,
    ProviderDirectoryPractitioner,
    ProviderDirectorySourceResource,
)
from process.provider_directory_proof_store import (
    PROVIDER_DIRECTORY_PROOF_SHARD_TABLE,
    ProviderDirectoryProofStoreError,
    delete_dataset_proof_shards,
    ensure_dataset_proof_shard_table,
)


importer = importlib.import_module("process.provider_directory_fhir")


DATASET_ID = "dataset-proof"
ENDPOINT_ID = "endpoint-proof"
ROOT_RUN_ID = "root-proof"
SOURCE_IDS = ("source-a", "source-b")
SELECTED_RESOURCES = (
    "Location",
    "Organization",
    "OrganizationAffiliation",
    "Practitioner",
)
LEGACY_MIRROR_MODELS = (
    ProviderDirectoryPractitioner,
    ProviderDirectoryCanonicalResource,
    ProviderDirectorySourceResource,
)


async def _require_disposable_postgres(database: Database) -> None:
    try:
        database_name = str(
            await database.scalar("SELECT current_database();") or ""
        )
    except (OSError, OperationalError):
        pytest.skip("provider proof tests need disposable Postgres")
    if "test" not in database_name.lower():
        pytest.skip("provider proof tests need a test database")


def _resource(
    resource_type,
    resource_id,
    payload,
    *,
    dataset_id=DATASET_ID,
):
    return {
        "dataset_id": dataset_id,
        "resource_type": resource_type,
        "resource_id": resource_id,
        "payload_hash": hashlib.sha256(
            json.dumps(payload, sort_keys=True).encode()
        ).hexdigest(),
        "payload_json": payload,
    }


def _resource_rows():
    return [
        _resource(
            "Practitioner",
            "practitioner-1",
            {"npi": "100", "addresses": [{}, {}]},
        ),
        _resource(
            "Organization",
            "organization-1",
            {"npi": "100", "address_json": [{}]},
        ),
        _resource(
            "Location",
            "location-1",
            {
                "first_line": "1 Main St",
                "latitude": "41.0",
                "longitude": "-87.0",
            },
        ),
    ]


async def _create_tables(database: Database, schema: str) -> None:
    await database.status(f'CREATE SCHEMA "{schema}";')
    await database.status(
        f"""
        CREATE TABLE "{schema}".provider_directory_endpoint_dataset (
            dataset_id varchar(96) PRIMARY KEY,
            endpoint_id varchar(64) NOT NULL,
            acquisition_root_run_id varchar(64),
            status varchar(32) NOT NULL,
            is_current boolean NOT NULL DEFAULT false,
            publication_metadata_json jsonb NOT NULL DEFAULT '{{}}'::jsonb
        );
        """
    )
    await database.status(
        f"""
        CREATE TABLE "{schema}".provider_directory_dataset_resource (
            dataset_id varchar(96) NOT NULL REFERENCES
                "{schema}".provider_directory_endpoint_dataset(dataset_id),
            resource_type varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            payload_hash varchar(64) NOT NULL,
            payload_json jsonb NOT NULL,
            PRIMARY KEY (dataset_id, resource_type, resource_id)
        );
        """
    )
    await database.status(
        f"""
        INSERT INTO "{schema}".provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, acquisition_root_run_id,
            status, is_current, publication_metadata_json
        ) VALUES (
            :dataset_id, :endpoint_id, :root_run_id,
            :status, false, CAST(:metadata_json AS jsonb)
        );
        """,
        dataset_id=DATASET_ID,
        endpoint_id=ENDPOINT_ID,
        root_run_id=ROOT_RUN_ID,
        status=importer.ENDPOINT_DATASET_ACQUIRING,
        metadata_json=json.dumps({"source_ids": list(SOURCE_IDS)}),
    )
    for model in LEGACY_MIRROR_MODELS:
        await database.status(
            importer._provider_directory_artifact_scope_table_sql(
                model,
                schema,
                model.__tablename__,
            )
        )
        for statement in importer._artifact_scope_pk_sql(
            model,
            schema,
            model.__tablename__,
        ):
            await database.status(statement)
    await ensure_dataset_proof_shard_table(database, schema)


@asynccontextmanager
async def _proof_database(monkeypatch):
    schema = f"provider_directory_proof_{uuid.uuid4().hex[:12]}"
    database = Database()
    is_schema_created = False
    models_with_schema = (
        importer.ProviderDirectoryDatasetResource,
        *LEGACY_MIRROR_MODELS,
    )
    original_schema_by_model = {
        model: model.__table__.schema for model in models_with_schema
    }
    try:
        await database.connect()
        await _require_disposable_postgres(database)
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
        for model in models_with_schema:
            monkeypatch.setattr(model.__table__, "schema", schema)
        await _create_tables(database, schema)
        is_schema_created = True
        yield database, schema
    finally:
        for model, original_schema in original_schema_by_model.items():
            model.__table__.schema = original_schema
        if is_schema_created:
            await database.status(
                f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;'
            )
        await database.disconnect()


def _candidate(
    *,
    dataset_id: str = DATASET_ID,
    endpoint_id: str = ENDPOINT_ID,
    root_run_id: str = ROOT_RUN_ID,
    source_ids: tuple[str, ...] = SOURCE_IDS,
    selected_resources: tuple[str, ...] = SELECTED_RESOURCES,
):
    return importer.EndpointDatasetCandidate(
        endpoint_id=endpoint_id,
        dataset_id=dataset_id,
        acquisition_root_run_id=root_run_id,
        source_ids=source_ids,
        selected_resources=selected_resources,
        expected_resources=selected_resources,
        import_run_id=root_run_id,
        previous_dataset_id=None,
    )


async def _write_candidate_batch(
    database: Database,
    dataset_resources,
) -> None:
    async with database.acquire() as connection:
        await _write_resource_batch(connection, dataset_resources)


async def _write_resource_batch(connection, dataset_resources) -> None:
    """Write resource-family batches with their transactional proof shards."""

    resource_types = sorted(
        {
            dataset_resource["resource_type"]
            for dataset_resource in dataset_resources
        }
    )
    for resource_type in resource_types:
        await importer._upsert_dataset_resource_rows_on_connection(
            connection,
            [
                dataset_resource
                for dataset_resource in dataset_resources
                if dataset_resource["resource_type"] == resource_type
            ],
            persist_content_proof=True,
        )


async def _assert_proof_batch_rollback(
    database,
    schema: str,
    dataset_resources,
) -> None:
    """Assert resource and proof rows roll back in the same transaction."""
    with pytest.raises(RuntimeError, match="rollback-proof-batch"):
        async with database.acquire() as connection:
            await _write_resource_batch(connection, dataset_resources)
            raise RuntimeError("rollback-proof-batch")
    assert await database.scalar(
        f'SELECT count(*) FROM "{schema}".provider_directory_dataset_resource;'
    ) == 0
    assert await database.scalar(
        f'SELECT count(*) FROM "{schema}"."{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}";'
    ) == 0


async def _scan_free_candidate_proof(monkeypatch, database):
    """Return proof derived from shards and assert legacy JSON is not scanned."""
    legacy_proof = await importer._endpoint_dataset_content_proof(
        database,
        DATASET_ID,
        SELECTED_RESOURCES,
        verify_payload_hashes=True,
    )
    legacy_scan = AsyncMock(
        side_effect=AssertionError("normal candidate must not scan JSON")
    )
    monkeypatch.setattr(
        importer,
        "_endpoint_dataset_content_proof",
        legacy_scan,
    )
    async with database.acquire() as connection:
        proof = await importer._candidate_endpoint_dataset_content_proof(
            connection,
            _candidate(),
        )
    legacy_scan.assert_not_awaited()
    assert (
        proof.dataset_hash,
        proof.resource_count,
        proof.resource_hashes,
        proof.resource_counts,
    ) == (
        legacy_proof.dataset_hash,
        legacy_proof.resource_count,
        legacy_proof.resource_hashes,
        legacy_proof.resource_counts,
    )
    return proof


async def _assert_source_summary_reuses_proof(proof) -> None:
    """Assert source summary derives metrics from the supplied proof."""
    no_metric_scan = type(
        "NoMetricScan",
        (),
        {
            "first": AsyncMock(
                side_effect=AssertionError("summary must reuse proof")
            )
        },
    )()
    summary = await importer._endpoint_dataset_source_summary(
        no_metric_scan,
        _candidate(),
        proof,
        {
            importer.PROVIDER_DIRECTORY_DATASET_NETWORK_PLAN_METADATA_KEY: {
                "complete": True,
                "edge_count": 4,
            },
            importer.PROVIDER_DIRECTORY_DATASET_AFFILIATION_ORGANIZATION_METADATA_KEY: {
                "complete": True,
                "edge_count": 2,
            },
        },
    )
    no_metric_scan.first.assert_not_awaited()
    assert summary["distinct_npis"] == 1
    assert summary["address_records"] == 3
    assert summary["addressed_locations"] == 1
    assert summary["geocoded_locations"] == 1


@pytest.mark.asyncio
async def test_postgres_proof_batch_is_atomic_reusable_and_scan_free(
    monkeypatch,
):
    """Prove rollback, retry reuse, exact parity, and scan-free summary."""

    async with _proof_database(monkeypatch) as (database, schema):
        dataset_resources = _resource_rows()
        await _assert_proof_batch_rollback(
            database, schema, dataset_resources
        )
        await _write_candidate_batch(database, dataset_resources)
        await _write_candidate_batch(
            database,
            list(reversed(dataset_resources)),
        )
        assert await database.scalar(
            f'SELECT count(*) FROM "{schema}"."{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}";'
        ) == 3
        proof = await _scan_free_candidate_proof(monkeypatch, database)
        assert proof.source_metrics == {
            "address_records": 3,
            "addressed_locations": 1,
            "distinct_npis": 1,
            "geocoded_locations": 1,
        }
        await _assert_source_summary_reuses_proof(proof)
        await delete_dataset_proof_shards(database, schema, DATASET_ID)
        assert await database.scalar(
            f'SELECT count(*) FROM "{schema}"."{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}";'
        ) == 0


@pytest.mark.asyncio
async def test_postgres_normal_fhir_batch_commits_resource_with_proof(
    monkeypatch,
):
    async with _proof_database(monkeypatch) as (database, schema):
        original_upsert = importer._upsert_rows

        async def dataset_or_compatibility_upsert(model, rows, **options):
            if model is importer.ProviderDirectoryDatasetResource:
                return await original_upsert(model, rows, **options)
            return len(rows)

        monkeypatch.setattr(importer, "db", database)
        monkeypatch.setattr(
            importer,
            "_upsert_rows",
            dataset_or_compatibility_upsert,
        )
        written = await importer._upsert_resource_rows(
            importer.ProviderDirectoryPractitioner,
            [
                {
                    "source_id": "source-a",
                    "resource_id": "practitioner-normal",
                    "npi": 1234567890,
                    "addresses": [{"city": "Chicago"}],
                }
            ],
            run_id=ROOT_RUN_ID,
            track_seen=False,
            dataset_scope=importer.EndpointDatasetWriteScope(
                DATASET_ID,
                importer.DEFAULT_RESOURCE_HASH_CONTRACT,
            ),
        )

        assert written == 1
        assert await database.scalar(
            f"""
            SELECT count(*)
              FROM "{schema}".provider_directory_dataset_resource
             WHERE resource_type='Practitioner';
            """
        ) == 1
        assert await database.scalar(
            f"""
            SELECT count(*)
              FROM "{schema}"."{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}"
             WHERE resource_counts_json ? 'Practitioner';
            """
        ) == 1


@pytest.mark.asyncio
async def test_postgres_proof_tamper_fails_and_resource_reset_is_atomic(
    monkeypatch,
):
    async with _proof_database(monkeypatch) as (database, schema):
        await _write_candidate_batch(database, _resource_rows())
        await database.status(
            f"""
            UPDATE "{schema}"."{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}"
               SET payload_bytes = payload_bytes || decode('00', 'hex')
             WHERE dataset_id = :dataset_id;
            """,
            dataset_id=DATASET_ID,
        )
        async with database.acquire() as connection:
            with pytest.raises(
                ProviderDirectoryProofStoreError,
                match="proof artifact changed",
            ):
                await importer._candidate_endpoint_dataset_content_proof(
                    connection,
                    _candidate(),
                )

        monkeypatch.setattr(importer, "db", database)
        context = importer.PaginationCheckpointContext(
            canonical_api_base="https://example.test/fhir",
            source_scope_hash="scope-proof",
            source_ids=SOURCE_IDS,
            owner_run_id=ROOT_RUN_ID,
            acquisition_root_run_id=ROOT_RUN_ID,
            endpoint_id=ENDPOINT_ID,
            dataset_id=DATASET_ID,
            lineage_verified=True,
        )
        await importer._clear_checkpoint_dataset_resource_type(
            context,
            "Practitioner",
        )
        assert await database.scalar(
            f"""
            SELECT count(*)
              FROM "{schema}".provider_directory_dataset_resource
             WHERE resource_type='Practitioner';
            """
        ) == 0
        assert await database.scalar(
            f"""
            SELECT count(*)
              FROM "{schema}"."{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}"
             WHERE resource_counts_json ? 'Practitioner';
            """
        ) == 0
