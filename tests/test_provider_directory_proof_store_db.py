# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import importlib
import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from db.connection import Database
from process.provider_directory_generic_source_adapters import candidate_resource_count, completed_source_scope
from process.provider_directory_generic_source_policy import source_transaction
from process.provider_directory_proof_store import (
    PROVIDER_DIRECTORY_PROOF_SHARD_TABLE,
    ProviderDirectoryProofStoreError,
    delete_dataset_proof_shards,
)
from tests.provider_directory_source_postgres_support import source_component_database

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
        "payload_hash": hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest(),
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


@asynccontextmanager
async def _proof_database(monkeypatch):
    """Use real operator provisioning and the original Source Publisher/Owner."""
    async with source_component_database(monkeypatch) as component:
        database, schema = component.database, component.policy.schema
        monkeypatch.setattr(importer, "db", database)
        async with source_transaction(database):
            await database.status(
                f'INSERT INTO "{schema}".provider_directory_api_endpoint '
                "(endpoint_id,canonical_api_base,credential_descriptor_hash,endpoint_signature_hash) "
                "VALUES (:endpoint_id,'https://example.test/fhir',repeat('c',64),repeat('d',64))",
                endpoint_id=ENDPOINT_ID,
            )
            for source_id in SOURCE_IDS:
                await database.status(
                    f'INSERT INTO "{schema}".provider_directory_source '
                    "(source_id,org_name,endpoint_id,canonical_api_base,requires_registration,requires_api_key) "
                    "VALUES (:source_id,'Proof fixture',:endpoint_id,'https://example.test/fhir',false,false)",
                    source_id=source_id,
                    endpoint_id=ENDPOINT_ID,
                )
            await importer._ensure_endpoint_dataset_candidate(_candidate())
        yield database, schema


def _checkpoint_context():
    return importer.PaginationCheckpointContext(
        canonical_api_base="https://example.test/fhir",
        source_scope_hash="scope-proof",
        source_ids=SOURCE_IDS,
        owner_run_id=ROOT_RUN_ID,
        acquisition_root_run_id=ROOT_RUN_ID,
        endpoint_id=ENDPOINT_ID,
        dataset_id=DATASET_ID,
        lineage_verified=True,
    )


async def _active_resource_count(database, resource_type):
    assert database is importer.db
    return await candidate_resource_count(importer, _checkpoint_context(), resource_type)


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
        resource_hash_contract=importer.LEGACY_RESOURCE_HASH_CONTRACT,
    )


async def _write_candidate_batch(
    database: Database,
    dataset_resources,
) -> None:
    async with source_transaction(database), database.acquire() as connection:
        await _write_resource_batch(connection, dataset_resources)


async def _write_resource_batch(connection, dataset_resources) -> None:
    """Write resource-family batches with their transactional proof shards."""

    resource_types = sorted({dataset_resource["resource_type"] for dataset_resource in dataset_resources})
    for resource_type in resource_types:
        await importer._upsert_dataset_resource_rows_on_connection(
            connection,
            [
                dataset_resource
                for dataset_resource in dataset_resources
                if dataset_resource["resource_type"] == resource_type
            ],
            persist_content_proof=True,
            resource_hash_contract=importer.LEGACY_RESOURCE_HASH_CONTRACT,
        )


async def _assert_proof_batch_rollback(
    database,
    schema: str,
    dataset_resources,
) -> None:
    """Assert resource and proof rows roll back in the same transaction."""
    with pytest.raises(RuntimeError, match="rollback-proof-batch"):
        async with source_transaction(database), database.acquire() as connection:
            await _write_resource_batch(connection, dataset_resources)
            raise RuntimeError("rollback-proof-batch")
    assert await database.scalar(f'SELECT count(*) FROM "{schema}".provider_directory_dataset_resource;') == 0
    assert await database.scalar(f'SELECT count(*) FROM "{schema}"."{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}";') == 0
    for resource_type in ("Practitioner", "Organization", "Location"):
        assert await _active_resource_count(database, resource_type) == 0


async def _scan_free_candidate_proof(monkeypatch, database):
    """Return proof derived from shards and assert legacy JSON is not scanned."""
    async with (
        source_transaction(database, isolation="repeatable read"),
        completed_source_scope(importer, _candidate()),
    ):
        legacy_proof = await importer._endpoint_dataset_content_proof(
            database,
            DATASET_ID,
            SELECTED_RESOURCES,
            verify_payload_hashes=True,
        )
        legacy_scan = AsyncMock(side_effect=AssertionError("normal candidate must not scan JSON"))
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
        {"first": AsyncMock(side_effect=AssertionError("summary must reuse proof"))},
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
        await _assert_proof_batch_rollback(database, schema, dataset_resources)
        await _write_candidate_batch(database, dataset_resources)
        await _write_candidate_batch(
            database,
            list(reversed(dataset_resources)),
        )
        assert await database.scalar(f'SELECT count(*) FROM "{schema}"."{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}";') == 3
        proof = await _scan_free_candidate_proof(monkeypatch, database)
        assert proof.source_metrics == {
            "address_records": 3,
            "addressed_locations": 1,
            "distinct_npis": 1,
            "geocoded_locations": 1,
        }
        await _assert_source_summary_reuses_proof(proof)
        async with source_transaction(database):
            await delete_dataset_proof_shards(database, schema, DATASET_ID)
        assert await database.scalar(f'SELECT count(*) FROM "{schema}"."{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}";') == 0


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
            options=importer.ProviderDirectoryResourceWriteOptions(
                run_id=ROOT_RUN_ID,
                track_seen=False,
                dataset_scope=importer.EndpointDatasetWriteScope(
                    DATASET_ID,
                    importer.LEGACY_RESOURCE_HASH_CONTRACT,
                ),
            ),
        )

        assert written == 1
        assert await _active_resource_count(database, "Practitioner") == 1
        assert (
            await database.scalar(
                f"""
            SELECT count(*)
              FROM "{schema}"."{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}"
             WHERE resource_counts_json ? 'Practitioner';
            """
            )
            == 1
        )


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
        async with source_transaction(database), database.acquire() as connection:
            with pytest.raises(
                ProviderDirectoryProofStoreError,
                match="proof artifact changed",
            ):
                await importer._candidate_endpoint_dataset_content_proof(
                    connection,
                    _candidate(),
                )

        monkeypatch.setattr(importer, "db", database)
        context = _checkpoint_context()
        await importer._clear_checkpoint_dataset_resource_type(
            context,
            "Practitioner",
        )
        assert (
            await database.scalar(
                f"""
            SELECT count(*)
              FROM "{schema}".provider_directory_dataset_resource
             WHERE resource_type='Practitioner';
            """
            )
            == 0
        )
        assert await _active_resource_count(database, "Practitioner") == 0
        assert (
            await database.scalar(
                f"""
            SELECT count(*)
              FROM "{schema}"."{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}"
             WHERE resource_counts_json ? 'Practitioner';
            """
            )
            == 0
        )
