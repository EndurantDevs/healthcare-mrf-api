# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import hashlib
import json
from unittest.mock import AsyncMock

import pytest

from db.connection import Database
from tests.test_provider_directory_dataset_serving_relations_db import (
    _dataset_a_relation_rows,
    _dataset_database,
    importer,
)


SELECTED_RESOURCES = (
    "InsurancePlan",
    "Organization",
    "OrganizationAffiliation",
)


async def _prepare_summary_rollback_fixture(
    database: Database,
    schema: str,
) -> tuple[importer.ProviderDirectoryArtifactDataset, dict[str, object]]:
    """Install stale identity plus sentinel relations for rollback proof."""
    content_proof = await importer._endpoint_dataset_content_proof(
        database,
        "dataset-a",
        SELECTED_RESOURCES,
        verify_payload_hashes=True,
    )
    original_metadata_map = {
        "source_ids": ["source-a"],
        "selected_resources": list(SELECTED_RESOURCES),
    }
    await database.status(
        f"UPDATE {schema}.provider_directory_endpoint_dataset "
        "SET dataset_hash = :dataset_hash, resource_count = :resource_count, "
        "publication_metadata_json = CAST(:metadata_json AS jsonb) "
        "WHERE dataset_id = 'dataset-a';",
        dataset_hash="f" * 64,
        resource_count=content_proof.resource_count,
        metadata_json=json.dumps(original_metadata_map, sort_keys=True),
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_dataset_insurance_plan "
        "VALUES ('dataset-a', 'rollback-plan', repeat('0', 64), '{}'::jsonb);"
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_dataset_network_plan "
        "VALUES ('dataset-a', 'rollback-network', 'rollback-plan');"
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_dataset_affiliation_organization "
        "VALUES ('dataset-a', 'rollback-org', 'rollback-affiliation');"
    )
    return (
        importer.ProviderDirectoryArtifactDataset(
            source_id="source-a",
            endpoint_id="endpoint-a",
            dataset_id="dataset-a",
            evidence_run_id="root-a",
            selected_resources=SELECTED_RESOURCES,
            expected_resources=SELECTED_RESOURCES,
            dataset_hash="f" * 64,
            resource_count=content_proof.resource_count,
        ),
        original_metadata_map,
    )


class _ProofRecorder:
    def __init__(self):
        self.metadata_keys = []
        self.original_recorder = (
            importer._record_current_dataset_publication_proof
        )

    async def __call__(self, dataset, metadata_key, proof):
        self.metadata_keys.append(metadata_key)
        await self.original_recorder(dataset, metadata_key, proof)


async def _assert_rollback_state(
    database: Database,
    schema: str,
    original_metadata_map: dict[str, object],
) -> None:
    metadata_is_original = await database.scalar(
        f"SELECT publication_metadata_json = CAST(:metadata_json AS jsonb) "
        f"FROM {schema}.provider_directory_endpoint_dataset "
        "WHERE dataset_id = 'dataset-a';",
        metadata_json=json.dumps(original_metadata_map, sort_keys=True),
    )
    network_rows, affiliation_rows = await _dataset_a_relation_rows(
        database,
        schema,
    )
    assert metadata_is_original is True
    assert [tuple(relation_row) for relation_row in network_rows] == [
        ("rollback-network", "rollback-plan")
    ]
    assert [tuple(relation_row) for relation_row in affiliation_rows] == [
        ("rollback-org", "rollback-affiliation")
    ]
    assert await database.scalar(
        f"SELECT count(*) FROM {schema}.provider_directory_dataset_insurance_plan "
        "WHERE dataset_id = 'dataset-a' AND resource_id = 'rollback-plan';"
    ) == 1


@pytest.mark.asyncio
async def test_real_postgres_rolls_back_relations_and_proofs_on_content_mismatch(
    monkeypatch,
):
    """Prove relation and metadata writes share the summary transaction."""
    async with _dataset_database(monkeypatch) as (database, schema):
        dataset, original_metadata_map = await _prepare_summary_rollback_fixture(
            database,
            schema,
        )
        proof_recorder = _ProofRecorder()
        monkeypatch.setattr(importer, "db", database)
        monkeypatch.setattr(
            importer,
            "_lock_and_verify_artifact_dataset_fence",
            AsyncMock(),
        )
        monkeypatch.setattr(
            importer,
            "_record_current_dataset_publication_proof",
            proof_recorder,
        )
        with pytest.raises(
            importer.ProviderDirectoryArtifactBuildStale,
            match="dataset_content_changed",
        ):
            await importer._rebuild_one_current_dataset_artifacts(
                dataset,
                importer.ProviderDirectoryArtifactDatasetFence((dataset,)),
                build_run_id="rollback-build",
                should_rebuild_network_plan=True,
                should_rebuild_affiliation_organization=True,
                should_rebuild_source_summary=True,
            )

        assert proof_recorder.metadata_keys == [
            importer.PROVIDER_DIRECTORY_DATASET_NETWORK_PLAN_METADATA_KEY,
            importer.PROVIDER_DIRECTORY_DATASET_AFFILIATION_ORGANIZATION_METADATA_KEY,
        ]
        await _assert_rollback_state(database, schema, original_metadata_map)


class _SnapshotMutation:
    def __init__(self, schema: str):
        self.schema = schema
        self.writer_database = Database()
        self.original_page_reader = importer._endpoint_dataset_content_page
        self.is_committed = False

    async def read_page(self, *args, **kwargs):
        page_rows = await self.original_page_reader(*args, **kwargs)
        if page_rows and not self.is_committed:
            await self._mutate_later_resource()
            self.is_committed = True
        return page_rows

    async def _mutate_later_resource(self):
        mutated_payload = json.dumps({"name": "mutated"}, sort_keys=True)
        await self.writer_database.status(
            f"UPDATE {self.schema}.provider_directory_dataset_resource "
            "SET payload_json = CAST(:payload_json AS jsonb), "
            "payload_hash = :payload_hash "
            "WHERE dataset_id = 'dataset-a' "
            "AND resource_type = 'Organization' "
            "AND resource_id = 'org-b';",
            payload_json=mutated_payload,
            payload_hash=hashlib.sha256(mutated_payload.encode()).hexdigest(),
        )


async def _repeatable_snapshot_proof(database: Database):
    async with database.transaction():
        await database.status(
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;"
        )
        return await importer._endpoint_dataset_content_proof(
            database,
            "dataset-a",
            SELECTED_RESOURCES,
            verify_payload_hashes=True,
        )


@pytest.mark.asyncio
async def test_real_postgres_content_proof_uses_one_repeatable_snapshot(
    monkeypatch,
):
    """Prove a paged content hash never mixes concurrent row versions."""
    async with _dataset_database(monkeypatch) as (database, _schema):
        original_proof = await importer._endpoint_dataset_content_proof(
            database,
            "dataset-a",
            SELECTED_RESOURCES,
            verify_payload_hashes=True,
        )
        mutation = _SnapshotMutation(_schema)
        monkeypatch.setattr(importer, "ENDPOINT_DATASET_HASH_BATCH_SIZE", 1)
        monkeypatch.setattr(
            importer,
            "_endpoint_dataset_content_page",
            mutation.read_page,
        )
        try:
            snapshot_proof = await _repeatable_snapshot_proof(database)
        finally:
            await mutation.writer_database.disconnect()

        assert snapshot_proof == original_proof
        fresh_proof = await importer._endpoint_dataset_content_proof(
            database,
            "dataset-a",
            SELECTED_RESOURCES,
            verify_payload_hashes=True,
        )
        assert fresh_proof.dataset_hash != original_proof.dataset_hash
