# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from contextlib import asynccontextmanager
import importlib
import types
from unittest.mock import AsyncMock

import pytest


importer = importlib.import_module("process.provider_directory_fhir")


def _network_proof_by_field(**overrides):
    proof_by_field = {
        "target_dataset_count": 1,
        "acquisition_root_run_id": "root-run",
        "insurance_plan_resource_count": 3,
        "insurance_plan_with_network_refs_count": 2,
        "zero_network_reference_plan_count": 1,
        "malformed_network_refs_payload_count": 0,
        "network_reference_value_count": 5,
        "valid_network_reference_count": 5,
        "invalid_network_reference_count": 0,
        "expected_edge_count": 3,
    }
    proof_by_field.update(overrides)
    return proof_by_field


def _affiliation_proof_by_field(**overrides):
    proof_by_field = {
        "target_dataset_count": 1,
        "acquisition_root_run_id": "root-run",
        "affiliation_resource_count": 5,
        "affiliation_with_participating_organization_count": 3,
        "empty_participating_organization_reference_count": 2,
        "malformed_reference_payload_count": 0,
        "valid_reference_count": 3,
        "invalid_reference_count": 0,
        "expected_edge_count": 3,
    }
    proof_by_field.update(overrides)
    return proof_by_field


def _artifact_dataset(
    source_id: str,
    endpoint_id: str = "endpoint-1",
    dataset_id: str = "dataset-1",
    evidence_run_id: str = "root-run",
) -> importer.ProviderDirectoryArtifactDataset:
    return importer.ProviderDirectoryArtifactDataset(
        source_id=source_id,
        endpoint_id=endpoint_id,
        dataset_id=dataset_id,
        evidence_run_id=evidence_run_id,
        selected_resources=("InsurancePlan",),
        expected_resources=("InsurancePlan",),
    )


@pytest.mark.parametrize(
    "sql_factory",
    [
        importer._dataset_network_plan_edge_insert_sql,
        importer._dataset_affiliation_organization_edge_insert_sql,
    ],
)
def test_relation_edge_insert_sql_uses_only_relation_key_columns(sql_factory):
    sql = sql_factory(should_verify_acquisition_root=True)
    insert_sql = sql.split("INSERT INTO", maxsplit=1)[1]

    assert "INSERT INTO" in sql
    assert "acquisition_root_run_id" not in insert_sql
    assert "build_run_id" not in insert_sql
    assert "built_at" not in insert_sql
    assert "RETURNING" not in insert_sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("builder", "proof_by_field"),
    [
        (
            importer._build_provider_directory_dataset_network_plan,
            _network_proof_by_field(invalid_network_reference_count=1),
        ),
        (
            importer._build_provider_directory_dataset_affiliation_organization,
            _affiliation_proof_by_field(invalid_reference_count=1),
        ),
    ],
)
async def test_invalid_relation_refs_do_not_delete_existing_edges(
    builder,
    proof_by_field,
):
    executor = types.SimpleNamespace(
        first=AsyncMock(return_value=proof_by_field),
        status=AsyncMock(),
    )

    with pytest.raises(RuntimeError, match="invalid_references"):
        await builder(
            executor,
            "dataset-1",
            build_run_id="build-run",
            expected_acquisition_root_run_id="root-run",
        )

    executor.status.assert_not_awaited()


def test_artifact_fence_rejects_evidence_run_drift():
    fence = importer.ProviderDirectoryArtifactDatasetFence(
        (_artifact_dataset("source-a"),)
    )
    drifted_dataset_by_field = {
        "source_id": "source-a",
        "endpoint_id": "endpoint-1",
        "dataset_id": "dataset-1",
        "evidence_run_id": "changed-root-run",
        "selected_resources": ["InsurancePlan"],
        "recorded_expected_resources": ["InsurancePlan"],
    }

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="evidence_run_changed",
    ):
        importer._assert_locked_artifact_fence_tuples(
            fence,
            [drifted_dataset_by_field],
        )


class _TransactionRecorder:
    def __init__(self):
        self.events = []

    @asynccontextmanager
    async def transaction(self):
        self.events.append("begin")
        yield
        self.events.append("commit")


async def _network_builder(_executor, dataset_id, **options):
    evidence_run_id = options["expected_acquisition_root_run_id"]
    return importer._validated_dataset_network_plan_proof(
        _network_proof_by_field(acquisition_root_run_id=evidence_run_id),
        dataset_id=dataset_id,
        build_run_id=options["build_run_id"],
        replaced_edge_count=0,
        inserted_edge_count=3,
        expected_acquisition_root_run_id=evidence_run_id,
    )


def _two_dataset_fence():
    datasets = (
        _artifact_dataset("source-a"),
        _artifact_dataset(
            "source-b",
            endpoint_id="endpoint-2",
            dataset_id="dataset-2",
            evidence_run_id="root-run-2",
        ),
    )
    return datasets, importer.ProviderDirectoryArtifactDatasetFence(datasets)


def _install_relation_rebuild_mocks(monkeypatch, transaction_recorder):
    fence_lock = AsyncMock()
    relation_lock = AsyncMock()
    monkeypatch.setattr(
        importer.db,
        "transaction",
        transaction_recorder.transaction,
    )
    monkeypatch.setattr(
        importer,
        "_lock_and_verify_artifact_dataset_fence",
        fence_lock,
    )
    monkeypatch.setattr(
        importer,
        "_lock_dataset_serving_relation_build",
        relation_lock,
    )
    monkeypatch.setattr(
        importer,
        "_build_provider_directory_dataset_network_plan",
        _network_builder,
    )
    monkeypatch.setattr(
        importer,
        "_record_current_dataset_serving_relation_proof",
        AsyncMock(),
    )
    return fence_lock, relation_lock


@pytest.mark.asyncio
async def test_artifact_rebuild_uses_one_transaction_and_lock_per_dataset(
    monkeypatch,
):
    datasets, fence = _two_dataset_fence()
    transaction_recorder = _TransactionRecorder()
    fence_lock, relation_lock = _install_relation_rebuild_mocks(
        monkeypatch,
        transaction_recorder,
    )

    aggregates = await importer._rebuild_current_dataset_serving_relations(
        fence,
        build_run_id="artifact-run",
        should_rebuild_network_plan=True,
        should_rebuild_affiliation_organization=False,
    )

    assert transaction_recorder.events == ["begin", "commit", "begin", "commit"]
    assert [call.args[1] for call in relation_lock.await_args_list] == [
        "dataset-1",
        "dataset-2",
    ]
    assert [call.args[0].datasets for call in fence_lock.await_args_list] == [
        (datasets[0],),
        (datasets[1],),
    ]
    assert aggregates["dataset_network_plan"]["dataset_ids"] == [
        "dataset-1",
        "dataset-2",
    ]
