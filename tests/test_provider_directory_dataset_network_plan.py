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
        "edge_count": 3,
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
        "edge_count": 3,
    }
    proof_by_field.update(overrides)
    return proof_by_field


def _candidate() -> importer.EndpointDatasetCandidate:
    return importer.EndpointDatasetCandidate(
        endpoint_id="endpoint-1",
        dataset_id="dataset-1",
        acquisition_root_run_id="root-run",
        source_ids=("source-a", "source-b"),
        selected_resources=("InsurancePlan",),
        expected_resources=("InsurancePlan",),
        import_run_id="retry-child-run",
        previous_dataset_id="dataset-old",
    )


def _artifact_dataset(source_id: str) -> importer.ProviderDirectoryArtifactDataset:
    return importer.ProviderDirectoryArtifactDataset(
        source_id=source_id,
        endpoint_id="endpoint-1",
        dataset_id="dataset-1",
        evidence_run_id="root-run",
        selected_resources=("InsurancePlan",),
        expected_resources=("InsurancePlan",),
    )


def _network_rebuild_proof():
    return importer._validated_dataset_network_plan_proof(
        _network_proof_by_field(),
        dataset_id="dataset-1",
        build_run_id="artifact-run",
        replaced_edge_count=3,
        expected_acquisition_root_run_id=(
            importer._DATASET_SERVING_RELATION_ROOT_UNSET
        ),
    )


def _affiliation_rebuild_proof():
    return importer._validated_dataset_affiliation_organization_proof(
        _affiliation_proof_by_field(),
        dataset_id="dataset-1",
        build_run_id="artifact-run",
        replaced_edge_count=2,
        expected_acquisition_root_run_id=(
            importer._DATASET_SERVING_RELATION_ROOT_UNSET
        ),
    )


def _mock_artifact_relation_rebuild(monkeypatch):
    @asynccontextmanager
    async def transaction():
        yield

    network_builder = AsyncMock(return_value=_network_rebuild_proof())
    affiliation_builder = AsyncMock(return_value=_affiliation_rebuild_proof())
    proof_recorder = AsyncMock()
    monkeypatch.setattr(importer.db, "transaction", transaction)
    monkeypatch.setattr(
        importer,
        "_lock_artifact_fence_endpoints",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_verify_provider_directory_artifact_dataset_fence",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_build_provider_directory_dataset_network_plan",
        network_builder,
    )
    monkeypatch.setattr(
        importer,
        "_build_provider_directory_dataset_affiliation_organization",
        affiliation_builder,
    )
    monkeypatch.setattr(
        importer,
        "_record_current_dataset_serving_relation_proof",
        proof_recorder,
    )
    return network_builder, affiliation_builder, proof_recorder


def _promotion_acquire(transaction_events, connection_executor):
    @asynccontextmanager
    async def acquire():
        transaction_events.append("begin")
        try:
            yield connection_executor
        except BaseException:
            transaction_events.append("rollback")
            raise
        transaction_events.append("commit")

    return acquire


def _promotion_relation_builders(transaction_events, connection_executor):
    async def build_network_plan(
        executor,
        dataset_id,
        *,
        build_run_id,
        expected_acquisition_root_run_id,
    ):
        transaction_events.append("build-edges")
        assert (executor, dataset_id) == (connection_executor, "dataset-1")
        assert (build_run_id, expected_acquisition_root_run_id) == (
            "retry-child-run",
            "root-run",
        )
        return importer._validated_dataset_network_plan_proof(
            _network_proof_by_field(),
            dataset_id=dataset_id,
            build_run_id=build_run_id,
            replaced_edge_count=0,
            expected_acquisition_root_run_id=expected_acquisition_root_run_id,
        )

    async def build_affiliations(
        executor,
        dataset_id,
        *,
        build_run_id,
        expected_acquisition_root_run_id,
    ):
        transaction_events.append("build-affiliations")
        assert (executor, dataset_id) == (connection_executor, "dataset-1")
        return importer._validated_dataset_affiliation_organization_proof(
            _affiliation_proof_by_field(),
            dataset_id=dataset_id,
            build_run_id=build_run_id,
            replaced_edge_count=0,
            expected_acquisition_root_run_id=expected_acquisition_root_run_id,
        )

    return build_network_plan, build_affiliations


def _mock_promotion_dependencies(
    monkeypatch,
    acquire,
    network_builder,
    affiliation_builder,
    supersede_dataset,
):
    monkeypatch.setattr(importer.db, "acquire", acquire)
    monkeypatch.setattr(
        importer,
        "_lock_endpoint_dataset_for_promotion",
        AsyncMock(return_value="dataset-old"),
    )
    monkeypatch.setattr(
        importer,
        "_validated_endpoint_bulk_transaction_times",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        importer,
        "_endpoint_dataset_content_hash",
        AsyncMock(return_value=("dataset-hash", 3)),
    )
    monkeypatch.setattr(
        importer,
        "_build_provider_directory_dataset_network_plan",
        network_builder,
    )
    monkeypatch.setattr(
        importer,
        "_build_provider_directory_dataset_affiliation_organization",
        affiliation_builder,
    )
    monkeypatch.setattr(
        importer,
        "_supersede_endpoint_dataset",
        supersede_dataset,
    )
    monkeypatch.setattr(importer, "_publish_endpoint_dataset", AsyncMock())
    monkeypatch.setattr(
        importer,
        "_clear_published_endpoint_partition_state",
        AsyncMock(),
    )


def test_edge_sql_is_dataset_scoped_insurance_plan_only_and_normalizes_refs():
    sql = importer._dataset_network_plan_insert_sql(
        should_verify_acquisition_root=True,
    )

    assert "resource.dataset_id" in sql
    assert "resource.resource_type = 'InsurancePlan'" in sql
    assert "PractitionerRole" not in sql
    assert "OrganizationAffiliation" not in sql
    assert "Organization/([A-Za-z0-9.-]{1,64})" in sql
    assert "~ '^[A-Za-z0-9.-]{1,64}$'" in sql
    assert "SELECT DISTINCT insurance_plan_resource_id, network_resource_id" in sql
    assert "acquisition_root_run_id IS NOT DISTINCT FROM" in sql


def test_affiliation_sql_is_dataset_scoped_and_normalizes_participating_org():
    sql = (
        importer
        ._dataset_affiliation_organization_insert_sql(
            should_verify_acquisition_root=True,
        )
    )

    assert "resource.dataset_id" in sql
    assert "resource.resource_type = 'OrganizationAffiliation'" in sql
    assert "participating_organization_ref" in sql
    assert "Organization/([A-Za-z0-9.-]{1,64})" in sql
    assert "reference_text ~ '^[A-Za-z0-9.-]{1,64}$'" in sql
    assert "InsurancePlan" not in sql
    assert "acquisition_root_run_id IS NOT DISTINCT FROM" in sql


def test_edge_proof_counts_duplicates_and_accepts_valid_zero_edges():
    duplicate_proof = importer._validated_dataset_network_plan_proof(
        _network_proof_by_field(),
        dataset_id="dataset-1",
        build_run_id="retry-child-run",
        replaced_edge_count=2,
        expected_acquisition_root_run_id="root-run",
    )
    zero_proof = importer._validated_dataset_network_plan_proof(
        _network_proof_by_field(
            insurance_plan_resource_count=4,
            insurance_plan_with_network_refs_count=0,
            zero_network_reference_plan_count=4,
            network_reference_value_count=0,
            valid_network_reference_count=0,
            expected_edge_count=0,
            edge_count=0,
        ),
        dataset_id="dataset-zero",
        build_run_id="build-zero",
        replaced_edge_count=0,
        expected_acquisition_root_run_id="root-run",
    )

    assert duplicate_proof["duplicate_network_reference_count"] == 2
    assert duplicate_proof["replaced_edge_count"] == 2
    assert zero_proof["complete"] is True
    assert zero_proof["edge_count"] == 0
    assert zero_proof["zero_network_reference_plan_count"] == 4


@pytest.mark.parametrize(
    ("overrides", "error_fragment"),
    [
        ({"invalid_network_reference_count": 1}, "invalid_references"),
        ({"malformed_network_refs_payload_count": 1}, "invalid_references"),
        ({"edge_count": 2}, "incomplete"),
    ],
)
def test_edge_proof_fails_closed(overrides, error_fragment):
    with pytest.raises(RuntimeError, match=error_fragment):
        importer._validated_dataset_network_plan_proof(
            _network_proof_by_field(**overrides),
            dataset_id="dataset-1",
            build_run_id="retry-child-run",
            replaced_edge_count=0,
            expected_acquisition_root_run_id="root-run",
        )


def test_affiliation_proof_accepts_empty_refs_and_fails_invalid_nonempty_refs():
    proof = importer._validated_dataset_affiliation_organization_proof(
        _affiliation_proof_by_field(),
        dataset_id="dataset-1",
        build_run_id="retry-child-run",
        replaced_edge_count=1,
        expected_acquisition_root_run_id="root-run",
    )

    assert proof["complete"] is True
    assert proof["empty_participating_organization_reference_count"] == 2
    assert proof["edge_count"] == 3
    with pytest.raises(RuntimeError, match="invalid_references"):
        importer._validated_dataset_affiliation_organization_proof(
            _affiliation_proof_by_field(invalid_reference_count=1),
            dataset_id="dataset-1",
            build_run_id="retry-child-run",
            replaced_edge_count=1,
            expected_acquisition_root_run_id="root-run",
        )


@pytest.mark.asyncio
async def test_artifact_rebuilds_both_relations_once_per_dataset_alias_family(
    monkeypatch,
):
    fence = importer.ProviderDirectoryArtifactDatasetFence(
        (_artifact_dataset("source-a"), _artifact_dataset("source-b"))
    )
    network_builder, affiliation_builder, proof_recorder = (
        _mock_artifact_relation_rebuild(monkeypatch)
    )

    aggregates = await importer._rebuild_current_dataset_serving_relations(
        fence,
        build_run_id="artifact-run",
        should_rebuild_network_plan=True,
        should_rebuild_affiliation_organization=True,
    )

    network_builder.assert_awaited_once_with(
        importer.db,
        "dataset-1",
        build_run_id="artifact-run",
    )
    affiliation_builder.assert_awaited_once_with(
        importer.db,
        "dataset-1",
        build_run_id="artifact-run",
    )
    assert proof_recorder.await_count == 2
    network_aggregate = aggregates["dataset_network_plan"]
    affiliation_aggregate = aggregates["dataset_affiliation_organization"]
    assert network_aggregate["dataset_count"] == 1
    assert network_aggregate["dataset_ids"] == ["dataset-1"]
    assert network_aggregate["edge_count"] == 3
    assert affiliation_aggregate["edge_count"] == 3


@pytest.mark.asyncio
async def test_publication_builds_edges_before_superseding_and_uses_root_identity(
    monkeypatch,
):
    transaction_events = []
    connection_executor = types.SimpleNamespace(status=AsyncMock())
    acquire = _promotion_acquire(transaction_events, connection_executor)
    network_builder, affiliation_builder = _promotion_relation_builders(
        transaction_events,
        connection_executor,
    )

    async def supersede(*_args):
        transaction_events.append("supersede")

    _mock_promotion_dependencies(
        monkeypatch,
        acquire,
        network_builder,
        affiliation_builder,
        supersede,
    )

    publication_summary = await importer._promote_endpoint_dataset_candidate(
        _candidate(),
        {"InsurancePlan": {"complete": True}},
    )

    assert transaction_events == [
        "begin",
        "build-edges",
        "build-affiliations",
        "supersede",
        "commit",
    ]
    assert publication_summary["dataset_network_plan"]["complete"] is True
    publish_metadata = importer._publish_endpoint_dataset.await_args.args[-1]
    assert publish_metadata["dataset_network_plan"]["build_run_id"] == (
        "retry-child-run"
    )
    assert publish_metadata["dataset_affiliation_organization"][
        "acquisition_root_run_id"
    ] == "root-run"


@pytest.mark.parametrize("failing_relation", ["network", "affiliation"])
@pytest.mark.asyncio
async def test_relation_build_failure_rolls_back_before_current_dataset_changes(
    monkeypatch,
    failing_relation,
):
    transaction_events = []
    connection_executor = types.SimpleNamespace(status=AsyncMock())
    network_builder = AsyncMock(return_value={"complete": True})
    affiliation_builder = AsyncMock(return_value={"complete": True})
    if failing_relation == "network":
        network_builder.side_effect = RuntimeError("edge build failed")
    else:
        affiliation_builder.side_effect = RuntimeError("edge build failed")
    supersede_dataset = AsyncMock()
    _mock_promotion_dependencies(
        monkeypatch,
        _promotion_acquire(transaction_events, connection_executor),
        network_builder,
        affiliation_builder,
        supersede_dataset,
    )

    with pytest.raises(RuntimeError, match="edge build failed"):
        await importer._promote_endpoint_dataset_candidate(
            _candidate(),
            {"InsurancePlan": {"complete": True}},
        )

    assert transaction_events == ["begin", "rollback"]
    supersede_dataset.assert_not_awaited()
    importer._publish_endpoint_dataset.assert_not_awaited()
