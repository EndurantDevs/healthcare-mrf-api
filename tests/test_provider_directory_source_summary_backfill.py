# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import hashlib
import importlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.test_provider_directory_uhc_source_summary import _uhc_summary


importer = importlib.import_module("process.provider_directory_fhir")


DATASET_HASH = "a" * 64
RESOURCE_HASHES = {
    "InsurancePlan": "b" * 64,
    "OrganizationAffiliation": "c" * 64,
}
RESOURCE_COUNTS = {
    "InsurancePlan": 1,
    "OrganizationAffiliation": 1,
}


def _dataset() -> importer.ProviderDirectoryArtifactDataset:
    resources = tuple(sorted(RESOURCE_COUNTS))
    return importer.ProviderDirectoryArtifactDataset(
        source_id="source-a",
        endpoint_id="endpoint-1",
        dataset_id="dataset-1",
        evidence_run_id="root-1",
        selected_resources=resources,
        expected_resources=resources,
        dataset_hash=DATASET_HASH,
        resource_count=2,
    )


def _candidate() -> importer.EndpointDatasetCandidate:
    dataset = _dataset()
    return importer.EndpointDatasetCandidate(
        endpoint_id=dataset.endpoint_id,
        dataset_id=dataset.dataset_id,
        acquisition_root_run_id=dataset.evidence_run_id,
        source_ids=("source-a", "source-b"),
        selected_resources=dataset.selected_resources,
        import_run_id=dataset.evidence_run_id,
        previous_dataset_id=None,
        expected_resources=dataset.expected_resources,
    )


def _content_proof(
    *,
    dataset_hash: str = DATASET_HASH,
) -> importer.EndpointDatasetContentProof:
    return importer.EndpointDatasetContentProof(
        dataset_hash=dataset_hash,
        resource_count=2,
        resource_hashes=RESOURCE_HASHES,
        resource_counts=RESOURCE_COUNTS,
    )


def _relation_proofs() -> dict[str, dict[str, object]]:
    network_count_fields = (
        "insurance_plan_resource_count",
        "insurance_plan_with_network_refs_count",
        "zero_network_reference_plan_count",
        "malformed_network_refs_payload_count",
        "network_reference_value_count",
        "valid_network_reference_count",
        "invalid_network_reference_count",
        "expected_edge_count",
        "edge_count",
        "duplicate_network_reference_count",
        "replaced_edge_count",
        "plan_projection_count",
        "replaced_plan_projection_count",
        "inserted_plan_projection_count",
    )
    affiliation_count_fields = (
        "affiliation_resource_count",
        "affiliation_with_participating_organization_count",
        "empty_participating_organization_reference_count",
        "malformed_reference_payload_count",
        "valid_reference_count",
        "invalid_reference_count",
        "expected_edge_count",
        "edge_count",
        "replaced_edge_count",
    )
    return {
        importer.PROVIDER_DIRECTORY_DATASET_NETWORK_PLAN_METADATA_KEY: {
            "complete": True,
            "dataset_id": "dataset-1",
            **dict.fromkeys(network_count_fields, 0),
        },
        importer.PROVIDER_DIRECTORY_DATASET_AFFILIATION_ORGANIZATION_METADATA_KEY: {
            "complete": True,
            "dataset_id": "dataset-1",
            **dict.fromkeys(affiliation_count_fields, 0),
        },
    }


def _source_summary() -> dict[str, object]:
    return importer._build_endpoint_dataset_source_summary(
        _candidate(),
        _content_proof(),
        {
            "individual_practitioners": 0,
            "organization_resources": 0,
            "practitioner_role_resources": 0,
            "network_plan_links": 0,
            "organization_affiliation_links": 0,
            "distinct_npis": 0,
            "address_records": 0,
            "addressed_locations": 0,
            "geocoded_locations": 0,
        },
        "root-1",
    )


def _publication_metadata_with_summary() -> dict[str, object]:
    candidate = _candidate()
    content_proof = _content_proof()
    return {
        "source_ids": list(candidate.source_ids),
        importer.PROVIDER_DIRECTORY_OUTCOME_RESOURCE_COUNTS_METADATA_KEY: (
            importer._outcome_resource_count_proof(candidate, content_proof)
        ),
        importer.SOURCE_SUMMARY_METADATA_KEY: _source_summary(),
    }


@pytest.mark.asyncio
async def test_targeted_backfill_uses_full_locked_dataset_source_ids():
    executor = SimpleNamespace(
        first=AsyncMock(
            return_value={
                "acquisition_root_run_id": "root-1",
                "publication_metadata_json": {
                    "source_ids": ["source-b", "source-a"],
                },
            }
        )
    )

    candidate, publication_metadata = (
        await importer._current_artifact_dataset_source_summary_candidate(
            executor,
            _dataset(),
            ("source-a",),
        )
    )

    assert candidate.source_ids == ("source-a", "source-b")
    assert publication_metadata["source_ids"] == ["source-b", "source-a"]
    assert "superseded_at IS NULL" in executor.first.await_args.args[0]


@pytest.mark.parametrize(
    "raw_source_ids",
    (
        None,
        "source-a",
        [],
        ["source-a", "source-a"],
        ["source-b"],
        ["source-a", " "],
        [" source-a "],
        [1],
    ),
)
def test_backfill_rejects_invalid_full_dataset_source_ids(raw_source_ids):
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="source_summary_source_ids_invalid",
    ):
        importer._artifact_dataset_source_summary_source_ids(
            {"source_ids": raw_source_ids},
            _dataset(),
            ("source-a",),
        )


@pytest.mark.asyncio
async def test_backfill_rejects_legacy_import_run_without_acquisition_root():
    executor = SimpleNamespace(
        first=AsyncMock(
            return_value={
                "acquisition_root_run_id": None,
                "publication_metadata_json": {"source_ids": ["source-a"]},
            }
        )
    )

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="source_summary_lineage_missing",
    ):
        await importer._current_artifact_dataset_source_summary_candidate(
            executor,
            _dataset(),
            ("source-a",),
        )


def test_backfill_verifies_payload_json_against_stored_hash():
    payload_by_field = {"name": "Example", "npi": "1215387113"}
    payload_hash = hashlib.sha256(
        json.dumps(payload_by_field, sort_keys=True).encode("utf-8")
    ).hexdigest()

    importer._assert_endpoint_dataset_resource_payload_hash(
        {"payload_json": payload_by_field, "payload_hash": payload_hash}
    )
    importer._assert_endpoint_dataset_resource_payload_hash(
        {
            "payload_json": json.dumps(payload_by_field),
            "payload_hash": payload_hash,
        }
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="payload_hash_mismatch",
    ):
        importer._assert_endpoint_dataset_resource_payload_hash(
            {"payload_json": payload_by_field, "payload_hash": "f" * 64}
        )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="payload_invalid",
    ):
        importer._assert_endpoint_dataset_resource_payload_hash(
            {"payload_json": [], "payload_hash": payload_hash}
        )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="payload_invalid",
    ):
        importer._assert_endpoint_dataset_resource_payload_hash(
            {"payload_json": "{invalid", "payload_hash": payload_hash}
        )


@pytest.mark.asyncio
async def test_backfill_content_proof_requests_and_verifies_payload_json():
    payload_by_field = {"resource_id": "plan-1"}
    payload_hash = hashlib.sha256(
        json.dumps(payload_by_field, sort_keys=True).encode("utf-8")
    ).hexdigest()

    class ContentConnection:
        def __init__(self):
            self.statements = []

        async def all(self, statement, **_params):
            self.statements.append(statement)
            return (
                [
                    {
                        "resource_type": "InsurancePlan",
                        "resource_id": "plan-1",
                        "payload_hash": payload_hash,
                        "payload_json": payload_by_field,
                    }
                ]
                if len(self.statements) == 1
                else []
            )

    connection = ContentConnection()
    content_proof = await importer._endpoint_dataset_content_proof(
        connection,
        "dataset-1",
        ("InsurancePlan",),
        verify_payload_hashes=True,
    )

    assert content_proof.resource_count == 1
    assert "payload_json" in connection.statements[0]


def test_backfill_content_proof_must_match_locked_dataset():
    importer._assert_artifact_dataset_content_proof(
        _dataset(),
        _content_proof(),
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="dataset_content_changed",
    ):
        importer._assert_artifact_dataset_content_proof(
            _dataset(),
            _content_proof(dataset_hash="f" * 64),
        )


def test_existing_summary_reuse_requires_exact_outcome_and_relations():
    metadata = _publication_metadata_with_summary()
    relation_proof_by_name = _relation_proofs()
    assert importer._existing_artifact_dataset_source_summary(
        metadata,
        _dataset(),
        _candidate(),
        relation_proof_by_name,
    ) == _source_summary()

    invalid_outcome_metadata_map = dict(metadata)
    invalid_outcome_metadata_map[
        importer.PROVIDER_DIRECTORY_OUTCOME_RESOURCE_COUNTS_METADATA_KEY
    ] = {
        **metadata[
            importer.PROVIDER_DIRECTORY_OUTCOME_RESOURCE_COUNTS_METADATA_KEY
        ],
        "version": True,
    }
    assert importer._existing_artifact_dataset_source_summary(
        invalid_outcome_metadata_map,
        _dataset(),
        _candidate(),
        relation_proof_by_name,
    ) is None

    mismatched_relation_proof_by_name = _relation_proofs()
    mismatched_relation_proof_by_name[
        importer.PROVIDER_DIRECTORY_DATASET_NETWORK_PLAN_METADATA_KEY
    ]["edge_count"] = 1
    assert importer._existing_artifact_dataset_source_summary(
        metadata,
        _dataset(),
        _candidate(),
        mismatched_relation_proof_by_name,
    ) is None


def _uhc_dataset_context():
    source_summary_map = _uhc_summary()
    content_proof = importer.EndpointDatasetContentProof(
        dataset_hash=source_summary_map["dataset_hash"],
        resource_count=source_summary_map["total_resources"],
        resource_hashes=source_summary_map["resource_hashes"],
        resource_counts=source_summary_map["resource_counts"],
    )
    dataset = importer.ProviderDirectoryArtifactDataset(
        source_id=source_summary_map["source_ids"][0],
        endpoint_id=source_summary_map["endpoint_id"],
        dataset_id=source_summary_map["dataset_id"],
        evidence_run_id=source_summary_map["acquisition_root_run_id"],
        selected_resources=tuple(source_summary_map["selected_resources"]),
        expected_resources=tuple(source_summary_map["selected_resources"]),
        dataset_hash=source_summary_map["dataset_hash"],
        resource_count=source_summary_map["total_resources"],
    )
    candidate = importer.EndpointDatasetCandidate(
        endpoint_id=dataset.endpoint_id,
        dataset_id=dataset.dataset_id,
        acquisition_root_run_id=dataset.evidence_run_id,
        source_ids=tuple(source_summary_map["source_ids"]),
        selected_resources=dataset.selected_resources,
        import_run_id=dataset.evidence_run_id,
        previous_dataset_id=None,
        expected_resources=dataset.expected_resources,
    )
    publication_metadata_map = {
        "source_ids": list(candidate.source_ids),
        importer.PROVIDER_DIRECTORY_OUTCOME_RESOURCE_COUNTS_METADATA_KEY: (
            importer._outcome_resource_count_proof(candidate, content_proof)
        ),
        importer.SOURCE_SUMMARY_METADATA_KEY: source_summary_map,
    }
    return dataset, candidate, publication_metadata_map, source_summary_map


def test_relation_refresh_preserves_exact_uhc_semantic_summary():
    dataset, candidate, publication_metadata_map, source_summary_map = (
        _uhc_dataset_context()
    )

    assert importer._existing_artifact_dataset_source_summary(
        publication_metadata_map,
        dataset,
        candidate,
        {},
    ) == source_summary_map


def test_invalid_uhc_summary_or_outcome_fails_instead_of_retyping_as_fhir():
    dataset, candidate, publication_metadata_map, _source_summary_map = (
        _uhc_dataset_context()
    )
    publication_metadata_map[
        importer.PROVIDER_DIRECTORY_OUTCOME_RESOURCE_COUNTS_METADATA_KEY
    ]["version"] = True

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="uhc_source_summary_outcome_invalid",
    ):
        importer._existing_artifact_dataset_source_summary(
            publication_metadata_map,
            dataset,
            candidate,
            {},
        )
