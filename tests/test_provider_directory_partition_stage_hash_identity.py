# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
from unittest.mock import Mock

import pytest


importer = importlib.import_module("process.provider_directory_fhir")


def _context() -> importer.PaginationCheckpointContext:
    return importer.PaginationCheckpointContext(
        canonical_api_base="https://example.test/fhir",
        source_scope_hash="scope-1",
        source_ids=("source-1",),
        owner_run_id="run-1",
        acquisition_root_run_id="root-1",
        endpoint_id="endpoint-1",
        dataset_id="dataset-1",
        lineage_verified=True,
    )


def _stage_source(resource_hash_contract, semantic_projection_as_of=None):
    source_by_field = {
        "source_id": "source-1",
        "_resource_hash_contract": resource_hash_contract,
    }
    if semantic_projection_as_of is not None:
        source_by_field["_semantic_projection_as_of"] = semantic_projection_as_of
    return source_by_field


@pytest.mark.asyncio
async def test_partition_stage_binds_v3_hash_identity(monkeypatch):
    acquisitions = []

    def fake_parse(_source_id, _resource, *, acquisition, run_id):
        acquisitions.append(acquisition)
        return importer.ProviderDirectoryPractitioner, {"resource_id": "p1"}

    endpoint_rows = Mock(
        return_value=[{"resource_id": "p1", "payload_hash": "payload-1"}]
    )
    monkeypatch.setattr(importer, "parse_fhir_resource", fake_parse)
    monkeypatch.setattr(importer, "_endpoint_dataset_resource_rows", endpoint_rows)

    staged = await importer._stage_last_updated_partition_window(
        _context(),
        _stage_source(
            importer.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
            "2026-08-09",
        ),
        "Practitioner",
        importer.ProviderDirectoryPractitioner,
        ({"resourceType": "Practitioner", "id": "p1"},),
        importer.LastUpdatedPartitionStageOptions(
            run_id="run-1",
            fetch_url="https://example.test/fhir/Practitioner",
        ),
    )

    assert acquisitions[0].semantic_projection_as_of.isoformat() == "2026-08-09"
    assert endpoint_rows.call_args.kwargs == {
        "dataset_id": "dataset-1",
        "resource_hash_contract": importer.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    }
    assert staged.resource_hash_contract == importer.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
    assert staged.semantic_projection_as_of == "2026-08-09"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("source_by_field", "error"),
    (
        ({"source_id": "source-1"}, "resource_hash_contract_required"),
        (
            _stage_source(importer.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT),
            "semantic_projection_as_of_missing",
        ),
        (
            _stage_source(
                importer.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
                "2026-8-9",
            ),
            "semantic_projection_as_of_invalid",
        ),
    ),
)
async def test_partition_stage_rejects_invalid_hash_identity(
    source_by_field,
    error,
):
    with pytest.raises(RuntimeError, match=error):
        await importer._stage_last_updated_partition_window(
            _context(),
            source_by_field,
            "Practitioner",
            object,
            (),
            importer.LastUpdatedPartitionStageOptions(
                run_id="run-1",
                fetch_url="https://example.test/fhir/Practitioner",
            ),
        )
