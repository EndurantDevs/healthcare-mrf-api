# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json

import pytest

from tests.test_provider_directory_artifact_cutover import (
    _create_artifact_bundle_relations,
    importer,
)
from tests.test_provider_directory_dataset_artifact_db import _dataset_database
from tests.test_provider_directory_two_phase_publication import (
    _active_dataset_fence,
    _artifact_bundle_stages,
)


async def _insert_graphql_candidate(database, schema: str) -> None:
    candidate_metadata = json.dumps(
        {
            "acquisition_root_run_id": "root-graphql-candidate",
            "selected_resources": ["Location"],
            "expected_resources": ["Location"],
            "source_ids": ["source_primary"],
            "resource_diagnostics": {
                "Location": {
                    "complete": True,
                    "bounded": False,
                    "error": None,
                    "next_url_remaining": False,
                }
            },
        }
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_api_endpoint (endpoint_id) "
        "VALUES ('endpoint_graphql');"
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_endpoint_dataset ("
        "dataset_id, endpoint_id, import_run_id, acquisition_root_run_id, "
        "previous_dataset_id, dataset_hash, status, is_current, "
        "resource_count, validated_at, publication_metadata_json"
        ") VALUES ("
        "'dataset_graphql_candidate', 'endpoint_graphql', "
        "'run-graphql-candidate', 'root-graphql-candidate', NULL, "
        ":dataset_hash, :validated_status, false, 1, now(), "
        "CAST(:metadata AS json));",
        dataset_hash="4" * 64,
        validated_status=importer.ENDPOINT_DATASET_VALIDATED,
        metadata=candidate_metadata,
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_dataset_resource ("
        "dataset_id, resource_type, resource_id, payload_hash, payload_json"
        ") VALUES ("
        "'dataset_graphql_candidate', 'Location', 'location-graphql', "
        ":payload_hash, CAST(:payload AS json));",
        payload_hash="5" * 64,
        payload=json.dumps(
            {
                "status": "active",
                "name": "GraphQL Candidate Clinic",
                "first_line": "4 Contract Way",
                "city_name": "Montgomery",
                "state_code": "AL",
                "postal_code": "36104",
            }
        ),
    )


async def _resolvable_source_dataset_id(database, schema: str) -> str | None:
    return await database.scalar(
        f"""
        SELECT dataset.dataset_id
          FROM {schema}.provider_directory_source AS source
          JOIN {schema}.provider_directory_endpoint_dataset AS dataset
            ON dataset.endpoint_id = source.endpoint_id
           AND dataset.status = :published_status
           AND dataset.is_current = true
         WHERE source.source_id = 'source_primary';
        """,
        published_status=importer.ENDPOINT_DATASET_PUBLISHED,
    )


async def _assert_failed_cutover_state(database, schema: str) -> None:
    assert await _resolvable_source_dataset_id(database, schema) == "dataset_shared"
    assert await database.scalar(
        f"SELECT endpoint_id FROM {schema}.provider_directory_source "
        "WHERE source_id = 'source_primary';"
    ) == "endpoint_shared"
    assert await database.scalar(
        f"SELECT status FROM {schema}.provider_directory_endpoint_dataset "
        "WHERE dataset_id = 'dataset_graphql_candidate';"
    ) == importer.ENDPOINT_DATASET_VALIDATED


async def _assert_successful_cutover_state(database, schema: str) -> None:
    assert await _resolvable_source_dataset_id(database, schema) == (
        "dataset_graphql_candidate"
    )
    assert await database.scalar(
        f"SELECT endpoint_id FROM {schema}.provider_directory_source "
        "WHERE source_id = 'source_primary';"
    ) == "endpoint_graphql"
    assert await database.scalar(
        f"SELECT status FROM {schema}.provider_directory_endpoint_dataset "
        "WHERE dataset_id = 'dataset_shared';"
    ) == importer.ENDPOINT_DATASET_PUBLISHED
    assert await database.scalar(
        f"SELECT is_current FROM {schema}.provider_directory_endpoint_dataset "
        "WHERE dataset_id = 'dataset_shared';"
    ) is True


@pytest.mark.asyncio
async def test_contract_identity_cutover_preserves_published_evidence(monkeypatch):
    """Keep old evidence resolvable until the replacement publishes atomically."""
    async with _dataset_database(monkeypatch) as (database, schema):
        await _insert_graphql_candidate(database, schema)
        assert await _resolvable_source_dataset_id(database, schema) == (
            "dataset_shared"
        )
        dataset_fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"],
            should_select_validated_candidates=True,
        )
        candidate = dataset_fence.promotion_datasets[0]
        assert (
            candidate.dataset_id,
            candidate.endpoint_id,
            candidate.serving_endpoint_id,
        ) == (
            "dataset_graphql_candidate",
            "endpoint_graphql",
            "endpoint_shared",
        )

        await _create_artifact_bundle_relations(database, schema)
        failing_stages = await _artifact_bundle_stages(
            schema,
            should_fail_second=True,
        )
        with _active_dataset_fence(dataset_fence):
            with pytest.raises(RuntimeError, match="second target failed"):
                await importer._promote_provider_directory_artifact_bundle_transaction(
                    failing_stages
                )
        await _assert_failed_cutover_state(database, schema)

        with _active_dataset_fence(dataset_fence):
            await importer._promote_provider_directory_artifact_bundle_transaction(
                await _artifact_bundle_stages(schema)
            )
        await _assert_successful_cutover_state(database, schema)

        serving_fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"],
            should_select_validated_candidates=False,
        )
        assert serving_fence.dataset_id_by_endpoint_id == {
            "endpoint_graphql": "dataset_graphql_candidate"
        }
