# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json

import pytest

from tests import test_provider_directory_artifact_eligibility_db as eligibility


importer = eligibility.importer


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("alias_endpoint_id", "alias_metadata"),
    [
        (
            eligibility.SERVING_ENDPOINT_ID,
            eligibility._source_metadata(
                status=importer.PROVIDER_DIRECTORY_TWIN_ROOT_VERIFIED
            ),
        ),
        (eligibility.ENDPOINT_ID, {}),
    ],
)
async def test_verified_gate_rejects_unproven_configured_or_serving_alias(
    monkeypatch,
    alias_endpoint_id,
    alias_metadata,
):
    async with eligibility._candidate_database(monkeypatch) as (
        database,
        schema,
    ):
        await eligibility._set_all_source_profiles(
            database,
            schema,
            importer.PROVIDER_DIRECTORY_TWIN_ROOT_VERIFIED,
        )
        await database.status(
            f"""
            INSERT INTO {schema}.provider_directory_source (
                source_id, endpoint_id, metadata_json
            ) VALUES ('source_c', :endpoint_id, CAST(:metadata AS jsonb));
            """,
            endpoint_id=alias_endpoint_id,
            metadata=json.dumps(alias_metadata),
        )
        assert eligibility._option_ids(
            await eligibility._artifact_options(database, schema)
        ) == ["dataset_current"]


@pytest.mark.asyncio
async def test_verified_gate_allows_ordinary_refresh_with_matched_anchor(
    monkeypatch,
):
    async with eligibility._candidate_database(monkeypatch) as (
        database,
        schema,
    ):
        verified_metadata = eligibility._source_metadata(
            status=importer.PROVIDER_DIRECTORY_TWIN_ROOT_VERIFIED
        )
        for source_id in ("source_a", "source_b"):
            await eligibility._set_source_metadata(
                database, schema, source_id, verified_metadata
            )
        await database.status(
            f"""
            UPDATE {schema}.provider_directory_endpoint_dataset
               SET status = :superseded
             WHERE dataset_id = 'dataset_exact_matched';
            """,
            superseded=importer.ENDPOINT_DATASET_SUPERSEDED,
        )
        await database.status(
            f"""
            INSERT INTO {schema}.provider_directory_endpoint_dataset (
                dataset_id, endpoint_id, acquisition_root_run_id,
                dataset_hash, status, resource_count,
                publication_metadata_json
            ) VALUES (
                'dataset_ordinary_refresh', :endpoint_id, 'root_refresh',
                :dataset_hash, :validated, :resource_count,
                CAST(:metadata AS jsonb)
            );
            """,
            endpoint_id=eligibility.ENDPOINT_ID,
            dataset_hash=eligibility.DATASET_HASH,
            validated=importer.ENDPOINT_DATASET_VALIDATED,
            resource_count=eligibility.RESOURCE_COUNT,
            metadata=json.dumps(eligibility._ordinary_metadata()),
        )
        options = await eligibility._artifact_options(database, schema)
        assert eligibility._option_ids(options) == [
            "dataset_current",
            "dataset_ordinary_refresh",
        ]
        await eligibility._set_source_metadata(
            database,
            schema,
            "source_a",
            eligibility._source_metadata(
                status=importer.PROVIDER_DIRECTORY_TWIN_ROOT_VERIFIED,
                campaign_id="reviewed-candidate-v2",
            ),
        )
        assert eligibility._option_ids(
            await eligibility._artifact_options(database, schema)
        ) == ["dataset_current"]
