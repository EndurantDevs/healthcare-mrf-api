# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Admission contract for one retired reviewed Provider Directory generation."""

from __future__ import annotations

from dataclasses import replace
import json

import pytest

from api.provider_directory_source_catalog_outcomes import (
    _canonical_validated_datasets_by_source_id,
)
from tests.provider_directory_dataset_artifact_pg_support import (
    seal_validated_dataset,
)
from tests.test_provider_directory_dataset_artifact_db import (
    _dataset_database,
    importer,
)
from tests.test_provider_directory_dataset_selection_sealed_db import (
    _fresh_acquisition_candidate,
)


def _retired_metadata() -> dict[str, object]:
    return {
        "source_ids": ["source_primary"],
        "requires_twin_root_verification": True,
        importer.TWIN_ROOT_VERIFICATION_CAMPAIGN_KEY: (
            importer.LEGACY_REVIEWED_PARTITION_CAMPAIGN_ID
        ),
        importer.TWIN_ROOT_VERIFICATION_ROLE_KEY: (
            importer.TWIN_ROOT_VERIFICATION_CANDIDATE_ROLE
        ),
        importer.TWIN_ROOT_VERIFICATION_BASELINE_DATASET_KEY: (
            "dataset_legacy_baseline"
        ),
        "ignored_large_proof": "x" * 1_100_000,
    }


async def _insert_retired_candidate(database, schema: str) -> None:
    metadata = _retired_metadata()
    assert len(json.dumps(metadata).encode()) > 1024 * 1024
    await database.status(
        f"""
        INSERT INTO {schema}.provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, import_run_id, acquisition_root_run_id,
            previous_dataset_id, dataset_hash, status, is_current,
            resource_count, validated_at, publication_metadata_json
        ) VALUES (
            'dataset_legacy_retired', 'endpoint_shared', 'run-legacy-retired',
            'root-legacy-retired', 'dataset_shared', repeat('e', 64),
            :validated_status, false, 2048, now(), CAST(:metadata AS json)
        );
        """,
        validated_status=importer.ENDPOINT_DATASET_VALIDATED,
        metadata=json.dumps(metadata),
    )


async def _store_ordinary_source(database, schema: str) -> None:
    metadata = {
        "provider_directory_override": "reviewed_candidate_acquisition",
        "provider_directory_acquisition_enabled": True,
        "provider_directory_coverage_mode": "full",
        "provider_directory_supported_resources": ["Location"],
        "provider_directory_fully_enumerable_resources": ["Location"],
        importer.PROVIDER_DIRECTORY_CONFIGURED_ENDPOINT_METADATA_KEY: (
            "endpoint_shared"
        ),
        importer.LAST_UPDATED_PARTITION_METADATA_KEY: {
            "enabled": True,
            "resources": importer.REVIEWED_PRACTITIONER_ROLE_PARTITION_RESOURCES,
        },
    }
    await database.status(
        f"""
        UPDATE {schema}.provider_directory_source
           SET metadata_json = CAST(:metadata AS json)
         WHERE source_id = 'source_primary';
        """,
        metadata=json.dumps(metadata),
    )


async def _assert_blocked(database, candidate) -> None:
    async with database.acquire() as connection:
        with pytest.raises(RuntimeError, match="active_conflict"):
            await importer._assert_no_conflicting_endpoint_candidate(
                connection, candidate
            )


async def _assert_admitted(database, candidate) -> None:
    async with database.acquire() as connection:
        assert await importer._assert_no_conflicting_endpoint_candidate(
            connection, candidate
        ) == candidate


async def _advance_current_dataset(database, schema: str, candidate):
    await database.status(
        f"""
        UPDATE {schema}.provider_directory_endpoint_dataset
           SET status = :superseded_status,
               is_current = false,
               superseded_at = now()
         WHERE dataset_id = 'dataset_shared';
        """,
        superseded_status=importer.ENDPOINT_DATASET_SUPERSEDED,
    )
    await database.status(
        f"""
        INSERT INTO {schema}.provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, import_run_id, acquisition_root_run_id,
            previous_dataset_id, status, is_current, validated_at,
            published_at, publication_metadata_json
        ) VALUES (
            'dataset_next', 'endpoint_shared', 'run-next', 'root-next',
            'dataset_shared', :published_status, true, now(), now(), '{{}}'
        );
        """,
        published_status=importer.ENDPOINT_DATASET_PUBLISHED,
    )
    return replace(
        candidate,
        dataset_id="dataset_fresh_next",
        acquisition_root_run_id="root-fresh-next",
        import_run_id="run-fresh-next",
        previous_dataset_id="dataset_next",
    )


async def _set_retired_campaign(database, schema: str, campaign: str) -> None:
    await database.status(
        f"""
        UPDATE {schema}.provider_directory_endpoint_dataset
           SET publication_metadata_json = jsonb_set(
               publication_metadata_json, '{{verification_campaign_id}}',
               to_jsonb(CAST(:campaign AS text)))
         WHERE dataset_id = 'dataset_legacy_retired';
        """,
        campaign=campaign,
    )


@pytest.mark.asyncio
async def test_retired_candidate_never_reblocks_ordinary_acquisition(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        await _insert_retired_candidate(database, schema)
        candidate = _fresh_acquisition_candidate()
        await _assert_blocked(database, candidate)
        await _store_ordinary_source(database, schema)
        await _assert_blocked(database, candidate)
        await database.status(
            f"UPDATE {schema}.provider_directory_source "
            "SET endpoint_id = 'endpoint_repoint' "
            "WHERE source_id = 'source_sibling';"
        )
        await _assert_blocked(
            database, replace(candidate, verification_campaign_id="new-review")
        )
        await _assert_blocked(
            database, replace(candidate, previous_dataset_id="dataset_other")
        )
        await _assert_admitted(database, candidate)
        candidate = await _advance_current_dataset(database, schema, candidate)
        await _assert_admitted(database, candidate)
        await _set_retired_campaign(database, schema, "other-generation")
        await _assert_blocked(database, candidate)
        await _set_retired_campaign(
            database, schema, importer.LEGACY_REVIEWED_PARTITION_CAMPAIGN_ID
        )
        await seal_validated_dataset(database, schema, "dataset_legacy_retired")
        assert await _canonical_validated_datasets_by_source_id(
            ["source_primary"]
        ) == {}
        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset "
            "SET artifact_selection_receipt_json = '{}'::jsonb "
            "WHERE dataset_id = 'dataset_legacy_retired';"
        )
        await _assert_admitted(database, candidate)
