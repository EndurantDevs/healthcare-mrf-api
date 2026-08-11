# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL concurrency proof for source-local Provider Directory shards."""

from __future__ import annotations

import asyncio
import json

import pytest

from db.connection import Database
from process.provider_directory_proof_store import (
    PROVIDER_DIRECTORY_PROOF_SHARD_TABLE,
    ProviderDirectoryProofStoreError,
)
from tests import test_provider_directory_proof_store_db as proof_support


importer = proof_support.importer
SECOND_DATASET_ID = "dataset-proof-second"
SECOND_ENDPOINT_ID = "endpoint-proof-second"
SECOND_ROOT_RUN_ID = "root-proof-second"
SECOND_SOURCE_IDS = ("source-c",)
SECOND_SELECTED_RESOURCES = ("Location", "Practitioner")


async def _insert_candidate_dataset(
    database: Database,
    schema: str,
) -> None:
    """Insert the second mutable candidate with isolated source lineage."""

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
        dataset_id=SECOND_DATASET_ID,
        endpoint_id=SECOND_ENDPOINT_ID,
        root_run_id=SECOND_ROOT_RUN_ID,
        status=importer.ENDPOINT_DATASET_ACQUIRING,
        metadata_json=json.dumps(
            {
                "selected_resources": list(SECOND_SELECTED_RESOURCES),
                "source_ids": list(SECOND_SOURCE_IDS),
            }
        ),
    )


def _second_resource_rows():
    """Return a source-disjoint second candidate's canonical resources."""

    return [
        proof_support._resource(
            "Practitioner",
            "practitioner-second",
            {"npi": "200", "addresses": [{"city": "Pittsburgh"}]},
            dataset_id=SECOND_DATASET_ID,
        ),
        proof_support._resource(
            "Location",
            "location-second",
            {
                "first_line": "2 Liberty Ave",
                "latitude": "40.4",
                "longitude": "-80.0",
            },
            dataset_id=SECOND_DATASET_ID,
        ),
    ]


def _second_candidate():
    """Return the source-disjoint candidate used by the isolation proof."""

    return proof_support._candidate(
        dataset_id=SECOND_DATASET_ID,
        endpoint_id=SECOND_ENDPOINT_ID,
        root_run_id=SECOND_ROOT_RUN_ID,
        source_ids=SECOND_SOURCE_IDS,
        selected_resources=SECOND_SELECTED_RESOURCES,
    )


def _complete_relation_proofs():
    """Return exact zero-edge relation proofs for summary finalization."""

    return {
        importer.PROVIDER_DIRECTORY_DATASET_NETWORK_PLAN_METADATA_KEY: {
            "complete": True,
            "edge_count": 0,
        },
        importer.PROVIDER_DIRECTORY_DATASET_AFFILIATION_ORGANIZATION_METADATA_KEY: {
            "complete": True,
            "edge_count": 0,
        },
    }


async def _materialize_candidate_summary(
    database: Database,
    candidate,
    dataset_resources,
):
    """Commit one candidate and finalize its source-local proof summary."""

    async with database.acquire() as connection:
        await proof_support._write_resource_batch(
            connection,
            dataset_resources,
        )
        content_proof = (
            await importer._candidate_endpoint_dataset_content_proof(
                connection,
                candidate,
            )
        )
        source_summary = await importer._endpoint_dataset_source_summary(
            connection,
            candidate,
            content_proof,
            _complete_relation_proofs(),
        )
    return content_proof, source_summary


async def _hold_candidate_then_fail(
    database: Database,
    lock_acquired: asyncio.Event,
    release_failure: asyncio.Event,
) -> None:
    """Hold candidate A's row lock, then force its batch to roll back."""

    async with database.acquire() as connection:
        await proof_support._write_resource_batch(
            connection,
            proof_support._resource_rows(),
        )
        lock_acquired.set()
        await release_failure.wait()
        raise RuntimeError("source-a-delayed-failure")


async def _assert_second_candidate_is_scoped(
    database: Database,
    schema: str,
    content_proof,
    source_summary,
) -> None:
    """Assert candidate B committed only under its dataset/source keys."""

    assert await database.scalar(
        f'SELECT count(*) FROM "{schema}".provider_directory_dataset_resource '
        "WHERE dataset_id=:dataset_id;",
        dataset_id=proof_support.DATASET_ID,
    ) == 0
    assert await database.scalar(
        f'SELECT count(*) FROM "{schema}".provider_directory_dataset_resource '
        "WHERE dataset_id=:dataset_id;",
        dataset_id=SECOND_DATASET_ID,
    ) == 2
    assert await database.scalar(
        f'SELECT count(*) FROM "{schema}"."{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}" '
        "WHERE dataset_id=:dataset_id;",
        dataset_id=proof_support.DATASET_ID,
    ) == 0
    assert await database.scalar(
        f"""
        SELECT count(*)
          FROM "{schema}"."{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}"
         WHERE dataset_id=:dataset_id
           AND endpoint_id=:endpoint_id
           AND acquisition_root_run_id=:root_run_id
           AND source_ids_json=CAST(:source_ids_json AS jsonb);
        """,
        dataset_id=SECOND_DATASET_ID,
        endpoint_id=SECOND_ENDPOINT_ID,
        root_run_id=SECOND_ROOT_RUN_ID,
        source_ids_json=json.dumps(list(SECOND_SOURCE_IDS)),
    ) == 2
    assert await database.scalar(
        f'SELECT count(*) FROM "{schema}".provider_directory_endpoint_dataset '
        "WHERE is_current=true;"
    ) == 0
    primary_key = await database.scalar(
        """
        SELECT pg_get_constraintdef(oid)
          FROM pg_constraint
         WHERE conrelid=to_regclass(:table_ref) AND contype='p';
        """,
        table_ref=(
            f'"{schema}"."{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}"'
        ),
    )
    assert primary_key == "PRIMARY KEY (dataset_id, shard_id)"
    assert content_proof.proof_metadata["dataset_id"] == SECOND_DATASET_ID
    assert content_proof.proof_metadata["source_ids"] == list(
        SECOND_SOURCE_IDS
    )
    assert source_summary["dataset_id"] == SECOND_DATASET_ID
    assert source_summary["endpoint_id"] == SECOND_ENDPOINT_ID
    assert source_summary["source_ids"] == list(SECOND_SOURCE_IDS)


async def _assert_first_candidate_tamper_is_isolated(
    database: Database,
    schema: str,
    second_content_proof,
    second_source_summary,
) -> None:
    """Tamper candidate A and prove candidate B's summary survives."""

    await proof_support._write_candidate_batch(
        database,
        proof_support._resource_rows(),
    )
    await database.status(
        f"""
        UPDATE "{schema}"."{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}"
           SET payload_bytes = payload_bytes || decode('00', 'hex')
         WHERE dataset_id = :dataset_id;
        """,
        dataset_id=proof_support.DATASET_ID,
    )
    async with database.acquire() as connection:
        with pytest.raises(
            ProviderDirectoryProofStoreError,
            match="proof artifact changed",
        ):
            await importer._candidate_endpoint_dataset_content_proof(
                connection,
                proof_support._candidate(),
            )
    content_proof, source_summary = await _materialize_candidate_summary(
        database,
        _second_candidate(),
        _second_resource_rows(),
    )
    assert content_proof.dataset_hash == second_content_proof.dataset_hash
    assert content_proof.proof_metadata == second_content_proof.proof_metadata
    assert source_summary == second_source_summary


@pytest.mark.asyncio
async def test_two_source_candidates_finalize_without_global_lock_coupling(
    monkeypatch,
):
    """Prove disjoint candidate writes, rollback, and tamper stay isolated."""

    async with proof_support._proof_database(monkeypatch) as (
        database,
        schema,
    ):
        await _insert_candidate_dataset(database, schema)
        first_lock_acquired = asyncio.Event()
        release_first_failure = asyncio.Event()
        first_task = asyncio.create_task(
            _hold_candidate_then_fail(
                database,
                first_lock_acquired,
                release_first_failure,
            )
        )
        await asyncio.wait_for(first_lock_acquired.wait(), timeout=5)
        try:
            second_content_proof, second_source_summary = (
                await asyncio.wait_for(
                    _materialize_candidate_summary(
                        database,
                        _second_candidate(),
                        _second_resource_rows(),
                    ),
                    timeout=5,
                )
            )
            assert not first_task.done()
        finally:
            release_first_failure.set()
            with pytest.raises(
                RuntimeError,
                match="source-a-delayed-failure",
            ):
                await first_task
        await _assert_second_candidate_is_scoped(
            database,
            schema,
            second_content_proof,
            second_source_summary,
        )
        await _assert_first_candidate_tamper_is_isolated(
            database,
            schema,
            second_content_proof,
            second_source_summary,
        )
