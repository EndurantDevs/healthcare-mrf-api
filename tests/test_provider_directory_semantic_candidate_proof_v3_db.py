# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL parity proof for semantic-v3 candidate content."""

from __future__ import annotations

import json

import pytest

from db.connection import Database
from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    canonical_practitioner_payload,
    resource_payload_sha256_for_contract,
)
from tests.provider_directory_semantic_proof_v3_postgres_support import (
    PROJECTION_AS_OF,
    RESOURCE_ID,
    SOURCE_ID,
    _insert_parent,
    _observation,
    _proof,
    _semantic_database,
    _shard_records,
    _stored_dataset_row,
    _write_page,
    importer,
)


DATASET_ID = "dataset-semantic-candidate-proof"
RESOURCE_SCOPE = ("Practitioner",)


def _semantic_candidate() -> importer.EndpointDatasetCandidate:
    """Return the exact candidate identity persisted by the shared fixture."""
    return importer.EndpointDatasetCandidate(
        endpoint_id=f"endpoint-{DATASET_ID}"[:64],
        dataset_id=DATASET_ID,
        acquisition_root_run_id=f"root-{DATASET_ID}"[:64],
        source_ids=(SOURCE_ID,),
        selected_resources=RESOURCE_SCOPE,
        expected_resources=RESOURCE_SCOPE,
        import_run_id="run-semantic-proof",
        previous_dataset_id=None,
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        semantic_projection_as_of=PROJECTION_AS_OF,
        proof_resource_scope=RESOURCE_SCOPE,
    )


def _assert_candidate_stored_proof_parity(
    candidate_proof,
    stored_proof,
) -> None:
    """Compare every stored proof field projected into candidate content."""
    assert candidate_proof.dataset_hash == stored_proof.dataset_hash
    assert candidate_proof.resource_count == stored_proof.resource_count
    assert candidate_proof.resource_hashes == stored_proof.resource_hashes
    assert candidate_proof.resource_counts == stored_proof.resource_counts
    assert candidate_proof.source_metrics == stored_proof.source_metrics
    assert candidate_proof.proof_metadata == stored_proof.metadata


async def _replace_retained_payload_v3(
    database: Database,
    schema: str,
) -> None:
    """Replace only the retained row with a valid, independently hashed payload."""
    original_hash, stored_payload_by_field = await _stored_dataset_row(
        database,
        schema,
        DATASET_ID,
    )
    mutated_payload_by_field = canonical_practitioner_payload(
        {**stored_payload_by_field, "active": False}
    )
    mutated_hash = resource_payload_sha256_for_contract(
        mutated_payload_by_field,
        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    )
    assert mutated_payload_by_field["active"] is False
    assert mutated_hash != original_hash
    await database.status(
        f'UPDATE "{schema}".provider_directory_dataset_resource '
        "SET payload_hash=:payload_hash, payload_json=CAST(:payload_json AS jsonb) "
        "WHERE dataset_id=:dataset_id AND resource_type='Practitioner' "
        "AND resource_id=:resource_id;",
        payload_hash=mutated_hash,
        payload_json=json.dumps(mutated_payload_by_field),
        dataset_id=DATASET_ID,
        resource_id=RESOURCE_ID,
    )
    persisted_hash, persisted_payload_by_field = await _stored_dataset_row(
        database,
        schema,
        DATASET_ID,
    )
    assert persisted_hash == mutated_hash
    assert persisted_payload_by_field == mutated_payload_by_field


@pytest.mark.asyncio
async def test_postgres_v3_candidate_proof_parity_and_retained_mismatch(
    monkeypatch,
):
    """Accept exact shard parity and reject independently valid retained drift."""
    async with _semantic_database(monkeypatch) as (database, schema):
        await _insert_parent(database, schema, DATASET_ID)
        assert await _write_page(
            DATASET_ID,
            _observation("Alex Example", page_number=1),
        ) == 1
        candidate = _semantic_candidate()
        stored_proof = await _proof(database, schema, DATASET_ID)
        shard_hashes_before, shard_records_before = await _shard_records(
            database,
            schema,
            DATASET_ID,
        )
        async with database.acquire() as connection:
            candidate_proof = await importer._candidate_endpoint_dataset_content_proof(
                connection,
                candidate,
            )
        _assert_candidate_stored_proof_parity(candidate_proof, stored_proof)

        await _replace_retained_payload_v3(database, schema)
        assert await _shard_records(database, schema, DATASET_ID) == (
            shard_hashes_before,
            shard_records_before,
        )
        async with database.acquire() as connection:
            with pytest.raises(
                RuntimeError,
                match="provider_directory_semantic_content_proof_mismatch",
            ):
                await importer._candidate_endpoint_dataset_content_proof(
                    connection,
                    candidate,
                )
