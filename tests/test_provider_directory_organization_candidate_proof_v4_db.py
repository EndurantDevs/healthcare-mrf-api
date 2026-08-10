# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL candidate-proof parity for semantic-v4 Organizations."""

from __future__ import annotations

import json

import pytest

from db.connection import Database
from process.provider_directory_organization_hash import (
    canonical_organization_payload,
)
from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
    resource_payload_sha256_for_contract,
)
from tests.provider_directory_organization_union_v4_postgres_support import (
    RESOURCE_ID,
    organization_observation,
    stored_dataset_row,
    stored_proof,
    write_page,
)
from tests.provider_directory_semantic_proof_v3_postgres_support import (
    PROJECTION_AS_OF,
    SOURCE_ID,
    _insert_parent,
    _semantic_database,
    _shard_records,
    importer,
)


DATASET_ID = "dataset-organization-candidate-proof"
RESOURCE_SCOPE = ("Organization",)


def _candidate() -> importer.EndpointDatasetCandidate:
    """Return the exact semantic-v4 candidate identity."""

    return importer.EndpointDatasetCandidate(
        endpoint_id=f"endpoint-{DATASET_ID}"[:64],
        dataset_id=DATASET_ID,
        acquisition_root_run_id=f"root-{DATASET_ID}"[:64],
        source_ids=(SOURCE_ID,),
        selected_resources=RESOURCE_SCOPE,
        expected_resources=RESOURCE_SCOPE,
        import_run_id="run-organization-union",
        previous_dataset_id=None,
        resource_hash_contract=SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
        semantic_projection_as_of=PROJECTION_AS_OF,
        proof_resource_scope=RESOURCE_SCOPE,
    )


def _assert_proof_parity(candidate_proof, stored_proof_by_field) -> None:
    """Compare every stored proof field exposed by candidate finalization."""

    assert candidate_proof.dataset_hash == stored_proof_by_field.dataset_hash
    assert candidate_proof.resource_count == stored_proof_by_field.resource_count
    assert candidate_proof.resource_hashes == stored_proof_by_field.resource_hashes
    assert candidate_proof.resource_counts == stored_proof_by_field.resource_counts
    assert candidate_proof.source_metrics == stored_proof_by_field.source_metrics
    assert candidate_proof.proof_metadata == stored_proof_by_field.metadata


async def _replace_retained_payload(
    database: Database,
    schema: str,
) -> None:
    """Change only retained aliases under a valid independent v4 hash."""

    original_hash, payload_by_field = await stored_dataset_row(
        database,
        schema,
        DATASET_ID,
    )
    mutated_payload_by_field = canonical_organization_payload(
        {
            **payload_by_field,
            "aliases": [*payload_by_field["aliases"], "Unobserved Alias"],
        }
    )
    mutated_hash = resource_payload_sha256_for_contract(
        mutated_payload_by_field,
        SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
        resource_type="Organization",
    )
    assert mutated_hash != original_hash
    await database.status(
        f'UPDATE "{schema}".provider_directory_dataset_resource '
        "SET payload_hash=:payload_hash, payload_json=CAST(:payload_json AS jsonb) "
        "WHERE dataset_id=:dataset_id AND resource_type='Organization' "
        "AND resource_id=:resource_id;",
        payload_hash=mutated_hash,
        payload_json=json.dumps(mutated_payload_by_field),
        dataset_id=DATASET_ID,
        resource_id=RESOURCE_ID,
    )
    assert await stored_dataset_row(database, schema, DATASET_ID) == (
        mutated_hash,
        mutated_payload_by_field,
    )


@pytest.mark.asyncio
async def test_postgres_v4_candidate_proof_rejects_retained_alias_drift(
    monkeypatch,
) -> None:
    """Accept exact union parity and reject a valid hash absent from shards."""

    async with _semantic_database(monkeypatch) as (database, schema):
        await _insert_parent(
            database,
            schema,
            DATASET_ID,
            resource_hash_contract=SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
            selected_resources=RESOURCE_SCOPE,
        )
        await write_page(
            DATASET_ID,
            organization_observation(
                "Community Health Center",
                aliases=["Regional Clinic"],
                page_number=1,
                transport_host="a.example.test",
            ),
        )
        await write_page(
            DATASET_ID,
            organization_observation(
                "COMMUNITY HEALTH SERVICES",
                page_number=2,
                transport_host="z.example.test",
            ),
        )
        candidate = _candidate()
        sealed_proof = await stored_proof(database, schema, DATASET_ID)
        shards_before = await _shard_records(database, schema, DATASET_ID)
        async with database.acquire() as connection:
            candidate_proof = (
                await importer._candidate_endpoint_dataset_content_proof(
                    connection,
                    candidate,
                )
            )
        _assert_proof_parity(candidate_proof, sealed_proof)

        await _replace_retained_payload(database, schema)
        assert await _shard_records(database, schema, DATASET_ID) == shards_before
        async with database.acquire() as connection:
            with pytest.raises(
                RuntimeError,
                match="provider_directory_semantic_content_proof_mismatch",
            ):
                await importer._candidate_endpoint_dataset_content_proof(
                    connection,
                    candidate,
                )
