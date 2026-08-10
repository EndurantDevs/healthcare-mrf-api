# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Sealed proof-contract coverage for semantic proof v3."""

from __future__ import annotations

import pytest

from process import provider_directory_proof_store as proof_store
from process.provider_directory_proof_store import (
    PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY,
    PROVIDER_DIRECTORY_SEMANTIC_CONTENT_PROOF_CONTRACT_ID,
    ProviderDirectoryProofStoreError,
)
from process.provider_directory_resource_hash import (
    LEGACY_RESOURCE_HASH_CONTRACT,
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
)
from tests.test_provider_directory_proof_store import (
    DATASET_ID as PROOF_DATASET_ID,
    ENDPOINT_ID as PROOF_ENDPOINT_ID,
    ROOT_RUN_ID as PROOF_ROOT_RUN_ID,
    SOURCE_IDS as PROOF_SOURCE_IDS,
    _MemoryProofConnection,
    _dataset_resource as _legacy_dataset_resource,
    _persist_rows_by_resource,
    _sample_dataset_resources,
)
from tests.test_provider_directory_semantic_proof_v3 import (
    PROJECTION_AS_OF,
    _dataset_row,
    _observation,
)


PROOF_RESOURCE_SCOPE = ["Practitioner"]
HISTORICAL_RESOURCE_SCOPE = [
    "InsurancePlan",
    "Location",
    "Organization",
    "OrganizationAffiliation",
    "Practitioner",
]


def _proof_identity(selected_resources):
    return {
        "dataset_id": PROOF_DATASET_ID,
        "endpoint_id": PROOF_ENDPOINT_ID,
        "acquisition_root_run_id": PROOF_ROOT_RUN_ID,
        "source_ids": PROOF_SOURCE_IDS,
        "selected_resources": selected_resources,
    }

def _semantic_proof_connection() -> _MemoryProofConnection:
    connection = _MemoryProofConnection()
    connection.parent["publication_metadata_json"] = {
        "source_ids": PROOF_SOURCE_IDS,
        "selected_resources": ["Practitioner"],
        "proof_resource_scope": PROOF_RESOURCE_SCOPE,
        "resource_hash_contract": SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        "semantic_projection_as_of": PROJECTION_AS_OF,
    }
    return connection


def _semantic_proof_row(**observation_changes) -> dict[str, object]:
    return {
        **_dataset_row(_observation(**observation_changes)),
        "dataset_id": PROOF_DATASET_ID,
    }


async def _semantic_stored_proof(connection: _MemoryProofConnection):
    return await proof_store.build_stored_dataset_proof(
        connection,
        "mrf",
        dataset_id=PROOF_DATASET_ID,
        endpoint_id=PROOF_ENDPOINT_ID,
        acquisition_root_run_id=PROOF_ROOT_RUN_ID,
        source_ids=PROOF_SOURCE_IDS,
        selected_resources=["Practitioner"],
        options=proof_store.ProviderDirectoryStoredProofOptions(
            proof_resource_scope=PROOF_RESOURCE_SCOPE,
            expected_resource_hash_contract=(
                SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
            ),
            expected_semantic_projection_as_of=PROJECTION_AS_OF,
        ),
    )


async def _semantic_proof_with_one_row():
    semantic_connection = _semantic_proof_connection()
    await proof_store.persist_dataset_proof_shard(
        semantic_connection,
        "mrf",
        [_semantic_proof_row()],
        dataset_id=PROOF_DATASET_ID,
        expected_resource_hash_contract=(
            SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
        ),
    )
    return semantic_connection, await _semantic_stored_proof(
        semantic_connection
    )


async def _append_legacy_organization_shard(semantic_connection):
    legacy_descriptor, legacy_payload = proof_store.build_dataset_proof_shard(
        [
            _legacy_dataset_resource(
                "Organization",
                "organization-1",
                {"resource_id": "organization-1", "name": "Example"},
            )
        ],
        **{
            key: value
            for key, value in _proof_identity(["Organization"]).items()
            if key != "selected_resources"
        },
        resource_hash_contract=LEGACY_RESOURCE_HASH_CONTRACT,
    )
    await semantic_connection.status(
        "INSERT INTO provider_directory_dataset_proof_shard",
        **proof_store._proof_shard_insert_params(
            legacy_descriptor,
            legacy_payload,
        ),
    )


@pytest.mark.asyncio
async def test_v3_sealed_proof_binds_contract_and_projection_date():
    """Bind a semantic seal to its hash contract and immutable root date."""

    connection = _semantic_proof_connection()
    await proof_store.persist_dataset_proof_shard(
        connection,
        "mrf",
        [_semantic_proof_row()],
        dataset_id=PROOF_DATASET_ID,
        expected_resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    )

    stored_proof = await _semantic_stored_proof(connection)
    sealed = stored_proof.metadata

    assert sealed["contract_id"] == (
        PROVIDER_DIRECTORY_SEMANTIC_CONTENT_PROOF_CONTRACT_ID
    )
    assert sealed["resource_hash_contract"] == (
        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
    )
    assert sealed["semantic_projection_as_of"] == PROJECTION_AS_OF
    assert PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY == (
        "provider_directory_content_proof_v1"
    )
    assert proof_store.validate_stored_dataset_proof_metadata(
        sealed,
        dataset_id=PROOF_DATASET_ID,
        endpoint_id=PROOF_ENDPOINT_ID,
        acquisition_root_run_id=PROOF_ROOT_RUN_ID,
        source_ids=PROOF_SOURCE_IDS,
        selected_resources=["Practitioner"],
        options=proof_store.ProviderDirectoryStoredProofOptions(
            proof_resource_scope=PROOF_RESOURCE_SCOPE,
            expected_resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
            expected_semantic_projection_as_of=PROJECTION_AS_OF,
        ),
    ) == sealed

    with pytest.raises(
        ProviderDirectoryProofStoreError,
        match="projection date changed",
    ):
        proof_store.validate_stored_dataset_proof_metadata(
            sealed,
            dataset_id=PROOF_DATASET_ID,
            endpoint_id=PROOF_ENDPOINT_ID,
            acquisition_root_run_id=PROOF_ROOT_RUN_ID,
            source_ids=PROOF_SOURCE_IDS,
            selected_resources=["Practitioner"],
            options=proof_store.ProviderDirectoryStoredProofOptions(
                proof_resource_scope=PROOF_RESOURCE_SCOPE,
                expected_resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
                expected_semantic_projection_as_of="2026-08-10",
            ),
        )


@pytest.mark.asyncio
async def test_proof_contracts_reject_cross_version_and_mixed_shards():
    """Reject a semantic seal as v2 and a mixed legacy shard set."""

    semantic_connection, semantic_proof = await _semantic_proof_with_one_row()
    with pytest.raises(
        ProviderDirectoryProofStoreError,
        match="contract changed",
    ):
        proof_store.validate_stored_dataset_proof_metadata(
            semantic_proof.metadata,
            **_proof_identity(["Practitioner"]),
            options=proof_store.ProviderDirectoryStoredProofOptions(
                proof_resource_scope=PROOF_RESOURCE_SCOPE,
                expected_resource_hash_contract=(
                    TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT
                ),
            ),
        )

    await _append_legacy_organization_shard(semantic_connection)
    with pytest.raises(
        ProviderDirectoryProofStoreError,
        match="contract changed",
    ):
        await proof_store.build_stored_dataset_proof(
            semantic_connection,
            "mrf",
            **_proof_identity(["Practitioner"]),
            options=proof_store.ProviderDirectoryStoredProofOptions(
                proof_resource_scope=["Organization", "Practitioner"],
                expected_resource_hash_contract=(
                    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
                ),
                expected_semantic_projection_as_of=PROJECTION_AS_OF,
            ),
        )


@pytest.mark.asyncio
async def test_historical_proof_shape_remains_readable_for_v1_and_v2():
    """Keep a legacy seal readable under both historical hash contracts."""

    legacy_connection = _MemoryProofConnection()
    await _persist_rows_by_resource(
        legacy_connection,
        _sample_dataset_resources(),
    )
    legacy_proof = await proof_store.build_stored_dataset_proof(
        legacy_connection,
        "mrf",
        **_proof_identity(HISTORICAL_RESOURCE_SCOPE),
        options=proof_store.ProviderDirectoryStoredProofOptions(
            expected_resource_hash_contract=LEGACY_RESOURCE_HASH_CONTRACT,
        ),
    )
    proof_store.validate_stored_dataset_proof_metadata(
        legacy_proof.metadata,
        **_proof_identity(HISTORICAL_RESOURCE_SCOPE),
        options=proof_store.ProviderDirectoryStoredProofOptions(
            expected_resource_hash_contract=LEGACY_RESOURCE_HASH_CONTRACT,
        ),
    )
    proof_store.validate_stored_dataset_proof_metadata(
        legacy_proof.metadata,
        **_proof_identity(HISTORICAL_RESOURCE_SCOPE),
        options=proof_store.ProviderDirectoryStoredProofOptions(
            expected_resource_hash_contract=(
                TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT
            ),
        ),
    )
    with pytest.raises(
        ProviderDirectoryProofStoreError,
        match="contract changed",
    ):
        proof_store.validate_stored_dataset_proof_metadata(
            legacy_proof.metadata,
            **_proof_identity(HISTORICAL_RESOURCE_SCOPE),
            options=proof_store.ProviderDirectoryStoredProofOptions(
                expected_resource_hash_contract=(
                    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
                ),
                expected_semantic_projection_as_of=PROJECTION_AS_OF,
            ),
        )


def test_proof_shard_rejects_unknown_hash_contract():
    with pytest.raises(
        ProviderDirectoryProofStoreError,
        match="hash contract is invalid",
    ):
        proof_store.build_dataset_proof_shard(
            [_semantic_proof_row()],
            dataset_id=PROOF_DATASET_ID,
            endpoint_id=PROOF_ENDPOINT_ID,
            acquisition_root_run_id=PROOF_ROOT_RUN_ID,
            source_ids=PROOF_SOURCE_IDS,
            resource_hash_contract="unknown-v4",
        )
