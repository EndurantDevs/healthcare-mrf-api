# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Version-shape and recomputed-seal defenses for durable proof metadata."""

from __future__ import annotations

import copy
import hashlib
import json
import zlib

import pytest

from process import provider_directory_proof_store as proof_store
from process.provider_directory_proof_store import (
    PROVIDER_DIRECTORY_CONTENT_PROOF_CONTRACT_ID,
    PROVIDER_DIRECTORY_SEMANTIC_CONTENT_PROOF_CONTRACT_ID,
    ProviderDirectoryProofStoreError,
    ProviderDirectoryStoredProofOptions,
)
from process.provider_directory_resource_hash import (
    LEGACY_RESOURCE_HASH_CONTRACT,
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    resource_payload_sha256_for_contract,
)
from tests.test_provider_directory_proof_store import (
    DATASET_ID,
    ENDPOINT_ID,
    ROOT_RUN_ID,
    SELECTED_RESOURCES,
    SOURCE_IDS,
    _MemoryProofConnection,
    _dataset_resource,
    _persist_rows_by_resource,
    _sample_dataset_resources,
    _stored_proof,
)


SEMANTIC_PROJECTION_AS_OF = "2026-08-09"
SEMANTIC_PROOF_SCOPE = ["Practitioner"]


def _semantic_practitioner_row() -> dict[str, object]:
    payload_by_field = {
        "npi": "123",
        "names": [{"family": "Example", "given": ["A"]}],
        "family_name": "Example",
        "given_names": ["A"],
        "full_name": "A Example",
    }
    resource_row_by_field = _dataset_resource(
        "Practitioner",
        "practitioner-1",
        payload_by_field,
    )
    resource_row_by_field["payload_hash"] = (
        resource_payload_sha256_for_contract(
            payload_by_field,
            SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        )
    )
    return resource_row_by_field


def _semantic_proof_connection() -> _MemoryProofConnection:
    connection = _MemoryProofConnection()
    connection.parent["publication_metadata_json"] = {
        "source_ids": SOURCE_IDS,
        "selected_resources": ["Practitioner"],
        "proof_resource_scope": SEMANTIC_PROOF_SCOPE,
        "resource_hash_contract": SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        "semantic_projection_as_of": SEMANTIC_PROJECTION_AS_OF,
    }
    return connection


async def _sealed_semantic_proof_metadata() -> dict[str, object]:
    connection = _semantic_proof_connection()
    await proof_store.persist_dataset_proof_shard(
        connection,
        "mrf",
        [_semantic_practitioner_row()],
        dataset_id=DATASET_ID,
        expected_resource_hash_contract=(
            SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
        ),
    )
    stored_proof = await proof_store.build_stored_dataset_proof(
        connection,
        "mrf",
        dataset_id=DATASET_ID,
        endpoint_id=ENDPOINT_ID,
        acquisition_root_run_id=ROOT_RUN_ID,
        source_ids=SOURCE_IDS,
        selected_resources=["Practitioner"],
        options=ProviderDirectoryStoredProofOptions(
            proof_resource_scope=SEMANTIC_PROOF_SCOPE,
            expected_resource_hash_contract=(
                SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
            ),
            expected_semantic_projection_as_of=(
                SEMANTIC_PROJECTION_AS_OF
            ),
        ),
    )
    return stored_proof.metadata


def _resign_proof_metadata(metadata_by_field: dict[str, object]) -> None:
    """Recompute the public seal after an intentional test mutation."""

    metadata_by_field.pop("proof_sha256", None)
    metadata_by_field["proof_sha256"] = proof_store._json_hash(
        metadata_by_field
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field_name", "replacement_value", "error_pattern"),
    (
        (
            "contract_id",
            PROVIDER_DIRECTORY_CONTENT_PROOF_CONTRACT_ID,
            "content proof contract changed",
        ),
        (
            "resource_hash_contract",
            TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
            "semantic proof summary is invalid",
        ),
        (
            "proof_resource_scope",
            ["Organization", "Practitioner"],
            "content proof lineage is invalid",
        ),
        (
            "semantic_projection_as_of",
            "2026-08-10",
            "projection date changed",
        ),
    ),
)
async def test_semantic_proof_rejects_recomputed_seal_tampering(
    field_name,
    replacement_value,
    error_pattern,
):
    """Reject contract, scope, or date drift even under a recomputed seal."""

    metadata_by_field = copy.deepcopy(
        await _sealed_semantic_proof_metadata()
    )
    metadata_by_field[field_name] = replacement_value
    _resign_proof_metadata(metadata_by_field)

    with pytest.raises(
        ProviderDirectoryProofStoreError,
        match=error_pattern,
    ):
        proof_store.validate_stored_dataset_proof_metadata(
            metadata_by_field,
            dataset_id=DATASET_ID,
            endpoint_id=ENDPOINT_ID,
            acquisition_root_run_id=ROOT_RUN_ID,
            source_ids=SOURCE_IDS,
            selected_resources=["Practitioner"],
            options=ProviderDirectoryStoredProofOptions(
                proof_resource_scope=SEMANTIC_PROOF_SCOPE,
                expected_resource_hash_contract=(
                    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
                ),
                expected_semantic_projection_as_of=(
                    SEMANTIC_PROJECTION_AS_OF
                ),
            ),
        )


@pytest.mark.asyncio
async def test_legacy_proof_rejects_resigned_semantic_fields():
    """Keep v3-only identity fields out of historical proof shapes."""

    connection = _MemoryProofConnection()
    await _persist_rows_by_resource(connection, _sample_dataset_resources())
    metadata_by_field = copy.deepcopy((await _stored_proof(connection)).metadata)
    metadata_by_field.update(
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        semantic_projection_as_of=SEMANTIC_PROJECTION_AS_OF,
        semantic_union={
            "added_name_count": 0,
            "collision_identities": 0,
            "observation_variants": 0,
            "union_name_count": 0,
        },
    )
    _resign_proof_metadata(metadata_by_field)

    with pytest.raises(
        ProviderDirectoryProofStoreError,
        match="content proof contract changed",
    ):
        proof_store.validate_stored_dataset_proof_metadata(
            metadata_by_field,
            dataset_id=DATASET_ID,
            endpoint_id=ENDPOINT_ID,
            acquisition_root_run_id=ROOT_RUN_ID,
            source_ids=SOURCE_IDS,
            selected_resources=SELECTED_RESOURCES,
            options=ProviderDirectoryStoredProofOptions(
                expected_resource_hash_contract=(
                    LEGACY_RESOURCE_HASH_CONTRACT
                ),
            ),
        )


@pytest.mark.asyncio
async def test_semantic_proof_accepts_one_shot_scope_iterable():
    """Normalize a one-shot scope once before every identity comparison."""

    metadata_by_field = await _sealed_semantic_proof_metadata()
    proof_resource_types = (
        resource_type for resource_type in SEMANTIC_PROOF_SCOPE
    )
    assert proof_store.validate_stored_dataset_proof_metadata(
        metadata_by_field,
        dataset_id=DATASET_ID,
        endpoint_id=ENDPOINT_ID,
        acquisition_root_run_id=ROOT_RUN_ID,
        source_ids=SOURCE_IDS,
        selected_resources=["Practitioner"],
        options=ProviderDirectoryStoredProofOptions(
            proof_resource_scope=proof_resource_types,
            expected_resource_hash_contract=(
                SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
            ),
            expected_semantic_projection_as_of=SEMANTIC_PROJECTION_AS_OF,
        ),
    ) == metadata_by_field


def _semantic_expectation_proof() -> dict[str, object]:
    return {
        "contract_id": PROVIDER_DIRECTORY_SEMANTIC_CONTENT_PROOF_CONTRACT_ID,
        "resource_hash_contract": SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        "proof_resource_scope": SEMANTIC_PROOF_SCOPE,
        "semantic_projection_as_of": SEMANTIC_PROJECTION_AS_OF,
    }


@pytest.mark.parametrize(
    (
        "expected_contract",
        "expected_scope",
        "expected_projection_as_of",
        "error_pattern",
    ),
    (
        (
            TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
            None,
            None,
            "content proof contract changed",
        ),
        (
            SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
            ["Organization", "Practitioner"],
            SEMANTIC_PROJECTION_AS_OF,
            "proof resource scope changed",
        ),
        (
            SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
            SEMANTIC_PROOF_SCOPE,
            "2026-08-10",
            "projection date changed",
        ),
    ),
)
def test_expected_proof_identity_rejects_contract_scope_or_date_drift(
    expected_contract,
    expected_scope,
    expected_projection_as_of,
    error_pattern,
):
    with pytest.raises(
        ProviderDirectoryProofStoreError,
        match=error_pattern,
    ):
        proof_store._assert_expected_proof_contract(
            _semantic_expectation_proof(),
            expected_contract,
            expected_projection_as_of,
            expected_scope,
        )


def _semantic_shard_fields() -> tuple[dict[str, object], bytes]:
    descriptor_by_field, compressed = proof_store.build_dataset_proof_shard(
        [_semantic_practitioner_row()],
        dataset_id=DATASET_ID,
        endpoint_id=ENDPOINT_ID,
        acquisition_root_run_id=ROOT_RUN_ID,
        source_ids=SOURCE_IDS,
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    )
    shard_fields_by_name = {
        **proof_store._expected_persisted_shard_fields(
            descriptor_by_field,
            compressed,
        ),
        "source_ids_json": descriptor_by_field["source_ids"],
        "resource_counts_json": descriptor_by_field["resource_counts"],
        "first_identity_json": descriptor_by_field["first_identity"],
        "last_identity_json": descriptor_by_field["last_identity"],
    }
    return shard_fields_by_name, compressed


def test_semantic_shard_rejects_recomputed_outer_hash_tampering():
    """Validate intrinsic composition after every outer hash is recomputed."""

    shard_fields_by_name, compressed = _semantic_shard_fields()
    proof_record_fields = json.loads(zlib.decompress(compressed))
    tampered_base_hash = hashlib.sha256(b"tampered-base").hexdigest()
    assert tampered_base_hash != proof_record_fields[8]
    proof_record_fields[8] = tampered_base_hash
    tampered_input = (
        proof_store._stable_json(proof_record_fields).encode() + b"\n"
    )
    tampered_compressed = zlib.compress(tampered_input, level=1)
    shard_fields_by_name.update(
        input_sha256=hashlib.sha256(tampered_input).hexdigest(),
        artifact_sha256=hashlib.sha256(tampered_compressed).hexdigest(),
        artifact_byte_count=len(tampered_compressed),
        first_identity_json=proof_record_fields[:3],
        last_identity_json=proof_record_fields[:3],
    )

    with pytest.raises(
        ProviderDirectoryProofStoreError,
        match="record shape changed",
    ):
        proof_store._validated_shard_lines(
            shard_fields_by_name,
            tampered_compressed,
        )
