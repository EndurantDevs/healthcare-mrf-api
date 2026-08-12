# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Semantic v4 Organization name-state and proof contracts."""

from __future__ import annotations

import hashlib
import importlib
import json

import pytest

from db.models import ProviderDirectoryOrganization
from process import provider_directory_proof_store as proof_store
from process.provider_directory_admission_seal import (
    admission_seal_from_validated_metadata,
)
from process.provider_directory_organization_hash import (
    canonical_organization_payload,
    merge_organization_semantic_payloads,
    organization_label_hashes,
    organization_primary_name_hashes,
    organization_semantic_base_sha256,
    organization_semantic_payload_sha256,
)
from process.provider_directory_proof_store import (
    PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY,
    PROVIDER_DIRECTORY_SEMANTIC_CONTENT_V4_PROOF_CONTRACT_ID,
    ProviderDirectoryProofStoreError,
    ProviderDirectoryStoredProofOptions,
)
from process.provider_directory_resource_hash import (
    DEFAULT_RESOURCE_HASH_CONTRACT,
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
    is_resource_payload_hash_match,
    resource_payload_sha256_for_contract,
)
from tests.test_provider_directory_proof_store import (
    DATASET_ID as PROOF_DATASET_ID,
    ENDPOINT_ID as PROOF_ENDPOINT_ID,
    ROOT_RUN_ID as PROOF_ROOT_RUN_ID,
    SOURCE_IDS as PROOF_SOURCE_IDS,
    _MemoryProofConnection,
)


importer = importlib.import_module("process.provider_directory_fhir")


def _organization_payload(
    name: str,
    *,
    aliases: list[str] | None = None,
    host: str = "a.example.test",
) -> dict[str, object]:
    """Return one complete mapped Organization observation."""

    return {
        "resource_id": "organization-a",
        "resource_url": f"https://{host}/Organization/organization-a",
        "fhir_meta": {
            "versionId": "1",
            "source": "https://directory.example.test/fhir",
            "lastUpdated": f"2026-08-10T00:00:0{1 if host[0] == 'a' else 2}Z",
        },
        "fhir_self_url": None,
        "fhir_fetch_url": f"https://{host}/Organization?_count=100",
        "fhir_fetch_mode": "rest_bundle",
        "npi": None,
        "tax_id": None,
        "tin_status": None,
        "active": True,
        "identifiers": [{"system": "urn:example", "value": "organization-a"}],
        "name": name,
        "aliases": aliases or [],
        "type_codes": [],
        "telecom": [],
        "address_json": [],
        "contacts": [],
        "part_of_ref": None,
        "endpoint_refs": [],
        "source_lineage": None,
    }


def _resource_row(payload_by_field: dict[str, object]) -> dict[str, object]:
    """Attach transient typed-row provenance to one mapped payload."""

    return {
        **payload_by_field,
        "source_id": "source-a",
        "last_seen_run_id": "run-a",
        "observed_at": None,
        "updated_at": None,
    }


def _dataset_row(
    payload_by_field: dict[str, object],
    resource_hash_contract: str,
) -> dict[str, object]:
    """Build one retained row under the exact requested contract."""

    return {
        "dataset_id": "dataset-a",
        "resource_type": "Organization",
        "resource_id": payload_by_field["resource_id"],
        "payload_hash": resource_payload_sha256_for_contract(
            payload_by_field,
            resource_hash_contract,
            resource_type="Organization",
        ),
        "payload_json": payload_by_field,
        "acquired_resource_sha256": None,
    }


def _v4_proof_connection() -> _MemoryProofConnection:
    """Return one mutable in-memory parent with exact v4 lineage."""

    connection = _MemoryProofConnection()
    connection.parent["publication_metadata_json"] = {
        "source_ids": PROOF_SOURCE_IDS,
        "selected_resources": ["Organization"],
        "proof_resource_scope": ["Organization"],
        "resource_hash_contract": SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
        "semantic_projection_as_of": "2026-08-10",
    }
    return connection


def test_v4_preserves_unique_primary_over_earlier_alias() -> None:
    """Keep a source primary scalar even when an alias sorts before it."""

    canonical = canonical_organization_payload(
        _organization_payload(
            "Zeta Community Health",
            aliases=["Alpha Community Alias", "Zeta Community Health"],
        )
    )
    assert canonical["name"] == "Zeta Community Health"
    assert canonical["name_variants"] == ["Zeta Community Health"]
    assert canonical["aliases"] == ["Alpha Community Alias"]


def test_v4_union_is_lossless_associative_and_order_independent() -> None:
    """Retain every exact primary and alias regardless of observation order."""

    first = _organization_payload(
        "Community Health Center",
        aliases=["Regional Clinic"],
        host="a.example.test",
    )
    second = _organization_payload(
        "COMMUNITY HEALTH SERVICES",
        aliases=["Outpatient Center", "Regional Clinic"],
        host="z.example.test",
    )
    forward = merge_organization_semantic_payloads(first, second)
    reverse = merge_organization_semantic_payloads(second, first)
    assert forward == reverse
    assert forward["name"] == "Community Health Center"
    assert forward["name_variants"] == [
        "Community Health Center",
        "COMMUNITY HEALTH SERVICES",
    ]
    assert forward["aliases"] == [
        "COMMUNITY HEALTH SERVICES",
        "Outpatient Center",
        "Regional Clinic",
    ]
    assert forward["resource_url"].startswith("https://z.example.test/")
    assert merge_organization_semantic_payloads(forward, first) == forward
    assert organization_semantic_payload_sha256(forward) == (
        organization_semantic_payload_sha256(reverse)
    )


def test_v4_primary_alias_overlap_has_proof_parity() -> None:
    """Commit a label once even when observations disagree on its role."""

    primary = _organization_payload(
        "Community Health Center",
        aliases=["Regional Clinic"],
    )
    alias = _organization_payload(
        "Regional Clinic",
        aliases=["Community Health Center"],
    )
    merged = merge_organization_semantic_payloads(primary, alias)
    merged_components = organization_label_hashes(merged)
    observed_components = sorted(
        set(organization_label_hashes(primary))
        | set(organization_label_hashes(alias))
    )
    assert list(merged_components) == observed_components
    assert merged["name_variants"] == [
        "Community Health Center",
        "Regional Clinic",
    ]


def test_v4_rejects_non_name_drift_and_noncanonical_projection() -> None:
    """Permit label union only; bind the exact deterministic projection."""

    first = _organization_payload("Community Health Center")
    drifted_payload_by_field = {
        **first,
        "active": False,
        "name": "Community Health Group",
    }
    with pytest.raises(ValueError, match="identity_payload_conflict"):
        merge_organization_semantic_payloads(first, drifted_payload_by_field)
    invalid_aliases_by_field = {**first, "aliases": ("Alias",)}
    with pytest.raises(ValueError, match="organization_names_invalid"):
        canonical_organization_payload(invalid_aliases_by_field)
    noncanonical_payload_by_field = {
        **canonical_organization_payload(first),
        "name_variants": ["Second Name", "Community Health Center"],
        "name": "Second Name",
        "aliases": ["Community Health Center"],
    }
    with pytest.raises(ValueError, match="name_projection_invalid"):
        organization_semantic_payload_sha256(noncanonical_payload_by_field)


def test_v4_hash_is_typed_and_v3_stays_immutable() -> None:
    """Dispatch Organization composition only under the new exact contract."""

    raw_payload = _organization_payload("Community Health Center")
    canonical_payload = canonical_organization_payload(raw_payload)
    v3_hash = resource_payload_sha256_for_contract(
        raw_payload,
        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    )
    v4_hash = resource_payload_sha256_for_contract(
        canonical_payload,
        SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
        resource_type="Organization",
    )
    assert DEFAULT_RESOURCE_HASH_CONTRACT == (
        SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
    )
    assert v4_hash == organization_semantic_payload_sha256(canonical_payload)
    assert v4_hash != v3_hash
    assert not is_resource_payload_hash_match(
        canonical_payload,
        v4_hash,
        "Organization",
    )
    with pytest.raises(ValueError, match="resource_payload_conflict"):
        importer._endpoint_dataset_resource_rows(
            ProviderDirectoryOrganization,
            [
                _resource_row(raw_payload),
                _resource_row({**raw_payload, "name": "Second Name"}),
            ],
            dataset_id="dataset-v3",
            resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        )


def test_v4_batch_unions_while_v3_record_shape_remains_readable() -> None:
    """Keep old v3 Organization proof records strict and add v4 composition."""

    first = _organization_payload("Community Health Center")
    second_payload_by_field = {
        **first,
        "name": "Community Health Group",
    }
    retained_rows = importer._endpoint_dataset_resource_rows(
        ProviderDirectoryOrganization,
        [_resource_row(second_payload_by_field), _resource_row(first)],
        dataset_id="dataset-v4",
        resource_hash_contract=SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
    )
    merged_payload = retained_rows[0]["payload_json"]
    assert merged_payload["name_variants"] == [
        "Community Health Center",
        "Community Health Group",
    ]
    v4_record = proof_store._proof_record(
        retained_rows[0],
        SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
    )
    raw_v3_row = _dataset_row(
        first,
        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    )
    v3_record = proof_store._proof_record(
        raw_v3_row,
        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    )
    assert v3_record[7:] == [
        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        raw_v3_row["payload_hash"],
        [],
    ]
    assert v4_record[7] == SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
    assert v4_record[8] == organization_semantic_base_sha256(merged_payload)
    assert v4_record[9] == list(organization_label_hashes(merged_payload))
    assert v4_record[10] == list(
        organization_primary_name_hashes(merged_payload)
    )
    _merged_record, diagnostics_by_name = (
        proof_store._finalized_proof_record_group([v4_record])
    )
    assert diagnostics_by_name == {
        "added_name_count": 1,
        "collision_identities": 1,
        "observation_variants": 2,
        "union_name_count": 2,
    }
    assert proof_store._decoded_record(json.dumps(v3_record).encode()) == v3_record
    assert proof_store._decoded_record(json.dumps(v4_record).encode()) == v4_record


def test_v4_proof_composes_labels_and_rejects_mixed_contracts() -> None:
    """Compose same-base v4 observations but reject v3/v4 shard mixing."""

    first = canonical_organization_payload(
        _organization_payload("Community Health Center")
    )
    second = canonical_organization_payload(
        _organization_payload("Community Health Group")
    )
    first_record = proof_store._proof_record(
        _dataset_row(first, SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT),
        SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
    )
    second_record = proof_store._proof_record(
        _dataset_row(second, SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT),
        SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
    )
    merged_record, diagnostics = proof_store._finalized_proof_record_group(
        [second_record, first_record]
    )
    assert merged_record[9] == sorted(
        set(first_record[9]) | set(second_record[9])
    )
    assert merged_record[10] == sorted(
        set(first_record[10]) | set(second_record[10])
    )
    assert diagnostics["collision_identities"] == 1
    v3_record = proof_store._proof_record(
        _dataset_row(
            _organization_payload("Community Health Center"),
            SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        ),
        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    )
    with pytest.raises(proof_store.ProviderDirectoryProofStoreError, match="conflict"):
        proof_store._finalized_proof_record_group([v3_record, first_record])


async def _stored_v4_union_proof():
    """Build one sealed two-observation Organization proof."""

    connection = _v4_proof_connection()
    first = canonical_organization_payload(
        _organization_payload(
            "Community Health Center",
            aliases=["Regional Clinic"],
        )
    )
    second = canonical_organization_payload(
        _organization_payload(
            "Regional Clinic",
            aliases=["Community Health Center"],
        )
    )
    for payload_by_field in (first, second):
        await proof_store.persist_dataset_proof_shard(
            connection,
            "mrf",
            [
                {
                    **_dataset_row(
                        payload_by_field,
                        SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
                    ),
                    "dataset_id": PROOF_DATASET_ID,
                }
            ],
            dataset_id=PROOF_DATASET_ID,
            expected_resource_hash_contract=(
                SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
            ),
        )
    stored_proof = await proof_store.build_stored_dataset_proof(
        connection,
        "mrf",
        dataset_id=PROOF_DATASET_ID,
        endpoint_id=PROOF_ENDPOINT_ID,
        acquisition_root_run_id=PROOF_ROOT_RUN_ID,
        source_ids=PROOF_SOURCE_IDS,
        selected_resources=["Organization"],
        options=ProviderDirectoryStoredProofOptions(
            proof_resource_scope=["Organization"],
            expected_resource_hash_contract=(
                SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
            ),
            expected_semantic_projection_as_of="2026-08-10",
        ),
    )
    merged = merge_organization_semantic_payloads(first, second)
    return stored_proof, organization_semantic_payload_sha256(merged)


@pytest.mark.asyncio
async def test_v4_sealed_proof_binds_union_and_contract() -> None:
    """Seal the reduced Organization union only under the v4 envelope."""

    stored_proof, merged_hash = await _stored_v4_union_proof()
    expected_identity = proof_store._stable_json(
        ["Organization", "organization-a", merged_hash]
    ).encode()

    assert stored_proof.dataset_hash == hashlib.sha256(
        expected_identity
    ).hexdigest()
    assert stored_proof.metadata["contract_id"] == (
        PROVIDER_DIRECTORY_SEMANTIC_CONTENT_V4_PROOF_CONTRACT_ID
    )
    assert stored_proof.metadata["resource_hash_contract"] == (
        SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
    )
    assert stored_proof.metadata["semantic_union"] == {
        "added_name_count": 1,
        "collision_identities": 1,
        "observation_variants": 2,
        "union_name_count": 2,
    }
    with pytest.raises(
        ProviderDirectoryProofStoreError,
        match="semantic proof contract changed",
    ):
        proof_store.validate_stored_dataset_proof_metadata(
            stored_proof.metadata,
            dataset_id=PROOF_DATASET_ID,
            endpoint_id=PROOF_ENDPOINT_ID,
            acquisition_root_run_id=PROOF_ROOT_RUN_ID,
            source_ids=PROOF_SOURCE_IDS,
            selected_resources=["Organization"],
            options=ProviderDirectoryStoredProofOptions(
                proof_resource_scope=["Organization"],
                expected_resource_hash_contract=(
                    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
                ),
                expected_semantic_projection_as_of="2026-08-10",
            ),
        )


@pytest.mark.asyncio
async def test_v4_union_proof_admits_deduplicated_shards() -> None:
    """Admit a validated semantic union with duplicate observations."""

    stored_proof, _merged_hash = await _stored_v4_union_proof()
    proof = stored_proof.metadata

    assert sum(shard["resource_count"] for shard in proof["shards"]) == 2
    assert proof["resource_count"] == 1
    receipt = admission_seal_from_validated_metadata(
        {PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY: proof}
    )
    assert receipt is not None
    assert receipt.proof_sha256 == proof["proof_sha256"]


def test_v4_model_exposes_retained_primary_variants() -> None:
    """Keep the lossless primary-name state available to typed consumers."""

    assert "name_variants" in ProviderDirectoryOrganization.__table__.c
