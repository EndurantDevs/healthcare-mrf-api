# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime as dt
import hashlib
import json
from typing import Any
from unittest.mock import Mock

import pytest

from db.models import (
    ProviderDirectoryLocation,
    ProviderDirectoryOrganization,
    ProviderDirectoryOrganizationAffiliation,
    ProviderDirectoryPractitionerRole,
)
from process.provider_directory_dataset_rehydrate import (
    DatasetRehydrationError,
    RehydrationCheckpoint,
    RehydrationRequest,
    RehydrationRuntime,
    _is_proof_complete,
    _validate_payload,
    rehydrate_current_dataset,
)
from process import provider_directory_dataset_rehydrate_scope as rehydrate_scope
from process import provider_directory_proof_store as proof_store
from process.provider_directory_dataset_rehydrate_types import ResourceProof
from process.provider_directory_proof_store import (
    PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY,
    ProviderDirectoryStoredProofOptions,
    build_stored_dataset_proof,
)
from process.provider_directory_resource_hash import (
    LEGACY_RESOURCE_HASH_CONTRACT,
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    canonical_practitioner_payload,
    resource_payload_sha256_for_contract,
)
from process.provider_directory_fhir_subset_completion import (
    canonical_payload_sha256 as subset_payload_sha256,
)
from process.provider_directory_resource_hash import resource_content_hash_payload
from tests.test_provider_directory_dataset_rehydrate_boundaries import (
    _request as _scope_request,
    _scope_record,
)


_SEMANTIC_SOURCE_ID = "pdfhir_directory_fixture"
_SEMANTIC_ENDPOINT_ID = "endpoint_directory_fixture"
_SEMANTIC_DATASET_ID = "pdds_directory_fixture"
_SEMANTIC_ROOT_RUN_ID = "run_directory_fixture_root"
_SEMANTIC_PROJECTION_AS_OF = "2026-08-09"


def _payload() -> dict[str, object]:
    return {
        "resource_id": "location-1",
        "status": "active",
        "name": "Example Clinic",
        "city_name": "Louisville",
    }


def _hash(mapped_payload: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(mapped_payload, sort_keys=True).encode()
    ).hexdigest()


def _semantic_practitioner_payload_by_field() -> dict[str, object]:
    """Return one canonical v3 Practitioner with two exact source names."""
    return canonical_practitioner_payload(
        {
            "resource_id": "practitioner-1",
            "npi": 1000000001,
            "names": [
                {
                    "use": "official", "family": "Example",
                    "given": ["Alex"], "text": "Alex Example",
                },
                {
                    "use": "usual", "family": "Sample",
                    "given": ["A."], "text": "A. Sample",
                },
            ],
            "fhir_meta": {
                "versionId": "7",
                "lastUpdated": "2026-08-09T12:00:00Z",
                "source": "https://directory.fixture.test/fhir",
            },
            "resource_url": "https://directory.fixture.test/fhir/Practitioner/practitioner-1",
            "fhir_self_url": "https://directory.fixture.test/fhir/Practitioner/practitioner-1",
            "fhir_fetch_url": "https://directory.fixture.test/fhir/Practitioner?page=1",
            "fhir_fetch_mode": "rest_bundle",
        }
    )


def _semantic_retained_resources() -> tuple[
    dict[str, object], str, tuple[dict[str, object], ...]
]:
    """Build the exact Practitioner and linked Organization retained rows."""
    practitioner_payload_by_field = _semantic_practitioner_payload_by_field()
    practitioner_hash = resource_payload_sha256_for_contract(
        practitioner_payload_by_field, SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
    )
    organization_payload_by_field = {
        "resource_id": "organization-linked-1",
        "active": True,
        "name": "Linked Organization",
        "npi": 1234567890,
    }
    organization_hash = resource_payload_sha256_for_contract(
        organization_payload_by_field, SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
    )
    retained_resources = (
        {
            "dataset_id": _SEMANTIC_DATASET_ID,
            "resource_type": "Practitioner",
            "resource_id": practitioner_payload_by_field["resource_id"],
            "payload_hash": practitioner_hash,
            "payload_json": practitioner_payload_by_field,
            "acquired_resource_sha256": None,
        },
        {
            "dataset_id": _SEMANTIC_DATASET_ID,
            "resource_type": "Organization",
            "resource_id": organization_payload_by_field["resource_id"],
            "payload_hash": organization_hash,
            "payload_json": organization_payload_by_field,
            "acquired_resource_sha256": None,
        },
    )
    return practitioner_payload_by_field, practitioner_hash, retained_resources


async def _insert_semantic_retained_resources(
    database: Any,
    schema: str,
    retained_resources: tuple[dict[str, object], ...],
) -> None:
    """Replace legacy retained rows with the exact semantic fixture rows."""
    await database.status(
        f"DELETE FROM {schema}.provider_directory_dataset_resource "
        "WHERE dataset_id=:dataset_id;",
        dataset_id=_SEMANTIC_DATASET_ID,
    )
    for retained_resource_by_field in retained_resources:
        await database.status(
            f"INSERT INTO {schema}.provider_directory_dataset_resource ("
            "dataset_id, resource_type, resource_id, payload_hash, payload_json) "
            "VALUES (:dataset_id, :resource_type, :resource_id, :payload_hash, "
            "CAST(:payload_json AS json));",
            **{
                **retained_resource_by_field,
                "payload_json": json.dumps(
                    retained_resource_by_field["payload_json"], sort_keys=True
                ),
            },
        )


async def _persist_semantic_proof_shards(
    database: Any,
    schema: str,
    retained_resources: tuple[dict[str, object], ...],
) -> None:
    """Persist one exact proof shard per retained resource family."""
    for retained_resource_by_field in retained_resources:
        descriptor_by_field, compressed_payload_bytes = (
            proof_store.build_dataset_proof_shard(
                [retained_resource_by_field],
                dataset_id=_SEMANTIC_DATASET_ID,
                endpoint_id=_SEMANTIC_ENDPOINT_ID,
                acquisition_root_run_id=_SEMANTIC_ROOT_RUN_ID,
                source_ids=[_SEMANTIC_SOURCE_ID],
                resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
            )
        )
        await database.status(
            f'INSERT INTO "{schema}".'
            f'"{proof_store.PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}" ('
            "dataset_id, shard_id, endpoint_id, acquisition_root_run_id, "
            "source_ids_json, resource_count, resource_counts_json, "
            "first_identity_json, last_identity_json, input_sha256, "
            "artifact_sha256, artifact_byte_count, payload_bytes) VALUES ("
            ":dataset_id, :shard_id, :endpoint_id, :acquisition_root_run_id, "
            "CAST(:source_ids_json AS jsonb), :resource_count, "
            "CAST(:resource_counts_json AS jsonb), "
            "CAST(:first_identity_json AS jsonb), "
            "CAST(:last_identity_json AS jsonb), :input_sha256, "
            ":artifact_sha256, :artifact_byte_count, :payload_bytes);",
            **proof_store._proof_shard_insert_params(
                descriptor_by_field, compressed_payload_bytes
            ),
        )


async def _seal_semantic_dataset(database: Any, schema: str) -> None:
    """Seal exact linked-family proof metadata onto the fixture parent."""
    stored_proof = await build_stored_dataset_proof(
        database,
        schema,
        dataset_id=_SEMANTIC_DATASET_ID,
        endpoint_id=_SEMANTIC_ENDPOINT_ID,
        acquisition_root_run_id=_SEMANTIC_ROOT_RUN_ID,
        source_ids=[_SEMANTIC_SOURCE_ID],
        selected_resources=["Practitioner"],
        options=ProviderDirectoryStoredProofOptions(
            proof_resource_scope=["Organization", "Practitioner"],
            expected_resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
            expected_semantic_projection_as_of=_SEMANTIC_PROJECTION_AS_OF,
        ),
    )
    metadata_by_field = {
        "selected_resources": ["Practitioner"],
        "source_ids": [_SEMANTIC_SOURCE_ID],
        "resource_hash_contract": SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        "semantic_projection_as_of": _SEMANTIC_PROJECTION_AS_OF,
        "proof_resource_scope": ["Organization", "Practitioner"],
        PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY: stored_proof.metadata,
    }
    await database.status(
        f"UPDATE {schema}.provider_directory_endpoint_dataset SET "
        "dataset_hash=:dataset_hash, resource_count=2, "
        "publication_metadata_json=CAST(:metadata_json AS json) "
        "WHERE dataset_id=:dataset_id;",
        dataset_id=_SEMANTIC_DATASET_ID,
        dataset_hash=stored_proof.dataset_hash,
        metadata_json=json.dumps(metadata_by_field),
    )


async def _replace_fixture_with_semantic_practitioner(
    database: Any, schema: str
) -> tuple[dict[str, object], str]:
    """Replace retained fixture content with one sealed v3 Practitioner."""
    practitioner_payload_by_field, practitioner_hash, retained_resources = (
        _semantic_retained_resources()
    )
    await _insert_semantic_retained_resources(database, schema, retained_resources)
    await _persist_semantic_proof_shards(database, schema, retained_resources)
    await _seal_semantic_dataset(database, schema)
    return practitioner_payload_by_field, practitioner_hash


def test_retained_payload_accepts_exact_mapped_typed_shape():
    payload = _payload()
    assert _validate_payload(
        ProviderDirectoryLocation,
        "location-1",
        _hash(payload),
        payload,
        resource_hash_contract=LEGACY_RESOURCE_HASH_CONTRACT,
    ) is None


def test_retained_payload_is_validated_under_exact_dataset_contract():
    payload = {
        **_payload(),
        "resource_url": "https://directory.example.test/Location/location-1",
    }
    legacy_hash = resource_payload_sha256_for_contract(
        payload,
        LEGACY_RESOURCE_HASH_CONTRACT,
    )
    neutral_hash = resource_payload_sha256_for_contract(
        payload,
        TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    )
    assert legacy_hash != neutral_hash

    assert _validate_payload(
        ProviderDirectoryLocation,
        "location-1",
        neutral_hash,
        payload,
        resource_hash_contract=TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    ) is None
    assert _validate_payload(
        ProviderDirectoryLocation,
        "location-1",
        neutral_hash,
        payload,
        resource_hash_contract=LEGACY_RESOURCE_HASH_CONTRACT,
    ) == "payload_hash_mismatch"


def test_scope_decodes_exact_persisted_hash_contract_and_projection_date(
    monkeypatch,
):
    v3_scope_record = _scope_record()
    v3_scope_record["publication_metadata_json"].update(
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        semantic_projection_as_of="2026-08-09",
        proof_resource_scope=["Location"],
        provider_directory_content_proof_v1={"sealed": True},
    )
    validate_proof = Mock(return_value={"resource_counts": {"Location": 1}})
    monkeypatch.setattr(
        rehydrate_scope, "validate_stored_dataset_proof_metadata", validate_proof
    )
    v3_scope = rehydrate_scope._decode_dataset_scope(_scope_request(), v3_scope_record)
    assert v3_scope.resource_hash_contract == SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
    assert v3_scope.semantic_projection_as_of == "2026-08-09"
    assert v3_scope.resource_types == ("Location",)
    proof_options = validate_proof.call_args.kwargs["options"]
    assert proof_options.proof_resource_scope == ("Location",)

    v2_scope_record = _scope_record()
    v2_scope_record["publication_metadata_json"]["resource_hash_contract"] = (
        TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT
    )
    v2_scope = rehydrate_scope._decode_dataset_scope(
        _scope_request(), v2_scope_record
    )
    assert v2_scope.resource_hash_contract == TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT
    assert v2_scope.semantic_projection_as_of is None


@pytest.mark.parametrize(
    "invalid_projection_as_of",
    [None, "2026-8-9", " 2026-08-09", dt.date(2026, 8, 9)],
)
def test_scope_rejects_invalid_v3_projection_date(invalid_projection_as_of):
    v3_scope_record = _scope_record()
    v3_scope_record["publication_metadata_json"].update(
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        semantic_projection_as_of=invalid_projection_as_of,
    )
    with pytest.raises(DatasetRehydrationError, match="hash_identity_invalid"):
        rehydrate_scope._decode_dataset_scope(_scope_request(), v3_scope_record)


def test_scope_rejects_noncanonical_v3_proof_resource_scope():
    v3_scope_record = _scope_record()
    v3_scope_record["publication_metadata_json"].update(
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        semantic_projection_as_of="2026-08-09",
        proof_resource_scope=["Practitioner", "Location"],
        provider_directory_content_proof_v1={"sealed": True},
    )
    with pytest.raises(DatasetRehydrationError, match="proof_scope_invalid"):
        rehydrate_scope._decode_dataset_scope(_scope_request(), v3_scope_record)


def test_retained_subset_payload_uses_its_reviewed_canonical_hash():
    mapped_payload_by_field = {
        **_payload(),
        "resource_url": "https://directory.example.test/Location/location-1",
    }
    acquired_sha256 = "a" * 64
    subset_hash = subset_payload_sha256(
        resource_content_hash_payload(mapped_payload_by_field)
    )

    assert _validate_payload(
        ProviderDirectoryLocation,
        "location-1",
        subset_hash,
        mapped_payload_by_field,
        resource_hash_contract=TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
        acquired_resource_sha256=acquired_sha256,
    ) is None
    assert _validate_payload(
        ProviderDirectoryLocation,
        "location-1",
        subset_hash,
        mapped_payload_by_field,
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        acquired_resource_sha256="bad",
    ) == "payload_hash_mismatch"
    assert _validate_payload(
        ProviderDirectoryLocation,
        "location-1",
        subset_hash,
        mapped_payload_by_field,
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        acquired_resource_sha256=acquired_sha256,
    ) == "payload_hash_mismatch"


@pytest.mark.parametrize(
    ("model", "resource_id", "payload"),
    (
        (
            ProviderDirectoryOrganization,
            "uhc-facility",
            {
                "resource_id": "uhc-facility",
                "npi": 1000000491,
                "tax_id": None,
                "tin_status": "unavailable_from_uhc_source",
                "name": "Example UHC Facility",
                "type_codes": ["Clinic"],
                "address_json": [{"city": "Chicago"}],
                "source_lineage": {
                    "source_file_id": "f" * 64,
                    "record_ordinal": 17,
                },
            },
        ),
        (
            ProviderDirectoryOrganizationAffiliation,
            "uhc-membership",
            {
                "resource_id": "uhc-membership",
                "organization_ref": None,
                "participating_organization_ref": (
                    "Organization/uhc-facility"
                ),
                "insurance_plan_refs": ["InsurancePlan/uhc-plan"],
                "plan_scope": {"plan_id": "12345IL0010001"},
                "network_tier": "PREFERRED",
                "network_key_id": "a" * 64,
                "relationship_type": (
                    "payer_reported_provider_plan_membership"
                ),
                "ownership_status": "not_asserted",
                "source_lineage": {
                    "source_file_id": "f" * 64,
                    "record_ordinal": 17,
                },
            },
        ),
        (
            ProviderDirectoryPractitionerRole,
            "uhc-role",
            {
                "resource_id": "uhc-role",
                "insurance_plan_refs": ["InsurancePlan/uhc-plan"],
                "plan_scope": {"plan_id": "12345IL0010001"},
                "network_tier": "PREFERRED",
                "network_key_id": "a" * 64,
            },
        ),
    ),
)
def test_uhc_retained_payload_accepts_organization_plan_evidence(
    model,
    resource_id,
    payload,
):
    assert _validate_payload(
        model,
        resource_id,
        _hash(payload),
        payload,
        resource_hash_contract=LEGACY_RESOURCE_HASH_CONTRACT,
    ) is None


@pytest.mark.parametrize(
    "payload, expected",
    [
        ({"resource_id": "location-1", "unknown": "value"}, "payload_unknown_field"),
        ({"resource_id": "location-1", "source_id": "wrong"}, "payload_provenance_invalid"),
        ({"resource_id": "other"}, "payload_provenance_invalid"),
    ],
)
def test_retained_payload_rejects_wrong_shape_or_provenance(payload, expected):
    assert _validate_payload(
        ProviderDirectoryLocation,
        "location-1",
        _hash(payload),
        payload,
        resource_hash_contract=LEGACY_RESOURCE_HASH_CONTRACT,
    ) == expected


def test_exact_proof_rejects_rows_outside_dataset_membership():
    checkpoint_record = RehydrationCheckpoint(
        state="complete", last_resource_id="location-2", expected_count=2,
        input_count=2, mapped_count=2, rejected_count=0,
    )
    resource_proof = ResourceProof(
        input_count=2, typed_count=2, typed_extra_count=1,
        canonical_hash_count=2, canonical_extra_count=0, source_edge_count=2,
        source_edge_extra_count=0,
    )
    assert not _is_proof_complete(checkpoint_record, resource_proof)


@pytest.mark.asyncio
async def test_rehydrate_rejects_unbounded_batch_before_database_access():
    with pytest.raises(DatasetRehydrationError, match="batch_size_invalid"):
        runtime = RehydrationRuntime(object(), "mrf", {}, None)
        request = RehydrationRequest(
            source_id="source", dataset_id="dataset",
            acquisition_root_run_id="root", owner_run_id="owner",
            batch_size=25_001,
        )
        await rehydrate_current_dataset(runtime, request)
