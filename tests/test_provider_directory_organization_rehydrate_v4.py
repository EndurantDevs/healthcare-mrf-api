# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Rehydration contracts for semantic-v4 Organization name unions."""

from __future__ import annotations

import datetime as dt
from unittest.mock import Mock

import pytest

from db.models import ProviderDirectoryOrganization
from process import provider_directory_dataset_rehydrate as rehydrate
from process import provider_directory_dataset_rehydrate_scope as rehydrate_scope
from process.provider_directory_dataset_rehydrate_types import (
    DatasetScope,
    RehydrationRequest,
)
from process.provider_directory_organization_hash import (
    canonical_organization_payload,
)
from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
    resource_payload_sha256_for_contract,
)


PROJECTION_AS_OF = "2026-08-10"


def _organization_payload() -> dict[str, object]:
    """Return one canonical v4 Organization union payload."""

    return canonical_organization_payload(
        {
            "resource_id": "organization-a",
            "active": True,
            "name": "Community Health Center",
            "name_variants": [
                "Community Health Center",
                "COMMUNITY HEALTH SERVICES",
            ],
            "aliases": [
                "COMMUNITY HEALTH SERVICES",
                "Regional Clinic",
            ],
        }
    )


def _dataset_scope() -> DatasetScope:
    """Return one exact v4 rehydration fence."""

    return DatasetScope(
        source_id="source-a",
        dataset_id="dataset-a",
        acquisition_root_run_id="root-a",
        endpoint_id="endpoint-a",
        canonical_api_base="https://directory.example.test/fhir",
        dataset_hash="a" * 64,
        resource_count=1,
        resource_types=("Organization",),
        resource_hash_contract=SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
        semantic_projection_as_of=PROJECTION_AS_OF,
        publication_metadata_hash="b" * 64,
        published_at=dt.datetime(2026, 8, 10, tzinfo=dt.UTC),
    )


def test_v4_rehydrate_restores_exact_organization_union() -> None:
    """Validate and restore the canonical primary-name state exactly."""

    payload_by_field = _organization_payload()
    payload_hash = resource_payload_sha256_for_contract(
        payload_by_field,
        SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
        resource_type="Organization",
    )
    retained_batch = rehydrate._map_retained_batch(
        ProviderDirectoryOrganization,
        [
            {
                "resource_id": "organization-a",
                "payload_hash": payload_hash,
                "payload_json": payload_by_field,
                "acquired_resource_sha256": None,
            }
        ],
        _dataset_scope(),
        "Organization",
    )

    assert retained_batch.rejection_reasons == ()
    assert retained_batch.typed_rows[0]["name"] == "Community Health Center"
    assert retained_batch.typed_rows[0]["name_variants"] == [
        "Community Health Center",
        "COMMUNITY HEALTH SERVICES",
    ]
    assert retained_batch.typed_rows[0]["aliases"] == [
        "COMMUNITY HEALTH SERVICES",
        "Regional Clinic",
    ]


def test_v4_rehydrate_rejects_tamper_and_missing_type() -> None:
    """Require the exact typed hash dispatcher before any row is restored."""

    payload_by_field = _organization_payload()
    payload_hash = resource_payload_sha256_for_contract(
        payload_by_field,
        SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
        resource_type="Organization",
    )
    tampered_payload_by_field = {
        **payload_by_field,
        "aliases": [*payload_by_field["aliases"], "Unobserved Alias"],
    }
    assert rehydrate._validate_payload(
        ProviderDirectoryOrganization,
        "organization-a",
        payload_hash,
        tampered_payload_by_field,
        resource_hash_contract=SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
        resource_type="Organization",
    ) == "payload_hash_mismatch"
    assert rehydrate._validate_payload(
        ProviderDirectoryOrganization,
        "organization-a",
        payload_hash,
        payload_by_field,
        resource_hash_contract=SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
    ) == "payload_hash_mismatch"


def test_preunion_v3_organization_remains_readable() -> None:
    """Keep generic v3 Organization hashes independent of v4 name state."""

    payload_by_field = {
        "resource_id": "organization-a",
        "active": True,
        "name": "Community Health Center",
        "aliases": ["Regional Clinic"],
    }
    payload_hash = resource_payload_sha256_for_contract(
        payload_by_field,
        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    )
    assert rehydrate._validate_payload(
        ProviderDirectoryOrganization,
        "organization-a",
        payload_hash,
        payload_by_field,
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        resource_type="Organization",
    ) is None


def test_v4_scope_binds_proof_families_and_projection(monkeypatch) -> None:
    """Decode v4 with the same sealed scope/date fence as v3."""

    validate_proof = Mock(
        return_value={
            "resource_counts": {"Location": 1, "Organization": 1}
        }
    )
    monkeypatch.setattr(
        rehydrate_scope,
        "validate_stored_dataset_proof_metadata",
        validate_proof,
    )
    request = RehydrationRequest(
        source_id="source-a",
        dataset_id="dataset-a",
        acquisition_root_run_id="root-a",
        owner_run_id="owner-a",
    )
    metadata_by_field = {
        "proof_resource_scope": ["Location", "Organization"],
        "provider_directory_content_proof_v1": {"sealed": True},
    }
    retained_types = rehydrate_scope._retained_resource_types(
        request,
        {"endpoint_id": "endpoint-a"},
        metadata_by_field,
        ["Organization"],
        ["source-a"],
        SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
        PROJECTION_AS_OF,
    )

    assert retained_types == ("Location", "Organization")
    options = validate_proof.call_args.kwargs["options"]
    assert options.expected_resource_hash_contract == (
        SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
    )
    assert options.expected_semantic_projection_as_of == PROJECTION_AS_OF
    assert rehydrate_scope._semantic_projection_as_of(
        {"semantic_projection_as_of": PROJECTION_AS_OF},
        SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
    ) == PROJECTION_AS_OF


@pytest.mark.parametrize("resource_hash_contract", (None, "unknown"))
def test_v4_hash_call_requires_exact_contract(resource_hash_contract) -> None:
    """Keep malformed rehydration identities fail closed."""

    assert rehydrate._validate_payload(
        ProviderDirectoryOrganization,
        "organization-a",
        "0" * 64,
        _organization_payload(),
        resource_hash_contract=resource_hash_contract,
        resource_type="Organization",
    ) == "payload_hash_mismatch"
