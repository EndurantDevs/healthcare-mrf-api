# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed-contract tests for reviewed acquisition root policy."""

from __future__ import annotations

import copy

import pytest

from process import provider_directory_fhir_subset_identity as subset_identity
from process.provider_directory_fhir_subset_completion import canonical_sha256
from process.provider_directory_fhir_root_policy import (
    DEFAULT_REQUIRED_ROOT_COUNT,
    LEGACY_PENDING_STATUS,
    POLICY_PENDING_STATUS,
    REVIEWED_ROOT_POLICY_METADATA_KEY,
    REVIEWED_ROOT_POLICY_VERSION,
    ReviewedRootPolicy,
    reviewed_root_policy_for_status,
)


def _metadata(required_root_count: object) -> dict[str, object]:
    return {
        REVIEWED_ROOT_POLICY_METADATA_KEY: {
            "policy_version": REVIEWED_ROOT_POLICY_VERSION,
            "required_root_count": required_root_count,
        }
    }


def test_legacy_reviewed_status_defaults_to_two_roots():
    policy = reviewed_root_policy_for_status({}, LEGACY_PENDING_STATUS)

    assert policy == ReviewedRootPolicy(DEFAULT_REQUIRED_ROOT_COUNT)
    assert policy.is_twin_root_required is True


@pytest.mark.parametrize("required_root_count", (1, 2))
def test_policy_status_accepts_only_closed_root_counts(required_root_count):
    policy = reviewed_root_policy_for_status(
        _metadata(required_root_count),
        POLICY_PENDING_STATUS,
    )

    assert policy == ReviewedRootPolicy(required_root_count)
    assert policy.document() == _metadata(required_root_count)[
        REVIEWED_ROOT_POLICY_METADATA_KEY
    ]


@pytest.mark.parametrize(
    "raw_policy",
    (
        None,
        {},
        {"policy_version": REVIEWED_ROOT_POLICY_VERSION},
        {
            "policy_version": REVIEWED_ROOT_POLICY_VERSION,
            "required_root_count": True,
        },
        {
            "policy_version": REVIEWED_ROOT_POLICY_VERSION,
            "required_root_count": 0,
        },
        {
            "policy_version": REVIEWED_ROOT_POLICY_VERSION,
            "required_root_count": 3,
        },
        {
            "policy_version": "unknown",
            "required_root_count": 1,
        },
        {
            "policy_version": REVIEWED_ROOT_POLICY_VERSION,
            "required_root_count": 1,
            "extra": "rejected",
        },
    ),
)
def test_policy_status_rejects_malformed_or_open_documents(raw_policy):
    with pytest.raises(ValueError, match="reviewed_root_policy"):
        reviewed_root_policy_for_status(
            {REVIEWED_ROOT_POLICY_METADATA_KEY: raw_policy},
            POLICY_PENDING_STATUS,
        )


@pytest.mark.parametrize("required_root_count", (1, 2))
def test_legacy_status_rejects_policy_interpretation(required_root_count):
    with pytest.raises(ValueError, match="status_mismatch"):
        reviewed_root_policy_for_status(
            _metadata(required_root_count),
            LEGACY_PENDING_STATUS,
        )


def test_policy_without_reviewed_status_is_rejected():
    with pytest.raises(ValueError, match="status_required"):
        reviewed_root_policy_for_status(_metadata(1), None)


def _source_record() -> dict[str, object]:
    return {
        "source_id": "source-a",
        "endpoint_id": "endpoint-serving",
        "canonical_api_base": "https://directory.example.test/fhir",
        "requires_registration": False,
        "requires_api_key": False,
        "auth_type": "none",
        "metadata_json": {
            subset_identity.CONFIGURED_ENDPOINT_ID_METADATA_FIELD: (
                "endpoint-acquisition"
            ),
        },
    }


def _identity_payloads(source_record):
    return (
        subset_identity.subset_activation_source_contract_payload(
            source_record
        ),
        subset_identity.server_issued_subset_source_scope_payload(
            source_record,
            (source_record["source_id"],),
            "2026-08-09T00:00:00.000000Z",
            source_record["canonical_api_base"],
        ),
    )


def test_policy_identity_is_v2_without_changing_legacy_v1_payload():
    source_record = _source_record()
    legacy_activation, legacy_scope = _identity_payloads(source_record)

    assert legacy_activation["identity_version"] == (
        subset_identity.SERVER_ISSUED_SUBSET_ACTIVATION_SOURCE_CONTRACT_VERSION
    )
    assert legacy_scope["identity_version"] == (
        subset_identity.SERVER_ISSUED_SUBSET_SOURCE_SCOPE_VERSION
    )
    assert all(
        field[0] != REVIEWED_ROOT_POLICY_METADATA_KEY
        for field in legacy_scope["metadata_identity"]
    )

    policy_hashes = []
    for required_root_count in (1, 2):
        policy_source = copy.deepcopy(source_record)
        policy_source["metadata_json"].update(
            _metadata(required_root_count)
        )
        activation_payload, scope_payload = _identity_payloads(
            policy_source
        )
        assert activation_payload["identity_version"] == (
            subset_identity.SERVER_ISSUED_SUBSET_ACTIVATION_SOURCE_CONTRACT_VERSION_V2
        )
        assert scope_payload["identity_version"] == (
            subset_identity.SERVER_ISSUED_SUBSET_SOURCE_SCOPE_VERSION_V2
        )
        assert scope_payload["metadata_identity"][-1] == [
            REVIEWED_ROOT_POLICY_METADATA_KEY,
            True,
            _metadata(required_root_count)[
                REVIEWED_ROOT_POLICY_METADATA_KEY
            ],
        ]
        policy_hashes.append(canonical_sha256(scope_payload))

    assert len(set(policy_hashes)) == 2
    assert canonical_sha256(legacy_scope) not in policy_hashes


def test_policy_identity_rejects_malformed_policy_document():
    source_record = _source_record()
    source_record["metadata_json"][REVIEWED_ROOT_POLICY_METADATA_KEY] = {
        "policy_version": REVIEWED_ROOT_POLICY_VERSION,
        "required_root_count": True,
    }

    with pytest.raises(ValueError, match="activation_source_invalid"):
        subset_identity.subset_activation_source_contract_payload(
            source_record
        )
    with pytest.raises(ValueError, match="source_scope_invalid"):
        subset_identity.server_issued_subset_source_scope_payload(
            source_record,
            (source_record["source_id"],),
            "2026-08-09T00:00:00.000000Z",
            source_record["canonical_api_base"],
        )
