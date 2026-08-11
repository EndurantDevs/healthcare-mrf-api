# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Profile-bound advertised-count drift tests for reviewed FHIR subsets."""

from __future__ import annotations

import copy

import pytest

from process.provider_directory_fhir_subset_completion import (
    build_subset_completion_proof,
    canonical_sha256,
    validate_subset_completion_proof_pair,
)
from process.provider_directory_fhir_subset_identity import (
    SERVER_ISSUED_SUBSET_BOUNDED_COMPLETION_SCOPES,
    SERVER_ISSUED_SUBSET_BOUNDED_STRATEGY_VERSION,
    SERVER_ISSUED_SUBSET_EXACT_COMPLETION_SCOPES,
    SERVER_ISSUED_SUBSET_EXACT_STRATEGY_VERSION,
)
from process.provider_directory_fhir_subset_execution import (
    has_valid_reviewed_subset_counts,
)
from tests.provider_directory_fhir_subset_completion_support import (
    build_execution_proof,
    build_subset_contract,
)


LEGACY_PROOF_SHA256 = (
    "dca7eb48cb08ad7477048b078a00d64e8dac4c27f5399130eca5c1e1976d2327"
)
BOUNDED_PROOF_SHA256 = (
    "3def46f20f794260f03c0f389042ca9026334b2c550f3571e0d8f7359d714a90"
)


@pytest.mark.parametrize("malformed_count", (None, "1", True, -1))
def test_reviewed_count_validation_rejects_malformed_values(malformed_count):
    """Return false instead of raising for a malformed persisted count."""

    count_by_name = {
        "pre_count": 2,
        "post_count": malformed_count,
        "processed_rows": 1,
        "unique_candidate_rows": 1,
    }
    assert not has_valid_reviewed_subset_counts(count_by_name, 1)


def _contract(*, legacy: bool = False, **overrides):
    contract_value_by_field = {
        "strategy_version": (
            SERVER_ISSUED_SUBSET_EXACT_STRATEGY_VERSION
            if legacy
            else SERVER_ISSUED_SUBSET_BOUNDED_STRATEGY_VERSION
        ),
        "completion_scopes": (
            SERVER_ISSUED_SUBSET_EXACT_COMPLETION_SCOPES
            if legacy
            else SERVER_ISSUED_SUBSET_BOUNDED_COMPLETION_SCOPES
        ),
        "campaign_id": "synthetic-reviewed-subset-v3",
    }
    contract_value_by_field.update(overrides)
    return build_subset_contract(**contract_value_by_field)


def _proof_arguments(
    contract,
    *,
    advertised_pre: int = 2,
    advertised_post: int = 2,
    returned_unique: int = 1,
):
    resource_types = contract.resources
    resource_proof_by_type = {}
    for resource_type in resource_types:
        resource_proof = build_execution_proof()
        resource_proof.update(
            advertised_pre=advertised_pre,
            advertised_post=advertised_post,
            returned_unique=returned_unique,
            deficit=advertised_pre - returned_unique,
        )
        resource_proof_by_type[resource_type] = resource_proof
    return {
        "contract": contract,
        "resource_proof_by_type": resource_proof_by_type,
        "dataset_hash": "e" * 64,
        "resource_count": len(resource_types) * returned_unique,
        "resource_hash_by_type": dict.fromkeys(resource_types, "c" * 64),
        "acquired_resource_hash_by_type": dict.fromkeys(
            resource_types,
            "d" * 64,
        ),
        "resource_count_by_type": dict.fromkeys(
            resource_types,
            returned_unique,
        ),
    }


def test_legacy_profile_preserves_completion_bytes_and_hash():
    proof, proof_sha256 = build_subset_completion_proof(
        **_proof_arguments(_contract(legacy=True))
    )

    assert proof["strategy_version"] == (
        SERVER_ISSUED_SUBSET_EXACT_STRATEGY_VERSION
    )
    assert proof["completion_scopes"] == list(
        SERVER_ISSUED_SUBSET_EXACT_COMPLETION_SCOPES
    )
    assert proof_sha256 == LEGACY_PROOF_SHA256
    assert canonical_sha256(proof) == LEGACY_PROOF_SHA256
    assert validate_subset_completion_proof_pair(proof, proof_sha256) == (
        proof,
        proof_sha256,
    )


@pytest.mark.parametrize("advertised_post", (2, 1))
def test_bounded_profile_accepts_equal_or_one_lower_post_count(
    advertised_post,
):
    proof, proof_sha256 = build_subset_completion_proof(
        **_proof_arguments(
            _contract(),
            advertised_pre=2,
            advertised_post=advertised_post,
        )
    )

    assert proof["strategy_version"] == (
        SERVER_ISSUED_SUBSET_BOUNDED_STRATEGY_VERSION
    )
    assert proof["completion_scopes"] == list(
        SERVER_ISSUED_SUBSET_BOUNDED_COMPLETION_SCOPES
    )
    validate_subset_completion_proof_pair(proof, proof_sha256)


@pytest.mark.parametrize(
    ("advertised_pre", "advertised_post", "returned_unique"),
    (
        (2, 3, 1),
        (3, 1, 1),
        (1, 0, 1),
    ),
)
def test_bounded_profile_rejects_out_of_bound_or_returned_above_post(
    advertised_pre,
    advertised_post,
    returned_unique,
):
    arguments = _proof_arguments(
        _contract(),
        advertised_pre=advertised_pre,
        advertised_post=advertised_post,
        returned_unique=returned_unique,
    )

    with pytest.raises(ValueError, match="completion_counts_invalid"):
        build_subset_completion_proof(**arguments)

    valid_proof, _ = build_subset_completion_proof(
        **_proof_arguments(_contract())
    )
    tampered = copy.deepcopy(valid_proof)
    tampered_resource = tampered["resources"]["Organization"]
    tampered_resource.update(
        advertised_pre=advertised_pre,
        advertised_post=advertised_post,
        returned_unique=returned_unique,
        deficit=advertised_pre - returned_unique,
    )
    with pytest.raises(ValueError, match="completion_resource_invalid"):
        validate_subset_completion_proof_pair(
            tampered,
            canonical_sha256(tampered),
        )


def test_legacy_profile_still_rejects_one_lower_post_count():
    with pytest.raises(ValueError, match="completion_counts_invalid"):
        build_subset_completion_proof(
            **_proof_arguments(
                _contract(legacy=True),
                advertised_pre=2,
                advertised_post=1,
            )
        )

    proof, _ = build_subset_completion_proof(
        **_proof_arguments(_contract(legacy=True))
    )
    proof["resources"]["Organization"]["advertised_post"] = 1
    with pytest.raises(ValueError, match="completion_resource_invalid"):
        validate_subset_completion_proof_pair(
            proof,
            canonical_sha256(proof),
        )


def test_bounded_profile_preserves_pre_v5_completion_bytes_and_hash():
    proof, proof_sha256 = build_subset_completion_proof(
        **_proof_arguments(_contract())
    )

    assert proof_sha256 == BOUNDED_PROOF_SHA256
    assert canonical_sha256(proof) == BOUNDED_PROOF_SHA256
    assert validate_subset_completion_proof_pair(proof, proof_sha256) == (
        proof,
        proof_sha256,
    )

@pytest.mark.parametrize(
    ("strategy_version", "completion_scopes"),
    (
        (
            SERVER_ISSUED_SUBSET_BOUNDED_STRATEGY_VERSION,
            SERVER_ISSUED_SUBSET_EXACT_COMPLETION_SCOPES,
        ),
        (
            SERVER_ISSUED_SUBSET_EXACT_STRATEGY_VERSION,
            SERVER_ISSUED_SUBSET_BOUNDED_COMPLETION_SCOPES,
        ),
    ),
)
def test_mixed_profile_tuple_is_rejected_by_builder_and_validator(
    strategy_version,
    completion_scopes,
):
    mixed_contract = _contract(
        strategy_version=strategy_version,
        completion_scopes=completion_scopes,
    )
    with pytest.raises(ValueError, match="completion_resources_invalid"):
        build_subset_completion_proof(**_proof_arguments(mixed_contract))

    proof, _ = build_subset_completion_proof(
        **_proof_arguments(_contract())
    )
    proof["strategy_version"] = strategy_version
    proof["completion_scopes"] = list(completion_scopes)
    with pytest.raises(ValueError, match="completion_proof_invalid"):
        validate_subset_completion_proof_pair(
            proof,
            canonical_sha256(proof),
        )
