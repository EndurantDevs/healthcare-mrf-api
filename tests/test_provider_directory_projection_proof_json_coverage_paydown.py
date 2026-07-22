# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact JSON and claimed native-proof mutation coverage."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from process.provider_directory_projection_contract import (
    claimed_projection_proof_shard,
    canonical_row_digest,
)
from process.provider_directory_projection_json import (
    canonical_exact_json,
    canonical_json,
    canonical_native_record_bytes,
    decoded_fhir_object,
    decoded_json_object,
    exactly_decoded_object,
    native_record_payload_bytes,
)
from process.provider_directory_projection_types import (
    PROJECTION_PROOF_SHARD_CONTRACT_ID,
    ProviderDirectoryProjectionError,
)
from tests.provider_directory_projection_materializer_context import (
    synthetic_projection_context,
)
from tests.provider_directory_projection_materializer_support import (
    resource_counts,
)
from tests.provider_directory_projection_native_copy_support import (
    native_copy_stream,
)


def test_exact_json_round_trip_preserves_lexical_numbers_and_record_shell() -> None:
    decoded = decoded_fhir_object(b'{"latitude":41.500,"rank":2}')
    assert decoded == {"latitude": Decimal("41.500"), "rank": 2}

    exact = exactly_decoded_object(b'{"z":1e2,"a":[-0.50,true,null,"x"]}')
    assert canonical_exact_json(exact) == b'{"a":[-0.50,true,null,"x"],"z":1e2}'
    assert canonical_exact_json(exact, should_sort_keys=False) == (
        b'{"z":1e2,"a":[-0.50,true,null,"x"]}'
    )
    assert canonical_json({"name": "M\u00fcnchen"}) == b'{"name":"M\xc3\xbcnchen"}'

    record_map = {
        "record_kind": "resource",
        "ordinal": 2,
        "payload_json": {"resourceType": "Organization", "id": "o-1"},
    }
    payload_bytes = b'{"resourceType":"Organization","id":"o-1"}'
    record_bytes = canonical_native_record_bytes(
        record_map,
        payload_bytes,
        ("record_kind", "ordinal", "payload_json"),
    )
    assert native_record_payload_bytes(record_bytes) == payload_bytes
    assert decoded_json_object(record_bytes)["ordinal"] == 2


@pytest.mark.parametrize(
    "encoded_json",
    (
        b'{"value":NaN}',
        b'{"duplicate":1,"duplicate":2}',
        b'\xff',
        b'{"unterminated":',
        b'[1,2,3]',
    ),
)
def test_strict_json_decoder_rejects_noncanonical_or_nonobject_input(
    encoded_json,
) -> None:
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="provider_directory_projection_native_spool_invalid$",
    ):
        decoded_json_object(encoded_json)


@pytest.mark.parametrize("json_content", ({1, 2}, float("nan")))
def test_canonical_json_rejects_unrepresentable_values(json_content) -> None:
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="provider_directory_projection_native_spool_invalid$",
    ):
        canonical_json(json_content)


def test_exact_json_and_native_record_extractors_reject_mutations() -> None:
    with pytest.raises(ProviderDirectoryProjectionError):
        canonical_exact_json(7)
    for frame_payload in (
        b'{}',
        b'{"payload_json":{}}',
        b'{"x":1,"payload_json":{},"payload_json":{}}',
        b'{"x":1,"payload_json":}',
    ):
        with pytest.raises(
            ProviderDirectoryProjectionError,
            match="provider_directory_projection_native_spool_invalid$",
        ):
            native_record_payload_bytes(frame_payload)


def _valid_claimed_proof(context):
    _copy_stream, projection_rows = native_copy_stream(context)
    canonical_row_sha256, row_count = canonical_row_digest(projection_rows)
    first_row = projection_rows[0]
    last_row = projection_rows[-1]
    claim = context.claim
    return {
        "contract_id": PROJECTION_PROOF_SHARD_CONTRACT_ID,
        "recipe_id": claim.recipe_lease.recipe.recipe_id,
        "attempt": claim.recipe_lease.attempt,
        "partition_attempt": claim.partition_attempt,
        "partition_id": claim.shard.partition_id,
        "partition_ordinal": claim.shard.partition_ordinal,
        "resource_type": claim.shard.resource_type,
        "input_sha256": claim.shard.input_sha256,
        "canonical_row_sha256": canonical_row_sha256,
        "resource_count": row_count,
        "resource_counts": resource_counts(context.fixture.resources),
        "first_identity": [first_row["resource_type"], first_row["resource_id"]],
        "last_identity": [last_row["resource_type"], last_row["resource_id"]],
    }


def test_claimed_proof_accepts_exact_native_census_with_optional_producer() -> None:
    context = synthetic_projection_context("ndjson")
    proof_map = _valid_claimed_proof(context)
    shard = claimed_projection_proof_shard(proof_map, claim=context.claim)
    assert shard.proof == proof_map

    proof_map["producer_proof"] = {"binary": "rust-v2", "copy_bytes": 1024}
    shard = claimed_projection_proof_shard(proof_map, claim=context.claim)
    assert shard.proof["producer_proof"] == proof_map["producer_proof"]


def _single_organization_context():
    return synthetic_projection_context(
        "ndjson",
        resources=(
            {
                "resourceType": "Organization",
                "id": "organization-only",
                "active": True,
            },
        ),
        label="semantic-coverage-single-family",
    )


@pytest.mark.parametrize(
    ("mutation", "error_code"),
    (
        (lambda proof: proof.update({"unexpected": True}), "proof_shard_invalid"),
        (lambda proof: proof.pop("last_identity"), "proof_shard_invalid"),
        (lambda proof: proof.update({"partition_ordinal": 9}), "coordinate_mismatch"),
        (lambda proof: proof.update({"resource_count": 0}), "resource_counts_invalid"),
        (lambda proof: proof.update({"resource_counts": []}), "resource_counts_invalid"),
        (
            lambda proof: proof.update(
                {"resource_counts": {**proof["resource_counts"], "Unknown": 0}}
            ),
            "resource_counts_invalid",
        ),
        (
            lambda proof: proof.update(
                {
                    "resource_counts": {
                        **proof["resource_counts"],
                        "Organization": True,
                    }
                }
            ),
            "resource_counts_invalid",
        ),
        (
            lambda proof: proof.update(
                {
                    "resource_counts": {
                        resource_type: 0
                        for resource_type in proof["resource_counts"]
                    }
                }
            ),
            "resource_counts_invalid",
        ),
        (lambda proof: proof.update({"first_identity": ["one"]}), "first_identity_invalid"),
        (
            lambda proof: proof.update(
                {"first_identity": ["Unknown", "resource-id"]}
            ),
            "identity_invalid",
        ),
        (
            lambda proof: proof.update(
                {
                    "first_identity": proof["last_identity"],
                    "last_identity": proof["first_identity"],
                }
            ),
            "identity_invalid",
        ),
        (lambda proof: proof.update({"producer_proof": "rust"}), "producer_proof_invalid"),
        (
            lambda proof: proof.update({"producer_proof": {"bad": object()}}),
            "producer_proof_invalid",
        ),
        (
            lambda proof: proof.update(
                {"first_identity": tuple(proof["first_identity"])}
            ),
            "proof_shard_not_canonical",
        ),
    ),
)
def test_claimed_proof_mutations_fail_closed_with_stable_codes(
    mutation,
    error_code,
) -> None:
    context = synthetic_projection_context("ndjson")
    proof_map = _valid_claimed_proof(context)
    mutation(proof_map)
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match=rf"provider_directory_projection_.*{error_code}$",
    ):
        claimed_projection_proof_shard(proof_map, claim=context.claim)


def test_claimed_proof_rejects_nonmapping_and_single_family_census_drift() -> None:
    context = _single_organization_context()
    proof_map = _valid_claimed_proof(context)
    shard = claimed_projection_proof_shard(proof_map, claim=context.claim)
    assert shard.resource_count == 1

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="provider_directory_projection_proof_shard_invalid$",
    ):
        claimed_projection_proof_shard([], claim=context.claim)

    mixed_context = synthetic_projection_context("ndjson")
    organization_claim = replace(
        mixed_context.claim,
        shard=replace(mixed_context.claim.shard, resource_type="Organization"),
    )
    proof_map = _valid_claimed_proof(mixed_context)
    proof_map["resource_type"] = "Organization"
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="provider_directory_projection_proof_shard_resource_counts_invalid$",
    ):
        claimed_projection_proof_shard(proof_map, claim=organization_claim)
