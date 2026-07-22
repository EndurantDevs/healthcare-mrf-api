# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Locator-free contracts for one projection shard's retained read lease."""

from __future__ import annotations

from typing import Any, Mapping

from process.provider_directory_projection_contract import (
    PROJECTION_SHARD_SPEC_CONTRACT_ID,
    validated_physical_projection_recipe_identity,
)
from process.provider_directory_projection_types import (
    PROJECTION_MIXED_RESOURCE_TYPE,
    ProjectionLease,
    ProjectionRetainedChildLease,
    ProjectionShardClaim,
    ProjectionShardSpec,
    ProviderDirectoryProjectionError,
    required_hash,
    required_text,
    stable_hash,
)


def _invalid_shard_claim() -> ProviderDirectoryProjectionError:
    return ProviderDirectoryProjectionError(
        "provider_directory_projection_child_shard_claim_invalid"
    )


def _expected_partition_id(claim: ProjectionShardClaim) -> str:
    recipe = claim.recipe_lease.recipe
    shard = claim.shard
    return stable_hash(
        {
            "contract_id": PROJECTION_SHARD_SPEC_CONTRACT_ID,
            "recipe_id": recipe.recipe_id,
            "partition_ordinal": shard.partition_ordinal,
            "partition_key": shard.input_block_id,
            "input_block_id": shard.input_block_id,
            "resource_type": PROJECTION_MIXED_RESOURCE_TYPE,
            "input_sha256": shard.input_sha256,
        },
        domain="provider-directory-projection-partition-id-v2",
    )


def _has_exact_shard_claim_identity(
    claim: ProjectionShardClaim,
    recipe_id: str,
) -> bool:
    shard = claim.shard
    integer_entries = (
        claim.recipe_lease.attempt,
        claim.partition_attempt,
        shard.partition_ordinal,
    )
    return not (
        any(type(integer_entry) is not int for integer_entry in integer_entries)
        or claim.recipe_lease.attempt < 1
        or claim.partition_attempt < 1
        or shard.partition_ordinal < 0
        or required_hash(claim.admission_id, "admission_id") != claim.admission_id
        or required_hash(shard.partition_id, "partition_id") != shard.partition_id
        or shard.partition_id != _expected_partition_id(claim)
        or required_hash(shard.partition_key, "partition_key") != shard.partition_key
        or shard.partition_key != shard.input_block_id
        or required_hash(shard.input_block_id, "input_block_id")
        != shard.input_block_id
        or required_hash(shard.input_sha256, "input_sha256") != shard.input_sha256
        or required_hash(claim.recipe_lease.lease_token, "recipe_lease_token")
        != claim.recipe_lease.lease_token
        or required_hash(claim.lease_token, "shard_lease_token")
        != claim.lease_token
        or required_text(shard.resource_type, "resource_type", limit=64)
        != shard.resource_type
        or shard.resource_type != PROJECTION_MIXED_RESOURCE_TYPE
        or recipe_id != claim.recipe_lease.recipe.recipe_id
    )


def validated_child_shard_claim(claim: ProjectionShardClaim) -> ProjectionShardClaim:
    """Reject forged shard claims before they influence a lease transaction."""

    if type(claim) is not ProjectionShardClaim:
        raise _invalid_shard_claim()
    if (
        type(claim.recipe_lease) is not ProjectionLease
        or type(claim.shard) is not ProjectionShardSpec
    ):
        raise _invalid_shard_claim()
    recipe = validated_physical_projection_recipe_identity(claim.recipe_lease.recipe)
    if not _has_exact_shard_claim_identity(claim, recipe.recipe_id):
        raise _invalid_shard_claim()
    return claim


def _required_positive(value: Any, field_name: str) -> int:
    if type(value) is not int or value < 1:
        raise ProviderDirectoryProjectionError(
            f"provider_directory_projection_child_{field_name}_invalid"
        )
    return value


def _required_nonnegative(value: Any, field_name: str) -> int:
    if type(value) is not int or value < 0:
        raise ProviderDirectoryProjectionError(
            f"provider_directory_projection_child_{field_name}_invalid"
        )
    return value


def _validated_range_ordinal(value: Any) -> int | None:
    if value is None:
        return None
    return _required_nonnegative(value, "retained_range_ordinal")


def child_lease_database_identity(
    lease: ProjectionRetainedChildLease,
) -> dict[str, Any]:
    """Return every immutable or generation-fenced field stored for a child."""

    claim = lease.shard_claim
    return {
        "recipe_id": claim.recipe_lease.recipe.recipe_id,
        "recipe_attempt": claim.recipe_lease.attempt,
        "admission_id": claim.admission_id,
        "partition_id": claim.shard.partition_id,
        "partition_attempt": claim.partition_attempt,
        "recipe_lease_token": claim.recipe_lease.lease_token,
        "shard_lease_token": claim.lease_token,
        "binding_id": lease.binding_id,
        "block_id": lease.block_id,
        "retained_campaign_id": lease.retained_campaign_id,
        "retained_campaign_sha256": lease.retained_campaign_sha256,
        "retained_consumer_recipe_id": lease.retained_consumer_recipe_id,
        "retained_claim_generation": lease.retained_claim_generation,
        "retained_source_item_id": lease.retained_source_item_id,
        "retained_artifact_sha256": lease.retained_artifact_sha256,
        "retained_layout_sha256": lease.retained_layout_sha256,
        "retained_range_ordinal": lease.retained_range_ordinal,
        "artifact_byte_count": lease.artifact_byte_count,
        "raw_byte_start": lease.raw_byte_start,
        "expected_byte_count": lease.expected_byte_count,
        "expected_record_count": lease.expected_record_count,
        "input_sha256": lease.input_sha256,
        "expected_payload_sha256": lease.expected_payload_sha256,
        "child_generation": lease.child_generation,
        "child_lease_token": lease.child_lease_token,
    }


def _validate_child_read_hashes(lease: ProjectionRetainedChildLease) -> None:
    hash_by_field = {
        "binding_id": lease.binding_id,
        "block_id": lease.block_id,
        "retained_campaign_id": lease.retained_campaign_id,
        "retained_campaign_sha256": lease.retained_campaign_sha256,
        "retained_source_item_id": lease.retained_source_item_id,
        "retained_artifact_sha256": lease.retained_artifact_sha256,
        "retained_layout_sha256": lease.retained_layout_sha256,
        "input_sha256": lease.input_sha256,
        "expected_payload_sha256": lease.expected_payload_sha256,
        "child_lease_token": lease.child_lease_token,
    }
    if any(
        required_hash(hash_entry, field_name) != hash_entry
        for field_name, hash_entry in hash_by_field.items()
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_child_read_lease_invalid"
        )


def _validate_child_read_counts(lease: ProjectionRetainedChildLease) -> None:
    positive_by_field = {
        "retained_claim_generation": lease.retained_claim_generation,
        "artifact_byte_count": lease.artifact_byte_count,
        "expected_byte_count": lease.expected_byte_count,
        "expected_record_count": lease.expected_record_count,
        "child_generation": lease.child_generation,
    }
    for field_name, positive_entry in positive_by_field.items():
        _required_positive(positive_entry, field_name)
    _required_nonnegative(lease.raw_byte_start, "raw_byte_start")
    _validated_range_ordinal(lease.retained_range_ordinal)


def _has_exact_child_read_scope(
    lease: ProjectionRetainedChildLease,
    consumer_recipe_id: str,
) -> bool:
    return bool(
        consumer_recipe_id == lease.retained_consumer_recipe_id
        and lease.block_id == lease.shard_claim.shard.input_block_id
        and lease.input_sha256 == lease.shard_claim.shard.input_sha256
        and lease.raw_byte_start + lease.expected_byte_count
        <= lease.artifact_byte_count
        and (
            lease.retained_range_ordinal is not None
            or (
                lease.raw_byte_start == 0
                and lease.expected_byte_count == lease.artifact_byte_count
            )
        )
    )


def validated_child_read_lease(
    lease: ProjectionRetainedChildLease,
) -> ProjectionRetainedChildLease:
    """Revalidate a public lease record without exposing a storage locator."""

    if type(lease) is not ProjectionRetainedChildLease:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_child_read_lease_invalid"
        )
    validated_child_shard_claim(lease.shard_claim)
    _validate_child_read_hashes(lease)
    consumer_recipe_id = required_text(
        lease.retained_consumer_recipe_id,
        "retained_consumer_recipe_id",
        limit=128,
    )
    _validate_child_read_counts(lease)
    if not _has_exact_child_read_scope(lease, consumer_recipe_id):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_child_read_lease_invalid"
        )
    return lease


def child_read_lease_from_row(
    claim: ProjectionShardClaim,
    lease_row: Mapping[str, Any],
) -> ProjectionRetainedChildLease:
    """Build a validated locator-free lease from one database row."""

    lease = ProjectionRetainedChildLease(
        shard_claim=claim,
        binding_id=str(lease_row["binding_id"]),
        block_id=str(lease_row["block_id"]),
        retained_campaign_id=str(lease_row["retained_campaign_id"]),
        retained_campaign_sha256=str(lease_row["retained_campaign_sha256"]),
        retained_consumer_recipe_id=str(lease_row["retained_consumer_recipe_id"]),
        retained_claim_generation=int(lease_row["retained_claim_generation"]),
        retained_source_item_id=str(lease_row["retained_source_item_id"]),
        retained_artifact_sha256=str(lease_row["retained_artifact_sha256"]),
        retained_layout_sha256=str(lease_row["retained_layout_sha256"]),
        retained_range_ordinal=lease_row.get("retained_range_ordinal"),
        artifact_byte_count=int(lease_row["artifact_byte_count"]),
        raw_byte_start=int(lease_row["raw_byte_start"]),
        expected_byte_count=int(lease_row["expected_byte_count"]),
        expected_record_count=int(lease_row["expected_record_count"]),
        input_sha256=str(lease_row["input_sha256"]),
        expected_payload_sha256=str(lease_row["expected_payload_sha256"]),
        child_generation=int(lease_row["child_generation"]),
        child_lease_token=str(lease_row["child_lease_token"]),
    )
    return validated_child_read_lease(lease)


def has_exact_child_row_identity(
    lease_row: Mapping[str, Any],
    lease: ProjectionRetainedChildLease,
) -> bool:
    """Return whether a locked row is the exact public lease generation."""

    identity = child_lease_database_identity(validated_child_read_lease(lease))
    return bool(lease_row) and all(
        lease_row.get(field_name) == identity_entry
        for field_name, identity_entry in identity.items()
    )


child_row_matches_lease = has_exact_child_row_identity


__all__ = (
    "child_lease_database_identity",
    "child_read_lease_from_row",
    "child_row_matches_lease",
    "has_exact_child_row_identity",
    "validated_child_read_lease",
    "validated_child_shard_claim",
)
