# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Lease-fenced transitions for one projection shard's retained input."""

from __future__ import annotations

import secrets
from typing import Any

from db.connection import db
from process.provider_directory_projection_child_read_contract import (
    child_lease_database_identity,
    child_read_lease_from_row,
    has_exact_child_row_identity,
    validated_child_read_lease,
    validated_child_shard_claim,
)
from process.provider_directory_projection_child_read_lock import (
    locked_child_read_dependencies,
)
from process.provider_directory_projection_child_read_persistence import (
    _claim_values_by_column,
    _has_live_current_child,
    _insert_child,
    _locked_child_row,
    _reclaim_child,
    _set_child_action,
    _validated_lease_seconds,
    _verify_child_row,
)
from process.provider_directory_projection_db import table_ref
from process.provider_directory_projection_types import (
    ProjectionRetainedChildLease,
    ProjectionShardClaim,
    ProviderDirectoryProjectionBusy,
    ProviderDirectoryProjectionError,
    ProviderDirectoryProjectionLeaseLost,
    required_hash,
)


def _lease_lost() -> ProviderDirectoryProjectionLeaseLost:
    return ProviderDirectoryProjectionLeaseLost(
        "provider_directory_projection_child_read_lease_lost"
    )


async def claim_projection_child_read_lease(
    claim: ProjectionShardClaim,
    *,
    lease_seconds: int = 300,
    database: Any = db,
    schema: str = "mrf",
) -> ProjectionRetainedChildLease:
    """Claim or safely reclaim the exact retained bytes for one shard."""

    claim = validated_child_shard_claim(claim)
    lease_seconds = _validated_lease_seconds(lease_seconds)
    child_table = table_ref(schema, "provider_directory_projection_child_read_lease")
    child_token = secrets.token_hex(32)
    async with database.transaction():
        descriptor_by_field = await locked_child_read_dependencies(
            database,
            schema,
            claim,
        )
        previous_row = await _locked_child_row(database, schema, claim)
        if previous_row and await _has_live_current_child(database, previous_row, claim):
            raise ProviderDirectoryProjectionBusy(
                "provider_directory_projection_child_read_busy"
            )
        generation = int(previous_row.get("child_generation") or 0) + 1
        await _set_child_action(database, "claim", claim, generation, child_token)
        claim_values_by_column = _claim_values_by_column(
            claim,
            descriptor_by_field,
            generation,
            child_token,
            lease_seconds,
        )
        if previous_row:
            child_row = await _reclaim_child(
                database,
                child_table,
                claim_values_by_column,
                generation - 1,
            )
        else:
            child_row = await _insert_child(
                database,
                child_table,
                claim_values_by_column,
            )
    if not child_row:
        raise _lease_lost()
    return child_read_lease_from_row(claim, child_row)


async def _locked_exact_lease_row(
    database: Any,
    schema: str,
    lease: ProjectionRetainedChildLease,
    *,
    allow_completed: bool,
) -> dict[str, Any]:
    lease = validated_child_read_lease(lease)
    await locked_child_read_dependencies(
        database,
        schema,
        lease.shard_claim,
        allow_completed=allow_completed,
    )
    child_row = await _locked_child_row(database, schema, lease.shard_claim)
    if not has_exact_child_row_identity(child_row, lease):
        raise _lease_lost()
    return child_row


async def heartbeat_projection_child_read_lease(
    lease: ProjectionRetainedChildLease,
    *,
    lease_seconds: int = 300,
    database: Any = db,
    schema: str = "mrf",
) -> None:
    """Extend one exact active child generation."""

    lease = validated_child_read_lease(lease)
    lease_seconds = _validated_lease_seconds(lease_seconds)
    async with database.transaction():
        child_row = await _locked_exact_lease_row(
            database,
            schema,
            lease,
            allow_completed=False,
        )
        if child_row.get("status") != "active":
            raise _lease_lost()
        await _set_child_action(
            database,
            "heartbeat",
            lease.shard_claim,
            lease.child_generation,
            lease.child_lease_token,
        )
        updated_count = await database.status(
            f"""
            UPDATE {table_ref(schema, 'provider_directory_projection_child_read_lease')}
               SET lease_expires_at =
                       now() + make_interval(secs => :lease_seconds),
                   lease_heartbeat_at = now(), updated_at = now()
             WHERE recipe_id = :recipe_id
               AND recipe_attempt = :recipe_attempt
               AND partition_id = :partition_id
               AND child_generation = :child_generation
               AND child_lease_token = :child_lease_token
               AND status = 'active' AND lease_expires_at > now();
            """,
            **child_lease_database_identity(lease),
            lease_seconds=lease_seconds,
        )
    if updated_count != 1:
        raise _lease_lost()


def _validated_verification_digests(
    lease: ProjectionRetainedChildLease,
    byte_count: int,
    record_count: int,
    input_sha256: str,
    payload_sha256: str,
) -> tuple[str, str]:
    normalized_payload_sha256 = required_hash(
        payload_sha256,
        "verified_payload_sha256",
    )
    normalized_input_sha256 = required_hash(
        input_sha256,
        "verified_input_sha256",
    )
    if (
        type(byte_count) is not int
        or type(record_count) is not int
        or byte_count != lease.expected_byte_count
        or record_count != lease.expected_record_count
        or normalized_input_sha256 != lease.input_sha256
        or normalized_payload_sha256 != lease.expected_payload_sha256
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_child_read_verification_mismatch"
        )
    return normalized_input_sha256, normalized_payload_sha256


async def verify_projection_child_read_lease(
    lease: ProjectionRetainedChildLease,
    *,
    byte_count: int,
    record_count: int,
    input_sha256: str,
    payload_sha256: str,
    database: Any = db,
    schema: str = "mrf",
) -> None:
    """Seal the exact raw-byte and decoded-record census for one child."""

    lease = validated_child_read_lease(lease)
    normalized_input_sha256, normalized_payload_sha256 = (
        _validated_verification_digests(
            lease,
            byte_count,
            record_count,
            input_sha256,
            payload_sha256,
        )
    )
    async with database.transaction():
        child_row = await _locked_exact_lease_row(
            database,
            schema,
            lease,
            allow_completed=False,
        )
        if child_row.get("status") != "active":
            raise _lease_lost()
        await _set_child_action(
            database,
            "verify",
            lease.shard_claim,
            lease.child_generation,
            lease.child_lease_token,
        )
        updated_count = await _verify_child_row(
            database,
            schema,
            lease,
            byte_count,
            record_count,
            normalized_input_sha256,
            normalized_payload_sha256,
        )
    if updated_count != 1:
        raise _lease_lost()


async def release_projection_child_read_lease(
    lease: ProjectionRetainedChildLease,
    *,
    database: Any = db,
    schema: str = "mrf",
) -> None:
    """Release only the child generation, never its shared parent claim."""

    lease = validated_child_read_lease(lease)
    async with database.transaction():
        child_row = await _locked_exact_lease_row(
            database,
            schema,
            lease,
            allow_completed=True,
        )
        if child_row.get("status") == "released":
            return
        if child_row.get("status") not in {"active", "verified"}:
            raise _lease_lost()
        await _set_child_action(
            database,
            "release",
            lease.shard_claim,
            lease.child_generation,
            lease.child_lease_token,
        )
        updated_count = await database.status(
            f"""
            UPDATE {table_ref(schema, 'provider_directory_projection_child_read_lease')}
               SET status = 'released', lease_expires_at = now(),
                   lease_heartbeat_at = now(), released_at = now(),
                   updated_at = now()
             WHERE recipe_id = :recipe_id
               AND recipe_attempt = :recipe_attempt
               AND partition_id = :partition_id
               AND child_generation = :child_generation
               AND child_lease_token = :child_lease_token
               AND status IN ('active', 'verified');
            """,
            **child_lease_database_identity(lease),
        )
    if updated_count != 1:
        raise _lease_lost()


async def assert_verified_projection_child_read_lease(
    lease: ProjectionRetainedChildLease,
    *,
    database: Any = db,
    schema: str = "mrf",
) -> None:
    """Require the exact verified child before shard completion."""

    lease = validated_child_read_lease(lease)
    async with database.transaction():
        child_row = await _locked_exact_lease_row(
            database,
            schema,
            lease,
            allow_completed=True,
        )
        exact_verified = bool(
            child_row.get("status") == "verified"
            and child_row.get("verified_byte_count")
            == lease.expected_byte_count
            and child_row.get("verified_record_count")
            == lease.expected_record_count
            and child_row.get("verified_input_sha256") == lease.input_sha256
            and child_row.get("verified_payload_sha256")
            == lease.expected_payload_sha256
        )
        if not exact_verified:
            raise _lease_lost()


__all__ = (
    "assert_verified_projection_child_read_lease",
    "claim_projection_child_read_lease",
    "heartbeat_projection_child_read_lease",
    "release_projection_child_read_lease",
    "verify_projection_child_read_lease",
)
