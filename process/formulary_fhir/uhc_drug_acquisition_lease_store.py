# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL claim, heartbeat, validation, and release operations."""

from __future__ import annotations

import secrets
from typing import Any

from db.models import db
from process.formulary_fhir.repository_shared import strict_text
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.uhc_drug_acquisition_lease_contract import (
    DEFAULT_LEASE_SECONDS,
    UHCDrugSourceAcquisitionClaim,
    UHCDrugSourceAcquisitionLeaseError,
    _claim_from_row,
    _lease_seconds,
    _set_action,
)


async def claim_uhc_drug_source_acquisition(
    source_id: str,
    *,
    lease_seconds: int = DEFAULT_LEASE_SECONDS,
    database: Any = db,
) -> UHCDrugSourceAcquisitionClaim:
    """Claim one free or expired source generation; reject a live owner."""

    normalized_source_id = strict_text(source_id, "source id", 64)
    normalized_lease_seconds = _lease_seconds(lease_seconds)
    lease_token = secrets.token_hex(32)
    async with database.transaction():
        await _set_action(
            database,
            "claim",
            source_id=normalized_source_id,
            lease_generation=None,
            lease_token=lease_token,
        )
        await database.status(
            f"INSERT INTO {table_name('fhir_formulary_source_acquisition_lease')} "
            "(source_id) VALUES (:source_id) ON CONFLICT (source_id) "
            "DO NOTHING;",
            source_id=normalized_source_id,
        )
        database_row = await database.first(
            f"UPDATE {table_name('fhir_formulary_source_acquisition_lease')} "
            "SET lease_generation = lease_generation + 1, "
            "lease_token = :lease_token, "
            "lease_expires_at = transaction_timestamp() + "
            "make_interval(secs => :lease_seconds), "
            "lease_heartbeat_at = transaction_timestamp(), "
            "claimed_at = transaction_timestamp(), "
            "updated_at = transaction_timestamp() "
            "WHERE source_id = :source_id AND (lease_token IS NULL OR "
            "lease_expires_at <= clock_timestamp()) "
            "RETURNING source_id, lease_generation, lease_token;",
            source_id=normalized_source_id,
            lease_token=lease_token,
            lease_seconds=normalized_lease_seconds,
        )
    return _claim_from_row(database_row)


async def require_active_uhc_drug_source_acquisition(
    claim: UHCDrugSourceAcquisitionClaim,
    *,
    database: Any = db,
) -> None:
    """Lock the exact token, then recheck expiry using the post-lock clock."""

    if type(claim) is not UHCDrugSourceAcquisitionClaim:
        raise ValueError("FHIR formulary source acquisition claim is invalid")
    locked_generation = await database.scalar(
        "SELECT lease_generation FROM "
        f"{table_name('fhir_formulary_source_acquisition_lease')} WHERE "
        "source_id = :source_id AND lease_generation = :lease_generation "
        "AND lease_token = :lease_token FOR UPDATE;",
        source_id=claim.source_id,
        lease_generation=claim.lease_generation,
        lease_token=claim.lease_token,
    )
    if (
        type(locked_generation) is not int
        or locked_generation != claim.lease_generation
    ):
        raise UHCDrugSourceAcquisitionLeaseError("lease_lost")
    active_generation = await database.scalar(
        "SELECT lease_generation FROM "
        f"{table_name('fhir_formulary_source_acquisition_lease')} WHERE "
        "source_id = :source_id AND lease_generation = :lease_generation "
        "AND lease_token = :lease_token AND "
        "lease_expires_at > clock_timestamp();",
        source_id=claim.source_id,
        lease_generation=claim.lease_generation,
        lease_token=claim.lease_token,
    )
    if (
        type(active_generation) is not int
        or active_generation != claim.lease_generation
    ):
        raise UHCDrugSourceAcquisitionLeaseError("lease_lost")


async def heartbeat_uhc_drug_source_acquisition(
    claim: UHCDrugSourceAcquisitionClaim,
    *,
    lease_seconds: int = DEFAULT_LEASE_SECONDS,
    database: Any = db,
) -> None:
    """Extend only the exact current and still-live source generation."""

    if type(claim) is not UHCDrugSourceAcquisitionClaim:
        raise ValueError("FHIR formulary source acquisition claim is invalid")
    normalized_lease_seconds = _lease_seconds(lease_seconds)
    async with database.transaction():
        await _set_action(
            database,
            "heartbeat",
            source_id=claim.source_id,
            lease_generation=claim.lease_generation,
            lease_token=claim.lease_token,
        )
        updated_count = await database.status(
            f"UPDATE {table_name('fhir_formulary_source_acquisition_lease')} "
            "SET lease_expires_at = transaction_timestamp() + "
            "make_interval(secs => :lease_seconds), "
            "lease_heartbeat_at = transaction_timestamp(), "
            "updated_at = transaction_timestamp() "
            "WHERE source_id = :source_id "
            "AND lease_generation = :lease_generation "
            "AND lease_token = :lease_token "
            "AND lease_expires_at > clock_timestamp();",
            source_id=claim.source_id,
            lease_generation=claim.lease_generation,
            lease_token=claim.lease_token,
            lease_seconds=normalized_lease_seconds,
        )
    if updated_count != 1:
        raise UHCDrugSourceAcquisitionLeaseError("lease_lost")


async def release_uhc_drug_source_acquisition(
    claim: UHCDrugSourceAcquisitionClaim,
    *,
    database: Any = db,
) -> None:
    """Release only the exact live token, retaining its monotonic generation."""

    if type(claim) is not UHCDrugSourceAcquisitionClaim:
        raise ValueError("FHIR formulary source acquisition claim is invalid")
    async with database.transaction():
        await _set_action(
            database,
            "release",
            source_id=claim.source_id,
            lease_generation=claim.lease_generation,
            lease_token=claim.lease_token,
        )
        updated_count = await database.status(
            f"UPDATE {table_name('fhir_formulary_source_acquisition_lease')} "
            "SET lease_token = NULL, lease_expires_at = NULL, "
            "lease_heartbeat_at = NULL, claimed_at = NULL, "
            "updated_at = transaction_timestamp() "
            "WHERE source_id = :source_id "
            "AND lease_generation = :lease_generation "
            "AND lease_token = :lease_token "
            "AND lease_expires_at > clock_timestamp();",
            source_id=claim.source_id,
            lease_generation=claim.lease_generation,
            lease_token=claim.lease_token,
        )
    if updated_count != 1:
        raise UHCDrugSourceAcquisitionLeaseError("lease_lost")


__all__ = (
    "claim_uhc_drug_source_acquisition",
    "heartbeat_uhc_drug_source_acquisition",
    "release_uhc_drug_source_acquisition",
    "require_active_uhc_drug_source_acquisition",
)
