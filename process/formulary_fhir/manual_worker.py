# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Default-off manual adapter for verify-only formulary synchronization."""

from __future__ import annotations

import asyncio
import datetime as dt
import os
from typing import Any

from db.models import db
import process.formulary_fhir.manual_lock as manual_lock
from process.formulary_fhir.repository_shared import json_text
from process.formulary_fhir.repository_shared import strict_text
from process.formulary_fhir.repository_shared import utc_timestamp
from process.formulary_fhir.synchronizer import SynchronizationResult
from process.formulary_fhir.synchronizer import synchronize_verified_dataset


MANUAL_SYNC_ENABLED_ENV = "HLTHPRT_FHIR_FORMULARY_MANUAL_SYNC_ENABLED"
LOCK_WAIT_SECONDS = 5.0
LOCK_RETRY_SECONDS = 0.1
MAX_TIMEOUT_SECONDS = 604_800
TRUE_ENV_VALUES = frozenset({"1", "true", "yes", "on"})
ERROR_MESSAGES = {
    "busy": "FHIR formulary manual synchronization source is busy",
    "cleanup": "FHIR formulary manual synchronization cleanup failed",
    "disabled": "FHIR formulary manual synchronization is disabled",
    "invalid_request": "FHIR formulary manual synchronization request is invalid",
    "invalid_result": "FHIR formulary manual synchronization result is invalid",
    "lock_unavailable": "FHIR formulary manual synchronization lock is unavailable",
}


class ManualSynchronizationError(RuntimeError):
    """Expose only one stable adapter failure code and sanitized message."""

    def __init__(self, code: str) -> None:
        if code not in ERROR_MESSAGES:
            code = "lock_unavailable"
        self.code = code
        super().__init__(ERROR_MESSAGES[code])


def _is_manual_sync_enabled() -> bool:
    raw_value = os.getenv(MANUAL_SYNC_ENABLED_ENV, "")
    return raw_value.strip().lower() in TRUE_ENV_VALUES


def _normalized_cutoff(cutoff: object) -> dt.datetime:
    try:
        if type(cutoff) is dt.datetime:
            cutoff_at = utc_timestamp(cutoff, "manual cutoff")
        else:
            cutoff_text = strict_text(cutoff, "manual cutoff", 64)
            if cutoff_text.endswith("Z"):
                cutoff_text = cutoff_text[:-1] + "+00:00"
            cutoff_at = utc_timestamp(
                dt.datetime.fromisoformat(cutoff_text),
                "manual cutoff",
            )
        if cutoff_at > dt.datetime.now(dt.UTC):
            raise ValueError("future cutoff")
        return cutoff_at
    except (OverflowError, TypeError, ValueError):
        raise ManualSynchronizationError("invalid_request") from None


def _normalized_request(
    source_id: object,
    run_id: object,
    cutoff: object,
    timeout_seconds: object,
) -> tuple[str, str, dt.datetime, int]:
    try:
        normalized_source_id = strict_text(source_id, "source id", 64)
        normalized_run_id = strict_text(run_id, "run id", 64)
    except ValueError:
        raise ManualSynchronizationError("invalid_request") from None
    if (
        type(timeout_seconds) is not int
        or timeout_seconds < 1
        or timeout_seconds > MAX_TIMEOUT_SECONDS
    ):
        raise ManualSynchronizationError("invalid_request")
    return (
        normalized_source_id,
        normalized_run_id,
        _normalized_cutoff(cutoff),
        timeout_seconds,
    )


async def synchronize_verified_dataset_manually(
    *,
    source_id: str,
    run_id: str,
    cutoff: object,
    timeout_seconds: int,
    database: Any = db,
) -> SynchronizationResult:
    """Run one explicitly gated, source-locked, verify-only synchronization."""

    if not _is_manual_sync_enabled():
        raise ManualSynchronizationError("disabled")
    (
        normalized_source_id,
        normalized_run_id,
        cutoff_at,
        bounded_timeout_seconds,
    ) = _normalized_request(source_id, run_id, cutoff, timeout_seconds)
    try:
        async with manual_lock.manual_source_lease(
            database,
            normalized_source_id,
            wait_seconds=LOCK_WAIT_SECONDS,
            retry_seconds=LOCK_RETRY_SECONDS,
        ):
            async with asyncio.timeout(bounded_timeout_seconds):
                synchronization_result = await synchronize_verified_dataset(
                    source_id=normalized_source_id,
                    run_id=normalized_run_id,
                    cutoff=cutoff_at,
                    database=database,
                )
                if type(synchronization_result) is not SynchronizationResult:
                    raise ManualSynchronizationError("invalid_result")
    except manual_lock.ManualSourceLockError as error:
        raise ManualSynchronizationError(error.code) from None
    return synchronization_result


def manual_result_json(result: SynchronizationResult) -> str:
    """Serialize the fixed safe success schema for the manual command."""

    if type(result) is not SynchronizationResult:
        raise ManualSynchronizationError("invalid_result")
    result_by_field = {
        "status": "verified",
        "dataset_id": result.dataset_id,
        "acquisition_contract_hash": result.acquisition_contract_hash,
        "list_count": result.list_count,
        "alias_count": result.alias_count,
        "medication_membership_count": result.medication_membership_count,
        "coverage_hash": result.coverage_hash,
        "membership_hash": result.membership_hash,
        "full_aliases": result.full_aliases,
        "reused_aliases": result.reused_aliases,
        "resumed_aliases": result.resumed_aliases,
        "request_count": result.request_count,
        "transient_retry_count": result.transient_retry_count,
        "throttle_count": result.throttle_count,
    }
    return json_text(result_by_field)


__all__ = (
    "MANUAL_SYNC_ENABLED_ENV",
    "ManualSynchronizationError",
    "manual_result_json",
    "synchronize_verified_dataset_manually",
)


# Retain this private alias for the focused cancellation-defense contract.
_drain = manual_lock._drain
