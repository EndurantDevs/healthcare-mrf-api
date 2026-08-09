# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Selector-free orchestration for reviewed Provider Directory subset state."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from process.provider_directory_fhir_subset_activation_contract import (
    ACTIVATION_CONTRACT_VERSION,
    ACTIVATION_CONTRACT_VERSION_V2,
    ACTIVATION_METADATA_KEY,
    ACTIVATION_METADATA_KEY_V2,
    DEFAULT_REVIEWED_SUBSET_ACTIVATION_MANIFEST,
    PENDING_STATUS,
    STATE_SYNC_ENABLED_ENV,
    STATE_SYNC_TIMEOUT_SECONDS,
    VERIFIED_STATUS,
    ReviewedSubsetActivationError,
    ReviewedSubsetActivationEvidence,
    ReviewedSubsetActivationManifest,
    ReviewedSubsetActivationResult,
    ReviewedSubsetActivationSelection,
    _quoted_relation,
    require_reviewed_subset_state_sync_gate,
    reviewed_subset_activation_manifest,
    reviewed_subset_source_contract_sha256,
)
from process.provider_directory_fhir_subset_activation_selection import (
    validated_reviewed_subset_activation_selection,
)
from process.provider_directory_fhir_subset_activation_store import (
    sync_reviewed_subset_transaction,
)


async def sync_reviewed_subset_verified_state(
    *,
    database: Any | None = None,
) -> ReviewedSubsetActivationResult:
    """Apply the sole checked-in reviewed desired state without selectors."""

    require_reviewed_subset_state_sync_gate()
    evidence = reviewed_subset_activation_manifest().require_verified_evidence()
    try:
        from process.provider_directory_fhir_manual_catalog import (
            reviewed_manual_census_source_id,
        )

        expected_source_id = reviewed_manual_census_source_id()
    except (OSError, RuntimeError, TypeError, ValueError):
        raise ReviewedSubsetActivationError("evidence") from None
    try:
        runtime_database = database
        if runtime_database is None:
            from db.connection import db

            runtime_database = db
        async with asyncio.timeout(STATE_SYNC_TIMEOUT_SECONDS):
            return await sync_reviewed_subset_transaction(
                runtime_database,
                expected_source_id,
                evidence,
            )
    except (asyncio.CancelledError, TimeoutError):
        raise
    except ReviewedSubsetActivationError:
        raise
    except Exception:
        raise ReviewedSubsetActivationError("state") from None


def reviewed_subset_activation_result_json(
    activation_result: ReviewedSubsetActivationResult,
) -> str:
    """Render a deterministic selector-free success result."""

    if type(activation_result) is not ReviewedSubsetActivationResult:
        raise ReviewedSubsetActivationError("state")
    return json.dumps(
        {
            "activated": activation_result.activated,
            "already_applied": activation_result.is_already_applied,
            "status": "ok",
        },
        sort_keys=True,
        separators=(",", ":"),
    )


__all__ = (
    "ACTIVATION_CONTRACT_VERSION",
    "ACTIVATION_CONTRACT_VERSION_V2",
    "ACTIVATION_METADATA_KEY",
    "ACTIVATION_METADATA_KEY_V2",
    "DEFAULT_REVIEWED_SUBSET_ACTIVATION_MANIFEST",
    "PENDING_STATUS",
    "STATE_SYNC_ENABLED_ENV",
    "VERIFIED_STATUS",
    "ReviewedSubsetActivationError",
    "ReviewedSubsetActivationEvidence",
    "ReviewedSubsetActivationManifest",
    "ReviewedSubsetActivationResult",
    "ReviewedSubsetActivationSelection",
    "reviewed_subset_activation_result_json",
    "reviewed_subset_activation_manifest",
    "reviewed_subset_source_contract_sha256",
    "require_reviewed_subset_state_sync_gate",
    "sync_reviewed_subset_verified_state",
    "validated_reviewed_subset_activation_selection",
)
