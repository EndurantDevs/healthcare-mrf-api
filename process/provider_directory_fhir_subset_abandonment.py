# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Selector-free orchestration for reviewed subset abandonment."""

from __future__ import annotations

import asyncio
from typing import Any

from process.provider_directory_fhir_census_contract import (
    SERVER_ISSUED_SUBSET_RESOURCE_TYPES,
)
from process.provider_directory_fhir_subset_abandonment_contract import (
    ABANDONMENT_TIMEOUT_SECONDS,
    ReviewedSubsetAbandonmentError,
    ReviewedSubsetAbandonmentResult,
    abandonment_result_json,
    require_reviewed_subset_abandonment_gate,
)
from process.provider_directory_fhir_subset_abandonment_store import (
    sync_reviewed_subset_abandonment_transaction,
)


async def abandon_reviewed_subset_expired_root(
    *,
    database: Any | None = None,
) -> ReviewedSubsetAbandonmentResult:
    """Seal the sole checked-in reviewed root without accepting selectors."""

    require_reviewed_subset_abandonment_gate()
    try:
        from process.provider_directory_fhir_manual_catalog import (
            reviewed_manual_census_source_id,
        )

        expected_source_id = reviewed_manual_census_source_id()
    except (OSError, RuntimeError, TypeError, ValueError):
        raise ReviewedSubsetAbandonmentError("evidence") from None
    try:
        runtime_database = database
        if runtime_database is None:
            from db.connection import db

            runtime_database = db
        async with asyncio.timeout(ABANDONMENT_TIMEOUT_SECONDS):
            return await sync_reviewed_subset_abandonment_transaction(
                runtime_database,
                expected_source_id,
                tuple(sorted(SERVER_ISSUED_SUBSET_RESOURCE_TYPES)),
            )
    except (asyncio.CancelledError, TimeoutError):
        raise
    except ReviewedSubsetAbandonmentError:
        raise
    except Exception:
        raise ReviewedSubsetAbandonmentError("state") from None


__all__ = (
    "ReviewedSubsetAbandonmentError",
    "ReviewedSubsetAbandonmentResult",
    "abandon_reviewed_subset_expired_root",
    "abandonment_result_json",
    "require_reviewed_subset_abandonment_gate",
)
