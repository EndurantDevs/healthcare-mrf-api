# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Selector-free operator facade for one reviewed terminal disposition."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from process.provider_directory_fhir_subset_terminal_disposition_contract import (
    TERMINAL_DISPOSITION_TIMEOUT_SECONDS,
    ReviewedSubsetTerminalDispositionError,
    ReviewedSubsetTerminalDispositionResult,
    require_reviewed_subset_terminal_disposition_gate,
)
from process.provider_directory_fhir_subset_terminal_disposition_store import (
    sync_reviewed_subset_terminal_disposition_transaction,
)


async def dispose_reviewed_subset_census_drift_root(
    *,
    database: Any | None = None,
) -> ReviewedSubsetTerminalDispositionResult:
    """Seal the sole checked-in eligible root without accepting selectors."""

    require_reviewed_subset_terminal_disposition_gate()
    try:
        from process.provider_directory_fhir_manual_catalog import (
            reviewed_manual_census_source_id,
        )

        expected_source_id = reviewed_manual_census_source_id()
    except (OSError, RuntimeError, TypeError, ValueError):
        raise ReviewedSubsetTerminalDispositionError("evidence") from None
    try:
        runtime_database = database
        if runtime_database is None:
            from db.connection import db

            runtime_database = db
        async with asyncio.timeout(TERMINAL_DISPOSITION_TIMEOUT_SECONDS):
            return await sync_reviewed_subset_terminal_disposition_transaction(
                runtime_database,
                expected_source_id,
            )
    except (asyncio.CancelledError, TimeoutError):
        raise
    except ReviewedSubsetTerminalDispositionError:
        raise
    except Exception:
        raise ReviewedSubsetTerminalDispositionError("state") from None


def terminal_disposition_result_json(
    result: ReviewedSubsetTerminalDispositionResult,
) -> str:
    """Render one closed selector-free success result."""

    if type(result) is not ReviewedSubsetTerminalDispositionResult:
        raise ReviewedSubsetTerminalDispositionError("state")
    return json.dumps(
        {
            "already_applied": result.is_already_applied,
            "disposed": result.disposed,
            "status": "ok",
        },
        sort_keys=True,
        separators=(",", ":"),
    )


__all__ = (
    "ReviewedSubsetTerminalDispositionError",
    "ReviewedSubsetTerminalDispositionResult",
    "dispose_reviewed_subset_census_drift_root",
    "require_reviewed_subset_terminal_disposition_gate",
    "terminal_disposition_result_json",
)
