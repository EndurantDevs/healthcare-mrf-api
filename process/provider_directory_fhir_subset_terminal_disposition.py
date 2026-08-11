# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Selector-free operator facade for one reviewed terminal disposition."""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any

from process.provider_directory_fhir_subset_terminal_disposition_contract import (
    TERMINAL_DISPOSITION_TIMEOUT_SECONDS,
    ReviewedSubsetTerminalDispositionError,
    ReviewedSubsetTerminalDispositionResult,
    require_reviewed_subset_terminal_disposition_gate,
)
from process.provider_directory_fhir_subset_terminal_disposition_store import (
    sync_v4_terminal_disposition,
    sync_v5_terminal_disposition,
    sync_reviewed_subset_terminal_disposition_transaction,
)
from process.provider_directory_fhir_subset_terminal_disposition_profile import (
    DIRECT_V4_TERMINAL_DISPOSITION_ENABLED_ENV,
    DIRECT_V5_HTTP410_TERMINAL_DISPOSITION_ENABLED_ENV,
)


def require_v4_disposition_gate() -> None:
    """Require the explicit direct-v4 one-shot disposition gate."""

    if os.getenv(DIRECT_V4_TERMINAL_DISPOSITION_ENABLED_ENV) != "true":
        raise ReviewedSubsetTerminalDispositionError("disabled")


def require_v5_disposition_gate() -> None:
    """Require the explicit direct-v5 HTTP-410 one-shot gate."""

    if os.getenv(DIRECT_V5_HTTP410_TERMINAL_DISPOSITION_ENABLED_ENV) != "true":
        raise ReviewedSubsetTerminalDispositionError("disabled")


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


async def dispose_v4_census_drift_root(
    *,
    database: Any | None = None,
) -> ReviewedSubsetTerminalDispositionResult:
    """Seal the sole direct-v4 failed root without accepting selectors."""

    require_v4_disposition_gate()
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
            return await (
                sync_v4_terminal_disposition(
                    runtime_database,
                    expected_source_id,
                )
            )
    except (asyncio.CancelledError, TimeoutError):
        raise
    except ReviewedSubsetTerminalDispositionError:
        raise
    except Exception:
        raise ReviewedSubsetTerminalDispositionError("state") from None


async def dispose_v5_terminal_root(
    *,
    database: Any | None = None,
) -> ReviewedSubsetTerminalDispositionResult:
    """Seal the sole direct-v5 HTTP-410 root without accepting selectors."""

    require_v5_disposition_gate()
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
            return await sync_v5_terminal_disposition(
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
    "dispose_v4_census_drift_root",
    "dispose_v5_terminal_root",
    "require_v4_disposition_gate",
    "require_v5_disposition_gate",
    "require_reviewed_subset_terminal_disposition_gate",
    "terminal_disposition_result_json",
)
