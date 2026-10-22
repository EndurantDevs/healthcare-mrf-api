# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Default-off facade for exact terminal root retirement preview and apply."""

from __future__ import annotations

import asyncio
import json
import re
from typing import Any

from process.provider_directory_terminal_root_retirement_contract import (
    RETIREMENT_TIMEOUT_SECONDS,
    TerminalRootRetirementError,
    TerminalRootRetirementRequest,
    TerminalRootRetirementResult,
    require_terminal_root_retirement_gate,
)
from process.provider_directory_terminal_root_retirement_store import (
    apply_terminal_root_retirement_transaction,
    preview_terminal_root_retirement_transaction,
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def retirement_preview_json(evidence_sha256: str) -> str:
    """Render only the token needed for the separately authorized apply."""

    if type(evidence_sha256) is not str or _SHA256.fullmatch(evidence_sha256) is None:
        raise TerminalRootRetirementError("state_invalid")
    return json.dumps(
        {"evidence_sha256": evidence_sha256, "status": "ok"},
        sort_keys=True,
        separators=(",", ":"),
    )


def _runtime_database(database: Any | None) -> Any:
    if database is not None:
        return database
    from db.connection import db

    return db


def _runtime_error(error: Exception) -> TerminalRootRetirementError:
    cause = getattr(error, "orig", error)
    locked = any(getattr(item, "sqlstate", None) == "55P03" for item in (error, cause))
    return TerminalRootRetirementError("busy" if locked else "state_invalid")


def _operation_timeout(request: TerminalRootRetirementRequest) -> int:
    # Complete retained-payload evidence can exceed the ordinary maintenance budget.
    first_acquisition = isinstance(request, TerminalRootRetirementRequest) and request.expect_no_current_dataset
    return 3600 if first_acquisition else RETIREMENT_TIMEOUT_SECONDS


async def preview_terminal_root_retirement(
    request: TerminalRootRetirementRequest,
    *,
    database: Any | None = None,
) -> str:
    """Return one closed evidence token for the exact request selectors."""

    require_terminal_root_retirement_gate()
    try:
        async with asyncio.timeout(_operation_timeout(request)):
            return await preview_terminal_root_retirement_transaction(_runtime_database(database), request)
    except asyncio.CancelledError, TimeoutError:
        raise
    except TerminalRootRetirementError:
        raise
    except Exception as error:
        raise _runtime_error(error) from None


async def apply_terminal_root_retirement(
    request: TerminalRootRetirementRequest,
    *,
    database: Any | None = None,
) -> TerminalRootRetirementResult:
    """Apply or idempotently verify the exact evidence-bound retirement."""

    require_terminal_root_retirement_gate()
    try:
        async with asyncio.timeout(_operation_timeout(request)):
            return await apply_terminal_root_retirement_transaction(_runtime_database(database), request)
    except asyncio.CancelledError, TimeoutError:
        raise
    except TerminalRootRetirementError:
        raise
    except Exception as error:
        raise _runtime_error(error) from None


__all__ = (
    "apply_terminal_root_retirement",
    "preview_terminal_root_retirement",
    "retirement_preview_json",
)
