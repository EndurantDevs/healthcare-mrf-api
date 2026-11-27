"""Endpoint deadline for /npi match-candidate database statements."""

import asyncio
import contextlib
import contextvars
import math
import time
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process.custom_import.read_core import _is_statement_timeout

_DEADLINE = contextvars.ContextVar("match_candidate_deadline", default=None)
_SET_STATEMENT_TIMEOUT = text("SELECT set_config('statement_timeout', :timeout_ms, true)")


async def cap_statement_at_deadline(session: Any) -> None:
    """Make the next statement in this transaction stop at the endpoint deadline."""
    deadline = _DEADLINE.get()
    if deadline is None or session is None:
        return
    remaining_ms = max(1, math.ceil((deadline - time.monotonic()) * 1000))
    # Transaction-local, so no restore: the endpoint returns right after its last stage.
    await session.execute(_SET_STATEMENT_TIMEOUT, {"timeout_ms": str(remaining_ms)})


@contextlib.asynccontextmanager
async def match_candidate_deadline(deadline: float, session: Any = None):
    """Bound one stage on the client and the server; a server timeout surfaces as TimeoutError."""
    remaining_seconds = deadline - time.monotonic()
    if remaining_seconds <= 0:
        raise asyncio.TimeoutError()
    token = _DEADLINE.set(deadline)
    try:
        async with asyncio.timeout(remaining_seconds):
            await cap_statement_at_deadline(session)
            yield
    except DBAPIError as exc:
        if _is_statement_timeout(exc):
            raise asyncio.TimeoutError() from exc
        raise
    finally:
        _DEADLINE.reset(token)
