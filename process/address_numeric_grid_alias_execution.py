# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Execution wrapper for validated numeric-grid alias workflows."""

from __future__ import annotations

import asyncio
import os
from typing import Any

from process.address_numeric_grid_alias_support import (
    NumericGridAliasResult,
    _statement_timeout_seconds,
)
from process.ext import address_alias_sql
from process.ptg_parts.rust_scanner import _await_cancellation_resistant_cleanup

_EVIDENCE_ALIAS_LIFECYCLE_TIMEOUT_SECONDS = 120.0
_EVIDENCE_ALIAS_CLEANUP_TIMEOUT_SECONDS = 10.0
_EVIDENCE_ALIAS_TERMINAL_TIMEOUT_SECONDS = 10.0


async def _mark_failed_cancellation_resistant(
    runner: Any,
    schema: str,
    exc: BaseException,
) -> None:
    cleanup_task = asyncio.create_task(runner._mark_execution_failed(schema, exc))
    await _await_cancellation_resistant_cleanup(cleanup_task)


def _configure_evidence_deadlines(runner: Any, request: Any) -> float:
    """Partition the fixed lifecycle budget into work, cleanup, and terminal phases."""
    loop = asyncio.get_running_loop()
    started_monotonic = loop.time()
    runner._lifecycle_deadline_monotonic = (
        started_monotonic + _EVIDENCE_ALIAS_LIFECYCLE_TIMEOUT_SECONDS
    )
    timeout_seconds = min(
        _statement_timeout_seconds(request.timeout),
        max(
            _EVIDENCE_ALIAS_LIFECYCLE_TIMEOUT_SECONDS
            - _EVIDENCE_ALIAS_CLEANUP_TIMEOUT_SECONDS
            - _EVIDENCE_ALIAS_TERMINAL_TIMEOUT_SECONDS,
            0.0,
        ),
    )
    runner._cleanup_deadline_monotonic = min(
        runner._lifecycle_deadline_monotonic
        - _EVIDENCE_ALIAS_TERMINAL_TIMEOUT_SECONDS,
        started_monotonic
        + timeout_seconds
        + _EVIDENCE_ALIAS_CLEANUP_TIMEOUT_SECONDS,
    )
    return timeout_seconds


async def run_validated_alias_workflow(
    runner: Any,
) -> NumericGridAliasResult:
    """Execute one validated alias workflow request."""
    request = runner.request
    operation = address_alias_sql.numeric_grid_alias_mode(request.mode)
    alias_kind = str(request.alias_kind or "").strip()
    ruleset_version = address_alias_sql.alias_ruleset(alias_kind)
    schema = (
        request.schema
        or os.getenv("HLTHPRT_DB_SCHEMA")
        or os.getenv("DB_SCHEMA")
        or "mrf"
    )
    if operation == "off":
        return await runner._off_result(schema, alias_kind)
    try:
        if alias_kind == address_alias_sql.EVIDENCE_ADDRESS_MATCH_ALIAS_KIND:
            timeout_seconds = _configure_evidence_deadlines(runner, request)
            async with asyncio.timeout(timeout_seconds):
                await runner._prepare_and_execute(
                    operation, schema, alias_kind, ruleset_version
                )
        else:
            await runner._prepare_and_execute(
                operation, schema, alias_kind, ruleset_version
            )
        return runner._result()
    except asyncio.CancelledError:
        if runner.execution is not None:
            await _mark_failed_cancellation_resistant(
                runner,
                schema,
                RuntimeError("address alias task cancelled"),
            )
        raise
    except Exception as exc:
        if runner.execution is not None:
            await _mark_failed_cancellation_resistant(runner, schema, exc)
        raise
