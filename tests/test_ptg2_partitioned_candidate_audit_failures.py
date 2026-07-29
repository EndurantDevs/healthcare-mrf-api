from __future__ import annotations

import asyncio
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest
from aiohttp import web

from process.ptg_parts import ptg2_partitioned_candidate_audit as audit
from process.ptg_parts.ptg2_batch_candidate_audit import (
    BatchCandidateAuditContractError,
    BatchCandidateAuditTransportError,
)
from tests.test_ptg2_partitioned_candidate_audit import (
    _http,
    _one_request_plan,
)


def _assert_partition_identity(failure, request):
    assert failure.partition_index == request.partition_index
    assert failure.partition_count == request.partition_count
    assert failure.partition_digest == request.partition_digest
    assert failure.plan_digest == request.plan_digest
    assert failure.request_digest == request.request_digest


@pytest.mark.asyncio
async def test_primary_failure_survives_secondary_task_cancellation(
    monkeypatch,
):
    plan = _one_request_plan()
    request = plan.requests[0]
    primary_failure = BatchCandidateAuditContractError(
        "batch_endpoint_rejected_400_forward_occurrence_retention_limit_exceeded"
    ).for_partition(
        partition_index=request.partition_index,
        partition_count=request.partition_count,
        partition_digest=request.partition_digest,
        plan_digest=request.plan_digest,
        request_digest=request.request_digest,
    )
    secondary_cleanup_observed = asyncio.Event()

    async def fail_primary():
        raise primary_failure

    async def fail_during_cancellation():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            secondary_cleanup_observed.set()
            raise RuntimeError("secondary cleanup failure")

    def create_partition_tasks(**_kwargs):
        return [
            asyncio.create_task(fail_primary()),
            asyncio.create_task(fail_during_cancellation()),
        ]

    monkeypatch.setattr(
        audit,
        "_create_partition_tasks",
        create_partition_tasks,
    )
    failure_callback = AsyncMock()
    with pytest.raises(BatchCandidateAuditContractError) as exc_info:
        await audit._execute_partition_plan(
            plan=plan,
            http_config=_http("https://audit.internal.example"),
            failure_callback=failure_callback,
        )

    assert exc_info.value is primary_failure
    assert secondary_cleanup_observed.is_set()
    callback_failure = failure_callback.await_args.args[2]
    assert callback_failure is primary_failure
    assert failure_callback.await_args.args[:2] == (0, 1)


@pytest.mark.asyncio
async def test_partition_timeout_is_terminal_transport_failure(
    unused_tcp_port,
):
    async def handler(_request):
        await asyncio.sleep(0.1)
        return web.json_response({})

    application = web.Application()
    application.router.add_post(
        "/api/v1/pricing/providers/audit-source-witness-batch",
        handler,
    )
    runner = web.AppRunner(application)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", unused_tcp_port)
    await site.start()
    try:
        with pytest.raises(
            BatchCandidateAuditTransportError,
            match="deadline",
        ) as exc_info:
            await audit._execute_partition_plan(
                plan=_one_request_plan(),
                http_config=replace(
                    _http(f"http://127.0.0.1:{unused_tcp_port}"),
                    deadline_seconds=0.01,
                ),
            )
    finally:
        await runner.cleanup()

    _assert_partition_identity(
        exc_info.value,
        _one_request_plan().requests[0],
    )
