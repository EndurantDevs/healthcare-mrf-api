# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Thin SOURCE HTTP boundary; engine transaction behavior is tested separately."""

import asyncio
import json
import sys
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.compat import Header

from api import control_admission_batch as admission
from api import control_source_batch as service
from process.custom_import import source_authorization as source
from process.custom_import import source_worker
from process.custom_import.runner_types import CancellationRequested, LeaseAuthorityLost
from tests import test_custom_import_source_authorization as envelope
from tests.test_custom_import_admission_service import BodyStream


def _wire():
    request = SimpleNamespace(
        headers=Header(envelope.headers()),
        body=envelope.legacy.canonical(envelope.BODY),
        method="POST",
        path=source.SOURCE_PATH,
        query_string="",
    )
    return request, envelope.verify()


def _committed():
    return source_worker.SourceBatchReceipt(
        40,
        51,
        7,
        2,
        60,
        source.SourceCursor(**envelope.CURSOR),
        source.SourceCursor(1, 3, 3, 1),
        "source",
        3,
        False,
    )


async def _serve(request, session_factory=None):
    return await service.serve_source_batch(
        request,
        session_factory,
        keyring=envelope.legacy.keyring(),
        expected_origin=envelope.legacy.ORIGIN,
        trusted_now=envelope.legacy.NOW,
    )


async def test_handler_forwards_only_verified_closed_inputs_and_returns_committed_receipt(monkeypatch):
    request, verified = _wire()
    operation = AsyncMock(return_value=_committed())
    monkeypatch.setattr(service, "_source_operation", operation)
    reply = await _serve(request, "existing-pool")
    operation.assert_awaited_once_with("existing-pool", verified)
    assert reply.status == 200 and reply.headers["Cache-Control"] == "no-store"
    assert json.loads(reply.body) == asdict(_committed()) and len(reply.body) <= 1_024


async def test_fixed_step_seam_converts_cursor_and_preserves_exact_token_and_permit(monkeypatch):
    _, verified = _wire()
    step = AsyncMock(return_value=_committed())
    module = SimpleNamespace(
        SourceCursor=source.SourceCursor, SourceCursorConflict=LookupError, serve_source_batch=step
    )
    monkeypatch.setitem(sys.modules, "process.custom_import.source_batch", module)
    assert await service._source_operation("existing-pool", verified) == _committed()
    assert step.await_args.args == ("existing-pool",)
    assert step.await_args.kwargs == {
        "execution_id": 40,
        "build_id": 51,
        "fence": 7,
        "stream_slot": 2,
        "expected_cursor": verified.pins.expected_cursor,
        "lease_token": verified.token.value,
        "source_permit": verified.permit,
    }
    assert step.await_args.kwargs["source_permit"] is verified.permit


async def test_engine_cursor_conflict_cannot_leak_cursors_or_grant_retry(monkeypatch):
    _, verified = _wire()
    step = AsyncMock(side_effect=LookupError("do-not-log"))
    module = SimpleNamespace(
        SourceCursor=source.SourceCursor, SourceCursorConflict=LookupError, serve_source_batch=step
    )
    monkeypatch.setitem(sys.modules, "process.custom_import.source_batch", module)
    with pytest.raises(service.SourceProgressConflict, match="^custom_import_source_progress_conflict$"):
        await service._source_operation(None, verified)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda request: request.headers.add(source.KEY_ID_HEADER.lower(), "current"),
        lambda request: request.headers.add("Authorization", "Bearer duplicate"),
        lambda request: request.headers.add("X-Custom-Import-Source-Extra", "1"),
        lambda request: request.headers.add("X-Custom-Import-Admission-Context", "cross-purpose"),
        lambda request: setattr(request, "body", request.body + b"\n"),
        lambda request: setattr(request, "query_string", "more=1"),
        lambda request: setattr(request, "body", envelope.legacy.canonical(envelope.BODY | {"row": {}})),
    ],
)
async def test_denial_precedes_database_or_source_work(monkeypatch, mutate):
    request, _ = _wire()
    mutate(request)
    operation = AsyncMock()
    monkeypatch.setattr(service, "_source_operation", operation)
    reply = await _serve(request)
    assert reply.status == 403 and reply.headers["Cache-Control"] == "no-store"
    operation.assert_not_awaited()


@pytest.mark.parametrize(
    "error,status,code",
    [
        (LeaseAuthorityLost("do-not-log"), 409, "source_authority_lost"),
        (CancellationRequested("do-not-log"), 409, "source_authority_lost"),
        (service.SourceProgressConflict("do-not-log"), 409, "source_progress_conflict"),
        (RuntimeError("uncertain COMMIT do-not-log"), 503, "source_unavailable"),
        (OSError("do-not-log"), 503, "source_unavailable"),
    ],
)
async def test_failure_is_metadata_only_no_retry_or_rollback_claim(monkeypatch, caplog, error, status, code):
    request, _ = _wire()
    operation = AsyncMock(side_effect=error)
    monkeypatch.setattr(service, "_source_operation", operation)
    reply = await _serve(request)
    assert reply.status == status and json.loads(reply.body) == {"error": code}
    assert reply.headers["Cache-Control"] == "no-store" and not caplog.records
    assert b"rollback" not in reply.body and b"do-not-log" not in reply.body
    operation.assert_awaited_once()


async def test_cancelled_handler_does_not_synthesize_receipt(monkeypatch):
    request, _ = _wire()
    monkeypatch.setattr(service, "_source_operation", AsyncMock(side_effect=asyncio.CancelledError))
    with pytest.raises(asyncio.CancelledError):
        await _serve(request)


@pytest.mark.parametrize(
    "changes",
    [
        {"execution_id": 99},
        {"before": source.SourceCursor(2, 0, 0, 0)},
        {"rows_processed": 0},
        {"phase": "admission"},
        {"capture_bundle_id": True},
    ],
)
async def test_malformed_service_result_is_unavailable_not_a_false_committed_ack(monkeypatch, changes):
    request, _ = _wire()
    monkeypatch.setattr(service, "_source_operation", AsyncMock(return_value=replace(_committed(), **changes)))
    assert (await _serve(request)).status == 503


def _streaming(*chunks):
    request, _ = _wire()
    request.app = SimpleNamespace(
        ctx=SimpleNamespace(
            custom_import_admission_authority=(
                envelope.legacy.keyring(),
                envelope.legacy.ORIGIN,
            )
        )
    )
    request.stream = BodyStream(*chunks)
    request.body = b""
    return request


async def test_route_default_off_without_shared_dedicated_authority():
    reply = await service.source_batch(SimpleNamespace(app=SimpleNamespace(ctx=SimpleNamespace())))
    assert reply.status == 503 and reply.headers["Cache-Control"] == "no-store"


@pytest.mark.parametrize("chunks", [(b"x" * 513,), (b"x" * 256, b"x" * 257)])
async def test_stream_is_bounded_before_service(monkeypatch, chunks):
    operation = AsyncMock()
    monkeypatch.setattr(service, "serve_source_batch", operation)
    reply = await service.source_batch(_streaming(*chunks))
    assert reply.status == 413
    operation.assert_not_awaited()


async def test_wrong_purpose_header_is_rejected_before_body_accumulation(monkeypatch):
    request = _streaming(b"never-read")
    request.headers.add("X-Custom-Import-Admission-Key-Id", "current")
    assert (await service.source_batch(request)).status == 403
    assert request.stream.request_max_size is None and request.body == b""


async def test_wrapper_uses_existing_pool_and_independent_origin(monkeypatch):
    request = _streaming(b"x" * 512)
    request.headers.extend([("Host", "forged.invalid"), ("X-Forwarded-Host", "forged.invalid")])
    operation = AsyncMock(return_value=admission._reply({"synthetic": True}))
    monkeypatch.setattr(service, "serve_source_batch", operation)
    monkeypatch.setattr(service, "db", SimpleNamespace(session_factory="existing-pool"))
    assert (await service.source_batch(request)).status == 200
    assert operation.await_args.args == (request, "existing-pool")
    assert operation.await_args.kwargs["expected_origin"] == envelope.legacy.ORIGIN
    assert request.body == b"x" * 512
