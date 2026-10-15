# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed projection HTTP boundary independent of engine transaction tests."""

import asyncio
import json
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.compat import Header

from api import control_query_projection_batch as service
from process.custom_import import query_projection_authorization as wire
from process.custom_import import query_projection_build as build
from process.custom_import.query_projection_build import ProjectionBatchReceipt, ProjectionProgress
from process.custom_import.runner_types import CancellationRequested, LeaseAuthorityLost
from tests import test_custom_import_query_projection_authorization as envelope
from tests.test_custom_import_admission_service import BodyStream


def _wire():
    return SimpleNamespace(
        headers=Header(envelope.headers()),
        body=envelope.legacy.canonical(envelope.BODY),
        method="POST",
        path=wire.PROJECTION_PATH,
        query_string="",
    )


def _committed():
    before = ProjectionProgress("root", 0, None, None, 0, 0, 0)
    return ProjectionBatchReceipt(
        40, 51, 7, 20, 60, before, replace(before, after_entity_binding_id=1, after_selected_value=2030, root_count=1)
    )


async def _serve(request, factory=None):
    return await service.serve_projection_batch(
        request,
        factory,
        keyring=envelope.legacy.keyring(),
        expected_origin=envelope.legacy.ORIGIN,
        trusted_now=envelope.legacy.NOW,
    )


async def test_service_passes_only_verified_envelope_and_existing_pool(monkeypatch):
    operation = AsyncMock(return_value=_committed())
    monkeypatch.setattr(service, "_projection_operation", operation)
    reply = await _serve(_wire(), "existing-pool")
    operation.assert_awaited_once_with("existing-pool", envelope.verify())
    assert reply.status == 200 and reply.headers["Cache-Control"] == "no-store"
    assert json.loads(reply.body) == asdict(_committed()) and len(reply.body) <= 1024


@pytest.mark.parametrize(
    "mutate",
    [
        lambda request: request.headers.add("x-custom-import-projection-key-id", "duplicate"),
        lambda request: request.headers.add("Authorization", "Bearer duplicate"),
        lambda request: request.headers.add("X-Custom-Import-Source-Context", "cross-purpose"),
        lambda request: request.headers.add("X-Custom-Import-Admission-Context", "cross-purpose"),
        lambda request: setattr(request, "query_string", "more=1"),
        lambda request: setattr(request, "body", request.body + b"\n"),
        lambda request: setattr(request, "body", envelope.legacy.canonical(envelope.BODY | {"sql": "closed"})),
    ],
)
async def test_rejection_precedes_engine_access(monkeypatch, mutate):
    request = _wire()
    mutate(request)
    operation = AsyncMock()
    monkeypatch.setattr(service, "_projection_operation", operation)
    assert (await _serve(request)).status == 403
    operation.assert_not_awaited()


@pytest.mark.parametrize(
    "error,status,code",
    [
        (LeaseAuthorityLost("private"), 409, "projection_authority_lost"),
        (CancellationRequested("private"), 409, "projection_authority_lost"),
        (service.ProjectionProgressConflict("private"), 409, "projection_progress_conflict"),
        (RuntimeError("uncertain COMMIT private"), 503, "projection_unavailable"),
    ],
)
async def test_failure_is_sanitized_and_never_claims_rollback(monkeypatch, caplog, error, status, code):
    monkeypatch.setattr(service, "_projection_operation", AsyncMock(side_effect=error))
    reply = await _serve(_wire())
    assert reply.status == status and json.loads(reply.body) == {"error": code}
    assert b"private" not in reply.body and b"rollback" not in reply.body and not caplog.records


async def test_engine_cursor_conflict_retains_the_http_progress_conflict_contract(monkeypatch):
    operation = AsyncMock(side_effect=build.ProjectionCursorConflict("retained cursor changed"))
    monkeypatch.setattr(build, "serve_projection_batch", operation)
    reply = await _serve(_wire(), "existing-pool")
    assert reply.status == 409
    assert json.loads(reply.body) == {"error": "projection_progress_conflict"}
    operation.assert_awaited_once_with("existing-pool", envelope.verify())


@pytest.mark.parametrize(
    "changes",
    [{"generation_id": True}, {"execution_id": 41}, {"after": ProjectionProgress("root", 0, None, None, 0, 0, 0)}],
)
async def test_malformed_service_result_cannot_fabricate_committed_receipt(monkeypatch, changes):
    monkeypatch.setattr(service, "_projection_operation", AsyncMock(return_value=replace(_committed(), **changes)))
    assert (await _serve(_wire())).status == 503


async def test_cancellation_never_synthesizes_receipt(monkeypatch):
    monkeypatch.setattr(service, "_projection_operation", AsyncMock(side_effect=asyncio.CancelledError))
    with pytest.raises(asyncio.CancelledError):
        await _serve(_wire())


def _streaming(*chunks):
    request = _wire()
    request.app = SimpleNamespace(
        ctx=SimpleNamespace(custom_import_admission_authority=(envelope.legacy.keyring(), envelope.legacy.ORIGIN))
    )
    request.stream, request.body = BodyStream(*chunks), b""
    return request


async def test_route_is_closed_without_dedicated_authority():
    assert (
        await service.query_projection_batch(SimpleNamespace(app=SimpleNamespace(ctx=SimpleNamespace())))
    ).status == 503


@pytest.mark.parametrize("chunks", [(b"x" * 513,), (b"x" * 256, b"x" * 257)])
async def test_stream_is_bounded_before_engine_access(monkeypatch, chunks):
    operation = AsyncMock()
    monkeypatch.setattr(service, "serve_projection_batch", operation)
    assert (await service.query_projection_batch(_streaming(*chunks))).status == 413
    operation.assert_not_awaited()


async def test_cross_purpose_header_fails_before_body_read():
    request = _streaming(b"never-read")
    request.headers.add("X-Custom-Import-Source-Key-Id", "current")
    assert (await service.query_projection_batch(request)).status == 403
    assert request.body == b"" and request.stream.request_max_size is None


async def test_unexpected_stream_failure_cannot_start_projection_work(monkeypatch):
    request = _streaming(b"never-read")
    monkeypatch.setattr(
        service.admission, "_receive_admission_body", AsyncMock(side_effect=RuntimeError("stream failed"))
    )
    operation = AsyncMock()
    monkeypatch.setattr(service, "serve_projection_batch", operation)
    reply = await service.query_projection_batch(request)
    assert reply.status == 503 and json.loads(reply.body) == {"error": "projection_unavailable"}
    operation.assert_not_awaited()


async def test_wrapper_uses_existing_pool_and_pinned_origin(monkeypatch):
    request = _streaming(b"x" * 512)
    request.headers.extend([("Host", "forged.invalid"), ("X-Forwarded-Host", "forged.invalid")])
    operation = AsyncMock(return_value=service.admission._reply({"synthetic": True}))
    monkeypatch.setattr(service, "serve_projection_batch", operation)
    monkeypatch.setattr(service, "db", SimpleNamespace(session_factory="existing-pool"))
    assert (await service.query_projection_batch(request)).status == 200
    assert operation.await_args.args == (request, "existing-pool")
    assert operation.await_args.kwargs["expected_origin"] == envelope.legacy.ORIGIN
