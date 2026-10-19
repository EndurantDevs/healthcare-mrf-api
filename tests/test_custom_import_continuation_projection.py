# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Continuation nonce transport and owner handoff retain exact projection cursors."""

import asyncio
import datetime as dt
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from sanic.compat import Header

from process.custom_import import continuation_projection as projection
from process.custom_import import query_projection_worker as worker
from process.custom_import.continuation_runtime import BuildContinuationContext
from process.custom_import.query_projection_build import (
    ProjectionBatchReceipt,
    ProjectionCursorConflict,
    ProjectionProgress,
)
from process.custom_import.runner_types import LeaseAuthorityLost
from tests.admission_transport_test_support import writer_tls_context
from tests.test_custom_import_admission_service import BodyStream as AdmissionBodyStream
from tests.test_custom_import_admission_worker import ORIGIN, _canonical, _reply
from tests.test_custom_import_build_source import _request
from tests.test_custom_import_continuation_runtime import _continued

NONCE = b"synthetic-processing-nonce-value"
AUTHORIZATION_ID = "a" * 64
BEFORE = ProjectionProgress("root", 0, None, None, 0, 0, 0)
AFTER = replace(BEFORE, after_entity_binding_id=1, after_selected_value=10, root_count=1)
RECEIPT = ProjectionBatchReceipt(4, 19, 1, 20, 8, BEFORE, AFTER)
BODY = dict(authorization_id=AUTHORIZATION_ID, continuation_id=41, build_id=19, expected_progress=asdict(BEFORE))


class BodyStream(AdmissionBodyStream):
    async def __aiter__(self):
        assert self.request_max_size == projection.MAX_BODY_BYTES
        for chunk in self.chunks:
            yield chunk


def _build_request():
    return _request(
        continuation=BuildContinuationContext(41, 19, b"p" * 32),
        lease_token=NONCE,
        build_deadline_at=dt.datetime(2000, 1, 1, tzinfo=dt.UTC),
    )


def _wire(body=BODY):
    return SimpleNamespace(
        headers=Header(
            {"Content-Type": "application/json", "Authorization": projection.wire.encode_lease_bearer(NONCE)}
        ),
        body=_canonical(body),
        method="POST",
        path=projection.PROJECTION_PATH,
        query_string="",
    )


def _transport(handler, **changes):
    return projection.ContinuationProjectionTransport(
        **(
            dict(
                expected_origin=ORIGIN,
                authorization_id=AUTHORIZATION_ID,
                continuation_id=41,
                build_id=19,
                nonce=NONCE,
                tls_context=writer_tls_context(),
                transport=httpx.MockTransport(handler),
            )
            | changes
        )
    )


async def test_fixed_nonce_transport_after_expiry():
    calls = []

    def handler(outbound):
        calls.append(outbound)
        assert str(outbound.url) == ORIGIN + projection.PROJECTION_PATH
        assert outbound.content == _canonical(BODY)
        assert outbound.headers["authorization"] == projection.wire.encode_lease_bearer(NONCE)
        assert "cookie" not in outbound.headers
        assert not any(key.startswith("x-custom-import-") for key in outbound.headers)
        assert NONCE not in outbound.content
        return _reply(asdict(RECEIPT))

    async with _transport(handler) as transport:
        assert repr(NONCE) not in repr(transport)
        transport._client.cookies.set("synthetic", "not-forwarded")
        result = await transport.send(
            _build_request(), worker._pins(19, 4, 1, BEFORE), generation_id=20, capture_bundle_id=8, before=BEFORE
        )
    assert result == RECEIPT and len(calls) == 1


@pytest.mark.parametrize(
    "changes", [{"continuation_id": 42}, {"build_id": 20}, {"lease_token": b"x" * 32}, {"continuation": None}]
)
async def test_transport_rejects_other_epoch_without_request(changes):
    request = _build_request()
    if "continuation_id" in changes or "build_id" in changes:
        request = replace(request, continuation=replace(request.continuation, **changes))
    else:
        request = replace(request, **changes)
    handler = Mock()
    async with _transport(handler) as transport:
        with pytest.raises(projection.worker.AdmissionTransportError):
            transport.bind_request(request)
    handler.assert_not_called()


@pytest.mark.parametrize(
    "origin",
    ["http://engine.example.invalid", ORIGIN + "/path", "https://user@engine.example.invalid", ORIGIN + "?x=1"],
)
def test_transport_rejects_unapproved_origin_shape(origin):
    with pytest.raises(ValueError):
        _transport(Mock(), expected_origin=origin)


@pytest.mark.parametrize(
    "status,headers",
    [(302, [("Location", ORIGIN)]), (200, [("Set-Cookie", "x=y")]), (200, [("Cache-Control", "public")])],
)
async def test_transport_never_follows_redirect_or_accepts_cached_cookie_response(status, headers):
    calls = []
    async with _transport(
        lambda outbound: calls.append(outbound) or _reply(asdict(RECEIPT), status=status, headers=headers)
    ) as transport:
        with pytest.raises(projection.worker.AdmissionTransportError):
            await transport.send(
                _build_request(), worker._pins(19, 4, 1, BEFORE), generation_id=20, capture_bundle_id=8, before=BEFORE
            )
        assert not transport._client.cookies
    assert len(calls) == 1


async def test_lost_ack_uses_existing_single_read_reconciliation_without_resend(monkeypatch):
    observations = [worker.RetainedProjectionProgress(4, 19, 1, 20, 8, progress) for progress in (BEFORE, AFTER)]
    observe = AsyncMock(side_effect=observations)
    monkeypatch.setattr(worker, "_retained_progress", observe)
    calls = []

    def failed(outbound):
        calls.append(outbound)
        raise httpx.ReadError("synthetic uncertain response")

    async with _transport(failed) as transport:
        with pytest.raises(worker.ProjectionReconciliationRequired) as caught:
            await worker.projection_next_batch("worker-pool", _build_request(), 19, transport)
    assert caught.value.progress == observations[1] and len(calls) == 1 and observe.await_count == 2


async def test_handler_runs_exactly_one_owner_step_after_authenticated_preload(monkeypatch):
    retained = _build_request()
    load = AsyncMock(return_value=(retained, 20, 8, dt.datetime.now(dt.UTC) + dt.timedelta(minutes=5)))
    step = AsyncMock(return_value=RECEIPT)
    monkeypatch.setattr(projection, "_retained_request", load)
    monkeypatch.setattr(projection, "projection_step", step)
    reply = await projection.serve_continuation_projection(_wire(), "owner-pool")
    assert reply.status == 200 and reply.headers["Cache-Control"] == "no-store"
    load.assert_awaited_once_with("owner-pool", BODY, NONCE)
    step.assert_awaited_once_with("owner-pool", retained, 19, expected=BEFORE)


@pytest.mark.parametrize(
    "change",
    ["duplicate", "other_purpose", "cookie", "query", "unknown", "raw_nonce", "progress", "noncanonical", "method"],
)
async def test_wire_rejection_precedes_database_and_never_echoes_nonce(monkeypatch, change):
    request = _wire()
    mutation_by_name = {
        "duplicate": lambda: request.headers.add("Authorization", request.headers["Authorization"]),
        "other_purpose": lambda: request.headers.add("X-Custom-Import-Projection-Context", "synthetic"),
        "cookie": lambda: request.headers.add("Cookie", "synthetic"),
        "query": lambda: setattr(request, "query_string", "unexpected=true"),
        "unknown": lambda: setattr(request, "body", _canonical(BODY | {"lease_seconds": 3600})),
        "raw_nonce": lambda: setattr(request, "body", _canonical(BODY | {"nonce": NONCE.hex()})),
        "progress": lambda: setattr(
            request, "body", _canonical(BODY | {"expected_progress": asdict(BEFORE) | {"root_count": True}})
        ),
        "noncanonical": lambda: setattr(request, "body", request.body + b"\n"),
        "method": lambda: setattr(request, "method", "GET"),
    }
    mutation_by_name[change]()
    load = AsyncMock()
    monkeypatch.setattr(projection, "_retained_request", load)
    reply = await projection.serve_continuation_projection(request, "owner-pool")
    assert reply.status == 403 and reply.headers["Cache-Control"] == "no-store" and NONCE not in reply.body
    load.assert_not_awaited()


async def test_stream_limit_and_stream_failure_are_sanitized_before_database(monkeypatch):
    load = AsyncMock()
    monkeypatch.setattr(projection, "_retained_request", load)
    request = _wire()
    request.stream = BodyStream(b"x" * 2048, b"x")
    assert (await projection.serve_continuation_projection(request, None)).status == 413
    assert request.stream.request_max_size == 2048
    monkeypatch.setattr(
        projection, "receive_continuation_projection_body", AsyncMock(side_effect=RuntimeError("private stream"))
    )
    reply = await projection.serve_continuation_projection(request, None)
    assert reply.status == 503 and b"private" not in reply.body
    load.assert_not_awaited()


@pytest.mark.parametrize(
    "error,status",
    [
        (ProjectionCursorConflict("private"), 409),
        (LeaseAuthorityLost("private"), 409),
        (RuntimeError("private COMMIT"), 503),
    ],
)
async def test_operation_failure_is_sanitized_without_retry(monkeypatch, error, status):
    monkeypatch.setattr(
        projection,
        "_retained_request",
        AsyncMock(return_value=(_build_request(), 20, 8, dt.datetime.now(dt.UTC) + dt.timedelta(minutes=5))),
    )
    step = AsyncMock(side_effect=error)
    monkeypatch.setattr(projection, "projection_step", step)
    reply = await projection.serve_continuation_projection(_wire(), "owner-pool")
    assert reply.status == status and b"private" not in reply.body and b"rollback" not in reply.body
    step.assert_awaited_once()


def _preload_state():
    request, execution, lease, authority, now = _continued()
    request = replace(request, lease_token=NONCE)
    authority.authorization_id = AUTHORIZATION_ID
    authority.nonce_sha256 = lease.token_sha256 = projection.lease_token_sha256(NONCE)
    lease.heartbeat_at = lease.expires_at - dt.timedelta(seconds=request.lease_seconds)
    build = SimpleNamespace(
        **{
            key: getattr(authority, key)
            for key in (
                "build_id",
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
                "execution_id",
                "capture_bundle_id",
                "request_identity_sha256",
                "base_generation_id",
                "base_pointer_version",
            )
        },
        producing_fence=authority.producer_fence,
        producing_token_sha256=authority.producer_token_sha256,
        build_deadline_at=authority.original_deadline_at,
        generation_id=20,
        phase="output",
    )
    return request, execution, lease, authority, now, build


def _mock_preload(monkeypatch, state):
    request, execution, lease, authority, now, build = state
    session = SimpleNamespace(execute=AsyncMock(), get=AsyncMock(side_effect=[authority, execution, lease, build]))

    @asynccontextmanager
    async def reading(_factory):
        yield session

    load = AsyncMock(side_effect=lambda *args: replace(request, lease_seconds=args[-1]))
    monkeypatch.setattr(projection, "_read_session", reading)
    monkeypatch.setattr(projection, "database_now", AsyncMock(return_value=now))
    monkeypatch.setattr(projection, "_request", load)
    return session, load


@pytest.mark.parametrize(
    "mutation", [None, "nonce", "epoch", "unstarted", "expired", "canceling", "producer", "authorization", "build"]
)
async def test_readonly_preload_authenticates_before_renewal(monkeypatch, mutation):
    state = _preload_state()
    request, execution, lease, authority, now, build = state
    mutation_by_name = {
        "nonce": (lease, "token_sha256", b"x" * 32),
        "epoch": (lease, "continuation_id", lease.continuation_id + 1),
        "unstarted": (lease, "continuation_started_at", None),
        "expired": (lease, "expires_at", now),
        "canceling": (execution, "state", "canceling"),
        "producer": (build, "producing_token_sha256", b"x" * 32),
        "authorization": (authority, "authorization_id", "b" * 64),
        "build": (build, "capture_bundle_id", build.capture_bundle_id + 1),
    }
    if mutation is not None:
        setattr(*mutation_by_name[mutation])
    session, load = _mock_preload(monkeypatch, state)
    if mutation is None:
        retained_request = await projection._retained_request("owner-pool", BODY, NONCE)
        assert retained_request[0] == request
        load.assert_awaited_once_with(session, authority, build, NONCE, request.lease_seconds)
    else:
        with pytest.raises(projection.ContinuationProjectionForbidden):
            await projection._retained_request("owner-pool", BODY, NONCE)
        load.assert_not_awaited()
    assert [str(call.args[0]) for call in session.execute.await_args_list] == [
        "SET TRANSACTION READ ONLY",
        "SET LOCAL statement_timeout = '2000ms'",
    ]


@pytest.mark.parametrize(
    "duration,elapsed,expected",
    [(1, 0, 1), (1, 0.75, 1), (30, 0, 30), (30, 29.75, 30), (3600, 0, 3600), (3600, 120, 3600), (7200, 120, 3600)],
)
async def test_preload_retains_renewal_interval(monkeypatch, duration, elapsed, expected):
    state = _preload_state()
    _, _, lease, authority, now, build = state
    lease.heartbeat_at = now - dt.timedelta(seconds=elapsed)
    lease.expires_at = lease.heartbeat_at + dt.timedelta(seconds=duration)
    session, load = _mock_preload(monkeypatch, state)
    retained, *_ = await projection._retained_request("owner-pool", BODY, NONCE)
    assert retained.lease_seconds == expected
    load.assert_awaited_once_with(session, authority, build, NONCE, expected)


@pytest.mark.parametrize("mutation", ["missing_heartbeat", "future_heartbeat", "subsecond_interval", "expired"])
async def test_preload_rejects_invalid_grant(monkeypatch, mutation):
    state = _preload_state()
    _, _, lease, _, now, _ = state
    if mutation == "missing_heartbeat":
        lease.heartbeat_at = None
    elif mutation == "future_heartbeat":
        lease.heartbeat_at = now + dt.timedelta(seconds=1)
    elif mutation == "subsecond_interval":
        lease.heartbeat_at, lease.expires_at = now, now + dt.timedelta(milliseconds=500)
    else:
        lease.heartbeat_at, lease.expires_at = now - dt.timedelta(seconds=30), now
    _, load = _mock_preload(monkeypatch, state)
    with pytest.raises(projection.ContinuationProjectionForbidden):
        await projection._retained_request("owner-pool", BODY, NONCE)
    load.assert_not_awaited()


async def test_cancellation_propagates_without_fabricated_receipt(monkeypatch):
    monkeypatch.setattr(projection, "_retained_request", AsyncMock(side_effect=asyncio.CancelledError))
    with pytest.raises(asyncio.CancelledError):
        await projection.serve_continuation_projection(_wire(), None)


async def test_factory_uses_only_fixed_ca_and_explicit_origin(monkeypatch):
    monkeypatch.setenv(projection.ORIGIN_ENV, ORIGIN)
    monkeypatch.setattr(projection, "configured_fields", lambda _: ("synthetic",))
    tls = Mock(return_value=writer_tls_context())
    monkeypatch.setattr(projection.ssl, "create_default_context", tls)
    client = AsyncMock()
    client.__aenter__.return_value = client
    constructor = Mock(return_value=client)
    monkeypatch.setattr(projection, "ContinuationProjectionTransport", constructor)
    async with projection.continuation_projection_transport(BODY, _build_request()) as received:
        assert received is client
    tls.assert_called_once_with(cafile="/var/run/custom-import-processing/writer-ca.pem")
    assert constructor.call_args.kwargs == dict(
        expected_origin=ORIGIN,
        authorization_id=AUTHORIZATION_ID,
        continuation_id=41,
        build_id=19,
        nonce=NONCE,
        tls_context=tls.return_value,
    )
    client.__aexit__.assert_awaited_once()


async def test_factory_skips_transport_only_when_no_fields_are_configured(monkeypatch):
    monkeypatch.delenv(projection.ORIGIN_ENV, raising=False)
    monkeypatch.setattr(projection, "configured_fields", lambda _: ())
    constructor = Mock()
    monkeypatch.setattr(projection, "ContinuationProjectionTransport", constructor)
    async with projection.continuation_projection_transport(BODY, _build_request()) as received:
        assert received is None
    constructor.assert_not_called()
    monkeypatch.setattr(projection, "configured_fields", lambda _: ("synthetic",))
    with pytest.raises(ValueError):
        async with projection.continuation_projection_transport(BODY, _build_request()):
            pytest.fail("missing origin must fail closed")


def test_transport_requires_verified_tls_hostname():
    context = writer_tls_context()
    context.check_hostname = False
    with pytest.raises(projection.worker.AdmissionTransportError):
        _transport(Mock(), tls_context=context)
