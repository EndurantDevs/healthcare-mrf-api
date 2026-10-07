# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fixed-destination batch handoff, response bounds and uncertain delivery."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import ssl
from contextlib import asynccontextmanager
from dataclasses import FrozenInstanceError, replace
from types import SimpleNamespace

import httpx
import pytest

from process.custom_import import admission_authorization as auth
from process.custom_import import admission_worker as worker
from process.custom_import import build_source as staging
from process.custom_import.runner_types import LeaseAuthorityLost
from tests.admission_transport_test_support import authorized_request, writer_tls_context
from tests.test_custom_import_build_source import _request

NOW = dt.datetime(2030, 1, 1, tzinfo=dt.UTC)
ORIGIN = "https://engine.example.invalid"
EXPIRES = NOW + dt.timedelta(seconds=90)
KEYRING = auth.AdmissionKeyring("test-key", (("test-key", b"k" * 32),))
PINS = auth.BatchPins(8, 4, 5, 1)
RECEIPT = {
    "execution_id": 4,
    "build_id": 8,
    "fence": 1,
    "phase": "admission",
    "after_occurrence_id": 7,
    "rows_processed": 2,
    "candidate_error_count": 0,
}


def _canonical(document):
    return json.dumps(document, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("ascii")


def _signed(**changes):
    document = {
        "audience": "custom-import-engine",
        "contract": auth.PERMIT_CONTRACT,
        "dataset_id": 1,
        "definition_revision_id": 2,
        "schema_revision_id": 3,
        "source_binding_revision_id": 6,
        "source_binding_sha256": "a" * 64,
        "issued_at": "2030-01-01T00:00:00Z",
        "expires_at": "2030-01-01T00:01:30Z",
        "idempotency_key": "synthetic-run",
        "issuer": "custom-import-execution-controller",
        "method": "POST",
        "origin": ORIGIN,
        "path": auth.ADMISSION_PATH,
    } | changes
    return auth.sign_permit(
        _canonical(document),
        expected_origin=ORIGIN,
        trusted_now=NOW,
        launch_expires_at=EXPIRES,
        keyring=KEYRING,
    )


@pytest.fixture(autouse=True)
def _clock(monkeypatch):
    monkeypatch.setattr(worker, "_utcnow", lambda: NOW)


def _transport(handler, **changes):
    return worker.AdmissionBatchTransport(
        **(
            {
                "expected_origin": ORIGIN,
                "signed_permit": _signed(),
                "launch_expires_at": EXPIRES,
                "tls_context": writer_tls_context(),
                "transport": httpx.MockTransport(handler),
            }
            | changes
        )
    )


def _build_request(**changes):
    return authorized_request(_request(build_deadline_at=NOW + dt.timedelta(minutes=5)), **changes)


class _Stream(httpx.AsyncByteStream):
    def __init__(self, chunks):
        self.chunks = chunks
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk

    async def aclose(self):
        self.closed = True


def _reply(payload=None, *, status=200, headers=(), raw=None):
    body = json.dumps(RECEIPT if payload is None else payload).encode() if raw is None else raw
    pairs = [("Content-Type", "application/json"), ("Cache-Control", "no-store")]
    overridden_names = {name.lower() for name, _value in headers}
    pairs = [(name, value) for name, value in pairs if name.lower() not in overridden_names]
    return httpx.Response(status, headers=pairs + list(headers), stream=_Stream([body]))


@pytest.mark.parametrize("token", [b"\x00\xff\x80 original bytes\r\n", "original-λ", b"x" * 4_096])
def test_fixed_envelope_preserves_lease_bytes_and_returns_exact_receipt(token):
    calls = []

    async def handler(outbound):
        calls.append(outbound)
        assert outbound.method == "POST"
        assert str(outbound.url) == ORIGIN + auth.ADMISSION_PATH
        assert outbound.content == b'{"build_id":8,"execution_id":4,"expected_after_occurrence_id":5,"fence":1}'
        assert len(outbound.content) <= 512
        for name, expected_value in {
            "accept": "application/json",
            "accept-encoding": "identity",
            "cache-control": "no-store",
        }.items():
            assert outbound.headers.get_list(name) == [expected_value]
        assert "cookie" not in outbound.headers
        verified = auth.verify_request(
            headers=[
                (name, outbound.headers[name])
                for name in (
                    "Content-Type",
                    "Authorization",
                    auth.CONTEXT_HEADER,
                    auth.KEY_ID_HEADER,
                    auth.SIGNATURE_HEADER,
                )
            ],
            body=outbound.content,
            method="POST",
            path=auth.ADMISSION_PATH,
            query_string="",
            trusted_now=NOW,
            expected_origin=ORIGIN,
            keyring=KEYRING,
        )
        assert verified.pins == PINS
        assert verified.token.value == (token.encode() if isinstance(token, str) else token)
        return _reply()

    async def run():
        async with _transport(handler) as transport:
            original = _build_request(lease_token=token)
            request = transport.bind_request(original)
            assert request == original
            assert request.lease_token is original.lease_token
            assert original.authorization_expires_at is None
            assert request.authorization_expires_at == EXPIRES
            assert request.build_deadline_at == original.build_deadline_at
            result = await transport.send(request, PINS)
            assert result == worker.AdmissionBatchReceipt(**RECEIPT)
            assert repr(token) not in repr(transport)
            assert transport.signed_permit.context not in repr(transport)

    asyncio.run(run())
    assert len(calls) == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"expected_origin": "http://engine.example.invalid"},
        {"expected_origin": "https://other.example.invalid"},
        {"expected_origin": ORIGIN + "/"},
        {"launch_expires_at": EXPIRES - dt.timedelta(seconds=1)},
        {"launch_expires_at": EXPIRES.replace(tzinfo=None)},
        {"signed_permit": object()},
        {"transport": object()},
        {"signed_permit": auth.SignedPermit("!", "test-key", "a" * 43)},
    ],
)
def test_constructor_rejects_unbound_or_malformed_context(changes):
    with pytest.raises(worker.AdmissionTransportError, match="^custom_import_admission_transport_unavailable$"):
        _transport(lambda request: _reply(), **changes)


@pytest.mark.parametrize("field,value", [("key_id", "unknown space"), ("signature", "a"), ("context", "e30")])
def test_constructor_validates_header_encoding_without_worker_signing_key(field, value):
    with pytest.raises(worker.AdmissionTransportError):
        _transport(lambda request: _reply(), signed_permit=replace(_signed(), **{field: value}))


def test_client_is_verified_tls_no_environment_no_redirects_and_configuration_frozen(monkeypatch):
    original = httpx.AsyncClient
    configurations = []

    def capture(**kwargs):
        configurations.append(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", capture)

    async def run():
        async with _transport(lambda request: _reply()) as transport:
            with pytest.raises(FrozenInstanceError):
                transport.expected_origin = "https://other.example.invalid"

    asyncio.run(run())
    assert type(configurations[0]["verify"]) is ssl.SSLContext
    assert configurations[0]["verify"].verify_mode == ssl.CERT_REQUIRED
    assert configurations[0]["verify"].check_hostname
    assert configurations[0]["trust_env"] is False
    assert configurations[0]["follow_redirects"] is False


@pytest.mark.parametrize(
    "changes",
    [
        {"dataset_id": 7},
        {"definition_revision_id": 7},
        {"schema_revision_id": 7},
        {"authorization_expires_at": EXPIRES + dt.timedelta(seconds=1)},
    ],
)
def test_only_matching_request_scope_and_original_absolute_expiry_bind(changes):
    async def run():
        async with _transport(lambda request: _reply()) as transport:
            with pytest.raises(worker.AdmissionTransportError):
                transport.bind_request(_build_request(**changes))

    asyncio.run(run())


@pytest.mark.parametrize("status", [201, 302, 307, 403, 409, 500, 503])
def test_non_200_never_follows_redirects_or_retries(status):
    calls = []

    def handler(request):
        calls.append(request)
        return _reply(status=status, headers=[("Location", "https://other.example.invalid/receive")])

    async def run():
        async with _transport(handler) as transport:
            with pytest.raises(worker.AdmissionTransportError):
                await transport.send(transport.bind_request(_build_request()), PINS)

    asyncio.run(run())
    assert len(calls) == 1


@pytest.mark.parametrize(
    "headers",
    [
        [("Cache-Control", "public")],
        [("Cache-Control", "no-store"), ("Cache-Control", "no-store")],
        [("Content-Type", "text/plain")],
        [("Content-Type", "application/json"), ("Content-Type", "application/json")],
        [("Content-Encoding", "gzip")],
        [("Content-Encoding", "identity"), ("Content-Encoding", "identity")],
        [("Content-Length", "513")],
        [("Content-Length", "0")],
        [("Content-Length", "1")],
        [("Content-Length", "1"), ("Content-Length", "1")],
        [("Content-Length", "+1")],
        [("Content-Length", "１２".encode())],
        [("Content-Length", "1000")],
        [("Transfer-Encoding", "gzip")],
        [("Transfer-Encoding", "chunked"), ("Content-Length", "1")],
        [("Set-Cookie", "untrusted=value; Path=/")],
    ],
)
def test_response_transport_metadata_is_closed_and_bounded(headers):
    async def run():
        async with _transport(lambda request: _reply(headers=headers)) as transport:
            with pytest.raises(worker.AdmissionTransportError):
                await transport.send(transport.bind_request(_build_request()), PINS)

    asyncio.run(run())


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"x" * 513,
        b"[]",
        b"null",
        b"\xff",
        b"{" * 400,
        b'{"execution_id":4,"execution_id":4}',
        _canonical(RECEIPT | {"extra": 1}),
        _canonical({name: value for name, value in RECEIPT.items() if name != "fence"}),
        _canonical(RECEIPT | {"execution_id": 5}),
        _canonical(RECEIPT | {"build_id": 9}),
        _canonical(RECEIPT | {"fence": 2}),
        _canonical(RECEIPT | {"fence": True}),
        _canonical(RECEIPT | {"rows_processed": 100_001}),
        _canonical(RECEIPT | {"rows_processed": -1}),
        _canonical(RECEIPT | {"after_occurrence_id": 4}),
        _canonical(RECEIPT | {"after_occurrence_id": 5}),
        _canonical(RECEIPT | {"candidate_error_count": 2**63}),
        _canonical(RECEIPT | {"candidate_error_count": 0.0}),
        _canonical(RECEIPT | {"phase": "verified"}),
        _canonical(RECEIPT | {"phase": []}),
        json.dumps(RECEIPT | {"candidate_error_count": float("nan")}).encode(),
        json.dumps(RECEIPT | {"candidate_error_count": float("inf")}).encode(),
    ],
)
def test_receipt_rejects_duplicate_unknown_wrong_run_or_unsafe_values(raw):
    async def run():
        async with _transport(lambda request: _reply(raw=raw)) as transport:
            with pytest.raises(worker.AdmissionTransportError):
                await transport.send(transport.bind_request(_build_request()), PINS)

    asyncio.run(run())


@pytest.mark.parametrize("phase", ["graph", "rejected"])
def test_empty_eof_receipt_is_valid_without_advancing_cursor(phase):
    async def run():
        document = RECEIPT | {"phase": phase, "rows_processed": 0, "after_occurrence_id": 5}
        async with _transport(lambda request: _reply(document)) as transport:
            assert await transport.send(transport.bind_request(_build_request()), PINS) == worker.AdmissionBatchReceipt(
                **document
            )

    asyncio.run(run())


@pytest.mark.parametrize(
    "phase,cursor",
    [
        ("admission", 5),
        ("admission", 7),
        ("graph", 7),
        ("rejected", 7),
    ],
)
def test_zero_rows_require_eof_phase_and_unchanged_attempted_cursor(phase, cursor):
    async def run():
        document = RECEIPT | {"phase": phase, "rows_processed": 0, "after_occurrence_id": cursor}
        async with _transport(lambda request: _reply(document)) as transport:
            with pytest.raises(worker.AdmissionTransportError):
                await transport.send(transport.bind_request(_build_request()), PINS)

    asyncio.run(run())


def test_response_is_closed_and_cookies_never_forwarded_even_on_next_call():
    replies, calls = [], []

    def handler(request):
        calls.append(request)
        reply = _reply(headers=[("Set-Cookie", "untrusted=value; Path=/")]) if len(calls) == 1 else _reply()
        replies.append(reply)
        return reply

    async def run():
        async with _transport(handler) as transport:
            request = transport.bind_request(_build_request())
            with pytest.raises(worker.AdmissionTransportError):
                await transport.send(request, PINS)
            transport._client.cookies.set("injected", "never-send", domain="engine.example.invalid")
            await transport.send(request, PINS)

    asyncio.run(run())
    assert all("cookie" not in call.headers for call in calls)
    assert all(reply.is_closed for reply in replies)


@pytest.mark.parametrize(
    "error", [httpx.ReadError("secret-token"), httpx.ReadTimeout("secret-token"), OSError("secret-token")]
)
def test_transport_errors_are_metadata_only_and_not_chained(error):
    async def handler(request):
        raise error

    async def run():
        async with _transport(handler) as transport:
            with pytest.raises(worker.AdmissionTransportError) as caught:
                await transport.send(transport.bind_request(_build_request()), PINS)
            assert str(caught.value) == "custom_import_admission_transport_unavailable"
            assert caught.value.__context__ is None
            assert caught.value.__cause__ is None

    asyncio.run(run())


def test_timeout_budget_cannot_extend_original_permit_or_build_deadline(monkeypatch):
    timeouts = []

    def handler(request):
        timeouts.append(request.extensions["timeout"])
        return _reply()

    async def run():
        async with _transport(handler) as transport:
            request = transport.bind_request(_build_request(build_deadline_at=NOW + dt.timedelta(seconds=3)))
            await transport.send(request, PINS)
            monkeypatch.setattr(worker, "_utcnow", lambda: EXPIRES)
            with pytest.raises(LeaseAuthorityLost):
                await transport.send(request, PINS)

    asyncio.run(run())
    assert timeouts == [{"connect": 3.0, "read": 3.0, "write": 3.0, "pool": 3.0}]


@pytest.mark.parametrize("seconds", [1, 60, 120])
def test_lease_and_permit_cap_total_network_budget(seconds):
    timeouts = []

    def handler(request):
        timeouts.append(request.extensions["timeout"])
        return _reply()

    async def run():
        async with _transport(handler) as transport:
            await transport.send(transport.bind_request(_build_request(lease_seconds=seconds)), PINS)

    asyncio.run(run())
    budget = min(seconds, 90)
    assert timeouts == [{"connect": min(5, budget), "read": budget, "write": budget, "pool": budget}]


@pytest.mark.parametrize(
    "pins",
    [
        replace(PINS, build_id=True),
        replace(PINS, build_id=2**63),
        replace(PINS, build_id=1.0),
        replace(PINS, execution_id=5),
        replace(PINS, fence=2),
        replace(PINS, expected_after_occurrence_id=-1),
        replace(PINS, expected_after_occurrence_id=True),
    ],
)
def test_invalid_run_pins_are_rejected_before_network(pins):
    def handler(request):
        pytest.fail("invalid pins must not dispatch")

    async def run():
        async with _transport(handler) as transport:
            with pytest.raises((worker.AdmissionTransportError, auth.AdmissionAuthorizationError)):
                await transport.send(transport.bind_request(_build_request()), pins)

    asyncio.run(run())


def test_stream_bound_stops_before_excess_body_is_retained():
    stream = _Stream([b" " * 256, b" " * 256, b" "])

    async def run():
        async with _transport(
            lambda request: httpx.Response(
                200,
                headers={"Content-Type": "application/json", "Cache-Control": "no-store"},
                stream=stream,
            )
        ) as transport:
            with pytest.raises(worker.AdmissionTransportError):
                await transport.send(transport.bind_request(_build_request()), PINS)

    asyncio.run(run())
    assert stream.closed


def _pages(monkeypatch, *, phases=("admission", "admission"), cursors=(5, 5), fail_close=False, failure=None):
    state = SimpleNamespace(active=False, events=[], requests=[], snapshots=0)

    @asynccontextmanager
    async def page(factory, request, build_id):
        assert not state.active
        assert factory == "existing-pool" and build_id == 8
        index = state.snapshots
        state.snapshots += 1
        state.requests.append(request)
        state.events.append("page-enter")
        if index == 1 and failure is not None:
            raise failure
        state.active = True
        try:
            yield (
                None,
                SimpleNamespace(
                    phase=phases[min(index, len(phases) - 1)],
                    admission_after_occurrence_id=cursors[min(index, len(cursors) - 1)],
                    source_occurrence_count=10,
                    candidate_error_count=0,
                ),
            )
            state.events.append("page-commit")
            if fail_close:
                raise RuntimeError("uncertain-page-commit")
        finally:
            state.active = False
            state.events.append("page-closed")

    monkeypatch.setattr(worker, "_page_session", page)
    return state


def test_local_commit_and_session_close_finish_before_http(monkeypatch):
    state = _pages(monkeypatch)

    def handler(request):
        assert not state.active
        assert state.events == ["page-enter", "page-commit", "page-closed"]
        state.events.append("http")
        return _reply()

    async def run():
        async with _transport(handler) as transport:
            result = await worker.admit_next_batch("existing-pool", _build_request(), 8, transport)
            assert isinstance(result, worker.AdmissionBatchReceipt)

    asyncio.run(run())
    assert state.snapshots == 1


@pytest.mark.parametrize("outcome", ["lost-ack", 409, 503])
@pytest.mark.parametrize("cursor,phase", [(5, "admission"), (7, "admission"), (7, "graph")])
def test_uncertain_delivery_reconciles_original_pins_without_replay(monkeypatch, outcome, cursor, phase):
    state = _pages(monkeypatch, phases=("admission", phase), cursors=(5, cursor))
    calls = []

    def handler(request):
        assert not state.active
        calls.append(request)
        state.events.append("http")
        if outcome == "lost-ack":
            raise httpx.ReadError("synthetic-lost-ack")
        return _reply(status=outcome)

    async def run():
        original = _build_request()
        async with _transport(handler) as transport:
            with pytest.raises(worker.AdmissionReconciliationRequired) as caught:
                await worker.admit_next_batch("existing-pool", original, 8, transport)
            error = caught.value
            assert error.pins == PINS
            assert error.progress.after_occurrence_id == cursor and error.progress.phase == phase
            assert isinstance(error.progress, worker.RetainedAdmissionProgress)
            assert not hasattr(error.progress, "rows_processed")
            assert error.__context__ is None
        assert state.requests[0] is state.requests[1]
        assert state.requests[0].lease_token is original.lease_token
        assert state.requests[0].fence == original.fence
        assert state.requests[0].authorization_expires_at == EXPIRES

    asyncio.run(run())
    assert len(calls) == 1
    assert state.events == [
        "page-enter",
        "page-commit",
        "page-closed",
        "http",
        "page-enter",
        "page-commit",
        "page-closed",
    ]


@pytest.mark.parametrize("phase", ["graph", "rejected", "output", "verifying", "verified"])
def test_existing_post_admission_status_never_resends(monkeypatch, phase):
    _pages(monkeypatch, phases=(phase,))

    def handler(request):
        pytest.fail("completed admission must not send HTTP")

    async def run():
        async with _transport(handler) as transport:
            result = await worker.admit_next_batch("existing-pool", _build_request(), 8, transport)
            assert isinstance(result, worker.RetainedAdmissionProgress) and result.phase == phase

    asyncio.run(run())


def test_failed_local_commit_prevents_http(monkeypatch):
    _pages(monkeypatch, fail_close=True)

    def handler(request):
        pytest.fail("local commit uncertainty must not dispatch")

    async def run():
        async with _transport(handler) as transport:
            with pytest.raises(RuntimeError, match="uncertain-page-commit"):
                await worker.admit_next_batch("existing-pool", _build_request(), 8, transport)

    asyncio.run(run())


@pytest.mark.parametrize("error", [LeaseAuthorityLost("lease lost"), asyncio.CancelledError()])
def test_reconciliation_cannot_renew_authority_or_swallow_cancellation(monkeypatch, error):
    state = _pages(monkeypatch, failure=error)

    async def run():
        async with _transport(lambda request: _reply(status=503)) as transport:
            with pytest.raises(type(error)):
                await worker.admit_next_batch("existing-pool", _build_request(), 8, transport)

    asyncio.run(run())
    assert state.requests[0] is state.requests[1]


def test_unavailable_reconciliation_retains_attempted_cursor_without_claiming_rollback(monkeypatch):
    _pages(monkeypatch, failure=RuntimeError("database unavailable"))

    async def run():
        async with _transport(lambda request: _reply(status=503)) as transport:
            with pytest.raises(worker.AdmissionReconciliationRequired) as caught:
                await worker.admit_next_batch("existing-pool", _build_request(), 8, transport)
            assert caught.value.pins == PINS and caught.value.progress is None
            assert "rollback" not in str(caught.value)

    asyncio.run(run())


def test_http_cancellation_propagates_without_a_second_attempt_or_reconciliation(monkeypatch):
    state = _pages(monkeypatch)

    async def handler(request):
        raise asyncio.CancelledError()

    async def run():
        async with _transport(handler) as transport:
            with pytest.raises(asyncio.CancelledError):
                await worker.admit_next_batch("existing-pool", _build_request(), 8, transport)

    asyncio.run(run())
    assert state.snapshots == 1


def _candidate(**changes):
    return SimpleNamespace(
        **(
            {
                "dataset_id": 1,
                "definition_revision_id": 2,
                "schema_revision_id": 3,
                "source_binding_revision_id": 6,
                "source_binding_sha256": bytes.fromhex("a" * 64),
                "idempotency_key": "synthetic-run",
                "bundle_request": SimpleNamespace(processing_policy=object()),
            }
            | changes
        )
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("dataset_id", 99),
        ("definition_revision_id", 99),
        ("schema_revision_id", 99),
        ("source_binding_revision_id", 99),
        ("source_binding_sha256", b"b" * 32),
        ("idempotency_key", "other-run"),
        ("bundle_request", SimpleNamespace(processing_policy=None)),
    ],
)
async def test_candidate_scope_denial_precedes_claim_or_source(field, value):
    async with _transport(lambda _: pytest.fail("scope denial must not contact HTTP")) as transport:
        transport.require_candidate_binding(_candidate())
        with pytest.raises(worker.AdmissionTransportError):
            transport.require_candidate_binding(_candidate(**{field: value}))


@pytest.mark.parametrize("advanced", [True, False])
async def test_staging_handoff_reconciles_without_replaying_attempted_cursor(monkeypatch, advanced):
    state = _pages(
        monkeypatch, phases=("admission", "admission", "admission", "graph"), cursors=(5, 7 if advanced else 5, 7, 9)
    )
    attempted_cursors = []

    def handler(request):
        assert not state.active
        attempted_cursors.append(json.loads(request.content)["expected_after_occurrence_id"])
        if len(attempted_cursors) == 1:
            raise httpx.ReadError("synthetic lost acknowledgement")
        return _reply(RECEIPT | {"phase": "graph", "after_occurrence_id": 9})

    async with _transport(handler) as transport:
        request = transport.bind_request(_build_request())
        if advanced:
            outcome = await staging._stage_admission_handoff("existing-pool", request, 8, transport)
            assert outcome == staging.SourceBuildResult(8, 4, "graph", 10, 0)
        else:
            with pytest.raises(worker.AdmissionReconciliationRequired):
                await staging._stage_admission_handoff("existing-pool", request, 8, transport)
    assert attempted_cursors == ([5, 7] if advanced else [5])


async def test_terminal_reconciliation_returns_retained_aggregates_not_a_fabricated_receipt(monkeypatch):
    state = _pages(monkeypatch, phases=("admission", "graph"), cursors=(5, 5))
    attempted_requests = []

    def handler(request):
        assert not state.active
        attempted_requests.append(request)
        raise httpx.ReadError("synthetic terminal acknowledgement loss")

    async with _transport(handler) as transport:
        request = transport.bind_request(_build_request())
        outcome = await staging._stage_admission_handoff("existing-pool", request, 8, transport)
        assert outcome == staging.SourceBuildResult(8, 4, "graph", 10, 0)
    assert len(attempted_requests) == 1 and state.snapshots == 3
