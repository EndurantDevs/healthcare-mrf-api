# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Complete SOURCE receipts, fixed transport and lost-ACK transaction boundaries."""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from types import SimpleNamespace

import httpx
import pytest

from process.custom_import import admission_authorization as auth
from process.custom_import import admission_worker as admission
from process.custom_import import source_authorization as wire
from process.custom_import import source_worker as worker
from process.custom_import.runner_types import CancellationRequested, LeaseAuthorityLost
from tests import test_custom_import_admission_worker as legacy
from tests.admission_transport_test_support import writer_tls_context

BEFORE = wire.SourceCursor(2, 10, 40, 7)
AFTER = wire.SourceCursor(2, 13, 43, 8)
PINS = wire.SourceBatchPins(8, 4, 1, 2, BEFORE)
DOCUMENT = {
    "execution_id": 4,
    "build_id": 8,
    "fence": 1,
    "stream_slot": 2,
    "capture_bundle_id": 9,
    "before": asdict(BEFORE),
    "after": asdict(AFTER),
    "phase": "source",
    "rows_processed": 3,
    "stream_complete": False,
}


@pytest.fixture(autouse=True)
def _clock(monkeypatch):
    monkeypatch.setattr(admission, "_utcnow", lambda: legacy.NOW)


def signed():
    context = json.loads(auth._base64url_decode(legacy._signed().context, 2_048))
    context.update(contract=wire.PERMIT_CONTRACT, path=wire.SOURCE_PATH)
    return auth.sign_permit(
        legacy._canonical(context),
        expected_origin=legacy.ORIGIN,
        trusted_now=legacy.NOW,
        launch_expires_at=legacy.EXPIRES,
        keyring=legacy.KEYRING,
        is_source=True,
    )


def transport(handler, **changes):
    return worker.SourceBatchTransport(
        **(
            {
                "expected_origin": legacy.ORIGIN,
                "signed_permit": signed(),
                "launch_expires_at": legacy.EXPIRES,
                "tls_context": writer_tls_context(),
                "transport": httpx.MockTransport(handler),
            }
            | changes
        )
    )


async def send(document=None, *, was_complete=False, **response_options):
    async with transport(
        lambda _: legacy._reply(DOCUMENT if document is None else document, **response_options)
    ) as client:
        return await client.send(
            client.bind_request(legacy._build_request()),
            PINS,
            capture_bundle_id=9,
            stream_complete=was_complete,
        )


async def test_fixed_body_headers_original_token_and_complete_receipt():
    calls = []

    def receive(request):
        calls.append(request)
        assert str(request.url) == legacy.ORIGIN + wire.SOURCE_PATH
        assert request.content == legacy._canonical(asdict(PINS)) and len(request.content) <= 512
        verified = wire.verify_request(
            headers=[
                (name, request.headers[name])
                for name in (
                    "Content-Type",
                    "Authorization",
                    wire.CONTEXT_HEADER,
                    wire.KEY_ID_HEADER,
                    wire.SIGNATURE_HEADER,
                )
            ],
            body=request.content,
            method="POST",
            path=wire.SOURCE_PATH,
            query_string="",
            expected_origin=legacy.ORIGIN,
            trusted_now=legacy.NOW,
            keyring=legacy.KEYRING,
        )
        assert verified.pins == PINS and verified.token.value == b"original\x00\xff"
        return legacy._reply(DOCUMENT)

    async with transport(receive) as client:
        request = client.bind_request(legacy._build_request(lease_token=b"original\x00\xff"))
        receipt = await client.send(request, PINS, capture_bundle_id=9, stream_complete=False)
        assert asdict(receipt) == DOCUMENT and request.authorization_expires_at == legacy.EXPIRES
    assert len(calls) == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"execution_id": 5},
        {"build_id": 9},
        {"fence": 2},
        {"stream_slot": 1},
        {"capture_bundle_id": 10},
        {"capture_bundle_id": True},
        {"stream_complete": 1},
        {"rows_processed": True},
        {"rows_processed": 2},
        {"rows_processed": 100_001},
        {"rows_processed": -1},
        {"phase": "graph"},
        {"phase": []},
        {"phase": "admission"},
        {"extra": True},
        {"before": asdict(replace(BEFORE, next_pack_ordinal=6))},
        {"after": asdict(replace(AFTER, next_part_ordinal=1))},
        {"after": asdict(replace(AFTER, next_pack_ordinal=7))},
        {"after": asdict(replace(AFTER, next_pack_ordinal=2**31))},
        {"after": asdict(replace(AFTER, next_part_row_ordinal=10))},
        {"after": asdict(AFTER) | {"unknown": 0}},
    ],
)
async def test_receipt_rejects_every_wrong_identity_or_inconsistent_coordinate(changes):
    with pytest.raises(admission.AdmissionTransportError):
        await send(DOCUMENT | changes)


@pytest.mark.parametrize(
    "after,complete,phase,was_complete",
    [
        (wire.SourceCursor(3, 0, 40, 7), False, "source", False),
        (wire.SourceCursor(3, 0, 40, 7), True, "source", False),
        (BEFORE, True, "source", False),
        (BEFORE, True, "admission", True),
        (BEFORE, True, "admission", False),
    ],
)
async def test_zero_rows_require_a_new_verified_part_stream_or_global_transition(after, complete, phase, was_complete):
    receipt = await send(
        DOCUMENT
        | {
            "after": asdict(after),
            "rows_processed": 0,
            "stream_complete": complete,
            "phase": phase,
        },
        was_complete=was_complete,
    )
    assert receipt.after == after and receipt.phase == phase


@pytest.mark.parametrize(
    "after,complete,phase,was_complete",
    [
        (BEFORE, False, "source", False),
        (BEFORE, True, "source", True),
        (BEFORE, False, "admission", False),
        (replace(BEFORE, next_pack_ordinal=8), True, "source", False),
        (replace(BEFORE, next_source_ordinal=41), True, "source", False),
        (replace(BEFORE, next_part_row_ordinal=11), False, "source", False),
        (wire.SourceCursor(3, 1, 40, 7), False, "source", False),
    ],
)
async def test_zero_rows_cannot_acknowledge_a_noop_or_fabricate_progress(after, complete, phase, was_complete):
    with pytest.raises(admission.AdmissionTransportError):
        await send(
            DOCUMENT
            | {
                "after": asdict(after),
                "rows_processed": 0,
                "stream_complete": complete,
                "phase": phase,
            },
            was_complete=was_complete,
        )


async def test_positive_rows_cannot_write_an_already_complete_stream():
    with pytest.raises(admission.AdmissionTransportError):
        await send(was_complete=True)


@pytest.mark.parametrize(
    "raw",
    [
        b"x" * 1025,
        b"[]",
        b'{"fence":1,' + legacy._canonical(DOCUMENT)[1:],
        legacy._canonical(DOCUMENT | {"rows_processed": 3.0}),
        legacy._canonical(DOCUMENT | {"rows_processed": float("nan")}),
        legacy._canonical(DOCUMENT | {"rows_processed": 2**63}),
    ],
)
async def test_response_body_is_closed_bounded_and_exactly_typed(raw):
    with pytest.raises(admission.AdmissionTransportError):
        await send(raw=raw)


async def test_source_response_cap_is_1024_not_admission_512():
    raw = legacy._canonical(DOCUMENT)
    padded = b" " * (1024 - len(raw)) + raw
    assert (await send(raw=padded, headers=[("Content-Length", "1024")])).after == AFTER
    with pytest.raises(admission.AdmissionTransportError):
        await send(raw=padded + b" ")


@pytest.mark.parametrize(
    "headers",
    [
        [("Content-Length", "1025")],
        [("Content-Encoding", "gzip")],
        [("Set-Cookie", "untrusted=value")],
        [("Cache-Control", "public")],
        [("Content-Type", "application/json"), ("Content-Type", "application/json")],
    ],
)
async def test_source_uses_shared_no_store_no_cookies_no_decompression_boundary(headers):
    with pytest.raises(admission.AdmissionTransportError):
        await send(headers=headers)


def _pages(monkeypatch, *, after=AFTER, complete=False, phase="source", failure=None):
    state = SimpleNamespace(active=False, events=[], requests=[])

    @asynccontextmanager
    async def page(factory, request, build_id):
        assert not state.active and factory == "existing-pool" and build_id == 8
        index = len(state.requests)
        state.requests.append(request)
        if index and failure is not None:
            raise failure
        state.active = True
        state.events.append("page-enter")
        cursor = BEFORE if index == 0 else after

        class Session:
            async def scalars(self, statement):
                assert "FOR UPDATE" in str(statement)
                retained = SimpleNamespace(
                    **asdict(cursor), replay_verified_at=legacy.NOW if index and complete else None
                )
                return SimpleNamespace(one_or_none=lambda: retained)

        try:
            yield Session(), SimpleNamespace(phase="source" if index == 0 else phase, capture_bundle_id=9)
            state.events.append("commit")
        finally:
            state.active = False
            state.events.append("close")

    async def prepare(_session):
        assert state.active

    monkeypatch.setattr(worker, "_page_session", page)
    monkeypatch.setattr(worker, "_prepare_statement", prepare)
    return state


@pytest.mark.parametrize("outcome", ["lost-ack", 409, 503, 307])
@pytest.mark.parametrize("advanced", [True, False])
async def test_uncertain_post_observes_once_with_same_authority_and_never_resends(monkeypatch, outcome, advanced):
    state = _pages(monkeypatch, after=AFTER if advanced else BEFORE)
    calls = []

    def receive(request):
        assert not state.active and state.events == ["page-enter", "commit", "close"]
        calls.append(request)
        if outcome == "lost-ack":
            raise httpx.ReadError("do-not-log")
        return legacy._reply(status=outcome)

    async with transport(receive) as client:
        original = legacy._build_request()
        with pytest.raises(worker.SourceReconciliationRequired) as error:
            await worker.source_next_batch("existing-pool", original, 8, 2, client)
        assert error.value.pins == PINS
        assert (error.value.progress is not None) is advanced
        assert error.value.__context__ is None
    assert len(calls) == 1 and len(state.requests) == 2
    assert state.requests[0] is state.requests[1]
    assert state.requests[0].lease_token is original.lease_token
    assert state.requests[0].fence == original.fence and state.requests[0].authorization_expires_at == legacy.EXPIRES


@pytest.mark.parametrize(
    "error", [LeaseAuthorityLost("lost"), CancellationRequested("cancelled"), asyncio.CancelledError()]
)
async def test_reconciliation_propagates_authority_loss_and_cancellation(monkeypatch, error):
    _pages(monkeypatch, failure=error)
    async with transport(lambda _: legacy._reply(status=503)) as client:
        with pytest.raises(type(error)):
            await worker.source_next_batch("existing-pool", legacy._build_request(), 8, 2, client)


async def test_unknown_reconciliation_is_not_rollback_or_retry_permission(monkeypatch):
    _pages(monkeypatch, failure=RuntimeError("do-not-log"))
    async with transport(lambda _: legacy._reply(status=503)) as client:
        with pytest.raises(worker.SourceReconciliationRequired) as error:
            await worker.source_next_batch("existing-pool", legacy._build_request(), 8, 2, client)
        assert error.value.progress is None and "rollback" not in str(error.value)


@pytest.mark.parametrize(
    "after",
    [
        replace(BEFORE, next_pack_ordinal=8),
        replace(BEFORE, next_source_ordinal=41),
        replace(BEFORE, next_part_row_ordinal=11),
        replace(AFTER, next_pack_ordinal=6),
    ],
)
async def test_inconsistent_retained_coordinates_do_not_authorize_continuation(monkeypatch, after):
    _pages(monkeypatch, after=after)
    async with transport(lambda _: legacy._reply(status=503)) as client:
        with pytest.raises(worker.SourceReconciliationRequired) as error:
            await worker.source_next_batch("existing-pool", legacy._build_request(), 8, 2, client)
        assert error.value.progress is None


async def test_source_constructor_rejects_admission_permit_without_contact():
    with pytest.raises(admission.AdmissionTransportError):
        transport(lambda _: pytest.fail("no network"), signed_permit=legacy._signed())
