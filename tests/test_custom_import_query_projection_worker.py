# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Projection receipts, fixed-purpose transport and uncertain-delivery boundaries."""

import json
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from process.custom_import import admission_authorization as auth
from process.custom_import import admission_worker as admission
from process.custom_import import query_projection_authorization as wire
from process.custom_import import query_projection_worker as worker
from process.custom_import.query_projection_binding import QueryProjectionUnavailable
from process.custom_import.query_projection_build import ProjectionProgress
from process.custom_import.runner_types import CancellationRequested, LeaseAuthorityLost
from tests import test_custom_import_admission_worker as legacy
from tests.admission_transport_test_support import writer_tls_context
from tests.test_custom_import_query_projection_binding import _DEFINITION, _manifest, _parents, _result

BEFORE = ProjectionProgress("root", 2, -1, None, 0, 3, 0)
AFTER = replace(BEFORE, after_selected_value=0, root_count=4)
PINS = worker._pins(8, 4, 1, BEFORE)
DOCUMENT = dict(
    execution_id=4,
    build_id=8,
    fence=1,
    generation_id=20,
    capture_bundle_id=9,
    before=asdict(BEFORE),
    after=asdict(AFTER),
)


@pytest.fixture(autouse=True)
def _clock(monkeypatch):
    monkeypatch.setattr(admission, "_utcnow", lambda: legacy.NOW)


def signed():
    context = json.loads(auth._base64url_decode(legacy._signed().context, 2_048))
    context.update(contract=wire.PROJECTION_PERMIT_CONTRACT, path=wire.PROJECTION_PATH)
    return auth.sign_permit(
        legacy._canonical(context),
        expected_origin=legacy.ORIGIN,
        trusted_now=legacy.NOW,
        launch_expires_at=legacy.EXPIRES,
        keyring=legacy.KEYRING,
        is_projection=True,
    )


def transport(handler, **changes):
    return worker.QueryProjectionBatchTransport(
        **(
            dict(
                expected_origin=legacy.ORIGIN,
                signed_permit=signed(),
                launch_expires_at=legacy.EXPIRES,
                tls_context=writer_tls_context(),
                transport=httpx.MockTransport(handler),
            )
            | changes
        )
    )


async def send(document=None, **response_options):
    async with transport(
        lambda _: legacy._reply(DOCUMENT if document is None else document, **response_options)
    ) as client:
        return await client.send(
            client.bind_request(legacy._build_request()), PINS, generation_id=20, capture_bundle_id=9, before=BEFORE
        )


async def test_fixed_purpose_body_and_original_token():
    calls = []

    def receive(request):
        calls.append(request)
        assert str(request.url) == legacy.ORIGIN + wire.PROJECTION_PATH
        assert request.content == legacy._canonical(asdict(PINS)) and len(request.content) <= 512
        verified = wire.verify_request(
            headers=[
                (name, request.headers[name])
                for name in (
                    "Content-Type",
                    "Authorization",
                    "X-Custom-Import-Projection-Context",
                    "X-Custom-Import-Projection-Key-Id",
                    "X-Custom-Import-Projection-Signature",
                )
            ],
            body=request.content,
            method="POST",
            path=wire.PROJECTION_PATH,
            query_string="",
            expected_origin=legacy.ORIGIN,
            trusted_now=legacy.NOW,
            keyring=legacy.KEYRING,
        )
        assert verified.pins == PINS and verified.token.value == b"original\x00\xff"
        return legacy._reply(DOCUMENT)

    async with transport(receive) as client:
        receipt = await client.send(
            client.bind_request(legacy._build_request(lease_token=b"original\x00\xff")),
            PINS,
            generation_id=20,
            capture_bundle_id=9,
            before=BEFORE,
        )
    assert asdict(receipt) == DOCUMENT and len(calls) == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"execution_id": 5},
        {"build_id": 9},
        {"fence": 2},
        {"generation_id": 21},
        {"capture_bundle_id": 10},
        {"generation_id": True},
        {"extra": 0},
        {"before": asdict(replace(BEFORE, root_count=2))},
        {"after": asdict(BEFORE)},
        {"after": asdict(replace(AFTER, after_selected_value=-2))},
        {"after": asdict(replace(AFTER, root_count=True))},
        {"after": asdict(replace(AFTER, child_count=1))},
        {"after": asdict(AFTER) | {"unknown": 0}},
        {"after": asdict(replace(AFTER, after_selected_value=2**63))},
        {"after": asdict(replace(AFTER, phase="indexes", after_entity_binding_id=0, after_selected_value=None))},
    ],
)
async def test_receipt_rejects_foreign_or_inconsistent_progress(changes):
    with pytest.raises(admission.AdmissionTransportError):
        await send(DOCUMENT | changes)


@pytest.mark.parametrize(
    "raw",
    [
        b"x" * 1025,
        b"[]",
        b'{"fence":1,' + legacy._canonical(DOCUMENT)[1:],
        legacy._canonical(DOCUMENT | {"fence": 1.0}),
        legacy._canonical(DOCUMENT | {"fence": float("nan")}),
    ],
)
async def test_wire_response_is_closed_and_bounded(raw):
    with pytest.raises(admission.AdmissionTransportError):
        await send(raw=raw)


async def test_projection_response_uses_1024_byte_cap():
    raw = legacy._canonical(DOCUMENT)
    padded = b" " * (1024 - len(raw)) + raw
    assert (await send(raw=padded, headers=[("Content-Length", "1024")])).after == AFTER
    with pytest.raises(admission.AdmissionTransportError):
        await send(raw=padded + b" ")


@pytest.mark.parametrize(
    "before,after,forward",
    [
        (BEFORE, AFTER, True),
        (AFTER, BEFORE, False),
        (replace(BEFORE, after_selected_value=-(2**63)), replace(AFTER, after_selected_value=2**63 - 1), True),
        (BEFORE, ProjectionProgress("child", 0, None, None, 0, 3, 0), True),
        (
            ProjectionProgress("indexes", 0, None, None, 1, 3, 4),
            ProjectionProgress("indexes", 0, None, None, 2, 3, 4),
            True,
        ),
        (
            ProjectionProgress("complete", 0, None, None, 2, 3, 4),
            ProjectionProgress("complete", 0, None, None, 2, 3, 4),
            False,
        ),
    ],
)
def test_forward_progress_preserves_signed_selection_and_index_counts(before, after, forward):
    assert worker.has_forward_progress(before, after) is forward


def retained(progress=BEFORE, **changes):
    return worker.RetainedProjectionProgress(
        **(
            dict(execution_id=4, build_id=8, fence=1, generation_id=20, capture_bundle_id=9, progress=progress)
            | changes
        )
    )


async def test_no_transaction_crosses_http_and_uncertain_delivery_is_not_retried(monkeypatch):
    events = []

    async def uncertain(*args, **kwargs):
        assert events == ["read-open", "read-closed"]
        raise admission.AdmissionTransportError("do-not-log")

    observations = iter((retained(), retained(AFTER)))

    @asynccontextmanager
    async def reads(_factory):
        events.append("read-open")
        try:
            yield SimpleNamespace(execute=AsyncMock(), run_sync=AsyncMock(return_value=next(observations)))
        finally:
            events.append("read-closed")

    client = SimpleNamespace(bind_request=lambda request: request, send=AsyncMock(side_effect=uncertain))
    monkeypatch.setattr(worker, "_read_session", reads)
    with pytest.raises(worker.ProjectionReconciliationRequired) as failure:
        await worker.projection_next_batch(None, legacy._build_request(), 8, client)
    assert failure.value.pins == PINS and failure.value.progress == retained(AFTER)
    assert events == ["read-open", "read-closed"] * 2
    client.send.assert_awaited_once()


@pytest.mark.parametrize("observation", [retained(), retained(AFTER, generation_id=21), OSError("private")])
async def test_uncertain_delivery_accepts_only_same_attempt_forward_observation(monkeypatch, observation):
    client = SimpleNamespace(
        bind_request=lambda request: request, send=AsyncMock(side_effect=admission.AdmissionTransportError("private"))
    )
    monkeypatch.setattr(worker, "_retained_progress", AsyncMock(side_effect=[retained(), observation]))
    with pytest.raises(worker.ProjectionReconciliationRequired) as failure:
        await worker.projection_next_batch(None, legacy._build_request(), 8, client)
    assert failure.value.progress is None and failure.value.pins == PINS
    client.send.assert_awaited_once()


@pytest.mark.parametrize("error", [CancellationRequested("private"), LeaseAuthorityLost("private")])
async def test_reconciliation_cannot_hide_lost_authority(monkeypatch, error):
    client = SimpleNamespace(
        bind_request=lambda request: request, send=AsyncMock(side_effect=admission.AdmissionTransportError("private"))
    )
    monkeypatch.setattr(worker, "_retained_progress", AsyncMock(side_effect=[retained(), error]))
    with pytest.raises(type(error)):
        await worker.projection_next_batch(None, legacy._build_request(), 8, client)


async def test_completed_observation_does_not_post(monkeypatch):
    observation = retained(ProjectionProgress("complete", 0, None, None, 2, 3, 4))
    client = SimpleNamespace(bind_request=lambda request: request, send=AsyncMock())
    monkeypatch.setattr(worker, "_retained_progress", AsyncMock(return_value=observation))
    receipt = await worker.projection_next_batch(None, legacy._build_request(), 8, client)
    assert receipt.before == receipt.after == observation.progress
    assert (receipt.generation_id, receipt.capture_bundle_id) == (20, 9)
    client.send.assert_not_awaited()


def _observation_case(monkeypatch, *, absent=False):
    parent, _generation, _definition = _parents(frozen_at=None)
    build = SimpleNamespace(**(vars(parent) | {"phase": "output"}))
    manifest = (
        None
        if absent
        else _manifest(
            phase="root",
            complete_at=None,
            projection_sha256=None,
            after_entity_binding_id=2,
            after_selected_value=-1,
            after_child_key_sha256=None,
            index_count=0,
            root_count=3,
            child_count=0,
        )
    )
    request = SimpleNamespace(execution_id=3, fence=2, definition=_DEFINITION)
    session = SimpleNamespace(execute=lambda _: _result((parent, manifest)))
    monkeypatch.setattr(worker, "_read_authority", lambda *_: (build, 60))
    return session, request, build, parent, manifest


@pytest.mark.parametrize("absent", [True, False])
def test_observation_selects_only_retained_attempt_control_rows(monkeypatch, absent):
    session, request, _build, _parent, _manifest_row = _observation_case(monkeypatch, absent=absent)
    statements = []
    execute = session.execute
    session.execute = lambda statement: (statements.append(str(statement)), execute(statement))[1]
    observed = worker._observe(session, request, 8)
    assert (observed.generation_id, observed.capture_bundle_id) == (13, 8)
    assert observed.progress.phase == "root" and observed.progress.after_entity_binding_id == (0 if absent else 2)
    assert len(statements) == 1 and "LEFT OUTER JOIN" in statements[0]
    assert all(word not in statements[0] for word in ("INSERT", "UPDATE", "CREATE", "FUNCTION", "FOR UPDATE"))


@pytest.mark.parametrize(
    "fault", ["missing", "phase", "foreign-capture", "foreign-token", "foreign-definition", "proof"]
)
def test_observation_refuses_foreign_or_incomplete_retained_authority(monkeypatch, fault):
    session, request, build, parent, manifest = _observation_case(monkeypatch, absent=fault == "missing")
    if fault in {"missing", "phase"}:
        build.phase = "verifying" if fault == "missing" else "graph"
    if fault == "foreign-capture":
        parent.capture_bundle_id += 1
    if fault == "foreign-token":
        manifest.producing_token_sha256 = b"x" * 32
    if fault == "foreign-definition":
        manifest.definition_sha256 = b"x" * 32
    if fault == "proof":
        manifest.verified_root_count = 1
    expected_error = (
        QueryProjectionUnavailable if fault in {"foreign-definition", "proof"} else admission.AdmissionTransportError
    )
    with pytest.raises(expected_error):
        worker._observe(session, request, 8)


@pytest.mark.parametrize("phase", ["digest_root", "digest_child"])
def test_digest_progress_uses_proof_count_when_physical_ids_regress(phase):
    before = ProjectionProgress(phase, 99, 2030, None, 3, 200, 400, 10)
    after = replace(before, after_entity_binding_id=2, proof_count=11)
    assert worker.has_forward_progress(before, after)
    assert not worker.has_forward_progress(before, replace(after, proof_count=10))


@pytest.mark.parametrize("phase", ["verify_root", "verify_child"])
@pytest.mark.parametrize("counter", ["root_count", "child_count", "index_count"])
def test_verification_receipt_preserves_both_scope_totals(phase, counter):
    before = ProjectionProgress(phase, 2, 2030, None, 3, 20, 30, 10)
    after = replace(before, after_entity_binding_id=3, proof_count=11)
    pins = worker._pins(8, 4, 1, before)
    document = DOCUMENT | {"before": asdict(before), "after": asdict(after)}
    assert worker.validate_receipt(document, pins, generation_id=20, capture_bundle_id=9).after == after
    after = replace(after, **{counter: getattr(after, counter) + 1})
    with pytest.raises(admission.AdmissionTransportError):
        worker.validate_receipt(document | {"after": asdict(after)}, pins, generation_id=20, capture_bundle_id=9)


@pytest.mark.parametrize("phase", ["verify_root", "verify_child"])
def test_verification_empty_entity_window_can_advance_without_proof_rows(phase):
    before = ProjectionProgress(phase, 2, 2030, None, 3, 20, 30, 10)
    assert worker.has_forward_progress(before, replace(before, after_entity_binding_id=3))
    assert not worker.has_forward_progress(before, replace(before, proof_count=11))


def test_complete_wire_sizes_fit_existing_bounds():
    progress = ProjectionProgress("digest_child", 2**63 - 1, -(2**63), "f" * 64, 32, 2**63 - 1, 2**63 - 1, 2**63 - 1)
    pins = worker._pins(2**63 - 1, 2**63 - 1, 2**63 - 1, progress)
    assert len(legacy._canonical(asdict(pins))) <= 512
    document = DOCUMENT | {name: 2**63 - 1 for name in worker._IDENTITY_NAMES}
    document.update(before=asdict(progress), after=asdict(progress))
    assert len(legacy._canonical(document)) <= 1024
