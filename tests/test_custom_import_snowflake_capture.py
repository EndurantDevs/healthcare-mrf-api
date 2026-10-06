# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Capture-only coordination over a synthetic native cursor and fake store."""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import json
import struct
import threading
import weakref
from dataclasses import asdict, replace
from decimal import Decimal
from types import SimpleNamespace

import pytest

from process.custom_import import snowflake_capture as capture
from process.custom_import.capture import CaptureError
from process.custom_import.capture_limits import CaptureLimits
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.execution import ExecutionSubmission, LeaseGrant
from process.custom_import.processing_policy import BuildPolicy, ProcessingPolicy
from process.custom_import.segmented_capture_policy import SegmentedCapturePolicy
from process.custom_import.snowflake import SnowflakeConnectorError
from process.custom_import.snowflake_candidate import SnowflakeBundleCandidateRequest
from process.custom_import.snowflake_python import SnowflakeLandingEOF, SnowflakeLandingPart
from tests.test_custom_import_snowflake_bundle import _Credentials
from tests.test_custom_import_snowflake_shared_capture import _runtime, _shared_row


def _policy(**changes):
    document_by_key = dict(
        part_limits=CaptureLimits(
            maximum_compressed_bytes=1024 * 1024,
            maximum_decoded_bytes=1024 * 1024,
            maximum_record_bytes=4096,
            maximum_records=4,
            maximum_fields_per_record=32,
            read_chunk_bytes=4096,
        ),
        stream_budget=dict(
            maximum_parts=2000,
            maximum_compressed_bytes=8 * 1024 * 1024,
            maximum_decoded_bytes=8 * 1024 * 1024,
            maximum_arrow_bytes=8 * 1024 * 1024,
            maximum_records=4000,
            maximum_manifest_bytes=4 * 1024 * 1024,
        ),
        maximum_part_arrow_bytes=1024 * 1024,
        maximum_part_manifest_bytes=4096,
        maximum_dataset_retained_bytes=24 * 1024 * 1024,
        acquisition_deadline_seconds=60,
    )
    document_by_key["bundle_budget"] = {key: value * 2 for key, value in document_by_key["stream_budget"].items()}
    return SegmentedCapturePolicy(**(document_by_key | changes))


class _Session:
    def __init__(self, harness):
        self.harness = harness
        self.active = False

    async def __aenter__(self):
        self.active = True
        self.harness.timeline.append("transaction")
        return self

    async def __aexit__(self, kind, value, traceback):
        self.harness.timeline.append("commit" if kind is None else "rollback")
        self.active = False

    def begin(self):
        return self

    async def get(self, model, identifier):
        return self.harness.bound


class _Harness:
    def __init__(self, monkeypatch, rows=(), *, capture_policy=None, **options):
        self.policy = capture_policy or _policy()
        processing_policy = ProcessingPolicy(self.policy, 17, BuildPolicy(2, 4096, 1000, 60, 300))
        self.builder, bundle, self.adapter, self.cursor, self.connection = _runtime(
            monkeypatch, rows, processing_policy=processing_policy, **options
        )
        self.request = SnowflakeBundleCandidateRequest(
            1, 2, 3, bundle.definition, bundle, "synthetic-capture", b"owner"
        )
        self.grant = LeaseGrant(9, 1, dt.datetime.now(dt.UTC) + dt.timedelta(seconds=300), "running")
        self.submission = ExecutionSubmission(9, "queued", True)
        self.timeline = []
        self.parts = []
        self.receipts = ()
        self.pending = None
        self.bound = None
        self.threads = []
        self.started = dt.datetime.now(dt.UTC)
        self.finishes = []
        self.credentials = _Credentials()
        self._install(monkeypatch)

    def session(self):
        return _Session(self)

    def _install(self, monkeypatch):
        monkeypatch.setattr(self.adapter, "_connect", lambda _credentials, **_options: (self.connection, self.cursor))
        original = self.credentials.load_key_pair

        def credentials():
            self.timeline.append("credentials")
            self.threads.append(threading.get_ident())
            return original()

        monkeypatch.setattr(self.credentials, "load_key_pair", credentials)
        monkeypatch.setattr(capture, "_reserve_bundle_execution", self.reserve)
        monkeypatch.setattr(capture, "_renew_bundle_lease", self.renew)
        monkeypatch.setattr(capture.lifecycle, "_lock_current_capture_binding", self.lock)
        monkeypatch.setattr(capture.lifecycle, "_database_now", self.now)
        monkeypatch.setattr(capture.lifecycle, "finish_execution", self.finish)
        monkeypatch.setattr(capture, "begin_pending_parquet_bundle", self.begin)
        monkeypatch.setattr(capture, "append_pending_parquet_part", self.append)
        monkeypatch.setattr(capture, "mark_pending_parquet_eof", self.eof)
        monkeypatch.setattr(capture, "seal_pending_parquet_bundle", self.seal)

    async def reserve(self, sessions, request, statement, identity, *, processing_policy=None):
        assert request == self.request
        self.identity = identity
        self.timeline.append("reserve")
        return self.submission, self.grant

    async def renew(self, sessions, request, grant):
        self.timeline.append("heartbeat")
        return self.grant

    async def lock(self, session, **authority):
        assert session.active and authority["fence"] == self.grant.fence
        self.timeline.append("authority")
        return SimpleNamespace(started_at=self.started), None, None

    async def now(self, session):
        return dt.datetime.now(dt.UTC)

    async def finish(self, session, **authority):
        assert session.active
        self.timeline.append("finish")
        self.finishes.append(authority)
        return SimpleNamespace(state=authority["terminal_state"], changed=True)

    async def begin(self, session, *, request):
        assert session.active
        self.timeline.append("begin")
        self.pending = request
        return SimpleNamespace(capture_bundle_id=23)

    async def append(self, session, **part):
        assert session.active
        self.timeline.append("append")
        self.parts.append(part)
        return 1

    async def eof(self, session, **evidence):
        assert session.active and self.cursor.closed and self.connection.closed
        self.timeline.append("eof")
        return 1

    async def seal(self, session, **receipt):
        assert session.active and self.cursor.closed and self.connection.closed
        self.timeline.append("seal")
        self.receipts = receipt["receipts"]

    async def run(self, **changes):
        return await capture.acquire_segmented_snowflake_capture(
            self.session,
            self.request,
            statement_builder=self.builder,
            adapter=self.adapter,
            credential_provider=self.credentials,
            policy=changes.pop("policy", self.policy),
            driver_timeout_seconds=changes.pop("driver_timeout_seconds", 17),
            processing_policy=self.request.bundle_request.processing_policy,
            **changes,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("rows", [(), (_shared_row(), _shared_row(key="b"))])
async def test_capture_seals_only_source_with_compact_framed_receipts(monkeypatch, rows):
    harness = _Harness(monkeypatch, rows)
    result = await harness.run()
    assert result == capture.SnowflakeCaptureResult("capture_sealed", 9, 23, 1)
    assert harness.timeline.index("authority") < harness.timeline.index("credentials")
    assert harness.timeline.index("reserve") < harness.timeline.index("credentials")
    assert harness.pending.request_identity_sha256 == harness.identity
    assert harness.pending.source_request_sha256 == bytes.fromhex(harness.request.bundle_request.request_sha256)
    assert len(harness.cursor.executed) == 1
    assert len(harness.receipts) == 2
    for receipt in harness.receipts:
        document = json.loads(receipt.canonical_manifest)
        assert set(document["source"]) == {
            "contract",
            "query_id",
            "encoding",
            "relation",
            "selected_field_ids",
            "semantic_token_metadata_key",
            "result_schema",
            "schema_fingerprint",
            "request_sha256",
            "statement_sha256",
        }
        assert document["source"]["contract"] == "custom-import/snowflake-bundle/v2"
        assert document["record_count"] == len(rows)
        assert "captures" not in document and "parts" not in document
        _assert_framing(receipt, harness.parts)


@pytest.mark.asyncio
async def test_capture_rejects_decimal_scale_expansion_before_retaining_a_part(monkeypatch):
    """Encoded Decimal scale must still satisfy the decoded record byte limit."""

    policy = _policy(part_limits=replace(_policy().part_limits, maximum_record_bytes=40))
    harness = _Harness(monkeypatch, (_shared_row("a", key="a", amount=Decimal("1")),), capture_policy=policy)
    with pytest.raises(SnowflakeConnectorError, match="landing fetch failed") as caught:
        await harness.run()
    assert isinstance(caught.value.__cause__, CaptureError)
    assert "exceeds the byte limit" in str(caught.value.__cause__)
    assert not harness.parts and not harness.receipts
    assert harness.cursor.closed and harness.connection.closed
    assert harness.finishes[0]["terminal_state"] == "failed"


def _assert_framing(receipt, parts):
    payload = hashlib.sha256(capture._PARQUET_PART_SET_DOMAIN)
    manifest = hashlib.sha256(capture._MANIFEST_SET_DOMAIN)
    for part in parts:
        sealed = part["capture"]
        if sealed.manifest.stream_id != receipt.stream_id:
            continue
        canonical = capture.canonical_json(asdict(sealed.manifest)).encode()
        payload.update(
            struct.pack(">iq", part["ordinal"], len(sealed.payload)) + hashlib.sha256(sealed.payload).digest()
        )
        manifest.update(struct.pack(">iq", part["ordinal"], len(canonical)) + hashlib.sha256(canonical).digest())
        manifest.update(
            struct.pack(">qqq", sealed.manifest.decoded_bytes, part["arrow_byte_count"], part["record_count"])
        )
    document = json.loads(receipt.canonical_manifest)
    assert document["payload_set_sha256"] == payload.hexdigest() == receipt.content_sha256
    assert document["manifest_set_sha256"] == manifest.hexdigest()
    assert hashlib.sha256(receipt.canonical_manifest.encode()).hexdigest() == receipt.manifest_sha256


@pytest.mark.asyncio
@pytest.mark.parametrize("scopes", [("root",), ("child",), ("root", "child")])
async def test_capture_uses_field_slots_when_declarations_are_reordered(monkeypatch, scopes):
    """Capture accepts slot-ordered projections independent of declaration order."""

    harness = _Harness(monkeypatch, (_shared_row(),))
    bundle = harness.request.bundle_request
    document = json.loads(bundle.definition.canonical)
    if "root" in scopes:
        document["schema"]["root"]["fields"].reverse()
    if "child" in scopes:
        document["schema"]["children"][0]["fields"].reverse()
    definition = CustomImportDefinition.from_mapping(document)
    reordered_bundle = replace(bundle, definition=definition)
    assert reordered_bundle.bindings == bundle.bindings
    harness.request = replace(harness.request, definition=definition, bundle_request=reordered_bundle)

    result = await harness.run()

    assert result == capture.SnowflakeCaptureResult("capture_sealed", 9, 23, 1)
    assert len(harness.parts) == len(harness.receipts) == 2
    assert all(part["record_count"] == 1 for part in harness.parts)
    assert harness.cursor.closed and harness.connection.closed
    assert harness.finishes == []


@pytest.mark.asyncio
async def test_one_affinity_thread_and_commit_before_next_source_event(monkeypatch):
    harness = _Harness(monkeypatch, (_shared_row(), _shared_row(key="b")))
    next_event = capture._AffinityLanding.next_event
    close = capture._AffinityLanding.close

    def advance(owner):
        harness.threads.append(threading.get_ident())
        if harness.parts:
            assert harness.timeline[-1] in {"commit", "heartbeat"}
        return next_event(owner)

    def cleanup(owner):
        harness.threads.append(threading.get_ident())
        return close(owner)

    monkeypatch.setattr(capture._AffinityLanding, "next_event", advance)
    monkeypatch.setattr(capture._AffinityLanding, "close", cleanup)
    await harness.run()
    assert len(set(harness.threads)) == 1
    assert harness.threads[0] != threading.get_ident()
    assert harness.timeline.count("append") == 4
    assert harness.timeline.index("eof") < harness.timeline.index("seal")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "option,value",
    [
        ("policy", None),
        ("driver_timeout_seconds", 0),
        ("driver_timeout_seconds", True),
        ("driver_timeout_seconds", 121),
    ],
)
async def test_invalid_options_never_reserve_or_load_credentials(monkeypatch, option, value):
    harness = _Harness(monkeypatch)
    with pytest.raises(ValueError):
        await harness.run(**{option: value})
    assert harness.timeline == []
    assert harness.finishes == []


def test_segmented_request_identity_pins_policy_and_does_not_change_v1(monkeypatch):
    harness = _Harness(monkeypatch)
    statement = harness.builder.build_statement(replace(harness.request.bundle_request, processing_policy=None))
    old = capture.bundle_request_identity_sha256(statement.request, statement)
    new = capture.segmented_bundle_request_identity_sha256(statement.request, statement, harness.policy)
    assert new == hashlib.sha256(capture._REQUEST_DOMAIN + old + bytes.fromhex(harness.policy.digest)).digest()
    changed = replace(harness.policy, acquisition_deadline_seconds=61)
    assert capture.segmented_bundle_request_identity_sha256(statement.request, statement, changed) != new
    assert capture.bundle_request_identity_sha256(statement.request, statement) == old


@pytest.mark.asyncio
async def test_deadline_includes_time_before_open_and_cannot_reset(monkeypatch):
    harness = _Harness(monkeypatch, capture_policy=replace(_policy(), acquisition_deadline_seconds=1))
    harness.started -= dt.timedelta(seconds=2)
    with pytest.raises(capture.SnowflakeCaptureError, match="deadline expired"):
        await harness.run()
    assert "credentials" not in harness.timeline and "begin" not in harness.timeline
    assert harness.finishes == [
        {
            "execution_id": 9,
            "fence": 1,
            "token": b"owner",
            "terminal_state": "failed",
            "terminal_reason": "source_capture_failed",
        }
    ]


@pytest.mark.asyncio
async def test_future_execution_start_rejected_before_credentials_or_source(monkeypatch):
    harness = _Harness(monkeypatch)
    harness.started += dt.timedelta(seconds=120)
    monkeypatch.setattr(capture._AffinityLanding, "open", lambda _: pytest.fail("source opener must not run"))
    with pytest.raises(capture.SnowflakeCaptureError, match="start is in the future"):
        await harness.run()
    assert "credentials" not in harness.timeline and "begin" not in harness.timeline
    assert harness.cursor.executed == []


@pytest.mark.asyncio
async def test_reservation_binding_rejection_precedes_credentials(monkeypatch):
    harness = _Harness(monkeypatch)

    async def reject(*arguments, **options):
        raise capture.SnowflakeCandidateError("synthetic binding mismatch")

    monkeypatch.setattr(capture, "_reserve_bundle_execution", reject)
    with pytest.raises(capture.SnowflakeCandidateError):
        await harness.run()
    assert "credentials" not in harness.timeline
    assert harness.finishes == []


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["failed", "canceling", "running"])
async def test_claimed_failure_finishes_exact_fence_and_preserves_cancellation(monkeypatch, state):
    harness = _Harness(monkeypatch)
    primary = capture.SnowflakeCaptureError("synthetic capture failure")

    async def fail(*arguments):
        raise primary

    async def finish(session, **authority):
        await harness.finish(session, **authority)
        return SimpleNamespace(state=state if len(harness.finishes) == 1 else "canceled", changed=False)

    monkeypatch.setattr(capture, "_capture_reserved", fail)
    monkeypatch.setattr(capture.lifecycle, "finish_execution", finish)
    with pytest.raises(capture.SnowflakeCaptureError) as caught:
        await harness.run()
    assert caught.value is primary
    assert harness.finishes[0] == {
        "execution_id": 9,
        "fence": 1,
        "token": b"owner",
        "terminal_state": "failed",
        "terminal_reason": "source_capture_failed",
    }
    assert len(harness.finishes) == (2 if state == "canceling" else 1)
    if state == "canceling":
        assert harness.finishes[1] == {
            "execution_id": 9,
            "fence": 1,
            "token": b"owner",
            "terminal_state": "canceled",
        }


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", [capture.SnowflakeCaptureCleanupUncertain, asyncio.CancelledError])
@pytest.mark.parametrize("finish_error", [RuntimeError, asyncio.CancelledError])
async def test_finalization_failure_never_masks_primary_cleanup_or_cancellation(monkeypatch, error_type, finish_error):
    harness = _Harness(monkeypatch)
    primary = error_type("synthetic primary failure")
    cause = RuntimeError("synthetic cleanup cause")
    primary.__cause__ = cause
    primary.add_note("retained cleanup evidence")

    async def fail(*arguments):
        raise primary

    async def finish(session, **authority):
        await harness.finish(session, **authority)
        raise finish_error("synthetic finalization failure")

    monkeypatch.setattr(capture, "_capture_reserved", fail)
    monkeypatch.setattr(capture.lifecycle, "finish_execution", finish)
    with pytest.raises(error_type) as caught:
        await harness.run()
    assert caught.value is primary and primary.__cause__ is cause
    assert primary.__notes__ == [
        "retained cleanup evidence",
        "Capture execution finalization is unconfirmed; worker supervision is required.",
    ]
    assert harness.finishes[0]["terminal_state"] == ("canceled" if error_type is asyncio.CancelledError else "failed")


@pytest.mark.asyncio
async def test_bound_capture_returns_without_source_or_credentials(monkeypatch):
    harness = _Harness(monkeypatch)
    statement = harness.builder.build_statement(harness.request.bundle_request)
    identity = capture.segmented_bundle_request_identity_sha256(statement.request, statement, harness.policy)
    harness.submission = replace(harness.submission, capture_bundle_id=23)
    harness.bound = SimpleNamespace(
        dataset_id=1,
        definition_revision_id=2,
        schema_revision_id=3,
        capture_state="sealed",
        payload_contract=capture.SEGMENTED_PAYLOAD_CONTRACT,
        canonical_policy=harness.policy.canonical,
        source_binding_revision_id=None,
        producing_execution_id=9,
        source_binding_sha256=None,
        request_identity_sha256=identity,
        policy_sha256=bytes.fromhex(harness.policy.digest),
        source_request_sha256=bytes.fromhex(statement.request.request_sha256),
        statement_sha256=bytes.fromhex(statement.statement_sha256),
    )
    assert await harness.run() == capture.SnowflakeCaptureResult("capture_bound", 9, 23, 1)
    harness.bound.producing_execution_id = 10
    with pytest.raises(capture.SnowflakeCaptureError, match="bound capture identity"):
        await harness.run()
    harness.bound.producing_execution_id = 9
    harness.bound.capture_state = "pending"
    with pytest.raises(capture.SnowflakeCaptureError, match="bound capture identity"):
        await harness.run()
    assert "credentials" not in harness.timeline and harness.cursor.executed == []
    assert len(harness.finishes) == 2
    assert all(finish["terminal_state"] == "failed" for finish in harness.finishes)


@pytest.mark.asyncio
async def test_heartbeat_loss_during_blocking_open_discards_late_result(monkeypatch):
    harness = _Harness(monkeypatch, (_shared_row(),))
    loop = asyncio.get_running_loop()
    entered = asyncio.Event()
    release = threading.Event()
    opener = harness.adapter.open_bundle_landing

    def blocking_open(*arguments, **options):
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5)
        return opener(*arguments, **options)

    async def lost(*arguments):
        await entered.wait()
        return None

    monkeypatch.setattr(harness.adapter, "open_bundle_landing", blocking_open)
    monkeypatch.setattr(capture, "_renew_bundle_lease", lost)
    task = asyncio.create_task(harness.run())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        await asyncio.sleep(0.02)
        assert not task.done() and harness.pending is None
    finally:
        release.set()
    with pytest.raises(capture.SnowflakeCaptureError, match="authority was lost"):
        await task
    assert harness.cursor.closed and harness.connection.closed
    assert harness.parts == [] and "seal" not in harness.timeline


@pytest.mark.asyncio
async def test_cancel_retains_late_future_and_waits_for_affinity_close(monkeypatch):
    harness = _Harness(monkeypatch, (_shared_row(),))
    loop = asyncio.get_running_loop()
    entered = asyncio.Event()
    closing = asyncio.Event()
    fetch_release = threading.Event()
    close_release = threading.Event()
    advance, close = capture._AffinityLanding.next_event, capture._AffinityLanding.close

    def blocked_next(owner):
        event = advance(owner)
        loop.call_soon_threadsafe(entered.set)
        assert fetch_release.wait(5)
        return event

    def blocked_close(owner):
        loop.call_soon_threadsafe(closing.set)
        assert close_release.wait(5)
        return close(owner)

    monkeypatch.setattr(capture._AffinityLanding, "next_event", blocked_next)
    monkeypatch.setattr(capture._AffinityLanding, "close", blocked_close)
    task = asyncio.create_task(harness.run())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        fetch_release.set()
        await asyncio.wait_for(closing.wait(), 2)
        task.cancel()  # Repeated cancellation must not abandon cleanup either.
        await asyncio.sleep(0)
        assert not task.done() and harness.parts == []
    finally:
        fetch_release.set()
        close_release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert harness.cursor.closed and harness.connection.closed
    assert "eof" not in harness.timeline and "seal" not in harness.timeline
    assert harness.finishes[0]["terminal_state"] == "canceled"


@pytest.mark.asyncio
async def test_repeated_cancel_preserves_cancellation_and_retains_failed_close(monkeypatch):
    harness = _Harness(monkeypatch, (_shared_row(),))
    loop = asyncio.get_running_loop()
    entered, closing = asyncio.Event(), asyncio.Event()
    fetch_release, close_release = threading.Event(), threading.Event()
    advance, close = capture._AffinityLanding.next_event, capture._AffinityLanding.close
    evidence = SimpleNamespace(owner=None)
    failure = RuntimeError("synthetic close failure")

    def fail_close():
        raise failure

    def blocked_next(owner):
        event = advance(owner)
        loop.call_soon_threadsafe(entered.set)
        assert fetch_release.wait(5)
        return event

    def blocked_close(owner):
        evidence.owner = owner
        loop.call_soon_threadsafe(closing.set)
        assert close_release.wait(5)
        return close(owner)

    harness.cursor.close = fail_close
    monkeypatch.setattr(capture._AffinityLanding, "next_event", blocked_next)
    monkeypatch.setattr(capture._AffinityLanding, "close", blocked_close)
    task = asyncio.create_task(harness.run())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel("primary cancellation")
        await asyncio.sleep(0)
        fetch_release.set()
        await asyncio.wait_for(closing.wait(), 2)
        task.cancel("repeated cancellation")
        await asyncio.sleep(0)
        assert not task.done() and harness.parts == []
    finally:
        fetch_release.set()
        close_release.set()
    with pytest.raises(asyncio.CancelledError) as caught:
        await task
    assert caught.value.args == ("primary cancellation",)
    assert caught.value.__cause__ is evidence.owner.close_error
    assert caught.value.__cause__.__cause__.__cause__ is failure
    assert any("cleanup is unconfirmed" in note for note in caught.value.__notes__)
    assert harness.connection.closed and "eof" not in harness.timeline and "seal" not in harness.timeline


def _block_cleanup_through_heartbeat(harness, monkeypatch, cancel_fetch):
    loop = asyncio.get_running_loop()
    advance, close, await_owned = (
        capture._AffinityLanding.next_event,
        capture._AffinityLanding.close,
        capture._await_owned,
    )
    evidence = SimpleNamespace(
        owner=None,
        entered=asyncio.Event(),
        closing=asyncio.Event(),
        renewing=asyncio.Event(),
        draining=asyncio.Event(),
        fetch_release=threading.Event(),
        close_release=threading.Event(),
        renew_release=asyncio.Event(),
    )
    failure = RuntimeError("synthetic close failure")

    def fail_close():
        raise failure

    def blocked_next(owner):
        event = advance(owner)
        loop.call_soon_threadsafe(evidence.entered.set)
        assert evidence.fetch_release.wait(5)
        return event

    def blocked_close(owner):
        evidence.owner = owner
        loop.call_soon_threadsafe(evidence.closing.set)
        assert evidence.close_release.wait(5)
        return close(owner)

    async def blocked_renew(*arguments):
        evidence.renewing.set()
        await evidence.renew_release.wait()
        return harness.grant

    async def observe_drain(future, attempt):
        if isinstance(future, asyncio.Task):
            evidence.draining.set()
        return await await_owned(future, attempt)

    async def successful_body(*arguments):
        await evidence.renewing.wait()
        return capture.SnowflakeCaptureResult("capture_sealed", 9, 23, 1)

    harness.cursor.close = fail_close
    monkeypatch.setattr(capture._AffinityLanding, "next_event", blocked_next)
    monkeypatch.setattr(capture._AffinityLanding, "close", blocked_close)
    monkeypatch.setattr(capture, "_renew_bundle_lease", blocked_renew)
    monkeypatch.setattr(capture, "_await_owned", observe_drain)
    if not cancel_fetch:
        monkeypatch.setattr(capture, "_land", successful_body)
    return evidence


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_fetch", [True, False])
async def test_cleanup_error_survives_cancellation_while_draining_heartbeat(monkeypatch, cancel_fetch):
    harness = _Harness(monkeypatch, (_shared_row(),))
    evidence = _block_cleanup_through_heartbeat(harness, monkeypatch, cancel_fetch)
    task = asyncio.create_task(harness.run())
    try:
        await asyncio.wait_for(evidence.renewing.wait(), 2)
        if cancel_fetch:
            await asyncio.wait_for(evidence.entered.wait(), 2)
            task.cancel("primary cancellation")
            await asyncio.sleep(0)
            evidence.fetch_release.set()
        await asyncio.wait_for(evidence.closing.wait(), 2)
        if cancel_fetch:
            task.cancel("second cancellation")
            await asyncio.sleep(0)
        evidence.close_release.set()
        await asyncio.wait_for(evidence.draining.wait(), 2)
        task.cancel("third cancellation")
        await asyncio.sleep(0)
        assert not task.done() and harness.parts == []
    finally:
        evidence.fetch_release.set()
        evidence.close_release.set()
        evidence.renew_release.set()
    error = asyncio.CancelledError if cancel_fetch else capture.SnowflakeCaptureCleanupUncertain
    with pytest.raises(error) as caught:
        await task
    if cancel_fetch:
        assert caught.value.args == ("primary cancellation",)
        assert any("cleanup is unconfirmed" in note for note in caught.value.__notes__)
    assert caught.value.__cause__ is evidence.owner.close_error
    assert harness.connection.closed and "eof" not in harness.timeline and "seal" not in harness.timeline


@pytest.mark.asyncio
async def test_eof_events_do_not_authorize_seal_before_final_next_cleanup(monkeypatch):
    harness = _Harness(monkeypatch, (_shared_row(),))
    advance = capture._AffinityLanding.next_event
    evidence = SimpleNamespace(eof_count=0)

    def final_failure(owner):
        if evidence.eof_count == 2:
            raise RuntimeError("synthetic final advance failure")
        event = advance(owner)
        evidence.eof_count += isinstance(event, SnowflakeLandingEOF)
        return event

    monkeypatch.setattr(capture._AffinityLanding, "next_event", final_failure)
    with pytest.raises(RuntimeError, match="final advance"):
        await harness.run()
    assert harness.cursor.closed and harness.connection.closed
    assert "eof" not in harness.timeline and "seal" not in harness.timeline


@pytest.mark.asyncio
async def test_append_failure_closes_and_keeps_capture_pending(monkeypatch):
    harness = _Harness(monkeypatch, (_shared_row(),))

    async def reject(*arguments, **options):
        raise RuntimeError("synthetic append failure")

    monkeypatch.setattr(capture, "append_pending_parquet_part", reject)
    with pytest.raises(RuntimeError, match="append failure"):
        await harness.run()
    assert harness.pending is not None and harness.cursor.closed and harness.connection.closed
    assert "rollback" in harness.timeline and "eof" not in harness.timeline and "seal" not in harness.timeline


@pytest.mark.asyncio
async def test_primary_failure_reports_cleanup_uncertainty_without_masking(monkeypatch):
    harness = _Harness(monkeypatch, (_shared_row(),))
    primary = RuntimeError("synthetic append failure")

    async def reject(*arguments, **options):
        raise primary

    def fail_close():
        raise RuntimeError("synthetic close failure")

    harness.cursor.close = fail_close
    monkeypatch.setattr(capture, "append_pending_parquet_part", reject)
    with pytest.raises(RuntimeError) as caught:
        await harness.run()
    assert caught.value is primary
    assert any("cleanup is unconfirmed" in note for note in caught.value.__notes__)
    assert harness.connection.closed and "seal" not in harness.timeline


@pytest.mark.asyncio
async def test_coordinator_retains_no_previous_part_events(monkeypatch):
    harness = _Harness(monkeypatch, (_shared_row(key=str(index)) for index in range(100)))
    advance = capture._AffinityLanding.next_event
    evidence = SimpleNamespace(previous=None)

    def bounded_next(owner):
        event = advance(owner)
        assert evidence.previous is None or evidence.previous() is None
        evidence.previous = None if event is capture._END else weakref.ref(event)
        return event

    monkeypatch.setattr(capture._AffinityLanding, "next_event", bounded_next)
    await harness.run()
    assert len(harness.parts) == 200 and len(harness.receipts) == 2
    assert max(len(receipt.canonical_manifest) for receipt in harness.receipts) < 4096


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["stream_order", "metadata_key", "schema", "schema_type", "token", "query"])
async def test_landing_metadata_drift_closes_before_pending_begin(monkeypatch, drift):
    harness = _Harness(monkeypatch)
    opener = harness.adapter.open_bundle_landing

    def invalid(*arguments, **options):
        return _drift_landing_metadata(opener(*arguments, **options), drift)

    monkeypatch.setattr(harness.adapter, "open_bundle_landing", invalid)
    with pytest.raises(ValueError):
        await harness.run()
    assert harness.cursor.closed and harness.connection.closed
    assert harness.pending is None


def _drift_landing_metadata(result, drift):
    if drift == "stream_order":
        result.metadata = tuple(reversed(result.metadata))
        return result
    if drift == "metadata_key":
        result.metadata = (replace(result.metadata[0], semantic_token_metadata_key="wrong_key"), result.metadata[1])
        return result
    if drift == "schema":
        result.schemas = (tuple(reversed(result.schemas[0])), result.schemas[1])
        return result
    if drift == "schema_type":
        result.schemas = (
            (replace(result.schemas[0][0], source_type="BOOLEAN"), *result.schemas[0][1:]),
            result.schemas[1],
        )
        return result
    if drift == "token":
        result.source_snapshot_token = "synthetic-other-snapshot"
        return result
    result.query_id = ""

    return result


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["stream", "ordinal", "records", "arrow", "capture_stream", "early_eof"])
async def test_malformed_event_stops_before_append_or_eof(monkeypatch, drift):
    harness = _Harness(monkeypatch, (_shared_row(),))
    advance = capture._AffinityLanding.next_event

    def invalid(owner):
        event = advance(owner)
        if not isinstance(event, SnowflakeLandingPart):
            return event
        if drift == "stream":
            return replace(event, stream_id="unknown_source")
        if drift == "ordinal":
            return replace(event, ordinal=True)
        if drift == "records":
            return replace(event, record_count=-1)
        if drift == "arrow":
            return replace(event, arrow_byte_count=True)
        if drift == "capture_stream":
            return replace(
                event,
                capture=replace(event.capture, manifest=replace(event.capture.manifest, stream_id="detail_source")),
            )
        return SnowflakeLandingEOF(event.stream_id, 1, 1)

    monkeypatch.setattr(capture._AffinityLanding, "next_event", invalid)
    with pytest.raises(ValueError):
        await harness.run()
    assert harness.cursor.closed and harness.connection.closed
    assert harness.parts == [] and "eof" not in harness.timeline and "seal" not in harness.timeline


@pytest.mark.asyncio
async def test_heartbeat_runs_during_blocking_driver_fetch(monkeypatch):
    harness = _Harness(monkeypatch, (_shared_row(),))
    monkeypatch.setattr(capture.lifecycle, "DEFAULT_LEASE_SECONDS", 0.03)
    loop = asyncio.get_running_loop()
    entered = asyncio.Event()
    renewed = asyncio.Event()
    release = threading.Event()
    advance = capture._AffinityLanding.next_event

    def blocking(owner):
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5)
        return advance(owner)

    async def renew(*arguments):
        if entered.is_set():
            renewed.set()
        return harness.grant

    monkeypatch.setattr(capture._AffinityLanding, "next_event", blocking)
    monkeypatch.setattr(capture, "_renew_bundle_lease", renew)
    task = asyncio.create_task(harness.run())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        await asyncio.wait_for(renewed.wait(), 2)
        assert not task.done() and harness.parts == []
    finally:
        release.set()
    assert (await task).status == "capture_sealed"


@pytest.mark.asyncio
async def test_deadline_expiry_during_fetch_discards_late_part(monkeypatch):
    harness = _Harness(monkeypatch, (_shared_row(),))
    advance = capture._AffinityLanding.next_event

    def expire(owner):
        event = advance(owner)
        owner.attempt.deadline = 0
        return event

    monkeypatch.setattr(capture._AffinityLanding, "next_event", expire)
    with pytest.raises(capture.SnowflakeCaptureError, match="deadline expired"):
        await harness.run()
    assert harness.cursor.closed and harness.connection.closed
    assert harness.parts == [] and "eof" not in harness.timeline


@pytest.mark.asyncio
async def test_real_final_cursor_close_failure_is_not_a_capture_receipt(monkeypatch):
    harness = _Harness(monkeypatch, (_shared_row(),))

    def fail_close():
        raise RuntimeError("synthetic source close failure")

    harness.cursor.close = fail_close
    with pytest.raises(ValueError, match="resource cleanup failed") as caught:
        await harness.run()
    assert any("cleanup is unconfirmed" in note for note in caught.value.__notes__)
    assert harness.connection.closed and "eof" not in harness.timeline and "seal" not in harness.timeline


@pytest.mark.asyncio
async def test_incompatible_landing_signature_rejected_before_reservation(monkeypatch):
    harness = _Harness(monkeypatch)
    monkeypatch.setattr(harness.adapter, "open_bundle_landing", lambda statement, credentials: None)
    with pytest.raises(capture.SnowflakeCaptureError, match="explicit landing contract"):
        await harness.run()
    assert harness.timeline == []
