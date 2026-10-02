# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Segmented capture rejects incomplete identity before build or publication."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from contextlib import nullcontext
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.custom_import import build_graph as graph
from process.custom_import import build_output as output
from process.custom_import import build_source as staging
from process.custom_import import capture_pending as pending
from process.custom_import import capture_store
from process.custom_import import snowflake_capture as capture
from process.custom_import.capture_store import CaptureBundleConflict, CaptureStoreError
from process.custom_import.execution import lease_token_sha256
from process.custom_import.runner_types import CandidateRunnerError, LeaseAuthorityLost
from tests.test_custom_import_build_graph import _request as _graph_request
from tests.test_custom_import_build_output import _candidate, _generation, _group
from tests.test_custom_import_build_source import _request as _source_request
from tests.test_custom_import_capture_pending import (
    _PART_IDS,
    _bundle,
    _capture,
    _receipt,
    _request,
    _segmented_read_case,
    _Session,
    _stream_header,
)
from tests.test_custom_import_snowflake_capture import _Harness


@pytest.mark.parametrize(
    "changes,message",
    [
        ({"source_snapshot_token": "synthetic-other-snapshot"}, "snapshot has drifted"),
        ({"content_sha256": "ab" * 32}, "payload identity has drifted"),
        ({"manifest_sha256": "ab" * 32}, "manifest digest has drifted"),
    ],
)
def test_receipt_identity_rejection(changes, message):
    """A well-formed receipt cannot replace any retained identity component."""

    request = _request()
    receipt = replace(_receipt(request), **changes)
    with pytest.raises(CaptureBundleConflict, match=message):
        pending._receipt_document(receipt, _bundle(request), request.policy)


@pytest.mark.parametrize("damage", ["type", "accounting", "budget"])
def test_stream_receipt_rejection(damage):
    """Stream accounting must match both the receipt and its admitted budget."""

    request = _request()
    receipt = _receipt(request)
    document = json.loads(receipt.canonical_manifest)
    header = _stream_header(**{"committed_" + name: document[name] for name in pending._ACCOUNTING_FIELDS})
    expected_error, message = CaptureBundleConflict, "accounting has drifted"
    if damage == "type":
        receipt = object()
        expected_error, message = CaptureStoreError, "requires CaptureReceipt"
    elif damage == "accounting":
        header.committed_record_count = 1
    else:
        excess = request.policy.stream_budget["maximum_manifest_bytes"] + 1
        receipt = _receipt(request, manifest_byte_count=excess)
        header.committed_manifest_byte_count = excess
        expected_error, message = CaptureStoreError, "declared stream budget"
    with pytest.raises(expected_error, match=message):
        pending._validate_stream_receipt(receipt, header, _bundle(request), request.policy)


@pytest.mark.parametrize("damage", ["metadata", "snapshot", "format", "compression"])
async def test_retained_part_rejection_closes(monkeypatch, damage):
    """A damaged first part never reaches the consumer and closes its cursor."""

    case, _ = _segmented_read_case(monkeypatch)
    part_fields = list(case.source[0])
    if damage == "metadata":
        part_fields.pop()
    else:
        manifest = json.loads(part_fields[6])
        field, value = {
            "snapshot": ("source_snapshot_token", "synthetic-other-snapshot"),
            "format": ("format", "csv"),
            "compression": ("compression", "gzip"),
        }[damage]
        manifest[field] = value
        part_fields[6] = pending.canonical_json(manifest)
        part_fields[7] = hashlib.sha256(part_fields[6].encode()).digest()
    case.source[0] = tuple(part_fields)
    message = "metadata is incomplete" if damage == "metadata" else "manifest identity has drifted"
    delivered_parts = []
    with pytest.raises(CaptureBundleConflict, match=message):
        async with capture_store.open_segmented_parquet_parts(case.session, **_PART_IDS) as parts:
            async for retained in parts:
                delivered_parts.append(retained)
    assert delivered_parts == []
    assert case.session.rows.close_count == 1


@pytest.mark.parametrize("damage", ["missing", "request", "binding_missing", "binding_owner", "binding_digest"])
async def test_pending_authority_rejection(monkeypatch, damage):
    """A missing or drifted source authority never returns a usable producer."""

    request = _request(source_binding_revision_id=7, source_binding_sha256=b"b" * 32)
    execution = SimpleNamespace(request_identity_sha256=request.request_identity_sha256, source_binding_revision_id=7)
    binding = SimpleNamespace(dataset_id=1, definition_revision_id=2, schema_revision_id=3, binding_sha256=b"b" * 32)
    if damage == "request":
        execution.request_identity_sha256 = b"x" * 32
    elif damage == "binding_owner":
        binding.schema_revision_id = 99
    elif damage == "binding_digest":
        binding.binding_sha256 = b"x" * 32
    lock = AsyncMock(return_value=None if damage == "missing" else (execution, None, None))
    monkeypatch.setattr(pending.lifecycle, "_lock_current_capture_binding", lock)
    session = _Session()
    session.get = AsyncMock(return_value=None if damage == "binding_missing" else binding)
    message = (
        "running lease"
        if damage == "missing"
        else "request has drifted"
        if damage == "request"
        else "binding has drifted"
    )
    with pytest.raises(CaptureBundleConflict, match=message):
        await pending._lock_execution(session, request)
    assert session.statements == []
    assert session.get.await_count == int(damage.startswith("binding"))


@pytest.mark.parametrize(
    "field,value,message",
    [
        ("query_id", None, "query identity is invalid"),
        ("metadata", (), "metadata stream coverage is invalid"),
        ("schemas", (), "schema stream coverage is invalid"),
        ("metadata", (object(), object()), "stream metadata is invalid"),
    ],
)
async def test_landing_shape_rejection_closes(monkeypatch, field, value, message):
    """Malformed metadata closes the real fake landing before any retention."""

    harness = _Harness(monkeypatch)
    opener = harness.adapter.open_bundle_landing

    def malformed_landing(*arguments, **options):
        landing = opener(*arguments, **options)
        setattr(landing, field, value)
        return landing

    monkeypatch.setattr(harness.adapter, "open_bundle_landing", malformed_landing)
    with pytest.raises(capture.SnowflakeCaptureError, match=message):
        await harness.run()
    assert harness.cursor.closed and harness.connection.closed
    assert harness.pending is None and harness.parts == [] and harness.receipts == ()
    assert "seal" not in harness.timeline
    assert harness.finishes[-1]["terminal_state"] == "failed"


def test_landing_requires_owned_result(monkeypatch):
    """An unsupported result is rejected without opening or owning a source."""

    harness = _Harness(monkeypatch)
    statement = harness.builder.build_statement(harness.request.bundle_request)
    with pytest.raises(capture.SnowflakeCaptureError, match="invalid landing result"):
        capture._landing_metadata(statement, object())
    assert harness.timeline == [] and harness.cursor.executed == []


@pytest.mark.parametrize("capability", ["session", "statement", "landing", "credentials"])
async def test_missing_capability_is_source_free(monkeypatch, capability):
    """Capability admission runs before reservation, credentials or source I/O."""

    harness = _Harness(monkeypatch)
    if capability == "session":
        harness.session = None
    elif capability == "statement":
        monkeypatch.setattr(harness.builder, "build_statement", None)
    elif capability == "landing":
        monkeypatch.setattr(harness.adapter, "open_bundle_landing", None)
    else:
        monkeypatch.setattr(harness.credentials, "load_key_pair", None)
    message = (
        "session factory and statement builder" if capability in {"session", "statement"} else "landing and credential"
    )
    with pytest.raises(capture.SnowflakeCaptureError, match=message):
        await harness.run()
    assert harness.timeline == [] and harness.finishes == [] and harness.pending is None
    assert harness.cursor.executed == []


@pytest.mark.parametrize("stream_index,code", [(0, "root_not_object"), (1, "child_not_object")])
def test_non_object_source_retains_rejection(stream_index, code):
    """Invalid row shapes retain a typed rejection without accepted payloads."""

    request = _source_request()
    prepared = staging._prepare_row(request, request.definition.source_streams[stream_index], [])
    assert prepared.rejection.code == code
    assert prepared.raw_key is None and prepared.typed_key is None
    assert prepared.payload is None and prepared.payload_hash is None
    assert prepared.child_key is None and prepared.child_hash is None
    assert 0 < prepared.byte_count <= request.page_byte_limit


@pytest.mark.parametrize("damage", ["isolation", "capture", "fence", "token", "expiry", "deadline"])
def test_graph_read_authority_rejection(monkeypatch, damage):
    """A valid build identity still requires current read isolation and lease."""

    request = _source_request()
    build = SimpleNamespace(
        **vars(request),
        capture_bundle_id=20,
        request_identity_sha256=b"r" * 32,
        producing_fence=request.fence,
        producing_token_sha256=lease_token_sha256(request.lease_token),
        base_generation_id=None,
        base_pointer_version=0,
        refresh_mode=request.definition.refresh_mode,
    )
    execution = SimpleNamespace(state="running", capture_bundle_id=20, request_identity_sha256=b"r" * 32)
    now = dt.datetime(2029, 1, 1, tzinfo=dt.UTC)
    lease = SimpleNamespace(
        fence=1, token_sha256=build.producing_token_sha256, expires_at=now + dt.timedelta(seconds=10)
    )
    isolation = "repeatable read" if damage == "isolation" else "read committed"
    if damage == "capture":
        execution.capture_bundle_id = 21
    elif damage in {"fence", "token", "expiry"}:
        setattr(
            lease,
            {"fence": "fence", "token": "token_sha256", "expiry": "expires_at"}[damage],
            {"fence": 2, "token": None, "expiry": None}[damage],
        )
    elif damage == "deadline":
        lease.expires_at = now
    session = SimpleNamespace(
        begin=nullcontext,
        execute=Mock(side_effect=[None, SimpleNamespace(one=lambda: (build, execution, lease, now, isolation))]),
    )
    error = CandidateRunnerError if damage in {"isolation", "capture"} else LeaseAuthorityLost
    message = (
        "READ COMMITTED"
        if damage == "isolation"
        else "identity or bounds"
        if damage == "capture"
        else "deadline elapsed"
        if damage == "deadline"
        else "current running attempt"
    )
    with pytest.raises(error, match=message), graph._read_transaction(session, request, 7):
        pytest.fail("rejected authority cannot expose a graph page")
    assert session.execute.call_count == 2


@pytest.mark.parametrize("damage", ["empty", "missing_context"])
def test_output_requires_retained_winner(monkeypatch, damage):
    """A complete group cannot finalize without its unique retained context."""

    request = _graph_request()
    candidate = _candidate()
    registry, group = _group(request, candidate)
    monkeypatch.setattr(
        output, "_context_candidates", lambda *_arguments: iter(()) if damage == "empty" else iter((candidate,))
    )
    context_lookup = Mock(return_value=None)
    monkeypatch.setattr(output, "_one_row", context_lookup)
    message = "produce one winner" if damage == "empty" else "no retained candidate context"
    with pytest.raises(CandidateRunnerError, match=message):
        output._reduce_group(None, request, registry, 7, _generation(request), group)
    assert context_lookup.call_count == int(damage == "missing_context")


@pytest.mark.parametrize(
    "changes,message",
    [
        ({"token": "synthetic-owner"}, "token must be bytes-like"),
        ({"request_identity_sha256": b"short"}, "exactly 32 bytes"),
        ({"policy": None}, "explicit segmented policy"),
    ],
)
def test_pending_request_requires_exact_types(changes, message):
    """Construction preserves immutable binary authority and explicit policy."""

    with pytest.raises(CaptureStoreError, match=message):
        _request(**changes)


async def test_pending_request_rejects_untyped_input():
    """An active clean session cannot substitute for exact request identity."""

    session = _Session()
    with pytest.raises(CaptureStoreError, match="requires an exact request"):
        await pending.begin_pending_parquet_bundle(session, request=object())
    assert session.statements == []


@pytest.mark.parametrize("damage", ["unrequested_binding", "bundle_budget"])
def test_pending_bundle_rejects_unadmitted_identity(damage):
    """Unrequested source bindings and excessive aggregate counts fail closed."""

    request = _request()
    bundle = _bundle(request)
    if damage == "unrequested_binding":
        bundle.source_binding_sha256 = b"b" * 32
        with pytest.raises(CaptureBundleConflict, match="identity has drifted"):
            pending._validate_bundle(bundle, request)
    else:
        bundle.committed_part_count = request.policy.bundle_budget["maximum_parts"] + 1
        with pytest.raises(CaptureStoreError, match="declared bundle budget"):
            pending._bundle_manifest(bundle, (("records", 1),), {"records": _receipt(request)}, request.policy)


@pytest.mark.parametrize("damage", ["unknown", "missing", "bundle_closed", "stream_closed"])
async def test_pending_stream_requires_open_coverage(monkeypatch, damage):
    """Only a declared pending stream can accept new durable capture bytes."""

    request = _request()
    bundle, header = _bundle(request), _stream_header()
    if damage == "bundle_closed":
        bundle.capture_state = "sealed"
    elif damage == "stream_closed":
        header.capture_state = "sealed"
    session = _Session()
    session.get = AsyncMock(return_value=None if damage == "missing" else header)
    monkeypatch.setattr(pending, "_validated_streams", AsyncMock(return_value=(("records", 1),)))
    error = CaptureStoreError if damage == "unknown" else CaptureBundleConflict
    message = "not declared" if damage == "unknown" else "stream is closed"
    with pytest.raises(error, match=message):
        await pending._pending_stream(session, bundle, request, "unknown" if damage == "unknown" else "records")
    assert session.statements == []


@pytest.mark.parametrize("damage", ["missing", "owner", "stream"])
async def test_pending_source_requires_declared_revision(damage):
    """Retained parts cannot use absent definitions or undeclared streams."""

    request = _request()
    revision = SimpleNamespace(
        dataset_id=1, schema_revision_id=3, canonical_definition=_source_request().definition.canonical
    )
    if damage == "owner":
        revision.dataset_id = 99
    session = _Session()
    session.get = AsyncMock(return_value=None if damage == "missing" else revision)
    message = "no declared Parquet stream" if damage == "stream" else "definition has drifted"
    with pytest.raises(CaptureStoreError, match=message):
        await pending._source_stream(session, request, "unknown")
    assert session.statements == []


@pytest.mark.parametrize("damage", ["ordinal", "manifest"])
def test_pending_part_requires_admitted_size(damage):
    """Part admission checks the ordinal and complete canonical manifest size."""

    request = _request()
    policy = request.policy
    ordinal = policy.stream_budget["maximum_parts"] + 1 if damage == "ordinal" else 1
    if damage == "manifest":
        policy = replace(policy, maximum_part_manifest_bytes=1)
    with pytest.raises(CaptureStoreError, match="declared .* budget"):
        pending._part_values(_capture(request), ordinal, 0, 0, policy)


@pytest.mark.parametrize("receipts", [(), [], (object(),)])
async def test_seal_requires_typed_receipts(monkeypatch, receipts):
    """Incomplete receipt coverage is rejected before acquiring bundle locks."""

    lock = AsyncMock()
    monkeypatch.setattr(pending, "_lock_bundle", lock)
    with pytest.raises(CaptureStoreError, match="non-empty tuple of CaptureReceipts"):
        await pending.seal_pending_parquet_bundle(
            _Session(), request=_request(), capture_bundle_id=5, receipts=receipts
        )
    lock.assert_not_awaited()


@pytest.mark.parametrize("damage", ["deadline", "state", "retry"])
async def test_seal_rejection_performs_no_write(monkeypatch, damage):
    """Seal authority and exact retries are validated before durable updates."""

    request = _request()
    bundle = _bundle(request)
    session = _Session()
    if damage == "deadline":
        monkeypatch.setattr(pending, "_fresh_remaining_seconds", AsyncMock(return_value=0.001))
        with pytest.raises(CaptureBundleConflict, match="insufficient seal authority"):
            await pending._arm_seal_timeout(session, request, bundle)
    else:
        bundle.capture_state = "sealed" if damage == "retry" else "invalid"
        bundle.canonical_manifest = "{}"
        with pytest.raises(
            CaptureBundleConflict, match="retry has drifted" if damage == "retry" else "state is invalid"
        ):
            await pending._seal_bundle(
                session, request, bundle, SimpleNamespace(capture_bundle_id=5), '{"different":true}'
            )
    assert session.statements == []


@pytest.mark.parametrize(
    "phase,generation,expected", [("rejected", None, None), ("output", 10, 10), ("source", None, "error")]
)
async def test_graph_phase_admission_is_source_free(monkeypatch, phase, generation, expected):
    """Rejected, resumed and premature builds do not start graph work."""

    snapshot = SimpleNamespace(phase=phase, generation_id=generation)
    monkeypatch.setattr(graph, "_snapshot", AsyncMock(return_value=snapshot))
    page = Mock(side_effect=AssertionError("a gated phase cannot open a graph page"))
    monkeypatch.setattr(graph, "_page_session", page)
    if expected == "error":
        with pytest.raises(CandidateRunnerError, match="completed source admission"):
            await graph._build_graph(None, _source_request(), 7)
    else:
        assert await graph._build_graph(None, _source_request(), 7) == expected
    page.assert_not_called()


def test_graph_rejects_absent_build():
    """A nonexistent build cannot carry read or publication authority."""

    with pytest.raises(CandidateRunnerError, match="build does not exist"):
        graph._verify_request(None, _source_request(), object())


def test_graph_rejects_negative_remaining_budget():
    """Reserved payload bytes cannot silently exhaust a graph page budget."""

    request = _source_request()
    query = Mock()
    with pytest.raises(CandidateRunnerError, match="no remaining payload budget"):
        list(
            graph._read_rows(
                None, request, 7, query, (), (), bounds=graph._ReadPage(reserve_bytes=request.page_byte_limit + 1)
            )
        )
    query.assert_not_called()


def test_graph_fingerprint_requires_complete_families(monkeypatch):
    """A partial retained family cannot be included in a candidate fingerprint."""

    family = SimpleNamespace(complete_at=None, root_key_sha256=b"r" * 32)
    monkeypatch.setattr(graph, "_read_rows", lambda *_arguments: iter(((family,),)))
    with pytest.raises(CandidateRunnerError, match="completed families"):
        graph._candidate_digest(None, _source_request(), 7)


@pytest.mark.parametrize("dirty_field", ["transaction", "new", "dirty", "deleted"])
async def test_source_page_requires_clean_transaction(dirty_field):
    """Foreign pending mutations cannot be included in a source build page."""

    session = SimpleNamespace(
        in_transaction=lambda: dirty_field != "transaction", new=(), dirty=(), deleted=(), execute=AsyncMock()
    )
    if dirty_field != "transaction":
        setattr(session, dirty_field, (object(),))
    with pytest.raises(CandidateRunnerError, match="clean caller-owned transaction"):
        await staging._lock_page(session, _source_request(), 7)
    session.execute.assert_not_awaited()


async def test_source_entry_requires_exact_request():
    """An invalid request never opens capture replay or a build transaction."""

    factory = Mock()
    with pytest.raises(TypeError, match="requires SourceBuildRequest"):
        await staging.stage_segmented_source(factory, object())
    factory.assert_not_called()


def test_source_request_requires_definition():
    """Explicit bounds cannot admit a build without its typed definition."""

    with pytest.raises(ValueError, match="build definition is required"):
        _source_request(definition=None)


@pytest.mark.parametrize("started", [None, dt.datetime(2030, 1, 1)])
async def test_capture_requires_authoritative_start(monkeypatch, started):
    """Missing or naive execution time cannot start credentials or capture."""

    harness = _Harness(monkeypatch)
    harness.started = started
    with pytest.raises(capture.SnowflakeCaptureError, match="acquisition start is unavailable"):
        await harness.run()
    assert "credentials" not in harness.timeline and harness.pending is None
    assert harness.cursor.executed == []
