# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic standalone replay and coordination checks; no external I/O."""

from __future__ import annotations

import asyncio
import hashlib
import json
import socket
import traceback
from dataclasses import replace
from datetime import datetime, timezone
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock

import aiohttp
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from process.custom_import import https_candidate
from process.custom_import.capture import capture_stream
from process.custom_import.capture_limits import CaptureLimits
from process.custom_import.definition import CustomImportDefinition, canonical_json
from process.custom_import.execution import ExecutionSubmission, ExecutionTransition, LeaseGrant
from process.custom_import.family import assemble_root_families
from process.custom_import.https_candidate import (
    HttpsCandidateError,
    HttpsCandidateRequest,
    HttpsObject,
    https_request_identity_sha256,
    run_https_candidate,
)
from process.custom_import.runner_types import CandidateRunResult
from tests.test_custom_import_definition import _raw_definition


def _request(*, tables_by_stream=None, definition=None, limits=CaptureLimits()):
    """Use existing declaration fixtures with a complete two-object Parquet source."""
    if definition is None:
        document = _raw_definition()
        document["refresh_mode"] = "snapshot"
        for stream in document["streams"]:
            stream.update(format="parquet", compression="none")
        definition = CustomImportDefinition.from_mapping(document)
    if tables_by_stream is None:
        tables_by_stream = {
            "providers": pa.table({"Provider ID": ["1234567893"], "Provider Name": ["Synthetic Provider"]}),
            "rates": pa.table(
                {
                    "Provider ID": ["1234567893"],
                    "Service Code": ["A100"],
                    "Amount": pa.array([None], type=pa.decimal128(10, 2)),
                }
            ),
        }
    payloads_by_stream = {}
    for stream_id, table in tables_by_stream.items():
        destination = BytesIO()
        pq.write_table(table, destination)
        payloads_by_stream[stream_id] = destination.getvalue()
    request = HttpsCandidateRequest(
        dataset_id=11,
        definition_revision_id=12,
        schema_revision_id=13,
        definition=definition,
        objects=tuple(
            HttpsObject(
                stream_id=stream.stream_id,
                url=f"https://downloads.example/{stream.stream_id}.parquet?sample=redaction-marker",
                expected_sha256=hashlib.sha256(payloads_by_stream[stream.stream_id]).hexdigest(),
                expected_bytes=len(payloads_by_stream[stream.stream_id]),
            )
            for stream in definition.source_streams
        ),
        source_snapshot_token="synthetic-snapshot",
        idempotency_key="synthetic-execution",
        lease_token="synthetic-owner",
        limits=limits,
        timeout_seconds=5,
        complete_scope=True,
    )
    return request, payloads_by_stream


def _retained(request, payloads_by_stream):
    return tuple(
        https_candidate._retained_capture(
            request,
            item,
            stream,
            capture_stream(
                BytesIO(payloads_by_stream[stream.stream_id]),
                stream,
                source_snapshot_token=request.source_snapshot_token,
                limits=request.limits,
            ),
        )
        for item, stream in zip(request.objects, request.definition.source_streams, strict=True)
    )


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    attempts = []

    def forbidden_network(*_args, **_kwargs):
        attempts.append(True)
        raise AssertionError("external I/O is forbidden")

    monkeypatch.setattr(socket, "getaddrinfo", forbidden_network)
    monkeypatch.setattr(aiohttp.ClientSession, "_request", forbidden_network)
    yield
    assert not attempts


class _Transaction:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        self.session.is_active = True
        return self

    async def __aexit__(self, exception_type, *_args):
        self.session.outcome = "rollback" if exception_type else "commit"
        self.session.is_active = False


class _Session:
    def __init__(self):
        self.is_active = False
        self.outcome = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        assert not self.is_active

    def begin(self):
        return _Transaction(self)


@pytest.fixture
def coordinator(monkeypatch):
    """Keep the adapter real; replace only database lifecycle and source I/O."""
    request, payloads_by_stream = _request()
    grant = LeaseGrant(17, 1, datetime(2030, 1, 1, tzinfo=timezone.utc), "running")

    async def renew(_session, **kwargs):
        return replace(grant, fence=kwargs["fence"])

    mocks_by_name = {
        "load_registry": AsyncMock(),
        "reserve_execution": AsyncMock(return_value=ExecutionSubmission(17, "queued", True)),
        "lookup_execution_request": AsyncMock(return_value=ExecutionSubmission(17, "running", False, 23)),
        "claim_execution": AsyncMock(return_value=grant),
        "resume_execution": AsyncMock(return_value=replace(grant, fence=2)),
        "heartbeat_execution": AsyncMock(side_effect=renew),
        "bind_execution_capture_bundle": AsyncMock(return_value=ExecutionSubmission(17, "running", False, 23)),
        "finish_execution": AsyncMock(return_value=ExecutionTransition(17, "failed", True)),
        "register_replayable_parquet_bundle": AsyncMock(return_value=SimpleNamespace(capture_bundle_id=23)),
        "load_replayable_parquet_bundle": AsyncMock(return_value=_retained(request, payloads_by_stream)),
        "run_claimed_candidate": AsyncMock(return_value=CandidateRunResult(status="no_change", execution_id=17)),
    }
    for name, replacement in mocks_by_name.items():
        owner = https_candidate
        if hasattr(https_candidate.execution, name):
            owner = https_candidate.execution
        elif name == "run_claimed_candidate":
            owner = https_candidate.runner
        monkeypatch.setattr(owner, name, replacement)

    async def acquire(_url, stream, **kwargs):
        return capture_stream(
            BytesIO(payloads_by_stream[stream.stream_id]),
            stream,
            source_snapshot_token=kwargs["source_snapshot_token"],
            limits=kwargs["limits"],
        )

    acquire_mock = AsyncMock(side_effect=acquire)
    monkeypatch.setattr(https_candidate.https, "acquire_https", acquire_mock)
    sessions = []

    def session_factory():
        session = _Session()
        sessions.append(session)
        return session

    return SimpleNamespace(
        request=request,
        payloads_by_stream=payloads_by_stream,
        mocks_by_name=mocks_by_name,
        acquire=acquire_mock,
        session_factory=session_factory,
        sessions=sessions,
        grant=grant,
    )


def test_identity_is_complete_stable_and_omits_ephemeral_lease():
    request, _ = _request()
    identity = https_request_identity_sha256(request)
    assert len(identity) == 32
    assert identity == https_request_identity_sha256(replace(request, lease_token="replacement-owner"))
    assert identity == https_request_identity_sha256(replace(request, objects=request.objects[::-1]))
    for changed in (
        replace(request, dataset_id=14),
        replace(request, definition_revision_id=14),
        replace(request, schema_revision_id=14),
        replace(request, source_snapshot_token="another-snapshot"),
        replace(request, timeout_seconds=4),
        replace(request, limits=replace(request.limits, maximum_records=3)),
        replace(
            request,
            objects=(replace(request.objects[0], url="https://downloads.example/replacement"), request.objects[1]),
        ),
        replace(request, objects=(replace(request.objects[0], expected_sha256="0" * 64), request.objects[1])),
        replace(request, objects=(replace(request.objects[0], expected_bytes=10), request.objects[1])),
    ):
        assert identity != https_request_identity_sha256(changed)
    document = json.loads(request.definition.canonical)
    document["aliases"]["providers"]["Alternate Name"] = "display_name"
    assert identity != https_request_identity_sha256(
        replace(request, definition=CustomImportDefinition.from_mapping(document))
    )
    assert "redaction-marker" not in repr(request)
    assert request.source_snapshot_token not in repr(request)
    assert request.lease_token not in repr(request)


@pytest.mark.asyncio
async def test_retained_content_identity_ignores_execution_transport():
    request, payloads = _request()
    refreshed = replace(
        request,
        objects=tuple(replace(item, url=item.url + "&renewed=true") for item in request.objects),
        timeout_seconds=10,
        idempotency_key="synthetic-refreshed-execution",
    )
    retained = _retained(request, payloads)
    assert https_request_identity_sha256(refreshed) != https_request_identity_sha256(request)
    assert _retained(refreshed, payloads) == retained
    records = await https_candidate._replay(refreshed, retained)
    assert records["providers"][0]["display_name"] == "Synthetic Provider"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid",
    [
        "partial",
        "duplicate",
        "extra",
        "scope",
        "format",
        "token",
        "url",
        "bytes",
        "digest",
        "limits",
        "limits_type",
        "deadline",
    ],
)
async def test_invalid_envelope_fails_before_database_or_acquisition(coordinator, invalid):
    request = coordinator.request
    match invalid:
        case "partial":
            request = replace(request, objects=request.objects[:1])
        case "duplicate":
            request = replace(request, objects=request.objects + request.objects[:1])
        case "extra":
            request = replace(request, objects=request.objects + (replace(request.objects[0], stream_id="extra"),))
        case "scope":
            request = replace(request, complete_scope=False)
        case "format":
            document = json.loads(request.definition.canonical)
            document["streams"][0]["format"] = "csv"
            request = replace(request, definition=CustomImportDefinition.from_mapping(document))
        case "token":
            request = replace(request, source_snapshot_token="not\na-token")
        case "url":
            request = replace(
                request, objects=(replace(request.objects[0], url="https://127.0.0.1/object"), request.objects[1])
            )
        case "bytes":
            request = replace(request, objects=(replace(request.objects[0], expected_bytes=True), request.objects[1]))
        case "digest":
            request = replace(
                request, objects=(replace(request.objects[0], expected_sha256="invalid"), request.objects[1])
            )
        case "limits":
            request = replace(request, limits=replace(request.limits, maximum_records=1_000_001))
        case "limits_type":
            request = replace(request, limits=None)
        case _:
            request = replace(request, timeout_seconds=float("nan"))
    with pytest.raises(HttpsCandidateError, match="^https_candidate_failed$"):
        await run_https_candidate(coordinator.session_factory, request)
    assert not coordinator.sessions
    coordinator.acquire.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout_seconds", (3600.1, 3601))
@pytest.mark.parametrize("resume", (False, True))
async def test_preparation_ceiling_rejects_before_database_or_acquisition(coordinator, timeout_seconds, resume):
    request = replace(coordinator.request, timeout_seconds=timeout_seconds)
    with pytest.raises(HttpsCandidateError, match="^https_request_invalid$"):
        https_request_identity_sha256(request)
    with pytest.raises(HttpsCandidateError, match="^https_candidate_failed$"):
        await run_https_candidate(coordinator.session_factory, request, resume=resume)
    assert not coordinator.sessions
    coordinator.acquire.assert_not_awaited()
    for operation in coordinator.mocks_by_name.values():
        operation.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("argument", "value"), [("session_factory", None), ("session_factory", object()), ("resume", None), ("resume", 1)]
)
async def test_invalid_entrypoint_arguments_do_not_touch_capture_or_authority(coordinator, argument, value):
    arguments_by_name = {
        "session_factory": coordinator.session_factory,
        "request": coordinator.request,
        "resume": False,
    }
    arguments_by_name[argument] = value
    with pytest.raises(HttpsCandidateError, match="^https_candidate_failed$"):
        await run_https_candidate(**arguments_by_name)
    assert not coordinator.sessions
    coordinator.acquire.assert_not_awaited()
    for operation in coordinator.mocks_by_name.values():
        operation.assert_not_awaited()


@pytest.mark.asyncio
async def test_content_budget_is_aggregate_before_acquisition(coordinator):
    request = coordinator.request
    largest_object = max(item.expected_bytes for item in request.objects)
    request = replace(request, limits=replace(request.limits, maximum_compressed_bytes=largest_object))
    with pytest.raises(HttpsCandidateError):
        await run_https_candidate(coordinator.session_factory, request)
    assert not coordinator.sessions
    coordinator.acquire.assert_not_awaited()


@pytest.mark.asyncio
async def test_durable_replay_maps_aliases_and_preserves_nullable_values():
    request, payloads = _request()
    retained = _retained(request, payloads)
    records = await https_candidate._replay(request, retained[::-1])
    assert records["providers"] == ({"npi": "1234567893", "display_name": "Synthetic Provider"},)
    assert records["rates"] == ({"rate_npi": "1234567893", "service_code": "A100", "amount": None},)
    admitted = assemble_root_families(request.definition, records["providers"], {"rates": records["rates"]})
    assert len(admitted.families) == 1
    assert not admitted.rejections
    assert not admitted.candidate_errors


@pytest.mark.asyncio
async def test_missing_nullable_field_retains_existing_family_admission_semantics():
    request, payloads = _request(
        tables_by_stream={
            "providers": pa.table({"Provider ID": ["1234567893"], "Provider Name": ["Example"]}),
            "rates": pa.table({"Provider ID": ["1234567893"], "Service Code": ["A100"]}),
        }
    )
    records = await https_candidate._replay(request, _retained(request, payloads))
    admitted = assemble_root_families(request.definition, records["providers"], {"rates": records["rates"]})
    assert not admitted.rejections
    assert "amount" not in admitted.families[0].children["rates"][0]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tamper", ["payload", "seal", "receipt", "contract", "token", "size", "scope", "duplicate", "parts"]
)
async def test_replay_rejects_retained_or_identity_drift(tamper):
    request, payloads = _request()
    retained_captures = list(_retained(request, payloads))
    first = retained_captures[0]
    match tamper:
        case "payload":
            retained_captures[0] = replace(first, parts=(first.parts[0][:-1] + b"X",))
        case "seal" | "contract" | "token":
            document = json.loads(first.receipt.canonical_manifest)
            if tamper == "seal":
                document["capture"]["capture_sha256"] = "0" * 64
            elif tamper == "contract":
                document["contract"] = "invalid"
            else:
                document["capture"]["source_snapshot_token"] = "other-snapshot"
            retained_captures[0] = replace(
                first, receipt=replace(first.receipt, canonical_manifest=canonical_json(document))
            )
        case "receipt":
            retained_captures[0] = replace(first, receipt=replace(first.receipt, content_sha256="0" * 64))
        case "size":
            request = replace(request, objects=(replace(request.objects[0], expected_bytes=10), request.objects[1]))
        case "scope":
            retained_captures.pop()
        case "duplicate":
            retained_captures.append(first)
        case _:
            midpoint = len(first.parts[0]) // 2
            retained_captures[0] = replace(first, parts=(first.parts[0][:midpoint], first.parts[0][midpoint:]))
    with pytest.raises(ValueError):
        await https_candidate._replay(request, tuple(retained_captures))


@pytest.mark.asyncio
async def test_alias_collision_rejects_without_overwriting_a_source_value():
    request, payloads = _request(
        tables_by_stream={
            "providers": pa.table({"Provider ID": ["1234567893"], "npi": ["1234567893"], "Provider Name": ["Example"]}),
            "rates": pa.table({"Provider ID": ["1234567893"], "Service Code": ["A100"]}),
        }
    )
    with pytest.raises(HttpsCandidateError, match="https_source_alias_collision"):
        await https_candidate._replay(request, _retained(request, payloads))


@pytest.mark.asyncio
async def test_global_record_budget_rejects_before_decoding_later_stream(monkeypatch):
    request, payloads = _request(limits=CaptureLimits(maximum_records=1))
    decoded_streams = []
    original_decoder = https_candidate.capture._iter_verified_records

    def decode(sealed, stream, **kwargs):
        decoded_streams.append(stream.stream_id)
        return original_decoder(sealed, stream, **kwargs)

    monkeypatch.setattr(https_candidate.capture, "_iter_verified_records", decode)
    with pytest.raises(ValueError, match="https_replay_budget_exceeded"):
        await https_candidate._replay(request, _retained(request, payloads))
    assert decoded_streams == ["providers"]


@pytest.mark.asyncio
async def test_replay_enforces_aggregate_payload_budget_before_later_stream_decoding(monkeypatch):
    request, payloads = _request(
        tables_by_stream={
            "providers": pa.table(
                {"Provider ID": ["1234567893"], "Provider Name": ["Example"]}
            ).replace_schema_metadata({b"synthetic-note": b"x" * 65_536}),
            "rates": pa.table({"Provider ID": ["1234567893"], "Service Code": ["A100"]}).replace_schema_metadata(
                {b"synthetic-note": b"x" * 65_536}
            ),
        }
    )
    retained = _retained(request, payloads)
    largest_object = max(map(len, payloads.values()))
    assert largest_object < sum(map(len, payloads.values()))
    # Exercise replay's own accounting independently of the stricter entrypoint envelope check.
    request = replace(
        request,
        limits=replace(request.limits, maximum_decoded_bytes=largest_object, maximum_record_bytes=largest_object),
    )
    decoded_streams = []
    original_decoder = https_candidate.capture._iter_verified_records

    def decode(sealed, stream, **kwargs):
        decoded_streams.append(stream.stream_id)
        return original_decoder(sealed, stream, **kwargs)

    monkeypatch.setattr(https_candidate.capture, "_iter_verified_records", decode)
    with pytest.raises(HttpsCandidateError, match="^https_replay_budget_exceeded$"):
        await https_candidate._replay(request, retained)
    assert decoded_streams == ["providers"]


@pytest.mark.asyncio
@pytest.mark.parametrize("batch_bytes", [False, 0.5, -1])
async def test_malformed_arrow_accounting_closes_reader_and_preserves_offline_resume(
    coordinator, monkeypatch, batch_bytes
):
    readers, closed_readers = [], []
    original_close = pq.ParquetFile.close

    def malformed_batches(reader, **options):
        assert options == {
            "batch_size": https_candidate.capture._PARQUET_BATCH_ROWS,
            "use_threads": False,
            "use_pandas_metadata": False,
        }
        readers.append(reader)
        yield SimpleNamespace(nbytes=batch_bytes)

    def close(reader, *args, **kwargs):
        original_close(reader, *args, **kwargs)
        closed_readers.append(reader)

    with monkeypatch.context() as decoder_patch:
        decoder_patch.setattr(pq.ParquetFile, "iter_batches", malformed_batches)
        decoder_patch.setattr(pq.ParquetFile, "close", close)
        with pytest.raises(HttpsCandidateError, match="^https_candidate_failed$"):
            await run_https_candidate(coordinator.session_factory, coordinator.request, resume=True)
    assert len(readers) == 1 and closed_readers == readers
    assert all(not session.is_active for session in coordinator.sessions)
    coordinator.mocks_by_name["run_claimed_candidate"].assert_not_awaited()
    coordinator.mocks_by_name["finish_execution"].assert_not_awaited()
    recovered = await run_https_candidate(coordinator.session_factory, coordinator.request, resume=True)
    assert recovered.status == "no_change" and recovered.execution_id == 17
    coordinator.acquire.assert_not_awaited()
    coordinator.mocks_by_name["register_replayable_parquet_bundle"].assert_not_awaited()
    coordinator.mocks_by_name["bind_execution_capture_bundle"].assert_not_awaited()


@pytest.mark.asyncio
async def test_global_arrow_budget_is_not_sealed_payload_length(monkeypatch):
    text = "Synthetic value " * 800
    request, payloads = _request(
        tables_by_stream={
            "providers": pa.table({"Provider ID": ["1234567893"] * 32, "Provider Name": [text] * 32}),
            "rates": pa.table({"Provider ID": ["1234567893"] * 32, "Service Code": [text] * 32}),
        },
        limits=CaptureLimits(maximum_decoded_bytes=600_000, maximum_record_bytes=20_000),
    )
    assert sum(map(len, payloads.values())) < request.limits.maximum_decoded_bytes
    decoded_streams = []
    original_decoder = https_candidate.capture._iter_verified_records

    def decode(sealed, stream, **kwargs):
        decoded_streams.append(stream.stream_id)
        return original_decoder(sealed, stream, **kwargs)

    monkeypatch.setattr(https_candidate.capture, "_iter_verified_records", decode)
    with pytest.raises(ValueError, match="https_replay_budget_exceeded"):
        await https_candidate._replay(request, _retained(request, payloads))
    assert decoded_streams == ["providers"]


@pytest.mark.asyncio
async def test_dictionary_expansion_accounting_never_requests_an_unguarded_batch(monkeypatch):
    text = "Synthetic value " * 1_024
    request, payloads = _request(
        tables_by_stream={
            "providers": pa.table({"Provider ID": ["1234567893"] * 1_024, "Provider Name": [text] * 1_024}),
            "rates": pa.table({"Provider ID": ["1234567893"], "Service Code": ["A100"]}),
        },
        limits=CaptureLimits(maximum_decoded_bytes=300_000, maximum_record_bytes=20_000),
    )
    native_batch_sizes = []
    original_batches = pq.ParquetFile.iter_batches

    def guarded_batches(reader, *args, **kwargs):
        native_batch_sizes.append(kwargs["batch_size"])
        assert kwargs["batch_size"] == https_candidate.capture._PARQUET_BATCH_ROWS == 1
        yield from original_batches(reader, *args, **kwargs)

    monkeypatch.setattr(pq.ParquetFile, "iter_batches", guarded_batches)
    with pytest.raises(ValueError, match="https_replay_budget_exceeded"):
        await https_candidate._replay(request, _retained(request, payloads))
    assert native_batch_sizes == [1]


@pytest.mark.asyncio
async def test_null_only_arrow_output_still_consumes_retained_record_content_budget():
    table = pa.table({label: pa.nulls(15_000) for label in ("Provider ID", "Provider Name", "extra_a", "extra_b")})
    assert table.nbytes == 0
    request, payloads = _request(
        tables_by_stream={
            "providers": table,
            "rates": pa.table({"Provider ID": ["1234567893"], "Service Code": ["A100"]}),
        },
        limits=CaptureLimits(maximum_decoded_bytes=200_000, maximum_record_bytes=1_000),
    )
    with pytest.raises(HttpsCandidateError, match="https_replay_budget_exceeded"):
        await https_candidate._replay(request, _retained(request, payloads))


@pytest.mark.asyncio
async def test_decoder_close_failure_is_not_silently_discarded(monkeypatch):
    request, payloads = _request()
    original_decoder = https_candidate.capture._iter_verified_records
    closed_streams = []

    def broken_close(sealed, stream, **kwargs):
        try:
            yield from original_decoder(sealed, stream, **kwargs)
        finally:
            closed_streams.append(stream.stream_id)
            raise RuntimeError("decoder cleanup failed")

    monkeypatch.setattr(https_candidate.capture, "_iter_verified_records", broken_close)
    with pytest.raises(RuntimeError, match="decoder cleanup failed"):
        await https_candidate._replay(request, _retained(request, payloads))
    assert closed_streams == ["providers"]


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout_seconds", (5, 3599.5, 3600))
async def test_execute_binds_complete_bundle_atomically_and_reloads_in_new_session(coordinator, timeout_seconds):
    request = replace(coordinator.request, timeout_seconds=timeout_seconds)
    result = await run_https_candidate(coordinator.session_factory, request)
    assert result.status == "no_change"
    assert coordinator.acquire.await_count == 2
    mocks = coordinator.mocks_by_name
    registered_session = mocks["register_replayable_parquet_bundle"].call_args.args[0]
    bound_session = mocks["bind_execution_capture_bundle"].call_args.args[0]
    loaded_session = mocks["load_replayable_parquet_bundle"].call_args.args[0]
    assert registered_session is bound_session
    assert loaded_session is not bound_session
    assert registered_session.outcome == loaded_session.outcome == "commit"
    assert {
        part.receipt.stream_id for part in mocks["register_replayable_parquet_bundle"].call_args.kwargs["captures"]
    } == {"providers", "rates"}
    bind_arguments = mocks["bind_execution_capture_bundle"].call_args.kwargs
    assert bind_arguments["fence"] == 1
    assert bind_arguments["token"] == coordinator.request.lease_token
    candidate = mocks["run_claimed_candidate"].call_args.args[1]
    admitted = mocks["run_claimed_candidate"].call_args.args[3]
    assert candidate.execution_id == 17
    assert candidate.complete_scope is True
    assert candidate.children_by_collection["rates"][0]["amount"] is None
    assert len(admitted.families) == 1
    identity = mocks["reserve_execution"].call_args.kwargs["request_identity_sha256"]
    assert identity == https_request_identity_sha256(request)


@pytest.mark.asyncio
async def test_expired_bound_resume_uses_exact_lookup_and_never_acquires(coordinator):
    request = replace(coordinator.request, lease_token="replacement-owner")
    result = await run_https_candidate(coordinator.session_factory, request, resume=True)
    assert result.status == "no_change"
    mocks = coordinator.mocks_by_name
    mocks["reserve_execution"].assert_not_awaited()
    mocks["claim_execution"].assert_not_awaited()
    mocks["resume_execution"].assert_awaited_once()
    mocks["register_replayable_parquet_bundle"].assert_not_awaited()
    mocks["bind_execution_capture_bundle"].assert_not_awaited()
    coordinator.acquire.assert_not_awaited()
    arguments = mocks["lookup_execution_request"].call_args.kwargs
    assert arguments["request_identity_sha256"] == https_request_identity_sha256(request)
    assert arguments["idempotency_key"] == request.idempotency_key
    assert mocks["load_replayable_parquet_bundle"].call_args.kwargs["capture_bundle_id"] == 23
    assert mocks["run_claimed_candidate"].call_args.args[2].fence == 2


@pytest.mark.asyncio
async def test_execute_reentry_on_bound_execution_never_refetches(coordinator):
    mocks = coordinator.mocks_by_name
    mocks["reserve_execution"].return_value = ExecutionSubmission(17, "running", False, 23)
    mocks["claim_execution"].return_value = replace(coordinator.grant, fence=2)
    result = await run_https_candidate(coordinator.session_factory, coordinator.request)
    assert result.status == "no_change"
    coordinator.acquire.assert_not_awaited()
    mocks["register_replayable_parquet_bundle"].assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "submission",
    [
        None,
        ExecutionSubmission(17, "queued", False, 23),
        ExecutionSubmission(17, "running", False),
        ExecutionSubmission(17, "failed", False, 23),
    ],
)
async def test_resume_requires_an_existing_active_bound_execution(coordinator, submission):
    coordinator.mocks_by_name["lookup_execution_request"].return_value = submission
    with pytest.raises(HttpsCandidateError):
        await run_https_candidate(coordinator.session_factory, coordinator.request, resume=True)
    coordinator.acquire.assert_not_awaited()
    coordinator.mocks_by_name["resume_execution"].assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("resume", [False, True])
async def test_live_or_unclaimable_lease_does_no_work(coordinator, resume):
    coordinator.mocks_by_name["resume_execution" if resume else "claim_execution"].return_value = None
    result = await run_https_candidate(coordinator.session_factory, coordinator.request, resume=resume)
    assert result.status == "not_claimed"
    coordinator.acquire.assert_not_awaited()
    coordinator.mocks_by_name["load_replayable_parquet_bundle"].assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("resume", [False, True])
async def test_already_canceling_claim_is_finished_before_preparation(coordinator, resume):
    mocks = coordinator.mocks_by_name
    mocks["lookup_execution_request" if resume else "reserve_execution"].return_value = ExecutionSubmission(
        17, "canceling", False, 23
    )
    grant = replace(coordinator.grant, state="canceling", fence=2 if resume else 1)
    mocks["resume_execution" if resume else "claim_execution"].return_value = grant
    mocks["finish_execution"].return_value = ExecutionTransition(17, "canceled", True)
    result = await run_https_candidate(coordinator.session_factory, coordinator.request, resume=resume)
    assert result == CandidateRunResult(status="canceled", execution_id=17)
    mocks["finish_execution"].assert_awaited_once_with(
        coordinator.sessions[-1],
        execution_id=17,
        fence=grant.fence,
        token=coordinator.request.lease_token,
        terminal_state="canceled",
        terminal_reason=None,
    )
    assert len(coordinator.sessions) == 2
    assert all(session.outcome == "commit" and not session.is_active for session in coordinator.sessions)
    coordinator.acquire.assert_not_awaited()
    for operation in (
        "heartbeat_execution",
        "load_replayable_parquet_bundle",
        "register_replayable_parquet_bundle",
        "bind_execution_capture_bundle",
        "run_claimed_candidate",
    ):
        mocks[operation].assert_not_awaited()


@pytest.mark.asyncio
async def test_registry_mismatch_is_checked_before_source_acquisition(coordinator):
    coordinator.mocks_by_name["load_registry"].side_effect = ValueError("registry mismatch")
    with pytest.raises(HttpsCandidateError):
        await run_https_candidate(coordinator.session_factory, coordinator.request)
    coordinator.acquire.assert_not_awaited()
    coordinator.mocks_by_name["reserve_execution"].assert_not_awaited()


@pytest.mark.asyncio
async def test_idempotency_identity_conflict_is_rejected_before_resume_or_refetch(coordinator):
    coordinator.mocks_by_name["lookup_execution_request"].side_effect = ValueError("redaction-marker")
    with pytest.raises(HttpsCandidateError) as error:
        await run_https_candidate(coordinator.session_factory, coordinator.request, resume=True)
    assert "redaction-marker" not in "".join(traceback.format_exception(error.value))
    coordinator.acquire.assert_not_awaited()
    coordinator.mocks_by_name["resume_execution"].assert_not_awaited()


@pytest.mark.asyncio
async def test_stale_bind_rolls_back_registration_and_never_runs_candidate(coordinator):
    coordinator.mocks_by_name["bind_execution_capture_bundle"].return_value = None
    result = await run_https_candidate(coordinator.session_factory, coordinator.request)
    assert result.status == "lease_lost"
    registered_session = coordinator.mocks_by_name["register_replayable_parquet_bundle"].call_args.args[0]
    assert registered_session.outcome == "rollback"
    coordinator.mocks_by_name["load_replayable_parquet_bundle"].assert_not_awaited()
    coordinator.mocks_by_name["run_claimed_candidate"].assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_acquisition_then_expired_unbound_retry_never_refetches(coordinator):
    coordinator.acquire.side_effect = RuntimeError("https://downloads.example/?sample=redaction-marker")
    with pytest.raises(HttpsCandidateError) as error:
        await run_https_candidate(coordinator.session_factory, coordinator.request)
    assert "redaction-marker" not in "".join(traceback.format_exception(error.value))
    assert coordinator.acquire.await_count == 1
    mocks = coordinator.mocks_by_name
    assert mocks["finish_execution"].call_args.kwargs["terminal_state"] == "failed"
    mocks["reserve_execution"].return_value = ExecutionSubmission(17, "running", False)
    mocks["claim_execution"].return_value = replace(coordinator.grant, fence=2)
    with pytest.raises(HttpsCandidateError):
        await run_https_candidate(coordinator.session_factory, coordinator.request)
    assert coordinator.acquire.await_count == 1
    mocks["register_replayable_parquet_bundle"].assert_not_awaited()


@pytest.mark.asyncio
async def test_registration_failure_rolls_back_and_unbound_takeover_never_refetches(coordinator):
    mocks = coordinator.mocks_by_name
    mocks["register_replayable_parquet_bundle"].side_effect = ValueError("registration unavailable")
    with pytest.raises(HttpsCandidateError):
        await run_https_candidate(coordinator.session_factory, coordinator.request)
    registered_session = mocks["register_replayable_parquet_bundle"].call_args.args[0]
    assert registered_session.outcome == "rollback"
    assert coordinator.acquire.await_count == 2
    mocks["reserve_execution"].return_value = ExecutionSubmission(17, "running", False)
    mocks["claim_execution"].return_value = replace(coordinator.grant, fence=2)
    with pytest.raises(HttpsCandidateError):
        await run_https_candidate(coordinator.session_factory, coordinator.request)
    assert coordinator.acquire.await_count == 2
    mocks["bind_execution_capture_bundle"].assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["token", "bytes", "stream"])
async def test_acquired_capture_must_match_exact_envelope_before_registration(coordinator, drift):
    request = coordinator.request
    first_stream = request.definition.source_streams[0]
    if drift == "stream":
        first_stream = replace(first_stream, stream_id="unexpected")
    sealed = capture_stream(
        BytesIO(coordinator.payloads_by_stream["providers"] + (b"X" if drift == "bytes" else b"")),
        first_stream,
        source_snapshot_token="another-snapshot" if drift == "token" else request.source_snapshot_token,
    )
    coordinator.acquire.side_effect = None
    coordinator.acquire.return_value = sealed
    with pytest.raises(HttpsCandidateError):
        await run_https_candidate(coordinator.session_factory, request)
    coordinator.mocks_by_name["register_replayable_parquet_bundle"].assert_not_awaited()
    assert coordinator.mocks_by_name["finish_execution"].call_args.kwargs["terminal_state"] == "failed"


@pytest.mark.asyncio
async def test_lost_lease_after_acquisition_never_registers_partial_bundle(coordinator):
    coordinator.mocks_by_name["heartbeat_execution"].side_effect = [coordinator.grant, None]
    result = await run_https_candidate(coordinator.session_factory, coordinator.request)
    assert result.status == "lease_lost"
    assert coordinator.acquire.await_count == 1
    coordinator.mocks_by_name["register_replayable_parquet_bundle"].assert_not_awaited()


@pytest.mark.asyncio
async def test_cancellation_after_acquisition_is_fenced_without_registering(coordinator):
    coordinator.mocks_by_name["heartbeat_execution"].side_effect = [
        coordinator.grant,
        replace(coordinator.grant, state="canceling"),
    ]
    coordinator.mocks_by_name["finish_execution"].return_value = ExecutionTransition(17, "canceled", True)
    result = await run_https_candidate(coordinator.session_factory, coordinator.request)
    assert result.status == "canceled"
    coordinator.mocks_by_name["register_replayable_parquet_bundle"].assert_not_awaited()
    assert coordinator.mocks_by_name["finish_execution"].call_args.kwargs["terminal_state"] == "canceled"


@pytest.mark.asyncio
async def test_bound_readback_failure_preserves_offline_retry_without_refetch(coordinator):
    mocks = coordinator.mocks_by_name
    valid_captures = mocks["load_replayable_parquet_bundle"].return_value
    mocks["load_replayable_parquet_bundle"].return_value = valid_captures[:1]
    with pytest.raises(HttpsCandidateError):
        await run_https_candidate(coordinator.session_factory, coordinator.request)
    mocks["finish_execution"].assert_not_awaited()
    mocks["load_replayable_parquet_bundle"].return_value = valid_captures
    result = await run_https_candidate(coordinator.session_factory, coordinator.request, resume=True)
    assert result.status == "no_change"
    assert coordinator.acquire.await_count == 2


@pytest.mark.asyncio
async def test_worker_cancellation_does_not_refetch_unbound_execution(coordinator):
    coordinator.acquire.side_effect = asyncio.CancelledError("redaction-marker")
    with pytest.raises(asyncio.CancelledError) as error:
        await run_https_candidate(coordinator.session_factory, coordinator.request)
    assert "redaction-marker" not in "".join(traceback.format_exception(error.value))
    coordinator.mocks_by_name["reserve_execution"].return_value = ExecutionSubmission(17, "running", False)
    coordinator.mocks_by_name["claim_execution"].return_value = replace(coordinator.grant, fence=2)
    with pytest.raises(HttpsCandidateError):
        await run_https_candidate(coordinator.session_factory, coordinator.request)
    assert coordinator.acquire.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["accounting", "decoder"])
async def test_bound_replay_cancellation_closes_owned_reader_and_resumes_offline(coordinator, monkeypatch, stage):
    """Cancellation while a native reader is owned retains only the durable bundle."""
    started = asyncio.Event()
    ownership = SimpleNamespace(phase="accounting", reader=None, is_closed=False)
    original_batches = pq.ParquetFile.iter_batches
    original_close = pq.ParquetFile.close
    original_decoder = https_candidate.capture._iter_verified_records

    def observed_batches(reader, *args, **kwargs):
        for batch in original_batches(reader, *args, **kwargs):
            if ownership.phase == stage and ownership.reader is None:
                ownership.reader = reader
                started.set()
            yield batch

    def observed_close(reader, *args, **kwargs):
        original_close(reader, *args, **kwargs)
        if reader is ownership.reader:
            ownership.is_closed = True

    def observed_decoder(*args, **kwargs):
        ownership.phase = "decoder"
        yield from original_decoder(*args, **kwargs)

    monkeypatch.setattr(pq.ParquetFile, "iter_batches", observed_batches)
    monkeypatch.setattr(pq.ParquetFile, "close", observed_close)
    monkeypatch.setattr(https_candidate.capture, "_iter_verified_records", observed_decoder)
    worker = asyncio.create_task(run_https_candidate(coordinator.session_factory, coordinator.request))
    try:
        await asyncio.wait_for(started.wait(), timeout=2)
        coordinator.mocks_by_name["bind_execution_capture_bundle"].assert_awaited_once()
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker
    finally:
        if not worker.done():
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
    assert ownership.reader is not None and ownership.is_closed
    coordinator.mocks_by_name["run_claimed_candidate"].assert_not_awaited()
    coordinator.mocks_by_name["finish_execution"].assert_not_awaited()
    coordinator.acquire.side_effect = AssertionError("offline resume must not fetch")
    recovered = await run_https_candidate(
        coordinator.session_factory, replace(coordinator.request, lease_token="replacement-owner"), resume=True
    )
    assert recovered.status == "no_change" and recovered.execution_id == 17
    assert coordinator.acquire.await_count == 2
    coordinator.mocks_by_name["register_replayable_parquet_bundle"].assert_awaited_once()


@pytest.mark.asyncio
async def test_one_deadline_spans_all_source_objects(coordinator):
    request = replace(coordinator.request, timeout_seconds=0.05)
    retained = _retained(request, coordinator.payloads_by_stream)
    coordinator.mocks_by_name["load_replayable_parquet_bundle"].return_value = retained
    original_acquire = coordinator.acquire.side_effect

    async def delayed_acquire(*args, **kwargs):
        await asyncio.sleep(0.03)
        return await original_acquire(*args, **kwargs)

    coordinator.acquire.side_effect = delayed_acquire
    with pytest.raises(HttpsCandidateError):
        await run_https_candidate(coordinator.session_factory, request)
    assert coordinator.acquire.await_count == 2
    first_timeout = coordinator.acquire.call_args_list[0].kwargs["timeout_seconds"]
    second_timeout = coordinator.acquire.call_args_list[1].kwargs["timeout_seconds"]
    assert 0 < second_timeout < first_timeout <= request.timeout_seconds
    coordinator.mocks_by_name["register_replayable_parquet_bundle"].assert_not_awaited()
    finish = coordinator.mocks_by_name["finish_execution"]
    finish.assert_awaited_once()
    assert finish.call_args.kwargs["terminal_state"] == "failed"
    assert finish.call_args.args[0].outcome == "commit"


@pytest.mark.asyncio
async def test_durable_readback_does_not_reset_acquisition_deadline(coordinator):
    request = replace(coordinator.request, timeout_seconds=0.2)
    retained = _retained(request, coordinator.payloads_by_stream)
    original_acquire = coordinator.acquire.side_effect

    async def delayed_acquire(*args, **kwargs):
        await asyncio.sleep(0.03)
        return await original_acquire(*args, **kwargs)

    async def delayed_load(*_args, **_kwargs):
        await asyncio.sleep(0.16)
        return retained

    coordinator.acquire.side_effect = delayed_acquire
    coordinator.mocks_by_name["load_replayable_parquet_bundle"].side_effect = delayed_load
    with pytest.raises(HttpsCandidateError):
        await run_https_candidate(coordinator.session_factory, request)
    coordinator.mocks_by_name["bind_execution_capture_bundle"].assert_awaited_once()
    coordinator.mocks_by_name["run_claimed_candidate"].assert_not_awaited()
    coordinator.mocks_by_name["finish_execution"].assert_not_awaited()
    load = coordinator.mocks_by_name["load_replayable_parquet_bundle"]
    assert load.call_args.args[0].outcome == "rollback"
    load.side_effect = None
    load.return_value = retained
    coordinator.acquire.side_effect = AssertionError("offline resume must not fetch")
    recovered = await run_https_candidate(coordinator.session_factory, request, resume=True)
    assert recovered.status == "no_change"
    assert coordinator.acquire.await_count == 2
    coordinator.mocks_by_name["register_replayable_parquet_bundle"].assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["replay", "admission", "heartbeat"])
async def test_synchronous_deadline_expiry_cannot_escape_to_claimed_runner(coordinator, monkeypatch, stage):
    loop = asyncio.get_running_loop()
    original_time = loop.time
    original_admission = https_candidate.assemble_root_families

    def expire_deadline():
        monkeypatch.setattr(loop, "time", lambda: original_time() + 10)

    if stage == "replay":

        async def expired_replay(*_args, **_kwargs):
            expire_deadline()
            return {"providers": (), "rates": ()}

        monkeypatch.setattr(https_candidate, "_load_replayed_records", expired_replay)
    elif stage == "admission":

        def expired_admission(*args, **kwargs):
            admitted = original_admission(*args, **kwargs)
            expire_deadline()
            return admitted

        monkeypatch.setattr(https_candidate, "assemble_root_families", expired_admission)
    else:

        async def expired_heartbeat(*_args, **_kwargs):
            expire_deadline()
            return coordinator.grant

        monkeypatch.setattr(https_candidate, "_heartbeat", expired_heartbeat)
        coordinator.mocks_by_name["reserve_execution"].return_value = ExecutionSubmission(17, "running", False, 23)
    try:
        with pytest.raises(HttpsCandidateError):
            await run_https_candidate(coordinator.session_factory, coordinator.request)
    finally:
        monkeypatch.setattr(loop, "time", original_time)
    coordinator.mocks_by_name["run_claimed_candidate"].assert_not_awaited()
