# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded standalone HTTPS Parquet acquisition and exact durable replay."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
from contextlib import asynccontextmanager, closing
from dataclasses import asdict, dataclass, field, replace

from process.custom_import import capture, execution, https, runner
from process.custom_import._source_text import validate_snapshot_token
from process.custom_import.capture import CaptureManifest, SealedCapture
from process.custom_import.capture_limits import CaptureLimits
from process.custom_import.capture_store import (
    _MAX_PARQUET_BYTES_PER_BUNDLE,
    _MAX_PARQUET_PART_BYTES,
    CaptureReceipt,
    ReplayableParquetCapture,
    load_replayable_parquet_bundle,
    register_replayable_parquet_bundle,
)
from process.custom_import.definition import CustomImportDefinition, canonical_json
from process.custom_import.family import assemble_root_families, validate_source_snapshot_tokens
from process.custom_import.runner_registry import load_registry
from process.custom_import.runner_types import (
    CancellationRequested,
    CandidateRunRequest,
    CandidateRunResult,
    LeaseAuthorityLost,
    SessionFactory,
)

_CONTRACT = "custom-import/https-capture/v1"


class HttpsCandidateError(ValueError):
    """A redacted standalone capture or replay failure."""


@dataclass(frozen=True)
class HttpsObject:
    """One complete expected object for a declared source stream."""

    stream_id: str
    url: str = field(repr=False)
    expected_sha256: str
    expected_bytes: int


@dataclass(frozen=True)
class HttpsCandidateRequest:
    """An exact local request; limits apply across the complete stream bundle.

    Limits may tighten the existing capture defaults. The shared snapshot token
    asserts upstream provenance; HTTPS content pins do not prove upstream atomicity.
    """

    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    definition: CustomImportDefinition = field(repr=False)
    objects: tuple[HttpsObject, ...]
    source_snapshot_token: str = field(repr=False)
    idempotency_key: str = field(repr=False)
    lease_token: str | bytes = field(repr=False)
    limits: CaptureLimits
    timeout_seconds: float
    complete_scope: bool


def _candidate_request(request, execution_id, records_by_stream=None):
    records_by_stream = {} if records_by_stream is None else records_by_stream
    root = next(stream for stream in request.definition.source_streams if stream.record_kind == "root")
    return CandidateRunRequest(
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        execution_id=execution_id,
        lease_token=request.lease_token,
        definition=request.definition,
        roots=records_by_stream.get(root.stream_id, ()),
        children_by_collection={
            stream.child_collection: records_by_stream.get(stream.stream_id, ())
            for stream in request.definition.source_streams
            if stream.record_kind == "child"
        },
        complete_scope=request.complete_scope,
    )


def _checked_request(request):
    if not isinstance(request, HttpsCandidateRequest) or request.complete_scope is not True:
        raise HttpsCandidateError("https_request_invalid")
    if not isinstance(request.limits, CaptureLimits):
        raise HttpsCandidateError("https_request_invalid")
    limits = CaptureLimits(**asdict(request.limits))
    if any(limit_value > getattr(CaptureLimits(), name) for name, limit_value in asdict(limits).items()):
        raise HttpsCandidateError("https_request_invalid")
    if (
        isinstance(request.timeout_seconds, bool)
        or not isinstance(request.timeout_seconds, (int, float))
        or not math.isfinite(request.timeout_seconds)
        or request.timeout_seconds <= 0
        or request.timeout_seconds > execution.MAX_LEASE_SECONDS
        or not isinstance(request.objects, tuple)
        or not all(isinstance(source_object, HttpsObject) for source_object in request.objects)
    ):
        raise HttpsCandidateError("https_request_invalid")
    runner.validate_candidate_request(lambda: None, _candidate_request(request, 1))
    validate_snapshot_token(request.source_snapshot_token)
    objects_by_stream = {source_object.stream_id: source_object for source_object in request.objects}
    streams = request.definition.source_streams
    if len(objects_by_stream) != len(request.objects) or set(objects_by_stream) != {
        stream.stream_id for stream in streams
    }:
        raise HttpsCandidateError("https_request_invalid")
    for stream in streams:
        if stream.format != "parquet" or stream.compression != "none":
            raise HttpsCandidateError("https_request_invalid")
        source_object = objects_by_stream[stream.stream_id]
        https._https_url(source_object.url)
        https._validate_identity(
            source_object.expected_sha256, source_object.expected_bytes, limits, request.timeout_seconds
        )
        if not 1 <= source_object.expected_bytes <= _MAX_PARQUET_PART_BYTES:
            raise HttpsCandidateError("https_request_invalid")
    if sum(source_object.expected_bytes for source_object in request.objects) > min(
        limits.maximum_compressed_bytes, limits.maximum_decoded_bytes, _MAX_PARQUET_BYTES_PER_BUNDLE
    ):
        raise HttpsCandidateError("https_request_invalid")
    return replace(request, objects=tuple(objects_by_stream[stream.stream_id] for stream in streams), limits=limits)


def https_request_identity_sha256(request: HttpsCandidateRequest) -> bytes:
    """Bind source content, definition, full scope and limits without retaining URLs."""
    try:
        request = _checked_request(request)
        document_by_key = {
            "contract": _CONTRACT,
            "dataset_id": request.dataset_id,
            "definition_revision_id": request.definition_revision_id,
            "schema_revision_id": request.schema_revision_id,
            "definition_sha256": request.definition.digest,
            "source_snapshot_token": request.source_snapshot_token,
            "complete_scope": request.complete_scope,
            "limits": asdict(request.limits),
            "timeout_seconds": request.timeout_seconds,
            "objects": [
                {
                    "stream_id": item.stream_id,
                    "url_sha256": hashlib.sha256(item.url.encode("utf-8")).hexdigest(),
                    "expected_sha256": item.expected_sha256,
                    "expected_bytes": item.expected_bytes,
                }
                for item in request.objects
            ],
        }
        encoded = json.dumps(document_by_key, allow_nan=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(_CONTRACT.encode("ascii") + b"\0request\0" + encoded).digest()
    except Exception:
        raise HttpsCandidateError("https_request_invalid") from None


def _check_capture(request, item, stream, sealed):
    capture.verify_capture(sealed, stream, limits=request.limits)
    manifest = sealed.manifest
    if (
        manifest.source_snapshot_token != request.source_snapshot_token
        or manifest.compressed_bytes != item.expected_bytes
        or manifest.compressed_sha256 != item.expected_sha256
    ):
        raise HttpsCandidateError("https_capture_mismatch")


def _retained_capture(request, item, stream, sealed):
    _check_capture(request, item, stream, sealed)
    manifest = sealed.manifest
    return ReplayableParquetCapture(
        receipt=CaptureReceipt(
            stream_id=stream.stream_id,
            source_snapshot_token=request.source_snapshot_token,
            byte_count=manifest.compressed_bytes,
            content_sha256=manifest.compressed_sha256,
            canonical_manifest=canonical_json({"contract": _CONTRACT, "capture": asdict(manifest)}),
            manifest_sha256=manifest.capture_sha256,
        ),
        parts=(sealed.payload,),
    )


def _reconstructed_capture(request, item, stream, retained):
    document = json.loads(retained.receipt.canonical_manifest)
    if set(document) != {"contract", "capture"} or document["contract"] != _CONTRACT or len(retained.parts) != 1:
        raise HttpsCandidateError("https_capture_mismatch")
    manifest = CaptureManifest(**document["capture"])
    sealed = SealedCapture(payload=retained.parts[0], manifest=manifest)
    _check_capture(request, item, stream, sealed)
    if (
        retained.receipt.stream_id != manifest.stream_id
        or retained.receipt.source_snapshot_token != manifest.source_snapshot_token
        or retained.receipt.byte_count != manifest.compressed_bytes
        or retained.receipt.content_sha256 != manifest.compressed_sha256
        or retained.receipt.manifest_sha256 != manifest.capture_sha256
    ):
        raise HttpsCandidateError("https_capture_mismatch")
    return sealed


async def _heartbeat(session_factory, request, grant):
    async with session_factory() as session, session.begin():
        renewed = await execution.heartbeat_execution(
            session, execution_id=grant.execution_id, fence=grant.fence, token=request.lease_token
        )
    if renewed is None:
        raise LeaseAuthorityLost("https_lease_lost")
    if renewed.state != "running":
        raise CancellationRequested("https_canceled")
    return renewed


@asynccontextmanager
async def _renew_while_preparing(session_factory, request, grant):
    """Keep yielding acquisition and replay live; join before runner handoff."""

    async def renew():
        """Renew the exact fence in fresh transactions until preparation ends."""
        while True:
            await asyncio.sleep(min(5.0, execution.DEFAULT_LEASE_SECONDS / 3))
            await _heartbeat(session_factory, request, grant)

    try:
        async with asyncio.TaskGroup() as tasks:
            heartbeat = tasks.create_task(renew())
            try:
                yield
            finally:
                heartbeat.cancel()
    except BaseExceptionGroup as failure:
        if len(failure.exceptions) == 1:
            raise failure.exceptions[0] from None
        raise


async def _acquire(session_factory, request, grant, deadline):
    retained_captures = []
    async with asyncio.timeout_at(deadline):
        for item, stream in zip(request.objects, request.definition.source_streams, strict=True):
            await _heartbeat(session_factory, request, grant)
            sealed = await https.acquire_https(
                item.url,
                stream,
                source_snapshot_token=request.source_snapshot_token,
                expected_sha256=item.expected_sha256,
                expected_bytes=item.expected_bytes,
                limits=request.limits,
                timeout_seconds=_remaining_seconds(deadline),
            )
            retained_captures.append(_retained_capture(request, item, stream, sealed))
            await _heartbeat(session_factory, request, grant)
        _remaining_seconds(deadline)
    return tuple(retained_captures)


async def _reserve(session_factory, request, identity, resume):
    async with session_factory() as session, session.begin():
        await load_registry(session, _candidate_request(request, 1))
        submit = execution.lookup_execution_request if resume else execution.reserve_execution
        submission = await submit(
            session,
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
            idempotency_key=request.idempotency_key,
            mechanism="local",
            request_identity_sha256=identity,
        )
        if resume and (
            submission is None
            or submission.state not in {"running", "canceling"}
            or submission.capture_bundle_id is None
        ):
            raise HttpsCandidateError("https_resume_unavailable")
        claim = execution.resume_execution if resume else execution.claim_execution
        grant = await claim(session, execution_id=submission.execution_id, token=request.lease_token)
    return submission, grant


async def _store(session_factory, request, grant, captures):
    async with session_factory() as session, session.begin():
        registered = await register_replayable_parquet_bundle(
            session,
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
            captures=captures,
        )
        bound = await execution.bind_execution_capture_bundle(
            session,
            execution_id=grant.execution_id,
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
            capture_bundle_id=registered.capture_bundle_id,
            fence=grant.fence,
            token=request.lease_token,
        )
        if bound is None:
            raise LeaseAuthorityLost("https_lease_lost")
        return bound.capture_bundle_id


def _remaining_seconds(deadline):
    remaining = deadline - asyncio.get_running_loop().time()
    if remaining <= 0:
        raise TimeoutError
    return remaining


async def _bounded_arrow_bytes(sealed, limits, record_count, arrow_bytes, deadline):
    """Preflight native decoding before accounting the remaining bundle budgets."""
    capture._validate_parquet_envelope(sealed.payload)
    with capture._open_parquet_reader(sealed.payload, limits) as reader:
        schema = reader.schema_arrow
        labels = capture._validated_parquet_schema(schema, limits)
        expected_records = capture._validated_parquet_metadata(
            reader.metadata, expected_columns=len(labels), limits=limits
        )
        if record_count + expected_records > limits.maximum_records:
            raise capture.CaptureError("https_replay_budget_exceeded")
        capture._validate_parquet_page_preflight(
            sealed.payload, reader.metadata, schema, maximum_decoded_bytes=limits.maximum_decoded_bytes
        )
        # Page preflight bounds one-row output, not dictionary-expanded batches.
        for batch in reader.iter_batches(
            batch_size=capture._PARQUET_BATCH_ROWS, use_threads=False, use_pandas_metadata=False
        ):
            batch_bytes = batch.nbytes
            if isinstance(batch_bytes, bool) or not isinstance(batch_bytes, int) or batch_bytes < 0:
                raise capture.CaptureError("https_replay_budget_exceeded")
            arrow_bytes += batch_bytes
            if arrow_bytes > limits.maximum_decoded_bytes:
                raise capture.CaptureError("https_replay_budget_exceeded")
            await asyncio.sleep(0)
            _remaining_seconds(deadline)
    _remaining_seconds(deadline)
    return arrow_bytes


async def _replay(request, captures, *, deadline=None):
    deadline = asyncio.get_running_loop().time() + request.timeout_seconds if deadline is None else deadline
    by_stream = {retained.receipt.stream_id: retained for retained in captures}
    if len(by_stream) != len(captures) or set(by_stream) != {
        source_object.stream_id for source_object in request.objects
    }:
        raise HttpsCandidateError("https_capture_mismatch")
    records_by_stream, tokens = {}, {}
    decoded_bytes = record_count = arrow_bytes = record_bytes = 0
    for source_object, stream in zip(request.objects, request.definition.source_streams, strict=True):
        sealed = _reconstructed_capture(request, source_object, stream, by_stream[stream.stream_id])
        tokens[stream.stream_id] = [sealed.manifest.source_snapshot_token]
        decoded_bytes += sealed.manifest.decoded_bytes
        if decoded_bytes > request.limits.maximum_decoded_bytes:
            raise HttpsCandidateError("https_replay_budget_exceeded")
        arrow_bytes = await _bounded_arrow_bytes(sealed, request.limits, record_count, arrow_bytes, deadline)
        await asyncio.sleep(0)
        field_by_source_label = {
            alias.source_label: alias.field_id
            for alias in request.definition.aliases
            if alias.stream_id == stream.stream_id
        }
        stream_records = []
        with closing(capture._iter_verified_records(sealed, stream, limits=request.limits)) as decoded:
            for decoded_record in decoded:
                values_by_field = {
                    field_by_source_label.get(label, label): scalar_value
                    for label, scalar_value in decoded_record.values.items()
                }
                if len(values_by_field) != len(decoded_record.values):
                    raise HttpsCandidateError("https_source_alias_collision")
                record_count += 1
                record_bytes += capture._record_size(values_by_field)
                if record_count > request.limits.maximum_records or record_bytes > request.limits.maximum_decoded_bytes:
                    raise HttpsCandidateError("https_replay_budget_exceeded")
                stream_records.append(values_by_field)
                await asyncio.sleep(0)
                _remaining_seconds(deadline)
        records_by_stream[stream.stream_id] = tuple(stream_records)
    validate_source_snapshot_tokens(request.definition, tokens)
    return records_by_stream


async def _finish(session_factory, request, grant, *, canceled=False):
    async with session_factory() as session, session.begin():
        transition = await execution.finish_execution(
            session,
            execution_id=grant.execution_id,
            fence=grant.fence,
            token=request.lease_token,
            terminal_state="canceled" if canceled else "failed",
            terminal_reason=None if canceled else "source_capture_failed",
        )
    return CandidateRunResult(
        status="canceled" if transition.state == "canceled" else "lease_lost", execution_id=grant.execution_id
    )


async def _load_replayed_records(session_factory, request, grant, submission, deadline):
    """Bind a complete acquisition, then verify its fresh-session durable readback."""
    bundle_id = submission.capture_bundle_id
    captures = None
    if bundle_id is None:
        if submission.state != "queued" or grant.fence != 1:
            await _finish(session_factory, request, grant)
            raise HttpsCandidateError("https_unbound_capture")
        try:
            captures = await _acquire(session_factory, request, grant, deadline)
        except CancellationRequested, LeaseAuthorityLost:
            raise
        except Exception:
            await _finish(session_factory, request, grant)
            raise
    async with asyncio.timeout_at(deadline):
        if captures is not None:
            bundle_id = await _store(session_factory, request, grant, captures)
            del captures
        async with session_factory() as session, session.begin():
            captures = await load_replayable_parquet_bundle(
                session,
                capture_bundle_id=bundle_id,
                dataset_id=request.dataset_id,
                definition_revision_id=request.definition_revision_id,
                schema_revision_id=request.schema_revision_id,
            )
        return await _replay(request, captures, deadline=deadline)


async def _run(session_factory, request, identity, *, resume):
    submission, grant = await _reserve(session_factory, request, identity, resume)
    if grant is None:
        return CandidateRunResult(status="not_claimed", execution_id=submission.execution_id)
    if grant.state != "running":
        return await _finish(session_factory, request, grant, canceled=True)
    try:
        deadline = asyncio.get_running_loop().time() + request.timeout_seconds
        async with _renew_while_preparing(session_factory, request, grant):
            records = await _load_replayed_records(session_factory, request, grant, submission, deadline)
            candidate = _candidate_request(request, grant.execution_id, records)
            admitted = runner.reject_duplicate_canonical_root_keys(
                request.definition,
                assemble_root_families(request.definition, candidate.roots, candidate.children_by_collection),
            )
            _remaining_seconds(deadline)
        async with asyncio.timeout_at(deadline):
            grant = await _heartbeat(session_factory, request, grant)
            _remaining_seconds(deadline)
        return await runner.run_claimed_candidate(session_factory, candidate, grant, admitted)
    except CancellationRequested:
        return await _finish(session_factory, request, grant, canceled=True)
    except LeaseAuthorityLost:
        return CandidateRunResult(status="lease_lost", execution_id=grant.execution_id)


async def run_https_candidate(
    session_factory: SessionFactory, request: HttpsCandidateRequest, *, resume: bool = False
) -> CandidateRunResult:
    """Acquire once, durably bind, then replay through the existing claimed runner.

    Resume only takes an expired lease on an exact bound active execution. An
    interrupted unbound acquisition requires a new idempotency key, never a refetch.
    """
    try:
        if not callable(session_factory) or not isinstance(resume, bool):
            raise HttpsCandidateError("https_request_invalid")
        request = _checked_request(request)
        identity = https_request_identity_sha256(request)
        return await _run(session_factory, request, identity, resume=resume)
    except asyncio.CancelledError:
        raise asyncio.CancelledError from None
    except HttpsCandidateError:
        raise HttpsCandidateError("https_candidate_failed") from None
    except Exception:
        raise HttpsCandidateError("https_candidate_failed") from None
