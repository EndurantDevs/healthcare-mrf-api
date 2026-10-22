# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded standalone HTTPS acquisition and exact durable replay."""

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
from process.custom_import.capture_authority import require_raw_capture_authority
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
from process.custom_import.raw_capture_store import load_replayable_raw_bundle, register_raw_capture_bundle
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
    maximum_dataset_retained_bytes: int | None = None


def _uses_originals(request, *, raw_originals=False):
    return (
        raw_originals
        or getattr(request, "source_kind", "https") != "https"
        or any(
            stream.format != "parquet" or stream.compression != "none" for stream in request.definition.source_streams
        )
    )


def _check_retention_cap(request, *, raw_originals=False):
    cap = request.maximum_dataset_retained_bytes
    if _uses_originals(request, raw_originals=raw_originals):
        if isinstance(cap, bool) or not isinstance(cap, int) or not 0 <= cap <= 2**63 - 1:
            raise HttpsCandidateError("https_request_invalid")
    elif cap is not None:
        raise HttpsCandidateError("https_request_invalid")


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


def _checked_request(request, *, raw_originals=False):
    if not isinstance(request, HttpsCandidateRequest) or request.complete_scope is not True:
        raise HttpsCandidateError("https_request_invalid")
    if not isinstance(request.objects, tuple) or not all(
        isinstance(source_object, HttpsObject) for source_object in request.objects
    ):
        raise HttpsCandidateError("https_request_invalid")
    request = _checked_request_fields(request, raw_originals=raw_originals)
    for source_object in request.objects:
        https._https_url(source_object.url)
    return request


def _checked_request_fields(request, *, raw_originals=False):
    """Normalize bundle-wide limits and pins after transport-specific type checks."""
    if request.complete_scope is not True:
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
    ):
        raise HttpsCandidateError("https_request_invalid")
    runner.validate_candidate_request(lambda: None, _candidate_request(request, 1))
    validate_snapshot_token(request.source_snapshot_token)
    _check_retention_cap(request, raw_originals=raw_originals)
    objects_by_stream = {source_object.stream_id: source_object for source_object in request.objects}
    streams = request.definition.source_streams
    if len(objects_by_stream) != len(request.objects) or set(objects_by_stream) != {
        stream.stream_id for stream in streams
    }:
        raise HttpsCandidateError("https_request_invalid")
    for stream in streams:
        source_object = objects_by_stream[stream.stream_id]
        https._validate_identity(
            source_object.expected_sha256, source_object.expected_bytes, limits, request.timeout_seconds
        )
        minimum_bytes = 0 if _uses_originals(request, raw_originals=raw_originals) else 1
        if not minimum_bytes <= source_object.expected_bytes <= _MAX_PARQUET_PART_BYTES:
            raise HttpsCandidateError("https_request_invalid")
    bundle_limit = min(limits.maximum_compressed_bytes, _MAX_PARQUET_BYTES_PER_BUNDLE)
    if not _uses_originals(request, raw_originals=raw_originals):
        bundle_limit = min(bundle_limit, limits.maximum_decoded_bytes)
    if sum(source_object.expected_bytes for source_object in request.objects) > bundle_limit:
        raise HttpsCandidateError("https_request_invalid")
    return replace(request, objects=tuple(objects_by_stream[stream.stream_id] for stream in streams), limits=limits)


def https_request_identity_sha256(request: HttpsCandidateRequest, *, raw_originals=False) -> bytes:
    """Bind source content, definition, full scope and limits without retaining URLs."""
    try:
        request = _checked_request(request, raw_originals=raw_originals)
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
                    "stream_id": source_object.stream_id,
                    "url_sha256": hashlib.sha256(source_object.url.encode("utf-8")).hexdigest(),
                    "expected_sha256": source_object.expected_sha256,
                    "expected_bytes": source_object.expected_bytes,
                }
                for source_object in request.objects
            ],
        }
        if request.maximum_dataset_retained_bytes is not None:
            document_by_key["maximum_dataset_retained_bytes"] = request.maximum_dataset_retained_bytes
        encoded_request = json.dumps(document_by_key, allow_nan=False, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
        return hashlib.sha256(_CONTRACT.encode("ascii") + b"\0request\0" + encoded_request).digest()
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


def _retained_capture(request, item, stream, sealed, *, raw_originals=False):
    _check_capture(request, item, stream, sealed)
    if _uses_originals(request, raw_originals=raw_originals):
        return sealed
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


def _reconstructed_capture(request, item, stream, retained, *, raw_originals=False):
    if _uses_originals(request, raw_originals=raw_originals):
        if not isinstance(retained, SealedCapture):
            raise HttpsCandidateError("https_capture_mismatch")
        _check_capture(request, item, stream, retained)
        return retained
    if isinstance(retained, SealedCapture):
        raise HttpsCandidateError("https_capture_mismatch")
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


async def _acquire_https_object(item, stream, **options):
    return await https.acquire_https(
        item.url,
        stream,
        expected_sha256=item.expected_sha256,
        expected_bytes=item.expected_bytes,
        **options,
    )


async def _acquire(session_factory, request, grant, deadline, *, acquire_one=None, raw_originals=False):
    acquire_one = _acquire_https_object if acquire_one is None else acquire_one
    retained_captures = []
    async with asyncio.timeout_at(deadline):
        for item, stream in zip(request.objects, request.definition.source_streams, strict=True):
            await _heartbeat(session_factory, request, grant)
            sealed = await acquire_one(
                item,
                stream,
                source_snapshot_token=request.source_snapshot_token,
                limits=request.limits,
                timeout_seconds=_remaining_seconds(deadline),
            )
            retained_captures.append(_retained_capture(request, item, stream, sealed, raw_originals=raw_originals))
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
        registrar = register_replayable_parquet_bundle
        extra_by_name = {}
        if _uses_originals(request):
            registrar = register_raw_capture_bundle
            extra_by_name = {
                "maximum_dataset_retained_bytes": request.maximum_dataset_retained_bytes,
                "limits": request.limits,
            }
        registered = await registrar(
            session,
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
            captures=captures,
            **extra_by_name,
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
    decoded_payload = capture._bounded_parquet_payload(
        sealed.payload,
        sealed.manifest.compression,
        limits,
        expected_decoded_bytes=sealed.manifest.decoded_bytes,
        expected_decoded_sha256=sealed.manifest.decoded_sha256,
    )
    capture._validate_parquet_envelope(decoded_payload)
    with capture._open_parquet_reader(decoded_payload, limits) as reader:
        schema = reader.schema_arrow
        labels = capture._validated_parquet_schema(schema, limits)
        expected_records = capture._validated_parquet_metadata(
            reader.metadata, expected_columns=len(labels), limits=limits
        )
        if record_count + expected_records > limits.maximum_records:
            raise capture.CaptureError("https_replay_budget_exceeded")
        capture._validate_parquet_page_preflight(
            decoded_payload, reader.metadata, schema, maximum_decoded_bytes=limits.maximum_decoded_bytes
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


async def _replay(request, captures, *, deadline=None, raw_originals=False):
    deadline = asyncio.get_running_loop().time() + request.timeout_seconds if deadline is None else deadline
    by_stream = {
        retained.manifest.stream_id if isinstance(retained, SealedCapture) else retained.receipt.stream_id: retained
        for retained in captures
    }
    if len(by_stream) != len(captures) or set(by_stream) != {
        source_object.stream_id for source_object in request.objects
    }:
        raise HttpsCandidateError("https_capture_mismatch")
    records_by_stream, tokens = {}, {}
    decoded_bytes = record_count = arrow_bytes = record_bytes = 0
    for source_object, stream in zip(request.objects, request.definition.source_streams, strict=True):
        sealed = _reconstructed_capture(
            request, source_object, stream, by_stream[stream.stream_id], raw_originals=raw_originals
        )
        tokens[stream.stream_id] = [sealed.manifest.source_snapshot_token]
        decoded_bytes += sealed.manifest.decoded_bytes
        if decoded_bytes > request.limits.maximum_decoded_bytes:
            raise HttpsCandidateError("https_replay_budget_exceeded")
        if stream.format == "parquet":
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


async def _capture_acquisition_deadline(session_factory, request, grant, deadline, *, capture_handoff):
    """Check acquisition authority before source access, without retaining local locks."""
    async with asyncio.timeout_at(deadline):
        if capture_handoff is not None:
            deadline = min(deadline, await capture_handoff.preflight(request, grant))
            _remaining_seconds(deadline)
        elif _uses_originals(request):
            async with session_factory() as session, session.begin():
                await require_raw_capture_authority(session)
    return deadline


async def _load_replayed_records(
    session_factory, request, grant, submission, deadline, *, acquire_one=None, capture_handoff=None
):
    """Bind a complete acquisition, then verify its fresh-session durable readback."""
    bundle_id = submission.capture_bundle_id
    captures = None
    acquisition_deadline = deadline
    if bundle_id is None:
        if submission.state != "queued" or grant.fence != 1:
            await _finish(session_factory, request, grant)
            raise HttpsCandidateError("https_unbound_capture")
        try:
            acquisition_deadline = await _capture_acquisition_deadline(
                session_factory, request, grant, deadline, capture_handoff=capture_handoff
            )
            captures = await _acquire(
                session_factory,
                request,
                grant,
                acquisition_deadline,
                acquire_one=acquire_one,
                raw_originals=capture_handoff is not None,
            )
        except CancellationRequested, LeaseAuthorityLost:
            raise
        except Exception:
            await _finish(session_factory, request, grant)
            raise
    async with asyncio.timeout_at(deadline):
        if captures is not None:
            # Registration may commit without acknowledgement; never terminalize or refetch here.
            if capture_handoff is None:
                bundle_id = await _store(session_factory, request, grant, captures)
            else:
                async with asyncio.timeout_at(acquisition_deadline):
                    _remaining_seconds(acquisition_deadline)
                    bundle_id = await capture_handoff.register(request, grant, captures)
            del captures
        if capture_handoff is not None:
            captures = await capture_handoff.retained(request, grant, bundle_id)
        else:
            async with session_factory() as session, session.begin():
                loader = load_replayable_raw_bundle if _uses_originals(request) else load_replayable_parquet_bundle
                extra_by_name = {"limits": request.limits} if _uses_originals(request) else {}
                captures = await loader(
                    session,
                    capture_bundle_id=bundle_id,
                    dataset_id=request.dataset_id,
                    definition_revision_id=request.definition_revision_id,
                    schema_revision_id=request.schema_revision_id,
                    **extra_by_name,
                )
        return await _replay(request, captures, deadline=deadline, raw_originals=capture_handoff is not None)


async def _run_candidate(session_factory, request, identity, *, resume, acquire_one=None, capture_handoff=None):
    if capture_handoff is not None:
        capture_handoff.require_candidate_binding(request)
    submission, grant = await _reserve(session_factory, request, identity, resume)
    if grant is None:
        return CandidateRunResult(status="not_claimed", execution_id=submission.execution_id)
    if grant.state != "running":
        return await _finish(session_factory, request, grant, canceled=True)
    try:
        deadline = asyncio.get_running_loop().time() + request.timeout_seconds
        async with _renew_while_preparing(session_factory, request, grant):
            records_by_stream = await _load_replayed_records(
                session_factory,
                request,
                grant,
                submission,
                deadline,
                acquire_one=acquire_one,
                capture_handoff=capture_handoff,
            )
            candidate = _candidate_request(request, grant.execution_id, records_by_stream)
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
    session_factory: SessionFactory, request: HttpsCandidateRequest, *, resume: bool = False, capture_handoff=None
) -> CandidateRunResult:
    """Acquire once, durably bind, then replay through the existing claimed runner.

    Resume only takes an expired lease on an exact bound active execution. An
    interrupted unbound acquisition requires a new idempotency key, never a refetch.
    """
    try:
        if not callable(session_factory) or not isinstance(resume, bool):
            raise HttpsCandidateError("https_request_invalid")
        request = _checked_request(request, raw_originals=capture_handoff is not None)
        identity = https_request_identity_sha256(request, raw_originals=capture_handoff is not None)
        return await _run_candidate(session_factory, request, identity, resume=resume, capture_handoff=capture_handoff)
    except asyncio.CancelledError:
        raise asyncio.CancelledError from None
    except HttpsCandidateError:
        raise HttpsCandidateError("https_candidate_failed") from None
    except Exception:
        raise HttpsCandidateError("https_candidate_failed") from None
