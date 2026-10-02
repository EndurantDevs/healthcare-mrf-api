# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Capture-only acquisition for a dedicated supervised worker.

The caller must independently admit the explicit policy and supervise the
worker process. Driver timeouts and executor cancellation are not hard shutdown
bounds. Cancellation awaits actual affinity-thread cleanup; an unresponsive
driver requires the outer supervisor to terminate the dedicated process.
No capture result establishes replay verification, generation or publication.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import inspect
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from functools import partial
from typing import Any

from db.models.custom_import import CustomImportCaptureBundle
from process.custom_import import execution as lifecycle
from process.custom_import.capture import verify_capture
from process.custom_import.capture_pending import (
    _ACCOUNTING_FIELDS,
    _MANIFEST_SET_DOMAIN,
    SEGMENTED_PAYLOAD_CONTRACT,
    PendingCaptureRequest,
    append_pending_parquet_part,
    begin_pending_parquet_bundle,
    mark_pending_parquet_eof,
    seal_pending_parquet_bundle,
)
from process.custom_import.capture_store import _PARQUET_PART_SET_DOMAIN, CaptureReceipt, _add_payload_part_digest
from process.custom_import.definition import canonical_json
from process.custom_import.family import validate_source_snapshot_tokens
from process.custom_import.runner_types import SessionFactory
from process.custom_import.segmented_capture_policy import SegmentedCapturePolicy
from process.custom_import.snowflake import (
    CONNECTOR_CONTRACT,
    PARQUET_RESULT_FORMAT,
    SnowflakeCredentialProvider,
    SnowflakeKeyPairCredentials,
    SnowflakeResultColumn,
    _identity_sha256,
)
from process.custom_import.snowflake_bundle import (
    SnowflakeBundleStatementBuilder,
    SnowflakeBundleStreamMetadata,
    _diagnostic_query_id,
    _query_identity_snapshot_token,
    _validated_bundle_statement,
)
from process.custom_import.snowflake_bundle_replay import (
    _is_bundle_column_type_valid,
    _validate_replay_partition_schema,
)
from process.custom_import.snowflake_candidate import (
    SnowflakeBundleCandidateRequest,
    SnowflakeCandidateError,
    _prepared_bundle_statement,
    _renew_bundle_lease,
    _reservation_unavailable_result,
    _reserve_bundle_execution,
    _unbound_capture_result,
    _validated_bundle_request,
    bundle_request_identity_sha256,
)
from process.custom_import.snowflake_python import (
    SnowflakeBundleLandingResult,
    SnowflakeLandingEOF,
    SnowflakeLandingPart,
    SnowflakePythonConnectorAdapter,
    _arrow_type,
    _execution_timeout,
    _validate_landing_limits,
)

_REQUEST_DOMAIN = b"custom-import/snowflake-segmented-request-identity/v1\0"
_SOURCE_CONTRACT = "custom-import/snowflake-bundle/v2"
_END = object()


class SnowflakeCaptureError(SnowflakeCandidateError):
    """Acquisition failed without establishing a complete source capture."""


class SnowflakeCaptureCleanupUncertain(SnowflakeCaptureError):
    """Cleanup was attempted, but a supervised termination may still be needed."""


@dataclass(frozen=True)
class SnowflakeCaptureResult:
    """Capture-only progress, never an execution-completion receipt."""

    status: str
    execution_id: int
    capture_bundle_id: int | None = None
    fence: int | None = None


def segmented_bundle_request_identity_sha256(request, statement, policy, *, source_binding_sha256=None) -> bytes:
    """Pin the prepared identity and separately admitted capture policy in a new domain."""

    policy = _policy(policy)
    if request.processing_policy is not None and request.processing_policy.capture != policy:
        raise SnowflakeCaptureError("capture policy differs from the request processing policy")
    prior = bundle_request_identity_sha256(request, statement, source_binding_sha256=source_binding_sha256)
    return hashlib.sha256(_REQUEST_DOMAIN + prior + bytes.fromhex(policy.digest)).digest()


def _policy(policy) -> SegmentedCapturePolicy:
    if not isinstance(policy, SegmentedCapturePolicy):
        raise SnowflakeCaptureError("segmented capture requires an explicitly admitted policy")
    return SegmentedCapturePolicy.from_mapping(policy.to_mapping())


def _validate_capture_policy(request, policy, timeout, processing_policy):
    """Require the selected source semantics and admitted capture bounds to agree."""

    if request.bundle_request.processing_policy != processing_policy or (
        processing_policy is not None
        and (processing_policy.capture != policy or processing_policy.driver_timeout_seconds != timeout)
    ):
        raise SnowflakeCaptureError("capture policy differs from the request processing policy")


@dataclass
class _Attempt:
    deadline: float
    stopped: threading.Event = field(default_factory=threading.Event)
    finished: asyncio.Event = field(default_factory=asyncio.Event)
    reason: str | None = None

    def stop(self, reason: str) -> None:
        """Latch a stop reason without granting any durable mutation authority."""

        if self.reason is None:
            self.reason = reason
        self.stopped.set()

    def check(self) -> None:
        """Reject every later step once the absolute deadline or stop latch wins."""

        if time.monotonic() >= self.deadline:
            self.stop("acquisition deadline expired")
        if self.stopped.is_set():
            raise SnowflakeCaptureError(self.reason or "capture authority was lost")


async def _acquisition_deadline(session_factory, request, grant, policy) -> float:
    # Anchor conservatively before the database round trip, never after it.
    observed = time.monotonic()
    async with session_factory() as session, session.begin():
        context = await lifecycle._lock_current_capture_binding(
            session,
            execution_id=grant.execution_id,
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
            fence=grant.fence,
            token_sha256=lifecycle.lease_token_sha256(request.lease_token),
        )
        if context is None:
            raise SnowflakeCaptureError("capture authority was lost before source opening")
        execution, _, _ = context
        started = execution.started_at
        if not isinstance(started, dt.datetime) or started.tzinfo is None:
            raise SnowflakeCaptureError("capture acquisition start is unavailable")
        now = await lifecycle._database_now(session)
        if started > now:
            raise SnowflakeCaptureError("capture acquisition start is in the future")
        remaining = (started + dt.timedelta(seconds=policy.acquisition_deadline_seconds) - now).total_seconds()
    deadline = observed + remaining
    if time.monotonic() >= deadline:
        raise SnowflakeCaptureError("acquisition deadline expired before source opening")
    return deadline


async def _heartbeat(session_factory, request, grant, attempt) -> None:
    interval = lifecycle.DEFAULT_LEASE_SECONDS / 3
    while not attempt.finished.is_set() and not attempt.stopped.is_set():
        try:
            attempt.check()
            async with asyncio.timeout(min(interval, attempt.deadline - time.monotonic())):
                renewed = await _renew_bundle_lease(session_factory, request, grant)
            if renewed is None or renewed.state != "running":
                attempt.stop("capture authority was lost")
                return
        except Exception:
            attempt.stop("capture heartbeat failed")
            return
        try:
            await asyncio.wait_for(attempt.finished.wait(), min(interval, attempt.deadline - time.monotonic()))
        except TimeoutError:
            continue


async def _await_owned(future, attempt):
    """Keep ownership through cancellation; never abandon an affinity operation."""

    cancellation = None
    while True:
        try:
            value = await asyncio.shield(future)
            break
        except asyncio.CancelledError as exc:
            attempt.stop("capture caller canceled")
            if cancellation is None:
                cancellation = exc
            if future.cancelled():
                raise
        except BaseException:
            if cancellation is not None:
                raise cancellation from None
            raise
    if cancellation is not None:
        raise cancellation
    return value


def _schema_source(statement, binding, schema, query_id) -> dict[str, Any]:
    if not isinstance(schema, tuple) or not all(isinstance(column, SnowflakeResultColumn) for column in schema):
        raise SnowflakeCaptureError("capture result schema is invalid")
    rebuilt_columns = tuple(SnowflakeResultColumn(**asdict(column)) for column in schema)
    if rebuilt_columns != schema or tuple(column.field_id for column in rebuilt_columns) != binding.selected_field_ids:
        raise SnowflakeCaptureError("capture result schema does not match selected fields")
    fields_by_id = {field.field_id: field for field in statement.request.definition.fields}
    if any(
        not _is_bundle_column_type_valid(_arrow_type(column), fields_by_id[column.field_id])
        for column in rebuilt_columns
    ):
        raise SnowflakeCaptureError("capture result schema type does not match declared fields")
    schema_documents = [asdict(column) for column in rebuilt_columns]
    _, fingerprint = _identity_sha256(
        "result-schema", {"columns": schema_documents, "contract": CONNECTOR_CONTRACT, "format": PARQUET_RESULT_FORMAT}
    )
    return {
        "contract": _SOURCE_CONTRACT,
        "query_id": query_id,
        "encoding": statement.request.encoding.as_identity_document(),
        "relation": list(binding.relation.parts),
        "selected_field_ids": list(binding.selected_field_ids),
        "semantic_token_metadata_key": binding.semantic_token_metadata_key,
        "result_schema": schema_documents,
        "schema_fingerprint": fingerprint,
        "request_sha256": statement.request.request_sha256,
        "statement_sha256": statement.statement_sha256,
    }


def _landing_metadata(statement, landing_result):
    if not isinstance(landing_result, SnowflakeBundleLandingResult):
        raise SnowflakeCaptureError("capture opener returned an invalid landing result")
    query_id = _diagnostic_query_id(landing_result.query_id)
    if query_id is None or query_id != landing_result.query_id:
        raise SnowflakeCaptureError("capture query identity is invalid")
    bindings = statement.request.bindings
    if not isinstance(landing_result.metadata, tuple) or len(landing_result.metadata) != len(bindings):
        raise SnowflakeCaptureError("capture metadata stream coverage is invalid")
    if not isinstance(landing_result.schemas, tuple) or len(landing_result.schemas) != len(bindings):
        raise SnowflakeCaptureError("capture schema stream coverage is invalid")
    tokens_by_stream = {}
    source_by_stream = {}
    for binding, metadata, schema in zip(bindings, landing_result.metadata, landing_result.schemas, strict=True):
        if not isinstance(metadata, SnowflakeBundleStreamMetadata):
            raise SnowflakeCaptureError("capture stream metadata is invalid")
        rebuilt = SnowflakeBundleStreamMetadata(**asdict(metadata))
        if rebuilt != metadata or (rebuilt.stream_id, rebuilt.semantic_token_metadata_key) != (
            binding.stream_id,
            binding.semantic_token_metadata_key,
        ):
            raise SnowflakeCaptureError("capture stream metadata has drifted")
        tokens_by_stream[binding.stream_id] = rebuilt.source_snapshot_tokens
        source_by_stream[binding.stream_id] = _schema_source(statement, binding, schema, query_id)
    token = validate_source_snapshot_tokens(statement.request.definition, tokens_by_stream)
    if landing_result.source_snapshot_token != token or (
        any(binding.source_snapshot_token_relation is None for binding in bindings)
        and token != _query_identity_snapshot_token(query_id)
    ):
        raise SnowflakeCaptureError("capture source snapshot identity has drifted")
    return token, source_by_stream


class _AffinityLanding:
    """The sole source owner; all methods run on the same dedicated thread."""

    def __init__(self, builder, adapter, credential_provider, statement, policy, timeout_seconds, attempt):
        self.builder = builder
        self.adapter = adapter
        self.credential_provider = credential_provider
        self.statement = statement
        self.policy = policy
        self.timeout_seconds = timeout_seconds
        self.attempt = attempt
        self.result = None
        self.events = None
        self.close_error = None
        self.source_failed = False

    def open(self):
        """Load credentials and open once after immutable source preflight."""

        self.attempt.check()
        if _validated_bundle_statement(self.builder.build_statement(self.statement.request)) != self.statement:
            raise SnowflakeCaptureError("capture prepared statement has drifted")
        credentials = self.credential_provider.load_key_pair()
        if not isinstance(credentials, SnowflakeKeyPairCredentials):
            raise SnowflakeCaptureError("capture credentials have an invalid type")
        self.attempt.check()
        timeout = min(self.timeout_seconds, int(self.attempt.deadline - time.monotonic()))
        if timeout < 1:
            raise SnowflakeCaptureError("capture has insufficient source-opening time")
        try:
            self.result = self.adapter.open_bundle_landing(
                self.statement,
                credentials,
                part_limits=self.policy.part_limits,
                maximum_part_arrow_bytes=self.policy.maximum_part_arrow_bytes,
                timeout_seconds=timeout,
            )
        except BaseException:
            self.source_failed = True
            raise
        self.attempt.check()
        metadata = _landing_metadata(self.statement, self.result)
        self.events = self.result.consume_events()
        return metadata

    def next_event(self):
        """Advance one event, reject late results, and validate its bounded schema."""

        self.attempt.check()
        try:
            event = next(self.events, _END)
        except BaseException:
            # A failed source step may have used best-effort internal cleanup.
            self.source_failed = True
            raise
        self.attempt.check()
        if isinstance(event, SnowflakeLandingPart):
            streams_by_id = {stream.stream_id: stream for stream in self.statement.request.definition.source_streams}
            if event.stream_id not in streams_by_id:
                raise SnowflakeCaptureError("capture event stream coverage is invalid")
            stream = streams_by_id[event.stream_id]
            fields = tuple(
                field
                for field in self.statement.request.definition.fields
                if field.collection == stream.child_collection
            )
            _validate_replay_partition_schema(event.capture, fields=fields, limits=self.policy.part_limits)
        return event

    def close(self):
        """Confirm affinity cleanup or retain its failure without retry camouflage."""

        if self.close_error is not None:
            raise self.close_error
        if self.result is not None:
            try:
                self.result.close()
            except BaseException as exc:
                self.close_error = exc
                raise
            self.result = self.events = None


@dataclass
class _StreamReceipt:
    stream: Any
    source: dict[str, Any]
    totals: dict[str, int] = field(default_factory=lambda: dict.fromkeys(_ACCOUNTING_FIELDS, 0))
    payload_digest: Any = field(default_factory=lambda: hashlib.sha256(_PARQUET_PART_SET_DOMAIN))
    manifest_digest: Any = field(default_factory=lambda: hashlib.sha256(_MANIFEST_SET_DOMAIN))
    eof: bool = False

    def part(self, event, request):
        """Validate one provisional part without changing committed digest state."""

        if self.eof or type(event.ordinal) is not int or event.ordinal != self.totals["part_count"] + 1:
            raise SnowflakeCaptureError("capture part order is invalid")
        for value, maximum in (
            (event.record_count, request.policy.part_limits.maximum_records),
            (event.arrow_byte_count, request.policy.maximum_part_arrow_bytes),
        ):
            if type(value) is not int or not 0 <= value <= maximum:
                raise SnowflakeCaptureError("capture part accounting is invalid")
        verify_capture(event.capture, self.stream, limits=request.policy.part_limits)
        if event.capture.manifest.source_snapshot_token != request.source_snapshot_token:
            raise SnowflakeCaptureError("capture part snapshot identity has drifted")
        canonical = canonical_json(asdict(event.capture.manifest)).encode("utf-8")
        values = (
            1,
            len(event.capture.payload),
            event.capture.manifest.decoded_bytes,
            event.arrow_byte_count,
            event.record_count,
            len(canonical),
        )
        if len(canonical) > request.policy.maximum_part_manifest_bytes:
            raise SnowflakeCaptureError("capture part manifest exceeds policy")
        return canonical, values

    def committed(self, event, canonical, values):
        """Fold only a confirmed commit into bounded counters and hash states."""

        _add_payload_part_digest(
            self.payload_digest,
            event.ordinal,
            len(event.capture.payload),
            hashlib.sha256(event.capture.payload).digest(),
        )
        _add_payload_part_digest(
            self.manifest_digest, event.ordinal, len(canonical), hashlib.sha256(canonical).digest()
        )
        for value in (event.capture.manifest.decoded_bytes, event.arrow_byte_count, event.record_count):
            self.manifest_digest.update(value.to_bytes(8, "big"))
        for name, value in zip(_ACCOUNTING_FIELDS, values, strict=True):
            self.totals[name] += value

    def finish(self, event):
        """Retain compact EOF evidence, without authorizing seal before cleanup."""

        if (
            self.eof
            or type(event.part_count) is not int
            or type(event.record_count) is not int
            or (event.part_count, event.record_count) != (self.totals["part_count"], self.totals["record_count"])
            or event.part_count < 1
        ):
            raise SnowflakeCaptureError("capture EOF accounting is invalid")
        self.eof = True

    def receipt(self, request):
        """Render one compact stream receipt with no per-part arrays."""

        document_by_key = {
            "contract_version": SEGMENTED_PAYLOAD_CONTRACT,
            "policy_sha256": request.policy.digest,
            "source_request_sha256": request.source_request_sha256.hex(),
            "statement_sha256": request.statement_sha256.hex(),
            "source_snapshot_token": request.source_snapshot_token,
            "stream_id": self.stream.stream_id,
            "payload_set_sha256": self.payload_digest.hexdigest(),
            "manifest_set_sha256": self.manifest_digest.hexdigest(),
            "source": self.source,
            **self.totals,
        }
        if request.source_binding_sha256 is not None:
            document_by_key["source_binding_sha256"] = request.source_binding_sha256.hex()
        canonical = canonical_json(document_by_key)
        return CaptureReceipt(
            self.stream.stream_id,
            request.source_snapshot_token,
            self.totals["byte_count"],
            self.payload_digest.hexdigest(),
            canonical,
            hashlib.sha256(canonical.encode()).hexdigest(),
        )


def _pending_request(request, grant, identity, statement, token, policy):
    return PendingCaptureRequest(
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        execution_id=grant.execution_id,
        fence=grant.fence,
        token=request.lease_token.encode("utf-8")
        if isinstance(request.lease_token, str)
        else bytes(request.lease_token),
        request_identity_sha256=identity,
        source_request_sha256=bytes.fromhex(statement.request.request_sha256),
        statement_sha256=bytes.fromhex(statement.statement_sha256),
        source_snapshot_token=token,
        policy=policy,
        source_binding_revision_id=request.source_binding_revision_id,
        source_binding_sha256=request.source_binding_sha256,
    )


async def _append_event(session_factory, request, bundle_id, event, state, attempt):
    canonical, values = state.part(event, request)
    attempt.check()
    async with session_factory() as session, session.begin():
        attempt.check()
        await append_pending_parquet_part(
            session,
            request=request,
            capture_bundle_id=bundle_id,
            capture=event.capture,
            ordinal=event.ordinal,
            record_count=event.record_count,
            arrow_byte_count=event.arrow_byte_count,
        )
    state.committed(event, canonical, values)


async def _finish_landing(session_factory, request, bundle_id, receipt_by_stream, attempt):
    if not all(state.eof for state in receipt_by_stream.values()):
        raise SnowflakeCaptureError("capture did not exhaust every declared stream")
    for stream_id, state in receipt_by_stream.items():
        attempt.check()
        async with session_factory() as session, session.begin():
            attempt.check()
            await mark_pending_parquet_eof(
                session,
                request=request,
                capture_bundle_id=bundle_id,
                stream_id=stream_id,
                part_count=state.totals["part_count"],
                record_count=state.totals["record_count"],
            )
    attempt.check()
    async with session_factory() as session, session.begin():
        attempt.check()
        await seal_pending_parquet_bundle(
            session,
            request=request,
            capture_bundle_id=bundle_id,
            receipts=tuple(state.receipt(request) for state in receipt_by_stream.values()),
        )
    return SnowflakeCaptureResult("capture_sealed", request.execution_id, bundle_id, request.fence)


async def _land(session_factory, pending, definition, source_by_stream, bundle_id, owner, submit, attempt):
    receipt_by_stream = {
        stream.stream_id: _StreamReceipt(stream, source_by_stream[stream.stream_id])
        for stream in definition.source_streams
    }
    while True:
        event = await _await_owned(submit(owner.next_event), attempt)
        attempt.check()
        if event is _END:
            break
        if (
            not isinstance(event, (SnowflakeLandingPart, SnowflakeLandingEOF))
            or event.stream_id not in receipt_by_stream
        ):
            raise SnowflakeCaptureError("capture event stream coverage is invalid")
        state = receipt_by_stream[event.stream_id]
        if isinstance(event, SnowflakeLandingPart):
            if any(state.eof for state in receipt_by_stream.values()):
                raise SnowflakeCaptureError("capture part followed source EOF evidence")
            await _append_event(session_factory, pending, bundle_id, event, state, attempt)
        else:
            state.finish(event)
        event = None
    await _await_owned(submit(owner.close), attempt)
    attempt.check()
    return await _finish_landing(session_factory, pending, bundle_id, receipt_by_stream, attempt)


async def _bound_result(session_factory, request, grant, bundle_id, identity, statement, policy):
    async with session_factory() as session:
        bundle = await session.get(CustomImportCaptureBundle, bundle_id)
        expected_by_field = {
            "dataset_id": request.dataset_id,
            "definition_revision_id": request.definition_revision_id,
            "schema_revision_id": request.schema_revision_id,
            "capture_state": "sealed",
            "payload_contract": SEGMENTED_PAYLOAD_CONTRACT,
            "canonical_policy": policy.canonical,
            "source_binding_revision_id": request.source_binding_revision_id,
            "producing_execution_id": grant.execution_id,
        }
        digests_by_field = {
            "request_identity_sha256": identity,
            "policy_sha256": bytes.fromhex(policy.digest),
            "source_request_sha256": bytes.fromhex(statement.request.request_sha256),
            "statement_sha256": bytes.fromhex(statement.statement_sha256),
            "source_binding_sha256": request.source_binding_sha256,
        }
        if (
            bundle is None
            or any(getattr(bundle, key) != value for key, value in expected_by_field.items())
            or any(getattr(bundle, key) != value for key, value in digests_by_field.items())
        ):
            raise SnowflakeCaptureError("bound capture identity has drifted")
    return SnowflakeCaptureResult("capture_bound", grant.execution_id, bundle_id, grant.fence)


async def _acquire_claimed(session_factory, request, grant, identity, owner, executor, attempt):
    loop = asyncio.get_running_loop()
    submit = lambda call: loop.run_in_executor(executor, call)
    heartbeat = asyncio.create_task(_heartbeat(session_factory, request, grant, attempt))
    primary = None
    bundle_id = None
    try:
        token, source_by_stream = await _await_owned(submit(owner.open), attempt)
        attempt.check()
        pending = _pending_request(request, grant, identity, owner.statement, token, owner.policy)
        async with session_factory() as session, session.begin():
            attempt.check()
            registration = await begin_pending_parquet_bundle(session, request=pending)
        bundle_id = registration.capture_bundle_id
        outcome = await _land(
            session_factory,
            pending,
            request.definition,
            source_by_stream,
            registration.capture_bundle_id,
            owner,
            submit,
            attempt,
        )
    except BaseException as exc:
        primary = exc
        if bundle_id is not None:
            primary.add_note(
                f"Capture bundle {bundle_id} remains unpublished; retained pending storage may require cleanup."
            )
        if owner.source_failed:
            primary.add_note("Source operation failed; its internal cleanup is unconfirmed.")
        attempt.stop("capture acquisition failed")
    finally:
        attempt.finished.set()
        try:
            await _await_owned(submit(owner.close), attempt)
        except BaseException as exc:
            if primary is None:
                if isinstance(exc, asyncio.CancelledError):
                    primary = exc
                else:
                    primary = SnowflakeCaptureCleanupUncertain("source cleanup is unconfirmed")
                    primary.__cause__ = exc
            if owner.close_error is not None or not isinstance(exc, asyncio.CancelledError):
                primary.add_note("Source cleanup is unconfirmed; dedicated worker supervision is required.")
                if isinstance(primary, asyncio.CancelledError):
                    primary.__cause__ = owner.close_error or exc
        finally:
            executor.shutdown(wait=False)
            try:
                await _await_owned(heartbeat, attempt)
            except BaseException as exc:
                if primary is None:
                    primary = exc
    if primary is not None:
        raise primary
    return outcome


async def acquire_segmented_snowflake_capture(
    session_factory: SessionFactory,
    request: SnowflakeBundleCandidateRequest,
    *,
    statement_builder: SnowflakeBundleStatementBuilder,
    adapter: SnowflakePythonConnectorAdapter,
    credential_provider: SnowflakeCredentialProvider,
    policy: SegmentedCapturePolicy,
    driver_timeout_seconds: int,
    processing_policy=None,
) -> SnowflakeCaptureResult:
    """Acquire and bind only a source capture under an independently admitted policy.

    This entry point must run inside a dedicated supervised worker. Retained v2
    bindings select it; legacy execution remains unchanged. On cancellation it
    retains the worker future and awaits cleanup, with no hard local time claim.
    """

    request = _validated_bundle_request(request)
    policy = _policy(policy)
    _validate_capture_policy(request, policy, driver_timeout_seconds, processing_policy)
    _require_capture_capabilities(session_factory, statement_builder, adapter, credential_provider)
    _validate_landing_limits(policy.part_limits, policy.maximum_part_arrow_bytes)
    timeout = _execution_timeout(driver_timeout_seconds)
    statement = _prepared_bundle_statement(statement_builder.build_statement, request)
    _require_landing_signature(adapter, statement, policy, timeout)
    if any(
        len(columns) > policy.part_limits.maximum_fields_per_record for columns in statement.selected_columns_by_stream
    ):
        raise SnowflakeCaptureError("capture projection exceeds the field limit")
    identity = segmented_bundle_request_identity_sha256(
        request.bundle_request, statement, policy, source_binding_sha256=request.source_binding_sha256
    )
    submission, grant = await _reserve_bundle_execution(
        session_factory, request, statement, identity, processing_policy=processing_policy
    )
    if grant is None:
        return SnowflakeCaptureResult("not_claimed", submission.execution_id)
    if grant.state != "running":
        unavailable = await _reservation_unavailable_result(session_factory, request, grant)
        return SnowflakeCaptureResult(unavailable.status, unavailable.execution_id)
    try:
        return await _capture_reserved(
            session_factory,
            request,
            submission,
            grant,
            identity,
            statement,
            policy,
            partial(_AffinityLanding, statement_builder, adapter, credential_provider, statement, policy, timeout),
        )
    except BaseException as primary:
        await _finish_capture_failure(session_factory, request, grant, primary)
        raise


def _require_capture_capabilities(session_factory, statement_builder, adapter, credential_provider):
    """Reject unsupported resources before reserving an execution or opening a source."""

    if not callable(session_factory) or not callable(getattr(statement_builder, "build_statement", None)):
        raise SnowflakeCaptureError("capture requires a session factory and statement builder")
    if not callable(getattr(adapter, "open_bundle_landing", None)) or not callable(
        getattr(credential_provider, "load_key_pair", None)
    ):
        raise SnowflakeCaptureError("capture requires landing and credential capabilities")


async def _finish_capture_failure(session_factory, request, grant, primary):
    """Finish only the current fence after cleanup; never mask the primary failure."""

    terminal_state = "canceled" if isinstance(primary, asyncio.CancelledError) else "failed"
    try:
        async with session_factory() as session, session.begin():
            transition = await lifecycle.finish_execution(
                session,
                execution_id=grant.execution_id,
                fence=grant.fence,
                token=request.lease_token,
                terminal_state=terminal_state,
                terminal_reason="source_capture_failed" if terminal_state == "failed" else None,
            )
            if transition.state == "canceling" and terminal_state == "failed":
                await lifecycle.finish_execution(
                    session,
                    execution_id=grant.execution_id,
                    fence=grant.fence,
                    token=request.lease_token,
                    terminal_state="canceled",
                )
    except BaseException:
        primary.add_note("Capture execution finalization is unconfirmed; worker supervision is required.")


async def _capture_reserved(
    session_factory,
    request,
    submission,
    grant,
    identity,
    statement,
    policy,
    make_landing,
):
    """Use the granted reservation and drain source cleanup before failures escape."""

    if submission.capture_bundle_id is not None:
        return await _bound_result(
            session_factory, request, grant, submission.capture_bundle_id, identity, statement, policy
        )
    if submission.state != "queued" or grant.fence != 1:
        unbound_result = await _unbound_capture_result(session_factory, request, grant)
        return SnowflakeCaptureResult(unbound_result.status, unbound_result.execution_id)
    attempt = _Attempt(await _acquisition_deadline(session_factory, request, grant, policy))
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="source-capture")
    owner = make_landing(attempt)
    return await _acquire_claimed(session_factory, request, grant, identity, owner, executor, attempt)


def _require_landing_signature(adapter, statement, policy, timeout):
    try:
        inspect.signature(adapter.open_bundle_landing).bind(
            statement,
            None,
            part_limits=policy.part_limits,
            maximum_part_arrow_bytes=policy.maximum_part_arrow_bytes,
            timeout_seconds=timeout,
        )
    except (TypeError, ValueError) as exc:
        raise SnowflakeCaptureError("capture opener must accept the explicit landing contract") from exc
