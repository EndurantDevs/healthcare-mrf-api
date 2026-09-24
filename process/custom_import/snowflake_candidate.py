# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""One narrow, replay-verified Snowflake candidate admission path."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from process.custom_import.capture import (
    CaptureError,
    SealedCapture,
    _open_parquet_reader,
    _validate_parquet_envelope,
    _validated_parquet_schema,
    iter_records,
)
from process.custom_import.capture_store import (
    CaptureReceipt,
    ReplayableParquetCapture,
    load_replayable_parquet_bundle,
    register_capture_bundle,
    register_replayable_parquet_bundle,
)
from process.custom_import.definition import CustomImportDefinition, Field, SourceStream
from process.custom_import.execution import (
    ExecutionSubmission,
    LeaseGrant,
    bind_execution_capture_bundle,
    claim_execution,
    create_execution,
    finish_execution,
    heartbeat_execution,
    lease_token_sha256,
    reserve_execution,
)
from process.custom_import.family import (
    SourceSnapshotError,
    assemble_root_families,
    validate_source_snapshot_tokens,
)
from process.custom_import.runner import (
    CandidateRunnerError,
    CandidateRunRequest,
    CandidateRunResult,
    reject_duplicate_canonical_root_keys,
    run_candidate,
    run_claimed_candidate,
    validate_definition_canonical,
)
from process.custom_import.runner_registry import validate_revision_identity
from process.custom_import.runner_types import LeaseAuthorityLost as _LeaseLost
from process.custom_import.runner_types import SessionFactory
from process.custom_import.snowflake import (
    SNOWFLAKE_RESULT_STREAM,
    SnowflakeAcquisition,
    SnowflakeAcquisitionManifest,
    SnowflakeConnectorError,
    SnowflakeDeclaredColumn,
    SnowflakeReadRequest,
    SnowflakeReadStatement,
    SnowflakeRelation,
    SnowflakeResultPartitionManifest,
)
from process.custom_import.snowflake_bundle import (
    SnowflakeBundleAcquisition,
    SnowflakeBundleAcquisitionConnector,
    SnowflakeBundleError,
    SnowflakeBundleReplay,
    SnowflakeBundleRequest,
    SnowflakeBundleStatement,
    _validated_bundle_statement,
    prepare_bundle_replay,
    reconstruct_replayable_parquet_bundle,
    replayable_parquet_captures,
)

__all__ = (
    "SnowflakeBundleCandidateRequest",
    "SnowflakeCandidateError",
    "SnowflakeCandidateRequest",
    "bundle_request_identity_sha256",
    "run_snowflake_bundle_candidate",
    "run_snowflake_candidate",
)


class SnowflakeCandidateError(ValueError):
    """A Snowflake acquisition cannot safely become a generic candidate."""


@dataclass(frozen=True)
class SnowflakeCandidateRequest:
    """A legacy root acquisition with optional child replay data.

    Acquisitions retain the connector's fixed Parquet transport stream. Their
    roles bind them to the definition's sole root and optional child stream;
    this boundary cannot create a shared snapshot from independent reads.
    ``run_snowflake_candidate`` rejects child streams before storage; the
    one-statement bundle candidate path owns that later publication join.
    """

    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    definition: CustomImportDefinition
    acquisition: SnowflakeAcquisition
    idempotency_key: str
    lease_token: str | bytes | bytearray | memoryview
    child_acquisition: SnowflakeAcquisition | None = None


@dataclass(frozen=True)
class SnowflakeBundleCandidateRequest:
    """One configured multi-stream bundle and its durable candidate identity."""

    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    definition: CustomImportDefinition
    bundle_request: SnowflakeBundleRequest
    idempotency_key: str
    lease_token: str | bytes | bytearray | memoryview


@dataclass(frozen=True)
class _PreparedCandidate:
    """Fully replayed source records and the connector-owned bundle receipt."""

    roots: tuple[Mapping[str, Any], ...]
    children_by_collection: Mapping[str, tuple[Mapping[str, Any], ...]]
    receipts: tuple[CaptureReceipt, ...]


def _validated_request(request: object) -> SnowflakeCandidateRequest:
    if not isinstance(request, SnowflakeCandidateRequest):
        raise SnowflakeCandidateError("Snowflake candidate request is invalid")
    if not isinstance(request.definition, CustomImportDefinition):
        raise SnowflakeCandidateError("Snowflake candidate definition is invalid")
    try:
        validate_definition_canonical(request.definition)
        lease_token_sha256(request.lease_token)
    except (CandidateRunnerError, ValueError) as exc:
        raise SnowflakeCandidateError("Snowflake candidate request is invalid") from exc
    for value in (request.dataset_id, request.definition_revision_id, request.schema_revision_id):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise SnowflakeCandidateError("Snowflake candidate identifiers are invalid")
    return request


def _validated_bundle_request(request: object) -> SnowflakeBundleCandidateRequest:
    if not isinstance(request, SnowflakeBundleCandidateRequest):
        raise SnowflakeCandidateError("Snowflake bundle candidate request is invalid")
    if not isinstance(request.definition, CustomImportDefinition) or not isinstance(
        request.bundle_request, SnowflakeBundleRequest
    ):
        raise SnowflakeCandidateError("Snowflake bundle candidate definition is invalid")
    try:
        validate_definition_canonical(request.definition)
        lease_token_sha256(request.lease_token)
    except (CandidateRunnerError, ValueError) as exc:
        raise SnowflakeCandidateError("Snowflake bundle candidate request is invalid") from exc
    for value in (request.dataset_id, request.definition_revision_id, request.schema_revision_id):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise SnowflakeCandidateError("Snowflake bundle candidate identifiers are invalid")
    if request.bundle_request.definition != request.definition:
        raise SnowflakeCandidateError("Snowflake bundle candidate definition does not match its configured bundle")
    return request


def _verified_acquisition(acquisition: object) -> SnowflakeAcquisition:
    if not isinstance(acquisition, SnowflakeAcquisition):
        raise SnowflakeCandidateError("Snowflake acquisition is invalid")
    try:
        supplied_statement = acquisition.statement
        supplied_request = supplied_statement.request
        supplied_manifest = acquisition.manifest
        request = SnowflakeReadRequest(
            relation=SnowflakeRelation(
                database=supplied_request.relation.database,
                schema=supplied_request.relation.schema,
                name=supplied_request.relation.name,
            ),
            selected_columns=tuple(
                SnowflakeDeclaredColumn(
                    field_id=column.field_id,
                    column_identifier=column.column_identifier,
                )
                for column in supplied_request.selected_columns
            ),
            definition_sha256=supplied_request.definition_sha256,
            schema_sha256=supplied_request.schema_sha256,
        )
        statement = SnowflakeReadStatement(request=request)
        manifest = SnowflakeAcquisitionManifest(
            request_sha256=supplied_manifest.request_sha256,
            statement_sha256=supplied_manifest.statement_sha256,
            source_snapshot_token=supplied_manifest.source_snapshot_token,
            schema_fingerprint=supplied_manifest.schema_fingerprint,
            result_partitions=tuple(
                SnowflakeResultPartitionManifest(
                    ordinal=partition.ordinal,
                    content_bytes=partition.content_bytes,
                    content_sha256=partition.content_sha256,
                )
                for partition in supplied_manifest.result_partitions
            ),
            content_sha256=supplied_manifest.content_sha256,
        )
        if request != supplied_request or statement != supplied_statement or manifest != supplied_manifest:
            raise SnowflakeConnectorError("acquisition contains a stale nested identity seal")
        return SnowflakeAcquisition(
            statement=statement,
            manifest=manifest,
            parquet_captures=acquisition.parquet_captures,
            capture_limits=acquisition.capture_limits,
        )
    except (AttributeError, CaptureError, SnowflakeConnectorError, TypeError, ValueError) as exc:
        raise SnowflakeCandidateError("Snowflake acquisition seal is invalid") from exc


def _bound_acquisitions(
    request: SnowflakeCandidateRequest,
) -> tuple[tuple[SourceStream, SnowflakeAcquisition], ...]:
    """Require exactly one acquisition for each supported declared stream."""

    definition = request.definition
    if len(definition.child_collections) > 1:
        raise SnowflakeCandidateError("Snowflake candidate supports at most one declared child collection")
    if bool(definition.child_collections) != (request.child_acquisition is not None):
        raise SnowflakeCandidateError("Snowflake acquisitions must exactly cover the declared source streams")
    acquisitions = []
    for stream in definition.source_streams:
        if (
            stream.format != SNOWFLAKE_RESULT_STREAM.format
            or stream.compression != SNOWFLAKE_RESULT_STREAM.compression
            or stream.record_path is not None
            or stream.snapshot_token != SNOWFLAKE_RESULT_STREAM.snapshot_token
        ):
            raise SnowflakeCandidateError("Snowflake source streams must use the fixed Parquet result shape")
        acquisition = request.acquisition if stream.record_kind == "root" else request.child_acquisition
        acquisitions.append((stream, _verified_acquisition(acquisition)))
    try:
        validate_source_snapshot_tokens(
            definition,
            {stream.stream_id: (acquisition.manifest.source_snapshot_token,) for stream, acquisition in acquisitions},
        )
    except SourceSnapshotError as exc:
        raise SnowflakeCandidateError("Snowflake acquisitions must share one exact snapshot token") from exc
    return tuple(acquisitions)


def _validate_stream_scope(
    definition: CustomImportDefinition,
    stream: SourceStream,
    acquisition: SnowflakeAcquisition,
) -> tuple[Field, ...]:
    """Bind selected columns and aliases to the exact declared record scope."""

    request = acquisition.statement.request
    if request.definition_sha256 != definition.digest or request.schema_sha256 != definition.schema_digest:
        raise SnowflakeCandidateError("Snowflake acquisition definition identity does not match the registered scope")
    fields = tuple(field for field in definition.fields if field.collection == stream.child_collection)
    field_ids = {field.field_id for field in fields}
    if set(request.selected_field_ids) != field_ids:
        raise SnowflakeCandidateError("Snowflake selected fields do not exactly cover the registered stream scope")
    selected_by_column = {column.column_identifier: column.field_id for column in request.selected_columns}
    if len(selected_by_column) != len(request.selected_columns):
        raise SnowflakeCandidateError("Snowflake selected source columns must be unique")
    for alias in definition.aliases:
        if alias.stream_id == stream.stream_id and selected_by_column.get(alias.source_label) != alias.field_id:
            raise SnowflakeCandidateError("Snowflake source aliases do not match the selected column bindings")
    return fields


def _validate_partition_schema(
    acquisition: SnowflakeAcquisition,
    capture: SealedCapture,
    fields_by_id: Mapping[str, Field],
) -> None:
    """Check declared column identity and types even for a zero-row partition."""

    _validate_parquet_envelope(capture.payload)
    with _open_parquet_reader(capture.payload, acquisition.capture_limits) as parquet_reader:
        schema = parquet_reader.schema_arrow
        labels = _validated_parquet_schema(schema, acquisition.capture_limits)
    if labels != acquisition.statement.request.selected_field_ids:
        raise SnowflakeCandidateError("Snowflake result schema does not match the selected fields")
    for column in schema:
        if not _is_column_type_valid(column.type, fields_by_id[column.name]):
            raise SnowflakeCandidateError("Snowflake result schema type does not match the declared field")


def _is_column_type_valid(column_type: pa.DataType, field: Field) -> bool:
    """Match the shared scalar domain without coercing any source values."""

    if pa.types.is_null(column_type):
        return field.nullable
    is_string = pa.types.is_string(column_type) or pa.types.is_large_string(column_type)
    if field.value_type == "string":
        return is_string
    if field.value_type == "integer":
        return pa.types.is_integer(column_type)
    if field.value_type == "decimal":
        return is_string or pa.types.is_integer(column_type) or pa.types.is_decimal(column_type)
    if field.value_type == "boolean":
        return pa.types.is_boolean(column_type)
    return False


def _decode_records(
    acquisition: SnowflakeAcquisition,
    *,
    fields: tuple[Field, ...],
) -> tuple[Mapping[str, Any], ...]:
    """Replay every sealed partition without accepting a partial aggregate."""

    limits = acquisition.capture_limits
    decoded_records: list[Mapping[str, Any]] = []
    record_count = 0
    fields_by_id = {field.field_id: field for field in fields}
    expected_field_ids = set(fields_by_id)
    if not acquisition.parquet_captures:
        raise SnowflakeCandidateError("Snowflake acquisition requires a schema-bearing Parquet partition")
    for capture in acquisition.parquet_captures:
        try:
            _validate_partition_schema(acquisition, capture, fields_by_id)
            partition_records = iter_records(capture, SNOWFLAKE_RESULT_STREAM, limits=limits)
            for decoded_record in partition_records:
                values_by_field = decoded_record.values
                if set(values_by_field) != expected_field_ids:
                    raise SnowflakeCandidateError("Snowflake result fields do not match the selected fields")
                record_count += 1
                if record_count > limits.maximum_records:
                    raise SnowflakeCandidateError("Snowflake acquisition exceeds the aggregate record limit")
                decoded_records.append(dict(values_by_field))
        except CaptureError as exc:
            raise SnowflakeCandidateError("Snowflake acquisition partition cannot be replayed") from exc
    return tuple(decoded_records)


def _prepare_candidate(request: SnowflakeCandidateRequest) -> _PreparedCandidate:
    roots = ()
    children_by_collection = {}
    receipts = []
    for stream, acquisition in _bound_acquisitions(request):
        fields = _validate_stream_scope(request.definition, stream, acquisition)
        stream_records = _decode_records(
            acquisition,
            fields=fields,
        )
        if stream.record_kind == "root":
            roots = stream_records
        else:
            children_by_collection[stream.child_collection] = stream_records
        receipts.append(_capture_receipt(stream, acquisition))
    return _PreparedCandidate(roots=roots, children_by_collection=children_by_collection, receipts=tuple(receipts))


def _capture_receipt(stream: SourceStream, acquisition: SnowflakeAcquisition) -> CaptureReceipt:
    """Retain the connector seal under its exact definition-owned stream."""

    return CaptureReceipt(
        stream_id=stream.stream_id,
        source_snapshot_token=acquisition.manifest.source_snapshot_token,
        byte_count=sum(partition.content_bytes for partition in acquisition.manifest.result_partitions),
        content_sha256=acquisition.manifest.content_sha256,
        canonical_manifest=acquisition.manifest.canonical_manifest,
        manifest_sha256=acquisition.manifest.manifest_sha256,
    )


async def run_snowflake_candidate(
    session_factory: SessionFactory,
    request: SnowflakeCandidateRequest,
) -> CandidateRunResult:
    """Register and run one legacy single-stream Snowflake candidate."""

    request = _validated_request(request)
    if request.definition.child_collections:
        raise SnowflakeCandidateError(
            "multi-stream Snowflake publication requires the one-statement bundle candidate path"
        )
    if not callable(session_factory):
        raise SnowflakeCandidateError("Snowflake candidate requires a session factory")
    prepared = _prepare_candidate(request)
    validation_request = CandidateRunRequest(
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        execution_id=1,
        lease_token=request.lease_token,
        definition=request.definition,
        roots=prepared.roots,
        children_by_collection=prepared.children_by_collection,
        complete_scope=True,
    )
    async with session_factory() as session, session.begin():
        try:
            await validate_revision_identity(session, validation_request)
        except CandidateRunnerError as exc:
            raise SnowflakeCandidateError("Snowflake registered definition does not match the acquisition") from exc
        bundle = await register_capture_bundle(
            session,
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
            receipts=prepared.receipts,
        )
        submission = await create_execution(
            session,
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
            idempotency_key=request.idempotency_key,
            mechanism="local",
            capture_bundle_id=bundle.capture_bundle_id,
        )
    return await run_candidate(
        session_factory,
        CandidateRunRequest(
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
            execution_id=submission.execution_id,
            lease_token=request.lease_token,
            definition=request.definition,
            roots=prepared.roots,
            children_by_collection=prepared.children_by_collection,
            complete_scope=True,
        ),
    )


_BUNDLE_REQUEST_IDENTITY_DOMAIN = b"custom-import/snowflake-bundle-request-identity/v1\x00"


def _bundle_connector(
    connector: object,
) -> tuple[Callable[[SnowflakeBundleRequest], object], Callable[..., object]]:
    """Require the pure preflight and prepared-acquisition seam before reservation."""

    build_statement = getattr(connector, "build_statement", None)
    acquire = getattr(connector, "acquire", None)
    if not callable(build_statement) or not callable(acquire):
        raise SnowflakeCandidateError("Snowflake bundle candidate requires a bundle acquisition connector")
    return build_statement, acquire


def _prepared_bundle_statement(
    build_statement: Callable[[SnowflakeBundleRequest], object],
    request: SnowflakeBundleCandidateRequest,
) -> SnowflakeBundleStatement:
    """Build the complete approved source mapping without contacting a source."""

    try:
        statement = _validated_bundle_statement(build_statement(request.bundle_request))
    except (SnowflakeBundleError, TypeError, ValueError) as exc:
        raise SnowflakeCandidateError("Snowflake bundle statement cannot be prepared") from exc
    if statement.request != request.bundle_request:
        raise SnowflakeCandidateError("Snowflake bundle statement does not match the configured bundle request")
    return statement


def _requires_prepared_statement(
    acquire: Callable[..., object],
    request: SnowflakeBundleCandidateRequest,
    statement: SnowflakeBundleStatement,
) -> None:
    """Reject an incompatible connector before it can leave a running reservation."""

    try:
        inspect.signature(acquire).bind(request.bundle_request, prepared_statement=statement)
    except (TypeError, ValueError) as exc:
        raise SnowflakeCandidateError("Snowflake bundle acquisition must accept a prepared statement") from exc


def bundle_request_identity_sha256(
    request: SnowflakeBundleRequest,
    statement: SnowflakeBundleStatement,
) -> bytes:
    """Bind idempotency to a validated request and its resolved SQL mapping."""

    try:
        statement = _validated_bundle_statement(statement)
    except SnowflakeBundleError as exc:
        raise SnowflakeCandidateError("Snowflake bundle statement identity is invalid") from exc
    if statement.request != request:
        raise SnowflakeCandidateError("Snowflake bundle statement does not match the configured request")
    try:
        request_digest = bytes.fromhex(request.request_sha256)
        statement_digest = bytes.fromhex(statement.statement_sha256)
    except ValueError as exc:
        raise SnowflakeCandidateError("Snowflake bundle statement identity is invalid") from exc
    digest = hashlib.sha256(_BUNDLE_REQUEST_IDENTITY_DOMAIN)
    digest.update(request_digest)
    digest.update(statement_digest)
    return digest.digest()


def _acquired_bundle(
    acquire: Callable[..., object],
    request: SnowflakeBundleCandidateRequest,
    prepared_statement: SnowflakeBundleStatement,
) -> SnowflakeBundleAcquisition:
    try:
        acquisition = acquire(request.bundle_request, prepared_statement=prepared_statement)
    except SnowflakeConnectorError as exc:
        raise SnowflakeCandidateError("Snowflake bundle cannot be acquired") from exc
    if not isinstance(acquisition, SnowflakeBundleAcquisition) or acquisition.statement != prepared_statement:
        raise SnowflakeCandidateError("Snowflake acquired bundle does not match the prepared bundle statement")
    return acquisition


def _prepared_bundle_captures(acquisition: SnowflakeBundleAcquisition) -> tuple[ReplayableParquetCapture, ...]:
    """Decode and seal a closed acquisition outside the event loop."""

    prepare_bundle_replay(acquisition)
    return replayable_parquet_captures(acquisition)


def _bundle_candidate_run_request(
    request: SnowflakeBundleCandidateRequest,
    execution_id: int,
    *,
    roots: tuple[Mapping[str, Any], ...],
    children_by_collection: Mapping[str, tuple[Mapping[str, Any], ...]],
) -> CandidateRunRequest:
    return CandidateRunRequest(
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        execution_id=execution_id,
        lease_token=request.lease_token,
        definition=request.definition,
        roots=roots,
        children_by_collection=children_by_collection,
        complete_scope=True,
    )


async def _register_bundle_captures(
    session_factory: SessionFactory,
    request: SnowflakeBundleCandidateRequest,
    grant: LeaseGrant,
    captures: tuple[ReplayableParquetCapture, ...],
) -> int | None:
    """Retain a newly acquired bundle only while its claimant owns the fence."""

    validation_request = _bundle_candidate_run_request(
        request,
        grant.execution_id,
        roots=(),
        children_by_collection={},
    )
    try:
        async with session_factory() as session, session.begin():
            try:
                await validate_revision_identity(session, validation_request)
            except CandidateRunnerError as exc:
                raise SnowflakeCandidateError("Snowflake registered definition does not match the bundle") from exc
            capture_bundle = await register_replayable_parquet_bundle(
                session,
                dataset_id=request.dataset_id,
                definition_revision_id=request.definition_revision_id,
                schema_revision_id=request.schema_revision_id,
                captures=captures,
            )
            binding = await bind_execution_capture_bundle(
                session,
                execution_id=grant.execution_id,
                dataset_id=request.dataset_id,
                definition_revision_id=request.definition_revision_id,
                schema_revision_id=request.schema_revision_id,
                capture_bundle_id=capture_bundle.capture_bundle_id,
                fence=grant.fence,
                token=request.lease_token,
            )
            if binding is None:
                raise _LeaseLost("Snowflake bundle candidate lease is no longer current")
            return binding.capture_bundle_id
    except _LeaseLost:
        return None


async def _reserve_bundle_execution(
    session_factory: SessionFactory,
    request: SnowflakeBundleCandidateRequest,
    request_identity_sha256: bytes,
) -> tuple[ExecutionSubmission, LeaseGrant | None]:
    """Validate, reserve, and claim the current owner before source contact."""

    validation_request = _bundle_candidate_run_request(
        request,
        1,
        roots=(),
        children_by_collection={},
    )
    async with session_factory() as session, session.begin():
        try:
            await validate_revision_identity(session, validation_request)
        except CandidateRunnerError as exc:
            raise SnowflakeCandidateError("Snowflake registered definition does not match the bundle") from exc
        submission = await reserve_execution(
            session,
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
            idempotency_key=request.idempotency_key,
            mechanism="local",
            request_identity_sha256=request_identity_sha256,
        )
        grant = await claim_execution(session, execution_id=submission.execution_id, token=request.lease_token)
    return submission, grant


async def _renew_bundle_lease(
    session_factory: SessionFactory,
    request: SnowflakeBundleCandidateRequest,
    grant: LeaseGrant,
) -> LeaseGrant | None:
    """Renew the source owner's exact fence immediately around source work."""

    async with session_factory() as session, session.begin():
        return await heartbeat_execution(
            session,
            execution_id=grant.execution_id,
            fence=grant.fence,
            token=request.lease_token,
        )


async def _load_bundle_replay(
    session_factory: SessionFactory,
    request: SnowflakeBundleCandidateRequest,
    prepared_statement: SnowflakeBundleStatement,
    capture_bundle_id: int,
) -> SnowflakeBundleReplay:
    """Reload and verify the durable bundle without reaching its source connector."""

    async with session_factory() as session:
        durable_captures = await load_replayable_parquet_bundle(
            session,
            capture_bundle_id=capture_bundle_id,
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
        )
    try:
        return await asyncio.to_thread(reconstruct_replayable_parquet_bundle, prepared_statement, durable_captures)
    except SnowflakeBundleError as exc:
        raise SnowflakeCandidateError("Snowflake durable bundle cannot be replayed") from exc


async def _reservation_unavailable_result(
    session_factory: SessionFactory,
    request: SnowflakeBundleCandidateRequest,
    grant: LeaseGrant,
) -> CandidateRunResult:
    """Acknowledge a winning cancellation or report an unavailable lease."""

    async with session_factory() as session, session.begin():
        transition = await finish_execution(
            session,
            execution_id=grant.execution_id,
            fence=grant.fence,
            token=request.lease_token,
            terminal_state="canceled",
        )
    status = "canceled" if transition.state == "canceled" else "lease_lost"
    return CandidateRunResult(status=status, execution_id=grant.execution_id)


async def _unbound_capture_result(
    session_factory: SessionFactory,
    request: SnowflakeBundleCandidateRequest,
    grant: LeaseGrant,
) -> CandidateRunResult:
    """Fail closed when a resumed reservation lacks a durable source capture."""

    async with session_factory() as session, session.begin():
        transition = await finish_execution(
            session,
            execution_id=grant.execution_id,
            fence=grant.fence,
            token=request.lease_token,
            terminal_state="failed",
            terminal_reason="unbound_source_capture",
        )
    if transition.state == "failed":
        raise SnowflakeCandidateError("Snowflake bundle has an unbound source capture; submit a new execution")
    return await _reservation_unavailable_result(session_factory, request, grant)


async def _fence_source_capture_failure(
    session_factory: SessionFactory,
    request: SnowflakeBundleCandidateRequest,
    grant: LeaseGrant,
) -> None:
    """Fail the current owner without replacing a cancellation or newer lease."""

    async with session_factory() as session, session.begin():
        await finish_execution(
            session,
            execution_id=grant.execution_id,
            fence=grant.fence,
            token=request.lease_token,
            terminal_state="failed",
            terminal_reason="source_capture_failed",
        )


async def _run_claimed_bundle_candidate(
    session_factory: SessionFactory,
    request: SnowflakeBundleCandidateRequest,
    grant: LeaseGrant,
    replay: SnowflakeBundleReplay,
) -> CandidateRunResult:
    """Pass the durable replay through the existing generic fenced runner."""

    candidate_request = _bundle_candidate_run_request(
        request,
        grant.execution_id,
        roots=replay.roots,
        children_by_collection=replay.children_by_collection,
    )
    admitted = reject_duplicate_canonical_root_keys(
        candidate_request.definition,
        assemble_root_families(
            candidate_request.definition,
            candidate_request.roots,
            candidate_request.children_by_collection,
        ),
    )
    return await run_claimed_candidate(session_factory, candidate_request, grant, admitted)


async def run_snowflake_bundle_candidate(
    session_factory: SessionFactory,
    connector: SnowflakeBundleAcquisitionConnector,
    request: SnowflakeBundleCandidateRequest,
) -> CandidateRunResult:
    """Fence one reservation, then acquire or replay it into the generic runner."""

    request = _validated_bundle_request(request)
    if not callable(session_factory):
        raise SnowflakeCandidateError("Snowflake bundle candidate requires a session factory")
    build_statement, acquire = _bundle_connector(connector)
    prepared_statement = _prepared_bundle_statement(build_statement, request)
    _requires_prepared_statement(acquire, request, prepared_statement)
    request_identity_sha256 = bundle_request_identity_sha256(request.bundle_request, prepared_statement)
    submission, grant = await _reserve_bundle_execution(session_factory, request, request_identity_sha256)
    if grant is None:
        return CandidateRunResult(status="not_claimed", execution_id=submission.execution_id)
    if grant.state != "running":
        return await _reservation_unavailable_result(session_factory, request, grant)
    capture_bundle_id = submission.capture_bundle_id
    if capture_bundle_id is None:
        if submission.state != "queued" or grant.fence != 1:
            return await _unbound_capture_result(session_factory, request, grant)
        renewed_grant = await _renew_bundle_lease(session_factory, request, grant)
        if renewed_grant is None or renewed_grant.state != "running":
            return await _reservation_unavailable_result(session_factory, request, grant)
        grant = renewed_grant
        try:
            acquisition = await asyncio.to_thread(_acquired_bundle, acquire, request, prepared_statement)
        except Exception:
            await _fence_source_capture_failure(session_factory, request, grant)
            raise
        renewed_grant = await _renew_bundle_lease(session_factory, request, grant)
        if renewed_grant is None or renewed_grant.state != "running":
            return await _reservation_unavailable_result(session_factory, request, grant)
        grant = renewed_grant
        try:
            captures = await asyncio.to_thread(_prepared_bundle_captures, acquisition)
        except Exception as exc:
            await _fence_source_capture_failure(session_factory, request, grant)
            if isinstance(exc, SnowflakeBundleError):
                raise SnowflakeCandidateError("Snowflake bundle acquisition cannot be retained") from exc
            raise
        renewed_grant = await _renew_bundle_lease(session_factory, request, grant)
        if renewed_grant is None or renewed_grant.state != "running":
            return await _reservation_unavailable_result(session_factory, request, grant)
        grant = renewed_grant
        capture_bundle_id = await _register_bundle_captures(session_factory, request, grant, captures)
        if capture_bundle_id is None:
            return await _reservation_unavailable_result(session_factory, request, grant)
    replay = await _load_bundle_replay(session_factory, request, prepared_statement, capture_bundle_id)
    return await _run_claimed_bundle_candidate(session_factory, request, grant, replay)
