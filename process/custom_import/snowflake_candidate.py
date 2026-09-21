# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""One narrow, replay-verified Snowflake candidate admission path."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from process.custom_import.capture import CaptureError, iter_records
from process.custom_import.capture_store import CaptureReceipt, register_capture_bundle
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.execution import create_execution, lease_token_sha256
from process.custom_import.family import _is_valid_npi
from process.custom_import.runner import (
    CandidateRunnerError,
    CandidateRunRequest,
    CandidateRunResult,
    run_candidate,
    validate_definition_canonical,
)
from process.custom_import.runner_registry import validate_revision_identity
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

__all__ = (
    "SnowflakeCandidateError",
    "SnowflakeCandidateRequest",
    "run_snowflake_candidate",
)


class SnowflakeCandidateError(ValueError):
    """A Snowflake acquisition cannot safely become a generic candidate."""


@dataclass(frozen=True)
class SnowflakeCandidateRequest:
    """One root-only Snowflake acquisition and its durable candidate identity."""

    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    definition: CustomImportDefinition
    acquisition: SnowflakeAcquisition
    idempotency_key: str
    lease_token: str | bytes | bytearray | memoryview


@dataclass(frozen=True)
class _PreparedCandidate:
    """Fully replayed source records and the connector-owned bundle receipt."""

    roots: tuple[Mapping[str, Any], ...]
    receipt: CaptureReceipt


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


def _validate_root_scope(definition: CustomImportDefinition, acquisition: SnowflakeAcquisition) -> tuple[str, ...]:
    """Bind this connector's one result stream to one exact root definition."""

    if (
        definition.child_collections
        or definition.child_fields
        or definition.aliases
        or definition.source_streams != (SNOWFLAKE_RESULT_STREAM,)
    ):
        raise SnowflakeCandidateError("Snowflake candidate requires one root-only result definition")
    request = acquisition.statement.request
    if request.definition_sha256 != definition.digest or request.schema_sha256 != definition.schema_digest:
        raise SnowflakeCandidateError("Snowflake acquisition definition identity does not match the registered scope")
    selected_field_ids = request.selected_field_ids
    root_field_ids = {field.field_id for field in definition.root_fields}
    if set(selected_field_ids) != root_field_ids:
        raise SnowflakeCandidateError("Snowflake selected fields do not exactly cover the registered root scope")
    return selected_field_ids


def _decode_roots(
    acquisition: SnowflakeAcquisition,
    *,
    entity_field: str,
    selected_field_ids: tuple[str, ...],
) -> tuple[Mapping[str, Any], ...]:
    """Replay every sealed partition without accepting a partial aggregate."""

    limits = acquisition.capture_limits
    roots: list[Mapping[str, Any]] = []
    record_count = 0
    expected_field_ids = set(selected_field_ids)
    for capture in acquisition.parquet_captures:
        try:
            partition_records = iter_records(capture, SNOWFLAKE_RESULT_STREAM, limits=limits)
            for decoded_record in partition_records:
                values = decoded_record.values
                if set(values) != expected_field_ids:
                    raise SnowflakeCandidateError("Snowflake result fields do not match the selected root fields")
                entity_value = values.get(entity_field)
                if not isinstance(entity_value, str) or not _is_valid_npi(entity_value):
                    raise SnowflakeCandidateError("Snowflake root entity values must be valid NPI strings")
                record_count += 1
                if record_count > limits.maximum_records:
                    raise SnowflakeCandidateError("Snowflake acquisition exceeds the aggregate record limit")
                roots.append(dict(values))
        except CaptureError as exc:
            raise SnowflakeCandidateError("Snowflake acquisition partition cannot be replayed") from exc
    return tuple(roots)


def _prepare_candidate(request: SnowflakeCandidateRequest) -> _PreparedCandidate:
    acquisition = _verified_acquisition(request.acquisition)
    selected_field_ids = _validate_root_scope(request.definition, acquisition)
    roots = _decode_roots(
        acquisition,
        entity_field=request.definition.entity_field,
        selected_field_ids=selected_field_ids,
    )
    return _PreparedCandidate(
        roots=roots,
        receipt=CaptureReceipt(
            stream_id=SNOWFLAKE_RESULT_STREAM.stream_id,
            source_snapshot_token=acquisition.manifest.source_snapshot_token,
            byte_count=sum(partition.content_bytes for partition in acquisition.manifest.result_partitions),
            content_sha256=acquisition.manifest.content_sha256,
            canonical_manifest=acquisition.manifest.canonical_manifest,
            manifest_sha256=acquisition.manifest.manifest_sha256,
        ),
    )


async def run_snowflake_candidate(
    session_factory: SessionFactory,
    request: SnowflakeCandidateRequest,
) -> CandidateRunResult:
    """Register one verified result bundle, commit its execution, then run it."""

    request = _validated_request(request)
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
        children_by_collection={},
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
            receipts=(prepared.receipt,),
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
            children_by_collection={},
            complete_scope=True,
        ),
    )
