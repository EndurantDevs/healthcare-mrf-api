# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Offline replay and durable handoff for one-statement Snowflake bundles."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from io import BytesIO
from types import MappingProxyType
from typing import Any

import pyarrow as pa

from process.custom_import.capture import (
    CaptureError,
    CaptureLimits,
    SealedCapture,
    _open_parquet_reader,
    _validate_parquet_envelope,
    _validated_parquet_schema,
    capture_stream,
    iter_records,
)
from process.custom_import.capture_store import CaptureReceipt, ReplayableParquetCapture
from process.custom_import.definition import CustomImportDefinition, Field, SourceStream
from process.custom_import.family import (
    MAX_SCALAR_INTEGER,
    MIN_SCALAR_INTEGER,
    SourceSnapshotError,
    validate_source_snapshot_tokens,
)
from process.custom_import.snowflake import (
    MAX_RESULT_PARTITIONS,
    SnowflakeConnectorError,
    SnowflakeDeclaredColumn,
    SnowflakeRelation,
    SnowflakeResultColumn,
)
from process.custom_import.snowflake_bundle import (
    BUNDLE_CONNECTOR_CONTRACT,
    MAX_BUNDLE_PARTITIONS,
    SnowflakeBundleAcquisition,
    SnowflakeBundleBinding,
    SnowflakeBundleEncoding,
    SnowflakeBundleError,
    SnowflakeBundleRequest,
    SnowflakeBundleStatement,
    SnowflakeBundleStreamCapture,
    _canonical_identity,
    _capture_limits_document,
    _durable_bundle_receipt,
    _DurableBundleReceipt,
    _field_id,
    _remaining_partition_limits,
    _snapshot_token,
    _stream_content_sha256,
)


@dataclass(frozen=True)
class SnowflakeBundleReplayStream:
    """One fully replayed stream and its future durable-Parquet handoff facts."""

    stream_id: str
    records: tuple[Mapping[str, Any], ...]
    receipt: CaptureReceipt
    parts: tuple[bytes, ...] = field(repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "stream_id", _field_id(self.stream_id, "bundle replay stream id"))
        if not isinstance(self.records, tuple) or not all(isinstance(record, Mapping) for record in self.records):
            raise SnowflakeBundleError("bundle replay records must be a tuple of mappings")
        if not isinstance(self.receipt, CaptureReceipt) or self.receipt.stream_id != self.stream_id:
            raise SnowflakeBundleError("bundle replay receipt does not match its stream")
        if (
            not isinstance(self.parts, tuple)
            or not self.parts
            or not all(isinstance(part, bytes) and part for part in self.parts)
        ):
            raise SnowflakeBundleError("bundle replay parts must be non-empty bytes")
        if sum(len(part) for part in self.parts) != self.receipt.byte_count:
            raise SnowflakeBundleError("bundle replay parts do not match their receipt byte count")
        if _stream_content_sha256(self.stream_id, self.parts) != self.receipt.content_sha256:
            raise SnowflakeBundleError("bundle replay parts do not match their receipt digest")
        object.__setattr__(
            self,
            "records",
            tuple(MappingProxyType(dict(record)) for record in self.records),
        )


@dataclass(frozen=True)
class SnowflakeBundleReplay:
    """Replayable source records prepared from sealed bytes without a connector."""

    definition: CustomImportDefinition
    source_snapshot_token: str
    streams: tuple[SnowflakeBundleReplayStream, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.definition, CustomImportDefinition):
            raise SnowflakeBundleError("bundle replay requires a custom-import definition")
        object.__setattr__(self, "source_snapshot_token", _snapshot_token(self.source_snapshot_token))
        if not isinstance(self.streams, tuple) or not all(
            isinstance(stream, SnowflakeBundleReplayStream) for stream in self.streams
        ):
            raise SnowflakeBundleError("bundle replay streams are invalid")
        expected_stream_ids = tuple(stream.stream_id for stream in self.definition.source_streams)
        if tuple(stream.stream_id for stream in self.streams) != expected_stream_ids:
            raise SnowflakeBundleError("bundle replay streams must retain declared order")
        if any(stream.receipt.source_snapshot_token != self.source_snapshot_token for stream in self.streams):
            raise SnowflakeBundleError("bundle replay streams do not share their acquisition evidence")

    @property
    def roots(self) -> tuple[Mapping[str, Any], ...]:
        """Return root records ready for a later lifecycle-owned candidate join."""

        root_stream_id = next(
            stream.stream_id for stream in self.definition.source_streams if stream.record_kind == "root"
        )
        return next(stream.records for stream in self.streams if stream.stream_id == root_stream_id)

    @property
    def children_by_collection(self) -> Mapping[str, tuple[Mapping[str, Any], ...]]:
        """Return every declared child stream, including an explicitly empty one."""

        stream_by_id = {stream.stream_id: stream for stream in self.streams}
        return MappingProxyType(
            {
                source_stream.child_collection: stream_by_id[source_stream.stream_id].records
                for source_stream in self.definition.source_streams
                if source_stream.record_kind == "child"
            }
        )


def _rebuilt_bundle_statement(
    supplied_statement: object,
) -> tuple[SnowflakeBundleRequest, SnowflakeBundleStatement]:
    supplied_request = supplied_statement.request
    request = SnowflakeBundleRequest(
        definition=supplied_request.definition,
        bindings=tuple(
            SnowflakeBundleBinding(
                stream_id=binding.stream_id,
                relation=SnowflakeRelation(*binding.relation.parts),
                source_snapshot_token_relation=SnowflakeRelation(*binding.source_snapshot_token_relation.parts),
                selected_field_ids=tuple(binding.selected_field_ids),
                semantic_token_metadata_key=binding.semantic_token_metadata_key,
            )
            for binding in supplied_request.bindings
        ),
        encoding=SnowflakeBundleEncoding(
            supplied_request.encoding.partition_rows,
            supplied_request.encoding.parquet_compression,
        ),
        capture_limits=supplied_request.capture_limits,
    )
    statement = SnowflakeBundleStatement(
        request=request,
        selected_columns_by_stream=tuple(
            tuple(SnowflakeDeclaredColumn(column.field_id, column.column_identifier) for column in selected_columns)
            for selected_columns in supplied_statement.selected_columns_by_stream
        ),
        source_snapshot_token_columns_by_stream=tuple(
            SnowflakeDeclaredColumn(column.field_id, column.column_identifier)
            for column in supplied_statement.source_snapshot_token_columns_by_stream
        ),
    )
    return request, statement


def _rebuilt_stream_captures(
    acquisition: SnowflakeBundleAcquisition,
) -> tuple[SnowflakeBundleStreamCapture, ...]:
    return tuple(
        SnowflakeBundleStreamCapture(
            stream_id=stream_capture.stream_id,
            schema=tuple(
                SnowflakeResultColumn(column.field_id, column.source_type, column.nullable)
                for column in stream_capture.schema
            ),
            captures=tuple(stream_capture.captures),
        )
        for stream_capture in acquisition.stream_captures
    )


def _verified_bundle_acquisition(candidate_acquisition: object) -> SnowflakeBundleAcquisition:
    """Rebuild nested seals before a later durable handoff trusts replay bytes."""

    if not isinstance(candidate_acquisition, SnowflakeBundleAcquisition):
        raise SnowflakeBundleError("bundle acquisition is invalid")
    try:
        supplied_statement = candidate_acquisition.statement
        request, statement = _rebuilt_bundle_statement(supplied_statement)
        stream_captures = _rebuilt_stream_captures(candidate_acquisition)
        rebuilt = SnowflakeBundleAcquisition(
            statement=statement,
            source_snapshot_token=candidate_acquisition.source_snapshot_token,
            stream_captures=stream_captures,
            capture_limits=candidate_acquisition.capture_limits,
            diagnostic_query_id=candidate_acquisition.diagnostic_query_id,
        )
        if request != supplied_statement.request or statement != supplied_statement or rebuilt != candidate_acquisition:
            raise SnowflakeBundleError("bundle acquisition contains a stale nested identity seal")
        return rebuilt
    except (AttributeError, CaptureError, SnowflakeConnectorError, TypeError, ValueError) as exc:
        if isinstance(exc, SnowflakeBundleError):
            raise
        raise SnowflakeBundleError("bundle acquisition seal is invalid") from exc


def _is_bundle_column_type_valid(column_type: pa.DataType, field: Field) -> bool:
    """Match the shared scalar domain before a durable bundle can be registered."""

    if pa.types.is_null(column_type):
        return field.nullable
    is_string = pa.types.is_string(column_type) or pa.types.is_large_string(column_type)
    if field.value_type == "string":
        return is_string
    if field.value_type == "integer":
        return pa.types.is_integer(column_type) or (pa.types.is_decimal128(column_type) and column_type.scale == 0)
    if field.value_type == "decimal":
        return is_string or pa.types.is_integer(column_type) or pa.types.is_decimal(column_type)
    if field.value_type == "boolean":
        return pa.types.is_boolean(column_type)
    return False


def _normalized_integer_replay_values(
    record_by_field: Mapping[str, Any],
    fields: tuple[Field, ...],
) -> dict[str, Any]:
    """Convert scale-zero Snowflake decimal transport values at the replay boundary."""

    normalized_by_field = dict(record_by_field)
    for field in fields:
        value = normalized_by_field[field.field_id]
        if field.value_type != "integer" or not isinstance(value, Decimal):
            continue
        if not value.is_finite() or value != value.to_integral_value():
            raise SnowflakeBundleError("bundle replay integer result is invalid")
        integer_value = int(value)
        if not MIN_SCALAR_INTEGER <= integer_value <= MAX_SCALAR_INTEGER:
            raise SnowflakeBundleError("bundle replay integer result exceeds the signed 64-bit range")
        normalized_by_field[field.field_id] = integer_value
    return normalized_by_field


def _validate_replay_partition_schema(
    capture: SealedCapture,
    *,
    fields: tuple[Field, ...],
    limits: CaptureLimits,
) -> None:
    _validate_parquet_envelope(capture.payload)
    with _open_parquet_reader(capture.payload, limits) as parquet_reader:
        schema = parquet_reader.schema_arrow
        labels = _validated_parquet_schema(schema, limits)
    expected_field_ids = tuple(field.field_id for field in fields)
    if labels != expected_field_ids:
        raise SnowflakeBundleError("bundle replay schema does not match the declared stream fields")
    fields_by_id = {field.field_id: field for field in fields}
    if any(not _is_bundle_column_type_valid(column.type, fields_by_id[column.name]) for column in schema):
        raise SnowflakeBundleError("bundle replay schema type does not match the declared field")


def _replay_stream_records(
    stream_capture: SnowflakeBundleStreamCapture,
    *,
    stream: SourceStream,
    fields: tuple[Field, ...],
    limits: CaptureLimits,
    decoded_bytes: int,
) -> tuple[tuple[Mapping[str, Any], ...], int]:
    expected_field_ids = tuple(field.field_id for field in fields)
    if tuple(column.field_id for column in stream_capture.schema) != expected_field_ids:
        raise SnowflakeBundleError("bundle result schema does not match the declared stream fields")
    stream_records: list[Mapping[str, Any]] = []
    for capture in stream_capture.captures:
        try:
            _validate_replay_partition_schema(capture, fields=fields, limits=limits)
            for decoded_record in iter_records(capture, stream, limits=limits):
                record_by_field = dict(decoded_record.values)
                if tuple(record_by_field) != expected_field_ids:
                    raise SnowflakeBundleError("bundle replay record fields do not match the declared stream fields")
                stream_records.append(_normalized_integer_replay_values(record_by_field, fields))
                if len(stream_records) > limits.maximum_records:
                    raise SnowflakeBundleError("bundle replay stream exceeds the aggregate record limit")
            decoded_bytes = _aggregate_parquet_arrow_bytes(capture, limits=limits, decoded_bytes=decoded_bytes)
        except CaptureError as exc:
            raise SnowflakeBundleError("bundle replay partition cannot be decoded") from exc
    return tuple(stream_records), decoded_bytes


def _aggregate_parquet_arrow_bytes(
    capture: SealedCapture,
    *,
    limits: CaptureLimits,
    decoded_bytes: int,
) -> int:
    """Bound actual Arrow output across replay parts without materializing a table."""

    has_exceeded_limit = False
    try:
        _validate_parquet_envelope(capture.payload)
        with _open_parquet_reader(capture.payload, limits) as parquet_reader:
            for record_batch in parquet_reader.iter_batches(
                batch_size=1_024,
                use_threads=False,
                use_pandas_metadata=False,
            ):
                batch_bytes = record_batch.nbytes
                if isinstance(batch_bytes, bool) or not isinstance(batch_bytes, int) or batch_bytes < 0:
                    raise CaptureError("Parquet source payload has invalid decoded batch bytes")
                decoded_bytes += batch_bytes
                if decoded_bytes > limits.maximum_decoded_bytes:
                    has_exceeded_limit = True
                    break
    except (pa.ArrowException, EOFError, OSError, OverflowError, ValueError) as exc:
        raise CaptureError("Parquet source payload is invalid") from exc
    if has_exceeded_limit:
        raise SnowflakeBundleError("bundle replay exceeds the aggregate decoded-byte limit")
    return decoded_bytes


def _stream_receipt(
    acquisition: SnowflakeBundleAcquisition,
    stream_capture: SnowflakeBundleStreamCapture,
) -> CaptureReceipt:
    """Build one connector-domain stream receipt for the durable-store seam."""

    parts = tuple(capture.payload for capture in stream_capture.captures)
    receipt_manifest_by_key = {
        "acquisition_manifest_sha256": acquisition.manifest_sha256,
        "capture_limits": _capture_limits_document(acquisition.capture_limits),
        "captures": [
            {
                "capture_sha256": capture.manifest.capture_sha256,
                "content_bytes": capture.manifest.compressed_bytes,
                "content_sha256": capture.manifest.compressed_sha256,
                "ordinal": ordinal,
            }
            for ordinal, capture in enumerate(stream_capture.captures, start=1)
        ],
        "contract": BUNDLE_CONNECTOR_CONTRACT,
        "request_sha256": acquisition.statement.request.request_sha256,
        "result_schema": [
            {
                "field_id": column.field_id,
                "nullable": column.nullable,
                "source_type": column.source_type,
            }
            for column in stream_capture.schema
        ],
        "statement_sha256": acquisition.statement.statement_sha256,
        "source_snapshot_token": acquisition.source_snapshot_token,
        "stream_id": stream_capture.stream_id,
    }
    canonical_manifest, manifest_sha256 = _canonical_identity("durable-stream-capture", receipt_manifest_by_key)
    return CaptureReceipt(
        stream_id=stream_capture.stream_id,
        source_snapshot_token=acquisition.source_snapshot_token,
        byte_count=sum(len(part) for part in parts),
        content_sha256=_stream_content_sha256(stream_capture.stream_id, parts),
        canonical_manifest=canonical_manifest,
        manifest_sha256=manifest_sha256,
    )


def prepare_bundle_replay(acquisition: SnowflakeBundleAcquisition) -> SnowflakeBundleReplay:
    """Verify and replay every sealed stream after the source connection is gone."""

    verified_acquisition = _verified_bundle_acquisition(acquisition)
    streams_by_id = {
        stream.stream_id: stream for stream in verified_acquisition.statement.request.definition.source_streams
    }
    replay_streams = []
    decoded_bytes = 0
    for stream_capture in verified_acquisition.stream_captures:
        stream = streams_by_id[stream_capture.stream_id]
        fields = _stream_fields(verified_acquisition.statement.request.definition, stream)
        stream_records, decoded_bytes = _replay_stream_records(
            stream_capture,
            stream=stream,
            fields=fields,
            limits=verified_acquisition.capture_limits,
            decoded_bytes=decoded_bytes,
        )
        replay_streams.append(
            SnowflakeBundleReplayStream(
                stream_id=stream_capture.stream_id,
                records=stream_records,
                receipt=_stream_receipt(verified_acquisition, stream_capture),
                parts=tuple(capture.payload for capture in stream_capture.captures),
            )
        )
    return SnowflakeBundleReplay(
        definition=verified_acquisition.statement.request.definition,
        source_snapshot_token=verified_acquisition.source_snapshot_token,
        streams=tuple(replay_streams),
    )


def replayable_parquet_captures(
    acquisition: SnowflakeBundleAcquisition,
) -> tuple[ReplayableParquetCapture, ...]:
    """Convert one closed source bundle into the landed durable-store value."""

    verified_acquisition = _verified_bundle_acquisition(acquisition)
    return tuple(
        ReplayableParquetCapture(
            receipt=_stream_receipt(verified_acquisition, stream_capture),
            parts=tuple(capture.payload for capture in stream_capture.captures),
        )
        for stream_capture in verified_acquisition.stream_captures
    )


def _validated_durable_bundle(
    candidate_statement: object,
    captures: tuple[ReplayableParquetCapture, ...],
) -> tuple[
    SnowflakeBundleStatement,
    CustomImportDefinition,
    dict[str, ReplayableParquetCapture],
    str,
    dict[str, _DurableBundleReceipt],
]:
    """Bind retained bytes and receipts to the exact resolved statement."""

    if not isinstance(candidate_statement, SnowflakeBundleStatement):
        raise SnowflakeBundleError("durable bundle replay requires a sealed bundle statement")
    try:
        request, statement = _rebuilt_bundle_statement(candidate_statement)
    except (AttributeError, TypeError, ValueError) as exc:
        raise SnowflakeBundleError("durable bundle replay requires a sealed bundle statement") from exc
    if candidate_statement != statement:
        raise SnowflakeBundleError("durable bundle replay statement contains a stale identity seal")
    definition = request.definition
    captures_by_stream, source_snapshot_token = _validated_durable_captures(definition, captures)
    durable_receipts_by_stream = _validated_durable_receipts(statement, source_snapshot_token, captures_by_stream)
    return statement, definition, captures_by_stream, source_snapshot_token, durable_receipts_by_stream


def _validated_durable_captures(
    definition: CustomImportDefinition,
    captures: tuple[ReplayableParquetCapture, ...],
) -> tuple[dict[str, ReplayableParquetCapture], str]:
    """Verify retained capture coverage and shared source-snapshot evidence."""

    if (
        not isinstance(captures, tuple)
        or not captures
        or not all(isinstance(capture, ReplayableParquetCapture) for capture in captures)
    ):
        raise SnowflakeBundleError("durable bundle replay captures are invalid")
    sealed_captures = tuple(
        ReplayableParquetCapture(receipt=capture.receipt, parts=capture.parts) for capture in captures
    )
    captures_by_stream = {capture.receipt.stream_id: capture for capture in sealed_captures}
    expected_stream_ids = {stream.stream_id for stream in definition.source_streams}
    if len(captures_by_stream) != len(sealed_captures) or set(captures_by_stream) != expected_stream_ids:
        raise SnowflakeBundleError("durable bundle replay captures do not match the declared streams")
    _validate_durable_partition_counts(definition, captures_by_stream)
    if any(
        stream.format != "parquet" or stream.compression != "none" or stream.record_path is not None
        for stream in definition.source_streams
    ):
        raise SnowflakeBundleError("durable bundle replay requires the fixed Parquet result shape")
    try:
        source_snapshot_token = validate_source_snapshot_tokens(
            definition,
            {
                stream.stream_id: (captures_by_stream[stream.stream_id].receipt.source_snapshot_token,)
                for stream in definition.source_streams
            },
        )
    except SourceSnapshotError as exc:
        raise SnowflakeBundleError("durable bundle streams require one shared semantic snapshot token") from exc
    return captures_by_stream, source_snapshot_token


def _validate_durable_partition_counts(
    definition: CustomImportDefinition,
    captures_by_stream: Mapping[str, ReplayableParquetCapture],
) -> None:
    """Keep durable replay within the source admission partition shape."""

    partition_count = 0
    for stream in definition.source_streams:
        stream_partition_count = len(captures_by_stream[stream.stream_id].parts)
        if stream_partition_count > MAX_RESULT_PARTITIONS:
            raise SnowflakeBundleError("durable bundle replay stream partitions exceed the result limit")
        if stream_partition_count > MAX_BUNDLE_PARTITIONS - partition_count:
            raise SnowflakeBundleError("durable bundle replay capture partitions exceed the manifest limit")
        partition_count += stream_partition_count


def _validated_durable_receipts(
    statement: SnowflakeBundleStatement,
    source_snapshot_token: str,
    captures_by_stream: Mapping[str, ReplayableParquetCapture],
) -> dict[str, _DurableBundleReceipt]:
    """Verify each durable receipt against trusted policy, statement, and bytes."""

    request = statement.request
    durable_receipts_by_stream = {
        capture.receipt.stream_id: _durable_bundle_receipt(capture) for capture in captures_by_stream.values()
    }
    if any(receipt.capture_limits != request.capture_limits for receipt in durable_receipts_by_stream.values()):
        raise SnowflakeBundleError("durable bundle replay receipts do not match configured capture limits")
    if any(receipt.request_sha256 != request.request_sha256 for receipt in durable_receipts_by_stream.values()):
        raise SnowflakeBundleError("durable bundle replay receipts do not match the configured request")
    if any(receipt.statement_sha256 != statement.statement_sha256 for receipt in durable_receipts_by_stream.values()):
        raise SnowflakeBundleError("durable bundle replay receipts do not match the configured statement")
    expected_manifest_sha256 = _expected_acquisition_manifest_sha256(
        statement,
        source_snapshot_token,
        captures_by_stream,
        durable_receipts_by_stream,
    )
    if any(
        receipt.acquisition_manifest_sha256 != expected_manifest_sha256
        for receipt in durable_receipts_by_stream.values()
    ):
        raise SnowflakeBundleError("durable bundle replay receipts do not match the acquisition identity")
    return durable_receipts_by_stream


def _expected_acquisition_manifest_sha256(
    statement: SnowflakeBundleStatement,
    source_snapshot_token: str,
    captures_by_stream: Mapping[str, ReplayableParquetCapture],
    durable_receipts_by_stream: Mapping[str, _DurableBundleReceipt],
) -> str:
    """Rebuild the source acquisition identity from trusted mapping and retained bytes."""

    streams = []
    for binding in statement.request.bindings:
        capture = captures_by_stream[binding.stream_id]
        receipt = durable_receipts_by_stream[binding.stream_id]
        streams.append(
            {
                "captures": [
                    {
                        "capture_sha256": capture_sha256,
                        "content_bytes": len(part),
                        "content_sha256": hashlib.sha256(part).hexdigest(),
                        "ordinal": ordinal,
                    }
                    for ordinal, (part, capture_sha256) in enumerate(
                        zip(capture.parts, receipt.capture_sha256s, strict=True), start=1
                    )
                ],
                "relation": list(binding.relation.parts),
                "result_schema": [
                    {
                        "field_id": column.field_id,
                        "nullable": column.nullable,
                        "source_type": column.source_type,
                    }
                    for column in receipt.result_schema
                ],
                "selected_field_ids": list(binding.selected_field_ids),
                "semantic_token_metadata_key": binding.semantic_token_metadata_key,
                "stream_id": binding.stream_id,
            }
        )
    _canonical_manifest, manifest_sha256 = _canonical_identity(
        "acquisition-manifest",
        {
            "contract": BUNDLE_CONNECTOR_CONTRACT,
            "encoding": statement.request.encoding.as_identity_document(),
            "request_sha256": statement.request.request_sha256,
            "source_snapshot_token": source_snapshot_token,
            "statement_sha256": statement.statement_sha256,
            "streams": streams,
        },
    )
    return manifest_sha256


def reconstruct_replayable_parquet_bundle(
    statement: SnowflakeBundleStatement,
    captures: tuple[ReplayableParquetCapture, ...],
) -> SnowflakeBundleReplay:
    """Decode durable bytes against the exact resolved statement that acquired them."""

    statement, definition, captures_by_stream, source_snapshot_token, durable_receipts_by_stream = (
        _validated_durable_bundle(statement, captures)
    )
    replay_streams = []
    capture_compressed_bytes = capture_decoded_bytes = 0
    decoded_bytes = 0
    for stream in definition.source_streams:
        capture = captures_by_stream[stream.stream_id]
        durable_receipt = durable_receipts_by_stream[stream.stream_id]
        fields = _stream_fields(definition, stream)
        if tuple(column.field_id for column in durable_receipt.result_schema) != tuple(
            field.field_id for field in fields
        ):
            raise SnowflakeBundleError("durable bundle replay receipt schema does not match the declared stream fields")
        stream_records, capture_compressed_bytes, capture_decoded_bytes, decoded_bytes = _replay_durable_stream(
            capture,
            stream,
            fields,
            capture_sha256s=durable_receipt.capture_sha256s,
            limits=statement.request.capture_limits,
            capture_compressed_bytes=capture_compressed_bytes,
            capture_decoded_bytes=capture_decoded_bytes,
            decoded_bytes=decoded_bytes,
        )
        replay_streams.append(
            SnowflakeBundleReplayStream(
                stream_id=stream.stream_id,
                records=stream_records,
                receipt=capture.receipt,
                parts=capture.parts,
            )
        )
    return SnowflakeBundleReplay(
        definition=definition,
        source_snapshot_token=source_snapshot_token,
        streams=tuple(replay_streams),
    )


def _stream_fields(definition: CustomImportDefinition, stream: SourceStream) -> tuple[Field, ...]:
    return tuple(
        sorted(
            (field for field in definition.fields if field.collection == stream.child_collection),
            key=lambda field: field.field_slot,
        )
    )


def _replay_durable_stream(
    capture: ReplayableParquetCapture,
    stream: SourceStream,
    fields: tuple[Field, ...],
    *,
    capture_sha256s: tuple[str, ...],
    limits: CaptureLimits,
    capture_compressed_bytes: int,
    capture_decoded_bytes: int,
    decoded_bytes: int,
) -> tuple[tuple[Mapping[str, Any], ...], int, int, int]:
    """Reseal durable bytes before decoding them through the shared parser."""

    stream_records: list[Mapping[str, Any]] = []
    expected_field_ids = tuple(field.field_id for field in fields)
    stream_compressed_bytes = 0
    for part, capture_sha256 in zip(capture.parts, capture_sha256s, strict=True):
        try:
            part_limits = _remaining_partition_limits(
                limits,
                compressed_bytes=capture_compressed_bytes,
                decoded_bytes=capture_decoded_bytes,
                stream_compressed_bytes=stream_compressed_bytes,
            )
            sealed = capture_stream(
                BytesIO(part),
                stream,
                source_snapshot_token=capture.receipt.source_snapshot_token,
                limits=part_limits,
            )
            if sealed.manifest.capture_sha256 != capture_sha256:
                raise SnowflakeBundleError("durable bundle replay capture seal does not match its receipt")
            capture_compressed_bytes += sealed.manifest.compressed_bytes
            capture_decoded_bytes += sealed.manifest.decoded_bytes
            stream_compressed_bytes += sealed.manifest.compressed_bytes
            _validate_replay_partition_schema(sealed, fields=fields, limits=part_limits)
            for decoded_record in iter_records(sealed, stream, limits=part_limits):
                record_by_field = dict(decoded_record.values)
                if tuple(record_by_field) != expected_field_ids:
                    raise SnowflakeBundleError("durable bundle replay record fields do not match the declared stream")
                stream_records.append(_normalized_integer_replay_values(record_by_field, fields))
                if len(stream_records) > limits.maximum_records:
                    raise SnowflakeBundleError("durable bundle replay stream exceeds the aggregate record limit")
            decoded_bytes = _aggregate_parquet_arrow_bytes(sealed, limits=limits, decoded_bytes=decoded_bytes)
        except CaptureError as exc:
            raise SnowflakeBundleError("durable bundle replay partition cannot be decoded") from exc
    return tuple(stream_records), capture_compressed_bytes, capture_decoded_bytes, decoded_bytes
