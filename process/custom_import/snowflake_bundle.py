# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""One-statement Snowflake bundle acquisition and durable Parquet replay."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from typing import Any, Protocol

from process.custom_import._source_text import _SourceTextValidationError, validate_snapshot_token
from process.custom_import.capture import (
    CaptureError,
    CaptureLimits,
    CaptureManifest,
    SealedCapture,
    capture_stream,
    verify_capture,
)
from process.custom_import.capture_store import ReplayableParquetCapture
from process.custom_import.definition import CONTRACT_VERSION, CustomImportDefinition, Field, SourceStream
from process.custom_import.family import SourceSnapshotError, validate_source_snapshot_tokens
from process.custom_import.snowflake import (
    DEFAULT_CAPTURE_LIMITS,
    MAX_APPROVED_RELATIONS,
    MAX_MANIFEST_CANONICAL_BYTES,
    MAX_RESULT_BYTES,
    MAX_RESULT_PARTITION_BYTES,
    MAX_RESULT_PARTITIONS,
    MAX_SELECTED_COLUMNS,
    SnowflakeApprovedRelation,
    SnowflakeConnectorError,
    SnowflakeCredentialProvider,
    SnowflakeDeclaredColumn,
    SnowflakeKeyPairCredentials,
    SnowflakeParquetResult,
    SnowflakeRelation,
    SnowflakeResultColumn,
)

BUNDLE_CONNECTOR_CONTRACT = "custom-import/snowflake-bundle-acquisition/v1"
PARQUET_COMPRESSION = "zstd"
DEFAULT_PARTITION_ROWS = 1_024
MAX_PARTITION_ROWS = 1_000_000
MAX_DIAGNOSTIC_QUERY_ID_BYTES = 255
# Keep source admission no broader than the later durable capture bundle without
# coupling this source-neutral boundary to its database implementation.
MAX_BUNDLE_PARTITIONS = 8_192
MAX_STREAM_CAPTURE_BYTES = MAX_RESULT_PARTITION_BYTES
MAX_BUNDLE_CAPTURE_BYTES = 128 * 1024 * 1024

_FIELD_ID = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_BUNDLE_ROW_KIND_COLUMN = "__ci_bundle_row_kind"
_STREAM_ORDINAL_COLUMN = "__ci_stream_ordinal"
_STREAM_ID_COLUMN = "__ci_stream_id"
_SOURCE_SNAPSHOT_TOKEN_COLUMN = "__ci_source_snapshot_token"
_METADATA_ROW_KIND = 0
_DATA_ROW_KIND = 1


class SnowflakeBundleError(SnowflakeConnectorError):
    """A one-statement Snowflake bundle is malformed or cannot be sealed."""


def _field_id(value: object, label: str) -> str:
    if not isinstance(value, str) or not _FIELD_ID.fullmatch(value):
        raise SnowflakeBundleError(f"{label} must be a declared lower_snake_case field id")
    return value


def _snapshot_token(value: object) -> str:
    try:
        return validate_snapshot_token(value)
    except _SourceTextValidationError as exc:
        raise SnowflakeBundleError("source snapshot token is invalid") from exc


def _canonical_identity(domain: str, document: dict[str, Any]) -> tuple[str, str]:
    try:
        canonical = json.dumps(
            document,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        encoded = canonical.encode("utf-8")
    except (TypeError, UnicodeEncodeError, ValueError) as exc:
        raise SnowflakeBundleError("bundle identity cannot be canonically serialized") from exc
    if len(encoded) > MAX_MANIFEST_CANONICAL_BYTES:
        raise SnowflakeBundleError("bundle identity exceeds the canonical byte limit")
    digest = hashlib.sha256(
        f"{CONTRACT_VERSION}\x00{BUNDLE_CONNECTOR_CONTRACT}\x00{domain}\x00".encode("ascii") + encoded
    ).hexdigest()
    return canonical, digest


def _stream_content_sha256(stream_id: str, parts: tuple[bytes, ...]) -> str:
    """Bind ordered retained payload bytes without building a second aggregate."""

    digest = hashlib.sha256(
        f"{CONTRACT_VERSION}\x00{BUNDLE_CONNECTOR_CONTRACT}\x00stream-content\x00{stream_id}\x00".encode("ascii")
    )
    for ordinal, part in enumerate(parts, start=1):
        digest.update(ordinal.to_bytes(4, byteorder="big", signed=False))
        digest.update(len(part).to_bytes(8, byteorder="big", signed=False))
        digest.update(hashlib.sha256(part).digest())
    return digest.hexdigest()


def _quoted_identifier(identifier: str) -> str:
    return f'"{identifier}"'


def _diagnostic_query_id(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.isprintable():
        raise SnowflakeBundleError("bundle query id must be printable diagnostic text")
    try:
        if not 1 <= len(value.encode("utf-8")) <= MAX_DIAGNOSTIC_QUERY_ID_BYTES:
            raise SnowflakeBundleError("bundle query id exceeds the byte limit")
    except UnicodeEncodeError as exc:
        raise SnowflakeBundleError("bundle query id must be valid UTF-8") from exc
    return value


def _remaining_partition_limits(
    limits: CaptureLimits,
    *,
    compressed_bytes: int,
    decoded_bytes: int,
    stream_compressed_bytes: int,
) -> CaptureLimits:
    compressed_remaining = min(
        MAX_BUNDLE_CAPTURE_BYTES - compressed_bytes,
        limits.maximum_compressed_bytes - stream_compressed_bytes,
    )
    decoded_remaining = limits.maximum_decoded_bytes - decoded_bytes
    if compressed_remaining <= 0 or decoded_remaining <= 0:
        raise SnowflakeBundleError("bundle result exceeds the aggregate byte limit")
    maximum_decoded_bytes = min(decoded_remaining, MAX_RESULT_PARTITION_BYTES)
    return replace(
        limits,
        maximum_compressed_bytes=min(compressed_remaining, MAX_STREAM_CAPTURE_BYTES),
        maximum_decoded_bytes=maximum_decoded_bytes,
        maximum_record_bytes=min(limits.maximum_record_bytes, maximum_decoded_bytes),
        read_chunk_bytes=min(limits.read_chunk_bytes, maximum_decoded_bytes),
    )


def _capture_limits(value: object) -> CaptureLimits:
    if not isinstance(value, CaptureLimits):
        raise SnowflakeBundleError("bundle capture limits must use the declared capture-limit type")
    if value.maximum_compressed_bytes > MAX_STREAM_CAPTURE_BYTES:
        raise SnowflakeBundleError("bundle compressed-byte limit exceeds the durable stream bound")
    if (
        value.maximum_decoded_bytes > MAX_RESULT_BYTES
        or value.maximum_record_bytes > MAX_RESULT_PARTITION_BYTES
        or value.read_chunk_bytes > MAX_RESULT_PARTITION_BYTES
    ):
        raise SnowflakeBundleError("bundle capture limits exceed the connector result-byte bounds")
    if (
        value.maximum_records > DEFAULT_CAPTURE_LIMITS.maximum_records
        or value.maximum_fields_per_record > DEFAULT_CAPTURE_LIMITS.maximum_fields_per_record
    ):
        raise SnowflakeBundleError("bundle capture limits exceed the default record or field bounds")
    return value


_CAPTURE_LIMIT_NAMES = (
    "maximum_compressed_bytes",
    "maximum_decoded_bytes",
    "maximum_record_bytes",
    "maximum_records",
    "maximum_fields_per_record",
    "read_chunk_bytes",
)


def _capture_limits_document(limits: CaptureLimits) -> dict[str, int]:
    return {name: getattr(limits, name) for name in _CAPTURE_LIMIT_NAMES}


@dataclass(frozen=True)
class _DurableBundleReceipt:
    """The connector-owned receipt facts needed to replay one durable stream."""

    acquisition_manifest_sha256: str
    request_sha256: str
    statement_sha256: str
    capture_sha256s: tuple[str, ...]
    capture_limits: CaptureLimits
    result_schema: tuple[SnowflakeResultColumn, ...]


_DURABLE_RECEIPT_KEYS = frozenset(
    {
        "acquisition_manifest_sha256",
        "capture_limits",
        "captures",
        "contract",
        "request_sha256",
        "result_schema",
        "statement_sha256",
        "source_snapshot_token",
        "stream_id",
    }
)
_DURABLE_CAPTURE_KEYS = frozenset({"capture_sha256", "content_bytes", "content_sha256", "ordinal"})
_DURABLE_SCHEMA_KEYS = frozenset({"field_id", "nullable", "source_type"})


def _receipt_sha256(value: object) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError("receipt digest is invalid")
    return value


def _durable_capture_limits(document: Mapping[str, object]) -> CaptureLimits:
    values = document.get("capture_limits")
    if not isinstance(values, Mapping) or set(values) != set(_CAPTURE_LIMIT_NAMES):
        raise ValueError("missing capture limits")
    return _capture_limits(CaptureLimits(**{name: values[name] for name in _CAPTURE_LIMIT_NAMES}))


def _durable_result_schema(document: Mapping[str, object]) -> tuple[SnowflakeResultColumn, ...]:
    schema_entries = document.get("result_schema")
    if not isinstance(schema_entries, list) or not schema_entries:
        raise ValueError("receipt result schema is invalid")
    if any(not isinstance(entry, Mapping) or set(entry) != _DURABLE_SCHEMA_KEYS for entry in schema_entries):
        raise ValueError("receipt result schema is invalid")
    result_columns = tuple(
        SnowflakeResultColumn(
            field_id=entry["field_id"],
            source_type=entry["source_type"],
            nullable=entry["nullable"],
        )
        for entry in schema_entries
    )
    if len({column.field_id for column in result_columns}) != len(result_columns):
        raise ValueError("receipt result schema is invalid")
    return result_columns


def _validate_durable_capture_parts(document: Mapping[str, object], parts: tuple[bytes, ...]) -> tuple[str, ...]:
    manifests = document.get("captures")
    if not isinstance(manifests, list) or len(manifests) != len(parts):
        raise ValueError("receipt capture manifests are invalid")
    capture_sha256s = []
    for ordinal, (manifest, part) in enumerate(zip(manifests, parts, strict=True), start=1):
        if not isinstance(manifest, Mapping) or set(manifest) != _DURABLE_CAPTURE_KEYS:
            raise ValueError("receipt capture manifest is invalid")
        if (
            isinstance(manifest["ordinal"], bool)
            or manifest["ordinal"] != ordinal
            or isinstance(manifest["content_bytes"], bool)
            or manifest["content_bytes"] != len(part)
            or manifest["content_sha256"] != hashlib.sha256(part).hexdigest()
        ):
            raise ValueError("receipt capture manifest is invalid")
        capture_sha256s.append(_receipt_sha256(manifest["capture_sha256"]))
    return tuple(capture_sha256s)


def _durable_bundle_receipt(capture: ReplayableParquetCapture) -> _DurableBundleReceipt:
    """Verify a connector-sealed durable receipt before using its policy facts."""

    try:
        receipt = capture.receipt
        document = json.loads(receipt.canonical_manifest)
        if not isinstance(document, dict) or set(document) != _DURABLE_RECEIPT_KEYS:
            raise ValueError("receipt document is invalid")
        canonical_manifest, manifest_sha256 = _canonical_identity("durable-stream-capture", document)
        if canonical_manifest != receipt.canonical_manifest or manifest_sha256 != receipt.manifest_sha256:
            raise ValueError("receipt manifest digest does not match")
        if (
            document["contract"] != BUNDLE_CONNECTOR_CONTRACT
            or document["stream_id"] != receipt.stream_id
            or document["source_snapshot_token"] != receipt.source_snapshot_token
            or _stream_content_sha256(receipt.stream_id, capture.parts) != receipt.content_sha256
        ):
            raise ValueError("receipt does not match durable capture")
        acquisition_manifest_sha256 = _receipt_sha256(document["acquisition_manifest_sha256"])
        request_sha256 = _receipt_sha256(document["request_sha256"])
        statement_sha256 = _receipt_sha256(document["statement_sha256"])
        capture_limits = _durable_capture_limits(document)
        result_schema = _durable_result_schema(document)
        capture_sha256s = _validate_durable_capture_parts(document, capture.parts)
    except (AttributeError, SnowflakeConnectorError, TypeError, ValueError) as exc:
        raise SnowflakeBundleError("durable bundle replay receipt is invalid") from exc
    return _DurableBundleReceipt(
        acquisition_manifest_sha256=acquisition_manifest_sha256,
        request_sha256=request_sha256,
        statement_sha256=statement_sha256,
        capture_sha256s=capture_sha256s,
        capture_limits=capture_limits,
        result_schema=result_schema,
    )


@dataclass(frozen=True)
class SnowflakeBundleEncoding:
    """Fixed adapter inputs which make capture partitioning reproducible."""

    partition_rows: int = DEFAULT_PARTITION_ROWS
    parquet_compression: str = PARQUET_COMPRESSION

    def __post_init__(self) -> None:
        if (
            isinstance(self.partition_rows, bool)
            or not isinstance(self.partition_rows, int)
            or not 1 <= self.partition_rows <= MAX_PARTITION_ROWS
        ):
            raise SnowflakeBundleError("bundle partition rows must be a bounded positive integer")
        if self.parquet_compression != PARQUET_COMPRESSION:
            raise SnowflakeBundleError("bundle Parquet compression must be zstd")

    def as_identity_document(self) -> dict[str, object]:
        """Return the adapter inputs that are part of the sealed identity."""

        return {
            "format": "parquet",
            "parquet_compression": self.parquet_compression,
            "partition_rows": self.partition_rows,
        }


DEFAULT_BUNDLE_ENCODING = SnowflakeBundleEncoding()


@dataclass(frozen=True)
class SnowflakeBundleBinding:
    """One stream's approved data and source-snapshot relations."""

    stream_id: str
    relation: SnowflakeRelation
    source_snapshot_token_relation: SnowflakeRelation
    selected_field_ids: tuple[str, ...]
    semantic_token_metadata_key: str | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "stream_id", _field_id(self.stream_id, "bundle stream id"))
        if not isinstance(self.relation, SnowflakeRelation):
            raise SnowflakeBundleError("bundle relation must use the declared relation type")
        if not isinstance(self.source_snapshot_token_relation, SnowflakeRelation):
            raise SnowflakeBundleError("bundle source snapshot relation must use the declared relation type")
        if (
            not isinstance(self.selected_field_ids, tuple)
            or not 1 <= len(self.selected_field_ids) <= MAX_SELECTED_COLUMNS
        ):
            raise SnowflakeBundleError("bundle selected fields must contain from 1 through 128 values")
        field_ids = tuple(_field_id(field_id, "bundle selected field id") for field_id in self.selected_field_ids)
        if len(field_ids) != len(set(field_ids)):
            raise SnowflakeBundleError("bundle selected field ids must be unique")
        object.__setattr__(self, "selected_field_ids", field_ids)
        if self.semantic_token_metadata_key is not None:
            object.__setattr__(
                self,
                "semantic_token_metadata_key",
                _field_id(self.semantic_token_metadata_key, "semantic token metadata key"),
            )


@dataclass(frozen=True)
class SnowflakeBundleRequest:
    """Immutable configured parent/child bundle before relation approval is resolved."""

    definition: CustomImportDefinition
    bindings: tuple[SnowflakeBundleBinding, ...]
    encoding: SnowflakeBundleEncoding = DEFAULT_BUNDLE_ENCODING
    capture_limits: CaptureLimits = field(default=DEFAULT_CAPTURE_LIMITS, repr=False)
    canonical_request: str = field(init=False, repr=False)
    request_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        """Normalize sealed request inputs, including the durable capture policy."""
        if not isinstance(self.definition, CustomImportDefinition):
            raise SnowflakeBundleError("bundle request requires a custom-import definition")
        if not isinstance(self.bindings, tuple) or not self.bindings:
            raise SnowflakeBundleError("bundle request requires one binding per declared stream")
        if not all(isinstance(binding, SnowflakeBundleBinding) for binding in self.bindings):
            raise SnowflakeBundleError("bundle bindings must use the declared binding type")
        if not isinstance(self.encoding, SnowflakeBundleEncoding):
            raise SnowflakeBundleError("bundle encoding must use the declared encoding type")
        capture_limits = _capture_limits(self.capture_limits)

        streams_by_id = {stream.stream_id: stream for stream in self.definition.source_streams}
        bindings_by_stream = {binding.stream_id: binding for binding in self.bindings}
        if len(bindings_by_stream) != len(self.bindings) or set(bindings_by_stream) != set(streams_by_id):
            raise SnowflakeBundleError("bundle bindings must exactly cover the declared source streams")
        normalized_bindings = []
        for stream in self.definition.source_streams:
            binding = bindings_by_stream[stream.stream_id]
            if stream.format != "parquet" or stream.compression != "none" or stream.record_path is not None:
                raise SnowflakeBundleError("bundle streams must use the fixed Parquet result shape")
            expected_field_ids = _stream_field_ids(self.definition, stream)
            if set(binding.selected_field_ids) != set(expected_field_ids):
                raise SnowflakeBundleError("bundle selected fields must exactly cover their declared stream scope")
            if binding.semantic_token_metadata_key is None:
                raise SnowflakeBundleError("bundle streams require explicit semantic token metadata")
            if binding.semantic_token_metadata_key != stream.snapshot_token:
                raise SnowflakeBundleError("bundle semantic token metadata must match the declared stream selector")
            normalized_bindings.append(
                SnowflakeBundleBinding(
                    stream_id=binding.stream_id,
                    relation=binding.relation,
                    source_snapshot_token_relation=binding.source_snapshot_token_relation,
                    selected_field_ids=expected_field_ids,
                    semantic_token_metadata_key=binding.semantic_token_metadata_key,
                )
            )
        bindings = tuple(normalized_bindings)
        request_identity_by_key = {
            "bindings": [
                {
                    "relation": list(binding.relation.parts),
                    "selected_field_ids": list(binding.selected_field_ids),
                    "semantic_token_metadata_key": binding.semantic_token_metadata_key,
                    "source_snapshot_token_relation": list(binding.source_snapshot_token_relation.parts),
                    "stream_id": binding.stream_id,
                }
                for binding in bindings
            ],
            "capture_limits": _capture_limits_document(capture_limits),
            "contract": BUNDLE_CONNECTOR_CONTRACT,
            "definition_sha256": self.definition.digest,
            "encoding": self.encoding.as_identity_document(),
            "schema_sha256": self.definition.schema_digest,
        }
        canonical, digest = _canonical_identity("request", request_identity_by_key)
        object.__setattr__(self, "bindings", bindings)
        object.__setattr__(self, "capture_limits", capture_limits)
        object.__setattr__(self, "canonical_request", canonical)
        object.__setattr__(self, "request_sha256", digest)


def _validated_bundle_request(request: object) -> SnowflakeBundleRequest:
    """Rebuild a request before source work trusts its cached identity seal."""

    if not isinstance(request, SnowflakeBundleRequest):
        raise SnowflakeBundleError("bundle statement requires a declared bundle request")
    try:
        rebuilt = SnowflakeBundleRequest(
            definition=request.definition,
            bindings=tuple(
                SnowflakeBundleBinding(
                    stream_id=binding.stream_id,
                    relation=SnowflakeRelation(*binding.relation.parts),
                    source_snapshot_token_relation=SnowflakeRelation(*binding.source_snapshot_token_relation.parts),
                    selected_field_ids=tuple(binding.selected_field_ids),
                    semantic_token_metadata_key=binding.semantic_token_metadata_key,
                )
                for binding in request.bindings
            ),
            encoding=SnowflakeBundleEncoding(
                partition_rows=request.encoding.partition_rows,
                parquet_compression=request.encoding.parquet_compression,
            ),
            capture_limits=request.capture_limits,
        )
    except (AttributeError, TypeError) as exc:
        raise SnowflakeBundleError("bundle request seal is invalid") from exc
    if rebuilt != request:
        raise SnowflakeBundleError("bundle request contains a stale identity seal")
    return rebuilt


def _stream_field_ids(definition: CustomImportDefinition, stream: SourceStream) -> tuple[str, ...]:
    fields = tuple(field for field in definition.fields if field.collection == stream.child_collection)
    return tuple(field.field_id for field in sorted(fields, key=lambda field: field.field_slot))


@dataclass(frozen=True)
class SnowflakeBundleStatement:
    """The only generated statement shape for one configured bundle."""

    request: SnowflakeBundleRequest
    selected_columns_by_stream: tuple[tuple[SnowflakeDeclaredColumn, ...], ...]
    source_snapshot_token_columns_by_stream: tuple[SnowflakeDeclaredColumn, ...]
    sql: str = field(init=False)
    canonical_statement: str = field(init=False, repr=False)
    statement_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.request, SnowflakeBundleRequest):
            raise SnowflakeBundleError("bundle statement requires a declared bundle request")
        if not isinstance(self.selected_columns_by_stream, tuple) or len(self.selected_columns_by_stream) != len(
            self.request.bindings
        ):
            raise SnowflakeBundleError("bundle statement columns must match its stream bindings")
        if not isinstance(self.source_snapshot_token_columns_by_stream, tuple) or len(
            self.source_snapshot_token_columns_by_stream
        ) != len(self.request.bindings):
            raise SnowflakeBundleError("bundle statement snapshot columns must match its stream bindings")
        for binding, selected_columns, snapshot_column in zip(
            self.request.bindings,
            self.selected_columns_by_stream,
            self.source_snapshot_token_columns_by_stream,
            strict=True,
        ):
            if not isinstance(selected_columns, tuple) or not all(
                isinstance(column, SnowflakeDeclaredColumn) for column in selected_columns
            ):
                raise SnowflakeBundleError("bundle statement columns must use declared column values")
            if tuple(column.field_id for column in selected_columns) != binding.selected_field_ids:
                raise SnowflakeBundleError("bundle statement columns do not match the configured selected fields")
            if not isinstance(snapshot_column, SnowflakeDeclaredColumn):
                raise SnowflakeBundleError("bundle statement snapshot columns must use declared column values")
            if snapshot_column.field_id != binding.semantic_token_metadata_key:
                raise SnowflakeBundleError("bundle statement snapshot column does not match semantic token metadata")

        sql = _bundle_sql(
            self.request,
            self.selected_columns_by_stream,
            self.source_snapshot_token_columns_by_stream,
        )
        statement_identity_by_key = {
            "contract": BUNDLE_CONNECTOR_CONTRACT,
            "request_sha256": self.request.request_sha256,
            "sql": sql,
        }
        canonical, digest = _canonical_identity("statement", statement_identity_by_key)
        object.__setattr__(self, "sql", sql)
        object.__setattr__(self, "canonical_statement", canonical)
        object.__setattr__(self, "statement_sha256", digest)


def _validated_bundle_statement(statement: object) -> SnowflakeBundleStatement:
    """Rebuild every nested statement seal before using its source identity."""

    if not isinstance(statement, SnowflakeBundleStatement):
        raise SnowflakeBundleError("bundle statement is invalid")
    try:
        rebuilt = SnowflakeBundleStatement(
            request=_validated_bundle_request(statement.request),
            selected_columns_by_stream=tuple(
                tuple(SnowflakeDeclaredColumn(column.field_id, column.column_identifier) for column in selected_columns)
                for selected_columns in statement.selected_columns_by_stream
            ),
            source_snapshot_token_columns_by_stream=tuple(
                SnowflakeDeclaredColumn(column.field_id, column.column_identifier)
                for column in statement.source_snapshot_token_columns_by_stream
            ),
        )
    except (AttributeError, TypeError) as exc:
        raise SnowflakeBundleError("bundle statement seal is invalid") from exc
    if rebuilt != statement:
        raise SnowflakeBundleError("bundle statement contains a stale identity seal")
    return rebuilt


def _bundle_sql_branches(
    request: SnowflakeBundleRequest,
    fields: tuple[Field, ...],
    selected_columns_by_stream: tuple[tuple[SnowflakeDeclaredColumn, ...], ...],
    source_snapshot_token_columns_by_stream: tuple[SnowflakeDeclaredColumn, ...],
) -> tuple[list[str], list[str]]:
    metadata_branches, data_branches = [], []
    for ordinal, (binding, selected_columns, snapshot_column) in enumerate(
        zip(
            request.bindings,
            selected_columns_by_stream,
            source_snapshot_token_columns_by_stream,
            strict=True,
        ),
        start=1,
    ):
        selected_by_field = {column.field_id: column for column in selected_columns}
        metadata_values = [
            f"{_METADATA_ROW_KIND} AS {_quoted_identifier(_BUNDLE_ROW_KIND_COLUMN)}",
            f"{ordinal} AS {_quoted_identifier(_STREAM_ORDINAL_COLUMN)}",
            f"'{binding.stream_id}' AS {_quoted_identifier(_STREAM_ID_COLUMN)}",
            f"(SELECT {_quoted_identifier(snapshot_column.column_identifier)} "
            f"FROM {binding.source_snapshot_token_relation.quoted_sql}) "
            f"AS {_quoted_identifier(_SOURCE_SNAPSHOT_TOKEN_COLUMN)}",
            *(f"NULL AS {_quoted_identifier(field.field_id)}" for field in fields),
        ]
        metadata_branches.append(f"SELECT {', '.join(metadata_values)}")
        data_values = [
            f"{_DATA_ROW_KIND} AS {_quoted_identifier(_BUNDLE_ROW_KIND_COLUMN)}",
            f"{ordinal} AS {_quoted_identifier(_STREAM_ORDINAL_COLUMN)}",
            f"'{binding.stream_id}' AS {_quoted_identifier(_STREAM_ID_COLUMN)}",
            f"NULL AS {_quoted_identifier(_SOURCE_SNAPSHOT_TOKEN_COLUMN)}",
        ]
        data_values.extend(
            (
                f"{_quoted_identifier(selected_by_field[field.field_id].column_identifier)} "
                f"AS {_quoted_identifier(field.field_id)}"
                if field.field_id in selected_by_field
                else f"NULL AS {_quoted_identifier(field.field_id)}"
            )
            for field in fields
        )
        data_branches.append(f"SELECT {', '.join(data_values)} FROM {binding.relation.quoted_sql}")
    return metadata_branches, data_branches


def _bundle_sql(
    request: SnowflakeBundleRequest,
    selected_columns_by_stream: tuple[tuple[SnowflakeDeclaredColumn, ...], ...],
    source_snapshot_token_columns_by_stream: tuple[SnowflakeDeclaredColumn, ...],
) -> str:
    """Render the fixed metadata-and-data union for one approved bundle."""

    fields = tuple(sorted(request.definition.fields, key=lambda field: field.field_slot))
    output_columns = (
        _BUNDLE_ROW_KIND_COLUMN,
        _STREAM_ORDINAL_COLUMN,
        _STREAM_ID_COLUMN,
        _SOURCE_SNAPSHOT_TOKEN_COLUMN,
        *(field.field_id for field in fields),
    )
    metadata_branches, data_branches = _bundle_sql_branches(
        request,
        fields,
        selected_columns_by_stream,
        source_snapshot_token_columns_by_stream,
    )
    projected = ", ".join(_quoted_identifier(column) for column in output_columns)
    order = ", ".join(
        (
            _quoted_identifier(_BUNDLE_ROW_KIND_COLUMN),
            _quoted_identifier(_STREAM_ORDINAL_COLUMN),
            *(_quoted_identifier(field.field_id) + " NULLS FIRST" for field in fields),
        )
    )
    return (
        f"SELECT {projected} FROM ({' UNION ALL '.join((*metadata_branches, *data_branches))}) "
        f'AS "__ci_bundle" ORDER BY {order}'
    )


@dataclass(frozen=True)
class SnowflakeBundleStreamMetadata:
    """Adapter metadata retained even when a stream has no result rows."""

    stream_id: str
    semantic_token_metadata_key: str
    source_snapshot_tokens: tuple[str | None, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "stream_id", _field_id(self.stream_id, "bundle metadata stream id"))
        object.__setattr__(
            self,
            "semantic_token_metadata_key",
            _field_id(self.semantic_token_metadata_key, "bundle semantic token metadata key"),
        )
        if (
            not isinstance(self.source_snapshot_tokens, tuple)
            or len(self.source_snapshot_tokens) > MAX_BUNDLE_PARTITIONS
        ):
            raise SnowflakeBundleError("bundle snapshot metadata has an invalid observation count")


@dataclass(frozen=True)
class SnowflakeBundleStreamResult:
    """One adapter-owned stream result from the single bundle statement."""

    metadata: SnowflakeBundleStreamMetadata
    parquet_result: SnowflakeParquetResult

    def __post_init__(self) -> None:
        if not isinstance(self.metadata, SnowflakeBundleStreamMetadata):
            raise SnowflakeBundleError("bundle stream metadata is invalid")
        if not isinstance(self.parquet_result, SnowflakeParquetResult):
            raise SnowflakeBundleError("bundle stream result must own a Parquet result")


@dataclass
class SnowflakeBundleResult:
    """One adapter response; its query id is diagnostic and never an identity input."""

    stream_results: tuple[SnowflakeBundleStreamResult, ...]
    query_id: str | None = None
    on_close: Callable[[], None] | None = field(default=None, repr=False)
    _closed: bool = field(init=False, default=False, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.stream_results, tuple) or not self.stream_results:
            raise SnowflakeBundleError("bundle result requires one stream result per declared stream")
        if not all(isinstance(result, SnowflakeBundleStreamResult) for result in self.stream_results):
            raise SnowflakeBundleError("bundle result stream results are invalid")
        self.query_id = _diagnostic_query_id(self.query_id)
        if self.on_close is not None and not callable(self.on_close):
            raise SnowflakeBundleError("bundle cleanup callback must be callable")

    def close(self) -> None:
        """Close every stream result and then the adapter-level transport once."""

        if self._closed:
            return
        self._closed = True
        errors: list[BaseException] = []
        for stream_result in self.stream_results:
            try:
                stream_result.parquet_result.close()
            except BaseException as exc:
                errors.append(exc)
        if self.on_close is not None:
            try:
                self.on_close()
            except BaseException as exc:
                errors.append(exc)
        if errors:
            for error in errors:
                if not isinstance(error, Exception):
                    raise error
            raise SnowflakeBundleError("bundle adapter result cleanup failed") from errors[0]


class SnowflakeBundleAdapter(Protocol):
    """Later runtime seam: execute one generated statement and return split streams."""

    def fetch_bundle(
        self,
        statement: SnowflakeBundleStatement,
        credentials: SnowflakeKeyPairCredentials,
    ) -> SnowflakeBundleResult:
        """Return all declared streams from exactly one generated statement."""


@dataclass(frozen=True)
class SnowflakeBundleStreamCapture:
    """One stream's replayable Parquet captures and adapter-reported scalar schema."""

    stream_id: str
    schema: tuple[SnowflakeResultColumn, ...]
    captures: tuple[SealedCapture, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "stream_id", _field_id(self.stream_id, "bundle capture stream id"))
        if (
            not isinstance(self.schema, tuple)
            or not self.schema
            or not all(isinstance(column, SnowflakeResultColumn) for column in self.schema)
        ):
            raise SnowflakeBundleError("bundle capture schema is invalid")
        if (
            not isinstance(self.captures, tuple)
            or not self.captures
            or not all(isinstance(capture, SealedCapture) for capture in self.captures)
        ):
            raise SnowflakeBundleError("bundle capture requires schema-bearing Parquet partitions")


@dataclass(frozen=True)
class SnowflakeBundleAcquisition:
    """A sealed multi-stream source bundle, intentionally detached from storage/lifecycle."""

    statement: SnowflakeBundleStatement
    source_snapshot_token: str
    stream_captures: tuple[SnowflakeBundleStreamCapture, ...]
    capture_limits: CaptureLimits = field(default=DEFAULT_CAPTURE_LIMITS, repr=False)
    diagnostic_query_id: str | None = None
    canonical_manifest: str = field(init=False, repr=False)
    manifest_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.statement, SnowflakeBundleStatement):
            raise SnowflakeBundleError("bundle acquisition requires a generated bundle statement")
        snapshot_token = _snapshot_token(self.source_snapshot_token)
        capture_limits = _capture_limits(self.capture_limits)
        if capture_limits != self.statement.request.capture_limits:
            raise SnowflakeBundleError("bundle acquisition capture limits do not match its request")
        diagnostic_query_id = _diagnostic_query_id(self.diagnostic_query_id)
        if not isinstance(self.stream_captures, tuple) or len(self.stream_captures) != len(
            self.statement.request.bindings
        ):
            raise SnowflakeBundleError("bundle captures must exactly cover the declared source streams")
        expected_stream_ids = tuple(binding.stream_id for binding in self.statement.request.bindings)
        if tuple(capture.stream_id for capture in self.stream_captures) != expected_stream_ids:
            raise SnowflakeBundleError("bundle captures must retain declared stream order")

        manifest_streams = _validated_stream_capture_manifests(
            self.statement,
            self.stream_captures,
            snapshot_token,
            capture_limits,
        )
        manifest_by_key = {
            "contract": BUNDLE_CONNECTOR_CONTRACT,
            "encoding": self.statement.request.encoding.as_identity_document(),
            "request_sha256": self.statement.request.request_sha256,
            "source_snapshot_token": snapshot_token,
            "statement_sha256": self.statement.statement_sha256,
            "streams": manifest_streams,
        }
        canonical, digest = _canonical_identity("acquisition-manifest", manifest_by_key)
        object.__setattr__(self, "capture_limits", capture_limits)
        object.__setattr__(self, "source_snapshot_token", snapshot_token)
        object.__setattr__(self, "diagnostic_query_id", diagnostic_query_id)
        object.__setattr__(self, "canonical_manifest", canonical)
        object.__setattr__(self, "manifest_sha256", digest)


def _stream_capture_manifest(
    binding: SnowflakeBundleBinding,
    stream_capture: SnowflakeBundleStreamCapture,
) -> dict[str, object]:
    return {
        "captures": [
            {
                "capture_sha256": capture.manifest.capture_sha256,
                "content_bytes": capture.manifest.compressed_bytes,
                "content_sha256": capture.manifest.compressed_sha256,
                "ordinal": ordinal,
            }
            for ordinal, capture in enumerate(stream_capture.captures, start=1)
        ],
        "relation": list(binding.relation.parts),
        "result_schema": [
            {
                "field_id": column.field_id,
                "nullable": column.nullable,
                "source_type": column.source_type,
            }
            for column in stream_capture.schema
        ],
        "selected_field_ids": list(binding.selected_field_ids),
        "semantic_token_metadata_key": binding.semantic_token_metadata_key,
        "stream_id": binding.stream_id,
    }


def _validated_stream_capture_manifests(
    statement: SnowflakeBundleStatement,
    stream_captures: tuple[SnowflakeBundleStreamCapture, ...],
    snapshot_token: str,
    capture_limits: CaptureLimits,
) -> list[dict[str, object]]:
    streams_by_id = {stream.stream_id: stream for stream in statement.request.definition.source_streams}
    manifest_streams = []
    compressed_bytes = decoded_bytes = partition_count = 0
    for binding, expected_columns, stream_capture in zip(
        statement.request.bindings,
        statement.selected_columns_by_stream,
        stream_captures,
        strict=True,
    ):
        compressed_bytes, decoded_bytes, partition_count = _verify_stream_capture(
            stream_capture,
            streams_by_id[binding.stream_id],
            snapshot_token,
            capture_limits,
            compressed_bytes=compressed_bytes,
            decoded_bytes=decoded_bytes,
            partition_count=partition_count,
        )
        if tuple(column.field_id for column in stream_capture.schema) != tuple(
            column.field_id for column in expected_columns
        ):
            raise SnowflakeBundleError("bundle capture schema does not match configured selected fields")
        manifest_streams.append(_stream_capture_manifest(binding, stream_capture))
    return manifest_streams


def _verify_stream_capture(
    stream_capture: SnowflakeBundleStreamCapture,
    stream: SourceStream,
    source_snapshot_token: str,
    capture_limits: CaptureLimits,
    *,
    compressed_bytes: int,
    decoded_bytes: int,
    partition_count: int,
) -> tuple[int, int, int]:
    if len(stream_capture.captures) > MAX_RESULT_PARTITIONS:
        raise SnowflakeBundleError("bundle stream partitions exceed the result limit")
    if len(stream_capture.captures) > MAX_BUNDLE_PARTITIONS - partition_count:
        raise SnowflakeBundleError("bundle capture partitions exceed the manifest limit")
    stream_compressed_bytes = 0
    for capture in stream_capture.captures:
        manifest = capture.manifest
        if not isinstance(manifest, CaptureManifest):
            raise SnowflakeBundleError("bundle capture manifest is invalid")
        if manifest.source_snapshot_token != source_snapshot_token:
            raise SnowflakeBundleError("bundle capture token does not match semantic metadata")
        if (
            not 1 <= manifest.compressed_bytes <= MAX_STREAM_CAPTURE_BYTES
            or not 1 <= manifest.decoded_bytes <= MAX_RESULT_PARTITION_BYTES
        ):
            raise SnowflakeBundleError("bundle capture partition exceeds the byte limit")
        partition_limits = _remaining_partition_limits(
            capture_limits,
            compressed_bytes=compressed_bytes,
            decoded_bytes=decoded_bytes,
            stream_compressed_bytes=stream_compressed_bytes,
        )
        try:
            verify_capture(capture, stream, limits=partition_limits)
        except CaptureError as exc:
            raise SnowflakeBundleError("bundle capture cannot be replayed") from exc
        compressed_bytes += manifest.compressed_bytes
        stream_compressed_bytes += manifest.compressed_bytes
        decoded_bytes += manifest.decoded_bytes
        partition_count += 1
    return compressed_bytes, decoded_bytes, partition_count


class SnowflakeBundleAcquisitionConnector:
    """Build, execute, and seal only one allowlisted parent/child bundle statement."""

    def __init__(
        self,
        *,
        approved_relations: tuple[SnowflakeApprovedRelation, ...],
        credential_provider: SnowflakeCredentialProvider,
        adapter: SnowflakeBundleAdapter,
        capture_limits: CaptureLimits = DEFAULT_CAPTURE_LIMITS,
    ) -> None:
        if not isinstance(approved_relations, tuple) or not 1 <= len(approved_relations) <= MAX_APPROVED_RELATIONS:
            raise SnowflakeBundleError("approved relations must contain from 1 through 128 entries")
        if not all(isinstance(relation, SnowflakeApprovedRelation) for relation in approved_relations):
            raise SnowflakeBundleError("approved relations must use declared relation values")
        relation_keys = tuple(relation.relation.parts for relation in approved_relations)
        if len(relation_keys) != len(set(relation_keys)):
            raise SnowflakeBundleError("approved relation identifiers must be unique")
        if not callable(getattr(credential_provider, "load_key_pair", None)):
            raise SnowflakeBundleError("credential provider must load a key pair")
        if not callable(getattr(adapter, "fetch_bundle", None)):
            raise SnowflakeBundleError("bundle adapter must fetch one generated bundle")
        capture_limits = _capture_limits(capture_limits)
        self._approved_by_relation = {relation.relation.parts: relation for relation in approved_relations}
        self._credential_provider = credential_provider
        self._adapter = adapter
        self._capture_limits = capture_limits

    def prepare_request(
        self,
        definition: CustomImportDefinition,
        *,
        bindings: tuple[SnowflakeBundleBinding, ...],
        encoding: SnowflakeBundleEncoding = DEFAULT_BUNDLE_ENCODING,
    ) -> SnowflakeBundleRequest:
        """Validate definition-owned stream configuration before generating SQL."""

        return SnowflakeBundleRequest(
            definition=definition,
            bindings=bindings,
            encoding=encoding,
            capture_limits=self._capture_limits,
        )

    def build_statement(self, request: SnowflakeBundleRequest) -> SnowflakeBundleStatement:
        """Generate the sole SELECT after resolving every configured allowlist mapping."""

        request = _validated_bundle_request(request)
        if request.capture_limits != self._capture_limits:
            raise SnowflakeBundleError("bundle request capture limits do not match the configured connector")
        selected_column_groups = tuple(self._approved_columns(request, binding) for binding in request.bindings)
        snapshot_token_columns = tuple(self._approved_snapshot_token_column(binding) for binding in request.bindings)
        return SnowflakeBundleStatement(
            request=request,
            selected_columns_by_stream=selected_column_groups,
            source_snapshot_token_columns_by_stream=snapshot_token_columns,
        )

    def acquire(
        self,
        request: SnowflakeBundleRequest,
        *,
        prepared_statement: SnowflakeBundleStatement | None = None,
    ) -> SnowflakeBundleAcquisition:
        """Execute exactly one generated statement, validate metadata, and seal bytes."""

        statement = self.build_statement(request)
        if prepared_statement is not None:
            prepared_statement = _validated_bundle_statement(prepared_statement)
            if prepared_statement != statement:
                raise SnowflakeBundleError("prepared bundle statement does not match the configured statement")
            statement = prepared_statement
        credentials = self._credential_provider.load_key_pair()
        if not isinstance(credentials, SnowflakeKeyPairCredentials):
            raise SnowflakeBundleError("credential provider returned an invalid key-pair value")
        bundle_result: object | None = None
        is_complete = False
        try:
            bundle_result = self._adapter.fetch_bundle(statement, credentials)
            if not isinstance(bundle_result, SnowflakeBundleResult):
                raise SnowflakeBundleError("bundle adapter returned an invalid result")
            try:
                acquisition = _seal_bundle(statement, bundle_result, self._capture_limits)
            except SnowflakeBundleError:
                raise
            except SnowflakeConnectorError as exc:
                raise SnowflakeBundleError("bundle result cannot be sealed") from exc
            is_complete = True
            return acquisition
        finally:
            if isinstance(bundle_result, SnowflakeBundleResult):
                if is_complete:
                    bundle_result.close()
                else:
                    _close_after_failure(bundle_result)

    def _approved_columns(
        self,
        request: SnowflakeBundleRequest,
        binding: SnowflakeBundleBinding,
    ) -> tuple[SnowflakeDeclaredColumn, ...]:
        approved_relation = self._approved_by_relation.get(binding.relation.parts)
        if approved_relation is None:
            raise SnowflakeBundleError("bundle relation identifier is not approved")
        selected_columns = tuple(approved_relation.column_for(field_id) for field_id in binding.selected_field_ids)
        if any(column is None for column in selected_columns):
            raise SnowflakeBundleError("bundle selected field is not declared for the approved relation")
        columns = tuple(column for column in selected_columns if column is not None)
        selected_by_column = {column.column_identifier: column.field_id for column in columns}
        for alias in request.definition.aliases:
            if alias.stream_id == binding.stream_id and selected_by_column.get(alias.source_label) != alias.field_id:
                raise SnowflakeBundleError("bundle source aliases do not match the selected column bindings")
        return columns

    def _approved_snapshot_token_column(self, binding: SnowflakeBundleBinding) -> SnowflakeDeclaredColumn:
        approved_relation = self._approved_by_relation.get(binding.source_snapshot_token_relation.parts)
        if approved_relation is None:
            raise SnowflakeBundleError("bundle source snapshot relation identifier is not approved")
        column = approved_relation.column_for(binding.semantic_token_metadata_key)
        if column is None:
            raise SnowflakeBundleError("bundle source snapshot token column is not declared for the approved relation")
        return column


def _close_after_failure(result: SnowflakeBundleResult) -> None:
    try:
        result.close()
    except Exception:
        return None


def _bundle_snapshot_token(
    statement: SnowflakeBundleStatement,
    bundle_result: SnowflakeBundleResult,
) -> str:
    expected_bindings = statement.request.bindings
    if tuple(stream_result.metadata.stream_id for stream_result in bundle_result.stream_results) != tuple(
        binding.stream_id for binding in expected_bindings
    ):
        raise SnowflakeBundleError("bundle result streams must retain configured stream order")
    snapshot_tokens_by_stream = {}
    for binding, stream_result in zip(expected_bindings, bundle_result.stream_results, strict=True):
        if stream_result.metadata.semantic_token_metadata_key != binding.semantic_token_metadata_key:
            raise SnowflakeBundleError("bundle result semantic token metadata does not match configuration")
        snapshot_tokens_by_stream[binding.stream_id] = stream_result.metadata.source_snapshot_tokens
    try:
        return validate_source_snapshot_tokens(statement.request.definition, snapshot_tokens_by_stream)
    except SourceSnapshotError as exc:
        raise SnowflakeBundleError("bundle streams require one shared semantic snapshot token") from exc


def _seal_bundle(
    statement: SnowflakeBundleStatement,
    bundle_result: SnowflakeBundleResult,
    capture_limits: CaptureLimits,
) -> SnowflakeBundleAcquisition:
    """Capture every ordered source stream after its shared token is verified."""

    expected_bindings = statement.request.bindings
    stream_captures = []
    compressed_bytes = decoded_bytes = partition_count = 0
    source_snapshot_token = _bundle_snapshot_token(statement, bundle_result)
    streams_by_id = {stream.stream_id: stream for stream in statement.request.definition.source_streams}
    for binding, expected_columns, stream_result in zip(
        expected_bindings,
        statement.selected_columns_by_stream,
        bundle_result.stream_results,
        strict=True,
    ):
        parquet_result = stream_result.parquet_result
        if parquet_result.source_snapshot_token != source_snapshot_token:
            raise SnowflakeBundleError("bundle Parquet result token does not match semantic metadata")
        if tuple(column.field_id for column in parquet_result.schema) != tuple(
            column.field_id for column in expected_columns
        ):
            raise SnowflakeBundleError("bundle result schema does not match selected fields")
        captures, compressed_bytes, decoded_bytes, partition_count = _capture_stream_result(
            parquet_result,
            stream=streams_by_id[binding.stream_id],
            source_snapshot_token=source_snapshot_token,
            capture_limits=capture_limits,
            compressed_bytes=compressed_bytes,
            decoded_bytes=decoded_bytes,
            partition_count=partition_count,
        )
        stream_captures.append(
            SnowflakeBundleStreamCapture(
                stream_id=binding.stream_id,
                schema=parquet_result.schema,
                captures=captures,
            )
        )
    return SnowflakeBundleAcquisition(
        statement=statement,
        source_snapshot_token=source_snapshot_token,
        stream_captures=tuple(stream_captures),
        capture_limits=capture_limits,
        diagnostic_query_id=bundle_result.query_id,
    )


def _capture_stream_result(
    parquet_result: SnowflakeParquetResult,
    *,
    stream: SourceStream,
    source_snapshot_token: str,
    capture_limits: CaptureLimits,
    compressed_bytes: int,
    decoded_bytes: int,
    partition_count: int,
) -> tuple[tuple[SealedCapture, ...], int, int, int]:
    captures = []
    stream_compressed_bytes = 0
    partition_iterator = parquet_result.consume_partition_sources()
    try:
        for partition_source in partition_iterator:
            parquet_result.claim_partition_source(partition_source)
            try:
                if len(captures) >= MAX_RESULT_PARTITIONS:
                    raise SnowflakeBundleError("bundle stream partitions exceed the result limit")
                if partition_count >= MAX_BUNDLE_PARTITIONS:
                    raise SnowflakeBundleError("bundle result partitions exceed the manifest limit")
                if not callable(getattr(partition_source, "read", None)):
                    raise SnowflakeBundleError("bundle result partition source must be a binary reader")
                partition_limits = _remaining_partition_limits(
                    capture_limits,
                    compressed_bytes=compressed_bytes,
                    decoded_bytes=decoded_bytes,
                    stream_compressed_bytes=stream_compressed_bytes,
                )
                try:
                    capture = capture_stream(
                        partition_source,
                        stream,
                        source_snapshot_token=source_snapshot_token,
                        limits=partition_limits,
                    )
                except (CaptureError, OSError) as exc:
                    raise SnowflakeBundleError("bundle result partition cannot be captured") from exc
                compressed_bytes += capture.manifest.compressed_bytes
                stream_compressed_bytes += capture.manifest.compressed_bytes
                decoded_bytes += capture.manifest.decoded_bytes
                partition_count += 1
                captures.append(capture)
            finally:
                parquet_result.close_partition_source(partition_source)
    finally:
        parquet_result.close_partition_iterator()
    return tuple(captures), compressed_bytes, decoded_bytes, partition_count


_REPLAY_EXPORT_NAMES = frozenset(
    {
        "SnowflakeBundleReplay",
        "SnowflakeBundleReplayStream",
        "prepare_bundle_replay",
        "reconstruct_replayable_parquet_bundle",
        "replayable_parquet_captures",
    }
)


def __getattr__(name: str) -> Any:
    """Lazily preserve bundle replay imports without creating a circular import."""

    if name not in _REPLAY_EXPORT_NAMES:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from process.custom_import import snowflake_bundle_replay

    return getattr(snowflake_bundle_replay, name)
