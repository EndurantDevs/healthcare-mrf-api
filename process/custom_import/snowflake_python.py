# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Python-connector runtime for bounded Snowflake custom-import reads."""

from __future__ import annotations

import re
from collections.abc import Iterator, Sequence
from decimal import Decimal
from io import BytesIO
from typing import Any, BinaryIO

import pyarrow as pa
import pyarrow.parquet as pq
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from snowflake.connector.constants import FIELD_TYPES as SNOWFLAKE_FIELD_TYPES

from process.custom_import.family import SourceSnapshotError, validate_source_snapshot_tokens
from process.custom_import.snowflake import (
    MAX_RESULT_PARTITION_BYTES,
    SnowflakeConnectorError,
    SnowflakeCredentialError,
    SnowflakeKeyPairCredentials,
    SnowflakeParquetResult,
    SnowflakeReadStatement,
    SnowflakeResultColumn,
)
from process.custom_import.snowflake_bundle import (
    _BUNDLE_ROW_KIND_COLUMN,
    _DATA_ROW_KIND,
    _METADATA_ROW_KIND,
    _SOURCE_SNAPSHOT_TOKEN_COLUMN,
    _STREAM_ID_COLUMN,
    _STREAM_ORDINAL_COLUMN,
    SnowflakeBundleResult,
    SnowflakeBundleStatement,
    SnowflakeBundleStreamMetadata,
    SnowflakeBundleStreamResult,
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]{0,254}$")
_FETCH_ROWS = 1_024
_QUERY_TAG = "custom-import/v1-snowflake"
_LOGIN_TIMEOUT_SECONDS = 30
_NETWORK_TIMEOUT_SECONDS = 120
_STATEMENT_TIMEOUT_SECONDS = 120
_SUPPORTED_SOURCE_TYPES = frozenset({"BOOLEAN", "FIXED", "TEXT"})
_FIXED_SOURCE_TYPE = re.compile(r"^FIXED\(([1-9]|[1-2][0-9]|3[0-8]),([0-9]|[1-2][0-9]|3[0-7])\)$")
_MIN_INT64 = -(2**63)
_MAX_INT64 = 2**63 - 1


class SnowflakePythonConnectorAdapter:
    """Run the connector-generated SELECT and return bounded Parquet batches."""

    def __init__(self, *, role: str, warehouse: str) -> None:
        self._role = _identifier(role, "role")
        self._warehouse = _identifier(warehouse, "warehouse")

    def fetch_parquet(
        self,
        statement: SnowflakeReadStatement,
        credentials: SnowflakeKeyPairCredentials,
    ) -> SnowflakeParquetResult:
        """Execute one generated single-stream statement and retain its query identity."""

        if not isinstance(statement, SnowflakeReadStatement):
            raise SnowflakeConnectorError("runtime adapter requires a generated Snowflake statement")
        if not isinstance(credentials, SnowflakeKeyPairCredentials):
            raise SnowflakeCredentialError("runtime adapter requires key-pair credentials")
        connection = None
        cursor = None
        try:
            connection = self._connect(credentials)
            cursor = connection.cursor()
            cursor.execute(statement.sql)
            query_id = getattr(cursor, "sfqid", None)
            if not isinstance(query_id, str) or not query_id.strip():
                raise SnowflakeConnectorError("Snowflake did not return a statement identity")
            result_schema = _result_schema(statement, cursor.description)
            parquet_result = SnowflakeParquetResult(
                source_snapshot_token=f"snowflake-query:{query_id}",
                schema=result_schema,
                partition_sources=_SnowflakeParquetPartitionSources(
                    connection=connection,
                    cursor=cursor,
                    result_schema=result_schema,
                ),
            )
            connection = None
            cursor = None
            return parquet_result
        except SnowflakeConnectorError:
            _best_effort_close(cursor, connection)
            raise
        except Exception as exc:
            _best_effort_close(cursor, connection)
            raise SnowflakeConnectorError("Snowflake read failed") from exc
        except BaseException:
            _best_effort_close(cursor, connection)
            raise

    def fetch_bundle(
        self,
        statement: SnowflakeBundleStatement,
        credentials: SnowflakeKeyPairCredentials,
    ) -> SnowflakeBundleResult:
        """Execute one generated bundle statement with one shared query receipt."""

        if not isinstance(statement, SnowflakeBundleStatement):
            raise SnowflakeConnectorError("runtime adapter requires a generated Snowflake bundle statement")
        if not isinstance(credentials, SnowflakeKeyPairCredentials):
            raise SnowflakeCredentialError("runtime adapter requires key-pair credentials")
        connection = None
        cursor = None
        try:
            connection = self._connect(credentials)
            cursor = connection.cursor()
            cursor.execute(statement.sql)
            query_id = getattr(cursor, "sfqid", None)
            if not isinstance(query_id, str) or not query_id.strip():
                raise SnowflakeConnectorError("Snowflake did not return a statement identity")
            schemas = _bundle_result_schemas(statement, cursor.description)
            metadata, source_snapshot_token, pending_row = _bundle_stream_metadata(statement, cursor)
            partition_sources = _SnowflakeBundlePartitionSources(
                connection=connection,
                cursor=cursor,
                statement=statement,
                schemas=schemas,
                pending_row=pending_row,
            )
            bundle_result = SnowflakeBundleResult(
                stream_results=tuple(
                    SnowflakeBundleStreamResult(
                        metadata=stream_metadata,
                        parquet_result=SnowflakeParquetResult(
                            source_snapshot_token=source_snapshot_token,
                            schema=schema,
                            partition_sources=partition_sources.stream_sources(index),
                        ),
                    )
                    for index, (stream_metadata, schema) in enumerate(zip(metadata, schemas, strict=True))
                ),
                query_id=query_id,
            )
            connection = None
            cursor = None
            return bundle_result
        except SnowflakeConnectorError:
            _best_effort_close(cursor, connection)
            raise
        except Exception as exc:
            _best_effort_close(cursor, connection)
            raise SnowflakeConnectorError("Snowflake bundle read failed") from exc
        except BaseException:
            _best_effort_close(cursor, connection)
            raise

    def _connect(self, credentials: SnowflakeKeyPairCredentials) -> Any:
        """Open the fixed policy connection for bundle reads."""

        return snowflake.connector.connect(
            account=credentials.account,
            user=credentials.user,
            authenticator="SNOWFLAKE_JWT",
            private_key=_private_key_der(credentials),
            role=self._role,
            warehouse=self._warehouse,
            autocommit=False,
            client_session_keep_alive=False,
            login_timeout=_LOGIN_TIMEOUT_SECONDS,
            network_timeout=_NETWORK_TIMEOUT_SECONDS,
            socket_timeout=_NETWORK_TIMEOUT_SECONDS,
            session_parameters={
                "QUERY_TAG": _QUERY_TAG,
                "STATEMENT_TIMEOUT_IN_SECONDS": _STATEMENT_TIMEOUT_SECONDS,
            },
        )


class _SnowflakeParquetPartitionSources:
    """Single-use cursor batches which close their exact connection on exit."""

    def __init__(
        self,
        *,
        connection: Any,
        cursor: Any,
        result_schema: tuple[SnowflakeResultColumn, ...],
    ) -> None:
        self._connection = connection
        self._cursor = cursor
        self._result_schema = result_schema
        self._started = False
        self._closed = False

    def __iter__(self) -> Iterator[BinaryIO]:
        if self._started:
            raise SnowflakeConnectorError("Snowflake result batches were already consumed")
        self._started = True
        has_emitted_partition = False
        has_primary_failure = False
        has_source_exhausted = False
        pending_row: Sequence[object] | None = None
        try:
            while not has_source_exhausted:
                partition_rows, pending_row, has_source_exhausted = self._next_partition_rows(pending_row)
                if partition_rows:
                    has_emitted_partition = True
                    yield _parquet_reader(partition_rows, self._result_schema)
            if not has_emitted_partition:
                yield _parquet_reader((), self._result_schema)
        except SnowflakeConnectorError:
            has_primary_failure = True
            raise
        except Exception as exc:
            has_primary_failure = True
            raise SnowflakeConnectorError("Snowflake result fetch failed") from exc
        except BaseException:
            has_primary_failure = True
            raise
        finally:
            if has_primary_failure:
                _best_effort_close(self)
            else:
                self.close()

    def _next_partition_rows(
        self,
        pending_row: Sequence[object] | None,
    ) -> tuple[list[Sequence[object]], Sequence[object] | None, bool]:
        partition_rows: list[Sequence[object]] = []
        variable_bytes = 0
        result_row = pending_row
        while len(partition_rows) < _FETCH_ROWS:
            if result_row is None:
                result_row = self._cursor.fetchone()
                if result_row is None:
                    return partition_rows, None, True
            row_variable_bytes = _result_row_variable_bytes(result_row, self._result_schema)
            decoded_bytes = (
                variable_bytes
                + row_variable_bytes
                + _fixed_batch_bytes(
                    self._result_schema,
                    len(partition_rows) + 1,
                )
            )
            if decoded_bytes > MAX_RESULT_PARTITION_BYTES:
                if not partition_rows:
                    raise SnowflakeConnectorError("Snowflake result batch exceeds the decoded-byte limit")
                return partition_rows, result_row, False
            partition_rows.append(result_row)
            variable_bytes += row_variable_bytes
            result_row = None
        return partition_rows, None, False

    def close(self) -> None:
        """Close the cursor and connection exactly once, attempting both."""

        if self._closed:
            return
        self._closed = True
        cursor, connection = self._cursor, self._connection
        self._cursor = None
        self._connection = None
        _close_resources(cursor, connection)


def _bundle_result_schemas(
    statement: SnowflakeBundleStatement,
    description: object,
) -> tuple[tuple[SnowflakeResultColumn, ...], ...]:
    """Split the one union result schema back into declared stream schemas."""

    fields = tuple(sorted(statement.request.definition.fields, key=lambda field: field.field_slot))
    expected_labels = (
        _BUNDLE_ROW_KIND_COLUMN,
        _STREAM_ORDINAL_COLUMN,
        _STREAM_ID_COLUMN,
        _SOURCE_SNAPSHOT_TOKEN_COLUMN,
        *(field.field_id for field in fields),
    )
    if not isinstance(description, Sequence) or len(description) != len(expected_labels):
        raise SnowflakeConnectorError("Snowflake bundle result schema does not match the selected fields")
    if tuple(getattr(metadata, "name", None) for metadata in description) != expected_labels:
        raise SnowflakeConnectorError("Snowflake bundle result schema does not match the selected fields")
    row_kind_type = _source_type(description[0])
    if _fixed_type_parts(row_kind_type)[1] != 0 or _is_nullable(description[0]):
        raise SnowflakeConnectorError("Snowflake bundle discriminator schema is invalid")
    ordinal_type = _source_type(description[1])
    if _fixed_type_parts(ordinal_type)[1] != 0 or _is_nullable(description[1]):
        raise SnowflakeConnectorError("Snowflake bundle discriminator schema is invalid")
    if _source_type(description[2]) != "TEXT" or _is_nullable(description[2]):
        raise SnowflakeConnectorError("Snowflake bundle discriminator schema is invalid")
    if _source_type(description[3]) != "TEXT":
        raise SnowflakeConnectorError("Snowflake bundle snapshot metadata schema is invalid")
    columns_by_field = {
        field.field_id: SnowflakeResultColumn(
            field_id=field.field_id,
            source_type=_source_type(metadata),
            nullable=_is_nullable(metadata),
        )
        for field, metadata in zip(fields, description[4:], strict=True)
    }
    return tuple(
        tuple(columns_by_field[column.field_id] for column in selected_columns)
        for selected_columns in statement.selected_columns_by_stream
    )


def _bundle_stream_metadata(
    statement: SnowflakeBundleStatement,
    cursor: Any,
) -> tuple[tuple[SnowflakeBundleStreamMetadata, ...], str, Sequence[object] | None]:
    """Read exactly one configured semantic-token observation before bundle rows."""

    expected_row_length = len(statement.request.definition.fields) + 4
    stream_metadata_items = []
    for ordinal, binding in enumerate(statement.request.bindings, start=1):
        result_row = cursor.fetchone()
        if result_row is None:
            raise SnowflakeConnectorError("Snowflake bundle metadata observations are incomplete")
        if (
            isinstance(result_row, (str, bytes, bytearray, memoryview))
            or not isinstance(result_row, Sequence)
            or len(result_row) != expected_row_length
            or _bundle_row_kind(result_row) != _METADATA_ROW_KIND
            or result_row[1] != ordinal
            or result_row[2] != binding.stream_id
            or any(metadata_value is not None for metadata_value in result_row[4:])
        ):
            raise SnowflakeConnectorError("Snowflake bundle metadata row does not match the generated statement")
        stream_metadata_items.append(
            SnowflakeBundleStreamMetadata(
                stream_id=binding.stream_id,
                semantic_token_metadata_key=binding.semantic_token_metadata_key,
                source_snapshot_tokens=(result_row[3],),
            )
        )
    pending_row = cursor.fetchone()
    if pending_row is not None and _bundle_row_kind(pending_row) != _DATA_ROW_KIND:
        raise SnowflakeConnectorError("Snowflake bundle metadata observations are not exactly one per stream")
    try:
        source_snapshot_token = validate_source_snapshot_tokens(
            statement.request.definition,
            {metadata_item.stream_id: metadata_item.source_snapshot_tokens for metadata_item in stream_metadata_items},
        )
    except SourceSnapshotError as exc:
        raise SnowflakeConnectorError("Snowflake bundle streams require one shared semantic snapshot token") from exc
    return tuple(stream_metadata_items), source_snapshot_token, pending_row


def _bundle_row_kind(result_row: Sequence[object]) -> int:
    value = result_row[0]
    if isinstance(value, bool) or not isinstance(value, int) or value not in {_METADATA_ROW_KIND, _DATA_ROW_KIND}:
        raise SnowflakeConnectorError("Snowflake bundle row kind is invalid")
    return value


class _SnowflakeBundlePartitionSources:
    """One cursor split into ordered stream readers without another source read."""

    def __init__(
        self,
        *,
        connection: Any,
        cursor: Any,
        statement: SnowflakeBundleStatement,
        schemas: tuple[tuple[SnowflakeResultColumn, ...], ...],
        pending_row: Sequence[object] | None,
    ) -> None:
        self._connection = connection
        self._cursor = cursor
        self._schemas = schemas
        self._stream_ids = tuple(binding.stream_id for binding in statement.request.bindings)
        fields = tuple(sorted(statement.request.definition.fields, key=lambda field: field.field_slot))
        output_index_by_field = {field.field_id: index for index, field in enumerate(fields, start=4)}
        self._field_indexes_by_stream = tuple(
            tuple(output_index_by_field[column.field_id] for column in selected_columns)
            for selected_columns in statement.selected_columns_by_stream
        )
        self._partition_rows = statement.request.encoding.partition_rows
        self._expected_row_length = len(fields) + 4
        self._next_stream_index = 0
        self._pending_row = pending_row
        self._closed_stream_indexes: set[int] = set()
        self._closed = False
        self._stream_sources = tuple(
            _SnowflakeBundleStreamPartitionSources(self, stream_index) for stream_index in range(len(self._stream_ids))
        )

    def stream_sources(self, stream_index: int) -> _SnowflakeBundleStreamPartitionSources:
        """Return the one ordered source collection for a configured stream."""

        return self._stream_sources[stream_index]

    def next_partition_rows(self, stream_index: int) -> tuple[list[Sequence[object]], bool]:
        """Return the next deterministic partition for one configured stream."""

        if self._closed:
            raise SnowflakeConnectorError("Snowflake bundle result is closed")
        if stream_index != self._next_stream_index:
            raise SnowflakeConnectorError("Snowflake bundle streams must be consumed in configured order")
        partition_rows: list[Sequence[object]] = []
        variable_bytes = 0
        expected_ordinal = stream_index + 1
        while len(partition_rows) < self._partition_rows:
            result_row = self._pending_row
            if result_row is None:
                result_row = self._cursor.fetchone()
            else:
                self._pending_row = None
            if result_row is None:
                return partition_rows, True
            ordinal, selected_values = self._split_row(result_row, stream_index)
            if ordinal > expected_ordinal:
                self._pending_row = result_row
                return partition_rows, True
            if ordinal < expected_ordinal:
                raise SnowflakeConnectorError("Snowflake bundle result rows are not ordered by stream")
            schema = self._schemas[stream_index]
            row_variable_bytes = _result_row_variable_bytes(selected_values, schema)
            decoded_bytes = variable_bytes + row_variable_bytes + _fixed_batch_bytes(schema, len(partition_rows) + 1)
            if decoded_bytes > MAX_RESULT_PARTITION_BYTES:
                if not partition_rows:
                    raise SnowflakeConnectorError("Snowflake bundle result batch exceeds the decoded-byte limit")
                self._pending_row = result_row
                return partition_rows, False
            partition_rows.append(selected_values)
            variable_bytes += row_variable_bytes
        return partition_rows, False

    def finish_stream(self, stream_index: int) -> None:
        """Advance only after this stream's final partition has been yielded."""

        if self._closed or stream_index != self._next_stream_index:
            raise SnowflakeConnectorError("Snowflake bundle streams are not contiguous")
        self._next_stream_index += 1

    def close_stream(self, stream_index: int) -> None:
        """Release one stream and close the shared cursor after the final release."""

        self._closed_stream_indexes.add(stream_index)
        if len(self._closed_stream_indexes) == len(self._stream_ids):
            self.close()

    def close(self) -> None:
        """Close the sole cursor and connection once every stream is released."""

        if self._closed:
            return
        self._closed = True
        cursor, connection = self._cursor, self._connection
        self._cursor = None
        self._connection = None
        _close_resources(cursor, connection)

    def _split_row(self, result_row: object, stream_index: int) -> tuple[int, Sequence[object]]:
        if isinstance(result_row, (str, bytes, bytearray, memoryview)) or not isinstance(result_row, Sequence):
            raise SnowflakeConnectorError("Snowflake bundle result row does not match the generated statement")
        if len(result_row) != self._expected_row_length:
            raise SnowflakeConnectorError("Snowflake bundle result row does not match the generated statement")
        if _bundle_row_kind(result_row) != _DATA_ROW_KIND:
            raise SnowflakeConnectorError("Snowflake bundle data row does not match the generated statement")
        ordinal = result_row[1]
        if isinstance(ordinal, bool) or not isinstance(ordinal, int) or not 1 <= ordinal <= len(self._stream_ids):
            raise SnowflakeConnectorError("Snowflake bundle stream ordinal is invalid")
        if result_row[2] != self._stream_ids[ordinal - 1] or result_row[3] is not None:
            raise SnowflakeConnectorError("Snowflake bundle stream identity is invalid")
        return ordinal, tuple(result_row[index] for index in self._field_indexes_by_stream[stream_index])


class _SnowflakeBundleStreamPartitionSources:
    """The single-use source set exposed for one stream of a shared cursor."""

    def __init__(self, owner: _SnowflakeBundlePartitionSources, stream_index: int) -> None:
        self._owner = owner
        self._stream_index = stream_index
        self._started = False
        self._closed = False

    def __iter__(self) -> Iterator[BinaryIO]:
        if self._started:
            raise SnowflakeConnectorError("Snowflake bundle stream batches were already consumed")
        self._started = True
        has_emitted_partition = False
        has_primary_failure = False
        try:
            while True:
                partition_rows, is_complete = self._owner.next_partition_rows(self._stream_index)
                if partition_rows:
                    has_emitted_partition = True
                    yield _parquet_reader(partition_rows, self._owner._schemas[self._stream_index])
                if is_complete:
                    if not has_emitted_partition:
                        yield _parquet_reader((), self._owner._schemas[self._stream_index])
                    self._owner.finish_stream(self._stream_index)
                    return
        except SnowflakeConnectorError:
            has_primary_failure = True
            raise
        except Exception as exc:
            has_primary_failure = True
            raise SnowflakeConnectorError("Snowflake bundle result fetch failed") from exc
        except BaseException:
            has_primary_failure = True
            raise
        finally:
            if has_primary_failure:
                _best_effort_close(self._owner)

    def close(self) -> None:
        """Release this stream's share of the one underlying cursor."""

        if self._closed:
            return
        self._closed = True
        self._owner.close_stream(self._stream_index)


def _identifier(value: object, label: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise SnowflakeConnectorError(f"Snowflake {label} must be a simple identifier")
    return value.upper()


def _private_key_der(credentials: SnowflakeKeyPairCredentials) -> bytes:
    try:
        private_key = serialization.load_pem_private_key(
            credentials.private_key_pem,
            password=credentials.private_key_passphrase,
        )
        return private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    except (TypeError, ValueError) as exc:
        raise SnowflakeCredentialError("private key material cannot be loaded") from exc


def _result_schema(statement: SnowflakeReadStatement, description: object) -> tuple[SnowflakeResultColumn, ...]:
    if not isinstance(description, Sequence) or len(description) != len(statement.request.selected_columns):
        raise SnowflakeConnectorError("Snowflake result schema does not match the selected fields")
    result_columns = []
    for column, metadata in zip(statement.request.selected_columns, description, strict=True):
        if getattr(metadata, "name", None) != column.field_id:
            raise SnowflakeConnectorError("Snowflake result schema does not match the selected fields")
        result_columns.append(
            SnowflakeResultColumn(
                field_id=column.field_id,
                source_type=_source_type(metadata),
                nullable=_is_nullable(metadata),
            )
        )
    return tuple(result_columns)


def _source_type(metadata: object) -> str:
    type_name = getattr(metadata, "type_name", None)
    type_code = getattr(metadata, "type_code", None)
    if (
        type_name is None
        and isinstance(type_code, int)
        and not isinstance(type_code, bool)
        and 0 <= type_code < len(SNOWFLAKE_FIELD_TYPES)
    ):
        type_name = SNOWFLAKE_FIELD_TYPES[type_code].name
    if isinstance(type_name, str) and type_name:
        normalized = type_name.upper()
        if normalized == "FIXED":
            precision = _metadata_integer(metadata, "precision", minimum=1, maximum=38)
            scale = _metadata_integer(metadata, "scale", minimum=0, maximum=min(precision, 37))
            return f"FIXED({precision},{scale})"
        if normalized in _SUPPORTED_SOURCE_TYPES:
            return normalized
        raise SnowflakeConnectorError("Snowflake result column type is not supported by the capture runtime")
    raise SnowflakeConnectorError("Snowflake result column type is unavailable")


def _metadata_integer(metadata: object, name: str, *, minimum: int, maximum: int) -> int:
    number = getattr(metadata, name, None)
    if isinstance(number, bool) or not isinstance(number, int) or not minimum <= number <= maximum:
        raise SnowflakeConnectorError(f"Snowflake result {name} is invalid")
    return number


def _is_nullable(metadata: object) -> bool:
    is_nullable = getattr(metadata, "is_nullable", None)
    if is_nullable is None:
        return True
    if not isinstance(is_nullable, bool):
        raise SnowflakeConnectorError("Snowflake result nullability is invalid")
    return is_nullable


def _parquet_reader(
    result_rows: Sequence[Sequence[object]],
    result_schema: tuple[SnowflakeResultColumn, ...],
) -> BytesIO:
    _validate_result_rows(result_rows, result_schema)
    result_columns = tuple(zip(*result_rows, strict=True)) if result_rows else ((),) * len(result_schema)
    try:
        table = pa.Table.from_arrays(
            [
                pa.array(column_values, type=_arrow_type(result_column))
                for column_values, result_column in zip(result_columns, result_schema, strict=True)
            ],
            schema=pa.schema(
                [
                    pa.field(
                        result_column.field_id,
                        _arrow_type(result_column),
                        nullable=result_column.nullable,
                    )
                    for result_column in result_schema
                ]
            ),
        )
        if table.nbytes > MAX_RESULT_PARTITION_BYTES:
            raise SnowflakeConnectorError("Snowflake result batch exceeds the decoded-byte limit")
        destination = BytesIO()
        pq.write_table(table, destination, compression="zstd")
    except SnowflakeConnectorError:
        raise
    except (pa.ArrowException, OverflowError, TypeError, ValueError) as exc:
        raise SnowflakeConnectorError("Snowflake result cannot be encoded as Parquet") from exc
    if destination.tell() > MAX_RESULT_PARTITION_BYTES:
        destination.close()
        raise SnowflakeConnectorError("Snowflake Parquet partition exceeds the byte limit")
    destination.seek(0)
    return destination


def _validate_result_rows(
    result_rows: Sequence[Sequence[object]],
    result_schema: tuple[SnowflakeResultColumn, ...],
) -> None:
    """Reject malformed or oversized decoded batches before Arrow allocation."""

    decoded_bytes = _fixed_batch_bytes(result_schema, len(result_rows))
    for result_row in result_rows:
        decoded_bytes += _result_row_variable_bytes(result_row, result_schema)
        if decoded_bytes > MAX_RESULT_PARTITION_BYTES:
            raise SnowflakeConnectorError("Snowflake result batch exceeds the decoded-byte limit")


def _result_row_variable_bytes(
    result_row: object,
    result_schema: tuple[SnowflakeResultColumn, ...],
) -> int:
    if isinstance(result_row, (str, bytes, bytearray, memoryview)) or not isinstance(result_row, Sequence):
        raise SnowflakeConnectorError("Snowflake result row does not match the selected fields")
    if len(result_row) != len(result_schema):
        raise SnowflakeConnectorError("Snowflake result row does not match the selected fields")
    return sum(
        _variable_scalar_bytes(scalar_value, result_column)
        for scalar_value, result_column in zip(result_row, result_schema, strict=True)
    )


def _fixed_batch_bytes(result_schema: tuple[SnowflakeResultColumn, ...], row_count: int) -> int:
    return sum(_arrow_fixed_bytes(column.source_type, row_count) for column in result_schema)


def _arrow_fixed_bytes(source_type: str, row_count: int) -> int:
    null_bitmap_bytes = (row_count + 7) // 8
    if source_type == "TEXT":
        return null_bitmap_bytes + 4 * (row_count + 1)
    if source_type == "BOOLEAN":
        return 2 * null_bitmap_bytes
    if not _uses_decimal_storage(source_type):
        return null_bitmap_bytes + 8 * row_count
    return null_bitmap_bytes + 16 * row_count


def _variable_scalar_bytes(scalar_value: object, result_column: SnowflakeResultColumn) -> int:
    if scalar_value is None:
        if not result_column.nullable:
            raise SnowflakeConnectorError("Snowflake non-nullable result cannot be null")
        return 0
    source_type = result_column.source_type
    if source_type == "TEXT":
        if not isinstance(scalar_value, str) or len(scalar_value) > MAX_RESULT_PARTITION_BYTES:
            raise SnowflakeConnectorError("Snowflake TEXT result value is invalid or oversized")
        try:
            return len(scalar_value.encode("utf-8"))
        except UnicodeEncodeError as exc:
            raise SnowflakeConnectorError("Snowflake TEXT result value is not valid UTF-8") from exc
    if source_type == "BOOLEAN":
        if not isinstance(scalar_value, bool):
            raise SnowflakeConnectorError("Snowflake BOOLEAN result value is invalid")
        return 0
    if not _uses_decimal_storage(source_type):
        if (
            isinstance(scalar_value, bool)
            or not isinstance(scalar_value, int)
            or not _MIN_INT64 <= scalar_value <= _MAX_INT64
        ):
            raise SnowflakeConnectorError("Snowflake scale-zero FIXED result must fit a signed 64-bit integer")
        return 0
    if isinstance(scalar_value, bool) or not isinstance(scalar_value, (Decimal, int)):
        raise SnowflakeConnectorError("Snowflake FIXED result value is invalid")
    return 0


def _arrow_type(result_column: SnowflakeResultColumn) -> pa.DataType:
    if result_column.source_type == "TEXT":
        return pa.string()
    if result_column.source_type == "BOOLEAN":
        return pa.bool_()
    precision, scale = _fixed_type_parts(result_column.source_type)
    if not _uses_decimal_storage(result_column.source_type):
        return pa.int64()
    return pa.decimal128(precision, scale)


def _uses_decimal_storage(source_type: str) -> bool:
    if source_type in {"TEXT", "BOOLEAN"}:
        return False
    precision, scale = _fixed_type_parts(source_type)
    return scale != 0 or precision > 18


def _fixed_type_parts(source_type: str) -> tuple[int, int]:
    fixed_match = _FIXED_SOURCE_TYPE.fullmatch(source_type)
    if fixed_match is None:
        raise SnowflakeConnectorError("Snowflake result column type is not supported by the Parquet encoder")
    return int(fixed_match.group(1)), int(fixed_match.group(2))


def _close_resources(*resources: object | None) -> None:
    errors: list[BaseException] = []
    for resource in resources:
        close = getattr(resource, "close", None)
        if callable(close):
            try:
                close()
            except BaseException as exc:
                errors.append(exc)
    if errors:
        raise SnowflakeConnectorError("Snowflake resource cleanup failed") from errors[0]


def _best_effort_close(*resources: object | None) -> None:
    try:
        _close_resources(*resources)
    except SnowflakeConnectorError:
        return
