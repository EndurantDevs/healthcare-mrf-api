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

from process.custom_import.snowflake import (
    MAX_RESULT_PARTITION_BYTES,
    SnowflakeConnectorError,
    SnowflakeCredentialError,
    SnowflakeKeyPairCredentials,
    SnowflakeParquetResult,
    SnowflakeReadStatement,
    SnowflakeResultColumn,
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
        """Execute exactly one generated statement and retain its query identity."""

        if not isinstance(statement, SnowflakeReadStatement):
            raise SnowflakeConnectorError("runtime adapter requires a generated Snowflake statement")
        if not isinstance(credentials, SnowflakeKeyPairCredentials):
            raise SnowflakeCredentialError("runtime adapter requires key-pair credentials")
        connection = None
        cursor = None
        try:
            connection = snowflake.connector.connect(
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
            cursor = connection.cursor()
            cursor.execute(statement.sql)
            query_id = getattr(cursor, "sfqid", None)
            if not isinstance(query_id, str) or not query_id.strip():
                raise SnowflakeConnectorError("Snowflake did not return a statement identity")
            result_schema = _result_schema(statement, cursor.description)
            partition_sources = _SnowflakeParquetPartitionSources(
                connection=connection,
                cursor=cursor,
                result_schema=result_schema,
            )
            parquet_result = SnowflakeParquetResult(
                source_snapshot_token=f"snowflake-query:{query_id}",
                schema=result_schema,
                partition_sources=partition_sources,
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
    if _fixed_type_parts(source_type)[1] == 0:
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
    _, scale = _fixed_type_parts(source_type)
    if scale == 0:
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
    if scale == 0:
        return pa.int64()
    return pa.decimal128(precision, scale)


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
