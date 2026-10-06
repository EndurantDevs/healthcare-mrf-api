# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Python-connector runtime for bounded Snowflake custom-import reads."""

from __future__ import annotations

import re
import warnings
from collections import deque
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, replace
from decimal import Decimal
from io import BytesIO
from typing import Any, BinaryIO

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

with warnings.catch_warnings():
    # Arrow 25 supports row/Parquet reads; only the optional pandas extra caps it.
    warnings.filterwarnings(
        "ignore",
        message=r"You have an incompatible version of 'pyarrow' installed \(25\.0\.1\), ",
        category=UserWarning,
        module=r"snowflake\.connector\.options$",
    )
    import snowflake.connector
from cryptography.hazmat.primitives import serialization
from snowflake.connector.constants import FIELD_TYPES as SNOWFLAKE_FIELD_TYPES

from process.custom_import.capture import (
    CaptureError,
    SealedCapture,
    _decoded_record,
    _iter_parquet_batch_records,
    _open_parquet_reader,
    _validate_parquet_envelope,
    _validate_parquet_page_preflight,
    _validated_parquet_metadata,
    _validated_parquet_schema,
    capture_stream,
)
from process.custom_import.capture_limits import CaptureLimits
from process.custom_import.family import (
    SourceSnapshotError,
    is_decimal_scalar_storage_valid,
    validate_source_snapshot_tokens,
)
from process.custom_import.snowflake import (
    MAX_RESULT_BYTES,
    MAX_RESULT_PARTITION_BYTES,
    MAX_RESULT_PARTITIONS,
    SnowflakeConnectorError,
    SnowflakeCredentialError,
    SnowflakeCredentialProvider,
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
    MAX_BUNDLE_CAPTURE_BYTES,
    MAX_BUNDLE_PARTITIONS,
    MAX_STREAM_CAPTURE_BYTES,
    SnowflakeBundleResult,
    SnowflakeBundleStatement,
    SnowflakeBundleStreamMetadata,
    SnowflakeBundleStreamResult,
    _bundle_source_field_ids,
    _bundle_source_key,
    _capture_limits,
    _query_identity_snapshot_token,
)
from process.custom_import.snowflake_inspection import SnowflakeInspectionStatement
from process.custom_import.snowflake_preflight import SnowflakePreflightStatement
from process.custom_import.snowflake_preflight_schema import (
    _fixed_type_parts,
    _is_integral_fixed,
    _metadata_integer,
    convert_snowflake_float,
    validate_decimal_conversion_sources,
    validate_preflight_result_schema,
)
from process.custom_import.snowflake_preflight_schema import (
    _source_type as _preflight_source_type,
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]{0,254}$")
_FETCH_ROWS = 1_024
_QUERY_TAG = "custom-import/v1-snowflake"
_LOGIN_TIMEOUT_SECONDS = 30
_NETWORK_TIMEOUT_SECONDS = 120
_STATEMENT_TIMEOUT_SECONDS = 120
_MIN_INT64 = -(2**63)
_MAX_INT64 = 2**63 - 1


def _execute_filtered_statement(cursor, statement) -> None:
    """Bind only parameters rebuilt from the sealed declarative statement."""

    if replace(statement) != statement:
        raise SnowflakeConnectorError("generated Snowflake statement has a stale identity seal")
    if statement.parameters:
        cursor.execute(statement.sql, statement.parameters)
    else:
        cursor.execute(statement.sql)


@dataclass(frozen=True)
class SnowflakeLandingPart:
    """One bounded projection, provisional until every source EOF is observed."""

    stream_id: str
    ordinal: int
    capture: SealedCapture
    record_count: int
    arrow_byte_count: int


@dataclass(frozen=True)
class SnowflakeLandingEOF:
    """One stream's compact completion evidence after actual cursor exhaustion."""

    stream_id: str
    part_count: int
    record_count: int


class SnowflakeBundleLandingResult:
    """Own one landing cursor; open, advance and close on its affinity thread."""

    def __init__(
        self,
        *,
        owner: _SnowflakeBundlePartitionSources,
        query_id: str,
        source_snapshot_token: str,
        metadata: tuple[SnowflakeBundleStreamMetadata, ...],
        schemas: tuple[tuple[SnowflakeResultColumn, ...], ...],
    ) -> None:
        self.query_id = query_id
        self.source_snapshot_token = source_snapshot_token
        self.metadata = metadata
        self.schemas = schemas
        self._owner = owner
        self._iterator: Iterator[SnowflakeLandingPart | SnowflakeLandingEOF] | None = None
        self._closed = False

    def consume_events(self) -> Iterator[SnowflakeLandingPart | SnowflakeLandingEOF]:
        """Transfer the single-use iterator, retaining no yielded event history."""

        if self._closed or self._iterator is not None:
            raise SnowflakeConnectorError("Snowflake landing events were already consumed or closed")
        self._iterator = self._owner.landing_events(self.source_snapshot_token)
        return self._iterator

    def close(self) -> None:
        """Close a suspended iterator and its cursor even before first advance."""

        if self._closed:
            return
        self._closed = True
        _close_resources(self._iterator, self._owner)

    def __enter__(self) -> SnowflakeBundleLandingResult:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if exc_type is None:
            self.close()
        else:
            _best_effort_close(self)


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
            connection, cursor = self._connect(credentials)
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
            connection, cursor = self._connect(credentials)
            _execute_filtered_statement(cursor, statement)
            query_id = getattr(cursor, "sfqid", None)
            if not isinstance(query_id, str) or not query_id.strip():
                raise SnowflakeConnectorError("Snowflake did not return a statement identity")
            schemas = _bundle_result_schemas(statement, cursor.description)
            metadata, source_snapshot_token, pending_row = _bundle_stream_metadata(
                statement,
                cursor,
                query_id=query_id,
            )
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

    def open_bundle_landing(
        self,
        statement: SnowflakeBundleStatement,
        credentials: SnowflakeKeyPairCredentials,
        *,
        part_limits: CaptureLimits,
        maximum_part_arrow_bytes: int,
        timeout_seconds: int,
    ) -> SnowflakeBundleLandingResult:
        """Open one generated SELECT with explicit, separately admitted part limits."""

        if not isinstance(statement, SnowflakeBundleStatement):
            raise SnowflakeConnectorError("runtime adapter requires a generated Snowflake bundle statement")
        if not isinstance(credentials, SnowflakeKeyPairCredentials):
            raise SnowflakeCredentialError("runtime adapter requires key-pair credentials")
        _validate_landing_limits(part_limits, maximum_part_arrow_bytes)
        if any(
            len(columns) > part_limits.maximum_fields_per_record for columns in statement.selected_columns_by_stream
        ):
            raise SnowflakeConnectorError("Snowflake landing projection exceeds the field limit")
        timeout_seconds = _execution_timeout(timeout_seconds)
        connection = cursor = None
        try:
            connection, cursor = self._connect(credentials, timeout_seconds=timeout_seconds)
            _execute_filtered_statement(cursor, statement)
            query_id = getattr(cursor, "sfqid", None)
            if not isinstance(query_id, str) or not query_id.strip():
                raise SnowflakeConnectorError("Snowflake did not return a statement identity")
            schemas = _bundle_result_schemas(statement, cursor.description)
            metadata, token, pending_row = _bundle_stream_metadata(statement, cursor, query_id=query_id)
            owner = _SnowflakeBundlePartitionSources(
                connection=connection,
                cursor=cursor,
                statement=statement,
                schemas=schemas,
                pending_row=pending_row,
                landing_limits=part_limits,
                maximum_part_arrow_bytes=maximum_part_arrow_bytes,
            )
            return SnowflakeBundleLandingResult(
                owner=owner,
                query_id=query_id,
                source_snapshot_token=token,
                metadata=metadata,
                schemas=schemas,
            )
        except SnowflakeConnectorError:
            _best_effort_close(cursor, connection)
            raise
        except Exception as exc:
            _best_effort_close(cursor, connection)
            raise SnowflakeConnectorError("Snowflake bundle landing read failed") from exc
        except BaseException:
            _best_effort_close(cursor, connection)
            raise

    def _connect(
        self,
        credentials: SnowflakeKeyPairCredentials,
        *,
        timeout_seconds: int = _STATEMENT_TIMEOUT_SECONDS,
    ) -> tuple[Any, Any]:
        """Open a fixed-primary-role session before executing generated reads."""

        timeout_seconds = _execution_timeout(timeout_seconds)
        login_timeout = min(_LOGIN_TIMEOUT_SECONDS, timeout_seconds)
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
                ocsp_fail_open=True,
                login_timeout=login_timeout,
                network_timeout=timeout_seconds,
                socket_timeout=timeout_seconds,
                session_parameters={
                    "QUERY_TAG": _QUERY_TAG,
                    "STATEMENT_TIMEOUT_IN_SECONDS": timeout_seconds,
                },
            )
            cursor = connection.cursor()
            cursor.execute("USE SECONDARY ROLES NONE")
            return connection, cursor
        except BaseException:
            _best_effort_close(cursor, connection)
            raise


class SnowflakePythonPreflightAdapter:
    """Open one generated preflight cursor with fixed credential composition."""

    def __init__(
        self,
        *,
        connector: SnowflakePythonConnectorAdapter,
        credential_provider: SnowflakeCredentialProvider,
    ) -> None:
        if not isinstance(connector, SnowflakePythonConnectorAdapter):
            raise SnowflakeConnectorError("preflight adapter requires the Snowflake Python connector")
        if not callable(getattr(credential_provider, "load_key_pair", None)):
            raise SnowflakeCredentialError("preflight adapter requires a fixed key-pair credential provider")
        self._connector = connector
        self._credential_provider = credential_provider

    def open_preflight(
        self,
        statement: SnowflakePreflightStatement,
        *,
        timeout_seconds: int,
    ) -> _SnowflakePreflightCursor:
        """Execute one generated preview statement and transfer its cursor ownership."""

        if not isinstance(statement, SnowflakePreflightStatement):
            raise SnowflakeConnectorError("preflight adapter requires a generated Snowflake preflight statement")
        return self._open_statement(statement, timeout_seconds=timeout_seconds)

    def open_inspection(
        self,
        statement: SnowflakeInspectionStatement,
        *,
        timeout_seconds: int,
    ) -> _SnowflakePreflightCursor:
        """Execute only a rebuilt discovery/count statement, without source rows."""

        if not isinstance(statement, SnowflakeInspectionStatement):
            raise SnowflakeConnectorError("inspection adapter requires a generated Snowflake inspection statement")
        return self._open_statement(statement.validated(), timeout_seconds=timeout_seconds)

    def _open_statement(
        self,
        statement: SnowflakePreflightStatement | SnowflakeInspectionStatement,
        *,
        timeout_seconds: int,
    ) -> _SnowflakePreflightCursor:
        timeout_seconds = _execution_timeout(timeout_seconds)
        connection = None
        cursor = None
        try:
            credentials = self._credential_provider.load_key_pair()
            if not isinstance(credentials, SnowflakeKeyPairCredentials):
                raise SnowflakeCredentialError("credential provider returned an invalid key-pair value")
            connection, cursor = self._connector._connect(credentials, timeout_seconds=timeout_seconds)
            _execute_filtered_statement(cursor, statement)
            if isinstance(statement, SnowflakePreflightStatement):
                _preflight_result_schema(statement, cursor.description)
            query_id = getattr(cursor, "sfqid", None)
            if query_id is not None and not isinstance(query_id, str):
                raise SnowflakeConnectorError("Snowflake preflight statement identity is invalid")
            preflight_cursor = _SnowflakePreflightCursor(
                connection=connection,
                cursor=cursor,
                column_ids=statement.column_ids,
                query_id=query_id,
            )
            if isinstance(statement, SnowflakeInspectionStatement):
                preflight_cursor.description = cursor.description
                preflight_cursor.field_types = SNOWFLAKE_FIELD_TYPES
            connection = None
            cursor = None
            return preflight_cursor
        except SnowflakeConnectorError:
            _best_effort_close(cursor, connection)
            raise
        except Exception as exc:
            _best_effort_close(cursor, connection)
            raise SnowflakeConnectorError("Snowflake preflight read failed") from exc
        except BaseException:
            _best_effort_close(cursor, connection)
            raise


class _SnowflakePreflightCursor:
    """One cursor owner for the bounded preflight row protocol."""

    def __init__(
        self,
        *,
        connection: Any,
        cursor: Any,
        column_ids: tuple[str, ...],
        query_id: str | None,
    ) -> None:
        self._connection = connection
        self._cursor = cursor
        self.column_ids = column_ids
        self.query_id = query_id
        self._closed = False

    def fetchone(self) -> Sequence[object] | None:
        """Read one row and close both owned resources if it fails."""

        if self._closed:
            raise SnowflakeConnectorError("Snowflake preflight cursor is closed")
        try:
            return self._cursor.fetchone()
        except SnowflakeConnectorError:
            _best_effort_close(self)
            raise
        except Exception as exc:
            _best_effort_close(self)
            raise SnowflakeConnectorError("Snowflake preflight result fetch failed") from exc
        except BaseException:
            _best_effort_close(self)
            raise

    def close(self) -> None:
        """Close the cursor and its exact connection once, attempting both."""

        if self._closed:
            return
        self._closed = True
        cursor, connection = self._cursor, self._connection
        self._cursor = None
        self._connection = None
        _close_resources(cursor, connection)


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
    source_fields = _bundle_source_field_ids(statement.request, statement.selected_columns_by_stream)
    schemas = tuple(
        tuple(
            SnowflakeResultColumn(
                field_id=column.field_id,
                source_type=columns_by_field[source_fields[column.field_id]].source_type,
                nullable=columns_by_field[source_fields[column.field_id]].nullable,
            )
            for column in selected_columns
        )
        for selected_columns in statement.selected_columns_by_stream
    )
    for schema in schemas:
        validate_decimal_conversion_sources(schema, statement.request.decimal_conversions)
    return schemas


def _bundle_stream_metadata(
    statement: SnowflakeBundleStatement,
    cursor: Any,
    *,
    query_id: str | None = None,
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
        source_snapshot_token = result_row[3]
        if binding.source_snapshot_token_relation is None:
            if source_snapshot_token is not None:
                raise SnowflakeConnectorError("Snowflake bundle metadata row does not match the generated statement")
            source_snapshot_token = _query_identity_snapshot_token(query_id)
        stream_metadata_items.append(
            SnowflakeBundleStreamMetadata(
                stream_id=binding.stream_id,
                semantic_token_metadata_key=binding.semantic_token_metadata_key,
                source_snapshot_tokens=(source_snapshot_token,),
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
        landing_limits: CaptureLimits | None = None,
        maximum_part_arrow_bytes: int = MAX_RESULT_PARTITION_BYTES,
    ) -> None:
        self._connection = connection
        self._cursor = cursor
        self._schemas = schemas
        self._stream_ids = tuple(binding.stream_id for binding in statement.request.bindings)
        fields = tuple(sorted(statement.request.definition.fields, key=lambda field: field.field_slot))
        output_index_by_field = {field.field_id: index for index, field in enumerate(fields, start=4)}
        source_fields = _bundle_source_field_ids(statement.request, statement.selected_columns_by_stream)
        self._field_indexes_by_stream = tuple(
            tuple(output_index_by_field[source_fields[column.field_id]] for column in selected_columns)
            for selected_columns in statement.selected_columns_by_stream
        )
        self._landing_limits = landing_limits
        self._maximum_part_arrow_bytes = min(maximum_part_arrow_bytes, MAX_RESULT_PARTITION_BYTES)
        self._streams = {stream.stream_id: stream for stream in statement.request.definition.source_streams}
        self._configure_shared_sources(statement)
        self._partition_rows = statement.request.encoding.partition_rows
        if landing_limits is not None:
            self._partition_rows = min(self._partition_rows, landing_limits.maximum_records)
        self._expected_row_length = len(fields) + 4
        self._next_stream_index = 0
        self._pending_row = pending_row
        self._source_exhausted = False
        self._closed_stream_indexes: set[int] = set()
        self._closed = False
        self._stream_sources = tuple(
            _SnowflakeBundleStreamPartitionSources(self, stream_index) for stream_index in range(len(self._stream_ids))
        )

    def stream_sources(self, stream_index: int) -> _SnowflakeBundleStreamPartitionSources:
        """Return the one ordered source collection for a configured stream."""

        return self._stream_sources[stream_index]

    def _configure_shared_sources(self, statement: SnowflakeBundleStatement) -> None:
        """Keep bounded later-stream Parquet partitions from each relation's sole read."""

        streams_by_source = {}
        for index, binding in enumerate(statement.request.bindings):
            source_key = _bundle_source_key(statement.request, binding, statement.selected_columns_by_stream[index])
            streams_by_source.setdefault(source_key, []).append(index)
        self._streams_by_source = {indexes[0]: tuple(indexes) for indexes in streams_by_source.values()}
        self._source_field_indexes = {}
        self._source_schemas = {}
        self._projection_indexes = {}
        self._shared_partitions: dict[int, deque[BinaryIO]] = {}
        for source_index, stream_indexes in self._streams_by_source.items():
            columns_by_index = {}
            for stream_index in stream_indexes:
                for index, column in zip(
                    self._field_indexes_by_stream[stream_index], self._schemas[stream_index], strict=True
                ):
                    columns_by_index.setdefault(index, column)
                if stream_index != source_index and self._landing_limits is None:
                    self._shared_partitions[stream_index] = deque()
            indexes = tuple(columns_by_index)
            self._source_field_indexes[source_index] = indexes
            self._source_schemas[source_index] = tuple(columns_by_index.values())
            for stream_index in stream_indexes:
                self._projection_indexes[stream_index] = tuple(
                    indexes.index(index) for index in self._field_indexes_by_stream[stream_index]
                )
        self._stream_byte_limit = min(
            MAX_STREAM_CAPTURE_BYTES, statement.request.capture_limits.maximum_compressed_bytes
        )
        self._generated_bytes_by_stream = [0] * len(self._stream_ids)
        self._generated_partitions_by_stream = [0] * len(self._stream_ids)
        self._generated_bytes = self._generated_partitions = 0

    def next_partition(self, stream_index: int) -> tuple[BinaryIO | None, bool]:
        """Fan out one captured batch, or transfer a previously retained partition."""

        self._check_stream_order(stream_index)
        if stream_index in self._shared_partitions:
            partitions = self._shared_partitions[stream_index]
            return (partitions.popleft() if partitions else None), not partitions
        rows, is_complete = self.next_partition_rows(stream_index)
        if not rows:
            return None, is_complete
        reader = self._projected_partition(rows, stream_index)
        try:
            for shared_index in self._streams_by_source[stream_index][1:]:
                self._shared_partitions[shared_index].append(self._projected_partition(rows, shared_index))
        except BaseException:
            _best_effort_close(reader)
            raise
        return reader, is_complete

    def _projected_partition(self, rows: Sequence[Sequence[object]], stream_index: int) -> BinaryIO:
        indexes = self._projection_indexes[stream_index]
        return self.partition_reader([tuple(row[index] for index in indexes) for row in rows], stream_index)

    def partition_reader(self, rows: Sequence[Sequence[object]], stream_index: int) -> BinaryIO:
        """Bound all generated partitions before later-stream bytes can be retained."""

        reader = _parquet_reader(rows, self._schemas[stream_index])
        if not self._shared_partitions:
            return reader
        size = reader.getbuffer().nbytes
        if (
            self._generated_bytes + size > MAX_BUNDLE_CAPTURE_BYTES
            or self._generated_bytes_by_stream[stream_index] + size > self._stream_byte_limit
            or self._generated_partitions >= MAX_BUNDLE_PARTITIONS
            or self._generated_partitions_by_stream[stream_index] >= MAX_RESULT_PARTITIONS
        ):
            reader.close()
            raise SnowflakeConnectorError("Snowflake shared capture exceeds the bundle byte or partition limit")
        self._generated_bytes += size
        self._generated_bytes_by_stream[stream_index] += size
        self._generated_partitions += 1
        self._generated_partitions_by_stream[stream_index] += 1
        return reader

    def _check_stream_order(self, stream_index: int) -> None:
        if self._closed:
            raise SnowflakeConnectorError("Snowflake bundle result is closed")
        if stream_index != self._next_stream_index:
            raise SnowflakeConnectorError("Snowflake bundle streams must be consumed in configured order")

    def next_partition_rows(self, stream_index: int) -> tuple[list[Sequence[object]], bool]:
        """Return the next deterministic partition for one configured stream."""

        self._check_stream_order(stream_index)
        partition_rows: list[Sequence[object]] = []
        variable_bytes = 0
        projection_bytes_by_stream = {}
        expected_ordinal = stream_index + 1
        while len(partition_rows) < self._partition_rows:
            result_row = self._pending_row
            if result_row is None:
                result_row = self._cursor.fetchone()
            else:
                self._pending_row = None
            if result_row is None:
                self._source_exhausted = True
                return partition_rows, True
            ordinal, selected_values = self._split_row(result_row, stream_index)
            if ordinal > expected_ordinal:
                self._pending_row = result_row
                return partition_rows, True
            if ordinal < expected_ordinal:
                raise SnowflakeConnectorError("Snowflake bundle result rows are not ordered by stream")
            schema = self._source_schemas[stream_index]
            selected_values = tuple(
                convert_snowflake_float(scalar_value) if column.source_type == "REAL" else scalar_value
                for scalar_value, column in zip(selected_values, schema, strict=True)
            )
            row_variable_bytes = _result_row_variable_bytes(selected_values, schema)
            has_exceeded_projection = False
            if self._landing_limits is not None:
                has_exceeded_projection = self._has_exceeded_landing_projection(
                    selected_values, stream_index, len(partition_rows) + 1, projection_bytes_by_stream
                )
            decoded_bytes = variable_bytes + row_variable_bytes + _fixed_batch_bytes(schema, len(partition_rows) + 1)
            if decoded_bytes > MAX_RESULT_PARTITION_BYTES or has_exceeded_projection:
                if not partition_rows:
                    raise SnowflakeConnectorError("Snowflake bundle result batch exceeds the decoded-byte limit")
                self._pending_row = result_row
                return partition_rows, False
            partition_rows.append(selected_values)
            variable_bytes += row_variable_bytes
        return partition_rows, False

    def _has_exceeded_landing_projection(
        self, row: Sequence[object], source_index: int, row_count: int, variable_bytes_by_stream: dict[int, int]
    ) -> bool:
        """Validate records and preflight each projection independently of union bytes."""

        has_exceeded_limit = False
        for stream_index in self._streams_by_source[source_index]:
            values_by_field = {
                column.field_id: row[index]
                for column, index in zip(
                    self._schemas[stream_index], self._projection_indexes[stream_index], strict=True
                )
            }
            _decoded_record(1, values_by_field, self._landing_limits)
            schema = self._schemas[stream_index]
            variable_bytes_by_stream[stream_index] = variable_bytes_by_stream.get(stream_index, 0) + (
                _result_row_variable_bytes(tuple(values_by_field.values()), schema)
            )
            if (
                variable_bytes_by_stream[stream_index] + _fixed_batch_bytes(schema, row_count)
                > self._maximum_part_arrow_bytes
            ):
                has_exceeded_limit = True
        return has_exceeded_limit

    def landing_events(self, source_snapshot_token: str) -> Iterator[SnowflakeLandingPart | SnowflakeLandingEOF]:
        """Fan out each bounded batch before fetching another; EOF follows cursor EOF."""

        part_counts = [0] * len(self._stream_ids)
        record_counts = [0] * len(self._stream_ids)
        has_primary_failure = False
        try:
            for source_index, stream_indexes in self._streams_by_source.items():
                self._next_stream_index = source_index
                for part in self._landing_source_parts(source_index, stream_indexes, source_snapshot_token):
                    stream_index = self._stream_ids.index(part.stream_id)
                    part_counts[stream_index] = part.ordinal
                    record_counts[stream_index] += part.record_count
                    yield part
                    part = None
            if not self._source_exhausted or self._pending_row is not None:
                raise SnowflakeConnectorError("Snowflake landing did not exhaust the source cursor")
            for index, stream_id in enumerate(self._stream_ids):
                yield SnowflakeLandingEOF(stream_id, part_counts[index], record_counts[index])
        except SnowflakeConnectorError:
            has_primary_failure = True
            raise
        except Exception as exc:
            has_primary_failure = True
            raise SnowflakeConnectorError("Snowflake bundle landing fetch failed") from exc
        except GeneratorExit:
            # Explicit close has no body failure to preserve; cleanup must report.
            raise
        except BaseException:
            has_primary_failure = True
            raise
        finally:
            if has_primary_failure:
                _best_effort_close(self)
            else:
                self.close()

    def _landing_source_parts(
        self, source_index: int, stream_indexes: tuple[int, ...], source_snapshot_token: str
    ) -> Iterator[SnowflakeLandingPart]:
        """Release every projection of one source batch before fetching another."""

        ordinal = 0
        while True:
            rows, is_complete = self.next_partition_rows(source_index)
            if rows or not ordinal:
                ordinal += 1
                for stream_index in stream_indexes:
                    yield self._landing_part(rows, stream_index, ordinal, source_snapshot_token)
            rows = None
            if is_complete:
                return

    def _landing_part(
        self, rows: Sequence[Sequence[object]], stream_index: int, ordinal: int, token: str
    ) -> SnowflakeLandingPart:
        """Seal and release the sole encoded reader before exposing one event."""

        indexes = self._projection_indexes[stream_index]
        projected_rows = [tuple(row[index] for index in indexes) for row in rows]
        reader, arrow_bytes = _parquet_reader_with_metrics(
            projected_rows,
            self._schemas[stream_index],
            maximum_arrow_bytes=self._maximum_part_arrow_bytes,
            record_limits=self._landing_limits,
        )
        has_primary_failure = False
        try:
            stream_id = self._stream_ids[stream_index]
            stream = self._streams[stream_id]
            capture = capture_stream(reader, stream, source_snapshot_token=token, limits=self._landing_limits)
            return SnowflakeLandingPart(stream_id, ordinal, capture, len(rows), arrow_bytes)
        except BaseException:
            has_primary_failure = True
            raise
        finally:
            if has_primary_failure:
                _best_effort_close(reader)
            else:
                reader.close()

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
        if self._landing_limits is not None:
            self._pending_row = None
        readers = [reader for partitions in self._shared_partitions.values() for reader in partitions]
        for partitions in self._shared_partitions.values():
            partitions.clear()
        _close_resources(*readers, cursor, connection)

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
        if ordinal - 1 not in self._streams_by_source:
            raise SnowflakeConnectorError("Snowflake bundle data repeats a shared source relation")
        if result_row[2] != self._stream_ids[ordinal - 1] or result_row[3] is not None:
            raise SnowflakeConnectorError("Snowflake bundle stream identity is invalid")
        return ordinal, tuple(result_row[index] for index in self._source_field_indexes[stream_index])


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
                reader, is_complete = self._owner.next_partition(self._stream_index)
                if reader is not None:
                    has_emitted_partition = True
                    yield reader
                if is_complete:
                    if not has_emitted_partition:
                        yield self._owner.partition_reader((), self._stream_index)
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
    validate_decimal_conversion_sources(result_columns, None)
    return tuple(result_columns)


def _preflight_result_schema(statement: SnowflakePreflightStatement, description: object) -> None:
    """Require the generated preview's exact labels and capture-supported types."""

    validate_preflight_result_schema(statement, description, field_types=SNOWFLAKE_FIELD_TYPES)


def _execution_timeout(value: object) -> int:
    maximum_timeout = min(_NETWORK_TIMEOUT_SECONDS, _STATEMENT_TIMEOUT_SECONDS)
    if type(value) is not int or not 1 <= value <= maximum_timeout:
        raise SnowflakeConnectorError(f"Snowflake execution timeout must be from 1 through {maximum_timeout} seconds")
    return value


def _source_type(metadata: object) -> str:
    return _preflight_source_type(metadata, field_types=SNOWFLAKE_FIELD_TYPES)


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
    reader, _arrow_bytes = _parquet_reader_with_metrics(result_rows, result_schema)
    return reader


def _parquet_reader_with_metrics(
    result_rows: Sequence[Sequence[object]],
    result_schema: tuple[SnowflakeResultColumn, ...],
    *,
    maximum_arrow_bytes: int = MAX_RESULT_PARTITION_BYTES,
    record_limits: CaptureLimits | None = None,
) -> tuple[BytesIO, int]:
    """Encode with the existing codec and report actual Arrow, not encoded bytes."""

    _validate_result_rows(result_rows, result_schema)
    result_columns = tuple(zip(*result_rows, strict=True)) if result_rows else ((),) * len(result_schema)
    destination = None
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
        if table.nbytes > min(maximum_arrow_bytes, MAX_RESULT_PARTITION_BYTES):
            raise SnowflakeConnectorError("Snowflake result batch exceeds the decoded-byte limit")
        arrow_bytes = table.nbytes
        expected_schema = table.schema if record_limits is not None else None
        destination = BytesIO()
        pq.write_table(table, destination, compression="zstd")
        if destination.tell() > MAX_RESULT_PARTITION_BYTES:
            raise SnowflakeConnectorError("Snowflake Parquet partition exceeds the byte limit")
        if record_limits is not None and not _is_generated_landing_table_verified(
            destination, table, result_rows, expected_schema, record_limits
        ):
            table = None
            _is_generated_landing_table_verified(destination, None, result_rows, expected_schema, record_limits)
    except SnowflakeConnectorError, CaptureError:
        _best_effort_close(destination)
        raise
    except (pa.ArrowException, OverflowError, TypeError, ValueError) as exc:
        _best_effort_close(destination)
        raise SnowflakeConnectorError("Snowflake result cannot be encoded as Parquet") from exc
    except BaseException:
        _best_effort_close(destination)
        raise
    destination.seek(0)
    return destination, arrow_bytes


def _is_generated_landing_table_verified(
    reader: BytesIO,
    table: pa.Table | None,
    expected_rows: Sequence[Sequence[object]],
    expected_schema: pa.Schema,
    limits: CaptureLimits,
) -> bool:
    """Verify only bytes just written here; retained/external Parquet uses generic replay."""

    encoded_bytes = reader.getvalue()
    if len(encoded_bytes) > limits.maximum_compressed_bytes:
        raise CaptureError("source stream exceeds the compressed-byte limit")
    if len(encoded_bytes) > limits.maximum_decoded_bytes:
        raise CaptureError("source stream exceeds the decoded-byte limit")
    # Native replay can add validity bitmaps; preflight already reserves each
    # column's fixed buffers. Retain the original table inside the same budget.
    output_bound = 0 if table is None else table.nbytes + ((table.num_rows + 7) // 8) * table.num_columns
    working_budget = limits.maximum_decoded_bytes - output_bound - (0 if table is None else table.nbytes)
    if working_budget <= 0:
        return False
    _validate_parquet_envelope(encoded_bytes)
    with _open_parquet_reader(encoded_bytes, limits) as parquet:
        labels = _validated_parquet_schema(parquet.schema_arrow, limits)
        record_count = _validated_parquet_metadata(parquet.metadata, expected_columns=len(labels), limits=limits)
        if record_count != len(expected_rows) or not parquet.schema_arrow.equals(expected_schema):
            raise CaptureError("Snowflake landing schema or record count does not match the source batch")
        try:
            _validate_parquet_page_preflight(
                encoded_bytes, parquet.metadata, parquet.schema_arrow, maximum_decoded_bytes=working_budget
            )
        except CaptureError:
            if table is not None:
                return False
            raise
        if table is None:
            for decoded_record, expected in zip(
                _iter_parquet_batch_records(parquet, labels, record_count, limits), expected_rows, strict=True
            ):
                if tuple(decoded_record.values.values()) != tuple(expected):
                    raise CaptureError("Snowflake landing encoded values do not match the source batch")
        else:
            decoded = parquet.read(use_threads=False, use_pandas_metadata=False)
            if decoded.nbytes > output_bound or not decoded.equals(table):
                raise CaptureError("Snowflake landing encoded values do not match the source batch")
            _validate_generated_landing_records(decoded, limits)
    return True


def _validate_generated_landing_records(table: pa.Table, limits: CaptureLimits) -> None:
    """Apply canonical sizes to verified scalar columns, retaining one-row accounting."""

    ordinal = logical_bytes = 0
    label_bytes = sum(len(label.encode("utf-8")) + 4 for label in table.column_names)
    for batch in table.to_batches(max_chunksize=min(_FETCH_ROWS, limits.maximum_records)):
        if not batch.num_rows:
            continue
        record_sizes = pa.repeat(pa.scalar(label_bytes, type=pa.int64()), batch.num_rows)
        decoded_sizes = pa.repeat(pa.scalar(0, type=pa.int64()), batch.num_rows)
        # The single-row reader omits a non-integer validity bitmap when that
        # row is present, even when a larger decoded batch has null neighbors.
        for column in batch.columns:
            is_text = pa.types.is_string(column.type) or pa.types.is_large_string(column.type)
            text = column if is_text else pc.cast(column, pa.string())
            sizes = pc.binary_length(text)
            record_sizes = pc.add(record_sizes, pc.fill_null(sizes, 4))
            if is_text:
                decoded_sizes = pc.add(decoded_sizes, pc.fill_null(sizes, 0))
                fixed_bytes = 8 if pa.types.is_large_string(column.type) else 4
            else:
                fixed_bytes = 0 if pa.types.is_null(column.type) else (column.type.bit_width + 7) // 8
            if column.buffers()[0] is not None:
                if pa.types.is_integer(column.type):
                    fixed_bytes += 1
                else:
                    decoded_sizes = pc.add(decoded_sizes, pc.cast(pc.is_null(column), pa.int64()))
            decoded_sizes = pc.add(decoded_sizes, fixed_bytes)
        cumulative_bytes = pc.cumulative_sum(decoded_sizes)
        decoded_over = pc.greater(cumulative_bytes, min(limits.maximum_decoded_bytes - logical_bytes, _MAX_INT64))
        record_over = pc.greater(record_sizes, min(limits.maximum_record_bytes, _MAX_INT64))
        first_error = pc.index(pc.or_(decoded_over, record_over), True).as_py()
        if first_error >= 0:
            if decoded_over[first_error].as_py():
                raise CaptureError("Parquet source payload exceeds the decoded-byte limit")
            raise CaptureError(f"record {ordinal + first_error + 1} exceeds the byte limit")
        logical_bytes += cumulative_bytes[-1].as_py()
        ordinal += batch.num_rows


def _validate_landing_limits(part_limits: CaptureLimits, maximum_arrow_bytes: int) -> None:
    """Validate the separate trusted limits before opening a source connection."""

    try:
        _capture_limits(part_limits)
    except ValueError as exc:
        raise SnowflakeConnectorError("Snowflake landing part limits are invalid") from exc
    if (
        isinstance(maximum_arrow_bytes, bool)
        or not isinstance(maximum_arrow_bytes, int)
        or not 1 <= maximum_arrow_bytes <= MAX_RESULT_BYTES
    ):
        raise SnowflakeConnectorError("Snowflake landing Arrow limit is invalid")


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
    source_type = result_column.source_type
    if scalar_value is None:
        if not result_column.nullable:
            raise SnowflakeConnectorError("Snowflake non-nullable result cannot be null")
        return 0
    if source_type == "REAL":
        if not is_decimal_scalar_storage_valid(scalar_value):
            raise SnowflakeConnectorError("Snowflake converted REAL result is invalid")
        return 0
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
    if result_column.source_type == "REAL":
        return pa.decimal128(30, 12)
    precision, scale = _fixed_type_parts(result_column.source_type)
    if not _uses_decimal_storage(result_column.source_type):
        return pa.int64()
    return pa.decimal128(precision, scale)


def _uses_decimal_storage(source_type: str) -> bool:
    if source_type == "REAL":
        return True
    if source_type in {"TEXT", "BOOLEAN"}:
        return False
    precision, scale = _fixed_type_parts(source_type)
    return scale != 0 or precision > 18


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
