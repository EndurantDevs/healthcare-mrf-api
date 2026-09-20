# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic checks for the concrete Snowflake Python adapter."""

from __future__ import annotations

from asyncio import CancelledError
from dataclasses import dataclass
from decimal import Decimal
from io import BytesIO
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest

from process.custom_import.capture import capture_stream, iter_records
from process.custom_import.snowflake import (
    SNOWFLAKE_RESULT_STREAM,
    SnowflakeConnectorError,
    SnowflakeDeclaredColumn,
    SnowflakeKeyPairCredentials,
    SnowflakeReadRequest,
    SnowflakeReadStatement,
    SnowflakeRelation,
)
from process.custom_import.snowflake_python import SnowflakePythonConnectorAdapter

_PRIVATE_KEY = b"""-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIGf0q2V1QeH4rYk0uXf9ThwM3U7v6w9NfX3qf8wVv8oP
-----END PRIVATE KEY-----
"""


@dataclass(frozen=True)
class _Metadata:
    name: str
    type_name: str
    is_nullable: bool
    precision: int | None = None
    scale: int | None = None


class _Cursor:
    def __init__(self, rows):
        self.description = (_Metadata("npi", "TEXT", False), _Metadata("specialty", "TEXT", True))
        self.sfqid = "01abc"
        self._rows = list(rows)
        self.executed: list[str] = []
        self.fetch_count = 0
        self.closed = False

    def execute(self, sql: str) -> None:
        self.executed.append(sql)

    def fetchone(self):
        self.fetch_count += 1
        if not self._rows:
            return None
        return self._rows.pop(0)

    def close(self) -> None:
        self.closed = True


class _Connection:
    def __init__(self, cursor: _Cursor):
        self._cursor = cursor
        self.closed = False

    def cursor(self) -> _Cursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True


def _statement() -> SnowflakeReadStatement:
    request = SnowflakeReadRequest(
        relation=SnowflakeRelation(database="demo", schema="curated", name="providers"),
        selected_columns=(
            SnowflakeDeclaredColumn(field_id="npi", column_identifier="npi_num"),
            SnowflakeDeclaredColumn(field_id="specialty", column_identifier="specialty_name"),
        ),
        definition_sha256="1" * 64,
        schema_sha256="2" * 64,
    )
    return SnowflakeReadStatement(request)


def _metric_statement() -> SnowflakeReadStatement:
    return SnowflakeReadStatement(
        SnowflakeReadRequest(
            relation=SnowflakeRelation(database="demo", schema="curated", name="metrics"),
            selected_columns=(SnowflakeDeclaredColumn(field_id="metric", column_identifier="metric_value"),),
            definition_sha256="1" * 64,
            schema_sha256="2" * 64,
        )
    )


@pytest.fixture
def credentials(monkeypatch) -> SnowflakeKeyPairCredentials:
    monkeypatch.setattr(
        "process.custom_import.snowflake_python._private_key_der",
        lambda _credentials: b"private-key-der",
    )
    return SnowflakeKeyPairCredentials(account="example", user="reader", private_key_pem=_PRIVATE_KEY)


def test_adapter_executes_only_generated_select_and_yields_parquet(monkeypatch, credentials):
    cursor = _Cursor((("1234567893", "Cardiology"), ("1003000126", None)))
    connection = _Connection(cursor)
    connection_arguments_by_name = {}

    def connect(**arguments):
        connection_arguments_by_name.update(arguments)
        return connection

    monkeypatch.setattr("process.custom_import.snowflake_python.snowflake.connector.connect", connect)
    statement = _statement()
    parquet_result = SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh").fetch_parquet(
        statement,
        credentials,
    )

    assert cursor.executed == [statement.sql]
    assert parquet_result.source_snapshot_token == "snowflake-query:01abc"
    assert [column.field_id for column in parquet_result.schema] == ["npi", "specialty"]
    assert connection_arguments_by_name["authenticator"] == "SNOWFLAKE_JWT"
    assert connection_arguments_by_name["role"] == "READER_ROLE"
    assert connection_arguments_by_name["warehouse"] == "IMPORT_WH"
    assert connection_arguments_by_name["private_key"] == b"private-key-der"
    assert connection_arguments_by_name["login_timeout"] == 30
    assert connection_arguments_by_name["network_timeout"] == 120
    assert connection_arguments_by_name["socket_timeout"] == 120
    assert "private_key_file" not in connection_arguments_by_name

    iterator = parquet_result.consume_partition_sources()
    reader = next(iterator)
    parquet_result.claim_partition_source(reader)
    table = pq.read_table(BytesIO(reader.read()))
    assert table.to_pydict() == {
        "npi": ["1234567893", "1003000126"],
        "specialty": ["Cardiology", None],
    }
    assert not table.schema.field("npi").nullable
    assert table.schema.field("specialty").nullable
    parquet_result.close_partition_source(reader)
    with pytest.raises(StopIteration):
        next(iterator)
    parquet_result.close_partition_iterator()
    parquet_result.close()
    assert cursor.closed
    assert connection.closed
    assert cursor.fetch_count == 3


def test_adapter_emits_one_schema_only_partition_for_empty_result(monkeypatch, credentials):
    cursor = _Cursor(())
    connection = _Connection(cursor)
    monkeypatch.setattr(
        "process.custom_import.snowflake_python.snowflake.connector.connect",
        lambda **_arguments: connection,
    )
    result = SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh").fetch_parquet(
        _statement(),
        credentials,
    )

    iterator = result.consume_partition_sources()
    reader = next(iterator)
    result.claim_partition_source(reader)
    table = pq.read_table(BytesIO(reader.read()))
    assert table.column_names == ["npi", "specialty"]
    assert table.num_rows == 0
    result.close_partition_source(reader)
    with pytest.raises(StopIteration):
        next(iterator)
    result.close()


def test_adapter_rejects_source_types_the_capture_runtime_cannot_replay(monkeypatch, credentials):
    cursor = _Cursor(())
    cursor.description = (_Metadata("npi", "TEXT", False), _Metadata("specialty", "DATE", True))
    connection = _Connection(cursor)
    monkeypatch.setattr(
        "process.custom_import.snowflake_python.snowflake.connector.connect",
        lambda **_arguments: connection,
    )

    with pytest.raises(SnowflakeConnectorError, match="not supported by the capture runtime"):
        SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh").fetch_parquet(
            _statement(),
            credentials,
        )

    assert cursor.closed
    assert connection.closed


def test_adapter_closes_connection_when_result_construction_fails(monkeypatch, credentials):
    cursor = _Cursor(())
    connection = _Connection(cursor)
    monkeypatch.setattr(
        "process.custom_import.snowflake_python.snowflake.connector.connect",
        lambda **_arguments: connection,
    )

    def reject_result(**_arguments):
        raise SnowflakeConnectorError("invalid adapter result")

    monkeypatch.setattr("process.custom_import.snowflake_python.SnowflakeParquetResult", reject_result)

    with pytest.raises(SnowflakeConnectorError, match="invalid adapter result"):
        SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh").fetch_parquet(
            _statement(),
            credentials,
        )

    assert cursor.closed
    assert connection.closed


def test_adapter_resolves_connector_type_codes(monkeypatch, credentials):
    cursor = _Cursor((("1234567893", "Cardiology"),))
    cursor.description = (
        SimpleNamespace(name="npi", type_code=2, is_nullable=False),
        SimpleNamespace(name="specialty", type_code=2, is_nullable=True),
    )
    connection = _Connection(cursor)
    monkeypatch.setattr(
        "process.custom_import.snowflake_python.snowflake.connector.connect",
        lambda **_arguments: connection,
    )

    result = SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh").fetch_parquet(
        _statement(),
        credentials,
    )

    assert [column.source_type for column in result.schema] == ["TEXT", "TEXT"]
    result.close()


def test_adapter_preserves_integer_values_and_fixed_schema_identity(monkeypatch, credentials):
    cursor = _Cursor(((42,),))
    cursor.description = (_Metadata("metric", "FIXED", False, precision=10, scale=0),)
    connection = _Connection(cursor)
    monkeypatch.setattr(
        "process.custom_import.snowflake_python.snowflake.connector.connect",
        lambda **_arguments: connection,
    )

    parquet_result = SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh").fetch_parquet(
        _metric_statement(),
        credentials,
    )
    assert parquet_result.schema[0].source_type == "FIXED(10,0)"
    partition_iterator = parquet_result.consume_partition_sources()
    partition_reader = next(partition_iterator)
    parquet_result.claim_partition_source(partition_reader)
    partition_payload = partition_reader.read()
    assert pq.read_table(BytesIO(partition_payload)).to_pydict() == {"metric": [42]}
    sealed_capture = capture_stream(
        BytesIO(partition_payload),
        SNOWFLAKE_RESULT_STREAM,
        source_snapshot_token=parquet_result.source_snapshot_token,
    )
    decoded_value = next(iter(iter_records(sealed_capture, SNOWFLAKE_RESULT_STREAM))).values["metric"]
    assert type(decoded_value) is int
    assert decoded_value == 42
    parquet_result.close()


def test_adapter_rejects_scale_zero_fixed_values_outside_int64(monkeypatch, credentials):
    cursor = _Cursor(((2**63,),))
    cursor.description = (_Metadata("metric", "FIXED", False, precision=38, scale=0),)
    connection = _Connection(cursor)
    monkeypatch.setattr(
        "process.custom_import.snowflake_python.snowflake.connector.connect",
        lambda **_arguments: connection,
    )
    parquet_result = SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh").fetch_parquet(
        _metric_statement(),
        credentials,
    )

    assert parquet_result.schema[0].source_type == "FIXED(38,0)"
    with pytest.raises(SnowflakeConnectorError, match="signed 64-bit integer"):
        next(parquet_result.consume_partition_sources())
    parquet_result.close()


def test_adapter_rejects_null_in_non_nullable_result(monkeypatch, credentials):
    cursor = _Cursor(((None, "Cardiology"),))
    connection = _Connection(cursor)
    monkeypatch.setattr(
        "process.custom_import.snowflake_python.snowflake.connector.connect",
        lambda **_arguments: connection,
    )
    parquet_result = SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh").fetch_parquet(
        _statement(),
        credentials,
    )

    with pytest.raises(SnowflakeConnectorError, match="non-nullable result cannot be null"):
        next(parquet_result.consume_partition_sources())

    assert cursor.closed
    assert connection.closed
    parquet_result.close()


def test_adapter_preserves_scaled_fixed_values_as_decimals(monkeypatch, credentials):
    cursor = _Cursor(((Decimal("12.500"),),))
    cursor.description = (_Metadata("metric", "FIXED", False, precision=10, scale=3),)
    connection = _Connection(cursor)
    monkeypatch.setattr(
        "process.custom_import.snowflake_python.snowflake.connector.connect",
        lambda **_arguments: connection,
    )
    parquet_result = SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh").fetch_parquet(
        _metric_statement(),
        credentials,
    )

    assert parquet_result.schema[0].source_type == "FIXED(10,3)"
    partition_reader = next(parquet_result.consume_partition_sources())
    parquet_result.claim_partition_source(partition_reader)
    assert pq.read_table(BytesIO(partition_reader.read())).to_pydict() == {"metric": [Decimal("12.500")]}
    parquet_result.close()


def test_adapter_rejects_oversized_decoded_batch_before_arrow_allocation(monkeypatch, credentials):
    cursor = _Cursor((("1" * 5_000, "Cardiology"),))
    connection = _Connection(cursor)
    monkeypatch.setattr(
        "process.custom_import.snowflake_python.snowflake.connector.connect",
        lambda **_arguments: connection,
    )
    monkeypatch.setattr("process.custom_import.snowflake_python.MAX_RESULT_PARTITION_BYTES", 4_096)
    parquet_result = SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh").fetch_parquet(
        _statement(),
        credentials,
    )

    with pytest.raises(SnowflakeConnectorError, match="invalid or oversized"):
        next(parquet_result.consume_partition_sources())

    assert cursor.closed
    assert connection.closed
    parquet_result.close()


def test_adapter_partitions_rows_before_the_decoded_byte_limit(monkeypatch, credentials):
    cursor = _Cursor(tuple((str(index).ljust(600, "x"), "Cardiology") for index in range(10)))
    connection = _Connection(cursor)
    monkeypatch.setattr(
        "process.custom_import.snowflake_python.snowflake.connector.connect",
        lambda **_arguments: connection,
    )
    monkeypatch.setattr("process.custom_import.snowflake_python.MAX_RESULT_PARTITION_BYTES", 4_096)
    parquet_result = SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh").fetch_parquet(
        _statement(),
        credentials,
    )

    partition_row_counts = []
    for partition_reader in parquet_result.consume_partition_sources():
        parquet_result.claim_partition_source(partition_reader)
        partition_row_counts.append(pq.read_table(BytesIO(partition_reader.read())).num_rows)
        parquet_result.close_partition_source(partition_reader)

    assert partition_row_counts == [6, 4]
    assert cursor.fetch_count == 11
    parquet_result.close()


def test_adapter_closes_connection_when_execute_fails(monkeypatch, credentials):
    cursor = _Cursor(())
    connection = _Connection(cursor)

    def fail(_sql: str) -> None:
        raise RuntimeError("private server detail")

    cursor.execute = fail
    monkeypatch.setattr(
        "process.custom_import.snowflake_python.snowflake.connector.connect",
        lambda **_arguments: connection,
    )

    with pytest.raises(SnowflakeConnectorError, match="Snowflake read failed") as failure:
        SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh").fetch_parquet(
            _statement(),
            credentials,
        )

    assert "private server detail" not in str(failure.value)
    assert cursor.closed
    assert connection.closed


@pytest.mark.parametrize("interruption", (KeyboardInterrupt(), pytest.param(CancelledError(), id="cancelled")))
def test_adapter_closes_connection_when_execute_is_interrupted(monkeypatch, credentials, interruption):
    cursor = _Cursor(())
    connection = _Connection(cursor)

    def interrupt(_sql: str) -> None:
        raise interruption

    cursor.execute = interrupt
    monkeypatch.setattr(
        "process.custom_import.snowflake_python.snowflake.connector.connect",
        lambda **_arguments: connection,
    )

    with pytest.raises(type(interruption)):
        SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh").fetch_parquet(
            _statement(),
            credentials,
        )

    assert cursor.closed
    assert connection.closed


def test_adapter_preserves_fetch_cancellation_when_cleanup_fails(monkeypatch, credentials):
    cursor = _Cursor(())
    connection = _Connection(cursor)

    def cancel():
        raise CancelledError

    def fail_close() -> None:
        cursor.closed = True
        raise RuntimeError("cleanup detail")

    cursor.fetchone = cancel
    cursor.close = fail_close
    monkeypatch.setattr(
        "process.custom_import.snowflake_python.snowflake.connector.connect",
        lambda **_arguments: connection,
    )
    parquet_result = SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh").fetch_parquet(
        _statement(),
        credentials,
    )

    with pytest.raises(CancelledError):
        next(parquet_result.consume_partition_sources())

    assert cursor.closed
    assert connection.closed
    parquet_result.close()


@pytest.mark.parametrize("value", ("", "role name", "role;drop"))
def test_adapter_rejects_unsafe_connection_identifiers(value):
    with pytest.raises(SnowflakeConnectorError, match="simple identifier"):
        SnowflakePythonConnectorAdapter(role=value, warehouse="warehouse")
