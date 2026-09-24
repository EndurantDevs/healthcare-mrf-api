# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused checks for the concrete one-statement Snowflake Python adapter."""

from __future__ import annotations

import subprocess
import sys
from asyncio import CancelledError
from dataclasses import dataclass, replace
from decimal import Decimal
from io import BytesIO
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest

import process.custom_import.snowflake_python as snowflake_python
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.snowflake import (
    SnowflakeConnectorError,
    SnowflakeCredentialError,
    SnowflakeDeclaredColumn,
    SnowflakeKeyPairCredentials,
    SnowflakeReadRequest,
    SnowflakeReadStatement,
    SnowflakeRelation,
    SnowflakeResultColumn,
)
from process.custom_import.snowflake_bundle import (
    SnowflakeBundleBinding,
    SnowflakeBundleRequest,
    SnowflakeBundleStatement,
)
from process.custom_import.snowflake_python import SnowflakePythonConnectorAdapter

_PRIVATE_KEY = b"""-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIGf0q2V1QeH4rYk0uXf9ThwM3U7v6w9NfX3qf8wVv8oP
-----END PRIVATE KEY-----
"""


@dataclass(frozen=True)
class _Metadata:
    name: str
    type_name: str | None
    is_nullable: bool | None
    precision: int | None = None
    scale: int | None = None
    type_code: int | None = None


class _Cursor:
    def __init__(self, rows, *, description=None, query_id: str = "01abc") -> None:
        self.description = _description() if description is None else description
        self.sfqid = query_id
        self._rows = list(rows)
        self.executed: list[str] = []
        self.fetch_count = 0
        self.closed = False

    def execute(self, sql: str) -> None:
        self.executed.append(sql)

    def fetchone(self):
        self.fetch_count += 1
        return self._rows.pop(0) if self._rows else None

    def close(self) -> None:
        self.closed = True


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor
        self.closed = False

    def cursor(self) -> _Cursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True


def _definition() -> CustomImportDefinition:
    return CustomImportDefinition.from_mapping(
        {
            "contract": "custom-import/v1",
            "revision": {"definition": 1, "schema": 1},
            "refresh_mode": "snapshot",
            "streams": [
                {
                    "id": "root_source",
                    "kind": "root",
                    "format": "parquet",
                    "compression": "none",
                    "snapshot_token": "source_snapshot",
                }
            ],
            "schema": {
                "root": {
                    "logical_key": ["npi"],
                    "entity": {"adapter": "npi", "field": "npi"},
                    "fields": [
                        {"id": "npi", "slot": 1, "type": "string", "nullable": False},
                        {"id": "specialty", "slot": 2, "type": "string", "nullable": True},
                    ],
                },
                "children": [],
            },
            "aliases": {"root_source": {}},
            "query": {"root_fields": [], "order": []},
            "selection_profiles": [],
        }
    )


def _bundle_statement() -> SnowflakeBundleStatement:
    definition = _definition()
    data_relation = SnowflakeRelation(database="demo", schema="curated", name="providers")
    snapshot_relation = SnowflakeRelation(database="demo", schema="curated", name="snapshots")
    request = SnowflakeBundleRequest(
        definition=definition,
        bindings=(
            SnowflakeBundleBinding(
                stream_id="root_source",
                relation=data_relation,
                source_snapshot_token_relation=snapshot_relation,
                selected_field_ids=("npi", "specialty"),
                semantic_token_metadata_key="source_snapshot",
            ),
        ),
    )
    return SnowflakeBundleStatement(
        request=request,
        selected_columns_by_stream=(
            (
                SnowflakeDeclaredColumn(field_id="npi", column_identifier="npi_num"),
                SnowflakeDeclaredColumn(field_id="specialty", column_identifier="specialty_name"),
            ),
        ),
        source_snapshot_token_columns_by_stream=(
            SnowflakeDeclaredColumn(field_id="source_snapshot", column_identifier="snapshot_token"),
        ),
    )


def _legacy_statement() -> SnowflakeReadStatement:
    return SnowflakeReadStatement(
        SnowflakeReadRequest(
            relation=SnowflakeRelation(database="demo", schema="curated", name="providers"),
            selected_columns=(SnowflakeDeclaredColumn(field_id="npi", column_identifier="npi_num"),),
            definition_sha256="1" * 64,
            schema_sha256="2" * 64,
        )
    )


def _description(*, data_columns=None):
    return (
        _Metadata("__ci_bundle_row_kind", "FIXED", False, precision=1, scale=0),
        _Metadata("__ci_stream_ordinal", "FIXED", False, precision=2, scale=0),
        _Metadata("__ci_stream_id", "TEXT", False),
        _Metadata("__ci_source_snapshot_token", "TEXT", True),
        *(data_columns or (_Metadata("npi", "TEXT", False), _Metadata("specialty", "TEXT", True))),
    )


def _metadata_row() -> tuple[object, ...]:
    return (0, 1, "root_source", "synthetic-snapshot", None, None)


def _data_row(npi: object, specialty: object) -> tuple[object, ...]:
    return (1, 1, "root_source", None, npi, specialty)


@pytest.fixture
def credentials(monkeypatch) -> SnowflakeKeyPairCredentials:
    monkeypatch.setattr(snowflake_python, "_private_key_der", lambda _credentials: b"private-key-der")
    return SnowflakeKeyPairCredentials(account="example", user="reader", private_key_pem=_PRIVATE_KEY)


def _connect(monkeypatch, cursor: _Cursor):
    connection = _Connection(cursor)
    arguments_by_name = {}

    def connect(**arguments):
        arguments_by_name.update(arguments)
        return connection

    monkeypatch.setattr(snowflake_python.snowflake.connector, "connect", connect)
    return connection, arguments_by_name


def _parquet_tables(result) -> list[dict[str, list[object]]]:
    parquet_result = result.stream_results[0].parquet_result
    tables = []
    for reader in parquet_result.consume_partition_sources():
        parquet_result.claim_partition_source(reader)
        tables.append(pq.read_table(BytesIO(reader.read())).to_pydict())
        parquet_result.close_partition_source(reader)
    return tables


def test_connector_import_accepts_the_project_pyarrow_version():
    completed = subprocess.run(
        [sys.executable, "-W", "error::UserWarning", "-c", "import snowflake.connector"],
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr


def test_adapter_fetches_one_generated_bundle_with_fixed_connection_policy(monkeypatch, credentials):
    cursor = _Cursor((_metadata_row(), _data_row("1234567893", "Cardiology"), _data_row("1003000126", None)))
    connection, arguments = _connect(monkeypatch, cursor)
    statement = _bundle_statement()

    bundle_result = SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh").fetch_bundle(
        statement,
        credentials,
    )

    assert cursor.executed == ["USE SECONDARY ROLES NONE", statement.sql]
    assert arguments == {
        "account": "example",
        "user": "reader",
        "authenticator": "SNOWFLAKE_JWT",
        "private_key": b"private-key-der",
        "role": "READER_ROLE",
        "warehouse": "IMPORT_WH",
        "autocommit": False,
        "client_session_keep_alive": False,
        "login_timeout": 30,
        "network_timeout": 120,
        "socket_timeout": 120,
        "session_parameters": {
            "QUERY_TAG": "custom-import/v1-snowflake",
            "STATEMENT_TIMEOUT_IN_SECONDS": 120,
        },
    }
    assert bundle_result.query_id == "01abc"
    assert bundle_result.stream_results[0].parquet_result.source_snapshot_token == "synthetic-snapshot"
    assert _parquet_tables(bundle_result) == [{"npi": ["1234567893", "1003000126"], "specialty": ["Cardiology", None]}]
    bundle_result.close()
    assert cursor.closed and connection.closed


def test_adapter_emits_a_schema_bearing_empty_bundle_stream(monkeypatch, credentials):
    cursor = _Cursor((_metadata_row(),))
    connection, _arguments = _connect(monkeypatch, cursor)

    result = SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh").fetch_bundle(
        _bundle_statement(),
        credentials,
    )

    assert _parquet_tables(result) == [{"npi": [], "specialty": []}]
    result.close()
    assert cursor.closed and connection.closed


def test_adapter_resolves_type_codes_and_rejects_bad_result_boundaries(monkeypatch, credentials):
    text_type_code = next(
        index for index, field_type in enumerate(snowflake_python.SNOWFLAKE_FIELD_TYPES) if field_type.name == "TEXT"
    )
    cursor = _Cursor(
        (_metadata_row(),),
        description=_description(
            data_columns=(
                SimpleNamespace(name="npi", type_name=None, type_code=text_type_code, is_nullable=False),
                SimpleNamespace(name="specialty", type_name=None, type_code=text_type_code, is_nullable=True),
            )
        ),
    )
    connection, _arguments = _connect(monkeypatch, cursor)
    bundle_result = SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh").fetch_bundle(
        _bundle_statement(), credentials
    )
    assert [column.source_type for column in bundle_result.stream_results[0].parquet_result.schema] == ["TEXT", "TEXT"]
    bundle_result.close()
    assert cursor.closed and connection.closed

    for description, query_id, message in (
        ((), "01abc", "result schema"),
        (
            _description(data_columns=(_Metadata("wrong", "TEXT", False), _Metadata("specialty", "TEXT", True))),
            "01abc",
            "result schema",
        ),
        (_description(), " ", "statement identity"),
        (
            _description(data_columns=(_Metadata("npi", "DATE", False), _Metadata("specialty", "TEXT", True))),
            "01abc",
            "not supported",
        ),
    ):
        bad_cursor = _Cursor((_metadata_row(),), description=description, query_id=query_id)
        bad_connection, _arguments = _connect(monkeypatch, bad_cursor)
        with pytest.raises(SnowflakeConnectorError, match=message):
            SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh").fetch_bundle(
                _bundle_statement(), credentials
            )
        assert bad_cursor.closed and bad_connection.closed


def test_adapter_closes_when_result_construction_or_execute_fails(monkeypatch, credentials):
    cursor = _Cursor((_metadata_row(),))
    connection, _arguments = _connect(monkeypatch, cursor)
    monkeypatch.setattr(
        snowflake_python,
        "SnowflakeBundleResult",
        lambda **_arguments: (_ for _ in ()).throw(SnowflakeConnectorError("invalid adapter result")),
    )

    with pytest.raises(SnowflakeConnectorError, match="invalid adapter result"):
        SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh").fetch_bundle(
            _bundle_statement(), credentials
        )
    assert cursor.closed and connection.closed

    cursor = _Cursor(())
    connection, _arguments = _connect(monkeypatch, cursor)

    executed_statements = []

    def fail_execute(sql: str) -> None:
        executed_statements.append(sql)
        if sql == "USE SECONDARY ROLES NONE":
            return
        raise RuntimeError("private server detail")

    cursor.execute = fail_execute
    with pytest.raises(SnowflakeConnectorError, match="Snowflake bundle read failed") as failure:
        SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh").fetch_bundle(
            _bundle_statement(), credentials
        )
    assert "private server detail" not in str(failure.value)
    assert executed_statements == ["USE SECONDARY ROLES NONE", _bundle_statement().sql]
    assert cursor.closed and connection.closed


@pytest.mark.parametrize(
    ("method_name", "statement"),
    (("fetch_parquet", _legacy_statement()), ("fetch_bundle", _bundle_statement())),
)
def test_adapter_closes_when_role_initialization_fails(monkeypatch, credentials, method_name, statement):
    cursor = _Cursor(())
    connection, _arguments = _connect(monkeypatch, cursor)
    executed_statements = []

    def fail_initialization(sql: str) -> None:
        executed_statements.append(sql)
        raise RuntimeError("synthetic initialization failure")

    cursor.execute = fail_initialization
    adapter = SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh")
    with pytest.raises(SnowflakeConnectorError, match=r"Snowflake (?:bundle )?read failed"):
        getattr(adapter, method_name)(statement, credentials)
    assert executed_statements == ["USE SECONDARY ROLES NONE"]
    assert cursor.closed and connection.closed


@pytest.mark.parametrize("interruption", (KeyboardInterrupt(), pytest.param(CancelledError(), id="cancelled")))
def test_adapter_preserves_execute_interruptions_while_closing(monkeypatch, credentials, interruption):
    cursor = _Cursor(())
    connection, _arguments = _connect(monkeypatch, cursor)

    def interrupt(_sql: str) -> None:
        raise interruption

    cursor.execute = interrupt
    with pytest.raises(type(interruption)):
        SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh").fetch_bundle(
            _bundle_statement(), credentials
        )
    assert cursor.closed and connection.closed


def test_adapter_preserves_fetch_cancellation_when_cleanup_fails(monkeypatch, credentials):
    cursor = _Cursor((_metadata_row(),))
    connection, _arguments = _connect(monkeypatch, cursor)
    result = SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh").fetch_bundle(
        _bundle_statement(), credentials
    )

    def cancel():
        raise CancelledError

    def fail_close() -> None:
        cursor.closed = True
        raise RuntimeError("cleanup detail")

    cursor.fetchone = cancel
    cursor.close = fail_close
    with pytest.raises(CancelledError):
        next(result.stream_results[0].parquet_result.consume_partition_sources())
    assert cursor.closed and connection.closed
    result.close()


def test_adapter_closes_after_a_deferred_fetch_error(monkeypatch, credentials):
    cursor = _Cursor((_metadata_row(),))
    connection, _arguments = _connect(monkeypatch, cursor)
    result = SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh").fetch_bundle(
        _bundle_statement(), credentials
    )

    def fail_fetch():
        raise RuntimeError("synthetic fetch failure")

    cursor.fetchone = fail_fetch
    with pytest.raises(SnowflakeConnectorError, match="result fetch failed"):
        next(result.stream_results[0].parquet_result.consume_partition_sources())
    assert cursor.closed and connection.closed
    result.close()


def test_adapter_preserves_wide_decimals_and_bounds_partitions(monkeypatch, credentials):
    cursor = _Cursor(
        (_metadata_row(), _data_row(2**63, "Cardiology")),
        description=_description(
            data_columns=(
                _Metadata("npi", "FIXED", False, precision=38, scale=0),
                _Metadata("specialty", "TEXT", True),
            )
        ),
    )
    connection, _arguments = _connect(monkeypatch, cursor)
    bundle_result = SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh").fetch_bundle(
        _bundle_statement(), credentials
    )
    assert _parquet_tables(bundle_result)[0]["npi"] == [Decimal(2**63)]
    bundle_result.close()
    assert cursor.closed and connection.closed

    cursor = _Cursor((_metadata_row(), *(_data_row(str(index).ljust(600, "x"), "Cardiology") for index in range(10))))
    connection, _arguments = _connect(monkeypatch, cursor)
    monkeypatch.setattr(snowflake_python, "MAX_RESULT_PARTITION_BYTES", 4_096)
    bundle_result = SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh").fetch_bundle(
        _bundle_statement(), credentials
    )
    assert [len(table["npi"]) for table in _parquet_tables(bundle_result)] == [6, 4]
    bundle_result.close()
    assert cursor.closed and connection.closed


def _assert_scalar_validation_bounds() -> SnowflakeResultColumn:
    text_column = SnowflakeResultColumn(field_id="value", source_type="TEXT", nullable=False)
    boolean_column = SnowflakeResultColumn(field_id="value", source_type="BOOLEAN", nullable=True)
    fixed_column = SnowflakeResultColumn(field_id="value", source_type="FIXED(10,0)", nullable=True)
    scaled_column = SnowflakeResultColumn(field_id="value", source_type="FIXED(10,2)", nullable=True)

    with pytest.raises(SnowflakeConnectorError, match="unavailable"):
        snowflake_python._source_type(SimpleNamespace(type_name=None, type_code=True))
    with pytest.raises(SnowflakeConnectorError, match="precision is invalid"):
        snowflake_python._metadata_integer(SimpleNamespace(precision=True), "precision", minimum=1, maximum=38)
    with pytest.raises(SnowflakeConnectorError, match="nullability is invalid"):
        snowflake_python._is_nullable(SimpleNamespace(is_nullable="yes"))
    with pytest.raises(SnowflakeConnectorError, match="does not match"):
        snowflake_python._result_row_variable_bytes("x", (text_column,))
    with pytest.raises(SnowflakeConnectorError, match="non-nullable"):
        snowflake_python._variable_scalar_bytes(None, text_column)
    with pytest.raises(SnowflakeConnectorError, match="TEXT result value"):
        snowflake_python._variable_scalar_bytes(1, text_column)
    with pytest.raises(SnowflakeConnectorError, match="valid UTF-8"):
        snowflake_python._variable_scalar_bytes("\ud800", SnowflakeResultColumn("value", "TEXT", True))
    with pytest.raises(SnowflakeConnectorError, match="BOOLEAN result value"):
        snowflake_python._variable_scalar_bytes(1, boolean_column)
    with pytest.raises(SnowflakeConnectorError, match="signed 64-bit"):
        snowflake_python._variable_scalar_bytes(True, fixed_column)
    with pytest.raises(SnowflakeConnectorError, match="FIXED result value"):
        snowflake_python._variable_scalar_bytes("not-a-decimal", scaled_column)
    assert snowflake_python._variable_scalar_bytes(Decimal("12.50"), scaled_column) == 0
    assert snowflake_python._arrow_fixed_bytes("BOOLEAN", 1) == 2
    assert snowflake_python._arrow_fixed_bytes("FIXED(10,0)", 1) == 9
    assert snowflake_python._arrow_type(boolean_column) == snowflake_python.pa.bool_()
    with pytest.raises(SnowflakeConnectorError, match="not supported by the Parquet encoder"):
        snowflake_python._fixed_type_parts("invalid")

    return text_column


def test_adapter_validates_scalars_and_parquet_encoding_bounds(monkeypatch):
    text_column = _assert_scalar_validation_bounds()

    monkeypatch.setattr(snowflake_python, "MAX_RESULT_PARTITION_BYTES", 1)
    with pytest.raises(SnowflakeConnectorError, match="decoded-byte limit"):
        snowflake_python._validate_result_rows((("x",),), (text_column,))

    fake_arrow = SimpleNamespace(
        ArrowException=RuntimeError,
        Table=SimpleNamespace(from_arrays=lambda *_args, **_kwargs: SimpleNamespace(nbytes=101)),
        array=lambda *_args, **_kwargs: object(),
        field=lambda *_args, **_kwargs: object(),
        schema=lambda *_args, **_kwargs: object(),
        string=lambda: object(),
    )
    monkeypatch.setattr(snowflake_python, "pa", fake_arrow)
    monkeypatch.setattr(snowflake_python, "MAX_RESULT_PARTITION_BYTES", 100)
    with pytest.raises(SnowflakeConnectorError, match="decoded-byte limit"):
        snowflake_python._parquet_reader((("x",),), (text_column,))

    def invalid_table(*_args, **_kwargs):
        raise TypeError("synthetic Arrow failure")

    fake_arrow.Table.from_arrays = invalid_table
    with pytest.raises(SnowflakeConnectorError, match="cannot be encoded"):
        snowflake_python._parquet_reader((("x",),), (text_column,))

    fake_arrow.Table.from_arrays = lambda *_args, **_kwargs: SimpleNamespace(nbytes=0)
    monkeypatch.setattr(
        snowflake_python.pq,
        "write_table",
        lambda _table, destination, **_kwargs: destination.write(b"x" * 101),
    )
    with pytest.raises(SnowflakeConnectorError, match="Parquet partition exceeds"):
        snowflake_python._parquet_reader((("x",),), (text_column,))


def test_adapter_rejects_invalid_bundle_arguments_and_unsafe_connection_identifiers(credentials):
    adapter = SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh")
    with pytest.raises(SnowflakeConnectorError, match="generated Snowflake bundle statement"):
        adapter.fetch_bundle(object(), credentials)
    with pytest.raises(SnowflakeCredentialError, match="key-pair credentials"):
        adapter.fetch_bundle(_bundle_statement(), object())

    for value in ("", "role name", "role;drop"):
        with pytest.raises(SnowflakeConnectorError, match="simple identifier"):
            SnowflakePythonConnectorAdapter(role=value, warehouse="warehouse")


def test_adapter_fetches_legacy_single_stream_reads(monkeypatch, credentials):
    cursor = _Cursor((("1003000126",),), description=(_Metadata("npi", "TEXT", False),))
    connection, _arguments = _connect(monkeypatch, cursor)

    result = SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh").fetch_parquet(
        _legacy_statement(), credentials
    )
    iterator = result.consume_partition_sources()
    reader = next(iterator)
    result.claim_partition_source(reader)
    assert result.source_snapshot_token == "snowflake-query:01abc"
    assert pq.read_table(BytesIO(reader.read())).to_pydict() == {"npi": ["1003000126"]}
    result.close_partition_source(reader)
    with pytest.raises(StopIteration):
        next(iterator)
    result.close_partition_iterator()
    result.close()
    assert cursor.executed == ["USE SECONDARY ROLES NONE", _legacy_statement().sql]
    assert cursor.closed and connection.closed


def test_adapter_wraps_invalid_private_key_material(monkeypatch):
    credentials = SnowflakeKeyPairCredentials(account="example", user="reader", private_key_pem=_PRIVATE_KEY)
    monkeypatch.setattr(
        snowflake_python.serialization,
        "load_pem_private_key",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("synthetic invalid key")),
    )

    with pytest.raises(SnowflakeCredentialError, match="cannot be loaded"):
        snowflake_python._private_key_der(credentials)


def test_legacy_adapter_and_partition_sources_reject_invalid_transport_states(monkeypatch, credentials):
    adapter = SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh")
    with pytest.raises(SnowflakeConnectorError, match="generated Snowflake statement"):
        adapter.fetch_parquet(object(), credentials)
    with pytest.raises(SnowflakeCredentialError, match="key-pair credentials"):
        adapter.fetch_parquet(_legacy_statement(), object())

    cursor = _Cursor((), description=(_Metadata("npi", "TEXT", False),), query_id=" ")
    connection, _arguments = _connect(monkeypatch, cursor)
    with pytest.raises(SnowflakeConnectorError, match="statement identity"):
        adapter.fetch_parquet(_legacy_statement(), credentials)
    assert cursor.closed and connection.closed

    schema = (SnowflakeResultColumn(field_id="npi", source_type="TEXT", nullable=False),)
    cursor = _Cursor((), description=(_Metadata("npi", "TEXT", False),))
    connection = _Connection(cursor)
    partition_sources = snowflake_python._SnowflakeParquetPartitionSources(
        connection=connection,
        cursor=cursor,
        result_schema=schema,
    )
    reader = next(iter(partition_sources))
    reader.close()
    with pytest.raises(SnowflakeConnectorError, match="already consumed"):
        next(iter(partition_sources))
    partition_sources.close()
    partition_sources.close()
    assert cursor.closed and connection.closed

    cursor = _Cursor((), description=(_Metadata("npi", "TEXT", False),))
    partition_source = snowflake_python._SnowflakeParquetPartitionSources(
        connection=_Connection(cursor), cursor=cursor, result_schema=schema
    )
    monkeypatch.setattr(snowflake_python, "_FETCH_ROWS", 1)
    cursor._rows = [("1234567893",)]
    assert partition_source._next_partition_rows(None) == ([("1234567893",)], None, False)
    partition_source.close()

    cursor = _Cursor((("",),), description=(_Metadata("npi", "TEXT", False),))
    partition_source = snowflake_python._SnowflakeParquetPartitionSources(
        connection=_Connection(cursor), cursor=cursor, result_schema=schema
    )
    monkeypatch.setattr(snowflake_python, "MAX_RESULT_PARTITION_BYTES", 4)
    with pytest.raises(SnowflakeConnectorError, match="decoded-byte limit"):
        partition_source._next_partition_rows(None)
    partition_source.close()


def test_bundle_transport_rejects_malformed_metadata():
    statement = _bundle_statement()
    valid_description = _description()
    for description, message in (
        (
            valid_description[:0] + (replace(valid_description[0], scale=1),) + valid_description[1:],
            "discriminator schema",
        ),
        (
            valid_description[:1] + (replace(valid_description[1], scale=1),) + valid_description[2:],
            "discriminator schema",
        ),
        (
            valid_description[:2] + (replace(valid_description[2], is_nullable=True),) + valid_description[3:],
            "discriminator schema",
        ),
        (
            valid_description[:3] + (replace(valid_description[3], type_name="BOOLEAN"),) + valid_description[4:],
            "snapshot metadata",
        ),
    ):
        with pytest.raises(SnowflakeConnectorError, match=message):
            snowflake_python._bundle_result_schemas(statement, description)

    for metadata_rows, message in (
        ((), "metadata observations are incomplete"),
        (((0, 1, "wrong", "synthetic-snapshot", None, None),), "metadata row"),
        ((_metadata_row(), _metadata_row()), "exactly one per stream"),
    ):
        with pytest.raises(SnowflakeConnectorError, match=message):
            snowflake_python._bundle_stream_metadata(statement, _Cursor(metadata_rows))
    with pytest.raises(SnowflakeConnectorError, match="row kind"):
        snowflake_python._bundle_row_kind((True,))


def test_bundle_transport_rejects_invalid_stream_ownership_and_result_rows():
    statement = _bundle_statement()
    valid_description = _description()
    schemas = snowflake_python._bundle_result_schemas(statement, valid_description)
    cursor = _Cursor(())
    connection = _Connection(cursor)
    owner = snowflake_python._SnowflakeBundlePartitionSources(
        connection=connection,
        cursor=cursor,
        statement=statement,
        schemas=schemas,
        pending_row=None,
    )
    with pytest.raises(SnowflakeConnectorError, match="configured order"):
        owner.next_partition_rows(1)
    with pytest.raises(SnowflakeConnectorError, match="not contiguous"):
        owner.finish_stream(1)
    for result_row, message in (
        ("bad", "result row"),
        ((1,), "result row"),
        ((0, 1, "root_source", None, "npi", None), "data row"),
    ):
        with pytest.raises(SnowflakeConnectorError, match=message):
            owner._split_row(result_row, 0)
    stream = owner.stream_sources(0)
    stream.close()
    stream.close()
    owner.close()
    with pytest.raises(SnowflakeConnectorError, match="result is closed"):
        owner.next_partition_rows(0)
    assert cursor.closed and connection.closed

    with pytest.raises(SnowflakeConnectorError, match="selected fields"):
        snowflake_python._result_schema(_legacy_statement(), ())
    with pytest.raises(SnowflakeConnectorError, match="selected fields"):
        snowflake_python._result_schema(_legacy_statement(), (_Metadata("wrong", "TEXT", False),))
    assert snowflake_python._is_nullable(SimpleNamespace(is_nullable=None)) is True
    assert snowflake_python._uses_decimal_storage("TEXT") is False
    with pytest.raises(SnowflakeConnectorError, match="selected fields"):
        snowflake_python._result_row_variable_bytes(("a", "b"), (SnowflakeResultColumn("value", "TEXT", True),))
