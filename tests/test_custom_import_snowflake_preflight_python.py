# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused synthetic checks for the Snowflake Python preflight cursor adapter."""

from __future__ import annotations

from dataclasses import dataclass, replace

import pytest

import process.custom_import.snowflake_python as snowflake_python
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.snowflake import (
    SnowflakeConnectorError,
    SnowflakeCredentialError,
    SnowflakeDeclaredColumn,
    SnowflakeKeyPairCredentials,
    SnowflakeRelation,
)
from process.custom_import.snowflake_bundle import (
    SnowflakeBundleBinding,
    SnowflakeBundleRequest,
    SnowflakeBundleStatement,
)
from process.custom_import.snowflake_preflight import SnowflakePreflightLimits, SnowflakePreflightStatement
from process.custom_import.snowflake_python import SnowflakePythonConnectorAdapter, SnowflakePythonPreflightAdapter

_PRIVATE_KEY = b"-----BEGIN PRIVATE KEY-----\nsynthetic\n-----END PRIVATE KEY-----"


@dataclass(frozen=True)
class _Metadata:
    name: str
    type_name: str
    is_nullable: bool
    precision: int | None = None
    scale: int | None = None


class _Cursor:
    def __init__(
        self,
        rows: tuple[tuple[object, ...], ...],
        *,
        description: tuple[_Metadata, ...],
        query_id: str = "synthetic-query-id",
    ) -> None:
        self.description = description
        self.sfqid = query_id
        self._rows = list(rows)
        self.executed: list[str] = []
        self.fetch_count = 0
        self.close_count = 0

    def execute(self, sql: str) -> None:
        self.executed.append(sql)

    def fetchone(self) -> tuple[object, ...] | None:
        self.fetch_count += 1
        return self._rows.pop(0) if self._rows else None

    def close(self) -> None:
        self.close_count += 1


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor
        self.close_count = 0

    def cursor(self) -> _Cursor:
        return self._cursor

    def close(self) -> None:
        self.close_count += 1


class _CredentialProvider:
    def __init__(self, credentials: SnowflakeKeyPairCredentials) -> None:
        self._credentials = credentials
        self.calls = 0

    def load_key_pair(self) -> SnowflakeKeyPairCredentials:
        self.calls += 1
        return self._credentials


def _definition(*, includes_scalars: bool = False) -> CustomImportDefinition:
    root_fields = [{"id": "npi", "slot": 1, "type": "string", "nullable": False}]
    if includes_scalars:
        root_fields.extend(
            (
                {"id": "edition", "slot": 2, "type": "integer", "nullable": False},
                {"id": "amount", "slot": 3, "type": "decimal", "nullable": True},
                {"id": "active", "slot": 4, "type": "boolean", "nullable": True},
            )
        )
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
                    "fields": root_fields,
                },
                "children": [],
            },
            "aliases": {"root_source": {}},
            "query": {"root_fields": [], "order": []},
            "selection_profiles": [],
        }
    )


def _bundle_statement(*, includes_scalars: bool = False) -> SnowflakeBundleStatement:
    definition = _definition(includes_scalars=includes_scalars)
    selected_field_ids = tuple(field.field_id for field in definition.root_fields)
    request = SnowflakeBundleRequest(
        definition=definition,
        bindings=(
            SnowflakeBundleBinding(
                stream_id="root_source",
                relation=SnowflakeRelation(database="sample", schema="curated", name="records"),
                source_snapshot_token_relation=None,
                selected_field_ids=selected_field_ids,
                semantic_token_metadata_key="source_snapshot",
            ),
        ),
    )
    return SnowflakeBundleStatement(
        request=request,
        selected_columns_by_stream=(
            tuple(
                SnowflakeDeclaredColumn(field_id=field_id, column_identifier=f"{field_id}_value")
                for field_id in selected_field_ids
            ),
        ),
        source_snapshot_token_columns_by_stream=(None,),
    )


def _statement(*, includes_scalars: bool = False) -> SnowflakePreflightStatement:
    return SnowflakePreflightStatement(
        bundle_statement=_bundle_statement(includes_scalars=includes_scalars),
        limits=SnowflakePreflightLimits(maximum_root_keys=1),
    )


def _description(statement: SnowflakePreflightStatement) -> tuple[_Metadata, ...]:
    fixed_columns = {
        "__ci_preflight_kind",
        "__ci_preflight_stream_ordinal",
        "__ci_preflight_key_ordinal",
        "__ci_preflight_key_multiplicity",
    }
    source_type_by_value_type = {
        "string": ("TEXT", None, None),
        "integer": ("FIXED", 38, 0),
        "decimal": ("FIXED", 18, 2),
        "boolean": ("BOOLEAN", None, None),
    }
    fields_by_id = statement.definition.fields_by_id
    metadata = []
    for column_id in statement.column_ids:
        if column_id in fixed_columns:
            type_name, precision, scale = "FIXED", 38, 0
        elif column_id in fields_by_id:
            type_name, precision, scale = source_type_by_value_type[fields_by_id[column_id].value_type]
        else:
            type_name, precision, scale = "TEXT", None, None
        metadata.append(_Metadata(column_id, type_name, True, precision, scale))
    return tuple(metadata)


def _connect(monkeypatch, cursor: _Cursor) -> tuple[_Connection, dict[str, object]]:
    connection = _Connection(cursor)
    connection_arguments_by_key: dict[str, object] = {}

    def connect(**kwargs):
        connection_arguments_by_key.update(kwargs)
        return connection

    monkeypatch.setattr(snowflake_python.snowflake.connector, "connect", connect)
    return connection, connection_arguments_by_key


@pytest.fixture
def credentials(monkeypatch) -> SnowflakeKeyPairCredentials:
    monkeypatch.setattr(snowflake_python, "_private_key_der", lambda _credentials: b"synthetic-private-key")
    return SnowflakeKeyPairCredentials(account="sample", user="reader", private_key_pem=_PRIVATE_KEY)


def _adapter(credentials: SnowflakeKeyPairCredentials) -> tuple[SnowflakePythonPreflightAdapter, _CredentialProvider]:
    provider = _CredentialProvider(credentials)
    return (
        SnowflakePythonPreflightAdapter(
            connector=SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh"),
            credential_provider=provider,
        ),
        provider,
    )


def test_preflight_adapter_executes_generated_statement_once_with_bounded_timeout(monkeypatch, credentials):
    statement = _statement()
    preflight_row = (0, 1, "root_source", None, None, None, None)
    cursor = _Cursor((preflight_row,), description=_description(statement), query_id="native-sfqid-123")
    connection, connection_arguments_by_key = _connect(monkeypatch, cursor)
    adapter, provider = _adapter(credentials)

    preflight_cursor = adapter.open_preflight(statement, timeout_seconds=17)

    assert provider.calls == 1
    assert cursor.executed == ["USE SECONDARY ROLES NONE", statement.sql]
    assert connection_arguments_by_key == {
        "account": "sample",
        "user": "reader",
        "authenticator": "SNOWFLAKE_JWT",
        "private_key": b"synthetic-private-key",
        "role": "READER_ROLE",
        "warehouse": "IMPORT_WH",
        "autocommit": False,
        "client_session_keep_alive": False,
        "login_timeout": 17,
        "network_timeout": 17,
        "socket_timeout": 17,
        "session_parameters": {
            "QUERY_TAG": "custom-import/v1-snowflake",
            "STATEMENT_TIMEOUT_IN_SECONDS": 17,
        },
    }
    assert preflight_cursor.column_ids == statement.column_ids
    assert preflight_cursor.query_id == "native-sfqid-123"
    assert preflight_cursor.fetchone() == preflight_row
    preflight_cursor.close()
    preflight_cursor.close()
    assert cursor.close_count == 1
    assert connection.close_count == 1


def test_preflight_adapter_bounds_login_timeout_to_the_requested_second(monkeypatch, credentials):
    statement = _statement()
    cursor = _Cursor((), description=_description(statement))
    connection, connection_arguments_by_key = _connect(monkeypatch, cursor)
    adapter, _provider = _adapter(credentials)

    preflight_cursor = adapter.open_preflight(statement, timeout_seconds=1)

    assert connection_arguments_by_key["login_timeout"] == 1
    preflight_cursor.close()
    assert cursor.close_count == 1
    assert connection.close_count == 1


def test_preflight_adapter_validates_exact_schema_and_types_before_fetching(monkeypatch, credentials):
    statement = _statement()
    valid_description = _description(statement)
    invalid_descriptions = (
        replace(valid_description[0], name="unexpected"),
        replace(valid_description[-1], type_name="DATE"),
    )

    for invalid_column in invalid_descriptions:
        description = (
            (invalid_column, *valid_description[1:])
            if invalid_column.name == "unexpected"
            else (
                *valid_description[:-1],
                invalid_column,
            )
        )
        cursor = _Cursor((), description=description)
        connection, _arguments = _connect(monkeypatch, cursor)
        adapter, _provider = _adapter(credentials)

        with pytest.raises(SnowflakeConnectorError):
            adapter.open_preflight(statement, timeout_seconds=1)

        assert cursor.fetch_count == 0
        assert cursor.close_count == 1
        assert connection.close_count == 1


def test_preflight_adapter_rejects_wrong_metadata_and_declared_field_types_before_fetching(monkeypatch, credentials):
    statement = _statement(includes_scalars=True)
    valid_description = _description(statement)
    invalid_type_by_column = {
        "__ci_preflight_kind": ("FIXED", 38, 1),
        "__ci_preflight_stream_ordinal": ("TEXT", None, None),
        "__ci_preflight_stream_id": ("FIXED", 38, 0),
        "__ci_preflight_key_ordinal": ("BOOLEAN", None, None),
        "__ci_preflight_source_snapshot_token": ("BOOLEAN", None, None),
        "__ci_preflight_key_multiplicity": ("FIXED", 38, 1),
        "npi": ("FIXED", 38, 0),
        "edition": ("FIXED", 38, 1),
        "amount": ("BOOLEAN", None, None),
        "active": ("TEXT", None, None),
    }

    for column_id, (type_name, precision, scale) in invalid_type_by_column.items():
        index = statement.column_ids.index(column_id)
        invalid_column = _Metadata(column_id, type_name, True, precision, scale)
        description = (*valid_description[:index], invalid_column, *valid_description[index + 1 :])
        cursor = _Cursor((), description=description)
        connection, _arguments = _connect(monkeypatch, cursor)
        adapter, _provider = _adapter(credentials)

        with pytest.raises(SnowflakeConnectorError, match="preflight result schema"):
            adapter.open_preflight(statement, timeout_seconds=1)

        assert cursor.fetch_count == 0
        assert cursor.close_count == 1
        assert connection.close_count == 1


def test_preflight_adapter_closes_resources_after_execute_and_fetch_errors(monkeypatch, credentials):
    statement = _statement()
    cursor = _Cursor((), description=_description(statement))
    connection, _arguments = _connect(monkeypatch, cursor)

    def fail_execute(sql: str) -> None:
        cursor.executed.append(sql)
        if sql != "USE SECONDARY ROLES NONE":
            raise RuntimeError("synthetic execution failure")

    cursor.execute = fail_execute
    adapter, _provider = _adapter(credentials)
    with pytest.raises(SnowflakeConnectorError, match="preflight read failed"):
        adapter.open_preflight(statement, timeout_seconds=1)
    assert cursor.close_count == 1
    assert connection.close_count == 1

    cursor = _Cursor((), description=_description(statement))
    connection, _arguments = _connect(monkeypatch, cursor)
    adapter, _provider = _adapter(credentials)
    preflight_cursor = adapter.open_preflight(statement, timeout_seconds=1)

    def fail_fetch() -> None:
        raise RuntimeError("synthetic fetch failure")

    cursor.fetchone = fail_fetch
    with pytest.raises(SnowflakeConnectorError, match="preflight result fetch failed"):
        preflight_cursor.fetchone()
    assert cursor.close_count == 1
    assert connection.close_count == 1


@pytest.mark.parametrize("timeout_seconds", (0, 121, True, "1"))
def test_preflight_adapter_rejects_invalid_timeout_before_loading_credentials(credentials, timeout_seconds):
    adapter, provider = _adapter(credentials)

    with pytest.raises(SnowflakeConnectorError, match="execution timeout"):
        adapter.open_preflight(_statement(), timeout_seconds=timeout_seconds)

    assert provider.calls == 0


def test_preflight_adapter_requires_generated_statement_and_fixed_provider(credentials):
    adapter, provider = _adapter(credentials)
    with pytest.raises(SnowflakeConnectorError, match="generated Snowflake preflight statement"):
        adapter.open_preflight(object(), timeout_seconds=1)
    assert provider.calls == 0

    with pytest.raises(SnowflakeCredentialError, match="fixed key-pair credential provider"):
        SnowflakePythonPreflightAdapter(
            connector=SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh"),
            credential_provider=object(),
        )


def test_preflight_statement_rejects_constructor_supplied_sql():
    with pytest.raises(TypeError):
        SnowflakePreflightStatement(
            bundle_statement=_bundle_statement(),
            limits=SnowflakePreflightLimits(maximum_root_keys=1),
            sql="SELECT synthetic_value",
        )
