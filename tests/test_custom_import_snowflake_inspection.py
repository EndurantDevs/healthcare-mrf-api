# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic SQL execution and boundary checks for discovery/source estimates."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from types import SimpleNamespace

import pytest

import process.custom_import.snowflake_inspection as inspection
import process.custom_import.snowflake_operator_cli as operator_cli
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.snowflake_binding import SnowflakeSourceBinding
from process.custom_import.snowflake_bundle import SnowflakeBundleStatementBuilder
from process.custom_import.snowflake_inspection import SnowflakeInspectionError, inspect_snowflake_bundle
from process.custom_import.snowflake_preflight import SnowflakePreflightLimits
from tests.test_custom_import_processing_policy import _policy_document
from tests.test_custom_import_snowflake_decimal_conversions import _binding as _decimal_binding
from tests.test_custom_import_snowflake_preflight_cli import (
    _CredentialProvider,
    _Database,
    _loaded_binding,
    _PreflightAdapter,
    _SourceAdapter,
)


class _Cursor:
    def __init__(self, cursor, statement):
        self.cursor = cursor
        self.description = tuple(
            SimpleNamespace(
                name=column[0],
                type_name="FIXED" if statement.operation == "estimate" else "TEXT",
                precision=38,
                scale=0,
            )
            for column in cursor.description
        )
        self.fetches = 0
        self.closed = False

    def fetchone(self):
        self.fetches += 1
        return self.cursor.fetchone()

    def close(self):
        self.closed = True
        self.cursor.close()


class _Adapter:
    def __init__(self, connection):
        self.connection = connection
        self.statements = []
        self.cursors = []
        self.timeouts = []
        self.after_open = lambda cursor: None

    def open_inspection(self, statement, *, timeout_seconds):
        self.statements.append(statement.validated())
        self.timeouts.append(timeout_seconds)
        # SQLite has two-part names; preserve the generated SELECT and COUNT.
        sql = statement.sql.replace('"SYNTHETIC"."PUBLIC".', "").replace("%s", "?")
        cursor = _Cursor(self.connection.execute(sql, statement.parameters), statement)
        self.cursors.append(cursor)
        self.after_open(cursor)
        return cursor


@pytest.fixture
def source():
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.executescript(
            """
            CREATE TABLE root_records(root_npi TEXT, unselected TEXT);
            INSERT INTO root_records VALUES ('synthetic-root','private-record'), ('synthetic-root','private-record'), ('synthetic-other','private-record');
            CREATE TABLE detail_records(detail_npi TEXT, detail_id TEXT, unselected TEXT);
            INSERT INTO detail_records VALUES ('synthetic-root','one','private-record'), ('synthetic-root','one','private-record'), ('missing-parent','two','private-record'), ('synthetic-other','three','private-record');
            """
        )
        adapter = _Adapter(connection)
        loaded = _loaded_binding()
        builder = SnowflakeBundleStatementBuilder(approved_relations=loaded.approved_relations)

        def run(operation, *, limits=SnowflakePreflightLimits(), clock=lambda: 0):
            return inspect_snowflake_bundle(
                loaded.definition, loaded.binding, builder, adapter, operation=operation, limits=limits, clock=clock
            )

        yield adapter, run


def test_estimate_executes_counts_without_claiming_import_yield(source):
    adapter, run = source
    receipt = json.loads(run("estimate"))
    assert receipt["streams"] == [
        {"stream_id": "root_source", "source_rows": 3, "precision": "exact"},
        {"stream_id": "detail_source", "source_rows": 4, "precision": "exact"},
    ]
    assert receipt["scope"] == "approved_relations_at_one_statement"
    assert receipt["warehouse_scan_bounded"] is False
    assert all(value == {"precision": "unknown", "value": None} for value in receipt["estimates"].values())
    assert len(adapter.statements) == 1
    assert adapter.cursors[0].fetches == 3
    assert adapter.cursors[0].closed
    assert "snapshot" not in adapter.statements[0].sql.lower()
    assert "private-record" not in json.dumps(receipt)


def test_discovery_queries_selected_metadata_without_fetching_records(source):
    adapter, run = source
    receipt = json.loads(run("discover"))
    assert receipt["scope"] == "selected_fields_at_each_statement"
    assert [stream["stream_id"] for stream in receipt["streams"]] == ["root_source", "detail_source"]
    assert receipt["streams"][0]["selected_fields"] == [
        {"field_id": "npi", "source_type": "TEXT", "runtime_supported": True}
    ]
    assert all(cursor.fetches == 0 and cursor.closed for cursor in adapter.cursors)
    assert all(
        statement.sql.endswith("LIMIT 0") and "unselected" not in statement.sql for statement in adapter.statements
    )
    assert "private-record" not in json.dumps(receipt)


@pytest.mark.parametrize("operation", ["discover", "estimate"])
def test_shared_relation_preserves_stream_counts_and_logical_field_aliases(source, operation):
    adapter, _ = source
    loaded = _loaded_binding()
    definition_dict = json.loads(loaded.definition.canonical)
    definition_dict["aliases"]["detail_source"] = {"ROOT_NPI": "detail_npi", "DETAIL_ID": "detail_id"}
    definition = CustomImportDefinition.from_mapping(definition_dict)
    binding_dict = json.loads(loaded.binding.canonical)
    binding_dict["definition_sha256"] = definition.digest
    binding_dict["streams"][1]["relation"] = binding_dict["streams"][0]["relation"]
    binding_dict["streams"][1]["columns"][0]["column_identifier"] = "ROOT_NPI"
    binding = SnowflakeSourceBinding.from_mapping(binding_dict)
    approved_relations, _ = binding.bundle_components(definition)
    builder = SnowflakeBundleStatementBuilder(approved_relations=approved_relations)
    adapter.connection.execute("ALTER TABLE root_records ADD COLUMN detail_id TEXT DEFAULT 'synthetic-detail'")
    receipt = json.loads(inspect_snowflake_bundle(definition, binding, builder, adapter, operation=operation))
    assert [stream["stream_id"] for stream in receipt["streams"]] == ["root_source", "detail_source"]
    if operation == "estimate":
        assert [stream["source_rows"] for stream in receipt["streams"]] == [3, 3]
    else:
        assert [field["field_id"] for field in receipt["streams"][1]["selected_fields"]] == ["detail_npi", "detail_id"]
        assert all(cursor.fetches == 0 for cursor in adapter.cursors)
    assert all(cursor.closed for cursor in adapter.cursors)


def test_discovery_redacts_unsupported_type_without_claiming_compatibility(source):
    adapter, run = source
    adapter.after_open = lambda cursor: setattr(cursor.description[0], "type_name", "unsupported-sensitive-value")
    rendered = run("discover")
    field = json.loads(rendered)["streams"][0]["selected_fields"][0]
    assert field == {"field_id": "npi", "source_type": "unknown", "runtime_supported": False}
    assert "unsupported-sensitive-value" not in rendered


@pytest.mark.parametrize("operation", ["discover", "estimate"])
def test_filtered_shared_relation_keeps_each_stream_parameter_scope(request, operation):
    adapter, _ = request.getfixturevalue("source")
    loaded = _loaded_binding()
    definition_document = json.loads(loaded.definition.canonical)
    definition_document["aliases"]["detail_source"] = {"ROOT_NPI": "detail_npi", "DETAIL_ID": "detail_id"}
    definition = CustomImportDefinition.from_mapping(definition_document)
    binding_document = json.loads(loaded.binding.canonical)
    binding_document["definition_sha256"] = definition.digest
    binding_document["contract"] = "custom-import/source-binding/v2"
    binding_document["processing_policy"] = _policy_document()
    binding_document["streams"][1]["relation"] = binding_document["streams"][0]["relation"]
    binding_document["streams"][1]["columns"][0]["column_identifier"] = "ROOT_NPI"
    for stream, field_id in zip(binding_document["streams"], ("npi", "detail_npi"), strict=True):
        stream["row_filters"] = [{"field_id": field_id, "operator": "eq", "value": "synthetic-root"}]
    binding = SnowflakeSourceBinding.from_mapping(binding_document)
    approved_relations, _ = binding.bundle_components(definition)
    builder = SnowflakeBundleStatementBuilder(approved_relations=approved_relations)
    adapter.connection.execute("ALTER TABLE root_records ADD COLUMN detail_id TEXT DEFAULT 'synthetic-detail'")
    receipt = json.loads(inspect_snowflake_bundle(definition, binding, builder, adapter, operation=operation))
    assert "synthetic-root" not in json.dumps(receipt)
    assert all("synthetic-root" not in statement.sql for statement in adapter.statements)
    if operation == "estimate":
        statement = adapter.statements[0]
        assert [stream["source_rows"] for stream in receipt["streams"]] == [2, 2]
        assert statement.bundle_statement.parameters == ("synthetic-root",)
        assert statement.parameters == ("synthetic-root", "synthetic-root")
    else:
        assert all(statement.parameters == ("synthetic-root",) for statement in adapter.statements)
        assert all(cursor.fetches == 0 for cursor in adapter.cursors)
    assert all(cursor.closed for cursor in adapter.cursors)


@pytest.mark.parametrize("opted_in", [False, True])
@pytest.mark.parametrize("source_type", ["REAL", "FIXED"])
def test_discovery_matches_explicit_decimal_conversion_compatibility(opted_in, source_type):
    definition, binding = _decimal_binding(opted_in=opted_in)
    approved, _ = binding.bundle_components(definition)
    builder = SnowflakeBundleStatementBuilder(approved_relations=approved)
    cursors = []

    class Adapter:
        def open_inspection(self, statement, *, timeout_seconds):
            cursor = SimpleNamespace(
                description=tuple(
                    SimpleNamespace(
                        name=name, type_name=source_type if name == "score" else "TEXT", precision=38, scale=12
                    )
                    for name in statement.column_ids
                ),
                closed=False,
            )
            cursor.close = lambda: setattr(cursor, "closed", True)
            cursors.append(cursor)
            return cursor

    receipt = json.loads(inspect_snowflake_bundle(definition, binding, builder, Adapter(), operation="discover"))
    field = next(
        field for stream in receipt["streams"] for field in stream["selected_fields"] if field["field_id"] == "score"
    )
    assert field["runtime_supported"] is (opted_in == (source_type == "REAL"))
    assert all(cursor.closed for cursor in cursors)


@pytest.mark.parametrize("operation", ["discover", "estimate"])
def test_inspection_rejects_unexpected_actual_column_name(source, operation):
    adapter, run = source
    adapter.after_open = lambda cursor: setattr(cursor.description[0], "name", "sensitive-extra-column")
    with pytest.raises(SnowflakeInspectionError, match="^result_invalid$"):
        run(operation)
    assert all(cursor.closed for cursor in adapter.cursors)


@pytest.mark.parametrize(
    "rows", [[], [(0, True)], [(0, -1)], [(0, 1.0)], [(1, 2)], [(0, None)], [(0, 3), (0, 4)], [(0, 3), (1, 4), (2, 5)]]
)
def test_estimate_rejects_partial_extra_or_invalid_rows(source, rows):
    adapter, run = source

    def replace_rows(cursor):
        iterator = iter(rows)
        cursor.fetchone = lambda: next(iterator, None)

    adapter.after_open = replace_rows
    with pytest.raises(SnowflakeInspectionError, match="^result_invalid$"):
        run("estimate")
    assert all(cursor.closed for cursor in adapter.cursors)


@pytest.mark.parametrize(
    "attribute,value",
    [("type_name", "unsupported-sensitive-value"), ("type_name", None), ("precision", None), ("scale", None)],
)
@pytest.mark.parametrize("close_fails", [False, True])
def test_estimate_rejects_invalid_metadata_before_fetch_and_preserves_failure(source, attribute, value, close_fails):
    adapter, run = source

    def invalid_metadata(cursor):
        setattr(cursor.description[1], attribute, value)
        if close_fails:
            original_close = cursor.close

            def close():
                original_close()
                raise RuntimeError("sensitive-cleanup-detail")

            cursor.close = close

    adapter.after_open = invalid_metadata
    with pytest.raises(SnowflakeInspectionError, match="^result_invalid$") as exc:
        run("estimate")
    assert exc.value.code == "result_invalid"
    assert exc.value.__cause__ is None
    assert exc.value.__suppress_context__ is True
    assert adapter.cursors[0].fetches == 0
    assert adapter.cursors[0].closed


def test_empty_relations_are_exact_zero_not_unknown(source):
    adapter, run = source
    adapter.connection.execute("DELETE FROM root_records")
    adapter.connection.execute("DELETE FROM detail_records")
    assert [stream["source_rows"] for stream in json.loads(run("estimate"))["streams"]] == [0, 0]


@pytest.mark.parametrize("operation", ["discover", "estimate"])
def test_byte_limit_discards_result_and_closes_cursors(source, operation):
    adapter, run = source
    with pytest.raises(SnowflakeInspectionError, match="^byte_limit$"):
        run(operation, limits=SnowflakePreflightLimits(maximum_total_bytes=1))
    assert all(cursor.closed for cursor in adapter.cursors)


def test_discovery_shares_elapsed_budget_across_queries(source):
    adapter, run = source
    clock_values = [0]
    adapter.after_open = lambda cursor: clock_values.__setitem__(0, clock_values[0] + 1)
    run("discover", limits=SnowflakePreflightLimits(maximum_elapsed_seconds=3), clock=lambda: clock_values[0])
    assert adapter.timeouts == [3, 2]
    clock_values[0] = 0
    with pytest.raises(SnowflakeInspectionError, match="^query_timeout$"):
        run("discover", limits=SnowflakePreflightLimits(maximum_elapsed_seconds=1), clock=lambda: clock_values[0])
    assert all(cursor.closed for cursor in adapter.cursors)


def test_primary_cancellation_survives_cleanup_failure(source):
    adapter, run = source

    def cancellation(cursor):
        original_close = cursor.close

        def fetchone():
            raise KeyboardInterrupt("sensitive-driver-detail")

        def close():
            original_close()
            raise RuntimeError("sensitive-cleanup-detail")

        cursor.fetchone = fetchone
        cursor.close = close

    adapter.after_open = cancellation
    with pytest.raises(KeyboardInterrupt):
        run("estimate")
    assert adapter.cursors[0].closed


def test_source_failure_is_redacted_and_closes_cursor(source):
    adapter, run = source

    def failure(cursor):
        def fetchone():
            raise RuntimeError("sensitive-driver-detail")

        cursor.fetchone = fetchone

    adapter.after_open = failure
    with pytest.raises(SnowflakeInspectionError, match="^query_unavailable$"):
        run("estimate")
    assert adapter.cursors[0].closed


@pytest.mark.parametrize("invalid_argument", ["definition", "binding", "connector", "limits"])
def test_invalid_preparation_uses_closed_error_without_adapter_call(source, invalid_argument):
    adapter, _ = source
    loaded = _loaded_binding()
    inspection_arguments_dict = {
        "definition": loaded.definition,
        "binding": loaded.binding,
        "connector": SnowflakeBundleStatementBuilder(approved_relations=loaded.approved_relations),
        "adapter": adapter,
        "operation": "estimate",
        "limits": SnowflakePreflightLimits(),
    }
    inspection_arguments_dict[invalid_argument] = object()
    with pytest.raises(SnowflakeInspectionError, match="^result_invalid$") as exc:
        inspect_snowflake_bundle(**inspection_arguments_dict)
    assert exc.value.code == "result_invalid"
    assert exc.value.__cause__ is None
    assert exc.value.__suppress_context__ is True
    assert adapter.statements == adapter.cursors == []


def test_preparation_preserves_keyboard_interrupt_without_adapter_call(source, monkeypatch):
    adapter, run = source

    def interrupted(*arguments):
        raise KeyboardInterrupt("synthetic-interruption")

    monkeypatch.setattr(inspection, "_prepare_preflight", interrupted)
    with pytest.raises(KeyboardInterrupt):
        run("estimate")
    assert adapter.statements == adapter.cursors == []


@pytest.mark.parametrize("operation", ["discover", "estimate"])
def test_cli_dispatches_only_retained_ids_and_inspection_limits(monkeypatch, capsys, operation):
    captured_calls_dict = {}

    async def inspect(**arguments):
        captured_calls_dict.update(arguments)
        return '{"status":"complete"}'

    monkeypatch.setattr(operator_cli, "_preflight_retained_snowflake_binding", inspect)
    assert (
        operator_cli.run_command(
            [
                operation,
                "--definition-revision-id",
                "32",
                "--source-binding-revision-id",
                "34",
                "--maximum-total-bytes",
                "4096",
                "--maximum-elapsed-seconds",
                "4",
            ]
        )
        == 0
    )
    assert captured_calls_dict == {
        "definition_revision_id": 32,
        "source_binding_revision_id": 34,
        "include_sample": False,
        "operation": operation,
        "limits": SnowflakePreflightLimits(maximum_total_bytes=4096, maximum_elapsed_seconds=4),
    }
    assert capsys.readouterr().out == '{"status":"complete"}\n'


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["discover", "estimate"])
async def test_cli_loads_binding_read_only_and_cleans_up(source, monkeypatch, operation):
    _, run = source
    loaded = _loaded_binding()
    database = _Database()

    async def load(session, **arguments):
        assert session is database.read_session
        assert arguments == {"definition_revision_id": 32, "source_binding_revision_id": 34}
        return loaded

    def inspect(actual_binding, limits, *, operation):
        assert actual_binding == loaded
        return run(operation, limits=limits)

    monkeypatch.setattr(operator_cli, "load_snowflake_source_binding", load)
    monkeypatch.setattr(operator_cli, "_run_snowflake_preflight", inspect)
    rendered = await operator_cli._preflight_retained_snowflake_binding(
        definition_revision_id=32,
        source_binding_revision_id=34,
        limits=SnowflakePreflightLimits(),
        include_sample=False,
        operation=operation,
        database=database,
    )
    assert json.loads(rendered)["operation"] == operation
    assert database.connected == database.disconnected == 1
    assert database.read_session.begin_calls == 0


@pytest.mark.parametrize("operation", ["discover", "estimate"])
def test_cli_composes_inspection_with_fixed_credentials_and_approved_binding(source, monkeypatch, operation):
    _, run = source
    loaded = _loaded_binding()
    captured_calls_dict = {}

    def inspect(definition, binding, connector, adapter, **arguments):
        captured_calls_dict.update(arguments)
        assert definition == loaded.definition
        assert binding == loaded.binding
        assert connector.prepare_request(definition, bindings=loaded.bundle_bindings).bindings == loaded.bundle_bindings
        assert adapter.connector.role == loaded.binding.role
        assert adapter.connector.warehouse == loaded.binding.warehouse
        assert adapter.credential_provider.directory == operator_cli.FIXED_CREDENTIAL_DIRECTORY
        assert adapter.credential_provider.load_calls == 0
        return run(arguments["operation"], limits=arguments["limits"])

    monkeypatch.setattr(operator_cli, "FixedLocalKeyPairCredentialProvider", _CredentialProvider)
    monkeypatch.setattr(operator_cli, "SnowflakePythonConnectorAdapter", _SourceAdapter)
    monkeypatch.setattr(operator_cli, "SnowflakePythonPreflightAdapter", _PreflightAdapter)
    monkeypatch.setattr(operator_cli, "inspect_snowflake_bundle", inspect)
    limits = SnowflakePreflightLimits()
    rendered = operator_cli._run_snowflake_preflight(loaded, limits, operation=operation)
    assert json.loads(rendered)["operation"] == operation
    assert captured_calls_dict == {"operation": operation, "limits": limits}


@pytest.mark.parametrize("operation", ["discover", "estimate"])
@pytest.mark.parametrize(
    "extra",
    [
        ["--sql", "sensitive-input"],
        ["--include-sample"],
        ["--maximum-total-bytes", "16777217"],
        ["--maximum-elapsed-seconds", "121"],
    ],
)
def test_cli_rejects_raw_sql_sample_and_excessive_limits(capsys, operation, extra):
    with pytest.raises(SystemExit) as exc:
        operator_cli.run_command(
            [operation, "--definition-revision-id", "32", "--source-binding-revision-id", "34", *extra]
        )
    assert exc.value.code == 2
    output = capsys.readouterr()
    assert output.out == ""
    assert json.loads(output.err) == {"code": "invalid_arguments", "status": "error"}


def test_cli_redacts_inspection_failure(monkeypatch, capsys):
    async def failure(**arguments):
        raise SnowflakeInspectionError("sensitive-driver-detail")

    monkeypatch.setattr(operator_cli, "_preflight_retained_snowflake_binding", failure)
    assert (
        operator_cli.run_command(["estimate", "--definition-revision-id", "32", "--source-binding-revision-id", "34"])
        == 1
    )
    output = capsys.readouterr()
    assert output.out == ""
    assert json.loads(output.err) == {"code": "query_unavailable", "status": "error"}
