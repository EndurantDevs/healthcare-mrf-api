# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded source predicates, SQL scopes, and retained capture identities."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from process.custom_import.processing_policy import ProcessingPolicy
from process.custom_import.snowflake import SnowflakeConnectorError, SnowflakeRowFilter
from process.custom_import.snowflake_binding import SnowflakeSourceBinding, SnowflakeSourceBindingError
from process.custom_import.snowflake_bundle import (
    SnowflakeBundleError,
    prepare_bundle_replay,
    reconstruct_replayable_parquet_bundle,
    replayable_parquet_captures,
)
from process.custom_import.snowflake_preflight import (
    SnowflakePreflightLimits,
    SnowflakePreflightStatement,
    preflight_snowflake_bundle,
)
from process.custom_import.snowflake_python import _execute_filtered_statement
from tests.test_custom_import_processing_policy import _policy_document
from tests.test_custom_import_snowflake_landing import _open
from tests.test_custom_import_snowflake_preflight import _Adapter, _binding, _bundle_statement, _connector, _definition
from tests.test_custom_import_snowflake_shared_capture import _runtime, _shared_row


def _predicate(field_id="label", value="primary", **extra):
    return {"field_id": field_id, "operator": "eq", "value": value, **extra}


def _filtered_binding(filters_by_stream):
    definition = _definition()
    document = json.loads(_binding(definition).canonical)
    for stream in document["streams"]:
        if stream["stream_id"] in filters_by_stream:
            stream["row_filters"] = filters_by_stream[stream["stream_id"]]
    return definition, SnowflakeSourceBinding.from_mapping(document)


@pytest.mark.parametrize(
    "filters",
    [
        None,
        [],
        (),
        [_predicate()] * 2,
        [_predicate(str(index)) for index in range(4)],
        [_predicate(operator="gt")],
        [_predicate(sql="TRUE")],
        [{"field_id": "label", "value": "x"}],
        [_predicate(value=None)],
        [_predicate(value=True)],
        [_predicate(value=1)],
        [_predicate(value=1.5)],
        [_predicate(value=[])],
        [_predicate(value={})],
        [_predicate(value="nul\0value")],
        [_predicate(value="\ud800")],
        [_predicate(value="é" * 1025)],
        [_predicate(field_id="RAW_COLUMN")],
    ],
)
def test_source_filter_wire_rejects_invalid_or_ambiguous_input(filters):
    with pytest.raises(SnowflakeSourceBindingError):
        _filtered_binding({"root_source": filters})


@pytest.mark.parametrize("field_id", ["detail_id", "edition", "missing_field"])
def test_filter_scope_requires_a_declared_string_in_its_own_stream(field_id):
    definition, binding = _filtered_binding({"root_source": [_predicate(field_id)]})
    with pytest.raises(SnowflakeSourceBindingError, match="fixed bundle"):
        binding.bundle_components(definition)


@pytest.mark.parametrize(
    "filters", [[], (object(),), tuple(SnowflakeRowFilter(f"field_{index}", "eq", "x") for index in range(4))]
)
def test_in_memory_bindings_cannot_bypass_filter_validation(filters):
    definition = _definition()
    binding = _binding(definition)
    with pytest.raises(SnowflakeSourceBindingError, match="filters"):
        replace(binding.streams[0], row_filters=filters)
    _, bundle_bindings = binding.bundle_components(definition)
    with pytest.raises(SnowflakeBundleError, match="filters"):
        replace(bundle_bindings[0], row_filters=filters)


def test_filters_do_not_relax_the_root_only_query_identity_guard():
    definition, binding = _filtered_binding({"root_source": [_predicate()]})
    document = json.loads(binding.canonical)
    for stream in document["streams"]:
        stream["source_snapshot_token_relation"] = None
        stream["source_snapshot_token_column_identifier"] = None
    candidate = SnowflakeSourceBinding.from_mapping(document)
    with pytest.raises(SnowflakeSourceBindingError, match="one root"):
        candidate.bundle_components(definition)


@pytest.mark.parametrize("value", ["", "é" * 1024, "  Mixed Case  ", "x' OR TRUE --\\%s\n雪"])
def test_values_are_bound_without_normalization_or_sql_interpolation(value):
    definition, binding = _filtered_binding({"root_source": [_predicate(value=value)]})
    statement, request, _ = _bundle_statement(definition, binding)
    assert statement.parameters == (value,)
    assert 'FROM "SYNTHETIC"."PUBLIC"."ROOTS" WHERE "ROOT_LABEL" = %s' in statement.sql
    if value:
        assert value not in statement.sql
    assert request.bindings[0].row_filters == (SnowflakeRowFilter("label", "eq", value),)
    assert definition.fields_by_id["label"].projection_slot is None
    assert json.loads(statement.canonical_statement)["parameters"] == [value]
    assert SnowflakeSourceBinding.from_json(binding.canonical) == binding
    assert binding.digest != _binding(definition).digest


def test_filter_order_is_canonical_and_value_changes_invalidate_all_seals():
    filters = [_predicate("npi", "1003000126"), _predicate("label", "primary")]
    definition, first = _filtered_binding({"root_source": filters})
    _, reordered = _filtered_binding({"root_source": list(reversed(filters))})
    assert first == reordered
    root = next(stream for stream in json.loads(first.canonical)["streams"] if stream["stream_id"] == "root_source")
    assert [item["field_id"] for item in root["row_filters"]] == ["label", "npi"]
    first_statement, _, _ = _bundle_statement(definition, first)
    assert first_statement.parameters == ("primary", "1003000126")
    _, changed = _filtered_binding({"root_source": [_predicate("npi", "1003000126"), _predicate(value="secondary")]})
    changed_statement, _, _ = _bundle_statement(definition, changed)
    assert changed.digest != first.digest
    assert changed_statement.request.request_sha256 != first_statement.request.request_sha256
    assert changed_statement.statement_sha256 != first_statement.statement_sha256
    assert changed_statement.sql == first_statement.sql


_UNFILTERED_HASHES = (
    (
        "02e19ff9cc6bd8610d2846aa8e1e7161fbb9d7346e0e99b744d85240db70e818",
        "ee7527976e04efdc58e3daa3ca1d11432d8403816383ac2d4fb69b13f827f068",
        "203e25d2efe4b01d6f29ba53ec8623f51dfd2f55cea33e2831f743b9e00606db",
        "ea8cbf9e0efa2b379544d29c6625e0d631ad1d7885e65c529887386c91f974b4",
        "ed840d07dd579c8b216516d669aa929543648c638e292a7e4810f710eedcbdf1",
    ),
    (
        "4c4f5ca108f09ce49f378655ca3c431bc72e6d5d63dbf0493c28a200d25ca685",
        "701956b6fac354cea5e5f9548373c8abf0187714eaf01060473b4d5af5fd09b0",
        "6ea8293d6fb838b0bba6e9d5af7043b88691c4265310828d14c61e51fe40e646",
        "31b0fa57b6b2f964727005be00c2efbc7ebe7fc9ab8225c101971ad53ed5236d",
        "be33f538ec9db1aa35101361fc38329988707a82abd955a575f3e3fdfaa11d52",
    ),
)


@pytest.mark.parametrize("policy", [False, True])
def test_absent_filters_preserve_legacy_and_policy_bytes_and_execute_signature(policy):
    definition = _definition()
    binding = _binding(definition)
    if policy:
        binding = replace(binding, processing_policy=ProcessingPolicy.from_mapping(_policy_document()))
    statement, request, _ = _bundle_statement(definition, binding)
    preview = SnowflakePreflightStatement(statement, SnowflakePreflightLimits())
    documents = binding.canonical, request.canonical_request, statement.canonical_statement, statement.sql, preview.sql
    assert tuple(hashlib.sha256(value.encode()).hexdigest() for value in documents) == _UNFILTERED_HASHES[int(policy)]
    assert statement.parameters == preview.parameters == ()
    cursor = SimpleNamespace(execute=Mock())
    for generated in (statement, preview):
        _execute_filtered_statement(cursor, generated)
        cursor.execute.assert_called_with(generated.sql)


@pytest.mark.parametrize("same_scope", [False, True])
@pytest.mark.parametrize("tamper", ["parameters", "predicate"])
def test_shared_relations_share_only_identical_physical_predicates_and_replay(monkeypatch, same_scope, tamper):
    root_npi, child_npi = "1003000126", "1003000126" if same_scope else "1234567893"
    source_rows = (
        (_shared_row(root_npi),)
        if same_scope
        else (
            (1, 1, "root_source", None, root_npi, Decimal("1"), True, None, None, None),
            (1, 2, "detail_source", None, None, None, None, child_npi, "b", Decimal("2")),
        )
    )
    connector, request, _adapter, cursor, connection = _runtime(monkeypatch, source_rows)
    request = replace(
        request,
        bindings=tuple(
            replace(binding, row_filters=(SnowflakeRowFilter(field_id, "eq", filter_value),))
            for binding, field_id, filter_value in zip(
                request.bindings, ("npi", "detail_npi"), (root_npi, child_npi), strict=True
            )
        ),
    )
    if not same_scope:
        descriptions = list(cursor.description)
        descriptions[7] = SimpleNamespace(name="detail_npi", type_name="TEXT", is_nullable=True)
        descriptions[9] = SimpleNamespace(name="amount", type_name="FIXED", precision=30, scale=12, is_nullable=True)
        cursor.description = tuple(descriptions)
    cursor.execute = Mock()
    acquisition = connector.acquire(request)
    replay = prepare_bundle_replay(acquisition)
    statement = acquisition.statement
    assert statement.parameters == ((root_npi,) if same_scope else (root_npi, child_npi))
    assert statement.sql.count('FROM "SYNTHETIC"."PUBLIC"."ROOTS"') == (1 if same_scope else 2)
    cursor.execute.assert_called_once_with(statement.sql, statement.parameters)
    assert [root_row["npi"] for root_row in replay.roots] == [root_npi]
    assert [child_row["detail_npi"] for child_row in replay.children_by_collection["details"]] == [child_npi]
    captures = replayable_parquet_captures(acquisition)
    durable_replay = reconstruct_replayable_parquet_bundle(statement, captures)
    assert (
        durable_replay.roots == replay.roots and durable_replay.children_by_collection == replay.children_by_collection
    )
    changed_root = replace(request.bindings[0], row_filters=(SnowflakeRowFilter("npi", "eq", "other"),))
    changed_statement = connector.build_statement(replace(request, bindings=(changed_root, request.bindings[1])))
    with pytest.raises(SnowflakeBundleError, match="configured request"):
        reconstruct_replayable_parquet_bundle(changed_statement, captures)
    assert cursor.closed and connection.closed
    if tamper == "parameters":
        object.__setattr__(statement, "parameters", ("changed",))
    else:
        object.__setattr__(statement.request.bindings[0].row_filters[0], "value", "changed")
    with pytest.raises(SnowflakeBundleError, match="seal"):
        prepare_bundle_replay(acquisition)


def test_landing_adapter_binds_filters_and_closes_resources(monkeypatch):
    connector, request, adapter, cursor, connection = _runtime(monkeypatch, (_shared_row(),))
    request = replace(
        request,
        bindings=tuple(
            replace(binding, row_filters=(SnowflakeRowFilter(field_id, "eq", "1003000126"),))
            for binding, field_id in zip(request.bindings, ("npi", "detail_npi"), strict=True)
        ),
    )
    monkeypatch.setattr(adapter, "_connect", lambda _credentials, **_options: (connection, cursor))
    cursor.execute = Mock()
    statement = connector.build_statement(request)
    with _open(adapter, statement) as landing:
        events = list(landing.consume_events())
    assert len(events) == 4
    cursor.execute.assert_called_once_with(statement.sql, ("1003000126",))
    assert cursor.closed and connection.closed


@pytest.mark.parametrize("preview", [False, True])
@pytest.mark.parametrize("attribute,value", [("parameters", ()), ("parameters", ("changed",)), ("sql", "SELECT 1")])
def test_tampered_statements_cannot_execute(preview, attribute, value):
    definition, binding = _filtered_binding({"root_source": [_predicate()]})
    statement = _bundle_statement(definition, binding)[0]
    if preview:
        statement = SnowflakePreflightStatement(statement, SnowflakePreflightLimits())
    object.__setattr__(statement, attribute, value)
    cursor = SimpleNamespace(execute=Mock())
    with pytest.raises(SnowflakeConnectorError, match="seal"):
        _execute_filtered_statement(cursor, statement)
    cursor.execute.assert_not_called()


def _populate_preview_source(connection):
    connection.create_function("TO_VARCHAR", 1, str)
    connection.create_function("OCTET_LENGTH", 1, lambda value: len(value.encode()))
    roots = [
        ("0000000000", 1, "secondary"),
        ("1003000126", 1, "primary"),
        ("1003000126", 1, "secondary"),
        ("1234567893", 1, None),
    ]
    connection.execute("CREATE TABLE ROOTS (ROOT_NPI TEXT, ROOT_EDITION INTEGER, ROOT_LABEL TEXT)")
    connection.executemany("INSERT INTO ROOTS VALUES (?, ?, ?)", roots)
    for name, prefix in (("DETAILS", "DETAIL"), ("NOTES", "NOTE")):
        connection.execute(f"CREATE TABLE {name} ({prefix}_NPI TEXT, {prefix}_EDITION INTEGER, {prefix}_ID TEXT)")
        connection.executemany(
            f"INSERT INTO {name} VALUES (?, ?, ?)",
            [
                ("1003000126", 1, "keep"),
                ("1003000126", 1, "drop_a"),
                ("1003000126", 1, "drop_b"),
                ("1003000126", 1, None),
            ],
        )
    for prefix in ("ROOT", "DETAIL", "NOTE"):
        connection.execute(f"CREATE TABLE {prefix}_TOKENS ({prefix}_TOKEN TEXT)")
        connection.execute(f"INSERT INTO {prefix}_TOKENS VALUES (?)", ("synthetic-snapshot",))


@pytest.mark.parametrize("scope", ["none", "root", "all"])
def test_generated_preview_scopes_precede_root_multiplicity_and_child_sentinels(scope):
    filters_by_stream = {
        "root_source": [_predicate()],
        "detail_source": [_predicate("detail_id", "keep")],
        "note_source": [_predicate("note_id", "keep")],
    }
    selected = (
        {}
        if scope == "none"
        else {"root_source": filters_by_stream["root_source"]}
        if scope == "root"
        else filters_by_stream
    )
    definition, binding = _filtered_binding(selected)
    connector = _connector(definition, binding)
    with sqlite3.connect(":memory:") as connection:
        _populate_preview_source(connection)

        def rows(statement):
            sql = statement.sql.replace('"SYNTHETIC"."PUBLIC".', "").replace("%s", "?")
            return tuple(connection.execute(sql, statement.parameters))

        adapter = _Adapter(rows)
        preview_result = preflight_snowflake_bundle(
            definition,
            binding,
            connector,
            adapter,
            limits=SnowflakePreflightLimits(maximum_root_keys=1, maximum_child_rows=1),
        )
    if scope == "all":
        assert preview_result.status == "complete" and preview_result.sample is not None
        assert len(preview_result.sample.families) == 1
        assert [observation.observed_rows for observation in preview_result.observations] == [1, 1, 1]
        assert adapter.calls[0][0].parameters == ("primary", "primary", "keep", "keep")
    elif scope == "root":
        assert preview_result.status == "unavailable" and preview_result.unavailable_reason == "child_limit_reached"
        assert preview_result.sample is None
    else:
        assert preview_result.status == "unavailable" or preview_result.sample is None
