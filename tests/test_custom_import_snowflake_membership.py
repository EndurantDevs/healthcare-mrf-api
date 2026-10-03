# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded membership preserves source scope, ordering, and retained identities."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.custom_import import snowflake_capture as capture
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.processing_policy import ProcessingPolicy
from process.custom_import.snowflake import SnowflakeConnectorError, SnowflakeRowFilter
from process.custom_import.snowflake_binding import SnowflakeSourceBinding, SnowflakeSourceBindingError
from process.custom_import.snowflake_bundle import (
    SnowflakeBundleError,
    prepare_bundle_replay,
    reconstruct_replayable_parquet_bundle,
    replayable_parquet_captures,
)
from process.custom_import.snowflake_candidate import bundle_request_identity_sha256
from process.custom_import.snowflake_operator_cli import _retained_bundle_request
from process.custom_import.snowflake_preflight import (
    SnowflakePreflightLimits,
    SnowflakePreflightStatement,
    _prepare_preflight,
)
from process.custom_import.snowflake_preflight_schema import FLOAT_DECIMAL_CONVERSION
from tests.test_custom_import_processing_policy import _policy_document
from tests.test_custom_import_snowflake_capture import _Harness
from tests.test_custom_import_snowflake_composition import _grouped_source
from tests.test_custom_import_snowflake_preflight import _bundle_statement
from tests.test_custom_import_snowflake_row_filters import _filtered_binding, _populate_preview_source, _predicate
from tests.test_custom_import_snowflake_shared_capture import _runtime, _shared_row

_SCALAR_ORDER = ("", " a ", "Z", "a", "é", "\ue000", "\U00010000", "😀")
_EQ_HASHES = (
    (
        "b7bc487c6b2ce64ca6f7ca9ba69e86527054bcc5f54596f4f4c0724ed7159edb",
        "2a248521599b491a97f7ab97d2247a2d6c0694aac236db58f897809e97f932f1",
        "bd4cac4c771c4ba5e92b897eda59c810bccc56ac5906810d56abd8f9830d0ca2",
        "5a565683b16b92a47ab082758edb56124b7d2b0ae6750b1e30d2b3dd0380ebf7",
        "085b9da5fa12d5917312bd08ce2b5a0814bbec6bb1c81dc6e8078d40c6cf130c",
    ),
    (
        "1d5681572ceada0825bf1140d4d9ac2fa73867ff0498480ae39d9bfe1e5b4eb8",
        "f3a68063ea3147862f4d2ad534a4d9a71e340cfad6ed29499773c9245e1ba89a",
        "3db231740112107be2056d2eed4a1d7dd7a2b278e53e9948edbfe72a440b080b",
        "f66c29f45cd822767c55effbb022be52c712d2a384122e664f54b25f706cf0cb",
        "b493a4a55dca1349b4268a0ca42a800a332889142022f7f78ac054d8482d3229",
    ),
)


@pytest.mark.parametrize("policy", [False, True])
def test_eq_canonical_documents_sql_and_parameters_remain_identical(policy):
    definition, binding = _filtered_binding({"root_source": [_predicate("npi", "1003000126"), _predicate()]})
    if policy:
        binding = replace(binding, processing_policy=ProcessingPolicy.from_mapping(_policy_document()))
    statement, request, _ = _bundle_statement(definition, binding)
    preview = SnowflakePreflightStatement(statement, SnowflakePreflightLimits())
    documents = binding.canonical, request.canonical_request, statement.canonical_statement, statement.sql, preview.sql
    assert tuple(hashlib.sha256(value.encode()).hexdigest() for value in documents) == _EQ_HASHES[int(policy)]
    assert statement.parameters == ("primary", "1003000126")
    assert preview.parameters == statement.parameters * 2


@pytest.mark.parametrize(
    "values",
    [
        None,
        "x",
        [],
        (),
        ("x",),
        ["x"] * 2,
        list("123456789"),
        [None],
        [False],
        [1],
        [[]],
        [{}],
        ["nul\0value"],
        ["\ud800"],
        ["\udfff"],
        ["é" * 1024, "x"],
    ],
)
def test_membership_wire_rejects_invalid_operands(values):
    with pytest.raises(SnowflakeSourceBindingError):
        _filtered_binding({"root_source": [_predicate(operator="in", value=values)]})


def test_membership_scalar_order_roundtrip_bounds_and_frozen_operands():
    values = ["\U00010000", "\ue000", "a", "😀", "é", "Z", "", " a "]
    definition, binding = _filtered_binding({"root_source": [_predicate(operator="in", value=values)]})
    _, reordered = _filtered_binding({"root_source": [_predicate(operator="in", value=list(reversed(values)))]})
    assert binding == reordered == SnowflakeSourceBinding.from_json(binding.canonical)
    statement, _, _ = _bundle_statement(definition, binding)
    assert statement.parameters == _SCALAR_ORDER == tuple(sorted(values, key=lambda value: value.encode("utf-8")))
    predicate = statement.request.bindings[0].row_filters[0]
    assert predicate.value == _SCALAR_ORDER
    assert predicate.to_mapping()["value"] == list(_SCALAR_ORDER)
    values.clear()
    assert predicate.value == _SCALAR_ORDER
    assert SnowflakeRowFilter("label", "in", ["é" * 1023, "xx"]).value == ("xx", "é" * 1023)
    assert SnowflakeRowFilter("label", "in", [""]).value == ("",)
    with pytest.raises(SnowflakeConnectorError):
        SnowflakeRowFilter("label", "in", ("x", "x"))


@pytest.mark.parametrize("version", [1, 2])
def test_membership_preserves_grouped_statement_decimal_options_and_changes_all_identities(version):
    loaded, builder = _grouped_source(version)
    source_binding = loaded.binding
    memberships = tuple(
        replace(stream, row_filters=(SnowflakeRowFilter(stream.row_filters[0].field_id, "in", ["z", "a"]),))
        for stream in source_binding.streams
    )
    loaded.binding = replace(source_binding, streams=memberships)
    _, loaded.bundle_bindings = loaded.binding.bundle_components(loaded.definition)
    request = _retained_bundle_request(builder, loaded)
    statement = builder.build_statement(request)
    preview = _prepare_preflight(loaded.definition, loaded.binding, builder, SnowflakePreflightLimits()).statement
    assert preview.bundle_statement == statement
    assert request.snapshot_token_mode == "statement_query_id" and request.decimal_conversions is not None
    assert request.definition.query.entity_selection == loaded.definition.query.entity_selection
    assert statement.parameters == ("a", "z", "a", "z")
    assert preview.parameters == ("a", "z") * 3
    assert 'WHERE "DISPLAY_NAME" IN (%s, %s) GROUP BY' in preview.sql
    document = json.loads(loaded.binding.canonical)
    document["streams"][0]["row_filters"][0]["value"] = ["a", "y"]
    changed = SnowflakeSourceBinding.from_mapping(document)
    _, bindings = changed.bundle_components(loaded.definition)
    changed_request = replace(request, bindings=bindings)
    changed_statement = builder.build_statement(changed_request)
    assert changed.digest != loaded.binding.digest
    assert changed_request.request_sha256 != request.request_sha256
    assert changed_statement.statement_sha256 != statement.statement_sha256
    assert changed_statement.sql == statement.sql
    assert bundle_request_identity_sha256(request, statement) != bundle_request_identity_sha256(
        changed_request, changed_statement
    )
    if request.processing_policy is not None:
        policy = request.processing_policy.capture
        assert capture.segmented_bundle_request_identity_sha256(request, statement, policy) != (
            capture.segmented_bundle_request_identity_sha256(changed_request, changed_statement, policy)
        )


@pytest.mark.parametrize("policy", [False, True])
@pytest.mark.parametrize("scope", ["reordered", "different", "eq"])
def test_physical_scan_identity_requires_same_operator_and_operands(monkeypatch, policy, scope):
    connector, request, _, _, _ = _runtime(monkeypatch)
    if not policy:
        request = replace(request, processing_policy=None)
    root = SnowflakeRowFilter("npi", "in", ["z", "a"] if scope != "eq" else ["a"])
    child = SnowflakeRowFilter(
        "detail_npi",
        "eq" if scope == "eq" else "in",
        "a" if scope == "eq" else ["a", "z" if scope == "reordered" else "y"],
    )
    request = replace(
        request,
        bindings=tuple(
            replace(binding, row_filters=(predicate,))
            for binding, predicate in zip(request.bindings, (root, child), strict=True)
        ),
    )
    statement = connector.build_statement(request)
    shared = policy and scope == "reordered"
    assert statement.sql.count('FROM "SYNTHETIC"."PUBLIC"."ROOTS"') == (1 if shared else 2)
    assert statement.parameters == (
        ("a", "z") if shared else ("a", "a") if scope == "eq" else ("a", "z", "a", "z" if scope == "reordered" else "y")
    )
    assert ('"ROOT_NPI" AS "detail_npi"' in statement.sql) is (not shared)
    with pytest.raises(SnowflakeBundleError, match="inconsistent"):
        connector.build_statement(replace(request, decimal_conversions={"score": FLOAT_DECIMAL_CONVERSION}))


def test_membership_parameters_follow_physical_columns_and_keep_filter_guards():
    definition, binding = _filtered_binding(
        {"root_source": [_predicate("label", ["z", "a"], operator="in"), _predicate("npi", ["2", "1"], operator="in")]}
    )
    document = json.loads(binding.canonical)
    for column in document["streams"][0]["columns"]:
        column["column_identifier"] = {"label": "Z_LABEL", "npi": "A_NPI"}.get(
            column["field_id"], column["column_identifier"]
        )
    binding = SnowflakeSourceBinding.from_mapping(document)
    with pytest.raises(SnowflakeSourceBindingError, match="aliases"):
        binding.bundle_components(definition)
    definition_document = json.loads(definition.canonical)
    aliases = definition_document["aliases"]["root_source"]
    aliases["A_NPI"] = aliases.pop("ROOT_NPI")
    aliases["Z_LABEL"] = aliases.pop("ROOT_LABEL")
    definition = CustomImportDefinition.from_mapping(definition_document)
    binding = replace(binding, definition_sha256=definition.digest, schema_sha256=definition.schema_digest)
    statement, _, _ = _bundle_statement(definition, binding)
    preview = SnowflakePreflightStatement(statement, SnowflakePreflightLimits())
    assert statement.parameters == ("1", "2", "a", "z")
    assert preview.parameters == statement.parameters * 2
    assert 'WHERE "A_NPI" IN (%s, %s) AND "Z_LABEL" IN (%s, %s)' in statement.sql
    for filters in (
        [_predicate(operator="in", value=["a"], extra=True)],
        [_predicate(operator="in", value=["a"])] * 2,
        [_predicate(str(index), ["a"], operator="in") for index in range(4)],
    ):
        with pytest.raises(SnowflakeSourceBindingError):
            _filtered_binding({"root_source": filters})
    for field_id in ("edition", "detail_id", "missing"):
        definition, binding = _filtered_binding({"root_source": [_predicate(field_id, ["a"], operator="in")]})
        with pytest.raises(SnowflakeSourceBindingError, match="fixed bundle"):
            binding.bundle_components(definition)


def test_membership_query_binds_injection_and_selects_only_five_quality_outcomes():
    injection = "x' OR TRUE --\\%s\n雪"
    outcomes = ["quality_a", "quality_b", "quality_c", "quality_d", injection]
    definition, binding = _filtered_binding(
        {
            "root_source": [_predicate(value=["secondary", "primary"], operator="in"), _predicate("npi", "1003000126")],
            "detail_source": [_predicate("detail_id", outcomes, operator="in")],
            "note_source": [_predicate("note_id", ["keep"], operator="in")],
        }
    )
    statement, _, _ = _bundle_statement(definition, binding)
    preview = SnowflakePreflightStatement(
        statement, SnowflakePreflightLimits(maximum_root_keys=1, maximum_child_rows=5)
    )
    root_parameters = ("primary", "secondary", "1003000126")
    assert statement.parameters == (*root_parameters, *sorted(outcomes), "keep")
    assert preview.parameters == (*root_parameters, *statement.parameters)
    assert injection not in statement.sql + preview.sql
    with sqlite3.connect(":memory:") as connection:
        _populate_preview_source(connection)
        connection.execute("DELETE FROM ROOTS WHERE ROOT_LABEL = 'secondary'")
        connection.execute("DELETE FROM DETAILS")
        connection.executemany(
            "INSERT INTO DETAILS VALUES (?, ?, ?)",
            [("1003000126", 1, outcome) for outcome in [*outcomes, "payment_metric", None]],
        )
        preview_records = tuple(
            connection.execute(preview.sql.replace('"SYNTHETIC"."PUBLIC".', "").replace("%s", "?"), preview.parameters)
        )
    detail_id_index = preview.column_ids.index("detail_id")
    assert sorted(
        preview_record[detail_id_index]
        for preview_record in preview_records
        if preview_record[0] == 2 and preview_record[2] == "detail_source"
    ) == sorted(outcomes)
    assert len([preview_record for preview_record in preview_records if preview_record[0] == 1]) == 1


@pytest.mark.parametrize("tamper", ["parameters", "predicate"])
def test_membership_durable_replay_rejects_stale_scope_without_source(monkeypatch, tamper):
    connector, request, _, cursor, _ = _runtime(monkeypatch, (_shared_row(),))
    request = replace(
        request,
        bindings=tuple(
            replace(binding, row_filters=(SnowflakeRowFilter(field_id, "in", ["1003000126", "1234567893"]),))
            for binding, field_id in zip(request.bindings, ("npi", "detail_npi"), strict=True)
        ),
    )
    cursor.execute = Mock()
    acquired = connector.acquire(request)
    retained = replayable_parquet_captures(acquired)
    cursor.execute = Mock(side_effect=AssertionError("replay cannot access source"))
    assert reconstruct_replayable_parquet_bundle(acquired.statement, retained) == prepare_bundle_replay(acquired)
    changed = replace(request.bindings[0], row_filters=(SnowflakeRowFilter("npi", "in", ["other"]),))
    with pytest.raises(SnowflakeBundleError, match="configured request"):
        reconstruct_replayable_parquet_bundle(
            connector.build_statement(replace(request, bindings=(changed, request.bindings[1]))), retained
        )
    if tamper == "parameters":
        object.__setattr__(acquired.statement, "parameters", ("other",))
    else:
        object.__setattr__(acquired.statement.request.bindings[0].row_filters[0], "value", ("other",))
    with pytest.raises(SnowflakeBundleError, match="seal"):
        prepare_bundle_replay(acquired)
    cursor.execute.assert_not_called()


async def test_membership_segmented_bound_replay_rejects_changed_scope_without_source(monkeypatch):
    harness = _Harness(monkeypatch, (_shared_row(),), snapshot_token_mode="statement_query_id")
    request = replace(
        harness.request.bundle_request,
        bindings=tuple(
            replace(binding, row_filters=(SnowflakeRowFilter(field_id, "in", ["1003000126", "1234567893"]),))
            for binding, field_id in zip(harness.request.bundle_request.bindings, ("npi", "detail_npi"), strict=True)
        ),
    )
    harness.request = replace(harness.request, bundle_request=request)
    harness.cursor.execute = Mock()
    assert (await harness.run()).status == "capture_sealed"
    statement = harness.builder.build_statement(request)
    harness.cursor.execute.assert_called_once_with(statement.sql, ("1003000126", "1234567893"))
    harness.submission = replace(harness.submission, capture_bundle_id=23)
    harness.bound = SimpleNamespace(
        dataset_id=1,
        definition_revision_id=2,
        schema_revision_id=3,
        capture_state="sealed",
        payload_contract=capture.SEGMENTED_PAYLOAD_CONTRACT,
        canonical_policy=harness.policy.canonical,
        source_binding_revision_id=None,
        producing_execution_id=9,
        source_binding_sha256=None,
        request_identity_sha256=harness.identity,
        policy_sha256=bytes.fromhex(harness.policy.digest),
        source_request_sha256=bytes.fromhex(request.request_sha256),
        statement_sha256=bytes.fromhex(statement.statement_sha256),
        snapshot_token=harness.pending.source_snapshot_token,
        capture_bundle_id=23,
    )
    monkeypatch.setattr(
        capture,
        "_load_parquet_part_metadata",
        AsyncMock(return_value=({1: harness.receipts[0], 2: harness.receipts[1]}, {}, None)),
    )
    harness.cursor.execute.reset_mock(side_effect=True)
    harness.cursor.execute.side_effect = AssertionError("bound replay cannot access source")
    monkeypatch.setattr(harness.credentials, "load_key_pair", Mock(side_effect=AssertionError("no credentials")))
    assert (await harness.run()).status == "capture_bound"
    changed = replace(request.bindings[0], row_filters=(SnowflakeRowFilter("npi", "in", ["other"]),))
    harness.request = replace(harness.request, bundle_request=replace(request, bindings=(changed, request.bindings[1])))
    with pytest.raises(capture.SnowflakeCaptureError, match="bound capture identity"):
        await harness.run()
    harness.cursor.execute.assert_not_called()
