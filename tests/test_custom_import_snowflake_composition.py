# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Source options remain one sealed contract with grouped-family definitions."""

from __future__ import annotations

import json
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.custom_import import snowflake_capture
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.snowflake import SnowflakeRowFilter
from process.custom_import.snowflake_binding import SnowflakeSourceBinding
from process.custom_import.snowflake_bundle import (
    SnowflakeBundleError,
    SnowflakeBundleStatementBuilder,
    prepare_bundle_replay,
    reconstruct_replayable_parquet_bundle,
    replayable_parquet_captures,
)
from process.custom_import.snowflake_bundle_replay import _rebuilt_bundle_statement
from process.custom_import.snowflake_operator_cli import _retained_bundle_request
from process.custom_import.snowflake_preflight import SnowflakePreflightLimits, _prepare_preflight
from tests.custom_import_grouped_support import definition_document
from tests.test_custom_import_processing_policy import _policy_document
from tests.test_custom_import_snowflake_capture import _Harness
from tests.test_custom_import_snowflake_decimal_capture import _converted_runtime, _real_metadata
from tests.test_custom_import_snowflake_decimal_conversions import _CONVERSIONS
from tests.test_custom_import_snowflake_shared_capture import _shared_row
from tests.test_custom_import_snowflake_source_binding import _binding_document
from tests.test_custom_import_snowflake_statement_snapshot import _reseal_receipt


def _grouped_source(version):
    document = definition_document()
    for stream in document["streams"]:
        stream.update(format="parquet", compression="none")
    document["aliases"] = {stream["id"]: {} for stream in document["streams"]}
    definition = CustomImportDefinition.from_mapping(document)
    document = _binding_document(definition)
    document["snapshot_token_mode"] = "statement_query_id"
    document["decimal_conversions"] = _CONVERSIONS
    if version == 2:
        document.update(contract="custom-import/source-binding/v2", processing_policy=_policy_document())
    document["streams"] = [
        {
            "stream_id": stream.stream_id,
            "relation": ["SYNTHETIC", "PUBLIC", stream.stream_id.upper()],
            "source_snapshot_token_relation": None,
            "source_snapshot_token_column_identifier": None,
            "semantic_token_metadata_key": stream.snapshot_token,
            "columns": [
                {"field_id": field.field_id, "column_identifier": field.field_id.upper()}
                for field in definition.fields
                if field.collection == stream.child_collection
            ],
            "row_filters": [{"field_id": field_id, "operator": "eq", "value": "primary"}],
        }
        for stream, field_id in zip(definition.source_streams, ("display_name", "service_code"), strict=True)
    ]
    binding = SnowflakeSourceBinding.from_mapping(document)
    approved, bindings = binding.bundle_components(definition)
    builder = SnowflakeBundleStatementBuilder(approved_relations=approved)
    loaded = SimpleNamespace(definition=definition, binding=binding, bundle_bindings=bindings)
    return loaded, builder


@pytest.mark.parametrize("version", [1, 2])
def test_grouped_source_options_survive_preview_cli_and_replay(version):
    loaded, builder = _grouped_source(version)
    request = _retained_bundle_request(builder, loaded)
    statement = builder.build_statement(request)
    prepared = _prepare_preflight(loaded.definition, loaded.binding, builder, SnowflakePreflightLimits())
    assert SnowflakeSourceBinding.from_json(loaded.binding.canonical) == loaded.binding
    assert _rebuilt_bundle_statement(statement) == (request, statement)
    assert prepared.statement.bundle_statement == statement
    assert request.definition.query.entity_selection == loaded.definition.query.entity_selection
    assert request.snapshot_token_mode == "statement_query_id"
    assert request.decimal_conversions == _CONVERSIONS
    assert statement.parameters == ("primary", "primary")
    assert prepared.statement.parameters == ("primary", "primary", "primary")
    assert json.loads(request.canonical_request)["bindings"][0]["row_filters"] == [
        {"field_id": "display_name", "operator": "eq", "value": "primary"}
    ]
    assert 'FROM "SYNTHETIC"."PUBLIC"."PROVIDERS" WHERE "DISPLAY_NAME" = %s GROUP BY' in prepared.statement.sql
    with pytest.raises(SnowflakeBundleError, match="one root"):
        replace(request, snapshot_token_mode=None)
    changed = replace(request.bindings[0], row_filters=(replace(request.bindings[0].row_filters[0], value="other"),))
    changed_statement = builder.build_statement(replace(request, bindings=(changed, request.bindings[1])))
    assert changed_statement.sql == statement.sql
    assert changed_statement.request.request_sha256 != request.request_sha256
    assert changed_statement.statement_sha256 != statement.statement_sha256


def _scoped_request(request, same_scope=True):
    return replace(
        request,
        bindings=tuple(
            replace(binding, row_filters=(SnowflakeRowFilter(field_id, "eq", value),))
            for binding, field_id, value in zip(
                request.bindings,
                ("npi", "detail_npi"),
                ("1003000126", "1003000126" if same_scope else "1234567893"),
                strict=True,
            )
        ),
    )


@pytest.mark.parametrize("same_scope", [False, True])
@pytest.mark.parametrize("empty", [False, True])
def test_statement_identity_retains_scoped_converted_streams(monkeypatch, same_scope, empty):
    source_rows = (
        (_shared_row(amount=0.1),)
        if same_scope
        else (
            (1, 1, "root_source", None, "1003000126", 0.1, True, None, None, None),
            (1, 2, "detail_source", None, None, None, None, "1234567893", "b", 0.2),
        )
    )
    connector, request, _, cursor, connection = _converted_runtime(
        monkeypatch, () if empty else source_rows, snapshot_token_mode="statement_query_id"
    )
    request = _scoped_request(request, same_scope)
    if not same_scope:
        metadata = list(cursor.description)
        metadata[7] = SimpleNamespace(name="detail_npi", type_name="TEXT", is_nullable=True)
        cursor.description = tuple(metadata)
    cursor.execute = Mock()
    acquired = connector.acquire(request)
    replay = prepare_bundle_replay(acquired)
    retained = replayable_parquet_captures(acquired)
    assert reconstruct_replayable_parquet_bundle(acquired.statement, retained) == replay
    assert acquired.source_snapshot_token == f"snowflake-query:{cursor.sfqid}"
    assert acquired.statement.parameters == (("1003000126",) if same_scope else ("1003000126", "1234567893"))
    assert acquired.statement.sql.count('FROM "SYNTHETIC"."PUBLIC"."ROOTS"') == (1 if same_scope else 2)
    cursor.execute.assert_called_once_with(acquired.statement.sql, acquired.statement.parameters)
    if empty:
        assert not replay.roots and not replay.children_by_collection["details"]
    else:
        assert replay.roots[0]["score"] == Decimal("0.1")
        assert replay.children_by_collection["details"][0]["amount"] == Decimal("0.1" if same_scope else "0.2")
    for changed in (replace(request, decimal_conversions=None), _scoped_request(request, not same_scope)):
        with pytest.raises(SnowflakeBundleError, match="configured request"):
            reconstruct_replayable_parquet_bundle(connector.build_statement(changed), retained)
    assert cursor.closed and connection.closed


@pytest.mark.parametrize("empty", [False, True])
async def test_segmented_source_options_share_one_retained_header_check(monkeypatch, empty):
    harness = _Harness(
        monkeypatch, () if empty else (_shared_row(amount=0.1),), snapshot_token_mode="statement_query_id"
    )
    request = replace(_scoped_request(harness.request.bundle_request), decimal_conversions=_CONVERSIONS)
    harness.request = replace(harness.request, bundle_request=request)
    harness.cursor.execute = Mock()
    _real_metadata(harness.cursor)
    assert (await harness.run()).status == "capture_sealed"
    statement = harness.builder.build_statement(request)
    token = harness.pending.source_snapshot_token
    bundle = SimpleNamespace(capture_bundle_id=23, snapshot_token=token)
    loader = AsyncMock(return_value=({1: harness.receipts[0], 2: harness.receipts[1]}, {}, None))
    monkeypatch.setattr(snowflake_capture, "_load_parquet_part_metadata", loader)
    await snowflake_capture._validate_retained_source(object(), harness.request, statement, bundle)
    assert loader.await_count == 1
    harness.cursor.execute.assert_called_once_with(statement.sql, ("1003000126",))
    mutations = (
        lambda document: document["source"].update(query_id="other-query"),
        lambda document: document["source"]["result_schema"][1].update(source_type="FIXED(30,12)"),
    )
    for mutation in mutations:
        tampered = _reseal_receipt(harness.receipts[0], mutation)
        loader.return_value = ({1: tampered, 2: harness.receipts[1]}, {}, None)
        with pytest.raises(snowflake_capture.SnowflakeCaptureError):
            await snowflake_capture._validate_retained_source(object(), harness.request, statement, bundle)
    assert len(harness.parts) == 2 and harness.cursor.closed and harness.connection.closed
