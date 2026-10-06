# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Deterministic entity selection preserves complete multi-dimensional families."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from itertools import product
from types import SimpleNamespace

import pytest

from process.custom_import.definition import CustomImportDefinition
from process.custom_import.snowflake_binding import SnowflakeSourceBinding, SnowflakeSourceBindingError
from process.custom_import.snowflake_bundle import (
    SnowflakeBundleError,
    SnowflakeBundleStatementBuilder,
    prepare_bundle_replay,
    reconstruct_replayable_parquet_bundle,
    replayable_parquet_captures,
)
from process.custom_import.snowflake_bundle_replay import _rebuilt_bundle_statement
from process.custom_import.snowflake_inspection import SnowflakeInspectionStatement
from process.custom_import.snowflake_operator_cli import _retained_bundle_request
from process.custom_import.snowflake_preflight import SnowflakePreflightLimits, _prepare_preflight
from tests.test_custom_import_processing_policy import _policy_document
from tests.test_custom_import_snowflake_preflight import _binding, _definition
from tests.test_custom_import_snowflake_shared_capture import _runtime, _shared_row


def _prepared(definition, binding):
    approved, bindings = binding.bundle_components(definition)
    builder = SnowflakeBundleStatementBuilder(approved_relations=approved)
    loaded = SimpleNamespace(definition=definition, binding=binding, bundle_bindings=bindings)
    request = _retained_bundle_request(builder, loaded)
    return builder, builder.build_statement(request)


def _sql(statement):
    return statement.sql.replace('"SYNTHETIC"."PUBLIC".', "").replace("%s", "?")


def _family_scope(limit, *, segmented=True):
    document = json.loads(_definition(includes_aliases=False).canonical)
    document["schema"]["root"]["logical_key"].append("panel")
    document["schema"]["root"]["fields"].append({"id": "panel", "slot": 4, "type": "string", "nullable": False})
    for child, prefix, slot in zip(document["schema"]["children"], ("detail", "note"), (13, 23), strict=True):
        child["parent_key"].append({"root": "panel", "child": prefix + "_panel"})
        child["fields"].append({"id": prefix + "_panel", "slot": slot, "type": "string", "nullable": False})
    notes = document["schema"]["children"][1]
    notes["fields"].append({"id": "note_episode", "slot": 24, "type": "string", "nullable": False})
    notes["child_key"] = ["note_episode", "note_id"]
    document["child_memberships"] = [
        {
            "outer_collection": "details",
            "inner_collection": "notes",
            "key_mapping": [{"outer_field": "detail_id", "inner_field": "note_episode"}],
        }
    ]
    definition = CustomImportDefinition.from_mapping(document)
    binding = json.loads(_binding(definition).canonical)
    binding.update(snapshot_token_mode="statement_query_id", entity_limit=limit)
    if segmented:
        binding.update(contract="custom-import/source-binding/v2", processing_policy=_policy_document())
    for stream, source_stream in zip(binding["streams"], definition.source_streams, strict=True):
        stream.update(source_snapshot_token_relation=None, source_snapshot_token_column_identifier=None)
        stream["columns"] = [
            {"field_id": field.field_id, "column_identifier": field.field_id.upper()}
            for field in definition.fields
            if field.collection == source_stream.child_collection
        ]
    binding["streams"][0]["row_filters"] = [{"field_id": "label", "operator": "eq", "value": "keep"}]
    return definition, SnowflakeSourceBinding.from_mapping(binding)


def _populate_families(connection):
    connection.executescript("""
        CREATE TABLE ROOTS(NPI TEXT, EDITION INTEGER, LABEL TEXT, PANEL TEXT);
        CREATE TABLE DETAILS(DETAIL_NPI TEXT, DETAIL_EDITION INTEGER, DETAIL_ID TEXT, DETAIL_PANEL TEXT);
        CREATE TABLE NOTES(NOTE_NPI TEXT, NOTE_EDITION INTEGER, NOTE_ID TEXT, NOTE_PANEL TEXT, NOTE_EPISODE TEXT);
    """)
    providers = ("1999999996", "1234567893", "1003000126", "0000000000")
    for npi, year, panel in product(providers, (2022, 2021), ("panel_b", "panel_a")):
        connection.execute(
            "INSERT INTO ROOTS VALUES (?, ?, ?, ?)", (npi, year, "skip" if npi == "0000000000" else "keep", panel)
        )
        for episode in ("episode_b", "episode_a"):
            connection.execute("INSERT INTO DETAILS VALUES (?, ?, ?, ?)", (npi, year, episode, panel))
            for measure in ("measure_b", "measure_a"):
                connection.execute("INSERT INTO NOTES VALUES (?, ?, ?, ?, ?)", (npi, year, measure, panel, episode))


@pytest.mark.parametrize("value", [None, False, True, 0, -1, 1_000_001, 1.0, "100000", [], {}, 2**63])
def test_wire_denies_non_integer_null_and_unbounded_limits(value):
    definition = _definition()
    document = json.loads(_binding(definition).canonical)
    with pytest.raises(SnowflakeSourceBindingError):
        SnowflakeSourceBinding.from_mapping(document | {"entity_limit": value})


@pytest.mark.parametrize("limit", [1, 100_000, 1_000_000])
def test_limit_is_sealed_through_binding_cli_request_preview_and_replay(limit):
    definition, binding = _family_scope(limit)
    builder, statement = _prepared(definition, binding)
    assert SnowflakeSourceBinding.from_json(binding.canonical) == binding
    assert json.loads(binding.canonical)["entity_limit"] == limit
    assert json.loads(statement.request.canonical_request)["entity_limit"] == limit
    assert _rebuilt_bundle_statement(statement) == (statement.request, statement)
    preview = _prepare_preflight(definition, binding, builder, SnowflakePreflightLimits()).statement
    assert preview.bundle_statement == statement
    assert preview.sql.count('"__ci_entity_cohort" AS (') == 1
    assert statement.sql.count(" LIMIT ") == 1 and f"LIMIT {limit})" in statement.sql
    assert statement.parameters == ("keep", "keep")
    assert preview.parameters == ("keep", "keep", "keep")
    unlimited = replace(binding, entity_limit=None)
    _, unlimited_statement = _prepared(definition, unlimited)
    assert binding.digest != unlimited.digest
    assert statement.request.request_sha256 != unlimited_statement.request.request_sha256
    assert statement.statement_sha256 != unlimited_statement.statement_sha256
    assert "entity_limit" not in json.loads(unlimited.canonical)
    assert "entity_limit" not in json.loads(unlimited_statement.request.canonical_request)


@pytest.mark.parametrize("segmented", [False, True])
@pytest.mark.parametrize("limit", [1, 2, 100_000])
def test_generated_statement_keeps_all_year_panel_episode_measure_rows(limit, segmented):
    definition, binding = _family_scope(limit, segmented=segmented)
    _, statement = _prepared(definition, binding)
    with sqlite3.connect(":memory:") as connection:
        _populate_families(connection)
        records = tuple(connection.execute(_sql(statement), statement.parameters))
    fields = tuple(field.field_id for field in sorted(definition.fields, key=lambda field: field.field_slot))
    by_stream = {source.stream_id: [] for source in definition.source_streams}
    for row in records:
        if row[0] == 1:
            by_stream[row[2]].append(dict(zip(fields, row[4:], strict=True)))
    providers = {"1003000126", "1234567893", "1999999996"}
    expected_providers = set(sorted(providers)[:limit])
    for stream, entity, per_provider in (
        ("root_source", "npi", 4),
        ("detail_source", "detail_npi", 8),
        ("note_source", "note_npi", 16),
    ):
        assert {row[entity] for row in by_stream[stream]} == expected_providers
        assert len(by_stream[stream]) == len(expected_providers) * per_provider
    for npi, year, panel, episode, measure in product(
        expected_providers, (2021, 2022), ("panel_a", "panel_b"), ("episode_a", "episode_b"), ("measure_a", "measure_b")
    ):
        assert any(
            (row["note_npi"], row["note_edition"], row["note_panel"], row["note_episode"], row["note_id"])
            == (npi, year, panel, episode, measure)
            for row in by_stream["note_source"]
        )


def test_exact_100000_distinct_entities_is_not_a_100000_row_limit():
    definition = _definition(root_only=True)
    binding = replace(_binding(definition, uses_query_identity_snapshot=True), entity_limit=100_000)
    _, statement = _prepared(definition, binding)
    with sqlite3.connect(":memory:") as connection:
        connection.execute("CREATE TABLE ROOTS(ROOT_NPI TEXT, ROOT_EDITION INTEGER, ROOT_LABEL TEXT)")
        connection.executemany(
            "INSERT INTO ROOTS VALUES (?, ?, ?)",
            (
                (str(1_000_000_000 + ordinal), year, "synthetic")
                for ordinal in range(100_000, -1, -1)
                for year in (2, 1)
            ),
        )
        count, distinct, first, last = connection.execute(
            "SELECT COUNT(*), COUNT(DISTINCT npi), MIN(npi), MAX(npi) FROM ("
            + _sql(statement)
            + ') WHERE "__ci_bundle_row_kind" = 1',
            statement.parameters,
        ).fetchone()
    assert (count, distinct, first, last) == (200_000, 100_000, "1000000000", "1000099999")


def test_preview_and_estimates_are_scoped_to_the_same_entity_cohort():
    definition, binding = _family_scope(1)
    builder, statement = _prepared(definition, binding)
    preview = _prepare_preflight(definition, binding, builder, SnowflakePreflightLimits(maximum_root_keys=8)).statement
    estimate = SnowflakeInspectionStatement(statement, "estimate")
    with sqlite3.connect(":memory:") as connection:
        _populate_families(connection)
        connection.create_function("TO_VARCHAR", 1, str)
        connection.create_function("OCTET_LENGTH", 1, lambda value: len(value.encode()))
        observations = tuple(connection.execute(_sql(estimate), estimate.parameters))
        records = tuple(connection.execute(_sql(preview), preview.parameters))
        for ordinal in range(3):
            discovery = SnowflakeInspectionStatement(statement, "discover", ordinal)
            assert tuple(connection.execute(_sql(discovery), discovery.parameters)) == ()
    assert observations == ((0, 4), (1, 8), (2, 16))
    assert {row[6] for row in records if row[0] in {1, 2} and row[6] is not None} == {"1003000126"}
    assert [sum(row[0] == 2 and row[1] == ordinal for row in records) for ordinal in (1, 2, 3)] == [4, 8, 16]


def test_cohort_requires_entity_parent_membership_not_an_unrelated_key():
    document = json.loads(_definition(root_only=True).canonical)
    document["schema"]["root"]["logical_key"] = ["edition"]
    definition = CustomImportDefinition.from_mapping(document)
    binding = _binding(definition, uses_query_identity_snapshot=True)
    _prepared(definition, binding)
    with pytest.raises(SnowflakeSourceBindingError, match="fixed bundle"):
        _prepared(definition, replace(binding, entity_limit=1))


def test_shared_physical_scan_requires_the_same_entity_membership_column(monkeypatch):
    connector, request, _, _, _ = _runtime(monkeypatch)
    request = replace(request, entity_limit=1)
    statement = connector.build_statement(request)
    assert statement.sql.count('FROM "SYNTHETIC"."PUBLIC"."ROOTS"') == 2
    columns = list(statement.selected_columns_by_stream)
    columns[1] = tuple(
        replace(column, column_identifier="OTHER_NPI") if column.field_id == "detail_npi" else column
        for column in columns[1]
    )
    from process.custom_import.snowflake_bundle import _bundle_source_key

    assert _bundle_source_key(request, request.bindings[0], columns[0]) != _bundle_source_key(
        request, request.bindings[1], columns[1]
    )
    distinct_sources = replace(statement, selected_columns_by_stream=tuple(columns))
    assert distinct_sources.sql.count('FROM "SYNTHETIC"."PUBLIC"."ROOTS"') == 3
    assert '"OTHER_NPI" IN (SELECT "__ci_entity_id" FROM "__ci_entity_cohort")' in distinct_sources.sql


def test_one_snapshot_capture_and_retained_replay_preserve_the_cohort(monkeypatch):
    connector, request, _, cursor, _ = _runtime(monkeypatch, (_shared_row(),), snapshot_token_mode="statement_query_id")
    request = replace(request, entity_limit=1)
    acquired = connector.acquire(request)
    assert acquired.statement.request.entity_limit == 1
    assert acquired.source_snapshot_token == f"snowflake-query:{cursor.sfqid}"
    retained = replayable_parquet_captures(acquired)
    assert reconstruct_replayable_parquet_bundle(acquired.statement, retained) == prepare_bundle_replay(acquired)
    object.__setattr__(acquired.statement.request, "entity_limit", 2)
    with pytest.raises(SnowflakeBundleError):
        prepare_bundle_replay(acquired)
