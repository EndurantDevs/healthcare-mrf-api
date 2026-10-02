# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared data projections fan out captured scalars without rereading a relation."""

from __future__ import annotations

import json
from asyncio import CancelledError
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pyarrow.parquet as pq
import pytest

from process.custom_import import snowflake_python
from process.custom_import.capture_limits import CaptureLimits
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import assemble_root_families
from process.custom_import.processing_policy import ProcessingPolicy
from process.custom_import.snowflake import SnowflakeApprovedRelation, SnowflakeConnectorError, SnowflakeDeclaredColumn
from process.custom_import.snowflake_bundle import (
    SnowflakeBundleAcquisitionConnector,
    SnowflakeBundleBinding,
    SnowflakeBundleEncoding,
    SnowflakeBundleError,
    _bundle_sql_branches,
    prepare_bundle_replay,
)
from tests.test_custom_import_processing_policy import _policy_document
from tests.test_custom_import_snowflake_bundle import (
    _SNAPSHOT,
    _Credentials,
    _definition,
    _PythonConnection,
    _PythonCursor,
    _relations,
)

_PROCESSING_POLICY = ProcessingPolicy.from_mapping(_policy_document())


def _shared_definition(*, interleaved=False):
    document = json.loads(_definition().canonical)
    document["schema"]["root"]["fields"][1]["type"] = "decimal"
    if interleaved:
        document["streams"].insert(1, {**document["streams"][1], "id": "other_source", "child": "other"})
        document["schema"]["children"].append(
            {
                "name": "other",
                "parent_key": [{"child": "other_npi", "root": "npi"}],
                "child_key": ["other_id"],
                "fields": [
                    {"id": "other_npi", "slot": 7, "type": "string", "nullable": False},
                    {"id": "other_id", "slot": 8, "type": "string", "nullable": False},
                    {"id": "other_amount", "slot": 9, "type": "decimal", "nullable": False},
                ],
            }
        )
        document["aliases"]["other_source"] = {}
    return CustomImportDefinition.from_mapping(document)


def _configuration(*, interleaved=False):
    snapshot, root, detail = _relations()
    columns = (
        root.columns[0],
        replace(root.columns[1], column_identifier="detail_amount"),
        root.columns[2],
        replace(detail.columns[0], column_identifier="root_npi"),
        *detail.columns[1:],
    )
    approved_relations = [snapshot, SnowflakeApprovedRelation(root.relation, columns)]
    definition = _shared_definition(interleaved=interleaved)
    bindings = [
        SnowflakeBundleBinding(
            stream.stream_id,
            root.relation,
            snapshot.relation,
            tuple(field.field_id for field in definition.fields if field.collection == stream.child_collection),
            "semantic_snapshot",
        )
        for stream in definition.source_streams
    ]
    if interleaved:
        approved_relations.append(
            SnowflakeApprovedRelation(
                detail.relation,
                tuple(SnowflakeDeclaredColumn(field, field) for field in ("other_npi", "other_id", "other_amount")),
            )
        )
        bindings[1] = replace(bindings[1], relation=detail.relation)
    return definition, tuple(approved_relations), tuple(bindings)


def _runtime(
    monkeypatch, data_rows=(), *, interleaved=False, limits=None, partition_rows=1, processing_policy=_PROCESSING_POLICY
):
    definition, approved, bindings = _configuration(interleaved=interleaved)
    adapter = snowflake_python.SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh")
    connector = SnowflakeBundleAcquisitionConnector(
        approved_relations=approved,
        credential_provider=_Credentials(),
        adapter=adapter,
        capture_limits=limits or CaptureLimits(),
    )
    request = connector.prepare_request(
        definition,
        bindings=bindings,
        encoding=SnowflakeBundleEncoding(partition_rows=partition_rows),
        processing_policy=processing_policy,
    )
    metadata_rows = tuple(
        (0, ordinal, binding.stream_id, _SNAPSHOT, *((None,) * len(definition.fields)))
        for ordinal, binding in enumerate(request.bindings, start=1)
    )
    cursor = _PythonCursor((*metadata_rows, *data_rows))
    metadata = list(cursor.description)
    metadata[5] = SimpleNamespace(name="score", type_name="FIXED", is_nullable=True, precision=30, scale=12)
    for index in (7, 9):
        metadata[index] = SimpleNamespace(
            name=metadata[index].name, type_name="FIXED", is_nullable=True, precision=38, scale=0
        )
    if interleaved:
        metadata.extend(
            (
                SimpleNamespace(name="other_npi", type_name="TEXT", is_nullable=True),
                SimpleNamespace(name="other_id", type_name="TEXT", is_nullable=True),
                SimpleNamespace(name="other_amount", type_name="FIXED", is_nullable=True, precision=30, scale=12),
            )
        )
    cursor.description = tuple(metadata)
    cursor.fetchone = Mock(wraps=cursor.fetchone)
    connection = _PythonConnection(cursor)
    monkeypatch.setattr(adapter, "_connect", lambda _credentials: (connection, cursor))
    return connector, request, adapter, cursor, connection


def _shared_row(npi="1003000126", *, key="a", amount=Decimal("123456789012345678.123456789012")):
    return (1, 1, "root_source", None, npi, amount, True, None, key, None)


def _consume_stream(bundle, stream_index):
    parquet_result = bundle.stream_results[stream_index].parquet_result
    records = []
    for reader in parquet_result.consume_partition_sources():
        parquet_result.claim_partition_source(reader)
        try:
            records.extend(pq.read_table(reader).to_pylist())
        finally:
            parquet_result.close_partition_source(reader)
    parquet_result.close_partition_iterator()
    return records


@pytest.mark.parametrize("token_on_data_relation", (False, True))
def test_shared_projection_reads_each_scalar_once(monkeypatch, token_on_data_relation):
    connector, request, _, _, _ = _runtime(monkeypatch)
    statement = connector.build_statement(request)
    if token_on_data_relation:
        request = replace(
            request,
            bindings=tuple(
                replace(binding, source_snapshot_token_relation=binding.relation) for binding in request.bindings
            ),
        )
        statement = replace(statement, request=request)
    metadata, data = _bundle_sql_branches(
        request,
        tuple(sorted(request.definition.fields, key=lambda field: field.field_slot)),
        statement.selected_columns_by_stream,
        statement.source_snapshot_token_columns_by_stream,
    )
    assert len(metadata) == 2 and len(data) == 1
    assert data[0].count('"ROOT_NPI"') == 1
    assert data[0].count('"DETAIL_AMOUNT"') == 1
    assert 'NULL AS "detail_npi"' in data[0] and 'NULL AS "amount"' in data[0]
    assert all('SELECT DISTINCT "SNAPSHOT_TOKEN" AS "__ci_snapshot_native"' in branch for branch in metadata)
    if token_on_data_relation:
        assert all(request.bindings[0].relation.quoted_sql in branch for branch in metadata)


def test_shared_capture_replays_exact_native_scalars(monkeypatch):
    physical_rows = (_shared_row(), _shared_row(), _shared_row("invalid-provider", key="invalid"))
    connector, request, _, cursor, connection = _runtime(monkeypatch, physical_rows)
    acquisition = connector.acquire(request)
    replay = prepare_bundle_replay(acquisition)
    children = replay.children_by_collection["details"]

    assert len(replay.roots) == len(children) == 3
    assert [root["npi"] for root in replay.roots] == [child["detail_npi"] for child in children]
    assert [root["score"] for root in replay.roots] == [child["amount"] for child in children]
    assert children[0]["amount"] == Decimal("123456789012345678.123456789012")
    assert acquisition.stream_captures[1].schema[0].source_type == "TEXT"
    assert acquisition.stream_captures[1].schema[2].source_type == "FIXED(30,12)"
    assert acquisition.diagnostic_query_id == cursor.sfqid
    assert acquisition.source_snapshot_token == _SNAPSHOT
    assert cursor.executed == [acquisition.statement.sql]
    assert cursor.fetchone.call_count == len(request.bindings) + len(physical_rows) + 1
    families = assemble_root_families(request.definition, replay.roots, replay.children_by_collection)
    assert {rejection.code for rejection in families.rejections} >= {"duplicate_root_key", "entity_binding_invalid"}
    assert not families.families
    assert cursor.closed and connection.closed


def test_shared_stream_reuses_captured_bytes(monkeypatch):
    connector, request, adapter, cursor, connection = _runtime(monkeypatch, (_shared_row(),))
    bundle = adapter.fetch_bundle(connector.build_statement(request), _Credentials().load_key_pair())
    roots = _consume_stream(bundle, 0)
    fetch_count = cursor.fetchone.call_count
    children = _consume_stream(bundle, 1)
    bundle.close()
    assert roots[0]["npi"] == children[0]["detail_npi"]
    assert roots[0]["score"] == children[0]["amount"]
    assert cursor.fetchone.call_count == fetch_count
    assert cursor.closed and connection.closed


def test_shared_capture_preserves_distinct_parent_columns(monkeypatch):
    physical_values = list(_shared_row())
    physical_values[7] = "1234567893"
    _, request, adapter, cursor, connection = _runtime(monkeypatch, (tuple(physical_values),))
    definition, approved, _ = _configuration()
    shared_relation = replace(
        approved[1],
        columns=tuple(
            replace(column, column_identifier="detail_parent_npi") if column.field_id == "detail_npi" else column
            for column in approved[1].columns
        ),
    )
    connector = SnowflakeBundleAcquisitionConnector(
        approved_relations=(approved[0], shared_relation), credential_provider=_Credentials(), adapter=adapter
    )
    cursor.description = (
        *cursor.description[:7],
        SimpleNamespace(name="detail_npi", type_name="TEXT", is_nullable=True),
        *cursor.description[8:],
    )
    replay = prepare_bundle_replay(connector.acquire(request))
    assert replay.roots[0]["npi"] != replay.children_by_collection["details"][0]["detail_npi"]
    families = assemble_root_families(definition, replay.roots, replay.children_by_collection)
    assert families.candidate_errors == ("orphan_child",)
    assert cursor.closed and connection.closed


@pytest.mark.parametrize("shared_rows", ((), (_shared_row(), _shared_row("1234567893", key="b"))))
def test_shared_streams_allow_interleaved_relations(monkeypatch, shared_rows):
    physical_rows = (
        *(row + (None, None, None) for row in shared_rows),
        (1, 2, "other_source", None, None, None, None, None, None, None, "1003000126", "other", Decimal("2.25")),
    )
    connector, request, _, cursor, connection = _runtime(monkeypatch, physical_rows, interleaved=True)
    acquisition = connector.acquire(request)
    replay = prepare_bundle_replay(acquisition)
    assert [stream.stream_id for stream in replay.streams] == ["root_source", "other_source", "detail_source"]
    assert len(replay.roots) == len(replay.children_by_collection["details"]) == len(shared_rows)
    assert replay.children_by_collection["other"][0]["other_amount"] == Decimal("2.25")
    assert all(stream.parts for stream in replay.streams)
    assert cursor.closed and connection.closed


def test_empty_shared_streams_retain_metadata(monkeypatch):
    connector, request, _, cursor, connection = _runtime(monkeypatch)
    acquisition = connector.acquire(request)
    replay = prepare_bundle_replay(acquisition)
    assert replay.roots == () and replay.children_by_collection == {"details": ()}
    assert all(stream.receipt.source_snapshot_token == _SNAPSHOT and stream.parts for stream in replay.streams)
    assert cursor.closed and connection.closed


@pytest.mark.parametrize(
    ("limit_name", "limit"),
    (
        ("MAX_BUNDLE_CAPTURE_BYTES", 1),
        ("MAX_STREAM_CAPTURE_BYTES", 1),
        ("MAX_BUNDLE_PARTITIONS", 1),
        ("MAX_RESULT_PARTITIONS", 1),
    ),
)
def test_shared_capture_limits_close_retained_readers(monkeypatch, limit_name, limit):
    monkeypatch.setattr(snowflake_python, limit_name, limit)
    readers = []
    encode = snowflake_python._parquet_reader

    def retain_reader(*arguments):
        reader = encode(*arguments)
        readers.append(reader)
        return reader

    monkeypatch.setattr(snowflake_python, "_parquet_reader", retain_reader)
    connector, request, _, cursor, connection = _runtime(monkeypatch, (_shared_row(), _shared_row(key="b")))
    with pytest.raises(SnowflakeBundleError, match="cannot be sealed"):
        connector.acquire(request)
    assert readers and all(reader.closed for reader in readers)
    assert cursor.closed and connection.closed


def test_shared_capture_honors_configured_byte_bound(monkeypatch):
    limits = CaptureLimits(maximum_compressed_bytes=1)
    connector, request, _, cursor, connection = _runtime(monkeypatch, (_shared_row(),), limits=limits)
    with pytest.raises(SnowflakeBundleError, match="cannot be sealed"):
        connector.acquire(request)
    assert cursor.closed and connection.closed


def test_shared_capture_counts_buffered_bytes(monkeypatch):
    connector, request, adapter, cursor, connection = _runtime(monkeypatch, (_shared_row(),))
    bundle = adapter.fetch_bundle(connector.build_statement(request), _Credentials().load_key_pair())
    owner = bundle.stream_results[0].parquet_result.partition_sources._owner
    root_values = ((_shared_row()[4], _shared_row()[5], True),)
    child_values = ((_shared_row()[4], "a", _shared_row()[5]),)
    sizes = []
    for stream_index, values in enumerate((root_values, child_values)):
        with snowflake_python._parquet_reader(values, owner._schemas[stream_index]) as reader:
            sizes.append(reader.getbuffer().nbytes)
    monkeypatch.setattr(snowflake_python, "MAX_BUNDLE_CAPTURE_BYTES", sum(sizes) - 1)
    with pytest.raises(SnowflakeConnectorError, match="bundle byte or partition limit"):
        _consume_stream(bundle, 0)
    bundle.close()
    assert owner._generated_bytes == sizes[0]
    assert cursor.closed and connection.closed


@pytest.mark.parametrize(
    ("root_token", "child_token"),
    (
        (_SNAPSHOT, "synthetic-other-snapshot"),
        ("TIMESTAMP_NTZ:2025-01-01T12:00:00.123456781", "TIMESTAMP_NTZ:2025-01-01T12:00:00.123456789"),
        ("NUMBER:42", "VARCHAR:42"),
    ),
)
def test_shared_capture_rejects_differing_snapshot_tokens(monkeypatch, root_token, child_token):
    connector, request, _, cursor, connection = _runtime(monkeypatch, (_shared_row(),))
    for index, token in enumerate((root_token, child_token)):
        metadata_values = list(cursor._rows[index])
        metadata_values[3] = token
        cursor._rows[index] = tuple(metadata_values)
    with pytest.raises(SnowflakeConnectorError, match="one shared semantic snapshot token"):
        connector.acquire(request)
    assert cursor.closed and connection.closed


def test_shared_capture_validates_child_scalar_before_buffering(monkeypatch):
    connector, request, _, cursor, connection = _runtime(monkeypatch, (_shared_row(key=["invalid"]),))
    with pytest.raises(SnowflakeBundleError, match="cannot be sealed"):
        connector.acquire(request)
    assert cursor.closed and connection.closed


@pytest.mark.parametrize("failure", (CancelledError(), RuntimeError("synthetic fetch failure")))
def test_shared_capture_abort_closes_buffered_readers(monkeypatch, failure):
    connector, request, adapter, cursor, connection = _runtime(monkeypatch, (_shared_row(), _shared_row(key="b")))
    bundle = adapter.fetch_bundle(connector.build_statement(request), _Credentials().load_key_pair())
    owner = bundle.stream_results[0].parquet_result.partition_sources._owner
    root = bundle.stream_results[0].parquet_result
    iterator = root.consume_partition_sources()
    reader = next(iterator)
    root.claim_partition_source(reader)
    root.close_partition_source(reader)
    buffered_readers = tuple(owner._shared_partitions[1])
    cursor.fetchone = Mock(side_effect=failure)
    expected = CancelledError if isinstance(failure, CancelledError) else SnowflakeConnectorError
    with pytest.raises(expected):
        next(iterator)
    bundle.close()
    assert buffered_readers and all(reader.closed for reader in buffered_readers)
    assert cursor.closed and connection.closed


def test_shared_capture_close_releases_unconsumed_streams(monkeypatch):
    connector, request, adapter, cursor, connection = _runtime(monkeypatch, (_shared_row(),))
    bundle = adapter.fetch_bundle(connector.build_statement(request), _Credentials().load_key_pair())
    _consume_stream(bundle, 0)
    owner = bundle.stream_results[0].parquet_result.partition_sources._owner
    buffered_readers = tuple(owner._shared_partitions[1])
    bundle.close()
    bundle.close()
    assert buffered_readers and all(reader.closed for reader in buffered_readers)
    assert cursor.closed and connection.closed


def test_shared_capture_rejects_repeated_data_scan(monkeypatch):
    repeated = (1, 2, "detail_source", None, None, None, None, "other-npi", "b", Decimal("1.25"))
    connector, request, _, cursor, connection = _runtime(monkeypatch, (_shared_row(), repeated))
    with pytest.raises(SnowflakeBundleError, match="cannot be sealed"):
        connector.acquire(request)
    assert cursor.closed and connection.closed
