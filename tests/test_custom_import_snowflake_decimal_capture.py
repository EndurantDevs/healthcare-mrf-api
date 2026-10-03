# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic FLOAT conversion parity across preview, capture, and sealed replay."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from decimal import Decimal
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from process.custom_import import snowflake_candidate, snowflake_capture, snowflake_operator_cli, snowflake_python
from process.custom_import.capture import capture_stream
from process.custom_import.capture_limits import CaptureLimits
from process.custom_import.runner import CandidateRunResult
from process.custom_import.snowflake import SnowflakeConnectorError
from process.custom_import.snowflake_bundle import (
    SnowflakeBundleError,
    SnowflakeBundleStatementBuilder,
    prepare_bundle_replay,
    reconstruct_replayable_parquet_bundle,
    replayable_parquet_captures,
)
from process.custom_import.snowflake_bundle_replay import _validate_replay_partition_schema
from process.custom_import.snowflake_preflight import (
    SnowflakePreflightLimits,
    SnowflakePreflightStatement,
    preflight_snowflake_bundle,
)
from process.custom_import.snowflake_preflight_schema import validate_preflight_result_schema
from tests import test_custom_import_snowflake_operator_cli as cli_cases
from tests.test_custom_import_snowflake_capture import _Harness
from tests.test_custom_import_snowflake_decimal_conversions import _CONVERSIONS, _binding, _statement
from tests.test_custom_import_snowflake_landing import _open
from tests.test_custom_import_snowflake_preflight import _Adapter
from tests.test_custom_import_snowflake_preflight_python import _description
from tests.test_custom_import_snowflake_shared_capture import _PROCESSING_POLICY, _runtime, _shared_row


def _real_metadata(cursor):
    metadata = list(cursor.description)
    for index in (5, 9):
        metadata[index] = SimpleNamespace(name=metadata[index].name, type_name="REAL", is_nullable=True)
    cursor.description = tuple(metadata)


def _converted_runtime(monkeypatch, rows=(), **options):
    connector, request, adapter, cursor, connection = _runtime(monkeypatch, rows, **options)
    request = replace(request, decimal_conversions=_CONVERSIONS)
    _real_metadata(cursor)
    monkeypatch.setattr(adapter, "_connect", lambda _credentials, **_options: (connection, cursor))
    return connector, request, adapter, cursor, connection


@pytest.mark.parametrize("rows", [(), (_shared_row(amount=0.1),), (_shared_row(amount=None),)])
def test_landing_keeps_real_evidence_and_exact_decimal_transport_even_when_empty(monkeypatch, rows):
    connector, request, adapter, cursor, connection = _converted_runtime(monkeypatch, rows)
    statement = connector.build_statement(request)
    with _open(adapter, statement) as result:
        assert result.schemas[0][1].source_type == result.schemas[1][2].source_type == "REAL"
        token, sources = snowflake_capture._landing_metadata(statement, result)
        assert all(source["query_id"] == cursor.sfqid for source in sources.values())
        assert token == result.source_snapshot_token
        parts = [event for event in result.consume_events() if isinstance(event, snowflake_python.SnowflakeLandingPart)]
    assert len(parts) == 2 and cursor.closed and connection.closed
    for part in parts:
        field_id = "score" if part.stream_id == "root_source" else "amount"
        table = pq.read_table(BytesIO(part.capture.payload))
        assert table[field_id].type == pa.decimal128(30, 12)
        expected = [] if not rows else [None if rows[0][5] is None else Decimal("0.100000000000")]
        assert table[field_id].to_pylist() == expected
        assert part.arrow_byte_count == table.nbytes
    assert len(cursor.executed) == 1


@pytest.mark.parametrize("processing_policy", [None, _PROCESSING_POLICY])
def test_acquisition_and_offline_replays_preserve_conversion_identity(monkeypatch, processing_policy):
    source_row = _shared_row(amount=0.1)
    connector, request, _, cursor, connection = _converted_runtime(
        monkeypatch,
        (source_row,),
        processing_policy=processing_policy,
    )
    if processing_policy is None:
        metadata = list(cursor.description)
        metadata[7] = SimpleNamespace(name="detail_npi", type_name="TEXT", is_nullable=True)
        cursor.description = tuple(metadata)
        cursor._rows.append((1, 2, "detail_source", None, None, None, None, "1003000126", "a", 0.1))
    acquired = connector.acquire(request)
    replay = prepare_bundle_replay(acquired)
    assert replay.roots[0]["score"] == Decimal("0.1")
    durable = replayable_parquet_captures(acquired)
    assert reconstruct_replayable_parquet_bundle(acquired.statement, durable) == replay
    changed_statement = replace(acquired.statement, request=replace(request, decimal_conversions=None))
    with pytest.raises(SnowflakeBundleError, match="configured request"):
        reconstruct_replayable_parquet_bundle(changed_statement, durable)
    changed_columns = tuple(
        replace(column, source_type="FIXED(30,12)") if column.field_id == "score" else column
        for column in acquired.stream_captures[0].schema
    )
    with pytest.raises(SnowflakeConnectorError, match="declared decimal conversion"):
        replace(
            acquired,
            stream_captures=(replace(acquired.stream_captures[0], schema=changed_columns), acquired.stream_captures[1]),
        )
    assert cursor.closed and connection.closed


@pytest.mark.parametrize("invalid", [1, Decimal("0.1"), "0.1", float("nan"), float("inf"), 1e18])
def test_bad_source_float_fails_before_capture_and_closes_source(monkeypatch, invalid):
    connector, request, adapter, cursor, connection = _converted_runtime(monkeypatch, (_shared_row(amount=invalid),))
    with pytest.raises(SnowflakeConnectorError):
        with _open(adapter, connector.build_statement(request)) as result:
            list(result.consume_events())
    assert cursor.closed and connection.closed


def test_converted_arrow_limit_remains_enforced(monkeypatch):
    connector, request, adapter, cursor, connection = _converted_runtime(monkeypatch, (_shared_row(amount=0.1),))
    with pytest.raises(SnowflakeConnectorError, match="decoded-byte"):
        with _open(adapter, connector.build_statement(request), maximum_arrow_bytes=1) as result:
            list(result.consume_events())
    assert cursor.closed and connection.closed


def test_replay_does_not_accept_string_or_wider_decimal_as_converted_transport():
    definition, binding = _binding()
    statement = _statement(definition, binding)
    fields = definition.root_fields
    stream = definition.source_streams[0]
    for data_type in (pa.string(), pa.decimal128(31, 13), pa.int64()):
        sink = BytesIO()
        pq.write_table(pa.table({"npi": pa.array([], type=pa.string()), "score": pa.array([], type=data_type)}), sink)
        sealed = capture_stream(BytesIO(sink.getvalue()), stream, source_snapshot_token="snapshot")
        with pytest.raises(SnowflakeBundleError, match="decimal128"):
            _validate_replay_partition_schema(
                sealed, fields=fields, limits=CaptureLimits(), decimal_conversions=statement.request.decimal_conversions
            )


def _preview_rows(_statement, value=0.1):
    return (
        (0, 1, "root_source", None, "snapshot", None, None, None, None, None),
        (0, 2, "detail_source", None, "snapshot", None, None, None, None, None),
        (1, 0, None, 1, None, 1, "1003000126", None, None, None),
        (2, 1, "root_source", None, None, None, "1003000126", value, None, None),
    )


@pytest.mark.parametrize(
    "value,available", [(0.1, True), (float("nan"), False), (1e18, False), (Decimal("0.1"), False)]
)
def test_preview_uses_same_converter_before_bounded_accounting(value, available):
    definition, binding = _binding()
    approved, _ = binding.bundle_components(definition)
    builder = SnowflakeBundleStatementBuilder(approved_relations=approved)
    adapter = _Adapter(lambda statement: _preview_rows(statement, value))
    result = preflight_snowflake_bundle(
        definition, binding, builder, adapter, limits=SnowflakePreflightLimits(maximum_root_keys=1)
    )
    assert (result.sample is not None) == available
    assert adapter.cursor.closed
    if available:
        assert result.sample.families[0].root["score"] == Decimal("0.1")


@pytest.mark.parametrize("operation", ["execute", "resume"])
@pytest.mark.parametrize("version", [1, 2])
async def test_operator_forwards_retained_conversion_without_resume_source_access(monkeypatch, operation, version):
    definition, binding = _binding(version=version)
    approved, bindings = binding.bundle_components(definition)
    loaded = replace(
        cli_cases._loaded_binding(),
        definition=definition,
        binding=binding,
        source_binding_sha256=bytes.fromhex(binding.digest),
        approved_relations=approved,
        bundle_bindings=bindings,
    )
    database = cli_cases._resume_database(loaded)
    cli_cases._install_resume_preflight(monkeypatch, database, loaded)
    expected = CandidateRunResult(status="sealed_unpublished", execution_id=40)
    runner = AsyncMock(return_value=expected)
    forbidden = Mock(side_effect=AssertionError("resume cannot open the source"))
    monkeypatch.setattr(snowflake_operator_cli, "run_snowflake_bundle_candidate", runner)
    monkeypatch.setattr(snowflake_operator_cli, "run_segmented_snowflake_candidate", runner)
    monkeypatch.setattr(
        snowflake_operator_cli,
        "FixedLocalKeyPairCredentialProvider",
        cli_cases._CredentialProvider if operation == "execute" else forbidden,
    )
    monkeypatch.setattr(
        snowflake_operator_cli,
        "SnowflakePythonConnectorAdapter",
        cli_cases._Adapter if operation == "execute" else forbidden,
    )
    execute = (
        snowflake_operator_cli._run_retained_snowflake_binding
        if operation == "execute"
        else snowflake_operator_cli._run_resumed_snowflake_binding
    )
    assert (
        await execute(
            definition_revision_id=loaded.definition_revision_id,
            source_binding_revision_id=loaded.source_binding_revision_id,
            idempotency_key="synthetic-resume",
            database=database,
        )
        == expected
    )
    assert runner.await_args.args[2].bundle_request.decimal_conversions == binding.decimal_conversions
    assert database.connected == database.disconnected == 1
    forbidden.assert_not_called()


def test_preview_metadata_must_be_real_exactly_where_conversion_is_declared():
    definition, binding = _binding()
    statement = SnowflakePreflightStatement(_statement(definition, binding), SnowflakePreflightLimits())
    result_columns = _description(statement)
    with pytest.raises(SnowflakeConnectorError):
        validate_preflight_result_schema(statement, result_columns)
    result_columns = tuple(
        replace(column, type_name="REAL", precision=None, scale=None) if column.name == "score" else column
        for column in result_columns
    )
    validate_preflight_result_schema(statement, result_columns)
    legacy = replace(
        statement,
        bundle_statement=replace(
            statement.bundle_statement, request=replace(statement.bundle_statement.request, decimal_conversions=None)
        ),
    )
    with pytest.raises(SnowflakeConnectorError):
        validate_preflight_result_schema(legacy, result_columns)


async def test_capture_seal_and_reuse_reject_recomputed_source_type_tamper(monkeypatch):
    harness = _Harness(monkeypatch, (_shared_row(amount=0.1),))
    harness.request = replace(
        harness.request, bundle_request=replace(harness.request.bundle_request, decimal_conversions=_CONVERSIONS)
    )
    _real_metadata(harness.cursor)
    result = await harness.run()
    assert result.status == "capture_sealed"
    statement = harness.builder.build_statement(harness.request.bundle_request)
    token = harness.pending.source_snapshot_token
    snowflake_capture._validate_decimal_receipts(statement, token, harness.receipts)
    receipt = harness.receipts[0]
    document = json.loads(receipt.canonical_manifest)
    document["source"]["result_schema"][1]["source_type"] = "FIXED(30,12)"
    canonical = json.dumps(document, sort_keys=True, separators=(",", ":"))
    tampered = replace(
        receipt, canonical_manifest=canonical, manifest_sha256=hashlib.sha256(canonical.encode()).hexdigest()
    )
    with pytest.raises(snowflake_capture.SnowflakeCaptureError, match="decimal conversion metadata"):
        snowflake_capture._validate_decimal_receipts(statement, token, (tampered, harness.receipts[1]))
    loader = AsyncMock(return_value=({1: tampered, 2: harness.receipts[1]}, {}, None))
    monkeypatch.setattr(snowflake_capture, "_load_parquet_part_metadata", loader)
    bundle = SimpleNamespace(capture_bundle_id=7, snapshot_token=token)
    with pytest.raises(snowflake_capture.SnowflakeCaptureError):
        await snowflake_capture._validate_retained_source(object(), harness.request, statement, bundle)
    assert loader.await_count == 1


async def test_bound_source_rejects_conversion_identity_mismatch(monkeypatch):
    definition, binding = _binding()
    statement = _statement(definition, binding)
    request = SimpleNamespace(
        source_binding_revision_id=4,
        source_binding_sha256=b"a" * 32,
        dataset_id=1,
        schema_revision_id=3,
        definition_revision_id=2,
        definition=definition,
        bundle_request=statement.request,
    )
    loaded = SimpleNamespace(
        dataset_id=1,
        schema_revision_id=3,
        definition=definition,
        source_binding_sha256=request.source_binding_sha256,
        bundle_bindings=statement.request.bindings,
        binding=replace(binding, decimal_conversions=None),
    )
    monkeypatch.setattr(snowflake_candidate, "load_snowflake_source_binding", AsyncMock(return_value=loaded))
    with pytest.raises(snowflake_candidate.SnowflakeCandidateError, match="source binding identity"):
        await snowflake_candidate._validate_bound_source_identity(object(), request, statement)


async def test_already_bound_conversion_rechecks_headers_without_opening_source(monkeypatch):
    harness = _Harness(monkeypatch)
    harness.request = replace(
        harness.request, bundle_request=replace(harness.request.bundle_request, decimal_conversions=_CONVERSIONS)
    )
    _real_metadata(harness.cursor)
    assert (await harness.run()).status == "capture_sealed"
    statement = harness.builder.build_statement(harness.request.bundle_request)
    harness.bound = SimpleNamespace(
        dataset_id=1,
        definition_revision_id=2,
        schema_revision_id=3,
        capture_state="sealed",
        payload_contract=snowflake_capture.SEGMENTED_PAYLOAD_CONTRACT,
        canonical_policy=harness.policy.canonical,
        source_binding_revision_id=None,
        producing_execution_id=9,
        source_binding_sha256=None,
        request_identity_sha256=harness.identity,
        policy_sha256=bytes.fromhex(harness.policy.digest),
        source_request_sha256=bytes.fromhex(statement.request.request_sha256),
        statement_sha256=bytes.fromhex(statement.statement_sha256),
        capture_bundle_id=23,
        snapshot_token=harness.pending.source_snapshot_token,
    )
    loader = AsyncMock(return_value=({index: receipt for index, receipt in enumerate(harness.receipts)}, {}, None))
    monkeypatch.setattr(snowflake_capture, "_load_parquet_part_metadata", loader)
    harness.submission = replace(harness.submission, capture_bundle_id=23)
    harness.timeline.clear()
    assert (await harness.run()).status == "capture_bound"
    assert "credentials" not in harness.timeline and len(harness.cursor.executed) == 1
    loader.assert_awaited_once()
    loader.return_value = ({0: harness.receipts[0]}, {}, None)
    with pytest.raises(snowflake_capture.SnowflakeCaptureError, match="decimal conversion metadata"):
        await harness.run()


async def test_conversion_receipts_bind_every_stream_and_query_even_after_rehash(monkeypatch):
    harness = _Harness(monkeypatch)
    harness.request = replace(
        harness.request, bundle_request=replace(harness.request.bundle_request, decimal_conversions=_CONVERSIONS)
    )
    _real_metadata(harness.cursor)
    await harness.run()
    statement = harness.builder.build_statement(harness.request.bundle_request)
    token = harness.pending.source_snapshot_token
    for receipts in (harness.receipts[:1], (harness.receipts[0],) * 2):
        with pytest.raises(snowflake_capture.SnowflakeCaptureError):
            snowflake_capture._validate_decimal_receipts(statement, token, receipts)
    mutations = [
        ("source_request_sha256", "0" * 64),
        ("statement_sha256", "0" * 64),
        ("source_snapshot_token", "different"),
        ("stream_id", "different"),
    ]
    for field_id, replacement in mutations + [("query_id", "other-query"), ("query_id", None)]:
        document = json.loads(harness.receipts[0].canonical_manifest)
        destination = document["source"] if field_id == "query_id" else document
        destination[field_id] = replacement
        canonical = json.dumps(document, sort_keys=True, separators=(",", ":"))
        receipt = replace(
            harness.receipts[0],
            canonical_manifest=canonical,
            manifest_sha256=hashlib.sha256(canonical.encode()).hexdigest(),
        )
        with pytest.raises(snowflake_capture.SnowflakeCaptureError):
            snowflake_capture._validate_decimal_receipts(statement, token, (receipt, harness.receipts[1]))
