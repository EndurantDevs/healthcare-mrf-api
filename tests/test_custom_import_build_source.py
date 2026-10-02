# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Source page bounds, existing scalar semantics, and replay cleanup ownership."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects.postgresql import dialect

import process.custom_import.build_source as staging
from process.custom_import.capture import capture_stream
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import assemble_root_families
from process.custom_import.runner_codec import fields_by_collection, record_payload
from process.custom_import.runner_types import CandidateRegistry, CandidateRunnerError, LeaseAuthorityLost
from process.custom_import.snowflake_python import _parquet_reader_with_metrics
from tests.test_custom_import_snowflake_bundle import _ROOT_SCHEMA
from tests.test_custom_import_snowflake_bundle import _definition as _integer_definition
from tests.test_custom_import_snowflake_capture import _policy
from tests.test_custom_import_snowflake_shared_capture import _shared_definition


def _request(**changes):
    return staging.SourceBuildRequest(
        **(
            dict(
                dataset_id=1,
                definition_revision_id=2,
                schema_revision_id=3,
                execution_id=4,
                lease_token=b"synthetic-build-owner",
                fence=1,
                definition=_shared_definition(),
                expected_base_generation_id=None,
                expected_pointer_version=0,
                complete_scope=False,
                page_row_limit=2,
                page_byte_limit=16_384,
                statement_timeout_ms=1_000,
                build_deadline_at=dt.datetime(2030, 1, 1, tzinfo=dt.UTC),
                lease_seconds=120,
            )
            | changes
        )
    )


def _root(npi="1003000126", score=Decimal("1.00")):
    return dict(npi=npi, score=score, enabled=True)


def _child(npi="1003000126", key="a", amount=Decimal("2.00")):
    return dict(detail_npi=npi, detail_id=key, amount=amount)


@pytest.mark.parametrize(
    "changes",
    [
        {"fence": True},
        {"page_row_limit": 257},
        {"page_byte_limit": 268_435_457},
        {"statement_timeout_ms": 0},
        {"lease_seconds": 3_601},
        {"expected_pointer_version": 1},
        {"expected_base_generation_id": 5},
        {"build_deadline_at": dt.datetime(2030, 1, 1)},
        {"lease_token": bytearray(b"mutable")},
    ],
)
def test_request_requires_explicit_exact_immutable_bounds(changes):
    with pytest.raises(ValueError):
        _request(**changes)


def test_request_hides_token_and_rejects_noncanonical_definition_object():
    request = _request()
    assert "synthetic-build-owner" not in repr(request)
    with pytest.raises(ValueError, match="canonical"):
        _request(definition=replace(request.definition, root_logical_key=("enabled",)))


def test_source_payload_and_validation_use_original_definition_order():
    document = json.loads(_shared_definition().canonical)
    document["schema"]["root"]["fields"].reverse()
    document["schema"]["children"][0]["fields"].reverse()
    request = _request(definition=CustomImportDefinition.from_mapping(document))
    root_stream, child_stream = request.definition.source_streams
    root = staging._prepare_row(request, root_stream, _root())
    child = staging._prepare_row(request, child_stream, _child())
    assert root.payload == record_payload(request.definition.root_fields, _root())
    assert child.payload == record_payload(fields_by_collection(request.definition)["details"], _child())
    invalid_by_field = dict(npi="not-an-entity", score=None, enabled=None)
    expected = assemble_root_families(request.definition, [invalid_by_field], {"details": []})
    assert staging._prepare_row(request, root_stream, invalid_by_field).rejection.code == expected.rejections[0].code


def test_invalid_root_retains_raw_presence_and_typed_rejection_identity():
    request = _request()
    root_stream = request.definition.source_streams[0]
    row = staging._prepare_row(request, root_stream, _root(score=None))
    assert row.rejection.code == "required_field_null"
    assert row.raw_key is not None and row.typed_key is not None
    assert row.payload is None and row.payload_hash is None
    assert row.rejection.canonical_root_key == row.typed_key[0]
    assert row.byte_count == sum(
        len(value.encode())
        for value in (
            row.raw_key[0],
            row.typed_key[0],
            row.rejection.canonical_root_key,
            row.rejection.canonical_evidence,
        )
    )
    missing = staging._prepare_row(request, root_stream, _root(npi=None))
    assert missing.rejection.code == "root_key_missing"
    assert missing.raw_key is None and missing.typed_key is None


def test_raw_decimal_parent_strings_stay_distinct_before_typed_key_normalization():
    document = json.loads(_shared_definition().canonical)
    document["schema"]["root"]["logical_key"] = ["score"]
    document["schema"]["children"][0]["parent_key"] = [{"child": "amount", "root": "score"}]
    request = _request(definition=CustomImportDefinition.from_mapping(document))
    root_stream, child_stream = request.definition.source_streams
    root = staging._prepare_row(request, root_stream, _root(score="1.0"))
    child = staging._prepare_row(request, child_stream, _child(amount="1.00"))
    assert root.raw_key != child.raw_key
    assert root.typed_key == child.typed_key


def test_child_byte_accounting_counts_both_copies_of_parent_identity():
    request = _request()
    row = staging._prepare_row(request, request.definition.source_streams[1], _child())
    assert row.byte_count == sum(
        len(value.encode())
        for value in (
            row.raw_key[0],
            row.typed_key[0],
            row.typed_key[0],
            row.child_key,
            row.payload,
        )
    )
    with pytest.raises(CandidateRunnerError, match="page byte"):
        staging._prepare_row(
            replace(request, page_byte_limit=row.byte_count - 1), request.definition.source_streams[1], _child()
        )


def test_local_child_validation_does_not_infer_global_parent_presence():
    request = _request()
    stream = request.definition.source_streams[1]
    valid_orphan = staging._prepare_row(request, stream, _child(npi="1234567893"))
    assert valid_orphan.rejection is None and valid_orphan.payload is not None
    invalid_orphan = staging._prepare_row(request, stream, _child(npi="1234567893", amount=None))
    assert invalid_orphan.rejection.code == "required_field_null"
    assert invalid_orphan.raw_key is not None


async def test_statement_budget_is_strictly_below_remaining_window(monkeypatch):
    session = SimpleNamespace(info={staging._WINDOW: staging._PageWindow(12.0, 9_000)})
    timeout = AsyncMock()
    monkeypatch.setattr(staging, "_set_timeout", timeout)
    monkeypatch.setattr(staging.time, "monotonic", lambda: 10.0)
    await staging._prepare_statement(session)
    timeout.assert_awaited_once_with(session, 1_000)
    monkeypatch.setattr(staging.time, "monotonic", lambda: 12.0)
    with pytest.raises(LeaseAuthorityLost):
        await staging._prepare_statement(session)
    assert timeout.await_count == 1


async def test_sql_calls_use_the_translated_model_schema(monkeypatch):
    monkeypatch.setattr(staging, "_prepare_statement", AsyncMock())
    monkeypatch.setattr(staging.CustomImportBuildAttempt.__table__, "schema", "configured_schema")
    connection = SimpleNamespace(
        dialect=dialect(),
        sync_connection=SimpleNamespace(
            get_execution_options=lambda: {
                "schema_translate_map": {"configured_schema": "mapped_schema"},
            }
        ),
    )
    session = SimpleNamespace(connection=AsyncMock(return_value=connection), execute=AsyncMock())
    await staging._call(session, "freeze_custom_import_build_source", (("bigint", 7),))
    statement, parameters = session.execute.await_args.args
    assert str(statement) == "SELECT * FROM mapped_schema.freeze_custom_import_build_source(CAST(:p0 AS bigint))"
    assert parameters == {"p0": 7}


@pytest.mark.parametrize("primary_kind", ["cancel", "error"])
async def test_cleanup_drains_repeated_cancellation_without_losing_primary(primary_kind):
    entered, release = asyncio.Event(), asyncio.Event()
    primary = asyncio.CancelledError("first") if primary_kind == "cancel" else ValueError("first")

    async def close():
        entered.set()
        await release.wait()
        raise RuntimeError("late cleanup failure")

    task = asyncio.create_task(staging._drain_cleanup(close(), primary))
    await entered.wait()
    task.cancel("second")
    await asyncio.sleep(0)
    task.cancel("third")
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    assert await task is primary
    assert isinstance(primary.__cause__, RuntimeError)
    assert any("cleanup also failed" in note for note in primary.__notes__)


@pytest.mark.parametrize("late_error", ["iterator", "close"])
async def test_stream_finality_waits_for_real_outer_eof_and_cleanup(monkeypatch, late_error):
    request = _request()
    registry = CandidateRegistry({"details": 1}, {"root_source": 1, "detail_source": 2}, 1)
    finish = AsyncMock()
    monkeypatch.setattr(staging, "_finish_part", finish)
    monkeypatch.setattr(staging, "_replay_part", AsyncMock(return_value=1))
    monkeypatch.setattr(staging, "_set_timeout", AsyncMock())

    @asynccontextmanager
    async def transaction():
        yield

    @asynccontextmanager
    async def sessions():
        yield SimpleNamespace(begin=transaction)

    @asynccontextmanager
    async def open_parts(*_args, **_kwargs):
        async def records():
            for stream_id in registry.stream_slots:
                yield SimpleNamespace(
                    ordinal=1,
                    receipt=SimpleNamespace(
                        stream_id=stream_id,
                        canonical_manifest='{"part_count":1}',
                    ),
                )
            if late_error == "iterator":
                raise RuntimeError("late iterator failure")

        yield records()
        if late_error == "close":
            raise RuntimeError("late close failure")

    monkeypatch.setattr(staging, "open_segmented_parquet_parts", open_parts)
    with pytest.raises(RuntimeError, match="late"):
        await staging._replay_source(sessions, request, registry, 5, 6, None, {1: (1, 0), 2: (1, 0)})
    finish.assert_not_awaited()


async def test_first_cleanup_failure_survives_later_session_failure():
    first = ConnectionError("commit result uncertain")
    last = RuntimeError("session cleanup failed")
    transaction = SimpleNamespace(__aexit__=AsyncMock(side_effect=first))
    session = SimpleNamespace(__aexit__=AsyncMock(side_effect=last))
    primary = await staging._close_contexts([session, transaction], None)
    assert primary is first and primary.__cause__ is last
    transaction.__aexit__.assert_awaited_once_with(None, None, None)
    assert session.__aexit__.await_args.args[0] is ConnectionError
    assert session.__aexit__.await_args.args[1] is first


async def test_part_resume_compares_committed_prefix_then_appends_bounded_suffix(monkeypatch):
    request = _request()
    stream = request.definition.source_streams[0]
    registry = CandidateRegistry({"details": 1}, {"root_source": 1, "detail_source": 2}, 1)
    records = [_root(score=Decimal(index)) for index in range(5)]
    comparison, append = AsyncMock(), AsyncMock()
    monkeypatch.setattr(staging, "_compare_committed_page", comparison)
    monkeypatch.setattr(staging, "_store_page", append)
    monkeypatch.setattr(staging, "_validate_replay_partition_schema", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(staging, "_aggregate_parquet_arrow_bytes", lambda *_args, **_kwargs: 30)
    monkeypatch.setattr(
        staging, "iter_records", lambda *_args, **_kwargs: (SimpleNamespace(values=row) for row in records)
    )
    part = SimpleNamespace(capture=object(), ordinal=2, record_count=5, arrow_byte_count=30)
    context = staging._StreamContext(request, registry, 5, stream)
    assert await staging._replay_part(None, context, part, _policy(), (2, 3, 7)) == 5
    assert [
        (call.args[2].first_row, call.args[2].first_source, len(call.args[2].records))
        for call in comparison.await_args_list
    ] == [
        (0, 7, 2),
        (2, 9, 1),
    ]
    assert [
        (call.args[2].first_row, call.args[2].first_source, len(call.args[2].records))
        for call in append.await_args_list
    ] == [(3, 10, 2)]


def _nullable_integer_part():
    definition = _integer_definition(score_nullable=True)
    columns = tuple(replace(column, nullable=True) if column.field_id == "score" else column for column in _ROOT_SCHEMA)
    reader, landing_bytes = _parquet_reader_with_metrics([("1003000126", 7, True)], columns)
    try:
        capture = capture_stream(reader, definition.source_streams[0], source_snapshot_token="synthetic")
    finally:
        reader.close()
    assert landing_bytes == 23
    return definition, SimpleNamespace(
        capture=capture,
        ordinal=1,
        record_count=1,
        arrow_byte_count=landing_bytes,
        receipt=SimpleNamespace(stream_id="root_source", canonical_manifest='{"part_count":1}'),
    )


async def test_nullable_arrow_allocation_can_differ_from_bounded_replay(monkeypatch):
    definition, part = _nullable_integer_part()
    request = _request(definition=definition)
    registry = CandidateRegistry({"details": 1}, {"root_source": 1, "detail_source": 2}, 1)
    policy = _policy(maximum_part_arrow_bytes=24)
    actual = staging._aggregate_parquet_arrow_bytes(part.capture, limits=policy.part_limits, decoded_bytes=0)
    assert actual == 24 and actual != part.arrow_byte_count
    append = AsyncMock()
    monkeypatch.setattr(staging, "_store_page", append)
    context = staging._StreamContext(request, registry, 5, definition.source_streams[0])
    assert await staging._replay_part(None, context, part, policy, (1, 0, 0)) == 1
    append.assert_awaited_once()


async def test_actual_arrow_replay_over_cap_closes_without_stream_eof(monkeypatch):
    definition, part = _nullable_integer_part()
    request = _request(definition=definition)
    registry = CandidateRegistry({"details": 1}, {"root_source": 1, "detail_source": 2}, 1)
    closed_contexts = []
    finish = AsyncMock()
    monkeypatch.setattr(staging, "_store_page", AsyncMock())
    monkeypatch.setattr(staging, "_finish_part", finish)
    monkeypatch.setattr(staging, "_set_timeout", AsyncMock())

    @asynccontextmanager
    async def transaction():
        try:
            yield
        finally:
            closed_contexts.append("transaction")

    @asynccontextmanager
    async def sessions():
        try:
            yield SimpleNamespace(begin=transaction)
        finally:
            closed_contexts.append("session")

    @asynccontextmanager
    async def open_parts(*_args, **_kwargs):
        async def parts():
            yield part

        try:
            yield parts()
        finally:
            closed_contexts.append("reader")

    monkeypatch.setattr(staging, "open_segmented_parquet_parts", open_parts)
    with pytest.raises(CandidateRunnerError, match="Arrow bytes exceed"):
        await staging._replay_source(
            sessions, request, registry, 5, 6, _policy(maximum_part_arrow_bytes=23), {1: (1, 0), 2: (1, 0)}
        )
    assert closed_contexts == ["reader", "transaction", "session"]
    finish.assert_not_awaited()
