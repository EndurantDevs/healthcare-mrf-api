# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Ordered SOURCE encoding and cleanup, with a portable native-call double."""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import multiprocessing
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process.custom_import import build_source as staging
from process.custom_import import bulk_page_codec as codec
from process.custom_import import capture, runner_codec
from process.custom_import import source_batch as service
from process.custom_import import source_preparation as preparation
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.runner_types import CandidateRunnerError, LeaseAuthorityLost
from tests.test_custom_import_build_source import _child, _root
from tests.test_custom_import_build_source_bulk import _bulk_context
from tests.test_custom_import_snowflake_capture import _policy
from tests.test_custom_import_source_batch import _CURSOR, _PERMIT


def _reference_encoder(layout, cell_rows):
    child, fields, root_key, child_key = layout

    def document(contract, domain, selected, cells):
        encoded_fields = []
        for name, index in selected:
            state, value = cells[index]
            encoded_value_by_key = {"state": state}
            if state != "missing":
                encoded_value_by_key["type"] = fields[index][1]
            if state == "value":
                encoded_value_by_key["value"] = value
            encoded_fields.append({"field": name, "value": encoded_value_by_key})
        canonical = runner_codec.canonical({"contract": contract, "fields": encoded_fields})
        return canonical, runner_codec.digest_text(domain, canonical)

    encoded_rows = []
    for cells in cell_rows:
        root = document("custom-import-key/v1", "root-key", root_key, cells)
        payload_pair = document(
            "custom-import-record/v1",
            "child-payload" if child else "root-payload",
            tuple((name, index) for index, (name, _kind) in enumerate(fields)),
            cells,
        )
        key = (
            document(
                "custom-import-key/v1", "child-key", tuple((fields[index][0], index) for index in child_key), cells
            )
            if child
            else (None, None)
        )
        encoded_rows.append((*root, *payload_pair, *key))
    return encoded_rows


def _native_double(monkeypatch, encoder=_reference_encoder):
    calls = []

    def checked(layout, rows):
        assert type(layout) is tuple and type(rows) is list
        assert 1 <= len(rows) <= preparation.MAX_ROWS
        assert (
            preparation._layout_bytes(layout)
            + sum(64 + sum(64 + preparation._value_bytes(cell) for cell in cells) for cells in rows)
            <= preparation.MAX_INPUT_BYTES
        )
        assert sum(preparation._output_bound(layout, cells) for cells in rows) <= preparation.MAX_OUTPUT_BYTES
        calls.append((layout, rows))
        return encoder(layout, rows)

    async def direct(operation, *args):
        return operation(*args)

    monkeypatch.setattr(preparation, "native_encoder", lambda: checked)
    monkeypatch.setattr(preparation.asyncio, "to_thread", direct)
    return calls


def _decoded(monkeypatch, values, *, read_failure=None, cleanup=None):
    events = []

    def records(*_args, _cleanup_failures=None, **_kwargs):
        try:
            for values_by_field in values:
                yield SimpleNamespace(values=values_by_field)
            if read_failure is not None:
                raise read_failure
        finally:
            events.append("closed")
            if cleanup is not None:
                if _cleanup_failures is not None:
                    _cleanup_failures.append(cleanup)
                raise cleanup

    monkeypatch.setattr(staging, "iter_records", records)
    return events


def _part(count):
    return SimpleNamespace(ordinal=1, record_count=count, capture=object())


async def _pages(context, part, *, serial=False):
    iterator = (
        staging._iter_source_pages(context, part, _policy(), (1, 0, 0))
        if serial
        else staging._source_pages(context, part, _policy(), (1, 0, 0))
    )
    pages = []
    try:
        while (page := await staging._next_source_page(iterator)) is not None:
            pages.append(page)
    finally:
        failure = await staging._close_source_pages(iterator, None)
        if failure is not None:
            raise failure
    return tuple(pages)


def _landing(context, pages):
    return codec.encode_landing_batch(context, pages, batch_id=UUID(int=7), first_pack_ordinal=3)


@pytest.mark.parametrize(
    "stream,values",
    [
        (0, [_root(), _root(score=None), _root(npi=None), _root(npi="invalid")]),
        (1, [_child(key='Ω\n"\\'), _child(amount=None), _child(npi=None), _child(key=None)]),
    ],
)
async def test_native_and_serial_preserve_complete_landing_bytes_hashes_and_rejections(monkeypatch, stream, values):
    context = _bulk_context(stream)
    _decoded(monkeypatch, values)
    calls = _native_double(monkeypatch)
    expected = await _pages(context, _part(len(values)), serial=True)
    actual = await _pages(context, _part(len(values)))
    assert calls and _landing(context, actual) == _landing(context, expected)


@pytest.mark.parametrize("rich", [False, True])
async def test_thousand_row_parts_reach_multiple_bounded_native_batches(monkeypatch, rich):
    context = _bulk_context(1)
    extra_values_by_field = {}
    if rich:
        document = json.loads(context.request.definition.canonical)
        for slot in range(7, 24):
            name = f"extra_{slot}"
            document["schema"]["children"][0]["fields"].append(
                {"id": name, "slot": slot, "type": "string", "nullable": False}
            )
            extra_values_by_field[name] = f"Ω synthetic value {slot}"
        definition = CustomImportDefinition.from_mapping(document)
        context = replace(
            context, request=replace(context.request, definition=definition), stream=definition.source_streams[1]
        )
    values = tuple(_child(key=str(index)) | extra_values_by_field for index in range(1000))
    _decoded(monkeypatch, values)
    calls = _native_double(monkeypatch)
    expected = await _pages(context, _part(1000), serial=True)
    actual = await _pages(context, _part(1000))
    assert sum(len(rows) for _layout, rows in calls) == 1000
    assert 1 < len(calls) < 1000
    assert _landing(context, actual) == _landing(context, expected)


async def test_accepted_native_rows_do_not_run_serial_document_serializers(monkeypatch):
    context = _bulk_context(1)
    _decoded(monkeypatch, [_child()] * 4)
    calls = _native_double(monkeypatch)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("accepted native rows must not be serialized twice")

    for name in ("record_payload", "child_key_document", "root_key_evidence_from_tuple", "digest_text"):
        monkeypatch.setattr(staging, name, forbidden)
    actual = await _pages(context, _part(4))
    assert len(calls) == 1 and len(actual[0].records) == 4
    # The unchanged landing validator still parses every generated payload.
    decoded_calls = []
    original = runner_codec.payload_values

    def checked(*args, **kwargs):
        decoded_calls.append(args)
        return original(*args, **kwargs)

    monkeypatch.setattr(runner_codec, "payload_values", checked)
    _landing(context, actual)
    assert len(decoded_calls) == 4


@pytest.mark.parametrize("value", ["\ud800", "Ω" * 4097, 2**100, Decimal("1e1000"), Decimal("NaN")])
def test_wide_or_malformed_scalars_are_serial_eligibility_not_new_validation(value):
    assert not preparation._has_native_scalar_values({"value": value})


def test_scalar_subclasses_do_not_cross_the_native_boundary():
    class Text(str):
        pass

    class Integer(int):
        pass

    assert not preparation._has_native_scalar_values({"value": Text("safe")})
    assert not preparation._has_native_scalar_values({"value": Integer(1)})


async def test_large_valid_nonhot_payload_falls_back_before_native_without_narrowing_domain(monkeypatch):
    context = _bulk_context(1)
    document = json.loads(context.request.definition.canonical)
    field = next(field for field in document["schema"]["children"][0]["fields"] if field["id"] == "detail_id")
    field.pop("projection_slot", None)
    definition = CustomImportDefinition.from_mapping(document)
    context = replace(
        context,
        request=replace(context.request, definition=definition, page_byte_limit=100_000),
        stream=definition.source_streams[1],
    )
    _decoded(monkeypatch, [_child(key="x" * 5000)])
    calls = _native_double(monkeypatch)
    expected = await _pages(context, _part(1), serial=True)
    actual = await _pages(context, _part(1))
    assert not calls and _landing(context, actual) == _landing(context, expected)


async def test_wide_valid_definitions_remain_on_the_complete_serial_path(monkeypatch):
    context = _bulk_context(1)
    document = json.loads(context.request.definition.canonical)
    extra_values_by_field = {}
    for slot in range(7, 69):
        name = f"extra_{slot}"
        document["schema"]["children"][0]["fields"].append(
            {"id": name, "slot": slot, "type": "string", "nullable": False}
        )
        extra_values_by_field[name] = "synthetic"
    definition = CustomImportDefinition.from_mapping(document)
    context = replace(
        context, request=replace(context.request, definition=definition), stream=definition.source_streams[1]
    )
    _decoded(monkeypatch, [_child() | extra_values_by_field])
    calls = _native_double(monkeypatch)
    expected = await _pages(context, _part(1), serial=True)
    assert _landing(context, await _pages(context, _part(1))) == _landing(context, expected)
    assert not calls and preparation.native_layout(definition, context.stream) is None


async def test_wide_integer_values_preselect_serial_before_native(monkeypatch):
    context = _bulk_context(1)
    document = json.loads(context.request.definition.canonical)
    field = next(field for field in document["schema"]["children"][0]["fields"] if field["id"] == "amount")
    field["type"] = "integer"
    field.pop("projection_slot", None)
    definition = CustomImportDefinition.from_mapping(document)
    context = replace(
        context, request=replace(context.request, definition=definition), stream=definition.source_streams[1]
    )
    _decoded(monkeypatch, [_child(amount=2**100)])
    calls = _native_double(monkeypatch)
    expected = await _pages(context, _part(1), serial=True)
    assert _landing(context, await _pages(context, _part(1))) == _landing(context, expected)
    assert not calls


async def test_repeated_parent_source_indices_preserve_distinct_root_fields(monkeypatch):
    context = _bulk_context(1)
    document = json.loads(context.request.definition.canonical)
    document["schema"]["root"]["fields"].append({"id": "other_npi", "slot": 7, "type": "string", "nullable": False})
    document["schema"]["root"]["logical_key"].append("other_npi")
    document["schema"]["children"][0]["parent_key"].append({"child": "detail_npi", "root": "other_npi"})
    definition = CustomImportDefinition.from_mapping(document)
    context = replace(
        context, request=replace(context.request, definition=definition), stream=definition.source_streams[1]
    )
    _decoded(monkeypatch, [_child()])
    calls = _native_double(monkeypatch)
    expected = await _pages(context, _part(1), serial=True)
    assert _landing(context, await _pages(context, _part(1))) == _landing(context, expected)
    assert calls[0][0][2] == (("npi", 0), ("other_npi", 0))


async def test_raw_decimal_key_spelling_stays_distinct_from_native_typed_identity(monkeypatch):
    context = _bulk_context(1)
    document = json.loads(context.request.definition.canonical)
    document["schema"]["root"]["logical_key"] = ["score"]
    document["schema"]["children"][0]["parent_key"] = [{"child": "amount", "root": "score"}]
    definition = CustomImportDefinition.from_mapping(document)
    context = replace(
        context, request=replace(context.request, definition=definition), stream=definition.source_streams[1]
    )
    _decoded(monkeypatch, [_child(amount=amount) for amount in ("1.0", "1.00", Decimal("1.000"), Decimal("-0.00"))])
    calls = _native_double(monkeypatch)
    expected = await _pages(context, _part(4), serial=True)
    actual = await _pages(context, _part(4))
    assert calls and _landing(context, actual) == _landing(context, expected)
    assert actual[0].records[0].typed_key == actual[0].records[1].typed_key
    assert actual[0].records[0].raw_key != actual[0].records[1].raw_key


def test_root_key_type_mismatch_is_not_native_eligible():
    definition = _bulk_context().request.definition
    changed_fields = tuple(
        replace(field, value_type="integer") if field.field_id == "detail_npi" else field
        for field in definition.child_fields
    )
    mismatched = replace(definition, child_fields=changed_fields)
    assert preparation.native_layout(mismatched, definition.source_streams[1]) is None


async def test_wide_root_keys_keep_the_serial_definition_domain(monkeypatch):
    context = _bulk_context()
    document = json.loads(context.request.definition.canonical)
    for slot, name in enumerate(("key_a", "key_b", "key_c"), start=7):
        document["schema"]["root"]["fields"].append({"id": name, "slot": slot, "type": "string", "nullable": False})
        document["schema"]["root"]["logical_key"].append(name)
        document["schema"]["children"][0]["parent_key"].append({"child": "detail_npi", "root": name})
    definition = CustomImportDefinition.from_mapping(document)
    context = replace(
        context, request=replace(context.request, definition=definition), stream=definition.source_streams[0]
    )
    _decoded(monkeypatch, [_root() | {name: "Ω" for name in ("key_a", "key_b", "key_c")}])
    calls = _native_double(monkeypatch)
    expected = await _pages(context, _part(1), serial=True)
    assert _landing(context, await _pages(context, _part(1))) == _landing(context, expected)
    assert not calls and preparation.native_layout(definition, context.stream) is None


@pytest.mark.parametrize("serial", [False, True])
async def test_failed_lookahead_does_not_yield_a_previous_full_page(monkeypatch, serial):
    context = replace(_bulk_context(), request=replace(_bulk_context().request, page_row_limit=2))
    _decoded(monkeypatch, [_root(), _root(), {"wrong": 1}], read_failure=OSError("later read"))
    _native_double(monkeypatch)
    pages = (
        staging._iter_source_pages(context, _part(3), _policy(), (1, 0, 0))
        if serial
        else staging._source_pages(context, _part(3), _policy(), (1, 0, 0))
    )
    with pytest.raises(CandidateRunnerError, match="replayed fields"):
        await staging._next_source_page(pages)


async def test_native_failure_precedes_later_python_failure_without_serial_retry(monkeypatch):
    context = _bulk_context()
    _decoded(monkeypatch, [_root(), {"wrong": 1}])
    failure = ValueError("synthetic native rejection")

    def fail(*_args):
        raise failure

    calls = _native_double(monkeypatch, fail)
    serial = AsyncMock(side_effect=AssertionError("native rejection must not retry"))
    monkeypatch.setattr(staging, "_prepare_decoded_row", serial)
    with pytest.raises(ValueError) as caught:
        await _pages(context, _part(2))
    assert caught.value is failure and len(calls) == 1 and serial.call_count == 0


@pytest.mark.parametrize("serial", [False, True])
async def test_overflow_row_preserves_the_same_successful_page_prefix(monkeypatch, serial):
    context = _bulk_context(1)
    ordinary = staging._prepare_row(context.request, context.stream, _child())
    context = replace(
        context, request=replace(context.request, page_row_limit=2, page_byte_limit=ordinary.byte_count * 2)
    )
    _decoded(monkeypatch, [_child(), _child(), _child(), _child(key="x" * 1800)])
    _native_double(monkeypatch)
    pages = (
        staging._iter_source_pages(context, _part(4), _policy(), (1, 0, 0))
        if serial
        else staging._source_pages(context, _part(4), _policy(), (1, 0, 0))
    )
    assert len((await staging._next_source_page(pages)).records) == 2
    with pytest.raises(CandidateRunnerError, match="page byte limit"):
        await staging._next_source_page(pages)


async def test_cancellation_drains_the_native_call_before_decoder_cleanup(monkeypatch):
    context = _bulk_context()
    events = _decoded(monkeypatch, [_root()] * 100)
    _native_double(monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()

    async def suspended(operation, *args):
        entered.set()
        await release.wait()
        events.append("native-settled")
        return operation(*args)

    monkeypatch.setattr(preparation.asyncio, "to_thread", suspended)
    task = asyncio.create_task(_pages(context, _part(100)))
    await entered.wait()
    task.cancel("first")
    await asyncio.sleep(0)
    task.cancel("second")
    await asyncio.sleep(0)
    assert not task.done() and events == []
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert events == ["native-settled", "closed"]


async def _exercise_source_route(monkeypatch, hosted, encoder):
    context = _bulk_context()
    _decoded(monkeypatch, [_root()] * 4)
    calls = _native_double(monkeypatch, encoder)
    monkeypatch.setattr(multiprocessing.current_process(), "daemon", True)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("a daemon SOURCE worker cannot create a child process")

    monkeypatch.setattr(concurrent.futures, "ProcessPoolExecutor", forbidden)
    monkeypatch.setattr(multiprocessing, "get_context", forbidden)
    if hosted:
        monkeypatch.setattr(service, "_verify_part", lambda *_args: None)
        monkeypatch.setattr(
            service, "_load_context", AsyncMock(return_value=(context, _policy(), 9, float("inf"), False))
        )

        async def read_parts(_factory, loaded, policy, _bundle, cursor, permit, deadline):
            pages, eof = await service._prepare_part(None, loaded, _part(4), policy, cursor, permit, deadline)
            assert eof
            return pages, (1,)

        monkeypatch.setattr(service, "_read_parts", read_parts)
        commit = AsyncMock(return_value="complete")
        monkeypatch.setattr(service, "_commit", commit)
        assert (
            await service.serve_source_batch(
                None,
                execution_id=4,
                build_id=11,
                fence=1,
                stream_slot=1,
                expected_cursor=_CURSOR,
                lease_token=context.request.lease_token,
                source_permit=_PERMIT,
            )
            == "complete"
        )
        assert len(commit.await_args.args[2][0].records) == 4
    else:
        monkeypatch.setattr(staging, "_validate_replay_partition_schema", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(staging, "_aggregate_parquet_arrow_bytes", lambda *_args, **_kwargs: 0)
        store = AsyncMock()
        assert await staging._replay_part(None, context, _part(4), _policy(), (1, 0, 0), store_page=store) == 4
        assert len(store.await_args.args[2].records) == 4
    assert calls and sum(len(cell_rows) for _layout, cell_rows in calls) == 4


@pytest.mark.parametrize("hosted", [False, True])
async def test_both_normal_source_routes_reach_native_without_process_creation(monkeypatch, hosted):
    await _exercise_source_route(monkeypatch, hosted, _reference_encoder)


@pytest.mark.parametrize("native", [False, True])
async def test_hosted_soft_stop_cleanup_failure_prevents_commit(monkeypatch, native):
    context = replace(_bulk_context(), request=replace(_bulk_context().request, page_row_limit=2))
    cleanup = OSError("synthetic decoder close")
    _decoded(monkeypatch, [_root()] * 200, cleanup=cleanup)
    calls = _native_double(monkeypatch)
    if not native:
        monkeypatch.setattr(preparation, "native_encoder", lambda: None)
    monkeypatch.setattr(service, "_verify_part", lambda *_args: None)
    monkeypatch.setattr(service, "_load_context", AsyncMock(return_value=(context, _policy(), 9, float("inf"), False)))

    async def read_parts(_factory, loaded, policy, _bundle, cursor, permit, deadline):
        return await service._prepare_part(
            None, loaded, _part(200), policy, cursor, permit, deadline, (2, service.MAX_BATCH_BYTES, deadline)
        )

    monkeypatch.setattr(service, "_read_parts", read_parts)
    commit = AsyncMock()
    monkeypatch.setattr(service, "_commit", commit)
    with pytest.raises(OSError) as caught:
        await service.serve_source_batch(
            None,
            execution_id=4,
            build_id=11,
            fence=1,
            stream_slot=1,
            expected_cursor=_CURSOR,
            lease_token=context.request.lease_token,
            source_permit=_PERMIT,
        )
    assert caught.value is cleanup and bool(calls) is native
    commit.assert_not_awaited()


@pytest.mark.parametrize("cursor", [(1, 1, 0), (2, 0, 0)])
def test_replayed_prefix_is_serial_before_capability_selection(monkeypatch, cursor):
    context = _bulk_context()
    _decoded(monkeypatch, [_root()] * 2)

    def forbidden():
        raise AssertionError("a resumed prefix must stay serial")

    monkeypatch.setattr(preparation, "native_encoder", forbidden)
    pages = staging._source_pages(context, _part(2), _policy(), cursor)
    assert not hasattr(pages, "__anext__")
    assert sum(len(page.records) for page in pages) == 2


@pytest.mark.parametrize("native", [False, True])
@pytest.mark.parametrize("count", [3, 200])
@pytest.mark.parametrize("parent_failure", [False, True])
async def test_soft_stop_or_parent_error_retains_exact_cleanup_failure(monkeypatch, native, count, parent_failure):
    context = replace(_bulk_context(), request=replace(_bulk_context().request, page_row_limit=2))
    cleanup = OSError("synthetic close failure")
    _decoded(monkeypatch, [_root()] * count, cleanup=cleanup)
    _native_double(monkeypatch)
    if not native:
        monkeypatch.setattr(preparation, "native_encoder", lambda: None)
    pages = staging._source_pages(context, _part(count), _policy(), (1, 0, 0))
    assert len((await staging._next_source_page(pages)).records) == 2
    primary = LeaseAuthorityLost("synthetic deadline") if parent_failure else None
    failure = await staging._close_source_pages(pages, primary)
    if parent_failure:
        assert failure is primary and primary.__cause__ is cleanup
        assert primary.__notes__ == ["source decoder cleanup also failed: OSError"]
    else:
        assert failure is cleanup and failure.args == ("synthetic close failure",)


@pytest.mark.parametrize("native", [False, True])
@pytest.mark.parametrize("count", [3, 200])
async def test_semantic_error_merges_prefetched_or_live_cleanup_once(monkeypatch, native, count):
    cleanup = OSError("synthetic close failure")
    _decoded(monkeypatch, [{"wrong": 1}, *([_root()] * (count - 1))], cleanup=cleanup)
    _native_double(monkeypatch)
    if not native:
        monkeypatch.setattr(preparation, "native_encoder", lambda: None)
    with pytest.raises(CandidateRunnerError, match="replayed fields") as caught:
        await _pages(_bulk_context(), _part(count))
    assert caught.value.__cause__ is cleanup
    assert caught.value.__notes__ == ["source decoder cleanup also failed: OSError"]


@pytest.mark.parametrize("native", [False, True])
async def test_soft_stop_discards_read_fault_but_keeps_cleanup_error(monkeypatch, native):
    context = replace(_bulk_context(), request=replace(_bulk_context().request, page_row_limit=2))
    _decoded(monkeypatch, [_root()] * 3, read_failure=OSError("synthetic read"))
    _native_double(monkeypatch)
    if not native:
        monkeypatch.setattr(preparation, "native_encoder", lambda: None)
    pages = staging._source_pages(context, _part(3), _policy(), (1, 0, 0))
    assert len((await staging._next_source_page(pages)).records) == 2
    assert await staging._close_source_pages(pages, None) is None
    cleanup = OSError("synthetic EOF close")
    _decoded(monkeypatch, [_root()] * 2, cleanup=cleanup)
    with pytest.raises(OSError) as caught:
        await _pages(context, _part(2))
    assert caught.value is cleanup and cleanup.__cause__ is None


@pytest.mark.parametrize("observed", [False, True])
@pytest.mark.parametrize("failed_close", ["reader", "buffer"])
def test_parquet_finally_marker_preserves_type_context_and_close_order(monkeypatch, observed, failed_close):
    events, failures = [], []
    cleanup, body = OSError("synthetic close"), RuntimeError("synthetic body")

    def close(name):
        events.append(name)
        if name == failed_close:
            raise cleanup

    monkeypatch.setattr(capture.pa, "BufferReader", lambda *_: SimpleNamespace(close=lambda: close("buffer")))
    monkeypatch.setattr(
        capture.pq, "ParquetFile", lambda *_args, **_kwargs: SimpleNamespace(close=lambda: close("reader"))
    )
    with pytest.raises(OSError) as caught:
        with capture._open_parquet_reader(
            b"synthetic", _policy().part_limits, _cleanup_failures=failures if observed else None
        ):
            raise body
    assert caught.value is cleanup and cleanup.__context__ is body
    assert failures == ([cleanup] if observed else [])
    assert events == (["reader"] if failed_close == "reader" else ["reader", "buffer"])


@pytest.mark.parametrize("serial", [False, True])
async def test_read_then_cleanup_failure_keeps_existing_cleanup_primary(monkeypatch, serial):
    read, cleanup = RuntimeError("synthetic read"), OSError("synthetic close")
    _decoded(monkeypatch, [_root()] * 2, read_failure=read, cleanup=cleanup)
    _native_double(monkeypatch)
    with pytest.raises(OSError) as caught:
        await _pages(_bulk_context(), _part(2), serial=serial)
    assert caught.value is cleanup and cleanup.__context__ is read


@pytest.mark.parametrize("serial", [False, True])
async def test_final_page_is_not_a_sealed_count_or_eof_proof(monkeypatch, serial):
    context = replace(_bulk_context(), request=replace(_bulk_context().request, page_row_limit=2))
    _decoded(monkeypatch, [_root()] * 3)
    _native_double(monkeypatch)
    pages = (
        staging._iter_source_pages(context, _part(4), _policy(), (1, 0, 0))
        if serial
        else staging._source_pages(context, _part(4), _policy(), (1, 0, 0))
    )
    assert len((await staging._next_source_page(pages)).records) == 2
    assert len((await staging._next_source_page(pages)).records) == 1
    with pytest.raises(CandidateRunnerError, match="record count differs"):
        await staging._next_source_page(pages)


@pytest.mark.parametrize("value", ["\ud800", 2**100, Decimal("NaN")])
async def test_serial_preselection_preserves_error_or_landing_outcome(monkeypatch, value):
    context = _bulk_context(1)
    _decoded(monkeypatch, [_child(key=value)])
    calls = _native_double(monkeypatch)

    async def outcome(serial):
        try:
            return _landing(context, await _pages(context, _part(1), serial=serial))
        except Exception as error:
            return type(error), error.args, type(error.__cause__)

    assert await outcome(False) == await outcome(True)
    assert not calls


async def test_empty_part_and_missing_capability_never_call_native(monkeypatch):
    context = _bulk_context()
    _decoded(monkeypatch, [])
    calls = _native_double(monkeypatch)
    assert await _pages(context, _part(0)) == () and not calls
    monkeypatch.setattr(preparation, "native_encoder", lambda: None)
    _decoded(monkeypatch, [_root()])
    assert _landing(context, await _pages(context, _part(1))) == _landing(
        context, await _pages(context, _part(1), serial=True)
    )


@pytest.mark.parametrize("result", [[], [(None,) * 6], [("key", b"short", "payload", b"x" * 32, None, None)]])
async def test_malformed_native_results_fail_closed_without_serial_retry(monkeypatch, result):
    _decoded(monkeypatch, [_root()])
    calls = _native_double(monkeypatch, lambda *_args: result)
    with pytest.raises(CandidateRunnerError, match="source serializer"):
        await _pages(_bulk_context(), _part(1))
    assert len(calls) == 1
