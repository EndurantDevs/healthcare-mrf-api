# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Early partition preparation preserves original records and delayed refusals."""

import asyncio
import inspect
import threading
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from process.custom_import import build_source as original
from process.custom_import import family_shard_input as shards
from process.custom_import import family_shard_input_records as records
from process.custom_import import source_column_preparation as columns
from process.custom_import import source_decoded_preparation as decoded_preparation
from process.custom_import.capture import _record_size
from process.custom_import.family_raw_key import RawFamilyKeyError
from process.custom_import.runner_types import CancellationRequested, CandidateRegistry, CandidateRunnerError
from tests.test_custom_import_build_source import _request
from tests.test_custom_import_family_shard_input import _capture_input, _child, _part_source, _root
from tests.test_custom_import_source_decoded_preparation import native_calls as native_calls


def _case(rows=None, **options):
    definition, source, parts = _capture_input(
        rows or {"root_source": [[_root()]], "detail_source": [[_child()]]}, **options
    )
    request = _request(
        dataset_id=source.identity.dataset_id,
        definition_revision_id=source.identity.definition_revision_id,
        schema_revision_id=source.identity.schema_revision_id,
        definition=definition,
    )
    registry = CandidateRegistry({"details": 7}, {"root_source": 1, "detail_source": 2}, 1)
    return (definition, source, parts, request, registry)


async def _partition(tmp_path, case, name, *, native=True, memory=8 * 1024**2, request=None):
    definition, source, parts, actual_request, registry = case
    arguments_by_name = (
        {"preparations": records.partition_preparations(request or actual_request, registry, 17, memory)}
        if native
        else {}
    )
    return await shards.partition_capture(
        _part_source(parts),
        definition=definition,
        source=source,
        root=tmp_path / name,
        register_cleanup=lambda _path: None,
        limits=shards.ShardInputLimits(32 * 1024**2, 4, 1024),
        **arguments_by_name,
    )


def _bytes(partition):
    return tuple(sorted((records.encode_record(record) for record in shards.read_partition(partition))))


def _assert_manifest_equal(left, right):
    left_document, right_document = (shards.manifest_document(left), shards.manifest_document(right))
    assert left_document.pop("owner") == left.owner
    assert right_document.pop("owner") == right.owner and left.owner != right.owner
    assert left_document == right_document


def _host_outcomes(context, batch):
    outcomes = []
    for index in range(batch.num_rows):
        values_by_name = {name: batch.column(column)[index].as_py() for column, name in enumerate(batch.schema.names)}
        transport = _record_size(values_by_name)
        try:
            prepared = original._prepare_decoded_row(
                context.request,
                context.stream,
                original._stream_fields(context.request.definition, context.stream),
                values_by_name,
            )
        except RawFamilyKeyError:
            outcomes.append(("preparation-error", "raw_key_byte_limit", transport))
            continue
        except CandidateRunnerError as error:
            if error.args != ("one source occurrence exceeds the admitted page byte limit",):
                raise
            outcomes.append(("preparation-error", "occurrence_byte_limit", transport))
            continue
        if prepared.rejection is not None:
            raw, typed = (prepared.raw_key or (None, None), prepared.typed_key or (None, None))
            outcomes.append((prepared.rejection.code, *raw, *typed, transport))
        else:
            outcomes.append(
                (
                    *prepared.raw_key,
                    *prepared.typed_key,
                    prepared.payload,
                    prepared.payload_hash,
                    prepared.child_key,
                    prepared.child_hash,
                    prepared.byte_count,
                    transport,
                )
            )
    return outcomes


@pytest.fixture
def host_column_oracle(monkeypatch):
    """A declared host adapter double; genuine Rust coverage is a separate case."""
    batch_by_id, calls, contexts = ([], [], [])
    arrow = decoded_preparation._arrow_batch

    def batch(*args):
        actual = arrow(*args)
        batch_by_id[:] = [actual]
        return actual

    def bind(layout):
        contexts.append(layout)

        def invoke(descriptors, limit):
            calls.append(descriptors[0][3])
            actual_batch = batch_by_id[0]
            context = active_contexts[0]
            return _host_outcomes(context, actual_batch)

        return invoke

    active_contexts = []
    constructor = records.PartitionPreparation.__init__

    def created(self, context, **options):
        constructor(self, context, **options)
        invoke = self.native
        if invoke is not None:

            def current(*args):
                active_contexts[:] = [context]
                return invoke(*args)

            self.native = current

    monkeypatch.setattr(decoded_preparation, "_arrow_batch", batch)
    monkeypatch.setattr(columns, "bind_column_encoder", bind)
    monkeypatch.setattr(records.PartitionPreparation, "__init__", created)
    return calls


@pytest.mark.asyncio
async def test_host_adapter_keeps_exact_partition_bytes_and_rejection_priority(tmp_path, host_column_oracle):
    case = _case(
        {
            "root_source": [[_root(), _root(npi=None), _root(npi="invalid")]],
            "detail_source": [[_child(key='Ω\n\x00"\\'), _child(key=None), _child(amount=None)]],
        }
    )
    plain = await _partition(tmp_path, case, "plain", native=False)
    native = await _partition(tmp_path, case, "adapter")
    try:
        assert _bytes(native) == _bytes(plain) and host_column_oracle == [3, 3]
        _assert_manifest_equal(native, plain)
    finally:
        shards.cleanup_spool(native)
        shards.cleanup_spool(plain)


@pytest.mark.asyncio
@pytest.mark.parametrize("limit_kind", ["raw", "prefix"])
async def test_page_refusal_stays_delayed_and_does_not_duplicate_native_prefix(
    tmp_path, host_column_oracle, monkeypatch, limit_kind
):
    case = _case({"root_source": [[_root(), _root(npi="x" * 1024), _root()]], "detail_source": [[]]})
    context = original._StreamContext(case[3], case[4], 17, case[0].source_streams[0])
    prepared = original._prepare_decoded_row(
        context.request, context.stream, original._stream_fields(case[0], context.stream), _root()
    )
    request = replace(case[3], page_byte_limit=1 if limit_kind == "raw" else prepared.byte_count)
    plain = await _partition(tmp_path, case, "plain", native=False)
    serial_calls, serial = ([], records.prepare_record)

    def observed(*args, **kwargs):
        serial_calls.append(kwargs["source_ordinal"])
        return serial(*args, **kwargs)

    monkeypatch.setattr(records, "prepare_record", observed)
    actual = await _partition(tmp_path, case, "adapter", request=request)
    try:
        assert _bytes(actual) == _bytes(plain) and len(_bytes(actual)) == 3
        assert host_column_oracle == [3]
        assert serial_calls == ([0, 1, 2] if limit_kind == "raw" else [1, 2])
    finally:
        shards.cleanup_spool(actual)
        shards.cleanup_spool(plain)


@pytest.mark.asyncio
async def test_absent_capability_and_small_original_budget_keep_serial_mode(tmp_path, monkeypatch):
    case = _case()
    monkeypatch.setattr(columns, "bind_column_encoder", lambda _layout: None)
    plain = await _partition(tmp_path, case, "plain", native=False)
    absent = await _partition(tmp_path, case, "absent")
    tiny = await _partition(tmp_path, case, "tiny", memory=1)
    try:
        assert _bytes(plain) == _bytes(absent) == _bytes(tiny)
    finally:
        for partition in (plain, absent, tiny):
            shards.cleanup_spool(partition)


@pytest.mark.parametrize("scalar_value", [Decimal("-0.00"), Decimal("1E+40"), datetime(2024, 1, 2, tzinfo=UTC)])
def test_ineligible_scalar_retains_opaque_original_value(monkeypatch, scalar_value):
    from types import SimpleNamespace

    from tests.test_custom_import_build_source_bulk import _bulk_context

    context = _bulk_context(0)
    monkeypatch.setattr(
        columns, "bind_column_encoder", lambda _layout: lambda *_args: pytest.fail("unsupported scalar entered native")
    )
    preparation = records.PartitionPreparation(context, memory_bytes=8 * 1024**2)
    values_by_name = dict(npi="1003000126", score=scalar_value, enabled=True)
    expected = records.prepare_record(
        context.request.definition,
        context.stream,
        values_by_name,
        stream_slot=1,
        collection_slot=0,
        part_ordinal=2,
        row_ordinal=0,
        source_ordinal=79000000,
    )
    actual_records = list(
        records.partition_records(
            context.request.definition,
            context.stream,
            iter([SimpleNamespace(values=values_by_name)]),
            1,
            0,
            2,
            79000000,
            preparation,
        )
    )
    assert len(actual_records) == 1 and records.encode_record(actual_records[0]) == records.encode_record(expected)
    assert dict(actual_records[0].values)["score"] is scalar_value


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["shape", "count", "internal"])
async def test_native_corruption_and_internal_errors_never_fall_back(tmp_path, monkeypatch, damage):

    def bind(_layout):

        def invoke(*_args):
            if damage == "internal":
                raise RuntimeError("native internal failure")
            return [] if damage == "count" else [("invalid",)]

        return invoke

    monkeypatch.setattr(columns, "bind_column_encoder", bind)
    with pytest.raises((CandidateRunnerError, RuntimeError), match="source column|native internal"):
        await _partition(tmp_path, _case(), "failed")
    assert not (tmp_path / "failed").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["build_id", "memory_bytes", "request", "registry"])
async def test_partial_or_swapped_context_refuses_before_root_creation(tmp_path, field):
    definition, source, parts, request, registry = _case()
    arguments_by_name = dict(request=request, registry=registry, build_id=17, memory_bytes=1024**2)
    arguments_by_name[field] = (
        None
        if field in {"build_id", "memory_bytes"}
        else replace(request, dataset_id=request.dataset_id + 1)
        if field == "request"
        else CandidateRegistry({}, {}, 1)
    )
    with pytest.raises(ValueError, match="context"):
        preparations = records.partition_preparations(**arguments_by_name)
        await shards.partition_capture(
            _part_source(parts),
            definition=definition,
            source=source,
            root=tmp_path / "refused",
            register_cleanup=lambda _path: pytest.fail("context created a root"),
            limits=shards.ShardInputLimits(32 * 1024**2, 4, 1024),
            preparations=preparations,
        )
    assert not (tmp_path / "refused").exists()


@pytest.mark.asyncio
async def test_supported_rows_invoke_real_compiled_rust_with_original_bytes(tmp_path, native_calls):
    assert inspect.isbuiltin(columns.native_column_encoder())
    roots = [_root(score=Decimal("7.000")) for _ in range(65)]
    child_rows = [_child(key=f'Ω-{index}\n\x00"\\') for index in range(65)]
    roots[11], child_rows[17] = (_root(npi=None), _child(key=None))
    case = _case(
        {"root_source": [[], roots[:33], roots[33:], []], "detail_source": [[], child_rows[:33], child_rows[33:], []]},
        decimal_scores=True,
    )
    plain = await _partition(tmp_path, case, "plain", native=False)
    actual = await _partition(tmp_path, case, "real-rust")
    try:
        assert _bytes(actual) == _bytes(plain)
        _assert_manifest_equal(actual, plain)
        assert native_calls == [33, 32, 33, 32]
    finally:
        shards.cleanup_spool(actual)
        shards.cleanup_spool(plain)


@pytest.mark.asyncio
async def test_supported_native_outcomes_do_not_repeat_partition_key_preparation(
    tmp_path, monkeypatch, host_column_oracle
):
    case = _case({"root_source": [[_root()] * 7], "detail_source": [[_child()] * 7]})
    plain = await _partition(tmp_path, case, "plain", native=False)

    def forbidden(*_args, **_kwargs):
        pytest.fail("supported native outcome recomputed partition keys")

    monkeypatch.setattr(records, "prepare_record", forbidden)
    actual = await _partition(tmp_path, case, "adapter")
    try:
        assert _bytes(actual) == _bytes(plain) and host_column_oracle == [7, 7]
    finally:
        shards.cleanup_spool(actual)
        shards.cleanup_spool(plain)


@pytest.mark.asyncio
async def test_native_allocation_failure_preserves_primary_and_discards_owned_spool(tmp_path, monkeypatch):

    def bind(_layout):

        def invoke(*_args):
            raise MemoryError("native allocation failed")

        return invoke

    monkeypatch.setattr(columns, "bind_column_encoder", bind)
    with pytest.raises(MemoryError, match="native allocation"):
        await _partition(tmp_path, _case(), "allocation-failed")
    assert not (tmp_path / "allocation-failed").exists()


@pytest.mark.asyncio
async def test_cancelled_native_preparation_drains_parts_and_discards_owned_spool(tmp_path, monkeypatch):
    observations = []
    case = _case()

    def bind(_layout):

        def invoke(*_args):
            raise CancellationRequested("native preparation cancelled")

        return invoke

    monkeypatch.setattr(columns, "bind_column_encoder", bind)
    with pytest.raises(CancellationRequested, match="preparation cancelled"):
        await shards.partition_capture(
            _part_source(case[2], observations=observations),
            definition=case[0],
            source=case[1],
            root=tmp_path / "cancelled",
            register_cleanup=lambda _path: None,
            limits=shards.ShardInputLimits(32 * 1024**2, 4, 1024),
            preparations=records.partition_preparations(case[3], case[4], 17, 8 * 1024**2),
        )
    assert observations[-2:] == ["iterator_closed", "context_closed"]
    assert not (tmp_path / "cancelled").exists()


@pytest.mark.parametrize("scalar_value", [Decimal("1.200"), Decimal("1.20E+4"), Decimal("0.00")])
def test_real_rust_preserves_supported_original_decimal_spelling(native_calls, scalar_value):
    from types import SimpleNamespace

    from tests.test_custom_import_build_source_bulk import _bulk_context

    context = _bulk_context(0)
    values_by_name = dict(npi="1003000126", score=scalar_value, enabled=True)
    expected = records.prepare_record(
        context.request.definition,
        context.stream,
        values_by_name,
        stream_slot=1,
        collection_slot=0,
        part_ordinal=2,
        row_ordinal=0,
        source_ordinal=79000000,
    )
    preparation = records.PartitionPreparation(context, memory_bytes=8 * 1024**2)
    actual_records = list(
        records.partition_records(
            context.request.definition,
            context.stream,
            iter([SimpleNamespace(values=values_by_name)]),
            1,
            0,
            2,
            79000000,
            preparation,
        )
    )
    assert records.encode_record(actual_records[0]) == records.encode_record(expected)
    assert dict(actual_records[0].values)["score"] is scalar_value and native_calls == [1]


@pytest.mark.asyncio
async def test_async_cancellation_keeps_reader_and_root_until_native_worker_drains(tmp_path, monkeypatch):
    entered, release, observed = (threading.Event(), threading.Event(), [])
    case = _case()

    def bind(_layout):

        def invoke(*_args):
            entered.set()
            assert release.wait(5), "test did not release native worker"
            return None

        return invoke

    monkeypatch.setattr(columns, "bind_column_encoder", bind)
    task = asyncio.create_task(
        shards.partition_capture(
            _part_source(case[2], observations=observed),
            definition=case[0],
            source=case[1],
            root=tmp_path / "cancelled-async",
            register_cleanup=lambda _path: None,
            limits=shards.ShardInputLimits(32 * 1024**2, 4, 1024),
            preparations=records.partition_preparations(case[3], case[4], 17, 8 * 1024**2),
        )
    )
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and (tmp_path / "cancelled-async").exists()
        assert "context_closed" not in observed
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert observed[-2:] == ["iterator_closed", "context_closed"]
    assert not (tmp_path / "cancelled-async").exists()


@pytest.mark.asyncio
async def test_capture_binds_original_request_registry_build_and_memory_budget(tmp_path, monkeypatch):
    from process.custom_import import family_shard_capture as capture
    from tests.test_custom_import_family_shard_capture import _inputs, _limits

    request, registry, _, _ = await _inputs(monkeypatch, {"root_source": [[_root()]], "detail_source": [[_child()]]})
    limits = _limits()
    observations = []
    factory = capture.partition_preparations

    def bind(*args):
        observations.append(args)
        return factory(*args)

    monkeypatch.setattr(columns, "bind_column_encoder", lambda _layout: None)
    monkeypatch.setattr(capture, "partition_preparations", bind)
    prepared = await capture.prepare_captured_source(
        None, request, registry, 17, root=tmp_path, register_cleanup=lambda _path: None, limits=limits, build_id=29
    )
    try:
        assert observations == [(request, registry, 29, limits.plan.memory_bytes)]
        assert shards.manifest_document(prepared.partition)["records"] == 2
    finally:
        capture.cleanup_source_plan(prepared.plan)
        capture.cleanup_spool(prepared.partition)
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_owner_stage_forwards_genuine_source_context_to_capture(tmp_path, monkeypatch):
    from process.custom_import import family_shard_source_stage as stage
    from tests.test_custom_import_family_shard_source_stage import _owner, _Pipeline

    owner = _owner(tmp_path)
    boundary = _Pipeline(owner)
    owner.base_keys = boundary.base_keys
    boundary.install(monkeypatch)
    observations = []
    factory = object()

    async def capture(*args, **options):
        observations.append((args, options["build_id"]))
        return await boundary.capture(*args, **options)

    monkeypatch.setattr(stage.capture_api, "prepare_captured_source", capture)
    try:
        await stage.advance_source_admission(factory, owner, attempt_root=owner.root / "first")
        context = owner.context
        assert observations == [((factory, context.request, context.registry, context.family_id), context.build_id)]
        assert owner.phase == "graph" and owner.joined
    finally:
        await owner.close_terminal()
    assert not owner.root.exists()
