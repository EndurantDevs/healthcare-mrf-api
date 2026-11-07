# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real bounded spools with explicitly fake native transport; no native proof."""

import asyncio
import json
import threading
import time
from contextlib import asynccontextmanager, nullcontext
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from functools import partial
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.sql.util import find_tables

from process.custom_import import family_shard_projection_stage as stage
from process.custom_import.build_source import _WINDOW, _PageWindow
from process.custom_import.continuation_runtime import producer_token_sha256
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_block_binding import FamilyBlockBinding, FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_block_store import ShardOutputProof
from process.custom_import.family_shard_allocate import SourceAllocationLimits, cleanup_source_allocations
from process.custom_import.family_shard_common import SourceCommonContext
from process.custom_import.family_shard_facts import SourceFactLimits
from process.custom_import.family_shard_input import cleanup_spool
from process.custom_import.family_shard_plan import PlanWork
from process.custom_import.family_shard_source_plan import build_source_plan, cleanup_source_plan
from process.custom_import.materialization import GenerationIdentity
from process.custom_import.runner_types import CandidateRegistry
from tests.test_custom_import_build_source import _request
from tests.test_custom_import_family_shard_allocate import _allocate, _native_boundary
from tests.test_custom_import_family_shard_input import _capture_input, _partition
from tests.test_custom_import_query_projection_sources import _definition

_LIMITS = SourceAllocationLimits(SourceFactLimits(16 * 1024**2, 64 * 1024, 1024**2), 2, 8192)


@asynccontextmanager
async def _source_context(tmp_path, monkeypatch, *, disabled=False):
    definition = _definition()
    document = json.loads(definition.canonical)
    for stream in document["streams"]:
        stream.update(format="parquet", compression="none")
    if disabled:
        document["query"].update(filterable_fields=[], sortable_fields=[], order=[])
    definition = CustomImportDefinition.from_mapping(document)
    rows_by_stream = {stream.stream_id: [[]] for stream in definition.source_streams}
    definition, capture, parts = _capture_input(rows_by_stream, definition=definition)
    partition = await _partition(tmp_path, definition, capture, parts)
    request = _request(
        dataset_id=capture.identity.dataset_id,
        definition_revision_id=capture.identity.definition_revision_id,
        schema_revision_id=capture.identity.schema_revision_id,
        definition=definition,
    )
    registry = CandidateRegistry(
        {"rates": 7}, {stream.stream_id: slot for slot, stream in enumerate(definition.source_streams, 1)}, 1
    )
    part_entries = tuple(
        (registry.stream_slots[part.receipt.stream_id], part.ordinal, part.record_count) for part in parts
    )
    plan = build_source_plan(
        partition,
        request,
        registry,
        part_entries,
        root=tmp_path / "plan",
        register_cleanup=lambda _path: None,
        limits=_LIMITS.spool,
    )
    _native_boundary(monkeypatch, plan)
    allocated = await _allocate(tmp_path, request, plan)
    try:
        yield SourceCommonContext(request, FamilyBlockLayout(definition, registry.child_collection_slots), allocated)
    finally:
        cleanup_source_allocations(allocated)
        cleanup_source_plan(plan)
        cleanup_spool(partition)


def _binding(common):
    tables_by_role = common.layout.tables(common.source.snapshot_family_id)
    return FamilyBlockBinding(
        17, common.layout, tables_by_role, {role: 1000 + index for index, role in enumerate(tables_by_role)}, 9
    )


def _rows(count=3):
    return {
        "query_root": [(10 + index, 2024, Decimal("1.250000000000")) for index in range(count)],
        "query_child": [(10 + index, 2024, 7001, 17, None, Decimal("0")) for index in range(count)],
    }


class _Cursor:
    def __init__(self, session, role):
        self.session, self.role, self.closed = session, role, False

    async def partitions(self, count):
        rows = self.session.factory.rows_by_role[self.role]
        for offset in range(0, len(rows), count):
            self.session.factory.events.append(("partition", self.role, count))
            yield rows[offset : offset + count]
        if self.session.factory.late_error is not None:
            raise self.session.factory.late_error

    async def close(self):
        self.closed = True
        self.session.factory.events.append(("cursor_closed", self.role))


class _Session:
    def __init__(self, factory):
        self.factory, self.info = factory, {}
        self.root = self.connection_root = self.nested = self.connection_nested = None
        self.new, self.dirty, self.deleted = (), (), ()
        self.readonly = False
        self.no_autoflush = nullcontext()

    @asynccontextmanager
    async def begin(self):
        self.root, self.connection_root = SimpleNamespace(is_active=True), SimpleNamespace(is_active=True)
        try:
            yield self
        finally:
            self.root.is_active = self.connection_root.is_active = False
            self.factory.events.append("transaction_closed")

    def in_transaction(self):
        return self.root is not None and self.root.is_active

    def get_transaction(self):
        return self.root

    def get_nested_transaction(self):
        return self.nested

    async def connection(self):
        return SimpleNamespace(
            get_transaction=lambda: self.connection_root,
            get_nested_transaction=lambda: self.connection_nested,
        )

    async def execute(self, statement):
        sql = str(statement.compile(dialect=dialect()))
        self.factory.statements.append(sql)
        if "SET TRANSACTION" in sql:
            self.readonly = True
            return None
        if "current_setting('transaction_isolation')" in sql:
            return SimpleNamespace(one=lambda: ("repeatable read", "on" if self.readonly else "off"))
        if "set_config" in sql:
            arguments = tuple(statement.selected_columns[0].clauses)
            self.factory.timeouts.append(int(arguments[1].value))
            return None
        if tuple(statement.selected_columns.keys()) == ("row_count", "row_bytes"):
            role = self.factory.role(statement)
            size = self.factory.width if self.factory.rows_by_role[role] else 0
            census = SimpleNamespace(
                row_count=len(self.factory.rows_by_role[role]) + self.factory.count_delta, row_bytes=size
            )
            return SimpleNamespace(one=lambda: census)
        self.factory.guards.append(sql)
        return SimpleNamespace(first=lambda: self.factory.guard_failure)

    async def stream(self, statement):
        assert self.readonly
        self.factory.fetch_sizes.append(statement.get_execution_options()["yield_per"])
        cursor = _Cursor(self, self.factory.role(statement))
        self.factory.cursors.append(cursor)
        return cursor


class _Factory:
    def __init__(self, rows):
        self.rows_by_role = rows
        self.events, self.statements, self.guards, self.cursors, self.fetch_sizes, self.timeouts = (
            [],
            [],
            [],
            [],
            [],
            [],
        )
        self.width, self.count_delta, self.guard_failure, self.late_error = 2048, 0, None, None
        self.sessions, self.observations = [], []
        self.change = None

    @asynccontextmanager
    async def __call__(self):
        session = _Session(self)
        self.sessions.append(session)
        try:
            yield session
        finally:
            self.events.append("session_closed")

    def role(self, statement):
        tables = find_tables(statement, include_aliases=False)
        return "query_child" if any(table.name == "custom_import_block_key" for table in tables) else "query_root"


def _native_transport(monkeypatch, common, factory):
    binding = _binding(common)
    request = common.request
    generation = GenerationIdentity(401, request.dataset_id, request.definition_revision_id, request.schema_revision_id)
    header = stage._proof_inputs(common, 301, binding, generation)

    async def initial(session, supplied):
        assert supplied is request
        session.info[_WINDOW] = _PageWindow(time.monotonic() + 30, supplied.statement_timeout_ms)
        factory.observations.append("fresh_read_committed")

    async def state(session, supplied, build_id):
        assert supplied is common and build_id == 301
        factory.observations.append("read_repeatable" if session.readonly else "native_observation")
        if factory.change is not None:
            factory.change(session)
        return binding, generation, dict(header)

    monkeypatch.setattr(stage, "_initial_page_window", initial)
    monkeypatch.setattr(stage, "_native_state", state)
    return binding, generation, header


async def _stage(tmp_path, common, factory, *, limits=_LIMITS, check=None):
    registered_paths = []
    output = await stage.stage_shard_query_projection(
        factory,
        common,
        301,
        work=PlanWork(tmp_path / "projection", limits.spool, registered_paths.append, check),
        limits=limits,
        shard_id=23,
    )
    return output, registered_paths


@pytest.mark.asyncio
@pytest.mark.parametrize("empty", [False, True])
async def test_stages_complete_rows_with_scope_receipt_without_database_writes(tmp_path, monkeypatch, empty):
    async with _source_context(tmp_path, monkeypatch) as common:
        factory = _Factory(_rows(0 if empty else 3))
        _binding_row, _generation, expected_header = _native_transport(monkeypatch, common, factory)
        output, registered = await _stage(tmp_path, common, factory)
        try:
            assert registered == [tmp_path / "projection"]
            assert output.proof_inputs == expected_header
            expected = ShardOutputProof(common.layout, 23, bytes.fromhex(common.source.source.partition_sha256), 0)
            actual_by_role = {role: [] for role in factory.rows_by_role}
            for batch in output.batches(maximum_rows=2, maximum_bytes=8192):
                for role, cells in batch.items():
                    actual_by_role[role].extend(cells)
                    expected.add(role, cells)
            assert actual_by_role == factory.rows_by_role
            assert output.receipt == expected.receipt()
            assert output.receipt["source_record_count"] == 0
            assert len(factory.guards) == 4 and all(cursor.closed for cursor in factory.cursors)
            assert all(1 <= size <= 2 for size in factory.fetch_sizes)
            assert all(0 < timeout <= common.request.statement_timeout_ms for timeout in factory.timeouts)
            assert factory.observations.count("fresh_read_committed") == 2
            assert factory.observations.count("read_repeatable") == 1
            assert not any(
                word in sql for sql in factory.statements for word in ("INSERT", "UPDATE", "DELETE", "FOR UPDATE")
            )
            assert common.source.source.root and Path(common.source.root).exists()
            output.receipt["role_counts"]["query_root"] = 1000
            assert output.receipt == expected.receipt()
        finally:
            output.close()
        assert not (tmp_path / "projection").exists()


@pytest.mark.asyncio
async def test_disabled_capability_observes_authority_without_creating_work_or_manifest(tmp_path, monkeypatch):
    async with _source_context(tmp_path, monkeypatch, disabled=True) as common:
        factory = _Factory(_rows())
        _native_transport(monkeypatch, common, factory)
        output, registered = await _stage(tmp_path, common, factory)
        assert output is None and registered == [] and factory.cursors == []
        assert factory.observations == ["fresh_read_committed", "native_observation"]
        assert not (tmp_path / "projection").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["ambiguity", "eof", "late", "width", "root", "connection", "savepoint", "window"])
async def test_native_refusals_close_cursors_and_remove_only_owned_work(tmp_path, monkeypatch, failure):
    async with _source_context(tmp_path, monkeypatch) as common:
        factory = _Factory(_rows())
        _native_transport(monkeypatch, common, factory)
        expected = (FamilyBlockUnavailable, FamilyBlockResourceLimit, OSError)
        _configure_refusal(factory, failure)
        with pytest.raises(expected):
            await _stage(tmp_path, common, factory)
        assert all(cursor.closed for cursor in factory.cursors)
        assert not (tmp_path / "projection").exists() and Path(common.source.root).exists()


def _replace_read_identity(failure, session):
    if not session.readonly:
        return
    if failure == "window":
        session.info[_WINDOW] = replace(session.info[_WINDOW], deadline=time.monotonic() + 300)
    else:
        attribute = {"root": "root", "connection": "connection_root", "savepoint": "nested"}[failure]
        setattr(session, attribute, SimpleNamespace(is_active=True))


def _configure_refusal(factory, failure):
    fault_by_name = {
        "ambiguity": ("guard_failure", ("duplicate",)),
        "eof": ("count_delta", 1),
        "late": ("late_error", OSError("synthetic cursor read failure")),
        "width": ("width", _LIMITS.spool.memory_bytes),
    }
    if failure in fault_by_name:
        attribute, changed = fault_by_name[failure]
        setattr(factory, attribute, changed)
    else:
        factory.change = partial(_replace_read_identity, failure)


@pytest.mark.asyncio
async def test_late_spool_tampering_is_rejected_at_replay_eof(tmp_path, monkeypatch):
    async with _source_context(tmp_path, monkeypatch) as common:
        factory = _Factory(_rows())
        _native_transport(monkeypatch, common, factory)
        output, _registered = await _stage(tmp_path, common, factory)
        try:
            output.path.chmod(0o600)
            contents = output.path.read_bytes()
            output.path.write_bytes(
                contents.replace(b"2024", b"2025") if b"2024" in contents else contents[:-1] + bytes([contents[-1] ^ 1])
            )
            with pytest.raises((ValueError, TypeError)):
                list(output.batches(maximum_rows=2, maximum_bytes=8192))
        finally:
            output.close()


@pytest.mark.asyncio
async def test_repeated_cancel_waits_for_file_thread_before_cursor_and_root_cleanup(tmp_path, monkeypatch):
    async with _source_context(tmp_path, monkeypatch) as common:
        factory = _Factory(_rows())
        _native_transport(monkeypatch, common, factory)
        started, release, finished = threading.Event(), threading.Event(), threading.Event()
        original = stage._append_partition

        def blocked(*arguments):
            started.set()
            try:
                assert release.wait(5)
                return original(*arguments)
            finally:
                finished.set()

        monkeypatch.setattr(stage, "_append_partition", blocked)
        pending = asyncio.create_task(_stage(tmp_path, common, factory))
        try:
            assert await asyncio.to_thread(started.wait, 3)
            pending.cancel()
            await asyncio.sleep(0)
            pending.cancel()
            await asyncio.sleep(0)
            assert not pending.done() and (tmp_path / "projection").exists()
            assert not any(cursor.closed for cursor in factory.cursors)
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert finished.is_set() and all(cursor.closed for cursor in factory.cursors)
        assert not (tmp_path / "projection").exists() and Path(common.source.root).exists()


@pytest.mark.asyncio
async def test_ordinary_file_error_retains_identity_without_false_cleanup_note(tmp_path, monkeypatch):
    async with _source_context(tmp_path, monkeypatch) as common:
        factory = _Factory(_rows())
        _native_transport(monkeypatch, common, factory)
        original = OSError("synthetic spool failure")

        def failed(*_arguments):
            raise original

        monkeypatch.setattr(stage, "_append_partition", failed)
        with pytest.raises(OSError) as caught:
            await _stage(tmp_path, common, factory)
        assert caught.value is original and not getattr(original, "__notes__", ())
        assert all(cursor.closed for cursor in factory.cursors) and not (tmp_path / "projection").exists()


def test_queries_keep_complete_guards_and_dictionary_mapping_without_insert_statements():
    from tests.test_custom_import_family_shard_projection_sql import _binding as binding_fixture

    binding = binding_fixture()
    guards, outputs = stage._queries(binding, GenerationIdentity(4, 1, 2, 3))
    assert len(guards) == 4 and len(outputs) == 2
    assert tuple(outputs[1][1].selected_columns.keys()) == tuple(binding.tables_by_role["query_child"].c.keys())
    guard = str(guards[-1].compile(dialect=dialect()))
    assert "LEFT OUTER JOIN" in guard and "count(" in guard and "HAVING" in guard
    assert "key_rank" in guard and "canonical_child_key" in guard and 'COLLATE "C"' in guard
    assert "INSERT" not in guard


def _control_rows(common):
    request = common.request
    capture_by_name = dict(common.source.source.scope[0])
    producer_by_name = dict(
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        execution_id=request.execution_id,
        producing_fence=request.fence,
        producing_token_sha256=producer_token_sha256(request),
        capture_bundle_id=capture_by_name["capture_bundle_id"],
        generation_id=401,
    )
    stamp = datetime(2030, 1, 2, tzinfo=timezone.utc)
    build = SimpleNamespace(
        **producer_by_name,
        build_id=301,
        phase="output",
        graph_frozen_at=stamp,
        source_frozen_at=stamp,
        source_occurrence_count=common.source.source.counts[0],
        candidate_error_count=0,
        output_frozen_at=None,
        verified_at=None,
    )
    generation = SimpleNamespace(**producer_by_name, base_generation_id=request.expected_base_generation_id)
    snapshot = SimpleNamespace(**producer_by_name, family_id=common.source.snapshot_family_id, frozen_at=None)
    return build, generation, snapshot, capture_by_name["source_binding_revision_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [None, "phase", "graph", "source", "count", "error", "capture", "producer", "generation", "frozen", "binding"],
)
async def test_control_row_observations_require_exact_original_open_output_scope(tmp_path, monkeypatch, fault):
    async with _source_context(tmp_path, monkeypatch) as common:
        native_rows = list(_control_rows(common))
        changes_by_fault = {
            "phase": (native_rows[0], "phase", "graph"),
            "graph": (native_rows[0], "graph_frozen_at", None),
            "source": (native_rows[0], "source_frozen_at", None),
            "count": (native_rows[0], "source_occurrence_count", 1),
            "error": (native_rows[0], "candidate_error_count", 1),
            "capture": (native_rows[1], "capture_bundle_id", 999),
            "producer": (native_rows[2], "producing_token_sha256", b"x" * 32),
            "generation": (native_rows[2], "generation_id", 999),
            "frozen": (native_rows[2], "frozen_at", datetime.now(timezone.utc)),
        }
        if fault in changes_by_fault:
            producer, field, changed = changes_by_fault[fault]
            setattr(producer, field, changed)
        if fault == "binding":
            native_rows[3] = 999
        if fault is None:
            identity = stage._generation_state(common, 301, native_rows)
            assert identity.generation_id == 401 and identity.dataset_id == common.request.dataset_id
        else:
            with pytest.raises(FamilyBlockUnavailable):
                stage._generation_state(common, 301, native_rows)
