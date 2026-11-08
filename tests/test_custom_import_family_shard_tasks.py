# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Observed SOURCE IDs reach bounded spawned tasks without losing audit custody."""

import asyncio
import hashlib
import multiprocessing
import pickle
import struct
import threading
import time
from concurrent.futures.process import BrokenProcessPool
from contextlib import asynccontextmanager
from dataclasses import asdict, astuple, fields, replace
from functools import partial
from pathlib import Path
from types import SimpleNamespace

import pytest

from process.custom_import import family_shard_allocate as allocation
from process.custom_import import family_shard_owner_lock as locks
from process.custom_import import family_shard_tasks as tasks
from process.custom_import.family import CandidateRejected
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_shard_facts import SourceFactLimits
from process.custom_import.family_shard_input import cleanup_spool
from process.custom_import.family_shard_plan import _cpu
from process.custom_import.family_shard_route import route_capture
from process.custom_import.family_shard_source_plan import cleanup_source_plan
from process.custom_import.family_shard_worker import run_source_shard
from process.custom_import.runner_codec import digest_text
from tests import test_custom_import_family_shard_source_plan as source_fixture
from tests.custom_import_family_shard_tasks_support import (
    _crash_second_source_worker,
    _locked_source_worker,
    _refuse_completed_worker_execution,
    _slow_worker,
)
from tests.test_custom_import_family_shard_allocate import _allocate, _native_boundary
from tests.test_custom_import_family_shard_input import _capture_input, _child, _root
from tests.test_custom_import_family_shard_worker import _limits as worker_limits
from tests.test_custom_import_family_shard_worker import _projected_definition


def _limits():
    return SourceFactLimits(64 * 1024**2, 1024**2, 4096)


def _dictionaries(allocated_source):
    family_by_key, query_by_key = {}, {}
    for pack, _pack_id, _roots, revisions, _rejections in allocation.read_source_allocations(allocated_source):
        for index, source_record in enumerate(pack.records):
            if source_record.collection_slot == 0 and source_record.typed_parent_key is not None:
                family_by_key.setdefault(
                    source_record.typed_parent_key, (7001 + 17 * len(family_by_key), 9001 + 17 * len(family_by_key))
                )
            if source_record.collection_slot and revisions[index] is not None:
                query_by_key[(pack.landing.records[index].child_key_sha256, source_record.child_key)] = None
    queries = tuple((digest, text, 501 + index * 17, 9) for index, (digest, text) in enumerate(sorted(query_by_key)))
    return tuple(family_by_key.items()), queries


@asynccontextmanager
async def _source(tmp_path, monkeypatch, rows=None):
    def _projected_capture(rows, **options):
        options["definition"] = options.get("definition") or _projected_definition()
        return _capture_input(rows, **options)

    monkeypatch.setattr(source_fixture, "_capture_input", _projected_capture)
    case = await source_fixture._case(
        tmp_path,
        rows
        or {
            "root_source": [[_root(), _root("1234567893")]],
            "detail_source": [[_child(key="same"), _child("1234567893", "same"), _child(key="same")]],
        },
    )
    case = (case[0], replace(case[1], complete_scope=True), *case[2:])
    plan = source_fixture._build(tmp_path, case)
    _native_boundary(monkeypatch, plan)
    allocated = await _allocate(tmp_path, case[1], plan)
    try:
        with route_capture(
            case[0], case[1].definition, root=tmp_path / "route", register_cleanup=lambda path: None, limits=_limits()
        ) as shards:
            yield tasks.SourceTaskScope(case[1].definition, case[1].complete_scope), shards, allocated
    finally:
        allocation.cleanup_source_allocations(allocated)
        cleanup_source_plan(plan)
        cleanup_spool(case[0])


def _prepare(tmp_path, definition, shards, allocated_source, *, families=None, queries=None):
    expected_families, expected_queries = _dictionaries(allocated_source)
    return tasks.prepare_source_tasks(
        shards,
        allocated_source,
        definition,
        iter(expected_families if families is None else families),
        iter(expected_queries if queries is None else queries),
        root=tmp_path / "metadata",
        register_cleanup=lambda path: None,
        limits=_limits(),
    )


def _observed_records(allocated_source):
    return {
        (source_record.stream_slot, source_record.source_ordinal): (revisions[index], pack_id, roots[index])
        for pack, pack_id, roots, revisions, _rejections in allocation.read_source_allocations(allocated_source)
        for index, source_record in enumerate(pack.records)
        if revisions[index] is not None
    }


@pytest.mark.asyncio
async def test_external_join_keeps_every_initial_revision_and_only_requested_global_ranks(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (definition, shards, allocated_source):
        families, queries = _dictionaries(allocated_source)
        unused = '{"unused":"key"}'
        prepared = _prepare(
            tmp_path,
            definition,
            shards,
            allocated_source,
            queries=(*queries, (digest_text("child-key", unused), unused, 9999, 11)),
        )
        try:
            actual_by_coordinate = {}
            for shard in shards:
                plan = prepared.load_plan(shard.shard_id, worker_limits())
                actual_by_coordinate.update(plan.record_ids)
                assert all(
                    assignment in {(rank, variant) for _hash, _text, rank, variant in queries}
                    for assignment in plan.query_key_ids.values()
                )
                assert all(key[1] != unused for key in plan.query_key_ids)
            expected_by_coordinate = _observed_records(allocated_source)
            assert actual_by_coordinate == expected_by_coordinate and len(actual_by_coordinate) == 5
            assert prepared.counts == dict(records=5, packs=3, revisions=5, missing_family=0, missing_query=0)
            assert len(families) == 2
        finally:
            prepared.close()
        assert Path(allocated_source.root).exists()


@pytest.mark.asyncio
async def test_parent_check_is_not_semantic_or_worker_metadata(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (scope, shards, allocated):
        checks = []
        checked = tasks.SourceTaskScope(scope.definition, scope.complete_scope, lambda: checks.append(True))
        assert checked == scope and asdict(checked) == asdict(scope) and astuple(checked) == astuple(scope)
        assert [field.name for field in fields(checked)] == ["definition", "complete_scope"]
        with pytest.raises(ValueError, match="cancellation check"):
            tasks.SourceTaskScope(scope.definition, scope.complete_scope, False)
        prepared = _prepare(tmp_path, checked, shards, allocated)
        try:
            before = len(checks)
            plans = [prepared.load_plan(shard.shard_id, worker_limits()) for shard in shards]
            assert len(checks) > before > 0
            assert all(pickle.loads(pickle.dumps(plan)) == plan for plan in plans)
            assert all(not hasattr(plan, "_check") for plan in plans)
            digest = tasks._prepared_sha256(prepared)
            prepared._work._io_check = None
            assert tasks._prepared_sha256(prepared) == digest
        finally:
            prepared.close()


@pytest.mark.asyncio
async def test_task_file_hashes_honor_parent_check_before_and_after_loading(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (scope, shards, allocated):
        observed, checks, original = [], [], tasks._plan_digest

        def digest(source, check):
            observed.append((source, check))
            return original(source, check)

        current = lambda: checks.append(True)
        monkeypatch.setattr(tasks, "_plan_digest", digest)
        prepared = _prepare(
            tmp_path, tasks.SourceTaskScope(scope.definition, scope.complete_scope, current), shards, allocated
        )
        try:
            assert len(observed) == len(shards) and all(check is current for _file, check in observed)
            descriptor = next(entry for entry in prepared.files if entry.byte_count)
            prepared.load_plan(descriptor.shard.shard_id, worker_limits())
            assert len(observed) == len(shards) + 2 and all(check is current for _file, check in observed)
            assert checks and all(source.closed for source, _check in observed)
        finally:
            prepared.close()


@pytest.mark.asyncio
async def test_cancelled_preparation_joins_writer_before_removal(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (scope, shards, allocated):
        entered = threading.Event()
        stopped = threading.Event()
        release = threading.Event()
        closed = threading.Event()
        stop = threading.Event()
        families, queries = _dictionaries(allocated)

        def current():
            if stop.is_set():
                raise InterruptedError("synthetic preparation stopped")

        def borrowed():
            try:
                yield from families
                entered.set()
                assert stop.wait(5)
                stopped.set()
                assert release.wait(5)
                current()
            finally:
                closed.set()

        operation = partial(
            _prepare,
            tmp_path,
            tasks.SourceTaskScope(scope.definition, scope.complete_scope, current),
            shards,
            allocated,
            families=borrowed(),
            queries=queries,
        )
        pending = asyncio.create_task(_cpu(SimpleNamespace(stop=stop), operation))
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            pending.cancel("first preparation cancellation")
            assert await asyncio.to_thread(stopped.wait, 5)
            assert not pending.done() and (tmp_path / "metadata").exists()
            pending.cancel("second preparation cancellation")
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError, match="first preparation cancellation"):
            await pending
        assert closed.is_set() and not (tmp_path / "metadata").exists()
        assert (
            sum(len(allocated_pack[0].records) for allocated_pack in allocation.read_source_allocations(allocated))
            == allocated.source.counts[0]
        )
        assert all(Path(shard.root).exists() for shard in shards)


@pytest.mark.asyncio
async def test_preparation_keeps_first_failure_if_cleanup_also_fails(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (scope, shards, allocated):
        primary = InterruptedError("synthetic original stop")
        cleanup = RuntimeError("synthetic cleanup failure")
        original_close = tasks._Work.close

        def current():
            if (tmp_path / "metadata").exists():
                raise primary

        def failed_close(work):
            original_close(work)
            if work.root == tmp_path / "metadata":
                raise cleanup

        monkeypatch.setattr(tasks._Work, "close", failed_close)
        with pytest.raises(InterruptedError, match="original stop") as raised:
            _prepare(
                tmp_path, tasks.SourceTaskScope(scope.definition, scope.complete_scope, current), shards, allocated
            )
        assert raised.value is primary and primary.__cause__ is cleanup
        assert primary._custom_import_retry_blocked and not (tmp_path / "metadata").exists()
        assert list(allocation.read_source_allocations(allocated))


@pytest.mark.asyncio
async def test_async_spawn_keeps_heartbeat_and_completed_evidence_until_explicit_cleanup(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (definition, shards, allocated_source):
        prepared = _prepare(tmp_path, definition, shards, allocated_source)
        original = prepared.load_plan
        ticks, done, registered_roots = [], asyncio.Event(), []

        def _load_off_loop(shard_id, limits):
            assert threading.current_thread() is not threading.main_thread()
            time.sleep(0.03)
            return original(shard_id, limits)

        async def _heartbeat():
            while not done.is_set():
                ticks.append(True)
                await asyncio.sleep(0.01)

        def _register(path):
            assert not path.exists()
            registered_roots.append(path)

        monkeypatch.setattr(prepared, "load_plan", _load_off_loop)
        heartbeat = asyncio.create_task(_heartbeat())
        run = None
        try:
            async with tasks.run_source_tasks(
                prepared,
                root=tmp_path / "workers",
                register_cleanup=_register,
                worker_limits=worker_limits(),
                maximum_workers=2,
            ) as run:
                worker_outputs = [worker_output async for worker_output in run]
                assert run.all_returned and run.peak_inflight <= 2
            assert len(worker_outputs) == len(shards) and len(ticks) >= 5
            assert all(
                worker_output.path.exists() and list(worker_output.audit_rows())[-1][0] == "eof"
                for worker_output in run.completed
            )
            assert all(not process.is_alive() for process in run._processes.values())
            assert len(registered_roots) == len(shards) + 1
            await run.cleanup()
            assert not (tmp_path / "workers").exists()
        finally:
            done.set()
            await heartbeat
            if run is not None:
                await run.cleanup()
            prepared.close()


@pytest.mark.asyncio
async def test_next_shard_finishes_while_owner_holds_completed_output(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (scope, shards, allocated):
        prepared = _prepare(tmp_path, scope, shards, allocated)
        run = None
        try:
            async with tasks.run_source_tasks(
                prepared,
                root=tmp_path / "workers",
                register_cleanup=lambda _path: None,
                worker_limits=worker_limits(),
                maximum_workers=1,
            ) as run:
                first = await anext(aiter(run))
                assert len(run._pending) == 1 and run.peak_inflight == 1
                future = next(iter(run._pending))
                second = await asyncio.wait_for(asyncio.shield(future), 30)
                assert second.receipt["shard_id"] != first.receipt["shard_id"]
                outputs = [first, *[output async for output in run]]
                assert len(outputs) == len(shards) and run.all_returned
                assert {output.receipt["shard_id"] for output in outputs} == set(range(len(shards)))
                assert all(list(output.audit_rows())[-1][0] == "eof" for output in outputs)
            assert all(not process.is_alive() for process in run._processes.values())
            await run.cleanup()
            assert not (tmp_path / "workers").exists()
        finally:
            if run is not None:
                await run.cleanup()
            prepared.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage,message",
    [
        (None, None),
        ("metadata", "native metadata commitment"),
        ("counts", "complete census"),
        ("missing_eof", "audit EOF is missing"),
        ("late_bytes", "changed while reading"),
    ],
)
async def test_closed_output_uses_one_complete_audit_and_preserves_tamper_checks(
    tmp_path, monkeypatch, damage, message
):
    async with _source(tmp_path, monkeypatch) as (scope, shards, allocated):
        prepared = _prepare(tmp_path, scope, shards, allocated)
        output = None
        try:
            shard = next(shard.shard_id for shard in shards if shard.records)
            output = run_source_shard(
                prepared.load_plan(shard, worker_limits()), str(tmp_path / "worker"), worker_limits()
            )
            original, observed, receipt = output.audit_rows, [], output.receipt

            def audit():
                observed.append("open")
                for entry in original():
                    if damage == "metadata" and entry[0] == "worker":
                        entry = entry[0], entry[1] | {"metadata_sha256": b"\0" * 32}
                    if entry[0] == "eof":
                        if damage == "missing_eof":
                            continue
                        if damage == "counts":
                            entry = entry[0], entry[1] | {"records": entry[1]["records"] + 1}
                    yield entry
                    if damage == "late_bytes" and entry[0] == "eof":
                        output.path.chmod(0o600)
                        try:
                            with output.path.open("r+b") as target:
                                target.seek(-1, 2)
                                last = target.read(1)
                                target.seek(-1, 2)
                                target.write(bytes([last[0] ^ 1]))
                        finally:
                            output.path.chmod(0o400)
                observed.append("eof")

            monkeypatch.setattr(output, "audit_rows", audit)
            if damage is None:
                tasks._validate_closed_output(prepared, output, shard, worker_limits())
                assert observed == ["open", "eof"] and output.receipt == receipt
            else:
                with pytest.raises(ValueError, match=message):
                    tasks._validate_closed_output(prepared, output, shard, worker_limits())
                assert observed.count("open") == 1
        finally:
            if output is not None:
                output.close()
            prepared.close()
        assert not (tmp_path / "worker").exists()


async def _assert_pool_holds_lock(run, shards, holder, identity):
    assert len([output async for output in run]) == len(shards)
    processes = tuple(run._processes.values())
    assert len(processes) == 2 and all(process.is_alive() for process in processes)
    assert all((holder.root / f"observed-{process.pid}").is_file() for process in processes)
    holder.close()
    with pytest.raises(BlockingIOError):
        locks.acquire_owner_job_lock(holder.root, identity=identity)
    return processes


async def _cleanup_locked_run(run, prepared, root):
    if run is not None:
        await run.stop(terminate=True)
        await run.cleanup()
    prepared.close()
    if run is not None:
        for process in run._processes.values():
            (root / f"observed-{process.pid}").unlink(missing_ok=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["spawn", "forkserver"])
async def test_actual_pool_retains_inherited_lock_after_tasks_until_worker_exit(tmp_path, monkeypatch, method):
    root = tmp_path / "job"
    root.mkdir(mode=0o700)
    observed = root.stat()
    identity = observed.st_dev, observed.st_ino
    holder = locks.acquire_owner_job_lock(root, identity=identity)
    executor = tasks.ProcessPoolExecutor

    def start_pool(**options):
        options["mp_context"] = multiprocessing.get_context(method)
        return executor(**options)

    monkeypatch.setattr(tasks, "ProcessPoolExecutor", start_pool)
    monkeypatch.setattr(tasks, "run_source_shard", _locked_source_worker)
    try:
        async with _source(tmp_path, monkeypatch) as (scope, shards, allocated):
            prepared = _prepare(tmp_path, scope, shards, allocated)
            run = None
            try:
                async with tasks.run_source_tasks(
                    prepared,
                    root=root / "attempt",
                    register_cleanup=lambda _path: None,
                    worker_limits=worker_limits(),
                    maximum_workers=2,
                    owner_lock=holder,
                ) as run:
                    processes = await _assert_pool_holds_lock(run, shards, holder, identity)
                assert all(not process.is_alive() for process in processes)
                with locks.acquire_owner_job_lock(root, identity=identity) as replacement:
                    replacement.check()
            finally:
                await _cleanup_locked_run(run, prepared, root)
    finally:
        holder.close()
        with locks.acquire_owner_job_lock(root, identity=identity) as replacement:
            replacement.remove_terminal_root()


@pytest.mark.asyncio
async def test_cancellation_joins_only_owned_workers_before_terminal_cleanup(tmp_path, monkeypatch):
    unhandled_errors = []
    monkeypatch.setattr(asyncio.get_running_loop(), "call_exception_handler", unhandled_errors.append)
    async with _source(tmp_path, monkeypatch) as (definition, shards, allocated_source):
        prepared = _prepare(tmp_path, definition, shards, allocated_source)
        monkeypatch.setattr(tasks, "run_source_shard", _slow_worker)
        run = None
        try:
            async with tasks.run_source_tasks(
                prepared,
                root=tmp_path / "workers",
                register_cleanup=lambda path: None,
                worker_limits=worker_limits(),
                maximum_workers=2,
            ) as run:
                pending = asyncio.create_task(anext(aiter(run)))
                await asyncio.sleep(0.2)
                pending.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await pending
            assert not run.all_returned and all(not process.is_alive() for process in run._processes.values())
            await asyncio.sleep(0)
            assert not unhandled_errors
            assert (tmp_path / "workers").exists()
            await run.cleanup()
            assert not (tmp_path / "workers").exists()
        finally:
            if run is not None:
                await run.cleanup()
            prepared.close()


@pytest.mark.asyncio
async def test_dictionary_eof_conflicts_and_worker_budgets_fail_closed(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (definition, shards, allocated_source):
        families, queries = _dictionaries(allocated_source)
        conflicting = (*families, (families[0][0], (111, 333)))
        with pytest.raises(ValueError, match="assignment conflicts"):
            _prepare(tmp_path, definition, shards, allocated_source, families=conflicting)
        assert not (tmp_path / "metadata").exists()

        def _late_queries():
            yield from queries
            raise RuntimeError("synthetic late dictionary EOF failure")

        with pytest.raises(RuntimeError, match="dictionary EOF"):
            tasks.prepare_source_tasks(
                shards,
                allocated_source,
                definition,
                iter(families),
                _late_queries(),
                root=tmp_path / "metadata",
                register_cleanup=lambda path: None,
                limits=_limits(),
            )
        assert not (tmp_path / "metadata").exists()
        prepared = _prepare(tmp_path, definition, shards, allocated_source)
        try:
            populated = next(
                index for index in range(len(shards)) if prepared.load_plan(index, worker_limits()).record_ids
            )
            with pytest.raises(FamilyBlockResourceLimit):
                prepared.load_plan(populated, replace(worker_limits(), maximum_metadata_rows=1))
        finally:
            prepared.close()


@pytest.mark.asyncio
async def test_missing_global_mapping_cannot_make_an_accepted_worker_output(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (definition, shards, allocated_source):
        prepared = _prepare(tmp_path, definition, shards, allocated_source, queries=())
        run = None
        try:
            assert prepared.counts["missing_query"] > 0
            with pytest.raises(ValueError, match="dictionary identity"):
                async with tasks.run_source_tasks(
                    prepared,
                    root=tmp_path / "workers",
                    register_cleanup=lambda path: None,
                    worker_limits=worker_limits(),
                    maximum_workers=2,
                ) as run:
                    await _consume_outputs(run)
            assert all(not process.is_alive() for process in run._processes.values())
            assert (tmp_path / "workers").exists()
            assert "ValueError" in run.failures_by_shard.values()
        finally:
            if run is not None:
                await run.cleanup()
            prepared.close()


async def _consume_outputs(run):
    return [output async for output in run]


@pytest.mark.asyncio
async def test_initial_orphans_and_duplicates_keep_ids_while_invalid_lane_stays_routed(tmp_path, monkeypatch):
    records_by_stream = {
        "root_source": [[_root(), _root(), _root(npi=None)]],
        "detail_source": [[_child(), _child("1111111111"), _child(key=None)]],
    }
    async with _source(tmp_path, monkeypatch, records_by_stream) as (scope, shards, allocated_source):
        prepared = _prepare(tmp_path, scope, shards, allocated_source)
        try:
            observed_by_coordinate = {}
            for shard in shards:
                observed_by_coordinate.update(prepared.load_plan(shard.shard_id, worker_limits()).record_ids)
            assert observed_by_coordinate == _observed_records(allocated_source)
            assert len(observed_by_coordinate) == 4 and (2, 1) in observed_by_coordinate
            assert (1, 2) not in observed_by_coordinate and (2, 2) not in observed_by_coordinate
            assert sum(shard.records for shard in shards) == prepared.counts["records"] == 6
        finally:
            prepared.close()


@pytest.mark.asyncio
async def test_original_false_scope_is_carried_and_snapshot_worker_refuses_it(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (scope, shards, allocated_source):
        prepared = _prepare(tmp_path, replace(scope, complete_scope=False), shards, allocated_source)
        try:
            populated = next(shard for shard in shards if shard.records)
            plan = prepared.load_plan(populated.shard_id, worker_limits())
            assert plan.complete_scope is False
            with run_source_shard(plan, tmp_path / "false-scope", worker_limits()) as output:
                assert output.status == "refused" and list(output.audit_rows())[-1][0] == "eof"
        finally:
            prepared.close()


@pytest.mark.asyncio
async def test_metadata_tamper_and_excess_concurrency_refuse_before_dispatch(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (scope, shards, allocated_source):
        prepared = _prepare(tmp_path, scope, shards, allocated_source)
        try:
            with pytest.raises(ValueError, match="concurrency"):
                async with tasks.run_source_tasks(
                    prepared,
                    root=tmp_path / "workers",
                    register_cleanup=lambda path: None,
                    worker_limits=worker_limits(),
                    maximum_workers=9,
                ):
                    raise AssertionError("invalid pool was opened")
            assert not (tmp_path / "workers").exists()
            descriptor = next(entry for entry in prepared.files if entry.byte_count)
            path = prepared._work.root / descriptor.name
            path.chmod(0o600)
            payload = path.read_bytes()
            path.write_bytes(bytes([payload[0] ^ 1]) + payload[1:])
            path.chmod(0o400)
            with pytest.raises(ValueError, match="metadata bytes"):
                prepared.load_plan(descriptor.shard.shard_id, worker_limits())
        finally:
            prepared.close()


@pytest.mark.asyncio
async def test_cancelled_metadata_load_finishes_before_its_resources_can_be_removed(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (scope, shards, allocated_source):
        prepared = _prepare(tmp_path, scope, shards, allocated_source)
        original, finished = prepared.load_plan, threading.Event()

        def _slow_load(shard_id, limits):
            time.sleep(0.2)
            try:
                return original(shard_id, limits)
            finally:
                finished.set()

        monkeypatch.setattr(prepared, "load_plan", _slow_load)
        run = None
        try:
            async with tasks.run_source_tasks(
                prepared,
                root=tmp_path / "workers",
                register_cleanup=lambda path: None,
                worker_limits=worker_limits(),
                maximum_workers=2,
            ) as run:
                pending = asyncio.create_task(anext(aiter(run)))
                await asyncio.sleep(0.05)
                pending.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await pending
            assert finished.is_set() and run._loading is None
            assert not run._processes
            await run.cleanup()
        finally:
            if run is not None:
                await run.cleanup()
            prepared.close()


@pytest.mark.asyncio
async def test_executor_initialization_failure_removes_only_new_run_root(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (scope, shards, allocated_source):
        prepared = _prepare(tmp_path, scope, shards, allocated_source)

        def _unavailable_executor(**options):
            raise RuntimeError("synthetic spawn resource refusal")

        monkeypatch.setattr(tasks, "ProcessPoolExecutor", _unavailable_executor)
        try:
            with pytest.raises(RuntimeError, match="resource refusal"):
                async with tasks.run_source_tasks(
                    prepared,
                    root=tmp_path / "workers",
                    register_cleanup=lambda path: None,
                    worker_limits=worker_limits(),
                ):
                    raise AssertionError("unavailable executor was opened")
            assert not (tmp_path / "workers").exists()
            assert Path(allocated_source.root).exists()
        finally:
            prepared.close()


async def _resume_missing_attempt(prepared, root, checkpoint, registered_roots):
    async with tasks.run_source_tasks(
        prepared,
        root=root,
        register_cleanup=registered_roots.append,
        worker_limits=worker_limits(),
        maximum_workers=2,
        completed=(checkpoint,),
    ) as resumed:
        observed_outputs = []
        async for output in resumed:
            if not observed_outputs:
                assert output.receipt["shard_id"] == 0
                assert resumed._pending and resumed._processes
            observed_outputs.append(output)
            assert output.status == "source_eof" and list(output.audit_rows())[-1][0] == "eof"
            list(output.batches(maximum_rows=2, maximum_bytes=1024**2))
        assert resumed.all_returned and resumed.peak_inflight <= 2
        assert {output.receipt["shard_id"] for output in observed_outputs} == set(range(len(prepared.files)))
        assert {output.receipt["shard_id"] for output in resumed.completed} == set(range(len(prepared.files)))
        assert not (root / "shard-0000").exists()
    assert all(not process.is_alive() for process in resumed._processes.values())
    return resumed, observed_outputs


@pytest.mark.asyncio
async def test_joined_crash_checkpoint_resumes_only_missing_shards_and_preserves_old_ownership(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (scope, shards, allocated):
        prepared = _prepare(tmp_path, scope, shards, allocated)
        original_ids, original_sha = _observed_records(allocated), allocated.sha256
        first = resumed = complete = None
        try:
            monkeypatch.setattr(tasks, "run_source_shard", _crash_second_source_worker)
            with pytest.raises(BrokenProcessPool):
                async with tasks.run_source_tasks(
                    prepared,
                    root=tmp_path / "attempt-one",
                    register_cleanup=lambda path: None,
                    worker_limits=worker_limits(),
                    maximum_workers=1,
                ) as first:
                    await _consume_outputs(first)
            assert all(not process.is_alive() for process in first._processes.values())
            assert [output.receipt["shard_id"] for output in first.completed] == [0]
            uncertain = tmp_path / "attempt-one" / "shard-0001" / "interrupted"
            assert uncertain.exists()
            checkpoint = pickle.loads(pickle.dumps(await first.checkpoint()))
            assert type(checkpoint) is tasks.JoinedSourceAttempt
            assert first._work.used >= checkpoint.file_bytes
            assert not first.all_returned and 1 in first.failures_by_shard
            original_output = first.completed[0]
            original_audit = original_output.sha256
            monkeypatch.setattr(tasks, "run_source_shard", _refuse_completed_worker_execution)
            registered_roots = []
            resumed, observed_outputs = await _resume_missing_attempt(
                prepared, tmp_path / "attempt-two", checkpoint, registered_roots
            )
            assert {path.name for path in registered_roots[1:]} == {"shard-0001", "shard-0002", "shard-0003"}
            second_checkpoint = pickle.loads(pickle.dumps(await resumed.checkpoint()))
            async with tasks.run_source_tasks(
                prepared,
                root=tmp_path / "attempt-three",
                register_cleanup=lambda path: None,
                worker_limits=worker_limits(),
                maximum_workers=2,
                completed=(checkpoint, second_checkpoint),
            ) as complete:
                outputs = await _consume_outputs(complete)
                assert complete.all_returned and len(outputs) == len(shards)
                assert not complete._processes and not complete._owned_completed
            await complete.cleanup()
            assert not (tmp_path / "attempt-three").exists() and uncertain.exists()
            await resumed.cleanup()
            assert not (tmp_path / "attempt-two").exists() and uncertain.exists()
            assert original_output.sha256 == original_audit and list(original_output.audit_rows())[-1][0] == "eof"
            assert not observed_outputs[0]._closed
            assert _observed_records(allocated) == original_ids and allocated.sha256 == original_sha
            await first.cleanup()
            assert not (tmp_path / "attempt-one").exists()
        finally:
            for run in (complete, resumed, first):
                if run is not None:
                    await run.cleanup()
            prepared.close()


def _rewrite_checkpoint(checkpoint, prepared, replace_receipt):
    path = Path(checkpoint.root) / tasks._JOINED
    work = tasks._BorrowedWork(Path(checkpoint.root), (checkpoint.device, checkpoint.inode), prepared._work.limits)
    with path.open("rb") as source:
        entries = list(work.read(source))
    if replace_receipt:
        first = entries[1][1]
        receipt = tasks._unpack_row(first[-1]) | {"result_sha256": b"x" * 32}
        entries[1] = "shard", (*first[:-1], tasks._pack_row(receipt))
    path.chmod(0o600)
    with path.open("wb") as output:
        for entry in entries:
            payload = tasks._pack_row(entry)
            output.write(struct.pack("!Q", len(payload)))
            output.write(payload)
    path.chmod(0o400)
    return replace(checkpoint, file_bytes=path.stat().st_size, sha256=hashlib.sha256(path.read_bytes()).digest())


async def _assert_resume_refused(prepared, root, completed, message):
    with pytest.raises(ValueError, match=message):
        async with tasks.run_source_tasks(
            prepared,
            root=root,
            register_cleanup=lambda path: None,
            worker_limits=worker_limits(),
            completed=completed,
        ):
            raise AssertionError("invalid completion opened a pool")
    assert not root.exists()


async def _assert_changed_metadata_refused(tmp_path, prepared, checkpoint, monkeypatch):
    original_load = prepared.load_plan
    populated = next(shard for shard in range(len(prepared.files)) if original_load(shard, worker_limits()).record_ids)

    def changed_metadata(shard, limits):
        plan = original_load(shard, limits)
        if shard == populated:
            ids_by_coordinate = dict(plan.record_ids)
            coordinate = next(iter(ids_by_coordinate))
            revision, pack, root_id = ids_by_coordinate[coordinate]
            ids_by_coordinate[coordinate] = revision + 313, pack, root_id
            plan = replace(plan, record_ids=ids_by_coordinate)
        return plan

    with monkeypatch.context() as changed:
        changed.setattr(prepared, "load_plan", changed_metadata)
        await _assert_resume_refused(prepared, tmp_path / "metadata-change", (checkpoint,), "native metadata")


async def _assert_changed_files_refused(tmp_path, prepared, checkpoint, output):
    path = Path(checkpoint.root) / tasks._JOINED
    original_bytes = path.read_bytes()
    try:
        changed_checkpoint = _rewrite_checkpoint(checkpoint, prepared, True)
        await _assert_resume_refused(prepared, tmp_path / "receipt-change", (changed_checkpoint,), "receipt differs")
        path.chmod(0o600)
        path.write_bytes(original_bytes[:-1] + bytes([original_bytes[-1] ^ 1]))
        path.chmod(0o400)
        await _assert_resume_refused(prepared, tmp_path / "manifest-change", (checkpoint,), "attempt bytes")
    finally:
        path.chmod(0o600)
        path.write_bytes(original_bytes)
        path.chmod(0o400)
    audit_bytes = output.path.read_bytes()
    try:
        output.path.chmod(0o600)
        output.path.write_bytes(audit_bytes[:-1] + bytes([audit_bytes[-1] ^ 1]))
        output.path.chmod(0o400)
        await _assert_resume_refused(prepared, tmp_path / "audit-change", (checkpoint,), "artifact bytes")
    finally:
        output.path.chmod(0o600)
        output.path.write_bytes(audit_bytes)
        output.path.chmod(0o400)
    foreign = output.path.parent / "foreign"
    foreign.write_text("preserve")
    try:
        await _assert_resume_refused(prepared, tmp_path / "foreign-file", (checkpoint,), "file census")
        assert foreign.read_text() == "preserve"
    finally:
        foreign.unlink()


@pytest.mark.asyncio
async def test_resume_refuses_raw_duplicate_foreign_and_changed_completed_evidence(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (scope, shards, allocated):
        prepared = _prepare(tmp_path, scope, shards, allocated)
        run = None
        try:
            async with tasks.run_source_tasks(
                prepared,
                root=tmp_path / "original",
                register_cleanup=lambda path: None,
                worker_limits=worker_limits(),
                maximum_workers=2,
            ) as run:
                with pytest.raises(ValueError, match="joined"):
                    await run.checkpoint()
                await _consume_outputs(run)
            checkpoint = await run.checkpoint()
            cases = (
                (run.completed, tmp_path / "raw", "joined descriptors"),
                ((checkpoint, checkpoint), tmp_path / "duplicate", "disjoint"),
                ((checkpoint,), tmp_path / "original" / "nested", "disjoint"),
                ((replace(checkpoint, scope_sha256=b"x" * 32),), tmp_path / "scope", "scope"),
            )
            for completed, root, message in cases:
                await _assert_resume_refused(prepared, root, completed, message)
            await _assert_changed_metadata_refused(tmp_path, prepared, checkpoint, monkeypatch)
            await _assert_changed_files_refused(tmp_path, prepared, checkpoint, run.completed[0])
            with monkeypatch.context() as changed:
                changed.setattr(prepared, "complete_scope", False)
                await _assert_resume_refused(prepared, tmp_path / "scope-change", (checkpoint,), "scope")
            with monkeypatch.context() as changed:
                changed.setattr(prepared, "source", replace(allocated, sha256=b"a" * 32))
                await _assert_resume_refused(prepared, tmp_path / "allocation-change", (checkpoint,), "scope")
            assert all(output.path.exists() for output in run.completed)
        finally:
            if run is not None:
                await run.cleanup()
            prepared.close()


@pytest.mark.asyncio
async def test_refused_worker_result_cannot_be_exported_as_joined_completion(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (scope, shards, allocated):
        prepared = _prepare(tmp_path, replace(scope, complete_scope=False), shards, allocated)
        run = None
        try:
            async with tasks.run_source_tasks(
                prepared,
                root=tmp_path / "refused",
                register_cleanup=lambda path: None,
                worker_limits=worker_limits(),
                maximum_workers=2,
            ) as run:
                await _consume_outputs(run)
            assert all(not process.is_alive() for process in run._processes.values())
            assert any(output.status == "refused" for output in run.completed)
            refused = next(output for output in run.completed if output.status == "refused")
            custody = tasks.source_output_custody(prepared, refused, tmp_path / "refused", worker_limits())
            assert custody.artifact[0] == refused.receipt["shard_id"]
            with pytest.raises(CandidateRejected, match="candidate or structural"):
                list(refused.batches(maximum_rows=32, maximum_bytes=1024**2))
            survivors_by_path = {
                path.relative_to(run._work.root): (path.stat().st_ino, path.read_bytes())
                for path in run._work.root.rglob("*")
                if path.is_file()
            }
            accounting = (dict(run._work.paths), run._work.next_file, run._work.used, run._work._entry_bytes)
            with pytest.raises(ValueError, match="refused"):
                await run.checkpoint()
            assert not (tmp_path / "refused" / tasks._JOINED).exists()
            assert {
                path.relative_to(run._work.root): (path.stat().st_ino, path.read_bytes())
                for path in run._work.root.rglob("*")
                if path.is_file()
            } == survivors_by_path
            assert (dict(run._work.paths), run._work.next_file, run._work.used, run._work._entry_bytes) == accounting
            with pytest.raises(ValueError, match="refused"):
                tasks._validate_output(prepared, refused, refused.receipt["shard_id"], worker_limits())
        finally:
            if run is not None:
                await run.cleanup()
            prepared.close()


@pytest.mark.asyncio
async def test_repeated_stop_cancellation_joins_processes_before_checkpoint(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (scope, shards, allocated):
        prepared = _prepare(tmp_path, scope, shards, allocated)
        entered, release = threading.Event(), threading.Event()
        original = tasks._join_pool
        run = None

        def blocked_join(*arguments):
            entered.set()
            if not release.wait(5):
                raise TimeoutError("test did not release process join")
            original(*arguments)

        monkeypatch.setattr(tasks, "_join_pool", blocked_join)
        try:
            async with tasks.run_source_tasks(
                prepared,
                root=tmp_path / "join-cancel",
                register_cleanup=lambda path: None,
                worker_limits=worker_limits(),
                maximum_workers=2,
            ) as run:
                await anext(aiter(run))
                stopping = asyncio.create_task(run.stop(terminate=True))
                await _cancel_stopping(run, stopping, entered, release)
                assert not run._running and all(not process.is_alive() for process in run._processes.values())
                checkpoint = await run.checkpoint()
                assert Path(checkpoint.root, tasks._JOINED).exists()
        finally:
            release.set()
            if run is not None:
                await run.cleanup()
            prepared.close()


async def _cancel_stopping(run, stopping, entered, release):
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        for message in ("first process cancellation", "second", "third"):
            stopping.cancel(message)
            for _ in range(3):
                await asyncio.sleep(0)
        assert not stopping.done() and run._running
        with pytest.raises(ValueError, match="joined"):
            await run.checkpoint()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError) as captured:
            await stopping
    assert captured.value.args == ("first process cancellation",)
