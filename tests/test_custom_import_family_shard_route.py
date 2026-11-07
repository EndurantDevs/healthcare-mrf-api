# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real partition custody and process-safe complete raw/typed reconciliation."""

import asyncio
import hashlib
import json
import multiprocessing
import pickle
import threading
from concurrent.futures import ProcessPoolExecutor
from contextlib import suppress
from dataclasses import asdict, replace

import pytest

from process.custom_import import family_shard_route as route
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_shard_facts import SourceFactLimits, prepare_source_families
from process.custom_import.family_shard_input import cleanup_spool
from process.custom_import.family_shard_input_records import encode_record
from process.custom_import.family_shard_plan import PlanWork, _cpu, _Scratch
from process.custom_import.family_shard_route import prepare_routed_families, route_capture
from process.custom_import.runner_types import CancellationRequested
from tests.test_custom_import_family_shard_build import _compound_definition, _root
from tests.test_custom_import_family_shard_facts import _records
from tests.test_custom_import_family_shard_input import _all_records, _capture_input, _child, _partition


def _limits():
    return SourceFactLimits(64 * 1024**2, 1024**2, 4096)


def _snapshot(families):
    return [
        (
            family.typed_key,
            tuple(
                (encode_record(record), asdict(facts), asdict(decision)) for record, facts, decision in family.rows()
            ),
            None if family.root_resolution is None else asdict(family.root_resolution),
            family.candidate_errors,
            family.fatal_problem,
        )
        for family in families
    ]


def _worker(arguments):
    shard, canonical, root = arguments
    definition = CustomImportDefinition.from_mapping(json.loads(canonical))
    with prepare_routed_families(
        shard, definition, root=root, register_cleanup=lambda path: None, limits=_limits()
    ) as families:
        return _snapshot(families)


@pytest.mark.asyncio
async def test_real_partition_parallel_processes_match_complete_serial_facts(tmp_path):
    definition, captured_source, parts = _capture_input(
        {
            "root_source": [[_root(), _root("1234567893", None)], [_root()]],
            "detail_source": [[_child(), _child("1234567893")], [_child(), _child("absent-parent")]],
        }
    )
    partition = await _partition(tmp_path, definition, captured_source, parts)
    input_records = _all_records(partition)
    try:
        with prepare_source_families(
            iter(input_records),
            definition,
            root=tmp_path / "serial",
            register_cleanup=lambda path: None,
            limits=_limits(),
            expected_records=len(input_records),
        ) as families:
            expected = _snapshot(families)
        registered_paths = []
        with route_capture(
            partition, definition, root=tmp_path / "route", register_cleanup=registered_paths.append, limits=_limits()
        ) as shards:
            assert len(shards) == 4 and sum(shard.records for shard in shards) == len(input_records)
            assert pickle.loads(pickle.dumps(shards)) == shards
            arguments = [(shard, definition.canonical, str(tmp_path / f"worker-{shard.shard_id}")) for shard in shards]
            with ProcessPoolExecutor(max_workers=2, mp_context=multiprocessing.get_context("spawn")) as executor:
                actual_families = [
                    family for worker_families in executor.map(_worker, arguments) for family in worker_families
                ]
            order = lambda family: (family[0] is None, b"" if family[0] is None else family[0][1])
            assert sorted(actual_families, key=order) == sorted(expected, key=order)
        assert registered_paths == [tmp_path / "route"] and not (tmp_path / "route").exists()
        assert not list(tmp_path.glob("worker-*"))
    finally:
        cleanup_spool(partition)


@pytest.mark.asyncio
async def test_invalid_raw_equal_records_are_routed_to_their_typed_parent(tmp_path, monkeypatch):
    definition = _compound_definition()
    records = _records(definition, {"root_source": [[_root(score=1), _root(score=True)]], "detail_source": [[]]})
    # A physical retained part cannot mix boolean/integer Arrow types; exercise
    # the already validated lossless partition-record boundary directly here.
    from types import SimpleNamespace

    from process.custom_import import family_shard_route as route

    partition = SimpleNamespace(identity=SimpleNamespace(definition_sha256=definition.digest), sha256="a" * 64)
    monkeypatch.setattr(route, "manifest_document", lambda value: {"records": 2, "limits": {"shard_count": 4}})
    monkeypatch.setattr(route, "read_partition", lambda value, **options: iter(records))
    with route_capture(
        partition, definition, root=tmp_path / "route", register_cleanup=lambda path: None, limits=_limits()
    ) as shards:
        actual_families = [
            family
            for shard in shards
            for family in _worker((shard, definition.canonical, tmp_path / f"w{shard.shard_id}"))
        ]
    assert len(actual_families) == 1 and actual_families[0][1][0][1]["raw_root_count"] == 2
    assert [row[2]["code"] for row in actual_families[0][1]] == ["duplicate_root_key", "field_type_invalid"]


def _changed_shard(tmp_path, shard, change):
    if change == "count":
        return replace(shard, records=shard.records + 1)
    if change == "definition":
        return replace(shard, definition_sha256="b" * 64)
    path = tmp_path / "route" / shard.name
    if change == "bytes":
        path.chmod(0o600)
        original_bytes = path.read_bytes()
        path.write_bytes(original_bytes[:-1] + bytes((original_bytes[-1] ^ 1,)))
        path.chmod(0o400)
    else:
        original = tmp_path / "route" / "preserved"
        path.rename(original)
        path.symlink_to(original)
    return shard


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["bytes", "count", "symlink", "definition"])
async def test_worker_refuses_changed_custody_scope_and_census(tmp_path, change):
    definition, source, parts = _capture_input({"root_source": [[_root()]], "detail_source": [[_child()]]})
    partition = await _partition(tmp_path, definition, source, parts)
    try:
        with route_capture(
            partition, definition, root=tmp_path / "route", register_cleanup=lambda path: None, limits=_limits()
        ) as shards:
            shard = _changed_shard(tmp_path, next(shard for shard in shards if shard.records), change)
            with pytest.raises((ValueError, OSError)):
                _worker((shard, definition.canonical, tmp_path / "worker"))
            assert not (tmp_path / "worker").exists()
    finally:
        cleanup_spool(partition)


@pytest.mark.asyncio
async def test_checked_route_preserves_every_original_byte_and_descriptor_value(tmp_path):
    definition, capture_source, parts = _capture_input(
        {"root_source": [[_root(), _root("1234567893", None)]], "detail_source": [[_child(), _child("1234567893")]]}
    )
    partition = await _partition(tmp_path, definition, capture_source, parts)
    checks, registered = [], []
    try:
        with (
            route_capture(
                partition, definition, root=tmp_path / "default", register_cleanup=registered.append, limits=_limits()
            ) as original,
            route_capture(
                partition,
                definition,
                root=tmp_path / "checked",
                register_cleanup=registered.append,
                limits=_limits(),
                check=lambda: checks.append(True),
            ) as checked,
        ):
            for first, second in zip(original, checked, strict=True):
                original_path, checked_path = tmp_path / "default" / first.name, tmp_path / "checked" / second.name
                assert original_path.read_bytes() == checked_path.read_bytes()
                with original_path.open("rb") as source_file:
                    assert first.sha256 == second.sha256 == hashlib.file_digest(source_file, "sha256").digest()
                assert (
                    replace(
                        second,
                        root=first.root,
                        root_device=first.root_device,
                        root_inode=first.root_inode,
                        file_device=first.file_device,
                        file_inode=first.file_inode,
                    )
                    == first
                )
        assert checks and registered == [tmp_path / "default", tmp_path / "checked"]
        assert not (tmp_path / "default").exists() and not (tmp_path / "checked").exists()
    finally:
        cleanup_spool(partition)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["input", "routing", "digest"])
async def test_stop_reaches_original_input_route_and_digest_and_preserves_originals(tmp_path, monkeypatch, phase):
    definition, capture_source, parts = _capture_input({"root_source": [[_root()]], "detail_source": [[_child()]]})
    partition = await _partition(tmp_path, definition, capture_source, parts)
    originals = _all_records(partition)
    failure = RuntimeError("synthetic route owner stop")
    active, registered, ticks = [], [], []
    read_partition, write_shard, digest_file = route.read_partition, route._write_shard, route._plan_digest

    def checked():
        if active:
            ticks.append(True)
            if phase != "digest" or len(ticks) == 2:
                raise failure

    def inputs(*arguments, **options):
        if phase == "input":
            active.append(True)
        yield from read_partition(*arguments, **options)

    def routing(*arguments):
        if phase == "routing" and arguments[1]:
            active.append(True)
        return write_shard(*arguments)

    def digest(source_file, check):
        assert check is checked
        if phase == "digest" and source_file.seek(0, 2):
            active.append(True)
        source_file.seek(0)
        return digest_file(source_file, check)

    monkeypatch.setattr(route, "read_partition", inputs)
    monkeypatch.setattr(route, "_write_shard", routing)
    monkeypatch.setattr(route, "_plan_digest", digest)
    try:
        with (
            pytest.raises(RuntimeError) as caught,
            route_capture(
                partition,
                definition,
                root=tmp_path / "route",
                register_cleanup=registered.append,
                limits=_limits(),
                check=checked,
            ),
        ):
            raise AssertionError("stopped routing must not expose a closed shard")
        assert caught.value is failure and ticks
        assert registered == [tmp_path / "route"] and not (tmp_path / "route").exists()
        assert _all_records(partition) == originals
    finally:
        cleanup_spool(partition)


@pytest.mark.asyncio
async def test_cancelled_routing_drains_before_removal_and_keeps_capture(tmp_path, monkeypatch):
    definition, capture_source, parts = _capture_input({"root_source": [[_root()]], "detail_source": [[_child()]]})
    partition = await _partition(tmp_path, definition, capture_source, parts)
    originals = _all_records(partition)
    entered_events, stopped_events, release_events, drained_events = (threading.Event() for _index in range(4))
    scratch = _Scratch(PlanWork(tmp_path / "driver", _limits(), lambda _path: None))
    route_root = tmp_path / "route"
    early_removals = []

    def blocked(source_file, check):
        entered_events.set()
        try:
            while not release_events.wait(0.005):
                try:
                    check()
                except CancellationRequested:
                    stopped_events.set()
                    assert release_events.wait(5)
                    raise
        finally:
            early_removals.append(not route_root.exists())
            drained_events.set()

    monkeypatch.setattr(route, "_plan_digest", blocked)
    context = route_capture(
        partition,
        definition,
        root=route_root,
        register_cleanup=lambda _path: None,
        limits=_limits(),
        check=scratch.current,
    )
    pending = asyncio.create_task(_cpu(scratch, context.__enter__))
    try:
        assert await asyncio.to_thread(entered_events.wait, 5)
        pending.cancel("first routing cancellation")
        assert await asyncio.to_thread(stopped_events.wait, 5)
        assert route_root.exists() and not pending.done()
        pending.cancel("second routing cancellation")
        release_events.set()
        with pytest.raises(asyncio.CancelledError, match="first routing cancellation"):
            await pending
        assert drained_events.is_set() and early_removals == [False] and not route_root.exists()
        assert _all_records(partition) == originals
    finally:
        release_events.set()
        if not pending.done():
            pending.cancel()
            with suppress(asyncio.CancelledError):
                await pending
        scratch.close()
        cleanup_spool(partition)


@pytest.mark.asyncio
async def test_invalid_route_check_is_refused_before_resource_creation(tmp_path):
    definition, source, parts = _capture_input({"root_source": [[]], "detail_source": [[]]})
    partition = await _partition(tmp_path, definition, source, parts)
    registered_paths = []
    try:
        with (
            pytest.raises(ValueError, match="cancellation check"),
            route_capture(
                partition,
                definition,
                root=tmp_path / "route",
                register_cleanup=registered_paths.append,
                limits=_limits(),
                check=True,
            ),
        ):
            raise AssertionError("invalid callback must be refused")
        assert not registered_paths and not (tmp_path / "route").exists()
    finally:
        cleanup_spool(partition)
