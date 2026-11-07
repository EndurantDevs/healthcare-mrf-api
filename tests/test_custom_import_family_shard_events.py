# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Original SOURCE IDs, observed admission IDs and complete resolved-target replay."""

import asyncio
import json
import pickle
import threading
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from process.custom_import import family_shard_admission as audit
from process.custom_import import family_shard_allocate as allocation
from process.custom_import import family_shard_checkpoint as checkpoint
from process.custom_import import family_shard_events as events
from process.custom_import import family_shard_events_journal as journal_api
from process.custom_import import family_shard_metadata as metadata
from process.custom_import import family_shard_metadata_journal as metadata_journal
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_shard_allocate import SourceAllocationLimits
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded, SourceFactLimits
from process.custom_import.family_shard_identity import GlobalIdentity
from tests import test_custom_import_family_shard_admission as admission_fixture
from tests.test_custom_import_family_shard_admission import _build_audit, _inputs
from tests.test_custom_import_family_shard_source_plan import _child, _root
from tests.test_custom_import_family_shard_worker import _projected_definition

_STAMP = datetime(2030, 1, 2, 3, 4, 5, tzinfo=timezone.utc)


@asynccontextmanager
async def _original_inputs(tmp_path, monkeypatch, rows, definition):
    owners = SimpleNamespace()
    original_allocate, original_boundary = allocation.allocate_source_plan, admission_fixture._native_boundary

    async def capture_journal(*args, **kwargs):
        owners.journal = kwargs["journal"]
        return await original_allocate(*args, **kwargs)

    def capture_boundary(*args, **kwargs):
        owners.native = original_boundary(*args, **kwargs)
        return owners.native

    monkeypatch.setattr(allocation, "allocate_source_plan", capture_journal)
    monkeypatch.setattr(admission_fixture, "_native_boundary", capture_boundary)
    try:
        async with _inputs(tmp_path, monkeypatch, rows, definition=definition, complete_scope=False) as values:
            yield (*values, owners)
    finally:
        if hasattr(owners, "journal"):
            owners.journal.close_terminal()


@asynccontextmanager
async def _case(tmp_path, monkeypatch, rows=None, *, definition=None):
    rows = rows or {
        "root_source": [[_root(), _root(npi=None)]],
        "detail_source": [[_child(amount="1"), _child(amount="2"), _child(key=None)]],
    }
    async with _original_inputs(tmp_path, monkeypatch, rows, definition) as (case, allocated, workers, owners):
        limits = SourceFactLimits(64 * 1024**2, 1024**2, 512)
        admission = _build_audit(tmp_path, case, allocated, workers, limits, complete_scope=case[1].complete_scope)
        layout = FamilyBlockLayout(case[1].definition, case[2].child_collection_slots)
        allocation_limits = SourceAllocationLimits(owners.journal.work.limits, 32, 1024**2)
        _metadata_boundary(monkeypatch, allocated)
        try:
            async with metadata.owner_source_metadata(
                None, case[1], layout, allocated, journal=owners.journal, limits=allocation_limits
            ) as original_metadata:
                yield SimpleNamespace(
                    request=case[1],
                    layout=layout,
                    source=allocated,
                    admission=admission,
                    journal=owners.journal,
                    native=owners.native,
                    limits=allocation_limits,
                    metadata=original_metadata,
                    capture=case,
                    workers=workers,
                )
        finally:
            audit.cleanup_admission_output(admission)


def _metadata_boundary(monkeypatch, source):
    roots_by_key = {
        record.typed_parent_key: roots[index]
        for pack, _pack, roots, _revisions, _rejections in allocation.read_source_allocations(source)
        for index, record in enumerate(pack.records)
        if record.typed_parent_key is not None
    }

    async def intern(_session, _definition, _dataset, keys, **_limits):
        return tuple(GlobalIdentity(roots_by_key[(key.root_key_text, key.root_key_sha256)], 9001) for key in keys)

    async def allocate(_session, kinds):
        assert set(kinds) == {"family_revision"}
        return tuple(2001 + 17 * index for index in range(len(kinds)))

    monkeypatch.setattr(metadata, "intern_global_identities", intern)
    monkeypatch.setattr(metadata, "_allocate_record_ids", allocate)
    monkeypatch.setattr(metadata_journal, "_page_session", allocation._page_session)
    monkeypatch.setattr(metadata_journal, "_execute", allocation._execute)


def _native_boundary(monkeypatch, native, *, wrong_capture=False, fail_commit=None):
    observations, observed_ids = [], []
    native.owner_stamps = []
    original_page = allocation._page_session
    capture_by_field = dict(native.source.source.scope[0])

    @asynccontextmanager
    async def page(factory, request, build):
        assert request is native.request and build == native.journal.build_id
        window = sum(item[0] == "begin" for item in observations) + 1
        observations.append(("begin", window))
        async with original_page(factory, request, build) as (session, owner):
            yield session, owner
            if session.writing:
                writing = sum(item[0] == "writing" for item in observations) + 1
                observations.append(("writing", writing))
                if writing == fail_commit:
                    raise RuntimeError("changed current owner before admission allocation commit")
        observations.append(("commit", window))

    async def candidate(session, request, family, layout, phases):
        assert (request, family, layout, phases) == (
            native.request,
            native.source.snapshot_family_id,
            native.layout,
            {"loading"},
        )
        observations.append(("candidate", family))

    async def execute(session, statement, parameters=None):
        if "custom_import_execution" in str(statement):
            observations.append(("capture",))
            return SimpleNamespace(
                one=lambda: (
                    -1 if wrong_capture else capture_by_field["capture_bundle_id"],
                    capture_by_field["source_binding_revision_id"],
                )
            )
        assert statement == "SELECT pg_catalog.transaction_timestamp()" and parameters == {}
        observations.append(("timestamp",))
        stamp = _STAMP + timedelta(seconds=len(observations))
        native.owner_stamps.append(stamp)
        return SimpleNamespace(scalar_one=lambda: stamp)

    async def allocate(session, kinds):
        assert kinds and all(kind == "rejection" for kind in kinds)
        observations.append(("allocate", len(kinds)))
        identifiers = tuple(10007 + 23 * index for index in range(len(observed_ids), len(observed_ids) + len(kinds)))
        observed_ids.extend(identifiers)
        return identifiers

    monkeypatch.setattr(journal_api, "_page_session", page)
    monkeypatch.setattr(checkpoint, "_page_session", page)
    monkeypatch.setattr(events, "_candidate", candidate)
    monkeypatch.setattr(events, "_execute", execute)
    monkeypatch.setattr(events, "_allocate_record_ids", allocate)
    return observations, observed_ids


async def _allocate(tmp_path, native, *, limits=None, register=None):
    if register is not None:
        native.journal.work = replace(native.journal.work, register_cleanup=register)
    return await events.allocate_admission_events(
        None,
        native.request,
        native.layout,
        native.source,
        native.admission,
        journal=native.journal,
        limits=limits or native.limits,
    )


def _attempts(native):
    return tuple(path for path in (native.journal.root / "outputs").iterdir() if path.name.startswith("events-"))


def _copy_rows(descriptor):
    return [
        dict(zip(events._COLUMNS, row, strict=True))
        for batch in events.admission_event_batches(descriptor, maximum_rows=1, maximum_bytes=1024**2)
        for row in batch["rejection"]
    ]


@pytest.mark.asyncio
async def test_pending_events_preserve_source_ids_and_exact_native_copy(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        observations, native_ids = _native_boundary(monkeypatch, native)
        registered_roots = []
        descriptor = await _allocate(tmp_path, native, register=registered_roots.append)
        try:
            assert Path(descriptor.root) in registered_roots
            assert Path(descriptor.root).parent == native.journal.root / "outputs"
            assert pickle.loads(pickle.dumps(descriptor)) == descriptor
            original_events = list(audit.read_admission_events(native.admission))
            actual_events = list(events.read_allocated_events(descriptor))
            assert len(actual_events) == len(original_events) and actual_events
            source_ids = {event.rejection_id for event in original_events if event.origin == "source"}
            added_events = [event for event in actual_events if event.origin == "admission"]
            assert [event.rejection_id for event in added_events] == native_ids
            assert native_ids and all(right - left == 23 for left, right in zip(native_ids, native_ids[1:]))
            for expected, observed in zip(original_events, actual_events, strict=True):
                assert replace(observed, rejection_id=expected.rejection_id) == expected
                if expected.origin == "source":
                    assert observed.rejection_id == expected.rejection_id
            assert [event.ordinal for event in actual_events] == list(range(len(actual_events)))
            assert min(event.ordinal for event in added_events) == len(source_ids) == native.source.source.counts[4]
            copy_rows = _copy_rows(descriptor)
            assert {copy_row["rejection_id"] for copy_row in copy_rows} == set(native_ids)
            assert not source_ids.intersection(copy_row["rejection_id"] for copy_row in copy_rows)
            for copy_row, event in zip(copy_rows, added_events, strict=True):
                assert copy_row["rejection_ordinal"] == event.ordinal
                assert copy_row["canonical_evidence"] == event.canonical_evidence == "{}"
                assert (copy_row["root_key_sha256"], copy_row["canonical_root_key"], copy_row["pack_id"]) == (
                    event.root_key_sha256,
                    event.canonical_root_key,
                    event.pack_id,
                )
                assert copy_row["source_ordinal"] == event.source_ordinal
                assert copy_row["collection_slot"] == event.collection_slot == 7
                assert copy_row["field_slot"] is None
                assert copy_row["created_at"] == datetime.fromisoformat(descriptor.owner_created_at)
                assert copy_row["producing_token_sha256"] == native.source.source.scope[3]
                assert copy_row["producing_fence"] == native.request.fence
            assert copy_rows == _copy_rows(descriptor)
            with pytest.raises(FamilyBlockResourceLimit):
                list(events.admission_event_batches(descriptor, maximum_rows=1, maximum_bytes=1))
            assert copy_rows == _copy_rows(descriptor)
            assert descriptor.owner_created_at == native.owner_stamps[0].isoformat()
            assert descriptor.role_counts == {"rejection": len(added_events)}
            assert [observation[1] for observation in observations if observation[0] == "allocate"] == [
                len(added_events)
            ]
            assert sum(observation[0] == "writing" for observation in observations) == 3
            by_ordinal = {event.ordinal: event for event in actual_events}
            expected_resolutions = list(audit.read_admission_resolutions(native.admission))
            assert list(events.read_event_resolutions(descriptor)) == [
                audit.AdmissionResolution(resolution.target, by_ordinal[resolution.event.ordinal])
                for resolution in expected_resolutions
            ]
            assert Path(native.admission.root).exists() and Path(native.source.root).exists()
        finally:
            events.cleanup_admission_events(descriptor)
        assert not Path(descriptor.root).exists()


@pytest.mark.asyncio
async def test_earliest_coordinate_resolution_keeps_admission_id_over_lower_source_ordinal(tmp_path, monkeypatch):
    document = json.loads(_projected_definition().canonical)
    document["streams"].reverse()
    rows_by_stream = {"root_source": [[_root(score=None)]], "detail_source": [[_child(amount="1"), _child(amount="2")]]}
    async with _case(
        tmp_path, monkeypatch, rows_by_stream, definition=CustomImportDefinition.from_mapping(document)
    ) as native:
        _native_boundary(monkeypatch, native)
        descriptor = await _allocate(tmp_path, native)
        try:
            actual_events = list(events.read_allocated_events(descriptor))
            assert actual_events[0].origin == "source" and actual_events[0].ordinal == 0
            chosen = next(
                resolution for resolution in events.read_event_resolutions(descriptor) if resolution.target == (2, 0)
            )
            assert chosen.event.origin == "admission" and chosen.event.coordinate == (1, 0)
            assert chosen.event.ordinal == 1 and chosen.event.rejection_id != actual_events[0].rejection_id
            assert chosen.event.source_ordinal is None
        finally:
            events.cleanup_admission_events(descriptor)


@pytest.mark.asyncio
async def test_empty_complete_input_still_checks_two_fresh_owner_windows(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch, {"root_source": [[]], "detail_source": [[]]}) as native:
        observations, native_ids = _native_boundary(monkeypatch, native)
        descriptor = await _allocate(tmp_path, native)
        try:
            assert descriptor.counts == (0, 0, 0) and descriptor.role_counts == {"rejection": 0}
            assert not list(events.read_allocated_events(descriptor))
            assert not list(events.read_event_resolutions(descriptor)) and not _copy_rows(descriptor)
            assert not native_ids and sum(item[0] == "writing" for item in observations) == 2
        finally:
            events.cleanup_admission_events(descriptor)


@pytest.mark.asyncio
@pytest.mark.parametrize("source_rejected", [False, True])
async def test_source_only_events_allocate_no_ids_and_copy_no_rows(tmp_path, monkeypatch, source_rejected):
    rows_by_stream = {"root_source": [[_root(score=None) if source_rejected else _root()]], "detail_source": [[]]}
    async with _case(tmp_path, monkeypatch, rows_by_stream) as native:
        observations, native_ids = _native_boundary(monkeypatch, native)
        descriptor = await _allocate(tmp_path, native)
        try:
            original_events = list(audit.read_admission_events(native.admission))
            assert list(events.read_allocated_events(descriptor)) == original_events
            assert len(original_events) == int(source_rejected)
            assert descriptor.role_counts == {"rejection": 0} and not _copy_rows(descriptor)
            assert not native_ids and sum(observation[0] == "writing" for observation in observations) == 2
        finally:
            events.cleanup_admission_events(descriptor)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["source_sha256", "allocation_sha256", "complete_scope", "worker_inputs"])
async def test_changed_source_admission_scope_or_cohort_refuses_before_allocation(tmp_path, monkeypatch, changed):
    async with _case(tmp_path, monkeypatch) as native:
        observations, native_ids = _native_boundary(monkeypatch, native)
        value = (
            not native.request.complete_scope
            if changed == "complete_scope"
            else ((1, b"a" * 32, b"b" * 32),)
            if changed == "worker_inputs"
            else b"z" * 32
        )
        native.admission = replace(native.admission, **{changed: value})
        with pytest.raises(FamilyBlockUnavailable):
            await _allocate(tmp_path, native)
        assert not observations and not native_ids and not _attempts(native)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["capture", "commit", "budget", "late_resolution"])
async def test_failure_preserves_original_inputs_and_retains_its_accounted_output(tmp_path, monkeypatch, failure):
    async with _case(tmp_path, monkeypatch) as native:
        _observations, native_ids = _native_boundary(
            monkeypatch, native, wrong_capture=failure == "capture", fail_commit=2 if failure == "commit" else None
        )
        sibling = tmp_path / "preserve"
        sibling.write_text("keep")
        limits = replace(native.limits, maximum_batch_bytes=1) if failure == "budget" else native.limits
        original = events.read_admission_resolutions

        def late(descriptor):
            yield from original(descriptor)
            raise ValueError("late frozen admission resolution EOF failed")

        if failure == "late_resolution":
            monkeypatch.setattr(events, "read_admission_resolutions", late)
        with pytest.raises((FamilyBlockUnavailable, FamilyBlockResourceLimit, RuntimeError, ValueError)):
            await _allocate(tmp_path, native, limits=limits)
        assert _attempts(native) and sibling.read_text() == "keep"
        assert Path(native.source.root).exists() and Path(native.admission.root).exists()
        assert bool(native_ids) == (failure in {"commit", "late_resolution"})


@pytest.mark.asyncio
async def test_layout_collection_scope_must_match_original_source(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        native.layout = FamilyBlockLayout(native.request.definition, {"details": 8})
        observations, native_ids = _native_boundary(monkeypatch, native)
        with pytest.raises(FamilyBlockUnavailable, match="definition or cohort"):
            await _allocate(tmp_path, native)
        assert not observations and not native_ids and not _attempts(native)


@pytest.mark.asyncio
async def test_corrupt_frozen_admission_input_finishes_eof_check_before_any_sequence_use(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        _observations, native_ids = _native_boundary(monkeypatch, native)
        path = Path(native.admission.root) / audit._FILE
        original = path.read_bytes()
        path.chmod(0o600)
        path.write_bytes(original[:-1] + bytes([original[-1] ^ 1]))
        path.chmod(0o400)
        with pytest.raises(ValueError):
            await _allocate(tmp_path, native)
        assert not native_ids and _attempts(native)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["source_id", "typed_key", "pack_id"])
async def test_event_evidence_must_match_actual_source_before_native_allocation(tmp_path, monkeypatch, changed):
    async with _case(tmp_path, monkeypatch) as native:
        _observations, native_ids = _native_boundary(monkeypatch, native)
        original = events.read_admission_events

        def altered(descriptor):
            for event in original(descriptor):
                if changed == "source_id" and event.origin == "source":
                    event = replace(event, rejection_id=event.rejection_id + 1)
                elif changed == "typed_key" and event.origin == "admission":
                    event = replace(event, canonical_root_key="byte-distinct-root")
                elif changed == "pack_id" and event.origin == "admission":
                    event = replace(event, pack_id=event.pack_id + 1)
                yield event

        monkeypatch.setattr(events, "read_admission_events", altered)
        with pytest.raises(FamilyBlockUnavailable, match="original"):
            await _allocate(tmp_path, native)
        assert not native_ids and _attempts(native)


@pytest.mark.asyncio
async def test_native_repeated_ids_are_refused_without_reclaiming_sequence_results(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        _native_boundary(monkeypatch, native)
        observed_ids = []

        async def repeated(session, kinds):
            observed_ids.extend([10007] * len(kinds))
            return (10007,) * len(kinds)

        monkeypatch.setattr(events, "_allocate_record_ids", repeated)
        with pytest.raises(FamilyBlockUnavailable, match="repeats"):
            await _allocate(tmp_path, native)
        assert observed_ids == [10007, 10007] and _attempts(native)


@pytest.mark.asyncio
async def test_completed_output_tamper_and_foreign_cleanup_are_refused(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        _native_boundary(monkeypatch, native)
        descriptor = await _allocate(tmp_path, native)
        path = Path(descriptor.root) / events._FILE
        original = path.read_bytes()
        try:
            path.chmod(0o600)
            path.write_bytes(original[:-1] + bytes([original[-1] ^ 1]))
            path.chmod(0o400)
            with pytest.raises(ValueError):
                list(events.read_allocated_events(descriptor))
            path.chmod(0o600)
            path.write_bytes(original)
            path.chmod(0o400)
            sibling = Path(descriptor.root) / "foreign"
            sibling.write_text("preserve")
            with pytest.raises(ValueError, match="custody"):
                events.cleanup_admission_events(descriptor)
            assert sibling.read_text() == "preserve"
            sibling.unlink()
        finally:
            events.cleanup_admission_events(descriptor)


@pytest.mark.asyncio
async def test_expired_event_work_preserves_source_and_admission(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        _observations, native_ids = _native_boundary(monkeypatch, native)
        monkeypatch.setattr(journal_api, "_read_deadline", lambda _request: 0)
        with pytest.raises(TimeoutError, match="operation deadline"):
            await _allocate(tmp_path, native)
        assert not _attempts(native) and not native.journal.busy
        assert not native_ids and not native.owner_stamps
        assert Path(native.source.root).exists() and Path(native.admission.root).exists()


@pytest.mark.asyncio
async def test_repeated_cancel_joins_active_event_file_work_before_cleanup(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        _native_boundary(monkeypatch, native)
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()
        original, original_close = events._stage_events, metadata_journal.MetadataJournalScratch.close
        cleanup_observations = []

        def blocked(*arguments):
            entered.set()
            try:
                if not release.wait(5):
                    raise TimeoutError("event staging was not released")
                return original(*arguments)
            finally:
                finished.set()

        def close(work):
            if work.root.name.startswith("events-"):
                cleanup_observations.append(finished.is_set())
            original_close(work)

        monkeypatch.setattr(events, "_stage_events", blocked)
        monkeypatch.setattr(metadata_journal.MetadataJournalScratch, "close", close)
        pending = asyncio.create_task(_allocate(tmp_path, native))
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            for message in ("first admission cancellation", "again", "third"):
                pending.cancel(message)
                for _ in range(3):
                    await asyncio.sleep(0)
            assert not pending.done() and not finished.is_set()
        finally:
            release.set()
            with pytest.raises(asyncio.CancelledError) as captured:
                await pending
        assert captured.value.args == ("first admission cancellation",)
        assert cleanup_observations == [] and _attempts(native)
        assert finished.is_set() and not native.journal.busy
    assert not native.journal.root.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("argument", ("omitted", "none"))
async def test_admission_requires_original_typed_journal_before_work(tmp_path, monkeypatch, argument):
    async with _case(tmp_path, monkeypatch) as native:
        observations, native_ids = _native_boundary(monkeypatch, native)
        arguments_by_name = {"limits": native.limits}
        error = TypeError
        if argument == "none":
            arguments_by_name["journal"] = None
            error = ValueError
        with pytest.raises(error, match="journal"):
            await events.allocate_admission_events(
                None, native.request, native.layout, native.source, native.admission, **arguments_by_name
            )
        assert not observations and not native_ids and not _attempts(native)


def _lose_reply(monkeypatch, native, tag, failure):
    original = checkpoint.append_owner_checkpoint
    injected_records = []

    async def append(*args, **kwargs):
        result = await original(*args, **kwargs)
        record = kwargs["record"]
        if record.kind == "admission_allocation" and not injected_records:
            run_tag = journal_api.msgpack.unpackb(record.payload, raw=False, use_list=False)[1]
            if run_tag == tag:
                injected_records.append(record)
                if failure == "read":
                    native.native.fail_read = True
                else:
                    native.native.failure = failure
        return result

    monkeypatch.setattr(checkpoint, "append_owner_checkpoint", append)
    return injected_records


@pytest.mark.asyncio
@pytest.mark.parametrize("tag", ("start", "batch", "complete"))
@pytest.mark.parametrize("failure", ("after", "unknown", "read"))
async def test_committed_reply_loss_replays_original_timestamp_ids_and_copy_bytes(tmp_path, monkeypatch, tag, failure):
    async with _case(tmp_path, monkeypatch) as native:
        _observations, native_ids = _native_boundary(monkeypatch, native)
        injected_records = _lose_reply(monkeypatch, native, tag, failure)
        if failure == "after":
            descriptor = await _allocate(tmp_path, native)
        else:
            with pytest.raises(FamilyBlockUnavailable if failure == "unknown" else ConnectionError):
                await _allocate(tmp_path, native)
            assert injected_records and not native.journal.busy
            retained = _attempts(native)
            observed_ids = tuple(native_ids)
            completed_paths = tuple(path / events._FILE for path in retained if (path / events._FILE).exists())
            original_bytes = completed_paths[0].read_bytes() if completed_paths else None
            descriptor = await _allocate(tmp_path, native)
            assert all(path.exists() for path in retained)
            if tag != "start":
                assert tuple(native_ids) == observed_ids
            if original_bytes is not None:
                assert (Path(descriptor.root) / events._FILE).read_bytes() == original_bytes
        try:
            assert injected_records and len(native_ids) == native.admission.counts_by_kind["admission_events"]
            assert descriptor.owner_created_at == native.owner_stamps[0].isoformat()
            assert [copy_row["rejection_id"] for copy_row in _copy_rows(descriptor)] == native_ids
            assert all(copy_row["created_at"] == native.owner_stamps[0] for copy_row in _copy_rows(descriptor))
            protected = native.native.committed.copy()
            again = await _allocate(tmp_path, native)
            try:
                assert again.sha256 == descriptor.sha256 and _copy_rows(again) == _copy_rows(descriptor)
                assert native.native.committed == protected
                assert len(native_ids) == descriptor.counts[1]
            finally:
                events.cleanup_admission_events(again)
        finally:
            events.cleanup_admission_events(descriptor)


@pytest.mark.asyncio
async def test_rolled_back_vector_files_cannot_authorize_native_id_reuse(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        _observations, native_ids = _native_boundary(monkeypatch, native)
        injected_records = _lose_reply(monkeypatch, native, "batch", "before")
        with pytest.raises(RuntimeError, match="changed lease before commit"):
            await _allocate(tmp_path, native)
        assert injected_records and native_ids and not native.journal.busy
        uncommitted_ids = tuple(native_ids)
        retained = _attempts(native)
        native.native.failure = None
        descriptor = await _allocate(tmp_path, native)
        try:
            committed_ids = [copy_row["rejection_id"] for copy_row in _copy_rows(descriptor)]
            assert committed_ids == native_ids[len(uncommitted_ids) :]
            assert not set(committed_ids).intersection(uncommitted_ids)
            assert descriptor.owner_created_at == native.owner_stamps[0].isoformat()
            assert all(path.exists() for path in retained)
        finally:
            events.cleanup_admission_events(descriptor)


async def _protected_events(native):
    return await checkpoint.read_owner_checkpoint(
        None,
        native.request,
        native.journal.build_id,
        native.source.snapshot_family_id,
        native.layout,
        root=native.journal.checkpoints,
        limits=native.journal.control_limits,
    )


def _changed_event_chain(checkpoint_records, damage):
    altered_records = list(checkpoint_records)
    head, event_record = altered_records[1]
    match damage:
        case "duplicate":
            altered_records.insert(0, altered_records[0])
        case "gap":
            del altered_records[1]
        case "missing_start":
            del altered_records[2]
        case "misplaced":
            altered_records[2], altered_records[3] = altered_records[3], altered_records[2]
        case "retained":
            altered_records.insert(0, (head, replace(event_record, kind="retained_allocation")))
        case "scope":
            altered_records[1] = (
                head,
                replace(event_record, commitments=(("admission_events", b"z" * 32), *event_record.commitments[1:])),
            )
        case _:
            cells = list(journal_api.msgpack.unpackb(event_record.payload, raw=False, use_list=False))
            match damage:
                case "stamp":
                    cells[2] = (_STAMP + timedelta(days=1)).isoformat()
                case "tag":
                    cells[1] = "complete"
                case "vector":
                    cells[3] = (cells[3][0], cells[3][0])
                case _:
                    raise AssertionError("unknown event chain damage")
            altered_records[1] = head, replace(event_record, payload=journal_api._pack(cells))
    return altered_records


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", ("duplicate", "gap", "missing_start", "misplaced", "retained", "scope", "stamp", "tag", "vector")
)
async def test_admission_prefix_requires_closed_order(tmp_path, monkeypatch, damage):
    """Alter actual producer records for parser tests; no modified head is native proof."""
    async with _case(tmp_path, monkeypatch) as native:
        _observations, native_ids = _native_boundary(monkeypatch, native)
        descriptor = await _allocate(tmp_path, native)
        try:
            snapshot = await _protected_events(native)
            checkpoint_records = tuple(snapshot.records())
            prefix = journal_api.AdmissionSourcePrefix(snapshot, native.limits.maximum_batch_records)
            assert tuple(prefix.records()) == checkpoint_records[3:]
            assert prefix.head == snapshot.head and native_ids
            journal_api.require_completed_admission(snapshot, native.limits.maximum_batch_records, None)
            altered = SimpleNamespace(
                limits=snapshot.limits,
                records=lambda **_keywords: iter(_changed_event_chain(checkpoint_records, damage)),
            )
            with pytest.raises(FamilyBlockUnavailable):
                tuple(journal_api.AdmissionSourcePrefix(altered, native.limits.maximum_batch_records).records())
        finally:
            events.cleanup_admission_events(descriptor)


@pytest.mark.asyncio
async def test_typed_prefix_preserves_exact_batch_proof(tmp_path, monkeypatch):
    """Valid envelope classification cannot substitute for the original event batch hash."""
    async with _case(tmp_path, monkeypatch) as native:
        observations, native_ids = _native_boundary(monkeypatch, native)
        descriptor = await _allocate(tmp_path, native)
        try:
            snapshot = await _protected_events(native)
            checkpoint_records = list(snapshot.records())
            head, event_record = checkpoint_records[1]
            checkpoint_records[1] = head, replace(event_record, input_sha256=b"z" * 32)
            altered = SimpleNamespace(
                limits=snapshot.limits, head=snapshot.head, records=lambda **_keywords: iter(checkpoint_records)
            )
            assert tuple(journal_api.AdmissionSourcePrefix(altered, native.limits.maximum_batch_records).records())

            async def changed_snapshot(*_arguments, **_keywords):
                return altered

            calls_before = tuple(observations), tuple(native_ids)
            monkeypatch.setattr(checkpoint, "read_owner_checkpoint", changed_snapshot)
            with pytest.raises(FamilyBlockUnavailable, match="original event batch"):
                await _allocate(tmp_path, native)
            assert (tuple(observations), tuple(native_ids)) == calls_before
        finally:
            events.cleanup_admission_events(descriptor)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", ("missing_query", "query_bytes", "source_vector_bytes", "family_vector_bytes", "source_after_metadata")
)
async def test_complete_source_and_metadata_prefix_is_required_before_admission_ids(tmp_path, monkeypatch, damage):
    async with _case(tmp_path, monkeypatch) as native:
        _observations, native_ids = _native_boundary(monkeypatch, native)
        snapshot = await checkpoint.read_owner_checkpoint(
            None,
            native.request,
            native.journal.build_id,
            native.source.snapshot_family_id,
            native.layout,
            root=native.journal.checkpoints,
            limits=native.journal.control_limits,
        )
        checkpoint_records = tuple(snapshot.records())
        if damage == "missing_query":
            head = checkpoint_records[1][0]
            native.native.committed.update(owner_checkpoint_revision=head.revision, owner_checkpoint_sha256=head.sha256)
        elif damage.endswith("bytes"):
            kind = {
                "query_bytes": "artifact",
                "source_vector_bytes": "source_allocation",
                "family_vector_bytes": "family_allocation",
            }[damage]
            checkpoint_record = next(
                checkpoint_record for _head, checkpoint_record in checkpoint_records if checkpoint_record.kind == kind
            )
            name = metadata_journal._decode(checkpoint_record.payload, native.journal.control_limits.max_record_bytes)[
                4
            ]
            path = native.journal.root / "vectors" / name
            path.chmod(0o600)
            path.write_bytes(path.read_bytes() + b"\x00")
            path.chmod(0o400)
        else:
            source_record = next(
                checkpoint_record
                for _head, checkpoint_record in checkpoint_records
                if checkpoint_record.kind == "source_allocation"
            )
            async with journal_api._page_session(None, native.request, native.journal.build_id) as (session, _build):
                await checkpoint.append_owner_checkpoint(
                    session,
                    native.request,
                    native.journal.build_id,
                    native.source.snapshot_family_id,
                    native.layout,
                    storage=snapshot,
                    record=source_record,
                    expected_head=snapshot.head,
                )
        with pytest.raises(FamilyBlockUnavailable):
            await _allocate(tmp_path, native)
        assert not native_ids and not native.owner_stamps and not native.journal.busy


@pytest.mark.asyncio
async def test_rejections_are_allocated_in_bounded_vectors_without_per_record_transactions(tmp_path, monkeypatch):
    rows_by_stream = {
        "root_source": [[_root()]],
        "detail_source": [[_child(amount=str(index)) for index in range(100)]],
    }
    async with _case(tmp_path, monkeypatch, rows_by_stream) as native:
        observations, native_ids = _native_boundary(monkeypatch, native)
        descriptor = await _allocate(tmp_path, native)
        try:
            batches = [count for kind, *values in observations if kind == "allocate" for count in values]
            assert len(native_ids) == descriptor.counts[1] == 100
            assert batches == [32, 32, 32, 4]
            assert sum(item[0] == "writing" for item in observations) == len(batches) + 2
            assert [row["rejection_id"] for row in _copy_rows(descriptor)] == native_ids
        finally:
            events.cleanup_admission_events(descriptor)


@pytest.mark.asyncio
async def test_retained_failed_files_share_the_aggregate_budget_and_require_terminal_cleanup(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        _native_boundary(monkeypatch, native, wrong_capture=True)
        with pytest.raises(FamilyBlockUnavailable, match="capture binding"):
            await _allocate(tmp_path, native)
        retained = _attempts(native)
        files_by_path = {path: path.read_bytes() for root in retained for path in root.iterdir()}
        assert files_by_path and not native.journal.busy
        original = native.journal.reconcile_budget

        def exhausted():
            original()
            native.journal.budget().maximum = native.journal.budget().used

        monkeypatch.setattr(native.journal, "reconcile_budget", exhausted)
        _observations, native_ids = _native_boundary(monkeypatch, native)
        with pytest.raises(SourceFactBudgetExceeded, match="aggregate spool budget"):
            await _allocate(tmp_path, native)
        assert not native_ids and not native.journal.busy and _attempts(native) == retained
        assert all(path.read_bytes() == data for path, data in files_by_path.items())
    assert not native.journal.root.exists()
