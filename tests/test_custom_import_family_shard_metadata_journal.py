# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Actual SOURCE/journal files; only native transaction and authority are doubled."""

import asyncio
import hashlib
import shutil
import threading
from contextlib import asynccontextmanager, closing
from dataclasses import replace
from pathlib import Path

import msgpack
import pytest

from process.custom_import import family_shard_allocate as allocation
from process.custom_import import family_shard_checkpoint as checkpoint
from process.custom_import import family_shard_metadata as metadata
from process.custom_import import family_shard_metadata_journal as journal_api
from process.custom_import.family import _is_valid_npi
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_shard_identity import GlobalIdentity
from process.custom_import.family_shard_input import cleanup_spool
from process.custom_import.family_shard_plan import PlanWork
from process.custom_import.family_shard_source_plan import cleanup_source_plan
from process.custom_import.runner_codec import digest_text
from tests.test_custom_import_family_shard_allocate import _allocate, _journal_for_plan, _native_boundary
from tests.test_custom_import_family_shard_metadata import _layout
from tests.test_custom_import_family_shard_source_plan import _build, _case, _child, _root
from tests.test_custom_import_family_shard_worker import _projected_definition

pytestmark = pytest.mark.asyncio


def _npi(index):
    prefix = str(100300000 + index)
    return next(prefix + str(check) for check in range(10) if _is_valid_npi(prefix + str(check)))


@asynccontextmanager
async def _source(tmp_path, monkeypatch, *, count=2, maximum=32, empty=False):
    provider_npis = tuple(_npi(index) for index in range(count))
    metadata_rows_by_key = {
        "root_source": [[] if empty else [_root(observed_value) for observed_value in provider_npis]],
        "detail_source": [[] if empty else [_child(observed_value, "exact") for observed_value in provider_npis]],
    }
    case = await _case(tmp_path, metadata_rows_by_key, definition=_projected_definition())
    plan = _build(tmp_path, case)
    native = _native_boundary(monkeypatch, plan)
    journal = _journal_for_plan(tmp_path, plan, maximum)
    allocated_source = None
    try:
        allocated_source = await _allocate(tmp_path, case[1], plan, maximum=maximum, journal=journal)
        root_by_key = {
            metadata_record.typed_parent_key: roots[index]
            for pack, _pack, roots, _revisions, _rejections in allocation.read_source_allocations(allocated_source)
            for index, metadata_record in enumerate(pack.records)
            if metadata_record.typed_parent_key is not None
        }
        native.family_calls, native.family_next, native.entities = [], 2001, {}
        native.rebound_root = False

        async def intern(_session, _definition, _dataset, keys, **_limits):
            return tuple(
                GlobalIdentity(
                    root_by_key[(key.root_key_text, key.root_key_sha256)] + int(native.rebound_root),
                    native.entities.setdefault(key.entity_value, 4001 + 17 * len(native.entities)),
                )
                for key in keys
            )

        async def allocate(_session, kinds):
            assert kinds and set(kinds) == {"family_revision"}
            native.family_calls.append(tuple(kinds))
            values = tuple(native.family_next + 17 * index for index in range(len(kinds)))
            native.family_next += 17 * len(kinds)
            return values

        monkeypatch.setattr(metadata, "intern_global_identities", intern)
        monkeypatch.setattr(metadata, "_allocate_record_ids", allocate)
        monkeypatch.setattr(journal_api, "_page_session", allocation._page_session)
        monkeypatch.setattr(journal_api, "_execute", allocation._execute)
        limits = allocation.SourceAllocationLimits(journal.work.limits, maximum, 1024**2)
        yield case, allocated_source, journal, native, limits
    finally:
        if allocated_source is not None:
            allocation.cleanup_source_allocations(allocated_source)
        journal.close_terminal()
        cleanup_source_plan(plan)
        cleanup_spool(case[0])


def _metadata(case, source, journal, limits, *, retained=()):
    return metadata.owner_source_metadata(
        None,
        case[1],
        _layout(case[1], source),
        source,
        limits=limits,
        retained_query_keys=retained,
        journal=journal,
    )


def _values(value):
    return tuple(value.family_ids()), tuple(value.query_keys())


async def _snapshot(case, source, journal):
    return await checkpoint.read_owner_checkpoint(
        None,
        case[1],
        journal.build_id,
        source.snapshot_family_id,
        None,
        root=journal.checkpoints,
        limits=journal.control_limits,
    )


@pytest.mark.parametrize("argument", ("omitted", "none"))
async def test_metadata_requires_the_original_source_journal_before_work(tmp_path, monkeypatch, argument):
    async with _source(tmp_path, monkeypatch) as (case, source, journal, native, limits):
        original_outputs = tuple((journal.root / "outputs").iterdir())
        arguments_by_name = {"limits": limits}
        error, message = TypeError, "journal"
        if argument == "none":
            arguments_by_name["journal"] = None
            error, message = ValueError, "existing typed SOURCE journal"
        with pytest.raises(error, match=message):
            async with metadata.owner_source_metadata(None, case[1], None, source, **arguments_by_name):
                pytest.fail("unjournalled metadata escaped")
        assert not native.family_calls and not journal.busy
        assert tuple((journal.root / "outputs").iterdir()) == original_outputs


async def test_exact_family_and_query_mapping_survives_original_cleanup_and_native_reopen(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch, count=4, maximum=2) as (case, allocated_source, journal, native, limits):
        retained = ((digest_text("child-key", "exact retained lexical key"), "exact retained lexical key"),)
        async with _metadata(case, allocated_source, journal, limits, retained=iter(retained)) as original:
            expected = _values(original)
            original_root = original.work.root
            assert [ids[0] for _key, ids in expected[0]] == [2001, 2018, 2035, 2052]
            assert len(native.family_calls) == 2
            assert len(tuple(original_root.iterdir())) == 2
            sealed = metadata.seal_owner_metadata(
                original,
                case[1],
                allocated_source,
                root=tmp_path / "sealed",
                register_cleanup=lambda _path: None,
                work=PlanWork(tmp_path / "seal-scratch", limits.spool, lambda _path: None),
            )
        assert not original_root.exists()
        protected = await _snapshot(case, allocated_source, journal)
        metadata_records = list(protected.records())
        assert metadata_records[0][1].kind == "artifact"
        assert [
            metadata_record.ordinal
            for _head, metadata_record in metadata_records
            if metadata_record.kind == "family_allocation"
        ] == [1, 0]
        assert (
            len(
                [
                    metadata_record
                    for _head, metadata_record in metadata_records
                    if metadata_record.kind == "source_allocation"
                ]
            )
            == 4
        )

        async def no_allocate(*_args, **_kwargs):
            pytest.fail("metadata recovery cannot allocate replacement native IDs")

        def no_retained_replay():
            pytest.fail("protected complete query mapping must be reused")
            yield

        monkeypatch.setattr(metadata, "_allocate_record_ids", no_allocate)
        monkeypatch.setattr(metadata, "intern_global_identities", no_allocate)
        async with _metadata(case, allocated_source, journal, limits, retained=no_retained_replay()) as reopened:
            assert _values(reopened) == expected
            assert len(tuple(reopened.work.root.iterdir())) == 2
            assert [entry[2:] for entry in expected[1]] == [(index, 1) for index in range(1, len(expected[1]) + 1)]
        with metadata.reopen_owner_metadata(
            sealed.root,
            expected_scope=sealed.scope,
            descriptor_sha256=sealed.descriptor_sha256,
            work=PlanWork(tmp_path / "read-sealed", limits.spool, lambda _path: None),
        ) as copy:
            assert _values(copy) == expected
        shutil.rmtree(sealed.root)
        assert (await _snapshot(case, allocated_source, journal)).head == protected.head


@pytest.mark.parametrize("failure", ("after", "unknown", "before"))
async def test_family_commit_response_reconciliation_preserves_exact_vectors(tmp_path, monkeypatch, failure):
    async with _source(tmp_path, monkeypatch) as (case, source, journal, native, limits):
        native.failure = failure
        if failure == "after":
            async with _metadata(case, source, journal, limits) as value:
                assert [ids[0] for _key, ids in value.family_ids()] == [2001, 2018]
            assert len(native.family_calls) == 1
        else:
            error = FamilyBlockUnavailable if failure == "unknown" else RuntimeError
            with pytest.raises(error):
                async with _metadata(case, source, journal, limits):
                    pytest.fail("unconfirmed metadata escaped")
            prior_attempts = tuple((journal.root / "outputs").iterdir())
            assert not journal.busy and len(native.family_calls) == 1
            native.failure = None
            async with _metadata(case, source, journal, limits) as value:
                expected = [2001, 2018] if failure == "unknown" else [2035, 2052]
                assert [ids[0] for _key, ids in value.family_ids()] == expected
                assert all(path.exists() for path in prior_attempts)
            assert len(native.family_calls) == (1 if failure == "unknown" else 2)


@pytest.mark.parametrize("failure", ("after", "unknown", "before"))
async def test_query_checkpoint_response_loss_never_reallocates_family_ids(tmp_path, monkeypatch, failure):
    async with _source(tmp_path, monkeypatch) as (case, source, journal, native, limits):
        copy = journal_api._copy_query

        def injected(*args):
            result = copy(*args)
            native.failure = failure
            return result

        monkeypatch.setattr(journal_api, "_copy_query", injected)
        if failure == "after":
            async with _metadata(case, source, journal, limits) as value:
                assert len(tuple(value.work.root.iterdir())) == 2
                assert len(tuple(value.query_keys())) == 1
        else:
            with pytest.raises(FamilyBlockUnavailable if failure == "unknown" else RuntimeError):
                async with _metadata(case, source, journal, limits):
                    pytest.fail("unconfirmed dictionary escaped")
            native.failure = None
            monkeypatch.setattr(journal_api, "_copy_query", copy)
            async with _metadata(case, source, journal, limits) as value:
                assert len(tuple(value.work.root.iterdir())) == 2
                assert [ids[0] for _key, ids in value.family_ids()] == [2001, 2018]
        assert len(native.family_calls) == 1


@pytest.mark.parametrize("problem", ("duplicate", "gap", "source_after_family", "other_phase"))
async def test_full_chain_rejects_mixed_or_malformed_phase_order_before_new_ids(tmp_path, monkeypatch, problem):
    async with _source(tmp_path, monkeypatch) as (case, allocated_source, journal, native, limits):
        native.failure = "unknown"
        with pytest.raises(FamilyBlockUnavailable):
            async with _metadata(case, allocated_source, journal, limits):
                pytest.fail("unconfirmed metadata escaped")
        snapshot = await _snapshot(case, allocated_source, journal)
        metadata_records = list(snapshot.records())
        family = metadata_records[0][1]
        assert family.kind == "family_allocation"
        if problem == "duplicate":
            extra = family
        elif problem == "gap":
            extra = replace(family, ordinal=2)
        elif problem == "source_after_family":
            extra = metadata_records[-1][1]
        else:
            extra = checkpoint.OwnerCheckpointRecord("admission_allocation", 0, b"x" * 32)
        async with allocation._page_session(None, case[1], journal.build_id) as (session, _build):
            await checkpoint.append_owner_checkpoint(
                session,
                case[1],
                journal.build_id,
                allocated_source.snapshot_family_id,
                None,
                storage=snapshot,
                record=extra,
                expected_head=snapshot.head,
            )
        calls = len(native.family_calls)
        with pytest.raises(FamilyBlockUnavailable):
            async with _metadata(case, allocated_source, journal, limits):
                pytest.fail("invalid phase order escaped")
        assert len(native.family_calls) == calls


@pytest.mark.parametrize("role", ("family", "query"))
async def test_tampered_protected_metadata_refuses_without_new_ids(tmp_path, monkeypatch, role):
    async with _source(tmp_path, monkeypatch) as (case, source, journal, native, limits):
        async with _metadata(case, source, journal, limits) as completed_metadata:
            assert completed_metadata.work.root.exists()
        snapshot = await _snapshot(case, source, journal)
        record = next(
            record
            for _head, record in snapshot.records()
            if record.kind == ("family_allocation" if role == "family" else "artifact")
        )
        cells = msgpack.unpackb(record.payload, raw=False, use_list=False)
        path = journal.root / "vectors" / cells[4]
        path.chmod(0o600)
        path.write_bytes(path.read_bytes() + b"x")
        path.chmod(0o400)
        calls = len(native.family_calls)
        with pytest.raises(FamilyBlockUnavailable):
            async with _metadata(case, source, journal, limits):
                pytest.fail("tampered metadata escaped")
        assert len(native.family_calls) == calls and path.exists()


async def test_empty_source_keeps_empty_family_commit_and_typed_dictionary_eof(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch, empty=True) as (case, source, journal, native, limits):
        async with _metadata(case, source, journal, limits) as value:
            assert _values(value) == ((), ())
        records = list((await _snapshot(case, source, journal)).records())
        assert [record.kind for _head, record in records] == ["artifact", "family_allocation", "source_allocation"]
        assert not native.family_calls
        async with _metadata(case, source, journal, limits) as value:
            assert _values(value) == ((), ())


async def test_wrong_native_root_binding_cannot_commit_family_vector(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (case, source, journal, native, limits):
        original_head = (await _snapshot(case, source, journal)).head
        native.rebound_root = True
        with pytest.raises(FamilyBlockUnavailable, match="root dictionary"):
            async with _metadata(case, source, journal, limits):
                pytest.fail("wrong root binding escaped")
        assert not native.family_calls and (await _snapshot(case, source, journal)).head == original_head


async def test_cancel_after_family_append_retains_provisional_files_and_original_source(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (case, source, journal, native, limits):
        append = checkpoint.append_owner_checkpoint

        async def cancelled(*args, **kwargs):
            prepared = await append(*args, **kwargs)
            if kwargs["record"].kind == "family_allocation":
                raise asyncio.CancelledError
            return prepared

        monkeypatch.setattr(checkpoint, "append_owner_checkpoint", cancelled)
        with pytest.raises(asyncio.CancelledError):
            async with _metadata(case, source, journal, limits):
                pytest.fail("cancelled metadata escaped")
        assert not journal.busy and Path(source.root).exists()
        held_items = tuple((journal.root / "vectors").iterdir())
        assert any("metadata-family" in path.name for path in held_items)
        monkeypatch.setattr(checkpoint, "append_owner_checkpoint", append)
        async with _metadata(case, source, journal, limits) as value:
            assert [ids[0] for _key, ids in value.family_ids()] == [2035, 2052]
        assert all(path.exists() for path in held_items)


async def test_metadata_aggregate_budget_counts_retained_attempts_and_query_copies(tmp_path, monkeypatch):
    from process.custom_import.family_shard_facts import SourceFactBudgetExceeded

    async with _source(tmp_path, monkeypatch) as (case, source, journal, native, limits):
        async with _metadata(case, source, journal, limits) as completed_metadata:
            assert completed_metadata.work.root.exists()
        used = sum(path.stat().st_size for path in journal.root.rglob("*") if path.is_file())
        padding = journal.root / "vectors" / "provisional-held.mpk"
        padding.write_bytes(b"x" * (limits.spool.max_spool_bytes - used))
        padding.chmod(0o400)
        before_by_key = {
            str(path.relative_to(journal.root)): hashlib.sha256(path.read_bytes()).digest()
            for path in journal.root.rglob("*")
            if path.is_file()
        }
        paths = {str(path.relative_to(journal.root)) for path in journal.root.rglob("*")}
        calls = len(native.family_calls)
        with pytest.raises(SourceFactBudgetExceeded, match="aggregate"):
            async with _metadata(case, source, journal, limits):
                pytest.fail("exhausted metadata escaped")
        assert len(native.family_calls) == calls
        assert {str(path.relative_to(journal.root)) for path in journal.root.rglob("*")} == paths
        assert all(
            hashlib.sha256((journal.root / name).read_bytes()).digest() == digest
            for name, digest in before_by_key.items()
        )


async def test_completed_source_file_without_protected_source_prefix_grants_no_metadata_ids(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (case, source, journal, native, limits):
        native.committed.update(owner_checkpoint_revision=None, owner_checkpoint_sha256=None)
        with pytest.raises(FamilyBlockUnavailable, match="complete original SOURCE prefix"):
            async with _metadata(case, source, journal, limits):
                pytest.fail("a file or count was mistaken for committed SOURCE authority")
        assert not native.family_calls and Path(source.root).exists()


async def test_query_checkpoint_must_be_the_unique_last_metadata_record(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (case, source, journal, native, limits):
        async with _metadata(case, source, journal, limits) as completed_metadata:
            assert completed_metadata.work.root.exists()
        snapshot = await _snapshot(case, source, journal)
        record = next(snapshot.records())[1]
        assert record.kind == "artifact"
        async with allocation._page_session(None, case[1], journal.build_id) as (session, _build):
            await checkpoint.append_owner_checkpoint(
                session,
                case[1],
                journal.build_id,
                source.snapshot_family_id,
                None,
                storage=snapshot,
                record=record,
                expected_head=snapshot.head,
            )
        with pytest.raises(FamilyBlockUnavailable, match="duplicate or misplaced"):
            async with _metadata(case, source, journal, limits):
                pytest.fail("duplicated query checkpoint escaped")
        assert len(native.family_calls) == 1


@pytest.mark.parametrize("problem", ("head", "phase", "capture"))
async def test_fresh_native_preflight_refuses_before_metadata_ids(tmp_path, monkeypatch, problem):
    async with _source(tmp_path, monkeypatch) as (case, source, journal, native, limits):
        if problem == "head":
            original = checkpoint._native_scope

            async def changed(*args, **kwargs):
                descriptor, scope = await original(*args, **kwargs)
                if kwargs["writing"]:
                    descriptor.owner_checkpoint_revision += 1
                    descriptor.owner_checkpoint_sha256 = b"x" * 32
                return descriptor, scope

            monkeypatch.setattr(checkpoint, "_native_scope", changed)
        elif problem == "phase":
            native.phase = "admission"
        else:
            fields = list(native.capture_scope)
            fields[3] = b"x" * 32
            native.capture_scope = tuple(fields)
        with pytest.raises(FamilyBlockUnavailable):
            async with _metadata(case, source, journal, limits):
                pytest.fail("stale native authority escaped")
        assert not native.family_calls


async def test_cancelled_query_drains_and_keeps_committed_family_and_partial_attempt(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (case, allocated_source, journal, native, limits):
        original, scratches = journal_api.MetadataJournalScratch, []
        entered, released, closed = threading.Event(), threading.Event(), threading.Event()

        def scratch(*args):
            work = original(*args)
            scratches.append(work)
            return work

        monkeypatch.setattr(journal_api, "MetadataJournalScratch", scratch)
        retained_key = digest_text("child-key", "retained exact"), "retained exact"

        def retained():
            try:
                yield retained_key
                entered.set()
                assert scratches[0].stop.wait(5)
                assert released.wait(5)
                scratches[0].current()
            finally:
                closed.set()

        async def prepare():
            async with _metadata(case, allocated_source, journal, limits, retained=retained()):
                pytest.fail("cancelled dictionary escaped")

        pending = asyncio.create_task(prepare())
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            pending.cancel("metadata dictionary stop")
            assert await asyncio.to_thread(scratches[0].stop.wait, 5)
            assert not pending.done() and scratches[0].root.exists() and journal.busy
        finally:
            released.set()
        with pytest.raises(asyncio.CancelledError, match="metadata dictionary stop"):
            await pending
        assert closed.is_set() and not journal.busy and scratches[0].root.exists()
        assert len(native.family_calls) == 1
        monkeypatch.setattr(journal_api, "MetadataJournalScratch", original)
        async with _metadata(case, allocated_source, journal, limits, retained=(retained_key,)) as observed_value:
            assert len(tuple(observed_value.query_keys())) == 2
            assert [ids[0] for _key, ids in observed_value.family_ids()] == [2001, 2018]
        assert len(native.family_calls) == 1


async def test_source_prefix_checks_batch_budget_before_retaining_an_oversized_cohort(tmp_path, monkeypatch):
    from process.custom_import.family_block_codec import FamilyBlockResourceLimit

    async with _source(tmp_path, monkeypatch, count=40, maximum=64) as (case, source, journal, native, limits):
        original, observed = journal_api.read_source_allocations, []

        def measured(*args, **kwargs):
            with closing(original(*args, **kwargs)) as rows:
                for row in rows:
                    observed.append(len(row[0].records))
                    yield row

        monkeypatch.setattr(journal_api, "read_source_allocations", measured)
        smaller = replace(limits, maximum_batch_records=32)
        with pytest.raises(FamilyBlockResourceLimit, match="original batch budget"):
            async with _metadata(case, source, journal, smaller):
                pytest.fail("oversized SOURCE replay escaped")
        assert len(observed) == 17 and sum(observed) == 34
        assert not native.family_calls


async def test_deadline_admission_failure_cannot_leave_the_shared_journal_busy(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (case, source, journal, native, limits):
        roots = tuple((journal.root / "outputs").iterdir())

        def stopped(_request):
            raise TimeoutError("synthetic original deadline admission failed")

        monkeypatch.setattr(journal_api, "_read_deadline", stopped)
        with pytest.raises(TimeoutError, match="deadline admission"):
            async with _metadata(case, source, journal, limits):
                pytest.fail("deadline failure escaped")
        assert not journal.busy and not native.family_calls
        assert tuple((journal.root / "outputs").iterdir()) == roots
