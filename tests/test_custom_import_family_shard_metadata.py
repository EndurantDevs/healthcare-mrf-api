# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real SOURCE coordinates feed bounded owner dictionaries without local ranks."""

import asyncio
import hashlib
import json
import os
import shutil
import threading
import time
from contextlib import asynccontextmanager, closing
from dataclasses import replace
from functools import partial
from pathlib import Path
from types import SimpleNamespace

import pytest

from process.custom_import import family_shard_allocate as allocation
from process.custom_import import family_shard_metadata as metadata
from process.custom_import import family_shard_metadata_journal as journal_api
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_validate import FamilyBlockValidationError
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded, SourceFactLimits
from process.custom_import.family_shard_identity import GlobalIdentity
from process.custom_import.family_shard_input import cleanup_spool
from process.custom_import.family_shard_plan import PlanWork, _cpu, _Scratch
from process.custom_import.family_shard_source_plan import cleanup_source_plan
from process.custom_import.runner_codec import digest_text
from process.custom_import.runner_types import CancellationRequested
from tests.test_custom_import_family_shard_allocate import _allocate, _journal_for_plan, _native_boundary
from tests.test_custom_import_family_shard_source_plan import _build, _case, _child, _root
from tests.test_custom_import_snowflake_bundle import _definition

pytestmark = pytest.mark.asyncio


@asynccontextmanager
async def _source(tmp_path, monkeypatch, *, empty=False, query_child=False):
    rows = (
        {"root_source": [[]], "detail_source": [[]]}
        if empty
        else {
            "root_source": [[_root(), _root("1234567893"), _root()]],
            "detail_source": [[_child(), _child(), _child("orphan-parent", key="z")]],
        }
    )
    document = json.loads(_definition().canonical)
    if query_child:
        child_fields = document["schema"]["children"][0]["fields"]
        child_fields[1]["projection_slot"], child_fields[2]["projection_slot"] = 4, 5
        document["query"]["child"] = {"collection": "details", "fields": ["detail_id", "amount"]}
    case = await _case(tmp_path, rows, definition=CustomImportDefinition.from_mapping(document))
    plan = _build(tmp_path, case)
    _native_boundary(monkeypatch, plan)
    allocated = None
    journal = _journal_for_plan(tmp_path, plan)
    try:
        allocated = await _allocate(tmp_path, case[1], plan, journal=journal)
        yield case[1], allocated, journal
    finally:
        if allocated is not None:
            allocation.cleanup_source_allocations(allocated)
        journal.close_terminal()
        cleanup_source_plan(plan)
        cleanup_spool(case[0])


def _owner(monkeypatch, allocated_source, journal, *, lost_commit=False, rebound_root=False):
    root_by_key, events = {}, []
    for pack, _pack_id, roots, _revisions, _rejections in allocation.read_source_allocations(allocated_source):
        for source_record, root in zip(pack.records, roots, strict=True):
            if source_record.typed_parent_key is not None:
                root_by_key[source_record.typed_parent_key] = root
    native_page = allocation._page_session

    @asynccontextmanager
    async def page(*args):
        assert args[2] == journal.build_id
        events.append("authority")
        async with native_page(*args) as (session, build):
            events.append("candidate")
            yield session, build
            if lost_commit:
                raise RuntimeError("synthetic lost owner lease")
        events.append("commit")

    async def intern(session, definition, dataset, keys, **kwargs):
        events.append(tuple(keys))
        return tuple(
            GlobalIdentity(root_by_key[(key.root_key_text, key.root_key_sha256)] + int(rebound_root), 91 + 17 * index)
            for index, key in enumerate(keys)
        )

    async def reserve(session, kinds):
        assert set(kinds) == {"family_revision"}
        return tuple(1001 + 17 * index for index in range(len(kinds)))

    monkeypatch.setattr(journal_api, "_page_session", page)
    monkeypatch.setattr(journal_api, "_execute", allocation._execute)
    monkeypatch.setattr(metadata, "intern_global_identities", intern)
    monkeypatch.setattr(metadata, "_allocate_record_ids", reserve)
    return events


def _layout(request, source):
    from process.custom_import.family_block_layout import FamilyBlockLayout

    return FamilyBlockLayout(request.definition, dict(source.source.collection_slots))


def _work(tmp_path, request, source, journal, *, retained=()):
    return metadata.owner_source_metadata(
        None,
        request,
        _layout(request, source),
        source,
        journal=journal,
        limits=allocation.SourceAllocationLimits(journal.work.limits, 32, 1024**2),
        retained_query_keys=retained,
    )


def _attempts(journal):
    return tuple(path for path in (journal.root / "outputs").iterdir() if path.name.startswith("metadata-"))


async def test_families_use_observed_ids_and_keys_use_one_complete_global_order(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch, query_child=True) as (request, source, journal):
        events = _owner(monkeypatch, source, journal)
        retained = (digest_text("child-key", "exact retained lexical key"), "exact retained lexical key")
        async with _work(tmp_path, request, source, journal, retained=iter((retained,))) as prepared:
            family_ids = list(prepared.family_ids())
            assert len(family_ids) == 2 and [value[1][0] for value in family_ids] == [1001, 1018]
            assert list(prepared.family_ids()) == family_ids
            native_keys = {
                (landing.child_key_sha256, landing.canonical_child_key)
                for pack, *_ in allocation.read_source_allocations(source)
                if pack.collection_slot
                for landing in pack.landing.records
                if landing.rejection_code is None
            }
            expected_keys = sorted(native_keys | {retained})
            query_keys = list(prepared.query_keys())
            assert [row[:2] for row in query_keys] == expected_keys
            assert [row[2:] for row in query_keys] == [(index, 1) for index in range(1, len(expected_keys) + 1)]
            original_root = prepared.work.root
        assert events[0:2] == ["authority", "candidate"] and events[-1] == "commit"
        assert not original_root.exists() and not _attempts(journal)


async def test_undeclared_children_do_not_enter_query_dictionary(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (request, source, journal):
        _owner(monkeypatch, source, journal)
        async with _work(tmp_path, request, source, journal) as prepared:
            assert list(prepared.query_keys()) == []


async def test_empty_source_still_binds_a_current_owner_window(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch, empty=True) as (request, source, journal):
        events = _owner(monkeypatch, source, journal)
        async with _work(tmp_path, request, source, journal) as prepared:
            assert list(prepared.family_ids()) == list(prepared.query_keys()) == []
        assert events == ["authority", "candidate", "commit"] * 2


@pytest.mark.parametrize("problem", ["lease", "root"])
async def test_owner_failure_refuses_metadata_and_preserves_source(tmp_path, monkeypatch, problem):
    async with _source(tmp_path, monkeypatch) as (request, source, journal):
        _owner(monkeypatch, source, journal, lost_commit=problem == "lease", rebound_root=problem == "root")
        with pytest.raises((RuntimeError, FamilyBlockUnavailable)):
            async with _work(tmp_path, request, source, journal):
                raise AssertionError("owner failure exposed completed metadata")
        assert len(_attempts(journal)) == 1
        assert sum(len(row[0].records) for row in allocation.read_source_allocations(source)) == 6


async def test_incomplete_retained_keys_close_and_refuse_the_global_dictionary(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (request, source, journal):
        _owner(monkeypatch, source, journal)
        close_events = []

        def retained():
            try:
                yield digest_text("child-key", "retained"), "retained"
                raise RuntimeError("synthetic retained-key EOF loss")
            finally:
                close_events.append("closed")

        with pytest.raises(RuntimeError, match="EOF loss"):
            async with _work(tmp_path, request, source, journal, retained=retained()):
                raise AssertionError("incomplete keys were accepted")
        assert close_events == ["closed"] and len(_attempts(journal)) == 1


@pytest.mark.parametrize("phase", ["_source_family_keys", "_write_assignments", "_write_query_keys"])
async def test_bulk_metadata_stays_off_the_owner_loop(tmp_path, monkeypatch, phase):
    async with _source(tmp_path, monkeypatch, query_child=True) as (request, source, journal):
        _owner(monkeypatch, source, journal)
        original, entered, release, threads = getattr(metadata, phase), threading.Event(), threading.Event(), []

        def blocked(*args):
            threads.append(threading.get_ident())
            entered.set()
            assert release.wait(5), "metadata worker was not released"
            return original(*args)

        async def prepare():
            async with _work(tmp_path, request, source, journal) as prepared:
                return list(prepared.family_ids()), list(prepared.query_keys())

        monkeypatch.setattr(metadata, phase, blocked)
        pending = asyncio.create_task(prepare())
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            await asyncio.sleep(0)
            assert not pending.done() and threads[0] != threading.get_ident()
        finally:
            release.set()
            families, keys = await pending
        assert len(families) == 2 and [entry[2:] for entry in keys] == [(1, 1), (2, 1)]
        assert not _attempts(journal)


async def test_cancelled_dictionary_drains_before_removal_and_preserves_originals(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch, query_child=True) as (request, allocated_source, journal):
        _owner(monkeypatch, allocated_source, journal)
        original, scratches = journal_api.MetadataJournalScratch, []
        entered = threading.Event()
        stopped = threading.Event()
        release = threading.Event()
        closed = threading.Event()

        def scratch(*args):
            work = original(*args)
            scratches.append(work)
            return work

        def retained():
            try:
                yield digest_text("child-key", "retained"), "retained"
                entered.set()
                assert scratches[0].stop.wait(5)
                stopped.set()
                assert release.wait(5)
                scratches[0].current()
            finally:
                closed.set()

        async def prepare():
            async with _work(tmp_path, request, allocated_source, journal, retained=retained()):
                raise AssertionError("cancelled metadata was exposed")

        monkeypatch.setattr(journal_api, "MetadataJournalScratch", scratch)
        pending = asyncio.create_task(prepare())
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            pending.cancel("first metadata cancellation")
            assert await asyncio.to_thread(stopped.wait, 5)
            assert not pending.done() and scratches[0].root.exists()
            pending.cancel("second metadata cancellation")
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError, match="first metadata cancellation"):
            await pending
        assert closed.is_set() and scratches[0].root.exists()
        assert (
            sum(
                len(allocated_pack[0].records)
                for allocated_pack in allocation.read_source_allocations(allocated_source)
            )
            == 6
        )


async def test_original_deadline_is_enforced_during_global_dictionary_pass(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (request, source, journal):
        _owner(monkeypatch, source, journal)
        request = replace(request, operation_deadline=time.monotonic() + 10)
        deadline = journal_api._read_deadline(request)
        clock, closed = [deadline - 1], []
        monkeypatch.setattr(journal_api, "_read_deadline", lambda observed: deadline)
        monkeypatch.setattr("process.custom_import.family_shard_base.time", SimpleNamespace(monotonic=lambda: clock[0]))

        def retained():
            try:
                yield digest_text("child-key", "retained"), "retained"
                clock[0] = deadline
                yield digest_text("child-key", "late"), "late"
            finally:
                closed.append(True)

        with pytest.raises(TimeoutError, match="existing operation deadline"):
            async with _work(tmp_path, request, source, journal, retained=retained()):
                raise AssertionError("expired metadata was exposed")
        assert closed == [True] and len(_attempts(journal)) == 1


def _restart_work(tmp_path, name, *, check=None, memory=4096):
    return PlanWork(tmp_path / name, SourceFactLimits(16 * 1024**2, 1024**2, memory), lambda _path: None, check)


def _seal(tmp_path, prepared, request, source, registered, *, check=None, memory=4096):
    return metadata.seal_owner_metadata(
        prepared,
        request,
        source,
        root=tmp_path / "retained-metadata",
        register_cleanup=registered.append,
        work=_restart_work(tmp_path, "seal-work", check=check, memory=memory),
    )


def _reopen(tmp_path, sealed, **arguments):
    return metadata.reopen_owner_metadata(
        arguments.pop("root", sealed.root),
        expected_scope=arguments.pop("expected_scope", sealed.scope),
        descriptor_sha256=arguments.pop("descriptor_sha256", sealed.descriptor_sha256),
        work=arguments.pop("work", _restart_work(tmp_path, "reopen-work")),
        **arguments,
    )


async def test_sealed_metadata_reopens_exact_native_ids_after_original_cleanup(tmp_path, monkeypatch):
    registered_roots = []
    async with _source(tmp_path, monkeypatch, query_child=True) as (request, allocated_source, journal):
        _owner(monkeypatch, allocated_source, journal)
        retained = digest_text("child-key", "retained key"), "retained key"
        async with _work(tmp_path, request, allocated_source, journal, retained=(retained,)) as prepared:
            expected = list(prepared.family_ids()), list(prepared.query_keys())
            original_bytes = prepared.family_path.read_bytes(), prepared.query_path.read_bytes()
            sealed = _seal(tmp_path, prepared, request, allocated_source, registered_roots)
            assert list(prepared.family_ids()) == expected[0]
            assert sealed.scope == metadata.owner_metadata_scope(request, allocated_source)
            original_root = prepared.work.root
        assert not original_root.exists() and not _attempts(journal)
        assert registered_roots == [tmp_path / "retained-metadata"]

        async def no_allocate(*_arguments):
            raise AssertionError("reopening cannot allocate replacement identities")

        monkeypatch.setattr(metadata, "_allocate_record_ids", no_allocate)
        for _attempt in range(2):
            with _reopen(tmp_path, sealed) as reopened:
                assert (list(reopened.family_ids()), list(reopened.query_keys())) == expected
                assert (reopened.family_path.read_bytes(), reopened.query_path.read_bytes()) == original_bytes
                assert tuple(count for _role, count in sealed.counts) == tuple(map(len, expected))
                assert reopened.scope == sealed.scope
            assert not (tmp_path / "reopen-work").exists()
        document = json.loads((Path(sealed.root) / metadata._DESCRIPTOR).read_text())
        assert document.keys() == {"contract", "scope", "directory", "files"}
        assert all(
            Path(sealed.root, entry["name"]).stat().st_mode & 0o777 == 0o400 for entry in document["files"].values()
        )
        assert not any(name in json.dumps(document) for name in ("lease_token", "producer_token", "deadline", "nonce"))
        assert [entry[1][0] for entry in expected[0]] == [1001, 1018]
    assert Path(sealed.root).exists() and not (tmp_path / "seal-work").exists()
    shutil.rmtree(sealed.root)


@pytest.mark.parametrize("empty,query_child", [(True, False), (False, False)])
async def test_sealed_empty_or_no_query_metadata_has_exact_eof(tmp_path, monkeypatch, empty, query_child):
    async with _source(tmp_path, monkeypatch, empty=empty, query_child=query_child) as (request, source, journal):
        _owner(monkeypatch, source, journal)
        async with _work(tmp_path, request, source, journal) as prepared:
            sealed = _seal(tmp_path, prepared, request, source, [])
        with _reopen(tmp_path, sealed) as reopened:
            assert len(list(reopened.family_ids())) == (0 if empty else 2)
            assert list(reopened.query_keys()) == []
    shutil.rmtree(sealed.root)


@pytest.mark.parametrize("damage", ("commitment", "scope", "root", "missing_commitment"))
async def test_reopen_requires_protected_commitment_and_exact_actual_scope(tmp_path, monkeypatch, damage):
    async with _source(tmp_path, monkeypatch) as (request, source, journal):
        _owner(monkeypatch, source, journal)
        async with _work(tmp_path, request, source, journal) as prepared:
            sealed = _seal(tmp_path, prepared, request, source, [])
        options_by_name = {}
        if damage == "commitment":
            options_by_name["descriptor_sha256"] = b"x" * 32
        elif damage == "scope":
            options_by_name["expected_scope"] = tuple(
                (name, value + 1 if name == "execution_id" else value) for name, value in sealed.scope
            )
        elif damage == "root":
            options_by_name["root"] = tmp_path / "different-root"
            shutil.copytree(sealed.root, options_by_name["root"])
        else:
            options_by_name["descriptor_sha256"] = None
        with pytest.raises(ValueError):
            with _reopen(tmp_path, sealed, **options_by_name):
                pytest.fail("unbound metadata cannot be exposed")
        assert not (tmp_path / "reopen-work").exists()
    shutil.rmtree(sealed.root)
    if damage == "root":
        shutil.rmtree(options_by_name["root"])


@pytest.mark.parametrize("damage", ("bytes", "truncated", "missing", "symlink", "extra"))
async def test_reopen_refuses_mutated_files_and_incomplete_census(tmp_path, monkeypatch, damage):
    async with _source(tmp_path, monkeypatch) as (request, source, journal):
        _owner(monkeypatch, source, journal)
        async with _work(tmp_path, request, source, journal) as prepared:
            sealed = _seal(tmp_path, prepared, request, source, [])
        path = Path(sealed.root) / metadata._FILES["family"]
        _damage_sealed_file(path, damage)
        with pytest.raises((ValueError, FileNotFoundError, OSError)):
            with _reopen(tmp_path, sealed):
                pytest.fail("incomplete retained bytes cannot be exposed")
        assert not (tmp_path / "reopen-work").exists()
    shutil.rmtree(sealed.root)


def _damage_sealed_file(path, damage):
    if damage in ("bytes", "truncated"):
        content = path.read_bytes()
        path.chmod(0o600)
        path.write_bytes((content[:-1] + bytes((content[-1] ^ 1,))) if damage == "bytes" else content[:-1])
        path.chmod(0o400)
        return
    if damage == "missing":
        path.unlink()
        return
    if damage == "symlink":
        path.unlink()
        path.symlink_to(metadata._FILES["query"])
        return
    (path.parent / "unexpected").write_bytes(b"not part of the metadata")


def _rewrite_descriptor(sealed, change):
    path = Path(sealed.root) / metadata._DESCRIPTOR
    document = json.loads(path.read_bytes())
    change(document)
    path.chmod(0o600)
    contents = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    path.write_bytes(contents)
    path.chmod(0o400)
    return hashlib.sha256(contents).digest()


@pytest.mark.parametrize("damage", ("count", "relative_path", "saved_complete"))
async def test_even_a_committed_descriptor_requires_typed_eof_and_owned_names(tmp_path, monkeypatch, damage):
    async with _source(tmp_path, monkeypatch) as (request, source, journal):
        _owner(monkeypatch, source, journal)
        async with _work(tmp_path, request, source, journal) as prepared:
            sealed = _seal(tmp_path, prepared, request, source, [])

        def change(document):
            if damage == "count":
                document["files"]["family"]["count"] += 1
            elif damage == "relative_path":
                document["files"]["family"]["name"] = "../families.mpk"
            else:
                document["complete"] = True

        digest = _rewrite_descriptor(sealed, change)
        with pytest.raises(ValueError):
            with _reopen(tmp_path, sealed, descriptor_sha256=digest):
                pytest.fail("a saved flag cannot replace actual EOF")
    shutil.rmtree(sealed.root)


async def test_original_producer_witness_refuses_arbitrary_dataclass_and_mutation(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (request, source, journal):
        _owner(monkeypatch, source, journal)
        async with _work(tmp_path, request, source, journal) as prepared:
            constructed = metadata.OwnerSourceMetadata(prepared.work, prepared.family_path, prepared.query_path)
            with pytest.raises(ValueError, match="original producer scope"):
                _seal(tmp_path, constructed, request, source, [])
            prepared.family_path.write_bytes(prepared.family_path.read_bytes() + b"corrupt")
            with pytest.raises(ValueError, match="original file custody"):
                _seal(tmp_path, prepared, request, source, [])
    assert not (tmp_path / "retained-metadata").exists()


async def test_reopened_reader_lifetime_and_mutation_at_eof(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (request, source, journal):
        _owner(monkeypatch, source, journal)
        async with _work(tmp_path, request, source, journal) as prepared:
            sealed = _seal(tmp_path, prepared, request, source, [])
        with _reopen(tmp_path, sealed) as reopened:
            with closing(reopened.family_ids()) as families:
                assert next(families)[1][0] == 1001
        with pytest.raises(ValueError, match="closed"):
            reopened.family_ids()
        with pytest.raises(ValueError):
            with _reopen(tmp_path, sealed) as reopened:
                with closing(reopened.family_ids()) as families:
                    next(families)
                    _damage_sealed_file(reopened.family_path, "bytes")
                    list(families)
        assert not (tmp_path / "reopen-work").exists()
    shutil.rmtree(sealed.root)


@pytest.mark.parametrize("failure", ("file", "directory"))
async def test_unknown_fsync_never_returns_a_seal_and_preserves_owned_source(tmp_path, monkeypatch, failure):
    registered_roots = []
    async with _source(tmp_path, monkeypatch) as (request, source, journal):
        _owner(monkeypatch, source, journal)
        async with _work(tmp_path, request, source, journal) as prepared:
            expected_families = list(prepared.family_ids())
            original_fsync = metadata.os.fsync

            def uncertain(descriptor):
                if metadata.stat.S_ISDIR(os.fstat(descriptor).st_mode) == (failure == "directory"):
                    raise OSError("synthetic uncertain metadata fsync")
                return original_fsync(descriptor)

            monkeypatch.setattr(metadata.os, "fsync", uncertain)
            with pytest.raises(OSError, match="uncertain metadata fsync"):
                _seal(tmp_path, prepared, request, source, registered_roots)
            assert list(prepared.family_ids()) == expected_families
            assert not (tmp_path / "seal-work").exists()
            assert registered_roots == [tmp_path / "retained-metadata"]
            with pytest.raises(ValueError, match="protected descriptor commitment"):
                with metadata.reopen_owner_metadata(
                    registered_roots[0],
                    expected_scope=prepared.scope,
                    descriptor_sha256=None,
                    work=_restart_work(tmp_path, "reopen-work"),
                ):
                    pytest.fail("an uncertain filesystem write is not a protected checkpoint")
    shutil.rmtree(registered_roots[0])


def _rewrite_typed_file(tmp_path, sealed, role, transform):
    path = Path(sealed.root) / metadata._FILES[role]
    with closing(_Scratch(_restart_work(tmp_path, "rewrite-work"))) as work:
        rows = list(work.rows(path))
        transform(rows)
        path.chmod(0o600)
        with path.open("wb") as output:
            for row in rows:
                work.frame(output, row)
        path.chmod(0o400)

    def change(document):
        document["files"][role]["stamp"] = metadata._metadata_stamp(path)
        document["files"][role]["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()

    return _rewrite_descriptor(sealed, change)


@pytest.mark.parametrize("damage", ("family_id", "family_key", "query_rank", "query_order"))
async def test_reopening_checks_typed_native_rows_instead_of_only_hashes(tmp_path, monkeypatch, damage):
    async with _source(tmp_path, monkeypatch, query_child=True) as (request, source, journal):
        _owner(monkeypatch, source, journal)
        async with _work(tmp_path, request, source, journal) as prepared:
            sealed = _seal(tmp_path, prepared, request, source, [])
        role = "family" if damage.startswith("family") else "query"

        def change(rows):
            if damage == "family_id":
                rows[0] = rows[0][0], (True, rows[0][1][1])
            elif damage == "family_key":
                rows[0] = (("changed key", rows[0][0][1]), rows[0][1])
            elif damage == "query_rank":
                rows[0] = (100, *rows[0][1:])
            else:
                rows.reverse()

        digest = _rewrite_typed_file(tmp_path, sealed, role, change)
        with pytest.raises((ValueError, FamilyBlockValidationError)):
            with _reopen(tmp_path, sealed, descriptor_sha256=digest):
                pytest.fail("committed bytes still require typed native identity validation")
    shutil.rmtree(sealed.root)


async def test_global_query_hash_variants_keep_the_original_shared_rank(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch, query_child=True) as (request, source, journal):
        _owner(monkeypatch, source, journal)
        digest = b"h" * 32
        retained = ((digest, "variant z"), (digest, "variant a"))
        async with _work(tmp_path, request, source, journal, retained=retained) as prepared:
            expected_keys = list(prepared.query_keys())
            sealed = _seal(tmp_path, prepared, request, source, [])
        with _reopen(tmp_path, sealed) as reopened:
            assert list(reopened.query_keys()) == expected_keys
            variants = [row for row in expected_keys if row[0] == digest]
            assert [row[1] for row in variants] == ["variant a", "variant z"]
            assert variants[0][2] == variants[1][2] and [row[3] for row in variants] == [1, 2]
    shutil.rmtree(sealed.root)


async def test_cancelled_sealing_drains_before_scratch_cleanup(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (request, allocated_source, journal):
        _owner(monkeypatch, allocated_source, journal)
        async with _work(tmp_path, request, allocated_source, journal) as prepared:
            outer = _Scratch(_restart_work(tmp_path, "cpu-work"))
            entered, stopped, release = threading.Event(), threading.Event(), threading.Event()
            visits, registered_roots = [], []

            def check():
                visits.append(True)
                if len(visits) == 32:
                    entered.set()
                    assert outer.stop.wait(5)
                    stopped.set()
                    assert release.wait(5)
                outer.current()

            operation = partial(
                _seal, tmp_path, prepared, request, allocated_source, registered_roots, check=check, memory=1
            )
            pending = asyncio.create_task(_cpu(outer, operation))
            try:
                assert await asyncio.to_thread(entered.wait, 5)
                pending.cancel("first metadata seal cancellation")
                assert await asyncio.to_thread(stopped.wait, 5)
                assert not pending.done() and (tmp_path / "seal-work").exists()
                pending.cancel("second metadata seal cancellation")
            finally:
                release.set()
                await _drain_sealing_task(pending, outer)
            assert len(visits) == 32 and not (tmp_path / "seal-work").exists()
            assert len(list(prepared.family_ids())) == 2
            assert registered_roots == [tmp_path / "retained-metadata"]
            assert not (registered_roots[0] / metadata._DESCRIPTOR).exists()
    shutil.rmtree(registered_roots[0])


async def _drain_sealing_task(pending, outer):
    try:
        with pytest.raises(asyncio.CancelledError, match="first metadata seal cancellation"):
            await pending
    finally:
        outer.close()


async def test_cancelled_reopen_never_exposes_metadata_and_preserves_copy(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (request, source, journal):
        _owner(monkeypatch, source, journal)
        async with _work(tmp_path, request, source, journal) as prepared:
            sealed = _seal(tmp_path, prepared, request, source, [])

        def stopped():
            raise CancellationRequested("synthetic reopened owner cancellation")

        with pytest.raises(CancellationRequested, match="reopened owner cancellation"):
            with _reopen(tmp_path, sealed, work=_restart_work(tmp_path, "reopen-work", check=stopped)):
                pytest.fail("cancelled checkpoint metadata cannot escape")
        assert not (tmp_path / "reopen-work").exists()
        with _reopen(tmp_path, sealed) as reopened:
            assert len(list(reopened.family_ids())) == 2
    shutil.rmtree(sealed.root)


async def test_reopen_budget_refusal_preserves_copy_and_does_not_truncate_inputs(tmp_path, monkeypatch):
    async with _source(tmp_path, monkeypatch) as (request, source, journal):
        _owner(monkeypatch, source, journal)
        async with _work(tmp_path, request, source, journal) as prepared:
            sealed = _seal(tmp_path, prepared, request, source, [])
        work = PlanWork(tmp_path / "reopen-work", SourceFactLimits(64, 64, 64), lambda _path: None)
        with pytest.raises((FamilyBlockResourceLimit, SourceFactBudgetExceeded)):
            with _reopen(tmp_path, sealed, work=work):
                pytest.fail("resource exhaustion cannot expose a prefix as completed metadata")
        assert not (tmp_path / "reopen-work").exists()
        with _reopen(tmp_path, sealed) as reopened:
            assert len(list(reopened.family_ids())) == 2
    shutil.rmtree(sealed.root)
