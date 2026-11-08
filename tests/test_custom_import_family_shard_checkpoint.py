# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real immutable journal files with explicitly doubled native transactions."""

import asyncio
import importlib.util
import os
import shutil
import sqlite3
from contextlib import asynccontextmanager, contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import msgpack
import pytest
from sqlalchemy import BigInteger, CheckConstraint, LargeBinary

from db.models.custom_import_family_block import CustomImportFamilyBlockLayout
from process.custom_import import family_shard_checkpoint as checkpoint
from process.custom_import.build_source import _WINDOW
from process.custom_import.continuation_runtime import BuildContinuationContext
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_shard_control import _new_build
from process.custom_import.family_shard_facts import SourceFactLimits
from process.custom_import.family_shard_ids import _checked_ids
from process.custom_import.runner_types import CandidateRunnerError, LeaseAuthorityLost
from tests.test_custom_import_build_graph import _registry, _request


class _Native:
    """Only native authority/transport is doubled; commit visibility is separate."""

    def __init__(self, monkeypatch, *, continued=False):
        continuation = BuildContinuationContext(9, 7, b"p" * 32) if continued else None
        self.request = _request(continuation=continuation)
        registry = _registry(self.request.definition)
        self.layout = FamilyBlockLayout(self.request.definition, registry.child_collection_slots)
        self.execution = SimpleNamespace(capture_bundle_id=8, request_identity_sha256=b"r" * 32)
        self.build = _new_build(self.request, self.execution)
        self.build.build_id, self.build.phase = 7, "source"
        self.capture = SimpleNamespace(
            capture_bundle_id=8,
            dataset_id=1,
            definition_revision_id=2,
            schema_revision_id=3,
            capture_state="sealed",
            source_binding_revision_id=10,
            source_binding_sha256=b"b" * 32,
            payload_contract="custom-import/parquet-parts/v2",
            manifest_sha256=b"m" * 32,
            source_request_sha256=b"s" * 32,
            statement_sha256=b"q" * 32,
        )
        self.committed = dict(
            family_id=17,
            phase="loading",
            layout_contract=checkpoint.FAMILY_BLOCK_LAYOUT_CONTRACT,
            definition_sha256=bytes.fromhex(self.request.definition.digest),
            collection_slots=dict(self.layout.collection_slots),
            owner_checkpoint_revision=None,
            owner_checkpoint_sha256=None,
        )
        self.binding = SimpleNamespace(owner_oid=501, oids_by_role={"family": 601, "pack": 602})
        self.failure, self.live, self.transactions, self.authority_calls = None, True, 0, 0
        monkeypatch.setattr(checkpoint, "_page_session", self.page)
        monkeypatch.setattr(checkpoint.store, "_fresh_commit_authority", self.authority)
        monkeypatch.setattr(checkpoint.store, "_candidate", self.candidate)

    @asynccontextmanager
    async def page(self, _factory, request, build_id):
        assert request is self.request and build_id == 7
        self.transactions += 1
        session = SimpleNamespace(info={_WINDOW: object()}, in_transaction=lambda: True)
        session.descriptor = SimpleNamespace(**self.committed)
        session.get = self.get
        yield session, self.build
        if self.failure == "before":
            raise ConnectionError("synthetic failed commit")
        self.committed = vars(session.descriptor).copy()
        if self.failure == "after":
            raise ConnectionError("synthetic committed reply lost")

    async def get(self, model, identity, **_kwargs):
        observed_values_by_key = {
            checkpoint.CustomImportBuildAttempt: (7, self.build),
            checkpoint.CustomImportExecution: (4, self.execution),
            checkpoint.CustomImportCaptureBundle: (8, self.capture),
        }
        expected, value = observed_values_by_key[model]
        assert identity == expected
        return value

    async def authority(self, _session, request):
        self.authority_calls += 1
        if not self.live:
            raise LeaseAuthorityLost("synthetic current authority revoked")
        assert request is self.request

    async def candidate(self, session, request, family_id, layout, phases):
        assert request is self.request and family_id == 17 and layout is self.layout
        if session.descriptor.phase not in phases:
            raise FamilyBlockUnavailable("synthetic candidate phase differs")
        return session.descriptor, self.binding


@pytest.fixture
def case(tmp_path, monkeypatch):
    native = _Native(monkeypatch)
    root = tmp_path / "journal"
    root.mkdir(mode=0o700)
    limits = SourceFactLimits(8 * 1024**2, 1024**2, 256 * 1024)
    scope = checkpoint._scope_cells(native.build, native.capture, native.request, native.layout, native.binding, 17)
    native.checkpoint_storage = checkpoint.CommittedOwnerCheckpoint(str(root), scope, None, None, limits)
    return native, root, limits


def _record(ordinal=0, *, payload=None, kind="source_allocation"):
    actual_ids = _checked_ids(("root_revision", "child_revision", "root_revision"), [(1, 11), (2, 11), (3, 97)])
    return checkpoint.OwnerCheckpointRecord(
        kind,
        ordinal,
        b"i" * 32,
        (("source_plan", b"s" * 32),),
        msgpack.packb(actual_ids) if payload is None else payload,
    )


async def _append(case, record, expected=None):
    native, root, limits = case
    if not hasattr(native, "checkpoint_storage"):
        scope = checkpoint._scope_cells(native.build, native.capture, native.request, native.layout, native.binding, 17)
        native.checkpoint_storage = checkpoint.CommittedOwnerCheckpoint(str(root), scope, None, None, limits)
    async with native.page(None, native.request, 7) as (session, _build):
        return await checkpoint.append_owner_checkpoint(
            session,
            native.request,
            7,
            17,
            native.layout,
            storage=replace(native.checkpoint_storage, root=str(root), limits=limits),
            record=record,
            expected_head=expected,
        )


async def _read(case):
    native, root, limits = case
    return await checkpoint.read_owner_checkpoint(
        None,
        native.request,
        7,
        17,
        native.layout,
        root=root,
        limits=limits,
    )


async def test_observed_ids_and_named_artifact_commitments_survive_native_reopening(case):
    native, root, _limits = case
    empty = await _read(case)
    assert empty.head is None and list(empty.records()) == [] and not list(root.iterdir())
    first_record = _record()
    first = await _append(case, first_record)
    assert type(first) is checkpoint.PreparedOwnerCheckpoint
    second_record = checkpoint.OwnerCheckpointRecord("artifact", 1, b"i" * 32, (("metadata_descriptor", b"d" * 32),))
    second = await _append(case, second_record, first.head)
    reopened = await _read(case)
    assert reopened.head == second.head
    assert list(reopened.records()) == [(second.head, second_record), (first.head, first_record)]
    assert msgpack.unpackb(first_record.payload, use_list=False) == (11, 11, 97)
    assert native.authority_calls == 4
    assert all(path.stat().st_mode & 0o777 == 0o400 for path in root.iterdir())


@pytest.mark.parametrize("record_count", [0, 1, 64])
async def test_read_checks_directory_emptiness_without_enumerating_the_chain(case, monkeypatch, record_count):
    _native, root, _limits = case
    head, expected_records = None, []
    for ordinal in range(record_count):
        checkpoint_record = _record(ordinal)
        head = (await _append(case, checkpoint_record, head)).head
        expected_records.append((head, checkpoint_record))
    observed_names, opened_scans = [], []
    original_listdir, original_scandir = os.listdir, os.scandir

    def observed_listdir(directory):
        names = original_listdir(directory)
        observed_names.extend(names)
        return names

    @contextmanager
    def observed_scandir(directory):
        assert type(directory) is int and os.fstat(directory).st_ino == root.stat().st_ino
        with original_scandir(directory) as entries:
            opened_scans.append((directory, entries))

            def observed_entries():
                for entry in entries:
                    observed_names.append(entry.name)
                    yield entry

            yield observed_entries()

    monkeypatch.setattr(checkpoint.os, "listdir", observed_listdir)
    monkeypatch.setattr(checkpoint.os, "scandir", observed_scandir)
    reopened = await _read(case)
    assert len(observed_names) == int(record_count > 0)
    assert len(opened_scans) == 1 and next(opened_scans[0][1], None) is None
    with pytest.raises(OSError):
        os.fstat(opened_scans[0][0])
    assert reopened.head == head and list(reopened.records()) == list(reversed(expected_records))


@pytest.mark.parametrize("failure", ["open", "read"])
async def test_directory_probe_errors_propagate_and_close_descriptors(case, monkeypatch, failure):
    descriptors, opened_scans = [], []
    original_scandir = os.scandir
    error = OSError("synthetic directory scan failure")

    def failed_read():
        raise error

    @contextmanager
    def failed_scan(directory):
        descriptors.append(directory)
        if failure == "open":
            raise error
        with original_scandir(directory) as entries:
            opened_scans.append(entries)
            yield iter(failed_read, None)

    monkeypatch.setattr(checkpoint.os, "scandir", failed_scan)
    with pytest.raises(OSError) as caught:
        await _read(case)
    assert caught.value is error and len(descriptors) == 1
    assert all(next(entries, None) is None for entries in opened_scans)
    with pytest.raises(OSError):
        os.fstat(descriptors[0])


@pytest.mark.parametrize("create", [False, True])
async def test_nonempty_directory_without_an_owner_record_is_not_adopted(case, create):
    _native, root, _limits = case
    (root / "unowned").write_bytes(b"retained")
    with pytest.raises(FamilyBlockUnavailable, match="no original owner record"):
        if create:
            await _append(case, _record())
        else:
            await _read(case)
    assert not (root / checkpoint._OWNER).exists()


@pytest.mark.parametrize("damage", ["missing", "symlink", "directory", "stat_error"])
async def test_exact_owner_lookup_preserves_missing_type_and_access_failures(case, monkeypatch, damage):
    native, root, _limits = case
    prepared = await _append(case, _record())
    owner = root / checkpoint._OWNER
    original_stat = os.stat

    def failed_owner_stat(path, **kwargs):
        if path == checkpoint._OWNER and "dir_fd" in kwargs:
            raise PermissionError("synthetic owner lookup denied")
        return original_stat(path, **kwargs)

    if damage == "stat_error":
        monkeypatch.setattr(checkpoint.os, "stat", failed_owner_stat)
    else:
        owner.rename(root / "original-owner")
        if damage == "symlink":
            owner.symlink_to("original-owner")
        elif damage == "directory":
            owner.mkdir(mode=0o700)
    expected_error = PermissionError if damage == "stat_error" else (FamilyBlockUnavailable, OSError)
    with pytest.raises(expected_error):
        await _read(case)
    assert native.committed["owner_checkpoint_revision"] == prepared.head.revision
    assert native.committed["owner_checkpoint_sha256"] == prepared.head.sha256


async def test_directory_replacement_during_probe_is_rejected(case, monkeypatch):
    _native, root, _limits = case
    original_scandir = os.scandir

    @contextmanager
    def replaced_scan(directory):
        with original_scandir(directory) as entries:
            yield entries
        root.rename(root.with_name("previous"))
        root.mkdir(mode=0o700)

    monkeypatch.setattr(checkpoint.os, "scandir", replaced_scan)
    with pytest.raises(FamilyBlockUnavailable, match="root changed during access"):
        await _read(case)


async def test_exact_immediate_replay_never_writes_another_file_and_conflict_refuses(case):
    _native, root, _limits = case
    first = await _append(case, _record())
    names = {path.name for path in root.iterdir()}
    assert await _append(case, _record()) == first
    assert {path.name for path in root.iterdir()} == names
    with pytest.raises(FamilyBlockUnavailable, match="conflicts"):
        await _append(case, _record(payload=b"different"))
    second = await _append(case, _record(1), first.head)
    with pytest.raises(FamilyBlockUnavailable, match="conflicts"):
        await _append(case, _record())
    assert (await _read(case)).head == second.head


@pytest.mark.parametrize("failure", ["before", "after", "cancel"])
async def test_prepared_file_never_proves_commit_and_fresh_read_reconciles(case, failure):
    native, root, limits = case
    native.failure = failure
    prepared = None
    error_type = asyncio.CancelledError if failure == "cancel" else ConnectionError
    with pytest.raises(error_type):
        async with native.page(None, native.request, 7) as (session, _build):
            prepared = await checkpoint.append_owner_checkpoint(
                session,
                native.request,
                7,
                17,
                native.layout,
                storage=replace(native.checkpoint_storage, root=str(root), limits=limits),
                record=_record(),
                expected_head=None,
            )
            assert native.committed["owner_checkpoint_revision"] is None
            assert (root / checkpoint._filename(prepared.head)).exists()
            if failure == "cancel":
                raise asyncio.CancelledError()
    native.failure = None
    observed = await _read(case)
    assert observed.head == (prepared.head if failure == "after" else None)
    assert (root / checkpoint._filename(prepared.head)).exists()
    if failure == "after":
        assert await _append(case, _record()) == prepared
    else:
        committed = await _append(case, _record(payload=b"new actual observations"))
        assert committed.head != prepared.head and committed.head.revision == 1
        assert (await _read(case)).head == committed.head


@pytest.mark.parametrize("damage", ["missing", "truncated", "trailing", "bytes", "inode", "mode", "symlink"])
async def test_reopening_refuses_missing_or_changed_committed_file(case, damage):
    _native, root, _limits = case
    prepared = await _append(case, _record())
    path = root / checkpoint._filename(prepared.head)
    wire = path.read_bytes()
    if damage == "missing":
        path.unlink()
    elif damage == "inode":
        original = path.rename(root.with_name("original-checkpoint"))
        path.write_bytes(wire)
        path.chmod(0o400)
        assert path.stat().st_ino != original.stat().st_ino
    elif damage == "mode":
        path.chmod(0o600)
    elif damage == "symlink":
        other = root / "other"
        path.rename(other)
        path.symlink_to(other.name)
    else:
        path.chmod(0o600)
        replacement = {"truncated": wire[:-1], "trailing": wire + b"extra", "bytes": wire[:-1] + bytes([wire[-1] ^ 1])}[
            damage
        ]
        path.write_bytes(replacement)
        path.chmod(0o400)
    with pytest.raises((FamilyBlockUnavailable, OSError, ValueError)):
        await _read(case)


async def test_full_chain_requires_previous_file_sha_and_eof(case):
    _native, root, _limits = case
    first = await _append(case, _record())
    await _append(case, _record(1), first.head)
    earlier = root / checkpoint._filename(first.head)
    earlier.chmod(0o600)
    earlier.write_bytes(earlier.read_bytes()[:-1])
    earlier.chmod(0o400)
    reopened = await _read(case)
    records = reopened.records()
    assert next(records)[1].ordinal == 1
    with pytest.raises(FamilyBlockUnavailable):
        next(records)


async def test_directory_identity_and_native_scope_are_not_adopted(case, tmp_path):
    native, root, limits = case
    await _append(case, _record())
    foreign = tmp_path / "copied"
    shutil.copytree(root, foreign)
    with pytest.raises(FamilyBlockUnavailable):
        await _read((native, foreign, limits))
    native.capture.manifest_sha256 = b"x" * 32
    with pytest.raises(FamilyBlockUnavailable, match="another native scope"):
        await _read(case)


@pytest.mark.parametrize("damage", ["request", "capture", "layout", "relation", "partial_head", "revoked"])
async def test_current_native_scope_and_authority_are_rechecked(case, damage):
    native, _root, _limits = case
    await _append(case, _record())
    match damage:
        case "request":
            native.build.page_byte_limit += 1
        case "capture":
            native.capture.capture_state = "pending"
        case "layout":
            native.committed["definition_sha256"] = b"x" * 32
        case "relation":
            native.binding.oids_by_role["pack"] += 1
        case "partial_head":
            native.committed["owner_checkpoint_revision"] = None
        case "revoked":
            native.live = False
    with pytest.raises((FamilyBlockUnavailable, CandidateRunnerError, LeaseAuthorityLost)):
        await _read(case)


@pytest.mark.parametrize("phase", ["closed", "verified", "logged"])
async def test_closed_head_is_readable_but_never_advanced(case, phase):
    native, root, _limits = case
    first = await _append(case, _record())
    native.committed["phase"] = phase
    assert (await _read(case)).head == first.head
    names = {path.name for path in root.iterdir()}
    with pytest.raises(FamilyBlockUnavailable, match="phase"):
        await _append(case, _record(1), first.head)
    assert {path.name for path in root.iterdir()} == names


async def test_continued_request_uses_current_epoch_and_retained_producer(tmp_path, monkeypatch):
    native = _Native(monkeypatch, continued=True)
    root = tmp_path / "continued"
    root.mkdir(mode=0o700)
    case = native, root, SourceFactLimits(1024**2, 128 * 1024, 128 * 1024)
    first = await _append(case, _record())
    result = await _read(case)
    assert result.head == first.head and result.scope[7] == b"p" * 32
    native.live = False
    with pytest.raises(LeaseAuthorityLost):
        await _read(case)


def test_record_accepts_only_bounded_explicit_primitive_metadata():
    record = _record()
    for changes in (
        {"kind": "unknown"},
        {"ordinal": True},
        {"ordinal": -1},
        {"input_sha256": b"short"},
        {"payload": object()},
        {"commitments": (("x", b"x" * 32), ("x", b"x" * 32))},
        {"commitments": (("../path", b"x" * 32),)},
        {"commitments": tuple((f"a{i:02d}", b"x" * 32) for i in range(33))},
    ):
        with pytest.raises(ValueError):
            replace(record, **changes)


async def test_file_budget_and_non_page_append_fail_without_advancing_head(case):
    native, root, limits = case
    with pytest.raises(ValueError, match="budget"):
        await _append(case, _record(payload=b"x" * limits.memory_bytes))
    assert (await _read(case)).head is None
    session = SimpleNamespace(info={}, in_transaction=lambda: False)
    with pytest.raises(FamilyBlockUnavailable, match="page transaction"):
        await checkpoint.append_owner_checkpoint(
            session,
            native.request,
            7,
            17,
            native.layout,
            storage=replace(native.checkpoint_storage, root=str(root), limits=limits),
            record=_record(),
            expected_head=None,
        )


@pytest.mark.parametrize(
    "pair", [(None, None), (None, b"x" * 32), (1, None), (0, b"x" * 32), (-1, b"x" * 32), (1, b"x"), (1, b"x" * 32)]
)
def test_model_nullable_checkpoint_pair_and_exact_unpublished_migration(pair):
    table = CustomImportFamilyBlockLayout.__table__
    assert isinstance(table.c.owner_checkpoint_revision.type, BigInteger)
    assert isinstance(table.c.owner_checkpoint_sha256.type, LargeBinary)
    check = next(
        item
        for item in table.constraints
        if isinstance(item, CheckConstraint) and "owner_checkpoint_revision" in str(item.sqltext)
    )
    with sqlite3.connect(":memory:") as database:
        database.create_function("octet_length", 1, lambda value: None if value is None else len(value))
        database.execute(
            f"CREATE TABLE checkpoint (owner_checkpoint_revision BIGINT, owner_checkpoint_sha256 BLOB, CHECK ({check.sqltext}))"
        )
        is_valid = pair == (None, None) or pair[0] == 1 and pair[1] == b"x" * 32
        if is_valid:
            database.execute("INSERT INTO checkpoint VALUES (?,?)", pair)
        else:
            with pytest.raises(sqlite3.IntegrityError):
                database.execute("INSERT INTO checkpoint VALUES (?,?)", pair)
    path = Path("alembic/versions/20261104020000_custom_import_family_blocks.py")
    spec = importlib.util.spec_from_file_location("checkpoint_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    compact = lambda value: "".join(value.split())
    assert compact(str(check.sqltext)) in compact("".join(migration._DDL))
