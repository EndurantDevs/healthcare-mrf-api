# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Original pack/rejection bytes and bounded native common-dictionary lookups."""

import asyncio
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy.dialects import postgresql

from process.custom_import import family_shard_allocate as allocation
from process.custom_import import family_shard_common as common
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_shard_facts import SourceFactLimits
from process.custom_import.family_shard_input import cleanup_spool
from process.custom_import.family_shard_source_plan import cleanup_source_plan
from process.custom_import.runner_codec import root_key_contract_hash
from process.custom_import.runner_graph import entity_value_digest
from tests.test_custom_import_family_shard_allocate import _allocate, _native_boundary
from tests.test_custom_import_family_shard_source_plan import _build, _case, _child, _root

_STAMP = datetime(2030, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
_ROOT_STAMP = datetime(2000, 2, 3, tzinfo=timezone.utc)
_ENTITY_STAMP = datetime(2001, 3, 4, tzinfo=timezone.utc)


@pytest.fixture
async def common_custody_case(tmp_path, monkeypatch):
    """Close real SOURCE/admission producers; only native dictionary lookup is doubled."""
    from tests.test_custom_import_family_shard_source_completion import _completion_case

    async with _completion_case(tmp_path, monkeypatch) as (context, admission, output, receipt, limits):
        custody = common.source_common_custody(context, admission, output, receipt, limits, None)
        yield context, admission, output, receipt, limits, custody


async def test_common_custody_preserves_original_bytes(common_custody_case, monkeypatch):
    """Borrowing the committed spool never consults a replacement native timestamp."""
    context, admission, output, receipt, limits, custody = common_custody_case

    def forbid_lookup(*_arguments, **_keywords):
        pytest.fail("common reopening must not read a new timestamp or dictionary")

    monkeypatch.setattr(common, "_lookup", forbid_lookup)
    checkpoint_record = common.source_common_record(b"r" * 32, custody)
    cells = common._unpack_row(checkpoint_record.payload)
    assert common.source_common_payload(checkpoint_record, cells) == (b"r" * 32, custody)
    with common.reopen_source_common(context, admission, custody, limits, None) as reopened:
        assert reopened.sha256 == output.sha256 and reopened.proof_inputs == output.proof_inputs
        assert reopened.proof_inputs["owner_created_at"] == _STAMP.isoformat()
        assert _rows(context, reopened) == _rows(context, output)
        assert common.source_common_custody(context, admission, reopened, receipt, limits, None) == custody
    assert (Path(custody.root) / "output.mpk").is_file()


@pytest.mark.parametrize("damage", ("bytes", "receipt", "timestamp", "identity"))
async def test_common_reopen_refuses_changed_evidence(common_custody_case, damage):
    """Original file bytes, receipt and typed proof must agree before any COPY."""
    context, admission, _output, receipt, limits, custody = common_custody_case
    if damage == "bytes":
        path = Path(custody.root) / "output.mpk"
        path.chmod(0o600)
        path.write_bytes(path.read_bytes() + b"changed")
        path.chmod(0o400)
    elif damage == "receipt":
        custody = replace(custody, receipt=common._pack_row(receipt | {"result_sha256": b"x" * 32}))
    else:
        proof_by_name = common._common_control(custody.proof_inputs)
        field, replacement = ("owner_created_at", None) if damage == "timestamp" else ("execution_id", "changed")
        custody = replace(custody, proof_inputs=common._pack_row(proof_by_name | {field: replacement}))
    with pytest.raises((ValueError, FamilyBlockUnavailable)):
        common.reopen_source_common(context, admission, custody, limits, None)


@pytest.mark.parametrize("directory", ("common", "parent"))
async def test_common_reopen_refuses_replaced_directory(common_custody_case, directory):
    """Even unchanged file bytes cannot authorize a replacement intermediate directory."""
    context, admission, _output, _receipt, limits, custody = common_custody_case
    root = Path(custody.root)
    replaced = root if directory == "common" else root.parent
    retained = replaced.with_name(replaced.name + "-retained")
    replaced.rename(retained)
    replaced.mkdir(mode=0o700)
    if directory == "parent":
        (retained / "common").rename(root)
    try:
        with pytest.raises(ValueError, match="custody"):
            common.reopen_source_common(context, admission, custody, limits, None)
    finally:
        if directory == "parent":
            root.rename(retained / "common")
        replaced.rmdir()
        retained.rename(replaced)


@asynccontextmanager
async def _context(tmp_path, monkeypatch, *, empty=False):
    records = (
        {"root_source": [[]], "detail_source": [[]]}
        if empty
        else {
            "root_source": [[_root(), _root(npi=None), _root(score=None)]],
            "detail_source": [[_child(), _child("orphan-parent"), _child(key=None)]],
        }
    )
    case = await _case(tmp_path, records, row_limit=2)
    plan = _build(tmp_path, case)
    _native_boundary(monkeypatch, plan)
    source = await _allocate(tmp_path, case[1], plan)
    try:
        layout = FamilyBlockLayout(case[1].definition, case[2].child_collection_slots)
        yield common.SourceCommonContext(case[1], layout, source)
    finally:
        allocation.cleanup_source_allocations(source)
        cleanup_source_plan(plan)
        cleanup_spool(case[0])


def _root_rows(context):
    rows_by_id = {}
    for pack, _pack_id, roots, _revisions, _rejections in allocation.read_source_allocations(context.source):
        for record, root in zip(pack.records, roots, strict=True):
            if record.typed_parent_key is not None:
                key, digest = record.typed_parent_key
                rows_by_id[root] = dict(
                    root_record_id=root,
                    dataset_id=context.request.dataset_id,
                    key_contract_sha256=root_key_contract_hash(context.request.definition),
                    canonical_logical_key=key,
                    logical_key_sha256=digest,
                    created_at=_ROOT_STAMP,
                )
    return rows_by_id


def _entity_row(context, identity=71):
    return dict(
        entity_binding_id=identity,
        dataset_id=context.request.dataset_id,
        adapter_id="npi",
        canonical_value="1003000126",
        value_sha256=entity_value_digest("1003000126"),
        created_at=_ENTITY_STAMP,
    )


def _lookup_fixture(monkeypatch, context, *, refusal=False):
    roots, events = _root_rows(context), []

    async def lookup(factory, supplied, role=None, requests=()):
        assert supplied is context
        events.append((role, requests))
        if role is None:
            return _STAMP
        if refusal:
            raise FamilyBlockUnavailable("synthetic exact native dictionary mismatch")
        native_rows = (
            (roots[native_row[0]] for native_row in requests)
            if role == "root_record"
            else (_entity_row(context, native_row) for native_row in requests)
        )
        return tuple(common._cells(context, role, native_row) for native_row in native_rows)

    monkeypatch.setattr(common, "_lookup", lookup)
    return events, roots


async def _build_common(tmp_path, context, selected=(71,), keys=(), *, limits=None):
    return await common.build_source_common(
        None,
        context,
        iter(selected),
        iter(keys),
        root=tmp_path / "common",
        register_cleanup=lambda path: None,
        limits=limits or allocation.SourceAllocationLimits(SourceFactLimits(64 * 1024**2, 1024**2, 4096), 1, 1024**2),
    )


def _rows(context, output):
    by_role = {role: [] for role in common._ROLES}
    for batch in output.batches(maximum_rows=2, maximum_bytes=1024**2):
        for role, records in batch.items():
            by_role[role].extend(
                dict(zip(context.columns_by_role[role], native_row, strict=True)) for native_row in records
            )
    return by_role


def _source_rejections(originals):
    return [
        (ids[index], pack.initial_rejection_ordinals[index], pack_id, landing)
        for pack, pack_id, _roots, _revisions, ids in originals
        for index, landing in enumerate(pack.landing.records)
        if ids[index] is not None
    ]


@pytest.mark.asyncio
async def test_source_common_preserves_typed_roots_and_exact_rejections(tmp_path, monkeypatch):
    async with _context(tmp_path, monkeypatch) as context:
        events, expected_roots = _lookup_fixture(monkeypatch, context)
        # Physical shapes are compiled once; no per-row table/model regeneration.
        monkeypatch.setattr(
            FamilyBlockLayout, "tables", lambda *args: (_ for _ in ()).throw(AssertionError("recompiled tables"))
        )
        with await _build_common(tmp_path, context, selected=(71, 71)) as output:
            rows_by_role = _rows(context, output)
            assert rows_by_role["root_record"] == [expected_roots[key] for key in sorted(expected_roots)]
            assert len(expected_roots) == 2 and "orphan-parent" in repr(rows_by_role["root_record"])
            assert rows_by_role["entity_binding"] == [_entity_row(context)]
            originals = list(allocation.read_source_allocations(context.source))
            assert [native_row["pack_id"] for native_row in rows_by_role["pack"]] == [
                pack_id for _pack, pack_id, *_ids in originals
            ]
            assert [native_row["record_count"] for native_row in rows_by_role["pack"]] == [1, 0, 2, 0]
            wanted_rows = _source_rejections(originals)
            for actual, (native_id, ordinal, pack_id, landing) in zip(
                rows_by_role["rejection"], wanted_rows, strict=True
            ):
                assert (actual["rejection_id"], actual["rejection_ordinal"], actual["pack_id"]) == (
                    native_id,
                    ordinal,
                    pack_id,
                )
                assert (
                    actual["canonical_root_key"],
                    actual["root_key_sha256"],
                    actual["canonical_evidence"],
                    actual["code"],
                ) == (
                    landing.rejection_root_key_canonical,
                    landing.rejection_root_key_sha256,
                    landing.canonical_evidence,
                    landing.rejection_code,
                )
                assert actual["created_at"] == _STAMP and actual["field_slot"] is None
            assert [native_row["collection_slot"] for native_row in rows_by_role["rejection"]] == [
                None,
                None,
                context.layout.collection_slots["details"],
            ]
            assert all(native_row["created_at"] == _STAMP for native_row in rows_by_role["pack"])
            assert output.proof_inputs["next_admission_rejection_ordinal"] == context.source.source.counts[4] == 3
            assert output.proof_inputs["admission_events_pending"] is True
            assert output.role_counts == {role: len(native_rows) for role, native_rows in rows_by_role.items()}
            assert _rows(context, output) == rows_by_role
            assert output.proof_inputs["owner_created_at"] == _STAMP.isoformat()
        assert not (tmp_path / "common").exists() and Path(context.source.root).exists()
        assert len([event for event in events if event[0] == "root_record"]) == 2
        assert [event[0] for event in events].count(None) == 2


@pytest.mark.asyncio
async def test_empty_source_still_observes_current_authority_and_closed_replay(tmp_path, monkeypatch):
    async with _context(tmp_path, monkeypatch, empty=True) as context:
        events, _roots = _lookup_fixture(monkeypatch, context)
        with await _build_common(tmp_path, context, selected=()) as output:
            assert output.role_counts == dict.fromkeys(common._ROLES, 0)
            assert output.proof_inputs["source_records"] == 0
            assert list(output.batches(maximum_rows=1, maximum_bytes=4096)) == []
        assert events == [(None, ()), (None, ())]


@pytest.mark.asyncio
async def test_native_lookup_preserves_scopes_and_actual_bytes(tmp_path, monkeypatch):
    async with _context(tmp_path, monkeypatch) as context:
        root_by_id = _root_rows(context)
        root = next(iter(root_by_id.values()))
        queries, authority = [], []

        @asynccontextmanager
        async def page(factory, request, build):
            authority.append("begin")
            assert request is context.request and build is None
            yield None, None
            authority.append("fresh-owner-commit")

        async def candidate(*args):
            authority.append("candidate")

        async def schema(session):
            return postgresql.dialect().identifier_preparer, 'common"scope'

        async def execute(session, statement, parameters=None):
            sql = str(statement)
            queries.append((sql, parameters))
            if "custom_import_execution" in sql:
                capture_by_field = dict(context.source.source.scope[0])
                return SimpleNamespace(
                    one=lambda: (capture_by_field["capture_bundle_id"], capture_by_field["source_binding_revision_id"])
                )
            if "transaction_timestamp" in sql:
                return SimpleNamespace(scalar_one=lambda: _STAMP)
            native_rows = [root] if "custom_import_root_record" in sql else [_entity_row(context)]
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: native_rows))

        monkeypatch.setattr(common, "_page_session", page)
        monkeypatch.setattr(common, "_candidate", candidate)
        monkeypatch.setattr(common, "_control_schema", schema)
        monkeypatch.setattr(common, "_execute", execute)
        request = (root["root_record_id"], root["canonical_logical_key"], root["logical_key_sha256"])
        assert await common._lookup(None, context) == _STAMP
        assert await common._lookup(None, context, "root_record", (request,)) == (
            common._cells(context, "root_record", root),
        )
        assert await common._lookup(None, context, "entity_binding", (71,)) == (
            common._cells(context, "entity_binding", _entity_row(context)),
        )
        assert authority == ["begin", "candidate", "fresh-owner-commit"] * 3
        sql, parameters = next(native_row for native_row in queries if "WITH requested" in native_row[0])
        assert '"common""scope".custom_import_root_record' in sql and "WITH ORDINALITY" in sql
        assert all(
            fragment in sql
            for fragment in (
                "g.root_record_id=i.id",
                "g.dataset_id=:dataset",
                "g.key_contract_sha256=:contract",
                "g.logical_key_sha256=i.hash",
                "convert_to(g.canonical_logical_key,'UTF8')=convert_to(i.key,'UTF8')",
            )
        )
        assert parameters["keys"] == [request[1]] and parameters["ids"] == [request[0]]
        assert parameters["contract"] == root_key_contract_hash(context.request.definition)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["lookup", "budget", "late_keys"])
async def test_failed_common_output_removes_only_its_exact_root(tmp_path, monkeypatch, failure):
    async with _context(tmp_path, monkeypatch) as context:
        _lookup_fixture(monkeypatch, context, refusal=failure == "lookup")
        sibling = tmp_path / "preserve"
        sibling.write_text("keep")
        bounds = allocation.SourceAllocationLimits(SourceFactLimits(64 * 1024**2, 1024**2, 4096), 1, 1024**2)
        if failure == "budget":
            bounds = replace(bounds, spool=SourceFactLimits(128, 128, 128))

        def late():
            raise RuntimeError("synthetic global key EOF failure")
            yield

        with pytest.raises((FamilyBlockUnavailable, OSError, RuntimeError)):
            await common.build_source_common(
                None,
                context,
                iter((71,)),
                late() if failure == "late_keys" else (),
                root=tmp_path / "common",
                register_cleanup=lambda path: None,
                limits=bounds,
            )
        assert not (tmp_path / "common").exists() and sibling.read_text() == "keep"
        assert Path(context.source.root).exists()


@pytest.mark.asyncio
async def test_completed_global_query_ranks_are_copied_without_local_reassignment(tmp_path, monkeypatch):
    async with _context(tmp_path, monkeypatch) as context:
        _lookup_fixture(monkeypatch, context)
        keys = ((b"a" * 32, "legacy-a", 1, 1), (b"a" * 32, "legacy-b", 1, 2), (b"b" * 32, "later", 2, 1))
        with await _build_common(tmp_path, context, keys=keys) as output:
            rows = _rows(context, output)["key_dictionary"]
            assert [
                (row["child_key_sha256"], row["canonical_child_key"], row["key_rank"], row["key_variant"])
                for row in rows
            ] == list(keys)
        with pytest.raises(FamilyBlockUnavailable, match="dictionary rank"):
            await _build_common(tmp_path, context, keys=((b"a" * 32, "legacy-a", 7, 1),))
        assert not (tmp_path / "common").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("empty", [False, True])
async def test_expired_common_work_preserves_originals(tmp_path, monkeypatch, empty):
    from process.custom_import import family_shard_base as base

    async with _context(tmp_path, monkeypatch, empty=empty) as context:
        _lookup_fixture(monkeypatch, context)
        monkeypatch.setattr(base, "_read_deadline", lambda _request: 0)
        with pytest.raises(TimeoutError, match="operation deadline"):
            await _build_common(tmp_path, context)
        assert not (tmp_path / "common").exists()
        assert Path(context.source.root).exists() and Path(context.source.source.root).exists()


@pytest.mark.asyncio
async def test_cancelled_file_work_finishes_before_owned_spool_cleanup(tmp_path, monkeypatch):
    async with _context(tmp_path, monkeypatch) as context:
        _lookup_fixture(monkeypatch, context)
        original, finished = common._source_rows, threading.Event()

        def _slow_rows(*arguments):
            time.sleep(0.2)
            try:
                yield from original(*arguments)
            finally:
                finished.set()

        monkeypatch.setattr(common, "_source_rows", _slow_rows)
        pending = asyncio.create_task(_build_common(tmp_path, context))
        await asyncio.sleep(0.05)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert finished.is_set() and not (tmp_path / "common").exists()
        assert Path(context.source.root).exists()


@pytest.mark.asyncio
async def test_repeated_cancellation_drains_original_thread_before_owned_cleanup(tmp_path, monkeypatch):
    async with _context(tmp_path, monkeypatch) as context:
        _lookup_fixture(monkeypatch, context)
        original_rows, original_close = common._source_rows, common._Scratch.close
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()
        cleanup_observations = []
        root = tmp_path / "common"

        def blocked(*arguments):
            entered.set()
            try:
                if not release.wait(5):
                    raise TimeoutError("test did not release original SOURCE writer")
                yield from original_rows(*arguments)
            finally:
                finished.set()

        def observe_close(work):
            if work.root == root:
                cleanup_observations.append(finished.is_set())
            original_close(work)

        monkeypatch.setattr(common, "_source_rows", blocked)
        monkeypatch.setattr(common._Scratch, "close", observe_close)
        pending = asyncio.create_task(_build_common(tmp_path, context))
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            for message in ("first SOURCE cancellation", "second cancellation", "third cancellation"):
                pending.cancel(message)
                for _ in range(3):
                    await asyncio.sleep(0)
            assert not pending.done() and root.exists() and not finished.is_set()
        finally:
            release.set()
            with pytest.raises(asyncio.CancelledError) as captured:
                await pending
        assert captured.value.args == ("first SOURCE cancellation",)
        assert finished.is_set() and cleanup_observations == [True]
        assert not root.exists() and Path(context.source.root).exists()


@pytest.mark.asyncio
async def test_normal_file_error_retains_identity_without_cleanup_error_annotation(tmp_path, monkeypatch):
    async with _context(tmp_path, monkeypatch) as context:
        _lookup_fixture(monkeypatch, context)
        failure = OSError("original SOURCE file failure")

        def failed_rows(*arguments):
            raise failure
            yield

        monkeypatch.setattr(common, "_source_rows", failed_rows)
        with pytest.raises(OSError) as captured:
            await _build_common(tmp_path, context)
        assert captured.value is failure and failure.__cause__ is None
        assert not getattr(failure, "__notes__", ())
        assert not getattr(failure, "_custom_import_retry_blocked", False)
        assert not (tmp_path / "common").exists() and Path(context.source.root).exists()


@pytest.mark.asyncio
async def test_common_replay_refuses_changed_closed_bytes(tmp_path, monkeypatch):
    async with _context(tmp_path, monkeypatch) as context:
        _lookup_fixture(monkeypatch, context)
        with await _build_common(tmp_path, context) as output:
            path = output._replay.path
            path.chmod(0o600)
            payload = path.read_bytes()
            path.write_bytes(payload[:-1] + bytes([payload[-1] ^ 1]))
            path.chmod(0o400)
            with pytest.raises((ValueError, TypeError)):
                list(output.batches(maximum_rows=1, maximum_bytes=1024**2))


@pytest.mark.asyncio
@pytest.mark.parametrize("problem", ["capture", "missing", "entity_hash"])
async def test_native_common_lookup_refuses_changed_custody_or_dictionary(problem, tmp_path, monkeypatch):
    async with _context(tmp_path, monkeypatch) as context:

        @asynccontextmanager
        async def page(*arguments):
            yield None, None

        async def candidate(*arguments):
            return None

        async def schema(session):
            return postgresql.dialect().identifier_preparer, "synthetic_common"

        async def execute(session, statement, parameters=None):
            if "custom_import_execution" in str(statement):
                capture_by_field = dict(context.source.source.scope[0])
                identity = (
                    capture_by_field["capture_bundle_id"] + int(problem == "capture"),
                    capture_by_field["source_binding_revision_id"],
                )
                return SimpleNamespace(one=lambda: identity)
            native = _entity_row(context) | {"value_sha256": b"x" * 32}
            return SimpleNamespace(
                mappings=lambda: SimpleNamespace(all=lambda: [] if problem == "missing" else [native])
            )

        monkeypatch.setattr(common, "_page_session", page)
        monkeypatch.setattr(common, "_candidate", candidate)
        monkeypatch.setattr(common, "_control_schema", schema)
        monkeypatch.setattr(common, "_execute", execute)
        with pytest.raises(FamilyBlockUnavailable):
            await common._lookup(None, context, "entity_binding", (71,))
