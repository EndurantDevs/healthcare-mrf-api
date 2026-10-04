# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded graph pages preserve existing family and projection semantics."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
from contextlib import contextmanager, nullcontext
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import select
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import DBAPIError

from db.models.custom_import import (
    CustomImportBuildCandidateContext,
    CustomImportBuildFamily,
    CustomImportBuildOccurrence,
    CustomImportChildRevision,
    CustomImportChildScalar,
    CustomImportFamilyChild,
    CustomImportFamilyRevision,
    CustomImportPack,
    CustomImportRootRecord,
    CustomImportRootRevision,
)
from process.custom_import import build_graph as graph
from process.custom_import import build_source as staging
from process.custom_import.build_source import SourceBuildRequest
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import assemble_root_families
from process.custom_import.runner_codec import (
    child_key_document,
    child_key_hash,
    digest_text,
    fields_by_collection,
    new_family_hash,
    record_payload,
)
from process.custom_import.runner_types import (
    CancellationRequested,
    CandidateRegistry,
    CandidateRunnerError,
    LeaseAuthorityLost,
)
from tests.test_custom_import_definition import _raw_definition, _root
from tests.test_custom_import_snowflake_shared_capture import _shared_definition


def _request(**changes):
    arguments_by_name = dict(
        dataset_id=1,
        definition_revision_id=2,
        schema_revision_id=3,
        execution_id=4,
        lease_token=b"synthetic-owner",
        fence=1,
        definition=CustomImportDefinition.from_mapping(_raw_definition()),
        expected_base_generation_id=None,
        expected_pointer_version=0,
        complete_scope=False,
        page_row_limit=16,
        page_byte_limit=65_536,
        statement_timeout_ms=1000,
        build_deadline_at=dt.datetime(2030, 1, 1, tzinfo=dt.UTC),
        lease_seconds=120,
    )
    return SourceBuildRequest(**(arguments_by_name | changes))


def _registry(definition):
    return CandidateRegistry(
        {collection.name: slot for slot, collection in enumerate(definition.child_collections, 1)},
        {stream.stream_id: slot for slot, stream in enumerate(definition.source_streams, 1)},
        1,
    )


def _revision(definition, collection, child_values, child_id=9, collection_slot=1):
    payload = record_payload(fields_by_collection(definition)[collection], child_values)
    return CustomImportChildRevision(
        child_revision_id=child_id,
        collection_slot=collection_slot,
        canonical_child_key=child_key_document(definition, collection, child_values),
        child_key_sha256=child_key_hash(definition, collection, child_values),
        canonical_payload=payload,
        payload_sha256=digest_text("child-payload", payload),
        canonical_parent_key="synthetic-parent",
        parent_key_sha256=b"p" * 32,
    )


def test_metadata_admission_stops_before_over_budget_payload():
    assert graph._admitted_keys([(1, 2), (2, 3), (3, 4)], 5) == [(1,), (2,)]
    with pytest.raises(CandidateRunnerError, match="one build record"):
        graph._admitted_keys([(1, 6)], 5)


@pytest.mark.parametrize(
    "state,failure_type",
    [("canceling", CancellationRequested), ("failed", LeaseAuthorityLost), ("completed", LeaseAuthorityLost)],
)
def test_read_authority_preserves_cancellation_before_payload_fetch(monkeypatch, state, failure_type):
    request = _request()
    build, execution = object(), SimpleNamespace(state=state)
    identity_row = build, execution, object(), dt.datetime(2030, 1, 1, tzinfo=dt.UTC), "read committed"
    session = SimpleNamespace(
        begin=nullcontext,
        execute=Mock(side_effect=[None, SimpleNamespace(one=lambda: identity_row)]),
    )
    verify_identity = Mock()
    monkeypatch.setattr(graph, "_verify_request", verify_identity)
    with pytest.raises(failure_type), graph._read_transaction(session, request, 7):
        raise AssertionError("a non-running execution cannot read payloads")
    verify_identity.assert_called_once_with(build, request, execution)
    assert session.execute.call_count == 2


def test_page_fanout_accounts_for_each_model_and_utf8_copy():
    revision = CustomImportRootRevision(canonical_payload="é", payload_sha256=b"x" * 32)
    assert graph._model_bytes([revision, revision]) == 68
    graph._page_cost(_request(page_row_limit=2, page_byte_limit=68), [revision, revision])
    with pytest.raises(CandidateRunnerError, match="byte page"):
        graph._page_cost(_request(page_row_limit=2, page_byte_limit=67), [revision, revision])
    with pytest.raises(CandidateRunnerError, match="row page"):
        graph._page_cost(_request(page_row_limit=1), [revision, revision])
    with pytest.raises(CandidateRunnerError, match="row page"):
        graph._page_cost(_request(page_row_limit=2), [revision], reserved_rows=2)


def _read_session(monkeypatch, pages):
    session = SimpleNamespace(
        info={}, execute=Mock(side_effect=[SimpleNamespace(all=lambda part=part: part) for part in pages])
    )

    @contextmanager
    def _transaction(*_args):
        session.info["custom_import_build_read_deadline"] = 20
        yield None, 20

    monkeypatch.setattr(graph, "_read_transaction", _transaction)
    monkeypatch.setattr(graph, "_prepare_read", Mock())
    monkeypatch.setattr(graph.time, "monotonic", lambda: 10)
    return session


def test_read_pages_fetch_only_admitted_keys_and_resume(monkeypatch):
    session = _read_session(monkeypatch, [[(1, 3), (2, 3)], [("first",)], [(2, 3)], [("second",)], []])
    root = CustomImportRootRevision
    assert list(
        graph._read_rows(session, _request(page_byte_limit=5), 7, select(root), (root.root_revision_id,), (root,))
    ) == [("first",), ("second",)]
    queries = [call.args[0].compile(dialect=postgresql.dialect()) for call in session.execute.call_args_list]
    assert queries[0].params["param_1"] == 16
    assert "octet_length" in str(queries[0])
    assert list(queries[1].params.values()) == [[(1,)]]
    assert any(value == 1 for value in queries[2].params.values())


def test_one_read_page_stops_at_the_metadata_byte_boundary(monkeypatch):
    session = _read_session(monkeypatch, [[(1, 3), (2, 3)], [("first",)]])
    root = CustomImportRootRevision
    assert list(
        graph._read_rows(
            session,
            _request(page_byte_limit=5),
            7,
            select(root),
            (root.root_revision_id,),
            (root,),
            bounds=graph._ReadPage(single_page=True),
        )
    ) == [("first",)]
    assert session.execute.call_count == 2


def test_child_batch_limit_reserves_all_scalar_and_context_rows():
    request = _request(page_row_limit=256)
    registry = _registry(request.definition)
    plan = CustomImportBuildFamily(selection_kind="source")
    child_values_by_field = dict(rate_npi="1234567893", service_code="A100", amount=Decimal("12.50"))
    child = _revision(request.definition, "rates", child_values_by_field)
    family = CustomImportFamilyRevision(
        family_revision_id=7, entity_binding_id=8, root_record_id=9, root_revision_id=10, family_sha256=b"f" * 32
    )
    projections = graph._child_models(request, registry, 6, family, _root(), child, child_values_by_field, "rates")
    assert graph._child_page_limit(request, registry, plan) == 256 // (len(projections) + 2)
    plan.selection_kind = "retained"
    assert graph._child_page_limit(request, registry, plan) == 1


@pytest.mark.parametrize("driver", ["asyncpg", "psycopg"])
@pytest.mark.parametrize(
    "message,sqlstate,expected",
    [
        ("custom_import_build_page_too_large", "P0001", True),
        ("custom_import_build_page_too_large", "57014", False),
        ("custom_import_build_page_too_large", None, False),
        ("custom_import_build_structure_mismatch", "P0001", False),
        ("prefix custom_import_build_page_too_large", "P0001", False),
        (None, "P0001", False),
    ],
)
def test_child_batch_retries_only_the_exact_database_bound(driver, message, sqlstate, expected):
    driver_error = RuntimeError(message)
    driver_error.sqlstate = sqlstate
    if driver == "asyncpg":
        driver_error.__cause__ = RuntimeError(message)
        driver_error.__cause__.message = message
    else:
        driver_error.diag = SimpleNamespace(message_primary=message)
    assert graph._is_child_page_bound_error(DBAPIError(None, None, driver_error)) is expected
    assert not graph._is_child_page_bound_error(CandidateRunnerError("build child payload digest differs"))


@pytest.mark.parametrize(
    "child_count,message,retry",
    [
        (4, "record projection fanout exceeds the admitted row page", True),
        (4, "record projection fanout exceeds the admitted byte page", True),
        (1, "record projection fanout exceeds the admitted row page", False),
        (4, "build child payload digest differs", False),
        (4, "prefix record projection fanout exceeds the admitted row page", False),
    ],
)
async def test_child_batch_retry_preserves_the_cursor_and_exact_failure(monkeypatch, child_count, message, retry):
    request = _request()
    family_input = SimpleNamespace(plan=CustomImportBuildFamily(selection_kind="source"))
    current = SimpleNamespace(attached_child_count=0, complete_at=None)
    finished = SimpleNamespace(complete_at=dt.datetime(2030, 1, 1, tzinfo=dt.UTC))
    child_revisions = [object() for _ in range(child_count)]
    reads = Mock(side_effect=[(current, child_revisions), (current, child_revisions[:2]), (finished, [])])
    failure = CandidateRunnerError(message)
    append = AsyncMock(side_effect=[failure, None])
    heartbeat = AsyncMock()
    session = SimpleNamespace(run_sync=AsyncMock(side_effect=lambda callback: callback(None)))
    monkeypatch.setattr(graph, "_session", lambda _factory: nullcontext(session))
    monkeypatch.setattr(graph, "_child_page_limit", Mock(return_value=4))
    monkeypatch.setattr(graph, "_next_children", reads)
    monkeypatch.setattr(graph, "_append_child_batch", append)
    monkeypatch.setattr(graph, "_heartbeat", heartbeat)
    if retry:
        await graph._append_children(None, request, None, family_input)
        assert [call.args[-1] for call in reads.call_args_list] == [4, 2, 2]
        assert all(call.args[-2] is current for call in append.await_args_list)
        assert [len(call.args[-1]) for call in append.await_args_list] == [4, 2]
        heartbeat.assert_awaited_once_with(None, request)
    else:
        with pytest.raises(CandidateRunnerError) as caught:
            await graph._append_children(None, request, None, family_input)
        assert caught.value is failure
        assert reads.call_count == append.await_count == 1
        heartbeat.assert_not_awaited()


def _retry_page_error(database_error):
    """Build an admitted page-limit error, retaining a normal driver cause chain."""
    if not database_error:
        return CandidateRunnerError("record projection fanout exceeds the admitted row page")
    driver_error = RuntimeError("custom_import_build_page_too_large")
    driver_error.sqlstate = "P0001"
    driver_error.__cause__ = RuntimeError("custom_import_build_page_too_large")
    driver_error.__cause__.message = "custom_import_build_page_too_large"
    primary = DBAPIError(None, None, driver_error)
    primary.__cause__ = driver_error
    return primary


def _owned_cleanup(cleanup_stage, cancelled):
    """Control one real page-owned exit without replacing its cleanup machinery."""
    cleanup = SimpleNamespace(
        error=RuntimeError("synthetic cleanup failure"), entered=asyncio.Event(), release=asyncio.Event()
    )

    async def exit_owned(*_args):
        if cancelled:
            cleanup.entered.set()
            await cleanup.release.wait()
        else:
            raise cleanup.error

    cleanup.transaction_exit = AsyncMock(side_effect=exit_owned if cleanup_stage == "transaction" else None)
    cleanup.session_exit = AsyncMock(side_effect=exit_owned if cleanup_stage == "session" else None)
    cleanup.factory = _session_contexts(cleanup.transaction_exit, cleanup.session_exit)
    return cleanup


@pytest.mark.parametrize(
    "cleanup_stage,cancelled",
    [(None, False), ("transaction", False), ("session", False), ("transaction", True), ("session", True)],
)
@pytest.mark.parametrize("database_error", [False, True])
async def test_child_batch_retry_requires_uninterrupted_owned_cleanup(
    monkeypatch, cleanup_stage, cancelled, database_error
):
    """Only clean, uninterrupted page exits permit a smaller retry."""
    primary = _retry_page_error(database_error)
    original_cause = primary.__cause__
    cleanup = _owned_cleanup(cleanup_stage, cancelled)
    request = _request()
    family_input = SimpleNamespace(plan=CustomImportBuildFamily(build_id=7, selection_kind="source"))
    current = SimpleNamespace(attached_child_count=0, complete_at=None)
    finished = SimpleNamespace(complete_at=dt.datetime(2030, 1, 1, tzinfo=dt.UTC))
    child_revisions = [object() for _ in range(4)]
    reads = Mock(side_effect=[(current, child_revisions), (current, child_revisions[:2]), (finished, [])])
    read_session = SimpleNamespace(run_sync=AsyncMock(side_effect=lambda callback: callback(None)))

    async def append_page(_factory, _request, _registry, _family_input, _current, batch):
        if len(batch) == 4:
            async with graph._page_session(cleanup.factory, request, 7):
                raise primary

    append = AsyncMock(side_effect=append_page)
    heartbeat = AsyncMock()
    monkeypatch.setattr(staging, "_lock_page", AsyncMock())
    monkeypatch.setattr(graph, "_session", lambda _factory: nullcontext(read_session))
    monkeypatch.setattr(graph, "_child_page_limit", Mock(return_value=4))
    monkeypatch.setattr(graph, "_next_children", reads)
    monkeypatch.setattr(graph, "_append_child_batch", append)
    monkeypatch.setattr(graph, "_heartbeat", heartbeat)
    if cleanup_stage is not None:
        task = asyncio.create_task(graph._append_children(cleanup.factory, request, None, family_input))
        if cancelled:
            await cleanup.entered.wait()
            task.cancel("secondary cancellation")
            await asyncio.sleep(0)
            task.cancel("repeated cancellation")
            await asyncio.sleep(0)
            assert not task.done()
            cleanup.release.set()
        with pytest.raises(type(primary)) as caught:
            await task
        assert caught.value is primary
        assert primary.__cause__ is (original_cause if cancelled else cleanup.error)
        assert reads.call_count == append.await_count == 1
        heartbeat.assert_not_awaited()
    else:
        await graph._append_children(cleanup.factory, request, None, family_input)
        assert [call.args[-1] for call in reads.call_args_list] == [4, 2, 2]
        assert append.await_count == 2
        assert primary.__cause__ is original_cause
        heartbeat.assert_awaited_once_with(cleanup.factory, request)
    cleanup.transaction_exit.assert_awaited_once()
    cleanup.session_exit.assert_awaited_once()


@pytest.mark.parametrize("completed", [False, True])
async def test_child_batch_skips_progress_changed_after_the_read(monkeypatch, completed):
    plan = CustomImportBuildFamily(build_id=7, root_record_id=8)
    progress = SimpleNamespace(attached_child_count=0 if completed else 1, complete_at=object() if completed else None)
    session = SimpleNamespace(get=AsyncMock(return_value=progress))
    prepare = AsyncMock()
    monkeypatch.setattr(graph, "_page_session", lambda *_args: nullcontext((session, object())))
    monkeypatch.setattr(graph, "_prepare_statement", prepare)
    await graph._append_child_batch(
        None, _request(), None, SimpleNamespace(plan=plan), SimpleNamespace(attached_child_count=0), [object()]
    )
    session.get.assert_awaited_once_with(CustomImportBuildFamily, (7, 8), with_for_update=True)
    prepare.assert_awaited_once_with(session)


@pytest.mark.parametrize("completed,has_child", [(True, False), (False, False), (False, True)])
def test_child_page_preserves_completion_and_admitted_children(monkeypatch, completed, has_child):
    request = _request()
    current = CustomImportBuildFamily(
        build_id=7,
        root_record_id=8,
        selection_kind="source",
        complete_at=dt.datetime(2030, 1, 1, tzinfo=dt.UTC) if completed else None,
    )
    family_input = SimpleNamespace(plan=current, root=CustomImportRootRevision(), record=CustomImportRootRecord())
    child = object()
    reads = Mock(side_effect=lambda *_args, **_kwargs: ((child,) for _ in range(int(has_child))))
    monkeypatch.setattr(graph, "_one_row", Mock(return_value=(current,)))
    monkeypatch.setattr(graph, "_read_rows", reads)
    assert graph._next_children(None, request, _registry(request.definition), family_input, 3) == (
        current,
        [child] if has_child else [],
    )
    if completed:
        reads.assert_not_called()
    else:
        assert reads.call_count == len(request.definition.child_collections)
        assert all(call.kwargs["bounds"].row_limit == 3 for call in reads.call_args_list)
        assert all(call.kwargs["bounds"].single_page for call in reads.call_args_list)


@pytest.mark.parametrize("completed", [False, True])
async def test_exhausted_child_input_requires_sql_completion(monkeypatch, completed):
    plan = CustomImportBuildFamily(build_id=7, root_record_id=8, selection_kind="source")
    progress = SimpleNamespace(attached_child_count=0, complete_at=None, family_revision_id=9)
    family = SimpleNamespace(family_revision_id=9)
    session = SimpleNamespace(get=AsyncMock(side_effect=[progress, family]), add_all=Mock())
    commit = AsyncMock(return_value=SimpleNamespace(complete=completed))
    monkeypatch.setattr(graph, "_page_session", lambda *_args: nullcontext((session, object())))
    monkeypatch.setattr(graph, "_prepare_statement", AsyncMock())
    monkeypatch.setattr(graph, "_flush_page", AsyncMock())
    monkeypatch.setattr(graph, "_commit_family", commit)
    operation = graph._append_child_batch(None, _request(), None, SimpleNamespace(plan=plan), progress, [])
    if completed:
        await operation
    else:
        with pytest.raises(CandidateRunnerError, match="family input ended before SQL completion"):
            await operation
    commit.assert_awaited_once_with(session, 7, 8, 9)
    assert not any(list(call.args[0]) for call in session.add_all.call_args_list)


def test_oversize_metadata_prevents_payload_query(monkeypatch):
    session = _read_session(monkeypatch, [[(1, 100)]])
    root = CustomImportRootRevision
    with pytest.raises(CandidateRunnerError, match="byte page"):
        list(
            graph._read_rows(session, _request(page_byte_limit=20), 7, select(root), (root.root_revision_id,), (root,))
        )
    assert session.execute.call_count == 1


def test_projected_page_charges_only_explicit_variable_columns(monkeypatch):
    session = _read_session(monkeypatch, [[(1, 3)], [(1, "one")], []])
    root = CustomImportRootRevision
    rows = graph._read_rows(
        session,
        _request(page_byte_limit=3),
        7,
        select(root.root_revision_id, root.canonical_payload),
        (root.root_revision_id,),
        (root,),
        bounds=graph._ReadPage(variable_columns=(root.canonical_payload,)),
    )
    assert list(rows) == [(1, "one")]
    metadata = str(session.execute.call_args_list[0].args[0])
    assert "custom_import_root_revision.canonical_payload)" in metadata
    assert "payload_sha256" not in metadata and "producing_token_sha256" not in metadata


@pytest.mark.parametrize("metadata", [[], [(1, 1)]])
def test_zero_remaining_payload_budget_reads_only_empty_eof(monkeypatch, metadata):
    session = _read_session(monkeypatch, [metadata])
    root = CustomImportRootRevision
    rows = graph._read_rows(
        session,
        _request(page_byte_limit=20),
        7,
        select(root),
        (root.root_revision_id,),
        (root,),
        bounds=graph._ReadPage(reserve_bytes=20),
    )
    if metadata:
        with pytest.raises(CandidateRunnerError, match="byte page"):
            list(rows)
    else:
        assert list(rows) == []
    assert session.execute.call_count == 1


def test_consumer_work_must_fit_latest_bounded_read_window(monkeypatch):
    session = _read_session(monkeypatch, [[(1, 3)], [("first",)]])
    root = CustomImportRootRevision
    iterator = graph._read_rows(session, _request(), 7, select(root), (root.root_revision_id,), (root,))
    assert next(iterator) == ("first",)
    monkeypatch.setattr(graph.time, "monotonic", lambda: 21)
    with pytest.raises(LeaseAuthorityLost):
        next(iterator)


def test_nested_pages_can_refresh_the_read_window(monkeypatch):
    session = _read_session(monkeypatch, [[(1, 3)], [("first",)], []])
    root = CustomImportRootRevision
    iterator = graph._read_rows(session, _request(), 7, select(root), (root.root_revision_id,), (root,))
    assert next(iterator) == ("first",)
    session.info["custom_import_build_read_deadline"] = 30
    monkeypatch.setattr(graph.time, "monotonic", lambda: 21)
    assert next(iterator, None) is None


def test_family_retry_traverses_completed_keys_without_a_filter_scan(monkeypatch):
    observation_by_key = {}
    chosen = object()

    def _plans(_session, _request, _build, statement, _keys, _models, *, after):
        observation_by_key.update(after=after, query=str(statement))
        yield 11, dt.datetime(2030, 1, 1, tzinfo=dt.UTC)
        yield 12, None
        raise AssertionError("the current family must complete before advancing")

    family_input = Mock(return_value=chosen)
    monkeypatch.setattr(graph, "_read_rows", _plans)
    monkeypatch.setattr(graph, "_family_input", family_input)
    assert graph._next_family_input(None, _request(), None, 7, 10) is chosen
    assert observation_by_key["after"] == (10,)
    assert "IS NULL" not in observation_by_key["query"]
    assert family_input.call_args.args[-1] == 12


def test_retained_copy_order_uses_membership_not_typed_hash():
    plan = CustomImportBuildFamily(build_id=7, root_record_id=8, selection_kind="retained", base_family_revision_id=6)
    statement, keys = graph._child_statement(plan, _request().definition)
    assert [column.key for column in keys] == ["collection_slot", "child_revision_id"]
    assert "custom_import_family_child" in str(statement)
    statement, keys = graph._child_statement(plan, _request().definition, canonical=True)
    assert [column.key for column in keys] == ["collection_slot", "child_key_sha256", "child_revision_id"]
    assert statement.compile().params["origin_1"] == "retained"


def test_source_append_cursor_uses_collection_name_not_slot():
    registry = CandidateRegistry({"zebra": 1, "details": 3, "other": 2}, {}, 1)
    plan = CustomImportBuildFamily(
        build_id=7,
        root_record_id=8,
        selection_kind="source",
        last_child_collection_slot=3,
        last_child_key_sha256=b"c" * 32,
        last_input_child_revision_id=9,
    )
    assert list(graph._child_ranges(registry, plan)) == [(3, (3, b"c" * 32, 9)), (2, None), (1, None)]
    plan.last_child_collection_slot = 2
    assert list(graph._child_ranges(registry, plan)) == [(2, (2, b"c" * 32, 9)), (1, None)]
    plan.selection_kind = "retained"
    assert list(graph._child_ranges(registry, plan)) == [(None, (2, 9))]


def test_source_family_hash_uses_names_despite_reversed_collection_slots(monkeypatch):
    document = json.loads(_shared_definition(interleaved=True).canonical)
    document["schema"]["children"].reverse()
    definition = CustomImportDefinition.from_mapping(document)
    registry = _registry(definition)
    request = _request(definition=definition)
    root_values_by_field = dict(npi="1003000126", score=Decimal("1"), enabled=True)
    children_by_collection = {
        "other": [dict(other_npi="1003000126", other_id="z", other_amount=Decimal("4"))],
        "details": [dict(detail_npi="1003000126", detail_id="a", amount=Decimal("2"))],
    }
    children_by_slot = {
        registry.child_collection_slots[name]: _revision(
            definition, name, children[0], collection_slot=registry.child_collection_slots[name]
        )
        for name, children in children_by_collection.items()
    }
    visited_slots = []

    def _children(_session, _request, _build, statement, *_args, **_kwargs):
        slot = statement.compile().params["collection_slot_1"]
        visited_slots.append(slot)
        yield (children_by_slot[slot],)

    monkeypatch.setattr(graph, "_read_rows", _children)
    root = CustomImportRootRevision(canonical_payload=record_payload(definition.root_fields, root_values_by_field))
    plan = CustomImportBuildFamily(build_id=7, root_record_id=8, selection_kind="source")
    digest, count = graph._source_family_digest(
        None, request, registry, plan, root, CustomImportRootRecord(), root_values_by_field
    )
    assembled = assemble_root_families(definition, [root_values_by_field], children_by_collection)
    assert digest == new_family_hash(definition, assembled.families[0])
    assert count == 2
    assert visited_slots == [registry.child_collection_slots[name] for name in sorted(children_by_collection)]


def test_copy_models_preserve_payload_but_use_local_pack_position():
    request = _request()
    original = CustomImportRootRevision(
        root_revision_id=12, canonical_payload="synthetic-payload", payload_sha256=b"p" * 32, source_ordinal=89
    )
    plan = CustomImportBuildFamily(build_id=7, root_record_id=8, base_family_revision_id=6, selection_kind="retained")
    pack = CustomImportPack(pack_id=10, stream_slot=1)
    copied = graph._copy_record(request, SimpleNamespace(plan=plan), original, pack, None)
    copied.root_revision_id = 13
    occurrence = graph._copy_occurrence(SimpleNamespace(build_id=7), plan, original, copied, pack, None)
    assert copied.source_ordinal == 0 and copied.pack_id == 10
    assert copied.canonical_payload == original.canonical_payload
    assert occurrence.base_root_revision_id == 12 and occurrence.root_revision_id == 13
    assert occurrence.source_ordinal is None and occurrence.origin == "retained"


@pytest.mark.parametrize("uses_child_context", [False, True])
def test_projection_contexts_follow_declared_profile_scope(uses_child_context):
    document = _raw_definition()
    if not uses_child_context:
        for profile in document["selection_profiles"]:
            profile["selection"] = [{"field": "display_name", "direction": "asc", "nulls": "last"}]
            profile["context_dimensions"] = []
    request = _request(definition=CustomImportDefinition.from_mapping(document))
    registry = _registry(request.definition)
    child_values_by_field = dict(rate_npi="1234567893", service_code="A100", amount=Decimal("12.50"))
    child = _revision(request.definition, "rates", child_values_by_field)
    family = CustomImportFamilyRevision(
        family_revision_id=7, entity_binding_id=8, root_record_id=9, root_revision_id=10, family_sha256=b"f" * 32
    )
    projections = graph._child_models(request, registry, 6, family, _root(), child, child_values_by_field, "rates")
    contexts = [model for model in projections if isinstance(model, CustomImportBuildCandidateContext)]
    profile_count = len(request.definition.selection_profiles)
    assert len(contexts) == (profile_count if uses_child_context else 0)
    assert all(context.context_child_revision_id == child.child_revision_id for context in contexts)
    assert any(isinstance(model, CustomImportFamilyChild) for model in projections)
    assert {model.field_slot for model in projections if isinstance(model, CustomImportChildScalar)} == {
        field.field_slot for field in request.definition.child_fields if field.projection_slot is not None
    }
    root_models = graph._root_models(request, registry, 6, family, _root())
    root_contexts = [model for model in root_models if isinstance(model, CustomImportBuildCandidateContext)]
    assert len(root_contexts) == (0 if uses_child_context else profile_count)
    assert all(context.context_child_revision_id is None for context in root_contexts)


def test_read_statement_budget_is_strictly_inside_remaining_window(monkeypatch):
    session = SimpleNamespace(execute=Mock())
    monkeypatch.setattr(graph.time, "monotonic", lambda: 10)
    graph._prepare_read(session, _request(), 10.020)
    timeout = next(
        value
        for value in session.execute.call_args.args[0].compile().params.values()
        if type(value) is str and value.isdigit()
    )
    assert 0 < int(timeout) < 20
    with pytest.raises(LeaseAuthorityLost):
        graph._prepare_read(session, _request(), 10.001)


@pytest.mark.asyncio
async def test_scan_renewal_preserves_the_primary_error():
    primary = CandidateRunnerError("synthetic page failure")
    with pytest.raises(CandidateRunnerError) as failure:
        async with graph._renew_while_reading(None, _request()):
            raise primary
    assert failure.value is primary


def _session_contexts(transaction_exit, session_exit):
    transaction = SimpleNamespace(__aenter__=AsyncMock(), __aexit__=transaction_exit)
    session = SimpleNamespace(begin=lambda: transaction)
    context = SimpleNamespace(__aenter__=AsyncMock(return_value=session), __aexit__=session_exit)
    return lambda: context


@pytest.mark.asyncio
async def test_terminal_context_drains_repeated_cancellation():
    entered, release = asyncio.Event(), asyncio.Event()
    primary = CandidateRunnerError("first body error")

    async def _transaction_exit(*_args):
        entered.set()
        await release.wait()
        raise RuntimeError("late rollback failure")

    session_exit = AsyncMock(side_effect=RuntimeError("late session close failure"))
    factory = _session_contexts(_transaction_exit, session_exit)

    async def _operation():
        async with graph._session(factory, transaction=True):
            raise primary

    task = asyncio.create_task(_operation())
    await entered.wait()
    task.cancel("second")
    await asyncio.sleep(0)
    task.cancel("third")
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(CandidateRunnerError) as failure:
        await task
    assert failure.value is primary
    session_exit.assert_awaited_once()


@pytest.mark.asyncio
async def test_terminal_context_keeps_first_commit_error():
    primary = ConnectionError("commit acknowledgement lost")
    commit_exit = AsyncMock(side_effect=primary)
    session_exit = AsyncMock(side_effect=RuntimeError("session close failed"))
    with pytest.raises(ConnectionError) as failure:
        async with graph._session(_session_contexts(commit_exit, session_exit), transaction=True) as session:
            assert session.begin is not None
    assert failure.value is primary
    assert isinstance(primary.__cause__, RuntimeError)
    session_exit.assert_awaited_once()
