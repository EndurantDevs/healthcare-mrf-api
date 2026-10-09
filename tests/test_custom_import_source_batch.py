# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Trusted SOURCE bounds, exact cursor reconciliation, EOF and rollback."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import math
import time
import weakref
from contextlib import asynccontextmanager
from copy import copy
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from sqlalchemy.dialects.postgresql import dialect

from process.custom_import import build_source as staging
from process.custom_import import source_batch as step
from process.custom_import.execution import lease_token_sha256
from process.custom_import.processing_policy import BuildPolicy, ProcessingPolicy
from process.custom_import.runner_types import CandidateRunnerError, LeaseAuthorityLost
from tests.test_custom_import_build_source import _child, _nullable_integer_part, _request, _root
from tests.test_custom_import_build_source_bulk import _bulk_context, _prepared_page
from tests.test_custom_import_snowflake_capture import _policy
from tests.test_custom_import_source_finalize_sql import _page as _finalizer_page

_EXPIRY = dt.datetime(2030, 1, 1, tzinfo=dt.UTC)
_CURSOR = step.SourceCursor(1, 0, 0, 0)


def _permit(**changes):
    return step.SourcePermit(
        **(
            dict(
                audience="custom-import-engine",
                contract=step.PERMIT_CONTRACT,
                dataset_id=1,
                definition_revision_id=2,
                schema_revision_id=3,
                source_binding_revision_id=5,
                source_binding_sha256="b" * 64,
                idempotency_key="synthetic-request",
                issued_at=_EXPIRY - dt.timedelta(hours=1),
                expires_at=_EXPIRY,
                issuer="custom-import-execution-controller",
                method="POST",
                origin="https://writer.example.invalid",
                path=step.SOURCE_PATH,
            )
            | changes
        )
    )


_PERMIT = _permit()


@pytest.mark.parametrize(
    "position,bad", [(0, True), (0, 0), (0, 2**31), (1, -1), (1, 2**63), (2, 0.0), (3, None), (3, 2**31)]
)
def test_cursor_requires_all_exact_native_types(position, bad):
    values = [1, 0, 0, 0]
    values[position] = bad
    with pytest.raises(ValueError):
        step.SourceCursor(*values)


def _retained_build(request):
    """Synthetic persisted build with the same immutable original request."""
    build = SimpleNamespace(
        build_id=11,
        capture_bundle_id=9,
        **{
            name: getattr(request, name)
            for name in (
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
                "execution_id",
                "page_row_limit",
                "page_byte_limit",
                "statement_timeout_ms",
                "build_deadline_at",
            )
        },
        producing_fence=1,
        producing_token_sha256=lease_token_sha256(request.lease_token),
        request_identity_sha256=b"r" * 32,
        base_generation_id=None,
        base_pointer_version=0,
        complete_scope=False,
        refresh_mode="upsert",
    )
    return build


def _retained(monkeypatch):
    request = _request()
    policy = ProcessingPolicy(
        _policy(),
        30,
        BuildPolicy(
            request.page_row_limit, request.page_byte_limit, request.statement_timeout_ms, request.lease_seconds, 120
        ),
    )
    build = _retained_build(request)
    execution = SimpleNamespace(
        execution_id=4,
        dataset_id=1,
        definition_revision_id=2,
        schema_revision_id=3,
        capture_bundle_id=9,
        source_binding_revision_id=5,
        request_identity_sha256=b"r" * 32,
    )
    bundle = SimpleNamespace(
        dataset_id=1,
        definition_revision_id=2,
        schema_revision_id=3,
        capture_state="sealed",
        payload_contract="custom-import/parquet-parts/v2",
        source_binding_revision_id=5,
        source_binding_sha256=b"b" * 32,
        request_identity_sha256=b"r" * 32,
        canonical_policy=policy.capture.canonical,
        policy_sha256=bytes.fromhex(policy.capture.digest),
    )
    loaded = SimpleNamespace(
        dataset_id=1,
        schema_revision_id=3,
        source_binding_revision_id=5,
        source_binding_sha256=b"b" * 32,
        definition=request.definition,
        binding=SimpleNamespace(processing_policy=policy),
    )
    by_model = {
        step.CustomImportBuildAttempt: build,
        step.CustomImportExecution: execution,
        step.CustomImportCaptureBundle: bundle,
    }
    session = SimpleNamespace(get=AsyncMock(side_effect=lambda model, identifier: by_model[model]))
    monkeypatch.setattr(step, "load_snowflake_source_binding", AsyncMock(return_value=loaded))
    build.refresh_mode = request.definition.refresh_mode
    return session, request, build, execution, bundle, loaded


async def test_request_is_derived_from_persisted_definition_binding_and_bounds(monkeypatch):
    session, expected, _, _, _, loaded = _retained(monkeypatch)
    request, policy, bundle_id = await step._load_request(session, 4, 11, 1, expected.lease_token, _PERMIT)
    assert request == expected and policy is loaded.binding.processing_policy.capture and bundle_id == 9
    assert request.authorization_expires_at == _PERMIT.expires_at
    step.load_snowflake_source_binding.assert_awaited_once_with(
        session, definition_revision_id=2, source_binding_revision_id=5
    )


def _context_case(monkeypatch):
    read, request, build, _, _, loaded = _retained(monkeypatch)
    read.execute = AsyncMock()
    build.phase = "source"
    stream = SimpleNamespace(**asdict(_CURSOR), replay_verified_at=None)
    locked = SimpleNamespace(
        execute=AsyncMock(),
        scalars=AsyncMock(return_value=SimpleNamespace(one=lambda: stream)),
        info={staging._WINDOW: staging._PageWindow(60, 1000)},
    )
    registry, events = _bulk_context().registry, []

    @asynccontextmanager
    async def read_scope(_factory):
        try:
            yield read
        finally:
            events.append("read-closed")

    @asynccontextmanager
    async def page_scope(_factory, actual, build_id):
        assert events == ["read-closed"] and build_id == 11
        assert actual == replace(request, authorization_expires_at=_PERMIT.expires_at)
        try:
            yield locked, build
        finally:
            events.append("page-closed")

    monkeypatch.setattr(step, "_read_session", read_scope)
    monkeypatch.setattr(staging, "_page_session", page_scope)
    monkeypatch.setattr(staging, "_prepare_statement", AsyncMock())
    monkeypatch.setattr(step, "_verify_source_permit", AsyncMock())
    monkeypatch.setattr(step, "load_registry", AsyncMock(return_value=registry))
    return SimpleNamespace(
        read=read,
        locked=locked,
        request=request,
        build=build,
        stream=stream,
        registry=registry,
        policy=loaded.binding.processing_policy.capture,
        events=events,
    )


@pytest.mark.parametrize("slot,complete", [(1, False), (2, True)])
async def test_context_uses_fresh_locked_cursor_and_declared_stream(monkeypatch, slot, complete):
    case = _context_case(monkeypatch)
    case.stream.replay_verified_at = _EXPIRY if complete else None
    context, policy, bundle_id, deadline, actual_complete = await step._load_context(
        None, 4, 11, 1, slot, _CURSOR, case.request.lease_token, _PERMIT
    )
    assert context.registry is case.registry and context.stream == case.request.definition.source_streams[slot - 1]
    assert context.stream_slot == slot and context.build_id == 11
    assert policy is case.policy and (bundle_id, deadline, actual_complete) == (9, 60, complete)
    assert "statement_timeout" in str(case.read.execute.await_args.args[0])
    step._verify_source_permit.assert_awaited_once_with(case.locked, context.request, _PERMIT)
    statement = case.locked.scalars.await_args.args[0]
    assert "FOR UPDATE" in str(statement) and statement.get_execution_options()["populate_existing"]
    assert statement.compile(dialect=dialect()).params == {"build_id_1": 11, "stream_slot_1": slot}
    assert case.events == ["read-closed", "page-closed"]


@pytest.mark.parametrize("fault", ["cursor", "phase", "undeclared"])
async def test_context_rejects_changed_cursor_phase_or_undeclared_stream(monkeypatch, fault):
    case = _context_case(monkeypatch)
    if fault == "cursor":
        case.stream.next_source_ordinal = 1
    elif fault == "phase":
        case.build.phase = "admission"
    error = CandidateRunnerError if fault == "undeclared" else step.SourceCursorConflict
    with pytest.raises(error):
        await step._load_context(
            None, 4, 11, 1, 3 if fault == "undeclared" else 1, _CURSOR, case.request.lease_token, _PERMIT
        )
    assert case.events == ["read-closed", "page-closed"]
    step._verify_source_permit.assert_awaited_once()


@pytest.mark.parametrize(
    "owner,attribute,invalid",
    [
        ("build", "execution_id", 99),
        ("build", "producing_fence", 2),
        ("build", "request_identity_sha256", b"x" * 32),
        ("build", "page_row_limit", 3),
        ("bundle", "source_binding_revision_id", 6),
        ("bundle", "source_binding_sha256", b"x" * 32),
        ("bundle", "schema_revision_id", 8),
        ("bundle", "request_identity_sha256", b"x" * 32),
        ("bundle", "canonical_policy", "{}"),
        ("bundle", "policy_sha256", b"x" * 32),
        ("bundle", "capture_state", "pending"),
        ("loaded", "dataset_id", 8),
    ],
)
async def test_retained_identity_mismatch_fails_before_read_or_copy(monkeypatch, owner, attribute, invalid):
    session, request, build, execution, bundle, loaded = _retained(monkeypatch)
    setattr(dict(build=build, execution=execution, bundle=bundle, loaded=loaded)[owner], attribute, invalid)
    with pytest.raises(CandidateRunnerError):
        await step._load_request(session, 4, 11, 1, request.lease_token, _PERMIT)


@pytest.mark.parametrize("is_owner", [True, False])
async def test_owner_gate_uses_exact_signatures_and_quoted_mapped_schema(monkeypatch, is_owner):
    monkeypatch.setattr(staging, "_prepare_statement", AsyncMock())
    connection = SimpleNamespace(
        dialect=dialect(),
        sync_connection=SimpleNamespace(
            get_execution_options=lambda: {
                "schema_translate_map": {step.CustomImportBuildAttempt.__table__.schema: 'synthetic"control'}
            }
        ),
    )
    session = SimpleNamespace(
        connection=AsyncMock(return_value=connection),
        execute=AsyncMock(return_value=SimpleNamespace(scalar_one=lambda: is_owner)),
    )
    if is_owner:
        await step._require_owner(session)
    else:
        with pytest.raises(RuntimeError, match="entry-point owners"):
            await step._require_owner(session)
    query, parameters = session.execute.await_args.args
    assert "=CURRENT_USER" in str(query) and "pg_catalog.count(*)=2" in str(query)
    assert parameters["authorize"].startswith('"synthetic""control".source_bulk_authorize(')
    assert parameters["finalize"].endswith("source_set_finalize(pg_catalog.uuid,pg_catalog.int4[])")


@pytest.mark.parametrize(
    "attribute,invalid",
    [
        ("contract", "custom-import-admission-permit/v1"),
        ("path", "/control/v1/custom-import/admission-batch"),
        ("method", "GET"),
        ("dataset_id", 99),
        ("definition_revision_id", 99),
        ("schema_revision_id", 99),
    ],
)
async def test_locked_permit_purpose_and_scope_mismatch_never_reaches_owner(monkeypatch, attribute, invalid):
    owner = AsyncMock()
    monkeypatch.setattr(step, "_require_owner", owner)
    session = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(RuntimeError, match="authority_mismatch"):
        await step._verify_source_permit(
            session, _request(authorization_expires_at=_EXPIRY), _permit(**{attribute: invalid})
        )
    session.execute.assert_not_awaited()
    owner.assert_not_awaited()


@pytest.mark.parametrize("matched", [4, None])
async def test_locked_permit_query_binds_retained_idempotency_and_exact_binding(monkeypatch, matched):
    monkeypatch.setattr(staging, "_prepare_statement", AsyncMock())
    owner = AsyncMock()
    monkeypatch.setattr(step, "_require_owner", owner)
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(scalar_one_or_none=lambda: matched)))
    if matched is None:
        with pytest.raises(RuntimeError, match="authority_mismatch"):
            await step._verify_source_permit(session, _request(authorization_expires_at=_EXPIRY), _PERMIT)
        owner.assert_not_awaited()
    else:
        await step._verify_source_permit(session, _request(authorization_expires_at=_EXPIRY), _PERMIT)
        owner.assert_awaited_once_with(session)
    statement = session.execute.await_args.args[0]
    compiled = statement.compile(dialect=dialect())
    assert "idempotency_key" in str(compiled) and "binding_sha256" in str(compiled)
    assert "synthetic-request" in compiled.params.values() and bytes.fromhex("b" * 64) in compiled.params.values()
    assert str(statement).count("custom_import_source_binding_revision") >= 5


@pytest.mark.parametrize("expiry", [None, _EXPIRY - dt.timedelta(seconds=1)])
async def test_permit_expiry_must_be_the_shared_request_budget(monkeypatch, expiry):
    owner = AsyncMock()
    monkeypatch.setattr(step, "_require_owner", owner)
    session = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(RuntimeError, match="authority_mismatch"):
        await step._verify_source_permit(session, _request(authorization_expires_at=expiry), _PERMIT)
    session.execute.assert_not_awaited()
    owner.assert_not_awaited()


@pytest.mark.parametrize("operation", ["initial", "renew"])
async def test_shared_initial_and_renewed_windows_cap_at_permit_expiry(monkeypatch, operation):
    now = _EXPIRY - dt.timedelta(seconds=10)
    request = _request(authorization_expires_at=_EXPIRY, build_deadline_at=_EXPIRY + dt.timedelta(hours=1))
    lease = SimpleNamespace(state="running", expires_at=now + dt.timedelta(minutes=5))
    result = SimpleNamespace(one=lambda: (lease.expires_at, now, "read committed"), scalar_one=lambda: now)
    session = SimpleNamespace(info={}, execute=AsyncMock(return_value=result))
    monkeypatch.setattr(staging.time, "monotonic", lambda: 20.0)
    monkeypatch.setattr(staging, "_set_timeout", AsyncMock())
    monkeypatch.setattr(staging, "_prepare_statement", AsyncMock())
    monkeypatch.setattr(staging, "heartbeat_execution", AsyncMock(return_value=lease))
    if operation == "initial":
        await staging._initial_page_window(session, request)
    else:
        await staging._renew_page_window(session, request)
    assert session.info[staging._WINDOW].deadline == 30.0


async def test_nullable_landing_and_replay_allocation_are_independently_bounded(monkeypatch):
    definition, part = _nullable_integer_part()
    context = replace(_bulk_context(), request=_request(definition=definition))
    context = replace(context, stream=definition.source_streams[0])
    pages, has_eof = await step._prepare_part(None, context, part, _policy(), _CURSOR, _PERMIT, time.monotonic() + 60)
    assert has_eof and len(pages) == 1 and part.arrow_byte_count == 23
    actual = staging._aggregate_parquet_arrow_bytes(part.capture, limits=_policy().part_limits, decoded_bytes=0)
    assert actual == 24
    with pytest.raises(CandidateRunnerError, match="Arrow bytes"):
        await step._prepare_part(
            None, context, part, _policy(maximum_part_arrow_bytes=23), _CURSOR, _PERMIT, time.monotonic() + 60
        )


@pytest.mark.parametrize("cursor", [step.SourceCursor(1, 2, 2, 0), step.SourceCursor(1, 1, 0, 0)])
def test_retained_cursor_cannot_exceed_part_or_absolute_source_progress(cursor):
    definition, part = _nullable_integer_part()
    context = replace(_bulk_context(), request=_request(definition=definition), stream=definition.source_streams[0])
    with pytest.raises(CandidateRunnerError, match="cursor exceeds the retained part"):
        step._verify_part(context, part, _policy(), cursor)


@pytest.mark.parametrize("before_prefix", [False, True])
async def test_preparation_deadline_closes_iterator_without_returning_rows(monkeypatch, before_prefix):
    monkeypatch.setattr("process.custom_import.source_preparation.native_encoder", lambda: None)
    definition, part = _nullable_integer_part()
    context = replace(_bulk_context(), request=_request(definition=definition), stream=definition.source_streams[0])
    times = iter((60,) if before_prefix else (59, 60))
    closed = Mock(wraps=staging._close_iterator)
    monkeypatch.setattr(step, "time", SimpleNamespace(monotonic=lambda: next(times)))
    monkeypatch.setattr(staging, "_close_iterator", closed)
    with pytest.raises(LeaseAuthorityLost, match="read deadline elapsed") as caught:
        await step._prepare_part(None, context, part, _policy(), _CURSOR, _PERMIT, 60)
    assert closed.call_args_list[0].args[1] is caught.value
    assert all(call.args[0].gi_frame is None for call in closed.call_args_list)


@pytest.mark.parametrize("bound", ["rows", "bytes"])
async def test_late_part_cursor_compares_current_prefix_and_bounds_new_batch(monkeypatch, bound):
    context = _bulk_context()
    prepared = _prepared_page(context).records[0]
    old_count, part_count = 900_000, 1_000_000
    cursor = step.SourceCursor(19, old_count, old_count + 17, 4000)
    part = SimpleNamespace(ordinal=19, record_count=part_count, capture=object(), arrow_byte_count=100)
    close_events = []

    def pages(_context, _part, _policy, position):
        assert position == (19, old_count, 17)
        try:
            for first, stop in ((0, old_count), (old_count, part_count)):
                for offset in range(first, stop, 256):
                    yield staging._SourcePage(19, offset, offset + 17, (prepared,) * min(256, stop - offset))
        finally:
            close_events.append(True)

    monkeypatch.setattr(step, "_verify_part", lambda *_: None)
    monkeypatch.setattr(staging, "_source_pages", pages)
    clock = SimpleNamespace(elapsed=0.0)

    async def compare(*_args):
        clock.elapsed += 0.1 if bound == "rows" else 0

    monkeypatch.setattr(step, "time", SimpleNamespace(monotonic=lambda: clock.elapsed))
    compared = AsyncMock(side_effect=compare)
    monkeypatch.setattr(step, "_compare_prefix", compared)
    if bound == "bytes":
        monkeypatch.setattr(step, "MAX_BATCH_BYTES", prepared.byte_count * 1024)
        context = replace(context, request=replace(context.request, page_byte_limit=prepared.byte_count * 256))
    new_pages, has_eof = await step._prepare_part(None, context, part, _policy(), cursor, _PERMIT, 60)
    new_count = sum(len(page.records) for page in new_pages)
    assert 0 < new_count <= (100_000 if bound == "rows" else 1024) and not has_eof and close_events == [True]
    comparison_rows = 99_840 if bound == "rows" else 1024
    assert compared.await_count == math.ceil(old_count / comparison_rows)
    assert sum(len(call.args[2].records) for call in compared.await_args_list) == old_count
    assert all(len(call.args[2].records) <= comparison_rows for call in compared.await_args_list)
    assert all(
        sum(prepared_row.byte_count for prepared_row in call.args[2].records) <= step.MAX_BATCH_BYTES
        for call in compared.await_args_list
    )
    assert all(call.args[2].part_ordinal == 19 for call in compared.await_args_list)
    assert new_pages[0].first_row == old_count and new_pages[0].first_source == old_count + 17


def _stored_prefix_row(page, offset, stream):
    fingerprint = staging._prepared_fingerprint(page, offset)
    occurrence = SimpleNamespace(
        part_row_ordinal=fingerprint[0],
        source_ordinal=fingerprint[1],
        raw_parent_key_canonical=fingerprint[2],
        raw_parent_key_sha256=fingerprint[3],
        root_revision_id=None if stream else 1000 + offset,
        child_revision_id=1000 + offset if stream else None,
    )
    return (
        occurrence,
        fingerprint[4],
        None if stream else fingerprint[5],
        fingerprint[5] if stream else None,
        *fingerprint[6:],
    )


def _prefix_read_case(monkeypatch, stream=0):
    context = _bulk_context(stream)
    first = _prepared_page(context, [_child() if stream else _root()], part=19, row=5000, ordinal=7000)
    page = replace(first, records=first.records * 512)
    stored_rows = [_stored_prefix_row(page, offset, stream) for offset in range(len(page.records))]
    homes = [
        SimpleNamespace(revision_kind=stream + 1, revision_id=1000 + offset, family_id=17)
        for offset in range(len(page.records))
    ]
    session = SimpleNamespace(
        info={staging._WINDOW: staging._PageWindow(500, 1000)},
        execute=AsyncMock(return_value=SimpleNamespace(all=lambda: stored_rows)),
    )
    transactions, build = [], SimpleNamespace(phase="source")

    @asynccontextmanager
    async def page_session(_factory, request, build_id):
        assert request is context.request and build_id == context.build_id
        try:
            yield session, build
        except BaseException:
            transactions.append("rollback")
            raise
        else:
            transactions.append("commit")

    monkeypatch.setattr(staging, "_page_session", page_session)
    monkeypatch.setattr(staging, "_prepare_statement", AsyncMock())
    monkeypatch.setattr(staging, "_resolve_build_snapshot", AsyncMock(return_value=17))
    monkeypatch.setattr(staging, "_call", AsyncMock(return_value=SimpleNamespace(all=lambda: homes)))
    monkeypatch.setattr(step, "_verify_source_permit", AsyncMock())
    return SimpleNamespace(
        context=context,
        page=page,
        stored=stored_rows,
        homes=homes,
        session=session,
        transactions=transactions,
        build=build,
    )


@pytest.mark.parametrize("stream", [0, 1])
async def test_prefix_ranges_bind_identity(monkeypatch, stream):
    case = _prefix_read_case(monkeypatch, stream)
    await step._compare_prefix(None, case.context, case.page, _PERMIT, 60)
    assert case.transactions == ["commit"] and case.session.info[staging._WINDOW].deadline == 60
    step._verify_source_permit.assert_awaited_once_with(case.session, case.context.request, _PERMIT)
    query = case.session.execute.await_args.args[0]
    assert query._limit_clause.value == 512 > case.context.request.page_row_limit
    parameters = query.compile(dialect=dialect()).params
    assert parameters["build_id_1"] == case.context.build_id
    assert parameters["stream_slot_1"] == case.context.stream_slot
    assert parameters["source_part_ordinal_1"] == 19 and parameters["origin_1"] == "source"
    assert parameters["part_row_ordinal_1"] == 5000 and parameters["part_row_ordinal_2"] == 5512
    ids = list(range(1000, 1512))
    staging._call.assert_awaited_once_with(
        case.session,
        "lookup_custom_import_revision_home",
        (("bigint[]", [] if stream else ids), ("bigint[]", ids if stream else [])),
    )


@pytest.mark.parametrize("failure", ["phase", "missing_row", "payload", "position"])
async def test_prefix_mismatch_prevents_progress(monkeypatch, failure):
    case = _prefix_read_case(monkeypatch)
    if failure == "phase":
        case.build.phase = "admission"
    elif failure == "missing_row":
        case.stored.pop()
    elif failure == "payload":
        case.stored[0] = (*case.stored[0][:2], "{}", *case.stored[0][3:])
    else:
        case.stored[0][0].source_ordinal += 1
    with pytest.raises(CandidateRunnerError, match="committed source"):
        await step._compare_prefix(None, case.context, case.page, _PERMIT, 60)
    assert case.transactions == ["rollback"]
    staging._call.assert_not_awaited()


@pytest.mark.parametrize("failure", ["missing_home", "omitted_home", "wrong_family", "wrong_kind"])
async def test_prefix_home_requires_exact_identity(monkeypatch, failure):
    case = _prefix_read_case(monkeypatch)
    if failure == "omitted_home":
        case.homes.pop()
    elif failure == "wrong_kind":
        case.homes[0].revision_kind = 2
    else:
        case.homes[0].family_id = None if failure == "missing_home" else 18
    with pytest.raises(CandidateRunnerError, match="committed source"):
        await step._compare_prefix(None, case.context, case.page, _PERMIT, 60)
    assert case.transactions == ["rollback"]


@pytest.mark.parametrize("failure", ["empty", "rows", "bytes"])
async def test_prefix_bounds_precede_database_access(monkeypatch, failure):
    context = _bulk_context()
    page = _prepared_page(context)
    if failure == "bytes":
        page = replace(page, records=(replace(page.records[0], byte_count=step.MAX_BATCH_BYTES + 1),))
    else:
        page = replace(page, records=page.records * (0 if failure == "empty" else step.MAX_BATCH_ROWS + 1))
    monkeypatch.setattr(step, "_page", lambda *_: pytest.fail("invalid range must not access the database"))
    with pytest.raises(CandidateRunnerError, match="physical batch bounds"):
        await step._compare_prefix(None, context, page, _PERMIT, 60)


@pytest.mark.parametrize("field", ["part_ordinal", "first_row", "first_source"])
async def test_prefix_pages_require_contiguity(monkeypatch, field):
    context = _bulk_context()
    first = _prepared_page(context)
    second = replace(first, first_row=1, first_source=1)
    second = replace(second, **{field: getattr(second, field) + 1})
    compared = AsyncMock()
    monkeypatch.setattr(step, "_compare_prefix", compared)
    with pytest.raises(CandidateRunnerError, match="not contiguous"):
        await step._compare_prefix_batch(None, context, (first, second), _PERMIT, 60)
    compared.assert_not_awaited()


async def test_prefix_failure_closes_iterator(monkeypatch):
    context, closed = _bulk_context(), []
    first = _prepared_page(context)

    def pages(*_args):
        try:
            for offset in range(3):
                yield replace(first, first_row=offset, first_source=offset)
        finally:
            closed.append(True)

    compared = AsyncMock(side_effect=CandidateRunnerError("synthetic prefix mismatch"))
    monkeypatch.setattr(step, "_compare_prefix", compared)
    monkeypatch.setattr(step, "_verify_part", lambda *_: None)
    monkeypatch.setattr(staging, "_source_pages", pages)
    with pytest.raises(CandidateRunnerError, match="prefix mismatch"):
        await step._prepare_part(
            None, context, object(), _policy(), step.SourceCursor(1, 2, 2, 2), _PERMIT, time.monotonic() + 60
        )
    assert closed == [True] and compared.await_count == 1
    assert len(compared.await_args.args[2].records) == 2


@pytest.mark.parametrize("has_uncommitted_page", [False, True])
async def test_prefix_expiry_prevents_progress(monkeypatch, has_uncommitted_page):
    context, closed = _bulk_context(), []
    first = _prepared_page(context)
    clock = SimpleNamespace(elapsed=0.0)

    def pages(*_args):
        try:
            yield first
            if has_uncommitted_page:
                yield replace(first, first_row=1, first_source=1)
        finally:
            closed.append(True)

    async def compare(*_args):
        clock.elapsed = 60

    compared = AsyncMock(side_effect=compare)
    monkeypatch.setattr(step, "time", SimpleNamespace(monotonic=lambda: clock.elapsed))
    monkeypatch.setattr(step, "_compare_prefix", compared)
    monkeypatch.setattr(step, "_verify_part", lambda *_: None)
    monkeypatch.setattr(staging, "_source_pages", pages)
    with pytest.raises(LeaseAuthorityLost, match="deadline elapsed"):
        await step._prepare_part(None, context, object(), _policy(), step.SourceCursor(1, 1, 1, 1), _PERMIT, 60)
    assert closed == [True] and compared.await_count == 1


class _CommitHarness:
    def __init__(self, monkeypatch, *, final=False, other_complete=True, failure=None):
        self.context = _bulk_context()
        self.stream = SimpleNamespace(**asdict(_CURSOR), replay_verified_at=None)
        self.build = SimpleNamespace(phase="source")
        self.session = SimpleNamespace(
            scalars=AsyncMock(
                return_value=SimpleNamespace(
                    all=lambda: [self.stream, SimpleNamespace(replay_verified_at=_EXPIRY if other_complete else None)]
                )
            ),
            refresh=AsyncMock(),
        )
        self.events, self.landing = [], ()
        self.final, self.failure = final, failure
        monkeypatch.setattr(step, "_page", self.page)
        monkeypatch.setattr(step, "_cursor", AsyncMock(side_effect=lambda *_: self.stream))
        monkeypatch.setattr(staging, "_prepare_statement", AsyncMock())
        monkeypatch.setattr(staging, "_call", self.call)
        monkeypatch.setattr(staging, "_copy_source_landing", self.copy)
        monkeypatch.setattr(step, "finalize_source_batch", self.finalize)

    @asynccontextmanager
    async def page(self, *arguments):
        before, phase = copy(self.stream), self.build.phase
        self.events.append("begin")
        try:
            yield self.session, self.build
            self.events.append("fresh_authority")
            if self.failure == "authority":
                raise LeaseAuthorityLost("synthetic fresh fence")
        except BaseException:
            self.stream.__dict__.update(before.__dict__)
            self.build.phase = phase
            self.events.append("rollback")
            raise
        else:
            self.events.append("commit")

    async def call(self, session, name, arguments):
        assert session is self.session
        self.events.append(name)
        if name == "source_bulk_authorize":
            return SimpleNamespace(scalar_one=lambda: UUID(int=17))
        if name == "finish_custom_import_build_source_part":
            self.stream.next_part_ordinal += 1
            self.stream.next_part_row_ordinal = 0
            if self.final:
                self.stream.replay_verified_at = _EXPIRY
        elif name == "freeze_custom_import_build_source":
            self.build.phase = "admission"
        else:
            pytest.fail(f"unexpected dispatcher {name}")
        return SimpleNamespace(scalar_one=lambda: 1)

    async def copy(self, session, landing):
        assert session is self.session
        self.events.append("copy")
        self.landing = landing
        if self.failure == "copy":
            raise asyncio.CancelledError("synthetic copy cancel")

    async def finalize(self, session, batch_id, earlier_parts):
        assert session is self.session and batch_id == UUID(int=17)
        self.earlier_parts = earlier_parts
        self.events.append("ordinary_finalize")
        if self.failure == "finalize":
            raise RuntimeError("synthetic finalizer")
        self.stream.next_part_ordinal = self.landing[-1][4]
        self.stream.next_part_row_ordinal = self.landing[-1][5] + 1
        self.stream.next_source_ordinal += len(self.landing)
        self.stream.next_pack_ordinal += len({row[2] for row in self.landing})
        return len(self.landing)


@pytest.mark.parametrize("has_eof,is_final", [(False, False), (True, False), (True, True)])
async def test_same_transaction_copy_finalization_completion_and_receipt(monkeypatch, has_eof, is_final):
    harness = _CommitHarness(monkeypatch, final=is_final)
    receipt = await step._commit(
        None, harness.context, (_prepared_page(harness.context),), (1,) if has_eof else (), _CURSOR, 9, _PERMIT
    )
    assert receipt.rows_processed == 1 and receipt.after.next_source_ordinal == 1
    assert receipt.after.next_pack_ordinal == 1 and receipt.stream_complete is is_final
    assert receipt.phase == ("admission" if is_final else "source")
    assert harness.events[:4] == ["begin", "source_bulk_authorize", "copy", "ordinary_finalize"]
    assert harness.events[-2:] == ["fresh_authority", "commit"]
    assert receipt.before == _CURSOR and receipt.capture_bundle_id == 9


@pytest.mark.parametrize("is_final,other_complete", [(False, False), (True, False), (True, True)])
async def test_empty_part_requires_verified_eof_and_may_freeze_all_streams(monkeypatch, is_final, other_complete):
    harness = _CommitHarness(monkeypatch, final=is_final, other_complete=other_complete)
    receipt = await step._commit(None, harness.context, (), (1,), _CURSOR, 9, _PERMIT)
    assert receipt.rows_processed == 0 and receipt.after == step.SourceCursor(2, 0, 0, 0)
    assert receipt.stream_complete is is_final
    assert receipt.phase == ("admission" if is_final and other_complete else "source")
    assert "copy" not in harness.events and "ordinary_finalize" not in harness.events


async def test_interrupted_all_verified_unfrozen_source_can_freeze_exactly_once(monkeypatch):
    harness = _CommitHarness(monkeypatch)
    harness.stream.replay_verified_at = _EXPIRY
    receipt = await step._commit(None, harness.context, (), (), _CURSOR, 9, _PERMIT)
    assert receipt.before == receipt.after and receipt.phase == "admission" and receipt.stream_complete
    assert receipt.rows_processed == 0 and "copy" not in harness.events
    with pytest.raises(step.SourceCursorConflict) as caught:
        await step._commit(None, harness.context, (), (), _CURSOR, 9, _PERMIT)
    assert caught.value.phase == "admission" and caught.value.stream_complete


async def test_lost_response_uses_full_committed_cursor_not_blind_resend(monkeypatch):
    harness = _CommitHarness(monkeypatch)
    pages = (_prepared_page(harness.context),)
    receipt = await step._commit(None, harness.context, pages, (), _CURSOR, 9, _PERMIT)
    harness.events.clear()
    with pytest.raises(step.SourceCursorConflict) as caught:
        await step._commit(None, harness.context, pages, (), _CURSOR, 9, _PERMIT)
    assert caught.value.actual_cursor == receipt.after and not caught.value.stream_complete
    assert harness.events == ["begin", "rollback"]


@pytest.mark.parametrize(
    "failure,error", [("copy", asyncio.CancelledError), ("finalize", RuntimeError), ("authority", LeaseAuthorityLost)]
)
async def test_copy_finalize_or_fresh_authority_failure_rolls_back_all_progress(monkeypatch, failure, error):
    harness = _CommitHarness(monkeypatch, final=True, failure=failure)
    with pytest.raises(error):
        await step._commit(None, harness.context, (_prepared_page(harness.context),), (1,), _CURSOR, 9, _PERMIT)
    assert step.SourceCursor.from_model(harness.stream) == _CURSOR and harness.stream.replay_verified_at is None
    assert harness.build.phase == "source" and harness.events[-1] == "rollback"


async def test_unchanged_zero_row_nonterminal_is_not_an_ack(monkeypatch):
    harness = _CommitHarness(monkeypatch)
    with pytest.raises(step.SourceCursorConflict):
        await step._commit(None, harness.context, (), (), _CURSOR, 9, _PERMIT)
    assert harness.events == ["begin", "rollback"]


@pytest.mark.parametrize("completed", [0, True, 2])
async def test_finalizer_must_account_for_exact_attempted_rows(monkeypatch, completed):
    harness = _CommitHarness(monkeypatch)
    monkeypatch.setattr(step, "finalize_source_batch", AsyncMock(return_value=completed))
    with pytest.raises(RuntimeError, match="count differs"):
        await step._commit(None, harness.context, (_prepared_page(harness.context),), (), _CURSOR, 9, _PERMIT)
    assert harness.events[-1] == "rollback" and step.SourceCursor.from_model(harness.stream) == _CURSOR


async def test_finalizer_count_without_durable_progress_cannot_issue_receipt(monkeypatch):
    harness = _CommitHarness(monkeypatch)
    monkeypatch.setattr(step, "_copy_and_finalize", AsyncMock(return_value=1))
    with pytest.raises(RuntimeError, match="exact durable row progress"):
        await step._commit(None, harness.context, (_prepared_page(harness.context),), (), _CURSOR, 9, _PERMIT)
    assert harness.events == ["begin", "rollback"]


@pytest.mark.parametrize("with_rows", [False, True])
async def test_verified_stream_rejects_more_rows_or_completion(monkeypatch, with_rows):
    harness = _CommitHarness(monkeypatch)
    harness.stream.replay_verified_at = _EXPIRY
    pages = (_prepared_page(harness.context),) if with_rows else ()
    with pytest.raises(step.SourceCursorConflict) as caught:
        await step._commit(None, harness.context, pages, () if with_rows else (1,), _CURSOR, 9, _PERMIT)
    assert caught.value.stream_complete and caught.value.actual_cursor == _CURSOR
    assert harness.events == ["begin", "rollback"]


@pytest.mark.parametrize("expired_at", ["before", "after", None])
async def test_real_page_permit_budget_catalog_path_and_reset(monkeypatch, expired_at):
    session, _ = _finalizer_page(monkeypatch)
    session.info = {staging._WINDOW: staging._PageWindow(time.monotonic() + 120, 1000)}
    if expired_at == "before":
        monkeypatch.setattr(staging, "_lock_page", AsyncMock(side_effect=LeaseAuthorityLost("synthetic expiry")))
    now = _EXPIRY if expired_at == "after" else _EXPIRY - dt.timedelta(seconds=1)
    monkeypatch.setattr(staging, "verify_live_attempt", AsyncMock(return_value=now))
    monkeypatch.setattr(step, "_verify_source_permit", AsyncMock())
    try:
        async with step._page(
            lambda: session,
            _request(authorization_expires_at=_EXPIRY, build_deadline_at=_EXPIRY + dt.timedelta(days=1)),
            11,
            _PERMIT,
        ):
            assert session.search_path == "pg_catalog"
    except LeaseAuthorityLost:
        assert expired_at is not None
    else:
        assert expired_at is None
    assert session.events[-2:] == ["rollback" if expired_at else "commit", "close"]
    assert session.search_path == '"synthetic_untrusted", pg_catalog'


@pytest.mark.parametrize("field", list(step.SourceCursor.__dataclass_fields__))
async def test_every_cursor_coordinate_is_rechecked_before_authorization(monkeypatch, field):
    harness = _CommitHarness(monkeypatch)
    setattr(harness.stream, field, getattr(harness.stream, field) + 1)
    with pytest.raises(step.SourceCursorConflict):
        await step._commit(None, harness.context, (_prepared_page(harness.context),), (), _CURSOR, 9, _PERMIT)
    assert harness.events == ["begin", "rollback"]


def _read_flow(monkeypatch, *, fault=None, has_eof=True, is_final=True):
    events = []

    @asynccontextmanager
    async def owned_session():
        try:
            yield SimpleNamespace(begin=transaction)
        finally:
            events.append("session_closed")
            if fault == "session":
                raise RuntimeError("synthetic session close")

    @asynccontextmanager
    async def transaction():
        try:
            yield
        finally:
            events.append("read_transaction_closed")
            if fault == "transaction":
                raise RuntimeError("synthetic transaction close")

    @asynccontextmanager
    async def part_reader(*arguments, **keywords):
        assert keywords["part_ordinal"] in (1, 2) and keywords["stream_slot"] == 1
        yield SimpleNamespace(
            ordinal=keywords["part_ordinal"],
            receipt=SimpleNamespace(canonical_manifest='{"part_count":1}' if is_final else '{"part_count":2}'),
        )
        events.append("part_closed")
        if fault == "part":
            raise RuntimeError("synthetic part close")

    async def verify(*arguments, **keywords):
        assert events and events[-1] == "session_closed"
        events.append("full_metadata_verified")
        if fault == "metadata":
            raise RuntimeError("synthetic metadata drift")

    monkeypatch.setattr(staging, "_set_timeout", AsyncMock())
    monkeypatch.setattr(step, "open_segmented_cursor_part", part_reader)
    monkeypatch.setattr(step, "_prepare_part", AsyncMock(return_value=((), has_eof)))
    monkeypatch.setattr(step, "verify_segmented_stream_metadata", verify)
    return owned_session, events


@pytest.mark.parametrize("has_eof,is_final", [(False, True), (True, False), (True, True)])
async def test_actual_final_eof_checks_metadata_and_closes_every_reader(monkeypatch, has_eof, is_final):
    sessions, events = _read_flow(monkeypatch, has_eof=has_eof, is_final=is_final)
    result = await step._read_parts(sessions, _bulk_context(), _policy(), 9, _CURSOR, _PERMIT, time.monotonic() + 60)
    count = 2 if has_eof and not is_final else 1
    assert result == ((), tuple(range(1, count + 1)) if has_eof else ())
    assert events == ["part_closed", "read_transaction_closed", "session_closed"] * count + (
        ["full_metadata_verified", "read_transaction_closed", "session_closed"] if has_eof else []
    )


@pytest.mark.parametrize("fault", ["part", "metadata", "transaction", "session"])
async def test_failed_reader_or_metadata_cleanup_never_reaches_commit(monkeypatch, fault):
    sessions, events = _read_flow(monkeypatch, fault=fault)
    commit = AsyncMock()
    monkeypatch.setattr(step, "_commit", commit)
    monkeypatch.setattr(
        step, "_load_context", AsyncMock(return_value=(_bulk_context(), _policy(), 9, time.monotonic() + 60, False))
    )
    with pytest.raises(RuntimeError, match="synthetic"):
        await step.serve_source_batch(
            sessions,
            execution_id=4,
            build_id=11,
            fence=1,
            stream_slot=1,
            expected_cursor=_CURSOR,
            lease_token=_request().lease_token,
            source_permit=_PERMIT,
        )
    commit.assert_not_awaited()
    assert events[-1] == "session_closed"


async def test_completed_stream_recovery_does_not_reopen_prior_payloads(monkeypatch):
    context = _bulk_context()
    monkeypatch.setattr(
        step, "_load_context", AsyncMock(return_value=(context, _policy(), 9, time.monotonic() + 60, True))
    )
    reader, commit = AsyncMock(), AsyncMock(return_value="synthetic-receipt")
    monkeypatch.setattr(step, "_read_parts", reader)
    monkeypatch.setattr(step, "_commit", commit)
    result = await step.serve_source_batch(
        None,
        execution_id=4,
        build_id=11,
        fence=1,
        stream_slot=1,
        expected_cursor=_CURSOR,
        lease_token=_request().lease_token,
        source_permit=_PERMIT,
    )
    assert result == "synthetic-receipt"
    reader.assert_not_awaited()
    commit.assert_awaited_once_with(None, context, (), (), _CURSOR, 9, _PERMIT)


class _RetainedPart(SimpleNamespace):
    """Synthetic retained capture with observable lifetime after reader cleanup."""


def _coalesced_reader(monkeypatch, *, part_count=100, rows_per_part=1024, failure=None, part_refs=None):
    events = []
    context = _bulk_context()
    context = replace(context, request=replace(context.request, page_row_limit=256, page_byte_limit=8 * 1024**2))
    prepared = _prepared_page(context).records[0]

    @asynccontextmanager
    async def transaction():
        try:
            yield
        finally:
            events.append(("transaction_closed", None))

    @asynccontextmanager
    async def sessions():
        try:
            yield SimpleNamespace(begin=transaction)
        finally:
            events.append(("session_closed", None))

    @asynccontextmanager
    async def reader(*arguments, **keywords):
        ordinal = keywords["part_ordinal"]
        assert not events or events[-1][0] == "session_closed"
        if part_refs is not None:
            assert all(reference() is None for reference in part_refs)
        events.append(("open", ordinal))
        try:
            part = _RetainedPart(
                ordinal=ordinal,
                record_count=rows_per_part,
                capture=object(),
                receipt=SimpleNamespace(canonical_manifest=json.dumps({"part_count": part_count})),
            )
            if part_refs is not None:
                part_refs.append(weakref.ref(part))
            yield part
        finally:
            events.append(("close", ordinal))
            if failure == ordinal:
                raise RuntimeError("synthetic later part cleanup")

    def pages(_context, part, _policy, position):
        for offset in range(0, rows_per_part, 256):
            yield staging._SourcePage(
                part.ordinal, offset, position[2] + offset, (prepared,) * min(256, rows_per_part - offset)
            )

    monkeypatch.setattr(staging, "_set_timeout", AsyncMock())
    monkeypatch.setattr(staging, "_source_pages", pages)
    monkeypatch.setattr(step, "_verify_part", lambda *_: None)
    monkeypatch.setattr(step, "open_segmented_cursor_part", reader)
    monkeypatch.setattr(step, "verify_segmented_stream_metadata", AsyncMock())
    return sessions, context, events


async def test_many_small_parts_coalesce_to_one_bounded_copy_and_finalization(monkeypatch):
    harness = _CommitHarness(monkeypatch)
    part_refs = []
    sessions, context, events = _coalesced_reader(monkeypatch, part_refs=part_refs)
    harness.context = context
    pages, closed = await step._read_parts(sessions, context, _policy(), 9, _CURSOR, _PERMIT, time.monotonic() + 60)
    assert sum(len(page.records) for page in pages) == 99_840
    assert len(pages) == 390 and closed == tuple(range(1, 98))
    assert len(part_refs) == 98 and all(reference() is None for reference in part_refs)
    assert pages[-1].part_ordinal == 98 and pages[-1].first_row == 256
    assert events == [
        event
        for ordinal in range(1, 99)
        for event in (("open", ordinal), ("close", ordinal), ("transaction_closed", None), ("session_closed", None))
    ]
    receipt = await step._commit(None, context, pages, closed, _CURSOR, 9, _PERMIT)
    assert receipt.rows_processed == 99_840 and receipt.after == step.SourceCursor(98, 512, 99_840, 390)
    assert harness.earlier_parts == list(range(1, 98))
    assert harness.events.count("source_bulk_authorize") == harness.events.count("copy") == 1
    assert harness.events.count("ordinary_finalize") == 1
    assert "finish_custom_import_build_source_part" not in harness.events
    step.verify_segmented_stream_metadata.assert_not_awaited()


@pytest.mark.parametrize("rows_per_part", [0, 256])
@pytest.mark.parametrize("elapsed", [30.0, 60.0])
async def test_soft_deadline_commits_closed_prefix_without_opening_another_part(monkeypatch, rows_per_part, elapsed):
    harness = _CommitHarness(monkeypatch)
    sessions, context, events = _coalesced_reader(monkeypatch, part_count=3, rows_per_part=rows_per_part)
    clock = SimpleNamespace(now=0.0)
    prepare = step._prepare_part

    async def finish_after_elapsed_time(*arguments, **keywords):
        result = await prepare(*arguments, **keywords)
        clock.now = elapsed
        return result

    monkeypatch.setattr(step, "time", SimpleNamespace(monotonic=lambda: clock.now))
    monkeypatch.setattr(step, "_prepare_part", finish_after_elapsed_time)
    if elapsed == 60:
        with pytest.raises(LeaseAuthorityLost, match="read deadline elapsed"):
            await step._read_parts(sessions, context, _policy(), 9, _CURSOR, _PERMIT, 60)
        assert harness.events == []
    else:
        pages, closed = await step._read_parts(sessions, context, _policy(), 9, _CURSOR, _PERMIT, 60)
        assert closed == (1,) and sum(len(page.records) for page in pages) == rows_per_part
        receipt = await step._commit(None, context, pages, closed, _CURSOR, 9, _PERMIT)
        assert receipt.after == step.SourceCursor(2, 0, rows_per_part, int(bool(rows_per_part)))
        assert harness.events[-2:] == ["fresh_authority", "commit"]
    assert [ordinal for event, ordinal in events if event == "open"] == [1]
    step.verify_segmented_stream_metadata.assert_not_awaited()


@pytest.mark.parametrize("elapsed,expires", [(30.0, False), (60.0, True)])
async def test_slow_page_reserves_commit_time_without_relaxing_hard_expiry(monkeypatch, elapsed, expires):
    harness = _CommitHarness(monkeypatch)
    sessions, context, events = _coalesced_reader(monkeypatch, part_count=2)
    clock = SimpleNamespace(now=0.0)
    source_pages = staging._source_pages
    closed = Mock(wraps=staging._close_iterator)

    def slow_pages(*arguments):
        for page in source_pages(*arguments):
            clock.now = elapsed
            yield page

    monkeypatch.setattr(step, "time", SimpleNamespace(monotonic=lambda: clock.now))
    monkeypatch.setattr(staging, "_source_pages", slow_pages)
    monkeypatch.setattr(staging, "_close_iterator", closed)
    if expires:
        with pytest.raises(LeaseAuthorityLost, match="read deadline elapsed"):
            await step._read_parts(sessions, context, _policy(), 9, _CURSOR, _PERMIT, 60)
        assert harness.events == []
    else:
        pages, completed_parts = await step._read_parts(sessions, context, _policy(), 9, _CURSOR, _PERMIT, 60)
        assert completed_parts == () and sum(len(page.records) for page in pages) == 256
        receipt = await step._commit(None, context, pages, completed_parts, _CURSOR, 9, _PERMIT)
        assert receipt.after == step.SourceCursor(1, 256, 256, 1)
        assert harness.events[-2:] == ["fresh_authority", "commit"]
    assert [ordinal for event, ordinal in events if event == "open"] == [1]
    assert closed.call_count == 1 and closed.call_args.args[0].gi_frame is None
    step.verify_segmented_stream_metadata.assert_not_awaited()


async def test_partial_large_part_resume_coalesces_only_current_and_next_part(monkeypatch):
    sessions, context, events = _coalesced_reader(monkeypatch, part_count=4, rows_per_part=100_000)
    compared = AsyncMock()
    monkeypatch.setattr(step, "_compare_prefix", compared)
    cursor = step.SourceCursor(3, 90_000, 290_000, 1200)

    # The existing page iterator creates a boundary exactly at the durable cursor.
    def pages(_context, part, _policy, position):
        prepared = _prepared_page(context).records[0]
        splits = (0, position[1], 100_000) if position[1] else (0, 100_000)
        for first, stop in zip(splits, splits[1:]):
            for offset in range(first, stop, 256):
                yield staging._SourcePage(
                    part.ordinal, offset, position[2] + offset, (prepared,) * min(256, stop - offset)
                )

    monkeypatch.setattr(staging, "_source_pages", pages)
    pages, closed = await step._read_parts(sessions, context, _policy(), 9, cursor, _PERMIT, time.monotonic() + 60)
    assert closed == (3,) and 99_000 < sum(len(page.records) for page in pages) <= 100_000
    assert pages[0].first_row == 90_000 and pages[0].first_source == 290_000
    assert pages[-1].part_ordinal == 4 and compared.await_count == 1
    assert {ordinal for event, ordinal in events if event == "open"} == {3, 4}


async def test_partial_resume_uses_single_pool_slot_without_nested_read_checkout(monkeypatch):
    sessions, context, events = _coalesced_reader(monkeypatch, part_count=2)
    pool, comparisons = asyncio.Semaphore(1), []

    @asynccontextmanager
    async def single_slot():
        async with pool:
            async with sessions() as session:
                yield session

    async def compare(factory, _context, page, _permit, _deadline):
        assert events[-1][0] == "session_closed"
        async with factory():
            comparisons.append(page.first_row)

    monkeypatch.setattr(step, "_compare_prefix", compare)
    pages, closed = await asyncio.wait_for(
        step._read_parts(
            single_slot, context, _policy(), 9, step.SourceCursor(1, 512, 512, 2), _PERMIT, time.monotonic() + 60
        ),
        timeout=1,
    )
    assert comparisons == [0] and closed == (1, 2)
    assert sum(len(page.records) for page in pages) == 1536
    assert not pool.locked() and events[-1] == ("session_closed", None)


async def test_later_part_close_failure_discards_all_prepared_cross_part_rows(monkeypatch):
    sessions, context, events = _coalesced_reader(monkeypatch, part_count=3, failure=2)
    monkeypatch.setattr(
        step, "_load_context", AsyncMock(return_value=(context, _policy(), 9, time.monotonic() + 60, False))
    )
    commit = AsyncMock()
    monkeypatch.setattr(step, "_commit", commit)
    with pytest.raises(RuntimeError, match="later part cleanup"):
        await step.serve_source_batch(
            sessions,
            execution_id=4,
            build_id=11,
            fence=1,
            stream_slot=1,
            expected_cursor=_CURSOR,
            lease_token=context.request.lease_token,
            source_permit=_PERMIT,
        )
    commit.assert_not_awaited()
    assert events[-3:] == [("close", 2), ("transaction_closed", None), ("session_closed", None)]


async def test_cross_part_final_eof_finishes_last_part_after_one_promotion(monkeypatch):
    harness = _CommitHarness(monkeypatch, final=True)
    sessions, context, events = _coalesced_reader(monkeypatch, part_count=3)
    pages, closed = await step._read_parts(sessions, context, _policy(), 9, _CURSOR, _PERMIT, time.monotonic() + 60)
    assert closed == (1, 2, 3) and events[-4:] == [("transaction_closed", None), ("session_closed", None)] * 2
    step.verify_segmented_stream_metadata.assert_awaited_once()
    receipt = await step._commit(None, context, pages, closed, _CURSOR, 9, _PERMIT)
    assert receipt.rows_processed == 3072 and receipt.after == step.SourceCursor(4, 0, 3072, 12)
    assert receipt.phase == "admission" and receipt.stream_complete
    assert harness.earlier_parts == [1, 2] and harness.events.count("ordinary_finalize") == 1
    assert harness.events.count("finish_custom_import_build_source_part") == 1


async def test_cross_part_canonical_byte_cap_stops_inside_next_part(monkeypatch):
    sessions, context, events = _coalesced_reader(monkeypatch, part_count=3)
    prepared_bytes = _prepared_page(context).records[0].byte_count
    context = replace(context, request=replace(context.request, page_byte_limit=prepared_bytes * 256))
    monkeypatch.setattr(step, "MAX_BATCH_BYTES", prepared_bytes * 1280)
    pages, closed = await step._read_parts(sessions, context, _policy(), 9, _CURSOR, _PERMIT, time.monotonic() + 60)
    assert sum(len(page.records) for page in pages) == 1280 and closed == (1,)
    assert sum(prepared.byte_count for page in pages for prepared in page.records) == step.MAX_BATCH_BYTES
    assert pages[-1].part_ordinal == 2 and pages[-1].first_row == 0
    assert events[-3:] == [("close", 2), ("transaction_closed", None), ("session_closed", None)]


async def test_cross_part_empty_eofs_advance_without_copy_then_freeze(monkeypatch):
    harness = _CommitHarness(monkeypatch)
    sessions, context, events = _coalesced_reader(monkeypatch, part_count=3, rows_per_part=0)
    pages, closed = await step._read_parts(sessions, context, _policy(), 9, _CURSOR, _PERMIT, time.monotonic() + 60)
    assert pages == () and closed == (1, 2, 3)
    step.verify_segmented_stream_metadata.assert_awaited_once()
    original_call = harness.call

    async def finish_final(session, name, arguments):
        harness.final = name == "finish_custom_import_build_source_part" and arguments[-1][1] == 3
        return await original_call(session, name, arguments)

    monkeypatch.setattr(staging, "_call", finish_final)
    receipt = await step._commit(None, context, pages, closed, _CURSOR, 9, _PERMIT)
    assert receipt.rows_processed == 0 and receipt.after == step.SourceCursor(4, 0, 0, 0)
    assert receipt.phase == "admission" and receipt.stream_complete
    assert harness.events.count("finish_custom_import_build_source_part") == 3
    assert "copy" not in harness.events and "ordinary_finalize" not in harness.events
    assert events[-1] == ("session_closed", None)


@pytest.mark.parametrize("failure,error", [("finalize", RuntimeError), ("authority", LeaseAuthorityLost)])
async def test_cross_part_commit_failure_rolls_back_complete_prefix_atomically(monkeypatch, failure, error):
    harness = _CommitHarness(monkeypatch, final=True, failure=failure)
    sessions, context, _ = _coalesced_reader(monkeypatch, part_count=3)
    pages, closed = await step._read_parts(sessions, context, _policy(), 9, _CURSOR, _PERMIT, time.monotonic() + 60)
    with pytest.raises(error):
        await step._commit(None, context, pages, closed, _CURSOR, 9, _PERMIT)
    assert step.SourceCursor.from_model(harness.stream) == _CURSOR
    assert harness.stream.replay_verified_at is None and harness.build.phase == "source"
    assert harness.events[-1] == "rollback"


@pytest.mark.parametrize("permit", [None, object()])
async def test_missing_typed_source_authority_fails_before_sessions(monkeypatch, permit):
    sessions = AsyncMock()
    with pytest.raises(TypeError, match="typed permit"):
        await step.serve_source_batch(
            sessions,
            execution_id=4,
            build_id=11,
            fence=1,
            stream_slot=1,
            expected_cursor=_CURSOR,
            lease_token=_request().lease_token,
            source_permit=permit,
        )
    sessions.assert_not_called()


@pytest.mark.parametrize(
    "changes,error",
    [
        ({"execution_id": True}, ValueError),
        ({"fence": 0}, ValueError),
        ({"stream_slot": 2**15}, ValueError),
        ({"expected_cursor": asdict(_CURSOR)}, TypeError),
        ({"source_permit": _permit(expires_at=_EXPIRY.replace(tzinfo=None))}, ValueError),
        ({"source_permit": _permit(expires_at=_EXPIRY.astimezone(dt.timezone(dt.timedelta(hours=1))))}, ValueError),
    ],
)
async def test_invalid_source_bounds_fail_before_loading_retained_state(monkeypatch, changes, error):
    load = AsyncMock()
    monkeypatch.setattr(step, "_load_context", load)
    arguments_by_name = dict(
        execution_id=4,
        build_id=11,
        fence=1,
        stream_slot=1,
        expected_cursor=_CURSOR,
        lease_token=_request().lease_token,
        source_permit=_PERMIT,
    )
    with pytest.raises(error):
        await step.serve_source_batch(None, **(arguments_by_name | changes))
    load.assert_not_awaited()


async def test_expired_read_budget_cannot_set_another_query_timeout(monkeypatch):
    timeout = AsyncMock()
    monkeypatch.setattr(staging, "_set_timeout", timeout)
    monkeypatch.setattr(step, "time", SimpleNamespace(monotonic=lambda: 60))
    with pytest.raises(LeaseAuthorityLost, match="read deadline elapsed"):
        await step._read_timeout(object(), _request(), 60)
    timeout.assert_not_awaited()
