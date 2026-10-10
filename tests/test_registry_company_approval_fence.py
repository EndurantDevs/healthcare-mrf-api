# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Current-company fencing and retained replay without database connections."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import registry_company_approval_fence as fence
from process import registry_ptg_producer_scope as scope
from tests.test_registry_ptg_producer_scope import _approved, _record, _Result, _session


@pytest.mark.asyncio
@pytest.mark.parametrize("current_revision,archived", [(4, True), (4, False), (3, True), (None, False), (3, False)])
async def test_new_approval_requires_actual_current_active_company(monkeypatch, current_revision, archived):
    monkeypatch.setattr(scope, "require_registry_company_approval_fence", AsyncMock())
    approved = _approved()
    session = _session(
        _Result(rows=[]), _Result(value=current_revision == approved["approved_revision"] and not archived)
    )
    if current_revision == 3 and not archived:
        await scope._require_current_approved_company(
            session, '"synthetic_control"."registry_ptg_producer_scope"', approved
        )
    else:
        with pytest.raises(scope.RegistryPTGProducerScopeError, match="company_unapproved"):
            await scope._require_current_approved_company(
                session, '"synthetic_control"."registry_ptg_producer_scope"', approved
            )
    query = str(session.execute.await_args.args[0])
    assert "company.approved_revision=current.approved_revision" in query
    assert "current.id=1 AND current.approved_revision=:revision" in query
    assert "'archived'='false'::jsonb" in query
    assert "FOR UPDATE" not in query and "UPDATE " not in query


@pytest.mark.asyncio
async def test_retained_exact_replay_does_not_consult_later_company_revision(monkeypatch):
    monkeypatch.setattr(scope, "require_registry_company_approval_fence", AsyncMock())
    session = _session(_Result(rows=[_record(_approved())]))
    await scope._require_current_approved_company(
        session, '"synthetic_control"."registry_ptg_producer_scope"', _approved()
    )
    assert session.execute.await_count == 1
    assert "registry_revision_control" not in str(session.execute.await_args.args[0])
    changed_by_field = {**_approved(), "reason": "Changed statement"}
    with pytest.raises(scope.RegistryPTGProducerScopeError, match="idempotency_conflict"):
        await scope._require_current_approved_company(
            _session(_Result(rows=[_record(_approved())])),
            '"synthetic_control"."registry_ptg_producer_scope"',
            changed_by_field,
        )


@pytest.mark.asyncio
async def test_pointer_writer_uses_same_native_key_and_caller_transaction():
    connection = SimpleNamespace(is_in_transaction=lambda: True, execute=AsyncMock())
    await fence.lock_registry_company_approval_writer(connection, "synthetic_control")
    connection.execute.assert_awaited_once_with(
        "SELECT pg_catalog.pg_advisory_xact_lock($1)", fence._fence_key("synthetic_control")
    )
    connection.is_in_transaction = lambda: False
    with pytest.raises(ValueError, match="caller_transaction"):
        await fence.lock_registry_company_approval_writer(connection, "synthetic_control")


def test_scope_factory_requires_owned_native_single_connection_pool():
    engine = create_async_engine("postgresql+asyncpg://synthetic@localhost/synthetic", pool_size=1, max_overflow=0)
    assert fence._source_engine(async_sessionmaker(engine)) is engine
    with pytest.raises(ValueError, match="fence_unavailable"):
        fence._source_engine(lambda: None)
    other = create_async_engine("postgresql+asyncpg://synthetic@localhost/synthetic", pool_size=2, max_overflow=0)
    with pytest.raises(ValueError, match="fence_unavailable"):
        fence._source_engine(async_sessionmaker(other))


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "body", "cancel", "lock", "unlock"])
@pytest.mark.parametrize("is_exclusive", [False, True])
async def test_scope_connection_fence_precedes_rr_and_never_leaks(monkeypatch, failure, is_exclusive):
    events = []
    connection = SimpleNamespace(sync_connection=object(), in_transaction=lambda: False)
    connection.start = AsyncMock(side_effect=lambda: events.append("start"))
    connection.execution_options = AsyncMock(side_effect=lambda **options: events.append(options["isolation_level"]))
    connection.commit = AsyncMock(side_effect=lambda: events.append("commit"))
    connection.close = AsyncMock(side_effect=lambda: events.append("close"))
    connection.invalidate = AsyncMock(side_effect=lambda: events.append("invalidate"))

    async def is_lock_success(statement, parameters):
        operation = "unlock" if "unlock" in str(statement) else "lock"
        suffix = "" if is_exclusive else "_shared"
        expected = "pg_advisory_unlock" if operation == "unlock" else "pg_try_advisory_lock"
        assert expected + suffix + "(:key)" in str(statement)
        events.append(operation)
        return failure != operation

    connection.scalar = is_lock_success
    monkeypatch.setattr(fence, "_source_engine", lambda sessions: SimpleNamespace(connect=lambda: connection))
    transaction = object()
    session = SimpleNamespace(info={}, get_transaction=lambda: transaction)

    @asynccontextmanager
    async def begin():
        events.append("begin")
        yield
        events.append("end")

    session.begin = begin

    @asynccontextmanager
    async def sessions(**kwargs):
        assert kwargs["bind"] is connection
        yield session

    async def invoke():
        async with fence.registry_company_approval_transaction(
            sessions, control_schema="synthetic_control", is_exclusive=is_exclusive
        ) as owned:
            assert owned is session and fence._FENCE_INFO in session.info
            if failure == "body":
                raise RuntimeError("synthetic failure")
            if failure == "cancel":
                raise asyncio.CancelledError()

    if failure:
        with pytest.raises((RuntimeError, ValueError, asyncio.CancelledError)):
            await invoke()
    else:
        await invoke()
    assert events.index("AUTOCOMMIT") < events.index("lock") < events.index("commit")
    if failure != "lock":
        assert events.index("commit") < events.index("REPEATABLE READ") < events.index("begin")
    assert events[-1] == "close" and not session.info
    assert ("invalidate" in events) is bool(failure)


@pytest.mark.asyncio
async def test_detached_scope_session_has_no_fence_authority():
    session = SimpleNamespace(info={}, get_transaction=lambda: object(), in_transaction=lambda: True)
    with pytest.raises(ValueError, match="fence_unavailable"):
        await fence.require_registry_company_approval_fence(session, "synthetic_control")


@pytest.mark.asyncio
@pytest.mark.parametrize("held", [False, True])
async def test_scope_context_requires_actual_native_shared_lock(held):
    transaction = object()
    session = SimpleNamespace(
        info={}, get_transaction=lambda: transaction, in_transaction=lambda: True, scalar=AsyncMock(return_value=held)
    )
    key = fence._fence_key("synthetic_control")
    session.info[fence._FENCE_INFO] = fence._ScopeFence(session, transaction, "synthetic_control", key)
    if held:
        await fence.require_registry_company_approval_fence(session, "synthetic_control")
    else:
        with pytest.raises(ValueError, match="fence_unavailable"):
            await fence.require_registry_company_approval_fence(session, "synthetic_control")
    statement, parameters = session.scalar.await_args.args
    assert "pg_backend_pid()" in str(statement) and "mode='ShareLock'" in str(statement)
    assert parameters == {"high": (key >> 32) & 0xFFFFFFFF, "low": key & 0xFFFFFFFF}
    session.get_transaction = lambda: object()
    with pytest.raises(ValueError, match="fence_unavailable"):
        await fence.require_registry_company_approval_fence(session, "synthetic_control")


@pytest.mark.asyncio
async def test_actual_pointer_approval_fences_before_selection_and_exact_replay(monkeypatch):
    from process import registry_approval_store as approvals

    events = []
    connection = SimpleNamespace(is_in_transaction=lambda: True)

    @asynccontextmanager
    async def transaction():
        yield

    async def execute(statement, key):
        assert statement == "SELECT pg_catalog.pg_advisory_xact_lock($1)"
        assert key == fence._fence_key("synthetic_control")
        events.append("fence")

    async def selection(connection, selected):
        events.append("selection")
        return selected

    async def fetchrow(statement, *parameters):
        if "FOR UPDATE" in statement:
            events.append("control")
            return {"draft_revision": 5, "approved_revision": 4}
        events.append("replay")
        return {"request_sha256": "exact", "approved_revision": 3, "previous_approved_revision": 2, "selected_count": 1}

    connection.transaction, connection.execute, connection.fetchrow = transaction, execute, fetchrow
    monkeypatch.setattr(approvals, "_validated_command", lambda command: ("Reviewed", "[]"))
    monkeypatch.setattr(approvals, "_validated_actor", lambda actor: {})
    monkeypatch.setattr(approvals, "_namespace", lambda schema: '"synthetic_control"')
    monkeypatch.setattr(approvals, "_canonical_selection", selection)
    monkeypatch.setattr(
        approvals, "_approval_metadata", lambda *args: {"actor_key": "actor", "request_sha256": "exact"}
    )
    command = SimpleNamespace(idempotency_key="retry", expected_draft_revision=2, expected_approved_revision=1)
    receipt = await approvals.approve_registry_records(connection, command, object())
    assert receipt == {"approved_revision": 3, "previous_approved_revision": 2, "selected_count": 1, "replayed": True}
    assert events == ["fence", "selection", "control", "replay"]


@pytest.mark.asyncio
async def test_exclusive_cleanup_requires_original_native_lock_and_exact_context():
    transaction = object()
    session = SimpleNamespace(
        info={}, get_transaction=lambda: transaction, in_transaction=lambda: True, scalar=AsyncMock(return_value=True)
    )
    key = fence._fence_key("synthetic_control")
    session.info[fence._FENCE_INFO] = fence._ScopeFence(session, transaction, "synthetic_control", key)
    await fence.require_registry_company_approval_fence(session, "synthetic_control", is_exclusive=True)
    assert "mode='ExclusiveLock'" in str(session.scalar.await_args.args[0])
    session.scalar.return_value = False
    with pytest.raises(ValueError, match="fence_unavailable"):
        await fence.require_registry_company_approval_fence(session, "synthetic_control", is_exclusive=True)
    with pytest.raises(ValueError, match="fence_unavailable"):
        await fence.require_registry_company_approval_fence(session, "synthetic_control", is_exclusive=1)
