# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Required-proof policy distinguishes CMS NPD serving from legacy CMS education."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import ServiceUnavailable
from sqlalchemy.exc import DBAPIError

from api import network_read_execution as execution
from api import provider_profile_snapshot as snapshot
from tests.test_provider_profile_snapshot_read_identity import _Session


async def _issued_failure(session, error):
    """Use the real guarded executor to issue a failure bound to this session."""
    session.execute = AsyncMock(side_effect=error)
    with execution.network_read_name_inventory(session.driver, (), absent_names=(("synthetic", "missing"),)):
        try:
            await execution.execute_network_read(session, "SELECT provider_payload")
        except execution._ReadSQLFailure as failure:
            return failure
    pytest.fail("the guarded executor did not issue a SQL failure")


@pytest.mark.parametrize(
    "has_history_table,has_profile_table,answers,expected",
    [
        (False, False, [], False),
        (False, True, [False], False),
        (False, True, [True], True),
        (True, True, [False, False], False),
        (True, True, [False, True], True),
        (True, False, [True], True),
        (True, True, [True], True),
    ],
)
async def test_required_proof_survives_profile_withdrawal(has_history_table, has_profile_table, answers, expected):
    session = SimpleNamespace(
        execute=AsyncMock(side_effect=[SimpleNamespace(scalar=lambda value=value: value) for value in answers])
    )
    oid_by_relation = {
        "mrf.provider_directory_cms_serving_receipt": 1 if has_history_table else None,
        "mrf.provider_directory_profile_serving_generation": 2 if has_profile_table else None,
    }
    assert await snapshot._requires_cms_receipt(session, "mrf", oid_by_relation) is expected
    assert session.execute.await_count == len(answers)


@pytest.mark.parametrize("has_receipt_table", [False, True])
async def test_missing_common_proof_is_unavailable(monkeypatch, has_receipt_table):
    read_receipt = AsyncMock(return_value=None)
    monkeypatch.setattr(snapshot.cms_serving_receipt, "read_serving_receipt", read_receipt)
    oid_by_relation = {"mrf.provider_directory_cms_serving_receipt": 1 if has_receipt_table else None}
    with pytest.raises(ServiceUnavailable, match="temporarily unavailable"):
        await snapshot._read_cms_receipt(SimpleNamespace(), "mrf", oid_by_relation, verify_names=AsyncMock())
    assert read_receipt.await_count == int(has_receipt_table)


@pytest.mark.parametrize("wrapped", [False, True])
@pytest.mark.parametrize(
    "borrowed,phase,sqlstate,failures,expected_attempts,unavailable",
    [
        (False, "setup", "55P03", 1, 1, True),
        (False, "setup", "changed", 1, 2, False),
        (False, "setup", "changed", 2, 3, False),
        (False, "setup", "changed", 3, 3, True),
        (True, "setup", "changed", 1, 1, True),
        (False, "loader", "changed", 1, 1, True),
        (True, "setup", "55P03", 1, 1, True),
        (False, "setup", "57014", 1, 1, True),
        (False, "loader", "55P03", 1, 1, True),
        (False, "loader", "57014", 1, 1, True),
    ],
)
async def test_only_changed_snapshot_retries_unyielded_sessions(
    monkeypatch, borrowed, phase, sqlstate, failures, expected_attempts, unavailable, wrapped
):
    sessions, closed_sessions, loader_calls = [], [], []
    error = (
        snapshot._SnapshotSetupChanged("Synthetic catalog change")
        if sqlstate == "changed"
        else DBAPIError(None, None, SimpleNamespace(sqlstate=sqlstate))
    )

    @asynccontextmanager
    async def reader_session(**_options):
        session = _Session()
        sessions.append(session)
        try:
            yield session
        finally:
            session.driver.active = False
            closed_sessions.append(session)

    @asynccontextmanager
    async def scope(session, _schema, **_options):
        assert sessions[:-1] == closed_sessions
        if phase == "setup" and len(sessions) <= failures:
            raise await _issued_failure(session, error) if wrapped and sqlstate != "changed" else error
        yield

    database = SimpleNamespace(
        has_reader_session=lambda: borrowed, _transaction_binding=lambda: None, reader_session=reader_session
    )
    monkeypatch.setattr(snapshot, "_read_snapshot_scope", scope)
    try:
        async with snapshot.provider_profile_read_snapshot(database, "synthetic") as session:
            loader_calls.append(session)
            if phase == "loader":
                raise await _issued_failure(session, error) if wrapped and sqlstate != "changed" else error
    except ServiceUnavailable as caught:
        assert unavailable
        assert caught is error if sqlstate == "changed" else caught.__cause__ is error
    else:
        assert not unavailable
    assert len(sessions) == expected_attempts and sessions == closed_sessions
    assert len(loader_calls) == (1 if phase == "loader" or not unavailable else 0)


@pytest.mark.parametrize("damage", ["issuer", "context", "session", "subclass", "service"])
async def test_snapshot_does_not_retry_unissued_sqlstate(monkeypatch, damage):
    sessions, closed_sessions = [], []
    original = DBAPIError(None, None, SimpleNamespace(sqlstate="55P03"))

    @asynccontextmanager
    async def reader_session(**_options):
        session = _Session()
        sessions.append(session)
        try:
            yield session
        finally:
            session.driver.active = False
            closed_sessions.append(session)

    @asynccontextmanager
    async def scope(session, _schema, **_options):
        issued = await _issued_failure(session, original)
        assert execution._read_sql_failure_cause(issued, session) is original
        if damage == "issuer":
            issued.issuer = object()
        elif damage == "context":
            issued.context = SimpleNamespace(session=session)
        elif damage == "session":
            issued = await _issued_failure(_Session(), original)
        elif damage == "subclass":

            class InheritedFailure(execution._ReadSQLFailure):
                pass

            issued = InheritedFailure(issued.context, original, issued.issuer)
        else:
            issued = ServiceUnavailable("Unavailable")
            issued.__cause__ = original
        assert execution._read_sql_failure_cause(issued, session) is None
        raise issued
        yield

    database = SimpleNamespace(
        has_reader_session=lambda: False, _transaction_binding=lambda: None, reader_session=reader_session
    )
    monkeypatch.setattr(snapshot, "_read_snapshot_scope", scope)
    with pytest.raises(ServiceUnavailable):
        async with snapshot.provider_profile_read_snapshot(database, "synthetic"):
            pytest.fail("unissued failure reached a loader")
    assert len(sessions) == 1 and sessions == closed_sessions


@pytest.mark.parametrize("damage", [None, "new_optional", "missing_optional", "changed_oid"])
async def test_fresh_snapshot_locks_before_reading_current_catalog(monkeypatch, damage):
    events, statements = [], []
    prior_oid_by_ref = {"synthetic.provider_directory_profile": 10}
    current_oid_by_ref = {"synthetic.provider_directory_profile": 20}
    if damage == "new_optional":
        prior_oid_by_ref["synthetic.provider_directory_profile_evidence"] = None
        current_oid_by_ref["synthetic.provider_directory_profile_evidence"] = 30
    elif damage == "missing_optional":
        current_oid_by_ref["synthetic.provider_directory_profile"] = None
    actual_oid_by_ref = dict(current_oid_by_ref)
    if damage == "changed_oid":
        actual_oid_by_ref["synthetic.provider_directory_profile"] = 99

    async def inventory(_session, _schema, table_names):
        events.append("catalog")
        return {"synthetic." + name: current_oid_by_ref.get("synthetic." + name) for name in table_names}

    async def resolve(_session, _schema, table_names):
        is_initial_census = "names" not in events
        events.append("names" if is_initial_census else "actual")
        selected_oid_by_ref = prior_oid_by_ref if is_initial_census else actual_oid_by_ref
        return {"synthetic." + name: selected_oid_by_ref.get("synthetic." + name) for name in table_names}

    async def rollback():
        events.append("rollback")

    async def execute(sql):
        statements.append(str(sql))
        events.append("lock" if str(sql).startswith("LOCK TABLE") else "setting")

    session = SimpleNamespace(execute=execute, rollback=rollback)
    monkeypatch.setattr(snapshot, "_snapshot_relation_oids", inventory)
    monkeypatch.setattr(snapshot, "_relation_oids", resolve)
    if damage is None:
        observed = await snapshot._lock_serving_relations(
            session, "synthetic", include_detail=False, fresh_snapshot=True
        )
        assert observed["synthetic.provider_directory_profile"] == 20
    else:
        with pytest.raises(snapshot._SnapshotSetupChanged):
            await snapshot._lock_serving_relations(session, "synthetic", include_detail=False, fresh_snapshot=True)
    assert events[:8] == ["names", "rollback", "setting", "setting", "setting", "lock", "setting", "catalog"]
    assert statements[:3] == [
        "SET LOCAL lock_timeout = '250ms'",
        "SET LOCAL statement_timeout = '5s'",
        "SET LOCAL lock_timeout = '5s'",
    ]
    assert statements[4] == "SET LOCAL lock_timeout = '250ms'"


async def test_borrowed_snapshot_never_discards_existing_reads(monkeypatch):
    expected_oid_by_ref = {"synthetic.provider_directory_profile": 10}

    async def inventory(_session, _schema, table_names):
        return {"synthetic." + name: expected_oid_by_ref.get("synthetic." + name) for name in table_names}

    session = SimpleNamespace(execute=AsyncMock(), rollback=AsyncMock())
    monkeypatch.setattr(snapshot, "_snapshot_relation_oids", inventory)
    monkeypatch.setattr(snapshot, "_relation_oids", inventory)
    await snapshot._lock_serving_relations(session, "synthetic", include_detail=False)
    session.rollback.assert_not_awaited()
    assert len(session.execute.await_args_list) == 1


@pytest.mark.parametrize("sqlstate", ["55P03", "57014"])
async def test_owned_setup_timeout_preserves_original_failure(monkeypatch, sqlstate):
    """An aborted lock statement is terminal; do not issue settings in its failed transaction."""
    statements = []
    error = DBAPIError(None, None, SimpleNamespace(sqlstate=sqlstate))

    async def inventory(_session, _schema, table_names):
        return {"synthetic." + name: 10 if name == "provider_directory_profile" else None for name in table_names}

    async def execute(sql):
        statements.append(str(sql))
        if statements[-1].startswith("LOCK TABLE"):
            raise error

    session = SimpleNamespace(execute=execute, rollback=AsyncMock())
    resolve = AsyncMock()
    monkeypatch.setattr(snapshot, "_relation_oids", inventory)
    monkeypatch.setattr(snapshot, "_snapshot_relation_oids", resolve)
    with pytest.raises(DBAPIError) as caught:
        await snapshot._lock_serving_relations(session, "synthetic", include_detail=False, fresh_snapshot=True)
    assert caught.value is error
    session.rollback.assert_awaited_once()
    resolve.assert_not_awaited()
    assert statements == [
        "SET LOCAL lock_timeout = '250ms'",
        "SET LOCAL statement_timeout = '5s'",
        "SET LOCAL lock_timeout = '5s'",
        'LOCK TABLE "synthetic"."provider_directory_profile" IN ACCESS SHARE MODE',
    ]
