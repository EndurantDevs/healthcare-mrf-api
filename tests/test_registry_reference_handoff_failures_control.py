# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Control transaction protocols; queued SQL replies are host data, not native authorization."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import db
from process import nucc_protected_control as control
from process import reference_family_archive as native
from process import source_profile_result_pins as pins
from tests.test_nucc_protected_control import custody

pytestmark = pytest.mark.asyncio


def _reply(record=None, rows=()):
    return SimpleNamespace(
        mappings=lambda: SimpleNamespace(one=lambda: record, one_or_none=lambda: record, all=lambda: rows)
    )


@pytest.fixture
def source_actor_case(monkeypatch):
    actor_by_field = {
        "session": "source_login",
        "current": "source_login",
        "safe": True,
        "pid": 11,
        "xid": "9",
        "database_oid": 100,
        "actor_oid": 102,
        "owner_oid": 101,
    }
    session = AsyncSession()
    execute = AsyncMock(side_effect=lambda _statement: _reply(actor_by_field))
    monkeypatch.setattr(session, "execute", execute)
    monkeypatch.setenv("HLTHPRT_DB_USER", "source_login")
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "test-node")
    monkeypatch.setattr(pins, "load_role_policy", lambda: ("retained_owner", ("source_login",)))
    monkeypatch.setattr(db, "_reader_binding", lambda: None)
    monkeypatch.setattr(db, "_transaction_binding", lambda: None)
    return SimpleNamespace(session=session, actor=actor_by_field, execute=execute)


async def test_source_transaction_binds_original_session_actor_and_runtime_policy(source_actor_case):
    case = source_actor_case
    async with case.session, case.session.begin():
        actor = await control._source_transaction(case.session)
        assert actor == {**case.actor, "runtime_roles": ("source_login",), "node_id": "test-node"}
        await control._recheck_source_transaction(case.session, actor)
        case.actor["xid"] = "10"
        with pytest.raises(control.NuccProtectedControlError, match="transaction changed"):
            await control._recheck_source_transaction(case.session, actor)
    sql = str(case.execute.await_args.args[0])
    for required in ("session_replication_role", "caller.rolcanlogin", "NOT pg_has_role", "pg_execute_server_program"):
        assert required in sql


@pytest.mark.parametrize("failure", ["login", "reader", "binding", "nested"])
async def test_source_transaction_refuses_changed_identity_or_binding(source_actor_case, monkeypatch, failure):
    case = source_actor_case
    if failure == "login":
        monkeypatch.setenv("HLTHPRT_DB_USER", "unreviewed_login")
    if failure == "reader":
        monkeypatch.setattr(db, "_reader_binding", lambda: object())
    if failure == "binding":
        monkeypatch.setattr(db, "_transaction_binding", lambda: SimpleNamespace(session=object()))
    async with case.session, case.session.begin():
        if failure == "nested":
            async with case.session.begin_nested():
                with pytest.raises(control.NuccProtectedControlError):
                    await control._source_transaction(case.session)
        else:
            with pytest.raises(control.NuccProtectedControlError):
                await control._source_transaction(case.session)
    if failure in {"login", "reader", "binding", "nested"}:
        case.execute.assert_not_awaited()


@pytest.mark.parametrize("failure", [None, "recapture"])
async def test_event_custody_locks_and_recaptures_exact_column_authority(monkeypatch, failure):
    first = custody()
    second = deepcopy(first)
    if failure == "recapture":
        second["oid"] += 1
    session = SimpleNamespace(execute=AsyncMock(side_effect=[_reply(first), _reply(), _reply(second)]))
    catalog = AsyncMock()
    monkeypatch.setattr(native, "require_native_read_catalog", catalog)
    if failure:
        with pytest.raises(control.NuccProtectedControlError):
            await control._lock_event_custody(session, {"owner_oid": 101, "actor_oid": 102})
    else:
        await control._lock_event_custody(session, {"owner_oid": 101, "actor_oid": 102})
    catalog.assert_awaited_once_with(session, (100,))
    assert [str(call.args[0]) for call in session.execute.await_args_list] == [
        control._EVENT_CUSTODY_SQL,
        "SELECT fence_token FROM hp_snapshot_retention.publication WHERE false",
        control._EVENT_CUSTODY_SQL,
    ]
