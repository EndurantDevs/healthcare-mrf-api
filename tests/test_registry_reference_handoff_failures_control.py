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


async def test_source_transaction_binds_configured_role_node_and_active_transaction(source_actor_case):
    case = source_actor_case
    async with case.session, case.session.begin():
        actor = await control._source_transaction(case.session)
        assert actor == {
            "role": "source_login",
            "runtime_roles": ("source_login",),
            "node_id": "test-node",
            "transaction": case.session.get_transaction(),
        }
        await control._recheck_source_transaction(case.session, actor)
    assert str(case.execute.await_args.args[0]) == "SELECT 1"
    async with case.session.begin():
        with pytest.raises(control.NuccProtectedControlError, match="transaction changed"):
            await control._recheck_source_transaction(case.session, actor)


@pytest.mark.parametrize("failure", ["login", "nested"])
async def test_source_transaction_refuses_an_unconfigured_role_or_nested_transaction(
    source_actor_case, monkeypatch, failure
):
    case = source_actor_case
    if failure == "login":
        monkeypatch.setenv("HLTHPRT_DB_USER", "unreviewed_login")
    async with case.session, case.session.begin():
        if failure == "nested":
            async with case.session.begin_nested():
                with pytest.raises(control.NuccProtectedControlError):
                    await control._source_transaction(case.session)
        else:
            with pytest.raises(control.NuccProtectedControlError):
                await control._source_transaction(case.session)
    case.execute.assert_not_awaited()
