# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Unit ordering only; full native admin/profile acceptance remains separate."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import geo_source_provision as provision
from process.provider_directory_generic_source_policy import GenericSourcePolicy


@pytest.fixture
def cut(monkeypatch):
    events = []

    @asynccontextmanager
    async def savepoint():
        events.append("savepoint")
        try:
            yield
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("commit")

    async def execute(sql):
        events.append("drop" if sql.startswith("DROP TRIGGER") else "sql")

    policy = GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("source_reader",))
    native = SimpleNamespace(
        execute=AsyncMock(side_effect=execute),
        fetchval=AsyncMock(side_effect=[101, "geo"]),
        fetch=AsyncMock(return_value=[{"tgname": "reference_source_generation_revision_guard"}]),
    )
    session = SimpleNamespace(in_transaction=lambda: True, begin_nested=savepoint)
    monkeypatch.setattr(provision, "_original_admin_driver", AsyncMock(return_value=native))
    monkeypatch.setattr(provision, "storage_roles", AsyncMock())
    monkeypatch.setattr(provision, "_close_geo_access", AsyncMock(side_effect=lambda *args: events.append("close")))
    shape = AsyncMock(return_value=101)
    monkeypatch.setattr(provision, "require_geo_storage", shape)
    callbacks_by_name = {
        "verify_role_profile": AsyncMock(side_effect=lambda *args, **kwargs: events.append("profile")),
        "verify_legacy_guards": AsyncMock(side_effect=lambda *args: events.append("legacy")),
        "verify_writer_closure": AsyncMock(side_effect=lambda *args: events.append("writers")),
    }
    return SimpleNamespace(
        policy=policy, native=native, session=session, events=events, shape=shape, callbacks_by_name=callbacks_by_name
    )


@pytest.mark.asyncio
async def test_full_closure_precedes_only_attested_guard_retirement(cut):
    assert await provision.provision_geo_source_storage(cut.session, cut.policy, **cut.callbacks_by_name) == 101
    assert cut.events.index("profile") < cut.events.index("legacy") < cut.events.index("close")
    assert cut.events.index("writers") < cut.events.index("drop")
    assert cut.events[-2:] == ["profile", "commit"]
    assert cut.shape.await_args_list[0].kwargs == {
        "schema_name": "source_schema",
        "legacy": True,
        "native_policy": cut.policy,
    }
    statements = [call.args[0] for call in cut.native.execute.await_args_list]
    grant = next(sql for sql in statements if sql.startswith("GRANT"))
    assert 'UPDATE("local_generation"' in grant and 'UPDATE("importer_id"' not in grant
    assert not any("FUNCTION" in sql or "CREATE TRIGGER" in sql for sql in statements)
    update = next(sql for sql in cut.native.fetchval.call_args_list if "UPDATE" in sql.args[0])
    assert "SET source_revision_tracked=FALSE" in update.args[0] and "local_generation=" not in update.args[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("writers_unqualified"), asyncio.CancelledError()])
async def test_writer_failure_or_cancellation_cannot_commit_guard_retirement(cut, failure):
    cut.callbacks_by_name["verify_writer_closure"].side_effect = failure
    with pytest.raises(type(failure)):
        await provision.provision_geo_source_storage(cut.session, cut.policy, **cut.callbacks_by_name)
    assert cut.events[-1] == "rollback" and "drop" not in cut.events
    assert cut.native.fetchval.await_count == 1


@pytest.mark.asyncio
async def test_unknown_hook_refuses_before_any_hook_drop(cut):
    cut.native.fetch.return_value = [{"tgname": "unqualified_hook"}]
    with pytest.raises(RuntimeError, match="hooks_changed"):
        await provision.provision_geo_source_storage(cut.session, cut.policy, **cut.callbacks_by_name)
    assert cut.events[-1] == "rollback" and "drop" not in cut.events


@pytest.mark.asyncio
async def test_missing_profile_callback_refuses_before_admin_connection(cut):
    cut.callbacks_by_name["verify_role_profile"] = None
    with pytest.raises(RuntimeError, match="complete_closure_verifiers_required"):
        await provision.provision_geo_source_storage(cut.session, cut.policy, **cut.callbacks_by_name)
    provision._original_admin_driver.assert_not_awaited()
