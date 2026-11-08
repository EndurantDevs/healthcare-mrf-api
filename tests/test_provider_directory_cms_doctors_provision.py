# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The fixed source cut requires complete application and independent authority."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import cms_doctors_immutable as immutable
from process import provider_directory_cms_doctors_provision as provision
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_policy import GenericSourcePolicy


@pytest.fixture
def cut(monkeypatch):
    events = []
    policy = GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("source_reader",))
    family_rows = [
        dict(oid=100 + index, nspname=policy.schema, relname=root) for index, root in enumerate(provision.DOCTORS_ROOTS)
    ]
    hooks = [
        dict(oid=relation_by_field["oid"], tgname="reference_source_generation_revision_guard")
        for relation_by_field in family_rows
    ]

    async def execute(sql):
        events.append("drop" if sql.startswith("DROP TRIGGER") else "sql")

    native = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetch=AsyncMock(side_effect=[family_rows, hooks]),
        fetchval=AsyncMock(return_value=True),
        execute=AsyncMock(side_effect=execute),
    )

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

    session, actor_by_field = _actor_session(events, savepoint)
    pairs = tuple((relation_by_field["relname"], relation_by_field["oid"]) for relation_by_field in family_rows)
    monkeypatch.setattr(provision, "_copy_driver", AsyncMock(return_value=native))
    monkeypatch.setattr(provision, "storage_roles", AsyncMock())
    close = AsyncMock(side_effect=lambda *args: events.append("close"))
    monkeypatch.setattr(provision, "_close_relation_grants", close)
    shape = AsyncMock(return_value=pairs)
    monkeypatch.setattr(immutable, "require_doctors_storage", shape)
    callbacks_by_name = {
        "verify_role_profile": AsyncMock(side_effect=lambda *args, **kwargs: events.append("profile")),
        "verify_legacy_guards": AsyncMock(side_effect=lambda *args: events.append("legacy")),
        "verify_writer_closure": AsyncMock(side_effect=lambda *args: events.append("writers")),
    }
    return SimpleNamespace(
        native=native,
        policy=policy,
        session=session,
        pairs=pairs,
        events=events,
        callbacks_by_name=callbacks_by_name,
        shape=shape,
        actor_by_field=actor_by_field,
    )


def _actor_session(events, savepoint):
    actor_by_field = {"session": "native_admin", "current": "native_admin"}
    actor_result = SimpleNamespace(mappings=lambda: SimpleNamespace(one=lambda: actor_by_field))

    async def actor_read(statement):
        assert "session_user" in str(statement)
        events.append("actor_read")
        return actor_result

    return SimpleNamespace(
        in_transaction=lambda: True, begin_nested=savepoint, execute=AsyncMock(side_effect=actor_read)
    ), actor_by_field


def test_only_three_source_models_require_operator_owned_storage():
    for model in immutable._models():
        assert model.__runtime_schema_sync__ is False
        assert model.__table__.info["managed_immutable_storage"] is True
        assert model.__tablename__ in provision.DOCTORS_ROOTS
        assert model.__table__.primary_key and not model.__table__.foreign_keys


@pytest.mark.asyncio
async def test_complete_cut_preserves_oids_and_orders_authority_before_retirement(cut):
    assert (
        await provision.provision_doctors_source_storage(cut.session, cut.policy, **cut.callbacks_by_name) == cut.pairs
    )
    events = cut.events
    assert events.index("profile") < events.index("legacy") < events.index("close") < events.index("writers")
    assert events.index("writers") < events.index("drop") < len(events) - 2
    assert events[-2:] == ["profile", "commit"]
    assert cut.shape.await_args_list[0].kwargs == {
        "schema_name": "source_schema",
        "legacy": True,
        "native_policy": cut.policy,
    }
    assert cut.shape.await_args_list[1].kwargs == {"schema_name": "source_schema", "native_policy": cut.policy}
    statements = [call.args[0] for call in cut.native.execute.await_args_list]
    grant = next(sql for sql in statements if sql.startswith("GRANT"))
    assert 'UPDATE("local_generation"' in grant and 'UPDATE("importer_id"' not in grant
    assert "EXECUTE" not in grant and not any("FUNCTION" in sql or "CREATE TRIGGER" in sql for sql in statements)


@pytest.mark.asyncio
async def test_unqualified_writer_rolls_back_before_any_old_hook_is_dropped(cut):
    cut.callbacks_by_name["verify_writer_closure"].side_effect = RuntimeError("writer_not_qualified")
    with pytest.raises(RuntimeError, match="writer_not_qualified"):
        await provision.provision_doctors_source_storage(cut.session, cut.policy, **cut.callbacks_by_name)
    assert "close" in cut.events and cut.events[-1] == "rollback" and "drop" not in cut.events
    cut.shape.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("actor", ["source_publisher", "source_reader", "source_owner"])
async def test_existing_runtime_role_cannot_invoke_the_native_admin_cut(cut, actor):
    cut.actor_by_field.update(session=actor, current=actor)
    with pytest.raises(GenericCandidateError, match="independent_admin_required"):
        await provision.provision_doctors_source_storage(cut.session, cut.policy, **cut.callbacks_by_name)
    assert cut.events == ["actor_read"]
    cut.native.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_complete_profile_callback_refuses_before_physical_io(cut):
    cut.callbacks_by_name["verify_role_profile"] = None
    with pytest.raises(GenericCandidateError, match="complete_closure_verifiers_required"):
        await provision.provision_doctors_source_storage(cut.session, cut.policy, **cut.callbacks_by_name)
    provision._copy_driver.assert_not_awaited()


@pytest.mark.asyncio
async def test_original_session_actor_read_materializes_lazy_native_transaction(cut):
    cut.native.is_in_transaction = lambda: "actor_read" in cut.events

    async def driver(session):
        assert session is cut.session and "actor_read" in cut.events
        cut.events.append("driver")
        return cut.native

    provision._copy_driver.side_effect = driver
    await provision.provision_doctors_source_storage(cut.session, cut.policy, **cut.callbacks_by_name)
    assert cut.events.index("actor_read") < cut.events.index("driver") < cut.events.index("savepoint")


@pytest.mark.asyncio
async def test_missing_physical_transaction_refuses_before_storage_changes(cut):
    cut.native.is_in_transaction = lambda: False
    with pytest.raises(GenericCandidateError, match="explicit_admin_transaction_required"):
        await provision.provision_doctors_source_storage(cut.session, cut.policy, **cut.callbacks_by_name)
    assert cut.events == ["actor_read"]
    cut.native.execute.assert_not_awaited()
