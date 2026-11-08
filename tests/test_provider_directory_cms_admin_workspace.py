# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admin workspace component checks; mock SQL does not establish native authority."""

import asyncio
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from process import provider_directory_cms_admin_workspace as admin
from process import provider_directory_cms_prepared_workspace as workspace
from process import provider_directory_generic_source_policy as source
from tests.test_provider_directory_cms_prepared_context import case as case
from tests.test_provider_directory_cms_prepared_workspace import allocation_state


@pytest_asyncio.fixture
async def retained(case, monkeypatch):
    """Use real borrowed-session/workspace types with independently controlled native responses."""
    state = allocation_state(case)
    state.inputs = {
        key: input_value for key, input_value in state.inputs.items() if key not in {"origin", "native_coordinates"}
    }
    binding = workspace._binding(case.storage, state.inputs, state.handoff, state.preparation_id, state.admission)
    names = workspace._names(binding)
    state.outputs = {
        name: dict(name=name, oid=1000 + index, owner=76, layout_sha256="2" * 64)
        for index, name in enumerate(names.values())
    }
    state.binding = {
        **binding,
        "tables": {root: deepcopy(state.outputs[name]) for root, name in names.items()},
        "indexed": True,
    }
    case.origin.update(session="independent_admin", current="independent_admin")
    state.path, state.namespace, state.superuser = "pg_catalog", "namespace_owner", True

    async def fetchval(sql, *arguments):
        if sql == "SHOW search_path":
            return state.path
        if "WHERE oid=$1::oid" in sql:
            assert arguments == (case.storage.namespace_owner_oid,)
            return state.namespace
        assert sql == "SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname=session_user"
        return state.superuser

    async def capture(connection, policy, requested):
        assert connection is case.driver and policy == case.policy
        return {name: deepcopy(state.outputs[name]) for name in requested}

    case.driver.fetchval = AsyncMock(side_effect=fetchval)
    case.driver.execute = AsyncMock()
    monkeypatch.setattr(workspace.storage, "_capture", AsyncMock(side_effect=capture))
    return state


def open_workspace(state, **changes):
    """Keep the production eight-argument entrypoint visible in each component check."""
    case = state.case
    arguments_by_name = dict(
        source_binding=case.storage,
        prepared_inputs=state.inputs,
        handoff=state.handoff,
        expected_workspace=state.binding,
        migration_role="migration_login",
    )
    arguments_by_name.update(changes)
    return admin.open_admin_cms_workspace(case.database, case.session, case.driver, **arguments_by_name)


async def test_independent_admin_pins_exact_inputs_and_outputs_without_writes(retained):
    state, case = retained, retained.case
    before = deepcopy(state.outputs)
    ambient = (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get())
    async with case.session.begin(), case.database.bind_existing_session(case.session):
        transaction = case.session.get_transaction()
        async with open_workspace(state) as current:
            assert type(current) is workspace.CMSPreparedWorkspace
            assert current.binding == state.binding
            assert current._inputs["origin"]["current"] == "independent_admin"
            assert current._inputs["origin"]["current"] != case.policy.publisher_role
            assert current._indexed is True and current._readonly is True
            await current.verify_current()
            for operation in (current.copy_rows("invalid", (), ()), current.finish_indexes()):
                with pytest.raises(ValueError, match="readonly_workspace"):
                    await operation
        assert case.session.get_transaction() is transaction
        assert (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get()) == ambient
        with pytest.raises(ValueError, match="scope_ended"):
            await current.verify_current()
    names = [row["relation_name"] for row in state.inputs["frozen_inventory"]["relations"]]
    names.extend(receipt["name"] for receipt in state.binding["tables"].values())
    expected = sorted(workspace.storage_relation(case.policy.schema, name) for name in names)
    case.driver.execute.assert_awaited_once_with(
        "LOCK TABLE " + ",".join("ONLY " + relation for relation in expected) + " IN SHARE MODE NOWAIT"
    )
    assert len(expected) == len(set(expected)) == 19
    assert state.outputs == before
    assert not hasattr(admin, "finalize_frozen_cms_source_candidate")


@pytest.mark.parametrize(
    "actor",
    [
        "source_owner",
        "source_publisher",
        "prepared_owner",
        "prepared_publisher",
        "reader",
        "namespace_owner",
        "migration_login",
    ],
)
async def test_source_prepared_namespace_and_migration_identities_are_not_independent_admin(retained, actor):
    state, case = retained, retained.case
    case.origin.update(session=actor, current=actor)
    with pytest.raises(ValueError, match="independent_original_admin_required"):
        async with case.session.begin(), case.database.bind_existing_session(case.session), open_workspace(state):
            pytest.fail("excluded actor reached the workspace")
    case.driver.execute.assert_not_awaited()


@pytest.mark.parametrize(
    "field,value",
    [
        ("current", "other_admin"),
        ("backend", 99),
        ("database_oid", 99),
        ("isolation", "serializable"),
        ("readonly", "on"),
        ("replication_role", "replica"),
        ("transaction", None),
    ],
)
async def test_native_admin_identity_is_checked_before_locks(retained, field, value):
    state, case = retained, retained.case
    case.origin[field] = value
    with pytest.raises(ValueError, match="independent_original_admin_required"):
        async with case.session.begin(), case.database.bind_existing_session(case.session), open_workspace(state):
            pytest.fail("wrong native actor reached the workspace")
    case.driver.execute.assert_not_awaited()


@pytest.mark.parametrize("fault", ["path", "namespace", "superuser"])
async def test_native_path_namespace_and_superuser_are_required(retained, fault):
    state, case = retained, retained.case
    setattr(state, fault, {"path": '"public",pg_catalog', "namespace": None, "superuser": False}[fault])
    with pytest.raises(ValueError, match="search_path|namespace_owner_missing|independent_original_admin_required"):
        async with case.session.begin(), case.database.bind_existing_session(case.session), open_workspace(state):
            pytest.fail("unsafe native context reached the workspace")
    case.driver.execute.assert_not_awaited()


@pytest.mark.parametrize("fault", ["claimed_origin", "coordinates", "frozen", "indexed", "output"])
async def test_detached_actor_claims_and_native_drift_are_refused(retained, fault):
    state, case = retained, retained.case
    if fault == "claimed_origin":
        state.inputs["origin"] = deepcopy(case.origin)
    elif fault == "coordinates":
        case.coordinates["prepared_owner_oid"] = 999
    elif fault == "frozen":
        case.native_rows[0]["relfilenode"] = 999
    elif fault == "indexed":
        state.binding["indexed"] = False
    else:
        next(iter(state.outputs.values()))["layout_sha256"] = "3" * 64
    with pytest.raises(ValueError, match="inputs_changed|frozen_inventory_changed|retained_|output_changed"):
        async with case.session.begin(), case.database.bind_existing_session(case.session), open_workspace(state):
            pytest.fail("changed observation reached the workspace")
    case.driver.execute.assert_not_awaited()


@pytest.mark.parametrize(
    "fault", ["inputs", "binding", "native_input", "native_output", "actor", "path", "physical", "flags"]
)
async def test_normal_exit_rechecks_every_caller_and_native_boundary(retained, fault):
    state, case = retained, retained.case
    with pytest.raises(ValueError):
        async with case.session.begin(), case.database.bind_existing_session(case.session):
            async with open_workspace(state) as current:
                match fault:
                    case "inputs":
                        state.inputs["frozen_inventory"]["relations"][0]["relfilenode"] = 999
                    case "binding":
                        state.binding["admission_sha256"] = "9" * 64
                    case "native_input":
                        case.native_rows[0]["relfilenode"] = 999
                    case "native_output":
                        next(iter(state.outputs.values()))["oid"] = 999
                    case "actor":
                        case.origin["transaction"] = "3/5"
                    case "path":
                        state.path = "public"
                    case "physical":
                        case.physical.get_raw_connection.return_value.driver_connection = object()
                    case _:
                        current._readonly = False
    assert current._active is False


async def test_publisher_entrypoint_still_refuses_admin_origin(retained):
    state, case = retained, retained.case
    with pytest.raises(ValueError, match="original_publisher_required"):
        async with case.session.begin(), case.database.bind_existing_session(case.session):
            async with workspace.context.observe_prepared_cms_inputs(
                case.database,
                case.session,
                case.driver,
                source_binding=case.storage,
                source_policy=case.source_policy,
                prepared_policy=case.policy,
                frozen_inventory=case.inventory,
            ):
                pytest.fail("Publisher factory accepted the admin")


async def test_drift_between_initial_observation_and_lock_is_refused_before_yield(retained):
    state, case = retained, retained.case

    async def replace_input(_sql):
        case.native_rows[0]["relfilenode"] = 999

    case.driver.execute.side_effect = replace_input
    with pytest.raises(ValueError, match="frozen_inventory_changed"):
        async with case.session.begin(), case.database.bind_existing_session(case.session), open_workspace(state):
            pytest.fail("post-lock input drift reached the reader")
    case.driver.execute.assert_awaited_once()


async def test_lock_conflict_propagates_without_repair_or_retry(retained):
    state, case = retained, retained.case
    failure = RuntimeError("synthetic NOWAIT refusal")
    case.driver.execute.side_effect = failure
    with pytest.raises(RuntimeError, match="synthetic NOWAIT refusal") as caught:
        async with case.session.begin(), case.database.bind_existing_session(case.session), open_workspace(state):
            pytest.fail("failed lock reached the reader")
    assert caught.value is failure
    case.driver.execute.assert_awaited_once()


@pytest.mark.parametrize("error", [RuntimeError("caller failure"), asyncio.CancelledError()])
async def test_caller_failure_or_cancellation_ends_view_without_cleanup_sql(retained, error):
    state, case = retained, retained.case
    with pytest.raises(type(error)):
        async with case.session.begin(), case.database.bind_existing_session(case.session):
            async with open_workspace(state) as current:
                reads = case.driver.fetchrow.await_count
                raise error
    assert current._active is False
    assert case.driver.fetchrow.await_count == reads
    assert not case.session.in_transaction()


async def test_original_borrowed_session_and_task_are_required(retained):
    state, case = retained, retained.case
    async with case.session.begin():
        with pytest.raises(ValueError, match="borrowed_session_required"):
            async with open_workspace(state):
                pytest.fail("unbound session was accepted")
    async with case.session.begin(), case.database.bind_existing_session(case.session):
        async with open_workspace(state) as current:
            with pytest.raises(ValueError, match="original_task_required"):
                await asyncio.create_task(current.verify_current())
        async with case.session.begin_nested():
            with pytest.raises(ValueError, match="original_transaction_required"):
                async with open_workspace(state):
                    pytest.fail("nested session was accepted")
