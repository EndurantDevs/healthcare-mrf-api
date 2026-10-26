# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete proof-batch custody replaces only the retired control hooks."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from db.connection import ConnectionProxy
from process import provider_directory_proof_store as store
from process import provider_directory_source_proof_control as control
from process.provider_directory_generic_candidate import GenericCandidateError
from tests.test_provider_directory_generic_source_storage import _case


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["ensure", "persist", "delete", "delete_resource"])
async def test_every_proof_writer_refuses_ambient_executor_before_any_target_io(operation):
    executor = SimpleNamespace(status=AsyncMock(), first=AsyncMock(), all=AsyncMock())
    calls_by_operation = {
        "ensure": lambda: store.ensure_dataset_proof_shard_table(executor, "source_schema"),
        "persist": lambda: store.persist_dataset_proof_shard(executor, "source_schema", [], dataset_id="dataset"),
        "delete": lambda: store.delete_dataset_proof_shards(executor, "source_schema", "dataset"),
        "delete_resource": lambda: store.delete_dataset_resource_proof_shards(
            executor, "source_schema", "dataset", "Organization"
        ),
    }
    with pytest.raises(GenericCandidateError, match="publisher_connection_required"):
        await calls_by_operation[operation]()
    executor.status.assert_not_awaited()
    executor.first.assert_not_awaited()
    executor.all.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [None, "session", "driver", "schema", "executor"])
async def test_proof_executor_is_original_same_task_owner_before_native_custody(monkeypatch, change):
    session, native = object(), object()
    publisher = SimpleNamespace(connection=native, policy=SimpleNamespace(schema="source_schema"))
    monkeypatch.setattr(control, "bound_publisher", lambda: publisher)
    monkeypatch.setattr(control, "source_copy_session", lambda: session)
    driver = AsyncMock(return_value=object() if change == "driver" else native)
    monkeypatch.setattr(control, "_copy_driver", driver)
    custody = AsyncMock()
    monkeypatch.setattr(control, "verify_proof_control", custody)
    executor = ConnectionProxy(None, object() if change == "session" else session, None)
    if change == "executor":
        executor = SimpleNamespace(_connection=session)
    if change is None:
        assert await control.require_proof_executor(executor, "source_schema") is publisher
        custody.assert_awaited_once_with(native, publisher.policy)
    else:
        with pytest.raises(GenericCandidateError, match="proof_transaction_changed"):
            await control.require_proof_executor(executor, "other" if change == "schema" else "source_schema")
        custody.assert_not_awaited()


@pytest.mark.asyncio
async def test_private_proof_executor_uses_issued_storage_with_the_original_catalog_locator(monkeypatch):
    from process import provider_directory_generic_source_policy as policy
    from tests.test_provider_directory_generic_source_policy import _isolated_factory

    factory = _isolated_factory()
    session, native = object(), object()
    publisher = SimpleNamespace(connection=native, policy=factory.policy, storage_binding=factory.storage_binding)
    monkeypatch.setattr(control, "bound_publisher", lambda: publisher)
    monkeypatch.setattr(policy, "bound_publisher", lambda: publisher)
    monkeypatch.setattr(control, "source_copy_session", lambda: session)
    monkeypatch.setattr(control, "_copy_driver", AsyncMock(return_value=native))
    custody = AsyncMock()
    monkeypatch.setattr(control, "verify_proof_control", custody)
    executor = ConnectionProxy(None, session, None)
    assert await control.require_proof_executor(executor, "catalog_schema") is publisher
    custody.assert_awaited_once_with(native, factory.policy, storage_binding=factory.storage_binding)
    with pytest.raises(GenericCandidateError, match="proof_transaction_changed"):
        await control.require_proof_executor(executor, "source_schema")
    assert custody.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [None, "foreign_key", "hook", "check", "namespace", "reader_access"])
async def test_isolated_proof_control_rejects_foreign_keys_and_changed_native_custody(monkeypatch, change):
    from tests.test_provider_directory_generic_source_policy import _isolated_factory
    from tests.test_provider_directory_proof_store_contract_shape import _proof_control_catalog

    factory = _isolated_factory()
    binding = factory.storage_binding
    catalog, _ = _proof_control_catalog()
    catalog.update(nspname=binding.storage_schema, relowner=binding.owner_oid, inert=True)
    if change != "foreign_key":
        catalog["constraints"] = [entry for entry in catalog["constraints"] if entry["type"] != "f"]
    if change == "hook":
        catalog["inert"] = False
    if change == "check":
        catalog["constraints"][-1]["definition"] = "CHECK (true)"
    coordinates_by_field = {
        field: getattr(binding, field)
        for field in ("database_oid", "catalog_schema_oid", "storage_schema_oid", "owner_oid", "namespace_owner_oid")
    }
    if change == "namespace":
        coordinates_by_field["namespace_owner_oid"] += 1
    connection = SimpleNamespace(
        is_in_transaction=Mock(return_value=True),
        fetchrow=AsyncMock(return_value=coordinates_by_field),
        fetch=AsyncMock(return_value=[catalog]),
        fetchval=AsyncMock(return_value=change != "reader_access"),
    )
    monkeypatch.setattr(control, "storage_roles", AsyncMock(return_value=(binding.owner_oid, [3])))
    if change is None:
        assert await control.verify_proof_control(connection, factory.policy, storage_binding=binding) == catalog
        connection.fetch.assert_awaited_once()
        assert connection.fetch.await_args.args[1] == [f'"source_schema"."{control.PROOF_ROOT}"']
        assert connection.fetch.await_args.args[3] == []
    else:
        with pytest.raises(
            GenericCandidateError,
            match="constraints_changed|hooks_changed|storage_identity_changed|private_reader_access",
        ):
            await control.verify_proof_control(connection, factory.policy, storage_binding=binding)
        if change == "namespace":
            connection.fetch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status", ["acquiring", "validated", "published", "failed", "acquisition_abandoned", "acquisition_retired"]
)
async def test_whole_batch_delete_checks_exact_parent_and_terminal_immutability(status):
    _, _, parent = _case()
    parent["status"] = status
    publisher = SimpleNamespace(
        connection=SimpleNamespace(fetchrow=AsyncMock(return_value=parent)),
        policy=SimpleNamespace(schema="source_schema"),
    )
    if status in {"acquisition_abandoned", "acquisition_retired"}:
        with pytest.raises(GenericCandidateError, match="proof_parent_immutable"):
            await control.lock_proof_parent(publisher, "dataset", acquiring=False)
    else:
        assert await control.lock_proof_parent(publisher, "dataset", acquiring=False) == parent
    publisher.connection.fetchrow.assert_awaited_once()
    assert "FOR SHARE" in publisher.connection.fetchrow.await_args.args[0]


@pytest.mark.parametrize("change", ["dataset", "endpoint", "root", "metadata", "phase"])
def test_proof_batch_cannot_rebind_any_candidate_identity_coordinate(change):
    _, _, parent = _case()
    if change == "phase":
        parent["candidate_storage_json"]["phase"] = "prepared"
    elif change == "metadata":
        parent["publication_metadata_json"] = {"source_ids": ["other"]}
    else:
        field = {"dataset": "dataset_id", "endpoint": "endpoint_id", "root": "acquisition_root_run_id"}[change]
        parent["candidate_storage_json"]["identity"][field] = "other"
    with pytest.raises(GenericCandidateError, match="binding_changed|tables_changed|proof_identity_changed"):
        control._require_proof_identity(parent, "dataset", acquiring=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [None, "extra", "missing", "function", "event"])
async def test_only_complete_exact_six_obsolete_proof_hooks_are_retired(change):
    hooks = [
        dict(tgname=name, tgtype=event, proname=function, nspname="source_schema", tgenabled="A")
        for name, (event, function) in control.PROOF_HOOKS.items()
    ]
    if change == "extra":
        hooks.append({**hooks[0], "tgname": "unreviewed"})
    elif change == "missing":
        hooks.pop()
    elif change is not None:
        hooks[0]["proname" if change == "function" else "tgtype"] = "other" if change == "function" else 5
    connection = SimpleNamespace(fetch=AsyncMock(return_value=deepcopy(hooks)), execute=AsyncMock())
    if change is None:
        await control.retire_proof_control_hooks(connection, SimpleNamespace(schema="source_schema"))
        assert connection.execute.await_count == 6
        assert all("DROP TRIGGER" in call.args[0] for call in connection.execute.await_args_list)
    else:
        with pytest.raises(GenericCandidateError, match="proof_control_legacy_hooks_changed"):
            await control.retire_proof_control_hooks(connection, SimpleNamespace(schema="source_schema"))
        connection.execute.assert_not_awaited()
