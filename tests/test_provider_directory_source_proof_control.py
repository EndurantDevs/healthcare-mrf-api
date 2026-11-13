# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete proof-batch custody replaces only the retired control hooks."""

from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from db.connection import ConnectionProxy, Database
from process import provider_directory_proof_store as store
from process import provider_directory_source_proof_control as control
from process.provider_directory_generic_candidate import GenericCandidateError
from tests.test_provider_directory_generic_source_storage import _case


def _named_proof_parent(factory):
    """Create a logical loading candidate with ordinary source and lineage metadata."""
    metadata = {"source_ids": ["source-a"], "selected_resources": ["Organization"]}
    identity_by_field = {
        "dataset_id": "dataset-proof",
        "endpoint_id": "endpoint-proof",
        "acquisition_root_run_id": "root-proof",
        "metadata": metadata,
    }
    binding_by_field = {
        "contract": "generic-source-detached-by-name-v1",
        "identity": identity_by_field,
        "phase": "loading",
        "ordinal": 0,
        "resets": {},
        "auxiliary": None,
        "relationship_sha256": None,
        "tables": {
            kind: {"name": "pdgs_" + "a" * 32 + suffix}
            for kind, suffix in (("resource_input", "_ri"), ("witness_input", "_wi"))
        },
        "storage": {
            "catalog_schema": "catalog_schema",
            "storage_schema": "source_schema",
            "producer": list(factory.producer_identity),
        },
        "admission": None,
    }
    return {
        **identity_by_field,
        "status": "acquiring",
        "is_current": False,
        "publication_metadata_json": metadata,
        "candidate_storage_json": binding_by_field,
    }


@pytest.fixture
def named_proof_scope(monkeypatch):
    """Use the genuine Source task and logical candidate checks with in-memory SQL results."""
    from process import provider_directory_generic_source_policy as policy
    from tests.test_provider_directory_generic_source_policy import _connection, _named_factory
    from tests.test_provider_directory_proof_store import _MemoryProofConnection

    factory = _named_factory()
    native, _ = _connection()
    memory = _MemoryProofConnection()
    parent_by_field = _named_proof_parent(factory)
    native.fetchrow = AsyncMock(side_effect=lambda *arguments: deepcopy(parent_by_field))

    async def execute(statement, parameters):
        query = str(statement).lstrip()
        if query.startswith("DELETE"):
            memory.shards.clear()
        elif query.startswith("INSERT"):
            await memory.status(statement, **parameters)
        row = await memory.first(statement, **parameters) if query.startswith("SELECT") else None
        return SimpleNamespace(rowcount=1, first=lambda: row)

    session = SimpleNamespace(execute=AsyncMock(side_effect=execute), info={})
    driver_lookup = AsyncMock(side_effect=AssertionError("named proof identity lookup"))
    monkeypatch.setattr(control, "_copy_driver", driver_lookup)

    @asynccontextmanager
    async def scope():
        async with policy.bind_generic_source_transaction_factory(factory):
            async with policy.bind_generic_source_publisher(
                native, factory.policy, verify_role_profile=AsyncMock(), transaction_isolation="repeatable read"
            ) as publisher:
                token = policy._OWNER.set((publisher, session))
                try:
                    yield SimpleNamespace(session=session, native=native, memory=memory, parent=parent_by_field)
                finally:
                    policy._OWNER.reset(token)
        driver_lookup.assert_not_awaited()

    return scope


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["persist", "built", "delete", "delete_resource"])
async def test_named_proof_writers_route_unrelated_executor_to_source_session(named_proof_scope, operation):
    from tests.test_provider_directory_proof_store import _dataset_resource

    resource_rows = [_dataset_resource("Organization", "organization", {"npi": "123"})]
    unrelated = ConnectionProxy(None, object(), None)
    unrelated.status = AsyncMock(side_effect=AssertionError("unrelated proof writer used"))
    unrelated.first = AsyncMock(side_effect=AssertionError("unrelated proof reader used"))
    async with named_proof_scope() as case:
        if operation == "built":
            descriptor, compressed = store.build_dataset_proof_shard(
                resource_rows,
                dataset_id="dataset-proof",
                endpoint_id="endpoint-proof",
                acquisition_root_run_id="root-proof",
                source_ids=["source-a"],
            )
            await store.persist_built_dataset_proof_shard(
                unrelated, "catalog_schema", descriptor, compressed, dataset_id="dataset-proof"
            )
        else:
            await store.persist_dataset_proof_shard(
                unrelated, "catalog_schema", resource_rows, dataset_id="dataset-proof"
            )
            if operation == "delete":
                await store.delete_dataset_proof_shards(unrelated, "catalog_schema", "dataset-proof")
            elif operation == "delete_resource":
                await store.delete_dataset_resource_proof_shards(
                    unrelated, "catalog_schema", "dataset-proof", "Organization"
                )
        assert bool(case.memory.shards) == (operation in {"persist", "built"})
        assert case.session.execute.await_count >= 2
        assert '"source_schema"."provider_directory_endpoint_dataset"' in case.native.fetchrow.await_args.args[0]
        assert "FOR SHARE" in case.native.fetchrow.await_args.args[0]
        unrelated.status.assert_not_awaited()
        unrelated.first.assert_not_awaited()


@pytest.mark.asyncio
async def test_named_proof_routing_keeps_parent_lineage_before_any_write(named_proof_scope):
    from tests.test_provider_directory_proof_store import _dataset_resource

    async with named_proof_scope() as case:
        case.parent["endpoint_id"] = "changed-endpoint"
        with pytest.raises(GenericCandidateError, match="proof_identity_changed"):
            await store.persist_dataset_proof_shard(
                object(),
                "catalog_schema",
                [_dataset_resource("Organization", "organization", {})],
                dataset_id="dataset-proof",
            )
        case.session.execute.assert_not_awaited()
        assert case.memory.shards == {}


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
@pytest.mark.parametrize("executor_kind", ["proxy", "database"])
async def test_proof_executor_is_original_same_task_owner_before_native_custody(monkeypatch, change, executor_kind):
    session, native = object(), object()
    publisher = SimpleNamespace(connection=native, policy=SimpleNamespace(schema="source_schema"))
    monkeypatch.setattr(control, "bound_publisher", lambda: publisher)
    monkeypatch.setattr(control, "source_copy_session", lambda: session)
    driver = AsyncMock(return_value=object() if change == "driver" else native)
    monkeypatch.setattr(control, "_copy_driver", driver)
    custody = AsyncMock()
    monkeypatch.setattr(control, "verify_proof_control", custody)
    supplied_session = object() if change == "session" else session
    if executor_kind == "database":
        executor = Database()
        monkeypatch.setattr(executor, "_transaction_binding", lambda: SimpleNamespace(session=supplied_session))
    else:
        executor = ConnectionProxy(None, supplied_session, None)
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
@pytest.mark.parametrize("named", [False, True])
async def test_private_proof_executor_uses_issued_storage_with_the_original_catalog_locator(monkeypatch, named):
    from process import provider_directory_generic_source_policy as policy
    from tests.test_provider_directory_generic_source_policy import _isolated_factory, _named_factory

    factory = _named_factory() if named else _isolated_factory()
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
    with pytest.raises(GenericCandidateError, match="proof_transaction_changed"):
        await control.require_proof_executor(executor, "source_schema")
    if named:
        control._copy_driver.assert_not_awaited()
        custody.assert_not_awaited()
    else:
        custody.assert_awaited_once_with(native, factory.policy, storage_binding=factory.storage_binding)


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
