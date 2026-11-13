# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named proof batches retain exact data, layout and transaction behavior."""

import asyncio
import json
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest

from db.connection import ConnectionProxy
from process import provider_directory_generic_source_policy as source
from process import provider_directory_generic_source_storage as storage
from process import provider_directory_proof_store as store
from process import provider_directory_source_proof_control as control
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_resource_hash import LEGACY_RESOURCE_HASH_CONTRACT
from tests.test_provider_directory_generic_source_policy import _connection, _isolated_factory, _named_factory
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
@pytest.mark.parametrize("factory_builder", [_isolated_factory, _named_factory])
async def test_named_proof_executor_uses_declared_catalog_without_capture(monkeypatch, factory_builder):
    factory = factory_builder()
    native = SimpleNamespace(is_in_transaction=lambda: True)
    publisher = SimpleNamespace(connection=native, policy=factory.policy, storage_binding=factory.storage_binding)
    monkeypatch.setattr(control, "bound_publisher", lambda: publisher)
    verify_layout = AsyncMock(side_effect=AssertionError("named batch captured catalog layout"))
    monkeypatch.setattr(control, "verify_proof_control", verify_layout)
    for executor in (object(), SimpleNamespace(_connection=object())):
        assert await control.require_proof_executor(executor, "catalog_schema") is publisher
    with pytest.raises(GenericCandidateError, match="proof_transaction_changed"):
        await control.require_proof_executor(object(), "source_schema")
    verify_layout.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["transaction", "catalog", "payload", "binding_shape"])
async def test_proof_executor_refuses_changed_transaction_or_declared_scope(monkeypatch, change):
    factory = _named_factory()
    publisher = SimpleNamespace(
        connection=SimpleNamespace(is_in_transaction=lambda: change != "transaction"),
        policy=factory.policy,
        storage_binding=factory.storage_binding,
    )
    if change == "payload":
        publisher.policy = SimpleNamespace(schema="other_schema")
    elif change == "binding_shape":
        publisher.storage_binding = SimpleNamespace(**vars(factory.storage_binding))
    monkeypatch.setattr(control, "bound_publisher", lambda: publisher)
    with pytest.raises(GenericCandidateError, match="proof_transaction_changed|storage_changed"):
        await control.require_proof_executor(object(), "other_catalog" if change == "catalog" else "catalog_schema")


def _proof_layout(*, isolated):
    constraints = [dict(name=control.PROOF_ROOT + "_pkey", type="p", columns=[1, 2], valid=True)]
    if not isolated:
        constraints.append(dict(name=control.PROOF_ROOT + "_dataset_fkey", type="f", columns=[1], valid=True))
    constraints.extend(
        dict(name=name, type="c", definition=definition, valid=True) for name, definition in control._CHECKS.items()
    )
    return dict(
        columns=[[name, kind, True, "now()" if name == "created_at" else None] for name, kind in control._COLUMNS],
        constraints=constraints,
        indexes=[
            dict(valid=True, native=True, method="btree", predicate=None, expression=None, keys=keys)
            for keys in (["dataset_id", "shard_id"], ["dataset_id", "acquisition_root_run_id", "shard_id"])
        ],
    )


def _proof_foreign_key_rows():
    return [
        dict(
            tgtype=event,
            proname=function,
            tgisinternal=True,
            tgenabled="O",
            function_schema="pg_catalog",
            conname=control.PROOF_ROOT + "_dataset_fkey",
            contype="f",
            convalidated=True,
            condeferrable=False,
            confupdtype="a",
            confdeltype="c",
            confmatchtype="s",
            conkey=[1],
            exact_parent=True,
            target_columns=["dataset_id"],
            has_rules=False,
        )
        for event, function in ((5, "RI_FKey_check_ins"), (17, "RI_FKey_check_upd"))
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("isolated", [False, True])
@pytest.mark.parametrize("encoded", [False, True])
async def test_proof_layout_returns_only_named_logical_fields(isolated, encoded):
    factory = _named_factory()
    layout = _proof_layout(isolated=isolated)
    returned_by_field = {key: json.dumps(value) if encoded else value for key, value in layout.items()}
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        execute=AsyncMock(),
        fetchrow=AsyncMock(return_value=returned_by_field),
        fetch=AsyncMock(return_value=_proof_foreign_key_rows()),
    )
    result = await control.verify_proof_control(
        connection, factory.policy, storage_binding=factory.storage_binding if isolated else None
    )
    assert result == {"schema": "source_schema", "name": control.PROOF_ROOT, **layout}
    relation = '"source_schema"."' + control.PROOF_ROOT + '"'
    connection.execute.assert_awaited_once_with(f"SELECT dataset_id FROM {relation} LIMIT 0")
    connection.fetchrow.assert_awaited_once_with(control._PROOF_LAYOUT_SQL, relation)
    if isolated:
        connection.fetch.assert_not_awaited()
    else:
        assert connection.fetch.await_args.args[1:] == (relation, '"source_schema"."' + control.HEADER + '"')
    for forbidden in ("relowner", "relacl", "pg_roles", "pg_database", "session_user", "has_table_privilege"):
        assert forbidden not in control._PROOF_LAYOUT_SQL


@pytest.mark.parametrize(
    "section,index,field,value",
    [
        ("columns", 0, 2, False),
        ("constraints", 0, "type", "f"),
        ("constraints", -1, "definition", "CHECK (true)"),
        ("constraints", 0, "columns", [2, 1]),
        ("indexes", 0, "keys", ["shard_id", "dataset_id"]),
        ("indexes", 0, "valid", False),
        ("indexes", 0, "native", False),
    ],
)
def test_proof_layout_preserves_fixed_columns_checks_and_native_keys(section, index, field, value):
    layout = _proof_layout(isolated=True)
    layout[section][index][field] = value
    with pytest.raises(GenericCandidateError, match="columns_changed|constraints_changed|indexes_changed"):
        control._require_proof_layout(layout, isolated=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("tgtype", 9),
        ("proname", "other"),
        ("tgenabled", "D"),
        ("convalidated", False),
        ("condeferrable", True),
        ("confdeltype", "r"),
        ("confupdtype", "c"),
        ("confmatchtype", "f"),
        ("conkey", [2]),
        ("exact_parent", False),
        ("target_columns", ["endpoint_id"]),
        ("has_rules", True),
    ],
)
async def test_existing_parent_relationship_preserves_all_native_semantics(field, value):
    hooks = _proof_foreign_key_rows()
    hooks[0][field] = value
    connection = SimpleNamespace(fetch=AsyncMock(return_value=hooks))
    with pytest.raises(GenericCandidateError, match="proof_control_hooks_changed"):
        await control._require_batch_foreign_key(connection, "source_schema", '"source_schema"."proof"')
    assert "WHERE t.tgrelid=to_regclass($1)" in connection.fetch.await_args.args[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["access", "layout"])
@pytest.mark.parametrize("failure", [PermissionError("denied"), asyncio.CancelledError()])
async def test_proof_layout_preserves_access_and_cancel_failure(stage, failure):
    connection = SimpleNamespace(is_in_transaction=lambda: True, execute=AsyncMock(), fetchrow=AsyncMock())
    (connection.execute if stage == "access" else connection.fetchrow).side_effect = failure
    with pytest.raises(type(failure)) as raised:
        await control.verify_proof_control(connection, _named_factory().policy)
    assert raised.value is failure
    if stage == "access":
        connection.fetchrow.assert_not_awaited()


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


def _named_proof_parent(factory):
    _, _, parent = _case()
    metadata = {
        "source_ids": ["source-a"],
        "selected_resources": ["Organization"],
        "resource_hash_contract": LEGACY_RESOURCE_HASH_CONTRACT,
    }
    parent["publication_metadata_json"] = metadata
    binding = parent["candidate_storage_json"]
    binding["identity"]["metadata"] = deepcopy(metadata)
    binding.update(contract=storage.NAMED_ISOLATED_CONTRACT, storage=vars(factory.storage_binding), admission=None)
    return parent


@asynccontextmanager
async def _named_proof_writer():
    factory = _named_factory()
    parent = _named_proof_parent(factory)
    resources = [
        dict(
            dataset_id="dataset",
            resource_type="Organization",
            resource_id=str(index),
            payload_hash="a" * 64,
            payload_json={"name": str(index)},
        )
        for index in range(2)
    ]
    descriptor, compressed = store.build_dataset_proof_shard(
        resources, dataset_id="dataset", endpoint_id="endpoint", acquisition_root_run_id="root", source_ids=["source-a"]
    )
    native, transitions = _connection()

    async def fetchrow(query, *arguments):
        if "candidate_storage_json FROM" in query:
            assert arguments == ("dataset",)
            return deepcopy(parent)
        assert arguments == ("dataset", descriptor["shard_id"])
        return store._expected_persisted_shard_fields(descriptor, compressed)

    native.fetchrow.side_effect = fetchrow
    session = SimpleNamespace(raw_connection=SimpleNamespace(driver_connection=native))
    database = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=session))
    state = SimpleNamespace(
        native=native,
        parent=parent,
        resources=resources,
        descriptor=descriptor,
        compressed=compressed,
        transitions=transitions,
        caller=SimpleNamespace(status=AsyncMock(), first=AsyncMock(), all=AsyncMock()),
    )
    async with source.bind_generic_source_transaction_factory(factory):
        async with source.bind_generic_source_publisher(
            native, factory.policy, transaction_isolation="repeatable read"
        ):
            async with source.source_owner_transaction(database):
                yield state


@pytest.mark.asyncio
@pytest.mark.parametrize("prebuilt", [False, True])
async def test_named_proof_batch_uses_bound_writer_for_insert_and_replay(prebuilt):
    async with _named_proof_writer() as state:
        if prebuilt:
            actual = await store.persist_built_dataset_proof_shard(
                state.caller, "catalog_schema", state.descriptor, state.compressed, dataset_id="dataset"
            )
        else:
            actual = await store.persist_dataset_proof_shard(
                state.caller, "catalog_schema", state.resources, dataset_id="dataset"
            )
        assert actual == state.descriptor and actual["resource_count"] == 2
        insert = next(call.args for call in state.native.execute.await_args_list if "INSERT INTO" in call.args[0])
        assert '"source_schema"."' + control.PROOF_ROOT + '"' in insert[0]
        assert "ON CONFLICT (dataset_id, shard_id) DO NOTHING" in insert[0]
        assert insert[1:5] == ("dataset", state.descriptor["shard_id"], "endpoint", "root")
        assert json.loads(insert[5]) == ["source-a"] and insert[6] == 2
        assert json.loads(insert[7]) == {"Organization": 2} and insert[-1] == state.compressed
        assert state.native.fetchrow.await_count == 2
        assert "FOR SHARE" in state.native.fetchrow.await_args_list[0].args[0]
        assert '"source_schema"."' + control.PROOF_ROOT + '"' in state.native.fetchrow.await_args_list[1].args[0]
        state.caller.status.assert_not_awaited()
        state.caller.first.assert_not_awaited()
    assert state.transitions == ["savepoint", "release"]


@pytest.mark.asyncio
async def test_native_proof_replay_accepts_driver_record_mapping_protocol():
    async with _named_proof_writer() as state:
        original = state.native.fetchrow.side_effect
        expected_by_field = store._expected_persisted_shard_fields(state.descriptor, state.compressed)
        driver_record = MagicMock(spec=asyncpg.Record)
        driver_record.keys.return_value = list(expected_by_field)
        driver_record.__getitem__.side_effect = expected_by_field.__getitem__

        async def fetchrow(query, *arguments):
            return driver_record if "payload_bytes" in query else await original(query, *arguments)

        state.native.fetchrow.side_effect = fetchrow
        assert (
            await store.persist_built_dataset_proof_shard(
                state.caller, "catalog_schema", state.descriptor, state.compressed, dataset_id="dataset"
            )
            == state.descriptor
        )
    assert state.transitions == ["savepoint", "release"]


def _changed_proof_descriptor(state, change):
    descriptor_by_field = deepcopy(state.descriptor)
    if change == "source":
        state.parent["publication_metadata_json"]["source_ids"] = ["other"]
    elif change == "sealed":
        state.parent["status"] = "validated"
    elif change == "endpoint":
        descriptor_by_field["endpoint_id"] = "other"
    elif change == "artifact":
        descriptor_by_field["artifact_byte_count"] += 1
    else:
        original = state.native.fetchrow.side_effect

        async def changed_replay(query, *arguments):
            result = await original(query, *arguments)
            return {**result, "resource_count": 99} if "payload_bytes" in query else result

        state.native.fetchrow.side_effect = changed_replay
    return descriptor_by_field


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["source", "sealed", "endpoint", "artifact", "replay"])
async def test_named_proof_batch_refuses_changed_data_and_rolls_back(change):
    with pytest.raises((GenericCandidateError, store.ProviderDirectoryProofStoreError)):
        async with _named_proof_writer() as state:
            descriptor_by_field = _changed_proof_descriptor(state, change)
            await store.persist_built_dataset_proof_shard(
                state.caller, "catalog_schema", descriptor_by_field, state.compressed, dataset_id="dataset"
            )
    assert state.transitions == ["savepoint", "rollback"]
    inserts = [call for call in state.native.execute.await_args_list if "INSERT INTO" in call.args[0]]
    assert len(inserts) == (1 if change == "replay" else 0)
    state.caller.status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("resource_only", [False, True])
async def test_named_proof_delete_uses_bound_writer_and_locked_parent(resource_only):
    async with _named_proof_writer() as state:
        if resource_only:
            await store.delete_dataset_resource_proof_shards(state.caller, "catalog_schema", "dataset", "Organization")
        else:
            await store.delete_dataset_proof_shards(state.caller, "catalog_schema", "dataset")
        statement = next(call.args for call in state.native.execute.await_args_list if "DELETE FROM" in call.args[0])
        assert '"source_schema"."' + control.PROOF_ROOT + '"' in statement[0]
        assert statement[1:] == (("dataset", "Organization") if resource_only else ("dataset",))
        assert ("resource_counts_json ?" in statement[0]) is resource_only
        assert "FOR SHARE" in state.native.fetchrow.await_args.args[0]
        state.caller.status.assert_not_awaited()
    assert state.transitions == ["savepoint", "release"]


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["parent", "insert", "replay"])
@pytest.mark.parametrize("failure", [PermissionError("denied"), asyncio.CancelledError()])
async def test_named_proof_failure_preserves_error_and_transaction_rollback(stage, failure):
    with pytest.raises(type(failure)) as raised:
        async with _named_proof_writer() as state:
            original = state.native.fetchrow.side_effect

            async def fetchrow(query, *arguments):
                if (
                    stage == "parent"
                    and "candidate_storage_json FROM" in query
                    or stage == "replay"
                    and "payload_bytes" in query
                ):
                    raise failure
                return await original(query, *arguments)

            async def execute(query, *arguments):
                if stage == "insert" and "INSERT INTO" in query:
                    raise failure

            state.native.fetchrow.side_effect = fetchrow
            state.native.execute.side_effect = execute
            await store.persist_dataset_proof_shard(
                state.caller, "catalog_schema", state.resources, dataset_id="dataset"
            )
    assert raised.value is failure
    assert state.transitions == ["savepoint", "rollback"]
    state.caller.status.assert_not_awaited()
    state.caller.first.assert_not_awaited()


def _historical_proof_batch():
    descriptor_by_field, compressed = store.build_dataset_proof_shard(
        [
            dict(
                dataset_id="historical",
                resource_type="Organization",
                resource_id="organization",
                payload_hash="a" * 64,
                payload_json={"name": "Example"},
            )
        ],
        dataset_id="historical",
        endpoint_id="endpoint",
        acquisition_root_run_id="run",
        source_ids=["source"],
    )
    native = SimpleNamespace(
        is_in_transaction=lambda: True,
        execute=AsyncMock(),
        fetchrow=AsyncMock(return_value=store._expected_persisted_shard_fields(descriptor_by_field, compressed)),
    )
    return native, descriptor_by_field, compressed


@pytest.mark.asyncio
async def test_historical_proof_batch_uses_native_transaction_without_publisher():
    native, descriptor_by_field, compressed = _historical_proof_batch()
    assert source._PUBLISHER.get() is None
    assert (
        await store._store_dataset_proof_shard(
            native, "historical_schema", "historical", descriptor_by_field, compressed, None
        )
        == descriptor_by_field
    )
    native.execute.assert_awaited_once()
    native.fetchrow.assert_awaited_once()
    assert '"historical_schema"."' + control.PROOF_ROOT + '"' in native.execute.await_args.args[0]
    assert native.fetchrow.await_args.args[1:] == ("historical", descriptor_by_field["shard_id"])
    assert source._PUBLISHER.get() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["inactive", "scope", "replay", "permission", "cancel"])
async def test_historical_proof_batch_retains_transaction_scope_and_error_checks(change):
    native, descriptor_by_field, compressed = _historical_proof_batch()
    proof_resource_types = None
    if change == "inactive":
        native.is_in_transaction = lambda: False
    elif change == "scope":
        proof_resource_types = ["Location"]
    elif change == "replay":
        native.fetchrow.return_value["resource_count"] = 2
    elif change == "permission":
        native.execute.side_effect = PermissionError("denied")
    else:
        native.fetchrow.side_effect = asyncio.CancelledError()
    with pytest.raises((store.ProviderDirectoryProofStoreError, PermissionError, asyncio.CancelledError)):
        await store._store_dataset_proof_shard(
            native, "historical_schema", "historical", descriptor_by_field, compressed, proof_resource_types
        )
    if change in {"inactive", "scope"}:
        native.execute.assert_not_awaited()
        native.fetchrow.assert_not_awaited()
    assert source._PUBLISHER.get() is None


@pytest.fixture
def named_proof_scope():
    """Separate the logical parent reader from the current native batch writer."""
    from tests.test_provider_directory_proof_store import _MemoryProofConnection

    factory = _named_factory()
    native, _ = _connection()
    memory = _MemoryProofConnection()
    parent = _named_proof_parent(factory)
    native.fetchrow = AsyncMock(side_effect=lambda *arguments: deepcopy(parent))
    fields = (
        "dataset_id",
        "shard_id",
        "endpoint_id",
        "acquisition_root_run_id",
        "source_ids_json",
        "resource_count",
        "resource_counts_json",
        "first_identity_json",
        "last_identity_json",
        "input_sha256",
        "artifact_sha256",
        "artifact_byte_count",
        "payload_bytes",
    )

    async def execute(query, *values):
        if query.lstrip().startswith("DELETE"):
            memory.shards.clear()
        else:
            await memory.status(query, **dict(zip(fields, values, strict=True)))

    async def fetchrow(query, dataset_id, shard_id):
        row = await memory.first(query, dataset_id=dataset_id, shard_id=shard_id)
        return {key: value for key, value in row.items() if key != "shard_id"} if row else None

    writer = SimpleNamespace(
        is_in_transaction=lambda: True, execute=AsyncMock(side_effect=execute), fetchrow=AsyncMock(side_effect=fetchrow)
    )
    raw = SimpleNamespace(get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=writer)))
    session = SimpleNamespace(connection=AsyncMock(return_value=raw))

    @asynccontextmanager
    async def scope():
        async with source.bind_generic_source_transaction_factory(factory):
            async with source.bind_generic_source_publisher(
                native, factory.policy, transaction_isolation="repeatable read"
            ) as publisher:
                token = source._OWNER.set((publisher, session))
                try:
                    yield SimpleNamespace(session=session, native=native, writer=writer, memory=memory, parent=parent)
                finally:
                    source._OWNER.reset(token)

    return scope


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["persist", "built", "delete", "delete_resource"])
async def test_named_proof_writers_route_unrelated_executor_to_source_session(named_proof_scope, operation):
    from tests.test_provider_directory_proof_store import _dataset_resource

    resources = [{**_dataset_resource("Organization", "organization", {"npi": "123"}), "dataset_id": "dataset"}]
    unrelated = ConnectionProxy(None, object(), None)
    unrelated.status = AsyncMock(side_effect=AssertionError("unrelated proof writer used"))
    unrelated.first = AsyncMock(side_effect=AssertionError("unrelated proof reader used"))
    async with named_proof_scope() as case:
        if operation == "built":
            descriptor, compressed = store.build_dataset_proof_shard(
                resources,
                dataset_id="dataset",
                endpoint_id="endpoint",
                acquisition_root_run_id="root",
                source_ids=["source-a"],
            )
            await store.persist_built_dataset_proof_shard(
                unrelated, "catalog_schema", descriptor, compressed, dataset_id="dataset"
            )
        else:
            await store.persist_dataset_proof_shard(unrelated, "catalog_schema", resources, dataset_id="dataset")
            if operation == "delete":
                await store.delete_dataset_proof_shards(unrelated, "catalog_schema", "dataset")
            elif operation == "delete_resource":
                await store.delete_dataset_resource_proof_shards(unrelated, "catalog_schema", "dataset", "Organization")
        assert bool(case.memory.shards) == (operation in {"persist", "built"})
        assert case.session.connection.await_count == (2 if operation in {"delete", "delete_resource"} else 1)
        assert case.writer.execute.await_count == (2 if operation in {"delete", "delete_resource"} else 1)
        case.writer.fetchrow.assert_awaited_once()
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
                dataset_id="dataset",
            )
        case.session.connection.assert_not_awaited()
        case.writer.execute.assert_not_awaited()
        assert case.memory.shards == {}
