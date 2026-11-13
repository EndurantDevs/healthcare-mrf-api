# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Batch fences retain both parent coordinates and the original transaction."""

import ast
import asyncio
import importlib
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_source_checkpoint_control as control
from process.provider_directory_generic_candidate import GenericCandidateError
from tests.test_provider_directory_generic_source_policy import _connection, _named_factory

_IDENTITY = {
    "checkpoint_id": "checkpoint",
    "canonical_api_base": "https://example.org/fhir",
    "resource_type": "Organization",
    "source_scope_hash": "a" * 64,
    "strategy_version": "sample-version",
    "acquisition_root_run_id": "root",
    "endpoint_id": "endpoint",
    "dataset_id": "dataset",
    "start_url_hash": "b" * 64,
}


def _publisher(*, existing=(), locked=None, parents=()):
    native = SimpleNamespace(
        fetch=AsyncMock(side_effect=[list(existing), list(parents), list(existing if locked is None else locked)])
    )
    return SimpleNamespace(connection=native, policy=SimpleNamespace(schema="sample"))


@pytest.mark.asyncio
async def test_fence_includes_old_and_new_dataset_and_root_coordinates():
    old_by_field = {
        "checkpoint_id": "checkpoint",
        "dataset_id": "old_by_field-dataset",
        "acquisition_root_run_id": "old_by_field-root",
    }
    publisher = _publisher(existing=[old_by_field], parents=[{"dataset_id": "root-only-parent", "status": "acquiring"}])
    await control.fence_bulk_parents(publisher, ["new-dataset"], ["new-root"], ["checkpoint"])
    calls = publisher.connection.fetch.await_args_list
    sql, datasets, roots = calls[1].args
    assert datasets == ["new-dataset", "old_by_field-dataset"] and roots == ["new-root", "old_by_field-root"]
    assert " OR acquisition_root_run_id=ANY" in sql and 'ORDER BY dataset_id COLLATE "C" FOR SHARE' in sql
    assert "FOR UPDATE" in calls[2].args[0]
    assert len(calls) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["acquisition_abandoned", "acquisition_retired"])
async def test_abandoned_parent_found_only_by_root_refuses_before_checkpoint_mutation(status):
    publisher = _publisher(parents=[{"dataset_id": "another-dataset", "status": status}])
    with pytest.raises(GenericCandidateError, match="checkpoint_parent_abandoned"):
        await control.fence_bulk_parents(publisher, ["new"], ["shared-root"], ["checkpoint"])
    assert publisher.connection.fetch.await_count == 2


@pytest.mark.asyncio
async def test_concurrent_coordinate_change_requires_retry():
    old_by_field = {"checkpoint_id": "checkpoint", "dataset_id": "dataset", "acquisition_root_run_id": "root"}
    changed_by_field = {**old_by_field, "acquisition_root_run_id": "unfenced"}
    publisher = _publisher(existing=[old_by_field], locked=[changed_by_field])
    with pytest.raises(GenericCandidateError, match="checkpoint_parent_changed"):
        await control.fence_bulk_parents(publisher, ["dataset"], ["root"], ["checkpoint"])


@pytest.mark.asyncio
async def test_concurrent_same_identity_insert_remains_replayable():
    inserted_by_field = {"checkpoint_id": "checkpoint", "dataset_id": "dataset", "acquisition_root_run_id": "root"}
    await control.fence_bulk_parents(_publisher(locked=[inserted_by_field]), ["dataset"], ["root"], ["checkpoint"])


@pytest.mark.parametrize("values", ["root", [""], [None], ["r" * 65], ["root"] * 257])
def test_parent_coordinate_shape_and_bounded_size_refuse(values):
    with pytest.raises(GenericCandidateError, match="checkpoint_coordinates_invalid"):
        control._coordinates(values, 64)


@pytest.mark.asyncio
async def test_named_checkpoint_transaction_fences_parent_in_private_schema(monkeypatch):
    factory = _named_factory()
    binding = factory.storage_binding
    connection, _ = _connection()
    connection.fetch = AsyncMock(
        return_value=[{"dataset_id": "dataset", "status": "acquiring", "acquisition_root_run_id": "root"}]
    )
    database, session = object(), object()

    @asynccontextmanager
    async def transaction(executor):
        assert executor is database
        yield

    monkeypatch.setattr(control, "source_transaction", transaction)
    monkeypatch.setattr(
        control, "require_checkpoint_executor", AsyncMock(side_effect=lambda _: control.bound_publisher())
    )
    monkeypatch.setattr(control, "source_copy_session", lambda: session)
    capture = AsyncMock()
    monkeypatch.setattr(control, "capture_checkpoint_controls", capture)
    async with control.source.bind_generic_source_transaction_factory(factory):
        async with control.source.bind_generic_source_publisher(
            connection, factory.policy, verify_role_profile=AsyncMock(), transaction_isolation="repeatable read"
        ):
            with pytest.raises(GenericCandidateError, match="checkpoint_namespace_changed"):
                async with control.source_checkpoint_transaction(database, schema=binding.storage_schema):
                    pytest.fail("private schema used as the catalog locator")
            connection.fetch.assert_not_awaited()
            async with control.source_checkpoint_transaction(
                database, schema=binding.catalog_schema, dataset_ids=["dataset"], root_run_ids=["root"]
            ) as executor:
                assert executor is session
    query, datasets, roots = connection.fetch.await_args.args
    assert '"source_schema"."provider_directory_endpoint_dataset"' in query
    assert "FOR KEY SHARE" in query and datasets == ["dataset"] and roots == ["root"]
    capture.assert_not_awaited()


@pytest.mark.asyncio
async def test_nested_control_scope_reuses_custody_but_not_another_namespace(monkeypatch):
    publisher = _publisher()
    publisher.policy = SimpleNamespace(schema="sample")

    @asynccontextmanager
    async def transaction(database):
        yield

    monkeypatch.setattr(control, "source_transaction", transaction)
    monkeypatch.setattr(control, "require_checkpoint_executor", AsyncMock(return_value=publisher))
    monkeypatch.setattr(control, "source_copy_session", lambda: "original-session")
    capture = AsyncMock()
    monkeypatch.setattr(control, "capture_checkpoint_controls", capture)
    async with control.source_checkpoint_transaction(object(), schema="sample") as session:
        assert session == "original-session"
        async with control.source_checkpoint_transaction(object(), schema="sample"):
            assert capture.await_count == 1
        with pytest.raises(GenericCandidateError, match="checkpoint_namespace_changed"):
            async with control.source_checkpoint_transaction(object(), schema="other"):
                pytest.fail("substituted namespace reached the writer")
    assert control._CUSTODY.get() is None


@pytest.mark.asyncio
async def test_absent_publisher_refuses_before_any_target_access():
    with pytest.raises(GenericCandidateError, match="publisher_connection_required"):
        async with control.source_checkpoint_transaction(object()):
            pytest.fail("ordinary writer reached its target")


@pytest.mark.asyncio
@pytest.mark.parametrize("field", tuple(_IDENTITY))
async def test_bulk_immutable_identity_mismatch_refuses_before_parent_or_payload_write(field):
    checkpoint_by_field = {**_IDENTITY, field: "different"}
    publisher = _publisher(existing=[checkpoint_by_field])
    with pytest.raises(GenericCandidateError, match="checkpoint_identity_changed"):
        await control.fence_bulk_parents(
            publisher, ["dataset"], ["root"], ["checkpoint"], bulk_identity=SimpleNamespace(**_IDENTITY)
        )
    assert publisher.connection.fetch.await_count == 1


@pytest.mark.asyncio
async def test_bulk_identity_rechecks_full_locked_row_without_another_query():
    publisher = _publisher(existing=[_IDENTITY], locked=[{**_IDENTITY, "start_url_hash": "c" * 64}])
    with pytest.raises(GenericCandidateError, match="checkpoint_identity_changed"):
        await control.fence_bulk_parents(
            publisher, ["dataset"], ["root"], ["checkpoint"], bulk_identity=SimpleNamespace(**_IDENTITY)
        )
    assert publisher.connection.fetch.await_count == 3


@pytest.mark.asyncio
async def test_bulk_full_identity_preserves_adoption_and_new_reservation():
    for existing, locked in [([_IDENTITY], None), ([], [_IDENTITY]), ([], [])]:
        publisher = _publisher(existing=existing, locked=locked)
        identity = SimpleNamespace(**_IDENTITY, owner_run_id="new-owner", retry_of_run_id="old-owner")
        await control.fence_bulk_parents(publisher, ["dataset"], ["root"], ["checkpoint"], bulk_identity=identity)
        assert publisher.connection.fetch.await_count == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [None, "missing", "endpoint_id", "acquisition_root_run_id", "retired"])
async def test_private_bulk_parent_must_exist_and_match_before_checkpoint_lock(change):
    parent_by_field = {field: _IDENTITY[field] for field in ("dataset_id", "endpoint_id", "acquisition_root_run_id")}
    parent_by_field["status"] = "acquisition_retired" if change == "retired" else "acquiring"
    if change in {"endpoint_id", "acquisition_root_run_id"}:
        parent_by_field[change] = "other"
    publisher = _publisher(existing=[_IDENTITY], parents=[] if change == "missing" else [parent_by_field])
    publisher.storage_binding = object()
    operation = control.fence_bulk_parents(
        publisher, ["dataset"], ["root"], ["checkpoint"], bulk_identity=SimpleNamespace(**_IDENTITY)
    )
    if change is None:
        await operation
        assert publisher.connection.fetch.await_count == 3
    else:
        with pytest.raises(
            GenericCandidateError,
            match="checkpoint_parent_missing|checkpoint_parent_changed|checkpoint_parent_abandoned",
        ):
            await operation
        assert publisher.connection.fetch.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [None, "missing", "root", "retired", "root_retired"])
async def test_private_pagination_fence_requires_live_parent_and_matching_root(change):
    parents = [{"dataset_id": "dataset", "acquisition_root_run_id": "root", "status": "acquiring"}]
    if change == "missing":
        parents = []
    elif change == "root":
        parents[0]["acquisition_root_run_id"] = "other"
    elif change == "retired":
        parents[0]["status"] = "acquisition_retired"
    elif change == "root_retired":
        parents.append({"dataset_id": "other", "acquisition_root_run_id": "root", "status": "acquisition_retired"})
    publisher = _publisher()
    publisher.connection.fetch = AsyncMock(return_value=parents)
    if change is None:
        await control.fence_checkpoint_dataset_parents(publisher, ["dataset"], ["root"])
    else:
        with pytest.raises(
            GenericCandidateError,
            match="checkpoint_parent_missing|checkpoint_parent_changed|checkpoint_parent_abandoned",
        ):
            await control.fence_checkpoint_dataset_parents(publisher, ["dataset"], ["root"])
    publisher.connection.fetch.assert_awaited_once()
    query, datasets, roots = publisher.connection.fetch.await_args.args
    assert query.endswith('ORDER BY dataset_id COLLATE "C" FOR KEY SHARE')
    assert datasets == ["dataset"] and roots == ["root"]


@pytest.mark.asyncio
async def test_bulk_scope_derives_exact_coordinates_and_passes_original_identity(monkeypatch):
    publisher = _publisher()
    identity = SimpleNamespace(**_IDENTITY)

    @asynccontextmanager
    async def transaction(database):
        yield

    monkeypatch.setattr(control, "source_transaction", transaction)
    monkeypatch.setattr(control, "require_checkpoint_executor", AsyncMock(return_value=publisher))
    monkeypatch.setattr(control, "source_copy_session", lambda: "original-session")
    monkeypatch.setattr(control, "capture_checkpoint_controls", AsyncMock())
    fence = AsyncMock()
    monkeypatch.setattr(control, "fence_bulk_parents", fence)
    async with control.source_checkpoint_transaction(object(), bulk_identity=identity):
        fence.assert_awaited_once_with(publisher, ["dataset"], ["root"], ["checkpoint"], bulk_identity=identity)
    assert control._BULK_IDENTITY.get() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("identity", [SimpleNamespace(), SimpleNamespace(**{**_IDENTITY, "dataset_id": None})])
async def test_incomplete_bulk_identity_refuses_before_native_queries(identity):
    publisher = _publisher()
    with pytest.raises(GenericCandidateError, match="checkpoint_identity_invalid"):
        await control.fence_bulk_parents(publisher, ["dataset"], ["root"], ["checkpoint"], bulk_identity=identity)
    publisher.connection.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_bulk_identity_cannot_be_combined_with_another_parent_scope():
    publisher = _publisher()
    with pytest.raises(GenericCandidateError, match="checkpoint_coordinates_conflict"):
        await control.fence_bulk_parents(
            publisher, ["other-dataset"], ["root"], ["checkpoint"], bulk_identity=SimpleNamespace(**_IDENTITY)
        )
    publisher.connection.fetch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("active", [None, {**_IDENTITY, "dataset_id": "different"}, _IDENTITY])
async def test_connection_child_requires_same_original_immutable_bulk_scope(monkeypatch, active):
    session = SimpleNamespace(in_transaction=lambda: True)
    native = SimpleNamespace(is_in_transaction=lambda: True)
    publisher = SimpleNamespace(connection=native)
    monkeypatch.setattr(control, "bound_publisher", lambda: publisher)
    monkeypatch.setattr(control, "source_copy_session", lambda: session)
    monkeypatch.setattr(control, "_copy_driver", AsyncMock(return_value=native))
    executor = control.ConnectionProxy(None, session, None)
    token = control._BULK_IDENTITY.set(active)
    try:
        if active == _IDENTITY:
            assert (
                await control.require_checkpoint_executor(executor, bulk_identity=SimpleNamespace(**_IDENTITY))
                is publisher
            )
        else:
            with pytest.raises(GenericCandidateError, match="checkpoint_identity_scope_changed"):
                await control.require_checkpoint_executor(executor, bulk_identity=SimpleNamespace(**_IDENTITY))
    finally:
        control._BULK_IDENTITY.reset(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [None, "missing", "dataset_id", "endpoint_id", "resource_type"])
async def test_private_output_writer_fences_its_exact_checkpoint_parent(monkeypatch, change):
    session = SimpleNamespace(in_transaction=lambda: True)
    checkpoint_by_field = {**_IDENTITY}
    if change not in {None, "missing"}:
        checkpoint_by_field[change] = "other"
    native = SimpleNamespace(
        fetchrow=AsyncMock(return_value=None if change == "missing" else checkpoint_by_field),
        is_in_transaction=lambda: True,
    )
    publisher = SimpleNamespace(
        connection=native, policy=SimpleNamespace(schema="private_source"), storage_binding=object()
    )
    monkeypatch.setattr(control, "bound_publisher", lambda: publisher)
    monkeypatch.setattr(control, "source_copy_session", lambda: session)
    monkeypatch.setattr(control, "_copy_driver", AsyncMock(return_value=native))
    executor = control.ConnectionProxy(None, session, None)
    token = control._BULK_IDENTITY.set(_IDENTITY)
    try:
        if change is None:
            assert (
                await control.require_checkpoint_executor(executor, bulk_identity=SimpleNamespace(**_IDENTITY))
                is publisher
            )
        else:
            with pytest.raises(GenericCandidateError, match="checkpoint_parent_missing|checkpoint_identity_changed"):
                await control.require_checkpoint_executor(executor, bulk_identity=SimpleNamespace(**_IDENTITY))
        native.fetchrow.assert_awaited_once()
        query, checkpoint = native.fetchrow.await_args.args
        assert '"private_source"."provider_directory_bulk_acquisition_checkpoint"' in query
        assert query.endswith("FOR SHARE") and checkpoint == _IDENTITY["checkpoint_id"]
    finally:
        control._BULK_IDENTITY.reset(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("executor_kind", ["database", "proxy"])
@pytest.mark.parametrize("inactive", [None, "session", "driver", "publisher"])
async def test_checkpoint_executor_uses_the_actual_active_batch_session(monkeypatch, executor_kind, inactive):
    session = SimpleNamespace(in_transaction=lambda: inactive != "session")
    batch_driver = SimpleNamespace(is_in_transaction=lambda: inactive != "driver")
    publisher = SimpleNamespace(connection=SimpleNamespace(is_in_transaction=lambda: inactive != "publisher"))
    monkeypatch.setattr(control, "bound_publisher", lambda: publisher)
    monkeypatch.setattr(control, "source_copy_session", lambda: SimpleNamespace(in_transaction=lambda: True))
    driver_lookup = AsyncMock(return_value=batch_driver)
    monkeypatch.setattr(control, "_copy_driver", driver_lookup)
    if executor_kind == "database":
        executor = control.Database()
        executor._transaction_binding = lambda: SimpleNamespace(session=session)
    else:
        executor = control.ConnectionProxy(None, session, None)
    if inactive is None:
        assert await control.require_checkpoint_executor(executor) is publisher
        driver_lookup.assert_awaited_once_with(session)
    else:
        with pytest.raises(GenericCandidateError, match="transaction_required"):
            await control.require_checkpoint_executor(executor)


@pytest.mark.asyncio
@pytest.mark.parametrize("owns_lock,owns_row", [(True, True), (False, True), (True, False)])
async def test_source_bulk_guard_keeps_private_reads_off_ordinary_lock_connection(monkeypatch, owns_lock, owns_row):
    directory = importlib.import_module("process.provider_directory_fhir")
    guard = SimpleNamespace(scalar=AsyncMock(return_value=owns_lock))
    publisher_session = SimpleNamespace(scalar=AsyncMock(return_value=owns_row))
    events = []

    @asynccontextmanager
    async def issued_read(actual):
        assert actual is directory
        events.append("enter")
        yield
        events.append("exit")

    monkeypatch.setattr(control.source, "is_source_storage_required", lambda: True)
    monkeypatch.setattr(control, "source_copy_session", lambda: publisher_session)
    monkeypatch.setattr(directory.profile_scope_copy, "source_read_transaction", issued_read)
    monkeypatch.setattr(directory, "_bulk_acquisition_checkpoint_table_ref", lambda: '"private"."checkpoint"')
    identity = SimpleNamespace(checkpoint_id="checkpoint", owner_run_id="owner")
    if owns_lock and owns_row:
        await control.assert_bulk_checkpoint_guard_owner(directory, guard, identity)
    else:
        with pytest.raises(RuntimeError, match="checkpoint_worker_guard_lost|checkpoint_ownership_lost"):
            await control.assert_bulk_checkpoint_guard_owner(directory, guard, identity)
    guard.scalar.assert_awaited_once()
    assert "pg_locks" in str(guard.scalar.await_args.args[0])
    assert events == (["enter", "exit"] if owns_lock else [])
    assert publisher_session.scalar.await_count == int(owns_lock)
    if owns_lock:
        assert 'FROM "private"."checkpoint"' in str(publisher_session.scalar.await_args.args[0])


@pytest.mark.asyncio
@pytest.mark.parametrize("reject", [False, True])
@pytest.mark.parametrize(
    "name,arguments,method",
    [
        ("_load_bulk_export_checkpoint", (SimpleNamespace(checkpoint_id="checkpoint"),), "first"),
        ("_load_bulk_output_checkpoints", ("checkpoint",), "all"),
        ("_fetch_pagination_checkpoint", (SimpleNamespace(**_IDENTITY), "Organization"), "first"),
        (
            "_checkpoint_candidate_dataset_id",
            (SimpleNamespace(**_IDENTITY, retry_of_run_id=None, owner_run_id="owner"), "endpoint", ("Organization",)),
            "first",
        ),
        ("_endpoint_dataset_state", ("dataset",), "first"),
    ],
)
async def test_private_checkpoint_read_entrypoints_borrow_short_source_scope(
    monkeypatch, reject, name, arguments, method
):
    directory = importlib.import_module("process.provider_directory_fhir")
    events = []

    @asynccontextmanager
    async def scope(actual):
        assert actual is directory
        if reject:
            raise RuntimeError("synthetic_scope_refused")
        events.append("enter")
        yield
        events.append("exit")

    async def read(*args, **kwargs):
        assert events == ["enter"]
        return [] if method == "all" else None

    query = AsyncMock(side_effect=read)
    monkeypatch.setattr(directory.profile_scope_copy, "source_read_transaction", scope)
    monkeypatch.setattr(directory.db, method, query)
    if reject:
        with pytest.raises(RuntimeError, match="synthetic_scope_refused"):
            await getattr(directory, name)(*arguments)
        query.assert_not_awaited()
    else:
        await getattr(directory, name)(*arguments)
        query.assert_awaited_once()
        assert events == ["enter", "exit"]


@pytest.mark.parametrize(
    ("source_name", "expected_count_by_helper"),
    [
        ("provider_directory_fhir.py", {"source_checkpoint_transaction": 20, "require_checkpoint_executor": 3}),
        ("provider_directory_bulk_parallel.py", {"source_checkpoint_transaction": 2, "require_checkpoint_executor": 1}),
    ],
)
def test_all_bulk_writer_scopes_and_connection_children_pass_full_identity(source_name, expected_count_by_helper):
    source = Path(control.__file__).with_name(source_name)
    tree = ast.parse(source.read_text())
    count_by_helper = {name: 0 for name in expected_count_by_helper}
    for function in ast.walk(tree):
        if not isinstance(function, ast.AsyncFunctionDef):
            continue
        if source_name == "provider_directory_fhir.py" and "bulk" not in function.name:
            continue
        if not any(argument.arg == "identity" for argument in function.args.args):
            continue
        for call in ast.walk(function):
            if not isinstance(call, ast.Call):
                continue
            helper_name = getattr(call.func, "id", None) or getattr(call.func, "attr", None)
            if helper_name not in count_by_helper:
                continue
            keyword_by_name = {keyword.arg: keyword.value for keyword in call.keywords}
            assert (
                isinstance(keyword_by_name.get("bulk_identity"), ast.Name)
                and keyword_by_name["bulk_identity"].id == "identity"
            )
            assert not {"dataset_ids", "root_run_ids", "checkpoint_ids"}.intersection(keyword_by_name)
            count_by_helper[helper_name] += 1
    assert count_by_helper == expected_count_by_helper


@pytest.mark.asyncio
@pytest.mark.parametrize("reject", [False, True])
async def test_shared_refresh_child_requires_original_executor_before_write(monkeypatch, reject):
    directory = importlib.import_module("process.provider_directory_fhir")
    identity = SimpleNamespace(**_IDENTITY)
    connection = SimpleNamespace(status=AsyncMock(return_value=1))
    require = AsyncMock(side_effect=GenericCandidateError("synthetic_executor_refused") if reject else None)
    monkeypatch.setattr(directory, "require_checkpoint_executor", require)
    monkeypatch.setattr(directory.bulk_parallel, "source_copy_executor", lambda executor: executor)
    operation = directory.bulk_parallel.reserve_refresh_attempts(directory, connection, identity, 1, 1)
    if reject:
        with pytest.raises(GenericCandidateError, match="synthetic_executor_refused"):
            await operation
        connection.status.assert_not_awaited()
    else:
        await operation
        connection.status.assert_awaited_once()
        assert connection.status.await_args.kwargs["checkpoint_id"] == identity.checkpoint_id
    require.assert_awaited_once_with(connection, bulk_identity=identity)


@pytest.mark.asyncio
@pytest.mark.parametrize("is_batch_loading", [True, False])
async def test_checkpoint_transaction_passes_batch_loading_to_its_source_transaction(monkeypatch, is_batch_loading):
    opened_options = []

    @asynccontextmanager
    async def source_transaction(database, **options):
        opened_options.append(options)
        raise RuntimeError("opened")
        yield

    monkeypatch.setattr(control, "source_transaction", source_transaction)
    with pytest.raises(RuntimeError, match="opened"):
        async with control.source_checkpoint_transaction(object(), is_batch_loading=is_batch_loading):
            pytest.fail("the transaction body must not run")
    assert opened_options == [{"is_batch_loading": True}] if is_batch_loading else [{}]

    opened_options.clear()
    with pytest.raises(RuntimeError, match="opened"):
        async with control.source_checkpoint_transaction(object()):
            pytest.fail("the transaction body must not run")
    assert opened_options == [{}]


@pytest.fixture
def named_writer(monkeypatch):
    """Keep Source task/parent authorization while exposing its actual SQLAlchemy executor."""
    factory = _named_factory()
    native, _ = _connection()
    directory = importlib.import_module("process.provider_directory_fhir")
    monkeypatch.setattr(directory, "_schema", lambda: factory.storage_binding.catalog_schema)
    native.fetchrow = AsyncMock(return_value=dict(_IDENTITY))
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(rowcount=1)), in_transaction=lambda: True)
    driver_lookup = AsyncMock(side_effect=AssertionError("named SQL needs no connection identity probe"))
    monkeypatch.setattr(control, "_copy_driver", driver_lookup)

    @asynccontextmanager
    async def transaction():
        """Issue task-scoped authority inside the requesting test's task."""
        async with control.source.bind_generic_source_transaction_factory(factory):
            async with control.source.bind_generic_source_publisher(
                native, factory.policy, verify_role_profile=AsyncMock(), transaction_isolation="repeatable read"
            ) as publisher:
                owner_token = control.source._OWNER.set((publisher, session))
                bulk_token = control._BULK_IDENTITY.set(dict(_IDENTITY))
                try:
                    yield SimpleNamespace(
                        session=session,
                        native=native,
                        publisher=publisher,
                        identity=SimpleNamespace(**_IDENTITY, owner_run_id="owner-run"),
                    )
                finally:
                    control._BULK_IDENTITY.reset(bulk_token)
                    control.source._OWNER.reset(owner_token)
        driver_lookup.assert_not_awaited()

    return transaction


def _unrelated_executor(kind):
    """Expose supported executor shapes whose SQL must remain untouched."""
    if kind == "database":
        executor = control.Database()
        executor._transaction_binding = lambda: SimpleNamespace(session=object())
    else:
        executor = control.ConnectionProxy(None, object(), object())
    executor.status = AsyncMock(side_effect=AssertionError("unrelated executor received a Source write"))
    return executor


async def _bulk_child(directory, name, executor, identity):
    """Invoke each existing bulk child through its normal parameters."""
    output = SimpleNamespace(
        output_index=0,
        resource_type="Organization",
        url="https://example.org/output",
        url_hash="c" * 64,
        identity_hash="d" * 64,
    )
    manifest = SimpleNamespace(outputs=[output], identity_hash="e" * 64, payload={}, status_url=None)
    if name == "manifest":
        await directory._persist_bulk_manifest_outputs(executor, identity, manifest)
    elif name == "output":
        validator = SimpleNamespace(etag=None, output_expires_at=None, content_length_bytes=42, etag_hash=None)
        await directory._persist_refreshed_bulk_output(executor, identity, output, {"output_id": "output"}, validator)
    elif name == "refresh":
        await directory._persist_refreshed_bulk_manifest(executor, identity, {}, manifest, "{}")
    else:
        await directory.bulk_parallel.reserve_refresh_attempts(directory, executor, identity, 1, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("executor_kind", ["database", "proxy"])
@pytest.mark.parametrize("child_name", ["manifest", "output", "refresh", "attempts"])
async def test_named_bulk_children_route_wrong_executor_to_copy_and_checkpoint_session(
    named_writer,
    monkeypatch,
    executor_kind,
    child_name,
):
    directory = importlib.import_module("process.provider_directory_fhir")
    monkeypatch.setattr(directory, "_encrypt_bulk_capability", lambda value: "synthetic-ciphertext")
    executor = _unrelated_executor(executor_kind)
    async with named_writer() as writer:
        await _bulk_child(directory, child_name, executor, writer.identity)
        executor.status.assert_not_awaited()
        writer.session.execute.assert_awaited_once()
        statement, parameters = writer.session.execute.await_args.args
        table = (
            "provider_directory_bulk_acquisition_checkpoint"
            if child_name == "refresh"
            else "provider_directory_bulk_output_checkpoint"
        )
        assert f'"source_schema"."{table}"' in str(statement)
        assert parameters["checkpoint_id"] == _IDENTITY["checkpoint_id"]
        assert "FOR SHARE" in writer.native.fetchrow.await_args.args[0]


@pytest.mark.asyncio
async def test_named_checkpoint_routing_preserves_active_session_and_cursor_scope(named_writer):
    executor = _unrelated_executor("proxy")
    async with named_writer() as writer:
        assert (
            await asyncio.create_task(control.require_checkpoint_executor(executor, bulk_identity=writer.identity))
            is writer.publisher
        )
        token = control._BULK_IDENTITY.set({**_IDENTITY, "dataset_id": "different"})
        try:
            with pytest.raises(GenericCandidateError, match="checkpoint_identity_scope_changed"):
                await control.require_checkpoint_executor(executor, bulk_identity=writer.identity)
        finally:
            control._BULK_IDENTITY.reset(token)
        writer.session.execute.assert_not_awaited()
        executor.status.assert_not_awaited()


@pytest.mark.asyncio
async def test_named_retained_copy_and_proof_share_the_issued_session(named_writer, monkeypatch):
    from process import provider_directory_generic_source_adapters as adapters

    directory = importlib.import_module("process.provider_directory_fhir")
    executor = _unrelated_executor("proxy")
    resource_rows = [{"dataset_id": "dataset", "resource_type": "Organization", "resource_id": "organization"}]
    async with named_writer() as writer:

        async def copy_rows(_directory, rows, **options):
            assert rows == resource_rows and control.source_copy_session() is writer.session
            await writer.session.execute("COPY synthetic_resource", {})

        async def proof_rows(routed, _schema, rows, **options):
            assert rows == resource_rows
            await routed.status("INSERT INTO synthetic_proof VALUES (:value)", value="proof")

        monkeypatch.setattr(adapters, "persist_source_rows", copy_rows)
        monkeypatch.setattr(directory, "persist_dataset_proof_shard", proof_rows)
        await directory._upsert_dataset_resource_rows_on_connection(
            executor, resource_rows, persist_content_proof=True, resource_hash_contract="transport_neutral_v2"
        )
        assert writer.session.execute.await_count == 2
        executor.status.assert_not_awaited()


@asynccontextmanager
async def _plain_transaction(database, **options):
    yield


@pytest.fixture
def batch_controls(monkeypatch):
    """Stand in for the transaction, clock and captures around one isolated publisher."""
    publisher = SimpleNamespace(
        connection=object(),
        policy=SimpleNamespace(schema="private_source"),
        storage_binding=SimpleNamespace(catalog_schema="source_schema"),
    )
    stand_ins = SimpleNamespace(publisher=publisher, capture=AsyncMock(), coordinates=AsyncMock(), seconds=[0.0])
    monkeypatch.setattr(control, "source_transaction", _plain_transaction)
    monkeypatch.setattr(control, "require_checkpoint_executor", AsyncMock(return_value=publisher))
    monkeypatch.setattr(control, "require_storage_schema", lambda *_: None)
    monkeypatch.setattr(control, "source_copy_session", lambda: "session")
    monkeypatch.setattr(control, "capture_checkpoint_controls", stand_ins.capture)
    monkeypatch.setattr(control, "verify_isolated_source_storage_coordinates", stand_ins.coordinates)
    monkeypatch.setattr(control, "time", SimpleNamespace(monotonic=lambda: stand_ins.seconds[0]))
    monkeypatch.setattr(control, "_BATCH_CONTROLS", [])
    return stand_ins


async def _open_checkpoint_transaction(stand_ins, seconds, *, is_batch_loading=True):
    stand_ins.seconds[0] = seconds
    async with control.source_checkpoint_transaction(object(), is_batch_loading=is_batch_loading) as session:
        assert session == "session"


@pytest.mark.asyncio
async def test_bulk_batches_capture_checkpoint_controls_once_per_interval(batch_controls):
    for seconds in (0.0, 10.0, 59.0):
        await _open_checkpoint_transaction(batch_controls, seconds)
    publisher = batch_controls.publisher
    batch_controls.capture.assert_awaited_once_with(
        publisher.connection, publisher.policy, storage_binding=publisher.storage_binding
    )
    # A reused capture still checks the storage coordinates on the batch's own connection.
    assert (
        batch_controls.coordinates.await_args_list
        == [((publisher.connection, publisher.policy, publisher.storage_binding),)] * 2
    )

    await _open_checkpoint_transaction(batch_controls, 61.0)
    assert batch_controls.capture.await_count == 2


@pytest.mark.asyncio
async def test_other_checkpoint_transactions_capture_controls_every_time(batch_controls):
    await _open_checkpoint_transaction(batch_controls, 0.0)
    for seconds in (1.0, 2.0):
        await _open_checkpoint_transaction(batch_controls, seconds, is_batch_loading=False)
    assert batch_controls.capture.await_count == 3
    batch_controls.coordinates.assert_not_awaited()
    # They leave nothing behind for a batch either: only the first batch's capture is on record.
    assert control._BATCH_CONTROLS[0][3] == 0.0


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["factory", "policy", "storage_binding"])
async def test_batch_controls_are_reused_only_for_the_same_issued_storage(batch_controls, changed):
    await _open_checkpoint_transaction(batch_controls, 0.0)
    token = control.source._FACTORY.set(object()) if changed == "factory" else None
    try:
        if changed != "factory":
            setattr(batch_controls.publisher, changed, SimpleNamespace(schema="other", catalog_schema="other"))
        await _open_checkpoint_transaction(batch_controls, 1.0)
    finally:
        if token is not None:
            control.source._FACTORY.reset(token)
    assert batch_controls.capture.await_count == 2
    batch_controls.coordinates.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_batch_controls_capture_is_never_reused(batch_controls):
    batch_controls.capture.side_effect = [GenericCandidateError("checkpoint_custody_changed"), None]
    with pytest.raises(GenericCandidateError, match="checkpoint_custody_changed"):
        await _open_checkpoint_transaction(batch_controls, 0.0)
    assert control._BATCH_CONTROLS == []
    await _open_checkpoint_transaction(batch_controls, 1.0)
    assert batch_controls.capture.await_count == 2
