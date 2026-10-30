# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Batch fences retain both parent coordinates and the original transaction."""

import ast
import importlib
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_source_checkpoint_control as control
from process.provider_directory_generic_candidate import GenericCandidateError

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
    session = object()
    native = object()
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
    session = object()
    checkpoint_by_field = {**_IDENTITY}
    if change not in {None, "missing"}:
        checkpoint_by_field[change] = "other"
    native = SimpleNamespace(fetchrow=AsyncMock(return_value=None if change == "missing" else checkpoint_by_field))
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


def test_all_bulk_writer_scopes_and_connection_children_pass_full_identity():
    source = Path(control.__file__).with_name("provider_directory_fhir.py")
    tree = ast.parse(source.read_text())
    count_by_helper = {"source_checkpoint_transaction": 0, "require_checkpoint_executor": 0}
    for function in ast.walk(tree):
        if not isinstance(function, ast.AsyncFunctionDef) or "bulk" not in function.name:
            continue
        if not any(argument.arg == "identity" for argument in function.args.args):
            continue
        for call in ast.walk(function):
            if (
                not isinstance(call, ast.Call)
                or not isinstance(call.func, ast.Name)
                or call.func.id not in count_by_helper
            ):
                continue
            keyword_by_name = {keyword.arg: keyword.value for keyword in call.keywords}
            assert (
                isinstance(keyword_by_name.get("bulk_identity"), ast.Name)
                and keyword_by_name["bulk_identity"].id == "identity"
            )
            assert not {"dataset_ids", "root_run_ids", "checkpoint_ids"}.intersection(keyword_by_name)
            count_by_helper[call.func.id] += 1
    assert count_by_helper == {"source_checkpoint_transaction": 21, "require_checkpoint_executor": 3}
