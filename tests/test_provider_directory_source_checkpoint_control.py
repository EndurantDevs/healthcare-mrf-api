# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Batch fences retain both parent coordinates and the original transaction."""

import ast
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
async def test_abandoned_parent_found_only_by_root_refuses_before_checkpoint_mutation():
    publisher = _publisher(parents=[{"dataset_id": "another-dataset", "status": "acquisition_abandoned"}])
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
