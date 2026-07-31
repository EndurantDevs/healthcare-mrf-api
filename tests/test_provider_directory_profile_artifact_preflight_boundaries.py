# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Boundary coverage for artifact layout, cleanup, and cancellation."""

from __future__ import annotations

import asyncio
import dataclasses
import datetime
import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from .test_provider_directory_profile_selection_attestation import _execution
from .test_provider_directory_profile_control_capacity import (
    _bound_control_wal_projection,
)
from .provider_directory_profile_execution_test_support import (
    _capacity_consumption_row,
    _capacity_geometry_identity,
    _profile_serving_state,
    _published_dataset_state,
    _wal_tracker_admission,
)

importer = importlib.import_module("process.provider_directory_fhir")
capacity = importlib.import_module("process.provider_directory_profile_capacity")


def _evidence_batch_fixture():
    """Return one fact batch plus its target stage build."""
    build = importer._ProviderDirectoryProfileBuild(
        schema="mrf",
        generation_id="generation-a",
        source_ids=("source-a",),
        retained_source_ids=("source-a",),
        dataset_ids=("dataset-a",),
        profile_as_of="2026-07-30",
        evidence_stage="evidence-stage",
        profile_stage="profile-stage",
    )
    batch = importer._ProviderDirectoryProfileEvidenceBatch(
        kind="fact",
        source_id="source-a",
        dataset_id="dataset-a",
        fact_type="name",
    )
    return build, batch


def _patch_evidence_batch_collaborators(monkeypatch):
    """Install exact projection, insert, transaction, and storage seams."""
    projection = AsyncMock(
        return_value=SimpleNamespace(
            _mapping={
                "projected_rows": 2,
                "projected_logical_bytes": 512,
            }
        )
    )
    insert = AsyncMock(return_value=2)
    count_sql = Mock(return_value="SELECT projected")
    insert_sql = Mock(return_value="INSERT projected")

    @asynccontextmanager
    async def capacity_transaction():
        yield

    monkeypatch.setattr(importer.db, "first", projection)
    monkeypatch.setattr(importer.db, "status", insert)
    monkeypatch.setattr(
        importer.profile_artifact, "profile_evidence_count_sql", count_sql
    )
    monkeypatch.setattr(
        importer.profile_artifact, "profile_evidence_insert_sql", insert_sql
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_transaction",
        capacity_transaction,
    )
    monkeypatch.setattr(
        importer,
        "_assert_provider_directory_profile_stage_storage_identity",
        AsyncMock(),
    )
    return projection, insert, count_sql, insert_sql


@pytest.mark.asyncio
async def test_capacity_admitted_evidence_batch_preflights_exact_rows(
    monkeypatch,
):
    """Preflight exact evidence rows before the admitted insert."""
    build, batch = _evidence_batch_fixture()
    projection, insert, count_sql, insert_sql = (
        _patch_evidence_batch_collaborators(monkeypatch)
    )
    capacity_token = (
        importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(
            object()
        )
    )
    try:
        assert await importer._execute_profile_evidence_batch(
            build,
            batch,
            "COPY",
            {},
        ) == 2
    finally:
        importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(
            capacity_token
        )
    projection.assert_awaited_once()
    insert.assert_awaited_once()
    assert count_sql.call_args.kwargs["fact_type"] == "name"
    assert insert_sql.call_args.kwargs["fact_type"] == "name"


def _empty_artifact_projection():
    """Return every artifact model with a zero-row exact projection."""
    source_model = importer.ProviderDirectorySource
    models = (source_model, *importer.RESOURCE_MODELS)
    projection = importer._ProviderDirectoryArtifactScopeExactProjection(
        tables=tuple(
            importer._ProviderDirectoryArtifactScopeTableProjection(
                table_name=model.__tablename__,
                resource_type="source"
                if model is source_model
                else importer.RESOURCE_TYPES_BY_MODEL[model],
                projected_rows=0,
                projected_logical_bytes=0,
            )
            for model in models
        )
    )
    return models, projection


def _patch_artifact_preflight_refusal(monkeypatch, events: list[str]) -> None:
    """Record complete layout creation and refuse before any payload DML."""
    async def create_layout(model, _schema, _table_name):
        events.append("layout:" + model.__tablename__)

    async def refuse_preflight(*_args, **_kwargs):
        events.append("preflight")
        raise RuntimeError("projected capacity refused")

    async def materialize_payload(*_args, **_kwargs):
        events.append("payload")
        return 0

    monkeypatch.setattr(
        importer, "_create_provider_directory_artifact_scope_layout", create_layout
    )
    monkeypatch.setattr(
        importer,
        "_preflight_provider_directory_artifact_scope_capacity",
        refuse_preflight,
    )
    monkeypatch.setattr(
        importer,
        "_materialize_provider_directory_artifact_source_scope",
        materialize_payload,
    )
    monkeypatch.setattr(
        importer,
        "_materialize_provider_directory_artifact_resource_scope",
        materialize_payload,
    )
    monkeypatch.setattr(importer, "_drop_artifact_scope_tables", AsyncMock())


@pytest.mark.asyncio
async def test_artifact_scope_creates_complete_layout_and_preflights_before_payload(
    monkeypatch,
):
    """Create every empty relation before refusing projected capacity."""
    events: list[str] = []
    models, projection = _empty_artifact_projection()
    _patch_artifact_preflight_refusal(monkeypatch, events)
    with pytest.raises(RuntimeError, match="projected capacity refused"):
        await importer._materialize_artifact_scope_tables(
            "mrf",
            "run_" + "a" * 32,
            importer.ProviderDirectoryArtifactDatasetFence(()),
            frozenset(),
            projection=projection,
            worker_count=2,
        )

    assert events[-1] == "preflight"
    assert events.count("preflight") == 1
    assert "payload" not in events
    assert events[:-1] == sorted(
        ("layout:" + model.__tablename__ for model in models)
    )


@pytest.mark.asyncio
async def test_recovery_refusal_never_generic_cleans_planned_owner_names(
    monkeypatch,
):
    """Only tables created by this invocation may reach generic cleanup."""
    run_id = "run_" + "a" * 32
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        lambda: SimpleNamespace(run_id=run_id),
    )
    monkeypatch.setattr(
        importer,
        "_recover_provider_directory_artifact_scope",
        AsyncMock(
            side_effect=importer.ProviderDirectoryArtifactBuildStale(
                "provider_directory_artifact_scope_current_owner_present"
            )
        ),
    )
    drop_scope = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_drop_artifact_scope_tables",
        drop_scope,
    )

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="current_owner_present",
    ):
        await importer._materialize_artifact_scope_tables(
            "mrf",
            run_id,
            importer.ProviderDirectoryArtifactDatasetFence(()),
            frozenset(),
        )

    drop_scope.assert_awaited_once_with("mrf", [])


@pytest.mark.asyncio
async def test_admitted_artifact_scope_cleanup_propagates_drop_failure(
    monkeypatch,
):
    reserve = AsyncMock()
    drop = AsyncMock(side_effect=RuntimeError("drop failed"))
    monkeypatch.setattr(
        importer,
        "_reserve_provider_directory_profile_wal_budget",
        reserve,
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_status",
        drop,
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        lambda: SimpleNamespace(),
    )

    with pytest.raises(RuntimeError, match="drop failed"):
        await importer._drop_artifact_scope_tables(
            "mrf",
            ["artifact_scope"],
        )

    reserve.assert_awaited_once()
    drop.assert_awaited_once()


@pytest.mark.asyncio
async def test_artifact_materialization_drains_cleanup_under_repeated_cancel(
    monkeypatch,
):
    """Drain materialization cleanup despite repeated task cancellation."""
    materialization_started = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    cleanup_completed = asyncio.Event()

    async def create_layout(*_args, **_kwargs):
        return None

    async def materialize_source(*_args, **_kwargs):
        materialization_started.set()
        await asyncio.Future()

    async def drop_scope(*_args, **_kwargs):
        cleanup_started.set()
        await release_cleanup.wait()
        cleanup_completed.set()

    monkeypatch.setattr(
        importer,
        "_create_provider_directory_artifact_scope_layout",
        create_layout,
    )
    monkeypatch.setattr(
        importer,
        "_materialize_provider_directory_artifact_source_scope",
        materialize_source,
    )
    monkeypatch.setattr(
        importer,
        "_materialize_provider_directory_artifact_resource_scope",
        AsyncMock(),
    )
    monkeypatch.setattr(importer, "_drop_artifact_scope_tables", drop_scope)
    task = asyncio.create_task(
        importer._materialize_artifact_scope_tables(
            "mrf",
            "run_" + "a" * 32,
            importer.ProviderDirectoryArtifactDatasetFence(()),
            frozenset(),
        )
    )

    await materialization_started.wait()
    task.cancel()
    await cleanup_started.wait()
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()

    release_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cleanup_completed.is_set()


@pytest.mark.asyncio
async def test_artifact_scope_exit_drains_cleanup_under_repeated_cancel(
    monkeypatch,
):
    """Drain context-exit cleanup despite repeated task cancellation."""
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    cleanup_completed = asyncio.Event()

    @asynccontextmanager
    async def artifact_guard(*_args, **_kwargs):
        yield

    async def drop_scope(*_args, **_kwargs):
        cleanup_started.set()
        await release_cleanup.wait()
        cleanup_completed.set()

    monkeypatch.setattr(
        importer,
        "_provider_directory_artifact_scope_guard",
        artifact_guard,
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_artifact_scope_projection",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        importer,
        "_assert_provider_directory_artifact_scope_capacity",
        Mock(),
    )
    monkeypatch.setattr(
        importer,
        "_reap_provider_directory_artifact_scope_tables",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        importer,
        "_materialize_artifact_scope_tables",
        AsyncMock(return_value=({}, ["artifact_scope"])),
    )
    monkeypatch.setattr(
        importer,
        "_drop_artifact_scope_tables",
        drop_scope,
    )

    task = asyncio.create_task(_consume_empty_artifact_scope())
    await cleanup_started.wait()
    task.cancel()
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()

    release_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cleanup_completed.is_set()


async def _consume_empty_artifact_scope() -> None:
    """Enter and immediately exit one empty artifact dataset scope."""
    async with importer._provider_directory_artifact_dataset_scope(
        run_id="run_" + "b" * 32,
        source_ids=(),
        fence=importer.ProviderDirectoryArtifactDatasetFence(()),
        resource_types=frozenset(),
    ):
        return


def test_source_delta_build_coordinates_require_matching_admission():
    """Resolve admitted delta coordinates before scratch construction."""
    source_vector = (("source-a", "dataset-a"),)
    context_vector = (("source-a", "a" * 64),)
    source_vector_hash = (
        importer._provider_directory_profile_source_vector_hash(source_vector)
    )
    context_vector_hash = (
        importer._provider_directory_profile_source_context_vector_hash(
            context_vector
        )
    )
    plan = importer._provider_directory_profile_batch_plan(
        ["source-a"],
        ["source-a"],
        ["dataset-a"],
        has_existing_artifacts=True,
        materialization_mode="source_delta",
        current_source_vector_hash=source_vector_hash,
        desired_source_vector_hash=source_vector_hash,
        current_source_context_vector_hash=context_vector_hash,
        desired_source_context_vector_hash=context_vector_hash,
    )
    identity = importer._ProviderDirectoryProfileIdentityInputs(
        source_ids=["source-a"],
        retained_source_ids=["source-a"],
        dataset_ids=["dataset-a"],
        resume_lineage_hash="a" * 64,
        batch_plan=plan,
        materialization_mode="source_delta",
        current_source_vector=source_vector,
        desired_source_vector=source_vector,
        current_source_vector_hash=source_vector_hash,
        desired_source_vector_hash=source_vector_hash,
        current_source_context_vector=context_vector,
        desired_source_context_vector=context_vector,
        current_source_context_vector_hash=context_vector_hash,
        desired_source_context_vector_hash=context_vector_hash,
        removed_source_ids=(),
        serving_state=_profile_serving_state(source_vector, context_vector),
    )
    admission = _wal_tracker_admission()
    capacity_token = (
        importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(admission)
    )
    try:
        coordinates = importer._profile_build_coordinates(identity)
    finally:
        importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(
            capacity_token
        )

    assert coordinates.build_id == admission.build_id
    assert coordinates.capacity_geometry_status == "verified"
    assert coordinates.capacity_geometry_hash == (
        capacity.capacity_geometry_hash(admission.geometry)
    )
    assert coordinates.affected_npi_stage is not None
