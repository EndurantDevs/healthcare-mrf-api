# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Profile checkpoint failures preserve the original build and transaction scope."""

import asyncio
import importlib
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_generic_source_policy as policy
from tests.test_provider_directory_profile_artifact import _planned_fresh_profile_build, _single_source_stage_build

importer = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.asyncio
@pytest.mark.parametrize("has_capacity", [False, True])
@pytest.mark.parametrize("marker_fails", [False, True])
async def test_repeated_cancellation_drains_failure_marker_without_losing_build_error(
    monkeypatch, has_capacity, marker_fails
):
    """A second cancellation cannot abandon a durable failure update."""
    build_error, marker_error = RuntimeError("projection failed"), ValueError("marker failed")
    started, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def mark_failed(*_args):
        started.set()
        await release.wait()
        finished.set()
        if marker_fails:
            raise marker_error

    monkeypatch.setattr(importer, "_claim_provider_directory_profile_build_checkpoint", AsyncMock())
    monkeypatch.setattr(
        importer, "_populate_claimed_provider_directory_profile_stages", AsyncMock(side_effect=build_error)
    )
    monkeypatch.setattr(importer, "_mark_profile_build_checkpoint_failed", mark_failed)
    monkeypatch.setattr(
        importer, "_provider_directory_profile_capacity_admission", lambda: object() if has_capacity else None
    )
    fence = importer.ProviderDirectoryArtifactBuildFence()
    task = asyncio.create_task(
        importer._build_provider_directory_profile_stages(
            _single_source_stage_build(), fence, fence, has_existing_artifacts=False
        )
    )
    try:
        await asyncio.wait_for(started.wait(), 1)
        for _attempt in range(3):
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done() and not finished.is_set()
        release.set()
        with pytest.raises(BaseException) as caught:
            await task
        if marker_fails and has_capacity:
            assert isinstance(caught.value, BaseExceptionGroup)
            assert caught.value.exceptions == (build_error, marker_error)
        else:
            assert caught.value is build_error
        assert finished.is_set()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["state", "failed", "delete"])
async def test_checkpoint_row_loss_refuses_the_capacity_transition(monkeypatch, operation):
    """A missing checkpoint cannot report a completed capacity operation."""
    reserve, status = AsyncMock(), AsyncMock(return_value=0)
    monkeypatch.setattr(importer, "_provider_directory_profile_capacity_admission", lambda: object())
    monkeypatch.setattr(importer, "_reserve_provider_directory_profile_wal_budget", reserve)
    monkeypatch.setattr(importer, "_provider_directory_profile_capacity_status", status)
    build = _planned_fresh_profile_build(_single_source_stage_build())
    with pytest.raises(RuntimeError, match="ownership_lost"):
        if operation == "state":
            await importer._mark_profile_build_checkpoint_state(build, "ready")
        elif operation == "failed":
            await importer._mark_profile_build_checkpoint_failed(build, RuntimeError("build failed"))
        else:
            await importer._delete_provider_directory_profile_build_checkpoint(build.schema, build.build_id)
    reserve.assert_awaited_once()
    status.assert_awaited_once()


@pytest.mark.asyncio
async def test_unplanned_checkpoint_state_refuses_before_reserving_or_writing(monkeypatch):
    """Only planned checkpoint state transitions may consume capacity."""
    reserve, status = AsyncMock(), AsyncMock()
    monkeypatch.setattr(importer, "_provider_directory_profile_capacity_admission", lambda: object())
    monkeypatch.setattr(importer, "_reserve_provider_directory_profile_wal_budget", reserve)
    monkeypatch.setattr(importer, "_provider_directory_profile_capacity_status", status)
    with pytest.raises(RuntimeError, match="checkpoint_state_unplanned"):
        await importer._mark_profile_build_checkpoint_state(_single_source_stage_build(), "unplanned")
    reserve.assert_not_awaited()
    status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("source_required", [False, True])
async def test_evidence_projection_failure_exits_both_transaction_scopes(monkeypatch, source_required):
    """Projection failure drains capacity before releasing the prepared read."""
    events = []
    failure = RuntimeError("projection refused")

    @asynccontextmanager
    async def prepared_read(_importer):
        events.append("read_enter")
        try:
            yield
        finally:
            events.append("read_exit")

    @asynccontextmanager
    async def capacity(*, reuse_borrowed):
        assert reuse_borrowed is source_required
        events.append("capacity_enter")
        try:
            yield
        finally:
            events.append("capacity_exit")

    projection = AsyncMock(side_effect=failure)
    monkeypatch.setattr(policy, "is_source_storage_required", lambda: source_required)
    monkeypatch.setattr(importer.profile_scope_copy, "prepared_read_transaction", prepared_read)
    monkeypatch.setattr(importer, "_provider_directory_profile_capacity_transaction", capacity)
    monkeypatch.setattr(importer, "_project_profile_evidence_batch", projection)
    batch = object()
    with pytest.raises(RuntimeError) as caught:
        await importer._project_profile_evidence_batch_with_capacity(batch, {}, {})
    assert caught.value is failure
    projection.assert_awaited_once_with(batch, {}, {})
    assert events == ["read_enter", "capacity_enter", "capacity_exit", "read_exit"]
