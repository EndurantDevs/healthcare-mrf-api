# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Both queued finalizer paths claim completion ownership before live effects."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.test_process_mrf_file_shutdown import (
    _install_mrf_shutdown_finalizer_mocks,
    process_initial,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("tracked", [True, False])
@pytest.mark.parametrize("failure", ["archive", "family", "summary"])
async def test_shared_finalizer_claims_before_live_effects(monkeypatch, tracked, failure):
    operations = []
    mark_run = AsyncMock()
    _install_mrf_shutdown_finalizer_mocks(monkeypatch, operations, mark_run, AsyncMock())
    monkeypatch.setattr(process_initial.db, "scalar", AsyncMock(return_value=500))
    monkeypatch.setattr(process_initial, "source_enabled", lambda _source: True)
    monkeypatch.setattr(process_initial, "stamp_address_keys", AsyncMock())
    monkeypatch.setattr(process_initial, "propagate_child_address_keys", AsyncMock())
    monkeypatch.setenv("HLTHPRT_MRF_OPENADDRESSES_BACKFILL", "false")
    effects = []

    async def claim(*_args):
        effects.append("pending")
        return "attempt"

    def phase(name, result=None):
        async def perform(*_args, **_kwargs):
            effects.append(name)
            if failure == name:
                raise RuntimeError(f"{name} failure")
            return result
        return perform

    monkeypatch.setattr(process_initial, "begin_publication", claim)
    monkeypatch.setattr(process_initial, "resolve_into_archive", phase("archive"))
    monkeypatch.setattr(process_initial, "_publish_mrf_table_generation", phase("family", "generation"))
    monkeypatch.setattr(process_initial, "rebuild_plan_search_summary", phase("summary"))
    if tracked:
        task_by_field = {"context": {"import_date": "synthetic", "control_run_id": "tracked", "test_mode": False}}
    else:
        pool = SimpleNamespace(enqueue_job=AsyncMock())
        monkeypatch.setattr(process_initial, "create_pool", AsyncMock(return_value=pool))
        await process_initial.finish_main(import_id="synthetic")
        task_by_field = pool.enqueue_job.await_args.args[1]
        assert "control_run_id" not in task_by_field["context"]
    with pytest.raises(RuntimeError, match=f"{failure} failure"):
        await process_initial.publish_initial_generation({}, task_by_field)
    assert effects == ["pending", "archive", "family", "summary"][:["archive", "family", "summary"].index(failure) + 2]
    assert all(call.kwargs.get("status") != "succeeded" for call in mark_run.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [True, False])
@pytest.mark.parametrize("tracked", [True, False])
async def test_success_records_actual_address_resolution_branch(monkeypatch, enabled, tracked):
    mark_run = AsyncMock()
    _install_mrf_shutdown_finalizer_mocks(monkeypatch, [], mark_run, AsyncMock())
    monkeypatch.setattr(process_initial.db, "scalar", AsyncMock(return_value=500))
    monkeypatch.setattr(process_initial, "source_enabled", lambda _source: enabled)
    monkeypatch.setattr(process_initial, "stamp_address_keys", AsyncMock())
    monkeypatch.setattr(process_initial, "propagate_child_address_keys", AsyncMock())
    resolver = AsyncMock(return_value=SimpleNamespace())
    monkeypatch.setattr(process_initial, "resolve_into_archive", resolver)
    monkeypatch.setenv("HLTHPRT_MRF_OPENADDRESSES_BACKFILL", "false")
    monkeypatch.setattr(process_initial, "begin_publication", AsyncMock(return_value="attempt"))

    async def publish(*_args):
        # Configuration may change after resolution; completion must retain execution history.
        monkeypatch.setattr(process_initial, "source_enabled", lambda _source: not enabled)
        return "generation"

    monkeypatch.setattr(process_initial, "_publish_mrf_table_generation", publish)
    summary = AsyncMock(return_value=1)
    monkeypatch.setattr(process_initial, "rebuild_plan_search_summary", summary)
    if tracked:
        task_by_field = {"context": {"import_date": "synthetic", "control_run_id": "tracked", "test_mode": False}}
    else:
        pool = SimpleNamespace(enqueue_job=AsyncMock())
        monkeypatch.setattr(process_initial, "create_pool", AsyncMock(return_value=pool))
        await process_initial.finish_main(import_id="synthetic")
        task_by_field = pool.enqueue_job.await_args.args[1]
    await process_initial.publish_initial_generation({}, task_by_field)
    assert resolver.await_count == int(enabled)
    assert summary.await_args.kwargs["publication"] == ("attempt", "generation", enabled)
    assert mark_run.await_args.kwargs["status"] == "succeeded"
