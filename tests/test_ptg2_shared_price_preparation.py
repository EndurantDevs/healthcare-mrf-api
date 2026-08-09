# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused tests split from a shared contract fixture module."""

from __future__ import annotations

from tests.test_ptg2_shared_price import (
    AsyncMock,
    asyncio,
    pytest,
    shared_price,
)



@pytest.mark.asyncio
async def test_prepared_price_artifacts_rank_summary_in_parallel(monkeypatch):
    status = AsyncMock()
    monkeypatch.setattr(shared_price.db, "status", status)
    monkeypatch.setattr(
        shared_price,
        "_normalize_strict_price_atom_stage",
        AsyncMock(return_value={"rows_after": 2}),
    )
    monkeypatch.setattr(
        shared_price,
        "_rewrite_price_atom_lean_dictionary",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        shared_price,
        "_create_v3_price_key_stage",
        AsyncMock(return_value={"row_count": 2}),
    )
    monkeypatch.setattr(
        shared_price,
        "_create_v3_atom_key_stage",
        AsyncMock(return_value={"row_count": 2}),
    )

    prepared = await shared_price.prepare_shared_price_artifacts(
        schema_name="mrf",
        manifest_stage_table="manifest_stage",
        price_set_summary_source_count=1,
    )

    shared_price._normalize_strict_price_atom_stage.assert_not_awaited()
    assert prepared.stage_metrics["price_atom_source_mode"] == (
        "single_scanner_unique_provenance"
    )
    assert prepared.stage_metrics["normalization_seconds"] == 0.0
    assert prepared.stage_metrics["rows_before"] == 2
    assert prepared.stage_metrics["rows_after"] == 2
    assert prepared.stage_metrics["duplicate_rows_removed"] == 0
    assert prepared.stage_metrics["conflicting_ids"] == 0
    assert not any(
        "negotiated_rate_numeric" in call.args[0] for call in status.await_args_list
    )
    price_stage_call = shared_price._create_v3_price_key_stage.await_args
    assert price_stage_call.kwargs["price_set_summary_table"].startswith(
        "ptg2_manifest_stage_price_set_summary_"
    )
    assert price_stage_call.kwargs["price_set_summary_source_count"] == 1


@pytest.mark.asyncio
async def test_prepared_price_artifacts_normalize_cross_source_atoms(monkeypatch):
    normalizer = AsyncMock(
        return_value={
            "rows_before": 3,
            "rows_after": 2,
            "duplicate_rows_removed": 1,
            "conflicting_ids": 0,
        }
    )
    monkeypatch.setattr(shared_price.db, "status", AsyncMock())
    monkeypatch.setattr(
        shared_price,
        "_normalize_strict_price_atom_stage",
        normalizer,
    )
    monkeypatch.setattr(
        shared_price,
        "_rewrite_price_atom_lean_dictionary",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        shared_price,
        "_create_v3_price_key_stage",
        AsyncMock(return_value={"row_count": 2}),
    )
    monkeypatch.setattr(
        shared_price,
        "_create_v3_atom_key_stage",
        AsyncMock(return_value={"row_count": 2}),
    )

    prepared = await shared_price.prepare_shared_price_artifacts(
        schema_name="mrf",
        manifest_stage_table="manifest_stage",
        price_set_summary_source_count=2,
    )

    normalizer.assert_awaited_once()
    assert prepared.stage_metrics["price_atom_source_mode"] == (
        "cross_file_canonicalize"
    )
    assert prepared.stage_metrics["rows_before"] == 3
    assert prepared.stage_metrics["rows_after"] == 2
    assert prepared.stage_metrics["duplicate_rows_removed"] == 1


@pytest.mark.asyncio
async def test_price_key_ready_fires_while_atom_preparation_is_still_running(
    monkeypatch,
):
    atom_release = asyncio.Event()
    ready = asyncio.Event()
    observed_keys = []

    async def rewrite_atom_stage(**_kwargs):
        await atom_release.wait()
        return {}

    def price_key_ready(prepared_key):
        observed_keys.append(prepared_key)
        ready.set()

    monkeypatch.setattr(shared_price.db, "status", AsyncMock())
    monkeypatch.setattr(
        shared_price,
        "_normalize_strict_price_atom_stage",
        AsyncMock(),
    )
    monkeypatch.setattr(
        shared_price,
        "_rewrite_price_atom_lean_dictionary",
        rewrite_atom_stage,
    )
    monkeypatch.setattr(
        shared_price,
        "_create_v3_price_key_stage",
        AsyncMock(return_value={"row_count": 2}),
    )
    monkeypatch.setattr(
        shared_price,
        "_create_v3_atom_key_stage",
        AsyncMock(return_value={"row_count": 2}),
    )

    prepare_task = asyncio.create_task(
        shared_price.prepare_shared_price_artifacts(
            schema_name="mrf",
            manifest_stage_table="manifest_stage",
            price_set_summary_source_count=1,
            price_key_ready=price_key_ready,
        )
    )
    await asyncio.wait_for(ready.wait(), timeout=0.5)
    assert not prepare_task.done()
    assert len(observed_keys) == 1
    observed_key = observed_keys[0]
    assert observed_key.schema_name == "mrf"
    assert observed_key.price_set_count == 2
    assert observed_key.price_key_map.startswith("ptg2_manifest_stage_v3_price_key_")

    atom_release.set()
    prepared = await prepare_task
    shared_price._normalize_strict_price_atom_stage.assert_not_awaited()
    assert prepared.price_key_map == observed_key.price_key_map
    assert prepared.stage_metrics["price_key_build_seconds"] >= 0


@pytest.mark.asyncio
async def test_price_prepare_failure_removes_partial_key_stages(monkeypatch):
    status = AsyncMock()
    monkeypatch.setattr(shared_price.db, "status", status)
    monkeypatch.setattr(
        shared_price,
        "_normalize_strict_price_atom_stage",
        AsyncMock(side_effect=RuntimeError("broken stage")),
    )
    monkeypatch.setattr(
        shared_price,
        "_create_v3_price_key_stage",
        AsyncMock(return_value={"row_count": 2}),
    )

    with pytest.raises(RuntimeError, match="broken stage"):
        await shared_price.prepare_shared_price_artifacts(
            schema_name="mrf",
            manifest_stage_table="manifest_stage",
            price_set_summary_source_count=2,
        )

    cleanup_sql = status.await_args_list[-1].args[0]
    assert "v3_price_key" in cleanup_sql
    assert "v3_atom_key" in cleanup_sql
    assert "v3_price_attr" in cleanup_sql


@pytest.mark.asyncio
async def test_price_prepare_repeated_cancellation_finishes_drain_and_cleanup(
    monkeypatch,
):
    """Drain child and stage cleanup despite repeated task cancellation."""
    child_cleanup_started = asyncio.Event()
    release_child_cleanup = asyncio.Event()
    child_cleanup_finished = asyncio.Event()
    stage_cleanup_started = asyncio.Event()
    release_stage_cleanup = asyncio.Event()
    stage_cleanup_finished = asyncio.Event()
    async def delayed_price_stage(**_kwargs):
        try:
            await asyncio.Future()
        finally:
            child_cleanup_started.set()
            await release_child_cleanup.wait()
            child_cleanup_finished.set()
    async def fail_atom_stage(**_kwargs):
        raise RuntimeError("broken atom stage")
    async def status(sql):
        if "," in sql:
            stage_cleanup_started.set()
            await release_stage_cleanup.wait()
            stage_cleanup_finished.set()
    monkeypatch.setattr(shared_price.db, "status", status)
    monkeypatch.setattr(
        shared_price,
        "_normalize_strict_price_atom_stage",
        fail_atom_stage,
    )
    monkeypatch.setattr(
        shared_price,
        "_create_v3_price_key_stage",
        delayed_price_stage,
    )
    prepare_task = asyncio.create_task(
        shared_price.prepare_shared_price_artifacts(
            schema_name="mrf",
            manifest_stage_table="manifest_stage",
            price_set_summary_source_count=2,
        )
    )
    await child_cleanup_started.wait()
    prepare_task.cancel()
    await asyncio.sleep(0)
    prepare_task.cancel()
    await asyncio.sleep(0)
    assert not prepare_task.done()
    release_child_cleanup.set()
    await stage_cleanup_started.wait()
    assert child_cleanup_finished.is_set()
    prepare_task.cancel()
    await asyncio.sleep(0)
    prepare_task.cancel()
    await asyncio.sleep(0)
    assert not prepare_task.done()
    release_stage_cleanup.set()
    with pytest.raises(RuntimeError, match="broken atom stage"):
        await prepare_task
    assert stage_cleanup_finished.is_set()
