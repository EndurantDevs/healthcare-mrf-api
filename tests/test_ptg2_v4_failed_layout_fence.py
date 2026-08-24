# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Unit ordering proof for failed-layout recovery attempt fencing."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_v4_failed_layout_fence as recovery_fence

_ACTIVE_FENCE = {
    "snapshot_id": "ptg2:202607:test",
    "internal_run_id": "ptg2:test-run",
    "fence_nonce": "fence-nonce",
    "state": "active",
    "target_digest": None,
    "plan_digest": None,
    "marker_digest": None,
    "marker": None,
    "created_at": "2026-08-24T00:00:00+00:00",
    "reconciled_at": None,
}


def test_active_fence_requires_creation_identity() -> None:
    with pytest.raises(recovery_fence.PTG2V4RecoveryConflict, match="changed"):
        recovery_fence.require_active_recovery_fence(
            {
                "snapshot_id": "snapshot",
                "internal_run_id": "run",
                "fence_nonce": "nonce",
                "state": "active",
            },
            snapshot_id="snapshot",
            import_run_id="run",
        )


@pytest.mark.asyncio
async def test_load_recovery_attempt_fence_returns_exact_single_row() -> None:
    empty_executor = SimpleNamespace(all=AsyncMock(return_value=[]))
    multiple_rows_executor = SimpleNamespace(
        all=AsyncMock(return_value=[dict(_ACTIVE_FENCE), dict(_ACTIVE_FENCE)])
    )
    row_executor = SimpleNamespace(all=AsyncMock(return_value=[dict(_ACTIVE_FENCE)]))

    assert await recovery_fence.load_recovery_attempt_fence(
        empty_executor,
        schema_name="mrf",
        snapshot_id="missing",
        import_run_id="run",
    ) == {}

    assert await recovery_fence.load_recovery_attempt_fence(
        multiple_rows_executor,
        schema_name="mrf",
        snapshot_id="ptg2:202607:test",
        import_run_id="ptg2:test-run",
    ) == {}

    loaded = await recovery_fence.load_recovery_attempt_fence(
        row_executor,
        schema_name="mrf",
        snapshot_id="ptg2:202607:test",
        import_run_id="ptg2:test-run",
        lock_row=True,
    )

    assert loaded == _ACTIVE_FENCE
    statement = row_executor.all.await_args.args[0]
    assert "FOR UPDATE" in statement
    assert row_executor.all.await_args.kwargs == {
        "snapshot_id": "ptg2:202607:test",
        "import_run_id": "ptg2:test-run",
    }


@pytest.mark.asyncio
async def test_attempt_fence_seal_cas_binds_nonce_and_creation_time() -> None:
    executor = SimpleNamespace(all=AsyncMock(return_value=[{"snapshot_id": "s"}]))

    await recovery_fence.seal_recovery_attempt_fence(
        executor,
        schema_name="mrf",
        snapshot_id="s",
        import_run_id="r",
        expected_fence_nonce="11111111-1111-1111-1111-111111111111",
        expected_fence_created_at="2026-08-24T00:00:00+00:00",
        marker_by_field={
            "contract": "test",
            "target_digest": "a" * 64,
            "plan_digest": "b" * 64,
        },
    )

    statement = executor.all.await_args.args[0]
    params = executor.all.await_args.kwargs
    assert "created_at = :expected_fence_created_at" in statement
    assert params["expected_fence_created_at"] == "2026-08-24T00:00:00+00:00"
    assert "fence_nonce = CAST(:expected_fence_nonce AS uuid)" in statement
    assert params["expected_fence_nonce"] == "11111111-1111-1111-1111-111111111111"


@pytest.mark.asyncio
async def test_attempt_fence_seal_rejects_changed_row() -> None:
    executor = SimpleNamespace(all=AsyncMock(return_value=[]))

    with pytest.raises(recovery_fence.PTG2V4RecoveryConflict, match="changed"):
        await recovery_fence.seal_recovery_attempt_fence(
            executor,
            schema_name="mrf",
            snapshot_id="s",
            import_run_id="r",
            expected_fence_nonce="11111111-1111-1111-1111-111111111111",
            expected_fence_created_at="2026-08-24T00:00:00+00:00",
            marker_by_field={
                "contract": "test",
                "target_digest": "a" * 64,
                "plan_digest": "b" * 64,
            },
        )


@pytest.mark.asyncio
async def test_recovery_fence_precedes_physical_mutation(monkeypatch) -> None:
    """Require fence validation before the shared engine can mutate."""

    event_names: list[str] = []
    expected_result = object()

    async def record_fence(*_args, **_kwargs) -> None:
        event_names.append("fence")

    connection = object()

    async def record_abandonment(*_args, **kwargs):
        await kwargs["callbacks"].step_guard(connection)
        event_names.append("abandon")
        return expected_result

    fence_mock = AsyncMock(side_effect=record_fence)
    abandonment_mock = AsyncMock(side_effect=record_abandonment)
    monkeypatch.setattr(recovery_fence, "lock_writable_snapshot", fence_mock)
    monkeypatch.setattr(
        recovery_fence,
        "load_recovery_attempt_fence",
        AsyncMock(return_value=dict(_ACTIVE_FENCE)),
    )
    monkeypatch.setattr(
        recovery_fence,
        "abandon_owned_v4_layout",
        abandonment_mock,
    )
    abandonment_result = await recovery_fence.abandon_writable_v4_layout(
        schema_name="mrf",
        snapshot_id="ptg2:202607:test",
        import_run_id="ptg2:test-run",
        snapshot_key=491,
        build_token="owned",
        expected_fence_by_field={
            "fence_nonce": "fence-nonce",
            "created_at": "2026-08-24T00:00:00+00:00",
        },
    )

    assert abandonment_result is expected_result
    assert event_names == ["fence", "abandon"]
    fence_mock.assert_awaited_once_with(
        connection,
        recovery_fence.db,
        schema_name="mrf",
        snapshot_id="ptg2:202607:test",
        internal_run_id="ptg2:test-run",
    )
    abandonment_kwargs = abandonment_mock.await_args.kwargs
    assert abandonment_kwargs["schema_name"] == "mrf"
    assert abandonment_kwargs["snapshot_key"] == 491
    assert abandonment_kwargs["build_token"] == "owned"
    assert callable(abandonment_kwargs["callbacks"].step_guard)


@pytest.mark.asyncio
async def test_recovery_fence_runs_optional_step_guard(monkeypatch) -> None:
    connection = object()
    step_guard = AsyncMock()
    monkeypatch.setattr(recovery_fence, "lock_writable_snapshot", AsyncMock())
    monkeypatch.setattr(
        recovery_fence,
        "load_recovery_attempt_fence",
        AsyncMock(return_value=dict(_ACTIVE_FENCE)),
    )

    async def invoke_guard(*_args, **kwargs):
        await kwargs["callbacks"].step_guard(connection)

    monkeypatch.setattr(
        recovery_fence,
        "abandon_owned_v4_layout",
        AsyncMock(side_effect=invoke_guard),
    )

    await recovery_fence.abandon_writable_v4_layout(
        schema_name="mrf",
        snapshot_id="ptg2:202607:test",
        import_run_id="ptg2:test-run",
        snapshot_key=491,
        build_token="owned",
        expected_fence_by_field={
            "fence_nonce": "fence-nonce",
            "created_at": "2026-08-24T00:00:00+00:00",
        },
        step_guard=step_guard,
    )

    step_guard.assert_awaited_once_with(connection)


@pytest.mark.asyncio
async def test_reconciled_fence_blocks_physical_mutation(monkeypatch) -> None:
    fence_mock = AsyncMock(
        side_effect=recovery_fence.StaleMetadataFenceError("reconciled")
    )
    abandonment_mock = AsyncMock()
    monkeypatch.setattr(recovery_fence, "lock_writable_snapshot", fence_mock)
    monkeypatch.setattr(
        recovery_fence,
        "abandon_owned_v4_layout",
        abandonment_mock,
    )

    async def invoke_guard(*_args, **kwargs):
        await kwargs["callbacks"].step_guard(object())

    abandonment_mock.side_effect = invoke_guard

    with pytest.raises(
        recovery_fence.PTG2V4RecoveryConflict,
        match="active writable attempt fence",
    ):
        await recovery_fence.abandon_writable_v4_layout(
            schema_name="mrf",
            snapshot_id="ptg2:202607:test",
            import_run_id="ptg2:test-run",
            snapshot_key=491,
            build_token="owned",
            expected_fence_by_field={
                "fence_nonce": "fence-nonce",
                "created_at": "2026-08-24T00:00:00+00:00",
            },
        )

    fence_mock.assert_awaited_once()
    abandonment_mock.assert_awaited_once()
