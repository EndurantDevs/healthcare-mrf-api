# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Unit ordering proof for failed-layout recovery attempt fencing."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_v4_failed_layout_fence as recovery_fence


@pytest.mark.asyncio
async def test_recovery_fence_precedes_physical_mutation(monkeypatch) -> None:
    event_names: list[str] = []
    expected_result = object()

    async def record_fence(*_args, **_kwargs) -> None:
        event_names.append("fence")

    async def record_abandonment(*_args, **_kwargs):
        event_names.append("abandon")
        return expected_result

    fence_mock = AsyncMock(side_effect=record_fence)
    abandonment_mock = AsyncMock(side_effect=record_abandonment)
    monkeypatch.setattr(recovery_fence, "lock_writable_snapshot", fence_mock)
    monkeypatch.setattr(
        recovery_fence,
        "abandon_owned_v4_layout",
        abandonment_mock,
    )
    connection = object()

    abandonment_result = await recovery_fence.abandon_writable_v4_layout(
        connection,
        schema_name="mrf",
        snapshot_id="ptg2:202607:test",
        import_run_id="ptg2:test-run",
        snapshot_key=491,
        build_token="owned",
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
    abandonment_mock.assert_awaited_once_with(
        schema_name="mrf",
        snapshot_key=491,
        build_token="owned",
        executor=connection,
    )


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

    with pytest.raises(
        recovery_fence.PTG2V4RecoveryConflict,
        match="active writable attempt fence",
    ):
        await recovery_fence.abandon_writable_v4_layout(
            object(),
            schema_name="mrf",
            snapshot_id="ptg2:202607:test",
            import_run_id="ptg2:test-run",
            snapshot_key=491,
            build_token="owned",
        )

    fence_mock.assert_awaited_once()
    abandonment_mock.assert_not_awaited()
