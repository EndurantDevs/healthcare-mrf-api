# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Step-level safety gates for failed PTG V4 layout recovery."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_v4_failed_layout_recovery as recovery
from tests.test_ptg2_v4_failed_layout_recovery import (
    _failed_owner_fixture,
    _recovery_context,
)


class _ConnectionContext:
    def __init__(self, connection: object) -> None:
        self.connection = connection

    async def __aenter__(self) -> object:
        return self.connection

    async def __aexit__(self, *_exc: object) -> None:
        return None


def _guard_context(report_by_field: dict[str, object]) -> recovery._RecoveryContext:
    context = _recovery_context()
    return recovery._RecoveryContext(
        snapshot_id=context.snapshot_id,
        import_run_id=context.import_run_id,
        snapshot_key=context.snapshot_key,
        build_token=context.build_token,
        expected_report=report_by_field,
        plan_by_field=context.plan_by_field,
        fence_nonce=context.fence_nonce,
        fence_created_at=context.fence_created_at,
    )


@pytest.mark.asyncio
async def test_release_context_rejects_changed_physical_state(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        recovery,
        "abandon_writable_v4_layout",
        AsyncMock(
            return_value=SimpleNamespace(
                logical_layout_count=0,
                candidate_hash_count=0,
                stored_bytes=0,
            )
        ),
    )
    monkeypatch.setattr(
        recovery,
        "load_recovery_postconditions",
        AsyncMock(return_value={}),
    )
    with pytest.raises(RuntimeError, match="ownership changed"):
        await recovery._release_recovery_context(
            schema_name="mrf",
            context=_recovery_context(),
        )


@pytest.mark.asyncio
async def test_guard_recovery_step_rejects_changed_report(monkeypatch) -> None:
    snapshot_by_field, run_by_field, layout_by_field, _ = _failed_owner_fixture()
    monkeypatch.setattr(
        recovery,
        "_owner_records",
        AsyncMock(return_value=(snapshot_by_field, run_by_field, layout_by_field)),
    )

    with pytest.raises(recovery.PTG2V4RecoveryConflict, match="owner report changed"):
        await recovery._guard_recovery_step(
            object(),
            schema_name="mrf",
            context=_guard_context({"changed": True}),
        )


@pytest.mark.asyncio
async def test_guard_recovery_step_rejects_changed_build_token(monkeypatch) -> None:
    snapshot_by_field, run_by_field, layout_by_field, count_by_name = (
        _failed_owner_fixture()
    )
    layout_by_field["build_token"] = "b" * 32
    monkeypatch.setattr(
        recovery,
        "_owner_records",
        AsyncMock(return_value=(snapshot_by_field, run_by_field, layout_by_field)),
    )
    monkeypatch.setattr(
        recovery,
        "load_reference_counts",
        AsyncMock(return_value=count_by_name),
    )

    with pytest.raises(recovery.PTG2V4RecoveryConflict, match="ownership changed"):
        await recovery._guard_recovery_step(
            object(),
            schema_name="mrf",
            context=_guard_context(dict(run_by_field["report"])),
        )


@pytest.mark.asyncio
async def test_finalization_requires_every_physical_owner_zero(monkeypatch) -> None:
    monkeypatch.setattr(
        recovery,
        "load_recovery_postconditions",
        AsyncMock(return_value={"build_pins": 1}),
    )

    with pytest.raises(RuntimeError, match="left physical ownership rows"):
        await recovery._finalize_recovery_step(
            object(),
            schema_name="mrf",
            context=_recovery_context(),
            abandonment=SimpleNamespace(logical_layout_count=1),
        )


@pytest.mark.asyncio
async def test_finalization_requires_one_abandoned_layout() -> None:
    with pytest.raises(recovery.PTG2V4RecoveryConflict, match="ownership changed"):
        await recovery._finalize_recovery_step(
            object(),
            schema_name="mrf",
            context=_recovery_context(),
            abandonment=SimpleNamespace(logical_layout_count=0),
        )


@pytest.mark.asyncio
async def test_release_context_requires_sealed_recovery_marker(monkeypatch) -> None:
    monkeypatch.setattr(
        recovery,
        "abandon_writable_v4_layout",
        AsyncMock(
            return_value=SimpleNamespace(
                logical_layout_count=1,
                candidate_hash_count=0,
                stored_bytes=0,
            )
        ),
    )
    monkeypatch.setattr(
        recovery,
        "load_completed_recovery_result",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(recovery.db, "acquire", lambda: _ConnectionContext(object()))

    with pytest.raises(RuntimeError, match="did not seal durable evidence"):
        await recovery._release_recovery_context(
            schema_name="mrf",
            context=_recovery_context(),
        )
