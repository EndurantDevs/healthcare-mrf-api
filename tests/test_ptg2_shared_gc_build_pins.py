# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact-owner PTG V4 build-pin cleanup contracts."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_shared_gc as shared_gc
from tests.ptg2_shared_gc_test_support import (
    _BUILD_TOKEN,
    _Executor,
    _abandonment_context,
)


@pytest.mark.asyncio
async def test_owned_v4_build_pin_inventory_rejects_mixed_owners():
    executor = _Executor(
        [
            {"build_token": _BUILD_TOKEN},
            {"build_token": "other-token"},
        ]
    )

    with pytest.raises(RuntimeError, match="build-pin ownership is ambiguous"):
        await shared_gc._owned_v4_build_pin_token(
            executor,
            context=_abandonment_context(),
        )

    assert not any("DELETE FROM" in statement for statement, _ in executor.all_calls)


@pytest.mark.asyncio
async def test_owned_v4_build_pin_batch_queues_before_exact_delete(monkeypatch):
    monkeypatch.setattr(
        shared_gc,
        "_is_owned_v4_layout_locked",
        AsyncMock(return_value=True),
    )
    executor = _Executor(
        [],
        [
            {
                "selected_rows": 2,
                "selected_hashes": 1,
                "resolved_hashes": 1,
                "queued_hashes": 1,
                "deleted_rows": 2,
            }
        ],
    )

    deleted = await shared_gc._delete_v4_pin_batch(
        executor,
        context=_abandonment_context(),
        build_pin_token=_BUILD_TOKEN,
    )

    assert deleted == 2
    statement, params = executor.all_calls[-1]
    assert 'INSERT INTO "mrf".ptg2_v3_gc_candidate' in statement
    assert statement.index("queued AS") < statement.index("deleted AS")
    assert params["build_pin_token"] == _BUILD_TOKEN
    assert "pin.pin_token = selected.pin_token" in statement


@pytest.mark.asyncio
async def test_owned_v4_build_pin_batch_rejects_missing_cas(monkeypatch):
    monkeypatch.setattr(
        shared_gc,
        "_is_owned_v4_layout_locked",
        AsyncMock(return_value=True),
    )
    executor = _Executor(
        [],
        [
            {
                "selected_rows": 2,
                "selected_hashes": 2,
                "resolved_hashes": 1,
                "queued_hashes": 1,
                "deleted_rows": 0,
            }
        ],
    )

    with pytest.raises(RuntimeError, match="missing CAS block"):
        await shared_gc._delete_v4_pin_batch(
            executor,
            context=_abandonment_context(),
            build_pin_token=_BUILD_TOKEN,
        )
