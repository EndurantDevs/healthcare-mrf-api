# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import ptg_allowed_amount_blank_evidence, ptg_control
from process.ptg_allowed_amount_blank import ALLOWED_AMOUNT_BLANK_ERROR
from tests.ptg_blank_terminal_support import blank_ordinary_result


async def _allow_active_run(_run_id):
    return None


@pytest.fixture(autouse=True)
def _admit_unit_ptg_run(monkeypatch):
    monkeypatch.setattr(
        ptg_control, "guard_ptg_worker_start", AsyncMock(return_value=None)
    )


@pytest.mark.asyncio
async def test_ptg_control_persists_durable_allowed_amount_blank(monkeypatch):
    blank_metrics_map = {
        "status": "blank",
        "snapshot_id": "ptg2:202608:snapshot-neutral",
        "file_domains": ["allowed_amounts"],
    }

    async def fake_ptg_main(**_kwargs):
        raise RuntimeError(ALLOWED_AMOUNT_BLANK_ERROR)

    mark_control_run = AsyncMock(return_value=True)
    load_blank_metrics = AsyncMock(return_value=blank_metrics_map)
    monkeypatch.setattr(ptg_control, "ptg_main", fake_ptg_main)
    monkeypatch.setattr(ptg_control, "mark_control_run", mark_control_run)
    monkeypatch.setattr(ptg_control, "_flush_terminal_status_events", AsyncMock())
    monkeypatch.setattr(
        ptg_control, "load_blank_failure_metrics", load_blank_metrics
    )
    monkeypatch.setattr(ptg_control, "_stale_ptg_job_result", _allow_active_run)

    with pytest.raises(RuntimeError, match=ALLOWED_AMOUNT_BLANK_ERROR):
        await ptg_control.ptg_control_start(
            {},
            {
                "run_id": "run-blank-neutral",
                "params": {"source_key": "source-neutral"},
            },
        )

    assert mark_control_run.await_args_list[-1].kwargs["metrics"] == blank_metrics_map
    load_blank_metrics.assert_awaited_once()


@pytest.mark.asyncio
async def test_ptg_control_preserves_failure_when_blank_lookup_fails(monkeypatch):
    mark_control_run = AsyncMock(return_value=True)
    monkeypatch.setattr(
        ptg_control,
        "ptg_main",
        AsyncMock(side_effect=RuntimeError(ALLOWED_AMOUNT_BLANK_ERROR)),
    )
    monkeypatch.setattr(ptg_control, "mark_control_run", mark_control_run)
    monkeypatch.setattr(ptg_control, "_flush_terminal_status_events", AsyncMock())
    monkeypatch.setattr(
        ptg_control,
        "validated_worker_frozen_rate_params",
        AsyncMock(side_effect=lambda _task, params_by_name: params_by_name),
    )
    monkeypatch.setattr(ptg_control, "_stale_ptg_job_result", _allow_active_run)
    monkeypatch.setattr(
        ptg_allowed_amount_blank_evidence,
        "db",
        SimpleNamespace(
            execute=AsyncMock(side_effect=RuntimeError("blank lookup failed"))
        ),
    )

    params_by_name = {
        "source_file_import_id": "source-import-neutral",
        "import_id": "source-import-neutral",
        "source_key": "source-neutral",
        "import_month": "2026-08",
        "plan_ids": ["plan-neutral"],
        "plan_market_types": ["group"],
        "max_files": 1,
        "allowed_url": "https://example.test/allowed.json",
    }
    with pytest.raises(RuntimeError, match=ALLOWED_AMOUNT_BLANK_ERROR):
        await ptg_control.ptg_control_start(
            {}, {"run_id": "run-blank-neutral", "params": params_by_name}
        )

    failed_mark = mark_control_run.await_args_list[-1].kwargs
    assert failed_mark["error"] == {
        "code": "ptg_import_failed",
        "message": ALLOWED_AMOUNT_BLANK_ERROR,
    }
    assert "metrics" not in failed_mark


@pytest.mark.asyncio
async def test_ptg_control_rejects_conflicting_allowed_amount_blank(monkeypatch):
    state = blank_ordinary_result(monkeypatch)

    class Result:
        def __init__(self, value):
            self.value = value

        def scalar_one_or_none(self):
            return self.value

    execute = AsyncMock()
    monkeypatch.setattr(
        ptg_allowed_amount_blank_evidence,
        "db",
        SimpleNamespace(execute=execute),
    )

    async def load_metrics():
        execute.side_effect = (
            Result(state["engine_run"]),
            Result(state["engine_snapshot"]),
        )
        return await ptg_allowed_amount_blank_evidence.load_blank_failure_metrics(
            state["run"].params,
            state["run"].error,
        )

    assert await load_metrics() == state["run"].metrics
    state["engine_snapshot"].manifest["allowed_amount_lane"][
        "successful_files"
    ][0]["summary"]["allowed_amount_plans"] += 1
    assert await load_metrics() == {}
