from __future__ import annotations

import pytest

from api import control_imports
from tests.test_control_terminal_queue_residue import (
    RUN_ID,
    _install_dependencies,
    _run,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "resource_class", "queue", "worker_class"),
    (
        ("failed", "small", "arq:PTGSmall", "process.PTGSmall"),
        ("succeeded", "huge", "arq:PTGHuge", "process.PTGHuge"),
    ),
)
async def test_terminal_ptg_residue_uses_persisted_lane(
    monkeypatch,
    status,
    resource_class,
    queue,
    worker_class,
):
    job_id = f"ptg_start_{RUN_ID}"
    _, _, pipeline = _install_dependencies(
        monkeypatch,
        _run(
            importer="ptg",
            status=status,
            params={},
            metrics={
                "queue": queue,
                "job_id": job_id,
                "worker_class": worker_class,
                "resource_class": resource_class,
            },
        ),
    )

    receipt_by_field = await control_imports.reconcile_terminal_queue_residue(
        RUN_ID,
        {"expected_importer": "ptg", "expected_status": status},
    )

    assert (receipt_by_field["queue"], receipt_by_field["job_id"]) == (
        queue,
        job_id,
    )
    pipeline.zrem.assert_called_once_with(queue, job_id)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "metrics_by_name",
    (
        {
            "queue": "arq:PTGSmall",
            "job_id": f"ptg_start_{RUN_ID}",
            "resource_class": "huge",
        },
        {
            "queue": "arq:PTGSmall",
            "job_id": "different-job",
            "resource_class": "small",
        },
        {
            "queue": "arq:PTGSmall",
            "job_id": f"ptg_start_{RUN_ID}",
        },
    ),
)
async def test_terminal_ptg_residue_requires_exact_identity(
    monkeypatch,
    metrics_by_name,
):
    _, _, pipeline = _install_dependencies(
        monkeypatch,
        _run(importer="ptg", params={}, metrics=metrics_by_name),
    )

    with pytest.raises(control_imports.StaleWorkerReconciliationConflict):
        await control_imports.reconcile_terminal_queue_residue(
            RUN_ID,
            {"expected_importer": "ptg", "expected_status": "failed"},
        )

    control_imports.create_pool.assert_not_awaited()
    pipeline.zrem.assert_not_called()
