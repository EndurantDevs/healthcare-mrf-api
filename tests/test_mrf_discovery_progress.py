# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Discovery's checkpoint percentage must replace the initial loading value."""

from __future__ import annotations

import datetime as dt
from unittest.mock import Mock

import pytest

from process import live_progress
from process import mrf_source_discovery as discovery


@pytest.mark.parametrize("completed,total,percentage", [(1, 4, 25.0), (799, 800, 99.875), (0, 0, 0.0), (4, 4, 100.0)])
def test_checkpoint_emitter_replaces_carried_loading_percentage(monkeypatch, completed, total, percentage):
    enqueue = Mock()
    monkeypatch.setattr(discovery, "enqueue_live_progress", enqueue)
    report = discovery._source_checkpoint_progress_reporter("run_synthetic")
    report(completed, total, "source_synthetic", "succeeded")
    emitted = enqueue.call_args.kwargs
    assert (emitted["done"], emitted["total"], emitted["pct"]) == (completed, total, percentage)
    now = dt.datetime(2026, 1, 1)
    merged = live_progress._merged_live_progress_candidate(
        run_id="run_synthetic",
        context={},
        progress_by_field=emitted,
        observed_at=now.isoformat() + "Z",
        now=now,
        previous={
            "run_id": "run_synthetic",
            "attempt_id": "run_synthetic",
            "status": "running",
            "pct": 0.0,
            "unit": "providers",
            "done": 0,
            "total": 1,
        },
    )
    assert merged["pct"] == pytest.approx(min(percentage, 99.9))
