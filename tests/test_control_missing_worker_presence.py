from __future__ import annotations

import datetime as dt
import types
from unittest.mock import AsyncMock

import pytest

from api import control_imports, control_workers


RUN_ID = "run_missing_worker"


def _run(importer: str = "plan-pricing-projection") -> dict[str, object]:
    return {
        "run_id": RUN_ID,
        "engine": "healthcare-mrf-api",
        "importer": importer,
        "family": "mrf",
        "status": "running",
        "phase_detail": "build running",
        "params": {},
        "created_at": dt.datetime(2026, 8, 27, 22, 13),
        "started_at": dt.datetime(2026, 8, 27, 22, 24),
        "heartbeat_at": dt.datetime(2026, 8, 27, 22, 30),
        "progress": {},
        "metrics": {
            "queue": "arq:PTGCandidateAudit",
            "worker_class": "process.PTGCandidateAudit",
            "job_id": f"plan_pricing_{importer.removeprefix('plan-pricing-')}_{RUN_ID}",
        },
    }


class _QueryResult:
    def __init__(self, run):
        self.run = run

    def scalar_one_or_none(self):
        return self.run


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("importer", "sync_expected"),
    (("plan-pricing-projection", False), ("npi", True)),
)
async def test_get_worker_sync_is_read_only_only_for_plan_pricing(
    monkeypatch,
    importer,
    sync_expected,
):
    durable_run = types.SimpleNamespace(**_run(importer))
    legacy_sync = AsyncMock(
        return_value={**_run(importer), "status": "failed"}
    )
    monkeypatch.setattr(
        control_imports,
        "db",
        types.SimpleNamespace(
            execute=AsyncMock(return_value=_QueryResult(durable_run))
        ),
    )
    monkeypatch.setattr(control_imports, "read_live_progress", lambda _run_id: None)
    monkeypatch.setattr(control_imports, "_sync_terminal_worker_failure", legacy_sync)
    monkeypatch.setattr(
        control_imports,
        "_allowed_amount_blank_terminal_metrics",
        AsyncMock(return_value=None),
    )

    public_run = await control_imports.get_import_run(RUN_ID)

    assert public_run["status"] == ("failed" if sync_expected else "running")
    assert legacy_sync.await_count == int(sync_expected)


@pytest.mark.parametrize(
    "payload",
    (
        {"run_id": RUN_ID, "importer": "plan-pricing-projection", "queue": "arq:PTG"},
        {
            "run_id": RUN_ID,
            "importer": "plan-pricing-prewarm",
            "worker_class": "process.PTGNormal",
        },
    ),
)
def test_exact_worker_presence_rejects_noncanonical_plan_pricing_spec(payload):
    with pytest.raises(ValueError, match="conflicts with importer"):
        control_workers._exact_worker_spec(payload)


def test_exact_worker_presence_requires_exact_identity():
    with pytest.raises(RuntimeError, match="identity is unavailable"):
        control_workers._exact_worker_spec({})


@pytest.mark.asyncio
async def test_arq_presence_uses_every_exact_canonical_key(monkeypatch):
    recorded_calls: list[tuple[str, str]] = []

    class _Pipeline:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        def zscore(self, queue, job_id):
            recorded_calls.append(("zscore", f"{queue}:{job_id}"))

        def exists(self, key):
            recorded_calls.append(("exists", key))

        async def execute(self):
            return [None, 0, 0, 0, 1]

    class _Redis:
        def __init__(self):
            self.aclose = AsyncMock()

        def pipeline(self, *, transaction):
            assert transaction is True
            return _Pipeline()

    redis = _Redis()
    monkeypatch.setattr(
        control_imports,
        "create_pool",
        AsyncMock(return_value=redis),
    )

    presence_by_field = await control_imports._arq_worker_presence(_run())

    job_id = f"plan_pricing_projection_{RUN_ID}"
    assert recorded_calls == [
        ("zscore", f"arq:PTGCandidateAudit:{job_id}"),
        ("exists", f"arq:job:{job_id}"),
        ("exists", f"arq:retry:{job_id}"),
        ("exists", f"arq:in-progress:{job_id}"),
        ("exists", f"arq:result:{job_id}"),
    ]
    assert presence_by_field["result"] is True
    redis.aclose.assert_awaited_once_with(close_connection_pool=True)


@pytest.mark.asyncio
async def test_arq_presence_closes_pool_when_pipeline_fails(monkeypatch):
    class _Pipeline:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        def zscore(self, *_args):
            return None

        def exists(self, *_args):
            return None

        async def execute(self):
            raise RuntimeError("redis unavailable")

    class _Redis:
        def __init__(self):
            self.aclose = AsyncMock()

        def pipeline(self, *, transaction):
            assert transaction is True
            return _Pipeline()

    redis = _Redis()
    monkeypatch.setattr(
        control_imports,
        "create_pool",
        AsyncMock(return_value=redis),
    )

    with pytest.raises(RuntimeError, match="redis unavailable"):
        await control_imports._arq_worker_presence(_run())

    redis.aclose.assert_awaited_once_with(close_connection_pool=True)
