# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native publication capacity must match its actual bounded Publisher pool."""

import importlib

import pytest
from sqlalchemy.engine import make_url

from tests import provider_directory_source_postgres_support as support

fhir = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.asyncio
@pytest.mark.parametrize("pool_size,capacity,overflow", [(None, 5, 10), (6, 6, 0)])
async def test_declared_pool_matches_native_workers(monkeypatch, pool_size, capacity, overflow):
    """Inspect real disconnected pools and the full, unmodified payload batch plan."""
    monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", "5")
    with monkeypatch.context() as settings:
        engine = support._source_publisher_engine(
            make_url("postgresql+asyncpg://synthetic@127.0.0.1/synthetic"),
            "synthetic_publisher",
            settings,
            pool_size=pool_size,
        )
        try:
            assert engine.pool.size() == capacity and engine.pool._max_overflow == overflow
            assert engine.url.username == "synthetic_publisher"
            assert fhir._provider_directory_database_pool_capacity() == capacity
            plan = fhir._provider_directory_profile_batch_plan(
                ("cms-npd",), ("cms-npd",), ("synthetic-dataset",), has_existing_artifacts=False
            )
            assert len(plan.evidence_batches) == 115 and len(plan.compact_batches) == 400
            workers = 1 if pool_size is None else 2
            assert plan.evidence_window_size == plan.compact_window_size == workers
            assert plan.evidence_waves == tuple((start, min(start + workers, 115)) for start in range(0, 115, workers))
            assert plan.compact_waves == tuple((start, start + workers) for start in range(0, 400, workers))
        finally:
            await engine.dispose()
    assert fhir._provider_directory_database_pool_capacity() == 5
