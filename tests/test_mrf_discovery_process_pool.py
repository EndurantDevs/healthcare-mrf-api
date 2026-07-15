# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio

import pytest

from process import mrf_source_discovery as discovery
from process.mrf_discovery_checkpoints import DatabaseDiscoveryCheckpointStore
from process.mrf_source_discovery import (
    DiscoverySourceBatchContext,
    DiscoverySourceProcessingOptions,
    DiscoverySourceProcessPool,
    _discovery_process_worker_count,
    _execute_discovery_source_batch,
)


def test_process_worker_count_is_bounded(monkeypatch):
    monkeypatch.setenv("HLTHPRT_MRF_DISCOVERY_PROCESS_WORKERS", "4")
    assert _discovery_process_worker_count(10) == 4
    assert _discovery_process_worker_count(2) == 2

    monkeypatch.setenv("HLTHPRT_MRF_DISCOVERY_PROCESS_WORKERS", "200")
    assert _discovery_process_worker_count(200) == 8

    monkeypatch.setenv("HLTHPRT_MRF_DISCOVERY_PROCESS_WORKERS", "invalid")
    assert _discovery_process_worker_count(10) == 1


@pytest.mark.asyncio
async def test_spawned_process_pool_returns_source_result():
    process_pool = DiscoverySourceProcessPool(2)
    try:
        source_result = await process_pool.process_source(
            {"source_id": "source_process_test"},
            DiscoverySourceProcessingOptions(
                test_mode=True,
                check_urls=False,
                crawl=False,
                observation_run_id="run_process_test",
                max_toc_bytes=1024,
                crawl_target_limit=None,
            ),
        )
    except BaseException:
        await process_pool.terminate()
        raise
    await process_pool.close()

    assert source_result.urls_checked == 0
    assert source_result.plans_discovered == 0
    assert source_result.files_discovered == 0
    assert source_result.bytes_streamed == 0


@pytest.mark.asyncio
async def test_batch_cancellation_terminates_processes_before_retry(monkeypatch):
    pool_state_by_key = {"closed": False, "terminated": False}

    class FakeProcessPool:
        def __init__(self, process_workers):
            assert process_workers == 4

        async def process_source(self, _source_record, _processing_options):
            raise AssertionError("canceled batch submitted source work")

        async def close(self):
            pool_state_by_key["closed"] = True

        async def terminate(self):
            pool_state_by_key["terminated"] = True

    async def cancel_batch(**_kwargs):
        raise asyncio.CancelledError

    monkeypatch.setattr(discovery, "DiscoverySourceProcessPool", FakeProcessPool)
    monkeypatch.setattr(
        discovery,
        "execute_checkpointed_source_batch",
        cancel_batch,
    )

    with pytest.raises(asyncio.CancelledError):
        await _execute_discovery_source_batch(
            DiscoverySourceBatchContext(
                root_run_id="run_root",
                owner_run_id="run_owner",
                source_records=[{"source_id": "source_alpha"}],
                concurrency=10,
                process_workers=4,
                processing_options=DiscoverySourceProcessingOptions(
                    test_mode=False,
                    check_urls=True,
                    crawl=True,
                    observation_run_id="run_owner",
                    max_toc_bytes=1024,
                    crawl_target_limit=None,
                ),
            ),
            DatabaseDiscoveryCheckpointStore(),
        )

    assert pool_state_by_key == {"closed": False, "terminated": True}
