# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import pytest

from process import mrf_source_discovery as discovery
from process.mrf_discovery_checkpoints import DatabaseDiscoveryCheckpointStore
from process.mrf_source_discovery import (
    DiscoverySourceBatchContext,
    DiscoverySourceProcessingOptions,
    DiscoverySourceProcessPool,
    SourceBatchSummary,
    SourceProcessResult,
    _discovery_http_connection_limit,
    _discovery_http_per_host_limit,
    _discovery_process_http_limits,
    _discovery_process_worker_count,
    _discovery_target_concurrency,
    _consume_bounded_crawl_targets,
    _consume_completed_crawl_tasks,
    _execute_discovery_source_batch,
    _process_discovery_source_record,
)


async def _execute_all_source_callbacks(**batch_options):
    for source_record in batch_options["source_records"]:
        await batch_options["process_source"](source_record)
    return SourceBatchSummary(
        root_run_id=batch_options["root_run_id"],
        source_set_count=2,
        source_set_sha256="source-digest",
        completed_source_count=2,
        completed_source_set_sha256="source-digest",
        failed_source_count=0,
        urls_checked=0,
        plans_discovered=0,
        files_discovered=0,
        bytes_streamed=0,
    )


def _shared_session_batch_context() -> DiscoverySourceBatchContext:
    return DiscoverySourceBatchContext(
        root_run_id="run_root",
        owner_run_id="run_owner",
        source_records=[
            {"source_id": "source_alpha"},
            {"source_id": "source_beta"},
        ],
        concurrency=17,
        process_workers=1,
        processing_options=DiscoverySourceProcessingOptions(
            test_mode=False,
            check_urls=True,
            crawl=True,
            observation_run_id="run_owner",
            max_toc_bytes=1024,
            crawl_target_limit=None,
            target_concurrency=4,
        ),
    )


def _reject_process_pool(_process_workers):
    raise AssertionError("async execution constructed the process pool")


def test_process_worker_count_is_bounded(monkeypatch):
    monkeypatch.setenv("HLTHPRT_MRF_DISCOVERY_PROCESS_WORKERS", "4")
    assert _discovery_process_worker_count(10) == 4
    assert _discovery_process_worker_count(2) == 2

    monkeypatch.setenv("HLTHPRT_MRF_DISCOVERY_PROCESS_WORKERS", "200")
    assert _discovery_process_worker_count(200) == 8

    monkeypatch.setenv("HLTHPRT_MRF_DISCOVERY_PROCESS_WORKERS", "invalid")
    assert _discovery_process_worker_count(10) == 1


def test_async_discovery_limits_are_bounded(monkeypatch):
    monkeypatch.setenv("HLTHPRT_MRF_DISCOVERY_TARGET_CONCURRENCY", "200")
    monkeypatch.setenv("HLTHPRT_MRF_DISCOVERY_HTTP_CONNECTION_LIMIT", "40")
    monkeypatch.setenv("HLTHPRT_MRF_DISCOVERY_HTTP_PER_HOST_LIMIT", "3")

    assert _discovery_target_concurrency() == 16
    assert _discovery_http_connection_limit(32, 16) == 40
    assert _discovery_http_per_host_limit() == 3
    assert _discovery_process_http_limits(40, 4, 4) == (10, 1)


@pytest.mark.asyncio
async def test_isolated_source_uses_explicit_process_connector_budget(monkeypatch):
    observed_session_option_list = []

    @asynccontextmanager
    async def fake_http_session(**kwargs):
        observed_session_option_list.append(kwargs)
        yield object()

    monkeypatch.setattr(discovery, "_discovery_http_session", fake_http_session)

    await _process_discovery_source_record(
        {"source_id": "source_alpha"},
        DiscoverySourceProcessingOptions(
            test_mode=False,
            check_urls=False,
            crawl=False,
            observation_run_id="run_owner",
            max_toc_bytes=1024,
            crawl_target_limit=None,
            http_connection_limit=10,
            http_per_host_limit=1,
        ),
    )

    assert observed_session_option_list == [
        {
            "existing_session": None,
            "timeout": observed_session_option_list[0]["timeout"],
            "connector_limit": 10,
            "connector_limit_per_host": 1,
        }
    ]


@pytest.mark.asyncio
async def test_source_processing_uses_bounded_target_concurrency(monkeypatch):
    observed_concurrency_list = []

    async def fake_crawl(_source_rows, **kwargs):
        observed_concurrency_list.append(kwargs["concurrency"])
        return 2, 3, []

    monkeypatch.setattr(discovery, "_crawl_toc_metadata", fake_crawl)
    monkeypatch.setattr(
        discovery,
        "_is_source_row_importable",
        lambda _source_record: True,
    )

    source_result = await _process_discovery_source_record(
        {"source_id": "source_alpha"},
        DiscoverySourceProcessingOptions(
            test_mode=False,
            check_urls=False,
            crawl=True,
            observation_run_id="run_owner",
            max_toc_bytes=1024,
            crawl_target_limit=None,
            target_concurrency=4,
        ),
        session=object(),
    )

    assert observed_concurrency_list == [4]
    assert source_result.plans_discovered == 2
    assert source_result.files_discovered == 3


class _SharedSessionBatchProbe:
    """Capture one-process session and concurrency propagation."""

    def __init__(self) -> None:
        self.shared_session = object()
        self.observed_session_list = []
        self.observed_target_semaphore_list = []
        self.observed_executor_concurrency_list = []
        self.observed_session_option_list = []

    @asynccontextmanager
    async def http_session(self, **option_by_name):
        self.observed_session_option_list.append(option_by_name)
        yield self.shared_session

    async def process_source(
        self,
        _source_record,
        _processing_options,
        *,
        session=None,
        target_semaphore=None,
    ):
        self.observed_session_list.append(session)
        self.observed_target_semaphore_list.append(target_semaphore)
        return SourceProcessResult()

    async def execute_batch(self, **option_by_name):
        self.observed_executor_concurrency_list.append(
            option_by_name["concurrency"]
        )
        return await _execute_all_source_callbacks(**option_by_name)


@pytest.mark.asyncio
async def test_async_batch_reuses_session_and_forwards_source_concurrency(monkeypatch):
    """One-process mode shares its connector across concurrent sources."""

    probe = _SharedSessionBatchProbe()

    monkeypatch.setattr(discovery, "_discovery_http_session", probe.http_session)
    monkeypatch.setattr(
        discovery,
        "_process_discovery_source_record",
        probe.process_source,
    )
    monkeypatch.setattr(
        discovery,
        "execute_checkpointed_source_batch",
        probe.execute_batch,
    )
    monkeypatch.setattr(
        discovery,
        "DiscoverySourceProcessPool",
        _reject_process_pool,
    )

    await _execute_discovery_source_batch(
        _shared_session_batch_context(),
        DatabaseDiscoveryCheckpointStore(),
    )

    assert probe.observed_executor_concurrency_list == [17]
    assert probe.observed_session_list == [
        probe.shared_session,
        probe.shared_session,
    ]
    assert len({
        id(semaphore)
        for semaphore in probe.observed_target_semaphore_list
    }) == 1
    assert probe.observed_target_semaphore_list[0]._value == 34
    assert probe.observed_session_option_list == [
        {
            "existing_session": None,
            "timeout": probe.observed_session_option_list[0]["timeout"],
            "connector_limit": 34,
            "connector_limit_per_host": 4,
        }
    ]


class _BoundedTargetConsumerProbe:
    """Hold a rolling target window and record its bounded progress."""

    def __init__(self) -> None:
        self.release_targets = asyncio.Event()
        self.initial_window_started = asyncio.Event()
        self.started_target_count = 0
        self.completed_target_urls = []

    async def crawl(self, crawl_target):
        self.started_target_count += 1
        if self.started_target_count == 3:
            self.initial_window_started.set()
        await self.release_targets.wait()
        return ([], [], [], crawl_target.url)

    async def record(self, result):
        self.completed_target_urls.append(result[3])


@pytest.mark.asyncio
async def test_target_consumer_creates_only_one_bounded_window() -> None:
    probe = _BoundedTargetConsumerProbe()

    crawl_targets = [
        discovery.CrawlTarget(
            source={"source_id": "source_alpha"},
            url=f"https://example.test/{target_index}",
        )
        for target_index in range(100)
    ]
    consumer_task = asyncio.create_task(
        _consume_bounded_crawl_targets(
            crawl_targets,
            concurrency=3,
            crawl_target=probe.crawl,
            on_result=probe.record,
        )
    )

    await asyncio.wait_for(probe.initial_window_started.wait(), timeout=1)
    await asyncio.sleep(0)
    assert probe.started_target_count == 3

    probe.release_targets.set()
    await consumer_task

    assert probe.started_target_count == 100
    assert len(probe.completed_target_urls) == 100


@pytest.mark.asyncio
async def test_result_handler_failure_stops_remaining_crawl_tasks():
    slow_task_started = asyncio.Event()
    slow_task_stopped = asyncio.Event()

    async def slow_crawl():
        slow_task_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            slow_task_stopped.set()

    async def fail_result_handler(_crawl_result):
        raise RuntimeError("catalog write failed")

    slow_task = asyncio.create_task(slow_crawl())
    await slow_task_started.wait()
    completed_task = asyncio.create_task(asyncio.sleep(0, result=([], [], [], "fast")))

    with pytest.raises(RuntimeError, match="catalog write failed"):
        await _consume_completed_crawl_tasks(
            [completed_task, slow_task],
            fail_result_handler,
        )

    assert slow_task_stopped.is_set()


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
