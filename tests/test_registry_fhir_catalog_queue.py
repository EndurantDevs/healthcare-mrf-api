# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exercise ownership, recovery and cancellation with actual ARQ and isolated Redis."""

import asyncio
import shutil
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from arq.connections import RedisSettings, create_pool
from arq.constants import in_progress_key_prefix, job_key_prefix, result_key_prefix, retry_key_prefix
from arq.worker import func
from redis.exceptions import ConnectionError

from process import registry_fhir_catalog_queue as queue


@pytest.fixture
async def catalog_redis():
    binary = shutil.which("redis-server")
    if binary is None:
        pytest.skip("isolated queue tests require the native redis-server binary")
    with tempfile.TemporaryDirectory(prefix="catalog-redis-", dir="/tmp") as directory:
        socket = str(Path(directory) / "redis.sock")
        server, redis = None, None
        try:
            server = await asyncio.create_subprocess_exec(
                binary,
                "--port",
                "0",
                "--unixsocket",
                socket,
                "--save",
                "",
                "--appendonly",
                "no",
                "--dir",
                directory,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            async with asyncio.timeout(5):
                while not Path(socket).exists():
                    assert server.returncode is None
                    await asyncio.sleep(0.01)
                redis = await create_pool(RedisSettings(unix_socket_path=socket, conn_timeout=1, conn_retries=0))
                await redis.ping()
            yield redis
        finally:
            if redis is not None:
                await redis.aclose(close_connection_pool=True)
            if server is not None and server.returncode is None:
                server.terminate()
                try:
                    await asyncio.wait_for(server.wait(), 5)
                except TimeoutError:
                    server.kill()
                    await server.wait()
            assert server is None or server.returncode is not None


@pytest.fixture
async def workers(catalog_redis, monkeypatch):
    monkeypatch.setattr(queue, "CLAIM_SECONDS", 0.3)
    monkeypatch.setattr(queue, "RENEW_SECONDS", 0.05)
    monkeypatch.setattr(queue, "REDIS_SECONDS", 0.1)
    opened_workers = []

    def create(body, queue_name=None):
        worker = queue.FHIRCatalogWorker(
            functions=[func(body, name="control_single_job_start", max_tries=1)],
            queue_name=queue_name or "catalog-test:" + uuid4().hex,
            redis_pool=catalog_redis,
            job_timeout=None,
            max_jobs=1,
            max_tries=1,
            handle_signals=False,
        )
        worker.ctx["redis"] = catalog_redis
        opened_workers.append(worker)
        return worker

    try:
        yield create
    finally:
        tasks = [task for worker in opened_workers for task in worker.tasks.values()]
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _start(worker):
    job = await worker.pool.enqueue_job("control_single_job_start", _queue_name=worker.queue_name)
    await worker.start_jobs([job.job_id.encode()])
    return job


async def test_long_job_renews_claim_and_retention_without_a_runtime_timeout(workers):
    entered, finish = asyncio.Event(), asyncio.Event()

    async def body(_ctx):
        entered.set()
        await finish.wait()
        return "published"

    first = workers(body)
    second = workers(body, first.queue_name)
    job = await _start(first)
    await asyncio.wait_for(entered.wait(), 1)
    keys = [prefix + job.job_id for prefix in (in_progress_key_prefix, job_key_prefix, retry_key_prefix)]
    for key in keys[1:]:
        await first.pool.pexpire(key, 150)
    token = await first.pool.get(keys[0])
    await asyncio.sleep(0.45)
    assert first.job_timeout_s is None
    assert await first.pool.get(keys[0]) == token
    assert all([await first.pool.pttl(key) > 0 for key in keys])
    await second.start_jobs([job.job_id.encode()])
    assert not second.tasks and second.job_counter == 0
    finish.set()
    await asyncio.wait_for(first.tasks[job.job_id], 1)
    assert await job.result(timeout=1) == "published"
    assert await first.pool.exists(*keys) == 0
    assert await first.pool.zscore(first.queue_name, job.job_id) is None
    assert not first._claims


async def test_lost_claim_cancels_work_without_touching_replacement_state(workers):
    entered, canceled = asyncio.Event(), asyncio.Event()

    async def body(_ctx):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            canceled.set()

    worker = workers(body)
    job = await _start(worker)
    await asyncio.wait_for(entered.wait(), 1)
    replacements_by_key = {
        in_progress_key_prefix + job.job_id: b"replacement-owner",
        job_key_prefix + job.job_id: b"replacement-job",
        retry_key_prefix + job.job_id: b"replacement-retry",
        result_key_prefix + job.job_id: b"replacement-result",
    }
    for key, value in replacements_by_key.items():
        await worker.pool.set(key, value, px=5000)
    await asyncio.wait_for(worker.tasks[job.job_id], 1)
    assert canceled.is_set()
    for key, value in replacements_by_key.items():
        assert await worker.pool.get(key) == value
    assert await worker.pool.zscore(worker.queue_name, job.job_id) is not None
    assert not worker._claims


@pytest.mark.parametrize("failure", ["error", "stall"])
async def test_renewal_io_failure_cancels_work_and_preserves_normal_retry_cleanup(workers, monkeypatch, failure):
    entered, canceled = asyncio.Event(), asyncio.Event()

    async def body(_ctx):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            canceled.set()

    worker = workers(body)
    job = await _start(worker)
    await asyncio.wait_for(entered.wait(), 1)

    async def fail(*_args):
        if failure == "stall":
            await asyncio.Event().wait()
        raise ConnectionError("test renewal failure")

    monkeypatch.setattr(worker.pool, "eval", fail)
    await asyncio.wait_for(worker.tasks[job.job_id], 1)
    assert canceled.is_set()
    assert await worker.pool.get(in_progress_key_prefix + job.job_id) is None
    assert await worker.pool.zscore(worker.queue_name, job.job_id) is not None
    assert not worker._claims


async def test_explicit_cancellation_releases_only_its_claim(workers):
    entered, canceled = asyncio.Event(), asyncio.Event()

    async def body(_ctx):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            canceled.set()

    worker = workers(body)
    job = await _start(worker)
    await asyncio.wait_for(entered.wait(), 1)
    worker.tasks[job.job_id].cancel()
    await asyncio.wait_for(worker.tasks[job.job_id], 1)
    assert canceled.is_set()
    assert not await worker.pool.exists(in_progress_key_prefix + job.job_id)
    assert await worker.pool.zscore(worker.queue_name, job.job_id) is not None
    assert not worker._claims


async def test_expired_claim_recovery_cannot_be_overwritten_by_previous_worker(workers, monkeypatch):
    entered, finish = asyncio.Event(), asyncio.Event()
    calls = []

    async def body(_ctx):
        calls.append("start")
        entered.set()
        await finish.wait()
        return "old-result"

    first = workers(body)
    second = workers(body, first.queue_name)
    job = await _start(first)
    await asyncio.wait_for(entered.wait(), 1)
    monkeypatch.setattr(queue, "RENEW_SECONDS", 10)
    await first.pool.pexpire(in_progress_key_prefix + job.job_id, 1)
    await asyncio.sleep(0.02)
    await second.start_jobs([job.job_id.encode()])
    await asyncio.wait_for(second.tasks[job.job_id], 1)
    recovered = await first.pool.get(result_key_prefix + job.job_id)
    assert recovered is not None
    finish.set()
    await asyncio.wait_for(first.tasks[job.job_id], 1)
    assert await first.pool.get(result_key_prefix + job.job_id) == recovered
    assert calls == ["start"]
    assert not first._claims and not second._claims


async def test_concurrent_claims_start_exactly_one_worker(workers):
    entered, finish = asyncio.Event(), asyncio.Event()
    calls = []

    async def body(_ctx):
        calls.append("start")
        entered.set()
        await finish.wait()

    first = workers(body)
    second = workers(body, first.queue_name)
    job = await first.pool.enqueue_job("control_single_job_start", _queue_name=first.queue_name)
    await asyncio.gather(first.start_jobs([job.job_id.encode()]), second.start_jobs([job.job_id.encode()]))
    await asyncio.wait_for(entered.wait(), 1)
    assert len(first.tasks) + len(second.tasks) == 1
    finish.set()
    await asyncio.gather(*first.tasks.values(), *second.tasks.values())
    assert calls == ["start"]


async def test_invalid_job_uses_owned_failure_cleanup(workers):
    async def body(_ctx):
        pytest.fail("invalid queue entry started work")

    worker = workers(body)
    job = await worker.pool.enqueue_job("control_single_job_start", _queue_name=worker.queue_name)
    await worker.pool.set(job_key_prefix + job.job_id, b"invalid serialized job")
    await worker.start_jobs([job.job_id.encode()])
    await asyncio.wait_for(worker.tasks[job.job_id], 1)
    assert await worker.pool.get(result_key_prefix + job.job_id) is not None
    assert not await worker.pool.exists(in_progress_key_prefix + job.job_id, job_key_prefix + job.job_id)
    assert await worker.pool.zscore(worker.queue_name, job.job_id) is None


async def test_replacement_between_finalization_read_and_commit_is_preserved(workers, monkeypatch):
    entered, finish = asyncio.Event(), asyncio.Event()

    async def body(_ctx):
        entered.set()
        await finish.wait()
        return "old-result"

    worker = workers(body)
    job = await _start(worker)
    await asyncio.wait_for(entered.wait(), 1)
    original_pipeline = worker.pool.pipeline
    replacement_events = []

    def pipeline(*args, **kwargs):
        pipe = original_pipeline(*args, **kwargs)
        execute = pipe.execute

        async def replace_before_commit(*execute_args, **execute_kwargs):
            if worker._claims[job.job_id].finalizing and not replacement_events:
                replacement_events.append("replaced")
                await worker.pool.set(in_progress_key_prefix + job.job_id, b"replacement-owner", px=5000)
                await worker.pool.set(result_key_prefix + job.job_id, b"replacement-result", px=5000)
            return await execute(*execute_args, **execute_kwargs)

        pipe.execute = replace_before_commit
        return pipe

    monkeypatch.setattr(worker.pool, "pipeline", pipeline)
    finish.set()
    await asyncio.wait_for(worker.tasks[job.job_id], 1)
    assert replacement_events == ["replaced"]
    assert await worker.pool.get(in_progress_key_prefix + job.job_id) == b"replacement-owner"
    assert await worker.pool.get(result_key_prefix + job.job_id) == b"replacement-result"
    assert await worker.pool.zscore(worker.queue_name, job.job_id) is not None


@pytest.mark.parametrize("settings", [{"max_jobs": 2}, {"max_tries": 2}, {"job_timeout": 1}])
async def test_catalog_worker_refuses_configuration_that_escapes_its_single_claim(catalog_redis, settings):
    async def body(_ctx):
        pytest.fail("invalid worker settings started work")

    option_by_name = dict(
        functions=[func(body, name="control_single_job_start", max_tries=1)],
        redis_pool=catalog_redis,
        job_timeout=None,
        max_jobs=1,
        max_tries=1,
        handle_signals=False,
    )
    with pytest.raises(ValueError, match="registry_fhir_catalog_queue_settings_invalid"):
        queue.FHIRCatalogWorker(**(option_by_name | settings))


async def test_deferred_or_removed_job_cannot_acquire_a_live_claim(workers):
    async def body(_ctx):
        pytest.fail("ineligible queue entry started work")

    worker = workers(body)
    deferred = await worker.pool.enqueue_job("control_single_job_start", _queue_name=worker.queue_name, _defer_by=60)
    await worker.start_jobs([deferred.job_id.encode(), b"removed-job"])
    assert not worker.tasks and not worker._claims and worker.job_counter == 0
    assert (
        await worker.pool.exists(in_progress_key_prefix + deferred.job_id, in_progress_key_prefix + "removed-job") == 0
    )
    assert await worker.pool.zscore(worker.queue_name, deferred.job_id) is not None


async def test_expired_local_deadline_cannot_renew_the_stored_claim(workers, monkeypatch):
    async def body(_ctx):
        pytest.fail("expired claim started work")

    worker = workers(body)
    job_id = "expired-job"
    key = in_progress_key_prefix + job_id
    claim = queue._Claim(b"original-owner", worker.loop.time() - 1)
    await worker.pool.set(key, claim.token, px=5000)
    evaluate = AsyncMock(wraps=worker.pool.eval)
    monkeypatch.setattr(worker.pool, "eval", evaluate)
    with pytest.raises(RuntimeError, match="registry_fhir_catalog_queue_claim_lost"):
        await worker._renew_claim(job_id, claim)
    evaluate.assert_not_awaited()
    assert await worker.pool.get(key) == claim.token


async def test_lost_initial_claim_never_enters_job_or_erases_the_replacement(workers):
    body = AsyncMock(side_effect=AssertionError("unowned job started"))
    worker = workers(body)
    job = await worker.pool.enqueue_job("control_single_job_start", _queue_name=worker.queue_name)
    key = in_progress_key_prefix + job.job_id
    worker._claims[job.job_id] = queue._Claim(b"original", worker.loop.time() + 5)
    await worker.pool.set(key, b"replacement", px=5000)
    original_job = await worker.pool.get(job_key_prefix + job.job_id)
    score = await worker.pool.zscore(worker.queue_name, job.job_id)
    await worker.run_job(job.job_id, int(score))
    body.assert_not_awaited()
    assert worker.jobs_failed == 1 and not worker._claims
    assert await worker.pool.get(key) == b"replacement"
    assert await worker.pool.get(job_key_prefix + job.job_id) == original_job
    assert await worker.pool.zscore(worker.queue_name, job.job_id) == score
    assert await worker.pool.get(result_key_prefix + job.job_id) is None


async def test_owned_deferred_finalization_preserves_payload_and_finite_claim(workers):
    worker = workers(AsyncMock())
    job = await worker.pool.enqueue_job("control_single_job_start", _queue_name=worker.queue_name)
    key = in_progress_key_prefix + job.job_id
    claim = queue._Claim(b"owner", worker.loop.time() + 5)
    worker._claims[job.job_id] = claim
    await worker.pool.set(key, claim.token, px=5000)
    original_job = await worker.pool.get(job_key_prefix + job.job_id)
    score = await worker.pool.zscore(worker.queue_name, job.job_id)
    await worker.finish_job(job.job_id, False, None, 0, False, 1000, 2)
    assert claim.finalizing
    assert await worker.pool.get(key) == claim.token
    assert 0 < await worker.pool.pttl(key) <= 2000
    assert await worker.pool.zscore(worker.queue_name, job.job_id) == score + 1000
    assert await worker.pool.get(job_key_prefix + job.job_id) == original_job
    assert await worker.pool.get(result_key_prefix + job.job_id) is None


async def test_finalizing_claim_stops_renewal_without_cancelling_its_owner(workers, monkeypatch):
    from unittest.mock import Mock

    worker = workers(AsyncMock())
    claim = queue._Claim(b"owner", worker.loop.time() + 5)
    owner = Mock()
    evaluate = AsyncMock(wraps=worker.pool.eval)
    monkeypatch.setattr(worker.pool, "eval", evaluate)
    task = asyncio.create_task(worker._keep_claim("job", claim, owner))
    try:
        await asyncio.sleep(0)
        claim.finalizing = True
        await asyncio.wait_for(task, 1)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    evaluate.assert_not_awaited()
    owner.cancel.assert_not_called()


async def test_catalog_factory_keeps_the_closed_execution_settings(catalog_redis):
    from types import SimpleNamespace

    settings = SimpleNamespace(
        functions=[func(AsyncMock(), name="control_single_job_start", max_tries=1)],
        job_timeout=None,
        max_jobs=1,
        max_tries=1,
        handle_signals=False,
    )
    worker = queue.create_fhir_catalog_worker(settings, redis_pool=catalog_redis)
    assert type(worker) is queue.FHIRCatalogWorker
    assert worker.pool is catalog_redis and worker.max_jobs == worker.max_tries == 1
    assert worker.job_timeout_s is None and not worker._claims and not worker.tasks
    with pytest.raises(ValueError, match="registry_fhir_catalog_queue_settings_invalid"):
        queue.create_fhir_catalog_worker(settings, redis_pool=catalog_redis, max_jobs=2)


async def test_claim_acquisition_race_releases_capacity_without_overwriting_new_owner(workers, monkeypatch):
    body = AsyncMock(side_effect=AssertionError("contested job started"))
    worker = workers(body)
    job = await worker.pool.enqueue_job("control_single_job_start", _queue_name=worker.queue_name)
    key = in_progress_key_prefix + job.job_id
    original_pipeline = worker.pool.pipeline

    def pipeline(*args, **kwargs):
        pipe = original_pipeline(*args, **kwargs)
        execute = pipe.execute

        async def replace_before_commit(*execute_args, **execute_kwargs):
            await worker.pool.set(key, b"replacement", px=5000)
            return await execute(*execute_args, **execute_kwargs)

        pipe.execute = replace_before_commit
        return pipe

    monkeypatch.setattr(worker.pool, "pipeline", pipeline)
    await worker.start_jobs([job.job_id.encode()])
    body.assert_not_awaited()
    assert not worker.tasks and not worker._claims and worker.job_counter == 0
    assert await worker.pool.get(key) == b"replacement"
    assert await worker.pool.get(job_key_prefix + job.job_id) is not None
    assert await worker.pool.zscore(worker.queue_name, job.job_id) is not None


async def test_running_catalog_job_keeps_the_second_job_queued_without_a_claim(workers):
    entered, finish = asyncio.Event(), asyncio.Event()

    async def body(_ctx):
        entered.set()
        await finish.wait()

    worker = workers(body)
    first = await _start(worker)
    await asyncio.wait_for(entered.wait(), 1)
    second = await worker.pool.enqueue_job("control_single_job_start", _queue_name=worker.queue_name)
    await asyncio.wait_for(worker.start_jobs([second.job_id.encode()]), 1)
    assert set(worker.tasks) == set(worker._claims) == {first.job_id}
    assert worker.job_counter == 1
    assert await worker.pool.get(in_progress_key_prefix + second.job_id) is None
    assert await worker.pool.zscore(worker.queue_name, second.job_id) is not None
    finish.set()
    await asyncio.wait_for(worker.tasks[first.job_id], 1)
    assert not worker._claims and worker.job_counter == 0
