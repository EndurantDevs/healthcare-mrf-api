from __future__ import annotations

from types import SimpleNamespace

import pytest

import main


class WorkerSettings:
    queue_read_limit = 24


@pytest.mark.asyncio
async def test_worker_once_scans_queue_but_starts_one_job(monkeypatch):
    created_worker_context_by_name = {}

    def fake_create_worker(settings, **kwargs):
        worker = SimpleNamespace(**kwargs)
        worker.started = 0

        def jobs_started():
            return worker.started

        async def start_jobs(job_ids):
            worker.job_ids = list(job_ids)
            worker.started += 1

        worker._jobs_started = jobs_started
        worker.start_jobs = start_jobs
        created_worker_context_by_name["settings"] = settings
        created_worker_context_by_name["kwargs"] = kwargs
        return worker

    monkeypatch.setattr(main, "create_worker", fake_create_worker)

    worker = main._create_single_job_worker(WorkerSettings)
    await worker.start_jobs(["already-running", "target-job"])

    assert created_worker_context_by_name["settings"] is WorkerSettings
    assert created_worker_context_by_name["kwargs"]["burst"] is True
    assert created_worker_context_by_name["kwargs"]["max_jobs"] == 1
    assert created_worker_context_by_name["kwargs"]["max_burst_jobs"] == 24
    assert created_worker_context_by_name["kwargs"]["queue_read_limit"] == 24
    assert worker.job_ids == ["already-running", "target-job"]
    assert worker.max_burst_jobs == 1


@pytest.mark.asyncio
async def test_worker_once_target_job_filters_scanned_queue(monkeypatch):
    def fake_create_worker(_settings, **kwargs):
        worker = SimpleNamespace(**kwargs)
        worker.started = 0

        def jobs_started():
            return worker.started

        async def start_jobs(job_ids):
            worker.job_ids = list(job_ids)
            worker.started += 1

        worker._jobs_started = jobs_started
        worker.start_jobs = start_jobs
        return worker

    monkeypatch.setenv("HLTHPRT_WORKER_ONCE_TARGET_JOB_ID", "target-job")
    monkeypatch.setattr(main, "create_worker", fake_create_worker)

    worker = main._create_single_job_worker(WorkerSettings)
    await worker.start_jobs([b"other-job", b"target-job"])

    assert worker.job_ids == [b"target-job"]
    assert worker.max_burst_jobs == 1


@pytest.mark.asyncio
async def test_worker_once_target_job_exits_without_claiming_unrelated_jobs(monkeypatch):
    def fake_create_worker(_settings, **kwargs):
        worker = SimpleNamespace(**kwargs)
        worker.started = 0

        def jobs_started():
            return worker.started

        async def start_jobs(job_ids):
            worker.job_ids = list(job_ids)
            worker.started += 1

        worker._jobs_started = jobs_started
        worker.start_jobs = start_jobs
        return worker

    monkeypatch.setenv("HLTHPRT_WORKER_ONCE_TARGET_JOB_ID", "target-job")
    monkeypatch.setattr(main, "create_worker", fake_create_worker)

    worker = main._create_single_job_worker(WorkerSettings)
    await worker.start_jobs([b"other-job"])

    assert not hasattr(worker, "job_ids")
    assert worker.max_burst_jobs == 0


def test_worker_once_scan_limit_can_be_overridden(monkeypatch):
    monkeypatch.setenv("HLTHPRT_WORKER_ONCE_QUEUE_SCAN_LIMIT", "7")

    assert main._worker_once_scan_limit(WorkerSettings) == 7


def test_worker_once_target_job_uses_large_scan_window(monkeypatch):
    captured_kwargs = {}

    def fake_create_worker(_settings, **kwargs):
        captured_kwargs.update(kwargs)
        worker = SimpleNamespace(**kwargs)
        worker._jobs_started = lambda: 0

        async def start_jobs(_job_ids):
            return None

        worker.start_jobs = start_jobs
        return worker

    monkeypatch.setenv("HLTHPRT_WORKER_ONCE_TARGET_JOB_ID", "target-job")
    monkeypatch.setattr(main, "create_worker", fake_create_worker)

    main._create_single_job_worker(WorkerSettings)

    assert captured_kwargs["queue_read_limit"] == 10000
    assert captured_kwargs["max_burst_jobs"] == 10000
