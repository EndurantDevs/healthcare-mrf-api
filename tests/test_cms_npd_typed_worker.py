# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Worker entry points, CLI command and control-plane registration of the typed CMS NPD importer."""

from click.testing import CliRunner

import process
from api import control_imports, control_workers
from process import cms_npd_typed_worker as worker


async def test_startup_connects_before_taking_jobs(monkeypatch):
    calls = []

    async def fake_init(database):
        calls.append(("init", database))

    async def fake_ensure(flag):
        calls.append(("ensure", flag))

    monkeypatch.setattr(worker, "my_init_db", fake_init)
    monkeypatch.setattr(worker, "ensure_database", fake_ensure)
    context_by_name = {}
    await worker.startup(context_by_name)
    assert calls == [("init", worker.db), ("ensure", False)] and context_by_name == {"context": {}}
    assert await worker.shutdown(context_by_name) is None


async def test_process_data_passes_the_control_run_id(monkeypatch):
    calls = []

    async def fake_run(ctx, task, run_id):
        calls.append((ctx, task, run_id))
        return {"status": "unchanged"}

    monkeypatch.setattr(worker, "run", fake_run)
    assert await worker.process_data({"c": 1}, {"run_id": "run_1"}) == {"status": "unchanged"}
    await worker.process_data({}, None)
    assert calls == [({"c": 1}, {"run_id": "run_1"}, "run_1"), ({}, {}, "")]


async def test_main_queues_one_job_on_the_typed_queue(monkeypatch):
    enqueued_jobs = []

    class FakeRedis:
        async def enqueue_job(self, function, *args, **kwargs):
            enqueued_jobs.append((function, args, kwargs))

    async def fake_create_pool(settings, **kwargs):
        assert settings == "redis-settings" and kwargs["job_serializer"] is worker.serialize_job
        return FakeRedis()

    monkeypatch.setattr(worker, "build_redis_settings", lambda: "redis-settings")
    monkeypatch.setattr(worker, "create_pool", fake_create_pool)
    await worker.main()
    assert enqueued_jobs == [("process_data", ({},), {"_queue_name": "arq:CMSNPD"})]


def test_worker_class_runs_one_long_job_at_a_time():
    assert process.CMSNPD.functions == [worker.process_data, process.control_single_job_start]
    assert (process.CMSNPD.on_startup, process.CMSNPD.on_shutdown) == (worker.startup, worker.shutdown)
    assert (process.CMSNPD.max_jobs, process.CMSNPD.queue_read_limit, process.CMSNPD.job_timeout) == (1, 1, 21600)


def test_cli_command_queues_the_import(monkeypatch):
    ran_calls = []
    monkeypatch.setattr(process, "initiate_cms_npd", lambda: "queued-coroutine")
    monkeypatch.setattr(process, "_run", ran_calls.append)
    outcome = CliRunner().invoke(process.process_group, ["cms-npd"])
    assert outcome.exit_code == 0, outcome.output
    assert ran_calls == ["queued-coroutine"]


def test_control_plane_routes_cms_npd_to_the_typed_worker():
    adapter = control_imports._SINGLE_JOB_ADAPTERS["cms-npd"]
    matching_specs = [spec for spec in control_workers._START_WORKERS if "cms-npd" in spec.importers]
    assert len(matching_specs) == 1
    spec = matching_specs[0]
    assert adapter["queue"] == spec.queue == process.CMSNPD.queue_name == worker.QUEUE_NAME
    assert spec.worker_class == "process.CMSNPD" and spec.importers == ("cms-npd",)
    assert control_imports._importer_family("cms-npd") == control_imports._importer_family("cms-doctors")
    payload_by_field = control_imports._adapter_payload(adapter, {"run_id": "run_x", "importer": "cms-npd"}, {})
    assert payload_by_field["target_module"] == "process.cms_npd_typed_worker"
    assert payload_by_field["target_function"] == "process_data" and payload_by_field["call_style"] == "ctx_task"
