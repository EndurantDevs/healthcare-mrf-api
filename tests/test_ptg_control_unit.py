# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import pytest

from process import ptg_control


@pytest.mark.asyncio
async def test_ptg_control_start_maps_payload_to_ptg_main(monkeypatch):
    calls = []
    marks = []

    async def fake_ptg_main(**kwargs):
        calls.append(kwargs)

    async def fake_mark_control_run(*args, **kwargs):
        marks.append((args, kwargs))

    monkeypatch.setattr(ptg_control, "ptg_main", fake_ptg_main)
    monkeypatch.setattr(ptg_control, "mark_control_run", fake_mark_control_run)

    result = await ptg_control.ptg_control_start(
        {},
        {
            "run_id": "run_ptg",
            "params": {
                "test_mode": True,
                "in_network_url": "https://example.com/rates.json.gz",
                "source_key": "asr_1208",
                "plan_ids": ["823166837"],
                "plan_market_types": ["group"],
                "max_files": "1",
            },
        },
    )

    assert result == {"status": "succeeded", "run_id": "run_ptg"}
    assert calls[0]["test_mode"] is True
    assert calls[0]["in_network_url"] == "https://example.com/rates.json.gz"
    assert calls[0]["source_key"] == "asr_1208"
    assert calls[0]["plan_ids"] == ["823166837"]
    assert calls[0]["plan_market_types"] == ["group"]
    assert calls[0]["max_files"] == 1
    assert calls[0]["control_run_id"] == "run_ptg"
    assert [kwargs["status"] for _args, kwargs in marks] == ["running", "succeeded"]


@pytest.mark.asyncio
async def test_ptg_control_start_records_ptg2_terminal_identity(monkeypatch):
    marks = []

    async def fake_ptg_main(**_kwargs):
        return {
            "import_run_id": "ptg2:demo",
            "snapshot_id": "ptg2:202606:demo",
            "source_key": "demo_source",
            "files_processed": 1,
            "source_file_versions": [
                {
                    "canonical_url": "https://example.com/rates.json.gz",
                    "engine_source_identity_hash": "source_hash_1",
                    "engine_source_file_version_id": "version_1",
                }
            ],
        }

    async def fake_mark_control_run(*args, **kwargs):
        marks.append((args, kwargs))

    monkeypatch.setattr(ptg_control, "ptg_main", fake_ptg_main)
    monkeypatch.setattr(ptg_control, "mark_control_run", fake_mark_control_run)

    result = await ptg_control.ptg_control_start(
        {},
        {"run_id": "run_ptg", "params": {"test_mode": True, "source_key": "demo_source"}},
    )

    assert result["status"] == "succeeded"
    assert result["run_id"] == "run_ptg"
    assert result["snapshot_id"] == "ptg2:202606:demo"
    succeeded_mark = marks[-1][1]
    assert succeeded_mark["status"] == "succeeded"
    assert succeeded_mark["snapshot_id"] == "ptg2:202606:demo"
    assert succeeded_mark["metrics"]["source_key"] == "demo_source"
    assert succeeded_mark["metrics"]["source_file_versions"][0]["engine_source_file_version_id"] == "version_1"


@pytest.mark.asyncio
async def test_ptg_control_start_runs_live_progress_heartbeat(monkeypatch):
    heartbeat_calls = []
    stopped = []

    async def fake_ptg_main(**_kwargs):
        await ptg_control.asyncio.sleep(0)
        return {}

    async def fake_mark_control_run(*_args, **_kwargs):
        return None

    async def fake_heartbeat(*args):
        heartbeat_calls.append(args)

    async def fake_stop(task):
        stopped.append(task)

    monkeypatch.setattr(ptg_control, "ptg_main", fake_ptg_main)
    monkeypatch.setattr(ptg_control, "mark_control_run", fake_mark_control_run)
    monkeypatch.setattr(ptg_control, "_live_progress_heartbeat", fake_heartbeat)
    monkeypatch.setattr(ptg_control, "_stop_live_progress_heartbeat", fake_stop)

    await ptg_control.ptg_control_start(
        {},
        {"run_id": "run_ptg", "params": {"test_mode": True, "source_key": "demo_source"}},
    )

    assert heartbeat_calls
    assert heartbeat_calls[0][:3] == ("run_ptg", "ptg", "ptg_control_start")
    assert stopped


@pytest.mark.asyncio
async def test_ptg_control_start_applies_lane_scanner_env(monkeypatch):
    observed = {}

    async def fake_ptg_main(**_kwargs):
        observed["workers"] = ptg_control.os.environ.get("HLTHPRT_PTG2_RUST_WORKERS")
        observed["parse"] = ptg_control.os.environ.get("HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS")
        observed["work_queue"] = ptg_control.os.environ.get("HLTHPRT_PTG2_RUST_WORK_QUEUE")
        observed["event_queue"] = ptg_control.os.environ.get("HLTHPRT_PTG2_RUST_EVENT_QUEUE")
        return {}

    async def fake_mark_control_run(*_args, **_kwargs):
        return None

    monkeypatch.setenv("HLTHPRT_ACTIVE_WORKER_QUEUE", "arq:PTGSmall")
    monkeypatch.setenv("HLTHPRT_ACTIVE_WORKER_CLASS", "process.PTGSmall")
    monkeypatch.setattr(ptg_control, "ptg_main", fake_ptg_main)
    monkeypatch.setattr(ptg_control, "mark_control_run", fake_mark_control_run)

    await ptg_control.ptg_control_start(
        {},
        {
            "run_id": "run_ptg",
            "params": {
                "_expected_queue": "arq:PTGSmall",
                "_expected_worker_class": "process.PTGSmall",
                "_scanner_rust_workers": 4,
                "_scanner_parse_in_workers": True,
                "_scanner_work_queue": 5,
                "_scanner_event_queue": 9,
            },
        },
    )

    assert observed == {"workers": "4", "parse": "true", "work_queue": "5", "event_queue": "9"}


@pytest.mark.asyncio
async def test_ptg_control_start_rejects_wrong_lane(monkeypatch):
    async def fake_mark_control_run(*_args, **_kwargs):
        return None

    monkeypatch.setenv("HLTHPRT_ACTIVE_WORKER_QUEUE", "arq:PTGLarge")
    monkeypatch.setenv("HLTHPRT_ACTIVE_WORKER_CLASS", "process.PTGLarge")
    monkeypatch.setattr(ptg_control, "mark_control_run", fake_mark_control_run)

    with pytest.raises(RuntimeError, match="expected arq:PTGSmall"):
        await ptg_control.ptg_control_start(
            {},
            {
                "run_id": "run_ptg",
                "params": {
                    "_expected_queue": "arq:PTGSmall",
                    "_expected_worker_class": "process.PTGSmall",
                },
            },
        )
