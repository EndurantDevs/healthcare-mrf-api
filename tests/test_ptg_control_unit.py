# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio

import pytest

from process import ptg_control


async def _allow_active_run(_run_id):
    return None


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
    monkeypatch.setattr(ptg_control, "_stale_ptg_job_result", _allow_active_run)

    result = await ptg_control.ptg_control_start(
        {},
        {
            "run_id": "run_ptg",
            "params": {
                "test_mode": True,
                "in_network_url": "https://example.com/rates.json.gz",
                "source_key": "example_source_a",
                "plan_ids": ["TESTPLAN001"],
                "plan_market_types": ["group"],
                "max_files": "1",
            },
        },
    )

    assert result == {"status": "succeeded", "run_id": "run_ptg"}
    assert calls[0]["test_mode"] is True
    assert calls[0]["in_network_url"] == "https://example.com/rates.json.gz"
    assert calls[0]["source_key"] == "example_source_a"
    assert calls[0]["plan_ids"] == ["TESTPLAN001"]
    assert calls[0]["plan_market_types"] == ["group"]
    assert calls[0]["max_files"] == 1
    assert calls[0]["control_run_id"] == "run_ptg"
    assert [kwargs["status"] for _args, kwargs in marks] == ["running", "succeeded"]


@pytest.mark.asyncio
async def test_ptg_control_start_marks_failed_and_reraises_cancelled_ptg_main(monkeypatch):
    marks = []
    flushes = []

    async def fake_ptg_main(**_kwargs):
        raise asyncio.CancelledError()

    async def fake_mark_control_run(*args, **kwargs):
        marks.append((args, kwargs))

    async def fake_flush_terminal_status_events():
        flushes.append(True)

    monkeypatch.setattr(ptg_control, "ptg_main", fake_ptg_main)
    monkeypatch.setattr(ptg_control, "mark_control_run", fake_mark_control_run)
    monkeypatch.setattr(ptg_control, "_flush_terminal_status_events", fake_flush_terminal_status_events)
    monkeypatch.setattr(ptg_control, "_stale_ptg_job_result", _allow_active_run)

    with pytest.raises(asyncio.CancelledError):
        await ptg_control.ptg_control_start(
            {},
            {"run_id": "run_ptg", "params": {"test_mode": True, "source_key": "demo_source"}},
        )

    assert [kwargs["status"] for _args, kwargs in marks] == ["running", "failed"]
    failed_mark = marks[-1][1]
    assert failed_mark["phase_detail"] == "ptg import interrupted"
    assert failed_mark["progress_message"] == "interrupted"
    assert failed_mark["error"] == {
        "code": "import_interrupted",
        "message": "worker task was cancelled",
    }
    assert flushes == [True]


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
    monkeypatch.setattr(ptg_control, "_stale_ptg_job_result", _allow_active_run)

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
    monkeypatch.setattr(ptg_control, "_stale_ptg_job_result", _allow_active_run)

    await ptg_control.ptg_control_start(
        {},
        {"run_id": "run_ptg", "params": {"test_mode": True, "source_key": "demo_source"}},
    )

    assert heartbeat_calls
    assert heartbeat_calls[0][:3] == ("run_ptg", "ptg", "ptg_control_start")
    assert stopped


def test_ptg_thread_heartbeat_preserves_importer_progress_source(monkeypatch):
    events = []

    class FakeEvent:
        def __init__(self):
            self.calls = 0
            self.set_called = False

        def wait(self, _interval):
            self.calls += 1
            return self.calls > 1

        def set(self):
            self.set_called = True

    class FakeThread:
        def __init__(self, target, **_kwargs):
            self.target = target

        def start(self):
            self.target()

    monkeypatch.setattr(ptg_control.threading, "Event", FakeEvent)
    monkeypatch.setattr(ptg_control.threading, "Thread", FakeThread)
    monkeypatch.setattr(ptg_control, "write_live_progress", lambda **payload: events.append(payload))

    stop_event = ptg_control._start_threaded_ptg_heartbeat("run_ptg", "2026-07-03T12:00:00+00:00")
    ptg_control._stop_threaded_ptg_heartbeat(stop_event)

    assert events
    assert events[0]["source"] == ptg_control.PTG_CONTROL_HEARTBEAT_SOURCE
    assert events[0]["confidence"] == "heartbeat"
    assert events[0]["publish_event"] is False
    assert stop_event.set_called


@pytest.mark.asyncio
async def test_ptg_control_start_skips_stale_terminal_run(monkeypatch):
    async def fake_ptg_main(**_kwargs):
        raise AssertionError("stale PTG job should not start scanner")

    async def fake_mark_control_run(*_args, **_kwargs):
        raise AssertionError("stale PTG job should not mark run running")

    async def fake_db_first(*_args, **_kwargs):
        return ("canceled",)

    monkeypatch.setattr(ptg_control, "ptg_main", fake_ptg_main)
    monkeypatch.setattr(ptg_control, "mark_control_run", fake_mark_control_run)
    monkeypatch.setattr(ptg_control.db, "first", fake_db_first)

    result = await ptg_control.ptg_control_start(
        {},
        {
            "run_id": "run_old",
            "source_file_import_id": "source_import_1",
            "params": {"test_mode": True, "source_key": "demo_source"},
        },
    )

    assert result == {
        "status": "skipped",
        "run_id": "run_old",
        "reason": "run_canceled",
    }


@pytest.mark.asyncio
async def test_ptg_stale_job_check_allows_nonterminal_local_run(monkeypatch):
    async def fake_db_first(*_args, **_kwargs):
        return ("queued",)

    monkeypatch.setattr(ptg_control.db, "first", fake_db_first)

    assert await ptg_control._stale_ptg_job_result("run_old") is None


@pytest.mark.asyncio
async def test_ptg_control_start_applies_lane_scanner_env(monkeypatch):
    """Verify ptg control start applies lane scanner env."""
    observed = {}

    async def fake_ptg_main(**_kwargs):
        observed["workers"] = ptg_control.os.environ.get("HLTHPRT_PTG2_RUST_WORKERS")
        observed["parse"] = ptg_control.os.environ.get("HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS")
        observed["top_level_byte_scan"] = ptg_control.os.environ.get(
            "HLTHPRT_PTG2_RUST_TOP_LEVEL_BYTE_SCAN"
        )
        observed["work_queue"] = ptg_control.os.environ.get("HLTHPRT_PTG2_RUST_WORK_QUEUE")
        observed["event_queue"] = ptg_control.os.environ.get("HLTHPRT_PTG2_RUST_EVENT_QUEUE")
        observed["split_negotiated_rates"] = ptg_control.os.environ.get(
            "HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES"
        )
        observed["raw_chunk_bytes"] = ptg_control.os.environ.get("HLTHPRT_PTG2_RUST_RAW_CHUNK_BYTES")
        observed["provider_refs_in_workers"] = ptg_control.os.environ.get(
            "HLTHPRT_PTG2_RUST_PROVIDER_REFS_IN_WORKERS"
        )
        observed["provider_ref_workers"] = ptg_control.os.environ.get("HLTHPRT_PTG2_RUST_PROVIDER_REF_WORKERS")
        observed["provider_ref_queue"] = ptg_control.os.environ.get("HLTHPRT_PTG2_RUST_PROVIDER_REF_QUEUE")
        observed["provider_ref_chunk_items"] = ptg_control.os.environ.get(
            "HLTHPRT_PTG2_RUST_PROVIDER_REF_CHUNK_ITEMS"
        )
        observed["provider_ref_raw_chunk_bytes"] = ptg_control.os.environ.get(
            "HLTHPRT_PTG2_RUST_PROVIDER_REF_RAW_CHUNK_BYTES"
        )
        observed["manifest_merge_chunk_bytes"] = ptg_control.os.environ.get(
            "HLTHPRT_PTG2_MANIFEST_MERGE_CHUNK_BYTES"
        )
        observed["manifest_merge_sort_workers"] = ptg_control.os.environ.get(
            "HLTHPRT_PTG2_MANIFEST_MERGE_SORT_WORKERS"
        )
        observed["file_process_concurrency"] = ptg_control.os.environ.get("HLTHPRT_PTG2_FILE_PROCESS_CONCURRENCY")
        return {}

    async def fake_mark_control_run(*_args, **_kwargs):
        return None

    monkeypatch.setenv("HLTHPRT_ACTIVE_WORKER_QUEUE", "arq:PTGSmall")
    monkeypatch.setenv("HLTHPRT_ACTIVE_WORKER_CLASS", "process.PTGSmall")
    monkeypatch.setattr(ptg_control, "ptg_main", fake_ptg_main)
    monkeypatch.setattr(ptg_control, "mark_control_run", fake_mark_control_run)
    monkeypatch.setattr(ptg_control, "_stale_ptg_job_result", _allow_active_run)

    await ptg_control.ptg_control_start(
        {},
        {
            "run_id": "run_ptg",
            "params": {
                "_expected_queue": "arq:PTGSmall",
                "_expected_worker_class": "process.PTGSmall",
                "_scanner_rust_workers": 4,
                "_scanner_parse_in_workers": True,
                "_scanner_top_level_byte_scan": True,
                "_scanner_work_queue": 5,
                "_scanner_event_queue": 9,
                "_scanner_split_negotiated_rates": 8192,
                "_scanner_raw_chunk_bytes": 33554432,
                "_scanner_provider_refs_in_workers": False,
                "_scanner_provider_ref_workers": 3,
                "_scanner_provider_ref_queue": 4,
                "_scanner_provider_ref_chunk_items": 512,
                "_scanner_provider_ref_raw_chunk_bytes": 524288,
                "_manifest_merge_chunk_bytes": 268435456,
                "_manifest_merge_sort_workers": 4,
                "_file_process_concurrency": 2,
            },
        },
    )

    assert observed == {
        "workers": "4",
        "parse": "true",
        "top_level_byte_scan": "true",
        "work_queue": "5",
        "event_queue": "9",
        "split_negotiated_rates": "8192",
        "raw_chunk_bytes": "33554432",
        "provider_refs_in_workers": "false",
        "provider_ref_workers": "3",
        "provider_ref_queue": "4",
        "provider_ref_chunk_items": "512",
        "provider_ref_raw_chunk_bytes": "524288",
        "manifest_merge_chunk_bytes": "268435456",
        "manifest_merge_sort_workers": "4",
        "file_process_concurrency": "2",
    }


@pytest.mark.asyncio
async def test_ptg_control_start_rejects_wrong_lane(monkeypatch):
    async def fake_mark_control_run(*_args, **_kwargs):
        return None

    monkeypatch.setenv("HLTHPRT_ACTIVE_WORKER_QUEUE", "arq:PTGLarge")
    monkeypatch.setenv("HLTHPRT_ACTIVE_WORKER_CLASS", "process.PTGLarge")
    monkeypatch.setattr(ptg_control, "mark_control_run", fake_mark_control_run)
    monkeypatch.setattr(ptg_control, "_stale_ptg_job_result", _allow_active_run)

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
