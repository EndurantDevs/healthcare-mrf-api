# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio

import pytest

from process import ptg_control
from process.ptg_parts.ptg2_source_witness_contract import (
    WitnessPayloadLimitError,
)


async def _allow_active_run(_run_id):
    return None


@pytest.mark.asyncio
async def test_ptg_control_start_maps_payload_to_ptg_main(monkeypatch):
    calls = []
    marks = []

    async def fake_ptg_main(**kwargs):
        calls.append(kwargs)

    async def has_fake_marked_control_run(*args, **kwargs):
        marks.append((args, kwargs))
        return True

    monkeypatch.setattr(ptg_control, "ptg_main", fake_ptg_main)
    monkeypatch.setattr(
        ptg_control,
        "mark_control_run",
        has_fake_marked_control_run,
    )
    monkeypatch.setattr(ptg_control, "_stale_ptg_job_result", _allow_active_run)

    control_result = await ptg_control.ptg_control_start(
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

    assert control_result == {"status": "succeeded", "run_id": "run_ptg"}
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

    async def has_fake_marked_control_run(*args, **kwargs):
        marks.append((args, kwargs))
        return True

    async def fake_flush_terminal_status_events():
        flushes.append(True)

    monkeypatch.setattr(ptg_control, "ptg_main", fake_ptg_main)
    monkeypatch.setattr(
        ptg_control,
        "mark_control_run",
        has_fake_marked_control_run,
    )
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
async def test_ptg_control_surfaces_nested_source_witness_budget_failure(monkeypatch):
    marks = []

    async def fake_ptg_main(**_kwargs):
        raise ExceptionGroup(
            "publication lanes failed",
            [
                WitnessPayloadLimitError(
                    "strict V3 source witness exceeds its 512 MiB logical "
                    "payload safety bound"
                )
            ],
        )

    async def has_fake_marked_control_run(*args, **kwargs):
        marks.append((args, kwargs))
        return True

    async def fake_flush_terminal_status_events():
        return None

    monkeypatch.setattr(ptg_control, "ptg_main", fake_ptg_main)
    monkeypatch.setattr(
        ptg_control,
        "mark_control_run",
        has_fake_marked_control_run,
    )
    monkeypatch.setattr(
        ptg_control,
        "_flush_terminal_status_events",
        fake_flush_terminal_status_events,
    )
    monkeypatch.setattr(ptg_control, "_stale_ptg_job_result", _allow_active_run)

    with pytest.raises(ExceptionGroup):
        await ptg_control.ptg_control_start(
            {},
            {"run_id": "", "params": {"source_key": "demo_source"}},
        )

    assert marks[-1][1]["error"] == {
        "code": "ptg_source_witness_payload_budget_exceeded",
        "message": (
            "strict V3 source witness exceeds its 512 MiB logical "
            "payload safety bound"
        ),
        "retryable": False,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scanner_message",
    [
        "conflicting provider_group_id definition: 1232062",
        "duplicate provider_group_id definition: 1232062",
    ],
)
async def test_ptg_control_classifies_provider_group_definition_failures(
    monkeypatch, scanner_message
):
    marks = []

    async def fake_ptg_main(**_kwargs):
        raise RuntimeError(
            "PTG2 Rust compact scanner failed with exit code 1: " + scanner_message
        )

    async def has_fake_marked_control_run(*args, **kwargs):
        marks.append((args, kwargs))
        return True

    async def fake_flush_terminal_status_events():
        return None

    monkeypatch.setattr(ptg_control, "ptg_main", fake_ptg_main)
    monkeypatch.setattr(
        ptg_control,
        "mark_control_run",
        has_fake_marked_control_run,
    )
    monkeypatch.setattr(
        ptg_control,
        "_flush_terminal_status_events",
        fake_flush_terminal_status_events,
    )
    monkeypatch.setattr(ptg_control, "_stale_ptg_job_result", _allow_active_run)

    with pytest.raises(RuntimeError, match="provider_group_id definition"):
        await ptg_control.ptg_control_start(
            {},
            {"run_id": "", "params": {"source_key": "demo_source"}},
        )

    assert marks[-1][1]["error"] == {
        "code": "ptg_provider_group_definition_conflict",
        "message": (
            "PTG2 Rust compact scanner failed with exit code 1: "
            + scanner_message
        ),
    }


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

    async def has_fake_marked_control_run(*args, **kwargs):
        marks.append((args, kwargs))
        return True

    monkeypatch.setattr(ptg_control, "ptg_main", fake_ptg_main)
    monkeypatch.setattr(
        ptg_control,
        "mark_control_run",
        has_fake_marked_control_run,
    )
    monkeypatch.setattr(ptg_control, "_stale_ptg_job_result", _allow_active_run)

    control_result = await ptg_control.ptg_control_start(
        {},
        {"run_id": "run_ptg", "params": {"test_mode": True, "source_key": "demo_source"}},
    )

    assert control_result["status"] == "succeeded"
    assert control_result["run_id"] == "run_ptg"
    assert control_result["snapshot_id"] == "ptg2:202606:demo"
    succeeded_mark = marks[-1][1]
    assert succeeded_mark["status"] == "succeeded"
    assert succeeded_mark["snapshot_id"] == "ptg2:202606:demo"
    assert succeeded_mark["metrics"]["source_key"] == "demo_source"
    assert succeeded_mark["metrics"]["source_file_versions"][0]["engine_source_file_version_id"] == "version_1"


@pytest.mark.asyncio
async def test_ptg_control_start_runs_live_progress_heartbeat(monkeypatch):
    heartbeat_calls = []
    stopped_tasks = []

    async def fake_ptg_main(**_kwargs):
        await ptg_control.asyncio.sleep(0)
        return {}

    async def has_fake_marked_control_run(*_args, **_kwargs):
        return True

    async def fake_heartbeat(*args, **kwargs):
        heartbeat_calls.append((args, kwargs))

    async def fake_stop(task):
        stopped_tasks.append(task)

    monkeypatch.setattr(ptg_control, "ptg_main", fake_ptg_main)
    monkeypatch.setattr(
        ptg_control,
        "mark_control_run",
        has_fake_marked_control_run,
    )
    monkeypatch.setattr(ptg_control, "_live_progress_heartbeat", fake_heartbeat)
    monkeypatch.setattr(ptg_control, "_stop_live_progress_heartbeat", fake_stop)
    monkeypatch.setattr(ptg_control, "_stale_ptg_job_result", _allow_active_run)

    await ptg_control.ptg_control_start(
        {},
        {"run_id": "run_ptg", "params": {"test_mode": True, "source_key": "demo_source"}},
    )

    assert heartbeat_calls
    heartbeat_args, heartbeat_kwargs = heartbeat_calls[0]
    assert heartbeat_args[:3] == ("run_ptg", "ptg", "ptg_control_start")
    assert heartbeat_kwargs["attempt_id"].startswith("run_ptg:")
    assert (
        heartbeat_kwargs["attempt_started_at"]
        == heartbeat_args[3]
    )
    assert stopped_tasks


@pytest.mark.asyncio
async def test_ptg_control_start_applies_lane_scanner_env(monkeypatch):
    """Verify ptg control start applies lane scanner env."""
    observed_env_map = {}

    async def fake_ptg_main(**_kwargs):
        observed_env_map["workers"] = ptg_control.os.environ.get("HLTHPRT_PTG2_RUST_WORKERS")
        observed_env_map["rapidgzip_threads"] = ptg_control.os.environ.get(
            "HLTHPRT_PTG2_RUST_RAPIDGZIP_THREADS"
        )
        observed_env_map["parse"] = ptg_control.os.environ.get("HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS")
        observed_env_map["top_level_byte_scan"] = ptg_control.os.environ.get(
            "HLTHPRT_PTG2_RUST_TOP_LEVEL_BYTE_SCAN"
        )
        observed_env_map["work_queue"] = ptg_control.os.environ.get("HLTHPRT_PTG2_RUST_WORK_QUEUE")
        observed_env_map["event_queue"] = ptg_control.os.environ.get("HLTHPRT_PTG2_RUST_EVENT_QUEUE")
        observed_env_map["split_negotiated_rates"] = ptg_control.os.environ.get(
            "HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES"
        )
        observed_env_map["raw_chunk_bytes"] = ptg_control.os.environ.get("HLTHPRT_PTG2_RUST_RAW_CHUNK_BYTES")
        observed_env_map["provider_refs_in_workers"] = ptg_control.os.environ.get(
            "HLTHPRT_PTG2_RUST_PROVIDER_REFS_IN_WORKERS"
        )
        observed_env_map["provider_ref_workers"] = ptg_control.os.environ.get("HLTHPRT_PTG2_RUST_PROVIDER_REF_WORKERS")
        observed_env_map["provider_ref_queue"] = ptg_control.os.environ.get("HLTHPRT_PTG2_RUST_PROVIDER_REF_QUEUE")
        observed_env_map["provider_ref_chunk_items"] = ptg_control.os.environ.get(
            "HLTHPRT_PTG2_RUST_PROVIDER_REF_CHUNK_ITEMS"
        )
        observed_env_map["provider_ref_raw_chunk_bytes"] = ptg_control.os.environ.get(
            "HLTHPRT_PTG2_RUST_PROVIDER_REF_RAW_CHUNK_BYTES"
        )
        observed_env_map["manifest_merge_chunk_bytes"] = ptg_control.os.environ.get(
            "HLTHPRT_PTG2_MANIFEST_MERGE_CHUNK_BYTES"
        )
        observed_env_map["manifest_merge_sort_workers"] = ptg_control.os.environ.get(
            "HLTHPRT_PTG2_MANIFEST_MERGE_SORT_WORKERS"
        )
        observed_env_map["file_process_concurrency"] = ptg_control.os.environ.get(
            "HLTHPRT_PTG2_FILE_PROCESS_CONCURRENCY"
        )
        return {}

    async def has_fake_marked_control_run(*_args, **_kwargs):
        return True

    monkeypatch.setenv("HLTHPRT_ACTIVE_WORKER_QUEUE", "arq:PTGSmall")
    monkeypatch.setenv("HLTHPRT_ACTIVE_WORKER_CLASS", "process.PTGSmall")
    monkeypatch.setattr(ptg_control, "ptg_main", fake_ptg_main)
    monkeypatch.setattr(
        ptg_control,
        "mark_control_run",
        has_fake_marked_control_run,
    )
    monkeypatch.setattr(ptg_control, "_stale_ptg_job_result", _allow_active_run)

    await ptg_control.ptg_control_start(
        {},
        {
            "run_id": "run_ptg",
            "params": {
                "_expected_queue": "arq:PTGSmall",
                "_expected_worker_class": "process.PTGSmall",
                "_scanner_rust_workers": 4,
                "_scanner_rapidgzip_threads": 8,
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

    assert observed_env_map == {
        "workers": "4",
        "rapidgzip_threads": "8",
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
    async def has_fake_marked_control_run(*_args, **_kwargs):
        return True

    monkeypatch.setenv("HLTHPRT_ACTIVE_WORKER_QUEUE", "arq:PTGLarge")
    monkeypatch.setenv("HLTHPRT_ACTIVE_WORKER_CLASS", "process.PTGLarge")
    monkeypatch.setattr(
        ptg_control,
        "mark_control_run",
        has_fake_marked_control_run,
    )
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


def test_ptg_lane_helpers_reject_mismatch_and_restore_environment(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ACTIVE_WORKER_CLASS", "PTGSmall")
    with pytest.raises(RuntimeError, match="expected PTGLarge"):
        ptg_control._assert_expected_lane({"_expected_worker_class": "PTGLarge"})

    environment_name = ptg_control.PTG2_RUST_WORKERS_ENV
    monkeypatch.setenv(environment_name, "original")
    with ptg_control._ptg_lane_environment({"_scanner_rust_workers": "2"}):
        assert ptg_control.os.environ[environment_name] == "2"
    assert ptg_control.os.environ[environment_name] == "original"


def test_ptg_string_list_normalizes_text_and_rejects_other_values():
    assert ptg_control._string_list(" value ") == ["value"]
    assert ptg_control._string_list({"value"}) is None
