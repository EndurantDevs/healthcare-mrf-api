# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import datetime
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, Mock

import pytest

from process import initial

@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        (True, True),
        (False, False),
        (" YES ", True),
        ("1", True),
        ("no", False),
        ("0", False),
        ([], False),
        (["value"], True),
    ],
)
def test_optional_boolean_normalization_is_explicit(value, expected):
    assert initial._parse_optional_bool(value) is expected


class _Workbook:
    def __init__(self, worksheet_name, rows):
        self.ws_names = [worksheet_name]
        self._rows = rows

    def ws(self, *, ws):
        assert ws == self.ws_names[0]
        return SimpleNamespace(rows=self._rows)


def _initial_import_workbooks():
    transparency_rows = [
        [],
        [],
        [
            "State",
            "Issuer_Name",
            "Issuer_ID",
            "Is_Issuer_New_to_Exchange? (Yes_or_No)",
            "SADP_Only?",
            "Plan_ID",
            "QHP/SADP",
            "Plan_Type",
            "Metal_Level",
            "URL_Claims_Payment_Policies",
        ],
        [
            "aa",
            "Synthetic Issuer",
            12345,
            "Yes",
            "No",
            "12345AA0000000",
            "QHP",
            "PPO",
            "SILVER",
            "https://data.example.invalid/policies",
        ],
    ]
    index_rows = [
        ["State", "Issuer ID", "Index URL", "Contact"],
        [
            "aa",
            12345,
            json.dumps(
                [
                    "https://data.example.invalid/index-b.json",
                    "https://data.example.invalid/index-a.json",
                ]
            ),
            "data@example.invalid",
        ],
        ["bb", 23456, "https://data.example.invalid/index-c.json", ""],
        ["cc", 34567, "", ""],
    ]
    return {
        "/tmp/transparency.xlsx": _Workbook("Transparency 2026", transparency_rows),
        "/tmp/mrf-index.xlsx": _Workbook("Index", index_rows),
    }


def _install_initial_import_mocks(
    monkeypatch,
    workbook_by_path,
    pushed_batches,
    progress_events,
):
    workbook_paths = iter(workbook_by_path)

    async def push(rows, stage, **_kwargs):
        pushed_batches.append((stage.__name__, [dict(row) for row in rows]))

    monkeypatch.setenv(
        "HLTHPRT_CMSGOV_MRF_URL_PUF",
        json.dumps(["https://data.example.invalid/catalog.zip"]),
    )
    monkeypatch.setenv(
        "HLTHPRT_CMSGOV_PLAN_TRANSPARENCY_URL_PUF",
        json.dumps(
            [
                {"url": "https://data.example.invalid/transparency.zip", "year": 2026},
                {"url": "https://data.example.invalid/ignored.zip", "year": 2025},
            ]
        ),
    )
    monkeypatch.setenv("HLTHPRT_SAVE_PER_PACK", "0")
    for name in (
        "ensure_database",
        "mark_control_run",
        "_prepare_import_tables",
        "download_it_and_save",
        "unzip",
        "_init_mrf_run_state",
    ):
        monkeypatch.setattr(initial, name, AsyncMock())
    monkeypatch.setattr(initial.glob, "glob", lambda _pattern: [next(workbook_paths)])
    monkeypatch.setattr(initial.xl, "readxl", lambda path: workbook_by_path[path])
    monkeypatch.setattr(initial.os, "unlink", lambda _path: None)
    monkeypatch.setattr(initial, "make_class", lambda model, *_args, **_kwargs: model)
    monkeypatch.setattr(initial, "push_objects", push)


def _install_initial_import_catalog_mocks(monkeypatch, progress_events):
    async def has_registered_work(*_args, **_kwargs):
        return True

    monkeypatch.setattr(
        initial,
        "import_unknown_state_issuers_data",
        AsyncMock(
            return_value=(
                {
                    12345: {
                        "issuer_id": 12345,
                        "issuer_name": "",
                        "issuer_marketing_name": "",
                        "data_contact_email": "",
                        "mrf_url": "",
                    }
                },
                {"12345AA0000000_2026": {"plan_id": "12345AA0000000", "year": 2026}},
            )
        ),
    )
    monkeypatch.setattr(
        initial,
        "update_issuer_names_data",
        AsyncMock(
            return_value={
                99999: {
                    "issuer_id": 99999,
                    "issuer_name": "Reference Issuer",
                    "mrf_url": "",
                }
            }
        ),
    )
    monkeypatch.setattr(initial.db, "scalar", AsyncMock(return_value="Catalog Issuer"))
    monkeypatch.setattr(
        initial,
        "_has_registered_mrf_work",
        has_registered_work,
    )
    monkeypatch.setattr(
        initial,
        "enqueue_live_progress",
        lambda **event: progress_events.append(event),
    )


def _assert_initial_import_catalog_result(context_by_field, pushed_batches):
    assert context_by_field["context"] == {
        "import_date": "20260724",
        "run": 1,
        "test_mode": True,
        "control_run_id": "run-contract",
        "mrf_file_chunking": "providers,formularies",
    }
    transparency_rows = [
        pushed_row
        for label, row_dicts in pushed_batches
        for pushed_row in row_dicts
        if label == "PlanTransparency"
    ]
    assert transparency_rows == [
        {
            "state": "AA",
            "issuer_name": "Synthetic Issuer",
            "issuer_id": 12345,
            "new_issuer_to_exchange": True,
            "sadp_only": False,
            "plan_id": "12345AA0000000",
            "year": 2026,
            "qhp_sadp": "QHP",
            "plan_type": "PPO",
            "metal": "SILVER",
            "claims_payment_policies_url": "https://data.example.invalid/policies",
        }
    ]
    issuer_rows = [
        pushed_row
        for label, row_dicts in pushed_batches
        for pushed_row in row_dicts
        if label == "Issuer"
    ]
    assert {issuer_row["issuer_id"] for issuer_row in issuer_rows} == {
        12345,
        23456,
        99999,
    }
    assert next(
        issuer_row for issuer_row in issuer_rows if issuer_row["issuer_id"] == 12345
    ) == {
        "issuer_id": 12345,
        "issuer_name": "Synthetic Issuer",
        "issuer_marketing_name": "",
        "data_contact_email": "data@example.invalid",
        "mrf_url": "https://data.example.invalid/index-b.json",
    }
    assert initial.db.scalar.await_count == 0


def _assert_initial_import_queue_result(progress_events, redis):
    index_calls = [
        call
        for call in redis.enqueue_job.await_args_list
        if call.args[0] == "process_json_index"
    ]
    assert [call.args[1]["url"] for call in index_calls] == [
        "https://data.example.invalid/index-a.json",
        "https://data.example.invalid/index-b.json",
    ]
    assert all(
        call.kwargs["_queue_name"] == initial.MRF_QUEUE_NAME
        for call in index_calls
    )
    shutdown_call = redis.enqueue_job.await_args_list[-1]
    assert shutdown_call.args[0] == "shutdown"
    assert shutdown_call.kwargs == {
        "_job_id": "shutdown_mrf_20260724",
        "_queue_name": initial.MRF_FINISH_QUEUE_NAME,
    }
    assert [event["phase"] for event in progress_events] == [
        "mrf issuer data staged",
        "mrf index jobs enqueued",
        "mrf index jobs enqueued",
    ]


@pytest.mark.asyncio
async def test_initial_import_stages_catalog_rows_and_enqueues_bounded_index_jobs(
    monkeypatch,
):
    """Stage catalog records before enqueuing a bounded, deterministic URL set."""
    workbook_by_path = _initial_import_workbooks()
    pushed_batches = []
    progress_events = []
    _install_initial_import_mocks(
        monkeypatch, workbook_by_path, pushed_batches, progress_events
    )
    _install_initial_import_catalog_mocks(monkeypatch, progress_events)
    redis = SimpleNamespace(enqueue_job=AsyncMock(return_value=SimpleNamespace()))
    context_by_field = {
        "redis": redis,
        "context": {"import_date": "20260724", "run": 0},
    }

    await initial.init_file(
        context_by_field,
        {
            "test_mode": True,
            "run_id": "run-contract",
            "mrf_file_chunking": "providers,formularies",
        },
    )
    _assert_initial_import_catalog_result(context_by_field, pushed_batches)
    _assert_initial_import_queue_result(progress_events, redis)


@pytest.mark.parametrize(
    ("raw_source", "message"),
    [
        ("[not-json", "must be JSON array or single URL"),
        ("[]", "did not provide any usable URLs"),
        ('["", "   "]', "did not provide any usable URLs"),
    ],
)
@pytest.mark.asyncio
async def test_initial_import_rejects_invalid_source_configuration(
    monkeypatch,
    raw_source,
    message,
):
    monkeypatch.setenv("HLTHPRT_CMSGOV_MRF_URL_PUF", raw_source)
    monkeypatch.setattr(initial, "ensure_database", AsyncMock())
    context_by_field = {
        "redis": SimpleNamespace(),
        "context": {"import_date": "20260724", "run": 0},
    }

    with pytest.raises(RuntimeError, match=message):
        await initial.init_file(context_by_field, {"test_mode": True})


@pytest.mark.parametrize(
    ("configured_name", "configured_value", "expected"),
    [
        (None, None, 99),
        ("HLTHPRT_LIMIT_MB", "2", 2 * 1024 * 1024),
        ("HLTHPRT_LIMIT_BYTES", "2048", 2048),
        ("HLTHPRT_LIMIT_BYTES", "3KiB", 3 * 1024),
        ("HLTHPRT_LIMIT_BYTES", "4mb", 4 * 1024 * 1024),
        ("HLTHPRT_LIMIT_BYTES", "1g", 1024 * 1024 * 1024),
        ("HLTHPRT_LIMIT_BYTES", "1t", 1024 * 1024 * 1024 * 1024),
        ("HLTHPRT_LIMIT_BYTES", "invalid", 99),
        ("HLTHPRT_LIMIT_BYTES", "   ", 99),
    ],
)
def test_chunk_size_configuration_accepts_bytes_units_and_legacy_megabytes(
    monkeypatch,
    configured_name,
    configured_value,
    expected,
):
    monkeypatch.delenv("HLTHPRT_LIMIT_BYTES", raising=False)
    monkeypatch.delenv("HLTHPRT_LIMIT_MB", raising=False)
    if configured_name:
        monkeypatch.setenv(configured_name, configured_value)

    assert initial._mrf_size_bytes("HLTHPRT_LIMIT_BYTES", 99) == expected


@pytest.mark.parametrize(
    ("kind", "configured", "expected"),
    [
        ("provider", "", False),
        ("provider", "none", False),
        ("provider", "off", False),
        ("provider", "all", True),
        ("provider", "YES", True),
        ("provider", "plans, providers", True),
        ("formulary", "drugs", True),
        ("plan", "formularies", False),
        ("custom", "custom", True),
    ],
)
def test_chunking_configuration_maps_operator_values_to_import_kinds(
    monkeypatch,
    kind,
    configured,
    expected,
):
    monkeypatch.setenv("HLTHPRT_MRF_FILE_CHUNKING", configured)

    assert initial._is_mrf_file_chunking_enabled(kind) is expected
    assert (
        initial._is_mrf_file_chunking_enabled(
            kind,
            {"context": {"mrf_file_chunking": configured}},
        )
        is expected
    )


@pytest.mark.asyncio
async def test_large_file_chunk_handoff_registers_each_chunk_before_enqueue(
    monkeypatch,
    tmp_path,
):
    source_file = tmp_path / "providers.json"
    source_file.write_text("[]", encoding="utf-8")
    chunks = [
        {"path": str(tmp_path / "provider_00000.json"), "record_count": 2, "byte_count": 20},
        {"path": str(tmp_path / "provider_00001.json"), "record_count": 1, "byte_count": 10},
    ]
    registered = AsyncMock(side_effect=[True, False])
    redis = SimpleNamespace(enqueue_job=AsyncMock(return_value=SimpleNamespace()))
    monkeypatch.setattr(initial, "_is_mrf_file_chunking_enabled", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(initial, "_mrf_size_bytes", lambda name, _default: 1 if name.endswith("MIN_BYTES") else 10)
    monkeypatch.setattr(initial, "_mrf_chunk_dir", lambda *_args, **_kwargs: tmp_path)
    monkeypatch.setattr(initial, "_split_json_array_file_to_chunks", lambda *_args, **_kwargs: chunks)
    monkeypatch.setattr(initial, "_has_registered_mrf_work", registered)
    context_by_field = {
        "redis": redis,
        "context": {"import_date": "20260724", "control_run_id": "run-contract"},
    }
    task_by_field = {
        "url": "https://data.example.invalid/providers.json",
        "issuer_array": [12345],
    }

    handed_off = await initial._has_enqueued_mrf_file_chunks(
        context_by_field,
        task_by_field,
        str(source_file),
        "provider",
        "process_provider",
    )

    assert handed_off is True
    assert registered.await_count == 2
    first_task = registered.await_args_list[0].kwargs["task"]
    assert first_task["source_url"] == task_by_field["url"]
    assert first_task["input_url"].startswith("file://")
    assert first_task["chunk_index"] == 0
    assert first_task["chunk_count"] == 2
    redis.enqueue_job.assert_awaited_once_with(
        "process_provider",
        first_task,
        _queue_name=initial.MRF_QUEUE_NAME,
        _job_id=first_task["work_id"],
    )


@pytest.mark.parametrize(
    ("has_input_url", "chunking_enabled", "file_size", "has_chunks"),
    [
        (True, True, 100, True),
        (False, False, 100, True),
        (False, True, 0, True),
        (False, True, 100, False),
    ],
)
@pytest.mark.asyncio
async def test_chunk_handoff_skips_ineligible_or_unsplittable_files(
    monkeypatch,
    tmp_path,
    has_input_url,
    chunking_enabled,
    file_size,
    has_chunks,
):
    source_file = tmp_path / "plans.json"
    source_file.write_bytes(b"x" * file_size)
    split = Mock(return_value=[{"path": str(tmp_path / "chunk.json")} ] if has_chunks else [])
    monkeypatch.setattr(initial, "_is_mrf_file_chunking_enabled", lambda *_args, **_kwargs: chunking_enabled)
    monkeypatch.setattr(initial, "_mrf_size_bytes", lambda name, _default: 50 if name.endswith("MIN_BYTES") else 10)
    monkeypatch.setattr(initial, "_mrf_chunk_dir", lambda *_args, **_kwargs: tmp_path)
    monkeypatch.setattr(initial, "_split_json_array_file_to_chunks", split)
    task_by_field = {"url": "https://data.example.invalid/plans.json"}
    if has_input_url:
        task_by_field["input_url"] = source_file.as_uri()

    handed_off = await initial._has_enqueued_mrf_file_chunks(
        {"redis": SimpleNamespace(), "context": {"import_date": "20260724"}},
        task_by_field,
        str(source_file),
        "plan",
        "process_plan",
    )

    assert handed_off is False
