# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The normal control API preserves complete ordinary rate selectors."""

import importlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sanic.exceptions import BadRequest

from api import control, control_imports


def test_complete_rate_set_is_a_registered_repeatable_parameter():
    importers_by_name = {importer["name"]: importer for importer in control_imports.importer_registry()}
    parameter_by_name = next(
        parameter for parameter in importers_by_name["ptg"]["params_schema"] if parameter["name"] == "in_network_urls"
    )
    assert parameter_by_name["opts"] == ["--in-network-urls"]
    assert parameter_by_name["multiple"] is True
    assert parameter_by_name["type"] == "text"


@pytest.mark.asyncio
async def test_normal_control_route_preserves_complete_rate_set_to_queue(monkeypatch):
    urls = [f"https://example.test/rates_{part}_of_2.json.gz" for part in (1, 2)]
    params_by_name = {
        "in_network_urls": urls,
        "max_files": 2,
        "source_file_import_id": "attempt_neutral",
        "import_id": "attempt_neutral",
        "source_key": "source_neutral",
        "import_month": "2026-10",
        "plan_ids": ["plan_neutral"],
        "plan_market_types": ["group"],
    }
    admit = AsyncMock(return_value=None)
    enqueue = AsyncMock(return_value=SimpleNamespace(job_id="job_neutral"))
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "test-token")
    monkeypatch.setattr(control_imports, "_admit_import_row", admit)
    monkeypatch.setattr(control_imports, "db", SimpleNamespace(execute=AsyncMock()))
    monkeypatch.setattr(control_imports, "create_pool", AsyncMock(return_value=SimpleNamespace(enqueue_job=enqueue)))
    monkeypatch.setattr(control_imports, "enqueue_status_event", Mock())
    monkeypatch.setattr(control_imports, "_write_run_live_progress", Mock())
    monkeypatch.setattr(control_imports, "read_live_progress", Mock(return_value=None))
    request = SimpleNamespace(
        headers={"Authorization": "Bearer test-token"},
        json={
            "run_id": "run_neutral",
            "importer": "ptg",
            "params": params_by_name,
            "source_file_import_id": "attempt_neutral",
            "import_id": "attempt_neutral",
        },
    )

    api_response = await control.control_create_import(request)

    assert api_response.status == 201
    assert json.loads(api_response.body)["params"] == params_by_name
    assert admit.await_args.args[1]["params"] == params_by_name
    assert admit.await_args.kwargs["is_ptg_source_file_admission"] is True
    assert enqueue.await_args.args[0] == "ptg_control_start"
    assert enqueue.await_args.args[1] == {
        "run_id": "run_neutral",
        "source_file_import_id": "attempt_neutral",
        "import_id": "attempt_neutral",
        "params": params_by_name,
    }


@pytest.mark.asyncio
async def test_normal_control_route_rejects_mixed_rate_selectors_before_admission(monkeypatch):
    urls = [f"https://example.test/rates_{part}_of_2.json.gz" for part in (1, 2)]
    admit, pool = AsyncMock(), AsyncMock()
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "test-token")
    monkeypatch.setattr(control_imports, "_admit_import_row", admit)
    monkeypatch.setattr(control_imports, "create_pool", pool)
    request = SimpleNamespace(
        headers={"Authorization": "Bearer test-token"},
        json={
            "importer": "ptg",
            "params": {
                "in_network_urls": urls,
                "max_files": 2,
                "toc_urls": ["https://example.test/index.json"],
            },
        },
    )

    with pytest.raises(BadRequest, match="cannot mix"):
        await control.control_create_import(request)

    admit.assert_not_awaited()
    pool.assert_not_awaited()


@pytest.mark.asyncio
async def test_complete_rate_set_reaches_worker_main_without_scalar_selector(monkeypatch):
    worker = importlib.import_module("process.ptg_control")
    frozen = importlib.import_module("process.ptg_frozen_control")
    urls = [f"https://example.test/rates_{part}_of_2.json.gz" for part in (1, 2)]
    main = AsyncMock(return_value={})
    monkeypatch.setattr(worker, "ptg_main", main)
    monkeypatch.setattr(worker, "guard_ptg_worker_start", AsyncMock(return_value=None))
    monkeypatch.setattr(worker, "mark_control_run", AsyncMock(return_value=True))
    monkeypatch.setattr(worker, "_flush_terminal_status_events", AsyncMock())
    monkeypatch.setattr(frozen, "recheck_frozen_binding", AsyncMock(return_value=None))
    worker_report_by_name = await worker.ptg_control_start({}, {"params": {"in_network_urls": urls, "max_files": 2}})

    assert worker_report_by_name["status"] == "succeeded"
    assert main.await_args.kwargs["in_network_urls"] == urls
    assert main.await_args.kwargs["in_network_url"] is None
    assert main.await_args.kwargs["toc_urls"] is None
    assert main.await_args.kwargs["max_files"] == 2
