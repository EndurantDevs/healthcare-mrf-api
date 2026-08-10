# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime as dt
import json
import threading
from contextlib import suppress
from types import SimpleNamespace
from unittest.mock import mock_open

import pytest

import main
from process import import_status_events as status_events
from process import live_progress
from tests.live_progress_atomic_redis import AtomicLiveProgressRedis

@pytest.fixture(autouse=True)
def _reset_status_publisher():
    status_events._publisher_state.queue = None
    status_events._publisher_state.worker = None
    status_events._publisher_state.loop = None
    status_events._publisher_state.pending.clear()
    status_events._publisher_state.coalesced_by_run.clear()
    status_events._publisher_state.flush_handle_by_run.clear()
    status_events._last_sent_by_run.clear()
    yield
    worker = status_events._publisher_state.worker
    if isinstance(worker, asyncio.Task):
        worker.cancel()
    status_events._publisher_state.queue = None
    status_events._publisher_state.worker = None
    status_events._publisher_state.loop = None
    status_events._publisher_state.pending.clear()
    status_events._publisher_state.coalesced_by_run.clear()
    status_events._publisher_state.flush_handle_by_run.clear()
    status_events._last_sent_by_run.clear()


@pytest.mark.parametrize(
    ("command", "targets"),
    [
        (main.stop_mrf.callback, ("process.MRF", "process.MRF_finish")),
        (
            main.stop_claims_pricing.callback,
            ("process.ClaimsPricing", "process.ClaimsPricing_finish"),
        ),
        (
            main.stop_claims_procedures.callback,
            ("process.ClaimsProcedures", "process.ClaimsProcedures_finish"),
        ),
        (
            main.stop_drug_claims.callback,
            ("process.DrugClaims", "process.DrugClaims_finish"),
        ),
        (
            main.stop_provider_enrichment.callback,
            ("process.ProviderEnrichment", "process.ProviderEnrichment_finish"),
        ),
        (
            main.stop_partd_formulary_network.callback,
            ("process.PartDFormularyNetwork", "process.PartDFormularyNetwork_finish"),
        ),
        (
            main.stop_pharmacy_license.callback,
            ("process.PharmacyLicense", "process.PharmacyLicense_finish"),
        ),
    ],
)
def test_stop_commands_control_import_override(monkeypatch, command, targets):
    calls: list[tuple[str, bool, dict[str, str]]] = []
    monkeypatch.setattr(
        main,
        "_run_worker_command",
        lambda target, burst, env: calls.append((target, burst, env)),
    )
    monkeypatch.setenv("HLTHPRT_IMPORT_ID_OVERRIDE", "stale")

    command(burst=False, import_id="run-22")
    command(burst=True, import_id=None)

    assert [call[0] for call in calls] == [
        targets[0],
        targets[1],
        targets[0],
        targets[1],
    ]
    assert calls[0][2]["HLTHPRT_IMPORT_ID_OVERRIDE"] == "run-22"
    assert "HLTHPRT_IMPORT_ID_OVERRIDE" not in calls[2][2]
    assert calls[0][1] is False
    assert calls[1][1] is True


def test_main_runtime_helpers_cover_error_and_command_paths(monkeypatch):
    monkeypatch.setenv("HLTHPRT_API_WORKERS", "invalid")
    monkeypatch.delenv("HLTHPRT_DB_ECHO", raising=False)
    monkeypatch.setattr(
        main,
        "connection",
        SimpleNamespace(
            _detect_server_capabilities=main.connection._detect_server_capabilities
        ),
    )
    assert main._default_api_workers() == 1
    assert main._job_id_text(b"job-1") == "job-1"
    assert main._job_id_text(22) == "22"

    run_calls: list[tuple[list[str], dict[str, str]]] = []
    monkeypatch.setattr(
        main.subprocess,
        "run",
        lambda command, **kwargs: run_calls.append((command, kwargs)),
    )
    main._run_worker_command("process.Target", True, {"TOKEN": "one"})
    main._run_worker_command("process.Target", False, {"TOKEN": "two"})
    assert run_calls[0][0][-1] == "--burst"
    assert run_calls[1][0][-1] == "process.Target"

    monkeypatch.setattr("builtins.open", mock_open(read_data="{}"))
    monkeypatch.setattr(main.yaml, "safe_load", lambda _stream: {})
    monkeypatch.setattr(main.logging.config, "dictConfig", lambda _config: None)
    api_runs: list[dict[str, object]] = []
    monkeypatch.setattr(
        type(main.api), "run", lambda _api, **kwargs: api_runs.append(kwargs)
    )
    main.start.callback("127.0.0.1", 8081, 2, False, False)
    main.start.callback("127.0.0.1", 8082, 3, True, True)
    assert api_runs[1]["auto_reload"] is True
    assert main.os.environ["HLTHPRT_DB_ECHO"] == "True"


def test_main_cli_repairs_missing_or_non_uvloop_event_loop(monkeypatch):
    installed_loops: list[object] = []
    replacement = object()
    monkeypatch.setattr(main, "_new_event_loop", lambda: replacement)
    monkeypatch.setattr(main.asyncio, "set_event_loop", installed_loops.append)

    monkeypatch.setattr(
        main.asyncio, "get_event_loop", lambda: (_ for _ in ()).throw(RuntimeError())
    )
    main.cli.callback()

    current = SimpleNamespace(is_closed=lambda: False)
    monkeypatch.setattr(main.asyncio, "get_event_loop", lambda: current)
    main.cli.callback()

    closed = SimpleNamespace(is_closed=lambda: True)
    monkeypatch.setattr(main.asyncio, "get_event_loop", lambda: closed)
    main.cli.callback()
    assert installed_loops == [replacement, replacement, replacement]
