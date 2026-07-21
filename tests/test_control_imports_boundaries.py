# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Boundary coverage for control-plane serialization and health helpers."""

from __future__ import annotations

import base64
import datetime as dt
import importlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import click
import pytest

control_imports = importlib.import_module("api.control_imports")


def test_parameter_schema_skips_arguments_and_serializes_container_defaults():
    command = click.Command(
        "coverage",
        params=[
            click.Argument(["positional"]),
            click.Option(
                ["--mode"],
                type=click.Choice(["one", "two"]),
                default="one",
            ),
        ],
    )

    assert control_imports._param_schema(command) == [
        {
            "name": "mode",
            "opts": ["--mode"],
            "required": False,
            "multiple": False,
            "is_flag": False,
            "type": "choice",
            "default": "one",
            "help": None,
            "choices": ["one", "two"],
        }
    ]
    assert control_imports._json_safe_default((1, {"two": [3]})) == [
        1,
        {"two": [3]},
    ]
    assert control_imports._json_safe_default(object()) is None


@pytest.mark.asyncio
async def test_health_helpers_fail_independently(monkeypatch, tmp_path):
    monkeypatch.setattr(
        control_imports.shutil,
        "disk_usage",
        Mock(side_effect=OSError("disk unavailable")),
    )
    assert control_imports._disk_status_by_name(tmp_path)["total"] is None

    monkeypatch.setattr(
        control_imports.db,
        "execute",
        AsyncMock(side_effect=RuntimeError("database unavailable")),
    )
    assert (await control_imports._database_check())["ok"] is False

    broken_redis = Mock()
    broken_redis.ping.side_effect = RuntimeError("redis unavailable")
    monkeypatch.setattr(control_imports, "_redis_client", lambda: broken_redis)
    assert control_imports._redis_check()["ok"] is False

    monkeypatch.setattr(
        control_imports,
        "_worker_health",
        Mock(side_effect=RuntimeError("worker unavailable")),
    )
    monkeypatch.setattr(
        control_imports,
        "_queue_depths",
        Mock(side_effect=RuntimeError("queue unavailable")),
    )
    checks, workers, depths = control_imports._worker_and_queue_health()
    assert checks["workers"]["ok"] is False
    assert checks["queue_depth"]["ok"] is False
    assert workers == {}
    assert depths == {}


def test_queue_depths_and_redis_clients_cover_both_configurations(monkeypatch):
    redis_client_function = control_imports._redis_client
    client = Mock()
    client.zcard.side_effect = lambda queue: len(queue)
    monkeypatch.setattr(control_imports, "_redis_client", lambda: client)
    monkeypatch.setattr(
        control_imports,
        "_SINGLE_JOB_ADAPTERS",
        {"start": {"queue": "arq:start"}, "finish": {"queue": "arq:finish"}},
    )
    monkeypatch.setattr(
        control_imports,
        "_FINISH_IMPORTERS",
        {"finish", "missing"},
    )
    monkeypatch.setattr(control_imports, "_PTG_CONTROL_QUEUES", {"arq:base"})
    depths = control_imports._queue_depths()
    assert set(depths) == {"arq:base", "arq:start", "arq:finish", "arq:finish_finish"}
    monkeypatch.setattr(control_imports, "_redis_client", redis_client_function)

    from_url = Mock(return_value="url-client")
    monkeypatch.setattr(control_imports.redis.Redis, "from_url", from_url)
    monkeypatch.setenv("HLTHPRT_REDIS_ADDRESS", "redis://coverage")
    assert control_imports._redis_client() == "url-client"

    monkeypatch.delenv("HLTHPRT_REDIS_ADDRESS")
    monkeypatch.setattr(
        control_imports,
        "build_redis_settings",
        lambda: SimpleNamespace(
            host="localhost",
            port=6379,
            password=None,
            database=2,
            ssl=False,
        ),
    )
    redis_constructor = Mock(return_value="settings-client")
    monkeypatch.setattr(control_imports.redis, "Redis", redis_constructor)
    assert control_imports._redis_client() == "settings-client"


@pytest.mark.asyncio
async def test_import_run_table_guards_cover_racing_and_unconfigured_db(
    monkeypatch,
):
    ensure_table_once_function = control_imports._ensure_import_run_table_once

    class RacingLock:
        async def __aenter__(self):
            control_imports._IMPORT_RUN_ENSURE_STATE.ensured = True

        async def __aexit__(self, *_args):
            return False

    monkeypatch.setattr(
        control_imports._IMPORT_RUN_ENSURE_STATE,
        "ensured",
        False,
    )
    monkeypatch.setattr(control_imports, "_IMPORT_RUN_ENSURE_LOCK", RacingLock())
    ensure_once = AsyncMock()
    monkeypatch.setattr(control_imports, "_ensure_import_run_table_once", ensure_once)
    await control_imports.ensure_import_run_table()
    ensure_once.assert_not_awaited()

    monkeypatch.setattr(
        control_imports,
        "_ensure_import_run_table_once",
        ensure_table_once_function,
    )
    monkeypatch.setattr(control_imports, "db", SimpleNamespace())
    await control_imports._ensure_import_run_table_once()
    database = SimpleNamespace(connect=AsyncMock(), engine=None)
    monkeypatch.setattr(control_imports, "db", database)
    await control_imports._ensure_import_run_table_once()
    database.connect.assert_awaited_once()


def test_string_normalization_and_run_shapes_cover_alternates(monkeypatch):
    assert control_imports._string_list("  one ") == ["one"]
    assert control_imports._string_list("  ") is None
    assert control_imports._string_list([" one ", "", "two"]) == ["one", "two"]
    assert control_imports._string_list(object()) is None
    assert control_imports.normalize_run(None) == {}

    class JsonRun:
        def to_json_dict(self):
            return {"run_id": "run-json", "status": "queued"}

    monkeypatch.setattr(control_imports, "_overlay_live_progress", lambda run: run)
    assert control_imports.normalize_run(JsonRun())["run_id"] == "run-json"


def test_import_run_cursor_rejects_invalid_and_normalizes_aware_time():
    assert control_imports._encode_import_run_cursor(None, "run") is None
    aware_time = dt.datetime(2026, 7, 21, tzinfo=dt.UTC)
    cursor = control_imports._encode_import_run_cursor(aware_time, "run-aware")
    decoded_time, decoded_run_id = control_imports._decode_import_run_cursor(cursor)
    assert decoded_time == aware_time.replace(tzinfo=None)
    assert decoded_run_id == "run-aware"

    with pytest.raises(ValueError, match="invalid cursor"):
        control_imports._decode_import_run_cursor("not-json")
    blank_payload = json.dumps(
        {"created_at": "2026-07-21T00:00:00", "run_id": " "}
    ).encode()
    blank_cursor = base64.urlsafe_b64encode(blank_payload).decode().rstrip("=")
    with pytest.raises(ValueError, match="invalid cursor"):
        control_imports._decode_import_run_cursor(blank_cursor)


@pytest.mark.asyncio
async def test_worker_state_helpers_cover_unsupported_error_and_failed_items(
    monkeypatch,
):
    monkeypatch.setattr(
        control_imports,
        "_active_worker_cancel_payload",
        lambda _run: {},
    )
    assert (await control_imports._active_worker_state({}))["status"] == "unsupported"

    monkeypatch.setattr(
        control_imports,
        "_active_worker_cancel_payload",
        lambda _run: {"queue": "arq:test"},
    )
    monkeypatch.setattr(
        control_imports.asyncio,
        "to_thread",
        AsyncMock(side_effect=RuntimeError("worker lookup failed")),
    )
    assert (await control_imports._active_worker_state({}))["status"] == "error"
    assert control_imports._failed_worker_state_item({"items": None}) is None
    assert control_imports._failed_worker_state_item({"items": ["invalid"]}) is None
    failed_item_by_field = {"job_status": "failed", "job_name": "coverage-job"}
    assert control_imports._failed_worker_state_item(
        {"items": [failed_item_by_field]}
    ) == failed_item_by_field
    error = control_imports._worker_job_failure_error(
        {**failed_item_by_field, "failure": {"exitCode": 17}}
    )
    assert error["exitCode"] == 17


def _acquisition_params() -> dict[str, object]:
    return {
        "import_resources": True,
        "stale_cleanup": False,
        "publish_artifacts": False,
        "publish_after_acquisition": False,
        "publish_corroboration": False,
        "source_concurrency": 1,
        "source_ids": ["source-one"],
    }


@pytest.mark.parametrize("source_concurrency", [object(), 2])
def test_acquisition_scope_rejects_invalid_concurrency(source_concurrency):
    params = _acquisition_params()
    params["source_concurrency"] = source_concurrency
    assert control_imports._provider_directory_acquisition_scope(params) is None


def test_acquisition_scope_rejects_source_and_discovered_endpoint_edges():
    params = _acquisition_params()
    params["source_ids"] = []
    assert control_imports._provider_directory_acquisition_scope(params) is None

    params = _acquisition_params()
    metrics_by_name = {
        "active_source_groups": ["invalid", {}, {"api_base": "http://bad"}]
    }
    assert (
        control_imports._provider_directory_acquisition_scope(
            params,
            metrics_by_name,
        )
        is None
    )

    metrics_by_name = {
        "active_source_groups": [
            {"api_base": "https://one.test/fhir"},
            {"api_base": "https://two.test/fhir"},
        ]
    }
    assert (
        control_imports._provider_directory_acquisition_scope(
            params,
            metrics_by_name,
        )
        is None
    )
