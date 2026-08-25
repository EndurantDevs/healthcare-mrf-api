# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""CLI proof for the default-off exact-cohort Practitioner operator."""

from __future__ import annotations

import argparse
import asyncio
import builtins
import importlib.util
from pathlib import Path
import signal
import sys
from types import ModuleType
from typing import Any

import pytest

from process import uhc_flex_practitioner_operator as operation


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "smoke" / "uhc_flex_practitioner_operator.py"
OPERATION_KEY = "a" * 64
CANDIDATE_ACQUISITION_ID = "pdufpa_" + "b" * 48
INVALID_ARGUMENTS_JSON = '{"code":"invalid_arguments","status":"error"}\n'


def _script_module():
    module_spec = importlib.util.spec_from_file_location(
        "uhc_flex_practitioner_operator_script",
        SCRIPT_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    script_module = importlib.util.module_from_spec(module_spec)
    sys.modules[module_spec.name] = script_module
    module_spec.loader.exec_module(script_module)
    return script_module


def _disable_all_gates(monkeypatch) -> None:
    for gate_name in (
        operation.COHORT_ENABLED_ENV,
        operation.ACQUISITION_ENABLED_ENV,
        operation.SINGLE_ROOT_ACQUISITION_ENABLED_ENV,
        operation.PUBLICATION_ENABLED_ENV,
    ):
        monkeypatch.delenv(gate_name, raising=False)


def _forbid_runtime_imports(monkeypatch) -> None:
    original_import = builtins.__import__

    def guarded_import(module_name, *args, **kwargs):
        if module_name == "db.connection" or module_name.startswith(
            "process.uhc_flex_"
        ):
            raise AssertionError(
                f"runtime import before accepted arguments: {module_name}"
            )
        return original_import(module_name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)


def _acquisition_arguments() -> list[str]:
    return [
        "acquire-admit",
        "--operation-key",
        OPERATION_KEY,
        "--semantic-projection-as-of",
        "2026-08-10",
    ]


def test_help_is_runtime_free_and_exposes_no_broad_scan(monkeypatch, capsys):
    script_module = _script_module()
    _forbid_runtime_imports(monkeypatch)
    with pytest.raises(SystemExit) as caught:
        script_module.run_command(["--help"])

    captured = capsys.readouterr()
    assert caught.value.code == 0
    assert captured.err == ""
    assert "sync-cohort" in captured.out
    assert "acquire-admit" in captured.out
    assert "acquire-admit-single-root" in captured.out
    assert "publish-admitted" in captured.out
    for forbidden_control in (
        "--source-id",
        "--endpoint",
        "--url",
        "--page-limit",
        "--resource-type",
        "profile-dispatch",
    ):
        assert forbidden_control not in captured.out


@pytest.mark.parametrize(
    "arguments",
    [
        ["different"],
        ["acquire-admit", "--operation-key", "secret-token"],
        [
            *_acquisition_arguments(),
            "--source-id",
            "https://private.example.invalid/fhir?token=secret",
        ],
        [
            *_acquisition_arguments(),
            "--concurrency",
            "33",
        ],
        [
            *_acquisition_arguments(),
            "--semantic-projection-as-of",
            "2026-8-10",
        ],
        ["publish-admitted", "--candidate-acquisition-id", "secret"],
        [
            "publish-admitted",
            "--candidate-acquisition-id",
            CANDIDATE_ACQUISITION_ID,
            "--batch-size",
            "0",
        ],
    ],
)
def test_invalid_arguments_are_redacted_before_runtime_import(
    monkeypatch,
    capsys,
    arguments,
):
    script_module = _script_module()
    _forbid_runtime_imports(monkeypatch)

    with pytest.raises(SystemExit) as caught:
        script_module.run_command(arguments)

    captured = capsys.readouterr()
    assert caught.value.code == 2
    assert captured.out == ""
    assert captured.err == INVALID_ARGUMENTS_JSON
    assert "private" not in captured.err
    assert "secret" not in captured.err


def test_acquisition_defaults_match_the_bounded_runtime_contract() -> None:
    script_module = _script_module()

    parsed = script_module._parser().parse_args(_acquisition_arguments())

    assert parsed.operation_key == OPERATION_KEY
    assert parsed.semantic_projection_as_of == "2026-08-10"
    assert parsed.concurrency == 4
    assert parsed.max_attempts == 3
    assert parsed.lease_seconds == 300
    assert parsed.retry_base_seconds == 1.0
    assert parsed.max_retry_seconds == 60.0
    assert script_module._parser().parse_args(
        [*_acquisition_arguments(), "--concurrency", "32"]
    ).concurrency == 32


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("arguments", "operation_name", "expected_values"),
    [
        (
            ["sync-cohort"],
            "sync_uhc_flex_practitioner_cohort_operation",
            {},
        ),
        (
            _acquisition_arguments(),
            "acquire_admit_uhc_flex_practitioner_operation",
            {
                "operation_key": OPERATION_KEY,
                "semantic_projection_as_of": "2026-08-10",
                "concurrency": 4,
                "max_attempts": 3,
                "lease_seconds": 300,
                "retry_base_seconds": 1.0,
                "max_retry_seconds": 60.0,
            },
        ),
        (
            [
                "publish-admitted",
                "--candidate-acquisition-id",
                CANDIDATE_ACQUISITION_ID,
            ],
            "publish_admitted_uhc_flex_practitioner_operation",
            {
                "candidate_acquisition_id": CANDIDATE_ACQUISITION_ID,
                "batch_size": 500,
            },
        ),
    ],
)
async def test_execute_dispatches_only_the_selected_exact_phase(
    monkeypatch,
    arguments,
    operation_name,
    expected_values,
):
    script_module = _script_module()
    parsed = script_module._parser().parse_args(arguments)
    database = object()
    calls = []

    async def fixed_operation(**keyword_arguments):
        calls.append(keyword_arguments)
        return '{"status":"safe"}'

    monkeypatch.setattr(operation, operation_name, fixed_operation)

    rendered = await script_module._execute_operation(parsed, database)

    assert rendered == '{"status":"safe"}'
    assert calls == [{**expected_values, "database": database}]


@pytest.mark.parametrize(
    ("arguments", "enabled_gate"),
    (
        (["sync-cohort"], None),
        (_acquisition_arguments(), operation.ACQUISITION_ENABLED_ENV),
    ),
)
def test_disabled_phase_fails_before_database_import(
    monkeypatch,
    capsys,
    arguments,
    enabled_gate,
) -> None:
    script_module = _script_module()
    _disable_all_gates(monkeypatch)
    if enabled_gate is not None:
        monkeypatch.setenv(enabled_gate, "true")
    original_import = builtins.__import__

    def guarded_import(module_name, *args, **kwargs):
        if module_name == "db.connection":
            raise AssertionError("database imported while operator disabled")
        return original_import(module_name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    exit_code = script_module.run_command(arguments)

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert captured.err == '{"code":"disabled","status":"error"}\n'


def test_historical_publication_prints_only_the_sanitized_receipt(
    monkeypatch,
    capsys,
) -> None:
    script_module = _script_module()
    safe_receipt = '{"dataset_id":"pdufpd_safe","status":"published"}'
    monkeypatch.setenv(operation.PUBLICATION_ENABLED_ENV, "true")

    async def run_operation(parsed_arguments):
        assert parsed_arguments.command == "publish-admitted"
        return safe_receipt

    monkeypatch.setattr(script_module, "_run_operation", run_operation)

    exit_code = script_module.run_command(
        ["publish-admitted", "--candidate-acquisition-id", CANDIDATE_ACQUISITION_ID]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.out == safe_receipt + "\n"
    assert captured.err == ""


@pytest.mark.parametrize(
    ("raised_error", "expected_code", "expected_exit"),
    [
        (TimeoutError("private timeout"), "timeout", 1),
        (operation.UHCFlexPractitionerOperatorError("root_retryable"), "root_retryable", 75),
        (RuntimeError("private token=secret"), "failed", 1),
        (asyncio.CancelledError(signal.SIGINT), "canceled", 130),
        (asyncio.CancelledError(signal.SIGTERM), "canceled", 143),
    ],
)
def test_failures_are_sanitized(
    monkeypatch, capsys, raised_error, expected_code, expected_exit
) -> None:
    script_module = _script_module()

    async def fail_operation(_parsed_arguments):
        raise raised_error

    monkeypatch.setattr(script_module, "_run_operation", fail_operation)

    exit_code = script_module.run_command(["sync-cohort"])

    captured = capsys.readouterr()
    assert exit_code == expected_exit
    assert captured.out == ""
    assert captured.err == (f'{{"code":"{expected_code}","status":"error"}}\n')
    assert "private" not in captured.err
    assert "secret" not in captured.err


class _FakeSignalRegistration:
    def __init__(self, lifecycle_events: list[str]) -> None:
        self.lifecycle_events = lifecycle_events

    def restore(self) -> None:
        self.lifecycle_events.append("restore-signals")


class _FakeDatabase:
    def __init__(
        self,
        lifecycle_events: list[str],
        disconnect_error: BaseException | None = None,
    ) -> None:
        self.lifecycle_events = lifecycle_events
        self.disconnect_error = disconnect_error

    async def disconnect(self) -> None:
        self.lifecycle_events.append("disconnect")
        if self.disconnect_error is not None:
            raise self.disconnect_error


@pytest.mark.asyncio
async def test_cancellation_survives_disconnect_failure(monkeypatch) -> None:
    script_module = _script_module()
    lifecycle_events: list[str] = []
    database = _FakeDatabase(
        lifecycle_events,
        RuntimeError("private disconnect failure"),
    )
    parsed = argparse.Namespace(command="sync-cohort")

    monkeypatch.setenv(operation.COHORT_ENABLED_ENV, "true")
    monkeypatch.delenv(operation.ACQUISITION_ENABLED_ENV, raising=False)
    monkeypatch.delenv(operation.PUBLICATION_ENABLED_ENV, raising=False)

    async def cancel_operation(_parsed_arguments, _database):
        lifecycle_events.append("operation")
        raise asyncio.CancelledError(signal.SIGTERM)

    monkeypatch.setattr(script_module, "_execute_operation", cancel_operation)
    monkeypatch.setattr(
        script_module,
        "_install_signal_handlers",
        lambda _task: _FakeSignalRegistration(lifecycle_events),
    )

    with pytest.raises(asyncio.CancelledError) as caught:
        await script_module._run_operation(parsed, database=database)

    assert caught.value.args == (signal.SIGTERM,)
    assert lifecycle_events == ["operation", "disconnect", "restore-signals"]


@pytest.mark.asyncio
async def test_disconnect_drains_through_repeated_cancellation() -> None:
    script_module = _script_module()
    disconnect_started = asyncio.Event()
    allow_disconnect = asyncio.Event()
    disconnect_events: list[str] = []

    class BlockingDatabase:
        async def disconnect(self) -> None:
            disconnect_started.set()
            await allow_disconnect.wait()
            disconnect_events.append("completed")

    drain_task = asyncio.create_task(
        script_module._drain_disconnect(
            BlockingDatabase(),
            preserve_cancellation=False,
        )
    )
    await disconnect_started.wait()
    drain_task.cancel(signal.SIGTERM)
    await asyncio.sleep(0)
    drain_task.cancel(signal.SIGINT)
    await asyncio.sleep(0)

    assert not drain_task.done()
    allow_disconnect.set()
    with pytest.raises(asyncio.CancelledError) as caught:
        await drain_task

    assert caught.value.args == (signal.SIGTERM,)
    assert disconnect_events == ["completed"]


@pytest.mark.asyncio
async def test_disconnect_failure_propagates_without_outer_cancellation() -> None:
    script_module = _script_module()

    class FailingDatabase:
        async def disconnect(self) -> None:
            raise RuntimeError("private disconnect failure")

    with pytest.raises(RuntimeError, match="private disconnect failure"):
        await script_module._drain_disconnect(
            FailingDatabase(),
            preserve_cancellation=False,
        )


@pytest.mark.asyncio
async def test_first_outer_cancellation_wins_over_disconnect_failure() -> None:
    script_module = _script_module()
    disconnect_started = asyncio.Event()
    allow_failure = asyncio.Event()

    class FailingDatabase:
        async def disconnect(self) -> None:
            disconnect_started.set()
            await allow_failure.wait()
            raise RuntimeError("private disconnect failure")

    drain_task = asyncio.create_task(
        script_module._drain_disconnect(
            FailingDatabase(),
            preserve_cancellation=False,
        )
    )
    await disconnect_started.wait()
    drain_task.cancel(signal.SIGTERM)
    await asyncio.sleep(0)
    allow_failure.set()

    with pytest.raises(asyncio.CancelledError) as caught:
        await drain_task

    assert caught.value.args == (signal.SIGTERM,)


@pytest.mark.asyncio
async def test_runtime_database_is_loaded_only_after_the_gate(monkeypatch) -> None:
    script_module = _script_module()
    _disable_all_gates(monkeypatch)
    monkeypatch.setenv(operation.COHORT_ENABLED_ENV, "true")
    lifecycle_events: list[str] = []

    class RuntimeDatabase:
        async def disconnect(self) -> None:
            lifecycle_events.append("disconnect")

    runtime_database = RuntimeDatabase()
    connection_module = ModuleType("db.connection")
    connection_module.db = runtime_database
    monkeypatch.setitem(sys.modules, "db.connection", connection_module)

    async def execute_operation(parsed_arguments, database):
        assert parsed_arguments.command == "sync-cohort"
        assert database is runtime_database
        lifecycle_events.append("operation")
        return '{"status":"sealed"}'

    class SignalRegistration:
        def restore(self) -> None:
            lifecycle_events.append("restore-signals")

    monkeypatch.setattr(script_module, "_execute_operation", execute_operation)
    monkeypatch.setattr(
        script_module,
        "_install_signal_handlers",
        lambda _task: SignalRegistration(),
    )

    rendered_receipt = await script_module._run_operation(
        argparse.Namespace(command="sync-cohort")
    )

    assert rendered_receipt == '{"status":"sealed"}'
    assert lifecycle_events == ["operation", "disconnect", "restore-signals"]


def test_keyboard_interrupt_is_sanitized(monkeypatch, capsys) -> None:
    script_module = _script_module()

    async def interrupt_operation(_parsed_arguments):
        raise KeyboardInterrupt()

    monkeypatch.setattr(script_module, "_run_operation", interrupt_operation)

    exit_code = script_module.run_command(["sync-cohort"])

    captured = capsys.readouterr()
    assert exit_code == 130
    assert captured.out == ""
    assert captured.err == '{"code":"canceled","status":"error"}\n'
