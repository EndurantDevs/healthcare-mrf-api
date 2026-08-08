# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""CLI contracts for the fixed reviewed formulary operator."""

from __future__ import annotations

import asyncio
import builtins
import datetime as dt
import importlib.util
from pathlib import Path
import signal
import sys
from types import ModuleType
from typing import Any

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = (
    ROOT / "scripts" / "smoke" / "formulary_fhir_reviewed_operator.py"
)
CUTOFF_TEXT = "2026-08-08T08:00:00Z"
CUTOFF_AT = dt.datetime(2026, 8, 8, 8, tzinfo=dt.UTC)
INVALID_ARGUMENTS_JSON = (
    '{"code":"invalid_arguments","status":"error"}\n'
)


def _script_module():
    module_spec = importlib.util.spec_from_file_location(
        "formulary_fhir_reviewed_operator_script",
        SCRIPT_PATH,
    )
    if module_spec is None or module_spec.loader is None:
        raise AssertionError("reviewed operator script is unavailable")
    script_module = importlib.util.module_from_spec(module_spec)
    sys.modules[module_spec.name] = script_module
    module_spec.loader.exec_module(script_module)
    return script_module


def _forbid_runtime_imports(monkeypatch):
    original_import = builtins.__import__

    def guarded_import(module_name, *positional_arguments, **keyword_arguments):
        if module_name == "db.connection" or module_name.startswith(
            "process.formulary_fhir"
        ):
            raise AssertionError(
                f"runtime import before accepted arguments: {module_name}"
            )
        return original_import(
            module_name,
            *positional_arguments,
            **keyword_arguments,
        )

    monkeypatch.setattr(builtins, "__import__", guarded_import)


def test_help_is_fixed_and_does_not_import_runtime(monkeypatch, capsys):
    script_module = _script_module()
    _forbid_runtime_imports(monkeypatch)

    with pytest.raises(SystemExit) as caught:
        script_module.run_command(["--help"])

    captured = capsys.readouterr()
    assert caught.value.code == 0
    assert captured.err == ""
    assert "acquire-twins" in captured.out
    assert "publish-admitted" in captured.out
    assert "--cutoff" in captured.out
    for forbidden_selector in (
        "--source-id",
        "--run-id",
        "--baseline-run-id",
        "--candidate-run-id",
        "--dataset-id",
        "--generation",
        "--intent",
    ):
        assert forbidden_selector not in captured.out


@pytest.mark.parametrize(
    "arguments",
    [
        ["different", "--cutoff", CUTOFF_TEXT],
        ["acquire-twins", "--cut", CUTOFF_TEXT],
        [
            "acquire-twins",
            "--source-id",
            "https://private.example.invalid/fhir?token=secret-cursor",
            "--cutoff",
            CUTOFF_TEXT,
        ],
        [
            "publish-admitted",
            "--cutoff",
            "https://private.example.invalid/secret-cutoff",
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


def test_cutoff_accepts_only_the_canonical_utc_round_trip():
    script_module = _script_module()

    parsed_arguments = script_module._parser().parse_args(
        ["acquire-twins", "--cutoff", CUTOFF_TEXT]
    )

    assert parsed_arguments.cutoff == CUTOFF_AT


@pytest.mark.parametrize(
    "cutoff_text",
    [
        "2026-08-08T08:00:00+00:00",
        "2026-08-08T10:00:00+02:00",
        "2026-08-08T08:00:00",
        " 2026-08-08T08:00:00Z",
        "2026-08-08T08:00:00Z ",
        "2026-08-08 08:00:00Z",
        "2026-08-08T08:00Z",
        "2026-08-08T08:00:00.000000Z",
    ],
)
def test_noncanonical_cutoffs_are_redacted(capsys, cutoff_text):
    script_module = _script_module()

    with pytest.raises(SystemExit) as caught:
        script_module.run_command(
            ["acquire-twins", "--cutoff", cutoff_text]
        )

    captured = capsys.readouterr()
    assert caught.value.code == 2
    assert captured.out == ""
    assert captured.err == INVALID_ARGUMENTS_JSON


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("command", "module_name", "operation_name", "serializer_name"),
    [
        (
            "acquire-twins",
            "process.formulary_fhir.reviewed_acquisition",
            "acquire_reviewed_twins",
            "acquisition_result_json",
        ),
        (
            "publish-admitted",
            "process.formulary_fhir.reviewed_publication",
            "publish_reviewed_candidate",
            "publication_result_json",
        ),
    ],
)
async def test_execute_dispatches_only_the_fixed_operation(
    monkeypatch,
    command,
    module_name,
    operation_name,
    serializer_name,
):
    script_module = _script_module()
    operation_module = ModuleType(module_name)
    database = object()
    operation_result = object()
    operation_calls: list[dict[str, Any]] = []

    async def fixed_operation(**keyword_arguments):
        operation_calls.append(keyword_arguments)
        return operation_result

    def fixed_serializer(candidate_result):
        assert candidate_result is operation_result
        return '{"status":"safe"}'

    setattr(operation_module, operation_name, fixed_operation)
    setattr(operation_module, serializer_name, fixed_serializer)
    monkeypatch.setitem(sys.modules, module_name, operation_module)

    rendered_result = await script_module._execute_operation(
        command,
        CUTOFF_AT,
        database,
    )

    assert rendered_result == '{"status":"safe"}'
    assert operation_calls == [{"cutoff": CUTOFF_AT, "database": database}]


def test_success_prints_only_serializer_json(monkeypatch, capsys):
    script_module = _script_module()
    safe_result = '{"candidate_dataset_id":"ffd_safe","status":"published"}'

    async def run_operation(command, cutoff_at):
        assert command == "publish-admitted"
        assert cutoff_at == CUTOFF_AT
        return safe_result

    monkeypatch.setattr(script_module, "_run_reviewed_operation", run_operation)

    exit_code = script_module.run_command(
        ["publish-admitted", "--cutoff", CUTOFF_TEXT]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.out == safe_result + "\n"
    assert captured.err == ""


@pytest.mark.parametrize(
    ("raised_error", "expected_code", "expected_exit"),
    [
        (TimeoutError("private timeout"), "timeout", 1),
        (RuntimeError("private token=secret"), "failed", 1),
        (asyncio.CancelledError(signal.SIGINT), "canceled", 130),
        (asyncio.CancelledError(signal.SIGTERM), "canceled", 143),
    ],
)
def test_failures_and_signal_cancellation_are_sanitized(
    monkeypatch,
    capsys,
    raised_error,
    expected_code,
    expected_exit,
):
    script_module = _script_module()

    async def fail_operation(_command, _cutoff_at):
        raise raised_error

    monkeypatch.setattr(script_module, "_run_reviewed_operation", fail_operation)

    exit_code = script_module.run_command(
        ["acquire-twins", "--cutoff", CUTOFF_TEXT]
    )

    captured = capsys.readouterr()
    assert exit_code == expected_exit
    assert captured.out == ""
    assert captured.err == f'{{"code":"{expected_code}","status":"error"}}\n'
    assert "private" not in captured.err
    assert "secret" not in captured.err


def test_stable_domain_code_is_preserved(monkeypatch, capsys):
    script_module = _script_module()

    class StableOperationError(RuntimeError):
        code = "missing"

    async def fail_operation(_command, _cutoff_at):
        raise StableOperationError("private admission evidence")

    monkeypatch.setattr(script_module, "_run_reviewed_operation", fail_operation)

    exit_code = script_module.run_command(
        ["publish-admitted", "--cutoff", CUTOFF_TEXT]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert captured.err == '{"code":"missing","status":"error"}\n'
    assert "private" not in captured.err


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
async def test_run_disconnects_once_and_restores_signals(monkeypatch):
    script_module = _script_module()
    lifecycle_events: list[str] = []
    database = _FakeDatabase(lifecycle_events)

    async def execute_operation(command, cutoff_at, selected_database):
        assert (command, cutoff_at, selected_database) == (
            "acquire-twins",
            CUTOFF_AT,
            database,
        )
        lifecycle_events.append("operation")
        return '{"status":"admitted"}'

    monkeypatch.setattr(script_module, "_execute_operation", execute_operation)
    monkeypatch.setattr(
        script_module,
        "_install_signal_handlers",
        lambda _task: _FakeSignalRegistration(lifecycle_events),
    )

    rendered_result = await script_module._run_reviewed_operation(
        "acquire-twins",
        CUTOFF_AT,
        database=database,
    )

    assert rendered_result == '{"status":"admitted"}'
    assert lifecycle_events == ["operation", "disconnect", "restore-signals"]


@pytest.mark.asyncio
async def test_operation_cancellation_survives_disconnect_failure(monkeypatch):
    script_module = _script_module()
    lifecycle_events: list[str] = []
    database = _FakeDatabase(
        lifecycle_events,
        RuntimeError("private disconnect failure"),
    )

    async def cancel_operation(_command, _cutoff_at, _database):
        lifecycle_events.append("operation")
        raise asyncio.CancelledError(signal.SIGTERM)

    monkeypatch.setattr(script_module, "_execute_operation", cancel_operation)
    monkeypatch.setattr(
        script_module,
        "_install_signal_handlers",
        lambda _task: _FakeSignalRegistration(lifecycle_events),
    )

    with pytest.raises(asyncio.CancelledError) as caught:
        await script_module._run_reviewed_operation(
            "acquire-twins",
            CUTOFF_AT,
            database=database,
        )

    assert caught.value.args == (signal.SIGTERM,)
    assert lifecycle_events == ["operation", "disconnect", "restore-signals"]


@pytest.mark.asyncio
async def test_disconnect_is_drained_through_repeated_cancellation():
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


class _FakeSignalLoop:
    def __init__(self, failing_signal: signal.Signals | None = None) -> None:
        self.failing_signal = failing_signal
        self.callback_by_signal: dict[signal.Signals, tuple[Any, tuple[Any, ...]]] = {}
        self.removed_signals: list[signal.Signals] = []

    def add_signal_handler(self, signal_number, callback, *arguments) -> None:
        if signal_number == self.failing_signal:
            raise RuntimeError("unsupported")
        self.callback_by_signal[signal_number] = (callback, arguments)

    def remove_signal_handler(self, signal_number) -> None:
        self.removed_signals.append(signal_number)
        self.callback_by_signal.pop(signal_number, None)


class _FakeTask:
    def __init__(self) -> None:
        self.canceled_by_signal: list[signal.Signals] = []

    def cancel(self, signal_number) -> None:
        self.canceled_by_signal.append(signal_number)


def test_signal_handlers_cancel_task_and_restore_prior_handlers(monkeypatch):
    script_module = _script_module()
    operation_loop = _FakeSignalLoop()
    active_task = _FakeTask()
    restored_handlers: list[tuple[signal.Signals, object]] = []
    monkeypatch.setattr(
        script_module.signal,
        "getsignal",
        lambda signal_number: f"previous-{signal_number.value}",
    )
    monkeypatch.setattr(
        script_module.signal,
        "signal",
        lambda signal_number, handler: restored_handlers.append(
            (signal_number, handler)
        ),
    )

    registration = script_module._install_signal_handlers(
        active_task,
        operation_loop=operation_loop,
    )
    for signal_number in script_module.SIGNALS:
        callback, callback_arguments = operation_loop.callback_by_signal[
            signal_number
        ]
        callback(*callback_arguments)
    registration.restore()
    registration.restore()

    assert active_task.canceled_by_signal == [signal.SIGTERM, signal.SIGINT]
    assert operation_loop.removed_signals == [signal.SIGINT, signal.SIGTERM]
    assert restored_handlers == [
        (signal.SIGINT, f"previous-{signal.SIGINT.value}"),
        (signal.SIGTERM, f"previous-{signal.SIGTERM.value}"),
    ]


def test_partial_signal_setup_is_rolled_back(monkeypatch):
    script_module = _script_module()
    operation_loop = _FakeSignalLoop(failing_signal=signal.SIGINT)
    restored_signals: list[signal.Signals] = []
    monkeypatch.setattr(script_module.signal, "getsignal", lambda _signal: None)
    monkeypatch.setattr(
        script_module.signal,
        "signal",
        lambda signal_number, _handler: restored_signals.append(signal_number),
    )

    with pytest.raises(RuntimeError, match="signal setup failed"):
        script_module._install_signal_handlers(
            _FakeTask(),
            operation_loop=operation_loop,
        )

    assert operation_loop.removed_signals == [signal.SIGTERM]
    assert restored_signals == [signal.SIGTERM]
