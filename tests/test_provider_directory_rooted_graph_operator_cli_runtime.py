# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Dispatch and process-boundary coverage for the rooted graph CLI."""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys
from types import ModuleType
from typing import Any

import pytest

from process import provider_directory_rooted_graph_operator as operator
from process import provider_directory_rooted_graph_operator_contract as contract
from scripts.smoke import provider_directory_rooted_graph_operator as cli


OPERATION_KEY = "a" * 64
PUBLICATION_ACQUISITION_ID = "pdrga_" + "b" * 48


def _enable_only(monkeypatch: pytest.MonkeyPatch, selected: str) -> None:
    for gate_name in cli._GATE_BY_COMMAND.values():
        monkeypatch.setenv(
            gate_name,
            "true" if gate_name == selected else "false",
        )


def _arguments(command: str) -> argparse.Namespace:
    values_by_name: dict[str, Any] = {"command": command}
    if command == "acquire":
        values_by_name.update(
            operation_key=OPERATION_KEY,
            concurrency=4,
            max_attempts=3,
            lease_seconds=300,
            retry_base_seconds=1.0,
            max_retry_seconds=60.0,
            root_timeout_seconds=604_800.0,
        )
    elif command == "publish":
        values_by_name.update(
            publication_acquisition_id=PUBLICATION_ACQUISITION_ID,
            batch_size=4096,
        )
    return argparse.Namespace(**values_by_name)


@pytest.mark.asyncio
async def test_dispatches_each_exact_phase_and_rejects_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    async def register(**values: Any) -> str:
        calls.append(("register", values))
        return "registered"

    async def acquire(**values: Any) -> str:
        calls.append(("acquire", values))
        return "acquired"

    async def publish(**values: Any) -> str:
        calls.append(("publish", values))
        return "published"

    monkeypatch.setattr(operator, "register_rooted_graph_source_operation", register)
    monkeypatch.setattr(operator, "acquire_admit_rooted_graph_operation", acquire)
    monkeypatch.setattr(operator, "publish_admitted_rooted_graph_operation", publish)
    database = object()

    assert await cli._execute_selected_phase(_arguments("register"), database) == (
        "registered"
    )
    assert await cli._execute_selected_phase(_arguments("acquire"), database) == (
        "acquired"
    )
    assert await cli._execute_selected_phase(_arguments("publish"), database) == (
        "published"
    )
    with pytest.raises(RuntimeError, match="command is invalid"):
        await cli._execute_selected_phase(_arguments("unknown"), database)

    assert [name for name, _values in calls] == ["register", "acquire", "publish"]
    assert calls[0][1] == {"database": database}
    assert calls[1][1]["operation_key"] == OPERATION_KEY
    assert calls[2][1]["batch_size"] == 4096


class _Registration:
    def __init__(self) -> None:
        self.restored = False

    def restore(self) -> None:
        self.restored = True


class _Database:
    def __init__(self) -> None:
        self.disconnected = False

    async def disconnect(self) -> None:
        self.disconnected = True


@pytest.mark.asyncio
async def test_run_operator_uses_packaged_database_and_drains(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_only(monkeypatch, cli.REGISTRATION_ENABLED_ENV)
    registration = _Registration()
    database = _Database()
    connection_module = ModuleType("db.connection")
    connection_module.db = database
    monkeypatch.setitem(sys.modules, connection_module.__name__, connection_module)
    monkeypatch.setattr(
        cli,
        "_install_signal_handlers",
        lambda _task: registration,
    )

    async def execute(arguments: argparse.Namespace, selected: Any) -> str:
        assert arguments.command == "register"
        assert selected is database
        return '{"status":"registered"}'

    monkeypatch.setattr(cli, "_execute_selected_phase", execute)

    rendered = await cli._run_operator(_arguments("register"))

    assert rendered == '{"status":"registered"}'
    assert database.disconnected is True
    assert registration.restored is True


@pytest.mark.asyncio
async def test_run_operator_rejects_missing_active_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_only(monkeypatch, cli.REGISTRATION_ENABLED_ENV)
    monkeypatch.setattr(cli.asyncio, "current_task", lambda: None)

    with pytest.raises(RuntimeError, match="task is unavailable"):
        await cli._run_operator(_arguments("register"), database=_Database())


@pytest.mark.asyncio
async def test_run_operator_restores_signals_when_database_import_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_only(monkeypatch, cli.REGISTRATION_ENABLED_ENV)
    registration = _Registration()
    connection_module = ModuleType("db.connection")
    monkeypatch.setitem(sys.modules, connection_module.__name__, connection_module)
    monkeypatch.setattr(
        cli,
        "_install_signal_handlers",
        lambda _task: registration,
    )

    with pytest.raises(ImportError):
        await cli._run_operator(_arguments("register"))
    assert registration.restored is True


def test_error_and_cancellation_code_helpers_are_closed() -> None:
    assert (
        cli._operation_error_code(
            contract.ProviderDirectoryRootedGraphOperatorError("busy")
        )
        == "busy"
    )
    assert cli._operation_error_code(RuntimeError("private")) == "failed"
    assert cli._cancellation_exit_code(asyncio.CancelledError(signal.SIGTERM)) == 143
    assert cli._cancellation_exit_code(asyncio.CancelledError()) == 1


@pytest.mark.parametrize(
    ("raised_error", "expected_code", "expected_exit"),
    (
        (KeyboardInterrupt(), "canceled", 130),
        (TimeoutError(), "timeout", 1),
        (
            contract.ProviderDirectoryRootedGraphOperatorError("busy"),
            "busy",
            1,
        ),
        (RuntimeError("private"), "failed", 1),
    ),
)
def test_run_command_serializes_bounded_failures(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    raised_error: BaseException,
    expected_code: str,
    expected_exit: int,
) -> None:
    _enable_only(monkeypatch, cli.REGISTRATION_ENABLED_ENV)

    async def fail(_arguments: argparse.Namespace) -> str:
        raise raised_error

    monkeypatch.setattr(cli, "_run_operator", fail)

    assert cli.run_command(["register"]) == expected_exit
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == f'{{"code":"{expected_code}","status":"error"}}\n'


def test_run_command_prints_only_rendered_success(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _enable_only(monkeypatch, cli.REGISTRATION_ENABLED_ENV)

    async def succeed(_arguments: argparse.Namespace) -> str:
        return '{"status":"registered"}'

    monkeypatch.setattr(cli, "_run_operator", succeed)

    assert cli.run_command(["register"]) == 0
    captured = capsys.readouterr()
    assert captured.out == '{"status":"registered"}\n'
    assert captured.err == ""
