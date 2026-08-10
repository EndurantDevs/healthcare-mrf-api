# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Utility and shutdown boundaries for the rooted graph operator CLI."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
import runpy
import signal
import sys
from typing import Any

import pytest

from scripts.smoke import provider_directory_rooted_graph_operator as cli


def _disable_all(monkeypatch: pytest.MonkeyPatch) -> None:
    for gate_name in cli._GATE_BY_COMMAND.values():
        monkeypatch.delenv(gate_name, raising=False)


def test_direct_module_entrypoint_inserts_root_and_stays_dormant(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _disable_all(monkeypatch)
    root = str(cli.ROOT)
    monkeypatch.setattr(sys, "path", [path for path in sys.path if path != root])
    monkeypatch.setattr(sys, "argv", [str(Path(cli.__file__))])

    with pytest.raises(SystemExit) as caught:
        runpy.run_path(cli.__file__, run_name="__main__")

    assert caught.value.code == 1
    assert root in sys.path
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == '{"code":"disabled","status":"error"}\n'


def test_numeric_parsers_cover_success_and_type_failures() -> None:
    assert cli._bounded_integer("2", 1, 3) == 2
    assert cli._bounded_float("1.5", 1.0, 2.0) == 1.5

    for raw_value in (object(), "not-an-integer"):
        with pytest.raises(argparse.ArgumentTypeError):
            cli._bounded_integer(raw_value, 1, 3)
    for raw_value in (object(), "not-a-float", "nan", " 1.5"):
        with pytest.raises(argparse.ArgumentTypeError):
            cli._bounded_float(raw_value, 1.0, 2.0)


def test_unknown_command_preflight_requires_some_explicit_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _disable_all(monkeypatch)
    assert cli._preflight_gate(("unknown",)) == "disabled"
    monkeypatch.setenv(cli.REGISTRATION_ENABLED_ENV, "true")
    assert cli._preflight_gate(("unknown",)) is None


class _Loop:
    def __init__(self, *, fail_after: int | None = None) -> None:
        self.added: list[tuple[Any, ...]] = []
        self.removed: list[signal.Signals] = []
        self.fail_after = fail_after

    def add_signal_handler(self, *values: Any) -> None:
        if self.fail_after is not None and len(self.added) == self.fail_after:
            raise RuntimeError("synthetic")
        self.added.append(values)

    def remove_signal_handler(self, signal_number: signal.Signals) -> None:
        self.removed.append(signal_number)


class _Task:
    def cancel(self, *_args: Any) -> None:
        return None


def test_signal_handlers_install_restore_and_restore_idempotently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = _Loop()
    restored_handlers: list[tuple[Any, Any]] = []
    monkeypatch.setattr(signal, "getsignal", lambda number: ("prior", number))
    monkeypatch.setattr(
        signal,
        "signal",
        lambda *values: restored_handlers.append(values),
    )

    registration = cli._install_signal_handlers(_Task(), operation_loop=loop)
    registration.restore()
    registration.restore()

    assert [values[0] for values in loop.added] == list(cli._SIGNALS)
    assert loop.removed == list(reversed(cli._SIGNALS))
    assert len(restored_handlers) == len(cli._SIGNALS)


@pytest.mark.asyncio
async def test_signal_handlers_use_running_loop_and_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = _Loop(fail_after=1)
    restored_handlers: list[tuple[Any, Any]] = []
    monkeypatch.setattr(cli.asyncio, "get_running_loop", lambda: loop)
    monkeypatch.setattr(signal, "getsignal", lambda number: ("prior", number))
    monkeypatch.setattr(
        signal,
        "signal",
        lambda *values: restored_handlers.append(values),
    )

    with pytest.raises(RuntimeError, match="signal setup failed"):
        cli._install_signal_handlers(_Task())

    assert loop.removed == [cli._SIGNALS[0]]
    assert len(restored_handlers) == 1


class _DisconnectDatabase:
    def __init__(self, raised_error: BaseException | None = None) -> None:
        self.raised_error = raised_error

    async def disconnect(self) -> None:
        await asyncio.sleep(0)
        if self.raised_error is not None:
            raise self.raised_error


@pytest.mark.asyncio
async def test_disconnect_drain_succeeds_and_propagates_failure() -> None:
    await cli._drain_disconnect(
        _DisconnectDatabase(),
        is_preserving_cancellation=False,
    )
    with pytest.raises(RuntimeError, match="synthetic"):
        await cli._drain_disconnect(
            _DisconnectDatabase(RuntimeError("synthetic")),
            is_preserving_cancellation=False,
        )
    await cli._drain_disconnect(
        _DisconnectDatabase(RuntimeError("ignored")),
        is_preserving_cancellation=True,
    )


class _CancelFirstShield:
    def __init__(self, original_shield: Any) -> None:
        self.original_shield = original_shield
        self.has_canceled = False

    async def __call__(self, awaitable: Any) -> Any:
        if not self.has_canceled:
            self.has_canceled = True
            await asyncio.sleep(0)
            raise asyncio.CancelledError()
        return await self.original_shield(awaitable)


def _cancel_first_shield(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli.asyncio, "shield", _CancelFirstShield(asyncio.shield))


@pytest.mark.asyncio
async def test_disconnect_drain_replays_cancellation_after_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _cancel_first_shield(monkeypatch)
    with pytest.raises(asyncio.CancelledError):
        await cli._drain_disconnect(
            _DisconnectDatabase(),
            is_preserving_cancellation=False,
        )


@pytest.mark.asyncio
async def test_disconnect_drain_prefers_cancellation_over_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _cancel_first_shield(monkeypatch)
    with pytest.raises(asyncio.CancelledError):
        await cli._drain_disconnect(
            _DisconnectDatabase(RuntimeError("private")),
            is_preserving_cancellation=False,
        )
