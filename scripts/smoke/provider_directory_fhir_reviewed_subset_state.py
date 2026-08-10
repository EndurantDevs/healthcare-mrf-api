#!/usr/bin/env python
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Render or synchronize one fixed reviewed Provider Directory subset state."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass, field
import os
from pathlib import Path
import signal
import sys
from typing import Any, Sequence


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.environ.setdefault("HLTHPRT_LOG_CFG", str(ROOT / "logging.yaml"))

COMMAND = "sync-verified-state"
EVIDENCE_COMMAND = "render-neutral-evidence"
ABANDON_COMMAND = "abandon-expired-root"
TERMINAL_DISPOSITION_COMMAND = "seal-terminal-root"
_ENABLED_ENV_BY_COMMAND = {
    COMMAND: "HLTHPRT_PROVIDER_DIRECTORY_SUBSET_STATE_SYNC_ENABLED",
    ABANDON_COMMAND: (
        "HLTHPRT_PROVIDER_DIRECTORY_REVIEWED_SUBSET_ABANDONMENT_ENABLED"
    ),
    TERMINAL_DISPOSITION_COMMAND: (
        "HLTHPRT_PROVIDER_DIRECTORY_REVIEWED_SUBSET_"
        "TERMINAL_DISPOSITION_ENABLED"
    ),
}
SIGNALS = (signal.SIGTERM, signal.SIGINT)
SAFE_ERROR_CODES = frozenset(
    {
        "busy",
        "canceled",
        "disabled",
        "evidence",
        "failed",
        "invalid_arguments",
        "state",
        "timeout",
    }
)


def _error_json(code: object) -> str:
    safe_code = (
        code if type(code) is str and code in SAFE_ERROR_CODES else "failed"
    )
    return f'{{"code":"{safe_code}","status":"error"}}'


class RedactedArgumentParser(argparse.ArgumentParser):
    """Reject invalid arguments without reflecting their values."""

    def error(self, _message: str) -> None:
        """Exit with fixed JSON instead of echoing rejected arguments."""

        self.exit(2, _error_json("invalid_arguments") + "\n")


class _DormantGateError(RuntimeError):
    """Fail before runtime imports when a mutating operator is disabled."""

    code = "disabled"


def _parser() -> argparse.ArgumentParser:
    parser = RedactedArgumentParser(
        allow_abbrev=False,
        description=(
            "Render evidence for or synchronize the sole checked-in reviewed "
            "Provider Directory subset state. No selector is accepted."
        ),
    )
    parser.add_argument(
        "command",
        choices=(
            COMMAND,
            EVIDENCE_COMMAND,
            ABANDON_COMMAND,
            TERMINAL_DISPOSITION_COMMAND,
        ),
    )
    return parser


@dataclass(slots=True)
class _SignalRegistration:
    operation_loop: Any
    previous_handler_by_signal: dict[signal.Signals, Any] = field(
        default_factory=dict
    )
    is_restored: bool = False

    def restore(self) -> None:
        """Restore every process handler replaced by this registration."""

        if self.is_restored:
            return
        for signal_number in reversed(tuple(self.previous_handler_by_signal)):
            self.operation_loop.remove_signal_handler(signal_number)
            signal.signal(
                signal_number,
                self.previous_handler_by_signal[signal_number],
            )
        self.is_restored = True


def _install_signal_handlers(
    active_task: asyncio.Task[Any],
    *,
    operation_loop: Any | None = None,
) -> _SignalRegistration:
    selected_loop = operation_loop or asyncio.get_running_loop()
    registration = _SignalRegistration(selected_loop)
    try:
        for signal_number in SIGNALS:
            previous_handler = signal.getsignal(signal_number)
            selected_loop.add_signal_handler(
                signal_number,
                active_task.cancel,
                signal_number,
            )
            registration.previous_handler_by_signal[signal_number] = (
                previous_handler
            )
    except (NotImplementedError, RuntimeError, ValueError):
        registration.restore()
        raise RuntimeError("reviewed subset signal setup failed") from None
    return registration


async def _drain_disconnect(
    database: Any,
    *,
    preserve_cancellation: bool,
) -> None:
    """Finish database disposal despite repeated outer cancellation."""

    disconnect_task = asyncio.create_task(database.disconnect())
    cancellation_error: asyncio.CancelledError | None = None
    while not disconnect_task.done():
        try:
            await asyncio.shield(disconnect_task)
        except asyncio.CancelledError as error:
            if cancellation_error is None:
                cancellation_error = error
        except BaseException:
            break
    try:
        disconnect_task.result()
    except BaseException:
        if preserve_cancellation:
            return
        if cancellation_error is not None:
            raise cancellation_error
        raise
    if cancellation_error is not None and not preserve_cancellation:
        raise cancellation_error


async def _execute_state_sync(database: Any) -> str:
    from process.provider_directory_fhir_subset_activation import (
        reviewed_subset_activation_result_json,
    )
    from process.provider_directory_fhir_subset_activation import (
        sync_reviewed_subset_verified_state,
    )

    result = await sync_reviewed_subset_verified_state(database=database)
    return reviewed_subset_activation_result_json(result)


async def _execute_evidence_render(database: Any) -> str:
    from process.provider_directory_fhir_subset_activation_evidence import (
        reviewed_subset_activation_evidence,
        reviewed_subset_activation_verified_manifest_json,
    )

    evidence = await reviewed_subset_activation_evidence(database=database)
    return reviewed_subset_activation_verified_manifest_json(evidence)


async def _execute_abandonment(database: Any) -> str:
    from process.provider_directory_fhir_subset_abandonment import (
        abandon_reviewed_subset_expired_root,
        abandonment_result_json,
    )

    result = await abandon_reviewed_subset_expired_root(database=database)
    return abandonment_result_json(result)


async def _execute_terminal_disposition(database: Any) -> str:
    from process.provider_directory_fhir_subset_terminal_disposition import (
        dispose_reviewed_subset_census_drift_root,
        terminal_disposition_result_json,
    )

    result = await dispose_reviewed_subset_census_drift_root(database=database)
    return terminal_disposition_result_json(result)


async def _execute_operation(database: Any, command: str) -> str:
    if command == COMMAND:
        return await _execute_state_sync(database)
    if command == EVIDENCE_COMMAND:
        return await _execute_evidence_render(database)
    if command == ABANDON_COMMAND:
        return await _execute_abandonment(database)
    if command == TERMINAL_DISPOSITION_COMMAND:
        return await _execute_terminal_disposition(database)
    raise RuntimeError("reviewed subset operator command is invalid")


async def _run_operator(
    command: str,
    *,
    database: Any | None = None,
) -> str:
    enabled_environment = _ENABLED_ENV_BY_COMMAND.get(command)
    if (
        database is None
        and enabled_environment is not None
        and os.getenv(enabled_environment) != "true"
    ):
        raise _DormantGateError()
    active_task = asyncio.current_task()
    if active_task is None:
        raise RuntimeError("reviewed subset operator task is unavailable")
    signal_registration = _install_signal_handlers(active_task)
    runtime_database = database
    should_preserve_cancellation = False
    try:
        if runtime_database is None:
            from db.connection import db

            runtime_database = db
        return await _execute_operation(runtime_database, command)
    except asyncio.CancelledError:
        should_preserve_cancellation = True
        raise
    finally:
        try:
            if runtime_database is not None:
                await _drain_disconnect(
                    runtime_database,
                    preserve_cancellation=should_preserve_cancellation,
                )
        finally:
            signal_registration.restore()


def _operation_error_code(error: Exception) -> str:
    error_code = getattr(error, "code", None)
    if type(error_code) is str and error_code in SAFE_ERROR_CODES:
        return error_code
    return "failed"


def _cancellation_exit_code(error: asyncio.CancelledError) -> int:
    signal_number = error.args[0] if error.args else None
    if signal_number == signal.SIGINT:
        return 130
    if signal_number == signal.SIGTERM:
        return 143
    return 1


def _print_error(code: str) -> None:
    print(_error_json(code), file=sys.stderr)


def run_command(arguments: Sequence[str] | None = None) -> int:
    """Run one selector-free operation with deterministic JSON output."""

    parsed_arguments = _parser().parse_args(arguments)
    try:
        rendered_result = asyncio.run(_run_operator(parsed_arguments.command))
    except asyncio.CancelledError as error:
        _print_error("canceled")
        return _cancellation_exit_code(error)
    except KeyboardInterrupt:
        _print_error("canceled")
        return 130
    except TimeoutError:
        _print_error("timeout")
        return 1
    except Exception as error:
        _print_error(_operation_error_code(error))
        return 1
    print(rendered_result)
    return 0


if __name__ == "__main__":
    raise SystemExit(run_command())
