#!/usr/bin/env python
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Run one fixed, default-off reviewed formulary operation."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
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

COMMANDS = ("acquire-twins", "publish-admitted")
SIGNALS = (signal.SIGTERM, signal.SIGINT)
SAFE_ERROR_CODES = frozenset(
    {
        "acquisition",
        "busy",
        "canceled",
        "disabled",
        "evidence",
        "failed",
        "gate_conflict",
        "invalid_arguments",
        "invalid_request",
        "mismatch",
        "missing",
        "publication",
        "timeout",
    }
)


def _error_json(code: object) -> str:
    safe_code = (
        code if type(code) is str and code in SAFE_ERROR_CODES else "failed"
    )
    return f'{{"code":"{safe_code}","status":"error"}}'


class RedactedArgumentParser(argparse.ArgumentParser):
    """Reject invalid arguments without echoing their values."""

    def error(self, _message: str) -> None:
        """Exit with fixed JSON without reflecting the rejected arguments."""

        self.exit(2, _error_json("invalid_arguments") + "\n")


def _utc_cutoff(cutoff_text: str) -> dt.datetime:
    try:
        if type(cutoff_text) is not str or not cutoff_text.endswith("Z"):
            raise ValueError("non-UTC cutoff")
        cutoff_at = dt.datetime.fromisoformat(cutoff_text[:-1] + "+00:00")
        canonical_text = cutoff_at.isoformat().replace("+00:00", "Z")
        if cutoff_at.tzinfo is None or canonical_text != cutoff_text:
            raise ValueError("non-canonical cutoff")
        return cutoff_at
    except (OverflowError, TypeError, ValueError):
        raise argparse.ArgumentTypeError("invalid") from None


def _parser() -> argparse.ArgumentParser:
    parser = RedactedArgumentParser(
        allow_abbrev=False,
        description=(
            "Acquire and admit the fixed reviewed formulary twins, or publish "
            "the exact admitted candidate. Operations are separately gated."
        ),
    )
    parser.add_argument("command", choices=COMMANDS)
    parser.add_argument(
        "--cutoff",
        required=True,
        type=_utc_cutoff,
        metavar="YYYY-MM-DDTHH:MM:SS[.ffffff]Z",
        help="Canonical RFC3339 UTC acquisition cutoff ending in Z.",
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
        raise RuntimeError("reviewed operator signal setup failed") from None
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


async def _execute_operation(
    command: str,
    cutoff_at: dt.datetime,
    database: Any,
) -> str:
    if command == "acquire-twins":
        from process.formulary_fhir.reviewed_acquisition import (
            acquire_reviewed_twins,
        )
        from process.formulary_fhir.reviewed_acquisition import (
            acquisition_result_json,
        )

        acquisition = await acquire_reviewed_twins(
            cutoff=cutoff_at,
            database=database,
        )
        return acquisition_result_json(acquisition)
    if command == "publish-admitted":
        from process.formulary_fhir.reviewed_publication import (
            publication_result_json,
        )
        from process.formulary_fhir.reviewed_publication import (
            publish_reviewed_candidate,
        )

        publication = await publish_reviewed_candidate(
            cutoff=cutoff_at,
            database=database,
        )
        return publication_result_json(publication)
    raise RuntimeError("reviewed operator command is invalid")


async def _run_reviewed_operation(
    command: str,
    cutoff_at: dt.datetime,
    *,
    database: Any | None = None,
) -> str:
    active_task = asyncio.current_task()
    if active_task is None:
        raise RuntimeError("reviewed operator task is unavailable")
    signal_registration = _install_signal_handlers(active_task)
    runtime_database = database
    should_preserve_cancellation = False
    try:
        if runtime_database is None:
            from db.connection import db

            runtime_database = db
        return await _execute_operation(command, cutoff_at, runtime_database)
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
    """Run one reviewed operation with deterministic JSON-only output."""

    parsed_arguments = _parser().parse_args(arguments)
    try:
        rendered_result = asyncio.run(
            _run_reviewed_operation(
                parsed_arguments.command,
                parsed_arguments.cutoff,
            )
        )
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
