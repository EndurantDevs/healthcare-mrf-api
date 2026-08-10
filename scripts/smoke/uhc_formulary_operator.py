#!/usr/bin/env python
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Run one fixed, default-off UHC formulary operation."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass, field
import os
from pathlib import Path
import re
import signal
import sys
from typing import Any, Sequence


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.environ.setdefault("HLTHPRT_LOG_CFG", str(ROOT / "logging.yaml"))

COMMANDS = ("acquire-admit", "publish-admitted")
SIGNALS = (signal.SIGTERM, signal.SIGINT)
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
RECEIPT_PATTERN = re.compile(r"ffur_[0-9a-f]{48}\Z")
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
    """Reject invalid arguments without reflecting caller values."""

    def error(self, _message: str) -> None:
        """Exit with fixed JSON rather than argparse's argument echo."""

        self.exit(2, _error_json("invalid_arguments") + "\n")


def _exact_sha256(raw_value: str) -> str:
    if type(raw_value) is not str or not SHA256_PATTERN.fullmatch(raw_value):
        raise argparse.ArgumentTypeError("invalid")
    return raw_value


def _receipt_id(raw_value: str) -> str:
    if type(raw_value) is not str or not RECEIPT_PATTERN.fullmatch(raw_value):
        raise argparse.ArgumentTypeError("invalid")
    return raw_value


def _parser() -> argparse.ArgumentParser:
    parser = RedactedArgumentParser(
        allow_abbrev=False,
        description=(
            "Acquire and admit the exact UHC drug catalog, or publish one "
            "durable receipt. Operations are separately gated."
        ),
    )
    commands = parser.add_subparsers(dest="command", required=True)
    acquire = commands.add_parser("acquire-admit", allow_abbrev=False)
    acquire.add_argument(
        "--raw-set-sha256",
        required=True,
        type=_exact_sha256,
        help="Exact retained two-listing observation hash.",
    )
    publish = commands.add_parser("publish-admitted", allow_abbrev=False)
    publish.add_argument(
        "--receipt-id",
        required=True,
        type=_receipt_id,
        help="Exact durable UHC admission receipt identifier.",
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
        """Restore every signal handler replaced for one operation."""

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
        raise RuntimeError("UHC operator signal setup failed") from None
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
    selector: str,
    database: Any,
) -> str:
    if command == "acquire-admit":
        from process.formulary_fhir.uhc_drug_acquire_operation import (
            acquire_and_admit_uhc_drugs,
        )
        from process.formulary_fhir.uhc_drug_operation import (
            admission_result_json,
        )

        admitted = await acquire_and_admit_uhc_drugs(
            raw_set_sha256=selector,
            database=database,
        )
        return admission_result_json(admitted)
    if command == "publish-admitted":
        from process.formulary_fhir.uhc_drug_operation import (
            publication_result_json,
        )
        from process.formulary_fhir.uhc_drug_publish_operation import (
            publish_uhc_drug_receipt,
        )

        publication = await publish_uhc_drug_receipt(
            receipt_id=selector,
            database=database,
        )
        return publication_result_json(publication)
    raise RuntimeError("UHC operator command is invalid")


async def _run_operation(
    command: str,
    selector: str,
    *,
    database: Any | None = None,
) -> str:
    active_task = asyncio.current_task()
    if active_task is None:
        raise RuntimeError("UHC operator task is unavailable")
    signal_registration = _install_signal_handlers(active_task)
    runtime_database = database
    should_preserve_cancellation = False
    try:
        if runtime_database is None:
            from db.connection import db

            runtime_database = db
        return await _execute_operation(command, selector, runtime_database)
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
    """Run one UHC operation with deterministic JSON-only output."""

    parsed_arguments = _parser().parse_args(arguments)
    selector = (
        parsed_arguments.raw_set_sha256
        if parsed_arguments.command == "acquire-admit"
        else parsed_arguments.receipt_id
    )
    try:
        rendered_result = asyncio.run(
            _run_operation(parsed_arguments.command, selector)
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
