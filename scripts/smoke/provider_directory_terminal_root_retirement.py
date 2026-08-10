#!/usr/bin/env python
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Preview or apply one exact, default-off terminal root retirement."""

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

SIGNALS = (signal.SIGTERM, signal.SIGINT)
SAFE_ERROR_CODES = frozenset(
    {
        "busy",
        "canceled",
        "disabled",
        "evidence_changed",
        "evidence_invalid",
        "failed",
        "invalid_arguments",
        "request_invalid",
        "state_invalid",
        "timeout",
    }
)


def _error_json(code: object) -> str:
    safe_code = code if type(code) is str and code in SAFE_ERROR_CODES else "failed"
    return f'{{"code":"{safe_code}","status":"error"}}'


class RedactedArgumentParser(argparse.ArgumentParser):
    """Reject invalid arguments without reflecting private selectors."""

    def error(self, _message: str) -> None:
        """Exit with one fixed JSON error."""

        self.exit(2, _error_json("invalid_arguments") + "\n")


def _add_selectors(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--endpoint-id", required=True)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--acquisition-root-run-id", required=True)
    parser.add_argument("--owner-run-id", required=True)
    parser.add_argument("--expected-current-dataset-id", required=True)


def _parser() -> argparse.ArgumentParser:
    parser = RedactedArgumentParser(
        allow_abbrev=False,
        description=(
            "Preview a closed evidence token or apply its exact parent-only CAS."
        ),
    )
    commands = parser.add_subparsers(dest="command", required=True)
    preview_parser = commands.add_parser("preview", allow_abbrev=False)
    _add_selectors(preview_parser)
    apply_parser = commands.add_parser("apply", allow_abbrev=False)
    _add_selectors(apply_parser)
    apply_parser.add_argument("--expected-evidence-sha256", required=True)
    return parser


@dataclass(slots=True)
class _SignalRegistration:
    operation_loop: Any
    previous_handler_by_signal: dict[signal.Signals, Any] = field(
        default_factory=dict
    )
    is_restored: bool = False

    def restore(self) -> None:
        """Restore every handler replaced for this operation."""

        if self.is_restored:
            return
        for signal_number in reversed(tuple(self.previous_handler_by_signal)):
            self.operation_loop.remove_signal_handler(signal_number)
            signal.signal(
                signal_number,
                self.previous_handler_by_signal[signal_number],
            )
        self.is_restored = True


def _install_signal_handlers(active_task: asyncio.Task[Any]) -> _SignalRegistration:
    operation_loop = asyncio.get_running_loop()
    registration = _SignalRegistration(operation_loop)
    try:
        for signal_number in SIGNALS:
            previous_handler = signal.getsignal(signal_number)
            operation_loop.add_signal_handler(
                signal_number, active_task.cancel, signal_number
            )
            registration.previous_handler_by_signal[signal_number] = previous_handler
    except (NotImplementedError, RuntimeError, ValueError):
        registration.restore()
        raise RuntimeError("terminal retirement signal setup failed") from None
    return registration


async def _drain_disconnect(database: Any, preserve_cancellation: bool) -> None:
    disconnect_task = asyncio.create_task(database.disconnect())
    cancellation_error: asyncio.CancelledError | None = None
    while not disconnect_task.done():
        try:
            await asyncio.shield(disconnect_task)
        except asyncio.CancelledError as error:
            cancellation_error = cancellation_error or error
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


def _request(arguments: argparse.Namespace) -> Any:
    from process.provider_directory_terminal_root_retirement_contract import (
        TerminalRootRetirementRequest,
    )

    return TerminalRootRetirementRequest(
        source_id=arguments.source_id,
        endpoint_id=arguments.endpoint_id,
        dataset_id=arguments.dataset_id,
        acquisition_root_run_id=arguments.acquisition_root_run_id,
        owner_run_id=arguments.owner_run_id,
        expected_current_dataset_id=arguments.expected_current_dataset_id,
        expected_evidence_sha256=getattr(
            arguments, "expected_evidence_sha256", None
        ),
    )


async def _execute(arguments: argparse.Namespace, database: Any) -> str:
    from process.provider_directory_terminal_root_retirement_contract import (
        retirement_result_json,
    )
    from process.provider_directory_terminal_root_retirement_operator import (
        apply_terminal_root_retirement,
        preview_terminal_root_retirement,
        retirement_preview_json,
    )

    request = _request(arguments)
    if arguments.command == "preview":
        token = await preview_terminal_root_retirement(request, database=database)
        return retirement_preview_json(token)
    if arguments.command == "apply":
        result = await apply_terminal_root_retirement(request, database=database)
        return retirement_result_json(result)
    raise RuntimeError("terminal retirement command invalid")


async def _run_operator(
    arguments: argparse.Namespace,
    *,
    database: Any | None = None,
) -> str:
    active_task = asyncio.current_task()
    if active_task is None:
        raise RuntimeError("terminal retirement task unavailable")
    registration = _install_signal_handlers(active_task)
    runtime_database = database
    should_preserve_cancellation = False
    try:
        if runtime_database is None:
            from db.connection import db

            runtime_database = db
        return await _execute(arguments, runtime_database)
    except asyncio.CancelledError:
        should_preserve_cancellation = True
        raise
    finally:
        try:
            if runtime_database is not None:
                await _drain_disconnect(
                    runtime_database,
                    should_preserve_cancellation,
                )
        finally:
            registration.restore()


def _is_gate_enabled() -> bool:
    from process.provider_directory_terminal_root_retirement_contract import (
        RETIREMENT_ENABLED_ENV,
    )

    return os.getenv(RETIREMENT_ENABLED_ENV) == "true"


def _operation_error_code(error: Exception) -> str:
    code = getattr(error, "code", None)
    return code if type(code) is str and code in SAFE_ERROR_CODES else "failed"


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
    """Run a gated operation with selector-free JSON output."""

    if not _is_gate_enabled():
        _print_error("disabled")
        return 1
    parsed_arguments = _parser().parse_args(arguments)
    try:
        rendered_result = asyncio.run(_run_operator(parsed_arguments))
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
