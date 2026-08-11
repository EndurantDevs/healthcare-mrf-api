#!/usr/bin/env python
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Run one exact, default-off rooted Provider Directory graph phase."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass, field
import math
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

REGISTRATION_ENABLED_ENV = (
    "HLTHPRT_PROVIDER_DIRECTORY_ROOTED_GRAPH_REGISTRATION_ENABLED"
)
ACQUISITION_ENABLED_ENV = "HLTHPRT_PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_ENABLED"
PUBLICATION_ENABLED_ENV = "HLTHPRT_PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_ENABLED"
_GATE_BY_COMMAND = {
    "register": REGISTRATION_ENABLED_ENV,
    "acquire": ACQUISITION_ENABLED_ENV,
    "publish": PUBLICATION_ENABLED_ENV,
}
_SIGNALS = (signal.SIGTERM, signal.SIGINT)
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_ACQUISITION_PATTERN = re.compile(r"pdrga_[0-9a-f]{48}\Z")
_SAFE_ERROR_CODES = frozenset(
    {
        "acquisition",
        "admission",
        "both_current",
        "busy",
        "canceled",
        "content",
        "disabled",
        "drift",
        "evidence",
        "failed",
        "foreign_current",
        "gate_conflict",
        "identity",
        "input_drift",
        "invalid_arguments",
        "invalid_request",
        "mismatch",
        "missing",
        "publication",
        "registration",
        "replay",
        "root_unsealable",
        "source_drift",
        "stale",
        "state",
        "timeout",
    }
)


def _error_json(code: object) -> str:
    safe_code = code if type(code) is str and code in _SAFE_ERROR_CODES else "failed"
    return f'{{"code":"{safe_code}","status":"error"}}'


class RedactedArgumentParser(argparse.ArgumentParser):
    """Reject invalid selectors without reflecting caller-controlled values."""

    def error(self, _message: str) -> None:
        """Exit with one fixed JSON document for every invalid argument."""

        self.exit(2, _error_json("invalid_arguments") + "\n")


def _exact_sha256(raw_value: str) -> str:
    if type(raw_value) is not str or _SHA256_PATTERN.fullmatch(raw_value) is None:
        raise argparse.ArgumentTypeError("invalid")
    return raw_value


def _publication_acquisition_id(raw_value: str) -> str:
    if type(raw_value) is not str or _ACQUISITION_PATTERN.fullmatch(raw_value) is None:
        raise argparse.ArgumentTypeError("invalid")
    return raw_value


def _bounded_integer(raw_value: str, minimum: int, maximum: int) -> int:
    try:
        parsed = int(raw_value)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError("invalid") from None
    if str(parsed) != raw_value or not minimum <= parsed <= maximum:
        raise argparse.ArgumentTypeError("invalid")
    return parsed


def _bounded_float(raw_value: str, minimum: float, maximum: float) -> float:
    try:
        parsed = float(raw_value)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError("invalid") from None
    if (
        not math.isfinite(parsed)
        or not minimum <= parsed <= maximum
        or raw_value != raw_value.strip()
    ):
        raise argparse.ArgumentTypeError("invalid")
    return parsed


def _add_acquisition_arguments(commands: Any) -> None:
    """Add bounded controls and the required stable resume selector."""

    acquisition = commands.add_parser(
        "acquire",
        add_help=False,
        allow_abbrev=False,
    )
    acquisition.add_argument(
        "--operation-key",
        required=True,
        type=_exact_sha256,
    )
    acquisition.add_argument(
        "--concurrency",
        type=lambda value: _bounded_integer(value, 1, 16),
        default=4,
    )
    acquisition.add_argument(
        "--max-attempts",
        type=lambda value: _bounded_integer(value, 1, 8),
        default=3,
    )
    acquisition.add_argument(
        "--lease-seconds",
        type=lambda value: _bounded_integer(value, 60, 3600),
        default=300,
    )
    acquisition.add_argument(
        "--retry-base-seconds",
        type=lambda value: _bounded_float(value, 0.001, 60.0),
        default=1.0,
    )
    acquisition.add_argument(
        "--max-retry-seconds",
        type=lambda value: _bounded_float(value, 0.001, 60.0),
        default=60.0,
    )
    acquisition.add_argument(
        "--root-timeout-seconds",
        type=lambda value: _bounded_float(value, 1.0, 2_592_000.0),
        default=604_800.0,
    )


def _add_publication_arguments(commands: Any) -> None:
    """Add the sole exact admission selector and bounded batch control."""

    publication = commands.add_parser(
        "publish",
        add_help=False,
        allow_abbrev=False,
    )
    publication.add_argument(
        "--publication-acquisition-id",
        required=True,
        type=_publication_acquisition_id,
    )
    publication.add_argument(
        "--batch-size",
        type=lambda value: _bounded_integer(value, 1, 4096),
        default=4096,
    )


def _parser() -> argparse.ArgumentParser:
    """Build the fixed three-phase parser without a free-form help path."""

    parser = RedactedArgumentParser(
        add_help=False,
        allow_abbrev=False,
        description="Run one exact manual rooted graph phase.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "register",
        add_help=False,
        allow_abbrev=False,
    )
    _add_acquisition_arguments(commands)
    _add_publication_arguments(commands)
    return parser


def _raw_arguments(arguments: Sequence[str] | None) -> tuple[str, ...]:
    return tuple(sys.argv[1:] if arguments is None else arguments)


def _preflight_gate(raw_arguments: tuple[str, ...]) -> str | None:
    enabled_gates = {
        gate_name
        for gate_name in _GATE_BY_COMMAND.values()
        if os.getenv(gate_name, "") == "true"
    }
    if len(enabled_gates) > 1:
        return "gate_conflict"
    command = raw_arguments[0] if raw_arguments else None
    expected_gate = _GATE_BY_COMMAND.get(command)
    if expected_gate is None:
        return "disabled" if not enabled_gates else None
    return None if enabled_gates == {expected_gate} else "disabled"


@dataclass(slots=True)
class _SignalRegistration:
    operation_loop: Any
    previous_handler_by_signal: dict[signal.Signals, Any] = field(default_factory=dict)
    is_restored: bool = False

    def restore(self) -> None:
        """Restore every process handler replaced for the active phase."""

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
        for signal_number in _SIGNALS:
            previous_handler = signal.getsignal(signal_number)
            selected_loop.add_signal_handler(
                signal_number,
                active_task.cancel,
                signal_number,
            )
            registration.previous_handler_by_signal[signal_number] = previous_handler
    except (NotImplementedError, RuntimeError, ValueError):
        registration.restore()
        raise RuntimeError("rooted graph operator signal setup failed") from None
    return registration


async def _drain_disconnect(
    database: Any,
    *,
    is_preserving_cancellation: bool,
) -> None:
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
        if is_preserving_cancellation:
            return
        if cancellation_error is not None:
            raise cancellation_error
        raise
    if cancellation_error is not None and not is_preserving_cancellation:
        raise cancellation_error


async def _execute_selected_phase(
    arguments: argparse.Namespace,
    database: Any,
) -> str:
    if arguments.command == "register":
        from process.provider_directory_rooted_graph_operator import (
            register_rooted_graph_source_operation,
        )

        return await register_rooted_graph_source_operation(
            database=database,
        )
    if arguments.command == "acquire":
        from process.provider_directory_rooted_graph_operator import (
            acquire_admit_rooted_graph_operation,
        )

        return await acquire_admit_rooted_graph_operation(
            operation_key=arguments.operation_key,
            concurrency=arguments.concurrency,
            max_attempts=arguments.max_attempts,
            lease_seconds=arguments.lease_seconds,
            retry_base_seconds=arguments.retry_base_seconds,
            max_retry_seconds=arguments.max_retry_seconds,
            root_timeout_seconds=arguments.root_timeout_seconds,
            database=database,
        )
    if arguments.command == "publish":
        from process.provider_directory_rooted_graph_operator import (
            publish_admitted_rooted_graph_operation,
        )

        return await publish_admitted_rooted_graph_operation(
            publication_acquisition_id=(arguments.publication_acquisition_id),
            batch_size=arguments.batch_size,
            database=database,
        )
    raise RuntimeError("rooted graph operator command is invalid")


async def _run_operator(
    arguments: argparse.Namespace,
    *,
    database: Any | None = None,
) -> str:
    from process.provider_directory_rooted_graph_operator import (
        require_rooted_graph_operator_gate,
    )

    require_rooted_graph_operator_gate(arguments.command)
    active_task = asyncio.current_task()
    if active_task is None:
        raise RuntimeError("rooted graph operator task is unavailable")
    registration = _install_signal_handlers(active_task)
    runtime_database = database
    is_preserving_cancellation = False
    try:
        if runtime_database is None:
            from db.connection import db

            runtime_database = db
        return await _execute_selected_phase(arguments, runtime_database)
    except asyncio.CancelledError:
        is_preserving_cancellation = True
        raise
    finally:
        try:
            if runtime_database is not None:
                await _drain_disconnect(
                    runtime_database,
                    is_preserving_cancellation=is_preserving_cancellation,
                )
        finally:
            registration.restore()


def _operation_error_code(error: Exception) -> str:
    code = getattr(error, "code", None)
    return code if type(code) is str and code in _SAFE_ERROR_CODES else "failed"


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
    """Run one gated phase with canonical JSON-only output."""

    raw_arguments = _raw_arguments(arguments)
    gate_error = _preflight_gate(raw_arguments)
    if gate_error is not None:
        _print_error(gate_error)
        return 1
    parsed_arguments = _parser().parse_args(raw_arguments)
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
