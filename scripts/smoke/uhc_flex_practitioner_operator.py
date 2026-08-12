#!/usr/bin/env python
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Run one fixed, default-off exact-cohort Flex Practitioner operation."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
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

SIGNALS = (signal.SIGTERM, signal.SIGINT)
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
ACQUISITION_PATTERN = re.compile(r"pdufpa_[0-9a-f]{48}\Z")
SAFE_ERROR_CODES = frozenset(
    {
        "acquisition",
        "admission",
        "busy",
        "canceled",
        "cohort_drift",
        "content",
        "disabled",
        "evidence",
        "failed",
        "foreign_current",
        "gate_conflict",
        "identity",
        "invalid_arguments",
        "invalid_request",
        "mismatch",
        "missing",
        "progress",
        "publication",
        "replay",
        "root_unsealable",
        "source_drift",
        "state",
        "timeout",
    }
)


def _error_json(code: object) -> str:
    safe_code = code if type(code) is str and code in SAFE_ERROR_CODES else "failed"
    return f'{{"code":"{safe_code}","status":"error"}}'


class RedactedArgumentParser(argparse.ArgumentParser):
    """Reject invalid arguments without reflecting caller-controlled values."""

    def error(self, _message: str) -> None:
        """Exit with fixed JSON instead of reflecting rejected arguments."""

        self.exit(2, _error_json("invalid_arguments") + "\n")


def _exact_sha256(raw_value: str) -> str:
    if type(raw_value) is not str or SHA256_PATTERN.fullmatch(raw_value) is None:
        raise argparse.ArgumentTypeError("invalid")
    return raw_value


def _candidate_acquisition_id(raw_value: str) -> str:
    if type(raw_value) is not str or ACQUISITION_PATTERN.fullmatch(raw_value) is None:
        raise argparse.ArgumentTypeError("invalid")
    return raw_value


def _projection_date(raw_value: str) -> str:
    try:
        parsed_date = dt.date.fromisoformat(raw_value)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError("invalid") from None
    if parsed_date.isoformat() != raw_value:
        raise argparse.ArgumentTypeError("invalid")
    return raw_value


def _bounded_integer(raw_value: str, minimum: int, maximum: int) -> int:
    try:
        parsed_value = int(raw_value)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError("invalid") from None
    if str(parsed_value) != raw_value or not minimum <= parsed_value <= maximum:
        raise argparse.ArgumentTypeError("invalid")
    return parsed_value


def _concurrency(raw_value: str) -> int:
    return _bounded_integer(raw_value, 1, 16)


def _max_attempts(raw_value: str) -> int:
    return _bounded_integer(raw_value, 1, 8)


def _lease_seconds(raw_value: str) -> int:
    return _bounded_integer(raw_value, 30, 3600)


def _batch_size(raw_value: str) -> int:
    return _bounded_integer(raw_value, 1, 1000)


def _retry_seconds(raw_value: str) -> float:
    try:
        parsed_value = float(raw_value)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError("invalid") from None
    if (
        not math.isfinite(parsed_value)
        or not 0.0 < parsed_value <= 60.0
        or raw_value != raw_value.strip()
    ):
        raise argparse.ArgumentTypeError("invalid")
    return parsed_value


def _add_acquisition_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--operation-key",
        required=True,
        type=_exact_sha256,
        help="Stable exact campaign key; reuse it to resume the same roots.",
    )
    parser.add_argument(
        "--semantic-projection-as-of",
        required=True,
        type=_projection_date,
        metavar="YYYY-MM-DD",
        help="Legacy semantic projection date retained for argument validation.",
    )
    parser.add_argument("--concurrency", type=_concurrency, default=4)
    parser.add_argument("--max-attempts", type=_max_attempts, default=3)
    parser.add_argument("--lease-seconds", type=_lease_seconds, default=300)
    parser.add_argument(
        "--retry-base-seconds",
        type=_retry_seconds,
        default=1.0,
    )
    parser.add_argument(
        "--max-retry-seconds",
        type=_retry_seconds,
        default=60.0,
    )


def _parser() -> argparse.ArgumentParser:
    parser = RedactedArgumentParser(
        allow_abbrev=False,
        description=(
            "Seal an official Practitioner NPI cohort or publish one exact "
            "historical admission; legacy acquisition remains disabled."
        ),
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("sync-cohort", allow_abbrev=False)
    acquisition = commands.add_parser("acquire-admit", allow_abbrev=False)
    _add_acquisition_arguments(acquisition)
    single_root = commands.add_parser(
        "acquire-admit-single-root",
        allow_abbrev=False,
    )
    _add_acquisition_arguments(single_root)
    publication = commands.add_parser(
        "publish-admitted",
        allow_abbrev=False,
    )
    publication.add_argument(
        "--candidate-acquisition-id",
        required=True,
        type=_candidate_acquisition_id,
    )
    publication.add_argument("--batch-size", type=_batch_size, default=500)
    return parser


@dataclass(slots=True)
class _SignalRegistration:
    operation_loop: Any
    previous_handler_by_signal: dict[signal.Signals, Any] = field(default_factory=dict)
    is_restored: bool = False

    def restore(self) -> None:
        """Restore every process signal handler replaced for this phase."""

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
            registration.previous_handler_by_signal[signal_number] = previous_handler
    except (NotImplementedError, RuntimeError, ValueError):
        registration.restore()
        raise RuntimeError("Flex Practitioner operator signal setup failed") from None
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
    parsed_arguments: argparse.Namespace, database: Any
) -> str:
    if parsed_arguments.command == "sync-cohort":
        from process.uhc_flex_practitioner_operator import (
            sync_uhc_flex_practitioner_cohort_operation,
        )

        return await sync_uhc_flex_practitioner_cohort_operation(
            database=database,
        )
    if parsed_arguments.command == "acquire-admit":
        from process.uhc_flex_practitioner_operator import (
            acquire_admit_uhc_flex_practitioner_operation,
        )

        return await acquire_admit_uhc_flex_practitioner_operation(
            operation_key=parsed_arguments.operation_key,
            semantic_projection_as_of=parsed_arguments.semantic_projection_as_of,
            concurrency=parsed_arguments.concurrency,
            max_attempts=parsed_arguments.max_attempts,
            lease_seconds=parsed_arguments.lease_seconds,
            retry_base_seconds=parsed_arguments.retry_base_seconds,
            max_retry_seconds=parsed_arguments.max_retry_seconds,
            database=database,
        )
    if parsed_arguments.command == "acquire-admit-single-root":
        from process.uhc_flex_practitioner_operator import (
            acquire_uhc_flex_single_root_operation,
        )

        return await acquire_uhc_flex_single_root_operation(
            operation_key=parsed_arguments.operation_key,
            semantic_projection_as_of=parsed_arguments.semantic_projection_as_of,
            concurrency=parsed_arguments.concurrency,
            max_attempts=parsed_arguments.max_attempts,
            lease_seconds=parsed_arguments.lease_seconds,
            retry_base_seconds=parsed_arguments.retry_base_seconds,
            max_retry_seconds=parsed_arguments.max_retry_seconds,
            database=database,
        )
    if parsed_arguments.command == "publish-admitted":
        from process.uhc_flex_practitioner_operator import (
            publish_admitted_uhc_flex_practitioner_operation,
        )

        return await publish_admitted_uhc_flex_practitioner_operation(
            candidate_acquisition_id=(parsed_arguments.candidate_acquisition_id),
            batch_size=parsed_arguments.batch_size,
            database=database,
        )
    raise RuntimeError("Flex Practitioner operator command is invalid")


async def _run_operation(
    parsed_arguments: argparse.Namespace,
    *,
    database: Any | None = None,
) -> str:
    active_task = asyncio.current_task()
    if active_task is None:
        raise RuntimeError("Flex Practitioner operator task is unavailable")
    from process.uhc_flex_practitioner_operator import (
        require_uhc_flex_practitioner_operator_gate,
    )

    require_uhc_flex_practitioner_operator_gate(parsed_arguments.command)
    signal_registration = _install_signal_handlers(active_task)
    runtime_database = database
    should_preserve_cancellation = False
    try:
        if runtime_database is None:
            from db.connection import db

            runtime_database = db
        return await _execute_operation(parsed_arguments, runtime_database)
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
    """Run one gated phase with deterministic JSON-only output."""

    parsed_arguments = _parser().parse_args(arguments)
    if parsed_arguments.command == "acquire-admit":
        _print_error("disabled")
        return 1
    try:
        rendered_result = asyncio.run(_run_operation(parsed_arguments))
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
