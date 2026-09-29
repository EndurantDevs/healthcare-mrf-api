# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Redacted validation and lifecycle commands for generic custom imports."""

from __future__ import annotations

import argparse
import asyncio
from contextlib import contextmanager, redirect_stderr, redirect_stdout
import json
import logging
import os
import re
import sys
from typing import TYPE_CHECKING, Any, Sequence

from db.connection import db
from process.custom_import.definition import MAX_DEFINITION_BYTES, CustomImportDefinition
from process.custom_import.execution import (
    ExecutionNotFound,
    ExecutionTransition,
    request_cancellation,
)
from process.custom_import.operator import (
    CurrentGenerationStatus,
    ExecutionEvidenceExecution,
    ExecutionEvidenceStatus,
    ExecutionStatus,
    GenerationStatus,
    OperatorObjectNotFound,
    inspect_execution,
    inspect_execution_evidence,
    inspect_generation,
)
from process.custom_import.publication import (
    PublicationConflict,
    PublicationReceipt,
    activate_generation,
    rollback_generation,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from process.custom_import.definition_store import RegisteredDefinition


_SAFE_ERROR_CODES = frozenset(
    {"canceled", "conflict", "failed", "invalid_arguments", "invalid_definition", "not_found"}
)
_MAX_BIGINT = 2**63 - 1
_MISSING = object()


class _DefinitionInputError(ValueError):
    """Stdin cannot provide one bounded definition document."""


class _RedactedArgumentParser(argparse.ArgumentParser):
    def error(self, _message: str) -> None:
        """Emit only the fixed argument failure receipt."""

        self.exit(2, _error_json("invalid_arguments") + "\n")


def _error_json(code: str) -> str:
    return json.dumps(
        {"code": code if code in _SAFE_ERROR_CODES else "failed", "status": "error"},
        separators=(",", ":"),
        sort_keys=True,
    )


def _parser() -> argparse.ArgumentParser:
    parser = _RedactedArgumentParser(allow_abbrev=False)
    commands = parser.add_subparsers(dest="command", required=True, parser_class=_RedactedArgumentParser)
    validate = commands.add_parser("validate", allow_abbrev=False)
    validate.add_argument("--format", choices=("json", "yaml"), required=True)

    status = commands.add_parser("status", allow_abbrev=False, help="read one execution or generation")
    status.add_argument("--dataset-id", required=True, type=_positive_identifier)
    status_identity = status.add_mutually_exclusive_group(required=True)
    status_identity.add_argument("--execution-id", type=_positive_identifier)
    status_identity.add_argument("--generation-id", type=_positive_identifier)

    captures = commands.add_parser("captures", allow_abbrev=False, help="inspect one execution's retained capture")
    captures.add_argument("--dataset-id", required=True, type=_positive_identifier)
    captures.add_argument("--execution-id", required=True, type=_positive_identifier)

    cancel = commands.add_parser("cancel", allow_abbrev=False, help="request cancellation for one execution")
    cancel.add_argument("--execution-id", required=True, type=_positive_identifier)

    for command in ("activate", "rollback"):
        publication = commands.add_parser(
            command,
            allow_abbrev=False,
            help="activate or roll back one generation with a pointer precondition",
        )
        publication.add_argument("--dataset-id", required=True, type=_positive_identifier)
        publication.add_argument("--target-generation-id", required=True, type=_positive_identifier)
        publication.add_argument("--expected-pointer-version", required=True, type=_pointer_version)
        publication.add_argument(
            "--expected-generation-id",
            required=command == "rollback",
            type=_positive_identifier,
        )
    return parser


def _positive_identifier(value: str) -> int:
    if not isinstance(value, str) or re.fullmatch(r"[1-9][0-9]{0,18}", value, flags=re.ASCII) is None:
        raise argparse.ArgumentTypeError("invalid")
    parsed = int(value)
    if parsed > _MAX_BIGINT:
        raise argparse.ArgumentTypeError("invalid")
    return parsed


def _pointer_version(value: str) -> int:
    if not isinstance(value, str) or re.fullmatch(r"0|[1-9][0-9]{0,18}", value, flags=re.ASCII) is None:
        raise argparse.ArgumentTypeError("invalid")
    parsed = int(value)
    if parsed > _MAX_BIGINT:
        raise argparse.ArgumentTypeError("invalid")
    return parsed


def _parse_arguments(arguments: Sequence[str] | None) -> argparse.Namespace:
    """Parse fixed CLI arguments without reflecting invalid input."""

    parser = _parser()
    parsed = parser.parse_args(arguments)
    if (
        parsed.command == "activate"
        and parsed.expected_generation_id is None
        and parsed.expected_pointer_version != 0
    ):
        parser.error("invalid")
    return parsed


def _read_stdin(stream: Any | None = None) -> bytes:
    selected = stream if stream is not None else getattr(sys.stdin, "buffer", sys.stdin)
    isatty = getattr(selected, "isatty", None)
    if callable(isatty) and isatty():
        raise _DefinitionInputError("stdin is unavailable")
    payload = selected.read(MAX_DEFINITION_BYTES + 1)
    if isinstance(payload, str):
        try:
            payload = payload.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise _DefinitionInputError("stdin is invalid") from exc
    if not isinstance(payload, bytes) or len(payload) > MAX_DEFINITION_BYTES:
        raise _DefinitionInputError("stdin is invalid")
    return payload


def load_definition_from_stdin(definition_format: str, *, stream: Any | None = None) -> CustomImportDefinition:
    """Parse exactly one bounded JSON or YAML definition from stdin."""

    payload = _read_stdin(stream)
    if definition_format == "json":
        return CustomImportDefinition.from_json(payload)
    if definition_format == "yaml":
        return CustomImportDefinition.from_yaml(payload)
    raise _DefinitionInputError("definition format is invalid")


def validation_receipt(definition: CustomImportDefinition) -> str:
    """Return a stable receipt that excludes the supplied definition body."""

    return json.dumps(
        {
            "definition_digest": definition.digest,
            "definition_revision": definition.definition_revision,
            "schema_digest": definition.schema_digest,
            "schema_revision": definition.schema_revision,
            "status": "valid",
        },
        separators=(",", ":"),
        sort_keys=True,
    )


def _receipt_json(receipt: dict[str, object]) -> str:
    return json.dumps(receipt, separators=(",", ":"), sort_keys=True)


def _execution_status_receipt(status: ExecutionStatus, *, dataset_id: int, execution_id: int) -> str:
    if not isinstance(status, ExecutionStatus) or (status.dataset_id, status.execution_id) != (
        dataset_id,
        execution_id,
    ):
        raise ValueError("operator status is invalid")
    receipt_dict: dict[str, object] = {
        "command": "status",
        "dataset_id": status.dataset_id,
        "definition_revision_id": status.definition_revision_id,
        "execution_id": status.execution_id,
        "mechanism": status.mechanism,
        "resource": "execution",
        "schema_revision_id": status.schema_revision_id,
        "state": status.state,
        "status": "ok",
    }
    if status.capture_bundle_id is not None:
        receipt_dict["capture_bundle_id"] = status.capture_bundle_id
    if status.lease is not None:
        receipt_dict["lease_fence"] = status.lease.fence
    return _receipt_json(receipt_dict)


def _generation_status_receipt(status: GenerationStatus, *, dataset_id: int, generation_id: int) -> str:
    if not isinstance(status, GenerationStatus) or (status.dataset_id, status.generation_id) != (
        dataset_id,
        generation_id,
    ):
        raise ValueError("operator status is invalid")
    receipt_dict: dict[str, object] = {
        "capture_bundle_id": status.capture_bundle_id,
        "command": "status",
        "dataset_id": status.dataset_id,
        "definition_revision_id": status.definition_revision_id,
        "ever_published": status.ever_published,
        "execution_id": status.execution_id,
        "generation_id": status.generation_id,
        "publication_state": status.publication_state,
        "resource": "generation",
        "schema_revision_id": status.schema_revision_id,
        "status": "ok",
    }
    if status.base_generation_id is not None:
        receipt_dict["base_generation_id"] = status.base_generation_id
    if status.current is not None:
        if not isinstance(status.current, CurrentGenerationStatus):
            raise ValueError("operator current status is invalid")
        receipt_dict["current_generation_id"] = status.current.generation_id
        receipt_dict["current_pointer_version"] = status.current.pointer_version
    return _receipt_json(receipt_dict)


def _capture_receipt(evidence: ExecutionEvidenceStatus, *, dataset_id: int, execution_id: int) -> str:
    """Render only checked capture identity and the retained manifest digest."""

    if not isinstance(evidence, ExecutionEvidenceStatus):
        raise ValueError("operator evidence is invalid")
    execution = evidence.execution
    if not isinstance(execution, ExecutionEvidenceExecution) or (execution.dataset_id, execution.execution_id) != (
        dataset_id,
        execution_id,
    ):
        raise ValueError("operator evidence is invalid")
    return _receipt_json(
        {
            "capture": (
                {"bundle_id": execution.capture_bundle_id, "manifest_sha256": evidence.capture_manifest_sha256}
                if execution.capture_bundle_id is not None
                else None
            ),
            "command": "captures",
            "dataset_id": execution.dataset_id,
            "definition_revision_id": execution.definition_revision_id,
            "execution_id": execution.execution_id,
            "schema_revision_id": execution.schema_revision_id,
            "state": execution.state,
            "status": "ok",
        }
    )


def _transition_receipt(command: str, *, execution_id: int, transition: ExecutionTransition) -> str:
    if not isinstance(transition, ExecutionTransition) or transition.execution_id != execution_id:
        raise ValueError("execution transition is invalid")
    return _receipt_json(
        {
            "changed": transition.changed,
            "command": command,
            "execution_id": execution_id,
            "state": transition.state,
            "status": "ok",
        }
    )


def _publication_receipt(
    command: str,
    *,
    dataset_id: int,
    target_generation_id: int,
    expected_pointer_version: int,
    receipt: PublicationReceipt,
) -> str:
    expected_event_kind = "activated" if command == "activate" else "rolled_back"
    if (
        not isinstance(receipt, PublicationReceipt)
        or receipt.event_kind != expected_event_kind
        or receipt.dataset_id != dataset_id
        or receipt.to_generation_id != target_generation_id
        or receipt.expected_pointer_version != expected_pointer_version
    ):
        raise ValueError("publication receipt is invalid")
    return _receipt_json(
        {
            "command": command,
            "committed_pointer_version": receipt.committed_pointer_version,
            "dataset_id": dataset_id,
            "definition_revision_id": receipt.definition_revision_id,
            "execution_id": receipt.execution_id,
            "expected_pointer_version": expected_pointer_version,
            "from_generation_id": receipt.from_generation_id,
            "publication_event_id": receipt.publication_event_id,
            "replayed": receipt.replayed,
            "schema_revision_id": receipt.schema_revision_id,
            "status": "ok",
            "to_generation_id": target_generation_id,
        }
    )


def _set_engine_echo(database: Any, value: bool) -> None:
    engine = getattr(database, "engine", None)
    if engine is not None and hasattr(engine, "echo"):
        engine.echo = value


def _logging_handler_snapshot() -> dict[logging.Logger, tuple[logging.Handler, ...]]:
    loggers = (logging.getLogger(), *(
        logger for logger in logging.Logger.manager.loggerDict.values() if isinstance(logger, logging.Logger)
    ))
    return {logger: tuple(logger.handlers) for logger in loggers}


def _remove_added_handlers(handler_snapshot: dict[logging.Logger, tuple[logging.Handler, ...]]) -> None:
    loggers = (logging.getLogger(), *(
        logger for logger in logging.Logger.manager.loggerDict.values() if isinstance(logger, logging.Logger)
    ))
    for logger in loggers:
        previous_handlers = handler_snapshot.get(logger, ())
        for handler in tuple(logger.handlers):
            if all(handler is not previous_handler for previous_handler in previous_handlers):
                logger.removeHandler(handler)
                handler.close()


@contextmanager
def _receipt_only_database_output(database: Any):
    """Suppress database output only while this receipt CLI touches it."""

    engine = getattr(database, "engine", None)
    previous_echo = getattr(engine, "echo", _MISSING)
    previous_disable = logging.root.manager.disable
    handler_snapshot = _logging_handler_snapshot()
    try:
        logging.disable(logging.CRITICAL)
        with open(os.devnull, "w", encoding="utf-8") as discarded:
            with redirect_stdout(discarded), redirect_stderr(discarded):
                yield
    finally:
        try:
            if previous_echo is not _MISSING:
                engine.echo = previous_echo
            _remove_added_handlers(handler_snapshot)
        finally:
            logging.disable(previous_disable)


async def _status_receipt(session: Any, parsed: argparse.Namespace) -> str:
    if parsed.execution_id is not None:
        status = await inspect_execution(
            session,
            dataset_id=parsed.dataset_id,
            execution_id=parsed.execution_id,
        )
        return _execution_status_receipt(status, dataset_id=parsed.dataset_id, execution_id=parsed.execution_id)
    status = await inspect_generation(session, dataset_id=parsed.dataset_id, generation_id=parsed.generation_id)
    return _generation_status_receipt(status, dataset_id=parsed.dataset_id, generation_id=parsed.generation_id)


async def _publication_command(session: Any, parsed: argparse.Namespace) -> str:
    operation = activate_generation if parsed.command == "activate" else rollback_generation
    receipt = await operation(
        session,
        dataset_id=parsed.dataset_id,
        target_generation_id=parsed.target_generation_id,
        expected_generation_id=parsed.expected_generation_id,
        expected_pointer_version=parsed.expected_pointer_version,
    )
    return _publication_receipt(
        parsed.command,
        dataset_id=parsed.dataset_id,
        target_generation_id=parsed.target_generation_id,
        expected_pointer_version=parsed.expected_pointer_version,
        receipt=receipt,
    )


async def _lifecycle_receipt(session: Any, parsed: argparse.Namespace) -> str:
    if parsed.command == "status":
        return await _status_receipt(session, parsed)
    if parsed.command == "captures":
        evidence = await inspect_execution_evidence(
            session, dataset_id=parsed.dataset_id, execution_id=parsed.execution_id
        )
        return _capture_receipt(evidence, dataset_id=parsed.dataset_id, execution_id=parsed.execution_id)
    if parsed.command == "cancel":
        transition = await request_cancellation(session, execution_id=parsed.execution_id)
        return _transition_receipt("cancel", execution_id=parsed.execution_id, transition=transition)
    if parsed.command in {"activate", "rollback"}:
        return await _publication_command(session, parsed)
    raise ValueError("lifecycle command is invalid")


async def _run_lifecycle_command(parsed: argparse.Namespace, *, database: Any | None = None) -> str:
    """Run exactly one lifecycle operation in a fresh caller-owned transaction."""

    database = db if database is None else database
    with _receipt_only_database_output(database):
        engine = None
        previous_echo = _MISSING
        has_body_failure = False
        try:
            await database.connect()
            engine = getattr(database, "engine", None)
            previous_echo = getattr(engine, "echo", _MISSING)
            _set_engine_echo(database, False)
            async with database.transaction() as session:
                return await _lifecycle_receipt(session, parsed)
        except BaseException:
            has_body_failure = True
            raise
        finally:
            if previous_echo is not _MISSING:
                engine.echo = previous_echo
            try:
                await database.disconnect()
            except Exception:
                if not has_body_failure:
                    raise


async def register_definition_from_stdin(
    session: AsyncSession,
    *,
    dataset_key: str,
    definition_format: str,
    stream: Any | None = None,
) -> RegisteredDefinition:
    """Register a parsed definition through the caller-owned transaction."""

    from process.custom_import.definition_store import register_definition

    return await register_definition(session, dataset_key, load_definition_from_stdin(definition_format, stream=stream))


def run_command(arguments: Sequence[str] | None = None, *, stream: Any | None = None) -> int:
    """Run one standalone command while emitting only compact safe receipts."""

    parsed = _parse_arguments(arguments)
    try:
        if parsed.command == "validate":
            rendered = validation_receipt(load_definition_from_stdin(parsed.format, stream=stream))
        else:
            rendered = asyncio.run(_run_lifecycle_command(parsed))
    except KeyboardInterrupt:
        print(_error_json("canceled"), file=sys.stderr)
        return 130
    except (OperatorObjectNotFound, ExecutionNotFound):
        print(_error_json("not_found"), file=sys.stderr)
        return 1
    except PublicationConflict:
        print(_error_json("conflict"), file=sys.stderr)
        return 1
    except (OSError, TypeError, ValueError):
        print(_error_json("invalid_definition" if parsed.command == "validate" else "failed"), file=sys.stderr)
        return 1
    except Exception:
        print(_error_json("failed"), file=sys.stderr)
        return 1
    print(rendered)
    return 0
