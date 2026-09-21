# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded stdin validation and caller-composed registration for custom imports."""

from __future__ import annotations

import argparse
import json
import sys
from typing import TYPE_CHECKING, Any, Sequence

from process.custom_import.definition import MAX_DEFINITION_BYTES, CustomImportDefinition

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from process.custom_import.definition_store import RegisteredDefinition


_SAFE_ERROR_CODES = frozenset({"canceled", "failed", "invalid_arguments", "invalid_definition"})


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
    return parser


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
    """Run the standalone validation command with redacted JSON failures."""

    parsed = _parser().parse_args(arguments)
    try:
        rendered = validation_receipt(load_definition_from_stdin(parsed.format, stream=stream))
    except KeyboardInterrupt:
        print(_error_json("canceled"), file=sys.stderr)
        return 130
    except OSError, TypeError, ValueError:
        print(_error_json("invalid_definition"), file=sys.stderr)
        return 1
    except Exception:
        print(_error_json("failed"), file=sys.stderr)
        return 1
    print(rendered)
    return 0
