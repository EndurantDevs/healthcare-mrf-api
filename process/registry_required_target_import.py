# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain a digest-verified required-network CSV without changing serving data."""

import asyncio
import json
import sys
from pathlib import Path
from uuid import UUID

from db.connection import Database
from process.custom_import.cli import _receipt_only_database_output, _set_engine_echo
from process.registry_required_target_store import (
    _DESCRIPTOR_FIELDS,
    RegistryRequiredTargetEdition,
    admit_registry_required_targets,
)
from process.registry_source_import import (
    _disconnect_database,
    _error_json,
    _InputError,
    _read_input,
    _ReceiptArgumentParser,
)


def _parser():
    parser = _ReceiptArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--input-file", type=Path, required=True)
    parser.add_argument("--snapshot-id", type=UUID, required=True)
    parser.add_argument("--source-url", required=True)
    parser.add_argument("--input-sha256", required=True)
    return parser


async def _run_import(input_bytes, edition):
    database = Database()
    with _receipt_only_database_output(database):
        try:
            await database.connect()
            _set_engine_echo(database, False)
            async with database.acquire_driver() as connection, connection.transaction():
                admission = await admit_registry_required_targets(connection, input_bytes, edition)
                fields = _DESCRIPTOR_FIELDS | {"copy_sha256", "copy_bytes", "replayed"}
                receipt = json.dumps(
                    {"status": "ok", **{field: admission[field] for field in fields}},
                    sort_keys=True,
                    separators=(",", ":"),
                )
                if len(receipt.encode()) > 4096:
                    raise ValueError("registry_required_target_receipt_limit")
            return receipt
        finally:
            await _disconnect_database(database)


def run_command(arguments=None):
    """Emit one aggregate receipt; source cells and connection details stay private."""
    parsed = _parser().parse_args(arguments)
    try:
        edition = RegistryRequiredTargetEdition(parsed.snapshot_id, parsed.source_url, parsed.input_sha256)
        input_bytes = _read_input(parsed.input_file, edition.input_sha256)
        if not input_bytes:
            raise _InputError("input_empty")
    except _InputError as error:
        print(_error_json(str(error)), file=sys.stderr)
        return 1
    except OSError:
        print(_error_json("input_unavailable"), file=sys.stderr)
        return 1
    except ValueError:
        print(_error_json("invalid_arguments"), file=sys.stderr)
        return 2
    try:
        receipt = asyncio.run(_run_import(input_bytes, edition))
    except KeyboardInterrupt, asyncio.CancelledError:
        print(_error_json("canceled"), file=sys.stderr)
        return 130
    except Exception:
        print(_error_json("admission_failed"), file=sys.stderr)
        return 1
    print(receipt)
    return 0


if __name__ == "__main__":
    raise SystemExit(run_command())
