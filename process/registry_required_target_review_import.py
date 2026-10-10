# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain an explicitly reviewed target edition without approving or publishing it."""

import asyncio
import json
import re
import sys

from db.connection import Database
from process.custom_import.cli import _receipt_only_database_output, _set_engine_echo
from process.registry_required_target_cli_preflight import review_parser
from process.registry_required_target_review_store import (
    _DESCRIPTOR_FIELDS,
    EVIDENCE_PREFIX,
    PARSER_VERSION,
    admit_registry_required_target_review,
)
from process.registry_required_target_store import MAX_COPY_BYTES, RegistryRequiredTargetEdition
from process.registry_source_import import (
    _disconnect_database,
    _error_json,
    _InputError,
    _read_input,
)


def _parser():
    return review_parser(__doc__)


def _receipt_json(edition, ledger_snapshot_id, admission):
    """Project and validate aggregate metadata before it can reach operator output."""
    field_names = _DESCRIPTOR_FIELDS | {"evidence_id", "copy_sha256", "copy_bytes", "replayed"}
    try:
        receipt_by_field = {field: admission[field] for field in field_names}
        if (
            receipt_by_field["component"] != "registry_required_target_review"
            or type(receipt_by_field["revision"]) is not int
            or receipt_by_field["revision"] != 1
            or receipt_by_field["parser_version"] != PARSER_VERSION
            or receipt_by_field["snapshot_id"] != str(edition.snapshot_id)
            or receipt_by_field["ledger_snapshot_id"] != str(ledger_snapshot_id)
            or receipt_by_field["source_sha256"] != edition.input_sha256
            or receipt_by_field["evidence_id"] != EVIDENCE_PREFIX + str(edition.snapshot_id)
            or type(receipt_by_field["physical_records"]) is not int
            or receipt_by_field["physical_records"] != 1
            or type(receipt_by_field["decision_count"]) is not int
            or not 1 <= receipt_by_field["decision_count"] <= 5000
            or type(receipt_by_field["resolved_count"]) is not int
            or not 0 <= receipt_by_field["resolved_count"] <= receipt_by_field["decision_count"]
            or type(receipt_by_field["copy_bytes"]) is not int
            or not 21 <= receipt_by_field["copy_bytes"] <= MAX_COPY_BYTES
            or type(receipt_by_field["replayed"]) is not bool
            or any(
                type(receipt_by_field[field]) is not str
                or re.fullmatch(r"[0-9a-f]{64}", receipt_by_field[field]) is None
                for field in ("artifact_sha256", "ledger_artifact_sha256", "copy_sha256")
            )
        ):
            raise ValueError
        encoded = json.dumps({"status": "ok", **receipt_by_field}, sort_keys=True, separators=(",", ":"))
    except KeyError, TypeError, ValueError:
        raise ValueError("registry_required_target_review_receipt_invalid") from None
    if len(encoded.encode()) > 4096:
        raise ValueError("registry_required_target_review_receipt_limit")
    return encoded


async def _run_import(input_bytes, edition, ledger_snapshot_id):
    database = Database()
    with _receipt_only_database_output(database):
        try:
            await database.connect()
            _set_engine_echo(database, False)
            async with database.acquire_driver() as connection, connection.transaction():
                admission = await admit_registry_required_target_review(
                    connection, input_bytes, edition, ledger_snapshot_id
                )
                receipt = _receipt_json(edition, ledger_snapshot_id, admission)
            return receipt
        finally:
            await _disconnect_database(database)


def run_command(arguments=None):
    """Emit one bounded receipt; source decisions and connection details stay private."""
    parsed = _parser().parse_args(arguments)
    try:
        edition = RegistryRequiredTargetEdition(parsed.snapshot_id, parsed.source_url, parsed.input_sha256)
        if not parsed.ledger_snapshot_id.int:
            raise ValueError
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
        receipt = asyncio.run(_run_import(input_bytes, edition, parsed.ledger_snapshot_id))
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
