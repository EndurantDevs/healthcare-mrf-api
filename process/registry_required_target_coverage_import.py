# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read complete target coverage from one retained ledger and serving generation."""

import asyncio
import json
import sys

from db.connection import Database
from process.custom_import.cli import _receipt_only_database_output, _set_engine_echo
from process.registry_required_target_cli_preflight import coverage_parser
from process.registry_required_target_coverage import read_registry_required_target_coverage
from process.registry_source_import import _disconnect_database, _error_json

MAX_REPORT_BYTES = 16 * 1024 * 1024
_REPORT_FIELDS = {"component", "revision", "targets", "totals", "provenance", "assessment"}


def _parser():
    return coverage_parser(__doc__)


def _report_json(report):
    if (
        type(report) is not dict
        or set(report) != _REPORT_FIELDS
        or report["component"] != "registry_required_target_coverage"
        or type(report["revision"]) is not int
        or report["revision"] != 1
        or type(report["targets"]) is not list
        or any(type(report[field]) is not dict for field in ("totals", "provenance", "assessment"))
    ):
        raise ValueError("registry_required_target_coverage_report_invalid")
    encoded = json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if len(encoded.encode()) > MAX_REPORT_BYTES:
        raise ValueError("registry_required_target_coverage_report_limit")
    return encoded


async def _run_report(ledger_snapshot_id, generation_id):
    database = Database()
    with _receipt_only_database_output(database):
        try:
            await database.connect()
            _set_engine_echo(database, False)
            async with (
                database.acquire_driver() as connection,
                connection.transaction(isolation="repeatable_read", readonly=True),
            ):
                report = await read_registry_required_target_coverage(
                    connection, ledger_snapshot_id, generation_id=generation_id
                )
                encoded = _report_json(report)
            return encoded
        finally:
            await _disconnect_database(database)


def run_command(arguments=None):
    """Emit the bounded report or a fixed error without database diagnostics."""
    parsed = _parser().parse_args(arguments)
    if not parsed.ledger_snapshot_id.int:
        print(_error_json("invalid_arguments"), file=sys.stderr)
        return 2
    try:
        report = asyncio.run(_run_report(parsed.ledger_snapshot_id, parsed.generation_id))
    except KeyboardInterrupt, asyncio.CancelledError:
        print(_error_json("canceled"), file=sys.stderr)
        return 130
    except Exception:
        print(_error_json("coverage_failed"), file=sys.stderr)
        return 1
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(run_command())
