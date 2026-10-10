# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit one explicitly identified, digest-verified CMS source edition."""

import argparse
import asyncio
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path
from uuid import UUID

from db.connection import Database
from process.cms_planfinder_workbook_input import MAX_WORKBOOK_BYTES
from process.custom_import.cli import _receipt_only_database_output, _set_engine_echo
from process.ptg_parts.artifacts import sha256_file
from process.registry_source_admission import (
    MAX_INPUT_BYTES,
    RegistrySourceEdition,
    admit_cms_mlr_edition,
    admit_cms_planfinder_edition,
)

_COUNTERS = (
    "observations",
    "accepted",
    "unresolved",
    "rejected",
    "identifiers",
    "resolved_identifiers",
    "issuer_assertions",
    "resolved_issuers",
    "conflicting_issuers",
    "group_assertions",
    "resolved_groups",
)
_IDENTITY_COUNTERS = (
    "source_rows",
    "companies_created",
    "groups_created",
    "bindings_created",
    "company_candidates",
    "company_conflicts",
    "company_gaps",
    "unsupported_company_names",
    "group_candidates",
    "group_gaps",
    "unsupported_group_labels",
)
_NATIVE_COUNTERS = (
    "input_rows",
    "accepted_rows",
    "unresolved_rows",
    "rejected_rows",
    "grand_total_rows",
    "issuer_rows",
    "nonempty_raw_company_pk_rows",
    "source_company_anchors",
    "distinct_hios_issuers",
    "distinct_naic_company_codes",
    "distinct_naic_group_codes",
    "ambiguous_group_labels",
)


class _InputError(ValueError):
    """A fixed error code describes rejected local input without exposing it."""


class _ReceiptArgumentParser(argparse.ArgumentParser):
    def error(self, _message):
        """Suppress argument values in the fixed failure receipt."""
        self.exit(2, _error_json("invalid_arguments") + "\n")


def _error_json(code):
    return json.dumps({"status": "error", "code": code}, separators=(",", ":"), sort_keys=True)


def _publication_date(value):
    if value == "unknown":
        return None
    published_at = datetime.fromisoformat(value)
    if published_at.utcoffset() is None:
        raise ValueError("Publication date requires a timezone")
    return published_at


def _parser():
    parser = _ReceiptArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--input-file", type=Path, required=True)
    parser.add_argument("--snapshot-id", type=UUID, required=True)
    parser.add_argument("--source-system", choices=("cms",), required=True)
    parser.add_argument("--source-id", choices=("commercial-mlr", "plan-finder"), required=True)
    for field in ("edition-id", "source-url", "artifact-sha256", "input-sha256", "parser-version"):
        parser.add_argument("--" + field, required=True)
    parser.add_argument("--reporting-year", type=int, required=True)
    parser.add_argument(
        "--published-at", type=_publication_date, required=True, help="timezone-aware ISO date or unknown"
    )
    return parser


def _edition(parsed):
    metadata_by_name = vars(parsed).copy()
    metadata_by_name.pop("input_file")
    return RegistrySourceEdition(**metadata_by_name)


def _read_input(input_file, input_sha256):
    with input_file.open("rb") as source:
        input_bytes = source.read(MAX_INPUT_BYTES + 1)
    if len(input_bytes) > MAX_INPUT_BYTES:
        raise _InputError("input_limit")
    if hashlib.sha256(input_bytes).hexdigest() != input_sha256:
        raise _InputError("digest_mismatch")
    return input_bytes


def _read_planfinder_input(input_file, input_sha256):
    if input_file.stat().st_size > MAX_WORKBOOK_BYTES:
        raise _InputError("input_limit")
    if sha256_file(input_file)[0] != input_sha256:
        raise _InputError("digest_mismatch")
    return input_file


def _aggregate_counters(totals_by_name, allowed_fields):
    counters_by_name = {field: totals_by_name[field] for field in allowed_fields if field in totals_by_name}
    if any(type(value) is not int or value < 0 for value in counters_by_name.values()):
        raise ValueError("Admission counters are invalid")
    return counters_by_name


def _receipt_json(edition, admission_by_name):
    receipt_by_name = _aggregate_counters(admission_by_name, _COUNTERS)
    if type(admission_by_name["replayed"]) is not bool:
        raise ValueError("Admission replay status is invalid")
    receipt_by_name.update(
        status="ok",
        snapshot_id=str(edition.snapshot_id),
        input_sha256=edition.input_sha256,
        artifact_sha256=edition.artifact_sha256,
        copy_sha256=admission_by_name["copy_sha256"],
        copy_bytes=admission_by_name["copy_bytes"],
        replayed=admission_by_name["replayed"],
        identity_materialization=_aggregate_counters(admission_by_name["identity_materialization"], _IDENTITY_COUNTERS),
        native_counts=_aggregate_counters(admission_by_name["native_counts"], _NATIVE_COUNTERS),
    )
    if "copy_batches" in admission_by_name:
        receipt_by_name.update(_aggregate_counters(admission_by_name, ("copy_batches",)))
    receipt = json.dumps(receipt_by_name, separators=(",", ":"), sort_keys=True)
    if len(receipt.encode()) > 8192:
        raise ValueError("Admission receipt exceeds its bound")
    return receipt


async def _disconnect_database(database):
    """Preserve cancellation if database cleanup also fails."""
    primary_error = sys.exception()
    try:
        await database.disconnect()
    except Exception:
        if isinstance(primary_error, (KeyboardInterrupt, asyncio.CancelledError)):
            raise primary_error from None
        raise


async def _run_import(input_bytes, edition):
    database = Database()
    with _receipt_only_database_output(database):
        try:
            await database.connect()
            _set_engine_echo(database, False)
            async with database.acquire_driver() as connection, connection.transaction():
                admit = admit_cms_planfinder_edition if edition.source_id == "plan-finder" else admit_cms_mlr_edition
                admission_by_name = await admit(connection, input_bytes, edition)
                receipt = _receipt_json(edition, admission_by_name)
            return receipt
        finally:
            await _disconnect_database(database)


def run_command(arguments=None):
    """Run one local admission, emitting one receipt and no source or connection data."""
    parsed = _parser().parse_args(arguments)
    try:
        edition = _edition(parsed)
        read_input = _read_planfinder_input if edition.source_id == "plan-finder" else _read_input
        input_bytes = read_input(parsed.input_file, edition.input_sha256)
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
