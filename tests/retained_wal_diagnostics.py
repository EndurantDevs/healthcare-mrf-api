# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed aggregate diagnostics for the native retained-copy component test."""

import asyncio
import json
import re
import time

_PHASES = (
    "admitted_preparation",
    "raw_validation",
    "raw_create",
    "raw_copy",
    "raw_index",
    "address_prepare",
    "stamp",
    "readiness",
    "final_bounds",
)
_OPERANDS = {"data_delta_bytes", "data_limit_bytes", "wal_bytes", "wal_limit_bytes"}
MAX_INSPECTION_BYTES = 128 * 1024**2


async def retained_wal_attribution(connection, start_lsn, wal_bytes):
    """Annotate only an original failed interval; never replace its exception."""
    try:
        first = _lsn(start_lsn)
        if type(wal_bytes) is not int or not 0 < wal_bytes <= MAX_INSPECTION_BYTES or first + wal_bytes >= 2**64:
            return None
        if not connection.is_in_transaction():
            return None
        last = first + wal_bytes
        end_lsn = f"{last >> 32:X}/{last & 0xFFFFFFFF:X}"
        async with asyncio.timeout(5):
            savepoint = connection.transaction()
            await savepoint.start()
            try:
                document_by_field = await _inspect_original_interval(connection, start_lsn, end_lsn)
            finally:
                await savepoint.rollback()
        document_by_field.update(wal_start_lsn=start_lsn, wal_end_lsn=end_lsn, lsn_span_bytes=wal_bytes)
        encoded = json.dumps(document_by_field, sort_keys=True, allow_nan=False)
        if len(encoded.encode()) > 65536:
            return None
        return "retention_native_wal_attribution " + encoded
    except Exception, asyncio.CancelledError:
        return None


async def _inspect_original_interval(connection, start_lsn, end_lsn):
    """Inspect flushed WAL under rollback-only budgets in a disposable database."""
    database = await connection.fetchval("SELECT pg_catalog.current_database()")
    if re.fullmatch(r"hc_cms_admission_test_[0-9a-f]{32}", database or "") is None:
        raise ValueError("retained_diagnostic_database_invalid")
    for name, setting in (("statement_timeout", "2s"), ("work_mem", "4MB"), ("temp_file_limit", "64MB")):
        await connection.execute("SELECT pg_catalog.set_config($1,$2,true)", name, setting)
    # The native inspector clamps future end LSNs to flushed WAL; never label a partial interval complete.
    for _ in range(20):
        if await connection.fetchval("SELECT pg_catalog.pg_current_wal_flush_lsn() >= $1::text::pg_lsn", end_lsn):
            break
        await asyncio.sleep(0.05)
    else:
        raise ValueError("retained_diagnostic_end_not_flushed")
    record_totals = await connection.fetch(
        """SELECT "resource_manager/record_type" AS kind,count AS records,
        record_size AS record_bytes,fpi_size AS fpi_bytes,combined_size AS total_bytes
        FROM public.pg_get_wal_stats($1::text::pg_lsn,$2::text::pg_lsn,true)
        WHERE count>0 ORDER BY "resource_manager/record_type" LIMIT 257""",
        start_lsn,
        end_lsn,
    )
    blocks = await connection.fetch(
        """SELECT reltablespace,reldatabase,relfilenode,relforknumber,
        count(*)::bigint AS block_references,sum(block_data_length)::bigint AS block_data_bytes,
        sum(block_fpi_length)::bigint AS fpi_bytes,count(*) OVER()::bigint AS relation_groups
        FROM public.pg_get_wal_block_info($1::text::pg_lsn,$2::text::pg_lsn,false)
        GROUP BY reltablespace,reldatabase,relfilenode,relforknumber
        ORDER BY sum(block_fpi_length)+sum(block_data_length) DESC,
        reltablespace,reldatabase,relfilenode,relforknumber LIMIT 16""",
        start_lsn,
        end_lsn,
    )
    return _closed_attribution(record_totals, blocks)


def _closed_attribution(record_totals, blocks):
    """Detach only bounded labels and integer aggregates, never WAL descriptions or data."""
    if not 0 < len(record_totals) <= 256 or len(blocks) > 16:
        raise ValueError("retained_diagnostic_summary_invalid")
    record_keys = {"kind", "records", "record_bytes", "fpi_bytes", "total_bytes"}
    block_keys = {
        "reltablespace",
        "reldatabase",
        "relfilenode",
        "relforknumber",
        "block_references",
        "block_data_bytes",
        "fpi_bytes",
        "relation_groups",
    }
    record_entries, block_entries = [dict(entry) for entry in record_totals], [dict(entry) for entry in blocks]
    for entry in record_entries:
        if (
            set(entry) != record_keys
            or type(entry["kind"]) is not str
            or re.fullmatch(r"[A-Za-z0-9_ /()+-]{1,96}", entry["kind"]) is None
            or entry["record_bytes"] + entry["fpi_bytes"] != entry["total_bytes"]
        ):
            raise ValueError("retained_diagnostic_summary_invalid")
    for entries, keys in ((record_entries, record_keys - {"kind"}), (block_entries, block_keys)):
        for entry in entries:
            if set(entry) != (record_keys if entries is record_entries else block_keys) or any(
                type(entry[key]) is not int or not 0 <= entry[key] < 2**63 for key in keys
            ):
                raise ValueError("retained_diagnostic_summary_invalid")
    return {
        "version": 1,
        "status": "complete",
        "record_types": record_entries,
        "top_physical_relations": block_entries,
        "relation_limit": 16,
        "relation_bytes_are_block_data_and_fpi_only": True,
        "accounting_authority": False,
    }


def _lsn(value):
    """Accept only the two native 32-bit hexadecimal LSN components."""
    if type(value) is not str or re.fullmatch(r"[0-9A-F]{1,8}/[0-9A-F]{1,8}", value) is None:
        raise ValueError("retained_diagnostic_lsn_invalid")
    upper, lower = value.split("/")
    return (int(upper, 16) << 32) + int(lower, 16)


def retained_bound_operands(error, bounds_check, *, relation_limit):
    """Read only the original check's already measured operands, never SQL or error text."""
    pending_errors, seen_error_ids = [error], set()
    while pending_errors and len(seen_error_ids) < 32:
        current = pending_errors.pop()
        if id(current) in seen_error_ids:
            continue
        seen_error_ids.add(id(current))
        pending_errors.extend(link for link in (current.__context__, current.__cause__) if link is not None)
        traceback = current.__traceback__
        while traceback is not None:
            frame = traceback.tb_frame
            if frame.f_code is bounds_check.__code__:
                return _bound_frame_operands(frame, relation_limit)
            traceback = traceback.tb_next
    return None


def _bound_frame_operands(frame, relation_limit):
    """Validate only the original check's bounded numeric locals."""
    locals_by_name = frame.f_locals
    if not {"relations", "request", "before_bytes", "wal_bytes"}.issubset(locals_by_name):
        return None
    relations, request = locals_by_name["relations"], locals_by_name["request"]
    if type(relations) not in (tuple, list) or len(relations) > relation_limit:
        return None
    try:
        sizes = tuple(relation.total_bytes for relation in relations)
        if any(type(size) is not int or not 0 <= size < 2**63 for size in sizes):
            return None
        if type(locals_by_name["before_bytes"]) is not int:
            return None
        operand_by_name = {
            "data_delta_bytes": sum(sizes) - locals_by_name["before_bytes"],
            "data_limit_bytes": request.extra_data_upper_bound_bytes,
            "wal_bytes": locals_by_name["wal_bytes"],
            "wal_limit_bytes": request.extra_wal_upper_bound_bytes,
        }
    except AttributeError, TypeError:
        return None
    return operand_by_name if _valid_operands(operand_by_name) else None


def _valid_operands(values):
    """Refuse malformed operands and require an actual signed-bound excess."""
    return (
        type(values) is dict
        and set(values) == _OPERANDS
        and all(type(value) is int and 0 <= value < 2**63 for value in values.values())
        and values["data_limit_bytes"] > 0
        and values["wal_limit_bytes"] > 0
        and (values["data_delta_bytes"] > values["data_limit_bytes"] or values["wal_bytes"] > values["wal_limit_bytes"])
    )


class RetainedWalDiagnostic:
    """Retain at most nine phase aggregates using only existing native LSN reads."""

    def __init__(self):
        self.started_ns = time.monotonic_ns()
        self.started_at_unix_ns = time.time_ns()
        self.start_lsn = None
        self.baseline_elapsed_ns = None
        self.phase = "admitted_preparation"
        self.markers = {}
        self.samples = {}
        self.refused = False
        self.enter(self.phase)

    def enter(self, phase):
        """Mark a closed boundary without claiming a WAL read occurred there."""
        if phase not in _PHASES:
            self.refused = True
            return
        self.phase = phase
        self.markers.setdefault(phase, time.monotonic_ns() - self.started_ns)

    def begin(self, start_lsn):
        """Bind the actual original raw admission baseline, after preliminary checks."""
        try:
            _lsn(start_lsn)
        except ValueError:
            self.refused = True
            return
        if self.start_lsn is not None:
            self.refused = True
            return
        self.start_lsn = start_lsn
        self.baseline_elapsed_ns = time.monotonic_ns() - self.started_ns
        self.enter("raw_validation")

    def record(self, query_start_lsn, observed_wal_bytes):
        """Translate an existing query's baseline into this attempt's aggregate interval."""
        if self.start_lsn is None or self.refused:
            return
        try:
            if type(observed_wal_bytes) is not int or not 0 <= observed_wal_bytes < 2**63:
                raise ValueError("retained_diagnostic_operand_invalid")
            delta = _lsn(query_start_lsn) + observed_wal_bytes - _lsn(self.start_lsn)
            if not 0 <= delta < 2**63:
                raise ValueError("retained_diagnostic_operand_invalid")
        except ValueError:
            self.refused = True
            return
        elapsed = time.monotonic_ns() - self.started_ns
        sample = self.samples.setdefault(
            self.phase,
            {
                "checks": 0,
                "first_wal_bytes": delta,
                "last_wal_bytes": delta,
                "peak_wal_bytes": delta,
                "first_elapsed_ns": elapsed,
                "last_elapsed_ns": elapsed,
            },
        )
        sample.update(
            checks=sample["checks"] + 1,
            last_wal_bytes=delta,
            peak_wal_bytes=max(sample["peak_wal_bytes"], delta),
            last_elapsed_ns=elapsed,
        )

    def note(self, operands):
        """Emit only closed labels, numeric operands and validated native LSN positions."""
        if self.refused or self.start_lsn is None or not _valid_operands(operands):
            return None
        is_data_over = operands["data_delta_bytes"] > operands["data_limit_bytes"]
        is_wal_over = operands["wal_bytes"] > operands["wal_limit_bytes"]
        cause = "data_and_wal_limits" if is_data_over and is_wal_over else "data_limit" if is_data_over else "wal_limit"
        end = _lsn(self.start_lsn) + operands["wal_bytes"]
        if end >= 2**64:
            return None
        document_by_name = {
            "version": 1,
            "phase": self.phase,
            "cause": cause,
            "wal_start_lsn": self.start_lsn,
            "wal_end_lsn": f"{end >> 32:X}/{end & 0xFFFFFFFF:X}",
            "started_at_unix_ns": self.started_at_unix_ns,
            "baseline_elapsed_ns": self.baseline_elapsed_ns,
            "markers_elapsed_ns": self.markers,
            "phase_samples": self.samples,
        }
        return "retention_native_phases " + json.dumps(document_by_name, sort_keys=True)
