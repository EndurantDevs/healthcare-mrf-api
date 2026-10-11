# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed aggregate diagnostics for the native retained-copy component test."""

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
