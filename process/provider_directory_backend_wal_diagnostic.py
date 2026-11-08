# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Passive PostgreSQL 18 statistics on a caller-retained physical connection.

These counters do not replace signed reservations or global physical WAL limits.
The owner must retain exclusive driver custody and zero parallel execution for
the whole work interval, complete its own top-level work transaction, then finish.
The helper never commits, rolls back, releases, closes or acquires a connection.
On cancellation or loss, the owner must drain/invalidate its existing connection;
an incomplete diagnostic supplies no accounting result or reservation refund.
It cannot classify an already committed mutation as a confirmed refusal.
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import asyncpg

_FORCE_SQL = "SELECT pg_catalog.pg_stat_force_next_flush()"
_CLEAR_SQL = "SELECT pg_catalog.pg_stat_clear_snapshot()"
_PREFLIGHT_SQL = """
SELECT pg_catalog.pg_backend_pid() AS pid,
  pg_catalog.current_setting('server_version_num')::integer AS server_version,
  pg_catalog.current_setting('max_parallel_workers')::integer AS parallel_workers,
  pg_catalog.current_setting('max_parallel_workers_per_gather')::integer AS parallel_gather,
  pg_catalog.current_setting('max_parallel_maintenance_workers')::integer AS parallel_maintenance,
  pg_catalog.current_setting('debug_parallel_query') AS debug_parallel,
  pg_catalog.has_function_privilege(current_user,
    'pg_catalog.pg_stat_get_backend_wal(integer)','EXECUTE')
  AND pg_catalog.has_function_privilege(current_user,
    'pg_catalog.pg_stat_force_next_flush()','EXECUTE')
  AND pg_catalog.has_function_privilege(current_user,
    'pg_catalog.pg_stat_clear_snapshot()','EXECUTE')
  AND pg_catalog.has_function_privilege(current_user,
    'pg_catalog.pg_control_system()','EXECUTE')
  AND pg_catalog.has_function_privilege(current_user,
    'pg_catalog.pg_current_wal_insert_lsn()','EXECUTE') AS access_allowed
"""
_SAMPLE_SQL = """
SELECT activity.pid,activity.backend_start,activity.datid::bigint AS database_oid,
  activity.datname AS database_name,activity.leader_pid,
  control.system_identifier::text AS system_identifier,
  wal.wal_records,wal.wal_fpi,wal.wal_bytes,wal.wal_buffers_full,wal.stats_reset,
  pg_catalog.pg_current_wal_insert_lsn()::text AS insert_lsn,
  pg_catalog.current_setting('server_version_num')::integer AS server_version,
  pg_catalog.current_setting('max_parallel_workers')::integer AS parallel_workers,
  pg_catalog.current_setting('max_parallel_workers_per_gather')::integer AS parallel_gather,
  pg_catalog.current_setting('max_parallel_maintenance_workers')::integer AS parallel_maintenance,
  pg_catalog.current_setting('debug_parallel_query') AS debug_parallel
FROM pg_catalog.pg_stat_activity activity
CROSS JOIN pg_catalog.pg_control_system() control
CROSS JOIN LATERAL pg_catalog.pg_stat_get_backend_wal($1) wal
WHERE activity.pid=$1 AND activity.pid=pg_catalog.pg_backend_pid()
"""


_OWNER_BOUNDARY_SAMPLE_SQL = """
SELECT activity.pid,activity.backend_start,activity.datid::bigint AS database_oid,
  pg_catalog.current_database() AS database_name,activity.leader_pid,
  control.system_identifier::pg_catalog.text AS system_identifier,
  wal.wal_records,wal.wal_fpi,wal.wal_bytes,wal.wal_buffers_full,wal.stats_reset,
  pg_catalog.pg_current_wal_insert_lsn()::pg_catalog.text AS insert_lsn,
  pg_catalog.current_setting('server_version_num')::integer AS server_version
FROM pg_catalog.pg_stat_get_activity($1) activity
CROSS JOIN pg_catalog.pg_control_system() control
CROSS JOIN LATERAL pg_catalog.pg_stat_get_backend_wal($1) wal
WHERE activity.pid OPERATOR(pg_catalog.=) $1 AND activity.pid OPERATOR(pg_catalog.=) pg_catalog.pg_backend_pid()
"""


class BackendWalDiagnosticUnavailable(ValueError):
    """The passive observation is incomplete; existing exposure stays charged."""

    def __init__(self):
        super().__init__("provider_directory_backend_wal_diagnostic_unavailable")


@dataclass(frozen=True)
class BackendWalIdentity:
    pid: int
    backend_start: datetime
    database_oid: int
    database_name: str
    system_identifier: str
    stats_reset: datetime | None


@dataclass(frozen=True)
class BackendWalSnapshot:
    """Raw native values; wal_bytes excludes physical WAL framing overhead."""

    identity: BackendWalIdentity
    wal_records: int
    wal_fpi: int
    wal_bytes: Decimal
    wal_buffers_full: int
    insert_lsn: str


@dataclass(frozen=True)
class BackendWalDiagnosticResult:
    baseline: BackendWalSnapshot
    final: BackendWalSnapshot
    wal_records_delta: int
    wal_fpi_delta: int
    wal_bytes_delta: Decimal
    wal_buffers_full_delta: int
    global_insert_lsn_span: int


class BackendWalDiagnostic:
    """One task's passive observation on the same retained physical driver."""

    def __init__(self, connection, baseline):
        self._connection = connection
        self._baseline = baseline
        self._owner_task = asyncio.current_task()
        self._consumed = False

    @property
    def baseline(self):
        """Return the immutable identity/counters recorded before caller work."""
        return self._baseline


def _require(condition):
    if not condition:
        raise BackendWalDiagnosticUnavailable()


def _outside_transaction(connection, pid=None):
    """Require a live raw driver; never settle a caller transaction/savepoint."""
    _require(
        all(
            callable(getattr(connection, name, None))
            for name in ("is_closed", "is_in_transaction", "get_server_pid", "fetchrow", "execute")
        )
    )
    _require(connection.is_closed() is False and connection.is_in_transaction() is False)
    current_pid = connection.get_server_pid()
    _require(type(current_pid) is int and 0 < current_pid <= 2147483647)
    _require(pid is None or current_pid == pid)
    return current_pid


def _settings(row, pid):
    _require(row is not None and type(row["pid"]) is int and row["pid"] == pid)
    _require(type(row["server_version"]) is int and 180000 <= row["server_version"] < 190000)
    for field in ("parallel_workers", "parallel_gather", "parallel_maintenance"):
        _require(type(row[field]) is int and row[field] == 0)
    _require(row["debug_parallel"] == "off")


def _timestamp(value):
    _require(type(value) is datetime and value.utcoffset() is not None)
    return value


def _counter(value):
    _require(type(value) is int and 0 <= value <= 9223372036854775807)
    return value


def _lsn_number(value):
    _require(type(value) is str and re.fullmatch(r"[0-9A-F]{1,8}/[0-9A-F]{1,8}", value) is not None)
    segments = value.split("/")
    upper, lower = int(segments[0], 16), int(segments[1], 16)
    _require(value == f"{upper:X}/{lower:X}")
    return (upper << 32) + lower


def _snapshot(sample_by_field, pid):
    """Close identity, reset and numeric values without lossy coercion."""
    _settings(sample_by_field, pid)
    return _snapshot_values(sample_by_field, pid)


def _snapshot_values(sample_by_field, pid):
    """Validate raw identity/counters independently of the work-body contract."""
    _require(sample_by_field["leader_pid"] is None)
    system_identifier = sample_by_field["system_identifier"]
    _require(type(system_identifier) is str and re.fullmatch(r"[1-9][0-9]{0,19}", system_identifier) is not None)
    _require(int(system_identifier) <= 18446744073709551615)
    _require(type(sample_by_field["database_oid"]) is int and 0 < sample_by_field["database_oid"] <= 4294967295)
    database_name = sample_by_field["database_name"]
    _require(type(database_name) is str and 0 < len(database_name.encode()) <= 63 and "\0" not in database_name)
    wal_bytes = sample_by_field["wal_bytes"]
    _require(type(wal_bytes) is Decimal and wal_bytes.is_finite() and 0 <= wal_bytes <= 18446744073709551615)
    _require(wal_bytes == wal_bytes.to_integral_value())
    _lsn_number(sample_by_field["insert_lsn"])
    identity = BackendWalIdentity(
        pid,
        _timestamp(sample_by_field["backend_start"]),
        sample_by_field["database_oid"],
        database_name,
        system_identifier,
        None if sample_by_field["stats_reset"] is None else _timestamp(sample_by_field["stats_reset"]),
    )
    return BackendWalSnapshot(
        identity,
        _counter(sample_by_field["wal_records"]),
        _counter(sample_by_field["wal_fpi"]),
        wal_bytes,
        _counter(sample_by_field["wal_buffers_full"]),
        sample_by_field["insert_lsn"],
    )


async def _sample(connection, *, pid=None):
    """Complete fresh-snapshot, force and read commands separately outside TX."""
    try:
        pid = _outside_transaction(connection, pid)
        preflight = await connection.fetchrow(_PREFLIGHT_SQL)
        _outside_transaction(connection, pid)
        _settings(preflight, pid)
        _require(preflight["access_allowed"] is True)
        await connection.execute(_CLEAR_SQL)
        _outside_transaction(connection, pid)
        await connection.execute(_FORCE_SQL)
        _outside_transaction(connection, pid)
        sample = await connection.fetchrow(_SAMPLE_SQL, pid)
        _outside_transaction(connection, pid)
        return _snapshot(sample, pid)
    except asyncpg.PostgresError, asyncpg.InterfaceError, ConnectionError, OSError, KeyError, TypeError, UnicodeError:
        raise BackendWalDiagnosticUnavailable() from None


async def begin_backend_wal_diagnostic(connection):
    """Observe a baseline on the supplied driver, without transaction ownership."""
    _require(asyncio.current_task() is not None)
    return BackendWalDiagnostic(connection, await _sample(connection))


def _result(baseline, final):
    _require(final.identity == baseline.identity)
    for field in ("wal_records", "wal_fpi", "wal_bytes", "wal_buffers_full"):
        _require(getattr(final, field) >= getattr(baseline, field))
    span = _lsn_number(final.insert_lsn) - _lsn_number(baseline.insert_lsn)
    _require(span >= 0)
    return BackendWalDiagnosticResult(
        baseline,
        final,
        final.wal_records - baseline.wal_records,
        final.wal_fpi - baseline.wal_fpi,
        final.wal_bytes - baseline.wal_bytes,
        final.wal_buffers_full - baseline.wal_buffers_full,
        span,
    )


async def finish_backend_wal_diagnostic(diagnostic):
    """Consume after caller completes TX; cancellation never implies a refund."""
    _require(type(diagnostic) is BackendWalDiagnostic and not diagnostic._consumed)
    _require(asyncio.current_task() is diagnostic._owner_task)
    diagnostic._consumed = True
    final = await _sample(diagnostic._connection, pid=diagnostic.baseline.identity.pid)
    return _result(diagnostic.baseline, final)


async def sample_backend_wal_owner_boundary(connection, pid):
    """Sample fixed native scalar functions outside TX, with self-tail still held.

    The pinned native activity/WAL FunctionScans are parallel-restricted with
    no partial scan paths, preventing a worker/Gather route for this fixed SQL.
    This does not permit parallel work or prove the sampler's own SQL tail zero.
    """
    try:
        _require(asyncio.current_task() is not None)
        _outside_transaction(connection, pid)
        await connection.execute(_CLEAR_SQL)
        _outside_transaction(connection, pid)
        await connection.execute(_FORCE_SQL)
        _outside_transaction(connection, pid)
        sample = await connection.fetchrow(_OWNER_BOUNDARY_SAMPLE_SQL, pid)
        _outside_transaction(connection, pid)
        _require(sample is not None and type(sample["pid"]) is int and sample["pid"] == pid)
        _require(type(sample["server_version"]) is int and 180000 <= sample["server_version"] < 190000)
        return _snapshot_values(sample, pid)
    except asyncpg.PostgresError, asyncpg.InterfaceError, ConnectionError, OSError, KeyError, TypeError, UnicodeError:
        raise BackendWalDiagnosticUnavailable() from None
