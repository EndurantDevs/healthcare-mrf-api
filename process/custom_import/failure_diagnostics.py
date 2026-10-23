# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed failure labels without exception text, SQL, payloads or traceback data."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from enum import StrEnum
from inspect import getattr_static

from sqlalchemy import exc as database_errors
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError


class FailurePhase(StrEnum):
    UNKNOWN = "unknown"
    CONNECT = "connect"
    BINDING = "binding"
    RESUME_VALIDATION = "resume_validation"
    ADMISSION_STAGE = "admission_stage"
    CAPTURE = "capture"
    BEGIN_BUILD = "begin_build"
    SOURCE = "source"
    ADMISSION_INDEX = "admission_index"
    ADMISSION = "admission"
    COUNTS = "counts"
    GRAPH = "graph"
    OUTPUT = "output"
    ACTIVATE = "activate"
    RECEIPT = "receipt"
    DISCONNECT = "disconnect"


FAILURE_CATEGORIES = frozenset(
    {"canceled", "memory", "timeout", "database", "io", "invalid_state", "runtime", "unexpected"}
)
# Exact PostgreSQL SQLSTATE names, never server-supplied diagnostic strings.
_DATABASE_REASONS = {
    "08000": "connection_exception",
    "08003": "connection_does_not_exist",
    "08006": "connection_failure",
    "08P01": "protocol_violation",
    "22001": "string_data_right_truncation",
    "22003": "numeric_value_out_of_range",
    "22P02": "invalid_text_representation",
    "22P03": "invalid_binary_representation",
    "23502": "not_null_violation",
    "23503": "foreign_key_violation",
    "23505": "unique_violation",
    "23514": "check_violation",
    "23P01": "exclusion_violation",
    "25006": "read_only_sql_transaction",
    "3D000": "invalid_catalog_name",
    "3F000": "invalid_schema_name",
    "40001": "serialization_failure",
    "40P01": "deadlock_detected",
    "42501": "insufficient_privilege",
    "42703": "undefined_column",
    "42883": "undefined_function",
    "42P01": "undefined_table",
    "53100": "disk_full",
    "53200": "out_of_memory",
    "53300": "too_many_connections",
    "53400": "configuration_limit_exceeded",
    "54000": "program_limit_exceeded",
    "55P03": "lock_not_available",
    "57014": "query_canceled",
    "57P01": "admin_shutdown",
    "57P02": "crash_shutdown",
    "57P03": "cannot_connect_now",
}
DATABASE_REASONS = frozenset(_DATABASE_REASONS.values()) | {"not_applicable", "unknown", "connection_pool_timeout"}
_DATABASE_SUBTYPES = (
    (database_errors.PendingRollbackError, "sqlalchemy_pending_rollback"),
    (database_errors.ResourceClosedError, "sqlalchemy_resource_closed"),
    (database_errors.InvalidRequestError, "sqlalchemy_invalid_request"),
    (database_errors.DisconnectionError, "sqlalchemy_disconnection"),
    (database_errors.InterfaceError, "sqlalchemy_interface"),
    (database_errors.OperationalError, "sqlalchemy_operational"),
    (database_errors.ProgrammingError, "sqlalchemy_programming"),
    (database_errors.IntegrityError, "sqlalchemy_integrity"),
    (database_errors.DataError, "sqlalchemy_data"),
    (database_errors.InternalError, "sqlalchemy_internal"),
    (database_errors.DatabaseError, "sqlalchemy_database"),
    (database_errors.DBAPIError, "sqlalchemy_dbapi"),
    (database_errors.StatementError, "sqlalchemy_statement"),
    (SQLAlchemyTimeoutError, "sqlalchemy_timeout"),
    (SQLAlchemyError, "unknown"),
)
DATABASE_SUBTYPES = frozenset(label for _kind, label in _DATABASE_SUBTYPES) | {"not_applicable"}


def _static_attribute(error: BaseException, name: str):
    """Ignore properties and descriptors; never render an arbitrary attribute."""

    try:
        return getattr_static(error, name, None)
    except Exception:
        return None


def _failure_chain(error: BaseException):
    pending, seen = [error], set()
    for _ in range(8):
        if not pending:
            break
        current = pending.pop(0)
        if id(current) in seen:
            continue
        seen.add(id(current))
        yield current
        cause = BaseException.__cause__.__get__(current, BaseException)
        original = _static_attribute(current, "orig")
        pending.extend(value for value in (original, cause) if issubclass(type(value), BaseException))


def _database_reason(error: BaseException) -> str:
    has_database_error = False
    for current in _failure_chain(error):
        has_database_error |= issubclass(type(current), SQLAlchemyError)
        if issubclass(type(current), SQLAlchemyTimeoutError):
            return "connection_pool_timeout"
        for name in ("sqlstate", "pgcode"):
            code = _static_attribute(current, name)
            if type(code) is str and code in _DATABASE_REASONS:
                return _DATABASE_REASONS[code]
    return "unknown" if has_database_error else "not_applicable"


def database_sqlstate(error: BaseException) -> str | None:
    """Read a bounded explicit cause chain without rendering driver attributes."""

    for current in _failure_chain(error):
        for name in ("sqlstate", "pgcode"):
            code = _static_attribute(current, name)
            if type(code) is str and len(code) == 5 and code.isascii() and code.isalnum() and code == code.upper():
                return code
    return None


def database_failure_details(error: BaseException) -> tuple[str, bool, bool]:
    """Classify known wrappers without reading class names or invoking descriptors."""

    subtype, connection_invalidated, retry_blocked = "not_applicable", False, False
    for current in _failure_chain(error):
        retry_blocked |= _static_attribute(current, "_custom_import_retry_blocked") is True
        if issubclass(type(current), database_errors.DBAPIError):
            connection_invalidated |= _static_attribute(current, "connection_invalidated") is True
        if subtype == "not_applicable":
            for kind, label in _DATABASE_SUBTYPES:
                if issubclass(type(current), kind):
                    subtype = label
                    break
    return subtype, connection_invalidated, retry_blocked


def classify_failure(error: BaseException) -> tuple[str, str]:
    """Keep top-level category precedence and inspect only bounded driver codes."""

    for kind, category in (
        ((KeyboardInterrupt, asyncio.CancelledError), "canceled"),
        (MemoryError, "memory"),
        (TimeoutError, "timeout"),
        (SQLAlchemyError, "database"),
        (OSError, "io"),
        ((ValueError, TypeError, KeyError), "invalid_state"),
        (RuntimeError, "runtime"),
    ):
        if issubclass(type(error), kind):
            break
    else:
        category = "unexpected"
    return category, _database_reason(error)


def _identifier(value: object) -> int | None:
    return value if type(value) is int and 0 < value < 2**63 else None


@dataclass
class FailureContext:
    """One mutable snapshot shared with the command's asyncio task, never global state."""

    phase: FailurePhase = FailurePhase.UNKNOWN
    execution_id: int | None = None
    build_id: int | None = None
    failure: dict[str, object] | None = None

    def snapshot(self, error: BaseException) -> dict[str, object]:
        """Return only closed labels and validated identifiers from the first failure."""

        if self.failure is None:
            category, database_reason = classify_failure(error)
            self.failure = {
                "contract_version": "custom-import-failure/v1",
                "phase": self.phase.value,
                "category": category,
                "database_reason": database_reason,
                "execution_id": _identifier(self.execution_id),
                "build_id": _identifier(self.build_id),
            }
        return self.failure.copy()


_CURRENT: ContextVar[FailureContext | None] = ContextVar("custom_import_failure", default=None)


@contextmanager
def failure_context():
    """Create outside asyncio.run so the synchronous error handler keeps the snapshot."""

    context = FailureContext()
    token = _CURRENT.set(context)
    try:
        yield context
    finally:
        _CURRENT.reset(token)


def failure_identifiers(*, execution_id=None, build_id=None) -> None:
    """Remember identifiers already obtained by normal execution, without lookups."""

    context = _CURRENT.get()
    if context is not None:
        if execution_id is not None:
            context.execution_id = _identifier(execution_id)
        if build_id is not None:
            context.build_id = _identifier(build_id)


def record_failure(error: BaseException) -> None:
    """Retain the primary error before existing cleanup or terminal translation."""

    context = _CURRENT.get()
    if context is not None:
        context.snapshot(error)


@contextmanager
def failure_phase(phase: FailurePhase):
    """Freeze before cleanup, but discard exceptions handled inside a successful scope."""

    context = _CURRENT.get()
    if context is None:
        yield
        return
    previous_phase, previous_failure = context.phase, context.failure
    context.phase = phase
    try:
        yield
    except BaseException as error:
        context.snapshot(error)
        raise
    else:
        context.failure = previous_failure
    finally:
        context.phase = previous_phase
