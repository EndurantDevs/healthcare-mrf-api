# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared durable guard for one PTG source-file import attempt."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping

from sqlalchemy import text

from process.ptg_parts.db_tables import _quote_ident
from db.migration_ptg2_legacy_v3_metadata_reconcile import (
    ATTEMPT_AUTHORITY_SERVICE_NAME,
    CAPABILITY_TABLE,
    HEALTHCARE_SERVICE_NAME,
    PTG_SOURCE_ATTEMPT_LOCK_NAMESPACE,
    PTG_SOURCE_ATTEMPT_PROTOCOL,
)
from process.ptg_parts.ptg2_schema import resolve_ptg2_schema


_FENCE_ERROR_MARKERS = (
    "PTG2_LEGACY_V3_ATTEMPT_RECONCILED",
    "PTG_SOURCE_ATTEMPT_ID_INVALID",
)


class PTGSourceAttemptFencedError(RuntimeError):
    """A source-file attempt was terminally reconciled."""

    error_code = "ptg_source_attempt_terminally_reconciled"


class PTGSourceAttemptTerminalError(RuntimeError):
    """A terminal outer run cannot admit new remote work."""


def normalize_source_file_import_id(value: Any) -> str:
    """Validate the byte-preserving shared source-attempt identity."""

    if not isinstance(value, str):
        raise ValueError("source_file_import_id must be a string")
    normalized = value.strip()
    if not normalized or len(normalized) > 64 or normalized != value:
        raise ValueError(
            "source_file_import_id must be trimmed and 1-64 characters"
        )
    return normalized


def source_attempt_lock_key(source_file_import_id: str) -> str:
    """Return the exact cross-service advisory-lock key bytes."""

    source_id = normalize_source_file_import_id(source_file_import_id)
    return f"{PTG_SOURCE_ATTEMPT_LOCK_NAMESPACE}:{source_id}"


def source_file_import_id_from_payload(
    attempt_payload: Mapping[str, Any],
    *,
    required: bool,
) -> str | None:
    """Resolve an explicitly source-backed identity and its aliases.

    ``import_id`` predates the source-file attempt protocol and is also used
    by ordinary PTG imports.  It is therefore an alias only after at least one
    payload view explicitly supplies ``source_file_import_id``; it cannot opt
    an otherwise ordinary PTG request into the shared attempt protocol.
    """

    params = attempt_payload.get("params")
    params_by_name = params if isinstance(params, Mapping) else {}
    metrics = attempt_payload.get("metrics")
    metrics_by_name = metrics if isinstance(metrics, Mapping) else {}
    source_values = [
        view["source_file_import_id"]
        for view in (attempt_payload, params_by_name, metrics_by_name)
        if (
            "source_file_import_id" in view
            and view["source_file_import_id"] is not None
        )
    ]
    if not source_values:
        if required:
            raise ValueError("PTG source attempt identity is required")
        return None
    explicit_values = [
        view[field_name]
        for view in (attempt_payload, params_by_name, metrics_by_name)
        for field_name in ("source_file_import_id", "import_id")
        if field_name in view and view[field_name] is not None
    ]
    normalized_values = {
        normalize_source_file_import_id(identity_value)
        for identity_value in explicit_values
    }
    if len(normalized_values) != 1:
        raise ValueError("PTG source attempt identity views conflict")
    return normalized_values.pop()


def canonical_digest(value: Any) -> str:
    """Hash one stable JSON-compatible state envelope."""

    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _schema_table(schema_name: str, table_name: str) -> str:
    return (
        f"{_quote_ident(schema_name)}.{_quote_ident(table_name)}"
    )


async def _execute(
    executor: Any,
    statement: Any,
    parameters: Mapping[str, Any],
) -> Any:
    execute = getattr(executor, "execute", None)
    if execute is not None:
        return await execute(statement, dict(parameters))
    status = getattr(executor, "status", None)
    if status is None:
        raise TypeError("source-attempt executor cannot execute SQL")
    try:
        return await status(statement, **dict(parameters))
    except TypeError as error:
        if "unexpected keyword argument" not in str(error):
            raise
        bindparams = getattr(statement, "bindparams", None)
        bound_statement = (
            bindparams(**dict(parameters))
            if callable(bindparams)
            else statement
        )
        return await status(bound_statement)


async def _scalar(
    executor: Any,
    statement: Any,
    parameters: Mapping[str, Any],
) -> Any:
    scalar = getattr(executor, "scalar", None)
    if scalar is not None:
        try:
            return await scalar(statement, dict(parameters))
        except TypeError:
            return await scalar(statement, **dict(parameters))
    result = await _execute(executor, statement, parameters)
    return result.scalar()


def _is_fence_error(error: BaseException) -> bool:
    error_text = str(error)
    return any(marker in error_text for marker in _FENCE_ERROR_MARKERS)


async def guard_source_attempt(
    executor: Any,
    *,
    source_file_import_id: str,
    schema_name: str | None = None,
) -> None:
    """Acquire the exact attempt lock and reject a terminal fence."""

    source_id = normalize_source_file_import_id(source_file_import_id)
    schema = _quote_ident(resolve_ptg2_schema(schema_name))
    try:
        await _execute(
            executor,
            text(
                f"SELECT {schema}.guard_ptg_source_attempt("
                ":source_file_import_id)"
            ),
            {"source_file_import_id": source_id},
        )
    except Exception as error:
        if _is_fence_error(error):
            raise PTGSourceAttemptFencedError(
                "PTG source attempt is terminally reconciled"
            ) from error
        raise


async def require_source_attempt_capabilities(
    executor: Any,
    *,
    require_attempt_authority: bool,
    schema_name: str | None = None,
) -> None:
    """Fail closed unless exact protocol rows share this database."""

    resolved_schema = resolve_ptg2_schema(schema_name)
    capability = _schema_table(resolved_schema, CAPABILITY_TABLE)
    required_services = (
        [HEALTHCARE_SERVICE_NAME, ATTEMPT_AUTHORITY_SERVICE_NAME]
        if require_attempt_authority
        else [HEALTHCARE_SERVICE_NAME]
    )
    matched_count = await _scalar(
        executor,
        text(
            f"""
            SELECT COUNT(*)
              FROM {capability}
             WHERE service_name = ANY(CAST(:required_services AS text[]))
               AND protocol_version = :protocol_version
               AND lock_namespace = :lock_namespace
               AND hash_seed = 0
               AND database_name = current_database()
            """
        ),
        {
            "required_services": required_services,
            "protocol_version": PTG_SOURCE_ATTEMPT_PROTOCOL,
            "lock_namespace": PTG_SOURCE_ATTEMPT_LOCK_NAMESPACE,
        },
    )
    if int(matched_count or 0) != len(required_services):
        raise RuntimeError("PTG_SOURCE_ATTEMPT_CAPABILITY_UNAVAILABLE")


__all__ = [
    "PTGSourceAttemptFencedError",
    "PTGSourceAttemptTerminalError",
    "canonical_digest",
    "guard_source_attempt",
    "normalize_source_file_import_id",
    "require_source_attempt_capabilities",
    "source_attempt_lock_key",
    "source_file_import_id_from_payload",
]
