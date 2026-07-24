# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable writer fence for metadata-reconciled PTG V4 attempts."""

from __future__ import annotations

import re
from typing import Any, Iterable, Mapping, NoReturn

from sqlalchemy import text

from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_v4_attempt_registry import (
    AttemptAttachment,
    attempt_attachment_for_table,
)
from process.ptg_parts.ptg2_v4_stale_metadata_json import json_mapping
from process.ptg_parts.ptg2_v4_stale_metadata_types import (
    PTG2_V4_STALE_METADATA_MARKER,
)


class StaleMetadataFenceError(RuntimeError):
    """A resumed writer targeted a metadata-reconciled V4 attempt."""


def has_stale_metadata_marker(value: Any) -> bool:
    """Return whether a JSON envelope carries the durable fence marker."""

    return PTG2_V4_STALE_METADATA_MARKER in json_mapping(value)


_FENCE_ERROR_MARKERS = (
    "PTG2_STALE_METADATA_FENCE",
    "PTG2_ATTEMPT_FENCE_",
    "PTG2_RECONCILIATION_AUDIT_",
)
_TABLE_NAME_RE = re.compile(r"^[a-z0-9_]{1,63}$")


def is_stale_metadata_fence_error(error: BaseException) -> bool:
    """Recognize database and application forms of the durable fence."""

    if isinstance(error, StaleMetadataFenceError):
        return True
    error_text = str(error)
    return any(marker in error_text for marker in _FENCE_ERROR_MARKERS)


def raise_stale_metadata_fence(error: BaseException) -> NoReturn:
    """Translate a database guard rejection into the importer fence error."""

    raise StaleMetadataFenceError(
        "PTG V4 attempt is metadata-reconciled and durably fenced"
    ) from error


async def _execute_statement(
    session: Any,
    statement: Any,
    parameter_by_name: Mapping[str, Any],
) -> Any:
    execute = getattr(session, "execute", None)
    if execute is not None:
        return await execute(statement, dict(parameter_by_name))
    status = getattr(session, "status", None)
    if status is None:
        raise TypeError("attempt fence session cannot execute SQL")
    return await status(statement, **dict(parameter_by_name))


async def lock_writable_snapshot(
    session: Any,
    database: Any | None,
    *,
    schema_name: str,
    snapshot_id: str,
    internal_run_id: str | None = None,
    allow_reconciled: bool = False,
) -> None:
    """Lock one snapshot/run pair and reject a reconciled writer."""

    schema = _quote_ident(schema_name)
    statement = getattr(database, "text", text)
    try:
        await _execute_statement(
            session,
            statement(
                f"SELECT {schema}.guard_ptg2_v4_attempt("
                ":snapshot_id, :internal_run_id, :allow_reconciled)"
            ),
            {
                "snapshot_id": str(snapshot_id),
                "internal_run_id": (
                    str(internal_run_id)
                    if internal_run_id is not None
                    else None
                ),
                "allow_reconciled": bool(allow_reconciled),
            },
        )
    except Exception as error:
        if is_stale_metadata_fence_error(error):
            raise_stale_metadata_fence(error)
        raise


def _coordinate_values(
    attachment: AttemptAttachment,
    row_by_field: Mapping[str, Any],
) -> set[tuple[str | None, str | None]]:
    snapshot_values = {
        str(row_by_field[column])
        for column in attachment.snapshot_columns
        if row_by_field.get(column)
    }
    run_values = {
        str(row_by_field[column])
        for column in attachment.run_columns
        if row_by_field.get(column)
    }
    if snapshot_values and run_values:
        return {
            (snapshot_id, internal_run_id)
            for snapshot_id in snapshot_values
            for internal_run_id in run_values
        }
    if snapshot_values:
        return {(snapshot_id, None) for snapshot_id in snapshot_values}
    return {(None, internal_run_id) for internal_run_id in run_values}


async def guard_attempt_rows(
    session: Any,
    database: Any | None,
    *,
    schema_name: str,
    table_name: str,
    attempt_rows: Iterable[Mapping[str, Any]],
) -> None:
    """Fence every registered coordinate before one transactional write."""

    attachment = attempt_attachment_for_table(table_name)
    if attachment is None:
        return
    coordinates: set[tuple[str | None, str | None]] = set()
    row_count = 0
    for row_by_field in attempt_rows:
        row_count += 1
        coordinates.update(_coordinate_values(attachment, row_by_field))
    if row_count and not coordinates:
        raise ValueError(
            f"{table_name} write omitted its registered attempt coordinate"
        )
    for snapshot_id, internal_run_id in sorted(
        coordinates,
        key=lambda coordinate: (
            coordinate[0] or "",
            coordinate[1] or "",
        ),
    ):
        await lock_writable_snapshot(
            session,
            database,
            schema_name=schema_name,
            snapshot_id=snapshot_id or "",
            internal_run_id=internal_run_id,
        )


def _validated_table_names(table_names: list[str]) -> tuple[str, ...]:
    normalized_names = tuple(
        sorted({str(table_name or "").strip() for table_name in table_names})
    )
    if any(
        _TABLE_NAME_RE.fullmatch(table_name) is None
        for table_name in normalized_names
    ):
        raise ValueError("attempt stage table name is invalid")
    return normalized_names


async def register_attempt_stage_tables(
    database: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    internal_run_id: str,
    table_names: list[str],
) -> None:
    """Fence and register every deterministic stage before creating it."""

    normalized_names = _validated_table_names(table_names)
    if not normalized_names:
        return
    schema = _quote_ident(schema_name)
    async with database.transaction() as session:
        await lock_writable_snapshot(
            session,
            database,
            schema_name=schema_name,
            snapshot_id=snapshot_id,
            internal_run_id=internal_run_id,
        )
        await session.execute(
            database.text(
                f"""
                INSERT INTO {schema}.ptg2_v4_attempt_stage
                    (snapshot_id, internal_run_id, table_name)
                VALUES (:snapshot_id, :internal_run_id, :table_name)
                ON CONFLICT (snapshot_id, table_name) DO NOTHING
                """
            ),
            [
                {
                    "snapshot_id": snapshot_id,
                    "internal_run_id": internal_run_id,
                    "table_name": table_name,
                }
                for table_name in normalized_names
            ],
        )


async def drop_attempt_stage_tables(
    database: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    internal_run_id: str,
    table_names: list[str],
    retain_registration: bool = False,
) -> None:
    """Drop writable stages, optionally retaining their crash-gap attachment."""

    normalized_names = _validated_table_names(table_names)
    if not normalized_names:
        return
    schema = _quote_ident(schema_name)
    async with database.transaction() as session:
        await lock_writable_snapshot(
            session,
            database,
            schema_name=schema_name,
            snapshot_id=snapshot_id,
            internal_run_id=internal_run_id,
        )
        for table_name in normalized_names:
            await session.execute(
                text(
                    f"DROP TABLE IF EXISTS "
                    f"{schema}.{_quote_ident(table_name)}"
                )
            )
        if not retain_registration:
            await session.execute(
                database.text(
                    f"""
                    DELETE FROM {schema}.ptg2_v4_attempt_stage
                     WHERE snapshot_id = :snapshot_id
                       AND internal_run_id = :internal_run_id
                       AND table_name = ANY(:table_names)
                    """
                ),
                {
                    "snapshot_id": snapshot_id,
                    "internal_run_id": internal_run_id,
                    "table_names": list(normalized_names),
                },
            )


__all__ = [
    "StaleMetadataFenceError",
    "guard_attempt_rows",
    "has_stale_metadata_marker",
    "is_stale_metadata_fence_error",
    "lock_writable_snapshot",
    "drop_attempt_stage_tables",
    "raise_stale_metadata_fence",
    "register_attempt_stage_tables",
]
