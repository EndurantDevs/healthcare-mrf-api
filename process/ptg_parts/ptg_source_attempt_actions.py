# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable PTG source-attempt action admission and event epochs."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping

from sqlalchemy import text

from db.connection import db
from db.migration_ptg2_legacy_v3_metadata_reconcile import (
    EVENT_TABLE,
    PTG_SOURCE_ATTEMPT_PROTOCOL,
)
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_schema import resolve_ptg2_schema
from process.ptg_parts.ptg_source_attempt_guard import (
    PTGSourceAttemptFencedError,
    PTGSourceAttemptTerminalError,
    canonical_digest,
    guard_source_attempt,
    normalize_source_file_import_id,
    require_source_attempt_capabilities,
    source_file_import_id_from_payload,
)


_ACTION_KINDS = frozenset(
    {
        "start_admitted",
        "retry_admitted",
        "ensure_admitted",
        "finalize_admitted",
        "worker_start_admitted",
    }
)
_TERMINAL_OUTER_STATUSES = frozenset(
    {"succeeded", "failed", "canceled", "cancelled", "dead_letter"}
)
_OUTER_RUN_FIELDS = """
    run_id,
    importer,
    status,
    params,
    metrics,
    source_file_import_id,
    import_id,
    retry_of_run_id,
    heartbeat_at
"""
_ALLOWED_STATE_UPDATES = frozenset(
    {"status", "phase_detail", "heartbeat_at", "progress", "import_id"}
)
_UNSET_SOURCE_ID = object()


class PTGSourceAttemptIdentityError(RuntimeError):
    """Queued and durable source-attempt identities disagree."""


@dataclass(frozen=True)
class PTGWorkerActionSelection:
    """Worker selectors that one durable outer run must satisfy."""

    request_importer: str | None
    allowed_importers: frozenset[str]
    allowed_roles: frozenset[str]


@dataclass(frozen=True)
class _SourceActionAdmission:
    event_kind: str
    attempt_id: str | None
    state_updates: Mapping[str, Any] | None
    expected_source_file_import_id: str | None | object
    worker_selection: PTGWorkerActionSelection | None


def _schema_table(schema_name: str, table_name: str) -> str:
    return f'{_quote_ident(schema_name)}.{_quote_ident(table_name)}'


async def _load_outer_run(
    session: Any,
    *,
    outer_run_table: str,
    run_id: str,
    lock_row: bool,
) -> dict[str, Any] | None:
    lock_clause = "FOR UPDATE" if lock_row else ""
    result = await session.execute(
        text(
            f"""
            SELECT {_OUTER_RUN_FIELDS}
              FROM {outer_run_table}
             WHERE run_id = :run_id
             {lock_clause}
            """
        ),
        {"run_id": run_id},
    )
    row = result.mappings().one_or_none()
    return dict(row) if row is not None else None


async def _execute_statement(
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


def _event_state(
    outer_run: Mapping[str, Any],
    *,
    event_kind: str,
    attempt_id: str | None,
) -> dict[str, Any]:
    params = outer_run.get("params")
    params_by_name = params if isinstance(params, Mapping) else {}
    metrics = outer_run.get("metrics")
    metrics_by_name = metrics if isinstance(metrics, Mapping) else {}
    return {
        "protocol": PTG_SOURCE_ATTEMPT_PROTOCOL,
        "event_kind": event_kind,
        "outer_run_id": str(outer_run.get("run_id") or ""),
        "status": str(outer_run.get("status") or ""),
        "heartbeat_at": outer_run.get("heartbeat_at"),
        "retry_of_run_id": outer_run.get("retry_of_run_id"),
        "source_file_import_id": outer_run.get("source_file_import_id"),
        "import_id": outer_run.get("import_id"),
        "params_source_file_import_id": params_by_name.get(
            "source_file_import_id"
        ),
        "params_import_id": params_by_name.get("import_id"),
        "metrics_source_file_import_id": metrics_by_name.get(
            "source_file_import_id"
        ),
        "metrics_import_id": metrics_by_name.get("import_id"),
        "attempt_id": attempt_id,
    }


async def _insert_action_event(
    executor: Any,
    *,
    schema_name: str,
    source_file_import_id: str,
    event_kind: str,
    outer_run_id: str,
    attempt_id: str | None,
    state_digest: str,
) -> None:
    event_table = _schema_table(schema_name, EVENT_TABLE)
    await _execute_statement(
        executor,
        text(
            f"""
            INSERT INTO {event_table} (
                protocol_version, source_file_import_id, event_kind,
                outer_run_id, attempt_id, state_digest
            ) VALUES (
                :protocol_version, :source_file_import_id, :event_kind,
                :outer_run_id, :attempt_id, :state_digest
            )
            """
        ),
        {
            "protocol_version": PTG_SOURCE_ATTEMPT_PROTOCOL,
            "source_file_import_id": source_file_import_id,
            "event_kind": event_kind,
            "outer_run_id": outer_run_id,
            "attempt_id": attempt_id,
            "state_digest": state_digest,
        },
    )


async def record_source_attempt_event(
    executor: Any,
    *,
    source_file_import_id: str,
    event_kind: str,
    outer_run: Mapping[str, Any],
    attempt_id: str | None = None,
    schema_name: str | None = None,
) -> str:
    """Append one durable action epoch after its outer state is present."""

    source_id = normalize_source_file_import_id(source_file_import_id)
    if event_kind not in _ACTION_KINDS:
        raise ValueError("PTG source-attempt event kind is invalid")
    if source_file_import_id_from_payload(outer_run, required=True) != source_id:
        raise ValueError("PTG source-attempt event identity changed")
    outer_run_id = str(outer_run.get("run_id") or "").strip()
    if not outer_run_id:
        raise ValueError("PTG source-attempt event requires outer run_id")
    normalized_attempt_id = str(attempt_id or "").strip() or None
    state_digest = canonical_digest(
        _event_state(
            outer_run,
            event_kind=event_kind,
            attempt_id=normalized_attempt_id,
        )
    )
    await _insert_action_event(
        executor,
        schema_name=resolve_ptg2_schema(schema_name),
        source_file_import_id=source_id,
        event_kind=event_kind,
        outer_run_id=outer_run_id,
        attempt_id=normalized_attempt_id,
        state_digest=state_digest,
    )
    return state_digest


async def _apply_state_updates(
    session: Any,
    *,
    outer_run_table: str,
    outer_run: dict[str, Any],
    run_id: str,
    state_updates: Mapping[str, Any] | None,
) -> None:
    if not state_updates:
        return
    invalid_fields = set(state_updates) - _ALLOWED_STATE_UPDATES
    if invalid_fields:
        raise ValueError("PTG source-attempt state update is not allowed")
    assignment_clauses = [
        (
            f"{_quote_ident(field_name)} = CAST(:{field_name} AS json)"
            if field_name == "progress"
            else f"{_quote_ident(field_name)} = :{field_name}"
        )
        for field_name in state_updates
    ]
    bind_values_by_name = {"run_id": run_id, **state_updates}
    if "progress" in bind_values_by_name:
        bind_values_by_name["progress"] = json.dumps(
            bind_values_by_name["progress"],
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
    await session.execute(
        text(
            f"UPDATE {outer_run_table} SET "
            + ", ".join(assignment_clauses)
            + " WHERE run_id = :run_id"
        ),
        bind_values_by_name,
    )
    outer_run.update(state_updates)


def _validate_locked_outer_run(
    outer_run: Mapping[str, Any],
    *,
    source_file_import_id: str,
    admission: _SourceActionAdmission,
) -> None:
    locked_source_id = source_file_import_id_from_payload(
        outer_run,
        required=True,
    )
    if locked_source_id != source_file_import_id:
        raise RuntimeError("PTG source-attempt identity changed")
    if (
        admission.expected_source_file_import_id is not _UNSET_SOURCE_ID
        and locked_source_id != admission.expected_source_file_import_id
    ):
        raise PTGSourceAttemptIdentityError(
            "queued PTG source-attempt identity changed"
        )
    if str(outer_run.get("status") or "").strip().lower() in (
        _TERMINAL_OUTER_STATUSES
    ):
        raise PTGSourceAttemptTerminalError(
            "terminal PTG source attempt cannot admit remote work"
        )
    _validate_worker_selection(outer_run, admission.worker_selection)


def _validate_worker_selection(
    outer_run: Mapping[str, Any],
    worker_selection: PTGWorkerActionSelection | None,
) -> None:
    if worker_selection is None:
        return
    persisted_importer = str(outer_run.get("importer") or "").strip()
    persisted_status = str(outer_run.get("status") or "").strip().lower()
    metrics = outer_run.get("metrics")
    required_role = (
        "finish"
        if persisted_status == "finalizing"
        or (
            persisted_status == "running"
            and persisted_importer != "ptg"
            and isinstance(metrics, Mapping)
            and type(metrics.get("total_chunks")) is int
            and metrics["total_chunks"] >= 0
        )
        else "start"
    )
    if (
        not persisted_importer
        or persisted_importer not in worker_selection.allowed_importers
        or required_role not in worker_selection.allowed_roles
        or (
            worker_selection.request_importer is not None
            and worker_selection.request_importer != persisted_importer
        )
    ):
        raise PTGSourceAttemptIdentityError(
            "worker selector does not match durable import run"
        )


async def _admit_source_backed_action(
    session: Any,
    *,
    outer_run_table: str,
    run_id: str,
    source_file_import_id: str,
    admission: _SourceActionAdmission,
) -> dict[str, Any] | None:
    """Lock, re-read, update, and append one source-backed action."""

    await require_source_attempt_capabilities(
        session,
        require_attempt_authority=False,
    )
    await guard_source_attempt(
        session,
        source_file_import_id=source_file_import_id,
    )
    outer_run = await _load_outer_run(
        session,
        outer_run_table=outer_run_table,
        run_id=run_id,
        lock_row=True,
    )
    if outer_run is None:
        return None
    _validate_locked_outer_run(
        outer_run,
        source_file_import_id=source_file_import_id,
        admission=admission,
    )
    await _apply_state_updates(
        session,
        outer_run_table=outer_run_table,
        outer_run=outer_run,
        run_id=run_id,
        state_updates=admission.state_updates,
    )
    await record_source_attempt_event(
        session,
        source_file_import_id=source_file_import_id,
        event_kind=admission.event_kind,
        outer_run=outer_run,
        attempt_id=admission.attempt_id,
    )
    return outer_run


async def admit_existing_outer_run_action(
    *,
    run_id: str,
    event_kind: str,
    attempt_id: str | None = None,
    state_updates: Mapping[str, Any] | None = None,
    expected_source_file_import_id: str | None | object = _UNSET_SOURCE_ID,
    worker_selection: PTGWorkerActionSelection | None = None,
) -> dict[str, Any] | None:
    """Commit one locked outer-state/action epoch before external work."""

    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        raise ValueError("run_id is required")
    admission = _SourceActionAdmission(
        event_kind=event_kind,
        attempt_id=attempt_id,
        state_updates=state_updates,
        expected_source_file_import_id=expected_source_file_import_id,
        worker_selection=worker_selection,
    )
    schema_name = resolve_ptg2_schema()
    outer_run_table = _schema_table(schema_name, "import_run")
    async with db.transaction() as session:
        outer_run = await _load_outer_run(
            session,
            outer_run_table=outer_run_table,
            run_id=normalized_run_id,
            lock_row=False,
        )
        if outer_run is None:
            return None
        _validate_worker_selection(outer_run, worker_selection)
        if str(outer_run.get("importer") or "") != "ptg":
            return outer_run
        source_id = source_file_import_id_from_payload(
            outer_run,
            required=False,
        )
        if (
            expected_source_file_import_id is not _UNSET_SOURCE_ID
            and source_id != expected_source_file_import_id
        ):
            raise PTGSourceAttemptIdentityError(
                "queued PTG source-attempt identity changed"
            )
        if source_id is None:
            return outer_run
        return await _admit_source_backed_action(
            session,
            outer_run_table=outer_run_table,
            run_id=normalized_run_id,
            source_file_import_id=source_id,
            admission=admission,
        )


__all__ = [
    "PTGSourceAttemptFencedError",
    "PTGSourceAttemptIdentityError",
    "PTGWorkerActionSelection",
    "PTGSourceAttemptTerminalError",
    "admit_existing_outer_run_action",
    "record_source_attempt_event",
    "source_file_import_id_from_payload",
]
