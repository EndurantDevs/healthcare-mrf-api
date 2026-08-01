# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact PostgreSQL observation for legacy PTG V3 reconciliation."""

from __future__ import annotations

from typing import Any, Mapping

from sqlalchemy import text

from db.migration_ptg2_legacy_v3_metadata_reconcile import (
    AUDIT_TABLE,
    EVENT_TABLE,
)
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_legacy_v3_metadata_evidence import (
    load_attachment_evidence,
    load_dynamic_relation_evidence,
)
from process.ptg_parts.ptg_source_attempt_guard import canonical_digest


def _schema_table(schema_name: str, table_name: str) -> str:
    return f"{_quote_ident(schema_name)}.{_quote_ident(table_name)}"


def _mapping(row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    return dict(getattr(row, "_mapping", row))


async def _one_or_none(
    session: Any,
    statement: str,
    parameters_by_name: Mapping[str, Any],
) -> dict[str, Any] | None:
    result = await session.execute(text(statement), dict(parameters_by_name))
    return _mapping(result.one_or_none())


async def _all_rows(
    session: Any,
    statement: str,
    parameters_by_name: Mapping[str, Any],
) -> list[dict[str, Any]]:
    result = await session.execute(text(statement), dict(parameters_by_name))
    return [dict(row) for row in result.mappings().all()]


def _source_file_import_id(
    internal_run_row: Mapping[str, Any] | None,
) -> str:
    payload = (
        internal_run_row.get("payload")
        if isinstance(internal_run_row, Mapping)
        else None
    )
    options = payload.get("options") if isinstance(payload, Mapping) else None
    options_by_name = options if isinstance(options, Mapping) else {}
    return str(options_by_name.get("source_file_import_id") or "").strip()


async def _load_core_rows(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    internal_run_id: str,
    row_lock: str,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, list[dict[str, Any]]]:
    snapshot_table = _schema_table(schema_name, "ptg2_snapshot")
    internal_run_table = _schema_table(schema_name, "ptg2_import_run")
    snapshot_row = await _one_or_none(
        session,
        "SELECT to_jsonb(snapshot_row) AS payload, "
        f"xmin::text AS row_xmin FROM {snapshot_table} AS snapshot_row "
        "WHERE snapshot_id = :snapshot_id" + row_lock,
        {"snapshot_id": snapshot_id},
    )
    internal_run_row = await _one_or_none(
        session,
        "SELECT to_jsonb(run_row) AS payload, "
        f"xmin::text AS row_xmin FROM {internal_run_table} AS run_row "
        "WHERE import_run_id = :internal_run_id" + row_lock,
        {"internal_run_id": internal_run_id},
    )
    run_snapshot_rows = await _all_rows(
        session,
        f"SELECT snapshot_id, import_run_id, status FROM {snapshot_table} "
        "WHERE import_run_id = :internal_run_id ORDER BY snapshot_id"
        + row_lock,
        {"internal_run_id": internal_run_id},
    )
    return snapshot_row, internal_run_row, run_snapshot_rows


async def _load_outer_runs(
    session: Any,
    *,
    schema_name: str,
    source_file_import_id: str,
    row_lock: str,
) -> list[dict[str, Any]]:
    return await _all_rows(
        session,
        f"""
        SELECT run_id, importer, status, params, source_file_import_id,
               import_id, retry_of_run_id, created_at, started_at,
               finished_at, heartbeat_at, snapshot_id, metrics,
               outer_run_row.xmin::text AS row_xmin
          FROM {_schema_table(schema_name, 'import_run')} AS outer_run_row
         WHERE source_file_import_id = :source_file_import_id
            OR import_id = :source_file_import_id
            OR params::jsonb->>'source_file_import_id'
                = :source_file_import_id
            OR params::jsonb->>'import_id' = :source_file_import_id
            OR metrics::jsonb->>'source_file_import_id'
                = :source_file_import_id
            OR metrics::jsonb->>'import_id' = :source_file_import_id
         ORDER BY created_at, run_id
        """ + row_lock,
        {"source_file_import_id": source_file_import_id},
    )


async def _load_source_internal_rows(
    session: Any,
    *,
    schema_name: str,
    source_file_import_id: str,
    row_lock: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    internal_run_rows = await _all_rows(
        session,
        "SELECT to_jsonb(source_run) AS payload, "
        "source_run.xmin::text AS row_xmin FROM "
        f"{_schema_table(schema_name, 'ptg2_import_run')} AS source_run "
        "WHERE options::jsonb->>'source_file_import_id' "
        "= :source_file_import_id ORDER BY import_run_id" + row_lock,
        {"source_file_import_id": source_file_import_id},
    )
    internal_run_ids = sorted(
        str(row["payload"]["import_run_id"])
        for row in internal_run_rows
    )
    snapshot_rows = await _all_rows(
        session,
        "SELECT snapshot_id, import_run_id, status FROM "
        f"{_schema_table(schema_name, 'ptg2_snapshot')} "
        "WHERE import_run_id = ANY(CAST(:internal_run_ids AS text[])) "
        "ORDER BY import_run_id, snapshot_id" + row_lock,
        {"internal_run_ids": internal_run_ids},
    )
    return internal_run_rows, snapshot_rows


async def _load_control_mirrors(
    session: Any,
    *,
    control_schema_name: str,
    source_file_import_id: str,
    outer_run_ids: list[str],
    row_lock: str,
) -> list[dict[str, Any]]:
    return await _all_rows(
        session,
        f"""
        SELECT run_id, importer, status, params, snapshot_id, heartbeat_at,
               finished_at, synced_at, metrics,
               mirror_row.xmin::text AS row_xmin
          FROM {_schema_table(control_schema_name, 'run_mirror')} AS mirror_row
         WHERE params->>'source_file_import_id' = :source_file_import_id
            OR params->>'import_id' = :source_file_import_id
            OR metrics->>'source_file_import_id' = :source_file_import_id
            OR metrics->>'import_id' = :source_file_import_id
            OR run_id = ANY(CAST(:outer_run_ids AS text[]))
         ORDER BY created_at, run_id
        """ + row_lock,
        {
            "source_file_import_id": source_file_import_id,
            "outer_run_ids": outer_run_ids,
        },
    )


async def _load_outer_runs_and_mirrors(
    session: Any,
    *,
    schema_name: str,
    control_schema_name: str,
    source_file_import_id: str,
    row_lock: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    outer_run_rows = await _load_outer_runs(
        session,
        schema_name=schema_name,
        source_file_import_id=source_file_import_id,
        row_lock=row_lock,
    )
    mirror_rows = await _load_control_mirrors(
        session,
        control_schema_name=control_schema_name,
        source_file_import_id=source_file_import_id,
        outer_run_ids=sorted(
            str(outer_run_record["run_id"])
            for outer_run_record in outer_run_rows
        ),
        row_lock=row_lock,
    )
    return outer_run_rows, mirror_rows


async def _load_source_control_rows(
    session: Any,
    *,
    control_schema_name: str,
    source_file_import_id: str,
    row_lock: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    source_import_rows = await _all_rows(
        session,
        "SELECT to_jsonb(source_row) AS payload, xmin::text AS row_xmin "
        f"FROM {_schema_table(control_schema_name, 'source_file_import')} "
        "AS source_row WHERE source_file_import_id = :source_file_import_id"
        + row_lock,
        {"source_file_import_id": source_file_import_id},
    )
    placement_rows = await _all_rows(
        session,
        "SELECT to_jsonb(placement_row) AS payload, "
        "xmin::text AS row_xmin FROM "
        f"{_schema_table(control_schema_name, 'ptg_file_placement')} "
        "AS placement_row WHERE source_file_import_id "
        "= :source_file_import_id" + row_lock,
        {"source_file_import_id": source_file_import_id},
    )
    return source_import_rows, placement_rows


async def _load_event_rows(
    session: Any,
    *,
    schema_name: str,
    source_file_import_id: str,
    row_lock: str,
) -> list[dict[str, Any]]:
    return await _all_rows(
        session,
        "SELECT event_id, event_kind, outer_run_id, attempt_id, "
        f"state_digest, created_at FROM {_schema_table(schema_name, EVENT_TABLE)} "
        "WHERE source_file_import_id = :source_file_import_id "
        "ORDER BY event_id" + row_lock,
        {"source_file_import_id": source_file_import_id},
    )


async def _load_audit_row(
    session: Any,
    *,
    schema_name: str,
    source_file_import_id: str,
    snapshot_id: str,
    internal_run_id: str,
    row_lock: str,
) -> dict[str, Any] | None:
    return await _one_or_none(
        session,
        "SELECT to_jsonb(audit_row) AS payload, xmin::text AS row_xmin "
        f"FROM {_schema_table(schema_name, AUDIT_TABLE)} AS audit_row "
        "WHERE source_file_import_id = :source_file_import_id "
        "OR snapshot_id = :snapshot_id "
        "OR internal_run_id = :internal_run_id" + row_lock,
        {
            "source_file_import_id": source_file_import_id,
            "snapshot_id": snapshot_id,
            "internal_run_id": internal_run_id,
        },
    )


async def _load_source_linked_rows(
    session: Any,
    *,
    schema_name: str,
    control_schema_name: str,
    source_file_import_id: str,
    snapshot_id: str,
    internal_run_id: str,
    row_lock: str,
) -> dict[str, Any]:
    """Load all rows joined to one source-file import coordinate."""

    source_internal_rows, source_snapshot_rows = (
        await _load_source_internal_rows(
            session,
            schema_name=schema_name,
            source_file_import_id=source_file_import_id,
            row_lock=row_lock,
        )
    )
    outer_run_rows, mirror_rows = await _load_outer_runs_and_mirrors(
        session,
        schema_name=schema_name,
        control_schema_name=control_schema_name,
        source_file_import_id=source_file_import_id,
        row_lock=row_lock,
    )
    source_import_rows, placement_rows = await _load_source_control_rows(
        session,
        control_schema_name=control_schema_name,
        source_file_import_id=source_file_import_id,
        row_lock=row_lock,
    )
    event_rows = await _load_event_rows(
        session,
        schema_name=schema_name,
        source_file_import_id=source_file_import_id,
        row_lock=row_lock,
    )
    audit_row = await _load_audit_row(
        session,
        schema_name=schema_name,
        source_file_import_id=source_file_import_id,
        snapshot_id=snapshot_id,
        internal_run_id=internal_run_id,
        row_lock=row_lock,
    )
    return {
        "source_internal_runs": source_internal_rows,
        "source_snapshots": source_snapshot_rows,
        "outer_runs": outer_run_rows,
        "control_run_mirrors": mirror_rows,
        "source_import_rows": source_import_rows,
        "placement_rows": placement_rows,
        "event_rows": event_rows,
        "audit": audit_row,
    }


async def _load_attempt_rows(
    session: Any,
    *,
    schema_name: str,
    control_schema_name: str,
    snapshot_id: str,
    internal_run_id: str,
    row_lock: str,
) -> dict[str, Any]:
    snapshot_row, internal_run_row, run_snapshot_rows = await _load_core_rows(
        session,
        schema_name=schema_name,
        snapshot_id=snapshot_id,
        internal_run_id=internal_run_id,
        row_lock=row_lock,
    )
    source_file_import_id = _source_file_import_id(internal_run_row)
    linked_rows = await _load_source_linked_rows(
        session,
        schema_name=schema_name,
        control_schema_name=control_schema_name,
        source_file_import_id=source_file_import_id,
        snapshot_id=snapshot_id,
        internal_run_id=internal_run_id,
        row_lock=row_lock,
    )
    return {
        "snapshot": snapshot_row,
        "internal_run": internal_run_row,
        "run_snapshots": run_snapshot_rows,
        "source_file_import_id": source_file_import_id,
        **linked_rows,
    }


async def _load_attachment_payload(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    internal_run_id: str,
) -> dict[str, Any]:
    count_by_name, retained_rows_by_name, catalog_digest = (
        await load_attachment_evidence(
            session,
            schema_name=schema_name,
            snapshot_id=snapshot_id,
            internal_run_id=internal_run_id,
        )
    )
    dynamic_relations = await load_dynamic_relation_evidence(
        session,
        schema_name=schema_name,
        internal_run_id=internal_run_id,
    )
    return {
        "attachment_counts": count_by_name,
        "attachment_rows": retained_rows_by_name,
        "attachment_digest": canonical_digest(retained_rows_by_name),
        "catalog_digest": catalog_digest,
        "dynamic_relations": dynamic_relations,
    }


def _finalize_observation(
    attempt_rows: Mapping[str, Any],
    *,
    outer_run_id: str,
) -> dict[str, Any]:
    event_rows = attempt_rows.get("event_rows")
    events = event_rows if isinstance(event_rows, list) else []
    outer_runs = attempt_rows.get("outer_runs")
    outer_run_rows = outer_runs if isinstance(outer_runs, list) else []
    return {
        **attempt_rows,
        "outer_target": next(
            (row for row in outer_run_rows if row["run_id"] == outer_run_id),
            None,
        ),
        "event_high_water_mark": max(
            (int(row["event_id"]) for row in events),
            default=0,
        ),
        "event_digest": canonical_digest(events),
    }


async def load_legacy_v3_reconcile_observation(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    internal_run_id: str,
    outer_run_id: str,
    control_schema_name: str,
    lock_rows: bool,
) -> dict[str, Any]:
    """Load exact database evidence for one reviewed V3 target."""

    attempt_rows = await _load_attempt_rows(
        session,
        schema_name=schema_name,
        control_schema_name=control_schema_name,
        snapshot_id=snapshot_id,
        internal_run_id=internal_run_id,
        row_lock=" FOR UPDATE" if lock_rows else "",
    )
    attachment_payload = await _load_attachment_payload(
        session,
        schema_name=schema_name,
        snapshot_id=snapshot_id,
        internal_run_id=internal_run_id,
    )
    return _finalize_observation(
        {**attempt_rows, **attachment_payload},
        outer_run_id=outer_run_id,
    )


__all__ = ["load_legacy_v3_reconcile_observation"]
