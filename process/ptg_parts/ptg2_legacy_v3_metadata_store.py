# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL evidence and CAS writes for legacy PTG V3 reconciliation."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping

from sqlalchemy import text

from db.migration_ptg2_legacy_v3_metadata_reconcile import (
    AUDIT_TABLE,
    CAPABILITY_TABLE,
    EVENT_TABLE,
    LEGACY_V3_RECONCILE_CONTRACT,
)
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_legacy_v3_metadata_evidence import (
    ALLOWED_ATTACHMENT_NAMES,
    has_relation,
)
from process.ptg_parts.ptg2_legacy_v3_metadata_observation import (
    load_legacy_v3_reconcile_observation,
)
from process.ptg_parts.ptg2_v4_attempt_registry import ATTEMPT_ATTACHMENTS


@dataclass(frozen=True)
class LegacyV3ReconcileWrite:
    """Exact immutable values for the two-row CAS and audit fence."""

    schema_name: str
    snapshot_id: str
    internal_run_id: str
    source_file_import_id: str
    outer_run_id: str
    target_digest: str
    plan_digest: str
    attachment_digest: str
    catalog_digest: str
    event_high_water_mark: int
    reconciliation_id: str
    marker: Mapping[str, Any]


def _schema_table(schema_name: str, table_name: str) -> str:
    return (
        f"{_quote_ident(schema_name)}.{_quote_ident(table_name)}"
    )


def _mapping(row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    return dict(getattr(row, "_mapping", row))


async def _one_or_none(
    session: Any,
    statement: str,
    parameters: Mapping[str, Any],
) -> dict[str, Any] | None:
    result = await session.execute(text(statement), dict(parameters))
    return _mapping(result.one_or_none())


async def _all(
    session: Any,
    statement: str,
    parameters: Mapping[str, Any],
) -> list[dict[str, Any]]:
    result = await session.execute(text(statement), dict(parameters))
    return [dict(row) for row in result.mappings().all()]


async def lock_legacy_v3_reconcile_relations(
    session: Any,
    *,
    schema_name: str,
    control_schema_name: str,
) -> None:
    """Block phantom attachments while one reviewed transition commits."""

    mrf_tables = [
        "ptg2_snapshot",
        "ptg2_import_run",
        EVENT_TABLE,
        AUDIT_TABLE,
        CAPABILITY_TABLE,
    ]
    for attachment in ATTEMPT_ATTACHMENTS:
        if await has_relation(
            session,
            schema_name,
            attachment.table_name,
        ):
            mrf_tables.append(attachment.table_name)
    control_tables = ["run_mirror", "source_file_import", "ptg_file_placement"]
    qualified_tables = [
        *(
            _schema_table(schema_name, table_name)
            for table_name in dict.fromkeys(mrf_tables)
        ),
        *(
            _schema_table(control_schema_name, table_name)
            for table_name in control_tables
        ),
    ]
    await session.execute(
        text(
            "LOCK TABLE "
            + ", ".join(qualified_tables)
            + " IN SHARE ROW EXCLUSIVE MODE"
        )
    )


async def apply_legacy_v3_reconcile_rows(
    session: Any,
    write: LegacyV3ReconcileWrite,
) -> None:
    """CAS two stale metadata rows and append the terminal fence last."""

    await _update_snapshot_row(session, write)
    await _update_internal_run_row(session, write)
    await _insert_reconcile_audit(session, write)


async def _update_snapshot_row(
    session: Any,
    write: LegacyV3ReconcileWrite,
) -> None:
    snapshot_update_result = await session.execute(
        text(
            f"""
            UPDATE {_schema_table(write.schema_name, 'ptg2_snapshot')}
               SET status = 'failed'
             WHERE snapshot_id = :snapshot_id
               AND import_run_id = :internal_run_id
               AND status = 'building'
               AND validated_at IS NULL
               AND published_at IS NULL
               AND COALESCE(manifest::jsonb, '{{}}'::jsonb) = '{{}}'::jsonb
            """
        ),
        {
            "snapshot_id": write.snapshot_id,
            "internal_run_id": write.internal_run_id,
        },
    )
    if snapshot_update_result.rowcount != 1:
        raise RuntimeError("legacy V3 snapshot CAS changed")


async def _update_internal_run_row(
    session: Any,
    write: LegacyV3ReconcileWrite,
) -> None:
    result = await session.execute(
        text(
            f"""
            UPDATE {_schema_table(write.schema_name, 'ptg2_import_run')}
               SET status = 'failed',
                   finished_at = statement_timestamp(),
                   heartbeat_at = statement_timestamp()
             WHERE import_run_id = :internal_run_id
               AND status IN ('queued', 'starting', 'running', 'finalizing')
               AND finished_at IS NULL
            """
        ),
        {"internal_run_id": write.internal_run_id},
    )
    if result.rowcount != 1:
        raise RuntimeError("legacy V3 internal-run CAS changed")


async def _insert_reconcile_audit(
    session: Any,
    write: LegacyV3ReconcileWrite,
) -> None:
    audit_insert_result = await session.execute(
        text(
            f"""
            INSERT INTO {_schema_table(write.schema_name, AUDIT_TABLE)} (
                reconciliation_id,
                contract,
                source_file_import_id,
                snapshot_id,
                internal_run_id,
                outer_run_id,
                target_digest,
                plan_digest,
                attachment_digest,
                catalog_digest,
                event_high_water_mark,
                marker
            )
            VALUES (
                :reconciliation_id,
                :contract,
                :source_file_import_id,
                :snapshot_id,
                :internal_run_id,
                :outer_run_id,
                :target_digest,
                :plan_digest,
                :attachment_digest,
                :catalog_digest,
                :event_high_water_mark,
                CAST(:marker AS jsonb)
            )
            """
        ),
        {
            "reconciliation_id": write.reconciliation_id,
            "contract": LEGACY_V3_RECONCILE_CONTRACT,
            "source_file_import_id": write.source_file_import_id,
            "snapshot_id": write.snapshot_id,
            "internal_run_id": write.internal_run_id,
            "outer_run_id": write.outer_run_id,
            "target_digest": write.target_digest,
            "plan_digest": write.plan_digest,
            "attachment_digest": write.attachment_digest,
            "catalog_digest": write.catalog_digest,
            "event_high_water_mark": write.event_high_water_mark,
            "marker": json.dumps(
                dict(write.marker),
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ),
        },
    )
    if audit_insert_result.rowcount != 1:
        raise RuntimeError("legacy V3 reconciliation audit insert changed")


__all__ = [
    "ALLOWED_ATTACHMENT_NAMES",
    "LegacyV3ReconcileWrite",
    "apply_legacy_v3_reconcile_rows",
    "load_legacy_v3_reconcile_observation",
    "lock_legacy_v3_reconcile_relations",
]
