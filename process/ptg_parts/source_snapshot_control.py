# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Targeted control helpers for source-scoped PTG2 snapshots."""

from __future__ import annotations

import os
from typing import Any

from sqlalchemy import text

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_candidate_attestation import (
    CandidateAttestationApprovalConflict,
    parse_candidate_attestation_digest,
)
from process.ptg_parts.ptg2_shared_gc import release_unbound_ptg2_shared_layouts
from process.ptg_parts.snapshot_cleanup import is_shared_snapshot_control_manifest
from process.ptg_parts.source_snapshot_control_lifecycle import (
    lock_source_pointer_gc as _lock_source_pointer_gc,
)
from process.ptg_parts.source_snapshot_control_policy import (
    SUPPORTED_SHARED_SNAPSHOT_CONTROL_MESSAGE,
    manifest_dict,
    retirement_manifest_source_key,
    snapshot_remove_reasons,
)
from process.ptg_parts.source_pointers import (
    PTG2SourcePointerConflict,
    activate_ptg2_source_candidate,
)
from process.ptg_parts.source_snapshot_control_results import (
    executed_empty_remove_plan as _executed_empty_remove_plan,
    executed_snapshot_remove_plan as _executed_snapshot_remove_plan,
    missing_snapshot_remove_plan as _missing_snapshot_remove_plan,
    supported_snapshot_remove_plan as _supported_snapshot_remove_plan,
    unsupported_snapshot_remove_plan as _unsupported_snapshot_remove_plan,
)
from process.ptg_parts.source_snapshot_references import (
    load_snapshot_reference_rows,
    reference_string_values,
)
from process.ptg_parts.source_snapshot_shared_layout import (
    bound_shared_layout_keys,
    validate_retirement_shared_layout,
)

class SourceSnapshotConflict(ValueError):
    """Raised when a source snapshot pointer changed between plan and execute."""


class _TransactionExecutor:
    """Expose the current SQLAlchemy transaction through the shared-GC interface."""

    def __init__(self, session: Any):
        self._session = session

    async def all(self, statement: Any, **params: Any) -> list[Any]:
        """Execute a statement and return all result rows."""
        result = await self._session.execute(
            text(statement) if isinstance(statement, str) else statement,
            params,
        )
        return result.all()

    async def status(self, statement: Any, **params: Any) -> int | None:
        """Execute a statement and return its affected row count."""
        result = await self._session.execute(
            text(statement) if isinstance(statement, str) else statement,
            params,
        )
        return getattr(result, "rowcount", None)

def _schema_name() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _row_mapping(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return row
    return dict(getattr(row, "_mapping", row))


async def promote_ptg2_source_snapshot(
    *,
    source_key: str,
    snapshot_id: str,
    expected_current_snapshot_id: str | None = None,
    expected_audit_only_attestation_digest: str | None = None,
    rollback_owner_id: str | None = None,
) -> dict[str, Any]:
    """Activate one audited strict-V3 candidate and all of its live pointers."""
    source_key = str(source_key or "").strip().lower()
    snapshot_id = str(snapshot_id or "").strip()
    if not source_key or not snapshot_id:
        raise ValueError("source_key and snapshot_id are required")
    approval_digest = (
        parse_candidate_attestation_digest(
            expected_audit_only_attestation_digest
        )
        if expected_audit_only_attestation_digest is not None
        else None
    )
    try:
        promotion_by_field = await activate_ptg2_source_candidate(
            source_key=source_key,
            snapshot_id=snapshot_id,
            expected_current_snapshot_id=expected_current_snapshot_id,
            expected_audit_only_attestation_digest=approval_digest,
            rollback_owner_id=rollback_owner_id,
        )
    except (
        CandidateAttestationApprovalConflict,
        PTG2SourcePointerConflict,
    ) as exc:
        raise SourceSnapshotConflict(str(exc)) from exc
    _clear_ptg2_snapshot_cache()
    return promotion_by_field


async def build_source_snapshot_remove_plan(
    *,
    snapshot_id: str,
    source_key: str | None = None,
) -> dict[str, Any]:
    """Describe whether a source-scoped snapshot can be safely removed."""
    snapshot_id = str(snapshot_id or "").strip()
    source_key = str(source_key or "").strip() or None
    if not snapshot_id:
        raise ValueError("snapshot_id is required")
    schema = _schema_name()
    snapshot = await _snapshot_row(schema, snapshot_id)
    if not snapshot:
        return _missing_snapshot_remove_plan(snapshot_id, source_key)
    manifest = manifest_dict(snapshot.get("manifest"))
    serving_index = manifest.get("serving_index") if isinstance(manifest.get("serving_index"), dict) else {}
    storage_generation = str(
        serving_index.get("storage_generation") or ""
    ).strip().lower()
    if not is_shared_snapshot_control_manifest(serving_index):
        return _unsupported_snapshot_remove_plan(
            snapshot_id=snapshot_id,
            source_key=source_key,
            snapshot=snapshot,
            storage_generation=storage_generation,
            reason=f"{SUPPORTED_SHARED_SNAPSHOT_CONTROL_MESSAGE} can be removed",
        )
    manifest_source_key = str(serving_index.get("source_key") or "").strip() or None
    references = await _current_references(schema, snapshot_id)
    artifact_ids = await _artifact_manifest_ids(schema, snapshot_id)
    snapshot_status = str(snapshot.get("status") or "").strip().lower()
    reasons = snapshot_remove_reasons(
        source_key=source_key,
        manifest_source_key=manifest_source_key,
        manifest_snapshot_key=serving_index.get("shared_snapshot_key"),
        snapshot_status=snapshot_status,
        references=references,
    )
    return _supported_snapshot_remove_plan(
        snapshot_id=snapshot_id,
        source_key=source_key or manifest_source_key,
        snapshot=snapshot,
        serving_index=serving_index,
        storage_generation=storage_generation,
        references=references,
        artifact_ids=artifact_ids,
        reasons=reasons,
    )


async def _delete_snapshot_metadata(
    schema: str,
    snapshot_id: str,
    artifact_ids: list[str],
) -> dict[str, int]:
    """Delete snapshot-owned metadata while the removal transaction is active."""

    count_by_field = {
        "deleted_v3_snapshot_scopes": int(
            await db.status(
                f"DELETE FROM {_quote_ident(schema)}.ptg2_v3_snapshot_scope "
                "WHERE snapshot_id = :snapshot_id",
                snapshot_id=snapshot_id,
            )
            or 0
        ),
        "deleted_v3_snapshot_bindings": int(
            await db.status(
                f"DELETE FROM {_quote_ident(schema)}.ptg2_v3_snapshot_binding "
                "WHERE snapshot_id = :snapshot_id",
                snapshot_id=snapshot_id,
            )
            or 0
        ),
    }
    count_by_field["deleted_artifact_chunks"] = 0
    if artifact_ids:
        count_by_field["deleted_artifact_chunks"] = int(
            await db.status(
                f"DELETE FROM {_quote_ident(schema)}.ptg2_artifact_blob_chunk "
                "WHERE artifact_id = ANY(:artifact_ids)",
                artifact_ids=artifact_ids,
            )
            or 0
        )
    for count_name, table_name in (
        ("deleted_artifact_manifests", "ptg2_artifact_manifest"),
        ("deleted_snapshots", "ptg2_snapshot"),
    ):
        count_by_field[count_name] = int(
            await db.status(
                f"DELETE FROM {_quote_ident(schema)}.{table_name} "
                "WHERE snapshot_id = :snapshot_id",
                snapshot_id=snapshot_id,
            )
            or 0
        )
    return count_by_field


async def _release_removed_snapshot_layout(
    session: Any,
    *,
    schema: str,
    layout_keys: tuple[int, ...],
    deleted_binding_count: int,
) -> Any | None:
    """Release a shared layout only after its final binding disappears."""

    if deleted_binding_count <= 0 or not layout_keys:
        return None
    return await release_unbound_ptg2_shared_layouts(
        schema_name=schema,
        executor=_TransactionExecutor(session),
        require_shared=True,
        layout_keys=layout_keys,
    )


async def remove_ptg2_source_snapshot(
    *,
    snapshot_id: str,
    source_key: str | None = None,
) -> dict[str, Any]:
    """Remove an unreferenced source snapshot after validating its removal plan."""
    snapshot_id = str(snapshot_id or "").strip()
    source_key = str(source_key or "").strip() or None
    if not snapshot_id:
        raise ValueError("snapshot_id is required")
    schema = _schema_name()
    async with db.transaction() as session:
        await _lock_source_pointer_gc(
            session,
            source_key=source_key or f"snapshot_{snapshot_id}",
        )
        plan = await build_source_snapshot_remove_plan(
            snapshot_id=snapshot_id,
            source_key=source_key,
        )
        if not plan.get("removable"):
            raise ValueError(str(plan.get("reason") or "snapshot is not removable"))
        if not plan.get("exists"):
            return _executed_empty_remove_plan(plan)
        storage_generation = str(
            plan.get("storage_generation") or ""
        ).strip().lower()
        layout_keys = await bound_shared_layout_keys(
            session,
            schema=schema,
            snapshot_id=snapshot_id,
            expected_generation=storage_generation,
            expected_snapshot_key=plan.get("shared_snapshot_key"),
            allow_missing_binding=str(plan.get("status") or "").strip().lower()
            == "failed",
        )
        artifact_ids = [
            str(artifact_id)
            for artifact_id in plan.get("artifact_manifest_ids") or []
        ]
        deletion_counts = await _delete_snapshot_metadata(
            schema,
            snapshot_id,
            artifact_ids,
        )
        shared_layout_release = await _release_removed_snapshot_layout(
            session,
            schema=schema,
            layout_keys=layout_keys,
            deleted_binding_count=deletion_counts[
                "deleted_v3_snapshot_bindings"
            ],
        )
    return _executed_snapshot_remove_plan(
        plan=plan,
        deletion_counts=deletion_counts,
        layout_keys=layout_keys,
        shared_layout_release=shared_layout_release,
    )


async def retire_ptg2_source_snapshot(
    *,
    snapshot_id: str,
    source_key: str | None = None,
) -> dict[str, Any]:
    """Retire one source-scoped PTG2 snapshot and delete its serving artifacts."""
    snapshot_id = str(snapshot_id or "").strip()
    source_key = str(source_key or "").strip() or None
    if not snapshot_id:
        raise ValueError("snapshot_id is required")
    schema = _schema_name()
    async with db.transaction() as session:
        await _lock_source_pointer_gc(
            session,
            source_key=source_key or f"snapshot_{snapshot_id}",
        )
        snapshot = await _snapshot_row(schema, snapshot_id)
        manifest_source_key = retirement_manifest_source_key(snapshot, source_key)
        await validate_retirement_shared_layout(
            session,
            schema=schema,
            snapshot_id=snapshot_id,
            snapshot=snapshot,
        )
        before = await _current_references(schema, snapshot_id)
        if before.get("global_slots"):
            raise ValueError("snapshot is referenced by current global pointer")
        if any(
            before.get(reference_name)
            for reference_name in (
                "previous_global_slots",
                "previous_source_keys",
                "previous_plan_source_keys",
            )
        ):
            raise ValueError("snapshot is referenced by a previous snapshot pointer")
        deleted_plan_pointers, deleted_source_pointers = (
            await _delete_retired_source_pointers(
                schema,
                snapshot_id=snapshot_id,
                source_key=source_key,
            )
        )
        after = await _current_references(schema, snapshot_id)
    _clear_ptg2_snapshot_cache()
    return {
        "snapshot_id": snapshot_id,
        "source_key": source_key or manifest_source_key,
        "exists": bool(snapshot),
        "retired": True,
        "deleted_plan_pointers": int(deleted_plan_pointers or 0),
        "deleted_source_pointers": int(deleted_source_pointers or 0),
        "previous_current_references": before,
        "current_references": after,
    }


async def _delete_retired_source_pointers(
    schema: str,
    *,
    snapshot_id: str,
    source_key: str | None,
) -> tuple[int, int]:
    query_param_map: dict[str, Any] = {"snapshot_id": snapshot_id}
    source_filter = ""
    if source_key:
        query_param_map["source_key"] = source_key
        source_filter = " AND source_key = :source_key"
    table_names = (
        "ptg2_current_plan_source",
        "ptg2_current_source_snapshot",
    )
    deleted_counts = []
    for table_name in table_names:
        deleted_count = await db.status(
            f"""
            DELETE FROM {_quote_ident(schema)}.{table_name}
             WHERE snapshot_id = :snapshot_id{source_filter}
            """,
            **query_param_map,
        )
        deleted_counts.append(int(deleted_count or 0))
    return deleted_counts[0], deleted_counts[1]


async def _snapshot_row(schema: str, snapshot_id: str) -> dict[str, Any]:
    rows = await db.all(
        f"""
        SELECT snapshot_id, import_month, status, manifest
          FROM {_quote_ident(schema)}.ptg2_snapshot
         WHERE snapshot_id = :snapshot_id
         LIMIT 1
        """,
        snapshot_id=snapshot_id,
    )
    return _row_mapping(rows[0]) if rows else {}


async def _current_source_snapshot_state(schema: str, source_key: str) -> tuple[str | None, str | None]:
    row = await db.first(
        f"SELECT snapshot_id, previous_snapshot_id FROM {_quote_ident(schema)}.ptg2_current_source_snapshot "
        "WHERE source_key = :source_key LIMIT 1",
        source_key=source_key,
    )
    if row is None:
        return None, None
    return (str(row[0]) if row[0] else None, str(row[1]) if row[1] else None)


async def _snapshot_reference_rows(
    schema: str,
    snapshot_id: str,
    *,
    table: str,
    selected_fields: str,
    reference_field: str = "snapshot_id",
    order_fields: str | None = None,
) -> list[Any]:
    return await db.all(
        f"SELECT {selected_fields} FROM {_quote_ident(schema)}.{table} "
        f"WHERE {reference_field} = :snapshot_id "
        f"ORDER BY {order_fields or selected_fields}",
        snapshot_id=snapshot_id,
    )


async def _current_references(schema: str, snapshot_id: str) -> dict[str, list[str]]:
    """Return every current pointer or release pin retaining a snapshot."""

    reference_rows_by_kind = await load_snapshot_reference_rows(
        schema,
        snapshot_id,
        _snapshot_reference_rows,
    )
    return {
        "global_slots": reference_string_values(
            reference_rows_by_kind["global"], "slot", _row_mapping
        ),
        "source_keys": reference_string_values(
            reference_rows_by_kind["source"], "source_key", _row_mapping
        ),
        "plan_source_keys": reference_string_values(
            reference_rows_by_kind["plan"],
            "plan_source_key",
            _row_mapping,
        ),
        "previous_global_slots": reference_string_values(
            reference_rows_by_kind["previous_global"],
            "slot",
            _row_mapping,
        ),
        "previous_source_keys": reference_string_values(
            reference_rows_by_kind["previous_source"],
            "source_key",
            _row_mapping,
        ),
        "previous_plan_source_keys": reference_string_values(
            reference_rows_by_kind["previous_plan"],
            "plan_source_key",
            _row_mapping,
        ),
        "plan_release_pins": [
            f"{_row_mapping(reference_row).get('owner_type')}:"
            f"{_row_mapping(reference_row).get('owner_id')}"
            for reference_row in reference_rows_by_kind["pin"]
        ],
        "plan_release_bindings": [
            f"{_row_mapping(reference_row).get('serving_revision_id')}:"
            f"{_row_mapping(reference_row).get('role')}:"
            f"{_row_mapping(reference_row).get('binding_ordinal')}"
            for reference_row in reference_rows_by_kind["release_binding"]
        ],
    }


async def _artifact_manifest_ids(schema: str, snapshot_id: str) -> list[str]:
    rows = await db.all(
        f"""
        SELECT artifact_id
          FROM {_quote_ident(schema)}.ptg2_artifact_manifest
         WHERE snapshot_id = :snapshot_id
         ORDER BY artifact_id
        """,
        snapshot_id=snapshot_id,
    )
    return [str(_row_mapping(row).get("artifact_id")) for row in rows]


def _clear_ptg2_snapshot_cache() -> None:
    try:
        from api.ptg2_snapshot import _PTG2_SNAPSHOT_RESOLVE_CACHE
    except Exception:
        return
    _PTG2_SNAPSHOT_RESOLVE_CACHE.clear()
