# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact recovery for one failed, unpublished PTG V4 physical layout."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping

from db.connection import db
from process.ptg_parts.ptg2_shared_gc import (
    PTG2SharedLayoutGCStats,
    _owned_v4_abandonment_token,
    _v4_reachable_hashes,
    require_migration_owned_tables,
    resolve_ptg2_schema,
)
from process.ptg_parts.ptg2_v4_failed_layout_fence import (
    PTG2V4RecoveryConflict,
    abandon_writable_v4_layout,
    load_recovery_attempt_fence,
    lock_active_recovery_fence,
    require_active_recovery_fence,
    seal_recovery_attempt_fence,
)
from process.ptg_parts.ptg2_v4_failed_layout_state import (
    json_mapping,
    load_block_stats,
    load_recovery_postconditions,
    load_reference_counts,
)
from process.ptg_parts.ptg2_v4_failed_layout_owner import (
    _owner_records,
    _require_failed_owner,
    _require_recovery_build_token,
)
from process.ptg_parts.ptg2_v4_failed_layout_marker import (
    PTG2_V4_FAILED_LAYOUT_RECOVERY_CONTRACT,
    RecoveryMarkerWrite,
    load_completed_recovery_result,
    persist_recovery_marker,
)
from process.ptg_parts.ptg2_v4_failed_layout_request import (
    lock_recovery_pointer_state,
    normalize_plan_digest,
    normalize_recovery_request,
)
from process.ptg_parts.ptg2_v4_stale_metadata_json import canonical_json_digest
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_SHARED_GENERATION,
)


@dataclass(frozen=True)
class _RecoveryContext:
    snapshot_id: str
    import_run_id: str
    snapshot_key: int
    build_token: str
    expected_report: dict[str, Any]
    plan_by_field: dict[str, Any]
    fence_nonce: str
    fence_created_at: Any


def _plan_digest(
    plan_by_field: Mapping[str, Any],
    build_token: str,
) -> str:
    digest_by_field = {
        **dict(plan_by_field),
        "abandonment_token_sha256": hashlib.sha256(
            _owned_v4_abandonment_token(build_token).encode("utf-8")
        ).hexdigest(),
    }
    encoded = json.dumps(
        digest_by_field,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


async def _verified_block_stats(
    executor: Any,
    *,
    schema_name: str,
    snapshot_key: int,
) -> dict[str, int]:
    v4_hashes = await _v4_reachable_hashes(
        executor,
        schema_name=schema_name,
        snapshot_keys=(snapshot_key,),
    )
    stats_by_name = await load_block_stats(
        executor,
        schema_name=schema_name,
        snapshot_key=snapshot_key,
        v4_hashes=v4_hashes,
    )
    if stats_by_name["candidate_hashes"] != stats_by_name["resolved_hashes"]:
        raise PTG2V4RecoveryConflict(
            "failed PTG V4 layout references missing CAS blocks"
        )
    return stats_by_name


def _recovery_plan(
    *,
    owner_ids: tuple[str, str, int],
    layout_by_field: Mapping[str, Any],
    count_by_name: Mapping[str, int],
    stats_by_name: Mapping[str, int],
    gate_by_name: Mapping[str, Any],
    build_token: str,
    fence_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    snapshot_id, import_run_id, snapshot_key = owner_ids
    plan_by_field = {
        "contract": PTG2_V4_FAILED_LAYOUT_RECOVERY_CONTRACT,
        "snapshot_id": snapshot_id,
        "import_run_id": import_run_id,
        "snapshot_key": snapshot_key,
        "target_digest": canonical_json_digest(
            {
                "contract": PTG2_V4_FAILED_LAYOUT_RECOVERY_CONTRACT,
                "snapshot_id": snapshot_id,
                "import_run_id": import_run_id,
                "snapshot_key": snapshot_key,
            }
        ),
        "generation": str(layout_by_field["generation"]),
        "layout_state": str(layout_by_field["state"]),
        "semantic_fingerprint_sha256": hashlib.sha256(
            bytes(layout_by_field["semantic_fingerprint"])
        ).hexdigest(),
        "root_state": layout_by_field.get("root_state"),
        "representation": layout_by_field.get("representation"),
        "created_at": layout_by_field.get("created_at"),
        "heartbeat_at": layout_by_field.get("heartbeat_at"),
        "lease_until": layout_by_field.get("lease_until"),
        "reference_counts": dict(count_by_name),
        "candidate_hash_count": stats_by_name["candidate_hashes"],
        "candidate_stored_bytes": stats_by_name["stored_bytes"],
        "gates": dict(gate_by_name),
        "cas_payloads_deleted": 0,
        "candidate_metrics_scope": "layout_reachability",
        "attempt_fence": {
            "nonce_sha256": hashlib.sha256(
                str(fence_by_field["fence_nonce"]).encode("utf-8")
            ).hexdigest(),
            "created_at": fence_by_field["created_at"],
        },
        "executable": True,
    }
    plan_by_field["plan_digest"] = _plan_digest(plan_by_field, build_token)
    return plan_by_field


async def _build_context(
    executor: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    import_run_id: str,
    snapshot_key: int,
    fence_by_field: Mapping[str, Any],
    lock_owned_layout: bool = False,
) -> _RecoveryContext:
    """Build one token-bound plan from authenticated logical and CAS state."""

    await require_migration_owned_tables(executor, schema_name)
    snapshot_by_field, run_by_field, layout_by_field = await _owner_records(
        executor,
        schema_name=schema_name,
        snapshot_id=snapshot_id,
        import_run_id=import_run_id,
        snapshot_key=snapshot_key,
        lock_owned_layout=lock_owned_layout,
    )
    count_by_name = await load_reference_counts(
        executor,
        schema_name=schema_name,
        snapshot_id=snapshot_id,
        snapshot_key=snapshot_key,
    )
    gate_by_name = _require_failed_owner(
        snapshot_by_field=snapshot_by_field,
        run_by_field=run_by_field,
        layout_by_field=layout_by_field,
        snapshot_id=snapshot_id,
        import_run_id=import_run_id,
        snapshot_key=snapshot_key,
        count_by_name=count_by_name,
    )
    stats_by_name = await _verified_block_stats(
        executor, schema_name=schema_name, snapshot_key=snapshot_key
    )
    build_token = _require_recovery_build_token(layout_by_field["build_token"])
    plan_by_field = _recovery_plan(
        owner_ids=(snapshot_id, import_run_id, snapshot_key),
        layout_by_field=layout_by_field,
        count_by_name=count_by_name,
        stats_by_name=stats_by_name,
        gate_by_name=gate_by_name,
        build_token=build_token,
        fence_by_field=fence_by_field,
    )
    return _RecoveryContext(
        snapshot_id=snapshot_id,
        import_run_id=import_run_id,
        snapshot_key=snapshot_key,
        build_token=build_token,
        expected_report=json_mapping(run_by_field.get("report")),
        plan_by_field=plan_by_field,
        fence_nonce=str(fence_by_field["fence_nonce"]),
        fence_created_at=fence_by_field["created_at"],
    )


async def plan_ptg2_v4_recovery(
    *,
    snapshot_id: str,
    import_run_id: str,
    snapshot_key: int,
    schema_name: str | None = None,
) -> dict[str, Any]:
    """Build a read-only exact plan without exposing the physical build token."""

    snapshot_id, import_run_id, snapshot_key = normalize_recovery_request(
        snapshot_id=snapshot_id,
        import_run_id=import_run_id,
        snapshot_key=snapshot_key,
    )
    schema_name = resolve_ptg2_schema(schema_name)
    async with db.acquire() as connection:
        await require_migration_owned_tables(connection, schema_name)
        completed = await load_completed_recovery_result(
            connection,
            schema_name=schema_name,
            snapshot_id=snapshot_id,
            import_run_id=import_run_id,
            snapshot_key=snapshot_key,
        )
        if completed is not None:
            return completed
        fence_by_field = require_active_recovery_fence(
            await load_recovery_attempt_fence(
                connection,
                schema_name=schema_name,
                snapshot_id=snapshot_id,
                import_run_id=import_run_id,
            ),
            snapshot_id=snapshot_id,
            import_run_id=import_run_id,
        )
        context = await _build_context(
            connection,
            schema_name=schema_name,
            snapshot_id=snapshot_id,
            import_run_id=import_run_id,
            fence_by_field=fence_by_field,
            snapshot_key=snapshot_key,
        )
    return context.plan_by_field


def _require_matching_digest(
    plan_by_field: Mapping[str, Any],
    normalized_digest: str,
) -> None:
    if plan_by_field["plan_digest"] != normalized_digest:
        raise PTG2V4RecoveryConflict("failed PTG V4 layout recovery plan changed")


async def _guard_recovery_step(
    connection: Any,
    *,
    schema_name: str,
    context: _RecoveryContext,
) -> None:
    """Revalidate the exact logical owner before every committed batch."""

    snapshot_by_field, run_by_field, layout_by_field = await _owner_records(
        connection,
        schema_name=schema_name,
        snapshot_id=context.snapshot_id,
        import_run_id=context.import_run_id,
        snapshot_key=context.snapshot_key,
        lock_owned_layout=True,
    )
    if json_mapping(run_by_field.get("report")) != context.expected_report:
        raise PTG2V4RecoveryConflict(
            "failed PTG V4 layout recovery owner report changed"
        )
    count_by_name = await load_reference_counts(
        connection,
        schema_name=schema_name,
        snapshot_id=context.snapshot_id,
        snapshot_key=context.snapshot_key,
    )
    _require_failed_owner(
        snapshot_by_field=snapshot_by_field,
        run_by_field=run_by_field,
        layout_by_field=layout_by_field,
        snapshot_id=context.snapshot_id,
        import_run_id=context.import_run_id,
        snapshot_key=context.snapshot_key,
        count_by_name=count_by_name,
    )
    if str(layout_by_field.get("build_token") or "") not in {
        context.build_token,
        _owned_v4_abandonment_token(context.build_token),
    }:
        raise PTG2V4RecoveryConflict("failed PTG V4 layout ownership changed")


async def _finalize_recovery_step(
    connection: Any,
    *,
    schema_name: str,
    context: _RecoveryContext,
    abandonment: PTG2SharedLayoutGCStats,
) -> None:
    """Persist the marker and seal its attempt fence with layout deletion."""

    if abandonment.logical_layout_count != 1:
        raise PTG2V4RecoveryConflict("failed PTG V4 layout ownership changed")
    postcondition_by_name = await load_recovery_postconditions(
        connection,
        schema_name=schema_name,
        snapshot_key=context.snapshot_key,
    )
    if any(postcondition_by_name.values()):
        raise RuntimeError("failed PTG V4 layout recovery left physical ownership rows")
    marker_by_field = await persist_recovery_marker(
        connection,
        schema_name=schema_name,
        marker_write=RecoveryMarkerWrite(
            snapshot_id=context.snapshot_id,
            import_run_id=context.import_run_id,
            snapshot_key=context.snapshot_key,
            expected_report_by_field=context.expected_report,
            plan_by_field=context.plan_by_field,
            released_layouts=abandonment.logical_layout_count,
            queued_candidate_hashes=abandonment.candidate_hash_count,
            queued_candidate_stored_bytes=abandonment.stored_bytes,
            postcondition_by_name=postcondition_by_name,
        ),
    )
    await seal_recovery_attempt_fence(
        connection,
        schema_name=schema_name,
        snapshot_id=context.snapshot_id,
        import_run_id=context.import_run_id,
        expected_fence_nonce=context.fence_nonce,
        expected_fence_created_at=context.fence_created_at,
        marker_by_field=marker_by_field,
    )


async def _release_recovery_context(
    *,
    schema_name: str,
    context: _RecoveryContext,
) -> dict[str, Any]:
    """Run restartable cleanup and reload its sealed durable evidence."""

    abandonment = await abandon_writable_v4_layout(
        schema_name=schema_name,
        snapshot_id=context.snapshot_id,
        import_run_id=context.import_run_id,
        snapshot_key=context.snapshot_key,
        build_token=context.build_token,
        expected_fence_by_field={
            "fence_nonce": context.fence_nonce,
            "created_at": context.fence_created_at,
        },
        step_guard=lambda connection: _guard_recovery_step(
            connection,
            schema_name=schema_name,
            context=context,
        ),
        finalize_callback=lambda connection, stats: _finalize_recovery_step(
            connection,
            schema_name=schema_name,
            context=context,
            abandonment=stats,
        ),
    )
    if abandonment.logical_layout_count != 1:
        raise PTG2V4RecoveryConflict("failed PTG V4 layout ownership changed")
    async with db.acquire() as connection:
        completed = await load_completed_recovery_result(
            connection,
            schema_name=schema_name,
            snapshot_id=context.snapshot_id,
            import_run_id=context.import_run_id,
            snapshot_key=context.snapshot_key,
        )
    if completed is None:
        raise RuntimeError(
            "failed PTG V4 layout recovery did not seal durable evidence"
        )
    return completed


async def recover_ptg2_v4_layout(
    *,
    snapshot_id: str,
    import_run_id: str,
    snapshot_key: int,
    expected_plan_digest: str,
    schema_name: str | None = None,
) -> dict[str, Any]:
    """Recompute, compare, and execute one exact failed-layout recovery plan."""

    snapshot_id, import_run_id, snapshot_key = normalize_recovery_request(
        snapshot_id=snapshot_id,
        import_run_id=import_run_id,
        snapshot_key=snapshot_key,
    )
    schema_name = resolve_ptg2_schema(schema_name)
    normalized_digest = normalize_plan_digest(expected_plan_digest)
    async with db.acquire() as connection:
        await lock_recovery_pointer_state(connection)
        await require_migration_owned_tables(connection, schema_name)
        completed = await load_completed_recovery_result(
            connection,
            schema_name=schema_name,
            snapshot_id=snapshot_id,
            import_run_id=import_run_id,
            snapshot_key=snapshot_key,
        )
        if completed is not None:
            _require_matching_digest(completed, normalized_digest)
            return completed
        fence_by_field = await lock_active_recovery_fence(
            connection,
            schema_name=schema_name,
            snapshot_id=snapshot_id,
            import_run_id=import_run_id,
        )
        context = await _build_context(
            connection,
            schema_name=schema_name,
            snapshot_id=snapshot_id,
            import_run_id=import_run_id,
            snapshot_key=snapshot_key,
            fence_by_field=fence_by_field,
            lock_owned_layout=True,
        )
        _require_matching_digest(context.plan_by_field, normalized_digest)
    return await _release_recovery_context(
        schema_name=schema_name,
        context=context,
    )


__all__ = [
    "PTG2_V4_FAILED_LAYOUT_RECOVERY_CONTRACT",
    "PTG2V4RecoveryConflict",
    "plan_ptg2_v4_recovery",
    "recover_ptg2_v4_layout",
]
