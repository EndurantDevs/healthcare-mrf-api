# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact recovery for one failed, unpublished PTG V4 physical layout."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping

from db.connection import db
from process.ptg_parts.ptg2_shared_gc import (
    _is_owned_v4_layout_locked,
    _v4_reachable_hashes,
    require_migration_owned_tables,
    resolve_ptg2_schema,
)
from process.ptg_parts.ptg2_v4_failed_layout_fence import (
    PTG2V4RecoveryConflict,
    abandon_writable_v4_layout,
)
from process.ptg_parts.ptg2_v4_failed_layout_state import (
    json_mapping,
    load_block_stats,
    load_recovery_postconditions,
    load_recovery_records,
    load_reference_counts,
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
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_SHARED_GENERATION,
    v4_layout_fingerprint,
)
@dataclass(frozen=True)
class _RecoveryContext:
    snapshot_id: str
    import_run_id: str
    snapshot_key: int
    build_token: str
    expected_report: dict[str, Any]
    plan_by_field: dict[str, Any]


def _has_no_references(count_by_name: Mapping[str, int]) -> bool:
    return all(
        int(count_by_name.get(count_name) or 0) == 0
        for count_name in (
            "bindings",
            "attestations",
            "global_pointers",
            "source_pointers",
            "plan_pointers",
            "pins",
            "scopes",
            "sources",
        )
    )


def _failed_owner_gate_map(
    *,
    snapshot_by_field: Mapping[str, Any],
    run_by_field: Mapping[str, Any],
    layout_by_field: Mapping[str, Any],
    report_by_field: Mapping[str, Any],
    owner_ids: tuple[str, str, int],
    count_by_name: Mapping[str, int],
    expected_fingerprint: bytes,
) -> dict[str, Any]:
    snapshot_id, import_run_id, snapshot_key = owner_ids
    observed_fingerprint = bytes(
        layout_by_field.get("semantic_fingerprint") or b""
    )
    return {
        "snapshot_failed": (
            snapshot_by_field.get("snapshot_id") == snapshot_id
            and snapshot_by_field.get("import_run_id") == import_run_id
            and snapshot_by_field.get("status") == "failed"
            and snapshot_by_field.get("published_at") is None
        ),
        "import_run_failed": (
            run_by_field.get("import_run_id") == import_run_id
            and run_by_field.get("status") == "failed"
        ),
        "report_matches_layout": (
            str(report_by_field.get("shared_snapshot_key") or "")
            == str(snapshot_key)
            and report_by_field.get("shared_layout_abandoned") is False
            and report_by_field.get("shared_layout_abandonment_deferred")
            is True
        ),
        "layout_owned_building_v4": (
            int(layout_by_field.get("snapshot_key") or 0) == snapshot_key
            and layout_by_field.get("generation") == PTG2_V4_SHARED_GENERATION
            and layout_by_field.get("state") == "building"
            and bool(layout_by_field.get("build_token"))
            and layout_by_field.get("published_at") is None
            and layout_by_field.get("root_state") in (None, "building")
        ),
        "fingerprint_matches_report": (
            len(observed_fingerprint) == 32
            and observed_fingerprint == expected_fingerprint
        ),
        "single_fingerprint": (
            int(count_by_name.get("fingerprints") or 0) == 1
        ),
        "unreferenced": _has_no_references(count_by_name),
    }


def _require_failed_owner(
    *,
    snapshot_by_field: Mapping[str, Any],
    run_by_field: Mapping[str, Any],
    layout_by_field: Mapping[str, Any],
    snapshot_id: str,
    import_run_id: str,
    snapshot_key: int,
    count_by_name: Mapping[str, int],
) -> dict[str, Any]:
    """Validate the failed logical owner and every no-reference fence."""

    report_by_field = json_mapping(run_by_field.get("report"))
    raw_fingerprint_hex = str(
        report_by_field.get("shared_semantic_fingerprint") or ""
    ).strip()
    try:
        expected_fingerprint = v4_layout_fingerprint(
            bytes.fromhex(raw_fingerprint_hex)
        )
    except ValueError as exc:
        raise PTG2V4RecoveryConflict(
            "failed import report has no valid shared semantic fingerprint"
        ) from exc
    gate_by_name = _failed_owner_gate_map(
        snapshot_by_field=snapshot_by_field,
        run_by_field=run_by_field,
        layout_by_field=layout_by_field,
        report_by_field=report_by_field,
        owner_ids=(snapshot_id, import_run_id, snapshot_key),
        count_by_name=count_by_name,
        expected_fingerprint=expected_fingerprint,
    )
    failed_gates = sorted(
        gate_name
        for gate_name, is_passed in gate_by_name.items()
        if not is_passed
    )
    if failed_gates:
        raise PTG2V4RecoveryConflict(
            "failed PTG V4 layout recovery gates did not pass: "
            + ", ".join(failed_gates)
        )
    return gate_by_name


async def _owner_records(
    executor: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    import_run_id: str,
    snapshot_key: int,
    lock_owned_layout: bool,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    owner_records = await load_recovery_records(
        executor,
        schema_name=schema_name,
        snapshot_id=snapshot_id,
        import_run_id=import_run_id,
        snapshot_key=snapshot_key,
        lock_logical_owner=lock_owned_layout,
    )
    if not lock_owned_layout:
        return owner_records
    layout_by_field = owner_records[2]
    try:
        is_locked = await _is_owned_v4_layout_locked(
            executor,
            schema_name=schema_name,
            snapshot_key=snapshot_key,
            build_token=str(layout_by_field.get("build_token") or ""),
        )
    except RuntimeError as exc:
        raise PTG2V4RecoveryConflict(str(exc)) from exc
    if not is_locked:
        raise PTG2V4RecoveryConflict(
            "failed PTG V4 layout ownership changed"
        )
    return await load_recovery_records(
        executor,
        schema_name=schema_name,
        snapshot_id=snapshot_id,
        import_run_id=import_run_id,
        snapshot_key=snapshot_key,
        lock_logical_owner=False,
    )


def _plan_digest(
    plan_by_field: Mapping[str, Any],
    build_token: str,
) -> str:
    digest_by_field = {
        **dict(plan_by_field),
        "build_token_sha256": hashlib.sha256(
            str(build_token).encode("utf-8")
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
    snapshot_id: str,
    import_run_id: str,
    snapshot_key: int,
    layout_by_field: Mapping[str, Any],
    count_by_name: Mapping[str, int],
    stats_by_name: Mapping[str, int],
    gate_by_name: Mapping[str, Any],
    build_token: str,
) -> dict[str, Any]:
    plan_by_field = {
        "contract": PTG2_V4_FAILED_LAYOUT_RECOVERY_CONTRACT,
        "snapshot_id": snapshot_id,
        "import_run_id": import_run_id,
        "snapshot_key": snapshot_key,
        "generation": str(layout_by_field["generation"]),
        "layout_state": str(layout_by_field["state"]),
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
        executor,
        schema_name=schema_name,
        snapshot_key=snapshot_key,
    )
    build_token = str(layout_by_field["build_token"])
    plan_by_field = _recovery_plan(
        snapshot_id=snapshot_id,
        import_run_id=import_run_id,
        snapshot_key=snapshot_key,
        layout_by_field=layout_by_field,
        count_by_name=count_by_name,
        stats_by_name=stats_by_name,
        gate_by_name=gate_by_name,
        build_token=build_token,
    )
    return _RecoveryContext(
        snapshot_id=snapshot_id,
        import_run_id=import_run_id,
        snapshot_key=snapshot_key,
        build_token=build_token,
        expected_report=json_mapping(run_by_field.get("report")),
        plan_by_field=plan_by_field,
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
        context = await _build_context(
            connection,
            schema_name=schema_name,
            snapshot_id=snapshot_id,
            import_run_id=import_run_id,
            snapshot_key=snapshot_key,
        )
    return context.plan_by_field


def _require_matching_digest(
    plan_by_field: Mapping[str, Any],
    normalized_digest: str,
) -> None:
    if plan_by_field["plan_digest"] != normalized_digest:
        raise PTG2V4RecoveryConflict(
            "failed PTG V4 layout recovery plan changed"
        )


async def _release_recovery_context(
    connection: Any,
    *,
    schema_name: str,
    context: _RecoveryContext,
) -> dict[str, Any]:
    abandonment = await abandon_writable_v4_layout(
        connection, schema_name=schema_name,
        snapshot_id=context.snapshot_id, import_run_id=context.import_run_id,
        snapshot_key=context.snapshot_key, build_token=context.build_token,
    )
    if abandonment.logical_layout_count != 1:
        raise PTG2V4RecoveryConflict(
            "failed PTG V4 layout ownership changed"
        )
    postcondition_by_name = await load_recovery_postconditions(
        connection,
        schema_name=schema_name,
        snapshot_key=context.snapshot_key,
    )
    if any(postcondition_by_name.values()):
        raise RuntimeError(
            "failed PTG V4 layout recovery left physical ownership rows"
        )
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
    return {
        **context.plan_by_field,
        "executed": True,
        "released_layouts": abandonment.logical_layout_count,
        "queued_candidate_hashes": abandonment.candidate_hash_count,
        "queued_candidate_stored_bytes": abandonment.stored_bytes,
        "postconditions": postcondition_by_name,
        "recovered_at": marker_by_field["recovered_at"],
    }


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
        context = await _build_context(
            connection,
            schema_name=schema_name,
            snapshot_id=snapshot_id,
            import_run_id=import_run_id,
            snapshot_key=snapshot_key,
            lock_owned_layout=True,
        )
        _require_matching_digest(context.plan_by_field, normalized_digest)
        return await _release_recovery_context(
            connection,
            schema_name=schema_name,
            context=context,
        )


__all__ = [
    "PTG2_V4_FAILED_LAYOUT_RECOVERY_CONTRACT",
    "PTG2V4RecoveryConflict",
    "plan_ptg2_v4_recovery",
    "recover_ptg2_v4_layout",
]
