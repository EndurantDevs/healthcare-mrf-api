# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact logical and physical ownership gates for failed V4 recovery."""

from __future__ import annotations

import re
from typing import Any, Mapping

from process.ptg_parts.ptg2_shared_gc import (
    _is_owned_v4_layout_locked,
    _owned_v4_abandonment_token,
)
from process.ptg_parts.ptg2_v4_failed_layout_fence import (
    PTG2V4RecoveryConflict,
)
from process.ptg_parts.ptg2_v4_failed_layout_state import (
    _REFERENCE_FENCE_NAMES,
    _has_finalizer_map_tables,
    json_mapping,
    load_recovery_records,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_SHARED_GENERATION,
    v4_layout_fingerprint,
)


def _has_no_references(count_by_name: Mapping[str, int]) -> bool:
    return all(
        int(count_by_name.get(count_name) or 0) == 0
        for count_name in _REFERENCE_FENCE_NAMES
    )


def _require_recovery_build_token(raw_token: Any) -> str:
    """Accept only generated build tokens or the reserved recovery marker."""

    token = str(raw_token or "")
    if re.fullmatch(r"[0-9a-f]{32}", token):
        return token
    if _owned_v4_abandonment_token(token) == token:
        return token
    raise PTG2V4RecoveryConflict("owned PTG V4 build token namespace is invalid")


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
    observed_fingerprint = bytes(layout_by_field.get("semantic_fingerprint") or b"")
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
            str(report_by_field.get("shared_snapshot_key") or "") == str(snapshot_key)
            and report_by_field.get("shared_layout_abandoned") is False
            and report_by_field.get("shared_layout_abandonment_deferred") is True
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
        "single_fingerprint": int(count_by_name.get("fingerprints") or 0) == 1,
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
        expected_fingerprint = v4_layout_fingerprint(bytes.fromhex(raw_fingerprint_hex))
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
        gate_name for gate_name, is_passed in gate_by_name.items() if not is_passed
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
    """Load exact logical records and optionally lock their V4 layout."""

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
        finalizer_tables_available = await _has_finalizer_map_tables(
            executor,
            schema_name,
        )
        is_locked = await _is_owned_v4_layout_locked(
            executor,
            schema_name=schema_name,
            snapshot_key=snapshot_key,
            build_token=str(layout_by_field.get("build_token") or ""),
            finalizer_tables_available=finalizer_tables_available,
        )
    except RuntimeError as exc:
        raise PTG2V4RecoveryConflict(str(exc)) from exc
    if not is_locked:
        raise PTG2V4RecoveryConflict("failed PTG V4 layout ownership changed")
    return await load_recovery_records(
        executor,
        schema_name=schema_name,
        snapshot_id=snapshot_id,
        import_run_id=import_run_id,
        snapshot_key=snapshot_key,
        lock_logical_owner=False,
    )


__all__ = []
