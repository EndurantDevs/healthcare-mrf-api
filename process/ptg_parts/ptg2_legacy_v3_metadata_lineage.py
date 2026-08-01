# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed outer-control lineage for legacy V3 repair."""

from __future__ import annotations

from typing import Any, Mapping

from process.ptg_parts.ptg_source_attempt_guard import (
    source_file_import_id_from_payload,
)


_FAILED_OUTER_STATUSES = frozenset(
    {"failed", "canceled", "cancelled", "dead_letter"}
)
_FAILED_SOURCE_IMPORT_STATUS = "failed"


def _payload(row_envelope: Any) -> Mapping[str, Any]:
    if not isinstance(row_envelope, Mapping):
        return {}
    payload_by_field = row_envelope.get("payload")
    return (
        payload_by_field
        if isinstance(payload_by_field, Mapping)
        else {}
    )


def _exact_source_identity(
    run_by_field: Mapping[str, Any],
) -> tuple[str | None, bool]:
    try:
        source_file_import_id = source_file_import_id_from_payload(
            run_by_field,
            required=False,
        )
    except ValueError:
        return None, True
    return source_file_import_id, False


def _outer_run_reasons(
    run_rows: list[Mapping[str, Any]],
    *,
    source_file_import_id: str,
    outer_run_id: str,
    snapshot_id: str,
) -> tuple[list[str], set[str], dict[str, str]]:
    reasons: list[str] = []
    run_ids = {
        str(run_by_field.get("run_id") or "").strip()
        for run_by_field in run_rows
        if str(run_by_field.get("run_id") or "").strip()
    }
    if len(run_ids) != len(run_rows) or not run_ids:
        reasons.append("outer_run_cardinality_changed")
    for run_by_field in run_rows:
        run_source_id, has_invalid_identity = _exact_source_identity(
            run_by_field
        )
        if str(run_by_field.get("importer") or "") != "ptg":
            reasons.append("outer_source_importer_changed")
        if has_invalid_identity or run_source_id != source_file_import_id:
            reasons.append("outer_source_identity_changed")
        if (
            str(run_by_field.get("status") or "").strip().lower()
            not in _FAILED_OUTER_STATUSES
        ):
            reasons.append("outer_source_attempt_not_failed")
        if run_by_field.get("finished_at") is None:
            reasons.append("outer_source_attempt_not_finished")
        if run_by_field.get("snapshot_id") not in (None, snapshot_id):
            reasons.append("outer_source_snapshot_changed")
    reasons.extend(
        _retry_lineage_reasons(
            run_rows,
            run_ids=run_ids,
            outer_run_id=outer_run_id,
        )
    )
    status_by_run_id = {
        str(run_by_field.get("run_id") or "").strip(): str(
            run_by_field.get("status") or ""
        ).strip().lower()
        for run_by_field in run_rows
    }
    return reasons, run_ids, status_by_run_id


def _retry_lineage_reasons(
    run_rows: list[Mapping[str, Any]],
    *,
    run_ids: set[str],
    outer_run_id: str,
) -> list[str]:
    """Require one connected, acyclic retry chain ending at the target."""

    parent_by_run_id = {
        str(run_by_field.get("run_id") or "").strip(): str(
            run_by_field.get("retry_of_run_id") or ""
        ).strip()
        for run_by_field in run_rows
        if str(run_by_field.get("retry_of_run_id") or "").strip()
    }
    reasons: list[str] = []
    if set(parent_by_run_id.values()) - run_ids:
        reasons.append("outer_retry_lineage_incomplete")
    child_count_by_run_id: dict[str, int] = {}
    for parent_run_id in parent_by_run_id.values():
        child_count_by_run_id[parent_run_id] = (
            child_count_by_run_id.get(parent_run_id, 0) + 1
        )
    roots = run_ids - set(parent_by_run_id)
    has_branch = any(count > 1 for count in child_count_by_run_id.values())
    has_cycle = False
    for run_id in run_ids:
        seen_run_ids: set[str] = set()
        cursor = run_id
        while cursor in parent_by_run_id:
            if cursor in seen_run_ids:
                has_cycle = True
                break
            seen_run_ids.add(cursor)
            cursor = parent_by_run_id[cursor]
    if len(roots) != 1 or has_branch or has_cycle:
        reasons.append("outer_retry_lineage_not_single_chain")
    leaves = run_ids - set(parent_by_run_id.values())
    if leaves != {outer_run_id}:
        reasons.append("outer_target_not_retry_leaf")
    return reasons


def _outer_target_reasons(
    target_by_field: Any,
    *,
    outer_run_id: str,
    source_file_import_id: str,
) -> list[str]:
    if not isinstance(target_by_field, Mapping):
        return ["outer_target_missing"]
    reasons: list[str] = []
    if target_by_field.get("run_id") != outer_run_id:
        reasons.append("outer_target_identity_changed")
    if str(target_by_field.get("importer") or "") != "ptg":
        reasons.append("outer_target_not_ptg")
    if str(target_by_field.get("status") or "").lower() != "failed":
        reasons.append("outer_target_not_failed")
    if target_by_field.get("finished_at") is None:
        reasons.append("outer_target_not_finished")
    target_source_id, has_invalid_identity = _exact_source_identity(
        target_by_field
    )
    if has_invalid_identity or target_source_id != source_file_import_id:
        reasons.append("outer_target_source_changed")
    return reasons


def _control_mirror_reasons(
    mirror_rows: list[Mapping[str, Any]],
    *,
    outer_run_ids: set[str],
    outer_status_by_run_id: Mapping[str, str],
    source_file_import_id: str,
    snapshot_id: str,
) -> list[str]:
    reasons: list[str] = []
    mirror_run_ids = {
        str(mirror_by_field.get("run_id") or "").strip()
        for mirror_by_field in mirror_rows
        if str(mirror_by_field.get("run_id") or "").strip()
    }
    if mirror_run_ids != outer_run_ids or len(mirror_run_ids) != len(
        mirror_rows
    ):
        reasons.append("control_mirror_lineage_changed")
    for mirror_by_field in mirror_rows:
        mirror_source_id, has_invalid_identity = _exact_source_identity(
            mirror_by_field
        )
        if str(mirror_by_field.get("importer") or "") != "ptg":
            reasons.append("control_mirror_importer_changed")
        if has_invalid_identity or (
            mirror_source_id is not None
            and mirror_source_id != source_file_import_id
        ):
            reasons.append("control_mirror_source_changed")
        if (
            str(mirror_by_field.get("status") or "").strip().lower()
            not in _FAILED_OUTER_STATUSES
        ):
            reasons.append("control_mirror_not_failed")
        mirror_run_id = str(mirror_by_field.get("run_id") or "").strip()
        mirror_status = str(
            mirror_by_field.get("status") or ""
        ).strip().lower()
        if outer_status_by_run_id.get(mirror_run_id) != mirror_status:
            reasons.append("control_mirror_status_changed")
        if mirror_by_field.get("finished_at") is None:
            reasons.append("control_mirror_not_finished")
        if mirror_by_field.get("snapshot_id") not in (None, snapshot_id):
            reasons.append("control_mirror_snapshot_changed")
    return reasons


def _source_import_reasons(
    source_import_envelopes: list[Mapping[str, Any]],
    *,
    source_file_import_id: str,
    outer_run_id: str,
    snapshot_id: str,
) -> list[str]:
    if len(source_import_envelopes) != 1:
        return ["source_import_cardinality_changed"]
    source_import_by_field = _payload(source_import_envelopes[0])
    reasons: list[str] = []
    if (
        source_import_by_field.get("source_file_import_id")
        != source_file_import_id
        or source_import_by_field.get("engine_run_id") != outer_run_id
        or source_import_by_field.get("snapshot_id")
        not in (None, snapshot_id)
        or source_import_by_field.get("removed_at") is not None
    ):
        reasons.append("source_import_lineage_changed")
    if (
        str(source_import_by_field.get("status") or "").strip().lower()
        != _FAILED_SOURCE_IMPORT_STATUS
    ):
        reasons.append("source_import_not_failed")
    return reasons


def _source_event_reasons(
    event_rows: list[Mapping[str, Any]],
    *,
    outer_run_ids: set[str],
) -> list[str]:
    """Bind every durable action event to the reviewed outer retry chain."""

    for event_by_field in event_rows:
        outer_run_id = event_by_field.get("outer_run_id")
        if (
            not isinstance(outer_run_id, str)
            or not outer_run_id
            or outer_run_id != outer_run_id.strip()
            or outer_run_id not in outer_run_ids
        ):
            return ["source_event_outer_lineage_changed"]
    return []


def _source_control_reasons(
    observation: Mapping[str, Any],
    *,
    outer_run_ids: set[str],
    source_file_import_id: str,
    outer_run_id: str,
    snapshot_id: str,
) -> list[str]:
    source_imports = observation.get("source_import_rows")
    source_import_envelopes = (
        source_imports if isinstance(source_imports, list) else []
    )
    reasons = _source_import_reasons(
        source_import_envelopes,
        source_file_import_id=source_file_import_id,
        outer_run_id=outer_run_id,
        snapshot_id=snapshot_id,
    )
    event_rows = observation.get("event_rows")
    if not isinstance(event_rows, list):
        reasons.append("source_event_lineage_changed")
    elif any(not isinstance(event, Mapping) for event in event_rows):
        reasons.append("source_event_lineage_changed")
    else:
        reasons.extend(
            _source_event_reasons(event_rows, outer_run_ids=outer_run_ids)
        )
    placements = observation.get("placement_rows")
    if isinstance(placements, list) and placements:
        reasons.append("file_placement_present")
    return reasons


def legacy_v3_outer_lineage_reasons(
    observation: Mapping[str, Any],
    *,
    outer_run_id: str,
    source_file_import_id: str,
    snapshot_id: str,
) -> list[str]:
    """Validate exact terminal outer, mirror, source, and retry lineage."""

    outer_runs = observation.get("outer_runs")
    run_rows = outer_runs if isinstance(outer_runs, list) else []
    reasons, outer_run_ids, outer_status_by_run_id = _outer_run_reasons(
        run_rows,
        source_file_import_id=source_file_import_id,
        outer_run_id=outer_run_id,
        snapshot_id=snapshot_id,
    )
    reasons.extend(
        _outer_target_reasons(
            observation.get("outer_target"),
            outer_run_id=outer_run_id,
            source_file_import_id=source_file_import_id,
        )
    )
    mirrors = observation.get("control_run_mirrors")
    mirror_rows = mirrors if isinstance(mirrors, list) else []
    reasons.extend(
        _control_mirror_reasons(
            mirror_rows,
            outer_run_ids=outer_run_ids,
            outer_status_by_run_id=outer_status_by_run_id,
            source_file_import_id=source_file_import_id,
            snapshot_id=snapshot_id,
        )
    )
    reasons.extend(
        _source_control_reasons(
            observation,
            outer_run_ids=outer_run_ids,
            source_file_import_id=source_file_import_id,
            outer_run_id=outer_run_id,
            snapshot_id=snapshot_id,
        )
    )
    return reasons


__all__ = ["legacy_v3_outer_lineage_reasons"]
