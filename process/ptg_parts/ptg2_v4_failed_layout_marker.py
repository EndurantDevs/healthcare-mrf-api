# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable, replay-safe evidence for exact PTG V4 layout recovery."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_v4_failed_layout_state import (
    json_mapping,
    load_recovery_postconditions,
    load_recovery_records,
    load_reference_counts,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_SHARED_GENERATION,
)


PTG2_V4_FAILED_LAYOUT_RECOVERY_CONTRACT = (
    "ptg_v4_failed_layout_recovery_v1"
)

_REFERENCE_FENCE_NAMES = (
    "bindings",
    "attestations",
    "global_pointers",
    "source_pointers",
    "plan_pointers",
    "pins",
    "scopes",
    "sources",
)
_PHYSICAL_COUNT_NAMES = (
    "fingerprints",
    "mapping_rows",
    "map_packs",
    "relation_manifests",
)
_POSTCONDITION_NAMES = {
    "layouts",
    "fingerprints",
    "mappings",
    "map_roots",
    "map_packs",
    "relation_manifests",
    "dense_rows",
}


@dataclass(frozen=True)
class RecoveryMarkerWrite:
    """Inputs persisted atomically after one exact layout release."""

    snapshot_id: str
    import_run_id: str
    snapshot_key: int
    expected_report_by_field: Mapping[str, Any]
    plan_by_field: Mapping[str, Any]
    released_layouts: int
    queued_candidate_hashes: int
    queued_candidate_stored_bytes: int
    postcondition_by_name: Mapping[str, int]


def _normalized_digest(raw_digest: Any) -> str | None:
    digest = str(raw_digest or "").strip().lower()
    try:
        decoded = bytes.fromhex(digest)
    except ValueError:
        return None
    return digest if len(decoded) == 32 else None


def _non_negative_int(raw_value: Any) -> int | None:
    try:
        normalized_count = int(raw_value)
    except (TypeError, ValueError):
        return None
    return normalized_count if normalized_count >= 0 else None


def _valid_recovered_at(raw_value: Any) -> str | None:
    timestamp_text = str(raw_value or "").strip()
    if not timestamp_text:
        return None
    try:
        parsed_at = datetime.fromisoformat(
            timestamp_text.replace("Z", "+00:00")
        )
    except ValueError:
        return None
    return timestamp_text if parsed_at.tzinfo is not None else None


def _normalized_postconditions(
    marker_by_field: Mapping[str, Any],
) -> dict[str, Any] | None:
    postcondition_by_name = json_mapping(
        marker_by_field.get("postconditions")
    )
    if set(postcondition_by_name) != _POSTCONDITION_NAMES:
        return None
    normalized_count_by_name = {
        name: _non_negative_int(raw_count)
        for name, raw_count in postcondition_by_name.items()
    }
    if any(count != 0 for count in normalized_count_by_name.values()):
        return None
    return postcondition_by_name


def _completed_marker(
    *,
    report_by_field: Mapping[str, Any],
    snapshot_id: str,
    import_run_id: str,
    snapshot_key: int,
) -> dict[str, Any] | None:
    marker_by_field = json_mapping(
        report_by_field.get("shared_layout_recovery")
    )
    digest = _normalized_digest(marker_by_field.get("plan_digest"))
    recovered_at = _valid_recovered_at(marker_by_field.get("recovered_at"))
    candidate_hash_count = _non_negative_int(
        marker_by_field.get("queued_candidate_hashes")
    )
    candidate_stored_bytes = _non_negative_int(
        marker_by_field.get("queued_candidate_stored_bytes")
    )
    cas_payloads_deleted = _non_negative_int(
        marker_by_field.get("cas_payloads_deleted")
    )
    marker_snapshot_key = _non_negative_int(
        marker_by_field.get("snapshot_key")
    )
    released_layouts = _non_negative_int(
        marker_by_field.get("released_layouts")
    )
    postcondition_by_name = _normalized_postconditions(marker_by_field)
    if (
        report_by_field.get("shared_layout_abandoned") is not True
        or report_by_field.get("shared_layout_abandonment_deferred") is not False
        or str(report_by_field.get("shared_snapshot_key") or "")
        != str(snapshot_key)
        or marker_by_field.get("contract")
        != PTG2_V4_FAILED_LAYOUT_RECOVERY_CONTRACT
        or marker_by_field.get("snapshot_id") != snapshot_id
        or marker_by_field.get("import_run_id") != import_run_id
        or marker_snapshot_key != snapshot_key
        or digest is None
        or recovered_at is None
        or candidate_hash_count is None
        or candidate_stored_bytes is None
        or released_layouts != 1
        or cas_payloads_deleted != 0
        or postcondition_by_name is None
    ):
        return None
    return {
        **marker_by_field,
        "plan_digest": digest,
        "recovered_at": recovered_at,
        "queued_candidate_hashes": candidate_hash_count,
        "queued_candidate_stored_bytes": candidate_stored_bytes,
        "postconditions": postcondition_by_name,
    }


def _idempotent_response(
    *,
    marker_by_field: Mapping[str, Any],
    snapshot_id: str,
    import_run_id: str,
    snapshot_key: int,
    count_by_name: Mapping[str, int],
    postcondition_by_name: Mapping[str, int],
) -> dict[str, Any]:
    return {
        "contract": PTG2_V4_FAILED_LAYOUT_RECOVERY_CONTRACT,
        "snapshot_id": snapshot_id,
        "import_run_id": import_run_id,
        "snapshot_key": snapshot_key,
        "generation": PTG2_V4_SHARED_GENERATION,
        "layout_state": "released",
        "root_state": None,
        "representation": marker_by_field.get("representation"),
        "reference_counts": dict(count_by_name),
        "candidate_hash_count": marker_by_field[
            "queued_candidate_hashes"
        ],
        "candidate_stored_bytes": marker_by_field[
            "queued_candidate_stored_bytes"
        ],
        "gates": {
            "durable_recovery_marker": True,
            "physical_ownership_released": True,
            "unreferenced": True,
        },
        "cas_payloads_deleted": 0,
        "plan_digest": marker_by_field["plan_digest"],
        "executable": False,
        "executed": True,
        "idempotent": True,
        "released_layouts": 1,
        "queued_candidate_hashes": marker_by_field[
            "queued_candidate_hashes"
        ],
        "queued_candidate_stored_bytes": marker_by_field[
            "queued_candidate_stored_bytes"
        ],
        "postconditions": dict(postcondition_by_name),
        "recovered_at": marker_by_field["recovered_at"],
    }


def completed_recovery_result(
    *,
    snapshot_by_field: Mapping[str, Any],
    run_by_field: Mapping[str, Any],
    snapshot_id: str,
    import_run_id: str,
    snapshot_key: int,
    count_by_name: Mapping[str, int],
    postconditions_by_name: Mapping[str, int],
) -> dict[str, Any] | None:
    """Return authenticated idempotent evidence only after every zero gate."""
    marker_by_field = _completed_marker(
        report_by_field=json_mapping(run_by_field.get("report")),
        snapshot_id=snapshot_id,
        import_run_id=import_run_id,
        snapshot_key=snapshot_key,
    )
    if (
        marker_by_field is None
        or snapshot_by_field.get("snapshot_id") != snapshot_id
        or snapshot_by_field.get("import_run_id") != import_run_id
        or snapshot_by_field.get("status") != "failed"
        or snapshot_by_field.get("published_at") is not None
        or run_by_field.get("import_run_id") != import_run_id
        or run_by_field.get("status") != "failed"
        or any(int(count_by_name.get(name) or 0) for name in _REFERENCE_FENCE_NAMES)
        or any(int(count_by_name.get(name) or 0) for name in _PHYSICAL_COUNT_NAMES)
        or any(
            int(count or 0) for count in postconditions_by_name.values()
        )
    ):
        return None
    return _idempotent_response(
        marker_by_field=marker_by_field,
        snapshot_id=snapshot_id,
        import_run_id=import_run_id,
        snapshot_key=snapshot_key,
        count_by_name=count_by_name,
        postcondition_by_name=postconditions_by_name,
    )


async def load_completed_recovery_result(
    executor: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    import_run_id: str,
    snapshot_key: int,
) -> dict[str, Any] | None:
    """Load replay evidence only when every physical owner row is gone."""

    snapshot_by_field, run_by_field, layout_by_field = (
        await load_recovery_records(
            executor,
            schema_name=schema_name,
            snapshot_id=snapshot_id,
            import_run_id=import_run_id,
            snapshot_key=snapshot_key,
        )
    )
    if layout_by_field:
        return None
    count_by_name = await load_reference_counts(
        executor,
        schema_name=schema_name,
        snapshot_id=snapshot_id,
        snapshot_key=snapshot_key,
    )
    postcondition_by_name = await load_recovery_postconditions(
        executor,
        schema_name=schema_name,
        snapshot_key=snapshot_key,
    )
    return completed_recovery_result(
        snapshot_by_field=snapshot_by_field,
        run_by_field=run_by_field,
        snapshot_id=snapshot_id,
        import_run_id=import_run_id,
        snapshot_key=snapshot_key,
        count_by_name=count_by_name,
        postconditions_by_name=postcondition_by_name,
    )


def _marker_payload(
    marker_write: RecoveryMarkerWrite,
) -> tuple[dict[str, Any], dict[str, Any]]:
    recovered_at = datetime.now(timezone.utc).isoformat()
    marker_by_field = {
        "contract": PTG2_V4_FAILED_LAYOUT_RECOVERY_CONTRACT,
        "snapshot_id": marker_write.snapshot_id,
        "import_run_id": marker_write.import_run_id,
        "snapshot_key": marker_write.snapshot_key,
        "plan_digest": str(marker_write.plan_by_field["plan_digest"]),
        "representation": marker_write.plan_by_field.get("representation"),
        "recovered_at": recovered_at,
        "released_layouts": int(marker_write.released_layouts),
        "queued_candidate_hashes": int(
            marker_write.queued_candidate_hashes
        ),
        "queued_candidate_stored_bytes": int(
            marker_write.queued_candidate_stored_bytes
        ),
        "cas_payloads_deleted": 0,
        "postconditions": dict(marker_write.postcondition_by_name),
    }
    updated_report_by_field = {
        **dict(marker_write.expected_report_by_field),
        "shared_layout_abandoned": True,
        "shared_layout_abandonment_deferred": False,
        "shared_layout_recovery": marker_by_field,
    }
    return marker_by_field, updated_report_by_field


async def persist_recovery_marker(
    executor: Any,
    *,
    schema_name: str,
    marker_write: RecoveryMarkerWrite,
) -> dict[str, Any]:
    """CAS one complete recovery marker into the failed run report."""

    marker_by_field, updated_report_by_field = _marker_payload(marker_write)
    schema = _quote_ident(schema_name)
    updated_rows = await executor.all(
        f"""
        UPDATE {schema}.ptg2_import_run AS import_run
           SET report = CAST(:updated_report AS json)
         WHERE import_run.import_run_id = :import_run_id
           AND import_run.status = 'failed'
           AND import_run.report::jsonb = CAST(:expected_report AS jsonb)
           AND EXISTS (
                SELECT 1
                  FROM {schema}.ptg2_snapshot AS snapshot
                 WHERE snapshot.snapshot_id = :snapshot_id
                   AND snapshot.import_run_id = :import_run_id
                   AND snapshot.status = 'failed'
                   AND snapshot.published_at IS NULL
           )
        RETURNING import_run.report
        """,
        snapshot_id=marker_write.snapshot_id,
        import_run_id=marker_write.import_run_id,
        expected_report=json.dumps(
            dict(marker_write.expected_report_by_field),
            ensure_ascii=True,
            sort_keys=True,
            default=str,
        ),
        updated_report=json.dumps(
            updated_report_by_field,
            ensure_ascii=True,
            sort_keys=True,
            default=str,
        ),
    )
    if len(updated_rows) != 1:
        raise RuntimeError(
            "failed PTG V4 recovery owner changed before audit persistence"
        )
    return marker_by_field


__all__ = [
    "PTG2_V4_FAILED_LAYOUT_RECOVERY_CONTRACT",
    "RecoveryMarkerWrite",
    "completed_recovery_result",
    "load_completed_recovery_result",
    "persist_recovery_marker",
]
