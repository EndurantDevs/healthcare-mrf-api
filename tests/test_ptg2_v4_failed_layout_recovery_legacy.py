# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Legacy replay safety for exact failed PTG V4 layout recovery."""

from __future__ import annotations

from process.ptg_parts import ptg2_v4_failed_layout_marker as marker

_SNAPSHOT_ID = "ptg2:202607:test"
_RUN_ID = "ptg2:test-run"
_ZERO_POSTCONDITION_BY_NAME = {
    "layouts": 0,
    "fingerprints": 0,
    "mappings": 0,
    "map_roots": 0,
    "map_packs": 0,
    "relation_manifests": 0,
    "build_pins": 0,
    "build_pin_lease_groups": 0,
    "dense_rows": 0,
}


def _completed_report(**marker_overrides: object) -> dict[str, object]:
    marker_by_field = {
        "contract": marker.PTG2_V4_FAILED_LAYOUT_RECOVERY_CONTRACT,
        "snapshot_id": _SNAPSHOT_ID,
        "import_run_id": _RUN_ID,
        "snapshot_key": 491,
        "plan_digest": "a" * 64,
        "target_digest": "b" * 64,
        "representation": "direct_v1",
        "recovered_at": "2026-07-24T00:00:00+00:00",
        "released_layouts": 1,
        "queued_candidate_hashes": 3,
        "queued_candidate_stored_bytes": 18,
        "candidate_metrics_scope": "layout_reachability",
        "cas_payloads_deleted": 0,
        "postconditions": dict(_ZERO_POSTCONDITION_BY_NAME),
        **marker_overrides,
    }
    return {
        "shared_snapshot_key": 491,
        "shared_layout_abandoned": True,
        "shared_layout_abandonment_deferred": False,
        "shared_layout_recovery": marker_by_field,
    }


def _completed_result(
    report_by_field: dict[str, object],
    *,
    attempt_fence_by_field: dict[str, object] | None = None,
) -> dict[str, object] | None:
    marker_by_field = dict(report_by_field["shared_layout_recovery"])
    return marker.completed_recovery_result(
        snapshot_by_field={
            "snapshot_id": _SNAPSHOT_ID,
            "import_run_id": _RUN_ID,
            "status": "failed",
            "published_at": None,
        },
        run_by_field={
            "import_run_id": _RUN_ID,
            "status": "failed",
            "report": report_by_field,
        },
        snapshot_id=_SNAPSHOT_ID,
        import_run_id=_RUN_ID,
        snapshot_key=491,
        count_by_name={},
        postconditions_by_name=_ZERO_POSTCONDITION_BY_NAME,
        attempt_fence_by_field=attempt_fence_by_field
        or {
            "snapshot_id": _SNAPSHOT_ID,
            "internal_run_id": _RUN_ID,
            "state": "reconciled",
            "target_digest": marker_by_field.get("target_digest"),
            "plan_digest": marker_by_field.get("plan_digest"),
            "marker_digest": marker.canonical_json_digest(marker_by_field),
            "marker": marker_by_field,
            "reconciled_at": "2026-08-24T00:00:00+00:00",
        },
    )


def _legacy_completed_report() -> dict[str, object]:
    report_by_field = _completed_report()
    marker_by_field = dict(report_by_field["shared_layout_recovery"])
    marker_by_field.pop("candidate_metrics_scope")
    marker_by_field.pop("target_digest")
    postcondition_by_name = dict(marker_by_field["postconditions"])
    postcondition_by_name.pop("build_pins")
    postcondition_by_name.pop("build_pin_lease_groups")
    marker_by_field["postconditions"] = postcondition_by_name
    report_by_field["shared_layout_recovery"] = marker_by_field
    return report_by_field


def test_completed_recovery_replays_legacy_marker_without_sealed_fence() -> None:
    replay_by_field = _completed_result(
        _legacy_completed_report(),
        attempt_fence_by_field={
            "snapshot_id": _SNAPSHOT_ID,
            "internal_run_id": _RUN_ID,
            "state": "active",
            "target_digest": None,
            "plan_digest": None,
            "marker_digest": None,
            "marker": None,
            "reconciled_at": None,
        },
    )

    assert replay_by_field is not None
    assert replay_by_field["idempotent"] is True
    assert replay_by_field["candidate_metrics_scope"] == "layout_reachability"
    assert replay_by_field["target_digest"] is None


def test_completed_recovery_rejects_new_marker_missing_target_digest() -> None:
    assert _completed_result(_completed_report(target_digest=None)) is None
