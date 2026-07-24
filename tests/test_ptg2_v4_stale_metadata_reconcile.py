# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime as dt
import hashlib
import json

import pytest

from process.ptg_parts import ptg2_v4_stale_metadata_reconcile as reconcile
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_SHARED_GENERATION,
)


SNAPSHOT_ID = "ptg2:202607:synthetic"
INTERNAL_RUN_ID = "ptg2:synthetic-run"
FENCE_NONCE = "11111111-1111-4111-8111-111111111111"
NOW = dt.datetime(2026, 7, 24, 12, 0, 0)
STALE_HEARTBEAT = NOW - dt.timedelta(hours=8)
ZERO_ATTACHMENTS = {
    "layout_bindings": 0,
    "logical_metadata_rows": 0,
    "artifact_rows": 0,
    "payload_rows": 0,
    "pointer_references": 0,
    "internal_jobs": 0,
}


def _active_fence() -> dict:
    """Return one pristine immutable authority row."""

    return {
        "snapshot_id": SNAPSHOT_ID,
        "internal_run_id": INTERNAL_RUN_ID,
        "fence_nonce": FENCE_NONCE,
        "state": "active",
        "target_digest": None,
        "plan_digest": None,
        "marker_digest": None,
        "marker": None,
        "marker_is_sql_null": True,
        "created_at": STALE_HEARTBEAT,
        "reconciled_at": None,
    }


def _ready_context(
    *,
    observed_at: dt.datetime = NOW,
    manifest: dict | None = None,
    storage_generation: str = PTG2_V4_SHARED_GENERATION,
    attachments: dict[str, int] | None = None,
    source_key: str = "synthetic-source",
    report: dict | None = None,
    snapshot_overrides: dict | None = None,
    internal_run_overrides: dict | None = None,
):
    return reconcile.PTG2V4StaleMetadataContext(
        observed_at=observed_at,
        snapshot_by_field={
            "snapshot_id": SNAPSHOT_ID,
            "import_run_id": INTERNAL_RUN_ID,
            "status": "building",
            "created_at": STALE_HEARTBEAT,
            "validated_at": None,
            "published_at": None,
            "previous_snapshot_id": None,
            "manifest": {} if manifest is None else manifest,
            "manifest_is_sql_null": False,
            **dict(snapshot_overrides or {}),
        },
        internal_run_by_field={
            "import_run_id": INTERNAL_RUN_ID,
            "status": "running",
            "started_at": STALE_HEARTBEAT,
            "finished_at": None,
            "heartbeat_at": STALE_HEARTBEAT,
            "options": {
                "storage_generation": storage_generation,
                "source_key": source_key,
            },
            "report": {} if report is None else dict(report),
            "report_is_sql_null": False,
            "error": None,
            **dict(internal_run_overrides or {}),
        },
        attempt_fence_by_field=_active_fence(),
        attachment_count_by_name=(
            dict(ZERO_ATTACHMENTS)
            if attachments is None
            else dict(attachments)
        ),
    )


def _plan(context):
    target_digest = reconcile.stale_target_digest(
        SNAPSHOT_ID,
        INTERNAL_RUN_ID,
    )
    return reconcile.build_stale_plan(
        context,
        internal_run_id=INTERNAL_RUN_ID,
        stale_after_seconds=21_600,
        target_digest=target_digest,
    )


def _completed_context(
    ready_context,
    marker_by_field,
    *,
    fence_overrides: dict | None = None,
    attachments: dict[str, int] | None = None,
):
    marker_envelope_by_field = {
        reconcile.PTG2_V4_STALE_METADATA_MARKER: marker_by_field
    }
    marker_digest = hashlib.sha256(
        json.dumps(
            marker_by_field,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
    ).hexdigest()
    return reconcile.PTG2V4StaleMetadataContext(
        observed_at=NOW + dt.timedelta(days=1),
        snapshot_by_field={
            **ready_context.snapshot_by_field,
            "status": "failed",
            "manifest": marker_envelope_by_field,
        },
        internal_run_by_field={
            **ready_context.internal_run_by_field,
            "status": "failed",
            "report": marker_envelope_by_field,
        },
        attempt_fence_by_field={
            "snapshot_id": SNAPSHOT_ID,
            "internal_run_id": INTERNAL_RUN_ID,
            "fence_nonce": FENCE_NONCE,
            "state": "reconciled",
            "target_digest": marker_by_field.get("target_digest"),
            "plan_digest": marker_by_field.get("plan_digest"),
            "marker_digest": marker_digest,
            "marker": marker_by_field,
            "marker_is_sql_null": False,
            "created_at": STALE_HEARTBEAT,
            "reconciled_at": NOW,
            **dict(fence_overrides or {}),
        },
        attachment_count_by_name=(
            dict(ZERO_ATTACHMENTS)
            if attachments is None
            else dict(attachments)
        ),
    )


def test_ready_plan_is_redacted_and_has_only_metadata_updates():
    plan_by_field = _plan(_ready_context())
    serialized_plan = json.dumps(plan_by_field, sort_keys=True)

    assert plan_by_field["status"] == "ready"
    assert plan_by_field["planned_metadata_updates"] == {
        "snapshot_rows": 1,
        "internal_run_rows": 1,
        "attempt_fence_rows": 1,
    }
    assert set(plan_by_field["external_effects"].values()) == {0}
    assert SNAPSHOT_ID not in serialized_plan
    assert INTERNAL_RUN_ID not in serialized_plan
    assert FENCE_NONCE not in serialized_plan


def test_plan_digest_is_stable_while_stale_age_advances():
    first_plan = _plan(_ready_context(observed_at=NOW))
    later_plan = _plan(
        _ready_context(observed_at=NOW + dt.timedelta(minutes=10))
    )

    assert first_plan["plan_digest"] == later_plan["plan_digest"]
    assert (
        first_plan["observations"]["stale_age_seconds"]
        < later_plan["observations"]["stale_age_seconds"]
    )


def test_plan_digest_binds_the_redacted_stage_source_identity():
    first_plan = _plan(_ready_context(source_key="source-one"))
    changed_plan = _plan(_ready_context(source_key="source-two"))
    serialized_plans = json.dumps(
        [first_plan, changed_plan],
        sort_keys=True,
    )

    assert first_plan["plan_digest"] != changed_plan["plan_digest"]
    assert "source-one" not in serialized_plans
    assert "source-two" not in serialized_plans


def test_plan_digest_binds_the_complete_run_report_observation():
    first_plan = _plan(
        _ready_context(report={"progress_seq": 7, "phase": "scan"})
    )
    changed_plan = _plan(
        _ready_context(report={"progress_seq": 8, "phase": "scan"})
    )

    assert first_plan["status"] == "ready"
    assert changed_plan["status"] == "ready"
    assert first_plan["plan_digest"] != changed_plan["plan_digest"]
    assert (
        first_plan["observations"]["internal_run_report_digest"]
        != changed_plan["observations"]["internal_run_report_digest"]
    )


@pytest.mark.parametrize(
    ("context", "reason_code"),
    (
        (
            _ready_context(
                snapshot_overrides={"validated_at": NOW},
            ),
            "snapshot_validation_evidence_present",
        ),
        (
            _ready_context(
                snapshot_overrides={"published_at": NOW},
            ),
            "snapshot_publication_evidence_present",
        ),
        (
            _ready_context(
                internal_run_overrides={"finished_at": NOW},
            ),
            "internal_run_finished_at_present",
        ),
        (
            _ready_context(
                internal_run_overrides={"error": "late diagnostic"},
            ),
            "internal_run_error_present",
        ),
    ),
)
def test_plan_rejects_terminal_or_publication_evidence(
    context,
    reason_code,
):
    plan_by_field = _plan(context)

    assert plan_by_field["status"] == "ineligible"
    assert reason_code in plan_by_field["reason_codes"]


def test_plan_digest_binds_previous_snapshot_identity():
    first_plan = _plan(_ready_context())
    changed_plan = _plan(
        _ready_context(
            snapshot_overrides={"previous_snapshot_id": "ptg2:previous"}
        )
    )

    assert first_plan["status"] == changed_plan["status"] == "ready"
    assert first_plan["plan_digest"] != changed_plan["plan_digest"]
    assert "ptg2:previous" not in json.dumps(changed_plan, sort_keys=True)


@pytest.mark.parametrize(
    "report_field",
    (
        "shared_snapshot_key",
        "shared_layout_abandonment_deferred",
        "shared_layout_recovery",
        "physical_layout_owner",
        "failed_layout_recovery",
    ),
)
def test_plan_rejects_physical_layout_or_recovery_report_fields(
    report_field,
):
    plan_by_field = _plan(
        _ready_context(report={report_field: "observed"})
    )

    assert plan_by_field["status"] == "ineligible"
    assert (
        "physical_layout_or_recovery_report_present"
        in plan_by_field["reason_codes"]
    )
    assert plan_by_field["observations"][
        "physical_layout_report_keys"
    ] == [report_field]


@pytest.mark.parametrize(
    "context,reason_code",
    (
        (
            _ready_context(
                storage_generation="shared_blocks_v3",
            ),
            "internal_run_not_v4",
        ),
        (
            _ready_context(
                manifest={"serving_index": {"type": "unexpected"}},
            ),
            "snapshot_manifest_not_empty",
        ),
        (
            _ready_context(
                attachments={
                    **ZERO_ATTACHMENTS,
                    "layout_bindings": 1,
                }
            ),
            "attached_state_present",
        ),
        (
            _ready_context(
                observed_at=STALE_HEARTBEAT + dt.timedelta(minutes=30),
            ),
            "internal_run_not_stale",
        ),
    ),
)
def test_plan_fails_closed_for_non_metadata_only_targets(
    context,
    reason_code,
):
    plan_by_field = _plan(context)

    assert plan_by_field["status"] == "ineligible"
    assert reason_code in plan_by_field["reason_codes"]
    assert plan_by_field["planned_metadata_updates"] == {
        "snapshot_rows": 0,
        "internal_run_rows": 0,
        "attempt_fence_rows": 0,
    }


def test_matching_markers_make_retry_write_free():
    ready_context = _ready_context()
    ready_plan = _plan(ready_context)
    marker_by_field = reconcile.build_stale_marker(
        plan_by_field=ready_plan,
        reconciled_at_text="2026-07-24T12:00:00.000000Z",
    )
    idempotent_context = _completed_context(
        ready_context,
        marker_by_field,
    )

    retry_plan = _plan(idempotent_context)

    assert retry_plan["status"] == "already_reconciled"
    assert retry_plan["idempotent"] is True
    assert retry_plan["plan_digest"] == ready_plan["plan_digest"]
    assert retry_plan["planned_metadata_updates"] == {
        "snapshot_rows": 0,
        "internal_run_rows": 0,
        "attempt_fence_rows": 0,
    }


def test_idempotent_retry_fails_closed_if_attachment_appears():
    ready_context = _ready_context()
    ready_plan = _plan(ready_context)
    marker_by_field = reconcile.build_stale_marker(
        plan_by_field=ready_plan,
        reconciled_at_text="2026-07-24T12:00:00.000000Z",
    )
    attached_context = _completed_context(
        ready_context,
        marker_by_field,
        attachments={
            **ZERO_ATTACHMENTS,
            "serving_rate": 1,
        },
    )

    retry_plan = _plan(attached_context)

    assert retry_plan["status"] == "ineligible"
    assert retry_plan["idempotent"] is False
    assert "attached_state_present" in retry_plan["reason_codes"]


@pytest.mark.parametrize(
    "mutate_marker,fence_overrides",
    (
        (lambda marker: marker.update(plan_digest="G" * 64), {}),
        (lambda marker: marker.pop("audit_safe"), {}),
        (lambda marker: marker.update(unexpected=True), {}),
        (
            lambda _marker: None,
            {"marker_digest": "0" * 64},
        ),
        (
            lambda _marker: None,
            {"reconciled_at": NOW - dt.timedelta(seconds=1)},
        ),
    ),
)
def test_weak_marker_or_audit_does_not_authorize_idempotent_retry(
    mutate_marker,
    fence_overrides,
):
    ready_context = _ready_context()
    ready_plan = _plan(ready_context)
    marker_by_field = reconcile.build_stale_marker(
        plan_by_field=ready_plan,
        reconciled_at_text="2026-07-24T12:00:00.000000Z",
    )
    mutate_marker(marker_by_field)
    invalid_context = _completed_context(
        ready_context,
        marker_by_field,
        fence_overrides=fence_overrides,
    )

    retry_plan = _plan(invalid_context)

    assert retry_plan["status"] == "ineligible"
    assert retry_plan["idempotent"] is False


def test_server_threshold_rejects_dangerously_small_configuration(
    monkeypatch,
):
    monkeypatch.setenv(
        reconcile.PTG2_V4_STALE_METADATA_SECONDS_ENV,
        "0",
    )

    with pytest.raises(RuntimeError, match="must be at least 300"):
        reconcile._server_stale_after_seconds()


def test_schema_resolution_supports_the_legacy_alias(monkeypatch):
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    monkeypatch.setenv("DB_SCHEMA", "legacy_ptg")

    assert reconcile._schema_name() == "legacy_ptg"


def test_schema_resolution_rejects_conflicting_aliases(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "runtime_ptg")
    monkeypatch.setenv("DB_SCHEMA", "legacy_ptg")

    with pytest.raises(RuntimeError, match="must identify the same schema"):
        reconcile._schema_name()


@pytest.mark.parametrize(
    "field_name,value",
    (
        ("snapshot_id", ""),
        ("snapshot_id", "has whitespace"),
        ("internal_run_id", "x" * 97),
    ),
)
def test_exact_coordinates_have_a_bounded_identifier_contract(field_name, value):
    with pytest.raises(ValueError, match="1-96 character PTG identifier"):
        reconcile._normalized_identifier(value, field_name=field_name)
