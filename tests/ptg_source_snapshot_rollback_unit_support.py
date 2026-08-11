"""Pure fixtures for source-snapshot rollback unit tests."""

from __future__ import annotations

import datetime

from process.ptg_parts import source_snapshot_rollback_state as rollback_state
from process.ptg_parts.source_snapshot_rollback_types import (
    RollbackContext,
    RollbackDecision,
)


SOURCE_KEY = "source_a"
TARGET_SNAPSHOT = "snapshot_a"
CURRENT_SNAPSHOT = "snapshot_b"
ROLLBACK_OWNER = "source-a-reference"
IMPORT_MONTH = datetime.date(2026, 7, 1)


def serving_manifest(*, source_key: str = SOURCE_KEY) -> dict:
    """Return one source-bound serving manifest."""
    return {
        "serving_index": {
            "source_key": source_key,
            "arch_version": "postgres_binary_v3",
            "storage_generation": "shared_blocks_v3",
        }
    }


def allowed_index(*, previous_snapshot_id: str | None) -> dict:
    """Return one allowed-amount predecessor index."""
    return {
        "contract": "ptg2_allowed_amounts_v1",
        "arch_version": "postgres_binary_v3",
        "storage": "postgresql",
        "data_domain": "allowed_amounts",
        "source_key": SOURCE_KEY,
        "current_source_key": "source_a_allowed_amounts",
        "snapshot_scoped": True,
        "previous_snapshot_id": previous_snapshot_id,
    }


def target_snapshot() -> dict:
    """Return the retained rollback target."""
    return {
        "snapshot_id": TARGET_SNAPSHOT,
        "status": "published",
        "published_at": datetime.datetime(2026, 7, 1, 1, 0),
        "import_month": IMPORT_MONTH,
        "manifest": serving_manifest(),
        "snapshot_key": 17,
        "layout_state": "sealed",
        "layout_generation": "shared_blocks_v3",
        "mapping_digest": b"m" * 32,
        "support_digest": b"s" * 32,
    }


def expected_snapshot(*, allowed_predecessor: object = ...) -> dict:
    """Return the currently published successor snapshot."""
    manifest = serving_manifest()
    if allowed_predecessor is not ...:
        manifest["allowed_amount_index"] = allowed_index(
            previous_snapshot_id=allowed_predecessor,
        )
    return {
        "snapshot_id": CURRENT_SNAPSHOT,
        "status": "published",
        "manifest": manifest,
    }


def pin(*, owner_id: str = ROLLBACK_OWNER) -> dict:
    """Return the exact rollback-retention pin."""
    return {
        "owner_type": "ptg_v4_rollback",
        "owner_id": owner_id,
        "snapshot_id": TARGET_SNAPSHOT,
        "reason": "retained rollback reference",
    }


def snapshot_scope() -> dict:
    """Return the target snapshot plan scope."""
    return {
        "snapshot_id": TARGET_SNAPSHOT,
        "plan_id": "plan-1",
        "plan_market_type": "group",
        "coverage_scope_id": b"c" * 32,
    }


def activated_attestation(**overrides) -> dict:
    """Return an activated release attestation."""
    attestation_by_field = {
        **snapshot_scope(),
        "snapshot_key": 17,
        "source_key": SOURCE_KEY,
        "contract": "ptg2_v3_release_audit_attestation_v3",
        "activated_at": datetime.datetime(
            2026,
            7,
            1,
            1,
            tzinfo=datetime.UTC,
        ),
    }
    attestation_by_field.update(overrides)
    return attestation_by_field


def live_plan_pointer(
    *,
    snapshot_id: str = CURRENT_SNAPSHOT,
    previous_snapshot_id: str = TARGET_SNAPSHOT,
) -> dict:
    """Return the live source-plan pointer pair."""
    return {
        "plan_source_key": "current-plan-key",
        "plan_id": "plan-1",
        "plan_market_type": "group",
        "import_month": IMPORT_MONTH,
        "source_key": SOURCE_KEY,
        "snapshot_id": snapshot_id,
        "previous_snapshot_id": previous_snapshot_id,
    }


def context(**overrides) -> RollbackContext:
    """Build one complete rollback context."""
    context_by_field = {
        "source_pointer_by_field": {
            "source_key": SOURCE_KEY,
            "snapshot_id": CURRENT_SNAPSHOT,
            "previous_snapshot_id": TARGET_SNAPSHOT,
            "import_month": IMPORT_MONTH,
        },
        "target_snapshot_by_field": target_snapshot(),
        "expected_snapshot_by_field": expected_snapshot(),
        "rollback_pin_by_field": pin(),
        "target_snapshot_scope_by_field": snapshot_scope(),
        "target_attestation_by_field": activated_attestation(),
        "target_plan_scope_records": (
            {"plan_id": "plan-1", "plan_market_type": "group"},
        ),
        "source_plan_pointer_records": (live_plan_pointer(),),
        "global_pointer_by_field": {
            "snapshot_id": CURRENT_SNAPSHOT,
            "previous_snapshot_id": TARGET_SNAPSHOT,
            "source_key": SOURCE_KEY,
        },
        "allowed_pointer_by_field": {},
    }
    context_by_field.update(overrides)
    return RollbackContext(**context_by_field)


def decision(context_value: RollbackContext) -> RollbackDecision:
    """Evaluate one rollback context against the fixed coordinate."""
    return rollback_state.rollback_decision(
        context_value,
        source_key=SOURCE_KEY,
        snapshot_id=TARGET_SNAPSHOT,
        expected_current_snapshot_id=CURRENT_SNAPSHOT,
        rollback_owner_id=ROLLBACK_OWNER,
    )
