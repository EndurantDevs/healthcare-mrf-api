# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure validation and pointer decisions for published-snapshot rollback."""

from __future__ import annotations

import datetime
import json
from typing import Any, Mapping

from process.ptg_parts.domain import (
    PTG2_STATUS_PUBLISHED,
)
from process.ptg_parts.ptg2_candidate_attestation import (
    PTG2_CANDIDATE_ATTESTATION_SUPPORTED_CONTRACTS,
)
from process.ptg_parts.snapshot_cleanup import (
    is_strict_ptg2_v3_shared_blocks_manifest,
)
from process.ptg_parts.source_pointers import (
    _plan_pointer_entry,
)
from process.ptg_parts.source_snapshot_rollback_allowed import (
    allowed_pointer_decision,
)
from process.ptg_parts.source_snapshot_rollback_types import (
    ROLLBACK_PIN_OWNER_TYPE,
    PTG2SourceSnapshotRollbackConflict,
    RollbackContext,
    RollbackDecision,
)


def _manifest_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _snapshot_source_key(snapshot_by_field: Mapping[str, Any]) -> str | None:
    manifest_by_field = _manifest_mapping(snapshot_by_field.get("manifest"))
    serving_index_by_field = _manifest_mapping(
        manifest_by_field.get("serving_index")
    )
    return (
        str(serving_index_by_field.get("source_key") or "").strip().lower()
        or None
    )


def _validate_target_snapshot(
    target_by_field: Mapping[str, Any],
    *,
    source_key: str,
    snapshot_id: str,
) -> datetime.date:
    """Return the target month after validating immutable serving metadata."""

    if not target_by_field or str(target_by_field.get("snapshot_id") or "") != snapshot_id:
        raise ValueError("rollback target snapshot was not found")
    if str(target_by_field.get("status") or "").strip().lower() != PTG2_STATUS_PUBLISHED:
        raise ValueError("rollback target snapshot is not published")
    if target_by_field.get("published_at") is None:
        raise ValueError("rollback target snapshot has no publication timestamp")
    manifest_by_field = _manifest_mapping(target_by_field.get("manifest"))
    serving_index_by_field = _manifest_mapping(
        manifest_by_field.get("serving_index")
    )
    if not is_strict_ptg2_v3_shared_blocks_manifest(serving_index_by_field):
        raise ValueError(
            "rollback target must use postgres_binary_v3/shared_blocks_v3"
        )
    if _snapshot_source_key(target_by_field) != source_key:
        raise ValueError(
            "rollback target source_key does not match requested source_key"
        )
    if (
        str(target_by_field.get("layout_state") or "").strip().lower()
        != "sealed"
        or str(target_by_field.get("layout_generation") or "").strip().lower()
        != "shared_blocks_v3"
        or len(bytes(target_by_field.get("mapping_digest") or b"")) != 32
        or len(bytes(target_by_field.get("support_digest") or b"")) != 32
    ):
        raise ValueError("rollback target does not have a sealed immutable layout")
    import_month = target_by_field.get("import_month")
    if not isinstance(import_month, datetime.date):
        raise ValueError("rollback target has no import month")
    return import_month


def _validate_expected_snapshot(
    expected_by_field: Mapping[str, Any],
    *,
    source_key: str,
    expected_current_snapshot_id: str,
) -> None:
    """Require the expected live snapshot to belong to the same source."""

    if (
        not expected_by_field
        or str(expected_by_field.get("snapshot_id") or "")
        != expected_current_snapshot_id
        or str(expected_by_field.get("status") or "").strip().lower()
        != PTG2_STATUS_PUBLISHED
        or _snapshot_source_key(expected_by_field) != source_key
    ):
        raise ValueError(
            "expected current snapshot is not a published snapshot for this source"
        )


def _validate_rollback_pin(
    pin_by_field: Mapping[str, Any],
    *,
    snapshot_id: str,
    rollback_owner_id: str,
) -> None:
    """Require the exact nonempty canary rollback retention pin."""

    if (
        pin_by_field.get("owner_type") != ROLLBACK_PIN_OWNER_TYPE
        or pin_by_field.get("owner_id") != rollback_owner_id
        or pin_by_field.get("snapshot_id") != snapshot_id
        or not str(pin_by_field.get("reason") or "").strip()
    ):
        raise PTG2SourceSnapshotRollbackConflict(
            "rollback target does not have the exact requested rollback pin"
        )


def _normalized_plan_scope(
    scope_by_field: Mapping[str, Any],
) -> tuple[str, str]:
    return (
        str(scope_by_field.get("plan_id") or "").strip(),
        str(scope_by_field.get("plan_market_type") or "").strip().lower(),
    )


def _validate_target_serving_relations(
    context: RollbackContext,
    *,
    source_key: str,
    snapshot_id: str,
) -> None:
    """Require the resolver's immutable scope and activated audit closure."""

    scope_by_field = context.target_snapshot_scope_by_field
    scope_identity = _normalized_plan_scope(scope_by_field)
    coverage_scope_id = bytes(scope_by_field.get("coverage_scope_id") or b"")
    if (
        str(scope_by_field.get("snapshot_id") or "") != snapshot_id
        or not all(scope_identity)
        or len(coverage_scope_id) != 32
    ):
        raise ValueError("rollback target has no immutable snapshot scope")
    plan_scope_identities = {
        _normalized_plan_scope(plan_scope_by_field)
        for plan_scope_by_field in context.target_plan_scope_records
    }
    if scope_identity not in plan_scope_identities:
        raise ValueError(
            "rollback target snapshot scope is absent from logical plan mappings"
        )
    target_by_field = context.target_snapshot_by_field
    attestation_by_field = context.target_attestation_by_field
    attestation_identity = _normalized_plan_scope(attestation_by_field)
    if (
        str(attestation_by_field.get("snapshot_id") or "") != snapshot_id
        or attestation_by_field.get("snapshot_key")
        != target_by_field.get("snapshot_key")
        or str(attestation_by_field.get("source_key") or "").strip().lower()
        != source_key
        or attestation_identity != scope_identity
        or bytes(attestation_by_field.get("coverage_scope_id") or b"")
        != coverage_scope_id
        or attestation_by_field.get("contract")
        not in PTG2_CANDIDATE_ATTESTATION_SUPPORTED_CONTRACTS
        or attestation_by_field.get("activated_at") is None
    ):
        raise ValueError(
            "rollback target has no activated source-matched audit attestation"
        )


def _target_plan_pointer_entries(
    context: RollbackContext,
    *,
    source_key: str,
    snapshot_id: str,
    expected_current_snapshot_id: str,
    import_month: datetime.date,
) -> tuple[Mapping[str, Any], ...]:
    pointer_entries = tuple(
        _plan_pointer_entry(
            plan_id=str(scope_by_field.get("plan_id") or ""),
            plan_market_type=str(scope_by_field.get("plan_market_type") or ""),
            import_month=import_month,
            source_key=source_key,
            snapshot_id=snapshot_id,
            previous_snapshot_id=expected_current_snapshot_id,
            updated_at=datetime.datetime.min,
        )
        for scope_by_field in context.target_plan_scope_records
    )
    if not pointer_entries:
        raise ValueError("rollback target has no immutable logical plan mappings")
    return pointer_entries


def _pointer_identity(pointer_by_field: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        str(pointer_by_field.get("plan_source_key") or ""),
        str(pointer_by_field.get("plan_id") or ""),
        str(pointer_by_field.get("plan_market_type") or "").strip().lower(),
        pointer_by_field.get("import_month"),
        str(pointer_by_field.get("source_key") or "").strip().lower(),
        str(pointer_by_field.get("snapshot_id") or ""),
        str(pointer_by_field.get("previous_snapshot_id") or "") or None,
    )


def _is_plan_pointer_state_exact(
    actual_pointer_records: tuple[Mapping[str, Any], ...],
    expected_pointer_records: tuple[Mapping[str, Any], ...],
) -> bool:
    return sorted(map(_pointer_identity, actual_pointer_records)) == sorted(
        map(_pointer_identity, expected_pointer_records)
    )


def _validate_live_plan_state(
    context: RollbackContext,
    *,
    snapshot_id: str,
    expected_current_snapshot_id: str,
    expected_target_pointers: tuple[Mapping[str, Any], ...],
    is_already_rolled_back: bool,
) -> None:
    if is_already_rolled_back:
        if not _is_plan_pointer_state_exact(
            context.source_plan_pointer_records,
            expected_target_pointers,
        ):
            raise PTG2SourceSnapshotRollbackConflict(
                "source plan pointers do not match an exact completed rollback"
            )
        return
    if not context.source_plan_pointer_records or any(
        str(pointer_by_field.get("snapshot_id") or "")
        != expected_current_snapshot_id
        or (str(pointer_by_field.get("previous_snapshot_id") or "") or None)
        != snapshot_id
        for pointer_by_field in context.source_plan_pointer_records
    ):
        raise PTG2SourceSnapshotRollbackConflict(
            "source plan pointers do not match the requested rollback pair"
        )


def _should_reverse_global_pointer(
    context: RollbackContext,
    *,
    source_key: str,
    snapshot_id: str,
    expected_current_snapshot_id: str,
    is_already_rolled_back: bool,
) -> bool:
    pointer_by_field = context.global_pointer_by_field
    if not pointer_by_field:
        return False
    current_snapshot_id = str(pointer_by_field.get("snapshot_id") or "") or None
    previous_snapshot_id = (
        str(pointer_by_field.get("previous_snapshot_id") or "") or None
    )
    current_source_key = (
        str(pointer_by_field.get("source_key") or "").strip().lower()
    )
    if current_source_key and current_source_key != source_key:
        return False
    if not current_source_key and current_snapshot_id is not None:
        raise PTG2SourceSnapshotRollbackConflict(
            "global pointer source identity is unavailable"
        )
    required_pair = (
        (snapshot_id, expected_current_snapshot_id)
        if is_already_rolled_back
        else (expected_current_snapshot_id, snapshot_id)
    )
    if (current_snapshot_id, previous_snapshot_id) != required_pair:
        raise PTG2SourceSnapshotRollbackConflict(
            "same-source global pointer does not match the requested rollback pair"
        )
    return not is_already_rolled_back


def _is_exact_retry(
    context: RollbackContext,
    *,
    snapshot_id: str,
    expected_current_snapshot_id: str,
) -> bool:
    """Classify the source pair as forward, exactly reversed, or stale."""

    source_pair = (
        str(context.source_pointer_by_field.get("snapshot_id") or "") or None,
        str(context.source_pointer_by_field.get("previous_snapshot_id") or "")
        or None,
    )
    if source_pair == (expected_current_snapshot_id, snapshot_id):
        return False
    if source_pair == (snapshot_id, expected_current_snapshot_id):
        return True
    raise PTG2SourceSnapshotRollbackConflict(
        "source pointer does not match the requested current/previous pair"
    )


def _pointer_decision(
    context: RollbackContext,
    *,
    source_key: str,
    snapshot_id: str,
    expected_current_snapshot_id: str,
    import_month: datetime.date,
    is_already_rolled_back: bool,
) -> RollbackDecision:
    """Validate plan, global, and allowed pointer surfaces."""

    pointer_entries = _target_plan_pointer_entries(
        context,
        source_key=source_key,
        snapshot_id=snapshot_id,
        expected_current_snapshot_id=expected_current_snapshot_id,
        import_month=import_month,
    )
    _validate_live_plan_state(
        context,
        snapshot_id=snapshot_id,
        expected_current_snapshot_id=expected_current_snapshot_id,
        expected_target_pointers=pointer_entries,
        is_already_rolled_back=is_already_rolled_back,
    )
    should_reverse_global = _should_reverse_global_pointer(
        context,
        source_key=source_key,
        snapshot_id=snapshot_id,
        expected_current_snapshot_id=expected_current_snapshot_id,
        is_already_rolled_back=is_already_rolled_back,
    )
    allowed_decision = allowed_pointer_decision(
        context.expected_snapshot_by_field,
        context.allowed_pointer_by_field,
        source_key=source_key,
        expected_current_snapshot_id=expected_current_snapshot_id,
        is_already_rolled_back=is_already_rolled_back,
    )
    (
        allowed_action,
        allowed_snapshot_id,
        allowed_previous_snapshot_id,
        allowed_import_month,
    ) = allowed_decision
    return RollbackDecision(
        is_already_rolled_back=is_already_rolled_back,
        plan_pointer_entries=pointer_entries,
        should_reverse_global_pointer=should_reverse_global,
        allowed_action=allowed_action,
        allowed_snapshot_id=allowed_snapshot_id,
        allowed_previous_snapshot_id=allowed_previous_snapshot_id,
        allowed_import_month=allowed_import_month,
    )


def rollback_decision(
    context: RollbackContext,
    *,
    source_key: str,
    snapshot_id: str,
    expected_current_snapshot_id: str,
    rollback_owner_id: str,
) -> RollbackDecision:
    """Validate every live surface and return the atomic pointer mutations."""

    import_month = _validate_target_snapshot(
        context.target_snapshot_by_field,
        source_key=source_key,
        snapshot_id=snapshot_id,
    )
    _validate_expected_snapshot(
        context.expected_snapshot_by_field,
        source_key=source_key,
        expected_current_snapshot_id=expected_current_snapshot_id,
    )
    _validate_rollback_pin(
        context.rollback_pin_by_field,
        snapshot_id=snapshot_id,
        rollback_owner_id=rollback_owner_id,
    )
    _validate_target_serving_relations(
        context,
        source_key=source_key,
        snapshot_id=snapshot_id,
    )
    is_already_rolled_back = _is_exact_retry(
        context,
        snapshot_id=snapshot_id,
        expected_current_snapshot_id=expected_current_snapshot_id,
    )
    return _pointer_decision(
        context,
        source_key=source_key,
        snapshot_id=snapshot_id,
        expected_current_snapshot_id=expected_current_snapshot_id,
        import_month=import_month,
        is_already_rolled_back=is_already_rolled_back,
    )
