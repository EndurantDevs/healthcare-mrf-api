# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact replay and predecessor retention for reviewed PTG activation."""

from __future__ import annotations

import hmac
import json
from typing import Any, Mapping

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.domain import PTG2_STATUS_PUBLISHED
from process.ptg_parts.ptg2_candidate_attestation import (
    PTG2_CANDIDATE_ACTIVATION_INTENT_AUDIT_ONLY,
)
from process.ptg_parts.source_snapshot_rollback_types import (
    ROLLBACK_PIN_OWNER_TYPE,
)

_REVIEWED_ACTIVATION_SQL = """
SELECT snapshot.status, snapshot.published_at,
       snapshot.previous_snapshot_id, snapshot.manifest,
       layout.generation AS storage_generation,
       attestation.activation_intent,
       attestation.attestation_digest,
       attestation.activated_at,
       pointer.snapshot_id AS current_snapshot_id,
       pointer.previous_snapshot_id AS current_previous_snapshot_id,
       pin.reason AS rollback_pin_reason,
       (
           SELECT COUNT(*)
             FROM __SCHEMA__.ptg2_current_plan_source AS plan_pointer
            WHERE plan_pointer.source_key = :source_key
              AND plan_pointer.snapshot_id = :snapshot_id
              AND plan_pointer.previous_snapshot_id = :predecessor_snapshot_id
       ) AS plan_source_count,
       (
           SELECT COUNT(*)
             FROM __SCHEMA__.ptg2_current_plan_source AS plan_pointer
            WHERE plan_pointer.source_key = :source_key
              AND (
                  plan_pointer.snapshot_id <> :snapshot_id
                  OR plan_pointer.previous_snapshot_id IS DISTINCT
                     FROM :predecessor_snapshot_id
              )
       ) AS conflicting_plan_source_count
  FROM __SCHEMA__.ptg2_snapshot AS snapshot
  JOIN __SCHEMA__.ptg2_v3_snapshot_binding AS binding
    ON binding.snapshot_id = snapshot.snapshot_id
  JOIN __SCHEMA__.ptg2_v3_snapshot_layout AS layout
    ON layout.snapshot_key = binding.snapshot_key
  JOIN __SCHEMA__.ptg2_v3_snapshot_scope AS scope
    ON scope.snapshot_id = snapshot.snapshot_id
  JOIN __SCHEMA__.ptg2_v3_candidate_audit_attestation AS attestation
    ON attestation.snapshot_id = snapshot.snapshot_id
  LEFT JOIN __SCHEMA__.ptg2_current_source_snapshot AS pointer
    ON pointer.source_key = :source_key
  LEFT JOIN __SCHEMA__.ptg2_snapshot_pin AS pin
    ON pin.owner_type = :rollback_owner_type
   AND pin.owner_id = :rollback_owner_id
   AND pin.snapshot_id = :predecessor_snapshot_id
 WHERE snapshot.snapshot_id = :snapshot_id
 FOR SHARE OF snapshot, binding, layout, scope, attestation
"""


class PTG2SourcePointerConflict(RuntimeError):
    """Raised when a source pointer changed after an import observed it."""


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    if value is None:
        return {}
    return dict(getattr(value, "_mapping", value))


def reviewed_rollback_owner_id(
    rollback_owner_id: str | None,
    *,
    required: bool,
) -> str | None:
    """Validate one stable reviewed-activation rollback owner."""

    normalized_owner_id = str(rollback_owner_id or "").strip()
    if not normalized_owner_id:
        if required:
            raise ValueError(
                "reviewed audit-only activation requires rollback_owner_id"
            )
        return None
    if len(normalized_owner_id) > 96:
        raise ValueError("rollback_owner_id exceeds 96 characters")
    return normalized_owner_id


async def pin_reviewed_activation_predecessor(
    session: Any,
    *,
    schema_name: str,
    activation_by_field: Mapping[str, Any],
    activated_at: Any,
    rollback_owner_id: str | None,
    is_reviewed_audit_only: bool,
) -> None:
    """Retain the exact predecessor needed to reverse a reviewed cutover."""

    normalized_owner_id = reviewed_rollback_owner_id(
        rollback_owner_id,
        required=is_reviewed_audit_only,
    )
    if not is_reviewed_audit_only:
        if normalized_owner_id is not None:
            raise ValueError(
                "rollback_owner_id is supported only for reviewed "
                "audit-only activation"
            )
        return
    predecessor_snapshot_id = activation_by_field["previous_snapshot_id"]
    if not predecessor_snapshot_id:
        raise ValueError(
            "reviewed audit-only activation requires a published predecessor"
        )
    schema = _quote_ident(schema_name)
    await session.execute(
        db.text(
            f"""
            INSERT INTO {schema}.ptg2_snapshot_pin
                (owner_type, owner_id, snapshot_id, reason, created_at)
            SELECT :owner_type, :owner_id, snapshot.snapshot_id,
                   :reason, :created_at
              FROM {schema}.ptg2_snapshot AS snapshot
             WHERE snapshot.snapshot_id = :snapshot_id
               AND snapshot.status = :published_status
            ON CONFLICT (owner_type, owner_id, snapshot_id) DO NOTHING
            """
        ),
        {
            "owner_type": ROLLBACK_PIN_OWNER_TYPE,
            "owner_id": normalized_owner_id,
            "snapshot_id": predecessor_snapshot_id,
            "reason": "reviewed audit-only candidate rollback predecessor",
            "created_at": activated_at,
            "published_status": PTG2_STATUS_PUBLISHED,
        },
    )
    await _require_rollback_pin(
        session,
        schema_name=schema_name,
        owner_id=normalized_owner_id,
        snapshot_id=predecessor_snapshot_id,
    )


async def _require_rollback_pin(
    session: Any,
    *,
    schema_name: str,
    owner_id: str,
    snapshot_id: str,
) -> None:
    schema = _quote_ident(schema_name)
    pin_result = await session.execute(
        db.text(
            f"""
            SELECT reason
              FROM {schema}.ptg2_snapshot_pin
             WHERE owner_type = :owner_type
               AND owner_id = :owner_id
               AND snapshot_id = :snapshot_id
             FOR SHARE
            """
        ),
        {
            "owner_type": ROLLBACK_PIN_OWNER_TYPE,
            "owner_id": owner_id,
            "snapshot_id": snapshot_id,
        },
    )
    pin_row = pin_result.one_or_none()
    if pin_row is None or not str(pin_row[0] or "").strip():
        raise PTG2SourcePointerConflict(
            "reviewed activation predecessor rollback pin was not created"
        )


async def completed_reviewed_activation(
    session: Any,
    *,
    schema_name: str,
    source_key: str,
    snapshot_id: str,
    expected_current_snapshot_id: str | None,
    expected_audit_only_attestation_digest: bytes | None,
    rollback_owner_id: str | None,
) -> dict[str, Any] | None:
    """Return one exact already-committed reviewed activation."""

    if expected_audit_only_attestation_digest is None:
        return None
    normalized_owner_id = reviewed_rollback_owner_id(
        rollback_owner_id,
        required=True,
    )
    predecessor_snapshot_id = (
        str(expected_current_snapshot_id or "").strip() or None
    )
    if predecessor_snapshot_id is None:
        raise ValueError(
            "reviewed audit-only activation requires "
            "expected_current_snapshot_id"
        )
    activation_row = await _reviewed_activation_row(
        session,
        schema_name=schema_name,
        source_key=source_key,
        snapshot_id=snapshot_id,
        predecessor_snapshot_id=predecessor_snapshot_id,
        rollback_owner_id=normalized_owner_id,
    )
    if not activation_row:
        return None
    if (
        str(activation_row.get("status") or "").strip().lower()
        != PTG2_STATUS_PUBLISHED
    ):
        return None
    _require_exact_reviewed_activation(
        activation_row,
        source_key=source_key,
        snapshot_id=snapshot_id,
        predecessor_snapshot_id=predecessor_snapshot_id,
        expected_attestation_digest=expected_audit_only_attestation_digest,
    )
    return _completed_activation_result(
        activation_row,
        source_key=source_key,
        snapshot_id=snapshot_id,
        predecessor_snapshot_id=predecessor_snapshot_id,
        rollback_owner_id=normalized_owner_id,
    )


async def _reviewed_activation_row(
    session: Any,
    *,
    schema_name: str,
    source_key: str,
    snapshot_id: str,
    predecessor_snapshot_id: str,
    rollback_owner_id: str,
) -> dict[str, Any]:
    """Load the exact source/pin/attestation state for replay validation."""

    activation_result = await session.execute(
        db.text(
            _REVIEWED_ACTIVATION_SQL.replace(
                "__SCHEMA__",
                _quote_ident(schema_name),
            )
        ),
        {
            "source_key": source_key,
            "snapshot_id": snapshot_id,
            "predecessor_snapshot_id": predecessor_snapshot_id,
            "rollback_owner_type": ROLLBACK_PIN_OWNER_TYPE,
            "rollback_owner_id": rollback_owner_id,
        },
    )
    return _mapping(activation_result.one_or_none())


def _require_exact_reviewed_activation(
    activation_row: Mapping[str, Any],
    *,
    source_key: str,
    snapshot_id: str,
    predecessor_snapshot_id: str,
    expected_attestation_digest: bytes,
) -> None:
    activation = _mapping(_mapping(activation_row.get("manifest")).get("activation"))
    stored_digest = bytes(activation_row.get("attestation_digest") or b"")
    is_exact = (
        activation_row.get("published_at") is not None
        and str(activation_row.get("previous_snapshot_id") or "")
        == predecessor_snapshot_id
        and activation.get("state") == "activated"
        and activation.get("mode") == "reviewed_audit_only_control"
        and str(activation.get("source_key") or "").strip().lower()
        == source_key
        and activation_row.get("activation_intent")
        == PTG2_CANDIDATE_ACTIVATION_INTENT_AUDIT_ONLY
        and activation_row.get("activated_at") is not None
        and len(stored_digest) == 32
        and hmac.compare_digest(stored_digest, expected_attestation_digest)
        and activation_row.get("current_snapshot_id") == snapshot_id
        and activation_row.get("current_previous_snapshot_id")
        == predecessor_snapshot_id
        and str(activation_row.get("rollback_pin_reason") or "").strip()
        and int(activation_row.get("plan_source_count") or 0) > 0
        and int(activation_row.get("conflicting_plan_source_count") or 0) == 0
    )
    if not is_exact:
        raise PTG2SourcePointerConflict(
            "published candidate does not match the exact reviewed activation"
        )


def _completed_activation_result(
    activation_row: Mapping[str, Any],
    *,
    source_key: str,
    snapshot_id: str,
    predecessor_snapshot_id: str,
    rollback_owner_id: str,
) -> dict[str, Any]:
    return {
        "status": "already_promoted",
        "source_key": source_key,
        "snapshot_id": snapshot_id,
        "storage_generation": activation_row["storage_generation"],
        "previous_snapshot_id": predecessor_snapshot_id,
        "plan_source_count": int(activation_row["plan_source_count"]),
        "global_pointer": "reconciled",
        "rollback_owner_id": rollback_owner_id,
        "idempotent": True,
    }
