# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Plan and execute one durable legacy PTG V3 metadata repair."""

from __future__ import annotations

import datetime as dt
import os
import re
from dataclasses import dataclass
from typing import Any, Mapping

from sqlalchemy import text

from db.connection import db
from db.migration_ptg2_legacy_v3_metadata_reconcile import (
    LEGACY_V3_RECONCILE_CONTRACT,
)
from process.ptg_parts.ptg2_legacy_v3_metadata_contract import (
    build_legacy_v3_reconcile_plan,
    legacy_v3_target_digest,
)
from process.ptg_parts.ptg2_legacy_v3_metadata_store import (
    LegacyV3ReconcileWrite,
    apply_legacy_v3_reconcile_rows,
    load_legacy_v3_reconcile_observation,
    lock_legacy_v3_reconcile_relations,
)
from process.ptg_parts.ptg2_legacy_v3_operational_absence import (
    load_exact_operational_absence,
)
from process.ptg_parts.ptg2_lifecycle_lock import (
    acquire_ptg2_lifecycle_lock,
)
from process.ptg_parts.ptg2_schema import resolve_ptg2_schema
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg_source_attempt_guard import (
    canonical_digest,
    normalize_source_file_import_id,
    require_source_attempt_capabilities,
    source_attempt_lock_key,
)


_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,96}$")
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_SCHEMA_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
_PAIR_LOCK_NAMESPACE = "ptg2-legacy-v3-metadata-pair-v1"
_ATTEMPT_AUTHORITY_SCHEMA_ENV = "HLTHPRT_SOURCE_ATTEMPT_SCHEMA"


class LegacyV3MetadataConflict(RuntimeError):
    """One reviewed target changed or is not safely repairable."""


@dataclass(frozen=True)
class _ReconcileCoordinates:
    snapshot_id: str
    internal_run_id: str
    outer_run_id: str


def _coordinates(
    snapshot_id: str,
    internal_run_id: str,
    outer_run_id: str,
) -> _ReconcileCoordinates:
    return _ReconcileCoordinates(
        snapshot_id=_identifier(snapshot_id, "snapshot_id"),
        internal_run_id=_identifier(internal_run_id, "internal_run_id"),
        outer_run_id=_identifier(outer_run_id, "outer_run_id"),
    )


def _identifier(value: str, field_name: str) -> str:
    normalized = str(value or "").strip()
    if _IDENTIFIER_RE.fullmatch(normalized) is None:
        raise ValueError(
            f"{field_name} must be a 1-96 character PTG identifier"
        )
    return normalized


def _digest(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if _DIGEST_RE.fullmatch(normalized) is None:
        raise ValueError("expected_plan_digest must be a SHA-256 hex digest")
    return normalized


def _attempt_authority_schema_name() -> str:
    schema_name = str(os.getenv(_ATTEMPT_AUTHORITY_SCHEMA_ENV) or "")
    if _SCHEMA_IDENTIFIER_RE.fullmatch(schema_name) is None:
        raise LegacyV3MetadataConflict(
            "source-attempt authority schema is not configured correctly"
        )
    return schema_name


def _is_capability_unavailable(error: BaseException) -> bool:
    error_text = str(error)
    return (
        "PTG_SOURCE_ATTEMPT_CAPABILITY" in error_text
        or "does not exist" in error_text
    )


async def _has_required_capabilities(session: Any) -> bool:
    try:
        await require_source_attempt_capabilities(
            session,
            require_attempt_authority=True,
        )
    except Exception as error:
        if _is_capability_unavailable(error):
            return False
        raise
    return True


async def _require_complete_capabilities(session: Any) -> None:
    try:
        await require_source_attempt_capabilities(
            session,
            require_attempt_authority=True,
        )
    except Exception as error:
        if _is_capability_unavailable(error):
            raise LegacyV3MetadataConflict(
                "source-attempt authority capability is unavailable"
            ) from error
        raise


async def _database_observation(
    *,
    snapshot_id: str,
    internal_run_id: str,
    outer_run_id: str,
) -> tuple[dict[str, Any], bool]:
    async with db.transaction() as session:
        capability_ready = await _has_required_capabilities(session)
        observation = await load_legacy_v3_reconcile_observation(
            session,
            schema_name=resolve_ptg2_schema(),
            control_schema_name=_attempt_authority_schema_name(),
            snapshot_id=snapshot_id,
            internal_run_id=internal_run_id,
            outer_run_id=outer_run_id,
            lock_rows=False,
        )
    return observation, capability_ready


async def plan_legacy_v3_metadata_reconcile(
    *,
    snapshot_id: str,
    internal_run_id: str,
    outer_run_id: str,
) -> dict[str, Any]:
    """Return one redacted no-write plan for an exact stale V3 pair."""

    resolved_snapshot_id = _identifier(snapshot_id, "snapshot_id")
    resolved_internal_run_id = _identifier(
        internal_run_id,
        "internal_run_id",
    )
    resolved_outer_run_id = _identifier(outer_run_id, "outer_run_id")
    observation, capability_ready = await _database_observation(
        snapshot_id=resolved_snapshot_id,
        internal_run_id=resolved_internal_run_id,
        outer_run_id=resolved_outer_run_id,
    )
    operational_evidence = await load_exact_operational_absence(
        observation.get("outer_runs") or [],
        observation.get("event_rows") or [],
    )
    return build_legacy_v3_reconcile_plan(
        observation,
        operational_evidence,
        snapshot_id=resolved_snapshot_id,
        internal_run_id=resolved_internal_run_id,
        outer_run_id=resolved_outer_run_id,
        observed_at=dt.datetime.now(dt.UTC),
        capabilities_ready=capability_ready,
    )


async def _initial_source_file_import_id(
    session: Any,
    *,
    schema_name: str,
    internal_run_id: str,
) -> str:
    result = await session.execute(
        text(
            f"SELECT options::jsonb->>'source_file_import_id' "
            f"FROM {_quote_ident(schema_name)}.ptg2_import_run "
            "WHERE import_run_id = :internal_run_id"
        ),
        {"internal_run_id": internal_run_id},
    )
    return normalize_source_file_import_id(result.scalar_one_or_none())


async def _lock_reconcile_target(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    internal_run_id: str,
    outer_run_id: str,
) -> str:
    source_file_import_id = await _initial_source_file_import_id(
        session,
        schema_name=schema_name,
        internal_run_id=internal_run_id,
    )
    await session.execute(
        text(
            "SELECT pg_advisory_xact_lock("
            "hashtextextended(:lock_key, 0))"
        ),
        {"lock_key": source_attempt_lock_key(source_file_import_id)},
    )
    await acquire_ptg2_lifecycle_lock(session)
    target_digest = legacy_v3_target_digest(
        snapshot_id=snapshot_id,
        internal_run_id=internal_run_id,
        outer_run_id=outer_run_id,
    )
    await session.execute(
        text(
            "SELECT pg_advisory_xact_lock("
            "hashtextextended(:pair_lock_key, 0))"
        ),
        {"pair_lock_key": f"{_PAIR_LOCK_NAMESPACE}:{target_digest}"},
    )
    await lock_legacy_v3_reconcile_relations(
        session,
        schema_name=schema_name,
        control_schema_name=_attempt_authority_schema_name(),
    )
    return source_file_import_id


def _marker(
    *,
    source_file_import_id: str,
    snapshot_id: str,
    internal_run_id: str,
    outer_run_id: str,
    plan: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "contract": LEGACY_V3_RECONCILE_CONTRACT,
        "source_file_import_id": source_file_import_id,
        "snapshot_id": snapshot_id,
        "internal_run_id": internal_run_id,
        "outer_run_id": outer_run_id,
        "target_digest": plan["target_digest"],
        "plan_digest": plan["plan_digest"],
        "attachment_digest": plan["attachment_digest"],
        "catalog_digest": plan["catalog_digest"],
        "event_high_water_mark": plan["event_high_water_mark"],
        "retained_state_digest": plan["retained_state_digest"],
        "preserved_row_digest": plan["preserved_row_digest"],
    }


async def _locked_observation(
    session: Any,
    *,
    schema_name: str,
    coordinates: _ReconcileCoordinates,
) -> tuple[str, dict[str, Any]]:
    await _require_complete_capabilities(session)
    source_file_import_id = await _lock_reconcile_target(
        session,
        schema_name=schema_name,
        snapshot_id=coordinates.snapshot_id,
        internal_run_id=coordinates.internal_run_id,
        outer_run_id=coordinates.outer_run_id,
    )
    await _require_complete_capabilities(session)
    observation = await load_legacy_v3_reconcile_observation(
        session,
        schema_name=schema_name,
        control_schema_name=_attempt_authority_schema_name(),
        snapshot_id=coordinates.snapshot_id,
        internal_run_id=coordinates.internal_run_id,
        outer_run_id=coordinates.outer_run_id,
        lock_rows=True,
    )
    if observation.get("source_file_import_id") != source_file_import_id:
        raise LegacyV3MetadataConflict(
            "source attempt changed while acquiring lifecycle authority"
        )
    return source_file_import_id, observation


def _review_locked_plan(
    observation: Mapping[str, Any],
    operational_evidence: Mapping[str, Any],
    *,
    coordinates: _ReconcileCoordinates,
    reviewed_digest: str,
) -> dict[str, Any]:
    plan = build_legacy_v3_reconcile_plan(
        observation,
        operational_evidence,
        snapshot_id=coordinates.snapshot_id,
        internal_run_id=coordinates.internal_run_id,
        outer_run_id=coordinates.outer_run_id,
        observed_at=dt.datetime.now(dt.UTC),
        capabilities_ready=True,
    )
    if plan["status"] == "already_reconciled":
        if plan["plan_digest"] != reviewed_digest:
            raise LegacyV3MetadataConflict(
                "completed reconciliation does not match reviewed digest"
            )
        return plan
    if plan["status"] != "ready":
        raise LegacyV3MetadataConflict(
            "legacy V3 target is not eligible: "
            + ",".join(plan["reason_codes"])
        )
    if plan["plan_digest"] != reviewed_digest:
        raise LegacyV3MetadataConflict(
            "legacy V3 state changed after plan review"
        )
    return plan


async def _write_reconciliation(
    session: Any,
    *,
    schema_name: str,
    coordinates: _ReconcileCoordinates,
    source_file_import_id: str,
    plan: Mapping[str, Any],
) -> str:
    marker = _marker(
        source_file_import_id=source_file_import_id,
        snapshot_id=coordinates.snapshot_id,
        internal_run_id=coordinates.internal_run_id,
        outer_run_id=coordinates.outer_run_id,
        plan=plan,
    )
    reconciliation_id = canonical_digest(marker)
    await apply_legacy_v3_reconcile_rows(
        session,
        LegacyV3ReconcileWrite(
            schema_name=schema_name,
            snapshot_id=coordinates.snapshot_id,
            internal_run_id=coordinates.internal_run_id,
            source_file_import_id=source_file_import_id,
            outer_run_id=coordinates.outer_run_id,
            target_digest=plan["target_digest"],
            plan_digest=plan["plan_digest"],
            attachment_digest=plan["attachment_digest"],
            catalog_digest=plan["catalog_digest"],
            event_high_water_mark=int(plan["event_high_water_mark"]),
            reconciliation_id=reconciliation_id,
            marker=marker,
        ),
    )
    return reconciliation_id


async def _apply_reconcile_transaction(
    coordinates: _ReconcileCoordinates,
    *,
    reviewed_digest: str,
    operational_evidence: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], str | None]:
    schema_name = resolve_ptg2_schema()
    async with db.transaction() as session:
        source_file_import_id, observation = await _locked_observation(
            session,
            schema_name=schema_name,
            coordinates=coordinates,
        )
        plan = _review_locked_plan(
            observation,
            operational_evidence,
            coordinates=coordinates,
            reviewed_digest=reviewed_digest,
        )
        if plan["status"] == "already_reconciled":
            return plan, observation, None
        reconciliation_id = await _write_reconciliation(
            session,
            schema_name=schema_name,
            coordinates=coordinates,
            source_file_import_id=source_file_import_id,
            plan=plan,
        )
    return plan, observation, reconciliation_id


def _terminal_report(
    plan_by_field: Mapping[str, Any],
    *,
    state: str,
    reconciliation_id: str | None = None,
    postcheck_exact_external_absence: bool | None = None,
) -> dict[str, Any]:
    is_red = state == "applied_postcheck_red"
    report_by_field = {
        **plan_by_field,
        "state": state,
        "acceptance": "red" if is_red else "green",
        "retry_allowed": False,
        "operator_action": "stop_no_retry" if is_red else "none",
    }
    if reconciliation_id is not None:
        report_by_field["reconciliation_id"] = reconciliation_id
    if postcheck_exact_external_absence is not None:
        report_by_field["postcheck_exact_external_absence"] = (
            postcheck_exact_external_absence
        )
    return report_by_field


async def reconcile_legacy_v3_metadata(
    *,
    snapshot_id: str,
    internal_run_id: str,
    outer_run_id: str,
    expected_plan_digest: str,
) -> dict[str, Any]:
    """Apply one reviewed two-row CAS and append the durable fence."""

    coordinates = _coordinates(snapshot_id, internal_run_id, outer_run_id)
    reviewed_digest = _digest(expected_plan_digest)
    initial_observation, _ready = await _database_observation(
        snapshot_id=coordinates.snapshot_id,
        internal_run_id=coordinates.internal_run_id,
        outer_run_id=coordinates.outer_run_id,
    )
    prelock_operational_evidence = await load_exact_operational_absence(
        initial_observation.get("outer_runs") or [],
        initial_observation.get("event_rows") or [],
    )
    plan, observation, reconciliation_id = await _apply_reconcile_transaction(
        coordinates,
        reviewed_digest=reviewed_digest,
        operational_evidence=prelock_operational_evidence,
    )
    if plan["status"] == "already_reconciled":
        return _terminal_report(
            plan,
            state="already_reconciled",
        )
    postcheck = await load_exact_operational_absence(
        observation.get("outer_runs") or [],
        observation.get("event_rows") or [],
    )
    has_exact_absence = bool(postcheck.get("exact_external_absence"))
    if not has_exact_absence:
        return _terminal_report(
            {
                **plan,
                "reason_codes": sorted(
                    {
                        *plan.get("reason_codes", []),
                        "postcommit_external_identity_present",
                    }
                ),
            },
            state="applied_postcheck_red",
            reconciliation_id=reconciliation_id,
            postcheck_exact_external_absence=False,
        )
    return _terminal_report(
        plan,
        state="applied",
        reconciliation_id=reconciliation_id,
        postcheck_exact_external_absence=True,
    )


__all__ = [
    "LEGACY_V3_RECONCILE_CONTRACT",
    "LegacyV3MetadataConflict",
    "load_exact_operational_absence",
    "plan_legacy_v3_metadata_reconcile",
    "reconcile_legacy_v3_metadata",
]
