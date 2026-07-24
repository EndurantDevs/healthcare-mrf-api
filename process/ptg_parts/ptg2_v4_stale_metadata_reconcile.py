# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded reconciliation for one orphaned PTG V4 metadata-only build."""

from __future__ import annotations

import os
import re
from typing import Any, Mapping

from db.connection import db
from process.ptg_parts.ptg2_lifecycle_lock import acquire_ptg2_lifecycle_lock
from process.ptg_parts.ptg2_schema import resolve_ptg2_schema
from process.ptg_parts.ptg2_v4_snapshot_maps import PTG2_V4_SHARED_GENERATION
from process.ptg_parts.ptg2_v4_stale_metadata_authority import (
    timestamp_text,
)
from process.ptg_parts.ptg2_v4_stale_metadata_plan import (
    build_stale_plan,
    exact_stale_marker,
    stale_target_digest,
)
from process.ptg_parts.ptg2_v4_stale_metadata_report import (
    build_stale_marker,
    build_stale_report,
)
from process.ptg_parts.ptg2_v4_stale_metadata_store import (
    apply_stale_metadata_rows,
    load_stale_metadata_context,
)
from process.ptg_parts.ptg2_v4_stale_metadata_types import (
    PTG2V4StaleMetadataConflict,
    PTG2V4StaleMetadataContext,
    PTG2V4StaleMetadataRequest,
    PTG2V4StaleMetadataWrite,
    PTG2_V4_STALE_METADATA_CONTRACT,
    PTG2_V4_STALE_METADATA_MARKER,
    PTG2_V4_STALE_METADATA_SECONDS_DEFAULT,
    PTG2_V4_STALE_METADATA_SECONDS_ENV,
    PTG2_V4_STALE_METADATA_SECONDS_MINIMUM,
)


_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,96}$")
_PAIR_LOCK_NAMESPACE = "ptg2-v4-stale-metadata-pair-v1"


def _normalized_identifier(value: str, *, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not _IDENTIFIER_RE.fullmatch(normalized):
        raise ValueError(
            f"{field_name} must be a 1-96 character PTG identifier"
        )
    return normalized


def _normalized_plan_digest(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if not _DIGEST_RE.fullmatch(normalized):
        raise ValueError("expected_plan_digest must be a SHA-256 hex digest")
    return normalized


def _server_stale_after_seconds() -> int:
    raw_value = os.getenv(PTG2_V4_STALE_METADATA_SECONDS_ENV)
    if raw_value is None:
        return PTG2_V4_STALE_METADATA_SECONDS_DEFAULT
    try:
        configured_value = int(str(raw_value).strip())
    except ValueError as exc:
        raise RuntimeError(
            f"{PTG2_V4_STALE_METADATA_SECONDS_ENV} must be an integer"
        ) from exc
    if configured_value < PTG2_V4_STALE_METADATA_SECONDS_MINIMUM:
        raise RuntimeError(
            f"{PTG2_V4_STALE_METADATA_SECONDS_ENV} must be at least "
            f"{PTG2_V4_STALE_METADATA_SECONDS_MINIMUM}"
        )
    return configured_value


def _schema_name() -> str:
    return resolve_ptg2_schema()


async def plan_v4_stale_metadata(
    *,
    snapshot_id: str,
    internal_run_id: str,
) -> dict[str, Any]:
    """Return a redacted, no-write plan for one exact metadata-only target."""

    normalized_snapshot_id = _normalized_identifier(
        snapshot_id,
        field_name="snapshot_id",
    )
    normalized_internal_run_id = _normalized_identifier(
        internal_run_id,
        field_name="internal_run_id",
    )
    stale_after_seconds = _server_stale_after_seconds()
    target_digest = stale_target_digest(
        normalized_snapshot_id,
        normalized_internal_run_id,
    )
    async with db.transaction() as session:
        context = await load_stale_metadata_context(
            session,
            db,
            schema_name=_schema_name(),
            snapshot_id=normalized_snapshot_id,
            internal_run_id=normalized_internal_run_id,
            should_lock_rows=False,
        )
    return build_stale_plan(
        context,
        internal_run_id=normalized_internal_run_id,
        stale_after_seconds=stale_after_seconds,
        target_digest=target_digest,
    )


def _execute_request(
    *,
    snapshot_id: str,
    internal_run_id: str,
    expected_plan_digest: str,
) -> PTG2V4StaleMetadataRequest:
    normalized_snapshot_id = _normalized_identifier(
        snapshot_id,
        field_name="snapshot_id",
    )
    normalized_internal_run_id = _normalized_identifier(
        internal_run_id,
        field_name="internal_run_id",
    )
    stale_after_seconds = _server_stale_after_seconds()
    target_digest = stale_target_digest(
        normalized_snapshot_id,
        normalized_internal_run_id,
    )
    return PTG2V4StaleMetadataRequest(
        snapshot_id=normalized_snapshot_id,
        internal_run_id=normalized_internal_run_id,
        expected_plan_digest=_normalized_plan_digest(expected_plan_digest),
        stale_after_seconds=stale_after_seconds,
        target_digest=target_digest,
        schema_name=_schema_name(),
    )


async def _lock_stale_target(
    session: Any,
    request: PTG2V4StaleMetadataRequest,
) -> None:
    """Serialize the lifecycle before loading the existing reviewed fence."""

    await acquire_ptg2_lifecycle_lock(session)
    await session.execute(
        db.text(
            "SELECT pg_advisory_xact_lock("
            "hashtextextended(:pair_lock_key, 0))"
        ),
        {
            "pair_lock_key": (
                f"{_PAIR_LOCK_NAMESPACE}:{request.target_digest}"
            )
        },
    )


def _reviewed_marker(
    context: PTG2V4StaleMetadataContext,
    request: PTG2V4StaleMetadataRequest,
    plan_by_field: Mapping[str, Any],
) -> tuple[dict[str, Any], bool]:
    """Validate the reviewed digest and return a new or existing marker."""

    if plan_by_field["status"] == "already_reconciled":
        marker_by_field = exact_stale_marker(
            context,
            target_digest=request.target_digest,
        )
        assert marker_by_field is not None
        if marker_by_field["plan_digest"] != request.expected_plan_digest:
            raise PTG2V4StaleMetadataConflict(
                "expected plan digest does not match the completed "
                "reconciliation"
            )
        return marker_by_field, True
    if plan_by_field["status"] != "ready":
        joined_reasons = ",".join(plan_by_field["reason_codes"])
        raise PTG2V4StaleMetadataConflict(
            "stale-build target is not eligible: " + joined_reasons
        )
    if plan_by_field["plan_digest"] != request.expected_plan_digest:
        raise PTG2V4StaleMetadataConflict(
            "stale-build state changed after plan review"
        )
    return (
        build_stale_marker(
            plan_by_field=plan_by_field,
            reconciled_at_text=timestamp_text(context.observed_at) or "",
        ),
        False,
    )


async def _reconcile_locked(
    session: Any,
    request: PTG2V4StaleMetadataRequest,
) -> tuple[dict[str, Any], bool]:
    """Recheck and optionally apply one reviewed plan under all locks."""

    await _lock_stale_target(session, request)
    context = await load_stale_metadata_context(
        session,
        db,
        schema_name=request.schema_name,
        snapshot_id=request.snapshot_id,
        internal_run_id=request.internal_run_id,
        should_lock_rows=True,
    )
    plan_by_field = build_stale_plan(
        context,
        internal_run_id=request.internal_run_id,
        stale_after_seconds=request.stale_after_seconds,
        target_digest=request.target_digest,
    )
    marker_by_field, is_idempotent = _reviewed_marker(
        context,
        request,
        plan_by_field,
    )
    if not is_idempotent:
        await apply_stale_metadata_rows(
            session,
            db,
            stale_write=PTG2V4StaleMetadataWrite(
                schema_name=request.schema_name,
                snapshot_id=request.snapshot_id,
                internal_run_id=request.internal_run_id,
                context=context,
                marker_by_field=marker_by_field,
                v4_storage_generation=PTG2_V4_SHARED_GENERATION,
                stale_after_seconds=request.stale_after_seconds,
            ),
        )
    return marker_by_field, is_idempotent


async def reconcile_v4_stale_metadata(
    *,
    snapshot_id: str,
    internal_run_id: str,
    expected_plan_digest: str,
) -> dict[str, Any]:
    """Apply an exact reviewed plan without deleting or releasing any data."""

    request = _execute_request(
        snapshot_id=snapshot_id,
        internal_run_id=internal_run_id,
        expected_plan_digest=expected_plan_digest,
    )
    async with db.transaction() as session:
        marker_by_field, is_idempotent = await _reconcile_locked(
            session,
            request,
        )
    return build_stale_report(
        marker_by_field=marker_by_field,
        is_idempotent=is_idempotent,
    )


__all__ = [
    "PTG2V4StaleMetadataConflict",
    "PTG2_V4_STALE_METADATA_CONTRACT",
    "PTG2_V4_STALE_METADATA_MARKER",
    "plan_v4_stale_metadata",
    "reconcile_v4_stale_metadata",
]
