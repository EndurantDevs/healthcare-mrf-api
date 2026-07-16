# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Helpers for source-scoped PTG2 current-snapshot pointers."""

from __future__ import annotations

import datetime
import json
import os
from dataclasses import dataclass
from typing import Any

from db.connection import db
from process.ptg_parts.allowed_amounts import PTG2_ALLOWED_AMOUNT_CONTRACT
from process.ptg_parts.canonical import semantic_hash
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.domain import (
    PTG2_CANDIDATE_ACTIVATION_CONTRACT,
    PTG2_DOMAIN_ALLOWED_AMOUNT,
    PTG2_STATUS_PUBLISHED,
    PTG2_STATUS_VALIDATED,
)
from process.ptg_parts.ptg2_shared_blocks import bind_snapshot_to_shared_layout
from process.ptg_parts.ptg2_candidate_attestation import (
    consume_candidate_audit_attestation_in_transaction,
    verify_candidate_audit_attestation_in_transaction,
)
from process.ptg_parts.ptg2_lifecycle_lock import (
    PTG2_SOURCE_POINTER_GC_LOCK_KEY,
    acquire_ptg2_lifecycle_lock,
)
from process.ptg_parts.snapshot_tables import _normalize_source_key


_GLOBAL_SNAPSHOT_POINTER_RECONCILIATION_SQL = """
WITH publish_lock AS MATERIALIZED (
    SELECT pg_advisory_xact_lock(hashtext(:publish_lock_key))
), candidate_snapshot AS MATERIALIZED (
    SELECT candidate.snapshot_id, candidate.published_at
      FROM __SCHEMA__.ptg2_snapshot AS candidate,
           publish_lock
     WHERE candidate.snapshot_id = :snapshot_id
       AND candidate.status = 'published'
), updated_global_pointer AS (
    INSERT INTO __SCHEMA__.ptg2_current_snapshot AS current_pointer
        (slot, snapshot_id, previous_snapshot_id, updated_at)
    SELECT 'current', candidate_snapshot.snapshot_id, NULL, :updated_at
      FROM candidate_snapshot
    ON CONFLICT (slot) DO UPDATE SET
        snapshot_id = EXCLUDED.snapshot_id,
        previous_snapshot_id = CASE
            WHEN current_pointer.snapshot_id = EXCLUDED.snapshot_id
            THEN current_pointer.previous_snapshot_id
            ELSE current_pointer.snapshot_id
        END,
        updated_at = EXCLUDED.updated_at
    WHERE current_pointer.snapshot_id = EXCLUDED.snapshot_id
       OR NOT EXISTS (
            SELECT 1
              FROM __SCHEMA__.ptg2_snapshot AS incumbent
             WHERE incumbent.snapshot_id = current_pointer.snapshot_id
               AND incumbent.status = 'published'
               AND incumbent.published_at >= (
                    SELECT published_at FROM candidate_snapshot
               )
       )
    RETURNING snapshot_id
)
SELECT snapshot_id FROM updated_global_pointer
UNION ALL
SELECT current_pointer.snapshot_id
  FROM __SCHEMA__.ptg2_current_snapshot AS current_pointer,
       candidate_snapshot
 WHERE current_pointer.slot = 'current'
   AND NOT EXISTS (SELECT 1 FROM updated_global_pointer)
LIMIT 1
"""


class PTG2SourcePointerConflict(RuntimeError):
    """Raised when a source pointer changed after an import observed it."""


def _allowed_source_pointer_key(source_key: str) -> str:
    """Return the current-source key reserved for allowed evidence."""

    pointer_key = _normalize_source_key(f"{source_key}_allowed_amounts")
    if not pointer_key:
        raise ValueError("allowed-amount source pointer requires a source key")
    return pointer_key


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


def _row_mapping(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    return dict(getattr(row, "_mapping", row))


def candidate_snapshot_attributes(
    snapshot_attributes: dict[str, Any],
    *,
    source_key: str,
    previous_snapshot_id: str | None,
) -> dict[str, Any]:
    """Return immutable-layout metadata that is validated but not live."""

    candidate = dict(snapshot_attributes)
    manifest = _manifest_mapping(candidate.get("manifest"))
    manifest["activation"] = {
        "contract": PTG2_CANDIDATE_ACTIVATION_CONTRACT,
        "state": "validated",
        "source_key": str(source_key),
        "expected_previous_snapshot_id": (
            str(previous_snapshot_id) if previous_snapshot_id else None
        ),
    }
    candidate.update(
        {
            "status": PTG2_STATUS_VALIDATED,
            "published_at": None,
            "previous_snapshot_id": previous_snapshot_id,
            "manifest": manifest,
        }
    )
    return candidate


def activated_snapshot_attributes(
    candidate_attributes: dict[str, Any],
    *,
    activated_at: datetime.datetime,
    activation_mode: str,
) -> dict[str, Any]:
    """Return the published state written atomically with live pointers."""

    activated = dict(candidate_attributes)
    manifest = _manifest_mapping(activated.get("manifest"))
    activation = _manifest_mapping(manifest.get("activation"))
    if activation.get("contract") != PTG2_CANDIDATE_ACTIVATION_CONTRACT:
        raise ValueError("strict V3 snapshot is missing its candidate activation contract")
    activation.update(
        {
            "state": "activated",
            "mode": str(activation_mode),
            "activated_at": activated_at.isoformat(),
        }
    )
    manifest["activation"] = activation
    activated.update(
        {
            "status": PTG2_STATUS_PUBLISHED,
            "published_at": activated_at,
            "manifest": manifest,
        }
    )
    return activated


def _ptg2_plan_source_key(
    plan_id: str,
    plan_market_type: str | None,
    import_month: datetime.date,
    source_key: str = "",
) -> str:
    # source_key MUST be part of the pointer key: a plan served by multiple
    # network sources (e.g. a medical network plus a pharmacy carve-out) needs
    # one current-pointer row per source. Without it, whichever source
    # publishes last overwrites the others' rows via ON CONFLICT, and
    # current_source_snapshot_ids_for_plan only ever sees one network.
    payload = {
        "plan_id": plan_id,
        "plan_market_type": plan_market_type or "",
        "import_month": import_month.isoformat(),
    }
    if source_key:
        payload["source_key"] = source_key
    return semantic_hash(payload, domain="ptg2_current_plan_source")[:32]


def _plan_pointer_entry(
    *,
    plan_id: str,
    plan_market_type: str,
    import_month: datetime.date,
    source_key: str,
    snapshot_id: str,
    previous_snapshot_id: str | None,
    updated_at: datetime.datetime,
) -> dict[str, Any]:
    normalized_plan_id = str(plan_id or "").strip()
    normalized_market_type = str(plan_market_type or "").strip().lower()
    if not normalized_plan_id or not normalized_market_type:
        raise ValueError("strict V3 plan identity is incomplete")
    return {
        "plan_source_key": _ptg2_plan_source_key(
            normalized_plan_id,
            normalized_market_type,
            import_month,
            source_key,
        ),
        "plan_id": normalized_plan_id,
        "plan_market_type": normalized_market_type,
        "import_month": import_month,
        "source_key": source_key,
        "snapshot_id": snapshot_id,
        "previous_snapshot_id": previous_snapshot_id,
        "updated_at": updated_at,
    }


async def _current_source_snapshot_id(source_key: str) -> str | None:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    row = await db.first(
        f"""
        SELECT snapshot_id
          FROM {_quote_ident(schema_name)}.ptg2_current_source_snapshot
         WHERE source_key = :source_key
         LIMIT 1
        """,
        source_key=source_key,
    )
    if row is None:
        return None
    return str(row[0]) if row[0] else None


async def _source_plan_rows(
    *,
    snapshot_id: str,
    source_key: str,
    import_month: datetime.date,
    previous_snapshot_id: str | None,
    updated_at: datetime.datetime,
) -> list[dict[str, Any]]:
    """Build pointer rows from public snapshot scope and retained plan metadata."""
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    rows = await db.all(
        f"""
        SELECT DISTINCT plans.plan_id, plans.plan_market_type
          FROM (
                SELECT scope.plan_id,
                       COALESCE(scope.plan_market_type, '') AS plan_market_type
                  FROM {_quote_ident(schema_name)}.ptg2_v3_snapshot_scope AS scope
                 WHERE scope.snapshot_id = :snapshot_id
                UNION ALL
                SELECT plan.plan_id,
                       COALESCE(plan.plan_market_type, '') AS plan_market_type
                  FROM {_quote_ident(schema_name)}.ptg2_plan_month AS plan_month
                  JOIN {_quote_ident(schema_name)}.ptg2_plan AS plan
                    ON plan.plan_hash = plan_month.plan_hash
                 WHERE plan_month.snapshot_id = :snapshot_id
          ) AS plans
         WHERE plans.plan_id IS NOT NULL
           AND plans.plan_id <> ''
        """,
        snapshot_id=snapshot_id,
    )
    result: list[dict[str, Any]] = []
    for row in rows:
        data = row if isinstance(row, dict) else row._mapping
        plan_id = str(data.get("plan_id") or "").strip()
        if not plan_id:
            continue
        plan_market_type = str(data.get("plan_market_type") or "").strip().lower()
        result.append(
            _plan_pointer_entry(
                plan_id=plan_id,
                plan_market_type=plan_market_type,
                import_month=import_month,
                source_key=source_key,
                snapshot_id=snapshot_id,
                previous_snapshot_id=previous_snapshot_id,
                updated_at=updated_at,
            )
        )
    return result


def _has_result_row(query_result: Any) -> bool:
    if query_result is None:
        return True
    return query_result.first() is not None


async def _acquire_source_pointer_gc_lock(session: Any) -> None:
    await acquire_ptg2_lifecycle_lock(session)


async def _compare_and_swap_source_pointer(
    session: Any,
    *,
    schema_name: str,
    source_key: str,
    snapshot_id: str,
    previous_snapshot_id: str | None,
    import_month: datetime.date,
    updated_at: datetime.datetime,
    allow_already_current: bool = True,
) -> None:
    """Atomically replace a source pointer at its expected value."""
    cas_query_result = await session.execute(
        db.text(
            f"""
            WITH publish_lock AS MATERIALIZED (
                SELECT pg_advisory_xact_lock(hashtext(:publish_lock_key))
            ), updated_pointer AS (
                UPDATE {_quote_ident(schema_name)}.ptg2_current_source_snapshot AS current_pointer
                   SET snapshot_id = :snapshot_id,
                       previous_snapshot_id = :previous_snapshot_id,
                       import_month = :import_month,
                       updated_at = :updated_at
                  FROM publish_lock
                 WHERE current_pointer.source_key = :source_key
                   AND (
                        current_pointer.snapshot_id IS NOT DISTINCT FROM :previous_snapshot_id
                        OR (
                            :allow_already_current
                            AND current_pointer.snapshot_id = :snapshot_id
                        )
                   )
                RETURNING current_pointer.snapshot_id
            ), inserted_pointer AS (
                INSERT INTO {_quote_ident(schema_name)}.ptg2_current_source_snapshot
                    (source_key, snapshot_id, previous_snapshot_id, import_month, updated_at)
                SELECT :source_key, :snapshot_id, :previous_snapshot_id, :import_month, :updated_at
                  FROM publish_lock
                 WHERE :previous_snapshot_id IS NULL
                ON CONFLICT (source_key) DO NOTHING
                RETURNING snapshot_id
            )
            SELECT snapshot_id FROM updated_pointer
            UNION ALL
            SELECT snapshot_id FROM inserted_pointer
            LIMIT 1
            """
        ),
        {
            "publish_lock_key": PTG2_SOURCE_POINTER_GC_LOCK_KEY,
            "source_key": source_key,
            "snapshot_id": snapshot_id,
            "previous_snapshot_id": previous_snapshot_id,
            "import_month": import_month,
            "updated_at": updated_at,
            "allow_already_current": bool(allow_already_current),
        },
    )
    if not _has_result_row(cas_query_result):
        raise PTG2SourcePointerConflict(
            f"PTG source pointer changed after import planning for {source_key}; "
            f"expected {previous_snapshot_id or '<none>'}"
        )


async def _stage_snapshot_in_pointer_transaction(
    session: Any,
    *,
    schema_name: str,
    snapshot_attributes: dict[str, Any],
) -> None:
    if snapshot_attributes.get("status") != PTG2_STATUS_VALIDATED:
        raise ValueError("Candidate staging requires a validated snapshot row")
    staging_result = await session.execute(
        db.text(
            f"""
            WITH staged AS (
                UPDATE {_quote_ident(schema_name)}.ptg2_snapshot
                   SET import_run_id = :import_run_id,
                       import_month = :import_month,
                       status = :status,
                       created_at = :created_at,
                       validated_at = :validated_at,
                       published_at = NULL,
                       previous_snapshot_id = :previous_snapshot_id,
                       manifest = CAST(:manifest_json AS json)
                 WHERE snapshot_id = :snapshot_id
                   AND status = 'building'
                RETURNING status
            )
            SELECT status FROM staged
            UNION ALL
            SELECT existing.status
              FROM {_quote_ident(schema_name)}.ptg2_snapshot AS existing
             WHERE existing.snapshot_id = :snapshot_id
               AND existing.status = 'validated'
               AND existing.import_run_id IS NOT DISTINCT FROM :import_run_id
               AND existing.import_month IS NOT DISTINCT FROM :import_month
               AND existing.created_at IS NOT DISTINCT FROM :created_at
               AND existing.validated_at IS NOT DISTINCT FROM :validated_at
               AND existing.published_at IS NULL
               AND existing.previous_snapshot_id IS NOT DISTINCT FROM :previous_snapshot_id
               AND existing.manifest::jsonb = CAST(:manifest_json AS jsonb)
               AND NOT EXISTS (SELECT 1 FROM staged)
            LIMIT 1
            """
        ),
        {
            **snapshot_attributes,
            "manifest_json": json.dumps(
                snapshot_attributes.get("manifest") or {},
                default=str,
            ),
        },
    )
    if not _has_result_row(staging_result):
        raise RuntimeError(
            f"PTG snapshot {snapshot_attributes.get('snapshot_id')} could not be "
            "staged as a validated candidate"
        )


async def _publish_snapshot_in_pointer_transaction(
    session: Any,
    *,
    schema_name: str,
    snapshot_attributes: dict[str, Any] | None,
) -> None:
    """Publish snapshot metadata within a pointer transaction."""
    if snapshot_attributes is None:
        return
    if snapshot_attributes.get("status") != PTG2_STATUS_PUBLISHED:
        raise ValueError("Atomic source-pointer promotion requires a published snapshot row")
    manifest = _manifest_mapping(snapshot_attributes.get("manifest"))
    activation = _manifest_mapping(manifest.get("activation"))
    if activation.get("contract") == PTG2_CANDIDATE_ACTIVATION_CONTRACT:
        publication_query_result = await session.execute(
            db.text(
                f"""
                UPDATE {_quote_ident(schema_name)}.ptg2_snapshot
                   SET status = :status,
                       published_at = :published_at,
                       manifest = jsonb_set(
                           COALESCE(manifest::jsonb, '{{}}'::jsonb),
                           '{{activation}}',
                           CAST(:activation_json AS jsonb),
                           true
                       )::json
                 WHERE snapshot_id = :snapshot_id
                   AND status = 'validated'
                   AND previous_snapshot_id IS NOT DISTINCT FROM :previous_snapshot_id
                   AND manifest->'activation'->>'contract'
                       = :candidate_activation_contract
                   AND manifest->'activation'->>'state' = 'validated'
                RETURNING status
                """
            ),
            {
                "snapshot_id": snapshot_attributes["snapshot_id"],
                "status": snapshot_attributes["status"],
                "published_at": snapshot_attributes["published_at"],
                "previous_snapshot_id": snapshot_attributes.get(
                    "previous_snapshot_id"
                ),
                "candidate_activation_contract": (
                    PTG2_CANDIDATE_ACTIVATION_CONTRACT
                ),
                "activation_json": json.dumps(activation, default=str),
            },
        )
        if not _has_result_row(publication_query_result):
            raise RuntimeError(
                f"PTG candidate {snapshot_attributes.get('snapshot_id')} changed "
                "during audited source-pointer promotion"
            )
        return
    publication_query_result = await session.execute(
        db.text(
            f"""
            WITH updated_snapshot AS (
                UPDATE {_quote_ident(schema_name)}.ptg2_snapshot
                   SET import_run_id = :import_run_id,
                       import_month = :import_month,
                       status = :status,
                       created_at = :created_at,
                       validated_at = :validated_at,
                       published_at = :published_at,
                       previous_snapshot_id = :previous_snapshot_id,
                       manifest = CAST(:manifest_json AS json)
                 WHERE snapshot_id = :snapshot_id
                   AND status = 'validated'
                   AND COALESCE(manifest->'activation'->>'contract', '')
                       <> :candidate_activation_contract
                RETURNING status
            )
            SELECT status FROM updated_snapshot
            UNION ALL
            SELECT status
             FROM {_quote_ident(schema_name)}.ptg2_snapshot
             WHERE snapshot_id = :snapshot_id
               AND status = 'published'
               AND COALESCE(manifest->'activation'->>'contract', '')
                   <> :candidate_activation_contract
               AND NOT EXISTS (SELECT 1 FROM updated_snapshot)
            LIMIT 1
            """
        ),
        {
            **snapshot_attributes,
            "candidate_activation_contract": PTG2_CANDIDATE_ACTIVATION_CONTRACT,
            "manifest_json": json.dumps(
                snapshot_attributes.get("manifest") or {},
                default=str,
            ),
        },
    )
    if not _has_result_row(publication_query_result):
        raise RuntimeError(
            f"PTG snapshot {snapshot_attributes.get('snapshot_id')} disappeared "
            "during source-pointer promotion"
        )


async def _reconcile_global_snapshot_pointer(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    updated_at: datetime.datetime,
) -> None:
    """Advance a stale global pointer without replacing a newer publication."""
    reconciliation_sql = _GLOBAL_SNAPSHOT_POINTER_RECONCILIATION_SQL.replace(
        "__SCHEMA__",
        _quote_ident(schema_name),
    )
    reconciliation_result = await session.execute(
        db.text(reconciliation_sql),
        {
            "publish_lock_key": PTG2_SOURCE_POINTER_GC_LOCK_KEY,
            "snapshot_id": snapshot_id,
            "updated_at": updated_at,
        },
    )
    if not _has_result_row(reconciliation_result):
        raise RuntimeError(
            f"PTG snapshot {snapshot_id} was not available for global pointer reconciliation"
        )


async def _replace_source_plan_pointers(
    session: Any,
    *,
    schema_name: str,
    source_key: str,
    plan_pointer_entries: list[dict[str, Any]],
) -> None:
    await session.execute(
        db.text(
            f"DELETE FROM {_quote_ident(schema_name)}.ptg2_current_plan_source WHERE source_key = :source_key"
        ),
        {"source_key": source_key},
    )
    for plan_pointer_entry in plan_pointer_entries:
        await session.execute(
            db.text(
                f"""
                INSERT INTO {_quote_ident(schema_name)}.ptg2_current_plan_source
                    (plan_source_key, plan_id, plan_market_type, import_month, source_key, snapshot_id, previous_snapshot_id, updated_at)
                VALUES
                    (:plan_source_key, :plan_id, :plan_market_type, :import_month, :source_key, :snapshot_id, :previous_snapshot_id, :updated_at)
                ON CONFLICT (plan_source_key) DO UPDATE SET
                    plan_id = EXCLUDED.plan_id,
                    plan_market_type = EXCLUDED.plan_market_type,
                    import_month = EXCLUDED.import_month,
                    source_key = EXCLUDED.source_key,
                    snapshot_id = EXCLUDED.snapshot_id,
                    previous_snapshot_id = EXCLUDED.previous_snapshot_id,
                    updated_at = EXCLUDED.updated_at
                """
            ),
            plan_pointer_entry,
        )


async def _bind_snapshot_coverage_scope(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    coverage_scope_id: bytes,
    coverage_plan_id: str,
    coverage_plan_market_type: str,
    plan_pointer_entries: list[dict[str, Any]],
) -> None:
    """Bind logical plan ownership to one immutable physical coverage scope."""

    scope_id = bytes(coverage_scope_id)
    if len(scope_id) != 32:
        raise ValueError("strict V3 coverage scope id must contain exactly 32 bytes")
    distinct_plans = {
        (
            str(entry.get("plan_id") or "").strip(),
            str(entry.get("plan_market_type") or "").strip().lower(),
        )
        for entry in plan_pointer_entries
        if str(entry.get("plan_id") or "").strip()
    }
    if not distinct_plans:
        raise RuntimeError("strict V3 snapshot has no logical plan for its coverage scope")
    expected_plan = (
        str(coverage_plan_id or "").strip(),
        str(coverage_plan_market_type or "").strip().lower(),
    )
    if not expected_plan[0]:
        raise ValueError("strict V3 publication requires its logical coverage plan id")
    if distinct_plans != {expected_plan}:
        raise RuntimeError(
            f"PTG snapshot {snapshot_id} plan pointers do not match its immutable coverage scope"
        )
    for plan_id, plan_market_type in sorted(distinct_plans):
        result = await session.execute(
            db.text(
                f"""
                INSERT INTO {_quote_ident(schema_name)}.ptg2_v3_snapshot_scope
                    (snapshot_id, plan_id, plan_market_type, coverage_scope_id)
                VALUES
                    (:snapshot_id, :plan_id, :plan_market_type, :coverage_scope_id)
                ON CONFLICT (snapshot_id) DO UPDATE SET
                    coverage_scope_id = EXCLUDED.coverage_scope_id
                WHERE {_quote_ident(schema_name)}.ptg2_v3_snapshot_scope.plan_id
                      = EXCLUDED.plan_id
                  AND {_quote_ident(schema_name)}.ptg2_v3_snapshot_scope.plan_market_type
                      = EXCLUDED.plan_market_type
                  AND {_quote_ident(schema_name)}.ptg2_v3_snapshot_scope.coverage_scope_id
                      = EXCLUDED.coverage_scope_id
                RETURNING coverage_scope_id
                """
            ),
            {
                "snapshot_id": snapshot_id,
                "plan_id": plan_id,
                "plan_market_type": plan_market_type,
                "coverage_scope_id": scope_id,
            },
        )
        if not _has_result_row(result):
            raise RuntimeError(
                f"PTG snapshot {snapshot_id} is already bound to a different logical plan or coverage scope"
            )
    observed = await session.scalar(
        db.text(
            f"""
            SELECT COUNT(*)
              FROM {_quote_ident(schema_name)}.ptg2_v3_snapshot_scope
             WHERE snapshot_id = :snapshot_id
            """
        ),
        {"snapshot_id": snapshot_id},
    )
    if int(observed or 0) != len(distinct_plans):
        raise RuntimeError(
            f"PTG snapshot {snapshot_id} has stale logical coverage-scope mappings"
        )


async def _stage_ptg2_source_candidate(
    *,
    source_key: str,
    snapshot_id: str,
    previous_snapshot_id: str | None,
    import_month: datetime.date,
    updated_at: datetime.datetime,
    snapshot_attributes: dict[str, Any],
    shared_snapshot_key: int,
    coverage_scope_id: bytes,
    coverage_plan_id: str,
    coverage_plan_market_type: str,
) -> dict[str, Any]:
    """Bind a sealed V3 layout without changing any live serving pointer."""

    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    candidate_attributes = candidate_snapshot_attributes(
        snapshot_attributes,
        source_key=source_key,
        previous_snapshot_id=previous_snapshot_id,
    )
    async with db.transaction() as session:
        await _acquire_source_pointer_gc_lock(session)
        await bind_snapshot_to_shared_layout(
            session,
            schema_name=schema_name,
            snapshot_id=snapshot_id,
            snapshot_key=int(shared_snapshot_key),
        )
        plan_pointer_entries = [
            _plan_pointer_entry(
                plan_id=coverage_plan_id,
                plan_market_type=coverage_plan_market_type,
                import_month=import_month,
                source_key=source_key,
                snapshot_id=snapshot_id,
                previous_snapshot_id=previous_snapshot_id,
                updated_at=updated_at,
            )
        ]
        await _bind_snapshot_coverage_scope(
            session,
            schema_name=schema_name,
            snapshot_id=snapshot_id,
            coverage_scope_id=coverage_scope_id,
            coverage_plan_id=coverage_plan_id,
            coverage_plan_market_type=coverage_plan_market_type,
            plan_pointer_entries=plan_pointer_entries,
        )
        await _stage_snapshot_in_pointer_transaction(
            session,
            schema_name=schema_name,
            snapshot_attributes=candidate_attributes,
        )
    return {
        "status": "validated",
        "source_key": source_key,
        "snapshot_id": snapshot_id,
        "previous_snapshot_id": previous_snapshot_id,
        "plan_source_count": len(plan_pointer_entries),
        "candidate_attributes": candidate_attributes,
    }


async def _locked_snapshot_publication_row(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
) -> dict[str, Any]:
    result = await session.execute(
        db.text(
            f"""
            SELECT snapshot.snapshot_id,
                   snapshot.import_run_id,
                   snapshot.import_month,
                   snapshot.status,
                   snapshot.created_at,
                   snapshot.validated_at,
                   snapshot.published_at,
                   snapshot.previous_snapshot_id,
                   snapshot.manifest
              FROM {_quote_ident(schema_name)}.ptg2_snapshot AS snapshot
             WHERE snapshot.snapshot_id = :snapshot_id
             FOR UPDATE OF snapshot
            """
        ),
        {"snapshot_id": snapshot_id},
    )
    row = result.first()
    if row is None:
        raise ValueError("PTG snapshot is unavailable for publication")
    return _row_mapping(row)


def _requires_audited_candidate_activation(snapshot: dict[str, Any]) -> bool:
    activation = _manifest_mapping(
        _manifest_mapping(snapshot.get("manifest")).get("activation")
    )
    return (
        str(snapshot.get("status") or "").strip().lower()
        == PTG2_STATUS_VALIDATED
        and activation.get("contract") == PTG2_CANDIDATE_ACTIVATION_CONTRACT
    )


async def _locked_candidate_activation_row(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
) -> dict[str, Any]:
    result = await session.execute(
        db.text(
            f"""
            SELECT snapshot.snapshot_id,
                   snapshot.import_run_id,
                   snapshot.import_month,
                   snapshot.status,
                   snapshot.created_at,
                   snapshot.validated_at,
                   snapshot.published_at,
                   snapshot.previous_snapshot_id,
                   snapshot.manifest,
                   binding.snapshot_key,
                   scope.plan_id,
                   scope.plan_market_type,
                   scope.coverage_scope_id
              FROM {_quote_ident(schema_name)}.ptg2_snapshot AS snapshot
              JOIN {_quote_ident(schema_name)}.ptg2_v3_snapshot_binding AS binding
                ON binding.snapshot_id = snapshot.snapshot_id
              JOIN {_quote_ident(schema_name)}.ptg2_v3_snapshot_scope AS scope
                ON scope.snapshot_id = snapshot.snapshot_id
              JOIN {_quote_ident(schema_name)}.ptg2_v3_snapshot_layout AS layout
                ON layout.snapshot_key = binding.snapshot_key
             WHERE snapshot.snapshot_id = :snapshot_id
               AND layout.state = 'sealed'
               AND layout.generation = 'shared_blocks_v3'
             FOR UPDATE OF snapshot
            """
        ),
        {"snapshot_id": snapshot_id},
    )
    row = result.one_or_none()
    if row is None:
        raise ValueError("validated candidate is unavailable")
    return _row_mapping(row)


async def _database_utc_timestamp(session: Any) -> datetime.datetime:
    result = await session.execute(
        db.text("SELECT timezone('UTC', clock_timestamp())")
    )
    timestamp = result.scalar_one()
    if not isinstance(timestamp, datetime.datetime):
        raise RuntimeError("PostgreSQL did not return an activation timestamp")
    return timestamp.replace(tzinfo=None)


def _validated_activation_identity(
    candidate: dict[str, Any],
    *,
    source_key: str,
    expected_current_snapshot_id: str | None,
) -> dict[str, Any]:
    if str(candidate.get("status") or "").strip().lower() != PTG2_STATUS_VALIDATED:
        raise ValueError("snapshot is not a validated candidate")
    manifest = _manifest_mapping(candidate.get("manifest"))
    activation = _manifest_mapping(manifest.get("activation"))
    if (
        activation.get("contract") != PTG2_CANDIDATE_ACTIVATION_CONTRACT
        or activation.get("state") != "validated"
    ):
        raise ValueError("snapshot is missing its strict V3 activation contract")
    normalized_source_key = str(source_key or "").strip().lower()
    if str(activation.get("source_key") or "").strip().lower() != normalized_source_key:
        raise ValueError("snapshot source_key does not match requested source_key")
    previous_snapshot_id = (
        str(activation.get("expected_previous_snapshot_id") or "").strip() or None
    )
    row_previous_snapshot_id = (
        str(candidate.get("previous_snapshot_id") or "").strip() or None
    )
    if row_previous_snapshot_id != previous_snapshot_id:
        raise ValueError(
            "candidate predecessor disagrees with its immutable activation contract"
        )
    if expected_current_snapshot_id is not None and (
        str(expected_current_snapshot_id or "").strip() or None
    ) != previous_snapshot_id:
        raise PTG2SourcePointerConflict(
            "requested predecessor does not match the candidate"
        )
    plan_id = str(candidate.get("plan_id") or "").strip()
    plan_market_type = str(candidate.get("plan_market_type") or "").strip().lower()
    coverage_scope_id = bytes(candidate.get("coverage_scope_id") or b"")
    if not plan_id or not plan_market_type or len(coverage_scope_id) != 32:
        raise ValueError("validated candidate has an incomplete immutable scope")
    return {
        "source_key": normalized_source_key,
        "previous_snapshot_id": previous_snapshot_id,
        "plan_id": plan_id,
        "plan_market_type": plan_market_type,
        "coverage_scope_id": coverage_scope_id,
        "snapshot_key": int(candidate["snapshot_key"]),
    }


def _validated_allowed_activation_identity(
    candidate: dict[str, Any],
    *,
    source_key: str,
) -> dict[str, Any] | None:
    """Validate an optional allowed-evidence pointer bound to a candidate."""

    manifest = _manifest_mapping(candidate.get("manifest"))
    raw_allowed_index = manifest.get("allowed_amount_index")
    if raw_allowed_index is None:
        return None
    if not isinstance(raw_allowed_index, dict):
        raise ValueError("candidate allowed-amount index must be an object")
    allowed_index = dict(raw_allowed_index)
    normalized_source_key = str(source_key or "").strip().lower()
    current_source_key = _allowed_source_pointer_key(normalized_source_key)
    required_contract_by_field = {
        "contract": PTG2_ALLOWED_AMOUNT_CONTRACT,
        "arch_version": "postgres_binary_v3",
        "storage": "postgresql",
        "data_domain": PTG2_DOMAIN_ALLOWED_AMOUNT,
        "source_key": normalized_source_key,
        "current_source_key": current_source_key,
    }
    for field_name, required_value in required_contract_by_field.items():
        if allowed_index.get(field_name) != required_value:
            raise ValueError(
                "candidate allowed-amount index has an invalid "
                f"{field_name} binding"
            )
    if allowed_index.get("snapshot_scoped") is not True:
        raise ValueError(
            "candidate allowed-amount index is not snapshot scoped"
        )
    try:
        payment_count = int(allowed_index.get("allowed_amount_payments") or 0)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "candidate allowed-amount payment count is invalid"
        ) from exc
    if not allowed_index.get("allowed_amount_evidence") or payment_count <= 0:
        raise ValueError(
            "candidate allowed-amount index has no payment evidence"
        )
    previous_snapshot_id = (
        str(allowed_index.get("previous_snapshot_id") or "").strip() or None
    )
    return {
        "source_key": current_source_key,
        "previous_snapshot_id": previous_snapshot_id,
    }


async def activate_ptg2_source_candidate(
    *,
    source_key: str,
    snapshot_id: str,
    expected_current_snapshot_id: str | None = None,
) -> dict[str, Any]:
    """Audit and activate one authoritative candidate in a single transaction."""

    normalized_source_key = str(source_key or "").strip().lower()
    normalized_snapshot_id = str(snapshot_id or "").strip()
    if not normalized_source_key or not normalized_snapshot_id:
        raise ValueError("source_key and snapshot_id are required")
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    async with db.transaction() as session:
        await _acquire_source_pointer_gc_lock(session)
        return await _activate_ptg2_source_candidate_in_transaction(
            session,
            schema_name=schema_name,
            source_key=normalized_source_key,
            snapshot_id=normalized_snapshot_id,
            expected_current_snapshot_id=expected_current_snapshot_id,
        )


@dataclass(frozen=True)
class _CandidateActivationContext:
    candidate: dict[str, Any]
    activation_by_field: dict[str, Any]
    allowed_activation_by_field: dict[str, Any] | None
    activated_at: datetime.datetime
    import_month: datetime.date
    plan_pointer_entries: list[dict[str, Any]]


async def _candidate_activation_context(
    session: Any,
    *,
    schema_name: str,
    source_key: str,
    snapshot_id: str,
    expected_current_snapshot_id: str | None,
) -> _CandidateActivationContext:
    candidate = await _locked_candidate_activation_row(
        session,
        schema_name=schema_name,
        snapshot_id=snapshot_id,
    )
    activation_by_field = _validated_activation_identity(
        candidate,
        source_key=source_key,
        expected_current_snapshot_id=expected_current_snapshot_id,
    )
    allowed_activation_by_field = _validated_allowed_activation_identity(
        candidate,
        source_key=source_key,
    )
    activated_at = await _database_utc_timestamp(session)
    import_month = candidate.get("import_month")
    if not isinstance(import_month, datetime.date):
        raise ValueError("validated candidate has no import month")
    plan_pointer_entries = [
        _plan_pointer_entry(
            plan_id=activation_by_field["plan_id"],
            plan_market_type=activation_by_field["plan_market_type"],
            import_month=import_month,
            source_key=source_key,
            snapshot_id=snapshot_id,
            previous_snapshot_id=activation_by_field["previous_snapshot_id"],
            updated_at=activated_at,
        )
    ]
    return _CandidateActivationContext(
        candidate=candidate,
        activation_by_field=activation_by_field,
        allowed_activation_by_field=allowed_activation_by_field,
        activated_at=activated_at,
        import_month=import_month,
        plan_pointer_entries=plan_pointer_entries,
    )


async def _activate_candidate_source_pointer(
    session: Any,
    *,
    schema_name: str,
    source_key: str,
    snapshot_id: str,
    activation_context: _CandidateActivationContext,
) -> bytes:
    activation_by_field = activation_context.activation_by_field
    audit_report_digest = await verify_candidate_audit_attestation_in_transaction(
        session,
        schema_name=schema_name,
        snapshot_id=snapshot_id,
        snapshot_key=activation_by_field["snapshot_key"],
        source_key=source_key,
        plan_id=activation_by_field["plan_id"],
        plan_market_type=activation_by_field["plan_market_type"],
        coverage_scope_id=activation_by_field["coverage_scope_id"],
    )
    await _compare_and_swap_source_pointer(
        session,
        schema_name=schema_name,
        source_key=source_key,
        snapshot_id=snapshot_id,
        previous_snapshot_id=activation_by_field["previous_snapshot_id"],
        import_month=activation_context.import_month,
        updated_at=activation_context.activated_at,
        allow_already_current=False,
    )
    return audit_report_digest


async def _promote_allowed_amount_pointer(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    activation_context: _CandidateActivationContext,
) -> dict[str, Any] | None:
    allowed_activation_by_field = activation_context.allowed_activation_by_field
    if allowed_activation_by_field is None:
        return None
    await _compare_and_swap_source_pointer(
        session,
        schema_name=schema_name,
        source_key=allowed_activation_by_field["source_key"],
        snapshot_id=snapshot_id,
        previous_snapshot_id=allowed_activation_by_field[
            "previous_snapshot_id"
        ],
        import_month=activation_context.import_month,
        updated_at=activation_context.activated_at,
        allow_already_current=False,
    )
    return {
        "status": "promoted",
        "source_key": allowed_activation_by_field["source_key"],
        "snapshot_id": snapshot_id,
        "previous_snapshot_id": allowed_activation_by_field[
            "previous_snapshot_id"
        ],
    }


async def _persist_candidate_activation(
    session: Any,
    *,
    schema_name: str,
    source_key: str,
    snapshot_id: str,
    activation_context: _CandidateActivationContext,
    audit_report_digest: bytes,
) -> None:
    candidate_attributes_by_field = {
        key: activation_context.candidate.get(key)
        for key in (
            "snapshot_id",
            "import_run_id",
            "import_month",
            "status",
            "created_at",
            "validated_at",
            "published_at",
            "previous_snapshot_id",
            "manifest",
        )
    }
    await _publish_snapshot_in_pointer_transaction(
        session,
        schema_name=schema_name,
        snapshot_attributes=activated_snapshot_attributes(
            candidate_attributes_by_field,
            activated_at=activation_context.activated_at,
            activation_mode="audited_control",
        ),
    )
    await _reconcile_global_snapshot_pointer(
        session,
        schema_name=schema_name,
        snapshot_id=snapshot_id,
        updated_at=activation_context.activated_at,
    )
    await _replace_source_plan_pointers(
        session,
        schema_name=schema_name,
        source_key=source_key,
        plan_pointer_entries=activation_context.plan_pointer_entries,
    )
    await consume_candidate_audit_attestation_in_transaction(
        session,
        schema_name=schema_name,
        snapshot_id=snapshot_id,
        report_digest=audit_report_digest,
        activated_at=activation_context.activated_at,
    )


def _candidate_activation_result(
    *,
    source_key: str,
    snapshot_id: str,
    activation_context: _CandidateActivationContext,
    allowed_pointer_by_field: dict[str, Any] | None,
) -> dict[str, Any]:
    result_by_field = {
        "status": "promoted",
        "source_key": source_key,
        "snapshot_id": snapshot_id,
        "previous_snapshot_id": activation_context.activation_by_field[
            "previous_snapshot_id"
        ],
        "plan_source_count": 1,
        "global_pointer": "reconciled",
    }
    if allowed_pointer_by_field is not None:
        result_by_field["allowed_amount_pointer"] = allowed_pointer_by_field
    return result_by_field


async def _activate_ptg2_source_candidate_in_transaction(
    session: Any,
    *,
    schema_name: str,
    source_key: str,
    snapshot_id: str,
    expected_current_snapshot_id: str | None,
) -> dict[str, Any]:
    """Activate a source candidate within its pointer transaction."""
    activation_context = await _candidate_activation_context(
        session,
        schema_name=schema_name,
        source_key=source_key,
        snapshot_id=snapshot_id,
        expected_current_snapshot_id=expected_current_snapshot_id,
    )
    audit_report_digest = await _activate_candidate_source_pointer(
        session,
        schema_name=schema_name,
        source_key=source_key,
        snapshot_id=snapshot_id,
        activation_context=activation_context,
    )
    allowed_pointer_by_field = await _promote_allowed_amount_pointer(
        session,
        schema_name=schema_name,
        snapshot_id=snapshot_id,
        activation_context=activation_context,
    )
    await _persist_candidate_activation(
        session,
        schema_name=schema_name,
        source_key=source_key,
        snapshot_id=snapshot_id,
        activation_context=activation_context,
        audit_report_digest=audit_report_digest,
    )
    return _candidate_activation_result(
        source_key=source_key,
        snapshot_id=snapshot_id,
        activation_context=activation_context,
        allowed_pointer_by_field=allowed_pointer_by_field,
    )


async def _publish_ptg2_source_pointers(
    *,
    source_key: str,
    snapshot_id: str,
    previous_snapshot_id: str | None,
    import_month: datetime.date,
    updated_at: datetime.datetime,
    snapshot_attributes: dict[str, Any] | None = None,
    shared_snapshot_key: int | None = None,
    coverage_scope_id: bytes | None = None,
    coverage_plan_id: str | None = None,
    coverage_plan_market_type: str | None = None,
    require_audit_attestation: bool = False,
) -> dict[str, Any]:
    """Publish or activate source pointers under the strict audit contract."""

    normalized_source_key = str(source_key or "").strip().lower()
    normalized_snapshot_id = str(snapshot_id or "").strip()
    if not normalized_source_key or not normalized_snapshot_id:
        raise ValueError("source_key and snapshot_id are required")
    if snapshot_attributes is not None and (
        str(snapshot_attributes.get("snapshot_id") or "").strip()
        != normalized_snapshot_id
    ):
        raise ValueError("snapshot attributes do not match the requested snapshot")
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    async with db.transaction() as session:
        await _acquire_source_pointer_gc_lock(session)
        authoritative_snapshot = await _locked_snapshot_publication_row(
            session,
            schema_name=schema_name,
            snapshot_id=normalized_snapshot_id,
        )
        if require_audit_attestation or _requires_audited_candidate_activation(
            authoritative_snapshot
        ):
            return await _activate_ptg2_source_candidate_in_transaction(
                session,
                schema_name=schema_name,
                source_key=normalized_source_key,
                snapshot_id=normalized_snapshot_id,
                expected_current_snapshot_id=previous_snapshot_id,
            )
        if shared_snapshot_key is not None:
            await bind_snapshot_to_shared_layout(
                session,
                schema_name=schema_name,
                snapshot_id=normalized_snapshot_id,
                snapshot_key=int(shared_snapshot_key),
            )
        plan_pointer_entries = await _source_plan_rows(
            snapshot_id=normalized_snapshot_id,
            source_key=normalized_source_key,
            import_month=import_month,
            previous_snapshot_id=previous_snapshot_id,
            updated_at=updated_at,
        )
        if shared_snapshot_key is not None:
            if coverage_scope_id is None:
                raise ValueError("strict V3 publication requires a coverage scope id")
            await _bind_snapshot_coverage_scope(
                session,
                schema_name=schema_name,
                snapshot_id=normalized_snapshot_id,
                coverage_scope_id=coverage_scope_id,
                coverage_plan_id=str(coverage_plan_id or ""),
                coverage_plan_market_type=str(coverage_plan_market_type or ""),
                plan_pointer_entries=plan_pointer_entries,
            )
        if snapshot_attributes is None and (
            str(authoritative_snapshot.get("status") or "").strip().lower()
            != PTG2_STATUS_PUBLISHED
        ):
            raise ValueError(
                "source-pointer repoint requires an already published snapshot"
            )
        await _compare_and_swap_source_pointer(
            session,
            schema_name=schema_name,
            source_key=normalized_source_key,
            snapshot_id=normalized_snapshot_id,
            previous_snapshot_id=previous_snapshot_id,
            import_month=import_month,
            updated_at=updated_at,
        )
        await _publish_snapshot_in_pointer_transaction(
            session,
            schema_name=schema_name,
            snapshot_attributes=snapshot_attributes,
        )
        if snapshot_attributes is not None:
            await _reconcile_global_snapshot_pointer(
                session,
                schema_name=schema_name,
                snapshot_id=normalized_snapshot_id,
                updated_at=updated_at,
            )
        await _replace_source_plan_pointers(
            session,
            schema_name=schema_name,
            source_key=normalized_source_key,
            plan_pointer_entries=plan_pointer_entries,
        )
    return {
        "status": "promoted",
        "source_key": normalized_source_key,
        "snapshot_id": normalized_snapshot_id,
        "previous_snapshot_id": previous_snapshot_id,
        "plan_source_count": len(plan_pointer_entries),
        "global_pointer": "reconciled" if snapshot_attributes is not None else "not_requested",
    }


async def _publish_ptg2_global_snapshot_pointer(
    *,
    snapshot_attributes: dict[str, Any],
    updated_at: datetime.datetime,
    shared_snapshot_key: int | None = None,
) -> dict[str, Any]:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    snapshot_id = str(snapshot_attributes.get("snapshot_id") or "").strip()
    if not snapshot_id:
        raise ValueError("Global snapshot-pointer promotion requires a snapshot id")
    async with db.transaction() as session:
        await _acquire_source_pointer_gc_lock(session)
        authoritative_snapshot = await _locked_snapshot_publication_row(
            session,
            schema_name=schema_name,
            snapshot_id=snapshot_id,
        )
        authoritative_activation = _manifest_mapping(
            _manifest_mapping(authoritative_snapshot.get("manifest")).get("activation")
        )
        if (
            authoritative_activation.get("contract")
            == PTG2_CANDIDATE_ACTIVATION_CONTRACT
        ):
            raise ValueError(
                "strict V3 candidates must be activated through the audited source-pointer transaction"
            )
        if shared_snapshot_key is not None:
            await bind_snapshot_to_shared_layout(
                session,
                schema_name=schema_name,
                snapshot_id=snapshot_id,
                snapshot_key=int(shared_snapshot_key),
            )
        await _publish_snapshot_in_pointer_transaction(
            session,
            schema_name=schema_name,
            snapshot_attributes=snapshot_attributes,
        )
        await _reconcile_global_snapshot_pointer(
            session,
            schema_name=schema_name,
            snapshot_id=snapshot_id,
            updated_at=updated_at,
        )
    return {
        "status": "promoted",
        "snapshot_id": snapshot_id,
        "global_pointer": "reconciled",
    }
