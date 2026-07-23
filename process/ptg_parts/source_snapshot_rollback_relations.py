# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Locked strict-serving relations used by snapshot rollback."""

from __future__ import annotations

import datetime
from typing import Any

from db.connection import db


def _row_mapping(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    return dict(getattr(row, "_mapping", row))


async def _one(
    session: Any,
    statement: str,
    snapshot_id: str,
) -> dict[str, Any]:
    query_result = await session.execute(
        db.text(statement),
        {"snapshot_id": snapshot_id},
    )
    return _row_mapping(query_result.one_or_none())


async def load_target_snapshot_scope(
    session: Any,
    schema: str,
    snapshot_id: str,
) -> dict[str, Any]:
    """Lock the target snapshot's resolver scope."""

    return await _one(
        session,
        f"""
        SELECT snapshot_id, plan_id, plan_market_type, coverage_scope_id
          FROM {schema}.ptg2_v3_snapshot_scope
         WHERE snapshot_id = :snapshot_id
         FOR SHARE
        """,
        snapshot_id,
    )


async def load_target_attestation(
    session: Any,
    schema: str,
    snapshot_id: str,
) -> dict[str, Any]:
    """Lock the target snapshot's resolver attestation."""

    return await _one(
        session,
        f"""
        SELECT snapshot_id, snapshot_key, source_key, plan_id,
               plan_market_type, coverage_scope_id, contract, activated_at
          FROM {schema}.ptg2_v3_candidate_audit_attestation
         WHERE snapshot_id = :snapshot_id
         FOR SHARE
        """,
        snapshot_id,
    )


async def database_utc_timestamp(session: Any) -> datetime.datetime:
    """Read one database timestamp for all pointer mutations."""

    query_result = await session.execute(
        db.text("SELECT timezone('UTC', clock_timestamp())")
    )
    timestamp = query_result.scalar_one()
    if not isinstance(timestamp, datetime.datetime):
        raise RuntimeError("PostgreSQL did not return a rollback timestamp")
    return timestamp.replace(tzinfo=None)
