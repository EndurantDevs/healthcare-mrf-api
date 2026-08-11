# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Controlled, one-way revocation for a reviewed numeric-grid alias."""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass

from sqlalchemy import text

from db.models import db
from process.address_numeric_grid_alias import _alias_state, _insert_run, _mark_failed
from process.address_numeric_grid_alias_support import (
    _relation,
    _reviewer,
    _statement_timeout,
)
from process.ext import address_alias_sql


@dataclass(frozen=True)
class NumericGridAliasRevokeResult:
    run_id: str
    status: str
    source_address_key: str
    target_address_key: str
    revoked_reason: str
    revoked_by: str
    generation: int


def _uuid(value: str | None, *, name: str) -> str:
    normalized = str(value or "").strip().lower()
    try:
        return str(uuid.UUID(normalized))
    except (ValueError, TypeError):
        raise ValueError(f"{name} must be a valid UUID") from None


def _reason(value: str | None) -> str:
    reason = str(value or "").strip()
    if not reason:
        raise ValueError("revoke requires a non-empty reason")
    return reason[:2000]


async def revoke_numeric_grid_alias(
    *,
    source_address_key: str,
    expected_target_address_key: str,
    reason: str,
    reviewed_by: str,
    schema: str | None = None,
    timeout: str = "30s",
) -> NumericGridAliasRevokeResult:
    """Revoke exactly one active alias and advance the active-set generation."""
    db_schema = schema or os.getenv("HLTHPRT_DB_SCHEMA") or os.getenv("DB_SCHEMA") or "mrf"
    source_key = _uuid(source_address_key, name="source_address_key")
    target_key = _uuid(
        expected_target_address_key,
        name="expected_target_address_key",
    )
    revoke_reason = _reason(reason)
    reviewer = _reviewer(reviewed_by)
    normalized_timeout = _statement_timeout(timeout)
    run_id = str(uuid.uuid4())
    await _insert_run(
        schema=db_schema,
        run_id=run_id,
        mode="revoke",
        state_code=None,
        zip_prefix=None,
        shadow_run_id=None,
        reviewed_digest=None,
        reviewed_by=reviewer,
    )
    aliases = _relation(db_schema, address_alias_sql.ADDRESS_ALIAS_TABLE)
    runs = _relation(db_schema, address_alias_sql.ADDRESS_ALIAS_RUN_TABLE)
    try:
        async with db.transaction() as session:
            await session.execute(
                text(f"SET LOCAL lock_timeout = '{normalized_timeout}';")
            )
            await session.execute(
                text(f"SET LOCAL statement_timeout = '{normalized_timeout}';")
            )
            await session.execute(text(address_alias_sql.alias_advisory_xact_lock_sql()))
            await _alias_state(session, schema=db_schema, lock=True)
            active = (
                await session.execute(
                    text(
                        f"""
                        SELECT source_address_key::text, target_address_key::text
                        FROM {aliases}
                        WHERE source_address_key = CAST(:source_key AS uuid)
                          AND revoked_at IS NULL
                        FOR UPDATE;
                        """
                    ),
                    {"source_key": source_key},
                )
            ).first()
            if active is None:
                raise RuntimeError("active numeric-grid alias was not found")
            if str(active.target_address_key) != target_key:
                raise RuntimeError(
                    "active alias target differs from expected target: "
                    f"active={active.target_address_key} expected={target_key}"
                )
            await session.execute(
                text(
                    f"""
                    UPDATE {aliases}
                       SET revoked_at = now(),
                           revoked_reason = :reason,
                           revoked_by = :reviewed_by,
                           revoke_run_id = CAST(:run_id AS uuid),
                           updated_at = now()
                     WHERE source_address_key = CAST(:source_key AS uuid)
                       AND revoked_at IS NULL;
                    """
                ),
                {
                    "source_key": source_key,
                    "reason": revoke_reason,
                    "reviewed_by": reviewer,
                    "run_id": run_id,
                },
            )
            _, _, generation = await _alias_state(
                session,
                schema=db_schema,
                lock=False,
            )
            reason_buckets = {
                "source_address_key": source_key,
                "target_address_key": target_key,
                "revoked_reason": revoke_reason,
            }
            await session.execute(
                text(
                    f"""
                    UPDATE {runs}
                       SET status = 'revoked',
                           reviewed_at = now(),
                           reason_buckets = CAST(:reason_buckets AS jsonb),
                           completed_at = now()
                     WHERE run_id = CAST(:run_id AS uuid);
                    """
                ),
                {
                    "reason_buckets": json.dumps(reason_buckets, sort_keys=True),
                    "run_id": run_id,
                },
            )
        return NumericGridAliasRevokeResult(
            run_id=run_id,
            status="revoked",
            source_address_key=source_key,
            target_address_key=target_key,
            revoked_reason=revoke_reason,
            revoked_by=reviewer,
            generation=generation,
        )
    except Exception as exc:
        await _mark_failed(db_schema, run_id, exc)
        raise

