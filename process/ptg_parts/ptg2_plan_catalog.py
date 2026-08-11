# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded immutable compatibility writes for the shared PTG plan catalog."""

from __future__ import annotations

import json
from typing import Any, Mapping, Sequence

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_lifecycle_lock import (
    configure_ptg2_lifecycle_transaction,
    is_retryable_lifecycle_database_error,
)
from process.ptg_parts.ptg2_schema import resolve_ptg2_schema


_PLAN_CATALOG_LOCK_TIMEOUT = "50ms"
_PLAN_CATALOG_STATEMENT_TIMEOUT = "500ms"
_PLAN_FIELDS = (
    "plan_hash",
    "hash_prefix",
    "plan_id",
    "plan_id_type",
    "plan_name",
    "plan_market_type",
    "issuer_name",
    "plan_sponsor_name",
    "canonical_payload",
)
_ALIAS_FIELDS = (
    "alias_hash",
    "plan_hash",
    "alias_type",
    "alias_value",
)


class PTG2PlanCatalogConflict(RuntimeError):
    """A content-addressed plan identity resolved to different metadata."""


def _row_mapping(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return dict(row)
    return dict(getattr(row, "_mapping", row))


def _json_value(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _has_same_immutable_payload(
    actual: Mapping[str, Any],
    expected: Mapping[str, Any],
    fields: Sequence[str],
) -> bool:
    for field_name in fields:
        actual_value = actual.get(field_name)
        expected_value = expected.get(field_name)
        if field_name == "canonical_payload":
            actual_value = _json_value(actual_value)
            expected_value = _json_value(expected_value)
        if actual_value != expected_value:
            return False
    return True


async def _insert_and_validate_plans(
    session: Any,
    *,
    schema: str,
    plan_rows: Sequence[Mapping[str, Any]],
) -> None:
    for plan_row in plan_rows:
        await session.execute(
            db.text(
                f"""
                INSERT INTO {schema}.ptg2_plan
                    (plan_hash, hash_prefix, plan_id, plan_id_type, plan_name,
                     plan_market_type, issuer_name, plan_sponsor_name,
                     canonical_payload, created_at)
                VALUES
                    (:plan_hash, :hash_prefix, :plan_id, :plan_id_type,
                     :plan_name, :plan_market_type, :issuer_name,
                     :plan_sponsor_name, CAST(:canonical_payload AS json),
                     :created_at)
                ON CONFLICT (plan_hash) DO NOTHING
                """
            ),
            {
                **dict(plan_row),
                "canonical_payload": json.dumps(
                    _json_value(plan_row.get("canonical_payload")),
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            },
        )
        stored_plan_result = await session.execute(
            db.text(
                f"SELECT {', '.join(_PLAN_FIELDS)} "
                f"FROM {schema}.ptg2_plan WHERE plan_hash = :plan_hash"
            ),
            {"plan_hash": plan_row["plan_hash"]},
        )
        stored_row = stored_plan_result.one_or_none()
        if stored_row is None or not _has_same_immutable_payload(
            _row_mapping(stored_row),
            plan_row,
            _PLAN_FIELDS,
        ):
            raise PTG2PlanCatalogConflict(
                "PTG immutable plan catalog payload conflicts with its hash"
            )


async def _insert_and_validate_aliases(
    session: Any,
    *,
    schema: str,
    alias_rows: Sequence[Mapping[str, Any]],
) -> None:
    for alias_row in alias_rows:
        await session.execute(
            db.text(
                f"""
                INSERT INTO {schema}.ptg2_plan_alias
                    (alias_hash, plan_hash, alias_type, alias_value, created_at)
                VALUES
                    (:alias_hash, :plan_hash, :alias_type, :alias_value,
                     :created_at)
                ON CONFLICT (alias_hash) DO NOTHING
                """
            ),
            dict(alias_row),
        )
        stored_alias_result = await session.execute(
            db.text(
                f"SELECT {', '.join(_ALIAS_FIELDS)} "
                f"FROM {schema}.ptg2_plan_alias WHERE alias_hash = :alias_hash"
            ),
            {"alias_hash": alias_row["alias_hash"]},
        )
        stored_row = stored_alias_result.one_or_none()
        if stored_row is None or not _has_same_immutable_payload(
            _row_mapping(stored_row),
            alias_row,
            _ALIAS_FIELDS,
        ):
            raise PTG2PlanCatalogConflict(
                "PTG immutable plan alias payload conflicts with its hash"
            )


async def attempt_publish_immutable_plan_catalog(
    *,
    plan_rows: Sequence[Mapping[str, Any]],
    alias_rows: Sequence[Mapping[str, Any]],
) -> str:
    """Insert-once exact plan metadata without gating source-local layout."""

    if not plan_rows and not alias_rows:
        return "unchanged"
    schema = _quote_ident(resolve_ptg2_schema())
    try:
        async with db.transaction() as session:
            await configure_ptg2_lifecycle_transaction(
                session,
                lock_timeout=_PLAN_CATALOG_LOCK_TIMEOUT,
                statement_timeout=_PLAN_CATALOG_STATEMENT_TIMEOUT,
            )
            await _insert_and_validate_plans(
                session,
                schema=schema,
                plan_rows=plan_rows,
            )
            await _insert_and_validate_aliases(
                session,
                schema=schema,
                alias_rows=alias_rows,
            )
    except PTG2PlanCatalogConflict:
        raise
    except Exception as exc:
        if not is_retryable_lifecycle_database_error(exc):
            raise
        return "deferred"
    return "persisted"


__all__ = [
    "PTG2PlanCatalogConflict",
    "attempt_publish_immutable_plan_catalog",
]
