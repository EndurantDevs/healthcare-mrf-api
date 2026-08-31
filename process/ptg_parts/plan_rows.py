# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Canonical PTG plan and context row builders."""

from __future__ import annotations

import datetime
from typing import Any

from process.ptg_parts.canonical import (
    _canonicalize_for_json,
    hash_prefix,
    semantic_hash,
    semantic_sha256,
)
from process.ptg_parts.domain import PTG2_DOMAIN_IN_NETWORK, PTG2SourceVersion
from process.ptg_parts.progress import _utcnow


def _ptg2_context_row(
    plan_fields: dict[str, Any],
    import_month: datetime.date,
    source_version: PTG2SourceVersion | None,
) -> dict[str, Any]:
    payload = {
        "domain": PTG2_DOMAIN_IN_NETWORK,
        "plan": plan_fields,
        "import_month": import_month.isoformat(),
        "source_file_version_id": source_version.source_file_version_id if source_version else None,
    }
    context_hash = semantic_hash(payload, domain="rate_set_context")
    return {
        "context_hash": context_hash,
        "hash_prefix": hash_prefix(context_hash),
        "domain": PTG2_DOMAIN_IN_NETWORK,
        "canonical_payload": _canonicalize_for_json(payload),
        "created_at": _utcnow(),
    }


def _ptg2_plan_rows(
    plan_fields: dict[str, Any],
    snapshot_id: str,
    import_month: datetime.date,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    """Build canonical plan, alternate-id, and snapshot-plan rows."""

    plan_identity_by_field = {
        "plan_id": plan_fields.get("plan_id"),
        "plan_id_type": plan_fields.get("plan_id_type"),
        "plan_name": plan_fields.get("plan_name"),
        "plan_market_type": plan_fields.get("plan_market_type"),
        "issuer_name": plan_fields.get("issuer_name"),
        "plan_sponsor_name": plan_fields.get("plan_sponsor_name"),
    }
    plan_hash = semantic_sha256(plan_identity_by_field, domain="plan")
    plan_row_by_field = {
        "plan_hash": plan_hash,
        "hash_prefix": hash_prefix(plan_hash),
        **plan_identity_by_field,
        "canonical_payload": _canonicalize_for_json(plan_identity_by_field),
        "created_at": _utcnow(),
    }
    alias_rows = _ptg2_plan_alias_rows(plan_hash, plan_identity_by_field)
    plan_month_row_by_field = _ptg2_plan_month_row(
        plan_hash,
        snapshot_id,
        import_month,
    )
    return plan_row_by_field, alias_rows, plan_month_row_by_field


def _ptg2_plan_alias_rows(
    plan_hash: str,
    plan_identity_by_field: dict[str, Any],
) -> list[dict[str, Any]]:
    """Build the deterministic alternate identifiers for one plan."""

    alias_rows: list[dict[str, Any]] = []
    for alias_type, alias_value in (
        ("plan_id", plan_identity_by_field.get("plan_id")),
        ("plan_name", plan_identity_by_field.get("plan_name")),
    ):
        if not alias_value:
            continue
        alias_identity_by_field = {
            "plan_hash": plan_hash,
            "alias_type": alias_type,
            "alias_value": str(alias_value),
        }
        alias_hash = semantic_sha256(alias_identity_by_field, domain="plan_alias")
        alias_rows.append(
            {
                "alias_hash": alias_hash,
                "plan_hash": plan_hash,
                "alias_type": alias_type,
                "alias_value": str(alias_value),
                "created_at": _utcnow(),
            }
        )
    return alias_rows


def _ptg2_plan_month_row(
    plan_hash: str,
    snapshot_id: str,
    import_month: datetime.date,
) -> dict[str, Any]:
    """Build one logical plan-month scope row."""

    plan_month_identity_by_field = {
        "snapshot_id": snapshot_id,
        "plan_hash": plan_hash,
        "import_month": import_month.isoformat(),
    }
    plan_month_id = semantic_hash(
        plan_month_identity_by_field,
        domain="plan_month",
    )[:32]
    plan_month_row_by_field = {
        "plan_month_id": plan_month_id,
        "snapshot_id": snapshot_id,
        "plan_hash": plan_hash,
        "import_month": import_month,
        "created_at": _utcnow(),
    }
    return plan_month_row_by_field


__all__ = (
    "_ptg2_context_row",
    "_ptg2_plan_alias_rows",
    "_ptg2_plan_month_row",
    "_ptg2_plan_rows",
)
