# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PTG2 serving-row builders shared by compact import paths."""

from __future__ import annotations

import datetime
import hashlib
from typing import Any

from api.code_systems import canonical_catalog_code, normalize_code_system
from process.ext.utils import return_checksum
from process.ptg_parts.domain import (
    PTG2_CONFIDENCE_NPPES_PRACTICE_LOCATION,
    PTG2_CONFIDENCE_TIC_RATE_NPI_TIN,
    ptg2_confidence_statement,
)
from process.ptg_parts.row_helpers import _normalize_code_component, _normalized_npi_list


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC).replace(tzinfo=None)


def _ptg2_hp_procedure_code(code_system: Any, code: Any) -> int | None:
    normalized_system = normalize_code_system(code_system)
    normalized_code = canonical_catalog_code(normalized_system, code) if normalized_system else _normalize_code_component(code)
    if not normalized_system or not normalized_code:
        return None
    if normalized_system == "MS_DRG":
        return None
    return return_checksum([normalized_system, normalized_code])


def _ptg2_serving_rate_row(
    *,
    snapshot_id: str,
    plan_fields: dict[str, Any],
    procedure_payload: dict[str, Any],
    rate_pack_row: dict[str, Any],
    provider_set_hashes: list[str],
    provider_count: int,
    provider_set_count: int,
    prices: list[dict[str, Any]] | None,
    source_trace: list[dict[str, Any]] | None,
    source_trace_set_hash: str | None = None,
    confidence: dict[str, Any] | None = None,
    confidence_code: str | None = None,
) -> dict[str, Any]:
    plan_id = str(plan_fields.get("plan_id") or "")
    raw_billing_code = procedure_payload.get("billing_code")
    billing_code = str(raw_billing_code or "")
    serving_rate_id = hashlib.md5(
        "|".join(
            [
                snapshot_id,
                plan_id,
                billing_code,
                str(rate_pack_row.get("rate_pack_hash") or ""),
            ]
        ).encode("utf-8")
    ).hexdigest()
    billing_code_type = procedure_payload.get("billing_code_type")
    reported_code_system = normalize_code_system(billing_code_type)
    reported_code = canonical_catalog_code(reported_code_system, raw_billing_code) if reported_code_system else _normalize_code_component(raw_billing_code)
    return {
        "serving_rate_id": serving_rate_id,
        "snapshot_id": snapshot_id,
        "plan_id": plan_id,
        "plan_name": plan_fields.get("plan_name"),
        "plan_id_type": plan_fields.get("plan_id_type"),
        "plan_market_type": plan_fields.get("plan_market_type"),
        "issuer_name": plan_fields.get("issuer_name"),
        "plan_sponsor_name": plan_fields.get("plan_sponsor_name"),
        "procedure_code": _ptg2_hp_procedure_code(reported_code_system, reported_code),
        "reported_code_system": reported_code_system,
        "reported_code": reported_code,
        "billing_code": billing_code,
        "billing_code_type": billing_code_type,
        "procedure_name": procedure_payload.get("name"),
        "procedure_description": procedure_payload.get("description"),
        "procedure_display_name": procedure_payload.get("name") or procedure_payload.get("description"),
        "rate_pack_hash": rate_pack_row.get("rate_pack_hash"),
        "provider_set_hash": rate_pack_row.get("provider_set_hash"),
        "provider_set_hashes": provider_set_hashes,
        "provider_count": provider_count,
        "provider_set_count": provider_set_count,
        "price_set_hash": rate_pack_row.get("price_set_hash"),
        "source_trace_set_hash": source_trace_set_hash,
        "confidence_code": confidence_code or PTG2_CONFIDENCE_TIC_RATE_NPI_TIN,
        "prices": prices,
        "source_trace": source_trace,
        "confidence": confidence
        if confidence is not None
        else {
            "network": PTG2_CONFIDENCE_TIC_RATE_NPI_TIN,
            "location": PTG2_CONFIDENCE_NPPES_PRACTICE_LOCATION,
            "acceptance_statement": ptg2_confidence_statement(PTG2_CONFIDENCE_TIC_RATE_NPI_TIN),
        },
        "created_at": _utcnow(),
    }


def _ptg2_compact_serving_rate_row(
    serving_row: dict[str, Any],
    *,
    plan_month_id: str,
    procedure_hash: str,
) -> dict[str, Any]:
    return {
        "serving_rate_id": serving_row.get("serving_rate_id"),
        "snapshot_id": serving_row.get("snapshot_id"),
        "plan_id": serving_row.get("plan_id"),
        "plan_month_id": plan_month_id,
        "procedure_hash": procedure_hash,
        "procedure_code": serving_row.get("procedure_code"),
        "reported_code_system": serving_row.get("reported_code_system"),
        "reported_code": serving_row.get("reported_code"),
        "billing_code": serving_row.get("billing_code"),
        "billing_code_type": serving_row.get("billing_code_type"),
        "rate_pack_hash": serving_row.get("rate_pack_hash"),
        "provider_set_hash": serving_row.get("provider_set_hash"),
        "provider_count": serving_row.get("provider_count"),
        "price_set_hash": serving_row.get("price_set_hash"),
        "source_trace_set_hash": serving_row.get("source_trace_set_hash"),
        "confidence_code": serving_row.get("confidence_code"),
        "created_at": serving_row.get("created_at") or _utcnow(),
    }


def _provider_group_member_rows(provider_entry: dict[str, Any]) -> list[dict[str, Any]]:
    provider_group_hash = provider_entry.get("__hash__")
    if provider_group_hash is None:
        return []
    rows = []
    for npi in _normalized_npi_list(provider_entry.get("npi")):
        rows.append({"provider_group_hash": int(provider_group_hash), "npi": int(npi)})
    return rows


def _provider_set_component_rows(provider_set_hash: str, provider_group_hashes: list[int] | set[int]) -> list[dict[str, Any]]:
    return [
        {"provider_set_hash": provider_set_hash, "provider_group_hash": int(group_hash)}
        for group_hash in sorted({int(value) for value in provider_group_hashes})
    ]
