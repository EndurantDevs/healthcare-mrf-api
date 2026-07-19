# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PTG source-file metadata and file-row helpers."""

from __future__ import annotations

import os
from typing import Any

import ijson

from process.ptg_parts.artifact_streams import open_json_artifact_stream, stream_logical_artifact
from process.ptg_parts.row_helpers import _coerce_date, _make_checksum
from process.ptg_parts.source_jobs import _normalize_plan_payload


def _maybe_unzip(path: str) -> str:
    logical = stream_logical_artifact(path, output_dir=os.path.dirname(path))
    if logical.logical_path != path:
        return logical.logical_path
    return path


async def _extract_metadata_fields(file_path: str) -> dict[str, Any]:
    """
    Extract top-level metadata without loading the full file.
    """
    fields = {
        "reporting_entity_name",
        "reporting_entity_type",
        "last_updated_on",
        "version",
        "plan_name",
        "plan_id",
        "plan_id_type",
        "plan_market_type",
        "plan_sponsor_name",
        "issuer_name",
    }
    metadata_by_field: dict[str, Any] = {}
    with open_json_artifact_stream(file_path) as afp:
        for prefix, event, value in ijson.parse(afp):
            if event in ("string", "number") and prefix in fields:
                metadata_by_field[prefix] = value
                if len(metadata_by_field) == len(fields):
                    break
    return metadata_by_field


_PLAN_FIELD_NAMES = (
    "plan_name",
    "plan_id_type",
    "plan_id",
    "plan_market_type",
    "issuer_name",
    "plan_sponsor_name",
)


def _plan_field_consensus(
    plans: list[dict[str, Any]],
    field_name: str,
) -> str | None:
    """Return one shared value, allowing duplicate plan records to disagree elsewhere."""

    values_by_identity: dict[str, str] = {}
    for plan in plans:
        value = str(plan.get(field_name) or "").strip()
        if not value:
            continue
        values_by_identity.setdefault(value.casefold(), value)
    if len(values_by_identity) != 1:
        return None
    return next(iter(values_by_identity.values()))


def _derive_plan_fields(
    meta: dict[str, Any],
    plan_info: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    """Derive one logical plan scope from top-level or repeated plan metadata."""

    normalized_plans = [
        _normalize_plan_payload(plan)
        for plan in (plan_info or [])
        if isinstance(plan, dict)
    ]
    plan_fields_by_name = {
        field_name: _plan_field_consensus(normalized_plans, field_name)
        for field_name in _PLAN_FIELD_NAMES
    }
    for field_name in _PLAN_FIELD_NAMES:
        metadata_value = str(meta.get(field_name) or "").strip()
        if metadata_value:
            plan_fields_by_name[field_name] = metadata_value
    return (
        plan_fields_by_name
        if any(plan_fields_by_name.values())
        else {}
    )


def _build_file_row(
    url: str,
    file_type: str,
    meta: dict[str, Any],
    plan_info: list[dict[str, Any]] | None,
    description: str | None,
    from_index_url: str | None,
) -> dict[str, Any]:
    plan_fields = _derive_plan_fields(meta, plan_info)
    file_id = _make_checksum(url, file_type, plan_fields.get("plan_id") or plan_fields.get("plan_name") or "")
    return {
        "file_id": file_id,
        "file_type": file_type,
        "url": url,
        "description": description,
        "reporting_entity_name": meta.get("reporting_entity_name"),
        "reporting_entity_type": meta.get("reporting_entity_type"),
        "last_updated_on": _coerce_date(meta.get("last_updated_on")),
        "version": meta.get("version"),
        "plan_name": plan_fields.get("plan_name"),
        "plan_id_type": plan_fields.get("plan_id_type"),
        "plan_id": plan_fields.get("plan_id"),
        "plan_market_type": plan_fields.get("plan_market_type"),
        "issuer_name": plan_fields.get("issuer_name"),
        "plan_sponsor_name": plan_fields.get("plan_sponsor_name"),
        "from_index_url": from_index_url,
    }
