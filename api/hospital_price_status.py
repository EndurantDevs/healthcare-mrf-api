# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Cursor-paginated hospital price import status for the control plane."""

from __future__ import annotations

import asyncio
import datetime as dt
import os
import re
from collections.abc import Mapping
from typing import Any

from db.models import db
from process.hospital_hpt_registry import hospital_hpt_registry_groups


DEFAULT_HOSPITAL_PRICE_PAGE_SIZE = 50
MAX_HOSPITAL_PRICE_PAGE_SIZE = 200
_CURSOR_PATTERN = re.compile(r"^hospital-[0-9]{6}$")
_ATTEMPT_STATUSES = frozenset(
    {
        "never", "queued", "running", "verified", "published",
        "unchanged", "failed", "superseded",
    }
)
_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA") or os.getenv("DB_SCHEMA") or "mrf"
if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", _SCHEMA) is None:
    raise RuntimeError("hospital price database schema is invalid")
_STATUS_SQL = f"""
SELECT hospital.hospital_id,
       hospital.facility_anchor_id,
       attempt.attempt_id,
       attempt.status AS attempt_status,
       attempt.started_at,
       attempt.finished_at,
       attempt.error_code,
       current.version_id,
       current.generation,
       current.last_success_at,
       current.service_count,
       current.charge_count,
       current.payer_charge_count,
       current.npi_count,
       current.tax_identity_count,
       version.template_version,
       version.source_format
  FROM {_SCHEMA}.hospital_price_hospital AS hospital
  LEFT JOIN {_SCHEMA}.hospital_price_current AS current
    ON current.hospital_id = hospital.hospital_id
  LEFT JOIN {_SCHEMA}.hospital_price_import_attempt AS attempt
    ON attempt.hospital_id = current.hospital_id
   AND attempt.attempt_id = current.latest_attempt_id
  LEFT JOIN {_SCHEMA}.hospital_price_version AS version
    ON version.version_id = current.version_id
"""


def hospital_price_page_limit(value: Any) -> int:
    """Parse and bound one hospital status page limit."""

    if value in (None, ""):
        return DEFAULT_HOSPITAL_PRICE_PAGE_SIZE
    try:
        limit = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("limit must be an integer") from exc
    if limit < 1:
        raise ValueError("limit must be greater than zero")
    return min(limit, MAX_HOSPITAL_PRICE_PAGE_SIZE)


def _row_mapping(row: Any) -> Mapping[str, Any]:
    mapping = getattr(row, "_mapping", row)
    return mapping if isinstance(mapping, Mapping) else {}


def _timestamp_text(value: dt.datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _attempt_item(row: Mapping[str, Any]) -> dict[str, Any] | None:
    if not row.get("attempt_id"):
        return None
    return {
        "attempt_id": row["attempt_id"],
        "status": row.get("attempt_status"),
        "started_at": _timestamp_text(row.get("started_at")),
        "finished_at": _timestamp_text(row.get("finished_at")),
        "error_code": row.get("error_code"),
    }


def _publication_item(row: Mapping[str, Any]) -> dict[str, Any] | None:
    if not row.get("version_id"):
        return None
    return {
        "version_id": row["version_id"],
        "template_version": row.get("template_version"),
        "source_format": row.get("source_format"),
        "generation": row.get("generation"),
        "last_success_at": _timestamp_text(row.get("last_success_at")),
        "service_count": row.get("service_count"),
        "charge_count": row.get("charge_count"),
        "payer_charge_count": row.get("payer_charge_count"),
        "npi_count": row.get("npi_count"),
        "tax_identity_count": row.get("tax_identity_count"),
    }


def _latest_row(
    rows: list[Mapping[str, Any]], identity_field: str, timestamp_field: str
) -> Mapping[str, Any]:
    candidates = [row for row in rows if row.get(identity_field)]
    return max(
        candidates,
        key=lambda row: row.get(timestamp_field)
        or dt.datetime.min.replace(tzinfo=dt.UTC),
        default={},
    )


def _status_item(
    hospitals: tuple[Mapping[str, str], ...],
    rows_by_hospital_id: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    hospital = hospitals[0]
    rows = [
        rows_by_hospital_id.get(alias["hospital_id"], {}) for alias in hospitals
    ]
    attempt_row = _latest_row(rows, "attempt_id", "started_at")
    publication_row = _latest_row(rows, "version_id", "last_success_at")
    return {
        "hospital_id": hospital["hospital_id"],
        "alias_hospital_ids": [
            alias["hospital_id"] for alias in hospitals[1:]
        ],
        "name": hospital["name"],
        "cms_hpt_url": hospital["cms_hpt_url"],
        "facility_anchor_id": next(
            (
                row.get("facility_anchor_id")
                for row in rows
                if row.get("facility_anchor_id")
            ),
            None,
        ),
        "latest_attempt": _attempt_item(attempt_row),
        "publication": _publication_item(publication_row),
    }


def _item_status(item: Mapping[str, Any]) -> str:
    attempt = item.get("latest_attempt")
    if isinstance(attempt, Mapping) and attempt.get("status"):
        return str(attempt["status"])
    return "never"


def _is_status_match(item: Mapping[str, Any], status: str | None) -> bool:
    if status is None:
        return True
    if status == "unpublished":
        return item.get("publication") is None
    if status == "succeeded":
        return item.get("publication") is not None
    return _item_status(item) == status


def _summary(hospital_statuses: list[dict[str, Any]]) -> dict[str, Any]:
    count_by_status = {
        name: 0 for name in ("queued", "running", "succeeded", "failed", "unpublished")
    }
    count_by_template_version: dict[str, int] = {}
    count_by_source_format: dict[str, int] = {}
    for hospital_status in hospital_statuses:
        status = _item_status(hospital_status)
        if status in {"queued", "running", "failed"}:
            count_by_status[status] += 1
        if hospital_status["publication"] is not None:
            count_by_status["succeeded"] += 1
            version = hospital_status["publication"].get("template_version")
            if isinstance(version, str) and version:
                count_by_template_version[version] = (
                    count_by_template_version.get(version, 0) + 1
                )
            source_format = hospital_status["publication"].get("source_format")
            if isinstance(source_format, str) and source_format:
                count_by_source_format[source_format] = (
                    count_by_source_format.get(source_format, 0) + 1
                )
        else:
            count_by_status["unpublished"] += 1
    return {
        "total": len(hospital_statuses),
        **count_by_status,
        "template_versions": dict(sorted(count_by_template_version.items())),
        "source_formats": dict(sorted(count_by_source_format.items())),
    }


async def list_hospital_price_status_page(
    *,
    query: str | None = None,
    status: str | None = None,
    cursor: str | None = None,
    limit: int = DEFAULT_HOSPITAL_PRICE_PAGE_SIZE,
) -> dict[str, Any]:
    """Return reviewed registry rows with latest attempt and LKG kept separate."""

    limit = hospital_price_page_limit(limit)
    normalized_status = str(status or "").strip().lower() or None
    if normalized_status not in {None, "unpublished", "succeeded", *_ATTEMPT_STATUSES}:
        raise ValueError("status is invalid")
    normalized_cursor = str(cursor or "").strip() or None
    if normalized_cursor and _CURSOR_PATTERN.fullmatch(normalized_cursor) is None:
        raise ValueError("cursor is invalid")
    normalized_query = str(query or "").strip().casefold()
    hospital_groups, status_rows = await asyncio.gather(
        asyncio.to_thread(hospital_hpt_registry_groups),
        db.all(_STATUS_SQL),
    )
    rows_by_hospital_id = {
        str(mapping.get("hospital_id")): mapping
        for mapping in map(_row_mapping, status_rows)
        if mapping.get("hospital_id")
    }
    status_items = [
        _status_item(hospitals, rows_by_hospital_id)
        for hospitals in hospital_groups
        if not normalized_query
        or normalized_query
        in "\n".join(
            registry_value
            for hospital in hospitals
            for registry_value in hospital.values()
        ).casefold()
    ]
    summary = _summary(status_items)
    status_items = [
        status_item
        for status_item in status_items
        if _is_status_match(status_item, normalized_status)
    ]
    if normalized_cursor:
        status_items = [
            status_item
            for status_item in status_items
            if status_item["hospital_id"] > normalized_cursor
        ]
    page = status_items[: limit + 1]
    has_more = len(page) > limit
    page = page[:limit]
    return {
        "items": page,
        "next_cursor": page[-1]["hospital_id"] if has_more and page else None,
        "summary": summary,
    }
