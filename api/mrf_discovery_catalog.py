# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read-only control-plane views over the stored MRF discovery catalog."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from sqlalchemy import String, cast, func, or_, select

from api.mrf_discovery_catalog_paging import (
    MAX_FILE_PAGE_PLAN_REFERENCES,
    ambiguous_plan_identity_keys as _ambiguous_plan_identity_keys,
    bounded_file_windows,
    parse_file_cursor as _parse_file_cursor,
    plan_identity_key as _plan_identity_key,
    plan_reference_count as _plan_reference_count,
    slice_values as _slice_values,
)
from db.models import MRFFile, MRFPayer, MRFSource, db

DEFAULT_SOURCE_PAGE_SIZE = 100
MAX_SOURCE_PAGE_SIZE = 250
DEFAULT_FILE_PAGE_SIZE = 250
MAX_FILE_PAGE_SIZE = 500
_SOURCE_FIELDS = (
    "source_id",
    "source_key",
    "display_name",
    "source_type",
    "hosting_platform",
    "access_model",
    "index_url",
    "human_url",
    "canonical_url",
    "domain",
    "status",
    "latest_index_date",
    "num_plans",
    "num_files",
    "num_indices",
    "total_compressed_size",
    "provenance_url",
    "seed_provider",
    "confidence",
    "license_status",
    "review_status",
    "metadata_json",
    "updated_at",
)

_FILE_FIELDS = (
    "mrf_file_id",
    "source_id",
    "file_type",
    "url",
    "canonical_url",
    "from_index_url",
    "description",
    "network_name",
    "plan_ids",
    "plan_names",
    "market_types",
    "is_signed_url",
    "size_bytes",
    "etag",
    "last_modified",
    "schema_version",
    "metadata_json",
    "first_seen_at",
    "last_seen_at",
)


def page_limit(value: Any, *, default: int, maximum: int) -> int:
    """Validate and clamp a control-plane page size."""

    if value in (None, ""):
        return default
    try:
        requested = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("limit must be an integer") from exc
    if requested < 1:
        raise ValueError("limit must be greater than zero")
    return min(requested, maximum)


async def list_discovery_sources_page(
    *,
    cursor: str | None = None,
    limit: int = DEFAULT_SOURCE_PAGE_SIZE,
    query: str | None = None,
    discovery_run_id: str | None = None,
) -> dict[str, Any]:
    """Return one deterministic page of discovered source records."""

    source_table = MRFSource.__table__
    payer_table = MRFPayer.__table__
    selected_columns = [source_table.c[field] for field in _SOURCE_FIELDS]
    selected_columns.extend(
        (
            payer_table.c.canonical_name.label("payer_name"),
            payer_table.c.aliases.label("payer_aliases"),
            payer_table.c.parent_group.label("payer_parent_group"),
            payer_table.c.entity_type.label("payer_entity_type"),
            payer_table.c.states.label("payer_states"),
            payer_table.c.eins.label("payer_eins"),
            payer_table.c.metadata_json.label("payer_metadata_json"),
        )
    )
    source_statement = select(*selected_columns).select_from(
        source_table.outerjoin(
            payer_table,
            payer_table.c.payer_id == source_table.c.payer_id,
        )
    )
    source_statement = _filtered_source_statement(
        source_statement,
        source_table,
        payer_table,
        cursor=cursor,
        query=query,
        discovery_run_id=discovery_run_id,
    )
    source_query_rows = await db.all(
        source_statement.order_by(source_table.c.source_id).limit(limit + 1)
    )
    page_rows, next_cursor = _bounded_rows(
        source_query_rows, limit=limit, cursor_key="source_id"
    )
    return {
        "items": [_source_item(source_row) for source_row in page_rows],
        "next_cursor": next_cursor,
    }


def _filtered_source_statement(
    source_statement: Any,
    source_table: Any,
    payer_table: Any,
    *,
    cursor: str | None,
    query: str | None,
    discovery_run_id: str | None,
) -> Any:
    """Apply cursor, text, and run identity filters to a source query."""

    if cursor:
        source_statement = source_statement.where(source_table.c.source_id > cursor)
    normalized_query = str(query or "").strip().lower()
    if normalized_query:
        search_pattern = f"%{normalized_query}%"
        source_statement = source_statement.where(
            or_(
                func.lower(source_table.c.display_name).like(search_pattern),
                func.lower(func.coalesce(payer_table.c.canonical_name, "")).like(
                    search_pattern
                ),
                func.lower(cast(source_table.c.metadata_json, String)).like(
                    search_pattern
                ),
                func.lower(cast(payer_table.c.aliases, String)).like(search_pattern),
            )
        )
    normalized_run_id = str(discovery_run_id or "").strip()
    if normalized_run_id:
        source_statement = source_statement.where(
            source_table.c.metadata_json["discovery_run_id"].as_string()
            == normalized_run_id
        )
    return source_statement


async def list_discovery_source_files_page(
    source_id: str,
    *,
    cursor: str | None = None,
    limit: int = DEFAULT_FILE_PAGE_SIZE,
) -> dict[str, Any]:
    """Return one deterministic page of files discovered for a source."""

    normalized_source_id = str(source_id or "").strip()
    if not normalized_source_id:
        raise ValueError("source_id is required")
    file_table = MRFFile.__table__
    source_table = MRFSource.__table__
    selected_columns = [file_table.c[field] for field in _FILE_FIELDS]
    selected_columns.extend(
        (
            source_table.c.display_name.label("source_display_name"),
            source_table.c.index_url.label("source_index_url"),
            source_table.c.metadata_json.label("source_metadata_json"),
        )
    )
    statement = (
        select(*selected_columns)
        .select_from(
            file_table.join(
                source_table,
                source_table.c.source_id == file_table.c.source_id,
            )
        )
        .where(file_table.c.source_id == normalized_source_id)
    )
    cursor_file_id, cursor_plan_offset = _parse_file_cursor(cursor)
    if cursor_file_id:
        cursor_comparison = (
            file_table.c.mrf_file_id >= cursor_file_id
            if cursor_plan_offset
            else file_table.c.mrf_file_id > cursor_file_id
        )
        statement = statement.where(cursor_comparison)
    file_query_rows = await db.all(
        statement.order_by(file_table.c.mrf_file_id).limit(limit + 1)
    )
    page_items, next_cursor = _bounded_file_items(
        file_query_rows,
        limit=limit,
        cursor_plan_offset=cursor_plan_offset,
        plan_reference_limit=MAX_FILE_PAGE_PLAN_REFERENCES,
    )
    return {
        "items": page_items,
        "next_cursor": next_cursor,
    }


def _bounded_rows(
    rows: list[Any], *, limit: int, cursor_key: str
) -> tuple[list[Any], str | None]:
    has_more = len(rows) > limit
    page_rows = list(rows[:limit])
    if not has_more or not page_rows:
        return page_rows, None
    final_mapping = _row_mapping(page_rows[-1])
    return page_rows, str(final_mapping.get(cursor_key) or "") or None


def _bounded_file_items(
    file_query_rows: list[Any],
    *,
    limit: int,
    cursor_plan_offset: int,
    plan_reference_limit: int,
) -> tuple[list[dict[str, Any]], str | None]:
    """Build a file page bounded by both rows and expanded plan references."""

    plan_windows, next_cursor = bounded_file_windows(
        [_row_mapping(file_row) for file_row in file_query_rows],
        limit=limit,
        cursor_plan_offset=cursor_plan_offset,
        plan_reference_limit=plan_reference_limit,
    )
    return [
        _file_item(
            plan_window.file_data,
            plan_offset=plan_window.plan_offset,
            plan_limit=plan_window.plan_limit,
            plan_total=plan_window.plan_total,
        )
        for plan_window in plan_windows
    ], next_cursor


def _source_item(source_row: Any) -> dict[str, Any]:
    source_data = _row_mapping(source_row)
    source_metadata = _metadata_dict(source_data.get("metadata_json"))
    aliases = _text_list(source_metadata.get("aliases"))
    aliases.extend(_text_list(source_data.get("payer_aliases")))
    source_metadata["aliases"] = list(dict.fromkeys(aliases))
    source_metadata["engine_source_catalog_id"] = source_data.get("source_id")
    payer_metadata = _metadata_dict(source_data.get("payer_metadata_json"))
    payer_dict = {
        "canonical_name": source_data.get("payer_name"),
        "aliases": _text_list(source_data.get("payer_aliases")),
        "parent_group": source_data.get("payer_parent_group"),
        "entity_type": source_data.get("payer_entity_type"),
        "states": _text_list(source_data.get("payer_states")),
        "eins": _text_list(source_data.get("payer_eins")),
        "metadata": payer_metadata,
    }
    source_item_dict = {field: source_data.get(field) for field in _SOURCE_FIELDS}
    source_item_dict.pop("metadata_json", None)
    source_item_dict["metadata"] = source_metadata
    source_item_dict["payer"] = payer_dict
    return source_item_dict


def _file_item(
    file_row: Any,
    *,
    plan_offset: int = 0,
    plan_limit: int | None = None,
    plan_total: int | None = None,
) -> dict[str, Any]:
    """Normalize one stored file row and its selected plan slice."""

    file_data = _row_mapping(file_row)
    file_metadata = _metadata_dict(file_data.get("metadata_json"))
    source_metadata = _metadata_dict(file_data.get("source_metadata_json"))
    canonical_url = str(
        file_data.get("canonical_url") or file_data.get("url") or ""
    ).strip()
    source_index_url = str(
        file_data.get("from_index_url")
        or file_data.get("source_index_url")
        or ""
    ).strip()
    reporting_entity_name = str(
        file_metadata.get("reporting_entity_name")
        or file_data.get("source_display_name")
        or ""
    ).strip()
    file_item_dict = {field: file_data.get(field) for field in _FILE_FIELDS}
    for raw_plan_field in (
        "metadata_json",
        "plan_ids",
        "plan_names",
        "market_types",
    ):
        file_item_dict.pop(raw_plan_field, None)
    file_item_dict.update(
        {
            "canonical_url": canonical_url,
            "domain": file_metadata.get("domain") or file_data.get("file_type"),
            "source_index_url": source_index_url or None,
            "reporting_entity_name": reporting_entity_name,
            "engine_source_catalog_id": file_data.get("source_id"),
            "engine_source_file_version_id": file_data.get("mrf_file_id"),
            "content_length": file_data.get("size_bytes"),
            "company_name": file_metadata.get("company_name")
            or source_metadata.get("target_payer_query"),
            "plan_info": _normalized_plan_info(
                file_data,
                file_metadata,
                offset=plan_offset,
                limit=plan_limit,
            ),
            "plan_chunk_offset": plan_offset,
            "plan_chunk_total": (
                _plan_reference_count(file_data)
                if plan_total is None
                else plan_total
            ),
            "metadata": _file_response_metadata(file_metadata),
        }
    )
    return file_item_dict


def _file_response_metadata(
    file_metadata: Mapping[str, Any],
) -> dict[str, Any]:
    """Remove raw plan rows already represented by the bounded plan slice."""

    return {
        metadata_key: metadata_value
        for metadata_key, metadata_value in file_metadata.items()
        if metadata_key != "plan_info"
    }


def _normalized_plan_info(
    file_data: Mapping[str, Any],
    file_metadata: Mapping[str, Any],
    *,
    offset: int = 0,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    metadata_plans = file_metadata.get("plan_info")
    ambiguous_plan_keys = _ambiguous_plan_identity_keys(metadata_plans)
    plan_rows = [
        dict(plan)
        for plan in _slice_values(metadata_plans, offset=offset, limit=limit)
        if isinstance(plan, Mapping)
    ]
    if not plan_rows:
        plan_rows = _column_plan_info(file_data, offset=offset, limit=limit)
    normalized_rows: list[dict[str, Any]] = []
    for plan_row in plan_rows:
        plan_name = str(plan_row.get("plan_name") or "").strip() or None
        market_type = str(plan_row.get("plan_market_type") or "").strip() or None
        plan_id = str(plan_row.get("plan_id") or "").strip() or None
        if not plan_id and plan_name:
            plan_id = _context_plan_id(file_data, plan_name, market_type)
            plan_row["plan_id_type"] = "source_file_context_hash"
        if not plan_id or not market_type:
            continue
        plan_row["plan_id"] = plan_id
        plan_row["plan_market_type"] = market_type
        if (
            plan_name
            and not any(
                str(plan_row.get(hash_field) or "").strip()
                for hash_field in (
                    "engine_plan_hash",
                    "plan_hash",
                    "ptg2_plan_hash",
                )
            )
            and not str(plan_row.get("plan_identity_hint") or "").strip()
            and _plan_identity_key(plan_row) in ambiguous_plan_keys
        ):
            plan_row["plan_identity_hint"] = f"plan_name:{plan_name}"
        normalized_rows.append(
            {
                **plan_row,
                "plan_id": plan_id,
                "plan_name": plan_name,
                "plan_market_type": market_type,
            }
        )
    return normalized_rows


def _column_plan_info(
    file_data: Mapping[str, Any],
    *,
    offset: int = 0,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    plan_ids = _text_list(file_data.get("plan_ids"))
    plan_names = _text_list(file_data.get("plan_names"))
    market_types = _text_list(file_data.get("market_types"))
    row_count = max(len(plan_ids), len(plan_names), len(market_types), 0)
    rows: list[dict[str, Any]] = []
    stop_index = row_count if limit is None else min(row_count, offset + limit)
    for index in range(offset, stop_index):
        rows.append(
            {
                "plan_id": _value_at(plan_ids, index),
                "plan_name": _value_at(plan_names, index),
                "plan_market_type": _value_at(market_types, index),
            }
        )
    return rows


def _context_plan_id(
    file_data: Mapping[str, Any], plan_name: str, market_type: str | None
) -> str:
    identity_dict = {
        "source_id": file_data.get("source_id"),
        "source_index_url": file_data.get("from_index_url")
        or file_data.get("source_index_url"),
        "canonical_url": file_data.get("canonical_url") or file_data.get("url"),
        "plan_name": plan_name,
        "market_type": market_type,
    }
    encoded = json.dumps(identity_dict, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()


def _metadata_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _text_list(value: Any) -> list[str]:
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if not isinstance(value, (list, tuple, set)):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _value_at(values: list[str], index: int) -> str | None:
    if not values:
        return None
    if index < len(values):
        return values[index]
    return values[0] if len(values) == 1 else None


def _row_mapping(row: Any) -> Mapping[str, Any]:
    if isinstance(row, Mapping):
        return row
    mapping = getattr(row, "_mapping", None)
    if isinstance(mapping, Mapping):
        return mapping
    raise TypeError("catalog query returned an unsupported row type")
