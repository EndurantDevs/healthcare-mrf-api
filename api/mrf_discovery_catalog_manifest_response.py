# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Response-only access to cached MRF catalog paging manifests."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from sqlalchemy import select

from api.mrf_discovery_catalog_manifest import (
    CATALOG_PAGING_MANIFEST_METADATA_KEY,
    catalog_paging_manifest_for_file_page,
)
from db.models import MRFSource, db


async def catalog_file_page_response(
    file_rows: list[Any],
    *,
    page_items: list[dict[str, Any]],
    next_cursor: str | None,
    source_id: str,
    page_limit: int,
) -> dict[str, Any]:
    """Build a files response with a compatible cached total when available."""

    response_by_key = {"items": page_items, "next_cursor": next_cursor}
    paging_manifest = await cached_paging_manifest_for_file_page(
        file_rows,
        source_id=source_id,
        page_limit=page_limit,
    )
    if paging_manifest is not None:
        response_by_key["paging_manifest"] = paging_manifest
    return response_by_key


async def cached_paging_manifest_for_file_page(
    file_rows: list[Any],
    *,
    source_id: str,
    page_limit: int,
) -> dict[str, Any] | None:
    """Read only a precomputed total and never calculate a count on demand."""

    if file_rows:
        source_metadata = _row_mapping(file_rows[0]).get("source_metadata_json")
    else:
        source_metadata = await _empty_source_metadata(source_id)
    return catalog_paging_manifest_for_file_page(
        source_metadata,
        page_limit=page_limit,
    )


def public_source_metadata(source_metadata: Any) -> dict[str, Any]:
    """Return source metadata without the internal cached pagination key."""

    metadata_by_key = dict(source_metadata) if isinstance(source_metadata, Mapping) else {}
    metadata_by_key.pop(CATALOG_PAGING_MANIFEST_METADATA_KEY, None)
    return metadata_by_key


async def _empty_source_metadata(source_id: str) -> Any:
    source_table = MRFSource.__table__
    source_row = await db.first(
        select(source_table.c.metadata_json).where(
            source_table.c.source_id == source_id
        )
    )
    if source_row is None:
        return None
    return _row_mapping(source_row).get("metadata_json")


def _row_mapping(row: Any) -> Mapping[str, Any]:
    return row if isinstance(row, Mapping) else row._mapping
