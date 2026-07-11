# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded PostgreSQL page-projection readers for PTG2 v3 serving."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text

from api.ptg2_db_sidecars import _safe_qualified_table_name
from api.ptg2_db_serving_v3 import (
    _block_keys_for,
    _is_key_in_block,
    _logical_blocks_by_key,
    _requested_keys,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_serving_binary_v3 import read_uvarint


PTG2_SERVING_BINARY_V3_BY_CODE_PAGE_KIND = "by_code_page_v3_f2"
PTG2_SERVING_BINARY_V3_PROVIDER_SET_PAGE_KIND = "provider_set_page_v3_s1"
PTG2_SERVING_BINARY_V3_PAGE_ROWS = 64
PTG2_SERVING_BINARY_V3_PAGE_FORMAT_VERSION = 2
PTG2_SERVING_BINARY_V3_PROVIDER_PAGE_BLOCK_SPAN = 1


@dataclass(frozen=True)
class PTG2V3PageRecord:
    code_key: int
    provider_set_key: int
    provider_count: int
    price_key: int


@dataclass(frozen=True)
class PTG2V3ProviderPage:
    entries: tuple[PTG2V3PageRecord, ...]
    total_row_count: int


def _page_entry_count(block_bytes: bytes, entry_count: int) -> tuple[int, int]:
    if not block_bytes or block_bytes[0] != PTG2_SERVING_BINARY_V3_PAGE_FORMAT_VERSION:
        raise ValueError("page projection has an unsupported format version")
    encoded_entry_count, cursor = read_uvarint(block_bytes, 1)
    if encoded_entry_count != entry_count:
        raise ValueError("page projection entry count does not match its block row")
    return encoded_entry_count, cursor


def _decode_code_page_block(
    block_bytes: bytes,
    *,
    code_key: int,
    entry_count: int,
) -> tuple[PTG2V3PageRecord, ...]:
    try:
        encoded_entry_count, cursor = _page_entry_count(block_bytes, entry_count)
        if encoded_entry_count > PTG2_SERVING_BINARY_V3_PAGE_ROWS:
            raise ValueError("code page exceeds its row limit")
        page_entries = []
        previous_rank = None
        for _row_index in range(encoded_entry_count):
            provider_set_key, cursor = read_uvarint(block_bytes, cursor)
            provider_count, cursor = read_uvarint(block_bytes, cursor)
            price_key, cursor = read_uvarint(block_bytes, cursor)
            current_rank = (-provider_count, provider_set_key, price_key)
            if previous_rank is not None and current_rank < previous_rank:
                raise ValueError("code page rows are not ordered")
            page_entries.append(
                PTG2V3PageRecord(
                    code_key=code_key,
                    provider_set_key=provider_set_key,
                    provider_count=provider_count,
                    price_key=price_key,
                )
            )
            previous_rank = current_rank
        if cursor != len(block_bytes):
            raise ValueError("code page payload has trailing bytes")
        return tuple(page_entries)
    except Exception as exc:
        raise PTG2ManifestArtifactError(f"PTG2 v3 code page block {code_key} is corrupt") from exc


async def lookup_code_page_from_db(
    session: Any,
    table_name: str,
    code_key: int,
) -> tuple[PTG2V3PageRecord, ...] | None:
    """Read the bounded first-page projection for one code when present."""

    _safe_qualified_table_name(table_name)
    normalized_code_key = int(code_key)
    logical_blocks = await _logical_blocks_by_key(
        session,
        table_name,
        artifact_kind=PTG2_SERVING_BINARY_V3_BY_CODE_PAGE_KIND,
        block_keys=(normalized_code_key,),
    )
    code_block = logical_blocks.get(normalized_code_key)
    if code_block is None:
        return None
    block_bytes, entry_count = code_block
    return _decode_code_page_block(
        block_bytes,
        code_key=normalized_code_key,
        entry_count=entry_count,
    )


def _read_provider_page_entry(
    block_bytes: bytes,
    cursor: int,
    *,
    block_key: int,
    previous_provider_set_key: int | None,
    requested_provider_set_keys: set[int],
) -> tuple[int, PTG2V3ProviderPage | None, int]:
    provider_offset, cursor = read_uvarint(block_bytes, cursor)
    block_start = block_key * PTG2_SERVING_BINARY_V3_PROVIDER_PAGE_BLOCK_SPAN
    provider_set_key = block_start + provider_offset
    provider_count, cursor = read_uvarint(block_bytes, cursor)
    total_row_count, cursor = read_uvarint(block_bytes, cursor)
    page_row_count, cursor = read_uvarint(block_bytes, cursor)
    has_valid_metadata = (
        _is_key_in_block(
            provider_set_key,
            block_key,
            PTG2_SERVING_BINARY_V3_PROVIDER_PAGE_BLOCK_SPAN,
        )
        and (previous_provider_set_key is None or provider_set_key > previous_provider_set_key)
        and total_row_count > 0
        and page_row_count == min(total_row_count, PTG2_SERVING_BINARY_V3_PAGE_ROWS)
    )
    if not has_valid_metadata:
        raise ValueError("provider page metadata is invalid")
    current_code_key = 0
    previous_pair = None
    page_entries = []
    for _row_index in range(page_row_count):
        code_delta, cursor = read_uvarint(block_bytes, cursor)
        price_key, cursor = read_uvarint(block_bytes, cursor)
        current_code_key += code_delta
        current_pair = (current_code_key, price_key)
        if previous_pair is not None and current_pair < previous_pair:
            raise ValueError("provider page rows are not ordered")
        if provider_set_key in requested_provider_set_keys:
            page_entries.append(
                PTG2V3PageRecord(
                    code_key=current_code_key,
                    provider_set_key=provider_set_key,
                    provider_count=provider_count,
                    price_key=price_key,
                )
            )
        previous_pair = current_pair
    provider_page = None
    if provider_set_key in requested_provider_set_keys:
        provider_page = PTG2V3ProviderPage(
            entries=tuple(page_entries),
            total_row_count=total_row_count,
        )
    return provider_set_key, provider_page, cursor


def _decode_provider_page_block(
    block_bytes: bytes,
    *,
    block_key: int,
    entry_count: int,
    requested_provider_set_keys: set[int],
) -> dict[int, PTG2V3ProviderPage]:
    try:
        encoded_entry_count, cursor = _page_entry_count(block_bytes, entry_count)
        previous_provider_set_key = None
        pages_by_provider_set = {}
        for _provider_index in range(encoded_entry_count):
            provider_set_key, provider_page, cursor = _read_provider_page_entry(
                block_bytes,
                cursor,
                block_key=block_key,
                previous_provider_set_key=previous_provider_set_key,
                requested_provider_set_keys=requested_provider_set_keys,
            )
            if provider_page is not None:
                pages_by_provider_set[provider_set_key] = provider_page
            previous_provider_set_key = provider_set_key
        if cursor != len(block_bytes):
            raise ValueError("provider page payload has trailing bytes")
        return pages_by_provider_set
    except Exception as exc:
        raise PTG2ManifestArtifactError(f"PTG2 v3 provider page block {block_key} is corrupt") from exc


async def lookup_provider_pages_from_db(
    session: Any,
    table_name: str,
    provider_set_keys: Iterable[int],
) -> dict[int, PTG2V3ProviderPage] | None:
    """Read bounded provider-first page projections, or None for old snapshots."""

    _safe_qualified_table_name(table_name)
    requested_key_values = _requested_keys(provider_set_keys)
    logical_blocks = await _logical_blocks_by_key(
        session,
        table_name,
        artifact_kind=PTG2_SERVING_BINARY_V3_PROVIDER_SET_PAGE_KIND,
        block_keys=_block_keys_for(
            requested_key_values,
            PTG2_SERVING_BINARY_V3_PROVIDER_PAGE_BLOCK_SPAN,
        ),
    )
    if not logical_blocks:
        return None
    requested_key_set = set(requested_key_values)
    pages_by_provider_set = {}
    for block_key, (block_bytes, entry_count) in logical_blocks.items():
        pages_by_provider_set.update(
            _decode_provider_page_block(
                block_bytes,
                block_key=block_key,
                entry_count=entry_count,
                requested_provider_set_keys=requested_key_set,
            )
        )
    return pages_by_provider_set


async def has_provider_pages_in_db(session: Any, table_name: str) -> bool:
    """Return whether an immutable snapshot carries provider page blocks."""

    qualified_table = _safe_qualified_table_name(table_name)
    availability_result = await session.execute(
        text(
            f"""
            SELECT EXISTS (
                SELECT 1
                FROM {qualified_table}
                WHERE artifact_kind = :artifact_kind
                LIMIT 1
            )
            """
        ),
        {"artifact_kind": PTG2_SERVING_BINARY_V3_PROVIDER_SET_PAGE_KIND},
    )
    return bool(availability_result.scalar())
