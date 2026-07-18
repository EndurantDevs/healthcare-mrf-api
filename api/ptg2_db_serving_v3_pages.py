# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded page-projection decoders for strict shared-block PTG V3 serving."""

from __future__ import annotations

from dataclasses import dataclass

from api.ptg2_db_serving_v3 import _is_key_in_block
from api.ptg2_shared_blocks import (
    decode_dense_source_header,
    decode_dense_source_vector,
    read_strict_uvarint,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


PTG2_SERVING_BINARY_V3_BY_CODE_PAGE_KIND = "by_code_price_page_v4"
PTG2_SERVING_BINARY_V3_PROVIDER_SET_PAGE_KIND = "provider_set_page_v3_s2"
PTG2_SERVING_BINARY_V3_PAGE_ROWS = 64
PTG2_SERVING_BINARY_V3_PAGE_FORMAT_VERSION = 4
PTG2_SERVING_BINARY_V3_PROVIDER_PAGE_BLOCK_SPAN = 1


@dataclass(frozen=True)
class PTG2V3PageRecord:
    code_key: int
    provider_set_key: int
    provider_count: int
    price_key: int
    source_key: int


@dataclass(frozen=True)
class PTG2V3ProviderPage:
    entries: tuple[PTG2V3PageRecord, ...]
    total_row_count: int
    encoded_row_count: int | None = None
    declared_provider_count: int | None = None
    last_encoded_code_key: int | None = None

    @property
    def provider_count(self) -> int:
        """Return the provider count repeated by every projected row."""

        if self.declared_provider_count is not None:
            return self.declared_provider_count
        if not self.entries:
            raise PTG2ManifestArtifactError("PTG2 v3 provider page has no projected rows")
        return self.entries[0].provider_count

    @property
    def page_row_count(self) -> int:
        """Return the validated encoded row count, including skipped rows."""

        return (
            self.encoded_row_count
            if self.encoded_row_count is not None
            else len(self.entries)
        )

    @property
    def last_code_key(self) -> int:
        """Return the final encoded code key used for page-boundary proofs."""

        if self.last_encoded_code_key is not None:
            return self.last_encoded_code_key
        if not self.entries:
            raise PTG2ManifestArtifactError("PTG2 v3 provider page has no projected rows")
        return self.entries[-1].code_key


def _page_entry_count(
    block_bytes: bytes,
    entry_count: int,
    *,
    expected_source_count: int | None,
) -> tuple[int, int, int, int]:
    source_count, source_bits, cursor = decode_dense_source_header(
        block_bytes,
        0,
        format_version=PTG2_SERVING_BINARY_V3_PAGE_FORMAT_VERSION,
        expected_source_count=expected_source_count,
    )
    encoded_entry_count, cursor = read_strict_uvarint(block_bytes, cursor)
    if encoded_entry_count != entry_count:
        raise ValueError("page projection entry count does not match its block row")
    return encoded_entry_count, source_count, source_bits, cursor


def _decode_code_page_block(
    block_bytes: bytes,
    *,
    code_key: int,
    entry_count: int,
    expected_source_count: int | None = None,
) -> tuple[PTG2V3PageRecord, ...]:
    try:
        encoded_entry_count, source_count, source_bits, cursor = _page_entry_count(
            block_bytes,
            entry_count,
            expected_source_count=expected_source_count,
        )
        if encoded_entry_count <= 0 or encoded_entry_count > PTG2_SERVING_BINARY_V3_PAGE_ROWS:
            raise ValueError("code page exceeds its row limit")
        normal_rows: list[tuple[int, int, int]] = []
        for _row_index in range(encoded_entry_count):
            provider_set_key, cursor = read_strict_uvarint(block_bytes, cursor)
            provider_count, cursor = read_strict_uvarint(block_bytes, cursor)
            price_key, cursor = read_strict_uvarint(block_bytes, cursor)
            if provider_set_key > 2**31 - 1 or provider_count > 2**32 - 1 or price_key > 2**32 - 1:
                raise ValueError("code page row value is out of range")
            normal_rows.append((provider_set_key, provider_count, price_key))
        source_keys, cursor = decode_dense_source_vector(
            block_bytes,
            cursor,
            entry_count=encoded_entry_count,
            source_count=source_count,
            source_bits=source_bits,
        )
        page_entries = []
        previous_rank = None
        for (provider_set_key, provider_count, price_key), source_key in zip(
            normal_rows,
            source_keys,
        ):
            current_rank = (price_key, provider_set_key, source_key, provider_count)
            if previous_rank is not None and current_rank < previous_rank:
                raise ValueError("code page rows are not ordered")
            page_entries.append(
                PTG2V3PageRecord(
                    code_key=code_key,
                    provider_set_key=provider_set_key,
                    provider_count=provider_count,
                    price_key=price_key,
                    source_key=source_key,
                )
            )
            previous_rank = current_rank
        if cursor != len(block_bytes):
            raise ValueError("code page payload has trailing bytes")
        return tuple(page_entries)
    except Exception as exc:
        raise PTG2ManifestArtifactError(f"PTG2 v3 code page block {code_key} is corrupt") from exc


def _read_provider_page_entry(
    block_bytes: bytes,
    cursor: int,
    *,
    block_key: int,
    previous_provider_set_key: int | None,
    requested_provider_set_keys: set[int],
    requested_code_keys: set[int] | None,
    source_count: int,
    source_bits: int,
) -> tuple[int, PTG2V3ProviderPage | None, int]:
    """Decode and validate one provider-page entry from a strict V3 block."""

    provider_offset, cursor = read_strict_uvarint(block_bytes, cursor)
    block_start = block_key * PTG2_SERVING_BINARY_V3_PROVIDER_PAGE_BLOCK_SPAN
    provider_set_key = block_start + provider_offset
    provider_count, cursor = read_strict_uvarint(block_bytes, cursor)
    total_row_count, cursor = read_strict_uvarint(block_bytes, cursor)
    page_row_count, cursor = read_strict_uvarint(block_bytes, cursor)
    has_valid_metadata = (
        _is_key_in_block(
            provider_set_key,
            block_key,
            PTG2_SERVING_BINARY_V3_PROVIDER_PAGE_BLOCK_SPAN,
        )
        and (previous_provider_set_key is None or provider_set_key > previous_provider_set_key)
        and total_row_count > 0
        and page_row_count == min(total_row_count, PTG2_SERVING_BINARY_V3_PAGE_ROWS)
        and provider_count <= 2**32 - 1
    )
    if not has_valid_metadata:
        raise ValueError("provider page metadata is invalid")
    current_code_key = 0
    normal_rows: list[tuple[int, int]] = []
    for _row_index in range(page_row_count):
        code_delta, cursor = read_strict_uvarint(block_bytes, cursor)
        price_key, cursor = read_strict_uvarint(block_bytes, cursor)
        current_code_key += code_delta
        if current_code_key > 2**31 - 1 or price_key > 2**32 - 1:
            raise ValueError("provider page row value is out of range")
        normal_rows.append((current_code_key, price_key))
    source_keys, cursor = decode_dense_source_vector(
        block_bytes,
        cursor,
        entry_count=page_row_count,
        source_count=source_count,
        source_bits=source_bits,
    )
    previous_pair = None
    page_entries = []
    for (current_code_key, price_key), source_key in zip(normal_rows, source_keys):
        current_pair = (current_code_key, price_key, source_key)
        if previous_pair is not None and current_pair < previous_pair:
            raise ValueError("provider page rows are not ordered")
        if provider_set_key in requested_provider_set_keys and (
            requested_code_keys is None or current_code_key in requested_code_keys
        ):
            page_entries.append(
                PTG2V3PageRecord(
                    code_key=current_code_key,
                    provider_set_key=provider_set_key,
                    provider_count=provider_count,
                    price_key=price_key,
                    source_key=source_key,
                )
            )
        previous_pair = current_pair
    provider_page = None
    if provider_set_key in requested_provider_set_keys:
        provider_page = PTG2V3ProviderPage(
            entries=tuple(page_entries),
            total_row_count=total_row_count,
            encoded_row_count=(
                page_row_count if requested_code_keys is not None else None
            ),
            declared_provider_count=(
                provider_count if requested_code_keys is not None else None
            ),
            last_encoded_code_key=(
                current_code_key if requested_code_keys is not None else None
            ),
        )
    return provider_set_key, provider_page, cursor


def _decode_provider_page_block(
    block_bytes: bytes,
    *,
    block_key: int,
    entry_count: int,
    requested_provider_set_keys: set[int],
    requested_code_keys: set[int] | None = None,
    expected_source_count: int | None = None,
) -> dict[int, PTG2V3ProviderPage]:
    try:
        if requested_code_keys is not None and any(
            code_key < 0 or code_key > 2**31 - 1
            for code_key in requested_code_keys
        ):
            raise ValueError("requested provider-page code key is out of range")
        encoded_entry_count, source_count, source_bits, cursor = _page_entry_count(
            block_bytes,
            entry_count,
            expected_source_count=expected_source_count,
        )
        if encoded_entry_count != 1:
            raise ValueError("provider page block must contain exactly one provider entry")
        previous_provider_set_key = None
        pages_by_provider_set = {}
        for _provider_index in range(encoded_entry_count):
            provider_set_key, provider_page, cursor = _read_provider_page_entry(
                block_bytes,
                cursor,
                block_key=block_key,
                previous_provider_set_key=previous_provider_set_key,
                requested_provider_set_keys=requested_provider_set_keys,
                requested_code_keys=requested_code_keys,
                source_count=source_count,
                source_bits=source_bits,
            )
            if provider_page is not None:
                pages_by_provider_set[provider_set_key] = provider_page
            previous_provider_set_key = provider_set_key
        if cursor != len(block_bytes):
            raise ValueError("provider page payload has trailing bytes")
        return pages_by_provider_set
    except Exception as exc:
        raise PTG2ManifestArtifactError(f"PTG2 v3 provider page block {block_key} is corrupt") from exc
