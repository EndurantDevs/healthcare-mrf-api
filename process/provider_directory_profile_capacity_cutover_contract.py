# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Parse and validate retained cutover evidence fragments."""

from __future__ import annotations

from typing import Any, Mapping

from process.provider_directory_profile_capacity_geometry import (
    _error,
    _exact_fields,
)
from process.provider_directory_profile_capacity_types import (
    ProviderDirectoryProfileCapacityGeometry,
    _MAX_SIGNED_BIGINT,
)

def _cutover_nonnegative_integer(
    value_map: Mapping[str, Any],
    field_name: str,
) -> int:
    value = value_map.get(field_name)
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value <= _MAX_SIGNED_BIGINT
    ):
        raise _error("cutover_evidence_invalid:" + field_name)
    return value


def _cutover_target_projection(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    target_map: Mapping[str, Any],
    expected_name: str,
) -> tuple[int, int, int]:
    _exact_fields(
        target_map,
        frozenset(
            {
                "relation_name",
                "target_growth_bytes",
                "deleted_logical_bytes",
                "wal_bytes",
            }
        ),
        name="cutover_target_projection",
    )
    if target_map.get("relation_name") != expected_name:
        raise _error("cutover_target_order_invalid")
    growth_bytes = _cutover_nonnegative_integer(
        target_map,
        "target_growth_bytes",
    )
    deleted_bytes = _cutover_nonnegative_integer(
        target_map,
        "deleted_logical_bytes",
    )
    wal_bytes = _cutover_nonnegative_integer(target_map, "wal_bytes")
    relation_cap = next(
        (
            cap
            for cap in geometry.relation_byte_caps
            if cap.relation_name == expected_name
        ),
        None,
    )
    if (
        relation_cap is None
        or growth_bytes > relation_cap.max_target_growth_bytes
        or deleted_bytes > relation_cap.max_deleted_logical_bytes
        or wal_bytes > relation_cap.max_wal_bytes
    ):
        raise _error("cutover_target_projection_exceeded:" + expected_name)
    return growth_bytes, deleted_bytes, wal_bytes


def _assert_cutover_layout(
    layout_map: Mapping[str, Any],
    expected_fingerprint: str,
    *,
    includes_inserted_toast_chunks: bool,
) -> tuple[tuple[int, ...], tuple[int, ...], int, int]:
    """Return validated index pages and TOAST mutation counts."""

    toast_chunk_fields = (
        {"inserted_toast_chunks", "deleted_toast_chunks"}
        if includes_inserted_toast_chunks
        else {"deleted_toast_chunks"}
    )
    _exact_fields(
        layout_map,
        frozenset(
            {
                "exact_fingerprint",
                "main_index_oids",
                "main_index_pages",
                "toast_index_oids",
                "toast_index_pages",
                *toast_chunk_fields,
            }
        ),
        name="cutover_layout",
    )
    if layout_map.get("exact_fingerprint") != expected_fingerprint:
        raise _error("cutover_layout_fingerprint_changed")
    main_index_pages = _validated_index_pages(
        layout_map,
        oid_field="main_index_oids",
        page_field="main_index_pages",
        required=True,
    )
    toast_index_pages = _validated_index_pages(
        layout_map,
        oid_field="toast_index_oids",
        page_field="toast_index_pages",
        required=False,
    )
    inserted_toast_chunks = (
        _cutover_nonnegative_integer(
            layout_map,
            "inserted_toast_chunks",
        )
        if includes_inserted_toast_chunks
        else 0
    )
    deleted_toast_chunks = _cutover_nonnegative_integer(
        layout_map,
        "deleted_toast_chunks",
    )
    return (
        main_index_pages,
        toast_index_pages,
        inserted_toast_chunks,
        deleted_toast_chunks,
    )


def _validated_index_pages(
    layout_map: Mapping[str, Any],
    *,
    oid_field: str,
    page_field: str,
    required: bool,
) -> tuple[int, ...]:
    index_oids = layout_map.get(oid_field)
    index_pages = layout_map.get(page_field)
    if (
        not isinstance(index_oids, (list, tuple))
        or not isinstance(index_pages, (list, tuple))
        or len(index_oids) != len(index_pages)
        or (required and not index_oids)
        or any(
            not isinstance(index_value, int)
            or isinstance(index_value, bool)
            or index_value < 1
            for index_value in (*index_oids, *index_pages)
        )
    ):
        raise _error("cutover_layout_index_identity_invalid")
    return tuple(int(page_count) for page_count in index_pages)
