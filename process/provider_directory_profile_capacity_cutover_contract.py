# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Parse and validate retained cutover evidence fragments."""

from __future__ import annotations

from typing import Any, Mapping

from process.provider_directory_profile_capacity_geometry import (
    _error,
    _exact_fields,
)
from process.provider_directory_profile_capacity_types import (
    _MAX_SIGNED_BIGINT,
    ProviderDirectoryProfileCapacityGeometry,
)


def _cutover_nonnegative_integer(
    value_map: Mapping[str, Any],
    field_name: str,
) -> int:
    value = value_map.get(field_name)
    if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value <= _MAX_SIGNED_BIGINT:
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
        (cap for cap in geometry.relation_byte_caps if cap.relation_name == expected_name),
        None,
    )
    if (
        relation_cap is None
        or deleted_bytes > relation_cap.max_deleted_logical_bytes
        or (
            not geometry.bounded_admission
            and (growth_bytes > relation_cap.max_target_growth_bytes or wal_bytes > relation_cap.max_wal_bytes)
        )
    ):
        raise _error("cutover_target_projection_exceeded:" + expected_name)
    return growth_bytes, deleted_bytes, wal_bytes


def _assert_cutover_layout(
    layout_map: Mapping[str, Any],
    expected_relation: Mapping[str, Any] | str,
    *,
    includes_inserted_toast_chunks: bool,
) -> tuple[tuple[int, ...], tuple[int, ...], int, int]:
    """Return validated index pages and TOAST mutation counts."""

    toast_chunk_fields = (
        {"inserted_toast_chunks", "deleted_toast_chunks"}
        if includes_inserted_toast_chunks
        else {"deleted_toast_chunks"}
    )
    legacy = isinstance(expected_relation, str)
    common_fields = {"main_index_pages", "toast_index_pages", *toast_chunk_fields}
    coordinate_fields = {"exact_fingerprint", "main_index_oids", "toast_index_oids"} if legacy else {"schema", "name"}
    _exact_fields(layout_map, frozenset(common_fields | coordinate_fields), name="cutover_layout")
    if legacy:
        # This compares two authenticated historical payloads, never a live physical object.
        if layout_map["exact_fingerprint"] != expected_relation:
            raise _error("cutover_layout_history_changed")
    elif any(layout_map[name] != expected_relation[name] for name in ("schema", "name")):
        raise _error("cutover_layout_target_changed")
    main_index_pages = _validated_index_pages(layout_map, page_field="main_index_pages", required=True)
    toast_index_pages = _validated_index_pages(layout_map, page_field="toast_index_pages", required=False)
    if legacy:
        for oid_field, pages in (("main_index_oids", main_index_pages), ("toast_index_oids", toast_index_pages)):
            identifiers = layout_map[oid_field]
            if (
                not isinstance(identifiers, (list, tuple))
                or len(identifiers) != len(pages)
                or any(type(identifier) is not int or identifier < 1 for identifier in identifiers)
            ):
                raise _error("cutover_layout_history_invalid")
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
    page_field: str,
    required: bool,
) -> tuple[int, ...]:
    index_pages = layout_map.get(page_field)
    if (
        not isinstance(index_pages, (list, tuple))
        or (required and not index_pages)
        or any(
            not isinstance(index_value, int) or isinstance(index_value, bool) or index_value < 1
            for index_value in index_pages
        )
    ):
        raise _error("cutover_layout_index_pages_invalid")
    return tuple(int(page_count) for page_count in index_pages)
