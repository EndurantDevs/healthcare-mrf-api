# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Project bounded target-relation data and WAL capacity."""

from __future__ import annotations

from process.provider_directory_profile_capacity_geometry import (
    _error,
    revalidate_capacity_geometry,
)
from process.provider_directory_profile_capacity_types import (
    POSTGRES_BLOCK_SIZE_BYTES,
    ProviderDirectoryProfileCapacityGeometry,
    ProviderDirectoryProfileDeltaProjection,
    ProviderDirectoryProfileRelationByteCaps,
    ProviderDirectoryProfileTargetDeltaInput,
    ProviderDirectoryProfileTargetDeltaProjection,
    _MAX_SIGNED_BIGINT,
    _MAX_UNSIGNED_BIGINT,
    _TARGET_RELATION_NAMES,
)

def _checked_add(*values: int) -> int:
    result = sum(values)
    if result < 0 or result > _MAX_SIGNED_BIGINT:
        raise _error("delta_projection_overflow")
    return result


def _ceil_log2(value: int) -> int:
    if value < 1:
        raise _error("delta_projection_input_invalid")
    return (value - 1).bit_length()


def _btree_insert_growth_pages(
    existing_pages: tuple[int, ...],
    inserted_entries: int,
) -> int:
    if inserted_entries == 0:
        return 0
    return _checked_add(
        *(
            (
                2
                * inserted_entries
                * (
                    _ceil_log2(
                        _checked_add(index_pages, inserted_entries, 1)
                    )
                    + 2
                )
            )
            + 1
            for index_pages in existing_pages
        )
    )


def _btree_wal_page_touches(
    existing_pages: tuple[int, ...],
    *,
    inserted_entries: int,
    deleted_entries: int,
) -> int:
    if inserted_entries == 0 and deleted_entries == 0:
        return 0
    return _checked_add(
        *(
            deleted_entries
            + inserted_entries
            + (
                3
                * inserted_entries
                * (
                    _ceil_log2(
                        _checked_add(index_pages, inserted_entries, 1)
                    )
                    + 2
                )
            )
            for index_pages in existing_pages
        )
    )


def _target_delta_projection(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    delta_input: ProviderDirectoryProfileTargetDeltaInput,
) -> ProviderDirectoryProfileTargetDeltaProjection:
    """Project one validated target relation within its signed byte caps."""

    _validate_target_delta_input(delta_input)
    relation_cap = _target_relation_cap(geometry, delta_input.relation_name)
    target_growth_bytes = _target_growth_bytes(geometry, delta_input)
    deleted_logical_bytes = delta_input.deleted_logical_bytes
    wal_bytes = _target_wal_bytes(geometry, delta_input)
    _assert_target_projection_caps(
        relation_cap,
        target_growth_bytes=target_growth_bytes,
        deleted_logical_bytes=deleted_logical_bytes,
        wal_bytes=wal_bytes,
    )
    return ProviderDirectoryProfileTargetDeltaProjection(
        relation_name=delta_input.relation_name,
        target_growth_bytes=target_growth_bytes,
        deleted_logical_bytes=deleted_logical_bytes,
        wal_bytes=wal_bytes,
    )


def _validate_target_delta_input(
    delta_input: ProviderDirectoryProfileTargetDeltaInput,
) -> None:
    if (
        not isinstance(delta_input, ProviderDirectoryProfileTargetDeltaInput)
        or delta_input.relation_name not in _TARGET_RELATION_NAMES
        or any(
            delta_metric < 0
            for delta_metric in (
                delta_input.inserted_rows,
                delta_input.inserted_toast_chunks,
                delta_input.deleted_rows,
                delta_input.deleted_logical_bytes,
                delta_input.deleted_toast_chunks,
            )
        )
        or not isinstance(delta_input.main_index_pages, tuple)
        or not isinstance(delta_input.toast_index_pages, tuple)
        or not delta_input.main_index_pages
        or any(
            not isinstance(index_pages, int)
            or isinstance(index_pages, bool)
            or index_pages < 1
            for index_pages in (
                delta_input.main_index_pages
                + delta_input.toast_index_pages
            )
        )
    ):
        raise _error("delta_projection_input_invalid")


def _target_relation_cap(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    relation_name: str,
) -> ProviderDirectoryProfileRelationByteCaps:
    matching_caps = tuple(
        relation_cap
        for relation_cap in geometry.relation_byte_caps
        if relation_cap.relation_name == relation_name
    )
    if len(matching_caps) != 1:
        raise _error("delta_projection_relation_cap_invalid")
    return matching_caps[0]


def _target_growth_bytes(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    delta_input: ProviderDirectoryProfileTargetDeltaInput,
) -> int:
    inserted_heap_pages = _checked_add(
        delta_input.inserted_rows,
        delta_input.inserted_toast_chunks,
    )
    auxiliary_growth_pages = (
        2 * _checked_add(inserted_heap_pages, 2)
        if inserted_heap_pages
        else 0
    )
    index_growth_pages = _checked_add(
        _btree_insert_growth_pages(
            delta_input.main_index_pages,
            delta_input.inserted_rows,
        ),
        _btree_insert_growth_pages(
            delta_input.toast_index_pages,
            delta_input.inserted_toast_chunks,
        ),
    )
    growth_pages = _checked_add(
        inserted_heap_pages,
        auxiliary_growth_pages,
        index_growth_pages,
    )
    return _checked_add(
        growth_pages * geometry.postgres_block_size_bytes
    )


def _target_wal_bytes(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    delta_input: ProviderDirectoryProfileTargetDeltaInput,
) -> int:
    heap_operations = _checked_add(
        delta_input.inserted_rows,
        delta_input.inserted_toast_chunks,
        delta_input.deleted_rows,
        delta_input.deleted_toast_chunks,
    )
    heap_page_touches = (
        3 * _checked_add(heap_operations, 4)
        if heap_operations
        else 0
    )
    index_page_touches = _checked_add(
        _btree_wal_page_touches(
            delta_input.main_index_pages,
            inserted_entries=delta_input.inserted_rows,
            deleted_entries=delta_input.deleted_rows,
        ),
        _btree_wal_page_touches(
            delta_input.toast_index_pages,
            inserted_entries=delta_input.inserted_toast_chunks,
            deleted_entries=delta_input.deleted_toast_chunks,
        ),
    )
    wal_page_touches = _checked_add(
        heap_page_touches,
        index_page_touches,
    )
    return _checked_add(
        3 * geometry.postgres_block_size_bytes * wal_page_touches
    )


def _assert_target_projection_caps(
    relation_cap: ProviderDirectoryProfileRelationByteCaps,
    *,
    target_growth_bytes: int,
    deleted_logical_bytes: int,
    wal_bytes: int,
) -> None:
    if target_growth_bytes > relation_cap.max_target_growth_bytes:
        raise _error(
            "delta_projection_target_growth_exceeded:"
            + relation_cap.relation_name
        )
    if (
        deleted_logical_bytes
        > relation_cap.max_deleted_logical_bytes
    ):
        raise _error(
            "delta_projection_deleted_logical_bytes_exceeded:"
            + relation_cap.relation_name
        )
    if wal_bytes > relation_cap.max_wal_bytes:
        raise _error(
            "delta_projection_wal_exceeded:"
            + relation_cap.relation_name
        )


def project_profile_delta_capacity(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    target_inputs: tuple[ProviderDirectoryProfileTargetDeltaInput, ...],
) -> ProviderDirectoryProfileDeltaProjection:
    """Project signed physical and WAL bounds before the first target DML."""

    verified_geometry = revalidate_capacity_geometry(geometry)
    if (
        not isinstance(target_inputs, tuple)
        or tuple(
            target_input.relation_name
            for target_input in target_inputs
        )
        != ("evidence_target", "profile_target")
    ):
        raise _error("delta_projection_target_order_invalid")
    target_projections = tuple(
        _target_delta_projection(verified_geometry, target_input)
        for target_input in target_inputs
    )
    target_data_bytes = _checked_add(
        *(
            target_projection.target_growth_bytes
            for target_projection in target_projections
        )
    )
    wal_bytes = _checked_add(
        *(
            target_projection.wal_bytes
            for target_projection in target_projections
        ),
    )
    if (
        wal_bytes
        > verified_geometry.reservation_bytes_by_storage_class["wal"]
    ):
        raise _error("delta_projection_total_wal_exceeded")
    return ProviderDirectoryProfileDeltaProjection(
        targets=target_projections,
        target_data_bytes=target_data_bytes,
        wal_bytes=wal_bytes,
    )
