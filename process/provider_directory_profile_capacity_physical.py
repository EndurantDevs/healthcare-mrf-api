# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Project scratch and metadata capacity for bounded profile builds."""

from __future__ import annotations

from process.provider_directory_profile_capacity_geometry import (
    _error,
    revalidate_capacity_geometry,
)
from process.provider_directory_profile_capacity_target import (
    _btree_insert_growth_pages,
    _btree_wal_page_touches,
    _checked_add,
)
from process.provider_directory_profile_capacity_types import (
    METADATA_DATA_UPPER_BOUND_BYTES,
    METADATA_PAYLOAD_UPPER_BOUND_BYTES,
    METADATA_WAL_UPPER_BOUND_BYTES,
    POSTGRES_BLOCK_SIZE_BYTES,
    POSTGRES_TOAST_MAX_CHUNK_SIZE_BYTES,
    ProviderDirectoryProfileCapacityGeometry,
    ProviderDirectoryProfileMetadataMutationInput,
    ProviderDirectoryProfileMetadataProjection,
    ProviderDirectoryProfileRelationByteCaps,
    ProviderDirectoryProfileScratchInput,
    ProviderDirectoryProfileScratchProjection,
    _SCRATCH_RELATION_NAMES,
)

def project_profile_scratch_capacity(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    scratch_input: ProviderDirectoryProfileScratchInput,
) -> ProviderDirectoryProfileScratchProjection:
    """Project one scratch wave before PostgreSQL can emit data or WAL."""

    verified_geometry = revalidate_capacity_geometry(geometry)
    _validate_scratch_input(scratch_input)
    relation_cap = _scratch_relation_cap(
        verified_geometry,
        scratch_input.relation_name,
    )
    inserted_toast_chunks_upper = _inserted_toast_chunks_upper(
        verified_geometry,
        scratch_input,
    )
    growth_bytes = _scratch_growth_bytes(
        verified_geometry,
        scratch_input,
        inserted_toast_chunks_upper,
    )
    wal_bytes = _scratch_wal_bytes(
        verified_geometry,
        scratch_input,
        inserted_toast_chunks_upper,
    )
    _assert_scratch_projection_caps(
        relation_cap,
        growth_bytes=growth_bytes,
        wal_bytes=wal_bytes,
    )
    return ProviderDirectoryProfileScratchProjection(
        relation_name=scratch_input.relation_name,
        inserted_rows=scratch_input.inserted_rows,
        inserted_logical_bytes=scratch_input.inserted_logical_bytes,
        inserted_toast_chunks_upper=inserted_toast_chunks_upper,
        growth_bytes=growth_bytes,
        wal_bytes=wal_bytes,
    )


def _validate_scratch_input(
    scratch_input: ProviderDirectoryProfileScratchInput,
) -> None:
    if (
        not isinstance(
            scratch_input,
            ProviderDirectoryProfileScratchInput,
        )
        or scratch_input.relation_name not in _SCRATCH_RELATION_NAMES
        or any(
            not isinstance(scratch_metric, int)
            or isinstance(scratch_metric, bool)
            or scratch_metric < 0
            for scratch_metric in (
                scratch_input.inserted_rows,
                scratch_input.inserted_logical_bytes,
                scratch_input.toastable_column_count,
            )
        )
        or not isinstance(scratch_input.main_index_pages, tuple)
        or not isinstance(scratch_input.toast_index_pages, tuple)
        or not scratch_input.main_index_pages
        or any(
            not isinstance(index_pages, int)
            or isinstance(index_pages, bool)
            or index_pages < 1
            for index_pages in (
                scratch_input.main_index_pages
                + scratch_input.toast_index_pages
            )
        )
        or (
            scratch_input.inserted_rows == 0
            and scratch_input.inserted_logical_bytes != 0
        )
    ):
        raise _error("scratch_projection_input_invalid")


def _scratch_relation_cap(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    relation_name: str,
) -> ProviderDirectoryProfileRelationByteCaps:
    relation_cap = next(
        (
            candidate
            for candidate in geometry.relation_byte_caps
            if candidate.relation_name == relation_name
        ),
        None,
    )
    if relation_cap is None:
        raise _error("scratch_projection_relation_cap_invalid")
    return relation_cap


def _inserted_toast_chunks_upper(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    scratch_input: ProviderDirectoryProfileScratchInput,
) -> int:
    if not scratch_input.inserted_rows:
        return 0
    return _checked_add(
        (
            scratch_input.inserted_logical_bytes
            + geometry.postgres_toast_max_chunk_size_bytes
            - 1
        )
        // geometry.postgres_toast_max_chunk_size_bytes,
        scratch_input.inserted_rows * scratch_input.toastable_column_count,
    )


def _scratch_growth_bytes(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    scratch_input: ProviderDirectoryProfileScratchInput,
    inserted_toast_chunks_upper: int,
) -> int:
    inserted_heap_pages = _checked_add(
        scratch_input.inserted_rows,
        inserted_toast_chunks_upper,
    )
    auxiliary_growth_pages = (
        2 * _checked_add(inserted_heap_pages, 2)
        if inserted_heap_pages
        else 0
    )
    index_growth_pages = _checked_add(
        _btree_insert_growth_pages(
            scratch_input.main_index_pages,
            scratch_input.inserted_rows,
        ),
        _btree_insert_growth_pages(
            scratch_input.toast_index_pages,
            inserted_toast_chunks_upper,
        ),
    )
    growth_bytes = (
        _checked_add(
            inserted_heap_pages,
            auxiliary_growth_pages,
            index_growth_pages,
        )
        * geometry.postgres_block_size_bytes
    )
    return growth_bytes


def _scratch_wal_bytes(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    scratch_input: ProviderDirectoryProfileScratchInput,
    inserted_toast_chunks_upper: int,
) -> int:
    inserted_heap_pages = _checked_add(
        scratch_input.inserted_rows,
        inserted_toast_chunks_upper,
    )
    heap_page_touches = (
        3 * _checked_add(inserted_heap_pages, 4)
        if inserted_heap_pages
        else 0
    )
    index_page_touches = _checked_add(
        _btree_wal_page_touches(
            scratch_input.main_index_pages,
            inserted_entries=scratch_input.inserted_rows,
            deleted_entries=0,
        ),
        _btree_wal_page_touches(
            scratch_input.toast_index_pages,
            inserted_entries=inserted_toast_chunks_upper,
            deleted_entries=0,
        ),
    )
    wal_bytes = (
        3
        * geometry.postgres_block_size_bytes
        * _checked_add(heap_page_touches, index_page_touches)
    )
    return wal_bytes


def _assert_scratch_projection_caps(
    relation_cap: ProviderDirectoryProfileRelationByteCaps,
    *,
    growth_bytes: int,
    wal_bytes: int,
) -> None:
    if growth_bytes > relation_cap.max_scratch_bytes:
        raise _error(
            "scratch_projection_growth_exceeded:"
            + relation_cap.relation_name
        )
    if wal_bytes > relation_cap.max_wal_bytes:
        raise _error(
            "scratch_projection_wal_exceeded:"
            + relation_cap.relation_name
        )


def _metadata_mutation_projection(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    mutation: ProviderDirectoryProfileMetadataMutationInput,
) -> tuple[int, int]:
    """Return bounded data and WAL bytes for one metadata mutation."""

    _validate_metadata_mutation(mutation)
    new_toast_chunks = _metadata_toast_chunk_count(geometry, mutation)
    return (
        _metadata_growth_bytes(geometry, mutation, new_toast_chunks),
        _metadata_wal_bytes(geometry, mutation, new_toast_chunks),
    )


def _validate_metadata_mutation(
    mutation: ProviderDirectoryProfileMetadataMutationInput,
) -> None:
    if (
        not isinstance(
            mutation,
            ProviderDirectoryProfileMetadataMutationInput,
        )
        or mutation.operation not in {"insert", "update"}
        or not mutation.relation_name
        or not 0
        <= mutation.payload_upper_bytes
        <= METADATA_PAYLOAD_UPPER_BOUND_BYTES
        or mutation.deleted_toast_chunks < 0
        or not mutation.main_index_pages
    ):
        raise _error("metadata_projection_input_invalid")


def _metadata_toast_chunk_count(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    mutation: ProviderDirectoryProfileMetadataMutationInput,
) -> int:
    return (
        mutation.payload_upper_bytes
        + geometry.postgres_toast_max_chunk_size_bytes
        - 1
    ) // geometry.postgres_toast_max_chunk_size_bytes


def _metadata_growth_bytes(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    mutation: ProviderDirectoryProfileMetadataMutationInput,
    new_toast_chunks: int,
) -> int:
    inserted_rows = 1
    inserted_heap_pages = _checked_add(
        inserted_rows,
        new_toast_chunks,
    )
    growth_pages = _checked_add(
        inserted_heap_pages,
        2 * _checked_add(inserted_heap_pages, 2),
        _btree_insert_growth_pages(
            mutation.main_index_pages,
            inserted_rows,
        ),
        _btree_insert_growth_pages(
            mutation.toast_index_pages,
            new_toast_chunks,
        ),
    )
    return _checked_add(
        growth_pages * geometry.postgres_block_size_bytes
    )


def _metadata_wal_bytes(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    mutation: ProviderDirectoryProfileMetadataMutationInput,
    new_toast_chunks: int,
) -> int:
    inserted_rows = 1
    deleted_rows = 1 if mutation.operation == "update" else 0
    heap_operations = _checked_add(
        inserted_rows,
        new_toast_chunks,
        deleted_rows,
        mutation.deleted_toast_chunks,
    )
    wal_page_touches = _checked_add(
        3 * _checked_add(heap_operations, 4),
        _btree_wal_page_touches(
            mutation.main_index_pages,
            inserted_entries=inserted_rows,
            deleted_entries=deleted_rows,
        ),
        _btree_wal_page_touches(
            mutation.toast_index_pages,
            inserted_entries=new_toast_chunks,
            deleted_entries=mutation.deleted_toast_chunks,
        ),
    )
    return _checked_add(
        3 * geometry.postgres_block_size_bytes * wal_page_touches
    )


def project_profile_delta_metadata_capacity(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    mutations: tuple[
        ProviderDirectoryProfileMetadataMutationInput,
        ...,
    ],
    *,
    pending_commit_items: int,
) -> ProviderDirectoryProfileMetadataProjection:
    """Bound final serving/receipt writes and the commit envelope."""

    verified_geometry = revalidate_capacity_geometry(geometry)
    if (
        not isinstance(mutations, tuple)
        or tuple(
            metadata_mutation.relation_name
            for metadata_mutation in mutations
        )
        != ("build_checkpoint", "serving_generation", "delta_receipt")
        or not isinstance(pending_commit_items, int)
        or isinstance(pending_commit_items, bool)
        or pending_commit_items < 0
    ):
        raise _error("metadata_projection_input_invalid")
    relation_projections = tuple(
        _metadata_mutation_projection(verified_geometry, mutation)
        for mutation in mutations
    )
    data_bytes = _checked_add(
        *(projection[0] for projection in relation_projections)
    )
    wal_bytes = _checked_add(
        *(projection[1] for projection in relation_projections)
    )
    commit_envelope_bytes = _checked_add(
        verified_geometry.postgres_block_size_bytes
        * _checked_add(1, pending_commit_items)
    )
    if data_bytes > verified_geometry.metadata_data_upper_bound_bytes:
        raise _error("metadata_projection_data_exceeded")
    if (
        wal_bytes + commit_envelope_bytes
        > verified_geometry.metadata_wal_upper_bound_bytes
    ):
        raise _error("metadata_projection_wal_exceeded")
    return ProviderDirectoryProfileMetadataProjection(
        data_bytes=data_bytes,
        wal_bytes=wal_bytes,
        commit_envelope_bytes=commit_envelope_bytes,
    )
