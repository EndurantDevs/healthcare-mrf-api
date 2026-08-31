# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact published Flex Practitioner boundary for Provider Directory Profile."""

from __future__ import annotations

from typing import Any, Callable, Mapping

from process.provider_directory_dataset_scoped_publication import (
    EXACT_DATASET_PUBLICATION_LOCK_IDENTITY,
    LEGACY_PRACTITIONER_VARIANT,
    ROOTED_COMBINED_VARIANT,
)
from process.provider_directory_fhir_root_policy import ReviewedRootPolicy
from process.provider_directory_profile_uhc_flex_contract import (
    _clean_text,
    _is_rooted_resource_counts_valid,
    _json_object,
    is_uhc_flex_dataset_variant_matching,
    is_uhc_flex_profile_source,
    is_uhc_flex_publication_metadata_valid,
    uhc_flex_profile_dataset_variant,
    uhc_flex_profile_expected_resources,
    uhc_flex_profile_source_variant,
    UHC_FLEX_LEGACY_PROFILE_RESOURCES,
    UHC_FLEX_LEGACY_PROFILE_VARIANT,
    UHC_FLEX_PROFILE_SELECTION_LOCK_RELATIONS,
    UHC_FLEX_ROOTED_PROFILE_RESOURCES,
    UHC_FLEX_ROOTED_PROFILE_VARIANT,
)
from process.provider_directory_profile_uhc_flex_store import (
    load_profile_selection_dataset_rows,
)
from process.uhc_flex_official_cohort_contract import UHC_FLEX_OFFICIAL_AUTHORITY_ID
from process.uhc_flex_practitioner_contract import UHC_FLEX_PRACTITIONER_SOURCE_ID


async def lock_uhc_flex_profile_publication(database: Any) -> None:
    """Serialize Profile fences with both exact-generation publishers."""

    await database.status(
        "SELECT pg_catalog.pg_advisory_xact_lock("
        "pg_catalog.hashtextextended(:lock_identity, 0));",
        lock_identity=EXACT_DATASET_PUBLICATION_LOCK_IDENTITY,
    )


def is_uhc_flex_dataset_row_ready(dataset_row: Mapping[str, Any]) -> bool:
    """Return whether a selected row carries the exact dedicated readiness proof."""

    metadata_by_field = _json_object(dataset_row.get("publication_metadata_json"))
    dataset_id = _clean_text(dataset_row.get("dataset_id")) or ""
    endpoint_id = _clean_text(dataset_row.get("endpoint_id")) or ""
    evidence_run_id = (
        _clean_text(dataset_row.get("acquisition_root_run_id"))
        or _clean_text(dataset_row.get("import_run_id"))
        or ""
    )
    variant = uhc_flex_profile_dataset_variant(dataset_id)
    projection_value = dataset_row.get("dataset_scoped_projection_as_of")
    projection_text = (
        projection_value.isoformat()
        if hasattr(projection_value, "isoformat")
        else projection_value
    )
    if not _is_common_dataset_row_ready(
        dataset_row,
        metadata_by_field,
        dataset_id,
        endpoint_id,
        evidence_run_id,
        variant,
        projection_text,
    ):
        return False
    return _is_variant_dataset_row_ready(dataset_row, variant)


def _is_common_dataset_row_ready(
    dataset_row: Mapping[str, Any],
    metadata_by_field: Mapping[str, Any],
    dataset_id: str,
    endpoint_id: str,
    evidence_run_id: str,
    variant: str | None,
    projection_text: object,
) -> bool:
    source_id = metadata_by_field.get("source_id")
    return bool(
        dataset_row.get("dataset_scoped_ready") is True
        and dataset_row.get("dataset_scoped_variant") == variant
        and is_uhc_flex_dataset_variant_matching(source_id, dataset_id)
        and is_uhc_flex_publication_metadata_valid(
            metadata_by_field,
            dataset_id=dataset_id,
            endpoint_id=endpoint_id,
            evidence_run_id=evidence_run_id,
        )
        and metadata_by_field.get("cohort_complete")
        is dataset_row.get("dataset_scoped_cohort_complete")
        and metadata_by_field.get("endpoint_collection_complete") is False
        and metadata_by_field.get("endpoint_complete") is False
        and metadata_by_field.get("semantic_projection_as_of") == projection_text
        and metadata_by_field.get("admission_id")
        == dataset_row.get("dataset_scoped_admission_id")
        and metadata_by_field.get("operation_key")
        == dataset_row.get("dataset_scoped_operation_key")
        and metadata_by_field.get("source_authority_id")
        == dataset_row.get("dataset_scoped_authority_id")
        and dataset_row.get("dataset_scoped_authority_id")
        == UHC_FLEX_OFFICIAL_AUTHORITY_ID
    )


def _is_variant_dataset_row_ready(
    dataset_row: Mapping[str, Any],
    variant: str | None,
) -> bool:
    if variant == LEGACY_PRACTITIONER_VARIANT:
        return bool(
            type(dataset_row.get("dataset_scoped_cohort_complete")) is bool
            and dataset_row.get("dataset_scoped_endpoint_collection_complete") is False
            and dataset_row.get("dataset_scoped_endpoint_complete") is False
        )
    return bool(
        variant == ROOTED_COMBINED_VARIANT
        and dataset_row.get("dataset_scoped_publication_kind")
        == ROOTED_COMBINED_VARIANT
        and type(dataset_row.get("dataset_scoped_cohort_complete")) is bool
        and dataset_row.get("dataset_scoped_rooted_graph_complete") is True
        and dataset_row.get("dataset_scoped_endpoint_collection_complete") is False
        and dataset_row.get("dataset_scoped_endpoint_complete") is False
    )


def is_uhc_flex_dataset_readiness_matching(
    readiness: Any,
    dataset_row: Mapping[str, Any],
) -> bool:
    """Return whether dedicated readiness matches its generic immutable parent."""

    metadata_by_field = _json_object(dataset_row.get("publication_metadata_json"))
    evidence_run_id = (
        _clean_text(dataset_row.get("acquisition_root_run_id"))
        or _clean_text(dataset_row.get("import_run_id"))
        or ""
    )
    dataset_id = _clean_text(dataset_row.get("dataset_id")) or ""
    endpoint_id = _clean_text(dataset_row.get("endpoint_id")) or ""
    source_id = _clean_text(dataset_row.get("source_id")) or ""
    variant = uhc_flex_profile_dataset_variant(dataset_id)
    readiness_retry_exhausted_count = getattr(readiness, "retry_exhausted_count", 0)
    metadata_retry_exhausted_count = metadata_by_field.get("retry_exhausted_count", 0)
    readiness_resource_counts = getattr(readiness, "resource_counts", None)
    is_readiness_variant_ready = variant == LEGACY_PRACTITIONER_VARIANT or (
        variant == ROOTED_COMBINED_VARIANT
        and getattr(readiness, "publication_kind", None) == ROOTED_COMBINED_VARIANT
        and getattr(readiness, "rooted_graph_complete", None) is True
        and _is_rooted_resource_counts_valid(readiness_resource_counts)
    )
    return bool(
        readiness is not None
        and is_uhc_flex_dataset_variant_matching(source_id, dataset_id)
        and is_readiness_variant_ready
        and getattr(readiness, "dataset_id", None) == dataset_id
        and getattr(readiness, "endpoint_id", None) == endpoint_id
        and getattr(readiness, "source_id", None) == source_id
        and getattr(readiness, "source_authority_id", None)
        == UHC_FLEX_OFFICIAL_AUTHORITY_ID
        and getattr(readiness, "dataset_hash", None)
        == _clean_text(dataset_row.get("dataset_hash"))
        and getattr(readiness, "resource_count", None)
        == dataset_row.get("resource_count")
        and getattr(readiness, "semantic_projection_as_of", None)
        == metadata_by_field.get("semantic_projection_as_of")
        and getattr(readiness, "admission_id", None)
        == metadata_by_field.get("admission_id")
        and getattr(readiness, "operation_key", None)
        == metadata_by_field.get("operation_key")
        and getattr(readiness, "cohort_complete", None)
        is metadata_by_field.get("cohort_complete")
        and readiness_retry_exhausted_count == metadata_retry_exhausted_count
        and is_uhc_flex_publication_metadata_valid(
            metadata_by_field,
            dataset_id=dataset_id,
            endpoint_id=endpoint_id,
            evidence_run_id=evidence_run_id,
        )
    )


def _readiness_annotation(readiness: Any, is_ready: bool) -> dict[str, Any]:
    variant = (
        uhc_flex_profile_dataset_variant(getattr(readiness, "dataset_id", None))
        if is_ready
        else None
    )
    return {
        "dataset_scoped_ready": is_ready,
        "dataset_scoped_variant": variant,
        "dataset_scoped_publication_kind": (
            getattr(readiness, "publication_kind", variant) if is_ready else None
        ),
        "dataset_scoped_projection_as_of": (
            getattr(readiness, "semantic_projection_as_of", None) if is_ready else None
        ),
        "dataset_scoped_authority_id": (
            getattr(readiness, "source_authority_id", None) if is_ready else None
        ),
        "dataset_scoped_admission_id": (
            getattr(readiness, "admission_id", None) if is_ready else None
        ),
        "dataset_scoped_operation_key": (
            getattr(readiness, "operation_key", None) if is_ready else None
        ),
        "dataset_scoped_cohort_complete": (
            getattr(readiness, "cohort_complete", None) if is_ready else None
        ),
        "dataset_scoped_rooted_graph_complete": (
            getattr(readiness, "rooted_graph_complete", None) if is_ready else None
        ),
        "dataset_scoped_endpoint_collection_complete": (
            getattr(readiness, "endpoint_collection_complete", None)
            if is_ready
            else None
        ),
        "dataset_scoped_endpoint_complete": (
            getattr(readiness, "endpoint_complete", None) if is_ready else None
        ),
    }


async def load_uhc_flex_profile_dataset_readiness(
    source_id: object,
    dataset_id: str,
    *,
    database: Any,
) -> Any:
    """Load readiness only from the dedicated header matching the source."""

    if not is_uhc_flex_dataset_variant_matching(source_id, dataset_id):
        return None
    if source_id == UHC_FLEX_PRACTITIONER_SOURCE_ID:
        from process.uhc_flex_practitioner_publication import (
            load_uhc_flex_practitioner_dataset_readiness,
        )

        return await load_uhc_flex_practitioner_dataset_readiness(
            dataset_id,
            database=database,
        )
    from process.provider_directory_rooted_graph_publication import (
        load_provider_directory_rooted_graph_dataset_readiness,
    )

    return await load_provider_directory_rooted_graph_dataset_readiness(
        dataset_id,
        database=database,
    )


async def annotate_uhc_flex_profile_dataset_readiness(
    dataset_rows: list[Any],
    *,
    database: Any,
    row_mapping: Callable[[Any], Mapping[str, Any]],
) -> list[Any]:
    """Annotate only exact DB-ready legacy/rooted parents for Profile."""

    annotated_rows: list[Any] = []
    for database_row in dataset_rows:
        dataset_row_by_field = dict(row_mapping(database_row))
        source_id = _clean_text(dataset_row_by_field.get("source_id"))
        if not is_uhc_flex_profile_source(source_id):
            annotated_rows.append(database_row)
            continue
        dataset_id = _clean_text(dataset_row_by_field.get("dataset_id"))
        readiness = (
            await load_uhc_flex_profile_dataset_readiness(
                source_id,
                dataset_id,
                database=database,
            )
            if dataset_id is not None
            else None
        )
        is_ready = is_uhc_flex_dataset_readiness_matching(
            readiness,
            dataset_row_by_field,
        )
        dataset_row_by_field.update(_readiness_annotation(readiness, is_ready))
        annotated_rows.append(dataset_row_by_field)
    return annotated_rows


def is_uhc_flex_fence_dataset_ready(dataset: Any, readiness: Any) -> bool:
    """Return whether a locked dataset still matches exact cohort readiness."""

    if readiness is None:
        return False
    retry_exhausted_count = getattr(readiness, "retry_exhausted_count", 0)
    is_coverage_ready = bool(
        type(retry_exhausted_count) is int
        and retry_exhausted_count >= 0
        and readiness.cohort_complete is (retry_exhausted_count == 0)
        and readiness.cohort_complete
        is getattr(dataset, "dataset_scoped_cohort_complete", None)
        and (
            retry_exhausted_count == 0
            or getattr(dataset, "reviewed_root_policy", None)
            == ReviewedRootPolicy(1)
        )
    )
    is_legacy_ready = bool(
        dataset.dataset_scoped_variant == LEGACY_PRACTITIONER_VARIANT
        and is_coverage_ready
    )
    is_rooted_ready = bool(
        dataset.dataset_scoped_variant == ROOTED_COMBINED_VARIANT
        and is_coverage_ready
        and readiness.publication_kind == ROOTED_COMBINED_VARIANT
        and readiness.rooted_graph_complete is True
        and _is_rooted_resource_counts_valid(readiness.resource_counts)
    )
    return bool(
        dataset.dataset_scoped_ready
        and is_uhc_flex_dataset_variant_matching(
            dataset.source_id,
            dataset.dataset_id,
        )
        and dataset.dataset_scoped_variant
        == uhc_flex_profile_dataset_variant(dataset.dataset_id)
        and readiness.dataset_id == dataset.dataset_id
        and readiness.endpoint_id == dataset.endpoint_id
        and readiness.source_id == dataset.source_id
        and readiness.dataset_hash == dataset.dataset_hash
        and readiness.resource_count == dataset.resource_count
        and readiness.semantic_projection_as_of == dataset.semantic_projection_as_of
        and readiness.source_authority_id == dataset.source_authority_id
        and readiness.admission_id == dataset.admission_id
        and readiness.operation_key == dataset.operation_key
        and readiness.endpoint_collection_complete is False
        and readiness.endpoint_complete is False
        and (is_legacy_ready or is_rooted_ready)
    )


__all__ = (
    "annotate_uhc_flex_profile_dataset_readiness",
    "is_uhc_flex_dataset_variant_matching",
    "is_uhc_flex_profile_source",
    "load_profile_selection_dataset_rows",
    "load_uhc_flex_profile_dataset_readiness",
    "is_uhc_flex_dataset_readiness_matching",
    "is_uhc_flex_dataset_row_ready",
    "is_uhc_flex_fence_dataset_ready",
    "is_uhc_flex_publication_metadata_valid",
    "lock_uhc_flex_profile_publication",
    "uhc_flex_profile_expected_resources",
    "uhc_flex_profile_dataset_variant",
    "uhc_flex_profile_source_variant",
    "UHC_FLEX_LEGACY_PROFILE_RESOURCES",
    "UHC_FLEX_LEGACY_PROFILE_VARIANT",
    "UHC_FLEX_PROFILE_SELECTION_LOCK_RELATIONS",
    "UHC_FLEX_ROOTED_PROFILE_RESOURCES",
    "UHC_FLEX_ROOTED_PROFILE_VARIANT",
)
