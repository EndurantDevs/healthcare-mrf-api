# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact published Flex Practitioner boundary for Provider Directory Profile."""

from __future__ import annotations

import json
import re
from typing import Any, Callable, Mapping

from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
)
from process.uhc_flex_official_cohort_contract import (
    UHC_FLEX_OFFICIAL_AUTHORITY_ID,
    UHC_FLEX_OFFICIAL_RESOURCE_TYPE,
)
from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_PUBLICATION_LOCK_IDENTITY,
    UHC_FLEX_PRACTITIONER_SOURCE_ID,
)
from process.uhc_flex_practitioner_publication import (
    UHC_FLEX_PRACTITIONER_DATASET_PUBLICATION_CONTRACT_ID,
)


UHC_FLEX_PROFILE_SELECTION_LOCK_RELATIONS = (
    "provider_directory_dataset_resource",
    "provider_directory_uhc_flex_practitioner_acquisition",
    "provider_directory_uhc_flex_practitioner_dataset",
    "provider_directory_uhc_flex_practitioner_dataset_resource",
    "provider_directory_uhc_flex_practitioner_resource",
    "provider_directory_uhc_flex_practitioner_twin_admission",
    "provider_directory_uhc_flex_practitioner_work",
)


def _clean_text(raw_value: Any) -> str | None:
    return raw_value.strip() if isinstance(raw_value, str) and raw_value.strip() else None


def _json_object(raw_value: Any) -> dict[str, Any]:
    decoded_value = raw_value
    if isinstance(decoded_value, str):
        try:
            decoded_value = json.loads(decoded_value)
        except (TypeError, ValueError):
            return {}
    return dict(decoded_value) if isinstance(decoded_value, Mapping) else {}


async def lock_uhc_flex_profile_publication(database: Any) -> None:
    """Serialize Profile fences with exact-cohort publication mutations."""

    await database.status(
        "SELECT pg_catalog.pg_advisory_xact_lock("
        "pg_catalog.hashtextextended(:lock_identity, 0));",
        lock_identity=UHC_FLEX_PRACTITIONER_PUBLICATION_LOCK_IDENTITY,
    )


def is_uhc_flex_publication_metadata_valid(
    publication_metadata: Mapping[str, Any],
    *,
    dataset_id: str,
    endpoint_id: str,
    evidence_run_id: str,
) -> bool:
    """Return whether closed publication metadata binds the exact Profile row."""

    return bool(
        publication_metadata.get("publication_contract_id")
        == UHC_FLEX_PRACTITIONER_DATASET_PUBLICATION_CONTRACT_ID
        and publication_metadata.get("dataset_id") == dataset_id
        and publication_metadata.get("endpoint_id") == endpoint_id
        and publication_metadata.get("acquisition_root_run_id")
        == evidence_run_id
        and publication_metadata.get("source_id")
        == UHC_FLEX_PRACTITIONER_SOURCE_ID
        and publication_metadata.get("source_ids")
        == [UHC_FLEX_PRACTITIONER_SOURCE_ID]
        and publication_metadata.get("source_authority_id")
        == UHC_FLEX_OFFICIAL_AUTHORITY_ID
        and publication_metadata.get("selected_resources")
        == [UHC_FLEX_OFFICIAL_RESOURCE_TYPE]
        and publication_metadata.get("expected_resources")
        == [UHC_FLEX_OFFICIAL_RESOURCE_TYPE]
        and publication_metadata.get("resource_hash_contract")
        == SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
        and publication_metadata.get("cohort_complete") is True
        and publication_metadata.get("endpoint_collection_complete") is False
        and publication_metadata.get("endpoint_complete") is False
        and _clean_text(publication_metadata.get("semantic_projection_as_of"))
        is not None
        and _clean_text(publication_metadata.get("admission_id")) is not None
        and re.fullmatch(
            r"[0-9a-f]{64}",
            _clean_text(publication_metadata.get("operation_key")) or "",
        )
        is not None
    )


def is_uhc_flex_dataset_row_ready(dataset_row: Mapping[str, Any]) -> bool:
    """Return whether a selected row carries the exact dedicated readiness proof."""

    metadata_by_field = _json_object(
        dataset_row.get("publication_metadata_json")
    )
    projection_value = dataset_row.get("dataset_scoped_projection_as_of")
    projection_text = (
        projection_value.isoformat()
        if hasattr(projection_value, "isoformat")
        else projection_value
    )
    return bool(
        dataset_row.get("dataset_scoped_ready") is True
        and metadata_by_field.get("source_ids")
        == [UHC_FLEX_PRACTITIONER_SOURCE_ID]
        and metadata_by_field.get("source_id")
        == UHC_FLEX_PRACTITIONER_SOURCE_ID
        and metadata_by_field.get("cohort_complete") is True
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


async def load_profile_selection_dataset_rows(
    *,
    database: Any,
    endpoint_dataset_ref: str,
    schema_ref: str,
    row_mapping: Callable[[Any], Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    """Load published parent rows with exact Flex readiness projections."""

    header_ref = (
        f"{schema_ref}."
        '"provider_directory_uhc_flex_practitioner_dataset"'
    )
    ready_ref = (
        f"{schema_ref}."
        '"provider_directory_uhc_flex_practitioner_dataset_ready"'
    )
    database_rows = await database.all(
        f"""
        SELECT dataset.endpoint_id, dataset.dataset_id,
               dataset.acquisition_root_run_id, dataset.dataset_hash,
               dataset.status, dataset.is_current, dataset.resource_count,
               dataset.validated_at, dataset.published_at,
               dataset.superseded_at, dataset.publication_metadata_json,
               CASE WHEN scoped.dataset_id IS NULL THEN false
                    ELSE {ready_ref}(dataset.dataset_id)
               END AS dataset_scoped_ready,
               scoped.admission_id AS dataset_scoped_admission_id,
               scoped.semantic_projection_as_of AS dataset_scoped_projection_as_of,
               scoped.source_authority_id AS dataset_scoped_authority_id,
               scoped.operation_key AS dataset_scoped_operation_key
          FROM {endpoint_dataset_ref} AS dataset
          LEFT JOIN {header_ref} AS scoped
            ON scoped.dataset_id = dataset.dataset_id
         WHERE dataset.status = 'published'
           AND dataset.is_current = true
           AND dataset.published_at IS NOT NULL
           AND dataset.superseded_at IS NULL
         ORDER BY dataset.published_at DESC, dataset.dataset_id DESC,
                  dataset.endpoint_id DESC;
        """
    )
    return [row_mapping(database_row) for database_row in database_rows]


def is_uhc_flex_dataset_readiness_matching(
    readiness: Any,
    dataset_row: Mapping[str, Any],
) -> bool:
    """Return whether dedicated readiness matches its generic immutable parent."""

    metadata_by_field = _json_object(
        dataset_row.get("publication_metadata_json")
    )
    evidence_run_id = (
        _clean_text(dataset_row.get("acquisition_root_run_id"))
        or _clean_text(dataset_row.get("import_run_id"))
        or ""
    )
    dataset_id = _clean_text(dataset_row.get("dataset_id")) or ""
    endpoint_id = _clean_text(dataset_row.get("endpoint_id")) or ""
    return bool(
        readiness is not None
        and getattr(readiness, "dataset_id", None) == dataset_id
        and getattr(readiness, "endpoint_id", None) == endpoint_id
        and getattr(readiness, "source_id", None)
        == UHC_FLEX_PRACTITIONER_SOURCE_ID
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
        and is_uhc_flex_publication_metadata_valid(
            metadata_by_field,
            dataset_id=dataset_id,
            endpoint_id=endpoint_id,
            evidence_run_id=evidence_run_id,
        )
    )


def _readiness_annotation(readiness: Any, is_ready: bool) -> dict[str, Any]:
    return {
        "dataset_scoped_ready": is_ready,
        "dataset_scoped_projection_as_of": (
            getattr(readiness, "semantic_projection_as_of", None)
            if is_ready
            else None
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
    }


async def annotate_uhc_flex_profile_dataset_readiness(
    dataset_rows: list[Any],
    *,
    database: Any,
    row_mapping: Callable[[Any], Mapping[str, Any]],
) -> list[Any]:
    """Annotate only exact DB-ready Flex parents for Profile selection."""

    from process.uhc_flex_practitioner_publication import (
        load_uhc_flex_practitioner_dataset_readiness,
    )

    annotated_rows: list[Any] = []
    for database_row in dataset_rows:
        dataset_row_by_field = dict(row_mapping(database_row))
        if (
            _clean_text(dataset_row_by_field.get("source_id"))
            != UHC_FLEX_PRACTITIONER_SOURCE_ID
        ):
            annotated_rows.append(database_row)
            continue
        dataset_id = _clean_text(dataset_row_by_field.get("dataset_id"))
        readiness = (
            await load_uhc_flex_practitioner_dataset_readiness(
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
        dataset_row_by_field.update(
            _readiness_annotation(readiness, is_ready)
        )
        annotated_rows.append(dataset_row_by_field)
    return annotated_rows


def is_uhc_flex_fence_dataset_ready(dataset: Any, readiness: Any) -> bool:
    """Return whether a locked dataset still matches exact cohort readiness."""

    return bool(
        dataset.dataset_scoped_ready
        and readiness is not None
        and readiness.dataset_id == dataset.dataset_id
        and readiness.endpoint_id == dataset.endpoint_id
        and readiness.source_id == dataset.source_id
        and readiness.dataset_hash == dataset.dataset_hash
        and readiness.resource_count == dataset.resource_count
        and readiness.semantic_projection_as_of
        == dataset.semantic_projection_as_of
        and readiness.source_authority_id == dataset.source_authority_id
        and readiness.admission_id == dataset.admission_id
        and readiness.operation_key == dataset.operation_key
        and readiness.cohort_complete is True
        and readiness.endpoint_collection_complete is False
        and readiness.endpoint_complete is False
    )


__all__ = (
    "annotate_uhc_flex_profile_dataset_readiness",
    "load_profile_selection_dataset_rows",
    "is_uhc_flex_dataset_readiness_matching",
    "is_uhc_flex_dataset_row_ready",
    "is_uhc_flex_fence_dataset_ready",
    "is_uhc_flex_publication_metadata_valid",
    "lock_uhc_flex_profile_publication",
    "UHC_FLEX_PROFILE_SELECTION_LOCK_RELATIONS",
)
